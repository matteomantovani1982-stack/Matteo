"""
app/api/routes/email.py

Email verification and password reset endpoints.

Flows:

  EMAIL VERIFICATION:
    POST /api/v1/email/send-verification
      → generates token, sends verification email
      → idempotent: safe to call again if email not received
      → requires auth (user must be logged in)

    GET /api/v1/email/verify?token=...
      → validates token, sets user.is_verified = True
      → sends welcome email on first verification
      → no auth required (link from email)

  PASSWORD RESET:
    POST /api/v1/email/forgot-password
      → accepts email address
      → generates token, sends reset email
      → always returns 200 (don't reveal whether email exists)
      → no auth required

    POST /api/v1/email/reset-password
      → accepts token + new password
      → validates token, updates password hash, invalidates refresh token
      → no auth required
"""

import structlog

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db_session as get_db
from app.models.user import User
from app.services.auth.auth_service import AuthService
from app.services.auth.password import hash_password
from app.services.email.service import get_email_service
from app.services.email.tokens import (
    TokenError, generate_token, validate_token,
)
from app.dependencies import CurrentUser

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/email", tags=["email"])


# ── Request / Response schemas ────────────────────────────────────────────────

class SendVerificationResponse(BaseModel):
    message: str

class VerifyEmailResponse(BaseModel):
    message: str
    is_verified: bool

class ForgotPasswordRequest(BaseModel):
    email: EmailStr

class ForgotPasswordResponse(BaseModel):
    message: str

class ResetPasswordRequest(BaseModel):
    token: str
    new_password: str = Field(..., min_length=8, max_length=128)

class ResetPasswordResponse(BaseModel):
    message: str



# ── DB helpers (used by multiple endpoints) ────────────────────────────────────

from sqlalchemy import select
import uuid as _uuid

async def _get_user_by_id(db: AsyncSession, user_id: str) -> User | None:
    try:
        uid = _uuid.UUID(user_id)
    except ValueError:
        return None
    result = await db.execute(select(User).where(User.id == uid))
    return result.scalar_one_or_none()

async def _get_user_by_email(db: AsyncSession, email: str) -> User | None:
    result = await db.execute(select(User).where(User.email == email.lower()))
    return result.scalar_one_or_none()


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post(
    "/send-verification",
    response_model=SendVerificationResponse,
    summary="Send or resend email verification link",
)
async def send_verification(
    current_user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    """
    Send a verification link to the current user's email address.
    Safe to call multiple times — old tokens expire naturally.
    """
    if current_user.is_verified:
        return SendVerificationResponse(message="Email is already verified.")

    token = generate_token(current_user.id, "verify")
    email_svc = get_email_service()
    email_svc.send_verification_email(
        to_email=current_user.email,
        to_name=current_user.full_name,
        token=token,
    )

    logger.info("verification_email_sent", user_id=str(current_user.id))
    return SendVerificationResponse(
        message="Verification email sent. Check your inbox."
    )


@router.get(
    "/verify",
    response_model=VerifyEmailResponse,
    summary="Verify email address via token from link",
)
async def verify_email(
    token: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Validate the verification token and mark user as verified.
    Called when user clicks the link in their email.
    """
    try:
        user_id = validate_token(token, "verify")
    except TokenError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Verification link is invalid or has expired. {e}",
        )

    user = await _get_user_by_id(db, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")

    if user.is_verified:
        return VerifyEmailResponse(message="Email already verified.", is_verified=True)

    # Mark verified
    user.is_verified = True
    await db.commit()
    await db.refresh(user)

    # Send welcome email (fire and forget — don't block response)
    try:
        email_svc = get_email_service()
        email_svc.send_welcome_email(
            to_email=user.email,
            to_name=user.full_name,
        )
    except Exception as e:
        logger.warning("welcome_email_failed", user_id=str(user.id), error=str(e))

    logger.info("email_verified", user_id=str(user.id))
    return VerifyEmailResponse(
        message="Email verified successfully. Welcome to CVLab!",
        is_verified=True,
    )


@router.post(
    "/forgot-password",
    response_model=ForgotPasswordResponse,
    summary="Request a password reset email",
)
async def forgot_password(
    payload: ForgotPasswordRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Generate and send a password reset link.
    Always returns 200 — does not reveal whether the email exists.
    """
    user = await _get_user_by_email(db, payload.email)

    # Always log but only send if user exists — user can't tell the difference
    if user and user.is_active:
        token = generate_token(user.id, "reset")
        email_svc = get_email_service()
        email_svc.send_password_reset_email(
            to_email=user.email,
            to_name=user.full_name,
            token=token,
        )
        logger.info("password_reset_sent", user_id=str(user.id))
    else:
        logger.info("password_reset_no_user", email=payload.email)

    return ForgotPasswordResponse(
        message="If an account with that email exists, a reset link has been sent."
    )


@router.post(
    "/reset-password",
    response_model=ResetPasswordResponse,
    summary="Set a new password using a reset token",
)
async def reset_password(
    payload: ResetPasswordRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Validate the reset token and update the user's password.
    Also invalidates any active refresh tokens (forces re-login).
    """
    try:
        user_id = validate_token(payload.token, "reset")
    except TokenError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Reset link is invalid or has expired. {e}",
        )

    user = await _get_user_by_id(db, user_id)
    if not user or not user.is_active:
        raise HTTPException(status_code=404, detail="User not found.")

    # Update password + invalidate sessions
    user.password_hash = hash_password(payload.new_password)
    user.refresh_token_hash = None  # force re-login everywhere

    await db.commit()
    logger.info("password_reset_complete", user_id=str(user.id))

    return ResetPasswordResponse(
        message="Password updated successfully. You can now sign in with your new password."
    )
