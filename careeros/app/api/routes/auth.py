"""
app/api/routes/auth.py

Authentication endpoints.

Routes:
  POST /api/v1/auth/register   — create account, returns tokens
  POST /api/v1/auth/login      — authenticate, returns tokens
  POST /api/v1/auth/refresh    — rotate refresh token, returns new token pair
  POST /api/v1/auth/logout     — revoke session
  GET  /api/v1/auth/me         — current user info
"""

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.dependencies import DB
from app.schemas.auth import (
    LoginRequest,
    MessageResponse,
    RefreshRequest,
    RegisterRequest,
    RegisterResponse,
    TokenResponse,
    UserResponse,

    UpdateProfileRequest,
    ChangePasswordRequest,
)
from app.services.auth.auth_service import AuthError, AuthService

router = APIRouter(prefix="/api/v1/auth", tags=["Authentication"])
bearer = HTTPBearer(auto_error=False)


def _service(db: DB) -> AuthService:
    return AuthService(db=db)


def _auth_error_to_http(e: AuthError) -> HTTPException:
    return HTTPException(status_code=e.status_code, detail=e.message)


# ── Register ──────────────────────────────────────────────────────────────────

@router.post(
    "/register",
    status_code=status.HTTP_201_CREATED,
    response_model=RegisterResponse,
    summary="Create a new account",
)
async def register(payload: RegisterRequest, db: DB) -> RegisterResponse:
    svc = _service(db)
    try:
        user = await svc.register(
            email=payload.email,
            password=payload.password,
            full_name=payload.full_name,
        )

        # Provision free subscription for new user
        try:
            from app.services.billing.billing_service import BillingService
            billing = BillingService(db=db)
            await billing.provision_free_subscription(user.id)
        except Exception:
            await db.rollback()
            raise

        await db.commit()

        # Auto-login after registration
        _, access_token, refresh_token = await svc.login(
            email=payload.email,
            password=payload.password,
        )
        await db.commit()

        # Send verification email (non-fatal)
        try:
            from app.services.email.service import get_email_service
            from app.services.email.tokens import generate_token
            token = generate_token(user.id, "verify")
            get_email_service().send_verification_email(
                to_email=user.email,
                to_name=user.full_name,
                token=token,
            )
        except Exception as _e:
            import structlog
            structlog.get_logger(__name__).warning("verify_email_send_failed", error=str(_e))

    except AuthError as e:
        raise _auth_error_to_http(e)

    return RegisterResponse(
        user=UserResponse.model_validate(user),
        access_token=access_token,
        refresh_token=refresh_token,
    )


# ── Login ─────────────────────────────────────────────────────────────────────

@router.post(
    "/login",
    response_model=TokenResponse,
    summary="Authenticate and receive tokens",
)
async def login(payload: LoginRequest, db: DB) -> TokenResponse:
    svc = _service(db)
    try:
        _, access_token, refresh_token = await svc.login(
            email=payload.email,
            password=payload.password,
        )
        await db.commit()
    except AuthError as e:
        raise _auth_error_to_http(e)

    return TokenResponse(access_token=access_token, refresh_token=refresh_token)


# ── Refresh ───────────────────────────────────────────────────────────────────

@router.post(
    "/refresh",
    response_model=TokenResponse,
    summary="Rotate refresh token and get new access token",
)
async def refresh(payload: RefreshRequest, db: DB) -> TokenResponse:
    svc = _service(db)
    try:
        access_token, refresh_token = await svc.refresh(payload.refresh_token)
        await db.commit()
    except AuthError as e:
        raise _auth_error_to_http(e)

    return TokenResponse(access_token=access_token, refresh_token=refresh_token)


# ── Logout ────────────────────────────────────────────────────────────────────

@router.post(
    "/logout",
    response_model=MessageResponse,
    summary="Revoke current session",
)
async def logout(
    db: DB,
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
) -> MessageResponse:
    if not credentials:
        raise HTTPException(status_code=401, detail="Not authenticated")

    svc = _service(db)
    try:
        user = await svc.get_current_user_from_token(credentials.credentials)
        await svc.logout(user.id)
        await db.commit()
    except AuthError as e:
        raise _auth_error_to_http(e)

    return MessageResponse(message="Logged out successfully.")


# ── Me ────────────────────────────────────────────────────────────────────────

@router.get(
    "/me",
    response_model=UserResponse,
    summary="Get current user info",
)
async def me(
    db: DB,
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
) -> UserResponse:
    if not credentials:
        raise HTTPException(status_code=401, detail="Not authenticated")

    svc = _service(db)
    try:
        user = await svc.get_current_user_from_token(credentials.credentials)
    except AuthError as e:
        raise _auth_error_to_http(e)

    return UserResponse.model_validate(user)


# ── Update profile ────────────────────────────────────────────────────────────

@router.patch(
    "/me",
    response_model=UserResponse,
    summary="Update current user profile",
)
async def update_me(
    payload: UpdateProfileRequest,
    db: DB,
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
) -> UserResponse:
    if not credentials:
        raise HTTPException(status_code=401, detail="Not authenticated")
    svc = _service(db)
    try:
        user = await svc.get_current_user_from_token(credentials.credentials)
    except AuthError as e:
        raise _auth_error_to_http(e)

    if payload.full_name is not None:
        user.full_name = payload.full_name
    await db.flush()
    await db.commit()
    return UserResponse.model_validate(user)


@router.post(
    "/change-password",
    response_model=MessageResponse,
    summary="Change password",
)
async def change_password(
    payload: ChangePasswordRequest,
    db: DB,
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
) -> MessageResponse:
    if not credentials:
        raise HTTPException(status_code=401, detail="Not authenticated")
    svc = _service(db)
    try:
        user = await svc.get_current_user_from_token(credentials.credentials)
    except AuthError as e:
        raise _auth_error_to_http(e)

    # Verify current password
    from app.services.auth.password import hash_password, verify_password
    if not verify_password(payload.current_password, user.password_hash):
        raise HTTPException(status_code=400, detail={"error": "Current password is incorrect"})

    user.password_hash = hash_password(payload.new_password)
    await db.flush()
    await db.commit()
    return MessageResponse(message="Password updated successfully.")
