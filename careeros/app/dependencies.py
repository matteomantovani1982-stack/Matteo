"""
app/dependencies.py

Shared FastAPI dependency injectors.

The key addition in Sprint C:
  CurrentUser — validates Bearer token and returns the authenticated User.
  Use this in all protected routes instead of the old X-User-Id header.

Backward compatibility:
  Routes that previously used X-User-Id still work during transition.
  The UserIdFromAuth dependency provides the user_id string from the JWT
  so route signatures don't need to change immediately.
"""

import uuid
from collections.abc import AsyncGenerator
from typing import Annotated

import boto3
from fastapi import Depends, Header, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db.session import get_db_session

# ── Database ───────────────────────────────────────────────────────────────
DB = Annotated[AsyncSession, Depends(get_db_session)]

# ── Bearer token extractor ────────────────────────────────────────────────
_bearer = HTTPBearer(auto_error=False)


async def get_current_user(
    db: DB,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)] = None,
):
    """
    Validate Bearer token and return the authenticated User model.

    Raises 401 if:
      - No Authorization header
      - Token invalid or expired
      - User not found or disabled

    Usage in routes:
      async def my_route(current_user: CurrentUser): ...
    """
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    from app.services.auth.auth_service import AuthError, AuthService
    svc = AuthService(db=db)
    try:
        user = await svc.get_current_user_from_token(credentials.credentials)
    except AuthError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=e.message,
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user


# Type alias for use in route signatures
from app.models.user import User
CurrentUser = Annotated[User, Depends(get_current_user)]


async def get_current_user_id(current_user: CurrentUser) -> str:
    """Returns user_id as string — drop-in for old X-User-Id header pattern."""
    return str(current_user.id)


CurrentUserId = Annotated[str, Depends(get_current_user_id)]


# ── S3 client ─────────────────────────────────────────────────────────────
def get_s3_client():
    kwargs = {
        "region_name": settings.s3_region,
        "aws_access_key_id": settings.s3_access_key_id,
        "aws_secret_access_key": settings.s3_secret_access_key,
    }
    if settings.s3_endpoint_url:
        kwargs["endpoint_url"] = settings.s3_endpoint_url
    return boto3.client("s3", **kwargs)


S3Client = Annotated[object, Depends(get_s3_client)]
