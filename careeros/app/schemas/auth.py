"""
app/schemas/auth.py

Pydantic schemas for auth endpoints.
"""

import uuid
from datetime import datetime

from pydantic import BaseModel, EmailStr, Field, field_validator


class RegisterRequest(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=8, description="Minimum 8 characters")
    full_name: str | None = Field(None, max_length=255)


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class RefreshRequest(BaseModel):
    refresh_token: str


class TokenResponse(BaseModel):
    """Returned on login and token refresh."""
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int = Field(
        default=1800,
        description="Access token lifetime in seconds (30 minutes)",
    )


class UserResponse(BaseModel):
    """Public user info — never includes password or token data."""
    id: uuid.UUID
    email: str
    full_name: str | None
    is_active: bool
    is_verified: bool
    created_at: datetime

    model_config = {"from_attributes": True}


class RegisterResponse(BaseModel):
    """Returned on successful registration."""
    user: UserResponse
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class MessageResponse(BaseModel):
    """Generic success message."""
    message: str


class UpdateProfileRequest(BaseModel):
    full_name: str | None = Field(None, max_length=255)


class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str = Field(..., min_length=8)
