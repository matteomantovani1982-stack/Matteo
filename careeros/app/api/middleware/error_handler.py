"""
app/api/middleware/error_handler.py

Global exception handlers registered on the FastAPI app.
Converts unhandled exceptions into consistent JSON error responses.
"""

import structlog
from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError

logger = structlog.get_logger(__name__)


class CareerOSError(Exception):
    """Base class for all application-level errors."""

    def __init__(self, message: str, status_code: int = 500) -> None:
        self.message = message
        self.status_code = status_code
        super().__init__(message)


class NotFoundError(CareerOSError):
    def __init__(self, resource: str, resource_id: str) -> None:
        super().__init__(
            message=f"{resource} '{resource_id}' not found.",
            status_code=status.HTTP_404_NOT_FOUND,
        )


class ValidationError(CareerOSError):
    def __init__(self, message: str) -> None:
        super().__init__(message=message, status_code=status.HTTP_422_UNPROCESSABLE_ENTITY)


class StorageError(CareerOSError):
    def __init__(self, message: str) -> None:
        super().__init__(message=message, status_code=status.HTTP_502_BAD_GATEWAY)


def register_error_handlers(app: FastAPI) -> None:
    """
    Register all global exception handlers on the FastAPI app.
    Call this once during app startup in main.py.
    """

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        logger.warning(
            "validation_error",
            path=request.url.path,
            method=request.method,
            errors=str(exc.errors()),
        )
        return JSONResponse(
            status_code=422,
            content={"detail": exc.errors()},
        )

    @app.exception_handler(CareerOSError)
    async def careeros_error_handler(
        request: Request, exc: CareerOSError
    ) -> JSONResponse:
        logger.warning(
            "application_error",
            path=request.url.path,
            method=request.method,
            http_status=exc.status_code,
            message=exc.message,
        )
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": exc.message, "type": type(exc).__name__},
        )

    @app.exception_handler(SQLAlchemyError)
    async def sqlalchemy_error_handler(
        request: Request, exc: SQLAlchemyError
    ) -> JSONResponse:
        logger.error(
            "database_error",
            path=request.url.path,
            method=request.method,
            error=str(exc),
        )
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"error": "A database error occurred.", "type": "DatabaseError"},
        )

    @app.exception_handler(Exception)
    async def unhandled_error_handler(
        request: Request, exc: Exception
    ) -> JSONResponse:
        logger.error(
            "unhandled_error",
            path=request.url.path,
            method=request.method,
            error=str(exc),
            exc_info=True,
        )
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": "An unexpected error occurred.", "type": "InternalError"},
        )
