"""
Exception handlers for the FastAPI application.
"""

import logging

from fastapi import HTTPException, Request, status
from fastapi.responses import JSONResponse

from src.service.error_mapping import map_error
from src.service.exceptions import SparkManagerError

logger = logging.getLogger(__name__)


def _format_error(
    status_code: int, error_code: int, error_type_str: str, message: str | None
):
    """Format error response with consistent structure."""
    return JSONResponse(
        status_code=status_code,
        content={
            "error": error_code,
            "error_type": error_type_str,
            "message": message or error_type_str,
        },
    )


async def universal_error_handler(request: Request, exc: Exception):
    """
    Universal handler for all types of exceptions.

    Handles:
    - SparkManagerError and its subclasses (including all specific error types)
    - HTTPException from FastAPI
    - Generic exceptions
    """
    # Default values
    error_code = 0
    error_type_str = None
    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR

    if isinstance(exc, SparkManagerError):
        # handle SparkManagerError and subclasses
        error_type, status_code = map_error(exc)

        # Extract values from error_type if available
        if error_type:
            error_code = error_type.error_code
            error_type_str = error_type.error_type
        else:
            raise ValueError("this should be impossible")

        # Get the exception message, falling back to the error type string
        message = str(exc) if str(exc) else error_type_str

    elif isinstance(exc, HTTPException):
        # handle FastAPI Exceptions
        status_code = exc.status_code
        message = str(exc.detail)
    else:
        # handle all other generic exceptions
        logger.error("Unhandled exception: %s", exc, exc_info=True)
        message = "An unexpected error occurred"

    return _format_error(status_code, error_code, error_type_str, message)
