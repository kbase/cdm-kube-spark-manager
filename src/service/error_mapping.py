"""
Map errors from exception type to custom error type and HTTP status.
"""

# Copied from https://github.com/kbase/cdm-task-service/blob/main/cdmtaskservice/error_mapping.py

from typing import NamedTuple, Optional

from fastapi import status

from src.service.errors import ErrorType
from src.service.exceptions import (
    InvalidAuthHeaderError,
    InvalidTokenError,
    MissingRoleError,
    MissingTokenError,
)

_H400 = status.HTTP_400_BAD_REQUEST
_H401 = status.HTTP_401_UNAUTHORIZED
_H403 = status.HTTP_403_FORBIDDEN
_H404 = status.HTTP_404_NOT_FOUND


class ErrorMapping(NamedTuple):
    """The application error type and HTTP status code for an exception."""

    err_type: Optional[ErrorType]
    """ The type of application error. None if a 5XX error or Not Found."""
    http_code: int
    """ The HTTP code of the error. """


# Map only using ErrorTypes that actually exist in errors.py
_ERR_MAP = {
    MissingTokenError: ErrorMapping(ErrorType.NO_TOKEN, _H401),
    InvalidAuthHeaderError: ErrorMapping(ErrorType.INVALID_AUTH_HEADER, _H401),
    InvalidTokenError: ErrorMapping(ErrorType.INVALID_TOKEN, _H401),
    MissingRoleError: ErrorMapping(ErrorType.MISSING_ROLE, _H403),
}


def map_error(err: Exception) -> ErrorMapping:
    """
    Map an error to an optional error type and a HTTP code.
    """
    # May need to add code to go up the error hierarchy if multiple errors have the same type
    error_type = type(err)
    mapping = None

    # Safely lookup the error type
    if error_type in _ERR_MAP:
        mapping = _ERR_MAP[error_type]

    if not mapping:
        mapping = ErrorMapping(None, status.HTTP_500_INTERNAL_SERVER_ERROR)

    return mapping
