"""
Exceptions thrown by the Spark Manager system.
"""

# mostly copied from https://github.com/kbase/cdm-task-service/blob/main/cdmtaskservice/errors.py

from enum import Enum


class ErrorType(Enum):
    """
    The type of an error, consisting of an error code and a brief string describing the type.
    :ivar error_code: an integer error code.
    :ivar error_type: a brief string describing the error type.
    """

    AUTHENTICATION_FAILED = (10000, "Authentication failed")  # noqa: E222 @IgnorePep8
    """ A general authentication error. """

    NO_TOKEN = (10010, "No authentication token")  # noqa: E222 @IgnorePep8
    """ No token was provided when required. """

    INVALID_TOKEN = (10020, "Invalid token")  # noqa: E222 @IgnorePep8
    """ The token provided is not valid. """

    INVALID_AUTH_HEADER = (
        10030,
        "Invalid authentication header",
    )  # noqa: E222 @IgnorePep8
    """ The authentication header is not valid. """

    MISSING_ROLE = (10040, "Missing required role")  # noqa: E222 @IgnorePep8
    """ The user is missing a required role. """

    def __init__(self, error_code, error_type):
        self.error_code = error_code
        self.error_type = error_type
