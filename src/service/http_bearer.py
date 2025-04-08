"""
Alteration of FastAPI's HTTPBearer class to handle the KBase authorization
step and allow for more informative errors.

Also adds an `optional` keyword argument that allows for missing, but not malformed,
authentication headers. If `optional` is `True` and no authorization header is provided, `None`
will be returned in place of the normal `KBaseUser`.
"""

from typing import Optional

from fastapi.openapi.models import HTTPBearer as HTTPBearerModel
from fastapi.requests import Request
from fastapi.security.http import HTTPBase
from fastapi.security.utils import get_authorization_scheme_param

from src.service import app_state, errors, kb_auth

# Modified from https://github.com/tiangolo/fastapi/blob/e13df8ee79d11ad8e338026d99b1dcdcb2261c9f/fastapi/security/http.py#L100

_SCHEME = "Bearer"


class KBaseHTTPBearer(HTTPBase):
    def __init__(
        self,
        *,
        bearerFormat: Optional[str] = None,
        scheme_name: Optional[str] = None,
        description: Optional[str] = None,
        # FastAPI uses auto_error, but that allows for malformed headers as well as just
        # no header. Use a different variable name since the behavior is different.
        optional: bool = False,
        # Considered adding a required auth role here and throwing an exception if the user
        # doesn't have it, but often you want to customize the error message.
        # Easier to handle that in the route method.
    ):
        self.model = HTTPBearerModel(bearerFormat=bearerFormat, description=description)
        self.scheme_name = scheme_name or self.__class__.__name__
        self.optional = optional

    async def __call__(self, request: Request) -> Optional[kb_auth.KBaseUser]:
        authorization: str = request.headers.get("Authorization")
        if not authorization:
            if self.optional:
                return None
            raise errors.MissingTokenError("Authorization header required")
        scheme, credentials = get_authorization_scheme_param(authorization)
        if not (scheme and credentials):
            raise errors.InvalidAuthHeaderError(
                f"Authorization header requires {_SCHEME} scheme followed by token"
            )
        if scheme.lower() != _SCHEME.lower():
            # don't put the received scheme in the error message, might be a token
            raise errors.InvalidAuthHeaderError(
                f"Authorization header requires {_SCHEME} scheme"
            )
        try:
            return await app_state.get_app_state(request).auth.get_user(credentials)
        except kb_auth.InvalidTokenError:
            raise errors.InvalidTokenError("Invalid authentication token")
        except Exception as e:
            # Catch any other errors and convert to authentication error
            raise errors.AuthenticationError(
                errors.ErrorType.AUTHENTICATION_FAILED,
                f"Authentication failed: {str(e)}",
            )
