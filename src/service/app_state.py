"""
Application state information and retrieval functions.
"""

import os

from fastapi.requests import Request

from src.service.kb_auth import KBaseAuth

# Singleton application state
_APP_STATE = None


class AppState:
    """
    The application state, containing singletons for application components like the
    KBase auth client.
    """

    def __init__(self):
        """Create the application state."""
        # Initialize auth with KBase auth URL and admin roles from environment variables
        auth_url = os.environ.get(
            "KBASE_AUTH_URL", "https://ci.kbase.us/services/auth/"
        )
        admin_roles = os.environ.get("KBASE_ADMIN_ROLES", "KBASE_ADMIN").split(",")
        self.auth = KBaseAuth(auth_url, admin_roles)


def get_app_state(request: Request = None) -> AppState:
    """
    Get the application state.

    Args:
        request: The FastAPI request, not used but included for API compatibility.

    Returns:
        The application state.
    """
    global _APP_STATE
    if not _APP_STATE:
        _APP_STATE = AppState()
    return _APP_STATE
