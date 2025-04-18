"""
Configuration settings for the Spark Manager API.
"""

import logging
import os
from functools import lru_cache

from pydantic import BaseModel, Field

APP_VERSION = "0.1.0"


class Settings(BaseModel):
    """
    Application settings.
    """

    app_name: str = "CDM Spark Cluster Manager API"
    app_description: str = "API for managing Spark clusters in Kubernetes"
    api_version: str = APP_VERSION
    log_level: str = Field(
        default=os.getenv("LOG_LEVEL", "INFO"), description="Logging level"
    )


@lru_cache()
def get_settings() -> Settings:
    """
    Get the application settings.

    Uses lru_cache to avoid loading the settings for every request.
    """
    return Settings()


def configure_logging():
    """Configure logging for the application."""
    settings = get_settings()
    if settings.log_level.upper() not in logging.getLevelNamesMapping():
        logging.warning(
            "Unrecognized log level '%s'. Falling back to 'INFO'.",
            settings.log_level,
        )
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
