"""
Configuration settings for the Spark Manager API.
"""

import logging
import os
from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings

APP_VERSION = "0.0.1"


class Settings(BaseSettings):
    """
    Application settings.

    Values will be loaded from environment variables or use the provided defaults.
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
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
