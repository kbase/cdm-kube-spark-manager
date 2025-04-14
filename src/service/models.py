"""
Pydantic models for the Spark Manager API.
"""

import re

from pydantic import BaseModel, Field, field_validator


class SparkClusterConfig(BaseModel):
    """Configuration for creating a new Spark cluster."""

    worker_count: int = Field(default=2, description="Number of worker nodes")
    worker_cores: int = Field(default=10, description="CPU cores per worker")
    worker_memory: str = Field(
        default="10G", description="Memory per worker (format: {int}G)"
    )
    master_cores: int = Field(default=10, description="CPU cores for master node")
    master_memory: str = Field(
        default="10G", description="Memory for master node (format: {int}G)"
    )

    @field_validator("worker_memory", "master_memory")
    @classmethod
    def validate_memory_format(cls, v: str) -> str:
        """Validate that memory values follow the format '{int}G'."""
        if not re.match(r"^\d+G$", v):
            raise ValueError(f"Memory must be in format '{{int}}G', got '{v}'")
        return v


class SparkClusterCreateResponse(BaseModel):
    """Response model for cluster creation."""

    cluster_id: str = Field(..., description="Unique identifier for the cluster")
    master_url: str = Field(..., description="URL to connect to the Spark master")
    master_ui_url: str = Field(..., description="URL to access the Spark master UI")


class ErrorResponse(BaseModel):
    """Standard error response model."""

    error: int | None = Field(None, description="Error code")
    error_type: str | None = Field(None, description="Error type")
    message: str | None = Field(None, description="Error message")