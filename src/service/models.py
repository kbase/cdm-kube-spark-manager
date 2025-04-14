"""
Pydantic models for the Spark Manager API.
"""

import re
from typing import Annotated

from pydantic import BaseModel, Field, field_validator


MIN_MEMORY = 1  # GB
MAX_MEMORY = 256  # GB

MAX_WORKER_COUNT = 25
MAX_WORKER_CORES = 64
MAX_MASTER_CORES = 64

DEFAULT_WORKER_MEMORY = "10G"
DEFAULT_MASTER_MEMORY = "10G"


class SparkClusterConfig(BaseModel):
    """Configuration for creating a new Spark cluster."""

    worker_count: Annotated[int, Field(description="Number of worker nodes", ge=1, le=MAX_WORKER_COUNT)] = 2
    worker_cores: Annotated[int, Field(description="CPU cores per worker", ge=1, le=MAX_WORKER_CORES)] = 10
    worker_memory: Annotated[str, Field(description="Memory per worker (format: {int}G)")] = DEFAULT_WORKER_MEMORY
    master_cores: Annotated[int, Field(description="CPU cores for master node", ge=1, le=MAX_MASTER_CORES)] = 10
    master_memory: Annotated[str, Field(description="Memory for master node (format: {int}G)")] = DEFAULT_MASTER_MEMORY

    @field_validator("worker_memory", "master_memory")
    @classmethod
    def validate_memory_format(cls, v: str) -> str:
        """Validate that memory values follow the format '{int}G' and are within limits."""
        if not re.match(r"^\d+G$", v):
            raise ValueError(f"Memory must be in format '{{int}}G', got '{v}'")
        
        # Extract the numeric value
        memory_value = int(v[:-1])
        
        if memory_value < MIN_MEMORY:
            raise ValueError(f"Memory must be at least {MIN_MEMORY}G")
        if memory_value > MAX_MEMORY:
            raise ValueError(f"Memory cannot exceed {MAX_MEMORY}G")

        return v


class SparkClusterCreateResponse(BaseModel):
    """Response model for cluster creation."""

    cluster_id: Annotated[str, Field(description="Unique identifier for the cluster")]
    master_url: Annotated[str, Field(description="URL to connect to the Spark master")]
    master_ui_url: Annotated[
        str, Field(description="URL to access the Spark master UI")
    ]


class ErrorResponse(BaseModel):
    """Standard error response model."""

    error: Annotated[int | None, Field(description="Error code")] = None
    error_type: Annotated[str | None, Field(description="Error type")] = None
    message: Annotated[str | None, Field(description="Error message")] = None

