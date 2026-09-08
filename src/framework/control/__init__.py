"""Control table access and metadata ingestion."""

from .repository import ControlRepository
from .metadata_loader import MetadataLoader, TABLE_SPECS

__all__ = ["ControlRepository", "MetadataLoader", "TABLE_SPECS"]
