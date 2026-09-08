"""Framework exception hierarchy.

Every failure that the framework raises deliberately derives from FrameworkError, so
the layer runners can distinguish "the framework rejected this configuration" from
"Spark blew up" and audit them differently.
"""


class FrameworkError(Exception):
    """Base class for every error raised deliberately by the framework."""


class ConfigurationError(FrameworkError):
    """Framework level configuration (conf/framework.<env>.yml) is missing or invalid."""


class ControlTableError(FrameworkError):
    """A control table row is missing, ambiguous, or internally inconsistent."""


class MetadataValidationError(FrameworkError):
    """A YAML metadata file failed validation before it could be loaded."""


class IngestionError(FrameworkError):
    """Auto Loader ingestion failed."""


class DataQualityError(FrameworkError):
    """A fail-severity DQ rule fired, or the quarantine threshold was breached."""


class TransformationError(FrameworkError):
    """A gold layer transformation could not be resolved or executed."""


class UnsupportedLoadTypeError(FrameworkError):
    """The control table asked for a load type the writer does not implement."""
