"""Project-specific exception hierarchy."""


class StreamCommerceError(Exception):
    """Base class for all expected pipeline errors."""


class ConfigError(StreamCommerceError):
    """Invalid or missing configuration."""


class SchemaValidationError(StreamCommerceError):
    """A file or record does not match the expected schema."""


class IngestionError(StreamCommerceError):
    """A source file could not be ingested."""


class DataQualityError(StreamCommerceError):
    """A data quality rule definition or execution is invalid."""


class MetadataError(StreamCommerceError):
    """The metadata store is unavailable or returned an error."""


class MissingDependencyError(StreamCommerceError):
    """An optional dependency required for the requested feature is not installed."""


class SnowflakeConfigError(StreamCommerceError):
    """Snowflake integration is enabled but not configured correctly."""
