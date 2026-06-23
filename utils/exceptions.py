"""
Custom exception classes used across the Weather ETL/ELT pipeline.

Defining specific exception types allows calling code to catch and
handle different failure modes (API issues vs. DB issues vs. data
quality issues) appropriately instead of relying on broad except
clauses.
"""


class PipelineError(Exception):
    """Base class for all pipeline-related exceptions."""


class APIConnectionError(PipelineError):
    """Raised when the pipeline cannot connect to the weather API."""


class APIResponseError(PipelineError):
    """Raised when the API returns an invalid or unexpected response."""


class DataValidationError(PipelineError):
    """Raised when extracted or transformed data fails validation checks."""


class TransformationError(PipelineError):
    """Raised when an error occurs during data transformation."""


class DatabaseConnectionError(PipelineError):
    """Raised when the pipeline cannot connect to the target database."""


class DataLoadError(PipelineError):
    """Raised when an error occurs while loading data into the database."""
