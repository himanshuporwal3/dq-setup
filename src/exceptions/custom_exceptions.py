"""
Custom exceptions for the Data Quality application
"""


class DataQualityError(Exception):
    """Base exception for data quality application"""
    pass


class ConfigurationError(DataQualityError):
    """Raised when there's a configuration-related error"""
    pass


class ValidationError(DataQualityError):
    """Raised when data validation fails or encounters errors"""
    pass


class StorageError(DataQualityError):
    """Raised when storage operations fail"""
    pass


class ParsingError(DataQualityError):
    """Raised when YAML parsing fails"""
    pass


class ConnectionError(DataQualityError):
    """Raised when database or service connection fails"""
    pass


class AuthenticationError(DataQualityError):
    """Raised when authentication fails"""
    pass


class DataSourceError(DataQualityError):
    """Raised when data source operations fail"""
    pass


class ExpectationError(DataQualityError):
    """Raised when expectation creation or execution fails"""
    pass


class CheckpointError(DataQualityError):
    """Raised when checkpoint operations fail"""
    pass


class DocumentationError(DataQualityError):
    """Raised when documentation generation fails"""
    pass
