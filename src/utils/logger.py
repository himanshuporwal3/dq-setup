"""
Logging utilities for the data quality application
"""

import logging
import os
import sys
from datetime import datetime
from typing import Optional


def setup_logger(
    name: str,
    level: str = "INFO",
    log_file: Optional[str] = None,
    format_string: Optional[str] = None
) -> logging.Logger:
    """
    Setup and configure logger for the application
    
    Args:
        name (str): Logger name
        level (str): Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file (Optional[str]): Log file path (optional)
        format_string (Optional[str]): Custom log format (optional)
        
    Returns:
        logging.Logger: Configured logger instance
    """
    
    # Create logger
    logger = logging.getLogger(name)
    
    # Avoid adding multiple handlers to the same logger
    if logger.handlers:
        return logger
    
    # Set logging level
    log_level = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(log_level)
    
    # Default format
    if not format_string:
        format_string = '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    
    formatter = logging.Formatter(format_string)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (if log file is specified)
    if log_file:
        try:
            # Ensure log directory exists
            log_dir = os.path.dirname(log_file)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir, exist_ok=True)
            
            file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
            file_handler.setLevel(log_level)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            
        except Exception as e:
            logger.warning(f"Could not setup file handler for {log_file}: {e}")
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get existing logger instance
    
    Args:
        name (str): Logger name
        
    Returns:
        logging.Logger: Logger instance
    """
    return logging.getLogger(name)


def log_function_entry_exit(logger: logging.Logger):
    """
    Decorator to log function entry and exit
    
    Args:
        logger (logging.Logger): Logger instance to use
        
    Returns:
        Decorator function
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            logger.debug(f"Entering function: {func.__name__}")
            try:
                result = func(*args, **kwargs)
                logger.debug(f"Exiting function: {func.__name__}")
                return result
            except Exception as e:
                logger.error(f"Error in function {func.__name__}: {e}")
                raise
        return wrapper
    return decorator


def log_execution_time(logger: logging.Logger):
    """
    Decorator to log function execution time
    
    Args:
        logger (logging.Logger): Logger instance to use
        
    Returns:
        Decorator function
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = datetime.now()
            logger.debug(f"Starting execution of {func.__name__}")
            
            try:
                result = func(*args, **kwargs)
                end_time = datetime.now()
                execution_time = (end_time - start_time).total_seconds()
                logger.info(f"Function {func.__name__} completed in {execution_time:.2f} seconds")
                return result
                
            except Exception as e:
                end_time = datetime.now()
                execution_time = (end_time - start_time).total_seconds()
                logger.error(f"Function {func.__name__} failed after {execution_time:.2f} seconds: {e}")
                raise
                
        return wrapper
    return decorator


class ContextLogger:
    """Context manager for logging with additional context"""
    
    def __init__(self, logger: logging.Logger, context: str, level: str = "INFO"):
        """
        Initialize context logger
        
        Args:
            logger (logging.Logger): Base logger
            context (str): Context description
            level (str): Logging level for context messages
        """
        self.logger = logger
        self.context = context
        self.level = getattr(logging, level.upper(), logging.INFO)
        self.start_time = None
    
    def __enter__(self):
        """Enter context"""
        self.start_time = datetime.now()
        self.logger.log(self.level, f"Starting context: {self.context}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context"""
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()
        
        if exc_type is None:
            self.logger.log(self.level, f"Completed context: {self.context} in {duration:.2f} seconds")
        else:
            self.logger.error(f"Context failed: {self.context} after {duration:.2f} seconds - {exc_val}")
        
        return False  # Don't suppress exceptions


def setup_databricks_logger(name: str, level: str = "INFO") -> logging.Logger:
    """
    Setup logger specifically configured for Databricks environment
    
    Args:
        name (str): Logger name
        level (str): Logging level
        
    Returns:
        logging.Logger: Configured logger for Databricks
    """
    
    # In Databricks, use simpler format for better readability
    format_string = '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    
    # Use /tmp for log files in Databricks
    log_file = f"/tmp/dq_validation_{datetime.now().strftime('%Y%m%d')}.log"
    
    return setup_logger(
        name=name,
        level=level,
        log_file=log_file,
        format_string=format_string
    )
