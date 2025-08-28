"""
Main entry point for the Data Quality Application
Orchestrates the complete data validation workflow using Great Expectations
"""

import os
import sys
from datetime import datetime
from typing import List, Dict, Any

# Add src to Python path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.gx_runner import GXRunner
from src.utils.logger import setup_databricks_logger
from src.exceptions.custom_exceptions import (
    ConfigurationError, ValidationError, StorageError
)

# Setup logger
logger = setup_databricks_logger(__name__)


def main():
    """
    Main function to execute data quality validation workflow
    """
    logger.info("=" * 80)
    logger.info("Starting Data Quality Validation Application")
    logger.info("=" * 80)
    
    # Configuration file path
    config_path = os.path.join(os.path.dirname(__file__), 'config', 'dq_config.yaml')
    
    gx_runner = None
    validation_results = []
    
    try:
        # Validate environment variables
        _validate_environment()
        
        # Initialize GX Runner
        logger.info("Initializing GX Runner...")
        gx_runner = GXRunner(config_path)
        gx_runner.initialize()
        
        # Execute validation workflow
        logger.info("Starting validation workflow...")
        validation_results = gx_runner.run_all_validations()
        
        # Store results to ADLS Gen2
        logger.info("Storing validation results...")
        gx_runner.store_results(validation_results)
        
        # Generate documentation
        logger.info("Generating validation documentation...")
        gx_runner.generate_documentation(validation_results)
        
        # Print summary
        _print_summary(validation_results)
        
        logger.info("Data Quality Validation completed successfully!")
        
    except ConfigurationError as e:
        logger.error(f"Configuration error: {e}")
        return 1
        
    except ValidationError as e:
        logger.error(f"Validation error: {e}")
        return 1
        
    except StorageError as e:
        logger.error(f"Storage error: {e}")
        # Don't fail the entire process for storage errors
        logger.warning("Continuing despite storage error...")
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1
        
    finally:
        # Cleanup
        if gx_runner:
            gx_runner.cleanup()
    
    return 0


def _validate_environment():
    """
    Validate required environment variables
    
    Raises:
        ConfigurationError: If required environment variables are missing
    """
    required_env_vars = [
        'DATABRICKS_TOKEN',
        'DATABRICKS_SERVER_HOSTNAME', 
        'DATABRICKS_HTTP_PATH',
        'ADLS_ACCOUNT_NAME',
        'ADLS_ACCOUNT_KEY'
    ]
    
    missing_vars = []
    for var in required_env_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
        logger.error(error_msg)
        raise ConfigurationError(error_msg)
    
    logger.info("Environment validation passed")


def _print_summary(validation_results: List[Dict[str, Any]]):
    """
    Print summary of validation results
    
    Args:
        validation_results (List[Dict[str, Any]]): Validation results to summarize
    """
    total_tables = len(validation_results)
    successful_tables = sum(1 for r in validation_results if r.get('success', False))
    failed_tables = total_tables - successful_tables
    
    logger.info("=" * 60)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total Tables Validated: {total_tables}")
    logger.info(f"Successful Validations: {successful_tables}")
    logger.info(f"Failed Validations: {failed_tables}")
    logger.info(f"Success Rate: {(successful_tables/total_tables*100):.1f}%" if total_tables > 0 else "0%")
    logger.info("=" * 60)
    
    # Table-level details
    for result in validation_results:
        table_name = result['table_name']
        success = result.get('success', False)
        status = "✓ PASS" if success else "✗ FAIL"
        
        logger.info(f"{status} - {table_name}")
        
        if 'details' in result and isinstance(result['details'], dict):
            details = result['details']
            total_exp = details.get('total_expectations', 0)
            success_exp = details.get('successful_expectations', 0)
            logger.info(f"    Expectations: {success_exp}/{total_exp} passed")
        
        if 'error' in result:
            logger.info(f"    Error: {result['error']}")
    
    logger.info("=" * 60)


def run_specific_table(table_name: str):
    """
    Run validation for a specific table only
    
    Args:
        table_name (str): Name of the table to validate
        
    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    logger.info(f"Running validation for specific table: {table_name}")
    
    config_path = os.path.join(os.path.dirname(__file__), 'config', 'dq_config.yaml')
    
    try:
        _validate_environment()
        
        gx_runner = GXRunner(config_path)
        gx_runner.initialize()
        
        # Setup expectations and checkpoints
        expectation_suites = gx_runner.setup_expectations()
        checkpoints = gx_runner.setup_checkpoints(expectation_suites)
        
        # Find checkpoint for the specific table
        checkpoint_name = f"{table_name}_checkpoint"
        
        if checkpoint_name not in checkpoints:
            logger.error(f"No checkpoint found for table: {table_name}")
            return 1
        
        # Run validation for specific table
        result = gx_runner.run_validation(table_name, checkpoints[checkpoint_name])
        
        # Store and document results
        gx_runner.store_results([result])
        gx_runner.generate_documentation([result])
        
        _print_summary([result])
        
        logger.info(f"Validation completed for table: {table_name}")
        return 0
        
    except Exception as e:
        logger.error(f"Error validating table {table_name}: {e}")
        return 1


if __name__ == "__main__":
    # Check for command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "--table" and len(sys.argv) > 2:
            # Run validation for specific table
            table_name = sys.argv[2]
            exit_code = run_specific_table(table_name)
        else:
            logger.error("Usage: python main.py [--table TABLE_NAME]")
            exit_code = 1
    else:
        # Run validation for all tables
        exit_code = main()
    
    sys.exit(exit_code)
