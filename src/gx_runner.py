"""
Great Expectations Runner
Main execution module for running data quality validations
"""

import os
import sys
from datetime import datetime
from typing import Dict, List, Any, Optional
from pyspark.sql import SparkSession
import great_expectations as gx
from great_expectations.core import ExpectationSuite
from great_expectations.checkpoint.checkpoint import Checkpoint
from great_expectations.core.run_identifier import RunIdentifier

from .config.dq_yaml_parser import DQYamlParser
from .storage.adls_client import ADLSClient
from .docs.doc_generator import DocumentationGenerator
from .utils.logger import setup_logger
from .exceptions.custom_exceptions import ValidationError, StorageError, ConfigurationError

logger = setup_logger(__name__)


class GXRunner:
    """Main class for executing Great Expectations data quality validations"""
    
    def __init__(self, config_path: str):
        """
        Initialize the GX Runner
        
        Args:
            config_path (str): Path to the YAML configuration file
        """
        self.config_path = config_path
        self.parser = None
        self.context = None
        self.spark = None
        self.adls_client = None
        self.doc_generator = None
        self.validation_results = []
        
    def initialize(self) -> None:
        """
        Initialize all components required for validation
        
        Raises:
            ConfigurationError: If initialization fails
        """
        try:
            logger.info("Initializing GX Runner...")
            
            # Initialize YAML parser
            self.parser = DQYamlParser(self.config_path)
            config = self.parser.load_config()
            
            # Initialize GX context
            self.context = self.parser.initialize_gx_context()
            
            # Initialize Spark session
            self._initialize_spark()
            
            # Initialize ADLS client
            storage_config = self.parser.get_storage_config()
            if 'adls_gen2' in storage_config:
                self.adls_client = ADLSClient(storage_config['adls_gen2'])
                
            # Initialize documentation generator
            self.doc_generator = DocumentationGenerator(
                storage_config.get('adls_gen2', {}),
                self.adls_client
            )
            
            logger.info("GX Runner initialization completed successfully")
            
        except Exception as e:
            error_msg = f"Error during GX Runner initialization: {e}"
            logger.error(error_msg)
            raise ConfigurationError(error_msg)
    
    def _initialize_spark(self) -> None:
        """Initialize Spark session for Databricks"""
        try:
            # In Databricks, Spark session is already available
            self.spark = SparkSession.getActiveSession()
            
            if self.spark is None:
                # Create new Spark session if not in Databricks environment
                self.spark = SparkSession.builder \
                    .appName("DataQualityValidation") \
                    .config("spark.sql.adaptive.enabled", "true") \
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                    .getOrCreate()
                    
            logger.info("Spark session initialized successfully")
            
        except Exception as e:
            error_msg = f"Error initializing Spark session: {e}"
            logger.error(error_msg)
            raise ConfigurationError(error_msg)
    
    def setup_expectations(self) -> Dict[str, Any]:
        """
        Setup expectation suites for all configured tables
        
        Returns:
            Dict[str, Any]: Dictionary of table names to expectation suites
            
        Raises:
            ValidationError: If expectation setup fails
        """
        try:
            logger.info("Setting up expectation suites...")
            
            expectation_suites = self.parser.create_expectation_suites()
            
            logger.info(f"Created {len(expectation_suites)} expectation suites")
            return expectation_suites
            
        except Exception as e:
            error_msg = f"Error setting up expectations: {e}"
            logger.error(error_msg)
            raise ValidationError(error_msg)
    
    def setup_checkpoints(self, expectation_suites: Dict[str, Any]) -> Dict[str, Any]:
        """
        Setup checkpoints for validation execution
        
        Args:
            expectation_suites (Dict[str, Any]): Expectation suites
            
        Returns:
            Dict[str, Any]: Dictionary of checkpoint names to checkpoints
            
        Raises:
            ValidationError: If checkpoint setup fails
        """
        try:
            logger.info("Setting up checkpoints...")
            
            checkpoints = self.parser.create_checkpoints(expectation_suites)
            
            logger.info(f"Created {len(checkpoints)} checkpoints")
            return checkpoints
            
        except Exception as e:
            error_msg = f"Error setting up checkpoints: {e}"
            logger.error(error_msg)
            raise ValidationError(error_msg)
    
    def run_validation(self, table_name: str, checkpoint: Any) -> Dict[str, Any]:
        """
        Run validation for a specific table
        
        Args:
            table_name (str): Name of the table to validate
            checkpoint (Any): Checkpoint to execute
            
        Returns:
            Dict[str, Any]: Validation results
            
        Raises:
            ValidationError: If validation execution fails
        """
        try:
            logger.info(f"Running validation for table: {table_name}")
            
            # Execute checkpoint with modern API
            # For now, we'll run without specific batch request as GX 1.5+ handles this differently
            results = checkpoint.run()
            
            # Process results
            validation_result = {
                "table_name": table_name,
                "run_id": str(run_identifier),
                "success": results.success,
                "timestamp": datetime.now().isoformat(),
                "statistics": results.run_results,
                "details": self._extract_validation_details(results)
            }
            
            self.validation_results.append(validation_result)
            
            logger.info(f"Validation completed for table {table_name}. Success: {results.success}")
            return validation_result
            
        except Exception as e:
            error_msg = f"Error running validation for table {table_name}: {e}"
            logger.error(error_msg)
            raise ValidationError(error_msg)
    
    def _extract_validation_details(self, results) -> Dict[str, Any]:
        """
        Extract detailed validation results
        
        Args:
            results: GX validation results object
            
        Returns:
            Dict[str, Any]: Processed validation details
        """
        try:
            details = {
                "total_expectations": 0,
                "successful_expectations": 0,
                "failed_expectations": 0,
                "expectation_results": []
            }
            
            # Extract results from the run results
            for run_id, run_result in results.run_results.items():
                validation_result = run_result['validation_result']
                
                details["total_expectations"] = len(validation_result.results)
                details["successful_expectations"] = validation_result.statistics['successful_expectations']
                details["failed_expectations"] = validation_result.statistics['unsuccessful_expectations']
                
                # Extract individual expectation results
                for result in validation_result.results:
                    expectation_detail = {
                        "expectation_type": result.expectation_config.expectation_type,
                        "success": result.success,
                        "kwargs": result.expectation_config.kwargs,
                        "result": result.result if hasattr(result, 'result') else None
                    }
                    details["expectation_results"].append(expectation_detail)
            
            return details
            
        except Exception as e:
            logger.warning(f"Error extracting validation details: {e}")
            return {"error": str(e)}
    
    def run_all_validations(self) -> List[Dict[str, Any]]:
        """
        Run validations for all configured tables
        
        Returns:
            List[Dict[str, Any]]: List of all validation results
            
        Raises:
            ValidationError: If any validation fails critically
        """
        try:
            logger.info("Starting validation for all configured tables...")
            
            # Setup expectations and checkpoints
            expectation_suites = self.setup_expectations()
            checkpoints = self.setup_checkpoints(expectation_suites)
            
            # Run validations
            all_results = []
            table_configs = self.parser.get_table_configs()
            
            for table_config in table_configs:
                table_name = table_config['name']
                checkpoint_name = f"{table_name}_checkpoint"
                
                if checkpoint_name in checkpoints:
                    try:
                        result = self.run_validation(table_name, checkpoints[checkpoint_name])
                        all_results.append(result)
                    except Exception as e:
                        logger.error(f"Validation failed for table {table_name}: {e}")
                        # Continue with other tables even if one fails
                        error_result = {
                            "table_name": table_name,
                            "success": False,
                            "error": str(e),
                            "timestamp": datetime.now().isoformat()
                        }
                        all_results.append(error_result)
                else:
                    logger.warning(f"No checkpoint found for table {table_name}")
            
            logger.info(f"Completed validation for {len(all_results)} tables")
            return all_results
            
        except Exception as e:
            error_msg = f"Error running all validations: {e}"
            logger.error(error_msg)
            raise ValidationError(error_msg)
    
    def store_results(self, results: List[Dict[str, Any]]) -> None:
        """
        Store validation results to ADLS Gen2
        
        Args:
            results (List[Dict[str, Any]]): Validation results to store
            
        Raises:
            StorageError: If storage operation fails
        """
        try:
            if not self.adls_client:
                logger.warning("ADLS client not initialized, skipping result storage")
                return
                
            logger.info("Storing validation results to ADLS Gen2...")
            
            # Store individual results
            for result in results:
                result_path = f"validation_results/{result['table_name']}/{result['timestamp']}.json"
                self.adls_client.upload_json(result, result_path)
                
            # Store summary results
            summary = self._create_summary(results)
            summary_path = f"validation_results/summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            self.adls_client.upload_json(summary, summary_path)
            
            logger.info("Validation results stored successfully")
            
        except Exception as e:
            error_msg = f"Error storing validation results: {e}"
            logger.error(error_msg)
            raise StorageError(error_msg)
    
    def generate_documentation(self, results: List[Dict[str, Any]]) -> None:
        """
        Generate and store documentation for validation results
        
        Args:
            results (List[Dict[str, Any]]): Validation results
            
        Raises:
            StorageError: If documentation generation fails
        """
        try:
            if not self.doc_generator:
                logger.warning("Documentation generator not initialized, skipping doc generation")
                return
                
            logger.info("Generating validation documentation...")
            
            self.doc_generator.generate_html_report(results)
            self.doc_generator.generate_summary_report(results)
            
            logger.info("Validation documentation generated successfully")
            
        except Exception as e:
            error_msg = f"Error generating documentation: {e}"
            logger.error(error_msg)
            raise StorageError(error_msg)
    
    def _create_summary(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Create summary of validation results
        
        Args:
            results (List[Dict[str, Any]]): Individual validation results
            
        Returns:
            Dict[str, Any]: Summary of all results
        """
        total_tables = len(results)
        successful_tables = sum(1 for r in results if r.get('success', False))
        failed_tables = total_tables - successful_tables
        
        summary = {
            "validation_summary": {
                "timestamp": datetime.now().isoformat(),
                "total_tables": total_tables,
                "successful_tables": successful_tables,
                "failed_tables": failed_tables,
                "success_rate": (successful_tables / total_tables * 100) if total_tables > 0 else 0
            },
            "table_results": []
        }
        
        for result in results:
            table_summary = {
                "table_name": result['table_name'],
                "success": result.get('success', False),
                "timestamp": result['timestamp']
            }
            
            if 'details' in result and isinstance(result['details'], dict):
                table_summary.update({
                    "total_expectations": result['details'].get('total_expectations', 0),
                    "successful_expectations": result['details'].get('successful_expectations', 0),
                    "failed_expectations": result['details'].get('failed_expectations', 0)
                })
            
            summary["table_results"].append(table_summary)
        
        return summary
    
    def cleanup(self) -> None:
        """Clean up resources"""
        try:
            if self.spark:
                # In Databricks, don't stop the shared Spark session
                logger.info("Spark session cleanup completed")
                
            logger.info("GX Runner cleanup completed")
            
        except Exception as e:
            logger.warning(f"Error during cleanup: {e}")
