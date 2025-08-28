"""
Data Quality YAML Configuration Parser
This module parses the YAML configuration file and creates Great Expectations objects
"""

import yaml
import os
from typing import Dict, List, Any, Optional
import great_expectations as gx
from great_expectations.expectations.expectation_configuration import ExpectationConfiguration
from great_expectations.core import ExpectationSuite
from great_expectations.checkpoint.checkpoint import Checkpoint

from ..utils.logger import setup_logger
from ..exceptions.custom_exceptions import ConfigurationError, ValidationError

logger = setup_logger(__name__)


class DQYamlParser:
    """Parser for Data Quality YAML configuration file"""
    
    def __init__(self, config_path: str):
        """
        Initialize the parser with configuration file path
        
        Args:
            config_path (str): Path to the YAML configuration file
        """
        self.config_path = config_path
        self.config = None
        self.context = None
        
    def load_config(self) -> Dict[str, Any]:
        """
        Load and parse the YAML configuration file
        
        Returns:
            Dict[str, Any]: Parsed configuration dictionary
            
        Raises:
            ConfigurationError: If config file cannot be loaded or parsed
        """
        try:
            if not os.path.exists(self.config_path):
                raise ConfigurationError(f"Configuration file not found: {self.config_path}")
                
            with open(self.config_path, 'r', encoding='utf-8') as file:
                # Replace environment variables in YAML
                yaml_content = file.read()
                for key, value in os.environ.items():
                    yaml_content = yaml_content.replace(f"${{{key}}}", value)
                    
                self.config = yaml.safe_load(yaml_content)
                
            logger.info(f"Successfully loaded configuration from {self.config_path}")
            return self.config
            
        except yaml.YAMLError as e:
            error_msg = f"Error parsing YAML configuration: {e}"
            logger.error(error_msg)
            raise ConfigurationError(error_msg)
        except Exception as e:
            error_msg = f"Unexpected error loading configuration: {e}"
            logger.error(error_msg)
            raise ConfigurationError(error_msg)
    
    def initialize_gx_context(self):
        """
        Initialize Great Expectations Data Context
        
        Returns:
            GX Data Context: Initialized GX Data Context
            
        Raises:
            ConfigurationError: If context initialization fails
        """
        try:
            # Initialize GX context
            self.context = gx.get_context()
            
            # Configure data sources if specified
            if 'data_sources' in self.config:
                self._configure_data_sources()
                
            # Configure data docs sites if specified
            if 'data_docs' in self.config:
                self._configure_data_docs()
                
            logger.info("Successfully initialized Great Expectations context")
            return self.context
            
        except Exception as e:
            error_msg = f"Error initializing GX context: {e}"
            logger.error(error_msg)
            raise ConfigurationError(error_msg)
    
    def _configure_data_sources(self) -> None:
        """Configure data sources in GX context"""
        try:
            for ds_key, ds_config in self.config['data_sources'].items():
                if ds_config['type'] == 'spark':
                    # Configure Spark data source for Databricks
                    datasource_config = {
                        "name": ds_config['name'],
                        "class_name": "Datasource",
                        "execution_engine": {
                            "class_name": "SparkDFExecutionEngine",
                            "spark_config": {
                                "spark.sql.adaptive.enabled": "true",
                                "spark.sql.adaptive.coalescePartitions.enabled": "true"
                            }
                        },
                        "data_connectors": {
                            "default_runtime_data_connector": {
                                "class_name": "RuntimeDataConnector",
                                "batch_identifiers": ["default_identifier_name"]
                            }
                        }
                    }
                    
                    # Add or update the datasource
                    self.context.add_datasource(**datasource_config)
                    
            logger.info("Data sources configured successfully")
            
        except Exception as e:
            error_msg = f"Error configuring data sources: {e}"
            logger.error(error_msg)
            raise ConfigurationError(error_msg)
    
    def _configure_data_docs(self) -> None:
        """Configure data docs sites in GX context"""
        try:
            data_docs_config = self.config.get('data_docs', {})
            
            # This will be handled by the context's default configuration
            # and can be customized further if needed
            logger.info("Data docs configuration loaded")
            
        except Exception as e:
            error_msg = f"Error configuring data docs: {e}"
            logger.error(error_msg)
            raise ConfigurationError(error_msg)
    
    def create_expectation_suites(self) -> Dict[str, Any]:
        """
        Create expectation suites for all configured tables
        
        Returns:
            Dict[str, Any]: Dictionary of table names to expectation suites
            
        Raises:
            ValidationError: If expectation suite creation fails
        """
        expectation_suites = {}
        
        try:
            tables_config = self.config.get('tables', [])
            
            for table_config in tables_config:
                table_name = table_config['name']
                expectations_config = table_config.get('expectations', [])
                
                # Create or update expectation suite
                suite_name = f"{table_name}_suite"
                
                try:
                    # Create/update expectation suite
                    suite = self.context.add_or_update_expectation_suite(
                        expectation_suite_name=suite_name
                    )
                    logger.info(f"Created/updated expectation suite: {suite_name}")
                    
                    # Add expectations to the suite
                    for exp_config in expectations_config:
                        expectation_name = exp_config['name']
                        expectation_params = exp_config.get('parameters', {})
                        
                        # Handle column-specific expectations
                        if 'column' in exp_config:
                            expectation_params['column'] = exp_config['column']
                        
                        # Create expectation configuration
                        expectation_configuration = ExpectationConfiguration(
                            expectation_type=expectation_name,
                            kwargs=expectation_params
                        )
                        
                        # Add expectation to suite
                        suite.add_expectation(expectation_configuration)
                        logger.debug(f"Added expectation {expectation_name} to suite {suite_name}")
                    
                    # Update the suite in context
                    self.context.add_or_update_expectation_suite(expectation_suite=suite)
                    expectation_suites[table_name] = suite
                    
                except Exception as e:
                    logger.error(f"Error creating expectation suite for {table_name}: {e}")
                    continue
                
                logger.info(f"Created expectation suite for table {table_name} with {len(expectations_config)} expectations")
            
            return expectation_suites
            
        except Exception as e:
            error_msg = f"Error creating expectation suites: {e}"
            logger.error(error_msg)
            raise ValidationError(error_msg)
    
    def create_checkpoints(self, expectation_suites: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create checkpoints for validation execution
        
        Args:
            expectation_suites (Dict[str, Any]): Dictionary of expectation suites
            
        Returns:
            Dict[str, Any]: Dictionary of checkpoint names to checkpoint objects
            
        Raises:
            ValidationError: If checkpoint creation fails
        """
        checkpoints = {}
        
        try:
            checkpoints_config = self.config.get('checkpoints', [])
            
            for checkpoint_config in checkpoints_config:
                checkpoint_name = checkpoint_config['name']
                table_name = checkpoint_config['table']
                
                if table_name not in expectation_suites:
                    logger.warning(f"No expectation suite found for table {table_name}, skipping checkpoint {checkpoint_name}")
                    continue
                
                # Create checkpoint using modern API
                try:
                    checkpoint = self.context.add_or_update_checkpoint(
                        name=checkpoint_name,
                        validations=[
                            {
                                "expectation_suite_name": f"{table_name}_suite",
                                # We'll set batch_request dynamically during validation
                            }
                        ],
                        action_list=[
                            {
                                "name": "store_validation_result",
                                "action": {
                                    "class_name": "StoreValidationResultAction"
                                }
                            },
                            {
                                "name": "update_data_docs",
                                "action": {
                                    "class_name": "UpdateDataDocsAction"
                                }
                            }
                        ]
                    )
                    
                    checkpoints[checkpoint_name] = checkpoint
                    
                except Exception as e:
                    logger.error(f"Error creating checkpoint for {table_name}: {e}")
                    continue
                
                logger.info(f"Created checkpoint: {checkpoint_name} for table: {table_name}")
            
            return checkpoints
            
        except Exception as e:
            error_msg = f"Error creating checkpoints: {e}"
            logger.error(error_msg)
            raise ValidationError(error_msg)
    
    def get_table_configs(self) -> List[Dict[str, Any]]:
        """
        Get table configurations from parsed YAML
        
        Returns:
            List[Dict[str, Any]]: List of table configurations
        """
        return self.config.get('tables', [])
    
    def get_storage_config(self) -> Dict[str, Any]:
        """
        Get storage configuration from parsed YAML
        
        Returns:
            Dict[str, Any]: Storage configuration dictionary
        """
        return self.config.get('storage', {})
    
    def get_logging_config(self) -> Dict[str, Any]:
        """
        Get logging configuration from parsed YAML
        
        Returns:
            Dict[str, Any]: Logging configuration dictionary
        """
        return self.config.get('logging', {})
