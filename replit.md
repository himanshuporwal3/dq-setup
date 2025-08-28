# Data Quality Application with Great Expectations

## Overview

This is a comprehensive data quality validation application built on Great Expectations for validating Databricks Delta tables. The system provides configuration-driven validation workflows with results stored in Azure Data Lake Storage Gen2. It features automated HTML report generation, comprehensive logging, and modular architecture designed for production environments. The application orchestrates complete data validation workflows using YAML configuration files and integrates seamlessly with Spark execution engines for scalable data processing.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Core Architecture Pattern
The application follows a modular, service-oriented architecture with clear separation of concerns:

- **Main Orchestrator** (`main.py`): Entry point that coordinates the entire validation workflow
- **GX Runner** (`gx_runner.py`): Core execution engine that manages Great Expectations operations
- **Configuration Parser** (`dq_yaml_parser.py`): YAML-based configuration management system
- **Documentation Generator** (`doc_generator.py`): HTML report generation and formatting
- **Storage Client** (`adls_client.py`): Azure Data Lake Storage Gen2 integration
- **Utilities and Exceptions**: Centralized logging and custom error handling

### Data Processing Framework
- **Great Expectations Integration**: Uses the latest GX core library (v1.5+) for data validation
- **Spark Execution Engine**: Leverages PySpark for distributed data processing and Delta table operations
- **Configuration-Driven Approach**: YAML-based configuration system for defining validation rules and data sources

### Storage Architecture
- **Azure Data Lake Storage Gen2**: Primary storage for validation results and documentation
- **Blob Storage Integration**: Uses Azure Blob Service Client for file operations
- **Hierarchical Storage Structure**: Organized storage paths for results and documentation

### Error Handling Strategy
- **Custom Exception Hierarchy**: Specialized exceptions for different failure modes (ConfigurationError, ValidationError, StorageError, etc.)
- **Comprehensive Logging**: Structured logging system with configurable levels and output formats
- **Graceful Degradation**: Error recovery mechanisms for non-critical failures

### Documentation System
- **Automated HTML Report Generation**: Creates formatted validation reports with results and metrics
- **Real-time Documentation**: Documentation generated and uploaded during validation execution
- **Timestamped Artifacts**: All outputs include timestamps for version control and tracking

## External Dependencies

### Cloud Services
- **Azure Data Lake Storage Gen2**: Primary storage backend for validation results and documentation
- **Databricks**: Execution environment for Spark-based data processing and Delta table operations

### Python Libraries
- **Great Expectations**: Core data validation framework and expectation suite management
- **PySpark**: Distributed data processing and Spark SQL operations
- **Azure Storage Blob**: Azure SDK for blob storage operations and file management
- **PyYAML**: YAML configuration file parsing and environment variable substitution

### Data Sources
- **Delta Tables**: Primary data format for validation targets
- **Spark DataFrames**: In-memory data processing and validation execution

### Authentication
- **Azure Storage Account Keys**: Authentication method for ADLS Gen2 access
- **Environment Variables**: Configuration management for sensitive credentials and connection strings