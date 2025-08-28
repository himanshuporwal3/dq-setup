"""
Azure Data Lake Storage Gen2 Client
Handles storage operations for validation results and documentation
"""

import json
import os
from datetime import datetime
from typing import Dict, Any, List, Optional, Union
from io import StringIO

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core.exceptions import AzureError, ResourceNotFoundError

from ..utils.logger import setup_logger
from ..exceptions.custom_exceptions import StorageError, ConfigurationError

logger = setup_logger(__name__)


class ADLSClient:
    """Client for Azure Data Lake Storage Gen2 operations"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize ADLS Gen2 client
        
        Args:
            config (Dict[str, Any]): ADLS configuration containing account details
            
        Raises:
            ConfigurationError: If required configuration is missing
        """
        self.account_name = config.get('account_name')
        self.account_key = config.get('account_key')
        self.container_name = config.get('container_name', 'data-quality-results')
        self.results_path = config.get('results_path', 'validation_results')
        self.docs_path = config.get('docs_path', 'data_docs')
        
        if not self.account_name:
            raise ConfigurationError("ADLS account name is required")
        
        if not self.account_key:
            raise ConfigurationError("ADLS account key is required")
        
        # Initialize blob service client
        try:
            account_url = f"https://{self.account_name}.blob.core.windows.net"
            self.blob_service_client = BlobServiceClient(
                account_url=account_url,
                credential=self.account_key
            )
            
            # Ensure container exists
            self._ensure_container_exists()
            
            logger.info(f"ADLS Gen2 client initialized for account: {self.account_name}")
            
        except Exception as e:
            error_msg = f"Failed to initialize ADLS Gen2 client: {e}"
            logger.error(error_msg)
            raise ConfigurationError(error_msg)
    
    def _ensure_container_exists(self) -> None:
        """Ensure the container exists, create if it doesn't"""
        try:
            container_client = self.blob_service_client.get_container_client(self.container_name)
            
            # Try to get container properties (this will fail if container doesn't exist)
            try:
                container_client.get_container_properties()
                logger.info(f"Container '{self.container_name}' exists")
            except ResourceNotFoundError:
                # Create container if it doesn't exist
                container_client.create_container()
                logger.info(f"Created container: {self.container_name}")
                
        except Exception as e:
            error_msg = f"Error ensuring container exists: {e}"
            logger.error(error_msg)
            raise StorageError(error_msg)
    
    def upload_json(self, data: Union[Dict[str, Any], List[Dict[str, Any]]], blob_path: str) -> str:
        """
        Upload JSON data to ADLS Gen2
        
        Args:
            data (Union[Dict, List[Dict]]): Data to upload as JSON
            blob_path (str): Blob path for the uploaded file
            
        Returns:
            str: Full blob path of uploaded file
            
        Raises:
            StorageError: If upload operation fails
        """
        try:
            # Convert data to JSON string
            json_content = json.dumps(data, indent=2, default=str, ensure_ascii=False)
            
            # Get blob client
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_path
            )
            
            # Upload data
            blob_client.upload_blob(
                json_content,
                overwrite=True,
                content_type='application/json',
                encoding='utf-8'
            )
            
            full_path = f"{self.container_name}/{blob_path}"
            logger.info(f"Successfully uploaded JSON to: {full_path}")
            
            return full_path
            
        except Exception as e:
            error_msg = f"Error uploading JSON to {blob_path}: {e}"
            logger.error(error_msg)
            raise StorageError(error_msg)
    
    def upload_text(self, content: str, blob_path: str, content_type: str = 'text/plain') -> str:
        """
        Upload text content to ADLS Gen2
        
        Args:
            content (str): Text content to upload
            blob_path (str): Blob path for the uploaded file
            content_type (str): MIME type for the content
            
        Returns:
            str: Full blob path of uploaded file
            
        Raises:
            StorageError: If upload operation fails
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_path
            )
            
            blob_client.upload_blob(
                content,
                overwrite=True,
                content_type=content_type,
                encoding='utf-8'
            )
            
            full_path = f"{self.container_name}/{blob_path}"
            logger.info(f"Successfully uploaded text to: {full_path}")
            
            return full_path
            
        except Exception as e:
            error_msg = f"Error uploading text to {blob_path}: {e}"
            logger.error(error_msg)
            raise StorageError(error_msg)
    
    def upload_html(self, html_content: str, blob_path: str) -> str:
        """
        Upload HTML content to ADLS Gen2
        
        Args:
            html_content (str): HTML content to upload
            blob_path (str): Blob path for the uploaded file
            
        Returns:
            str: Full blob path of uploaded file
            
        Raises:
            StorageError: If upload operation fails
        """
        return self.upload_text(html_content, blob_path, 'text/html')
    
    def download_json(self, blob_path: str) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Download and parse JSON data from ADLS Gen2
        
        Args:
            blob_path (str): Blob path of the JSON file
            
        Returns:
            Union[Dict, List[Dict]]: Parsed JSON data
            
        Raises:
            StorageError: If download or parsing fails
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_path
            )
            
            # Download blob content
            blob_content = blob_client.download_blob().readall()
            content_str = blob_content.decode('utf-8')
            
            # Parse JSON
            data = json.loads(content_str)
            
            logger.info(f"Successfully downloaded JSON from: {blob_path}")
            return data
            
        except ResourceNotFoundError:
            error_msg = f"JSON file not found: {blob_path}"
            logger.error(error_msg)
            raise StorageError(error_msg)
        except json.JSONDecodeError as e:
            error_msg = f"Error parsing JSON from {blob_path}: {e}"
            logger.error(error_msg)
            raise StorageError(error_msg)
        except Exception as e:
            error_msg = f"Error downloading JSON from {blob_path}: {e}"
            logger.error(error_msg)
            raise StorageError(error_msg)
    
    def list_blobs(self, path_prefix: str = "") -> List[str]:
        """
        List blobs with specified path prefix
        
        Args:
            path_prefix (str): Path prefix to filter blobs
            
        Returns:
            List[str]: List of blob paths
            
        Raises:
            StorageError: If listing operation fails
        """
        try:
            container_client = self.blob_service_client.get_container_client(self.container_name)
            
            blob_list = []
            for blob in container_client.list_blobs(name_starts_with=path_prefix):
                blob_list.append(blob.name)
            
            logger.info(f"Listed {len(blob_list)} blobs with prefix: {path_prefix}")
            return blob_list
            
        except Exception as e:
            error_msg = f"Error listing blobs with prefix {path_prefix}: {e}"
            logger.error(error_msg)
            raise StorageError(error_msg)
    
    def delete_blob(self, blob_path: str) -> None:
        """
        Delete a blob from ADLS Gen2
        
        Args:
            blob_path (str): Path of the blob to delete
            
        Raises:
            StorageError: If deletion fails
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_path
            )
            
            blob_client.delete_blob()
            
            logger.info(f"Successfully deleted blob: {blob_path}")
            
        except ResourceNotFoundError:
            logger.warning(f"Blob not found for deletion: {blob_path}")
        except Exception as e:
            error_msg = f"Error deleting blob {blob_path}: {e}"
            logger.error(error_msg)
            raise StorageError(error_msg)
    
    def get_blob_url(self, blob_path: str, expiry_hours: int = 24) -> str:
        """
        Generate a SAS URL for blob access
        
        Args:
            blob_path (str): Path of the blob
            expiry_hours (int): Hours until URL expires
            
        Returns:
            str: SAS URL for blob access
            
        Raises:
            StorageError: If URL generation fails
        """
        try:
            from azure.storage.blob import generate_blob_sas, BlobSasPermissions
            from datetime import timedelta
            
            # Generate SAS token
            sas_token = generate_blob_sas(
                account_name=self.account_name,
                account_key=self.account_key,
                container_name=self.container_name,
                blob_name=blob_path,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.utcnow() + timedelta(hours=expiry_hours)
            )
            
            # Construct full URL
            blob_url = f"https://{self.account_name}.blob.core.windows.net/{self.container_name}/{blob_path}?{sas_token}"
            
            logger.info(f"Generated SAS URL for blob: {blob_path}")
            return blob_url
            
        except Exception as e:
            error_msg = f"Error generating SAS URL for {blob_path}: {e}"
            logger.error(error_msg)
            raise StorageError(error_msg)
    
    def upload_validation_result(self, result: Dict[str, Any], table_name: str, timestamp: str) -> str:
        """
        Upload validation result with standardized path structure
        
        Args:
            result (Dict[str, Any]): Validation result data
            table_name (str): Name of the validated table
            timestamp (str): Timestamp string for the validation
            
        Returns:
            str: Full blob path of uploaded result
        """
        blob_path = f"{self.results_path}/{table_name}/{timestamp.replace(':', '-')}.json"
        return self.upload_json(result, blob_path)
    
    def upload_documentation(self, html_content: str, doc_name: str, timestamp: str) -> str:
        """
        Upload documentation with standardized path structure
        
        Args:
            html_content (str): HTML documentation content
            doc_name (str): Name of the documentation file
            timestamp (str): Timestamp string for the documentation
            
        Returns:
            str: Full blob path of uploaded documentation
        """
        blob_path = f"{self.docs_path}/{doc_name}_{timestamp.replace(':', '-')}.html"
        return self.upload_html(html_content, blob_path)
    
    def create_folder_structure(self, paths: List[str]) -> None:
        """
        Create folder structure by uploading placeholder files
        
        Args:
            paths (List[str]): List of folder paths to create
        """
        try:
            for path in paths:
                placeholder_path = f"{path}/.placeholder"
                self.upload_text("# Placeholder file for folder structure", placeholder_path)
                
            logger.info(f"Created folder structure for {len(paths)} paths")
            
        except Exception as e:
            error_msg = f"Error creating folder structure: {e}"
            logger.error(error_msg)
            raise StorageError(error_msg)
