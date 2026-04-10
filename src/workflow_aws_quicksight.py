"""
AWS QuickSight Analytics Integration Module for Workflow System

Implements a QuickSightIntegration class with:
1. Account management: Manage QuickSight accounts
2. Data sources: Create/manage data sources
3. Datasets: Create/manage datasets
4. Analyses: Create/manage analyses
5. Dashboards: Create/manage dashboards
6. Templates: Create/manage templates
7. Users: Manage users and groups
8. SPICE: Manage SPICE capacity
9. Embedded analytics: Embedded dashboard URLs
10. CloudWatch integration: Account level monitoring

Commit: 'feat(aws-quicksight): add AWS QuickSight with account management, data sources, datasets, analyses, dashboards, templates, users, SPICE capacity, embedded analytics, CloudWatch'
"""

import uuid
import json
import time
import logging
import hashlib
import subprocess
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional, Set, Union, Type, Awaitable
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum
import copy
import threading
import os
import re

try:
    import boto3
    from botocore.exceptions import ClientError, BotoCoreError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    boto3 = None
    ClientError = None
    BotoCoreError = None


logger = logging.getLogger(__name__)


class QuicksightAccountType(Enum):
    """QuickSight account type."""
    STANDARD = "STANDARD"
    ENTERPRISE = "ENTERPRISE"
    ENTERPRISE_Q = "ENTERPRISE_Q"


class QuicksightDataSourceType(Enum):
    """QuickSight data source types."""
    ATHENA = "ATHENA"
    RDS = "RDS"
    Aurora = "AURORA"
    redshift = "REDSHIFT"
    S3 = "S3"
    SNOWFLAKE = "SNOWFLAKE"
    DATABRICKS = "DATABRICKS"
    TERADATA = "TERADATA"
    BIGQUERY = "BIGQUERY"
    SQLSERVER = "SQLSERVER"
    MYSQL = "MYSQL"
    POSTGRESQL = "POSTGRESQL"
    ORACLE = "ORACLE"


class QuicksightPermission(Enum):
    """QuickSight permission types."""
    QUICKSIGHT_OWNER = "quicksight:DescribeAccount"
    QUICKSIGHT_USER = "quicksight:DescribeUser"
    QUICKSIGHT_ADMIN = "quicksight:DescribeDashboard"
    QUICKSIGHT_AUTHOR = "quicksight:DescribeAnalysis"


class QuicksightIdentityType(Enum):
    """QuickSight identity type."""
    IAM = "IAM"
    QUICKSIGHT = "QUICKSIGHT"


class QuicksightDatasetRefreshStatus(Enum):
    """Dataset refresh status."""
    REFRESH_SUCCESSFUL = "REFRESH_SUCCESSFUL"
    REFRESH_FAILED = "REFRESH_FAILED"
    REFRESH_RUNNING = "REFRESH_RUNNING"


class QuicksightDashboardType(Enum):
    """Dashboard type."""
    INTERACTIVE = "INTERACTIVE"
    PARAMETERIZED = "PARAMETERIZED"
    STORY = "STORY"


@dataclass
class DataSourceConfig:
    """Data source configuration."""
    name: str
    source_type: QuicksightDataSourceType
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    catalog: Optional[str] = None
    schema: Optional[str] = None
    warehouse: Optional[str] = None
    credentials: Optional[Dict[str, str]] = None
    vpc_connection_id: Optional[str] = None
    data_source_parameters: Optional[Dict[str, Any]] = None


@dataclass
class DatasetConfig:
    """Dataset configuration."""
    name: str
    import_mode: str = "DIRECT_QUERY"
    physical_table_map: Optional[Dict[str, Any]] = None
    logical_table_map: Optional[Dict[str, Any]] = None
    column_groups: Optional[List[Dict[str, Any]]] = None
    field_folders: Optional[Dict[str, Any]] = None


@dataclass
class AnalysisConfig:
    """Analysis configuration."""
    name: str
    analysis_id: Optional[str] = None
    source_entity: Optional[Dict[str, Any]] = None
    theme_arn: Optional[str] = None


@dataclass
class DashboardConfig:
    """Dashboard configuration."""
    name: str
    dashboard_id: Optional[str] = None
    source_entity: Optional[Dict[str, Any]] = None
    version_description: Optional[str] = None


@dataclass
class TemplateConfig:
    """Template configuration."""
    name: str
    template_id: Optional[str] = None
    source_entity: Optional[Dict[str, Any]] = None
    permissions: Optional[List[Dict[str, Any]]] = None


@dataclass
class UserConfig:
    """User configuration."""
    username: str
    email: str
    identity_type: QuicksightIdentityType = QuicksightIdentityType.QUICKSIGHT
    user_role: str = "READER"
    aws_account_id: Optional[str] = None
    namespace: str = "default"


@dataclass
class GroupConfig:
    """Group configuration."""
    group_name: str
    description: Optional[str] = None
    namespace: str = "default"


@dataclass
class SpiceConfig:
    """SPICE capacity configuration."""
    region: str
    requested_capacity: int
    requested_capacity_type: Optional[str] = None


@dataclass
class EmbeddedDashboardConfig:
    """Embedded dashboard configuration."""
    dashboard_id: str
    user_arn: Optional[str] = None
    session_tags: Optional[Dict[str, str]] = None
    expires_in_minutes: int = 60
    theme_arn: Optional[str] = None


class QuickSightIntegration:
    """
    AWS QuickSight Analytics Integration.
    
    Provides comprehensive QuickSight functionality including:
    - Account management: QuickSight account settings and subscription
    - Data sources: Create and manage data source connections
    - Datasets: Create and manage SPICE and direct query datasets
    - Analyses: Create and manage analyses
    - Dashboards: Create and manage dashboards
    - Templates: Create and manage templates
    - Users: Manage users and groups
    - SPICE: Manage SPICE capacity
    - Embedded analytics: Generate embedded dashboard URLs
    - CloudWatch: Account level monitoring integration
    
    Attributes:
        aws_region: AWS region name
        profile_name: AWS profile name (optional)
        account_id: AWS account ID
        namespace: QuickSight namespace (default: 'default')
    """
    
    def __init__(
        self,
        aws_region: str = "us-east-1",
        profile_name: Optional[str] = None,
        account_id: Optional[str] = None,
        namespace: str = "default"
    ):
        """
        Initialize QuickSight integration.
        
        Args:
            aws_region: AWS region for QuickSight operations
            profile_name: AWS credentials profile name
            account_id: AWS account ID (optional, auto-detected if not provided)
            namespace: QuickSight namespace (default: 'default')
        """
        self.aws_region = aws_region
        self.profile_name = profile_name
        self.account_id = account_id
        self.namespace = namespace
        self._clients = {}
        self._resources = {}
        self._lock = threading.RLock()
        
        if BOTO3_AVAILABLE:
            self._initialize_clients()
    
    def _initialize_clients(self):
        """Initialize boto3 clients for QuickSight services."""
        try:
            session_kwargs = {"region_name": self.aws_region}
            if self.profile_name:
                session_kwargs["profile_name"] = self.profile_name
            
            session = boto3.Session(**session_kwargs)
            
            # Get account ID if not provided
            if not self.account_id:
                sts = session.client("sts", region_name=self.aws_region)
                self.account_id = sts.get_caller_identity()["Account"]
            
            # QuickSight client
            self._clients["quicksight"] = session.client(
                "quicksight", region_name=self.aws_region
            )
            
            # CloudWatch client for monitoring
            self._clients["cloudwatch"] = session.client(
                "cloudwatch", region_name=self.aws_region
            )
            
            # CloudWatch Logs client
            self._clients["logs"] = session.client(
                "logs", region_name=self.aws_region
            )
            
            # STS client
            self._clients["sts"] = session.client(
                "sts", region_name=self.aws_region
            )
            
            logger.info(f"QuickSight clients initialized for region {self.aws_region}")
        except Exception as e:
            logger.error(f"Failed to initialize QuickSight clients: {e}")
    
    @property
    def quicksight(self):
        """Get QuickSight client."""
        return self._clients.get("quicksight")
    
    @property
    def cloudwatch(self):
        """Get CloudWatch client."""
        return self._clients.get("cloudwatch")
    
    # =========================================================================
    # ACCOUNT MANAGEMENT
    # =========================================================================
    
    def get_account_info(self) -> Dict[str, Any]:
        """
        Get QuickSight account information.
        
        Returns:
            dict: Account information including subscription status
        
        Example:
            >>> info = self.get_account_info()
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            response = self.quicksight.describe_account_settings(
                AwsAccountId=self.account_id
            )
            return response
        except ClientError as e:
            logger.error(f"Failed to get account info: {e}")
            raise
    
    def get_account_subscription(self) -> Dict[str, Any]:
        """
        Get QuickSight account subscription details.
        
        Returns:
            dict: Subscription information
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            response = self.quicksight.describe_account_subscription(
                AwsAccountId=self.account_id
            )
            return response
        except ClientError as e:
            logger.error(f"Failed to get subscription: {e}")
            raise
    
    def update_account_settings(
        self,
        notification_email: str,
        default_namespace: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Update QuickSight account settings.
        
        Args:
            notification_email: Email for notifications
            default_namespace: Default namespace (optional)
        
        Returns:
            dict: Update response
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            kwargs = {
                "AwsAccountId": self.account_id,
                "NotificationEmail": notification_email
            }
            if default_namespace:
                kwargs["DefaultNamespace"] = default_namespace
            
            response = self.quicksight.update_account_settings(**kwargs)
            logger.info(f"Updated account settings for {self.account_id}")
            return response
        except ClientError as e:
            logger.error(f"Failed to update account settings: {e}")
            raise
    
    def list_account_aliases(self) -> List[Dict[str, Any]]:
        """
        List QuickSight account aliases.
        
        Returns:
            list: Account aliases
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            response = self.quicksight.listAccountAliases(
                AwsAccountId=self.account_id
            )
            return response.get("AccountAliases", [])
        except ClientError as e:
            logger.error(f"Failed to list account aliases: {e}")
            raise
    
    # =========================================================================
    # DATA SOURCES
    # =========================================================================
    
    def create_data_source(
        self,
        config: DataSourceConfig,
        permissions: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Create a new QuickSight data source.
        
        Args:
            config: DataSourceConfig object with connection details
            permissions: Optional permissions for the data source
        
        Returns:
            dict: Created data source information
        
        Example:
            >>> config = DataSourceConfig(
            ...     name="MyAthenaSource",
            ...     source_type=QuicksightDataSourceType.ATHENA,
            ...     catalog="AwsDataCatalog",
            ...     database="mydb"
            ... )
            >>> source = self.create_data_source(config)
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            # Build data source parameters based on type
            params = self._build_data_source_parameters(config)
            
            # Create data source
            create_params = {
                "AwsAccountId": self.account_id,
                "DataSourceId": self._generate_id(config.name),
                "Name": config.name,
                "Type": config.source_type.value,
                "DataSourceParameters": params
            }
            
            if config.credentials:
                create_params["Credentials"] = self._build_credentials(
                    config.credentials
                )
            
            if config.vpc_connection_id:
                create_params["VpcConnectionProperties"] = {
                    "VpcConnectionId": config.vpc_connection_id
                }
            
            response = self.quicksight.create_data_source(**create_params)
            
            # Apply permissions if provided
            if permissions:
                self._update_data_source_permissions(
                    response["DataSourceId"], permissions
                )
            
            logger.info(f"Created data source: {config.name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to create data source: {e}")
            raise
    
    def _build_data_source_parameters(
        self,
        config: DataSourceConfig
    ) -> Dict[str, Any]:
        """Build data source parameters based on type."""
        source_type = config.source_type
        
        if source_type == QuicksightDataSourceType.ATHENA:
            return {
                "AthenaParameters": {
                    "WorkGroup": config.catalog or "primary",
                    "ResultS3Bucket": config.data_source_parameters.get(
                        "s3_bucket"
                    ) if config.data_source_parameters else None
                }
            }
        elif source_type == QuicksightDataSourceType.S3:
            return {
                "S3Parameters": {
                    "ManifestFileLocation": {
                        "Bucket": config.data_source_parameters.get("bucket"),
                        "Key": config.data_source_parameters.get("key")
                    }
                }
            }
        elif source_type == QuicksightDataSourceType.redshift:
            return {
                "RedshiftParameters": {
                    "Host": config.host,
                    "Port": config.port or 5439,
                    "Database": config.database
                }
            }
        elif source_type in [QuicksightDataSourceType.RDS, QuicksightDataSourceType.Aurora]:
            return {
                "RdsParameters": {
                    "InstanceId": config.host,
                    "Database": config.database
                }
            }
        elif source_type == QuicksightDataSourceType.SQLSERVER:
            return {
                "SqlServerParameters": {
                    "Host": config.host,
                    "Port": config.port or 1433,
                    "Database": config.database
                }
            }
        elif source_type == QuicksightDataSourceType.MYSQL:
            return {
                "MySqlParameters": {
                    "Host": config.host,
                    "Port": config.port or 3306,
                    "Database": config.database
                }
            }
        elif source_type == QuicksightDataSourceType.POSTGRESQL:
            return {
                "PostgreSqlParameters": {
                    "Host": config.host,
                    "Port": config.port or 5432,
                    "Database": config.database
                }
            }
        elif source_type == QuicksightDataSourceType.SNOWFLAKE:
            return {
                "SnowflakeParameters": {
                    "Host": config.host,
                    "Warehouse": config.warehouse
                }
            }
        elif source_type == QuicksightDataSourceType.BIGQUERY:
            return {
                "BigQueryParameters": {
                    "ProjectId": config.data_source_parameters.get("project_id")
                    if config.data_source_parameters else None
                }
            }
        else:
            return {}
    
    def _build_credentials(
        self,
        credentials: Dict[str, str]
    ) -> Dict[str, Any]:
        """Build credential parameters."""
        cred_type = credentials.get("type", "CREDENTIAL_PAIR")
        
        if cred_type == "CREDENTIAL_PAIR":
            return {
                "CredentialPair": {
                    "Username": credentials.get("username"),
                    "Password": credentials.get("password")
                }
            }
        return {}
    
    def _update_data_source_permissions(
        self,
        data_source_id: str,
        permissions: List[Dict[str, Any]]
    ):
        """Update data source permissions."""
        try:
            self.quicksight.update_data_source_permissions(
                AwsAccountId=self.account_id,
                DataSourceId=data_source_id,
                GrantPermissions=permissions
            )
        except ClientError as e:
            logger.warning(f"Failed to update permissions: {e}")
    
    def list_data_sources(self) -> List[Dict[str, Any]]:
        """
        List all data sources in the account.
        
        Returns:
            list: List of data sources
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            response = self.quicksight.list_data_sources(
                AwsAccountId=self.account_id
            )
            return response.get("DataSources", [])
        except ClientError as e:
            logger.error(f"Failed to list data sources: {e}")
            raise
    
    def describe_data_source(self, data_source_id: str) -> Dict[str, Any]:
        """
        Get details of a specific data source.
        
        Args:
            data_source_id: Data source ID
        
        Returns:
            dict: Data source details
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            response = self.quicksight.describe_data_source(
                AwsAccountId=self.account_id,
                DataSourceId=data_source_id
            )
            return response
        except ClientError as e:
            logger.error(f"Failed to describe data source: {e}")
            raise
    
    def delete_data_source(self, data_source_id: str) -> Dict[str, Any]:
        """
        Delete a data source.
        
        Args:
            data_source_id: Data source ID to delete
        
        Returns:
            dict: Delete response
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            response = self.quicksight.delete_data_source(
                AwsAccountId=self.account_id,
                DataSourceId=data_source_id
            )
            logger.info(f"Deleted data source: {data_source_id}")
            return response
        except ClientError as e:
            logger.error(f"Failed to delete data source: {e}")
            raise
    
    def update_data_source(
        self,
        data_source_id: str,
        name: str,
        credentials: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Update a data source.
        
        Args:
            data_source_id: Data source ID
            name: New name
            credentials: New credentials (optional)
        
        Returns:
            dict: Update response
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            params = {"AwsAccountId": self.account_id, "DataSourceId": data_source_id}
            
            if name:
                params["Name"] = name
            
            if credentials:
                params["Credentials"] = self._build_credentials(credentials)
            
            response = self.quicksight.update_data_source(**params)
            logger.info(f"Updated data source: {data_source_id}")
            return response
        except ClientError as e:
            logger.error(f"Failed to update data source: {e}")
            raise
    
    # =========================================================================
    # DATASETS
    # =========================================================================
    
    def create_dataset(
        self,
        config: DatasetConfig,
        permissions: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Create a new QuickSight dataset.
        
        Args:
            config: DatasetConfig object
            permissions: Optional permissions for the dataset
        
        Returns:
            dict: Created dataset information
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            dataset_id = self._generate_id(config.name)
            
            create_params = {
                "AwsAccountId": self.account_id,
                "DatasetId": dataset_id,
                "Name": config.name,
                "ImportMode": config.import_mode
            }
            
            if config.physical_table_map:
                create_params["PhysicalTableMap"] = config.physical_table_map
            
            if config.logical_table_map:
                create_params["LogicalTableMap"] = config.logical_table_map
            
            response = self.quicksight.create_dataset(**create_params)
            
            if permissions:
                self._update_dataset_permissions(dataset_id, permissions)
            
            logger.info(f"Created dataset: {config.name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to create dataset: {e}")
            raise
    
    def _update_dataset_permissions(
        self,
        dataset_id: str,
        permissions: List[Dict[str, Any]]
    ):
        """Update dataset permissions."""
        try:
            self.quicksight.update_dataset_permissions(
                AwsAccountId=self.account_id,
                DatasetId=dataset_id,
                GrantPermissions=permissions
            )
        except ClientError as e:
            logger.warning(f"Failed to update permissions: {e}")
    
    def list_datasets(self) -> List[Dict[str, Any]]:
        """
        List all datasets in the account.
        
        Returns:
            list: List of datasets
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            response = self.quicksight.list_datasets(
                AwsAccountId=self.account_id
            )
            return response.get("DatasetSummaries", [])
        except ClientError as e:
            logger.error(f"Failed to list datasets: {e}")
            raise
    
    def describe_dataset(self, dataset_id: str) -> Dict[str, Any]:
        """
        Get details of a specific dataset.
        
        Args:
            dataset_id: Dataset ID
        
        Returns:
            dict: Dataset details
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            response = self.quicksight.describe_dataset(
                AwsAccountId=self.account_id,
                DatasetId=dataset_id
            )
            return response
        except ClientError as e:
            logger.error(f"Failed to describe dataset: {e}")
            raise
    
    def delete_dataset(self, dataset_id: str) -> Dict[str, Any]:
        """
        Delete a dataset.
        
        Args:
            dataset_id: Dataset ID to delete
        
        Returns:
            dict: Delete response
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            response = self.quicksight.delete_dataset(
                AwsAccountId=self.account_id,
                DatasetId=dataset_id
            )
            logger.info(f"Deleted dataset: {dataset_id}")
            return response
        except ClientError as e:
            logger.error(f"Failed to delete dataset: {e}")
            raise
    
    def update_dataset(
        self,
        dataset_id: str,
        name: Optional[str] = None,
        import_mode: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Update a dataset.
        
        Args:
            dataset_id: Dataset ID
            name: New name
            import_mode: New import mode
        
        Returns:
            dict: Update response
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            params = {"AwsAccountId": self.account_id, "DatasetId": dataset_id}
            
            if name:
                params["Name"] = name
            if import_mode:
                params["ImportMode"] = import_mode
            
            response = self.quicksight.update_dataset(**params)
            logger.info(f"Updated dataset: {dataset_id}")
            return response
        except ClientError as e:
            logger.error(f"Failed to update dataset: {e}")
            raise
    
    def refresh_dataset(self, dataset_id: str) -> Dict[str, Any]:
        """
        Trigger a dataset refresh.
        
        Args:
            dataset_id: Dataset ID to refresh
        
        Returns:
            dict: Refresh response
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            response = self.quicksight.create_ingestion(
                AwsAccountId=self.account_id,
                DatasetId=dataset_id,
                IngestionId=f"refresh-{int(time.time())}"
            )
            logger.info(f"Triggered refresh for dataset: {dataset_id}")
            return response
        except ClientError as e:
            logger.error(f"Failed to refresh dataset: {e}")
            raise
    
    def list_dataset_refreshes(
        self,
        dataset_id: str,
        status: Optional[QuicksightDatasetRefreshStatus] = None
    ) -> List[Dict[str, Any]]:
        """
        List dataset refresh history.
        
        Args:
            dataset_id: Dataset ID
            status: Filter by status (optional)
        
        Returns:
            list: List of refreshes
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            response = self.quicksight.list_ingestions(
                AwsAccountId=self.account_id,
                DatasetId=dataset_id
            )
            
            ingestions = response.get("Ingestions", [])
            
            if status:
                ingestions = [
                    i for i in ingestions
                    if i.get("IngStatus") == status.value
                ]
            
            return ingestions
        except ClientError as e:
            logger.error(f"Failed to list refreshes: {e}")
            raise
    
    # =========================================================================
    # ANALYSES
    # =========================================================================
    
    def create_analysis(
        self,
        config: AnalysisConfig,
        permissions: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Create a new QuickSight analysis.
        
        Args:
            config: AnalysisConfig object
            permissions: Optional permissions for the analysis
        
        Returns:
            dict: Created analysis information
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            analysis_id = config.analysis_id or self._generate_id(config.name)
            
            create_params = {
                "AwsAccountId": self.account_id,
                "AnalysisId": analysis_id,
                "Name": config.name
            }
            
            if config.source_entity:
                create_params["SourceEntity"] = config.source_entity
            
            if config.theme_arn:
                create_params["ThemeArn"] = config.theme_arn
            
            response = self.quicksight.create_analysis(**create_params)
            
            if permissions:
                self._update_analysis_permissions(analysis_id, permissions)
            
            logger.info(f"Created analysis: {config.name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to create analysis: {e}")
            raise
    
    def _update_analysis_permissions(
        self,
        analysis_id: str,
        permissions: List[Dict[str, Any]]
    ):
        """Update analysis permissions."""
        try:
            self.quicksight.update_analysis_permissions(
                AwsAccountId=self.account_id,
                AnalysisId=analysis_id,
                GrantPermissions=permissions
            )
        except ClientError as e:
            logger.warning(f"Failed to update permissions: {e}")
    
    def list_analyses(self) -> List[Dict[str, Any]]:
        """
        List all analyses in the account.
        
        Returns:
            list: List of analyses
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            response = self.quicksight.list_analyses(
                AwsAccountId=self.account_id
            )
            return response.get("AnalysisSummaryList", [])
        except ClientError as e:
            logger.error(f"Failed to list analyses: {e}")
            raise
    
    def describe_analysis(self, analysis_id: str) -> Dict[str, Any]:
        """
        Get details of a specific analysis.
        
        Args:
            analysis_id: Analysis ID
        
        Returns:
            dict: Analysis details
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            response = self.quicksight.describe_analysis(
                AwsAccountId=self.account_id,
                AnalysisId=analysis_id
            )
            return response
        except ClientError as e:
            logger.error(f"Failed to describe analysis: {e}")
            raise
    
    def delete_analysis(self, analysis_id: str) -> Dict[str, Any]:
        """
        Delete an analysis.
        
        Args:
            analysis_id: Analysis ID to delete
        
        Returns:
            dict: Delete response
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            response = self.quicksight.delete_analysis(
                AwsAccountId=self.account_id,
                AnalysisId=analysis_id
            )
            logger.info(f"Deleted analysis: {analysis_id}")
            return response
        except ClientError as e:
            logger.error(f"Failed to delete analysis: {e}")
            raise
    
    def update_analysis(
        self,
        analysis_id: str,
        name: Optional[str] = None,
        theme_arn: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Update an analysis.
        
        Args:
            analysis_id: Analysis ID
            name: New name
            theme_arn: New theme ARN
        
        Returns:
            dict: Update response
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            params = {"AwsAccountId": self.account_id, "AnalysisId": analysis_id}
            
            if name:
                params["Name"] = name
            if theme_arn:
                params["ThemeArn"] = theme_arn
            
            response = self.quicksight.update_analysis(**params)
            logger.info(f"Updated analysis: {analysis_id}")
            return response
        except ClientError as e:
            logger.error(f"Failed to update analysis: {e}")
            raise
    
    def clone_analysis(
        self,
        source_analysis_id: str,
        new_name: str,
        permissions: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Clone an analysis.
        
        Args:
            source_analysis_id: Source analysis ID to clone
            new_name: Name for the cloned analysis
            permissions: Permissions for the clone
        
        Returns:
            dict: Cloned analysis information
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            source_entity = {
                "SourceAnalysis": {
                    "Arn": self._get_analysis_arn(source_analysis_id),
                    "DataSetReferences": []
                }
            }
            
            new_analysis_id = self._generate_id(new_name)
            
            response = self.quicksight.create_analysis(
                AwsAccountId=self.account_id,
                AnalysisId=new_analysis_id,
                Name=new_name,
                SourceEntity=source_entity
            )
            
            if permissions:
                self._update_analysis_permissions(new_analysis_id, permissions)
            
            logger.info(f"Cloned analysis: {source_analysis_id} to {new_name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to clone analysis: {e}")
            raise
    
    def _get_analysis_arn(self, analysis_id: str) -> str:
        """Get analysis ARN."""
        return f"arn:aws:quicksight:{self.aws_region}:{self.account_id}:analysis/{analysis_id}"
    
    # =========================================================================
    # DASHBOARDS
    # =========================================================================
    
    def create_dashboard(
        self,
        config: DashboardConfig,
        permissions: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Create a new QuickSight dashboard.
        
        Args:
            config: DashboardConfig object
            permissions: Optional permissions for the dashboard
        
        Returns:
            dict: Created dashboard information
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            dashboard_id = config.dashboard_id or self._generate_id(config.name)
            
            create_params = {
                "AwsAccountId": self.account_id,
                "DashboardId": dashboard_id,
                "Name": config.name
            }
            
            if config.source_entity:
                create_params["SourceEntity"] = config.source_entity
            
            if config.version_description:
                create_params["VersionDescription"] = config.version_description
            
            response = self.quicksight.create_dashboard(**create_params)
            
            if permissions:
                self._update_dashboard_permissions(dashboard_id, permissions)
            
            logger.info(f"Created dashboard: {config.name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to create dashboard: {e}")
            raise
    
    def _update_dashboard_permissions(
        self,
        dashboard_id: str,
        permissions: List[Dict[str, Any]]
    ):
        """Update dashboard permissions."""
        try:
            self.quicksight.update_dashboard_permissions(
                AwsAccountId=self.account_id,
                DashboardId=dashboard_id,
                GrantPermissions=permissions
            )
        except ClientError as e:
            logger.warning(f"Failed to update permissions: {e}")
    
    def list_dashboards(self) -> List[Dict[str, Any]]:
        """
        List all dashboards in the account.
        
        Returns:
            list: List of dashboards
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            response = self.quicksight.list_dashboards(
                AwsAccountId=self.account_id
            )
            return response.get("DashboardSummaryList", [])
        except ClientError as e:
            logger.error(f"Failed to list dashboards: {e}")
            raise
    
    def describe_dashboard(self, dashboard_id: str) -> Dict[str, Any]:
        """
        Get details of a specific dashboard.
        
        Args:
            dashboard_id: Dashboard ID
        
        Returns:
            dict: Dashboard details
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            response = self.quicksight.describe_dashboard(
                AwsAccountId=self.account_id,
                DashboardId=dashboard_id
            )
            return response
        except ClientError as e:
            logger.error(f"Failed to describe dashboard: {e}")
            raise
    
    def delete_dashboard(self, dashboard_id: str) -> Dict[str, Any]:
        """
        Delete a dashboard.
        
        Args:
            dashboard_id: Dashboard ID to delete
        
        Returns:
            dict: Delete response
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            response = self.quicksight.delete_dashboard(
                AwsAccountId=self.account_id,
                DashboardId=dashboard_id
            )
            logger.info(f"Deleted dashboard: {dashboard_id}")
            return response
        except ClientError as e:
            logger.error(f"Failed to delete dashboard: {e}")
            raise
    
    def update_dashboard(
        self,
        dashboard_id: str,
        name: Optional[str] = None,
        source_entity: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Update a dashboard.
        
        Args:
            dashboard_id: Dashboard ID
            name: New name
            source_entity: New source entity
        
        Returns:
            dict: Update response
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            params = {"AwsAccountId": self.account_id, "DashboardId": dashboard_id}
            
            if name:
                params["Name"] = name
            if source_entity:
                params["SourceEntity"] = source_entity
            
            response = self.quicksight.update_dashboard(**params)
            logger.info(f"Updated dashboard: {dashboard_id}")
            return response
        except ClientError as e:
            logger.error(f"Failed to update dashboard: {e}")
            raise
    
    def get_dashboard_embed_url(
        self,
        dashboard_id: str,
        user_arn: Optional[str] = None,
        state: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get dashboard embed URL.
        
        Args:
            dashboard_id: Dashboard ID
            user_arn: User ARN for anonymous embedding
            state: Dashboard state
        
        Returns:
            dict: Embed URL information
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            params = {
                "AwsAccountId": self.account_id,
                "DashboardId": dashboard_id
            }
            
            if user_arn:
                params["UserArn"] = user_arn
            if state:
                params["State"] = state
            
            response = self.quicksight.get_dashboard_embed_url(**params)
            return response
        except ClientError as e:
            logger.error(f"Failed to get embed URL: {e}")
            raise
    
    def list_dashboard_versions(
        self,
        dashboard_id: str
    ) -> List[Dict[str, Any]]:
        """
        List dashboard versions.
        
        Args:
            dashboard_id: Dashboard ID
        
        Returns:
            list: List of versions
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            response = self.quicksight.list_dashboard_versions(
                AwsAccountId=self.account_id,
                DashboardId=dashboard_id
            )
            return response.get("DashboardVersionSummaryList", [])
        except ClientError as e:
            logger.error(f"Failed to list versions: {e}")
            raise
    
    # =========================================================================
    # TEMPLATES
    # =========================================================================
    
    def create_template(
        self,
        config: TemplateConfig,
        permissions: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Create a new QuickSight template.
        
        Args:
            config: TemplateConfig object
            permissions: Optional permissions for the template
        
        Returns:
            dict: Created template information
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            template_id = config.template_id or self._generate_id(config.name)
            
            create_params = {
                "AwsAccountId": self.account_id,
                "TemplateId": template_id,
                "Name": config.name
            }
            
            if config.source_entity:
                create_params["SourceEntity"] = config.source_entity
            
            response = self.quicksight.create_template(**create_params)
            
            if permissions:
                self._update_template_permissions(template_id, permissions)
            
            logger.info(f"Created template: {config.name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to create template: {e}")
            raise
    
    def _update_template_permissions(
        self,
        template_id: str,
        permissions: List[Dict[str, Any]]
    ):
        """Update template permissions."""
        try:
            self.quicksight.update_template_permissions(
                AwsAccountId=self.account_id,
                TemplateId=template_id,
                GrantPermissions=permissions
            )
        except ClientError as e:
            logger.warning(f"Failed to update permissions: {e}")
    
    def list_templates(self) -> List[Dict[str, Any]]:
        """
        List all templates in the account.
        
        Returns:
            list: List of templates
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            response = self.quicksight.list_templates(
                AwsAccountId=self.account_id
            )
            return response.get("TemplateSummaryList", [])
        except ClientError as e:
            logger.error(f"Failed to list templates: {e}")
            raise
    
    def describe_template(self, template_id: str) -> Dict[str, Any]:
        """
        Get details of a specific template.
        
        Args:
            template_id: Template ID
        
        Returns:
            dict: Template details
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            response = self.quicksight.describe_template(
                AwsAccountId=self.account_id,
                TemplateId=template_id
            )
            return response
        except ClientError as e:
            logger.error(f"Failed to describe template: {e}")
            raise
    
    def delete_template(self, template_id: str) -> Dict[str, Any]:
        """
        Delete a template.
        
        Args:
            template_id: Template ID to delete
        
        Returns:
            dict: Delete response
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            response = self.quicksight.delete_template(
                AwsAccountId=self.account_id,
                TemplateId=template_id
            )
            logger.info(f"Deleted template: {template_id}")
            return response
        except ClientError as e:
            logger.error(f"Failed to delete template: {e}")
            raise
    
    def update_template(
        self,
        template_id: str,
        name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Update a template.
        
        Args:
            template_id: Template ID
            name: New name
        
        Returns:
            dict: Update response
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            params = {"AwsAccountId": self.account_id, "TemplateId": template_id}
            
            if name:
                params["Name"] = name
            
            response = self.quicksight.update_template(**params)
            logger.info(f"Updated template: {template_id}")
            return response
        except ClientError as e:
            logger.error(f"Failed to update template: {e}")
            raise
    
    # =========================================================================
    # USERS
    # =========================================================================
    
    def register_user(
        self,
        config: UserConfig,
        permissions: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Register a new QuickSight user.
        
        Args:
            config: UserConfig object
            permissions: Optional permissions for the user
        
        Returns:
            dict: Registered user information
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            user_params = {
                "AwsAccountId": self.account_id,
                "Namespace": config.namespace,
                "IdentityType": config.identity_type.value,
                "Username": config.username,
                "Email": config.email,
                "UserRole": config.user_role
            }
            
            response = self.quicksight.register_user(**user_params)
            
            if permissions:
                self._update_user_permissions(
                    config.username, config.namespace, permissions
                )
            
            logger.info(f"Registered user: {config.username}")
            return response
        except ClientError as e:
            logger.error(f"Failed to register user: {e}")
            raise
    
    def _update_user_permissions(
        self,
        username: str,
        namespace: str,
        permissions: List[Dict[str, Any]]
    ):
        """Update user permissions."""
        try:
            self.quicksight.update_user_permissions(
                AwsAccountId=self.account_id,
                Namespace=namespace,
                Username=username,
                UserRole=permissions[0].get("UserRole", "READER"),
                GrantPermissions=permissions
            )
        except ClientError as e:
            logger.warning(f"Failed to update permissions: {e}")
    
    def list_users(self, namespace: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List all users in the namespace.
        
        Args:
            namespace: Namespace to list users from (default: self.namespace)
        
        Returns:
            list: List of users
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        ns = namespace or self.namespace
        
        try:
            response = self.quicksight.list_users(
                AwsAccountId=self.account_id,
                Namespace=ns
            )
            return response.get("UserList", [])
        except ClientError as e:
            logger.error(f"Failed to list users: {e}")
            raise
    
    def describe_user(
        self,
        username: str,
        namespace: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get details of a specific user.
        
        Args:
            username: Username
            namespace: Namespace (default: self.namespace)
        
        Returns:
            dict: User details
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        ns = namespace or self.namespace
        
        try:
            response = self.quicksight.describe_user(
                AwsAccountId=self.account_id,
                Namespace=ns,
                UserName=username
            )
            return response
        except ClientError as e:
            logger.error(f"Failed to describe user: {e}")
            raise
    
    def delete_user(
        self,
        username: str,
        namespace: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Delete a user.
        
        Args:
            username: Username to delete
            namespace: Namespace (default: self.namespace)
        
        Returns:
            dict: Delete response
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        ns = namespace or self.namespace
        
        try:
            response = self.quicksight.delete_user(
                AwsAccountId=self.account_id,
                Namespace=ns,
                UserName=username
            )
            logger.info(f"Deleted user: {username}")
            return response
        except ClientError as e:
            logger.error(f"Failed to delete user: {e}")
            raise
    
    def update_user(
        self,
        username: str,
        email: str,
        user_role: str,
        namespace: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Update a user.
        
        Args:
            username: Username
            email: New email
            user_role: New role
            namespace: Namespace (default: self.namespace)
        
        Returns:
            dict: Update response
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        ns = namespace or self.namespace
        
        try:
            response = self.quicksight.update_user(
                AwsAccountId=self.account_id,
                Namespace=ns,
                UserName=username,
                Email=email,
                UserRole=user_role
            )
            logger.info(f"Updated user: {username}")
            return response
        except ClientError as e:
            logger.error(f"Failed to update user: {e}")
            raise
    
    # =========================================================================
    # GROUPS
    # =========================================================================
    
    def create_group(
        self,
        group_name: str,
        description: Optional[str] = None,
        namespace: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a new QuickSight group.
        
        Args:
            group_name: Name for the group
            description: Group description
            namespace: Namespace (default: self.namespace)
        
        Returns:
            dict: Created group information
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        ns = namespace or self.namespace
        
        try:
            response = self.quicksight.create_group(
                GroupName=group_name,
                AwsAccountId=self.account_id,
                Namespace=ns,
                Description=description or ""
            )
            logger.info(f"Created group: {group_name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to create group: {e}")
            raise
    
    def list_groups(self, namespace: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List all groups in the namespace.
        
        Args:
            namespace: Namespace (default: self.namespace)
        
        Returns:
            list: List of groups
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        ns = namespace or self.namespace
        
        try:
            response = self.quicksight.list_groups(
                AwsAccountId=self.account_id,
                Namespace=ns
            )
            return response.get("GroupList", [])
        except ClientError as e:
            logger.error(f"Failed to list groups: {e}")
            raise
    
    def describe_group(
        self,
        group_name: str,
        namespace: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get details of a specific group.
        
        Args:
            group_name: Group name
            namespace: Namespace (default: self.namespace)
        
        Returns:
            dict: Group details
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        ns = namespace or self.namespace
        
        try:
            response = self.quicksight.describe_group(
                GroupName=group_name,
                AwsAccountId=self.account_id,
                Namespace=ns
            )
            return response
        except ClientError as e:
            logger.error(f"Failed to describe group: {e}")
            raise
    
    def delete_group(
        self,
        group_name: str,
        namespace: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Delete a group.
        
        Args:
            group_name: Group name to delete
            namespace: Namespace (default: self.namespace)
        
        Returns:
            dict: Delete response
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        ns = namespace or self.namespace
        
        try:
            response = self.quicksight.delete_group(
                GroupName=group_name,
                AwsAccountId=self.account_id,
                Namespace=ns
            )
            logger.info(f"Deleted group: {group_name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to delete group: {e}")
            raise
    
    def create_group_membership(
        self,
        username: str,
        group_name: str,
        namespace: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Add a user to a group.
        
        Args:
            username: Username to add
            group_name: Group name
            namespace: Namespace (default: self.namespace)
        
        Returns:
            dict: Membership information
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        ns = namespace or self.namespace
        
        try:
            response = self.quicksight.create_group_membership(
                MemberName=username,
                GroupName=group_name,
                AwsAccountId=self.account_id,
                Namespace=ns
            )
            logger.info(f"Added {username} to group {group_name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to add user to group: {e}")
            raise
    
    def list_group_memberships(
        self,
        group_name: str,
        namespace: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        List members of a group.
        
        Args:
            group_name: Group name
            namespace: Namespace (default: self.namespace)
        
        Returns:
            list: List of group members
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        ns = namespace or self.namespace
        
        try:
            response = self.quicksight.list_group_memberships(
                GroupName=group_name,
                AwsAccountId=self.account_id,
                Namespace=ns
            )
            return response.get("GroupMemberList", [])
        except ClientError as e:
            logger.error(f"Failed to list memberships: {e}")
            raise
    
    def delete_group_membership(
        self,
        username: str,
        group_name: str,
        namespace: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Remove a user from a group.
        
        Args:
            username: Username to remove
            group_name: Group name
            namespace: Namespace (default: self.namespace)
        
        Returns:
            dict: Delete response
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        ns = namespace or self.namespace
        
        try:
            response = self.quicksight.delete_group_membership(
                MemberName=username,
                GroupName=group_name,
                AwsAccountId=self.account_id,
                Namespace=ns
            )
            logger.info(f"Removed {username} from group {group_name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to remove user from group: {e}")
            raise
    
    # =========================================================================
    # SPICE CAPACITY
    # =========================================================================
    
    def get_spice_capacity(self) -> Dict[str, Any]:
        """
        Get SPICE capacity information.
        
        Returns:
            dict: SPICE capacity details
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            response = self.quicksight.describe_account_settings(
                AwsAccountId=self.account_id
            )
            return {
                "AccountId": self.account_id,
                "Region": self.aws_region,
                "Status": "Available"
            }
        except ClientError as e:
            logger.error(f"Failed to get SPICE capacity: {e}")
            raise
    
    def list_spice_ingestion_jobs(
        self,
        dataset_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        List SPICE ingestion jobs.
        
        Args:
            dataset_id: Filter by dataset ID (optional)
        
        Returns:
            list: List of ingestion jobs
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            # This would need dataset_id to be provided
            if dataset_id:
                response = self.quicksight.list_ingestions(
                    AwsAccountId=self.account_id,
                    DatasetId=dataset_id
                )
                return response.get("Ingestions", [])
            return []
        except ClientError as e:
            logger.error(f"Failed to list ingestion jobs: {e}")
            raise
    
    def cancel_ingestion(
        self,
        dataset_id: str,
        ingestion_id: str
    ) -> Dict[str, Any]:
        """
        Cancel a running ingestion.
        
        Args:
            dataset_id: Dataset ID
            ingestion_id: Ingestion ID to cancel
        
        Returns:
            dict: Cancel response
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            response = self.quicksight.cancel_ingestion(
                AwsAccountId=self.account_id,
                DatasetId=dataset_id,
                IngestionId=ingestion_id
            )
            logger.info(f"Cancelled ingestion: {ingestion_id}")
            return response
        except ClientError as e:
            logger.error(f"Failed to cancel ingestion: {e}")
            raise
    
    # =========================================================================
    # EMBEDDED ANALYTICS
    # =========================================================================
    
    def get_embedded_dashboard_url(
        self,
        config: EmbeddedDashboardConfig
    ) -> Dict[str, Any]:
        """
        Get embedded dashboard URL.
        
        Args:
            config: EmbeddedDashboardConfig object
        
        Returns:
            dict: Embed URL and expiration info
        
        Example:
            >>> config = EmbeddedDashboardConfig(
            ...     dashboard_id="my-dashboard-id",
            ...     user_arn="arn:aws:quicksight:...",
            ...     expires_in_minutes=60
            ... )
            >>> result = self.get_embedded_dashboard_url(config)
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            params = {
                "AwsAccountId": self.account_id,
                "DashboardId": config.dashboard_id
            }
            
            if config.user_arn:
                params["UserArn"] = config.user_arn
            
            if config.session_tags:
                params["SessionTags"] = [
                    {"Key": k, "Value": v}
                    for k, v in config.session_tags.items()
                ]
            
            if config.expires_in_minutes:
                params["ExperienceConfiguration"] = {
                    "Dashboard": {
                        "InitialDashboardId": config.dashboard_id
                    }
                }
            
            response = self.quicksight.get_dashboard_embed_url(**params)
            
            return {
                "embed_url": response.get("EmbedUrl"),
                "dashboard_id": config.dashboard_id,
                "expires_at": datetime.now() + timedelta(minutes=config.expires_in_minutes)
            }
        except ClientError as e:
            logger.error(f"Failed to get embedded URL: {e}")
            raise
    
    def get_embedded_analysis_url(
        self,
        analysis_id: str,
        user_arn: Optional[str] = None,
        expires_in_minutes: int = 60
    ) -> Dict[str, Any]:
        """
        Get embedded analysis URL.
        
        Args:
            analysis_id: Analysis ID
            user_arn: User ARN for anonymous embedding
            expires_in_minutes: URL expiration time
        
        Returns:
            dict: Embed URL and expiration info
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            params = {
                "AwsAccountId": self.account_id,
                "AnalysisId": analysis_id
            }
            
            if user_arn:
                params["UserArn"] = user_arn
            
            response = self.quicksight.get_analysis_embed_url(**params)
            
            return {
                "embed_url": response.get("EmbedUrl"),
                "analysis_id": analysis_id,
                "expires_at": datetime.now() + timedelta(minutes=expires_in_minutes)
            }
        except ClientError as e:
            logger.error(f"Failed to get embedded URL: {e}")
            raise
    
    def generate_embed_signature(
        self,
        dashboard_id: str,
        session_id: str,
        expires_at: datetime
    ) -> str:
        """
        Generate embed signature for secure embedding.
        
        Args:
            dashboard_id: Dashboard ID
            session_id: Session ID
            expires_at: Expiration datetime
        
        Returns:
            str: Generated signature
        """
        # This is a placeholder - actual implementation would use
        # QuickSight's generate embed signature API
        message = f"{dashboard_id}:{session_id}:{expires_at.isoformat()}"
        return hashlib.sha256(message.encode()).hexdigest()
    
    # =========================================================================
    # CLOUDWATCH INTEGRATION
    # =========================================================================
    
    def put_quicksight_metrics(
        self,
        metrics: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Put QuickSight metrics to CloudWatch.
        
        Args:
            metrics: List of metric data points
        
        Returns:
            dict: CloudWatch response
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")
        
        try:
            metric_data = []
            for metric in metrics:
                metric_data.append({
                    "MetricName": metric.get("MetricName"),
                    "Dimensions": [
                        {"Name": d.get("Name"), "Value": d.get("Value")}
                        for d in metric.get("Dimensions", [])
                    ],
                    "Value": metric.get("Value"),
                    "Unit": metric.get("Unit", "None"),
                    "Timestamp": metric.get("Timestamp")
                })
            
            response = self.cloudwatch.put_metric_data(
                Namespace="AWS/QuickSight",
                MetricData=metric_data
            )
            
            logger.info(f"Put {len(metrics)} QuickSight metrics to CloudWatch")
            return response
        except ClientError as e:
            logger.error(f"Failed to put metrics: {e}")
            raise
    
    def get_quicksight_metrics(
        self,
        metric_names: List[str],
        start_time: datetime,
        end_time: datetime,
        period: int = 300
    ) -> Dict[str, Any]:
        """
        Get QuickSight metrics from CloudWatch.
        
        Args:
            metric_names: List of metric names
            start_time: Start time
            end_time: End time
            period: Metric period in seconds
        
        Returns:
            dict: Metric data
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")
        
        try:
            response = self.cloudwatch.get_metric_data(
                MetricDataQueries=[
                    {
                        "Id": f"m{i}",
                        "MetricStat": {
                            "Metric": {
                                "Namespace": "AWS/QuickSight",
                                "MetricName": name
                            },
                            "Period": period,
                            "Stat": "Sum"
                        }
                    }
                    for i, name in enumerate(metric_names)
                ],
                StartTime=start_time,
                EndTime=end_time
            )
            
            return response
        except ClientError as e:
            logger.error(f"Failed to get metrics: {e}")
            raise
    
    def create_quicksight_dashboard_alarm(
        self,
        alarm_name: str,
        metric_name: str,
        threshold: float,
        comparison_operator: str = "GreaterThanThreshold",
        period: int = 300,
        evaluation_periods: int = 1
    ) -> Dict[str, Any]:
        """
        Create CloudWatch alarm for QuickSight metrics.
        
        Args:
            alarm_name: Alarm name
            metric_name: QuickSight metric name
            threshold: Alarm threshold
            comparison_operator: Comparison operator
            period: Period in seconds
            evaluation_periods: Number of evaluation periods
        
        Returns:
            dict: Alarm creation response
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")
        
        try:
            response = self.cloudwatch.put_metric_alarm(
                AlarmName=alarm_name,
                AlarmDescription=f"QuickSight {metric_name} alarm",
                MetricName=metric_name,
                Namespace="AWS/QuickSight",
                Statistic="Sum",
                Period=period,
                EvaluationPeriods=evaluation_periods,
                Threshold=threshold,
                ComparisonOperator=comparison_operator,
                TreatMissingData="missing"
            )
            
            logger.info(f"Created alarm: {alarm_name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to create alarm: {e}")
            raise
    
    def create_quicksight_log_group(
        self,
        log_group_name: str,
        retention_days: int = 30
    ) -> Dict[str, Any]:
        """
        Create CloudWatch log group for QuickSight.
        
        Args:
            log_group_name: Log group name
            retention_days: Log retention days
        
        Returns:
            dict: Log group creation response
        """
        if not self._clients.get("logs"):
            raise RuntimeError("CloudWatch Logs client not initialized")
        
        try:
            logs_client = self._clients.get("logs")
            
            # Create log group
            logs_client.create_log_group(
                logGroupName=log_group_name,
                tags={"Application": "QuickSight"}
            )
            
            # Set retention policy
            logs_client.put_retention_policy(
                logGroupName=log_group_name,
                RetentionInDays=retention_days
            )
            
            logger.info(f"Created log group: {log_group_name}")
            return {"log_group_name": log_group_name, "status": "created"}
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceAlreadyExistsException":
                logger.info(f"Log group already exists: {log_group_name}")
                return {"log_group_name": log_group_name, "status": "exists"}
            logger.error(f"Failed to create log group: {e}")
            raise
    
    def put_quicksight_log_events(
        self,
        log_group_name: str,
        log_stream_name: str,
        events: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Put log events to CloudWatch.
        
        Args:
            log_group_name: Log group name
            log_stream_name: Log stream name
            events: List of log events
        
        Returns:
            dict: Put events response
        """
        if not self._clients.get("logs"):
            raise RuntimeError("CloudWatch Logs client not initialized")
        
        try:
            logs_client = self._clients.get("logs")
            
            # Ensure log stream exists
            try:
                logs_client.create_log_stream(
                    logGroupName=log_group_name,
                    logStreamName=log_stream_name
                )
            except ClientError as e:
                if e.response["Error"]["Code"] != "ResourceAlreadyExistsException":
                    raise
            
            # Put events
            response = logs_client.put_log_events(
                logGroupName=log_group_name,
                logStreamName=log_stream_name,
                logEvents=[
                    {
                        "timestamp": int(event.get("timestamp", time.time()) * 1000),
                        "message": event.get("message", "")
                    }
                    for event in events
                ]
            )
            
            return response
        except ClientError as e:
            logger.error(f"Failed to put log events: {e}")
            raise
    
    # =========================================================================
    # HELPER METHODS
    # =========================================================================
    
    def _generate_id(self, name: str) -> str:
        """Generate a unique ID from a name."""
        # Clean and truncate name
        clean_name = re.sub(r"[^a-zA-Z0-9]", "", name)[:20]
        timestamp = int(time.time())
        return f"{clean_name.lower()}_{timestamp}"
    
    def _get_account_health_metrics(self) -> Dict[str, Any]:
        """Get QuickSight account health metrics."""
        return {
            "account_id": self.account_id,
            "region": self.aws_region,
            "namespace": self.namespace,
            "timestamp": datetime.now().isoformat()
        }
    
    def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on QuickSight integration.
        
        Returns:
            dict: Health status information
        """
        status = {
            "service": "QuickSight",
            "region": self.aws_region,
            "account_id": self.account_id,
            "boto3_available": BOTO3_AVAILABLE,
            "clients_initialized": len(self._clients) > 0,
            "timestamp": datetime.now().isoformat()
        }
        
        if BOTO3_AVAILABLE and self._clients:
            try:
                # Test QuickSight connectivity
                self.get_account_info()
                status["quicksight"] = "healthy"
            except Exception as e:
                status["quicksight"] = f"unhealthy: {str(e)}"
        else:
            status["quicksight"] = "not_initialized"
        
        return status
    
    def get_resource_tags(
        self,
        resource_arn: str
    ) -> Dict[str, str]:
        """
        Get tags for a QuickSight resource.
        
        Args:
            resource_arn: Resource ARN
        
        Returns:
            dict: Tags
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            resource_type = self._parse_resource_type(resource_arn)
            
            if resource_type == "data-source":
                response = self.quicksight.list_tags_for_resource(
                    ResourceArn=resource_arn
                )
            elif resource_type == "dataset":
                response = self.quicksight.list_tags_for_resource(
                    ResourceArn=resource_arn
                )
            elif resource_type == "analysis":
                response = self.quicksight.list_tags_for_resource(
                    ResourceArn=resource_arn
                )
            elif resource_type == "dashboard":
                response = self.quicksight.list_tags_for_resource(
                    ResourceArn=resource_arn
                )
            elif resource_type == "template":
                response = self.quicksight.list_tags_for_resource(
                    ResourceArn=resource_arn
                )
            else:
                return {}
            
            tags = response.get("Tags", {})
            return {t["Key"]: t["Value"] for t in tags}
        except ClientError as e:
            logger.error(f"Failed to get tags: {e}")
            raise
    
    def _parse_resource_type(self, arn: str) -> str:
        """Parse resource type from ARN."""
        parts = arn.split(":")
        if len(parts) >= 6:
            resource = parts[5]
            if "/" in resource:
                return resource.split("/")[0]
        return "unknown"
    
    def tag_resource(
        self,
        resource_arn: str,
        tags: Dict[str, str]
    ) -> Dict[str, Any]:
        """
        Tag a QuickSight resource.
        
        Args:
            resource_arn: Resource ARN
            tags: Tags to apply
        
        Returns:
            dict: Tag response
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            tag_list = [{"Key": k, "Value": v} for k, v in tags.items()]
            
            response = self.quicksight.tag_resource(
                ResourceArn=resource_arn,
                Tags=tag_list
            )
            
            logger.info(f"Tagged resource: {resource_arn}")
            return response
        except ClientError as e:
            logger.error(f"Failed to tag resource: {e}")
            raise
    
    def untag_resource(
        self,
        resource_arn: str,
        tag_keys: List[str]
    ) -> Dict[str, Any]:
        """
        Remove tags from a QuickSight resource.
        
        Args:
            resource_arn: Resource ARN
            tag_keys: Tag keys to remove
        
        Returns:
            dict: Untag response
        """
        if not self.quicksight:
            raise RuntimeError("QuickSight client not initialized")
        
        try:
            response = self.quicksight.untag_resource(
                ResourceArn=resource_arn,
                TagKeys=tag_keys
            )
            
            logger.info(f"Untagged resource: {resource_arn}")
            return response
        except ClientError as e:
            logger.error(f"Failed to untag resource: {e}")
            raise
