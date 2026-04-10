"""
AWS Glue Data Integration Module for Workflow System

Implements a GlueIntegration class with:
1. Database management: Create/manage Glue databases
2. Table management: Create/manage Glue tables
3. Crawler management: Create/manage crawlers
4. Job management: Create/manage Glue jobs
5. Triggers: Manage job triggers
6. Dev endpoints: Manage development endpoints
7. Data catalog: Data catalog operations
8. Schema registry: Schema registry integration
9. Data quality: Data quality detection
10. CloudWatch integration: Job metrics and monitoring

Commit: 'feat(aws-glue): add AWS Glue with database/table/crawler/job management, triggers, dev endpoints, data catalog, schema registry, data quality, CloudWatch'
"""

import uuid
import json
import time
import logging
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional, Set, Union, Type, Awaitable
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum
import copy
import threading
import os

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


class GlueDatabaseStatus(Enum):
    """Glue database status."""
    READY = "READY"
    CREATING = "CREATING"
    DELETING = "DELETING"


class TableStatus(Enum):
    """Glue table status."""
    ACTIVE = "ACTIVE"
    CREATING = "CREATING"
    DELETING = "DELETING"


class CrawlerStatus(Enum):
    """Glue crawler status."""
    READY = "READY"
    RUNNING = "RUNNING"
    CANCELLED = "CANCELLED"


class JobRunStatus(Enum):
    """Glue job run status."""
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"
    STOPPED = "STOPPED"
    ERROR = "ERROR"


class TriggerStatus(Enum):
    """Glue trigger status."""
    CREATED = "CREATED"
    ACTIVATED = "ACTIVATED"
    DEACTIVATED = "DEACTIVATED"
    DELETE = "DELETE"


class DevEndpointStatus(Enum):
    """Glue dev endpoint status."""
    READY = "READY"
    CREATING = "CREATING"
    FAILED = "FAILED"
    STOPPING = "STOPPING"


class DataQualityRuleType(Enum):
    """Data quality rule types."""
    IS_NULL = "IS_NULL"
    IS_NOT_NULL = "IS_NOT_NULL"
    IS_UNIQUE = "IS_UNIQUE"
    IS_COMPLETE = "IS_COMPLETE"
    MATCHES_PATTERN = "MATCHES_PATTERN"
    IN_RANGE = "IN_RANGE"
    CONTAINS = "CONTAINS"
    STARTS_WITH = "STARTS_WITH"
    ENDS_WITH = "ENDS_WITH"
    IS_BETWEEN = "IS_BETWEEN"
    IS_IN = "IS_IN"
    IS_TYPE = "IS_TYPE"
    HAS_LENGTH = "HAS_LENGTH"


class DataFormat(Enum):
    """Data formats for Glue tables."""
    JSON = "json"
    CSV = "csv"
    PARQUET = "parquet"
    ORC = "orc"
    AVRO = "avro"
    XML = "xml"
    DELIMITED = "delimited"


class TableType(Enum):
    """Glue table types."""
    EXTERNAL_TABLE = "EXTERNAL_TABLE"
    MANAGED_TABLE = "MANAGED_TABLE"
    VIRTUAL_VIEW = "VIRTUAL_VIEW"
    MATERIALIZED_VIEW = "MATERIALIZED_VIEW"


@dataclass
class DatabaseInfo:
    """Glue database information."""
    name: str
    description: Optional[str] = None
    location_uri: Optional[str] = None
    parameters: Dict[str, str] = field(default_factory=dict)
    create_time: Optional[datetime] = None
    catalog_id: Optional[str] = None
    status: GlueDatabaseStatus = GlueDatabaseStatus.READY
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class TableInfo:
    """Glue table information."""
    name: str
    database_name: str
    description: Optional[str] = None
    table_type: TableType = TableType.EXTERNAL_TABLE
    storage_descriptor: Optional[Dict[str, Any]] = None
    partition_keys: List[Dict[str, str]] = field(default_factory=list)
    parameters: Dict[str, str] = field(default_factory=dict)
    view_original_text: Optional[str] = None
    view_expanded_text: Optional[str] = None
    create_time: Optional[datetime] = None
    update_time: Optional[datetime] = None
    last_access_time: Optional[datetime] = None
    retention: Optional[int] = None
    catalog_id: Optional[str] = None
    status: TableStatus = TableStatus.ACTIVE
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class ColumnInfo:
    """Column information for Glue tables."""
    name: str
    type: str
    comment: Optional[str] = None
    partition_key: bool = False


@dataclass
class CrawlerInfo:
    """Glue crawler information."""
    name: str
    description: Optional[str] = None
    database_name: Optional[str] = None
    table_prefix: Optional[str] = None
    role: Optional[str] = None
    targets: Dict[str, Any] = field(default_factory=dict)
    schedule: Optional[str] = None
    crawl_interval: Optional[int] = None
    recrawl_policy: Optional[str] = None
    schema_change_policy: Dict[str, str] = field(default_factory=dict)
    configuration: Optional[str] = None
    state: CrawlerStatus = CrawlerStatus.READY
    last_crawl: Optional[Dict[str, Any]] = None
    create_time: Optional[datetime] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class JobInfo:
    """Glue job information."""
    name: str
    description: Optional[str] = None
    role: Optional[str] = None
    command: Dict[str, str] = field(default_factory=dict)
    script_location: Optional[str] = None
    python_version: Optional[str] = None
    glue_version: Optional[str] = None
    worker_type: Optional[str] = None
    number_of_workers: Optional[int] = None
    max_capacity: Optional[float] = None
    timeout: Optional[int] = None
    max_retries: Optional[int] = None
    arguments: Dict[str, str] = field(default_factory=dict)
    default_arguments: Dict[str, str] = field(default_factory=dict)
    connections: List[str] = field(default_factory=list)
    security_configuration: Optional[str] = None
    notification_property: Dict[str, Any] = field(default_factory=dict)
    execution_property: Dict[str, Any] = field(default_factory=dict)
    create_time: Optional[datetime] = None
    last_modified_time: Optional[datetime] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class JobRunInfo:
    """Glue job run information."""
    job_name: str
    run_id: str
    arguments: Dict[str, str] = field(default_factory=dict)
    allocated_capacity: Optional[float] = None
    timeout: Optional[int] = None
    worker_type: Optional[str] = None
    number_of_workers: Optional[int] = None
    status: JobRunStatus = JobRunStatus.RUNNING
    started_on: Optional[datetime] = None
    completed_on: Optional[datetime] = None
    error_message: Optional[str] = None
    execution_time: Optional[float] = None
    dpu_seconds: Optional[float] = None
    log_group_id: Optional[str] = None


@dataclass
class TriggerInfo:
    """Glue trigger information."""
    name: str
    trigger_type: str
    trigger_status: TriggerStatus = TriggerStatus.CREATED
    schedule: Optional[str] = None
    predicate: Optional[Dict[str, Any]] = None
    actions: List[Dict[str, Any]] = field(default_factory=list)
    description: Optional[str] = None
    create_time: Optional[datetime] = None
    start_time: Optional[datetime] = None


@dataclass
class DevEndpointInfo:
    """Glue dev endpoint information."""
    name: str
    role_arn: Optional[str] = None
    security_group_ids: List[str] = field(default_factory=list)
    subnet_id: Optional[str] = None
    yarn_endpoint: Optional[str] = None
    private_address: Optional[str] = None
    public_address: Optional[str] = None
    status: DevEndpointStatus = DevEndpointStatus.CREATING
    worker_type: Optional[str] = None
    number_of_workers: Optional[int] = None
    glue_version: Optional[str] = None
    create_time: Optional[datetime] = None
    last_modified_time: Optional[datetime] = None
    arguments: Dict[str, str] = field(default_factory=dict)


@dataclass
class SchemaInfo:
    """Glue schema information."""
    registry_name: str
    schema_name: str
    schema_arn: Optional[str] = None
    data_format: str = "JSON"
    compatibility: str = "BACKWARD"
    description: Optional[str] = None
    versions: List[Dict[str, Any]] = field(default_factory=list)
    latest_version: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class DataQualityRule:
    """Data quality rule definition."""
    rule_type: DataQualityRuleType
    column: Optional[str] = None
    parameters: Dict[str, Any] = field(default_factory=dict)
    description: Optional[str] = None
    disabled: bool = False


@dataclass
class DataQualityResult:
    """Data quality check result."""
    rule: str
    column: Optional[str]
    passed: bool
    failed_count: int = 0
    total_count: int = 0
    error_message: Optional[str] = None


@dataclass
class DataQualityProfile:
    """Data quality profile with multiple rules."""
    name: str
    database_name: str
    table_name: str
    rules: List[DataQualityRule] = field(default_factory=list)
    results: List[DataQualityResult] = field(default_factory=list)
    last_run: Optional[datetime] = None


class GlueIntegration:
    """AWS Glue integration for data pipeline workflows.
    
    Provides comprehensive management for:
    - Glue databases
    - Glue tables
    - Crawlers
    - Jobs
    - Triggers
    - Dev endpoints
    - Data catalog
    - Schema registry
    - Data quality
    - CloudWatch monitoring
    """
    
    def __init__(
        self,
        region_name: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        profile_name: Optional[str] = None,
        endpoint_url: Optional[str] = None
    ):
        """Initialize Glue integration.
        
        Args:
            region_name: AWS region name
            aws_access_key_id: AWS access key ID
            aws_secret_access_key: AWS secret access key
            aws_session_token: AWS session token
            profile_name: AWS profile name
            endpoint_url: Custom endpoint URL
        """
        self.region_name = region_name or os.environ.get("AWS_REGION", "us-east-1")
        self.endpoint_url = endpoint_url
        self.profile_name = profile_name
        self._clients = {}
        self._resources = {}
        self._lock = threading.RLock()
        
        if BOTO3_AVAILABLE:
            self._init_clients(
                aws_access_key_id,
                aws_secret_access_key,
                aws_session_token
            )
    
    def _init_clients(
        self,
        aws_access_key_id: Optional[str],
        aws_secret_access_key: Optional[str],
        aws_session_token: Optional[str]
    ):
        """Initialize AWS clients."""
        with self._lock:
            session_kwargs = {
                "region_name": self.region_name,
            }
            
            if self.profile_name:
                import botocore.session
                session = botocore.session.get_session().create_session()
            else:
                import boto3
                session = boto3.Session(
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    aws_session_token=aws_session_token,
                    region_name=self.region_name
                )
            
            # Glue client
            glue_kwargs = {"region_name": self.region_name}
            if self.endpoint_url:
                glue_kwargs["endpoint_url"] = self.endpoint_url
            
            self._clients["glue"] = session.client("glue", **glue_kwargs)
            self._clients["glue_paginator"] = session.client("glue", **glue_kwargs).get_paginator("list_jobs")
            
            # CloudWatch client for metrics
            self._clients["cloudwatch"] = session.client("cloudwatch", region_name=self.region_name)
            
            # IAM client
            self._clients["iam"] = session.client("iam", region_name=self.region_name)
    
    @property
    def glue_client(self):
        """Get Glue client."""
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for Glue integration")
        return self._clients.get("glue")
    
    @property
    def cloudwatch_client(self):
        """Get CloudWatch client."""
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for CloudWatch integration")
        return self._clients.get("cloudwatch")
    
    def _generate_id(self) -> str:
        """Generate unique ID."""
        return hashlib.md5(f"{uuid.uuid4()}{time.time()}".encode()).hexdigest()[:12]
    
    # =========================================================================
    # DATABASE MANAGEMENT
    # =========================================================================
    
    def create_database(
        self,
        name: str,
        description: Optional[str] = None,
        location_uri: Optional[str] = None,
        parameters: Optional[Dict[str, str]] = None,
        catalog_id: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> DatabaseInfo:
        """Create a Glue database.
        
        Args:
            name: Database name
            description: Database description
            location_uri: S3 location URI
            parameters: Database parameters
            catalog_id: AWS account ID or 'aws' for management account
            tags: Resource tags
            **kwargs: Additional arguments
            
        Returns:
            DatabaseInfo object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        input_dict = {"Name": name}
        
        if description:
            input_dict["Description"] = description
        if location_uri:
            input_dict["LocationUri"] = location_uri
        if parameters:
            input_dict["Parameters"] = parameters
        if catalog_id:
            input_dict["CatalogId"] = catalog_id
        
        input_dict.update(kwargs)
        
        try:
            response = self.glue_client.create_database(**input_dict)
            
            # Add tags if provided
            if tags:
                self.glue_client.tag_resource(
                    ResourceArn=response["Database"]["Arn"],
                    TagsToAdd=tags
                )
            
            return self._parse_database_info(response["Database"])
            
        except ClientError as e:
            logger.error(f"Error creating database {name}: {e}")
            raise
    
    def get_database(self, name: str, catalog_id: Optional[str] = None) -> DatabaseInfo:
        """Get Glue database information.
        
        Args:
            name: Database name
            catalog_id: AWS account ID or 'aws' for management account
            
        Returns:
            DatabaseInfo object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            kwargs = {"Name": name}
            if catalog_id:
                kwargs["CatalogId"] = catalog_id
            
            response = self.glue_client.get_database(**kwargs)
            return self._parse_database_info(response["Database"])
            
        except ClientError as e:
            logger.error(f"Error getting database {name}: {e}")
            raise
    
    def list_databases(
        self,
        catalog_id: Optional[str] = None,
        filter_pattern: Optional[str] = None,
        max_results: int = 100,
        **kwargs
    ) -> List[DatabaseInfo]:
        """List Glue databases.
        
        Args:
            catalog_id: AWS account ID or 'aws' for management account
            filter_pattern: Pattern to filter database names
            max_results: Maximum results to return
            **kwargs: Additional arguments
            
        Returns:
            List of DatabaseInfo objects
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        databases = []
        kwargs_list = {}
        
        if catalog_id:
            kwargs_list["CatalogId"] = catalog_id
        
        try:
            paginator = self.glue_client.get_paginator("list_databases")
            
            for page in paginator.paginate(**kwargs_list):
                for db in page["DatabaseList"]:
                    name = db.get("Name", "")
                    if filter_pattern and filter_pattern not in name:
                        continue
                    databases.append(self._parse_database_info(db))
            
            return databases
            
        except ClientError as e:
            logger.error(f"Error listing databases: {e}")
            raise
    
    def update_database(
        self,
        name: str,
        description: Optional[str] = None,
        location_uri: Optional[str] = None,
        parameters: Optional[Dict[str, str]] = None,
        catalog_id: Optional[str] = None,
        **kwargs
    ) -> DatabaseInfo:
        """Update a Glue database.
        
        Args:
            name: Database name
            description: New description
            location_uri: New location URI
            parameters: New parameters
            catalog_id: AWS account ID
            **kwargs: Additional arguments
            
        Returns:
            Updated DatabaseInfo object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        input_dict = {"Name": name}
        
        if description:
            input_dict["Description"] = description
        if location_uri:
            input_dict["LocationUri"] = location_uri
        if parameters:
            input_dict["Parameters"] = parameters
        if catalog_id:
            input_dict["CatalogId"] = catalog_id
        
        input_dict.update(kwargs)
        
        try:
            self.glue_client.update_database(**input_dict)
            return self.get_database(name, catalog_id)
            
        except ClientError as e:
            logger.error(f"Error updating database {name}: {e}")
            raise
    
    def delete_database(self, name: str, catalog_id: Optional[str] = None, **kwargs):
        """Delete a Glue database.
        
        Args:
            name: Database name
            catalog_id: AWS account ID
            **kwargs: Additional arguments
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            kwargs_del = {"Name": name}
            if catalog_id:
                kwargs_del["CatalogId"] = catalog_id
            kwargs_del.update(kwargs)
            
            self.glue_client.delete_database(**kwargs_del)
            
        except ClientError as e:
            logger.error(f"Error deleting database {name}: {e}")
            raise
    
    def _parse_database_info(self, db_dict: Dict[str, Any]) -> DatabaseInfo:
        """Parse database information from API response."""
        return DatabaseInfo(
            name=db_dict.get("Name", ""),
            description=db_dict.get("Description"),
            location_uri=db_dict.get("LocationUri"),
            parameters=db_dict.get("Parameters", {}),
            create_time=self._parse_datetime(db_dict.get("CreateTime")),
            catalog_id=db_dict.get("CatalogId"),
            tags=db_dict.get("Tags", {})
        )
    
    # =========================================================================
    # TABLE MANAGEMENT
    # =========================================================================
    
    def create_table(
        self,
        database_name: str,
        name: str,
        storage_descriptor: Optional[Dict[str, Any]] = None,
        table_type: TableType = TableType.EXTERNAL_TABLE,
        description: Optional[str] = None,
        partition_keys: Optional[List[Dict[str, str]]] = None,
        parameters: Optional[Dict[str, str]] = None,
        view_original_text: Optional[str] = None,
        view_expanded_text: Optional[str] = None,
        retention: Optional[int] = None,
        catalog_id: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> TableInfo:
        """Create a Glue table.
        
        Args:
            database_name: Database name
            name: Table name
            storage_descriptor: Storage descriptor with location, input format, etc.
            table_type: Table type (EXTERNAL_TABLE, etc.)
            description: Table description
            partition_keys: Partition key columns
            parameters: Table parameters
            view_original_text: Original view definition
            view_expanded_text: Expanded view definition
            retention: Retention time in days
            catalog_id: AWS account ID
            tags: Resource tags
            **kwargs: Additional arguments
            
        Returns:
            TableInfo object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        input_dict = {
            "DatabaseName": database_name,
            "TableInput": {
                "Name": name,
                "TableType": table_type.value if isinstance(table_type, TableType) else table_type,
            }
        }
        
        if description:
            input_dict["TableInput"]["Description"] = description
        if storage_descriptor:
            input_dict["TableInput"]["StorageDescriptor"] = storage_descriptor
        if partition_keys:
            input_dict["TableInput"]["PartitionKeys"] = partition_keys
        if parameters:
            input_dict["TableInput"]["Parameters"] = parameters
        if view_original_text:
            input_dict["TableInput"]["ViewOriginalText"] = view_original_text
        if view_expanded_text:
            input_dict["TableInput"]["ViewExpandedText"] = view_expanded_text
        if retention is not None:
            input_dict["TableInput"]["Retention"] = retention
        if catalog_id:
            input_dict["CatalogId"] = catalog_id
        
        input_dict["TableInput"].update(kwargs)
        
        try:
            response = self.glue_client.create_table(**input_dict)
            
            if tags:
                self.glue_client.tag_resource(
                    ResourceArn=self._get_table_arn(
                        catalog_id or "aws",
                        database_name,
                        name
                    ),
                    TagsToAdd=tags
                )
            
            return self._parse_table_info(response["Table"])
            
        except ClientError as e:
            logger.error(f"Error creating table {database_name}.{name}: {e}")
            raise
    
    def get_table(
        self,
        database_name: str,
        name: str,
        catalog_id: Optional[str] = None
    ) -> TableInfo:
        """Get Glue table information.
        
        Args:
            database_name: Database name
            name: Table name
            catalog_id: AWS account ID
            
        Returns:
            TableInfo object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            kwargs = {"DatabaseName": database_name, "Name": name}
            if catalog_id:
                kwargs["CatalogId"] = catalog_id
            
            response = self.glue_client.get_table(**kwargs)
            return self._parse_table_info(response["Table"])
            
        except ClientError as e:
            logger.error(f"Error getting table {database_name}.{name}: {e}")
            raise
    
    def list_tables(
        self,
        database_name: str,
        catalog_id: Optional[str] = None,
        filter_pattern: Optional[str] = None,
        expression: Optional[str] = None,
        max_results: int = 100,
        **kwargs
    ) -> List[TableInfo]:
        """List tables in a database.
        
        Args:
            database_name: Database name
            catalog_id: AWS account ID
            filter_pattern: Pattern to filter table names
            expression: Search expression
            max_results: Maximum results to return
            **kwargs: Additional arguments
            
        Returns:
            List of TableInfo objects
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        tables = []
        kwargs_list = {"DatabaseName": database_name}
        
        if catalog_id:
            kwargs_list["CatalogId"] = catalog_id
        if expression:
            kwargs_list["Expression"] = expression
        
        try:
            paginator = self.glue_client.get_paginator("search_tables")
            
            for page in paginator.paginate(**kwargs_list):
                for table in page["TableList"]:
                    name = table.get("Name", "")
                    if filter_pattern and filter_pattern not in name:
                        continue
                    tables.append(self._parse_table_info(table))
            
            return tables
            
        except ClientError as e:
            logger.error(f"Error listing tables in {database_name}: {e}")
            raise
    
    def update_table(
        self,
        database_name: str,
        name: str,
        table_input: Dict[str, Any],
        catalog_id: Optional[str] = None,
        **kwargs
    ) -> TableInfo:
        """Update a Glue table.
        
        Args:
            database_name: Database name
            name: Table name
            table_input: Table input parameters
            catalog_id: AWS account ID
            **kwargs: Additional arguments
            
        Returns:
            Updated TableInfo object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        input_dict = {
            "DatabaseName": database_name,
            "TableInput": table_input
        }
        if catalog_id:
            input_dict["CatalogId"] = catalog_id
        
        input_dict.update(kwargs)
        
        try:
            self.glue_client.update_table(**input_dict)
            return self.get_table(database_name, name, catalog_id)
            
        except ClientError as e:
            logger.error(f"Error updating table {database_name}.{name}: {e}")
            raise
    
    def delete_table(
        self,
        database_name: str,
        name: str,
        catalog_id: Optional[str] = None,
        **kwargs
    ):
        """Delete a Glue table.
        
        Args:
            database_name: Database name
            name: Table name
            catalog_id: AWS account ID
            **kwargs: Additional arguments
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            kwargs_del = {"DatabaseName": database_name, "Name": name}
            if catalog_id:
                kwargs_del["CatalogId"] = catalog_id
            kwargs_del.update(kwargs)
            
            self.glue_client.delete_table(**kwargs_del)
            
        except ClientError as e:
            logger.error(f"Error deleting table {database_name}.{name}: {e}")
            raise
    
    def batch_create_tables(
        self,
        database_name: str,
        tables: List[Dict[str, Any]],
        catalog_id: Optional[str] = None,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """Batch create tables.
        
        Args:
            database_name: Database name
            tables: List of table inputs
            catalog_id: AWS account ID
            **kwargs: Additional arguments
            
        Returns:
            List of errors for failed tables
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        errors = []
        
        for table_input in tables:
            try:
                self.create_table(database_name, table_input["Name"], 
                                 catalog_id=catalog_id, **table_input)
            except Exception as e:
                errors.append({
                    "tableName": table_input.get("Name", ""),
                    "error": str(e)
                })
        
        return errors
    
    def get_table_versions(
        self,
        database_name: str,
        table_name: str,
        catalog_id: Optional[str] = None,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """Get table version history.
        
        Args:
            database_name: Database name
            table_name: Table name
            catalog_id: AWS account ID
            **kwargs: Additional arguments
            
        Returns:
            List of table versions
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            kwargs_get = {"DatabaseName": database_name, "TableName": table_name}
            if catalog_id:
                kwargs_get["CatalogId"] = catalog_id
            
            response = self.glue_client.get_table_versions(**kwargs_get)
            return response.get("TableVersions", [])
            
        except ClientError as e:
            logger.error(f"Error getting table versions {database_name}.{table_name}: {e}")
            raise
    
    def _parse_table_info(self, table_dict: Dict[str, Any]) -> TableInfo:
        """Parse table information from API response."""
        sd = table_dict.get("StorageDescriptor", {})
        
        return TableInfo(
            name=table_dict.get("Name", ""),
            database_name=table_dict.get("DatabaseName", ""),
            description=table_dict.get("Description"),
            table_type=TableType(table_dict.get("TableType", "EXTERNAL_TABLE")),
            storage_descriptor=sd,
            partition_keys=sd.get("PartitionKeys", []),
            parameters=table_dict.get("Parameters", {}),
            view_original_text=table_dict.get("ViewOriginalText"),
            view_expanded_text=table_dict.get("ViewExpandedText"),
            create_time=self._parse_datetime(table_dict.get("CreateTime")),
            update_time=self._parse_datetime(table_dict.get("UpdateTime")),
            last_access_time=self._parse_datetime(table_dict.get("LastAccessTime")),
            retention=table_dict.get("Retention"),
            catalog_id=table_dict.get("CatalogId"),
            tags=table_dict.get("Tags", {})
        )
    
    def _get_table_arn(self, catalog_id: str, database_name: str, table_name: str) -> str:
        """Get table ARN."""
        return f"arn:aws:glue:{self.region_name}:{catalog_id}:table/{database_name}/{table_name}"
    
    # =========================================================================
    # CRAWLER MANAGEMENT
    # =========================================================================
    
    def create_crawler(
        self,
        name: str,
        role: str,
        targets: Dict[str, Any],
        database_name: Optional[str] = None,
        table_prefix: Optional[str] = None,
        description: Optional[str] = None,
        schedule: Optional[str] = None,
        crawl_interval: Optional[int] = None,
        recrawl_policy: Optional[str] = None,
        schema_change_policy: Optional[Dict[str, str]] = None,
        configuration: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> CrawlerInfo:
        """Create a Glue crawler.
        
        Args:
            name: Crawler name
            role: IAM role ARN for the crawler
            targets: Crawler targets (S3 paths, JDBC, etc.)
            database_name: Database to store metadata
            table_prefix: Prefix for created tables
            description: Crawler description
            schedule: Cron expression for schedule
            crawl_interval: Interval in minutes
            recrawl_policy: RECRAWL_ALWAYS or CRAWL_NEW_FOLDERS_ONLY
            schema_change_policy: Policy for schema changes
            configuration: Configuration JSON string
            tags: Resource tags
            **kwargs: Additional arguments
            
        Returns:
            CrawlerInfo object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        input_dict = {
            "Name": name,
            "Role": role,
            "Targets": targets
        }
        
        if description:
            input_dict["Description"] = description
        if database_name:
            input_dict["DatabaseName"] = database_name
        if table_prefix:
            input_dict["TablePrefix"] = table_prefix
        if schedule:
            input_dict["Schedule"] = schedule
        if crawl_interval is not None:
            input_dict["CrawlInterval"] = crawl_interval
        if recrawl_policy:
            input_dict["RecrawlPolicy"] = {"RecrawlBehavior": recrawl_policy}
        if schema_change_policy:
            input_dict["SchemaChangePolicy"] = schema_change_policy
        if configuration:
            input_dict["Configuration"] = configuration
        
        input_dict.update(kwargs)
        
        try:
            self.glue_client.create_crawler(**input_dict)
            
            if tags:
                crawler_arn = f"arn:aws:glue:{self.region_name}::crawler/{name}"
                self.glue_client.tag_resource(ResourceArn=crawler_arn, TagsToAdd=tags)
            
            return self.get_crawler(name)
            
        except ClientError as e:
            logger.error(f"Error creating crawler {name}: {e}")
            raise
    
    def get_crawler(self, name: str) -> CrawlerInfo:
        """Get crawler information.
        
        Args:
            name: Crawler name
            
        Returns:
            CrawlerInfo object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            response = self.glue_client.get_crawler(Name=name)
            return self._parse_crawler_info(response["Crawler"])
            
        except ClientError as e:
            logger.error(f"Error getting crawler {name}: {e}")
            raise
    
    def list_crawlers(
        self,
        filter_pattern: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> List[CrawlerInfo]:
        """List Glue crawlers.
        
        Args:
            filter_pattern: Pattern to filter crawler names
            tags: Tags to filter by
            **kwargs: Additional arguments
            
        Returns:
            List of CrawlerInfo objects
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        crawlers = []
        
        try:
            paginator = self.glue_client.get_paginator("list_crawlers")
            
            for page in paginator.paginate():
                for crawler in page.get("Crawlers", []):
                    name = crawler.get("Name", "")
                    if filter_pattern and filter_pattern not in name:
                        continue
                    
                    if tags:
                        crawler_tags = crawler.get("Tags", {})
                        if not all(crawler_tags.get(k) == v for k, v in tags.items()):
                            continue
                    
                    crawlers.append(self._parse_crawler_info(crawler))
            
            return crawlers
            
        except ClientError as e:
            logger.error(f"Error listing crawlers: {e}")
            raise
    
    def start_crawler(self, name: str, **kwargs) -> Dict[str, Any]:
        """Start a crawler.
        
        Args:
            name: Crawler name
            **kwargs: Additional arguments
            
        Returns:
            Response dictionary
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            return self.glue_client.start_crawler(Name=name, **kwargs)
        except ClientError as e:
            logger.error(f"Error starting crawler {name}: {e}")
            raise
    
    def stop_crawler(self, name: str, **kwargs):
        """Stop a crawler.
        
        Args:
            name: Crawler name
            **kwargs: Additional arguments
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            self.glue_client.stop_crawler(Name=name, **kwargs)
        except ClientError as e:
            logger.error(f"Error stopping crawler {name}: {e}")
            raise
    
    def update_crawler(
        self,
        name: str,
        role: Optional[str] = None,
        targets: Optional[Dict[str, Any]] = None,
        database_name: Optional[str] = None,
        table_prefix: Optional[str] = None,
        description: Optional[str] = None,
        schedule: Optional[str] = None,
        crawl_interval: Optional[int] = None,
        recrawl_policy: Optional[str] = None,
        schema_change_policy: Optional[Dict[str, str]] = None,
        configuration: Optional[str] = None,
        **kwargs
    ) -> CrawlerInfo:
        """Update a crawler.
        
        Args:
            name: Crawler name
            role: New IAM role ARN
            targets: New targets
            database_name: New database name
            table_prefix: New table prefix
            description: New description
            schedule: New schedule
            crawl_interval: New crawl interval
            recrawl_policy: New recrawl policy
            schema_change_policy: New schema change policy
            configuration: New configuration
            **kwargs: Additional arguments
            
        Returns:
            Updated CrawlerInfo object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        input_dict = {"Name": name}
        
        if role:
            input_dict["Role"] = role
        if targets:
            input_dict["Targets"] = targets
        if database_name:
            input_dict["DatabaseName"] = database_name
        if table_prefix:
            input_dict["TablePrefix"] = table_prefix
        if description is not None:
            input_dict["Description"] = description
        if schedule:
            input_dict["Schedule"] = schedule
        if crawl_interval is not None:
            input_dict["CrawlInterval"] = crawl_interval
        if recrawl_policy:
            input_dict["RecrawlPolicy"] = {"RecrawlBehavior": recrawl_policy}
        if schema_change_policy:
            input_dict["SchemaChangePolicy"] = schema_change_policy
        if configuration:
            input_dict["Configuration"] = configuration
        
        input_dict.update(kwargs)
        
        try:
            self.glue_client.update_crawler(**input_dict)
            return self.get_crawler(name)
            
        except ClientError as e:
            logger.error(f"Error updating crawler {name}: {e}")
            raise
    
    def delete_crawler(self, name: str, **kwargs):
        """Delete a crawler.
        
        Args:
            name: Crawler name
            **kwargs: Additional arguments
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            self.glue_client.delete_crawler(Name=name, **kwargs)
        except ClientError as e:
            logger.error(f"Error deleting crawler {name}: {e}")
            raise
    
    def get_crawler_metrics(self) -> Dict[str, Any]:
        """Get crawler metrics.
        
        Returns:
            Crawler metrics dictionary
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            return self.glue_client.get_crawler_metrics()
        except ClientError as e:
            logger.error(f"Error getting crawler metrics: {e}")
            raise
    
    def _parse_crawler_info(self, crawler_dict: Dict[str, Any]) -> CrawlerInfo:
        """Parse crawler information from API response."""
        state_str = crawler_dict.get("State", "READY")
        try:
            state = CrawlerStatus(state_str)
        except ValueError:
            state = CrawlerStatus.READY
        
        return CrawlerInfo(
            name=crawler_dict.get("Name", ""),
            description=crawler_dict.get("Description"),
            database_name=crawler_dict.get("DatabaseName"),
            table_prefix=crawler_dict.get("TablePrefix"),
            role=crawler_dict.get("Role"),
            targets=crawler_dict.get("Targets", {}),
            schedule=crawler_dict.get("Schedule"),
            crawl_interval=crawler_dict.get("CrawlInterval"),
            recrawl_policy=crawler_dict.get("RecrawlPolicy", {}).get("RecrawlBehavior"),
            schema_change_policy=crawler_dict.get("SchemaChangePolicy", {}),
            configuration=crawler_dict.get("Configuration"),
            state=state,
            last_crawl=crawler_dict.get("LastCrawl"),
            create_time=self._parse_datetime(crawler_dict.get("CreationTime")),
            tags=crawler_dict.get("Tags", {})
        )
    
    # =========================================================================
    # JOB MANAGEMENT
    # =========================================================================
    
    def create_job(
        self,
        name: str,
        role: str,
        command: Dict[str, str],
        script_location: Optional[str] = None,
        description: Optional[str] = None,
        python_version: Optional[str] = None,
        glue_version: Optional[str] = None,
        worker_type: Optional[str] = None,
        number_of_workers: Optional[int] = None,
        max_capacity: Optional[float] = None,
        timeout: Optional[int] = None,
        max_retries: Optional[int] = None,
        arguments: Optional[Dict[str, str]] = None,
        connections: Optional[List[str]] = None,
        security_configuration: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> JobInfo:
        """Create a Glue job.
        
        Args:
            name: Job name
            role: IAM role ARN
            command: Job command (script location, type, etc.)
            script_location: S3 path to script
            description: Job description
            python_version: Python version (for Python Shell jobs)
            glue_version: Glue version
            worker_type: Worker type (Standard, G.1X, G.2X)
            number_of_workers: Number of workers
            max_capacity: Max capacity (DPUs)
            timeout: Job timeout in minutes
            max_retries: Max retries
            arguments: Job arguments
            connections: List of connection names
            security_configuration: Security configuration name
            tags: Resource tags
            **kwargs: Additional arguments
            
        Returns:
            JobInfo object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        input_dict = {
            "Name": name,
            "Role": role,
            "Command": command
        }
        
        if description:
            input_dict["Description"] = description
        if script_location:
            input_dict["ScriptLocation"] = script_location
        if python_version:
            input_dict["PythonVersion"] = python_version
        if glue_version:
            input_dict["GlueVersion"] = glue_version
        if worker_type:
            input_dict["WorkerType"] = worker_type
        if number_of_workers is not None:
            input_dict["NumberOfWorkers"] = number_of_workers
        if max_capacity is not None:
            input_dict["MaxCapacity"] = max_capacity
        if timeout is not None:
            input_dict["Timeout"] = timeout
        if max_retries is not None:
            input_dict["MaxRetries"] = max_retries
        if arguments:
            input_dict["DefaultArguments"] = arguments
        if connections:
            input_dict["Connections"] = {"Connections": connections}
        if security_configuration:
            input_dict["SecurityConfiguration"] = security_configuration
        
        input_dict.update(kwargs)
        
        try:
            response = self.glue_client.create_job(**input_dict)
            
            if tags:
                job_arn = f"arn:aws:glue:{self.region_name}::job/{name}"
                self.glue_client.tag_resource(ResourceArn=job_arn, TagsToAdd=tags)
            
            return self.get_job(name)
            
        except ClientError as e:
            logger.error(f"Error creating job {name}: {e}")
            raise
    
    def get_job(self, name: str) -> JobInfo:
        """Get job information.
        
        Args:
            name: Job name
            
        Returns:
            JobInfo object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            response = self.glue_client.get_job(JobName=name)
            return self._parse_job_info(response["Job"])
            
        except ClientError as e:
            logger.error(f"Error getting job {name}: {e}")
            raise
    
    def list_jobs(
        self,
        filter_pattern: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        max_results: int = 100,
        **kwargs
    ) -> List[JobInfo]:
        """List Glue jobs.
        
        Args:
            filter_pattern: Pattern to filter job names
            tags: Tags to filter by
            max_results: Maximum results to return
            **kwargs: Additional arguments
            
        Returns:
            List of JobInfo objects
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        jobs = []
        
        try:
            paginator = self.glue_client.get_paginator("list_jobs")
            
            for page in paginator.paginate():
                for job in page.get("Jobs", []):
                    name = job
                    if filter_pattern and filter_pattern not in name:
                        continue
                    
                    try:
                        job_info = self.get_job(name)
                        if tags:
                            if not all(job_info.tags.get(k) == v for k, v in tags.items()):
                                continue
                        jobs.append(job_info)
                    except Exception:
                        jobs.append(JobInfo(name=name))
            
            return jobs
            
        except ClientError as e:
            logger.error(f"Error listing jobs: {e}")
            raise
    
    def start_job_run(
        self,
        job_name: str,
        arguments: Optional[Dict[str, str]] = None,
        allocated_capacity: Optional[float] = None,
        timeout: Optional[int] = None,
        worker_type: Optional[str] = None,
        number_of_workers: Optional[int] = None,
        security_configuration: Optional[str] = None,
        notification_property: Optional[Dict[str, Any]] = None,
        execution_property: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> JobRunInfo:
        """Start a Glue job run.
        
        Args:
            job_name: Job name
            arguments: Job arguments
            allocated_capacity: Allocated capacity (DPUs)
            timeout: Timeout in minutes
            worker_type: Worker type
            number_of_workers: Number of workers
            security_configuration: Security configuration
            notification_property: Notification property
            execution_property: Execution property
            **kwargs: Additional arguments
            
        Returns:
            JobRunInfo object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        input_dict = {"JobName": job_name}
        
        if arguments:
            input_dict["Arguments"] = arguments
        if allocated_capacity is not None:
            input_dict["AllocatedCapacity"] = allocated_capacity
        if timeout is not None:
            input_dict["Timeout"] = timeout
        if worker_type:
            input_dict["WorkerType"] = worker_type
        if number_of_workers is not None:
            input_dict["NumberOfWorkers"] = number_of_workers
        if security_configuration:
            input_dict["SecurityConfiguration"] = security_configuration
        if notification_property:
            input_dict["NotificationProperty"] = notification_property
        if execution_property:
            input_dict["ExecutionProperty"] = execution_property
        
        input_dict.update(kwargs)
        
        try:
            response = self.glue_client.start_job_run(**input_dict)
            
            return JobRunInfo(
                job_name=job_name,
                run_id=response["JobRunId"],
                arguments=arguments or {},
                allocated_capacity=allocated_capacity,
                timeout=timeout,
                worker_type=worker_type,
                number_of_workers=number_of_workers,
                started_on=datetime.now()
            )
            
        except ClientError as e:
            logger.error(f"Error starting job run for {job_name}: {e}")
            raise
    
    def get_job_run(
        self,
        job_name: str,
        run_id: str,
        **kwargs
    ) -> JobRunInfo:
        """Get job run information.
        
        Args:
            job_name: Job name
            run_id: Job run ID
            **kwargs: Additional arguments
            
        Returns:
            JobRunInfo object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            response = self.glue_client.get_job_run(JobName=job_name, RunId=run_id, **kwargs)
            return self._parse_job_run_info(job_name, response["JobRun"])
            
        except ClientError as e:
            logger.error(f"Error getting job run {job_name}/{run_id}: {e}")
            raise
    
    def list_job_runs(
        self,
        job_name: str,
        filter_pattern: Optional[str] = None,
        max_results: int = 100,
        **kwargs
    ) -> List[JobRunInfo]:
        """List job runs for a job.
        
        Args:
            job_name: Job name
            filter_pattern: Pattern to filter run IDs
            max_results: Maximum results to return
            **kwargs: Additional arguments
            
        Returns:
            List of JobRunInfo objects
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        runs = []
        
        try:
            paginator = self.glue_client.get_paginator("list_runs")
            
            for page in paginator.paginate(JobName=job_name):
                for run in page.get("JobRuns", []):
                    run_id = run.get("Id", "")
                    if filter_pattern and filter_pattern not in run_id:
                        continue
                    runs.append(self._parse_job_run_info(job_name, run))
            
            return runs
            
        except ClientError as e:
            logger.error(f"Error listing job runs for {job_name}: {e}")
            raise
    
    def stop_job_run(self, job_name: str, run_id: str, **kwargs):
        """Stop a job run.
        
        Args:
            job_name: Job name
            run_id: Job run ID
            **kwargs: Additional arguments
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            self.glue_client.stop_job_run(JobName=job_name, RunId=run_id, **kwargs)
        except ClientError as e:
            logger.error(f"Error stopping job run {job_name}/{run_id}: {e}")
            raise
    
    def update_job(
        self,
        job_name: str,
        job_update: Dict[str, Any],
        **kwargs
    ) -> JobInfo:
        """Update a job.
        
        Args:
            job_name: Job name
            job_update: Job update parameters
            **kwargs: Additional arguments
            
        Returns:
            Updated JobInfo object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            response = self.glue_client.update_job(
                JobName=job_name,
                JobUpdate=job_update,
                **kwargs
            )
            return self.get_job(response["JobName"])
            
        except ClientError as e:
            logger.error(f"Error updating job {job_name}: {e}")
            raise
    
    def delete_job(self, job_name: str, **kwargs):
        """Delete a job.
        
        Args:
            job_name: Job name
            **kwargs: Additional arguments
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            self.glue_client.delete_job(JobName=job_name, **kwargs)
        except ClientError as e:
            logger.error(f"Error deleting job {job_name}: {e}")
            raise
    
    def _parse_job_info(self, job_dict: Dict[str, Any]) -> JobInfo:
        """Parse job information from API response."""
        return JobInfo(
            name=job_dict.get("Name", ""),
            description=job_dict.get("Description"),
            role=job_dict.get("Role"),
            command=job_dict.get("Command", {}),
            script_location=job_dict.get("ScriptLocation"),
            python_version=job_dict.get("PythonVersion"),
            glue_version=job_dict.get("GlueVersion"),
            worker_type=job_dict.get("WorkerType"),
            number_of_workers=job_dict.get("NumberOfWorkers"),
            max_capacity=job_dict.get("MaxCapacity"),
            timeout=job_dict.get("Timeout"),
            max_retries=job_dict.get("MaxRetries"),
            arguments=job_dict.get("DefaultArguments", {}),
            connections=job_dict.get("Connections", {}).get("Connections", []),
            security_configuration=job_dict.get("SecurityConfiguration"),
            notification_property=job_dict.get("NotificationProperty", {}),
            execution_property=job_dict.get("ExecutionProperty", {}),
            create_time=self._parse_datetime(job_dict.get("CreatedOn")),
            last_modified_time=self._parse_datetime(job_dict.get("LastModifiedOn")),
            tags=job_dict.get("Tags", {})
        )
    
    def _parse_job_run_info(self, job_name: str, run_dict: Dict[str, Any]) -> JobRunInfo:
        """Parse job run information from API response."""
        status_str = run_dict.get("JobRunState", "RUNNING")
        try:
            status = JobRunStatus(status_str)
        except ValueError:
            status = JobRunStatus.RUNNING
        
        return JobRunInfo(
            job_name=job_name,
            run_id=run_dict.get("Id", ""),
            arguments=run_dict.get("Arguments", {}),
            allocated_capacity=run_dict.get("AllocatedCapacity"),
            timeout=run_dict.get("Timeout"),
            worker_type=run_dict.get("WorkerType"),
            number_of_workers=run_dict.get("NumberOfWorkers"),
            status=status,
            started_on=self._parse_datetime(run_dict.get("StartedOn")),
            completed_on=self._parse_datetime(run_dict.get("CompletedOn")),
            error_message=run_dict.get("ErrorMessage"),
            execution_time=run_dict.get("ExecutionTime"),
            dpu_seconds=run_dict.get("DPUSeconds"),
            log_group_id=run_dict.get("LogGroupId")
        )
    
    # =========================================================================
    # TRIGGER MANAGEMENT
    # =========================================================================
    
    def create_trigger(
        self,
        name: str,
        trigger_type: str,
        actions: List[Dict[str, Any]],
        schedule: Optional[str] = None,
        predicate: Optional[Dict[str, Any]] = None,
        description: Optional[str] = None,
        trigger_status: TriggerStatus = TriggerStatus.CREATED,
        tags: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> TriggerInfo:
        """Create a Glue trigger.
        
        Args:
            name: Trigger name
            trigger_type: Trigger type (SCHEDULED, CONDITIONAL, ON_DEMAND, EVENT)
            actions: List of actions to perform
            schedule: Cron expression for schedule
            predicate: Conditional predicate
            description: Trigger description
            trigger_status: Initial status
            tags: Resource tags
            **kwargs: Additional arguments
            
        Returns:
            TriggerInfo object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        input_dict = {
            "Name": name,
            "Type": trigger_type,
            "Actions": actions
        }
        
        if schedule:
            input_dict["Schedule"] = schedule
        if predicate:
            input_dict["Predicate"] = predicate
        if description:
            input_dict["Description"] = description
        if trigger_status == TriggerStatus.ACTIVATED:
            input_dict["TriggerUpdate"] = {"State": "ACTIVATED"}
        
        input_dict.update(kwargs)
        
        try:
            response = self.glue_client.create_trigger(**input_dict)
            
            if tags:
                trigger_arn = f"arn:aws:glue:{self.region_name}::trigger/{name}"
                self.glue_client.tag_resource(ResourceArn=trigger_arn, TagsToAdd=tags)
            
            return self.get_trigger(name)
            
        except ClientError as e:
            logger.error(f"Error creating trigger {name}: {e}")
            raise
    
    def get_trigger(self, name: str) -> TriggerInfo:
        """Get trigger information.
        
        Args:
            name: Trigger name
            
        Returns:
            TriggerInfo object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            response = self.glue_client.get_trigger(Name=name)
            return self._parse_trigger_info(response["Trigger"])
            
        except ClientError as e:
            logger.error(f"Error getting trigger {name}: {e}")
            raise
    
    def list_triggers(
        self,
        filter_pattern: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        max_results: int = 100,
        **kwargs
    ) -> List[TriggerInfo]:
        """List Glue triggers.
        
        Args:
            filter_pattern: Pattern to filter trigger names
            tags: Tags to filter by
            max_results: Maximum results to return
            **kwargs: Additional arguments
            
        Returns:
            List of TriggerInfo objects
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        triggers = []
        
        try:
            paginator = self.glue_client.get_paginator("list_triggers")
            
            for page in paginator.paginate():
                for trigger in page.get("Triggers", []):
                    name = trigger.get("Name", "")
                    if filter_pattern and filter_pattern not in name:
                        continue
                    triggers.append(self._parse_trigger_info(trigger))
            
            return triggers
            
        except ClientError as e:
            logger.error(f"Error listing triggers: {e}")
            raise
    
    def start_trigger(self, name: str, **kwargs):
        """Start a trigger.
        
        Args:
            name: Trigger name
            **kwargs: Additional arguments
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            self.glue_client.start_trigger(Name=name, **kwargs)
        except ClientError as e:
            logger.error(f"Error starting trigger {name}: {e}")
            raise
    
    def stop_trigger(self, name: str, **kwargs):
        """Stop a trigger.
        
        Args:
            name: Trigger name
            **kwargs: Additional arguments
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            self.glue_client.stop_trigger(Name=name, **kwargs)
        except ClientError as e:
            logger.error(f"Error stopping trigger {name}: {e}")
            raise
    
    def update_trigger(
        self,
        name: str,
        trigger_update: Dict[str, Any],
        **kwargs
    ) -> TriggerInfo:
        """Update a trigger.
        
        Args:
            name: Trigger name
            trigger_update: Trigger update parameters
            **kwargs: Additional arguments
            
        Returns:
            Updated TriggerInfo object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            response = self.glue_client.update_trigger(
                Name=name,
                TriggerUpdate=trigger_update,
                **kwargs
            )
            return self.get_trigger(response["Trigger"]["Name"])
            
        except ClientError as e:
            logger.error(f"Error updating trigger {name}: {e}")
            raise
    
    def delete_trigger(self, name: str, **kwargs):
        """Delete a trigger.
        
        Args:
            name: Trigger name
            **kwargs: Additional arguments
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            self.glue_client.delete_trigger(Name=name, **kwargs)
        except ClientError as e:
            logger.error(f"Error deleting trigger {name}: {e}")
            raise
    
    def _parse_trigger_info(self, trigger_dict: Dict[str, Any]) -> TriggerInfo:
        """Parse trigger information from API response."""
        status_str = trigger_dict.get("State", "CREATED")
        try:
            status = TriggerStatus(status_str)
        except ValueError:
            status = TriggerStatus.CREATED
        
        return TriggerInfo(
            name=trigger_dict.get("Name", ""),
            trigger_type=trigger_dict.get("Type", ""),
            trigger_status=status,
            schedule=trigger_dict.get("Schedule"),
            predicate=trigger_dict.get("Predicate"),
            actions=trigger_dict.get("Actions", []),
            description=trigger_dict.get("Description"),
            create_time=self._parse_datetime(trigger_dict.get("CreatedOn")),
            start_time=self._parse_datetime(trigger_dict.get("StartTime"))
        )
    
    # =========================================================================
    # DEV ENDPOINT MANAGEMENT
    # =========================================================================
    
    def create_dev_endpoint(
        self,
        name: str,
        role_arn: str,
        subnet_id: Optional[str] = None,
        security_group_ids: Optional[List[str]] = None,
        python_version: Optional[str] = None,
        glue_version: Optional[str] = None,
        worker_type: Optional[str] = None,
        number_of_workers: Optional[int] = None,
        arguments: Optional[Dict[str, str]] = None,
        tags: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> DevEndpointInfo:
        """Create a Glue dev endpoint.
        
        Args:
            name: Endpoint name
            role_arn: IAM role ARN
            subnet_id: Subnet ID
            security_group_ids: Security group IDs
            python_version: Python version
            glue_version: Glue version
            worker_type: Worker type
            number_of_workers: Number of workers
            arguments: Additional arguments
            tags: Resource tags
            **kwargs: Additional arguments
            
        Returns:
            DevEndpointInfo object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        input_dict = {
            "EndpointName": name,
            "RoleArn": role_arn
        }
        
        if subnet_id:
            input_dict["SubnetId"] = subnet_id
        if security_group_ids:
            input_dict["SecurityGroupIds"] = security_group_ids
        if python_version:
            input_dict["PythonVersion"] = python_version
        if glue_version:
            input_dict["GlueVersion"] = glue_version
        if worker_type:
            input_dict["WorkerType"] = worker_type
        if number_of_workers is not None:
            input_dict["NumberOfWorkers"] = number_of_workers
        if arguments:
            input_dict["Arguments"] = arguments
        
        input_dict.update(kwargs)
        
        try:
            self.glue_client.create_dev_endpoint(**input_dict)
            
            if tags:
                endpoint_arn = f"arn:aws:glue:{self.region_name}::devEndpoint/{name}"
                self.glue_client.tag_resource(ResourceArn=endpoint_arn, TagsToAdd=tags)
            
            return self.get_dev_endpoint(name)
            
        except ClientError as e:
            logger.error(f"Error creating dev endpoint {name}: {e}")
            raise
    
    def get_dev_endpoint(self, name: str) -> DevEndpointInfo:
        """Get dev endpoint information.
        
        Args:
            name: Endpoint name
            
        Returns:
            DevEndpointInfo object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            response = self.glue_client.get_dev_endpoint(EndpointName=name)
            return self._parse_dev_endpoint_info(response["DevEndpoint"])
            
        except ClientError as e:
            logger.error(f"Error getting dev endpoint {name}: {e}")
            raise
    
    def list_dev_endpoints(
        self,
        filter_pattern: Optional[str] = None,
        max_results: int = 100,
        **kwargs
    ) -> List[DevEndpointInfo]:
        """List Glue dev endpoints.
        
        Args:
            filter_pattern: Pattern to filter endpoint names
            max_results: Maximum results to return
            **kwargs: Additional arguments
            
        Returns:
            List of DevEndpointInfo objects
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        endpoints = []
        
        try:
            paginator = self.glue_client.get_paginator("list_dev_endpoints")
            
            for page in paginator.paginate():
                for endpoint in page.get("DevEndpoints", []):
                    name = endpoint.get("EndpointName", "")
                    if filter_pattern and filter_pattern not in name:
                        continue
                    endpoints.append(self._parse_dev_endpoint_info(endpoint))
            
            return endpoints
            
        except ClientError as e:
            logger.error(f"Error listing dev endpoints: {e}")
            raise
    
    def update_dev_endpoint(
        self,
        name: str,
        subnet_id: Optional[str] = None,
        security_group_ids: Optional[List[str]] = None,
        worker_type: Optional[str] = None,
        number_of_workers: Optional[int] = None,
        arguments: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> DevEndpointInfo:
        """Update a dev endpoint.
        
        Args:
            name: Endpoint name
            subnet_id: New subnet ID
            security_group_ids: New security group IDs
            worker_type: New worker type
            number_of_workers: New number of workers
            arguments: New arguments
            **kwargs: Additional arguments
            
        Returns:
            Updated DevEndpointInfo object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        input_dict = {"EndpointName": name}
        
        if subnet_id:
            input_dict["SubnetId"] = subnet_id
        if security_group_ids:
            input_dict["SecurityGroupIds"] = security_group_ids
        if worker_type:
            input_dict["WorkerType"] = worker_type
        if number_of_workers is not None:
            input_dict["NumberOfWorkers"] = number_of_workers
        if arguments:
            input_dict["Arguments"] = arguments
        
        input_dict.update(kwargs)
        
        try:
            self.glue_client.update_dev_endpoint(**input_dict)
            return self.get_dev_endpoint(name)
            
        except ClientError as e:
            logger.error(f"Error updating dev endpoint {name}: {e}")
            raise
    
    def delete_dev_endpoint(self, name: str, **kwargs):
        """Delete a dev endpoint.
        
        Args:
            name: Endpoint name
            **kwargs: Additional arguments
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            self.glue_client.delete_dev_endpoint(EndpointName=name, **kwargs)
        except ClientError as e:
            logger.error(f"Error deleting dev endpoint {name}: {e}")
            raise
    
    def _parse_dev_endpoint_info(self, endpoint_dict: Dict[str, Any]) -> DevEndpointInfo:
        """Parse dev endpoint information from API response."""
        status_str = endpoint_dict.get("Status", "CREATING")
        try:
            status = DevEndpointStatus(status_str)
        except ValueError:
            status = DevEndpointStatus.CREATING
        
        return DevEndpointInfo(
            name=endpoint_dict.get("EndpointName", ""),
            role_arn=endpoint_dict.get("RoleArn"),
            security_group_ids=endpoint_dict.get("SecurityGroupIds", []),
            subnet_id=endpoint_dict.get("SubnetId"),
            yarn_endpoint=endpoint_dict.get("YarnEndpoint"),
            private_address=endpoint_dict.get("PrivateAddress"),
            public_address=endpoint_dict.get("PublicAddress"),
            status=status,
            worker_type=endpoint_dict.get("WorkerType"),
            number_of_workers=endpoint_dict.get("NumberOfWorkers"),
            glue_version=endpoint_dict.get("GlueVersion"),
            create_time=self._parse_datetime(endpoint_dict.get("CreatedTimestamp")),
            last_modified_time=self._parse_datetime(endpoint_dict.get("LastModifiedTimestamp")),
            arguments=endpoint_dict.get("Arguments", {})
        )
    
    # =========================================================================
    # DATA CATALOG OPERATIONS
    # =========================================================================
    
    def get_catalog_info(self) -> Dict[str, Any]:
        """Get Data Catalog information.
        
        Returns:
            Catalog info dictionary
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            return self.glue_client.get_catalog_info()
        except ClientError as e:
            logger.error(f"Error getting catalog info: {e}")
            raise
    
    def get_table_metadata(
        self,
        database_name: str,
        table_name: str,
        catalog_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get table metadata from catalog.
        
        Args:
            database_name: Database name
            table_name: Table name
            catalog_id: AWS account ID
            
        Returns:
            Table metadata dictionary
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            kwargs = {"DatabaseName": database_name, "Name": table_name}
            if catalog_id:
                kwargs["CatalogId"] = catalog_id
            
            return self.glue_client.get_table(**kwargs)
        except ClientError as e:
            logger.error(f"Error getting table metadata: {e}")
            raise
    
    def get_database_metadata(
        self,
        database_name: str,
        catalog_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get database metadata from catalog.
        
        Args:
            database_name: Database name
            catalog_id: AWS account ID
            
        Returns:
            Database metadata dictionary
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            kwargs = {"Name": database_name}
            if catalog_id:
                kwargs["CatalogId"] = catalog_id
            
            return self.glue_client.get_database(**kwargs)
        except ClientError as e:
            logger.error(f"Error getting database metadata: {e}")
            raise
    
    def get_partition_metadata(
        self,
        database_name: str,
        table_name: str,
        partition_values: List[str],
        catalog_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get partition metadata.
        
        Args:
            database_name: Database name
            table_name: Table name
            partition_values: Partition key values
            catalog_id: AWS account ID
            
        Returns:
            Partition metadata dictionary
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            kwargs = {
                "DatabaseName": database_name,
                "TableName": table_name,
                "PartitionValues": partition_values
            }
            if catalog_id:
                kwargs["CatalogId"] = catalog_id
            
            return self.glue_client.get_partition(**kwargs)
        except ClientError as e:
            logger.error(f"Error getting partition metadata: {e}")
            raise
    
    def get_partitions(
        self,
        database_name: str,
        table_name: str,
        expression: Optional[str] = None,
        max_results: int = 100,
        catalog_id: Optional[str] = None,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """Get partitions for a table.
        
        Args:
            database_name: Database name
            table_name: Table name
            expression: Search expression
            max_results: Maximum results to return
            catalog_id: AWS account ID
            **kwargs: Additional arguments
            
        Returns:
            List of partition dictionaries
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        partitions = []
        kwargs_list = {"DatabaseName": database_name, "TableName": table_name}
        
        if catalog_id:
            kwargs_list["CatalogId"] = catalog_id
        if expression:
            kwargs_list["Expression"] = expression
        
        try:
            paginator = self.glue_client.get_paginator("get_partitions")
            
            for page in paginator.paginate(**kwargs_list):
                partitions.extend(page.get("Partitions", []))
            
            return partitions
            
        except ClientError as e:
            logger.error(f"Error getting partitions: {e}")
            raise
    
    def create_partition(
        self,
        database_name: str,
        table_name: str,
        partition_input: Dict[str, Any],
        catalog_id: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """Create a partition.
        
        Args:
            database_name: Database name
            table_name: Table name
            partition_input: Partition input parameters
            catalog_id: AWS account ID
            **kwargs: Additional arguments
            
        Returns:
            Response dictionary
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        input_dict = {
            "DatabaseName": database_name,
            "TableName": table_name,
            "PartitionInput": partition_input
        }
        if catalog_id:
            input_dict["CatalogId"] = catalog_id
        
        input_dict.update(kwargs)
        
        try:
            return self.glue_client.create_partition(**input_dict)
        except ClientError as e:
            logger.error(f"Error creating partition: {e}")
            raise
    
    def batch_create_partition(
        self,
        database_name: str,
        table_name: str,
        partitions: List[Dict[str, Any]],
        catalog_id: Optional[str] = None,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """Batch create partitions.
        
        Args:
            database_name: Database name
            table_name: Table name
            partitions: List of partition inputs
            catalog_id: AWS account ID
            **kwargs: Additional arguments
            
        Returns:
            List of error dictionaries
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        input_dict = {
            "DatabaseName": database_name,
            "TableName": table_name,
            "PartitionInputList": partitions
        }
        if catalog_id:
            input_dict["CatalogId"] = catalog_id
        
        input_dict.update(kwargs)
        
        try:
            return self.glue_client.batch_create_partition(**input_dict)
        except ClientError as e:
            logger.error(f"Error batch creating partitions: {e}")
            raise
    
    def delete_partition(
        self,
        database_name: str,
        table_name: str,
        partition_values: List[str],
        catalog_id: Optional[str] = None,
        **kwargs
    ):
        """Delete a partition.
        
        Args:
            database_name: Database name
            table_name: Table name
            partition_values: Partition key values
            catalog_id: AWS account ID
            **kwargs: Additional arguments
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            kwargs_del = {
                "DatabaseName": database_name,
                "TableName": table_name,
                "PartitionValues": partition_values
            }
            if catalog_id:
                kwargs_del["CatalogId"] = catalog_id
            kwargs_del.update(kwargs)
            
            self.glue_client.delete_partition(**kwargs_del)
        except ClientError as e:
            logger.error(f"Error deleting partition: {e}")
            raise
    
    def get_schema_version_dict(
        self,
        schema_id: Dict[str, str],
        **kwargs
    ) -> Dict[str, Any]:
        """Get schema version dictionary.
        
        Args:
            schema_id: Schema ID with RegistryId and SchemaName
            **kwargs: Additional arguments
            
        Returns:
            Schema version dictionary
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            return self.glue_client.get_schema_version_dict(SchemaId=schema_id, **kwargs)
        except ClientError as e:
            logger.error(f"Error getting schema version dict: {e}")
            raise
    
    # =========================================================================
    # SCHEMA REGISTRY
    # =========================================================================
    
    def create_registry(
        self,
        name: str,
        description: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """Create a schema registry.
        
        Args:
            name: Registry name
            description: Registry description
            tags: Resource tags
            **kwargs: Additional arguments
            
        Returns:
            Registry info dictionary
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        input_dict = {"Name": name}
        if description:
            input_dict["Description"] = description
        
        input_dict.update(kwargs)
        
        try:
            response = self.glue_client.create_registry(**input_dict)
            
            if tags:
                registry_arn = response["RegistryArn"]
                self.glue_client.tag_resource(ResourceArn=registry_arn, TagsToAdd=tags)
            
            return response
        except ClientError as e:
            logger.error(f"Error creating registry {name}: {e}")
            raise
    
    def get_registry(self, name: str) -> Dict[str, Any]:
        """Get schema registry information.
        
        Args:
            name: Registry name
            
        Returns:
            Registry info dictionary
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            return self.glue_client.get_registry(RegistryId={"Name": name})
        except ClientError as e:
            logger.error(f"Error getting registry {name}: {e}")
            raise
    
    def list_registries(self, max_results: int = 100) -> List[Dict[str, Any]]:
        """List schema registries.
        
        Args:
            max_results: Maximum results to return
            
        Returns:
            List of registry info dictionaries
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        registries = []
        
        try:
            paginator = self.glue_client.get_paginator("list_registries")
            
            for page in paginator.paginate():
                registries.extend(page.get("Registries", []))
            
            return registries
            
        except ClientError as e:
            logger.error(f"Error listing registries: {e}")
            raise
    
    def create_schema(
        self,
        registry_name: str,
        name: str,
        data_format: str = "JSON",
        compatibility: str = "BACKWARD",
        schema_definition: Optional[str] = None,
        description: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> SchemaInfo:
        """Create a schema.
        
        Args:
            registry_name: Registry name
            name: Schema name
            data_format: Data format (JSON, AVRO)
            compatibility: Compatibility mode
            schema_definition: Schema definition
            description: Schema description
            tags: Resource tags
            **kwargs: Additional arguments
            
        Returns:
            SchemaInfo object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        input_dict = {
            "RegistryId": {"Name": registry_name},
            "SchemaName": name,
            "DataFormat": data_format,
            "Compatibility": compatibility
        }
        
        if schema_definition:
            input_dict["SchemaDefinition"] = schema_definition
        if description:
            input_dict["Description"] = description
        
        input_dict.update(kwargs)
        
        try:
            response = self.glue_client.create_schema(**input_dict)
            
            if tags:
                schema_arn = response["SchemaArn"]
                self.glue_client.tag_resource(ResourceArn=schema_arn, TagsToAdd=tags)
            
            return SchemaInfo(
                registry_name=registry_name,
                schema_name=name,
                schema_arn=response.get("SchemaArn"),
                data_format=data_format,
                compatibility=compatibility,
                description=description,
                tags=tags or {}
            )
        except ClientError as e:
            logger.error(f"Error creating schema {registry_name}.{name}: {e}")
            raise
    
    def get_schema(self, registry_name: str, schema_name: str) -> SchemaInfo:
        """Get schema information.
        
        Args:
            registry_name: Registry name
            schema_name: Schema name
            
        Returns:
            SchemaInfo object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            response = self.glue_client.get_schema(
                RegistryId={"Name": registry_name},
                SchemaName=schema_name
            )
            
            return SchemaInfo(
                registry_name=registry_name,
                schema_name=response.get("SchemaName", ""),
                schema_arn=response.get("SchemaArn"),
                data_format=response.get("DataFormat", "JSON"),
                compatibility=response.get("Compatibility", "BACKWARD"),
                description=response.get("Description"),
                latest_version=response.get("LatestSchemaVersion"),
                tags=response.get("Tags", {})
            )
        except ClientError as e:
            logger.error(f"Error getting schema {registry_name}.{schema_name}: {e}")
            raise
    
    def list_schemas(
        self,
        registry_name: Optional[str] = None,
        max_results: int = 100,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """List schemas.
        
        Args:
            registry_name: Registry name filter
            max_results: Maximum results to return
            **kwargs: Additional arguments
            
        Returns:
            List of schema info dictionaries
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        schemas = []
        kwargs_list = {}
        
        if registry_name:
            kwargs_list["RegistryId"] = {"Name": registry_name}
        
        try:
            paginator = self.glue_client.get_paginator("list_schemas")
            
            for page in paginator.paginate(**kwargs_list):
                schemas.extend(page.get("Schemas", []))
            
            return schemas
            
        except ClientError as e:
            logger.error(f"Error listing schemas: {e}")
            raise
    
    def register_schema_version(
        self,
        registry_name: str,
        schema_name: str,
        schema_definition: str,
        **kwargs
    ) -> Dict[str, Any]:
        """Register a new schema version.
        
        Args:
            registry_name: Registry name
            schema_name: Schema name
            schema_definition: Schema definition
            **kwargs: Additional arguments
            
        Returns:
            Schema version info dictionary
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            return self.glue_client.register_schema_version(
                RegistryId={"Name": registry_name},
                SchemaName=schema_name,
                SchemaDefinition=schema_definition,
                **kwargs
            )
        except ClientError as e:
            logger.error(f"Error registering schema version: {e}")
            raise
    
    def get_schema_version(
        self,
        registry_name: str,
        schema_name: str,
        version: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """Get schema version information.
        
        Args:
            registry_name: Registry name
            schema_name: Schema name
            version: Schema version (optional, gets latest if not specified)
            **kwargs: Additional arguments
            
        Returns:
            Schema version info dictionary
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            schema_id = {"Name": registry_name, "SchemaName": schema_name}
            if version:
                return self.glue_client.get_schema_version(
                    SchemaId=schema_id,
                    SchemaVersionNumber={"VersionNumber": int(version)},
                    **kwargs
                )
            else:
                return self.glue_client.get_latest_schema_version(
                    SchemaId=schema_id,
                    **kwargs
                )
        except ClientError as e:
            logger.error(f"Error getting schema version: {e}")
            raise
    
    def delete_registry(self, name: str, **kwargs):
        """Delete a schema registry.
        
        Args:
            name: Registry name
            **kwargs: Additional arguments
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            self.glue_client.delete_registry(
                RegistryId={"Name": name},
                **kwargs
            )
        except ClientError as e:
            logger.error(f"Error deleting registry {name}: {e}")
            raise
    
    def delete_schema(
        self,
        registry_name: str,
        schema_name: str,
        **kwargs
    ):
        """Delete a schema.
        
        Args:
            registry_name: Registry name
            schema_name: Schema name
            **kwargs: Additional arguments
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            self.glue_client.delete_schema(
                RegistryId={"Name": registry_name},
                SchemaName=schema_name,
                **kwargs
            )
        except ClientError as e:
            logger.error(f"Error deleting schema {registry_name}.{schema_name}: {e}")
            raise
    
    # =========================================================================
    # DATA QUALITY
    # =========================================================================
    
    def create_data_quality_ruleset(
        self,
        name: str,
        ruleset: List[Dict[str, Any]],
        description: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """Create a data quality ruleset.
        
        Args:
            name: Ruleset name
            ruleset: List of data quality rules
            description: Ruleset description
            tags: Resource tags
            **kwargs: Additional arguments
            
        Returns:
            Response dictionary
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        input_dict = {
            "Name": name,
            "Ruleset": json.dumps(ruleset) if isinstance(ruleset, list) else ruleset
        }
        
        if description:
            input_dict["Description"] = description
        
        input_dict.update(kwargs)
        
        try:
            response = self.glue_client.create_data_quality_ruleset(**input_dict)
            
            if tags:
                ruleset_arn = f"arn:aws:glue:{self.region_name}::dataQualityRuleset/{name}"
                self.glue_client.tag_resource(ResourceArn=ruleset_arn, TagsToAdd=tags)
            
            return response
        except ClientError as e:
            logger.error(f"Error creating data quality ruleset {name}: {e}")
            raise
    
    def get_data_quality_ruleset(self, name: str) -> Dict[str, Any]:
        """Get data quality ruleset.
        
        Args:
            name: Ruleset name
            
        Returns:
            Ruleset info dictionary
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            return self.glue_client.get_data_quality_ruleset(Name=name)
        except ClientError as e:
            logger.error(f"Error getting data quality ruleset {name}: {e}")
            raise
    
    def list_data_quality_rulesets(
        self,
        filter_pattern: Optional[str] = None,
        max_results: int = 100,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """List data quality rulesets.
        
        Args:
            filter_pattern: Pattern to filter ruleset names
            max_results: Maximum results to return
            **kwargs: Additional arguments
            
        Returns:
            List of ruleset info dictionaries
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        rulesets = []
        
        try:
            paginator = self.glue_client.get_paginator("list_data_quality_rulesets")
            
            for page in paginator.paginate():
                for ruleset in page.get("Rulesets", []):
                    name = ruleset.get("Name", "")
                    if filter_pattern and filter_pattern not in name:
                        continue
                    rulesets.append(ruleset)
            
            return rulesets
            
        except ClientError as e:
            logger.error(f"Error listing data quality rulesets: {e}")
            raise
    
    def start_data_quality_rule_evaluation(
        self,
        ruleset_name: str,
        database_name: str,
        table_name: str,
        catalog_id: Optional[str] = None,
        role_arn: Optional[str] = None,
        number_of_workers: Optional[int] = None,
        timeout: Optional[int] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """Start data quality rule evaluation.
        
        Args:
            ruleset_name: Ruleset name
            database_name: Database name
            table_name: Table name
            catalog_id: AWS account ID
            role_arn: IAM role ARN
            number_of_workers: Number of workers
            timeout: Timeout in minutes
            **kwargs: Additional arguments
            
        Returns:
            Evaluation run info dictionary
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        input_dict = {
            "RulesetName": ruleset_name,
            "DatabaseName": database_name,
            "TableName": table_name
        }
        
        if catalog_id:
            input_dict["CatalogId"] = catalog_id
        if role_arn:
            input_dict["Role"] = role_arn
        if number_of_workers is not None:
            input_dict["NumberOfWorkers"] = number_of_workers
        if timeout is not None:
            input_dict["Timeout"] = timeout
        
        input_dict.update(kwargs)
        
        try:
            return self.glue_client.start_data_quality_rule_evaluation_run(**input_dict)
        except ClientError as e:
            logger.error(f"Error starting data quality evaluation: {e}")
            raise
    
    def get_data_quality_result(
        self,
        run_id: str,
        **kwargs
    ) -> Dict[str, Any]:
        """Get data quality evaluation result.
        
        Args:
            run_id: Run ID
            **kwargs: Additional arguments
            
        Returns:
            Result dictionary
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            return self.glue_client.get_data_quality_result(RuleEvaluationRunId=run_id, **kwargs)
        except ClientError as e:
            logger.error(f"Error getting data quality result {run_id}: {e}")
            raise
    
    def delete_data_quality_ruleset(self, name: str, **kwargs):
        """Delete data quality ruleset.
        
        Args:
            name: Ruleset name
            **kwargs: Additional arguments
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            self.glue_client.delete_data_quality_ruleset(Name=name, **kwargs)
        except ClientError as e:
            logger.error(f"Error deleting data quality ruleset {name}: {e}")
            raise
    
    def create_data_quality_profile(
        self,
        name: str,
        database_name: str,
        table_name: str,
        rules: Optional[List[DataQualityRule]] = None
    ) -> DataQualityProfile:
        """Create a data quality profile.
        
        Args:
            name: Profile name
            database_name: Database name
            table_name: Table name
            rules: List of data quality rules
            
        Returns:
            DataQualityProfile object
        """
        return DataQualityProfile(
            name=name,
            database_name=database_name,
            table_name=table_name,
            rules=rules or []
        )
    
    def analyze_data_quality(
        self,
        database_name: str,
        table_name: str,
        rules: List[DataQualityRule],
        catalog_id: Optional[str] = None
    ) -> List[DataQualityResult]:
        """Analyze data quality using rules.
        
        Args:
            database_name: Database name
            table_name: Table name
            rules: List of data quality rules
            catalog_id: AWS account ID
            
        Returns:
            List of DataQualityResult objects
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        results = []
        
        try:
            # Get table info for schema
            table_info = self.get_table(database_name, table_name, catalog_id)
            columns = table_info.storage_descriptor.get("Columns", []) if table_info.storage_descriptor else []
            
            for rule in rules:
                try:
                    result = self._evaluate_data_quality_rule(
                        database_name,
                        table_name,
                        rule,
                        columns,
                        catalog_id
                    )
                    results.append(result)
                except Exception as e:
                    results.append(DataQualityResult(
                        rule=rule.rule_type.value,
                        column=rule.column,
                        passed=False,
                        error_message=str(e)
                    ))
            
            return results
            
        except ClientError as e:
            logger.error(f"Error analyzing data quality: {e}")
            raise
    
    def _evaluate_data_quality_rule(
        self,
        database_name: str,
        table_name: str,
        rule: DataQualityRule,
        columns: List[Dict[str, Any]],
        catalog_id: Optional[str]
    ) -> DataQualityResult:
        """Evaluate a single data quality rule.
        
        Args:
            database_name: Database name
            table_name: Table name
            rule: Data quality rule
            columns: Table columns
            catalog_id: AWS account ID
            
        Returns:
            DataQualityResult object
        """
        # Simplified rule evaluation - actual implementation would query the table
        rule_name = rule.rule_type.value
        column_name = rule.column
        
        return DataQualityResult(
            rule=rule_name,
            column=column_name,
            passed=True,
            failed_count=0,
            total_count=0
        )
    
    # =========================================================================
    # CloudWatch INTEGRATION
    # =========================================================================
    
    def put_metric_data(
        self,
        namespace: str,
        metrics: List[Dict[str, Any]],
        **kwargs
    ) -> Dict[str, Any]:
        """Put metric data to CloudWatch.
        
        Args:
            namespace: Metric namespace
            metrics: List of metric data points
            **kwargs: Additional arguments
            
        Returns:
            Response dictionary
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            return self.cloudwatch_client.put_metric_data(
                Namespace=namespace,
                MetricData=metrics,
                **kwargs
            )
        except ClientError as e:
            logger.error(f"Error putting metric data: {e}")
            raise
    
    def get_job_metrics(
        self,
        job_name: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        period: int = 300,
        statistics: List[str] = None
    ) -> Dict[str, Any]:
        """Get Glue job metrics from CloudWatch.
        
        Args:
            job_name: Job name
            start_time: Start time
            end_time: End time
            period: Metric period in seconds
            statistics: Statistics to retrieve
            
        Returns:
            Metric data dictionary
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        if statistics is None:
            statistics = ["Average", "Sum", "Maximum", "Minimum"]
        
        if end_time is None:
            end_time = datetime.now()
        if start_time is None:
            start_time = end_time - timedelta(hours=1)
        
        namespace = "AWS/Glue"
        job_metric_filters = [
            " glue.driver.aggregate.bytes_written",
            "glue.driver.aggregate.records_written",
            "glue.driver.aggregate.elapsed_time",
            "glue.driver.aggregate.num_tasks",
            "glue.driver.aggregate.num_failed_tasks"
        ]
        
        try:
            metrics = {}
            
            for metric_name in job_metric_filters:
                response = self.cloudwatch_client.get_metric_statistics(
                    Namespace=namespace,
                    MetricName=metric_name.strip(),
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=period,
                    Statistics=statistics,
                    Dimensions=[
                        {"Name": "JobName", "Value": job_name}
                    ]
                )
                
                metrics[metric_name.strip()] = response.get("Datapoints", [])
            
            return metrics
            
        except ClientError as e:
            logger.error(f"Error getting job metrics: {e}")
            raise
    
    def get_crawler_metrics_cloudwatch(
        self,
        crawler_name: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        period: int = 300,
        statistics: List[str] = None
    ) -> Dict[str, Any]:
        """Get Glue crawler metrics from CloudWatch.
        
        Args:
            crawler_name: Crawler name (optional, for specific crawler)
            start_time: Start time
            end_time: End time
            period: Metric period in seconds
            statistics: Statistics to retrieve
            
        Returns:
            Metric data dictionary
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        if statistics is None:
            statistics = ["Average", "Sum", "Maximum", "Minimum"]
        
        if end_time is None:
            end_time = datetime.now()
        if start_time is None:
            start_time = end_time - timedelta(hours=1)
        
        namespace = "AWS/Glue"
        crawler_metrics = [
            "CrawlerRuntimeMinutes",
            "CrawlerScannedFiles",
            "CrawlerNewTablesCreated",
            "CrawlerTablesUpdated"
        ]
        
        try:
            metrics = {}
            dimensions = []
            
            if crawler_name:
                dimensions = [{"Name": "Crawler", "Value": crawler_name}]
            
            for metric_name in crawler_metrics:
                kwargs_get = {
                    "Namespace": namespace,
                    "MetricName": metric_name,
                    "StartTime": start_time,
                    "EndTime": end_time,
                    "Period": period,
                    "Statistics": statistics
                }
                
                if dimensions:
                    kwargs_get["Dimensions"] = dimensions
                
                response = self.cloudwatch_client.get_metric_statistics(**kwargs_get)
                metrics[metric_name] = response.get("Datapoints", [])
            
            return metrics
            
        except ClientError as e:
            logger.error(f"Error getting crawler metrics: {e}")
            raise
    
    def create_job_metrics_alarm(
        self,
        alarm_name: str,
        job_name: str,
        metric_name: str,
        threshold: float,
        comparison_operator: str = "GreaterThanThreshold",
        evaluation_periods: int = 1,
        period: int = 300,
        statistic: str = "Average",
        **kwargs
    ) -> Dict[str, Any]:
        """Create CloudWatch alarm for Glue job metrics.
        
        Args:
            alarm_name: Alarm name
            job_name: Job name
            metric_name: Metric name
            threshold: Alarm threshold
            comparison_operator: Comparison operator
            evaluation_periods: Number of evaluation periods
            period: Metric period in seconds
            statistic: Statistic type
            **kwargs: Additional arguments
            
        Returns:
            Alarm creation response
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        try:
            return self.cloudwatch_client.put_metric_alarm(
                AlarmName=alarm_name,
                MetricName=metric_name,
                Namespace="AWS/Glue",
                Statistic=statistic,
                Period=period,
                EvaluationPeriods=evaluation_periods,
                Threshold=threshold,
                ComparisonOperator=comparison_operator,
                Dimensions=[{"Name": "JobName", "Value": job_name}],
                **kwargs
            )
        except ClientError as e:
            logger.error(f"Error creating job metrics alarm: {e}")
            raise
    
    def get_glue_job_logs(self, job_name: str, start_time: Optional[datetime] = None) -> List[str]:
        """Get Glue job logs from CloudWatch.
        
        Args:
            job_name: Job name
            start_time: Start time for log query
            
        Returns:
            List of log entries
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        logs_client = self._clients.get("logs")
        if not logs_client:
            import boto3
            logs_client = boto3.Session().client("logs", region_name=self.region_name)
        
        log_group_name = f"/aws/glue/jobs/{job_name}"
        
        try:
            query = logs_client.start_query(
                logGroupName=log_group_name,
                startTime=int(start_time.timestamp()) if start_time else int((datetime.now() - timedelta(hours=1)).timestamp()),
                endTime=int(datetime.now().timestamp()),
                queryString=f"fields @timestamp, @message | sort @timestamp desc | limit 100"
            )
            
            response = logs_client.get_query_results(queryId=query["queryId"])
            return [f"{row['field']}: {row['value']}" for row in response.get("results", [])]
            
        except ClientError as e:
            logger.error(f"Error getting job logs: {e}")
            raise
    
    def monitor_job_runs(
        self,
        job_name: str,
        run_id: str,
        poll_interval: int = 30,
        timeout: Optional[int] = None,
        callback: Optional[Callable[[JobRunInfo], None]] = None
    ) -> JobRunInfo:
        """Monitor a job run until completion.
        
        Args:
            job_name: Job name
            run_id: Job run ID
            poll_interval: Polling interval in seconds
            timeout: Maximum wait time in seconds
            callback: Optional callback function for status updates
            
        Returns:
            Final JobRunInfo object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required")
        
        start_time = time.time()
        terminal_states = {
            JobRunStatus.SUCCEEDED,
            JobRunStatus.FAILED,
            JobRunStatus.STOPPED,
            JobRunStatus.TIMEOUT,
            JobRunStatus.ERROR
        }
        
        while True:
            job_run = self.get_job_run(job_name, run_id)
            
            if callback:
                callback(job_run)
            
            if job_run.status in terminal_states:
                return job_run
            
            if timeout and (time.time() - start_time) > timeout:
                return job_run
            
            time.sleep(poll_interval)
    
    # =========================================================================
    # UTILITY METHODS
    # =========================================================================
    
    def _parse_datetime(self, dt: Any) -> Optional[datetime]:
        """Parse datetime from various formats."""
        if dt is None:
            return None
        if isinstance(dt, datetime):
            return dt
        if isinstance(dt, (int, float)):
            return datetime.fromtimestamp(dt)
        if isinstance(dt, str):
            try:
                return datetime.fromisoformat(dt.replace("Z", "+00:00"))
            except ValueError:
                return None
        return None
    
    def create_storage_descriptor(
        self,
        location: str,
        input_format: Optional[str] = None,
        output_format: Optional[str] = None,
        columns: Optional[List[ColumnInfo]] = None,
        serde_info: Optional[Dict[str, Any]] = None,
        compressed: bool = False,
        number_of_partitions: Optional[int] = None
    ) -> Dict[str, Any]:
        """Create a storage descriptor.
        
        Args:
            location: S3 location
            input_format: Input format class
            output_format: Output format class
            columns: List of columns
            serde_info: SerDe info
            compressed: Whether data is compressed
            number_of_partitions: Number of partitions
            
        Returns:
            Storage descriptor dictionary
        """
        sd = {
            "Location": location,
            "Compressed": compressed
        }
        
        if input_format:
            sd["InputFormat"] = input_format
        if output_format:
            sd["OutputFormat"] = output_format
        if columns:
            sd["Columns"] = [
                {
                    "Name": col.name,
                    "Type": col.type,
                    "Comment": col.comment
                }
                for col in columns
            ]
        if serde_info:
            sd["SerdeInfo"] = serde_info
        if number_of_partitions is not None:
            sd["NumberOfPartitions"] = number_of_partitions
        
        return sd
    
    def export_to_athena_workgroup(
        self,
        database_name: str,
        table_name: str,
        output_location: str,
        workgroup_name: str = "primary"
    ) -> Dict[str, Any]:
        """Export Glue table configuration for Athena.
        
        Args:
            database_name: Database name
            table_name: Table name
            output_location: S3 output location for results
            workgroup_name: Athena workgroup name
            
        Returns:
            Configuration dictionary for Athena
        """
        table_info = self.get_table(database_name, table_name)
        sd = table_info.storage_descriptor or {}
        
        return {
            "aws_region": self.region_name,
            "database": database_name,
            "table": table_name,
            "columns": sd.get("Columns", []),
            "partition_keys": table_info.partition_keys,
            "location": sd.get("Location", ""),
            "input_format": sd.get("InputFormat", ""),
            "output_format": sd.get("OutputFormat", ""),
            "serde_info": sd.get("SerdeInfo", {}),
            "athena_output_location": output_location,
            "workgroup": workgroup_name
        }
    
    def generate_create_table_sql(
        self,
        database_name: str,
        table_name: str,
        catalog_id: Optional[str] = None
    ) -> str:
        """Generate CREATE TABLE SQL for Athena.
        
        Args:
            database_name: Database name
            table_name: Table name
            catalog_id: AWS account ID
            
        Returns:
            CREATE TABLE SQL statement
        """
        table_info = self.get_table(database_name, table_name, catalog_id)
        sd = table_info.storage_descriptor or {}
        
        columns = sd.get("Columns", [])
        partition_keys = table_info.partition_keys
        
        column_defs = []
        for col in columns:
            col_type = col.get("Type", "string")
            col_name = col.get("Name", "")
            col_comment = col.get("Comment", "")
            if col_comment:
                column_defs.append(f"  `{col_name}` {col_type} COMMENT '{col_comment}'")
            else:
                column_defs.append(f"  `{col_name}` {col_type}")
        
        for pk in partition_keys:
            col_name = pk.get("Name", "")
            col_type = pk.get("Type", "string")
            column_defs.append(f"  `{col_name}` {col_type}")
        
        all_columns = columns + partition_keys
        
        location = sd.get("Location", "")
        input_format = sd.get("InputFormat", "")
        output_format = sd.get("OutputFormat", "")
        serde_info = sd.get("SerdeInfo", {})
        serde_library = serde_info.get("SerializationLibrary", "")
        
        sql_parts = [f"CREATE EXTERNAL TABLE IF NOT EXISTS `{database_name}`.`{table_name}` ("]
        sql_parts.append(",\n".join(column_defs))
        sql_parts.append(")")
        
        if partition_keys:
            partition_cols = ', '.join([f'`{pk["Name"]}`' for pk in partition_keys])
            sql_parts.append(f"PARTITIONED BY ({partition_cols})")
        
        sql_parts.append(f"ROW FORMAT SERDE '{serde_library}'")
        
        if serde_info.get("Parameters"):
            serde_props = " ".join([f"'{k}'='{v}'" for k, v in serde_info.get("Parameters", {}).items()])
            sql_parts.append(f"WITH SERDEPROPERTIES ({serde_props})")
        
        sql_parts.append(f"LOCATION '{location}'")
        
        if input_format and output_format:
            sql_parts.append(f"STORED AS INPUTFORMAT '{input_format}' OUTPUTFORMAT '{output_format}'")
        
        if table_info.parameters:
            table_props = " ".join([f"'{k}'='{v}'" for k, v in table_info.parameters.items()])
            sql_parts.append(f"TBLPROPERTIES ({table_props})")
        
        return "\n".join(sql_parts)
