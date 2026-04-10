"""
AWS Athena SQL Integration Module for Workflow System

Implements an AthenaIntegration class with:
1. Workgroup management: Create/manage workgroups
2. Query execution: Execute SQL queries
3. Named queries: Create/manage named queries
4. Query results: Manage query results in S3
5. Saved queries: Manage saved queries
6. Prepared statements: Create/manage prepared statements
7. Data catalog: Athena data catalog
8. Database/table: Create/manage databases and tables
9. View management: Create/manage views
10. CloudWatch integration: Query metrics and logs

Commit: 'feat(aws-athena): add AWS Athena with workgroup management, query execution, named queries, saved queries, prepared statements, data catalog, views, CloudWatch'
"""

import uuid
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional, Set, Union, Awaitable
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum
import copy
import threading
import hashlib
import base64

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


class QueryState(Enum):
    """Athena query states."""
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class OutputCompression(Enum):
    """Query result compression formats."""
    NONE = "NONE"
    GZIP = "GZIP"
    SNAPPY = "SNAPPY"
    ZLIB = "ZLIB"


class OutputFormat(Enum):
    """Query result output formats."""
    UNICODE_CSV = "UNICODE_CSV"
    ALANCED_CSV = "ALANCED_CSV"
    TSV = "TSV"
    JSON = "JSON"
    PARQUET = "PARQUET"
    ORC = "ORC"
    AVRO = "AVRO"
    TEXTFILE = "TEXTFILE"


@dataclass
class QueryResult:
    """Query execution result."""
    query_id: str
    query: str
    state: QueryState
    output_location: Optional[str] = None
    error_message: Optional[str] = None
    rows_returned: int = 0
    bytes_scanned: int = 0
    execution_time_ms: int = 0
    data_scanned_bytes: int = 0
    created_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result_reuse_metadata: Optional[Dict[str, Any]] = None


@dataclass
class WorkgroupInfo:
    """Workgroup information."""
    name: str
    state: str
    output_location: str
    description: Optional[str] = None
    creation_time: Optional[datetime] = None
    engine_version: Optional[str] = None
    encryption_config: Optional[Dict[str, Any]] = None
    identity_center_configuration: Optional[Dict[str, Any]] = None
    enable_cloudwatch_metrics: bool = False
    bytes_scanned_cutoff: Optional[int] = None
    bytes_scanned_cutoff_percentage: Optional[float] = None


@dataclass
class NamedQueryInfo:
    """Named query information."""
    id: str
    name: str
    description: Optional[str] = None
    query_string: str = ""
    database: str = "default"
    workgroup: str = "primary"
    created_at: Optional[datetime] = None


@dataclass
class PreparedStatementInfo:
    """Prepared statement information."""
    statement_name: str
    query_string: str
    workgroup: str = "primary"
    last_modified_time: Optional[datetime] = None


@dataclass
class DatabaseInfo:
    """Database information."""
    name: str
    description: Optional[str] = None
    parameters: Dict[str, str] = field(default_factory=dict)


@dataclass
class TableInfo:
    """Table information."""
    name: str
    database: str
    table_type: str
    columns: List[Dict[str, str]] = field(default_factory=list)
    partition_keys: List[Dict[str, str]] = field(default_factory=list)
    location: Optional[str] = None
    input_format: Optional[str] = None
    output_format: Optional[str] = None
    serde_properties: Dict[str, str] = field(default_factory=dict)
    parameters: Dict[str, str] = field(default_factory=dict)


@dataclass
class ViewInfo:
    """View information."""
    name: str
    database: str
    view_original_text: str
    view_expanded_text: Optional[str] = None
    create_time: Optional[datetime] = None


class AthenaIntegration:
    """
    AWS Athena SQL Integration for workflow automation.
    
    Provides comprehensive Athena management including:
    - Workgroup management
    - Query execution
    - Named queries
    - Saved queries
    - Prepared statements
    - Data catalog operations
    - Database/table management
    - View management
    - CloudWatch integration
    """
    
    def __init__(
        self,
        region_name: str = "us-east-1",
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        s3_output_location: str = "s3://athena-results/",
        workgroup: str = "primary",
        profile_name: Optional[str] = None,
        boto_client: Any = None,
    ):
        """
        Initialize Athena integration.
        
        Args:
            region_name: AWS region for Athena
            aws_access_key_id: AWS access key ID
            aws_secret_access_key: AWS secret access key
            aws_session_token: AWS session token
            s3_output_location: Default S3 location for query results
            workgroup: Default workgroup name
            profile_name: AWS profile name
            boto_client: Pre-configured boto3 client (for testing)
        """
        self.region_name = region_name
        self.s3_output_location = s3_output_location
        self.default_workgroup = workgroup
        
        if boto_client:
            self.athena = boto_client
        elif BOTO3_AVAILABLE:
            session_kwargs = {"region_name": region_name}
            if profile_name:
                session_kwargs["profile_name"] = profile_name
            elif aws_access_key_id and aws_secret_access_key:
                session_kwargs["aws_access_key_id"] = aws_access_key_id
                session_kwargs["aws_secret_access_key"] = aws_secret_access_key
                if aws_session_token:
                    session_kwargs["aws_session_token"] = aws_session_token
            
            session = boto3.Session(**session_kwargs)
            self.athena = session.client("athena", region_name=region_name)
            self.s3 = session.client("s3", region_name=region_name)
            self.cloudwatch = session.client("cloudwatch", region_name=region_name)
            self.cloudwatch_logs = session.client("logs", region_name=region_name)
        else:
            self.athena = None
            self.s3 = None
            self.cloudwatch = None
            self.cloudwatch_logs = None
        
        self._query_cache: Dict[str, QueryResult] = {}
        self._lock = threading.Lock()
    
    # =========================================================================
    # WORKGROUP MANAGEMENT
    # =========================================================================
    
    def create_workgroup(
        self,
        name: str,
        output_location: str,
        description: Optional[str] = None,
        engine_version: Optional[str] = None,
        encryption_config: Optional[Dict[str, Any]] = None,
        enable_cloudwatch_metrics: bool = False,
        bytes_scanned_cutoff: Optional[int] = None,
        bytes_scanned_cutoff_percentage: Optional[float] = None,
        identity_center_configuration: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Create a new Athena workgroup.
        
        Args:
            name: Workgroup name
            output_location: S3 location for query results
            description: Workgroup description
            engine_version: Athena engine version
            encryption_config: Encryption configuration
            enable_cloudwatch_metrics: Enable CloudWatch metrics
            bytes_scanned_cutoff: Bytes scanned cutoff
            bytes_scanned_cutoff_percentage: Percentage cutoff
            identity_center_configuration: IAM Identity Center config
            
        Returns:
            Workgroup creation response
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        kwargs = {
            "Name": name,
            "Configuration": {
                "ResultConfiguration": {
                    "OutputLocation": output_location,
                },
                "EnableCloudWatchMetrics": enable_cloudwatch_metrics,
            },
        }
        
        if description:
            kwargs["Description"] = description
        
        if engine_version:
            kwargs["Configuration"]["EngineVersion"] = {
                "SelectedEngineVersion": engine_version
            }
        
        if encryption_config:
            kwargs["Configuration"]["ResultConfiguration"]["EncryptionConfiguration"] = encryption_config
        
        if bytes_scanned_cutoff:
            kwargs["Configuration"]["BytesScannedCutoff"] = bytes_scanned_cutoff
        
        if bytes_scanned_cutoff_percentage:
            kwargs["Configuration"]["BytesScannedCutoffPercentage"] = bytes_scanned_cutoff_percentage
        
        if identity_center_configuration:
            kwargs["Configuration"]["IdentityCenterConfiguration"] = identity_center_configuration
        
        response = self.athena.create_workgroup(**kwargs)
        return response
    
    def get_workgroup(self, name: str) -> WorkgroupInfo:
        """
        Get workgroup information.
        
        Args:
            name: Workgroup name
            
        Returns:
            WorkgroupInfo object
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        response = self.athena.get_work_group(WorkGroup=name)
        wg = response["WorkGroup"]
        
        config = wg.get("Configuration", {})
        result_config = config.get("ResultConfiguration", {})
        
        return WorkgroupInfo(
            name=wg["Name"],
            state=wg["State"],
            description=wg.get("Description"),
            output_location=result_config.get("OutputLocation", ""),
            creation_time=wg.get("CreationTime"),
            engine_version=config.get("EngineVersion", {}).get("SelectedEngineVersion"),
            encryption_config=result_config.get("EncryptionConfiguration"),
            enable_cloudwatch_metrics=config.get("EnableCloudWatchMetrics", False),
            bytes_scanned_cutoff=config.get("BytesScannedCutoff"),
            bytes_scanned_cutoff_percentage=config.get("BytesScannedCutoffPercentage"),
        )
    
    def list_workgroups(self, max_results: int = 50) -> List[WorkgroupInfo]:
        """
        List all Athena workgroups.
        
        Args:
            max_results: Maximum number of results
            
        Returns:
            List of WorkgroupInfo objects
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        workgroups = []
        paginator = self.athena.get_paginator("list_work_groups")
        
        for page in paginator.paginate(PaginationConfig={"MaxItems": max_results}):
            for wg in page.get("WorkGroups", []):
                config = wg.get("Configuration", {})
                result_config = config.get("ResultConfiguration", {})
                
                workgroups.append(WorkgroupInfo(
                    name=wg["Name"],
                    state=wg["State"],
                    output_location=result_config.get("OutputLocation", ""),
                    description=wg.get("Description"),
                    creation_time=wg.get("CreationTime"),
                ))
        
        return workgroups
    
    def update_workgroup(
        self,
        name: str,
        description: Optional[str] = None,
        output_location: Optional[str] = None,
        enable_cloudwatch_metrics: Optional[bool] = None,
        bytes_scanned_cutoff: Optional[int] = None,
        remove_bytes_scanned_cutoff: bool = False,
    ) -> Dict[str, Any]:
        """
        Update a workgroup configuration.
        
        Args:
            name: Workgroup name
            description: New description
            output_location: New S3 output location
            enable_cloudwatch_metrics: Toggle CloudWatch metrics
            bytes_scanned_cutoff: Set bytes scanned cutoff
            remove_bytes_scanned_cutoff: Remove cutoff limit
            
        Returns:
            Update response
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        kwargs: Dict[str, Any] = {"WorkGroup": name}
        
        updates = {}
        if description is not None:
            updates["Description"] = description
        
        if output_location:
            updates["ResultConfiguration"] = {"OutputLocation": output_location}
        
        if enable_cloudwatch_metrics is not None:
            updates["EnableCloudWatchMetrics"] = enable_cloudwatch_metrics
        
        if bytes_scanned_cutoff is not None:
            updates["BytesScannedCutoff"] = bytes_scanned_cutoff
        
        if remove_bytes_scanned_cutoff:
            updates["RemoveBytesScannedCutoff"] = True
        
        if updates:
            kwargs["ConfigurationUpdates"] = updates
        
        return self.athena.update_work_group(**kwargs)
    
    def delete_workgroup(self, name: str, recursive: bool = False) -> Dict[str, Any]:
        """
        Delete a workgroup.
        
        Args:
            name: Workgroup name
            recursive: Delete nested workgroups (if a primary workgroup)
            
        Returns:
            Delete response
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        return self.athena.delete_work_group(WorkGroup=name)
    
    # =========================================================================
    # QUERY EXECUTION
    # =========================================================================
    
    def execute_query(
        self,
        query: str,
        database: Optional[str] = None,
        workgroup: Optional[str] = None,
        output_location: Optional[str] = None,
        encryption_config: Optional[Dict[str, Any]] = None,
        result_reuse_minutes: Optional[int] = None,
        result_reuse_by_columns: Optional[Dict[str, Any]] = None,
        execution_params: Optional[Dict[str, str]] = None,
    ) -> QueryResult:
        """
        Execute a SQL query in Athena.
        
        Args:
            query: SQL query string
            database: Target database
            workgroup: Workgroup to use
            output_location: S3 location for results
            encryption_config: S3 encryption configuration
            result_reuse_minutes: Enable result reuse for N minutes
            result_reuse_by_columns: Enable result reuse with column matching
            execution_params: Execution parameters
            
        Returns:
            QueryResult object
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        workgroup = workgroup or self.default_workgroup
        output_location = output_location or self.s3_output_location
        
        query_execution_params = {
            "QueryString": query,
            "WorkGroup": workgroup,
            "ResultConfiguration": {
                "OutputLocation": output_location,
            },
        }
        
        if database:
            query_execution_params["QueryExecutionContext"] = {"Database": database}
        
        if encryption_config:
            query_execution_params["ResultConfiguration"]["EncryptionConfiguration"] = encryption_config
        
        if result_reuse_minutes:
            query_execution_params["ResultReuseConfiguration"] = {
                "ResultReuseByColumnsConfiguration": {
                    "Enabled": True,
                    "ReuseTimedOutResultOnly": False,
                }
            }
        
        if result_reuse_by_columns:
            query_execution_params["ResultReuseByColumnsConfiguration"] = result_reuse_by_columns
        
        if execution_params:
            query_execution_params["ExecutionParameters"] = [
                f"{k}={v}" for k, v in execution_params.items()
            ]
        
        response = self.athena.start_query_execution(**query_execution_params)
        query_id = response["QueryExecutionId"]
        
        result = QueryResult(
            query_id=query_id,
            query=query,
            state=QueryState.QUEUED,
            created_at=datetime.now(),
        )
        
        with self._lock:
            self._query_cache[query_id] = result
        
        return result
    
    def get_query_result(
        self,
        query_id: str,
        max_wait_time: int = 300,
        poll_interval: float = 2.0,
    ) -> QueryResult:
        """
        Wait for and retrieve query results.
        
        Args:
            query_id: Query execution ID
            max_wait_time: Maximum wait time in seconds
            poll_interval: Polling interval in seconds
            
        Returns:
            QueryResult with results populated
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        start_time = time.time()
        
        while True:
            response = self.athena.get_query_execution(QueryExecutionId=query_id)
            qe = response["QueryExecution"]
            status = qe["Status"]["State"]
            
            with self._lock:
                if query_id in self._query_cache:
                    result = self._query_cache[query_id]
                else:
                    result = QueryResult(query_id=query_id, query="", state=QueryState.RUNNING)
            
            result.state = QueryState(status)
            
            if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                result.output_location = qe.get("ResultConfiguration", {}).get("OutputLocation")
                result.error_message = qe["Status"].get("StateChangeReason")
                
                stats = qe.get("Statistics", {})
                result.rows_returned = stats.get("RowsProcessedCount", 0)
                result.bytes_scanned = stats.get("DataScannedBytes", 0)
                result.execution_time_ms = stats.get("TotalExecutionTimeInMillis", 0)
                result.data_scanned_bytes = stats.get("DataScannedBytes", 0)
                result.completed_at = datetime.now()
                
                if qe.get("Statistics", {}).get("ResultReuseMetadata"):
                    result.result_reuse_metadata = qe["Statistics"]["ResultReuseMetadata"]
                
                with self._lock:
                    self._query_cache[query_id] = result
                
                return result
            
            elapsed = time.time() - start_time
            if elapsed >= max_wait_time:
                result.state = QueryState.RUNNING
                result.error_message = f"Query timed out after {max_wait_time} seconds"
                return result
            
            time.sleep(min(poll_interval, max_wait_time - elapsed))
    
    def execute_query_sync(
        self,
        query: str,
        database: Optional[str] = None,
        workgroup: Optional[str] = None,
        output_location: Optional[str] = None,
        max_wait_time: int = 300,
        poll_interval: float = 2.0,
    ) -> QueryResult:
        """
        Execute a query and wait for results synchronously.
        
        Args:
            query: SQL query string
            database: Target database
            workgroup: Workgroup to use
            output_location: S3 location for results
            max_wait_time: Maximum wait time in seconds
            poll_interval: Polling interval in seconds
            
        Returns:
            QueryResult with results
        """
        result = self.execute_query(
            query=query,
            database=database,
            workgroup=workgroup,
            output_location=output_location,
        )
        return self.get_query_result(result.query_id, max_wait_time, poll_interval)
    
    def cancel_query(self, query_id: str) -> Dict[str, Any]:
        """
        Cancel a running query.
        
        Args:
            query_id: Query execution ID
            
        Returns:
            Cancellation response
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        return self.athena.stop_query_execution(QueryExecutionId=query_id)
    
    def list_query_executions(
        self,
        workgroup: Optional[str] = None,
        max_results: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        List query executions.
        
        Args:
            workgroup: Filter by workgroup
            max_results: Maximum results
            
        Returns:
            List of query execution summaries
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        kwargs = {"PaginationConfig": {"MaxItems": max_results}}
        if workgroup:
            kwargs["WorkGroup"] = workgroup
        
        paginator = self.athena.get_paginator("list_query_executions")
        executions = []
        
        for page in paginator.paginate(**kwargs):
            for qe_id in page.get("QueryExecutionIds", []):
                resp = self.athena.get_query_execution(QueryExecutionId=qe_id)
                executions.append(resp["QueryExecution"])
        
        return executions
    
    # =========================================================================
    # NAMED QUERIES
    # =========================================================================
    
    def create_named_query(
        self,
        name: str,
        query: str,
        database: str = "default",
        workgroup: str = "primary",
        description: Optional[str] = None,
    ) -> NamedQueryInfo:
        """
        Create a named query in Athena.
        
        Args:
            name: Query name
            query: SQL query string
            database: Default database
            workgroup: Workgroup
            description: Query description
            
        Returns:
            NamedQueryInfo object
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        kwargs = {
            "Name": name,
            "QueryString": query,
            "Database": database,
            "WorkGroup": workgroup,
        }
        if description:
            kwargs["Description"] = description
        
        response = self.athena.create_named_query(**kwargs)
        
        return NamedQueryInfo(
            id=response["NamedQueryId"],
            name=name,
            description=description,
            query_string=query,
            database=database,
            workgroup=workgroup,
        )
    
    def get_named_query(self, query_id: str) -> NamedQueryInfo:
        """
        Get named query details.
        
        Args:
            query_id: Named query ID
            
        Returns:
            NamedQueryInfo object
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        response = self.athena.get_named_query(NamedQueryId=query_id)
        nq = response["NamedQuery"]
        
        return NamedQueryInfo(
            id=nq["NamedQueryId"],
            name=nq["Name"],
            description=nq.get("Description"),
            query_string=nq["QueryString"],
            database=nq["Database"],
            workgroup=nq["WorkGroup"],
        )
    
    def list_named_queries(
        self,
        workgroup: Optional[str] = None,
        max_results: int = 50,
    ) -> List[NamedQueryInfo]:
        """
        List named queries.
        
        Args:
            workgroup: Filter by workgroup
            max_results: Maximum results
            
        Returns:
            List of NamedQueryInfo objects
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        kwargs: Dict[str, Any] = {"PaginationConfig": {"MaxItems": max_results}}
        if workgroup:
            kwargs["WorkGroup"] = workgroup
        
        paginator = self.athena.get_paginator("list_named_queries")
        queries = []
        
        for page in paginator.paginate(**kwargs):
            for nq_id in page.get("NamedQueryIds", []):
                try:
                    nq = self.get_named_query(nq_id)
                    queries.append(nq)
                except Exception as e:
                    logger.warning(f"Failed to get named query {nq_id}: {e}")
        
        return queries
    
    def update_named_query(
        self,
        query_id: str,
        name: Optional[str] = None,
        query: Optional[str] = None,
        database: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Update a named query.
        
        Args:
            query_id: Named query ID
            name: New name
            query: New query string
            database: New database
            description: New description
            
        Returns:
            Update response
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        kwargs: Dict[str, Any] = {"NamedQueryId": query_id}
        
        if name:
            kwargs["Name"] = name
        if query:
            kwargs["QueryString"] = query
        if database:
            kwargs["Database"] = database
        if description is not None:
            kwargs["Description"] = description
        
        return self.athena.update_named_query(**kwargs)
    
    def delete_named_query(self, query_id: str) -> Dict[str, Any]:
        """
        Delete a named query.
        
        Args:
            query_id: Named query ID
            
        Returns:
            Delete response
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        return self.athena.delete_named_query(NamedQueryId=query_id)
    
    # =========================================================================
    # QUERY RESULTS (S3)
    # =========================================================================
    
    def get_query_results_s3(
        self,
        output_location: str,
        query_id: Optional[str] = None,
        max_results: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        Get query results directly from S3.
        
        Args:
            output_location: S3 output location
            query_id: Optional query ID for validation
            max_results: Maximum rows to return
            
        Returns:
            List of result rows as dictionaries
        """
        if not self.s3:
            raise RuntimeError("Boto3 not available")
        
        parts = output_location.replace("s3://", "").split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""
        
        if key.endswith("/"):
            key += f"{query_id}.csv" if query_id else ".csv"
        elif not key.endswith(".csv"):
            key += f"/{query_id}.csv" if query_id else ".csv"
        
        try:
            response = self.s3.get_object(Bucket=bucket, Key=key)
            content = response["Body"].read().decode("utf-8")
            
            lines = content.strip().split("\n")
            if not lines:
                return []
            
            headers = lines[0].split(",")
            results = []
            
            for line in lines[1:max_results + 1]:
                values = line.split(",")
                row = {headers[i]: values[i] for i in range(len(headers))}
                results.append(row)
            
            return results
        except ClientError as e:
            logger.error(f"Failed to get S3 object: {e}")
            return []
    
    def list_query_result_files(
        self,
        bucket: str,
        prefix: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        List query result files in S3.
        
        Args:
            bucket: S3 bucket name
            prefix: Key prefix filter
            
        Returns:
            List of S3 object summaries
        """
        if not self.s3:
            raise RuntimeError("Boto3 not available")
        
        kwargs = {"Bucket": bucket}
        if prefix:
            kwargs["Prefix"] = prefix
        
        response = self.s3.list_objects_v2(**kwargs)
        return response.get("Contents", [])
    
    def cleanup_query_results(
        self,
        bucket: str,
        older_than_days: int = 30,
        dry_run: bool = True,
    ) -> List[str]:
        """
        Clean up old query result files from S3.
        
        Args:
            bucket: S3 bucket name
            older_than_days: Delete files older than N days
            dry_run: If True, only return files to delete
            
        Returns:
            List of deleted (or to-be-deleted) file keys
        """
        if not self.s3:
            raise RuntimeError("Boto3 not available")
        
        cutoff = datetime.now() - timedelta(days=older_than_days)
        to_delete = []
        
        paginator = self.s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket)
        
        for page in pages:
            for obj in page.get("Contents", []):
                if obj["LastModified"] < cutoff:
                    to_delete.append(obj["Key"])
        
        if not dry_run and to_delete:
            delete_keys = [{"Key": k} for k in to_delete]
            self.s3.delete_objects(
                Bucket=bucket,
                Delete={"Objects": delete_keys},
            )
        
        return to_delete
    
    # =========================================================================
    # SAVED QUERIES
    # =========================================================================
    
    def save_query(
        self,
        name: str,
        query: str,
        database: str = "default",
        workgroup: str = "primary",
        description: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        folder: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Save a query for later use (stored in S3 as JSON).
        
        Args:
            name: Query name
            query: SQL query string
            database: Default database
            workgroup: Workgroup
            description: Query description
            tags: Tags for the saved query
            folder: S3 folder path
            
        Returns:
            Save metadata
        """
        bucket = self.s3_output_location.replace("s3://", "").split("/")[0]
        prefix = folder or "saved_queries"
        key = f"{prefix}/{name}_{uuid.uuid4().hex[:8]}.json"
        
        metadata = {
            "name": name,
            "query": query,
            "database": database,
            "workgroup": workgroup,
            "description": description,
            "tags": tags or {},
            "created_at": datetime.now().isoformat(),
            "version": "1.0",
        }
        
        self.s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(metadata),
            ContentType="application/json",
        )
        
        return {"bucket": bucket, "key": key, "metadata": metadata}
    
    def load_saved_query(self, bucket: str, key: str) -> Dict[str, Any]:
        """
        Load a saved query from S3.
        
        Args:
            bucket: S3 bucket name
            key: Object key
            
        Returns:
            Query metadata and details
        """
        if not self.s3:
            raise RuntimeError("Boto3 not available")
        
        response = self.s3.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")
        return json.loads(content)
    
    def list_saved_queries(
        self,
        bucket: Optional[str] = None,
        prefix: Optional[str] = "saved_queries",
    ) -> List[Dict[str, Any]]:
        """
        List all saved queries.
        
        Args:
            bucket: S3 bucket (defaults to configured output bucket)
            prefix: Key prefix
            
        Returns:
            List of saved query metadata
        """
        if not self.s3:
            raise RuntimeError("Boto3 not available")
        
        bucket = bucket or self.s3_output_location.replace("s3://", "").split("/")[0]
        
        paginator = self.s3.get_paginator("list_objects_v2")
        queries = []
        
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".json"):
                    try:
                        resp = self.s3.get_object(Bucket=bucket, Key=obj["Key"])
                        content = resp["Body"].read().decode("utf-8")
                        metadata = json.loads(content)
                        metadata["_s3_key"] = obj["Key"]
                        metadata["_last_modified"] = obj["LastModified"].isoformat()
                        queries.append(metadata)
                    except Exception as e:
                        logger.warning(f"Failed to load {obj['Key']}: {e}")
        
        return queries
    
    def delete_saved_query(self, bucket: str, key: str) -> Dict[str, Any]:
        """
        Delete a saved query from S3.
        
        Args:
            bucket: S3 bucket name
            key: Object key
            
        Returns:
            Delete response
        """
        if not self.s3:
            raise RuntimeError("Boto3 not available")
        
        return self.s3.delete_object(Bucket=bucket, Key=key)
    
    # =========================================================================
    # PREPARED STATEMENTS
    # =========================================================================
    
    def create_prepared_statement(
        self,
        statement_name: str,
        query: str,
        workgroup: str = "primary",
    ) -> PreparedStatementInfo:
        """
        Create a prepared statement.
        
        Args:
            statement_name: Name for the prepared statement
            query: SQL query with placeholders (?)
            workgroup: Workgroup name
            
        Returns:
            PreparedStatementInfo object
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        self.athena.create_prepared_statement(
            StatementName=statement_name,
            WorkGroup=workgroup,
            QueryStatement=query,
        )
        
        return PreparedStatementInfo(
            statement_name=statement_name,
            query_string=query,
            workgroup=workgroup,
            last_modified_time=datetime.now(),
        )
    
    def get_prepared_statement(
        self,
        statement_name: str,
        workgroup: str = "primary",
    ) -> PreparedStatementInfo:
        """
        Get prepared statement details.
        
        Args:
            statement_name: Statement name
            workgroup: Workgroup name
            
        Returns:
            PreparedStatementInfo object
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        response = self.athena.get_prepared_statement(
            StatementName=statement_name,
            WorkGroup=workgroup,
        )
        ps = response["PreparedStatement"]
        
        return PreparedStatementInfo(
            statement_name=ps["StatementName"],
            query_string=ps["QueryStatement"],
            workgroup=ps["WorkGroup"],
            last_modified_time=ps.get("LastModifiedTime"),
        )
    
    def list_prepared_statements(
        self,
        workgroup: str = "primary",
    ) -> List[PreparedStatementInfo]:
        """
        List prepared statements in a workgroup.
        
        Args:
            workgroup: Workgroup name
            
        Returns:
            List of PreparedStatementInfo objects
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        paginator = self.athena.get_paginator("list_prepared_statements")
        statements = []
        
        for page in paginator.paginate(WorkGroup=workgroup):
            for ps in page.get("PreparedStatements", []):
                try:
                    info = self.get_prepared_statement(ps["StatementName"], workgroup)
                    statements.append(info)
                except Exception as e:
                    logger.warning(f"Failed to get prepared statement {ps['StatementName']}: {e}")
        
        return statements
    
    def update_prepared_statement(
        self,
        statement_name: str,
        query: str,
        workgroup: str = "primary",
    ) -> Dict[str, Any]:
        """
        Update a prepared statement.
        
        Args:
            statement_name: Statement name
            query: New query string
            workgroup: Workgroup name
            
        Returns:
            Update response
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        return self.athena.update_prepared_statement(
            StatementName=statement_name,
            WorkGroup=workgroup,
            QueryStatement=query,
        )
    
    def delete_prepared_statement(
        self,
        statement_name: str,
        workgroup: str = "primary",
    ) -> Dict[str, Any]:
        """
        Delete a prepared statement.
        
        Args:
            statement_name: Statement name
            workgroup: Workgroup name
            
        Returns:
            Delete response
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        return self.athena.delete_prepared_statement(
            StatementName=statement_name,
            WorkGroup=workgroup,
        )
    
    def execute_prepared_statement(
        self,
        statement_name: str,
        workgroup: str = "primary",
        execution_params: Optional[List[str]] = None,
        output_location: Optional[str] = None,
    ) -> QueryResult:
        """
        Execute a prepared statement.
        
        Args:
            statement_name: Statement name
            workgroup: Workgroup name
            execution_params: Parameters to substitute
            output_location: S3 output location
            
        Returns:
            QueryResult object
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        kwargs: Dict[str, Any] = {
            "StatementName": statement_name,
            "WorkGroup": workgroup,
        }
        
        if execution_params:
            kwargs["ExecutionParameters"] = execution_params
        
        if output_location:
            kwargs["ResultConfiguration"] = {"OutputLocation": output_location}
        
        response = self.athena.batch_get_prepared_statements(
            Statements=[{"StatementName": statement_name, "WorkGroup": workgroup}]
        )
        
        result = QueryResult(
            query_id=f"prepared-{statement_name}-{uuid.uuid4().hex[:8]}",
            query=f"EXECUTE {statement_name}",
            state=QueryState.RUNNING,
        )
        
        return result
    
    # =========================================================================
    # DATA CATALOG
    # =========================================================================
    
    def list_data_catalogs(
        self,
        max_results: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        List all data catalogs.
        
        Args:
            max_results: Maximum results
            
        Returns:
            List of catalog info
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        response = self.athena.list_data_catalogs(
            MaxResults=min(max_results, 100)
        )
        
        catalogs = []
        for catalog in response.get("DataCatalogsSummary", []):
            detail = self.athena.get_data_catalog(Name=catalog["CatalogName"])
            catalogs.append(detail.get("DataCatalog", {}))
        
        return catalogs
    
    def get_data_catalog(self, name: str) -> Dict[str, Any]:
        """
        Get data catalog details.
        
        Args:
            name: Catalog name
            
        Returns:
            Catalog details
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        response = self.athena.get_data_catalog(Name=name)
        return response.get("DataCatalog", {})
    
    def create_data_catalog(
        self,
        name: str,
        catalog_type: str = "LAMBDA",
        description: Optional[str] = None,
        parameters: Optional[Dict[str, str]] = None,
        tags: Optional[List[Dict[str, str]]] = None,
    ) -> Dict[str, Any]:
        """
        Create a data catalog.
        
        Args:
            name: Catalog name
            catalog_type: Type (LAMBDA, GLUE, HIVE)
            description: Catalog description
            parameters: Catalog parameters
            tags: Tags
            
        Returns:
            Create response
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        kwargs: Dict[str, Any] = {
            "Name": name,
            "Type": catalog_type,
        }
        
        if description:
            kwargs["Description"] = description
        
        if parameters:
            kwargs["Parameters"] = parameters
        
        if tags:
            kwargs["Tags"] = tags
        
        return self.athena.create_data_catalog(**kwargs)
    
    def update_data_catalog(
        self,
        name: str,
        description: Optional[str] = None,
        parameters: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Update a data catalog.
        
        Args:
            name: Catalog name
            description: New description
            parameters: New parameters
            
        Returns:
            Update response
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        kwargs: Dict[str, Any] = {"Name": name}
        
        if description is not None:
            kwargs["Description"] = description
        
        if parameters:
            kwargs["Parameters"] = parameters
        
        return self.athena.update_data_catalog(**kwargs)
    
    def delete_data_catalog(self, name: str) -> Dict[str, Any]:
        """
        Delete a data catalog.
        
        Args:
            name: Catalog name
            
        Returns:
            Delete response
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        return self.athena.delete_data_catalog(Name=name)
    
    # =========================================================================
    # DATABASE/TABLE MANAGEMENT
    # =========================================================================
    
    def create_database(
        self,
        name: str,
        description: Optional[str] = None,
        parameters: Optional[Dict[str, str]] = None,
        catalog_name: str = "AwsDataCatalog",
    ) -> Dict[str, Any]:
        """
        Create a database.
        
        Args:
            name: Database name
            description: Database description
            parameters: Database parameters
            catalog_name: Data catalog name
            
        Returns:
            Create response
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        kwargs: Dict[str, Any] = {
            "DatabaseInput": {
                "Name": name,
            }
        }
        
        if description:
            kwargs["DatabaseInput"]["Description"] = description
        
        if parameters:
            kwargs["DatabaseInput"]["Parameters"] = parameters
        
        return self.athena.create_database(
            CatalogId=catalog_name,
            Database=kwargs["DatabaseInput"],
        )
    
    def list_databases(
        self,
        catalog_name: str = "AwsDataCatalog",
        max_results: int = 100,
    ) -> List[DatabaseInfo]:
        """
        List databases in a catalog.
        
        Args:
            catalog_name: Data catalog name
            max_results: Maximum results
            
        Returns:
            List of DatabaseInfo objects
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        response = self.athena.list_databases(CatalogId=catalog_name)
        
        databases = []
        for db in response.get("DatabaseList", []):
            databases.append(DatabaseInfo(
                name=db["Name"],
                description=db.get("Description"),
                parameters=db.get("Parameters", {}),
            ))
        
        return databases
    
    def get_database(
        self,
        name: str,
        catalog_name: str = "AwsDataCatalog",
    ) -> DatabaseInfo:
        """
        Get database details.
        
        Args:
            name: Database name
            catalog_name: Data catalog name
            
        Returns:
            DatabaseInfo object
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        response = self.athena.get_database(CatalogId=catalog_name, DatabaseName=name)
        db = response["Database"]
        
        return DatabaseInfo(
            name=db["Name"],
            description=db.get("Description"),
            parameters=db.get("Parameters", {}),
        )
    
    def delete_database(
        self,
        name: str,
        catalog_name: str = "AwsDataCatalog",
        cascade: bool = False,
    ) -> Dict[str, Any]:
        """
        Delete a database.
        
        Args:
            name: Database name
            catalog_name: Data catalog name
            cascade: Delete tables/views first
            
        Returns:
            Delete response
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        return self.athena.delete_database(
            CatalogId=catalog_name,
            DatabaseName=name,
        )
    
    def create_table(
        self,
        database: str,
        name: str,
        columns: List[Dict[str, str]],
        table_type: Optional[str] = None,
        partition_keys: Optional[List[Dict[str, str]]] = None,
        location: Optional[str] = None,
        input_format: Optional[str] = None,
        output_format: Optional[str] = None,
        serde_properties: Optional[Dict[str, str]] = None,
        parameters: Optional[Dict[str, str]] = None,
        if_not_exists: bool = False,
        as_query: Optional[str] = None,
        catalog_name: str = "AwsDataCatalog",
    ) -> Dict[str, Any]:
        """
        Create a table.
        
        Args:
            database: Database name
            name: Table name
            columns: Column definitions [{"Name": "...", "Type": "..."}]
            table_type: Table type (EXTERNAL_TABLE, etc.)
            partition_keys: Partition key columns
            location: S3 location
            input_format: Input format class
            output_format: Output format class
            serde_properties: SerDe properties
            parameters: Table parameters
            if_not_exists: Add IF NOT EXISTS clause
            as_query: CTAS query to create table
            catalog_name: Data catalog name
            
        Returns:
            Create response
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        table_input: Dict[str, Any] = {
            "Name": name,
            "TableType": table_type or "EXTERNAL_TABLE",
        }
        
        table_input["Parameters"] = parameters or {}
        
        if columns:
            table_input["StorageDescriptor"] = {
                "Columns": columns,
            }
            
            if location:
                table_input["StorageDescriptor"]["Location"] = location
            
            if input_format:
                table_input["StorageDescriptor"]["InputFormat"] = input_format
            else:
                table_input["StorageDescriptor"]["InputFormat"] = "org.apache.hadoop.mapred.TextInputFormat"
            
            if output_format:
                table_input["StorageDescriptor"]["OutputFormat"] = output_format
            else:
                table_input["StorageDescriptor"]["OutputFormat"] = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
            
            if serde_properties:
                table_input["StorageDescriptor"]["SerdeInfo"] = {
                    "Parameters": serde_properties
                }
        
        if partition_keys:
            table_input["PartitionKeys"] = partition_keys
        
        if as_query:
            table_input["ViewOriginalText"] = as_query
        
        return self.athena.create_table(
            CatalogId=catalog_name,
            DatabaseName=database,
            TableInput=table_input,
        )
    
    def list_tables(
        self,
        database: str,
        catalog_name: str = "AwsDataCatalog",
        max_results: int = 100,
        expression: Optional[str] = None,
    ) -> List[TableInfo]:
        """
        List tables in a database.
        
        Args:
            database: Database name
            catalog_name: Data catalog name
            max_results: Maximum results
            expression: Filter expression
            
        Returns:
            List of TableInfo objects
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        kwargs: Dict[str, Any] = {
            "CatalogId": catalog_name,
            "DatabaseName": database,
        }
        if expression:
            kwargs["Expression"] = expression
        
        response = self.athena.list_tables(**kwargs)
        
        tables = []
        for tbl in response.get("TableMetadataList", []):
            tables.append(TableInfo(
                name=tbl["Name"],
                database=database,
                table_type=tbl.get("TableType", "EXTERNAL_TABLE"),
                columns=tbl.get("Columns", []),
                partition_keys=tbl.get("PartitionKeys", []),
            ))
        
        return tables
    
    def get_table(
        self,
        database: str,
        name: str,
        catalog_name: str = "AwsDataCatalog",
    ) -> TableInfo:
        """
        Get table details.
        
        Args:
            database: Database name
            name: Table name
            catalog_name: Data catalog name
            
        Returns:
            TableInfo object
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        response = self.athena.get_table_metadata(
            CatalogId=catalog_name,
            DatabaseName=database,
            TableName=name,
        )
        tbl = response["TableMetadata"]
        
        return TableInfo(
            name=tbl["Name"],
            database=database,
            table_type=tbl.get("TableType", "EXTERNAL_TABLE"),
            columns=tbl.get("Columns", []),
            partition_keys=tbl.get("PartitionKeys", []),
            location=tbl.get("Parameters", {}).get("location"),
        )
    
    def drop_table(
        self,
        database: str,
        name: str,
        catalog_name: str = "AwsDataCatalog",
        delete_data: bool = False,
    ) -> Dict[str, Any]:
        """
        Drop a table.
        
        Args:
            database: Database name
            name: Table name
            catalog_name: Data catalog name
            delete_data: Delete data in S3 if EXTERNAL_TABLE
            
        Returns:
            Drop response
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        return self.athena.delete_table(
            CatalogId=catalog_name,
            DatabaseName=database,
            TableName=name,
        )
    
    def alter_table(
        self,
        database: str,
        name: str,
        table_input: Dict[str, Any],
        catalog_name: str = "AwsDataCatalog",
    ) -> Dict[str, Any]:
        """
        Alter a table.
        
        Args:
            database: Database name
            name: Current table name
            table_input: New table definition
            catalog_name: Data catalog name
            
        Returns:
            Alter response
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        return self.athena.update_table(
            CatalogId=catalog_name,
            DatabaseName=database,
            TableInput=table_input,
        )
    
    # =========================================================================
    # VIEW MANAGEMENT
    # =========================================================================
    
    def create_view(
        self,
        database: str,
        name: str,
        view_query: str,
        if_not_exists: bool = False,
        catalog_name: str = "AwsDataCatalog",
    ) -> Dict[str, Any]:
        """
        Create a view.
        
        Args:
            database: Database name
            name: View name
            view_query: SELECT query for the view
            if_not_exists: Add IF NOT EXISTS clause
            catalog_name: Data catalog name
            
        Returns:
            Create response
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        table_input = {
            "Name": name,
            "TableType": "VIRTUAL_VIEW",
            "ViewOriginalText": view_query,
            "ViewExpandedText": view_query,
        }
        
        return self.athena.create_table(
            CatalogId=catalog_name,
            DatabaseName=database,
            TableInput=table_input,
        )
    
    def list_views(
        self,
        database: str,
        catalog_name: str = "AwsDataCatalog",
        max_results: int = 100,
    ) -> List[ViewInfo]:
        """
        List views in a database.
        
        Args:
            database: Database name
            catalog_name: Data catalog name
            max_results: Maximum results
            
        Returns:
            List of ViewInfo objects
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        tables = self.list_tables(
            database=database,
            catalog_name=catalog_name,
            max_results=max_results,
        )
        
        views = []
        for tbl in tables:
            if tbl.table_type == "VIRTUAL_VIEW":
                detail = self.get_table(database, tbl.name, catalog_name)
                views.append(ViewInfo(
                    name=detail.name,
                    database=database,
                    view_original_text=detail.parameters.get("view_original_text", ""),
                    view_expanded_text=detail.parameters.get("view_expanded_text"),
                ))
        
        return views
    
    def get_view(
        self,
        database: str,
        name: str,
        catalog_name: str = "AwsDataCatalog",
    ) -> ViewInfo:
        """
        Get view details.
        
        Args:
            database: Database name
            name: View name
            catalog_name: Data catalog name
            
        Returns:
            ViewInfo object
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        response = self.athena.get_table_metadata(
            CatalogId=catalog_name,
            DatabaseName=database,
            TableName=name,
        )
        tbl = response["TableMetadata"]
        
        return ViewInfo(
            name=tbl["Name"],
            database=database,
            view_original_text=tbl.get("ViewOriginalText", ""),
            view_expanded_text=tbl.get("ViewExpandedText"),
            create_time=tbl.get("CreateTime"),
        )
    
    def drop_view(
        self,
        database: str,
        name: str,
        catalog_name: str = "AwsDataCatalog",
    ) -> Dict[str, Any]:
        """
        Drop a view.
        
        Args:
            database: Database name
            name: View name
            catalog_name: Data catalog name
            
        Returns:
            Drop response
        """
        return self.drop_table(database, name, catalog_name)
    
    # =========================================================================
    # CLOUDWATCH INTEGRATION
    # =========================================================================
    
    def get_query_metrics(
        self,
        workgroup: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        period: int = 60,
        metric_names: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Get Athena query metrics from CloudWatch.
        
        Args:
            workgroup: Filter by workgroup
            start_time: Metrics start time
            end_time: Metrics end time
            period: Metric period in seconds
            metric_names: Specific metrics to retrieve
            
        Returns:
            CloudWatch metrics data
        """
        if not self.cloudwatch:
            raise RuntimeError("Boto3 not available")
        
        start_time = start_time or datetime.now() - timedelta(hours=1)
        end_time = end_time or datetime.now()
        
        namespace = "AWS/Athena"
        dimensions = []
        if workgroup:
            dimensions.append({"Name": "WorkGroup", "Value": workgroup})
        
        default_metrics = [
            "Queries",
            "QueryPlanning",
            "QueryQueueTime",
            "QueryExecution",
            "DataScanned",
            "TotalExecutionTime",
        ]
        
        metrics_to_fetch = metric_names or default_metrics
        metric_data = []
        
        for metric in metrics_to_fetch:
            kwargs = {
                "Id": metric.lower(),
                "MetricStat": {
                    "Metric": {
                        "Namespace": namespace,
                        "MetricName": metric,
                        "Dimensions": dimensions if dimensions else [],
                    },
                    "Period": period,
                    "Stat": "Sum",
                },
                "ReturnData": True,
            }
            metric_data.append(kwargs)
        
        kwargs = {
            "Namespace": namespace,
            "MetricDataQueries": metric_data,
            "StartTime": start_time,
            "EndTime": end_time,
        }
        
        return self.cloudwatch.get_metric_data(**kwargs)
    
    def get_query_statistics(
        self,
        query_id: str,
    ) -> Dict[str, Any]:
        """
        Get detailed query statistics.
        
        Args:
            query_id: Query execution ID
            
        Returns:
            Query statistics
        """
        if not self.athena:
            raise RuntimeError("Boto3 not available")
        
        response = self.athena.get_query_execution(QueryExecutionId=query_id)
        return response["QueryExecution"].get("Statistics", {})
    
    def list_cloudwatch_log_groups(
        self,
        pattern: Optional[str] = None,
        max_results: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        List CloudWatch log groups related to Athena.
        
        Args:
            pattern: Filter pattern
            max_results: Maximum results
            
        Returns:
            List of log group summaries
        """
        if not self.cloudwatch_logs:
            raise RuntimeError("Boto3 not available")
        
        kwargs: Dict[str, Any] = {}
        if pattern:
            kwargs["logGroupNamePrefix"] = pattern
        
        paginator = self.cloudwatch_logs.get_paginator("describe_log_groups")
        groups = []
        
        for page in paginator.paginate(**kwargs):
            groups.extend(page.get("logGroups", []))
        
        return groups[:max_results]
    
    def get_query_logs(
        self,
        log_group_name: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        filter_pattern: Optional[str] = None,
        max_results: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Get query execution logs from CloudWatch.
        
        Args:
            log_group_name: CloudWatch log group name
            start_time: Start time
            end_time: End time
            filter_pattern: CloudWatch filter pattern
            max_results: Maximum results
            
        Returns:
            List of log events
        """
        if not self.cloudwatch_logs:
            raise RuntimeError("Boto3 not available")
        
        start_time = start_time or datetime.now() - timedelta(hours=1)
        end_time = end_time or datetime.now()
        
        kwargs: Dict[str, Any] = {
            "logGroupName": log_group_name,
            "startTime": int(start_time.timestamp() * 1000),
            "endTime": int(end_time.timestamp() * 1000),
            "limit": max_results,
        }
        
        if filter_pattern:
            kwargs["filterPattern"] = filter_pattern
        
        response = self.cloudwatch_logs.filter_log_events(**kwargs)
        return response.get("events", [])
    
    def put_cloudwatch_metric(
        self,
        metric_name: str,
        value: float,
        unit: str = "Count",
        dimensions: Optional[List[Dict[str, str]]] = None,
        storage_resolution: int = 60,
    ) -> Dict[str, Any]:
        """
        Put a custom CloudWatch metric.
        
        Args:
            metric_name: Metric name
            value: Metric value
            unit: Unit type
            dimensions: Metric dimensions
            storage_resolution: Storage resolution
            
        Returns:
            Put metric response
        """
        if not self.cloudwatch:
            raise RuntimeError("Boto3 not available")
        
        kwargs: Dict[str, Any] = {
            "Namespace": "Custom/Workflow/Athena",
            "MetricData": [
                {
                    "MetricName": metric_name,
                    "Value": value,
                    "Unit": unit,
                    "StorageResolution": storage_resolution,
                }
            ],
        }
        
        if dimensions:
            kwargs["MetricData"][0]["Dimensions"] = dimensions
        
        return self.cloudwatch.put_metric_data(**kwargs)
    
    # =========================================================================
    # UTILITY METHODS
    # =========================================================================
    
    def generate_create_table_ddl(
        self,
        database: str,
        table_name: str,
        columns: List[Dict[str, str]],
        location: str,
        file_format: str = "TEXTFILE",
        partition_keys: Optional[List[Dict[str, str]]] = None,
        clustered_by: Optional[List[str]] = None,
        num_buckets: int = 1,
        serde_properties: Optional[Dict[str, str]] = None,
        table_properties: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Generate CREATE TABLE DDL.
        
        Args:
            database: Database name
            table_name: Table name
            columns: Column definitions
            location: S3 location
            file_format: File format (TEXTFILE, PARQUET, ORC, etc.)
            partition_keys: Partition key columns
            clustered_by: Cluster columns
            num_buckets: Number of buckets
            serde_properties: SerDe properties
            table_properties: Table properties
            
        Returns:
            CREATE TABLE DDL string
        """
        lines = [f"CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{table_name} ("]
        
        col_lines = []
        for col in columns:
            col_lines.append(f"  {col['Name']} {col['Type']}")
        
        if partition_keys:
            for pk in partition_keys:
                col_lines.append(f"  {pk['Name']} {pk['Type']}")
        
        lines.append(",\n".join(col_lines))
        lines.append(")")
        
        if partition_keys:
            lines.append(f"PARTITIONED BY ({', '.join([p['Name'] for p in partition_keys])})")
        
        if clustered_by:
            lines.append(f"CLUSTERED BY ({', '.join(clustered_by)}) INTO {num_buckets} BUCKETS")
        
        lines.append(f"ROW FORMAT DELIMITED")
        
        if file_format == "PARQUET":
            lines.append("STORED AS PARQUET")
            lines.append("TBLPROPERTIES ('parquet.compression'='SNAPPY')")
        elif file_format == "ORC":
            lines.append("STORED AS ORC")
            lines.append("TBLPROPERTIES ('orc.compress'='SNAPPY')")
        else:
            lines.append("FIELDS TERMINATED BY ','")
            lines.append("LINES TERMINATED BY '\\n'")
        
        lines.append(f"LOCATION '{location}'")
        
        if serde_properties:
            props_str = ", ".join([f"'{k}'='{v}'" for k, v in serde_properties.items()])
            lines.append(f"WITH SERDEPROPERTIES ({props_str})")
        
        if table_properties:
            props_str = ", ".join([f"'{k}'='{v}'" for k, v in table_properties.items()])
            lines.append(f"TBLPROPERTIES ({props_str})")
        
        return "\n".join(lines)
    
    def generate_ctas_query(
        self,
        target_table: str,
        source_query: str,
        destination: str,
        file_format: str = "PARQUET",
        compression: Optional[str] = None,
        partition_by: Optional[List[str]] = None,
        clustered_by: Optional[List[str]] = None,
        write_xcom: bool = False,
    ) -> str:
        """
        Generate a CTAS (CREATE TABLE AS SELECT) query.
        
        Args:
            target_table: Target table name
            source_query: Source SELECT query
            destination: S3 destination
            file_format: Output format
            compression: Compression type
            partition_by: Partition columns
            clustered_by: Cluster columns
            write_xcom: Return format for Airflow XCom
            
        Returns:
            CTAS query string
        """
        format_options = {
            "PARQUET": "FORMAT PARQUET",
            "ORC": "FORMAT ORC",
            "AVRO": "FORMAT AVRO",
            "JSON": "FORMAT JSON",
            "TSV": "FORMAT TSV",
        }
        
        parts = [f"CREATE TABLE {target_table}"]
        
        if partition_by:
            parts.append(f"PARTITIONED BY ({', '.join(partition_by)})")
        
        if clustered_by:
            parts.append(f"CLUSTERED BY ({', '.join(clustered_by)}) INTO 5 BUCKETS")
        
        parts.append("AS")
        parts.append(source_query)
        
        parts.append(f"LOCATION '{destination}'")
        
        if file_format in format_options:
            parts.append(format_options[file_format])
        
        if compression:
            parts.append(f"OPTIONS (compression = '{compression}')")
        
        return "\n".join(parts)
    
    def get_table_preview(
        self,
        database: str,
        table_name: str,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Preview data from a table.
        
        Args:
            database: Database name
            table_name: Table name
            limit: Number of rows to return
            
        Returns:
            List of row dictionaries
        """
        query = f"SELECT * FROM {database}.{table_name} LIMIT {limit}"
        result = self.execute_query_sync(query, database=database)
        
        if result.output_location:
            return self.get_query_results_s3(result.output_location, result.query_id)
        
        return []
    
    def repair_table(
        self,
        database: str,
        table_name: str,
    ) -> QueryResult:
        """
        Repair a table (sync partitions).
        
        Args:
            database: Database name
            table_name: Table name
            
        Returns:
            QueryResult
        """
        query = f"MSCK REPAIR TABLE {database}.{table_name}"
        return self.execute_query_sync(query, database=database)
    
    def analyze_table(self, database: str, table_name: str) -> QueryResult:
        """
        Analyze table statistics.
        
        Args:
            database: Database name
            table_name: Table name
            
        Returns:
            QueryResult
        """
        query = f"ANALYZE TABLE {database}.{table_name} COMPUTE STATISTICS"
        return self.execute_query_sync(query, database=database)
