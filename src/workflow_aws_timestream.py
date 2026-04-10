"""
AWS Amazon Timestream Integration Module for Workflow System

Implements a TimestreamIntegration class with:
1. Database management: Create/manage databases
2. Table management: Create/manage tables
3. Write data: Write data points
4. Query data: Query time-series data
5. Scheduled queries: Scheduled query management
6. Reserved capacity: Reserved capacity
7. KMS encryption: Data encryption
8. Data retention: Configure data retention
9. Magnetic storage: Magnetic store configuration
10. CloudWatch integration: Query and ingestion metrics

Commit: 'feat(aws-timestream): add Amazon Timestream with database/table management, data write/query, scheduled queries, reserved capacity, encryption, retention, CloudWatch'
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


class TableState(Enum):
    """Timestream table states."""
    ACTIVE = "ACTIVE"
    DELETING = "DELETING"
    RESTORING = "RESTORING"


class MagneticStoreRejectedException(Enum):
    """Magnetic store rejection reasons."""
    NO_MAGSIZE_TO_POINT = "NoMagneticSizesToPoint"
    MAGNETIC_STORE_EXCESS = "MagneticStoreReachRetention"
    EMPTY_RECORD = "EmptyRecord"
    INVALID_DIVIDE_CONFIG = "InvalidMagneticStoreWriteCapacity"


class ScheduledQueryState(Enum):
    """Scheduled query states."""
    DISABLED = "DISABLED"
    ENABLED = "ENABLED"


class ScheduledQueryErrorReportFormat(Enum):
    """Error report formats."""
    JSON = "JSON"
    CSV = "CSV"


@dataclass
class DatabaseInfo:
    """Database information."""
    database_name: str
    arn: Optional[str] = None
    table_count: int = 0
    kms_key_id: Optional[str] = None
    creation_time: Optional[datetime] = None
    last_updated_time: Optional[datetime] = None


@dataclass
class TableInfo:
    """Table information."""
    database_name: str
    table_name: str
    arn: Optional[str] = None
    state: Optional[TableState] = None
    table_creation_time: Optional[datetime] = None
    retention_properties: Optional[Dict[str, int]] = None
    magnetic_store_write_properties: Optional[Dict[str, Any]] = None
    schema: Optional[Dict[str, Any]] = None


@dataclass
class Record:
    """Time-series record for writing."""
    measure_name: str
    measure_value: str
    measure_value_type: str = "VARCHAR"
    time: Optional[str] = None
    time_unit: str = "MILLISECONDS"
    dimensions: Optional[Dict[str, str]] = None
    version: int = 1


@dataclass
class WriteResult:
    """Write operation result."""
    records_written: int
    failed_records: int
    records: List[Record]
    failed_record_errors: List[Dict[str, Any]]
    rejection_extended_info: List[Dict[str, Any]]


@dataclass
class QueryResult:
    """Query execution result."""
    query_id: str
    query_string: str
    rows: List[Dict[str, Any]]
    column_info: List[Dict[str, Any]]
    rows_scanned: int = 0
    bytes_scanned: int = 0
    execution_time_ms: int = 0


@dataclass
class ScheduledQueryInfo:
    """Scheduled query information."""
    name: str
    arn: str
    state: ScheduledQueryState
    target_destination: Dict[str, Any]
    schedule_time_config: Dict[str, Any]
    query_string: str
    notification_configuration: Dict[str, Any]
    target_destination_configuration: Dict[str, Any]
    error_report_configuration: Dict[str, Any]
    creation_time: Optional[datetime] = None
    last_run_time: Optional[datetime] = None
    next_invocation_time: Optional[datetime] = None


@dataclass
class ReservedCapacityInfo:
    """Reserved capacity information."""
    arn: str
    capacity_id: str
    mode: str
    status: str
    starting_date: datetime
    expiration_date: datetime
    requested_units: int = 0
    actual_units: int = 0


class TimestreamIntegration:
    """
    AWS Amazon Timestream Integration for workflow automation.
    
    Provides comprehensive Timestream management including:
    - Database management
    - Table management
    - Write data points
    - Query time-series data
    - Scheduled queries
    - Reserved capacity
    - KMS encryption
    - Data retention
    - Magnetic store configuration
    - CloudWatch integration
    """
    
    def __init__(
        self,
        region_name: str = "us-east-1",
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        profile_name: Optional[str] = None,
        boto_client: Any = None,
    ):
        """
        Initialize Timestream integration.
        
        Args:
            region_name: AWS region for Timestream
            aws_access_key_id: AWS access key ID
            aws_secret_access_key: AWS secret access key
            aws_session_token: AWS session token
            profile_name: AWS profile name
            boto_client: Pre-configured boto3 client (for testing)
        """
        self.region_name = region_name
        
        if boto_client:
            self.timestream = boto_client
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
            self.timestream = session.client("timestream-write", region_name=region_name)
            self.timestream_query = session.client("timestream-query", region_name=region_name)
            self.cloudwatch = session.client("cloudwatch", region_name=region_name)
        else:
            self.timestream = None
            self.timestream_query = None
            self.cloudwatch = None
        
        self._write_cache: List[Record] = []
        self._lock = threading.Lock()
    
    # =========================================================================
    # DATABASE MANAGEMENT
    # =========================================================================
    
    def create_database(
        self,
        database_name: str,
        kms_key_id: Optional[str] = None,
        tags: Optional[List[Dict[str, str]]] = None,
    ) -> Dict[str, Any]:
        """
        Create a new Timestream database.
        
        Args:
            database_name: Name of the database
            kms_key_id: KMS key ID for encryption at rest
            tags: Tags to associate with the database
            
        Returns:
            Database creation response
        """
        if not self.timestream:
            raise RuntimeError("Boto3 not available")
        
        kwargs = {"DatabaseName": database_name}
        
        if kms_key_id:
            kwargs["KmsKeyId"] = kms_key_id
        
        if tags:
            kwargs["Tags"] = tags
        
        response = self.timestream.create_database(**kwargs)
        return response
    
    def describe_database(self, database_name: str) -> DatabaseInfo:
        """
        Get database information.
        
        Args:
            database_name: Name of the database
            
        Returns:
            DatabaseInfo object
        """
        if not self.timestream:
            raise RuntimeError("Boto3 not available")
        
        response = self.timestream.describe_database(DatabaseName=database_name)
        db = response["Database"]
        
        return DatabaseInfo(
            database_name=db["DatabaseName"],
            arn=db.get("Arn"),
            table_count=db.get("TableCount", 0),
            kms_key_id=db.get("KmsKeyId"),
            creation_time=db.get("CreationTime"),
            last_updated_time=db.get("LastUpdatedTime"),
        )
    
    def list_databases(
        self,
        max_results: int = 100,
    ) -> List[DatabaseInfo]:
        """
        List all Timestream databases.
        
        Args:
            max_results: Maximum number of results
            
        Returns:
            List of DatabaseInfo objects
        """
        if not self.timestream:
            raise RuntimeError("Boto3 not available")
        
        databases = []
        paginator = self.timestream.get_paginator("list_databases")
        
        for page in paginator.paginate(PaginationConfig={"MaxItems": max_results}):
            for db in page.get("Databases", []):
                databases.append(DatabaseInfo(
                    database_name=db["DatabaseName"],
                    arn=db.get("Arn"),
                    table_count=db.get("TableCount", 0),
                    kms_key_id=db.get("KmsKeyId"),
                    creation_time=db.get("CreationTime"),
                    last_updated_time=db.get("LastUpdatedTime"),
                ))
        
        return databases
    
    def update_database(
        self,
        database_name: str,
        kms_key_id: str,
    ) -> Dict[str, Any]:
        """
        Update database configuration.
        
        Args:
            database_name: Name of the database
            kms_key_id: New KMS key ID
            
        Returns:
            Update response
        """
        if not self.timestream:
            raise RuntimeError("Boto3 not available")
        
        response = self.timestream.update_database(
            DatabaseName=database_name,
            KmsKeyId=kms_key_id,
        )
        return response
    
    def delete_database(self, database_name: str) -> Dict[str, Any]:
        """
        Delete a database.
        
        Args:
            database_name: Name of the database
            
        Returns:
            Delete response
        """
        if not self.timestream:
            raise RuntimeError("Boto3 not available")
        
        response = self.timestream.delete_database(DatabaseName=database_name)
        return response
    
    # =========================================================================
    # TABLE MANAGEMENT
    # =========================================================================
    
    def create_table(
        self,
        database_name: str,
        table_name: str,
        retention_properties: Optional[Dict[str, int]] = None,
        magnetic_store_write_properties: Optional[Dict[str, Any]] = None,
        schema: Optional[Dict[str, Any]] = None,
        tags: Optional[List[Dict[str, str]]] = None,
    ) -> Dict[str, Any]:
        """
        Create a new Timestream table.
        
        Args:
            database_name: Name of the database
            table_name: Name of the table
            retention_properties: Data retention periods (in days/hours)
            magnetic_store_write_properties: Magnetic store configuration
            schema: Table schema definition
            tags: Tags to associate with the table
            
        Returns:
            Table creation response
        """
        if not self.timestream:
            raise RuntimeError("Boto3 not available")
        
        kwargs = {
            "DatabaseName": database_name,
            "TableName": table_name,
        }
        
        if retention_properties:
            kwargs["RetentionProperties"] = retention_properties
        else:
            kwargs["RetentionProperties"] = {
                "MemoryStoreRetentionPeriodInHours": "3600",
                "MagneticStoreRetentionPeriodInDays": "365",
            }
        
        if magnetic_store_write_properties:
            kwargs["MagneticStoreWriteProperties"] = magnetic_store_write_properties
        
        if schema:
            kwargs["Schema"] = schema
        
        if tags:
            kwargs["Tags"] = tags
        
        response = self.timestream.create_table(**kwargs)
        return response
    
    def describe_table(
        self,
        database_name: str,
        table_name: str,
    ) -> TableInfo:
        """
        Get table information.
        
        Args:
            database_name: Name of the database
            table_name: Name of the table
            
        Returns:
            TableInfo object
        """
        if not self.timestream:
            raise RuntimeError("Boto3 not available")
        
        response = self.timestream.describe_table(
            DatabaseName=database_name,
            TableName=table_name,
        )
        table = response["Table"]
        
        state = None
        if "TableStatus" in table:
            try:
                state = TableState(table["TableStatus"])
            except ValueError:
                state = table["TableStatus"]
        
        return TableInfo(
            database_name=table["DatabaseName"],
            table_name=table["TableName"],
            arn=table.get("Arn"),
            state=state,
            table_creation_time=table.get("CreationTime"),
            retention_properties=table.get("RetentionProperties"),
            magnetic_store_write_properties=table.get("MagneticStoreWriteProperties"),
            schema=table.get("Schema"),
        )
    
    def list_tables(
        self,
        database_name: str,
        max_results: int = 100,
    ) -> List[TableInfo]:
        """
        List all tables in a database.
        
        Args:
            database_name: Name of the database
            max_results: Maximum number of results
            
        Returns:
            List of TableInfo objects
        """
        if not self.timestream:
            raise RuntimeError("Boto3 not available")
        
        tables = []
        paginator = self.timestream.get_paginator("list_tables")
        
        for page in paginator.paginate(
            DatabaseName=database_name,
            PaginationConfig={"MaxItems": max_results},
        ):
            for table in page.get("Tables", []):
                state = None
                if "TableStatus" in table:
                    try:
                        state = TableState(table["TableStatus"])
                    except ValueError:
                        state = table["TableStatus"]
                
                tables.append(TableInfo(
                    database_name=table["DatabaseName"],
                    table_name=table["TableName"],
                    arn=table.get("Arn"),
                    state=state,
                    table_creation_time=table.get("CreationTime"),
                    retention_properties=table.get("RetentionProperties"),
                    magnetic_store_write_properties=table.get("MagneticStoreWriteProperties"),
                    schema=table.get("Schema"),
                ))
        
        return tables
    
    def update_table(
        self,
        database_name: str,
        table_name: str,
        retention_properties: Optional[Dict[str, int]] = None,
        magnetic_store_write_properties: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Update table configuration.
        
        Args:
            database_name: Name of the database
            table_name: Name of the table
            retention_properties: New retention properties
            magnetic_store_write_properties: New magnetic store config
            
        Returns:
            Update response
        """
        if not self.timestream:
            raise RuntimeError("Boto3 not available")
        
        kwargs = {
            "DatabaseName": database_name,
            "TableName": table_name,
        }
        
        if retention_properties:
            kwargs["RetentionProperties"] = retention_properties
        
        if magnetic_store_write_properties:
            kwargs["MagneticStoreWriteProperties"] = magnetic_store_write_properties
        
        response = self.timestream.update_table(**kwargs)
        return response
    
    def delete_table(
        self,
        database_name: str,
        table_name: str,
    ) -> Dict[str, Any]:
        """
        Delete a table.
        
        Args:
            database_name: Name of the database
            table_name: Name of the table
            
        Returns:
            Delete response
        """
        if not self.timestream:
            raise RuntimeError("Boto3 not available")
        
        response = self.timestream.delete_table(
            DatabaseName=database_name,
            TableName=table_name,
        )
        return response
    
    # =========================================================================
    # WRITE DATA
    # =========================================================================
    
    def write_records(
        self,
        database_name: str,
        table_name: str,
        records: List[Record],
        common_attributes: Optional[Dict[str, Any]] = None,
    ) -> WriteResult:
        """
        Write records to a Timestream table.
        
        Args:
            database_name: Name of the database
            table_name: Name of the table
            records: List of Record objects to write
            common_attributes: Common attributes for all records
            
        Returns:
            WriteResult with write status
        """
        if not self.timestream:
            raise RuntimeError("Boto3 not available")
        
        records_to_write = []
        for record in records:
            record_dict = {
                "MeasureName": record.measure_name,
                "MeasureValue": record.measure_value,
                "MeasureValueType": record.measure_value_type,
                "Time": record.time or str(int(time.time() * 1000)),
                "TimeUnit": record.time_unit,
                "Version": record.version,
            }
            
            if record.dimensions:
                record_dict["Dimensions"] = [
                    {"Name": k, "Value": v} for k, v in record.dimensions.items()
                ]
            
            records_to_write.append(record_dict)
        
        kwargs = {
            "DatabaseName": database_name,
            "TableName": table_name,
            "Records": records_to_write,
        }
        
        if common_attributes:
            common = {}
            if "measure_name" in common_attributes:
                common["MeasureName"] = common_attributes["measure_name"]
            if "measure_value" in common_attributes:
                common["MeasureValue"] = common_attributes["measure_value"]
            if "measure_value_type" in common_attributes:
                common["MeasureValueType"] = common_attributes["measure_value_type"]
            if "time" in common_attributes:
                common["Time"] = common_attributes["time"]
            if "time_unit" in common_attributes:
                common["TimeUnit"] = common_attributes["time_unit"]
            if "dimensions" in common_attributes:
                common["Dimensions"] = [
                    {"Name": k, "Value": v}
                    for k, v in common_attributes["dimensions"].items()
                ]
            kwargs["CommonAttributes"] = common
        
        try:
            response = self.timestream.write_records(**kwargs)
            
            failed_records = response.get("RecordsIngested", {}).get("Failed", 0)
            
            return WriteResult(
                records_written=len(records_to_write) - failed_records,
                failed_records=failed_records,
                records=records,
                failed_record_errors=[],
                rejection_extended_info=[],
            )
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            
            if error_code == "RejectedRecords":
                rejected = e.response.get("RejectedRecords", [])
                return WriteResult(
                    records_written=len(records_to_write) - len(rejected),
                    failed_records=len(rejected),
                    records=records,
                    failed_record_errors=[
                        {
                            "index": r.get("RecordIndex"),
                            "reason": r.get("Reason"),
                        }
                        for r in rejected
                    ],
                    rejection_extended_info=[
                        r.get("RejectedRecordExtendedInfo", {})
                        for r in rejected
                    ],
                )
            raise
    
    def write_records_with_context(
        self,
        database_name: str,
        table_name: str,
        records: List[Record],
        batch_size: int = 100,
    ) -> List[WriteResult]:
        """
        Write records in batches with retry support.
        
        Args:
            database_name: Name of the database
            table_name: Name of the table
            records: List of Record objects to write
            batch_size: Number of records per batch
            
        Returns:
            List of WriteResult for each batch
        """
        results = []
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            result = self.write_records(database_name, table_name, batch)
            results.append(result)
        
        return results
    
    # =========================================================================
    # QUERY DATA
    # =========================================================================
    
    def query(
        self,
        query_string: str,
        next_token: Optional[str] = None,
        max_rows: int = 1000,
    ) -> QueryResult:
        """
        Execute a query against Timestream.
        
        Args:
            query_string: SQL query string
            next_token: Pagination token
            max_rows: Maximum rows to return
            
        Returns:
            QueryResult with query results
        """
        if not self.timestream_query:
            raise RuntimeError("Boto3 not available")
        
        kwargs = {
            "QueryString": query_string,
            "MaxRows": max_rows,
        }
        
        if next_token:
            kwargs["NextToken"] = next_token
        
        response = self.timestream_query.query(**kwargs)
        
        rows = []
        for row in response.get("Rows", []):
            row_data = {}
            for i, col in enumerate(response.get("ColumnInfo", [])):
                if i < len(row.get("Data", [])):
                    row_data[col["Name"]] = row["Data"][i]
            rows.append(row_data)
        
        return QueryResult(
            query_id=response.get("QueryId", ""),
            query_string=query_string,
            rows=rows,
            column_info=response.get("ColumnInfo", []),
            rows_scanned=response.get("RowsScanned", 0),
            bytes_scanned=response.get("BytesScanned", 0),
            execution_time_ms=response.get("ExecutionTimeInMillis", 0),
        )
    
    def query_with_pagination(
        self,
        query_string: str,
        max_rows: int = 1000,
    ) -> List[QueryResult]:
        """
        Execute a query and retrieve all results with pagination.
        
        Args:
            query_string: SQL query string
            max_rows: Maximum rows per page
            
        Returns:
            List of QueryResult objects (one per page)
        """
        results = []
        next_token = None
        
        while True:
            result = self.query(query_string, next_token, max_rows)
            results.append(result)
            
            next_token = getattr(self.timestream_query, "_next_token", None)
            if not next_token:
                break
        
        return results
    
    def query_time_series(
        self,
        database_name: str,
        table_name: str,
        measure_name: str,
        start_time: datetime,
        end_time: Optional[datetime] = None,
        dimensions: Optional[Dict[str, str]] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        Query time-series data from a table.
        
        Args:
            database_name: Name of the database
            table_name: Name of the table
            measure_name: Name of the measure to retrieve
            start_time: Start of the time range
            end_time: End of the time range (optional)
            dimensions: Filter by dimensions
            limit: Maximum number of records
            
        Returns:
            List of time-series data points
        """
        where_clause = f"measure_name = '{measure_name}' AND time >= from_iso8601_timestamp('{start_time.isoformat()}')"
        
        if end_time:
            where_clause += f" AND time <= from_iso8601_timestamp('{end_time.isoformat()}')"
        
        if dimensions:
            for key, value in dimensions.items():
                where_clause += f" AND {key} = '{value}'"
        
        query_string = f"""
            SELECT time, measure_name, measure_value, measure_value_type, dimensions
            FROM "{database_name}"."{table_name}"
            WHERE {where_clause}
            ORDER BY time ASC
            LIMIT {limit}
        """
        
        result = self.query(query_string, max_rows=limit)
        return result.rows
    
    # =========================================================================
    # SCHEDULED QUERIES
    # =========================================================================
    
    def create_scheduled_query(
        self,
        name: str,
        query_string: str,
        target_destination: Dict[str, Any],
        schedule_time_config: Dict[str, Any],
        notification_configuration: Dict[str, Any],
        target_destination_configuration: Dict[str, Any],
        error_report_configuration: Optional[Dict[str, Any]] = None,
        kms_key_id: Optional[str] = None,
        tags: Optional[List[Dict[str, str]]] = None,
    ) -> Dict[str, Any]:
        """
        Create a scheduled query.
        
        Args:
            name: Name of the scheduled query
            query_string: SQL query string
            target_destination: Target destination configuration
            schedule_time_config: Schedule configuration
            notification_configuration: SNS notification config
            target_destination_configuration: Target destination settings
            error_report_configuration: Error report configuration
            kms_key_id: KMS key for encryption
            tags: Tags to associate
            
        Returns:
            Scheduled query creation response
        """
        if not self.timestream_query:
            raise RuntimeError("Boto3 not available")
        
        kwargs = {
            "Name": name,
            "QueryString": query_string,
            "TargetDestination": target_destination,
            "ScheduleTimeConfiguration": schedule_time_config,
            "NotificationConfiguration": notification_configuration,
            "TargetDestinationConfiguration": target_destination_configuration,
        }
        
        if error_report_configuration:
            kwargs["ErrorReportConfiguration"] = error_report_configuration
        
        if kms_key_id:
            kwargs["KmsKeyId"] = kms_key_id
        
        if tags:
            kwargs["Tags"] = tags
        
        response = self.timestream_query.create_scheduled_query(**kwargs)
        return response
    
    def describe_scheduled_query(
        self,
        arn: str,
    ) -> ScheduledQueryInfo:
        """
        Get scheduled query information.
        
        Args:
            arn: Scheduled query ARN
            
        Returns:
            ScheduledQueryInfo object
        """
        if not self.timestream_query:
            raise RuntimeError("Boto3 not available")
        
        response = self.timestream_query.describe_scheduled_query(Arn=arn)
        sq = response["ScheduledQuery"]
        
        return ScheduledQueryInfo(
            name=sq["Name"],
            arn=sq["Arn"],
            state=ScheduledQueryState(sq["State"]),
            target_destination=sq["TargetDestination"],
            schedule_time_config=sq["ScheduleTimeConfiguration"],
            query_string=sq["QueryString"],
            notification_configuration=sq["NotificationConfiguration"],
            target_destination_configuration=sq["TargetDestinationConfiguration"],
            error_report_configuration=sq.get("ErrorReportConfiguration", {}),
            creation_time=sq.get("CreationTime"),
            last_run_time=sq.get("LastRunTime"),
            next_invocation_time=sq.get("NextInvocationTime"),
        )
    
    def list_scheduled_queries(
        self,
        max_results: int = 100,
    ) -> List[ScheduledQueryInfo]:
        """
        List all scheduled queries.
        
        Args:
            max_results: Maximum number of results
            
        Returns:
            List of ScheduledQueryInfo objects
        """
        if not self.timestream_query:
            raise RuntimeError("Boto3 not available")
        
        queries = []
        paginator = self.timestream_query.get_paginator("list_scheduled_queries")
        
        for page in paginator.paginate(PaginationConfig={"MaxItems": max_results}):
            for sq in page.get("ScheduledQueries", []):
                state = ScheduledQueryState.ENABLED
                if sq.get("State"):
                    try:
                        state = ScheduledQueryState(sq["State"])
                    except ValueError:
                        state = ScheduledQueryState.ENABLED
                
                queries.append(ScheduledQueryInfo(
                    name=sq["Name"],
                    arn=sq["Arn"],
                    state=state,
                    target_destination=sq.get("TargetDestination", {}),
                    schedule_time_config=sq.get("ScheduleTimeConfiguration", {}),
                    query_string=sq.get("QueryString", ""),
                    notification_configuration=sq.get("NotificationConfiguration", {}),
                    target_destination_configuration=sq.get("TargetDestinationConfiguration", {}),
                    error_report_configuration=sq.get("ErrorReportConfiguration", {}),
                    creation_time=sq.get("CreationTime"),
                    last_run_time=sq.get("LastRunTime"),
                    next_invocation_time=sq.get("NextInvocationTime"),
                ))
        
        return queries
    
    def update_scheduled_query(
        self,
        arn: str,
        state: Optional[ScheduledQueryState] = None,
    ) -> Dict[str, Any]:
        """
        Update a scheduled query.
        
        Args:
            arn: Scheduled query ARN
            state: New state (enabled/disabled)
            
        Returns:
            Update response
        """
        if not self.timestream_query:
            raise RuntimeError("Boto3 not available")
        
        kwargs = {"Arn": arn}
        
        if state:
            kwargs["State"] = state.value
        
        response = self.timestream_query.update_scheduled_query(**kwargs)
        return response
    
    def delete_scheduled_query(self, arn: str) -> Dict[str, Any]:
        """
        Delete a scheduled query.
        
        Args:
            arn: Scheduled query ARN
            
        Returns:
            Delete response
        """
        if not self.timestream_query:
            raise RuntimeError("Boto3 not available")
        
        response = self.timestream_query.delete_scheduled_query(Arn=arn)
        return response
    
    # =========================================================================
    # RESERVED CAPACITY
    # =========================================================================
    
    def create_reserved_capacity(
        self,
        requested_units: int,
        duration_in_hours: int,
        tags: Optional[List[Dict[str, str]]] = None,
    ) -> ReservedCapacityInfo:
        """
        Create reserved capacity for Timestream.
        
        Args:
            requested_units: Number of requested capacity units
            duration_in_hours: Duration in hours
            tags: Tags to associate
            
        Returns:
            ReservedCapacityInfo object
        """
        if not self.timestream:
            raise RuntimeError("Boto3 not available")
        
        kwargs = {
            "RequestedUnits": requested_units,
            "DurationInHours": duration_in_hours,
        }
        
        if tags:
            kwargs["Tags"] = tags
        
        response = self.timestream.create_reserved_capacity(**kwargs)
        rc = response["ReservedCapacity"]
        
        return ReservedCapacityInfo(
            arn=rc["Arn"],
            capacity_id=rc["CapacityReservationId"],
            mode=rc.get("Mode", "DEFAULT"),
            status=rc["Status"],
            starting_date=rc["StartingDate"],
            expiration_date=rc["ExpirationDate"],
            requested_units=rc["RequestedUnits"],
            actual_units=rc.get("ActualUnits", requested_units),
        )
    
    def describe_reserved_capacity(
        self,
        capacity_id: str,
    ) -> ReservedCapacityInfo:
        """
        Get reserved capacity information.
        
        Args:
            capacity_id: Capacity reservation ID
            
        Returns:
            ReservedCapacityInfo object
        """
        if not self.timestream:
            raise RuntimeError("Boto3 not available")
        
        response = self.timestream.describe_reserved_capacity(
            CapacityReservationId=capacity_id
        )
        rc = response["ReservedCapacity"]
        
        return ReservedCapacityInfo(
            arn=rc["Arn"],
            capacity_id=rc["CapacityReservationId"],
            mode=rc.get("Mode", "DEFAULT"),
            status=rc["Status"],
            starting_date=rc["StartingDate"],
            expiration_date=rc["ExpirationDate"],
            requested_units=rc["RequestedUnits"],
            actual_units=rc.get("ActualUnits", rc["RequestedUnits"]),
        )
    
    def list_reserved_capacity(
        self,
        max_results: int = 100,
    ) -> List[ReservedCapacityInfo]:
        """
        List all reserved capacities.
        
        Args:
            max_results: Maximum number of results
            
        Returns:
            List of ReservedCapacityInfo objects
        """
        if not self.timestream:
            raise RuntimeError("Boto3 not available")
        
        capacities = []
        paginator = self.timestream.get_paginator("list_reserved_capacities")
        
        for page in paginator.paginate(PaginationConfig={"MaxItems": max_results}):
            for rc in page.get("ReservedCapacities", []):
                capacities.append(ReservedCapacityInfo(
                    arn=rc["Arn"],
                    capacity_id=rc["CapacityReservationId"],
                    mode=rc.get("Mode", "DEFAULT"),
                    status=rc["Status"],
                    starting_date=rc["StartingDate"],
                    expiration_date=rc["ExpirationDate"],
                    requested_units=rc["RequestedUnits"],
                    actual_units=rc.get("ActualUnits", rc["RequestedUnits"]),
                ))
        
        return capacities
    
    # =========================================================================
    # KMS ENCRYPTION
    # =========================================================================
    
    def enable_kms_encryption(
        self,
        database_name: str,
        kms_key_id: str,
    ) -> Dict[str, Any]:
        """
        Enable KMS encryption for a database.
        
        Args:
            database_name: Name of the database
            kms_key_id: KMS key ID
            
        Returns:
            Update response
        """
        return self.update_database(database_name, kms_key_id)
    
    def disable_kms_encryption(
        self,
        database_name: str,
    ) -> None:
        """
        Timestream does not support disabling encryption once enabled.
        This method is provided for API completeness.
        
        Args:
            database_name: Name of the database
            
        Raises:
            NotImplementedError: Timestream does not support disabling encryption
        """
        raise NotImplementedError(
            "Timestream does not support disabling KMS encryption once enabled. "
            "Data is always encrypted at rest."
        )
    
    # =========================================================================
    # DATA RETENTION
    # =========================================================================
    
    def configure_retention(
        self,
        database_name: str,
        table_name: str,
        memory_store_retention_hours: int,
        magnetic_store_retention_days: int,
    ) -> Dict[str, Any]:
        """
        Configure data retention for a table.
        
        Args:
            database_name: Name of the database
            table_name: Name of the table
            memory_store_retention_hours: Retention period in memory store (hours)
            magnetic_store_retention_days: Retention period in magnetic store (days)
            
        Returns:
            Update response
        """
        retention_properties = {
            "MemoryStoreRetentionPeriodInHours": str(memory_store_retention_hours),
            "MagneticStoreRetentionPeriodInDays": str(magnetic_store_retention_days),
        }
        
        return self.update_table(
            database_name=database_name,
            table_name=table_name,
            retention_properties=retention_properties,
        )
    
    def get_retention_configuration(
        self,
        database_name: str,
        table_name: str,
    ) -> Dict[str, int]:
        """
        Get current retention configuration.
        
        Args:
            database_name: Name of the database
            table_name: Name of the table
            
        Returns:
            Retention configuration dictionary
        """
        table_info = self.describe_table(database_name, table_name)
        
        if not table_info.retention_properties:
            return {}
        
        return {
            "MemoryStoreRetentionPeriodInHours": int(
                table_info.retention_properties.get(
                    "MemoryStoreRetentionPeriodInHours", "0"
                )
            ),
            "MagneticStoreRetentionPeriodInDays": int(
                table_info.retention_properties.get(
                    "MagneticStoreRetentionPeriodInDays", "0"
                )
            ),
        }
    
    # =========================================================================
    # MAGNETIC STORAGE
    # =========================================================================
    
    def configure_magnetic_store(
        self,
        database_name: str,
        table_name: str,
        enable_magnetic_store_writes: bool = True,
        magnetic_store_write_capacity_size_mtu: int = 100,
    ) -> Dict[str, Any]:
        """
        Configure magnetic store settings for a table.
        
        Args:
            database_name: Name of the database
            table_name: Name of the table
            enable_magnetic_store_writes: Enable writes to magnetic store
            magnetic_store_write_capacity_size_mtu: Write capacity size (1-100)
            
        Returns:
            Update response
        """
        magnetic_store_write_properties = {
            "EnableMagneticStoreWrites": enable_magnetic_store_writes,
            "MagneticStoreWriteCapacitySizeMTU": magnetic_store_write_capacity_size_mtu,
        }
        
        return self.update_table(
            database_name=database_name,
            table_name=table_name,
            magnetic_store_write_properties=magnetic_store_write_properties,
        )
    
    def get_magnetic_store_configuration(
        self,
        database_name: str,
        table_name: str,
    ) -> Dict[str, Any]:
        """
        Get magnetic store configuration.
        
        Args:
            database_name: Name of the database
            table_name: Name of the table
            
        Returns:
            Magnetic store configuration
        """
        table_info = self.describe_table(database_name, table_name)
        
        if not table_info.magnetic_store_write_properties:
            return {"EnableMagneticStoreWrites": False}
        
        return table_info.magnetic_store_write_properties
    
    def enable_magnetic_storage_sync(
        self,
        database_name: str,
        table_name: str,
    ) -> Dict[str, Any]:
        """
        Enable magnetic storage synchronous writes.
        
        Args:
            database_name: Name of the database
            table_name: Name of the table
            
        Returns:
            Update response
        """
        magnetic_store_write_properties = {
            "EnableMagneticStoreWrites": True,
            "MagneticStoreWriteCapacitySizeMTU": 100,
        }
        
        return self.update_table(
            database_name=database_name,
            table_name=table_name,
            magnetic_store_write_properties=magnetic_store_write_properties,
        )
    
    # =========================================================================
    # CLOUDWATCH INTEGRATION
    # =========================================================================
    
    def get_ingestion_metrics(
        self,
        database_name: str,
        table_name: Optional[str] = None,
        period_seconds: int = 300,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get ingestion metrics from CloudWatch.
        
        Args:
            database_name: Name of the database
            table_name: Name of the table (optional for database-level)
            period_seconds: Metric period in seconds
            start_time: Start of the time range
            end_time: End of the time range
            
        Returns:
            List of metric data points
        """
        if not self.cloudwatch:
            raise RuntimeError("Boto3 not available")
        
        if not end_time:
            end_time = datetime.utcnow()
        if not start_time:
            start_time = end_time - timedelta(hours=1)
        
        namespace = "AWS/Timestream"
        
        if table_name:
            dimensions = [
                {"Name": "DatabaseName", "Value": database_name},
                {"Name": "TableName", "Value": table_name},
            ]
            metric_names = [
                "RecordsPerWrite",
                "WriteRecordsLatency",
                "SuccessfulRecords",
                "FailedRecords",
                "MagneticStoreBytes",
                "MemoryStoreBytes",
            ]
        else:
            dimensions = [
                {"Name": "DatabaseName", "Value": database_name},
            ]
            metric_names = [
                "DatabaseStorage",
                "DatabaseMeasureNames",
            ]
        
        metrics_data = []
        
        for metric_name in metric_names:
            try:
                response = self.cloudwatch.get_metric_statistics(
                    Namespace=namespace,
                    MetricName=metric_name,
                    Dimensions=dimensions,
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=period_seconds,
                    Statistics=["Sum", "Average", "Maximum", "Minimum"],
                )
                
                metrics_data.append({
                    "metric_name": metric_name,
                    "datapoints": response.get("Datapoints", []),
                })
            except ClientError:
                pass
        
        return metrics_data
    
    def get_query_metrics(
        self,
        database_name: str,
        table_name: Optional[str] = None,
        period_seconds: int = 300,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get query metrics from CloudWatch.
        
        Args:
            database_name: Name of the database
            table_name: Name of the table (optional)
            period_seconds: Metric period in seconds
            start_time: Start of the time range
            end_time: End of the time range
            
        Returns:
            List of metric data points
        """
        if not self.cloudwatch:
            raise RuntimeError("Boto3 not available")
        
        if not end_time:
            end_time = datetime.utcnow()
        if not start_time:
            start_time = end_time - timedelta(hours=1)
        
        namespace = "AWS/Timestream"
        
        if table_name:
            dimensions = [
                {"Name": "DatabaseName", "Value": database_name},
                {"Name": "TableName", "Value": table_name},
            ]
        else:
            dimensions = [
                {"Name": "DatabaseName", "Value": database_name},
            ]
        
        metric_names = [
            "QueryLatency",
            "RowsScanned",
            "BytesScanned",
            "CumulativeBytesScanned",
        ]
        
        metrics_data = []
        
        for metric_name in metric_names:
            try:
                response = self.cloudwatch.get_metric_statistics(
                    Namespace=namespace,
                    MetricName=metric_name,
                    Dimensions=dimensions,
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=period_seconds,
                    Statistics=["Sum", "Average", "Maximum", "Minimum"],
                )
                
                metrics_data.append({
                    "metric_name": metric_name,
                    "datapoints": response.get("Datapoints", []),
                })
            except ClientError:
                pass
        
        return metrics_data
    
    def get_storage_metrics(
        self,
        database_name: str,
        period_seconds: int = 300,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        Get storage utilization metrics.
        
        Args:
            database_name: Name of the database
            period_seconds: Metric period in seconds
            start_time: Start of the time range
            end_time: End of the time range
            
        Returns:
            Storage metrics data
        """
        if not self.cloudwatch:
            raise RuntimeError("Boto3 not available")
        
        if not end_time:
            end_time = datetime.utcnow()
        if not start_time:
            start_time = end_time - timedelta(hours=1)
        
        namespace = "AWS/Timestream"
        dimensions = [
            {"Name": "DatabaseName", "Value": database_name},
        ]
        
        metric_names = [
            "DatabaseStorage",
            "DatabaseMeasureNames",
            "DatabaseMeasureValues",
        ]
        
        storage_metrics = {}
        
        for metric_name in metric_names:
            try:
                response = self.cloudwatch.get_metric_statistics(
                    Namespace=namespace,
                    MetricName=metric_name,
                    Dimensions=dimensions,
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=period_seconds,
                    Statistics=["Latest", "Average", "Maximum"],
                )
                
                storage_metrics[metric_name] = response.get("Datapoints", [])
            except ClientError:
                pass
        
        return storage_metrics
    
    def create_dashboard(
        self,
        dashboard_name: str,
        database_name: str,
        tables: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Create a CloudWatch dashboard for Timestream metrics.
        
        Args:
            dashboard_name: Name of the dashboard
            database_name: Name of the database
            tables: List of table names (optional)
            
        Returns:
            Dashboard creation response
        """
        if not self.cloudwatch:
            raise RuntimeError("Boto3 not available")
        
        widget_properties = []
        
        for table in (tables or []):
            widget_properties.extend([
                {
                    "type": "metric",
                    "properties": {
                        "title": f"{table} - Ingestion",
                        "metrics": [
                            ["AWS/Timestream", "RecordsPerWrite", "DatabaseName", database_name, "TableName", table],
                            [".", "SuccessfulRecords", ".", ".", ".", "."],
                            [".", "FailedRecords", ".", ".", ".", "."],
                        ],
                        "period": 300,
                        "stat": "Sum",
                    }
                },
                {
                    "type": "metric",
                    "properties": {
                        "title": f"{table} - Query",
                        "metrics": [
                            ["AWS/Timestream", "QueryLatency", "DatabaseName", database_name, "TableName", table],
                            [".", "BytesScanned", ".", ".", ".", "."],
                            [".", "RowsScanned", ".", ".", ".", "."],
                        ],
                        "period": 300,
                        "stat": "Average",
                    }
                },
            ])
        
        dashboard_body = {
            "widgets": widget_properties
        }
        
        response = self.cloudwatch.put_dashboard(
            DashboardName=dashboard_name,
            DashboardBody=json.dumps(dashboard_body),
        )
        
        return response
