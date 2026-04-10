"""
AWS Cost and Usage Report Integration Module for Workflow System

Implements a CURIntegration class with:
1. Report creation: Create/manage cost reports
2. Report delivery: S3 bucket delivery
3. Report customization: Time granularity, units
4. Compression: Report compression options
5. Additional schemas: Linked account, tags, etc.
6. Report versioning: Multiple report versions
7. Athena integration: Query reports with Athena
8. Glue integration: Glue data catalog for reports
9. Retention: Report retention policies
10. CloudWatch integration: Report delivery metrics

Commit: 'feat(aws-cur): add AWS Cost and Usage Report with report creation, S3 delivery, customization, compression, Athena/Glue integration, retention, CloudWatch'
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


class TimeGranularity(Enum):
    """Time granularity options for CUR reports."""
    HOURLY = "HOURLY"
    DAILY = "DAILY"
    MONTHLY = "MONTHLY"


class CompressionFormat(Enum):
    """Compression format options for CUR reports."""
    ZIP = "ZIP"
    GZIP = "GZIP"


class SchemaType(Enum):
    """Schema type options for CUR reports."""
    RESOURCES = "RESOURCES"
    LINE_ITEM = "LINE_ITEM"


class ReportVersioning(Enum):
    """Report versioning options."""
    CREATE_NEW_REPORT = "CREATE_NEW_REPORT"
    OVERWRITE_EXISTING_REPORT = "OVERWRITE_EXISTING_REPORT"


class AthenaTableFormat(Enum):
    """Athena table output formats for CUR data."""
    PARQUET = "parquet"
    ORC = "orc"
    JSON = "json"
    CSV = "csv"
    TSV = "tsv"


@dataclass
class CURReport:
    """Cost and Usage Report information."""
    report_name: str
    s3_bucket: str
    s3_prefix: str
    s3_region: str
    time_unit: TimeGranularity
    format: str
    compression: CompressionFormat
    schema_type: SchemaType
    versioning: ReportVersioning
    additional_schema_elements: List[str] = field(default_factory=list)
    report_status: str = ""
    created_at: Optional[datetime] = None
    last_updated: Optional[datetime] = None


@dataclass
class ReportDefinition:
    """Detailed report definition configuration."""
    report_name: str
    time_unit: str
    format: str
    compression: str
    s3_bucket: str
    s3_prefix: str
    s3_region: str
    schema_elements: List[str]
    additional_schema_elements: List[str]
    versioning: str
    report_versioning: str
    billing_view_arn: Optional[str] = None


@dataclass
class AthenaIntegrationConfig:
    """Configuration for Athena CUR integration."""
    database_name: str = "cost_optimization"
    table_name: str = "cur_data"
    cur_bucket: str = ""
    cur_prefix: str = ""
    output_location: str = "s3://athena-results/"
    output_format: AthenaTableFormat = AthenaTableFormat.PARQUET
    partitioning: List[str] = field(default_factory=lambda: ["year", "month"])
    serde_properties: Dict[str, str] = field(default_factory=dict)


@dataclass
class GlueCatalogConfig:
    """Configuration for Glue data catalog integration."""
    database_name: str = "cost_optimization"
    table_name: str = "cur_data"
    cur_bucket: str = ""
    cur_prefix: str = ""
    region: str = "us-east-1"
    partition_update_enabled: bool = True
    catalog_encryption_mode: str = "DISABLED"


@dataclass
class RetentionPolicy:
    """Report retention policy configuration."""
    enabled: bool = True
    retention_days: int = 90
    archive_before_delete: bool = False
    archive_location: Optional[str] = None


@dataclass
class DeliveryMetric:
    """CloudWatch delivery metric information."""
    metric_name: str
    value: float
    unit: str
    timestamp: datetime
    dimensions: Dict[str, str] = field(default_factory=dict)


class CURIntegration:
    """
    AWS Cost and Usage Report Integration for workflow automation.
    
    Provides comprehensive CUR management including:
    - Report creation and management
    - S3 bucket delivery configuration
    - Report customization (time granularity, units)
    - Compression options
    - Additional schemas (linked accounts, tags, etc.)
    - Report versioning
    - Athena integration for querying reports
    - Glue integration for data catalog
    - Retention policies
    - CloudWatch metrics integration
    """
    
    def __init__(
        self,
        region_name: str = "us-east-1",
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        profile_name: Optional[str] = None,
        boto_cur_client: Any = None,
        boto_s3_client: Any = None,
        boto_athena_client: Any = None,
        boto_glue_client: Any = None,
        boto_cloudwatch_client: Any = None,
    ):
        """
        Initialize CUR integration.
        
        Args:
            region_name: AWS region for CUR operations
            aws_access_key_id: AWS access key ID
            aws_secret_access_key: AWS secret access key
            aws_session_token: AWS session token
            profile_name: AWS profile name
            boto_cur_client: Pre-configured CUR boto3 client (for testing)
            boto_s3_client: Pre-configured S3 boto3 client (for testing)
            boto_athena_client: Pre-configured Athena boto3 client (for testing)
            boto_glue_client: Pre-configured Glue boto3 client (for testing)
            boto_cloudwatch_client: Pre-configured CloudWatch boto3 client (for testing)
        """
        self.region_name = region_name
        
        if boto_cur_client:
            self.cur = boto_cur_client
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
            self.cur = session.client("cur", region_name=region_name)
            self.cur_service = session.client("costandusageReport", region_name=region_name)
        else:
            self.cur = None
            self.cur_service = None
        
        if boto_s3_client:
            self.s3 = boto_s3_client
        elif BOTO3_AVAILABLE:
            self.s3 = boto3.Session(**session_kwargs).client("s3", region_name=region_name) if 'session_kwargs' in dir() else None
        else:
            self.s3 = None
        
        if boto_athena_client:
            self.athena = boto_athena_client
        elif BOTO3_AVAILABLE:
            self.athena = boto3.Session(**session_kwargs).client("athena", region_name=region_name) if 'session_kwargs' in dir() else None
        else:
            self.athena = None
        
        if boto_glue_client:
            self.glue = boto_glue_client
        elif BOTO3_AVAILABLE:
            self.glue = boto3.Session(**session_kwargs).client("glue", region_name=region_name) if 'session_kwargs' in dir() else None
        else:
            self.glue = None
        
        if boto_cloudwatch_client:
            self.cloudwatch = boto_cloudwatch_client
        elif BOTO3_AVAILABLE:
            self.cloudwatch = boto3.Session(**session_kwargs).client("cloudwatch", region_name=region_name) if 'session_kwargs' in dir() else None
        else:
            self.cloudwatch = None
        
        self._reports: Dict[str, CURReport] = {}
        self._retention_policies: Dict[str, RetentionPolicy] = {}
        self._lock = threading.Lock()
    
    # =========================================================================
    # REPORT CREATION AND MANAGEMENT
    # =========================================================================
    
    def create_report(
        self,
        report_name: str,
        s3_bucket: str,
        s3_prefix: str,
        s3_region: str,
        time_unit: TimeGranularity = TimeGranularity.HOURLY,
        format: str = "textORcsv",
        compression: CompressionFormat = CompressionFormat.GZIP,
        schema_type: SchemaType = SchemaType.LINE_ITEM,
        versioning: ReportVersioning = ReportVersioning.CREATE_NEW_REPORT,
        additional_schema_elements: Optional[List[str]] = None,
        report_versioning: str = "CREATE_NEW_REPORT",
    ) -> Dict[str, Any]:
        """
        Create a new Cost and Usage Report.
        
        Args:
            report_name: Unique name for the report
            s3_bucket: S3 bucket name for report delivery
            s3_prefix: S3 prefix (path) for report delivery
            s3_region: S3 bucket region
            time_unit: Time granularity (HOURLY, DAILY, MONTHLY)
            format: Report format (textORcsv, csv, Parquet)
            compression: Compression format (ZIP, GZIP)
            schema_type: Schema type (RESOURCES, LINE_ITEM)
            versioning: Versioning option
            additional_schema_elements: Additional schema elements to include
            report_versioning: Report versioning setting
            
        Returns:
            Report creation response
        """
        if not self.cur_service:
            raise RuntimeError("Boto3 not available")
        
        if additional_schema_elements is None:
            additional_schema_elements = []
        
        schema_elements = [schema_type.value]
        schema_elements.extend(additional_schema_elements)
        
        kwargs = {
            "ReportName": report_name,
            "TimeUnit": time_unit.value,
            "Format": format,
            "Compression": compression.value,
            "S3Bucket": s3_bucket,
            "S3Prefix": s3_prefix,
            "S3Region": s3_region,
            "SchemaElements": schema_elements,
            "ReportVersioning": report_versioning,
        }
        
        response = self.cur_service.put_report_definition(
            ReportDefinition={
                "ReportName": report_name,
                "TimeUnit": time_unit.value,
                "Format": format,
                "Compression": compression.value,
                "S3Bucket": s3_bucket,
                "S3Prefix": s3_prefix,
                "S3Region": s3_region,
                "SchemaElements": schema_elements,
                "AdditionalSchemaElements": additional_schema_elements,
                "ReportVersioning": report_versioning,
            }
        )
        
        cur_report = CURReport(
            report_name=report_name,
            s3_bucket=s3_bucket,
            s3_prefix=s3_prefix,
            s3_region=s3_region,
            time_unit=time_unit,
            format=format,
            compression=compression,
            schema_type=schema_type,
            versioning=versioning,
            additional_schema_elements=additional_schema_elements,
            report_status="ACTIVE",
            created_at=datetime.utcnow(),
            last_updated=datetime.utcnow(),
        )
        
        with self._lock:
            self._reports[report_name] = cur_report
        
        self._emit_delivery_metric(
            metric_name="ReportCreated",
            value=1,
            unit="Count",
            dimensions={"ReportName": report_name},
        )
        
        return response
    
    def get_report(self, report_name: str) -> CURReport:
        """
        Get Cost and Usage Report information.
        
        Args:
            report_name: Report name
            
        Returns:
            CURReport object
        """
        if not self.cur_service:
            raise RuntimeError("Boto3 not available")
        
        response = self.cur_service.get_report_definition(ReportName=report_name)
        rd = response["ReportDefinition"]
        
        time_unit_map = {
            "HOURLY": TimeGranularity.HOURLY,
            "DAILY": TimeGranularity.DAILY,
            "MONTHLY": TimeGranularity.MONTHLY,
        }
        
        compression_map = {
            "ZIP": CompressionFormat.ZIP,
            "GZIP": CompressionFormat.GZIP,
        }
        
        schema_type = SchemaType.LINE_ITEM
        if rd.get("SchemaElements"):
            first_element = rd["SchemaElements"][0] if rd["SchemaElements"] else "LINE_ITEM"
            try:
                schema_type = SchemaType(first_element)
            except ValueError:
                schema_type = SchemaType.LINE_ITEM
        
        return CURReport(
            report_name=rd["ReportName"],
            s3_bucket=rd["S3Bucket"],
            s3_prefix=rd["S3Prefix"],
            s3_region=rd["S3Region"],
            time_unit=time_unit_map.get(rd.get("TimeUnit", "HOURLY"), TimeGranularity.HOURLY),
            format=rd.get("Format", "textORcsv"),
            compression=compression_map.get(rd.get("Compression", "GZIP"), CompressionFormat.GZIP),
            schema_type=schema_type,
            versioning=ReportVersioning.CREATE_NEW_REPORT,
            additional_schema_elements=rd.get("AdditionalSchemaElements", []),
            report_status="ACTIVE",
            created_at=datetime.utcnow(),
            last_updated=datetime.utcnow(),
        )
    
    def list_reports(self) -> List[CURReport]:
        """
        List all Cost and Usage Reports.
        
        Returns:
            List of CURReport objects
        """
        if not self.cur_service:
            raise RuntimeError("Boto3 not available")
        
        response = self.cur_service.list_report_definitions()
        reports = []
        
        time_unit_map = {
            "HOURLY": TimeGranularity.HOURLY,
            "DAILY": TimeGranularity.DAILY,
            "MONTHLY": TimeGranularity.MONTHLY,
        }
        
        compression_map = {
            "ZIP": CompressionFormat.ZIP,
            "GZIP": CompressionFormat.GZIP,
        }
        
        for rd in response.get("ReportDefinitions", []):
            schema_type = SchemaType.LINE_ITEM
            if rd.get("SchemaElements"):
                first_element = rd["SchemaElements"][0] if rd["SchemaElements"] else "LINE_ITEM"
                try:
                    schema_type = SchemaType(first_element)
                except ValueError:
                    schema_type = SchemaType.LINE_ITEM
            
            reports.append(CURReport(
                report_name=rd["ReportName"],
                s3_bucket=rd["S3Bucket"],
                s3_prefix=rd["S3Prefix"],
                s3_region=rd["S3Region"],
                time_unit=time_unit_map.get(rd.get("TimeUnit", "HOURLY"), TimeGranularity.HOURLY),
                format=rd.get("Format", "textORcsv"),
                compression=compression_map.get(rd.get("Compression", "GZIP"), CompressionFormat.GZIP),
                schema_type=schema_type,
                versioning=ReportVersioning.CREATE_NEW_REPORT,
                additional_schema_elements=rd.get("AdditionalSchemaElements", []),
                report_status="ACTIVE",
                created_at=datetime.utcnow(),
                last_updated=datetime.utcnow(),
            ))
        
        return reports
    
    def delete_report(self, report_name: str) -> Dict[str, Any]:
        """
        Delete a Cost and Usage Report.
        
        Args:
            report_name: Report name to delete
            
        Returns:
            Deletion response
        """
        if not self.cur_service:
            raise RuntimeError("Boto3 not available")
        
        response = self.cur_service.delete_report_definition(ReportName=report_name)
        
        with self._lock:
            if report_name in self._reports:
                del self._reports[report_name]
        
        self._emit_delivery_metric(
            metric_name="ReportDeleted",
            value=1,
            unit="Count",
            dimensions={"ReportName": report_name},
        )
        
        return response
    
    def update_report(
        self,
        report_name: str,
        time_unit: Optional[TimeGranularity] = None,
        format: Optional[str] = None,
        compression: Optional[CompressionFormat] = None,
        additional_schema_elements: Optional[List[str]] = None,
        versioning: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Update an existing Cost and Usage Report.
        
        Args:
            report_name: Report name to update
            time_unit: New time granularity
            format: New format
            compression: New compression
            additional_schema_elements: New additional schema elements
            versioning: New versioning setting
            
        Returns:
            Update response
        """
        if not self.cur_service:
            raise RuntimeError("Boto3 not available")
        
        current = self.get_report(report_name)
        
        kwargs = {
            "ReportName": report_name,
            "TimeUnit": time_unit.value if time_unit else current.time_unit.value,
            "Format": format if format else current.format,
            "Compression": compression.value if compression else current.compression.value,
            "S3Bucket": current.s3_bucket,
            "S3Prefix": current.s3_prefix,
            "S3Region": current.s3_region,
            "SchemaElements": [current.schema_type.value] + (additional_schema_elements if additional_schema_elements else current.additional_schema_elements),
            "AdditionalSchemaElements": additional_schema_elements if additional_schema_elements else current.additional_schema_elements,
            "ReportVersioning": versioning if versioning else "CREATE_NEW_REPORT",
        }
        
        response = self.cur_service.put_report_definition(ReportDefinition=kwargs)
        
        with self._lock:
            if report_name in self._reports:
                self._reports[report_name].last_updated = datetime.utcnow()
        
        self._emit_delivery_metric(
            metric_name="ReportUpdated",
            value=1,
            unit="Count",
            dimensions={"ReportName": report_name},
        )
        
        return response
    
    # =========================================================================
    # S3 BUCKET DELIVERY
    # =========================================================================
    
    def configure_s3_delivery(
        self,
        report_name: str,
        s3_bucket: str,
        s3_prefix: str,
        s3_region: str,
        create_bucket: bool = False,
        enable_bucket_versioning: bool = True,
    ) -> Dict[str, Any]:
        """
        Configure S3 bucket delivery for a CUR report.
        
        Args:
            report_name: Report name
            s3_bucket: S3 bucket name
            s3_prefix: S3 prefix for report delivery
            s3_region: S3 bucket region
            create_bucket: Whether to create the bucket if it doesn't exist
            enable_bucket_versioning: Whether to enable bucket versioning
            
        Returns:
            S3 delivery configuration result
        """
        if not self.s3:
            raise RuntimeError("Boto3 not available")
        
        result = {"bucket": s3_bucket, "prefix": s3_prefix, "configured": True}
        
        try:
            self.s3.head_bucket(Bucket=s3_bucket)
            result["bucket_exists"] = True
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "404":
                result["bucket_exists"] = False
                if create_bucket:
                    create_kwargs = {
                        "Bucket": s3_bucket,
                        "CreateBucketConfiguration": {
                            "LocationConstraint": s3_region
                        } if s3_region != "us-east-1" else {},
                    }
                    if s3_region == "us-east-1":
                        del create_kwargs["CreateBucketConfiguration"]
                    self.s3.create_bucket(**create_kwargs)
                    result["bucket_created"] = True
                    result["bucket_exists"] = True
            else:
                raise
        
        if enable_bucket_versioning and result.get("bucket_exists"):
            self.s3.put_bucket_versioning(
                Bucket=s3_bucket,
                VersioningConfiguration={"Status": "Enabled"}
            )
            result["versioning_enabled"] = True
        
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "AllowCURAccess",
                    "Effect": "Allow",
                    "Principal": {"Service": "costreports.amazonaws.com"},
                    "Action": ["s3:GetBucketAcl", "s3:GetBucketPolicy"],
                    "Resource": f"arn:aws:s3:::{s3_bucket}"
                },
                {
                    "Sid": "AllowCURWrite",
                    "Effect": "Allow",
                    "Principal": {"Service": "costreports.amazonaws.com"},
                    "Action": ["s3:PutObject"],
                    "Resource": f"arn:aws:s3:::{s3_bucket}/*"
                }
            ]
        }
        
        try:
            self.s3.put_bucket_policy(
                Bucket=s3_bucket,
                Policy=json.dumps(policy)
            )
            result["policy_attached"] = True
        except ClientError:
            pass
        
        return result
    
    def list_s3_report_files(
        self,
        s3_bucket: str,
        s3_prefix: str,
        report_name: Optional[str] = None,
        max_keys: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        List CUR report files in S3.
        
        Args:
            s3_bucket: S3 bucket name
            s3_prefix: S3 prefix for reports
            report_name: Optional report name filter
            max_keys: Maximum number of files to return
            
        Returns:
            List of report file information
        """
        if not self.s3:
            raise RuntimeError("Boto3 not available")
        
        prefix = s3_prefix
        if report_name:
            prefix = f"{s3_prefix}/{report_name}"
        
        response = self.s3.list_objects_v2(
            Bucket=s3_bucket,
            Prefix=prefix,
            MaxKeys=max_keys,
        )
        
        files = []
        for obj in response.get("Contents", []):
            files.append({
                "key": obj["Key"],
                "size": obj["Size"],
                "last_modified": obj["LastModified"],
                "etag": obj["ETag"],
            })
        
        return files
    
    # =========================================================================
    # REPORT CUSTOMIZATION (TIME GRANULARITY, UNITS)
    # =========================================================================
    
    def customize_report_granularity(
        self,
        report_name: str,
        time_unit: TimeGranularity,
    ) -> Dict[str, Any]:
        """
        Customize report time granularity.
        
        Args:
            report_name: Report name
            time_unit: Time granularity (HOURLY, DAILY, MONTHLY)
            
        Returns:
            Update response
        """
        return self.update_report(report_name, time_unit=time_unit)
    
    def customize_report_units(
        self,
        report_name: str,
        include_resources: bool = False,
        include_tags: bool = True,
        include_linked_accounts: bool = True,
        include_payer_id: bool = True,
        include_subscription_id: bool = False,
        include_credits: bool = True,
        include_discounts: bool = True,
        include_fees: bool = True,
        include_taxes: bool = False,
    ) -> Dict[str, Any]:
        """
        Customize report units and line item details.
        
        Args:
            report_name: Report name
            include_resources: Include resource IDs
            include_tags: Include tags
            include_linked_accounts: Include linked account IDs
            include_payer_id: Include payer ID
            include_subscription_id: Include subscription ID
            include_credits: Include credits
            include_discounts: Include discounts
            include_fees: Include fees
            include_taxes: Include taxes
            
        Returns:
            Update response
        """
        additional_elements = []
        
        if include_resources:
            additional_elements.append("RESOURCES")
        if include_tags:
            additional_elements.append("TAGS")
        if include_linked_accounts:
            additional_elements.append("LINE_ITEM_LINKED_ACCOUNT")
        if include_payer_id:
            additional_elements.append("PAYER_LINKED_ACCOUNT")
        if include_subscription_id:
            additional_elements.append("SUBSCRIPTION_ID")
        if include_credits:
            additional_elements.append("CREDITS")
        if include_discounts:
            additional_elements.append("DISCOUNTS")
        if include_fees:
            additional_elements.append("FEES")
        if include_taxes:
            additional_elements.append("TAX")
        
        return self.update_report(
            report_name,
            additional_schema_elements=additional_elements,
        )
    
    # =========================================================================
    # COMPRESSION OPTIONS
    # =========================================================================
    
    def set_compression(
        self,
        report_name: str,
        compression: CompressionFormat,
    ) -> Dict[str, Any]:
        """
        Set report compression format.
        
        Args:
            report_name: Report name
            compression: Compression format (ZIP, GZIP)
            
        Returns:
            Update response
        """
        return self.update_report(report_name, compression=compression)
    
    # =========================================================================
    # ADDITIONAL SCHEMAS (LINKED ACCOUNTS, TAGS, ETC.)
    # =========================================================================
    
    def add_linked_account_schema(
        self,
        report_name: str,
    ) -> Dict[str, Any]:
        """
        Add linked account schema to report.
        
        Args:
            report_name: Report name
            
        Returns:
            Update response
        """
        current = self.get_report(report_name)
        new_elements = current.additional_schema_elements + ["LINE_ITEM_LINKED_ACCOUNT"]
        return self.update_report(report_name, additional_schema_elements=new_elements)
    
    def add_tags_schema(
        self,
        report_name: str,
        tag_keys: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Add tags schema to report.
        
        Args:
            report_name: Report name
            tag_keys: Specific tag keys to include (None for all tags)
            
        Returns:
            Update response
        """
        current = self.get_report(report_name)
        new_elements = current.additional_schema_elements + ["TAGS"]
        return self.update_report(report_name, additional_schema_elements=new_elements)
    
    def add_resources_schema(
        self,
        report_name: str,
    ) -> Dict[str, Any]:
        """
        Add resources schema to report.
        
        Args:
            report_name: Report name
            
        Returns:
            Update response
        """
        current = self.get_report(report_name)
        new_elements = current.additional_schema_elements + ["RESOURCES"]
        return self.update_report(report_name, additional_schema_elements=new_elements)
    
    # =========================================================================
    # REPORT VERSIONING
    # =========================================================================
    
    def enable_report_versioning(
        self,
        report_name: str,
        versioning_type: ReportVersioning = ReportVersioning.CREATE_NEW_REPORT,
    ) -> Dict[str, Any]:
        """
        Enable report versioning.
        
        Args:
            report_name: Report name
            versioning_type: Versioning type (CREATE_NEW_REPORT, OVERWRITE_EXISTING_REPORT)
            
        Returns:
            Update response
        """
        return self.update_report(
            report_name,
            versioning=versioning_type.value,
        )
    
    def list_report_versions(
        self,
        s3_bucket: str,
        s3_prefix: str,
    ) -> List[Dict[str, Any]]:
        """
        List available report versions in S3.
        
        Args:
            s3_bucket: S3 bucket name
            s3_prefix: S3 prefix for reports
            
        Returns:
            List of report versions
        """
        if not self.s3:
            raise RuntimeError("Boto3 not available")
        
        response = self.s3.list_object_versions(
            Bucket=s3_bucket,
            Prefix=s3_prefix,
        )
        
        versions = []
        for version in response.get("Versions", []):
            versions.append({
                "version_id": version["VersionId"],
                "key": version["Key"],
                "last_modified": version["LastModified"],
                "size": version["Size"],
                "is_latest": version["IsLatest"],
            })
        
        return versions
    
    # =========================================================================
    # ATHENA INTEGRATION
    # =========================================================================
    
    def setup_athena_integration(
        self,
        config: AthenaIntegrationConfig,
        create_database: bool = True,
    ) -> Dict[str, Any]:
        """
        Set up Athena integration for CUR data.
        
        Args:
            config: Athena integration configuration
            create_database: Whether to create the database if it doesn't exist
            
        Returns:
            Setup result
        """
        if not self.athena:
            raise RuntimeError("Athena client not available")
        
        result = {"database": config.database_name, "table": config.table_name}
        
        if create_database:
            try:
                self.athena.start_query_execution(
                    QueryString=f"CREATE DATABASE IF NOT EXISTS {config.database_name}",
                    ResultConfiguration={"OutputLocation": config.output_location},
                )
                result["database_created"] = True
            except ClientError:
                pass
        
        columns = self._get_cur_schema_columns()
        
        partitioning = ", ".join([f"PARTITION ({' STRING,'.join(config.partitioning)} STRING)"])
        if config.output_format == AthenaTableFormat.PARQUET:
            storage_format = "stored as parquet"
            serde_props = "'parquet.compression' = 'SNAPPY'"
        elif config.output_format == AthenaTableFormat.ORC:
            storage_format = "stored as orc"
            serde_props = "'orc.compress' = 'ZLIB'"
        elif config.output_format == AthenaTableFormat.JSON:
            storage_format = "stored as json"
            serde_props = ""
        else:
            storage_format = "stored as textfile"
            serde_props = f"stored as INPUTFORMAT 'com.amazon.cur.CSVOutputFormat' OUTPUTFORMAT 'com.amazon.cur.TextOutputFormat'"

        create_table_sql = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {config.database_name}.{config.table_name} (
            {columns}
        )
        {storage_format}
        LOCATION '{config.cur_bucket}/{config.cur_prefix}'
        TBLPROPERTIES ({serde_props})
        """
        
        try:
            self.athena.start_query_execution(
                QueryString=create_table_sql,
                ResultConfiguration={"OutputLocation": config.output_location},
            )
            result["table_created"] = True
        except ClientError as e:
            result["table_created"] = False
            result["error"] = str(e)
        
        return result
    
    def _get_cur_schema_columns(self) -> str:
        """
        Get standard CUR schema columns for Athena table creation.
        
        Returns:
            Column definitions string
        """
        columns = [
            "line_item_id STRING",
            "line_item_type STRING",
            "bill_billing_entity STRING",
            "bill_bill_type STRING",
            "bill_payer_account_id STRING",
            "bill_billing_period_start_date STRING",
            "bill_billing_period_end_date STRING",
            "line_item_usage_account_id STRING",
            "line_item_usage_start_date STRING",
            "line_item_usage_end_date STRING",
            "line_item_product_code STRING",
            "line_item_purchase_option STRING",
            "line_item_territory STRING",
            "line_item_availability_zone STRING",
            "line_item_resource_id STRING",
            "line_item_usage_type STRING",
            "line_item_usage_quantity STRING",
            "line_item_currency_code STRING",
            "line_item_unblended_rate STRING",
            "line_item_unblended_cost STRING",
            "line_item_blended_rate STRING",
            "line_item_blended_cost STRING",
            "line_item_net_unblended_cost STRING",
            "line_item_net_blended_cost STRING",
            "product_product_name STRING",
            "product_account_id STRING",
            "product_availability STRING",
            "product_capacity_status STRING",
            "product_clock_speed STRING",
            "product_cpu_architecture STRING",
            "product_cpu_core_count STRING",
            "product_cpu_manufacturer STRING",
            "product_current_generation STRING",
            "product_database_engine STRING",
            "product_dedicated_ebs_throughput STRING",
            "product_description STRING",
            "product_duration STRING",
            "product_ebs_volume_throughput STRING",
            "product_ebs_volume_type STRING",
            "product_enhanced_networking_supported STRING",
            "product_event_type STRING",
            "product_instance_type STRING",
            "product_instance_family STRING",
            "product_license_model STRING",
            "product_location STRING",
            "product_location_type STRING",
            "product_marketoption STRING",
            "product_memory STRING",
            "product_network_performance STRING",
            "product_operating_system STRING",
            "product_operation STRING",
            "product_product_suite STRING",
            "product_processor_features STRING",
            "product_processor_speed STRING",
            "product_product_type STRING",
            "product_tenancy STRING",
            "product_usage_type STRING",
            "product_vcpu STRING",
            "product_version STRING",
            "product_volume_type STRING",
            "pricing_rate_id STRING",
            "pricing_currency STRING",
            "pricing_unit STRING",
            "pricing_term STRING",
            "pricing_public_ondemand_cost STRING",
            "pricing_public_ondemand_rate STRING",
            "pricing_retail_price STRING",
            "res_tag_keys STRING",
            "year STRING",
            "month STRING",
        ]
        return ",\n            ".join(columns)
    
    def query_cur_data(
        self,
        query: str,
        database: str = "cost_optimization",
        table: str = "cur_data",
        output_location: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Query CUR data using Athena.
        
        Args:
            query: SQL query string
            database: Athena database name
            table: Athena table name
            output_location: S3 location for query results
            
        Returns:
            Query execution result
        """
        if not self.athena:
            raise RuntimeError("Athena client not available")
        
        if "{" in query or "}" in query:
            query = query.format(database=database, table=table)
        
        query_execution = self.athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={
                "OutputLocation": output_location or "s3://athena-results/"
            },
        )
        
        query_execution_id = query_execution["QueryExecutionId"]
        
        for _ in range(60):
            response = self.athena.get_query_execution(QueryExecutionId=query_execution_id)
            state = response["QueryExecution"]["Status"]["State"]
            
            if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                break
            
            time.sleep(1)
        
        result = {
            "query_execution_id": query_execution_id,
            "state": state,
        }
        
        if state == "SUCCEEDED":
            result_response = self.athena.get_query_results(QueryExecutionId=query_execution_id)
            result["rows"] = result_response.get("ResultSet", {}).get("Rows", [])
            result["columns"] = result_response.get("ResultSet", {}).get("ResultSetMetadata", {}).get("ColumnInfo", [])
        
        return result
    
    def generate_cost_query(
        self,
        database: str = "cost_optimization",
        table: str = "cur_data",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        account_ids: Optional[List[str]] = None,
        service_names: Optional[List[str]] = None,
        group_by: Optional[List[str]] = None,
    ) -> str:
        """
        Generate a cost analysis query.
        
        Args:
            database: Athena database name
            table: Athena table name
            start_date: Start date filter (YYYY-MM-DD)
            end_date: End date filter (YYYY-MM-DD)
            account_ids: Filter by account IDs
            service_names: Filter by service names
            group_by: Fields to group by
            
        Returns:
            SQL query string
        """
        where_clauses = []
        
        if start_date:
            where_clauses.append(f"line_item_usage_start_date >= '{start_date}'")
        if end_date:
            where_clauses.append(f"line_item_usage_end_date <= '{end_date}'")
        if account_ids:
            accounts = ", ".join([f"'{a}'" for a in account_ids])
            where_clauses.append(f"line_item_usage_account_id IN ({accounts})")
        if service_names:
            services = ", ".join([f"'{s}'" for s in service_names])
            where_clauses.append(f"product_product_name IN ({services})")
        
        where_clause = ""
        if where_clauses:
            where_clause = "WHERE " + " AND ".join(where_clauses)
        
        group_by_clause = ""
        if group_by:
            group_by_clause = "GROUP BY " + ", ".join(group_by)
            select_fields = ", ".join(group_by) + ", "
            order_by = "ORDER BY " + ", ".join(group_by)
        else:
            select_fields = ""
            order_by = ""
        
        query = f"""
        SELECT 
            {select_fields}
            SUM(CAST(line_item_unblended_cost AS DOUBLE)) as total_cost
        FROM {database}.{table}
        {where_clause}
        {group_by_clause}
        {order_by}
        """
        
        return " ".join(query.split())
    
    # =========================================================================
    # GLUE INTEGRATION
    # =========================================================================
    
    def setup_glue_integration(
        self,
        config: GlueCatalogConfig,
        create_database: bool = True,
    ) -> Dict[str, Any]:
        """
        Set up Glue data catalog for CUR data.
        
        Args:
            config: Glue catalog configuration
            create_database: Whether to create the database if it doesn't exist
            
        Returns:
            Setup result
        """
        if not self.glue:
            raise RuntimeError("Glue client not available")
        
        result = {"database": config.database_name, "table": config.table_name}
        
        if create_database:
            try:
                self.glue.create_database(
                    DatabaseInput={
                        "Name": config.database_name,
                        "Description": "Cost and Usage Report data catalog",
                    }
                )
                result["database_created"] = True
            except ClientError as e:
                if e.response["Error"]["Code"] != "AlreadyExistsException":
                    raise
                result["database_created"] = False
        
        table_input = {
            "Name": config.table_name,
            "Description": "Cost and Usage Report table",
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {
                "parquet.compression": "SNAPPY",
                "classification": "parquet",
                "skip.header.line.count": "1",
            },
            "StorageDescriptor": {
                "Columns": self._get_glue_columns(),
                "Location": f"s3://{config.cur_bucket}/{config.cur_prefix}",
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    "Parameters": {},
                },
                "Compressed": True,
            },
        }
        
        try:
            self.glue.create_table(
                DatabaseName=config.database_name,
                TableInput=table_input,
            )
            result["table_created"] = True
        except ClientError as e:
            if e.response["Error"]["Code"] != "AlreadyExistsException":
                try:
                    self.glue.update_table(
                        DatabaseName=config.database_name,
                        TableInput=table_input,
                    )
                    result["table_updated"] = True
                except ClientError:
                    result["table_updated"] = False
                    result["error"] = str(e)
            else:
                result["table_created"] = False
        
        return result
    
    def _get_glue_columns(self) -> List[Dict[str, str]]:
        """
        Get standard CUR schema columns for Glue table creation.
        
        Returns:
            Column definitions list
        """
        columns = [
            {"Name": "line_item_id", "Type": "string"},
            {"Name": "line_item_type", "Type": "string"},
            {"Name": "bill_billing_entity", "Type": "string"},
            {"Name": "bill_bill_type", "Type": "string"},
            {"Name": "bill_payer_account_id", "Type": "string"},
            {"Name": "bill_billing_period_start_date", "Type": "string"},
            {"Name": "bill_billing_period_end_date", "Type": "string"},
            {"Name": "line_item_usage_account_id", "Type": "string"},
            {"Name": "line_item_usage_start_date", "Type": "string"},
            {"Name": "line_item_usage_end_date", "Type": "string"},
            {"Name": "line_item_product_code", "Type": "string"},
            {"Name": "line_item_purchase_option", "Type": "string"},
            {"Name": "line_item_territory", "Type": "string"},
            {"Name": "line_item_availability_zone", "Type": "string"},
            {"Name": "line_item_resource_id", "Type": "string"},
            {"Name": "line_item_usage_type", "Type": "string"},
            {"Name": "line_item_usage_quantity", "Type": "decimal(18,9)"},
            {"Name": "line_item_currency_code", "Type": "string"},
            {"Name": "line_item_unblended_rate", "Type": "string"},
            {"Name": "line_item_unblended_cost", "Type": "decimal(18,9)"},
            {"Name": "line_item_blended_rate", "Type": "string"},
            {"Name": "line_item_blended_cost", "Type": "decimal(18,9)"},
            {"Name": "product_product_name", "Type": "string"},
            {"Name": "product_instance_type", "Type": "string"},
            {"Name": "product_operating_system", "Type": "string"},
            {"Name": "product_region", "Type": "string"},
            {"Name": "product_sku", "Type": "string"},
            {"Name": "product_vcpu", "Type": "string"},
            {"Name": "product_memory", "Type": "string"},
            {"Name": "pricing_rate_id", "Type": "string"},
            {"Name": "pricing_currency", "Type": "string"},
            {"Name": "pricing_unit", "Type": "string"},
            {"Name": "pricing_term", "Type": "string"},
            {"Name": "pricing_public_ondemand_cost", "Type": "string"},
            {"Name": "res_tag_keys", "Type": "string"},
            {"Name": "year", "Type": "string"},
            {"Name": "month", "Type": "string"},
        ]
        return columns
    
    def update_glue_partitions(
        self,
        database: str,
        table: str,
        s3_bucket: str,
        s3_prefix: str,
    ) -> Dict[str, Any]:
        """
        Update Glue table partitions.
        
        Args:
            database: Glue database name
            table: Glue table name
            s3_bucket: S3 bucket name
            s3_prefix: S3 prefix for CUR data
            
        Returns:
            Partition update result
        """
        if not self.glue:
            raise RuntimeError("Glue client not available")
        
        response = self.s3.list_objects_v2(
            Bucket=s3_bucket,
            Prefix=s3_prefix,
        )
        
        partitions = []
        for obj in response.get("Contents", []):
            key = obj["Key"]
            parts = key.split("/")
            partition_values = {}
            
            for i, part in enumerate(parts):
                if part in ["year", "month"] and i + 1 < len(parts):
                    partition_values[part] = parts[i + 1]
            
            if partition_values:
                location = f"s3://{s3_bucket}/{s3_prefix}"
                partitions.append({
                    "Values": list(partition_values.values()),
                    "StorageDescriptor": {
                        "Location": location,
                    },
                })
        
        if partitions:
            try:
                self.glue.batch_create_partition(
                    DatabaseName=database,
                    TableName=table,
                    PartitionInputList=partitions,
                )
                return {"partitions_created": len(partitions)}
            except ClientError as e:
                return {"error": str(e)}
        
        return {"partitions_created": 0}
    
    # =========================================================================
    # RETENTION POLICIES
    # =========================================================================
    
    def set_retention_policy(
        self,
        report_name: str,
        retention_days: int = 90,
        archive_before_delete: bool = False,
        archive_location: Optional[str] = None,
    ) -> RetentionPolicy:
        """
        Set retention policy for a CUR report.
        
        Args:
            report_name: Report name
            retention_days: Number of days to retain data
            archive_before_delete: Whether to archive data before deletion
            archive_location: S3 location for archived data
            
        Returns:
            RetentionPolicy object
        """
        policy = RetentionPolicy(
            enabled=True,
            retention_days=retention_days,
            archive_before_delete=archive_before_delete,
            archive_location=archive_location,
        )
        
        with self._lock:
            self._retention_policies[report_name] = policy
        
        self._emit_delivery_metric(
            metric_name="RetentionPolicySet",
            value=retention_days,
            unit="Days",
            dimensions={"ReportName": report_name},
        )
        
        return policy
    
    def get_retention_policy(self, report_name: str) -> Optional[RetentionPolicy]:
        """
        Get retention policy for a report.
        
        Args:
            report_name: Report name
            
        Returns:
            RetentionPolicy object or None
        """
        with self._lock:
            return self._retention_policies.get(report_name)
    
    def apply_retention_policy(
        self,
        report_name: str,
        s3_bucket: str,
        s3_prefix: str,
    ) -> Dict[str, Any]:
        """
        Apply retention policy to CUR data in S3.
        
        Args:
            report_name: Report name
            s3_bucket: S3 bucket name
            s3_prefix: S3 prefix for CUR data
            
        Returns:
            Retention application result
        """
        policy = self.get_retention_policy(report_name)
        if not policy:
            return {"error": "No retention policy found"}
        
        cutoff_date = datetime.utcnow() - timedelta(days=policy.retention_days)
        
        response = self.s3.list_objects_v2(
            Bucket=s3_bucket,
            Prefix=s3_prefix,
        )
        
        objects_to_delete = []
        archived_count = 0
        
        for obj in response.get("Contents", []):
            if obj["LastModified"] < cutoff_date:
                if policy.archive_before_delete and policy.archive_location:
                    copy_source = {
                        "Bucket": s3_bucket,
                        "Key": obj["Key"],
                    }
                    archive_key = f"{policy.archive_location}/{obj['Key']}"
                    self.s3.copy(copy_source, s3_bucket, archive_key)
                    archived_count += 1
                
                objects_to_delete.append({"Key": obj["Key"]})
        
        deleted_count = 0
        if objects_to_delete:
            for i in range(0, len(objects_to_delete), 1000):
                batch = objects_to_delete[i:i + 1000]
                self.s3.delete_objects(
                    Bucket=s3_bucket,
                    Delete={"Objects": batch},
                )
                deleted_count += len(batch)
        
        self._emit_delivery_metric(
            metric_name="RetentionPolicyApplied",
            value=deleted_count,
            unit="Objects",
            dimensions={"ReportName": report_name},
        )
        
        return {
            "deleted_count": deleted_count,
            "archived_count": archived_count,
            "cutoff_date": cutoff_date.isoformat(),
        }
    
    # =========================================================================
    # CLOUDWATCH INTEGRATION
    # =========================================================================
    
    def _emit_delivery_metric(
        self,
        metric_name: str,
        value: float,
        unit: str,
        dimensions: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Emit delivery metric to CloudWatch.
        
        Args:
            metric_name: Name of the metric
            value: Metric value
            unit: Unit of measurement
            dimensions: Metric dimensions
        """
        if not self.cloudwatch:
            return
        
        try:
            metric_dimensions = []
            if dimensions:
                for name, value_dim in dimensions.items():
                    metric_dimensions.append({
                        "Name": name,
                        "Value": value_dim,
                    })
            
            self.cloudwatch.put_metric_data(
                Namespace="AWS/CUR",
                MetricData=[
                    {
                        "MetricName": metric_name,
                        "Value": value,
                        "Unit": unit,
                        "Timestamp": datetime.utcnow(),
                        "Dimensions": metric_dimensions,
                    }
                ],
            )
        except (ClientError, BotoCoreError):
            pass
    
    def get_delivery_metrics(
        self,
        report_name: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[DeliveryMetric]:
        """
        Get delivery metrics for a CUR report.
        
        Args:
            report_name: Report name
            start_time: Start time for metrics
            end_time: End time for metrics
            
        Returns:
            List of DeliveryMetric objects
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not available")
        
        if not start_time:
            start_time = datetime.utcnow() - timedelta(days=7)
        if not end_time:
            end_time = datetime.utcnow()
        
        response = self.cloudwatch.get_metric_statistics(
            Namespace="AWS/CUR",
            MetricName="ReportDeliverySuccess",
            StartTime=start_time,
            EndTime=end_time,
            Period=86400,
            Statistics=["Sum", "Average"],
            Dimensions=[
                {"Name": "ReportName", "Value": report_name},
            ],
        )
        
        metrics = []
        for dp in response.get("Datapoints", []):
            metrics.append(DeliveryMetric(
                metric_name="ReportDeliverySuccess",
                value=dp["Average"],
                unit=dp["Unit"],
                timestamp=dp["Timestamp"],
                dimensions={"ReportName": report_name},
            ))
        
        return metrics
    
    def setup_cloudwatch_alarms(
        self,
        report_name: str,
        s3_bucket: str,
        alarm_topic_arn: Optional[str] = None,
        delivery_failure_threshold: int = 3,
    ) -> Dict[str, Any]:
        """
        Set up CloudWatch alarms for CUR delivery.
        
        Args:
            report_name: Report name
            s3_bucket: S3 bucket name
            alarm_topic_arn: SNS topic ARN for alarm notifications
            delivery_failure_threshold: Number of failures before alarm triggers
            
        Returns:
            Alarm setup result
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not available")
        
        result = {}
        
        alarm_name = f"{report_name}-delivery-failure"
        
        alarm_config = {
            "AlarmName": alarm_name,
            "AlarmDescription": f"Alarm for {report_name} delivery failures",
            "Namespace": "AWS/CUR",
            "MetricName": "ReportDeliveryFailure",
            "Statistic": "Sum",
            "Period": 86400,
            "EvaluationPeriods": delivery_failure_threshold,
            "Threshold": 1,
            "ComparisonOperator": "GreaterThanOrEqualToThreshold",
            "Dimensions": [
                {"Name": "ReportName", "Value": report_name},
                {"Name": "S3Bucket", "Value": s3_bucket},
            ],
        }
        
        if alarm_topic_arn:
            alarm_config["AlarmActions"] = [alarm_topic_arn]
            alarm_config["OKActions"] = [alarm_topic_arn]
        
        try:
            self.cloudwatch.put_metric_alarm(**alarm_config)
            result["alarm_created"] = True
            result["alarm_name"] = alarm_name
        except (ClientError, BotoCoreError) as e:
            result["alarm_created"] = False
            result["error"] = str(e)
        
        return result
    
    # =========================================================================
    # UTILITY METHODS
    # =========================================================================
    
    def validate_report_configuration(
        self,
        report_name: str,
        s3_bucket: str,
        s3_prefix: str,
        s3_region: str,
    ) -> Dict[str, Any]:
        """
        Validate CUR report configuration.
        
        Args:
            report_name: Report name
            s3_bucket: S3 bucket name
            s3_prefix: S3 prefix
            s3_region: S3 region
            
        Returns:
            Validation result
        """
        validation_result = {
            "valid": True,
            "errors": [],
            "warnings": [],
        }
        
        if not report_name or len(report_name) > 256:
            validation_result["valid"] = False
            validation_result["errors"].append("Report name must be between 1 and 256 characters")
        
        if not s3_bucket:
            validation_result["valid"] = False
            validation_result["errors"].append("S3 bucket is required")
        
        if not s3_prefix:
            validation_result["warnings"].append("S3 prefix is empty")
        
        if s3_region not in ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1", "ap-northeast-1"]:
            validation_result["warnings"].append(f"S3 region {s3_region} may not be supported")
        
        return validation_result
    
    def get_report_status(self, report_name: str) -> Dict[str, Any]:
        """
        Get status of a CUR report.
        
        Args:
            report_name: Report name
            
        Returns:
            Status information
        """
        if not self.cur_service:
            raise RuntimeError("Boto3 not available")
        
        try:
            report = self.get_report(report_name)
            
            status = {
                "report_name": report_name,
                "status": report.report_status,
                "created_at": report.created_at.isoformat() if report.created_at else None,
                "last_updated": report.last_updated.isoformat() if report.last_updated else None,
                "s3_bucket": report.s3_bucket,
                "s3_prefix": report.s3_prefix,
                "time_unit": report.time_unit.value,
                "compression": report.compression.value,
            }
            
            files = self.list_s3_report_files(
                s3_bucket=report.s3_bucket,
                s3_prefix=report.s3_prefix,
                report_name=report_name,
                max_keys=10,
            )
            status["recent_files_count"] = len(files)
            if files:
                status["latest_file"] = files[0]["last_modified"].isoformat()
            
            return status
            
        except ClientError as e:
            return {
                "report_name": report_name,
                "status": "ERROR",
                "error": str(e),
            }
    
    def export_report_config(self, report_name: str) -> Dict[str, Any]:
        """
        Export report configuration as a dictionary.
        
        Args:
            report_name: Report name
            
        Returns:
            Configuration dictionary
        """
        report = self.get_report(report_name)
        
        config = {
            "report_name": report.report_name,
            "s3_bucket": report.s3_bucket,
            "s3_prefix": report.s3_prefix,
            "s3_region": report.s3_region,
            "time_unit": report.time_unit.value,
            "format": report.format,
            "compression": report.compression.value,
            "schema_type": report.schema_type.value,
            "versioning": report.versioning.value,
            "additional_schema_elements": report.additional_schema_elements,
        }
        
        retention_policy = self.get_retention_policy(report_name)
        if retention_policy:
            config["retention_policy"] = {
                "enabled": retention_policy.enabled,
                "retention_days": retention_policy.retention_days,
                "archive_before_delete": retention_policy.archive_before_delete,
                "archive_location": retention_policy.archive_location,
            }
        
        return config
