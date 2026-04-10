"""
AWS CloudWatch Monitoring Integration Module for Workflow System

Implements a CloudWatchIntegration class with:
1. Metrics: Put/get metrics, custom metrics
2. Alarms: Create/manage CloudWatch alarms
3. Dashboards: Create/manage dashboards
4. Logs: CloudWatch Logs management
5. Events: CloudWatch Events/EventBridge
6. Synthetics: CloudWatch Synthetics canaries
7. Contributor Insights: Contributor insights rules
8. Service Level: Service level indicators
9. Embedded metrics: Embedded metric format
10. Application Signals: Application Signals for ECS/EKS

Commit: 'feat(aws-cloudwatch): add AWS CloudWatch with metrics, alarms, dashboards, logs, events, synthetics, contributor insights, service level, embedded metrics, application signals'
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


class MetricStatistic(Enum):
    """Metric statistic types."""
    SAMPLE_COUNT = "SampleCount"
    AVERAGE = "Average"
    SUM = "Sum"
    MINIMUM = "Minimum"
    MAXIMUM = "Maximum"


class AlarmState(Enum):
    """CloudWatch alarm states."""
    OK = "OK"
    ALARM = "ALARM"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"


class AlarmActionType(Enum):
    """Alarm action types."""
    SNS = "sns"
    AUTO_SCALING = "autoscaling"
    EC2 = "ec2"


class LogFormat(Enum):
    """Log format types."""
    JSON = "json"
    PLAIN_TEXT = "plaintext"
    RAW = "raw"


class SyntheticsRunStatus(Enum):
    """Synthetics canary run status."""
    RUNNING = "RUNNING"
    PASSED = "PASSED"
    FAILED = "FAILED"


class InsightRuleState(Enum):
    """Contributor insights rule state."""
    ENABLED = "ENABLED"
    DISABLED = "DISABLED"
    DELETED = "DELETED"


class SLIType(Enum):
    """Service level indicator types."""
    AVAILABILITY = "availability"
    LATENCY = "latency"
    THROUGHPUT = "throughput"
    ERROR_RATE = "error_rate"


@dataclass
class MetricData:
    """Container for metric data point."""
    metric_name: str
    value: float
    timestamp: Optional[datetime] = None
    unit: str = "None"
    dimensions: Dict[str, str] = field(default_factory=dict)
    statistics: Optional[Dict[str, float]] = None


@dataclass
class AlarmConfig:
    """CloudWatch alarm configuration."""
    alarm_name: str
    metric_name: str
    namespace: str
    threshold: float
    comparison_operator: str
    period: int = 60
    evaluation_periods: int = 1
    statistic: str = "Average"
    alarm_actions: List[str] = field(default_factory=list)
    ok_actions: List[str] = field(default_factory=list)
    dimensions: Dict[str, str] = field(default_factory=dict)
    treat_missing_data: str = "missing"
    evaluate_low_sample_count_percentile: str = ""


@dataclass
class DashboardWidget:
    """Dashboard widget configuration."""
    widget_type: str
    title: str
    width: int = 6
    height: int = 6
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class LogQuery:
    """CloudWatch Logs query configuration."""
    query_string: str
    log_group_name: str
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    limit: int = 1000


@dataclass
class CanaryConfig:
    """Synthetics canary configuration."""
    name: str
    execution_role_arn: str
    handler: str
    code_bucket: str
    code_key: str
    schedule_expression: str = "rate(5 minutes)"
    runtime_version: str = "syn-nodejs-puppeteer-6.0"
    failure_retention_period: int = 31
    success_retention_period: int = 31


@dataclass
class ContributorInsightRule:
    """Contributor insights rule configuration."""
    rule_name: str
    log_group_name: str
    schema: Dict[str, Any]
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class ServiceLevelIndicator:
    """Service level indicator configuration."""
    name: str
    sli_type: SLIType
    metric_name: str
    namespace: str
    target: float
    period: int = 60
    dimensions: Dict[str, str] = field(default_factory=dict)


@dataclass
class EmbeddedMetricSpec:
    """Embedded metric format specification."""
    namespace: str
    metric_name: str
    dimensions: List[str]
    metrics: List[Dict[str, Any]]


class CloudWatchIntegration:
    """
    AWS CloudWatch Monitoring Integration.
    
    Provides comprehensive CloudWatch functionality including:
    - Metrics: Put/get custom metrics, metric math
    - Alarms: Create and manage CloudWatch alarms
    - Dashboards: Create and manage dashboards
    - Logs: CloudWatch Logs management and querying
    - Events: CloudWatch Events/EventBridge integration
    - Synthetics: CloudWatch Synthetics canaries
    - Contributor Insights: Contributor insights rules
    - Service Level: Service level indicators and objectives
    - Embedded Metrics: Embedded metric format support
    - Application Signals: Application Signals for ECS/EKS
    
    Attributes:
        region_name: AWS region name
        profile_name: AWS profile name (optional)
        endpoint_url: Custom endpoint URL (optional)
    """
    
    def __init__(
        self,
        region_name: str = "us-east-1",
        profile_name: Optional[str] = None,
        endpoint_url: Optional[str] = None
    ):
        """
        Initialize CloudWatch integration.
        
        Args:
            region_name: AWS region for CloudWatch operations
            profile_name: AWS credentials profile name
            endpoint_url: Custom CloudWatch endpoint URL
        """
        self.region_name = region_name
        self.profile_name = profile_name
        self.endpoint_url = endpoint_url
        self._clients = {}
        self._resources = {}
        self._lock = threading.RLock()
        
        if BOTO3_AVAILABLE:
            self._initialize_clients()
    
    def _initialize_clients(self):
        """Initialize boto3 clients for CloudWatch services."""
        try:
            session_kwargs = {"region_name": self.region_name}
            if self.profile_name:
                session_kwargs["profile_name"] = self.profile_name
            
            session = boto3.Session(**session_kwargs)
            
            # CloudWatch client
            cw_kwargs = {"region_name": self.region_name}
            if self.endpoint_url:
                cw_kwargs["endpoint_url"] = self.endpoint_url
            self._clients["cloudwatch"] = session.client("cloudwatch", **cw_kwargs)
            
            # CloudWatch Logs client
            self._clients["logs"] = session.client("logs", region_name=self.region_name)
            
            # CloudWatch Events/EventBridge client
            self._clients["events"] = session.client("events", region_name=self.region_name)
            
            # Application Auto Scaling client
            self._clients["application_autoscaling"] = session.client(
                "application-autoscaling", region_name=self.region_name
            )
            
            # S3 client for canary code
            self._clients["s3"] = session.client("s3", region_name=self.region_name)
            
            # IAM client
            self._clients["iam"] = session.client("iam", region_name=self.region_name)
            
            # Lambda client (for synthetics)
            self._clients["lambda"] = session.client("lambda", region_name=self.region_name)
            
            logger.info(f"CloudWatch clients initialized for region {self.region_name}")
        except Exception as e:
            logger.error(f"Failed to initialize CloudWatch clients: {e}")
    
    @property
    def cloudwatch(self):
        """Get CloudWatch client."""
        return self._clients.get("cloudwatch")
    
    @property
    def logs(self):
        """Get CloudWatch Logs client."""
        return self._clients.get("logs")
    
    @property
    def events(self):
        """Get CloudWatch Events client."""
        return self._clients.get("events")
    
    # =========================================================================
    # METRICS
    # =========================================================================
    
    def put_metric_data(
        self,
        namespace: str,
        metrics: List[MetricData],
        storage_resolution: int = 60
    ) -> Dict[str, Any]:
        """
        Put custom metric data to CloudWatch.
        
        Args:
            namespace: Metric namespace (e.g., 'MyApplication')
            metrics: List of MetricData objects
            storage_resolution: Storage resolution (1 or 60 seconds)
        
        Returns:
            dict: CloudWatch response
        
        Example:
            >>> metric = MetricData(
            ...     metric_name="RequestCount",
            ...     value=100.0,
            ...     unit="Count",
            ...     dimensions={"Service": "API"}
            ... )
            >>> self.put_metric_data("MyApp", [metric])
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")
        
        metric_data = []
        for metric in metrics:
            entry = {
                "MetricName": metric.metric_name,
                "Value": metric.value,
                "Unit": metric.unit,
                "StorageResolution": storage_resolution,
            }
            
            if metric.timestamp:
                entry["Timestamp"] = metric.timestamp.isoformat()
            
            if metric.dimensions:
                entry["Dimensions"] = [
                    {"Name": k, "Value": v}
                    for k, v in metric.dimensions.items()
                ]
            
            if metric.statistics:
                entry["StatisticValues"] = {
                    "SampleCount": metric.statistics.get("SampleCount", 0),
                    "Sum": metric.statistics.get("Sum", 0),
                    "Minimum": metric.statistics.get("Minimum", 0),
                    "Maximum": metric.statistics.get("Maximum", 0),
                    "Average": metric.statistics.get("Average", 0),
                }
            
            metric_data.append(entry)
        
        try:
            response = self.cloudwatch.put_metric_data(
                Namespace=namespace,
                MetricData=metric_data
            )
            logger.info(f"Put {len(metrics)} metrics to namespace {namespace}")
            return response
        except ClientError as e:
            logger.error(f"Failed to put metric data: {e}")
            raise
    
    def get_metric_data(
        self,
        metric_queries: List[Dict[str, Any]],
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """
        Get metric data from CloudWatch using metric math.
        
        Args:
            metric_queries: List of metric query definitions
            start_time: Start time for query
            end_time: End time for query
        
        Returns:
            dict: Metric data results
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")
        
        try:
            response = self.cloudwatch.get_metric_data(
                MetricDataQueries=metric_queries,
                StartTime=start_time.isoformat(),
                EndTime=end_time.isoformat()
            )
            return response
        except ClientError as e:
            logger.error(f"Failed to get metric data: {e}")
            raise
    
    def get_metric_statistics(
        self,
        namespace: str,
        metric_name: str,
        start_time: datetime,
        end_time: datetime,
        period: int = 60,
        statistics: List[str] = None,
        extended_statistics: List[str] = None,
        dimensions: List[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Get metric statistics for a specific metric.
        
        Args:
            namespace: Metric namespace
            metric_name: Metric name
            start_time: Start time
            end_time: End time
            period: Period in seconds
            statistics: List of statistics (SampleCount, Average, Sum, Minimum, Maximum)
            extended_statistics: List of extended statistics (percentiles)
            dimensions: Metric dimensions
        
        Returns:
            dict: Metric statistics
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")
        
        if statistics is None:
            statistics = ["Average", "Minimum", "Maximum"]
        
        try:
            kwargs = {
                "Namespace": namespace,
                "MetricName": metric_name,
                "StartTime": start_time.isoformat(),
                "EndTime": end_time.isoformat(),
                "Period": period,
                "Statistics": statistics,
            }
            
            if extended_statistics:
                kwargs["ExtendedStatistics"] = extended_statistics
            
            if dimensions:
                kwargs["Dimensions"] = dimensions
            
            response = self.cloudwatch.get_metric_statistics(**kwargs)
            return response
        except ClientError as e:
            logger.error(f"Failed to get metric statistics: {e}")
            raise
    
    def list_metrics(
        self,
        namespace: Optional[str] = None,
        metric_name: Optional[str] = None,
        dimensions: Optional[List[Dict[str, str]]] = None
    ) -> List[Dict[str, Any]]:
        """
        List metrics matching specified criteria.
        
        Args:
            namespace: Filter by namespace
            metric_name: Filter by metric name
            dimensions: Filter by dimensions
        
        Returns:
            list: Matching metrics
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")
        
        all_metrics = []
        try:
            kwargs = {}
            if namespace:
                kwargs["Namespace"] = namespace
            if metric_name:
                kwargs["MetricName"] = metric_name
            if dimensions:
                kwargs["Dimensions"] = dimensions
            
            paginator = self.cloudwatch.get_paginator("list_metrics")
            for page in paginator.paginate(**kwargs):
                all_metrics.extend(page.get("Metrics", []))
            
            return all_metrics
        except ClientError as e:
            logger.error(f"Failed to list metrics: {e}")
            raise
    
    def put_metric_alarm(
        self,
        config: AlarmConfig
    ) -> Dict[str, Any]:
        """
        Create or update a CloudWatch alarm.
        
        Args:
            config: AlarmConfig with alarm settings
        
        Returns:
            dict: CloudWatch response
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")
        
        try:
            kwargs = {
                "AlarmName": config.alarm_name,
                "MetricName": config.metric_name,
                "Namespace": config.namespace,
                "Threshold": config.threshold,
                "ComparisonOperator": config.comparison_operator,
                "Period": config.period,
                "EvaluationPeriods": config.evaluation_periods,
                "Statistic": config.statistic,
                "TreatMissingData": config.treat_missing_data,
            }
            
            if config.alarm_actions:
                kwargs["AlarmActions"] = config.alarm_actions
            if config.ok_actions:
                kwargs["OKActions"] = config.ok_actions
            if config.dimensions:
                kwargs["Dimensions"] = [
                    {"Name": k, "Value": v}
                    for k, v in config.dimensions.items()
                ]
            if config.evaluate_low_sample_count_percentile:
                kwargs["EvaluateLowSampleCountPercentile"] = config.evaluate_low_sample_count_percentile
            
            response = self.cloudwatch.put_metric_alarm(**kwargs)
            logger.info(f"Created/updated alarm: {config.alarm_name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to put metric alarm: {e}")
            raise
    
    def describe_alarms(
        self,
        alarm_names: Optional[List[str]] = None,
        alarm_prefix: Optional[str] = None,
        state_value: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Describe CloudWatch alarms.
        
        Args:
            alarm_names: Specific alarm names to describe
            alarm_prefix: Filter alarms by name prefix
            state_value: Filter by alarm state (OK, ALARM, INSUFFICIENT_DATA)
        
        Returns:
            list: Alarm descriptions
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")
        
        try:
            alarms = []
            
            if alarm_names:
                kwargs = {"AlarmNames": alarm_names}
                paginator = self.cloudwatch.get_paginator("describe_alarms")
                for page in paginator.paginate(**kwargs):
                    alarms.extend(page.get("MetricAlarms", []))
            else:
                kwargs = {}
                if alarm_prefix:
                    kwargs["AlarmNamePrefix"] = alarm_prefix
                if state_value:
                    kwargs["StateValue"] = state_value
                
                paginator = self.cloudwatch.get_paginator("describe_alarms")
                for page in paginator.paginate(**kwargs):
                    alarms.extend(page.get("MetricAlarms", []))
            
            return alarms
        except ClientError as e:
            logger.error(f"Failed to describe alarms: {e}")
            raise
    
    def delete_alarms(self, alarm_names: List[str]) -> Dict[str, Any]:
        """
        Delete CloudWatch alarms.
        
        Args:
            alarm_names: List of alarm names to delete
        
        Returns:
            dict: CloudWatch response
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")
        
        try:
            response = self.cloudwatch.delete_alarms(AlarmNames=alarm_names)
            logger.info(f"Deleted {len(alarm_names)} alarms")
            return response
        except ClientError as e:
            logger.error(f"Failed to delete alarms: {e}")
            raise
    
    def set_alarm_state(
        self,
        alarm_name: str,
        state_value: str,
        state_reason: str,
        state_reason_data: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Set the state of an alarm.
        
        Args:
            alarm_name: Alarm name
            state_value: State value (OK, ALARM, INSUFFICIENT_DATA)
            state_reason: Reason for the state change
            state_reason_data: JSON data for the state reason
        
        Returns:
            dict: CloudWatch response
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")
        
        try:
            kwargs = {
                "AlarmName": alarm_name,
                "StateValue": state_value,
                "StateReason": state_reason,
            }
            if state_reason_data:
                kwargs["StateReasonData"] = state_reason_data
            
            response = self.cloudwatch.set_alarm_state(**kwargs)
            logger.info(f"Set alarm {alarm_name} to state {state_value}")
            return response
        except ClientError as e:
            logger.error(f"Failed to set alarm state: {e}")
            raise
    
    # =========================================================================
    # DASHBOARDS
    # =========================================================================
    
    def create_dashboard(
        self,
        dashboard_name: str,
        widgets: List[DashboardWidget] = None,
        dashboard_body: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a CloudWatch dashboard.
        
        Args:
            dashboard_name: Name for the dashboard
            widgets: List of DashboardWidget objects
            dashboard_body: Raw dashboard body JSON string (alternative to widgets)
        
        Returns:
            dict: CloudWatch response
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")
        
        try:
            if dashboard_body:
                body = dashboard_body
            elif widgets:
                body = json.dumps({"widgets": [self._widget_to_dict(w) for w in widgets]})
            else:
                body = json.dumps({"widgets": []})
            
            response = self.cloudwatch.put_dashboard(
                DashboardName=dashboard_name,
                DashboardBody=body
            )
            logger.info(f"Created dashboard: {dashboard_name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to create dashboard: {e}")
            raise
    
    def _widget_to_dict(self, widget: DashboardWidget) -> Dict[str, Any]:
        """Convert DashboardWidget to dictionary format."""
        return {
            "type": widget.widget_type,
            "width": widget.width,
            "height": widget.height,
            "properties": {
                "title": widget.title,
                **widget.properties
            }
        }
    
    def get_dashboard(self, dashboard_name: str) -> Dict[str, Any]:
        """
        Get a CloudWatch dashboard.
        
        Args:
            dashboard_name: Name of the dashboard
        
        Returns:
            dict: Dashboard data including body
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")
        
        try:
            response = self.cloudwatch.get_dashboard(DashboardName=dashboard_name)
            return response
        except ClientError as e:
            logger.error(f"Failed to get dashboard: {e}")
            raise
    
    def list_dashboards(self) -> List[Dict[str, Any]]:
        """
        List all CloudWatch dashboards.
        
        Returns:
            list: Dashboard summaries
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")
        
        try:
            dashboards = []
            paginator = self.cloudwatch.get_paginator("list_dashboards")
            for page in paginator.paginate():
                dashboards.extend(page.get("DashboardEntries", []))
            return dashboards
        except ClientError as e:
            logger.error(f"Failed to list dashboards: {e}")
            raise
    
    def delete_dashboard(self, dashboard_name: str) -> Dict[str, Any]:
        """
        Delete a CloudWatch dashboard.
        
        Args:
            dashboard_name: Name of the dashboard to delete
        
        Returns:
            dict: CloudWatch response
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")
        
        try:
            response = self.cloudwatch.delete_dashboards(DashboardNames=[dashboard_name])
            logger.info(f"Deleted dashboard: {dashboard_name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to delete dashboard: {e}")
            raise
    
    def update_dashboard(
        self,
        dashboard_name: str,
        widgets: List[DashboardWidget]
    ) -> Dict[str, Any]:
        """
        Update a CloudWatch dashboard.
        
        Args:
            dashboard_name: Name of the dashboard to update
            widgets: Updated list of widgets
        
        Returns:
            dict: CloudWatch response
        """
        return self.create_dashboard(dashboard_name, widgets)
    
    # =========================================================================
    # LOGS
    # =========================================================================
    
    def create_log_group(
        self,
        log_group_name: str,
        retention_days: Optional[int] = None,
        kms_key_id: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Create a CloudWatch log group.
        
        Args:
            log_group_name: Name for the log group
            retention_days: Log retention period in days
            kms_key_id: KMS key ID for encryption
            tags: Tags for the log group
        
        Returns:
            dict: CloudWatch response
        """
        if not self.logs:
            raise RuntimeError("CloudWatch Logs client not initialized")
        
        try:
            kwargs = {"logGroupName": log_group_name}
            if retention_days:
                kwargs["retentionInDays"] = retention_days
            if kms_key_id:
                kwargs["kmsKeyId"] = kms_key_id
            if tags:
                kwargs["tags"] = tags
            
            response = self.logs.create_log_group(**kwargs)
            logger.info(f"Created log group: {log_group_name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to create log group: {e}")
            raise
    
    def delete_log_group(self, log_group_name: str) -> Dict[str, Any]:
        """
        Delete a CloudWatch log group.
        
        Args:
            log_group_name: Name of the log group to delete
        
        Returns:
            dict: CloudWatch response
        """
        if not self.logs:
            raise RuntimeError("CloudWatch Logs client not initialized")
        
        try:
            response = self.logs.delete_log_group(logGroupNamePrefix=log_group_name)
            logger.info(f"Deleted log group: {log_group_name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to delete log group: {e}")
            raise
    
    def describe_log_groups(
        self,
        log_group_name_prefix: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Describe CloudWatch log groups.
        
        Args:
            log_group_name_prefix: Filter log groups by name prefix
            limit: Maximum number of results
        
        Returns:
            list: Log group descriptions
        """
        if not self.logs:
            raise RuntimeError("CloudWatch Logs client not initialized")
        
        try:
            kwargs = {}
            if log_group_name_prefix:
                kwargs["logGroupNamePrefix"] = log_group_name_prefix
            kwargs["limit"] = limit
            
            log_groups = []
            paginator = self.logs.get_paginator("describe_log_groups")
            for page in paginator.paginate(**kwargs):
                log_groups.extend(page.get("logGroups", []))
            
            return log_groups
        except ClientError as e:
            logger.error(f"Failed to describe log groups: {e}")
            raise
    
    def put_log_events(
        self,
        log_group_name: str,
        log_stream_name: str,
        log_events: List[Dict[str, Any]],
        sequence_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Put log events to a CloudWatch log stream.
        
        Args:
            log_group_name: Log group name
            log_stream_name: Log stream name
            log_events: List of log events [{"timestamp": int, "message": str}, ...]
            sequence_token: Sequence token for ordering
        
        Returns:
            dict: CloudWatch response with next sequence token
        """
        if not self.logs:
            raise RuntimeError("CloudWatch Logs client not initialized")
        
        try:
            kwargs = {
                "logGroupName": log_group_name,
                "logStreamName": log_stream_name,
                "logEvents": log_events,
            }
            if sequence_token:
                kwargs["sequenceToken"] = sequence_token
            
            response = self.logs.put_log_events(**kwargs)
            logger.debug(f"Put {len(log_events)} log events to {log_stream_name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to put log events: {e}")
            raise
    
    def create_log_stream(
        self,
        log_group_name: str,
        log_stream_name: str
    ) -> Dict[str, Any]:
        """
        Create a CloudWatch log stream.
        
        Args:
            log_group_name: Log group name
            log_stream_name: Log stream name
        
        Returns:
            dict: CloudWatch response
        """
        if not self.logs:
            raise RuntimeError("CloudWatch Logs client not initialized")
        
        try:
            response = self.logs.create_log_stream(
                logGroupName=log_group_name,
                logStreamName=log_stream_name
            )
            logger.info(f"Created log stream: {log_stream_name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to create log stream: {e}")
            raise
    
    def filter_log_events(
        self,
        log_group_name: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        filter_pattern: Optional[str] = None,
        log_stream_names: Optional[List[str]] = None,
        limit: int = 10000
    ) -> List[Dict[str, Any]]:
        """
        Filter CloudWatch log events.
        
        Args:
            log_group_name: Log group name
            start_time: Start time in milliseconds
            end_time: End time in milliseconds
            filter_pattern: CloudWatch Logs filter pattern
            log_stream_names: Specific log streams to search
            limit: Maximum number of events to return
        
        Returns:
            list: Matching log events
        """
        if not self.logs:
            raise RuntimeError("CloudWatch Logs client not initialized")
        
        try:
            kwargs = {
                "logGroupName": log_group_name,
                "limit": limit,
            }
            if start_time:
                kwargs["startTime"] = start_time
            if end_time:
                kwargs["endTime"] = end_time
            if filter_pattern:
                kwargs["filterPattern"] = filter_pattern
            if log_stream_names:
                kwargs["logStreamNames"] = log_stream_names
            
            events = []
            paginator = self.logs.get_paginator("filter_log_events")
            for page in paginator.paginate(**kwargs):
                events.extend(page.get("events", []))
            
            return events
        except ClientError as e:
            logger.error(f"Failed to filter log events: {e}")
            raise
    
    def start_query(
        self,
        log_group_name: str,
        query_string: str,
        start_time: datetime,
        end_time: datetime,
        limit: int = 1000
    ) -> str:
        """
        Start a CloudWatch Logs Insights query.
        
        Args:
            log_group_name: Log group name
            query_string: CloudWatch Logs Insights query string
            start_time: Query start time
            end_time: Query end time
            limit: Maximum number of results
        
        Returns:
            str: Query ID for retrieving results
        """
        if not self.logs:
            raise RuntimeError("CloudWatch Logs client not initialized")
        
        try:
            response = self.logs.start_query(
                logGroupName=log_group_name,
                startTime=int(start_time.timestamp()),
                endTime=int(end_time.timestamp()),
                queryString=query_string,
                limit=limit
            )
            query_id = response.get("queryId")
            logger.info(f"Started query {query_id}")
            return query_id
        except ClientError as e:
            logger.error(f"Failed to start query: {e}")
            raise
    
    def get_query_results(self, query_id: str) -> Dict[str, Any]:
        """
        Get results of a CloudWatch Logs Insights query.
        
        Args:
            query_id: Query ID from start_query
        
        Returns:
            dict: Query results with status
        """
        if not self.logs:
            raise RuntimeError("CloudWatch Logs client not initialized")
        
        try:
            response = self.logs.get_query_results(queryId=query_id)
            return response
        except ClientError as e:
            logger.error(f"Failed to get query results: {e}")
            raise
    
    def describe_resource_policies(self) -> List[Dict[str, Any]]:
        """
        Describe CloudWatch Logs resource policies.
        
        Returns:
            list: Resource policies
        """
        if not self.logs:
            raise RuntimeError("CloudWatch Logs client not initialized")
        
        try:
            response = self.logs.describe_resource_policies()
            return response.get("resourcePolicies", [])
        except ClientError as e:
            logger.error(f"Failed to describe resource policies: {e}")
            raise
    
    def put_resource_policy(
        self,
        policy_name: str,
        policy_document: str
    ) -> Dict[str, Any]:
        """
        Put a CloudWatch Logs resource policy.
        
        Args:
            policy_name: Name of the policy
            policy_document: IAM policy document JSON
        
        Returns:
            dict: CloudWatch response
        """
        if not self.logs:
            raise RuntimeError("CloudWatch Logs client not initialized")
        
        try:
            response = self.logs.put_resource_policy(
                policyName=policy_name,
                policyDocument=policy_document
            )
            logger.info(f"Put resource policy: {policy_name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to put resource policy: {e}")
            raise
    
    # =========================================================================
    # EVENTS (EventBridge)
    # =========================================================================
    
    def create_event_bus(
        self,
        name: str,
        tags: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Create an EventBridge event bus.
        
        Args:
            name: Event bus name
            tags: Tags for the event bus
        
        Returns:
            dict: EventBridge response
        """
        if not self.events:
            raise RuntimeError("CloudWatch Events client not initialized")
        
        try:
            kwargs = {"Name": name}
            if tags:
                kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]
            
            response = self.events.create_event_bus(**kwargs)
            logger.info(f"Created event bus: {name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to create event bus: {e}")
            raise
    
    def put_rule(
        self,
        name: str,
        event_pattern: Optional[str] = None,
        schedule_expression: Optional[str] = None,
        state: str = "ENABLED",
        description: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create or update an EventBridge rule.
        
        Args:
            name: Rule name
            event_pattern: Event pattern JSON for event-driven rules
            schedule_expression: Cron or rate expression for scheduled rules
            state: Rule state (ENABLED or DISABLED)
            description: Rule description
        
        Returns:
            dict: EventBridge response with rule ARN
        """
        if not self.events:
            raise RuntimeError("CloudWatch Events client not initialized")
        
        if not event_pattern and not schedule_expression:
            raise ValueError("Either event_pattern or schedule_expression is required")
        
        try:
            kwargs = {"Name": name, "State": state}
            if event_pattern:
                kwargs["EventPattern"] = event_pattern
            if schedule_expression:
                kwargs["ScheduleExpression"] = schedule_expression
            if description:
                kwargs["Description"] = description
            
            response = self.events.put_rule(**kwargs)
            logger.info(f"Created/updated rule: {name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to put rule: {e}")
            raise
    
    def put_targets(
        self,
        rule_name: str,
        targets: List[Dict[str, Any]],
        event_bus_name: str = "default"
    ) -> Dict[str, Any]:
        """
        Add targets to an EventBridge rule.
        
        Args:
            rule_name: Rule name
            targets: List of targets [{"Id": str, "Arn": str, ...}, ...]
            event_bus_name: Event bus name
        
        Returns:
            dict: EventBridge response
        """
        if not self.events:
            raise RuntimeError("CloudWatch Events client not initialized")
        
        try:
            response = self.events.put_targets(
                Rule=rule_name,
                EventBusName=event_bus_name,
                Targets=targets
            )
            logger.info(f"Added {len(targets)} targets to rule: {rule_name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to put targets: {e}")
            raise
    
    def remove_targets(
        self,
        rule_name: str,
        target_ids: List[str],
        event_bus_name: str = "default"
    ) -> Dict[str, Any]:
        """
        Remove targets from an EventBridge rule.
        
        Args:
            rule_name: Rule name
            target_ids: List of target IDs to remove
            event_bus_name: Event bus name
        
        Returns:
            dict: EventBridge response
        """
        if not self.events:
            raise RuntimeError("CloudWatch Events client not initialized")
        
        try:
            response = self.events.remove_targets(
                Rule=rule_name,
                EventBusName=event_bus_name,
                Ids=target_ids
            )
            logger.info(f"Removed targets from rule: {rule_name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to remove targets: {e}")
            raise
    
    def describe_rules(
        self,
        name_prefix: Optional[str] = None,
        event_bus_name: str = "default"
    ) -> List[Dict[str, Any]]:
        """
        Describe EventBridge rules.
        
        Args:
            name_prefix: Filter rules by name prefix
            event_bus_name: Event bus name
        
        Returns:
            list: Rule descriptions
        """
        if not self.events:
            raise RuntimeError("CloudWatch Events client not initialized")
        
        try:
            kwargs = {"EventBusName": event_bus_name}
            if name_prefix:
                kwargs["NamePrefix"] = name_prefix
            
            rules = []
            paginator = self.events.get_paginator("list_rules")
            for page in paginator.paginate(**kwargs):
                rules.extend(page.get("Rules", []))
            
            return rules
        except ClientError as e:
            logger.error(f"Failed to describe rules: {e}")
            raise
    
    def delete_rule(self, name: str, event_bus_name: str = "default") -> Dict[str, Any]:
        """
        Delete an EventBridge rule.
        
        Args:
            name: Rule name
            event_bus_name: Event bus name
        
        Returns:
            dict: EventBridge response
        """
        if not self.events:
            raise RuntimeError("CloudWatch Events client not initialized")
        
        try:
            response = self.events.delete_rule(Name=name, EventBusName=event_bus_name)
            logger.info(f"Deleted rule: {name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to delete rule: {e}")
            raise
    
    def put_events(
        self,
        entries: List[Dict[str, Any]],
        event_bus_name: str = "default"
    ) -> Dict[str, Any]:
        """
        Put events to an EventBridge event bus.
        
        Args:
            entries: List of events [{"Source": str, "DetailType": str, "Detail": str}, ...]
            event_bus_name: Event bus name
        
        Returns:
            dict: EventBridge response
        """
        if not self.events:
            raise RuntimeError("CloudWatch Events client not initialized")
        
        try:
            response = self.events.put_events(
                Entries=entries,
                EventBusName=event_bus_name
            )
            return response
        except ClientError as e:
            logger.error(f"Failed to put events: {e}")
            raise
    
    # =========================================================================
    # SYNTHETICS
    # =========================================================================
    
    def create_canary(
        self,
        config: CanaryConfig,
        code_content: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a CloudWatch Synthetics canary.
        
        Args:
            config: CanaryConfig with canary settings
            code_content: Base64 encoded canary script (alternative to bucket)
        
        Returns:
            dict: Synthetics response
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")
        
        try:
            kwargs = {
                "Name": config.name,
                "ExecutionRoleArn": config.execution_role_arn,
                "Handler": config.handler,
                "RuntimeVersion": config.runtime_version,
                "Schedule": {
                    "Expression": config.schedule_expression,
                    "DurationInSeconds": 0
                },
                "FailureRetensionPeriod": config.failure_retention_period,
                "SuccessRetensionPeriod": config.success_retention_period,
            }
            
            if code_content:
                kwargs["Code"] = {"ZipFile": code_content}
            else:
                kwargs["Code"] = {
                    "S3Bucket": config.code_bucket,
                    "S3Key": config.code_key,
                }
            
            response = self.cloudwatch.put_canary(**kwargs)
            logger.info(f"Created canary: {config.name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to create canary: {e}")
            raise
    
    def start_canary(self, name: str) -> Dict[str, Any]:
        """
        Start a CloudWatch Synthetics canary.
        
        Args:
            name: Canary name
        
        Returns:
            dict: Synthetics response
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")
        
        try:
            response = self.cloudwatch.start_canary(Name=name)
            logger.info(f"Started canary: {name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to start canary: {e}")
            raise
    
    def stop_canary(self, name: str) -> Dict[str, Any]:
        """
        Stop a CloudWatch Synthetics canary.
        
        Args:
            name: Canary name
        
        Returns:
            dict: Synthetics response
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")
        
        try:
            response = self.cloudwatch.stop_canary(Name=name)
            logger.info(f"Stopped canary: {name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to stop canary: {e}")
            raise
    
    def describe_canaries(
        self,
        name_prefix: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Describe CloudWatch Synthetics canaries.
        
        Args:
            name_prefix: Filter canaries by name prefix
        
        Returns:
            list: Canary descriptions
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")
        
        try:
            kwargs = {}
            if name_prefix:
                kwargs["Name"] = name_prefix
            
            canaries = []
            paginator = self.cloudwatch.get_paginator("describe_canaries")
            for page in paginator.paginate(**kwargs):
                canaries.extend(page.get("Canaries", []))
            
            return canaries
        except ClientError as e:
            logger.error(f"Failed to describe canaries: {e}")
            raise
    
    def get_canary_runs(
        self,
        name: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Get runs for a CloudWatch Synthetics canary.
        
        Args:
            name: Canary name
            start_time: Filter by start time
            end_time: Filter by end time
        
        Returns:
            list: Canary runs
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")
        
        try:
            kwargs = {"Name": name}
            if start_time:
                kwargs["StartTime"] = start_time.isoformat()
            if end_time:
                kwargs["EndTime"] = end_time.isoformat()
            
            runs = []
            paginator = self.cloudwatch.get_paginator("get_canary_runs")
            for page in paginator.paginate(**kwargs):
                runs.extend(page.get("CanaryRuns", []))
            
            return runs
        except ClientError as e:
            logger.error(f"Failed to get canary runs: {e}")
            raise
    
    def delete_canary(self, name: str) -> Dict[str, Any]:
        """
        Delete a CloudWatch Synthetics canary.
        
        Args:
            name: Canary name
        
        Returns:
            dict: Synthetics response
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")
        
        try:
            response = self.cloudwatch.delete_canary(Name=name)
            logger.info(f"Deleted canary: {name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to delete canary: {e}")
            raise
    
    # =========================================================================
    # CONTRIBUTOR INSIGHTS
    # =========================================================================
    
    def create_insight_rule(
        self,
        rule: ContributorInsightRule
    ) -> Dict[str, Any]:
        """
        Create a CloudWatch Contributor Insights rule.
        
        Args:
            rule: ContributorInsightRule with rule settings
        
        Returns:
            dict: CloudWatch response
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")
        
        try:
            response = self.cloudwatch.put_insight_rule(
                RuleName=rule.rule_name,
                RuleState=InsightRuleState.ENABLED.value,
                Schema=Rule schema definition",
                LogGroupName=rule.log_group_name,
                Tags=rule.tags if rule.tags else None
            )
            logger.info(f"Created contributor insight rule: {rule.rule_name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to create insight rule: {e}")
            raise
    
    def enable_insight_rule(self, rule_name: str) -> Dict[str, Any]:
        """
        Enable a CloudWatch Contributor Insights rule.
        
        Args:
            rule_name: Rule name
        
        Returns:
            dict: CloudWatch response
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")
        
        try:
            response = self.cloudwatch.enable_insight_rules(Names=[rule_name])
            logger.info(f"Enabled insight rule: {rule_name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to enable insight rule: {e}")
            raise
    
    def disable_insight_rule(self, rule_name: str) -> Dict[str, Any]:
        """
        Disable a CloudWatch Contributor Insights rule.
        
        Args:
            rule_name: Rule name
        
        Returns:
            dict: CloudWatch response
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")
        
        try:
            response = self.cloudwatch.disable_insight_rules(Names=[rule_name])
            logger.info(f"Disabled insight rule: {rule_name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to disable insight rule: {e}")
            raise
    
    def describe_insight_rules(
        self,
        rule_names: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Describe CloudWatch Contributor Insights rules.
        
        Args:
            rule_names: Specific rule names to describe
        
        Returns:
            list: Rule descriptions
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")
        
        try:
            kwargs = {}
            if rule_names:
                kwargs["RuleNames"] = rule_names
            
            response = self.cloudwatch.describe_insight_rules(**kwargs)
            return response.get("InsightRules", [])
        except ClientError as e:
            logger.error(f"Failed to describe insight rules: {e}")
            raise
    
    def delete_insight_rule(self, rule_name: str) -> Dict[str, Any]:
        """
        Delete a CloudWatch Contributor Insights rule.
        
        Args:
            rule_name: Rule name
        
        Returns:
            dict: CloudWatch response
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")
        
        try:
            response = self.cloudwatch.delete_insight_rules(Names=[rule_name])
            logger.info(f"Deleted insight rule: {rule_name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to delete insight rule: {e}")
            raise
    
    def get_insight_rule_report(self, rule_name: str) -> Dict[str, Any]:
        """
        Get report data for a Contributor Insights rule.
        
        Args:
            rule_name: Rule name
        
        Returns:
            dict: Insight rule report with top contributors
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")
        
        try:
            response = self.cloudwatch.get_insight_rule_report(RuleName=rule_name)
            return response
        except ClientError as e:
            logger.error(f"Failed to get insight rule report: {e}")
            raise
    
    # =========================================================================
    # SERVICE LEVEL INDICATORS
    # =========================================================================
    
    def create_sli_alarm(
        self,
        sli: ServiceLevelIndicator,
        alarm_actions: List[str] = None,
        ok_actions: List[str] = None
    ) -> str:
        """
        Create an alarm for a service level indicator.
        
        Args:
            sli: ServiceLevelIndicator configuration
            alarm_actions: Actions to take when alarm triggers
            ok_actions: Actions to take when alarm clears
        
        Returns:
            str: Alarm name
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")
        
        alarm_name = f"{sli.name}-sli-alarm"
        
        # Determine comparison operator based on SLI type
        if sli.sli_type == SLIType.AVAILABILITY:
            # For availability, we want to alarm when below target
            comparison_operator = "LessThanThreshold"
        elif sli.sli_type == SLIType.LATENCY:
            # For latency, we want to alarm when above target
            comparison_operator = "GreaterThanThreshold"
        elif sli.sli_type == SLIType.ERROR_RATE:
            # For error rate, we want to alarm when above target
            comparison_operator = "GreaterThanThreshold"
        else:
            comparison_operator = "LessThanThreshold"
        
        config = AlarmConfig(
            alarm_name=alarm_name,
            metric_name=sli.metric_name,
            namespace=sli.namespace,
            threshold=sli.target,
            comparison_operator=comparison_operator,
            period=sli.period,
            evaluation_periods=1,
            statistic="Average",
            alarm_actions=alarm_actions or [],
            ok_actions=ok_actions or [],
            dimensions=sli.dimensions,
            treat_missing_data="breach",
        )
        
        self.put_metric_alarm(config)
        logger.info(f"Created SLI alarm: {alarm_name}")
        return alarm_name
    
    def calculate_sli(
        self,
        sli: ServiceLevelIndicator,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """
        Calculate service level indicator value.
        
        Args:
            sli: ServiceLevelIndicator configuration
            start_time: Start time for calculation
            end_time: End time for calculation
        
        Returns:
            dict: Calculated SLI value and status
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")
        
        try:
            dimensions = None
            if sli.dimensions:
                dimensions = [{"Name": k, "Value": v} for k, v in sli.dimensions.items()]
            
            stats = self.get_metric_statistics(
                namespace=sli.namespace,
                metric_name=sli.metric_name,
                start_time=start_time,
                end_time=end_time,
                period=sli.period,
                statistics=["Average"],
                dimensions=dimensions
            )
            
            if not stats.get("Datapoints"):
                return {"status": "no_data", "value": None}
            
            value = stats["Datapoints"][0].get("Average", 0)
            
            if sli.sli_type == SLIType.AVAILABILITY:
                status = "passing" if value >= sli.target else "failing"
            elif sli.sli_type == SLIType.LATENCY:
                status = "passing" if value <= sli.target else "failing"
            elif sli.sli_type == SLIType.ERROR_RATE:
                status = "passing" if value <= sli.target else "failing"
            else:
                status = "unknown"
            
            return {
                "status": status,
                "value": value,
                "target": sli.target,
                "sli_type": sli.sli_type.value
            }
        except ClientError as e:
            logger.error(f"Failed to calculate SLI: {e}")
            raise
    
    def create_slo(
        self,
        name: str,
        sli: ServiceLevelIndicator,
        target: float,
        period_days: int = 30,
        budget_days: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Create a service level objective (SLO).
        
        Args:
            name: SLO name
            sli: Underlying service level indicator
            target: SLO target (e.g., 0.999 for 99.9%)
            period_days: SLO evaluation period in days
            budget_days: Optional budget period in days
        
        Returns:
            dict: SLO configuration with associated alarm
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")
        
        slo_alarm_name = self.create_sli_alarm(sli)
        
        return {
            "name": name,
            "sli_type": sli.sli_type.value,
            "target": target,
            "period_days": period_days,
            "alarm_name": slo_alarm_name,
            "namespace": sli.namespace,
            "metric_name": sli.metric_name
        }
    
    # =========================================================================
    # EMBEDDED METRICS
    # =========================================================================
    
    def generate_embedded_metric(
        self,
        spec: EmbeddedMetricSpec,
        timestamp: datetime,
        value: float,
        unit: str = "None"
    ) -> Dict[str, Any]:
        """
        Generate an embedded metric format JSON object.
        
        Args:
            spec: EmbeddedMetricSpec configuration
            timestamp: Metric timestamp
            value: Metric value
            unit: Metric unit
        
        Returns:
            dict: Embedded metric format JSON
        """
        metric = {
            "MetricName": spec.metric_name,
            "Timestamp": timestamp.isoformat(),
            "Value": value,
            "Unit": unit,
        }
        
        embedded_metrics = {
            spec.metric_name: [value]
        }
        
        embedded_metric_format = {
            "_aws": {
                "Timestamp": int(timestamp.timestamp() * 1000),
                "CloudWatchMetrics": [
                    {
                        "Dimensions": [spec.dimensions],
                        "Namespace": spec.namespace,
                        "Metrics": [
                            {
                                "MetricName": m["MetricName"],
                                "Unit": m.get("Unit", "None")
                            }
                            for m in spec.metrics
                        ]
                    }
                ]
            },
            **{
                m["MetricName"]: [value] for m in spec.metrics
            }
        }
        
        return embedded_metric_format
    
    def parse_embedded_metric(self, json_str: str) -> Dict[str, Any]:
        """
        Parse an embedded metric format JSON string.
        
        Args:
            json_str: Embedded metric JSON string
        
        Returns:
            dict: Parsed metric data
        """
        try:
            data = json.loads(json_str)
            
            aws_metadata = data.get("_aws", {})
            cloudwatch_metrics = aws_metadata.get("CloudWatchMetrics", [])
            
            if not cloudwatch_metrics:
                raise ValueError("Invalid embedded metric format: missing CloudWatchMetrics")
            
            metrics_info = cloudwatch_metrics[0]
            namespace = metrics_info.get("Namespace")
            dimensions = metrics_info.get("Dimensions", [])
            metrics = metrics_info.get("Metrics", [])
            
            parsed_metrics = []
            for metric_def in metrics:
                metric_name = metric_def.get("MetricName")
                if metric_name in data:
                    parsed_metrics.append({
                        "metric_name": metric_name,
                        "value": data[metric_name][0],
                        "unit": metric_def.get("Unit", "None")
                    })
            
            return {
                "timestamp": datetime.fromtimestamp(aws_metadata.get("Timestamp", 0) / 1000),
                "namespace": namespace,
                "dimensions": dimensions,
                "metrics": parsed_metrics
            }
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse embedded metric: {e}")
            raise
    
    # =========================================================================
    # APPLICATION SIGNALS (ECS/EKS)
    # =========================================================================
    
    def enable_application_signals(
        self,
        cluster_name: str,
        service_name: str,
        platform: str = "ecs"
    ) -> Dict[str, Any]:
        """
        Enable CloudWatch Application Signals for ECS/EKS.
        
        Args:
            cluster_name: ECS cluster or EKS cluster name
            service_name: ECS service or EKS service name
            platform: Platform type ("ecs" or "eks")
        
        Returns:
            dict: Configuration for enabling Application Signals
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")
        
        if platform.lower() == "ecs":
            config = self._get_ecs_application_signals_config(cluster_name, service_name)
        else:
            config = self._get_eks_application_signals_config(cluster_name, service_name)
        
        logger.info(f"Application Signals enabled for {platform} service: {service_name}")
        return config
    
    def _get_ecs_application_signals_config(
        self,
        cluster_name: str,
        service_name: str
    ) -> Dict[str, Any]:
        """Generate ECS Application Signals configuration."""
        return {
            "cluster_name": cluster_name,
            "service_name": service_name,
            "platform": "ecs",
            "telemetry": {
                "metrics": {
                    "namespace": "AWS/ApplicationSignals",
                    "metrics_collection_level": "Enhanced",
                    "metrics_interval": 60
                },
                "traces": {
                    "mode": "ACTIVEGATEWAY",
                    "sample_rate": 1.0
                }
            },
            "logging": {
                "log_group": f"/ecs/application-signals/{cluster_name}/{service_name}",
                "retention_days": 7
            }
        }
    
    def _get_eks_application_signals_config(
        self,
        cluster_name: str,
        service_name: str
    ) -> Dict[str, Any]:
        """Generate EKS Application Signals configuration."""
        return {
            "cluster_name": cluster_name,
            "service_name": service_name,
            "platform": "eks",
            "telemetry": {
                "metrics": {
                    "namespace": "AWS/ApplicationSignals",
                    "metrics_collection_level": "Enhanced",
                    "metrics_interval": 60
                },
                "traces": {
                    "mode": "ACTIVEGATEWAY",
                    "sample_rate": 1.0
                }
            },
            "admission": {
                "webhook": f"application-signals.{cluster_name}.svc",
                "ports": [9443]
            },
            "logging": {
                "log_group": f"/eks/application-signals/{cluster_name}/{service_name}",
                "retention_days": 7
            }
        }
    
    def get_application_signals_metrics(
        self,
        cluster_name: str,
        service_name: str,
        start_time: datetime,
        end_time: datetime,
        platform: str = "ecs"
    ) -> Dict[str, Any]:
        """
        Get Application Signals metrics for ECS/EKS service.
        
        Args:
            cluster_name: ECS/EKS cluster name
            service_name: ECS/EKS service name
            start_time: Start time
            end_time: End time
            platform: Platform type ("ecs" or "eks")
        
        Returns:
            dict: Application Signals metrics
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")
        
        namespace = "AWS/ApplicationSignals"
        dimensions = [
            {"Name": "ClusterName", "Value": cluster_name},
            {"Name": "ServiceName", "Value": service_name}
        ]
        
        # Common SLI metrics
        sli_metrics = {
            "Availability": ["CallSuccessful", "CallCount"],
            "Latency": ["CallLatency"],
            "Faults": ["CallFaulted", "CallErrored"],
        }
        
        all_metrics = {}
        for sli_name, metric_names in sli_metrics.items():
            for metric_name in metric_names:
                try:
                    stats = self.get_metric_statistics(
                        namespace=namespace,
                        metric_name=metric_name,
                        start_time=start_time,
                        end_time=end_time,
                        period=60,
                        statistics=["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
                        dimensions=dimensions
                    )
                    all_metrics[f"{sli_name}_{metric_name}"] = stats
                except Exception as e:
                    logger.warning(f"Failed to get metric {metric_name}: {e}")
        
        return all_metrics
    
    def create_application_signals_dashboard(
        self,
        cluster_name: str,
        service_name: str,
        dashboard_name: str,
        platform: str = "ecs"
    ) -> Dict[str, Any]:
        """
        Create an Application Signals dashboard for ECS/EKS service.
        
        Args:
            cluster_name: ECS/EKS cluster name
            service_name: ECS/EKS service name
            dashboard_name: Name for the dashboard
            platform: Platform type ("ecs" or "eks")
        
        Returns:
            dict: CloudWatch response
        """
        namespace = "AWS/ApplicationSignals"
        dimensions = [
            {"Name": "ClusterName", "Value": cluster_name},
            {"Name": "ServiceName", "Value": service_name}
        ]
        
        widgets = [
            DashboardWidget(
                widget_type="metric",
                title=f"{service_name} Availability",
                width=12,
                height=6,
                properties={
                    "metrics": [
                        [namespace, "CallSuccessful", "ClusterName", cluster_name, "ServiceName", service_name],
                        [".", "CallCount", ".", ".", ".", "."]
                    ],
                    "period": 60,
                    "stat": "Sum",
                    "region": self.region_name,
                    "title": f"{service_name} Availability"
                }
            ),
            DashboardWidget(
                widget_type="metric",
                title=f"{service_name} Latency",
                width=12,
                height=6,
                properties={
                    "metrics": [
                        [namespace, "CallLatency", "ClusterName", cluster_name, "ServiceName", service_name]
                    ],
                    "period": 60,
                    "stat": "p99",
                    "region": self.region_name,
                    "title": f"{service_name} Latency (p99)"
                }
            ),
            DashboardWidget(
                widget_type="metric",
                title=f"{service_name} Fault Rate",
                width=12,
                height=6,
                properties={
                    "metrics": [
                        [namespace, "CallFaulted", "ClusterName", cluster_name, "ServiceName", service_name],
                        [".", "CallCount", ".", ".", ".", "."]
                    ],
                    "period": 60,
                    "stat": "Sum",
                    "region": self.region_name,
                    "title": f"{service_name} Fault Rate"
                }
            ),
        ]
        
        return self.create_dashboard(dashboard_name, widgets)
    
    # =========================================================================
    # UTILITY METHODS
    # =========================================================================
    
    def wait_for_alarm(
        self,
        alarm_name: str,
        expected_state: str,
        timeout: int = 300,
        poll_interval: int = 5
    ) -> bool:
        """
        Wait for an alarm to reach a specific state.
        
        Args:
            alarm_name: Alarm name
            expected_state: Expected state (OK, ALARM, INSUFFICIENT_DATA)
            timeout: Maximum wait time in seconds
            poll_interval: Poll interval in seconds
        
        Returns:
            bool: True if alarm reached expected state, False if timeout
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            alarms = self.describe_alarms(alarm_names=[alarm_name])
            if alarms:
                state = alarms[0].get("StateValue")
                if state == expected_state:
                    return True
            time.sleep(poll_interval)
        
        return False
    
    def get_metric_summary(
        self,
        namespace: str,
        metric_name: str,
        start_time: datetime,
        end_time: datetime,
        dimensions: Dict[str, str] = None
    ) -> Dict[str, Any]:
        """
        Get a summary of metric statistics.
        
        Args:
            namespace: Metric namespace
            metric_name: Metric name
            start_time: Start time
            end_time: End time
            dimensions: Metric dimensions
        
        Returns:
            dict: Summary with all statistics
        """
        dim_list = None
        if dimensions:
            dim_list = [{"Name": k, "Value": v} for k, v in dimensions.items()]
        
        stats = self.get_metric_statistics(
            namespace=namespace,
            metric_name=metric_name,
            start_time=start_time,
            end_time=end_time,
            period=60,
            statistics=["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
            dimensions=dim_list
        )
        
        if not stats.get("Datapoints"):
            return {"status": "no_data"}
        
        values = [dp["Average"] for dp in stats["Datapoints"]]
        
        return {
            "namespace": namespace,
            "metric_name": metric_name,
            "sample_count": stats["Datapoints"][0].get("SampleCount"),
            "average": sum(values) / len(values) if values else None,
            "min": min(values) if values else None,
            "max": max(values) if values else None,
            "sum": sum(values) if values else None,
            "data_points": len(stats["Datapoints"])
        }
    
    def close(self):
        """Close all client connections."""
        with self._lock:
            self._clients.clear()
            self._resources.clear()
        logger.info("CloudWatch integration closed")
