"""
AWS Kinesis Data Streaming Integration Module for Workflow System

Implements a KinesisIntegration class with:
1. Stream management: Create/manage Kinesis data streams
2. Shard management: Manage shards and iterators
3. Data operations: Put/get records
4. Enhanced fan-out: Enhanced consumer support
5. Analytics: Kinesis Data Analytics
6. Firehose: Kinesis Data Firehose
7. Video streams: Kinesis Video Streams
8. Streams for Lambda: Event source mapping
9. Metrics: CloudWatch metrics
10. Encryption: Server-side encryption

Commit: 'feat(aws-kinesis): add AWS Kinesis with stream management, shards, data operations, enhanced fan-out, Data Analytics, Firehose, Video Streams, Lambda integration, CloudWatch, encryption'
"""

import uuid
import json
import threading
import time
import logging
import hashlib
import base64
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional, Set, Union, Iterator
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum

try:
    import boto3
    from botocore.exceptions import (
        ClientError,
        BotoCoreError
    )
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    boto3 = None


logger = logging.getLogger(__name__)


class StreamStatus(Enum):
    """Kinesis stream statuses."""
    CREATING = "CREATING"
    DELETING = "DELETING"
    ACTIVE = "ACTIVE"
    UPDATING = "UPDATING"


class ShardIteratorType(Enum):
    """Shard iterator types."""
    AT_SEQUENCE_NUMBER = "AT_SEQUENCE_NUMBER"
    AFTER_SEQUENCE_NUMBER = "AFTER_SEQUENCE_NUMBER"
    AT_TIMESTAMP = "AT_TIMESTAMP"
    TRIM_HORIZON = "TRIM_HORIZON"
    LATEST = "LATEST"


class EncryptionType(Enum):
    """Kinesis encryption types."""
    NONE = "NONE"
    KMS = "KMS"


@dataclass
class StreamConfig:
    """Configuration for a Kinesis data stream."""
    name: str
    shard_count: int = 1
    retention_period_hours: int = 24
    stream_mode: str = "PROVISIONED"  # PROVISIONED or ON_DEMAND
    stream_arn: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    kms_key_id: Optional[str] = None
    encryption_type: EncryptionType = EncryptionType.NONE


@dataclass
class ShardConfig:
    """Configuration for a shard."""
    stream_name: str
    shard_id: str


@dataclass
class RecordConfig:
    """Configuration for putting a record."""
    stream_name: str
    data: Union[str, bytes, Dict, Any]
    partition_key: str
    explicit_hash_key: Optional[str] = None
    sequence_number_for_ordering: Optional[str] = None


@dataclass
class ConsumerConfig:
    """Configuration for a Kinesis consumer."""
    stream_name: str
    consumer_name: str
    consumer_arn: Optional[str] = None
    consumer_status: Optional[str] = None
    consumer_creation_timestamp: Optional[datetime] = None


@dataclass
class FirehoseConfig:
    """Configuration for a Kinesis Data Firehose delivery stream."""
    name: str
    delivery_stream_type: str = "DirectPut"  # DirectPut or KinesisStreamAsSource
    kinesis_stream_arn: Optional[str] = None
    s3_destination_arn: Optional[str] = None
    role_arn: Optional[str] = None
    buffer_size: int = 5  # MB
    buffer_interval: int = 300  # seconds
    compression_format: str = "UNCOMPRESSED"  # GZIP, ZIP, SNAPPY
    encryption_configuration: Dict[str, Any] = field(default_factory=dict)
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class AnalyticsConfig:
    """Configuration for Kinesis Data Analytics."""
    name: str
    runtime_environment: str = "FLINK-1_11"  # FLINK-1_11 or ZEPPLIN-0_8
    service_execution_role_arn: Optional[str] = None
    application_code: Optional[str] = None
    input_configs: List[Dict[str, Any]] = field(default_factory=list)
    output_configs: List[Dict[str, Any]] = field(default_factory=list)
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class VideoStreamConfig:
    """Configuration for Kinesis Video Streams."""
    name: str
    data_retention_in_hours: int = 24
    media_type: Optional[str] = None
    kms_key_id: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class LambdaEventSourceMappingConfig:
    """Configuration for Lambda event source mapping."""
    function_name: str
    stream_arn: str
    batch_size: int = 100
    max_batch_duration: int = 0
    parallelization_factor: int = 1
    starting_position: str = "TRIM_HORIZON"  # TRIM_HORIZON, LATEST, AT_TIMESTAMP
    starting_timestamp: Optional[datetime] = None
    destination_config: Dict[str, Any] = field(default_factory=dict)
    filter_criteria: List[Dict[str, Any]] = field(default_factory=list)
    bisect_batch_on_function_error: bool = False
    maximum_retry_attempts: int = -1  # -1 means unlimited
    tumbling_window_in_seconds: int = 0
    maximum_record_age_in_seconds: int = -1
    enable: bool = True


class KinesisIntegration:
    """
    AWS Kinesis integration class for data streaming operations.
    
    Supports:
    - Data stream management (create, describe, delete)
    - Shard management and iterator operations
    - Record put/get operations
    - Enhanced fan-out for consumers
    - Kinesis Data Analytics (Flink-based)
    - Kinesis Data Firehose delivery streams
    - Kinesis Video Streams
    - Lambda event source mapping
    - CloudWatch metrics integration
    - Server-side encryption
    """
    
    def __init__(
        self,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: str = "us-east-1",
        endpoint_url: Optional[str] = None,
        kinesis_client: Optional[Any] = None,
        firehose_client: Optional[Any] = None,
        analytics_client: Optional[Any] = None,
        video_client: Optional[Any] = None,
        lambda_client: Optional[Any] = None,
        cloudwatch_client: Optional[Any] = None,
        iam_client: Optional[Any] = None
    ):
        """
        Initialize Kinesis integration.
        
        Args:
            aws_access_key_id: AWS access key ID (uses boto3 credentials if None)
            aws_secret_access_key: AWS secret access key (uses boto3 credentials if None)
            region_name: AWS region name
            endpoint_url: Kinesis endpoint URL (for testing with LocalStack, etc.)
            kinesis_client: Pre-configured Kinesis client
            firehose_client: Pre-configured Firehose client
            analytics_client: Pre-configured Kinesis Analytics client
            video_client: Pre-configured Kinesis Video Streams client
            lambda_client: Pre-configured Lambda client
            cloudwatch_client: Pre-configured CloudWatch client
            iam_client: Pre-configured IAM client
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for Kinesis integration. Install with: pip install boto3")
        
        self.region_name = region_name
        self.endpoint_url = endpoint_url
        self._kinesis_client = kinesis_client
        self._firehose_client = firehose_client
        self._analytics_client = analytics_client
        self._video_client = video_client
        self._lambda_client = lambda_client
        self._cloudwatch_client = cloudwatch_client
        self._iam_client = iam_client
        self._cloudwatch_namespace = "Kinesis/Integration"
        
        self._stream_cache: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        
        session_kwargs = {"region_name": region_name}
        if aws_access_key_id and aws_secret_access_key:
            session_kwargs["aws_access_key_id"] = aws_access_key_id
            session_kwargs["aws_secret_access_key"] = aws_secret_access_key
        
        self._session = boto3.Session(**session_kwargs)
        
        self._metrics_buffer: List[Dict[str, Any]] = []
        self._metrics_lock = threading.Lock()
    
    @property
    def kinesis_client(self):
        """Get or create Kinesis client."""
        if self._kinesis_client is None:
            kwargs = {"region_name": self.region_name}
            if self.endpoint_url:
                kwargs["endpoint_url"] = self.endpoint_url
            self._kinesis_client = self._session.client("kinesis", **kwargs)
        return self._kinesis_client
    
    @property
    def firehose_client(self):
        """Get or create Firehose client."""
        if self._firehose_client is None:
            kwargs = {"region_name": self.region_name}
            if self.endpoint_url:
                kwargs["endpoint_url"] = self.endpoint_url
            self._firehose_client = self._session.client("firehose", **kwargs)
        return self._firehose_client
    
    @property
    def analytics_client(self):
        """Get or create Kinesis Analytics client."""
        if self._analytics_client is None:
            kwargs = {"region_name": self.region_name}
            if self.endpoint_url:
                kwargs["endpoint_url"] = self.endpoint_url
            self._analytics_client = self._session.client("kinesisanalytics", **kwargs)
        return self._analytics_client
    
    @property
    def video_client(self):
        """Get or create Kinesis Video Streams client."""
        if self._video_client is None:
            kwargs = {"region_name": self.region_name}
            if self.endpoint_url:
                kwargs["endpoint_url"] = self.endpoint_url
            self._video_client = self._session.client("kinesisvideo", **kwargs)
        return self._video_client
    
    @property
    def lambda_client(self):
        """Get or create Lambda client."""
        if self._lambda_client is None:
            kwargs = {"region_name": self.region_name}
            if self.endpoint_url:
                kwargs["endpoint_url"] = self.endpoint_url
            self._lambda_client = self._session.client("lambda", **kwargs)
        return self._lambda_client
    
    @property
    def cloudwatch_client(self):
        """Get or create CloudWatch client."""
        if self._cloudwatch_client is None:
            kwargs = {"region_name": self.region_name}
            if self.endpoint_url:
                kwargs["endpoint_url"] = self.endpoint_url
            self._cloudwatch_client = self._session.client("cloudwatch", **kwargs)
        return self._cloudwatch_client
    
    @property
    def iam_client(self):
        """Get or create IAM client."""
        if self._iam_client is None:
            kwargs = {"region_name": self.region_name}
            if self.endpoint_url:
                kwargs["endpoint_url"] = self.endpoint_url
            self._iam_client = self._session.client("iam", **kwargs)
        return self._iam_client
    
    def _record_metric(self, metric_name: str, value: float, unit: str = "Count", dimensions: Dict[str, str] = None):
        """Record a metric for CloudWatch."""
        metric = {
            "MetricName": metric_name,
            "Value": value,
            "Unit": unit,
            "Timestamp": datetime.utcnow().isoformat()
        }
        if dimensions:
            metric["Dimensions"] = [{"Name": k, "Value": v} for k, v in dimensions.items()]
        
        with self._metrics_lock:
            self._metrics_buffer.append(metric)
    
    def flush_metrics(self):
        """Flush buffered metrics to CloudWatch."""
        with self._metrics_lock:
            if not self._metrics_buffer:
                return
            
            try:
                self.cloudwatch_client.put_metric_data(
                    Namespace=self._cloudwatch_namespace,
                    MetricData=self._metrics_buffer
                )
                logger.info(f"Flushed {len(self._metrics_buffer)} metrics to CloudWatch")
                self._metrics_buffer.clear()
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to flush metrics to CloudWatch: {e}")
    
    def _serialize_data(self, data: Union[str, bytes, Dict, Any]) -> bytes:
        """Serialize data to bytes."""
        if isinstance(data, bytes):
            return data
        elif isinstance(data, dict):
            return json.dumps(data).encode("utf-8")
        elif isinstance(data, str):
            return data.encode("utf-8")
        else:
            return str(data).encode("utf-8")
    
    # ========================================================================
    # STREAM MANAGEMENT
    # ========================================================================
    
    def create_stream(self, config: StreamConfig) -> Dict[str, Any]:
        """
        Create a Kinesis data stream.
        
        Args:
            config: Stream configuration
            
        Returns:
            Response from create_stream API
        """
        with self._lock:
            try:
                kwargs = {
                    "StreamName": config.name,
                    "ShardCount": config.shard_count
                }
                
                if config.kms_key_id and config.encryption_type == EncryptionType.KMS:
                    kwargs["StreamEncryption"] = {
                        "EncryptionType": "KMS",
                        "KeyId": config.kms_key_id
                    }
                
                if config.stream_mode == "ON_DEMAND":
                    kwargs["StreamModeDetails"] = {"StreamMode": "ON_DEMAND"}
                
                response = self.kinesis_client.create_stream(**kwargs)
                
                self._stream_cache[config.name] = {
                    "StreamName": config.name,
                    "StreamARN": f"arn:aws:kinesis:{self.region_name}:*:stream/{config.name}",
                    "StreamStatus": StreamStatus.CREATING.value
                }
                
                if config.tags:
                    try:
                        self.kinesis_client.tag_resource(
                            ResourceARN=self._stream_cache[config.name]["StreamARN"],
                            Tags=[{"Key": k, "Value": v} for k, v in config.tags.items()]
                        )
                    except Exception as e:
                        logger.warning(f"Failed to tag stream: {e}")
                
                self._record_metric("StreamCreated", 1, "Count", {"StreamName": config.name})
                logger.info(f"Created Kinesis stream: {config.name}")
                return response
                
            except ClientError as e:
                logger.error(f"Failed to create stream {config.name}: {e}")
                raise
    
    def describe_stream(self, stream_name: str, force_refresh: bool = False) -> Dict[str, Any]:
        """
        Describe a Kinesis stream.
        
        Args:
            stream_name: Name of the stream
            force_refresh: Force refresh from API
            
        Returns:
            Stream description
        """
        with self._lock:
            if not force_refresh and stream_name in self._stream_cache:
                return self._stream_cache[stream_name]
            
            try:
                response = self.kinesis_client.describe_stream(StreamName=stream_name)
                stream_info = response["StreamDescription"]
                
                self._stream_cache[stream_name] = {
                    "StreamName": stream_info["StreamName"],
                    "StreamARN": stream_info["StreamARN"],
                    "StreamStatus": stream_info["StreamStatus"],
                    "Shards": [
                        {
                            "ShardId": shard["ShardId"],
                            "ParentShardId": shard.get("ParentShardId"),
                            "AdjacentParentShardId": shard.get("AdjacentParentShardId"),
                            "HashKeyRange": shard["HashKeyRange"],
                            "SequenceNumberRange": shard["SequenceNumberRange"]
                        }
                        for shard in stream_info.get("Shards", [])
                    ]
                }
                
                return self._stream_cache[stream_name]
                
            except ClientError as e:
                logger.error(f"Failed to describe stream {stream_name}: {e}")
                raise
    
    def list_streams(self) -> List[str]:
        """
        List all Kinesis streams.
        
        Returns:
            List of stream names
        """
        try:
            streams = []
            paginator = self.kinesis_client.get_paginator("list_streams")
            
            for page in paginator.paginate():
                streams.extend(page.get("StreamNames", []))
            
            self._record_metric("StreamsListed", len(streams), "Count")
            return streams
            
        except ClientError as e:
            logger.error(f"Failed to list streams: {e}")
            raise
    
    def delete_stream(self, stream_name: str, enforce_consumer_deletion: bool = False) -> Dict[str, Any]:
        """
        Delete a Kinesis stream.
        
        Args:
            stream_name: Name of the stream to delete
            enforce_consumer_deletion: Delete associated consumers first
            
        Returns:
            Response from delete_stream API
        """
        with self._lock:
            try:
                if enforce_consumer_deletion:
                    consumers = self.list_consumers(stream_name)
                    for consumer in consumers:
                        self.deregister_consumer(consumer["ConsumerName"], stream_name)
                
                response = self.kinesis_client.delete_stream(StreamName=stream_name)
                
                if stream_name in self._stream_cache:
                    del self._stream_cache[stream_name]
                
                self._record_metric("StreamDeleted", 1, "Count", {"StreamName": stream_name})
                logger.info(f"Deleted Kinesis stream: {stream_name}")
                return response
                
            except ClientError as e:
                logger.error(f"Failed to delete stream {stream_name}: {e}")
                raise
    
    def update_stream_mode(self, stream_name: str, stream_mode: str) -> Dict[str, Any]:
        """
        Update stream mode (PROVISIONED to ON_DEMAND or vice versa).
        
        Args:
            stream_name: Name of the stream
            stream_mode: NEW_STREAM_MODE (PROVISIONED or ON_DEMAND)
            
        Returns:
            Response from update_stream_mode API
        """
        try:
            response = self.kinesis_client.update_stream_mode(
                StreamName=stream_name,
                StreamModeDetails={"StreamMode": stream_mode}
            )
            
            if stream_name in self._stream_cache:
                self._stream_cache[stream_name]["StreamMode"] = stream_mode
            
            self._record_metric("StreamModeUpdated", 1, "Count", {"StreamName": stream_name})
            logger.info(f"Updated stream mode for {stream_name} to {stream_mode}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to update stream mode for {stream_name}: {e}")
            raise
    
    def wait_for_stream_active(self, stream_name: str, timeout: int = 600) -> bool:
        """
        Wait for a stream to become active.
        
        Args:
            stream_name: Name of the stream
            timeout: Maximum seconds to wait
            
        Returns:
            True if stream is active, False if timeout
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            description = self.describe_stream(stream_name, force_refresh=True)
            status = description.get("StreamStatus", description.get("StreamDescription", {}).get("StreamStatus"))
            
            if status == StreamStatus.ACTIVE.value:
                return True
            
            time.sleep(5)
        
        logger.warning(f"Timeout waiting for stream {stream_name} to become active")
        return False
    
    # ========================================================================
    # SHARD MANAGEMENT
    # ========================================================================
    
    def get_shards(self, stream_name: str, force_refresh: bool = False) -> List[Dict[str, Any]]:
        """
        Get all shards for a stream.
        
        Args:
            stream_name: Name of the stream
            force_refresh: Force refresh from API
            
        Returns:
            List of shard information
        """
        description = self.describe_stream(stream_name, force_refresh=force_refresh)
        return description.get("Shards", description.get("StreamDescription", {}).get("Shards", []))
    
    def get_shard_iterator(
        self,
        stream_name: str,
        shard_id: str,
        iterator_type: ShardIteratorType = ShardIteratorType.TRIM_HORIZON,
        starting_sequence_number: Optional[str] = None,
        timestamp: Optional[datetime] = None
    ) -> str:
        """
        Get a shard iterator.
        
        Args:
            stream_name: Name of the stream
            shard_id: ID of the shard
            iterator_type: Type of shard iterator
            starting_sequence_number: Required for AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER
            timestamp: Required for AT_TIMESTAMP
            
        Returns:
            Shard iterator
        """
        try:
            kwargs = {
                "StreamName": stream_name,
                "ShardId": shard_id,
                "ShardIteratorType": iterator_type.value
            }
            
            if starting_sequence_number:
                kwargs["StartingSequenceNumber"] = starting_sequence_number
            
            if timestamp:
                kwargs["Timestamp"] = timestamp.isoformat()
            
            response = self.kinesis_client.get_shard_iterator(**kwargs)
            iterator = response["ShardIterator"]
            
            self._record_metric("ShardIteratorAcquired", 1, "Count", {"StreamName": stream_name})
            return iterator
            
        except ClientError as e:
            logger.error(f"Failed to get shard iterator: {e}")
            raise
    
    def split_shard(
        self,
        stream_name: str,
        shard_id: str,
        starting_hash_key: str
    ) -> Dict[str, Any]:
        """
        Split a shard into two new shards.
        
        Args:
            stream_name: Name of the stream
            shard_id: ID of the shard to split
            starting_hash_key: Hash key to use for the split
            
        Returns:
            Response from split_shard API
        """
        try:
            response = self.kinesis_client.split_shard(
                StreamName=stream_name,
                ShardToSplit=shard_id,
                NewStartingHashKey=starting_hash_key
            )
            
            self.describe_stream(stream_name, force_refresh=True)
            self._record_metric("ShardSplit", 1, "Count", {"StreamName": stream_name})
            logger.info(f"Split shard {shard_id} in stream {stream_name}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to split shard: {e}")
            raise
    
    def mergeate_shards(
        self,
        stream_name: str,
        shard_id: str,
        adjacent_shard_id: str
    ) -> Dict[str, Any]:
        """
        Merge two adjacent shards.
        
        Args:
            stream_name: Name of the stream
            shard_id: ID of the shard to merge
            adjacent_shard_id: ID of the adjacent shard to merge
            
        Returns:
            Response from mergeate_shards API
        """
        try:
            response = self.kinesis_client.mergeate_shards(
                StreamName=stream_name,
                ShardToMerge=shard_id,
                AdjacentShardToMerge=adjacent_shard_id
            )
            
            self.describe_stream(stream_name, force_refresh=True)
            self._record_metric("ShardMerged", 1, "Count", {"StreamName": stream_name})
            logger.info(f"Merged shards {shard_id} and {adjacent_shard_id} in stream {stream_name}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to merge shards: {e}")
            raise
    
    def reshard_stream(
        self,
        stream_name: str,
        target_shard_count: int
    ) -> List[Dict[str, Any]]:
        """
        Reshard a stream to a new shard count.
        
        Args:
            stream_name: Name of the stream
            target_shard_count: Target number of shards
            
        Returns:
            List of split/merge operations performed
        """
        current_shards = self.get_shards(stream_name, force_refresh=True)
        current_count = len(current_shards)
        operations = []
        
        if target_shard_count > current_count:
            needed_splits = target_shard_count - current_count
            
            for i, shard in enumerate(current_shards[:needed_splits]):
                hash_range = shard["HashKeyRange"]
                mid_key = str((int(hash_range["StartingHashKey"]) + int(hash_range["EndingHashKey"])) // 2)
                
                try:
                    op = self.split_shard(stream_name, shard["ShardId"], mid_key)
                    operations.append({"type": "split", "shard_id": shard["ShardId"], "result": op})
                except ClientError as e:
                    logger.error(f"Failed to split shard {shard['ShardId']}: {e}")
        
        elif target_shard_count < current_count:
            needed_merges = current_count - target_shard_count
            
            for i in range(needed_merges):
                if i * 2 + 1 < len(current_shards):
                    shard = current_shards[i * 2]
                    adjacent = current_shards[i * 2 + 1]
                    
                    try:
                        op = self.mergeate_shards(stream_name, shard["ShardId"], adjacent["ShardId"])
                        operations.append({"type": "merge", "shard_ids": [shard["ShardId"], adjacent["ShardId"]], "result": op})
                    except ClientError as e:
                        logger.error(f"Failed to merge shards: {e}")
        
        self._record_metric("StreamResharded", len(operations), "Count", {"StreamName": stream_name})
        return operations
    
    # ========================================================================
    # DATA OPERATIONS
    # ========================================================================
    
    def put_record(
        self,
        stream_name: str,
        data: Union[str, bytes, Dict, Any],
        partition_key: str,
        explicit_hash_key: Optional[str] = None,
        sequence_number_for_ordering: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Put a record into a Kinesis stream.
        
        Args:
            stream_name: Name of the stream
            data: Data to put (will be serialized)
            partition_key: Partition key for routing
            explicit_hash_key: Explicit hash key (overrides partition key routing)
            sequence_number_for_ordering: Sequence number for ordering
            
        Returns:
            PutRecord response with SequenceNumber and ShardId
        """
        try:
            serialized_data = self._serialize_data(data)
            
            kwargs = {
                "StreamName": stream_name,
                "Data": serialized_data,
                "PartitionKey": partition_key
            }
            
            if explicit_hash_key:
                kwargs["ExplicitHashKey"] = explicit_hash_key
            
            if sequence_number_for_ordering:
                kwargs["SequenceNumberForOrdering"] = sequence_number_for_ordering
            
            response = self.kinesis_client.put_record(**kwargs)
            
            self._record_metric("RecordPut", 1, "Count", {"StreamName": stream_name})
            return response
            
        except ClientError as e:
            logger.error(f"Failed to put record to {stream_name}: {e}")
            raise
    
    def put_records(
        self,
        stream_name: str,
        records: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Put multiple records into a Kinesis stream.
        
        Args:
            stream_name: Name of the stream
            records: List of records with 'data' and 'partition_key' keys
            
        Returns:
            PutRecords response with SuccessCount and FailedRecordCount
        """
        try:
            formatted_records = []
            for record in records:
                serialized_data = self._serialize_data(record["data"])
                formatted_records.append({
                    "Data": serialized_data,
                    "PartitionKey": record["partition_key"]
                })
            
            response = self.kinesis_client.put_records(
                StreamName=stream_name,
                Records=formatted_records
            )
            
            success_count = response.get("SuccessCount", 0)
            failed_count = response.get("FailedRecordCount", 0)
            
            self._record_metric("RecordsPut", success_count, "Count", {"StreamName": stream_name})
            if failed_count > 0:
                self._record_metric("RecordsPutFailed", failed_count, "Count", {"StreamName": stream_name})
            
            return response
            
        except ClientError as e:
            logger.error(f"Failed to put records to {stream_name}: {e}")
            raise
    
    def get_records(
        self,
        shard_iterator: str,
        limit: int = 100
    ) -> Dict[str, Any]:
        """
        Get records from a shard iterator.
        
        Args:
            shard_iterator: Shard iterator
            limit: Maximum number of records to retrieve
            
        Returns:
            GetRecords response with Records, NextShardIterator, and MillisBehindLatest
        """
        try:
            response = self.kinesis_client.get_records(
                ShardIterator=shard_iterator,
                Limit=limit
            )
            
            records_count = len(response.get("Records", []))
            self._record_metric("RecordsGet", records_count, "Count")
            
            return response
            
        except ClientError as e:
            logger.error(f"Failed to get records: {e}")
            raise
    
    def read_stream(
        self,
        stream_name: str,
        shard_id: str,
        iterator_type: ShardIteratorType = ShardIteratorType.LATEST,
        max_records: Optional[int] = None,
        callback: Optional[Callable[[List[Dict[str, Any]]], None]] = None
    ) -> List[Dict[str, Any]]:
        """
        Read records from a stream shard.
        
        Args:
            stream_name: Name of the stream
            shard_id: ID of the shard
            iterator_type: Type of shard iterator
            max_records: Maximum number of records to read (None for all)
            callback: Optional callback to process records in batches
            
        Returns:
            List of records
        """
        shard_iterator = self.get_shard_iterator(stream_name, shard_id, iterator_type)
        all_records = []
        records_read = 0
        
        while True:
            response = self.get_records(shard_iterator)
            records = response.get("Records", [])
            
            if records:
                if callback:
                    callback(records)
                else:
                    all_records.extend(records)
                
                records_read += len(records)
                
                if max_records and records_read >= max_records:
                    break
            
            shard_iterator = response.get("NextShardIterator")
            
            if not shard_iterator or response.get("MillisBehindLatest", 0) == 0:
                break
            
            if max_records and records_read >= max_records:
                break
        
        self._record_metric("StreamRead", records_read, "Count", {"StreamName": stream_name})
        return all_records
    
    def get_record_decoder(self, record: Dict[str, Any]) -> Any:
        """
        Decode a record's data field.
        
        Args:
            record: Record from get_records
            
        Returns:
            Decoded data
        """
        if "Data" in record:
            data = record["Data"]
            if isinstance(data, bytes):
                try:
                    return json.loads(data.decode("utf-8"))
                except json.JSONDecodeError:
                    return data.decode("utf-8")
            return data
        return None
    
    # ========================================================================
    # ENHANCED FAN-OUT (CONSUMERS)
    # ========================================================================
    
    def register_consumer(
        self,
        stream_arn: str,
        consumer_name: str
    ) -> ConsumerConfig:
        """
        Register a consumer for enhanced fan-out.
        
        Args:
            stream_arn: ARN of the stream
            consumer_name: Name of the consumer
            
        Returns:
            Consumer configuration
        """
        try:
            response = self.kinesis_client.register_stream_consumer(
                StreamARN=stream_arn,
                ConsumerName=consumer_name
            )
            
            consumer = response["Consumer"]
            
            self._record_metric("ConsumerRegistered", 1, "Count", {"ConsumerName": consumer_name})
            logger.info(f"Registered consumer {consumer_name} for stream {stream_arn}")
            
            return ConsumerConfig(
                stream_name=stream_arn,
                consumer_name=consumer["ConsumerName"],
                consumer_arn=consumer["ConsumerARN"],
                consumer_status=consumer["ConsumerStatus"],
                consumer_creation_timestamp=consumer.get("ConsumerCreationTimestamp")
            )
            
        except ClientError as e:
            logger.error(f"Failed to register consumer: {e}")
            raise
    
    def list_consumers(self, stream_name: str = None, stream_arn: str = None) -> List[Dict[str, Any]]:
        """
        List consumers for a stream.
        
        Args:
            stream_name: Name of the stream
            stream_arn: ARN of the stream
            
        Returns:
            List of consumers
        """
        try:
            kwargs = {}
            if stream_arn:
                kwargs["StreamARN"] = stream_arn
            elif stream_name:
                description = self.describe_stream(stream_name)
                kwargs["StreamARN"] = description["StreamARN"]
            else:
                raise ValueError("Either stream_name or stream_arn must be provided")
            
            consumers = []
            paginator = self.kinesis_client.get_paginator("list_stream_consumers")
            
            for page in paginator.paginate(**kwargs):
                consumers.extend(page.get("Consumers", []))
            
            return consumers
            
        except ClientError as e:
            logger.error(f"Failed to list consumers: {e}")
            raise
    
    def deregister_consumer(
        self,
        consumer_name: str,
        stream_name: str = None,
        stream_arn: str = None
    ) -> Dict[str, Any]:
        """
        Deregister a consumer.
        
        Args:
            consumer_name: Name of the consumer
            stream_name: Name of the stream
            stream_arn: ARN of the stream
            
        Returns:
            Response from deregister_stream_consumer API
        """
        try:
            kwargs = {"ConsumerName": consumer_name}
            
            if stream_arn:
                kwargs["StreamARN"] = stream_arn
            elif stream_name:
                description = self.describe_stream(stream_name)
                kwargs["StreamARN"] = description["StreamARN"]
            else:
                raise ValueError("Either stream_name or stream_arn must be provided")
            
            response = self.kinesis_client.deregister_stream_consumer(**kwargs)
            
            self._record_metric("ConsumerDeregistered", 1, "Count", {"ConsumerName": consumer_name})
            logger.info(f"Deregistered consumer {consumer_name}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to deregister consumer: {e}")
            raise
    
    def subscribe_to_consumer(
        self,
        consumer_arn: str,
        stream_arn: str,
        callback: Callable[[List[Dict[str, Any]]], None],
        read_interval: float = 1.0
    ):
        """
        Subscribe to a consumer with enhanced fan-out.
        
        Args:
            consumer_arn: ARN of the consumer
            stream_arn: ARN of the stream
            callback: Callback function to process records
            read_interval: Interval between reads in seconds
        """
        def read_loop():
            while True:
                try:
                    response = self.kinesis_client.subscribe_to_consumer(
                        ConsumerARN=consumer_arn,
                        StreamARN=stream_arn
                    )
                    
                    for event in response.get("EventStream", []):
                        if "Records" in event:
                            records = event["Records"]
                            if records:
                                callback(records)
                
                except ClientError as e:
                    logger.error(f"Error in consumer subscription: {e}")
                    time.sleep(read_interval)
                except Exception as e:
                    logger.error(f"Unexpected error in consumer subscription: {e}")
                    time.sleep(read_interval)
        
        thread = threading.Thread(target=read_loop, daemon=True)
        thread.start()
        return thread
    
    # ========================================================================
    # KINESIS DATA ANALYTICS
    # ========================================================================
    
    def create_application(
        self,
        config: AnalyticsConfig
    ) -> Dict[str, Any]:
        """
        Create a Kinesis Data Analytics application.
        
        Args:
            config: Analytics application configuration
            
        Returns:
            CreateApplication response
        """
        try:
            kwargs = {
                "ApplicationName": config.name,
                "RuntimeEnvironment": config.runtime_environment
            }
            
            if config.service_execution_role_arn:
                kwargs["ServiceExecutionRole"] = config.service_execution_role_arn
            
            if config.application_code:
                kwargs["ApplicationCode"] = config.application_code
            
            if config.input_configs:
                kwargs["InputConfigurations"] = config.input_configs
            
            if config.output_configs:
                kwargs["OutputConfigurations"] = config.output_configs
            
            response = self.analytics_client.create_application(**kwargs)
            
            if config.tags:
                try:
                    self.analytics_client.tag_resource(
                        ResourceARN=response["ApplicationDetail"]["ApplicationARN"],
                        Tags=[{"Key": k, "Value": v} for k, v in config.tags.items()]
                    )
                except Exception as e:
                    logger.warning(f"Failed to tag analytics application: {e}")
            
            self._record_metric("AnalyticsApplicationCreated", 1, "Count", {"ApplicationName": config.name})
            logger.info(f"Created Kinesis Analytics application: {config.name}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to create analytics application: {e}")
            raise
    
    def describe_application(self, application_name: str) -> Dict[str, Any]:
        """
        Describe a Kinesis Data Analytics application.
        
        Args:
            application_name: Name of the application
            
        Returns:
            Application details
        """
        try:
            response = self.analytics_client.describe_application(
                ApplicationName=application_name
            )
            return response["ApplicationDetail"]
            
        except ClientError as e:
            logger.error(f"Failed to describe application {application_name}: {e}")
            raise
    
    def list_applications(self) -> List[Dict[str, Any]]:
        """
        List all Kinesis Data Analytics applications.
        
        Returns:
            List of applications
        """
        try:
            applications = []
            paginator = self.analytics_client.get_paginator("list_applications")
            
            for page in paginator.paginate():
                applications.extend(page.get("ApplicationSummaries", []))
            
            return applications
            
        except ClientError as e:
            logger.error(f"Failed to list applications: {e}")
            raise
    
    def delete_application(self, application_name: str, create_backup: bool = False) -> Dict[str, Any]:
        """
        Delete a Kinesis Data Analytics application.
        
        Args:
            application_name: Name of the application
            create_backup: Whether to create a backup of the application configuration
            
        Returns:
            DeleteApplication response
        """
        try:
            kwargs = {"ApplicationName": application_name}
            
            if create_backup:
                kwargs["CreateBackup"] = True
            
            response = self.analytics_client.delete_application(**kwargs)
            
            self._record_metric("AnalyticsApplicationDeleted", 1, "Count", {"ApplicationName": application_name})
            logger.info(f"Deleted Kinesis Analytics application: {application_name}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to delete application {application_name}: {e}")
            raise
    
    def start_application(self, application_name: str) -> Dict[str, Any]:
        """
        Start a Kinesis Data Analytics application.
        
        Args:
            application_name: Name of the application
            
        Returns:
            StartApplication response
        """
        try:
            response = self.analytics_client.start_application(
                ApplicationName=application_name
            )
            
            self._record_metric("AnalyticsApplicationStarted", 1, "Count", {"ApplicationName": application_name})
            logger.info(f"Started Kinesis Analytics application: {application_name}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to start application {application_name}: {e}")
            raise
    
    def stop_application(self, application_name: str) -> Dict[str, Any]:
        """
        Stop a Kinesis Data Analytics application.
        
        Args:
            application_name: Name of the application
            
        Returns:
            StopApplication response
        """
        try:
            response = self.analytics_client.stop_application(
                ApplicationName=application_name
            )
            
            self._record_metric("AnalyticsApplicationStopped", 1, "Count", {"ApplicationName": application_name})
            logger.info(f"Stopped Kinesis Analytics application: {application_name}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to stop application {application_name}: {e}")
            raise
    
    def update_application(
        self,
        application_name: str,
        configuration: Dict[str, Any],
        service_execution_role_arn: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Update a Kinesis Data Analytics application.
        
        Args:
            application_name: Name of the application
            configuration: New application configuration
            service_execution_role_arn: Optional new service execution role ARN
            
        Returns:
            UpdateApplication response
        """
        try:
            kwargs = {
                "ApplicationName": application_name,
                "Configuration": configuration
            }
            
            if service_execution_role_arn:
                kwargs["ServiceExecutionRole"] = service_execution_role_arn
            
            response = self.analytics_client.update_application(**kwargs)
            
            self._record_metric("AnalyticsApplicationUpdated", 1, "Count", {"ApplicationName": application_name})
            logger.info(f"Updated Kinesis Analytics application: {application_name}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to update application {application_name}: {e}")
            raise
    
    # ========================================================================
    # KINESIS DATA FIREHOSE
    # ========================================================================
    
    def create_firehose_delivery_stream(self, config: FirehoseConfig) -> str:
        """
        Create a Kinesis Data Firehose delivery stream.
        
        Args:
            config: Firehose delivery stream configuration
            
        Returns:
            Delivery stream ARN
        """
        try:
            kwargs = {
                "DeliveryStreamName": config.name,
                "DeliveryStreamType": config.delivery_stream_type
            }
            
            if config.delivery_stream_type == "KinesisStreamAsSource":
                kwargs["KinesisStreamSourceConfiguration"] = {
                    "KinesisStreamARN": config.kinesis_stream_arn,
                    "RoleARN": config.role_arn
                }
            
            if config.s3_destination_arn and config.role_arn:
                kwargs["S3DestinationConfiguration"] = {
                    "RoleARN": config.role_arn,
                    "BucketARN": config.s3_destination_arn,
                    "BufferingHints": {
                        "SizeInMBs": config.buffer_size,
                        "IntervalInSeconds": config.buffer_interval
                    },
                    "CompressionFormat": config.compression_format
                }
                
                if config.encryption_configuration:
                    kwargs["S3DestinationConfiguration"]["EncryptionConfiguration"] = config.encryption_configuration
            
            response = self.firehose_client.create_delivery_stream(**kwargs)
            stream_arn = response["DeliveryStreamARN"]
            
            if config.tags:
                try:
                    self.firehose_client.tag_delivery_stream(
                        DeliveryStreamName=config.name,
                        Tags=[{"Key": k, "Value": v} for k, v in config.tags.items()]
                    )
                except Exception as e:
                    logger.warning(f"Failed to tag firehose delivery stream: {e}")
            
            self._record_metric("FirehoseDeliveryStreamCreated", 1, "Count", {"DeliveryStreamName": config.name})
            logger.info(f"Created Firehose delivery stream: {config.name}")
            return stream_arn
            
        except ClientError as e:
            logger.error(f"Failed to create delivery stream {config.name}: {e}")
            raise
    
    def describe_firehose_delivery_stream(self, delivery_stream_name: str) -> Dict[str, Any]:
        """
        Describe a Firehose delivery stream.
        
        Args:
            delivery_stream_name: Name of the delivery stream
            
        Returns:
            Delivery stream description
        """
        try:
            response = self.firehose_client.describe_delivery_stream(
                DeliveryStreamName=delivery_stream_name
            )
            return response["DeliveryStreamDescription"]
            
        except ClientError as e:
            logger.error(f"Failed to describe delivery stream {delivery_stream_name}: {e}")
            raise
    
    def list_firehose_delivery_streams(self) -> List[Dict[str, Any]]:
        """
        List all Firehose delivery streams.
        
        Returns:
            List of delivery streams
        """
        try:
            streams = []
            paginator = self.firehose_client.get_paginator("list_delivery_streams")
            
            for page in paginator.paginate():
                streams.extend(page.get("DeliveryStreamNames", []))
            
            return streams
            
        except ClientError as e:
            logger.error(f"Failed to list delivery streams: {e}")
            raise
    
    def delete_firehose_delivery_stream(self, delivery_stream_name: str) -> Dict[str, Any]:
        """
        Delete a Firehose delivery stream.
        
        Args:
            delivery_stream_name: Name of the delivery stream
            
        Returns:
            DeleteDeliveryStream response
        """
        try:
            response = self.firehose_client.delete_delivery_stream(
                DeliveryStreamName=delivery_stream_name
            )
            
            self._record_metric("FirehoseDeliveryStreamDeleted", 1, "Count", {"DeliveryStreamName": delivery_stream_name})
            logger.info(f"Deleted Firehose delivery stream: {delivery_stream_name}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to delete delivery stream {delivery_stream_name}: {e}")
            raise
    
    def put_to_firehose(
        self,
        delivery_stream_name: str,
        data: Union[str, bytes, Dict, Any],
        record_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Put a record to a Firehose delivery stream.
        
        Args:
            delivery_stream_name: Name of the delivery stream
            data: Data to put (will be serialized)
            record_id: Optional record ID
            
        Returns:
            PutRecord response
        """
        try:
            serialized_data = self._serialize_data(data)
            
            kwargs = {
                "DeliveryStreamName": delivery_stream_name,
                "Record": {"Data": serialized_data}
            }
            
            response = self.firehose_client.put_record(**kwargs)
            
            self._record_metric("FirehoseRecordPut", 1, "Count", {"DeliveryStreamName": delivery_stream_name})
            return response
            
        except ClientError as e:
            logger.error(f"Failed to put record to Firehose {delivery_stream_name}: {e}")
            raise
    
    def update_firehose_destination(
        self,
        delivery_stream_name: str,
        s3_destination_arn: str,
        role_arn: str,
        buffer_size: int = 5,
        buffer_interval: int = 300,
        compression_format: str = "UNCOMPRESSED"
    ) -> Dict[str, Any]:
        """
        Update the S3 destination configuration for a Firehose delivery stream.
        
        Args:
            delivery_stream_name: Name of the delivery stream
            s3_destination_arn: ARN of the S3 bucket
            role_arn: ARN of the role to access S3
            buffer_size: Buffer size in MB
            buffer_interval: Buffer interval in seconds
            compression_format: Compression format
            
        Returns:
            UpdateDestination response
        """
        try:
            response = self.firehose_client.update_destination(
                DeliveryStreamName=delivery_stream_name,
                CurrentDeliveryStreamVersionId=str(int(time.time())),
                S3DestinationUpdate={
                    "RoleARN": role_arn,
                    "BucketARN": s3_destination_arn,
                    "BufferingHints": {
                        "SizeInMBs": buffer_size,
                        "IntervalInSeconds": buffer_interval
                    },
                    "CompressionFormat": compression_format
                }
            )
            
            self._record_metric("FirehoseDestinationUpdated", 1, "Count", {"DeliveryStreamName": delivery_stream_name})
            logger.info(f"Updated Firehose destination for {delivery_stream_name}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to update Firehose destination: {e}")
            raise
    
    # ========================================================================
    # KINESIS VIDEO STREAMS
    # ========================================================================
    
    def create_video_stream(self, config: VideoStreamConfig) -> str:
        """
        Create a Kinesis Video Stream.
        
        Args:
            config: Video stream configuration
            
        Returns:
            Stream ARN
        """
        try:
            kwargs = {
                "StreamName": config.name,
                "DataRetentionInHours": config.data_retention_in_hours
            }
            
            if config.media_type:
                kwargs["MediaType"] = config.media_type
            
            if config.kms_key_id:
                kwargs["KmsKeyId"] = config.kms_key_id
            
            response = self.video_client.create_stream(**kwargs)
            stream_arn = response["StreamARN"]
            
            if config.tags:
                try:
                    self.video_client.tag_stream(
                        StreamARN=stream_arn,
                        Tags=[{"Key": k, "Value": v} for k, v in config.tags.items()]
                    )
                except Exception as e:
                    logger.warning(f"Failed to tag video stream: {e}")
            
            self._record_metric("VideoStreamCreated", 1, "Count", {"StreamName": config.name})
            logger.info(f"Created Kinesis Video Stream: {config.name}")
            return stream_arn
            
        except ClientError as e:
            logger.error(f"Failed to create video stream {config.name}: {e}")
            raise
    
    def describe_video_stream(self, stream_name: str = None, stream_arn: str = None) -> Dict[str, Any]:
        """
        Describe a Kinesis Video Stream.
        
        Args:
            stream_name: Name of the stream
            stream_arn: ARN of the stream
            
        Returns:
            Stream description
        """
        try:
            kwargs = {}
            if stream_arn:
                kwargs["StreamARN"] = stream_arn
            elif stream_name:
                kwargs["StreamName"] = stream_name
            else:
                raise ValueError("Either stream_name or stream_arn must be provided")
            
            response = self.video_client.describe_stream(**kwargs)
            return response["StreamInfo"]
            
        except ClientError as e:
            logger.error(f"Failed to describe video stream: {e}")
            raise
    
    def list_video_streams(self) -> List[Dict[str, Any]]:
        """
        List all Kinesis Video Streams.
        
        Returns:
            List of video streams
        """
        try:
            streams = []
            paginator = self.video_client.get_paginator("list_streams")
            
            for page in paginator.paginate():
                streams.extend(page.get("StreamInfoList", []))
            
            return streams
            
        except ClientError as e:
            logger.error(f"Failed to list video streams: {e}")
            raise
    
    def delete_video_stream(self, stream_name: str = None, stream_arn: str = None) -> Dict[str, Any]:
        """
        Delete a Kinesis Video Stream.
        
        Args:
            stream_name: Name of the stream
            stream_arn: ARN of the stream
            
        Returns:
            DeleteStream response
        """
        try:
            kwargs = {}
            if stream_arn:
                kwargs["StreamARN"] = stream_arn
            elif stream_name:
                kwargs["StreamName"] = stream_name
            else:
                raise ValueError("Either stream_name or stream_arn must be provided")
            
            response = self.video_client.delete_stream(**kwargs)
            
            self._record_metric("VideoStreamDeleted", 1, "Count", {"StreamName": stream_name or stream_arn})
            logger.info(f"Deleted Kinesis Video Stream: {stream_name or stream_arn}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to delete video stream: {e}")
            raise
    
    def get_video_streaming_endpoint(
        self,
        stream_name: str = None,
        stream_arn: str = None,
        api_name: str = "PUT_MEDIA"
    ) -> str:
        """
        Get the streaming endpoint for a Kinesis Video Stream.
        
        Args:
            stream_name: Name of the stream
            stream_arn: ARN of the stream
            api_name: API name (PUT_MEDIA, GET_MEDIA, LIST_FRAGMENTS, GET_DASH_STREAMING_SESSIONURL)
            
        Returns:
            Streaming endpoint URL
        """
        try:
            kwargs = {}
            if stream_arn:
                kwargs["StreamARN"] = stream_arn
            elif stream_name:
                kwargs["StreamName"] = stream_name
            else:
                raise ValueError("Either stream_name or stream_arn must be provided")
            
            kwargs["APIName"] = api_name
            
            response = self.video_client.get_data_endpoint(**kwargs)
            return response["DataEndpoint"]
            
        except ClientError as e:
            logger.error(f"Failed to get video streaming endpoint: {e}")
            raise
    
    def get_hls_streaming_session_url(
        self,
        stream_name: str = None,
        stream_arn: str = None,
        playback_mode: str = "LIVE",
        fragment_selector_type: str = "SERVER_TIMESTAMP",
        hls_segment_duration_seconds: int = 2,
        display_fragment_timestamp: str = "NEVER",
        max_fragment_age_seconds: int = 300
    ) -> str:
        """
        Get HLS streaming session URL for a Kinesis Video Stream.
        
        Args:
            stream_name: Name of the stream
            stream_arn: ARN of the stream
            playback_mode: Playback mode (LIVE, LIVE_REPLAY, ON_DEMAND)
            fragment_selector_type: Fragment selector type (SERVER_TIMESTAMP, PRODUCER_TIMESTAMP)
            hls_segment_duration_seconds: HLS segment duration in seconds
            display_fragment_timestamp: When to display fragment timestamps
            max_fragment_age_seconds: Maximum age of fragments to serve
            
        Returns:
            HLS streaming session URL
        """
        try:
            kwargs = {}
            if stream_arn:
                kwargs["StreamARN"] = stream_arn
            elif stream_name:
                kwargs["StreamName"] = stream_name
            else:
                raise ValueError("Either stream_name or stream_arn must be provided")
            
            kwargs.update({
                "PlaybackMode": playback_mode,
                "HLSFragmentSelector": {
                    "FragmentSelectorType": fragment_selector_type
                },
                "HLSOutputConfiguration": {
                    "HLSManifestUrl": f"https://example.com/stream/{stream_name or stream_arn}/master.m3u8"
                },
                "ContainerFormat": "FRAGMENTED_MP4",
                "DecodingAttributes": {
                    "HLSFragmentSelector": {
                        "FragmentSelectorType": fragment_selector_type
                    }
                },
                "HLSSegmentDurationInSeconds": hls_segment_duration_seconds,
                "DisplayFragmentTimestamp": display_fragment_timestamp,
                "MaxFragmentAgeInSeconds": max_fragment_age_seconds
            })
            
            response = self.video_client.get_hls_streaming_session_url(**kwargs)
            return response["HLSStreamingSessionURL"]
            
        except ClientError as e:
            logger.error(f"Failed to get HLS streaming session URL: {e}")
            raise
    
    # ========================================================================
    # LAMBDA EVENT SOURCE MAPPING
    # ========================================================================
    
    def create_event_source_mapping(
        self,
        config: LambdaEventSourceMappingConfig
    ) -> Dict[str, Any]:
        """
        Create a Lambda event source mapping to consume from Kinesis.
        
        Args:
            config: Event source mapping configuration
            
        Returns:
            UUID of the created mapping
        """
        try:
            kwargs = {
                "EventSourceArn": config.stream_arn,
                "FunctionName": config.function_name,
                "BatchSize": config.batch_size,
                "MaximumBatchingWindowInSeconds": config.max_batch_duration,
                "ParallelizationFactor": config.parallelization_factor,
                "StartingPosition": config.starting_position,
                "BisectBatchOnFunctionError": config.bisect_batch_on_function_error,
                "MaximumRetryAttempts": config.maximum_retry_attempts,
                "TumblingWindowInSeconds": config.tumbling_window_in_seconds,
                "MaximumRecordAgeInSeconds": config.maximum_record_age_in_seconds,
                "Enabled": config.enable
            }
            
            if config.starting_position == "AT_TIMESTAMP" and config.starting_timestamp:
                kwargs["StartingTimestamp"] = config.starting_timestamp.isoformat()
            
            if config.destination_config:
                kwargs["DestinationConfig"] = config.destination_config
            
            if config.filter_criteria:
                kwargs["FilterCriteria"] = {"Filters": config.filter_criteria}
            
            response = self.lambda_client.create_event_source_mapping(**kwargs)
            uuid = response["UUID"]
            
            self._record_metric("EventSourceMappingCreated", 1, "Count", {"FunctionName": config.function_name})
            logger.info(f"Created Lambda event source mapping for {config.function_name}")
            return uuid
            
        except ClientError as e:
            logger.error(f"Failed to create event source mapping: {e}")
            raise
    
    def list_event_source_mappings(
        self,
        stream_arn: Optional[str] = None,
        function_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        List Lambda event source mappings.
        
        Args:
            stream_arn: Filter by stream ARN
            function_name: Filter by function name
            
        Returns:
            List of event source mappings
        """
        try:
            kwargs = {}
            
            if stream_arn:
                kwargs["EventSourceArn"] = stream_arn
            if function_name:
                kwargs["FunctionName"] = function_name
            
            response = self.lambda_client.list_event_source_mappings(**kwargs)
            return response.get("EventSourceMappings", [])
            
        except ClientError as e:
            logger.error(f"Failed to list event source mappings: {e}")
            raise
    
    def get_event_source_mapping(self, uuid: str) -> Dict[str, Any]:
        """
        Get a Lambda event source mapping by UUID.
        
        Args:
            uuid: UUID of the event source mapping
            
        Returns:
            Event source mapping details
        """
        try:
            response = self.lambda_client.get_event_source_mapping(UUID=uuid)
            return response
            
        except ClientError as e:
            logger.error(f"Failed to get event source mapping {uuid}: {e}")
            raise
    
    def update_event_source_mapping(
        self,
        uuid: str,
        batch_size: Optional[int] = None,
        maximum_batching_window_in_seconds: Optional[int] = None,
        parallelization_factor: Optional[int] = None,
        destination_config: Optional[Dict[str, Any]] = None,
        maximum_retry_attempts: Optional[int] = None,
        maximum_record_age_in_seconds: Optional[int] = None,
        bisect_batch_on_function_error: Optional[bool] = None,
        tumbling_window_in_seconds: Optional[int] = None,
        filter_criteria: Optional[List[Dict[str, Any]]] = None,
        enable: Optional[bool] = None
    ) -> Dict[str, Any]:
        """
        Update a Lambda event source mapping.
        
        Args:
            uuid: UUID of the event source mapping
            batch_size: New batch size
            maximum_batching_window_in_seconds: New batching window
            parallelization_factor: New parallelization factor
            destination_config: New destination config
            maximum_retry_attempts: New maximum retry attempts
            maximum_record_age_in_seconds: New maximum record age
            bisect_batch_on_function_error: New bisect batch setting
            tumbling_window_in_seconds: New tumbling window
            filter_criteria: New filter criteria
            enable: Enable or disable the mapping
            
        Returns:
            Updated event source mapping
        """
        try:
            kwargs = {"UUID": uuid}
            
            if batch_size is not None:
                kwargs["BatchSize"] = batch_size
            if maximum_batching_window_in_seconds is not None:
                kwargs["MaximumBatchingWindowInSeconds"] = maximum_batching_window_in_seconds
            if parallelization_factor is not None:
                kwargs["ParallelizationFactor"] = parallelization_factor
            if destination_config is not None:
                kwargs["DestinationConfig"] = destination_config
            if maximum_retry_attempts is not None:
                kwargs["MaximumRetryAttempts"] = maximum_retry_attempts
            if maximum_record_age_in_seconds is not None:
                kwargs["MaximumRecordAgeInSeconds"] = maximum_record_age_in_seconds
            if bisect_batch_on_function_error is not None:
                kwargs["BisectBatchOnFunctionError"] = bisect_batch_on_function_error
            if tumbling_window_in_seconds is not None:
                kwargs["TumblingWindowInSeconds"] = tumbling_window_in_seconds
            if filter_criteria is not None:
                kwargs["FilterCriteria"] = {"Filters": filter_criteria}
            if enable is not None:
                kwargs["Enabled"] = enable
            
            response = self.lambda_client.update_event_source_mapping(**kwargs)
            
            self._record_metric("EventSourceMappingUpdated", 1, "Count")
            logger.info(f"Updated Lambda event source mapping {uuid}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to update event source mapping {uuid}: {e}")
            raise
    
    def delete_event_source_mapping(self, uuid: str) -> Dict[str, Any]:
        """
        Delete a Lambda event source mapping.
        
        Args:
            uuid: UUID of the event source mapping
            
        Returns:
            DeleteEventSourceMapping response
        """
        try:
            response = self.lambda_client.delete_event_source_mapping(UUID=uuid)
            
            self._record_metric("EventSourceMappingDeleted", 1, "Count")
            logger.info(f"Deleted Lambda event source mapping {uuid}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to delete event source mapping {uuid}: {e}")
            raise
    
    # ========================================================================
    # SERVER-SIDE ENCRYPTION
    # ========================================================================
    
    def enable_stream_encryption(
        self,
        stream_name: str,
        kms_key_id: str
    ) -> Dict[str, Any]:
        """
        Enable server-side encryption for a stream.
        
        Args:
            stream_name: Name of the stream
            kms_key_id: KMS key ID to use for encryption
            
        Returns:
            StartStreamEncryption response
        """
        try:
            response = self.kinesis_client.start_stream_encryption(
                StreamName=stream_name,
                EncryptionType="KMS",
                KeyId=kms_key_id
            )
            
            self._record_metric("StreamEncryptionEnabled", 1, "Count", {"StreamName": stream_name})
            logger.info(f"Enabled encryption for stream {stream_name}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to enable encryption for {stream_name}: {e}")
            raise
    
    def disable_stream_encryption(self, stream_name: str) -> Dict[str, Any]:
        """
        Disable server-side encryption for a stream.
        
        Args:
            stream_name: Name of the stream
            
        Returns:
            StopStreamEncryption response
        """
        try:
            response = self.kinesis_client.stop_stream_encryption(
                StreamName=stream_name,
                EncryptionType="KMS"
            )
            
            self._record_metric("StreamEncryptionDisabled", 1, "Count", {"StreamName": stream_name})
            logger.info(f"Disabled encryption for stream {stream_name}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to disable encryption for {stream_name}: {e}")
            raise
    
    def describe_stream_encryption(self, stream_name: str) -> Dict[str, Any]:
        """
        Describe encryption configuration for a stream.
        
        Args:
            stream_name: Name of the stream
            
        Returns:
            Encryption configuration
        """
        try:
            response = self.kinesis_client.describe_stream_summary(
                StreamName=stream_name
            )
            return response.get("StreamDescriptionSummary", {}).get("EncryptionType", "NONE")
            
        except ClientError as e:
            logger.error(f"Failed to describe encryption for {stream_name}: {e}")
            raise
    
    # ========================================================================
    # CLOUDWATCH METRICS
    # ========================================================================
    
    def get_stream_metrics(
        self,
        stream_name: str,
        start_time: datetime,
        end_time: datetime,
        period: int = 60
    ) -> Dict[str, Any]:
        """
        Get CloudWatch metrics for a Kinesis stream.
        
        Args:
            stream_name: Name of the stream
            start_time: Start of the time range
            end_time: End of the time range
            period: Metric period in seconds
            
        Returns:
            CloudWatch metric data
        """
        try:
            metric_names = [
                "IncomingRecords",
                "IncomingBytes",
                "OutgoingRecords",
                "OutgoingBytes",
                "WriteProvisionedThroughputExceeded",
                "ReadProvisionedThroughputExceeded",
                "IteratorAgeMilliseconds",
                "Explore"
            ]
            
            dimensions = [{"Name": "StreamName", "Value": stream_name}]
            
            metrics_data = {}
            
            for metric_name in metric_names:
                response = self.cloudwatch_client.get_metric_statistics(
                    Namespace="AWS/Kinesis",
                    MetricName=metric_name,
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=period,
                    Statistics=["Sum", "Average", "Maximum"],
                    Dimensions=dimensions
                )
                
                metrics_data[metric_name] = response.get("Datapoints", [])
            
            return metrics_data
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get stream metrics: {e}")
            raise
    
    def get_consumer_metrics(
        self,
        consumer_arn: str,
        start_time: datetime,
        end_time: datetime,
        period: int = 60
    ) -> Dict[str, Any]:
        """
        Get CloudWatch metrics for a Kinesis consumer.
        
        Args:
            consumer_arn: ARN of the consumer
            start_time: Start of the time range
            end_time: End of the time range
            period: Metric period in seconds
            
        Returns:
            CloudWatch metric data
        """
        try:
            dimensions = [{"Name": "ConsumerARN", "Value": consumer_arn}]
            
            metrics_data = {}
            
            for metric_name in ["IncomingRecords", "OutgoingRecords", "MillisBehindLatest"]:
                response = self.cloudwatch_client.get_metric_statistics(
                    Namespace="AWS/Kinesis",
                    MetricName=metric_name,
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=period,
                    Statistics=["Sum", "Average", "Maximum"],
                    Dimensions=dimensions
                )
                
                metrics_data[metric_name] = response.get("Datapoints", [])
            
            return metrics_data
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get consumer metrics: {e}")
            raise
    
    def put_custom_metric(
        self,
        metric_name: str,
        value: float,
        unit: str = "Count",
        dimensions: Optional[Dict[str, str]] = None
    ):
        """
        Put a custom CloudWatch metric.
        
        Args:
            metric_name: Name of the metric
            value: Metric value
            unit: Unit of the metric
            dimensions: Metric dimensions
        """
        self._record_metric(metric_name, value, unit, dimensions)
    
    # ========================================================================
    # UTILITY METHODS
    # ========================================================================
    
    def get_stream_arn(self, stream_name: str) -> str:
        """
        Get the ARN of a stream.
        
        Args:
            stream_name: Name of the stream
            
        Returns:
            Stream ARN
        """
        description = self.describe_stream(stream_name)
        return description["StreamARN"]
    
    def get_consumer_arn(self, stream_name: str, consumer_name: str) -> str:
        """
        Get the ARN of a consumer.
        
        Args:
            stream_name: Name of the stream
            consumer_name: Name of the consumer
            
        Returns:
            Consumer ARN
        """
        consumers = self.list_consumers(stream_name)
        for consumer in consumers:
            if consumer["ConsumerName"] == consumer_name:
                return consumer["ConsumerARN"]
        raise ValueError(f"Consumer {consumer_name} not found")
    
    def hash_key_to_partition_key(self, hash_key: str, partition_keys: List[str]) -> str:
        """
        Map a hash key to a partition key by finding the appropriate range.
        
        Args:
            hash_key: Hash key value
            partition_keys: List of partition keys
            
        Returns:
            Best matching partition key
        """
        hash_int = int(hash_key)
        
        for pk in partition_keys:
            pk_hash = int(hashlib.md5(pk.encode()).hexdigest(), 16) % (2 ** 128)
            if hash_int < pk_hash:
                return pk
        
        return partition_keys[0] if partition_keys else "default"
    
    def compute_sequence_number(self, data: Union[str, bytes, Dict, Any]) -> str:
        """
        Compute a sequence number for ordering.
        
        Args:
            data: Data to hash
            
        Returns:
            Pseudo-random sequence number
        """
        serialized = self._serialize_data(data)
        hash_digest = hashlib.md5(serialized).hexdigest()
        return f"{int(hash_digest[:16], 16):032d}"
    
    def health_check(self) -> Dict[str, Any]:
        """
        Perform a health check on Kinesis integration.
        
        Returns:
            Health check results
        """
        results = {
            "kinesis": False,
            "firehose": False,
            "analytics": False,
            "video": False,
            "lambda": False,
            "cloudwatch": False
        }
        
        try:
            self.list_streams()
            results["kinesis"] = True
        except Exception as e:
            results["kinesis_error"] = str(e)
        
        try:
            self.list_firehose_delivery_streams()
            results["firehose"] = True
        except Exception as e:
            results["firehose_error"] = str(e)
        
        try:
            self.list_applications()
            results["analytics"] = True
        except Exception as e:
            results["analytics_error"] = str(e)
        
        try:
            self.list_video_streams()
            results["video"] = True
        except Exception as e:
            results["video_error"] = str(e)
        
        try:
            self.cloudwatch_client.list_metrics(Namespace=self._cloudwatch_namespace)
            results["cloudwatch"] = True
        except Exception as e:
            results["cloudwatch_error"] = str(e)
        
        results["healthy"] = all([results["kinesis"], results["firehose"]])
        return results
    
    def close(self):
        """Flush any pending metrics and close connections."""
        self.flush_metrics()
        logger.info("Kinesis integration closed")
