"""
Tests for workflow_aws_kinesis module
"""
import sys
sys.path.insert(0, '/Users/guige/my_project')

import unittest
from unittest.mock import Mock, patch, MagicMock, PropertyMock
import json
import time
import os
import types
from datetime import datetime

# Create mock boto3 module before importing workflow_aws_kinesis
mock_boto3 = types.ModuleType('boto3')
mock_session = MagicMock()
mock_boto3.Session = MagicMock(return_value=mock_session)
mock_boto3.client = MagicMock()

# Create mock botocore exceptions
mock_boto3_exceptions = types.ModuleType('botocore.exceptions')
mock_boto3_exceptions.ClientError = Exception
mock_boto3_exceptions.BotoCoreError = Exception

sys.modules['boto3'] = mock_boto3
sys.modules['botocore'] = types.ModuleType('botocore')
sys.modules['botocore.exceptions'] = mock_boto3_exceptions

# Now import the module
from src.workflow_aws_kinesis import (
    KinesisIntegration,
    StreamStatus,
    ShardIteratorType,
    EncryptionType,
    StreamConfig,
    ShardConfig,
    RecordConfig,
    ConsumerConfig,
    FirehoseConfig,
    AnalyticsConfig,
    VideoStreamConfig,
    LambdaEventSourceMappingConfig,
)


class TestStreamStatus(unittest.TestCase):
    """Test StreamStatus enum"""
    def test_stream_status_values(self):
        self.assertEqual(StreamStatus.CREATING.value, "CREATING")
        self.assertEqual(StreamStatus.DELETING.value, "DELETING")
        self.assertEqual(StreamStatus.ACTIVE.value, "ACTIVE")
        self.assertEqual(StreamStatus.UPDATING.value, "UPDATING")


class TestShardIteratorType(unittest.TestCase):
    """Test ShardIteratorType enum"""
    def test_shard_iterator_type_values(self):
        self.assertEqual(ShardIteratorType.AT_SEQUENCE_NUMBER.value, "AT_SEQUENCE_NUMBER")
        self.assertEqual(ShardIteratorType.AFTER_SEQUENCE_NUMBER.value, "AFTER_SEQUENCE_NUMBER")
        self.assertEqual(ShardIteratorType.AT_TIMESTAMP.value, "AT_TIMESTAMP")
        self.assertEqual(ShardIteratorType.TRIM_HORIZON.value, "TRIM_HORIZON")
        self.assertEqual(ShardIteratorType.LATEST.value, "LATEST")


class TestEncryptionType(unittest.TestCase):
    """Test EncryptionType enum"""
    def test_encryption_type_values(self):
        self.assertEqual(EncryptionType.NONE.value, "NONE")
        self.assertEqual(EncryptionType.KMS.value, "KMS")


class TestStreamConfig(unittest.TestCase):
    """Test StreamConfig dataclass"""
    def test_stream_config_defaults(self):
        config = StreamConfig(name="test-stream")
        self.assertEqual(config.name, "test-stream")
        self.assertEqual(config.shard_count, 1)
        self.assertEqual(config.retention_period_hours, 24)
        self.assertEqual(config.stream_mode, "PROVISIONED")
        self.assertEqual(config.encryption_type, EncryptionType.NONE)

    def test_stream_config_full(self):
        config = StreamConfig(
            name="prod-stream",
            shard_count=4,
            retention_period_hours=48,
            stream_mode="ON_DEMAND",
            kms_key_id="my-key-id",
            encryption_type=EncryptionType.KMS,
            tags={"env": "prod"}
        )
        self.assertEqual(config.name, "prod-stream")
        self.assertEqual(config.shard_count, 4)
        self.assertEqual(config.retention_period_hours, 48)
        self.assertEqual(config.stream_mode, "ON_DEMAND")
        self.assertEqual(config.kms_key_id, "my-key-id")
        self.assertEqual(config.encryption_type, EncryptionType.KMS)
        self.assertEqual(config.tags, {"env": "prod"})


class TestRecordConfig(unittest.TestCase):
    """Test RecordConfig dataclass"""
    def test_record_config_string_data(self):
        config = RecordConfig(
            stream_name="test-stream",
            data="test-data",
            partition_key="pk1"
        )
        self.assertEqual(config.stream_name, "test-stream")
        self.assertEqual(config.data, "test-data")
        self.assertEqual(config.partition_key, "pk1")
        self.assertIsNone(config.explicit_hash_key)

    def test_record_config_dict_data(self):
        config = RecordConfig(
            stream_name="test-stream",
            data={"key": "value"},
            partition_key="pk1"
        )
        self.assertEqual(config.data, {"key": "value"})

    def test_record_config_bytes_data(self):
        config = RecordConfig(
            stream_name="test-stream",
            data=b"binary-data",
            partition_key="pk1"
        )
        self.assertEqual(config.data, b"binary-data")


class TestConsumerConfig(unittest.TestCase):
    """Test ConsumerConfig dataclass"""
    def test_consumer_config(self):
        config = ConsumerConfig(
            stream_name="test-stream",
            consumer_name="my-consumer"
        )
        self.assertEqual(config.stream_name, "test-stream")
        self.assertEqual(config.consumer_name, "my-consumer")
        self.assertIsNone(config.consumer_arn)


class TestFirehoseConfig(unittest.TestCase):
    """Test FirehoseConfig dataclass"""
    def test_firehose_config_defaults(self):
        config = FirehoseConfig(name="my-firehose")
        self.assertEqual(config.name, "my-firehose")
        self.assertEqual(config.delivery_stream_type, "DirectPut")
        self.assertEqual(config.buffer_size, 5)
        self.assertEqual(config.buffer_interval, 300)
        self.assertEqual(config.compression_format, "UNCOMPRESSED")

    def test_firehose_config_full(self):
        config = FirehoseConfig(
            name="prod-firehose",
            delivery_stream_type="KinesisStreamAsSource",
            kinesis_stream_arn="arn:aws:kinesis:us-east-1:123456789:stream/my-stream",
            s3_destination_arn="arn:aws:s3:::my-bucket",
            role_arn="arn:aws:iam::123456789:role/my-role",
            buffer_size=10,
            buffer_interval=600,
            compression_format="GZIP"
        )
        self.assertEqual(config.name, "prod-firehose")
        self.assertEqual(config.delivery_stream_type, "KinesisStreamAsSource")
        self.assertEqual(config.buffer_size, 10)
        self.assertEqual(config.compression_format, "GZIP")


class TestAnalyticsConfig(unittest.TestCase):
    """Test AnalyticsConfig dataclass"""
    def test_analytics_config_defaults(self):
        config = AnalyticsConfig(name="my-analytics")
        self.assertEqual(config.name, "my-analytics")
        self.assertEqual(config.runtime_environment, "FLINK-1_11")
        self.assertEqual(config.input_configs, [])
        self.assertEqual(config.output_configs, [])

    def test_analytics_config_full(self):
        config = AnalyticsConfig(
            name="prod-analytics",
            runtime_environment="ZEPPLIN-0_8",
            service_execution_role_arn="arn:aws:iam::123456789:role/my-role",
            application_code="SELECT * FROM input_stream",
            input_configs=[{"InputSchema": {"RecordFormat": "JSON"}}],
            output_configs=[{"DestinationSchema": {"RecordFormat": "JSON"}}]
        )
        self.assertEqual(config.name, "prod-analytics")
        self.assertEqual(config.runtime_environment, "ZEPPLIN-0_8")


class TestVideoStreamConfig(unittest.TestCase):
    """Test VideoStreamConfig dataclass"""
    def test_video_stream_config_defaults(self):
        config = VideoStreamConfig(name="my-video-stream")
        self.assertEqual(config.name, "my-video-stream")
        self.assertEqual(config.data_retention_in_hours, 24)
        self.assertIsNone(config.media_type)

    def test_video_stream_config_full(self):
        config = VideoStreamConfig(
            name="prod-video-stream",
            data_retention_in_hours=48,
            media_type="video/h264",
            kms_key_id="my-key-id",
            tags={"env": "prod"}
        )
        self.assertEqual(config.name, "prod-video-stream")
        self.assertEqual(config.data_retention_in_hours, 48)
        self.assertEqual(config.media_type, "video/h264")


class TestLambdaEventSourceMappingConfig(unittest.TestCase):
    """Test LambdaEventSourceMappingConfig dataclass"""
    def test_lambda_event_source_mapping_defaults(self):
        config = LambdaEventSourceMappingConfig(
            function_name="my-function",
            stream_arn="arn:aws:kinesis:us-east-1:123456789:stream/my-stream"
        )
        self.assertEqual(config.function_name, "my-function")
        self.assertEqual(config.batch_size, 100)
        self.assertEqual(config.starting_position, "TRIM_HORIZON")
        self.assertEqual(config.maximum_retry_attempts, -1)
        self.assertTrue(config.enable)

    def test_lambda_event_source_mapping_full(self):
        config = LambdaEventSourceMappingConfig(
            function_name="my-function",
            stream_arn="arn:aws:kinesis:us-east-1:123456789:stream/my-stream",
            batch_size=500,
            parallelization_factor=4,
            starting_position="LATEST",
            maximum_retry_attempts=3,
            enable=False
        )
        self.assertEqual(config.batch_size, 500)
        self.assertEqual(config.parallelization_factor, 4)
        self.assertEqual(config.maximum_retry_attempts, 3)
        self.assertFalse(config.enable)


class TestKinesisIntegration(unittest.TestCase):
    """Test KinesisIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_kinesis_client = MagicMock()
        self.mock_firehose_client = MagicMock()
        self.mock_analytics_client = MagicMock()
        self.mock_video_client = MagicMock()
        self.mock_lambda_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()
        self.mock_iam_client = MagicMock()

        self.integration = KinesisIntegration(
            region_name="us-east-1",
            kinesis_client=self.mock_kinesis_client,
            firehose_client=self.mock_firehose_client,
            analytics_client=self.mock_analytics_client,
            video_client=self.mock_video_client,
            lambda_client=self.mock_lambda_client,
            cloudwatch_client=self.mock_cloudwatch_client,
            iam_client=self.mock_iam_client
        )

    def test_init_with_clients(self):
        """Test initialization with pre-configured clients"""
        self.assertEqual(self.integration.region_name, "us-east-1")
        self.assertEqual(self.integration.kinesis_client, self.mock_kinesis_client)
        self.assertEqual(self.integration.firehose_client, self.mock_firehose_client)

    def test_init_boto3_unavailable(self):
        """Test initialization raises ImportError when boto3 unavailable"""
        # Temporarily set BOTO3_AVAILABLE to False
        import src.workflow_aws_kinesis as kinesis_module
        original_value = kinesis_module.BOTO3_AVAILABLE
        kinesis_module.BOTO3_AVAILABLE = False
        kinesis_module.boto3 = None

        with self.assertRaises(ImportError):
            KinesisIntegration(region_name="us-east-1")

        # Restore
        kinesis_module.BOTO3_AVAILABLE = original_value
        kinesis_module.boto3 = mock_boto3

    def test_create_stream(self):
        """Test creating a Kinesis stream"""
        config = StreamConfig(name="test-stream", shard_count=2)
        self.mock_kinesis_client.create_stream.return_value = {
            "StreamARN": "arn:aws:kinesis:us-east-1:123456789:stream/test-stream"
        }

        result = self.integration.create_stream(config)

        self.mock_kinesis_client.create_stream.assert_called_once()
        call_args = self.mock_kinesis_client.create_stream.call_args
        self.assertEqual(call_args.kwargs["StreamName"], "test-stream")
        self.assertEqual(call_args.kwargs["ShardCount"], 2)

    def test_create_stream_with_kms_encryption(self):
        """Test creating a stream with KMS encryption"""
        config = StreamConfig(
            name="encrypted-stream",
            shard_count=1,
            kms_key_id="my-key-id",
            encryption_type=EncryptionType.KMS
        )
        self.mock_kinesis_client.create_stream.return_value = {}

        self.integration.create_stream(config)

        call_args = self.mock_kinesis_client.create_stream.call_args
        self.assertIn("StreamEncryption", call_args.kwargs)
        self.assertEqual(call_args.kwargs["StreamEncryption"]["EncryptionType"], "KMS")
        self.assertEqual(call_args.kwargs["StreamEncryption"]["KeyId"], "my-key-id")

    def test_create_stream_on_demand(self):
        """Test creating an on-demand stream"""
        config = StreamConfig(name="ondemand-stream", stream_mode="ON_DEMAND")
        self.mock_kinesis_client.create_stream.return_value = {}

        self.integration.create_stream(config)

        call_args = self.mock_kinesis_client.create_stream.call_args
        self.assertIn("StreamModeDetails", call_args.kwargs)
        self.assertEqual(call_args.kwargs["StreamModeDetails"]["StreamMode"], "ON_DEMAND")

    def test_describe_stream(self):
        """Test describing a stream"""
        self.mock_kinesis_client.describe_stream.return_value = {
            "StreamDescription": {
                "StreamName": "test-stream",
                "StreamARN": "arn:aws:kinesis:us-east-1:123456789:stream/test-stream",
                "StreamStatus": "ACTIVE",
                "Shards": [
                    {
                        "ShardId": "shard-id-1",
                        "HashKeyRange": {"StartingHashKey": "0", "EndingHashKey": "100"},
                        "SequenceNumberRange": {"StartingSequenceNumber": "001"}
                    }
                ]
            }
        }

        result = self.integration.describe_stream("test-stream")

        self.assertEqual(result["StreamName"], "test-stream")
        self.assertEqual(result["StreamStatus"], "ACTIVE")
        self.assertEqual(len(result["Shards"]), 1)
        self.assertEqual(result["Shards"][0]["ShardId"], "shard-id-1")

    def test_describe_stream_uses_cache(self):
        """Test describe_stream uses cached data"""
        self.integration._stream_cache["cached-stream"] = {
            "StreamName": "cached-stream",
            "StreamARN": "arn:aws:kinesis:us-east-1:123456789:stream/cached-stream",
            "StreamStatus": "ACTIVE"
        }

        result = self.integration.describe_stream("cached-stream")

        self.assertEqual(result["StreamName"], "cached-stream")
        self.mock_kinesis_client.describe_stream.assert_not_called()

    def test_describe_stream_force_refresh(self):
        """Test describe_stream force refresh"""
        self.integration._stream_cache["test-stream"] = {
            "StreamName": "test-stream",
            "StreamARN": "old-arn",
            "StreamStatus": "OLD"
        }
        self.mock_kinesis_client.describe_stream.return_value = {
            "StreamDescription": {
                "StreamName": "test-stream",
                "StreamARN": "arn:aws:kinesis:us-east-1:123456789:stream/test-stream",
                "StreamStatus": "ACTIVE",
                "Shards": []
            }
        }

        result = self.integration.describe_stream("test-stream", force_refresh=True)

        self.assertEqual(result["StreamARN"], "arn:aws:kinesis:us-east-1:123456789:stream/test-stream")
        self.mock_kinesis_client.describe_stream.assert_called_once()

    def test_list_streams(self):
        """Test listing streams"""
        mock_paginator = MagicMock()
        self.mock_kinesis_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {"StreamNames": ["stream1", "stream2"]},
            {"StreamNames": ["stream3"]}
        ]

        result = self.integration.list_streams()

        self.assertEqual(result, ["stream1", "stream2", "stream3"])
        self.mock_kinesis_client.get_paginator.assert_called_once_with("list_streams")

    def test_delete_stream(self):
        """Test deleting a stream"""
        self.mock_kinesis_client.delete_stream.return_value = {}

        result = self.integration.delete_stream("test-stream")

        self.mock_kinesis_client.delete_stream.assert_called_once_with(StreamName="test-stream")
        self.assertNotIn("test-stream", self.integration._stream_cache)

    def test_put_record(self):
        """Test putting a record"""
        self.mock_kinesis_client.put_record.return_value = {
            "ShardId": "shard-id-1",
            "SequenceNumber": "12345678901234567890",
            "Encrypted": True
        }

        result = self.integration.put_record(
            stream_name="test-stream",
            data="test-data",
            partition_key="pk1"
        )

        self.assertEqual(result["ShardId"], "shard-id-1")
        self.assertIn("SequenceNumber", result)
        self.mock_kinesis_client.put_record.assert_called_once()

    def test_put_record_bytes_data(self):
        """Test putting a record with bytes data"""
        self.mock_kinesis_client.put_record.return_value = {
            "ShardId": "shard-id-1",
            "SequenceNumber": "12345678901234567890"
        }

        self.integration.put_record(
            stream_name="test-stream",
            data=b"binary-data",
            partition_key="pk1"
        )

        call_args = self.mock_kinesis_client.put_record.call_args
        self.assertEqual(call_args.kwargs["Data"], b"binary-data")

    def test_put_record_dict_data(self):
        """Test putting a record with dict data"""
        self.mock_kinesis_client.put_record.return_value = {
            "ShardId": "shard-id-1",
            "SequenceNumber": "12345678901234567890"
        }

        self.integration.put_record(
            stream_name="test-stream",
            data={"key": "value"},
            partition_key="pk1"
        )

        call_args = self.mock_kinesis_client.put_record.call_args
        self.assertEqual(call_args.kwargs["Data"], b'{"key": "value"}')

    def test_put_records(self):
        """Test putting multiple records"""
        self.mock_kinesis_client.put_records.return_value = {
            "FailedRecordCount": 0,
            "Records": [
                {"ShardId": "shard-1", "SequenceNumber": "001"},
                {"ShardId": "shard-2", "SequenceNumber": "002"}
            ]
        }

        result = self.integration.put_records(
            stream_name="test-stream",
            records=[
                {"data": "data1", "partition_key": "pk1"},
                {"data": "data2", "partition_key": "pk2"}
            ]
        )

        self.assertEqual(result["FailedRecordCount"], 0)
        self.assertEqual(len(result["Records"]), 2)

    def test_get_shard_iterator(self):
        """Test getting a shard iterator"""
        self.mock_kinesis_client.get_shard_iterator.return_value = {
            "ShardIterator": "iterator-value"
        }

        result = self.integration.get_shard_iterator(
            stream_name="test-stream",
            shard_id="shard-id-1",
            iterator_type=ShardIteratorType.LATEST
        )

        self.assertEqual(result, "iterator-value")
        self.mock_kinesis_client.get_shard_iterator.assert_called_once()

    def test_get_records(self):
        """Test getting records from a shard"""
        self.mock_kinesis_client.get_records.return_value = {
            "Records": [
                {
                    "Data": b"record-data-1",
                    "SequenceNumber": "001",
                    "PartitionKey": "pk1"
                },
                {
                    "Data": b"record-data-2",
                    "SequenceNumber": "002",
                    "PartitionKey": "pk2"
                }
            ],
            "NextShardIterator": "next-iterator",
            "MillisBehindLatest": 0
        }

        result = self.integration.get_records(
            shard_iterator="test-iterator",
            limit=100
        )

        self.assertEqual(len(result["Records"]), 2)
        self.assertEqual(result["NextShardIterator"], "next-iterator")

    def test_deregister_consumer(self):
        """Test deregistering a consumer"""
        self.mock_kinesis_client.deregister_stream_consumer.return_value = {}

        result = self.integration.deregister_consumer(
            consumer_name="my-consumer",
            stream_name="test-stream"
        )

        self.mock_kinesis_client.deregister_stream_consumer.assert_called_once()

    def test_delete_firehose_delivery_stream(self):
        """Test deleting a Firehose delivery stream"""
        self.mock_firehose_client.delete_delivery_stream.return_value = {}

        result = self.integration.delete_firehose_delivery_stream("my-firehose")

        self.mock_firehose_client.delete_delivery_stream.assert_called_once_with(
            DeliveryStreamName="my-firehose"
        )

    def test_delete_video_stream(self):
        """Test deleting a Kinesis Video Stream"""
        self.mock_video_client.delete_stream.return_value = {}

        result = self.integration.delete_video_stream("my-video-stream")

        self.mock_video_client.delete_stream.assert_called_once()

    def test_flush_metrics(self):
        """Test flushing metrics to CloudWatch"""
        self.integration._metrics_buffer = [
            {"MetricName": "TestMetric", "Value": 1.0}
        ]
        self.mock_cloudwatch_client.put_metric_data.return_value = {}

        self.integration.flush_metrics()

        self.mock_cloudwatch_client.put_metric_data.assert_called_once()
        self.assertEqual(len(self.integration._metrics_buffer), 0)

    def test_serialize_data_string(self):
        """Test serializing string data"""
        result = self.integration._serialize_data("test-string")
        self.assertEqual(result, b"test-string")

    def test_serialize_data_bytes(self):
        """Test serializing bytes data"""
        result = self.integration._serialize_data(b"test-bytes")
        self.assertEqual(result, b"test-bytes")

    def test_serialize_data_dict(self):
        """Test serializing dict data"""
        result = self.integration._serialize_data({"key": "value"})
        self.assertEqual(result, b'{"key": "value"}')


if __name__ == "__main__":
    unittest.main()
