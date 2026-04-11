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
        import src.workflow_aws_kinesis as kinesis_module
        original_value = kinesis_module.BOTO3_AVAILABLE
        kinesis_module.BOTO3_AVAILABLE = False
        kinesis_module.boto3 = None

        with self.assertRaises(ImportError):
            KinesisIntegration(region_name="us-east-1")

        # Restore
        kinesis_module.BOTO3_AVAILABLE = original_value
        kinesis_module.boto3 = mock_boto3

    # ========================================================================
    # STREAM MANAGEMENT TESTS
    # ========================================================================

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

    def test_update_stream_mode(self):
        """Test updating stream mode"""
        self.mock_kinesis_client.update_stream_mode.return_value = {}

        result = self.integration.update_stream_mode("test-stream", "ON_DEMAND")

        self.mock_kinesis_client.update_stream_mode.assert_called_once_with(
            StreamName="test-stream",
            StreamModeDetails={"StreamMode": "ON_DEMAND"}
        )

    def test_wait_for_stream_active(self):
        """Test waiting for stream to become active"""
        self.mock_kinesis_client.describe_stream.return_value = {
            "StreamDescription": {
                "StreamName": "test-stream",
                "StreamARN": "arn:aws:kinesis:us-east-1:123456789:stream/test-stream",
                "StreamStatus": "ACTIVE",
                "Shards": []
            }
        }

        result = self.integration.wait_for_stream_active("test-stream", timeout=10)

        self.assertTrue(result)

    def test_wait_for_stream_active_timeout(self):
        """Test wait_for_stream_active timeout"""
        self.mock_kinesis_client.describe_stream.return_value = {
            "StreamDescription": {
                "StreamName": "test-stream",
                "StreamARN": "arn:aws:kinesis:us-east-1:123456789:stream/test-stream",
                "StreamStatus": "CREATING",
                "Shards": []
            }
        }

        result = self.integration.wait_for_stream_active("test-stream", timeout=2)

        self.assertFalse(result)

    # ========================================================================
    # SHARD MANAGEMENT TESTS
    # ========================================================================

    def test_get_shards(self):
        """Test getting shards for a stream"""
        self.mock_kinesis_client.describe_stream.return_value = {
            "StreamDescription": {
                "StreamName": "test-stream",
                "StreamARN": "arn:aws:kinesis:us-east-1:123456789:stream/test-stream",
                "StreamStatus": "ACTIVE",
                "Shards": [
                    {
                        "ShardId": "shard-1",
                        "HashKeyRange": {"StartingHashKey": "0", "EndingHashKey": "100"},
                        "SequenceNumberRange": {"StartingSequenceNumber": "001"}
                    },
                    {
                        "ShardId": "shard-2",
                        "HashKeyRange": {"StartingHashKey": "100", "EndingHashKey": "200"},
                        "SequenceNumberRange": {"StartingSequenceNumber": "002"}
                    }
                ]
            }
        }

        result = self.integration.get_shards("test-stream")

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["ShardId"], "shard-1")

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

    def test_split_shard(self):
        """Test splitting a shard"""
        self.mock_kinesis_client.split_shard.return_value = {}
        self.mock_kinesis_client.describe_stream.return_value = {
            "StreamDescription": {
                "StreamName": "test-stream",
                "StreamARN": "arn:aws:kinesis:us-east-1:123456789:stream/test-stream",
                "StreamStatus": "ACTIVE",
                "Shards": []
            }
        }

        result = self.integration.split_shard("test-stream", "shard-1", "50")

        self.mock_kinesis_client.split_shard.assert_called_once_with(
            StreamName="test-stream",
            ShardToSplit="shard-1",
            NewStartingHashKey="50"
        )

    def test_mergeate_shards(self):
        """Test merging shards"""
        self.mock_kinesis_client.mergeate_shards.return_value = {}
        self.mock_kinesis_client.describe_stream.return_value = {
            "StreamDescription": {
                "StreamName": "test-stream",
                "StreamARN": "arn:aws:kinesis:us-east-1:123456789:stream/test-stream",
                "StreamStatus": "ACTIVE",
                "Shards": []
            }
        }

        result = self.integration.mergeate_shards("test-stream", "shard-1", "shard-2")

        self.mock_kinesis_client.mergeate_shards.assert_called_once_with(
            StreamName="test-stream",
            ShardToMerge="shard-1",
            AdjacentShardToMerge="shard-2"
        )

    # ========================================================================
    # DATA OPERATIONS TESTS
    # ========================================================================

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

    def test_read_stream(self):
        """Test reading records from a stream"""
        self.mock_kinesis_client.get_shard_iterator.return_value = {
            "ShardIterator": "iterator-1"
        }
        self.mock_kinesis_client.get_records.side_effect = [
            {
                "Records": [{"Data": b"data1", "SequenceNumber": "001"}],
                "NextShardIterator": "iterator-2",
                "MillisBehindLatest": 1000
            },
            {
                "Records": [],
                "NextShardIterator": None,
                "MillisBehindLatest": 0
            }
        ]

        result = self.integration.read_stream(
            stream_name="test-stream",
            shard_id="shard-1"
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["Data"], b"data1")

    def test_get_record_decoder_bytes(self):
        """Test decoding record with bytes data"""
        record = {"Data": b'{"key": "value"}'}
        result = self.integration.get_record_decoder(record)
        self.assertEqual(result, {"key": "value"})

    def test_get_record_decoder_string(self):
        """Test decoding record with string data"""
        record = {"Data": "plain string"}
        result = self.integration.get_record_decoder(record)
        self.assertEqual(result, "plain string")

    # ========================================================================
    # ENHANCED FAN-OUT (CONSUMERS) TESTS
    # ========================================================================

    def test_register_consumer(self):
        """Test registering a consumer"""
        self.mock_kinesis_client.register_stream_consumer.return_value = {
            "Consumer": {
                "ConsumerName": "my-consumer",
                "ConsumerARN": "arn:aws:kinesis:us-east-1:123456789:consumer/my-stream:my-consumer",
                "ConsumerStatus": "ACTIVE"
            }
        }

        result = self.integration.register_consumer(
            stream_arn="arn:aws:kinesis:us-east-1:123456789:stream/my-stream",
            consumer_name="my-consumer"
        )

        self.assertEqual(result.consumer_name, "my-consumer")
        self.assertEqual(result.consumer_arn, "arn:aws:kinesis:us-east-1:123456789:consumer/my-stream:my-consumer")

    def test_list_consumers(self):
        """Test listing consumers"""
        mock_paginator = MagicMock()
        self.mock_kinesis_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {"Consumers": [{"ConsumerName": "consumer1"}, {"ConsumerName": "consumer2"}]}
        ]

        result = self.integration.list_consumers(stream_name="test-stream")

        self.assertEqual(len(result), 2)
        self.mock_kinesis_client.describe_stream.assert_called_once()

    def test_deregister_consumer(self):
        """Test deregistering a consumer"""
        self.mock_kinesis_client.deregister_stream_consumer.return_value = {}
        self.mock_kinesis_client.describe_stream.return_value = {
            "StreamDescription": {
                "StreamName": "test-stream",
                "StreamARN": "arn:aws:kinesis:us-east-1:123456789:stream/test-stream",
                "StreamStatus": "ACTIVE",
                "Shards": []
            }
        }

        result = self.integration.deregister_consumer(
            consumer_name="my-consumer",
            stream_name="test-stream"
        )

        self.mock_kinesis_client.deregister_stream_consumer.assert_called_once()

    # ========================================================================
    # KINESIS DATA ANALYTICS TESTS
    # ========================================================================

    def test_create_application(self):
        """Test creating an analytics application"""
        self.mock_analytics_client.create_application.return_value = {
            "ApplicationDetail": {
                "ApplicationARN": "arn:aws:kinesisanalytics:us-east-1:123456789:application/my-app"
            }
        }

        config = AnalyticsConfig(name="my-app", runtime_environment="FLINK-1_11")
        result = self.integration.create_application(config)

        self.assertIn("ApplicationDetail", result)
        self.mock_analytics_client.create_application.assert_called_once()

    def test_describe_application(self):
        """Test describing an analytics application"""
        self.mock_analytics_client.describe_application.return_value = {
            "ApplicationDetail": {
                "ApplicationName": "my-app",
                "ApplicationARN": "arn:aws:kinesisanalytics:us-east-1:123456789:application/my-app"
            }
        }

        result = self.integration.describe_application("my-app")

        self.assertEqual(result["ApplicationName"], "my-app")

    def test_list_applications(self):
        """Test listing analytics applications"""
        mock_paginator = MagicMock()
        self.mock_analytics_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {"ApplicationSummaries": [{"ApplicationName": "app1"}, {"ApplicationName": "app2"}]}
        ]

        result = self.integration.list_applications()

        self.assertEqual(len(result), 2)

    def test_delete_application(self):
        """Test deleting an analytics application"""
        self.mock_analytics_client.delete_application.return_value = {}

        result = self.integration.delete_application("my-app")

        self.mock_analytics_client.delete_application.assert_called_once_with(ApplicationName="my-app")

    def test_start_application(self):
        """Test starting an analytics application"""
        self.mock_analytics_client.start_application.return_value = {}

        result = self.integration.start_application("my-app")

        self.mock_analytics_client.start_application.assert_called_once_with(ApplicationName="my-app")

    def test_stop_application(self):
        """Test stopping an analytics application"""
        self.mock_analytics_client.stop_application.return_value = {}

        result = self.integration.stop_application("my-app")

        self.mock_analytics_client.stop_application.assert_called_once_with(ApplicationName="my-app")

    # ========================================================================
    # KINESIS DATA FIREHOSE TESTS
    # ========================================================================

    def test_create_firehose_delivery_stream(self):
        """Test creating a Firehose delivery stream"""
        self.mock_firehose_client.create_delivery_stream.return_value = {
            "DeliveryStreamARN": "arn:aws:firehose:us-east-1:123456789:deliverystream/my-firehose"
        }

        config = FirehoseConfig(name="my-firehose")
        result = self.integration.create_firehose_delivery_stream(config)

        self.assertEqual(result, "arn:aws:firehose:us-east-1:123456789:deliverystream/my-firehose")

    def test_describe_firehose_delivery_stream(self):
        """Test describing a Firehose delivery stream"""
        self.mock_firehose_client.describe_delivery_stream.return_value = {
            "DeliveryStreamDescription": {
                "DeliveryStreamName": "my-firehose",
                "DeliveryStreamARN": "arn:aws:firehose:us-east-1:123456789:deliverystream/my-firehose"
            }
        }

        result = self.integration.describe_firehose_delivery_stream("my-firehose")

        self.assertEqual(result["DeliveryStreamName"], "my-firehose")

    def test_list_firehose_delivery_streams(self):
        """Test listing Firehose delivery streams"""
        mock_paginator = MagicMock()
        self.mock_firehose_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {"DeliveryStreamNames": ["firehose1", "firehose2"]}
        ]

        result = self.integration.list_firehose_delivery_streams()

        self.assertEqual(result, ["firehose1", "firehose2"])

    def test_delete_firehose_delivery_stream(self):
        """Test deleting a Firehose delivery stream"""
        self.mock_firehose_client.delete_delivery_stream.return_value = {}

        result = self.integration.delete_firehose_delivery_stream("my-firehose")

        self.mock_firehose_client.delete_delivery_stream.assert_called_once_with(
            DeliveryStreamName="my-firehose"
        )

    def test_put_to_firehose(self):
        """Test putting a record to Firehose"""
        self.mock_firehose_client.put_record.return_value = {"RecordId": "record-1"}

        result = self.integration.put_to_firehose("my-firehose", {"key": "value"})

        self.assertEqual(result["RecordId"], "record-1")

    def test_update_firehose_destination(self):
        """Test updating Firehose destination"""
        self.mock_firehose_client.update_destination.return_value = {}

        result = self.integration.update_firehose_destination(
            delivery_stream_name="my-firehose",
            s3_destination_arn="arn:aws:s3:::my-bucket",
            role_arn="arn:aws:iam::123456789:role/my-role"
        )

        self.mock_firehose_client.update_destination.assert_called_once()

    # ========================================================================
    # KINESIS VIDEO STREAMS TESTS
    # ========================================================================

    def test_create_video_stream(self):
        """Test creating a video stream"""
        self.mock_video_client.create_stream.return_value = {
            "StreamARN": "arn:aws:kinesisvideo:us-east-1:123456789:stream/my-video-stream"
        }

        config = VideoStreamConfig(name="my-video-stream")
        result = self.integration.create_video_stream(config)

        self.assertEqual(result, "arn:aws:kinesisvideo:us-east-1:123456789:stream/my-video-stream")

    def test_describe_video_stream(self):
        """Test describing a video stream"""
        self.mock_video_client.describe_stream.return_value = {
            "StreamInfo": {
                "StreamName": "my-video-stream",
                "StreamARN": "arn:aws:kinesisvideo:us-east-1:123456789:stream/my-video-stream"
            }
        }

        result = self.integration.describe_video_stream(stream_name="my-video-stream")

        self.assertEqual(result["StreamName"], "my-video-stream")

    def test_list_video_streams(self):
        """Test listing video streams"""
        mock_paginator = MagicMock()
        self.mock_video_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {"StreamInfoList": [{"StreamName": "video1"}, {"StreamName": "video2"}]}
        ]

        result = self.integration.list_video_streams()

        self.assertEqual(len(result), 2)

    def test_delete_video_stream(self):
        """Test deleting a Kinesis Video Stream"""
        self.mock_video_client.delete_stream.return_value = {}

        result = self.integration.delete_video_stream("my-video-stream")

        self.mock_video_client.delete_stream.assert_called_once()

    def test_get_video_streaming_endpoint(self):
        """Test getting video streaming endpoint"""
        self.mock_video_client.get_data_endpoint.return_value = {
            "DataEndpoint": "https://video-endpoint.example.com"
        }

        result = self.integration.get_video_streaming_endpoint(stream_name="my-video-stream")

        self.assertEqual(result, "https://video-endpoint.example.com")

    def test_get_hls_streaming_session_url(self):
        """Test getting HLS streaming session URL"""
        self.mock_video_client.get_hls_streaming_session_url.return_value = {
            "HLSStreamingSessionURL": "https://example.com/hls/stream.m3u8"
        }

        result = self.integration.get_hls_streaming_session_url(stream_name="my-video-stream")

        self.assertEqual(result, "https://example.com/hls/stream.m3u8")

    # ========================================================================
    # LAMBDA EVENT SOURCE MAPPING TESTS
    # ========================================================================

    def test_create_event_source_mapping(self):
        """Test creating Lambda event source mapping"""
        self.mock_lambda_client.create_event_source_mapping.return_value = {
            "UUID": "test-uuid-123"
        }

        config = LambdaEventSourceMappingConfig(
            function_name="my-function",
            stream_arn="arn:aws:kinesis:us-east-1:123456789:stream/my-stream"
        )
        result = self.integration.create_event_source_mapping(config)

        self.assertEqual(result, "test-uuid-123")

    def test_list_event_source_mappings(self):
        """Test listing Lambda event source mappings"""
        self.mock_lambda_client.list_event_source_mappings.return_value = {
            "EventSourceMappings": [{"UUID": "uuid-1"}, {"UUID": "uuid-2"}]
        }

        result = self.integration.list_event_source_mappings(stream_arn="arn:aws:kinesis:us-east-1:123456789:stream/my-stream")

        self.assertEqual(len(result), 2)

    def test_get_event_source_mapping(self):
        """Test getting a Lambda event source mapping"""
        self.mock_lambda_client.get_event_source_mapping.return_value = {
            "UUID": "test-uuid-123",
            "FunctionName": "my-function"
        }

        result = self.integration.get_event_source_mapping("test-uuid-123")

        self.assertEqual(result["UUID"], "test-uuid-123")

    def test_update_event_source_mapping(self):
        """Test updating Lambda event source mapping"""
        self.mock_lambda_client.update_event_source_mapping.return_value = {
            "UUID": "test-uuid-123",
            "BatchSize": 500
        }

        result = self.integration.update_event_source_mapping("test-uuid-123", batch_size=500)

        self.assertEqual(result["BatchSize"], 500)

    def test_delete_event_source_mapping(self):
        """Test deleting Lambda event source mapping"""
        self.mock_lambda_client.delete_event_source_mapping.return_value = {}

        result = self.integration.delete_event_source_mapping("test-uuid-123")

        self.mock_lambda_client.delete_event_source_mapping.assert_called_once_with(UUID="test-uuid-123")

    # ========================================================================
    # SERVER-SIDE ENCRYPTION TESTS
    # ========================================================================

    def test_enable_stream_encryption(self):
        """Test enabling stream encryption"""
        self.mock_kinesis_client.start_stream_encryption.return_value = {}

        result = self.integration.enable_stream_encryption("test-stream", "my-key-id")

        self.mock_kinesis_client.start_stream_encryption.assert_called_once_with(
            StreamName="test-stream",
            EncryptionType="KMS",
            KeyId="my-key-id"
        )

    def test_disable_stream_encryption(self):
        """Test disabling stream encryption"""
        self.mock_kinesis_client.stop_stream_encryption.return_value = {}

        result = self.integration.disable_stream_encryption("test-stream")

        self.mock_kinesis_client.stop_stream_encryption.assert_called_once_with(
            StreamName="test-stream",
            EncryptionType="KMS"
        )

    def test_describe_stream_encryption(self):
        """Test describing stream encryption"""
        self.mock_kinesis_client.describe_stream_summary.return_value = {
            "StreamDescriptionSummary": {
                "EncryptionType": "KMS"
            }
        }

        result = self.integration.describe_stream_encryption("test-stream")

        self.assertEqual(result, "KMS")

    # ========================================================================
    # CLOUDWATCH METRICS TESTS
    # ========================================================================

    def test_flush_metrics(self):
        """Test flushing metrics to CloudWatch"""
        self.integration._metrics_buffer = [
            {"MetricName": "TestMetric", "Value": 1.0}
        ]
        self.mock_cloudwatch_client.put_metric_data.return_value = {}

        self.integration.flush_metrics()

        self.mock_cloudwatch_client.put_metric_data.assert_called_once()
        self.assertEqual(len(self.integration._metrics_buffer), 0)

    def test_get_stream_metrics(self):
        """Test getting stream metrics"""
        self.mock_cloudwatch_client.get_metric_statistics.return_value = {
            "Datapoints": [{"Sum": 100, "Average": 50}]
        }

        result = self.integration.get_stream_metrics(
            stream_name="test-stream",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 2)
        )

        self.assertIn("IncomingRecords", result)

    def test_get_consumer_metrics(self):
        """Test getting consumer metrics"""
        self.mock_cloudwatch_client.get_metric_statistics.return_value = {
            "Datapoints": [{"Sum": 50}]
        }

        result = self.integration.get_consumer_metrics(
            consumer_arn="arn:aws:kinesis:us-east-1:123456789:consumer/my-stream:my-consumer",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 2)
        )

        self.assertIn("IncomingRecords", result)

    def test_put_custom_metric(self):
        """Test putting custom metric"""
        self.integration.put_custom_metric("CustomMetric", 42.0, "Count")

        self.assertEqual(len(self.integration._metrics_buffer), 1)
        self.assertEqual(self.integration._metrics_buffer[0]["MetricName"], "CustomMetric")

    # ========================================================================
    # UTILITY METHODS TESTS
    # ========================================================================

    def test_get_stream_arn(self):
        """Test getting stream ARN"""
        self.mock_kinesis_client.describe_stream.return_value = {
            "StreamDescription": {
                "StreamName": "test-stream",
                "StreamARN": "arn:aws:kinesis:us-east-1:123456789:stream/test-stream",
                "StreamStatus": "ACTIVE",
                "Shards": []
            }
        }

        result = self.integration.get_stream_arn("test-stream")

        self.assertEqual(result, "arn:aws:kinesis:us-east-1:123456789:stream/test-stream")

    def test_get_consumer_arn(self):
        """Test getting consumer ARN"""
        mock_paginator = MagicMock()
        self.mock_kinesis_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {"Consumers": [
                {"ConsumerName": "my-consumer", "ConsumerARN": "arn:aws:kinesis:consumer/my-consumer"}
            ]}
        ]
        self.mock_kinesis_client.describe_stream.return_value = {
            "StreamDescription": {
                "StreamName": "test-stream",
                "StreamARN": "arn:aws:kinesis:us-east-1:123456789:stream/test-stream",
                "StreamStatus": "ACTIVE",
                "Shards": []
            }
        }

        result = self.integration.get_consumer_arn("test-stream", "my-consumer")

        self.assertEqual(result, "arn:aws:kinesis:consumer/my-consumer")

    def test_hash_key_to_partition_key(self):
        """Test hash key to partition key mapping"""
        partition_keys = ["pk1", "pk2", "pk3"]
        result = self.integration.hash_key_to_partition_key("50", partition_keys)
        self.assertIsInstance(result, str)

    def test_compute_sequence_number(self):
        """Test computing sequence number"""
        result = self.integration.compute_sequence_number("test-data")
        self.assertEqual(len(result), 32)
        self.assertTrue(result.isdigit())

    def test_health_check(self):
        """Test health check"""
        mock_paginator = MagicMock()
        self.mock_kinesis_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{"StreamNames": ["stream1"]}]
        self.mock_firehose_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{"DeliveryStreamNames": ["firehose1"]}]
        self.mock_analytics_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{"ApplicationSummaries": []}]
        self.mock_video_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{"StreamInfoList": []}]
        self.mock_cloudwatch_client.list_metrics.return_value = {}

        result = self.integration.health_check()

        self.assertTrue(result["kinesis"])
        self.assertTrue(result["firehose"])
        self.assertTrue(result["healthy"])

    def test_close(self):
        """Test close method"""
        self.integration._metrics_buffer = [{"MetricName": "Test"}]
        self.mock_cloudwatch_client.put_metric_data.return_value = {}

        self.integration.close()

        self.assertEqual(len(self.integration._metrics_buffer), 0)

    # ========================================================================
    # INTERNAL METHODS TESTS
    # ========================================================================

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
