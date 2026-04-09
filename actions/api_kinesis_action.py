"""
API Kinesis Action Module.

Provides Amazon Kinesis Data Streams operations for stream management,
record publishing, consumption, and consumer group coordination.

Author: RabAi Team
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Iterator, Optional


class StreamStatus(Enum):
    """Kinesis stream status."""
    CREATING = "CREATING"
    ACTIVE = "ACTIVE"
    UPDATING = "UPDATING"
    DELETING = "DELETING"


@dataclass
class KinesisConfig:
    """Kinesis client configuration."""
    region: str = "us-east-1"
    endpoint_url: Optional[str] = None
    credentials: Optional[dict[str, str]] = None


@dataclass
class Record:
    """Kinesis record representation."""
    partition_key: str
    sequence_number: str = ""
    data: bytes = b""
    explicit_hash_key: Optional[str] = None
    headers: dict[str, str] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    approximate_arrival_timestamp: float = field(default_factory=time.time)


@dataclass
class Shard:
    """Kinesis shard representation."""
    shard_id: str
    parent_shard_id: Optional[str] = None
    adjacent_parent_shard_id: Optional[str] = None
    hash_key_range_start: str = "0"
    hash_key_range_end: str = "340282366920938463463374607431768211455"
    sequence_number_range_start: str = ""
    sequence_number_range_end: str = ""


@dataclass
class StreamInfo:
    """Kinesis stream information."""
    stream_name: str
    stream_arn: str = ""
    status: StreamStatus = StreamStatus.ACTIVE
    shards: list[Shard] = field(default_factory=list)
    retention_period_hours: int = 24
    stream_creation_timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class PutResult:
    """Result of put operation."""
    shard_id: str
    sequence_number: str
    encryption_type: str = "NONE"


@dataclass
class GetResult:
    """Result of get records operation."""
    records: list[Record] = field(default_factory=list)
    millis_behind_latest: int = 0
    next_shard_iterator: Optional[str] = None


class KinesisClient:
    """Amazon Kinesis Data Streams client."""

    def __init__(self, config: Optional[KinesisConfig] = None):
        self.config = config or KinesisConfig()
        self._streams: dict[str, StreamInfo] = {}
        self._shard_iterators: dict[str, dict[str, Any]] = {}
        self._records: dict[str, list[Record]] = {}

    async def _request(
        self,
        action: str,
        stream_name: str,
        data: Optional[dict] = None,
    ) -> dict[str, Any]:
        """Make API request."""
        await asyncio.sleep(0.02)
        return {"status": 200}

    async def create_stream(
        self,
        stream_name: str,
        shard_count: int = 1,
        retention_period_hours: int = 24,
    ) -> bool:
        """Create a new Kinesis stream."""
        shards = [
            Shard(shard_id=f"{stream_name}/shardId-000000000000")
        ]
        stream = StreamInfo(
            stream_name=stream_name,
            stream_arn=f"arn:aws:kinesis:{self.config.region}::stream/{stream_name}",
            status=StreamStatus.ACTIVE,
            shards=shards,
            retention_period_hours=retention_period_hours,
        )
        self._streams[stream_name] = stream
        self._records[stream_name] = []
        return True

    async def describe_stream(self, stream_name: str) -> StreamInfo:
        """Describe a stream."""
        if stream_name not in self._streams:
            raise Exception(f"Stream {stream_name} not found")
        return self._streams[stream_name]

    async def delete_stream(self, stream_name: str) -> bool:
        """Delete a stream."""
        if stream_name in self._streams:
            del self._streams[stream_name]
        if stream_name in self._records:
            del self._records[stream_name]
        return True

    async def list_streams(self) -> list[str]:
        """List all streams."""
        return list(self._streams.keys())

    async def put_record(
        self,
        stream_name: str,
        data: bytes,
        partition_key: str,
        explicit_hash_key: Optional[str] = None,
        sequence_number_for_ordering: Optional[str] = None,
    ) -> PutResult:
        """Put a single record to stream."""
        if stream_name not in self._records:
            raise Exception(f"Stream {stream_name} not found")
        seq_num = str(uuid.uuid4().int)[:26]
        record = Record(
            partition_key=partition_key,
            sequence_number=seq_num,
            data=data,
            explicit_hash_key=explicit_hash_key,
        )
        self._records[stream_name].append(record)
        shard_id = self._streams[stream_name].shards[0].shard_id if self._streams[stream_name].shards else "shard-0"
        return PutResult(shard_id=shard_id, sequence_number=seq_num)

    async def put_records(
        self,
        stream_name: str,
        records: list[tuple[bytes, str]],
    ) -> dict[str, Any]:
        """Put multiple records to stream."""
        if stream_name not in self._records:
            raise Exception(f"Stream {stream_name} not found")
        results = {"FailedRecordCount": 0, "Records": []}
        for data, partition_key in records:
            result = await self.put_record(stream_name, data, partition_key)
            results["Records"].append({
                "ShardId": result.shard_id,
                "SequenceNumber": result.sequence_number,
                "EncryptionType": "NONE",
            })
        return results

    async def get_shard_iterator(
        self,
        stream_name: str,
        shard_id: str,
        iterator_type: str = "LATEST",
        starting_sequence_number: Optional[str] = None,
    ) -> str:
        """Get shard iterator."""
        iterator = str(uuid.uuid4())
        self._shard_iterators.setdefault(stream_name, {})[shard_id] = {
            "iterator": iterator,
            "type": iterator_type,
            "starting_sequence": starting_sequence_number,
            "timestamp": time.time(),
        }
        return iterator

    async def get_records(
        self,
        stream_name: str,
        shard_iterator: str,
        limit: int = 100,
    ) -> GetResult:
        """Get records from a shard."""
        if stream_name not in self._records:
            return GetResult()
        records = self._records[stream_name][:limit]
        self._records[stream_name] = self._records[stream_name][limit:]
        return GetResult(
            records=records,
            millis_behind_latest=0,
            next_shard_iterator=shard_iterator,
        )

    async def register_consumer(
        self,
        stream_arn: str,
        consumer_name: str,
    ) -> dict[str, Any]:
        """Register a consumer for enhanced fan-out."""
        consumer_arn = f"{stream_arn}/consumer/{consumer_name}"
        return {
            "Consumer": {
                "ConsumerName": consumer_name,
                "ConsumerARN": consumer_arn,
                "ConsumerStatus": "ACTIVE",
                "ConsumerCreationTimestamp": datetime.now(timezone.utc).isoformat(),
            }
        }

    async def subscribe_to_shard(
        self,
        consumer_arn: str,
        shard_iterator: str,
        records_handler: Callable[[list[Record]], Any],
    ) -> None:
        """Subscribe to a shard for enhanced fan-out."""
        while True:
            result = await self.get_records("", shard_iterator)
            if result.records:
                await records_handler(result.records)
            await asyncio.sleep(1)


class KinesisConsumer:
    """Kinesis consumer with checkpointing."""

    def __init__(self, client: KinesisClient, stream_name: str):
        self.client = client
        self.stream_name = stream_name
        self._checkpoint_store: dict[str, str] = {}
        self._shard_iterators: dict[str, str] = {}

    async def start(
        self,
        shard_ids: Optional[list[str]] = None,
        processor: Callable[[list[Record]], Any] = None,
    ) -> None:
        """Start consuming from shards."""
        stream = await self.client.describe_stream(self.stream_name)
        shards_to_read = [s for s in stream.shards if not shard_ids or s.shard_id in shard_ids]

        for shard in shards_to_read:
            iterator = await self.client.get_shard_iterator(
                self.stream_name,
                shard.shard_id,
                "LATEST",
            )
            self._shard_iterators[shard.shard_id] = iterator

        async def consumer_loop():
            while True:
                for shard_id, iterator in self._shard_iterators.items():
                    result = await self.client.get_records(self.stream_name, iterator)
                    if result.records and processor:
                        await processor(result.records)
                    if result.next_shard_iterator:
                        self._shard_iterators[shard_id] = result.next_shard_iterator
                await asyncio.sleep(1)

        asyncio.create_task(consumer_loop())

    def checkpoint(self, shard_id: str, sequence_number: str) -> None:
        """Checkpoint sequence number for a shard."""
        self._checkpoint_store[shard_id] = sequence_number

    def get_checkpoint(self, shard_id: str) -> Optional[str]:
        """Get checkpointed sequence number."""
        return self._checkpoint_store.get(shard_id)


async def demo():
    """Demo Kinesis operations."""
    client = KinesisClient(KinesisConfig(region="us-east-1"))

    await client.create_stream("test-stream", shard_count=1)
    print("Created stream")

    result = await client.put_record(
        "test-stream",
        b'{"event": "test", "data": 123}',
        "partition-key-1",
    )
    print(f"Put record: {result.sequence_number}")

    shard_id = "test-stream/shardId-000000000000"
    iterator = await client.get_shard_iterator("test-stream", shard_id)
    get_result = await client.get_records("test-stream", iterator)
    print(f"Got {len(get_result.records)} records")


if __name__ == "__main__":
    asyncio.run(demo())
