"""
Data Publisher Action Module

Multi-destination data publishing with format conversion,
batching, and delivery confirmation.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class PublishFormat(Enum):
    """Output formats for publishing."""
    
    JSON = "json"
    CSV = "csv"
    XML = "xml"
    YAML = "yaml"
    PARQUET = "parquet"


class DeliveryStatus(Enum):
    """Message delivery status."""
    
    PENDING = "pending"
    SENT = "sent"
    DELIVERED = "delivered"
    FAILED = "failed"


@dataclass
class Publisher:
    """Publisher destination configuration."""
    
    id: str
    name: str
    endpoint: str
    format: PublishFormat = PublishFormat.JSON
    enabled: bool = True
    batch_size: int = 100
    flush_interval_seconds: float = 60
    retry_count: int = 3
    timeout_seconds: float = 30
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PublishedMessage:
    """Published message record."""
    
    message_id: str
    publisher_id: str
    status: DeliveryStatus
    sent_at: float = field(default_factory=time.time)
    delivered_at: Optional[float] = None
    error: Optional[str] = None
    attempts: int = 1


@dataclass
class PublishResult:
    """Result of publish operation."""
    
    success: bool
    messages: List[PublishedMessage]
    total_records: int
    duration_ms: float


class FormatConverter:
    """Converts data between formats."""
    
    @staticmethod
    def to_json(data: Any, pretty: bool = False) -> str:
        """Convert to JSON."""
        if pretty:
            return json.dumps(data, indent=2, ensure_ascii=False)
        return json.dumps(data, ensure_ascii=False)
    
    @staticmethod
    def to_csv(data: List[Dict]) -> str:
        """Convert list of dicts to CSV."""
        if not data:
            return ""
        
        import csv
        import io
        
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
        return output.getvalue()
    
    @staticmethod
    def to_xml(data: Any, root_tag: str = "data") -> str:
        """Convert to XML."""
        def dict_to_xml(d: Dict, tag: str) -> str:
            items = []
            for key, value in d.items():
                if isinstance(value, dict):
                    items.append(f"<{key}>{dict_to_xml(value, key)}</{key}>")
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            items.append(f"<{key}>{dict_to_xml(item, key)}</{key}>")
                        else:
                            items.append(f"<{key}>{item}</{key}>")
                else:
                    items.append(f"<{key}>{value}</{key}>")
            return "".join(items)
        
        if isinstance(data, list):
            items = "".join(
                f"<item>{dict_to_xml(item, 'item')}</item>"
                if isinstance(item, dict) else f"<item>{item}</item>"
                for item in data
            )
            return f"<{root_tag}>{items}</{root_tag}>"
        
        return f"<{root_tag}>{dict_to_xml(data, root_tag)}</{root_tag}>"


class DataPublisher:
    """Core publishing logic."""
    
    def __init__(self, publisher: Publisher):
        self.publisher = publisher
        self._buffer: List[Dict] = []
        self._last_flush = time.time()
        self._messages: Dict[str, PublishedMessage] = {}
        self._converter = FormatConverter()
    
    async def publish(
        self,
        data: Any,
        client: Callable
    ) -> PublishedMessage:
        """Publish data to destination."""
        message_id = str(uuid.uuid4())
        
        message = PublishedMessage(
            message_id=message_id,
            publisher_id=self.publisher.id,
            status=DeliveryStatus.PENDING
        )
        
        formatted_data = self._format_data(data)
        
        for attempt in range(self.publisher.retry_count):
            try:
                response = await self._send(
                    formatted_data,
                    client,
                    attempt + 1
                )
                
                if response:
                    message.status = DeliveryStatus.DELIVERED
                    message.delivered_at = time.time()
                    break
            
            except Exception as e:
                message.attempts = attempt + 1
                message.error = str(e)
                
                if attempt == self.publisher.retry_count - 1:
                    message.status = DeliveryStatus.FAILED
        
        self._messages[message_id] = message
        return message
    
    def _format_data(self, data: Any) -> str:
        """Format data according to publisher format."""
        if self.publisher.format == PublishFormat.JSON:
            return self._converter.to_json(data)
        elif self.publisher.format == PublishFormat.CSV:
            return self._converter.to_csv(data)
        elif self.publisher.format == PublishFormat.XML:
            return self._converter.to_xml(data)
        return str(data)
    
    async def _send(
        self,
        data: str,
        client: Callable,
        attempt: int
    ) -> bool:
        """Send data to endpoint."""
        if self.publisher.format == PublishFormat.JSON:
            body = json.loads(data) if isinstance(data, str) else data
        else:
            body = data
        
        response = await client({
            "url": self.publisher.endpoint,
            "method": "POST",
            "body": body,
            "timeout": self.publisher.timeout_seconds
        })
        
        return 200 <= response.get("status_code", 500) < 300


class DataPublisherAction:
    """
    Main data publisher action handler.
    
    Provides multi-destination publishing with format conversion,
    batching, and delivery confirmation.
    """
    
    def __init__(self):
        self._publishers: Dict[str, Publisher] = {}
        self._client: Optional[Callable] = None
        self._middleware: List[Callable] = []
    
    def add_publisher(
        self,
        name: str,
        endpoint: str,
        format: PublishFormat = PublishFormat.JSON,
        **kwargs
    ) -> str:
        """Add a publisher destination."""
        publisher_id = str(uuid.uuid4())
        
        publisher = Publisher(
            id=publisher_id,
            name=name,
            endpoint=endpoint,
            format=format,
            **kwargs
        )
        
        self._publishers[publisher_id] = publisher
        return publisher_id
    
    def remove_publisher(self, publisher_id: str) -> bool:
        """Remove a publisher."""
        if publisher_id in self._publishers:
            del self._publishers[publisher_id]
            return True
        return False
    
    def get_publisher(self, publisher_id: str) -> Optional[Publisher]:
        """Get publisher by ID."""
        return self._publishers.get(publisher_id)
    
    def set_client(self, client: Callable) -> None:
        """Set HTTP client for publishing."""
        self._client = client
    
    async def publish(
        self,
        data: Any,
        publisher_ids: Optional[List[str]] = None,
        wait_for_delivery: bool = False
    ) -> Dict[str, PublishResult]:
        """Publish data to one or more publishers."""
        if publisher_ids is None:
            publisher_ids = [
                pid for pid, p in self._publishers.items()
                if p.enabled
            ]
        
        start_time = time.time()
        results = {}
        
        for publisher_id in publisher_ids:
            publisher = self._publishers.get(publisher_id)
            if not publisher or not publisher.enabled:
                continue
            
            data_publisher = DataPublisher(publisher)
            
            message = await data_publisher.publish(
                data,
                self._client or self._default_client
            )
            
            duration_ms = (time.time() - start_time) * 1000
            
            results[publisher_id] = PublishResult(
                success=message.status == DeliveryStatus.DELIVERED,
                messages=[message],
                total_records=1,
                duration_ms=duration_ms
            )
        
        return results
    
    async def _default_client(self, request: Dict) -> Dict:
        """Default HTTP client stub."""
        return {"status_code": 200}
    
    async def publish_batch(
        self,
        records: List[Dict],
        publisher_ids: Optional[List[str]] = None
    ) -> Dict[str, PublishResult]:
        """Publish multiple records as batch."""
        if publisher_ids is None:
            publisher_ids = [
                pid for pid, p in self._publishers.items()
                if p.enabled
            ]
        
        start_time = time.time()
        results = {}
        
        for publisher_id in publisher_ids:
            publisher = self._publishers.get(publisher_id)
            if not publisher or not publisher.enabled:
                continue
            
            messages = []
            
            for record in records:
                data_publisher = DataPublisher(publisher)
                message = await data_publisher.publish(
                    record,
                    self._client or self._default_client
                )
                messages.append(message)
            
            duration_ms = (time.time() - start_time) * 1000
            
            successful = sum(
                1 for m in messages
                if m.status == DeliveryStatus.DELIVERED
            )
            
            results[publisher_id] = PublishResult(
                success=successful == len(messages),
                messages=messages,
                total_records=len(records),
                duration_ms=duration_ms
            )
        
        return results
    
    def add_middleware(self, func: Callable) -> None:
        """Add publishing middleware."""
        self._middleware.append(func)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get publisher statistics."""
        return {
            "total_publishers": len(self._publishers),
            "enabled_publishers": sum(1 for p in self._publishers.values() if p.enabled),
            "publishers": [
                {
                    "id": p.id,
                    "name": p.name,
                    "format": p.format.value,
                    "enabled": p.enabled
                }
                for p in self._publishers.values()
            ]
        }
