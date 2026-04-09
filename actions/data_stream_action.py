"""Data stream processing action module for RabAI AutoClick.

Provides streaming data operations:
- StreamProducerAction: Produce data to stream
- StreamConsumerAction: Consume data from stream
- StreamProcessorAction: Process stream data
- StreamWindowAction: Windowed stream operations
"""

import sys
import os
import time
import logging
import threading
from typing import Any, Dict, List, Optional, Callable, Generic, TypeVar
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import deque
from enum import Enum

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult

logger = logging.getLogger(__name__)

T = TypeVar('T')


class StreamStatus(Enum):
    """Stream status."""
    IDLE = "idle"
    PRODUCING = "producing"
    CONSUMING = "consuming"
    PAUSED = "paused"
    CLOSED = "closed"


@dataclass
class StreamMessage(Generic[T]):
    """A message in the stream."""
    topic: str
    data: T
    timestamp: datetime = field(default_factory=datetime.now)
    key: Optional[str] = None
    headers: Dict[str, str] = field(default_factory=dict)
    offset: int = 0


class StreamBuffer:
    """In-memory stream buffer."""

    def __init__(self, topic: str, max_size: int = 1000) -> None:
        self.topic = topic
        self.max_size = max_size
        self._buffer: deque = deque(maxlen=max_size)
        self._offset = 0
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)

    def publish(self, message: StreamMessage) -> int:
        with self._lock:
            self._buffer.append(message)
            offset = self._offset + len(self._buffer) - 1
            message.offset = offset
            self._not_empty.notify()
        return offset

    def consume(self, timeout: float = 1.0) -> Optional[StreamMessage]:
        with self._not_empty:
            if not self._buffer:
                self._not_empty.wait(timeout=timeout)
            if self._buffer:
                return self._buffer.popleft()
        return None

    def consume_batch(self, max_count: int, timeout: float = 1.0) -> List[StreamMessage]:
        messages = []
        end_time = time.time() + timeout
        while len(messages) < max_count and time.time() < end_time:
            remaining = end_time - time.time()
            msg = self.consume(timeout=min(remaining, 0.1))
            if msg:
                messages.append(msg)
        return messages

    def seek(self, offset: int) -> None:
        with self._lock:
            self._offset = offset

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "topic": self.topic,
                "size": len(self._buffer),
                "max_size": self.max_size,
                "offset": self._offset
            }


class StreamWindow:
    """Windowed view of a stream."""

    def __init__(self, window_size: int, slide_interval: int) -> None:
        self.window_size = window_size
        self.slide_interval = slide_interval
        self._window: deque = deque(maxlen=window_size)
        self._last_slide = datetime.now()

    def add(self, item: Any) -> Optional[List[Any]]:
        self._window.append(item)
        elapsed = (datetime.now() - self._last_slide).total_seconds()
        if elapsed >= self.slide_interval or len(self._window) >= self.window_size:
            self._last_slide = datetime.now()
            return list(self._window)
        return None

    def get_current(self) -> List[Any]:
        return list(self._window)


class TumblingWindow(StreamWindow):
    """Tumbling window (non-overlapping)."""

    def __init__(self, window_size: int) -> None:
        super().__init__(window_size=window_size, slide_interval=window_size)


class SlidingWindow(StreamWindow):
    """Sliding window (overlapping)."""

    def __init__(self, window_size: int, slide_interval: int) -> None:
        super().__init__(window_size=window_size, slide_interval=slide_interval)


_streams: Dict[str, StreamBuffer] = {}
_stream_status: Dict[str, StreamStatus] = {}


class StreamProducerAction(BaseAction):
    """Produce data to a stream."""
    action_type = "data_stream_producer"
    display_name = "流数据生产者"
    description = "向数据流中发送消息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        topic = params.get("topic", "default")
        data = params.get("data")
        key = params.get("key")
        headers = params.get("headers", {})
        batch = params.get("batch", False)

        if data is None:
            return ActionResult(success=False, message="data参数是必需的")

        if topic not in _streams:
            max_size = params.get("max_size", 1000)
            _streams[topic] = StreamBuffer(topic=topic, max_size=max_size)
            _stream_status[topic] = StreamStatus.IDLE

        buffer = _streams[topic]
        _stream_status[topic] = StreamStatus.PRODUCING

        if batch and isinstance(data, list):
            offsets = []
            for item in data:
                msg = StreamMessage(topic=topic, data=item, key=key, headers=headers)
                offset = buffer.publish(msg)
                offsets.append(offset)
            _stream_status[topic] = StreamStatus.IDLE
            return ActionResult(
                success=True,
                message=f"批量发送 {len(offsets)} 条消息",
                data={"offsets": offsets, "count": len(offsets)}
            )

        msg = StreamMessage(topic=topic, data=data, key=key, headers=headers)
        offset = buffer.publish(msg)
        _stream_status[topic] = StreamStatus.IDLE

        return ActionResult(
            success=True,
            message=f"消息已发送，offset={offset}",
            data={"offset": offset, "topic": topic}
        )


class StreamConsumerAction(BaseAction):
    """Consume data from a stream."""
    action_type = "data_stream_consumer"
    display_name = "流数据消费者"
    description = "从数据流中消费消息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        topic = params.get("topic", "default")
        timeout = params.get("timeout", 1.0)
        max_count = params.get("max_count", 1)
        batch = params.get("batch", False)

        if topic not in _streams:
            return ActionResult(success=False, message=f"主题 {topic} 不存在")

        buffer = _streams[topic]
        _stream_status[topic] = StreamStatus.CONSUMING

        if batch:
            messages = buffer.consume_batch(max_count=max_count, timeout=timeout)
            _stream_status[topic] = StreamStatus.IDLE
            return ActionResult(
                success=True,
                message=f"消费 {len(messages)} 条消息",
                data={
                    "messages": [
                        {"data": m.data, "key": m.key, "offset": m.offset, "timestamp": m.timestamp.isoformat()}
                        for m in messages
                    ],
                    "count": len(messages)
                }
            )

        message = buffer.consume(timeout=timeout)
        _stream_status[topic] = StreamStatus.IDLE

        if message is None:
            return ActionResult(success=True, message="无新消息", data={"message": None})

        return ActionResult(
            success=True,
            message=f"消费消息 offset={message.offset}",
            data={
                "message": {
                    "data": message.data,
                    "key": message.key,
                    "offset": message.offset,
                    "timestamp": message.timestamp.isoformat()
                }
            }
        )


class StreamProcessorAction(BaseAction):
    """Process stream data with transforms."""
    action_type = "data_stream_processor"
    display_name = "流数据处理器"
    description = "对流数据进行转换处理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        topic = params.get("topic", "default")
        operation = params.get("operation", "filter")
        expression = params.get("expression", "")
        field_name = params.get("field_name", "")
        output_topic = params.get("output_topic", topic)

        if topic not in _streams:
            return ActionResult(success=False, message=f"主题 {topic} 不存在")

        buffer = _streams[topic]
        messages = buffer.consume_batch(max_count=100, timeout=0.1)

        if not messages:
            return ActionResult(success=True, message="无消息可处理", data={"processed": 0})

        processed = 0
        results = []

        for msg in messages:
            data = msg.data
            include = True

            if operation == "filter" and expression:
                try:
                    include = eval(expression, {"data": data})
                except Exception as e:
                    include = True

            elif operation == "map" and field_name:
                try:
                    data[field_name] = eval(expression, {"data": data})
                except Exception:
                    pass

            elif operation == "select" and field_name:
                try:
                    data = {k: data[k] for k in field_name.split(",") if k in data}
                except Exception:
                    pass

            if include:
                processed += 1
                results.append(data)
                if output_topic != topic:
                    if output_topic not in _streams:
                        _streams[output_topic] = StreamBuffer(topic=output_topic)
                    _streams[output_topic].publish(StreamMessage(topic=output_topic, data=data))

        return ActionResult(
            success=True,
            message=f"处理完成: {processed}/{len(messages)}",
            data={"processed": processed, "total": len(messages), "results": results[:10]}
        )


class StreamWindowAction(BaseAction):
    """Windowed stream operations."""
    action_type = "data_stream_window"
    display_name = "流数据窗口"
    description = "对流数据执行窗口操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        topic = params.get("topic", "default")
        window_type = params.get("window_type", "tumbling")
        window_size = params.get("window_size", 10)
        slide_interval = params.get("slide_interval", 5)

        if topic not in _streams:
            return ActionResult(success=False, message=f"主题 {topic} 不存在")

        buffer = _streams[topic]
        messages = buffer.consume_batch(max_count=window_size * 2, timeout=0.1)

        if not messages:
            return ActionResult(success=True, message="窗口为空", data={"window": []})

        if window_type == "tumbling":
            window = TumblingWindow(window_size=window_size)
        else:
            window = SlidingWindow(window_size=window_size, slide_interval=slide_interval)

        for msg in messages:
            result = window.add(msg.data)
            if result:
                return ActionResult(
                    success=True,
                    message=f"窗口触发: {len(result)} 条数据",
                    data={"window": result, "type": window_type, "size": len(result)}
                )

        return ActionResult(
            success=True,
            message=f"窗口当前: {len(window.get_current())} 条",
            data={"current": window.get_current(), "type": window_type}
        )
