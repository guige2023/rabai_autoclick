"""API Stream Action Module.

Handles streaming API responses with chunked processing, backpressure,
and real-time event parsing for long-poll and chunked transfer scenarios.
"""

import time
import json
import logging
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterator, List, Optional, Generator

logger = logging.getLogger(__name__)


@dataclass
class StreamChunk:
    chunk_index: int
    data: Any
    timestamp: float
    is_final: bool
    metadata: Dict[str, Any]


@dataclass
class StreamConfig:
    chunk_size: int = 1024
    timeout: float = 30.0
    buffer_size: int = 100
    parse_json: bool = False
    auto_close: bool = True


class APIStreamAction:
    """Processes streaming API responses with configurable chunk handling."""

    def __init__(self, config: Optional[StreamConfig] = None) -> None:
        self._config = config or StreamConfig()
        self._handlers: Dict[str, List[Callable]] = {
            "chunk": [],
            "complete": [],
            "error": [],
            "backpressure": [],
        }
        self._stats = {
            "chunks_received": 0,
            "bytes_received": 0,
            "chunks_processed": 0,
            "errors": 0,
        }

    def stream_request(
        self,
        url: str,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        body: Optional[bytes] = None,
        event_handler: Optional[Callable[[StreamChunk], None]] = None,
    ) -> Generator[StreamChunk, None, None]:
        import urllib.request
        chunks: List[StreamChunk] = []
        index = 0
        try:
            req = urllib.request.Request(
                url,
                data=body,
                headers=headers or {},
                method=method,
            )
            with urllib.request.urlopen(req, timeout=self._config.timeout) as resp:
                buffer = b""
                content_type = resp.headers.get("Content-Type", "")
                while True:
                    chunk = resp.read(self._config.chunk_size)
                    if not chunk:
                        break
                    buffer += chunk
                    self._stats["bytes_received"] += len(chunk)
                    self._stats["chunks_received"] += 1
                    if self._config.parse_json and "application/json" in content_type:
                        try:
                            data = json.loads(buffer.decode("utf-8"))
                            buffer = b""
                        except json.JSONDecodeError:
                            continue
                    else:
                        data = buffer.decode("utf-8", errors="replace")
                        buffer = b""
                    stream_chunk = StreamChunk(
                        chunk_index=index,
                        data=data,
                        timestamp=time.time(),
                        is_final=False,
                        metadata={"content_type": content_type},
                    )
                    chunks.append(stream_chunk)
                    if len(chunks) >= self._config.buffer_size:
                        self._notify("backpressure", chunks[-1])
                    index += 1
                    if event_handler:
                        event_handler(stream_chunk)
                    yield stream_chunk
                final_chunk = StreamChunk(
                    chunk_index=index,
                    data=None,
                    timestamp=time.time(),
                    is_final=True,
                    metadata={"total_chunks": index},
                )
                self._notify("complete", final_chunk)
                yield final_chunk
        except Exception as e:
            self._stats["errors"] += 1
            error_chunk = StreamChunk(
                chunk_index=-1,
                data={"error": str(e)},
                timestamp=time.time(),
                is_final=True,
                metadata={"error": True},
            )
            self._notify("error", error_chunk)
            yield error_chunk

    def register_handler(
        self,
        event: str,
        handler: Callable[[StreamChunk], None],
    ) -> None:
        if event in self._handlers:
            self._handlers[event].append(handler)

    def unregister_handler(
        self,
        event: str,
        handler: Callable[[StreamChunk], None],
    ) -> bool:
        if event in self._handlers and handler in self._handlers[event]:
            self._handlers[event].remove(handler)
            return True
        return False

    def _notify(self, event: str, chunk: StreamChunk) -> None:
        for handler in self._handlers.get(event, []):
            try:
                handler(chunk)
            except Exception as e:
                logger.error(f"Stream handler error for {event}: {e}")

    def get_stats(self) -> Dict[str, Any]:
        return {
            **self._stats,
            "buffer_size": self._config.buffer_size,
            "chunk_size": self._config.chunk_size,
        }

    def reset_stats(self) -> None:
        self._stats = {
            "chunks_received": 0,
            "bytes_received": 0,
            "chunks_processed": 0,
            "errors": 0,
        }

    def parse_sse(self, text: str) -> List[Dict[str, Any]]:
        events = []
        for line in text.split("\n"):
            line = line.strip()
            if not line or line.startswith(":"):
                continue
            if ":" in line:
                field, _, value = line.partition(":")
                events.append({"field": field.strip(), "value": value.strip()})
        return events
