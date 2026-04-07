"""
HTTP Stream Handler Action Module.

Handles streaming HTTP requests and responses, server-sent events,
chunked transfer encoding, and progressive data processing.

Example:
    >>> from http_stream_action import StreamClient, SSEHandler
    >>> client = StreamClient()
    >>> async for chunk in client.stream_get("https://api.example.com/stream"):
    ...     print(chunk)
"""
from __future__ import annotations

import asyncio
import json
import time
import urllib.request
import urllib.parse
import urllib.error
from dataclasses import dataclass
from typing import Any, AsyncGenerator, Callable, Optional


@dataclass
class StreamConfig:
    """Configuration for streaming requests."""
    url: str
    method: str = "GET"
    headers: dict[str, str] = None
    timeout: float = 60.0
    chunk_size: int = 1024
    auto_reconnect: bool = True
    max_retries: int = 3


@dataclass
class StreamChunk:
    """A chunk of streamed data."""
    data: bytes
    timestamp: float
    is_final: bool = False
    error: Optional[str] = None


class SSEHandler:
    """Parse Server-Sent Events (SSE) stream."""

    def __init__(self):
        self._data_buffer = ""
        self._event_type = ""
        self._event_id = ""
        self._retry = 3000

    def parse(self, text: str) -> list[dict[str, Any]]:
        """
        Parse SSE-formatted text into events.

        Returns:
            List of event dictionaries with type, data, id, retry
        """
        events: list[dict[str, Any]] = []
        lines = text.split("\n")

        event: dict[str, Any] = {}
        data_lines: list[str] = []

        for line in lines:
            if line.startswith(":"):
                continue
            if ":" in line:
                field, _, value = line.partition(":")
                field = field.strip()
                value = value.strip()
            else:
                field = line.strip()
                value = ""

            if field == "event":
                event["type"] = value or "message"
            elif field == "data":
                data_lines.append(value)
            elif field == "id":
                event["id"] = value
            elif field == "retry":
                try:
                    event["retry"] = int(value)
                except ValueError:
                    pass
            elif field == "":
                if data_lines:
                    event["data"] = "\n".join(data_lines)
                    events.append(event)
                    event = {}
                    data_lines = []
            else:
                event[field] = value

        if data_lines:
            event["data"] = "\n".join(data_lines)
            events.append(event)

        return events

    def parse_line(self, line: str) -> Optional[dict[str, Any]]:
        """Parse a single SSE line."""
        if line.startswith(":"):
            return None
        if ":" in line:
            field, _, value = line.partition(":")
            field = field.strip()
            value = value.strip()
        else:
            field = line.strip()
            value = ""

        if field == "event":
            self._event_type = value or "message"
        elif field == "data":
            self._data_buffer += value + "\n"
        elif field == "id":
            self._event_id = value
        elif field == "retry":
            try:
                self._retry = int(value)
            except ValueError:
                pass
        elif field == "":
            if self._data_buffer:
                event = {
                    "type": self._event_type or "message",
                    "data": self._data_buffer.rstrip("\n"),
                    "id": self._event_id,
                    "retry": self._retry,
                }
                self._data_buffer = ""
                return event
        return None

    def reset(self) -> None:
        """Reset parser state."""
        self._data_buffer = ""
        self._event_type = ""
        self._event_id = ""


class StreamClient:
    """Async streaming HTTP client."""

    def __init__(self):
        self._sse = SSEHandler()
        self._active = False

    async def stream_get(
        self,
        url: str,
        headers: Optional[dict[str, str]] = None,
        timeout: float = 60.0,
    ) -> AsyncGenerator[StreamChunk, None]:
        """Stream GET request with chunked transfer."""
        yield from await self._stream_request(
            StreamConfig(url=url, method="GET", headers=headers, timeout=timeout)
        )

    async def stream_post(
        self,
        url: str,
        data: Optional[bytes] = None,
        json_data: Optional[Any] = None,
        headers: Optional[dict[str, str]] = None,
        timeout: float = 60.0,
    ) -> AsyncGenerator[StreamChunk, None]:
        """Stream POST request."""
        body = data
        if json_data is not None:
            body = json.dumps(json_data, default=str).encode("utf-8")
            headers = dict(headers or {})
            headers["Content-Type"] = "application/json"
        yield from await self._stream_request(
            StreamConfig(url=url, method="POST", headers=headers, timeout=timeout)
        )

    async def stream_sse(
        self,
        url: str,
        headers: Optional[dict[str, str]] = None,
        timeout: float = 60.0,
        on_event: Optional[Callable[[dict[str, Any]], None]] = None,
    ) -> AsyncGenerator[dict[str, Any], None]:
        """
        Stream Server-Sent Events.

        Yields parsed event dictionaries.
        """
        self._sse.reset()
        chunk_text = ""
        async for chunk in self._stream_request(
            StreamConfig(url=url, method="GET", headers=headers, timeout=timeout)
        ):
            if chunk.error:
                yield {"type": "error", "data": chunk.error}
                return
            chunk_text += chunk.data.decode("utf-8", errors="replace")
            lines = chunk_text.split("\n")
            chunk_text = lines[-1]
            for line in lines[:-1]:
                event = self._sse.parse_line(line)
                if event:
                    if on_event:
                        on_event(event)
                    yield event

        if chunk_text:
            event = self._sse.parse_line(chunk_text)
            if event:
                if on_event:
                    on_event(event)
                yield event

    async def _stream_request(
        self,
        config: StreamConfig,
    ) -> AsyncGenerator[StreamChunk, None]:
        headers = dict(config.headers or {})
        headers.setdefault("Accept", "text/event-stream")
        headers.setdefault("Cache-Control", "no-cache")

        req = urllib.request.Request(
            config.url,
            method=config.method,
            headers=headers,
        )

        try:
            with urllib.request.urlopen(req, timeout=config.timeout) as resp:
                while True:
                    chunk = resp.read(config.chunk_size)
                    if not chunk:
                        yield StreamChunk(
                            data=b"",
                            timestamp=time.monotonic(),
                            is_final=True,
                        )
                        return
                    yield StreamChunk(
                        data=chunk,
                        timestamp=time.monotonic(),
                        is_final=False,
                    )
        except urllib.error.HTTPError as e:
            yield StreamChunk(
                data=b"",
                timestamp=time.monotonic(),
                is_final=True,
                error=f"HTTP {e.code}: {e.reason}",
            )
        except Exception as e:
            yield StreamChunk(
                data=b"",
                timestamp=time.monotonic(),
                is_final=True,
                error=str(e),
            )

    async def download_file(
        self,
        url: str,
        output_path: str,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> tuple[bool, str]:
        """
        Download file with progress reporting.

        Returns:
            Tuple of (success, message)
        """
        total_size = 0
        downloaded = 0
        headers = dict(headers or {})

        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=60) as resp:
                total_size = int(resp.headers.get("Content-Length", 0))
                with open(output_path, "wb") as f:
                    while True:
                        chunk = resp.read(8192)
                        if not chunk:
                            break
                        f.write(chunk)
                        downloaded += len(chunk)
                        if progress_callback:
                            progress_callback(downloaded, total_size)
            return True, f"Downloaded {downloaded} bytes"
        except Exception as e:
            return False, str(e)


if __name__ == "__main__":
    async def test():
        client = StreamClient()
        print("Testing SSE parser:")
        sse = SSEHandler()
        events = sse.parse("data: Hello\ndata: World\n\nevent: custom\ndata: Custom event\n\n")
        for event in events:
            print(f"  {event}")

    asyncio.run(test())
