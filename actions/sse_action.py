"""
Server-Sent Events (SSE) Action Module.

Provides SSE streaming capabilities for real-time unidirectional
data delivery to clients.
"""

from typing import Any, Callable, Dict, Generator, List, Optional
from dataclasses import dataclass, field
from enum import Enum
import time
import threading
import logging
import json

logger = logging.getLogger(__name__)


class SSEEventType(Enum):
    """Standard SSE event types."""
    MESSAGE = "message"
    EVENT = "event"
    COMMENT = "comment"
    ERROR = "error"
    HEARTBEAT = "heartbeat"


@dataclass
class SSEConfig:
    """Server-Sent Events configuration."""
    event_type: str = "message"
    retry_timeout: int = 5000
    heartbeat_interval: int = 30
    max_queue_size: int = 100
    content_type: str = "text/event-stream"
    enable_cors: bool = True
    cors_origins: List[str] = field(default_factory=list)


@dataclass
class SSEClient:
    """Represents a connected SSE client."""
    client_id: str
    event_type: str
    last_event_id: Optional[str] = None
    connected_at: float = field(default_factory=time.time)
    headers: Dict[str, str] = field(default_factory=dict)
    queue: List[str] = field(default_factory=list)


class SSEAction:
    """
    Server-Sent Events action handler.
    
    Manages SSE streams for real-time data delivery.
    
    Example:
        sse = SSEAction()
        sse.add_client("client1", "updates")
        sse.broadcast({"data": "Hello!"})
    """
    
    def __init__(self, config: Optional[SSEConfig] = None):
        """Initialize SSE handler."""
        self.config = config or SSEConfig()
        self._clients: Dict[str, SSEClient] = {}
        self._lock = threading.RLock()
        self._streams: Dict[str, Generator] = {}
    
    def add_client(
        self,
        client_id: str,
        event_type: str = "message",
        headers: Optional[Dict[str, str]] = None,
        last_event_id: Optional[str] = None
    ) -> SSEClient:
        """
        Register a new SSE client.
        
        Args:
            client_id: Unique client identifier
            event_type: Type of events to receive
            headers: Client headers
            last_event_id: Last event ID received (for reconnection)
            
        Returns:
            Created SSEClient
        """
        with self._lock:
            client = SSEClient(
                client_id=client_id,
                event_type=event_type,
                last_event_id=last_event_id,
                headers=headers or {}
            )
            self._clients[client_id] = client
            logger.info(f"SSE client connected: {client_id}")
            return client
    
    def remove_client(self, client_id: str) -> bool:
        """
        Remove a connected SSE client.
        
        Args:
            client_id: Client identifier
            
        Returns:
            True if client was removed
        """
        with self._lock:
            if client_id in self._clients:
                del self._clients[client_id]
                logger.info(f"SSE client disconnected: {client_id}")
                return True
            return False
    
    def send_event(
        self,
        client_id: str,
        data: Any,
        event_type: Optional[str] = None,
        event_id: Optional[str] = None
    ) -> bool:
        """
        Send an event to a specific client.
        
        Args:
            client_id: Target client
            data: Event data
            event_type: Optional event type override
            event_id: Optional event ID for tracking
            
        Returns:
            True if sent successfully
        """
        with self._lock:
            client = self._clients.get(client_id)
            if not client:
                return False
            
            event_data = self._format_event(
                data=data,
                event_type=event_type or client.event_type,
                event_id=event_id
            )
            
            if len(client.queue) >= self.config.max_queue_size:
                client.queue.pop(0)
            
            client.queue.append(event_data)
            return True
    
    def broadcast(
        self,
        data: Any,
        event_type: Optional[str] = None,
        event_id: Optional[str] = None,
        filter_client: Optional[Callable[[SSEClient], bool]] = None
    ) -> int:
        """
        Broadcast an event to all connected clients.
        
        Args:
            data: Event data
            event_type: Optional event type
            event_id: Optional event ID
            filter_client: Optional filter function
            
        Returns:
            Number of clients that received the event
        """
        with self._lock:
            count = 0
            for client in self._clients.values():
                if filter_client and not filter_client(client):
                    continue
                
                if self.send_event(
                    client.client_id,
                    data,
                    event_type,
                    event_id
                ):
                    count += 1
            return count
    
    def _format_event(
        self,
        data: Any,
        event_type: str,
        event_id: Optional[str] = None
    ) -> str:
        """Format data as SSE event string."""
        lines = []
        
        if event_id:
            lines.append(f"id: {event_id}")
        
        lines.append(f"event: {event_type}")
        
        if isinstance(data, dict):
            data = json.dumps(data)
        elif not isinstance(data, str):
            data = str(data)
        
        for line in data.split("\n"):
            lines.append(f"data: {line}")
        
        lines.append("")
        return "\n".join(lines)
    
    def stream_events(
        self,
        client_id: str,
        timeout: Optional[float] = None
    ) -> Generator[str, None, None]:
        """
        Generate SSE event stream for a client.
        
        Args:
            client_id: Client to stream to
            timeout: Optional stream timeout
            
        Yields:
            Formatted SSE event strings
        """
        with self._lock:
            client = self._clients.get(client_id)
            if not client:
                return
            
            retry = f"retry: {self.config.retry_timeout}\n\n"
            yield retry
        
        start_time = time.time()
        
        while True:
            if timeout and (time.time() - start_time) > timeout:
                break
            
            with self._lock:
                client = self._clients.get(client_id)
                if not client:
                    break
                
                while client.queue:
                    event = client.queue.pop(0)
                    yield event + "\n\n"
            
            time.sleep(0.1)
    
    def create_stream(
        self,
        stream_id: str,
        generator: Generator
    ) -> None:
        """
        Register a data generator as an SSE stream.
        
        Args:
            stream_id: Stream identifier
            generator: Data generator function
        """
        with self._lock:
            self._streams[stream_id] = generator
        logger.info(f"SSE stream created: {stream_id}")
    
    def attach_stream_to_client(
        self,
        client_id: str,
        stream_id: str
    ) -> bool:
        """
        Attach a stream to a client for automatic event generation.
        
        Args:
            client_id: Target client
            stream_id: Stream to attach
            
        Returns:
            True if attached successfully
        """
        with self._lock:
            if client_id not in self._clients:
                return False
            if stream_id not in self._streams:
                return False
            
            client = self._clients[client_id]
            client.queue.append(f": attached stream {stream_id}\n\n")
            return True
    
    def get_client_count(self) -> int:
        """Get number of connected clients."""
        with self._lock:
            return len(self._clients)
    
    def get_client_info(self, client_id: str) -> Optional[Dict[str, Any]]:
        """Get information about a connected client."""
        with self._lock:
            client = self._clients.get(client_id)
            if not client:
                return None
            
            return {
                "client_id": client.client_id,
                "event_type": client.event_type,
                "connected_at": client.connected_at,
                "last_event_id": client.last_event_id,
                "queue_size": len(client.queue)
            }
    
    def get_all_clients(self) -> List[Dict[str, Any]]:
        """Get information about all connected clients."""
        with self._lock:
            return [
                {
                    "client_id": c.client_id,
                    "event_type": c.event_type,
                    "connected_at": c.connected_at,
                    "queue_size": len(c.queue)
                }
                for c in self._clients.values()
            ]
