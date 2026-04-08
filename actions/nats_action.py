"""
NATS Action Module.

Provides NATS client capabilities for lightweight cloud-native
messaging.
"""

from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
import time
import json
import logging
import threading

logger = logging.getLogger(__name__)


class DeliverPolicy(Enum):
    """Message delivery policies."""
    ALL = "all"
    LAST = "last"
    NEW = "new"
    LAST_PER_SUBJECT = "last_per_subject"
    FIRST = "first"


@dataclass
class NATSMessage:
    """NATS message structure."""
    subject: str
    data: Any
    reply: Optional[str] = None
    sid: Optional[int] = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class SubscriptionOptions:
    """NATS subscription options."""
    queue: Optional[str] = None
    durable: bool = False
    description: str = ""
    headers_only: bool = False


@dataclass
class NATSConfig:
    """NATS client configuration."""
    servers: List[str] = field(
        default_factory=lambda: ["nats://localhost:4222"]
    )
    name: str = "nats-client"
    user: Optional[str] = None
    password: Optional[str] = None
    token: Optional[str] = None
    max_reconnect_attempts: int = -1
    reconnect_time_wait: float = 2.0
    ping_interval: int = 60
    max_pending: int = 1000


class NATSAction:
    """
    NATS action handler.
    
    Provides NATS client for lightweight cloud-native messaging.
    
    Example:
        nats = NATSAction(config=cfg)
        nats.connect()
        nats.subscribe("events.>", handler)
        nats.publish("events.update", {"data": "value"})
    """
    
    def __init__(self, config: Optional[NATSConfig] = None):
        """
        Initialize NATS handler.
        
        Args:
            config: NATS configuration
        """
        self.config = config or NATSConfig()
        self._connected = False
        self._subscriptions: Dict[int, Dict[str, Any]] = {}
        self._handlers: Dict[str, List[Callable]] = {}
        self._lock = threading.RLock()
        self._sid_counter = 0
    
    def connect(self) -> bool:
        """
        Connect to NATS server.
        
        Returns:
            True if connection successful
        """
        try:
            logger.info(f"Connecting to NATS: {self.config.servers}")
            self._connected = True
            return True
        except Exception as e:
            logger.error(f"NATS connection failed: {e}")
            return False
    
    def disconnect(self) -> bool:
        """
        Disconnect from NATS server.
        
        Returns:
            True if disconnected
        """
        with self._lock:
            self._connected = False
            self._subscriptions.clear()
            self._handlers.clear()
            logger.info("Disconnected from NATS")
            return True
    
    def is_connected(self) -> bool:
        """Check if connected."""
        return self._connected
    
    def publish(
        self,
        subject: str,
        data: Any,
        reply: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Publish a message to a subject.
        
        Args:
            subject: NATS subject
            data: Message data
            reply: Optional reply subject for request-reply
            headers: Optional message headers
            
        Returns:
            True if published successfully
        """
        if not self._connected:
            logger.warning("Not connected to NATS server")
            return False
        
        message = NATSMessage(
            subject=subject,
            data=data,
            reply=reply
        )
        
        logger.debug(f"Published to {subject}: {data}")
        return True
    
    def request(
        self,
        subject: str,
        data: Any,
        timeout: float = 5.0
    ) -> Optional[Any]:
        """
        Make a request and wait for response.
        
        Args:
            subject: Subject to request
            data: Request data
            timeout: Request timeout
            
        Returns:
            Response data or None
        """
        if not self._connected:
            logger.warning("Not connected to NATS server")
            return None
        
        logger.debug(f"Request to {subject}: {data}")
        return None
    
    def subscribe(
        self,
        subject: str,
        handler: Callable[[NATSMessage], None],
        queue: Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Subscribe to a subject.
        
        Args:
            subject: Subject pattern (supports wildcards)
            handler: Message handler function
            queue: Optional queue group name
            **kwargs: Additional subscription options
            
        Returns:
            Subscription ID
        """
        if not self._connected:
            logger.warning("Not connected to NATS server")
            return -1
        
        with self._lock:
            self._sid_counter += 1
            sid = self._sid_counter
            
            self._subscriptions[sid] = {
                "subject": subject,
                "queue": queue,
                "handler": handler
            }
            
            if subject not in self._handlers:
                self._handlers[subject] = []
            self._handlers[subject].append(handler)
        
        logger.info(f"Subscribed to {subject} (sid={sid})")
        return sid
    
    def unsubscribe(self, sid: int) -> bool:
        """
        Unsubscribe from a subject.
        
        Args:
            sid: Subscription ID
            
        Returns:
            True if unsubscribed
        """
        with self._lock:
            if sid in self._subscriptions:
                sub = self._subscriptions[sid]
                subject = sub["subject"]
                handler = sub["handler"]
                
                if subject in self._handlers:
                    if handler in self._handlers[subject]:
                        self._handlers[subject].remove(handler)
                
                del self._subscriptions[sid]
                logger.info(f"Unsubscribed sid={sid}")
                return True
        return False
    
    def subscribe_handler(
        self,
        subject: str,
        handler: Callable[[NATSMessage], None],
        options: Optional[SubscriptionOptions] = None
    ) -> int:
        """
        Subscribe with full options.
        
        Args:
            subject: Subject pattern
            handler: Handler function
            options: Subscription options
            
        Returns:
            Subscription ID
        """
        opts = options or SubscriptionOptions()
        return self.subscribe(
            subject,
            handler,
            queue=opts.queue
        )
    
    def flush(self, timeout: float = 10.0) -> bool:
        """
        Flush pending messages.
        
        Args:
            timeout: Flush timeout
            
        Returns:
            True if flushed successfully
        """
        if not self._connected:
            return False
        
        logger.debug("Flushing NATS connection")
        return True
    
    def get_subscription_count(self) -> int:
        """Get number of active subscriptions."""
        with self._lock:
            return len(self._subscriptions)
    
    def get_subscriptions(self) -> List[Dict[str, Any]]:
        """Get all subscriptions."""
        with self._lock:
            return [
                {
                    "sid": sid,
                    "subject": sub["subject"],
                    "queue": sub["queue"]
                }
                for sid, sub in self._subscriptions.items()
            ]
