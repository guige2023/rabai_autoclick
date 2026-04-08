"""NATS action module for RabAI AutoClick.

Provides NATS messaging operations for
pub/sub, request/reply, and stream management.
"""

import os
import sys
import time
from typing import Any, Dict, List, Optional, Union, Callable
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class NATSClient:
    """NATS client for messaging operations.
    
    Provides methods for pub/sub messaging,
    request/reply, and stream management.
    """
    
    def __init__(
        self,
        servers: Optional[List[str]] = None,
        name: str = "nats_client",
        user: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None
    ) -> None:
        """Initialize NATS client.
        
        Args:
            servers: List of NATS server URLs.
            name: Client name.
            user: Username.
            password: Password.
            token: Authentication token.
        """
        self.servers = servers or ["nats://localhost:4222"]
        self.name = name
        self.user = user
        self.password = password
        self.token = token
        self._nc: Optional[Any] = None
        self._js: Optional[Any] = None
    
    def connect(self) -> bool:
        """Connect to NATS server.
        
        Returns:
            True if connection successful, False otherwise.
        """
        try:
            import nats
        except ImportError:
            raise ImportError("nats-py is required. Install with: pip install nats-py")
        
        try:
            import asyncio
            
            async def _connect():
                options: Dict[str, Any] = {
                    "name": self.name,
                    "servers": self.servers
                }
                
                if self.user and self.password:
                    options["user"] = self.user
                    options["password"] = self.password
                
                if self.token:
                    options["token"] = self.token
                
                self._nc = await nats.connect(**options)
                
                try:
                    self._js = self._nc.jetstream()
                except Exception:
                    self._js = None
            
            asyncio.run(_connect())
            return self._nc is not None
        
        except Exception:
            self._nc = None
            self._js = None
            return False
    
    def disconnect(self) -> None:
        """Disconnect from NATS server."""
        if self._nc:
            try:
                import asyncio
                asyncio.run(self._nc.close())
            except Exception:
                pass
            self._nc = None
            self._js = None
    
    def publish(
        self,
        subject: str,
        payload: Union[str, bytes],
        headers: Optional[Dict[str, str]] = None
    ) -> bool:
        """Publish a message.
        
        Args:
            subject: Subject to publish to.
            payload: Message payload.
            headers: Optional message headers.
            
        Returns:
            True if publish succeeded.
        """
        if not self._nc:
            raise RuntimeError("Not connected to NATS")
        
        try:
            import asyncio
            
            async def _publish():
                if headers:
                    await self._nc.publish(
                        subject,
                        payload.encode() if isinstance(payload, str) else payload,
                        headers=headers
                    )
                else:
                    await self._nc.publish(
                        subject,
                        payload.encode() if isinstance(payload, str) else payload
                    )
            
            asyncio.run(_publish())
            return True
        
        except Exception:
            return False
    
    def subscribe(
        self,
        subject: str,
        callback: Optional[Callable] = None,
        queue: Optional[str] = None
    ) -> Optional[str]:
        """Subscribe to a subject.
        
        Args:
            subject: Subject to subscribe to.
            callback: Message callback function.
            queue: Optional queue group.
            
        Returns:
            Subscription ID or None.
        """
        if not self._nc:
            raise RuntimeError("Not connected to NATS")
        
        try:
            import asyncio
            
            async def _subscribe():
                if callback:
                    if queue:
                        return await self._nc.subscribe(subject, queue=queue, cb=callback)
                    else:
                        return await self._nc.subscribe(subject, cb=callback)
                else:
                    if queue:
                        return await self._nc.subscribe(subject, queue=queue)
                    else:
                        return await self._nc.subscribe(subject)
            
            sub = asyncio.run(_subscribe())
            return sub.sid if sub else None
        
        except Exception:
            return None
    
    def request(
        self,
        subject: str,
        payload: Union[str, bytes],
        timeout: float = 1.0
    ) -> Optional[Any]:
        """Send a request and wait for reply.
        
        Args:
            subject: Subject to request.
            payload: Request payload.
            timeout: Request timeout.
            
        Returns:
            Response message or None.
        """
        if not self._nc:
            raise RuntimeError("Not connected to NATS")
        
        try:
            import asyncio
            
            async def _request():
                msg = await self._nc.request(
                    subject,
                    payload.encode() if isinstance(payload, str) else payload,
                    timeout=timeout
                )
                return msg
            
            return asyncio.run(_request())
        
        except Exception:
            return None
    
    def drain_subscription(self, sid: str) -> bool:
        """Drain a subscription.
        
        Args:
            sid: Subscription ID.
            
        Returns:
            True if drain succeeded.
        """
        if not self._nc:
            raise RuntimeError("Not connected to NATS")
        
        try:
            for sub in self._nc._subs.values():
                if sub.sid == sid:
                    sub.drain()
                    return True
            return False
        except Exception:
            return False
    
    def jetstream_publish(
        self,
        stream: str,
        subject: str,
        payload: Union[str, bytes]
    ) -> bool:
        """Publish to JetStream.
        
        Args:
            stream: Stream name.
            subject: Subject to publish to.
            payload: Message payload.
            
        Returns:
            True if publish succeeded.
        """
        if not self._js:
            raise RuntimeError("JetStream not available")
        
        try:
            import asyncio
            
            async def _publish():
                await self._js.publish(
                    subject,
                    payload.encode() if isinstance(payload, str) else payload
                )
            
            asyncio.run(_publish())
            return True
        
        except Exception:
            return False
    
    def jetstream_subscribe(
        self,
        stream: str,
        subject: str,
        durable: Optional[str] = None
    ) -> Optional[str]:
        """Subscribe to JetStream.
        
        Args:
            stream: Stream name.
            subject: Subject to subscribe to.
            durable: Optional durable consumer name.
            
        Returns:
            Subscription ID or None.
        """
        if not self._js:
            raise RuntimeError("JetStream not available")
        
        try:
            import asyncio
            
            async def _subscribe():
                sub = await self._js.subscribe(subject, durable_name=durable)
                return sub.sid if sub else None
            
            return asyncio.run(_subscribe())
        
        except Exception:
            return None
    
    def create_stream(
        self,
        name: str,
        subjects: List[str],
        storage: str = "file",
        replicas: int = 1
    ) -> bool:
        """Create a JetStream stream.
        
        Args:
            name: Stream name.
            subjects: List of subjects for the stream.
            storage: Storage type (file or memory).
            replicas: Number of replicas.
            
        Returns:
            True if creation succeeded.
        """
        if not self._js:
            raise RuntimeError("JetStream not available")
        
        try:
            import asyncio
            
            async def _create():
                cfg = {
                    "Name": name,
                    "Subjects": subjects,
                    "Storage": storage,
                    "Replicas": replicas
                }
                await self._js.add_stream(**cfg)
            
            asyncio.run(_create())
            return True
        
        except Exception:
            return False
    
    def get_stream_info(self, name: str) -> Optional[Dict[str, Any]]:
        """Get stream information.
        
        Args:
            name: Stream name.
            
        Returns:
            Stream information or None.
        """
        if not self._js:
            raise RuntimeError("JetStream not available")
        
        try:
            import asyncio
            
            async def _info():
                return await self._js.stream_info(name)
            
            return asyncio.run(_info())
        
        except Exception:
            return None
    
    def list_streams(self) -> List[str]:
        """List all streams.
        
        Returns:
            List of stream names.
        """
        if not self._js:
            raise RuntimeError("JetStream not available")
        
        try:
            import asyncio
            
            async def _list():
                streams = []
                async for s in self._js.streams_list():
                    streams.append(s.config.name)
                return streams
            
            return asyncio.run(_list())
        
        except Exception:
            return []
    
    def delete_stream(self, name: str) -> bool:
        """Delete a stream.
        
        Args:
            name: Stream name to delete.
            
        Returns:
            True if deletion succeeded.
        """
        if not self._js:
            raise RuntimeError("JetStream not available")
        
        try:
            import asyncio
            
            async def _delete():
                await self._js.delete_stream(name)
            
            asyncio.run(_delete())
            return True
        
        except Exception:
            return False
    
    def get_msg_count(self, stream: str) -> int:
        """Get message count for a stream.
        
        Args:
            stream: Stream name.
            
        Returns:
            Number of messages.
        """
        if not self._js:
            raise RuntimeError("JetStream not available")
        
        try:
            info = self.get_stream_info(stream)
            if info:
                return info.state.get("messages", 0)
            return 0
        except Exception:
            return 0


class NATSAction(BaseAction):
    """NATS action for messaging operations.
    
    Supports pub/sub, request/reply, and JetStream management.
    """
    action_type: str = "nats"
    display_name: str = "NATS动作"
    description: str = "NATS消息队列和JetStream流管理"
    
    def __init__(self) -> None:
        super().__init__()
        self._client: Optional[NATSClient] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute NATS operation.
        
        Args:
            context: Execution context.
            params: Operation and parameters.
            
        Returns:
            ActionResult with operation outcome.
        """
        start_time = time.time()
        
        try:
            operation = params.get("operation", "connect")
            
            if operation == "connect":
                return self._connect(params, start_time)
            elif operation == "disconnect":
                return self._disconnect(start_time)
            elif operation == "publish":
                return self._publish(params, start_time)
            elif operation == "subscribe":
                return self._subscribe(params, start_time)
            elif operation == "request":
                return self._request(params, start_time)
            elif operation == "drain":
                return self._drain(params, start_time)
            elif operation == "js_publish":
                return self._js_publish(params, start_time)
            elif operation == "js_subscribe":
                return self._js_subscribe(params, start_time)
            elif operation == "create_stream":
                return self._create_stream(params, start_time)
            elif operation == "get_stream_info":
                return self._get_stream_info(params, start_time)
            elif operation == "list_streams":
                return self._list_streams(start_time)
            elif operation == "delete_stream":
                return self._delete_stream(params, start_time)
            elif operation == "get_msg_count":
                return self._get_msg_count(params, start_time)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}", duration=time.time() - start_time)
        
        except ImportError as e:
            return ActionResult(success=False, message=f"Import error: {str(e)}", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=f"NATS operation failed: {str(e)}", duration=time.time() - start_time)
    
    def _connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Connect to NATS."""
        servers = params.get("servers", ["nats://localhost:4222"])
        name = params.get("name", "nats_client")
        
        self._client = NATSClient(
            servers=servers if isinstance(servers, list) else [servers],
            name=name,
            user=params.get("user"),
            password=params.get("password"),
            token=params.get("token")
        )
        
        success = self._client.connect()
        
        return ActionResult(
            success=success,
            message=f"Connected to NATS" if success else "Failed to connect",
            duration=time.time() - start_time
        )
    
    def _disconnect(self, start_time: float) -> ActionResult:
        """Disconnect from NATS."""
        if self._client:
            self._client.disconnect()
            self._client = None
        
        return ActionResult(success=True, message="Disconnected from NATS", duration=time.time() - start_time)
    
    def _publish(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Publish a message."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        subject = params.get("subject", "")
        payload = params.get("payload", "")
        
        if not subject:
            return ActionResult(success=False, message="subject is required", duration=time.time() - start_time)
        
        try:
            success = self._client.publish(
                subject,
                payload,
                params.get("headers")
            )
            return ActionResult(success=success, message=f"Published to {subject}" if success else "Publish failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _subscribe(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Subscribe to a subject."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        subject = params.get("subject", "")
        if not subject:
            return ActionResult(success=False, message="subject is required", duration=time.time() - start_time)
        
        try:
            sid = self._client.subscribe(
                subject,
                queue=params.get("queue")
            )
            return ActionResult(success=sid is not None, message=f"Subscribed to {subject}" if sid else "Subscribe failed", data={"sid": sid}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _request(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Send a request."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        subject = params.get("subject", "")
        payload = params.get("payload", "")
        
        if not subject:
            return ActionResult(success=False, message="subject is required", duration=time.time() - start_time)
        
        try:
            response = self._client.request(
                subject,
                payload,
                timeout=params.get("timeout", 1.0)
            )
            return ActionResult(success=response is not None, message="Request sent" if response else "Request failed", data={"response": str(response) if response else None}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _drain(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Drain a subscription."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        sid = params.get("sid", "")
        if not sid:
            return ActionResult(success=False, message="sid is required", duration=time.time() - start_time)
        
        try:
            success = self._client.drain_subscription(sid)
            return ActionResult(success=success, message="Subscription drained" if success else "Drain failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _js_publish(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Publish to JetStream."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        stream = params.get("stream", "")
        subject = params.get("subject", "")
        payload = params.get("payload", "")
        
        if not stream or not subject:
            return ActionResult(success=False, message="stream and subject are required", duration=time.time() - start_time)
        
        try:
            success = self._client.jetstream_publish(stream, subject, payload)
            return ActionResult(success=success, message=f"Published to {subject}" if success else "Publish failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _js_subscribe(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Subscribe to JetStream."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        stream = params.get("stream", "")
        subject = params.get("subject", "")
        
        if not stream or not subject:
            return ActionResult(success=False, message="stream and subject are required", duration=time.time() - start_time)
        
        try:
            sid = self._client.jetstream_subscribe(
                stream,
                subject,
                durable=params.get("durable")
            )
            return ActionResult(success=sid is not None, message=f"Subscribed to {subject}" if sid else "Subscribe failed", data={"sid": sid}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _create_stream(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create a JetStream stream."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        name = params.get("name", "")
        subjects = params.get("subjects", [])
        
        if not name or not subjects:
            return ActionResult(success=False, message="name and subjects are required", duration=time.time() - start_time)
        
        try:
            success = self._client.create_stream(
                name=name,
                subjects=subjects,
                storage=params.get("storage", "file"),
                replicas=params.get("replicas", 1)
            )
            return ActionResult(success=success, message=f"Stream created: {name}" if success else "Create stream failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_stream_info(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get stream information."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            info = self._client.get_stream_info(name)
            return ActionResult(success=info is not None, message=f"Stream info retrieved: {name}", data={"info": info}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _list_streams(self, start_time: float) -> ActionResult:
        """List all streams."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            streams = self._client.list_streams()
            return ActionResult(success=True, message=f"Found {len(streams)} streams", data={"streams": streams}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _delete_stream(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete a stream."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            success = self._client.delete_stream(name)
            return ActionResult(success=success, message=f"Stream deleted: {name}" if success else "Delete failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_msg_count(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get message count for a stream."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        stream = params.get("stream", "")
        if not stream:
            return ActionResult(success=False, message="stream is required", duration=time.time() - start_time)
        
        try:
            count = self._client.get_msg_count(stream)
            return ActionResult(success=True, message=f"Stream {stream} has {count} messages", data={"count": count}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
