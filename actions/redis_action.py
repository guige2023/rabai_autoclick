"""Redis action module for RabAI AutoClick.

Provides Redis operations including string, hash, list, set manipulation,
pub/sub, and connection management.
"""

import os
import sys
import time
import json
from typing import Any, Dict, List, Optional, Union, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class RedisClient:
    """Redis client wrapper with connection management.
    
    Provides convenient methods for common Redis operations.
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        decode_responses: bool = True,
        socket_timeout: float = 5.0,
        socket_connect_timeout: float = 5.0
    ) -> None:
        """Initialize Redis client.
        
        Args:
            host: Redis server hostname.
            port: Redis server port.
            db: Database number (0-15).
            password: Optional password for authentication.
            decode_responses: Whether to decode bytes to strings.
            socket_timeout: Socket timeout in seconds.
            socket_connect_timeout: Socket connection timeout in seconds.
        """
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self._conn: Optional[Any] = None
        self._decode_responses = decode_responses
        self._socket_timeout = socket_timeout
        self._socket_connect_timeout = socket_connect_timeout
    
    def connect(self) -> bool:
        """Establish connection to Redis server.
        
        Returns:
            True if connection successful, False otherwise.
        """
        try:
            import redis
        except ImportError:
            raise ImportError(
                "redis is required for Redis support. Install with: pip install redis"
            )
        
        try:
            self._conn = redis.Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                password=self.password,
                decode_responses=self._decode_responses,
                socket_timeout=self._socket_timeout,
                socket_connect_timeout=self._socket_connect_timeout
            )
            self._conn.ping()
            return True
        except Exception:
            self._conn = None
            return False
    
    def disconnect(self) -> bool:
        """Close the Redis connection.
        
        Returns:
            True if disconnection successful.
        """
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
        return True
    
    @property
    def is_connected(self) -> bool:
        """Check if currently connected."""
        if not self._conn:
            return False
        try:
            self._conn.ping()
            return True
        except Exception:
            self._conn = None
            return False
    
    def _require_connection(self) -> Any:
        """Ensure an active Redis connection exists."""
        if not self.is_connected:
            raise RuntimeError("Not connected to Redis")
        return self._conn
    
    def ping(self) -> bool:
        """Ping the Redis server.
        
        Returns:
            True if server responds.
        """
        return self._require_connection().ping()
    
    def get(self, key: str) -> Optional[str]:
        """Get a string value.
        
        Args:
            key: Key to retrieve.
            
        Returns:
            Value or None if key doesn't exist.
        """
        return self._require_connection().get(key)
    
    def set(
        self,
        key: str,
        value: str,
        ex: Optional[int] = None,
        px: Optional[int] = None,
        nx: bool = False,
        xx: bool = False
    ) -> bool:
        """Set a string value.
        
        Args:
            key: Key to set.
            value: Value to store.
            ex: Expiration time in seconds.
            px: Expiration time in milliseconds.
            nx: Only set if key doesn't exist.
            xx: Only set if key exists.
            
        Returns:
            True if set was successful.
        """
        return self._require_connection().set(key, value, ex=ex, px=px, nx=nx, xx=xx)
    
    def delete(self, *keys: str) -> int:
        """Delete one or more keys.
        
        Args:
            *keys: Keys to delete.
            
        Returns:
            Number of keys deleted.
        """
        return self._require_connection().delete(*keys)
    
    def exists(self, *keys: str) -> int:
        """Check if one or more keys exist.
        
        Args:
            *keys: Keys to check.
            
        Returns:
            Number of keys that exist.
        """
        return self._require_connection().exists(*keys)
    
    def expire(self, key: str, seconds: int) -> bool:
        """Set expiration time on a key.
        
        Args:
            key: Key to set expiration on.
            seconds: Expiration time in seconds.
            
        Returns:
            True if expiration was set.
        """
        return self._require_connection().expire(key, seconds)
    
    def ttl(self, key: str) -> int:
        """Get remaining TTL on a key.
        
        Args:
            key: Key to check.
            
        Returns:
            TTL in seconds (-2 if key doesn't exist, -1 if no expiry).
        """
        return self._require_connection().ttl(key)
    
    def hget(self, key: str, field: str) -> Optional[str]:
        """Get a hash field value.
        
        Args:
            key: Hash key.
            field: Field name.
            
        Returns:
            Field value or None.
        """
        return self._require_connection().hget(key, field)
    
    def hset(
        self,
        key: str,
        field: Optional[str] = None,
        value: Optional[str] = None,
        mapping: Optional[Dict[str, str]] = None
    ) -> Union[int, bool]:
        """Set hash field(s).
        
        Args:
            key: Hash key.
            field: Field name (if setting single field).
            value: Field value (if setting single field).
            mapping: Dictionary of field-value pairs (if setting multiple).
            
        Returns:
            1 if field is new, 0 if field was updated, or True if mapping set.
        """
        conn = self._require_connection()
        if mapping:
            return conn.hset(key, mapping=mapping)
        elif field is not None and value is not None:
            return conn.hset(key, field, value)
        else:
            raise ValueError("Either field/value or mapping must be provided")
    
    def hgetall(self, key: str) -> Dict[str, str]:
        """Get all fields and values of a hash.
        
        Args:
            key: Hash key.
            
        Returns:
            Dictionary of field-value pairs.
        """
        return self._require_connection().hgetall(key)
    
    def hdel(self, key: str, *fields: str) -> int:
        """Delete hash fields.
        
        Args:
            key: Hash key.
            *fields: Field names to delete.
            
        Returns:
            Number of fields deleted.
        """
        return self._require_connection().hdel(key, *fields)
    
    def lpush(self, key: str, *values: str) -> int:
        """Push values to the left of a list.
        
        Args:
            key: List key.
            *values: Values to push.
            
        Returns:
            Length of list after push.
        """
        return self._require_connection().lpush(key, *values)
    
    def rpush(self, key: str, *values: str) -> int:
        """Push values to the right of a list.
        
        Args:
            key: List key.
            *values: Values to push.
            
        Returns:
            Length of list after push.
        """
        return self._require_connection().rpush(key, *values)
    
    def lpop(self, key: str, count: int = 1) -> Union[str, List[str], None]:
        """Pop values from the left of a list.
        
        Args:
            key: List key.
            count: Number of values to pop.
            
        Returns:
            Popped value(s) or None if list is empty.
        """
        conn = self._require_connection()
        if count == 1:
            return conn.lpop(key)
        else:
            return conn.lpop(key, count)
    
    def rpop(self, key: str, count: int = 1) -> Union[str, List[str], None]:
        """Pop values from the right of a list.
        
        Args:
            key: List key.
            count: Number of values to pop.
            
        Returns:
            Popped value(s) or None if list is empty.
        """
        conn = self._require_connection()
        if count == 1:
            return conn.rpop(key)
        else:
            return conn.rpop(key, count)
    
    def lrange(self, key: str, start: int = 0, end: int = -1) -> List[str]:
        """Get a range of elements from a list.
        
        Args:
            key: List key.
            start: Start index.
            end: End index (-1 for end of list).
            
        Returns:
            List of elements.
        """
        return self._require_connection().lrange(key, start, end)
    
    def llen(self, key: str) -> int:
        """Get the length of a list.
        
        Args:
            key: List key.
            
        Returns:
            Length of list.
        """
        return self._require_connection().llen(key)
    
    def sadd(self, key: str, *members: str) -> int:
        """Add members to a set.
        
        Args:
            key: Set key.
            *members: Members to add.
            
        Returns:
            Number of members added.
        """
        return self._require_connection().sadd(key, *members)
    
    def smembers(self, key: str) -> Set[str]:
        """Get all members of a set.
        
        Args:
            key: Set key.
            
        Returns:
            Set of members.
        """
        return self._require_connection().smembers(key)
    
    def sismember(self, key: str, member: str) -> bool:
        """Check if a member is in a set.
        
        Args:
            key: Set key.
            member: Member to check.
            
        Returns:
            True if member exists.
        """
        return self._require_connection().sismember(key, member)
    
    def srem(self, key: str, *members: str) -> int:
        """Remove members from a set.
        
        Args:
            key: Set key.
            *members: Members to remove.
            
        Returns:
            Number of members removed.
        """
        return self._require_connection().srem(key, *members)
    
    def keys(self, pattern: str = "*") -> List[str]:
        """Get all keys matching a pattern.
        
        Args:
            pattern: Glob-style pattern.
            
        Returns:
            List of matching keys.
        """
        return self._require_connection().keys(pattern)
    
    def flushdb(self) -> bool:
        """Delete all keys in the current database.
        
        Returns:
            True if successful.
        """
        return self._require_connection().flushdb()
    
    def info(self, section: Optional[str] = None) -> Dict[str, Any]:
        """Get Redis server information.
        
        Args:
            section: Optional info section (e.g., 'memory', 'stats').
            
        Returns:
            Server information dictionary.
        """
        conn = self._require_connection()
        if section:
            return conn.info(section)
        return conn.info()
    
    def incr(self, key: str, amount: int = 1) -> int:
        """Increment a numeric value.
        
        Args:
            key: Key to increment.
            amount: Amount to increment by.
            
        Returns:
            New value after increment.
        """
        return self._require_connection().incrby(key, amount)
    
    def decr(self, key: str, amount: int = 1) -> int:
        """Decrement a numeric value.
        
        Args:
            key: Key to decrement.
            amount: Amount to decrement by.
            
        Returns:
            New value after decrement.
        """
        return self._require_connection().decrby(key, amount)
    
    def type(self, key: str) -> str:
        """Get the type of a key.
        
        Args:
            key: Key to check.
            
        Returns:
            Type string ('string', 'list', 'set', 'hash', 'zset', 'none').
        """
        return self._require_connection().type(key)


class RedisAction(BaseAction):
    """Redis action for key-value and data structure operations.
    
    Supports string, hash, list, set operations and pub/sub.
    """
    action_type: str = "redis"
    display_name: str = "Redis动作"
    description: str = "Redis键值操作和数据结构操作"
    
    def __init__(self) -> None:
        super().__init__()
        self._client: Optional[RedisClient] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Redis operation.
        
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
            elif operation == "ping":
                return self._ping(start_time)
            elif operation == "get":
                return self._get(params, start_time)
            elif operation == "set":
                return self._set(params, start_time)
            elif operation == "delete":
                return self._delete(params, start_time)
            elif operation == "exists":
                return self._exists(params, start_time)
            elif operation == "expire":
                return self._expire(params, start_time)
            elif operation == "ttl":
                return self._ttl(params, start_time)
            elif operation == "hash":
                return self._hash(params, start_time)
            elif operation == "list":
                return self._list(params, start_time)
            elif operation == "set_op":
                return self._set_op(params, start_time)
            elif operation == "keys":
                return self._keys(params, start_time)
            elif operation == "info":
                return self._info(params, start_time)
            elif operation == "incr":
                return self._incr(params, start_time)
            elif operation == "decr":
                return self._decr(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
        
        except ImportError as e:
            return ActionResult(
                success=False,
                message=f"Import error: {str(e)}",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Redis operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Connect to Redis server."""
        host = params.get("host", "localhost")
        port = params.get("port", 6379)
        db = params.get("db", 0)
        password = params.get("password")
        
        self._client = RedisClient(
            host=host,
            port=port,
            db=db,
            password=password
        )
        
        success = self._client.connect()
        
        if success:
            info = self._client.info("server")
            version = info.get("redis_version", "unknown")
            return ActionResult(
                success=True,
                message=f"Connected to Redis {version}",
                data={"host": host, "port": port, "db": db},
                duration=time.time() - start_time
            )
        else:
            self._client = None
            return ActionResult(
                success=False,
                message=f"Failed to connect to Redis at {host}:{port}",
                duration=time.time() - start_time
            )
    
    def _disconnect(self, start_time: float) -> ActionResult:
        """Disconnect from Redis server."""
        if self._client:
            self._client.disconnect()
            self._client = None
        
        return ActionResult(
            success=True,
            message="Disconnected from Redis",
            duration=time.time() - start_time
        )
    
    def _require_client(self) -> RedisClient:
        """Ensure a Redis client exists."""
        if not self._client:
            raise RuntimeError("Not connected to Redis. Use 'connect' operation first.")
        return self._client
    
    def _ping(self, start_time: float) -> ActionResult:
        """Ping Redis server."""
        client = self._require_client()
        success = client.ping()
        
        return ActionResult(
            success=success,
            message="PONG" if success else "Ping failed",
            duration=time.time() - start_time
        )
    
    def _get(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get a value by key."""
        client = self._require_client()
        key = params.get("key", "")
        
        if not key:
            return ActionResult(
                success=False,
                message="key is required",
                duration=time.time() - start_time
            )
        
        value = client.get(key)
        
        return ActionResult(
            success=True,
            message=f"Retrieved key: {key}",
            data={"key": key, "value": value, "exists": value is not None},
            duration=time.time() - start_time
        )
    
    def _set(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Set a value."""
        client = self._require_client()
        key = params.get("key", "")
        value = params.get("value", "")
        
        if not key:
            return ActionResult(
                success=False,
                message="key is required",
                duration=time.time() - start_time
            )
        
        ex = params.get("ex")
        px = params.get("px")
        nx = params.get("nx", False)
        xx = params.get("xx", False)
        
        success = client.set(key, value, ex=ex, px=px, nx=nx, xx=xx)
        
        return ActionResult(
            success=success,
            message=f"Set key: {key}",
            data={"key": key, "success": success},
            duration=time.time() - start_time
        )
    
    def _delete(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete keys."""
        client = self._require_client()
        keys = params.get("keys", [])
        
        if not keys:
            return ActionResult(
                success=False,
                message="keys is required",
                duration=time.time() - start_time
            )
        
        if isinstance(keys, str):
            keys = [keys]
        
        count = client.delete(*keys)
        
        return ActionResult(
            success=True,
            message=f"Deleted {count} key(s)",
            data={"deleted": count},
            duration=time.time() - start_time
        )
    
    def _exists(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Check if keys exist."""
        client = self._require_client()
        keys = params.get("keys", [])
        
        if isinstance(keys, str):
            keys = [keys]
        
        count = client.exists(*keys) if keys else 0
        
        return ActionResult(
            success=True,
            message=f"{count} key(s) exist",
            data={"exists": count},
            duration=time.time() - start_time
        )
    
    def _expire(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Set expiration on a key."""
        client = self._require_client()
        key = params.get("key", "")
        seconds = params.get("seconds", 0)
        
        if not key or seconds <= 0:
            return ActionResult(
                success=False,
                message="key and seconds are required",
                duration=time.time() - start_time
            )
        
        success = client.expire(key, seconds)
        
        return ActionResult(
            success=success,
            message=f"Set expiration on {key}: {seconds}s",
            data={"success": success},
            duration=time.time() - start_time
        )
    
    def _ttl(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get TTL of a key."""
        client = self._require_client()
        key = params.get("key", "")
        
        if not key:
            return ActionResult(
                success=False,
                message="key is required",
                duration=time.time() - start_time
            )
        
        ttl = client.ttl(key)
        
        return ActionResult(
            success=True,
            message=f"TTL of {key}: {ttl}s",
            data={"key": key, "ttl": ttl},
            duration=time.time() - start_time
        )
    
    def _hash(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Execute hash operation."""
        client = self._require_client()
        operation = params.get("sub_operation", "get")
        key = params.get("key", "")
        
        if not key:
            return ActionResult(
                success=False,
                message="key is required",
                duration=time.time() - start_time
            )
        
        if operation == "get":
            field = params.get("field", "")
            if not field:
                return ActionResult(
                    success=False,
                    message="field is required for hash get",
                    duration=time.time() - start_time
                )
            value = client.hget(key, field)
            return ActionResult(
                success=True,
                message=f"HGET {key} {field}",
                data={"key": key, "field": field, "value": value},
                duration=time.time() - start_time
            )
        
        elif operation == "set":
            field = params.get("field", "")
            value = params.get("value", "")
            mapping = params.get("mapping")
            
            if mapping:
                client.hset(key, mapping=mapping)
            elif field and value:
                client.hset(key, field, value)
            else:
                return ActionResult(
                    success=False,
                    message="field/value or mapping required",
                    duration=time.time() - start_time
                )
            
            return ActionResult(
                success=True,
                message=f"HSET {key}",
                duration=time.time() - start_time
            )
        
        elif operation == "getall":
            data = client.hgetall(key)
            return ActionResult(
                success=True,
                message=f"HGETALL {key}: {len(data)} fields",
                data={"key": key, "data": data},
                duration=time.time() - start_time
            )
        
        elif operation == "delete":
            fields = params.get("fields", [])
            if isinstance(fields, str):
                fields = [fields]
            count = client.hdel(key, *fields) if fields else 0
            return ActionResult(
                success=True,
                message=f"HDEL {key}: {count} fields",
                data={"deleted": count},
                duration=time.time() - start_time
            )
        
        else:
            return ActionResult(
                success=False,
                message=f"Unknown hash sub-operation: {operation}",
                duration=time.time() - start_time
            )
    
    def _list(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Execute list operation."""
        client = self._require_client()
        operation = params.get("sub_operation", "push")
        key = params.get("key", "")
        
        if not key:
            return ActionResult(
                success=False,
                message="key is required",
                duration=time.time() - start_time
            )
        
        if operation in ("lpush", "rpush"):
            values = params.get("values", [])
            if isinstance(values, str):
                values = [values]
            if not values:
                return ActionResult(
                    success=False,
                    message="values required",
                    duration=time.time() - start_time
                )
            
            if operation == "lpush":
                length = client.lpush(key, *values)
            else:
                length = client.rpush(key, *values)
            
            return ActionResult(
                success=True,
                message=f"{operation.upper()} {key}: length={length}",
                data={"length": length},
                duration=time.time() - start_time
            )
        
        elif operation in ("lpop", "rpop"):
            count = params.get("count", 1)
            result = client.lpop(key, count) if operation == "lpop" else client.rpop(key, count)
            
            return ActionResult(
                success=True,
                message=f"{operation.upper()} {key}",
                data={"key": key, "value": result},
                duration=time.time() - start_time
            )
        
        elif operation == "range":
            start = params.get("start", 0)
            end = params.get("end", -1)
            items = client.lrange(key, start, end)
            
            return ActionResult(
                success=True,
                message=f"LRANGE {key} [{start}:{end}]: {len(items)} items",
                data={"key": key, "items": items, "count": len(items)},
                duration=time.time() - start_time
            )
        
        elif operation == "len":
            length = client.llen(key)
            return ActionResult(
                success=True,
                message=f"LLEN {key}: {length}",
                data={"key": key, "length": length},
                duration=time.time() - start_time
            )
        
        else:
            return ActionResult(
                success=False,
                message=f"Unknown list sub-operation: {operation}",
                duration=time.time() - start_time
            )
    
    def _set_op(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Execute set operation."""
        client = self._require_client()
        operation = params.get("sub_operation", "add")
        key = params.get("key", "")
        
        if not key:
            return ActionResult(
                success=False,
                message="key is required",
                duration=time.time() - start_time
            )
        
        if operation == "add":
            members = params.get("members", [])
            if isinstance(members, str):
                members = [members]
            if not members:
                return ActionResult(
                    success=False,
                    message="members required",
                    duration=time.time() - start_time
                )
            count = client.sadd(key, *members)
            return ActionResult(
                success=True,
                message=f"SADD {key}: {count} added",
                data={"added": count},
                duration=time.time() - start_time
            )
        
        elif operation == "members":
            members = client.smembers(key)
            return ActionResult(
                success=True,
                message=f"SMEMBERS {key}: {len(members)} members",
                data={"key": key, "members": list(members)},
                duration=time.time() - start_time
            )
        
        elif operation == "ismember":
            member = params.get("member", "")
            if not member:
                return ActionResult(
                    success=False,
                    message="member required",
                    duration=time.time() - start_time
                )
            exists = client.sismember(key, member)
            return ActionResult(
                success=True,
                message=f"SISMEMBER {key} {member}: {exists}",
                data={"exists": exists},
                duration=time.time() - start_time
            )
        
        elif operation == "remove":
            members = params.get("members", [])
            if isinstance(members, str):
                members = [members]
            count = client.srem(key, *members) if members else 0
            return ActionResult(
                success=True,
                message=f"SREM {key}: {count} removed",
                data={"removed": count},
                duration=time.time() - start_time
            )
        
        else:
            return ActionResult(
                success=False,
                message=f"Unknown set sub-operation: {operation}",
                duration=time.time() - start_time
            )
    
    def _keys(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get keys matching pattern."""
        client = self._require_client()
        pattern = params.get("pattern", "*")
        
        keys = client.keys(pattern)
        
        return ActionResult(
            success=True,
            message=f"Found {len(keys)} keys matching '{pattern}'",
            data={"keys": keys, "count": len(keys)},
            duration=time.time() - start_time
        )
    
    def _info(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get Redis server info."""
        client = self._require_client()
        section = params.get("section")
        
        info = client.info(section)
        
        return ActionResult(
            success=True,
            message="Redis info retrieved",
            data=info,
            duration=time.time() - start_time
        )
    
    def _incr(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Increment a value."""
        client = self._require_client()
        key = params.get("key", "")
        amount = params.get("amount", 1)
        
        if not key:
            return ActionResult(
                success=False,
                message="key is required",
                duration=time.time() - start_time
            )
        
        value = client.incr(key, amount)
        
        return ActionResult(
            success=True,
            message=f"INCR {key}: {value}",
            data={"key": key, "value": value},
            duration=time.time() - start_time
        )
    
    def _decr(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Decrement a value."""
        client = self._require_client()
        key = params.get("key", "")
        amount = params.get("amount", 1)
        
        if not key:
            return ActionResult(
                success=False,
                message="key is required",
                duration=time.time() - start_time
            )
        
        value = client.decr(key, amount)
        
        return ActionResult(
            success=True,
            message=f"DECR {key}: {value}",
            data={"key": key, "value": value},
            duration=time.time() - start_time
        )
