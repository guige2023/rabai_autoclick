"""Redis action module for RabAI AutoClick.

Provides Redis operations:
- RedisSetAction: Set key-value
- RedisGetAction: Get value
- RedisDeleteAction: Delete key
- RedisExistsAction: Check if key exists
- RedisExpireAction: Set key expiration
- RedisKeysAction: Find keys by pattern
- RedisHsetAction: Set hash field
- RedisHgetAction: Get hash field
- RedisLpushAction: Push to list
- RedisLrangeAction: Get list range
- RedisPublishAction: Publish to channel
- RedisInfoAction: Get Redis info
"""

import json
from typing import Any, Dict, List, Optional, Union

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


def get_redis_client(host='localhost', port=6379, db=0, password=None):
    """Get Redis client."""
    try:
        import redis
        return redis.Redis(host=host, port=port, db=db, password=password, decode_responses=True)
    except ImportError:
        return None


class RedisSetAction(BaseAction):
    """Set key-value."""
    action_type = "redis_set"
    display_name = "Redis设置"
    description = "设置Redis键值"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute set.

        Args:
            context: Execution context.
            params: Dict with key, value, host, port, db, expire.

        Returns:
            ActionResult indicating success.
        """
        key = params.get('key', '')
        value = params.get('value', '')
        host = params.get('host', 'localhost')
        port = params.get('port', 6379)
        db = params.get('db', 0)
        expire = params.get('expire', 0)

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_key = context.resolve_value(key)
            resolved_value = context.resolve_value(value)
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)
            resolved_db = context.resolve_value(db)
            resolved_expire = context.resolve_value(expire)

            client = get_redis_client(resolved_host, int(resolved_port), int(resolved_db))
            if client is None:
                return ActionResult(
                    success=False,
                    message="redis-py未安装: pip install redis"
                )

            if isinstance(resolved_value, (dict, list)):
                serialized = json.dumps(resolved_value)
            else:
                serialized = str(resolved_value)

            client.set(resolved_key, serialized)

            if resolved_expire and int(resolved_expire) > 0:
                client.expire(resolved_key, int(resolved_expire))

            return ActionResult(
                success=True,
                message=f"已设置: {resolved_key}",
                data={'key': resolved_key}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Redis set失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'host': 'localhost', 'port': 6379, 'db': 0, 'expire': 0}


class RedisGetAction(BaseAction):
    """Get value."""
    action_type = "redis_get"
    display_name = "Redis获取"
    description = "获取Redis值"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get.

        Args:
            context: Execution context.
            params: Dict with key, host, port, db, output_var.

        Returns:
            ActionResult with value.
        """
        key = params.get('key', '')
        host = params.get('host', 'localhost')
        port = params.get('port', 6379)
        db = params.get('db', 0)
        output_var = params.get('output_var', 'redis_value')

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_key = context.resolve_value(key)
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)
            resolved_db = context.resolve_value(db)

            client = get_redis_client(resolved_host, int(resolved_port), int(resolved_db))
            if client is None:
                return ActionResult(
                    success=False,
                    message="redis-py未安装"
                )

            value = client.get(resolved_key)

            if value is None:
                context.set(output_var, None)
                return ActionResult(
                    success=True,
                    message=f"键不存在: {resolved_key}",
                    data={'value': None, 'output_var': output_var}
                )

            # Try to deserialize JSON
            try:
                deserialized = json.loads(value)
                context.set(output_var, deserialized)
                value_display = deserialized
            except (json.JSONDecodeError, TypeError):
                context.set(output_var, value)
                value_display = value

            return ActionResult(
                success=True,
                message=f"获取: {str(value_display)[:50]}",
                data={'value': value_display, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Redis get失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'host': 'localhost', 'port': 6379, 'db': 0, 'output_var': 'redis_value'}


class RedisDeleteAction(BaseAction):
    """Delete key."""
    action_type = "redis_delete"
    display_name = "Redis删除"
    description = "删除Redis键"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute delete.

        Args:
            context: Execution context.
            params: Dict with key, host, port, db.

        Returns:
            ActionResult indicating success.
        """
        key = params.get('key', '')
        host = params.get('host', 'localhost')
        port = params.get('port', 6379)
        db = params.get('db', 0)

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_key = context.resolve_value(key)
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)
            resolved_db = context.resolve_value(db)

            client = get_redis_client(resolved_host, int(resolved_port), int(resolved_db))
            if client is None:
                return ActionResult(
                    success=False,
                    message="redis-py未安装"
                )

            deleted = client.delete(resolved_key)

            return ActionResult(
                success=True,
                message=f"已删除: {resolved_key} ({deleted} 键)",
                data={'deleted': deleted}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Redis delete失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'host': 'localhost', 'port': 6379, 'db': 0}


class RedisKeysAction(BaseAction):
    """Find keys by pattern."""
    action_type = "redis_keys"
    display_name = "Redis搜索键"
    description = "按模式搜索Redis键"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute keys.

        Args:
            context: Execution context.
            params: Dict with pattern, host, port, db, output_var.

        Returns:
            ActionResult with key list.
        """
        pattern = params.get('pattern', '*')
        host = params.get('host', 'localhost')
        port = params.get('port', 6379)
        db = params.get('db', 0)
        output_var = params.get('output_var', 'redis_keys')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_pattern = context.resolve_value(pattern)
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)
            resolved_db = context.resolve_value(db)

            client = get_redis_client(resolved_host, int(resolved_port), int(resolved_db))
            if client is None:
                return ActionResult(
                    success=False,
                    message="redis-py未安装"
                )

            keys = client.keys(resolved_pattern)
            context.set(output_var, keys)

            return ActionResult(
                success=True,
                message=f"找到 {len(keys)} 个键",
                data={'count': len(keys), 'keys': keys, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Redis keys失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'pattern': '*', 'host': 'localhost', 'port': 6379, 'db': 0, 'output_var': 'redis_keys'}


class RedisExpireAction(BaseAction):
    """Set key expiration."""
    action_type = "redis_expire"
    display_name = "Redis设置过期"
    description = "设置Redis键过期时间"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute expire.

        Args:
            context: Execution context.
            params: Dict with key, seconds, host, port, db.

        Returns:
            ActionResult indicating success.
        """
        key = params.get('key', '')
        seconds = params.get('seconds', 60)
        host = params.get('host', 'localhost')
        port = params.get('port', 6379)
        db = params.get('db', 0)

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_key = context.resolve_value(key)
            resolved_seconds = context.resolve_value(seconds)
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)
            resolved_db = context.resolve_value(db)

            client = get_redis_client(resolved_host, int(resolved_port), int(resolved_db))
            if client is None:
                return ActionResult(
                    success=False,
                    message="redis-py未安装"
                )

            result = client.expire(resolved_key, int(resolved_seconds))

            return ActionResult(
                success=result,
                message=f"过期时间已设置: {resolved_key} ({resolved_seconds}s)",
                data={'key': resolved_key, 'seconds': resolved_seconds, 'set': result}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Redis expire失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key', 'seconds']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'host': 'localhost', 'port': 6379, 'db': 0}


class RedisHsetAction(BaseAction):
    """Set hash field."""
    action_type = "redis_hset"
    display_name = "Redis哈希设置"
    description = "设置Redis哈希字段"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute hset.

        Args:
            context: Execution context.
            params: Dict with key, field, value, host, port, db.

        Returns:
            ActionResult indicating success.
        """
        key = params.get('key', '')
        field = params.get('field', '')
        value = params.get('value', '')
        host = params.get('host', 'localhost')
        port = params.get('port', 6379)
        db = params.get('db', 0)

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_key = context.resolve_value(key)
            resolved_field = context.resolve_value(field)
            resolved_value = context.resolve_value(value)
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)
            resolved_db = context.resolve_value(db)

            client = get_redis_client(resolved_host, int(resolved_port), int(resolved_db))
            if client is None:
                return ActionResult(
                    success=False,
                    message="redis-py未安装"
                )

            client.hset(resolved_key, resolved_field, str(resolved_value))

            return ActionResult(
                success=True,
                message=f"已设置: {resolved_key}.{resolved_field}",
                data={'key': resolved_key, 'field': resolved_field}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Redis hset失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key', 'field', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'host': 'localhost', 'port': 6379, 'db': 0}


class RedisHgetAction(BaseAction):
    """Get hash field."""
    action_type = "redis_hget"
    display_name = "Redis哈希获取"
    description = "获取Redis哈希字段"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute hget.

        Args:
            context: Execution context.
            params: Dict with key, field, host, port, db, output_var.

        Returns:
            ActionResult with value.
        """
        key = params.get('key', '')
        field = params.get('field', '')
        host = params.get('host', 'localhost')
        port = params.get('port', 6379)
        db = params.get('db', 0)
        output_var = params.get('output_var', 'redis_hash_value')

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_key = context.resolve_value(key)
            resolved_field = context.resolve_value(field)
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)
            resolved_db = context.resolve_value(db)

            client = get_redis_client(resolved_host, int(resolved_port), int(resolved_db))
            if client is None:
                return ActionResult(
                    success=False,
                    message="redis-py未安装"
                )

            value = client.hget(resolved_key, resolved_field)
            context.set(output_var, value)

            return ActionResult(
                success=True,
                message=f"获取: {resolved_key}.{resolved_field}",
                data={'value': value, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Redis hget失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key', 'field']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'host': 'localhost', 'port': 6379, 'db': 0, 'output_var': 'redis_hash_value'}


class RedisLpushAction(BaseAction):
    """Push to list."""
    action_type = "redis_lpush"
    display_name = "Redis列表推入"
    description = "向Redis列表左侧推入"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute lpush.

        Args:
            context: Execution context.
            params: Dict with key, value, host, port, db.

        Returns:
            ActionResult with list length.
        """
        key = params.get('key', '')
        value = params.get('value', '')
        host = params.get('host', 'localhost')
        port = params.get('port', 6379)
        db = params.get('db', 0)

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_key = context.resolve_value(key)
            resolved_value = context.resolve_value(value)
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)
            resolved_db = context.resolve_value(db)

            client = get_redis_client(resolved_host, int(resolved_port), int(resolved_db))
            if client is None:
                return ActionResult(
                    success=False,
                    message="redis-py未安装"
                )

            length = client.lpush(resolved_key, str(resolved_value))

            return ActionResult(
                success=True,
                message=f"已推入: {resolved_key} (长度 {length})",
                data={'key': resolved_key, 'length': length}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Redis lpush失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'host': 'localhost', 'port': 6379, 'db': 0}


class RedisLrangeAction(BaseAction):
    """Get list range."""
    action_type = "redis_lrange"
    display_name = "Redis列表范围"
    description = "获取Redis列表范围"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute lrange.

        Args:
            context: Execution context.
            params: Dict with key, start, stop, host, port, db, output_var.

        Returns:
            ActionResult with list items.
        """
        key = params.get('key', '')
        start = params.get('start', 0)
        stop = params.get('stop', -1)
        host = params.get('host', 'localhost')
        port = params.get('port', 6379)
        db = params.get('db', 0)
        output_var = params.get('output_var', 'redis_list')

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_key = context.resolve_value(key)
            resolved_start = context.resolve_value(start)
            resolved_stop = context.resolve_value(stop)
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)
            resolved_db = context.resolve_value(db)

            client = get_redis_client(resolved_host, int(resolved_port), int(resolved_db))
            if client is None:
                return ActionResult(
                    success=False,
                    message="redis-py未安装"
                )

            items = client.lrange(resolved_key, int(resolved_start), int(resolved_stop))
            context.set(output_var, items)

            return ActionResult(
                success=True,
                message=f"获取列表: {len(items)} 项",
                data={'items': items, 'count': len(items), 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Redis lrange失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'start': 0, 'stop': -1, 'host': 'localhost', 'port': 6379, 'db': 0, 'output_var': 'redis_list'}


class RedisPublishAction(BaseAction):
    """Publish to channel."""
    action_type = "redis_publish"
    display_name = "Redis发布"
    description = "向Redis频道发布消息"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute publish.

        Args:
            context: Execution context.
            params: Dict with channel, message, host, port.

        Returns:
            ActionResult with subscriber count.
        """
        channel = params.get('channel', '')
        message = params.get('message', '')
        host = params.get('host', 'localhost')
        port = params.get('port', 6379)

        valid, msg = self.validate_type(channel, str, 'channel')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_channel = context.resolve_value(channel)
            resolved_message = context.resolve_value(message)
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)

            client = get_redis_client(resolved_host, int(resolved_port))
            if client is None:
                return ActionResult(
                    success=False,
                    message="redis-py未安装"
                )

            count = client.publish(resolved_channel, str(resolved_message))

            return ActionResult(
                success=True,
                message=f"已发布到 {resolved_channel}: {count} 订阅者",
                data={'channel': resolved_channel, 'subscribers': count}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Redis publish失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['channel', 'message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'host': 'localhost', 'port': 6379}


class RedisInfoAction(BaseAction):
    """Get Redis info."""
    action_type = "redis_info"
    display_name = "Redis信息"
    description = "获取Redis服务器信息"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute info.

        Args:
            context: Execution context.
            params: Dict with host, port, db, output_var.

        Returns:
            ActionResult with Redis info.
        """
        host = params.get('host', 'localhost')
        port = params.get('port', 6379)
        db = params.get('db', 0)
        output_var = params.get('output_var', 'redis_info')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)
            resolved_db = context.resolve_value(db)

            client = get_redis_client(resolved_host, int(resolved_port), int(resolved_db))
            if client is None:
                return ActionResult(
                    success=False,
                    message="redis-py未安装"
                )

            info = client.info()
            context.set(output_var, info)

            return ActionResult(
                success=True,
                message=f"Redis: {info.get('redis_version', '?')} ({info.get('used_memory_human', '?')})",
                data=info
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Redis info失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'host': 'localhost', 'port': 6379, 'db': 0, 'output_var': 'redis_info'}
