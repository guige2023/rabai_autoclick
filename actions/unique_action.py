"""Unique identifier action module for RabAI AutoClick.

Provides UUID and unique ID generation with
various formats and versions.
"""

import uuid
import hashlib
import time
import sys
import os
from typing import Any, Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class UUIDGenerateAction(BaseAction):
    """Generate UUIDs.
    
    Supports all UUID versions including
    time-based, random, name-based, and nil.
    """
    action_type = "uuid_generate"
    display_name = "生成UUID"
    description = "生成UUID标识符"

    UUID_VERSIONS = {
        '1': 'uuid1 - Time-based',
        '4': 'uuid4 - Random',
        '3': 'uuid3 - Name-based MD5',
        '5': 'uuid5 - Name-based SHA-1',
        'nil': 'Nil UUID'
    }

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Generate UUID.
        
        Args:
            context: Execution context.
            params: Dict with keys: version, namespace,
                   name, uppercase, no_dashes, save_to_var.
        
        Returns:
            ActionResult with generated UUID.
        """
        version = params.get('version', '4')
        namespace = params.get('namespace', None)
        name = params.get('name', None)
        uppercase = params.get('uppercase', False)
        no_dashes = params.get('no_dashes', False)
        save_to_var = params.get('save_to_var', None)

        if version not in self.UUID_VERSIONS:
            return ActionResult(
                success=False,
                message=f"Invalid version: {version}. Valid: {list(self.UUID_VERSIONS.keys())}"
            )

        try:
            if version == '1':
                id = uuid.uuid1()
            elif version == '4':
                id = uuid.uuid4()
            elif version == '3':
                if not namespace or not name:
                    return ActionResult(
                        success=False,
                        message="namespace and name required for MD5 UUID"
                    )
                ns = self._get_namespace(namespace)
                id = uuid.uuid3(ns, name)
            elif version == '5':
                if not namespace or not name:
                    return ActionResult(
                        success=False,
                        message="namespace and name required for SHA-1 UUID"
                    )
                ns = self._get_namespace(namespace)
                id = uuid.uuid5(ns, name)
            elif version == 'nil':
                id = uuid.UUID(int=0)
            else:
                return ActionResult(success=False, message=f"Unknown version: {version}")

            result = str(id)
            if uppercase:
                result = result.upper()
            if no_dashes:
                result = result.replace('-', '')

            result_data = {
                'uuid': result,
                'version': version,
                'variant': id.variant,
                'bytes': id.bytes.hex()
            }

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message=f"UUID生成: {result}",
                data=result_data
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"UUID生成失败: {str(e)}"
            )

    def _get_namespace(self, ns: str) -> uuid.UUID:
        """Get namespace UUID."""
        namespaces = {
            'dns': uuid.NAMESPACE_DNS,
            'url': uuid.NAMESPACE_URL,
            'oid': uuid.NAMESPACE_OID,
            'x500': uuid.NAMESPACE_X500,
        }
        if ns in namespaces:
            return namespaces[ns]
        return uuid.UUID(ns)

    def get_required_params(self) -> List[str]:
        return ['version']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'namespace': None,
            'name': None,
            'uppercase': False,
            'no_dashes': False,
            'save_to_var': None
        }


class IDGenerateAction(BaseAction):
    """Generate sequential or timestamp-based IDs.
    
    Supports timestamp-prefixed, sequential,
    and hash-based identifiers.
    """
    action_type = "id_generate"
    display_name = "生成ID"
    description = "生成序列号或时间戳ID"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Generate ID.
        
        Args:
            context: Execution context.
            params: Dict with keys: prefix, suffix, timestamp,
                   random, sequential, padding, save_to_var.
        
        Returns:
            ActionResult with generated ID.
        """
        prefix = params.get('prefix', '')
        suffix = params.get('suffix', '')
        timestamp = params.get('timestamp', True)
        random = params.get('random', False)
        sequential = params.get('sequential', False)
        padding = params.get('padding', 6)
        save_to_var = params.get('save_to_var', None)

        parts = []

        if prefix:
            parts.append(str(prefix))

        if timestamp:
            ts = int(time.time() * 1000)
            parts.append(str(ts))

        if sequential:
            seq = getattr(self, '_seq_counter', 0)
            self._seq_counter = seq + 1
            parts.append(str(seq).zfill(padding))

        if random:
            import secrets
            parts.append(secrets.token_hex(4))

        if suffix:
            parts.append(str(suffix))

        id_str = '_'.join(parts)

        result_data = {
            'id': id_str,
            'parts': parts,
            'timestamp': int(time.time() * 1000) if timestamp else None
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"ID生成: {id_str}",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'prefix': '',
            'suffix': '',
            'timestamp': True,
            'random': False,
            'sequential': False,
            'padding': 6,
            'save_to_var': None
        }
