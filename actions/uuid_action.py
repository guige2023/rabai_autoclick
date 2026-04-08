"""UUID action module for RabAI AutoClick.

Provides UUID generation and manipulation actions.
"""

import uuid
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class UuidGenerateAction(BaseAction):
    """Generate UUID.
    
    Creates various types of UUIDs.
    """
    action_type = "uuid_generate"
    display_name = "生成UUID"
    description = "生成UUID标识符"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate UUID.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: uuid_type (uuid4/uuid1/ uuid3/uuid5), count.
        
        Returns:
            ActionResult with generated UUID(s).
        """
        uuid_type = params.get('uuid_type', 'uuid4')
        count = params.get('count', 1)
        
        if count < 1 or count > 1000:
            return ActionResult(success=False, message="count must be 1-1000")
        
        try:
            uuids = []
            for _ in range(count):
                if uuid_type == 'uuid1':
                    uuids.append(str(uuid.uuid1()))
                elif uuid_type == 'uuid3':
                    uuids.append(str(uuid.uuid3(uuid.NAMESPACE_DNS, 'example.com')))
                elif uuid_type == 'uuid5':
                    uuids.append(str(uuid.uuid5(uuid.NAMESPACE_DNS, 'example.com')))
                else:  # uuid4
                    uuids.append(str(uuid.uuid4()))
            
            result = uuids[0] if count == 1 else uuids
            
            return ActionResult(
                success=True,
                message=f"Generated {count} UUID(s)",
                data={'uuid': result, 'count': count}
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"UUID generation error: {e}",
                data={'error': str(e)}
            )


class UuidValidateAction(BaseAction):
    """Validate UUID format.
    
    Checks if a string is a valid UUID.
    """
    action_type = "uuid_validate"
    display_name = "验证UUID"
    description = "验证UUID格式是否有效"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Validate UUID.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: uuid_string.
        
        Returns:
            ActionResult with validation status.
        """
        uuid_string = params.get('uuid_string', '')
        
        if not uuid_string:
            return ActionResult(success=False, message="uuid_string required")
        
        try:
            parsed = uuid.UUID(uuid_string)
            is_valid = True
            
            return ActionResult(
                success=True,
                message=f"{'Valid' if is_valid else 'Invalid'} UUID: {parsed}",
                data={
                    'uuid': str(parsed),
                    'version': parsed.version,
                    'is_valid': is_valid
                }
            )
            
        except ValueError:
            return ActionResult(
                success=True,
                message="Invalid UUID format",
                data={'is_valid': False, 'uuid': uuid_string}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Validation error: {e}",
                data={'error': str(e)}
            )


class UuidParseAction(BaseAction):
    """Parse UUID into components.
    
    Extracts version and variant from UUID.
    """
    action_type = "uuid_parse"
    display_name = "解析UUID"
    description = "解析UUID的组成部分"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Parse UUID.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: uuid_string.
        
        Returns:
            ActionResult with UUID components.
        """
        uuid_string = params.get('uuid_string', '')
        
        if not uuid_string:
            return ActionResult(success=False, message="uuid_string required")
        
        try:
            parsed = uuid.UUID(uuid_string)
            
            return ActionResult(
                success=True,
                message=f"UUID parsed: version {parsed.version}",
                data={
                    'uuid': str(parsed),
                    'version': parsed.version,
                    'variant': str(parsed.variant),
                    'fields': parsed.fields,
                    'node': parsed.node,
                    'time': parsed.time if hasattr(parsed, 'time') else None
                }
            )
            
        except ValueError:
            return ActionResult(
                success=False,
                message="Invalid UUID format"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Parse error: {e}",
                data={'error': str(e)}
            )
