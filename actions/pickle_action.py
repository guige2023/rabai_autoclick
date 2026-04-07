"""Pickle action module for RabAI AutoClick.

Provides pickle serialization operations:
- PickleSerializeAction: Serialize object to pickle bytes
- PickleDeserializeAction: Deserialize pickle bytes to object
- PickleDumpAction: Serialize object to pickle file
- PickleLoadAction: Load object from pickle file
- PickleToBase64Action: Convert pickle bytes to base64 string
- PickleFromBase64Action: Convert base64 string to pickle object
"""

from typing import Any, Dict, List, Optional, Union
import pickle
import base64
import os
import sys

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PickleSerializeAction(BaseAction):
    """Serialize Python object to pickle bytes."""
    action_type = "pickle_serialize"
    display_name = "Pickle序列化"
    description = "将Python对象序列化为pickle字节"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute pickle serialize operation.

        Args:
            context: Execution context.
            params: Dict with obj, protocol, output_var.

        Returns:
            ActionResult with pickle bytes.
        """
        obj = params.get('obj', None)
        protocol = params.get('protocol', pickle.HIGHEST_PROTOCOL)
        output_var = params.get('output_var', 'pickle_bytes')

        if obj is None:
            return ActionResult(success=False, message="obj is required")

        try:
            resolved_obj = context.resolve_value(obj)
            resolved_protocol = context.resolve_value(protocol)

            pickle_bytes = pickle.dumps(resolved_obj, protocol=resolved_protocol)

            context.set(output_var, pickle_bytes)
            return ActionResult(success=True, data=pickle_bytes,
                               message=f"Serialized to pickle: {len(pickle_bytes)} bytes")

        except pickle.PicklingError as e:
            return ActionResult(success=False, message=f"Pickle error: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"Serialize error: {str(e)}")


class PickleDeserializeAction(BaseAction):
    """Deserialize pickle bytes to Python object."""
    action_type = "pickle_deserialize"
    display_name = "Pickle反序列化"
    description = "将pickle字节反序列化为Python对象"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute pickle deserialize operation.

        Args:
            context: Execution context.
            params: Dict with pickle_bytes, output_var.

        Returns:
            ActionResult with deserialized object.
        """
        pickle_bytes = params.get('pickle_bytes', b'')
        output_var = params.get('output_var', 'pickle_obj')

        if not pickle_bytes:
            return ActionResult(success=False, message="pickle_bytes is required")

        try:
            resolved_bytes = context.resolve_value(pickle_bytes)

            obj = pickle.loads(resolved_bytes)

            context.set(output_var, obj)
            return ActionResult(success=True, data=obj,
                               message=f"Deserialized from pickle: {type(obj).__name__}")

        except pickle.UnpicklingError as e:
            return ActionResult(success=False, message=f"Unpickle error: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"Deserialize error: {str(e)}")


class PickleDumpAction(BaseAction):
    """Serialize object to pickle file."""
    action_type = "pickle_dump"
    display_name = "Pickle文件写入"
    description = "将对象序列化写入pickle文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute pickle dump operation.

        Args:
            context: Execution context.
            params: Dict with file_path, obj, protocol, output_var.

        Returns:
            ActionResult with write status.
        """
        file_path = params.get('file_path', '')
        obj = params.get('obj', None)
        protocol = params.get('protocol', pickle.HIGHEST_PROTOCOL)
        output_var = params.get('output_var', 'pickle_dump_result')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")
        if obj is None:
            return ActionResult(success=False, message="obj is required")

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_obj = context.resolve_value(obj)
            resolved_protocol = context.resolve_value(protocol)

            os.makedirs(os.path.dirname(resolved_path) or '.', exist_ok=True)

            with open(resolved_path, 'wb') as f:
                pickle.dump(resolved_obj, f, protocol=resolved_protocol)

            context.set(output_var, True)
            return ActionResult(success=True, data=True,
                               message=f"Dumped pickle to {resolved_path}")

        except pickle.PicklingError as e:
            return ActionResult(success=False, message=f"Pickle error: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"Pickle dump error: {str(e)}")


class PickleLoadAction(BaseAction):
    """Load object from pickle file."""
    action_type = "pickle_load"
    display_name = "Pickle文件读取"
    description = "从pickle文件加载对象"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute pickle load operation.

        Args:
            context: Execution context.
            params: Dict with file_path, output_var.

        Returns:
            ActionResult with loaded object.
        """
        file_path = params.get('file_path', '')
        output_var = params.get('pickle_obj')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            resolved_path = context.resolve_value(file_path)

            with open(resolved_path, 'rb') as f:
                obj = pickle.load(f)

            context.set(output_var, obj)
            return ActionResult(success=True, data=obj,
                               message=f"Loaded pickle from {resolved_path}: {type(obj).__name__}")

        except FileNotFoundError:
            return ActionResult(success=False, message=f"File not found: {resolved_path}")
        except pickle.UnpicklingError as e:
            return ActionResult(success=False, message=f"Unpickle error: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"Pickle load error: {str(e)}")


class PickleToBase64Action(BaseAction):
    """Convert pickle bytes to base64 string."""
    action_type = "pickle_to_base64"
    display_name = "Pickle转Base64"
    description = "将pickle字节转换为base64字符串"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute pickle to base64 operation.

        Args:
            context: Execution context.
            params: Dict with obj, protocol, output_var.

        Returns:
            ActionResult with base64 string.
        """
        obj = params.get('obj', None)
        protocol = params.get('protocol', pickle.HIGHEST_PROTOCOL)
        output_var = params.get('output_var', 'pickle_base64')

        if obj is None:
            return ActionResult(success=False, message="obj is required")

        try:
            resolved_obj = context.resolve_value(obj)
            resolved_protocol = context.resolve_value(protocol)

            pickle_bytes = pickle.dumps(resolved_obj, protocol=resolved_protocol)
            b64_str = base64.b64encode(pickle_bytes).decode('ascii')

            context.set(output_var, b64_str)
            return ActionResult(success=True, data=b64_str,
                               message=f"Converted to base64: {len(b64_str)} chars")

        except Exception as e:
            return ActionResult(success=False, message=f"Pickle to base64 error: {str(e)}")


class PickleFromBase64Action(BaseAction):
    """Convert base64 string to pickle object."""
    action_type = "pickle_from_base64"
    display_name = "Base64转Pickle"
    description = "将base64字符串转换为pickle对象"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute base64 to pickle operation.

        Args:
            context: Execution context.
            params: Dict with base64_str, output_var.

        Returns:
            ActionResult with unpickled object.
        """
        base64_str = params.get('base64_str', '')
        output_var = params.get('output_var', 'pickle_obj')

        if not base64_str:
            return ActionResult(success=False, message="base64_str is required")

        try:
            resolved_str = context.resolve_value(base64_str)

            pickle_bytes = base64.b64decode(resolved_str)
            obj = pickle.loads(pickle_bytes)

            context.set(output_var, obj)
            return ActionResult(success=True, data=obj,
                               message=f"Restored from base64: {type(obj).__name__}")

        except Exception as e:
            return ActionResult(success=False, message=f"Base64 to pickle error: {str(e)}")
