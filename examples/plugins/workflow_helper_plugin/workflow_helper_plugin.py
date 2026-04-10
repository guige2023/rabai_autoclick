"""
Workflow Helper Plugin implementation.
"""

import re
import time
import json
from typing import Any, Dict, Optional, List

from ....src.plugin_system import PluginBase, PluginAPI


class WorkflowHelperPlugin(PluginBase):
    """Plugin that provides utility actions for workflows."""
    
    name = "workflow_helper_plugin"
    version = "1.0.0"
    description = "Utility actions for workflow manipulation and data transformation"
    author = "RabAI Team"
    
    def __init__(self, api: PluginAPI):
        super().__init__(api)
        self._action_count = 0
        self._default_log_level = "INFO"
        self._enable_context_logging = False
    
    def on_load(self) -> bool:
        """Initialize the workflow helper plugin."""
        self._api.logger.info(f"Loading {self.name} v{self.version}")
        
        # Load config
        self._default_log_level = self._api.get_config("default_log_level", "INFO")
        self._enable_context_logging = self._api.get_config("enable_context_logging", False)
        
        # Register utility actions
        self._api.register_action(
            action_type="transform_data",
            handler=self._transform_data_handler,
            description="Transform data using various operations",
            schema={
                "operation": {
                    "type": "string",
                    "required": True,
                    "enum": ["uppercase", "lowercase", "trim", "json_parse", "json_dump", "regex_replace", "template"]
                },
                "data": {"type": "any", "required": True},
                "pattern": {"type": "string", "required": False},
                "replacement": {"type": "string", "required": False},
                "template": {"type": "string", "required": False}
            }
        )
        
        self._api.register_action(
            action_type="log_message",
            handler=self._log_message_handler,
            description="Log a message with custom formatting",
            schema={
                "message": {"type": "string", "required": True},
                "level": {"type": "string", "required": False, "default": "INFO"},
                "include_timestamp": {"type": "boolean", "required": False, "default": true}
            }
        )
        
        self._api.register_action(
            action_type="merge_context",
            handler=self._merge_context_handler,
            description="Merge data into the workflow context",
            schema={
                "data": {"type": "object", "required": True},
                "key": {"type": "string", "required": False},
                "overwrite": {"type": "boolean", "required": False, "default": true}
            }
        )
        
        self._api.register_action(
            action_type="condition_check",
            handler=self._condition_check_handler,
            description="Evaluate a condition and return result",
            schema={
                "operator": {
                    "type": "string",
                    "required": True,
                    "enum": ["equals", "not_equals", "greater_than", "less_than", "contains", "matches_regex", "is_empty", "is_not_empty"]
                },
                "value": {"type": "any", "required": True},
                "compare_to": {"type": "any", "required": False}
            }
        )
        
        return True
    
    def on_unload(self) -> bool:
        """Clean up when plugin is unloaded."""
        self._api.logger.info(f"Unloading {self.name}")
        
        # Unregister all actions
        self._api.unregister_action("transform_data")
        self._api.unregister_action("log_message")
        self._api.unregister_action("merge_context")
        self._api.unregister_action("condition_check")
        
        return True
    
    def on_enable(self) -> None:
        """Called when plugin is enabled."""
        self._api.logger.info(f"{self.name} enabled")
    
    def on_disable(self) -> None:
        """Called when plugin is disabled."""
        self._api.logger.info(f"{self.name} disabled")
    
    def _transform_data_handler(self, **kwargs) -> Dict[str, Any]:
        """Handler for the transform_data action."""
        self._action_count += 1
        
        operation = kwargs.get("operation", "")
        data = kwargs.get("data")
        pattern = kwargs.get("pattern", "")
        replacement = kwargs.get("replacement", "")
        template = kwargs.get("template", "")
        
        result = None
        success = True
        error = None
        
        try:
            if operation == "uppercase":
                result = str(data).upper()
            elif operation == "lowercase":
                result = str(data).lower()
            elif operation == "trim":
                result = str(data).strip()
            elif operation == "json_parse":
                if isinstance(data, str):
                    result = json.loads(data)
                else:
                    result = data
            elif operation == "json_dump":
                if isinstance(data, (dict, list)):
                    result = json.dumps(data, indent=2)
                else:
                    result = str(data)
            elif operation == "regex_replace":
                if pattern:
                    result = re.sub(pattern, replacement, str(data))
                else:
                    raise ValueError("pattern is required for regex_replace")
            elif operation == "template":
                if template:
                    result = template
                    for key, value in kwargs.items():
                        if key not in ("operation", "data", "pattern", "replacement", "template"):
                            result = result.replace(f"{{{key}}}", str(value))
                    result = result.replace("{data}", str(data))
                else:
                    raise ValueError("template is required for template operation")
            else:
                raise ValueError(f"Unknown operation: {operation}")
                
        except Exception as e:
            success = False
            error = str(e)
            result = None
        
        return {
            "success": success,
            "operation": operation,
            "input": data,
            "result": result,
            "error": error,
            "action_id": self._action_count
        }
    
    def _log_message_handler(self, **kwargs) -> Dict[str, Any]:
        """Handler for the log_message action."""
        self._action_count += 1
        
        message = kwargs.get("message", "")
        level = kwargs.get("level", self._default_log_level)
        include_timestamp = kwargs.get("include_timestamp", True)
        
        timestamp_str = ""
        if include_timestamp:
            timestamp_str = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] "
        
        formatted_message = f"{timestamp_str}{message}"
        
        log_method = {
            "DEBUG": self._api.logger.debug,
            "INFO": self._api.logger.info,
            "WARNING": self._api.logger.warning,
            "ERROR": self._api.logger.error
        }.get(level.upper(), self._api.logger.info)
        
        log_method(formatted_message)
        
        # Optionally emit event
        if self._enable_context_logging:
            self._api.emit_event("workflow_log", {
                "level": level,
                "message": message,
                "timestamp": time.time()
            })
        
        return {
            "success": True,
            "message": message,
            "level": level,
            "action_id": self._action_count
        }
    
    def _merge_context_handler(self, **kwargs) -> Dict[str, Any]:
        """Handler for the merge_context action."""
        self._action_count += 1
        
        data = kwargs.get("data", {})
        key = kwargs.get("key")
        overwrite = kwargs.get("overwrite", True)
        
        if not isinstance(data, dict):
            return {
                "success": False,
                "error": "data must be a dictionary"
            }
        
        merged_keys = []
        
        if key:
            # Merge under a specific key
            existing = self._api.get_workflow_context(key, {})
            if not isinstance(existing, dict):
                existing = {}
            if overwrite:
                existing.update(data)
            else:
                for k, v in data.items():
                    if k not in existing:
                        existing[k] = v
            self._api.set_workflow_context(key, existing)
            merged_keys = list(data.keys())
        else:
            # Merge at root level
            for k, v in data.items():
                if overwrite or self._api.get_workflow_context(k) is None:
                    self._api.set_workflow_context(k, v)
                    merged_keys.append(k)
        
        return {
            "success": True,
            "merged_keys": merged_keys,
            "key": key,
            "overwrite": overwrite,
            "action_id": self._action_count
        }
    
    def _condition_check_handler(self, **kwargs) -> Dict[str, Any]:
        """Handler for the condition_check action."""
        self._action_count += 1
        
        operator = kwargs.get("operator", "")
        value = kwargs.get("value")
        compare_to = kwargs.get("compare_to")
        
        result = False
        error = None
        
        try:
            if operator == "equals":
                result = value == compare_to
            elif operator == "not_equals":
                result = value != compare_to
            elif operator == "greater_than":
                result = value > compare_to
            elif operator == "less_than":
                result = value < compare_to
            elif operator == "contains":
                result = compare_to in value if compare_to is not None else False
            elif operator == "matches_regex":
                if pattern := kwargs.get("pattern"):
                    result = bool(re.search(pattern, str(value)))
                else:
                    raise ValueError("pattern is required for matches_regex")
            elif operator == "is_empty":
                result = value is None or value == "" or (isinstance(value, (list, dict)) and len(value) == 0)
            elif operator == "is_not_empty":
                result = value is not None and value != "" and not (isinstance(value, (list, dict)) and len(value) == 0)
            else:
                raise ValueError(f"Unknown operator: {operator}")
                
        except Exception as e:
            error = str(e)
            result = False
        
        return {
            "success": error is None,
            "operator": operator,
            "value": value,
            "compare_to": compare_to,
            "result": result,
            "error": error,
            "action_id": self._action_count
        }
    
    def execute(self, action: str, **kwargs) -> Dict[str, Any]:
        """Execute a custom action provided by this plugin."""
        if action == "transform_data":
            return self._transform_data_handler(**kwargs)
        elif action == "log_message":
            return self._log_message_handler(**kwargs)
        elif action == "merge_context":
            return self._merge_context_handler(**kwargs)
        elif action == "condition_check":
            return self._condition_check_handler(**kwargs)
        else:
            return {
                "success": False,
                "error": f"Unknown action: {action}"
            }


def register() -> WorkflowHelperPlugin:
    """Register the workflow helper plugin."""
    def _create_plugin(api: PluginAPI) -> WorkflowHelperPlugin:
        return WorkflowHelperPlugin(api)
    return _create_plugin
