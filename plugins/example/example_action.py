"""
Example Action Plugin for RabAI AutoClick.

This plugin demonstrates how to create a plugin for the RabAI AutoClick system.
It provides a simple action that can be executed as part of a workflow.
"""

import time
from typing import Any, Dict, Optional


class ExamplePlugin:
    """Example plugin that demonstrates the plugin interface."""
    
    name = "example_plugin"
    version = "1.0.0"
    description = "Example plugin demonstrating the plugin system"
    author = "RabAI Team"
    
    def __init__(self):
        self._enabled = True
        self._context: Dict[str, Any] = {}
        self._execution_count = 0
    
    def on_load(self) -> bool:
        """Called when the plugin is loaded."""
        print(f"[ExamplePlugin] Plugin loaded: {self.name} v{self.version}")
        return True
    
    def on_unload(self) -> bool:
        """Called when the plugin is unloaded."""
        print(f"[ExamplePlugin] Plugin unloaded: {self.name}")
        return True
    
    def execute(self, action: str = "default", **kwargs) -> Dict[str, Any]:
        """
        Execute the plugin's main functionality.
        
        Args:
            action: The action to perform (default, greet, count, delay)
            **kwargs: Additional arguments for the action
            
        Returns:
            Dictionary containing the result of the action
        """
        self._execution_count += 1
        
        if action == "greet":
            name = kwargs.get("name", "World")
            message = kwargs.get("message", f"Hello, {name}!")
            return {
                "success": True,
                "action": action,
                "message": message,
                "execution_count": self._execution_count,
            }
        
        elif action == "count":
            return {
                "success": True,
                "action": action,
                "execution_count": self._execution_count,
                "message": f"Plugin has been executed {self._execution_count} times",
            }
        
        elif action == "delay":
            seconds = kwargs.get("seconds", 1)
            time.sleep(min(seconds, 10))  # Max 10 seconds
            return {
                "success": True,
                "action": action,
                "delay_seconds": seconds,
                "message": f"Delayed for {seconds} seconds",
            }
        
        else:  # default
            return {
                "success": True,
                "action": "default",
                "message": "Example plugin executed successfully",
                "execution_count": self._execution_count,
                "plugin_name": self.name,
                "plugin_version": self.version,
            }
    
    def enable(self) -> None:
        """Enable the plugin."""
        self._enabled = True
    
    def disable(self) -> None:
        """Disable the plugin."""
        self._enabled = False
    
    @property
    def is_enabled(self) -> bool:
        return self._enabled
    
    @property
    def context(self) -> Dict[str, Any]:
        """Get the plugin context."""
        return self._context
    
    def set_context(self, key: str, value: Any) -> None:
        """Set a value in the plugin context."""
        self._context[key] = value
    
    def get_context(self, key: str, default: Any = None) -> Any:
        """Get a value from the plugin context."""
        return self._context.get(key, default)


# Module-level entry point for the plugin loader
def register() -> ExamplePlugin:
    """Register the plugin with RabAI AutoClick."""
    return ExamplePlugin()
