"""
Example Plugin for RabAI AutoClick.

This module exports the ExamplePlugin class for use by the plugin manager.
"""

from .example_action import ExamplePlugin, register

__all__ = ['ExamplePlugin', 'register']
__version__ = "1.0.0"
