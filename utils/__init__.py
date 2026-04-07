"""Utils package for RabAI AutoClick.

This package uses lazy loading to avoid importing PyQt5-dependent modules
until they are actually needed. This allows tests to import non-PyQt5
modules without requiring PyQt5 to be installed.
"""

from typing import Any

# Lazy loading map: module_name -> (import_path, attribute_names)
_LAZY_IMPORTS = {
    # Hotkey - PyQt5 dependent
    'HotkeyManager': ('utils.hotkey', 'HotkeyManager'),
    # Logger - no PyQt5
    'Logger': ('utils.logger', 'Logger'),
    'logger': ('utils.logger', 'logger'),
    'AppLogger': ('utils.app_logger', 'AppLogger'),
    'app_logger': ('utils.app_logger', 'app_logger'),
    # Memory - PyQt5 dependent
    'MemoryManager': ('utils.memory', 'MemoryManager'),
    'ImageCache': ('utils.memory', 'ImageCache'),
    'memory_manager': ('utils.memory', 'memory_manager'),
    'image_cache': ('utils.memory', 'image_cache'),
    # Recording - PyQt5 dependent
    'RecordingManager': ('utils.recording', 'RecordingManager'),
    'RecordingEditor': ('utils.recording', 'RecordingEditor'),
    'RecordedAction': ('utils.recording', 'RecordedAction'),
    'PYNPUT_AVAILABLE': ('utils.recording', 'PYNPUT_AVAILABLE'),
    # History - PyQt5 dependent
    'WorkflowHistoryManager': ('utils.history', 'WorkflowHistoryManager'),
    'HistoryDialog': ('utils.history', 'HistoryDialog'),
    'QuickSaveDialog': ('utils.history', 'QuickSaveDialog'),
    # Key Display - PyQt5 dependent
    'KeyDisplayWindow': ('utils.key_display', 'KeyDisplayWindow'),
    'key_display_window': ('utils.key_display', 'key_display_window'),
    # Validation - no PyQt5
    'ValidationError': ('utils.validation', 'ValidationError'),
    'ValidationResult': ('utils.validation', 'ValidationResult'),
    'Severity': ('utils.validation', 'Severity'),
    'validate_workflow_config': ('utils.validation', 'validate_workflow_config'),
    'validate_step': ('utils.validation', 'validate_step'),
    'validate_coordinates': ('utils.validation', 'validate_coordinates'),
    'validate_file_path': ('utils.validation', 'validate_file_path'),
    'validate_action_params': ('utils.validation', 'validate_action_params'),
    'validate_screen_region': ('utils.validation', 'validate_screen_region'),
    'sanitize_string': ('utils.validation', 'sanitize_string'),
    'validate_json_serializable': ('utils.validation', 'validate_json_serializable'),
    # Retry - no PyQt5
    'RetryError': ('utils.retry', 'RetryError'),
    'retry': ('utils.retry', 'retry'),
    'retry_async': ('utils.retry', 'retry_async'),
    'CircuitBreaker': ('utils.retry', 'CircuitBreaker'),
    'CircuitState': ('utils.retry', 'CircuitState'),
    'CircuitBreakerConfig': ('utils.retry', 'CircuitBreakerConfig'),
    'RateLimiter': ('utils.retry', 'RateLimiter'),
    'rate_limit': ('utils.retry', 'rate_limit'),
    # Config - no PyQt5
    'ConfigManager': ('utils.config_manager', 'ConfigManager'),
    'config_manager': ('utils.config_manager', 'config_manager'),
    'AppConfig': ('utils.config_manager', 'AppConfig'),
    'DEFAULT_CONFIG': ('utils.config_manager', 'DEFAULT_CONFIG'),
    # Timer - PyQt5 dependent
    'Timer': ('utils.timer', 'Timer'),
    'timed': ('utils.timer', 'timed'),
    'IntervalTimer': ('utils.timer', 'IntervalTimer'),
    'AsyncIntervalTimer': ('utils.timer', 'AsyncIntervalTimer'),
    'Debouncer': ('utils.timer', 'Debouncer'),
    'Throttler': ('utils.timer', 'Throttler'),
    'PerformanceTimer': ('utils.timer', 'PerformanceTimer'),
    'TimingResult': ('utils.timer', 'TimingResult'),
    # Metrics - no PyQt5
    'Counter': ('utils.metrics', 'Counter'),
    'Gauge': ('utils.metrics', 'Gauge'),
    'Histogram': ('utils.metrics', 'Histogram'),
    'MetricsCollector': ('utils.metrics', 'MetricsCollector'),
    'metrics': ('utils.metrics', 'metrics'),
    # File Watcher - no PyQt5
    'FileWatcher': ('utils.file_watcher', 'FileWatcher'),
    'FileEvent': ('utils.file_watcher', 'FileEvent'),
    'FileEventType': ('utils.file_watcher', 'FileEventType'),
    'ConfigWatcher': ('utils.file_watcher', 'ConfigWatcher'),
    'DirectoryWatcher': ('utils.file_watcher', 'DirectoryWatcher'),
    # Event Bus - PyQt5 dependent
    'Event': ('utils.event_bus', 'Event'),
    'EventPriority': ('utils.event_bus', 'EventPriority'),
    'Subscription': ('utils.event_bus', 'Subscription'),
    'EventBus': ('utils.event_bus', 'EventBus'),
    'EventBusManager': ('utils.event_bus', 'EventBusManager'),
    'event_bus': ('utils.event_bus', 'event_bus'),
    'Events': ('utils.event_bus', 'Events'),
}

# Cache for imported modules
_module_cache = {}


def __getattr__(name: str) -> Any:
    """Lazy import attribute on first access."""
    if name not in _LAZY_IMPORTS:
        raise AttributeError(f"module 'utils' has no attribute '{name}'")

    if name in _module_cache:
        return _module_cache[name]

    import_path, attr_name = _LAZY_IMPORTS[name]

    # Import the module
    import importlib
    module = importlib.import_module(import_path)

    # Get the attribute
    attr = getattr(module, attr_name)

    # Cache it
    _module_cache[name] = attr

    return attr


def __dir__():
    """Return list of available attributes for tab completion."""
    return list(_LAZY_IMPORTS.keys())


# For backward compatibility, also provide __all__
__all__ = list(_LAZY_IMPORTS.keys())