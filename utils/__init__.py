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
    # Transform - no PyQt5
    'transform': ('utils.transform', 'transform'),
    'transform_dict': ('utils.transform', 'transform_dict'),
    'filter_map': ('utils.transform', 'filter_map'),
    'flatten': ('utils.transform', 'flatten'),
    'group_by': ('utils.transform', 'group_by'),
    'chunk': ('utils.transform', 'chunk'),
    'pluck': ('utils.transform', 'pluck'),
    'merge': ('utils.transform', 'merge'),
    'pick': ('utils.transform', 'pick'),
    'omit': ('utils.transform', 'omit'),
    'map_values': ('utils.transform', 'map_values'),
    'invert': ('utils.transform', 'invert'),
    'deep_get': ('utils.transform', 'deep_get'),
    'deep_set': ('utils.transform', 'deep_set'),
    'sanitize_string': ('utils.transform', 'sanitize_string'),
    'truncate': ('utils.transform', 'truncate'),
    'normalize_whitespace': ('utils.transform', 'normalize_whitespace'),
    'camel_to_snake': ('utils.transform', 'camel_to_snake'),
    'snake_to_camel': ('utils.transform', 'snake_to_camel'),
    'parse_int': ('utils.transform', 'parse_int'),
    'parse_float': ('utils.transform', 'parse_float'),
    'parse_bool': ('utils.transform', 'parse_bool'),
    'coerce_type': ('utils.transform', 'coerce_type'),
    # File Watcher - no PyQt5
    'FileWatcher': ('utils.file_watcher', 'FileWatcher'),
    'FileEvent': ('utils.file_watcher', 'FileEvent'),
    'FileEventType': ('utils.file_watcher', 'FileEventType'),
    'ConfigWatcher': ('utils.file_watcher', 'ConfigWatcher'),
    'DirectoryWatcher': ('utils.file_watcher', 'DirectoryWatcher'),
    # Math Utils - no PyQt5
    'clamp': ('utils.math_utils', 'clamp'),
    'lerp': ('utils.math_utils', 'lerp'),
    'inverse_lerp': ('utils.math_utils', 'inverse_lerp'),
    'map_range': ('utils.math_utils', 'map_range'),
    'mean': ('utils.math_utils', 'mean'),
    'median': ('utils.math_utils', 'median'),
    'mode': ('utils.math_utils', 'mode'),
    'variance': ('utils.math_utils', 'variance'),
    'standard_deviation': ('utils.math_utils', 'standard_deviation'),
    'percentile': ('utils.math_utils', 'percentile'),
    'min_max_normalize': ('utils.math_utils', 'min_max_normalize'),
    'z_score': ('utils.math_utils', 'z_score'),
    'round_to_decimal': ('utils.math_utils', 'round_to_decimal'),
    'is_close': ('utils.math_utils', 'is_close'),
    'gcd': ('utils.math_utils', 'gcd'),
    'lcm': ('utils.math_utils', 'lcm'),
    'is_prime': ('utils.math_utils', 'is_prime'),
    'fibonacci': ('utils.math_utils', 'fibonacci'),
    'factorial': ('utils.math_utils', 'factorial'),
    'combinations': ('utils.math_utils', 'combinations'),
    'permutations': ('utils.math_utils', 'permutations'),
    'format_number': ('utils.math_utils', 'format_number'),
    'format_bytes': ('utils.math_utils', 'format_bytes'),
    'format_duration': ('utils.math_utils', 'format_duration'),
    'distance_2d': ('utils.math_utils', 'distance_2d'),
    'distance_3d': ('utils.math_utils', 'distance_3d'),
    'manhattan_distance': ('utils.math_utils', 'manhattan_distance'),
    'chebyshev_distance': ('utils.math_utils', 'chebyshev_distance'),
    # Collection Utils - no PyQt5
    'unique': ('utils.collection_utils', 'unique'),
    'unique_by': ('utils.collection_utils', 'unique_by'),
    'partition': ('utils.collection_utils', 'partition'),
    'first': ('utils.collection_utils', 'first'),
    'last': ('utils.collection_utils', 'last'),
    'sample': ('utils.collection_utils', 'sample'),
    'transpose': ('utils.collection_utils', 'transpose'),
    'zip_with': ('utils.collection_utils', 'zip_with'),
    'batch': ('utils.collection_utils', 'batch'),
    'sliding_window': ('utils.collection_utils', 'sliding_window'),
    'count_by': ('utils.collection_utils', 'count_by'),
    'group_by_to_dict': ('utils.collection_utils', 'group_by_to_dict'),
    'intersection': ('utils.collection_utils', 'intersection'),
    'union': ('utils.collection_utils', 'union'),
    'difference': ('utils.collection_utils', 'difference'),
    'symmetric_difference': ('utils.collection_utils', 'symmetric_difference'),
    'find': ('utils.collection_utils', 'find'),
    'find_index': ('utils.collection_utils', 'find_index'),
    'contains': ('utils.collection_utils', 'contains'),
    'all_match': ('utils.collection_utils', 'all_match'),
    'none_match': ('utils.collection_utils', 'none_match'),
    'sort_by': ('utils.collection_utils', 'sort_by'),
    'chunk_list': ('utils.collection_utils', 'chunk_list'),
    'deduplicate': ('utils.collection_utils', 'deduplicate'),
    # File Utils - no PyQt5
    'ensure_dir': ('utils.file_utils', 'ensure_dir'),
    'ensure_parent_dir': ('utils.file_utils', 'ensure_parent_dir'),
    'read_text': ('utils.file_utils', 'read_text'),
    'write_text': ('utils.file_utils', 'write_text'),
    'read_bytes': ('utils.file_utils', 'read_bytes'),
    'write_bytes': ('utils.file_utils', 'write_bytes'),
    'read_json': ('utils.file_utils', 'read_json'),
    'write_json': ('utils.file_utils', 'write_json'),
    'copy_file': ('utils.file_utils', 'copy_file'),
    'move_file': ('utils.file_utils', 'move_file'),
    'delete_file': ('utils.file_utils', 'delete_file'),
    'delete_dir': ('utils.file_utils', 'delete_dir'),
    'file_exists': ('utils.file_utils', 'file_exists'),
    'dir_exists': ('utils.file_utils', 'dir_exists'),
    'get_size': ('utils.file_utils', 'get_size'),
    'get_extension': ('utils.file_utils', 'get_extension'),
    'get_name': ('utils.file_utils', 'get_name'),
    'get_basename': ('utils.file_utils', 'get_basename'),
    'list_files': ('utils.file_utils', 'list_files'),
    'list_dirs': ('utils.file_utils', 'list_dirs'),
    'walk_dir': ('utils.file_utils', 'walk_dir'),
    'is_empty_dir': ('utils.file_utils', 'is_empty_dir'),
    'get_relative_path': ('utils.file_utils', 'get_relative_path'),
    'join_paths': ('utils.file_utils', 'join_paths'),
    'normalize_path': ('utils.file_utils', 'normalize_path'),
    'is_absolute': ('utils.file_utils', 'is_absolute'),
    'make_absolute': ('utils.file_utils', 'make_absolute'),
    # Env Utils - no PyQt5
    'get_env': ('utils.env_utils', 'get_env'),
    'set_env': ('utils.env_utils', 'set_env'),
    'unset_env': ('utils.env_utils', 'unset_env'),
    'get_env_int': ('utils.env_utils', 'get_env_int'),
    'get_env_bool': ('utils.env_utils', 'get_env_bool'),
    'get_env_list': ('utils.env_utils', 'get_env_list'),
    'get_all_env': ('utils.env_utils', 'get_all_env'),
    'has_env': ('utils.env_utils', 'has_env'),
    'is_linux': ('utils.env_utils', 'is_linux'),
    'is_macos': ('utils.env_utils', 'is_macos'),
    'is_windows': ('utils.env_utils', 'is_windows'),
    'get_platform': ('utils.env_utils', 'get_platform'),
    'get_os_version': ('utils.env_utils', 'get_os_version'),
    'get_python_version': ('utils.env_utils', 'get_python_version'),
    'get_platform_info': ('utils.env_utils', 'get_platform_info'),
    'get_cpu_count': ('utils.env_utils', 'get_cpu_count'),
    'get_home_dir': ('utils.env_utils', 'get_home_dir'),
    'get_temp_dir': ('utils.env_utils', 'get_temp_dir'),
    'get_current_dir': ('utils.env_utils', 'get_current_dir'),
    'is_64bit': ('utils.env_utils', 'is_64bit'),
    'is_admin': ('utils.env_utils', 'is_admin'),
    'get_hostname': ('utils.env_utils', 'get_hostname'),
    'get_username': ('utils.env_utils', 'get_username'),
    'get_env_with_prefix': ('utils.env_utils', 'get_env_with_prefix'),
    'set_env_from_dict': ('utils.env_utils', 'set_env_from_dict'),
    'load_env_file': ('utils.env_utils', 'load_env_file'),
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