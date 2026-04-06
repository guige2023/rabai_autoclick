# Utils Directory

This directory contains utility modules for RabAI AutoClick.

## Overview

Utilities provide supporting functionality like logging, hotkey management, memory optimization, and more.

## Files

| File | Description |
|------|-------------|
| `hotkey.py` | Global hotkey management |
| `logger.py` | Basic logging functionality |
| `app_logger.py` | Application-wide logging |
| `memory.py` | Memory management and optimization |
| `recording.py` | Action recording functionality |
| `recording_mac.py` | macOS-specific recording (pynput) |
| `history.py` | Workflow history management |
| `key_display.py` | Key display overlay |
| `key_display_standalone.py` | Standalone key display app |
| `teaching_mode.py` | Teaching mode functionality |
| `teaching_window.py` | Teaching window overlay |
| `window_selector.py` | Window selection utility |
| `execution_stats.py` | Execution statistics |
| `variable_manager.py` | Variable management |

## Usage Examples

### HotkeyManager

```python
from utils.hotkey import HotkeyManager

manager = HotkeyManager()

# Register a hotkey
def on_hotkey():
    print("Hotkey pressed!")
    
manager.register('f6', on_hotkey)
manager.start()

# Parse and format hotkeys
parsed = HotkeyManager.parse_hotkey('Ctrl + Shift + A')
formatted = HotkeyManager.format_hotkey('ctrl+shift+a')
```

### AppLogger

```python
from utils.app_logger import AppLogger, app_logger

logger = AppLogger()

# Log messages
logger.info("Operation started", "ModuleName")
logger.warning("Low memory", "MemoryManager")
logger.error("Failed to connect", "Network")

# Get log entries
entries = logger.get_entries(100)
```

### MemoryManager

```python
from utils.memory import MemoryManager, memory_manager

manager = MemoryManager()

# Get system memory info
info = manager.get_memory_info()
print(f"Used: {info['used']}MB / {info['total']}MB")

# Optimize memory
manager.optimize()
```

### WorkflowHistoryManager

```python
from utils.history import WorkflowHistoryManager

manager = WorkflowHistoryManager("./history")

# Save workflow
filepath = manager.save_workflow("my_workflow", workflow_data, ["tag1"])

# Load workflow
loaded = manager.load_workflow("my_workflow.json")

# List all workflows
all_workflows = manager.get_all_workflows()

# Delete workflow
manager.delete_workflow("my_workflow.json")
```

### RecordingManager

```python
from utils.recording import RecordingManager

manager = RecordingManager()

# Start recording
manager.start_recording()

# Check if recording
if manager.is_recording():
    print("Recording in progress...")

# Stop recording
actions = manager.stop_recording()

# Convert to workflow format
workflow = manager.to_workflow()
```

### TeachingModeManager

```python
from utils.teaching_mode import TeachingModeManager, teaching_mode_manager

manager = TeachingModeManager()

# Enable teaching mode
if manager.enable():
    print("Teaching mode enabled")
    
# Check status
if manager.is_enabled():
    print("Teaching mode is active")

# Toggle on/off
manager.toggle()

# Disable
manager.disable()
```

## Singleton Pattern

Most utility classes use the singleton pattern for global access:

```python
from utils.app_logger import app_logger  # Global instance
from utils.memory import memory_manager   # Global instance

# These are the same instances throughout the app
```

## Thread Safety

Some utilities (like HotkeyManager) run in separate threads or processes:

- **HotkeyManager**: Uses pynput listener in separate thread
- **RecordingManager (macOS)**: Uses pynput in separate process
- **TeachingModeManager**: Uses pynput listeners in separate threads

Always call `manager.stop()` or `manager.disable()` before exiting to clean up listeners.
