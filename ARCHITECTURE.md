# Architecture Overview

This document describes the high-level architecture of RabAI AutoClick.

## System Design

RabAI AutoClick follows a modular, layered architecture:

```
┌─────────────────────────────────────────────────────────┐
│                    GUI Layer (PyQt5)                     │
│  MainWindow │ ActionEditor │ RegionSelector │ Dialogs  │
├─────────────────────────────────────────────────────────┤
│                   CLI Layer (Click)                      │
│       Main CLI │ predict │ heal │ scene │ diag        │
├─────────────────────────────────────────────────────────┤
│                 Core Engine Layer                       │
│   FlowEngine │ ContextManager │ ActionLoader │ Actions  │
├─────────────────────────────────────────────────────────┤
│                v22 Advanced Features                    │
│  PredictiveEngine │ SelfHealing │ Diagnostics │ Share  │
├─────────────────────────────────────────────────────────┤
│                   Utils Layer                           │
│  Hotkey │ Logger │ Memory │ Recording │ History       │
└─────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Flow Engine (`core/engine.py`)

The central execution engine that orchestrates workflow execution:

- **Workflow Loading**: Loads workflows from JSON/dict format
- **Step Execution**: Executes steps in sequence or based on conditions
- **Flow Control**: Handles loops, conditions, and jumps
- **State Management**: Manages execution state and context

### 2. Context Manager (`core/context.py`)

Manages workflow execution state:

- **Variable Storage**: Key-value store for workflow variables
- **Variable Resolution**: Resolves `{{variable}}` and `{{expression}}` references
- **Safe Execution**: Sandboxed Python code execution
- **History Tracking**: Tracks all variable changes

### 3. Action Loader (`core/action_loader.py`)

Dynamic action module loader:

- **Dynamic Discovery**: Auto-discovers action modules in `actions/` directory
- **Module Loading**: Loads and initializes action classes
- **Action Registry**: Maintains registry of available actions

### 4. Actions (`actions/`)

Individual automation actions:

| Action | File | Description |
|--------|------|-------------|
| Click | `click.py` | Mouse click at coordinates |
| Type | `keyboard.py` | Keyboard text input |
| KeyPress | `keyboard.py` | Special key combinations |
| ImageMatch | `image_match.py` | Template-based image matching |
| OCR | `ocr.py` | Text recognition and clicking |
| Delay | `script.py` | Wait for specified seconds |
| Condition | `script.py` | Conditional branching |
| Loop | `script.py` | Loop control |
| SetVariable | `script.py` | Variable assignment |
| Screenshot | `system.py` | Screen capture |
| GetMousePos | `system.py` | Get current mouse position |

### 5. UI Components (`ui/`)

PyQt5-based graphical interface:

- **MainWindow**: Primary application window
- **ActionEditor**: Step configuration editor
- **RegionSelector**: Screen region selection tool
- **HotkeyDialog**: Hotkey configuration
- **MessageManager**: Notification system

## Data Flow

### Workflow Execution

```
User Input (GUI/CLI)
       │
       ▼
┌─────────────────┐
│  FlowEngine    │
│  load_workflow │
└────────┬───────┘
         │
         ▼
┌─────────────────┐
│ ContextManager  │
│   (variables)   │
└────────┬───────┘
         │
         ▼
┌─────────────────┐     ┌──────────────────┐
│  ActionLoader   │────▶│  Action.execute()│
│  get_action()   │     │  (ContextManager)│
└─────────────────┘     └────────┬─────────┘
                                 │
                                 ▼
                        ┌──────────────────┐
                        │   ActionResult   │
                        │ (success/error)  │
                        └──────────────────┘
```

### Recording Flow

```
User Actions (Mouse/Keyboard)
       │
       ▼
┌─────────────────┐
│ RecordingManager│
│ (pynput listener)│
└────────┬───────┘
         │
         ▼
┌─────────────────┐
│ RecordedAction  │
│ (timestamp,type,│
│  params)        │
└────────┬───────┘
         │
         ▼
┌─────────────────┐
│ to_workflow()   │
│ (conversion)    │
└─────────────────┘
```

## v22 Advanced Features

### Predictive Engine (`src/predictive_engine.py`)

Learns from user behavior to predict next actions:

- **Action Recording**: Records user actions with context
- **Pattern Recognition**: Identifies recurring action sequences
- **Prediction**: Predicts next likely action

### Self-Healing System (`src/self_healing_system.py`)

Automatically recovers from execution failures:

- **Error Detection**: Identifies failure patterns
- **Root Cause Analysis**: Determines failure causes
- **Recovery Strategies**: Implements retry/alternative approaches

### Diagnostics (`src/workflow_diagnostics.py`)

Health monitoring and analysis:

- **Trend Analysis**: Tracks success rate over time
- **Anomaly Detection**: Identifies unusual patterns
- **Health Scoring**: Rates workflow health

### Workflow Sharing (`src/workflow_share.py`)

No-code workflow sharing:

- **Link Generation**: Creates shareable workflow URLs
- **Import/Export**: JSON and Base64 format support
- **Version Control**: Tracks workflow versions

### Pipeline Mode (`src/pipeline_mode.py`)

CLI pipeline integration:

- **Chain Management**: Creates action chains
- **Linear/Branch/Parallel**: Multiple execution modes
- **Data Passing**: Chains can pass data between steps

### Screen Recorder (`src/screen_recorder.py`)

Records screen actions for workflow generation:

- **Action Recording**: Captures mouse/keyboard actions
- **Workflow Conversion**: Converts recordings to executable workflows
- **Element Detection**: Image/text/coordinate detection modes

## Design Patterns

### Singleton Pattern

Used for managers that should have only one instance:

```python
class MemoryManager:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
```

Applied to: `MemoryManager`, `AppLogger`, `MessageManager`

### Factory Pattern

Action creation uses factory-like initialization:

```python
class ActionLoader:
    def get_action(self, action_type: str) -> BaseAction:
        action_class = self._actions.get(action_type)
        if action_class:
            return action_class()
```

### Strategy Pattern

Different recovery strategies in SelfHealingSystem:

```python
class SelfHealingSystem:
    def _apply_recovery(self, strategy: RecoveryStrategy):
        if strategy == RecoveryStrategy.RETRY:
            return self._retry()
        elif strategy == RecoveryStrategy.RELOCATE:
            return self._relocate()
        # ...
```

### Observer Pattern

Event handling in FlowEngine:

```python
class FlowEngine:
    def add_listener(self, listener):
        self._listeners.append(listener)
    
    def _notify(self, event):
        for listener in self._listeners:
            listener.on_event(event)
```

## Error Handling

### Layered Error Handling

1. **Action Level**: Each action validates params and catches own errors
2. **Engine Level**: FlowEngine catches action failures and implements recovery
3. **System Level**: SelfHealingSystem analyzes patterns and suggests fixes

### Safe Execution Sandbox

The `safe_exec()` method in ContextManager provides a sandboxed Python execution:

- **Allowed**: Basic operations, context variables, math functions
- **Blocked**: `import`, file I/O, network, system commands

## Threading Model

- **Main Thread**: GUI event loop (PyQt5)
- **Worker Threads**: Action execution via QThread
- **pynput Listener**: Separate process on macOS to avoid Qt conflicts

## Performance Considerations

1. **Action Caching**: Action instances are reused across executions
2. **Lazy Loading**: Actions loaded on-demand, not all at startup
3. **Memory Management**: Max history limits prevent unbounded growth
4. **Image Caching**: Cached template images for faster matching

## Extensibility

### Adding New Actions

1. Create `actions/my_action.py`:
   ```python
   from core.base_action import BaseAction, ActionResult
   
   class MyAction(BaseAction):
       action_type = "my_action"
       # ... implement methods
   ```

2. Export in `actions/__init__.py`

### Adding CLI Commands

1. Add to `cli/main.py`:
   ```python
   @cli.command('my-command')
   def my_command():
       # implementation
   ```

### Adding v22 Features

1. Create `src/my_feature.py`
2. Export in `src/__init__.py`
3. Add CLI commands if needed
