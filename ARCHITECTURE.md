# RabAI AutoClick Architecture

## System Overview

RabAI AutoClick is a desktop automation tool that allows users to record, edit, and replay mouse and keyboard actions. The system consists of a GUI layer, execution engine, and extensible action system.

```
┌─────────────────────────────────────────────────────────────────┐
│                         GUI Layer                               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐ │
│  │ MainWindow  │  │ WorkflowView │  │ ActionEditor           │ │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘ │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│                      Core Engine                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐ │
│  │   Engine    │  │  Context    │  │  ActionLoader          │ │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘ │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│                      Action System (34+ Actions)                 │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐  │
│  │  Mouse  │ │ Keyboard│ │   OCR   │ │  Image  │ │Control  │  │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Core Components

### 1. Execution Engine (`core/engine.py`)

The central component that orchestrates workflow execution.

**Responsibilities:**
- Parse and validate workflows
- Execute actions in sequence or parallel
- Manage execution state (running, paused, stopped)
- Handle exceptions and retries
- Coordinate with self-healing and predictive systems

**Key Classes:**
```python
class Engine:
    def __init__(self):
        self.context = ExecutionContext()
        self.action_loader = ActionLoader()
        self.self_healing = SelfHealingSystem()
        self.predictive = PredictiveEngine()

    def execute(self, workflow: Workflow) -> ExecutionResult:
        """Execute a complete workflow."""
        pass

    def execute_step(self, action: Action) -> ActionResult:
        """Execute a single action step."""
        pass

    def pause(self):
        """Pause execution."""
        pass

    def resume(self):
        """Resume execution."""
        pass

    def stop(self):
        """Stop execution."""
        pass
```

### 2. Execution Context (`core/context.py`)

Maintains state during workflow execution.

**Responsibilities:**
- Store variables and their values
- Track execution history
- Manage loop counters and conditions
- Provide access to runtime environment

**Key Classes:**
```python
class ExecutionContext:
    def __init__(self):
        self.variables: Dict[str, Any] = {}
        self.loop_counters: Dict[str, int] = {}
        self.execution_history: List[ActionResult] = []
        self.metadata: Dict[str, Any] = {}

    def set_variable(self, name: str, value: Any):
        """Set a variable value."""
        pass

    def get_variable(self, name: str) -> Any:
        """Get a variable value."""
        pass
```

### 3. Action Loader (`core/action_loader.py`)

Dynamically loads and registers action implementations.

**Responsibilities:**
- Discover action implementations
- Validate action schemas
- Provide action metadata
- Manage action lifecycle

**Key Classes:**
```python
class ActionLoader:
    def __init__(self):
        self.actions: Dict[str, Type[BaseAction]] = {}

    def load_actions(self):
        """Load all actions from the actions directory."""
        pass

    def get_action(self, action_type: str) -> BaseAction:
        """Get an action instance by type."""
        pass

    def list_actions(self) -> List[ActionMetadata]:
        """List all available actions."""
        pass
```

---

## Action System Design

### Base Action (`core/base_action.py`)

All actions inherit from `BaseAction`:

```python
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from dataclasses import dataclass

@dataclass
class ActionResult:
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None
    screenshot: Optional[str] = None

class BaseAction(ABC):
    action_type: str = ""
    display_name: str = ""
    description: str = ""
    category: str = "general"

    @abstractmethod
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute the action with given parameters."""
        pass

    def get_required_params(self) -> List[str]:
        """Return list of required parameter names."""
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        """Return dict of optional parameters with defaults."""
        return {}

    def validate_params(self, params: Dict[str, Any]) -> bool:
        """Validate parameters before execution."""
        return True
```

### Action Categories

| Category | Description | Actions |
|----------|-------------|---------|
| `mouse` | Mouse operations | click, mouse_click, double_click, scroll, mouse_move, drag |
| `keyboard` | Keyboard operations | type_text, key_press |
| `ocr` | OCR and text recognition | ocr |
| `image` | Image matching | click_image, find_image |
| `control` | Flow control | loop, loop_while, condition, try_catch, goto, label |
| `system` | System operations | screenshot, get_mouse_pos, alert, set_variable, delay |
| `wait` | Wait operations | wait_for_image, wait_for_text, wait_for_element |

### Action Execution Flow

```
┌─────────────┐
│   Engine    │
└──────┬──────┘
       │
       ▼
┌─────────────┐     ┌─────────────────┐
│ ActionLoader│────►│  BaseAction     │
└─────────────┘     │  .execute()     │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │   Validation    │
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
              ▼              ▼              ▼
        ┌──────────┐  ┌──────────┐  ┌──────────┐
        │ Success  │  │  Retry   │  │ Failure  │
        └──────────┘  └──────────┘  └──────────┘
                                         │
                                         ▼
                                 ┌──────────────────┐
                                 │ SelfHealingSystem │
                                 └──────────────────┘
```

---

## Extension Points

### 1. Custom Actions

Extend the system with custom actions:

```python
# actions/custom/my_custom_action.py
from core.base_action import BaseAction, ActionResult

class MyCustomAction(BaseAction):
    action_type = "my_custom_action"
    display_name = "My Custom Action"
    description = "A custom action for..."
    category = "custom"

    def execute(self, context, params):
        # Custom implementation
        return ActionResult(success=True, message="Done")
```

### 2. Plugins

The system supports plugins via entry points:

```toml
# pyproject.toml
[project.entry-points."rabai.plugins"]
my_plugin = "rabai_plugins.my_plugin:register"
```

### 3. Action Decorators

Use decorators to add functionality:

```python
@retry(max_attempts=3, delay=1.0)
@log_action
def execute(self, context, params):
    pass
```

---

## Data Flow

### Workflow Execution

```
User Input (GUI/CLI)
       │
       ▼
┌──────────────┐
│   Workflow   │
│   (JSON/YAML)│
└──────┬───────┘
       │
       ▼
┌──────────────┐     ┌──────────────┐
│    Engine    │────►│   Context    │
│  Validation  │     │  (State)     │
└──────┬───────┘     └──────────────┘
       │
       ▼
┌─────────────────────────────────────┐
│         Action Execution Loop        │
│  ┌───────┐  ┌───────┐  ┌───────┐   │
│  │Action1│→ │Action2│→ │Action3│→...│
│  └───┬───┘  └───┬───┘  └───┬───┘   │
│      │          │          │        │
│      ▼          ▼          ▼        │
│  ┌───────┐  ┌───────┐  ┌───────┐   │
│  │Result1│  │Result2│  │Result3│   │
│  └───────┘  └───────┘  └───────┘   │
└─────────────────────────────────────┘
       │
       ▼
┌──────────────┐
│  Execution   │
│    Result    │
└──────────────┘
```

### Context Data Flow

```
┌─────────────────────────────────────────────────────────┐
│                    ExecutionContext                      │
├─────────────────────────────────────────────────────────┤
│  Variables    │  Loop Counters  │  History  │ Metadata │
│  ─────────    │  ─────────────  │  ───────  │ ──────── │
│  x = 100      │  loop_1 = 5     │ [r1,r2]   │ app_name │
│  name = "test"│                 │           │ start_time│
└─────────────────────────────────────────────────────────┘
                           ▲
                           │
                    ┌──────┴──────┐
                    │ Set/Get     │
                    │ Variables   │
                    └─────────────┘
```

---

## Self-Healing System (`src/self_healing_system.py`)

Automatic recovery from action failures.

```
Action Fails
     │
     ▼
┌─────────────────┐
│ Failure Detect  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Retry Strategy  │◄──────┐
│ (max_attempts)  │       │
└────────┬────────┘       │
         │                │
    ┌────┴────┐          │
    │         │          │
    ▼         ▼          │
┌───────┐ ┌───────┐      │
│ Success│ │ Fail  │──────┘
└───────┘ └───────┘
```

**Recovery Strategies:**
1. Retry with same parameters
2. Retry with adjusted parameters
3. Skip action and continue
4. Fallback to alternative action
5. Stop workflow with error

---

## Predictive Engine (`src/predictive_engine.py`)

Predicts and pre-executes likely next actions.

```
┌─────────────────────────────────────────────────────┐
│              Predictive Engine                       │
├─────────────────────────────────────────────────────┤
│  ┌───────────┐   ┌───────────┐   ┌───────────┐    │
│  │ History   │ → │  Pattern  │ → │ Prediction│    │
│  │ Analysis  │   │ Matching  │   │  Output   │    │
│  └───────────┘   └───────────┘   └───────────┘    │
│                                         │          │
│                                         ▼          │
│                               ┌──────────────────┐ │
│                               │ Pre-execution    │ │
│                               │ (if confident)   │ │
│                               └──────────────────┘ │
└─────────────────────────────────────────────────────┘
```

---

## Workflow Diagnostics (`src/workflow_diagnostics.py`)

Real-time monitoring and analysis.

**Metrics Tracked:**
- Action execution time
- Success/failure rate
- Memory usage
- CPU usage
- Error frequency

**Output:**
- Real-time dashboard (GUI)
- Log files
- Execution reports

---

## File Structure

```
rabai_autoclick/
├── actions/                    # Action implementations
│   ├── __init__.py            # Action registration
│   ├── base_action.py         # Base class (may be in core)
│   ├── click.py               # Basic click action
│   ├── mouse.py               # Mouse actions (6 actions)
│   ├── keyboard.py            # Keyboard actions (2 actions)
│   ├── ocr.py                 # OCR action
│   ├── image_match.py         # Image matching (2 actions)
│   ├── script.py              # Script/condition/loop (6 actions)
│   ├── loop_while.py          # While loop controls (4 actions)
│   ├── try_catch.py           # Exception handling (4 actions)
│   ├── wait_for.py            # Wait actions (3 actions)
│   ├── system.py              # System actions (3 actions)
│   └── comment.py             # Label/goto/comment (4 actions)
│
├── cli/                        # CLI implementation
│   └── main.py                # CLI entry points
│
├── core/                       # Core engine
│   ├── engine.py              # Main execution engine
│   ├── context.py             # Execution context
│   └── action_loader.py       # Action loading
│
├── gui/                        # GUI components
│
├── src/                        # Advanced features
│   ├── self_healing_system.py
│   ├── predictive_engine.py
│   ├── workflow_diagnostics.py
│   ├── pipeline_mode.py
│   ├── screen_recorder.py
│   ├── workflow_share.py
│   └── workflow_package.py
│
├── tests/                      # Test suite
│   ├── test_core/
│   ├── test_actions/
│   └── conftest.py
│
├── ui/                         # PyQt5 UI
│
├── utils/                      # Utilities
│   ├── hotkey.py
│   └── logging.py
│
├── main.py                     # GUI entry point
├── pyproject.toml
└── README.md
```

---

## Dependencies

| Package | Purpose | Version |
|---------|---------|---------|
| PyQt5 | GUI framework | >=5.15.0 |
| pyautogui | Cross-platform GUI automation | >=0.9.53 |
| opencv-python | Image processing | >=4.5.0 |
| pynput | Global hotkeys | >=1.7.0 |
| rapidocr-onnxruntime | OCR engine | >=1.3.0 |
| numpy | Numerical operations | >=1.19.0 |
| psutil | System info | >=5.8.0 |
| Pillow | Image processing | >=8.0.0 |
| click | CLI framework | >=8.1.0 |

---

## Platform-Specific Considerations

### Windows
- Uses `pyautogui` for automation
- Win32 API for window management
- Global hotkeys via `pynput`

### macOS
- Requires Accessibility permissions
- Uses ` CGEvent` for mouse/keyboard
- Cocoa APIs for window management

### Linux
- X11 automation support
- XRecord for hotkeys
- (Limited platform support)
