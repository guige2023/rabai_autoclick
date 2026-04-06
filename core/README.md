# Core Directory

This directory contains the core engine components of RabAI AutoClick.

## Overview

The core provides the essential functionality for workflow execution, including the flow engine, context management, and action loading.

## Files

| File | Description |
|------|-------------|
| `base_action.py` | Base class for all actions |
| `context.py` | Workflow context and variable management |
| `action_loader.py` | Dynamic action module loading |
| `engine.py` | Main workflow execution engine |

## Architecture

```
┌──────────────────────────────────────────────────┐
│                   FlowEngine                      │
│  - load_workflow_from_dict()                      │
│  - load_workflow_from_file()                      │
│  - run()                                          │
│  - stop()                                         │
│  - pause() / resume()                             │
└──────────────────────┬───────────────────────────┘
                       │
        ┌──────────────┼──────────────┐
        ▼              ▼              ▼
┌─────────────┐ ┌─────────────┐ ┌─────────────┐
│  Context    │ │ ActionLoader│ │   Actions   │
│  Manager    │ │             │ │  (dynamic)  │
└─────────────┘ └─────────────┘ └─────────────┘
```

## FlowEngine

The main orchestrator for workflow execution:

```python
from core.engine import FlowEngine

engine = FlowEngine()

# Load workflow
workflow = {
    'variables': {'count': 0},
    'steps': [
        {'id': 1, 'type': 'delay', 'seconds': 1},
        {'id': 2, 'type': 'set_variable', 'name': 'count', 'value': '1'},
    ]
}
engine.load_workflow_from_dict(workflow)

# Execute
engine.run()

# Check context
print(engine.context.get_all())
```

## ContextManager

Manages workflow variables and execution state:

```python
from core.context import ContextManager

ctx = ContextManager()

# Set variables
ctx.set('name', 'Alice')
ctx.set('count', 42)

# Get variables
name = ctx.get('name')  # 'Alice'
count = ctx.get('count', 0)  # 42 (with default)

# Resolve expressions
ctx.set('a', 10)
ctx.set('b', 20)
result = ctx.resolve_value('{{a + b}}')  # 30

# Safe code execution
result = ctx.safe_exec("return_value = x * 2", output_var='x')
```

## ActionLoader

Dynamically loads action modules:

```python
from core.action_loader import ActionLoader

loader = ActionLoader('./actions')

# Load all actions
actions = loader.load_all()
print(list(actions.keys()))
# ['click', 'type_text', 'delay', 'condition', ...]

# Get specific action
click_action = loader.get_action('click')

# Get action info
info = loader.get_action_info()
```

## BaseAction

Base class for all actions:

```python
from core.base_action import BaseAction, ActionResult

class MyAction(BaseAction):
    action_type = "my_action"
    display_name = "My Action"
    description = "Does something"
    
    def execute(self, context, params):
        return ActionResult(success=True, message="Done")
    
    def get_required_params(self):
        return ['param1']
    
    def get_optional_params(self):
        return {'param2': 'default'}
```

## Execution Flow

1. **Load**: `engine.load_workflow_from_dict()` creates ContextManager and loads steps
2. **Validate**: Each step's action type is validated against loaded actions
3. **Execute**: Steps are executed in order (or based on flow control)
4. **Resolve**: Variable references `{{var}}` are resolved before each step
5. **Result**: ActionResult determines next step or workflow end

## Error Handling

```python
engine = FlowEngine()
engine.load_workflow_from_dict(workflow)

try:
    engine.run()
except Exception as e:
    print(f"Workflow failed: {e}")

# Check final state
print(f"Success: {engine.is_successful()}")
print(f"Failed step: {engine.get_failed_step()}")
```

## Extending the Engine

Add custom actions by creating files in `actions/` directory and they are auto-loaded by ActionLoader.

See `actions/README.md` for more details on creating custom actions.
