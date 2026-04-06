# Actions Directory

This directory contains all automation action implementations for RabAI AutoClick.

## Overview

Actions are the building blocks of workflows. Each action performs a specific automation task like clicking, typing, waiting, etc.

## Action Files

| File | Action Type | Description |
|------|------------|-------------|
| `click.py` | `click` | Mouse click at coordinates |
| `keyboard.py` | `type_text`, `key_press` | Keyboard input |
| `mouse.py` | `scroll`, `move`, `drag` | Mouse movement and scrolling |
| `ocr.py` | `ocr` | OCR text recognition |
| `image_match.py` | `click_image`, `find_image` | Template image matching |
| `script.py` | `delay`, `condition`, `loop`, `set_variable`, `script` | Control flow and scripting |
| `system.py` | `screenshot`, `get_mouse_pos`, `alert` | System operations |

## Creating Custom Actions

1. Create a new file `actions/my_action.py`:

```python
from typing import Any, Dict, List
from core.base_action import BaseAction, ActionResult

class MyAction(BaseAction):
    """Custom action for my specific use case."""
    
    action_type = "my_action"
    display_name = "My Action"
    description = "Does something useful"
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute the action.
        
        Args:
            context: Workflow context manager
            params: Action parameters
            
        Returns:
            ActionResult with success status and data
        """
        # Your implementation here
        x = params.get('x', 0)
        y = params.get('y', 0)
        
        # Do something with x, y
        
        return ActionResult(
            success=True,
            message="Action completed",
            data={'result': 'data'}
        )
    
    def get_required_params(self) -> List[str]:
        """Return list of required parameter names."""
        return ['x', 'y']
    
    def get_optional_params(self) -> Dict[str, Any]:
        """Return dict of optional parameters with defaults."""
        return {
            'button': 'left',
            'clicks': 1
        }
```

2. Export in `actions/__init__.py`:

```python
from .my_action import MyAction

__all__ = [
    # ... existing exports
    'MyAction',
]
```

## BaseAction Methods

### Required Overrides

- `execute(context, params)` - Main action logic
- `get_required_params()` - List of required params
- `get_optional_params()` - Dict of optional params with defaults

### Optional Overrides

- `validate_params(params)` - Custom validation logic
- `get_display_name()` - Custom display name
- `get_description()` - Custom description

## Action Result

All actions return an `ActionResult`:

```python
ActionResult(
    success=True,        # Boolean success flag
    message="Done",      # Human-readable message
    data={},             # Optional result data
    next_step_id=None    # Optional: override next step
)
```

## Examples

### Simple Click Action

```python
class ClickAction(BaseAction):
    action_type = "click"
    
    def execute(self, context, params):
        x = params['x']
        y = params['y']
        pyautogui.click(x, y)
        return ActionResult(success=True, message=f"Clicked ({x}, {y})")
    
    def get_required_params(self):
        return ['x', 'y']
```

### Conditional Action

```python
class ConditionAction(BaseAction):
    action_type = "condition"
    
    def execute(self, context, params):
        condition = params['condition']
        true_next = params.get('true_next')
        false_next = params.get('false_next')
        
        result = context.safe_exec(f"return_value = {condition}")
        
        if result:
            next_step = true_next
        else:
            next_step = false_next
        
        return ActionResult(
            success=True,
            message=f"Condition {condition} = {result}",
            data={'result': result},
            next_step_id=next_step
        )
```

## Best Practices

1. **Always validate inputs** - Check required params exist
2. **Return meaningful messages** - Help users understand what happened
3. **Handle errors gracefully** - Catch exceptions and return failure result
4. **Document params** - Clear parameter descriptions in docstrings
5. **Type hints** - Use type annotations for better IDE support
