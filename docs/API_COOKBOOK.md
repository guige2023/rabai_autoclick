# API Cookbook

Code snippets and recipes for using RabAI AutoClick API.

## Basic Usage

### Load and Run Workflow

```python
from core.engine import FlowEngine

engine = FlowEngine()
engine.load_workflow_from_file("workflows/example.json")
engine.run()
```

### Create Workflow Programmatically

```python
from core.engine import FlowEngine

workflow = {
    "name": "My Workflow",
    "variables": {"count": 0},
    "steps": [
        {"id": 1, "type": "delay", "seconds": 1},
        {"id": 2, "type": "click", "x": 100, "y": 200}
    ]
}

engine = FlowEngine()
engine.load_workflow(workflow)
engine.run()
```

### Monitor Execution

```python
from core.engine import FlowEngine

engine = FlowEngine()
engine.load_workflow_from_file("workflows/example.json")

# Run and monitor
engine.run()
while engine.is_running():
    status = engine.get_status()
    print(f"Step {status['current_step']} running...")
    time.sleep(0.1)
```

## Variable Management

### Set and Get Variables

```python
from core.context import ContextManager

ctx = ContextManager()

# Set variables
ctx.set("name", "Alice")
ctx.set("age", 30)
ctx.set("active", True)

# Get variables
name = ctx.get("name")  # "Alice"
age = ctx.get("age", 0)  # 30 (with default)
```

### Use Variables in Expressions

```python
from core.context import ContextManager

ctx = ContextManager()
ctx.set("a", 10)
ctx.set("b", 20)

# Resolve expressions
result = ctx.resolve_value("{{a + b}}")  # 30
result = ctx.resolve_value("{{a * b}}")  # 200
```

### String Operations

```python
ctx = ContextManager()
ctx.set("text", "Hello World")

# String methods
length = ctx.resolve_value("{{text.length}}")  # 11
upper = ctx.resolve_value("{{text.toUpperCase()}}")  # "HELLO WORLD"
contains = ctx.resolve_value("{{text.includes('World')}}")  # True
```

## Custom Actions

### Create Custom Action

```python
from core.base_action import BaseAction, ActionResult
from typing import Any, Dict, List

class RandomClickAction(BaseAction):
    action_type = "random_click"
    display_name = "Random Click"
    description = "Click at random position within bounds"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        import random

        x_min = params.get('x_min', 0)
        x_max = params.get('x_max', 1920)
        y_min = params.get('y_min', 0)
        y_max = params.get('y_max', 1080)

        x = random.randint(x_min, x_max)
        y = random.randint(y_min, y_max)

        pyautogui.click(x, y)
        return ActionResult(
            success=True,
            message=f"Random clicked at ({x}, {y})",
            data={'x': x, 'y': y}
        )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'x_min': 0,
            'x_max': 1920,
            'y_min': 0,
            'y_max': 1080
        }
```

### Use Custom Action

```python
from core.engine import FlowEngine
from my_actions import RandomClickAction

engine = FlowEngine()

# Register custom action
engine.action_loader.register(RandomClickAction())

# Use in workflow
workflow = {
    "steps": [
        {"id": 1, "type": "random_click", "x_min": 100, "x_max": 500}
    ]
}
engine.load_workflow(workflow)
engine.run()
```

## Error Handling

### Try-Except Pattern

```python
from core.engine import FlowEngine

engine = FlowEngine()
engine.load_workflow_from_file("workflows/example.json")

try:
    engine.run()
except Exception as e:
    print(f"Error: {e}")
    # Handle error
```

### Check Execution Status

```python
engine = FlowEngine()
engine.load_workflow_from_file("workflows/example.json")
engine.run()

# Check result
if engine.is_successful():
    print("Workflow completed successfully")
else:
    print(f"Failed at step {engine.get_failed_step()}")
```

## Advanced Recipes

### Conditional Loop

```python
from core.engine import FlowEngine

workflow = {
    "variables": {"count": 0, "max": 5},
    "steps": [
        {"id": 1, "type": "set_variable", "name": "count", "value": "0"},
        {"id": 2, "type": "loop", "loop_id": "main", "count": 5},
        {"id": 3, "type": "click", "x": 100, "y": 100},
        {"id": 4, "type": "delay", "seconds": 1},
        {"id": 5, "type": "set_variable", "name": "count", "value": "{{count + 1}}"},
        {"id": 6, "type": "condition", "condition": "{{count < max}}", "true_next": 2}
    ]
}

engine = FlowEngine()
engine.load_workflow(workflow)
engine.run()
```

### OCR-Based Click

```python
workflow = {
    "steps": [
        {"id": 1, "type": "screenshot", "save_path": "screen.png"},
        {"id": 2, "type": "ocr", "image_path": "screen.png", "output_var": "text"},
        {"id": 3, "type": "condition", "condition": "{{text.includes('Login')}}", "true_next": 4},
        {"id": 4, "type": "click", "x": 400, "y": 300}
    ]
}
```

### Image-Based Navigation

```python
workflow = {
    "steps": [
        {"id": 1, "type": "click_image", "template": "menu.png", "confidence": 0.85},
        {"id": 2, "type": "delay", "seconds": 1},
        {"id": 3, "type": "click_image", "template": "submenu.png", "confidence": 0.85},
        {"id": 4, "type": "delay", "seconds": 0.5},
        {"id": 5, "type": "click", "x": 200, "y": 400}
    ]
}
```

### Script-Based Processing

```python
workflow = {
    "steps": [
        {"id": 1, "type": "set_variable", "name": "items", "value": "[1, 2, 3, 4, 5]"},
        {"id": 2, "type": "script", "code": "total = sum(items); return_value = total"},
        {"id": 3, "type": "condition", "condition": "{{return_value > 10}}", "true_next": 4},
        {"id": 4, "type": "delay", "seconds": 1}
    ]
}
```

### Retry Pattern

```python
workflow = {
    "steps": [
        {"id": 1, "type": "set_variable", "name": "attempts", "value": "0"},
        {"id": 2, "type": "click_image", "template": "button.png", "confidence": 0.8},
        {"id": 3, "type": "condition", "condition": "{{last_result.success}}", "true_next": 6},
        {"id": 4, "type": "set_variable", "name": "attempts", "value": "{{attempts + 1}}"},
        {"id": 5, "type": "condition", "condition": "{{attempts < 3}}", "true_next": 2},
        {"id": 6, "type": "delay", "seconds": 1}
    ]
}
```

## Integration Examples

### Run from Shell Script

```bash
#!/bin/bash
# run_workflow.sh

WORKFLOW=$1
LOOP=${2:-1}

rabai run "$WORKFLOW" --loop "$LOOP" --verbose
if [ $? -eq 0 ]; then
    echo "Workflow completed successfully"
else
    echo "Workflow failed"
    exit 1
fi
```

### Use with Cron

```bash
# Run every day at 9am
0 9 * * * /path/to/rabai run daily_task.json --quiet

# Run every hour
0 * * * * /path/to/rabai run hourly_check.json --quiet
```

### Use as Python Module

```python
import rabai_autoclick as rabai

# Create and run workflow
rabai.run_workflow("workflows/example.json")

# Or use engine directly
from rabai_autoclick.core import FlowEngine
engine = FlowEngine()
engine.load_workflow_from_file("workflows/example.json")
engine.run()
```
