# Workflows Directory

This directory contains example and user workflow definitions.

## Files

| File | Description |
|------|-------------|
| `README.md` | This file |
| `example_basic.json` | Basic workflow example |
| `example_loop.json` | Loop workflow example |
| `example_ocr.json` | OCR workflow example |

## Workflow Format

```json
{
  "variables": {
    "key": "value"
  },
  "steps": [
    {
      "id": 1,
      "type": "click",
      "x": 100,
      "y": 200,
      "button": "left",
      "clicks": 1,
      "pre_delay": 0.5
    }
  ]
}
```

## Step Types

| Type | Description | Required Params |
|------|-------------|-----------------|
| `click` | Mouse click | `x`, `y` |
| `type_text` | Keyboard input | `text` |
| `key_press` | Single key | `key` |
| `delay` | Wait | `seconds` |
| `scroll` | Scroll mouse | `x`, `y` |
| `move` | Move mouse | `x`, `y` |
| `ocr` | Text recognition | `image_path` |
| `click_image` | Image match click | `image_path` |
| `screenshot` | Take screenshot | `save_path` |
| `condition` | Conditional branch | `condition` |
| `loop` | Loop block | `count`, `loop_start` |
| `set_variable` | Set variable | `name`, `value` |
| `script` | Run script | `code` |

## Creating Workflows

### Using GUI

1. Open RabAI AutoClick
2. Click "New Workflow"
3. Add steps using the action panel
4. Save workflow

### Using JSON

Create a JSON file in this directory:

```json
{
  "variables": {},
  "steps": [
    {"id": 1, "type": "delay", "seconds": 1},
    {"id": 2, "type": "click", "x": 100, "y": 200}
  ]
}
```

## Loading Workflows

```python
from core.engine import FlowEngine

engine = FlowEngine()
engine.load_workflow_from_file("./workflows/my_workflow.json")
engine.run()
```
