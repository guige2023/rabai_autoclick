# Workflows Directory

This directory contains example workflow files for RabAI AutoClick.

## Example Workflows

| File | Description |
|------|-------------|
| `example_basic.json` | Basic click and type workflow |
| `example_loop.json` | Workflow demonstrating loop functionality |
| `example_ocr.json` | OCR-based text recognition workflow |
| `example_script.json` | Script execution workflow |
| `example_workflow.json` | General workflow example |

## Workflow JSON Structure

```json
{
  "variables": {
    "variable_name": "value"
  },
  "steps": [
    {
      "id": 1,
      "type": "click",
      "x": 100,
      "y": 200,
      "button": "left",
      "clicks": 1
    }
  ]
}
```

## Loading Workflows

### Via GUI

1. Click the "Load" button
2. Select the workflow JSON file
3. The workflow will appear in the step list

### Via Code

```python
from core.engine import FlowEngine

engine = FlowEngine()
engine.load_workflow_from_file("workflows/example_basic.json")
engine.run()
```

## Creating Your Own

1. Create a new JSON file in this directory
2. Follow the structure shown above
3. Use action types from the `actions/` module

### Available Action Types

- `click` - Mouse click
- `type_text` - Keyboard input
- `key_press` - Special keys
- `delay` - Wait
- `condition` - Conditional
- `loop` - Loop
- `set_variable` - Set variable
- `click_image` - Image match click
- `ocr` - OCR click
- `screenshot` - Capture screen
- `get_mouse_pos` - Get mouse position
