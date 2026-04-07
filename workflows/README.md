# Workflows Directory

This directory contains example and user workflow definitions for RabAI AutoClick.

## 📁 Directory Structure

```
workflows/
├── README.md                        # This file
├── example_workflow.json           # Basic workflow example
├── example_basic.json              # Basic click and type actions
├── example_loop.json               # Loop and variable control
├── example_ocr.json                # OCR text recognition
├── example_conditional.json        # Conditional branching
├── example_image_match.json        # Image template matching
├── example_script.json             # Python script execution
├── example_social_media_post.json  # Social media automation
├── example_data_entry.json         # Data entry automation
├── example_gaming_macro.json       # Gaming combo macros
├── example_email_automation.json   # Email automation
└── example_web_testing.json        # Web UI testing
```

## 🎯 Quick Start

### Create Your First Workflow

1. Open RabAI AutoClick GUI
2. Click "New Workflow"
3. Add steps using the action panel
4. Save workflow to this directory

### Load and Run via Python

```python
from core.engine import FlowEngine

# Create engine instance
engine = FlowEngine()

# Load workflow from file
engine.load_workflow_from_file("./workflows/example_workflow.json")

# Run the workflow
engine.run()
```

## 📋 Workflow Format

### Basic Structure

```json
{
  "name": "Workflow Name",
  "description": "What this workflow does",
  "version": "1.0",
  "author": "Your Name",
  "variables": {
    "var_name": "value"
  },
  "steps": [
    {
      "id": 1,
      "type": "click",
      "x": 100,
      "y": 200,
      "button": "left",
      "clicks": 1,
      "pre_delay": 0.5,
      "post_delay": 0.2,
      "comment": "Optional comment"
    }
  ],
  "settings": {
    "loop_count": 1,
    "retry_on_failure": false,
    "max_retries": 3
  }
}
```

### Metadata Fields

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Workflow display name |
| `description` | string | Detailed description |
| `version` | string | Version number (semver) |
| `author` | string | Author name |

## 🔧 Step Types Reference

### Mouse Actions

| Type | Description | Required Params | Optional Params |
|------|-------------|-----------------|------------------|
| `click` | Mouse click | `x`, `y` | `button`, `clicks`, `pre_delay`, `post_delay` |
| `double_click` | Double click | `x`, `y` | `button`, `pre_delay`, `post_delay` |
| `move` | Move mouse | `x`, `y` | `duration` |
| `drag` | Drag mouse | `start_x`, `start_y`, `end_x`, `end_y` | `duration` |
| `scroll` | Scroll wheel | `clicks`, `direction` | `pre_delay` |

### Keyboard Actions

| Type | Description | Required Params | Optional Params |
|------|-------------|-----------------|------------------|
| `type_text` | Type text | `text` | `enter_after`, `interval`, `pre_delay` |
| `key_press` | Press key(s) | `key` or `keys[]` | `pre_delay` |

### Vision Actions

| Type | Description | Required Params | Optional Params |
|------|-------------|-----------------|------------------|
| `click_image` | Image match click | `template` | `confidence`, `region`, `double_click` |
| `find_image` | Find image location | `template` | `confidence`, `find_all` |
| `ocr` | OCR text recognition | - | `region`, `output_var`, `image_path` |
| `screenshot` | Take screenshot | `save_path` | `region`, `pre_delay` |

### Control Flow

| Type | Description | Required Params | Optional Params |
|------|-------------|-----------------|------------------|
| `delay` | Wait | `seconds` | `random_deviation` |
| `condition` | Branch condition | `condition` | `true_next`, `false_next` |
| `loop` | Loop block | `loop_id`, `count` | `loop_start`, `loop_end` |
| `goto` | Jump to step | `step_id` | - |
| `set_variable` | Set variable | `name`, `value` | `value_type` |
| `script` | Execute script | `code` | `output_var` |

### Window Actions

| Type | Description | Required Params | Optional Params |
|------|-------------|-----------------|------------------|
| `select_window` | Select window | `title` | - |
| `select_region` | Select region | `x`, `y`, `width`, `height` | - |

## 🔢 Parameter Types

| Type | Description | Example |
|------|-------------|---------|
| `int` | Integer | `5`, `-10`, `100` |
| `float` | Decimal | `0.5`, `3.14`, `-2.5` |
| `string` | Text | `"hello"` |
| `bool` | Boolean | `true`, `false` |
| `expression` | Math expression | `"{{counter + 1}}"` |
| `list` | Array | `["ctrl", "c"]` |

## 📖 Variable Syntax

### Referencing Variables

```
{{variable_name}}
```

### Expressions

```
{{counter + 1}}
{{x - 100}}
{{text.includes('success')}}
{{length > 0}}
```

### Built-in Functions

| Function | Description | Example |
|----------|-------------|---------|
| `length` | String/array length | `{{text.length}}` |
| `includes()` | String contains | `{{text.includes('hello')}}` |
| `split()` | Split string | `{{text.split(',')[0]}}` |
| `trim()` | Trim whitespace | `{{text.trim()}}` |
| `toUpperCase()` | Uppercase | `{{text.toUpperCase()}}` |
| `toLowerCase()` | Lowercase | `{{text.toLowerCase()}}` |

## 🔒 Security Notes

- Workflow files are plain JSON - do not store sensitive data
- Use environment variables or external config for passwords
- Review scripts before running from untrusted sources

## 📂 Loading Workflows

### Python API

```python
from core.engine import FlowEngine

engine = FlowEngine()

# Load from file
engine.load_workflow_from_file("./workflows/my_workflow.json")

# Load from dict
workflow = {
    "variables": {"count": 0},
    "steps": [{"id": 1, "type": "click", "x": 100, "y": 200}]
}
engine.load_workflow(workflow)

# Execute
engine.run()
```

### CLI

```bash
python main.py --workflow ./workflows/example.json
python main.py --workflow ./workflows/example.json --loop 5
```

## 📝 Best Practices

1. **Use Comments**: Add `comment` fields to document step purpose
2. **Set Delays**: Add `pre_delay` between steps for reliability
3. **Handle Errors**: Set `retry_on_failure: true` in settings
4. **Test Incrementally**: Test each step before running full workflow
5. **Use Variables**: Store reusable values in `variables` section
