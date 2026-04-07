# Quick Start Guide

Get up and running with RabAI AutoClick in 5 minutes.

## 🚀 Installation

### 1. Clone the Repository

```bash
git clone https://github.com/guige2023/rabai_autoclick.git
cd rabai_autoclick
```

### 2. Create Virtual Environment (Recommended)

```bash
python3 -m venv .venv
source .venv/bin/activate  # macOS/Linux
# .\.venv\\Scripts\\activate  # Windows
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Verify Installation

```bash
python main.py
```

You should see the GUI launch successfully.

## 📖 First Workflow

### Option A: Use the GUI

1. **Launch** the application: `python main.py`
2. **Click** "New Workflow"
3. **Add** steps from the action panel:
   - Click "Delay" → Set `seconds: 1`
   - Click "Click" → Click "Pick" to select screen location
   - Click "Type Text" → Enter "Hello World"
4. **Click** "Run" to execute

### Option B: Load an Example

```bash
# List available workflows
ls workflows/

# Run a workflow
python main.py --workflow workflows/example_workflow.json
```

## 🎯 Common Use Cases

### Automated Clicking

```json
{
  "name": "Auto Click",
  "steps": [
    {"id": 1, "type": "click", "x": 500, "y": 400, "pre_delay": 0.5},
    {"id": 2, "type": "delay", "seconds": 1},
    {"id": 3, "type": "click", "x": 600, "y": 500, "pre_delay": 0.5}
  ]
}
```

### Form Filling

```json
{
  "name": "Form Fill",
  "steps": [
    {"id": 1, "type": "click", "x": 200, "y": 150},
    {"id": 2, "type": "type_text", "text": "John Doe"},
    {"id": 3, "type": "key_press", "key": "tab"},
    {"id": 4, "type": "type_text", "text": "john@example.com"},
    {"id": 5, "type": "key_press", "key": "enter"}
  ]
}
```

### Loop Execution

```json
{
  "name": "Repeat Task",
  "variables": {"count": 0},
  "steps": [
    {"id": 1, "type": "set_variable", "name": "count", "value": "0"},
    {"id": 2, "type": "loop", "loop_id": "main", "count": 5},
    {"id": 3, "type": "click", "x": 100, "y": 100},
    {"id": 4, "type": "delay", "seconds": 1},
    {"id": 5, "type": "set_variable", "name": "count", "value": "{{count + 1}}"}
  ]
}
```

## ⌨️ Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| `Ctrl+F6` | Start/Run |
| `Ctrl+F7` | Stop |
| `Ctrl+F8` | Pause/Resume |
| `Ctrl+F9` | Start Recording |
| `Ctrl+F10` | Stop Recording |
| `Ctrl+F11` | Toggle Key Display |

## 🔧 Troubleshooting

### "keyboard module not found"

```bash
pip install keyboard
```

### macOS shortcuts not working

1. System Preferences → Security & Privacy → Accessibility
2. Add Terminal/Python to allowed apps
3. Restart the application

### OCR not recognizing text

- Reduce the recognition region
- Ensure text is clear and high contrast
- Try adjusting confidence threshold

## 📚 Next Steps

- Read the [Full Documentation](../README.md)
- Explore [Workflow Examples](../workflows/)
- Learn about [Actions](../actions/README.md)
- Check [API Reference](../README.md#api参考)

## 🆘 Getting Help

- Open an [Issue](https://github.com/guige2023/rabai_autoclick/issues)
- Check [Discussions](https://github.com/guige2023/rabai_autoclick/discussions)
- Read [Troubleshooting Guide](../docs/使用教程.md#⚠️-常见问题)
