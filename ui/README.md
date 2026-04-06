# UI Directory

This directory contains PyQt5-based graphical user interface components for RabAI AutoClick.

## Files

| File | Description |
|------|-------------|
| `main_window.py` | Main application window |
| `main_window_v22.py` | v22 enhanced main window |
| `action_config.py` | Action configuration panel |
| `hotkey_dialog.py` | Hotkey settings dialog |
| `region_selector.py` | Screen region selection tool |
| `stats_dialog.py` | Statistics display dialog |
| `message.py` | Message notification system |
| `mini_toolbar.py` | Floating mini toolbar |
| `script_editor.py` | Script code editor |

## Main Window

The main application window provides:

- Step list view with drag-and-drop reordering
- Action configuration panel
- Toolbar with common actions
- Status bar showing execution state
- Menu bar with File, Edit, View, Help

```python
from ui.main_window import MainWindow

window = MainWindow()
window.show()
```

## UI Components

### ActionConfigWidget

Configures action parameters:

```python
from ui.main_window import ActionConfigWidget

widget = ActionConfigWidget(action_info)
config = widget.get_config()
```

### RegionSelector

Allows user to select a screen region:

```python
from ui.region_selector import select_region

region = select_region()  # Returns (x, y, width, height)
```

### HotkeySettingsDialog

Configure global hotkeys:

```python
from ui.hotkey_dialog import HotkeySettingsDialog

dialog = HotkeySettingsDialog()
if dialog.exec_():
    hotkeys = dialog.get_hotkeys()
```

### MessageManager

Display notifications:

```python
from ui.message import show_info, show_success, show_error

show_info("Operation completed")
show_success("Workflow saved")
show_error("Failed to connect", details)
```

## Signals and Slots

PyQt signals used for inter-component communication:

```python
# Example: Workflow execution signals
class FlowEngine(QObject):
    step_started = pyqtSignal(int, str)  # step_id, action_type
    step_completed = pyqtSignal(int, bool)  # step_id, success
    execution_finished = pyqtSignal(bool)  # success
```

## Styling

UI uses Fusion style with custom stylesheet:

```python
app.setStyle("Fusion")
app.setStyleSheet("""
    QPushButton {
        background-color: #2196F3;
        color: white;
        padding: 6px 12px;
        border-radius: 4px;
    }
""")
```

## Layout Structure

```
┌─────────────────────────────────────────────────────┐
│ Menu Bar                                            │
├─────────────┬───────────────────────────────────────┤
│             │                                       │
│  Step List  │     Action Config Panel              │
│  (Tree)     │                                       │
│             │                                       │
│             ├───────────────────────────────────────┤
│             │     Preview / Log Panel               │
│             │                                       │
├─────────────┴───────────────────────────────────────┤
│ Status Bar                                          │
└─────────────────────────────────────────────────────┘
```

## Custom Widgets

### MiniToolbar

Floating toolbar for quick access:

```python
from ui.mini_toolbar import MiniToolbar

toolbar = MiniToolbar()
toolbar.show()
```

### ScriptEditor

Code editor for ScriptAction:

```python
from ui.script_editor import ScriptEditor

editor = ScriptEditor()
code = editor.get_code()
editor.set_code("print('hello')")
```

## Internationalization

UI text can be translated using Qt's translation system:

```python
# translations stored in i18n/ directory
translator = QTranslator()
translator.load("rabai_zh_CN.qm")
app.installTranslator(translator)
```

## Threading

GUI runs on main thread, heavy operations on worker threads:

```python
# Worker thread for execution
class ExecutionWorker(QThread):
    finished = pyqtSignal(bool)
    
    def run(self):
        result = engine.run()
        self.finished.emit(result)
```

## Extending the UI

### Adding New Dialogs

1. Create new dialog class inheriting from QDialog
2. Add to appropriate menu or toolbar
3. Use signals to communicate with main window

### Custom Action Config

Implement custom config widget for action-specific parameters:

```python
class MyActionConfig(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        # Add action-specific controls
    
    def get_config(self):
        return {'param': self.control.value()}
```
