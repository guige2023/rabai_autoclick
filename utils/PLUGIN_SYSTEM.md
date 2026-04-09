# RabAI AutoClick Plugin System

A comprehensive plugin system for extending RabAI AutoClick functionality.

## Table of Contents

- [Overview](#overview)
- [Plugin System](#plugin-system)
  - [Plugin Manager](#plugin-manager)
  - [Base Plugin Interface](#base-plugin-interface)
  - [Sandboxed Execution](#sandboxed-execution)
  - [Hot Reload](#hot-reload)
- [Workflow Marketplace](#workflow-marketplace)
  - [Features](#features)
  - [Usage](#usage)
- [Workflow Bundle](#workflow-bundle)
  - [Bundle Format](#bundle-format)
  - [Operations](#operations)
- [Creating a Plugin](#creating-a-plugin)
- [Example Plugin](#example-plugin)

## Overview

RabAI AutoClick provides a flexible plugin ecosystem with three main components:

1. **Plugin System** - Discover, load, and manage plugins with hot reload support
2. **Workflow Marketplace** - Share, search, and distribute workflow bundles
3. **Workflow Bundle** - Package workflows into portable `.rabai` files

## Plugin System

### Plugin Manager

The `PluginManager` class handles plugin discovery, loading, and lifecycle management.

```python
from rabai_autoclick.utils.plugin_manager import PluginManager

# Initialize plugin manager
pm = PluginManager('/path/to/project')

# Discover available plugins
plugins = pm.discover_plugins()

# Load a specific plugin
pm.load_plugin('my_plugin')

# Load all discovered plugins
results = pm.load_all()

# Get a loaded plugin instance
plugin = pm.get_plugin('my_plugin')
```

### Base Plugin Interface

All plugins must inherit from `BasePlugin` and implement required methods:

```python
from rabai_autoclick.utils.plugin_manager import BasePlugin

class MyPlugin(BasePlugin):
    name = "my_plugin"
    version = "1.0.0"
    
    def on_load(self) -> bool:
        # Initialize plugin resources
        return True
    
    def on_unload(self) -> bool:
        # Clean up plugin resources
        return True
    
    def execute(self, *args, **kwargs):
        # Main plugin functionality
        return {"result": "success"}
```

### Sandboxed Execution

Plugins run in a sandboxed environment with resource limits:

```python
pm = PluginManager()
pm._sandbox_enabled = True
pm._sandbox_restrictions = {
    'max_memory_mb': 256,
    'max_cpu_seconds': 30,
}
```

### Hot Reload

Enable automatic plugin reloading when files change:

```python
pm.enable_hot_reload(interval=2.0)  # Check every 2 seconds

# Disable when done
pm.disable_hot_reload()
```

## Workflow Marketplace

### Features

- **Categories**: automation, productivity, testing, data_processing, etc.
- **Tags**: Flexible tagging system for discovery
- **Search**: Full-text search across workflows
- **Local Registry**: Persistent storage of workflow metadata
- **Export/Import**: Share workflows as `.rabai` bundle files

### Usage

```python
from rabai_autoclick.utils.workflow_market import WorkflowMarket

market = WorkflowMarket()

# Import a workflow bundle
bundle = market.import_bundle(Path('/path/to/workflow.rabai'))

# Search workflows
results = market.search('automation')

# List by category
workflows = market.list_by_category('productivity')

# List by tag
workflows = market.list_by_tag('beginner')

# Export a workflow
market.export_bundle(bundle, Path('/output/workflow.rabai'))
```

## Workflow Bundle

### Bundle Format

`.rabai` files are ZIP archives containing:

```
workflow.rabai
├── manifest.json      # Bundle metadata
├── workflow.json     # Workflow definition
├── README.md         # Documentation (optional)
├── dependencies.json # Required dependencies (optional)
└── resources/        # Additional resources (optional)
    ├── images/
    ├── data/
    └── configs/
```

### Operations

```python
from rabai_autoclick.utils.workflow_bundle import WorkflowBundleManager

manager = WorkflowBundleManager()

# Create a bundle
bundle = market.create_bundle(
    workflow_data={...},
    name="My Workflow",
    author="Author Name",
    description="Workflow description",
    category="automation",
    tags=["beginner", "example"],
)
manager.save_bundle(bundle, Path('output.rabai'))

# Load a bundle
bundle = manager.load_bundle(Path('workflow.rabai'))

# Validate a bundle
valid, error = manager.validate_bundle(Path('workflow.rabai'))

# Extract a bundle
manager.extract_bundle(Path('workflow.rabai'), Path('/extract/here'))
```

## Creating a Plugin

### Step 1: Create Plugin Directory

```
plugins/
└── my_plugin/
    ├── __init__.py
    ├── plugin.json
    └── my_plugin.py
```

### Step 2: Create plugin.json Manifest

```json
{
    "name": "my_plugin",
    "version": "1.0.0",
    "description": "Description of my plugin",
    "author": "Your Name",
    "license": "MIT",
    "tags": ["automation", "custom"],
    "entry_point": "my_plugin",
    "min_rabai_version": "22.0.0",
    "dependencies": []
}
```

### Step 3: Implement the Plugin

```python
# my_plugin.py
from rabai_autoclick.utils.plugin_manager import BasePlugin

class MyPlugin(BasePlugin):
    name = "my_plugin"
    version = "1.0.0"
    
    def on_load(self) -> bool:
        # Initialization
        return True
    
    def on_unload(self) -> bool:
        # Cleanup
        return True
    
    def execute(self, *args, **kwargs):
        # Your plugin logic here
        return {"status": "success"}
```

### Step 4: Export the Plugin

```python
# __init__.py
from .my_plugin import MyPlugin

def register():
    return MyPlugin()

__all__ = ['MyPlugin', 'register']
```

## Example Plugin

The `plugins/example/` directory contains a complete example plugin demonstrating:

- Basic plugin structure
- Multiple action types
- Context management
- Error handling

```python
from rabai_autoclick.plugins.example import ExamplePlugin

plugin = ExamplePlugin()
result = plugin.execute(action="greet", name="User")
# {'success': True, 'action': 'greet', 'message': 'Hello, User!', ...}
```

## API Reference

### PluginManager

| Method | Description |
|--------|-------------|
| `discover_plugins()` | Find all plugins in plugins directory |
| `load_plugin(name)` | Load a plugin by name |
| `unload_plugin(name)` | Unload a plugin |
| `reload_plugin(name)` | Hot reload a plugin |
| `load_all()` | Load all discovered plugins |
| `unload_all()` | Unload all plugins |
| `get_plugin(name)` | Get loaded plugin instance |
| `enable_hot_reload(interval)` | Start hot reload monitoring |
| `disable_hot_reload()` | Stop hot reload monitoring |

### WorkflowMarket

| Method | Description |
|--------|-------------|
| `create_bundle(...)` | Create a new workflow bundle |
| `export_bundle(bundle, path)` | Save bundle to file |
| `import_bundle(path)` | Load bundle from file |
| `search(query)` | Search workflows |
| `list_by_category(category)` | List workflows in category |
| `list_by_tag(tag)` | List workflows with tag |
| `validate_bundle(path)` | Validate a bundle file |

### WorkflowBundleManager

| Method | Description |
|--------|-------------|
| `save_bundle(bundle, path)` | Save bundle to .rabai file |
| `load_bundle(path)` | Load bundle from .rabai file |
| `validate_bundle(path)` | Validate bundle without loading |
| `extract_bundle(path, dir)` | Extract bundle to directory |
| `list_bundle_contents(path)` | List files in bundle |
| `get_bundle_info(path)` | Get bundle metadata |

## License

MIT License
