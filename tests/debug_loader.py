#!/usr/bin/env python3
"""Debug script for loading and inspecting action modules.

This script loads all action modules and displays information
about the actions they contain.
"""

import importlib.util
import inspect
import os
import sys
import traceback
from pathlib import Path
from typing import Any, Dict, List


def main() -> None:
    """Load and inspect all action modules."""
    project_root = Path(__file__).parent.parent
    sys.path.insert(0, str(project_root))
    
    actions_dir = project_root / "actions"
    
    print(f"Actions directory: {actions_dir}")
    print("Files found:")
    
    if not actions_dir.exists():
        print(f"ERROR: Actions directory not found: {actions_dir}")
        return
    
    for file_path in sorted(actions_dir.glob("*.py")):
        if file_path.name.startswith("_"):
            continue
        
        print(f"\n--- Loading {file_path.name} ---")
        _load_action_module(file_path)


def _load_action_module(file_path: Path) -> None:
    """Load a single action module and display its actions.
    
    Args:
        file_path: Path to the action module file.
    """
    module_name = f"actions.{file_path.stem}"
    
    try:
        spec = importlib.util.spec_from_file_location(
            module_name,
            str(file_path)
        )
        if spec is None or spec.loader is None:
            print(f"  ERROR: Could not create module spec")
            return
        
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        
        _inspect_module_actions(module)
        
    except Exception as e:
        print(f"  ERROR: {e}")
        traceback.print_exc()


def _inspect_module_actions(module: Any) -> None:
    """Inspect a module for action classes.
    
    Args:
        module: The loaded module to inspect.
    """
    actions_found: List[str] = []
    
    for name, obj in inspect.getmembers(module, inspect.isclass):
        if hasattr(obj, 'action_type'):
            action_type = getattr(obj, 'action_type', None)
            actions_found.append(f"{name} (type: {action_type})")
            
            if hasattr(obj, 'params_schema'):
                schema = getattr(obj, 'params_schema', {})
                print(f"    Params schema: {list(schema.keys())}")
    
    for action_info in actions_found:
        print(f"  Found action: {action_info}")
    
    if not actions_found:
        print(f"  No actions found in module")


if __name__ == '__main__':
    main()
