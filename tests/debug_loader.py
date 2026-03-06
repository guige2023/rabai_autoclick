import sys
import os
import traceback

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from pathlib import Path
import importlib.util
import inspect

actions_dir = os.path.join(project_root, "actions")
actions_path = Path(actions_dir)

print("Actions directory:", actions_dir)
print("Files found:")

for file_path in actions_path.glob("*.py"):
    if file_path.name.startswith("_"):
        continue
    
    print(f"\n--- Loading {file_path.name} ---")
    try:
        spec = importlib.util.spec_from_file_location(
            f"actions.{file_path.stem}", 
            str(file_path)
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if hasattr(obj, 'action_type'):
                print(f"  Found action: {obj.action_type}")
    except Exception as e:
        print(f"  ERROR: {e}")
        traceback.print_exc()
