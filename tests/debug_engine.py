#!/usr/bin/env python3
"""Debug script for testing the FlowEngine.

This script loads a simple workflow and runs it to verify
the engine is working correctly.
"""

import sys
from pathlib import Path
from typing import Any, Dict

from core.engine import FlowEngine


def main() -> None:
    """Run a test workflow through the FlowEngine."""
    project_root = Path(__file__).parent.parent
    sys.path.insert(0, str(project_root))
    
    engine = FlowEngine()
    
    workflow: Dict[str, Any] = {
        'variables': {},
        'steps': [
            {
                'id': 1,
                'type': 'delay',
                'seconds': 0.1,
                'output_var': 'delay_result'
            },
            {
                'id': 2,
                'type': 'set_variable',
                'name': 'test_var',
                'value': 'hello'
            }
        ]
    }
    
    engine.load_workflow_from_dict(workflow)
    
    print(
        "Actions loaded:",
        list(engine.action_loader.get_all_actions().keys())
    )
    print("Before run:", engine.context.get_all())
    
    result = engine.run()
    print("Run result:", result)
    print("After run:", engine.context.get_all())
    print("test_var:", engine.context.get('test_var'))


if __name__ == '__main__':
    main()
