import sys
import os

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from core.engine import FlowEngine

engine = FlowEngine()

workflow = {
    'variables': {},
    'steps': [
        {'id': 1, 'type': 'delay', 'seconds': 0.1, 'output_var': 'delay_result'},
        {'id': 2, 'type': 'set_variable', 'name': 'test_var', 'value': 'hello'}
    ]
}

engine.load_workflow_from_dict(workflow)
print("Actions loaded:", list(engine.action_loader.get_all_actions().keys()))
print("Before run:", engine.context.get_all())

result = engine.run()
print("Run result:", result)
print("After run:", engine.context.get_all())
print("test_var:", engine.context.get('test_var'))
