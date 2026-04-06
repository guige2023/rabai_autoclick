# v22 Advanced Features Source

This directory contains the advanced features introduced in RabAI AutoClick v22.

## Files

| File | Description |
|------|-------------|
| `predictive_engine.py` | Predictive automation based on user behavior |
| `self_healing_system.py` | Automatic failure recovery |
| `workflow_package.py` | Scene-based workflow organization |
| `workflow_diagnostics.py` | Health monitoring and analysis |
| `workflow_share.py` | No-code workflow sharing |
| `pipeline_mode.py` | CLI pipeline integration |
| `screen_recorder.py` | Screen recording to workflow conversion |

## Quick Start

```python
from src.predictive_engine import create_predictive_engine
from src.workflow_diagnostics import create_diagnostics

# Create predictive engine
engine = create_predictive_engine("./data")

# Record an action
engine.record_action("click", "button_submit", {"app": "Chrome"})

# Get next action prediction
prediction = engine.predict_next_action({"app": "Chrome"})

# Run diagnostics
diag = create_diagnostics("./data")
report = diag.diagnose("workflow_id")
```

## Features Overview

### Predictive Engine

Learns from user actions to predict next actions:

```python
engine.record_action(action_type, target, context)
prediction = engine.predict_next_action(context)
suggestion = engine.suggest_workflow_creation()
analysis = engine.analyze_user_behavior()
```

### Self-Healing System

Automatically recovers from failures:

```python
system = create_self_healing_system("./data")
record = system.analyze_error(exception, workflow_name, step, index, context)
suggestions = system.get_fix_suggestions(record)
```

### Workflow Diagnostics

Monitors workflow health:

```python
diag = create_diagnostics("./data")
report = diag.diagnose(workflow_id)
summary = diag.get_health_summary()
```

### Workflow Sharing

Share workflows without code:

```python
share = create_share_system("./data")
link = share.create_share_link(workflow_id, ShareType.PUBLIC, expires=7)
report = share.import_workflow(data, format="base64")
```

### Pipeline Mode

CLI pipeline integration:

```python
runner = create_pipeline_runner("./data")
chain = runner.create_chain("My Chain", PipeMode.LINEAR)
runner.add_step(chain.chain_id, "Step 1", "echo hello")
result = runner.execute_chain(chain.chain_id, input_data)
```

### Screen Recorder

Convert recordings to workflows:

```python
recorder = create_screen_recorder("./data")
rec = recorder.start_recording("My Recording")
recorder.add_action(rec.recording_id, action_data)
rec = recorder.stop_recording(rec.recording_id)
result = recorder.convert_to_workflow(rec.recording_id)
```
