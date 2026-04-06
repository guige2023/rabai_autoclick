# CLI Directory

This directory contains the command-line interface for RabAI AutoClick v22.

## Files

| File | Description |
|------|-------------|
| `main.py` | Main CLI entry point with all commands |

## Usage

```bash
# Show help
python -m rabai_autoclick --help

# Run specific command
python -m rabai_autoclick predict next
python -m rabai_autoclick scene list
python -m rabai_autoclick diag summary
```

## Available Commands

### predict - Predictive Automation Engine
- `predict record <action_type> <target>` - Record action
- `predict next` - Predict next action
- `predict suggest` - Get workflow suggestions
- `predict analyze` - Analyze behavior patterns

### heal - Self-Healing System
- `heal fix <workflow> <step> --error <msg>` - Analyze and fix
- `heal stats` - Get error statistics

### scene - Scene-Based Workflow Packages
- `scene list` - List all scenes
- `scene activate <scene_id>` - Activate a scene
- `scene create <name>` - Create new scene
- `scene stats` - Get scene statistics

### diag - Enhanced Diagnostics
- `diag run [workflow_id]` - Run diagnostics
- `diag summary` - Get health summary
- `diag report <workflow_id>` - Generate detailed report

### share - No-Code Workflow Sharing
- `share register <workflow_file>` - Register workflow
- `share create-link <workflow_id>` - Create share link
- `share import <source>` - Import workflow
- `share export <workflow_id>` - Export workflow
- `share list` - List shared workflows
- `share stats` - Get sharing statistics

### pipe - CLI Pipeline Integration
- `pipe list` - List pipeline chains
- `pipe create <name>` - Create pipeline chain
- `pipe add <chain_id> <name> <command>` - Add step
- `pipe run <chain_id>` - Execute chain

### rec - Screen Recording to Workflow
- `rec start <name>` - Start recording
- `rec stop <recording_id>` - Stop recording
- `rec add-action <recording_id>` - Add action manually
- `rec list` - List recordings
- `rec convert <recording_id>` - Convert to workflow
- `rec analyze <recording_id>` - Analyze recording
