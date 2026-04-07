# CLI Reference

Command-line interface for RabAI AutoClick.

## Usage

```bash
rabai [command] [options]
rabai-pipe [command] [options]
python -m cli.main [command] [options]
```

## Commands

### run

Run a workflow from file.

```bash
rabai run [workflow_file] [options]
```

**Options:**
| Option | Description | Default |
|--------|-------------|---------|
| `--loop N` | Number of times to repeat | 1 |
| `--verbose` | Enable verbose output | false |
| `--dry-run` | Validate workflow without executing | false |

**Example:**
```bash
rabai run workflows/example.json
rabai run workflows/example.json --loop 5
rabai run workflows/example.json --verbose --loop 3
```

### pipe

Run in pipeline mode for shell integration.

```bash
rabai-pipe [command] [options]
```

**Example:**
```bash
echo '{"steps": [{"type": "delay", "seconds": 1}]}' | rabai-pipe run
```

### validate

Validate a workflow file.

```bash
rabai validate [workflow_file]
```

**Example:**
```bash
rabai validate workflows/example.json
```

### info

Display system and version information.

```bash
rabai info
```

**Output:**
```
RabAI AutoClick v22.0.0
Python 3.12.0
Platform: macOS 14.0
```

### list-actions

List all available actions.

```bash
rabai list-actions
```

**Output:**
```
Available Actions:
- click          Mouse click
- double_click   Mouse double click
- type_text      Keyboard text input
- key_press      Single key press
- delay          Wait/delay
- scroll         Mouse scroll
- move           Move mouse
- ocr            OCR text recognition
- click_image    Image template click
- find_image     Find image location
- screenshot     Take screenshot
- condition      Conditional branch
- loop           Loop control
- set_variable   Set variable
- script         Execute script
```

### list-workflows

List all workflow files.

```bash
rabai list-workflows [directory]
```

**Example:**
```bash
rabai list-workflows
rabai list-workflows workflows/
```

## Global Options

| Option | Description |
|--------|-------------|
| `--help` | Show help message |
| `--version` | Show version |
| `--quiet` | Suppress output |
| `--debug` | Enable debug mode |

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `RABAI_LOG_LEVEL` | Log level (DEBUG/INFO/WARNING/ERROR) | INFO |
| `RABAI_LOG_DIR` | Log directory | ./logs |
| `RABAI_DATA_DIR` | Data directory | ./data |
| `RABAI_CONFIG` | Config file path | ~/.rabai/config.json |

## Exit Codes

| Code | Description |
|------|-------------|
| 0 | Success |
| 1 | General error |
| 2 | Workflow file not found |
| 3 | Invalid workflow format |
| 4 | Action execution failed |
| 5 | Keyboard interrupt |
| 6 | Timeout |

## Examples

### Run workflow with 10 loops
```bash
rabai run workflow.json --loop 10
```

### Validate before running
```bash
rabai validate workflow.json && rabai run workflow.json
```

### Silent run (no output)
```bash
rabai run workflow.json --quiet
```

### Debug run
```bash
rabai run workflow.json --debug
```

### Using with cron
```bash
# Run every hour
0 * * * * /path/to/rabai run workflow.json --quiet
```

### Using with systemd
```ini
[Unit]
Description=RabAI AutoClick Workflow

[Service]
Type=oneshot
ExecStart=/path/to/rabai run workflow.json
WorkingDirectory=/path/to/rabai
```

## Keyboard Shortcuts (GUI Mode)

| Shortcut | Action |
|----------|--------|
| `Ctrl+F6` | Start/Run |
| `Ctrl+F7` | Stop |
| `Ctrl+F8` | Pause/Resume |
| `Ctrl+F9` | Start Recording |
| `Ctrl+F10` | Stop Recording |
| `Ctrl+F11` | Toggle Key Display |
| `Ctrl+N` | New Workflow |
| `Ctrl+O` | Open Workflow |
| `Ctrl+S` | Save Workflow |
| `Ctrl+Q` | Quit |
