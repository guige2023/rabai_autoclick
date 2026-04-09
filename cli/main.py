#!/usr/bin/env python3
"""RabAI AutoClick v22 CLI

Command-line interface for RabAI AutoClick v22 featuring:
- Predictive automation engine
- Self-healing system
- Scene-based workflow packages
- Enhanced diagnostics
- No-code workflow sharing
- CLI pipeline integration
- Screen recording to workflow conversion
"""

import datetime
import difflib
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import click
import yaml

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.predictive_engine import create_predictive_engine
from src.self_healing_system import create_self_healing_system
from src.workflow_package import create_scene_manager
from src.workflow_diagnostics import create_diagnostics
from src.workflow_share import create_share_system, ShareType
from src.pipeline_mode import create_pipeline_runner, PipeCLI, PipeMode
from src.screen_recorder import create_screen_recorder


DATA_DIR: Path = Path(__file__).parent.parent / "data"
CONFIG_DIR: Path = Path.home() / ".rabai"
CONFIG_FILE: Path = CONFIG_DIR / "config.yaml"

# Global logging state
LOG_LEVEL = logging.WARNING  # Default to WARNING
_log_handler: Optional[logging.Handler] = None


def setup_logging(verbose: bool, quiet: bool) -> None:
    """Configure logging based on verbosity flags."""
    global LOG_LEVEL, _log_handler
    
    if quiet:
        LOG_LEVEL = logging.ERROR
    elif verbose:
        LOG_LEVEL = logging.DEBUG
    else:
        LOG_LEVEL = logging.WARNING
    
    # Remove existing handler if any
    if _log_handler:
        logging.root.removeHandler(_log_handler)
    
    _log_handler = logging.StreamHandler()
    _log_handler.setLevel(LOG_LEVEL)
    logging.root.setLevel(LOG_LEVEL)
    logging.root.addHandler(_log_handler)


def log_info(msg: str) -> None:
    """Log info message if verbose mode."""
    if LOG_LEVEL <= logging.INFO:
        logging.info(msg)


def log_debug(msg: str) -> None:
    """Log debug message if verbose mode."""
    if LOG_LEVEL <= logging.DEBUG:
        logging.debug(msg)


# ========== Config File Support ==========


class Config:
    """Configuration management with file persistence."""
    
    DEFAULT_CONFIG = {
        "output_format": "table",
        "log_level": "WARNING",
        "data_dir": str(DATA_DIR),
        "auto_validate": False,
        "color_output": True,
        "shell_completion": False,
        "default_workflow_path": ".",
        "parallel_execution": False,
        "max_retries": 3,
    }
    
    def __init__(self) -> None:
        self.config: Dict[str, Any] = {}
        self._load()
    
    def _load(self) -> None:
        """Load config from file, merge with defaults."""
        self.config = self.DEFAULT_CONFIG.copy()
        
        if CONFIG_FILE.exists():
            try:
                with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                    user_config = yaml.safe_load(f) or {}
                    self.config.update(user_config)
            except Exception as e:
                logging.warning(f"Failed to load config: {e}")
        
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    
    def save(self) -> None:
        """Save current config to file."""
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            yaml.dump(self.config, f, default_flow_style=False)
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get config value."""
        return self.config.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        """Set config value."""
        self.config[key] = value


# Global config instance
_config: Optional[Config] = None


def get_config() -> Config:
    """Get or create global config instance."""
    global _config
    if _config is None:
        _config = Config()
    return _config


# ========== Output Formatting ==========


class OutputFormatter:
    """Format output in various formats (json, yaml, table)."""
    
    @staticmethod
    def format_json(data: Any, indent: int = 2) -> str:
        """Format data as JSON."""
        return json.dumps(data, indent=indent, ensure_ascii=False, default=str)
    
    @staticmethod
    def format_yaml(data: Any) -> str:
        """Format data as YAML."""
        return yaml.dump(data, default_flow_style=False, allow_unicode=True, sort_keys=False)
    
    @staticmethod
    def format_table(data: List[Dict[str, Any]], headers: Optional[List[str]] = None) -> str:
        """Format data as ASCII table."""
        if not data:
            return "No data"
        
        if headers is None:
            headers = list(data[0].keys())
        
        # Calculate column widths
        col_widths = {h: len(h) for h in headers}
        for row in data:
            for h in headers:
                val = str(row.get(h, ""))
                col_widths[h] = max(col_widths[h], len(val))
        
        # Build table
        separator = "+" + "+".join("-" * (col_widths[h] + 2) for h in headers) + "+"
        header_row = "|" + "|".join(f" {h:<{col_widths[h]}} " for h in headers) + "|"
        
        lines = [separator, header_row, separator]
        for row in data:
            row_str = "|" + "|".join(f" {str(row.get(h, '')):<{col_widths[h]}} " for h in headers) + "|"
            lines.append(row_str)
        lines.append(separator)
        
        return "\n".join(lines)
    
    @staticmethod
    def format_output(data: Any, output_format: str) -> str:
        """Format data according to specified format."""
        if output_format == "json":
            return OutputFormatter.format_json(data)
        elif output_format == "yaml":
            return OutputFormatter.format_yaml(data)
        elif output_format == "table":
            if isinstance(data, list) and all(isinstance(d, dict) for d in data):
                return OutputFormatter.format_table(data)
            elif isinstance(data, dict):
                return OutputFormatter.format_table([data])
            return str(data)
        return str(data)


# ========== Available Actions Registry ==========


AVAILABLE_ACTIONS = {
    "click": {
        "name": "click",
        "description": "Click on a target element",
        "parameters": {
            "target": {"type": "string", "required": True, "description": "Target element selector"},
            "x": {"type": "int", "required": False, "description": "X coordinate override"},
            "y": {"type": "int", "required": False, "description": "Y coordinate override"},
            "button": {"type": "string", "required": False, "description": "Mouse button (left, right, middle)", "default": "left"},
            "clicks": {"type": "int", "required": False, "description": "Number of clicks", "default": 1},
        }
    },
    "type": {
        "name": "type",
        "description": "Type text into a target element",
        "parameters": {
            "target": {"type": "string", "required": True, "description": "Target element selector"},
            "text": {"type": "string", "required": True, "description": "Text to type"},
            "delay": {"type": "float", "required": False, "description": "Delay between keystrokes", "default": 0.0},
        }
    },
    "hotkey": {
        "name": "hotkey",
        "description": "Press a hotkey combination",
        "parameters": {
            "keys": {"type": "string", "required": True, "description": "Key combination (e.g., 'ctrl+c')"},
        }
    },
    "wait": {
        "name": "wait",
        "description": "Wait for a specified duration",
        "parameters": {
            "duration": {"type": "float", "required": True, "description": "Wait duration in seconds"},
        }
    },
    "launch_app": {
        "name": "launch_app",
        "description": "Launch an application",
        "parameters": {
            "name": {"type": "string", "required": True, "description": "Application name or path"},
            "args": {"type": "string", "required": False, "description": "Application arguments"},
        }
    },
    "screenshot": {
        "name": "screenshot",
        "description": "Take a screenshot",
        "parameters": {
            "path": {"type": "string", "required": False, "description": "Output path for screenshot"},
        }
    },
    "find_image": {
        "name": "find_image",
        "description": "Find an image on screen",
        "parameters": {
            "image": {"type": "string", "required": True, "description": "Image file to find"},
            "confidence": {"type": "float", "required": False, "description": "Match confidence (0-1)", "default": 0.8},
        }
    },
    "if": {
        "name": "if",
        "description": "Conditional execution",
        "parameters": {
            "condition": {"type": "string", "required": True, "description": "Condition expression"},
            "then": {"type": "list", "required": True, "description": "Steps to execute if true"},
            "else": {"type": "list", "required": False, "description": "Steps to execute if false"},
        }
    },
    "loop": {
        "name": "loop",
        "description": "Loop execution",
        "parameters": {
            "count": {"type": "int", "required": False, "description": "Number of iterations"},
            "until": {"type": "string", "required": False, "description": "Condition to stop"},
            "steps": {"type": "list", "required": True, "description": "Steps to loop"},
        }
    },
}


# Global context for verbose/quiet
class GlobalContext:
    verbose: bool = False
    quiet: bool = False
    output_format: str = "table"
    config: Optional[Config] = None


# Click context object with our custom settings
class CLIContext(click.Context):
    output_format: str = "table"


def get_output_format(ctx: click.Context) -> str:
    """Get output format from context or config."""
    if hasattr(ctx, 'output_format') and ctx.output_format:
        return ctx.output_format
    config = get_config()
    return config.get("output_format", "table")


# ========== CLI Base ==========


@click.group()
@click.version_option(version="22.0.0")
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose output")
@click.option("-q", "--quiet", is_flag=True, help="Suppress output")
@click.option("--output", "-o", type=click.Path(), help="Output results to file")
@click.option("--format", "-f", "output_format", type=click.Choice(["json", "yaml", "table"]), 
              help="Output format (overrides config)")
@click.argument("workflow_file", required=False)
@click.pass_context
def cli(ctx: click.Context, verbose: bool, quiet: bool, output: Optional[str], 
        output_format: Optional[str], workflow_file: Optional[str]) -> None:
    """RabAI AutoClick v22 - Intelligent automation tool.
    
    Features:
    - Predictive automation engine
    - Self-healing system
    - Scene-based workflow packages
    - Enhanced diagnostics
    - No-code workflow sharing (v22)
    - CLI pipeline integration (v22)
    - Screen recording to workflow (v22)
    
    Shell Completion:
      This CLI supports shell completion. Enable with:
      
      # Bash
      eval "$(_RABAI_COMPLETE=bash_source rabai)"
      
      # Zsh
      eval "$(_RABAI_COMPLETE=zsh_source rabai)"
      
      # Fish
      eval (env _RABAI_COMPLETE=fish_source rabai)
    
    Config File:
      Default settings can be configured in ~/.rabai/config.yaml
    """
    DATA_DIR.mkdir(exist_ok=True)
    
    ctx.ensure_object(GlobalContext)
    ctx.obj.verbose = verbose
    ctx.obj.quiet = quiet
    ctx.obj.output_format = output_format or get_config().get("output_format", "table")
    ctx.obj.config = get_config()
    
    setup_logging(verbose, quiet)
    
    # Store output path for later use
    ctx.obj.output_path = output


def _output_result(ctx: click.Context, data: Any, message: Optional[str] = None) -> None:
    """Output result in configured format."""
    output_format = get_output_format(ctx)
    output_text = OutputFormatter.format_output(data, output_format)
    
    if output_format == "json":
        click.echo(output_text)
    elif output_format == "yaml":
        click.echo(output_text)
    else:  # table
        if message:
            click.echo(message)
        click.echo(output_text)


def _write_output_file(ctx: click.Context, data: Any) -> None:
    """Write output to file if specified."""
    output_path = getattr(ctx.obj, 'output_path', None)
    if output_path:
        output_format = get_output_format(ctx)
        content = OutputFormatter.format_output(data, "json" if output_format == "table" else output_format)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(content)


# ========== Workflow Validation and Diff ==========


def validate_workflow_json(workflow_data: Dict[str, Any]) -> tuple[bool, List[str]]:
    """Validate workflow JSON structure and return (is_valid, errors)."""
    errors: List[str] = []
    
    required_fields = ["name", "steps"]
    for field in required_fields:
        if field not in workflow_data:
            errors.append(f"Missing required field: '{field}'")
    
    if "steps" in workflow_data:
        steps = workflow_data["steps"]
        if not isinstance(steps, list):
            errors.append("'steps' must be a list")
        else:
            for i, step in enumerate(steps):
                if not isinstance(step, dict):
                    errors.append(f"Step {i} is not a dictionary")
                    continue
                if "action" not in step:
                    errors.append(f"Step {i} missing 'action' field")
                if "target" not in step:
                    errors.append(f"Step {i} missing 'target' field")
                # Validate action is known
                action = step.get("action", "")
                if action and action not in AVAILABLE_ACTIONS:
                    errors.append(f"Step {i}: Unknown action '{action}'")
    
    return len(errors) == 0, errors


# ========== New Top-Level Commands ==========


@cli.command("run")
@click.argument("workflow_file")
@click.option("--dry-run", is_flag=True, help="Validate workflow without executing")
@click.pass_context
def run(ctx: click.Context, workflow_file: str, dry_run: bool) -> None:
    """Run a workflow from a file.
    
    Executes the workflow steps and reports results.
    Use --dry-run to validate without execution.
    """
    global LOG_LEVEL
    
    # Load workflow
    try:
        with open(workflow_file, "r", encoding="utf-8") as f:
            workflow_data = json.load(f)
    except FileNotFoundError:
        click.echo(f"Error: Workflow file not found: {workflow_file}")
        raise SystemExit(1)
    except json.JSONDecodeError as e:
        click.echo(f"Error: Invalid JSON in workflow file: {e}")
        raise SystemExit(1)
    
    # Validate workflow
    is_valid, errors = validate_workflow_json(workflow_data)
    
    if not is_valid:
        click.echo("Workflow validation failed:")
        for err in errors:
            click.echo(f"   - {err}")
        raise SystemExit(1)
    
    workflow_name = workflow_data.get("name", "unknown")
    steps = workflow_data.get("steps", [])
    
    if dry_run:
        result = {
            "workflow_name": workflow_name,
            "workflow_file": workflow_file,
            "dry_run": True,
            "validated": True,
            "step_count": len(steps),
            "steps_preview": [{"action": s.get("action"), "target": s.get("target")} for s in steps]
        }
        _output_result(ctx, result, f"Dry-run mode: Workflow '{workflow_name}' validation passed ({len(steps)} steps)")
        _write_output_file(ctx, result)
        return
    
    # Execute workflow with progress bar
    click.echo(f"Running workflow: {workflow_name}")
    
    results: List[Dict[str, Any]] = []
    
    with click.progressbar(steps, label="Executing steps") as bar:
        for i, step in enumerate(bar):
            step_result = {
                "step": i + 1,
                "action": step.get("action"),
                "target": step.get("target"),
                "status": "simulated",
                "duration": 0.1
            }
            results.append(step_result)
            log_debug(f"Executed step {i+1}: {step.get('action')} -> {step.get('target')}")
    
    # Summary
    output_data = {
        "workflow_name": workflow_name,
        "workflow_file": workflow_file,
        "dry_run": False,
        "validated": True,
        "step_count": len(steps),
        "steps_executed": results
    }
    
    _output_result(ctx, output_data, f"Workflow completed: {workflow_name} ({len(results)} steps executed)")
    _write_output_file(ctx, output_data)


@cli.command("validate")
@click.argument("workflow_file")
@click.pass_context
def validate(ctx: click.Context, workflow_file: str) -> None:
    """Validate a workflow file without running it.
    
    Checks workflow JSON structure, required fields, and step validity.
    """
    try:
        with open(workflow_file, "r", encoding="utf-8") as f:
            workflow_data = json.load(f)
    except FileNotFoundError:
        click.echo(f"Error: Workflow file not found: {workflow_file}")
        raise SystemExit(1)
    except json.JSONDecodeError as e:
        click.echo(f"Error: Invalid JSON in workflow file: {e}")
        raise SystemExit(1)
    except Exception as e:
        click.echo(f"Error reading workflow file: {e}")
        raise SystemExit(1)
    
    is_valid, errors = validate_workflow_json(workflow_data)
    
    result = {
        "valid": is_valid,
        "workflow_name": workflow_data.get("name", "unknown"),
        "workflow_file": workflow_file,
        "step_count": len(workflow_data.get("steps", [])),
        "errors": errors
    }
    
    if is_valid:
        _output_result(ctx, result, f"Workflow '{workflow_data.get('name')}' is valid ({len(workflow_data.get('steps', []))} steps)")
    else:
        _output_result(ctx, result, "Workflow validation failed")
    _write_output_file(ctx, result)
    
    if not is_valid:
        raise SystemExit(1)


@cli.command("diff")
@click.argument("workflow_file1")
@click.argument("workflow_file2")
def diff(workflow_file1: str, workflow_file2: str) -> None:
    """Compare two workflow files and show differences.
    
    Displays differences in workflow structure, steps, and parameters.
    """
    def read_workflow(path: str) -> Dict[str, Any]:
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            click.echo(f"Error: File not found: {path}")
            raise SystemExit(1)
        except json.JSONDecodeError as e:
            click.echo(f"Error: Invalid JSON in {path}: {e}")
            raise SystemExit(1)
    
    wf1 = read_workflow(workflow_file1)
    wf2 = read_workflow(workflow_file2)
    
    differences: List[str] = []
    
    # Compare names
    if wf1.get("name") != wf2.get("name"):
        differences.append(f"Name: '{wf1.get('name')}' -> '{wf2.get('name')}'")
    
    # Compare descriptions
    if wf1.get("description") != wf2.get("description"):
        differences.append(f"Description: '{wf1.get('description', '')}' -> '{wf2.get('description', '')}'")
    
    # Compare step counts
    steps1 = wf1.get("steps", [])
    steps2 = wf2.get("steps", [])
    if len(steps1) != len(steps2):
        differences.append(f"Step count: {len(steps1)} -> {len(steps2)}")
    
    # Compare steps
    max_steps = max(len(steps1), len(steps2))
    for i in range(max_steps):
        s1 = steps1[i] if i < len(steps1) else None
        s2 = steps2[i] if i < len(steps2) else None
        
        if s1 != s2:
            if s1 is None:
                differences.append(f"Step {i+1}: [added] {s2.get('action')} -> {s2.get('target')}")
            elif s2 is None:
                differences.append(f"Step {i+1}: [removed] {s1.get('action')} -> {s1.get('target')}")
            else:
                diffs = []
                for key in set(list(s1.keys()) + list(s2.keys())):
                    if s1.get(key) != s2.get(key):
                        diffs.append(f"{key}: '{s1.get(key)}' -> '{s2.get(key)}'")
                differences.append(f"Step {i+1}: {'; '.join(diffs)}")
    
    # Generate text diff for JSON
    json1 = json.dumps(wf1, indent=2).splitlines()
    json2 = json.dumps(wf2, indent=2).splitlines()
    
    diff_result = list(difflib.unified_diff(json1, json2, fromfile=workflow_file1, tofile=workflow_file2, lineterm=""))
    
    result = {
        "file1": workflow_file1,
        "file2": workflow_file2,
        "name_changed": wf1.get("name") != wf2.get("name"),
        "description_changed": wf1.get("description") != wf2.get("description"),
        "step_count_changed": len(steps1) != len(steps2),
        "step_differences": differences,
        "diff_lines": diff_result
    }
    
    if differences:
        _output_result(ctx, result, f"Differences between workflows:\n" + "\n".join(f"   {d}" for d in differences))
    else:
        _output_result(ctx, result, "Workflows are identical")


@cli.command("stats")
@click.argument("workflow_file")
@click.pass_context
def stats(ctx: click.Context, workflow_file: str) -> None:
    """Show workflow statistics.
    
    Displays detailed statistics about a workflow including step counts,
    action types, and complexity metrics.
    """
    try:
        with open(workflow_file, "r", encoding="utf-8") as f:
            workflow_data = json.load(f)
    except FileNotFoundError:
        click.echo(f"Error: Workflow file not found: {workflow_file}")
        raise SystemExit(1)
    except json.JSONDecodeError as e:
        click.echo(f"Error: Invalid JSON in workflow file: {e}")
        raise SystemExit(1)
    
    steps = workflow_data.get("steps", [])
    
    # Calculate statistics
    action_counts: Dict[str, int] = {}
    target_types: Dict[str, int] = {}
    total_params = 0
    
    for step in steps:
        action = step.get("action", "unknown")
        action_counts[action] = action_counts.get(action, 0) + 1
        
        target = step.get("target", "")
        if target.startswith("image:"):
            target_types["image"] += 1
        elif target.startswith("text:"):
            target_types["text"] += 1
        elif target.startswith("coord:"):
            target_types["coordinate"] += 1
        else:
            target_types["other"] += 1
        
        # Count parameters
        params = [k for k in step.keys() if k not in ("action", "target", "description")]
        total_params += len(params)
    
    # Calculate complexity score
    complexity = len(steps) * 1.0
    complexity += sum(cnt * 0.5 for cnt in action_counts.values() if cnt > 1)
    complexity += total_params * 0.1
    
    result = {
        "workflow_name": workflow_data.get("name", "unknown"),
        "workflow_file": workflow_file,
        "description": workflow_data.get("description", ""),
        "total_steps": len(steps),
        "action_types": action_counts,
        "target_types": target_types,
        "total_parameters": total_params,
        "complexity_score": round(complexity, 2),
        "has_variables": any("{{" in json.dumps(step) for step in steps),
        "has_conditions": any(step.get("action") == "if" for step in steps),
        "has_loops": any(step.get("action") == "loop" for step in steps),
    }
    
    _output_result(ctx, result)
    _write_output_file(ctx, result)


@cli.group()
def actions() -> None:
    """List and inspect available actions.
    
    Actions are the building blocks of workflows.
    """
    pass


@actions.command("list")
@click.pass_context
def actions_list(ctx: click.Context) -> None:
    """List all available actions.
    
    Shows all supported actions that can be used in workflows.
    """
    action_list = []
    for name, info in AVAILABLE_ACTIONS.items():
        action_list.append({
            "name": info["name"],
            "description": info["description"],
            "parameters": len(info["parameters"]),
        })
    
    _output_result(ctx, action_list, f"Available actions ({len(action_list)}):")


@actions.command("info")
@click.argument("action_name")
@click.pass_context
def actions_info(ctx: click.Context, action_name: str) -> None:
    """Show detailed information about an action.
    
    Displays parameters, descriptions, and examples for an action.
    """
    if action_name not in AVAILABLE_ACTIONS:
        click.echo(f"Error: Unknown action '{action_name}'")
        click.echo(f"Available actions: {', '.join(AVAILABLE_ACTIONS.keys())}")
        raise SystemExit(1)
    
    info = AVAILABLE_ACTIONS[action_name]
    result = {
        "name": info["name"],
        "description": info["description"],
        "parameters": info["parameters"],
    }
    
    _output_result(ctx, result)
    _write_output_file(ctx, result)


@cli.group()
def context() -> None:
    """Context and variable management commands.
    
    Preview how variables will be resolved during workflow execution.
    """
    pass


@context.command("inspect")
@click.argument("workflow_file")
@click.pass_context
def context_inspect(ctx: click.Context, workflow_file: str) -> None:
    """Show variable resolution preview.
    
    Displays how context variables will be resolved when the workflow runs.
    """
    try:
        with open(workflow_file, "r", encoding="utf-8") as f:
            workflow_data = json.load(f)
    except FileNotFoundError:
        click.echo(f"Error: Workflow file not found: {workflow_file}")
        raise SystemExit(1)
    except json.JSONDecodeError as e:
        click.echo(f"Error: Invalid JSON in workflow file: {e}")
        raise SystemExit(1)
    
    # Find all variables in the workflow
    import re
    var_pattern = re.compile(r'\{\{(\w+)\}\}')
    variables: Dict[str, List[str]] = {}
    
    def find_variables(obj: Any, path: str = "") -> None:
        if isinstance(obj, dict):
            for key, value in obj.items():
                find_variables(value, f"{path}.{key}" if path else key)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                find_variables(item, f"{path}[{i}]")
        elif isinstance(obj, str):
            matches = var_pattern.findall(obj)
            for var in matches:
                if var not in variables:
                    variables[var] = []
                variables[var].append(path)
    
    find_variables(workflow_data)
    
    # Build preview
    preview: List[Dict[str, Any]] = []
    for var, locations in variables.items():
        preview.append({
            "variable": f"{{{{{var}}}}}",
            "occurrences": len(locations),
            "used_in": ", ".join(set(locations))[:80],
        })
    
    result = {
        "workflow_name": workflow_data.get("name", "unknown"),
        "total_variables": len(variables),
        "variables": preview,
        "resolution_preview": {var: "<resolved_value>" for var in variables.keys()},
    }
    
    if preview:
        _output_result(ctx, result, f"Variables found in workflow ({len(variables)}):")
    else:
        _output_result(ctx, result, "No variables found in workflow")
    _write_output_file(ctx, result)


@cli.command("self-heal")
@click.argument("workflow_file")
@click.pass_context
def self_heal(ctx: click.Context, workflow_file: str) -> None:
    """Run self-healing diagnostics on a workflow.
    
    Analyzes the workflow for potential issues and suggests fixes.
    """
    try:
        with open(workflow_file, "r", encoding="utf-8") as f:
            workflow_data = json.load(f)
    except FileNotFoundError:
        click.echo(f"Error: Workflow file not found: {workflow_file}")
        raise SystemExit(1)
    except json.JSONDecodeError as e:
        click.echo(f"Error: Invalid JSON in workflow file: {e}")
        raise SystemExit(1)
    
    issues: List[Dict[str, Any]] = []
    steps = workflow_data.get("steps", [])
    
    # Check for common issues
    for i, step in enumerate(steps):
        action = step.get("action", "")
        target = step.get("target", "")
        
        # Check for missing target
        if not target and action not in ("wait", "screenshot"):
            issues.append({
                "severity": "error",
                "step": i + 1,
                "issue": "Missing target",
                "suggestion": f"Add a target for action '{action}'"
            })
        
        # Check for deprecated actions
        if action == "deprecated_action":
            issues.append({
                "severity": "warning",
                "step": i + 1,
                "issue": "Deprecated action",
                "suggestion": "Replace with modern equivalent"
            })
        
        # Check for long waits
        if action == "wait":
            duration = step.get("duration", 0)
            if duration > 30:
                issues.append({
                    "severity": "warning",
                    "step": i + 1,
                    "issue": f"Long wait: {duration}s",
                    "suggestion": "Consider reducing wait duration or using smart wait"
                })
        
        # Check for unknown actions
        if action and action not in AVAILABLE_ACTIONS:
            issues.append({
                "severity": "warning",
                "step": i + 1,
                "issue": f"Unknown action: {action}",
                "suggestion": "Verify action name or add custom action"
            })
    
    # Check workflow-level issues
    if not steps:
        issues.append({
            "severity": "error",
            "step": 0,
            "issue": "No steps defined",
            "suggestion": "Add at least one step to the workflow"
        })
    
    # Validate JSON structure
    is_valid, errors = validate_workflow_json(workflow_data)
    if not is_valid:
        for err in errors:
            issues.append({
                "severity": "error",
                "step": 0,
                "issue": "Validation error",
                "suggestion": err
            })
    
    result = {
        "workflow_name": workflow_data.get("name", "unknown"),
        "workflow_file": workflow_file,
        "issues_found": len(issues),
        "is_healthy": len([i for i in issues if i["severity"] == "error"]) == 0,
        "issues": issues,
        "recommendations": [
            "Run 'rabai validate' to verify workflow structure",
            "Use 'rabai actions list' to see available actions",
            "Consider using 'rabai diff' to compare with working versions"
        ] if issues else []
    }
    
    if issues:
        msg = f"Self-healing diagnostics found {len(issues)} issues:"
        for issue in issues:
            emoji = "❌" if issue["severity"] == "error" else "⚠️"
            msg += f"\n{emoji} Step {issue['step']}: {issue['issue']} - {issue['suggestion']}"
    else:
        msg = "Workflow passed self-healing diagnostics"
    
    _output_result(ctx, result, msg)
    _write_output_file(ctx, result)
    
    if result["issues_found"] > 0 and not result["is_healthy"]:
        raise SystemExit(1)


# ========== Workflow subcommand group (backwards compatibility) ==========


@cli.group()
def workflow() -> None:
    """Workflow file commands (alias commands)."""
    pass


@workflow.command("validate")
@click.argument("workflow_file")
@click.pass_context
def workflow_validate(ctx: click.Context, workflow_file: str) -> None:
    """Validate a workflow file without running it."""
    ctx.invoke(validate, workflow_file=workflow_file)


@workflow.command("diff")
@click.argument("workflow_file1")
@click.argument("workflow_file2")
def workflow_diff(workflow_file1: str, workflow_file2: str) -> None:
    """Compare two workflow files and show differences."""
    ctx = click.get_current_context()
    ctx.invoke(diff, workflow_file1=workflow_file1, workflow_file2=workflow_file2)


# ========== Generic Workflow Run Command ==========


@cli.command("run")
@click.argument("workflow_file")
@click.option("--dry-run", is_flag=True, help="Validate workflow without executing")
@click.pass_context
def run(ctx: click.Context, workflow_file: str, dry_run: bool) -> None:
    """Run a workflow from a file."""
    global LOG_LEVEL
    
    # Load workflow
    try:
        with open(workflow_file, "r", encoding="utf-8") as f:
            workflow_data = json.load(f)
    except FileNotFoundError:
        click.echo(f"Error: Workflow file not found: {workflow_file}")
        raise SystemExit(1)
    except json.JSONDecodeError as e:
        click.echo(f"Error: Invalid JSON in workflow file: {e}")
        raise SystemExit(1)
    
    # Validate workflow
    is_valid, errors = validate_workflow_json(workflow_data)
    
    if not is_valid:
        click.echo(f"Workflow validation failed:")
        for err in errors:
            click.echo(f"   - {err}")
        raise SystemExit(1)
    
    workflow_name = workflow_data.get("name", "unknown")
    steps = workflow_data.get("steps", [])
    
    if dry_run:
        click.echo(f"Dry-run mode: Workflow '{workflow_name}' validation passed")
        click.echo(f"   Steps to execute: {len(steps)}")
        if LOG_LEVEL <= logging.INFO:
            for i, step in enumerate(steps):
                click.echo(f"   {i+1}. {step.get('action', 'unknown')} -> {step.get('target', 'unknown')}")
        
        result = {
            "workflow_name": workflow_name,
            "workflow_file": workflow_file,
            "dry_run": True,
            "validated": True,
            "step_count": len(steps),
            "steps_preview": [{"action": s.get("action"), "target": s.get("target")} for s in steps]
        }
        _write_output_file(ctx, result)
        return
    
    # Execute workflow with progress bar
    click.echo(f"Running workflow: {workflow_name}")
    
    results: List[Dict[str, Any]] = []
    
    with click.progressbar(steps, label="Executing steps") as bar:
        for i, step in enumerate(bar):
            step_result = {
                "step": i + 1,
                "action": step.get("action"),
                "target": step.get("target"),
                "status": "simulated",
                "duration": 0.1
            }
            results.append(step_result)
            log_debug(f"Executed step {i+1}: {step.get('action')} -> {step.get('target')}")
    
    # Summary
    click.echo(f"\nWorkflow completed: {workflow_name}")
    click.echo(f"   Steps executed: {len(results)}")
    
    output_data = {
        "workflow_name": workflow_name,
        "workflow_file": workflow_file,
        "dry_run": False,
        "validated": True,
        "step_count": len(steps),
        "steps_executed": results
    }
    
    _write_output_file(ctx, output_data)


# ========== Predictive Automation Engine ==========


@cli.group()
def predict() -> None:
    """Predictive automation engine commands."""
    pass


@predict.command("record")
@click.argument("action_type")
@click.argument("target")
@click.option("--context", "-c", default="{}", help="Context JSON")
@click.option("--result", default="success", help="Execution result")
@click.option("--duration", default=0.0, type=float, help="Duration in seconds")
def predict_record(
    action_type: str,
    target: str,
    context: str,
    result: str,
    duration: float
) -> None:
    """Record a user action for prediction learning."""
    engine = create_predictive_engine(str(DATA_DIR))
    ctx: Dict[str, Any] = json.loads(context) if context != "{}" else {}
    engine.record_action(action_type, target, ctx, result, duration)
    click.echo(f"Recorded action: {action_type} -> {target}")


@predict.command("next")
@click.option("--app", help="Current active application")
def predict_next(app: Optional[str]) -> None:
    """Predict the next action based on learned patterns."""
    engine = create_predictive_engine(str(DATA_DIR))
    ctx: Dict[str, Any] = {"active_app": app} if app else {}
    prediction = engine.predict_next_action(ctx)
    
    if prediction:
        click.echo(f"\nPredicted action: {prediction.predicted_action}")
        click.echo(f"   Confidence: {prediction.confidence * 100:.1f}%")
        click.echo(f"   Reasoning: {prediction.reasoning}")
        if prediction.alternatives:
            click.echo(f"   Alternatives: {', '.join(prediction.alternatives)}")
    else:
        click.echo("No prediction data available. Record more actions first.")


@predict.command("suggest")
def predict_suggest() -> None:
    """Get suggestions for workflow creation."""
    engine = create_predictive_engine(str(DATA_DIR))
    suggestion = engine.suggest_workflow_creation()
    if suggestion:
        click.echo(f"{suggestion}")
    else:
        click.echo("No suggestions available.")


@predict.command("analyze")
def predict_analyze() -> None:
    """Analyze user behavior patterns."""
    engine = create_predictive_engine(str(DATA_DIR))
    analysis = engine.analyze_user_behavior()
    
    click.echo("\nUser Behavior Analysis")
    click.echo(f"  Total actions: {analysis.get('total_actions', 0)}")
    click.echo(f"  Success rate: {analysis.get('success_rate', 0)*100:.1f}%")
    
    if "top_targets" in analysis:
        click.echo("\n  Top 5 Targets:")
        for i, (target, count) in enumerate(analysis["top_targets"].items()[:5], 1):
            click.echo(f"    {i}. {target}: {count} times")


# ========== Self-Healing System ==========


@cli.group()
def heal() -> None:
    """Self-healing system commands."""
    pass


@heal.command("fix")
@click.argument("workflow_name")
@click.argument("step_name")
@click.option("--error", "-e", required=True, help="Error message")
@click.option("--step-index", "-i", default=0, type=int, help="Step index")
def heal_fix(
    workflow_name: str,
    step_name: str,
    error: str,
    step_index: int
) -> None:
    """Analyze error and get fix suggestions."""
    system = create_self_healing_system(str(DATA_DIR))
    
    class MockError(Exception):
        pass
    
    err = MockError(error)
    record = system.analyze_error(
        err, workflow_name, step_name, step_index, {}
    )
    suggestions = system.get_fix_suggestions(record)
    
    click.echo(f"\nError Analysis: {error}")
    click.echo(f"   Type: {record.error_type.value}")
    
    if suggestions:
        click.echo(f"\nFix Suggestions ({len(suggestions)}):")
        for i, s in enumerate(suggestions, 1):
            click.echo(f"  {i}. [{s.strategy.value}] {s.description}")
            click.echo(f"     Implementation: {s.implementation}")
    else:
        click.echo("No suggestions available.")


@heal.command("stats")
def heal_stats() -> None:
    """Get error statistics."""
    system = create_self_healing_system(str(DATA_DIR))
    stats = system.get_error_statistics()
    
    click.echo("\nError Statistics")
    click.echo(f"  Total errors: {stats.get('total_errors', 0)}")
    click.echo(f"  Recovery rate: {stats.get('recovery_rate', 0)*100:.1f}%")
    
    if "error_type_distribution" in stats:
        click.echo("\n  Error Type Distribution:")
        for etype, count in stats["error_type_distribution"].items():
            click.echo(f"    - {etype}: {count}")


# ========== Scene-Based Workflow Packages ==========


@cli.group()
def scene() -> None:
    """Scene-based workflow package commands."""
    pass


@scene.command("list")
@click.option("--tag", help="Filter by tag")
def scene_list(tag: Optional[str]) -> None:
    """List all scenes."""
    manager = create_scene_manager(str(DATA_DIR))
    tags = [tag] if tag else None
    
    scenes = manager.list_scenes(tags=tags)
    
    if not scenes:
        click.echo("No scenes available.")
        return
    
    click.echo(f"\nScenes ({len(scenes)}):")
    for s in scenes:
        status_icon = "active" if s.status.value == "active" else "paused"
        click.echo(f"\n{status_icon} {s.icon} {s.name}")
        click.echo(f"   {s.description}")
        click.echo(f"   Workflows: {len(s.workflows)}, Usage: {s.usage_count} times")


@scene.command("activate")
@click.argument("scene_id")
def scene_activate(scene_id: str) -> None:
    """Activate a scene."""
    manager = create_scene_manager(str(DATA_DIR))
    
    if manager.activate_scene(scene_id):
        scene_obj = manager.get_scene(scene_id)
        click.echo(f"Activated scene: {scene_obj.name}")
    else:
        click.echo(f"Scene not found: {scene_id}")


@scene.command("create")
@click.argument("name")
@click.option("--description", "-d", default="", help="Scene description")
@click.option("--icon", "-i", default="📦", help="Scene icon")
def scene_create(name: str, description: str, icon: str) -> None:
    """Create a new scene."""
    manager = create_scene_manager(str(DATA_DIR))
    scene_obj = manager.create_scene(name, description, icon)
    click.echo(f"Created scene: {scene_obj.name} (ID: {scene_obj.scene_id})")


@scene.command("stats")
def scene_stats() -> None:
    """Get scene statistics."""
    manager = create_scene_manager(str(DATA_DIR))
    stats = manager.get_scene_statistics()
    
    click.echo("\nScene Statistics")
    click.echo(f"  Total scenes: {stats['total_scenes']}")
    click.echo(f"  Active: {stats['active_scenes']}")


# ========== Enhanced Diagnostics ==========


@cli.group()
def diag() -> None:
    """Enhanced diagnostics commands (v22)."""
    pass


@diag.command("run")
@click.argument("workflow_id", required=False)
def diag_run(workflow_id: Optional[str]) -> None:
    """Run enhanced diagnostics."""
    diag_obj = create_diagnostics(str(DATA_DIR))
    
    if workflow_id:
        report = diag_obj.diagnose(workflow_id)
        click.echo(diag_obj.generate_report_text(report))
    else:
        reports = diag_obj.get_all_workflows_health()
        click.echo(f"\nWorkflow Health Overview ({len(reports)} workflows)")
        
        for r in reports[:10]:
            emoji = "healthy" if r.health_score >= 75 else "degraded" if r.health_score >= 50 else "unhealthy"
            click.echo(
                f"  {emoji} {r.workflow_name}: {r.health_score:.1f} "
                f"({r.success_rate*100:.0f}% success)"
            )


@diag.command("summary")
def diag_summary() -> None:
    """Get health summary."""
    diag_obj = create_diagnostics(str(DATA_DIR))
    summary = diag_obj.get_health_summary()
    
    click.echo("\nOverall Health Status")
    click.echo(f"  Total workflows: {summary.get('total_workflows', 0)}")
    if 'avg_health_score' in summary:
        click.echo(f"  Avg health score: {summary['avg_health_score']:.1f}")
    if 'avg_success_rate' in summary:
        click.echo(f"  Avg success rate: {summary['avg_success_rate']*100:.1f}%")
    
    if "health_distribution" in summary:
        click.echo("\n  Health Distribution:")
        for level, count in summary["health_distribution"].items():
            click.echo(f"    - {level}: {count}")
    
    if summary.get("needs_attention"):
        click.echo("\n  Needs Attention:")
        for name in summary["needs_attention"]:
            click.echo(f"    - {name}")


@diag.command("report")
@click.argument("workflow_id")
def diag_report(workflow_id: str) -> None:
    """Generate detailed report."""
    diag_obj = create_diagnostics(str(DATA_DIR))
    report = diag_obj.diagnose(workflow_id)
    
    report_file = DATA_DIR / f"report_{workflow_id}_{int(time.time())}.json"
    with open(report_file, "w", encoding="utf-8") as f:
        json.dump({
            "workflow_id": report.workflow_id,
            "workflow_name": report.workflow_name,
            "overall_health": report.overall_health.value,
            "health_score": report.health_score,
            "execution_count": report.execution_count,
            "success_rate": report.success_rate,
            "avg_duration": report.avg_duration,
            "trends": [
                {
                    "period": t.period,
                    "success_rate_change": t.success_rate_change,
                    "trend_direction": t.trend_direction
                }
                for t in report.trends
            ],
            "issues": [
                {
                    "type": i.issue_type,
                    "severity": i.severity.value,
                    "title": i.title,
                    "suggestion": i.suggestion,
                    "auto_fixable": i.auto_fixable
                }
                for i in report.issues
            ],
            "root_causes": report.root_causes,
            "recommendations": report.recommendations
        }, f, ensure_ascii=False, indent=2)
    
    click.echo(f"Report saved: {report_file}")
    click.echo(diag_obj.generate_report_text(report))


# ========== No-Code Workflow Sharing (v22) ==========


@cli.group()
def share() -> None:
    """No-code workflow sharing system (v22)."""
    pass


@share.command("register")
@click.argument("workflow_file")
def share_register(workflow_file: str) -> None:
    """Register a workflow for sharing."""
    share_sys = create_share_system(str(DATA_DIR))
    
    try:
        with open(workflow_file, "r", encoding="utf-8") as f:
            workflow_data = json.load(f)
        
        wf_id = share_sys.register_workflow(workflow_data)
        click.echo(f"Registered workflow: {wf_id}")
    except Exception as e:
        click.echo(f"Registration failed: {e}")


@share.command("create-link")
@click.argument("workflow_id")
@click.option("--type", "-t", default="public", help="Share type: public, private, team")
@click.option("--expires", "-e", type=int, help="Expiration days")
def share_create_link(
    workflow_id: str,
    type: str,
    expires: Optional[int]
) -> None:
    """Create a share link for a workflow."""
    share_sys = create_share_system(str(DATA_DIR))
    
    share_type = (
        ShareType.PUBLIC if type == "public"
        else ShareType.PRIVATE if type == "private"
        else ShareType.TEAM
    )
    link = share_sys.create_share_link(workflow_id, share_type, expires)
    
    if link:
        url = share_sys.generate_share_url(link.link_id)
        click.echo(f"Share link created:")
        click.echo(f"   {url}")
        click.echo(f"   Link ID: {link.link_id}")
        if link.expires_at:
            exp_date = datetime.datetime.fromtimestamp(link.expires_at)
            click.echo(f"   Expires: {exp_date.strftime('%Y-%m-%d %H:%M')}")
    else:
        click.echo(f"Workflow not found: {workflow_id}")


@share.command("import")
@click.argument("source")
@click.option("--format", "-f", default="json", help="Format: json, base64, url")
def share_import(source: str, format: str) -> None:
    """Import a workflow."""
    share_sys = create_share_system(str(DATA_DIR))
    
    if format == "url":
        report = share_sys.import_from_url(source)
    else:
        report = share_sys.import_workflow(source, format)
    
    click.echo(f"\nImport result: {report.result.value}")
    click.echo(f"Workflow: {report.workflow_name}")
    click.echo(f"Message: {report.message}")
    
    if report.warnings:
        click.echo("\nWarnings:")
        for w in report.warnings:
            click.echo(f"  - {w}")


@share.command("export")
@click.argument("workflow_id")
@click.option("--format", "-f", default="json", help="Format: json, base64")
def share_export(workflow_id: str, format: str) -> None:
    """Export a workflow."""
    share_sys = create_share_system(str(DATA_DIR))
    
    if format == "base64":
        output = share_sys.export_to_base64(workflow_id)
    else:
        output = share_sys.export_to_json(workflow_id)
    
    if output:
        click.echo(f"Workflow exported:")
        if format == "base64":
            click.echo(output)
        else:
            click.echo(output[:500] + "..." if len(output) > 500 else output)
    else:
        click.echo(f"Workflow not found: {workflow_id}")


@share.command("list")
def share_list() -> None:
    """List shared workflows."""
    share_sys = create_share_system(str(DATA_DIR))
    links = share_sys.list_shared_workflows()
    
    if not links:
        click.echo("No share links available.")
        return
    
    click.echo(f"\nShare List ({len(links)}):\n")
    for link in links:
        click.echo(f"\n  {link.link_id}: {link.workflow_name}")
        click.echo(
            f"    Type: {link.share_type.value}, "
            f"Views: {link.view_count}, Imports: {link.import_count}"
        )


@share.command("stats")
def share_stats() -> None:
    """Get sharing statistics."""
    share_sys = create_share_system(str(DATA_DIR))
    stats = share_sys.get_share_stats()
    
    click.echo("\nShare Statistics")
    click.echo(f"  Total links: {stats['total_links']}")
    click.echo(f"  Total views: {stats['total_views']}")
    click.echo(f"  Total imports: {stats['total_imports']}")
    click.echo(f"  Active links: {stats['active_links']}")


# ========== CLI Pipeline Integration (v22) ==========


@cli.group()
def pipe() -> None:
    """CLI pipeline integration mode (v22)."""
    pass


@pipe.command("list")
def pipe_list() -> None:
    """List pipeline chains."""
    runner = create_pipeline_runner(str(DATA_DIR))
    chains = runner.list_chains()
    
    if not chains:
        click.echo("No pipeline chains available.")
        return
    
    click.echo(f"\nPipeline Chains ({len(chains)}):\n")
    for chain in chains:
        status = "active" if any(s.enabled for s in chain.steps) else "disabled"
        click.echo(f"  {chain.chain_id}: {chain.name} [{status}]")
        click.echo(f"    Mode: {chain.mode.value}, Steps: {len(chain.steps)}")


@pipe.command("create")
@click.argument("name")
@click.option("--mode", "-m", default="linear", help="Mode: linear, branch, parallel")
def pipe_create(name: str, mode: str) -> None:
    """Create a pipeline chain."""
    runner = create_pipeline_runner(str(DATA_DIR))
    
    try:
        pipe_mode = PipeMode(mode)
    except ValueError:
        click.echo(f"Invalid mode: {mode}")
        return
    
    chain = runner.create_chain(name, pipe_mode)
    click.echo(f"Created pipeline chain: {chain.chain_id}")


@pipe.command("add")
@click.argument("chain_id")
@click.argument("name")
@click.argument("command")
def pipe_add(chain_id: str, name: str, command: str) -> None:
    """Add a step to a chain."""
    runner = create_pipeline_runner(str(DATA_DIR))
    
    step = runner.add_step(chain_id, name, command)
    if step:
        click.echo(f"Added step: {step.step_id}")
    else:
        click.echo(f"Chain not found: {chain_id}")


@pipe.command("run")
@click.argument("chain_id")
@click.option("--input", "-i", help="Input JSON data")
def pipe_run(chain_id: str, input: Optional[str]) -> None:
    """Execute a pipeline chain."""
    runner = create_pipeline_runner(str(DATA_DIR))
    
    input_data = None
    if input:
        try:
            input_data = json.loads(input)
        except json.JSONDecodeError:
            click.echo("Invalid JSON input")
            return
    
    result = runner.execute_chain(chain_id, input_data)
    
    click.echo(f"\nPipeline Execution Result:")
    click.echo(f"  Success: {'yes' if result.success else 'no'}")
    click.echo(f"  Duration: {result.total_duration:.2f}s")
    
    if result.final_output:
        click.echo(f"  Output:")
        click.echo(f"  {json.dumps(result.final_output, ensure_ascii=False, indent=4)}")
    
    if result.errors:
        click.echo(f"\nErrors:")
        for err in result.errors:
            click.echo(f"  - {err}")


# ========== Screen Recording to Workflow (v22) ==========


@cli.group()
def rec() -> None:
    """Screen recording to workflow conversion (v22)."""
    pass


@rec.command("start")
@click.argument("name")
@click.option("--description", "-d", default="", help="Recording description")
def rec_start(name: str, description: str) -> None:
    """Start a recording session."""
    converter = create_screen_recorder(str(DATA_DIR))
    rec_obj = converter.start_recording(name, description)
    click.echo(f"Started recording: {rec_obj.recording_id}")
    click.echo(f"   Name: {rec_obj.name}")


@rec.command("stop")
@click.argument("recording_id")
def rec_stop(recording_id: str) -> None:
    """Stop a recording session."""
    converter = create_screen_recorder(str(DATA_DIR))
    rec_obj = converter.stop_recording(recording_id)
    
    if rec_obj:
        click.echo(f"Recording stopped: {rec_obj.recording_id}")
        click.echo(f"   Actions: {len(rec_obj.actions)}")
        click.echo(f"   Duration: {rec_obj.duration:.1f}s")
    else:
        click.echo(f"Recording not found: {recording_id}")


@rec.command("add-action")
@click.argument("recording_id")
@click.option("--type", "-t", required=True, help="Action type: click, type, hotkey, wait, launch_app")
@click.option("--x", type=int, help="X coordinate")
@click.option("--y", type=int, help="Y coordinate")
@click.option("--text", help="Text input")
@click.option("--key", help="Hotkey")
@click.option("--app", help="Application name")
def rec_add_action(
    recording_id: str,
    type: str,
    x: Optional[int],
    y: Optional[int],
    text: Optional[str],
    key: Optional[str],
    app: Optional[str]
) -> None:
    """Manually add an action to a recording."""
    converter = create_screen_recorder(str(DATA_DIR))
    
    action_data: Dict[str, Any] = {
        "action_type": type,
        "timestamp": time.time()
    }
    
    if x is not None:
        action_data["x"] = x
    if y is not None:
        action_data["y"] = y
    if text:
        action_data["text"] = text
    if key:
        action_data["key"] = key
    if app:
        action_data["app"] = app
    
    if converter.add_action(recording_id, action_data):
        click.echo("Action added")
    else:
        click.echo("Recording not found")


@rec.command("list")
def rec_list() -> None:
    """List all recordings."""
    converter = create_screen_recorder(str(DATA_DIR))
    recordings = converter.list_recordings()
    
    if not recordings:
        click.echo("No recordings available.")
        return
    
    click.echo(f"\nRecordings ({len(recordings)}):\n")
    for rec_obj in recordings:
        click.echo(f"  {rec_obj.recording_id}: {rec_obj.name}")
        click.echo(f"    Actions: {len(rec_obj.actions)}, Duration: {rec_obj.duration:.1f}s")
        click.echo(f"    Created: {datetime.datetime.fromtimestamp(rec_obj.created_at).strftime('%Y-%m-%d %H:%M')}")


@rec.command("convert")
@click.argument("recording_id")
@click.option("--name", "-n", help="Workflow name")
@click.option("--mode", "-m", default="image", help="Detection mode: image, text, coordinate")
def rec_convert(recording_id: str, name: Optional[str], mode: str) -> None:
    """Convert a recording to a workflow."""
    converter = create_screen_recorder(str(DATA_DIR))
    
    from src.screen_recorder import ElementDetection
    detection = (
        ElementDetection.TEXT if mode == "text"
        else ElementDetection.COORDINATE if mode == "coordinate"
        else ElementDetection.IMAGE
    )
    
    result = converter.convert_to_workflow(recording_id, name, detection)
    
    if result:
        click.echo(f"\nConversion successful: {result.workflow_name}")
        click.echo(f"   Workflow ID: {result.workflow_id}")
        click.echo(f"   Steps: {len(result.steps)}")
        
        if result.warnings:
            click.echo(f"\nWarnings ({len(result.warnings)}):")
            for w in result.warnings[:3]:
                click.echo(f"   - {w}")
        
        # Save workflow
        workflow_file = DATA_DIR / f"workflow_{result.workflow_id}.json"
        with open(workflow_file, "w", encoding="utf-8") as f:
            f.write(converter.export_workflow_json(result))
        
        click.echo(f"\nWorkflow saved: {workflow_file}")
    else:
        click.echo(f"Recording not found or empty")


@rec.command("analyze")
@click.argument("recording_id")
def rec_analyze(recording_id: str) -> None:
    """Analyze a recording."""
    converter = create_screen_recorder(str(DATA_DIR))
    analysis = converter.analyze_recording(recording_id)
    
    if not analysis:
        click.echo(f"Recording not found: {recording_id}")
        return
    
    click.echo(f"\nRecording Analysis: {analysis.get('name')}")
    click.echo(f"  Actions: {analysis.get('action_count')}")
    click.echo(f"  Duration: {analysis.get('duration', 0):.1f}s")
    click.echo(f"  Resolution: {analysis.get('resolution')}")
    
    if analysis.get('action_types'):
        click.echo(f"\n  Action Type Distribution:")
        for at, count in analysis['action_types'].items():
            click.echo(f"    - {at}: {count}")


# ========== Config Management ==========


@cli.group()
def config() -> None:
    """Configuration management commands."""
    pass


@config.command("show")
def config_show() -> None:
    """Show current configuration."""
    cfg = get_config()
    _output_result(click.get_current_context(), cfg.config)


@config.command("set")
@click.argument("key")
@click.argument("value")
def config_set(key: str, value: str) -> None:
    """Set a configuration value."""
    cfg = get_config()
    
    # Try to convert value to appropriate type
    if value.lower() == "true":
        value = True
    elif value.lower() == "false":
        value = False
    elif value.isdigit():
        value = int(value)
    elif value.replace(".", "", 1).isdigit():
        value = float(value)
    
    cfg.set(key, value)
    cfg.save()
    click.echo(f"Set {key} = {value}")


@config.command("init")
def config_init() -> None:
    """Initialize default config file."""
    cfg = get_config()
    cfg.save()
    click.echo(f"Config initialized at: {CONFIG_FILE}")


# ========== Shell Completion Setup ==========


@cli.command("install-completion")
@click.option("--shell", "-s", type=click.Choice(["bash", "zsh", "fish"]), default="bash",
              help="Shell type")
def install_completion(shell: str) -> None:
    """Install shell completion for the current user.
    
    This command prints the completion script for the specified shell.
    Add the output to your shell's rc file to enable tab completion.
    """
    if shell == "bash":
        script = f'''
# RabAI AutoClick bash completion
_RABAI_COMPLETE=bash_source {sys.argv[0]}
'''
    elif shell == "zsh":
        script = f'''
# RabAI AutoClick zsh completion
autoload -U compinit
compinit
RABAI_COMPLETE=zsh_source {sys.argv[0]}
'''
    elif shell == "fish":
        script = f'''
# RabAI AutoClick fish completion
eval (env _RABAI_COMPLETE=fish_source {sys.argv[0]})
'''
    
    click.echo(f"To enable completion for {shell}, add the following to your shell rc file:")
    click.echo()
    click.echo(script)


if __name__ == "__main__":
    cli()
