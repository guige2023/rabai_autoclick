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


# Global context for verbose/quiet
class GlobalContext:
    verbose: bool = False
    quiet: bool = False


@click.group()
@click.version_option(version="22.0.0")
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose output")
@click.option("-q", "--quiet", is_flag=True, help="Suppress output")
@click.pass_context
def cli(ctx: click.Context, verbose: bool, quiet: bool) -> None:
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
    """
    DATA_DIR.mkdir(exist_ok=True)
    ctx.ensure_object(GlobalContext)
    ctx.obj.verbose = verbose
    ctx.obj.quiet = quiet
    setup_logging(verbose, quiet)


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
@click.option("--duration", default=0.0, type=float, help="Duration in seconds)")
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
    click.echo(f"✓ Recorded action: {action_type} -> {target}")


@predict.command("next")
@click.option("--app", help="Current active application")
def predict_next(app: Optional[str]) -> None:
    """Predict the next action based on learned patterns."""
    engine = create_predictive_engine(str(DATA_DIR))
    ctx: Dict[str, Any] = {"active_app": app} if app else {}
    prediction = engine.predict_next_action(ctx)
    
    if prediction:
        click.echo(f"\n🔮 Predicted action: {prediction.predicted_action}")
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
        click.echo(f"💡 {suggestion}")
    else:
        click.echo("No suggestions available.")


@predict.command("analyze")
def predict_analyze() -> None:
    """Analyze user behavior patterns."""
    engine = create_predictive_engine(str(DATA_DIR))
    analysis = engine.analyze_user_behavior()
    
    click.echo("\n📊 User Behavior Analysis")
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
    
    click.echo(f"\n🔧 Error Analysis: {error}")
    click.echo(f"   Type: {record.error_type.value}")
    
    if suggestions:
        click.echo(f"\n💡 Fix Suggestions ({len(suggestions)}):")
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
    
    click.echo("\n📊 Error Statistics")
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
    
    click.echo(f"\n📦 Scenes ({len(scenes)}):")
    for s in scenes:
        status_icon = "✅" if s.status.value == "active" else "⏸️"
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
        click.echo(f"✅ Activated scene: {scene_obj.name}")
    else:
        click.echo(f"❌ Scene not found: {scene_id}")


@scene.command("create")
@click.argument("name")
@click.option("--description", "-d", default="", help="Scene description")
@click.option("--icon", "-i", default="📦", help="Scene icon")
def scene_create(name: str, description: str, icon: str) -> None:
    """Create a new scene."""
    manager = create_scene_manager(str(DATA_DIR))
    scene_obj = manager.create_scene(name, description, icon)
    click.echo(f"✅ Created scene: {scene_obj.name} (ID: {scene_obj.scene_id})")


@scene.command("stats")
def scene_stats() -> None:
    """Get scene statistics."""
    manager = create_scene_manager(str(DATA_DIR))
    stats = manager.get_scene_statistics()
    
    click.echo("\n📊 Scene Statistics")
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
        click.echo(f"\n📊 Workflow Health Overview ({len(reports)} workflows)")
        
        for r in reports[:10]:
            emoji = "🟢" if r.health_score >= 75 else "🟡" if r.health_score >= 50 else "🔴"
            click.echo(
                f"  {emoji} {r.workflow_name}: {r.health_score:.1f} "
                f"({r.success_rate*100:.0f}% success)"
            )


@diag.command("summary")
def diag_summary() -> None:
    """Get health summary."""
    diag_obj = create_diagnostics(str(DATA_DIR))
    summary = diag_obj.get_health_summary()
    
    click.echo("\n📊 Overall Health Status")
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
        click.echo("\n  ⚠️ Needs Attention:")
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
    
    click.echo(f"✅ Report saved: {report_file}")
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
        click.echo(f"✅ Registered workflow: {wf_id}")
    except Exception as e:
        click.echo(f"❌ Registration failed: {e}")


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
        click.echo(f"✅ Share link created:")
        click.echo(f"   {url}")
        click.echo(f"   Link ID: {link.link_id}")
        if link.expires_at:
            exp_date = datetime.datetime.fromtimestamp(link.expires_at)
            click.echo(f"   Expires: {exp_date.strftime('%Y-%m-%d %H:%M')}")
    else:
        click.echo(f"❌ Workflow not found: {workflow_id}")


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
        click.echo(f"✅ Workflow exported:")
        if format == "base64":
            click.echo(output)
        else:
            click.echo(output[:500] + "..." if len(output) > 500 else output)
    else:
        click.echo(f"❌ Workflow not found: {workflow_id}")


@share.command("list")
def share_list() -> None:
    """List shared workflows."""
    share_sys = create_share_system(str(DATA_DIR))
    links = share_sys.list_shared_workflows()
    
    if not links:
        click.echo("No share links available.")
        return
    
    click.echo(f"\n📤 Share List ({len(links)}):\n")
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
    
    click.echo("\n📊 Share Statistics")
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
    
    click.echo(f"\n🔗 Pipeline Chains ({len(chains)}):\n")
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
        click.echo(f"❌ Invalid mode: {mode}")
        return
    
    chain = runner.create_chain(name, pipe_mode)
    click.echo(f"✅ Created pipeline chain: {chain.chain_id}")


@pipe.command("add")
@click.argument("chain_id")
@click.argument("name")
@click.argument("command")
def pipe_add(chain_id: str, name: str, command: str) -> None:
    """Add a step to a chain."""
    runner = create_pipeline_runner(str(DATA_DIR))
    
    step = runner.add_step(chain_id, name, command)
    if step:
        click.echo(f"✅ Added step: {step.step_id}")
    else:
        click.echo(f"❌ Chain not found: {chain_id}")


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
            click.echo("❌ Invalid JSON input")
            return
    
    result = runner.execute_chain(chain_id, input_data)
    
    click.echo(f"\nPipeline Execution Result:")
    click.echo(f"  Success: {'✅' if result.success else '❌'}")
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
    click.echo(f"✅ Started recording: {rec_obj.recording_id}")
    click.echo(f"   Name: {rec_obj.name}")


@rec.command("stop")
@click.argument("recording_id")
def rec_stop(recording_id: str) -> None:
    """Stop a recording session."""
    converter = create_screen_recorder(str(DATA_DIR))
    rec_obj = converter.stop_recording(recording_id)
    
    if rec_obj:
        click.echo(f"✅ Recording stopped: {rec_obj.recording_id}")
        click.echo(f"   Actions: {len(rec_obj.actions)}")
        click.echo(f"   Duration: {rec_obj.duration:.1f}s")
    else:
        click.echo(f"❌ Recording not found: {recording_id}")


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
        click.echo(f"✅ Action added")
    else:
        click.echo(f"❌ Recording not found")


@rec.command("list")
def rec_list() -> None:
    """List all recordings."""
    converter = create_screen_recorder(str(DATA_DIR))
    recordings = converter.list_recordings()
    
    if not recordings:
        click.echo("No recordings available.")
        return
    
    click.echo(f"\n🎬 Recordings ({len(recordings)}):\n")
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
        click.echo(f"\n✅ Conversion successful: {result.workflow_name}")
        click.echo(f"   Workflow ID: {result.workflow_id}")
        click.echo(f"   Steps: {len(result.steps)}")
        
        if result.warnings:
            click.echo(f"\n⚠️ Warnings ({len(result.warnings)}):")
            for w in result.warnings[:3]:
                click.echo(f"   - {w}")
        
        # Save workflow
        workflow_file = DATA_DIR / f"workflow_{result.workflow_id}.json"
        with open(workflow_file, "w", encoding="utf-8") as f:
            f.write(converter.export_workflow_json(result))
        
        click.echo(f"\n💾 Workflow saved: {workflow_file}")
    else:
        click.echo(f"❌ Recording not found or empty")


@rec.command("analyze")
@click.argument("recording_id")
def rec_analyze(recording_id: str) -> None:
    """Analyze a recording."""
    converter = create_screen_recorder(str(DATA_DIR))
    analysis = converter.analyze_recording(recording_id)
    
    if not analysis:
        click.echo(f"❌ Recording not found: {recording_id}")
        return
    
    click.echo(f"\n📊 Recording Analysis: {analysis.get('name')}")
    click.echo(f"  Actions: {analysis.get('action_count')}")
    click.echo(f"  Duration: {analysis.get('duration', 0):.1f}s")
    click.echo(f"  Resolution: {analysis.get('resolution')}")
    
    if analysis.get('action_types'):
        click.echo(f"\n  Action Type Distribution:")
        for at, count in analysis['action_types'].items():
            click.echo(f"    - {at}: {count}")


if __name__ == "__main__":
    cli()
