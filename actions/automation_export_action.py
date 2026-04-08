"""Automation Export Action Module. Exports workflows to various formats."""
import sys, os, json, yaml
from typing import Any
from dataclasses import dataclass
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

@dataclass
class WorkflowSpec:
    name: str; description: str; steps: list; triggers: list; version: str = "1.0.0"

class AutomationExportAction(BaseAction):
    action_type = "automation_export"; display_name = "自动化导出"
    description = "导出工作流"
    def __init__(self) -> None: super().__init__()
    def execute(self, context: Any, params: dict) -> ActionResult:
        workflow_data = params.get("workflow",{}); output_format = params.get("format","json").lower()
        output_file = params.get("output_file")
        if isinstance(workflow_data, dict):
            workflow = WorkflowSpec(name=workflow_data.get("name","workflow"),
                                  description=workflow_data.get("description",""),
                                  steps=workflow_data.get("steps",[]),
                                  triggers=workflow_data.get("triggers",[]),
                                  version=workflow_data.get("version","1.0.0"))
        else: return ActionResult(success=False, message="Invalid workflow data")
        if output_format == "json": content = json.dumps(vars(workflow), indent=2)
        elif output_format == "yaml": content = yaml.dump(vars(workflow), default_flow_style=False)
        elif output_format == "python":
            lines = [f'"""Exported workflow: {workflow.name}"""', '', f'NAME = "{workflow.name}"',
                    f'DESCRIPTION = "{workflow.description}"', f'VERSION = "{workflow.version}"',
                    f'TRIGGERS = {workflow.triggers}', '', 'STEPS = [']
            for step in workflow.steps: lines.append(f"    {step},")
            lines.append("]")
            content = "\n".join(lines)
        elif output_format == "markdown":
            lines = [f"# Workflow: {workflow.name}", "", f"**Version:** {workflow.version}",
                    f"**Description:** {workflow.description}", "", "## Triggers"]
            for t in workflow.triggers: lines.append(f"- `{t}`")
            lines += ["", "## Steps", "| # | Action | Parameters |", "|---|---|---|"]
            for i, step in enumerate(workflow.steps, 1):
                lines.append(f"| {i} | `{step.get('action','unknown')}` | {json.dumps(step.get('params',{}))} |")
            content = "\n".join(lines)
        else: return ActionResult(success=False, message=f"Unknown format: {output_format}")
        if output_file:
            try:
                with open(output_file, "w", encoding="utf-8") as f: f.write(content)
                return ActionResult(success=True, message=f"Exported to {output_file}", data={"path": output_file})
            except Exception as e: return ActionResult(success=False, message=f"Write error: {e}")
        return ActionResult(success=True, message=f"Exported {workflow.name} as {output_format}: {len(content)} chars",
                          data={"content": content})
