"""Automation Import Action Module. Imports workflows from external formats."""
import sys, os, json, yaml, re
from typing import Any
from dataclasses import dataclass, field
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

@dataclass
class WorkflowDef:
    name: str; description: str; steps: list; triggers: list = field(default_factory=list)
    metadata: dict = field(default_factory=dict)

class AutomationImportAction(BaseAction):
    action_type = "automation_import"; display_name = "自动化导入"
    description = "从外部格式导入工作流"
    def __init__(self) -> None: super().__init__(); self._imported = {}
    def _parse_python(self, content: str) -> WorkflowDef:
        name_match = re.search(r'def\s+(\w+)_workflow', content)
        name = name_match.group(1) if name_match else "unknown"
        steps = re.findall(r'action[_\s]*name[=:\s]*["\']?(\w+)', content, re.IGNORECASE)
        return WorkflowDef(name=name, description="", steps=[{"action": s} for s in steps])
    def execute(self, context: Any, params: dict) -> ActionResult:
        source = params.get("source"); source_type = params.get("source_type","auto")
        workflow_name = params.get("workflow_name"); validate = params.get("validate", True)
        if not source: return ActionResult(success=False, message="No source provided")
        content = source
        if source_type in ("file","auto") and os.path.isfile(str(source)):
            with open(str(source), "r", encoding="utf-8", errors="replace") as f: content = f.read()
            if source_type == "auto": source_type = "json" if source.endswith(".json") else "yaml" if source.endswith((".yaml",".yml")) else "python" if source.endswith(".py") else "string"
        elif source_type == "auto":
            try: json.loads(source); source_type = "json"
            except: source_type = "yaml" if ":" in source else "string"
        try:
            if source_type == "json": parsed = json.loads(content)
            elif source_type == "yaml": parsed = yaml.safe_load(content)
            elif source_type == "python": parsed = self._parse_python(content)
            else: parsed = {"raw": content}
            if isinstance(parsed, dict):
                name = workflow_name or parsed.get("name","imported_workflow")
                workflow = WorkflowDef(name=name, description=parsed.get("description",""),
                                     steps=parsed.get("steps", parsed.get("actions",[])),
                                     triggers=parsed.get("triggers",[]),
                                     metadata=parsed.get("metadata",{}))
            else: workflow = WorkflowDef(name=workflow_name or "imported", description="", steps=parsed if isinstance(parsed,list) else [])
            if validate and not workflow.steps: return ActionResult(success=False, message="No steps")
            self._imported[workflow.name] = workflow
            return ActionResult(success=True, message=f"Imported '{workflow.name}' with {len(workflow.steps)} steps", data=vars(workflow))
        except Exception as e: return ActionResult(success=False, message=f"Error: {e}")
