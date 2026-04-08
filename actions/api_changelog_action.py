"""API Changelog Action Module. Tracks API changes and generates release notes."""
import sys, os
from typing import Any
from dataclasses import dataclass, field
from datetime import datetime
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

@dataclass
class ChangelogEntry:
    version: str; date: str; change_type: str; description: str
    endpoint: str = ""; breaking: bool = False

class APIChangelogAction(BaseAction):
    action_type = "api_changelog"; display_name = "API变更日志"
    description = "追踪API变更历史"
    def __init__(self) -> None: super().__init__(); self._entries = []
    def execute(self, context: Any, params: dict) -> ActionResult:
        mode = params.get("mode", "list")
        if mode == "add":
            entry = ChangelogEntry(version=params.get("version","1.0.0"),
                                   date=params.get("date", datetime.utcnow().strftime("%Y-%m-%d")),
                                   change_type=params.get("change_type","changed"),
                                   description=params.get("description",""),
                                   endpoint=params.get("endpoint",""),
                                   breaking=params.get("breaking", False))
            self._entries.insert(0, entry)
            return ActionResult(success=True, message=f"Added {entry.change_type} for v{entry.version}")
        if mode == "list":
            return ActionResult(success=True, message=f"{len(self._entries)} entries", data={"entries": [vars(e) for e in self._entries]})
        version = params.get("version")
        entries = [e for e in self._entries if e.version == version] if version else self._entries
        by_ver = {}
        for e in entries: by_ver.setdefault(e.version, []).append(e)
        lines = ["# API Changelog", ""]
        for ver in sorted(by_ver.keys(), reverse=True):
            ves = by_ver[ver]; breaking = any(e.breaking for e in ves)
            lines.append(f"## v{ver}{' ⚠️ BREAKING' if breaking else ''}")
            for ct in ["added","changed","deprecated","removed","fixed","security"]:
                ct_es = [e for e in ves if e.change_type == ct]
                if ct_es:
                    lines.append(f"### {ct.upper()}")
                    for e in ct_es:
                        ep = f" (`{e.endpoint}`)" if e.endpoint else ""
                        lines.append(f"- {e.description}{ep}")
            lines.append("")
        return ActionResult(success=True, message=f"{len(entries)} entries", data={"changelog": "\n".join(lines), "versions": list(by_ver.keys())})
