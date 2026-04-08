"""Data Loader Action Module. Loads data from various sources."""
import sys, os, json
from typing import Any
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

class DataLoaderAction(BaseAction):
    action_type = "data_loader"; display_name = "数据加载"
    description = "从多来源加载数据"
    def __init__(self) -> None: super().__init__()
    def _load_file(self, path: str):
        ext = os.path.splitext(path)[1].lower()
        with open(path, "r", encoding="utf-8", errors="replace") as f: content = f.read()
        if ext == ".json": return content, json.loads(content)
        elif ext == ".csv":
            import csv as csv_mod
            return content, list(csv_mod.DictReader(content.splitlines()))
        return content, content
    def _load_url(self, url: str):
        import urllib.request
        with urllib.request.urlopen(url, timeout=30) as response:
            content = response.read()
            try: data = json.loads(content)
            except: data = content.decode(errors="replace")
            return data, dict(response.headers)
    def execute(self, context: Any, params: dict) -> ActionResult:
        source = params.get("source"); source_type = params.get("source_type","auto")
        if not source: return ActionResult(success=False, message="No source")
        try:
            if source_type == "url" or (source_type == "auto" and str(source).startswith(("http://","https://"))):
                data, headers = self._load_url(source)
                return ActionResult(success=True, message=f"Loaded from URL: {len(str(data))} chars",
                                  data={"data": data, "source": "url", "headers": headers})
            if source_type == "file" or source_type == "auto":
                if os.path.isfile(str(source)):
                    content, parsed = self._load_file(str(source))
                    return ActionResult(success=True, message=f"Loaded file: {os.path.basename(str(source))}",
                                      data={"data": parsed, "source": "file", "path": str(source)})
            if source_type == "inline":
                try: parsed = json.loads(source)
                except: parsed = source
                return ActionResult(success=True, message="Loaded inline data", data={"data": parsed, "source": "inline"})
            return ActionResult(success=False, message=f"Source not found: {source}")
        except Exception as e: return ActionResult(success=False, message=f"Load error: {e}")
