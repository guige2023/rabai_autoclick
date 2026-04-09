"""Prompt library action module for RabAI AutoClick.

Provides LLM prompt management:
- PromptLibrary: Store and retrieve prompts
- PromptRenderer: Render prompts with variables
- PromptVersionManager: Version control for prompts
- PromptA/BTester: A/B test different prompts
- PromptTemplateEngine: Template-based prompt generation
"""

from __future__ import annotations

import json
import sys
import os
import hashlib
import re
from typing import Any, Callable, Dict, List, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PromptLibraryAction(BaseAction):
    """Store and retrieve prompts from a library."""
    action_type = "prompt_library"
    display_name = "Prompt库"
    description = "管理和存储Prompt模板"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "get")
            library_path = params.get("library_path", "/tmp/prompt_library")
            prompt_name = params.get("prompt_name", "")
            prompt_text = params.get("prompt_text", "")
            description = params.get("description", "")
            tags = params.get("tags", [])
            category = params.get("category", "general")

            if not os.path.exists(library_path):
                os.makedirs(library_path)

            if operation == "save":
                if not prompt_name or not prompt_text:
                    return ActionResult(success=False, message="prompt_name and prompt_text required")

                prompt_id = hashlib.md5(prompt_name.encode()).hexdigest()[:12]
                version = params.get("version", "1.0.0")

                entry = {
                    "id": prompt_id,
                    "name": prompt_name,
                    "text": prompt_text,
                    "description": description,
                    "tags": tags,
                    "category": category,
                    "version": version,
                    "created_at": datetime.now().isoformat(),
                    "updated_at": datetime.now().isoformat(),
                    "usage_count": 0,
                }

                prompt_file = os.path.join(library_path, f"{prompt_id}.json")
                with open(prompt_file, "w") as f:
                    json.dump(entry, f, indent=2)

                return ActionResult(
                    success=True,
                    message=f"Saved prompt: {prompt_name}",
                    data={"id": prompt_id, "name": prompt_name, "version": version}
                )

            elif operation == "get":
                if not prompt_name:
                    return ActionResult(success=False, message="prompt_name required")

                prompt_id = hashlib.md5(prompt_name.encode()).hexdigest()[:12]
                prompt_file = os.path.join(library_path, f"{prompt_id}.json")

                if not os.path.exists(prompt_file):
                    return ActionResult(success=False, message=f"Prompt not found: {prompt_name}")

                with open(prompt_file) as f:
                    entry = json.load(f)

                entry["usage_count"] = entry.get("usage_count", 0) + 1
                with open(prompt_file, "w") as f:
                    json.dump(entry, f, indent=2)

                return ActionResult(
                    success=True,
                    message=f"Retrieved: {prompt_name}",
                    data={"text": entry["text"], "description": entry.get("description", ""), "tags": entry.get("tags", [])}
                )

            elif operation == "list":
                prompts = []
                for filename in os.listdir(library_path):
                    if filename.endswith(".json"):
                        with open(os.path.join(library_path, filename)) as f:
                            entry = json.load(f)
                            prompts.append({
                                "id": entry.get("id"),
                                "name": entry.get("name"),
                                "category": entry.get("category"),
                                "tags": entry.get("tags", []),
                                "version": entry.get("version"),
                                "usage_count": entry.get("usage_count", 0),
                            })

                if category:
                    prompts = [p for p in prompts if p.get("category") == category]

                return ActionResult(success=True, message=f"{len(prompts)} prompts", data={"prompts": prompts, "count": len(prompts)})

            elif operation == "delete":
                if not prompt_name:
                    return ActionResult(success=False, message="prompt_name required")

                prompt_id = hashlib.md5(prompt_name.encode()).hexdigest()[:12]
                prompt_file = os.path.join(library_path, f"{prompt_id}.json")
                if os.path.exists(prompt_file):
                    os.remove(prompt_file)
                    return ActionResult(success=True, message=f"Deleted: {prompt_name}")
                return ActionResult(success=False, message=f"Not found: {prompt_name}")

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class PromptRendererAction(BaseAction):
    """Render prompts with variable substitution."""
    action_type = "prompt_renderer"
    display_name = "Prompt渲染"
    description = "渲染Prompt模板变量"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            template = params.get("template", "")
            variables = params.get("variables", {})
            strict = params.get("strict", False)

            if not template:
                return ActionResult(success=False, message="template is required")

            missing_vars = []
            rendered = template

            for var_name, var_value in variables.items():
                placeholder = "{{" + var_name + "}}"
                if placeholder in rendered:
                    rendered = rendered.replace(placeholder, str(var_value))
                else:
                    rendered = rendered.replace("{{ " + var_name + " }}", str(var_value))

            if strict:
                remaining = re.findall(r'\{\{(\w+)\}\}', rendered)
                remaining_alt = re.findall(r'\{\{ (\w+) \}\}', rendered)
                missing_vars = list(set(remaining + remaining_alt))

            remaining_placeholders = re.findall(r'\{\{.*?\}\}', rendered)
            if missing_vars:
                return ActionResult(
                    success=False,
                    message=f"Missing variables: {missing_vars}",
                    data={"rendered": rendered, "missing": missing_vars}
                )

            return ActionResult(
                success=True,
                message=f"Rendered prompt ({len(remaining_placeholders)} placeholders remaining)",
                data={"rendered": rendered, "remaining_placeholders": remaining_placeholders}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class PromptVersionManagerAction(BaseAction):
    """Version control for prompts."""
    action_type = "prompt_version_manager"
    display_name = "Prompt版本管理"
    description = "Prompt的版本控制"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "list")
            library_path = params.get("library_path", "/tmp/prompt_library")
            prompt_name = params.get("prompt_name", "")
            new_version = params.get("new_version", "")
            changelog = params.get("changelog", "")

            if not prompt_name:
                return ActionResult(success=False, message="prompt_name required")

            prompt_id = hashlib.md5(prompt_name.encode()).hexdigest()[:12]
            prompt_dir = os.path.join(library_path, prompt_id)
            os.makedirs(prompt_dir, exist_ok=True)

            if operation == "save_version":
                new_text = params.get("new_text", "")
                if not new_text or not new_version:
                    return ActionResult(success=False, message="new_text and new_version required")

                version_file = os.path.join(prompt_dir, f"v{new_version}.json")
                version_entry = {
                    "version": new_version,
                    "text": new_text,
                    "changelog": changelog,
                    "created_at": datetime.now().isoformat(),
                }

                with open(version_file, "w") as f:
                    json.dump(version_entry, f, indent=2)

                return ActionResult(success=True, message=f"Saved version: {new_version}")

            elif operation == "list_versions":
                versions = []
                for filename in os.listdir(prompt_dir):
                    if filename.startswith("v") and filename.endswith(".json"):
                        with open(os.path.join(prompt_dir, filename)) as f:
                            entry = json.load(f)
                            versions.append({
                                "version": entry.get("version"),
                                "changelog": entry.get("changelog", ""),
                                "created_at": entry.get("created_at"),
                            })

                versions.sort(key=lambda x: x["version"])
                return ActionResult(success=True, message=f"{len(versions)} versions", data={"versions": versions})

            elif operation == "get_version":
                if not new_version:
                    return ActionResult(success=False, message="new_version required")
                version_file = os.path.join(prompt_dir, f"v{new_version}.json")
                if not os.path.exists(version_file):
                    return ActionResult(success=False, message=f"Version not found: {new_version}")

                with open(version_file) as f:
                    entry = json.load(f)

                return ActionResult(success=True, message=f"Version: {new_version}", data=entry)

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class PromptABTesterAction(BaseAction):
    """A/B test different prompt variants."""
    action_type = "prompt_ab_tester"
    display_name = "Prompt A/B测试"
    description = "A/B测试不同Prompt变体"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "select")
            variant_a = params.get("variant_a", "")
            variant_b = params.get("variant_b", "")
            test_id = params.get("test_id", "default")
            results_path = params.get("results_path", "/tmp/prompt_tests")

            os.makedirs(results_path, exist_ok=True)
            results_file = os.path.join(results_path, f"{test_id}.json")

            if operation == "select":
                if not variant_a or not variant_b:
                    return ActionResult(success=False, message="variant_a and variant_b required")

                import random
                selected = random.choice([variant_a, variant_b])
                variant_key = "a" if selected == variant_a else "b"

                return ActionResult(
                    success=True,
                    message=f"Selected variant {variant_key}",
                    data={"selected": selected, "variant": variant_key, "test_id": test_id}
                )

            elif operation == "record":
                if not variant_a or not variant_b:
                    return ActionResult(success=False, message="variant_a and variant_b required")

                variant = params.get("variant", "")
                metric = params.get("metric", 0.0)
                metadata = params.get("metadata", {})

                results = {}
                if os.path.exists(results_file):
                    with open(results_file) as f:
                        results = json.load(f)

                if variant not in results:
                    results[variant] = {"metrics": [], "count": 0}

                results[variant]["metrics"].append(metric)
                results[variant]["count"] += 1
                results[variant]["metadata"] = metadata

                with open(results_file, "w") as f:
                    json.dump(results, f, indent=2)

                return ActionResult(success=True, message=f"Recorded metric for variant {variant}")

            elif operation == "get_results":
                if not os.path.exists(results_file):
                    return ActionResult(success=False, message=f"No results for test: {test_id}")

                with open(results_file) as f:
                    results = json.load(f)

                summary = {}
                for variant, data in results.items():
                    metrics = data.get("metrics", [])
                    if metrics:
                        summary[variant] = {
                            "count": data["count"],
                            "mean": sum(metrics) / len(metrics),
                            "min": min(metrics),
                            "max": max(metrics),
                        }

                winner = max(summary.items(), key=lambda x: x[1]["mean"])[0] if summary else None

                return ActionResult(
                    success=True,
                    message=f"Test results for {test_id}",
                    data={"summary": summary, "winner": winner, "test_id": test_id}
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class PromptTemplateEngineAction(BaseAction):
    """Template-based prompt generation with loops and conditionals."""
    action_type = "prompt_template_engine"
    display_name = "Prompt模板引擎"
    description = "支持循环和条件的Prompt模板引擎"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            template = params.get("template", "")
            data = params.get("data", {})

            if not template:
                return ActionResult(success=False, message="template is required")

            rendered = template

            for_loop_pattern = r'\{% for (\w+) in (\w+) %\}(.*?)\{% endfor %\}'
            for match in re.finditer(for_loop_pattern, rendered, re.DOTALL):
                item_name = match.group(1)
                list_name = match.group(2)
                loop_body = match.group(3)
                items = data.get(list_name, [])

                loop_results = []
                for item in items:
                    item_result = loop_body
                    if isinstance(item, dict):
                        for k, v in item.items():
                            item_result = item_result.replace(f"{{{{ {item_name}.{k} }}}}", str(v))
                    else:
                        item_result = item_result.replace(f"{{{{ {item_name} }}}}", str(item))
                    loop_results.append(item_result)

                rendered = rendered.replace(match.group(0), "\n".join(loop_results))

            conditional_pattern = r'\{% if (\w+) %\}(.*?)(?:\{% else %\}(.*?))?\{% endif %\}'
            for match in re.finditer(conditional_pattern, rendered, re.DOTALL):
                cond_var = match.group(1)
                if_true = match.group(2)
                if_false = match.group(3) or ""

                if data.get(cond_var):
                    rendered = rendered.replace(match, if_true)
                else:
                    rendered = rendered.replace(match, if_false)

            rendered = re.sub(r'\{\{ (\w+) \}\}', lambda m: str(data.get(m.group(1), "")), rendered)
            rendered = re.sub(r'\{\{(\w+)\}\}', lambda m: str(data.get(m.group(1), "")), rendered)

            return ActionResult(
                success=True,
                message="Template rendered",
                data={"rendered": rendered, "loops_resolved": len(re.findall(for_loop_pattern, template))}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
