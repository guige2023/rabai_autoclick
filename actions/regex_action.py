"""Regex operations action module for RabAI AutoClick.

Provides regex operations:
- RegexMatchAction: Match pattern
- RegexSearchAction: Search pattern
- RegexFindAllAction: Find all matches
- RegexReplaceAction: Replace pattern
- RegexSplitAction: Split by pattern
- RegexGroupsAction: Extract groups
"""

import re
from typing import Any, Dict, List, Optional, Tuple

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RegexMatchAction(BaseAction):
    """Match pattern at beginning."""
    action_type = "regex_match"
    display_name = "正则匹配"
    description = "匹配开头模式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            text = params.get("text", "")
            pattern = params.get("pattern", "")
            flags = params.get("flags", 0)

            if not text or not pattern:
                return ActionResult(success=False, message="text and pattern required")

            try:
                compiled = re.compile(pattern, flags)
            except re.error as e:
                return ActionResult(success=False, message=f"Invalid regex: {str(e)}")

            match = compiled.match(text)

            if match:
                return ActionResult(
                    success=True,
                    message="Pattern matched",
                    data={
                        "matched": True,
                        "match": match.group(),
                        "span": match.span(),
                        "groups": match.groups()
                    }
                )
            else:
                return ActionResult(success=True, message="No match", data={"matched": False})

        except Exception as e:
            return ActionResult(success=False, message=f"Match error: {str(e)}")


class RegexSearchAction(BaseAction):
    """Search for pattern."""
    action_type = "regex_search"
    display_name = "正则搜索"
    description = "搜索正则模式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            text = params.get("text", "")
            pattern = params.get("pattern", "")
            flags = params.get("flags", 0)

            if not text or not pattern:
                return ActionResult(success=False, message="text and pattern required")

            try:
                compiled = re.compile(pattern, flags)
            except re.error as e:
                return ActionResult(success=False, message=f"Invalid regex: {str(e)}")

            match = compiled.search(text)

            if match:
                return ActionResult(
                    success=True,
                    message="Pattern found",
                    data={
                        "found": True,
                        "match": match.group(),
                        "span": match.span(),
                        "position": match.start(),
                        "groups": match.groups()
                    }
                )
            else:
                return ActionResult(success=True, message="Pattern not found", data={"found": False})

        except Exception as e:
            return ActionResult(success=False, message=f"Search error: {str(e)}")


class RegexFindAllAction(BaseAction):
    """Find all matches."""
    action_type = "regex_findall"
    display_name = "正则查找全部"
    description = "查找所有匹配项"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            text = params.get("text", "")
            pattern = params.get("pattern", "")
            flags = params.get("flags", 0)
            max_count = params.get("max_count", 0)

            if not text or not pattern:
                return ActionResult(success=False, message="text and pattern required")

            try:
                compiled = re.compile(pattern, flags)
            except re.error as e:
                return ActionResult(success=False, message=f"Invalid regex: {str(e)}")

            if max_count > 0:
                matches = compiled.findall(text)[:max_count]
            else:
                matches = compiled.findall(text)

            results = []
            for match in compiled.finditer(text):
                results.append({
                    "match": match.group(),
                    "span": match.span(),
                    "position": match.start(),
                    "groups": match.groups()
                })
                if max_count > 0 and len(results) >= max_count:
                    break

            return ActionResult(
                success=True,
                message=f"Found {len(matches)} matches",
                data={"matches": results, "count": len(matches)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"FindAll error: {str(e)}")


class RegexReplaceAction(BaseAction):
    """Replace pattern."""
    action_type = "regex_replace"
    display_name = "正则替换"
    description = "替换正则模式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            text = params.get("text", "")
            pattern = params.get("pattern", "")
            replacement = params.get("replacement", "")
            flags = params.get("flags", 0)
            count = params.get("count", 0)

            if not text or not pattern:
                return ActionResult(success=False, message="text and pattern required")

            try:
                compiled = re.compile(pattern, flags)
            except re.error as e:
                return ActionResult(success=False, message=f"Invalid regex: {str(e)}")

            if count > 0:
                result = compiled.sub(replacement, text, count=count)
            else:
                result = compiled.sub(replacement, text)

            return ActionResult(
                success=True,
                message=f"Replaced pattern",
                data={"result": result, "original": text, "replacement": replacement}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Replace error: {str(e)}")


class RegexSplitAction(BaseAction):
    """Split by pattern."""
    action_type = "regex_split"
    display_name = "正则分割"
    description = "按正则分割"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            text = params.get("text", "")
            pattern = params.get("pattern", "")
            flags = params.get("flags", 0)
            max_split = params.get("max_split", 0)

            if not text or not pattern:
                return ActionResult(success=False, message="text and pattern required")

            try:
                compiled = re.compile(pattern, flags)
            except re.error as e:
                return ActionResult(success=False, message=f"Invalid regex: {str(e)}")

            if max_split > 0:
                parts = compiled.split(text, maxsplit=max_split)
            else:
                parts = compiled.split(text)

            return ActionResult(
                success=True,
                message=f"Split into {len(parts)} parts",
                data={"parts": parts, "count": len(parts)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Split error: {str(e)}")


class RegexGroupsAction(BaseAction):
    """Extract capture groups."""
    action_type = "regex_groups"
    display_name = "正则捕获组"
    description = "提取捕获组"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            text = params.get("text", "")
            pattern = params.get("pattern", "")
            flags = params.get("flags", 0)
            names = params.get("group_names", [])

            if not text or not pattern:
                return ActionResult(success=False, message="text and pattern required")

            try:
                compiled = re.compile(pattern, flags)
            except re.error as e:
                return ActionResult(success=False, message=f"Invalid regex: {str(e)}")

            matches = []
            for match in compiled.finditer(text):
                groups = match.groups()
                result = {"match": match.group(), "groups": list(groups)}

                if names:
                    named_groups = {}
                    for i, name in enumerate(names):
                        if i < len(groups):
                            named_groups[name] = groups[i]
                    result["named_groups"] = named_groups

                matches.append(result)

            return ActionResult(
                success=True,
                message=f"Extracted {len(matches)} matches with groups",
                data={"matches": matches, "count": len(matches)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Groups error: {str(e)}")
