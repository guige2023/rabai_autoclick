"""Data regex action module for RabAI AutoClick.

Provides regex-based pattern matching, extraction, and transformation
for text processing and data extraction tasks.
"""

import re
from typing import Any, Dict, List, Optional, Union, Pattern

from core.base_action import BaseAction, ActionResult


class RegexMatchAction(BaseAction):
    """Match regex patterns against text with various modes.
    
    Supports finding all matches, single match, test mode,
    and capture group extraction.
    """
    action_type = "data_regex_match"
    display_name = "正则匹配"
    description = "正则表达式模式匹配和提取"
    VALID_MODES = ["findall", "search", "match", "test", "split"]
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute regex operation.
        
        Args:
            context: Execution context.
            params: Dict with keys: pattern, text, mode, flags, group_index.
        
        Returns:
            ActionResult with match results.
        """
        pattern = params.get("pattern", "")
        text = params.get("text", "")
        mode = params.get("mode", "findall")
        flags = params.get("flags", 0)
        group_index = params.get("group_index")
        
        if not pattern:
            return ActionResult(success=False, message="Pattern is required")
        
        valid, msg = self.validate_in(mode, self.VALID_MODES, "mode")
        if not valid:
            return ActionResult(success=False, message=msg)
        
        try:
            if isinstance(flags, int):
                compiled = re.compile(pattern, flags)
            else:
                flag_val = 0
                if "i" in flags or "ignore" in flags:
                    flag_val |= re.IGNORECASE
                if "m" in flags or "multi" in flags:
                    flag_val |= re.MULTILINE
                if "s" in flags or "dotall" in flags:
                    flag_val |= re.DOTALL
                compiled = re.compile(pattern, flag_val)
            
            if mode == "findall":
                matches = compiled.findall(text)
                return ActionResult(
                    success=len(matches) > 0,
                    message=f"Found {len(matches)} matches",
                    data={
                        "matches": matches,
                        "count": len(matches)
                    }
                )
            elif mode == "search":
                match = compiled.search(text)
                if match:
                    groups = match.groups()
                    result = {
                        "match": match.group(0),
                        "start": match.start(),
                        "end": match.end(),
                        "groups": groups
                    }
                    if group_index is not None and groups:
                        result["group"] = groups[int(group_index)] if int(group_index) < len(groups) else None
                    return ActionResult(
                        success=True,
                        message=f"Found match at {match.start()}-{match.end()}",
                        data=result
                    )
                return ActionResult(success=False, message="No match found")
            elif mode == "match":
                match = compiled.match(text)
                if match:
                    return ActionResult(
                        success=True,
                        message=f"Matched at start: {match.group(0)}",
                        data={
                            "match": match.group(0),
                            "groups": match.groups()
                        }
                    )
                return ActionResult(success=False, message="No match at start")
            elif mode == "test":
                is_match = bool(compiled.search(text))
                return ActionResult(
                    success=is_match,
                    message="Pattern matches" if is_match else "No match",
                    data={"matches": is_match}
                )
            elif mode == "split":
                parts = compiled.split(text)
                return ActionResult(
                    success=True,
                    message=f"Split into {len(parts)} parts",
                    data={
                        "parts": parts,
                        "count": len(parts)
                    }
                )
        except re.error as e:
            return ActionResult(success=False, message=f"Regex error: {e}")
        except Exception as e:
            return ActionResult(success=False, message=f"Regex operation failed: {e}")


class RegexReplaceAction(BaseAction):
    """Replace regex pattern matches with substitution text.
    
    Supports backreferences, conditional replacement,
    and case-preserving transformations.
    """
    action_type = "data_regex_replace"
    display_name = "正则替换"
    description = "正则表达式模式替换"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Replace regex matches.
        
        Args:
            context: Execution context.
            params: Dict with keys: pattern, text, replacement, count,
                   flags, backref_template.
        
        Returns:
            ActionResult with replacement result.
        """
        pattern = params.get("pattern", "")
        text = params.get("text", "")
        replacement = params.get("replacement", "")
        count = params.get("count", 0)
        flags = params.get("flags", 0)
        
        if not pattern:
            return ActionResult(success=False, message="Pattern is required")
        
        try:
            if isinstance(flags, int):
                compiled = re.compile(pattern, flags)
            else:
                flag_val = 0
                if "i" in flags:
                    flag_val |= re.IGNORECASE
                if "m" in flags:
                    flag_val |= re.MULTILINE
                if "s" in flags:
                    flag_val |= re.DOTALL
                compiled = re.compile(pattern, flag_val)
            
            if count > 0:
                result, n = compiled.subn(replacement, text, count=count)
            else:
                result = compiled.sub(replacement, text)
                n = len(compiled.findall(text))
            
            return ActionResult(
                success=True,
                message=f"Replaced {n} matches",
                data={
                    "result": result,
                    "replacements": n,
                    "original_length": len(text),
                    "result_length": len(result)
                }
            )
        except re.error as e:
            return ActionResult(success=False, message=f"Regex error: {e}")
        except Exception as e:
            return ActionResult(success=False, message=f"Regex replace failed: {e}")


class RegexExtractAction(BaseAction):
    """Extract structured data from text using regex patterns.
    
    Supports named capture groups, multi-pattern extraction,
    and building structured records from unstructured text.
    """
    action_type = "data_regex_extract"
    display_name = "正则提取"
    description = "使用正则表达式提取结构化数据"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Extract structured data via regex.
        
        Args:
            context: Execution context.
            params: Dict with keys: pattern, text, flags, as_records.
        
        Returns:
            ActionResult with extracted records.
        """
        pattern = params.get("pattern", "")
        text = params.get("text", "")
        flags = params.get("flags", 0)
        as_records = params.get("as_records", True)
        
        if not pattern:
            return ActionResult(success=False, message="Pattern is required")
        
        try:
            flag_val = 0
            if isinstance(flags, str):
                if "i" in flags:
                    flag_val |= re.IGNORECASE
                if "m" in flags:
                    flag_val |= re.MULTILINE
                if "s" in flags:
                    flag_val |= re.DOTALL
            elif isinstance(flags, int):
                flag_val = flags
            
            compiled = re.compile(pattern, flag_val)
            matches = compiled.finditer(text)
            
            records = []
            for match in matches:
                if as_records:
                    record = {"match": match.group(0), "start": match.start(), "end": match.end()}
                    if match.groupdict():
                        record.update(match.groupdict())
                    elif match.groups():
                        for i, g in enumerate(match.groups()):
                            record[f"group_{i+1}"] = g
                    records.append(record)
                else:
                    records.append(match.group(0))
            
            return ActionResult(
                success=len(records) > 0,
                message=f"Extracted {len(records)} records",
                data={
                    "records": records,
                    "count": len(records)
                }
            )
        except re.error as e:
            return ActionResult(success=False, message=f"Regex error: {e}")
        except Exception as e:
            return ActionResult(success=False, message=f"Regex extract failed: {e}")
