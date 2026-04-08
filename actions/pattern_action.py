"""Pattern action module for RabAI AutoClick.

Provides pattern matching and regular expression actions for
text extraction, validation, and transformation.
"""

import re
import time
import sys
import os
import hashlib
from typing import Any, Dict, List, Optional, Union, Callable, Pattern
from dataclasses import dataclass
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class MatchResult:
    """Result of a pattern match operation.
    
    Attributes:
        matched: Whether the pattern matched.
        groups: Captured groups.
        group_dict: Named captured groups.
        start: Start position of match.
        end: End position of match.
    """
    matched: bool
    groups: tuple
    group_dict: Dict[str, str]
    start: int
    end: int


class PatternMatcher:
    """Thread-safe pattern matching engine with caching."""
    
    def __init__(self):
        self._cache: Dict[str, Pattern] = {}
        self._lock = type(threading.Lock())()
        self._match_history: List[Dict[str, Any]] = []
        self._max_history = 1000
    
    def _get_pattern(self, pattern: str, flags: int = 0) -> Pattern:
        """Get compiled pattern from cache.
        
        Args:
            pattern: Regular expression pattern.
            flags: Regex flags.
        
        Returns:
            Compiled pattern.
        """
        cache_key = f"{pattern}:{flags}"
        
        with self._lock:
            if cache_key not in self._cache:
                self._cache[cache_key] = re.compile(pattern, flags)
            return self._cache[cache_key]
    
    def match(self, pattern: str, text: str, flags: int = 0) -> MatchResult:
        """Match pattern at the start of text.
        
        Args:
            pattern: Regular expression.
            text: Text to search.
            flags: Regex flags.
        
        Returns:
            MatchResult with match information.
        """
        compiled = self._get_pattern(pattern, flags)
        match = compiled.match(text)
        
        if match is None:
            return MatchResult(matched=False, groups=(), group_dict={}, start=-1, end=-1)
        
        return MatchResult(
            matched=True,
            groups=match.groups(),
            group_dict=match.groupdict(),
            start=match.start(),
            end=match.end()
        )
    
    def search(self, pattern: str, text: str, flags: int = 0) -> MatchResult:
        """Search for pattern anywhere in text.
        
        Args:
            pattern: Regular expression.
            text: Text to search.
            flags: Regex flags.
        
        Returns:
            MatchResult with first match.
        """
        compiled = self._get_pattern(pattern, flags)
        match = compiled.search(text)
        
        if match is None:
            return MatchResult(matched=False, groups=(), group_dict={}, start=-1, end=-1)
        
        return MatchResult(
            matched=True,
            groups=match.groups(),
            group_dict=match.groupdict(),
            start=match.start(),
            end=match.end()
        )
    
    def findall(self, pattern: str, text: str, flags: int = 0) -> List[Dict[str, Any]]:
        """Find all matches of pattern.
        
        Args:
            pattern: Regular expression.
            text: Text to search.
            flags: Regex flags.
        
        Returns:
            List of match dictionaries.
        """
        compiled = self._get_pattern(pattern, flags)
        matches = compiled.finditer(text)
        
        results = []
        for match in matches:
            results.append({
                "matched": True,
                "groups": match.groups(),
                "group_dict": match.groupdict(),
                "start": match.start(),
                "end": match.end(),
                "text": match.group()
            })
        
        return results
    
    def replace(self, pattern: str, text: str, replacement: str, count: int = 0, flags: int = 0) -> str:
        """Replace pattern matches.
        
        Args:
            pattern: Regular expression.
            text: Text to search.
            replacement: Replacement string.
            count: Max replacements (0 = all).
            flags: Regex flags.
        
        Returns:
            Text with replacements.
        """
        compiled = self._get_pattern(pattern, flags)
        return compiled.sub(replacement, text, count=count)
    
    def split(self, pattern: str, text: str, maxsplit: int = 0, flags: int = 0) -> List[str]:
        """Split text by pattern.
        
        Args:
            pattern: Regular expression.
            text: Text to split.
            maxsplit: Max splits (0 = unlimited).
            flags: Regex flags.
        
        Returns:
            List of text parts.
        """
        compiled = self._get_pattern(pattern, flags)
        return compiled.split(text, maxsplit=maxsplit)
    
    def validate(self, pattern: str, text: str, flags: int = 0) -> bool:
        """Check if pattern matches entire text.
        
        Args:
            pattern: Regular expression.
            text: Text to validate.
            flags: Regex flags.
        
        Returns:
            True if entire text matches.
        """
        compiled = self._get_pattern(pattern, flags)
        return compiled.fullmatch(text) is not None
    
    def extract(self, pattern: str, text: str, group: Union[int, str] = 0, flags: int = 0) -> Optional[str]:
        """Extract a captured group.
        
        Args:
            pattern: Regular expression.
            text: Text to search.
            group: Group index or name.
            flags: Regex flags.
        
        Returns:
            Extracted group or None.
        """
        result = self.search(pattern, text, flags)
        
        if not result.matched:
            return None
        
        if isinstance(group, int):
            if group < len(result.groups):
                return result.groups[group]
        elif isinstance(group, str):
            return result.group_dict.get(group)
        
        return None


# Global pattern matcher
_matcher = PatternMatcher()


class PatternMatchAction(BaseAction):
    """Match a pattern at the start of text."""
    action_type = "pattern_match"
    display_name = "正则匹配"
    description = "在文本开头匹配正则"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Match pattern.
        
        Args:
            context: Execution context.
            params: Dict with keys: pattern, text, flags.
        
        Returns:
            ActionResult with match information.
        """
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        flags = params.get('flags', 0)
        
        if not pattern:
            return ActionResult(success=False, message="pattern is required")
        
        if text is None:
            return ActionResult(success=False, message="text is required")
        
        try:
            result = _matcher.match(pattern, text, flags)
            
            return ActionResult(
                success=True,
                message=f"Pattern {'matched' if result.matched else 'did not match'}",
                data={
                    "matched": result.matched,
                    "groups": result.groups,
                    "group_dict": result.group_dict,
                    "start": result.start,
                    "end": result.end,
                    "text": text[:100] if text else ""
                }
            )
        except re.error as e:
            return ActionResult(success=False, message=f"Invalid regex: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"Match failed: {str(e)}")


class PatternSearchAction(BaseAction):
    """Search for pattern anywhere in text."""
    action_type = "pattern_search"
    display_name = "正则搜索"
    description = "在文本中搜索正则"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Search for pattern.
        
        Args:
            context: Execution context.
            params: Dict with keys: pattern, text, flags.
        
        Returns:
            ActionResult with first match.
        """
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        flags = params.get('flags', 0)
        
        if not pattern:
            return ActionResult(success=False, message="pattern is required")
        
        if text is None:
            return ActionResult(success=False, message="text is required")
        
        try:
            result = _matcher.search(pattern, text, flags)
            
            return ActionResult(
                success=True,
                message=f"Pattern {'found' if result.matched else 'not found'}",
                data={
                    "matched": result.matched,
                    "groups": result.groups,
                    "group_dict": result.group_dict,
                    "start": result.start,
                    "end": result.end,
                    "matched_text": text[result.start:result.end] if result.matched else None
                }
            )
        except re.error as e:
            return ActionResult(success=False, message=f"Invalid regex: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"Search failed: {str(e)}")


class PatternFindAllAction(BaseAction):
    """Find all pattern matches."""
    action_type = "pattern_findall"
    display_name = "正则全搜索"
    description = "查找所有匹配项"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Find all matches.
        
        Args:
            context: Execution context.
            params: Dict with keys: pattern, text, flags, limit.
        
        Returns:
            ActionResult with all matches.
        """
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        flags = params.get('flags', 0)
        limit = params.get('limit', 1000)
        
        if not pattern:
            return ActionResult(success=False, message="pattern is required")
        
        if text is None:
            return ActionResult(success=False, message="text is required")
        
        try:
            results = _matcher.findall(pattern, text, flags)[:limit]
            
            return ActionResult(
                success=True,
                message=f"Found {len(results)} matches",
                data={
                    "matches": results,
                    "count": len(results),
                    "has_more": len(results) >= limit
                }
            )
        except re.error as e:
            return ActionResult(success=False, message=f"Invalid regex: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"FindAll failed: {str(e)}")


class PatternReplaceAction(BaseAction):
    """Replace pattern matches."""
    action_type = "pattern_replace"
    display_name = "正则替换"
    description = "替换匹配的文本"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Replace matches.
        
        Args:
            context: Execution context.
            params: Dict with keys: pattern, text, replacement, count, flags.
        
        Returns:
            ActionResult with replaced text.
        """
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        replacement = params.get('replacement', '')
        count = params.get('count', 0)
        flags = params.get('flags', 0)
        
        if not pattern:
            return ActionResult(success=False, message="pattern is required")
        
        if text is None:
            return ActionResult(success=False, message="text is required")
        
        try:
            result = _matcher.replace(pattern, text, replacement, count, flags)
            
            return ActionResult(
                success=True,
                message="Replacement completed",
                data={
                    "result": result,
                    "original": text,
                    "replacement": replacement
                }
            )
        except re.error as e:
            return ActionResult(success=False, message=f"Invalid regex: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"Replace failed: {str(e)}")


class PatternSplitAction(BaseAction):
    """Split text by pattern."""
    action_type = "pattern_split"
    display_name = "正则分割"
    description = "按正则分割文本"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Split by pattern.
        
        Args:
            context: Execution context.
            params: Dict with keys: pattern, text, maxsplit, flags.
        
        Returns:
            ActionResult with split parts.
        """
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        maxsplit = params.get('maxsplit', 0)
        flags = params.get('flags', 0)
        
        if not pattern:
            return ActionResult(success=False, message="pattern is required")
        
        if text is None:
            return ActionResult(success=False, message="text is required")
        
        try:
            parts = _matcher.split(pattern, text, maxsplit, flags)
            
            return ActionResult(
                success=True,
                message=f"Split into {len(parts)} parts",
                data={
                    "parts": parts,
                    "count": len(parts)
                }
            )
        except re.error as e:
            return ActionResult(success=False, message=f"Invalid regex: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"Split failed: {str(e)}")


class PatternValidateAction(BaseAction):
    """Validate text against a pattern."""
    action_type = "pattern_validate"
    display_name = "正则验证"
    description = "验证文本格式"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Validate text.
        
        Args:
            context: Execution context.
            params: Dict with keys: pattern, text, flags.
        
        Returns:
            ActionResult with validation result.
        """
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        flags = params.get('flags', 0)
        
        if not pattern:
            return ActionResult(success=False, message="pattern is required")
        
        if text is None:
            return ActionResult(success=False, message="text is required")
        
        try:
            valid = _matcher.validate(pattern, text, flags)
            
            return ActionResult(
                success=True,
                message=f"Text is {'valid' if valid else 'invalid'}",
                data={
                    "valid": valid,
                    "pattern": pattern,
                    "text": text
                }
            )
        except re.error as e:
            return ActionResult(success=False, message=f"Invalid regex: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"Validate failed: {str(e)}")


class PatternExtractAction(BaseAction):
    """Extract captured group from pattern."""
    action_type = "pattern_extract"
    display_name = "正则提取"
    description = "提取分组内容"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Extract group.
        
        Args:
            context: Execution context.
            params: Dict with keys: pattern, text, group, flags.
        
        Returns:
            ActionResult with extracted value.
        """
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        group = params.get('group', 0)
        flags = params.get('flags', 0)
        
        if not pattern:
            return ActionResult(success=False, message="pattern is required")
        
        if text is None:
            return ActionResult(success=False, message="text is required")
        
        try:
            extracted = _matcher.extract(pattern, text, group, flags)
            
            return ActionResult(
                success=True,
                message=f"Extracted: {extracted}" if extracted else "No match found",
                data={
                    "extracted": extracted,
                    "group": group,
                    "pattern": pattern
                }
            )
        except re.error as e:
            return ActionResult(success=False, message=f"Invalid regex: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"Extract failed: {str(e)}")
