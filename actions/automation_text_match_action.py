"""Automation Text Match Action Module for RabAI AutoClick.

Text pattern matching for UI element detection using
regex, fuzzy matching, and OCR-based text extraction.
"""

import time
import re
import sys
import os
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class MatchStrategy:
    """Text matching strategies."""
    EXACT = "exact"
    CONTAINS = "contains"
    REGEX = "regex"
    FUZZY = "fuzzy"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"


class AutomationTextMatchAction(BaseAction):
    """Text-based UI element matching.

    Finds UI elements by text content using exact, fuzzy,
    regex, and substring matching strategies. Supports
    case sensitivity options and match position reporting.
    """
    action_type = "automation_text_match"
    display_name = "文本匹配自动化"
    description = "基于文本的UI元素匹配，支持模糊和正则"

    _match_cache: Dict[str, List[Dict[str, Any]]] = {}

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute text match operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'match', 'find_all', 'match_many',
                               'replace', 'extract'
                - pattern: str - text pattern to match
                - text: str (optional) - text to search in
                - source: str (optional) - text source type ('clipboard', 'screenshot')
                - strategy: str (optional) - matching strategy
                - case_sensitive: bool (optional) - case sensitivity
                - threshold: float (optional) - fuzzy match threshold
                - region: tuple (optional) - (x, y, w, h) screen region

        Returns:
            ActionResult with match result.
        """
        start_time = time.time()

        try:
            operation = params.get('operation', 'match')

            if operation == 'match':
                return self._match_text(params, start_time)
            elif operation == 'find_all':
                return self._find_all_matches(params, start_time)
            elif operation == 'match_many':
                return self._match_many_patterns(params, start_time)
            elif operation == 'replace':
                return self._replace_text(params, start_time)
            elif operation == 'extract':
                return self._extract_text(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Text match action failed: {str(e)}",
                data={'error': str(e)},
                duration=time.time() - start_time
            )

    def _match_text(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Find first match of pattern in text."""
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        strategy = params.get('strategy', MatchStrategy.CONTAINS)
        case_sensitive = params.get('case_sensitive', False)
        threshold = params.get('threshold', 0.8)

        if not pattern:
            return ActionResult(
                success=False,
                message="pattern is required",
                duration=time.time() - start_time
            )

        if not text:
            text = self._get_text_from_source(params)

        if not text:
            return ActionResult(
                success=False,
                message="No text to search",
                duration=time.time() - start_time
            )

        match_result = self._find_match(
            pattern, text, strategy, case_sensitive, threshold
        )

        if match_result:
            return ActionResult(
                success=True,
                message=f"Text matched: {pattern}",
                data={
                    'matched': True,
                    'pattern': pattern,
                    'match': match_result['match'],
                    'position': match_result['position'],
                    'strategy': strategy
                },
                duration=time.time() - start_time
            )
        else:
            return ActionResult(
                success=False,
                message=f"Pattern not found: {pattern}",
                data={
                    'matched': False,
                    'pattern': pattern,
                    'strategy': strategy
                },
                duration=time.time() - start_time
            )

    def _find_all_matches(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Find all matches of pattern in text."""
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        strategy = params.get('strategy', MatchStrategy.CONTAINS)
        case_sensitive = params.get('case_sensitive', False)
        max_matches = params.get('max_matches', 100)

        if not pattern:
            return ActionResult(
                success=False,
                message="pattern is required",
                duration=time.time() - start_time
            )

        if not text:
            text = self._get_text_from_source(params)

        if not text:
            return ActionResult(
                success=False,
                message="No text to search",
                duration=time.time() - start_time
            )

        matches = self._find_all(
            pattern, text, strategy, case_sensitive, max_matches
        )

        return ActionResult(
            success=True,
            message=f"Found {len(matches)} matches",
            data={
                'pattern': pattern,
                'matches': matches,
                'count': len(matches),
                'strategy': strategy
            },
            duration=time.time() - start_time
        )

    def _match_many_patterns(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Match multiple patterns against text."""
        patterns = params.get('patterns', [])
        text = params.get('text', '')
        case_sensitive = params.get('case_sensitive', False)

        if not text:
            text = self._get_text_from_source(params)

        if not patterns:
            return ActionResult(
                success=False,
                message="patterns list is required",
                duration=time.time() - start_time
            )

        results = []
        for pattern in patterns:
            match_result = self._find_match(
                pattern, text, MatchStrategy.CONTAINS, case_sensitive
            )
            results.append({
                'pattern': pattern,
                'matched': match_result is not None,
                'match': match_result['match'] if match_result else None
            })

        matched_count = sum(1 for r in results if r['matched'])

        return ActionResult(
            success=True,
            message=f"{matched_count}/{len(patterns)} patterns matched",
            data={
                'results': results,
                'matched_count': matched_count,
                'total_patterns': len(patterns)
            },
            duration=time.time() - start_time
        )

    def _replace_text(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Replace pattern occurrences in text."""
        pattern = params.get('pattern', '')
        replacement = params.get('replacement', '')
        text = params.get('text', '')
        regex = params.get('regex', False)

        if not pattern or not text:
            return ActionResult(
                success=False,
                message="pattern and text are required",
                duration=time.time() - start_time
            )

        count = 0
        if regex:
            count = len(re.findall(pattern, text))
            new_text = re.sub(pattern, replacement, text)
        else:
            if not params.get('case_sensitive', False):
                pattern_lower = pattern.lower()
                text_lower = text.lower()
                count = text_lower.count(pattern_lower)
                new_text = text.replace(pattern, replacement)
            else:
                count = text.count(pattern)
                new_text = text.replace(pattern, replacement)

        return ActionResult(
            success=True,
            message=f"Replaced {count} occurrences",
            data={
                'original': text,
                'result': new_text,
                'replacements': count
            },
            duration=time.time() - start_time
        )

    def _extract_text(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Extract text using regex groups."""
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        regex = params.get('regex', True)

        if not pattern or not text:
            return ActionResult(
                success=False,
                message="pattern and text are required",
                duration=time.time() - start_time
            )

        if not regex:
            return ActionResult(
                success=False,
                message="Extract requires regex=True",
                duration=time.time() - start_time
            )

        try:
            matches = re.findall(pattern, text)
            groups = [m for m in matches if m]
            return ActionResult(
                success=True,
                message=f"Extracted {len(groups)} matches",
                data={
                    'extracted': groups,
                    'count': len(groups)
                },
                duration=time.time() - start_time
            )
        except re.error as e:
            return ActionResult(
                success=False,
                message=f"Regex error: {e}",
                duration=time.time() - start_time
            )

    def _find_match(
        self,
        pattern: str,
        text: str,
        strategy: str,
        case_sensitive: bool,
        threshold: float = 0.8
    ) -> Optional[Dict[str, Any]]:
        """Find first match using specified strategy."""
        if strategy == MatchStrategy.EXACT:
            if case_sensitive:
                if pattern in text:
                    pos = text.index(pattern)
                    return {'match': pattern, 'position': pos}
            else:
                lower_text = text.lower()
                lower_pattern = pattern.lower()
                if lower_pattern in lower_text:
                    pos = lower_text.index(lower_pattern)
                    return {'match': text[pos:pos+len(pattern)], 'position': pos}

        elif strategy == MatchStrategy.CONTAINS:
            if case_sensitive:
                if pattern in text:
                    pos = text.index(pattern)
                    return {'match': pattern, 'position': pos}
            else:
                lower_text = text.lower()
                lower_pattern = pattern.lower()
                if lower_pattern in lower_text:
                    pos = lower_text.index(lower_pattern)
                    return {'match': text[pos:pos+len(pattern)], 'position': pos}

        elif strategy == MatchStrategy.REGEX:
            try:
                flags = 0 if case_sensitive else re.IGNORECASE
                match = re.search(pattern, text, flags)
                if match:
                    return {'match': match.group(), 'position': match.start()}
            except re.error:
                pass

        elif strategy == MatchStrategy.STARTS_WITH:
            if case_sensitive:
                if text.startswith(pattern):
                    return {'match': pattern, 'position': 0}
            elif text.lower().startswith(pattern.lower()):
                return {'match': text[:len(pattern)], 'position': 0}

        elif strategy == MatchStrategy.ENDS_WITH:
            if case_sensitive:
                if text.endswith(pattern):
                    return {'match': pattern, 'position': len(text) - len(pattern)}
            elif text.lower().endswith(pattern.lower()):
                return {'match': text[-len(pattern):], 'position': len(text) - len(pattern)}

        return None

    def _find_all(
        self,
        pattern: str,
        text: str,
        strategy: str,
        case_sensitive: bool,
        max_matches: int
    ) -> List[Dict[str, Any]]:
        """Find all matches using specified strategy."""
        matches = []

        if strategy == MatchStrategy.CONTAINS:
            if case_sensitive:
                search_text = text
                search_pattern = pattern
            else:
                search_text = text.lower()
                search_pattern = pattern.lower()

            pos = 0
            while pos < len(search_text) and len(matches) < max_matches:
                idx = search_text.find(search_pattern, pos)
                if idx == -1:
                    break
                matches.append({
                    'match': text[idx:idx+len(pattern)],
                    'position': idx
                })
                pos = idx + 1

        elif strategy == MatchStrategy.REGEX:
            try:
                flags = 0 if case_sensitive else re.IGNORECASE
                for match in re.finditer(pattern, text, flags):
                    if len(matches) >= max_matches:
                        break
                    matches.append({
                        'match': match.group(),
                        'position': match.start()
                    })
            except re.error:
                pass

        return matches

    def _get_text_from_source(self, params: Dict[str, Any]) -> str:
        """Get text from specified source."""
        source = params.get('source', 'clipboard')

        if source == 'clipboard':
            try:
                import subprocess
                result = subprocess.run(
                    ['pbpaste'],
                    capture_output=True, timeout=2
                )
                if result.returncode == 0:
                    return result.stdout.decode('utf-8')
            except Exception:
                pass

        return params.get('text', '')
