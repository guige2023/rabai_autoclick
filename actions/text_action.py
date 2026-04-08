"""Text action module for RabAI AutoClick.

Provides text manipulation actions including regex, formatting, and parsing.
"""

import re
import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class TextReplaceAction(BaseAction):
    """Replace text patterns with replacement strings.
    
    Supports literal replacement and regex-based replacement.
    """
    action_type = "text_replace"
    display_name = "文本替换"
    description = "替换文本中的字符串"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Replace text.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: text, pattern, replacement, 
                   use_regex, case_sensitive, count.
        
        Returns:
            ActionResult with replaced text.
        """
        text = params.get('text', '')
        pattern = params.get('pattern', '')
        replacement = params.get('replacement', '')
        use_regex = params.get('use_regex', False)
        case_sensitive = params.get('case_sensitive', True)
        count = params.get('count', 0)  # 0 = all
        
        if not text:
            return ActionResult(success=False, message="text is required")
        
        if not pattern:
            return ActionResult(success=False, message="pattern is required")
        
        try:
            if use_regex:
                flags = 0 if case_sensitive else re.IGNORECASE
                compiled = re.compile(pattern, flags)
                result, n = compiled.subn(replacement, text, count=count)
                matches = n
            else:
                if case_sensitive:
                    result = text.replace(pattern, replacement, count)
                else:
                    # Case-insensitive literal replace
                    pattern_lower = pattern.lower()
                    text_lower = text.lower()
                    if count > 0:
                        # Need to do this manually for count-limited case-insensitive
                        matches = text_lower.count(pattern_lower)
                        if count > 0:
                            matches = min(matches, count)
                        result = text
                        for _ in range(matches):
                            idx = result.lower().find(pattern_lower)
                            if idx >= 0:
                                result = result[:idx] + replacement + result[idx + len(pattern):]
                    else:
                        matches = text_lower.count(pattern_lower)
                        result = text.lower().replace(pattern_lower, replacement)
                        # Preserve original case in result by reconstructing
                        result = text
                        start = 0
                        while True:
                            idx = result.lower().find(pattern_lower, start)
                            if idx == -1:
                                break
                            result = result[:idx] + replacement + result[idx + len(pattern):]
                            start = idx + len(replacement)
            
            return ActionResult(
                success=True,
                message=f"Replaced {matches} occurrence(s)",
                data={'text': result, 'matches': matches}
            )
            
        except re.error as e:
            return ActionResult(
                success=False,
                message=f"Regex error: {e}",
                data={'error': str(e)}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Replace error: {e}",
                data={'error': str(e)}
            )


class TextSplitJoinAction(BaseAction):
    """Split text by delimiter and rejoin with separator.
    
    Useful for parsing CSV, lists, and text restructuring.
    """
    action_type = "text_split_join"
    display_name = "文本分割合并"
    description = "分割并重新合并文本"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Split and join text.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: text, delimiter, separator, 
                   max_split, include_index.
        
        Returns:
            ActionResult with split/joined result.
        """
        text = params.get('text', '')
        delimiter = params.get('delimiter', ',')
        separator = params.get('separator', '\n')
        max_split = params.get('max_split', 0)
        include_index = params.get('include_index', False)
        
        if not text:
            return ActionResult(success=False, message="text is required")
        
        try:
            # Split
            if max_split > 0:
                parts = text.split(delimiter, max_split)
            else:
                parts = text.split(delimiter)
            
            # Optionally include index
            if include_index:
                parts = [f"[{i}]{p}" for i, p in enumerate(parts)]
            
            # Join
            result = separator.join(parts)
            
            return ActionResult(
                success=True,
                message=f"Split into {len(parts)} parts, joined with '{separator}'",
                data={'text': result, 'parts': parts, 'count': len(parts)}
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Split/Join error: {e}",
                data={'error': str(e)}
            )


class TextRegexExtractAction(BaseAction):
    """Extract text using regex patterns.
    
    Supports finding matches, groups, and named groups.
    """
    action_type = "text_regex_extract"
    display_name = "正则提取"
    description = "使用正则表达式提取文本"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Extract text using regex.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: text, pattern, mode (findall/finditer/match/search),
                   group, case_sensitive, as_list.
        
        Returns:
            ActionResult with extracted text.
        """
        text = params.get('text', '')
        pattern = params.get('pattern', '')
        mode = params.get('mode', 'findall')
        group = params.get('group', 0)
        case_sensitive = params.get('case_sensitive', True)
        as_list = params.get('as_list', True)
        
        if not text:
            return ActionResult(success=False, message="text is required")
        
        if not pattern:
            return ActionResult(success=False, message="pattern is required")
        
        flags = 0 if case_sensitive else re.IGNORECASE
        
        try:
            regex = re.compile(pattern, flags)
            
            if mode == 'findall':
                matches = regex.findall(text)
                if not as_list:
                    matches = ' | '.join(str(m) for m in matches) if matches else ''
                result = matches
                
            elif mode == 'search':
                match = regex.search(text)
                if match:
                    result = match.group(group) if match.groups() else match.group(0)
                else:
                    result = None
                    
            elif mode == 'match':
                match = regex.match(text)
                if match:
                    result = match.group(group) if match.groups() else match.group(0)
                else:
                    result = None
                    
            elif mode == 'finditer':
                matches = []
                for m in regex.finditer(text):
                    if m.groups():
                        matches.append(m.group(group))
                    else:
                        matches.append(m.group(0))
                result = matches if as_list else '\n'.join(matches)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown mode: {mode}"
                )
            
            count = len(result) if isinstance(result, list) else (1 if result else 0)
            
            return ActionResult(
                success=True,
                message=f"Found {count} match(es)",
                data={'result': result, 'count': count, 'pattern': pattern}
            )
            
        except re.error as e:
            return ActionResult(
                success=False,
                message=f"Regex error: {e}",
                data={'error': str(e)}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Regex extract error: {e}",
                data={'error': str(e)}
            )


class TextFormatAction(BaseAction):
    """Format text with template and variables.
    
    Supports Python format strings and template literals.
    """
    action_type = "text_format"
    display_name = "文本格式化"
    description = "格式化文本模板"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Format text template.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: template, vars, style (format/fstring/template).
        
        Returns:
            ActionResult with formatted text.
        """
        template = params.get('template', '')
        vars_dict = params.get('vars', {})
        style = params.get('style', 'format')
        
        if not template:
            return ActionResult(success=False, message="template is required")
        
        if not vars_dict:
            vars_dict = {}
        
        try:
            if style == 'format':
                result = template.format(**vars_dict)
            elif style == 'fstring':
                # Simple f-string simulation
                result = template
                for key, value in vars_dict.items():
                    result = result.replace(f'{{{key}}}', str(value))
            elif style == 'template':
                from string import Template
                t = Template(template)
                result = t.safe_substitute(vars_dict)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown style: {style}"
                )
            
            return ActionResult(
                success=True,
                message=f"Formatted ({len(result)} chars)",
                data={'text': result, 'length': len(result)}
            )
            
        except KeyError as e:
            return ActionResult(
                success=False,
                message=f"Missing variable: {e}",
                data={'error': str(e)}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Format error: {e}",
                data={'error': str(e)}
            )
