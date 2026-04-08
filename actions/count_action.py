"""Count utilities action module for RabAI AutoClick.

Provides counting operations for collections
including word count, character count, and frequency analysis.
"""

import sys
import os
import re
from collections import Counter
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CountCharsAction(BaseAction):
    """Count characters in string.
    
    Supports char types and frequency counting.
    """
    action_type = "count_chars"
    display_name = "字符计数"
    description = "统计字符串字符数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Count characters.
        
        Args:
            context: Execution context.
            params: Dict with keys: text, count_spaces, count_newlines,
                   save_to_var.
        
        Returns:
            ActionResult with character counts.
        """
        text = params.get('text', '')
        count_spaces = params.get('count_spaces', True)
        count_newlines = params.get('count_newlines', True)
        save_to_var = params.get('save_to_var', None)

        if count_spaces and count_newlines:
            total = len(text)
        elif count_spaces:
            total = len(text.replace('\n', '').replace('\r', ''))
        elif count_newlines:
            total = len(text.replace(' ', '').replace('\t', ''))
        else:
            total = len(text.replace(' ', '').replace('\t', '').replace('\n', '').replace('\r', ''))

        counts = {
            'total': total,
            'letters': sum(1 for c in text if c.isalpha()),
            'digits': sum(1 for c in text if c.isdigit()),
            'spaces': text.count(' '),
            'newlines': text.count('\n'),
            'uppercase': sum(1 for c in text if c.isupper()),
            'lowercase': sum(1 for c in text if c.islower())
        }

        result_data = {
            'counts': counts,
            'text_length': len(text)
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"字符统计: {total} 字符",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'count_spaces': True,
            'count_newlines': True,
            'save_to_var': None
        }


class CountWordsAction(BaseAction):
    """Count words in text.
    
    Supports multiple delimiters and unique word counting.
    """
    action_type = "count_words"
    display_name = "单词计数"
    description = "统计文本单词数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Count words.
        
        Args:
            context: Execution context.
            params: Dict with keys: text, pattern, unique,
                   case_sensitive, save_to_var.
        
        Returns:
            ActionResult with word counts.
        """
        text = params.get('text', '')
        pattern = params.get('pattern', r'\b\w+\b')
        unique = params.get('unique', False)
        case_sensitive = params.get('case_sensitive', False)
        save_to_var = params.get('save_to_var', None)

        if not text:
            return ActionResult(success=False, message="Text is empty")

        # Extract words
        words = re.findall(pattern, text)

        if not case_sensitive:
            words = [w.lower() for w in words]

        total = len(words)
        unique_count = len(set(words))

        result_data = {
            'total': total,
            'unique': unique_count,
            'words': list(set(words)) if unique else words[:100]
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"单词统计: {total} 词, {unique_count} 唯一词",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'pattern': r'\b\w+\b',
            'unique': False,
            'case_sensitive': False,
            'save_to_var': None
        }


class CountLinesAction(BaseAction):
    """Count lines in text or file.
    
    Supports line endings and blank line counting.
    """
    action_type = "count_lines"
    display_name = "行计数"
    description = "统计文本行数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Count lines.
        
        Args:
            context: Execution context.
            params: Dict with keys: text, count_blank, count_empty,
                   save_to_var.
        
        Returns:
            ActionResult with line counts.
        """
        text = params.get('text', '')
        count_blank = params.get('count_blank', True)
        count_empty = params.get('count_empty', True)
        save_to_var = params.get('save_to_var', None)

        if not text:
            return ActionResult(success=False, message="Text is empty")

        lines = text.splitlines()
        total = len(lines)

        non_blank = sum(1 for line in lines if line.strip())
        blank = total - non_blank

        result_data = {
            'total': total,
            'non_blank': non_blank,
            'blank': blank
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"行数统计: {total} 行 ({non_blank} 非空)",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'count_blank': True,
            'count_empty': True,
            'save_to_var': None
        }
