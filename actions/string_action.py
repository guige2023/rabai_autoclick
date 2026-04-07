"""String manipulation action module for RabAI AutoClick.

Provides string operations:
- StringUpperAction: Convert to uppercase
- StringLowerAction: Convert to lowercase
- StringReplaceAction: Replace substring
- StringSplitAction: Split string
- StringJoinAction: Join strings
- StringStripAction: Strip whitespace
- StringContainsAction: Check substring
- StringStartsWithAction: Check prefix
- StringEndsWithAction: Check suffix
- StringReverseAction: Reverse string
- StringLengthAction: Get length
- StringSliceAction: Slice string
- StringReverseSliceAction: Reverse slice
- StringCountAction: Count occurrences
- StringFindAction: Find index
- StringIsDigitAction: Check if digit
- StringIsAlphaAction: Check if alphabetic
- StringIsAlnumAction: Check if alphanumeric
- StringCapitalizeAction: Capitalize first letter
- StringTitleAction: Title case
- StringSwapCaseAction: Swap case
- StringZfillAction: Zero fill
- StringLjustAction: Left justify
- StringRjustAction: Right justify
- StringCenterAction: Center justify
- StringExpandTabsAction: Expand tabs
- StringTranslateAction: Translate characters
- StringStripTagsAction: Strip HTML tags
- StringEscapeHtmlAction: Escape HTML
- StringUnescapeHtmlAction: Unescape HTML
- StringBase64Action: Base64 encode/decode
- StringUrlEncodeAction: URL encode
- StringUrlDecodeAction: URL decode
- StringMd5Action: MD5 hash
- StringSha256Action: SHA256 hash
- StringLevenshteinAction: Levenshtein distance
- StringSimilarityAction: String similarity
- StringTruncateAction: Truncate string
- StringWordCountAction: Word count
- StringCharCountAction: Character count
- StringLineCountAction: Line count
- StringRemoveDupesAction: Remove duplicates
- StringUniqueAction: Get unique chars
- StringShuffleAction: Shuffle characters
- StringSortAction: Sort characters
- StringDiffAction: String difference
- StringIntersectAction: String intersection
"""

from typing import Any, Dict, List, Optional, Union

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


try:
    import re
    import string
    import html
    import urllib.parse
    import base64 as base64_module
    STRING_AVAILABLE = True
except ImportError:
    STRING_AVAILABLE = False


class StringUpperAction(BaseAction):
    """Convert to uppercase."""
    action_type = "string_upper"
    display_name = "转大写"
    description = "将字符串转换为大写"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute uppercase conversion."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        output_var = params.get('output_var', 'upper_result')

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        result = text.upper()
        context.set(output_var, result)

        return ActionResult(
            success=True,
            message=f"转大写成功: {result[:50]}...",
            data={'result': result}
        )


class StringLowerAction(BaseAction):
    """Convert to lowercase."""
    action_type = "string_lower"
    display_name = "转小写"
    description = "将字符串转换为小写"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute lowercase conversion."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        output_var = params.get('output_var', 'lower_result')

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        result = text.lower()
        context.set(output_var, result)

        return ActionResult(
            success=True,
            message=f"转小写成功: {result[:50]}...",
            data={'result': result}
        )


class StringReplaceAction(BaseAction):
    """Replace substring."""
    action_type = "string_replace"
    display_name = "字符串替换"
    description = "替换字符串中的子串"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute string replacement."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        old = params.get('old', '')
        new = params.get('new', '')
        count = params.get('count', -1)
        output_var = params.get('output_var', 'replace_result')

        if not text or not old:
            return ActionResult(success=False, message="文本和旧字符串都不能为空")

        result = text.replace(old, new, count)
        context.set(output_var, result)

        return ActionResult(
            success=True,
            message=f"替换成功: {text.count(old)} 处",
            data={'result': result, 'count': text.count(old)}
        )


class StringSplitAction(BaseAction):
    """Split string."""
    action_type = "string_split"
    display_name = "字符串分割"
    description = "分割字符串为列表"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute string split."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        separator = params.get('separator', None)
        max_split = params.get('max_split', -1)
        output_var = params.get('output_var', 'split_result')

        if not text:
            return ActionResult(success=False, message="文本不能为空")

        if separator is None:
            parts = text.split() if max_split == -1 else text.split(maxsplit=max_split)
        else:
            parts = text.split(separator, max_split if max_split != -1 else -1)

        context.set(output_var, parts)

        return ActionResult(
            success=True,
            message=f"分割成功: {len(parts)} 部分",
            data={'result': parts, 'count': len(parts)}
        )


class StringJoinAction(BaseAction):
    """Join strings."""
    action_type = "string_join"
    display_name = "字符串连接"
    description = "连接字符串列表"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute string join."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        items = params.get('items', [])
        separator = params.get('separator', '')
        output_var = params.get('output_var', 'join_result')

        if not items:
            return ActionResult(success=False, message="列表不能为空")

        valid, msg = self.validate_type(items, list, 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        result = separator.join(str(item) for item in items)
        context.set(output_var, result)

        return ActionResult(
            success=True,
            message=f"连接成功: {len(items)} 项",
            data={'result': result}
        )


class StringStripAction(BaseAction):
    """Strip whitespace."""
    action_type = "string_strip"
    display_name = "去除空白"
    description = "去除字符串首尾空白"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute string strip."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        chars = params.get('chars', None)
        side = params.get('side', 'both')
        output_var = params.get('output_var', 'strip_result')

        if not text:
            return ActionResult(success=False, message="文本不能为空")

        if chars is None:
            if side == 'left':
                result = text.lstrip()
            elif side == 'right':
                result = text.rstrip()
            else:
                result = text.strip()
        else:
            if side == 'left':
                result = text.lstrip(chars)
            elif side == 'right':
                result = text.rstrip(chars)
            else:
                result = text.strip(chars)

        context.set(output_var, result)

        return ActionResult(
            success=True,
            message=f"去空白成功: '{result[:30]}...'",
            data={'result': result}
        )


class StringContainsAction(BaseAction):
    """Check substring."""
    action_type = "string_contains"
    display_name = "包含判断"
    description = "判断字符串是否包含子串"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute substring check."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        substring = params.get('substring', '')
        case_sensitive = params.get('case_sensitive', True)
        output_var = params.get('output_var', 'contains_result')

        if not text or not substring:
            return ActionResult(success=False, message="文本和子串都不能为空")

        if case_sensitive:
            contains = substring in text
        else:
            contains = substring.lower() in text.lower()

        context.set(output_var, contains)

        return ActionResult(
            success=True,
            message=f"包含判断: {contains}",
            data={'contains': contains}
        )


class StringStartsWithAction(BaseAction):
    """Check prefix."""
    action_type = "string_startswith"
    display_name = "开头判断"
    description = "判断字符串是否以子串开头"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute prefix check."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        prefix = params.get('prefix', '')
        output_var = params.get('output_var', 'startswith_result')

        if not text or not prefix:
            return ActionResult(success=False, message="文本和前缀都不能为空")

        result = text.startswith(prefix)
        context.set(output_var, result)

        return ActionResult(
            success=True,
            message=f"开头判断: {result}",
            data={'startswith': result}
        )


class StringEndsWithAction(BaseAction):
    """Check suffix."""
    action_type = "string_endswith"
    display_name = "结尾判断"
    description = "判断字符串是否以子串结尾"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute suffix check."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        suffix = params.get('suffix', '')
        output_var = params.get('output_var', 'endswith_result')

        if not text or not suffix:
            return ActionResult(success=False, message="文本和后缀都不能为空")

        result = text.endswith(suffix)
        context.set(output_var, result)

        return ActionResult(
            success=True,
            message=f"结尾判断: {result}",
            data={'endswith': result}
        )


class StringReverseAction(BaseAction):
    """Reverse string."""
    action_type = "string_reverse"
    display_name = "字符串反转"
    description = "反转字符串"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute string reverse."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        output_var = params.get('output_var', 'reverse_result')

        if not text:
            return ActionResult(success=False, message="文本不能为空")

        result = text[::-1]
        context.set(output_var, result)

        return ActionResult(
            success=True,
            message=f"反转成功: {result[:30]}...",
            data={'result': result}
        )


class StringLengthAction(BaseAction):
    """Get length."""
    action_type = "string_length"
    display_name = "字符串长度"
    description = "获取字符串长度"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute length get."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        output_var = params.get('output_var', 'length_result')

        if text is None:
            return ActionResult(success=False, message="文本不能为空")

        length = len(text)
        context.set(output_var, length)

        return ActionResult(
            success=True,
            message=f"长度: {length}",
            data={'length': length}
        )


class StringSliceAction(BaseAction):
    """Slice string."""
    action_type = "string_slice"
    display_name = "字符串切片"
    description = "切片字符串"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute string slice."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        start = params.get('start', 0)
        end = params.get('end', None)
        step = params.get('step', 1)
        output_var = params.get('output_var', 'slice_result')

        if text is None:
            return ActionResult(success=False, message="文本不能为空")

        if end is None:
            result = text[start::step]
        else:
            result = text[start:end:step]

        context.set(output_var, result)

        return ActionResult(
            success=True,
            message=f"切片成功: {result[:30]}...",
            data={'result': result}
        )


class StringCountAction(BaseAction):
    """Count occurrences."""
    action_type = "string_count"
    display_name = "子串计数"
    description = "统计子串出现次数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute substring count."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        substring = params.get('substring', '')
        case_sensitive = params.get('case_sensitive', True)
        output_var = params.get('output_var', 'count_result')

        if not text or not substring:
            return ActionResult(success=False, message="文本和子串都不能为空")

        if case_sensitive:
            count = text.count(substring)
        else:
            count = text.lower().count(substring.lower())

        context.set(output_var, count)

        return ActionResult(
            success=True,
            message=f"出现 {count} 次",
            data={'count': count}
        )


class StringFindAction(BaseAction):
    """Find index."""
    action_type = "string_find"
    display_name = "查找位置"
    description = "查找子串位置"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute substring find."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        substring = params.get('substring', '')
        case_sensitive = params.get('case_sensitive', True)
        output_var = params.get('output_var', 'find_result')

        if not text or not substring:
            return ActionResult(success=False, message="文本和子串都不能为空")

        if case_sensitive:
            index = text.find(substring)
        else:
            index = text.lower().find(substring.lower())

        context.set(output_var, index)

        return ActionResult(
            success=True,
            message=f"位置: {index}" if index >= 0 else "未找到",
            data={'index': index, 'found': index >= 0}
        )


class StringIsDigitAction(BaseAction):
    """Check if digit."""
    action_type = "string_isdigit"
    display_name = "数字判断"
    description = "判断是否全是数字"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute digit check."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        output_var = params.get('output_var', 'isdigit_result')

        if text is None:
            return ActionResult(success=False, message="文本不能为空")

        result = text.isdigit()
        context.set(output_var, result)

        return ActionResult(
            success=True,
            message=f"是否数字: {result}",
            data={'isdigit': result}
        )


class StringIsAlphaAction(BaseAction):
    """Check if alphabetic."""
    action_type = "string_isalpha"
    display_name = "字母判断"
    description = "判断是否全是字母"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute alpha check."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        output_var = params.get('output_var', 'isalpha_result')

        if text is None:
            return ActionResult(success=False, message="文本不能为空")

        result = text.isalpha()
        context.set(output_var, result)

        return ActionResult(
            success=True,
            message=f"是否字母: {result}",
            data={'isalpha': result}
        )


class StringIsAlnumAction(BaseAction):
    """Check if alphanumeric."""
    action_type = "string_isalnum"
    display_name = "字母数字判断"
    description = "判断是否全是字母或数字"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute alnum check."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        output_var = params.get('output_var', 'isalnum_result')

        if text is None:
            return ActionResult(success=False, message="文本不能为空")

        result = text.isalnum()
        context.set(output_var, result)

        return ActionResult(
            success=True,
            message=f"是否字母数字: {result}",
            data={'isalnum': result}
        )


class StringCapitalizeAction(BaseAction):
    """Capitalize first letter."""
    action_type = "string_capitalize"
    display_name = "首字母大写"
    description = "首字母大写"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute capitalize."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        output_var = params.get('output_var', 'capitalize_result')

        if text is None:
            return ActionResult(success=False, message="文本不能为空")

        result = text.capitalize()
        context.set(output_var, result)

        return ActionResult(
            success=True,
            message=f"首字母大写: {result}",
            data={'result': result}
        )


class StringTitleAction(BaseAction):
    """Title case."""
    action_type = "string_title"
    display_name = "标题格式"
    description = "每个单词首字母大写"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute title case."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        output_var = params.get('output_var', 'title_result')

        if text is None:
            return ActionResult(success=False, message="文本不能为空")

        result = text.title()
        context.set(output_var, result)

        return ActionResult(
            success=True,
            message=f"标题格式: {result}",
            data={'result': result}
        )


class StringSwapCaseAction(BaseAction):
    """Swap case."""
    action_type = "string_swapcase"
    display_name = "大小写翻转"
    description = "翻转大小写"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute swap case."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        output_var = params.get('output_var', 'swapcase_result')

        if text is None:
            return ActionResult(success=False, message="文本不能为空")

        result = text.swapcase()
        context.set(output_var, result)

        return ActionResult(
            success=True,
            message=f"翻转: {result}",
            data={'result': result}
        )


class StringZfillAction(BaseAction):
    """Zero fill."""
    action_type = "string_zfill"
    display_name = "零填充"
    description = "字符串左侧零填充"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute zfill."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        width = params.get('width', 0)
        output_var = params.get('output_var', 'zfill_result')

        if text is None:
            return ActionResult(success=False, message="文本不能为空")

        result = text.zfill(width)
        context.set(output_var, result)

        return ActionResult(
            success=True,
            message=f"零填充: {result}",
            data={'result': result}
        )


class StringLjustAction(BaseAction):
    """Left justify."""
    action_type = "string_ljust"
    display_name = "左对齐"
    description = "字符串左对齐"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute ljust."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        width = params.get('width', 0)
        fillchar = params.get('fillchar', ' ')
        output_var = params.get('output_var', 'ljust_result')

        if text is None:
            return ActionResult(success=False, message="文本不能为空")

        if len(fillchar) != 1:
            return ActionResult(success=False, message="填充字符必须是单个字符")

        result = text.ljust(width, fillchar)
        context.set(output_var, result)

        return ActionResult(
            success=True,
            message=f"左对齐: '{result}'",
            data={'result': result}
        )


class StringRjustAction(BaseAction):
    """Right justify."""
    action_type = "string_rjust"
    display_name = "右对齐"
    description = "字符串右对齐"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute rjust."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        width = params.get('width', 0)
        fillchar = params.get('fillchar', ' ')
        output_var = params.get('output_var', 'rjust_result')

        if text is None:
            return ActionResult(success=False, message="文本不能为空")

        if len(fillchar) != 1:
            return ActionResult(success=False, message="填充字符必须是单个字符")

        result = text.rjust(width, fillchar)
        context.set(output_var, result)

        return ActionResult(
            success=True,
            message=f"右对齐: '{result}'",
            data={'result': result}
        )


class StringCenterAction(BaseAction):
    """Center justify."""
    action_type = "string_center"
    display_name = "居中对齐"
    description = "字符串居中对齐"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute center."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        width = params.get('width', 0)
        fillchar = params.get('fillchar', ' ')
        output_var = params.get('output_var', 'center_result')

        if text is None:
            return ActionResult(success=False, message="文本不能为空")

        if len(fillchar) != 1:
            return ActionResult(success=False, message="填充字符必须是单个字符")

        result = text.center(width, fillchar)
        context.set(output_var, result)

        return ActionResult(
            success=True,
            message=f"居中: '{result}'",
            data={'result': result}
        )


class StringExpandTabsAction(BaseAction):
    """Expand tabs."""
    action_type = "string_expandtabs"
    display_name = "制表符扩展"
    description = "将制表符扩展为空格"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute expand tabs."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        tabsize = params.get('tabsize', 8)
        output_var = params.get('output_var', 'expandtabs_result')

        if text is None:
            return ActionResult(success=False, message="文本不能为空")

        result = text.expandtabs(tabsize)
        context.set(output_var, result)

        return ActionResult(
            success=True,
            message=f"扩展成功",
            data={'result': result}
        )


class StringStripTagsAction(BaseAction):
    """Strip HTML tags."""
    action_type = "string_striptags"
    display_name = "去除HTML标签"
    description = "去除HTML标签"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute strip HTML tags."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        output_var = params.get('output_var', 'striptags_result')

        if text is None:
            return ActionResult(success=False, message="文本不能为空")

        result = re.sub(r'<[^>]+>', '', text)
        context.set(output_var, result)

        return ActionResult(
            success=True,
            message=f"去标签成功",
            data={'result': result}
        )


class StringEscapeHtmlAction(BaseAction):
    """Escape HTML."""
    action_type = "string_escape_html"
    display_name = "HTML转义"
    description = "转义HTML特殊字符"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute HTML escape."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        output_var = params.get('output_var', 'escape_html_result')

        if text is None:
            return ActionResult(success=False, message="文本不能为空")

        result = html.escape(text)
        context.set(output_var, result)

        return ActionResult(
            success=True,
            message=f"HTML转义成功",
            data={'result': result}
        )


class StringUnescapeHtmlAction(BaseAction):
    """Unescape HTML."""
    action_type = "string_unescape_html"
    display_name = "HTML反转义"
    description = "反转义HTML实体"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute HTML unescape."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        output_var = params.get('output_var', 'unescape_html_result')

        if text is None:
            return ActionResult(success=False, message="文本不能为空")

        result = html.unescape(text)
        context.set(output_var, result)

        return ActionResult(
            success=True,
            message=f"HTML反转义成功",
            data={'result': result}
        )


class StringBase64Action(BaseAction):
    """Base64 encode/decode."""
    action_type = "string_base64"
    display_name = "Base64编解码"
    description = "Base64编码或解码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Base64 encode/decode."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        mode = params.get('mode', 'encode')
        output_var = params.get('output_var', 'base64_result')

        if not text:
            return ActionResult(success=False, message="文本不能为空")

        try:
            if mode == 'encode':
                result = base64_module.b64encode(text.encode('utf-8')).decode('utf-8')
            else:
                result = base64_module.b64decode(text.encode('utf-8')).decode('utf-8', errors='replace')

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"Base64{'编码' if mode == 'encode' else '解码'}成功",
                data={'result': result}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Base64操作失败: {str(e)}"
            )


class StringUrlEncodeAction(BaseAction):
    """URL encode."""
    action_type = "string_url_encode"
    display_name = "URL编码"
    description = "URL编码字符串"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute URL encode."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        safe = params.get('safe', '')
        output_var = params.get('output_var', 'url_encode_result')

        if text is None:
            return ActionResult(success=False, message="文本不能为空")

        result = urllib.parse.quote(text, safe=safe)
        context.set(output_var, result)

        return ActionResult(
            success=True,
            message=f"URL编码成功",
            data={'result': result}
        )


class StringUrlDecodeAction(BaseAction):
    """URL decode."""
    action_type = "string_url_decode"
    display_name = "URL解码"
    description = "URL解码字符串"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute URL decode."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        output_var = params.get('output_var', 'url_decode_result')

        if text is None:
            return ActionResult(success=False, message="文本不能为空")

        result = urllib.parse.unquote(text)
        context.set(output_var, result)

        return ActionResult(
            success=True,
            message=f"URL解码成功",
            data={'result': result}
        )


class StringLevenshteinAction(BaseAction):
    """Levenshtein distance."""
    action_type = "string_levenshtein"
    display_name = "编辑距离"
    description = "计算两个字符串的编辑距离"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Levenshtein distance."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text1 = params.get('text1', '')
        text2 = params.get('text2', '')
        output_var = params.get('output_var', 'levenshtein_result')

        if not text1 or not text2:
            return ActionResult(success=False, message="两个文本都不能为空")

        if len(text1) < len(text2):
            return self._levenshtein(text2, text1, output_var)

        if len(text2) == 0:
            return ActionResult(success=True, message=f"编辑距离: {len(text1)}", data={'distance': len(text1)})

        previous_row = range(len(text2) + 1)
        for i, c1 in enumerate(text1):
            current_row = [i + 1]
            for j, c2 in enumerate(text2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row

        distance = previous_row[-1]
        context.set(output_var, distance)

        return ActionResult(
            success=True,
            message=f"编辑距离: {distance}",
            data={'distance': distance}
        )

    def _levenshtein(self, s1: str, s2: str, output_var: str) -> ActionResult:
        """Internal Levenshtein implementation."""
        if len(s2) == 0:
            return ActionResult(success=True, message=f"编辑距离: {len(s1)}", data={'distance': len(s1)})

        previous_row = range(len(s2) + 1)
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row

        distance = previous_row[-1]
        return ActionResult(success=True, message=f"编辑距离: {distance}", data={'distance': distance})


class StringSimilarityAction(BaseAction):
    """String similarity."""
    action_type = "string_similarity"
    display_name = "字符串相似度"
    description = "计算两个字符串的相似度"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute string similarity."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text1 = params.get('text1', '')
        text2 = params.get('text2', '')
        output_var = params.get('output_var', 'similarity_result')

        if not text1 or not text2:
            return ActionResult(success=False, message="两个文本都不能为空")

        if len(text1) < len(text2):
            text1, text2 = text2, text1

        if len(text2) == 0:
            context.set(output_var, 0.0)
            return ActionResult(success=True, message="相似度: 0.0", data={'similarity': 0.0})

        previous_row = range(len(text2) + 1)
        for i, c1 in enumerate(text1):
            current_row = [i + 1]
            for j, c2 in enumerate(text2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row

        distance = previous_row[-1]
        max_len = max(len(text1), len(text2))
        similarity = 1 - (distance / max_len)

        context.set(output_var, similarity)

        return ActionResult(
            success=True,
            message=f"相似度: {similarity:.4f}",
            data={'similarity': similarity}
        )


class StringTruncateAction(BaseAction):
    """Truncate string."""
    action_type = "string_truncate"
    display_name = "截断字符串"
    description = "截断字符串到指定长度"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute string truncate."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        length = params.get('length', 100)
        suffix = params.get('suffix', '...')
        output_var = params.get('output_var', 'truncate_result')

        if text is None:
            return ActionResult(success=False, message="文本不能为空")

        if len(text) <= length:
            context.set(output_var, text)
            return ActionResult(success=True, message="无需截断", data={'result': text})

        result = text[:length] + suffix
        context.set(output_var, result)

        return ActionResult(
            success=True,
            message=f"截断成功: {len(result)} 字符",
            data={'result': result}
        )


class StringWordCountAction(BaseAction):
    """Word count."""
    action_type = "string_wordcount"
    display_name = "单词计数"
    description = "统计单词数量"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute word count."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        output_var = params.get('output_var', 'wordcount_result')

        if text is None:
            return ActionResult(success=False, message="文本不能为空")

        words = text.split()
        count = len(words)

        context.set(output_var, count)

        return ActionResult(
            success=True,
            message=f"单词数: {count}",
            data={'count': count, 'words': words[:20]}
        )


class StringLineCountAction(BaseAction):
    """Line count."""
    action_type = "string_linecount"
    display_name = "行数统计"
    description = "统计行数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute line count."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        keep_empty = params.get('keep_empty', False)
        output_var = params.get('output_var', 'linecount_result')

        if text is None:
            return ActionResult(success=False, message="文本不能为空")

        if keep_empty:
            lines = text.splitlines(keepends=True)
        else:
            lines = [l for l in text.splitlines() if l.strip()]

        count = len(lines)
        context.set(output_var, count)

        return ActionResult(
            success=True,
            message=f"行数: {count}",
            data={'count': count}
        )


class StringRemoveDupesAction(BaseAction):
    """Remove duplicates."""
    action_type = "string_removedupes"
    display_name = "去除重复字符"
    description = "去除字符串中的重复字符"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute remove duplicates."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        preserve_order = params.get('preserve_order', True)
        output_var = params.get('output_var', 'removedupes_result')

        if text is None:
            return ActionResult(success=False, message="文本不能为空")

        if preserve_order:
            seen = set()
            result = []
            for c in text:
                if c not in seen:
                    seen.add(c)
                    result.append(c)
            result = ''.join(result)
        else:
            result = ''.join(set(text))

        context.set(output_var, result)

        return ActionResult(
            success=True,
            message=f"去重成功: {len(text)} → {len(result)} 字符",
            data={'result': result, 'original_length': len(text), 'new_length': len(result)}
        )


class StringUniqueAction(BaseAction):
    """Get unique chars."""
    action_type = "string_unique"
    display_name = "唯一字符"
    description = "获取唯一字符列表"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute unique chars."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        preserve_order = params.get('preserve_order', True)
        output_var = params.get('output_var', 'unique_result')

        if text is None:
            return ActionResult(success=False, message="文本不能为空")

        if preserve_order:
            seen = set()
            result = []
            for c in text:
                if c not in seen:
                    seen.add(c)
                    result.append(c)
        else:
            result = list(set(text))

        context.set(output_var, result)

        return ActionResult(
            success=True,
            message=f"唯一字符: {len(result)} 个",
            data={'result': result, 'count': len(result)}
        )


class StringShuffleAction(BaseAction):
    """Shuffle characters."""
    action_type = "string_shuffle"
    display_name = "打乱字符"
    description = "随机打乱字符串字符"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute shuffle."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        seed = params.get('seed', None)
        output_var = params.get('output_var', 'shuffle_result')

        if text is None:
            return ActionResult(success=False, message="文本不能为空")

        if seed is not None:
            import random
            random.seed(seed)

        chars = list(text)
        import random as rand_module
        rand_module.shuffle(chars)
        result = ''.join(chars)

        context.set(output_var, result)

        return ActionResult(
            success=True,
            message=f"打乱成功: {result[:30]}...",
            data={'result': result}
        )


class StringSortAction(BaseAction):
    """Sort characters."""
    action_type = "string_sort"
    display_name = "排序字符"
    description = "对字符串字符排序"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute sort."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text = params.get('text', '')
        reverse = params.get('reverse', False)
        output_var = params.get('output_var', 'sort_result')

        if text is None:
            return ActionResult(success=False, message="文本不能为空")

        result = ''.join(sorted(text, reverse=reverse))

        context.set(output_var, result)

        return ActionResult(
            success=True,
            message=f"排序成功: {result[:30]}...",
            data={'result': result}
        )


class StringDiffAction(BaseAction):
    """String difference."""
    action_type = "string_diff"
    display_name = "字符串差集"
    description = "获取两个字符串的差集"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute string diff."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text1 = params.get('text1', '')
        text2 = params.get('text2', '')
        output_var = params.get('output_var', 'diff_result')

        if not text1 or not text2:
            return ActionResult(success=False, message="两个文本都不能为空")

        set1 = set(text1)
        set2 = set(text2)
        diff = set1 - set2

        result = ''.join(sorted(diff))
        context.set(output_var, result)

        return ActionResult(
            success=True,
            message=f"差集: {result}",
            data={'result': result, 'count': len(diff)}
        )


class StringIntersectAction(BaseAction):
    """String intersection."""
    action_type = "string_intersect"
    display_name = "字符串交集"
    description = "获取两个字符串的交集"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute string intersection."""
        if not STRING_AVAILABLE:
            return ActionResult(success=False, message="string库不可用")

        text1 = params.get('text1', '')
        text2 = params.get('text2', '')
        output_var = params.get('output_var', 'intersect_result')

        if not text1 or not text2:
            return ActionResult(success=False, message="两个文本都不能为空")

        set1 = set(text1)
        set2 = set(text2)
        intersection = set1 & set2

        result = ''.join(sorted(intersection))
        context.set(output_var, result)

        return ActionResult(
            success=True,
            message=f"交集: {result}",
            data={'result': result, 'count': len(intersection)}
        )
