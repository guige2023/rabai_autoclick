"""Script editor dialog for RabAI AutoClick.

Provides a Python code editor with syntax highlighting
for creating and editing script actions.
"""

import os
import re
import sys
import traceback
from typing import Any, Dict, List, Optional, Tuple

from PyQt5.QtCore import Qt, pyqtSignal
from PyQt5.QtGui import (
    QColor, QFont, QKeySequence, QSyntaxHighlighter, QTextCharFormat
)
from PyQt5.QtWidgets import (
    QComboBox, QDialog, QFormLayout, QGroupBox, QHBoxLayout,
    QLabel, QLineEdit, QMessageBox, QPushButton, QSpinBox,
    QSplitter, QTextEdit, QVBoxLayout, QWidget
)


# Add project root to path
sys.path.insert(0, os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
))

from core.context import ContextManager


class PythonHighlighter(QSyntaxHighlighter):
    """Python syntax highlighter for QTextEdit with theme-aware colors.

    Highlights Python keywords, strings, comments, numbers,
    and builtin functions with distinct colors that adapt to theme.
    """

    # Theme-aware color palettes
    LIGHT_COLORS = {
        'keyword': '#0000FF',
        'string': '#008000',
        'comment': '#808080',
        'number': '#FF0000',
        'builtin': '#800080',
        'decorator': '#FF5500',
        'self': '#880000',
    }

    DARK_COLORS = {
        'keyword': '#569CD6',
        'string': '#CE9178',
        'comment': '#6A9955',
        'number': '#B5CEA8',
        'builtin': '#C586C0',
        'decorator': '#DCDCAA',
        'self': '#569CD6',
    }

    # Class-level keywords and builtins
    KEYWORDS = [
        'False', 'None', 'True', 'and', 'as', 'assert', 'async', 'await',
        'break', 'class', 'continue', 'def', 'del', 'elif', 'else', 'except',
        'finally', 'for', 'from', 'global', 'if', 'import', 'in', 'is',
        'lambda', 'nonlocal', 'not', 'or', 'pass', 'raise', 'return', 'try',
        'while', 'with', 'yield'
    ]

    BUILTINS = [
        'print', 'len', 'int', 'str', 'float', 'list', 'dict',
        'range', 'sum', 'min', 'max', 'abs', 'round', 'sorted',
        'enumerate', 'zip', 'map', 'filter', 'open', 'input'
    ]

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        """Initialize the syntax highlighter.

        Args:
            parent: Optional parent QTextDocument.
        """
        super().__init__(parent)

        self._is_dark_theme = False
        self.formats: Dict[str, Any] = {}
        self._rebuild_formats()

    def _rebuild_formats(self) -> None:
        """Rebuild all format objects based on current theme."""
        colors = self.DARK_COLORS if self._is_dark_theme else self.LIGHT_COLORS

        # Keyword highlighting (blue, bold)
        keyword_format = QTextCharFormat()
        keyword_format.setForeground(QColor(colors['keyword']))
        keyword_format.setFontWeight(QFont.Bold)
        self.formats['keyword'] = (keyword_format, self.KEYWORDS)

        # String highlighting (green)
        string_format = QTextCharFormat()
        string_format.setForeground(QColor(colors['string']))
        self.formats['string'] = string_format

        # Comment highlighting (gray, italic)
        comment_format = QTextCharFormat()
        comment_format.setForeground(QColor(colors['comment']))
        comment_format.setFontItalic(True)
        self.formats['comment'] = comment_format

        # Number highlighting (red)
        number_format = QTextCharFormat()
        number_format.setForeground(QColor(colors['number']))
        self.formats['number'] = number_format

        # Builtin function highlighting (purple)
        builtin_format = QTextCharFormat()
        builtin_format.setForeground(QColor(colors['builtin']))
        self.formats['builtin'] = (builtin_format, self.BUILTINS)

        # Decorator highlighting
        decorator_format = QTextCharFormat()
        decorator_format.setForeground(QColor(colors['decorator']))
        self.formats['decorator'] = decorator_format

        # Self keyword
        self_format = QTextCharFormat()
        self_format.setForeground(QColor(colors['self']))
        self_format.setFontItalic(True)
        self.formats['self'] = self_format

    def set_theme(self, is_dark: bool) -> None:
        """Update colors when theme changes.

        Args:
            is_dark: True for dark theme, False for light.
        """
        if self._is_dark_theme != is_dark:
            self._is_dark_theme = is_dark
            self._rebuild_formats()
            if self.document():
                self.rehighlight()
    
    def highlightBlock(self, text: str) -> None:
        """Apply syntax highlighting to a block of text.

        Args:
            text: Line of text to highlight.
        """
        # Highlight decorators first (before processing other elements)
        if 'decorator' in self.formats:
            self._highlight_decorators(text, self.formats['decorator'])

        for name, value in self.formats.items():
            if name in ('keyword', 'builtin'):
                fmt, words = value
                for word in words:
                    for pos in self._find_word(text, word):
                        self.setFormat(pos, len(word), fmt)
            elif name == 'string':
                self._highlight_strings(text, value)
            elif name == 'comment':
                if '#' in text:
                    pos = text.index('#')
                    self.setFormat(pos, len(text) - pos, value)
            elif name == 'number':
                self._highlight_numbers(text, value)

    def _highlight_decorators(self, text: str, fmt: QTextCharFormat) -> None:
        """Highlight Python decorators.

        Args:
            text: Line of text to process.
            fmt: QTextCharFormat for decorators.
        """
        for match in re.finditer(r'@\w+', text):
            self.setFormat(match.start(), match.end() - match.start(), fmt)
    
    def _find_word(self, text: str, word: str) -> List[int]:
        """Find all occurrences of a word in text.
        
        Args:
            text: Text to search.
            word: Word to find.
            
        Returns:
            List of starting positions for each match.
        """
        pattern = r'\b' + word + r'\b'
        return [m.start() for m in re.finditer(pattern, text)]
    
    def _highlight_strings(self, text: str, fmt: QTextCharFormat) -> None:
        """Highlight string literals.
        
        Args:
            text: Text to process.
            fmt: QTextCharFormat for strings.
        """
        for match in re.finditer(r'"[^"]*"|\'[^\']*\'', text):
            self.setFormat(match.start(), match.end() - match.start(), fmt)
    
    def _highlight_numbers(self, text: str, fmt: QTextCharFormat) -> None:
        """Highlight numeric literals.
        
        Args:
            text: Text to process.
            fmt: QTextCharFormat for numbers.
        """
        for match in re.finditer(r'\b\d+\.?\d*\b', text):
            self.setFormat(match.start(), match.end() - match.start(), fmt)


class ScriptEditorDialog(QDialog):
    """Dialog for editing Python script actions.
    
    Provides a code editor with syntax highlighting, template
    selection, and script validation.
    """
    
    def __init__(
        self,
        context: Optional[ContextManager] = None,
        initial_code: str = '',
        parent: Optional[QWidget] = None
    ) -> None:
        """Initialize the script editor dialog.
        
        Args:
            context: Optional ContextManager instance.
            initial_code: Initial code to display in editor.
            parent: Optional parent widget.
        """
        super().__init__(parent)
        self.context = context or ContextManager()
        self.setWindowTitle("脚本编辑器")
        self.setMinimumSize(900, 700)
        self._init_ui()
        self._load_templates()
        
        if initial_code:
            self.code_edit.setPlainText(initial_code)
