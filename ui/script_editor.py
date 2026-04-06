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
    """Python syntax highlighter for QTextEdit.
    
    Highlights Python keywords, strings, comments, numbers,
    and builtin functions with distinct colors.
    """
    
    def __init__(self, parent: Optional[QWidget] = None) -> None:
        """Initialize the syntax highlighter.
        
        Args:
            parent: Optional parent QTextDocument.
        """
        super().__init__(parent)
        
        self.formats: Dict[str, Any] = {}
        
        # Keyword highlighting (blue, bold)
        keyword_format = QTextCharFormat()
        keyword_format.setForeground(QColor('#0000FF'))
        keyword_format.setFontWeight(QFont.Bold)
        keywords: List[str] = [
            'False', 'None', 'True', 'and', 'as', 'assert', 'async', 'await',
            'break', 'class', 'continue', 'def', 'del', 'elif', 'else', 'except',
            'finally', 'for', 'from', 'global', 'if', 'import', 'in', 'is',
            'lambda', 'nonlocal', 'not', 'or', 'pass', 'raise', 'return', 'try',
            'while', 'with', 'yield'
        ]
        self.formats['keyword'] = (keyword_format, keywords)
        
        # String highlighting (green)
        string_format = QTextCharFormat()
        string_format.setForeground(QColor('#008000'))
        self.formats['string'] = string_format
        
        # Comment highlighting (gray)
        comment_format = QTextCharFormat()
        comment_format.setForeground(QColor('#808080'))
        self.formats['comment'] = comment_format
        
        # Number highlighting (red)
        number_format = QTextCharFormat()
        number_format.setForeground(QColor('#FF0000'))
        self.formats['number'] = number_format
        
        # Builtin function highlighting (purple)
        builtin_format = QTextCharFormat()
        builtin_format.setForeground(QColor('#800080'))
        builtins: List[str] = [
            'print', 'len', 'int', 'str', 'float', 'list', 'dict',
            'range', 'sum', 'min', 'max'
        ]
        self.formats['builtin'] = (builtin_format, builtins)
    
    def highlightBlock(self, text: str) -> None:
        """Apply syntax highlighting to a block of text.
        
        Args:
            text: Line of text to highlight.
        """
        for name, value in self.formats.items():
            if name == 'keyword':
                fmt, words = value
                for word in words:
                    for pos in self._find_word(text, word):
                        self.setFormat(pos, len(word), fmt)
            elif name == 'builtin':
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
