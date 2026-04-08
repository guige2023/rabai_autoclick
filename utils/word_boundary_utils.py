"""
Word Boundary Utilities

Provides utilities for handling word boundaries
in text processing for automation.

Author: Agent3
"""
from __future__ import annotations


class WordBoundary:
    """
    Handles word boundary detection in text.
    
    Provides methods for splitting and identifying
    word boundaries across different languages.
    """

    def __init__(self) -> None:
        self._delimiters = {" ", "\t", "\n", "\r", "-", "_", ".", ","}

    def split_words(self, text: str) -> list[str]:
        """
        Split text into words.
        
        Args:
            text: Text to split.
            
        Returns:
            List of words.
        """
        return [w for w in text.split() if w]

    def is_word_char(self, char: str) -> bool:
        """Check if character is a word character."""
        return char not in self._delimiters

    def find_word_at_position(
        self,
        text: str,
        position: int,
    ) -> tuple[int, int]:
        """
        Find word boundaries at position.
        
        Returns:
            (start, end) indices of word.
        """
        if position < 0 or position >= len(text):
            return (-1, -1)
        
        start = position
        while start > 0 and self.is_word_char(text[start - 1]):
            start -= 1
        
        end = position
        while end < len(text) and self.is_word_char(text[end]):
            end += 1
        
        return (start, end)
