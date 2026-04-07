"""String utilities action for string manipulation.

This module provides string operations including
formatting, padding, alignment, and encoding.

Example:
    >>> action = StringAction()
    >>> result = action.execute(operation="pad", text="Hello", width=10)
"""

from __future__ import annotations

from typing import Any, Optional


class StringAction:
    """String manipulation action.

    Provides string operations including formatting,
    padding, alignment, and transformation.

    Example:
        >>> action = StringAction()
        >>> result = action.execute(
        ...     operation="center",
        ...     text="Hello",
        ...     width=20
        ... )
    """

    def __init__(self) -> None:
        """Initialize string action."""
        pass

    def execute(
        self,
        operation: str,
        text: str = "",
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute string operation.

        Args:
            operation: Operation (pad, center, ljust, etc.).
            text: Input text.
            **kwargs: Additional parameters.

        Returns:
            Operation result dictionary.

        Raises:
            ValueError: If operation is invalid.
        """
        op = operation.lower()
        result: dict[str, Any] = {"operation": op, "success": True}

        width = kwargs.get("width", 0)
        fillchar = kwargs.get("fillchar", " ")

        if op == "pad":
            result["text"] = text.center(width, fillchar) if width else text.center(len(text) + 4, fillchar)

        elif op == "ljust":
            if not width:
                raise ValueError("width required for 'ljust'")
            result["text"] = text.ljust(width, fillchar)

        elif op == "rjust":
            if not width:
                raise ValueError("width required for 'rjust'")
            result["text"] = text.rjust(width, fillchar)

        elif op == "zfill":
            if not width:
                raise ValueError("width required for 'zfill'")
            result["text"] = text.zfill(width)

        elif op == "strip":
            result["text"] = text.strip()

        elif op == "lstrip":
            result["text"] = text.lstrip()

        elif op == "rstrip":
            result["text"] = text.rstrip()

        elif op == "upper":
            result["text"] = text.upper()

        elif op == "lower":
            result["text"] = text.lower()

        elif op == "title":
            result["text"] = text.title()

        elif op == "capitalize":
            result["text"] = text.capitalize()

        elif op == "swapcase":
            result["text"] = text.swapcase()

        elif op == "reverse":
            result["text"] = text[::-1]

        elif op == "repeat":
            times = kwargs.get("times", 2)
            result["text"] = text * times

        elif op == "index":
            substring = kwargs.get("substring")
            if not substring:
                raise ValueError("substring required for 'index'")
            try:
                result["index"] = text.index(substring)
            except ValueError:
                result["index"] = -1

        elif op == "count":
            substring = kwargs.get("substring", "")
            result["count"] = text.count(substring)

        elif op == "replace":
            old = kwargs.get("old", "")
            new = kwargs.get("new", "")
            result["text"] = text.replace(old, new)

        elif op == "split":
            delimiter = kwargs.get("delimiter", None)
            result["parts"] = text.split(delimiter)
            result["count"] = len(result["parts"])

        elif op == "rsplit":
            delimiter = kwargs.get("delimiter", None)
            result["parts"] = text.rsplit(delimiter)
            result["count"] = len(result["parts"])

        elif op == "partition":
            separator = kwargs.get("separator", " ")
            result["parts"] = list(text.partition(separator))

        elif op == "startswith":
            prefix = kwargs.get("prefix", "")
            result["startswith"] = text.startswith(prefix)

        elif op == "endswith":
            suffix = kwargs.get("suffix", "")
            result["endswith"] = text.endswith(suffix)

        elif op == "isalpha":
            result["isalpha"] = text.isalpha()

        elif op == "isdigit":
            result["isdigit"] = text.isdigit()

        elif op == "isalnum":
            result["isalnum"] = text.isalnum()

        elif op == "isspace":
            result["isspace"] = text.isspace()

        elif op == "isupper":
            result["isupper"] = text.isupper()

        elif op == "islower":
            result["islower"] = text.islower()

        elif op == "istitle":
            result["istitle"] = text.istitle()

        elif op == "encode":
            encoding = kwargs.get("encoding", "utf-8")
            result["encoded"] = text.encode(encoding).decode("latin-1")
            result["bytes"] = len(text.encode(encoding))

        elif op == "length":
            result["length"] = len(text)

        elif op == "truncate":
            length = kwargs.get("length", 100)
            suffix = kwargs.get("suffix", "...")
            if len(text) <= length:
                result["text"] = text
            else:
                result["text"] = text[:length - len(suffix)] + suffix

        else:
            raise ValueError(f"Unknown operation: {operation}")

        return result
