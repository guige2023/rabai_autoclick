"""Tests for terminal/CLI utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.terminal import (
    Colors,
    colorize,
    red,
    green,
    yellow,
    blue,
    cyan,
    bold,
    clear_line,
    hide_cursor,
    show_cursor,
    get_terminal_size,
    ProgressBar,
    Table,
)


class TestColors:
    """Tests for Colors."""

    def test_color_codes_exist(self) -> None:
        """Test color codes are defined."""
        assert Colors.RESET == "\033[0m"
        assert Colors.RED == "\033[31m"
        assert Colors.GREEN == "\033[32m"
        assert Colors.BLUE == "\033[34m"


class TestColorize:
    """Tests for colorize functions."""

    def test_colorize(self) -> None:
        """Test colorizing text."""
        result = colorize("hello", Colors.RED)
        assert result.startswith(Colors.RED)
        assert result.endswith(Colors.RESET)
        assert "hello" in result

    def test_red(self) -> None:
        """Test red function."""
        result = red("test")
        assert Colors.RED in result
        assert "test" in result

    def test_green(self) -> None:
        """Test green function."""
        result = green("test")
        assert Colors.GREEN in result

    def test_yellow(self) -> None:
        """Test yellow function."""
        result = yellow("test")
        assert Colors.YELLOW in result

    def test_blue(self) -> None:
        """Test blue function."""
        result = blue("test")
        assert Colors.BLUE in result

    def test_cyan(self) -> None:
        """Test cyan function."""
        result = cyan("test")
        assert Colors.CYAN in result

    def test_bold(self) -> None:
        """Test bold function."""
        result = bold("test")
        assert Colors.BOLD in result


class TestAnsiCodes:
    """Tests for ANSI code functions."""

    def test_clear_line(self) -> None:
        """Test clear_line."""
        assert clear_line() == "\033[2K\r"

    def test_hide_cursor(self) -> None:
        """Test hide_cursor."""
        assert hide_cursor() == "\033[?25l"

    def test_show_cursor(self) -> None:
        """Test show_cursor."""
        assert show_cursor() == "\033[?25h"


class TestGetTerminalSize:
    """Tests for get_terminal_size."""

    def test_returns_tuple(self) -> None:
        """Test returns columns and lines."""
        result = get_terminal_size()
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert result[0] > 0
        assert result[1] > 0


class TestProgressBar:
    """Tests for ProgressBar."""

    def test_create_default(self) -> None:
        """Test creating progress bar with defaults."""
        bar = ProgressBar()
        assert bar.total == 100
        assert bar.current == 0
        assert bar.fill_char == "="
        assert bar.empty_char == " "

    def test_create_custom(self) -> None:
        """Test creating custom progress bar."""
        bar = ProgressBar(total=50, width=30, prefix="Loading")
        assert bar.total == 50
        assert bar.width == 30
        assert bar.prefix == "Loading"

    def test_minimum_width(self) -> None:
        """Test minimum width is enforced."""
        bar = ProgressBar(width=5)
        assert bar.width == 10

    def test_update(self) -> None:
        """Test updating progress bar."""
        bar = ProgressBar(total=100)
        bar.update(50)
        assert bar.current == 50

    def test_update_specific(self) -> None:
        """Test update with specific value."""
        bar = ProgressBar(total=100)
        bar.update(75)
        assert bar.current == 75

    def test_finish(self) -> None:
        """Test finishing progress bar."""
        bar = ProgressBar(total=100)
        bar.update(50)
        bar.finish()
        assert bar.current == 100

    def test_context_manager(self) -> None:
        """Test using progress bar as context manager."""
        with ProgressBar(total=10) as bar:
            bar.update(5)
            assert bar.current == 5
        assert bar.current == 10

    def test_zero_total(self) -> None:
        """Test progress bar with zero total."""
        bar = ProgressBar(total=0)
        bar.update(0)
        assert bar.current == 0


class TestTable:
    """Tests for Table."""

    def test_create(self) -> None:
        """Test creating table."""
        table = Table(["Name", "Age", "City"])
        assert len(table.headers) == 3
        assert table.headers == ["Name", "Age", "City"]
        assert table.rows == []

    def test_create_with_align(self) -> None:
        """Test creating table with alignment."""
        table = Table(["Name", "Age"], align=["l", "r"])
        assert table.align == ["l", "r"]

    def test_add_row(self) -> None:
        """Test adding row to table."""
        table = Table(["Name", "Age"])
        table.add_row(["Alice", "30"])
        assert len(table.rows) == 1
        assert table.rows[0] == ["Alice", "30"]

    def test_add_row_length_mismatch(self) -> None:
        """Test adding row with wrong length."""
        table = Table(["Name", "Age"])
        with pytest.raises(ValueError):
            table.add_row(["Alice"])

    def test_str(self) -> None:
        """Test string representation."""
        table = Table(["Name", "Age"])
        table.add_row(["Alice", "30"])
        table.add_row(["Bob", "25"])
        result = str(table)
        assert "Name" in result
        assert "Age" in result
        assert "Alice" in result
        assert "Bob" in result

    def test_str_with_align(self) -> None:
        """Test string with alignment."""
        table = Table(["Name", "Score"], align=["l", "r"])
        table.add_row(["Alice", "100"])
        result = str(table)
        assert "Name" in result
        assert "Score" in result

    def test_column_widths(self) -> None:
        """Test column widths update correctly."""
        table = Table(["Name", "Age"])
        table.add_row(["Alice", "30"])
        table.add_row(["Alexander", "25"])
        assert len(table.rows) == 2


class TestConfirm:
    """Tests for confirm function."""
    # Note: confirm uses input(), tested separately with mocking

    def test_confirm_import(self) -> None:
        """Test confirm can be imported."""
        from utils.terminal import confirm
        assert callable(confirm)


class TestSelect:
    """Tests for select function."""
    # Note: select uses input(), tested separately with mocking

    def test_select_import(self) -> None:
        """Test select can be imported."""
        from utils.terminal import select
        assert callable(select)

    def test_select_empty_raises(self) -> None:
        """Test select with empty options raises."""
        from utils.terminal import select
        with pytest.raises(ValueError):
            select([])


class TestPrintBox:
    """Tests for print_box function."""

    def test_print_box_import(self) -> None:
        """Test print_box can be imported."""
        from utils.terminal import print_box
        assert callable(print_box)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])