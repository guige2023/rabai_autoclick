"""Tests for CLI utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.cli_utils import (
    print_info,
    print_success,
    print_warning,
    print_error,
    print_debug,
    confirm,
    prompt,
    prompt_choices,
    print_progress_bar,
    print_table,
    clear_screen,
    print_banner,
    print_header,
    print_divider,
    get_input,
    parse_args,
    is_interactive,
    beep,
    select_items,
)


class TestPrintFunctions:
    """Tests for print functions."""

    def test_print_info(self, capsys) -> None:
        """Test print info."""
        print_info("test message")
        captured = capsys.readouterr()
        assert "[INFO] test message" in captured.out

    def test_print_success(self, capsys) -> None:
        """Test print success."""
        print_success("test message")
        captured = capsys.readouterr()
        assert "[SUCCESS] test message" in captured.out

    def test_print_warning(self, capsys) -> None:
        """Test print warning."""
        print_warning("test message")
        captured = capsys.readouterr()
        assert "[WARNING] test message" in captured.out

    def test_print_error(self, capsys) -> None:
        """Test print error."""
        print_error("test message")
        captured = capsys.readouterr()
        assert "[ERROR] test message" in captured.out

    def test_print_debug(self, capsys) -> None:
        """Test print debug."""
        print_debug("test message")
        captured = capsys.readouterr()
        assert "[DEBUG] test message" in captured.out


class TestConfirm:
    """Tests for confirm function."""

    def test_confirm_default_yes(self, monkeypatch) -> None:
        """Test confirm default yes."""
        monkeypatch.setattr('builtins.input', lambda x: "")
        result = confirm("Continue?", default=True)
        assert result is True

    def test_confirm_default_no(self, monkeypatch) -> None:
        """Test confirm default no."""
        monkeypatch.setattr('builtins.input', lambda x: "")
        result = confirm("Continue?", default=False)
        assert result is False


class TestPrompt:
    """Tests for prompt function."""

    def test_prompt_with_default(self, monkeypatch) -> None:
        """Test prompt with default."""
        monkeypatch.setattr('builtins.input', lambda x: "")
        result = prompt("Enter name:", default="John")
        assert result == "John"

    def test_prompt_no_default(self, monkeypatch) -> None:
        """Test prompt without default."""
        monkeypatch.setattr('builtins.input', lambda x: "Jane")
        result = prompt("Enter name:")
        assert result == "Jane"


class TestPromptChoices:
    """Tests for prompt_choices function."""

    def test_prompt_choices(self, monkeypatch) -> None:
        """Test prompt choices."""
        choices = ["A", "B", "C"]
        monkeypatch.setattr('builtins.input', lambda x: "2")
        result = prompt_choices("Choose:", choices)
        assert result == "B"


class TestPrintProgressBar:
    """Tests for print_progress_bar function."""

    def test_print_progress_bar(self, capsys) -> None:
        """Test print progress bar."""
        print_progress_bar(50, 100)
        captured = capsys.readouterr()
        assert "50.0%" in captured.out


class TestPrintTable:
    """Tests for print_table function."""

    def test_print_table(self, capsys) -> None:
        """Test print table."""
        headers = ["Name", "Age"]
        rows = [["John", "30"], ["Jane", "25"]]
        print_table(headers, rows)
        captured = capsys.readouterr()
        assert "Name" in captured.out
        assert "Age" in captured.out


class TestClearScreen:
    """Tests for clear_screen function."""

    def test_clear_screen(self) -> None:
        """Test clear screen."""
        clear_screen()


class TestPrintBanner:
    """Tests for print_banner function."""

    def test_print_banner(self, capsys) -> None:
        """Test print banner."""
        print_banner("TEST")
        captured = capsys.readouterr()
        assert "TEST" in captured.out


class TestPrintHeader:
    """Tests for print_header function."""

    def test_print_header(self, capsys) -> None:
        """Test print header."""
        print_header("Section")
        captured = capsys.readouterr()
        assert "Section" in captured.out


class TestPrintDivider:
    """Tests for print_divider function."""

    def test_print_divider(self, capsys) -> None:
        """Test print divider."""
        print_divider()
        captured = capsys.readouterr()
        assert "-" in captured.out


class TestGetInput:
    """Tests for get_input function."""

    def test_get_input_no_validator(self, monkeypatch) -> None:
        """Test get_input without validator."""
        monkeypatch.setattr('builtins.input', lambda x: "test")
        result = get_input("Enter:")
        assert result == "test"

    def test_get_input_with_validator(self, monkeypatch) -> None:
        """Test get_input with validator."""
        monkeypatch.setattr('builtins.input', lambda x: "test")
        result = get_input("Enter:", validator=lambda x: None)
        assert result == "test"


class TestParseArgs:
    """Tests for parse_args function."""

    def test_parse_args_simple(self) -> None:
        """Test parsing simple args."""
        args = ["--name", "John", "--age", "30"]
        spec = {"name": "", "age": ""}
        result = parse_args(args, spec)
        assert result["name"] == "John"
        assert result["age"] == "30"

    def test_parse_args_flag(self) -> None:
        """Test parsing flag args."""
        args = ["--verbose"]
        spec = {"verbose": ""}
        result = parse_args(args, spec)
        assert result["verbose"] is True


class TestIsInteractive:
    """Tests for is_interactive function."""

    def test_is_interactive(self) -> None:
        """Test is_interactive."""
        result = is_interactive()
        assert isinstance(result, bool)


class TestBeep:
    """Tests for beep function."""

    def test_beep(self) -> None:
        """Test beep."""
        beep()


class TestSelectItems:
    """Tests for select_items function."""

    def test_select_items_single(self, monkeypatch) -> None:
        """Test selecting single item."""
        items = ["A", "B", "C"]
        inputs = iter(["2", "0"])
        monkeypatch.setattr('builtins.input', lambda x: next(inputs))
        result = select_items(items, multi=False)
        assert result == ["B"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
