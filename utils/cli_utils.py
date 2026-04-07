"""CLI utilities for RabAI AutoClick.

Provides:
- Command-line interface helpers
- Input validation
- Progress display
"""

import sys
import getpass
from typing import Any, Callable, List, Optional


def print_info(message: str) -> None:
    """Print info message.

    Args:
        message: Message to print.
    """
    print(f"[INFO] {message}")


def print_success(message: str) -> None:
    """Print success message.

    Args:
        message: Message to print.
    """
    print(f"[SUCCESS] {message}")


def print_warning(message: str) -> None:
    """Print warning message.

    Args:
        message: Message to print.
    """
    print(f"[WARNING] {message}")


def print_error(message: str) -> None:
    """Print error message.

    Args:
        message: Message to print.
    """
    print(f"[ERROR] {message}")


def print_debug(message: str) -> None:
    """Print debug message.

    Args:
        message: Message to print.
    """
    print(f"[DEBUG] {message}")


def confirm(message: str, default: bool = False) -> bool:
    """Ask for confirmation.

    Args:
        message: Prompt message.
        default: Default value.

    Returns:
        True if confirmed.
    """
    suffix = " [Y/n]: " if default else " [y/N]: "
    while True:
        response = input(message + suffix).lower().strip()
        if not response:
            return default
        if response in ('y', 'yes'):
            return True
        if response in ('n', 'no'):
            return False
        print("Please enter 'y' or 'n'")


def prompt(message: str, default: str = None) -> str:
    """Prompt for input.

    Args:
        message: Prompt message.
        default: Default value.

    Returns:
        User input or default.
    """
    if default:
        response = input(f"{message} [{default}]: ").strip()
        return response or default
    return input(f"{message}: ").strip()


def prompt_password(message: str) -> str:
    """Prompt for password input.

    Args:
        message: Prompt message.

    Returns:
        Password input.
    """
    return getpass.getpass(f"{message}: ")


def prompt_choices(message: str, choices: List[str], default: int = 0) -> str:
    """Prompt user to choose from list.

    Args:
        message: Prompt message.
        choices: List of choices.
        default: Default choice index.

    Returns:
        Selected choice.
    """
    print(message)
    for i, choice in enumerate(choices):
        marker = "*" if i == default else " "
        print(f"  {marker} {i + 1}. {choice}")
    while True:
        try:
            response = input("Enter choice number: ").strip()
            if not response:
                return choices[default]
            index = int(response) - 1
            if 0 <= index < len(choices):
                return choices[index]
            print(f"Please enter a number between 1 and {len(choices)}")
        except ValueError:
            print("Please enter a valid number")


def print_progress_bar(iteration: int, total: int, prefix: str = "", length: int = 50, fill: str = "█") -> None:
    """Print progress bar.

    Args:
        iteration: Current iteration.
        total: Total iterations.
        prefix: Prefix string.
        length: Bar length.
        fill: Fill character.
    """
    percent = f"{100 * (iteration / float(total)):.1f}"
    filled_length = int(length * iteration // total)
    bar = fill * filled_length + '-' * (length - filled_length)
    print(f"\r{prefix} |{bar}| {percent}% Complete", end='\r')
    if iteration == total:
        print()


def print_table(headers: List[str], rows: List[List[Any]]) -> None:
    """Print data as table.

    Args:
        headers: Column headers.
        rows: Data rows.
    """
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))

    def format_row(cells):
        return " | ".join(str(cell).ljust(width) for cell, width in zip(cells, col_widths))

    separator = "-+-".join("-" * width for width in col_widths)
    print(separator)
    print(format_row(headers))
    print(separator)
    for row in rows:
        print(format_row(row))
    print(separator)


def clear_screen() -> None:
    """Clear the terminal screen."""
    import os
    os.system('cls' if os.name == 'nt' else 'clear')


def print_banner(text: str, width: int = 60) -> None:
    """Print banner.

    Args:
        text: Banner text.
        width: Banner width.
    """
    print("=" * width)
    print(text.center(width))
    print("=" * width)


def print_header(text: str) -> None:
    """Print section header.

    Args:
        text: Header text.
    """
    print()
    print(f"=== {text} ===")
    print()


def print_divider(char: str = "-", length: int = 60) -> None:
    """Print divider line.

    Args:
        char: Divider character.
        length: Divider length.
    """
    print(char * length)


def get_input(prompt_text: str, validator: Callable[[str], Optional[str]] = None) -> str:
    """Get validated input.

    Args:
        prompt_text: Prompt to show.
        validator: Optional validator function that returns error message or None.

    Returns:
        Validated input.
    """
    while True:
        response = input(prompt_text).strip()
        if validator:
            error = validator(response)
            if error:
                print(f"Error: {error}")
                continue
        return response


def parse_args(args: List[str], spec: dict) -> dict:
    """Parse command-line arguments.

    Args:
        args: Arguments list.
        spec: Specification dict mapping flags to help text.

    Returns:
        Parsed arguments dict.
    """
    result = {}
    i = 0
    while i < len(args):
        arg = args[i]
        if arg.startswith('--'):
            flag = arg[2:]
            if flag in spec:
                if '=' in flag:
                    key, value = flag.split('=', 1)
                    result[key] = value
                else:
                    if i + 1 < len(args) and not args[i + 1].startswith('--'):
                        result[flag] = args[i + 1]
                        i += 1
                    else:
                        result[flag] = True
        i += 1
    return result


def is_interactive() -> bool:
    """Check if running in interactive mode.

    Returns:
        True if interactive.
    """
    return sys.stdin.isatty()


def beep() -> None:
    """Play beep sound."""
    import os
    if os.name == 'nt':
        import winsound
        winsound.Beep(1000, 200)
    else:
        sys.stdout.write('\a')
        sys.stdout.flush()


def progress_callback(iteration: int, total: int, start_time: float = None) -> None:
    """Print progress with elapsed time.

    Args:
        iteration: Current iteration.
        total: Total iterations.
        start_time: Start time from time.time().
    """
    import time
    if start_time:
        elapsed = time.time() - start_time
        rate = iteration / elapsed if elapsed > 0 else 0
        eta = (total - iteration) / rate if rate > 0 else 0
        print(f"Progress: {iteration}/{total} ({100 * iteration / total:.1f}%) - Elapsed: {elapsed:.1f}s - ETA: {eta:.1f}s", end='\r')
    else:
        print_progress_bar(iteration, total)


def multiline_input(prompt_text: str, terminator: str = ".") -> str:
    """Get multiline input.

    Args:
        prompt_text: Initial prompt.
        terminator: Line that ends input.

    Returns:
        Multiline input string.
    """
    print(prompt_text)
    lines = []
    while True:
        line = input()
        if line == terminator:
            break
        lines.append(line)
    return '\n'.join(lines)


def select_items(items: List[str], message: str = "Select items:", multi: bool = False) -> List[str]:
    """Select items from list.

    Args:
        items: Items to select from.
        message: Selection prompt.
        multi: Allow multiple selections.

    Returns:
        Selected items.
    """
    print(message)
    for i, item in enumerate(items):
        print(f"  {i + 1}. {item}")
    print("  0. Done")

    selected = []
    while True:
        try:
            choice = input("Enter number: ").strip()
            if not choice:
                continue
            num = int(choice)
            if num == 0:
                break
            if 1 <= num <= len(items):
                item = items[num - 1]
                if multi:
                    if item in selected:
                        selected.remove(item)
                        print(f"Deselected: {item}")
                    else:
                        selected.append(item)
                        print(f"Selected: {item}")
                else:
                    selected = [item]
                    break
            else:
                print(f"Invalid choice: {num}")
        except ValueError:
            print("Please enter a number")

    return selected
