"""Format utilities for RabAI AutoClick.

Provides:
- Data formatting helpers
- Number and text formatting
- Size and duration formatting
"""

from typing import Any, List, Dict


def format_bytes(size: int, precision: int = 2) -> str:
    """Format bytes as human-readable string.

    Args:
        size: Number of bytes.
        precision: Decimal precision.

    Returns:
        Formatted string (e.g., "1.5 MB").
    """
    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    unit_index = 0
    size_float = float(size)

    while size_float >= 1024 and unit_index < len(units) - 1:
        size_float /= 1024
        unit_index += 1

    if unit_index == 0:
        precision = 0
    return f"{size_float:.{precision}f} {units[unit_index]}"


def parse_bytes(size_str: str) -> int:
    """Parse human-readable bytes to integer.

    Args:
        size_str: String like "1.5 MB".

    Returns:
        Number of bytes.
    """
    units = [('PB', 1024**5), ('TB', 1024**4), ('GB', 1024**3), ('MB', 1024**2), ('KB', 1024), ('B', 1)]
    size_str = size_str.upper().strip()
    for unit, multiplier in units:
        if unit in size_str:
            number = float(size_str.replace(unit, '').strip())
            return int(number * multiplier)
    return int(float(size_str))


def format_duration(seconds: float) -> str:
    """Format duration in seconds as human-readable string.

    Args:
        seconds: Duration in seconds.

    Returns:
        Formatted string (e.g., "1h 30m").
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    seconds = seconds % 60
    if minutes < 60:
        return f"{minutes}m {seconds:.0f}s"
    hours = minutes // 60
    minutes = minutes % 60
    return f"{hours}h {minutes}m"


def format_duration_long(seconds: float) -> str:
    """Format duration in full form.

    Args:
        seconds: Duration in seconds.

    Returns:
        Formatted string (e.g., "2 hours, 30 minutes").
    """
    if seconds < 60:
        return f"{int(seconds)} seconds"
    minutes = int(seconds // 60)
    seconds = int(seconds % 60)
    if minutes < 60:
        parts = []
        if minutes > 0:
            parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
        if seconds > 0:
            parts.append(f"{seconds} second{'s' if seconds != 1 else ''}")
        return ', '.join(parts)
    hours = minutes // 60
    minutes = minutes % 60
    parts = []
    if hours > 0:
        parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
    if minutes > 0:
        parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
    return ', '.join(parts)


def format_number(num: float, precision: int = 2) -> str:
    """Format number with thousands separator.

    Args:
        num: Number to format.
        precision: Decimal precision.

    Returns:
        Formatted string (e.g., "1,234.56").
    """
    if precision > 0:
        return f"{num:,.{precision}f}"
    return f"{int(num):,}"


def parse_number(num_str: str) -> float:
    """Parse formatted number string.

    Args:
        num_str: String with number.

    Returns:
        Parsed number.
    """
    return float(num_str.replace(',', ''))


def format_percent(value: float, total: float, precision: int = 1) -> str:
    """Format value as percentage of total.

    Args:
        value: Numerator value.
        total: Denominator value.
        precision: Decimal precision.

    Returns:
        Formatted percentage (e.g., "50.0%").
    """
    if total == 0:
        return "0%"
    percent = (value / total) * 100
    return f"{percent:.{precision}f}%"


def format_ratio(value: float, total: float, precision: int = 2) -> str:
    """Format value as ratio of total.

    Args:
        value: Numerator value.
        total: Denominator value.
        precision: Decimal precision.

    Returns:
        Formatted ratio (e.g., "1/2").
    """
    if total == 0:
        return "0/0"
    return f"{value:.{precision}f}/{total:.{precision}f}"


def truncate_string(text: str, max_length: int, suffix: str = "...") -> str:
    """Truncate string to max length.

    Args:
        text: Text to truncate.
        max_length: Maximum length.
        suffix: Suffix to append if truncated.

    Returns:
        Truncated string.
    """
    if len(text) <= max_length:
        return text
    return text[:max_length - len(suffix)] + suffix


def pad_string(text: str, width: int, char: str = " ", align: str = "left") -> str:
    """Pad string to width.

    Args:
        text: Text to pad.
        width: Target width.
        char: Character to pad with.
        align: Alignment (left, right, center).

    Returns:
        Padded string.
    """
    if align == "left":
        return text.ljust(width, char)
    elif align == "right":
        return text.rjust(width, char)
    else:
        return text.center(width, char)


def wrap_text(text: str, width: int) -> List[str]:
    """Wrap text to specified width.

    Args:
        text: Text to wrap.
        width: Maximum line width.

    Returns:
        List of wrapped lines.
    """
    words = text.split()
    lines = []
    current_line = []
    current_length = 0

    for word in words:
        word_len = len(word)
        if current_length + word_len + len(current_line) <= width:
            current_line.append(word)
            current_length += word_len
        else:
            if current_line:
                lines.append(' '.join(current_line))
            current_line = [word]
            current_length = word_len

    if current_line:
        lines.append(' '.join(current_line))

    return lines


def indent_text(text: str, spaces: int, indent_first: bool = True) -> str:
    """Indent text by spaces.

    Args:
        text: Text to indent.
        spaces: Number of spaces to indent.
        indent_first: Whether to indent first line.

    Returns:
        Indented text.
    """
    lines = text.splitlines()
    if not lines:
        return text
    indent_str = ' ' * spaces
    if indent_first:
        return '\n'.join(indent_str + line for line in lines)
    return lines[0] + '\n' + '\n'.join(indent_str + line for line in lines[1:])


def format_table(headers: List[str], rows: List[List[Any]], padding: int = 2) -> str:
    """Format data as ASCII table.

    Args:
        headers: Column headers.
        rows: Data rows.
        padding: Space between columns.

    Returns:
        Formatted table string.
    """
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))

    def format_row(cells):
        return ' ' * padding + (' ' * padding).join(
            str(cell).ljust(width) for cell, width in zip(cells, col_widths)
        )

    separator = '+' + '+'.join('-' * (w + padding * 2) for w in col_widths) + '+'
    header_line = '|' + format_row(headers).center(sum(col_widths) + padding * (len(headers) - 1)) + '|'

    lines = [separator, header_line, separator]
    for row in rows:
        lines.append('|' + format_row(row) + '|')
    lines.append(separator)

    return '\n'.join(lines)


def format_list(items: List[Any], separator: str = ", ", last_separator: str = " and ") -> str:
    """Format list as human-readable string.

    Args:
        items: List of items.
        separator: Separator between items.
        last_separator: Separator before last item.

    Returns:
        Formatted string (e.g., "a, b and c").
    """
    if not items:
        return ""
    if len(items) == 1:
        return str(items[0])
    if len(items) == 2:
        return f"{items[0]}{last_separator}{items[1]}"
    return separator.join(str(x) for x in items[:-1]) + last_separator + str(items[-1])


def format_phone(phone: str, format_type: str = "US") -> str:
    """Format phone number.

    Args:
        phone: Raw phone number.
        format_type: Format type (US, UK, etc).

    Returns:
        Formatted phone number.
    """
    digits = ''.join(c for c in phone if c.isdigit())
    if format_type == "US" and len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    elif format_type == "UK" and len(digits) == 10:
        return f"{digits[:4]} {digits[4:]}"
    return phone


def format_credit_card(card: str) -> str:
    """Format credit card number.

    Args:
        card: Raw card number.

    Returns:
        Formatted card number (e.g., "1234 5678 9012 3456").
    """
    digits = ''.join(c for c in card if c.isdigit())
    return ' '.join(digits[i:i+4] for i in range(0, len(digits), 4))


def format_ssn(ssn: str) -> str:
    """Format Social Security Number.

    Args:
        ssn: Raw SSN.

    Returns:
        Formatted SSN (e.g., "123-45-6789").
    """
    digits = ''.join(c for c in ssn if c.isdigit())
    if len(digits) == 9:
        return f"{digits[:3]}-{digits[3:5]}-{digits[5:]}"
    return ssn


def format_zip_code(zip_code: str) -> str:
    """Format ZIP code.

    Args:
        zip_code: Raw ZIP code.

    Returns:
        Formatted ZIP code.
    """
    digits = ''.join(c for c in zip_code if c.isdigit())
    if len(digits) == 9:
        return f"{digits[:5]}-{digits[5:]}"
    return digits[:5] if len(digits) >= 5 else digits


def pluralize(word: str, count: int, plural_form: str = None) -> str:
    """Pluralize a word based on count.

    Args:
        word: Singular form.
        count: Count to determine plural.
        plural_form: Optional plural form.

    Returns:
        Singular or plural word.
    """
    if count == 1:
        return word
    if plural_form:
        return plural_form
    if word.endswith('y') and len(word) > 1 and word[-2] not in 'aeiou':
        return word[:-1] + 'ies'
    if word.endswith(('s', 'x', 'z', 'ch', 'sh')):
        return word + 'es'
    return word + 's'


def title_case(text: str) -> str:
    """Convert text to title case.

    Args:
        text: Text to convert.

    Returns:
        Title cased text.
    """
    return text.title()


def snake_to_camel(text: str) -> str:
    """Convert snake_case to camelCase.

    Args:
        text: Snake case text.

    Returns:
        Camel case text.
    """
    components = text.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])


def camel_to_snake(text: str) -> str:
    """Convert camelCase to snake_case.

    Args:
        text: Camel case text.

    Returns:
        Snake case text.
    """
    import re
    pattern = re.compile(r'(?<!^)(?=[A-Z])')
    return pattern.sub('_', text).lower()


def kebab_to_snake(text: str) -> str:
    """Convert kebab-case to snake_case.

    Args:
        text: Kebab case text.

    Returns:
        Snake case text.
    """
    return text.replace('-', '_')


def snake_to_kebab(text: str) -> str:
    """Convert snake_case to kebab-case.

    Args:
        text: Snake case text.

    Returns:
        Kebab case text.
    """
    return text.replace('_', '-')


def format_boolean(value: bool, true_str: str = "Yes", false_str: str = "No") -> str:
    """Format boolean as string.

    Args:
        value: Boolean value.
        true_str: String for True.
        false_str: String for False.

    Returns:
        Formatted string.
    """
    return true_str if value else false_str


def format_currency(amount: float, currency: str = "USD", symbol: str = "$") -> str:
    """Format currency amount.

    Args:
        amount: Amount to format.
        currency: Currency code.
        symbol: Currency symbol.

    Returns:
        Formatted currency string.
    """
    return f"{symbol}{amount:,.2f}"


def format_temperature(celsius: float, unit: str = "C") -> str:
    """Format temperature.

    Args:
        celsius: Temperature in Celsius.
        unit: Unit to display (C, F, or both).

    Returns:
        Formatted temperature.
    """
    if unit == "C":
        return f"{celsius:.1f}°C"
    elif unit == "F":
        fahrenheit = (celsius * 9/5) + 32
        return f"{fahrenheit:.1f}°F"
    else:
        fahrenheit = (celsius * 9/5) + 32
        return f"{celsius:.1f}°C ({fahrenheit:.1f}°F)"
