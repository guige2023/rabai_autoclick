"""
INI file parsing and manipulation actions.
"""
from __future__ import annotations

import configparser
from pathlib import Path
from typing import Dict, Any, Optional, List


def read_ini_file(file_path: str) -> configparser.ConfigParser:
    """
    Read an INI file.

    Args:
        file_path: Path to the INI file.

    Returns:
        ConfigParser object.

    Raises:
        FileNotFoundError: If file does not exist.
    """
    if not Path(file_path).exists():
        raise FileNotFoundError(f"INI file not found: {file_path}")

    parser = configparser.ConfigParser()
    parser.read(file_path)

    return parser


def parse_ini_string(ini_string: str) -> configparser.ConfigParser:
    """
    Parse an INI string.

    Args:
        ini_string: INI format string.

    Returns:
        ConfigParser object.
    """
    parser = configparser.ConfigParser()
    parser.read_string(ini_string)

    return parser


def get_ini_value(
    file_path: str,
    section: str,
    key: str,
    default: Optional[str] = None
) -> Optional[str]:
    """
    Get a value from an INI file.

    Args:
        file_path: Path to the INI file.
        section: Section name.
        key: Key name.
        default: Default value if not found.

    Returns:
        Value or default.
    """
    try:
        parser = read_ini_file(file_path)
        return parser.get(section, key)
    except (configparser.NoSectionError, configparser.NoOptionError):
        return default


def get_ini_section(file_path: str, section: str) -> Dict[str, str]:
    """
    Get all values from a section.

    Args:
        file_path: Path to the INI file.
        section: Section name.

    Returns:
        Dictionary of key-value pairs.
    """
    try:
        parser = read_ini_file(file_path)
        return dict(parser.items(section))
    except configparser.NoSectionError:
        return {}


def get_ini_sections(file_path: str) -> List[str]:
    """
    Get all section names from an INI file.

    Args:
        file_path: Path to the INI file.

    Returns:
        List of section names.
    """
    parser = read_ini_file(file_path)
    return parser.sections()


def set_ini_value(
    file_path: str,
    section: str,
    key: str,
    value: str
) -> None:
    """
    Set a value in an INI file.

    Args:
        file_path: Path to the INI file.
        section: Section name.
        key: Key name.
        value: Value to set.
    """
    parser = configparser.ConfigParser()

    if Path(file_path).exists():
        parser.read(file_path)

    if not parser.has_section(section):
        parser.add_section(section)

    parser.set(section, key, value)

    with open(file_path, 'w') as f:
        parser.write(f)


def remove_ini_key(
    file_path: str,
    section: str,
    key: str
) -> bool:
    """
    Remove a key from an INI file.

    Args:
        file_path: Path to the INI file.
        section: Section name.
        key: Key name.

    Returns:
        True if key was removed.
    """
    parser = read_ini_file(file_path)

    if not parser.has_section(section):
        return False

    if not parser.has_option(section, key):
        return False

    parser.remove_option(section, key)

    with open(file_path, 'w') as f:
        parser.write(f)

    return True


def remove_ini_section(file_path: str, section: str) -> bool:
    """
    Remove a section from an INI file.

    Args:
        file_path: Path to the INI file.
        section: Section name.

    Returns:
        True if section was removed.
    """
    parser = read_ini_file(file_path)

    if not parser.has_section(section):
        return False

    parser.remove_section(section)

    with open(file_path, 'w') as f:
        parser.write(f)

    return True


def create_ini_file(
    file_path: str,
    sections: Dict[str, Dict[str, str]]
) -> None:
    """
    Create an INI file from a dictionary.

    Args:
        file_path: Path to create.
        sections: Dictionary of sections with key-value pairs.
    """
    parser = configparser.ConfigParser()

    for section, values in sections.items():
        parser.add_section(section)
        for key, value in values.items():
            parser.set(section, key, value)

    Path(file_path).parent.mkdir(parents=True, exist_ok=True)

    with open(file_path, 'w') as f:
        parser.write(f)


def ini_to_dict(file_path: str) -> Dict[str, Dict[str, str]]:
    """
    Convert an INI file to a nested dictionary.

    Args:
        file_path: Path to the INI file.

    Returns:
        Nested dictionary representation.
    """
    parser = read_ini_file(file_path)

    result: Dict[str, Dict[str, str]] = {}

    for section in parser.sections():
        result[section] = dict(parser.items(section))

    return result


def dict_to_ini(
    data: Dict[str, Dict[str, str]],
    file_path: str
) -> None:
    """
    Write a dictionary to an INI file.

    Args:
        data: Nested dictionary.
        file_path: Output file path.
    """
    create_ini_file(file_path, data)


def merge_ini_files(
    base_path: str,
    override_path: str,
    output_path: str
) -> None:
    """
    Merge two INI files, with override values taking precedence.

    Args:
        base_path: Base INI file.
        override_path: Override INI file.
        output_path: Output merged file.
    """
    base = configparser.ConfigParser()
    base.read(base_path)

    override = configparser.ConfigParser()
    override.read(override_path)

    for section in override.sections():
        if not base.has_section(section):
            base.add_section(section)

        for key, value in override.items(section):
            base.set(section, key, value)

    with open(output_path, 'w') as f:
        base.write(f)


def has_ini_section(file_path: str, section: str) -> bool:
    """
    Check if an INI file has a section.

    Args:
        file_path: Path to the INI file.
        section: Section name.

    Returns:
        True if section exists.
    """
    parser = read_ini_file(file_path)
    return parser.has_section(section)


def has_ini_key(
    file_path: str,
    section: str,
    key: str
) -> bool:
    """
    Check if an INI file has a key in a section.

    Args:
        file_path: Path to the INI file.
        section: Section name.
        key: Key name.

    Returns:
        True if key exists.
    """
    parser = read_ini_file(file_path)
    return parser.has_option(section, key)


def get_ini_int(
    file_path: str,
    section: str,
    key: str,
    default: Optional[int] = None
) -> Optional[int]:
    """
    Get an integer value from an INI file.

    Args:
        file_path: Path to the INI file.
        section: Section name.
        key: Key name.
        default: Default value if not found.

    Returns:
        Integer value or default.
    """
    try:
        parser = read_ini_file(file_path)
        return parser.getint(section, key)
    except (configparser.NoSectionError, configparser.NoOptionError, ValueError):
        return default


def get_ini_float(
    file_path: str,
    section: str,
    key: str,
    default: Optional[float] = None
) -> Optional[float]:
    """
    Get a float value from an INI file.

    Args:
        file_path: Path to the INI file.
        section: Section name.
        key: Key name.
        default: Default value if not found.

    Returns:
        Float value or default.
    """
    try:
        parser = read_ini_file(file_path)
        return parser.getfloat(section, key)
    except (configparser.NoSectionError, configparser.NoOptionError, ValueError):
        return default


def get_ini_bool(
    file_path: str,
    section: str,
    key: str,
    default: Optional[bool] = None
) -> Optional[bool]:
    """
    Get a boolean value from an INI file.

    Args:
        file_path: Path to the INI file.
        section: Section name.
        key: Key name.
        default: Default value if not found.

    Returns:
        Boolean value or default.
    """
    try:
        parser = read_ini_file(file_path)
        return parser.getboolean(section, key)
    except (configparser.NoSectionError, configparser.NoOptionError, ValueError):
        return default
