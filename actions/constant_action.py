"""
Constant values and configuration actions.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional


# Mathematical constants
MATH_PI = 3.141592653589793
MATH_E = 2.718281828459045
MATH_TAU = 6.283185307179586
MATH_GOLDEN_RATIO = 1.618033988749895
MATH_EPSILON = 1e-10

# Physical constants
SPEED_OF_LIGHT = 299792458  # m/s
PLANCK_CONSTANT = 6.62607015e-34  # J*s
BOLTZMANN_CONSTANT = 1.380649e-23  # J/K
AVOGADRO_NUMBER = 6.02214076e23  # 1/mol
GRAVITATIONAL_CONSTANT = 6.67430e-11  # m^3/(kg*s^2)
SPEED_OF_SOUND_AIR = 343  # m/s at 20C

# Time constants
SECONDS_PER_MINUTE = 60
SECONDS_PER_HOUR = 3600
SECONDS_PER_DAY = 86400
SECONDS_PER_WEEK = 604800
SECONDS_PER_YEAR = 31536000
MINUTES_PER_HOUR = 60
HOURS_PER_DAY = 24
DAYS_PER_WEEK = 7
DAYS_PER_YEAR = 365
DAYS_PER_LEAP_YEAR = 366

# Data size constants
BYTES_PER_KB = 1024
BYTES_PER_MB = 1048576
BYTES_PER_GB = 1073741824
BYTES_PER_TB = 1099511627776
KB_PER_MB = 1024
MB_PER_GB = 1024
GB_PER_TB = 1024

# HTTP status codes
HTTP_STATUS_CODES = {
    200: 'OK',
    201: 'Created',
    204: 'No Content',
    301: 'Moved Permanently',
    302: 'Found',
    304: 'Not Modified',
    400: 'Bad Request',
    401: 'Unauthorized',
    403: 'Forbidden',
    404: 'Not Found',
    405: 'Method Not Allowed',
    409: 'Conflict',
    422: 'Unprocessable Entity',
    429: 'Too Many Requests',
    500: 'Internal Server Error',
    502: 'Bad Gateway',
    503: 'Service Unavailable',
    504: 'Gateway Timeout',
}

# Boolean constants
TRUE_VALUES = {True, 'true', 'True', 'TRUE', '1', 'yes', 'Yes', 'YES', 'on', 'On', 'ON'}
FALSE_VALUES = {False, 'false', 'False', 'FALSE', '0', 'no', 'No', 'NO', 'off', 'Off', 'OFF', ''}

# Weekday constants
WEEKDAYS = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
WEEKDAYS_SHORT = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
WEEKDAYS_3 = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

# Month constants
MONTHS = [
    'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December'
]
MONTHS_SHORT = [
    'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
    'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'
]

# ASCII constants
ASCII_CONTROL = list(range(32)) + [127]
ASCII_PRINTABLE = list(range(32, 127))
ASCII_LETTERS = list(range(65, 91)) + list(range(97, 123))
ASCII_UPPERCASE = list(range(65, 91))
ASCII_LOWERCASE = list(range(97, 123))
ASCII_DIGITS = list(range(48, 58))

# Common file extensions
FILE_EXTENSIONS = {
    'text': ['.txt', '.md', '.rst'],
    'code': ['.py', '.js', '.java', '.c', '.cpp', '.go', '.rs', '.rb'],
    'web': ['.html', '.css', '.xml', '.json', '.yaml', '.yml'],
    'image': ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.webp'],
    'video': ['.mp4', '.avi', '.mov', '.mkv', '.webm'],
    'audio': ['.mp3', '.wav', '.flac', '.aac', '.ogg', '.m4a'],
    'document': ['.pdf', '.doc', '.docx', '.odt', '.rtf'],
    'spreadsheet': ['.xls', '.xlsx', '.csv', '.ods'],
    'archive': ['.zip', '.tar', '.gz', '.bz2', '.7z', '.rar'],
}

# MIME types
MIME_TYPES = {
    'json': 'application/json',
    'xml': 'application/xml',
    'html': 'text/html',
    'text': 'text/plain',
    'css': 'text/css',
    'javascript': 'application/javascript',
    'pdf': 'application/pdf',
    'zip': 'application/zip',
    'gzip': 'application/gzip',
    'octet-stream': 'application/octet-stream',
    'form-data': 'multipart/form-data',
    'urlencoded': 'application/x-www-form-urlencoded',
}

# Country codes (ISO 3166-1 alpha-2)
COUNTRY_CODES = {
    'US': 'United States',
    'CN': 'China',
    'JP': 'Japan',
    'DE': 'Germany',
    'GB': 'United Kingdom',
    'FR': 'France',
    'IN': 'India',
    'IT': 'Italy',
    'CA': 'Canada',
    'AU': 'Australia',
    'BR': 'Brazil',
    'MX': 'Mexico',
    'RU': 'Russia',
    'KR': 'South Korea',
    'ES': 'Spain',
    'NL': 'Netherlands',
    'SE': 'Sweden',
    'CH': 'Switzerland',
    'PL': 'Poland',
    'BE': 'Belgium',
}

# Currency codes (ISO 4217)
CURRENCY_CODES = {
    'USD': ('US Dollar', '$'),
    'EUR': ('Euro', '€'),
    'GBP': ('British Pound', '£'),
    'JPY': ('Japanese Yen', '¥'),
    'CNY': ('Chinese Yuan', '¥'),
    'KRW': ('South Korean Won', '₩'),
    'INR': ('Indian Rupee', '₹'),
    'BRL': ('Brazilian Real', 'R$'),
    'CAD': ('Canadian Dollar', 'C$'),
    'AUD': ('Australian Dollar', 'A$'),
    'CHF': ('Swiss Franc', 'CHF'),
    'HKD': ('Hong Kong Dollar', 'HK$'),
    'SGD': ('Singapore Dollar', 'S$'),
    'SEK': ('Swedish Krona', 'kr'),
    'NOK': ('Norwegian Krone', 'kr'),
    'DKK': ('Danish Krone', 'kr'),
    'NZD': ('New Zealand Dollar', 'NZ$'),
    'ZAR': ('South African Rand', 'R'),
    'MXN': ('Mexican Peso', 'MX$'),
    'RUB': ('Russian Ruble', '₽'),
}

# Timezone abbreviations
TIMEZONE_ABBREV = {
    'EST': 'America/New_York',
    'CST': 'America/Chicago',
    'MST': 'America/Denver',
    'PST': 'America/Los_Angeles',
    'GMT': 'Europe/London',
    'CET': 'Europe/Paris',
    'JST': 'Asia/Tokyo',
    'CCT': 'Asia/Shanghai',
    'IST': 'Asia/Kolkata',
    'AEST': 'Australia/Sydney',
    'UTC': 'UTC',
}

# Common regex patterns
REGEX_PATTERNS = {
    'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
    'url': r'^https?://[^\s/$.?#].[^\s]*$',
    'ipv4': r'^(\d{1,3}\.){3}\d{1,3}$',
    'phone_us': r'^\+?1?\d{10}$',
    'phone_intl': r'^\+\d{1,3}\d{4,14}$',
    'zip_us': r'^\d{5}(-\d{4})?$',
    'hex_color': r'^#?([a-fA-F0-9]{6}|[a-fA-F0-9]{3})$',
    'uuid': r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
    'semver': r'^\d+\.\d+\.\d+$',
}

# SQL data types
SQL_TYPES = [
    'INT', 'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT',
    'DECIMAL', 'FLOAT', 'REAL', 'DOUBLE',
    'VARCHAR', 'CHAR', 'TEXT', 'NVARCHAR',
    'DATE', 'TIME', 'DATETIME', 'TIMESTAMP',
    'BOOLEAN', 'BIT',
    'BLOB', 'BINARY', 'VARBINARY',
    'JSON', 'XML',
]

# HTTP methods
HTTP_METHODS = ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD', 'OPTIONS', 'TRACE', 'CONNECT']

# Log levels
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL', 'FATAL']

# CLI color codes
CLI_COLORS = {
    'black': '\033[30m',
    'red': '\033[31m',
    'green': '\033[32m',
    'yellow': '\033[33m',
    'blue': '\033[34m',
    'magenta': '\033[35m',
    'cyan': '\033[36m',
    'white': '\033[37m',
    'reset': '\033[0m',
    'bold': '\033[1m',
    'dim': '\033[2m',
}


def get_constant(name: str) -> Any:
    """
    Get a constant value by name.

    Args:
        name: Constant name.

    Returns:
        Constant value or None.
    """
    return globals().get(name)


def get_all_constants() -> Dict[str, Any]:
    """
    Get all constant names and values.

    Returns:
        Dictionary of all constants.
    """
    return {k: v for k, v in globals().items() if k.isupper()}


def get_http_status_name(code: int) -> str:
    """
    Get HTTP status name from code.

    Args:
        code: HTTP status code.

    Returns:
        Status name.
    """
    return HTTP_STATUS_CODES.get(code, 'Unknown')


def get_mime_type(extension: str) -> str:
    """
    Get MIME type from file extension.

    Args:
        extension: File extension (e.g., '.json').

    Returns:
        MIME type string.
    """
    if not extension.startswith('.'):
        extension = '.' + extension

    mime = MIME_TYPES.get(extension.lstrip('.').lower())
    return mime or 'application/octet-stream'


def get_country_name(code: str) -> str:
    """
    Get country name from code.

    Args:
        code: ISO 3166-1 alpha-2 country code.

    Returns:
        Country name.
    """
    return COUNTRY_CODES.get(code.upper(), code)


def get_currency_info(code: str) -> tuple:
    """
    Get currency name and symbol from code.

    Args:
        code: ISO 4217 currency code.

    Returns:
        Tuple of (name, symbol).
    """
    return CURRENCY_CODES.get(code.upper(), (code, code))


def is_valid_http_status(code: int) -> bool:
    """
    Check if HTTP status code is valid.

    Args:
        code: HTTP status code.

    Returns:
        True if valid.
    """
    return code in HTTP_STATUS_CODES


def get_days_in_year(year: int) -> int:
    """
    Get number of days in a year.

    Args:
        year: Year.

    Returns:
        365 or 366.
    """
    if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0):
        return DAYS_PER_LEAP_YEAR
    return DAYS_PER_YEAR


def bytes_to_human(size_bytes: int) -> str:
    """
    Convert bytes to human-readable string.

    Args:
        size_bytes: Size in bytes.

    Returns:
        Human-readable string.
    """
    if size_bytes >= BYTES_PER_TB:
        return f'{size_bytes / BYTES_PER_TB:.2f} TB'
    elif size_bytes >= BYTES_PER_GB:
        return f'{size_bytes / BYTES_PER_GB:.2f} GB'
    elif size_bytes >= BYTES_PER_MB:
        return f'{size_bytes / BYTES_PER_MB:.2f} MB'
    elif size_bytes >= BYTES_PER_KB:
        return f'{size_bytes / BYTES_PER_KB:.2f} KB'
    return f'{size_bytes} B'


def human_to_bytes(size_str: str) -> int:
    """
    Convert human-readable size to bytes.

    Args:
        size_str: Size string (e.g., '1.5 GB').

    Returns:
        Size in bytes.
    """
    import re

    match = re.match(r'([\d.]+)\s*(B|KB|MB|GB|TB|KB|M|G|T)', size_str, re.IGNORECASE)
    if not match:
        return 0

    value = float(match.group(1))
    unit = match.group(2).upper()

    multipliers = {
        'B': 1,
        'KB': BYTES_PER_KB,
        'MB': BYTES_PER_MB,
        'GB': BYTES_PER_GB,
        'TB': BYTES_PER_TB,
    }

    return int(value * multipliers.get(unit, 1))
