"""
Currency conversion and formatting actions.
"""
from __future__ import annotations

from typing import Dict, Optional, List, Any


# Exchange rates relative to USD (fallback if no API available)
FALLBACK_RATES: Dict[str, float] = {
    'USD': 1.0,
    'EUR': 0.92,
    'GBP': 0.79,
    'JPY': 149.50,
    'CNY': 7.24,
    'AUD': 1.53,
    'CAD': 1.36,
    'CHF': 0.88,
    'HKD': 7.82,
    'SGD': 1.34,
    'INR': 83.12,
    'KRW': 1320.0,
    'MXN': 17.15,
    'BRL': 4.97,
    'RUB': 92.0,
    'ZAR': 18.50,
    'SEK': 10.42,
    'NOK': 10.55,
    'DKK': 6.87,
    'NZD': 1.63,
    'THB': 35.20,
    'MYR': 4.72,
    'PHP': 55.80,
    'IDR': 15650.0,
    'VND': 24500.0,
    'TWD': 31.50,
    'AED': 3.67,
    'SAR': 3.75,
    'PLN': 4.02,
    'TRY': 32.0,
}

CURRENCY_SYMBOLS: Dict[str, str] = {
    'USD': '$',
    'EUR': '€',
    'GBP': '£',
    'JPY': '¥',
    'CNY': '¥',
    'AUD': 'A$',
    'CAD': 'C$',
    'CHF': 'CHF',
    'HKD': 'HK$',
    'SGD': 'S$',
    'INR': '₹',
    'KRW': '₩',
    'MXN': 'MX$',
    'BRL': 'R$',
    'RUB': '₽',
    'ZAR': 'R',
    'SEK': 'kr',
    'NOK': 'kr',
    'DKK': 'kr',
    'NZD': 'NZ$',
    'THB': '฿',
    'MYR': 'RM',
    'PHP': '₱',
    'IDR': 'Rp',
    'VND': '₫',
    'TWD': 'NT$',
    'AED': 'د.إ',
    'SAR': '﷼',
    'PLN': 'zł',
    'TRY': '₺',
}

CURRENCY_NAMES: Dict[str, str] = {
    'USD': 'US Dollar',
    'EUR': 'Euro',
    'GBP': 'British Pound',
    'JPY': 'Japanese Yen',
    'CNY': 'Chinese Yuan',
    'AUD': 'Australian Dollar',
    'CAD': 'Canadian Dollar',
    'CHF': 'Swiss Franc',
    'HKD': 'Hong Kong Dollar',
    'SGD': 'Singapore Dollar',
    'INR': 'Indian Rupee',
    'KRW': 'South Korean Won',
    'MXN': 'Mexican Peso',
    'BRL': 'Brazilian Real',
    'RUB': 'Russian Ruble',
    'ZAR': 'South African Rand',
    'SEK': 'Swedish Krona',
    'NOK': 'Norwegian Krone',
    'DKK': 'Danish Krone',
    'NZD': 'New Zealand Dollar',
    'THB': 'Thai Baht',
    'MYR': 'Malaysian Ringgit',
    'PHP': 'Philippine Peso',
    'IDR': 'Indonesian Rupiah',
    'VND': 'Vietnamese Dong',
    'TWD': 'Taiwan Dollar',
    'AED': 'UAE Dirham',
    'SAR': 'Saudi Riyal',
    'PLN': 'Polish Zloty',
    'TRY': 'Turkish Lira',
}


def convert_currency(
    amount: float,
    from_currency: str,
    to_currency: str,
    rates: Optional[Dict[str, float]] = None
) -> float:
    """
    Convert an amount from one currency to another.

    Args:
        amount: The amount to convert.
        from_currency: Source currency code (e.g., 'USD').
        to_currency: Target currency code (e.g., 'EUR').
        rates: Optional exchange rate dictionary. Uses fallback rates if None.

    Returns:
        The converted amount.

    Raises:
        ValueError: If currency code is not supported.
    """
    from_currency = from_currency.upper()
    to_currency = to_currency.upper()

    if rates is None:
        rates = FALLBACK_RATES

    if from_currency not in rates:
        raise ValueError(f"Unsupported currency: {from_currency}")
    if to_currency not in rates:
        raise ValueError(f"Unsupported currency: {to_currency}")

    usd_amount = amount / rates[from_currency]
    return usd_amount * rates[to_currency]


def format_currency(
    amount: float,
    currency: str,
    include_symbol: bool = True,
    decimal_places: int = 2,
    locale: Optional[str] = None
) -> str:
    """
    Format an amount as a currency string.

    Args:
        amount: The amount to format.
        currency: Currency code (e.g., 'USD').
        include_symbol: Whether to include the currency symbol.
        decimal_places: Number of decimal places.
        locale: Optional locale for formatting (e.g., 'en_US').

    Returns:
        Formatted currency string.

    Raises:
        ValueError: If currency code is not supported.
    """
    currency = currency.upper()

    if locale:
        try:
            import locale as locale_module
            locale_module.setlocale(locale_module.LC_ALL, locale)
            return locale_module.currency(amount, symbol=include_symbol, decimalness=decimal_places)
        except Exception:
            pass

    formatted = f"{amount:,.{decimal_places}f}"

    if include_symbol:
        symbol = CURRENCY_SYMBOLS.get(currency, currency + ' ')
        return f"{symbol}{formatted}"

    return f"{currency} {formatted}"


def get_currency_symbol(currency: str) -> str:
    """
    Get the symbol for a currency code.

    Args:
        currency: Currency code (e.g., 'USD').

    Returns:
        Currency symbol.
    """
    return CURRENCY_SYMBOLS.get(currency.upper(), currency)


def get_currency_name(currency: str) -> str:
    """
    Get the full name of a currency.

    Args:
        currency: Currency code (e.g., 'USD').

    Returns:
        Full currency name.
    """
    return CURRENCY_NAMES.get(currency.upper(), currency.upper())


def get_exchange_rate(
    from_currency: str,
    to_currency: str,
    rates: Optional[Dict[str, float]] = None
) -> float:
    """
    Get the exchange rate between two currencies.

    Args:
        from_currency: Source currency code.
        to_currency: Target currency code.
        rates: Optional exchange rate dictionary.

    Returns:
        Exchange rate (how many target currency per 1 source).
    """
    return convert_currency(1.0, from_currency, to_currency, rates)


def list_currencies() -> List[Dict[str, str]]:
    """
    List all supported currencies.

    Returns:
        List of currency information dictionaries.
    """
    currencies = []
    for code in CURRENCY_NAMES.keys():
        currencies.append({
            'code': code,
            'name': CURRENCY_NAMES[code],
            'symbol': CURRENCY_SYMBOLS.get(code, code),
        })
    return currencies


def invert_currency_pair(
    amount: float,
    from_currency: str,
    to_currency: str,
    rates: Optional[Dict[str, float]] = None
) -> Dict[str, float]:
    """
    Convert in both directions for a currency pair.

    Args:
        amount: The amount to convert.
        from_currency: First currency code.
        to_currency: Second currency code.
        rates: Optional exchange rate dictionary.

    Returns:
        Dictionary with 'to_second' and 'to_first' conversions.
    """
    to_second = convert_currency(amount, from_currency, to_currency, rates)
    to_first = convert_currency(amount, to_currency, from_currency, rates)

    return {
        'to_second': to_second,
        'to_first': to_first,
        'from_currency': from_currency.upper(),
        'to_currency': to_currency.upper(),
        'amount': amount,
    }


def parse_currency_string(text: str) -> Dict[str, Any]:
    """
    Parse a currency string to extract amount and currency.

    Args:
        text: String like "$1,234.56" or "€ 100" or "500 JPY".

    Returns:
        Dictionary with 'amount' and 'currency'.
    """
    import re

    text = text.strip()

    for code, symbol in sorted(CURRENCY_SYMBOLS.items(), key=lambda x: -len(x[1])):
        if symbol in text:
            pattern = re.escape(symbol) + r'[\s]*'
            text = re.sub(pattern, '', text)
            text = re.sub(r',' , '', text)
            try:
                amount = float(text.strip())
                return {'amount': amount, 'currency': code}
            except ValueError:
                pass

    number_pattern = r'([\d,]+\.?\d*)'
    match = re.search(number_pattern, text)
    if match:
        amount_str = match.group(1).replace(',', '')
        return {'amount': float(amount_str), 'currency': 'USD'}

    return {'amount': 0.0, 'currency': 'USD'}


def calculate_currency_delta(
    old_amount: float,
    new_amount: float,
    currency: str,
    rates: Optional[Dict[str, float]] = None
) -> Dict[str, Any]:
    """
    Calculate the change in value between two currency amounts.

    Args:
        old_amount: Previous amount.
        new_amount: Current amount.
        currency: Currency code.
        rates: Optional exchange rate dictionary.

    Returns:
        Dictionary with delta information.
    """
    delta = new_amount - old_amount
    percentage = (delta / old_amount * 100) if old_amount != 0 else 0.0

    return {
        'old_amount': old_amount,
        'new_amount': new_amount,
        'delta': delta,
        'delta_percentage': round(percentage, 2),
        'currency': currency.upper(),
        'improved': delta > 0,
    }


def convert_to_all_currencies(
    amount: float,
    from_currency: str,
    rates: Optional[Dict[str, float]] = None
) -> Dict[str, float]:
    """
    Convert an amount to all supported currencies.

    Args:
        amount: The amount to convert.
        from_currency: Source currency code.
        rates: Optional exchange rate dictionary.

    Returns:
        Dictionary mapping currency codes to converted amounts.
    """
    if rates is None:
        rates = FALLBACK_RATES

    results = {}
    for currency in rates.keys():
        if currency != from_currency.upper():
            results[currency] = round(
                convert_currency(amount, from_currency, currency, rates),
                2
            )

    return results
