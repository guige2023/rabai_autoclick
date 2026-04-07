"""
Cryptography and encoding utilities.

Provides XOR cipher, Caesar cipher, substitution cipher,
Base64 encoding/decoding, and simple hash functions.
"""

from __future__ import annotations

import base64
import hashlib
import math


def caesar_cipher(text: str, shift: int, decrypt: bool = False) -> str:
    """
    Caesar cipher encryption/decryption.

    Args:
        text: Plain text
        shift: Shift amount (positive for right)
        decrypt: If True, decrypt instead of encrypt

    Returns:
        Encrypted/decrypted text.
    """
    if decrypt:
        shift = -shift
    result = []
    for ch in text:
        if 'a' <= ch <= 'z':
            result.append(chr((ord(ch) - ord('a') + shift) % 26 + ord('a')))
        elif 'A' <= ch <= 'Z':
            result.append(chr((ord(ch) - ord('A') + shift) % 26 + ord('A')))
        else:
            result.append(ch)
    return ''.join(result)


def vigenere_cipher(text: str, key: str, decrypt: bool = False) -> str:
    """
    Vigenere cipher encryption/decryption.

    Args:
        text: Plain text
        key: Keyword
        decrypt: If True, decrypt

    Returns:
        Encrypted/decrypted text.
    """
    if not key:
        return text
    result = []
    key_bytes = [ord(c.lower()) - ord('a') for c in key if c.isalpha()]
    if not key_bytes:
        return text
    ki = 0
    for ch in text:
        if ch.isalpha():
            base = ord('a') if ch.islower() else ord('A')
            shift = key_bytes[ki % len(key_bytes)]
            if decrypt:
                shift = -shift
            result.append(chr((ord(ch) - base + shift) % 26 + base))
            ki += 1
        else:
            result.append(ch)
    return ''.join(result)


def xor_cipher(data: bytes, key: bytes) -> bytes:
    """
    XOR cipher (single-byte key or repeating key).

    Args:
        data: Data to encrypt/decrypt
        key: Encryption key

    Returns:
        XOR'd data.
    """
    if not key:
        return data
    result = bytearray(data)
    for i in range(len(result)):
        result[i] ^= key[i % len(key)]
    return bytes(result)


def rail_fence_cipher(text: str, rails: int, decrypt: bool = False) -> str:
    """
    Rail fence cipher (zigzag).

    Args:
        text: Plain text
        rails: Number of rails
        decrypt: If True, decrypt

    Returns:
        Encrypted/decrypted text.
    """
    if rails < 2:
        return text
    if decrypt:
        # Compute pattern
        fence = [[] for _ in range(rails)]
        pattern = []
        direction = -1
        row = 0
        for _ in text:
            pattern.append(row)
            if row == 0 or row == rails - 1:
                direction *= -1
            row += direction
        # Fill
        idx = 0
        for r in range(rails):
            for i, p in enumerate(pattern):
                if p == r:
                    fence[r].append(text[idx])
                    idx += 1
        # Read
        result = []
        for r in range(rails):
            result.extend(fence[r])
        return ''.join(result)
    else:
        fence = [[] for _ in range(rails)]
        direction = -1
        row = 0
        for ch in text:
            fence[row].append(ch)
            if row == 0 or row == rails - 1:
                direction *= -1
            row += direction
        return ''.join(''.join(rail) for rail in fence)


def atbash_cipher(text: str) -> str:
    """Atbash cipher: alphabet reversed."""
    result = []
    for ch in text:
        if 'a' <= ch <= 'z':
            result.append(chr(ord('z') - (ord(ch) - ord('a'))))
        elif 'A' <= ch <= 'Z':
            result.append(chr(ord('Z') - (ord(ch) - ord('A'))))
        else:
            result.append(ch)
    return ''.join(result)


def base64_encode(data: bytes | str) -> str:
    """Base64 encoding."""
    if isinstance(data, str):
        data = data.encode()
    return base64.b64encode(data).decode()


def base64_decode(data: str) -> bytes:
    """Base64 decoding."""
    return base64.b64decode(data)


def url_safe_base64_encode(data: bytes | str) -> str:
    """URL-safe Base64 encoding."""
    if isinstance(data, str):
        data = data.encode()
    return base64.urlsafe_b64encode(data).rstrip(b'=').decode()


def url_safe_base64_decode(data: str) -> bytes:
    """URL-safe Base64 decoding."""
    padding = 4 - len(data) % 4
    if padding != 4:
        data += '=' * padding
    return base64.urlsafe_b64decode(data)


def md5_hash(data: str | bytes) -> str:
    """MD5 hash."""
    if isinstance(data, str):
        data = data.encode()
    return hashlib.md5(data).hexdigest()


def sha256_hash(data: str | bytes) -> str:
    """SHA-256 hash."""
    if isinstance(data, str):
        data = data.encode()
    return hashlib.sha256(data).hexdigest()


def sha1_hash(data: str | bytes) -> str:
    """SHA-1 hash."""
    if isinstance(data, str):
        data = data.encode()
    return hashlib.sha1(data).hexdigest()


def sha512_hash(data: str | bytes) -> str:
    """SHA-512 hash."""
    if isinstance(data, str):
        data = data.encode()
    return hashlib.sha512(data).hexdigest()


def hmac_sha256(key: str | bytes, message: str | bytes) -> str:
    """HMAC-SHA256."""
    import hmac as _hmac
    if isinstance(key, str):
        key = key.encode()
    if isinstance(message, str):
        message = message.encode()
    return _hmac.new(key, message, hashlib.sha256).hexdigest()


def bcrypt_hash(password: str, rounds: int = 12) -> str:
    """
    Simple bcrypt-like hash (simplified implementation).

    For production, use the bcrypt library.
    """
    salt = hashlib.sha512(str(hashlib.md5(password.encode()).hexdigest()).encode()).hexdigest()[:22]
    key = password + salt
    for _ in range(2 ** rounds):
        key = hashlib.sha512((key + salt).encode()).hexdigest()
    return f"$2b${rounds}${salt}${key}"


def pbkdf2(
    password: str,
    salt: str,
    iterations: int = 100000,
    key_length: int = 32,
) -> str:
    """
    PBKDF2-HMAC-SHA256.

    Args:
        password: Password
        salt: Salt
        iterations: Number of iterations
        key_length: Desired key length in bytes

    Returns:
        Hex-encoded derived key.
    """
    import hmac as _hmac
    block = password + salt
    result = block
    for i in range(iterations):
        result = _hmac.new(result.encode(), block.encode(), hashlib.sha256).hexdigest()
    return result[:key_length * 2]


def rot13(text: str) -> str:
    """ROT13 cipher (Caesar with shift 13)."""
    return caesar_cipher(text, 13)


def affine_cipher(text: str, a: int, b: int, decrypt: bool = False) -> str:
    """
    Affine cipher: f(x) = ax + b (mod 26).

    Args:
        text: Plain text
        a: Multiplier (must be coprime with 26)
        b: Shift
        decrypt: If True, decrypt
    """
    # Find modular inverse of a
    def modinv(a: int, m: int) -> int:
        for x in range(1, m):
            if (a * x) % m == 1:
                return x
        return 0

    if decrypt:
        a_inv = modinv(a, 26)
        if a_inv == 0:
            return text
        a = a_inv
        b = -b * a_inv % 26

    result = []
    for ch in text:
        if 'a' <= ch <= 'z':
            x = ord(ch) - ord('a')
            x = (a * x + b) % 26
            result.append(chr(x + ord('a')))
        elif 'A' <= ch <= 'Z':
            x = ord(ch) - ord('A')
            x = (a * x + b) % 26
            result.append(chr(x + ord('A')))
        else:
            result.append(ch)
    return ''.join(result)


def frequency_analysis(text: str) -> dict[str, float]:
    """
    Letter frequency analysis.

    Returns:
        Dictionary of letter -> frequency.
    """
    counts: dict[str, int] = {}
    total = 0
    for ch in text.lower():
        if 'a' <= ch <= 'z':
            counts[ch] = counts.get(ch, 0) + 1
            total += 1
    if total == 0:
        return {}
    return {ch: count / total for ch, count in counts.items()}


def crack_substitution(cipher_text: str) -> str:
    """
    Simple substitution cipher cracking using frequency analysis.

    Returns:
        Attempted plain text.
    """
    import english_frequencies
    freq = frequency_analysis(cipher_text)
    sorted_cipher = sorted(freq, key=freq.get, reverse=True)
    sorted_english = english_frequencies.LETTER_FREQ
    sorted_english_chars = sorted(sorted_english, key=sorted_english.get, reverse=True)
    mapping = dict(zip(sorted_cipher, sorted_english_chars))
    result = []
    for ch in cipher_text.lower():
        if ch in mapping:
            result.append(mapping[ch])
        else:
            result.append(ch)
    return ''.join(result)


class english_frequencies:
    """English letter frequency data."""
    LETTER_FREQ = {
        'e': 0.1270, 't': 0.0906, 'a': 0.0817, 'o': 0.0751, 'i': 0.0697,
        'n': 0.0675, 's': 0.0633, 'h': 0.0609, 'r': 0.0599, 'd': 0.0425,
        'l': 0.0403, 'c': 0.0278, 'u': 0.0276, 'm': 0.0241, 'w': 0.0236,
        'f': 0.0223, 'g': 0.0202, 'y': 0.0197, 'p': 0.0193, 'b': 0.0129,
        'v': 0.0098, 'k': 0.0077, 'j': 0.0015, 'x': 0.0015, 'q': 0.0010,
        'z': 0.0007,
    }
