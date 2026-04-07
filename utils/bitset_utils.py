"""
Bitset (bit array) utilities.

Provides efficient bit manipulation, set operations on bits,
and bitwise algorithm utilities.
"""

from __future__ import annotations


class BitSet:
    """
    Space-efficient bitset for boolean set operations.

    Example:
        >>> bs = BitSet(1000)
        >>> bs.add(42)
        >>> 42 in bs
        True
    """

    def __init__(self, size: int) -> None:
        self._size = size
        self._array = [0] * ((size + 63) // 64)

    def add(self, index: int) -> None:
        """Set bit at index to 1."""
        if 0 <= index < self._size:
            word, bit = divmod(index, 64)
            self._array[word] |= 1 << bit

    def remove(self, index: int) -> None:
        """Set bit at index to 0."""
        if 0 <= index < self._size:
            word, bit = divmod(index, 64)
            self._array[word] &= ~(1 << bit)

    def __contains__(self, index: int) -> bool:
        """Check if bit is set."""
        if 0 <= index < self._size:
            word, bit = divmod(index, 64)
            return bool(self._array[word] & (1 << bit))
        return False

    def __or__(self, other: BitSet) -> BitSet:
        """Union: bits that are set in either bitset."""
        result = BitSet(self._size)
        for i in range(len(self._array)):
            result._array[i] = self._array[i] | other._array[i]
        return result

    def __and__(self, other: BitSet) -> BitSet:
        """Intersection: bits set in both bitsets."""
        result = BitSet(self._size)
        for i in range(len(self._array)):
            result._array[i] = self._array[i] & other._array[i]
        return result

    def __xor__(self, other: BitSet) -> BitSet:
        """Symmetric difference."""
        result = BitSet(self._size)
        for i in range(len(self._array)):
            result._array[i] = self._array[i] ^ other._array[i]
        return result

    def __invert__(self) -> BitSet:
        """Complement: bits flipped."""
        result = BitSet(self._size)
        for i in range(len(self._array)):
            result._array[i] = ~self._array[i] & ((1 << 64) - 1)
        return result

    def count(self) -> int:
        """Count number of set bits."""
        return sum(bin(word).count("1") for word in self._array)

    def to_list(self) -> list[int]:
        """Return list of indices with bits set."""
        result = []
        for word_idx, word in enumerate(self._array):
            bit = 0
            while word:
                if word & 1:
                    result.append(word_idx * 64 + bit)
                word >>= 1
                bit += 1
        return result


def reverse_bits(n: int, width: int = 32) -> int:
    """
    Reverse the bits of an integer.

    Example:
        >>> bin(reverse_bits(0b1011, 4))
        '0b1101'
    """
    result = 0
    for i in range(width):
        if n & (1 << i):
            result |= 1 << (width - 1 - i)
    return result


def count_set_bits(n: int) -> int:
    """Count number of set bits (population count)."""
    count = 0
    while n:
        n &= n - 1
        count += 1
    return count


def next_higher_with_same_bits(n: int) -> int:
    """
    Given integer n, find the next integer with the same number of 1 bits.

    Based on "Next Higher with Same Number of 1 Bits" algorithm.
    """
    if n == 0:
        return 0
    c = n & -n
    c0 = c
    l = 0
    while c0 > 1:
        l += 1
        c0 >>= 1
    m = n + c
    right_ones = n ^ m
    right_ones = (right_ones >> 2) // c
    return m | right_ones
