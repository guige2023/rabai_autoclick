"""Operator utilities v4 - simple operator utilities.

Simple operator function utilities.
"""

from __future__ import annotations

import operator
from typing import Any, Callable

__all__ = [
    "add",
    "sub",
    "mul",
    "truediv",
    "neg",
    "pos",
    "abs",
    "eq",
    "ne",
    "lt",
    "le",
    "gt",
    "ge",
]


add = operator.add
sub = operator.sub
mul = operator.mul
truediv = operator.truediv
neg = operator.neg
pos = operator.pos
abs = operator.abs
eq = operator.eq
ne = operator.ne
lt = operator.lt
le = operator.le
gt = operator.gt
ge = operator.ge
