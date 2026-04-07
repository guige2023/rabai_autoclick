"""Tests for functional utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.fn_utils import (
    compose,
    pipe,
    curry,
    partial,
    memoize,
    once,
    flip,
    juxt,
    apply,
    identity,
    constant,
    thunk,
    lift,
    lift2,
    try_catch,
    tap,
    property_,
    getter,
    setter,
    swap,
    iterate,
    always,
    never,
    getter_attr,
    setter_attr,
    method_caller,
    accessor,
    itemgetter,
    truthy,
    falsy,
    is_none,
    is_not_none,
)


def add(a: int, b: int) -> int:
    """Add two numbers."""
    return a + b


def multiply(a: int, b: int) -> int:
    """Multiply two numbers."""
    return a * b


def increment(x: int) -> int:
    """Increment value."""
    return x + 1


def double(x: int) -> int:
    """Double value."""
    return x * 2


def divide(a: int, b: int) -> float:
    """Divide numbers."""
    return a / b


class Obj:
    """Test object."""

    def __init__(self, value: int = 0):
        self.value = value

    def get_value(self) -> int:
        """Get value."""
        return self.value

    def set_value(self, v: int) -> None:
        """Set value."""
        self.value = v


class TestCompose:
    """Tests for compose function."""

    def test_compose(self) -> None:
        """Test composing functions."""
        f = compose(double, increment)
        assert f(5) == 12  # (5 + 1) * 2

    def test_compose_single(self) -> None:
        """Test composing single function."""
        f = compose(double)
        assert f(5) == 10

    def test_compose_empty(self) -> None:
        """Test composing empty functions."""
        f = compose()
        assert f(5) == 5


class TestPipe:
    """Tests for pipe function."""

    def test_pipe(self) -> None:
        """Test piping functions."""
        f = pipe(increment, double)
        assert f(5) == 12  # (5 * 2) + 1

    def test_pipe_single(self) -> None:
        """Test piping single function."""
        f = pipe(double)
        assert f(5) == 10

    def test_pipe_empty(self) -> None:
        """Test piping empty functions."""
        f = pipe()
        assert f(5) == 5


class TestCurry:
    """Tests for curry function."""

    def test_curry(self) -> None:
        """Test currying."""
        curried_add = curry(add)
        assert curried_add(2)(3) == 5

    def test_curry_partial(self) -> None:
        """Test currying with partial application."""
        curried_add = curry(add)
        add_two = curried_add(2)
        assert add_two(3) == 5


class TestPartial:
    """Tests for partial function."""

    def test_partial(self) -> None:
        """Test partial application."""
        add_five = partial(add, 5)
        assert add_five(3) == 8

    def test_partial_kwargs(self) -> None:
        """Test partial with kwargs."""
        def greet(name: str, greeting: str = "Hello") -> str:
            return f"{greeting}, {name}!"
        hi_greet = partial(greet, greeting="Hi")
        assert hi_greet("World") == "Hi, World!"


class TestMemoize:
    """Tests for memoize function."""

    def test_memoize(self) -> None:
        """Test memoization."""
        call_count = [0]
        def slow_add(a: int, b: int) -> int:
            call_count[0] += 1
            return a + b
        memoized = memoize(slow_add)
        assert memoized(1, 2) == 3
        assert memoized(1, 2) == 3
        assert call_count[0] == 1

    def test_memoize_clear(self) -> None:
        """Test clearing memo cache."""
        memoized = memoize(add)
        memoized(1, 2)
        memoized.cache_clear()
        assert len(memoized.cache) == 0


class TestOnce:
    """Tests for once function."""

    def test_once(self) -> None:
        """Test calling once."""
        call_count = [0]
        def only_once() -> int:
            call_count[0] += 1
            return 42
        wrapper = once(only_once)
        assert wrapper() == 42
        assert wrapper() == 42
        assert call_count[0] == 1


class TestFlip:
    """Tests for flip function."""

    def test_flip(self) -> None:
        """Test flipping arguments."""
        flipped = flip(divide)
        assert flipped(6, 2) == divide(2, 6)


class TestJuxt:
    """Tests for juxt function."""

    def test_juxt(self) -> None:
        """Test juxtaposition."""
        j = juxt(increment, double, lambda x: x * 3)
        assert j(5) == [6, 10, 15]


class TestIdentity:
    """Tests for identity function."""

    def test_identity(self) -> None:
        """Test identity."""
        assert identity(42) == 42
        assert identity("hello") == "hello"


class TestConstant:
    """Tests for constant function."""

    def test_constant(self) -> None:
        """Test constant function."""
        const = constant(42)
        assert const(1) == 42
        assert const("anything") == 42


class TestThunk:
    """Tests for thunk function."""

    def test_thunk(self) -> None:
        """Test thunk."""
        th = thunk(add, 2, 3)
        assert th() == 5
        assert th() == 5


class TestLift:
    """Tests for lift function."""

    def test_lift(self) -> None:
        """Test lifting."""
        inc = lift(increment)
        assert inc(5) == 6
        assert inc(None) is None


class TestLift2:
    """Tests for lift2 function."""

    def test_lift2(self) -> None:
        """Test lifting binary function."""
        add_lifted = lift2(add)
        assert add_lifted(2, 3) == 5
        assert add_lifted(None, 3) is None
        assert add_lifted(2, None) is None


class TestTryCatch:
    """Tests for try_catch function."""

    def test_try_catch(self) -> None:
        """Test try-catch."""
        def safe_div(a: int, b: int) -> float:
            return divide(a, b)
        wrapped = try_catch(safe_div, lambda e: 0.0)
        assert wrapped(6, 2) == 3.0
        assert wrapped(1, 0) == 0.0


class TestTap:
    """Tests for tap function."""

    def test_tap(self) -> None:
        """Test tap."""
        tapped_values = []
        def tap_func(x: int) -> None:
            tapped_values.append(x)
        f = tap(tap_func)
        result = f(42)
        assert result == 42
        assert tapped_values == [42]


class TestSwap:
    """Tests for swap function."""

    def test_swap(self) -> None:
        """Test swapping."""
        swapped = swap(divide)
        assert swapped(2, 6) == 3.0


class TestIterate:
    """Tests for iterate function."""

    def test_iterate(self) -> None:
        """Test iteration."""
        f = iterate(increment, 3)
        assert f(0) == [0, 1, 2, 3]


class TestAlways:
    """Tests for always function."""

    def test_always(self) -> None:
        """Test always."""
        assert always()(1, 2, 3)
        assert always()(None)


class TestNever:
    """Tests for never function."""

    def test_never(self) -> None:
        """Test never."""
        assert not never()(1, 2, 3)
        assert not never()(None)


class TestGetterAttr:
    """Tests for getter_attr function."""

    def test_getter_attr(self) -> None:
        """Test attribute getter."""
        obj = Obj(42)
        get_val = getter_attr("value")
        assert get_val(obj) == 42


class TestSetterAttr:
    """Tests for setter_attr function."""

    def test_setter_attr(self) -> None:
        """Test attribute setter."""
        obj = Obj()
        set_val = setter_attr("value", 100)
        set_val(obj)
        assert obj.value == 100


class TestMethodCaller:
    """Tests for method_caller function."""

    def test_method_caller(self) -> None:
        """Test method caller."""
        obj = Obj(50)
        caller = method_caller("get_value")
        assert caller(obj) == 50


class TestAccessor:
    """Tests for accessor function."""

    def test_accessor(self) -> None:
        """Test accessor."""
        get_first = accessor(0)
        assert get_first([1, 2, 3]) == 1


class TestItemGetter:
    """Tests for itemgetter function."""

    def test_itemgetter(self) -> None:
        """Test item getter."""
        get_name = itemgetter("name")
        assert get_name({"name": "Alice"}) == "Alice"


class TestTruthy:
    """Tests for truthy function."""

    def test_truthy(self) -> None:
        """Test truthy."""
        t = truthy()
        assert t(1)
        assert t("hello")
        assert not t(0)
        assert not t("")


class TestFalsy:
    """Tests for falsy function."""

    def test_falsy(self) -> None:
        """Test falsy."""
        f = falsy()
        assert f(0)
        assert f("")
        assert not f(1)
        assert not f("hello")


class TestIsNone:
    """Tests for is_none function."""

    def test_is_none(self) -> None:
        """Test is_none."""
        check = is_none()
        assert check(None)
        assert not check(0)
        assert not check("")


class TestIsNotNone:
    """Tests for is_not_none function."""

    def test_is_not_none(self) -> None:
        """Test is_not_none."""
        check = is_not_none()
        assert check(0)
        assert check("")
        assert not check(None)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])