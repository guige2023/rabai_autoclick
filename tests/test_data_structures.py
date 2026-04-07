"""Tests for data structure utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.data_structures import (
    LinkedList,
    Stack,
    Queue,
    PriorityQueue,
    BiMap,
    MultiDict,
    group_by_seq,
    chunk_list,
)


class TestLinkedList:
    """Tests for LinkedList."""

    def test_append(self) -> None:
        """Test appending items."""
        ll = LinkedList[int]()
        ll.append(1)
        ll.append(2)
        assert len(ll) == 2

    def test_prepend(self) -> None:
        """Test prepending items."""
        ll = LinkedList[int]()
        ll.append(2)
        ll.prepend(1)
        assert list(ll) == [1, 2]

    def test_remove(self) -> None:
        """Test removing items."""
        ll = LinkedList[int]()
        ll.append(1)
        ll.append(2)
        ll.remove(1)
        assert list(ll) == [2]

    def test_iterate(self) -> None:
        """Test iteration."""
        ll = LinkedList[int]()
        ll.append(1)
        ll.append(2)
        assert list(ll) == [1, 2]


class TestStack:
    """Tests for Stack."""

    def test_push_pop(self) -> None:
        """Test push and pop."""
        stack = Stack[int]()
        stack.push(1)
        stack.push(2)
        assert stack.pop() == 2
        assert stack.pop() == 1

    def test_peek(self) -> None:
        """Test peek."""
        stack = Stack[int]()
        stack.push(1)
        stack.push(2)
        assert stack.peek() == 2

    def test_is_empty(self) -> None:
        """Test is_empty."""
        stack = Stack[int]()
        assert stack.is_empty()
        stack.push(1)
        assert not stack.is_empty()


class TestQueue:
    """Tests for Queue."""

    def test_enqueue_dequeue(self) -> None:
        """Test enqueue and dequeue."""
        queue = Queue[int]()
        queue.enqueue(1)
        queue.enqueue(2)
        assert queue.dequeue() == 1
        assert queue.dequeue() == 2

    def test_peek(self) -> None:
        """Test peek."""
        queue = Queue[int]()
        queue.enqueue(1)
        queue.enqueue(2)
        assert queue.peek() == 1


class TestPriorityQueue:
    """Tests for PriorityQueue."""

    def test_priority_order(self) -> None:
        """Test priority ordering."""
        pq = PriorityQueue[str]()
        pq.enqueue("low", 2)
        pq.enqueue("high", 1)
        pq.enqueue("medium", 3)

        assert pq.dequeue() == "high"


class TestBiMap:
    """Tests for BiMap."""

    def test_put_get(self) -> None:
        """Test put and get."""
        bimap = BiMap[str, int]()
        bimap.put("a", 1)

        assert bimap.get_by_key("a") == 1
        assert bimap.get_by_value(1) == "a"

    def test_reverse_lookup(self) -> None:
        """Test reverse lookup."""
        bimap = BiMap[str, int]()
        bimap.put("key", 42)

        assert bimap.get_by_value(42) == "key"


class TestMultiDict:
    """Tests for MultiDict."""

    def test_add_get(self) -> None:
        """Test adding and getting."""
        md = MultiDict[str, str]()
        md.add("key", "value1")
        md.add("key", "value2")

        values = md.get("key")
        assert len(values) == 2
        assert "value1" in values
        assert "value2" in values


class TestGroupBySeq:
    """Tests for group_by_seq."""

    def test_group_by(self) -> None:
        """Test grouping."""
        items = [{"type": "a", "v": 1}, {"type": "b", "v": 2}, {"type": "a", "v": 3}]
        result = group_by_seq(items, lambda x: x["type"])

        assert len(result["a"]) == 2
        assert len(result["b"]) == 1


class TestChunkList:
    """Tests for chunk_list."""

    def test_chunk(self) -> None:
        """Test chunking."""
        chunks = list(chunk_list([1, 2, 3, 4, 5], 2))
        assert chunks == [[1, 2], [3, 4], [5]]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])