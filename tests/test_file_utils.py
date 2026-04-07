"""Tests for file utilities."""

import json
import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.file_utils import (
    ensure_dir,
    ensure_parent_dir,
    read_text,
    write_text,
    read_bytes,
    write_bytes,
    read_json,
    write_json,
    copy_file,
    move_file,
    delete_file,
    delete_dir,
    file_exists,
    dir_exists,
    get_size,
    get_extension,
    get_name,
    get_basename,
    list_files,
    is_empty_dir,
    get_relative_path,
    join_paths,
    normalize_path,
    is_absolute,
    make_absolute,
)


class TestEnsureDir:
    """Tests for ensure_dir function."""

    def test_ensure_dir_creates_directory(self) -> None:
        """Test ensure_dir creates directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_dir = os.path.join(tmpdir, "test_dir")
            result = ensure_dir(test_dir)
            assert os.path.isdir(result)


class TestEnsureParentDir:
    """Tests for ensure_parent_dir function."""

    def test_ensure_parent_dir_creates_parent(self) -> None:
        """Test ensure_parent_dir creates parent directories."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = os.path.join(tmpdir, "subdir", "test.txt")
            result = ensure_parent_dir(test_file)
            assert os.path.isdir(os.path.dirname(result))


class TestReadWriteText:
    """Tests for read_text and write_text functions."""

    def test_write_and_read_text(self) -> None:
        """Test writing and reading text."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = os.path.join(tmpdir, "test.txt")
            write_text(test_file, "Hello, World!")
            result = read_text(test_file)
            assert result == "Hello, World!"


class TestReadWriteBytes:
    """Tests for read_bytes and write_bytes functions."""

    def test_write_and_read_bytes(self) -> None:
        """Test writing and reading bytes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = os.path.join(tmpdir, "test.bin")
            write_bytes(test_file, b"\x00\x01\x02\x03")
            result = read_bytes(test_file)
            assert result == b"\x00\x01\x02\x03"


class TestReadWriteJson:
    """Tests for read_json and write_json functions."""

    def test_write_and_read_json(self) -> None:
        """Test writing and reading JSON."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = os.path.join(tmpdir, "test.json")
            data = {"key": "value", "number": 42}
            write_json(test_file, data)
            result = read_json(test_file)
            assert result == data


class TestCopyFile:
    """Tests for copy_file function."""

    def test_copy_file(self) -> None:
        """Test copying file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            src = os.path.join(tmpdir, "source.txt")
            dst = os.path.join(tmpdir, "dest.txt")
            write_text(src, "Content")
            copy_file(src, dst)
            assert file_exists(dst)
            assert read_text(dst) == "Content"


class TestMoveFile:
    """Tests for move_file function."""

    def test_move_file(self) -> None:
        """Test moving file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            src = os.path.join(tmpdir, "source.txt")
            dst = os.path.join(tmpdir, "dest.txt")
            write_text(src, "Content")
            move_file(src, dst)
            assert not file_exists(src)
            assert file_exists(dst)


class TestDeleteFile:
    """Tests for delete_file function."""

    def test_delete_file(self) -> None:
        """Test deleting file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = os.path.join(tmpdir, "test.txt")
            write_text(test_file, "Content")
            delete_file(test_file)
            assert not file_exists(test_file)


class TestDeleteDir:
    """Tests for delete_dir function."""

    def test_delete_empty_dir(self) -> None:
        """Test deleting empty directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_dir = os.path.join(tmpdir, "subdir")
            os.makedirs(test_dir)
            delete_dir(test_dir)
            assert not dir_exists(test_dir)


class TestFileExists:
    """Tests for file_exists function."""

    def test_file_exists_true(self) -> None:
        """Test file_exists returns True for existing file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = os.path.join(tmpdir, "test.txt")
            write_text(test_file, "Content")
            assert file_exists(test_file)

    def test_file_exists_false(self) -> None:
        """Test file_exists returns False for non-existing file."""
        assert not file_exists("/nonexistent/file.txt")


class TestDirExists:
    """Tests for dir_exists function."""

    def test_dir_exists_true(self) -> None:
        """Test dir_exists returns True for existing directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            assert dir_exists(tmpdir)

    def test_dir_exists_false(self) -> None:
        """Test dir_exists returns False for non-existing directory."""
        assert not dir_exists("/nonexistent/dir")


class TestGetSize:
    """Tests for get_size function."""

    def test_get_size(self) -> None:
        """Test getting file size."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = os.path.join(tmpdir, "test.txt")
            write_text(test_file, "Hello")
            assert get_size(test_file) == 5


class TestGetExtension:
    """Tests for get_extension function."""

    def test_get_extension(self) -> None:
        """Test getting file extension."""
        assert get_extension("test.txt") == ".txt"


class TestGetName:
    """Tests for get_name function."""

    def test_get_name(self) -> None:
        """Test getting file name without extension."""
        assert get_name("test.txt") == "test"


class TestGetBasename:
    """Tests for get_basename function."""

    def test_get_basename(self) -> None:
        """Test getting file basename."""
        assert get_basename("/path/to/test.txt") == "test.txt"


class TestListFiles:
    """Tests for list_files function."""

    def test_list_files(self) -> None:
        """Test listing files in directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            open(os.path.join(tmpdir, "a.txt"), "w").close()
            open(os.path.join(tmpdir, "b.txt"), "w").close()
            files = list_files(tmpdir)
            assert len(files) == 2


class TestIsEmptyDir:
    """Tests for is_empty_dir function."""

    def test_is_empty_dir_true(self) -> None:
        """Test is_empty_dir returns True for empty directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            assert is_empty_dir(tmpdir)

    def test_is_empty_dir_false(self) -> None:
        """Test is_empty_dir returns False for non-empty directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            open(os.path.join(tmpdir, "file.txt"), "w").close()
            assert not is_empty_dir(tmpdir)


class TestGetRelativePath:
    """Tests for get_relative_path function."""

    def test_get_relative_path(self) -> None:
        """Test getting relative path."""
        result = get_relative_path("/a/b/c", "/a/b")
        assert str(result) == "c"


class TestJoinPaths:
    """Tests for join_paths function."""

    def test_join_paths(self) -> None:
        """Test joining paths."""
        result = join_paths("a", "b", "c")
        assert str(result) == "a/b/c"


class TestNormalizePath:
    """Tests for normalize_path function."""

    def test_normalize_path(self) -> None:
        """Test normalizing path."""
        result = normalize_path("./test")
        assert result.is_absolute()


class TestIsAbsolute:
    """Tests for is_absolute function."""

    def test_is_absolute_true(self) -> None:
        """Test is_absolute returns True for absolute path."""
        assert is_absolute("/absolute/path")

    def test_is_absolute_false(self) -> None:
        """Test is_absolute returns False for relative path."""
        assert not is_absolute("relative/path")


class TestMakeAbsolute:
    """Tests for make_absolute function."""

    def test_make_absolute(self) -> None:
        """Test making path absolute."""
        result = make_absolute("test", "/base")
        assert result.is_absolute()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
