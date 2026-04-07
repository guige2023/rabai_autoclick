"""Tests for path utilities."""

import os
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.path_utils import (
    FileType,
    get_file_type,
    ensure_dir,
    ensure_parent_dir,
    safe_filename,
    copy_file,
    move_file,
    delete_file,
    get_size,
    format_size,
    walk_dir,
    TempDir,
    find_files_by_type,
)


class TestGetFileType:
    """Tests for get_file_type."""

    def test_image_types(self) -> None:
        """Test image file types."""
        assert get_file_type("test.png") == FileType.IMAGE
        assert get_file_type("test.jpg") == FileType.IMAGE
        assert get_file_type("test.gif") == FileType.IMAGE

    def test_config_types(self) -> None:
        """Test config file types."""
        assert get_file_type("test.json") == FileType.CONFIG
        assert get_file_type("test.yaml") == FileType.CONFIG
        assert get_file_type("test.yml") == FileType.CONFIG

    def test_code_types(self) -> None:
        """Test code file types."""
        assert get_file_type("test.py") == FileType.CODE
        assert get_file_type("test.js") == FileType.CODE

    def test_unknown_type(self) -> None:
        """Test unknown file type."""
        assert get_file_type("test.xyz") == FileType.UNKNOWN


class TestEnsureDir:
    """Tests for ensure_dir."""

    def test_create_dir(self) -> None:
        """Test creating directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "new_dir"
            result = ensure_dir(path)

            assert result.exists()
            assert result.is_dir()

    def test_existing_dir(self) -> None:
        """Test existing directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)
            result = ensure_dir(path)

            assert result.exists()


class TestSafeFilename:
    """Tests for safe_filename."""

    def test_normal_filename(self) -> None:
        """Test normal filename unchanged."""
        assert safe_filename("normal.txt") == "normal.txt"

    def test_invalid_chars_replaced(self) -> None:
        """Test invalid characters replaced."""
        assert safe_filename("file<>:*.txt") == "file____.txt"

    def test_leading_dot_removed(self) -> None:
        """Test leading dot removed."""
        assert safe_filename(".hidden") == "unnamed"

    def test_max_length(self) -> None:
        """Test max length enforcement."""
        long_name = "a" * 300
        result = safe_filename(long_name)
        assert len(result) <= 255


class TestCopyMoveDelete:
    """Tests for file operations."""

    def test_copy_file(self) -> None:
        """Test file copy."""
        with tempfile.NamedTemporaryFile(delete=False) as src:
            src.write(b"test content")
            src_path = src.name

        with tempfile.TemporaryDirectory() as tmpdir:
            dst_path = Path(tmpdir) / "copied.txt"
            result = copy_file(src_path, dst_path)

            assert result is True
            assert dst_path.exists()
            assert dst_path.read_text() == "test content"

        Path(src_path).unlink(missing_ok=True)

    def test_move_file(self) -> None:
        """Test file move."""
        with tempfile.NamedTemporaryFile(delete=False) as src:
            src.write(b"test content")
            src_path = src.name

        with tempfile.TemporaryDirectory() as tmpdir:
            dst_path = Path(tmpdir) / "moved.txt"
            result = move_file(src_path, dst_path)

            assert result is True
            assert dst_path.exists()
            assert not Path(src_path).exists()

    def test_delete_file(self) -> None:
        """Test file deletion."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"test")
            path = f.name

        result = delete_file(path)
        assert result is True
        assert not Path(path).exists()


class TestGetSize:
    """Tests for get_size."""

    def test_file_size(self) -> None:
        """Test getting file size."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"12345")
            path = f.name

        try:
            size = get_size(path)
            assert size == 5
        finally:
            Path(path).unlink()

    def test_dir_size(self) -> None:
        """Test getting directory size."""
        with tempfile.TemporaryDirectory() as tmpdir:
            Path(tmpdir, "f1.txt").write_text("123")
            Path(tmpdir, "f2.txt").write_text("45")

            size = get_size(tmpdir)
            assert size >= 6


class TestFormatSize:
    """Tests for format_size."""

    def test_bytes(self) -> None:
        """Test bytes formatting."""
        assert "B" in format_size(100)

    def test_kilobytes(self) -> None:
        """Test KB formatting."""
        assert "KB" in format_size(2048)

    def test_megabytes(self) -> None:
        """Test MB formatting."""
        assert "MB" in format_size(2 * 1024 * 1024)


class TestWalkDir:
    """Tests for walk_dir."""

    def test_walk_dir(self) -> None:
        """Test directory walking."""
        with tempfile.TemporaryDirectory() as tmpdir:
            Path(tmpdir, "f1.txt").write_text("1")
            Path(tmpdir, "f2.txt").write_text("2")
            Path(tmpdir, "subdir").mkdir()
            Path(tmpdir, "subdir", "f3.txt").write_text("3")

            files = walk_dir(tmpdir)
            assert len(files) == 3

    def test_non_recursive(self) -> None:
        """Test non-recursive walk."""
        with tempfile.TemporaryDirectory() as tmpdir:
            Path(tmpdir, "f1.txt").write_text("1")
            Path(tmpdir, "subdir").mkdir()
            Path(tmpdir, "subdir", "f2.txt").write_text("2")

            files = walk_dir(tmpdir, recursive=False)
            assert len(files) == 1


class TestTempDir:
    """Tests for TempDir."""

    def test_create(self) -> None:
        """Test TempDir creation."""
        temp = TempDir.create()
        assert temp.path.exists()

        temp.cleanup()
        assert not temp.path.exists()

    def test_context_manager(self) -> None:
        """Test TempDir as context manager."""
        with TempDir.create() as path:
            assert path.exists()

        assert not path.exists()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])