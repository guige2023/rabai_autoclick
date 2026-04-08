"""
Write-Ahead Log (WAL) implementation for data persistence.

Provides a simple WAL for crash recovery and transaction logging
with configurable flushing and rotation.

Example:
    >>> from utils.wal_utils import WriteAheadLog
    >>> wal = WriteAheadLog("/data/wal")
    >>> wal.write(["SET", "key", "value"])
    >>> wal.flush()
"""

from __future__ import annotations

import json
import os
import struct
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Dict, List, Optional, Union


@dataclass
class WalEntry:
    """WAL entry representation."""
    sequence: int
    timestamp: float
    data: bytes
    checksum: int


class WriteAheadLog:
    """
    Write-Ahead Log for transaction durability.

    Provides sequential logging with checksums, optional
    compression, and checkpoint support.

    Attributes:
        log_dir: Directory for WAL files.
        segment_size: Size of each segment file in bytes.
        buffer_size: Write buffer size before flush.
    """

    HEADER_FORMAT = ">IQI"
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(
        self,
        log_dir: Union[str, Path],
        segment_size: int = 64 * 1024 * 1024,
        buffer_size: int = 4096,
        fsync_enabled: bool = True,
    ) -> None:
        """
        Initialize the WAL.

        Args:
            log_dir: Directory for WAL files.
            segment_size: Maximum size per segment file.
            buffer_size: Write buffer size.
            fsync_enabled: Enable fsync for durability.
        """
        self.log_dir = Path(log_dir)
        self.segment_size = segment_size
        self.buffer_size = buffer_size
        self.fsync_enabled = fsync_enabled

        self.log_dir.mkdir(parents=True, exist_ok=True)

        self._sequence = 0
        self._current_segment = 0
        self._buffer: List[bytes] = []
        self._buffer_size = 0
        self._file: Optional[BinaryIO] = None
        self._segment_files: List[Path] = []

        self._open_segment()
        self._recover()

    def _open_segment(self) -> None:
        """Open a new segment file."""
        if self._file:
            self._file.close()

        segment_path = self.log_dir / f"wal_{self._current_segment:06d}.seg"
        self._file = open(segment_path, "ab")
        self._segment_files.append(segment_path)

    def _write_entry(self, entry: WalEntry) -> None:
        """Write a single entry to the current segment."""
        if self._file is None:
            raise IOError("WAL file is not open")

        self._file.write(entry.data)

        if self.fsync_enabled:
            self._file.flush()
            os.fsync(self._file.fileno())

    def write(self, data: Any, serialize: bool = True) -> int:
        """
        Write data to the WAL.

        Args:
            data: Data to write (list/dict or pre-serialized bytes).
            serialize: If True, serialize data to JSON.

        Returns:
            Sequence number of the written entry.
        """
        self._sequence += 1

        if isinstance(data, bytes):
            serialized = data
        elif serialize:
            serialized = json.dumps(data).encode("utf-8")
        else:
            serialized = json.dumps(data).encode("utf-8")

        checksum = self._compute_checksum(serialized)

        header = struct.pack(
            self.HEADER_FORMAT,
            self._sequence,
            self._sequence,
            checksum,
        )

        entry_data = header + serialized + b"\n"

        if self._file:
            pos = self._file.tell()
            if pos + len(entry_data) > self.segment_size:
                self._open_segment()

        entry = WalEntry(
            sequence=self._sequence,
            timestamp=time.time(),
            data=entry_data,
            checksum=checksum,
        )

        self._write_entry(entry)
        return self._sequence

    def _compute_checksum(self, data: bytes) -> int:
        """Compute a simple checksum for data."""
        import zlib
        return zlib.crc32(data) & 0xFFFFFFFF

    def flush(self) -> None:
        """Flush the write buffer."""
        if self._file:
            self._file.flush()
            if self.fsync_enabled:
                os.fsync(self._file.fileno())

    def close(self) -> None:
        """Close the WAL."""
        self.flush()
        if self._file:
            self._file.close()
            self._file = None

    def _recover(self) -> None:
        """Recover entries from existing segments."""
        entries = self._read_all_entries()
        if entries:
            self._sequence = max(e.sequence for e in entries)
            self._current_segment = len(self._segment_files) - 1

    def _read_all_entries(self) -> List[WalEntry]:
        """Read all entries from all segment files."""
        entries: List[WalEntry] = []

        for segment_file in sorted(self._segment_files):
            try:
                with open(segment_file, "rb") as f:
                    entries.extend(self._read_segment(f))
            except (IOError, struct.error):
                pass

        return entries

    def _read_segment(self, f: BinaryIO) -> List[WalEntry]:
        """Read entries from a segment file."""
        entries: List[WalEntry] = []

        while True:
            pos = f.tell()
            header_data = f.read(self.HEADER_SIZE)
            if not header_data:
                break

            try:
                sequence, timestamp, checksum = struct.unpack(
                    self.HEADER_FORMAT, header_data
                )
            except struct.error:
                break

            remaining = f.read()
            newline_idx = remaining.find(b"\n")
            if newline_idx == -1:
                break

            serialized = remaining[:newline_idx]

            expected_checksum = self._compute_checksum(serialized)
            if checksum != expected_checksum:
                break

            entries.append(WalEntry(
                sequence=sequence,
                timestamp=timestamp,
                data=serialized,
                checksum=checksum,
            ))

            f.seek(pos + self.HEADER_SIZE + newline_idx + 1)

        return entries

    def replay(self) -> List[Any]:
        """
        Replay all WAL entries.

        Returns:
            List of deserialized data entries.
        """
        entries = self._read_all_entries()
        entries.sort(key=lambda e: e.sequence)

        results: List[Any] = []
        for entry in entries:
            try:
                data = json.loads(entry.data.decode("utf-8"))
                results.append(data)
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass

        return results

    def checkpoint(self) -> None:
        """Create a checkpoint by closing current segment."""
        self.flush()
        if self._file:
            self._file.close()
            self._file = None
        self._current_segment += 1
        self._open_segment()

    def get_stats(self) -> Dict[str, Any]:
        """Get WAL statistics."""
        return {
            "sequence": self._sequence,
            "segments": len(self._segment_files),
            "current_segment": self._current_segment,
            "fsync_enabled": self.fsync_enabled,
        }

    def __enter__(self) -> "WriteAheadLog":
        """Context manager entry."""
        return self

    def __exit__(self, *args: Any) -> None:
        """Context manager exit."""
        self.close()


class WalReader:
    """
    Read-only WAL reader for inspection and replay.
    """

    def __init__(self, log_dir: Union[str, Path]) -> None:
        """
        Initialize the WAL reader.

        Args:
            log_dir: Directory containing WAL files.
        """
        self.log_dir = Path(log_dir)

    def read_entries(self) -> List[WalEntry]:
        """Read all entries from WAL files."""
        entries: List[WalEntry] = []

        for segment_file in sorted(self.log_dir.glob("wal_*.seg")):
            with open(segment_file, "rb") as f:
                entries.extend(self._read_entries(f))

        entries.sort(key=lambda e: e.sequence)
        return entries

    def _read_entries(self, f: BinaryIO) -> List[WalEntry]:
        """Read entries from a segment file."""
        entries: List[WalEntry] = []
        header_size = struct.calcsize(WriteAheadLog.HEADER_FORMAT)

        while True:
            header_data = f.read(header_size)
            if not header_data:
                break

            try:
                sequence, timestamp, checksum = struct.unpack(
                    WriteAheadLog.HEADER_FORMAT, header_data
                )
            except struct.error:
                break

            remaining = f.read(1024 * 1024)
            newline_idx = remaining.find(b"\n")
            if newline_idx == -1:
                break

            serialized = remaining[:newline_idx]

            entries.append(WalEntry(
                sequence=sequence,
                timestamp=timestamp,
                data=serialized,
                checksum=checksum,
            ))

        return entries

    def replay(self) -> List[Any]:
        """Replay entries and return deserialized data."""
        entries = self.read_entries()
        results: List[Any] = []

        for entry in entries:
            try:
                data = json.loads(entry.data.decode("utf-8"))
                results.append(data)
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass

        return results
