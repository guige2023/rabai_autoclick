"""Screenshot metadata utilities for storing and retrieving screenshot information.

This module provides utilities for managing metadata associated with
screenshots, including timestamps, regions, display info, and custom fields.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Any
from pathlib import Path
import json
import hashlib


@dataclass
class DisplayInfo:
    """Display information at time of capture."""
    display_index: int = 0
    display_name: str = ""
    width: int = 0
    height: int = 0
    x_offset: int = 0
    y_offset: int = 0
    dpi: int = 72
    is_primary: bool = False


@dataclass
class CaptureMetadata:
    """Metadata for a screenshot capture.
    
    Attributes:
        timestamp: When the screenshot was captured.
        region: Optional (x, y, width, height) region captured.
        display_info: Information about the display.
        format: Image format (PNG, JPEG, etc).
        size_bytes: Size of the image in bytes.
        checksum: MD5/SHA256 hash of the image.
        workflow_id: Optional ID of the workflow that captured it.
        step_id: Optional ID of the step that captured it.
        tags: Optional list of tags for categorization.
        custom_fields: User-defined metadata fields.
    """
    timestamp: datetime = field(default_factory=datetime.now)
    region: Optional[tuple[int, int, int, int]] = None
    display_info: Optional[DisplayInfo] = None
    format: str = "PNG"
    size_bytes: int = 0
    checksum: str = ""
    workflow_id: Optional[str] = None
    step_id: Optional[str] = None
    tags: list[str] = field(default_factory=list)
    custom_fields: dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        """Convert metadata to dictionary for serialization."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "region": self.region,
            "display_info": {
                "display_index": self.display_info.display_index if self.display_info else 0,
                "display_name": self.display_info.display_name if self.display_info else "",
                "width": self.display_info.width if self.display_info else 0,
                "height": self.display_info.height if self.display_info else 0,
                "x_offset": self.display_info.x_offset if self.display_info else 0,
                "y_offset": self.display_info.y_offset if self.display_info else 0,
                "dpi": self.display_info.dpi if self.display_info else 72,
                "is_primary": self.display_info.is_primary if self.display_info else False,
            } if self.display_info else None,
            "format": self.format,
            "size_bytes": self.size_bytes,
            "checksum": self.checksum,
            "workflow_id": self.workflow_id,
            "step_id": self.step_id,
            "tags": self.tags,
            "custom_fields": self.custom_fields,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> CaptureMetadata:
        """Create metadata from dictionary."""
        data = data.copy()
        if "timestamp" in data and isinstance(data["timestamp"], str):
            data["timestamp"] = datetime.fromisoformat(data["timestamp"])
        if "display_info" in data and data["display_info"]:
            data["display_info"] = DisplayInfo(**data["display_info"])
        return cls(**data)


def calculate_image_checksum(image_data: bytes, algorithm: str = "md5") -> str:
    """Calculate checksum hash of image data.
    
    Args:
        image_data: Raw image bytes.
        algorithm: Hash algorithm ('md5' or 'sha256').
    
    Returns:
        Hex digest of the hash.
    """
    if algorithm == "md5":
        return hashlib.md5(image_data).hexdigest()
    elif algorithm == "sha256":
        return hashlib.sha256(image_data).hexdigest()
    else:
        raise ValueError(f"Unsupported algorithm: {algorithm}")


def extract_exif_metadata(image_path: str | Path) -> dict:
    """Extract EXIF metadata from an image file.
    
    Args:
        image_path: Path to the image file.
    
    Returns:
        Dictionary of EXIF data.
    """
    try:
        from PIL import Image
        from PIL.ExifTags import TAGS
        
        with Image.open(image_path) as img:
            exif_data = img._getexif()
            if exif_data is None:
                return {}
            
            exif = {}
            for tag_id, value in exif_data.items():
                tag = TAGS.get(tag_id, tag_id)
                exif[tag] = value
            
            return exif
    except Exception:
        return {}


def save_metadata_json(
    metadata: CaptureMetadata,
    output_path: str | Path,
) -> None:
    """Save capture metadata to a JSON file.
    
    Args:
        metadata: CaptureMetadata to save.
        output_path: Path to save the JSON file.
    """
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(metadata.to_dict(), f, indent=2, ensure_ascii=False)


def load_metadata_json(
    input_path: str | Path,
) -> CaptureMetadata:
    """Load capture metadata from a JSON file.
    
    Args:
        input_path: Path to the JSON file.
    
    Returns:
        CaptureMetadata loaded from file.
    """
    with open(input_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return CaptureMetadata.from_dict(data)


def merge_metadata(
    base: CaptureMetadata,
    updates: dict,
) -> CaptureMetadata:
    """Merge updates into base metadata.
    
    Args:
        base: Base metadata to update.
        updates: Dictionary of fields to update.
    
    Returns:
        Updated CaptureMetadata.
    """
    base_dict = base.to_dict()
    base_dict.update(updates)
    return CaptureMetadata.from_dict(base_dict)


def validate_metadata(metadata: CaptureMetadata) -> list[str]:
    """Validate metadata and return list of issues.
    
    Args:
        metadata: CaptureMetadata to validate.
    
    Returns:
        List of validation error messages (empty if valid).
    """
    errors = []
    
    if metadata.size_bytes < 0:
        errors.append("size_bytes cannot be negative")
    
    if metadata.region:
        x, y, w, h = metadata.region
        if w <= 0 or h <= 0:
            errors.append(f"Invalid region dimensions: {metadata.region}")
    
    if metadata.checksum and len(metadata.checksum) not in (32, 64):
        errors.append(f"Invalid checksum length: {len(metadata.checksum)}")
    
    return errors


class MetadataRegistry:
    """Registry for managing multiple metadata entries."""
    
    def __init__(self):
        self._entries: dict[str, CaptureMetadata] = {}
    
    def register(
        self,
        key: str,
        metadata: CaptureMetadata,
    ) -> None:
        """Register a metadata entry."""
        self._entries[key] = metadata
    
    def get(self, key: str) -> Optional[CaptureMetadata]:
        """Get metadata by key."""
        return self._entries.get(key)
    
    def unregister(self, key: str) -> bool:
        """Unregister a metadata entry."""
        if key in self._entries:
            del self._entries[key]
            return True
        return False
    
    def list_keys(self) -> list[str]:
        """List all registered keys."""
        return list(self._entries.keys())
    
    def filter_by_tag(self, tag: str) -> list[CaptureMetadata]:
        """Filter metadata entries by tag."""
        return [m for m in self._entries.values() if tag in m.tags]
    
    def filter_by_workflow(self, workflow_id: str) -> list[CaptureMetadata]:
        """Filter metadata entries by workflow ID."""
        return [m for m in self._entries.values() if m.workflow_id == workflow_id]
