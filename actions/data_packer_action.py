"""Data packer action module for RabAI AutoClick.

Provides data packing/unpacking operations:
- PackAction: Pack data into structured format
- UnpackAction: Unpack data from format
- PackSchemaAction: Define pack schema
- PackValidateAction: Validate packed data
"""

import struct
import json
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PackAction(BaseAction):
    """Pack data into structured binary format."""
    action_type = "pack"
    display_name = "打包数据"
    description = "打包数据为二进制格式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            schema = params.get("schema", [])
            format_str = params.get("format", "I")  # unsigned int

            if not data:
                return ActionResult(success=False, message="data is required")

            packed = b""
            for field in schema:
                field_name = field.get("name", "")
                field_type = field.get("type", "I")
                value = data.get(field_name, 0)

                try:
                    if field_type == "I":
                        packed += struct.pack("I", int(value))
                    elif field_type == "i":
                        packed += struct.pack("i", int(value))
                    elif field_type == "f":
                        packed += struct.pack("f", float(value))
                    elif field_type == "s":
                        packed += struct.pack(f"{len(str(value))}s", str(value).encode())
                    else:
                        packed += struct.pack("I", int(value))
                except struct.error:
                    pass

            return ActionResult(
                success=True,
                data={"packed_size": len(packed), "field_count": len(schema), "hex": packed.hex()[:40]},
                message=f"Packed {len(packed)} bytes with {len(schema)} fields",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pack failed: {e}")


class UnpackAction(BaseAction):
    """Unpack data from binary format."""
    action_type = "unpack"
    display_name = "解包数据"
    description = "从二进制格式解包数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            packed_data = params.get("packed_data", b"")
            schema = params.get("schema", [])
            format_str = params.get("format", "I")

            if not packed_data:
                return ActionResult(success=False, message="packed_data is required")

            try:
                unpacked = {}
                offset = 0
                for field in schema:
                    field_name = field.get("name", "")
                    field_type = field.get("type", "I")
                    size = field.get("size", 4)

                    try:
                        if field_type == "I":
                            unpacked[field_name] = struct.unpack_from("I", packed_data, offset)[0]
                            offset += 4
                        elif field_type == "i":
                            unpacked[field_name] = struct.unpack_from("i", packed_data, offset)[0]
                            offset += 4
                        elif field_type == "f":
                            unpacked[field_name] = struct.unpack_from("f", packed_data, offset)[0]
                            offset += 4
                        else:
                            offset += size
                    except struct.error:
                        break
            except Exception:
                unpacked = {"error": "unpack failed"}

            return ActionResult(
                success=True,
                data={"unpacked": unpacked, "offset": offset, "field_count": len(unpacked)},
                message=f"Unpacked {len(unpacked)} fields at offset {offset}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Unpack failed: {e}")


class PackSchemaAction(BaseAction):
    """Define pack schema."""
    action_type = "pack_schema"
    display_name = "打包Schema"
    description = "定义打包Schema"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            fields = params.get("fields", [])
            if not fields:
                return ActionResult(success=False, message="fields list is required")

            schema = []
            for f in fields:
                schema.append({
                    "name": f.get("name", ""),
                    "type": f.get("type", "I"),
                    "size": f.get("size", 4),
                    "offset": sum(s.get("size", 4) for s in schema),
                })

            total_size = sum(f.get("size", 4) for f in schema)

            return ActionResult(
                success=True,
                data={"schema": schema, "field_count": len(schema), "total_size": total_size},
                message=f"Schema: {len(schema)} fields, {total_size} bytes total",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pack schema failed: {e}")


class PackValidateAction(BaseAction):
    """Validate packed data."""
    action_type = "pack_validate"
    display_name = "验证打包数据"
    description = "验证打包数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            packed_data = params.get("packed_data", b"")
            schema = params.get("schema", [])

            if not packed_data:
                return ActionResult(success=False, message="packed_data is required")

            expected_size = sum(f.get("size", 4) for f in schema)
            is_valid = len(packed_data) >= expected_size

            return ActionResult(
                success=is_valid,
                data={"is_valid": is_valid, "packed_size": len(packed_data), "expected_size": expected_size},
                message=f"Pack validation: {'PASSED' if is_valid else 'FAILED'} ({len(packed_data)}/{expected_size} bytes)",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pack validate failed: {e}")
