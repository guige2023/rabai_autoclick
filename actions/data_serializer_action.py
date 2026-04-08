"""Data Serializer Action Module. Serializes Python objects."""
import sys, os, base64, pickle, json, zlib
from typing import Any
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

class DataSerializerAction(BaseAction):
    action_type = "data_serializer"; display_name = "数据序列化"
    description = "序列化Python对象"
    def __init__(self) -> None: super().__init__()
    def execute(self, context: Any, params: dict) -> ActionResult:
        mode = params.get("mode","serialize"); data = params.get("data")
        output_fmt = params.get("format","pickle").lower()
        compress = params.get("compress", False)
        return_enc = params.get("return_encoding","base64")
        if data is None: return ActionResult(success=False, message="No data")
        if mode == "deserialize":
            if not isinstance(data, str): return ActionResult(success=False, message="Need string for deserialize")
            try:
                if output_fmt == "pickle": result = pickle.loads(base64.b64decode(data))
                elif output_fmt == "json": result = json.loads(data)
                elif output_fmt == "hex": result = bytes.fromhex(data)
                else: return ActionResult(success=False, message=f"Unknown format: {output_fmt}")
                return ActionResult(success=True, message=f"Deserialized from {output_fmt}", data={"result": result})
            except Exception as e: return ActionResult(success=False, message=f"Error: {e}")
        try:
            if output_fmt == "pickle": serialized = pickle.dumps(data)
            elif output_fmt == "json": serialized = json.dumps(data, default=str).encode()
            else: return ActionResult(success=False, message=f"Unknown format: {output_fmt}")
            if compress: serialized = zlib.compress(serialized)
            if return_enc == "bytes": result_data = serialized
            elif return_enc == "base64": result_data = base64.b64encode(serialized).decode()
            elif return_enc == "hex": result_data = serialized.hex()
            else: result_data = serialized.decode(errors="replace")
            return ActionResult(success=True, message=f"Serialized: {len(serialized)} bytes",
                              data={"serialized": result_data, "size_bytes": len(serialized), "compressed": compress})
        except Exception as e: return ActionResult(success=False, message=f"Error: {e}")
