"""Data resampler action module for RabAI AutoClick.

Provides data resampling operations:
- ResampleUpAction: Upsample data
- ResampleDownAction: Downsample data
- ResampleInterpolateAction: Interpolate missing values
- ResampleReshapeAction: Reshape data
"""

from typing import Any, Dict, List

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ResampleUpAction(BaseAction):
    """Upsample data."""
    action_type = "resample_up"
    display_name = "上采样"
    description = "上采样数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            factor = params.get("factor", 2)
            method = params.get("method", "repeat")

            if not data:
                return ActionResult(success=False, message="data is required")

            if method == "repeat":
                upsampled = []
                for item in data:
                    for _ in range(factor):
                        upsampled.append(item.copy())
            else:
                upsampled = data

            return ActionResult(
                success=True,
                data={"upsampled": upsampled, "original_count": len(data), "upsampled_count": len(upsampled), "factor": factor},
                message=f"Upsampled {len(data)} → {len(upsampled)} (factor={factor})",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Resample up failed: {e}")


class ResampleDownAction(BaseAction):
    """Downsample data."""
    action_type = "resample_down"
    display_name = "下采样"
    description = "下采样数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            factor = params.get("factor", 2)
            method = params.get("method", "first")

            if not data:
                return ActionResult(success=False, message="data is required")

            if method == "first":
                downsampled = data[::factor]
            elif method == "last":
                downsampled = data[-1 :: -factor][::-1]
            elif method == "mean":
                downsampled = []
                for i in range(0, len(data), factor):
                    chunk = data[i : i + factor]
                    if chunk:
                        downsampled.append({"resampled": True, "chunk_size": len(chunk)})
            else:
                downsampled = data[::factor]

            return ActionResult(
                success=True,
                data={"downsampled": downsampled, "original_count": len(data), "downsampled_count": len(downsampled), "factor": factor},
                message=f"Downsampled {len(data)} → {len(downsampled)} (factor={factor})",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Resample down failed: {e}")


class ResampleInterpolateAction(BaseAction):
    """Interpolate missing values."""
    action_type = "resample_interpolate"
    display_name = "插值填充"
    description = "插值填充缺失值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "value")
            method = params.get("method", "linear")

            if not data:
                return ActionResult(success=False, message="data is required")

            values = [d.get(field, 0) for d in data]
            indices = [i for i, v in enumerate(values) if v is None or v == ""]

            interpolated = values.copy()
            for idx in indices:
                if method == "linear":
                    prev_idx = next((i for i in range(idx - 1, -1, -1) if interpolated[i] not in (None, "")), None)
                    next_idx = next((i for i in range(idx + 1, len(interpolated)) if interpolated[i] not in (None, "")), None)
                    if prev_idx is not None and next_idx is not None:
                        interpolated[idx] = (interpolated[prev_idx] + interpolated[next_idx]) / 2
                    elif prev_idx is not None:
                        interpolated[idx] = interpolated[prev_idx]
                    elif next_idx is not None:
                        interpolated[idx] = interpolated[next_idx]
                elif method == "forward":
                    prev_idx = next((i for i in range(idx - 1, -1, -1) if interpolated[i] not in (None, "")), None)
                    if prev_idx is not None:
                        interpolated[idx] = interpolated[prev_idx]

            result = []
            for i, d in enumerate(data):
                new_d = d.copy()
                new_d[f"{field}_interpolated"] = interpolated[i]
                result.append(new_d)

            return ActionResult(
                success=True,
                data={"result": result, "interpolated_count": len(indices), "method": method},
                message=f"Interpolated {len(indices)} missing values using {method}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Resample interpolate failed: {e}")


class ResampleReshapeAction(BaseAction):
    """Reshape data."""
    action_type = "resample_reshape"
    display_name = "重塑数据"
    description = "重塑数据结构"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            rows = params.get("rows", None)
            cols = params.get("cols", None)

            if not data:
                return ActionResult(success=False, message="data is required")

            total = len(data)
            if rows is None and cols is None:
                rows = total
                cols = 1
            elif rows is None:
                rows = (total + cols - 1) // cols
            elif cols is None:
                cols = (total + rows - 1) // rows

            reshaped = []
            idx = 0
            for r in range(rows):
                row = []
                for c in range(cols):
                    if idx < total:
                        row.append(data[idx])
                        idx += 1
                    else:
                        row.append(None)
                reshaped.append(row)

            return ActionResult(
                success=True,
                data={"reshaped": reshaped, "rows": rows, "cols": cols, "original_count": total},
                message=f"Reshaped {total} items into {rows}x{cols}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Resample reshape failed: {e}")
