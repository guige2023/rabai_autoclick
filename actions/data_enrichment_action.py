"""Data enrichment action module for RabAI AutoClick.

Provides data enrichment operations:
- LookupEnrichAction: Enrich data with lookup tables
- GeoEnrichAction: Add geographic metadata
- TemporalEnrichAction: Add time-based features
- CrossReferenceEnrichAction: Cross-reference with external data
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Callable

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class LookupEnrichAction(BaseAction):
    """Enrich data with lookup tables."""
    action_type = "lookup_enrich"
    display_name = "查表数据丰富"
    description = "通过查找表丰富数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            lookup_table = params.get("lookup_table", {})
            lookup_key = params.get("lookup_key", "id")
            enrich_fields = params.get("enrich_fields", [])
            default_value = params.get("default_value", None)
            strategy = params.get("strategy", "left")

            if not data:
                return ActionResult(success=False, message="data is required")

            if not isinstance(data, list):
                data = [data]

            if not isinstance(lookup_table, dict):
                return ActionResult(success=False, message="lookup_table must be a dict")

            enriched = []
            match_count = 0

            for item in data:
                if not isinstance(item, dict):
                    continue
                key_value = item.get(lookup_key)
                lookup_row = lookup_table.get(key_value) if key_value is not None else None

                if lookup_row and isinstance(lookup_row, dict):
                    match_count += 1
                    enriched_item = {**item}
                    for field in enrich_fields:
                        if field in lookup_row:
                            enriched_item[field] = lookup_row[field]
                        else:
                            enriched_item[field] = default_value
                    enriched.append(enriched_item)
                elif strategy == "left":
                    enriched.append({**item})
                else:
                    enriched.append({**item, **{f: default_value for f in enrich_fields}})

            return ActionResult(
                success=True,
                message=f"Enriched {match_count}/{len(data)} records",
                data={"enriched": enriched, "match_count": match_count, "total": len(data)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"LookupEnrich error: {e}")


class GeoEnrichAction(BaseAction):
    """Add geographic metadata to data."""
    action_type = "geo_enrich"
    display_name = "地理数据丰富"
    description = "为数据添加地理位置元信息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            location_field = params.get("location_field", "location")
            geo_data = params.get("geo_data", {})
            enrich_fields = params.get("enrich_fields", ["country", "region", "timezone"])

            if not data:
                return ActionResult(success=False, message="data is required")

            if not isinstance(data, list):
                data = [data]

            enriched = []
            matched = 0

            for item in data:
                if not isinstance(item, dict):
                    continue
                location = item.get(location_field, "")
                geo_info = geo_data.get(location, {})

                enriched_item = {**item}
                if geo_info:
                    matched += 1
                    for field in enrich_fields:
                        enriched_item[field] = geo_info.get(field, "")
                enriched.append(enriched_item)

            return ActionResult(
                success=True,
                message=f"Geo-enriched {matched}/{len(data)} records",
                data={"enriched": enriched, "matched": matched},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"GeoEnrich error: {e}")


class TemporalEnrichAction(BaseAction):
    """Add time-based features to data."""
    action_type = "temporal_enrich"
    display_name = "时间数据丰富"
    description = "为数据添加时间特征"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            timestamp_field = params.get("timestamp_field", "timestamp")
            features = params.get("features", ["year", "month", "day", "hour", "weekday", "quarter"])
            timezone_str = params.get("timezone", "UTC")
            input_format = params.get("input_format", None)

            if not data:
                return ActionResult(success=False, message="data is required")

            if not isinstance(data, list):
                data = [data]

            enriched = []

            for item in data:
                if not isinstance(item, dict):
                    continue
                ts_value = item.get(timestamp_field)
                dt = None

                if isinstance(ts_value, datetime):
                    dt = ts_value
                elif isinstance(ts_value, (int, float)):
                    dt = datetime.fromtimestamp(ts_value, tz=timezone.utc)
                elif isinstance(ts_value, str):
                    if input_format:
                        dt = datetime.strptime(ts_value, input_format)
                    else:
                        for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"]:
                            try:
                                dt = datetime.strptime(ts_value, fmt)
                                break
                            except ValueError:
                                continue

                enriched_item = {**item}
                if dt:
                    try:
                        if timezone_str != "UTC":
                            import zoneinfo
                            tz = zoneinfo.ZoneInfo(timezone_str)
                            dt = dt.astimezone(tz)
                    except Exception:
                        pass

                    if "year" in features:
                        enriched_item["year"] = dt.year
                    if "month" in features:
                        enriched_item["month"] = dt.month
                    if "day" in features:
                        enriched_item["day"] = dt.day
                    if "hour" in features:
                        enriched_item["hour"] = dt.hour
                    if "weekday" in features:
                        enriched_item["weekday"] = dt.strftime("%A")
                    if "quarter" in features:
                        enriched_item["quarter"] = (dt.month - 1) // 3 + 1
                    if "is_weekend" in features:
                        enriched_item["is_weekend"] = dt.weekday() >= 5
                    if "day_of_year" in features:
                        enriched_item["day_of_year"] = dt.timetuple().tm_yday
                enriched.append(enriched_item)

            return ActionResult(
                success=True,
                message=f"Temporal-enriched {len(enriched)} records",
                data={"enriched": enriched, "count": len(enriched)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"TemporalEnrich error: {e}")


class CrossReferenceEnrichAction(BaseAction):
    """Cross-reference data with external data sources."""
    action_type = "crossref_enrich"
    display_name = "交叉引用数据丰富"
    description = "与外部数据进行交叉引用"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            ref_datasets = params.get("ref_datasets", [])
            match_fields = params.get("match_fields", [])
            enrich_mode = params.get("enrich_mode", "merge")
            conflict_resolution = params.get("conflict_resolution", "source")

            if not data:
                return ActionResult(success=False, message="data is required")

            if not isinstance(data, list):
                data = [data]

            if not match_fields:
                return ActionResult(success=False, message="match_fields is required")

            ref_index: Dict[str, Dict] = {}
            for dataset in ref_datasets:
                dataset_name = dataset.get("name", "unknown")
                dataset_data = dataset.get("data", [])
                if not isinstance(dataset_data, list):
                    continue
                for row in dataset_data:
                    if not isinstance(row, dict):
                        continue
                    key_values = tuple(row.get(f, "") for f in match_fields)
                    key = "|".join(str(v) for v in key_values)
                    ref_index[f"{dataset_name}:{key}"] = row

            enriched = []
            match_count = 0

            for item in data:
                if not isinstance(item, dict):
                    continue
                key_values = tuple(item.get(f, "") for f in match_fields)
                key = "|".join(str(v) for v in key_values)

                enriched_item = {**item}

                for dataset_name, ref_key in [(name, f"{name}:{key}") for name in [ds.get("name", "unknown") for ds in ref_datasets]]:
                    if ref_key in ref_index:
                        match_count += 1
                        ref_row = ref_index[ref_key]
                        if enrich_mode == "merge":
                            for k, v in ref_row.items():
                                if k not in enriched_item or conflict_resolution == "ref":
                                    enriched_item[k] = v
                        elif enrich_mode == "overwrite":
                            enriched_item.update(ref_row)

                enriched.append(enriched_item)

            return ActionResult(
                success=True,
                message=f"Cross-referenced {match_count} matches",
                data={"enriched": enriched, "match_count": match_count, "total": len(data)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"CrossReferenceEnrich error: {e}")
