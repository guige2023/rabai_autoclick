"""Feature store action module for RabAI AutoClick.

Provides feature store operations:
- FeatureStoreWriter: Write features to store
- FeatureStoreReader: Read features from store
- FeatureGroupManager: Manage feature groups
- FeatureRegistry: Register and discover features
- FeatureExtractor: Extract features from raw data
"""

from __future__ import annotations

import json
import sys
import os
import time
import hashlib
from typing import Any, Callable, Dict, List, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FeatureStoreWriterAction(BaseAction):
    """Write features to the feature store."""
    action_type = "feature_store_writer"
    display_name = "特征存储写入"
    description = "向特征存储写入特征"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            store_path = params.get("store_path", "/tmp/feature_store")
            feature_group = params.get("feature_group", "default")
            entity_id = params.get("entity_id", "")
            features = params.get("features", {})
            timestamp = params.get("timestamp", None)
            ttl_days = params.get("ttl_days", 30)

            if not entity_id:
                return ActionResult(success=False, message="entity_id is required")
            if not features:
                return ActionResult(success=False, message="features dict is required")

            ts = timestamp or datetime.now().isoformat()
            entity_dir = os.path.join(store_path, feature_group, entity_id)
            os.makedirs(entity_dir, exist_ok=True)

            feature_file = os.path.join(entity_dir, f"{ts}.json")
            record = {
                "entity_id": entity_id,
                "feature_group": feature_group,
                "timestamp": ts,
                "features": features,
                "ttl_days": ttl_days,
            }

            with open(feature_file, "w") as f:
                json.dump(record, f, indent=2)

            return ActionResult(
                success=True,
                message=f"Feature written: {entity_id}",
                data={"entity_id": entity_id, "feature_group": feature_group, "timestamp": ts, "file": feature_file}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class FeatureStoreReaderAction(BaseAction):
    """Read features from the feature store."""
    action_type = "feature_store_reader"
    display_name = "特征存储读取"
    description = "从特征存储读取特征"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            store_path = params.get("store_path", "/tmp/feature_store")
            feature_group = params.get("feature_group", "default")
            entity_id = params.get("entity_id", "")
            feature_names = params.get("feature_names", None)
            latest_only = params.get("latest_only", True)
            since = params.get("since", None)

            if not entity_id:
                return ActionResult(success=False, message="entity_id is required")

            entity_dir = os.path.join(store_path, feature_group, entity_id)
            if not os.path.exists(entity_dir):
                return ActionResult(success=False, message=f"No features found for entity: {entity_id}")

            feature_files = sorted([f for f in os.listdir(entity_dir) if f.endswith(".json")])

            if since:
                cutoff = datetime.fromisoformat(since)
                feature_files = [f for f in feature_files if datetime.fromisoformat(f.replace(".json", "")) >= cutoff]

            if latest_only:
                feature_files = [feature_files[-1]] if feature_files else []

            results = []
            for f in feature_files:
                with open(os.path.join(entity_dir, f)) as fp:
                    record = json.load(fp)
                    features = record.get("features", {})
                    if feature_names:
                        features = {k: v for k, v in features.items() if k in feature_names}
                    record["features"] = features
                    results.append(record)

            if not results:
                return ActionResult(success=False, message=f"No features found for {entity_id}")

            latest = results[-1] if results else {}
            return ActionResult(
                success=True,
                message=f"Read {len(results)} feature snapshots",
                data={"latest": latest.get("features", {}), "history": results, "count": len(results)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class FeatureGroupManagerAction(BaseAction):
    """Manage feature groups in the store."""
    action_type = "feature_group_manager"
    display_name = "特征组管理"
    description = "管理特征存储中的特征组"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "list")
            store_path = params.get("store_path", "/tmp/feature_store")
            feature_group = params.get("feature_group", "")
            description = params.get("description", "")
            schema = params.get("schema", {})
            ttl_days = params.get("ttl_days", 30)

            if operation == "list":
                if not os.path.exists(store_path):
                    return ActionResult(success=True, message="Store empty", data={"groups": []})

                groups = []
                for group_name in os.listdir(store_path):
                    group_path = os.path.join(store_path, group_name)
                    if os.path.isdir(group_path):
                        entity_count = len(os.listdir(group_path))
                        groups.append({
                            "name": group_name,
                            "entity_count": entity_count,
                        })

                return ActionResult(success=True, message=f"{len(groups)} groups", data={"groups": groups})

            elif operation == "create":
                if not feature_group:
                    return ActionResult(success=False, message="feature_group name required")
                group_dir = os.path.join(store_path, feature_group)
                os.makedirs(group_dir, exist_ok=True)

                meta_file = os.path.join(group_dir, "_meta.json")
                meta = {
                    "name": feature_group,
                    "description": description,
                    "schema": schema,
                    "ttl_days": ttl_days,
                    "created_at": datetime.now().isoformat(),
                }
                with open(meta_file, "w") as f:
                    json.dump(meta, f, indent=2)

                return ActionResult(success=True, message=f"Created feature group: {feature_group}")

            elif operation == "describe":
                if not feature_group:
                    return ActionResult(success=False, message="feature_group required")
                group_dir = os.path.join(store_path, feature_group)
                if not os.path.exists(group_dir):
                    return ActionResult(success=False, message=f"Feature group not found: {feature_group}")

                meta_file = os.path.join(group_dir, "_meta.json")
                if os.path.exists(meta_file):
                    with open(meta_file) as f:
                        meta = json.load(f)
                else:
                    meta = {}

                entity_count = len([d for d in os.listdir(group_dir) if os.path.isdir(os.path.join(group_dir, d))])

                return ActionResult(
                    success=True,
                    message=f"Feature group: {feature_group}",
                    data={**meta, "entity_count": entity_count}
                )

            elif operation == "delete":
                if not feature_group:
                    return ActionResult(success=False, message="feature_group required")
                import shutil
                group_dir = os.path.join(store_path, feature_group)
                if os.path.exists(group_dir):
                    shutil.rmtree(group_dir)
                    return ActionResult(success=True, message=f"Deleted: {feature_group}")
                return ActionResult(success=False, message=f"Not found: {feature_group}")

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class FeatureRegistryAction(BaseAction):
    """Registry for discovering and registering features."""
    action_type = "feature_registry"
    display_name = "特征注册表"
    description = "特征的注册与发现"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "register")
            registry_path = params.get("registry_path", "/tmp/feature_registry")
            feature_name = params.get("feature_name", "")
            feature_type = params.get("feature_type", "float")
            description = params.get("description", "")
            tags = params.get("tags", [])
            owner = params.get("owner", "")
            version = params.get("version", "1.0.0")

            if operation == "register":
                if not feature_name:
                    return ActionResult(success=False, message="feature_name required")

                registry_file = os.path.join(registry_path, "registry.json")
                os.makedirs(registry_path, exist_ok=True)

                registry = {}
                if os.path.exists(registry_file):
                    with open(registry_file) as f:
                        registry = json.load(f)

                feature_entry = {
                    "name": feature_name,
                    "type": feature_type,
                    "description": description,
                    "tags": tags,
                    "owner": owner,
                    "version": version,
                    "registered_at": datetime.now().isoformat(),
                    "feature_id": hashlib.md5(feature_name.encode()).hexdigest()[:12],
                }

                registry[feature_name] = feature_entry
                with open(registry_file, "w") as f:
                    json.dump(registry, f, indent=2)

                return ActionResult(success=True, message=f"Registered: {feature_name}")

            elif operation == "list":
                registry_file = os.path.join(registry_path, "registry.json")
                if not os.path.exists(registry_file):
                    return ActionResult(success=True, message="Registry empty", data={"features": []})

                with open(registry_file) as f:
                    registry = json.load(f)

                filters = params.get("filters", {})
                if filters:
                    filtered = {}
                    for name, entry in registry.items():
                        match = True
                        if filters.get("tags"):
                            if not any(t in entry.get("tags", []) for t in filters["tags"]):
                                match = False
                        if filters.get("owner") and entry.get("owner") != filters["owner"]:
                            match = False
                        if match:
                            filtered[name] = entry
                    registry = filtered

                return ActionResult(
                    success=True,
                    message=f"{len(registry)} features",
                    data={"features": list(registry.values()), "count": len(registry)}
                )

            elif operation == "search":
                query = params.get("query", "").lower()
                registry_file = os.path.join(registry_path, "registry.json")
                if not os.path.exists(registry_file):
                    return ActionResult(success=False, message="Registry not found")

                with open(registry_file) as f:
                    registry = json.load(f)

                results = {
                    name: entry for name, entry in registry.items()
                    if query in name.lower() or query in entry.get("description", "").lower()
                }

                return ActionResult(
                    success=True,
                    message=f"Found {len(results)} matches",
                    data={"features": list(results.values()), "count": len(results)}
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class FeatureExtractorAction(BaseAction):
    """Extract features from raw data."""
    action_type = "feature_extractor"
    display_name = "特征提取"
    description = "从原始数据提取特征"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            extraction_rules = params.get("extraction_rules", {})
            output_format = params.get("output_format", "dict")

            if not data:
                return ActionResult(success=False, message="data is required")

            features = {}

            for field_name, rule in extraction_rules.items():
                rule_type = rule.get("type", "passthrough")
                source_field = rule.get("source", field_name)

                value = data.get(source_field)

                if rule_type == "passthrough":
                    features[field_name] = value

                elif rule_type == "numeric":
                    try:
                        features[field_name] = float(value) if value is not None else None
                    except:
                        features[field_name] = None

                elif rule_type == "categorical":
                    features[field_name] = str(value) if value is not None else "unknown"

                elif rule_type == "binary":
                    threshold = rule.get("threshold", 0)
                    try:
                        features[field_name] = 1 if float(value) >= threshold else 0
                    except:
                        features[field_name] = 0

                elif rule_type == "binned":
                    bins = rule.get("bins", [0, 0.25, 0.5, 0.75, 1.0])
                    labels = rule.get("labels", ["Q1", "Q2", "Q3", "Q4"])
                    try:
                        val = float(value)
                        for i, boundary in enumerate(bins[:-1]):
                            if val <= bins[i + 1]:
                                features[field_name] = labels[i] if i < len(labels) else i
                                break
                        else:
                            features[field_name] = labels[-1]
                    except:
                        features[field_name] = None

                elif rule_type == "aggregated":
                    agg_type = rule.get("agg", "mean")
                    values = data.get(source_field, [])
                    if isinstance(values, list):
                        if agg_type == "mean":
                            features[field_name] = sum(values) / len(values) if values else None
                        elif agg_type == "sum":
                            features[field_name] = sum(values)
                        elif agg_type == "min":
                            features[field_name] = min(values) if values else None
                        elif agg_type == "max":
                            features[field_name] = max(values) if values else None
                        elif agg_type == "count":
                            features[field_name] = len(values)
                        elif agg_type == "std":
                            import statistics
                            features[field_name] = statistics.stdev(values) if len(values) > 1 else 0
                        else:
                            features[field_name] = None
                    else:
                        features[field_name] = None

            return ActionResult(
                success=True,
                message=f"Extracted {len(features)} features",
                data={"features": features, "count": len(features)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
