"""Data classifier action module for RabAI AutoClick.

Provides data classification operations:
- ClassifyRuleAction: Rule-based classification
- ClassifyBinAction: Bin classification
- ClassifyQuantileAction: Quantile classification
- ClassifyCustomAction: Custom classification
- ClassifySummaryAction: Classification summary
"""

from typing import Any, Dict, List

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ClassifyRuleAction(BaseAction):
    """Rule-based classification."""
    action_type = "classify_rule"
    display_name = "规则分类"
    description = "基于规则的分类"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            rules = params.get("rules", [])

            if not data:
                return ActionResult(success=False, message="data is required")
            if not rules:
                return ActionResult(success=False, message="rules are required")

            classified = []
            for item in data:
                label = "unclassified"
                for rule in rules:
                    field = rule.get("field", "")
                    operator = rule.get("operator", "eq")
                    value = rule.get("value")
                    result_label = rule.get("label", "")
                    item_val = item.get(field)

                    match = False
                    if operator == "eq" and item_val == value:
                        match = True
                    elif operator == "gt" and item_val > value:
                        match = True
                    elif operator == "lt" and item_val < value:
                        match = True
                    elif operator == "gte" and item_val >= value:
                        match = True
                    elif operator == "lte" and item_val <= value:
                        match = True
                    elif operator == "contains" and isinstance(item_val, str) and value in item_val:
                        match = True

                    if match:
                        label = result_label
                        break

                new_item = item.copy()
                new_item["_classification"] = label
                classified.append(new_item)

            return ActionResult(
                success=True,
                data={"classified": classified, "count": len(classified), "rule_count": len(rules)},
                message=f"Rule classified {len(classified)} items with {len(rules)} rules",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Classify rule failed: {e}")


class ClassifyBinAction(BaseAction):
    """Bin-based classification."""
    action_type = "classify_bin"
    display_name = "分箱分类"
    description = "分箱分类"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "value")
            bins = params.get("bins", [0, 25, 50, 75, 100])
            labels = params.get("labels", None)

            if not data:
                return ActionResult(success=False, message="data is required")

            if labels is None:
                labels = [f"bin_{i}" for i in range(len(bins) - 1)]

            classified = []
            for item in data:
                val = item.get(field, 0)
                label = labels[-1]
                for i in range(len(bins) - 1):
                    if bins[i] <= val < bins[i + 1]:
                        label = labels[i]
                        break
                new_item = item.copy()
                new_item["_classification"] = label
                classified.append(new_item)

            return ActionResult(
                success=True,
                data={"classified": classified, "bin_count": len(bins) - 1, "bins": bins},
                message=f"Bin classified {len(classified)} items into {len(bins) - 1} bins",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Classify bin failed: {e}")


class ClassifyQuantileAction(BaseAction):
    """Quantile-based classification."""
    action_type = "classify_quantile"
    display_name = "分位数分类"
    description = "基于分位数的分类"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "value")
            num_classes = params.get("num_classes", 4)

            if not data:
                return ActionResult(success=False, message="data is required")

            values = sorted([d.get(field, 0) for d in data])
            n = len(values)
            quantiles = []
            for i in range(1, num_classes):
                idx = int(n * i / num_classes)
                quantiles.append(values[idx])
            quantiles.append(values[-1])

            labels = [f"Q{i+1}" for i in range(num_classes)]
            classified = []
            for item in data:
                val = item.get(field, 0)
                label = labels[-1]
                for i, q in enumerate(quantiles[:-1]):
                    if val <= q:
                        label = labels[i]
                        break
                new_item = item.copy()
                new_item["_classification"] = label
                classified.append(new_item)

            return ActionResult(
                success=True,
                data={"classified": classified, "quantile_boundaries": quantiles, "num_classes": num_classes},
                message=f"Quantile classified {len(classified)} items into {num_classes} classes",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Classify quantile failed: {e}")


class ClassifyCustomAction(BaseAction):
    """Custom classification."""
    action_type = "classify_custom"
    display_name = "自定义分类"
    description = "自定义分类"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            classifier_fn = params.get("classifier_fn", None)
            label_field = params.get("label_field", "category")

            if not data:
                return ActionResult(success=False, message="data is required")

            classified = []
            for item in data:
                new_item = item.copy()
                if classifier_fn:
                    new_item["_classification"] = str(item.get(label_field, "unknown"))
                else:
                    new_item["_classification"] = str(item.get(label_field, "unknown"))
                classified.append(new_item)

            return ActionResult(
                success=True,
                data={"classified": classified, "count": len(classified)},
                message=f"Custom classified {len(classified)} items",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Classify custom failed: {e}")


class ClassifySummaryAction(BaseAction):
    """Classification summary."""
    action_type = "classify_summary"
    display_name = "分类摘要"
    description = "生成分类摘要"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            class_field = params.get("class_field", "_classification")

            if not data:
                return ActionResult(success=False, message="data is required")

            from collections import Counter
            classes = [item.get(class_field, "unknown") for item in data]
            counter = Counter(classes)

            summary = [{"class": c, "count": cnt, "pct": (cnt / len(data)) * 100} for c, cnt in counter.most_common()]

            return ActionResult(
                success=True,
                data={"summary": summary, "total_classes": len(counter), "total_items": len(data)},
                message=f"Classification summary: {len(counter)} classes, {len(data)} items",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Classify summary failed: {e}")
