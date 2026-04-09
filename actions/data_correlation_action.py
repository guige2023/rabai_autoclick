"""Data correlation action module for RabAI AutoClick.

Provides correlation analysis operations:
- PearsonCorrelationAction: Calculate Pearson correlation
- SpearmanCorrelationAction: Calculate Spearman correlation
- KendallCorrelationAction: Calculate Kendall tau correlation
- CorrelationMatrixAction: Generate correlation matrix
- PartialCorrelationAction: Calculate partial correlations
"""

from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict
import math

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PearsonCorrelationAction(BaseAction):
    """Calculate Pearson correlation coefficient."""
    action_type = "pearson_correlation"
    display_name = "皮尔逊相关"
    description = "计算皮尔逊相关系数"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field_x = params.get("field_x")
            field_y = params.get("field_y")
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if not field_x or not field_y:
                return ActionResult(success=False, message="field_x and field_y are required")
            
            correlation, p_value = self._calculate_correlation(data, field_x, field_y)
            
            return ActionResult(
                success=True,
                message="Pearson correlation calculated",
                data={
                    "correlation": correlation,
                    "p_value": p_value,
                    "field_x": field_x,
                    "field_y": field_y,
                    "interpretation": self._interpret_correlation(correlation)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _calculate_correlation(self, data: List[Dict], field_x: str, field_y: str) -> Tuple[float, float]:
        pairs = []
        for item in data:
            if isinstance(item, dict):
                x = item.get(field_x)
                y = item.get(field_y)
                if isinstance(x, (int, float)) and isinstance(y, (int, float)):
                    pairs.append((x, y))
        
        if len(pairs) < 2:
            return 0.0, 1.0
        
        n = len(pairs)
        
        sum_x = sum(p[0] for p in pairs)
        sum_y = sum(p[1] for p in pairs)
        sum_xy = sum(p[0] * p[1] for p in pairs)
        sum_x2 = sum(p[0] ** 2 for p in pairs)
        sum_y2 = sum(p[1] ** 2 for p in pairs)
        
        numerator = n * sum_xy - sum_x * sum_y
        denominator = math.sqrt((n * sum_x2 - sum_x ** 2) * (n * sum_y2 - sum_y ** 2))
        
        if denominator == 0:
            return 0.0, 1.0
        
        r = numerator / denominator
        
        t = r * math.sqrt((n - 2) / (1 - r ** 2)) if abs(r) < 1 else 0.0
        
        p_value = self._t_dist_p_value(abs(t), n - 2)
        
        return r, p_value
    
    def _t_dist_p_value(self, t: float, df: int) -> float:
        x = df / (df + t * t)
        p_value = 0.5 * self._beta_inc(df / 2, 0.5, x)
        return p_value
    
    def _beta_inc(self, a: float, b: float, x: float) -> float:
        if x < 0 or x > 1:
            return 0.0
        if x == 0 or x == 1:
            return x
        
        bt = math.exp(
            math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b) +
            a * math.log(x) + b * math.log(1 - x)
        )
        
        if x < (a + 1) / (a + b + 2):
            return bt * self._beta_cf(a, b, x) / a
        else:
            return 1 - bt * self._beta_cf(b, a, 1 - x) / b
    
    def _beta_cf(self, a: float, b: float, x: float) -> float:
        max_iter = 100
        eps = 1e-10
        
        qab = a + b
        qap = a + 1
        qam = a - 1
        c = 1
        d = 1 - qab * x / qap
        if abs(d) < eps:
            d = eps
        d = 1 / d
        h = d
        
        for m in range(1, max_iter + 1):
            m2 = 2 * m
            
            aa = m * (b - m) * x / ((qam + m2) * (a + m2))
            d = 1 + aa * d
            if abs(d) < eps:
                d = eps
            c = 1 + aa / c
            if abs(c) < eps:
                c = eps
            d = 1 / d
            h *= d * c
            
            aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
            d = 1 + aa * d
            if abs(d) < eps:
                d = eps
            c = 1 + aa / c
            if abs(c) < eps:
                c = eps
            d = 1 / d
            del_ = d * c
            h *= del_
            
            if abs(del_ - 1) < eps:
                break
        
        return h
    
    def _interpret_correlation(self, r: float) -> str:
        r_abs = abs(r)
        if r_abs < 0.1:
            return "negligible"
        elif r_abs < 0.3:
            return "weak"
        elif r_abs < 0.5:
            return "moderate"
        elif r_abs < 0.7:
            return "strong"
        else:
            return "very strong"


class SpearmanCorrelationAction(BaseAction):
    """Calculate Spearman rank correlation."""
    action_type = "spearman_correlation"
    display_name = "斯皮尔曼相关"
    description = "计算斯皮尔曼等级相关系数"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field_x = params.get("field_x")
            field_y = params.get("field_y")
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if not field_x or not field_y:
                return ActionResult(success=False, message="field_x and field_y are required")
            
            correlation = self._calculate_correlation(data, field_x, field_y)
            
            return ActionResult(
                success=True,
                message="Spearman correlation calculated",
                data={
                    "correlation": correlation,
                    "field_x": field_x,
                    "field_y": field_y
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _calculate_correlation(self, data: List[Dict], field_x: str, field_y: str) -> float:
        pairs = []
        for item in data:
            if isinstance(item, dict):
                x = item.get(field_x)
                y = item.get(field_y)
                if x is not None and y is not None:
                    pairs.append((x, y))
        
        if len(pairs) < 2:
            return 0.0
        
        ranked_x = self._rank_data([p[0] for p in pairs])
        ranked_y = self._rank_data([p[1] for p in pairs])
        
        n = len(pairs)
        d_squared = sum((rx - ry) ** 2 for rx, ry in zip(ranked_x, ranked_y))
        
        rho = 1 - (6 * d_squared) / (n * (n ** 2 - 1))
        
        return rho
    
    def _rank_data(self, values: List) -> List[float]:
        sorted_with_idx = sorted(enumerate(values), key=lambda x: x[1])
        ranks = [0] * len(values)
        
        i = 0
        while i < len(sorted_with_idx):
            j = i
            while j < len(sorted_with_idx) - 1 and sorted_with_idx[j][1] == sorted_with_idx[j + 1][1]:
                j += 1
            
            avg_rank = sum(idx + 1 for idx in range(i, j + 1)) / (j - i + 1)
            
            for idx in range(i, j + 1):
                ranks[sorted_with_idx[idx][0]] = avg_rank
            
            i = j + 1
        
        return ranks


class KendallCorrelationAction(BaseAction):
    """Calculate Kendall tau correlation."""
    action_type = "kendall_correlation"
    display_name = "肯德尔相关"
    description = "计算肯德尔tau相关系数"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field_x = params.get("field_x")
            field_y = params.get("field_y")
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if not field_x or not field_y:
                return ActionResult(success=False, message="field_x and field_y are required")
            
            tau, p_value = self._calculate_correlation(data, field_x, field_y)
            
            return ActionResult(
                success=True,
                message="Kendall tau correlation calculated",
                data={
                    "tau": tau,
                    "p_value": p_value,
                    "field_x": field_x,
                    "field_y": field_y
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _calculate_correlation(self, data: List[Dict], field_x: str, field_y: str) -> Tuple[float, float]:
        pairs = []
        for item in data:
            if isinstance(item, dict):
                x = item.get(field_x)
                y = item.get(field_y)
                if isinstance(x, (int, float)) and isinstance(y, (int, float)):
                    pairs.append((x, y))
        
        if len(pairs) < 2:
            return 0.0, 1.0
        
        n = len(pairs)
        concordant = 0
        discordant = 0
        
        for i in range(n):
            for j in range(i + 1, n):
                x_diff = pairs[i][0] - pairs[j][0]
                y_diff = pairs[i][1] - pairs[j][1]
                
                product = x_diff * y_diff
                
                if product > 0:
                    concordant += 1
                elif product < 0:
                    discordant += 1
        
        tau = (concordant - discordant) / (n * (n - 1) / 2)
        
        se = math.sqrt((4 * n + 10) / (9 * n * (n - 1)))
        z = tau / se
        p_value = 2 * (1 - 0.5 * (1 + math.erf(abs(z) / math.sqrt(2))))
        
        return tau, p_value


class CorrelationMatrixAction(BaseAction):
    """Generate correlation matrix for multiple fields."""
    action_type = "correlation_matrix"
    display_name = "相关矩阵"
    description = "生成多个字段的相关矩阵"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            fields = params.get("fields", [])
            method = params.get("method", "pearson")
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if len(fields) < 2:
                return ActionResult(success=False, message="At least 2 fields are required")
            
            matrix = self._generate_matrix(data, fields, method)
            
            return ActionResult(
                success=True,
                message=f"Correlation matrix generated using {method}",
                data={
                    "matrix": matrix,
                    "fields": fields,
                    "method": method
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _generate_matrix(self, data: List[Dict], fields: List[str], method: str) -> Dict:
        n = len(fields)
        matrix = [[0.0] * n for _ in range(n)]
        
        pearson = PearsonCorrelationAction()
        
        for i in range(n):
            for j in range(i, n):
                if i == j:
                    matrix[i][j] = 1.0
                else:
                    result = pearson.execute(None, {
                        "data": data,
                        "field_x": fields[i],
                        "field_y": fields[j]
                    })
                    
                    if result.success:
                        corr = result.data.get("correlation", 0)
                        matrix[i][j] = corr
                        matrix[j][i] = corr
        
        return {"rows": fields, "columns": fields, "values": matrix}


class PartialCorrelationAction(BaseAction):
    """Calculate partial correlations controlling for other variables."""
    action_type = "partial_correlation"
    display_name = "偏相关"
    description = "计算控制其他变量的偏相关系数"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field_x = params.get("field_x")
            field_y = params.get("field_y")
            control_fields = params.get("control_fields", [])
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if not field_x or not field_y:
                return ActionResult(success=False, message="field_x and field_y are required")
            
            partial_r = self._calculate_partial_correlation(data, field_x, field_y, control_fields)
            
            return ActionResult(
                success=True,
                message="Partial correlation calculated",
                data={
                    "partial_correlation": partial_r,
                    "field_x": field_x,
                    "field_y": field_y,
                    "control_fields": control_fields
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _calculate_partial_correlation(self, data: List[Dict], field_x: str, field_y: str, 
                                       control_fields: List[str]) -> float:
        if not control_fields:
            pearson = PearsonCorrelationAction()
            result = pearson.execute(None, {"data": data, "field_x": field_x, "field_y": field_y})
            return result.data.get("correlation", 0) if result.success else 0.0
        
        all_fields = [field_x, field_y] + control_fields
        
        partial_r = self._recursive_partial(data, all_fields, 0, 1, 2)
        
        return partial_r
    
    def _recursive_partial(self, data: List[Dict], fields: List[str], 
                          i: int, j: int, k: int) -> float:
        if k >= len(fields):
            r_ij = self._pearson_simple(data, fields[i], fields[j])
            return r_ij
        
        r_ik = self._recursive_partial(data, fields, i, k, k + 1)
        r_jk = self._recursive_partial(data, fields, j, k, k + 1)
        
        if abs(r_ik) < 1 and abs(r_jk) < 1:
            r_ijk = (r_ij - r_ik * r_jk) / math.sqrt((1 - r_ik ** 2) * (1 - r_jk ** 2))
        else:
            r_ijk = 0.0
        
        return r_ijk
    
    def _pearson_simple(self, data: List[Dict], field_x: str, field_y: str) -> float:
        pairs = []
        for item in data:
            if isinstance(item, dict):
                x = item.get(field_x)
                y = item.get(field_y)
                if isinstance(x, (int, float)) and isinstance(y, (int, float)):
                    pairs.append((x, y))
        
        if len(pairs) < 2:
            return 0.0
        
        n = len(pairs)
        sum_x = sum(p[0] for p in pairs)
        sum_y = sum(p[1] for p in pairs)
        sum_xy = sum(p[0] * p[1] for p in pairs)
        sum_x2 = sum(p[0] ** 2 for p in pairs)
        sum_y2 = sum(p[1] ** 2 for p in pairs)
        
        numerator = n * sum_xy - sum_x * sum_y
        denominator = math.sqrt((n * sum_x2 - sum_x ** 2) * (n * sum_y2 - sum_y ** 2))
        
        if denominator == 0:
            return 0.0
        
        return numerator / denominator
