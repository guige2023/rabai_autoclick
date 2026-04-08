"""
Data Science utilities - pandas DataFrame operations, statistics, and data manipulation.
"""
from typing import Any, Dict, List, Optional, Tuple, Union
import logging

try:
    import pandas as pd
    import numpy as np
    HAS_DEPS = True
except ImportError:
    HAS_DEPS = False

logger = logging.getLogger(__name__)


class BaseAction:
    """Base class for all actions."""

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


class DataScienceAction(BaseAction):
    """Data science operations using pandas and numpy.

    Handles DataFrame creation, manipulation, aggregation, and statistical analysis.
    Requires: pip install pandas numpy
    """

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        operation = params.get("operation", "describe")
        data = params.get("data", [])
        columns = params.get("columns", [])
        column = params.get("column", "")
        value = params.get("value")
        by = params.get("by", "")
        func = params.get("func", "mean")
        how = params.get("how", "inner")
        fill_value = params.get("fill_value")

        if not HAS_DEPS:
            return {"success": False, "error": "pandas and numpy required: pip install pandas numpy"}

        try:
            if operation == "create_dataframe":
                df = pd.DataFrame(data)
                if columns:
                    df.columns = columns
                return {"success": True, "data": df.to_dict(orient="records"), "shape": df.shape}

            elif operation == "describe":
                if not data:
                    return {"success": False, "error": "data required for describe"}
                df = pd.DataFrame(data)
                desc = df.describe().to_dict()
                dtypes = {k: str(v) for k, v in df.dtypes.to_dict().items()}
                return {"success": True, "description": desc, "dtypes": dtypes, "shape": list(df.shape)}

            elif operation == "filter":
                df = pd.DataFrame(data)
                if column and value is not None:
                    mask = df[column] == value
                    result = df[mask]
                    return {"success": True, "data": result.to_dict(orient="records"), "count": len(result)}

            elif operation == "groupby_aggregate":
                df = pd.DataFrame(data)
                if not by or not column:
                    return {"success": False, "error": "by and column required"}
                agg_funcs = {"mean": "mean", "sum": "sum", "count": "count", "min": "min", "max": "max", "std": "std"}
                result = df.groupby(by)[column].agg(agg_funcs.get(func, "mean"))
                return {"success": True, "data": result.to_dict(), "func": func}

            elif operation == "sort":
                df = pd.DataFrame(data)
                by_col = by if by else (columns[0] if columns else df.columns[0])
                ascending = params.get("ascending", True)
                result = df.sort_values(by=by_col, ascending=ascending)
                return {"success": True, "data": result.to_dict(orient="records")}

            elif operation == "merge":
                other = params.get("other", [])
                df1 = pd.DataFrame(data)
                df2 = pd.DataFrame(other)
                result = pd.merge(df1, df2, on=by, how=how)
                return {"success": True, "data": result.to_dict(orient="records"), "shape": list(result.shape)}

            elif operation == "pivot":
                df = pd.DataFrame(data)
                index = params.get("index")
                columns = params.get("pivot_columns")
                values = params.get("values")
                result = pd.pivot_table(df, index=index, columns=columns, values=values, fill_value=fill_value)
                return {"success": True, "data": result.to_dict()}

            elif operation == "melt":
                df = pd.DataFrame(data)
                id_vars = params.get("id_vars", [])
                value_vars = params.get("value_vars", [])
                result = pd.melt(df, id_vars=id_vars, value_vars=value_vars)
                return {"success": True, "data": result.to_dict(orient="records")}

            elif operation == "fillna":
                df = pd.DataFrame(data)
                fill_val = params.get("fill_value", 0)
                result = df.fillna(fill_val)
                return {"success": True, "data": result.to_dict(orient="records")}

            elif operation == "drop_duplicates":
                df = pd.DataFrame(data)
                subset = params.get("subset", None)
                result = df.drop_duplicates(subset=subset)
                return {"success": True, "data": result.to_dict(orient="records"), "removed": len(df) - len(result)}

            elif operation == "corr":
                df = pd.DataFrame(data)
                method = params.get("method", "pearson")
                result = df.corr(method=method)
                return {"success": True, "correlation": result.to_dict()}

            elif operation == "stats":
                if not data:
                    return {"success": False, "error": "data required"}
                arr = np.array(data)
                return {
                    "success": True,
                    "stats": {
                        "mean": float(np.mean(arr)),
                        "median": float(np.median(arr)),
                        "std": float(np.std(arr)),
                        "min": float(np.min(arr)),
                        "max": float(np.max(arr)),
                        "q25": float(np.percentile(arr, 25)),
                        "q75": float(np.percentile(arr, 75)),
                    }
                }

            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}

        except Exception as e:
            logger.error(f"DataScienceAction error: {e}")
            return {"success": False, "error": str(e)}


def execute(context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """Entry point for data science operations."""
    return DataScienceAction().execute(context, params)
