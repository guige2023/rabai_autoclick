"""Data Join Action Module.

Provides data joining capabilities for combining multiple data sources
including inner, left, right, and full outer joins.

Example:
    >>> from actions.data.data_join_action import DataJoinAction
    >>> action = DataJoinAction()
    >>> result = action.join(data1, data2, on="id", how="left")
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import threading


class JoinType(Enum):
    """Types of join operations."""
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"
    CROSS = "cross"
    LEFT_ANTI = "left_anti"
    RIGHT_ANTI = "right_anti"


@dataclass
class JoinConfig:
    """Configuration for join operations.
    
    Attributes:
        how: Type of join to perform
        on: Field(s) to join on
        left_suffix: Suffix for overlapping fields from left table
        right_suffix: Suffix for overlapping fields from right table
        validate: Whether to validate join keys
    """
    how: JoinType = JoinType.INNER
    on: Union[str, List[str]] = "id"
    left_suffix: str = "_x"
    right_suffix: str = "_y"
    validate: bool = True


@dataclass
class JoinResult:
    """Result of a join operation.
    
    Attributes:
        success: Whether the join succeeded
        data: Joined data
        left_count: Number of records from left table
        right_count: Number of records from right table
        joined_count: Number of joined records
        unmatched_left: Count of unmatched left records
        unmatched_right: Count of unmatched right records
        errors: List of errors
    """
    success: bool
    data: Any = None
    left_count: int = 0
    right_count: int = 0
    joined_count: int = 0
    unmatched_left: int = 0
    unmatched_right: int = 0
    errors: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataJoinAction:
    """Handles data joining operations.
    
    Provides various join types for combining data from different
    sources with support for complex key matching and validation.
    
    Example:
        >>> action = DataJoinAction()
        >>> result = action.join(left_df, right_df, on="user_id", how=JoinType.LEFT)
    """
    
    def __init__(self):
        """Initialize the data join action."""
        self._custom_matchers: Dict[str, Callable[[Any, Any], bool]] = {}
        self._lock = threading.RLock()
    
    def register_matcher(
        self,
        name: str,
        matcher_fn: Callable[[Any, Any], bool]
    ) -> "DataJoinAction":
        """Register a custom matcher function.
        
        Args:
            name: Matcher name
            matcher_fn: Function to determine if two keys match
        
        Returns:
            Self for method chaining
        """
        with self._lock:
            self._custom_matchers[name] = matcher_fn
            return self
    
    def join(
        self,
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]],
        how: Union[JoinType, str] = JoinType.INNER,
        on: Optional[Union[str, List[str], Tuple[str, str]]] = None,
        left_on: Optional[str] = None,
        right_on: Optional[str] = None,
        custom_matcher: Optional[str] = None,
        config: Optional[JoinConfig] = None
    ) -> JoinResult:
        """Join two datasets.
        
        Args:
            left: Left dataset (list of dicts)
            right: Right dataset (list of dicts)
            how: Type of join (string or JoinType)
            on: Field(s) to join on (for both tables)
            left_on: Field to join on from left table
            right_on: Field to join on from right table
            custom_matcher: Name of custom matcher to use
            config: Optional JoinConfig for detailed configuration
        
        Returns:
            JoinResult with joined data
        """
        errors: List[str] = []
        
        # Normalize join type
        if isinstance(how, str):
            try:
                how = JoinType(how.lower())
            except ValueError:
                return JoinResult(
                    success=False,
                    errors=[f"Invalid join type: {how}"]
                )
        
        # Determine key fields
        if left_on and right_on:
            left_key = left_on
            right_key = right_on
        elif on:
            if isinstance(on, tuple):
                left_key, right_key = on
            else:
                left_key = right_key = on
        else:
            errors.append("Must specify join keys using 'on', 'left_on', or 'right_on'")
            return JoinResult(success=False, errors=errors)
        
        # Build index on right table for efficient joining
        try:
            right_index = self._build_index(right, right_key)
        except Exception as e:
            return JoinResult(success=False, errors=[f"Error building right index: {str(e)}"])
        
        # Perform join based on type
        result_data: List[Dict[str, Any]] = []
        matched_right: set = set()
        unmatched_left_count = 0
        unmatched_right_count = 0
        
        for i, left_row in enumerate(left):
            left_val = self._get_field_value(left_row, left_key)
            
            if left_val is None:
                if how in (JoinType.LEFT, JoinType.FULL):
                    result_data.append(left_row.copy())
                    unmatched_left_count += 1
                continue
            
            # Find matching right rows
            right_rows = right_index.get(left_val, [])
            
            if not right_rows:
                if how in (JoinType.LEFT, JoinType.FULL):
                    result_data.append(left_row.copy())
                    unmatched_left_count += 1
                elif how == JoinType.LEFT_ANTI:
                    result_data.append(left_row.copy())
                continue
            
            # Handle custom matcher
            if custom_matcher and self._custom_matchers.get(custom_matcher):
                matcher = self._custom_matchers[custom_matcher]
                right_rows = [r for r in right_rows if matcher(left_val, self._get_field_value(r, right_key))]
            
            for right_row in right_rows:
                joined_row = self._merge_rows(left_row, right_row, left_key, right_key)
                result_data.append(joined_row)
                matched_right.add(id(right_row))
        
        # Handle unmatched right records for RIGHT and FULL joins
        if how in (JoinType.RIGHT, JoinType.FULL):
            for right_row in right:
                if id(right_row) not in matched_right:
                    unmatched_right_count += 1
                    if how == JoinType.FULL:
                        # Create row with left fields as None
                        left_keys = left[0].keys() if left else []
                        row = {f"{k}{config.right_suffix if k == right_key else ''}": None for k in left_keys}
                        row.update(right_row)
                        result_data.append(row)
        
        joined_count = len(result_data)
        
        return JoinResult(
            success=True,
            data=result_data,
            left_count=len(left),
            right_count=len(right),
            joined_count=joined_count,
            unmatched_left=unmatched_left_count,
            unmatched_right=unmatched_right_count,
            metadata={
                "join_type": how.value,
                "left_key": left_key,
                "right_key": right_key,
                "timestamp": datetime.now().isoformat()
            }
        )
    
    def join_multiple(
        self,
        datasets: List[Tuple[List[Dict[str, Any]], str, str]],
        how: Union[JoinType, str] = JoinType.INNER
    ) -> JoinResult:
        """Join multiple datasets sequentially.
        
        Args:
            datasets: List of (data, left_key, right_key) tuples
            how: Type of join for all operations
        
        Returns:
            JoinResult with final joined data
        """
        if not datasets:
            return JoinResult(success=False, errors=["No datasets provided"])
        
        if len(datasets) == 1:
            data, _, _ = datasets[0]
            return JoinResult(success=True, data=data, left_count=len(data), right_count=len(data))
        
        # Start with first dataset
        result = JoinResult(success=True, data=datasets[0][0])
        
        for i, (data, left_key, right_key) in enumerate(datasets[1:], 1):
            join_result = self.join(
                result.data,
                data,
                how=how,
                left_on=left_key,
                right_on=right_key
            )
            
            if not join_result.success:
                return join_result
            
            result = join_result
        
        return result
    
    def cross_join(
        self,
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]]
    ) -> JoinResult:
        """Perform a cross join (Cartesian product).
        
        Args:
            left: Left dataset
            right: Right dataset
        
        Returns:
            JoinResult with cross joined data
        """
        result_data = []
        
        for left_row in left:
            for right_row in right:
                joined_row = {**left_row, **right_row}
                result_data.append(joined_row)
        
        return JoinResult(
            success=True,
            data=result_data,
            left_count=len(left),
            right_count=len(right),
            joined_count=len(result_data)
        )
    
    def _build_index(
        self,
        data: List[Dict[str, Any]],
        key: str
    ) -> Dict[Any, List[Dict[str, Any]]]:
        """Build an index on a dataset for efficient joining.
        
        Args:
            data: Dataset to index
            key: Field to index on
        
        Returns:
            Dictionary mapping key values to rows
        """
        index: Dict[Any, List[Dict[str, Any]]] = {}
        
        for row in data:
            key_val = self._get_field_value(row, key)
            if key_val not in index:
                index[key_val] = []
            index[key_val].append(row)
        
        return index
    
    def _get_field_value(self, row: Dict[str, Any], field_path: str) -> Any:
        """Get a field value from a row using dot notation.
        
        Args:
            row: Data row
            field_path: Dot-separated field path
        
        Returns:
            Field value or None
        """
        if not field_path:
            return None
        
        keys = field_path.split(".")
        current = row
        
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return None
        
        return current
    
    def _merge_rows(
        self,
        left_row: Dict[str, Any],
        right_row: Dict[str, Any],
        left_key: str,
        right_key: str
    ) -> Dict[str, Any]:
        """Merge two rows, handling overlapping fields.
        
        Args:
            left_row: Left row
            right_row: Right row
            left_key: Left key field name
            right_key: Right key field name
        
        Returns:
            Merged row
        """
        result = {}
        
        # Add left row fields
        for k, v in left_row.items():
            if k == left_key:
                result[k] = v
            elif k in right_row and k != right_key:
                result[f"{k}_x"] = v
            else:
                result[k] = v
        
        # Add right row fields
        for k, v in right_row.items():
            if k == right_key:
                if k not in result:
                    result[k] = v
            elif k in left_row and k != left_key:
                result[f"{k}_y"] = v
            else:
                result[k] = v
        
        return result
