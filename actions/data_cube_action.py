"""
Data Cube Action Module

Implements OLAP-style data cube operations for multi-dimensional
aggregation, roll-up, drill-down, and slice-and-dice operations.

Author: RabAi Team
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import logging

logger = logging.getLogger(__name__)


class AggregationFunc(Enum):
    """Aggregation functions for cube operations."""

    SUM = auto()
    COUNT = auto()
    AVG = auto()
    MIN = auto()
    MAX = auto()
    COUNT_DISTINCT = auto()
    MEDIAN = auto()


@dataclass
class CubeDimension:
    """A dimension axis in the data cube."""

    name: str
    values: List[Any] = field(default_factory=list)
    hierarchy: Optional[List[str]] = None

    def __hash__(self) -> int:
        return hash(self.name)


@dataclass
class CubeMeasure:
    """A measure in the data cube."""

    name: str
    aggregation: AggregationFunc = AggregationFunc.SUM
    format_str: Optional[str] = None

    def __hash__(self) -> int:
        return hash(self.name)


@dataclass
class CubeCell:
    """A single cell in the data cube."""

    coordinates: Dict[str, Any]
    measures: Dict[str, Union[int, float]]
    cell_count: int = 1

    def __getitem__(self, key: str) -> Any:
        if key in self.coordinates:
            return self.coordinates[key]
        if key in self.measures:
            return self.measures[key]
        raise KeyError(key)


@dataclass
class DataCube:
    """Multi-dimensional data cube."""

    dimensions: List[CubeDimension]
    measures: List[CubeMeasure]
    cells: Dict[Tuple, CubeCell] = field(default_factory=dict)
    total_count: int = 0

    def _coord_key(self, coords: Dict[str, Any]) -> Tuple:
        """Create a hashable key from coordinate dict."""
        dim_names = [d.name for d in self.dimensions]
        return tuple(coord for d in dim_names for coord in (coords.get(d),))

    def get_cell(self, coords: Dict[str, Any]) -> Optional[CubeCell]:
        """Get cell at given coordinates."""
        return self.cells.get(self._coord_key(coords))

    def set_cell(self, coords: Dict[str, Any], cell: CubeCell) -> None:
        """Set cell at given coordinates."""
        self.cells[self._coord_key(coords)] = cell


class DataCubeBuilder:
    """Builds a data cube from raw records."""

    def __init__(
        self,
        dimensions: List[str],
        measures: List[Tuple[str, AggregationFunc]],
    ) -> None:
        self.dimension_names = dimensions
        self.measure_specs = measures
        self._raw_data: List[Dict[str, Any]] = []

    def add_record(self, record: Dict[str, Any]) -> None:
        """Add a raw record to the cube builder."""
        self._raw_data.append(record)

    def add_records(self, records: List[Dict[str, Any]]) -> None:
        """Add multiple raw records."""
        self._raw_data.extend(records)

    def build(self) -> DataCube:
        """Build the complete data cube."""
        dims = [CubeDimension(name=n) for n in self.dimension_names]
        meas = [CubeMeasure(name=n, aggregation=agg) for n, agg in self.measure_specs]

        cube = DataCube(dimensions=dims, measures=meas)
        agg_values: Dict[Tuple, Dict[str, List[Any]]] = defaultdict(lambda: defaultdict(list))

        for record in self._raw_data:
            coords = {d: record.get(d) for d in self.dimension_names}
            key = cube._coord_key(coords)

            for measure_name, _ in self.measure_specs:
                agg_values[key][measure_name].append(record.get(measure_name, 0))

            cube.total_count += 1

        for key, measures in agg_values.items():
            dim_names = [d.name for d in dims]
            coords = dict(zip(dim_names, key))

            computed_measures = {}
            for measure_name, agg_func in self.measure_specs:
                values = measures.get(measure_name, [])
                computed_measures[measure_name] = self._aggregate(values, agg_func)

            cube.set_cell(
                coords,
                CubeCell(coordinates=coords, measures=computed_measures, cell_count=len(values)),
            )

        for dim in dims:
            dim.values = sorted(set(c for c in (cell.coordinates.get(dim.name) for cell in cube.cells.values()) if c is not None))

        return cube

    def _aggregate(
        self,
        values: List[Any],
        func: AggregationFunc,
    ) -> Union[int, float]:
        """Aggregate values using the specified function."""
        if not values:
            return 0
        numeric_values = [v for v in values if isinstance(v, (int, float))]
        if not numeric_values:
            return len(values) if func == AggregationFunc.COUNT else 0

        if func == AggregationFunc.SUM:
            return sum(numeric_values)
        elif func == AggregationFunc.COUNT:
            return len(values)
        elif func == AggregationFunc.AVG:
            return sum(numeric_values) / len(numeric_values)
        elif func == AggregationFunc.MIN:
            return min(numeric_values)
        elif func == AggregationFunc.MAX:
            return max(numeric_values)
        elif func == AggregationFunc.MEDIAN:
            sorted_vals = sorted(numeric_values)
            n = len(sorted_vals)
            if n % 2 == 0:
                return (sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2
            return sorted_vals[n // 2]
        return 0


class DataCubeAction:
    """Action class for data cube operations."""

    def __init__(self) -> None:
        self._cubes: Dict[str, DataCube] = {}

    def create_cube(
        self,
        cube_id: str,
        dimensions: List[str],
        measures: List[Tuple[str, AggregationFunc]],
    ) -> DataCubeBuilder:
        """Create a new data cube and return builder."""
        builder = DataCubeBuilder(dimensions, measures)
        return builder

    def rollup(
        self,
        cube: DataCube,
        rollup_dimensions: List[str],
    ) -> DataCube:
        """Roll up the cube along specified dimensions."""
        remaining_dims = [d for d in cube.dimensions if d.name not in rollup_dimensions]
        remaining_dim_names = [d.name for d in remaining_dims]

        rolled: Dict[Tuple, Dict[str, List[Any]]] = defaultdict(lambda: defaultdict(list))
        new_total = 0

        for cell in cube.cells.values():
            new_coords = {k: v for k, v in cell.coordinates.items() if k in remaining_dim_names}
            key = tuple(new_coords.get(d) for d in remaining_dim_names)

            for measure in cube.measures:
                rolled[key][measure.name].append(cell.measures.get(measure.name, 0))

            new_total += 1

        new_cube = DataCube(
            dimensions=remaining_dims,
            measures=cube.measures,
            total_count=new_total,
        )

        for key, measures in rolled.items():
            coords = dict(zip(remaining_dim_names, key))
            computed = {}
            for measure in cube.measures:
                values = measures[measure.name]
                if measure.aggregation == AggregationFunc.SUM:
                    computed[measure.name] = sum(values)
                elif measure.aggregation == AggregationFunc.COUNT:
                    computed[measure.name] = len(values)
                elif measure.aggregation == AggregationFunc.AVG:
                    computed[measure.name] = sum(values) / len(values) if values else 0
                elif measure.aggregation == AggregationFunc.MIN:
                    computed[measure.name] = min(values)
                elif measure.aggregation == AggregationFunc.MAX:
                    computed[measure.name] = max(values)
            new_cube.set_cell(coords, CubeCell(coordinates=coords, measures=computed))

        return new_cube

    def slice(
        self,
        cube: DataCube,
        dimension: str,
        value: Any,
    ) -> DataCube:
        """Slice the cube at a specific dimension value."""
        sliced_cells = {
            k: cell for k, cell in cube.cells.items()
            if dimension in cell.coordinates and cell.coordinates[dimension] == value
        }

        new_cube = DataCube(
            dimensions=cube.dimensions,
            measures=cube.measures,
            cells=sliced_cells,
            total_count=len(sliced_cells),
        )
        return new_cube

    def drilldown(
        self,
        cube: DataCube,
        drilldown_dimensions: List[str],
    ) -> DataCube:
        """Drill down to more granular dimensions (alias for identity with added dims)."""
        return cube

    def export_cube(self, cube: DataCube) -> List[Dict[str, Any]]:
        """Export cube cells as list of dicts."""
        return [
            {**cell.coordinates, **cell.measures}
            for cell in cube.cells.values()
        ]
