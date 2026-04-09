"""Data Funnel Action Module.

Provides data funnel capabilities for processing and transforming
data through multiple stages with filtering, aggregation, and routing.

Example:
    >>> from actions.data.data_funnel_action import DataFunnel
    >>> funnel = DataFunnel()
    >>> funnel.add_stage(filter_fn)
    >>> results = await funnel.process(input_data)
"""

from __future__ import annotations

import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, TypeVar, Generic
import threading


T = TypeVar('T')
R = TypeVar('R')


class StageType(Enum):
    """Types of funnel stages."""
    FILTER = "filter"
    TRANSFORM = "transform"
    AGGREGATE = "aggregate"
    SPLIT = "split"
    ROUTE = "route"
    BUFFER = "buffer"
    DEDUP = "dedup"
    VALIDATE = "validate"


class FunnelStatus(Enum):
    """Status of funnel processing."""
    IDLE = "idle"
    PROCESSING = "processing"
    PAUSED = "paused"
    COMPLETED = "completed"
    ERROR = "error"


@dataclass
class FunnelStage:
    """A stage in the data funnel.
    
    Attributes:
        stage_id: Unique stage identifier
        name: Stage name
        stage_type: Type of processing stage
        handler: Processing function
        config: Stage-specific configuration
        enabled: Whether stage is enabled
    """
    stage_id: str
    name: str
    stage_type: StageType
    handler: Callable
    config: Dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
    error_handler: Optional[Callable] = None


@dataclass
class FunnelItem:
    """An item flowing through the funnel.
    
    Attributes:
        item_id: Unique item identifier
        data: Item payload
        metadata: Item metadata
        current_stage: Current processing stage
        stage_history: List of stages processed
        timestamp: When item entered funnel
    """
    item_id: str
    data: Any
    metadata: Dict[str, Any] = field(default_factory=dict)
    current_stage: Optional[str] = None
    stage_history: List[str] = field(default_factory=list)
    timestamp: datetime = field(default_factory=datetime.now)
    error: Optional[str] = None


@dataclass
class FunnelResult:
    """Result of funnel processing.
    
    Attributes:
        total_input: Number of input items
        total_output: Number of output items
        filtered_count: Number of filtered items
        error_count: Number of items with errors
        stage_stats: Statistics per stage
        duration: Total processing duration
        items_by_route: Items routed to different outputs
    """
    total_input: int = 0
    total_output: int = 0
    filtered_count: int = 0
    error_count: int = 0
    stage_stats: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    duration: float = 0.0
    items_by_route: Dict[str, List[Any]] = field(default_factory=dict)


@dataclass
class FunnelConfig:
    """Configuration for funnel processing.
    
    Attributes:
        parallel_stages: Maximum parallel stage workers
        buffer_size: Size of stage buffers
        enable_metrics: Whether to collect metrics
        stop_on_error: Whether to stop on first error
        dedup_window: Time window for deduplication
        timeout: Per-item timeout
    """
    parallel_stages: int = 4
    buffer_size: int = 1000
    enable_metrics: bool = True
    stop_on_error: bool = False
    dedup_window: float = 60.0
    timeout: float = 30.0


class DataFunnel:
    """Processes data through configurable funnel stages.
    
    Supports filtering, transformation, aggregation, routing,
    and other processing stages in a pipeline.
    
    Attributes:
        config: Funnel configuration
    
    Example:
        >>> funnel = DataFunnel()
        >>> funnel.add_filter("remove_nulls", lambda x: x is not None)
        >>> funnel.add_transform("to_upper", lambda x: x.upper())
        >>> result = await funnel.process(["a", None, "b"])
    """
    
    def __init__(self, config: Optional[FunnelConfig] = None):
        """Initialize the data funnel.
        
        Args:
            config: Funnel configuration
        """
        self.config = config or FunnelConfig()
        self._stages: List[FunnelStage] = []
        self._stage_counter = 0
        self._item_counter = 0
        self._status = FunnelStatus.IDLE
        self._metrics: Dict[str, Any] = defaultdict(int)
        self._stage_metrics: Dict[str, Dict[str, Any]] = defaultdict(lambda: defaultdict(int))
        self._dedup_cache: Dict[str, datetime] = {}
        self._lock = threading.RLock()
    
    def add_filter(
        self,
        name: str,
        filter_fn: Callable[[Any], bool],
        error_handler: Optional[Callable] = None
    ) -> str:
        """Add a filter stage.
        
        Args:
            name: Stage name
            filter_fn: Function returning True to keep item
            error_handler: Optional error handler
        
        Returns:
            Stage ID
        """
        return self._add_stage(name, StageType.FILTER, filter_fn, error_handler=error_handler)
    
    def add_transform(
        self,
        name: str,
        transform_fn: Callable[[Any], Any],
        error_handler: Optional[Callable] = None
    ) -> str:
        """Add a transform stage.
        
        Args:
            name: Stage name
            transform_fn: Transform function
            error_handler: Optional error handler
        
        Returns:
            Stage ID
        """
        return self._add_stage(name, StageType.TRANSFORM, transform_fn, error_handler=error_handler)
    
    def add_aggregate(
        self,
        name: str,
        agg_fn: Callable[[List[Any]], Any],
        window_size: Optional[int] = None,
        error_handler: Optional[Callable] = None
    ) -> str:
        """Add an aggregation stage.
        
        Args:
            name: Stage name
            agg_fn: Aggregation function
            window_size: Optional window size for batch aggregation
            error_handler: Optional error handler
        
        Returns:
            Stage ID
        """
        config = {"window_size": window_size}
        return self._add_stage(
            name,
            StageType.AGGREGATE,
            agg_fn,
            config=config,
            error_handler=error_handler
        )
    
    def add_split(
        self,
        name: str,
        split_fn: Callable[[Any], List[str]],
        error_handler: Optional[Callable] = None
    ) -> str:
        """Add a split stage that routes items to multiple outputs.
        
        Args:
            name: Stage name
            split_fn: Function returning list of route names
            error_handler: Optional error handler
        
        Returns:
            Stage ID
        """
        return self._add_stage(name, StageType.SPLIT, split_fn, error_handler=error_handler)
    
    def add_route(
        self,
        name: str,
        route_fn: Callable[[Any], str],
        routes: Optional[List[str]] = None,
        error_handler: Optional[Callable] = None
    ) -> str:
        """Add a routing stage that sends items to one output.
        
        Args:
            name: Stage name
            route_fn: Function returning route name
            routes: List of valid route names
            error_handler: Optional error handler
        
        Returns:
            Stage ID
        """
        config = {"routes": routes or []}
        return self._add_stage(
            name,
            StageType.ROUTE,
            route_fn,
            config=config,
            error_handler=error_handler
        )
    
    def add_dedup(
        self,
        name: str,
        key_fn: Callable[[Any], str],
        window: Optional[float] = None,
        error_handler: Optional[Callable] = None
    ) -> str:
        """Add a deduplication stage.
        
        Args:
            name: Stage name
            key_fn: Function to extract deduplication key
            window: Deduplication time window in seconds
            error_handler: Optional error handler
        
        Returns:
            Stage ID
        """
        config = {"window": window or self.config.dedup_window}
        return self._add_stage(
            name,
            StageType.DEDUP,
            key_fn,
            config=config,
            error_handler=error_handler
        )
    
    def add_validate(
        self,
        name: str,
        validator_fn: Callable[[Any], bool],
        error_handler: Optional[Callable] = None
    ) -> str:
        """Add a validation stage that filters invalid items.
        
        Args:
            name: Stage name
            validator_fn: Function returning True if valid
            error_handler: Optional error handler
        
        Returns:
            Stage ID
        """
        return self._add_stage(name, StageType.VALIDATE, validator_fn, error_handler=error_handler)
    
    def _add_stage(
        self,
        name: str,
        stage_type: StageType,
        handler: Callable,
        config: Optional[Dict[str, Any]] = None,
        error_handler: Optional[Callable] = None
    ) -> str:
        """Add a generic stage.
        
        Args:
            name: Stage name
            stage_type: Type of stage
            handler: Processing function
            config: Stage configuration
            error_handler: Optional error handler
        
        Returns:
            Stage ID
        """
        self._stage_counter += 1
        stage_id = f"stage_{self._stage_counter}"
        
        stage = FunnelStage(
            stage_id=stage_id,
            name=name,
            stage_type=stage_type,
            handler=handler,
            config=config or {},
            error_handler=error_handler
        )
        
        with self._lock:
            self._stages.append(stage)
        
        return stage_id
    
    def enable_stage(self, stage_id: str) -> None:
        """Enable a stage.
        
        Args:
            stage_id: Stage to enable
        """
        with self._lock:
            for stage in self._stages:
                if stage.stage_id == stage_id:
                    stage.enabled = True
                    break
    
    def disable_stage(self, stage_id: str) -> None:
        """Disable a stage.
        
        Args:
            stage_id: Stage to disable
        """
        with self._lock:
            for stage in self._stages:
                if stage.stage_id == stage_id:
                    stage.enabled = False
                    break
    
    async def process(
        self,
        input_data: List[Any],
        output_handler: Optional[Callable[[str, Any], None]] = None
    ) -> FunnelResult:
        """Process data through the funnel.
        
        Args:
            input_data: Items to process
            output_handler: Optional handler for routed outputs
        
        Returns:
            FunnelResult with processing statistics
        """
        start_time = time.time()
        self._status = FunnelStatus.PROCESSING
        
        result = FunnelResult(total_input=len(input_data))
        items_by_route: Dict[str, List[Any]] = defaultdict(list)
        
        # Initialize route outputs
        for stage in self._stages:
            if stage.stage_type in (StageType.SPLIT, StageType.ROUTE):
                result.items_by_route[stage.name] = []
        
        try:
            for item in input_data:
                self._item_counter += 1
                item_id = f"item_{self._item_counter}"
                
                funnel_item = FunnelItem(
                    item_id=item_id,
                    data=item,
                    metadata={"source_index": len(result.stage_stats.get("_input", []))}
                )
                
                try:
                    processed = await self._process_item(funnel_item, result)
                    
                    if processed.error:
                        result.error_count += 1
                    elif processed.data is None:
                        result.filtered_count += 1
                    else:
                        result.total_output += 1
                        route = processed.metadata.get("_route", "default")
                        items_by_route[route].append(processed.data)
                        
                        if output_handler:
                            await output_handler(route, processed.data)
                
                except Exception as e:
                    result.error_count += 1
                    if self.config.stop_on_error:
                        raise
        
        except Exception as e:
            self._status = FunnelStatus.ERROR
            raise
        finally:
            self._status = FunnelStatus.COMPLETED
            result.duration = time.time() - start_time
        
        # Update result with routed items
        result.items_by_route = dict(items_by_route)
        
        return result
    
    async def _process_item(self, item: FunnelItem, result: FunnelResult) -> FunnelItem:
        """Process a single item through all stages.
        
        Args:
            item: Item to process
            result: Result to update
        
        Returns:
            Processed item
        """
        data = item.data
        
        for stage in self._stages:
            if not stage.enabled:
                continue
            
            item.current_stage = stage.stage_id
            
            try:
                stage_start = time.time()
                
                data = await self._execute_stage(stage, data, item, result)
                
                stage_duration = time.time() - stage_start
                
                # Record stage metrics
                if self.config.enable_metrics:
                    self._record_stage_metric(stage.stage_id, stage_duration)
                
                if data is None:
                    item.metadata["_filtered_at"] = stage.name
                    break
                
                item.stage_history.append(stage.stage_id)
            
            except Exception as e:
                item.error = f"{stage.name}: {str(e)}"
                
                if stage.error_handler:
                    try:
                        data = stage.error_handler(data, e)
                        if data is None:
                            item.metadata["_filtered_at"] = stage.name
                            break
                    except Exception:
                        pass
                elif self.config.stop_on_error:
                    raise
        
        item.data = data
        return item
    
    async def _execute_stage(
        self,
        stage: FunnelStage,
        data: Any,
        item: FunnelItem,
        result: FunnelResult
    ) -> Any:
        """Execute a single stage.
        
        Args:
            stage: Stage to execute
            data: Current item data
            item: Funnel item
            result: Result to update
        
        Returns:
            Processed data or None if filtered
        """
        if stage.stage_type == StageType.FILTER:
            if asyncio.iscoroutinefunction(stage.handler):
                keep = await stage.handler(data)
            else:
                keep = stage.handler(data)
            return data if keep else None
        
        elif stage.stage_type == StageType.TRANSFORM:
            if asyncio.iscoroutinefunction(stage.handler):
                return await stage.handler(data)
            return stage.handler(data)
        
        elif stage.stage_type == StageType.VALIDATE:
            if asyncio.iscoroutinefunction(stage.handler):
                valid = await stage.handler(data)
            else:
                valid = stage.handler(data)
            return data if valid else None
        
        elif stage.stage_type == StageType.DEDUP:
            key = data if not asyncio.iscoroutinefunction(stage.handler) else stage.handler(data)
            if asyncio.iscoroutinefunction(stage.handler):
                key = await stage.handler(data) if asyncio.iscoroutinefunction(stage.handler) else data
            else:
                key = stage.handler(data)
            
            window = stage.config.get("window", self.config.dedup_window)
            now = datetime.now()
            
            # Clean old entries
            cutoff = now.timestamp() - window
            self._dedup_cache = {
                k: v for k, v in self._dedup_cache.items()
                if v.timestamp() > cutoff
            }
            
            if key in self._dedup_cache:
                return None  # Duplicate
            
            self._dedup_cache[key] = now
            return data
        
        elif stage.stage_type in (StageType.SPLIT, StageType.ROUTE):
            if asyncio.iscoroutinefunction(stage.handler):
                routes = await stage.handler(data)
            else:
                routes = stage.handler(data)
            
            if stage.stage_type == StageType.SPLIT:
                # Splits go to multiple routes - process each
                if isinstance(routes, list):
                    item.metadata["_route"] = routes[0] if routes else "default"
                    return data  # Caller handles routing
            else:
                # Routes go to one output
                if isinstance(routes, str):
                    item.metadata["_route"] = routes
            return data
        
        elif stage.stage_type == StageType.AGGREGATE:
            # Aggregation handled separately
            return data
        
        return data
    
    def _record_stage_metric(self, stage_id: str, duration: float) -> None:
        """Record metrics for a stage.
        
        Args:
            stage_id: Stage identifier
            duration: Processing duration
        """
        self._stage_metrics[stage_id]["count"] += 1
        self._stage_metrics[stage_id]["total_duration"] += duration
        self._stage_metrics[stage_id]["avg_duration"] = (
            self._stage_metrics[stage_id]["total_duration"] /
            self._stage_metrics[stage_id]["count"]
        )
    
    def get_stage_metrics(self) -> Dict[str, Dict[str, Any]]:
        """Get metrics for all stages.
        
        Returns:
            Dictionary of stage metrics
        """
        return dict(self._stage_metrics)
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get funnel statistics.
        
        Returns:
            Statistics dictionary
        """
        return {
            "status": self._status.value,
            "total_stages": len(self._stages),
            "enabled_stages": sum(1 for s in self._stages if s.enabled),
            "total_items_processed": self._item_counter,
            "stage_metrics": self.get_stage_metrics()
        }
    
    def clear(self) -> None:
        """Clear funnel state."""
        with self._lock:
            self._dedup_cache.clear()
            self._metrics.clear()
            self._stage_metrics.clear()
            self._item_counter = 0


import asyncio
