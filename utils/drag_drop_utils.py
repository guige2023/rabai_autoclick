"""Drag and Drop Utilities.

This module provides drag and drop utilities for macOS desktop applications,
including file drop zones, drag gesture recognition, and drop target handling.

Example:
    >>> from drag_drop_utils import DropZone, DragGestureRecognizer
    >>> zone = DropZone()
    >>> zone.on_drop(handle_file_drop)
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union


class DropOperation(Enum):
    """Drag and drop operations."""
    COPY = auto()
    MOVE = auto()
    LINK = auto()
    CANCEL = auto()
    ALL = auto()


class DragModifier(Enum):
    """Modifier keys during drag."""
    NONE = 0
    OPTION = 1
    COMMAND = 2
    SHIFT = 3
    CONTROL = 4


@dataclass
class DragItem:
    """Represents a single item in a drag operation.
    
    Attributes:
        item_id: Unique item identifier
        type_identifier: Uniform Type Identifier (UTI)
        local_path: Local file path if file
        data: Raw data if not file
        preview_image: Preview image path
    """
    item_id: str
    type_identifier: str = "public.data"
    local_path: Optional[Path] = None
    data: Optional[bytes] = None
    preview_image: Optional[Path] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def is_file(self) -> bool:
        return self.local_path is not None
    
    @property
    def is_directory(self) -> bool:
        return self.local_path is not None and self.local_path.is_dir()
    
    @property
    def file_name(self) -> Optional[str]:
        return self.local_path.name if self.local_path else None
    
    @property
    def file_size(self) -> Optional[int]:
        if self.local_path and self.local_path.exists():
            return self.local_path.stat().st_size
        return None


@dataclass
class DragSession:
    """Represents an active drag session.
    
    Attributes:
        session_id: Unique session identifier
        items: List of items being dragged
        source_app: Source application bundle ID
        source_window: Source window ID
        mouse_location: Current mouse location
        modifiers: Active modifier keys
        allowed_operations: Operations allowed by source
    """
    session_id: str
    items: List[DragItem] = field(default_factory=list)
    source_app: Optional[str] = None
    source_window: Optional[int] = None
    mouse_location: Tuple[int, int] = (0, 0)
    modifiers: DragModifier = DragModifier.NONE
    allowed_operations: Set[DropOperation] = field(
        default_factory=lambda: {DropOperation.COPY, DropOperation.MOVE, DropOperation.LINK}
    )
    start_time: float = field(default_factory=lambda: 0)
    start_location: Tuple[int, int] = (0, 0)
    
    @property
    def is_file_drag(self) -> bool:
        return all(item.is_file for item in self.items)
    
    @property
    def item_count(self) -> int:
        return len(self.items)
    
    @property
    def total_size(self) -> int:
        return sum(item.file_size or 0 for item in self.items)


@dataclass
class DropRequest:
    """Request to perform a drop operation.
    
    Attributes:
        session: The drag session
        drop_location: Location to drop
        operation: Requested operation
    """
    session: DragSession
    drop_location: Tuple[int, int]
    requested_operation: DropOperation
    accepted_operation: Optional[DropOperation] = None


@dataclass
class DropResult:
    """Result of a drop operation.
    
    Attributes:
        success: Whether drop was successful
        operation: Operation that was performed
        files_created: Paths of created files
        error: Error message if failed
    """
    success: bool
    operation: Optional[DropOperation] = None
    files_created: List[Path] = field(default_factory=list)
    error: Optional[str] = None


class DropZone:
    """A drop target zone for receiving dragged content.
    
    Provides a high-level interface for registering drop zones
    and handling drop operations.
    
    Attributes:
        bounds: Zone bounds (x, y, width, height)
        accepted_types: Set of accepted UTI types
    """
    
    def __init__(
        self,
        bounds: Tuple[int, int, int, int] = (0, 0, 100, 100),
        accepted_types: Optional[Set[str]] = None,
    ):
        self.bounds = bounds
        self.accepted_types = accepted_types or {"public.file-url", "public.data"}
        
        self._highlighted = False
        self._on_drop_callbacks: List[Callable[[DropRequest], DropResult]] = []
        self._on_enter_callbacks: List[Callable[[DragSession], None]] = []
        self._on_exit_callbacks: List[Callable[[], None]] = []
        self._on_update_callbacks: List[Callable[[DragSession], None]] = []
    
    @property
    def x(self) -> int:
        return self.bounds[0]
    
    @property
    def y(self) -> int:
        return self.bounds[1]
    
    @property
    def width(self) -> int:
        return self.bounds[2]
    
    @property
    def height(self) -> int:
        return self.bounds[3]
    
    @property
    def center(self) -> Tuple[int, int]:
        return (self.x + self.width // 2, self.y + self.height // 2)
    
    def contains_point(self, x: int, y: int) -> bool:
        """Check if point is within drop zone."""
        return (self.x <= x < self.x + self.width and 
                self.y <= y < self.y + self.height)
    
    def accepts_item(self, item: DragItem) -> bool:
        """Check if drop zone accepts an item."""
        if not self.accepted_types:
            return True
        
        for accepted in self.accepted_types:
            if accepted in item.type_identifier:
                return True
        
        return False
    
    def on_drop(self, callback: Callable[[DropRequest], DropResult]) -> None:
        """Register drop callback."""
        self._on_drop_callbacks.append(callback)
    
    def on_enter(self, callback: Callable[[DragSession], None]) -> None:
        """Register drag enter callback."""
        self._on_enter_callbacks.append(callback)
    
    def on_exit(self, callback: Callable[[], None]) -> None:
        """Register drag exit callback."""
        self._on_exit_callbacks.append(callback)
    
    def on_update(self, callback: Callable[[DragSession], None]) -> None:
        """Register drag update callback."""
        self._on_update_callbacks.append(callback)
    
    def handle_drop(self, request: DropRequest) -> DropResult:
        """Handle a drop request."""
        result = DropResult(success=False)
        
        for callback in self._on_drop_callbacks:
            try:
                result = callback(request)
                if result.success:
                    break
            except Exception as e:
                result.error = str(e)
        
        return result
    
    def handle_drag_enter(self, session: DragSession) -> bool:
        """Handle drag entering zone.
        
        Returns:
            True if zone accepts the drag
        """
        self._highlighted = True
        
        for callback in self._on_enter_callbacks:
            try:
                callback(session)
            except Exception:
                pass
        
        return any(self.accepts_item(item) for item in session.items)
    
    def handle_drag_exit(self) -> None:
        """Handle drag exiting zone."""
        self._highlighted = False
        
        for callback in self._on_exit_callbacks:
            try:
                callback()
            except Exception:
                pass
    
    def handle_drag_update(self, session: DragSession) -> None:
        """Handle drag position update."""
        for callback in self._on_update_callbacks:
            try:
                callback(session)
            except Exception:
                pass
    
    def set_highlighted(self, highlighted: bool) -> None:
        """Set visual highlight state."""
        self._highlighted = highlighted


class DragGestureRecognizer:
    """Recognizes drag gestures from mouse events.
    
    Tracks mouse movement to detect drag start, movement,
    and completion.
    
    Attributes:
        threshold: Distance in pixels to trigger drag
    """
    
    def __init__(
        self,
        threshold: float = 5.0,
        delay: float = 0.0,
    ):
        self.threshold = threshold
        self.delay = delay
        
        self._is_dragging = False
        self._start_location: Tuple[int, int] = (0, 0)
        self._last_location: Tuple[int, int] = (0, 0)
        self._start_time = 0.0
        
        self._on_drag_start: Optional[Callable[[DragSession], None]] = None
        self._on_drag_move: Optional[Callable[[DragSession], None]] = None
        self._on_drag_end: Optional[Callable[[DragSession, Tuple[int, int]], None]] = None
    
    @property
    def is_dragging(self) -> bool:
        return self._is_dragging
    
    @property
    def start_location(self) -> Tuple[int, int]:
        return self._start_location
    
    def handle_mouse_down(self, x: int, y: int) -> None:
        """Handle mouse down event."""
        self._start_location = (x, y)
        self._last_location = (x, y)
        self._start_time = 0.0
    
    def handle_mouse_move(self, x: int, y: int, modifiers: DragModifier = DragModifier.NONE) -> bool:
        """Handle mouse move event.
        
        Returns:
            True if drag started
        """
        if self._is_dragging:
            self._last_location = (x, y)
            
            if self._on_drag_move:
                session = self._create_session(modifiers)
                self._on_drag_move(session)
            
            return True
        
        dx = x - self._start_location[0]
        dy = y - self._start_location[1]
        distance = (dx * dx + dy * dy) ** 0.5
        
        if distance >= self.threshold:
            self._is_dragging = True
            self._last_location = (x, y)
            self._start_time = 0.0
            
            if self._on_drag_start:
                session = self._create_session(modifiers)
                self._on_drag_start(session)
            
            return True
        
        return False
    
    def handle_mouse_up(self, x: int, y: int) -> None:
        """Handle mouse up event."""
        if self._is_dragging:
            if self._on_drag_end:
                self._on_drag_end(
                    self._create_session(DragModifier.NONE),
                    (x, y),
                )
            
            self._is_dragging = False
            self._start_time = 0.0
    
    def _create_session(self, modifiers: DragModifier) -> DragSession:
        """Create a drag session."""
        return DragSession(
            session_id="session",
            mouse_location=self._last_location,
            modifiers=modifiers,
            start_location=self._start_location,
        )
    
    def set_drag_start_handler(self, handler: Callable[[DragSession], None]) -> None:
        self._on_drag_start = handler
    
    def set_drag_move_handler(self, handler: Callable[[DragSession], None]) -> None:
        self._on_drag_move = handler
    
    def set_drag_end_handler(self, handler: Callable[[DragSession, Tuple[int, int]], None]) -> None:
        self._on_drag_end = handler


class FileDropHandler:
    """Handles file drops using macOS services."""
    
    def __init__(self, destination: Optional[Path] = None):
        self.destination = destination or Path.cwd()
    
    def copy_file(self, source: Path, operation: DropOperation = DropOperation.COPY) -> DropResult:
        """Copy a file to destination."""
        try:
            dest = self.destination / source.name
            
            if operation == DropOperation.MOVE:
                import shutil
                shutil.move(str(source), str(dest))
            else:
                import shutil
                shutil.copy2(str(source), str(dest))
            
            return DropResult(
                success=True,
                operation=operation,
                files_created=[dest],
            )
        except Exception as e:
            return DropResult(success=False, error=str(e))
    
    def copy_files(self, sources: List[Path], operation: DropOperation = DropOperation.COPY) -> DropResult:
        """Copy multiple files to destination."""
        created = []
        
        for source in sources:
            result = self.copy_file(source, operation)
            if result.success:
                created.extend(result.files_created)
            else:
                return result
        
        return DropResult(
            success=True,
            operation=operation,
            files_created=created,
        )
    
    def open_dropped_files(self, session: DragSession) -> bool:
        """Open files from a drag session using open command."""
        if not session.is_file_drag:
            return False
        
        paths = [str(item.local_path) for item in session.items if item.local_path]
        
        if paths:
            try:
                subprocess.run(['open', '-a', paths[0]] + paths[1:], check=True)
                return True
            except Exception:
                pass
        
        return False
