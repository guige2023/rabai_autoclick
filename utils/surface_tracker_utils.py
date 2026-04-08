"""
Surface Tracker Utilities

Provides utilities for tracking UI surfaces
in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class Surface:
    """A UI surface (window, panel, etc.)."""
    surface_id: str
    name: str
    bounds: tuple[int, int, int, int]
    z_order: int = 0


class SurfaceTracker:
    """
    Tracks UI surfaces and their relationships.
    
    Maintains z-order and spatial relationships
    between surfaces.
    """

    def __init__(self) -> None:
        self._surfaces: dict[str, Surface] = {}
        self._z_counter = 0

    def register_surface(
        self,
        surface_id: str,
        name: str,
        bounds: tuple[int, int, int, int],
    ) -> None:
        """Register a new surface."""
        self._z_counter += 1
        self._surfaces[surface_id] = Surface(
            surface_id=surface_id,
            name=name,
            bounds=bounds,
            z_order=self._z_counter,
        )

    def unregister_surface(self, surface_id: str) -> None:
        """Unregister a surface."""
        self._surfaces.pop(surface_id, None)

    def get_surface(self, surface_id: str) -> Surface | None:
        """Get surface by ID."""
        return self._surfaces.get(surface_id)

    def get_all_surfaces(self) -> list[Surface]:
        """Get all surfaces sorted by z-order."""
        return sorted(self._surfaces.values(), key=lambda s: s.z_order)

    def bring_to_front(self, surface_id: str) -> None:
        """Bring surface to front."""
        if surface_id in self._surfaces:
            self._z_counter += 1
            self._surfaces[surface_id].z_order = self._z_counter

    def get_surface_at_point(self, x: int, y: int) -> Surface | None:
        """Get topmost surface at point."""
        surfaces = self.get_all_surfaces()
        for surface in reversed(surfaces):
            sx, sy, sw, sh = surface.bounds
            if sx <= x <= sx + sw and sy <= y <= sy + sh:
                return surface
        return None
