"""
Animation smoothener utilities.

Smooth animations for natural motion.
"""

from __future__ import annotations

import math
from typing import Callable


class AnimationSmoothener:
    """Smooth animation curves."""
    
    @staticmethod
    def smooth_path(
        points: list[tuple[float, float]],
        iterations: int = 1
    ) -> list[tuple[float, float]]:
        """Smooth a path using averaging."""
        if len(points) < 3 or iterations < 1:
            return points
        
        result = list(points)
        
        for _ in range(iterations):
            smoothed = [result[0]]
            
            for i in range(1, len(result) - 1):
                x = (result[i-1][0] + result[i][0] * 2 + result[i+1][0]) / 4
                y = (result[i-1][1] + result[i][1] * 2 + result[i+1][1]) / 4
                smoothed.append((x, y))
            
            smoothed.append(result[-1])
            result = smoothed
        
        return result
    
    @staticmethod
    def catmull_rom_spline(
        points: list[tuple[float, float]],
        segments: int = 10
    ) -> list[tuple[float, float]]:
        """Create Catmull-Rom spline through points."""
        if len(points) < 4:
            return points
        
        result = []
        
        for i in range(len(points) - 3):
            p0, p1, p2, p3 = points[i:i+4]
            
            for j in range(segments):
                t = j / segments
                t2 = t * t
                t3 = t2 * t
                
                x = 0.5 * (
                    (2 * p1[0]) +
                    (-p0[0] + p2[0]) * t +
                    (2*p0[0] - 5*p1[0] + 4*p2[0] - p3[0]) * t2 +
                    (-p0[0] + 3*p1[0] - 3*p2[0] + p3[0]) * t3
                )
                
                y = 0.5 * (
                    (2 * p1[1]) +
                    (-p0[1] + p2[1]) * t +
                    (2*p0[1] - 5*p1[1] + 4*p2[1] - p3[1]) * t2 +
                    (-p0[1] + 3*p1[1] - 3*p2[1] + p3[1]) * t3
                )
                
                result.append((x, y))
        
        result.append(points[-2])
        return result
    
    @staticmethod
    def bezier_smooth(
        points: list[tuple[float, float]],
        tension: float = 0.5
    ) -> list[tuple[float, float]]:
        """Smooth points using Bezier curves."""
        if len(points) < 3:
            return points
        
        result = [points[0]]
        
        for i in range(1, len(points) - 1):
            prev = points[i-1]
            curr = points[i]
            next_p = points[i+1]
            
            dx = next_p[0] - prev[0]
            dy = next_p[1] - prev[1]
            
            cp1x = curr[0] - dx * tension / 2
            cp1y = curr[1] - dy * tension / 2
            cp2x = curr[0] + dx * tension / 2
            cp2y = curr[1] + dy * tension / 2
            
            for t in [0.25, 0.5, 0.75]:
                t1 = 1 - t
                x = t1**3 * prev[0] + 3*t1**2*t * cp1x + 3*t1*t**2 * cp2x + t**3 * curr[0]
                y = t1**3 * prev[1] + 3*t1**2*t * cp1y + 3*t1*t**2 * cp2y + t**3 * curr[1]
                result.append((x, y))
            
            result.append(curr)
        
        result.append(points[-1])
        return result
