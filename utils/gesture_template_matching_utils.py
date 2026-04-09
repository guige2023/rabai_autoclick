"""
Gesture template matching utilities.

Match recorded gestures against predefined templates.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional


@dataclass
class TemplatePoint:
    """A point in a gesture template."""
    x: float
    y: float
    pressure: float = 1.0


@dataclass
class GestureTemplate:
    """A predefined gesture template."""
    name: str
    points: list[TemplatePoint]
    tolerance: float = 0.15


@dataclass
class MatchResult:
    """Result of gesture template matching."""
    template_name: str
    similarity: float
    matched: bool
    offset_x: float = 0
    offset_y: float = 0
    scale: float = 1.0


class GestureTemplateMatcher:
    """Match gestures against templates."""
    
    def __init__(self):
        self._templates: dict[str, GestureTemplate] = {}
    
    def register_template(self, template: GestureTemplate) -> None:
        """Register a gesture template."""
        self._templates[template.name] = template
    
    def unregister_template(self, name: str) -> None:
        """Unregister a template."""
        self._templates.pop(name, None)
    
    def match(
        self,
        gesture: list[tuple[float, float]],
        min_similarity: float = 0.8
    ) -> Optional[MatchResult]:
        """Match a gesture against all templates."""
        if not gesture or not self._templates:
            return None
        
        best_match: Optional[MatchResult] = None
        best_similarity = 0.0
        
        for template in self._templates.values():
            result = self._match_against_template(gesture, template)
            if result and result.similarity > best_similarity:
                best_similarity = result.similarity
                best_match = result
        
        if best_match and best_match.similarity >= min_similarity:
            best_match.matched = True
            return best_match
        
        return None
    
    def _match_against_template(
        self,
        gesture: list[tuple[float, float]],
        template: GestureTemplate
    ) -> Optional[MatchResult]:
        """Match against a single template."""
        if not gesture or not template.points:
            return None
        
        normalized_gesture = self._normalize_path(gesture)
        normalized_template = self._normalize_path([(p.x, p.y) for p in template.points])
        
        if len(normalized_gesture) != len(normalized_template):
            normalized_gesture = self._resample(normalized_gesture, len(normalized_template))
        
        offset_x, offset_y, scale = self._calculate_transform(
            normalized_gesture, normalized_template
        )
        
        transformed = [
            (x * scale + offset_x, y * scale + offset_y)
            for x, y in normalized_gesture
        ]
        
        similarity = self._calculate_similarity(transformed, normalized_template)
        
        return MatchResult(
            template_name=template.name,
            similarity=similarity,
            matched=similarity >= template.tolerance,
            offset_x=offset_x,
            offset_y=offset_y,
            scale=scale
        )
    
    def _normalize_path(self, path: list[tuple[float, float]]) -> list[tuple[float, float]]:
        """Normalize path to standard size."""
        if not path:
            return []
        
        min_x = min(p[0] for p in path)
        max_x = max(p[0] for p in path)
        min_y = min(p[1] for p in path)
        max_y = max(p[1] for p in path)
        
        width = max_x - min_x or 1
        height = max_y - min_y or 1
        scale = max(width, height)
        
        return [
            ((p[0] - min_x) / scale, (p[1] - min_y) / scale)
            for p in path
        ]
    
    def _resample(
        self,
        path: list[tuple[float, float]],
        num_points: int
    ) -> list[tuple[float, float]]:
        """Resample path to have a specific number of points."""
        if len(path) < 2 or num_points < 2:
            return path
        
        total_length = sum(
            math.sqrt((path[i+1][0] - path[i][0])**2 + (path[i+1][1] - path[i][1])**2)
            for i in range(len(path) - 1)
        )
        
        interval = total_length / (num_points - 1)
        result = [path[0]]
        accumulated = 0.0
        path_index = 0
        
        for i in range(1, num_points):
            target = interval * i
            
            while path_index < len(path) - 1:
                seg_length = math.sqrt(
                    (path[path_index+1][0] - path[path_index][0])**2 +
                    (path[path_index+1][1] - path[path_index][1])**2
                )
                
                if accumulated + seg_length >= target:
                    t = (target - accumulated) / seg_length if seg_length > 0 else 0
                    x = path[path_index][0] + t * (path[path_index+1][0] - path[path_index][0])
                    y = path[path_index][1] + t * (path[path_index+1][1] - path[path_index][1])
                    result.append((x, y))
                    break
                
                accumulated += seg_length
                path_index += 1
            else:
                result.append(path[-1])
        
        return result
    
    def _calculate_transform(
        self,
        gesture: list[tuple[float, float]],
        template: list[tuple[float, float]]
    ) -> tuple[float, float, float]:
        """Calculate transformation to align gesture with template."""
        gesture_center = (
            sum(p[0] for p in gesture) / len(gesture),
            sum(p[1] for p in gesture) / len(gesture)
        )
        template_center = (
            sum(p[0] for p in template) / len(template),
            sum(p[1] for p in template) / len(template)
        )
        
        offset_x = template_center[0] - gesture_center[0]
        offset_y = template_center[1] - gesture_center[1]
        
        return offset_x, offset_y, 1.0
    
    def _calculate_similarity(
        self,
        gesture: list[tuple[float, float]],
        template: list[tuple[float, float]]
    ) -> float:
        """Calculate similarity between gesture and template."""
        if len(gesture) != len(template):
            return 0.0
        
        total_distance = sum(
            math.sqrt((gesture[i][0] - template[i][0])**2 + (gesture[i][1] - template[i][1])**2)
            for i in range(len(gesture))
        )
        
        avg_distance = total_distance / len(gesture)
        similarity = max(0.0, 1.0 - avg_distance * 2)
        
        return similarity
