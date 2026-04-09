"""Image feature matcher for template matching with feature descriptors."""
from typing import List, Dict, Optional, Any
import math


class ImageFeatureMatcher:
    """Matches image features using various feature descriptors.
    
    Provides feature-based image matching for robust template
    matching even when exact pixel matching fails.
    
    Example:
        matcher = ImageFeatureMatcher()
        match = matcher.match_features(template, screenshot, threshold=0.8)
        if match:
            print(f"Found at {match['position']}")
    """

    def __init__(self, method: str = "orb") -> None:
        self._method = method

    def detect_features(self, image: Any) -> List[Dict]:
        """Detect features in an image (stub)."""
        return []

    def match_features(
        self,
        template: Any,
        image: Any,
        threshold: float = 0.75,
    ) -> Optional[Dict[str, Any]]:
        """Match template features against image features."""
        template_features = self.detect_features(template)
        image_features = self.detect_features(image)
        if not template_features or not image_features:
            return None
        good_matches = []
        for tf in template_features:
            best = None
            best_dist = float("inf")
            for img_f in image_features:
                dist = self._compute_distance(tf, img_f)
                if dist < best_dist:
                    best_dist = dist
                    best = img_f
            if best and best_dist < (1 - threshold) * 100:
                good_matches.append((tf, best, best_dist))
        if len(good_matches) >= max(4, len(template_features) * threshold):
            points = [m[1].get("point", (0, 0)) for m in good_matches]
            cx = sum(p[0] for p in points) / len(points)
            cy = sum(p[1] for p in points) / len(points)
            return {
                "position": (cx, cy),
                "confidence": len(good_matches) / len(template_features),
                "match_count": len(good_matches),
            }
        return None

    def _compute_distance(self, f1: Dict, f2: Dict) -> float:
        """Compute distance between two feature descriptors."""
        p1 = f1.get("point", (0, 0))
        p2 = f2.get("point", (0, 0))
        dx = p1[0] - p2[0]
        dy = p1[1] - p2[1]
        return math.sqrt(dx * dx + dy * dy)
