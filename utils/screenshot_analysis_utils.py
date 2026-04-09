"""Screenshot analysis utilities for extracting UI information.

This module provides utilities for analyzing screenshots to extract
UI information including dominant colors, text regions, empty areas,
and visual composition, useful for adaptive automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, List, Tuple
import io


@dataclass
class RegionAnalysis:
    """Analysis result for a screen region."""
    x: int
    y: int
    width: int
    height: int
    mean_brightness: float
    std_brightness: float
    dominant_color: Tuple[int, int, int]
    entropy: float
    complexity_score: float
    is_empty: bool


@dataclass
class ScreenLayoutAnalysis:
    """Analysis of overall screen layout."""
    width: int
    height: int
    regions: List[RegionAnalysis]
    empty_regions: List[Tuple[int, int, int, int]]
    content_density: float
    layout_complexity: float


def analyze_region(
    image_data: bytes,
    x: int,
    y: int,
    width: int,
    height: int,
) -> RegionAnalysis:
    """Analyze a specific region of a screenshot.
    
    Args:
        image_data: Screenshot bytes.
        x: Region left edge.
        y: Region top edge.
        width: Region width.
        height: Region height.
    
    Returns:
        RegionAnalysis with region properties.
    """
    try:
        import numpy as np
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        
        x2 = min(x + width, img.width)
        y2 = min(y + height, img.height)
        
        region = img.crop((x, y, x2, y2))
        region_array = np.array(region)
        
        gray = np.mean(region_array, axis=2)
        mean_brightness = float(gray.mean() / 255.0)
        std_brightness = float(gray.std() / 255.0)
        
        r_mean = region_array[:, :, 0].mean()
        g_mean = region_array[:, :, 1].mean()
        b_mean = region_array[:, :, 2].mean()
        dominant_color = (int(r_mean), int(g_mean), int(b_mean))
        
        entropy = _calculate_entropy(gray)
        
        complexity = _calculate_complexity(region_array)
        
        is_empty = mean_brightness > 0.95 or mean_brightness < 0.05
        
        return RegionAnalysis(
            x=x,
            y=y,
            width=x2 - x,
            height=y2 - y,
            mean_brightness=mean_brightness,
            std_brightness=std_brightness,
            dominant_color=dominant_color,
            entropy=entropy,
            complexity_score=complexity,
            is_empty=is_empty,
        )
    except ImportError:
        raise ImportError("numpy and PIL are required for region analysis")


def _calculate_entropy(gray_array) -> float:
    """Calculate entropy of grayscale array."""
    import numpy as np
    
    hist, _ = np.histogram(gray_array, bins=256, range=(0, 256))
    hist = hist / hist.sum()
    
    entropy = -np.sum(hist * np.log2(hist + 1e-10))
    
    return float(entropy)


def _calculate_complexity(img_array) -> float:
    """Calculate visual complexity of image."""
    import numpy as np
    
    if len(img_array.shape) > 2:
        gray = np.mean(img_array, axis=2)
    else:
        gray = img_array
    
    dx = np.diff(gray, axis=1)
    dy = np.diff(gray, axis=0)
    
    edge_energy = np.sum(dx ** 2) + np.sum(dy ** 2)
    
    return float(edge_energy / (gray.shape[0] * gray.shape[1]))


def find_empty_regions(
    image_data: bytes,
    grid_x: int = 4,
    grid_y: int = 4,
    threshold: float = 0.05,
) -> List[Tuple[int, int, int, int]]:
    """Find empty regions in a grid pattern.
    
    Args:
        image_data: Screenshot bytes.
        grid_x: Number of grid columns.
        grid_y: Number of grid rows.
        threshold: Brightness threshold for empty detection.
    
    Returns:
        List of (x, y, width, height) tuples for empty regions.
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        
        cell_width = img.width // grid_x
        cell_height = img.height // grid_y
        
        empty_regions = []
        
        for gy in range(grid_y):
            for gx in range(grid_x):
                x = gx * cell_width
                y = gy * cell_height
                
                region = img.crop((
                    x, y,
                    min(x + cell_width, img.width),
                    min(y + cell_height, img.height),
                ))
                
                gray = region.convert("L")
                import numpy as np
                gray_array = np.array(gray)
                
                brightness = gray_array.mean() / 255.0
                variance = gray_array.std() / 255.0
                
                if brightness > (1 - threshold) or brightness < threshold:
                    if variance < 0.05:
                        empty_regions.append((
                            x, y,
                            min(cell_width, img.width - x),
                            min(cell_height, img.height - y),
                        ))
        
        return empty_regions
    except ImportError:
        raise ImportError("PIL is required for empty region detection")


def analyze_screen_layout(
    image_data: bytes,
    grid_x: int = 4,
    grid_y: int = 4,
) -> ScreenLayoutAnalysis:
    """Analyze overall screen layout.
    
    Args:
        image_data: Screenshot bytes.
        grid_x: Number of grid columns.
        grid_y: Number of grid rows.
    
    Returns:
        ScreenLayoutAnalysis with layout information.
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        
        cell_width = img.width // grid_x
        cell_height = img.height // grid_y
        
        regions = []
        empty_regions = []
        
        for gy in range(grid_y):
            for gx in range(grid_x):
                x = gx * cell_width
                y = gy * cell_height
                
                region_analysis = analyze_region(
                    image_data, x, y, cell_width, cell_height,
                )
                regions.append(region_analysis)
                
                if region_analysis.is_empty:
                    empty_regions.append((x, y, cell_width, cell_height))
        
        content_density = 1.0 - (len(empty_regions) / (grid_x * grid_y))
        
        total_complexity = sum(r.complexity_score for r in regions)
        layout_complexity = total_complexity / len(regions) if regions else 0
        
        return ScreenLayoutAnalysis(
            width=img.width,
            height=img.height,
            regions=regions,
            empty_regions=empty_regions,
            content_density=content_density,
            layout_complexity=layout_complexity,
        )
    except ImportError:
        raise ImportError("PIL is required for layout analysis")


def find_content_clusters(
    image_data: bytes,
    threshold: float = 0.3,
) -> List[Tuple[int, int, int, int]]:
    """Find clusters of content (non-empty areas).
    
    Args:
        image_data: Screenshot bytes.
        threshold: Content threshold.
    
    Returns:
        List of bounding boxes for content clusters.
    """
    try:
        import numpy as np
        from PIL import Image
        import cv2
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        img_array = np.array(img)
        
        gray = np.mean(img_array, axis=2)
        
        content_mask = (gray > threshold * 255) & (gray < (1 - threshold) * 255)
        
        content_mask_u8 = (content_mask * 255).astype(np.uint8)
        
        kernel = np.ones((10, 10), np.uint8)
        dilated = cv2.dilate(content_mask_u8, kernel, iterations=2)
        
        contours, _ = cv2.findContours(
            dilated,
            cv2.RETR_EXTERNAL,
            cv2.CHAIN_APPROX_SIMPLE,
        )
        
        clusters = []
        for contour in contours:
            x, y, w, h = cv2.boundingRect(contour)
            if w > 20 and h > 20:
                clusters.append((x, y, w, h))
        
        return clusters
    except ImportError:
        raise ImportError("OpenCV and numpy are required for content clustering")
