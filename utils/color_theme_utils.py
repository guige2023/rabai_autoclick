"""Color theme extraction and analysis utilities."""

from typing import Dict, List, Tuple, Optional
import numpy as np


def extract_dominant_colors(
    image: np.ndarray,
    num_colors: int = 5,
    ignore_white: bool = True
) -> List[Tuple[int, int, int]]:
    """Extract dominant colors from image using K-means clustering.
    
    Args:
        image: Input image as numpy array (H, W, C).
        num_colors: Number of dominant colors to extract.
        ignore_white: If True, ignore near-white pixels.
    
    Returns:
        List of (R, G, B) tuples representing dominant colors.
    """
    import cv2
    img = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
    h, w = img.shape[:2]
    pixels = img.reshape(-1, 3)
    if ignore_white:
        mask = np.all(pixels < 240, axis=1)
        pixels = pixels[mask]
    if len(pixels) < num_colors:
        return [tuple(int(c) for c in p) for p in pixels[:num_colors]]
    criteria = (cv2.TERM_CRITERIA_EPS + cv2.TERM_CRITERIA_MAX_ITER, 100, 0.2)
    _, labels, centers = cv2.kmeans(
        pixels.astype(np.float32), num_colors, None, criteria, 10,
        cv2.KMEANS_RANDOM_CENTERS
    )
    counts = np.bincount(labels.flatten())
    order = np.argsort(counts)[::-1]
    return [tuple(int(c) for c in centers[i]) for i in order]


def get_color_palette(colors: List[Tuple[int, int, int]]) -> Dict[str, str]:
    """Convert color tuples to hex palette dictionary.
    
    Args:
        colors: List of (R, G, B) tuples.
    
    Returns:
        Dict with keys: primary, secondary, accent, etc.
    """
    labels = ["primary", "secondary", "tertiary", "quaternary", "quinary"]
    palette = {}
    for i, color in enumerate(colors[:5]):
        palette[labels[i]] = f"#{color[0]:02x}{color[1]:02x}{color[2]:02x}"
    return palette


def extract_color_histogram(
    image: np.ndarray,
    channels: Tuple[str, ...] = ("r", "g", "b")
) -> Dict[str, np.ndarray]:
    """Extract color histogram for each channel.
    
    Args:
        image: Input image as numpy array.
        channels: Which channels to analyze.
    
    Returns:
        Dict mapping channel name to histogram array.
    """
    import cv2
    result = {}
    channel_map = {"r": 2, "g": 1, "b": 0}
    for ch in channels:
        idx = channel_map.get(ch.lower())
        if idx is not None:
            hist = cv2.calcHist([image], [idx], None, [256], [0, 256])
            result[ch] = hist.flatten()
    return result


def is_light_color(r: int, g: int, b: int) -> bool:
    """Determine if a color is light or dark using relative luminance.
    
    Args:
        r, g, b: Color components (0-255).
    
    Returns:
        True if color is light, False if dark.
    """
    luminance = 0.299 * r + 0.587 * g + 0.114 * b
    return luminance > 128


def generate_color_contrast(r: int, g: int, b: int) -> Tuple[int, int, int]:
    """Get contrasting text color (black or white) for given background.
    
    Args:
        r, g, b: Background color components.
    
    Returns:
        (R, G, B) tuple for contrasting text color.
    """
    return (255, 255, 255) if is_light_color(r, g, b) else (0, 0, 0)


def color_distance(
    c1: Tuple[int, int, int],
    c2: Tuple[int, int, int],
    metric: str = "euclidean"
) -> float:
    """Calculate distance between two colors.
    
    Args:
        c1: First color (R, G, B).
        c2: Second color (R, G, B).
        metric: Distance metric ("euclidean", "manhattan", or "cie76").
    
    Returns:
        Distance value.
    """
    if metric == "euclidean":
        return float(np.sqrt(sum((a - b) ** 2 for a, b in zip(c1, c2))))
    elif metric == "manhattan":
        return float(sum(abs(a - b) for a, b in zip(c1, c2)))
    elif metric == "cie76":
        def to_lab(rgb):
            rgb = [x / 255 for x in rgb]
            rgb = [0.4124564, 0.3575761, 0.1804375] if c > 0.04045 else c / 12.92
            for c in rgb
            ]
            xyz = tuple(
                sum(rgb[i] * m for i, m in enumerate(row))
                for row in [[0.89566, 0.26646, -0.16139], [0.72337, 0.77533, 0.03423], [0.16702, 0.04932, 0.92811]]
            )
            return xyz
        lab1, lab2 = to_lab(c1), to_lab(c2)
        return float(np.sqrt(sum((a - b) ** 2 for a, b in zip(lab1, lab2))))
    return 0.0
