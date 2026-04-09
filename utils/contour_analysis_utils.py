"""
Contour analysis utilities for shape detection and element identification.

Provides contour detection, approximation, filtering, and feature
extraction for UI element shape analysis.

Example:
    >>> from contour_analysis_utils import find_contours, approximate_contour, filter_by_area
    >>> contours = find_contours(edge_image)
    >>> filtered = filter_by_area(contours, min_area=100)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, List, Optional, Tuple

import numpy as np


# =============================================================================
# Types and Enums
# =============================================================================


class ContourApproximationMethod(Enum):
    """Contour approximation methods."""
    NONE = "none"
    SIMPLE = "simple"
    DOWNSAMPLED = "downsampled"
    RamerDouglasPeucker = "rdp"


@dataclass
class Contour:
    """Represents a contour with extracted features."""
    points: np.ndarray
    area: float
    perimeter: float
    centroid: Tuple[float, float]
    bounding_box: Tuple[int, int, int, int]  # x, y, w, h
    approximation: Optional[np.ndarray] = None
    convex_hull: Optional[np.ndarray] = None
    moments: Optional[dict] = None
    aspect_ratio: float = 0.0
    extent: float = 0.0
    solidity: float = 0.0
    eccentricity: float = 0.0

    @property
    def center(self) -> Tuple[float, float]:
        """Alias for centroid."""
        return self.centroid


@dataclass
class ContourHierarchy:
    """Hierarchical relationship between contours."""
    contours: List[Contour]
    parent: List[Optional[int]]
    child: List[Optional[int]]
    next: List[Optional[int]]
    previous: List[Optional[int]]


# =============================================================================
# Contour Detection
# =============================================================================


def find_contours(
    binary_image: np.ndarray,
    mode: str = "external",
) -> ContourHierarchy:
    """
    Find contours in a binary image.

    Args:
        binary_image: Binary image (0 or 255 values).
        mode: 'external' (outer contours only) or 'all' (includes holes).

    Returns:
        ContourHierarchy with detected contours.
    """
    if binary_image.dtype != np.uint8:
        binary_image = (binary_image > 0).astype(np.uint8) * 255

    h, w = binary_image.shape
    visited = np.zeros_like(binary_image, dtype=bool)
    contour_points_list: List[List[Tuple[int, int]]] = []
    parent: List[Optional[int]] = []
    child: List[Optional[int]] = []
    next_contour: List[Optional[int]] = []
    prev_contour: List[Optional[int]] = []

    current_id = 0

    for i in range(h):
        for j in range(w):
            if binary_image[i, j] > 0 and not visited[i, j]:
                points = _trace_contour(binary_image, visited, i, j)
                if len(points) >= 3:
                    contour_id = current_id
                    current_id += 1
                    contour_points_list.append(points)
                    parent.append(None)
                    child.append(None)
                    next_contour.append(None)
                    prev_contour.append(None)

    contours = []
    for pts in contour_points_list:
        pts_array = np.array(pts)
        area = _compute_polygon_area(pts_array)
        perimeter = _compute_perimeter(pts_array)
        cx = pts_array[:, 1].mean()
        cy = pts_array[:, 0].mean()
        bbox = _compute_bounding_box(pts_array)

        # Compute features
        hull = _convex_hull(pts_array)
        hull_area = _compute_polygon_area(hull) if hull is not None else area

        aspect_ratio = bbox[2] / max(bbox[3], 1)
        extent = area / max(bbox[2] * bbox[3], 1)
        solidity = area / max(hull_area, 1)

        contour = Contour(
            points=pts_array,
            area=abs(area),
            perimeter=perimeter,
            centroid=(cx, cy),
            bounding_box=bbox,
            convex_hull=hull,
            aspect_ratio=aspect_ratio,
            extent=extent,
            solidity=solidity,
            eccentricity=_compute_eccentricity(pts_array),
        )
        contours.append(contour)

    return ContourHierarchy(
        contours=contours,
        parent=parent,
        child=child,
        next=next_contour,
        previous=prev_contour,
    )


def _trace_contour(
    image: np.ndarray,
    visited: np.ndarray,
    start_i: int,
    start_j: int,
) -> List[Tuple[int, int]]:
    """
    Trace a contour using Moore-Neighbor following.

    Args:
        image: Binary image.
        visited: Visited pixel map.
        start_i: Starting row.
        start_j: Starting column.

    Returns:
        List of (row, col) points forming the contour.
    """
    h, w = image.shape
    contour = []
    stack = [(start_i, start_j)]
    boundary_pixels = []

    while stack:
        i, j = stack.pop()
        if visited[i, j]:
            continue

        found_boundary = False
        for di in range(-1, 2):
            for dj in range(-1, 2):
                if di == 0 and dj == 0:
                    continue
                ni, nj = i + di, j + dj
                if 0 <= ni < h and 0 <= nj < w:
                    if image[ni, nj] > 0:
                        found_boundary = True
                        if not visited[ni, nj]:
                            visited[ni, nj] = True
                            boundary_pixels.append((ni, nj))

        if found_boundary:
            visited[i, j] = True
            contour.append((i, j))

    return contour


# =============================================================================
# Contour Approximation
# =============================================================================


def approximate_contour(
    contour: Contour,
    method: ContourApproximationMethod = ContourApproximationMethod.RamerDouglasPeucker,
    epsilon_factor: float = 0.02,
) -> np.ndarray:
    """
    Approximate a contour to reduce the number of points.

    Args:
        contour: Input contour.
        method: Approximation method.
        epsilon_factor: Epsilon as fraction of perimeter.

    Returns:
        Approximated contour points.
    """
    points = contour.points
    epsilon = epsilon_factor * contour.perimeter

    if method == ContourApproximationMethod.NONE:
        return points
    elif method == ContourApproximationMethod.SIMPLE:
        return _simple_approximate(points)
    elif method == ContourApproximationMethod.DOWNSAMPLED:
        return _downsample_contour(points, factor=5)
    elif method == ContourApproximationMethod.RamerDouglasPeucker:
        return _rdp_approximate(points, epsilon)
    else:
        return points


def _simple_approximate(points: np.ndarray) -> np.ndarray:
    """Simple angle-based approximation."""
    if len(points) < 4:
        return points

    result = [points[0]]
    prev_angle = None

    for i in range(1, len(points)):
        p1 = points[i - 1]
        p2 = points[i]
        angle = np.arctan2(p2[0] - p1[0], p2[1] - p1[1])

        if prev_angle is None or abs(angle - prev_angle) > 0.1:
            result.append(points[i])
            prev_angle = angle

    return np.array(result)


def _downsample_contour(points: np.ndarray, factor: int = 3) -> np.ndarray:
    """Downsample contour by keeping every nth point."""
    if len(points) <= factor * 2:
        return points
    return points[::factor]


def _rdp_approximate(points: np.ndarray, epsilon: float) -> np.ndarray:
    """
    Ramer-Douglas-Peucker algorithm for contour simplification.

    Args:
        points: Input contour points (N x 2).
        epsilon: Maximum distance threshold.

    Returns:
        Simplified contour.
    """
    if len(points) < 3:
        return points

    # Find point with maximum distance from line
    start, end = points[0], points[-1]
    line_vec = end - start
    line_len = np.linalg.norm(line_vec)

    if line_len < epsilon:
        return np.array([start, end])

    line_unit = line_vec / line_len
    max_dist = 0.0
    max_idx = 0

    for i in range(1, len(points) - 1):
        point_vec = points[i] - start
        proj_len = np.dot(point_vec, line_unit)
        proj_vec = proj_len * line_unit
        perp_vec = point_vec - proj_vec
        dist = np.linalg.norm(perp_vec)

        if dist > max_dist:
            max_dist = dist
            max_idx = i

    if max_dist > epsilon:
        left = _rdp_approximate(points[:max_idx + 1], epsilon)
        right = _rdp_approximate(points[max_idx:], epsilon)
        return np.vstack([left[:-1], right])
    else:
        return np.array([start, end])


# =============================================================================
# Filtering
# =============================================================================


def filter_by_area(
    contours: List[Contour],
    min_area: float = 0.0,
    max_area: float = float("inf"),
) -> List[Contour]:
    """
    Filter contours by area.

    Args:
        contours: Input contours.
        min_area: Minimum area threshold.
        max_area: Maximum area threshold.

    Returns:
        Filtered contours.
    """
    return [
        c for c in contours
        if min_area <= c.area <= max_area
    ]


def filter_by_perimeter(
    contours: List[Contour],
    min_perimeter: float = 0.0,
    max_perimeter: float = float("inf"),
) -> List[Contour]:
    """
    Filter contours by perimeter.

    Args:
        contours: Input contours.
        min_perimeter: Minimum perimeter.
        max_perimeter: Maximum perimeter.

    Returns:
        Filtered contours.
    """
    return [
        c for c in contours
        if min_perimeter <= c.perimeter <= max_perimeter
    ]


def filter_by_aspect_ratio(
    contours: List[Contour],
    min_ratio: float = 0.0,
    max_ratio: float = float("inf"),
) -> List[Contour]:
    """
    Filter contours by aspect ratio (width/height of bounding box).

    Args:
        contours: Input contours.
        min_ratio: Minimum aspect ratio.
        max_ratio: Maximum aspect ratio.

    Returns:
        Filtered contours.
    """
    return [
        c for c in contours
        if min_ratio <= c.aspect_ratio <= max_ratio
    ]


def filter_by_solidity(
    contours: List[Contour],
    min_solidity: float = 0.0,
    max_solidity: float = 1.0,
) -> List[Contour]:
    """
    Filter contours by solidity (area / convex hull area).

    Args:
        contours: Input contours.
        min_solidity: Minimum solidity.
        max_solidity: Maximum solidity.

    Returns:
        Filtered contours.
    """
    return [
        c for c in contours
        if min_solidity <= c.solidity <= max_solidity
    ]


def filter_by_extent(
    contours: List[Contour],
    min_extent: float = 0.0,
    max_extent: float = 1.0,
) -> List[Contour]:
    """
    Filter contours by extent (area / bounding box area).

    Args:
        contours: Input contours.
        min_extent: Minimum extent.
        max_extent: Maximum extent.

    Returns:
        Filtered contours.
    """
    return [
        c for c in contours
        if min_extent <= c.extent <= max_extent
    ]


def filter_by_condition(
    contours: List[Contour],
    predicate: Callable[[Contour], bool],
) -> List[Contour]:
    """
    Filter contours by a custom predicate.

    Args:
        contours: Input contours.
        predicate: Function that returns True for contours to keep.

    Returns:
        Filtered contours.
    """
    return [c for c in contours if predicate(c)]


# =============================================================================
# Shape Classification
# =============================================================================


def classify_shape(contour: Contour) -> str:
    """
    Classify the shape of a contour.

    Args:
        contour: Input contour.

    Returns:
        Shape name: 'circle', 'ellipse', 'rectangle', 'triangle', 'line', 'polygon', 'unknown'.
    """
    area = contour.area
    perimeter = contour.perimeter

    if area < 10:
        return "unknown"

    # Circularity: 4 * pi * area / perimeter^2
    circularity = 4 * np.pi * area / (perimeter ** 2)

    if circularity > 0.85:
        return "circle"

    # Approximate with polygon
    approx = approximate_contour(contour, epsilon_factor=0.03)
    num_vertices = len(approx)

    if num_vertices <= 5:
        return "triangle" if num_vertices == 3 else "polygon"

    if contour.solidity > 0.9 and 0.8 <= contour.aspect_ratio <= 1.2:
        return "ellipse"

    if contour.extent > 0.8:
        return "rectangle"

    return "polygon"


def detect_circles(
    contours: List[Contour],
    circularity_threshold: float = 0.85,
    area_tolerance: float = 0.2,
) -> List[Contour]:
    """
    Filter contours that are likely circles.

    Args:
        contours: Input contours.
        circularity_threshold: Minimum circularity (4*pi*area/perimeter^2).
        area_tolerance: Tolerance for radius consistency.

    Returns:
        Contours classified as circles.
    """
    circles = []
    for c in contours:
        circularity = 4 * np.pi * c.area / (c.perimeter ** 2)
        if circularity >= circularity_threshold:
            circles.append(c)
    return circles


def detect_rectangles(
    contours: List[Contour],
    min_extent: float = 0.7,
    min_solidity: float = 0.8,
) -> List[Contour]:
    """
    Filter contours that are likely rectangles.

    Args:
        contours: Input contours.
        min_extent: Minimum extent.
        min_solidity: Minimum solidity.

    Returns:
        Contours classified as rectangles.
    """
    return [
        c for c in contours
        if c.extent >= min_extent and c.solidity >= min_solidity
    ]


# =============================================================================
# Geometric Computations
# =============================================================================


def _compute_polygon_area(points: np.ndarray) -> float:
    """Compute polygon area using the Shoelace formula."""
    if len(points) < 3:
        return 0.0
    x = points[:, 1]
    y = points[:, 0]
    return 0.5 * abs(sum(x[i] * y[(i + 1) % len(points)] - x[(i + 1) % len(points)] * y[i] for i in range(len(points))))


def _compute_perimeter(points: np.ndarray) -> float:
    """Compute perimeter as sum of edge lengths."""
    if len(points) < 2:
        return 0.0
    total = 0.0
    for i in range(len(points)):
        p1 = points[i]
        p2 = points[(i + 1) % len(points)]
        total += np.sqrt((p1[0] - p2[0]) ** 2 + (p1[1] - p2[1]) ** 2)
    return total


def _compute_bounding_box(points: np.ndarray) -> Tuple[int, int, int, int]:
    """Compute bounding box as (x, y, width, height)."""
    if len(points) == 0:
        return (0, 0, 0, 0)
    min_r, max_r = int(points[:, 0].min()), int(points[:, 0].max())
    min_c, max_c = int(points[:, 1].min()), int(points[:, 1].max())
    return (min_c, min_r, max_c - min_c, max_r - min_r)


def _convex_hull(points: np.ndarray) -> Optional[np.ndarray]:
    """Compute convex hull using Graham scan."""
    if len(points) < 3:
        return points if len(points) > 0 else None

    def cross(o, a, b):
        return (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])

    pts = sorted(points, key=lambda p: (p[1], p[0]))
    lower = []
    for p in pts:
        while len(lower) >= 2 and cross(lower[-2], lower[-1], p) <= 0:
            lower.pop()
        lower.append(p)

    upper = []
    for p in reversed(pts):
        while len(upper) >= 2 and cross(upper[-2], upper[-1], p) <= 0:
            upper.pop()
        upper.append(p)

    return np.array(lower[:-1] + upper[:-1])


def _compute_eccentricity(points: np.ndarray) -> float:
    """Compute eccentricity (ratio of semi-minor to semi-major axis)."""
    if len(points) < 3:
        return 0.0

    # Compute covariance matrix
    mean = points.mean(axis=0)
    centered = points - mean
    cov = np.cov(centered[:, 0], centered[:, 1])

    # Eigenvalues
    trace = cov[0, 0] + cov[1, 1]
    det = cov[0, 0] * cov[1, 1] - cov[0, 1] * cov[1, 0]
    discriminant = trace ** 2 - 4 * det

    if discriminant < 0:
        return 0.0

    sqrt_disc = np.sqrt(discriminant)
    lambda1 = (trace + sqrt_disc) / 2
    lambda2 = (trace - sqrt_disc) / 2

    if lambda1 == 0:
        return 0.0

    return np.sqrt(lambda2 / lambda1)


# =============================================================================
# Contour Matching
# =============================================================================


def match_contour_shape(
    contour: Contour,
    reference_points: np.ndarray,
) -> float:
    """
    Compute shape match score between contour and reference.

    Args:
        contour: Input contour.
        reference_points: Reference contour points.

    Returns:
        Match score (0-1, higher is better).
    """
    # Normalize both contours
    c_points = resample_contour(contour.points, len(reference_points))

    # Compute Hu moments
    hu_contour = _compute_hu_moments(c_points)
    hu_ref = _compute_hu_moments(reference_points)

    if hu_ref[0] == 0:
        return 0.0

    # Match using Hu moment invariant
    score = sum(abs(hu_contour[i] - hu_ref[i]) / abs(hu_ref[i]) for i in range(7))
    return max(0.0, 1.0 - score)


def resample_contour(points: np.ndarray, num_points: int) -> np.ndarray:
    """
    Resample contour to a fixed number of points.

    Args:
        points: Input contour points.
        num_points: Target number of points.

    Returns:
        Resampled contour.
    """
    if len(points) == num_points:
        return points

    perimeter = _compute_perimeter(points)
    if perimeter == 0:
        return points

    step = perimeter / num_points
    result = [points[0]]
    accumulated = 0.0
    i = 1

    while len(result) < num_points and i < len(points):
        dist = np.linalg.norm(points[i] - points[i - 1])
        accumulated += dist

        if accumulated >= step:
            result.append(points[i])
            accumulated = 0.0
        i += 1

    while len(result) < num_points:
        result.append(points[-1])

    return np.array(result[:num_points])


def _compute_hu_moments(points: np.ndarray) -> np.ndarray:
    """Compute Hu's 7 moment invariants."""
    m = _compute_raw_moments(points, p=2, q=2)
    hu = np.zeros(7)

    hu[0] = m["m20"] + m["02"]
    hu[1] = (m["m20"] - m["02"]) ** 2 + 4 * m["m11"] ** 2
    hu[2] = (m["m30"] - 3 * m["m12"]) ** 2 + (3 * m["m21"] - m["03"]) ** 2
    hu[3] = (m["m30"] + m["m12"]) ** 2 + (m["m21"] + m["m03"]) ** 2
    hu[4] = (m["m30"] - 3 * m["m12"]) * (m["m30"] + m["m12"]) * (
        (m["m30"] + m["m12"]) ** 2 - 3 * (m["m21"] + m["m03"]) ** 2
    ) + (3 * m["m21"] - m["m03"]) * (m["m21"] + m["m03"]) * (
        3 * (m["m30"] + m["m12"]) ** 2 - (m["m21"] + m["m03"]) ** 2
    )

    return hu


def _compute_raw_moments(points: np.ndarray, p: int = 2, q: int = 2) -> dict:
    """Compute raw moments of contour points."""
    m = {}
    for i in range(p + 1):
        for j in range(q + 1):
            if i + j <= max(p, q):
                key = f"m{i}{j}"
                m[key] = float(np.sum(points[:, 0] ** i * points[:, 1] ** j))
    return m
