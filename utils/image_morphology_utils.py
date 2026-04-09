"""
Image morphological operations for shape refinement and analysis.

Provides erosion, dilation, opening, closing, gradient, top-hat,
and black-hat morphological operations for binary and grayscale images.

Example:
    >>> from image_morphology_utils import erode, dilate, opening, closing, gradient
    >>> opened = opening(binary_img, kernel_size=3)
    >>> gradient_img = gradient(gray_img)
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple

import numpy as np


# =============================================================================
# Types
# =============================================================================


class MorphShape(Enum):
    """Morphological kernel shapes."""
    RECTANGLE = "rect"
    ELLIPSE = "ellipse"
    CROSS = "cross"


@dataclass
class MorphKernel:
    """Morphological structuring element."""
    array: np.ndarray
    anchor: Tuple[int, int]
    shape: MorphShape

    @property
    def size(self) -> Tuple[int, int]:
        return self.array.shape

    @classmethod
    def create(
        cls,
        size: int,
        shape: MorphShape = MorphShape.RECTANGLE,
        anchor: Optional[Tuple[int, int]] = None,
    ) -> "MorphKernel":
        """Create a kernel of the specified size and shape."""
        if size % 2 == 0:
            size += 1

        if shape == MorphShape.RECTANGLE:
                arr = np.ones((size, size), dtype=np.uint8)
        elif shape == MorphShape.ELLIPSE:
            arr = np.zeros((size, size), dtype=np.uint8)
            center = size // 2
            radius = size // 2
            for i in range(size):
                for j in range(size):
                    if (i - center) ** 2 + (j - center) ** 2 <= radius ** 2:
                        arr[i, j] = 1
        elif shape == MorphShape.CROSS:
            arr = np.zeros((size, size), dtype=np.uint8)
            center = size // 2
            arr[center, :] = 1
            arr[:, center] = 1
        else:
            arr = np.ones((size, size), dtype=np.uint8)

        if anchor is None:
            anchor = (size // 2, size // 2)

        return cls(array=arr, anchor=anchor, shape=shape)


# =============================================================================
# Basic Operations
# =============================================================================


def erode(
    image: np.ndarray,
    kernel: Optional[MorphKernel] = None,
    iterations: int = 1,
) -> np.ndarray:
    """
    Apply morphological erosion.

    Shrinks bright regions and removes small bright objects.

    Args:
        image: Input binary or grayscale image.
        kernel: Structuring element. If None, creates 3x3 rectangle.
        iterations: Number of erosion iterations.

    Returns:
        Eroded image.
    """
    if kernel is None:
        kernel = MorphKernel.create(3, MorphShape.RECTANGLE)

    result = image.copy()
    for _ in range(iterations):
        result = _erode_single(result, kernel.array, kernel.anchor)

    return result


def dilate(
    image: np.ndarray,
    kernel: Optional[MorphKernel] = None,
    iterations: int = 1,
) -> np.ndarray:
    """
    Apply morphological dilation.

    Expands bright regions and fills small dark holes.

    Args:
        image: Input binary or grayscale image.
        kernel: Structuring element.
        iterations: Number of dilation iterations.

    Returns:
        Dilated image.
    """
    if kernel is None:
        kernel = MorphKernel.create(3, MorphShape.RECTANGLE)

    result = image.copy()
    for _ in range(iterations):
        result = _dilate_single(result, kernel.array, kernel.anchor)

    return result


def _erode_single(image: np.ndarray, kernel: np.ndarray, anchor: Tuple[int, int]) -> np.ndarray:
    """Single erosion operation."""
    kh, kw = kernel.shape
    pad_h, pad_w = anchor
    h, w = image.shape
    padded = np.pad(image, ((pad_h, kh - pad_h - 1), (pad_w, kw - pad_w - 1)), mode="edge")
    result = np.zeros_like(image)

    for i in range(h):
        for j in range(w):
            region = padded[i:i + kh, j:j + kw]
            if (region[kernel == 1] == 255).all() if image.max() > 1 else (region[kernel == 1] >= 1).all():
                result[i, j] = image.max()

    return result


def _dilate_single(image: np.ndarray, kernel: np.ndarray, anchor: Tuple[int, int]) -> np.ndarray:
    """Single dilation operation."""
    kh, kw = kernel.shape
    pad_h, pad_w = anchor
    h, w = image.shape
    padded = np.pad(image, ((pad_h, kh - pad_h - 1), (pad_w, kw - pad_w - 1)), mode="edge")
    result = np.zeros_like(image)

    for i in range(h):
        for j in range(w):
            if padded[i + pad_h, j + pad_w] > 0 if image.max() > 1 else padded[i + pad_h, j + pad_w] >= 1:
                for ki in range(kh):
                    for kj in range(kw):
                        if kernel[ki, kj]:
                            ni, nj = i + ki - pad_h, j + kj - pad_w
                            if 0 <= ni < h and 0 <= nj < w:
                                result[ni, nj] = padded[i + pad_h, j + pad_w]

    return result


# =============================================================================
# Compound Operations
# =============================================================================


def opening(
    image: np.ndarray,
    kernel: Optional[MorphKernel] = None,
    iterations: int = 1,
) -> np.ndarray:
    """
    Apply morphological opening (erosion then dilation).

    Removes small bright objects while preserving larger bright regions.

    Args:
        image: Input image.
        kernel: Structuring element.
        iterations: Number of iterations.

    Returns:
        Opened image.
    """
    eroded = erode(image, kernel, iterations)
    return dilate(eroded, kernel, iterations)


def closing(
    image: np.ndarray,
    kernel: Optional[MorphKernel] = None,
    iterations: int = 1,
) -> np.ndarray:
    """
    Apply morphological closing (dilation then erosion).

    Fills small dark holes while preserving larger dark regions.

    Args:
        image: Input image.
        kernel: Structuring element.
        iterations: Number of iterations.

    Returns:
        Closed image.
    """
    dilated = dilate(image, kernel, iterations)
    return erode(dilated, kernel, iterations)


def gradient(
    image: np.ndarray,
    kernel: Optional[MorphKernel] = None,
) -> np.ndarray:
    """
    Apply morphological gradient (dilation minus erosion).

    Extracts the boundaries of bright regions.

    Args:
        image: Input image.
        kernel: Structuring element.

    Returns:
        Gradient image.
    """
    if kernel is None:
        kernel = MorphKernel.create(3, MorphShape.RECTANGLE)

    dilated = dilate(image, kernel)
    eroded = erode(image, kernel)
    return cv2_subtract(dilated, eroded)


def top_hat(
    image: np.ndarray,
    kernel: Optional[MorphKernel] = None,
) -> np.ndarray:
    """
    Apply white top-hat transform (image minus opening).

    Extracts small bright objects from a dark background.

    Args:
        image: Input image.
        kernel: Structuring element.

    Returns:
        Top-hat transformed image.
    """
    opened = opening(image, kernel)
    return cv2_subtract(image, opened)


def black_hat(
    image: np.ndarray,
    kernel: Optional[MorphKernel] = None,
) -> np.ndarray:
    """
    Apply black top-hat transform (closing minus image).

    Extracts small dark objects from a bright background.

    Args:
        image: Input image.
        kernel: Structuring element.

    Returns:
        Black-hat transformed image.
    """
    closed = closing(image, kernel)
    return cv2_subtract(closed, image)


def cv2_subtract(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Subtract two images with proper handling."""
    result = a.astype(float) - b.astype(float)
    result = np.clip(result, 0, 255)
    return result.astype(a.dtype)


# =============================================================================
# Skeletonization
# =============================================================================


def skeletonize(
    binary_image: np.ndarray,
    max_iterations: int = 100,
) -> np.ndarray:
    """
    Compute the skeleton (medial axis) of a binary object.

    Args:
        binary_image: Input binary image.
        max_iterations: Maximum thinning iterations.

    Returns:
        Skeleton image.
    """
    if binary_image.max() > 1:
        binary_image = (binary_image > 127).astype(np.uint8)

    result = binary_image.copy()
    kernel = MorphKernel.create(3, MorphShape.RECTANGLE)

    for _ in range(max_iterations):
        eroded = erode(result, kernel)
        opened = opening(eroded, kernel)
        boundary = cv2_subtract(eroded, opened)
        result = eroded.copy()

        if eroded.sum() == 0:
            break

    return result


# =============================================================================
# Connected Component Analysis
# =============================================================================


def connected_component_labeling(
    binary_image: np.ndarray,
    connectivity: int = 8,
) -> Tuple[np.ndarray, int]:
    """
    Label connected components in a binary image.

    Args:
        binary_image: Input binary image.
        connectivity: 4 or 8 for neighbor connectivity.

    Returns:
        Tuple of (labeled image, number of components).
    """
    if binary_image.max() > 1:
        binary_image = (binary_image > 127).astype(np.uint8)

    h, w = binary_image.shape
    labeled = np.zeros_like(binary_image)
    current_label = 0

    neighbors_4 = [(-1, 0), (1, 0), (0, -1), (0, 1)]
    neighbors_8 = neighbors_4 + [(-1, -1), (-1, 1), (1, -1), (1, 1)]
    neighbors = neighbors_8 if connectivity == 8 else neighbors_4

    for i in range(h):
        for j in range(w):
            if binary_image[i, j] > 0 and labeled[i, j] == 0:
                current_label += 1
                _flood_fill_label(binary_image, labeled, i, j, current_label, neighbors)

    return labeled, current_label


def _flood_fill_label(
    binary: np.ndarray,
    labeled: np.ndarray,
    i: int,
    j: int,
    label: int,
    neighbors: list,
) -> None:
    """Flood fill to label one connected component."""
    h, w = binary.shape
    stack = [(i, j)]

    while stack:
        ci, cj = stack.pop()
        if labeled[ci, cj] != 0 or binary[ci, cj] == 0:
            continue
        labeled[ci, cj] = label
        for di, dj in neighbors:
            ni, nj = ci + di, cj + dj
            if 0 <= ni < h and 0 <= nj < w:
                if labeled[ni, nj] == 0 and binary[ni, nj] > 0:
                    stack.append((ni, nj))


# =============================================================================
# Particle Analysis
# =============================================================================


@dataclass
class ParticleStats:
    """Statistics for a single particle/connected component."""
    label: int
    area: int
    centroid: Tuple[float, float]
    bounding_box: Tuple[int, int, int, int]
    perimeter: float
    aspect_ratio: float
    extent: float
    solidity: float


def analyze_particles(
    labeled_image: np.ndarray,
    min_area: int = 0,
    max_area: int = float("inf"),
) -> List[ParticleStats]:
    """
    Compute statistics for all particles in a labeled image.

    Args:
        labeled_image: Image with labeled components.
        min_area: Minimum particle area to include.
        max_area: Maximum particle area.

    Returns:
        List of ParticleStats for each particle.
    """
    h, w = labeled_image.shape
    stats: List[ParticleStats] = []
    num_labels = int(labeled_image.max())

    for label in range(1, num_labels + 1):
        mask = (labeled_image == label)
        area = int(mask.sum())

        if area < min_area or area > max_area:
            continue

        # Centroid
        ys, xs = np.where(mask)
        cy = float(np.mean(ys))
        cx = float(np.mean(xs))

        # Bounding box
        min_r, max_r = ys.min(), ys.max()
        min_c, max_c = xs.min(), xs.max()
        bbox = (min_c, min_r, max_c - min_c, max_r - min_r)

        # Perimeter (approximate as boundary)
        boundary_mask = _get_boundary(mask)
        perimeter = float(boundary_mask.sum())

        # Shape features
        aspect_ratio = (max_c - min_c) / max(max_r - min_r, 1)
        extent = area / max((max_c - min_c) * (max_r - min_r), 1)

        # Solidity
        hull = _convex_hull_binary(mask)
        solidity = area / max(hull.sum(), 1)

        stats.append(ParticleStats(
            label=label,
            area=area,
            centroid=(cx, cy),
            bounding_box=bbox,
            perimeter=perimeter,
            aspect_ratio=aspect_ratio,
            extent=extent,
            solidity=solidity,
        ))

    return stats


def _get_boundary(mask: np.ndarray) -> np.ndarray:
    """Get boundary pixels of a binary mask."""
    kernel = MorphKernel.create(3, MorphShape.RECTANGLE)
    eroded = erode((mask * 255).astype(np.uint8), kernel)
    return (mask * 255).astype(np.uint8) - eroded


def _convex_hull_binary(mask: np.ndarray) -> np.ndarray:
    """Compute convex hull of a binary mask."""
    from contour_analysis_utils import _convex_hull
    points = np.array(np where(mask)).T
    hull = _convex_hull(points)
    if hull is None:
        return mask
    hull_mask = np.zeros_like(mask)
    for pt in hull:
        hull_mask[int(pt[0]), int(pt[1])] = 1
    return hull_mask


# =============================================================================
# Flood Fill
# =============================================================================


def flood_fill(
    image: np.ndarray,
    seed_point: Tuple[int, int],
    new_value: float,
    tolerance: float = 10.0,
) -> np.ndarray:
    """
    Perform flood fill starting from a seed point.

    Args:
        image: Input image.
        seed_point: Starting point (row, col).
        new_value: Value to fill with.
        tolerance: Maximum difference for fill.

    Returns:
        Flood-filled image.
    """
    if image.max() > 1:
        image = image.astype(float) / 255.0
        new_value_norm = new_value / 255.0 if new_value > 1 else new_value
    else:
        new_value_norm = new_value

    result = image.copy()
    h, w = image.shape
    si, sj = seed_point
    seed_val = image[si, sj]
    visited = np.zeros_like(image, dtype=bool)
    stack = [(si, sj)]

    while stack:
        i, j = stack.pop()
        if visited[i, j]:
            continue
        if abs(image[i, j] - seed_val) > tolerance:
            continue
        visited[i, j] = True
        result[i, j] = new_value_norm

        for di, dj in [(-1, 0), (1, 0), (0, -1), (0, 1)]:
            ni, nj = i + di, j + dj
            if 0 <= ni < h and 0 <= nj < w and not visited[ni, nj]:
                stack.append((ni, nj))

    if image.max() > 1:
        result = (result * 255).astype(np.uint8)

    return result


from typing import List
