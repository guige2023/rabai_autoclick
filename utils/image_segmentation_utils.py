"""Image segmentation utilities for RabAI AutoClick.

Provides:
- Thresholding (otsu, adaptive, etc.)
- Region growing
- Watershed segmentation
- Connected component labeling
"""

from typing import List, Tuple, Set, Optional, Callable
import math


def threshold_otsu(image: List[List[float]]) -> float:
    """Compute Otsu's threshold for grayscale image.

    Args:
        image: 2D grayscale image.

    Returns:
        Optimal threshold value.
    """
    if not image or not image[0]:
        return 0.0

    # Flatten and compute histogram
    values = [pixel for row in image for pixel in row]
    min_val = min(values)
    max_val = max(values)
    if abs(max_val - min_val) < 1e-10:
        return min_val

    num_bins = 256
    bin_width = (max_val - min_val) / num_bins
    counts = [0] * num_bins
    for v in values:
        bin_idx = min(int((v - min_val) / bin_width), num_bins - 1)
        counts[bin_idx] += 1

    total = len(values)
    sum_total = sum(i * counts[i] for i in range(num_bins))

    sum_bg = 0.0
    w_bg = 0.0
    max_var = 0.0
    threshold = 0

    for i in range(num_bins):
        w_bg += counts[i]
        if w_bg == 0:
            continue
        w_fg = total - w_bg
        if w_fg == 0:
            break
        sum_bg += i * counts[i]
        m_bg = sum_bg / w_bg
        m_fg = (sum_total - sum_bg) / w_fg
        var = w_bg * w_fg * (m_bg - m_fg) ** 2
        if var > max_var:
            max_var = var
            threshold = i

    return min_val + threshold * bin_width


def apply_threshold(
    image: List[List[float]],
    threshold: float,
    high: float = 1.0,
    low: float = 0.0,
) -> List[List[float]]:
    """Apply binary threshold to image."""
    return [[high if v >= threshold else low for v in row] for row in image]


def adaptive_threshold(
    image: List[List[float]],
    block_size: int = 11,
    c: float = 2.0,
) -> List[List[float]]:
    """Apply adaptive threshold using local mean.

    Args:
        image: Grayscale image.
        block_size: Local window size (odd).
        c: Constant subtracted from mean.

    Returns:
        Binary image.
    """
    if not image:
        return []
    h, w = len(image), len(image[0])
    half = block_size // 2
    result: List[List[float]] = [[0.0] * w for _ in range(h)]

    # Compute integral image for fast mean
    integral = [[0.0] * (w + 1) for _ in range(h + 1)]
    for y in range(h):
        row_sum = 0.0
        for x in range(w):
            row_sum += image[y][x]
            integral[y + 1][x + 1] = integral[y][x + 1] + row_sum

    for y in range(h):
        for x in range(w):
            y1 = max(0, y - half)
            y2 = min(h, y + half + 1)
            x1 = max(0, x - half)
            x2 = min(w, x + half + 1)
            count = (y2 - y1) * (x2 - x1)
            local_sum = integral[y2][x2] - integral[y1][x2] - integral[y2][x1] + integral[y1][x1]
            mean = local_sum / count
            result[y][x] = 1.0 if image[y][x] > mean - c else 0.0

    return result


def flood_fill(
    image: List[List[float]],
    seed: Tuple[int, int],
    threshold: float,
    fill_value: float = 1.0,
) -> Set[Tuple[int, int]]:
    """Flood fill region growing from seed.

    Args:
        image: Input image.
        seed: (y, x) seed point.
        threshold: Value tolerance.
        fill_value: Value to fill with (not used, returns region).

    Returns:
        Set of (y, x) coordinates in region.
    """
    if not image:
        return set()
    h, w = len(image), len(image[0])
    seed_val = image[seed[0]][seed[1]]
    region: Set[Tuple[int, int]] = set()
    stack = [seed]

    while stack:
        y, x = stack.pop()
        if (y, x) in region:
            continue
        if not (0 <= y < h and 0 <= x < w):
            continue
        if abs(image[y][x] - seed_val) > threshold:
            continue
        region.add((y, x))
        stack.extend([(y + 1, x), (y - 1, x), (y, x + 1), (y, x - 1)])

    return region


def region_growing(
    image: List[List[float]],
    seeds: List[Tuple[int, int]],
    threshold: float,
) -> List[List[int]]:
    """Multi-seed region growing segmentation.

    Args:
        image: Grayscale image.
        seeds: List of seed points.
        threshold: Similarity threshold.

    Returns:
        Labeled image (same size, -1 = unlabeled).
    """
    if not image:
        return []
    h, w = len(image), len(image[0])
    labels = [[-1] * w for _ in range(h)]
    label = 0
    label_regions: List[Set[Tuple[int, int]]] = []

    for seed in seeds:
        if labels[seed[0]][seed[1]] != -1:
            continue
        region = flood_fill(image, seed, threshold)
        if region:
            label_regions.append(region)
            for y, x in region:
                labels[y][x] = label
            label += 1

    return labels


def connected_component_labeling(
    binary_image: List[List[int]],
    connectivity: int = 4,
) -> List[List[int]]:
    """Label connected components in binary image.

    Args:
        binary_image: 2D binary image (0/1 or True/False).
        connectivity: 4 or 8 connectivity.

    Returns:
        Labeled image (0 = background).
    """
    if not binary_image:
        return []
    h, w = len(binary_image), len(binary_image[0])
    labels = [[0] * w for _ in range(h)]
    current_label = 0

    def neighbors(y: int, x: int) -> List[Tuple[int, int]]:
        if connectivity == 4:
            return [(ny, nx) for ny, nx in [(y-1,x),(y,x+1),(y+1,x),(y,x-1)]
                    if 0 <= ny < h and 0 <= nx < w]
        else:
            return [(ny, nx) for ny, nx in [(y-1,x-1),(y-1,x),(y-1,x+1),(y,x+1),(y+1,x+1),(y+1,x),(y+1,x-1),(y,x-1)]
                    if 0 <= ny < h and 0 <= nx < w]

    for y in range(h):
        for x in range(w):
            if binary_image[y][x] and labels[y][x] == 0:
                current_label += 1
                stack = [(y, x)]
                while stack:
                    cy, cx = stack.pop()
                    if labels[cy][cx] != 0:
                        continue
                    labels[cy][cx] = current_label
                    for ny, nx in neighbors(cy, cx):
                        if binary_image[ny][nx] and labels[ny][nx] == 0:
                            stack.append((ny, nx))

    return labels


def kmeans_segmentation(
    image: List[List[float]],
    k: int = 3,
    max_iterations: int = 10,
) -> Tuple[List[List[int]], List[float]]:
    """Simple K-means clustering for image values.

    Args:
        image: 2D image.
        k: Number of clusters.
        max_iterations: Maximum iterations.

    Returns:
        (labels, centroids).
    """
    if not image:
        return [], []
    flat = [v for row in image for v in row]
    if not flat:
        return [], []

    # Initialize centroids randomly from data
    import random
    random.seed(42)
    centroids = random.sample(flat, min(k, len(flat)))
    if len(centroids) < k:
        centroids = list(flat[:k])

    labels = [0] * len(flat)

    for _ in range(max_iterations):
        # Assign points to nearest centroid
        changed = False
        for i, v in enumerate(flat):
            best = 0
            best_dist = abs(v - centroids[0])
            for j, c in enumerate(centroids):
                d = abs(v - c)
                if d < best_dist:
                    best_dist = d
                    best = j
            if labels[i] != best:
                labels[i] = best
                changed = True

        if not changed:
            break

        # Update centroids
        for j in range(len(centroids)):
            cluster_points = [flat[i] for i in range(len(flat)) if labels[i] == j]
            if cluster_points:
                centroids[j] = sum(cluster_points) / len(cluster_points)

    labeled = [[labels[i * len(image[0]) + j] for j in range(len(image[0]))] for i in range(len(image))]
    return labeled, centroids


def grabcut_iteration(
    image: List[List[Tuple[int, int, int]]],
    mask: List[List[int]],
    rect: Optional[Tuple[int, int, int, int]] = None,
) -> List[List[int]]:
    """One iteration of GrabCut foreground extraction.

    Args:
        image: RGB image.
        mask: Initial mask (0=definite BG, 1=definite FG, 2=probable BG, 3=probable FG).
        rect: Optional bounding rect of foreground.

    Returns:
        Updated mask.
    """
    h, w = len(image), len(image[0])
    result = [row[:] for row in mask]

    # Simplified: just expand FG/BG areas based on color similarity
    for y in range(h):
        for x in range(w):
            if mask[y][x] in (2, 3):
                # Assign based on proximity to known FG/BG
                result[y][x] = 1 if mask[y][x] == 3 else 0

    return result


def superpixel_slic(
    image: List[List[Tuple[int, int, int]]],
    k: int = 100,
    max_iterations: int = 10,
) -> List[List[int]]:
    """Simple SLIC-like superpixel segmentation.

    Args:
        image: RGB image.
        k: Number of superpixels.
        max_iterations: Iterations.

    Returns:
        Labeled image.
    """
    if not image:
        return []
    h, w = len(image), len(image[0])
    labels = [[-1] * w for _ in range(h)]
    step = int(math.sqrt(h * w / k))
    centers: List[Tuple[int, int, int, int, int]] = []

    # Initialize centers on grid
    for y in range(step // 2, h, step):
        for x in range(step // 2, w, step):
            r, g, b = image[y][x]
            centers.append((y, x, r, g, b))

    for _ in range(max_iterations):
        # Assign each pixel to nearest center
        for y in range(h):
            for x in range(w):
                r, g, b = image[y][x]
                best_idx = 0
                best_dist = float('inf')
                for i, (cy, cx, cr, cg, cb) in enumerate(centers):
                    d = math.sqrt((y - cy) ** 2 + (x - cx) ** 2) + \
                        math.sqrt((r - cr) ** 2 + (g - cg) ** 2 + (b - cb) ** 2)
                    if d < best_dist:
                        best_dist = d
                        best_idx = i
                labels[y][x] = best_idx

        # Update centers
        for i, (cy, cx, cr, cg, cb) in enumerate(centers):
            sum_y = sum_x = count = 0
            sum_r = sum_g = sum_b = 0
            for y in range(h):
                for x in range(w):
                    if labels[y][x] == i:
                        sum_y += y
                        sum_x += x
                        sum_r += image[y][x][0]
                        sum_g += image[y][x][1]
                        sum_b += image[y][x][2]
                        count += 1
            if count > 0:
                centers[i] = (sum_y / count, sum_x / count, sum_r / count, sum_g / count, sum_b / count)

    return labels
