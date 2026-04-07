"""
Image processing and computer vision utilities.

Provides image filtering, edge detection, histogram operations,
geometric transforms, and basic feature detection.
"""

from __future__ import annotations

import math


def convolve_2d(image: list[list[float]], kernel: list[list[float]]) -> list[list[float]]:
    """
    2D convolution with zero padding.

    Args:
        image: 2D grayscale image
        kernel: Convolution kernel

    Returns:
        Convolved image.
    """
    kh, kw = len(kernel), len(kernel[0])
    ih, iw = len(image), len(image[0])
    pad_h, pad_w = kh // 2, kw // 2
    result: list[list[float]] = [[0.0] * iw for _ in range(ih)]

    for i in range(ih):
        for j in range(iw):
            val = 0.0
            for ki in range(kh):
                for kj in range(kw):
                    ii = i - pad_h + ki
                    jj = j - pad_w + kj
                    if 0 <= ii < ih and 0 <= jj < iw:
                        val += image[ii][jj] * kernel[ki][kj]
            result[i][j] = val
    return result


def gaussian_blur_kernel(size: int, sigma: float) -> list[list[float]]:
    """
    Create Gaussian blur kernel.

    Args:
        size: Kernel size (odd)
        sigma: Standard deviation

    Returns:
        2D Gaussian kernel.
    """
    if size % 2 == 0:
        size += 1
    kernel: list[list[float]] = []
    half = size // 2
    for i in range(size):
        row: list[float] = []
        for j in range(size):
            x = i - half
            y = j - half
            val = math.exp(-(x * x + y * y) / (2 * sigma * sigma))
            row.append(val)
        kernel.append(row)
    # Normalize
    total = sum(sum(row) for row in kernel)
    return [[v / total for v in row] for row in kernel]


def sobel_edge_detection(image: list[list[float]]) -> tuple[list[list[float]], list[list[float]]]:
    """
    Sobel edge detection.

    Returns:
        Tuple of (gradient_x, gradient_y).
    """
    sobel_x = [
        [-1, 0, 1],
        [-2, 0, 2],
        [-1, 0, 1],
    ]
    sobel_y = [
        [-1, -2, -1],
        [0, 0, 0],
        [1, 2, 1],
    ]
    gx = convolve_2d(image, sobel_x)
    gy = convolve_2d(image, sobel_y)
    return gx, gy


def gradient_magnitude(gx: list[list[float]], gy: list[list[float]]) -> list[list[float]]:
    """Compute gradient magnitude from Sobel gradients."""
    h, w = len(gx), len(gx[0])
    mag: list[list[float]] = [[0.0] * w for _ in range(h)]
    for i in range(h):
        for j in range(w):
            mag[i][j] = math.sqrt(gx[i][j] ** 2 + gy[i][j] ** 2)
    return mag


def gradient_direction(gx: list[list[float]], gy: list[list[float]]) -> list[list[float]]:
    """Compute gradient direction (angle in radians)."""
    h, w = len(gx), len(gx[0])
    dir: list[list[float]] = [[0.0] * w for _ in range(h)]
    for i in range(h):
        for j in range(w):
            if abs(gy[i][j]) < 1e-12:
                dir[i][j] = math.pi / 2 if gx[i][j] > 0 else -math.pi / 2
            else:
                dir[i][j] = math.atan2(gx[i][j], gy[i][j])
    return dir


def laplacian_edge_detection(image: list[list[float]]) -> list[list[float]]:
    """
    Laplacian edge detection.

    Args:
        image: 2D grayscale image

    Returns:
        Laplacian response.
    """
    laplacian = [
        [0, 1, 0],
        [1, -4, 1],
        [0, 1, 0],
    ]
    return convolve_2d(image, laplacian)


def histogram(image: list[list[float]], bins: int = 256) -> list[int]:
    """
    Compute image histogram.

    Args:
        image: 2D grayscale image
        bins: Number of bins (default 256 for 8-bit)

    Returns:
        Histogram counts.
    """
    h = [0] * bins
    for row in image:
        for val in row:
            idx = int(min(val, bins - 1))
            idx = max(0, idx)
            h[idx] += 1
    return h


def histogram_equalization(image: list[list[float]], levels: int = 256) -> list[list[float]]:
    """
    Histogram equalization for contrast enhancement.

    Args:
        image: 2D grayscale image
        levels: Number of intensity levels

    Returns:
        Equalized image.
    """
    h, w = len(image), len(image[0])
    n = h * w
    hist = histogram(image, levels)

    # CDF
    cdf = [0] * levels
    cdf[0] = hist[0]
    for i in range(1, levels):
        cdf[i] = cdf[i - 1] + hist[i]

    # Normalize CDF
    cdf_min = next((c for c in cdf if c > 0), 0)
    lookup = [int((c - cdf_min) / (n - cdf_min) * (levels - 1)) for c in cdf]

    # Apply
    result: list[list[float]] = [[0.0] * w for _ in range(h)]
    for i in range(h):
        for j in range(w):
            val = int(image[i][j])
            val = max(0, min(val, levels - 1))
            result[i][j] = lookup[val]
    return result


def threshold_image(image: list[list[float]], threshold: float) -> list[list[int]]:
    """
    Simple binary thresholding.

    Args:
        image: 2D grayscale image
        threshold: Threshold value

    Returns:
        Binary image (0 or 255).
    """
    result: list[list[int]] = []
    for row in image:
        result.append([255 if val >= threshold else 0 for val in row])
    return result


def otsu_threshold(image: list[list[float]]) -> float:
    """
    Otsu's automatic threshold selection.

    Returns:
        Optimal threshold value.
    """
    hist = histogram(image, 256)
    n = sum(hist)
    if n == 0:
        return 128.0
    total = sum(i * hist[i] for i in range(256))
    sum_bg = 0
    weight_bg = 0
    max_variance = 0.0
    threshold = 0

    for i in range(256):
        weight_bg += hist[i]
        if weight_bg == 0:
            continue
        weight_fg = n - weight_bg
        if weight_fg == 0:
            break
        sum_bg += i * hist[i]
        mean_bg = sum_bg / weight_bg
        mean_fg = (total - sum_bg) / weight_fg
        variance = weight_bg * weight_fg * (mean_bg - mean_fg) ** 2
        if variance > max_variance:
            max_variance = variance
            threshold = i

    return float(threshold)


def resize_bilinear(
    image: list[list[float]],
    new_height: int,
    new_width: int,
) -> list[list[float]]:
    """
    Bilinear interpolation image resizing.

    Args:
        image: Original image
        new_height: Target height
        new_width: Target width

    Returns:
        Resized image.
    """
    h, w = len(image), len(image[0])
    result: list[list[float]] = [[0.0] * new_width for _ in range(new_height)]

    for i in range(new_height):
        for j in range(new_width):
            src_i = i * h / new_height
            src_j = j * w / new_width
            i0 = int(src_i)
            j0 = int(src_j)
            i1 = min(i0 + 1, h - 1)
            j1 = min(j0 + 1, w - 1)
            di = src_i - i0
            dj = src_j - j0
            v00 = image[i0][j0]
            v10 = image[i0][j1]
            v01 = image[i1][j0]
            v11 = image[i1][j1]
            top = v00 * (1 - dj) + v10 * dj
            bottom = v01 * (1 - dj) + v11 * dj
            result[i][j] = top * (1 - di) + bottom * di
    return result


def rotate_90(image: list[list[float]], clockwise: bool = True) -> list[list[float]]:
    """Rotate image by 90 degrees."""
    h, w = len(image), len(image[0])
    if clockwise:
        return [[image[h - 1 - i][j] for i in range(h)] for j in range(w)]
    return [[image[i][w - 1 - j] for i in range(h)] for j in range(w)]


def flip_horizontal(image: list[list[float]]) -> list[list[float]]:
    """Flip image horizontally."""
    h, w = len(image), len(image[0])
    return [[image[i][w - 1 - j] for j in range(w)] for i in range(h)]


def flip_vertical(image: list[list[float]]) -> list[list[float]]:
    """Flip image vertically."""
    h = len(image)
    return image[::-1]


def hough_line_transform(
    edge_image: list[list[float]],
    threshold: float = 50,
) -> list[tuple[float, float]]:
    """
    Hough transform for line detection.

    Args:
        edge_image: Edge-detected binary image
        threshold: Minimum vote threshold

    Returns:
        List of (rho, theta) lines.
    """
    h, w = len(edge_image), len(edge_image[0])
    diagonal = math.sqrt(h ** 2 + w ** 2)
    max_rho = int(diagonal)
    votes: dict[tuple[int, int], int] = {}

    # Accumulator
    for i in range(h):
        for j in range(w):
            if edge_image[i][j] > 0:
                for theta_idx in range(180):
                    theta = math.pi * theta_idx / 180
                    rho = int(j * math.cos(theta) + i * math.sin(theta))
                    key = (rho, theta_idx)
                    votes[key] = votes.get(key, 0) + 1

    # Extract lines above threshold
    lines: list[tuple[float, float]] = []
    for (rho, theta_idx), count in votes.items():
        if count >= threshold:
            theta = math.pi * theta_idx / 180
            lines.append((float(rho), theta))
    return lines


def non_maximum_suppression(
    magnitude: list[list[float]],
    direction: list[list[float]],
) -> list[list[float]]:
    """
    Non-maximum suppression for edge thinning.

    Args:
        magnitude: Gradient magnitude
        direction: Gradient direction

    Returns:
        Thinned edges.
    """
    h, w = len(magnitude), len(magnitude[0])
    result: list[list[float]] = [[0.0] * w for _ in range(h)]

    for i in range(1, h - 1):
        for j in range(1, w - 1):
            angle = direction[i][j]
            mag = magnitude[i][j]
            if mag < 1.0:
                continue
            q, r = 255.0, 255.0
            if -0.393 <= angle < 0.393:
                q = magnitude[i][j + 1]
                r = magnitude[i][j - 1]
            elif 0.393 <= angle < 1.178:
                q = magnitude[i - 1][j + 1]
                r = magnitude[i + 1][j - 1]
            elif 1.178 <= angle < -1.178 or angle >= 1.178 or angle < -1.178:
                q = magnitude[i + 1][j]
                r = magnitude[i - 1][j]
            else:
                q = magnitude[i + 1][j - 1]
                r = magnitude[i - 1][j + 1]
            if mag >= q and mag >= r:
                result[i][j] = mag
    return result


def connected_component_labeling(
    binary_image: list[list[int]],
) -> tuple[list[list[int]], int]:
    """
    Connected component labeling (4-connectivity).

    Args:
        binary_image: Binary image (0 = background, 255 = foreground)

    Returns:
        Tuple of (labeled_image, num_components).
    """
    h, w = len(binary_image), len(binary_image[0])
    labeled = [[0] * w for _ in range(h)]
    current_label = 0

    for i in range(h):
        for j in range(w):
            if binary_image[i][j] != 0 and labeled[i][j] == 0:
                # BFS
                current_label += 1
                queue = [(i, j)]
                labeled[i][j] = current_label
                while queue:
                    ci, cj = queue.pop(0)
                    for di, dj in [(-1, 0), (1, 0), (0, -1), (0, 1)]:
                        ni, nj = ci + di, cj + dj
                        if 0 <= ni < h and 0 <= nj < w:
                            if binary_image[ni][nj] != 0 and labeled[ni][nj] == 0:
                                labeled[ni][nj] = current_label
                                queue.append((ni, nj))

    return labeled, current_label
