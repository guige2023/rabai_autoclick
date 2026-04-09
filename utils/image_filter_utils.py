"""Image filtering utilities for RabAI AutoClick.

Provides:
- 2D convolution with custom kernels
- Image filters (blur, sharpen, edge detect, etc.)
- Morphological operations
- Color space conversions
"""

from typing import List, Tuple, Callable, Optional
import math


def convolve2d(
    image: List[List[float]],
    kernel: List[List[float]],
) -> List[List[float]]:
    """Apply 2D convolution to image.

    Args:
        image: 2D grayscale image (list of rows).
        kernel: 2D convolution kernel.

    Returns:
        Convolved image.
    """
    if not image or not image[0] or not kernel:
        return image[:]

    kh, kw = len(kernel), len(kernel[0])
    ih, iw = len(image), len(image[0])
    pad_h, pad_w = kh // 2, kw // 2

    result: List[List[float]] = [[0.0] * iw for _ in range(ih)]

    for y in range(ih):
        for x in range(iw):
            sum_val = 0.0
            for ky in range(kh):
                for kx in range(kw):
                    iy = y + ky - pad_h
                    ix = x + kx - pad_w
                    if 0 <= iy < ih and 0 <= ix < iw:
                        sum_val += image[iy][ix] * kernel[ky][kx]
            result[y][x] = sum_val

    return result


def create_gaussian_kernel(size: int, sigma: float) -> List[List[float]]:
    """Create Gaussian blur kernel.

    Args:
        size: Kernel size (odd).
        sigma: Standard deviation.

    Returns:
        2D Gaussian kernel.
    """
    k = size // 2
    kernel: List[List[float]] = []
    for y in range(size):
        row: List[float] = []
        for x in range(size):
            g = math.exp(-((x - k) ** 2 + (y - k) ** 2) / (2 * sigma * sigma))
            row.append(g)
        kernel.append(row)

    # Normalize
    total = sum(sum(row) for row in kernel)
    return [[v / total for v in row] for row in kernel]


def create_box_kernel(size: int) -> List[List[float]]:
    """Create box blur kernel."""
    val = 1.0 / (size * size)
    return [[val] * size for _ in range(size)]


def create_sharpen_kernel() -> List[List[float]]:
    """Create unsharp mask / sharpen kernel."""
    return [
        [0, -1, 0],
        [-1, 5, -1],
        [0, -1, 0],
    ]


def create_edge_kernel(type: str = "sobel") -> List[List[List[float]]]:
    """Create edge detection kernels.

    Args:
        type: 'sobel', 'prewitt', or 'laplacian'.

    Returns:
        List of [kx, ky] kernels.
    """
    if type == "sobel":
        return [
            [[-1, 0, 1], [-2, 0, 2], [-1, 0, 1]],  # x
            [[-1, -2, -1], [0, 0, 0], [1, 2, 1]],  # y
        ]
    elif type == "prewitt":
        return [
            [[-1, 0, 1], [-1, 0, 1], [-1, 0, 1]],
            [[-1, -1, -1], [0, 0, 0], [1, 1, 1]],
        ]
    else:  # laplacian
        return [
            [[0, 1, 0], [1, -4, 1], [0, 1, 0]],
            [[1, 1, 1], [1, -8, 1], [1, 1, 1]],
        ]


def apply_edge_detection(
    image: List[List[float]],
    kernel_type: str = "sobel",
) -> List[List[float]]:
    """Apply edge detection to grayscale image.

    Args:
        image: Grayscale image.
        kernel_type: 'sobel', 'prewitt', or 'laplacian'.

    Returns:
        Edge magnitude image.
    """
    kernels = create_edge_kernel(kernel_type)
    kx, ky = kernels[0], kernels[1]
    ix = convolve2d(image, kx)
    iy = convolve2d(image, ky)

    ih, iw = len(image), len(image[0])
    result: List[List[float]] = [[0.0] * iw for _ in range(ih)]
    for y in range(ih):
        for x in range(iw):
            result[y][x] = math.sqrt(ix[y][x] ** 2 + iy[y][x] ** 2)

    return result


def gaussian_blur(
    image: List[List[float]],
    kernel_size: int = 5,
    sigma: float = 1.4,
) -> List[List[float]]:
    """Apply Gaussian blur to image.

    Args:
        image: Grayscale image.
        kernel_size: Kernel size (odd).
        sigma: Gaussian sigma.

    Returns:
        Blurred image.
    """
    kernel = create_gaussian_kernel(kernel_size, sigma)
    return convolve2d(image, kernel)


def box_blur(image: List[List[float]], size: int = 3) -> List[List[float]]:
    """Apply box blur."""
    return convolve2d(image, create_box_kernel(size))


def median_filter(
    image: List[List[float]],
    size: int = 3,
) -> List[List[float]]:
    """Apply median filter.

    Args:
        image: Grayscale image.
        size: Filter size (odd).

    Returns:
        Filtered image.
    """
    ih, iw = len(image), len(image[0])
    pad = size // 2
    result: List[List[float]] = [[0.0] * iw for _ in range(ih)]

    for y in range(ih):
        for x in range(iw):
            values: List[float] = []
            for ky in range(-pad, pad + 1):
                for kx in range(-pad, pad + 1):
                    ny, nx = y + ky, x + kx
                    if 0 <= ny < ih and 0 <= nx < iw:
                        values.append(image[ny][nx])
            values.sort()
            result[y][x] = values[len(values) // 2]

    return result


def morphological_dilate(
    image: List[List[int]],
    kernel_size: int = 3,
) -> List[List[int]]:
    """Morphological dilation (binary)."""
    ih, iw = len(image), len(image[0])
    pad = kernel_size // 2
    result: List[List[int]] = [[0] * iw for _ in range(ih)]

    for y in range(ih):
        for x in range(iw):
            max_val = 0
            for ky in range(-pad, pad + 1):
                for kx in range(-pad, pad + 1):
                    ny, nx = y + ky, x + kx
                    if 0 <= ny < ih and 0 <= nx < iw:
                        max_val = max(max_val, image[ny][nx])
            result[y][x] = max_val

    return result


def morphological_erode(
    image: List[List[int]],
    kernel_size: int = 3,
) -> List[List[int]]:
    """Morphological erosion (binary)."""
    ih, iw = len(image), len(image[0])
    pad = kernel_size // 2
    result: List[List[int]] = [[0] * iw for _ in range(ih)]

    for y in range(ih):
        for x in range(iw):
            min_val = 255
            for ky in range(-pad, pad + 1):
                for kx in range(-pad, pad + 1):
                    ny, nx = y + ky, x + kx
                    if 0 <= ny < ih and 0 <= nx < iw:
                        min_val = min(min_val, image[ny][nx])
            result[y][x] = min_val

    return result


def adjust_brightness(
    image: List[List[float]],
    factor: float,
) -> List[List[float]]:
    """Adjust image brightness.

    Args:
        image: Grayscale image.
        factor: Brightness multiplier (>1 = brighter).

    Returns:
        Adjusted image.
    """
    return [[min(1.0, max(0.0, v * factor)) for v in row] for row in image]


def adjust_contrast(
    image: List[List[float]],
    factor: float,
) -> List[List[float]]:
    """Adjust image contrast.

    Args:
        image: Grayscale image.
        factor: Contrast multiplier.

    Returns:
        Adjusted image.
    """
    avg = sum(sum(row) for row in image) / (len(image) * len(image[0]))
    return [[min(1.0, max(0.0, avg + (v - avg) * factor)) for v in row] for row in image]


def apply_threshold(
    image: List[List[float]],
    threshold: float,
    high: float = 1.0,
    low: float = 0.0,
) -> List[List[float]]:
    """Apply hard threshold to image."""
    return [[high if v >= threshold else low for v in row] for row in image]


def image_resize_bilinear(
    image: List[List[float]],
    new_w: int,
    new_h: int,
) -> List[List[float]]:
    """Resize image using bilinear interpolation.

    Args:
        image: Input image.
        new_w, new_h: New dimensions.

    Returns:
        Resized image.
    """
    ih, iw = len(image), len(image[0])
    result: List[List[float]] = [[0.0] * new_w for _ in range(new_h)]

    scale_x = iw / new_w
    scale_y = ih / new_h

    for y in range(new_h):
        for x in range(new_w):
            src_x = x * scale_x
            src_y = y * scale_y
            x0, y0 = int(src_x), int(src_y)
            x1 = min(x0 + 1, iw - 1)
            y1 = min(y0 + 1, ih - 1)
            fx, fy = src_x - x0, src_y - y0

            v00 = image[y0][x0]
            v10 = image[y0][x1]
            v01 = image[y1][x0]
            v11 = image[y1][x1]

            v0 = v00 * (1 - fx) + v10 * fx
            v1 = v01 * (1 - fx) + v11 * fx
            result[y][x] = v0 * (1 - fy) + v1 * fy

    return result
