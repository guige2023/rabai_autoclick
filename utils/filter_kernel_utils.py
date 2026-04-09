"""Filter kernel utilities for RabAI AutoClick.

Provides:
- Common filter kernel generation
- Separable filter optimization
- Kernel normalization and operations
"""

from typing import List, Tuple
import math


def create_gaussian_kernel_1d(size: int, sigma: float) -> List[float]:
    """Create 1D Gaussian kernel.

    Args:
        size: Kernel size (odd).
        sigma: Standard deviation.

    Returns:
        1D Gaussian kernel values.
    """
    if size % 2 == 0:
        size += 1
    half = size // 2
    kernel: List[float] = []
    total = 0.0

    for i in range(size):
        x = i - half
        val = math.exp(-(x * x) / (2 * sigma * sigma))
        kernel.append(val)
        total += val

    return [v / total for v in kernel]


def create_gaussian_kernel_2d(size: int, sigma: float) -> List[List[float]]:
    """Create 2D Gaussian kernel.

    Args:
        size: Kernel size (odd).
        sigma: Standard deviation.

    Returns:
        2D Gaussian kernel.
    """
    if size % 2 == 0:
        size += 1
    half = size // 2
    kernel: List[List[float]] = []
    total = 0.0

    for i in range(size):
        row: List[float] = []
        for j in range(size):
            x = i - half
            y = j - half
            val = math.exp(-(x * x + y * y) / (2 * sigma * sigma))
            row.append(val)
            total += val
        kernel.append(row)

    return [[v / total for v in row] for row in kernel]


def create_gaussian_kernel_separable(
    size: int,
    sigma: float,
) -> Tuple[List[float], List[float]]:
    """Create separable Gaussian kernel.

    Args:
        size: Kernel size (odd).
        sigma: Standard deviation.

    Returns:
        (row_kernel, col_kernel) - same for symmetric Gaussian.
    """
    kernel_1d = create_gaussian_kernel_1d(size, sigma)
    return (kernel_1d, kernel_1d)


def create_box_kernel_2d(size: int) -> List[List[float]]:
    """Create 2D box (mean) filter kernel."""
    val = 1.0 / (size * size)
    return [[val] * size for _ in range(size)]


def create_sobel_x_kernel() -> List[List[float]]:
    """Create Sobel X (horizontal edge) kernel."""
    return [
        [-1, 0, 1],
        [-2, 0, 2],
        [-1, 0, 1],
    ]


def create_sobel_y_kernel() -> List[List[float]]:
    """Create Sobel Y (vertical edge) kernel."""
    return [
        [-1, -2, -1],
        [0, 0, 0],
        [1, 2, 1],
    ]


def create_prewitt_x_kernel() -> List[List[float]]:
    """Create Prewitt X kernel."""
    return [
        [-1, 0, 1],
        [-1, 0, 1],
        [-1, 0, 1],
    ]


def create_prewitt_y_kernel() -> List[List[float]]:
    """Create Prewitt Y kernel."""
    return [
        [-1, -1, -1],
        [0, 0, 0],
        [1, 1, 1],
    ]


def create_laplacian_kernel(variant: str = "4-connected") -> List[List[float]]:
    """Create Laplacian kernel for edge detection.

    Args:
        variant: '4-connected' or '8-connected'.

    Returns:
        Laplacian kernel.
    """
    if variant == "4-connected":
        return [
            [0, 1, 0],
            [1, -4, 1],
            [0, 1, 0],
        ]
    else:
        return [
            [1, 1, 1],
            [1, -8, 1],
            [1, 1, 1],
        ]


def create_sharpen_kernel() -> List[List[float]]:
    """Create unsharp masking / sharpen kernel."""
    return [
        [0, -1, 0],
        [-1, 5, -1],
        [0, -1, 0],
    ]


def create_emboss_kernel() -> List[List[float]]:
    """Create emboss kernel."""
    return [
        [-2, -1, 0],
        [-1, 1, 1],
        [0, 1, 2],
    ]


def create_outline_kernel() -> List[List[float]]:
    """Create outline (Kirsch) kernel."""
    return [
        [-3, -3, 5],
        [-3, 0, 5],
        [-3, -3, 5],
    ]


def create_motion_blur_kernel(
    size: int,
    angle: float = 0.0,
) -> List[List[float]]:
    """Create directional motion blur kernel.

    Args:
        size: Kernel size.
        angle: Blur angle in radians.

    Returns:
        Motion blur kernel.
    """
    if size % 2 == 0:
        size += 1
    half = size // 2
    kernel: List[List[float]] = [[0.0] * size for _ in range(size)]
    val = 1.0 / size

    cos_a = math.cos(angle)
    sin_a = math.sin(angle)

    for i in range(size):
        for j in range(size):
            x = i - half
            y = j - half
            proj = x * cos_a + y * sin_a
            if abs(proj) <= half:
                kernel[i][j] = val

    return kernel


def normalize_kernel(kernel: List[List[float]]) -> List[List[float]]:
    """Normalize kernel to sum to 1."""
    total = sum(sum(row) for row in kernel)
    if abs(total) < 1e-10:
        return kernel
    return [[v / total for v in row] for row in kernel]


def flip_kernel(kernel: List[List[float]]) -> List[List[float]]:
    """Flip kernel (180 degree rotation)."""
    size = len(kernel)
    return [[kernel[size - 1 - i][size - 1 - j] for j in range(size)] for i in range(size)]


def apply_kernel_2d(
    image: List[List[float]],
    kernel: List[List[float]],
    divisor: Optional[float] = None,
) -> List[List[float]]:
    """Apply 2D kernel to image.

    Args:
        image: 2D image.
        kernel: 2D kernel.
        divisor: Optional divisor (default: sum of kernel).

    Returns:
        Convolved image.
    """
    if not image or not kernel:
        return image[:]

    kh, kw = len(kernel), len(kernel[0])
    ih, iw = len(image), len(image[0])
    pad_h, pad_w = kh // 2, kw // 2

    if divisor is None:
        divisor = sum(sum(row) for row in kernel)

    result: List[List[float]] = [[0.0] * iw for _ in range(ih)]

    for y in range(ih):
        for x in range(iw):
            total = 0.0
            for ky in range(kh):
                for kx in range(kw):
                    iy = y + ky - pad_h
                    ix = x + kx - pad_w
                    if 0 <= iy < ih and 0 <= ix < iw:
                        total += image[iy][ix] * kernel[ky][kx]
            result[y][x] = total / divisor if divisor != 0 else total

    return result


def create_motion_blur_horizontal_kernel(size: int) -> List[List[float]]:
    """Create horizontal motion blur kernel."""
    val = 1.0 / size
    return [[0.0] * size for _ in range(size // 2)] + \
           [[val] * size] + \
           [[0.0] * size for _ in range(size // 2)]


def create_motion_blur_vertical_kernel(size: int) -> List[List[float]]:
    """Create vertical motion blur kernel."""
    val = 1.0 / size
    row = [0.0] * size
    result: List[List[float]] = []
    half = size // 2
    for i in range(size):
        if i == half:
            result.append([val] * size)
        else:
            result.append(row[:])
    return result


from typing import Optional
