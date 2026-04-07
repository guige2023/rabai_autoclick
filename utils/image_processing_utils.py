"""
Image processing utilities for computer vision tasks.

Provides common image transformations, filters, and operations.
"""
from __future__ import annotations

from typing import List, Tuple, Union

import numpy as np


def rgb_to_grayscale(image: np.ndarray) -> np.ndarray:
    """
    Convert RGB image to grayscale.

    Args:
        image: RGB image array of shape (H, W, 3) with values 0-255 or 0-1

    Returns:
        Grayscale image array of shape (H, W)

    Example:
        >>> rgb_to_grayscale(np.array([[[255, 0, 0], [0, 255, 0]]]))
        array([[76., 149.]])
    """
    if image.max() > 1:
        image = image / 255.0
    if len(image.shape) == 3 and image.shape[2] == 3:
        return 0.299 * image[:, :, 0] + 0.587 * image[:, :, 1] + 0.114 * image[:, :, 2]
    return image


def rgb_to_hsv(image: np.ndarray) -> np.ndarray:
    """
    Convert RGB image to HSV color space.

    Args:
        image: RGB image array of shape (H, W, 3)

    Returns:
        HSV image array of shape (H, W, 3)

    Example:
        >>> rgb_to_hsv(np.array([[[255, 0, 0]]]))[0, 0]
        array([  0., 255., 255.])
    """
    if image.max() > 1:
        image = image / 255.0
    r, g, b = image[:, :, 0], image[:, :, 1], image[:, :, 2]
    maxc = np.maximum(np.maximum(r, g), b)
    minc = np.minimum(np.minimum(r, g), b)
    v = maxc
    s = (maxc - minc) / (maxc + 1e-10)
    s[maxc == 0] = 0
    delta = maxc - minc
    delta[maxc == 0] = 1
    h = np.zeros_like(v)
    mask = (maxc == r) & (delta != 0)
    h[mask] = ((g[mask] - b[mask]) / delta[mask]) % 6
    mask = (maxc == g) & (delta != 0)
    h[mask] = ((b[mask] - r[mask]) / delta[mask]) + 2
    mask = (maxc == b) & (delta != 0)
    h[mask] = ((r[mask] - g[mask]) / delta[mask]) + 4
    h = h / 6.0
    h[h < 0] += 1
    return np.stack([h, s, v], axis=-1)


def adjust_brightness(image: np.ndarray, factor: float) -> np.ndarray:
    """
    Adjust image brightness.

    Args:
        image: Input image (0-255 or 0-1 range)
        factor: Brightness factor (1.0 = original, >1 = brighter, <1 = darker)

    Returns:
        Brightness-adjusted image

    Example:
        >>> adjust_brightness(np.array([128]), 1.5).astype(int)
        array([192])
    """
    if image.max() > 1:
        image = image / 255.0
    result = np.clip(image * factor, 0, 1)
    if image.max() > 1:
        result = (result * 255).astype(np.uint8)
    return result


def adjust_contrast(image: np.ndarray, factor: float) -> np.ndarray:
    """
    Adjust image contrast.

    Args:
        image: Input image
        factor: Contrast factor (1.0 = original, >1 = higher contrast)

    Returns:
        Contrast-adjusted image

    Example:
        >>> adjust_contrast(np.array([100, 150, 200]), 1.5).astype(int)
        array([ 79, 142, 221])
    """
    if image.max() > 1:
        image = image / 255.0
    mean = np.mean(image)
    result = np.clip((image - mean) * factor + mean, 0, 1)
    if image.max() > 1:
        result = (result * 255).astype(np.uint8)
    return result


def adjust_saturation(image: np.ndarray, factor: float) -> np.ndarray:
    """
    Adjust color saturation.

    Args:
        image: Input RGB image
        factor: Saturation factor (1.0 = original, 0 = grayscale, >1 = more saturated)

    Returns:
        Saturation-adjusted RGB image

    Example:
        >>> adjust_saturation(np.array([[[255, 0, 0]]]), 0.0).shape
        (1, 1, 3)
    """
    if image.max() > 1:
        image = image / 255.0
    gray = rgb_to_grayscale(image)
    result = np.clip((image - gray[..., np.newaxis]) * factor + gray[..., np.newaxis], 0, 1)
    if image.max() > 1:
        result = (result * 255).astype(np.uint8)
    return result


def gaussian_blur(image: np.ndarray, kernel_size: int = 5, sigma: float = 1.0) -> np.ndarray:
    """
    Apply Gaussian blur to image.

    Args:
        image: Input image (2D or 3D)
        kernel_size: Size of Gaussian kernel (odd number)
        sigma: Standard deviation of Gaussian

    Returns:
        Blurred image

    Example:
        >>> gaussian_blur(np.array([[0, 0, 0], [0, 255, 0], [0, 0, 0]]), 3, 1.0).astype(int)[1, 1]
        array([170])
    """
    if kernel_size % 2 == 0:
        kernel_size += 1
    x = np.arange(kernel_size) - kernel_size // 2
    gauss_1d = np.exp(-x ** 2 / (2 * sigma ** 2))
    gauss_1d /= gauss_1d.sum()
    kernel = np.outer(gauss_1d, gauss_1d)
    kernel /= kernel.sum()
    if len(image.shape) == 3:
        result = np.zeros_like(image)
        for c in range(image.shape[2]):
            from scipy.ndimage import convolve
            result[:, :, c] = convolve(image[:, :, c], kernel, mode='reflect')
        return result
    from scipy.ndimage import convolve
    return convolve(image, kernel, mode='reflect')


def sobel_edge_detection(image: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """
    Apply Sobel edge detection.

    Args:
        image: Grayscale image

    Returns:
        Tuple of (sobel_x, sobel_y) gradient images

    Example:
        >>> img = np.array([[0, 0, 0], [0, 1, 0], [0, 0, 0]])
        >>> sx, sy = sobel_edge_detection(img)
    """
    sobel_x = np.array([[-1, 0, 1], [-2, 0, 2], [-1, 0, 1]])
    sobel_y = np.array([[-1, -2, -1], [0, 0, 0], [1, 2, 1]])
    from scipy.ndimage import convolve
    gx = convolve(image.astype(float), sobel_x, mode='reflect')
    gy = convolve(image.astype(float), sobel_y, mode='reflect')
    return gx, gy


def canny_edge_detection(image: np.ndarray, low_threshold: float = 50, high_threshold: float = 150) -> np.ndarray:
    """
    Canny edge detection (simplified version).

    Args:
        image: Grayscale image (0-255 range)
        low_threshold: Low threshold for edge linking
        high_threshold: High threshold for edge detection

    Returns:
        Binary edge map

    Example:
        >>> canny_edge_detection(np.random.rand(100, 100) * 255).shape
        (100, 100)
    """
    from scipy.ndimage import convolve, maximum_filter
    gx, gy = sobel_edge_detection(image)
    magnitude = np.sqrt(gx ** 2 + gy ** 2)
    magnitude = (magnitude / magnitude.max() * 255).astype(np.uint8)
    edges = np.zeros_like(magnitude)
    edges[magnitude >= high_threshold] = 255
    edges[(magnitude >= low_threshold) & (magnitude < high_threshold)] = 128
    return edges


def resize_image(
    image: np.ndarray, size: Tuple[int, int], method: str = "bilinear"
) -> np.ndarray:
    """
    Resize image to target size.

    Args:
        image: Input image (H, W, C) or (H, W)
        size: Target size (height, width)
        method: Interpolation method (bilinear, nearest, bicubic)

    Returns:
        Resized image

    Example:
        >>> resize_image(np.ones((10, 10, 3)), (5, 5)).shape
        (5, 5, 3)
    """
    from scipy.ndimage import zoom
    h, w = image.shape[:2]
    target_h, target_w = size
    methods = {"bilinear": 1, "nearest": 0, "bicubic": 3}
    order = methods.get(method.lower(), 1)
    zoom_factors = (target_h / h, target_w / w)
    if len(image.shape) == 3:
        zoom_factors = zoom_factors + (1,)
    return zoom(image, zoom_factors, order=order)


def crop_image(image: np.ndarray, bbox: Tuple[int, int, int, int]) -> np.ndarray:
    """
    Crop image to bounding box.

    Args:
        image: Input image
        bbox: Bounding box (x_min, y_min, x_max, y_max)

    Returns:
        Cropped image

    Example:
        >>> crop_image(np.ones((100, 100, 3)), (10, 10, 50, 50)).shape
        (40, 40, 3)
    """
    x_min, y_min, x_max, y_max = bbox
    return image[y_min:y_max, x_min:x_max]


def pad_image(
    image: np.ndarray, padding: Union[int, Tuple[int, int]], mode: str = "constant"
) -> np.ndarray:
    """
    Pad image with border.

    Args:
        image: Input image
        padding: Padding size (int or (pad_h, pad_w))
        mode: Padding mode (constant, reflect, replicate)

    Returns:
        Padded image

    Example:
        >>> pad_image(np.ones((3, 3)), 1).shape
        (5, 5)
    """
    if isinstance(padding, int):
        pad_h = pad_w = padding
    else:
        pad_h, pad_w = padding
    pad_width = ((pad_h, pad_h), (pad_w, pad_w))
    if len(image.shape) == 3:
        pad_width = pad_width + ((0, 0),)
    if mode == "constant":
        return np.pad(image, pad_width, mode=mode, constant_values=0)
    return np.pad(image, pad_width, mode=mode)


def horizontal_flip(image: np.ndarray) -> np.ndarray:
    """Flip image horizontally."""
    return np.fliplr(image)


def vertical_flip(image: np.ndarray) -> np.ndarray:
    """Flip image vertically."""
    return np.flipud(image)


def rotate_image(image: np.ndarray, angle: float, mode: str = "constant") -> np.ndarray:
    """
    Rotate image by angle in degrees.

    Args:
        image: Input image
        angle: Rotation angle in degrees (positive = counter-clockwise)
        mode: Border mode (constant, reflect, nearest)

    Returns:
        Rotated image

    Example:
        >>> rotate_image(np.ones((3, 3)), 90).shape
        (3, 3)
    """
    from scipy.ndimage import rotate
    return rotate(image, angle, reshape=False, mode=mode)


def normalize_image(
    image: np.ndarray, mean: Union[float, List[float]] = None, std: Union[float, List[float]] = None
) -> np.ndarray:
    """
    Normalize image with mean and std.

    Args:
        image: Input image (0-255 range)
        mean: Mean value(s) for each channel
        std: Standard deviation value(s) for each channel

    Returns:
        Normalized image

    Example:
        >>> normalize_image(np.array([128, 128, 128]), mean=128, std=128)
        array([0., 0., 0.])
    """
    image = image / 255.0
    if mean is not None:
        mean = np.array(mean).reshape(-1, 1, 1) if len(image.shape) == 3 else np.array(mean).reshape(-1)
        image = image - mean
    if std is not None:
        std = np.array(std).reshape(-1, 1, 1) if len(image.shape) == 3 else np.array(std).reshape(-1)
        image = image / std
    return image


def compute_histogram(image: np.ndarray, bins: int = 256) -> Tuple[np.ndarray, np.ndarray]:
    """
    Compute image histogram.

    Args:
        image: Input grayscale image
        bins: Number of histogram bins

    Returns:
        Tuple of (histogram, bin_centers)

    Example:
        >>> h, bins = compute_histogram(np.array([0, 128, 255, 128]))
        >>> len(h)
        256
    """
    hist, bin_edges = np.histogram(image.flatten(), bins=bins, range=(0, 256))
    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
    return hist, bin_centers


def equalize_histogram(image: np.ndarray) -> np.ndarray:
    """
    Apply histogram equalization.

    Args:
        image: Input grayscale image (0-255)

    Returns:
        Equalized image

    Example:
        >>> equalize_histogram(np.array([[100, 150], [200, 250]])).dtype
        dtype('uint8')
    """
    hist, _ = np.histogram(image.flatten(), 256, (0, 256))
    cdf = hist.cumsum()
    cdf = (cdf - cdf.min()) * 255 / (cdf.max() - cdf.min())
    cdf = cdf.astype(np.uint8)
    return cdf[image]
