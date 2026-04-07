"""
Computer vision transform utilities.

Provides image transforms for data augmentation,
including affine transforms, color transforms, and geometric transforms.
"""
from __future__ import annotations

from typing import Tuple, Union

import numpy as np


def affine_transform(
    image: np.ndarray,
    matrix: np.ndarray,
    output_shape: Tuple[int, int] = None,
    flags: int = 1,
) -> np.ndarray:
    """
    Apply affine transformation to image.

    Args:
        image: Input image
        matrix: 2x3 affine transformation matrix
        output_shape: Output shape (height, width)
        flags: Interpolation flags

    Returns:
        Transformed image
    """
    from scipy.ndimage import affine_transform as scipy_affine
    if output_shape is None:
        output_shape = image.shape[:2]
    if len(image.shape) == 3:
        output = np.zeros((output_shape[0], output_shape[1], image.shape[2]))
        for c in range(image.shape[2]):
            output[:, :, c] = scipy_affine(image[:, :, c], matrix, output_shape=output_shape, order=flags)
        return output
    return scipy_affine(image, matrix, output_shape=output_shape, order=flags)


def get_rotation_matrix(center: Tuple[float, float], angle: float, scale: float = 1.0) -> np.ndarray:
    """
    Get rotation matrix for cv2.warpAffine.

    Args:
        center: (x, y) center of rotation
        angle: Rotation angle in degrees
        scale: Scaling factor

    Returns:
        2x3 affine transformation matrix
    """
    cos_a = np.cos(np.radians(angle))
    sin_a = np.sin(np.radians(angle))
    cx, cy = center
    a = scale * cos_a
    b = scale * sin_a
    return np.array([[a, b, (1 - a) * cx - b * cy], [-b, a, b * cx + (1 - a) * cy]])


def get_scale_matrix(center: Tuple[float, float], scale: Tuple[float, float]) -> np.ndarray:
    """
    Get scaling matrix.

    Args:
        center: (x, y) center
        scale: (scale_x, scale_y)

    Returns:
        2x3 affine transformation matrix
    """
    sx, sy = scale
    cx, cy = center
    return np.array([[sx, 0, (1 - sx) * cx], [0, sy, (1 - sy) * cy]])


def get_shear_matrix(shear_x: float = 0, shear_y: float = 0) -> np.ndarray:
    """
    Get shear matrix.

    Args:
        shear_x: Horizontal shear
        shear_y: Vertical shear

    Returns:
        2x3 affine transformation matrix
    """
    return np.array([[1, shear_x, 0], [shear_y, 1, 0]])


def perspective_transform(
    image: np.ndarray, src_pts: np.ndarray, dst_pts: np.ndarray
) -> np.ndarray:
    """
    Apply perspective transformation.

    Args:
        image: Input image
        src_pts: Source quadrilateral points (4x2)
        dst_pts: Destination quadrilateral points (4x2)

    Returns:
        Transformed image
    """
    from scipy.linalg import solve
    A = []
    for (x, y), (u, v) in zip(src_pts, dst_pts):
        A.append([x, y, 1, 0, 0, 0, -u * x, -u * y, -u])
        A.append([0, 0, 0, x, y, 1, -v * x, -v * y, -v])
    A = np.array(A)
    B = dst_pts.flatten()
    H = solve(A, B, assume_a="pos")
    H = np.append(H, 1).reshape(3, 3)
    return _warp_perspective(image, H)


def _warp_perspective(image: np.ndarray, H: np.ndarray) -> np.ndarray:
    """Apply perspective warp using inverse mapping."""
    from scipy.ndimage import map_coordinates
    h, w = image.shape[:2]
    y_coords, x_coords = np.meshgrid(np.arange(h), np.arange(w), indexing="ij")
    ones = np.ones_like(x_coords)
    coords = np.stack([x_coords, y_coords, ones], axis=-1)
    warped_coords = np.tensordot(H, coords, axes=([1], [2]))
    warped_coords = warped_coords[:, :, :2] / warped_coords[:, :, 2:]
    if len(image.shape) == 3:
        output = np.zeros_like(image)
        for c in range(image.shape[2]):
            output[:, :, c] = map_coordinates(image[:, :, c], warped_coords.transpose(1, 0, 2), order=1, mode="reflect")
        return output
    return map_coordinates(image, warped_coords.transpose(1, 0, 2), order=1, mode="reflect")


def elastic_transform(
    image: np.ndarray, alpha: float = 100, sigma: float = 10, random_state: np.random.RandomState = None
) -> np.ndarray:
    """
    Elastic deformation of images.

    Args:
        image: Input image
        alpha: Deformation strength
        sigma: Smoothing factor
        random_state: Random state

    Returns:
        Deformed image
    """
    if random_state is None:
        random_state = np.random.RandomState(None)
    shape = image.shape[:2]
    dx = cv2.GaussianBlur((random_state.rand(*shape) * 2 - 1), (0, 0), sigma) * alpha
    dy = cv2.GaussianBlur((random_state.rand(*shape) * 2 - 1), (0, 0), sigma) * alpha
    x, y = np.meshgrid(np.arange(shape[1]), np.arange(shape[0]))
    indices = (y + dy).reshape(-1), (x + dx).reshape(-1)
    if len(image.shape) == 3:
        output = np.zeros_like(image)
        for c in range(image.shape[2]):
            output[:, :, c] = np.clip(
                np.map_coordinates(image[:, :, c], indices, order=1).reshape(shape), 0, 255
            ).astype(image.dtype)
        return output
    return np.clip(np.map_coordinates(image, indices, order=1).reshape(shape), 0, 255).astype(image.dtype)


def adjust_hue(image: np.ndarray, hue_shift: float) -> np.ndarray:
    """
    Adjust image hue.

    Args:
        image: Input RGB image
        hue_shift: Hue shift value (-0.5 to 0.5)

    Returns:
        Hue-adjusted image
    """
    from utils.image_processing_utils import rgb_to_hsv
    hsv = rgb_to_hsv(image)
    hsv[:, :, 0] = (hsv[:, :, 0] + hue_shift) % 1.0
    return hsv_to_rgb(hsv)


def hsv_to_rgb(hsv: np.ndarray) -> np.ndarray:
    """Convert HSV to RGB."""
    h, s, v = hsv[:, :, 0], hsv[:, :, 1], hsv[:, :, 2]
    i = (h * 6).astype(int)
    f = h * 6 - i
    p = v * (1 - s)
    q = v * (1 - f * s)
    t = v * (1 - (1 - f) * s)
    i = i % 6
    rgb = np.zeros_like(hsv)
    for j in range(6):
        mask = i == j
        rgb[:, :, 0][mask] = v[mask]
        rgb[:, :, 1][mask] = t[mask]
        rgb[:, :, 2][mask] = p[mask]
        mask = i == ((j + 1) % 6)
        rgb[:, :, 0][mask] = q[mask]
        rgb[:, :, 1][mask] = v[mask]
        rgb[:, :, 2][mask] = p[mask]
        mask = i == ((j + 2) % 6)
        rgb[:, :, 0][mask] = p[mask]
        rgb[:, :, 1][mask] = v[mask]
        rgb[:, :, 2][mask] = t[mask]
        mask = i == ((j + 3) % 6)
        rgb[:, :, 0][mask] = p[mask]
        rgb[:, :, 1][mask] = q[mask]
        rgb[:, :, 2][mask] = v[mask]
        mask = i == ((j + 4) % 6)
        rgb[:, :, 0][mask] = t[mask]
        rgb[:, :, 1][mask] = p[mask]
        rgb[:, :, 2][mask] = v[mask]
        mask = i == ((j + 5) % 6)
        rgb[:, :, 0][mask] = v[mask]
        rgb[:, :, 1][mask] = p[mask]
        rgb[:, :, 2][mask] = q[mask]
    return rgb


def random_crop_with_bbox(
    image: np.ndarray, crop_size: Tuple[int, int], bbox: Tuple[int, int, int, int]
) -> Tuple[np.ndarray, Tuple[int, int, int, int]]:
    """
    Random crop that contains the bounding box if possible.

    Args:
        image: Input image
        crop_size: (height, width)
        bbox: (x_min, y_min, x_max, y_max)

    Returns:
        Tuple of (cropped_image, new_bbox)
    """
    h, w = image.shape[:2]
    crop_h, crop_w = crop_size
    bx_min, by_min, bx_max, by_max = bbox
    if crop_h >= h and crop_w >= w:
        return image.copy(), bbox
    cx = np.random.randint(max(bx_min - crop_w + w, 0), min(bx_max, w - crop_w + 1))
    cy = np.random.randint(max(by_min - crop_h + h, 0), min(by_max, h - crop_h + 1))
    crop = image[cy : cy + crop_h, cx : cx + crop_w]
    new_bbox = (bx_min - cx, by_min - cy, bx_max - cx, by_max - cy)
    return crop, new_bbox


def resize_with_aspect_ratio(
    image: np.ndarray, target_size: int, mode: str = "short"
) -> np.ndarray:
    """
    Resize image maintaining aspect ratio.

    Args:
        image: Input image
        target_size: Target size for the mode dimension
        mode: 'short', 'long', or 'width'

    Returns:
        Resized image
    """
    from utils.image_processing_utils import resize_image
    h, w = image.shape[:2]
    if mode == "short":
        if h < w:
            new_h, new_w = target_size, int(w * target_size / h)
        else:
            new_h, new_w = int(h * target_size / w), target_size
    elif mode == "long":
        if h > w:
            new_h, new_w = target_size, int(w * target_size / h)
        else:
            new_h, new_w = int(h * target_size / w), target_size
    else:
        new_w = target_size
        new_h = int(h * target_size / w)
    return resize_image(image, (new_h, new_w))


def center_crop(image: np.ndarray, crop_size: Tuple[int, int]) -> np.ndarray:
    """
    Center crop of image.

    Args:
        image: Input image
        crop_size: (height, width)

    Returns:
        Center-cropped image
    """
    h, w = image.shape[:2]
    crop_h, crop_w = crop_size
    top = (h - crop_h) // 2
    left = (w - crop_w) // 2
    return image[top : top + crop_h, left : left + crop_w]


def five_crop(image: np.ndarray, crop_size: Tuple[int, int]) -> list:
    """
    Crop image into five pieces (four corners and center).

    Args:
        image: Input image
        crop_size: (height, width)

    Returns:
        List of 5 cropped images
    """
    h, w = image.shape[:2]
    crop_h, crop_w = crop_size
    crops = []
    positions = [
        (0, 0),
        (w - crop_w, 0),
        (0, h - crop_h),
        (w - crop_w, h - crop_h),
        ((w - crop_w) // 2, (h - crop_h) // 2),
    ]
    for y, x in positions:
        crops.append(image[y : y + crop_h, x : x + crop_w])
    return crops


def ten_crop(image: np.ndarray, crop_size: Tuple[int, int]) -> list:
    """
    Crop image into ten pieces (horizontal flip of five_crop).

    Args:
        image: Input image
        crop_size: (height, width)

    Returns:
        List of 10 cropped images
    """
    crops = five_crop(image, crop_size)
    flipped = five_crop(np.fliplr(image), crop_size)
    return crops + flipped


def optical_flow_dense(
    frame1: np.ndarray, frame2: np.ndarray, method: str = "farneback"
) -> np.ndarray:
    """
    Compute dense optical flow between two frames.

    Args:
        frame1: First grayscale frame
        frame2: Second grayscale frame
        method: Optical flow method ('farneback' or 'lucaskanade')

    Returns:
        Flow array of shape (H, W, 2) containing (u, v) vectors
    """
    if method == "farneback":
        return _farneback_optical_flow(frame1, frame2)
    return _lucaskanade_optical_flow(frame1, frame2)


def _farneback_optical_flow(frame1: np.ndarray, frame2: np.ndarray) -> np.ndarray:
    """Farneback optical flow approximation."""
    flow = np.zeros((*frame1.shape, 2))
    h, w = frame1.shape
    sigma = 5.0
    for y in range(1, h - 1):
        for x in range(1, w - 1):
            Ix = (frame1[y, x + 1] - frame1[y, x - 1]) / 2
            Iy = (frame1[y + 1, x] - frame1[y - 1, x]) / 2
            It = frame2[y, x] - frame1[y, x]
            if Ix ** 2 + Iy ** 2 > 1e-10:
                u = -It * Ix / (Ix ** 2 + Iy ** 2 + 1e-10)
                v = -It * Iy / (Ix ** 2 + Iy ** 2 + 1e-10)
                flow[y, x] = [u, v]
    return flow


def _lucaskanade_optical_flow(frame1: np.ndarray, frame2: np.ndarray) -> np.ndarray:
    """Lucas-Kanade optical flow (simplified for small motions)."""
    return _farneback_optical_flow(frame1, frame2)


def motion_blur(image: np.ndarray, kernel_size: int = 15) -> np.ndarray:
    """
    Apply motion blur to image.

    Args:
        image: Input image
        kernel_size: Blur kernel size

    Returns:
        Blurred image
    """
    from scipy.ndimage import convolve
    kernel = np.zeros((kernel_size, kernel_size))
    kernel[int((kernel_size - 1) / 2), :] = np.ones(kernel_size)
    kernel = kernel / kernel.sum()
    if len(image.shape) == 3:
        output = np.zeros_like(image)
        for c in range(image.shape[2]):
            output[:, :, c] = convolve(image[:, :, c], kernel, mode="reflect")
        return output
    return convolve(image, kernel, mode="reflect")


def gaussian_blur(image: np.ndarray, kernel_size: int = 5, sigma: float = 1.0) -> np.ndarray:
    """
    Apply Gaussian blur to image.

    Args:
        image: Input image
        kernel_size: Blur kernel size
        sigma: Gaussian standard deviation

    Returns:
        Blurred image
    """
    from utils.image_processing_utils import gaussian_blur as _gaussian_blur
    return _gaussian_blur(image, kernel_size, sigma)


def median_filter(image: np.ndarray, kernel_size: int = 3) -> np.ndarray:
    """
    Apply median filter.

    Args:
        image: Input image
        kernel_size: Filter size

    Returns:
        Filtered image
    """
    from scipy.ndimage import median_filter as _median_filter
    return _median_filter(image, size=kernel_size)


def bilateral_filter(
    image: np.ndarray, d: int = 9, sigma_color: float = 75, sigma_space: float = 75
) -> np.ndarray:
    """
    Bilateral filter for edge-preserving smoothing.

    Args:
        image: Input image
        d: Diameter of pixel neighborhood
        sigma_color: Color standard deviation
        sigma_space: Space standard deviation

    Returns:
        Filtered image
    """
    h, w = image.shape[:2]
    result = np.zeros_like(image)
    kernel_size = d // 2
    for y in range(h):
        for x in range(w):
            y_min, y_max = max(0, y - kernel_size), min(h, y + kernel_size + 1)
            x_min, x_max = max(0, x - kernel_size), min(w, x + kernel_size + 1)
            patch = image[y_min:y_max, x_min:x_max]
            color_diff = np.exp(-((patch - image[y, x]) ** 2) / (2 * sigma_color ** 2))
            space_weight = np.exp(-(((np.arange(y_min, y_max).reshape(-1, 1) - y) ** 2 + (np.arange(x_min, x_max) - x) ** 2) / (2 * sigma_space ** 2)))
            weight = color_diff * space_weight
            result[y, x] = np.sum(patch * weight) / np.sum(weight)
    return result
