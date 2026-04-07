"""
Data augmentation utilities for ML.

Provides augmentation strategies for images, text, and tabular data.
"""
from __future__ import annotations

import random
from typing import List, Optional, Sequence

import numpy as np


def random_crop(image: np.ndarray, crop_size: tuple) -> np.ndarray:
    """
    Random crop of an image.

    Args:
        image: Input image array
        crop_size: Target (height, width)

    Returns:
        Cropped image
    """
    h, w = image.shape[:2]
    crop_h, crop_w = crop_size
    if h < crop_h or w < crop_w:
        raise ValueError(f"Image {h}x{w} smaller than crop {crop_h}x{crop_w}")
    top = random.randint(0, h - crop_h)
    left = random.randint(0, w - crop_w)
    return image[top : top + crop_h, left : left + crop_w]


def random_flip_horizontal(image: np.ndarray, p: float = 0.5) -> np.ndarray:
    """
    Random horizontal flip.

    Args:
        image: Input image
        p: Probability of flip

    Returns:
        Possibly flipped image
    """
    if random.random() < p:
        return np.fliplr(image)
    return image


def random_rotation(image: np.ndarray, max_angle: float = 15.0) -> np.ndarray:
    """
    Random rotation within angle range.

    Args:
        image: Input image
        max_angle: Maximum rotation angle in degrees

    Returns:
        Rotated image
    """
    from scipy.ndimage import rotate
    angle = random.uniform(-max_angle, max_angle)
    return rotate(image, angle, reshape=False, mode='reflect')


def random_brightness(image: np.ndarray, factor_range: tuple = (0.8, 1.2)) -> np.ndarray:
    """
    Randomly adjust brightness.

    Args:
        image: Input image
        factor_range: Min/max brightness factor

    Returns:
        Brightness-adjusted image
    """
    factor = random.uniform(*factor_range)
    return np.clip(image * factor, 0, 255).astype(image.dtype)


def random_contrast(image: np.ndarray, factor_range: tuple = (0.8, 1.2)) -> np.ndarray:
    """
    Randomly adjust contrast.

    Args:
        image: Input image
        factor_range: Min/max contrast factor

    Returns:
        Contrast-adjusted image
    """
    factor = random.uniform(*factor_range)
    mean = np.mean(image)
    return np.clip((image - mean) * factor + mean, 0, 255).astype(image.dtype)


def random_noise(image: np.ndarray, noise_level: float = 0.1) -> np.ndarray:
    """
    Add Gaussian noise to image.

    Args:
        image: Input image
        noise_level: Standard deviation of noise (relative to 255)

    Returns:
        Noisy image
    """
    noise = np.random.randn(*image.shape) * noise_level * 255
    noisy = image + noise
    return np.clip(noisy, 0, 255).astype(image.dtype)


def cutout(
    image: np.ndarray, n_holes: int = 1, hole_size: int = 16
) -> np.ndarray:
    """
    Cut out random holes in image (regularization technique).

    Args:
        image: Input image
        n_holes: Number of holes to cut
        hole_size: Size of each hole

    Returns:
        Image with cutout regions
    """
    h, w = image.shape[:2]
    result = image.copy()
    for _ in range(n_holes):
        y = random.randint(0, h - hole_size)
        x = random.randint(0, w - hole_size)
        result[y : y + hole_size, x : x + hole_size] = 0
    return result


def mixup(
    image1: np.ndarray, image2: np.ndarray, alpha: float = 0.5
) -> tuple:
    """
    Mixup augmentation: blend two images.

    Args:
        image1: First image
        image2: Second image
        alpha: Mixup parameter

    Returns:
        Tuple of (mixed_image, lambda_weight)
    """
    lam = np.random.beta(alpha, alpha)
    mixed = (lam * image1 + (1 - lam) * image2).astype(image1.dtype)
    return mixed, lam


def cutmix(
    image1: np.ndarray, image2: np.ndarray, hole_size: int = 32
) -> np.ndarray:
    """
    CutMix augmentation: cut and paste patches between images.

    Args:
        image1: First image
        image2: Second image
        hole_size: Size of patch to swap

    Returns:
        CutMix augmented image
    """
    h, w = image1.shape[:2]
    y = random.randint(0, h - hole_size)
    x = random.randint(0, w - hole_size)
    result = image1.copy()
    result[y : y + hole_size, x : x + hole_size] = image2[y : y + hole_size, x : x + hole_size]
    return result


def random_erasing(
    image: np.ndarray, p: float = 0.5, scale_range: tuple = (0.02, 0.1)
) -> np.ndarray:
    """
    Random Erasing data augmentation.

    Args:
        image: Input image
        p: Probability of applying
        scale_range: Area fraction range for erasing

    Returns:
        Possibly erased image
    """
    if random.random() > p:
        return image
    h, w = image.shape[:2]
    area = h * w
    target_area = random.uniform(*scale_range) * area
    aspect_ratio = random.uniform(0.3, 3.0)
    eh = int(np.sqrt(target_area * aspect_ratio))
    ew = int(np.sqrt(target_area / aspect_ratio))
    if eh >= h or ew >= w:
        return image
    y = random.randint(0, h - eh)
    x = random.randint(0, w - ew)
    result = image.copy()
    result[y : y + eh, x : x + ew] = np.random.randint(0, 256, (eh, ew, image.shape[2] if len(image.shape) == 3 else 1))
    return result


class ImageAugmenter:
    """Composable image augmentation pipeline."""

    def __init__(self, augmentations: List = None):
        self.augmentations = augmentations or []

    def add(self, aug: callable) -> "ImageAugmenter":
        """Add an augmentation function."""
        self.augmentations.append(aug)
        return self

    def __call__(self, image: np.ndarray) -> np.ndarray:
        """Apply all augmentations."""
        for aug in self.augmentations:
            image = aug(image)
        return image


def random_word_dropout(text: str, dropout_prob: float = 0.1) -> str:
    """
    Randomly drop words from text.

    Args:
        text: Input text
        dropout_prob: Probability of dropping each word

    Returns:
        Text with dropped words
    """
    words = text.split()
    kept = [w for w in words if random.random() > dropout_prob]
    return " ".join(kept) if kept else text


def random_word_swap(text: str, n_swaps: int = 1) -> str:
    """
    Randomly swap words in text.

    Args:
        text: Input text
        n_swaps: Number of word swaps

    Returns:
        Text with swapped words
    """
    words = text.split()
    if len(words) < 2:
        return text
    for _ in range(n_swaps):
        i, j = random.sample(range(len(words)), 2)
        words[i], words[j] = words[j], words[i]
    return " ".join(words)


def synonym_replacement(text: str, n_replacements: int = 1) -> str:
    """
    Replace words with synonyms (placeholder).

    Note: Requires external synonym dictionary or API.
    """
    synonyms = {
        "good": ["great", "excellent", "fine"],
        "bad": ["poor", "terrible", "awful"],
        "big": ["large", "huge", "enormous"],
        "small": ["tiny", "little", "miniature"],
    }
    words = text.split()
    replaced = 0
    for i, word in enumerate(words):
        if word.lower() in synonyms and replaced < n_replacements:
            words[i] = random.choice(synonyms[word.lower()])
            replaced += 1
    return " ".join(words)


def back_translation(text: str) -> str:
    """
    Back translation augmentation (placeholder).

    Note: Requires translation API.
    """
    return text


def tabular_noise(
    data: np.ndarray, noise_level: float = 0.01, strategy: str = "gaussian"
) -> np.ndarray:
    """
    Add noise to tabular data.

    Args:
        data: Input array
        noise_level: Standard deviation for gaussian, rate for poisson
        strategy: Noise type (gaussian, uniform, poisson)

    Returns:
        Noisy data
    """
    if strategy == "gaussian":
        noise = np.random.randn(*data.shape) * noise_level * np.std(data)
    elif strategy == "uniform":
        noise = np.random.uniform(-noise_level, noise_level, data.shape) * np.ptp(data)
    else:
        noise = np.random.poisson(noise_level * data) - noise_level * data
    return data + noise


def smote_oversample(
    X: np.ndarray, y: np.ndarray, k_neighbors: int = 5
) -> tuple:
    """
    SMOTE: Synthetic Minority Over-sampling Technique.

    Args:
        X: Feature matrix (n_samples, n_features)
        y: Labels
        k_neighbors: Number of neighbors for interpolation

    Returns:
        Tuple of (X_resampled, y_resampled)
    """
    from scipy.spatial.distance import cdist
    classes, counts = np.unique(y, return_counts=True)
    max_count = max(counts)
    X_resampled = [X.copy()]
    y_resampled = [y.copy()]
    for cls, count in zip(classes, counts):
        if count < max_count:
            cls_indices = np.where(y == cls)[0]
            cls_samples = X[cls_indices]
            n_synthetic = max_count - count
            for _ in range(n_synthetic):
                idx = random.choice(cls_indices)
                sample = X[idx]
                distances = cdist([sample], cls_samples, 'euclidean')[0]
                nearest_indices = np.argsort(distances)[1:k_neighbors + 1]
                neighbor = cls_samples[random.choice(nearest_indices)]
                alpha = random.random()
                synthetic = sample + alpha * (neighbor - sample)
                X_resampled.append(synthetic[np.newaxis, :])
                y_resampled.append(np.array([cls]))
    return np.vstack(X_resampled), np.concatenate(y_resampled)


def random_undersample(X: np.ndarray, y: np.ndarray) -> tuple:
    """
    Random undersampling of majority class.

    Args:
        X: Feature matrix
        y: Labels

    Returns:
        Tuple of (X_resampled, y_resampled)
    """
    classes, counts = np.unique(y, return_counts=True)
    min_count = min(counts)
    indices = []
    for cls in classes:
        cls_indices = np.where(y == cls)[0]
        indices.extend(random.sample(list(cls_indices), min_count))
    indices = np.array(indices)
    random.shuffle(indices)
    return X[indices], y[indices]
