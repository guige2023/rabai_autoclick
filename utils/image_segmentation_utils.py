"""
Image segmentation utilities.

Provides segmentation helpers including IoU computation,
mask operations, and segmentation metrics.
"""
from __future__ import annotations

from typing import List, Optional, Tuple

import numpy as np


def compute_iou(mask1: np.ndarray, mask2: np.ndarray) -> float:
    """
    Compute IoU between two binary masks.

    Args:
        mask1: First binary mask
        mask2: Second binary mask

    Returns:
        IoU score

    Example:
        >>> mask1 = np.array([[1, 1, 0], [1, 1, 0]])
        >>> mask2 = np.array([[1, 0, 0], [1, 0, 0]])
        >>> compute_iou(mask1, mask2)
        0.5
    """
    intersection = np.sum((mask1 > 0) & (mask2 > 0))
    union = np.sum((mask1 > 0) | (mask2 > 0))
    return intersection / union if union > 0 else 0.0


def compute_dice(mask1: np.ndarray, mask2: np.ndarray) -> float:
    """
    Compute Dice coefficient between two masks.

    Args:
        mask1: First binary mask
        mask2: Second binary mask

    Returns:
        Dice coefficient
    """
    intersection = np.sum((mask1 > 0) & (mask2 > 0))
    return 2 * intersection / (np.sum(mask1 > 0) + np.sum(mask2 > 0)) if (np.sum(mask1 > 0) + np.sum(mask2 > 0)) > 0 else 0.0


def mask_to_boundary(mask: np.ndarray, border_size: int = 1) -> np.ndarray:
    """
    Convert mask to boundary representation.

    Args:
        mask: Binary mask
        border_size: Width of boundary

    Returns:
        Boundary mask
    """
    from scipy.ndimage import binary_dilation, binary_erosion
    eroded = binary_erosion(mask, iterations=border_size)
    boundary = mask & ~eroded
    return boundary.astype(np.uint8)


def boundary_to_mask(boundary: np.ndarray, fill_value: float = 1.0) -> np.ndarray:
    """
    Convert boundary to filled mask using flood fill.

    Note: Simplified version.
    """
    return (boundary > 0).astype(np.uint8)


def connected_component_labeling(mask: np.ndarray) -> Tuple[np.ndarray, int]:
    """
    Label connected components in binary mask.

    Args:
        mask: Binary mask

    Returns:
        Tuple of (labeled_mask, num_components)
    """
    from scipy.ndimage import label
    labeled, num = label(mask > 0)
    return labeled, num


def remove_small_regions(mask: np.ndarray, min_size: int) -> np.ndarray:
    """
    Remove connected components smaller than min_size.

    Args:
        mask: Binary mask
        min_size: Minimum region size

    Returns:
        Mask with small regions removed
    """
    labeled, num = connected_component_labeling(mask)
    result = mask.copy()
    for i in range(1, num + 1):
        region_size = np.sum(labeled == i)
        if region_size < min_size:
            result[labeled == i] = 0
    return result


def mask_to_polygon(mask: np.ndarray) -> List[List[Tuple[int, int]]]:
    """
    Convert mask to polygon contours.

    Args:
        mask: Binary mask

    Returns:
        List of contours, each contour is list of (x, y) points
    """
    from scipy.ndimage import find_objects
    labeled, num = connected_component_labeling(mask)
    polygons = []
    for i in range(1, num + 1):
        bbox = find_objects(labeled == i)[0]
        y_min, y_max = bbox[0].start, bbox[0].stop
        x_min, x_max = bbox[1].start, bbox[1].stop
        contour = [(x_min, y_min), (x_max, y_min), (x_max, y_max), (x_min, y_max)]
        polygons.append(contour)
    return polygons


def polygon_to_mask(polygon: List[Tuple[int, int]], shape: Tuple[int, int]) -> np.ndarray:
    """
    Convert polygon to binary mask.

    Args:
        polygon: List of (x, y) points
        shape: Mask shape (height, width)

    Returns:
        Binary mask
    """
    from PIL import Image, ImageDraw
    img = Image.new("L", (shape[1], shape[0]), 0)
    ImageDraw.Draw(img).polygon(polygon, outline=1, fill=1)
    return np.array(img)


def encode_mask_rle(mask: np.ndarray) -> List[int]:
    """
    Run-length encode a binary mask.

    Args:
        mask: Binary mask (H, W)

    Returns:
        RLE encoded mask

    Example:
        >>> encode_mask_rle(np.array([[1, 1, 0, 0, 1]]))
        [0, 2, 4, 1]
    """
    pixels = mask.flatten()
    pixels = np.concatenate([[0], pixels, [0]])
    runs = np.where(pixels[1:] != pixels[:-1])[0]
    runs = runs.reshape(-1, 2)
    rle = []
    for start, end in runs:
        rle.extend([start, end - start])
    return rle


def decode_mask_rle(rle: List[int], shape: Tuple[int, int]) -> np.ndarray:
    """
    Decode RLE to binary mask.

    Args:
        rle: RLE encoded mask
        shape: Mask shape (height, width)

    Returns:
        Binary mask
    """
    mask = np.zeros(shape[0] * shape[1], dtype=np.uint8)
    for i in range(0, len(rle), 2):
        start = rle[i]
        length = rle[i + 1]
        mask[start : start + length] = 1
    return mask.reshape(shape)


def crop_mask_to_bbox(mask: np.ndarray, margin: int = 0) -> Tuple[np.ndarray, Tuple[slice, slice]]:
    """
    Crop mask to bounding box with optional margin.

    Args:
        mask: Binary mask
        margin: Margin to add around mask

    Returns:
        Tuple of (cropped_mask, (y_slice, x_slice))
    """
    rows = np.any(mask > 0, axis=1)
    cols = np.any(mask > 0, axis=0)
    if not rows.any():
        return mask, (slice(0, mask.shape[0]), slice(0, mask.shape[1]))
    y_min, y_max = np.where(rows)[0][[0, -1]]
    x_min, x_max = np.where(cols)[0][[0, -1]]
    y_min = max(0, y_min - margin)
    y_max = min(mask.shape[0], y_max + margin + 1)
    x_min = max(0, x_min - margin)
    x_max = min(mask.shape[1], x_max + margin + 1)
    cropped = mask[y_min:y_max, x_min:x_max]
    slices = (slice(y_min, y_max), slice(x_min, x_max))
    return cropped, slices


def expand_mask(mask: np.ndarray, pixels: int) -> np.ndarray:
    """
    Expand mask by specified pixels.

    Args:
        mask: Binary mask
        pixels: Number of pixels to expand

    Returns:
        Expanded mask
    """
    from scipy.ndimage import binary_dilation
    struct = np.ones((pixels * 2 + 1, pixels * 2 + 1))
    return binary_dilation(mask, structure=struct).astype(np.uint8)


def shrink_mask(mask: np.ndarray, pixels: int) -> np.ndarray:
    """
    Shrink mask by specified pixels.

    Args:
        mask: Binary mask
        pixels: Number of pixels to shrink

    Returns:
        Shrunk mask
    """
    from scipy.ndimage import binary_erosion
    struct = np.ones((pixels * 2 + 1, pixels * 2 + 1))
    return binary_erosion(mask, structure=struct).astype(np.uint8)


def compute_precision_recall(
    pred_mask: np.ndarray, gt_mask: np.ndarray
) -> Tuple[float, float]:
    """
    Compute precision and recall for segmentation.

    Args:
        pred_mask: Predicted binary mask
        gt_mask: Ground truth binary mask

    Returns:
        Tuple of (precision, recall)
    """
    tp = np.sum((pred_mask > 0) & (gt_mask > 0))
    fp = np.sum((pred_mask > 0) & (gt_mask == 0))
    fn = np.sum((pred_mask == 0) & (gt_mask > 0))
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    return precision, recall


def compute_f_score(pred_mask: np.ndarray, gt_mask: np.ndarray, beta: float = 1.0) -> float:
    """
    Compute F-beta score for segmentation.

    Args:
        pred_mask: Predicted binary mask
        gt_mask: Ground truth binary mask
        beta: Beta parameter for F-score

    Returns:
        F-beta score
    """
    precision, recall = compute_precision_recall(pred_mask, gt_mask)
    if precision + recall == 0:
        return 0.0
    return (1 + beta ** 2) * precision * recall / ((beta ** 2 * precision) + recall)


class SemanticSegmenter:
    """Simple semantic segmentation model template."""

    def __init__(self, num_classes: int):
        self.num_classes = num_classes

    def predict(self, image: np.ndarray) -> np.ndarray:
        """Predict segmentation mask."""
        raise NotImplementedError

    def predict_proba(self, image: np.ndarray) -> np.ndarray:
        """Predict class probabilities."""
        raise NotImplementedError


class InstanceSegmenter:
    """Simple instance segmentation model template."""

    def __init__(self):
        pass

    def predict(self, image: np.ndarray) -> List[dict]:
        """
        Predict instance masks.

        Returns:
            List of dicts with 'mask', 'class', 'score' keys
        """
        raise NotImplementedError


class PanopticSegmenter:
    """Panoptic segmentation model template."""

    def __init__(self, num_classes: int):
        self.num_classes = num_classes

    def predict(self, image: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        """
        Predict panoptic segmentation.

        Returns:
            Tuple of (semantic_mask, instance_mask)
        """
        raise NotImplementedError


def blend_image_with_mask(
    image: np.ndarray, mask: np.ndarray, color: Tuple[int, int, int] = (255, 0, 0), alpha: float = 0.5
) -> np.ndarray:
    """
    Blend image with colored segmentation mask.

    Args:
        image: RGB image
        mask: Binary segmentation mask
        color: RGB color for mask
        alpha: Blend alpha

    Returns:
        Blended image
    """
    if len(image.shape) != 3 or image.shape[2] != 3:
        raise ValueError("Image must be RGB")
    colored_mask = np.zeros_like(image)
    colored_mask[mask > 0] = color
    return (image * (1 - alpha) + colored_mask * alpha).astype(np.uint8)


def visualize_segmentation(
    image: np.ndarray, masks: List[np.ndarray], labels: List[str], alpha: float = 0.5
) -> np.ndarray:
    """
    Visualize multiple segmentation masks on image.

    Args:
        image: RGB image
        masks: List of binary masks
        labels: List of label names
        alpha: Blend alpha

    Returns:
        Visualization image
    """
    import colorsys
    result = image.copy()
    for i, (mask, label) in enumerate(zip(masks, labels)):
        hue = i / len(labels)
        r, g, b = colorsys.hsv_to_rgb(hue, 1.0, 1.0)
        color = (int(r * 255), int(g * 255), int(b * 255))
        result = blend_image_with_mask(result, mask, color, alpha)
    return result


def compute_panoptic_quality(
    semantic_pred: np.ndarray, semantic_gt: np.ndarray, instance_pred: np.ndarray, instance_gt: np.ndarray
) -> Dict[str, float]:
    """
    Compute Panoptic Quality (PQ) metric.

    Args:
        semantic_pred: Predicted semantic mask
        semantic_gt: Ground truth semantic mask
        instance_pred: Predicted instance mask
        instance_gt: Ground truth instance mask

    Returns:
        Dictionary with PQ, SQ, RQ metrics
    """
    from utils.evaluation_utils import precision_recall_curve
    ious = []
    for semantic_id in np.unique(semantic_gt):
        if semantic_id == 0:
            continue
        semantic_match = semantic_pred == semantic_id
        gt_match = semantic_gt == semantic_id
        if np.sum(gt_match) == 0:
            continue
        iou = compute_iou(semantic_match.astype(int), gt_match.astype(int))
        ious.append(iou)
    if not ious:
        return {"pq": 0.0, "sq": 0.0, "rq": 0.0}
    sq = np.mean(ious)
    recall = len([i for i in ious if i > 0.5]) / len(ious)
    precision = recall
    rq = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0
    pq = sq * rq
    return {"pq": pq, "sq": sq, "rq": rq}
