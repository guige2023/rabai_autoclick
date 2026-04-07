"""
Object detection utilities.

Provides bounding box operations, NMS, anchor generation,
and detection metric computations.
"""
from __future__ import annotations

from typing import List, Tuple

import numpy as np


def bbox_iou(box1: np.ndarray, box2: np.ndarray) -> float:
    """
    Compute IoU between two bounding boxes.

    Args:
        box1: First box (x1, y1, x2, y2)
        box2: Second box (x1, y1, x2, y2)

    Returns:
        IoU score

    Example:
        >>> bbox_iou(np.array([0, 0, 2, 2]), np.array([1, 1, 3, 3]))
        0.14285714285714285
    """
    x1 = max(box1[0], box2[0])
    y1 = max(box1[1], box2[1])
    x2 = min(box1[2], box2[2])
    y2 = min(box1[3], box2[3])
    if x2 <= x1 or y2 <= y1:
        return 0.0
    intersection = (x2 - x1) * (y2 - y1)
    area1 = (box1[2] - box1[0]) * (box1[3] - box1[1])
    area2 = (box2[2] - box2[0]) * (box2[3] - box2[1])
    union = area1 + area2 - intersection
    return intersection / union if union > 0 else 0.0


def bbox_iou_matrix(boxes1: np.ndarray, boxes2: np.ndarray) -> np.ndarray:
    """
    Compute IoU matrix between two sets of boxes.

    Args:
        boxes1: First set of boxes (N, 4)
        boxes2: Second set of boxes (M, 4)

    Returns:
        IoU matrix (N, M)

    Example:
        >>> boxes1 = np.array([[0, 0, 2, 2], [5, 5, 7, 7]])
        >>> boxes2 = np.array([[1, 1, 3, 3]])
        >>> bbox_iou_matrix(boxes1, boxes2).shape
        (2, 1)
    """
    n = len(boxes1)
    m = len(boxes2)
    iou_matrix = np.zeros((n, m))
    for i in range(n):
        for j in range(m):
            iou_matrix[i, j] = bbox_iou(boxes1[i], boxes2[j])
    return iou_matrix


def nms(
    boxes: np.ndarray, scores: np.ndarray, iou_threshold: float = 0.5
) -> List[int]:
    """
    Non-Maximum Suppression.

    Args:
        boxes: Bounding boxes (N, 4) as (x1, y1, x2, y2)
        scores: Confidence scores (N,)
        iou_threshold: IoU threshold for suppression

    Returns:
        List of indices to keep

    Example:
        >>> boxes = np.array([[0, 0, 10, 10], [1, 1, 11, 11], [5, 5, 15, 15]])
        >>> scores = np.array([0.9, 0.8, 0.7])
        >>> nms(boxes, scores, iou_threshold=0.5)
        [0, 2]
    """
    if len(boxes) == 0:
        return []
    indices = np.argsort(scores)[::-1]
    keep = []
    while len(indices) > 0:
        current = indices[0]
        keep.append(current)
        if len(indices) == 1:
            break
        rest = indices[1:]
        ious = np.array([bbox_iou(boxes[current], boxes[i]) for i in rest])
        indices = rest[ious <= iou_threshold]
    return keep


def soft_nms(
    boxes: np.ndarray, scores: np.ndarray, sigma: float = 0.5, thresh: float = 0.01
) -> List[int]:
    """
    Soft Non-Maximum Suppression.

    Args:
        boxes: Bounding boxes (N, 4)
        scores: Confidence scores (N,)
        sigma: Gaussian parameter
        thresh: Score threshold

    Returns:
        List of indices to keep
    """
    N = len(boxes)
    for i in range(N):
        max_idx = np.argmax(scores[i:]) + i
        boxes[[i, max_idx]] = boxes[[max_idx, i]]
        scores[[i, max_idx]] = scores[[max_idx, i]]
        for j in range(i + 1, N):
            iou = bbox_iou(boxes[i], boxes[j])
            scores[j] *= np.exp(-(iou ** 2) / sigma)
    return [i for i in range(N) if scores[i] > thresh]


def generate_anchors(
    base_size: int,
    ratios: List[float] = [0.5, 1.0, 2.0],
    scales: List[float] = [8, 16, 32],
    stride: int = 16,
) -> np.ndarray:
    """
    Generate anchor boxes.

    Args:
        base_size: Base size for anchors
        ratios: Aspect ratios
        scales: Scales
        stride: Feature map stride

    Returns:
        Anchor boxes (N, 4) as (x1, y1, x2, y2)

    Example:
        >>> anchors = generate_anchors(16)
        >>> anchors.shape[0]
        9
    """
    anchors = []
    for scale in scales:
        area = base_size * scale
        for ratio in ratios:
            h = np.sqrt(area / ratio)
            w = ratio * h
            x1, y1 = -w / 2, -h / 2
            x2, y2 = w / 2, h / 2
            anchors.append([x1, y1, x2, y2])
    return np.array(anchors)


def generate_anchor_grid(
    feat_h: int,
    feat_w: int,
    stride: int,
    base_anchors: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Generate anchor grid across feature map.

    Args:
        feat_h: Feature map height
        feat_w: Feature map width
        stride: Feature stride
        base_anchors: Base anchor templates

    Returns:
        Tuple of (all_anchors, shift_x, shift_y)
    """
    shift_x = np.arange(feat_w) * stride
    shift_y = np.arange(feat_h) * stride
    shift_x, shift_y = np.meshgrid(shift_x, shift_y)
    shifts = np.stack([shift_x, shift_y, shift_x, shift_y], axis=2).reshape(-1, 4)
    anchors = (shifts[:, np.newaxis, :] + base_anchors[np.newaxis, :, :]).reshape(-1, 4)
    return anchors, shift_x, shift_y


def bbox_encode(
    boxes: np.ndarray, anchors: np.ndarray, means: Tuple[float, ...] = (0.0, 0.0, 0.0, 0.0), stds: Tuple[float, ...] = (0.1, 0.1, 0.2, 0.2)
) -> np.ndarray:
    """
    Encode bounding boxes relative to anchors.

    Args:
        boxes: Target boxes (N, 4)
        anchors: Anchor boxes (N, 4)
        means: Encoding means
        stds: Encoding stds

    Returns:
        Encoded boxes

    Example:
        >>> boxes = np.array([[10, 10, 50, 50]])
        >>> anchors = np.array([[0, 0, 32, 32]])
        >>> encoded = bbox_encode(boxes, anchors)
    """
    wx, wy, ww, wh = means[0], means[1], means[2], means[3]
    sx, sy, sw, sh = stds[0], stds[1], stds[2], stds[3]
    a_x1, a_y1, a_x2, a_y2 = anchors[:, 0], anchors[:, 1], anchors[:, 2], anchors[:, 3]
    b_x1, b_y1, b_x2, b_y2 = boxes[:, 0], boxes[:, 1], boxes[:, 2], boxes[:, 3]
    a_w = a_x2 - a_x1 + 1
    a_h = a_y2 - a_y1 + 1
    a_cx = (a_x1 + a_x2) / 2
    a_cy = (a_y1 + a_y2) / 2
    b_cx = (b_x1 + b_x2) / 2
    b_cy = (b_y1 + b_y2) / 2
    b_w = b_x2 - b_x1 + 1
    b_h = b_y2 - b_y1 + 1
    encoded = np.zeros_like(boxes)
    encoded[:, 0] = (b_cx - a_cx) / a_w / sx - wx
    encoded[:, 1] = (b_cy - a_cy) / a_h / sy - wy
    encoded[:, 2] = (np.log(b_w / a_w)) / sw - ww
    encoded[:, 3] = (np.log(b_h / a_h)) / sh - wh
    return encoded


def bbox_decode(
    encoded: np.ndarray, anchors: np.ndarray, means: Tuple[float, ...] = (0.0, 0.0, 0.0, 0.0), stds: Tuple[float, ...] = (0.1, 0.1, 0.2, 0.2)
) -> np.ndarray:
    """
    Decode bounding boxes from anchors.

    Args:
        encoded: Encoded boxes (N, 4)
        anchors: Anchor boxes (N, 4)
        means: Decoding means
        stds: Decoding stds

    Returns:
        Decoded boxes
    """
    wx, wy, ww, wh = means[0], means[1], means[2], means[3]
    sx, sy, sw, sh = stds[0], stds[1], stds[2], stds[3]
    a_x1, a_y1, a_x2, a_y2 = anchors[:, 0], anchors[:, 1], anchors[:, 2], anchors[:, 3]
    a_w = a_x2 - a_x1 + 1
    a_h = a_y2 - a_y1 + 1
    a_cx = (a_x1 + a_x2) / 2
    a_cy = (a_y1 + a_y2) / 2
    decoded = np.zeros_like(encoded)
    decoded[:, 0] = (encoded[:, 0] + wx) * sx * a_w + a_cx
    decoded[:, 1] = (encoded[:, 1] + wy) * sy * a_h + a_cy
    decoded[:, 2] = np.exp((encoded[:, 2] + ww) * sw) * a_w
    decoded[:, 3] = np.exp((encoded[:, 3] + wh) * sh) * a_h
    decoded[:, [0, 2]] = decoded[:, [0, 2]] - decoded[:, [2]] / 2
    decoded[:, [1, 3]] = decoded[:, [1, 3]] - decoded[:, [3]] / 2
    return decoded


def bbox_clip(boxes: np.ndarray, img_shape: Tuple[int, int]) -> np.ndarray:
    """
    Clip bounding boxes to image boundaries.

    Args:
        boxes: Boxes (N, 4) as (x1, y1, x2, y2)
        img_shape: Image shape (height, width)

    Returns:
        Clipped boxes
    """
    h, w = img_shape
    boxes[:, [0, 2]] = np.clip(boxes[:, [0, 2]], 0, w - 1)
    boxes[:, [1, 3]] = np.clip(boxes[:, [1, 3]], 0, h - 1)
    return boxes


def compute_ap(recalls: np.ndarray, precisions: np.ndarray) -> float:
    """
    Compute Average Precision from recall-precision curve.

    Args:
        recalls: Recall values
        precisions: Precision values

    Returns:
        Average Precision
    """
    recalls = np.concatenate([[0], recalls, [1]])
    precisions = np.concatenate([[0], precisions, [0]])
    for i in range(len(precisions) - 2, -1, -1):
        precisions[i] = max(precisions[i], precisions[i + 1])
    indices = np.where(recalls[1:] != recalls[:-1])[0]
    ap = np.sum((recalls[indices + 1] - recalls[indices]) * precisions[indices + 1])
    return ap


def compute_detection_metrics(
    predictions: List[Tuple[np.ndarray, np.ndarray]],
    ground_truths: List[Tuple[np.ndarray, np.ndarray]],
    iou_threshold: float = 0.5,
) -> Dict[str, float]:
    """
    Compute detection metrics (AP, AR, etc.).

    Args:
        predictions: List of (boxes, scores) tuples
        ground_truths: List of (boxes, labels) tuples
        iou_threshold: IoU threshold for matching

    Returns:
        Dictionary of metrics
    """
    from utils.evaluation_utils import precision_recall_curve
    all_tp = []
    all_fp = []
    all_scores = []
    for (pred_boxes, pred_scores), (gt_boxes, gt_labels) in zip(predictions, ground_truths):
        matched_gt = set()
        for i, (box, score) in enumerate(zip(pred_boxes, pred_scores)):
            max_iou = 0
            max_gt_idx = -1
            for j, gt_box in enumerate(gt_boxes):
                if j in matched_gt:
                    continue
                iou = bbox_iou(box, gt_box)
                if iou > max_iou:
                    max_iou = iou
                    max_gt_idx = j
            if max_iou >= iou_threshold and max_gt_idx >= 0:
                all_tp.append(1)
                all_fp.append(0)
                matched_gt.add(max_gt_idx)
            else:
                all_tp.append(0)
                all_fp.append(1)
            all_scores.append(score)
    all_tp = np.array(all_tp)
    all_fp = np.array(all_fp)
    all_scores = np.array(all_scores)
    sorted_idx = np.argsort(all_scores)[::-1]
    tp_cumsum = np.cumsum(all_tp[sorted_idx])
    fp_cumsum = np.cumsum(all_fp[sorted_idx])
    recalls = tp_cumsum / (len(ground_truths) + 1e-10)
    precisions = tp_cumsum / (tp_cumsum + fp_cumsum + 1e-10)
    ap = compute_ap(recalls, precisions)
    return {"ap": ap, "precision": np.mean(precisions), "recall": np.mean(recalls)}


class BBox:
    """Bounding box class."""

    def __init__(self, x1: float, y1: float, x2: float, y2: float, label: int = None, score: float = None):
        self.x1 = x1
        self.y1 = y1
        self.x2 = x2
        self.y2 = y2
        self.label = label
        self.score = score

    @property
    def width(self) -> float:
        return self.x2 - self.x1

    @property
    def height(self) -> float:
        return self.y2 - self.y1

    @property
    def area(self) -> float:
        return self.width * self.height

    @property
    def center(self) -> Tuple[float, float]:
        return (self.x1 + self.x2) / 2, (self.y1 + self.y2) / 2

    def iou(self, other: "BBox") -> float:
        return bbox_iou(np.array([self.x1, self.y1, self.x2, self.y2]), np.array([other.x1, other.y1, other.x2, other.y2]))

    def __repr__(self) -> str:
        return f"BBox(x1={self.x1:.1f}, y1={self.y1:.1f}, x2={self.x2:.1f}, y2={self.y2:.1f}, label={self.label}, score={self.score:.2f})"
