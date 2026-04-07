"""Image utilities for RabAI AutoClick.

Provides:
- Image loading and saving
- Image processing
- Screenshot utilities
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Optional, Tuple, Union


@dataclass
class ImageSize:
    """Represents image dimensions."""
    width: int
    height: int

    @property
    def aspect_ratio(self) -> float:
        """Get aspect ratio."""
        if self.height == 0:
            return 0.0
        return self.width / self.height

    @property
    def area(self) -> int:
        """Get total pixels."""
        return self.width * self.height


def load_image(path: Union[str, Path]) -> Optional[Any]:
    """Load image from file.

    Args:
        path: Path to image file.

    Returns:
        Image as numpy array or None on error.
    """
    try:
        import cv2
        img = cv2.imread(str(path))
        return img
    except Exception:
        return None


def save_image(path: Union[str, Path], image: Any) -> bool:
    """Save image to file.

    Args:
        path: Output path.
        image: Image data.

    Returns:
        True on success.
    """
    try:
        import cv2
        cv2.imwrite(str(path), image)
        return True
    except Exception:
        return False


def get_image_size(image: Any) -> Optional[ImageSize]:
    """Get image dimensions.

    Args:
        image: Image data.

    Returns:
        ImageSize or None.
    """
    try:
        import cv2
        h, w = image.shape[:2]
        return ImageSize(width=w, height=h)
    except Exception:
        return None


def resize_image(
    image: Any,
    width: Optional[int] = None,
    height: Optional[int] = None,
    scale: Optional[float] = None,
) -> Optional[Any]:
    """Resize image.

    Args:
        image: Image data.
        width: Target width.
        height: Target height.
        scale: Scale factor.

    Returns:
        Resized image or None.
    """
    try:
        import cv2
        import numpy as np

        if scale is not None:
            h, w = image.shape[:2]
            new_w = int(w * scale)
            new_h = int(h * scale)
            return cv2.resize(image, (new_w, new_h))

        if width is None and height is None:
            return image

        if width is None:
            h, w = image.shape[:2]
            aspect = height / h
            width = int(w * aspect)
        elif height is None:
            h, w = image.shape[:2]
            aspect = width / w
            height = int(h * aspect)

        return cv2.resize(image, (width, height))
    except Exception:
        return None


def crop_image(
    image: Any,
    x: int,
    y: int,
    width: int,
    height: int,
) -> Optional[Any]:
    """Crop image region.

    Args:
        image: Image data.
        x: Left coordinate.
        y: Top coordinate.
        width: Crop width.
        height: Crop height.

    Returns:
        Cropped image or None.
    """
    try:
        return image[y:y+height, x:x+width]
    except Exception:
        return None


def grayscale(image: Any) -> Optional[Any]:
    """Convert image to grayscale.

    Args:
        image: Image data.

    Returns:
        Grayscale image or None.
    """
    try:
        import cv2
        return cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    except Exception:
        return None


def blur(image: Any, kernel_size: int = 5) -> Optional[Any]:
    """Apply Gaussian blur.

    Args:
        image: Image data.
        kernel_size: Blur kernel size.

    Returns:
        Blurred image or None.
    """
    try:
        import cv2
        return cv2.GaussianBlur(image, (kernel_size, kernel_size), 0)
    except Exception:
        return None


def threshold(image: Any, threshold_value: int = 127) -> Optional[Any]:
    """Apply threshold.

    Args:
        image: Image data.
        threshold_value: Threshold value.

    Returns:
        Thresholded image or None.
    """
    try:
        import cv2
        _, result = cv2.threshold(image, threshold_value, 255, cv2.THRESH_BINARY)
        return result
    except Exception:
        return None


def find_edges(image: Any, low: int = 50, high: int = 150) -> Optional[Any]:
    """Find edges using Canny.

    Args:
        image: Image data.
        low: Low threshold.
        high: High threshold.

    Returns:
        Edge image or None.
    """
    try:
        import cv2
        return cv2.Canny(image, low, high)
    except Exception:
        return None


def match_template(
    image: Any,
    template: Any,
    threshold: float = 0.8,
) -> List[Tuple[int, int]]:
    """Find template matches in image.

    Args:
        image: Image to search.
        template: Template to find.
        threshold: Match threshold.

    Returns:
        List of (x, y) match coordinates.
    """
    try:
        import cv2
        result = cv2.matchTemplate(image, template, cv2.TM_CCOEFF_NORMED)
        locations = []
        h, w = template.shape[:2]

        for y in range(result.shape[0]):
            for x in range(result.shape[1]):
                if result[y, x] >= threshold:
                    locations.append((x, y))

        return locations
    except Exception:
        return []


def screenshot(region: Optional[Tuple[int, int, int, int]] = None) -> Optional[Any]:
    """Take screenshot.

    Args:
        region: Optional (x, y, width, height) region.

    Returns:
        Screenshot as numpy array or None.
    """
    try:
        import pyautogui
        import numpy as np

        if region:
            x, y, w, h = region
            img = pyautogui.screenshot(region=(x, y, w, h))
        else:
            img = pyautogui.screenshot()

        return np.array(img)
    except Exception:
        return None


def save_screenshot(path: Union[str, Path], region: Optional[Tuple[int, int, int, int]] = None) -> bool:
    """Save screenshot.

    Args:
        path: Output path.
        region: Optional region.

    Returns:
        True on success.
    """
    img = screenshot(region)
    if img is None:
        return False
    return save_image(path, img)


def compare_images(image1: Any, image2: Any) -> float:
    """Compare two images and return similarity.

    Args:
        image1: First image.
        image2: Second image.

    Returns:
        Similarity score 0.0 to 1.0.
    """
    try:
        import cv2
        import numpy as np

        if image1.shape != image2.shape:
            image2 = cv2.resize(image2, (image1.shape[1], image1.shape[0]))

        diff = cv2.absdiff(image1, image2)
        score = 1.0 - (np.mean(diff) / 255.0)
        return max(0.0, min(1.0, score))
    except Exception:
        return 0.0


def find_template_on_screen(
    template_path: Union[str, Path],
    region: Optional[Tuple[int, int, int, int]] = None,
    threshold: float = 0.8,
) -> Optional[Tuple[int, int]]:
    """Find template on screen.

    Args:
        template_path: Path to template image.
        region: Optional screen region.
        threshold: Match threshold.

    Returns:
        (x, y) center of match or None.
    """
    try:
        import cv2

        template = load_image(template_path)
        if template is None:
            return None

        screen = screenshot(region)
        if screen is None:
            return None

        # Convert to grayscale if needed
        if len(template.shape) == 3:
            template = grayscale(template)
        if len(screen.shape) == 3:
            screen = grayscale(screen)

        if template is None or screen is None:
            return None

        result = cv2.matchTemplate(screen, template, cv2.TM_CCOEFF_NORMED)
        _, max_val, _, max_loc = cv2.minMaxLoc(result)

        if max_val >= threshold:
            h, w = template.shape[:2]
            center_x = max_loc[0] + w // 2
            center_y = max_loc[1] + h // 2

            # Adjust for region offset
            if region:
                center_x += region[0]
                center_y += region[1]

            return (center_x, center_y)

        return None
    except Exception:
        return None