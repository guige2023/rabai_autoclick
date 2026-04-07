"""Image processing utilities for RabAI AutoClick.

Provides:
- Image loading and saving
- Image matching
- Screenshot utilities
"""

from dataclasses import dataclass
from typing import List, Optional, Tuple


@dataclass
class ImageMatch:
    """Result of image matching."""
    x: int
    y: int
    width: int
    height: int
    confidence: float


class ImageLoader:
    """Load and validate images."""

    @staticmethod
    def load(path: str) -> Optional["Image"]:
        """Load image from file.

        Args:
            path: Path to image file.

        Returns:
            Image object or None on failure.
        """
        try:
            from PIL import Image
            pil_image = Image.open(path)
            return Image(
                width=pil_image.width,
                height=pil_image.height,
                mode=pil_image.mode,
                data=pil_image.tobytes(),
            )
        except Exception:
            return None

    @staticmethod
    def from_pil(pil_image) -> "Image":
        """Create Image from PIL Image.

        Args:
            pil_image: PIL Image object.

        Returns:
            Image object.
        """
        return Image(
            width=pil_image.width,
            height=pil_image.height,
            mode=pil_image.mode,
            data=pil_image.tobytes(),
        )


class Image:
    """Simple image representation."""

    def __init__(
        self,
        width: int,
        height: int,
        mode: str = "RGB",
        data: Optional[bytes] = None,
    ) -> None:
        """Initialize image.

        Args:
            width: Image width.
            height: Image height.
            mode: Color mode.
            data: Raw image data.
        """
        self.width = width
        self.height = height
        self.mode = mode
        self.data = data

    def to_pil(self):
        """Convert to PIL Image.

        Returns:
            PIL Image.
        """
        try:
            from PIL import Image
            return Image.frombytes(self.mode, (self.width, self.height), self.data)
        except Exception:
            return None

    def save(self, path: str) -> bool:
        """Save image to file.

        Args:
            path: Output path.

        Returns:
            True if successful.
        """
        try:
            pil = self.to_pil()
            if pil:
                pil.save(path)
                return True
            return False
        except Exception:
            return False

    def crop(self, x: int, y: int, width: int, height: int) -> "Image":
        """Crop image.

        Args:
            x: Left coordinate.
            y: Top coordinate.
            width: Crop width.
            height: Crop height.

        Returns:
            Cropped image.
        """
        pil = self.to_pil()
        if pil:
            cropped = pil.crop((x, y, x + width, y + height))
            return ImageLoader.from_pil(cropped)
        return Image(width=0, height=0)

    def resize(self, width: int, height: int) -> "Image":
        """Resize image.

        Args:
            width: New width.
            height: New height.

        Returns:
            Resized image.
        """
        pil = self.to_pil()
        if pil:
            resized = pil.resize((width, height))
            return ImageLoader.from_pil(resized)
        return Image(width=0, height=0)


class ImageMatcher:
    """Find images on screen or in other images."""

    def __init__(self, threshold: float = 0.8) -> None:
        """Initialize image matcher.

        Args:
            threshold: Match confidence threshold (0-1).
        """
        self.threshold = threshold

    def find_on_screen(
        self,
        template: Image,
        region: Optional[Tuple[int, int, int, int]] = None,
    ) -> Optional[ImageMatch]:
        """Find template on screen.

        Args:
            template: Image to find.
            region: Search region (x, y, width, height).

        Returns:
            Match result or None.
        """
        try:
            import numpy as np
            import cv2
            from PIL import ImageGrab, Image

            # Capture screen
            if region:
                x, y, w, h = region
                screenshot = ImageGrab.grab(bbox=(x, y, x + w, y + h))
            else:
                screenshot = ImageGrab.grab()

            # Convert to OpenCV format
            screen_np = np.array(screenshot)
            screen_gray = cv2.cvtColor(screen_np, cv2.COLOR_BGR2GRAY)

            template_pil = template.to_pil()
            if not template_pil:
                return None

            template_np = np.array(template_pil)
            if len(template_np.shape) == 3:
                template_gray = cv2.cvtColor(template_np, cv2.COLOR_BGR2GRAY)
            else:
                template_gray = template_np

            # Template matching
            result = cv2.matchTemplate(
                screen_gray, template_gray, cv2.TM_CCOEFF_NORMED
            )
            _, max_val, _, max_loc = cv2.minMaxLoc(result)

            if max_val >= self.threshold:
                return ImageMatch(
                    x=max_loc[0],
                    y=max_loc[1],
                    width=template.width,
                    height=template.height,
                    confidence=max_val,
                )
            return None
        except Exception:
            return None

    def find_all_on_screen(
        self,
        template: Image,
        region: Optional[Tuple[int, int, int, int]] = None,
        max_results: int = 10,
    ) -> List[ImageMatch]:
        """Find all matches on screen.

        Args:
            template: Image to find.
            region: Search region.
            max_results: Maximum matches to return.

        Returns:
            List of matches.
        """
        try:
            import numpy as np
            import cv2
            from PIL import ImageGrab

            if region:
                x, y, w, h = region
                screenshot = ImageGrab.grab(bbox=(x, y, x + w, y + h))
            else:
                screenshot = ImageGrab.grab()

            screen_np = np.array(screenshot)
            screen_gray = cv2.cvtColor(screen_np, cv2.COLOR_BGR2GRAY)

            template_pil = template.to_pil()
            if not template_pil:
                return []

            template_np = np.array(template_pil)
            if len(template_np.shape) == 3:
                template_gray = cv2.cvtColor(template_np, cv2.COLOR_BGR2GRAY)
            else:
                template_gray = template_np

            result = cv2.matchTemplate(
                screen_gray, template_gray, cv2.TM_CCOEFF_NORMED
            )

            locations = np.where(result >= self.threshold)
            matches = []

            for pt in zip(*locations[::-1]):
                matches.append(ImageMatch(
                    x=pt[0],
                    y=pt[1],
                    width=template.width,
                    height=template.height,
                    confidence=float(result[pt[1], pt[0]]),
                ))

            return matches[:max_results]
        except Exception:
            return []


class Screenshot:
    """Screenshot capture utilities."""

    @staticmethod
    def capture(
        region: Optional[Tuple[int, int, int, int]] = None,
    ) -> Optional[Image]:
        """Capture screenshot.

        Args:
            region: Optional region (x, y, width, height).

        Returns:
            Image or None on failure.
        """
        try:
            from PIL import ImageGrab
            if region:
                x, y, w, h = region
                pil = ImageGrab.grab(bbox=(x, y, x + w, y + h))
            else:
                pil = ImageGrab.grab()
            return ImageLoader.from_pil(pil)
        except Exception:
            return None

    @staticmethod
    def save(
        path: str,
        region: Optional[Tuple[int, int, int, int]] = None,
    ) -> bool:
        """Capture and save screenshot.

        Args:
            path: Output path.
            region: Optional region.

        Returns:
            True if successful.
        """
        img = Screenshot.capture(region)
        if img:
            return img.save(path)
        return False


class ImageComparator:
    """Compare images for similarity."""

    @staticmethod
    def compare(img1: Image, img2: Image) -> float:
        """Compare two images.

        Args:
            img1: First image.
            img2: Second image.

        Returns:
            Similarity score 0-1.
        """
        try:
            import numpy as np
            import cv2

            pil1 = img1.to_pil()
            pil2 = img2.to_pil()

            if not pil1 or not pil2:
                return 0.0

            arr1 = np.array(pil1.resize((100, 100)))
            arr2 = np.array(pil2.resize((100, 100)))

            if len(arr1.shape) == 3:
                arr1 = cv2.cvtColor(arr1, cv2.COLOR_BGR2GRAY)
            if len(arr2.shape) == 3:
                arr2 = cv2.cvtColor(arr2, cv2.COLOR_BGR2GRAY)

            score = np.mean(arr1 == arr2)
            return float(score)
        except Exception:
            return 0.0

    @staticmethod
    def is_identical(img1: Image, img2: Image, threshold: float = 0.95) -> bool:
        """Check if images are identical.

        Args:
            img1: First image.
            img2: Second image.
            threshold: Similarity threshold.

        Returns:
            True if identical.
        """
        return ImageComparator.compare(img1, img2) >= threshold
