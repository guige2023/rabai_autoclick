"""
Color picker automation module.

Provides screen color sampling, color matching, and
color-based automation triggers.

Author: Aito Auto Agent
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional, Callable


class ColorFormat(Enum):
    """Color format types."""
    HEX = auto()
    RGB = auto()
    RGBA = auto()
    HSL = auto()
    HSV = auto()


@dataclass
class Color:
    """Color representation with multiple format support."""
    r: int
    g: int
    b: int
    a: int = 255

    def to_hex(self, include_hash: bool = True) -> str:
        """Convert to hex format."""
        if self.a == 255:
            hex_str = f"{self.r:02x}{self.g:02x}{self.b:02x}"
        else:
            hex_str = f"{self.r:02x}{self.g:02x}{self.b:02x}{self.a:02x}"
        return f"#{hex_str}" if include_hash else hex_str

    def to_rgb(self) -> tuple[int, int, int]:
        """Convert to RGB tuple."""
        return (self.r, self.g, self.b)

    def to_rgba(self) -> tuple[int, int, int, int]:
        """Convert to RGBA tuple."""
        return (self.r, self.g, self.b, self.a)

    def to_hsl(self) -> tuple[float, float, float]:
        """Convert to HSL tuple."""
        r, g, b = self.r / 255, self.g / 255, self.b / 255

        max_c = max(r, g, b)
        min_c = min(r, g, b)
        l = (max_c + min_c) / 2

        if max_c == min_c:
            h = s = 0.0
        else:
            d = max_c - min_c
            s = d / (2 - max_c - min_c) if l > 0.5 else d / (max_c + min_c)

            if max_c == r:
                h = (g - b) / d + (6 if g < b else 0)
            elif max_c == g:
                h = (b - r) / d + 2
            else:
                h = (r - g) / d + 4

            h /= 6

        return (h * 360, s * 100, l * 100)

    def distance_to(self, other: Color) -> float:
        """Calculate Euclidean distance to another color."""
        dr = self.r - other.r
        dg = self.g - other.g
        db = self.b - other.b
        da = self.a - other.a
        return (dr * dr + dg * dg + db * db + da * da) ** 0.5

    def is_similar(
        self,
        other: Color,
        threshold: float = 30.0
    ) -> bool:
        """Check if color is similar within threshold."""
        return self.distance_to(other) <= threshold

    @classmethod
    def from_hex(cls, hex_str: str) -> Color:
        """Create Color from hex string."""
        hex_str = hex_str.lstrip("#")

        if len(hex_str) == 6:
            r = int(hex_str[0:2], 16)
            g = int(hex_str[2:4], 16)
            b = int(hex_str[4:6], 16)
            a = 255
        elif len(hex_str) == 8:
            r = int(hex_str[0:2], 16)
            g = int(hex_str[2:4], 16)
            b = int(hex_str[4:6], 16)
            a = int(hex_str[6:8], 16)
        else:
            raise ValueError(f"Invalid hex color: {hex_str}")

        return cls(r=r, g=g, b=b, a=a)

    @classmethod
    def from_rgb(cls, r: int, g: int, b: int, a: int = 255) -> Color:
        """Create Color from RGB values."""
        return cls(r=r, g=g, b=b, a=a)


class ColorPickerAutomator:
    """
    Color picker automation for screen color operations.

    Example:
        picker = ColorPickerAutomator(platform="macos")

        # Get color at position
        color = picker.get_color_at(100, 200)

        # Wait for color change
        result = picker.wait_for_color(100, 200, target_color=Color.from_hex("#FF0000"))

        # Find color on screen
        matches = picker.find_color_on_screen(Color.from_hex("#00FF00"), threshold=20)
    """

    def __init__(self, platform: str = "macos"):
        self._platform = platform

    def get_color_at(self, x: int, y: int) -> Optional[Color]:
        """
        Get color at screen position.

        Args:
            x: X coordinate
            y: Y coordinate

        Returns:
            Color at position or None
        """
        if self._platform == "macos":
            return self._get_color_macos(x, y)
        elif self._platform == "windows":
            return self._get_color_windows(x, y)
        else:
            return self._get_color_x11(x, y)

    def _get_color_macos(self, x: int, y: int) -> Optional[Color]:
        """Get color on macOS."""
        try:
            script = f'''
            tell application "System Events"
                set theDesktop to POSIX path of (path to desktop picture)
            end tell
            '''

            result = subprocess.run(
                ["screencapture", "-x", "-J", "selection", f"/tmp/color_picker_{x}_{y}.png"],
                capture_output=True,
                timeout=5
            )

            if result.returncode == 0:
                return self._analyze_pixel(f"/tmp/color_picker_{x}_{y}.png", x, y)

        except Exception:
            pass

        return None

    def _get_color_windows(self, x: int, y: int) -> Optional[Color]:
        """Get color on Windows."""
        try:
            script = f'''
            Add-Type -AssemblyName System.Drawing
            $bitmap = New-Object System.Drawing.Bitmap(1, 1)
            $graphics = [System.Drawing.Graphics]::FromImage($bitmap)
            $pixel = $graphics.GetPixel({x}, {y})
            $color = $pixel | Select-Object -Property R, G, B
            $color | ConvertTo-Json
            '''
            result = subprocess.run(
                ["powershell", "-Command", script],
                capture_output=True,
                text=True,
                timeout=5
            )

            if result.returncode == 0 and result.stdout.strip():
                import json
                data = json.loads(result.stdout)
                return Color(r=data["R"], g=data["G"], b=data["B"])

        except Exception:
            pass

        return None

    def _get_color_x11(self, x: int, y: int) -> Optional[Color]:
        """Get color on X11/Linux."""
        try:
            result = subprocess.run(
                ["xdotool", "mousemove", str(x), str(y)],
                capture_output=True,
                timeout=2
            )

            screencap_result = subprocess.run(
                ["import", "-window", "root", "-silent", f"/tmp/screen_{x}_{y}.png"],
                capture_output=True,
                timeout=5
            )

            if screencap_result.returncode == 0:
                return self._analyze_pixel(f"/tmp/screen_{x}_{y}.png", x, y)

        except Exception:
            pass

        return None

    def _analyze_pixel(self, image_path: str, x: int, y: int) -> Optional[Color]:
        """Analyze a single pixel in an image."""
        try:
            from PIL import Image
            img = Image.open(image_path)
            pixel = img.getpixel((x, y))

            if len(pixel) == 4:
                return Color(r=pixel[0], g=pixel[1], b=pixel[2], a=pixel[3])
            elif len(pixel) == 3:
                return Color(r=pixel[0], g=pixel[1], b=pixel[2])
            else:
                return Color(r=pixel[0], g=pixel[0], b=pixel[0])

        except ImportError:
            return None
        except Exception:
            return None

    def wait_for_color(
        self,
        x: int,
        y: int,
        target_color: Color,
        threshold: float = 30.0,
        timeout_ms: int = 30000,
        poll_interval_ms: int = 100
    ) -> tuple[bool, Optional[Color]]:
        """
        Wait for color at position to match target.

        Args:
            x: X coordinate
            y: Y coordinate
            target_color: Expected color
            threshold: Similarity threshold
            timeout_ms: Maximum wait time
            poll_interval_ms: Time between checks

        Returns:
            Tuple of (matched, actual_color)
        """
        import time
        start_time = time.time()
        timeout_sec = timeout_ms / 1000
        poll_sec = poll_interval_ms / 1000

        while time.time() - start_time < timeout_sec:
            color = self.get_color_at(x, y)
            if color and color.is_similar(target_color, threshold):
                return True, color
            time.sleep(poll_sec)

        return False, self.get_color_at(x, y)

    def wait_for_color_change(
        self,
        x: int,
        y: int,
        baseline_color: Optional[Color] = None,
        timeout_ms: int = 30000,
        poll_interval_ms: int = 100
    ) -> tuple[bool, Optional[Color], Optional[Color]]:
        """
        Wait for color at position to change from baseline.

        Args:
            x: X coordinate
            y: Y coordinate
            baseline_color: Starting color (if None, samples current)
            timeout_ms: Maximum wait time
            poll_interval_ms: Time between checks

        Returns:
            Tuple of (changed, baseline, new_color)
        """
        import time

        if baseline_color is None:
            baseline_color = self.get_color_at(x, y)
            if baseline_color is None:
                return False, None, None

        start_time = time.time()
        timeout_sec = timeout_ms / 1000
        poll_sec = poll_interval_ms / 1000

        while time.time() - start_time < timeout_sec:
            color = self.get_color_at(x, y)
            if color and not color.is_similar(baseline_color, threshold=5):
                return True, baseline_color, color
            time.sleep(poll_sec)

        return False, baseline_color, self.get_color_at(x, y)

    def find_color_on_screen(
        self,
        target_color: Color,
        region: Optional[tuple[int, int, int, int]] = None,
        threshold: float = 30.0
    ) -> list[tuple[int, int]]:
        """
        Find all occurrences of color on screen.

        Args:
            target_color: Color to find
            region: Optional (x, y, width, height) region
            threshold: Similarity threshold

        Returns:
            List of (x, y) positions where color was found
        """
        try:
            from PIL import Image
            import numpy as np

            screenshot_path = "/tmp/color_search_screen.png"

            if self._platform == "macos":
                subprocess.run(
                    ["screencapture", "-x", screenshot_path],
                    capture_output=True,
                    timeout=10
                )
            elif self._platform == "windows":
                script = f'''
                Add-Type -AssemblyName System.Drawing
                $bitmap = New-Object System.Drawing.Bitmap([System.Windows.Forms.Screen]::PrimaryScreen.Bounds.Width, [System.Windows.Forms.Screen]::PrimaryScreen.Bounds.Height)
                $graphics = [System.Drawing.Graphics]::FromImage($bitmap)
                $graphics.CopyFromScreen([System.Drawing.Point]::Empty, [System.Drawing.Point]::Empty, $bitmap.Size)
                $bitmap.Save("{screenshot_path}")
                '''
                subprocess.run(
                    ["powershell", "-Command", script],
                    capture_output=True,
                    timeout=10
                )
            else:
                subprocess.run(
                    ["import", "-window", "root", screenshot_path],
                    capture_output=True,
                    timeout=10
                )

            img = Image.open(screenshot_path)

            if region:
                x, y, w, h = region
                img = img.crop((x, y, x + w, y + h))

            img_array = np.array(img)
            target = np.array([target_color.r, target_color.g, target_color.b])

            matches = []

            height, width = img_array.shape[:2]
            step = 5

            for py in range(0, height, step):
                for px in range(0, width, step):
                    pixel = img_array[py, px, :3]
                    diff = np.linalg.norm(pixel.astype(float) - target)

                    if diff <= threshold:
                        matches.append((
                            px + (0 if not region else region[0]),
                            py + (0 if not region else region[1])
                        ))

            return matches

        except ImportError:
            return []
        except Exception:
            return []

    def create_color_watcher(
        self,
        x: int,
        y: int,
        on_change: Callable[[Color, Color], None],
        threshold: float = 10.0,
        poll_interval_ms: int = 100
    ):
        """Create a background color watcher."""
        import threading
        import time

        baseline = self.get_color_at(x, y)
        if baseline is None:
            return None

        running = True

        def watch():
            nonlocal baseline
            while running:
                color = self.get_color_at(x, y)
                if color and not color.is_similar(baseline, threshold):
                    old_baseline = baseline
                    baseline = color
                    on_change(old_baseline, color)
                time.sleep(poll_interval_ms / 1000)

        thread = threading.Thread(target=watch, daemon=True)
        thread.start()

        return lambda: nonlocal running; running = False


def create_color_picker(platform: str = "macos") -> ColorPickerAutomator:
    """Factory to create ColorPickerAutomator."""
    return ColorPickerAutomator(platform=platform)
