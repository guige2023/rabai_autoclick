"""Visual diff and image comparison action module for RabAI AutoClick.

Provides visual regression testing capabilities:
- VisualDiffAction: Compare screenshots for differences
- ImageComparatorAction: Detailed pixel-by-pixel comparison
- VisualRegressionTracker: Track visual changes over time
- ScreenshotMatcherAction: Find matching regions in screenshots
"""

from typing import Any, Dict, List, Optional, Tuple
import base64
import hashlib
import io
import logging
import math
import os

import sys
import os as _os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)

# Try to import PIL for image processing
try:
    from PIL import Image, ImageChops, ImageDraw, ImageStat
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False


class VisualDiffAction(BaseAction):
    """Compare two screenshots and detect visual differences."""
    
    action_type = "visual_diff"
    display_name = "视觉差异检测"
    description = "比较两张图片的视觉差异"
    
    def __init__(self) -> None:
        super().__init__()
        self._diff_history: List[Dict[str, Any]] = []
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Compare two images and return diff metrics.
        
        Args:
            params: {
                "image1": First image path or base64 (str, required),
                "image2": Second image path or base64 (str, required),
                "threshold": Pixel difference threshold 0-255 (float, default 10),
                "method": Comparison method - "pixel", "histogram", " perceptual" (str),
                "save_diff": Save diff image to disk (bool, default False),
                "diff_path": Path to save diff image (str)
            }
        """
        if not PIL_AVAILABLE:
            return ActionResult(
                success=False,
                message="PIL/Pillow not available. Install with: pip install Pillow"
            )
        
        try:
            image1_data = self._load_image(params.get("image1", ""))
            image2_data = self._load_image(params.get("image2", ""))
            threshold = params.get("threshold", 10)
            method = params.get("method", "pixel")
            save_diff = params.get("save_diff", False)
            diff_path = params.get("diff_path", "/tmp/visual_diff.png")
            
            if not image1_data or not image2_data:
                return ActionResult(success=False, message="Both image1 and image2 are required")
            
            img1 = Image.open(io.BytesIO(image1_data))
            img2 = Image.open(io.BytesIO(image2_data))
            
            # Convert to same mode for comparison
            if img1.mode != img2.mode:
                img2 = img2.convert(img1.mode)
            
            # Resize if dimensions differ
            if img1.size != img2.size:
                img2 = img2.resize(img1.size, Image.Resampling.LANCZOS)
            
            if method == "pixel":
                result = self._pixel_diff(img1, img2, threshold)
            elif method == "histogram":
                result = self._histogram_diff(img1, img2)
            elif method == "perceptual":
                result = self._perceptual_diff(img1, img2, threshold)
            else:
                return ActionResult(success=False, message=f"Unknown method: {method}")
            
            # Generate diff image
            if save_diff or params.get("include_diff_image", False):
                diff_img = self._generate_diff_image(img1, img2, threshold)
                if save_diff:
                    diff_img.save(diff_path)
                    result["diff_image_path"] = diff_path
                
                if params.get("include_diff_image", False):
                    buffered = io.BytesIO()
                    diff_img.save(buffered, format="PNG")
                    result["diff_image_base64"] = base64.b64encode(buffered.getvalue()).decode()
            
            # Record history
            self._diff_history.append({
                "timestamp": self._get_timestamp(),
                "method": method,
                "threshold": threshold,
                "images_differ": result.get("images_differ", False),
                "diff_percentage": result.get("diff_percentage", 0)
            })
            
            message = "Images are identical" if not result.get("images_differ") else f"Images differ by {result.get('diff_percentage', 0):.2f}%"
            
            return ActionResult(
                success=True,
                message=message,
                data=result
            )
        
        except Exception as e:
            logger.error(f"Visual diff failed: {e}")
            return ActionResult(success=False, message=f"Visual diff error: {str(e)}")
    
    def _load_image(self, image_data: Any) -> Optional[bytes]:
        """Load image from path or base64."""
        if not image_data:
            return None
        
        if isinstance(image_data, str):
            if os.path.exists(image_data):
                with open(image_data, "rb") as f:
                    return f.read()
            elif image_data.startswith("data:image"):
                return base64.b64decode(image_data.split(",")[1])
            elif len(image_data) > 100:
                try:
                    return base64.b64decode(image_data)
                except Exception:
                    pass
        return None
    
    def _pixel_diff(self, img1: Image.Image, img2: Image.Image, threshold: float) -> Dict[str, Any]:
        """Pixel-by-pixel difference analysis."""
        pixels1 = list(img1.getdata())
        pixels2 = list(img2.getdata())
        
        total_pixels = len(pixels1)
        diff_pixels = 0
        max_diff = 0
        
        diff_details: List[Tuple[int, int, float]] = []  # (x, y, diff)
        
        width, height = img1.size
        
        for i, (p1, p2) in enumerate(zip(pixels1, pixels2)):
            if isinstance(p1, int):
                p1 = (p1,)
            if isinstance(p2, int):
                p2 = (p2,)
            
            channel_diffs = []
            for c1, c2 in zip(p1, p2):
                channel_diffs.append(abs(int(c1) - int(c2)))
            
            pixel_diff = sum(channel_diffs) / len(channel_diffs) if channel_diffs else 0
            max_diff = max(max_diff, pixel_diff)
            
            if pixel_diff > threshold:
                diff_pixels += 1
                x = i % width
                y = i // width
                if len(diff_details) < 1000:  # Limit stored details
                    diff_details.append((x, y, pixel_diff))
        
        diff_percentage = (diff_pixels / total_pixels) * 100 if total_pixels > 0 else 0
        
        return {
            "images_differ": diff_pixels > 0,
            "diff_percentage": round(diff_percentage, 4),
            "diff_pixels": diff_pixels,
            "total_pixels": total_pixels,
            "max_pixel_diff": round(max_diff, 2),
            "method": "pixel",
            "diff_threshold": threshold,
            "diff_details": diff_details[:100]  # Top 100 diff locations
        }
    
    def _histogram_diff(self, img1: Image.Image, img2: Image.Image) -> Dict[str, Any]:
        """Compare images using histogram analysis."""
        h1 = img1.histogram()
        h2 = img2.histogram()
        
        # Ensure same length
        if len(h1) != len(h2):
            # Convert to same mode
            img1 = img1.convert("RGB")
            img2 = img2.convert("RGB")
            h1 = img1.histogram()
            h2 = img2.histogram()
        
        # Calculate RMS difference
        sum_sq_diff = sum((c1 - c2) ** 2 for c1, c2 in zip(h1, h2))
        rms = math.sqrt(sum_sq_diff / len(h1))
        
        # Normalize to 0-100
        diff_score = min(100, rms / 2.55)
        
        return {
            "images_differ": diff_score > 5,
            "diff_percentage": round(diff_score, 4),
            "histogram_rms": round(rms, 4),
            "method": "histogram"
        }
    
    def _perceptual_diff(
        self,
        img1: Image.Image,
        img2: Image.Image,
        threshold: float
    ) -> Dict[str, Any]:
        """Perceptual difference using structural comparison."""
        # Compute structural similarity approximation
        # Convert to grayscale for perceptual comparison
        gray1 = img1.convert("L")
        gray2 = img2.convert("L")
        
        # Get statistics
        stat1 = ImageStat.Stat(gray1)
        stat2 = ImageStat.Stat(gray2)
        
        mean1 = stat1.mean[0]
        mean2 = stat2.mean[0]
        std1 = math.sqrt(stat1.var[0])
        std2 = math.sqrt(stat2.var[0])
        
        # Structural difference
        struct_diff = abs(mean1 - mean2) / 255 + abs(std1 - std2) / 255
        similarity = max(0, 1 - struct_diff * 2)
        diff_percentage = (1 - similarity) * 100
        
        return {
            "images_differ": diff_percentage > threshold,
            "diff_percentage": round(diff_percentage, 4),
            "similarity_score": round(similarity, 4),
            "mean1": round(mean1, 2),
            "mean2": round(mean2, 2),
            "std1": round(std1, 2),
            "std2": round(std2, 2),
            "method": "perceptual"
        }
    
    def _generate_diff_image(
        self,
        img1: Image.Image,
        img2: Image.Image,
        threshold: float
    ) -> Image.Image:
        """Generate visual diff image with highlighting."""
        diff = ImageChops.difference(img1, img2)
        
        # Create output with diff overlay
        result = img1.copy().convert("RGB")
        diff_array = list(diff.getdata())
        
        overlay = Image.new("RGB", img1.size, (0, 0, 0))
        draw = ImageDraw.Draw(overlay)
        
        width, height = img1.size
        diff_count = 0
        
        for i, pixel in enumerate(diff_array):
            if isinstance(pixel, int):
                pixel = (pixel,)
            
            avg_diff = sum(pixel) / len(pixel) if pixel else 0
            
            if avg_diff > threshold:
                x = i % width
                y = i // width
                # Highlight in red
                draw.point((x, y), fill=(255, 0, 0))
                diff_count += 1
        
        # Blend overlay
        result = ImageChops.add(result, overlay)
        
        return result
    
    def _get_timestamp(self) -> str:
        """Get current timestamp."""
        from datetime import datetime
        return datetime.now().isoformat()
    
    def get_diff_history(self) -> List[Dict[str, Any]]:
        """Get history of diff comparisons."""
        return self._diff_history.copy()


class ImageComparatorAction(BaseAction):
    """Detailed image comparison with multiple metrics."""
    
    action_type = "image_comparator"
    display_name = "图片比较器"
    description = "多维度图片比较分析"
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Compare two images with multiple metrics.
        
        Args:
            params: {
                "image1": First image (str path or base64),
                "image2": Second image (str path or base64),
                "metrics": List of metrics to compute (list)
            }
        """
        if not PIL_AVAILABLE:
            return ActionResult(success=False, message="PIL not available")
        
        try:
            image1_data = self._load_image(params.get("image1", ""))
            image2_data = self._load_image(params.get("image2", ""))
            metrics = params.get("metrics", ["size", "mode", "format", "histogram", "aspect_ratio"])
            
            if not image1_data or not image2_data:
                return ActionResult(success=False, message="Both images required")
            
            img1 = Image.open(io.BytesIO(image1_data))
            img2 = Image.open(io.BytesIO(image2_data))
            
            results: Dict[str, Any] = {}
            
            if "size" in metrics:
                results["size"] = {
                    "image1": img1.size,
                    "image2": img2.size,
                    "match": img1.size == img2.size
                }
            
            if "mode" in metrics:
                results["mode"] = {
                    "image1": img1.mode,
                    "image2": img2.mode,
                    "match": img1.mode == img2.mode
                }
            
            if "aspect_ratio" in metrics:
                w1, h1 = img1.size
                w2, h2 = img2.size
                results["aspect_ratio"] = {
                    "image1": round(w1 / h1, 4),
                    "image2": round(w2 / h2, 4),
                    "match": abs(w1 / h1 - w2 / h2) < 0.01
                }
            
            if "histogram" in metrics:
                h1 = img1.histogram()
                h2 = img2.histogram()
                min_len = min(len(h1), len(h2))
                hist_diff = sum((h1[i] - h2[i]) ** 2 for i in range(min_len)) / min_len
                results["histogram"] = {
                    "difference": round(hist_diff, 4),
                    "similar": hist_diff < 1000
                }
            
            if "file_size" in metrics:
                results["file_size"] = {
                    "image1_bytes": len(image1_data),
                    "image2_bytes": len(image2_data),
                    "difference": abs(len(image1_data) - len(image2_data))
                }
            
            return ActionResult(
                success=True,
                message="Comparison complete",
                data=results
            )
        
        except Exception as e:
            return ActionResult(success=False, message=f"Comparison error: {str(e)}")
    
    def _load_image(self, image_data: Any) -> Optional[bytes]:
        """Load image from path or base64."""
        if not image_data:
            return None
        if isinstance(image_data, str):
            if os.path.exists(image_data):
                with open(image_data, "rb") as f:
                    return f.read()
            elif image_data.startswith("data:image"):
                return base64.b64decode(image_data.split(",")[1])
        return None


class VisualRegressionTracker(BaseAction):
    """Track visual changes across multiple test runs."""
    
    action_type = "visual_regression_tracker"
    display_name = "视觉回归追踪"
    description = "追踪多次测试的视觉变化"
    
    def __init__(self) -> None:
        super().__init__()
        self._baseline: Dict[str, Dict[str, Any]] = {}
        self._snapshots: List[Dict[str, Any]] = []
        self._regression_threshold = 1.0  # percent
    
    def set_baseline(self, name: str, image_data: Any, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Set a baseline image for comparison."""
        from datetime import datetime
        self._baseline[name] = {
            "image": image_data,
            "timestamp": datetime.now().isoformat(),
            "metadata": metadata or {}
        }
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Track visual regression against baseline.
        
        Args:
            params: {
                "name": Baseline name (str, required),
                "image": Current image to compare (str path or base64),
                "threshold": Regression threshold in percent (float, default 1.0),
                "update_baseline": Update baseline if different (bool, default False)
            }
        """
        if not PIL_AVAILABLE:
            return ActionResult(success=False, message="PIL not available")
        
        try:
            name = params.get("name", "")
            image_data = self._load_image(params.get("image", ""))
            threshold = params.get("threshold", self._regression_threshold)
            update_baseline = params.get("update_baseline", False)
            
            if not name or not image_data:
                return ActionResult(success=False, message="name and image are required")
            
            if name not in self._baseline:
                # First time - set as baseline
                self.set_baseline(name, image_data, params.get("metadata"))
                return ActionResult(
                    success=True,
                    message=f"Baseline '{name}' set",
                    data={"is_baseline": True, "is_regression": False}
                )
            
            # Compare with baseline
            visual_diff = VisualDiffAction()
            diff_result = visual_diff.execute(context, {
                "image1": self._baseline[name]["image"],
                "image2": image_data,
                "method": "pixel",
                "threshold": 10
            })
            
            if not diff_result.success:
                return diff_result
            
            diff_data = diff_result.data
            diff_percentage = diff_data.get("diff_percentage", 0)
            is_regression = diff_percentage > threshold
            
            from datetime import datetime
            snapshot = {
                "name": name,
                "timestamp": datetime.now().isoformat(),
                "diff_percentage": diff_percentage,
                "is_regression": is_regression,
                "baseline_timestamp": self._baseline[name]["timestamp"]
            }
            self._snapshots.append(snapshot)
            
            if update_baseline and not is_regression:
                self.set_baseline(name, image_data, params.get("metadata"))
                snapshot["baseline_updated"] = True
            
            return ActionResult(
                success=True,
                message=f"Regression check: {'REGRESSION' if is_regression else 'OK'} ({diff_percentage:.2f}%)",
                data={
                    "is_regression": is_regression,
                    "diff_percentage": diff_percentage,
                    "threshold": threshold,
                    "snapshots_count": len(self._snapshots)
                }
            )
        
        except Exception as e:
            return ActionResult(success=False, message=f"Tracker error: {str(e)}")
    
    def _load_image(self, image_data: Any) -> Optional[bytes]:
        """Load image from path or base64."""
        if not image_data:
            return None
        if isinstance(image_data, str):
            if os.path.exists(image_data):
                with open(image_data, "rb") as f:
                    return f.read()
            elif image_data.startswith("data:image"):
                return base64.b64decode(image_data.split(",")[1])
        return None
    
    def get_snapshots(self, name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get snapshots, optionally filtered by name."""
        if name:
            return [s for s in self._snapshots if s.get("name") == name]
        return self._snapshots.copy()
