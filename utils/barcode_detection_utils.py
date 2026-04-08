"""Barcode and QR code detection utilities."""

from typing import Optional, Dict, Any, List
import numpy as np


def detect_barcode_in_image(
    image: np.ndarray,
    barcode_type: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """Detect barcode in image and return its bounds and type.
    
    Args:
        image: Input image as numpy array (BGR format).
        barcode_type: Specific type to detect (ean13, ean8, qr, etc.).
    
    Returns:
        Dictionary with keys: type, bounds, value, or None if not found.
    """
    try:
        from pyzbar.pyzbar import decode as pyzbar_decode
        import cv2
        barcodes = pyzbar_decode(image)
        if not barcodes:
            return None
        for bc in barcodes:
            if barcode_type and bc.type != barcode_type.upper():
                continue
            return {
                "type": bc.type,
                "bounds": {
                    "left": bc.rect.left,
                    "top": bc.rect.top,
                    "width": bc.rect.width,
                    "height": bc.rect.height,
                },
                "points": [(p.x, p.y) for p in bc.polygon],
                "value": bc.data.decode("utf-8"),
            }
        return None
    except ImportError:
        return _detect_qr_opencv(image)


def _detect_qr_opencv(image: np.ndarray) -> Optional[Dict[str, Any]]:
    """Fallback QR detection using OpenCV QRCodeDetector."""
    try:
        import cv2
        detector = cv2.QRCodeDetector()
        data, vertices, _ = detector.detectAndDecode(image)
        if not data:
            return None
        return {
            "type": "QR",
            "bounds": None,
            "points": [(int(p[0]), int(p[1])) for p in vertices[0]] if vertices.size else [],
            "value": data,
        }
    except Exception:
        return None


def detect_qr_code(image: np.ndarray) -> Optional[str]:
    """Detect and decode QR code in image.
    
    Args:
        image: Input image as numpy array.
    
    Returns:
        Decoded QR code string or None.
    """
    result = detect_barcode_in_image(image, barcode_type="qr")
    return result["value"] if result else None


def detect_all_barcodes(image: np.ndarray) -> List[Dict[str, Any]]:
    """Detect all barcodes in image.
    
    Args:
        image: Input image as numpy array.
    
    Returns:
        List of barcode detections.
    """
    try:
        from pyzbar.pyzbar import decode as pyzbar_decode
        barcodes = pyzbar_decode(image)
        results = []
        for bc in barcodes:
            results.append({
                "type": bc.type,
                "bounds": {
                    "left": bc.rect.left,
                    "top": bc.rect.top,
                    "width": bc.rect.width,
                    "height": bc.rect.height,
                },
                "points": [(p.x, p.y) for p in bc.polygon],
                "value": bc.data.decode("utf-8"),
            })
        return results
    except ImportError:
        qr = detect_qr_code(image)
        return [{"type": "QR", "bounds": None, "points": [], "value": qr}] if qr else []


def generate_qr_code(data: str, size: int = 300) -> np.ndarray:
    """Generate QR code image from string data.
    
    Args:
        data: String data to encode in QR.
        size: Output image size in pixels.
    
    Returns:
        QR code image as numpy array.
    """
    try:
        import qrcode
        qr = qrcode.QRCode(version=1, box_size=10, border=4)
        qr.add_data(data)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        import numpy as np
        from PIL import Image
        return np.array(img.resize((size, size)))
    except ImportError:
        raise ImportError("qrcode library required: pip install qrcode pillow")


def extract_barcode_region(
    image: np.ndarray,
    margin: int = 10
) -> Optional[np.ndarray]:
    """Extract cropped region containing barcode.
    
    Args:
        image: Input image.
        margin: Extra margin around barcode in pixels.
    
    Returns:
        Cropped image region or None if no barcode found.
    """
    detection = detect_barcode_in_image(image)
    if not detection or not detection.get("bounds"):
        return None
    b = detection["bounds"]
    h, w = image.shape[:2]
    x1 = max(0, b["left"] - margin)
    y1 = max(0, b["top"] - margin)
    x2 = min(w, b["left"] + b["width"] + margin)
    y2 = min(h, b["top"] + b["height"] + margin)
    return image[y1:y2, x1:x2]
