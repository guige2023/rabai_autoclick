"""OCR action module for RabAI AutoClick.

Provides OCR (Optical Character Recognition) capabilities
for text extraction from images and screenshots.
"""

import sys
import os
import tempfile
from typing import Any, Dict, List, Optional, Union, Callable
import subprocess
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class OCREngine(Enum):
    """OCR engine types."""
    TESSERACT = "tesseract"
    PYTESSERACT = "pytesseract"
    EASYOCR = "easyocr"


class OCRResult:
    """Represents an OCR result."""
    
    def __init__(
        self,
        text: str,
        confidence: float = 0.0,
        boxes: Optional[List[Dict[str, Any]]] = None
    ):
        self.text = text
        self.confidence = confidence
        self.boxes = boxes or []
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'text': self.text,
            'confidence': self.confidence,
            'boxes': self.boxes
        }


class OCRAction(BaseAction):
    """OCR text extraction from images.
    
    Supports Tesseract OCR with multiple languages,
    region-based extraction, and confidence scoring.
    """
    action_type = "ocr"
    display_name = "OCR识别"
    description = "图像文字识别"
    DEFAULT_LANG = 'eng'
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute OCR operations.
        
        Args:
            context: Execution context.
            params: Dict with keys: image_path, image_data, lang,
                   region, engine.
        
        Returns:
            ActionResult with OCR result.
        """
        image_path = params.get('image_path')
        image_data = params.get('image_data')
        lang = params.get('lang', self.DEFAULT_LANG)
        region = params.get('region')
        
        if not image_path and not image_data:
            return ActionResult(
                success=False,
                message="image_path or image_data is required"
            )
        
        try:
            result = self._perform_ocr(
                image_path=image_path,
                image_data=image_data,
                lang=lang,
                region=region,
                params=params
            )
            
            return ActionResult(
                success=True,
                message=f"OCR completed: {len(result.text)} chars",
                data=result.to_dict()
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"OCR failed: {e}"
            )
    
    def _perform_ocr(
        self,
        image_path: Optional[str],
        image_data: Optional[str],
        lang: str,
        region: Optional[Dict[str, Any]],
        params: Dict[str, Any]
    ) -> OCRResult:
        """Perform OCR using available engine."""
        try:
            import pytesseract
            from PIL import Image
            
            if image_path:
                image = Image.open(image_path)
            elif image_data:
                import base64
                if isinstance(image_data, str):
                    image_data_bytes = base64.b64decode(image_data)
                else:
                    image_data_bytes = image_data
                
                import io
                image = Image.open(io.BytesIO(image_data_bytes))
            else:
                raise ValueError("No image source provided")
            
            if region:
                x = region.get('x', 0)
                y = region.get('y', 0)
                w = region.get('width', image.width)
                h = region.get('height', image.height)
                image = image.crop((x, y, x + w, y + h))
            
            custom_config = params.get('config', '--psm 6')
            
            text = pytesseract.image_to_string(
                image,
                lang=lang,
                config=custom_config
            )
            
            try:
                data = pytesseract.image_to_data(
                    image,
                    lang=lang,
                    config=custom_config,
                    output_type=pytesseract.Output.DICT
                )
                
                confidence_scores = [
                    float(conf) for conf in data.get('conf', [])
                    if conf != '-1'
                ]
                avg_confidence = sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0.0
            except Exception:
                avg_confidence = 0.0
            
            boxes = self._extract_boxes(data) if 'conf' in data else []
            
            return OCRResult(
                text=text.strip(),
                confidence=avg_confidence,
                boxes=boxes
            )
            
        except ImportError:
            return self._perform_ocr_fallback(image_path, image_data, lang)
    
    def _perform_ocr_fallback(
        self,
        image_path: Optional[str],
        image_data: Optional[str],
        lang: str
    ) -> OCRResult:
        """Fallback OCR using tesseract CLI."""
        if not image_path:
            raise ValueError("image_path required for fallback OCR")
        
        try:
            result = subprocess.run(
                ['tesseract', image_path, 'stdout', '-l', lang],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode != 0:
                raise ValueError(f"Tesseract failed: {result.stderr}")
            
            return OCRResult(
                text=result.stdout.strip(),
                confidence=0.0,
                boxes=[]
            )
            
        except FileNotFoundError:
            raise ValueError("Tesseract not installed and pytesseract not available")
        except subprocess.TimeoutExpired:
            raise ValueError("Tesseract OCR timed out")
    
    def _extract_boxes(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract bounding boxes from OCR data."""
        boxes = []
        n_boxes = len(data.get('text', []))
        
        for i in range(n_boxes):
            text = data['text'][i]
            if text.strip():
                boxes.append({
                    'text': text,
                    'x': data['left'][i],
                    'y': data['top'][i],
                    'width': data['width'][i],
                    'height': data['height'][i],
                    'confidence': float(data['conf'][i]) if data['conf'][i] != '-1' else 0.0
                })
        
        return boxes


from enum import Enum
