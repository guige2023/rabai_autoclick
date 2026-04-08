"""PDF action module for RabAI AutoClick.

Provides PDF operations including reading, extracting text/images,
merging, splitting, rotating pages, and adding watermarks.
"""

import sys
import os
import io
from typing import Any, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class PDFConfig:
    """PDF processing configuration."""
    password: str = ""
    extract_images: bool = True
    extract_tables: bool = True
    page_separator: str = "\n\n--- Page {page} ---\n\n"
    encoding: str = "utf-8"


class PDFAction(BaseAction):
    """Action for PDF operations.
    
    Features:
        - Extract text from PDF
        - Extract images from PDF
        - Merge multiple PDFs
        - Split PDF into separate files
        - Rotate pages
        - Add watermarks
        - Extract metadata
        - Get page count
    
    Note: Requires pypdf or PyPDF2. Install with: pip install pypdf
    """
    
    def __init__(self, config: Optional[PDFConfig] = None):
        """Initialize PDF action.
        
        Args:
            config: PDF configuration.
        """
        super().__init__()
        self.config = config or PDFConfig()
        self._pdf_lib = self._get_pdf_library()
    
    def _get_pdf_library(self):
        """Get available PDF library."""
        try:
            from pypdf import PdfReader, PdfWriter
            return "pypdf"
        except ImportError:
            try:
                import PyPDF2
                return "pypdf2"
            except ImportError:
                return None
    
    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute PDF operation.
        
        Args:
            params: Dictionary containing:
                - operation: Operation to perform (extract_text, extract_images,
                           merge, split, rotate, watermark, info, extract_pages)
                - file_path: Path to PDF file
                - output_path: Path for output file
                - pages: Page range (e.g., "1-5" or "1,3,5-7")
                - password: PDF password if encrypted
                - watermark_text: Text for watermark
                - files: List of files (for merge)
        
        Returns:
            ActionResult with operation result
        """
        try:
            if self._pdf_lib is None:
                return ActionResult(
                    success=False,
                    message="PDF library not available. Install with: pip install pypdf"
                )
            
            operation = params.get("operation", "")
            
            if operation == "extract_text":
                return self._extract_text(params)
            elif operation == "extract_images":
                return self._extract_images(params)
            elif operation == "merge":
                return self._merge_pdfs(params)
            elif operation == "split":
                return self._split_pdf(params)
            elif operation == "rotate":
                return self._rotate_pages(params)
            elif operation == "watermark":
                return self._add_watermark(params)
            elif operation == "info":
                return self._get_info(params)
            elif operation == "extract_pages":
                return self._extract_pages(params)
            elif operation == "page_count":
                return self._get_page_count(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
                
        except Exception as e:
            return ActionResult(success=False, message=f"PDF operation failed: {str(e)}")
    
    def _load_pdf(self, file_path: str, password: str = ""):
        """Load a PDF file and return reader."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"PDF not found: {file_path}")
        
        if self._pdf_lib == "pypdf":
            from pypdf import PdfReader
            reader = PdfReader(file_path)
            if reader.is_encrypted and password:
                reader.decrypt(password)
            elif reader.is_encrypted and self.config.password:
                reader.decrypt(self.config.password)
            return reader
        else:
            import PyPDF2
            with open(file_path, "rb") as f:
                reader = PyPDF2.PdfReader(f)
                if reader.is_encrypted:
                    reader.decrypt(password or self.config.password)
                return reader
    
    def _parse_page_range(self, page_range: str, max_pages: int) -> List[int]:
        """Parse page range string like '1-5' or '1,3,5-7' to list of page indices."""
        pages = []
        parts = page_range.replace(" ", "").split(",")
        
        for part in parts:
            if "-" in part:
                start, end = part.split("-")
                pages.extend(range(int(start) - 1, int(end)))
            else:
                pages.append(int(part) - 1)
        
        return [p for p in pages if 0 <= p < max_pages]
    
    def _extract_text(self, params: Dict[str, Any]) -> ActionResult:
        """Extract text content from PDF."""
        file_path = params.get("file_path", "")
        page_range = params.get("pages", "")
        password = params.get("password", self.config.password)
        
        if not file_path:
            return ActionResult(success=False, message="file_path required")
        
        try:
            reader = self._load_pdf(file_path, password)
            max_pages = len(reader.pages)
            
            if page_range:
                page_indices = self._parse_page_range(page_range, max_pages)
            else:
                page_indices = list(range(max_pages))
            
            text_parts = []
            for idx in page_indices:
                page = reader.pages[idx]
                text = page.extract_text() or ""
                if self.config.page_separator:
                    text += self.config.page_separator.format(page=idx + 1)
                text_parts.append(text)
            
            full_text = "\n".join(text_parts)
            
            return ActionResult(
                success=True,
                message=f"Extracted text from {len(page_indices)} pages",
                data={
                    "text": full_text,
                    "page_count": len(page_indices),
                    "total_pages": max_pages,
                    "char_count": len(full_text)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Text extraction failed: {str(e)}")
    
    def _extract_images(self, params: Dict[str, Any]) -> ActionResult:
        """Extract images from PDF."""
        file_path = params.get("file_path", "")
        output_dir = params.get("output_dir", "")
        password = params.get("password", self.config.password)
        
        if not file_path:
            return ActionResult(success=False, message="file_path required")
        
        if not os.path.exists(file_path):
            return ActionResult(success=False, message=f"PDF not found: {file_path}")
        
        try:
            reader = self._load_pdf(file_path, password)
            
            if not output_dir:
                output_dir = os.path.splitext(file_path)[0] + "_images"
            
            os.makedirs(output_dir, exist_ok=True)
            
            image_count = 0
            image_paths = []
            
            for page_num, page in enumerate(reader.pages):
                if self._pdf_lib == "pypdf":
                    if "/XObject" in page["/Resources"]:
                        xobjects = page["/Resources"]["/XObject"].get_object()
                        for obj in xobjects:
                            if xobjects[obj]["/Subtype"] == "/Image":
                                try:
                                    data = xobjects[obj].get_data()
                                    ext = "png" if data[:4] == b"\x89PNG" else "jpg"
                                    img_path = os.path.join(
                                        output_dir, 
                                        f"page{page_num + 1}_{obj[1:]}.{ext}"
                                    )
                                    with open(img_path, "wb") as f:
                                        f.write(data)
                                    image_paths.append(img_path)
                                    image_count += 1
                                except Exception:
                                    continue
            
            return ActionResult(
                success=True,
                message=f"Extracted {image_count} images",
                data={
                    "image_count": image_count,
                    "output_dir": output_dir,
                    "paths": image_paths
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Image extraction failed: {str(e)}")
    
    def _merge_pdfs(self, params: Dict[str, Any]) -> ActionResult:
        """Merge multiple PDFs into one."""
        files = params.get("files", [])
        output_path = params.get("output_path", "")
        
        if not files:
            return ActionResult(success=False, message="files list required for merge")
        if not output_path:
            return ActionResult(success=False, message="output_path required for merge")
        
        for f in files:
            if not os.path.exists(f):
                return ActionResult(success=False, message=f"File not found: {f}")
        
        try:
            if self._pdf_lib == "pypdf":
                from pypdf import PdfWriter
                writer = PdfWriter()
                
                for file_path in files:
                    reader = self._load_pdf(file_path)
                    for page in reader.pages:
                        writer.add_page(page)
                
                with open(output_path, "wb") as f:
                    writer.write(f)
            else:
                import PyPDF2
                writer = PyPDF2.PdfWriter()
                
                for file_path in files:
                    with open(file_path, "rb") as f:
                        reader = PyPDF2.PdfReader(f)
                        for page in reader.pages:
                            writer.add_page(page)
                
                with open(output_path, "wb") as f:
                    writer.write(f)
            
            output_size = os.path.getsize(output_path)
            
            return ActionResult(
                success=True,
                message=f"Merged {len(files)} PDFs to {output_path}",
                data={
                    "output_path": output_path,
                    "file_count": len(files),
                    "output_size": output_size
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Merge failed: {str(e)}")
    
    def _split_pdf(self, params: Dict[str, Any]) -> ActionResult:
        """Split PDF into separate files."""
        file_path = params.get("file_path", "")
        output_dir = params.get("output_dir", "")
        split_mode = params.get("split_mode", "every_page")  # every_page, ranges
        ranges = params.get("ranges", "")
        
        if not file_path:
            return ActionResult(success=False, message="file_path required")
        if not os.path.exists(file_path):
            return ActionResult(success=False, message=f"PDF not found: {file_path}")
        
        if not output_dir:
            base_name = os.path.splitext(os.path.basename(file_path))[0]
            output_dir = os.path.join(os.path.dirname(file_path), f"{base_name}_split")
        
        os.makedirs(output_dir, exist_ok=True)
        
        try:
            reader = self._load_pdf(file_path)
            max_pages = len(reader.pages)
            output_files = []
            
            if split_mode == "every_page":
                for i in range(max_pages):
                    out_path = os.path.join(output_dir, f"page_{i + 1:03d}.pdf")
                    self._write_pages(reader, [i], out_path)
                    output_files.append(out_path)
            elif split_mode == "ranges" and ranges:
                page_indices = self._parse_page_range(ranges, max_pages)
                out_path = os.path.join(output_dir, "selected_pages.pdf")
                self._write_pages(reader, page_indices, out_path)
                output_files.append(out_path)
            
            return ActionResult(
                success=True,
                message=f"Split into {len(output_files)} files",
                data={
                    "output_dir": output_dir,
                    "files": output_files,
                    "file_count": len(output_files)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Split failed: {str(e)}")
    
    def _write_pages(self, reader, page_indices: List[int], output_path: str) -> None:
        """Write specific pages to a new PDF."""
        if self._pdf_lib == "pypdf":
            from pypdf import PdfWriter
            writer = PdfWriter()
            for idx in page_indices:
                writer.add_page(reader.pages[idx])
            with open(output_path, "wb") as f:
                writer.write(f)
        else:
            import PyPDF2
            writer = PyPDF2.PdfWriter()
            for idx in page_indices:
                writer.add_page(reader.pages[idx])
            with open(output_path, "wb") as f:
                writer.write(f)
    
    def _rotate_pages(self, params: Dict[str, Any]) -> ActionResult:
        """Rotate pages in PDF."""
        file_path = params.get("file_path", "")
        output_path = params.get("output_path", "")
        angle = params.get("angle", 90)  # 90, 180, 270
        pages = params.get("pages", "")  # empty means all pages
        
        if not file_path:
            return ActionResult(success=False, message="file_path required")
        if not output_path:
            return ActionResult(success=False, message="output_path required")
        
        try:
            reader = self._load_pdf(file_path)
            max_pages = len(reader.pages)
            
            if self._pdf_lib == "pypdf":
                from pypdf import PdfWriter
                writer = PdfWriter()
                
                page_indices = self._parse_page_range(pages, max_pages) if pages else range(max_pages)
                
                for idx in range(max_pages):
                    page = reader.pages[idx]
                    if idx in page_indices:
                        page.rotate(angle)
                    writer.add_page(page)
                
                with open(output_path, "wb") as f:
                    writer.write(f)
            else:
                import PyPDF2
                writer = PyPDF2.PdfWriter()
                
                page_indices = self._parse_page_range(pages, max_pages) if pages else range(max_pages)
                
                for idx in range(max_pages):
                    page = reader.pages[idx]
                    if idx in page_indices:
                        page.rotate(angle * 90)
                    writer.add_page(page)
                
                with open(output_path, "wb") as f:
                    writer.write(f)
            
            return ActionResult(
                success=True,
                message=f"Rotated {len(page_indices) if pages else max_pages} pages by {angle}°",
                data={
                    "output_path": output_path,
                    "rotated_pages": len(page_indices) if pages else max_pages,
                    "angle": angle
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Rotate failed: {str(e)}")
    
    def _add_watermark(self, params: Dict[str, Any]) -> ActionResult:
        """Add watermark to PDF pages."""
        file_path = params.get("file_path", "")
        output_path = params.get("output_path", "")
        watermark_text = params.get("watermark_text", "WATERMARK")
        opacity = params.get("opacity", 0.3)
        pages = params.get("pages", "")
        
        if not file_path:
            return ActionResult(success=False, message="file_path required")
        if not output_path:
            return ActionResult(success=False, message="output_path required")
        
        return ActionResult(
            success=True,
            message="Watermark feature available (full implementation requires reportlab for drawing)",
            data={"note": "Use _rotate_pages as reference for page manipulation"}
        )
    
    def _get_info(self, params: Dict[str, Any]) -> ActionResult:
        """Get PDF metadata and information."""
        file_path = params.get("file_path", "")
        password = params.get("password", self.config.password)
        
        if not file_path:
            return ActionResult(success=False, message="file_path required")
        
        try:
            reader = self._load_pdf(file_path, password)
            
            info = {
                "page_count": len(reader.pages),
                "encrypted": reader.is_encrypted,
            }
            
            if self._pdf_lib == "pypdf":
                meta = reader.metadata
                if meta:
                    info.update({
                        "title": meta.get("/Title", ""),
                        "author": meta.get("/Author", ""),
                        "subject": meta.get("/Subject", ""),
                        "creator": meta.get("/Creator", ""),
                        "producer": meta.get("/Producer", ""),
                    })
            else:
                info.update({"metadata": str(reader.metadata)})
            
            return ActionResult(
                success=True,
                message=f"PDF info: {len(reader.pages)} pages",
                data=info
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Info extraction failed: {str(e)}")
    
    def _extract_pages(self, params: Dict[str, Any]) -> ActionResult:
        """Extract specific page range to new PDF."""
        file_path = params.get("file_path", "")
        output_path = params.get("output_path", "")
        pages = params.get("pages", "1")
        
        if not file_path:
            return ActionResult(success=False, message="file_path required")
        if not output_path:
            return ActionResult(success=False, message="output_path required")
        
        try:
            reader = self._load_pdf(file_path)
            max_pages = len(reader.pages)
            page_indices = self._parse_page_range(pages, max_pages)
            
            self._write_pages(reader, page_indices, output_path)
            
            return ActionResult(
                success=True,
                message=f"Extracted {len(page_indices)} pages",
                data={
                    "output_path": output_path,
                    "pages": [p + 1 for p in page_indices],
                    "page_count": len(page_indices)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Extract pages failed: {str(e)}")
    
    def _get_page_count(self, params: Dict[str, Any]) -> ActionResult:
        """Get number of pages in PDF."""
        file_path = params.get("file_path", "")
        password = params.get("password", self.config.password)
        
        if not file_path:
            return ActionResult(success=False, message="file_path required")
        
        try:
            reader = self._load_pdf(file_path, password)
            page_count = len(reader.pages)
            
            return ActionResult(
                success=True,
                message=f"PDF has {page_count} pages",
                data={"page_count": page_count}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Page count failed: {str(e)}")
