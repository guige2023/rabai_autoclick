"""QRCode action module for RabAI AutoClick.

Provides QR code operations:
- QRGenerateAction: Generate QR code
- QRReadAction: Read QR code from image
- QRBatchAction: Generate batch QR codes
- BarcodeGenerateAction: Generate barcode
"""

import os
import base64
import io
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class QRGenerateAction(BaseAction):
    """Generate QR code."""
    action_type = "qr_generate"
    display_name = "生成二维码"
    description = "生成QR二维码"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute generate.

        Args:
            context: Execution context.
            params: Dict with content, output_path, size, error_correction, output_var.

        Returns:
            ActionResult with QR code path or base64.
        """
        content = params.get('content', '')
        output_path = params.get('output_path', '/tmp/qrcode.png')
        size = params.get('size', 300)
        error_correction = params.get('error_correction', 'M')
        output_var = params.get('output_var', 'qrcode_path')
        return_base64 = params.get('return_base64', False)

        valid, msg = self.validate_type(content, str, 'content')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import qrcode

            resolved_content = context.resolve_value(content)
            resolved_output = context.resolve_value(output_path)
            resolved_size = context.resolve_value(size)

            ec_map = {
                'L': qrcode.constants.ERROR_CORRECT_L,
                'M': qrcode.constants.ERROR_CORRECT_M,
                'Q': qrcode.constants.ERROR_CORRECT_Q,
                'H': qrcode.constants.ERROR_CORRECT_H
            }
            ec = ec_map.get(error_correction, qrcode.constants.ERROR_CORRECT_M)

            qr = qrcode.QRCode(
                version=1,
                error_correction=ec,
                box_size=10,
                border=4
            )
            qr.add_data(resolved_content)
            qr.make(fit=True)

            img = qr.make_image(fill_color='black', back_color='white')

            if resolved_size != 300:
                from PIL import Image
                img = img.resize((int(resolved_size), int(resolved_size)), Image.LANCZOS)

            if return_base64:
                buffer = io.BytesIO()
                img.save(buffer, format='PNG')
                b64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
                context.set(output_var, b64)
                return ActionResult(
                    success=True,
                    message=f"QR码已生成 (Base64, {len(b64)} chars)",
                    data={'base64': b64, 'output_var': output_var}
                )
            else:
                img.save(resolved_output)
                context.set(output_var, resolved_output)
                return ActionResult(
                    success=True,
                    message=f"QR码已保存: {resolved_output}",
                    data={'path': resolved_output, 'output_var': output_var}
                )
        except ImportError:
            return ActionResult(
                success=False,
                message="qrcode库未安装: pip install qrcode pillow"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"QR码生成失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['content']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_path': '/tmp/qrcode.png', 'size': 300, 'error_correction': 'M', 'output_var': 'qrcode_path', 'return_base64': False}


class QRReadAction(BaseAction):
    """Read QR code from image."""
    action_type = "qr_read"
    display_name = "识别二维码"
    description = "从图片中识别QR码"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute read.

        Args:
            context: Execution context.
            params: Dict with image_path, base64_data, output_var.

        Returns:
            ActionResult with decoded content.
        """
        image_path = params.get('image_path', '')
        base64_data = params.get('base64_data', '')
        output_var = params.get('output_var', 'qrcode_content')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(image_path) if image_path else ''
            resolved_b64 = context.resolve_value(base64_data) if base64_data else ''

            if resolved_path and os.path.exists(resolved_path):
                image_data = open(resolved_path, 'rb').read()
            elif resolved_b64:
                image_data = base64.b64decode(resolved_b64)
            else:
                return ActionResult(
                    success=False,
                    message="必须提供image_path或base64_data"
                )

            try:
                from PIL import Image
                from pyzbar.pyzbar import decode as pyzbar_decode
                import sys
                sys.path.insert(0, _parent_dir)

                img = Image.open(io.BytesIO(image_data))
                decoded = pyzbar_decode(img)

                if not decoded:
                    return ActionResult(
                        success=False,
                        message="未检测到QR码"
                    )

                results = []
                for d in decoded:
                    results.append({
                        'data': d.data.decode('utf-8', errors='replace'),
                        'type': d.type,
                        'rect': {'left': d.rect.left, 'top': d.rect.top, 'width': d.rect.width, 'height': d.rect.height}
                    })

                content = results[0]['data'] if len(results) == 1 else [r['data'] for r in results]
                context.set(output_var, content)

                return ActionResult(
                    success=True,
                    message=f"识别到 {len(results)} 个QR码",
                    data={'count': len(results), 'content': content, 'results': results, 'output_var': output_var}
                )
            except ImportError:
                return ActionResult(
                    success=False,
                    message="pyzbar未安装: pip install pyzbar (macOS需要: brew install zbar)"
                )
            except Exception as e:
                return ActionResult(
                    success=False,
                    message=f"QR码识别失败: {str(e)}"
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"QR码读取失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'image_path': '', 'base64_data': '', 'output_var': 'qrcode_content'}


class QRBatchAction(BaseAction):
    """Generate batch QR codes."""
    action_type = "qr_batch"
    display_name = "批量生成二维码"
    description = "批量生成QR二维码"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute batch generate.

        Args:
            context: Execution context.
            params: Dict with items, output_dir, prefix, size, output_var.

        Returns:
            ActionResult with generated file paths.
        """
        items = params.get('items', [])
        output_dir = params.get('output_dir', '/tmp/qrcodes')
        prefix = params.get('prefix', 'qr')
        size = params.get('size', 300)
        output_var = params.get('output_var', 'qrcode_batch')

        valid, msg = self.validate_type(items, list, 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import qrcode
            from PIL import Image

            resolved_items = context.resolve_value(items)
            resolved_dir = context.resolve_value(output_dir)
            resolved_prefix = context.resolve_value(prefix)
            resolved_size = context.resolve_value(size)

            os.makedirs(resolved_dir, exist_ok=True)

            generated = []
            for i, item in enumerate(resolved_items):
                if isinstance(item, str):
                    content = item
                    filename = f"{resolved_prefix}_{i+1}.png"
                elif isinstance(item, dict):
                    content = item.get('content', str(item))
                    filename = item.get('filename', f"{resolved_prefix}_{i+1}.png")
                else:
                    content = str(item)
                    filename = f"{resolved_prefix}_{i+1}.png"

                qr = qrcode.QRCode(version=1, error_correction=qrcode.constants.ERROR_CORRECT_M, box_size=10, border=4)
                qr.add_data(content)
                qr.make(fit=True)

                img = qr.make_image(fill_color='black', back_color='white')
                if resolved_size != 300:
                    img = img.resize((int(resolved_size), int(resolved_size)), Image.LANCZOS)

                filepath = os.path.join(resolved_dir, filename)
                img.save(filepath)
                generated.append({'content': content, 'path': filepath})

            context.set(output_var, generated)

            return ActionResult(
                success=True,
                message=f"批量生成 {len(generated)} 个QR码",
                data={'count': len(generated), 'files': generated, 'output_var': output_var}
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="qrcode库未安装"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"批量QR码生成失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_dir': '/tmp/qrcodes', 'prefix': 'qr', 'size': 300, 'output_var': 'qrcode_batch'}


class BarcodeGenerateAction(BaseAction):
    """Generate barcode."""
    action_type = "barcode_generate"
    display_name = "生成条形码"
    description = "生成条形码"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute barcode generate.

        Args:
            context: Execution context.
            params: Dict with content, output_path, barcode_type, output_var.

        Returns:
            ActionResult with barcode path.
        """
        content = params.get('content', '')
        output_path = params.get('output_path', '/tmp/barcode.png')
        barcode_type = params.get('barcode_type', 'CODE128')
        output_var = params.get('output_var', 'barcode_path')

        valid, msg = self.validate_type(content, str, 'content')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            from barcode import generate as barcode_generate, get_barcode_class
            from barcode.writer import ImageWriter

            resolved_content = context.resolve_value(content)
            resolved_output = context.resolve_value(output_path)
            resolved_type = context.resolve_value(barcode_type)

            type_map = {
                'CODE128': 'code128',
                'CODE39': 'code39',
                'EAN13': 'ean13',
                'EAN8': 'ean8',
                'ISBN': 'isbn13',
                'UPC': 'upc',
                'ITF': 'itf'
            }

            barcode_cls_name = type_map.get(resolved_type, 'code128')
            barcode_cls = get_barcode_class(barcode_cls_name)

            # CODE128 and CODE39 don't need numeric-only
            barcode_obj = barcode_cls(resolved_content, writer=ImageWriter())
            barcode_obj.save(resolved_output.replace('.png', ''))

            context.set(output_var, resolved_output)

            return ActionResult(
                success=True,
                message=f"条形码已生成: {resolved_output}",
                data={'path': resolved_output, 'output_var': output_var}
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="python-barcode未安装: pip install python-barcode"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"条形码生成失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['content']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_path': '/tmp/barcode.png', 'barcode_type': 'CODE128', 'output_var': 'barcode_path'}
