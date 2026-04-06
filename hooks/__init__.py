"""Runtime hooks for RabAI AutoClick.

This package contains runtime hooks that are executed before the
main application starts to configure various components.

Available hooks:
- rthook_cv2: OpenCV initialization and configuration
"""

from .rthook_cv2 import _cleanup_cv2_modules

__all__ = [
    '_cleanup_cv2_modules',
]
