"""Runtime hook for OpenCV (cv2) initialization.

This module is imported at runtime to configure OpenCV before the
application starts. It removes any cached OpenCV modules and sets
environment variables for optimal performance.

This helps resolve issues with:
- OpenCV module caching
- Large image memory limits
- Multiple OpenCV installation conflicts
"""

import os
import sys
from typing import List


def _cleanup_cv2_modules() -> None:
    """Remove cached cv2 modules from sys.modules.
    
    This ensures a clean import of cv2 without any version conflicts
    from previously imported modules.
    """
    keys_to_remove: List[str] = []
    
    for key in sys.modules.keys():
        if 'cv2' in key or 'opencv' in key:
            keys_to_remove.append(key)
    
    for key in keys_to_remove:
        del sys.modules[key]


# Configure OpenCV settings
os.environ['CV_IO_MAX_IMAGE_PIXELS'] = '9223372036854775807'

# Clean up any cached OpenCV modules
_cleanup_cv2_modules()

# Import cv2 after cleanup
import cv2  # noqa: E402
