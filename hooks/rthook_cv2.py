"""Runtime hook for OpenCV (cv2) initialization.

This module is imported at runtime to configure OpenCV before the
application starts. It removes any cached OpenCV modules and sets
environment variables for optimal performance.

This helps resolve issues with:
- OpenCV module caching
- Large image memory limits
- Multiple OpenCV installation conflicts
- Thread safety on Windows
"""

import os
import sys
import platform
from typing import List


# Platform detection
IS_WINDOWS: bool = platform.system() == 'Windows'
IS_MACOS: bool = platform.system() == 'Darwin'
IS_LINUX: bool = platform.system() == 'Linux'


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


def _configure_opencv() -> None:
    """Configure OpenCV environment variables.
    
    Sets platform-specific configuration for optimal performance.
    """
    # Allow large images to be processed
    os.environ['CV_IO_MAX_IMAGE_PIXELS'] = '9223372036854775807'
    
    # Platform-specific settings
    if IS_WINDOWS:
        # Disable OpenCV's use of multiple threads on Windows
        # This can cause issues with PyQt
        os.environ['OPENCV_NUM_THREADS'] = '1'
        
        # Disable OpenCL for stability
        os.environ['OPENCV_OPENCL_RUNTIME'] = ''
        os.environ['OPENCV_OPENCL_DEVICE'] = 'disabled'
    
    elif IS_MACOS:
        # Enable high DPI support on macOS
        os.environ['QT_AUTO_SCREEN_SCALE_FACTOR'] = '1'
        
        # Metal GPU acceleration (macOS 12.3+)
        os.environ['OPENCV_VIDEOIO_Metal_ENABLE'] = '1'
    
    elif IS_LINUX:
        # Disable OpenCL for stability on Linux
        os.environ['OPENCV_OPENCL_RUNTIME'] = ''
        os.environ['OPENCV_OPENCL_DEVICE'] = 'disabled'


def _check_opencv_version() -> None:
    """Check and log OpenCV version for debugging.
    
    Reports the installed OpenCV version and available backends.
    """
    try:
        import cv2
        
        version = cv2.__version__
        build_info = cv2.getBuildInformation()
        
        # Log key information
        print(f"[rthook] OpenCV version: {version}")
        
        if 'Inference Engine' in build_info:
            print("[rthook] OpenVINO support: enabled")
        
        # Check for CUDA support
        if 'CUDA' in build_info and 'USE_CUDA' in build_info:
            print("[rthook] CUDA support: available")
        
    except ImportError:
        print("[rthook] Warning: OpenCV (cv2) not installed")
    except Exception as e:
        print(f"[rthook] Warning: Could not check OpenCV version: {e}")


# Execute configuration
_cleanup_cv2_modules()
_configure_opencv()

# Import cv2 after all configuration
import cv2  # noqa: E402

# Verify version
_check_opencv_version()
