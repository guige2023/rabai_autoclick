import sys
import os

os.environ['CV_IO_MAX_IMAGE_PIXELS'] = '9223372036854775807'

for key in list(sys.modules.keys()):
    if 'cv2' in key or 'opencv' in key:
        del sys.modules[key]

import cv2
