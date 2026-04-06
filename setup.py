#!/usr/bin/env python3
"""Setup script for building RabAI AutoClick as a macOS application.

This script is used with py2app to create a standalone .app bundle:

    python setup.py py2app

For development, use the main.py directly instead.

Usage:
    # Build the application
    python setup.py py2app
    
    # The app will be created in dist/RabAI AutoClick.app
"""

import os
import sys
from typing import List, Tuple, Dict, Any

from setuptools import setup


# Application bundle configuration
APP: List[str] = ['main.py']

DATA_FILES: List[Tuple[str, List[str]]] = [
    ('resources', ['resources']),
    ('actions', ['actions']),
    ('core', ['core']),
    ('ui', ['ui']),
    ('utils', ['utils']),
]

OPTIONS: Dict[str, Any] = {
    'argv_emulation': False,
    'packages': [
        'PyQt5',
        'pynput',
        'cv2',
        'numpy',
        'rapidocr_onnxruntime',
    ],
    'includes': [
        'PyQt5.QtCore',
        'PyQt5.QtGui',
        'PyQt5.QtWidgets',
        'pynput',
        'pynput.keyboard',
        'pynput.mouse',
        'cv2',
        'numpy',
        'rapidocr_onnxruntime',
    ],
    'excludes': [
        'tkinter',
        'matplotlib',
        'pandas',
        'scipy',
    ],
    'iconfile': 'resources/icon.icns',
    'plist': {
        'CFBundleName': 'RabAI AutoClick',
        'CFBundleDisplayName': 'RabAI AutoClick',
        'CFBundleIdentifier': 'com.rabai.autoclick',
        'CFBundleVersion': '2.3.0',
        'CFBundleShortVersionString': '2.3.0',
        'NSHighResolutionCapable': True,
        'NSRequiresAquaSystemAppearance': False,
        'NSAppleEventsUsageDescription': (
            'This app needs to control other applications.'
        ),
        'NSAccessibilityUsageDescription': (
            'This app needs accessibility permissions to automate clicks.'
        ),
    },
}


setup(
    name='RabAI AutoClick',
    app=APP,
    data_files=DATA_FILES,
    options={'py2app': OPTIONS},
    setup_requires=['py2app'],
)
