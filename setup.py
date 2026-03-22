#!/usr/bin/env python3
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from setuptools import setup

APP = ['main.py']
DATA_FILES = [
    ('resources', ['resources']),
    ('actions', ['actions']),
    ('core', ['core']),
    ('ui', ['ui']),
    ('utils', ['utils']),
]

OPTIONS = {
    'argv_emulation': False,
    'packages': ['PyQt5', 'pynput', 'cv2', 'numpy', 'rapidocr_onnxruntime'],
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
        'NSAppleEventsUsageDescription': 'This app needs to control other applications.',
        'NSAccessibilityUsageDescription': 'This app needs accessibility permissions to automate clicks.',
    }
}

setup(
    name='RabAI AutoClick',
    app=APP,
    data_files=DATA_FILES,
    options={'py2app': OPTIONS},
    setup_requires=['py2app'],
)
