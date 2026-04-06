#!/usr/bin/env python3
"""Launch RabAI AutoClick v22 GUI Desktop Application.

This is an alternative entry point that uses the gui.main module.
For the main application entry point, use main.py instead.
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from gui.main import main


if __name__ == "__main__":
    main()
