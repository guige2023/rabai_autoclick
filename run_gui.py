#!/usr/bin/env python3
"""
启动 RabAI AutoClick v22 GUI 桌面应用
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from gui.main import main

if __name__ == "__main__":
    main()
