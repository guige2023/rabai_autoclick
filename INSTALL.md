# Installation Guide

## Prerequisites

### System Requirements

- **Operating System**: macOS 10.14+ or Windows 10/11
- **Python**: 3.8, 3.9, 3.10, 3.11, or 3.12
- **RAM**: 4GB minimum (8GB recommended)
- **Disk Space**: 500MB for installation

### Required Permissions

#### macOS

RabAI AutoClick requires accessibility permissions to automate mouse and keyboard actions:

1. Go to **System Preferences → Security & Privacy → Privacy → Accessibility**
2. Click the lock icon to make changes
3. Click **+** to add applications
4. Add **Terminal** (or your Python application)
5. Ensure the entry is checked

For screen recording (OCR and image matching):
1. Go to **System Preferences → Security & Privacy → Privacy → Screen Recording**
2. Add Terminal/Python if not already present

#### Windows

Run as Administrator for the first time to enable automation features.

## Installation Methods

### Method 1: pip install (Recommended)

```bash
# Clone the repository
git clone https://github.com/guige2023/rabai_autoclick.git
cd rabai_autoclick

# Create virtual environment (recommended)
python3 -m venv .venv
source .venv/bin/activate  # macOS/Linux
# .\.venv\\Scripts\\activate  # Windows

# Install dependencies
pip install -r requirements.txt

# Run the application
python main.py
```

### Method 2: Development Installation

```bash
# Clone and setup
git clone https://github.com/guige2023/rabai_autoclick.git
cd rabai_autoclick

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install all dependencies including development tools
pip install -r requirements.txt
pip install black ruff pytest pytest-cov

# Run in development mode
python main.py
```

### Method 3: macOS Application Bundle

Download the latest `.dmg` file from the releases page:

1. Visit [GitHub Releases](https://github.com/guige2023/rabai_autoclick/releases)
2. Download the latest `RabAI-AutoClick-*.dmg`
3. Open the `.dmg` file
4. Drag RabAI AutoClick to Applications

## Optional Dependencies

### OCR Engines

RabAI AutoClick supports multiple OCR engines:

#### PaddleOCR (Default, Recommended)

Installed by default with requirements.txt.

#### EasyOCR (Optional)

```bash
pip install easyocr
```

#### RapidOCR (Optional, Best Accuracy)

```bash
pip install rapidocr_onnxruntime
```

## Verifying Installation

Run the built-in test to verify everything is working:

```bash
# Test basic functionality
python tests/test_core.py

# Or run the full test suite
pytest tests/ -v
```

## Troubleshooting

### "Python is not responding to accessibility permissions"

1. Close RabAI AutoClick completely
2. Go to **System Preferences → Security & Privacy → Privacy → Accessibility**
3. Remove Python/Terminal from the list
4. Re-add it following the steps above
5. Restart RabAI AutoClick

### "cv2 import error" or OpenCV issues

```bash
pip uninstall opencv-python
pip install opencv-python==4.8.0.76
```

### "pynput not working" on macOS

```bash
pip uninstall pynput
pip install pynput==1.7.6
```

### "Module not found" errors

```bash
# Make sure you're in the correct directory
cd rabai_autoclick

# Reinstall dependencies
pip install -r requirements.txt --force-reinstall
```

## Docker (Advanced)

For containerized execution:

```dockerfile
FROM python:3.11-slim

# Install system dependencies for GUI automation
RUN apt-get update && apt-get install -y \
    libxkbcommon-x11-0 \
    libxcb-icccm4-0 \
    libxcb-image0-0 \
    libxcb-keysyms1-0 \
    libxcb-randr0-0 \
    libxcb-render0-0 \
    libxcb-shm0-0 \
    libxcb-xfixes0-0 \
    libxdo3 \
    xdotool \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
CMD ["python", "main.py"]
```

Note: GUI automation in Docker requires X11 forwarding or virtual display.

## Next Steps

After installation, see [README.md](README.md) for:
- Quick start guide
- Feature overview
- Usage tutorials
