#!/bin/bash
#
# Build script for RabAI AutoClick DMG package
# Creates a distributable macOS application bundle
#

set -e  # Exit on error

APP_NAME="RabAI AutoClick"
VERSION="2.3.0"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

echo "========================================"
echo "  Building ${APP_NAME} v${VERSION}"
echo "========================================"

cd "${SCRIPT_DIR}"

# Step 1: Clean old files
log_info "[1/5] Cleaning old files..."
rm -rf build/ dist/

# Step 2: Install pyinstaller
log_info "[2/5] Installing pyinstaller..."
pip install pyinstaller -q

# Step 3: Create application bundle
log_info "[3/5] Creating application bundle..."
pyinstaller --noconfirm --onedir --windowed \
    --name "${APP_NAME}" \
    --add-data "actions:actions" \
    --add-data "core:core" \
    --add-data "ui:ui" \
    --add-data "utils:utils" \
    --add-data "workflows:workflows" \
    --add-data "data:data" \
    --runtime-hook "hooks/rthook_cv2.py" \
    --hidden-import "PyQt5.QtCore" \
    --hidden-import "PyQt5.QtGui" \
    --hidden-import "PyQt5.QtWidgets" \
    --hidden-import "pynput" \
    --hidden-import "pynput.keyboard" \
    --hidden-import "pynput.mouse" \
    --hidden-import "numpy" \
    --hidden-import "PIL" \
    --hidden-import "pyautogui" \
    --hidden-import "pyperclip" \
    --hidden-import "psutil" \
    --hidden-import "shapely" \
    --hidden-import "rapidocr_onnxruntime" \
    --hidden-import "onnxruntime" \
    --collect-all "rapidocr_onnxruntime" \
    --collect-all "onnxruntime" \
    --exclude-module "tkinter" \
    --exclude-module "matplotlib" \
    --exclude-module "pytest" \
    --exclude-module "pandas" \
    --exclude-module "scipy" \
    --osx-bundle-identifier "com.rabai.autoclick" \
    main.py

# Step 4: Create DMG
log_info "[4/5] Creating DMG..."
if [ -d "dist/${APP_NAME}.app" ]; then
    hdiutil create -volname "${APP_NAME}" \
        -srcfolder "dist/${APP_NAME}.app" \
        -ov -format UDZO \
        "dist/${APP_NAME}-${VERSION}.dmg"
    
    # Step 5: Cleanup
    log_info "[5/5] Cleaning temporary files..."
    rm -rf build/
    
    echo "========================================"
    echo -e "${GREEN}  Build complete!${NC}"
    echo "  Output: dist/${APP_NAME}-${VERSION}.dmg"
    echo "========================================"
else
    log_error "Build failed - application bundle not found"
    ls -la dist/
    exit 1
fi
