#!/bin/bash
#
# Build script for RabAI AutoClick application package (py2app)
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
log_info "[1/4] Cleaning old files..."
rm -rf build/ dist/

# Step 2: Install py2app
log_info "[2/4] Installing py2app..."
pip install py2app -q

# Step 3: Create application bundle
log_info "[3/4] Creating application bundle..."
python setup.py py2app

# Step 4: Create DMG
log_info "[4/4] Creating DMG..."
if [ -d "dist/${APP_NAME}.app" ]; then
    hdiutil create -volname "${APP_NAME}" \
        -srcfolder "dist/${APP_NAME}.app" \
        -ov -format UDZO \
        "dist/${APP_NAME}-${VERSION}.dmg"
    echo "========================================"
    echo -e "${GREEN}  Build complete!${NC}"
    echo "  Output: dist/${APP_NAME}-${VERSION}.dmg"
    echo "========================================"
else
    log_error "Build failed - application bundle not found"
    exit 1
fi
