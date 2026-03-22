#!/bin/bash

APP_NAME="RabAI AutoClick"
VERSION="2.3.0"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "========================================"
echo "  打包 ${APP_NAME} v${VERSION}"
echo "========================================"

cd "${SCRIPT_DIR}"

echo "[1/5] 清理旧文件..."
rm -rf build/ dist/

echo "[2/5] 安装 pyinstaller..."
pip install pyinstaller -q

echo "[3/5] 创建应用包..."
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

echo "[4/5] 创建 dmg..."
if [ -d "dist/${APP_NAME}.app" ]; then
    hdiutil create -volname "${APP_NAME}" \
        -srcfolder "dist/${APP_NAME}.app" \
        -ov -format UDZO \
        "dist/${APP_NAME}-${VERSION}.dmg"
    echo "[5/5] 清理临时文件..."
    rm -rf build/
    echo "========================================"
    echo "  打包完成！"
    echo "  输出: dist/${APP_NAME}-${VERSION}.dmg"
    echo "========================================"
else
    echo "错误: 应用包创建失败"
    ls -la dist/
    exit 1
fi
