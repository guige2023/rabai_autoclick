#!/bin/bash

APP_NAME="RabAI AutoClick"
VERSION="2.3.0"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "========================================"
echo "  打包 ${APP_NAME} v${VERSION}"
echo "========================================"

cd "${SCRIPT_DIR}"

echo "[1/4] 清理旧文件..."
rm -rf build/ dist/

echo "[2/4] 安装 py2app..."
pip install py2app -q

echo "[3/4] 创建应用包..."
python setup.py py2app

echo "[4/4] 创建 dmg..."
if [ -d "dist/${APP_NAME}.app" ]; then
    hdiutil create -volname "${APP_NAME}" \
        -srcfolder "dist/${APP_NAME}.app" \
        -ov -format UDZO \
        "dist/${APP_NAME}-${VERSION}.dmg"
    echo "========================================"
    echo "  打包完成！"
    echo "  输出: dist/${APP_NAME}-${VERSION}.dmg"
    echo "========================================"
else
    echo "错误: 应用包创建失败"
    exit 1
fi
