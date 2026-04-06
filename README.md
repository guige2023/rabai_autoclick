# RabAI AutoClick v22

<div align="center">

![Version](https://img.shields.io/badge/version-2.3.0-blue.svg)
![Python](https://img.shields.io/badge/Python-3.8+-green.svg)
![Platform](https://img.shields.io/badge/Platform-Windows%20%7C%20macOS-lightgrey.svg)
![License](https://img.shields.io/badge/License-MIT-yellow.svg)
[![GitHub stars](https://img.shields.io/github/stars/guige2023/rabai_autoclick)](https://github.com/guige2023/rabai_autoclick/stargazers)
[![GitHub forks](https://img.shields.io/github/forks/guige2023/rabai_autoclick)](https://github.com/guige2023/rabai_autoclick/network)
[![GitHub issues](https://img.shields.io/github/issues/guige2023/rabai_autoclick)](https://github.com/guige2023/rabai_autoclick/issues)
[![Last commit](https://img.shields.io/github/last-commit/guige2023/rabai_autoclick)](https://github.com/guige2023/rabai_autoclick/commits)

**智能桌面自动化工具 | 操作录制 | OCR文字识别 | 可视化编辑器 | 跨平台支持**

[功能特性](#功能特性) • [快速开始](#快速开始) • [详细文档](#详细文档) • [更新日志](#更新日志)

</div>

---

## 📖 目录

- [功能特性](#功能特性)
- [快速开始](#快速开始)
- [详细文档](#详细文档)
  - [操作录制](#操作录制)
  - [动作类型](#动作类型)
  - [OCR文字识别](#ocr文字识别)
  - [窗口与区域选择](#窗口与区域选择)
  - [快捷键设置](#快捷键设置)
- [更新日志](#更新日志)
- [常见问题](#常见问题)

---

## 功能特性

### 🎯 核心功能

| 功能模块 | 描述 |
|---------|------|
| **🎥 操作录制** | 一键录制鼠标点击、双击、滚轮、键盘操作，自动生成工作流 |
| **📝 可视化编辑器** | PyQt5 拖拽式界面，无需编程即可创建自动化流程 |
| **🔍 OCR文字识别** | 支持中英文识别，智能点击指定文字位置 |
| **🪟 窗口/区域选择** | 一键选择目标窗口或自定义识别区域 |
| **🔄 循环执行** | 支持设置循环次数和间隔时间 |
| **⌨️ 全局快捷键** | 运行/停止/录制快捷键，窗口最小化也能响应 |

### 🖱️ 鼠标操作

| 动作类型 | 功能描述 | 参数说明 |
|---------|---------|---------|
| **鼠标单击** | 在指定坐标执行单击操作 | `x`, `y`: 坐标位置<br>`button`: 左键/右键/中键 |
| **鼠标双击** | 在指定坐标执行双击操作 | `x`, `y`: 坐标位置<br>`button`: 左键/右键/中键 |
| **鼠标移动** | 移动鼠标到指定位置 | `x`, `y`: 目标坐标<br>`duration`: 移动耗时 |
| **鼠标拖拽** | 从起点拖拽到终点 | `start_x`, `start_y`: 起点<br>`end_x`, `end_y`: 终点 |
| **鼠标滚轮** | 模拟滚轮滚动 | `clicks`: 滚动格数<br>`direction`: 向上/向下 |

### ⌨️ 键盘操作

| 动作类型 | 功能描述 | 参数说明 |
|---------|---------|---------|
| **键盘输入** | 模拟键盘输入文本 | `text`: 输入内容<br>`enter_after`: 输入后按回车 |
| **按键操作** | 按下特定按键或组合键 | `keys`: 组合键如 ['ctrl', 'c'] |

### 🔍 图像识别

| 动作类型 | 功能描述 | 参数说明 |
|---------|---------|---------|
| **图像识别点击** | 通过模板匹配定位并点击 | `template`: 模板图片路径<br>`confidence`: 匹配置信度 |
| **查找图像** | 查找屏幕上的图像位置 | `find_all`: 查找所有匹配 |

---

## 快速开始

### 环境要求

- Python 3.8+
- Windows 10/11 或 macOS 10.14+

### 安装依赖

```bash
# 克隆仓库
git clone https://github.com/guige2023/rabai_autoclick.git
cd rabai_autoclick

# 安装依赖
pip install -r requirements.txt
```

### 启动程序

```bash
python main.py
```

### 基本使用流程

```
方式一：录制操作
1. 点击"开始录制" → 2. 执行操作 → 3. 点击"停止录制" → 4. 添加到工作流 → 5. 运行

方式二：手动添加
1. 选择动作类型 → 2. 配置参数 → 3. 添加步骤 → 4. 设置循环 → 5. 运行执行
```

---

## 详细文档

### 操作录制

#### 录制功能说明

v22 增强版新增操作录制功能，可以自动记录您的鼠标和键盘操作：

| 录制内容 | 说明 |
|---------|------|
| **鼠标单击** | 记录点击位置和按键 |
| **鼠标双击** | 自动识别双击操作 |
| **鼠标滚轮** | 记录滚动方向和格数 |
| **键盘快捷键** | 记录组合键如 Cmd+C |
| **文本输入** | 记录单个字符输入 |

#### 使用方法

1. 点击 **"🔴 开始录制"** 按钮或按 `Ctrl+F9`
2. 执行您想要录制的操作
3. 点击 **"⏹ 停止录制"** 按钮或按 `Ctrl+F10`
4. 点击 **"添加到工作流"** 将录制内容转为工作流步骤

#### 智能优化

录制完成后自动进行优化：
- 合并连续文本输入为完整字符串
- 移除过短延时（<0.1秒）
- 整理快捷键顺序（如 cmd+c 而非 c+cmd）

---

### 动作类型

#### 🖱️ 鼠标点击 (click)

**参数说明：**

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `x` | int | ✅ | - | X 坐标位置 |
| `y` | int | ✅ | - | Y 坐标位置 |
| `button` | string | ❌ | left | 鼠标按键：左键/右键/中键 |
| `clicks` | int | ❌ | 1 | 点击次数（双击填2） |

**坐标选取：** 点击参数旁的"选取"按钮，进入全屏选择模式

---

### OCR文字识别

#### 使用方法

1. **添加OCR步骤** - 从动作列表选择"OCR文字识别"
2. **设置识别区域** - 点击"📐 区域"按钮框选识别范围
3. **配置点击文字** - 在`click_text`填入要点击的文字
4. **选择匹配模式** - 勾选`exact_match`进行精确匹配

#### 参数详解

| 参数 | 说明 |
|------|------|
| `click_text` | 要点击的文字。OCR识别后点击包含此文字的区域中心 |
| `contains` | 只检测不点击，用于判断是否存在某文字 |
| `exact_match` | 勾选=完全一致，不勾选=包含即可 |
| `click_index` | 当有多个匹配时，点击第几个（0=第一个） |

---

### 窗口与区域选择

#### 窗口选择

1. 点击工具栏 **"🪟 窗口"** 按钮
2. 从窗口列表中选择目标窗口
3. 自动设置识别区域为窗口范围

#### 区域框选

1. 点击工具栏 **"📐 区域"** 按钮
2. 鼠标拖动选择区域
3. 自动应用到OCR步骤

---

### 快捷键设置

#### 默认快捷键

| 快捷键 | 功能 |
|--------|------|
| `Ctrl+F6` | 开始运行 |
| `Ctrl+F7` | 停止运行 |
| `Ctrl+F8` | 暂停/继续 |
| `Ctrl+F9` | 开始录制 |
| `Ctrl+F10` | 停止录制 |
| `Ctrl+F11` | 开启/关闭按键显示 |

#### 自定义快捷键

1. 点击工具栏 **"⌨ 快捷键"** 按钮
2. 点击输入框后按下新的快捷键
3. 支持组合键，如 `Ctrl+Shift+A`

> **macOS 用户**：全局快捷键需要在"系统偏好设置→安全性与隐私→辅助功能"中授权

---

### 按键显示功能

#### 功能说明

点击工具栏 **"🖥 显示"** 按钮或按 `Ctrl+F11` 开启按键显示功能：

| 显示内容 | 说明 |
|---------|------|
| **鼠标位置** | 实时显示鼠标坐标 |
| **按键内容** | 显示按下的按键，支持组合键 |
| **鼠标点击** | 显示鼠标点击（🖱L/🖱R/🖱M） |

#### 特点

- 独立窗口，主窗口最小化也能正常工作
- 按 `ESC` 或 `F11` 关闭显示
- 支持自定义快捷键开启/关闭

---

## 更新日志

### v2.3.0 (2025-03)
- ✨ **新增按键显示功能**：实时显示鼠标位置、按键内容、鼠标点击
- ✨ 新增独立窗口模式，主窗口最小化也能正常显示
- ✨ 新增显示快捷键（默认 Ctrl+F11）
- 🐛 **修复 macOS 上快捷键配置闪退问题**
- 🐛 修复 pynput 监听器与 Qt 的线程冲突问题
- 🐛 修复执行完成后主窗口不自动弹出的问题
- 🐛 修复快捷键停止录屏后主窗口不弹出的问题
- ⚡ 优化快捷键管理器，避免重复启动监听器
- ⚡ 使用 subprocess 隔离按键显示进程，提高稳定性

### v2.2.0 (2025-03)
- ✨ **新增操作录制功能**：一键录制鼠标点击、双击、滚轮、键盘操作
- ✨ 新增鼠标双击动作类型
- ✨ 新增全局快捷键支持（使用 pynput）
- ✨ 新增清空步骤按钮
- ✨ 快捷键设置支持录制快捷键配置
- 🐛 修复 macOS 上 keyboard 模块崩溃问题
- 🐛 修复窗口选择列表为空的问题
- 🐛 修复区域框选不透明的问题
- 🐛 修复录制停止快捷键被记录的问题
- 🐛 修复添加到工作流后动作类型为 None 的问题
- ⚡ 优化录制时自动最小化主窗口
- ⚡ 优化快捷键顺序整理（cmd+c 而非 c+cmd）

### v1.3.0 (2025-02)
- ✨ 新增窗口选择和区域框选功能
- ✨ 新增循环执行配置
- ✨ 新增内存优化功能
- ✨ 新增执行统计功能
- ✨ OCR参数改为下拉选择器
- 🐛 修复停止工作流不生效问题
- 🐛 修复OCR结果截断问题
- ⚡ 优化OCR识别速度

### v1.0.0 (2024-01)
- 🎉 初始版本发布
- ✅ 核心流程引擎
- ✅ 基础动作插件

---

## 常见问题

### Q: macOS 上快捷键不生效？

A: macOS 需要额外权限：
- 系统偏好设置 → 安全性与隐私 → 辅助功能
- 添加 Python 或终端到允许列表
- 重启程序后生效

### Q: 录制时停止快捷键被记录了？

A: v2.2.0 已修复此问题，停止录制的快捷键不会被记录到动作列表

### Q: OCR 识别不准确？

A: 尝试以下方法：
- 缩小识别区域（使用"📐 区域"框选）
- 确保文字清晰、对比度高
- 不勾选 `exact_match` 进行模糊匹配

### Q: 如何在 macOS 上使用？

A: macOS 需要授权辅助功能权限：
- 系统偏好设置 → 安全性与隐私 → 辅助功能
- 添加 Python 或终端到允许列表

---

## 许可证

[MIT License](LICENSE)

---

<div align="center">

**Made with ❤️ by RabAI Team**

</div>

---

## 🛠️ 开发指南

### 项目结构

```
rabai_autoclick/
├── actions/          # Action implementations (click, keyboard, OCR, etc.)
├── cli/              # CLI command-line interface
├── core/             # Core engine, context, and action loader
├── gui/              # GUI components
├── src/              # Advanced features (pipeline, diagnostics, etc.)
├── tests/            # Test suite
├── ui/               # PyQt5 UI components
├── utils/            # Utility modules (hotkey, logging, etc.)
├── main.py           # Main entry point
└── pyproject.toml    # Project configuration
```

### 开发环境设置

```bash
# 克隆并进入目录
git clone https://github.com/guige2023/rabai_autoclick.git
cd rabai_autoclick

# 创建虚拟环境 (推荐)
python3 -m venv .venv
source .venv/bin/activate  # macOS/Linux
# .\.venv\\Scripts\\activate  # Windows

# 安装依赖
pip install -r requirements.txt

# 安装开发依赖
pip install pytest pytest-cov black ruff

# 运行测试
pytest tests/test_core.py -v

# 代码格式化
black .

# 代码检查
ruff check .
```

### 添加新动作

1. 在 `actions/` 目录创建新文件，例如 `my_action.py`
2. 继承 `BaseAction` 并实现 `execute()` 方法
3. 定义 `action_type`, `display_name`, `description` 类属性
4. 实现 `get_required_params()` 和 `get_optional_params()` 方法

```python
from core.base_action import BaseAction, ActionResult
from typing import Any, Dict, List

class MyAction(BaseAction):
    action_type = "my_action"
    display_name = "我的动作"
    description = "这是一个示例动作"
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        # Your action logic here
        return ActionResult(success=True, message="操作成功")
    
    def get_required_params(self) -> List[str]:
        return ['param1']
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {'param2': 'default_value'}
```

### 运行测试

```bash
# 运行所有测试
pytest tests/ -v

# 运行带覆盖率的测试
pytest tests/ --cov=. --cov-report=html

# 运行特定测试文件
pytest tests/test_core.py -v
```

### 代码规范

- 遵循 PEP 8 规范
- 使用 type hints 标注函数签名
- 所有公开方法需要 docstring
- 使用 black 格式化代码 (line-length: 100)
- 使用 ruff 进行代码检查

## 📄 许可证

本项目基于 MIT 许可证开源。详见 [LICENSE](LICENSE) 文件。

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

---

**Made with ❤️ by RabAI Team**
