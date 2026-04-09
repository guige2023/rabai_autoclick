# RabAI AutoClick v2.4

<div align="center">

![Version](https://img.shields.io/badge/version-2.4.0-blue.svg)
![Python](https://img.shields.io/badge/Python-3.8+-green.svg)
![Platform](https://img.shields.io/badge/Platform-Windows%20%7C%20macOS-lightgrey.svg)
![License](https://img.shields.io/badge/License-MIT-yellow.svg)
[![GitHub stars](https://img.shields.io/github/stars/guige2023/rabai_autoclick)](https://github.com/guige2023/rabai_autoclick/stargazers)
[![GitHub forks](https://img.shields.io/github/forks/guige2023/rabai_autoclick)](https://github.com/guige2023/rabai_autoclick/network)
[![GitHub issues](https://img.shields.io/github/issues/guige2023/rabai_autoclick)](https://github.com/guige2023/rabai_autoclick/issues)
[![Last commit](https://img.shields.io/github/last-commit/guige2023/rabai_autoclick)](https://github.com/guige2023/rabai_autoclick/commits)

**智能桌面自动化工具 | 操作录制 | OCR文字识别 | 可视化编辑器 | 跨平台支持**

[功能特性](#功能特性) • [快速开始](#快速开始) • [架构设计](#架构设计) • [动作类型](#动作类型参考) • [开发指南](#开发指南) • [更新日志](../CHANGELOG.md)

</div>

---

## 📖 目录

- [功能特性](#功能特性)
- [快速开始](#快速开始)
- [架构设计](#架构设计)
- [动作类型参考](#动作类型参考)
- [开发指南](#开发指南)
- [常见问题](#常见问题)

---

## 功能特性

### 🎯 核心功能

| 功能模块 | 描述 |
|---------|------|
| **🎥 操作录制** | 一键录制鼠标点击、双击、滚轮、键盘操作，自动生成工作流 |
| **📝 可视化编辑器** | PyQt5 拖拽式界面，无需编程即可创建自动化流程 |
| **🔍 OCR文字识别** | 支持中英文识别，智能点击指定文字位置 |
| **🖼️ 图像匹配** | 模板匹配定位，支持置信度配置 |
| **🔄 循环执行** | 支持设置循环次数和间隔时间 |
| **⌨️ 全局快捷键** | 运行/停止/录制快捷键，窗口最小化也能响应 |
| **🔧 自愈系统** | 动作失败时自动尝试恢复策略 |
| **📊 预测引擎** | 智能预测下一个动作，优化执行效率 |
| **💊 流程诊断** | 实时监控和诊断工作流执行状态 |

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
| **文本输入** | 模拟键盘输入文本 | `text`: 输入内容<br>`enter_after`: 输入后按回车 |
| **按键操作** | 按下特定按键或组合键 | `keys`: 组合键如 ['ctrl', 'c'] |

### 🔍 图像识别

| 动作类型 | 功能描述 | 参数说明 |
|---------|---------|---------|
| **图像识别点击** | 通过模板匹配定位并点击 | `template`: 模板图片路径<br>`confidence`: 匹配置信度 |
| **查找图像** | 查找屏幕上的图像位置 | `find_all`: 查找所有匹配 |

### 🔄 流程控制

| 动作类型 | 功能描述 |
|---------|---------|
| **循环** | 设置循环次数执行动作 |
| **条件判断** | 根据条件分支执行 |
| **标签/跳转** | 流程标签和跳转 |
| **等待/延时** | 延时执行 |

### 🛡️ 自愈与诊断

| 功能 | 描述 |
|------|------|
| **自愈系统** | 动作失败时自动重试和恢复 |
| **预测引擎** | 预判下一个动作减少延迟 |
| **流程诊断** | 实时监控执行状态 |

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

## 架构设计

```
rabai_autoclick/
├── actions/          # 动作实现模块 (34+ 动作类型)
│   ├── click.py       # 基础点击动作
│   ├── mouse.py       # 鼠标相关动作
│   ├── keyboard.py    # 键盘相关动作
│   ├── ocr.py         # OCR识别动作
│   ├── image_match.py # 图像匹配动作
│   ├── script.py      # 脚本和流程控制
│   ├── loop_while.py  # 循环控制
│   ├── try_catch.py   # 异常处理
│   ├── wait_for.py    # 等待元素/图像/文字
│   ├── system.py      # 系统操作
│   └── comment.py     # 注释和标签
├── cli/               # 命令行接口
├── core/              # 核心引擎
│   ├── engine.py      # 执行引擎
│   ├── context.py     # 执行上下文
│   └── action_loader.py # 动作加载器
├── gui/               # GUI组件
├── src/               # 高级功能
│   ├── self_healing_system.py # 自愈系统
│   ├── predictive_engine.py    # 预测引擎
│   ├── workflow_diagnostics.py # 流程诊断
│   ├── pipeline_mode.py        # 管道模式
│   └── screen_recorder.py      # 屏幕录制
├── tests/             # 测试套件
├── ui/                # PyQt5 UI组件
├── utils/             # 工具模块
├── main.py            # 主入口
└── pyproject.toml     # 项目配置
```

详细架构设计请参阅 [ARCHITECTURE.md](ARCHITECTURE.md)。

---

## 动作类型参考

RabAI AutoClick 支持 **34+ 动作类型**，分为以下类别：

### 🖱️ 鼠标动作 (5)
| 动作ID | 显示名称 | 描述 |
|--------|---------|------|
| `click` | 鼠标单击 | 在指定坐标执行单击 |
| `mouse_click` | 鼠标单击(高级) | 支持多按钮和多次点击 |
| `double_click` | 鼠标双击 | 执行双击操作 |
| `scroll` | 滚轮滚动 | 模拟滚轮滚动 |
| `mouse_move` | 鼠标移动 | 移动到指定位置 |
| `drag` | 鼠标拖拽 | 从起点拖拽到终点 |

### ⌨️ 键盘动作 (2)
| 动作ID | 显示名称 | 描述 |
|--------|---------|------|
| `type_text` | 文本输入 | 输入文本内容 |
| `key_press` | 按键操作 | 按下组合键 |

### 🔍 图像与OCR (4)
| 动作ID | 显示名称 | 描述 |
|--------|---------|------|
| `click_image` | 图像识别点击 | 模板匹配后点击 |
| `find_image` | 查找图像 | 查找图像位置 |
| `ocr` | OCR文字识别 | 识别区域文字 |
| `screenshot` | 屏幕截图 | 截取屏幕 |

### 🔄 流程控制 (10)
| 动作ID | 显示名称 | 描述 |
|--------|---------|------|
| `loop` | 循环 | 循环执行指定次数 |
| `loop_while` | 条件循环 | 满足条件时循环 |
| `loop_while_break` | 跳出循环 | 跳出当前循环 |
| `loop_while_continue` | 继续循环 | 跳到下次循环 |
| `for_each` | 遍历循环 | 遍历列表元素 |
| `condition` | 条件判断 | 根据条件分支 |
| `try_catch` | 异常捕获 | 捕获并处理异常 |
| `throw` | 抛出异常 | 主动抛出异常 |
| `assert` | 断言 | 条件断言 |
| `delay` | 延时 | 等待指定时间 |

### 🏷️ 标签与跳转 (4)
| 动作ID | 显示名称 | 描述 |
|--------|---------|------|
| `label` | 标签 | 定义跳转目标 |
| `goto` | 跳转 | 跳转到标签 |
| `comment` | 注释 | 添加注释说明 |
| `log` | 日志 | 输出日志信息 |

### ⏳ 等待动作 (3)
| 动作ID | 显示名称 | 描述 |
|--------|---------|------|
| `wait_for_image` | 等待图像 | 等待图像出现 |
| `wait_for_text` | 等待文字 | 等待文字出现 |
| `wait_for_element` | 等待元素 | 等待元素出现 |

### 🔧 系统动作 (5)
| 动作ID | 显示名称 | 描述 |
|--------|---------|------|
| `get_mouse_pos` | 获取鼠标位置 | 获取当前鼠标坐标 |
| `alert` | 弹窗提示 | 显示消息框 |
| `set_variable` | 设置变量 | 设置变量值 |
| `script` | 执行脚本 | 执行Python脚本 |
| `rethrow` | 重新抛出 | 重新抛出异常 |

---

## 开发指南

### 项目结构

```
rabai_autoclick/
├── actions/          # Action implementations (34+ action types)
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

详细贡献指南请参阅 [CONTRIBUTING.md](CONTRIBUTING.md)。

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

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

详细贡献指南请参阅 [CONTRIBUTING.md](CONTRIBUTING.md)。

---

**Made with ❤️ by RabAI Team**
