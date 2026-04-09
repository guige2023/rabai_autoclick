# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.4.0] - 2025-04 (Unreleased)

### Added
- **Self-Healing System** (`src/self_healing_system.py`)
  - Automatic retry mechanism for failed actions
  - Recovery strategy execution
  - Failure detection and notification
- **Predictive Engine** (`src/predictive_engine.py`)
  - Action prediction for improved efficiency
  - Pre-execution of likely next actions
  - Performance optimization
- **Workflow Diagnostics** (`src/workflow_diagnostics.py`)
  - Real-time workflow execution monitoring
  - Performance metrics tracking
  - Error reporting and analysis
- **Pipeline Mode** (`src/pipeline_mode.py`)
  - Advanced workflow pipeline support
  - Data flow between actions
  - Parallel execution support
- **Screen Recorder** (`src/screen_recorder.py`)
  - Record workflow execution
  - Debug and replay functionality

### Changed
- Updated README with comprehensive documentation
- Improved action type reference (34+ action types documented)

### Fixed
- Various bug fixes and stability improvements

---

## [2.3.0] - 2025-03

### Added
- **按键显示功能**: 实时显示鼠标位置、按键内容、鼠标点击
- 独立窗口模式，主窗口最小化也能正常显示
- 显示快捷键（默认 Ctrl+F11）

### Fixed
- macOS 上快捷键配置闪退问题
- pynput 监听器与 Qt 的线程冲突问题
- 执行完成后主窗口不自动弹出的问题
- 快捷键停止录屏后主窗口不弹出的问题

### Optimized
- 快捷键管理器，避免重复启动监听器
- 使用 subprocess 隔离按键显示进程，提高稳定性

---

## [2.2.0] - 2025-03

### Added
- **操作录制功能**: 一键录制鼠标点击、双击、滚轮、键盘操作
- 鼠标双击动作类型
- 全局快捷键支持（使用 pynput）
- 清空步骤按钮
- 快捷键设置支持录制快捷键配置

### Fixed
- macOS 上 keyboard 模块崩溃问题
- 窗口选择列表为空的问题
- 区域框选不透明的问题
- 录制停止快捷键被记录的问题
- 添加到工作流后动作类型为 None 的问题

### Optimized
- 录制时自动最小化主窗口
- 快捷键顺序整理（cmd+c 而非 c+cmd）

---

## [2.1.0] - 2025-02

### Added
- 流程控制动作（条件判断、循环、标签跳转）
- 异常处理动作（try_catch, throw, assert）
- 变量系统支持
- 执行脚本功能

### Fixed
- 图像识别在多显示器环境下的问题

---

## [2.0.0] - 2025-02

### Added
- 可视化编辑器 (PyQt5)
- 窗口和区域选择功能
- 循环执行配置
- 内存优化功能
- 执行统计功能

### Changed
- 从 CLI 工具升级为 GUI 工具
- 全新设计的用户界面

---

## [1.3.0] - 2025-02

### Added
- 窗口选择和区域框选功能
- 循环执行配置
- 内存优化功能
- 执行统计功能
- OCR参数改为下拉选择器

### Fixed
- 停止工作流不生效问题
- OCR结果截断问题

### Optimized
- OCR识别速度

---

## [1.0.0] - 2024-01

### Added
- 初始版本发布
- 核心流程引擎
- 基础动作插件

---

## Breaking Changes

### v2.0.0
- 从 CLI 模式升级到 GUI 模式
- 工作流格式可能需要重新保存

### v1.0.0
- Initial release - no breaking changes
