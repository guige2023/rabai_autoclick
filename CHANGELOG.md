# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.3.0] - 2026-03-06

### Added
- Key display feature: Real-time display of mouse position, keys pressed, and mouse clicks
- Independent window mode for key display (works when main window is minimized)
- Display hotkey shortcut (default Ctrl+F11)
- Process isolation for key display using subprocess for better stability

### Fixed
- macOS shortcut configuration crash issue
- pynput listener thread conflict with Qt
- Main window not auto-appearing after execution completes
- Main window not appearing after stopping recording via shortcut
- Shortcut manager optimization to avoid duplicate listener startup

### Changed
- Shortcut order optimization (cmd+c instead of c+cmd)

## [2.2.0] - 2025-03

### Added
- Operation recording feature: One-click recording of mouse clicks, double-clicks, scroll, keyboard operations
- Mouse double-click action type
- Global shortcut support (using pynput)
- Clear steps button
- Shortcut settings support for recording shortcuts configuration
- Smart optimization when recording completes:
  - Merge consecutive text inputs into complete strings
  - Remove short delays (<0.1s)
  - Organize shortcut order

### Fixed
- macOS keyboard module crash issue
- Window selection list being empty
- Region selection not opaque
- Recording stop shortcut being recorded in action list
- Action type being None after adding to workflow

## [2.1.0] - 2025-02

### Added
- Window selection and region selection features
- Loop execution configuration
- Memory optimization feature
- Execution statistics feature
- OCR parameters changed to dropdown selectors

### Fixed
- Workflow stop not taking effect
- OCR result truncation issue
- OCR recognition speed optimization

## [2.0.0] - 2025-01

### Added
- New v2 core engine
- PyQt5-based GUI
- CLI interface
- Advanced features:
  - Predictive automation engine
  - Self-healing system
  - Scene-based workflow packages
  - Enhanced diagnostics
  - Workflow sharing system
  - CLI pipeline integration
  - Screen recording to workflow conversion

## [1.3.0] - 2025-02

### Added
- Window selection and region selection
- Loop execution configuration
- Memory optimization
- Execution statistics

### Fixed
- Stop workflow not working
- OCR result truncation

## [1.0.0] - 2024-01

### Added
- Initial release
- Core flow engine
- Basic action plugins
