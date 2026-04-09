"""
Theme system for RabAI AutoClick GUI.
Provides ThemeConfig dataclass, built-in themes, and ThemeManager.
"""

import json
import os
from dataclasses import dataclass, field
from typing import Dict, Any, Optional

try:
    from PyQt5.QtWidgets import QApplication
    from PyQt5.QtCore import QObject, pyqtSignal
    PYQT_AVAILABLE = True
except ImportError:
    PYQT_AVAILABLE = False
    QObject = object
    pyqtSignal = None


@dataclass
class ThemeColors:
    """Color palette for a theme."""
    # Primary colors
    primary: str = "#2196F3"
    primary_hover: str = "#1E88E5"
    primary_pressed: str = "#1976D2"
    
    # Background colors
    bg_main: str = "#FFFFFF"
    bg_secondary: str = "#F5F5F5"
    bg_tertiary: str = "#EEEEEE"
    bg_input: str = "#FFFFFF"
    
    # Text colors
    text_primary: str = "#212121"
    text_secondary: str = "#757575"
    text_disabled: str = "#BDBDBD"
    text_on_primary: str = "#FFFFFF"
    
    # Status colors
    success: str = "#4CAF50"
    warning: str = "#FF9800"
    error: str = "#F44336"
    info: str = "#2196F3"
    
    # Border colors
    border: str = "#E0E0E0"
    border_focus: str = "#2196F3"
    
    # Special elements
    toolbar_bg: str = "#FAFAFA"
    splitter: str = "#CCCCCC"
    highlight: str = "#E3F2FD"
    selected: str = "#BBDEFB"


@dataclass
class ThemeFonts:
    """Font configuration for a theme."""
    family: str = "Microsoft YaHei, Segoe UI, Arial, sans-serif"
    size_base: int = 10
    size_small: int = 9
    size_large: int = 11
    size_title: int = 12
    
    monospace: str = "Consolas, Monaco, monospace"
    monospace_size: int = 11


@dataclass
class ThemeSpacing:
    """Spacing configuration for a theme."""
    padding_small: int = 4
    padding_medium: int = 8
    padding_large: int = 12
    margin_small: int = 4
    margin_medium: int = 8
    margin_large: int = 16
    border_radius: int = 4


@dataclass
class ThemeConfig:
    """Complete theme configuration."""
    name: str = "light"
    display_name: str = "Light"
    colors: ThemeColors = field(default_factory=ThemeColors)
    fonts: ThemeFonts = field(default_factory=ThemeFonts)
    spacing: ThemeSpacing = field(default_factory=ThemeSpacing)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ThemeConfig":
        """Create ThemeConfig from dictionary."""
        colors = ThemeColors(**data.get('colors', {}))
        fonts = ThemeFonts(**data.get('fonts', {}))
        spacing = ThemeSpacing(**data.get('spacing', {}))
        return cls(
            name=data.get('name', 'custom'),
            display_name=data.get('display_name', 'Custom'),
            colors=colors,
            fonts=fonts,
            spacing=spacing
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert ThemeConfig to dictionary."""
        return {
            'name': self.name,
            'display_name': self.display_name,
            'colors': {
                'primary': self.colors.primary,
                'primary_hover': self.colors.primary_hover,
                'primary_pressed': self.colors.primary_pressed,
                'bg_main': self.colors.bg_main,
                'bg_secondary': self.colors.bg_secondary,
                'bg_tertiary': self.colors.bg_tertiary,
                'bg_input': self.colors.bg_input,
                'text_primary': self.colors.text_primary,
                'text_secondary': self.colors.text_secondary,
                'text_disabled': self.colors.text_disabled,
                'text_on_primary': self.colors.text_on_primary,
                'success': self.colors.success,
                'warning': self.colors.warning,
                'error': self.colors.error,
                'info': self.colors.info,
                'border': self.colors.border,
                'border_focus': self.colors.border_focus,
                'toolbar_bg': self.colors.toolbar_bg,
                'splitter': self.colors.splitter,
                'highlight': self.colors.highlight,
                'selected': self.colors.selected,
            },
            'fonts': {
                'family': self.fonts.family,
                'size_base': self.fonts.size_base,
                'size_small': self.fonts.size_small,
                'size_large': self.fonts.size_large,
                'size_title': self.fonts.size_title,
                'monospace': self.fonts.monospace,
                'monospace_size': self.fonts.monospace_size,
            },
            'spacing': {
                'padding_small': self.spacing.padding_small,
                'padding_medium': self.spacing.padding_medium,
                'padding_large': self.spacing.padding_large,
                'margin_small': self.spacing.margin_small,
                'margin_medium': self.spacing.margin_medium,
                'margin_large': self.spacing.margin_large,
                'border_radius': self.spacing.border_radius,
            }
        }


# Built-in themes
LIGHT_THEME = ThemeConfig(
    name="light",
    display_name="Light",
    colors=ThemeColors()
)

DARK_THEME = ThemeConfig(
    name="dark",
    display_name="Dark",
    colors=ThemeColors(
        primary="#64B5F6",
        primary_hover="#42A5F5",
        primary_pressed="#1E88E5",
        bg_main="#1E1E1E",
        bg_secondary="#252525",
        bg_tertiary="#2D2D2D",
        bg_input="#333333",
        text_primary="#E0E0E0",
        text_secondary="#9E9E9E",
        text_disabled="#616161",
        text_on_primary="#FFFFFF",
        success="#81C784",
        warning="#FFB74D",
        error="#E57373",
        info="#64B5F6",
        border="#424242",
        border_focus="#64B5F6",
        toolbar_bg="#2D2D2D",
        splitter="#424242",
        highlight="#1E3A5F",
        selected="#1565C0"
    ),
    fonts=ThemeFonts(
        family="Microsoft YaHei, Segoe UI, Arial, sans-serif",
        size_base=10,
        size_small=9,
        size_large=11,
        size_title=12
    ),
    spacing=ThemeSpacing(
        padding_small=4,
        padding_medium=8,
        padding_large=12,
        margin_small=4,
        margin_medium=8,
        margin_large=16,
        border_radius=4
    )
)

HIGH_CONTRAST_THEME = ThemeConfig(
    name="high_contrast",
    display_name="High Contrast",
    colors=ThemeColors(
        primary="#FFFF00",
        primary_hover="#FFEB3B",
        primary_pressed="#FFC107",
        bg_main="#000000",
        bg_secondary="#121212",
        bg_tertiary="#1E1E1E",
        bg_input="#000000",
        text_primary="#FFFFFF",
        text_secondary="#E0E0E0",
        text_disabled="#808080",
        text_on_primary="#000000",
        success="#00FF00",
        warning="#FFA500",
        error="#FF0000",
        info="#00FFFF",
        border="#FFFFFF",
        border_focus="#FFFF00",
        toolbar_bg="#1A1A1A",
        splitter="#FFFFFF",
        highlight="#333300",
        selected="#665500"
    ),
    fonts=ThemeFonts(
        family="Segoe UI, Arial, sans-serif",
        size_base=11,
        size_small=10,
        size_large=12,
        size_title=13
    ),
    spacing=ThemeSpacing(
        padding_small=6,
        padding_medium=10,
        padding_large=14,
        margin_small=6,
        margin_medium=10,
        margin_large=18,
        border_radius=2
    )
)


BUILT_IN_THEMES = {
    "light": LIGHT_THEME,
    "dark": DARK_THEME,
    "high_contrast": HIGH_CONTRAST_THEME
}


class ThemeManager(QObject if PYQT_AVAILABLE else object):
    """
    Manages theme loading, switching, and application.
    Supports built-in themes and custom themes via JSON.
    """
    
    if PYQT_AVAILABLE:
        theme_changed = pyqtSignal(str)
    else:
        theme_changed = None
    
    def __init__(self, parent=None):
        if PYQT_AVAILABLE:
            super().__init__(parent)
        
        self._current_theme_name = "light"
        self._current_theme = LIGHT_THEME
        self._custom_themes: Dict[str, ThemeConfig] = {}
        self._themes_dir = self._get_themes_dir()
    
    def _get_themes_dir(self) -> str:
        """Get the directory for custom themes."""
        if PYQT_AVAILABLE:
            app = QApplication.instance()
            if app:
                config_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                themes_dir = os.path.join(config_path, 'themes')
                os.makedirs(themes_dir, exist_ok=True)
                return themes_dir
        return ""
    
    def get_theme(self, name: str) -> Optional[ThemeConfig]:
        """Get a theme by name (built-in or custom)."""
        if name in BUILT_IN_THEMES:
            return BUILT_IN_THEMES[name]
        if name in self._custom_themes:
            return self._custom_themes[name]
        return None
    
    def get_current_theme(self) -> ThemeConfig:
        """Get the currently active theme."""
        return self._current_theme
    
    def get_current_theme_name(self) -> str:
        """Get the name of the current theme."""
        return self._current_theme_name
    
    def list_themes(self) -> Dict[str, str]:
        """List all available themes with their display names."""
        themes = {name: theme.display_name for name, theme in BUILT_IN_THEMES.items()}
        themes.update({name: theme.display_name for name, theme in self._custom_themes.items()})
        return themes
    
    def apply_theme(self, theme_name: str) -> bool:
        """
        Apply a theme by name.
        Returns True if successful, False otherwise.
        """
        theme = self.get_theme(theme_name)
        if theme is None:
            return False
        
        self._current_theme_name = theme_name
        self._current_theme = theme
        
        if PYQT_AVAILABLE:
            self._apply_theme_to_app(theme)
            if self.theme_changed is not None:
                self.theme_changed.emit(theme_name)
        
        return True
    
    def _apply_theme_to_app(self, theme: ThemeConfig) -> None:
        """Apply theme stylesheet to the application."""
        if not PYQT_AVAILABLE:
            return
        
        app = QApplication.instance()
        if app is None:
            return
        
        stylesheet = self._generate_stylesheet(theme)
        app.setStyleSheet(stylesheet)
        
        # Set application palette for some native colors
        from PyQt5.QtGui import QPalette, QColor
        from PyQt5.QtCore import Qt
        
        palette = QPalette()
        palette.setColor(QPalette.Window, QColor(theme.colors.bg_main))
        palette.setColor(QPalette.WindowText, QColor(theme.colors.text_primary))
        palette.setColor(QPalette.Base, QColor(theme.colors.bg_input))
        palette.setColor(QPalette.AlternateBase, QColor(theme.colors.bg_secondary))
        palette.setColor(QPalette.ToolTipBase, QColor(theme.colors.bg_main))
        palette.setColor(QPalette.ToolTipText, QColor(theme.colors.text_primary))
        palette.setColor(QPalette.Text, QColor(theme.colors.text_primary))
        palette.setColor(QPalette.Button, QColor(theme.colors.bg_secondary))
        palette.setColor(QPalette.ButtonText, QColor(theme.colors.text_primary))
        palette.setColor(QPalette.BrightText, QColor(theme.colors.text_on_primary))
        palette.setColor(QPalette.Highlight, QColor(theme.colors.selected))
        palette.setColor(QPalette.HighlightedText, QColor(theme.colors.text_on_primary))
        
        app.setPalette(palette)
    
    def _generate_stylesheet(self, theme: ThemeConfig) -> str:
        """Generate the complete stylesheet for a theme."""
        c = theme.colors
        f = theme.fonts
        s = theme.spacing
        
        return f"""
        /* General */
        QWidget {{
            font-family: "{f.family}";
            font-size: {f.size_base}pt;
            color: {c.text_primary};
            background-color: {c.bg_main};
        }}
        
        QMainWindow {{
            background-color: {c.bg_main};
        }}
        
        /* Labels */
        QLabel {{
            color: {c.text_primary};
            background-color: transparent;
        }}
        
        /* Buttons */
        QPushButton {{
            background-color: {c.bg_secondary};
            border: 1px solid {c.border};
            border-radius: {s.border_radius}px;
            padding: {s.padding_small}px {s.padding_medium}px;
            color: {c.text_primary};
        }}
        
        QPushButton:hover {{
            background-color: {c.bg_tertiary};
            border-color: {c.border_focus};
        }}
        
        QPushButton:pressed {{
            background-color: {c.bg_tertiary};
            border-color: {c.primary_pressed};
        }}
        
        QPushButton:disabled {{
            color: {c.text_disabled};
            background-color: {c.bg_secondary};
        }}
        
        /* Primary button */
        QPushButton[class="primary"] {{
            background-color: {c.primary};
            border-color: {c.primary};
            color: {c.text_on_primary};
        }}
        
        QPushButton[class="primary"]:hover {{
            background-color: {c.primary_hover};
            border-color: {c.primary_hover};
        }}
        
        QPushButton[class="primary"]:pressed {{
            background-color: {c.primary_pressed};
            border-color: {c.primary_pressed};
        }}
        
        /* Inputs */
        QLineEdit, QSpinBox, QDoubleSpinBox, QComboBox, QTextEdit {{
            background-color: {c.bg_input};
            border: 1px solid {c.border};
            border-radius: {s.border_radius}px;
            padding: {s.padding_small}px;
            color: {c.text_primary};
        }}
        
        QLineEdit:focus, QSpinBox:focus, QDoubleSpinBox:focus, 
        QComboBox:focus, QTextEdit:focus {{
            border-color: {c.border_focus};
        }}
        
        QLineEdit:disabled, QSpinBox:disabled, QDoubleSpinBox:disabled,
        QComboBox:disabled, QTextEdit:disabled {{
            background-color: {c.bg_secondary};
            color: {c.text_disabled};
        }}
        
        /* ComboBox dropdown */
        QComboBox::drop-down {{
            border: none;
            border-radius: {s.border_radius}px;
        }}
        
        QComboBox::down-arrow {{
            width: 12px;
            height: 12px;
        }}
        
        /* Checkbox and Radio */
        QCheckBox, QRadioButton {{
            spacing: {s.padding_small}px;
            color: {c.text_primary};
        }}
        
        QCheckBox::indicator, QRadioButton::indicator {{
            width: 16px;
            height: 16px;
            border: 1px solid {c.border};
            border-radius: {s.border_radius}px;
            background-color: {c.bg_input};
        }}
        
        QRadioButton::indicator {{
            border-radius: 8px;
        }}
        
        QCheckBox::indicator:checked {{
            background-color: {c.primary};
            border-color: {c.primary};
        }}
        
        QRadioButton::indicator:checked {{
            background-color: {c.primary};
            border-color: {c.primary};
        }}
        
        /* Groups */
        QGroupBox {{
            font-size: {f.size_large}pt;
            font-weight: bold;
            border: 1px solid {c.border};
            border-radius: {s.border_radius}px;
            margin-top: {s.margin_medium}px;
            padding-top: {s.margin_medium}px;
            background-color: {c.bg_secondary};
        }}
        
        QGroupBox::title {{
            subcontrol-origin: margin;
            subcontrol-position: top left;
            left: {s.margin_medium}px;
            padding: 0 {s.padding_small}px;
            color: {c.text_primary};
        }}
        
        /* Tab Widget */
        QTabWidget::pane {{
            border: 1px solid {c.border};
            border-radius: {s.border_radius}px;
            background-color: {c.bg_main};
        }}
        
        QTabBar::tab {{
            background-color: {c.bg_secondary};
            border: 1px solid {c.border};
            border-bottom: none;
            border-top-left-radius: {s.border_radius}px;
            border-top-right-radius: {s.border_radius}px;
            padding: {s.padding_small}px {s.padding_medium}px;
            margin-right: 2px;
            color: {c.text_secondary};
        }}
        
        QTabBar::tab:selected {{
            background-color: {c.bg_main};
            color: {c.primary};
            border-color: {c.border};
            border-bottom-color: {c.bg_main};
        }}
        
        QTabBar::tab:hover {{
            background-color: {c.bg_tertiary};
            color: {c.text_primary};
        }}
        
        /* Lists */
        QListWidget, QTreeWidget, QTableWidget {{
            background-color: {c.bg_input};
            border: 1px solid {c.border};
            border-radius: {s.border_radius}px;
            color: {c.text_primary};
            gridline-color: {c.border};
        }}
        
        QListWidget::item:selected, QTreeWidget::item:selected, 
        QTableWidget::item:selected {{
            background-color: {c.selected};
            color: {c.text_on_primary};
        }}
        
        QListWidget::item:hover, QTreeWidget::item:hover,
        QTableWidget::item:hover {{
            background-color: {c.highlight};
        }}
        
        /* Header */
        QHeaderView::section {{
            background-color: {c.bg_secondary};
            border: 1px solid {c.border};
            padding: {s.padding_small}px;
            color: {c.text_primary};
        }}
        
        /* Scrollbar */
        QScrollBar:vertical {{
            border: none;
            background: {c.bg_secondary};
            width: 12px;
            margin: 0px;
        }}
        
        QScrollBar::handle:vertical {{
            background: {c.border};
            border-radius: 6px;
            min-height: 20px;
        }}
        
        QScrollBar::handle:vertical:hover {{
            background: {c.text_secondary};
        }}
        
        QScrollBar:horizontal {{
            border: none;
            background: {c.bg_secondary};
            height: 12px;
            margin: 0px;
        }}
        
        QScrollBar::handle:horizontal {{
            background: {c.border};
            border-radius: 6px;
            min-width: 20px;
        }}
        
        QScrollBar::handle:horizontal:hover {{
            background: {c.text_secondary};
        }}
        
        /* Toolbars */
        QToolBar {{
            background-color: {c.toolbar_bg};
            border: none;
            spacing: {s.spacing}px;
            padding: {s.padding_small}px;
        }}
        
        QToolBar::separator {{
            background-color: {c.splitter};
            width: 1px;
        }}
        
        /* Status Bar */
        QStatusBar {{
            background-color: {c.bg_secondary};
            color: {c.text_secondary};
            border-top: 1px solid {c.border};
        }}
        
        /* Splitter */
        QSplitter::handle {{
            background-color: {c.splitter};
        }}
        
        /* Menu */
        QMenu {{
            background-color: {c.bg_secondary};
            border: 1px solid {c.border};
            border-radius: {s.border_radius}px;
            padding: {s.padding_small}px;
        }}
        
        QMenu::item {{
            padding: {s.padding_small}px {s.padding_medium}px;
            border-radius: {s.border_radius}px;
        }}
        
        QMenu::item:selected {{
            background-color: {c.selected};
        }}
        
        QMenuBar {{
            background-color: {c.toolbar_bg};
            border-bottom: 1px solid {c.border};
        }}
        
        QMenuBar::item {{
            padding: {s.padding_small}px {s.padding_medium}px;
        }}
        
        QMenuBar::item:selected {{
            background-color: {c.bg_tertiary};
        }}
        
        /* Progress Bar */
        QProgressBar {{
            border: 1px solid {c.border};
            border-radius: {s.border_radius}px;
            background-color: {c.bg_secondary};
            text-align: center;
        }}
        
        QProgressBar::chunk {{
            background-color: {c.primary};
            border-radius: {s.border_radius}px;
        }}
        
        /* Tool Tip */
        QToolTip {{
            background-color: {c.bg_secondary};
            border: 1px solid {c.border};
            border-radius: {s.border_radius}px;
            padding: {s.padding_small}px;
            color: {c.text_primary};
        }}
        
        /* Dialogs */
        QDialog {{
            background-color: {c.bg_main};
        }}
        
        /* Frames */
        QFrame[class="card"] {{
            background-color: {c.bg_secondary};
            border: 1px solid {c.border};
            border-radius: {s.border_radius}px;
            padding: {s.padding_medium}px;
        }}
        
        /* Special utility classes */
        QLabel[class="title"] {{
            font-size: {f.size_title}pt;
            font-weight: bold;
            color: {c.text_primary};
        }}
        
        QLabel[class="subtitle"] {{
            font-size: {f.size_large}pt;
            color: {c.text_secondary};
        }}
        
        QLabel[class="success"] {{
            color: {c.success};
        }}
        
        QLabel[class="warning"] {{
            color: {c.warning};
        }}
        
        QLabel[class="error"] {{
            color: {c.error};
        }}
        """
    
    def load_theme_from_json(self, filepath: str) -> Optional[ThemeConfig]:
        """Load a custom theme from a JSON file."""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            theme = ThemeConfig.from_dict(data)
            self._custom_themes[theme.name] = theme
            
            # Save to themes directory if not already there
            if self._themes_dir:
                dest_path = os.path.join(self._themes_dir, f"{theme.name}.json")
                if not os.path.exists(dest_path):
                    self.save_theme_to_json(theme.name, dest_path)
            
            return theme
        except Exception as e:
            import logging
            logging.getLogger('RabAI').error(f"Failed to load theme from {filepath}: {e}")
            return None
    
    def save_theme_to_json(self, theme_name: str, filepath: str) -> bool:
        """Save a theme to a JSON file."""
        theme = self.get_theme(theme_name)
        if theme is None:
            return False
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(theme.to_dict(), f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            import logging
            logging.getLogger('RabAI').error(f"Failed to save theme to {filepath}: {e}")
            return False
    
    def load_all_custom_themes(self) -> None:
        """Load all custom themes from the themes directory."""
        if not self._themes_dir or not os.path.exists(self._themes_dir):
            return
        
        for filename in os.listdir(self._themes_dir):
            if filename.endswith('.json'):
                filepath = os.path.join(self._themes_dir, filename)
                self.load_theme_from_json(filepath)
    
    def delete_custom_theme(self, theme_name: str) -> bool:
        """Delete a custom theme."""
        if theme_name not in self._custom_themes:
            return False
        
        del self._custom_themes[theme_name]
        
        # Also delete the file if it exists
        if self._themes_dir:
            filepath = os.path.join(self._themes_dir, f"{theme_name}.json")
            if os.path.exists(filepath):
                os.remove(filepath)
        
        return True


# Global theme manager instance
_theme_manager = None

def get_theme_manager() -> ThemeManager:
    """Get the global theme manager instance."""
    global _theme_manager
    if _theme_manager is None:
        _theme_manager = ThemeManager()
        _theme_manager.load_all_custom_themes()
    return _theme_manager


# PyQt signal for theme changes
# Note: Signal is defined as class attribute above
