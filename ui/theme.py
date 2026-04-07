"""Theme management for RabAI AutoClick UI.

Provides centralized theme management with light/dark mode support,
consistent styling, animations, and persistent theme preferences.
"""

import json
import os
from enum import Enum
from typing import Dict, Optional

from PyQt5.QtCore import QObject, pyqtSignal, QSettings


class ThemeType(Enum):
    """Available theme types."""
    LIGHT = "light"
    DARK = "dark"


# Theme persistence file path
_THEME_CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'theme_config.json')


class ThemeColors:
    """Color palette for themes."""

    # Light theme colors
    LIGHT = {
        # Primary colors
        "primary": "#2196F3",
        "primary_hover": "#1E88E5",
        "primary_active": "#1976D2",
        "success": "#4CAF50",
        "success_hover": "#45a049",
        "warning": "#FF9800",
        "warning_hover": "#FB8C00",
        "error": "#f44336",
        "error_hover": "#da190b",

        # Accent colors
        "accent": "#00BCD4",
        "accent_hover": "#00ACC1",
        "info": "#03A9F4",
        "info_hover": "#039BE5",

        # Background colors
        "bg_main": "#f5f5f5",
        "bg_widget": "#ffffff",
        "bg_toolbar": "#e0e0e0",
        "bg_panel": "#fafafa",
        "bg_hover": "#e8e8e8",
        "bg_active": "#d8d8d8",
        "bg_tooltip": "#ffffde",

        # Dark theme specific backgrounds
        "bg_dark_main": "#2d2d2d",
        "bg_dark_widget": "#3d3d3d",
        "bg_dark_toolbar": "#252525",
        "bg_dark_hover": "#4d4d4d",
        "bg_dark_active": "#5d5d5d",

        # Text colors
        "text_primary": "#212121",
        "text_secondary": "#757575",
        "text_disabled": "#9e9e9e",
        "text_on_primary": "#ffffff",

        # Dark theme text
        "text_dark_primary": "#ffffff",
        "text_dark_secondary": "#d4d4d4",
        "text_dark_disabled": "#888888",

        # Border colors
        "border": "#ddd",
        "border_focus": "#2196F3",
        "border_dark": "#555",

        # Log colors
        "log_debug": "#888888",
        "log_info": "#4fc3f7",
        "log_success": "#81c784",
        "log_warning": "#ffb74d",
        "log_error": "#e57373",
        "log_critical": "#f44336",

        # Status bar / dark widgets
        "status_bar": "#1e1e1e",
        "status_text": "#d4d4d4",

        # Selection highlight
        "selection": "#bbdefb",
        "selection_hover": "#90caf9",

        # Shadow
        "shadow": "rgba(0, 0, 0, 0.15)",
        "shadow_dark": "rgba(0, 0, 0, 0.3)",
    }

    # Dark theme colors (same keys, different values)
    DARK = {
        # Primary colors - slightly brighter for dark mode
        "primary": "#64B5F6",
        "primary_hover": "#90CAF9",
        "primary_active": "#42A5F5",
        "success": "#81C784",
        "success_hover": "#A5D6A7",
        "warning": "#FFB74D",
        "warning_hover": "#FFCC80",
        "error": "#EF9A9A",
        "error_hover": "#FFABAB",

        # Accent colors
        "accent": "#4DD0E1",
        "accent_hover": "#80DEEA",
        "info": "#29B6F6",
        "info_hover": "#4FC3F7",

        # Background colors - dark
        "bg_main": "#1e1e1e",
        "bg_widget": "#2d2d2d",
        "bg_toolbar": "#252525",
        "bg_panel": "#333333",
        "bg_hover": "#3d3d3d",
        "bg_active": "#4d4d4d",
        "bg_tooltip": "#4d4d4d",

        # Dark theme specific backgrounds (same as bg_* for dark)
        "bg_dark_main": "#2d2d2d",
        "bg_dark_widget": "#3d3d3d",
        "bg_dark_toolbar": "#252525",
        "bg_dark_hover": "#4d4d4d",
        "bg_dark_active": "#5d5d5d",

        # Text colors - inverted for dark mode
        "text_primary": "#ffffff",
        "text_secondary": "#d4d4d4",
        "text_disabled": "#888888",
        "text_on_primary": "#ffffff",

        # Dark theme text
        "text_dark_primary": "#ffffff",
        "text_dark_secondary": "#d4d4d4",
        "text_dark_disabled": "#888888",

        # Border colors
        "border": "#555555",
        "border_focus": "#64B5F6",
        "border_dark": "#555",

        # Log colors - brighter for dark mode
        "log_debug": "#888888",
        "log_info": "#4fc3f7",
        "log_success": "#81c784",
        "log_warning": "#ffb74d",
        "log_error": "#e57373",
        "log_critical": "#f44336",

        # Status bar / dark widgets
        "status_bar": "#1e1e1e",
        "status_text": "#d4d4d4",

        # Selection highlight
        "selection": "#1e3a5f",
        "selection_hover": "#2e5a8f",

        # Shadow
        "shadow": "rgba(0, 0, 0, 0.4)",
        "shadow_dark": "rgba(0, 0, 0, 0.6)",
    }


class AccentColors:
    """Additional accent color presets for customization."""

    # Ocean Blue
    OCEAN = {
        "primary": "#0077B6",
        "primary_hover": "#00A8E8",
        "primary_active": "#005F8A",
    }

    # Forest Green
    FOREST = {
        "primary": "#2D6A4F",
        "primary_hover": "#40916C",
        "primary_active": "#1B4332",
    }

    # Sunset Orange
    SUNSET = {
        "primary": "#FF6B35",
        "primary_hover": "#FF8C5A",
        "primary_active": "#E55A2B",
    }

    # Purple Rain
    PURPLE = {
        "primary": "#7B2CBF",
        "primary_hover": "#9D4EDD",
        "primary_active": "#5A189A",
    }

    # Rose Pink
    ROSE = {
        "primary": "#E91E63",
        "primary_hover": "#F06292",
        "primary_active": "#C2185B",
    }

    # Midnight
    MIDNIGHT = {
        "primary": "#311B92",
        "primary_hover": "#512DA8",
        "primary_active": "#1A0A6B",
    }

    # Teal
    TEAL = {
        "primary": "#00897B",
        "primary_hover": "#26A69A",
        "primary_active": "#00695C",
    }

    # Amber
    AMBER = {
        "primary": "#FF8F00",
        "primary_hover": "#FFA726",
        "primary_active": "#E65100",
    }

    # Dark theme colors (same keys, different values)
    DARK = {
        # Primary colors - slightly brighter for dark mode
        "primary": "#64B5F6",
        "primary_hover": "#90CAF9",
        "primary_active": "#42A5F5",
        "success": "#81C784",
        "success_hover": "#A5D6A7",
        "warning": "#FFB74D",
        "warning_hover": "#FFCC80",
        "error": "#EF9A9A",
        "error_hover": "#FFABAB",

        # Background colors - dark
        "bg_main": "#1e1e1e",
        "bg_widget": "#2d2d2d",
        "bg_toolbar": "#252525",
        "bg_panel": "#333333",
        "bg_hover": "#3d3d3d",
        "bg_active": "#4d4d4d",
        "bg_tooltip": "#4d4d4d",

        # Dark theme specific backgrounds (same as bg_* for dark)
        "bg_dark_main": "#2d2d2d",
        "bg_dark_widget": "#3d3d3d",
        "bg_dark_toolbar": "#252525",
        "bg_dark_hover": "#4d4d4d",
        "bg_dark_active": "#5d5d5d",

        # Text colors - inverted for dark mode
        "text_primary": "#ffffff",
        "text_secondary": "#d4d4d4",
        "text_disabled": "#888888",
        "text_on_primary": "#ffffff",

        # Dark theme text
        "text_dark_primary": "#ffffff",
        "text_dark_secondary": "#d4d4d4",
        "text_dark_disabled": "#888888",

        # Border colors
        "border": "#555555",
        "border_focus": "#64B5F6",
        "border_dark": "#555",

        # Log colors - brighter for dark mode
        "log_debug": "#888888",
        "log_info": "#4fc3f7",
        "log_success": "#81c784",
        "log_warning": "#ffb74d",
        "log_error": "#e57373",
        "log_critical": "#f44336",

        # Status bar / dark widgets
        "status_bar": "#1e1e1e",
        "status_text": "#d4d4d4",
    }


class ThemeManager(QObject):
    """Centralized theme management singleton.

    Manages theme switching, persistence, and provides theme-aware styling
    with animated transitions for all UI components.
    """

    _instance: Optional['ThemeManager'] = None
    theme_changed = pyqtSignal(ThemeType)

    def __new__(cls) -> 'ThemeManager':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return
        self._initialized = True
        self._current_theme: ThemeType = ThemeType.LIGHT
        self._colors: Dict[str, str] = ThemeColors.LIGHT.copy()
        self._settings = QSettings('RabAI', 'AutoClick')
        self._load_saved_theme()

    def _load_saved_theme(self) -> None:
        """Load saved theme preference from disk."""
        try:
            saved_theme = self._settings.value('theme', 'light')
            if saved_theme == 'dark':
                self._current_theme = ThemeType.DARK
                self._colors = ThemeColors.DARK.copy()
            else:
                self._current_theme = ThemeType.LIGHT
                self._colors = ThemeColors.LIGHT.copy()
        except Exception:
            pass

    def _save_theme(self) -> None:
        """Save current theme preference to disk."""
        try:
            self._settings.setValue('theme', self._current_theme.value)
        except Exception:
            pass

    @property
    def theme(self) -> ThemeType:
        """Get current theme type."""
        return self._current_theme

    @property
    def colors(self) -> Dict[str, str]:
        """Get current theme colors."""
        return self._colors.copy()

    def get_color(self, key: str) -> str:
        """Get a specific color by key.

        Args:
            key: Color key name.

        Returns:
            Hex color string.
        """
        return self._colors.get(key, "#000000")

    def set_theme(self, theme: ThemeType) -> None:
        """Switch to a different theme.

        Args:
            theme: ThemeType to switch to.
        """
        if self._current_theme == theme:
            return

        self._current_theme = theme
        if theme == ThemeType.DARK:
            self._colors = ThemeColors.DARK.copy()
        else:
            self._colors = ThemeColors.LIGHT.copy()

        self._save_theme()
        self.theme_changed.emit(theme)

    def toggle_theme(self) -> ThemeType:
        """Toggle between light and dark themes.

        Returns:
            The new theme type after toggling.
        """
        new_theme = (
            ThemeType.DARK
            if self._current_theme == ThemeType.LIGHT
            else ThemeType.LIGHT
        )
        self.set_theme(new_theme)
        return new_theme

    def get_stylesheet(self, component: str) -> str:
        """Get themed stylesheet for a component.

        Args:
            component: Component name ('main_window', 'mini_toolbar', 'log', etc.)

        Returns:
            CSS stylesheet string.
        """
        if self._current_theme == ThemeType.DARK:
            return self._get_dark_stylesheet(component)
        return self._get_light_stylesheet(component)

    def _get_light_stylesheet(self, component: str) -> str:
        """Get light theme stylesheet with animations.

        Args:
            component: Component name.

        Returns:
            CSS stylesheet string.
        """
        c = self._colors
        stylesheets = {
            "main_window": f"""
                QMainWindow {{ background-color: {c['bg_main']}; }}
                QTabWidget::pane {{ border: 1px solid {c['border']}; background-color: {c['bg_widget']}; }}
                QTabBar::tab {{ padding: 8px 16px; background-color: {c['bg_toolbar']}; color: {c['text_primary']}; border-top-left-radius: 4px; border-top-right-radius: 4px; }}
                QTabBar::tab:selected {{ background-color: {c['bg_widget']}; }}
                QTabBar::tab:hover {{ background-color: {c['bg_hover']}; cursor: pointinghand; }}
                QPushButton {{ padding: 6px 12px; border-radius: 4px; background-color: {c['primary']}; color: {c['text_on_primary']}; border: none; transition: background-color 0.2s ease; }}
                QPushButton:hover {{ background-color: {c['primary_hover']}; cursor: pointinghand; }}
                QPushButton:pressed {{ background-color: {c['primary_active']}; }}
                QPushButton:disabled {{ background-color: {c['bg_toolbar']}; color: {c['text_disabled']}; }}
                QLineEdit, QSpinBox, QDoubleSpinBox, QComboBox {{ padding: 6px; border: 1px solid {c['border']}; border-radius: 4px; background-color: {c['bg_widget']}; color: {c['text_primary']}; transition: border-color 0.2s ease; }}
                QLineEdit:focus, QSpinBox:focus, QDoubleSpinBox:focus, QComboBox:focus {{ border: 1px solid {c['border_focus']}; }}
                QSpinBox:hover, QDoubleSpinBox:hover {{ border-color: {c['border_focus']}; }}
                QComboBox:hover {{ border-color: {c['border_focus']}; }}
                QComboBox::drop-down {{ width: 20px; border: none; }}
                QComboBox::down-arrow {{ image: none; border-left: 4px solid transparent; border-right: 4px solid transparent; border-top: 6px solid {c['text_secondary']}; margin-right: 8px; }}
                QGroupBox {{ font-weight: bold; margin-top: 8px; padding-top: 8px; border: 1px solid {c['border']}; border-radius: 4px; background-color: {c['bg_widget']}; }}
                QGroupBox::title {{ subcontrol-origin: margin; left: 10px; padding: 0 5px; color: {c['text_primary']}; }}
                QLabel {{ color: {c['text_primary']}; transition: color 0.2s ease; }}
                QHeaderView::section {{ background-color: {c['bg_toolbar']}; color: {c['text_primary']}; padding: 6px; border: 1px solid {c['border']}; }}
                QTableWidget {{ background-color: {c['bg_widget']}; color: {c['text_primary']}; border: 1px solid {c['border']}; alternate-background-color: {c['bg_hover']}; }}
                QListWidget {{ background-color: {c['bg_widget']}; color: {c['text_primary']}; border: 1px solid {c['border']}; }}
                QListWidget::item:hover {{ background-color: {c['bg_hover']}; }}
                QMenuBar {{ background-color: {c['bg_toolbar']}; color: {c['text_primary']}; }}
                QMenuBar::item:selected {{ background-color: {c['bg_hover']}; }}
                QMenu {{ background-color: {c['bg_widget']}; color: {c['text_primary']}; border: 1px solid {c['border']}; }}
                QMenu::item:selected {{ background-color: {c['bg_hover']}; cursor: pointinghand; }}
                QStatusBar {{ background-color: {c['bg_toolbar']}; color: {c['text_primary']}; }}
                QProgressBar {{ border: 1px solid {c['border']}; border-radius: 4px; background-color: {c['bg_widget']}; text-align: center; }}
                QProgressBar::chunk {{ background-color: {c['primary']}; border-radius: 3px; }}
                QScrollBar:vertical {{ width: 8px; background: {c['bg_toolbar']}; }}
                QScrollBar::handle:vertical {{ background: {c['border']}; border-radius: 4px; min-height: 20px; }}
                QScrollBar::handle:vertical:hover {{ background: {c['text_secondary']}; }}
                QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{ height: 0px; }}
                QScrollBar:horizontal {{ height: 8px; background: {c['bg_toolbar']}; }}
                QScrollBar::handle:horizontal {{ background: {c['border']}; border-radius: 4px; min-width: 20px; }}
                QScrollBar::handle:horizontal:hover {{ background: {c['text_secondary']}; }}
                QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {{ width: 0px; }}
                QToolTip {{ background-color: {c['bg_tooltip']}; color: {c['text_primary']}; border: 1px solid {c['border']}; border-radius: 4px; padding: 4px 8px; }}
            """,
            "mini_toolbar": f"""
                QWidget {{ background-color: {c['bg_dark_main']}; border-radius: 8px; }}
                QPushButton {{ background-color: {c['bg_dark_hover']}; color: white; border: none; border-radius: 4px; padding: 6px 12px; font-size: 12px; min-width: 60px; transition: all 0.2s ease; }}
                QPushButton:hover {{ background-color: {c['bg_dark_active']}; transform: translateY(-1px); }}
                QPushButton:pressed {{ background-color: {c['bg_dark_hover']}; transform: translateY(0px); }}
                QPushButton#run_btn {{ background-color: {c['success']}; }}
                QPushButton#run_btn:hover {{ background-color: {c['success_hover']}; transform: translateY(-1px); }}
                QPushButton#stop_btn {{ background-color: {c['error']}; }}
                QPushButton#stop_btn:hover {{ background-color: {c['error_hover']}; transform: translateY(-1px); }}
                QLabel {{ color: {c['text_dark_primary']}; font-size: 12px; padding: 0 8px; transition: color 0.2s ease; }}
            """,
            "log": f"""
                QTextEdit {{
                    background-color: {c['status_bar']};
                    color: {c['status_text']};
                    font-family: Consolas, 'Microsoft YaHei';
                    font-size: 12px;
                    border: none;
                    transition: background-color 0.3s ease;
                }}
            """,
            "message_success": f"""
                QMessageBox {{ background-color: #f0f9eb; }}
                QPushButton {{ background-color: {c['success']}; color: white; padding: 8px 20px; border: none; border-radius: 4px; transition: background-color 0.2s ease; }}
                QPushButton:hover {{ background-color: {c['success_hover']}; }}
            """,
            "message_error": f"""
                QMessageBox {{ background-color: #fef0f0; }}
                QPushButton {{ background-color: {c['error']}; color: white; padding: 8px 20px; border: none; border-radius: 4px; transition: background-color 0.2s ease; }}
                QPushButton:hover {{ background-color: {c['error_hover']}; }}
            """,
            "toast": f"""
                QWidget {{ background-color: {c['primary']}; border-radius: 8px; transition: opacity 0.3s ease; }}
                QLabel {{ color: white; font-size: 14px; }}
            """,
            "dialog": f"""
                QDialog {{ background-color: {c['bg_widget']}; }}
                QLabel {{ color: {c['text_primary']}; }}
                QPushButton {{ padding: 6px 16px; border-radius: 4px; background-color: {c['primary']}; color: {c['text_on_primary']}; border: none; transition: all 0.2s ease; }}
                QPushButton:hover {{ background-color: {c['primary_hover']}; cursor: pointinghand; }}
            """,
            "button_animated": f"""
                QPushButton {{
                    padding: 6px 12px;
                    border-radius: 4px;
                    background-color: {c['primary']};
                    color: {c['text_on_primary']};
                    border: none;
                    transition: all 0.2s ease;
                }}
                QPushButton:hover {{
                    background-color: {c['primary_hover']};
                    transform: translateY(-2px);
                    box-shadow: 0 4px 8px rgba(0,0,0,0.15);
                }}
                QPushButton:pressed {{
                    background-color: {c['primary_active']};
                    transform: translateY(0px);
                    box-shadow: none;
                }}
                QPushButton:checked {{
                    background-color: {c['primary_active']};
                    border: 2px solid {c['primary']};
                }}
            """,
            "button_success": f"""
                QPushButton {{
                    padding: 6px 12px;
                    border-radius: 4px;
                    background-color: {c['success']};
                    color: white;
                    border: none;
                    transition: all 0.2s ease;
                }}
                QPushButton:hover {{
                    background-color: {c['success_hover']};
                    transform: translateY(-2px);
                    box-shadow: 0 4px 8px rgba(0,0,0,0.15);
                }}
                QPushButton:pressed {{
                    background-color: {c['success_hover']};
                    transform: translateY(0px);
                }}
            """,
            "button_danger": f"""
                QPushButton {{
                    padding: 6px 12px;
                    border-radius: 4px;
                    background-color: {c['error']};
                    color: white;
                    border: none;
                    transition: all 0.2s ease;
                }}
                QPushButton:hover {{
                    background-color: {c['error_hover']};
                    transform: translateY(-2px);
                    box-shadow: 0 4px 8px rgba(0,0,0,0.15);
                }}
                QPushButton:pressed {{
                    background-color: {c['error_hover']};
                    transform: translateY(0px);
                }}
            """,
            "button_info": f"""
                QPushButton {{
                    padding: 6px 12px;
                    border-radius: 4px;
                    background-color: {c['info']};
                    color: white;
                    border: none;
                    transition: all 0.2s ease;
                }}
                QPushButton:hover {{
                    background-color: {c['info_hover']};
                    transform: translateY(-2px);
                    box-shadow: 0 4px 8px rgba(0,0,0,0.15);
                }}
                QPushButton:pressed {{
                    background-color: {c['info_hover']};
                    transform: translateY(0px);
                }}
            """,
            "button_warning": f"""
                QPushButton {{
                    padding: 6px 12px;
                    border-radius: 4px;
                    background-color: {c['warning']};
                    color: white;
                    border: none;
                    transition: all 0.2s ease;
                }}
                QPushButton:hover {{
                    background-color: {c['warning_hover']};
                    transform: translateY(-2px);
                    box-shadow: 0 4px 8px rgba(0,0,0,0.15);
                }}
                QPushButton:pressed {{
                    background-color: {c['warning_hover']};
                    transform: translateY(0px);
                }}
            """,
            "splitter": f"""
                QSplitter {{
                    background-color: {c['bg_widget']};
                }}
                QSplitter::handle {{
                    background-color: {c['border']};
                }}
            """,
            "progress_animated": f"""
                QProgressBar {{
                    border: 1px solid {c['border']};
                    border-radius: 4px;
                    background-color: {c['bg_widget']};
                    text-align: center;
                }}
                QProgressBar::chunk {{
                    background-color: {c['primary']};
                    border-radius: 3px;
                    animation: progress_pulse 1.5s infinite;
                }}
                @keyframes progress_pulse {{
                    0% {{ opacity: 1.0; }}
                    50% {{ opacity: 0.6; }}
                    100% {{ opacity: 1.0; }}
                }}
            """,
        }
        return stylesheets.get(component, "")

    def _get_dark_stylesheet(self, component: str) -> str:
        """Get dark theme stylesheet with animations.

        Args:
            component: Component name.

        Returns:
            CSS stylesheet string.
        """
        c = self._colors
        stylesheets = {
            "main_window": f"""
                QMainWindow {{ background-color: {c['bg_main']}; }}
                QTabWidget::pane {{ border: 1px solid {c['border']}; background-color: {c['bg_widget']}; }}
                QTabBar::tab {{ padding: 8px 16px; background-color: {c['bg_toolbar']}; color: {c['text_primary']}; border-top-left-radius: 4px; border-top-right-radius: 4px; }}
                QTabBar::tab:selected {{ background-color: {c['bg_widget']}; }}
                QTabBar::tab:hover {{ background-color: {c['bg_hover']}; cursor: pointinghand; }}
                QPushButton {{ padding: 6px 12px; border-radius: 4px; background-color: {c['primary']}; color: {c['text_on_primary']}; border: none; transition: all 0.2s ease; }}
                QPushButton:hover {{ background-color: {c['primary_hover']}; cursor: pointinghand; }}
                QPushButton:pressed {{ background-color: {c['primary_active']}; }}
                QPushButton:disabled {{ background-color: {c['bg_toolbar']}; color: {c['text_disabled']}; }}
                QLineEdit, QSpinBox, QDoubleSpinBox, QComboBox {{ padding: 6px; border: 1px solid {c['border']}; border-radius: 4px; background-color: {c['bg_widget']}; color: {c['text_primary']}; transition: border-color 0.2s ease; }}
                QLineEdit:focus, QSpinBox:focus, QDoubleSpinBox:focus, QComboBox:focus {{ border: 1px solid {c['border_focus']}; }}
                QSpinBox:hover, QDoubleSpinBox:hover {{ border-color: {c['border_focus']}; }}
                QComboBox:hover {{ border-color: {c['border_focus']}; }}
                QComboBox::drop-down {{ width: 20px; border: none; }}
                QComboBox::down-arrow {{ image: none; border-left: 4px solid transparent; border-right: 4px solid transparent; border-top: 6px solid {c['text_secondary']}; margin-right: 8px; }}
                QGroupBox {{ font-weight: bold; margin-top: 8px; padding-top: 8px; border: 1px solid {c['border']}; border-radius: 4px; background-color: {c['bg_widget']}; }}
                QGroupBox::title {{ subcontrol-origin: margin; left: 10px; padding: 0 5px; color: {c['text_primary']}; }}
                QLabel {{ color: {c['text_primary']}; transition: color 0.2s ease; }}
                QHeaderView::section {{ background-color: {c['bg_toolbar']}; color: {c['text_primary']}; padding: 6px; border: 1px solid {c['border']}; }}
                QTableWidget {{ background-color: {c['bg_widget']}; color: {c['text_primary']}; border: 1px solid {c['border']}; alternate-background-color: {c['bg_hover']}; }}
                QListWidget {{ background-color: {c['bg_widget']}; color: {c['text_primary']}; border: 1px solid {c['border']}; }}
                QListWidget::item:hover {{ background-color: {c['bg_hover']}; }}
                QMenuBar {{ background-color: {c['bg_toolbar']}; color: {c['text_primary']}; }}
                QMenuBar::item:selected {{ background-color: {c['bg_hover']}; }}
                QMenu {{ background-color: {c['bg_widget']}; color: {c['text_primary']}; border: 1px solid {c['border']}; }}
                QMenu::item:selected {{ background-color: {c['bg_hover']}; cursor: pointinghand; }}
                QStatusBar {{ background-color: {c['bg_toolbar']}; color: {c['text_primary']}; }}
                QProgressBar {{ border: 1px solid {c['border']}; border-radius: 4px; background-color: {c['bg_widget']}; text-align: center; }}
                QProgressBar::chunk {{ background-color: {c['primary']}; border-radius: 3px; }}
                QScrollBar:vertical {{ width: 8px; background: {c['bg_toolbar']}; }}
                QScrollBar::handle:vertical {{ background: {c['border']}; border-radius: 4px; min-height: 20px; }}
                QScrollBar::handle:vertical:hover {{ background: {c['text_secondary']}; }}
                QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{ height: 0px; }}
                QScrollBar:horizontal {{ height: 8px; background: {c['bg_toolbar']}; }}
                QScrollBar::handle:horizontal {{ background: {c['border']}; border-radius: 4px; min-width: 20px; }}
                QScrollBar::handle:horizontal:hover {{ background: {c['text_secondary']}; }}
                QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {{ width: 0px; }}
                QToolTip {{ background-color: {c['bg_tooltip']}; color: {c['text_primary']}; border: 1px solid {c['border']}; border-radius: 4px; padding: 4px 8px; }}
            """,
            "mini_toolbar": f"""
                QWidget {{ background-color: {c['bg_dark_main']}; border-radius: 8px; }}
                QPushButton {{ background-color: {c['bg_dark_hover']}; color: white; border: none; border-radius: 4px; padding: 6px 12px; font-size: 12px; min-width: 60px; transition: all 0.2s ease; }}
                QPushButton:hover {{ background-color: {c['bg_dark_active']}; transform: translateY(-1px); }}
                QPushButton:pressed {{ background-color: {c['bg_dark_hover']}; transform: translateY(0px); }}
                QPushButton#run_btn {{ background-color: {c['success']}; }}
                QPushButton#run_btn:hover {{ background-color: {c['success_hover']}; transform: translateY(-1px); }}
                QPushButton#stop_btn {{ background-color: {c['error']}; }}
                QPushButton#stop_btn:hover {{ background-color: {c['error_hover']}; transform: translateY(-1px); }}
                QLabel {{ color: {c['text_dark_primary']}; font-size: 12px; padding: 0 8px; transition: color 0.2s ease; }}
            """,
            "log": f"""
                QTextEdit {{
                    background-color: {c['status_bar']};
                    color: {c['status_text']};
                    font-family: Consolas, 'Microsoft YaHei';
                    font-size: 12px;
                    border: none;
                    transition: background-color 0.3s ease;
                }}
            """,
            "message_success": f"""
                QMessageBox {{ background-color: {c['bg_widget']}; }}
                QPushButton {{ background-color: {c['success']}; color: {c['text_on_primary']}; padding: 8px 20px; border: none; border-radius: 4px; transition: all 0.2s ease; }}
                QPushButton:hover {{ background-color: {c['success_hover']}; }}
            """,
            "message_error": f"""
                QMessageBox {{ background-color: {c['bg_widget']}; }}
                QPushButton {{ background-color: {c['error']}; color: {c['text_on_primary']}; padding: 8px 20px; border: none; border-radius: 4px; transition: all 0.2s ease; }}
                QPushButton:hover {{ background-color: {c['error_hover']}; }}
            """,
            "toast": f"""
                QWidget {{ background-color: {c['primary']}; border-radius: 8px; transition: opacity 0.3s ease; }}
                QLabel {{ color: white; font-size: 14px; }}
            """,
            "dialog": f"""
                QDialog {{ background-color: {c['bg_widget']}; }}
                QLabel {{ color: {c['text_primary']}; }}
                QPushButton {{ padding: 6px 16px; border-radius: 4px; background-color: {c['primary']}; color: {c['text_on_primary']}; border: none; transition: all 0.2s ease; }}
                QPushButton:hover {{ background-color: {c['primary_hover']}; cursor: pointinghand; }}
            """,
            "button_animated": f"""
                QPushButton {{
                    padding: 6px 12px;
                    border-radius: 4px;
                    background-color: {c['primary']};
                    color: {c['text_on_primary']};
                    border: none;
                    transition: all 0.2s ease;
                }}
                QPushButton:hover {{
                    background-color: {c['primary_hover']};
                    transform: translateY(-2px);
                    box-shadow: 0 4px 12px rgba(0,0,0,0.3);
                }}
                QPushButton:pressed {{
                    background-color: {c['primary_active']};
                    transform: translateY(0px);
                    box-shadow: none;
                }}
                QPushButton:checked {{
                    background-color: {c['primary_active']};
                    border: 2px solid {c['primary']};
                }}
            """,
            "button_success": f"""
                QPushButton {{
                    padding: 6px 12px;
                    border-radius: 4px;
                    background-color: {c['success']};
                    color: white;
                    border: none;
                    transition: all 0.2s ease;
                }}
                QPushButton:hover {{
                    background-color: {c['success_hover']};
                    transform: translateY(-2px);
                    box-shadow: 0 4px 12px rgba(0,0,0,0.3);
                }}
                QPushButton:pressed {{
                    background-color: {c['success_hover']};
                    transform: translateY(0px);
                }}
            """,
            "button_danger": f"""
                QPushButton {{
                    padding: 6px 12px;
                    border-radius: 4px;
                    background-color: {c['error']};
                    color: white;
                    border: none;
                    transition: all 0.2s ease;
                }}
                QPushButton:hover {{
                    background-color: {c['error_hover']};
                    transform: translateY(-2px);
                    box-shadow: 0 4px 12px rgba(0,0,0,0.3);
                }}
                QPushButton:pressed {{
                    background-color: {c['error_hover']};
                    transform: translateY(0px);
                }}
            """,
            "button_info": f"""
                QPushButton {{
                    padding: 6px 12px;
                    border-radius: 4px;
                    background-color: {c['info']};
                    color: white;
                    border: none;
                    transition: all 0.2s ease;
                }}
                QPushButton:hover {{
                    background-color: {c['info_hover']};
                    transform: translateY(-2px);
                    box-shadow: 0 4px 12px rgba(0,0,0,0.3);
                }}
                QPushButton:pressed {{
                    background-color: {c['info_hover']};
                    transform: translateY(0px);
                }}
            """,
            "button_warning": f"""
                QPushButton {{
                    padding: 6px 12px;
                    border-radius: 4px;
                    background-color: {c['warning']};
                    color: white;
                    border: none;
                    transition: all 0.2s ease;
                }}
                QPushButton:hover {{
                    background-color: {c['warning_hover']};
                    transform: translateY(-2px);
                    box-shadow: 0 4px 12px rgba(0,0,0,0.3);
                }}
                QPushButton:pressed {{
                    background-color: {c['warning_hover']};
                    transform: translateY(0px);
                }}
            """,
            "splitter": f"""
                QSplitter {{
                    background-color: {c['bg_widget']};
                }}
                QSplitter::handle {{
                    background-color: {c['border']};
                }}
            """,
            "progress_animated": f"""
                QProgressBar {{
                    border: 1px solid {c['border']};
                    border-radius: 4px;
                    background-color: {c['bg_widget']};
                    text-align: center;
                }}
                QProgressBar::chunk {{
                    background-color: {c['primary']};
                    border-radius: 3px;
                    animation: progress_pulse 1.5s infinite;
                }}
                @keyframes progress_pulse {{
                    0% {{ opacity: 1.0; }}
                    50% {{ opacity: 0.6; }}
                    100% {{ opacity: 1.0; }}
                }}
            """,
        }
        return stylesheets.get(component, "")

    def get_log_colors(self) -> Dict[str, str]:
        """Get log level colors for the current theme.

        Returns:
            Dict mapping log level names to color strings.
        """
        return {
            "DEBUG": self._colors["log_debug"],
            "INFO": self._colors["log_info"],
            "SUCCESS": self._colors["log_success"],
            "WARNING": self._colors["log_warning"],
            "ERROR": self._colors["log_error"],
            "CRITICAL": self._colors["log_critical"],
        }

    def get_button_stylesheet(self, style: str = 'default') -> str:
        """Get animated button stylesheet.

        Args:
            style: Button style ('default', 'success', 'danger', 'info', 'warning').

        Returns:
            CSS stylesheet string for buttons.
        """
        style_map = {
            'success': 'button_success',
            'danger': 'button_danger',
            'info': 'button_info',
            'warning': 'button_warning',
        }
        stylesheet_name = style_map.get(style)
        if stylesheet_name:
            return self.get_stylesheet(stylesheet_name)
        return self.get_stylesheet('button_animated')


# Global singleton instance
theme_manager: ThemeManager = ThemeManager()
