# UI module for RabAI AutoClick

# Import message system first (no dependencies)
from .message import message_manager, show_error, show_success, show_warning, show_info, show_question, show_toast

# Theme system - independent module
from .themes import (
    ThemeConfig,
    ThemeColors,
    ThemeFonts,
    ThemeSpacing,
    ThemeManager,
    get_theme_manager,
    BUILT_IN_THEMES,
    LIGHT_THEME,
    DARK_THEME,
    HIGH_CONTRAST_THEME,
)

# UI Components - independent module
from .components import (
    StyledButton,
    StyledLineEdit,
    StyledComboBox,
    WorkflowStepCard,
    ActionParameterEditor,
    LogViewer,
    MetricsDashboard,
)

# Import other UI components with conditional error handling
try:
    from .hotkey_dialog import HotkeySettingsDialog, HotkeyEdit
except ImportError as e:
    HotkeySettingsDialog = None
    HotkeyEdit = None

try:
    from .region_selector import RegionSelector, PositionSelector, select_region, select_position
except ImportError as e:
    RegionSelector = None
    PositionSelector = None
    select_region = None
    select_position = None

# Main window - may have broken imports due to missing dependencies
try:
    from .main_window import MainWindow, main
except ImportError as e:
    MainWindow = None
    main = None


__all__ = [
    # Messages
    'message_manager',
    'show_error',
    'show_success',
    'show_warning',
    'show_info',
    'show_question',
    'show_toast',
    # Theme system
    'ThemeConfig',
    'ThemeColors',
    'ThemeFonts',
    'ThemeSpacing',
    'ThemeManager',
    'get_theme_manager',
    'BUILT_IN_THEMES',
    'LIGHT_THEME',
    'DARK_THEME',
    'HIGH_CONTRAST_THEME',
    # Components
    'StyledButton',
    'StyledLineEdit',
    'StyledComboBox',
    'WorkflowStepCard',
    'ActionParameterEditor',
    'LogViewer',
    'MetricsDashboard',
    # Dialogs
    'HotkeySettingsDialog',
    'HotkeyEdit',
    # Region selector
    'RegionSelector',
    'PositionSelector',
    'select_region',
    'select_position',
    # Main window
    'MainWindow',
    'main',
]
