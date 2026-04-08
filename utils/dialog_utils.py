"""
Dialog and alert manipulation utilities for macOS automation.

Provides utilities for creating, interacting with, and dismissing
system dialogs, alerts, and sheets through accessibility APIs.
"""

from __future__ import annotations

import subprocess
from typing import List, Optional, Callable, Dict, Any
from dataclasses import dataclass
from enum import Enum


class DialogType(Enum):
    """Types of dialogs."""
    ALERT = "alert"
    SHEET = "sheet"
    PANEL = "panel"
    FILE_SAVE = "file_save"
    FILE_OPEN = "file_open"
    FOLDER_CHOOSE = "folder_choose"
    MESSAGE = "message"


@dataclass
class DialogButton:
    """Represents a dialog button."""
    name: str
    label: str
    is_default: bool = False
    is_cancel: bool = False
    button_index: int = 0


@dataclass
class DialogInfo:
    """Information about a dialog."""
    dialog_type: DialogType
    title: str
    message: str
    buttons: List[DialogButton] = None
    
    def __post_init__(self):
        if self.buttons is None:
            self.buttons = []


class DialogManager:
    """Manages system dialogs and alerts."""
    
    def __init__(self):
        """Initialize dialog manager."""
        pass
    
    def show_alert(
        self,
        title: str,
        message: str,
        buttons: Optional[List[str]] = None,
        default_button: int = 0,
        alert_style: str = "informational"
    ) -> Optional[str]:
        """Show an alert dialog.
        
        Args:
            title: Alert title
            message: Alert message
            buttons: List of button labels
            default_button: Index of default button
            alert_style: Style (informational, warning, critical)
            
        Returns:
            Clicked button label or None
        """
        if buttons is None:
            buttons = ["OK"]
        
        buttons_str = ', '.join([f'"{b}"' for b in buttons])
        default_str = f'"{buttons[default_button]}"' if default_button < len(buttons) else ""
        
        script = f'''
        tell application "System Events"
            display alert "{title}" message "{message}" '''
        
        if alert_style == "warning":
            script += 'as warning '
        elif alert_style == "critical":
            script += 'as critical '
        else:
            script += 'as informational '
        
        script += f'buttons {{{buttons_str}}} '
        if default_str:
            script += f'default button {default_str} '
        
        script += '''
        end tell
        '''
        
        try:
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                return result.stdout.strip()
        except Exception:
            pass
        
        return None
    
    def show_message(
        self,
        message: str,
        title: Optional[str] = None,
        icon: Optional[str] = None
    ) -> bool:
        """Show a simple message dialog.
        
        Args:
            message: Message text
            title: Optional title
            icon: Optional icon type
            
        Returns:
            True if shown
        """
        title_part = f'with title "{title}" ' if title else ""
        icon_part = ""
        
        if icon == "note":
            icon_part = "giving up after 5"
        elif icon == "caution":
            icon_part = "giving up after 5"
        
        script = f'''
        tell application "System Events"
            display dialog "{message}" {title_part}{icon_part}
        end tell
        '''
        
        try:
            subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                timeout=10
            )
            return True
        except Exception:
            return False
    
    def show_file_open_dialog(
        self,
        title: Optional[str] = None,
        initial_directory: Optional[str] = None,
        file_types: Optional[List[str]] = None,
        allow_multiple: bool = False
    ) -> Optional[List[str]]:
        """Show a file open dialog.
        
        Args:
            title: Dialog title
            initial_directory: Starting directory
            file_types: Allowed file extensions
            allow_multiple: Allow multiple selections
            
        Returns:
            List of selected file paths or None
        """
        parts = []
        
        if title:
            parts.append(f'with prompt "{title}"')
        
        if initial_directory:
            parts.append(f'default location "{initial_directory}"')
        
        if file_types:
            types_str = ', '.join([f'"{ext}"' for ext in file_types])
            parts.append(f'of type {{{types_str}}}')
        
        if allow_multiple:
            parts.append("multiple selections allowed true")
        
        parts_str = ' '.join(parts)
        
        script = f'''
        tell application "System Events"
            choose file {parts_str}
        end tell
        '''
        
        if allow_multiple:
            script = f'''
            tell application "System Events"
                choose file {parts_str}
            end tell
            '''
        
        try:
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode == 0:
                output = result.stdout.strip()
                if allow_multiple:
                    # Multiple files are returned as comma-separated POSIX paths
                    files = [f.strip() for f in output.split(",")]
                    return files if files and files[0] else None
                else:
                    return [output]
        except Exception:
            pass
        
        return None
    
    def show_file_save_dialog(
        self,
        title: Optional[str] = None,
        initial_directory: Optional[str] = None,
        default_name: Optional[str] = None,
        file_types: Optional[List[str]] = None
    ) -> Optional[str]:
        """Show a file save dialog.
        
        Args:
            title: Dialog title
            initial_directory: Starting directory
            default_name: Default file name
            file_types: Allowed file extensions
            
        Returns:
            Selected file path or None
        """
        parts = []
        
        if title:
            parts.append(f'with prompt "{title}"')
        
        if initial_directory:
            parts.append(f'default location "{initial_directory}"')
        
        if default_name:
            parts.append(f'default name "{default_name}"')
        
        parts_str = ' '.join(parts)
        
        script = f'''
        tell application "System Events"
            choose file name {parts_str}
        end tell
        '''
        
        try:
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode == 0:
                return result.stdout.strip()
        except Exception:
            pass
        
        return None
    
    def show_folder_dialog(
        self,
        title: Optional[str] = None,
        initial_directory: Optional[str] = None,
        allow_multiple: bool = False
    ) -> Optional[List[str]]:
        """Show a folder chooser dialog.
        
        Args:
            title: Dialog title
            initial_directory: Starting directory
            allow_multiple: Allow multiple selections
            
        Returns:
            List of selected folder paths or None
        """
        parts = []
        
        if title:
            parts.append(f'with prompt "{title}"')
        
        if initial_directory:
            parts.append(f'default location "{initial_directory}"')
        
        parts_str = ' '.join(parts)
        
        script = f'''
        tell application "System Events"
            choose folder {parts_str}
        end tell
        '''
        
        try:
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode == 0:
                output = result.stdout.strip()
                return [output]
        except Exception:
            pass
        
        return None
    
    def click_dialog_button(
        self,
        button_label: str,
        dialog_title: Optional[str] = None
    ) -> bool:
        """Click a button in a dialog.
        
        Args:
            button_label: Button label to click
            dialog_title: Optional dialog title to match
            
        Returns:
            True if clicked
        """
        try:
            if dialog_title:
                script = f'''
                tell application "System Events"
                    tell process "SystemUIServer"
                        click button "{button_label}" of window "{dialog_title}"
                    end tell
                end tell
                '''
            else:
                script = f'''
                tell application "System Events"
                    click button "{button_label}" of first window
                end tell
                '''
            
            subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                timeout=5
            )
            return True
        except Exception:
            return False
    
    def press_dialog_button_by_index(self, index: int) -> bool:
        """Press a dialog button by index.
        
        Args:
            index: Button index (1-based)
            
        Returns:
            True if pressed
        """
        try:
            script = f'''
            tell application "System Events"
                tell process "SystemUIServer"
                    click button {index} of first window
                end tell
            end tell
            '''
            subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                timeout=5
            )
            return True
        except Exception:
            return False
    
    def dismiss_dialog(self) -> bool:
        """Dismiss the current dialog.
        
        Returns:
            True if dismissed
        """
        return self.press_escape()
    
    def press_escape(self) -> bool:
        """Press Escape key.
        
        Returns:
            True if pressed
        """
        try:
            script = '''
            tell application "System Events"
                key code 53
            end tell
            '''
            subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                timeout=2
            )
            return True
        except Exception:
            return False
    
    def press_return(self) -> bool:
        """Press Return key.
        
        Returns:
            True if pressed
        """
        try:
            script = '''
            tell application "System Events"
                key code 36
            end tell
            '''
            subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                timeout=2
            )
            return True
        except Exception:
            return False
    
    def press_tab(self) -> bool:
        """Press Tab key to move focus.
        
        Returns:
            True if pressed
        """
        try:
            script = '''
            tell application "System Events"
                key code 48
            end tell
            '''
            subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                timeout=2
            )
            return True
        except Exception:
            return False
    
    def get_current_dialog_info(self) -> Optional[DialogInfo]:
        """Get information about the current frontmost dialog.
        
        Returns:
            DialogInfo or None
        """
        try:
            script = '''
            tell application "System Events"
                tell process "SystemUIServer"
                    set windowName to name of first window
                    set buttonCount to count of buttons of first window
                    
                    set buttonList to {}
                    repeat with i from 1 to buttonCount
                        set btnName to name of button i of first window
                        set end of buttonList to btnName
                    end repeat
                    
                    return windowName & "||" & buttonCount & "||" & (buttonList as string)
                end tell
            end tell
            '''
            
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            if result.returncode == 0:
                parts = result.stdout.strip().split("||")
                if len(parts) >= 2:
                    title = parts[0]
                    button_count = int(parts[1])
                    button_names = parts[2].split(", ") if len(parts) > 2 else []
                    
                    buttons = [
                        DialogButton(
                            name=name.strip(),
                            label=name.strip(),
                            is_default=(i == 0),
                            button_index=i
                        )
                        for i, name in enumerate(button_names)
                        if name.strip()
                    ]
                    
                    return DialogInfo(
                        dialog_type=DialogType.ALERT,
                        title=title,
                        message="",
                        buttons=buttons
                    )
        except Exception:
            pass
        
        return None


class SheetController:
    """Controls sheet dialogs attached to windows."""
    
    def __init__(self):
        """Initialize sheet controller."""
        pass
    
    def wait_for_sheet(
        self,
        window_title: str,
        timeout_seconds: float = 30.0
    ) -> bool:
        """Wait for a sheet to appear.
        
        Args:
            window_title: Window title to match
            timeout_seconds: Maximum wait time
            
        Returns:
            True if sheet appeared
        """
        import time
        start = time.time()
        
        while time.time() - start < timeout_seconds:
            try:
                script = f'''
                tell application "System Events"
                    tell process "SystemUIServer"
                        if exists sheet 1 of window "{window_title}" then
                            return "true"
                        end if
                    end tell
                end tell
                '''
                
                result = subprocess.run(
                    ["osascript", "-e", script],
                    capture_output=True,
                    text=True,
                    timeout=2
                )
                
                if "true" in result.stdout.lower():
                    return True
            except Exception:
                pass
            
            time.sleep(0.1)
        
        return False
    
    def click_sheet_button(
        self,
        window_title: str,
        button_label: str
    ) -> bool:
        """Click a button in a sheet.
        
        Args:
            window_title: Window title
            button_label: Button label
            
        Returns:
            True if clicked
        """
        try:
            script = f'''
            tell application "System Events"
                tell process "SystemUIServer"
                    click button "{button_label}" of sheet 1 of window "{window_title}"
                end tell
            end tell
            '''
            subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                timeout=5
            )
            return True
        except Exception:
            return False
    
    def dismiss_sheet(self, window_title: str) -> bool:
        """Dismiss a sheet.
        
        Args:
            window_title: Window title
            
        Returns:
            True if dismissed
        """
        return self.click_sheet_button(window_title, "Cancel") or self.press_escape()


def show_notification(
    title: str,
    message: str,
    sound: bool = True
) -> bool:
    """Show a notification using Finder.
    
    Args:
        title: Notification title
        message: Notification message
        sound: Whether to play sound
        
    Returns:
        True if shown
    """
    try:
        sound_part = " " if sound else " giving up after 0 "
        
        script = f'''
        tell application "Finder"
            activate
            display notification "{message}" with title "{title}"{sound_part}
        end tell
        '''
        
        subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            timeout=5
        )
        return True
    except Exception:
        return False


def show_quick_action_confirmation(
    action: str,
    target: Optional[str] = None
) -> bool:
    """Show a quick action confirmation dialog.
    
    Args:
        action: Action being confirmed
        target: Optional target of the action
        
    Returns:
        True if confirmed
    """
    manager = DialogManager()
    
    message = f"Are you sure you want to {action}?"
    if target:
        message = f"Are you sure you want to {action} {target}?"
    
    result = manager.show_alert(
        title="Confirm Action",
        message=message,
        buttons=["Cancel", "OK"],
        default_button=1,
        alert_style="warning"
    )
    
    return result == "OK"
