"""Application action module for RabAI AutoClick.

Provides application management actions including launch, quit, and switch.
"""

import subprocess
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AppLaunchAction(BaseAction):
    """Launch an application.
    
    Opens application by name or bundle ID.
    """
    action_type = "app_launch"
    display_name = "启动应用"
    description = "启动应用程序"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Launch application.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: app_name, bundle_id, url, args.
        
        Returns:
            ActionResult with launch status.
        """
        app_name = params.get('app_name', '')
        bundle_id = params.get('bundle_id', '')
        url = params.get('url', '')
        args = params.get('args', [])
        
        if not app_name and not bundle_id and not url:
            return ActionResult(success=False, message="app_name, bundle_id, or url required")
        
        try:
            cmd = ['open']
            
            if bundle_id:
                cmd.extend(['-b', bundle_id])
            elif app_name:
                cmd.extend(['-a', app_name])
            elif url:
                cmd.append(url)
            
            if args:
                cmd.append('--args')
                if isinstance(args, list):
                    cmd.extend(args)
                else:
                    cmd.append(args)
            
            result = subprocess.run(cmd, capture_output=True, timeout=30)
            
            if result.returncode == 0:
                return ActionResult(
                    success=True,
                    message=f"Launched: {app_name or bundle_id or url}",
                    data={'app_name': app_name, 'bundle_id': bundle_id}
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"Launch failed: {result.stderr.decode()}"
                )
                
        except subprocess.TimeoutExpired:
            return ActionResult(success=False, message="Launch timed out")
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Launch error: {e}",
                data={'error': str(e)}
            )


class AppQuitAction(BaseAction):
    """Quit an application.
    
    Terminates running application.
    """
    action_type = "app_quit"
    display_name = "退出应用"
    description = "退出应用程序"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Quit application.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: app_name, force.
        
        Returns:
            ActionResult with quit status.
        """
        app_name = params.get('app_name', '')
        force = params.get('force', False)
        
        if not app_name:
            return ActionResult(success=False, message="app_name required")
        
        try:
            if force:
                script = f'tell application "{app_name}" to quit'
            else:
                script = f'tell application "{app_name}" to if it is running then quit'
            
            result = subprocess.run(
                ['osascript', '-e', script],
                capture_output=True,
                timeout=10
            )
            
            if result.returncode == 0:
                return ActionResult(
                    success=True,
                    message=f"Quit: {app_name}",
                    data={'app_name': app_name, 'force': force}
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"Quit failed: {result.stderr.decode()}"
                )
                
        except subprocess.TimeoutExpired:
            return ActionResult(success=False, message="Quit timed out")
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Quit error: {e}",
                data={'error': str(e)}
            )


class AppSwitchAction(BaseAction):
    """Switch to an application.
    
    Activates and brings application to front.
    """
    action_type = "app_switch"
    display_name = "切换应用"
    description = "切换到指定应用"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Switch to application.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: app_name.
        
        Returns:
            ActionResult with switch status.
        """
        app_name = params.get('app_name', '')
        
        if not app_name:
            return ActionResult(success=False, message="app_name required")
        
        try:
            script = f'tell application "{app_name}" to activate'
            result = subprocess.run(
                ['osascript', '-e', script],
                capture_output=True,
                timeout=10
            )
            
            if result.returncode == 0:
                return ActionResult(
                    success=True,
                    message=f"Switched to: {app_name}",
                    data={'app_name': app_name}
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"Switch failed: {result.stderr.decode()}"
                )
                
        except subprocess.TimeoutExpired:
            return ActionResult(success=False, message="Switch timed out")
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Switch error: {e}",
                data={'error': str(e)}
            )


class AppListAction(BaseAction):
    """List running applications.
    
    Returns list of running apps.
    """
    action_type = "app_list"
    display_name = "列出应用"
    description = "列出运行中的应用程序"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """List applications.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: filter_system.
        
        Returns:
            ActionResult with app list.
        """
        filter_system = params.get('filter_system', False)
        
        try:
            script = '''
            tell application "System Events"
                get name of every application process
            end tell
            '''
            result = subprocess.run(
                ['osascript', '-e', script],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode == 0:
                apps = [a.strip() for a in result.stdout.split(', ')]
                
                if filter_system:
                    system_apps = {'Finder', 'SystemUIServer', 'Dock', 'WindowServer', 'loginwindow'}
                    apps = [a for a in apps if a not in system_apps]
                
                return ActionResult(
                    success=True,
                    message=f"Found {len(apps)} application(s)",
                    data={'apps': apps, 'count': len(apps)}
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"List failed: {result.stderr.decode()}"
                )
                
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"List error: {e}",
                data={'error': str(e)}
            )
