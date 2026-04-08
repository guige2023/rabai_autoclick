"""Security action module for RabAI AutoClick.

Provides security-related actions including permissions and keychain access.
"""

import subprocess
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class PermissionCheckAction(BaseAction):
    """Check system permissions status.
    
    Checks if required permissions are granted.
    """
    action_type = "permission_check"
    display_name = "权限检查"
    description = "检查系统权限状态"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Check permissions.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: permission (screen_recording/automation/accessibility).
        
        Returns:
            ActionResult with permission status.
        """
        permission = params.get('permission', '')
        
        permission_map = {
            'screen_recording': 'kTCCServiceScreenCapture',
            'automation': 'kTCCServiceAppleEvents',
            'accessibility': 'kTCCServiceAccessibility',
            'camera': 'kTCCServiceCamera',
            'microphone': 'kTCCServiceMicrophone',
            'photos': 'kTCCServicePhotos',
            'contacts': 'kTCCServiceContacts'
        }
        
        if permission not in permission_map:
            return ActionResult(
                success=False,
                message=f"Unknown permission: {permission}"
            )
        
        try:
            # Use tccutil on older macOS or sqlite query
            script = f'''
            set permissionName to "{permission_map[permission]}"
            do shell script "sqlite3 /Library/Application\\ Support/com.apple.TCC/TCC.db 'select service,client,auth_value from access where service=\"' & permissionName & '\"'"
            '''
            
            result = subprocess.run(
                ['osascript', '-e', script],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.stdout.strip():
                return ActionResult(
                    success=True,
                    message=f"{permission} permission exists",
                    data={'permission': permission, 'granted': True, 'details': result.stdout}
                )
            else:
                return ActionResult(
                    success=True,
                    message=f"{permission} permission not found",
                    data={'permission': permission, 'granted': False}
                )
                
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Permission check error: {e}",
                data={'error': str(e)}
            )


class KeychainGetAction(BaseAction):
    """Get item from keychain.
    
    Retrieves password or secret from keychain.
    """
    action_type = "keychain_get"
    display_name = "获取钥匙串"
    description = "从钥匙串获取密码或密钥"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get keychain item.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: service, account.
        
        Returns:
            ActionResult with keychain value.
        """
        service = params.get('service', '')
        account = params.get('account', '')
        
        if not service and not account:
            return ActionResult(success=False, message="service or account required")
        
        try:
            cmd = ['security', 'find-generic-password']
            if service:
                cmd.extend(['-s', service])
            if account:
                cmd.extend(['-a', account])
            cmd.extend(['-w'])
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0 and result.stdout.strip():
                return ActionResult(
                    success=True,
                    message=f"Retrieved keychain item",
                    data={'password': result.stdout.strip()}
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"Keychain item not found"
                )
                
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Keychain error: {e}",
                data={'error': str(e)}
            )


class KeychainSetAction(BaseAction):
    """Store item in keychain.
    
    Saves password or secret to keychain.
    """
    action_type = "keychain_set"
    display_name = "存储钥匙串"
    description = "存储密码或密钥到钥匙串"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Set keychain item.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: service, account, password, notes.
        
        Returns:
            ActionResult with set status.
        """
        service = params.get('service', '')
        account = params.get('account', '')
        password = params.get('password', '')
        notes = params.get('notes', '')
        
        if not service or not account or not password:
            return ActionResult(success=False, message="service, account, and password required")
        
        try:
            cmd = ['security', 'add-generic-password', '-s', service, '-a', account, '-w', password]
            if notes:
                cmd.extend(['-j', notes])
            
            result = subprocess.run(cmd, capture_output=True, timeout=10)
            
            if result.returncode == 0:
                return ActionResult(
                    success=True,
                    message=f"Stored in keychain: {service}/{account}",
                    data={'service': service, 'account': account}
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"Keychain set failed: {result.stderr.decode()}"
                )
                
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Keychain error: {e}",
                data={'error': str(e)}
            )


class FirewallAction(BaseAction):
    """Manage macOS firewall.
    
    Gets or sets firewall status.
    """
    action_type = "firewall"
    display_name = "防火墙管理"
    description = "管理macOS防火墙"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Manage firewall.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: operation (status/on/off), app_path.
        
        Returns:
            ActionResult with firewall status.
        """
        operation = params.get('operation', 'status')
        app_path = params.get('app_path', '')
        
        try:
            if operation == 'status':
                result = subprocess.run(
                    ['/usr/libexec/ApplicationFirewall/socketfilterfw', '--getglobalstate'],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                
                enabled = 'enabled' in result.stdout.lower()
                return ActionResult(
                    success=True,
                    message=f"Firewall {'enabled' if enabled else 'disabled'}",
                    data={'enabled': enabled}
                )
            
            elif operation == 'on':
                cmd = ['/usr/libexec/ApplicationFirewall/socketfilterfw', '--setglobalstate', 'on']
                subprocess.run(cmd, capture_output=True, timeout=10, check=True)
                return ActionResult(
                    success=True,
                    message="Firewall enabled",
                    data={'enabled': True}
                )
            
            elif operation == 'off':
                cmd = ['/usr/libexec/ApplicationFirewall/socketfilterfw', '--setglobalstate', 'off']
                subprocess.run(cmd, capture_output=True, timeout=10, check=True)
                return ActionResult(
                    success=True,
                    message="Firewall disabled",
                    data={'enabled': False}
                )
            
            elif operation == 'add_app' and app_path:
                cmd = ['/usr/libexec/ApplicationFirewall/socketfilterfw', '--add', app_path]
                subprocess.run(cmd, capture_output=True, timeout=10, check=True)
                return ActionResult(
                    success=True,
                    message=f"App added to firewall: {app_path}",
                    data={'app_path': app_path}
                )
            
            return ActionResult(success=False, message=f"Unknown operation: {operation}")
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Firewall error: {e}",
                data={'error': str(e)}
            )
