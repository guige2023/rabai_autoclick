"""Automation Health Action Module.

Provides health check and self-healing capabilities
for automation workflows and systems.
"""

import sys
import os
import time
import threading
from typing import Any, Dict, List, Optional, Callable
from collections import defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class HealthCheckAction(BaseAction):
    """Perform health checks on system components.
    
    Supports custom health check functions and dependency tracking.
    """
    action_type = "health_check"
    display_name = "健康检查"
    description = "对系统组件执行健康检查"

    def __init__(self):
        super().__init__()
        self._health_checks: Dict[str, Callable] = {}
        self._health_history: Dict[str, List] = defaultdict(list)

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Perform health check.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'register', 'check', 'check_all', 'get_history'.
                - component: Component name.
                - check_func_var: Variable containing check function.
                - timeout: Check timeout in seconds.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with health check result or error.
        """
        operation = params.get('operation', 'check')
        component = params.get('component', '')
        check_func_var = params.get('check_func_var', '')
        timeout = params.get('timeout', 10)
        output_var = params.get('output_var', 'health_result')

        try:
            if operation == 'register':
                return self._register_check(component, check_func_var, context, output_var)
            elif operation == 'check':
                return self._check_component(component, timeout, context, output_var)
            elif operation == 'check_all':
                return self._check_all_components(timeout, context, output_var)
            elif operation == 'get_history':
                return self._get_history(component, output_var)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Health check failed: {str(e)}"
            )

    def _register_check(
        self,
        component: str,
        check_func_var: str,
        context: Any,
        output_var: str
    ) -> ActionResult:
        """Register a health check function."""
        if not component:
            return ActionResult(
                success=False,
                message="Component name is required"
            )

        check_func = None
        if check_func_var:
            check_func = context.variables.get(check_func_var)

        self._health_checks[component] = check_func

        context.variables[output_var] = {
            'component': component,
            'registered': True
        }
        return ActionResult(
            success=True,
            data={'component': component, 'registered': True},
            message=f"Health check registered for '{component}'"
        )

    def _check_component(
        self,
        component: str,
        timeout: float,
        context: Any,
        output_var: str
    ) -> ActionResult:
        """Check health of a specific component."""
        if component not in self._health_checks:
            return ActionResult(
                success=False,
                message=f"No health check registered for '{component}'"
            )

        check_func = self._health_checks[component]
        start_time = time.time()
        healthy = False
        error = None

        try:
            if callable(check_func):
                result = check_func()
                healthy = result.get('healthy', False) if isinstance(result, dict) else bool(result)
                error = result.get('error') if isinstance(result, dict) else None
            else:
                healthy = True
        except Exception as e:
            error = str(e)
            healthy = False

        duration = time.time() - start_time

        # Record in history
        self._health_history[component].append({
            'timestamp': datetime.now().isoformat(),
            'healthy': healthy,
            'duration': duration,
            'error': error
        })

        # Keep last 100 entries
        if len(self._health_history[component]) > 100:
            self._health_history[component] = self._health_history[component][-100:]

        result = {
            'component': component,
            'healthy': healthy,
            'duration': duration,
            'error': error,
            'timestamp': datetime.now().isoformat()
        }

        context.variables[output_var] = result
        return ActionResult(
            success=healthy,
            data=result,
            message=f"Health check for '{component}': {'healthy' if healthy else 'unhealthy'}"
        )

    def _check_all_components(
        self,
        timeout: float,
        context: Any,
        output_var: str
    ) -> ActionResult:
        """Check health of all registered components."""
        results = []
        all_healthy = True

        for component in self._health_checks:
            result = self._check_component(component, timeout, context, 'temp_health')
            results.append(result.data if result.data else {})

            if not result.success:
                all_healthy = False

        result = {
            'total_components': len(self._health_checks),
            'healthy_count': sum(1 for r in results if r.get('healthy', False)),
            'unhealthy_count': sum(1 for r in results if not r.get('healthy', False)),
            'all_healthy': all_healthy,
            'checks': results
        }

        context.variables[output_var] = result
        return ActionResult(
            success=all_healthy,
            data=result,
            message=f"Health check all: {result['healthy_count']}/{len(results)} healthy"
        )

    def _get_history(self, component: str, output_var: str) -> ActionResult:
        """Get health check history for a component."""
        history = self._health_history.get(component, [])

        # Calculate statistics
        if history:
            recent = history[-100:]
            healthy_count = sum(1 for h in recent if h['healthy'])

            stats = {
                'total_checks': len(recent),
                'healthy_checks': healthy_count,
                'unhealthy_checks': len(recent) - healthy_count,
                'health_ratio': healthy_count / len(recent) if recent else 0,
                'last_check': recent[-1] if recent else None
            }
        else:
            stats = {
                'total_checks': 0,
                'healthy_checks': 0,
                'unhealthy_checks': 0,
                'health_ratio': 0,
                'last_check': None
            }

        result = {
            'component': component,
            'history': history,
            'stats': stats
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Retrieved {len(history)} health check entries for '{component}'"
        )


class SelfHealingAction(BaseAction):
    """Implement self-healing automation capabilities.
    
    Supports automatic recovery, restart, and failover mechanisms.
    """
    action_type = "self_healing"
    display_name = "自愈系统"
    description = "实现自动化自愈能力"

    def __init__(self):
        super().__init__()
        self._healing_policies: Dict[str, Dict] = {}
        self._recovery_actions: Dict[str, List[Callable]] = defaultdict(list)

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute self-healing operations.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'configure', 'heal', 'register_recovery', 'get_status'.
                - component: Component to heal.
                - policy: Healing policy configuration.
                - recovery_action_var: Variable containing recovery action.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with healing result or error.
        """
        operation = params.get('operation', 'configure')
        component = params.get('component', '')
        policy = params.get('policy', {})
        recovery_action_var = params.get('recovery_action_var', '')
        output_var = params.get('output_var', 'healing_result')

        try:
            if operation == 'configure':
                return self._configure_policy(component, policy, output_var)
            elif operation == 'heal':
                return self._attempt_healing(component, context, output_var)
            elif operation == 'register_recovery':
                return self._register_recovery_action(
                    component, recovery_action_var, context, output_var
                )
            elif operation == 'get_status':
                return self._get_healing_status(component, output_var)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Self-healing failed: {str(e)}"
            )

    def _configure_policy(
        self, component: str, policy: Dict, output_var: str
    ) -> ActionResult:
        """Configure healing policy for a component."""
        default_policy = {
            'max_attempts': 3,
            'retry_delay': 30,
            'escalation_delay': 60,
            'enabled': True,
            'auto_heal': True
        }
        default_policy.update(policy)

        self._healing_policies[component] = default_policy

        context.variables[output_var] = {
            'component': component,
            'policy': default_policy
        }
        return ActionResult(
            success=True,
            data={'component': component, 'policy': default_policy},
            message=f"Healing policy configured for '{component}'"
        )

    def _attempt_healing(
        self, component: str, context: Any, output_var: str
    ) -> ActionResult:
        """Attempt to heal a component."""
        if component not in self._healing_policies:
            return ActionResult(
                success=False,
                message=f"No healing policy for '{component}'"
            )

        policy = self._healing_policies[component]
        if not policy.get('enabled', True):
            return ActionResult(
                success=False,
                message=f"Healing disabled for '{component}'"
            )

        recovery_actions = self._recovery_actions.get(component, [])

        if not recovery_actions:
            return ActionResult(
                success=False,
                message=f"No recovery actions registered for '{component}'"
            )

        # Attempt recovery
        healed = False
        attempts = 0
        last_error = None

        for action in recovery_actions:
            attempts += 1
            try:
                if callable(action):
                    result = action()
                    if result:
                        healed = True
                        break
            except Exception as e:
                last_error = str(e)

            if attempts >= policy.get('max_attempts', 3):
                break

            # Wait before next attempt
            time.sleep(policy.get('retry_delay', 30))

        result = {
            'component': component,
            'healed': healed,
            'attempts': attempts,
            'last_error': last_error,
            'policy': policy
        }

        context.variables[output_var] = result
        return ActionResult(
            success=healed,
            data=result,
            message=f"Self-healing for '{component}': {'success' if healed else 'failed'}"
        )

    def _register_recovery_action(
        self,
        component: str,
        action_var: str,
        context: Any,
        output_var: str
    ) -> ActionResult:
        """Register a recovery action for a component."""
        action = None
        if action_var:
            action = context.variables.get(action_var)

        if not action:
            return ActionResult(
                success=False,
                message=f"Recovery action not found: {action_var}"
            )

        self._recovery_actions[component].append(action)

        result = {
            'component': component,
            'action_registered': True,
            'total_actions': len(self._recovery_actions[component])
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Recovery action registered for '{component}'"
        )

    def _get_healing_status(self, component: str, output_var: str) -> ActionResult:
        """Get healing status for a component."""
        policy = self._healing_policies.get(component, {})
        recovery_actions = self._recovery_actions.get(component, [])

        result = {
            'component': component,
            'policy': policy,
            'recovery_actions_count': len(recovery_actions),
            'enabled': policy.get('enabled', False),
            'auto_heal': policy.get('auto_heal', False)
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Healing status for '{component}': enabled={result['enabled']}"
        )


class DependencyCheckerAction(BaseAction):
    """Check and manage system dependencies.
    
    Supports dependency validation and initialization ordering.
    """
    action_type = "dependency_checker"
    display_name = "依赖检查"
    description = "检查和管理系统依赖"

    def __init__(self):
        super().__init__()
        self._dependencies: Dict[str, List[str]] = defaultdict(list)
        self._initialized: Dict[str, bool] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Check and manage dependencies.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'add_dependency', 'check', 'check_all', 'get_init_order'.
                - component: Component name.
                - dependencies: List of dependency names.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with dependency check result or error.
        """
        operation = params.get('operation', 'add_dependency')
        component = params.get('component', '')
        dependencies = params.get('dependencies', [])
        output_var = params.get('output_var', 'dep_result')

        try:
            if operation == 'add_dependency':
                return self._add_dependency(component, dependencies, output_var)
            elif operation == 'check':
                return self._check_dependency(component, output_var)
            elif operation == 'check_all':
                return self._check_all_dependencies(output_var)
            elif operation == 'get_init_order':
                return self._get_init_order(output_var)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Dependency checker failed: {str(e)}"
            )

    def _add_dependency(
        self, component: str, dependencies: List[str], output_var: str
    ) -> ActionResult:
        """Add dependency relationship."""
        self._dependencies[component] = dependencies

        context.variables[output_var] = {
            'component': component,
            'dependencies': dependencies
        }
        return ActionResult(
            success=True,
            data={'component': component, 'dependencies': dependencies},
            message=f"Dependencies added for '{component}': {dependencies}"
        )

    def _check_dependency(self, component: str, output_var: str) -> ActionResult:
        """Check if dependencies for a component are satisfied."""
        deps = self._dependencies.get(component, [])

        unsatisfied = []
        for dep in deps:
            if not self._initialized.get(dep, False):
                unsatisfied.append(dep)

        result = {
            'component': component,
            'dependencies': deps,
            'satisfied': len(unsatisfied) == 0,
            'unsatisfied': unsatisfied
        }

        context.variables[output_var] = result
        return ActionResult(
            success=len(unsatisfied) == 0,
            data=result,
            message=f"Dependencies for '{component}': {'satisfied' if result['satisfied'] else f'missing: {unsatisfied}'}"
        )

    def _check_all_dependencies(self, output_var: str) -> ActionResult:
        """Check all dependency relationships."""
        results = {}
        all_satisfied = True

        for component in self._dependencies:
            deps = self._dependencies[component]
            unsatisfied = [d for d in deps if not self._initialized.get(d, False)]

            if unsatisfied:
                all_satisfied = False

            results[component] = {
                'dependencies': deps,
                'satisfied': len(unsatisfied) == 0,
                'unsatisfied': unsatisfied
            }

        context.variables[output_var] = {
            'all_satisfied': all_satisfied,
            'results': results
        }
        return ActionResult(
            success=all_satisfied,
            data={'all_satisfied': all_satisfied, 'results': results},
            message=f"Dependency check: {'all satisfied' if all_satisfied else 'some unsatisfied'}"
        )

    def _get_init_order(self, output_var: str) -> ActionResult:
        """Get topological sort of initialization order."""
        # Simple topological sort
        visited = set()
        order = []

        def visit(component):
            if component in visited:
                return
            visited.add(component)

            for dep in self._dependencies.get(component, []):
                if dep not in self._initialized:
                    visit(dep)

            order.append(component)

        for component in self._dependencies:
            if component not in visited:
                visit(component)

        context.variables[output_var] = {
            'init_order': order,
            'count': len(order)
        }
        return ActionResult(
            success=True,
            data={'init_order': order, 'count': len(order)},
            message=f"Initialization order: {order}"
        )
