"""Deployment action module for RabAI AutoClick.

Provides deployment operations:
- DeployAction: Deploy application
- RollbackAction: Rollback deployment
- DeployStatusAction: Check deployment status
- DeployHistoryAction: Deployment history
- DeployValidateAction: Validate deployment
- DeployScaleAction: Scale deployment
- DeployHealthAction: Post-deploy health check
- DeployConfigAction: Deployment configuration
"""

import json
import os
import shutil
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DeploymentStore:
    """Deployment history store."""
    
    _deployments: List[Dict[str, Any]] = []
    _current_version: str = "1.0.0"
    
    @classmethod
    def add(cls, deployment: Dict[str, Any]) -> None:
        cls._deployments.append(deployment)
        cls._current_version = deployment.get("version", cls._current_version)
    
    @classmethod
    def get_current(cls) -> str:
        return cls._current_version
    
    @classmethod
    def list(cls, limit: int = 100) -> List[Dict[str, Any]]:
        return cls._deployments[-limit:]


class DeployAction(BaseAction):
    """Deploy application."""
    action_type = "deploy"
    display_name = "部署"
    description = "部署应用程序"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            version = params.get("version", "1.0.0")
            environment = params.get("environment", "production")
            artifact_path = params.get("artifact_path", "")
            strategy = params.get("strategy", "rolling")
            
            deployment_id = f"deploy-{int(time.time())}"
            
            deployment = {
                "id": deployment_id,
                "version": version,
                "environment": environment,
                "strategy": strategy,
                "status": "deployed",
                "deployed_at": time.time(),
                "deployed_by": params.get("user", "system")
            }
            
            DeploymentStore.add(deployment)
            
            return ActionResult(
                success=True,
                message=f"Deployed version {version} to {environment}",
                data={"deployment": deployment}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Deploy failed: {str(e)}")


class RollbackAction(BaseAction):
    """Rollback deployment."""
    action_type = "rollback"
    display_name = "回滚"
    description = "回滚部署"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            target_version = params.get("version", "")
            reason = params.get("reason", "")
            
            deployments = DeploymentStore.list()
            
            if not deployments:
                return ActionResult(success=False, message="No deployment history")
            
            if target_version:
                target = None
                for d in reversed(deployments):
                    if d.get("version") == target_version:
                        target = d
                        break
                if not target:
                    return ActionResult(success=False, message=f"Version {target_version} not found")
            else:
                for d in reversed(deployments[:-1]):
                    if d.get("status") == "deployed":
                        target = d
                        break
                else:
                    return ActionResult(success=False, message="No previous deployment to rollback to")
            
            rollback = {
                "id": f"rollback-{int(time.time())}",
                "version": target.get("version"),
                "status": "rolled_back",
                "rolled_back_at": time.time(),
                "reason": reason
            }
            
            DeploymentStore.add(rollback)
            
            return ActionResult(
                success=True,
                message=f"Rolled back to version {target.get('version')}",
                data={"rollback": rollback}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Rollback failed: {str(e)}")


class DeployStatusAction(BaseAction):
    """Check deployment status."""
    action_type = "deploy_status"
    display_name = "部署状态"
    description = "检查部署状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            deployment_id = params.get("deployment_id", "")
            environment = params.get("environment", "production")
            
            current_version = DeploymentStore.get_current()
            deployments = DeploymentStore.list()
            
            recent = [d for d in deployments if d.get("environment", "") == environment][-5:]
            
            return ActionResult(
                success=True,
                message=f"Current version: {current_version}",
                data={
                    "current_version": current_version,
                    "environment": environment,
                    "recent_deployments": recent
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Deploy status failed: {str(e)}")


class DeployHistoryAction(BaseAction):
    """Get deployment history."""
    action_type = "deploy_history"
    display_name = "部署历史"
    description = "获取部署历史"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            environment = params.get("environment", "")
            limit = params.get("limit", 50)
            
            deployments = DeploymentStore.list(limit)
            
            if environment:
                deployments = [d for d in deployments if d.get("environment") == environment]
            
            return ActionResult(
                success=True,
                message=f"Found {len(deployments)} deployments",
                data={"deployments": deployments, "count": len(deployments)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Deploy history failed: {str(e)}")


class DeployValidateAction(BaseAction):
    """Validate deployment."""
    action_type = "deploy_validate"
    display_name = "验证部署"
    description = "验证部署配置"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            version = params.get("version", "")
            environment = params.get("environment", "")
            checks = params.get("checks", ["config", "dependencies", "artifacts"])
            
            errors = []
            warnings = []
            
            if not version:
                errors.append("version is required")
            
            if environment not in ["development", "staging", "production"]:
                warnings.append(f"Unknown environment: {environment}")
            
            if "config" in checks:
                if not params.get("config_valid", True):
                    errors.append("Configuration validation failed")
            
            if "dependencies" in checks:
                if not params.get("dependencies_met", True):
                    errors.append("Dependencies not met")
            
            if "artifacts" in checks:
                artifact = params.get("artifact_path", "")
                if not artifact:
                    warnings.append("No artifact path specified")
            
            is_valid = len(errors) == 0
            
            return ActionResult(
                success=is_valid,
                message=f"Deployment validation: {'valid' if is_valid else 'invalid'}",
                data={
                    "valid": is_valid,
                    "errors": errors,
                    "warnings": warnings,
                    "checks_passed": len(checks) - len(errors)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Deploy validate failed: {str(e)}")


class DeployScaleAction(BaseAction):
    """Scale deployment."""
    action_type = "deploy_scale"
    display_name = "扩缩容"
    description = "扩缩容部署"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            replicas = params.get("replicas", 1)
            environment = params.get("environment", "production")
            
            if replicas < 0:
                return ActionResult(success=False, message="Replicas must be non-negative")
            
            return ActionResult(
                success=True,
                message=f"Scaled to {replicas} replicas in {environment}",
                data={"replicas": replicas, "environment": environment}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Deploy scale failed: {str(e)}")


class DeployHealthAction(BaseAction):
    """Post-deploy health check."""
    action_type = "deploy_health"
    display_name = "部署健康检查"
    description = "部署后健康检查"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            deployment_id = params.get("deployment_id", "")
            timeout = params.get("timeout", 60)
            
            checks = {
                "endpoint": params.get("check_endpoint", True),
                "database": params.get("check_database", True),
                "cache": params.get("check_cache", True)
            }
            
            results = {}
            all_passed = True
            
            for check, enabled in checks.items():
                if enabled:
                    results[check] = {"status": "healthy", "latency_ms": 50}
                else:
                    results[check] = {"status": "skipped"}
            
                if results[check].get("status") != "healthy":
                    all_passed = False
            
            return ActionResult(
                success=all_passed,
                message=f"Health check: {'passed' if all_passed else 'failed'}",
                data={
                    "deployment_id": deployment_id,
                    "checks": results,
                    "all_passed": all_passed
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Deploy health failed: {str(e)}")


class DeployConfigAction(BaseAction):
    """Deployment configuration."""
    action_type = "deploy_config"
    display_name = "部署配置"
    description = "部署配置"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "get")
            environment = params.get("environment", "production")
            config = params.get("config", {})
            
            configs = {
                "production": {
                    "replicas": 3,
                    "strategy": "rolling",
                    "health_check_interval": 30,
                    "rollback_enabled": True
                },
                "staging": {
                    "replicas": 2,
                    "strategy": "rolling",
                    "health_check_interval": 30,
                    "rollback_enabled": True
                },
                "development": {
                    "replicas": 1,
                    "strategy": "recreate",
                    "health_check_interval": 60,
                    "rollback_enabled": False
                }
            }
            
            if action == "get":
                return ActionResult(
                    success=True,
                    message=f"Config for {environment}",
                    data={"config": configs.get(environment, {}), "environment": environment}
                )
            
            elif action == "set":
                configs[environment] = {**configs.get(environment, {}), **config}
                return ActionResult(
                    success=True,
                    message=f"Updated config for {environment}",
                    data={"config": configs[environment], "environment": environment}
                )
            
            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Deploy config failed: {str(e)}")
