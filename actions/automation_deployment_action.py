"""
Automation Deployment Action Module

Provides deployment automation, rollout strategies, and deployment monitoring.
"""
from typing import Any, Optional, Literal
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio


class DeploymentStrategy(Enum):
    """Deployment strategy types."""
    RECREATE = "recreate"
    ROLLING = "rolling"
    BLUE_GREEN = "blue_green"
    CANARY = "canary"
    FEATURE_TOGGLE = "feature_toggle"


class DeploymentStatus(Enum):
    """Deployment status."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"
    CANCELLED = "cancelled"


@dataclass
class DeploymentConfig:
    """Configuration for deployment."""
    strategy: DeploymentStrategy
    max_surge: int = 1
    max_unavailable: int = 0
    canary_percentage: float = 10.0
    health_check_interval: float = 10.0
    health_check_timeout: float = 30.0
    rollback_on_failure: bool = True


@dataclass
class DeploymentTarget:
    """A deployment target (server, instance, etc.)."""
    target_id: str
    name: str
    version: str
    healthy: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class DeploymentStep:
    """A single step in the deployment."""
    step_id: str
    name: str
    target_id: str
    action: str
    status: DeploymentStatus = DeploymentStatus.PENDING
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error: Optional[str] = None


@dataclass
class Deployment:
    """A deployment operation."""
    deployment_id: str
    version: str
    strategy: DeploymentStrategy
    targets: list[DeploymentTarget]
    steps: list[DeploymentStep]
    status: DeploymentStatus = DeploymentStatus.PENDING
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    rollback_from: Optional[str] = None


@dataclass
class DeploymentResult:
    """Result of a deployment operation."""
    success: bool
    deployment_id: str
    deployed_targets: int
    failed_targets: int
    duration_seconds: float
    error: Optional[str] = None
    rollback_performed: bool = False


class AutomationDeploymentAction:
    """Main deployment automation action handler."""
    
    def __init__(self):
        self._deployments: dict[str, Deployment] = {}
        self._deployment_history: list[Deployment] = []
        self._hooks: dict[str, list[Callable]] = {
            "pre_deploy": [],
            "post_deploy": [],
            "pre_rollback": [],
            "post_rollback": [],
            "health_check": []
        }
    
    def register_hook(
        self,
        event: str,
        hook: Callable[[dict], Awaitable]
    ) -> "AutomationDeploymentAction":
        """Register a deployment hook."""
        if event in self._hooks:
            self._hooks[event].append(hook)
        return self
    
    async def deploy(
        self,
        deployment_id: str,
        version: str,
        targets: list[DeploymentTarget],
        config: DeploymentConfig
    ) -> DeploymentResult:
        """
        Execute a deployment.
        
        Args:
            deployment_id: Unique identifier for deployment
            version: Version to deploy
            targets: List of deployment targets
            config: Deployment configuration
            
        Returns:
            DeploymentResult with deployment outcome
        """
        start_time = datetime.now()
        
        # Create deployment record
        deployment = Deployment(
            deployment_id=deployment_id,
            version=version,
            strategy=config.strategy,
            targets=targets,
            steps=[],
            started_at=start_time
        )
        
        self._deployments[deployment_id] = deployment
        
        # Run pre-deploy hooks
        await self._run_hooks("pre_deploy", {"deployment": deployment})
        
        try:
            # Execute deployment based on strategy
            if config.strategy == DeploymentStrategy.ROLLING:
                result = await self._rolling_deploy(deployment, config)
            elif config.strategy == DeploymentStrategy.BLUE_GREEN:
                result = await self._blue_green_deploy(deployment, config)
            elif config.strategy == DeploymentStrategy.CANARY:
                result = await self._canary_deploy(deployment, config)
            elif config.strategy == DeploymentStrategy.RECREATE:
                result = await self._recreate_deploy(deployment, config)
            else:
                result = DeploymentResult(
                    success=False,
                    deployment_id=deployment_id,
                    deployed_targets=0,
                    failed_targets=len(targets),
                    duration_seconds=0,
                    error=f"Unknown strategy: {config.strategy}"
                )
            
            # Run post-deploy hooks
            if result.success:
                await self._run_hooks("post_deploy", {"deployment": deployment})
                deployment.status = DeploymentStatus.COMPLETED
            else:
                deployment.status = DeploymentStatus.FAILED
                
                if config.rollback_on_failure:
                    await self.rollback(deployment_id)
                    result.rollback_performed = True
            
            deployment.completed_at = datetime.now()
            
            return result
            
        except Exception as e:
            deployment.status = DeploymentStatus.FAILED
            deployment.completed_at = datetime.now()
            
            return DeploymentResult(
                success=False,
                deployment_id=deployment_id,
                deployed_targets=0,
                failed_targets=len(targets),
                duration_seconds=(datetime.now() - start_time).total_seconds(),
                error=str(e)
            )
    
    async def _rolling_deploy(
        self,
        deployment: Deployment,
        config: DeploymentConfig
    ) -> DeploymentResult:
        """Execute rolling deployment."""
        deployed = 0
        failed = 0
        start_time = datetime.now()
        
        for target in deployment.targets:
            # Create step
            step = DeploymentStep(
                step_id=f"{deployment.deployment_id}:{target.target_id}",
                name=f"Deploy to {target.name}",
                target_id=target.target_id,
                action="deploy"
            )
            deployment.steps.append(step)
            
            try:
                step.status = DeploymentStatus.IN_PROGRESS
                step.started_at = datetime.now()
                
                # Perform health check before deployment
                if not await self._health_check(target):
                    raise Exception(f"Target {target.target_id} failed health check")
                
                # Deploy (simulated)
                await asyncio.sleep(0.1)
                
                step.status = DeploymentStatus.COMPLETED
                step.completed_at = datetime.now()
                target.version = deployment.version
                deployed += 1
                
                # Wait for rollout
                if deployed < len(deployment.targets):
                    await asyncio.sleep(0.05)
                
            except Exception as e:
                step.status = DeploymentStatus.FAILED
                step.error = str(e)
                failed += 1
        
        return DeploymentResult(
            success=failed == 0,
            deployment_id=deployment.deployment_id,
            deployed_targets=deployed,
            failed_targets=failed,
            duration_seconds=(datetime.now() - start_time).total_seconds()
        )
    
    async def _blue_green_deploy(
        self,
        deployment: Deployment,
        config: DeploymentConfig
    ) -> DeploymentResult:
        """Execute blue-green deployment."""
        start_time = datetime.now()
        
        # Deploy to green environment (all at once)
        for target in deployment.targets:
            step = DeploymentStep(
                step_id=f"{deployment.deployment_id}:{target.target_id}:green",
                name=f"Deploy green to {target.name}",
                target_id=target.target_id,
                action="deploy_green"
            )
            deployment.steps.append(step)
            
            step.status = DeploymentStatus.IN_PROGRESS
            step.started_at = datetime.now()
            
            # Simulate deployment
            await asyncio.sleep(0.1)
            
            step.status = DeploymentStatus.COMPLETED
            step.completed_at = datetime.now()
        
        # Run validation
        await asyncio.sleep(0.05)
        
        # Switch traffic (simulated)
        for target in deployment.targets:
            step = DeploymentStep(
                step_id=f"{deployment.deployment_id}:{target.target_id}:switch",
                name=f"Switch traffic for {target.name}",
                target_id=target.target_id,
                action="switch_traffic"
            )
            deployment.steps.append(step)
            step.status = DeploymentStatus.COMPLETED
            step.completed_at = datetime.now()
            target.version = deployment.version
        
        return DeploymentResult(
            success=True,
            deployment_id=deployment.deployment_id,
            deployed_targets=len(deployment.targets),
            failed_targets=0,
            duration_seconds=(datetime.now() - start_time).total_seconds()
        )
    
    async def _canary_deploy(
        self,
        deployment: Deployment,
        config: DeploymentConfig
    ) -> DeploymentResult:
        """Execute canary deployment."""
        start_time = datetime.now()
        deployed = 0
        failed = 0
        
        # Calculate canary count
        canary_count = max(1, int(len(deployment.targets) * config.canary_percentage / 100))
        canary_targets = deployment.targets[:canary_count]
        full_targets = deployment.targets[canary_count:]
        
        # Deploy canary
        for target in canary_targets:
            step = DeploymentStep(
                step_id=f"{deployment.deployment_id}:{target.target_id}:canary",
                name=f"Canary deploy to {target.name}",
                target_id=target.target_id,
                action="deploy_canary"
            )
            deployment.steps.append(step)
            step.status = DeploymentStatus.IN_PROGRESS
            step.started_at = datetime.now()
            
            await asyncio.sleep(0.1)
            
            step.status = DeploymentStatus.COMPLETED
            step.completed_at = datetime.now()
            target.version = deployment.version
            deployed += 1
        
        # Monitor canary
        await asyncio.sleep(0.1)
        
        # If canary is healthy, deploy to remaining
        for target in full_targets:
            step = DeploymentStep(
                step_id=f"{deployment.deployment_id}:{target.target_id}:full",
                name=f"Full deploy to {target.name}",
                target_id=target.target_id,
                action="deploy_full"
            )
            deployment.steps.append(step)
            step.status = DeploymentStatus.IN_PROGRESS
            step.started_at = datetime.now()
            
            await asyncio.sleep(0.1)
            
            step.status = DeploymentStatus.COMPLETED
            step.completed_at = datetime.now()
            target.version = deployment.version
            deployed += 1
        
        return DeploymentResult(
            success=True,
            deployment_id=deployment.deployment_id,
            deployed_targets=deployed,
            failed_targets=failed,
            duration_seconds=(datetime.now() - start_time).total_seconds()
        )
    
    async def _recreate_deploy(
        self,
        deployment: Deployment,
        config: DeploymentConfig
    ) -> DeploymentResult:
        """Execute recreate deployment (destroy and redeploy)."""
        start_time = datetime.now()
        
        # Delete old instances
        for target in deployment.targets:
            step = DeploymentStep(
                step_id=f"{deployment.deployment_id}:{target.target_id}:destroy",
                name=f"Destroy {target.name}",
                target_id=target.target_id,
                action="destroy"
            )
            deployment.steps.append(step)
            step.status = DeploymentStatus.IN_PROGRESS
            step.started_at = datetime.now()
            
            await asyncio.sleep(0.05)
            
            step.status = DeploymentStatus.COMPLETED
            step.completed_at = datetime.now()
        
        # Create new instances
        for target in deployment.targets:
            step = DeploymentStep(
                step_id=f"{deployment.deployment_id}:{target.target_id}:create",
                name=f"Create {target.name}",
                target_id=target.target_id,
                action="create"
            )
            deployment.steps.append(step)
            step.status = DeploymentStatus.IN_PROGRESS
            step.started_at = datetime.now()
            
            await asyncio.sleep(0.1)
            
            step.status = DeploymentStatus.COMPLETED
            step.completed_at = datetime.now()
            target.version = deployment.version
        
        return DeploymentResult(
            success=True,
            deployment_id=deployment.deployment_id,
            deployed_targets=len(deployment.targets),
            failed_targets=0,
            duration_seconds=(datetime.now() - start_time).total_seconds()
        )
    
    async def _health_check(self, target: DeploymentTarget) -> bool:
        """Perform health check on a target."""
        # Run health check hooks
        for hook in self._hooks["health_check"]:
            try:
                result = await hook({"target": target})
                if not result:
                    return False
            except Exception:
                return False
        
        return target.healthy
    
    async def _run_hooks(self, event: str, context: dict):
        """Run hooks for an event."""
        if event not in self._hooks:
            return
        
        for hook in self._hooks[event]:
            try:
                await hook(context)
            except Exception:
                pass  # Log but don't fail deployment
    
    async def rollback(self, deployment_id: str) -> bool:
        """Rollback a deployment."""
        if deployment_id not in self._deployments:
            return False
        
        deployment = self._deployments[deployment_id]
        
        # Run pre-rollback hooks
        await self._run_hooks("pre_rollback", {"deployment": deployment})
        
        # Rollback logic (simplified)
        deployment.status = DeploymentStatus.ROLLED_BACK
        deployment.completed_at = datetime.now()
        
        # Run post-rollback hooks
        await self._run_hooks("post_rollback", {"deployment": deployment})
        
        return True
    
    async def get_deployment_status(self, deployment_id: str) -> Optional[dict[str, Any]]:
        """Get status of a deployment."""
        if deployment_id not in self._deployments:
            return None
        
        deployment = self._deployments[deployment_id]
        
        return {
            "deployment_id": deployment.deployment_id,
            "version": deployment.version,
            "strategy": deployment.strategy.value,
            "status": deployment.status.value,
            "started_at": deployment.started_at.isoformat() if deployment.started_at else None,
            "completed_at": deployment.completed_at.isoformat() if deployment.completed_at else None,
            "total_steps": len(deployment.steps),
            "completed_steps": len([s for s in deployment.steps if s.status == DeploymentStatus.COMPLETED]),
            "failed_steps": len([s for s in deployment.steps if s.status == DeploymentStatus.FAILED])
        }
    
    def list_deployments(
        self,
        status: Optional[DeploymentStatus] = None,
        limit: int = 10
    ) -> list[dict[str, Any]]:
        """List deployments."""
        deployments = list(self._deployments.values())
        
        if status:
            deployments = [d for d in deployments if d.status == status]
        
        deployments.sort(key=lambda d: d.started_at or datetime.min, reverse=True)
        
        return [
            {
                "deployment_id": d.deployment_id,
                "version": d.version,
                "strategy": d.strategy.value,
                "status": d.status.value,
                "started_at": d.started_at.isoformat() if d.started_at else None
            }
            for d in deployments[:limit]
        ]
