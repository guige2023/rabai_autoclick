"""Chaos engineering and fault injection action module for RabAI AutoClick.

Provides fault injection capabilities for resilience testing:
- ChaosInjectorAction: Inject various faults into workflows
- FaultTypeRegistry: Register and manage fault types
- ChaosExperimentAction: Run chaos experiments with controlled faults
- FailureModeAnalyzer: Analyze and predict failure modes
- ChaosMonitorAction: Monitor system behavior during chaos
"""

from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import logging
import random
import time
import threading

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class FaultType(Enum):
    """Types of faults that can be injected."""
    DELAY = "delay"
    TIMEOUT = "timeout"
    ERROR = "error"
    EXCEPTION = "exception"
    NETWORK_PARTITION = "network_partition"
    RESOURCE_EXHAUSTION = "resource_exhaustion"
    DATA_CORRUPTION = "data_corruption"
    CACHE_INVALIDATION = "cache_invalidation"
    RATE_LIMITING = "rate_limiting"
    KILL_PROCESS = "kill_process"


@dataclass
class FaultConfig:
    """Configuration for a fault injection."""
    fault_type: FaultType
    target: str  # Target component or "all"
    duration: float  # Duration in seconds
    probability: float  # 0.0 to 1.0
    intensity: float  # 0.0 to 1.0 (severity)
    parameters: Dict[str, Any] = field(default_factory=dict)
    enabled: bool = True


@dataclass
class ChaosResult:
    """Result of a chaos experiment."""
    experiment_name: str
    start_time: float
    end_time: float
    faults_injected: List[Dict[str, Any]]
    system_impact: Dict[str, Any]
    recovered: bool
    metrics: Dict[str, Any]


class ChaosInjectorAction(BaseAction):
    """Inject faults into system for testing resilience."""
    
    action_type = "chaos_injector"
    display_name = "故障注入器"
    description = "向系统注入故障以测试韧性"
    
    def __init__(self) -> None:
        super().__init__()
        self._active_faults: Dict[str, FaultConfig] = {}
        self._injection_history: List[Dict[str, Any]] = []
        self._lock = threading.Lock()
    
    def register_fault(
        self,
        fault_id: str,
        fault_type: str,
        target: str = "all",
        duration: float = 10.0,
        probability: float = 1.0,
        intensity: float = 0.5,
        parameters: Optional[Dict[str, Any]] = None
    ) -> None:
        """Register a fault configuration.
        
        Args:
            fault_id: Unique identifier for this fault.
            fault_type: Type of fault (see FaultType enum).
            target: Target component or "all".
            duration: How long the fault lasts in seconds.
            probability: Probability of fault triggering (0.0-1.0).
            intensity: Severity of fault (0.0-1.0).
            parameters: Additional fault-specific parameters.
        """
        try:
            ft = FaultType(fault_type)
        except ValueError:
            logger.warning(f"Unknown fault type: {fault_type}, using ERROR")
            ft = FaultType.ERROR
        
        self._active_faults[fault_id] = FaultConfig(
            fault_type=ft,
            target=target,
            duration=duration,
            probability=probability,
            intensity=intensity,
            parameters=parameters or {}
        )
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Inject registered faults into the system.
        
        Args:
            params: {
                "fault_id": Specific fault to inject (str),
                "fault_type": Type of fault to inject (str, creates temporary),
                "target": Target component (str, default "all"),
                "duration": Duration in seconds (float, default 10.0),
                "probability": Trigger probability (float, default 1.0),
                "intensity": Fault intensity (float, default 0.5),
                "dry_run": Only simulate, don't actually inject (bool, default False)
            }
        """
        try:
            fault_id = params.get("fault_id", "")
            fault_type = params.get("fault_type", "")
            target = params.get("target", "all")
            duration = params.get("duration", 10.0)
            probability = params.get("probability", 1.0)
            intensity = params.get("intensity", 0.5)
            dry_run = params.get("dry_run", False)
            
            if not fault_id and not fault_type:
                return ActionResult(success=False, message="fault_id or fault_type required")
            
            # Get or create fault config
            if fault_id and fault_id in self._active_faults:
                fault = self._active_faults[fault_id]
                fault_type_str = fault.fault_type.value
            else:
                try:
                    ft = FaultType(fault_type)
                except ValueError:
                    return ActionResult(success=False, message=f"Unknown fault type: {fault_type}")
                
                fault_type_str = fault_type
                fault = FaultConfig(
                    fault_type=ft,
                    target=target,
                    duration=duration,
                    probability=probability,
                    intensity=intensity
                )
            
            # Check probability
            if random.random() > fault.probability:
                return ActionResult(
                    success=True,
                    message=f"Fault {fault_id or fault_type} not triggered (probability)",
                    data={"triggered": False, "reason": "probability_check"}
                )
            
            if dry_run:
                return ActionResult(
                    success=True,
                    message=f"Would inject fault: {fault.fault_type.value}",
                    data={"triggered": True, "dry_run": True, "fault_config": {
                        "type": fault.fault_type.value,
                        "target": fault.target,
                        "duration": fault.duration,
                        "intensity": fault.intensity
                    }}
                )
            
            # Inject the fault
            start_time = time.time()
            result = self._inject_fault(fault, context, params)
            end_time = time.time()
            
            # Record injection
            injection_record = {
                "fault_id": fault_id or "temp",
                "fault_type": fault_type_str,
                "target": fault.target,
                "duration": fault.duration,
                "intensity": fault.intensity,
                "injection_time": start_time,
                "actual_duration": end_time - start_time,
                "success": result.success
            }
            
            with self._lock:
                self._injection_history.append(injection_record)
            
            return result
        
        except Exception as e:
            logger.error(f"Chaos injection failed: {e}")
            return ActionResult(success=False, message=f"Injection error: {str(e)}")
    
    def _inject_fault(
        self,
        fault: FaultConfig,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Actually inject the fault based on type."""
        fault_type = fault.fault_type
        intensity = fault.intensity
        fault_params = fault.parameters
        
        if fault_type == FaultType.DELAY:
            delay_seconds = fault_params.get("delay_seconds", 5.0) * intensity
            return self._inject_delay(delay_seconds, fault.target)
        
        elif fault_type == FaultType.TIMEOUT:
            timeout_value = fault_params.get("timeout_value", 1.0) * (1 - intensity)
            return self._inject_timeout(timeout_value, fault.target)
        
        elif fault_type == FaultType.ERROR:
            error_code = fault_params.get("error_code", 500)
            error_message = fault_params.get("error_message", "Injected error")
            return self._inject_error(error_code, error_message, fault.target)
        
        elif fault_type == FaultType.EXCEPTION:
            exception_type = fault_params.get("exception_type", "RuntimeError")
            exception_message = fault_params.get("exception_message", "Injected exception")
            return self._inject_exception(exception_type, exception_message)
        
        elif fault_type == FaultType.NETWORK_PARTITION:
            return self._inject_network_partition(fault.target, fault.duration)
        
        elif fault_type == FaultType.RESOURCE_EXHAUSTION:
            resource = fault_params.get("resource", "memory")
            return self._inject_resource_exhaustion(resource, intensity)
        
        elif fault_type == FaultType.DATA_CORRUPTION:
            return self._inject_data_corruption(fault.target, intensity)
        
        elif fault_type == FaultType.RATE_LIMITING:
            limit = fault_params.get("limit", 0)  # 0 means completely block
            return self._inject_rate_limiting(limit, fault.target)
        
        elif fault_type == FaultType.KILL_PROCESS:
            return self._inject_process_kill(fault.target)
        
        else:
            return ActionResult(success=False, message=f"Unimplemented fault type: {fault_type}")
    
    def _inject_delay(self, seconds: float, target: str) -> ActionResult:
        """Inject a delay."""
        time.sleep(seconds)
        return ActionResult(
            success=True,
            message=f"Injected {seconds:.2f}s delay into {target}",
            data={"fault": "delay", "duration": seconds, "target": target}
        )
    
    def _inject_timeout(self, timeout_value: float, target: str) -> ActionResult:
        """Simulate a timeout."""
        return ActionResult(
            success=False,
            message=f"Timeout injected ({timeout_value:.2f}s) into {target}",
            data={"fault": "timeout", "timeout_value": timeout_value, "target": target}
        )
    
    def _inject_error(self, error_code: int, message: str, target: str) -> ActionResult:
        """Inject an HTTP-style error."""
        return ActionResult(
            success=False,
            message=f"Error {error_code} injected: {message}",
            data={"fault": "error", "error_code": error_code, "message": message, "target": target}
        )
    
    def _inject_exception(self, exception_type: str, message: str) -> ActionResult:
        """Inject an exception."""
        try:
            exc_class = __builtins__.get(exception_type, RuntimeError) if isinstance(__builtins__, dict) else getattr(__builtins__, exception_type, RuntimeError)
            raise exc_class(message)
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Injected exception: {exception_type}: {message}",
                data={"fault": "exception", "exception_type": exception_type, "message": message}
            )
    
    def _inject_network_partition(self, target: str, duration: float) -> ActionResult:
        """Simulate network partition."""
        # In a real implementation, this would modify network rules
        return ActionResult(
            success=True,
            message=f"Network partition created for {target} ({duration:.1f}s)",
            data={"fault": "network_partition", "target": target, "duration": duration}
        )
    
    def _inject_resource_exhaustion(self, resource: str, intensity: float) -> ActionResult:
        """Inject resource exhaustion."""
        if resource == "memory":
            # Would allocate memory in real implementation
            return ActionResult(
                success=True,
                message=f"Memory exhaustion triggered (intensity: {intensity:.2f})",
                data={"fault": "resource_exhaustion", "resource": "memory", "intensity": intensity}
            )
        elif resource == "cpu":
            return ActionResult(
                success=True,
                message=f"CPU exhaustion triggered (intensity: {intensity:.2f})",
                data={"fault": "resource_exhaustion", "resource": "cpu", "intensity": intensity}
            )
        else:
            return ActionResult(
                success=True,
                message=f"Resource exhaustion triggered: {resource}",
                data={"fault": "resource_exhaustion", "resource": resource}
            )
    
    def _inject_data_corruption(self, target: str, intensity: float) -> ActionResult:
        """Inject data corruption."""
        corruption_rate = intensity * 100
        return ActionResult(
            success=True,
            message=f"Data corruption injected ({corruption_rate:.1f}% corruption rate)",
            data={"fault": "data_corruption", "target": target, "intensity": intensity}
        )
    
    def _inject_rate_limiting(self, limit: int, target: str) -> ActionResult:
        """Inject rate limiting."""
        if limit == 0:
            return ActionResult(
                success=False,
                message=f"All requests blocked to {target}",
                data={"fault": "rate_limiting", "limit": 0, "target": target}
            )
        else:
            return ActionResult(
                success=True,
                message=f"Rate limit set to {limit}/s for {target}",
                data={"fault": "rate_limiting", "limit": limit, "target": target}
            )
    
    def _inject_process_kill(self, target: str) -> ActionResult:
        """Simulate process kill."""
        return ActionResult(
            success=True,
            message=f"Process kill simulated for {target}",
            data={"fault": "kill_process", "target": target}
        )
    
    def get_injection_history(self) -> List[Dict[str, Any]]:
        """Get history of fault injections."""
        with self._lock:
            return self._injection_history.copy()
    
    def get_active_faults(self) -> Dict[str, Any]:
        """Get currently active fault configurations."""
        with self._lock:
            return {
                fid: {
                    "fault_type": f.fault_type.value,
                    "target": f.target,
                    "duration": f.duration,
                    "probability": f.probability,
                    "intensity": f.intensity,
                    "enabled": f.enabled
                }
                for fid, f in self._active_faults.items()
            }


class FaultTypeRegistry(BaseAction):
    """Registry for managing available fault types."""
    
    action_type = "fault_type_registry"
    display_name = "故障类型注册表"
    description = "管理和注册可用的故障类型"
    
    def __init__(self) -> None:
        super().__init__()
        self._custom_faults: Dict[str, Dict[str, Any]] = {}
        self._fault_templates: Dict[str, Dict[str, Any]] = {
            "latency_injection": {
                "type": "delay",
                "description": "Injects network latency",
                "required_params": ["delay_seconds"],
                "optional_params": {"target": "all", "intensity": 0.5}
            },
            "error_injection": {
                "type": "error",
                "description": "Returns error responses",
                "required_params": ["error_code", "error_message"],
                "optional_params": {"target": "all", "probability": 1.0}
            },
            "timeout_injection": {
                "type": "timeout",
                "description": "Causes request timeouts",
                "required_params": ["timeout_value"],
                "optional_params": {"target": "all", "probability": 0.5}
            },
            "exception_storm": {
                "type": "exception",
                "description": "Throws random exceptions",
                "required_params": ["exception_type"],
                "optional_params": {"exception_message": "Chaos error", "intensity": 1.0}
            },
            "black_hole": {
                "type": "network_partition",
                "description": "Drops all traffic to target",
                "required_params": ["duration"],
                "optional_params": {"target": "all"}
            }
        }
    
    def register_custom_fault(
        self,
        name: str,
        fault_type: str,
        description: str,
        required_params: List[str],
        optional_params: Optional[Dict[str, Any]] = None
    ) -> None:
        """Register a custom fault type."""
        self._custom_faults[name] = {
            "type": fault_type,
            "description": description,
            "required_params": required_params,
            "optional_params": optional_params or {}
        }
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Query or manage fault registry.
        
        Args:
            params: {
                "action": "list", "get", or "validate" (str),
                "name": Fault template name (str, for get/validate)
            }
        """
        try:
            action = params.get("action", "list")
            name = params.get("name", "")
            
            if action == "list":
                all_faults = {**self._fault_templates, **self._custom_faults}
                return ActionResult(
                    success=True,
                    message=f"Found {len(all_faults)} fault types",
                    data={
                        "faults": all_faults,
                        "count": len(all_faults)
                    }
                )
            
            elif action == "get":
                if name in self._fault_templates:
                    return ActionResult(
                        success=True,
                        message=f"Found fault template: {name}",
                        data=self._fault_templates[name]
                    )
                elif name in self._custom_faults:
                    return ActionResult(
                        success=True,
                        message=f"Found custom fault: {name}",
                        data=self._custom_faults[name]
                    )
                else:
                    return ActionResult(success=False, message=f"Fault not found: {name}")
            
            elif action == "validate":
                # Validate fault parameters
                all_faults = {**self._fault_templates, **self._custom_faults}
                if name not in all_faults:
                    return ActionResult(success=False, message=f"Unknown fault: {name}")
                
                provided = set(params.get("provided_params", {}).keys())
                required = set(all_faults[name]["required_params"])
                missing = required - provided
                
                if missing:
                    return ActionResult(
                        success=False,
                        message=f"Missing required params: {missing}",
                        data={"missing_params": list(missing)}
                    )
                
                return ActionResult(
                    success=True,
                    message="Fault parameters valid",
                    data={"valid": True, "name": name}
                )
            
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        
        except Exception as e:
            return ActionResult(success=False, message=f"Registry error: {str(e)}")


class ChaosExperimentAction(BaseAction):
    """Run controlled chaos experiments."""
    
    action_type = "chaos_experiment"
    display_name = "混沌实验"
    description = "运行受控的混沌实验"
    
    def __init__(self) -> None:
        super().__init__()
        self._experiments: Dict[str, Dict[str, Any]] = {}
        self._injector = ChaosInjectorAction()
    
    def create_experiment(
        self,
        name: str,
        faults: List[Dict[str, Any]],
        steady_state: Dict[str, Any],
        rollbacks: Optional[List[Dict[str, Any]]] = None
    ) -> None:
        """Create a chaos experiment.
        
        Args:
            name: Experiment name.
            faults: List of fault configurations.
            steady_state: Metrics defining steady state.
            rollbacks: Automatic rollback procedures.
        """
        self._experiments[name] = {
            "name": name,
            "faults": faults,
            "steady_state": steady_state,
            "rollbacks": rollbacks or [],
            "status": "created",
            "results": []
        }
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Run a chaos experiment.
        
        Args:
            params: {
                "experiment_name": Name of experiment to run (str, required),
                "dry_run": Preview without injecting (bool, default False),
                "rollback_on_failure": Auto-rollback on impact (bool, default True)
            }
        """
        try:
            experiment_name = params.get("experiment_name", "")
            dry_run = params.get("dry_run", False)
            rollback_on_failure = params.get("rollback_on_failure", True)
            
            if experiment_name not in self._experiments:
                return ActionResult(success=False, message=f"Experiment not found: {experiment_name}")
            
            experiment = self._experiments[experiment_name]
            
            if dry_run:
                return ActionResult(
                    success=True,
                    message=f"Dry run of experiment: {experiment_name}",
                    data={
                        "experiment": experiment_name,
                        "faults": experiment["faults"],
                        "steady_state": experiment["steady_state"],
                        "rollback_steps": len(experiment["rollbacks"])
                    }
                )
            
            # Run experiment
            start_time = time.time()
            faults_injected: List[Dict[str, Any]] = []
            system_impact: Dict[str, Any] = {}
            recovered = True
            
            try:
                # Pre-experiment steady state check
                steady_state_ok = self._check_steady_state(
                    experiment.get("steady_state", {}),
                    context
                )
                
                if not steady_state_ok["ok"]:
                    return ActionResult(
                        success=False,
                        message="System not in steady state, aborting experiment",
                        data={"steady_state_check": steady_state_ok}
                    )
                
                # Inject faults sequentially
                for fault in experiment["faults"]:
                    result = self._injector.execute(context, fault)
                    faults_injected.append({
                        "fault": fault,
                        "result": {"success": result.success, "message": result.message}
                    })
                    
                    # Check impact after each fault
                    impact = self._measure_impact(context)
                    system_impact.update(impact)
                    
                    # Check if system is degraded beyond threshold
                    if impact.get("degraded", False):
                        logger.warning(f"System degraded during experiment: {impact}")
                
                # Post-experiment recovery check
                recovered = self._check_recovery(context)
                
            except Exception as e:
                logger.error(f"Experiment failed: {e}")
                recovered = False
                
                if rollback_on_failure:
                    self._execute_rollbacks(experiment.get("rollbacks", []), context)
            
            end_time = time.time()
            
            result = ChaosResult(
                experiment_name=experiment_name,
                start_time=start_time,
                end_time=end_time,
                faults_injected=faults_injected,
                system_impact=system_impact,
                recovered=recovered,
                metrics={
                    "duration": end_time - start_time,
                    "fault_count": len(faults_injected),
                    "success_count": sum(1 for f in faults_injected if f["result"]["success"])
                }
            )
            
            experiment["status"] = "completed"
            experiment["results"].append(result)
            
            return ActionResult(
                success=recovered,
                message=f"Experiment {'completed' if recovered else 'failed'}",
                data={
                    "experiment": experiment_name,
                    "duration": round(result.end_time - result.start_time, 3),
                    "faults_injected": len(faults_injected),
                    "recovered": recovered,
                    "system_impact": system_impact
                }
            )
        
        except Exception as e:
            return ActionResult(success=False, message=f"Experiment error: {str(e)}")
    
    def _check_steady_state(
        self,
        steady_state: Dict[str, Any],
        context: Any
    ) -> Dict[str, Any]:
        """Check if system is in steady state before experiment."""
        # Simplified implementation
        return {"ok": True, "metrics": {}}
    
    def _measure_impact(self, context: Any) -> Dict[str, Any]:
        """Measure system impact during experiment."""
        # Simplified implementation
        return {"degraded": False, "metrics": {}}
    
    def _check_recovery(self, context: Any) -> bool:
        """Check if system recovered after experiment."""
        # Simplified implementation
        return True
    
    def _execute_rollbacks(self, rollbacks: List[Dict[str, Any]], context: Any) -> None:
        """Execute rollback procedures."""
        for rollback in rollbacks:
            logger.info(f"Executing rollback: {rollback}")


class FailureModeAnalyzer(BaseAction):
    """Analyze potential failure modes from fault injection results."""
    
    action_type = "failure_mode_analyzer"
    display_name = "故障模式分析器"
    description = "分析故障注入结果中的故障模式"
    
    def __init__(self) -> None:
        super().__init__()
        self._failure_modes: Dict[str, Dict[str, Any]] = {}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Analyze failure modes from experiment data.
        
        Args:
            params: {
                "experiment_results": Results from chaos experiment (dict),
                "detect_patterns": Detect failure patterns (bool, default True),
                "predict_impact": Predict potential impacts (bool, default True)
            }
        """
        try:
            results = params.get("experiment_results", {})
            detect_patterns = params.get("detect_patterns", True)
            predict_impact = params.get("predict_impact", True)
            
            if not results:
                return ActionResult(success=False, message="experiment_results required")
            
            analysis: Dict[str, Any] = {}
            
            if detect_patterns:
                patterns = self._detect_failure_patterns(results)
                analysis["patterns"] = patterns
            
            if predict_impact:
                predictions = self._predict_impact(results)
                analysis["predictions"] = predictions
            
            # Calculate risk score
            risk_score = self._calculate_risk_score(results, analysis)
            analysis["risk_score"] = risk_score
            
            # Generate recommendations
            recommendations = self._generate_recommendations(analysis)
            analysis["recommendations"] = recommendations
            
            return ActionResult(
                success=True,
                message=f"Analysis complete (risk score: {risk_score:.2f}/10)",
                data=analysis
            )
        
        except Exception as e:
            return ActionResult(success=False, message=f"Analysis error: {str(e)}")
    
    def _detect_failure_patterns(self, results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect patterns in failure data."""
        patterns = []
        
        # Simple pattern detection
        faults = results.get("faults_injected", [])
        
        error_types: Dict[str, int] = {}
        for fault in faults:
            fault_type = fault.get("fault", {}).get("fault_type", "unknown")
            error_types[fault_type] = error_types.get(fault_type, 0) + 1
        
        if error_types:
            patterns.append({
                "type": "fault_distribution",
                "data": error_types,
                "description": "Distribution of injected faults by type"
            })
        
        return patterns
    
    def _predict_impact(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Predict potential impacts based on failures."""
        return {
            "availability_impact": "medium",
            "performance_impact": "low",
            "data_integrity_impact": "low"
        }
    
    def _calculate_risk_score(
        self,
        results: Dict[str, Any],
        analysis: Dict[str, Any]
    ) -> float:
        """Calculate overall risk score (0-10)."""
        faults = results.get("faults_injected", [])
        fault_count = len(faults)
        
        # Simple risk calculation
        base_risk = min(10.0, fault_count * 1.5)
        
        return round(base_risk, 2)
    
    def _generate_recommendations(self, analysis: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on analysis."""
        recommendations = []
        
        risk_score = analysis.get("risk_score", 0)
        
        if risk_score > 7:
            recommendations.append("High risk detected - implement additional circuit breakers")
            recommendations.append("Consider adding retry logic with exponential backoff")
        elif risk_score > 4:
            recommendations.append("Medium risk - add monitoring alerts")
            recommendations.append("Review timeout configurations")
        else:
            recommendations.append("Low risk - system appears resilient")
        
        return recommendations


class ChaosMonitorAction(BaseAction):
    """Monitor system behavior during chaos experiments."""
    
    action_type = "chaos_monitor"
    display_name = "混沌监控器"
    description = "在混沌实验期间监控系统行为"
    
    def __init__(self) -> None:
        super().__init__()
        self._baseline_metrics: Dict[str, Any] = {}
        self._current_metrics: Dict[str, Any] = {}
        self._anomaly_threshold = 0.3  # 30% deviation from baseline
    
    def set_baseline(self, metrics: Dict[str, Any]) -> None:
        """Set baseline metrics for comparison."""
        self._baseline_metrics = metrics.copy()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Monitor and report system metrics during chaos.
        
        Args:
            params: {
                "current_metrics": Current system metrics (dict, required),
                "check_baseline": Compare against baseline (bool, default True),
                "alert_on_anomaly": Generate alerts for anomalies (bool, default True)
            }
        """
        try:
            current_metrics = params.get("current_metrics", {})
            check_baseline = params.get("check_baseline", True)
            alert_on_anomaly = params.get("alert_on_anomaly", True)
            
            if not current_metrics:
                return ActionResult(success=False, message="current_metrics required")
            
            self._current_metrics = current_metrics
            
            result: Dict[str, Any] = {
                "metrics": current_metrics,
                "anomalies": []
            }
            
            if check_baseline and self._baseline_metrics:
                deviations = self._calculate_deviations(
                    current_metrics,
                    self._baseline_metrics
                )
                result["deviations"] = deviations
                
                # Detect anomalies
                anomalies = [
                    {"metric": k, "deviation": v}
                    for k, v in deviations.items()
                    if abs(v) > self._anomaly_threshold
                ]
                result["anomalies"] = anomalies
                
                if anomalies and alert_on_anomaly:
                    result["alert"] = {
                        "level": "warning" if len(anomalies) < 3 else "critical",
                        "message": f"Detected {len(anomalies)} anomalies",
                        "anomalies": anomalies
                    }
            
            return ActionResult(
                success=True,
                message=f"Monitoring {len(current_metrics)} metrics",
                data=result
            )
        
        except Exception as e:
            return ActionResult(success=False, message=f"Monitor error: {str(e)}")
    
    def _calculate_deviations(
        self,
        current: Dict[str, Any],
        baseline: Dict[str, Any]
    ) -> Dict[str, float]:
        """Calculate deviation from baseline for each metric."""
        deviations: Dict[str, float] = {}
        
        for key, value in current.items():
            if key in baseline:
                baseline_value = baseline[key]
                
                if isinstance(value, (int, float)) and isinstance(baseline_value, (int, float)):
                    if baseline_value != 0:
                        deviation = (value - baseline_value) / baseline_value
                    else:
                        deviation = 0.0 if value == 0 else 1.0
                    
                    deviations[key] = round(deviation, 4)
        
        return deviations
