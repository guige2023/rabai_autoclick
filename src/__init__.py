"""
RabAI AutoClick v22
桌面端+CLI 自动化工具
差异化功能+用户体验优化版本
新增 v22 功能:
- 无代码工作流分享链接
- CLI 管道集成模式
- 屏幕录制转自动化流程
- 增强版智能工作流健康诊断
"""
from .predictive_engine import PredictiveAutomationEngine, create_predictive_engine
from .self_healing_system import SelfHealingSystem, create_self_healing_system
from .workflow_package import WorkflowSceneManager, create_scene_manager
from .workflow_diagnostics import WorkflowDiagnosticsV2, create_diagnostics
from .workflow_share import WorkflowShareSystem, create_share_system, ShareType
from .pipeline_mode import PipelineRunner, create_pipeline_runner, PipeCLI, PipeMode
from .screen_recorder import ScreenRecorderConverter, create_screen_recorder
from .workflow_analytics import WorkflowAnalytics, create_workflow_analytics
from .workflow_security import (
    WorkflowSecurityModule, create_security_module,
    SecurityLevel, Permission, SecurityPolicy,
    AuditEvent, AuditEventType, WorkflowSignature, SecureVariable,
    EncryptionManager, AuditLogger, RateLimiter, IPAllowlistChecker,
    ContentFilter, IntrusionDetectionSystem, SandboxExecutor,
    WorkflowSignatureManager, SecureVariableStore, SecurityUtils,
    IntrusionPattern
)
from .workflow_hotkeys import WorkflowHotkeyManager, create_hotkey_manager, HotkeyActionType, HotkeyBinding, HotkeyProfile, HotkeyConflict, ConflictType
from .workflow_macro import (
    WorkflowMacro, create_macro_system,
    MacroAction, MacroActionType, MacroCondition, MacroConditionType,
    MacroVariable, Macro, MacroRecorder, MacroPlayer, MacroEditor,
    MacroLibrary, MacroScheduler, MacroToWorkflowConverter,
    MacroPosition, MouseButton, PlaybackResult
)

__version__ = "22.0.0"
__author__ = "RabAI Team"

__all__ = [
    # 核心模块
    "PredictiveAutomationEngine",
    "create_predictive_engine",
    "SelfHealingSystem", 
    "create_self_healing_system",
    "WorkflowSceneManager",
    "create_scene_manager",
    "WorkflowDiagnosticsV2",
    "create_diagnostics",
    
    # v22 新增模块
    "WorkflowShareSystem",
    "create_share_system",
    "ShareType",
    "PipelineRunner",
    "create_pipeline_runner",
    "PipeCLI",
    "PipeMode",
    "ScreenRecorderConverter",
    "create_screen_recorder",
    "WorkflowAnalytics",
    "create_workflow_analytics",

    # Security module
    "WorkflowSecurityModule",
    "create_security_module",
    "SecurityLevel",
    "Permission",
    "SecurityPolicy",
    "AuditEvent",
    "AuditEventType",
    "WorkflowSignature",
    "SecureVariable",
    "EncryptionManager",
    "AuditLogger",
    "RateLimiter",
    "IPAllowlistChecker",
    "ContentFilter",
    "IntrusionDetectionSystem",
    "SandboxExecutor",
    "WorkflowSignatureManager",
    "SecureVariableStore",
    "SecurityUtils",
    "IntrusionPattern",

    # Hotkey Manager
    "WorkflowHotkeyManager",
    "create_hotkey_manager",
    "HotkeyActionType",
    "HotkeyBinding",
    "HotkeyProfile",
    "HotkeyConflict",
    "ConflictType",
]
