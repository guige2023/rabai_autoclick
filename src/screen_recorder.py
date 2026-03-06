"""
屏幕录制转自动化流程 v22
P0级功能 - 将屏幕录制转换为可执行的工作流
"""
import json
import time
import hashlib
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime


class ActionType(Enum):
    """动作类型"""
    CLICK = "click"
    DOUBLE_CLICK = "double_click"
    RIGHT_CLICK = "right_click"
    DRAG = "drag"
    TYPE = "type"
    HOTKEY = "hotkey"
    WAIT = "wait"
    SCROLL = "scroll"
    LAUNCH_APP = "launch_app"
    CLOSE_APP = "close_app"
    URL_OPEN = "url_open"
    IMAGE_CLICK = "image_click"
    REGION_CLICK = "region_click"


class ElementDetection(Enum):
    """元素检测方式"""
    IMAGE = "image"           # 图像识别
    TEXT = "text"             # 文字识别
    COORDINATE = "coordinate"  # 坐标
    RELATIVE = "relative"     # 相对位置


@dataclass
class RecordingAction:
    """录制动作"""
    action_id: str
    action_type: ActionType
    timestamp: float
    
    # 位置信息
    x: Optional[int] = None
    y: Optional[int] = None
    x2: Optional[int] = None    # 拖拽结束位置
    y2: Optional[int] = None
    
    # 区域信息
    region: Optional[Tuple[int, int, int, int]] = None  # x, y, width, height
    
    # 内容信息
    text: Optional[str] = None
    key: Optional[str] = None  # 热键
    app: Optional[str] = None
    
    # 识别信息
    detection: ElementDetection = ElementDetection.COORDINATE
    image_hash: Optional[str] = None  # 截图哈希
    ocr_text: Optional[str] = None    # OCR识别文字
    
    # 上下文
    app_before: Optional[str] = None
    app_after: Optional[str] = None
    screen_region: Optional[str] = None
    
    # 元数据
    duration: float = 0.0  # 动作耗时
    confidence: float = 1.0  # 识别置信度


@dataclass
class Recording:
    """录制"""
    recording_id: str
    name: str
    description: str = ""
    actions: List[RecordingAction] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    duration: float = 0.0  # 总时长
    resolution: Tuple[int, int] = (1920, 1080)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class WorkflowStep:
    """工作流步骤"""
    step_id: str
    order: int
    action: str
    target: str
    params: Dict[str, Any] = field(default_factory=dict)
    condition: Optional[str] = None  # 条件执行
    retry: int = 0
    timeout: int = 30


@dataclass
class ConversionResult:
    """转换结果"""
    success: bool
    workflow_id: str
    workflow_name: str
    steps: List[WorkflowStep]
    warnings: List[str] = field(default_factory=list)
    statistics: Dict[str, Any] = field(default_factory=dict)


class ScreenRecorderConverter:
    """屏幕录制转自动化流程"""
    
    def __init__(self, data_dir: str = "./data"):
        self.data_dir = data_dir
        self.recordings: Dict[str, Recording] = {}
        self._load_recordings()
        
    def _load_recordings(self) -> None:
        """加载录制"""
        try:
            with open(f"{self.data_dir}/recordings.json", "r", encoding="utf-8") as f:
                data = json.load(f)
                for rec_id, rec_data in data.items():
                    if "actions" in rec_data:
                        # 转换动作类型
                        for action_data in rec_data["actions"]:
                            if "action_type" in action_data:
                                action_data["action_type"] = ActionType(action_data["action_type"])
                            if "detection" in action_data:
                                action_data["detection"] = ElementDetection(action_data["detection"])
                        rec_data["actions"] = [RecordingAction(**a) for a in rec_data["actions"]]
                    else:
                        rec_data["actions"] = []
                    self.recordings[rec_id] = Recording(**rec_data)
        except FileNotFoundError:
            pass
        except (json.JSONDecodeError, KeyError, TypeError):
            # 数据文件损坏，忽略
            pass
    
    def _save_recordings(self) -> None:
        """保存录制"""
        data = {}
        for rec_id, rec in self.recordings.items():
            d = {
                "recording_id": rec.recording_id,
                "name": rec.name,
                "description": rec.description,
                "actions": [asdict(a) for a in rec.actions],
                "created_at": rec.created_at,
                "duration": rec.duration,
                "resolution": rec.resolution,
                "metadata": rec.metadata
            }
            # 转换枚举
            for action in d["actions"]:
                action["action_type"] = action["action_type"].value
                action["detection"] = action["detection"].value
            data[rec_id] = d
            
        with open(f"{self.data_dir}/recordings.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    def start_recording(self, name: str, description: str = "",
                       resolution: Tuple[int, int] = (1920, 1080)) -> Recording:
        """开始录制"""
        recording_id = f"rec_{int(time.time())}"
        
        recording = Recording(
            recording_id=recording_id,
            name=name,
            description=description,
            resolution=resolution,
            metadata={
                "start_time": time.time(),
                "app_changes": [],
                "screen_changes": []
            }
        )
        
        self.recordings[recording_id] = recording
        return recording
    
    def record_action(self, recording_id: str, action: RecordingAction) -> bool:
        """记录动作"""
        recording = self.recordings.get(recording_id)
        if not recording:
            return False
            
        recording.actions.append(action)
        return True
    
    def stop_recording(self, recording_id: str) -> Optional[Recording]:
        """停止录制"""
        recording = self.recordings.get(recording_id)
        if not recording:
            return None
            
        # 计算时长
        if recording.actions:
            recording.duration = recording.actions[-1].timestamp - recording.actions[0].timestamp
        
        # 保存
        self._save_recordings()
        return recording
    
    def import_from_video(self, video_path: str, 
                         analysis_type: str = "auto") -> Optional[Recording]:
        """从视频导入录制"""
        # 注意：这里需要实际的视频处理能力
        # 简化版本：创建空录制，实际转换需要外部工具
        
        recording_id = f"rec_video_{int(time.time())}"
        
        recording = Recording(
            recording_id=recording_id,
            name=f"导入录制 {video_path}",
            description=f"从视频导入: {video_path}",
            metadata={
                "source": "video",
                "video_path": video_path,
                "analysis_type": analysis_type,
                "imported_at": time.time()
            }
        )
        
        # 添加警告：需要视频处理工具
        recording.metadata["warnings"] = [
            "视频导入需要安装 OpenCV 和相关依赖",
            "当前创建空录制，请使用 add_action 添加动作"
        ]
        
        self.recordings[recording_id] = recording
        self._save_recordings()
        
        return recording
    
    def add_action(self, recording_id: str, action_data: Dict) -> bool:
        """手动添加动作"""
        recording = self.recordings.get(recording_id)
        if not recording:
            return False
        
        # 转换动作类型
        action_type = ActionType(action_data.get("action_type", "click"))
        
        action = RecordingAction(
            action_id=f"act_{len(recording.actions) + 1}",
            action_type=action_type,
            timestamp=action_data.get("timestamp", time.time()),
            x=action_data.get("x"),
            y=action_data.get("y"),
            text=action_data.get("text"),
            key=action_data.get("key"),
            app=action_data.get("app"),
            detection=ElementDetection(action_data.get("detection", "coordinate")),
            duration=action_data.get("duration", 0.0)
        )
        
        recording.actions.append(action)
        self._save_recordings()
        
        return True
    
    def convert_to_workflow(self, recording_id: str,
                           name: str = None,
                           detection_mode: ElementDetection = ElementDetection.IMAGE,
                           add_retry: bool = True,
                           add_screenshots: bool = True) -> Optional[ConversionResult]:
        """转换为工作流"""
        recording = self.recordings.get(recording_id)
        if not recording or not recording.actions:
            return None
            
        workflow_id = f"wf_{int(time.time())}"
        workflow_name = name or f"{recording.name} (自动化)"
        
        steps = []
        warnings = []
        
        # 统计分析
        stats = {
            "total_actions": len(recording.actions),
            "action_types": {},
            "detection_modes": {},
            "avg_interval": 0.0,
            "needs_review": []
        }
        
        # 分析动作间隔
        if len(recording.actions) > 1:
            intervals = []
            for i in range(1, len(recording.actions)):
                interval = recording.actions[i].timestamp - recording.actions[i-1].timestamp
                intervals.append(interval)
            stats["avg_interval"] = sum(intervals) / len(intervals) if intervals else 0
        
        # 转换每个动作
        for i, action in enumerate(recording.actions):
            # 统计动作类型
            action_type_val = action.action_type.value
            stats["action_types"][action_type_val] = stats["action_types"].get(action_type_val, 0) + 1
            
            # 构建步骤
            step = self._action_to_step(action, i, detection_mode)
            
            if add_retry:
                step.retry = 2
                
            steps.append(step)
            
            # 检查需要人工审核的点
            if action.confidence < 0.8:
                warnings.append(f"动作 {i+1} 识别置信度较低 ({action.confidence:.0%})")
                stats["needs_review"].append(i)
                
            if action.action_type in [ActionType.TYPE, ActionType.HOTKEY]:
                warnings.append(f"动作 {i+1} 包含敏感操作 (输入/热键)")
        
        # 添加等待动作优化
        steps = self._optimize_timing(steps, recording.actions)
        
        # 添加错误处理
        if add_screenshots:
            steps = self._add_error_handling(steps)
        
        stats["conversion_mode"] = detection_mode.value
        stats["warnings_count"] = len(warnings)
        
        return ConversionResult(
            success=True,
            workflow_id=workflow_id,
            workflow_name=workflow_name,
            steps=steps,
            warnings=warnings,
            statistics=stats
        )
    
    def _action_to_step(self, action: RecordingAction, index: int,
                        detection_mode: ElementDetection) -> WorkflowStep:
        """将动作转换为工作流步骤"""
        step_id = f"step_{index + 1}"
        
        # 确定目标和参数
        target = ""
        params = {}
        
        if action.action_type == ActionType.CLICK:
            target = f"click_{action.x}_{action.y}"
            params = {
                "x": action.x,
                "y": action.y,
                "detection": detection_mode.value
            }
            if action.image_hash:
                params["image_hash"] = action.image_hash
            if action.ocr_text:
                params["target_text"] = action.ocr_text
                
        elif action.action_type == ActionType.DOUBLE_CLICK:
            target = f"double_click_{action.x}_{action.y}"
            params = {"x": action.x, "y": action.y}
            
        elif action.action_type == ActionType.RIGHT_CLICK:
            target = f"right_click_{action.x}_{action.y}"
            params = {"x": action.x, "y": action.y}
            
        elif action.action_type == ActionType.DRAG:
            target = f"drag_{action.x}_{action.y}_to_{action.x2}_{action.y2}"
            params = {
                "x1": action.x, "y1": action.y,
                "x2": action.x2, "y2": action.y2
            }
            
        elif action.action_type == ActionType.TYPE:
            target = "input_text"
            # 注意：不要在参数中存储敏感文本
            params = {"text_length": len(action.text) if action.text else 0}
            if action.text:
                # 用占位符标记，实际执行时需要用户输入
                params["text_placeholder"] = "[USER_INPUT]"
                
        elif action.action_type == ActionType.HOTKEY:
            target = "hotkey"
            params = {"keys": action.key}
            
        elif action.action_type == ActionType.WAIT:
            target = "wait"
            params = {"seconds": action.duration}
            
        elif action.action_type == ActionType.SCROLL:
            target = "scroll"
            params = {"direction": "down" if action.y2 and action.y2 > action.y else "up"}
            
        elif action.action_type == ActionType.LAUNCH_APP:
            target = "launch_app"
            params = {"app": action.app}
            
        elif action.action_type == ActionType.CLOSE_APP:
            target = "close_app"
            params = {"app": action.app}
            
        elif action.action_type == ActionType.URL_OPEN:
            target = "open_url"
            params = {"url": action.text}
            
        elif action.action_type == ActionType.IMAGE_CLICK:
            target = "image_click"
            params = {"template": action.image_hash}
            
        else:
            target = "unknown"
            params = {"raw": asdict(action)}
        
        return WorkflowStep(
            step_id=step_id,
            order=index,
            action=action.action_type.value,
            target=target,
            params=params
        )
    
    def _optimize_timing(self, steps: List[WorkflowStep], 
                        actions: List[RecordingAction]) -> List[WorkflowStep]:
        """优化时间间隔"""
        if len(steps) != len(actions):
            return steps
            
        optimized = []
        
        for i, (step, action) in enumerate(zip(steps, actions)):
            # 添加等待步骤如果间隔太长
            if i > 0:
                prev_action = actions[i - 1]
                interval = action.timestamp - prev_action.timestamp
                
                if interval > 3:  # 超过3秒
                    # 插入等待步骤
                    wait_step = WorkflowStep(
                        step_id=f"wait_{i}",
                        order=i - 0.5,
                        action="wait",
                        target="delay",
                        params={"seconds": min(interval, 10)}  # 最多等10秒
                    )
                    optimized.append(wait_step)
            
            step.order = len(optimized)
            optimized.append(step)
        
        # 重新排序
        optimized.sort(key=lambda s: s.order)
        
        # 重新编号
        for i, step in enumerate(optimized):
            step.step_id = f"step_{i + 1}"
            step.order = i
            
        return optimized
    
    def _add_error_handling(self, steps: List[WorkflowStep]) -> List[WorkflowStep]:
        """添加错误处理"""
        enhanced = []
        
        for step in steps:
            # 为关键步骤添加截图
            if step.action in ["click", "image_click", "type"]:
                step.params["screenshot_on_error"] = True
                step.params["error_screenshot_path"] = f"error_{step.step_id}.png"
            
            enhanced.append(step)
        
        return enhanced
    
    def export_workflow_json(self, result: ConversionResult) -> str:
        """导出为JSON"""
        data = {
            "version": "22.0.0",
            "workflow_id": result.workflow_id,
            "name": result.workflow_name,
            "description": f"从录制 {result.workflow_id} 转换",
            "steps": [
                {
                    "step_id": s.step_id,
                    "order": s.order,
                    "action": s.action,
                    "target": s.target,
                    "params": s.params,
                    "condition": s.condition,
                    "retry": s.retry,
                    "timeout": s.timeout
                }
                for s in result.steps
            ],
            "settings": {
                "execution_mode": "sequential",
                "continue_on_error": False,
                "screenshot_on_error": True
            },
            "statistics": result.statistics,
            "warnings": result.warnings,
            "generated_at": time.time()
        }
        
        return json.dumps(data, ensure_ascii=False, indent=2)
    
    def list_recordings(self) -> List[Recording]:
        """列出所有录制"""
        return list(self.recordings.values())
    
    def get_recording(self, recording_id: str) -> Optional[Recording]:
        """获取录制"""
        return self.recordings.get(recording_id)
    
    def delete_recording(self, recording_id: str) -> bool:
        """删除录制"""
        if recording_id in self.recordings:
            del self.recordings[recording_id]
            self._save_recordings()
            return True
        return False
    
    def analyze_recording(self, recording_id: str) -> Dict[str, Any]:
        """分析录制"""
        recording = self.recordings.get(recording_id)
        if not recording:
            return {}
        
        # 基本统计
        action_types = {}
        detection_modes = {}
        apps = []
        
        for action in recording.actions:
            # 动作类型统计
            at = action.action_type.value
            action_types[at] = action_types.get(at, 0) + 1
            
            # 检测模式统计
            dm = action.detection.value
            detection_modes[dm] = detection_modes.get(dm, 0) + 1
            
            # 应用使用
            if action.app:
                apps.append(action.app)
        
        return {
            "recording_id": recording_id,
            "name": recording.name,
            "duration": recording.duration,
            "action_count": len(recording.actions),
            "action_types": action_types,
            "detection_modes": detection_modes,
            "apps_used": list(set(apps)),
            "resolution": recording.resolution,
            "created_at": datetime.fromtimestamp(recording.created_at).strftime("%Y-%m-%d %H:%M:%S")
        }


def create_screen_recorder(data_dir: str = "./data") -> ScreenRecorderConverter:
    """创建屏幕录制转换器"""
    return ScreenRecorderConverter(data_dir)


# 测试
if __name__ == "__main__":
    converter = create_screen_recorder("./data")
    
    # 开始录制
    rec = converter.start_recording("测试录制", "用于测试转换功能")
    print(f"开始录制: {rec.recording_id}")
    
    # 添加一些动作
    converter.add_action(rec.recording_id, {
        "action_type": "launch_app",
        "timestamp": time.time(),
        "app": "Chrome"
    })
    
    converter.add_action(rec.recording_id, {
        "action_type": "click",
        "timestamp": time.time() + 1,
        "x": 100, "y": 200,
        "detection": "coordinate"
    })
    
    converter.add_action(rec.recording_id, {
        "action_type": "type",
        "timestamp": time.time() + 2,
        "text": "hello world"
    })
    
    converter.add_action(rec.recording_id, {
        "action_type": "hotkey",
        "timestamp": time.time() + 3,
        "key": "ctrl+s"
    })
    
    # 停止录制
    rec = converter.stop_recording(rec.recording_id)
    print(f"录制完成: {len(rec.actions)} 个动作")
    
    # 转换为工作流
    result = converter.convert_to_workflow(
        rec.recording_id,
        detection_mode=ElementDetection.IMAGE
    )
    
    if result:
        print(f"\n转换成功: {result.workflow_name}")
        print(f"步骤数: {len(result.steps)}")
        print(f"警告: {len(result.warnings)}")
        
        # 输出JSON
        json_output = converter.export_workflow_json(result)
        print(f"\n工作流JSON:\n{json_output[:500]}...")


from dataclasses import asdict
