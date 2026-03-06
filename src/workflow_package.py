"""
场景化工作流包 v21
用户体验优化 - 将常用工作流打包为场景，一键切换
"""
import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, field
from enum import Enum


class SceneStatus(Enum):
    """场景状态"""
    ACTIVE = "active"         # 启用中
    INACTIVE = "inactive"     # 未启用
    SCHEDULED = "scheduled"  # 预定启用


class TriggerType(Enum):
    """触发类型"""
    MANUAL = "manual"         # 手动触发
    TIME = "time"             # 时间触发
    LOCATION = "location"     # 位置触发
    APP_LAUNCH = "app_launch" # 应用启动触发
    EVENT = "event"           # 事件触发


@dataclass
class WorkflowRef:
    """工作流引用"""
    workflow_id: str
    workflow_name: str
    enabled: bool = True
    delay: float = 0.0  # 延迟秒数
    order: int = 0      # 执行顺序


@dataclass
class Schedule:
    """定时计划"""
    enabled: bool = False
    time: str = ""      # HH:MM 格式
    days: List[str] = field(default_factory=list)  # ["monday", "tuesday", ...]
    timezone: str = "Asia/Shanghai"


@dataclass
class WorkflowScene:
    """工作流场景"""
    scene_id: str
    name: str
    description: str
    icon: str = "📦"
    status: SceneStatus = SceneStatus.INACTIVE
    workflows: List[WorkflowRef] = field(default_factory=list)
    schedule: Schedule = field(default_factory=Schedule)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    usage_count: int = 0
    tags: List[str] = field(default_factory=list)
    settings: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "scene_id": self.scene_id,
            "name": self.name,
            "description": self.description,
            "icon": self.icon,
            "status": self.status.value,
            "workflows": [
                {
                    "workflow_id": w.workflow_id,
                    "workflow_name": w.workflow_name,
                    "enabled": w.enabled,
                    "delay": w.delay,
                    "order": w.order
                }
                for w in self.workflows
            ],
            "schedule": {
                "enabled": self.schedule.enabled,
                "time": self.schedule.time,
                "days": self.schedule.days,
                "timezone": self.schedule.timezone
            },
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "usage_count": self.usage_count,
            "tags": self.tags,
            "settings": self.settings
        }


class WorkflowSceneManager:
    """场景化工作流包管理器"""
    
    def __init__(self, data_dir: str = "./data"):
        self.data_dir = data_dir
        self.scenes: Dict[str, WorkflowScene] = {}
        self.active_scene_id: Optional[str] = None
        self._load_scenes()
        
    def _load_scenes(self) -> None:
        """加载场景数据"""
        try:
            with open(f"{self.data_dir}/workflow_scenes.json", "r", encoding="utf-8") as f:
                data = json.load(f)
                for item in data.get("scenes", []):
                    scene = WorkflowScene(
                        scene_id=item["scene_id"],
                        name=item["name"],
                        description=item["description"],
                        icon=item.get("icon", "📦"),
                        status=SceneStatus(item.get("status", "inactive")),
                        workflows=[
                            WorkflowRef(
                                workflow_id=w["workflow_id"],
                                workflow_name=w["workflow_name"],
                                enabled=w.get("enabled", True),
                                delay=w.get("delay", 0.0),
                                order=w.get("order", 0)
                            )
                            for w in item.get("workflows", [])
                        ],
                        schedule=Schedule(**item.get("schedule", {})),
                        created_at=item.get("created_at", time.time()),
                        updated_at=item.get("updated_at", time.time()),
                        usage_count=item.get("usage_count", 0),
                        tags=item.get("tags", []),
                        settings=item.get("settings", {})
                    )
                    self.scenes[scene.scene_id] = scene
                self.active_scene_id = data.get("active_scene_id")
        except FileNotFoundError:
            self._init_default_scenes()
    
    def _save_scenes(self) -> None:
        """保存场景数据"""
        data = {
            "scenes": [s.to_dict() for s in self.scenes.values()],
            "active_scene_id": self.active_scene_id
        }
        with open(f"{self.data_dir}/workflow_scenes.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    def _init_default_scenes(self) -> None:
        """初始化默认场景"""
        # 晨间场景
        morning = WorkflowScene(
            scene_id="morning_routine",
            name="晨间Routine",
            description="早上起床后的一系列自动化任务",
            icon="🌅",
            workflows=[
                WorkflowRef(workflow_id="wf_001", workflow_name="开灯", order=1),
                WorkflowRef(workflow_id="wf_002", workflow_name="播放音乐", delay=5, order=2),
                WorkflowRef(workflow_id="wf_003", workflow_name="煮咖啡", delay=10, order=3),
                WorkflowRef(workflow_id="wf_004", workflow_name="播报天气", delay=30, order=4),
                WorkflowRef(workflow_id="wf_005", workflow_name="打开新闻", delay=60, order=5)
            ],
            schedule=Schedule(
                enabled=True,
                time="07:00",
                days=["monday", "tuesday", "wednesday", "thursday", "friday"]
            ),
            tags=["生活", "早晨", "自动化"]
        )
        
        # 工作场景
        work = WorkflowScene(
            scene_id="work_mode",
            name="工作模式",
            description="开启专注工作模式",
            icon="💼",
            workflows=[
                WorkflowRef(workflow_id="wf_010", workflow_name="打开工作应用", order=1),
                WorkflowRef(workflow_id="wf_011", workflow_name="开启勿扰", order=2),
                WorkflowRef(workflow_id="wf_012", workflow_name="整理桌面", order=3)
            ],
            tags=["工作", "专注"]
        )
        
        # 下班场景
        shutdown = WorkflowScene(
            scene_id="shutdown_routine",
            name="下班Shutdown",
            description="下班前自动清理工作环境",
            icon="🏠",
            workflows=[
                WorkflowRef(workflow_id="wf_020", workflow_name="保存所有文件", order=1),
                WorkflowRef(workflow_id="wf_021", workflow_name="关闭不必要的应用", order=2),
                WorkflowRef(workflow_id="wf_022", workflow_name="清理桌面", order=3),
                WorkflowRef(workflow_id="wf_023", workflow_name="锁屏", delay=5, order=4)
            ],
            schedule=Schedule(
                enabled=True,
                time="18:30",
                days=["monday", "tuesday", "wednesday", "thursday", "friday"]
            ),
            tags=["工作", "下班", "清理"]
        )
        
        # 出差场景
        travel = WorkflowScene(
            scene_id="travel_ready",
            name="出差Ready",
            description="出差前的一系列准备任务",
            icon="✈️",
            workflows=[
                WorkflowRef(workflow_id="wf_030", workflow_name="查看天气", order=1),
                WorkflowRef(workflow_id="wf_031", workflow_name="打包清单", order=2),
                WorkflowRef(workflow_id="wf_032", workflow_name="叫车", delay=60, order=3),
                WorkflowRef(workflow_id="wf_033", workflow_name="发送行程", delay=120, order=4)
            ],
            tags=["出差", "旅行", "准备"]
        )
        
        # 会议场景
        meeting = WorkflowScene(
            scene_id="meeting_mode",
            name="会议模式",
            description="开会时自动调整环境",
            icon="📅",
            workflows=[
                WorkflowRef(workflow_id="wf_040", workflow_name="开启静音", order=1),
                WorkflowRef(workflow_id="wf_041", workflow_name="关闭通知", order=2),
                WorkflowRef(workflow_id="wf_042", workflow_name="打开会议软件", delay=5, order=3)
            ],
            tags=["会议", "专注"]
        )
        
        # 休息场景
        rest = WorkflowScene(
            scene_id="rest_time",
            name="休息时间",
            description="放松休息时的自动化",
            icon="🛋️",
            workflows=[
                WorkflowRef(workflow_id="wf_050", workflow_name="调暗灯光", order=1),
                WorkflowRef(workflow_id="wf_051", workflow_name="播放轻音乐", order=2),
                WorkflowRef(workflow_id="wf_052", workflow_name="关闭工作应用", order=3)
            ],
            tags=["休息", "放松"]
        )
        
        self.scenes = {
            morning.scene_id: morning,
            work.scene_id: work,
            shutdown.scene_id: shutdown,
            travel.scene_id: travel,
            meeting.scene_id: meeting,
            rest.scene_id: rest
        }
        self._save_scenes()
    
    def create_scene(self, name: str, description: str = "",
                    icon: str = "📦", tags: List[str] = None) -> WorkflowScene:
        """创建新场景"""
        scene_id = f"scene_{int(time.time())}"
        scene = WorkflowScene(
            scene_id=scene_id,
            name=name,
            description=description,
            icon=icon,
            tags=tags or []
        )
        self.scenes[scene_id] = scene
        self._save_scenes()
        return scene
    
    def update_scene(self, scene_id: str, **kwargs) -> Optional[WorkflowScene]:
        """更新场景"""
        scene = self.scenes.get(scene_id)
        if not scene:
            return None
            
        for key, value in kwargs.items():
            if hasattr(scene, key):
                setattr(scene, key, value)
        
        scene.updated_at = time.time()
        self._save_scenes()
        return scene
    
    def delete_scene(self, scene_id: str) -> bool:
        """删除场景"""
        if scene_id in self.scenes:
            # 如果删除的是当前激活的场景
            if self.active_scene_id == scene_id:
                self.active_scene_id = None
            del self.scenes[scene_id]
            self._save_scenes()
            return True
        return False
    
    def add_workflow_to_scene(self, scene_id: str, workflow_id: str,
                             workflow_name: str, order: int = 0,
                             delay: float = 0.0) -> bool:
        """添加工作流到场景"""
        scene = self.scenes.get(scene_id)
        if not scene:
            return False
            
        wf = WorkflowRef(
            workflow_id=workflow_id,
            workflow_name=workflow_name,
            order=order,
            delay=delay
        )
        scene.workflows.append(wf)
        scene.updated_at = time.time()
        self._save_scenes()
        return True
    
    def remove_workflow_from_scene(self, scene_id: str, workflow_id: str) -> bool:
        """从场景移除工作流"""
        scene = self.scenes.get(scene_id)
        if not scene:
            return False
            
        scene.workflows = [w for w in scene.workflows if w.workflow_id != workflow_id]
        scene.updated_at = time.time()
        self._save_scenes()
        return True
    
    def activate_scene(self, scene_id: str) -> bool:
        """激活场景"""
        scene = self.scenes.get(scene_id)
        if not scene:
            return False
        
        # 先停用当前场景
        if self.active_scene_id and self.active_scene_id in self.scenes:
            self.scenes[self.active_scene_id].status = SceneStatus.INACTIVE
        
        # 激活新场景
        scene.status = SceneStatus.ACTIVE
        scene.usage_count += 1
        scene.updated_at = time.time()
        self.active_scene_id = scene_id
        self._save_scenes()
        
        return True
    
    def deactivate_scene(self, scene_id: str) -> bool:
        """停用场景"""
        scene = self.scenes.get(scene_id)
        if not scene:
            return False
        
        scene.status = SceneStatus.INACTIVE
        if self.active_scene_id == scene_id:
            self.active_scene_id = None
        scene.updated_at = time.time()
        self._save_scenes()
        return True
    
    def get_active_scene(self) -> Optional[WorkflowScene]:
        """获取当前激活的场景"""
        if self.active_scene_id:
            return self.scenes.get(self.active_scene_id)
        return None
    
    def get_scene(self, scene_id: str) -> Optional[WorkflowScene]:
        """获取场景"""
        return self.scenes.get(scene_id)
    
    def list_scenes(self, status: SceneStatus = None,
                   tags: List[str] = None) -> List[WorkflowScene]:
        """列出场景"""
        scenes = list(self.scenes.values())
        
        if status:
            scenes = [s for s in scenes if s.status == status]
        
        if tags:
            scenes = [s for s in scenes if any(t in s.tags for t in tags)]
        
        return sorted(scenes, key=lambda s: s.usage_count, reverse=True)
    
    def execute_scene(self, scene_id: str, 
                     execute_callback: callable = None) -> Dict[str, Any]:
        """执行场景"""
        scene = self.scenes.get(scene_id)
        if not scene:
            return {"success": False, "error": "场景不存在"}
        
        # 按顺序执行工作流
        enabled_workflows = [w for w in scene.workflows if w.enabled]
        sorted_workflows = sorted(enabled_workflows, key=lambda w: w.order)
        
        results = []
        for wf in sorted_workflows:
            result = {
                "workflow_id": wf.workflow_id,
                "workflow_name": wf.workflow_name,
                "status": "pending"
            }
            
            try:
                # 如果有回调函数则执行
                if execute_callback:
                    execute_callback(wf.workflow_id)
                result["status"] = "success"
            except Exception as e:
                result["status"] = "failed"
                result["error"] = str(e)
            
            results.append(result)
        
        scene.usage_count += 1
        self._save_scenes()
        
        return {
            "success": True,
            "scene_id": scene_id,
            "scene_name": scene.name,
            "workflows_executed": len(results),
            "results": results
        }
    
    def get_scene_execution_order(self, scene_id: str) -> List[str]:
        """获取场景执行顺序"""
        scene = self.scenes.get(scene_id)
        if not scene:
            return []
        
        sorted_workflows = sorted(scene.workflows, key=lambda w: w.order)
        return [wf.workflow_name for wf in sorted_workflows]
    
    def enable_schedule(self, scene_id: str, schedule_time: str, 
                       days: List[str]) -> bool:
        """启用定时"""
        scene = self.scenes.get(scene_id)
        if not scene:
            return False
        
        scene.schedule.enabled = True
        scene.schedule.time = schedule_time
        scene.schedule.days = days
        import time as time_module
        scene.updated_at = time_module.time()
        self._save_scenes()
        return True
    
    def disable_schedule(self, scene_id: str) -> bool:
        """禁用定时"""
        scene = self.scenes.get(scene_id)
        if not scene:
            return False
        
        scene.schedule.enabled = False
        scene.updated_at = time.time()
        self._save_scenes()
        return True
    
    def check_scheduled_scenes(self) -> List[WorkflowScene]:
        """检查需要执行的定时场景"""
        now = datetime.now()
        current_time = now.strftime("%H:%M")
        current_day = now.strftime("%A").lower()
        
        scheduled = []
        for scene in self.scenes.values():
            if scene.schedule.enabled:
                if scene.schedule.time == current_time:
                    if current_day in scene.schedule.days or not scene.schedule.days:
                        scheduled.append(scene)
        
        return scheduled
    
    def export_scene(self, scene_id: str) -> str:
        """导出场景配置"""
        scene = self.scenes.get(scene_id)
        if not scene:
            return ""
        return json.dumps(scene.to_dict(), ensure_ascii=False, indent=2)
    
    def import_scene(self, scene_json: str) -> bool:
        """导入场景配置"""
        try:
            data = json.loads(scene_json)
            # 重新生成 ID 避免冲突
            data["scene_id"] = f"scene_{int(time.time())}"
            data["created_at"] = time.time()
            data["updated_at"] = time.time()
            data["usage_count"] = 0
            
            scene = WorkflowScene(
                scene_id=data["scene_id"],
                name=data["name"],
                description=data.get("description", ""),
                icon=data.get("icon", "📦"),
                status=SceneStatus(data.get("status", "inactive")),
                workflows=[
                    WorkflowRef(**w) for w in data.get("workflows", [])
                ],
                schedule=Schedule(**data.get("schedule", {})),
                tags=data.get("tags", []),
                settings=data.get("settings", {})
            )
            
            self.scenes[scene.scene_id] = scene
            self._save_scenes()
            return True
        except Exception:
            return False
    
    def get_scene_statistics(self) -> Dict[str, Any]:
        """获取场景统计"""
        total_scenes = len(self.scenes)
        active_scenes = len([s for s in self.scenes.values() 
                           if s.status == SceneStatus.ACTIVE])
        scheduled_scenes = len([s for s in self.scenes.values() 
                               if s.schedule.enabled])
        
        # 使用最多的场景
        top_scenes = sorted(self.scenes.values(), 
                          key=lambda s: s.usage_count, 
                          reverse=True)[:5]
        
        return {
            "total_scenes": total_scenes,
            "active_scenes": active_scenes,
            "scheduled_scenes": scheduled_scenes,
            "top_scenes": [
                {"name": s.name, "usage_count": s.usage_count}
                for s in top_scenes
            ]
        }


def create_scene_manager(data_dir: str = "./data") -> WorkflowSceneManager:
    """创建场景管理器实例"""
    return WorkflowSceneManager(data_dir)


# 测试
if __name__ == "__main__":
    manager = create_scene_manager("./data")
    
    # 列出所有场景
    scenes = manager.list_scenes()
    print("=== 场景列表 ===")
    for s in scenes:
        print(f"{s.icon} {s.name} - {s.description}")
        print(f"   状态: {s.status.value}, 使用次数: {s.usage_count}")
        print(f"   工作流数: {len(s.workflows)}")
        if s.schedule.enabled:
            print(f"   定时: {s.schedule.time} {s.schedule.days}")
        print()
    
    # 统计
    stats = manager.get_scene_statistics()
    print("=== 场景统计 ===")
    print(f"总场景数: {stats['total_scenes']}")
    print(f"激活中: {stats['active_scenes']}")
    print(f"定时任务: {stats['scheduled_scenes']}")
