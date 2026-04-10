"""
无代码工作流分享系统 v23
P0级功能 - 生成可分享的工作流链接，支持导入导出，云同步，版本控制，模板市场
"""
import json
import hashlib
import base64
import time
import re
import difflib
import uuid
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum
from urllib.parse import urlparse, parse_qs
from collections import defaultdict
import io


class ShareType(Enum):
    """分享类型"""
    PUBLIC = "public"           # 公开分享
    PRIVATE = "private"         # 私密分享
    TEAM = "team"              # 团队分享
    MARKETPLACE = "marketplace" # 市场分享


class ImportResult(Enum):
    """导入结果"""
    SUCCESS = "success"
    INVALID_FORMAT = "invalid_format"
    VERSION_MISMATCH = "version_mismatch"
    VALIDATION_FAILED = "validation_failed"
    NETWORK_ERROR = "network_error"
    URL_INVALID = "url_invalid"


@dataclass
class WorkflowShareLink:
    """工作流分享链接"""
    link_id: str
    workflow_id: str
    workflow_name: str
    workflow_data: Dict[str, Any]
    share_type: ShareType
    created_at: float
    expires_at: Optional[float] = None
    view_count: int = 0
    import_count: int = 0
    version: str = "23.0.0"
    metadata: Dict[str, Any] = field(default_factory=dict)
    clone_count: int = 0
    team_id: Optional[str] = None


@dataclass
class WorkflowExportData:
    """工作流导出数据"""
    version: str
    workflow_id: str
    name: str
    description: str
    steps: List[Dict]
    triggers: List[Dict]
    settings: Dict[str, Any]
    exported_at: float
    checksum: str


@dataclass
class ImportReport:
    """导入报告"""
    result: ImportResult
    workflow_id: str
    workflow_name: str
    message: str
    warnings: List[str] = field(default_factory=list)


# ========== New Data Classes for Enhanced Features ==========

@dataclass
class WorkflowVersion:
    """工作流版本信息"""
    version_id: str
    workflow_id: str
    version_number: str  # e.g., "1.0.0", "1.1.0"
    commit_message: str
    changes: Dict[str, Any]
    created_at: float
    author: str = "anonymous"
    parent_version: Optional[str] = None


@dataclass
class WorkflowDiff:
    """工作流差异"""
    workflow_id: str
    from_version: str
    to_version: str
    added_steps: List[Dict]
    removed_steps: List[Dict]
    modified_steps: List[Dict]
    step_changes: Dict[int, Tuple[Dict, Dict]]  # step_index -> (old, new)
    summary: str


@dataclass
class WorkflowTemplate:
    """工作流模板"""
    template_id: str
    name: str
    description: str
    category: str
    tags: List[str]
    template_data: Dict[str, Any]
    author: str
    created_at: float
    usage_count: int = 0
    rating: float = 0.0
    is_official: bool = False


@dataclass
class WorkflowComment:
    """工作流步骤评论"""
    comment_id: str
    workflow_id: str
    step_index: int
    content: str
    author: str
    created_at: float
    parent_comment_id: Optional[str] = None


@dataclass
class TeamMember:
    """团队成员"""
    user_id: str
    username: str
    role: str  # "owner", "editor", "viewer"
    joined_at: float


@dataclass
class Team:
    """团队"""
    team_id: str
    name: str
    members: List[TeamMember]
    shared_workflows: List[str]
    created_at: float


@dataclass
class WorkflowAnalytics:
    """工作流分析数据"""
    workflow_id: str
    execution_count: int = 0
    share_count: int = 0
    view_count: int = 0
    clone_count: int = 0
    import_count: int = 0
    last_executed_at: Optional[float] = None
    daily_stats: Dict[str, Dict[str, int]] = field(default_factory=dict)  # date -> {executions, views}


@dataclass
class MarketplaceEntry:
    """市场条目"""
    entry_id: str
    workflow_id: str
    template: WorkflowTemplate
    author: str
    submitted_at: float
    status: str = "pending"  # pending, approved, rejected
    review_notes: str = ""


# ========== Cloud Storage Interface ==========

class CloudStorageInterface:
    """云存储接口基类 (S3-compatible)"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.endpoint = config.get("endpoint", "")
        self.access_key = config.get("access_key", "")
        self.secret_key = config.get("secret_key", "")
        self.bucket = config.get("bucket", "workflows")
        self.region = config.get("region", "us-east-1")
    
    def upload(self, key: str, data: bytes) -> bool:
        """上传数据"""
        raise NotImplementedError
    
    def download(self, key: str) -> Optional[bytes]:
        """下载数据"""
        raise NotImplementedError
    
    def delete(self, key: str) -> bool:
        """删除数据"""
        raise NotImplementedError
    
    def list(self, prefix: str = "") -> List[str]:
        """列出对象"""
        raise NotImplementedError
    
    def exists(self, key: str) -> bool:
        """检查对象是否存在"""
        raise NotImplementedError


class MockCloudStorage(CloudStorageInterface):
    """模拟云存储 (用于测试)"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self._storage: Dict[str, bytes] = {}
    
    def upload(self, key: str, data: bytes) -> bool:
        self._storage[key] = data
        return True
    
    def download(self, key: str) -> Optional[bytes]:
        return self._storage.get(key)
    
    def delete(self, key: str) -> bool:
        if key in self._storage:
            del self._storage[key]
            return True
        return False
    
    def list(self, prefix: str = "") -> List[str]:
        return [k for k in self._storage.keys() if k.startswith(prefix)]
    
    def exists(self, key: str) -> bool:
        return key in self._storage


# ========== Workflow Version Control System ==========

class WorkflowVersionControl:
    """工作流版本控制系统 (Git-like)"""
    
    def __init__(self, parent: 'WorkflowShareSystem'):
        self.parent = parent
        self.versions: Dict[str, List[WorkflowVersion]] = defaultdict(list)
    
    def commit(self, workflow_id: str, message: str, author: str = "anonymous") -> Optional[WorkflowVersion]:
        """创建新版本 (commit)"""
        workflow_data = self.parent.workflow_registry.get(workflow_id)
        if not workflow_data:
            return None
        
        # 生成版本号
        existing = self.versions.get(workflow_id, [])
        if existing:
            last_version = existing[-1].version_number
            new_version = self._increment_version(last_version)
            parent_ver = last_version
        else:
            new_version = "1.0.0"
            parent_ver = None
        
        version_id = f"{workflow_id}_v{uuid.uuid4().hex[:8]}"
        
        # 计算变更
        changes = self._compute_changes(workflow_id, parent_ver, workflow_data)
        
        version = WorkflowVersion(
            version_id=version_id,
            workflow_id=workflow_id,
            version_number=new_version,
            commit_message=message,
            changes=changes,
            created_at=time.time(),
            author=author,
            parent_version=parent_ver
        )
        
        self.versions[workflow_id].append(version)
        self.parent._save_data()
        
        return version
    
    def _increment_version(self, version: str) -> str:
        """递增版本号"""
        parts = version.split(".")
        if len(parts) == 3:
            major, minor, patch = int(parts[0]), int(parts[1]), int(parts[2]) + 1
            return f"{major}.{minor}.{patch}"
        return version
    
    def _compute_changes(self, workflow_id: str, parent_version: Optional[str], 
                         current_data: Dict) -> Dict[str, Any]:
        """计算变更"""
        if not parent_version:
            return {"type": "initial", "description": "Initial version"}
        
        parent_data = self.get_version_data(workflow_id, parent_version)
        if not parent_data:
            return {"type": "unknown_parent"}
        
        changes = {"type": "modified"}
        
        # 比较步骤
        old_steps = parent_data.get("steps", [])
        new_steps = current_data.get("steps", [])
        
        changes["steps_added"] = len(new_steps) - len(old_steps) if len(new_steps) > len(old_steps) else 0
        changes["steps_removed"] = len(old_steps) - len(new_steps) if len(old_steps) > len(new_steps) else 0
        changes["steps_modified"] = sum(
            1 for i, (old, new) in enumerate(zip(old_steps, new_steps))
            if old != new
        )
        
        return changes
    
    def get_version_data(self, workflow_id: str, version_number: str) -> Optional[Dict]:
        """获取特定版本的数据"""
        versions = self.versions.get(workflow_id, [])
        for v in versions:
            if v.version_number == version_number:
                return self.parent.workflow_registry.get(workflow_id)
        return None
    
    def list_versions(self, workflow_id: str) -> List[WorkflowVersion]:
        """列出所有版本"""
        return self.versions.get(workflow_id, [])
    
    def diff_versions(self, workflow_id: str, from_version: str, 
                      to_version: str) -> Optional[WorkflowDiff]:
        """比较两个版本的差异"""
        from_data = self.get_version_data(workflow_id, from_version)
        to_data = self.get_version_data(workflow_id, to_version)
        
        if not from_data or not to_data:
            return None
        
        from_steps = from_data.get("steps", [])
        to_steps = to_data.get("steps", [])
        
        added = []
        removed = []
        modified = []
        step_changes = {}
        
        # 找出添加的步骤
        for i, step in enumerate(to_steps):
            if i >= len(from_steps):
                added.append(step)
            elif step != from_steps[i]:
                modified.append(step)
                step_changes[i] = (from_steps[i], step)
        
        # 找出移除的步骤
        for i, step in enumerate(from_steps):
            if i >= len(to_steps):
                removed.append(step)
        
        summary = f"+{len(added)}/-{len(removed)}/~{len(modified)}"
        
        return WorkflowDiff(
            workflow_id=workflow_id,
            from_version=from_version,
            to_version=to_version,
            added_steps=added,
            removed_steps=removed,
            modified_steps=modified,
            step_changes=step_changes,
            summary=summary
        )
    
    def rollback(self, workflow_id: str, version_number: str) -> bool:
        """回滚到指定版本"""
        version_data = self.get_version_data(workflow_id, version_number)
        if not version_data:
            return False
        
        # 创建回滚版本
        self.commit(workflow_id, f"Rollback to {version_number}", author="system")
        return True


# ========== Main Workflow Share System ==========

class WorkflowShareSystem:
    """无代码工作流分享系统 v23"""
    
    def __init__(self, data_dir: str = "./data", cloud_config: Optional[Dict] = None):
        self.data_dir = data_dir
        self.share_links: Dict[str, WorkflowShareLink] = {}
        self.workflow_registry: Dict[str, Dict] = {}
        self.cloud_storage: Optional[CloudStorageInterface] = None
        self.version_control: WorkflowVersionControl = WorkflowVersionControl(self)
        
        # 新增功能数据存储
        self.templates: Dict[str, WorkflowTemplate] = {}
        self.comments: Dict[str, List[WorkflowComment]] = defaultdict(list)
        self.teams: Dict[str, Team] = {}
        self.analytics: Dict[str, WorkflowAnalytics] = {}
        self.marketplace: Dict[str, MarketplaceEntry] = {}
        self.categories: Dict[str, List[str]] = defaultdict(list)  # category -> workflow_ids
        self.tag_index: Dict[str, List[str]] = defaultdict(list)  # tag -> workflow_ids
        
        # 云同步状态
        self.last_sync_at: Optional[float] = None
        self.sync_enabled: bool = False
        
        if cloud_config:
            self._init_cloud_storage(cloud_config)
        
        self._load_data()
        
    def _init_cloud_storage(self, config: Dict[str, Any]) -> None:
        """初始化云存储"""
        provider = config.get("provider", "mock")
        if provider == "s3" or provider == "minio":
            # 使用S3兼容存储 (需要boto3)
            try:
                import boto3
                self.cloud_storage = S3Storage(config)
            except ImportError:
                self.cloud_storage = MockCloudStorage(config)
        else:
            self.cloud_storage = MockCloudStorage(config)
        self.sync_enabled = True
    
    def _load_data(self) -> None:
        """加载数据"""
        # 加载分享链接
        try:
            with open(f"{self.data_dir}/share_links.json", "r", encoding="utf-8") as f:
                data = json.load(f)
                for link_id, link_data in data.items():
                    if "share_type" in link_data:
                        link_data["share_type"] = ShareType(link_data["share_type"])
                    else:
                        link_data["share_type"] = ShareType.PUBLIC
                    self.share_links[link_id] = WorkflowShareLink(**link_data)
        except FileNotFoundError:
            pass
        except (json.JSONDecodeError, KeyError):
            pass
            
        # 加载工作流注册表
        try:
            with open(f"{self.data_dir}/workflow_registry.json", "r", encoding="utf-8") as f:
                self.workflow_registry = json.load(f)
        except FileNotFoundError:
            pass
        except json.JSONDecodeError:
            pass
        
        # 加载模板
        try:
            with open(f"{self.data_dir}/templates.json", "r", encoding="utf-8") as f:
                templates_data = json.load(f)
                for tid, tdata in templates_data.items():
                    self.templates[tid] = WorkflowTemplate(**tdata)
        except FileNotFoundError:
            pass
        
        # 加载评论
        try:
            with open(f"{self.data_dir}/comments.json", "r", encoding="utf-8") as f:
                comments_data = json.load(f)
                for wid, clist in comments_data.items():
                    self.comments[wid] = [WorkflowComment(**c) for c in clist]
        except FileNotFoundError:
            pass
        
        # 加载团队
        try:
            with open(f"{self.data_dir}/teams.json", "r", encoding="utf-8") as f:
                teams_data = json.load(f)
                for tid, tdata in teams_data.items():
                    tdata["members"] = [TeamMember(**m) for m in tdata["members"]]
                    self.teams[tid] = Team(**tdata)
        except FileNotFoundError:
            pass
        
        # 加载分析数据
        try:
            with open(f"{self.data_dir}/analytics.json", "r", encoding="utf-8") as f:
                analytics_data = json.load(f)
                for wid, adata in analytics_data.items():
                    self.analytics[wid] = WorkflowAnalytics(**adata)
        except FileNotFoundError:
            pass
        
        # 加载市场
        try:
            with open(f"{self.data_dir}/marketplace.json", "r", encoding="utf-8") as f:
                market_data = json.load(f)
                for eid, edata in market_data.items():
                    edata["template"] = WorkflowTemplate(**edata["template"])
                    self.marketplace[eid] = MarketplaceEntry(**edata)
        except FileNotFoundError:
            pass
        
        # 加载版本控制
        try:
            with open(f"{self.data_dir}/versions.json", "r", encoding="utf-8") as f:
                versions_data = json.load(f)
                for wid, vlist in versions_data.items():
                    self.version_control.versions[wid] = [WorkflowVersion(**v) for v in vlist]
        except FileNotFoundError:
            pass
        
        # 加载分类和标签索引
        try:
            with open(f"{self.data_dir}/categories.json", "r", encoding="utf-8") as f:
                cat_data = json.load(f)
                self.categories = defaultdict(list, cat_data)
        except FileNotFoundError:
            pass
        
        try:
            with open(f"{self.data_dir}/tags.json", "r", encoding="utf-8") as f:
                tag_data = json.load(f)
                self.tag_index = defaultdict(list, tag_data)
        except FileNotFoundError:
            pass
    
    def _save_data(self) -> None:
        """保存数据"""
        def convert_for_json(obj):
            """转换对象以便 JSON 序列化"""
            if hasattr(obj, 'value'):
                return obj.value
            elif isinstance(obj, dict):
                return {k: convert_for_json(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_for_json(x) for x in obj]
            elif isinstance(obj, (set, frozenset)):
                return list(obj)
            else:
                return obj
        
        # 保存分享链接
        data = {}
        for link_id, link in self.share_links.items():
            d = asdict(link)
            d["share_type"] = link.share_type.value
            data[link_id] = convert_for_json(d)
            
        with open(f"{self.data_dir}/share_links.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            
        with open(f"{self.data_dir}/workflow_registry.json", "w", encoding="utf-8") as f:
            json.dump(convert_for_json(self.workflow_registry), f, ensure_ascii=False, indent=2)
        
        # 保存模板
        with open(f"{self.data_dir}/templates.json", "w", encoding="utf-8") as f:
            json.dump(convert_for_json(self.templates), f, ensure_ascii=False, indent=2)
        
        # 保存评论
        comments_data = {wid: [asdict(c) for c in clist] for wid, clist in self.comments.items()}
        with open(f"{self.data_dir}/comments.json", "w", encoding="utf-8") as f:
            json.dump(comments_data, f, ensure_ascii=False, indent=2)
        
        # 保存团队
        with open(f"{self.data_dir}/teams.json", "w", encoding="utf-8") as f:
            json.dump(convert_for_json(self.teams), f, ensure_ascii=False, indent=2)
        
        # 保存分析数据
        with open(f"{self.data_dir}/analytics.json", "w", encoding="utf-8") as f:
            json.dump(convert_for_json(self.analytics), f, ensure_ascii=False, indent=2)
        
        # 保存市场
        with open(f"{self.data_dir}/marketplace.json", "w", encoding="utf-8") as f:
            json.dump(convert_for_json(self.marketplace), f, ensure_ascii=False, indent=2)
        
        # 保存版本控制
        versions_data = {wid: [asdict(v) for v in vlist] for wid, vlist in self.version_control.versions.items()}
        with open(f"{self.data_dir}/versions.json", "w", encoding="utf-8") as f:
            json.dump(versions_data, f, ensure_ascii=False, indent=2)
        
        # 保存分类和标签
        with open(f"{self.data_dir}/categories.json", "w", encoding="utf-8") as f:
            json.dump(dict(self.categories), f, ensure_ascii=False, indent=2)
        
        with open(f"{self.data_dir}/tags.json", "w", encoding="utf-8") as f:
            json.dump(dict(self.tag_index), f, ensure_ascii=False, indent=2)
    
    def _generate_link_id(self, workflow_id: str) -> str:
        """生成链接ID"""
        raw = f"{workflow_id}{time.time()}"
        hash_obj = hashlib.sha256(raw.encode())
        return base64.urlsafe_b64encode(hash_obj.digest()[:8]).decode()[:12]
    
    # ========== 原有功能 (保持兼容) ==========
    
    def register_workflow(self, workflow_data: Dict[str, Any]) -> str:
        """注册工作流"""
        workflow_id = workflow_data.get("workflow_id", f"wf_{int(time.time())}")
        
        # 计算校验和
        checksum = self._calculate_checksum(workflow_data)
        workflow_data["_checksum"] = checksum
        
        # 更新分类索引
        category = workflow_data.get("category")
        if category:
            if workflow_id not in self.categories[category]:
                self.categories[category].append(workflow_id)
        
        # 更新标签索引
        tags = workflow_data.get("tags", [])
        for tag in tags:
            if workflow_id not in self.tag_index[tag]:
                self.tag_index[tag].append(workflow_id)
        
        # 初始化分析数据
        if workflow_id not in self.analytics:
            self.analytics[workflow_id] = WorkflowAnalytics(workflow_id=workflow_id)
        
        self.workflow_registry[workflow_id] = workflow_data
        self._save_data()
        
        return workflow_id
    
    def _calculate_checksum(self, data: Dict) -> str:
        """计算校验和"""
        def clean_dict(d):
            result = {}
            for k, v in d.items():
                if k.startswith("_"):
                    continue
                if hasattr(v, '__dict__'):
                    continue
                if isinstance(v, dict):
                    result[k] = clean_dict(v)
                elif isinstance(v, list):
                    result[k] = [clean_dict(x) if isinstance(x, dict) else x for x in v]
                else:
                    result[k] = v
            return result
        
        clean = clean_dict(data)
        content = json.dumps(clean, sort_keys=True, default=str)
        return hashlib.sha256(content.encode()).hexdigest()[:16]
    
    def verify_workflow(self, workflow_data: Dict) -> bool:
        """验证工作流数据"""
        stored_checksum = workflow_data.get("_checksum")
        if not stored_checksum:
            return False
            
        calculated = self._calculate_checksum(workflow_data)
        return stored_checksum == calculated
    
    def create_share_link(self, workflow_id: str, 
                         share_type: ShareType = ShareType.PUBLIC,
                         expires_in_days: Optional[int] = None,
                         team_id: Optional[str] = None) -> Optional[WorkflowShareLink]:
        """创建分享链接"""
        workflow_data = self.workflow_registry.get(workflow_id)
        if not workflow_data:
            return None
            
        link_id = self._generate_link_id(workflow_id)
        
        expires_at = None
        if expires_in_days:
            expires_at = time.time() + expires_in_days * 86400
        
        link = WorkflowShareLink(
            link_id=link_id,
            workflow_id=workflow_id,
            workflow_name=workflow_data.get("name", "未命名"),
            workflow_data=workflow_data,
            share_type=share_type,
            created_at=time.time(),
            expires_at=expires_at,
            metadata={
                "category": workflow_data.get("category", "general"),
                "tags": workflow_data.get("tags", []),
                "author": workflow_data.get("author", "anonymous")
            },
            team_id=team_id
        )
        
        self.share_links[link_id] = link
        self._save_data()
        
        # 更新分析数据
        if workflow_id in self.analytics:
            self.analytics[workflow_id].share_count += 1
        
        return link
    
    def get_share_link(self, link_id: str) -> Optional[WorkflowShareLink]:
        """获取分享链接"""
        link = self.share_links.get(link_id)
        
        if link:
            if link.expires_at and time.time() > link.expires_at:
                return None
                
            link.view_count += 1
            self._save_data()
            
            # 更新分析
            if link.workflow_id in self.analytics:
                self.analytics[link.workflow_id].view_count += 1
        
        return link
    
    def generate_share_url(self, link_id: str, base_url: str = "https://rabai.app/share") -> str:
        """生成分享URL"""
        return f"{base_url}/{link_id}"
    
    def parse_share_url(self, url: str) -> Optional[str]:
        """解析分享URL"""
        try:
            parsed = urlparse(url)
            path = parsed.path.strip("/")
            
            if path.startswith("share/"):
                return path.split("/")[-1]
            elif path.startswith("s/"):
                return path[2:]
            elif path.startswith("wf/"):
                return path[3:]
                
            return None
        except Exception:
            return None
    
    def export_workflow(self, workflow_id: str, 
                       include_sensitive: bool = False) -> Optional[WorkflowExportData]:
        """导出工作流"""
        workflow_data = self.workflow_registry.get(workflow_id)
        if not workflow_data:
            return None
            
        export_data = {k: v for k, v in workflow_data.items() 
                      if not k.startswith("_") 
                      and (include_sensitive or k not in ["api_key", "password", "token"])}
        
        checksum = self._calculate_checksum(export_data)
        
        return WorkflowExportData(
            version="23.0.0",
            workflow_id=workflow_id,
            name=export_data.get("name", "未命名"),
            description=export_data.get("description", ""),
            steps=export_data.get("steps", []),
            triggers=export_data.get("triggers", []),
            settings=export_data.get("settings", {}),
            exported_at=time.time(),
            checksum=checksum
        )
    
    def export_to_json(self, workflow_id: str, 
                      include_sensitive: bool = False) -> Optional[str]:
        """导出为JSON字符串"""
        export = self.export_workflow(workflow_id, include_sensitive)
        if not export:
            return None
            
        data = asdict(export)
        return json.dumps(data, ensure_ascii=False, indent=2)
    
    def export_to_base64(self, workflow_id: str) -> Optional[str]:
        """导出为Base64编码"""
        json_str = self.export_to_json(workflow_id)
        if not json_str:
            return None
            
        return base64.b64encode(json_str.encode()).decode()
    
    def import_workflow(self, data: str, 
                       source: str = "json") -> ImportReport:
        """导入工作流"""
        try:
            if source == "json":
                workflow_data = json.loads(data)
            elif source == "base64":
                workflow_data = json.loads(base64.b64decode(data.encode()).decode())
            else:
                return ImportReport(
                    result=ImportResult.INVALID_FORMAT,
                    workflow_id="",
                    workflow_name="",
                    message=f"不支持的导入格式: {source}"
                )
            
            version = workflow_data.get("version", "0.0.0")
            if not version.startswith("23") and not version.startswith("22"):
                return ImportReport(
                    result=ImportResult.VERSION_MISMATCH,
                    workflow_id=workflow_data.get("workflow_id", ""),
                    workflow_name=workflow_data.get("name", ""),
                    message=f"版本不匹配: 需要 v22.x.x 或 v23.x.x，当前为 {version}"
                )
            
            if not self._validate_workflow_structure(workflow_data):
                return ImportReport(
                    result=ImportResult.VALIDATION_FAILED,
                    workflow_id=workflow_data.get("workflow_id", ""),
                    workflow_name=workflow_data.get("name", ""),
                    message="工作流数据结构验证失败"
                )
            
            workflow_id = workflow_data.get("workflow_id", f"wf_{int(time.time())}")
            workflow_data["imported_at"] = time.time()
            workflow_data["import_source"] = source
            
            self.workflow_registry[workflow_id] = workflow_data
            self._save_data()
            
            # 更新分析
            if workflow_id in self.analytics:
                self.analytics[workflow_id].import_count += 1
            
            return ImportReport(
                result=ImportResult.SUCCESS,
                workflow_id=workflow_id,
                workflow_name=workflow_data.get("name", "未命名"),
                message="工作流导入成功"
            )
            
        except json.JSONDecodeError:
            return ImportReport(
                result=ImportResult.INVALID_FORMAT,
                workflow_id="",
                workflow_name="",
                message="无效的JSON格式"
            )
        except Exception as e:
            return ImportReport(
                result=ImportResult.INVALID_FORMAT,
                workflow_id="",
                workflow_name="",
                message=f"导入失败: {str(e)}"
            )
    
    def _validate_workflow_structure(self, data: Dict) -> bool:
        """验证工作流结构"""
        required_fields = ["name", "steps"]
        
        for field in required_fields:
            if field not in data:
                return False
                
        steps = data.get("steps", [])
        if not isinstance(steps, list):
            return False
            
        for step in steps:
            if not isinstance(step, dict):
                return False
            if "action" not in step:
                return False
                
        return True
    
    def import_from_url(self, url: str) -> ImportReport:
        """从URL导入"""
        # 支持直接HTTP URL导入
        if url.startswith("http://") or url.startswith("https://"):
            return self._import_from_http_url(url)
        
        link_id = self.parse_share_url(url)
        if not link_id:
            return ImportReport(
                result=ImportResult.INVALID_FORMAT,
                workflow_id="",
                workflow_name="",
                message="无效的分享链接"
            )
            
        link = self.get_share_link(link_id)
        if not link:
            return ImportReport(
                result=ImportResult.INVALID_FORMAT,
                workflow_id="",
                workflow_name="",
                message="链接不存在或已过期"
            )
        
        link.import_count += 1
        self._save_data()
        
        workflow_data = link.workflow_data.copy()
        workflow_id = workflow_data.get("workflow_id", f"wf_{int(time.time())}")
        
        self.workflow_registry[workflow_id] = workflow_data
        self._save_data()
        
        # 更新分析
        if workflow_id in self.analytics:
            self.analytics[workflow_id].import_count += 1
        
        return ImportReport(
            result=ImportResult.SUCCESS,
            workflow_id=workflow_id,
            workflow_name=workflow_data.get("name", "未命名"),
            message=f"从分享链接导入成功 (链接已使用 {link.import_count} 次)"
        )
    
    def _import_from_http_url(self, url: str) -> ImportReport:
        """从HTTP URL导入工作流"""
        try:
            import urllib.request
            with urllib.request.urlopen(url, timeout=30) as response:
                data = response.read().decode("utf-8")
                return self.import_workflow(data, source="json")
        except Exception as e:
            return ImportReport(
                result=ImportResult.NETWORK_ERROR,
                workflow_id="",
                workflow_name="",
                message=f"从URL导入失败: {str(e)}"
            )
    
    def list_shared_workflows(self, share_type: ShareType = None) -> List[WorkflowShareLink]:
        """列出分享的工作流"""
        if share_type:
            return [link for link in self.share_links.values() 
                   if link.share_type == share_type]
        return list(self.share_links.values())
    
    def delete_share_link(self, link_id: str) -> bool:
        """删除分享链接"""
        if link_id in self.share_links:
            del self.share_links[link_id]
            self._save_data()
            return True
        return False
    
    def get_share_stats(self) -> Dict[str, Any]:
        """获取分享统计"""
        total_views = sum(link.view_count for link in self.share_links.values())
        total_imports = sum(link.import_count for link in self.share_links.values())
        
        return {
            "total_links": len(self.share_links),
            "total_views": total_views,
            "total_imports": total_imports,
            "active_links": sum(1 for link in self.share_links.values() 
                              if not link.expires_at or time.time() < link.expires_at),
            "expired_links": sum(1 for link in self.share_links.values() 
                               if link.expires_at and time.time() > link.expires_at)
        }
    
    # ========== Cloud Sync ==========
    
    def sync_to_cloud(self) -> bool:
        """同步数据到云端"""
        if not self.cloud_storage or not self.sync_enabled:
            return False
        
        try:
            # 同步所有数据
            data = {
                "workflow_registry": self.workflow_registry,
                "share_links": {k: asdict(v) for k, v in self.share_links.items()},
                "versions": {wid: [asdict(v) for v in vlist] for wid, vlist in self.version_control.versions.items()},
                "templates": {k: asdict(v) for k, v in self.templates.items()},
                "synced_at": time.time()
            }
            
            json_data = json.dumps(data, ensure_ascii=False).encode("utf-8")
            key = f"sync/{self.data_dir}_{int(time.time())}.json"
            
            if self.cloud_storage.upload(key, json_data):
                self.last_sync_at = time.time()
                return True
            return False
        except Exception:
            return False
    
    def sync_from_cloud(self) -> bool:
        """从云端同步数据"""
        if not self.cloud_storage or not self.sync_enabled:
            return False
        
        try:
            # 获取最新同步文件
            keys = self.cloud_storage.list("sync/")
            if not keys:
                return False
            
            latest_key = sorted(keys)[-1]
            data = self.cloud_storage.download(latest_key)
            
            if not data:
                return False
            
            cloud_data = json.loads(data.decode("utf-8"))
            
            # 合并数据
            if "workflow_registry" in cloud_data:
                self.workflow_registry.update(cloud_data["workflow_registry"])
            
            self.last_sync_at = cloud_data.get("synced_at")
            self._save_data()
            return True
        except Exception:
            return False
    
    # ========== Version Control ==========
    
    def version_commit(self, workflow_id: str, message: str, author: str = "anonymous") -> Optional[WorkflowVersion]:
        """创建版本提交"""
        return self.version_control.commit(workflow_id, message, author)
    
    def version_list(self, workflow_id: str) -> List[WorkflowVersion]:
        """列出版本"""
        return self.version_control.list_versions(workflow_id)
    
    def version_diff(self, workflow_id: str, from_ver: str, to_ver: str) -> Optional[WorkflowDiff]:
        """比较版本差异"""
        return self.version_control.diff_versions(workflow_id, from_ver, to_ver)
    
    def version_rollback(self, workflow_id: str, version_number: str) -> bool:
        """回滚版本"""
        return self.version_control.rollback(workflow_id, version_number)
    
    def get_text_diff(self, from_text: str, to_text: str) -> str:
        """生成文本差异"""
        from_lines = from_text.splitlines(keepends=True)
        to_lines = to_text.splitlines(keepends=True)
        diff = difflib.unified_diff(from_lines, to_lines, fromfile="old", tofile="new")
        return "".join(diff)
    
    # ========== Templates ==========
    
    def create_template(self, workflow_id: str, name: str, description: str,
                       category: str, tags: List[str], author: str = "anonymous",
                       is_official: bool = False) -> Optional[WorkflowTemplate]:
        """创建工作流模板"""
        workflow_data = self.workflow_registry.get(workflow_id)
        if not workflow_data:
            return None
        
        template_id = f"tpl_{uuid.uuid4().hex[:12]}"
        
        template = WorkflowTemplate(
            template_id=template_id,
            name=name,
            description=description,
            category=category,
            tags=tags,
            template_data={k: v for k, v in workflow_data.items() if not k.startswith("_")},
            author=author,
            created_at=time.time(),
            is_official=is_official
        )
        
        self.templates[template_id] = template
        self._save_data()
        
        return template
    
    def instantiate_template(self, template_id: str, new_name: str, 
                            new_workflow_id: Optional[str] = None) -> Optional[str]:
        """从模板实例化工作流"""
        template = self.templates.get(template_id)
        if not template:
            return None
        
        workflow_id = new_workflow_id or f"wf_{int(time.time())}"
        
        new_workflow = template.template_data.copy()
        new_workflow["workflow_id"] = workflow_id
        new_workflow["name"] = new_name
        new_workflow["created_from_template"] = template_id
        new_workflow["created_at"] = time.time()
        
        self.workflow_registry[workflow_id] = new_workflow
        
        # 更新模板使用统计
        template.usage_count += 1
        self._save_data()
        
        return workflow_id
    
    def list_templates(self, category: Optional[str] = None, 
                      tags: Optional[List[str]] = None) -> List[WorkflowTemplate]:
        """列出模板"""
        templates = list(self.templates.values())
        
        if category:
            templates = [t for t in templates if t.category == category]
        
        if tags:
            templates = [t for t in templates if any(tag in t.tags for tag in tags)]
        
        return sorted(templates, key=lambda t: -t.usage_count)
    
    def clone_workflow(self, workflow_id: str, new_name: str, 
                       new_workflow_id: Optional[str] = None) -> Optional[str]:
        """克隆工作流"""
        workflow_data = self.workflow_registry.get(workflow_id)
        if not workflow_data:
            return None
        
        cloned_id = new_workflow_id or f"wf_{int(time.time())}"
        
        cloned_data = workflow_data.copy()
        cloned_data["workflow_id"] = cloned_id
        cloned_data["name"] = new_name
        cloned_data["cloned_from"] = workflow_id
        cloned_data["created_at"] = time.time()
        
        self.workflow_registry[cloned_id] = cloned_data
        
        # 更新分析
        if workflow_id in self.analytics:
            self.analytics[workflow_id].clone_count += 1
        
        self._save_data()
        return cloned_id
    
    # ========== Categories & Tags ==========
    
    def set_category(self, workflow_id: str, category: str) -> bool:
        """设置工作流分类"""
        if workflow_id not in self.workflow_registry:
            return False
        
        # 从旧分类移除
        for cat, ids in self.categories.items():
            if workflow_id in ids:
                ids.remove(workflow_id)
        
        # 添加到新分类
        self.categories[category].append(workflow_id)
        
        # 更新工作流数据
        self.workflow_registry[workflow_id]["category"] = category
        self._save_data()
        
        return True
    
    def add_tag(self, workflow_id: str, tag: str) -> bool:
        """添加标签"""
        if workflow_id not in self.workflow_registry:
            return False
        
        if workflow_id not in self.tag_index[tag]:
            self.tag_index[tag].append(workflow_id)
        
        tags = self.workflow_registry[workflow_id].get("tags", [])
        if tag not in tags:
            tags.append(tag)
            self.workflow_registry[workflow_id]["tags"] = tags
        
        self._save_data()
        return True
    
    def remove_tag(self, workflow_id: str, tag: str) -> bool:
        """移除标签"""
        if workflow_id not in self.workflow_registry:
            return False
        
        if workflow_id in self.tag_index[tag]:
            self.tag_index[tag].remove(workflow_id)
        
        tags = self.workflow_registry[workflow_id].get("tags", [])
        if tag in tags:
            tags.remove(tag)
            self.workflow_registry[workflow_id]["tags"] = tags
        
        self._save_data()
        return True
    
    def get_by_category(self, category: str) -> List[str]:
        """按分类获取工作流"""
        return self.categories.get(category, [])
    
    def get_by_tag(self, tag: str) -> List[str]:
        """按标签获取工作流"""
        return self.tag_index.get(tag, [])
    
    def list_categories(self) -> List[str]:
        """列出所有分类"""
        return list(self.categories.keys())
    
    def list_tags(self) -> List[str]:
        """列出所有标签"""
        return list(self.tag_index.keys())
    
    # ========== Search ==========
    
    def search_workflows(self, query: str, limit: int = 50) -> List[Tuple[str, float]]:
        """全文搜索工作流"""
        query_lower = query.lower()
        query_words = query_lower.split()
        
        results = []
        
        for workflow_id, workflow in self.workflow_registry.items():
            score = 0.0
            searchable_text = []
            
            # 收集可搜索文本
            if "name" in workflow:
                searchable_text.append(workflow["name"].lower())
            if "description" in workflow:
                searchable_text.append(workflow["description"].lower())
            if "category" in workflow:
                searchable_text.append(workflow["category"].lower())
            if "tags" in workflow:
                searchable_text.extend([t.lower() for t in workflow["tags"]])
            
            # 搜索步骤
            for step in workflow.get("steps", []):
                if "action" in step:
                    searchable_text.append(step["action"].lower())
                if "target" in step:
                    searchable_text.append(step["target"].lower())
            
            full_text = " ".join(searchable_text)
            
            # 计算匹配分数
            for word in query_words:
                if word in full_text:
                    score += 1.0
                    # 精确匹配额外加分
                    if any(word in text for text in searchable_text):
                        score += 0.5
            
            if score > 0:
                results.append((workflow_id, score))
        
        # 按分数排序
        results.sort(key=lambda x: -x[1])
        return results[:limit]
    
    # ========== Marketplace ==========
    
    def submit_to_marketplace(self, workflow_id: str, author: str = "anonymous",
                             description: str = "", category: str = "general",
                             tags: Optional[List[str]] = None) -> Optional[MarketplaceEntry]:
        """提交到市场"""
        workflow_data = self.workflow_registry.get(workflow_id)
        if not workflow_data:
            return None
        
        template = self.create_template(
            workflow_id=workflow_id,
            name=workflow_data.get("name", "未命名"),
            description=description or workflow_data.get("description", ""),
            category=category,
            tags=tags or workflow_data.get("tags", []),
            author=author
        )
        
        if not template:
            return None
        
        entry_id = f"mkt_{uuid.uuid4().hex[:12]}"
        
        entry = MarketplaceEntry(
            entry_id=entry_id,
            workflow_id=workflow_id,
            template=template,
            author=author,
            submitted_at=time.time(),
            status="pending"
        )
        
        self.marketplace[entry_id] = entry
        self._save_data()
        
        return entry
    
    def approve_marketplace_entry(self, entry_id: str, review_notes: str = "") -> bool:
        """批准市场条目"""
        entry = self.marketplace.get(entry_id)
        if not entry:
            return False
        
        entry.status = "approved"
        entry.review_notes = review_notes
        self._save_data()
        return True
    
    def reject_marketplace_entry(self, entry_id: str, reason: str) -> bool:
        """拒绝市场条目"""
        entry = self.marketplace.get(entry_id)
        if not entry:
            return False
        
        entry.status = "rejected"
        entry.review_notes = reason
        self._save_data()
        return True
    
    def list_marketplace(self, status: str = "approved", 
                        category: Optional[str] = None) -> List[MarketplaceEntry]:
        """列出市场条目"""
        entries = [e for e in self.marketplace.values() if e.status == status]
        
        if category:
            entries = [e for e in entries if e.template.category == category]
        
        return sorted(entries, key=lambda e: -e.template.usage_count)
    
    def purchase_marketplace_item(self, entry_id: str) -> Optional[str]:
        """购买/获取市场工作流"""
        entry = self.marketplace.get(entry_id)
        if not entry or entry.status != "approved":
            return None
        
        # 克隆到本地
        return self.clone_workflow(
            entry.workflow_id,
            f"{entry.template.name} (Copy)",
            f"wf_{int(time.time())}"
        )
    
    # ========== Analytics ==========
    
    def track_execution(self, workflow_id: str) -> bool:
        """跟踪工作流执行"""
        if workflow_id not in self.analytics:
            self.analytics[workflow_id] = WorkflowAnalytics(workflow_id=workflow_id)
        
        analytics = self.analytics[workflow_id]
        analytics.execution_count += 1
        analytics.last_executed_at = time.time()
        
        # 更新日统计
        date_key = time.strftime("%Y-%m-%d")
        if date_key not in analytics.daily_stats:
            analytics.daily_stats[date_key] = {"executions": 0, "views": 0}
        analytics.daily_stats[date_key]["executions"] += 1
        
        self._save_data()
        return True
    
    def get_analytics(self, workflow_id: str) -> Optional[WorkflowAnalytics]:
        """获取分析数据"""
        return self.analytics.get(workflow_id)
    
    def get_all_analytics(self) -> Dict[str, WorkflowAnalytics]:
        """获取所有分析数据"""
        return self.analytics
    
    # ========== Team Collaboration ==========
    
    def create_team(self, name: str, owner_id: str, owner_name: str) -> Team:
        """创建团队"""
        team_id = f"team_{uuid.uuid4().hex[:12]}"
        
        owner = TeamMember(
            user_id=owner_id,
            username=owner_name,
            role="owner",
            joined_at=time.time()
        )
        
        team = Team(
            team_id=team_id,
            name=name,
            members=[owner],
            shared_workflows=[],
            created_at=time.time()
        )
        
        self.teams[team_id] = team
        self._save_data()
        
        return team
    
    def add_team_member(self, team_id: str, user_id: str, username: str, 
                       role: str = "editor") -> bool:
        """添加团队成员"""
        team = self.teams.get(team_id)
        if not team:
            return False
        
        # 检查是否已存在
        if any(m.user_id == user_id for m in team.members):
            return False
        
        member = TeamMember(
            user_id=user_id,
            username=username,
            role=role,
            joined_at=time.time()
        )
        
        team.members.append(member)
        self._save_data()
        return True
    
    def remove_team_member(self, team_id: str, user_id: str) -> bool:
        """移除团队成员"""
        team = self.teams.get(team_id)
        if not team:
            return False
        
        # 不能移除所有者
        if any(m.user_id == user_id and m.role == "owner" for m in team.members):
            return False
        
        team.members = [m for m in team.members if m.user_id != user_id]
        self._save_data()
        return True
    
    def share_with_team(self, team_id: str, workflow_id: str) -> bool:
        """与团队分享工作流"""
        team = self.teams.get(team_id)
        if not team:
            return False
        
        if workflow_id not in team.shared_workflows:
            team.shared_workflows.append(workflow_id)
        
        # 创建团队分享链接
        self.create_share_link(workflow_id, ShareType.TEAM, team_id=team_id)
        self._save_data()
        return True
    
    def get_team_workflows(self, team_id: str, user_id: str) -> List[str]:
        """获取团队工作流"""
        team = self.teams.get(team_id)
        if not team:
            return []
        
        # 检查用户是否是团队成员
        if not any(m.user_id == user_id for m in team.members):
            return []
        
        return team.shared_workflows
    
    def list_teams_for_user(self, user_id: str) -> List[Team]:
        """列出用户所属团队"""
        return [t for t in self.teams.values() if any(m.user_id == user_id for m in t.members)]
    
    # ========== Comments ==========
    
    def add_comment(self, workflow_id: str, step_index: int, content: str,
                   author: str = "anonymous", parent_comment_id: Optional[str] = None) -> WorkflowComment:
        """添加评论"""
        comment_id = f"cmt_{uuid.uuid4().hex[:12]}"
        
        comment = WorkflowComment(
            comment_id=comment_id,
            workflow_id=workflow_id,
            step_index=step_index,
            content=content,
            author=author,
            created_at=time.time(),
            parent_comment_id=parent_comment_id
        )
        
        self.comments[workflow_id].append(comment)
        self._save_data()
        
        return comment
    
    def get_comments(self, workflow_id: str, step_index: Optional[int] = None) -> List[WorkflowComment]:
        """获取评论"""
        comments = self.comments.get(workflow_id, [])
        
        if step_index is not None:
            comments = [c for c in comments if c.step_index == step_index]
        
        return sorted(comments, key=lambda c: c.created_at)
    
    def delete_comment(self, comment_id: str, workflow_id: str) -> bool:
        """删除评论"""
        comments = self.comments.get(workflow_id, [])
        original_len = len(comments)
        
        comments = [c for c in comments if c.comment_id != comment_id]
        
        if len(comments) < original_len:
            self.comments[workflow_id] = comments
            self._save_data()
            return True
        
        return False
    
    def resolve_comment(self, comment_id: str, workflow_id: str) -> bool:
        """标记评论为已解决"""
        comments = self.comments.get(workflow_id, [])
        
        for comment in comments:
            if comment.comment_id == comment_id:
                # 添加标记到metadata
                if not hasattr(comment, 'resolved'):
                    comment.resolved = True
                self._save_data()
                return True
        
        return False


def create_share_system(data_dir: str = "./data", 
                       cloud_config: Optional[Dict] = None) -> WorkflowShareSystem:
    """创建分享系统实例"""
    return WorkflowShareSystem(data_dir, cloud_config)


# ========== Backward Compatibility Alias ==========
WorkflowShareSystemV22 = WorkflowShareSystem


# 测试
if __name__ == "__main__":
    share = create_share_system("./data")
    
    # 注册工作流
    workflow = {
        "workflow_id": "wf_email_morning",
        "name": "晨间邮件处理",
        "description": "自动处理早晨邮件",
        "category": "productivity",
        "tags": ["email", "morning", "automation"],
        "steps": [
            {"action": "open_app", "target": "Outlook"},
            {"action": "click", "target": "Inbox"},
            {"action": "read", "target": "unread_emails"}
        ],
        "triggers": [
            {"type": "time", "value": "09:00"}
        ]
    }
    
    wf_id = share.register_workflow(workflow)
    print(f"已注册工作流: {wf_id}")
    
    # 版本控制测试
    version1 = share.version_commit(wf_id, "Initial commit", author="test")
    print(f"创建版本: {version1.version_number if version1 else 'failed'}")
    
    # 修改工作流
    workflow["steps"].append({"action": "send", "target": "response"})
    share.workflow_registry[wf_id] = workflow
    
    version2 = share.version_commit(wf_id, "Add send response step", author="test")
    print(f"创建版本: {version2.version_number if version2 else 'failed'}")
    
    # 版本差异
    if version1 and version2:
        diff = share.version_diff(wf_id, version1.version_number, version2.version_number)
        if diff:
            print(f"版本差异: {diff.summary}")
    
    # 创建模板
    template = share.create_template(
        wf_id, "邮件处理模板", "通用的邮件处理工作流",
        "productivity", ["email", "automation"], author="test"
    )
    print(f"创建模板: {template.template_id if template else 'failed'}")
    
    # 创建分享链接
    link = share.create_share_link(wf_id, ShareType.PUBLIC, expires_in_days=7)
    if link:
        print(f"\n分享链接: {share.generate_share_url(link.link_id)}")
        print(f"链接ID: {link.link_id}")
        
        # 导出为Base64
        b64 = share.export_to_base64(wf_id)
        print(f"\nBase64导出: {b64[:50]}...")
        
        # 测试导入
        report = share.import_workflow(b64, "base64")
        print(f"\n导入报告: {report.result.value} - {report.message}")
    
    # 搜索测试
    results = share.search_workflows("邮件")
    print(f"\n搜索'邮件'结果: {results}")
    
    # 添加评论
    comment = share.add_comment(wf_id, 0, "这个步骤需要检查应用是否已安装", author="reviewer")
    print(f"添加评论: {comment.comment_id}")
    
    # 创建团队
    team = share.create_team("自动化团队", "user1", "Alice")
    print(f"创建团队: {team.team_id}")
    
    share.add_team_member(team.team_id, "user2", "Bob", role="editor")
    share.share_with_team(team.team_id, wf_id)
    print(f"团队分享工作流: {share.get_team_workflows(team.team_id, 'user2')}")
    
    # 提交到市场
    entry = share.submit_to_marketplace(wf_id, author="test", description="晨间邮件自动化")
    print(f"市场提交: {entry.entry_id if entry else 'failed'}")
    
    # 模拟执行跟踪
    share.track_execution(wf_id)
    analytics = share.get_analytics(wf_id)
    if analytics:
        print(f"执行次数: {analytics.execution_count}")


if __name__ == "__main__":
    test_import()
