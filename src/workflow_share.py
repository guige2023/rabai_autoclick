"""
无代码工作流分享系统 v22
P0级功能 - 生成可分享的工作流链接，支持导入导出
"""
import json
import hashlib
import base64
import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field, asdict
from enum import Enum
from urllib.parse import urlparse, parse_qs


class ShareType(Enum):
    """分享类型"""
    PUBLIC = "public"           # 公开分享
    PRIVATE = "private"         # 私密分享
    TEAM = "team"              # 团队分享


class ImportResult(Enum):
    """导入结果"""
    SUCCESS = "success"
    INVALID_FORMAT = "invalid_format"
    VERSION_MISMATCH = "version_mismatch"
    VALIDATION_FAILED = "validation_failed"


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
    version: str = "22.0.0"
    metadata: Dict[str, Any] = field(default_factory=dict)


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


class WorkflowShareSystem:
    """无代码工作流分享系统"""
    
    def __init__(self, data_dir: str = "./data"):
        self.data_dir = data_dir
        self.share_links: Dict[str, WorkflowShareLink] = {}
        self.workflow_registry: Dict[str, Dict] = {}  # 本地工作流注册表
        self._load_data()
        
    def _load_data(self) -> None:
        """加载数据"""
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
            # 数据文件损坏，忽略
            pass
            
        try:
            with open(f"{self.data_dir}/workflow_registry.json", "r", encoding="utf-8") as f:
                self.workflow_registry = json.load(f)
        except FileNotFoundError:
            pass
        except json.JSONDecodeError:
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
        
        data = {}
        for link_id, link in self.share_links.items():
            d = asdict(link)
            d["share_type"] = link.share_type.value
            data[link_id] = convert_for_json(d)
            
        with open(f"{self.data_dir}/share_links.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            
        with open(f"{self.data_dir}/workflow_registry.json", "w", encoding="utf-8") as f:
            json.dump(convert_for_json(self.workflow_registry), f, ensure_ascii=False, indent=2)
    
    def _generate_link_id(self, workflow_id: str) -> str:
        """生成链接ID"""
        raw = f"{workflow_id}{time.time()}"
        hash_obj = hashlib.sha256(raw.encode())
        return base64.urlsafe_b64encode(hash_obj.digest()[:8]).decode()[:12]
    
    def register_workflow(self, workflow_data: Dict[str, Any]) -> str:
        """注册工作流"""
        workflow_id = workflow_data.get("workflow_id", f"wf_{int(time.time())}")
        
        # 计算校验和
        checksum = self._calculate_checksum(workflow_data)
        workflow_data["_checksum"] = checksum
        
        self.workflow_registry[workflow_id] = workflow_data
        self._save_data()
        
        return workflow_id
    
    def _calculate_checksum(self, data: Dict) -> str:
        """计算校验和"""
        # 移除动态字段和不可序列化对象
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
                         expires_in_days: Optional[int] = None) -> Optional[WorkflowShareLink]:
        """创建分享链接"""
        # 获取工作流数据
        workflow_data = self.workflow_registry.get(workflow_id)
        if not workflow_data:
            return None
            
        link_id = self._generate_link_id(workflow_id)
        
        # 计算过期时间
        expires_at = None
        if expires_in_days:
            expires_at = time.time() + expires_in_days * 86400
        
        # 创建分享链接
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
            }
        )
        
        self.share_links[link_id] = link
        self._save_data()
        
        return link
    
    def get_share_link(self, link_id: str) -> Optional[WorkflowShareLink]:
        """获取分享链接"""
        link = self.share_links.get(link_id)
        
        if link:
            # 检查是否过期
            if link.expires_at and time.time() > link.expires_at:
                return None
                
            # 更新查看次数
            link.view_count += 1
            self._save_data()
            
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
            
        # 过滤敏感信息
        export_data = {k: v for k, v in workflow_data.items() 
                      if not k.startswith("_") 
                      and (include_sensitive or k not in ["api_key", "password", "token"])}
        
        checksum = self._calculate_checksum(export_data)
        
        return WorkflowExportData(
            version="22.0.0",
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
            # 解析数据
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
            
            # 验证版本
            version = workflow_data.get("version", "0.0.0")
            if not version.startswith("22"):
                return ImportReport(
                    result=ImportResult.VERSION_MISMATCH,
                    workflow_id=workflow_data.get("workflow_id", ""),
                    workflow_name=workflow_data.get("name", ""),
                    message=f"版本不匹配: 需要 v22.x.x，当前为 {version}"
                )
            
            # 验证数据格式
            if not self._validate_workflow_structure(workflow_data):
                return ImportReport(
                    result=ImportResult.VALIDATION_FAILED,
                    workflow_id=workflow_data.get("workflow_id", ""),
                    workflow_name=workflow_data.get("name", ""),
                    message="工作流数据结构验证失败"
                )
            
            # 导入工作流
            workflow_id = workflow_data.get("workflow_id", f"wf_{int(time.time())}")
            workflow_data["imported_at"] = time.time()
            workflow_data["import_source"] = source
            
            self.workflow_registry[workflow_id] = workflow_data
            self._save_data()
            
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
                
        # 验证步骤格式
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
        """(self, url:从URL导入"""
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
        
        # 更新导入次数
        link.import_count += 1
        self._save_data()
        
        # 导入工作流数据
        workflow_data = link.workflow_data.copy()
        workflow_id = workflow_data.get("workflow_id", f"wf_{int(time.time())}")
        
        self.workflow_registry[workflow_id] = workflow_data
        self._save_data()
        
        return ImportReport(
            result=ImportResult.SUCCESS,
            workflow_id=workflow_id,
            workflow_name=workflow_data.get("name", "未命名"),
            message=f"从分享链接导入成功 (链接已使用 {link.import_count} 次)"
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


def create_share_system(data_dir: str = "./data") -> WorkflowShareSystem:
    """创建分享系统实例"""
    return WorkflowShareSystem(data_dir)


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
    
    # 创建分享链接
    link = share.create_share_link(wf_id, ShareType.PUBLIC, expires_in_days=7)
    if link:
        print(f"\n分享链接: {share.generate_share_url(link.link_id)}")
        print(f"链接ID: {link.link_id}")
        
        # 导出为Base64
        b64 = share.export_to_base64(wf_id)
        print(f"\nBase64导出: {b64[:50]}...")
        
        # 测试导入
        report = share.import_to_json(b64, "base64")
        print(f"\n导入报告: {report.result.value} - {report.message}")


# 修正测试代码
def test_import():
    share = create_share_system("./data")
    
    # 导出
    b64 = share.export_to_base64("wf_email_morning")
    if b64:
        # 导入
        report = share.import_workflow(b64, "base64")
        print(f"导入报告: {report.result.value} - {report.message}")


if __name__ == "__main__":
    test_import()
