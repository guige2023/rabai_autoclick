"""
工作流导入导出系统 v24
支持JSON/YAML/BINARY格式，云端导入，工作流打包，加密签名，版本迁移
"""
import json
import hashlib
import base64
import time
import zlib
import struct
import uuid
import os
import tarfile
import tempfile
import shutil
from typing import Dict, List, Optional, Any, Set, Callable
from dataclasses import dataclass, field, asdict
from enum import Enum
from urllib.parse import urlparse
import io


class ExportFormat(Enum):
    """导出格式"""
    JSON = "json"
    YAML = "yaml"
    BINARY = "binary"  # .rabai
    BUNDLE = "bundle"  # 多工作流+资源


class ImportSource(Enum):
    """导入来源"""
    LOCAL = "local"
    HTTP = "http"
    S3 = "s3"


class ValidationLevel(Enum):
    """验证级别"""
    STRICT = "strict"
    LENIENT = "lenient"
    MINIMAL = "minimal"


@dataclass
class WorkflowStep:
    """工作流步骤"""
    step_id: str
    action: str
    params: Dict[str, Any] = field(default_factory=dict)
    conditions: List[Dict] = field(default_factory=list)
    timeout: float = 30.0
    retry: int = 0


@dataclass
class Workflow:
    """工作流"""
    workflow_id: str
    name: str
    description: str
    version: str = "24.0.0"
    steps: List[WorkflowStep] = field(default_factory=list)
    triggers: List[Dict] = field(default_factory=list)
    settings: Dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "name": self.name,
            "description": self.description,
            "version": self.version,
            "steps": [s.__dict__ if isinstance(s, WorkflowStep) else s for s in self.steps],
            "triggers": self.triggers,
            "settings": self.settings,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "metadata": self.metadata
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Workflow":
        steps = []
        for s in data.get("steps", []):
            if isinstance(s, WorkflowStep):
                steps.append(s)
            else:
                steps.append(WorkflowStep(**s) if isinstance(s, dict) else s)
        return cls(
            workflow_id=data["workflow_id"],
            name=data["name"],
            description=data.get("description", ""),
            version=data.get("version", "24.0.0"),
            steps=steps,
            triggers=data.get("triggers", []),
            settings=data.get("settings", {}),
            created_at=data.get("created_at", time.time()),
            updated_at=data.get("updated_at", time.time()),
            metadata=data.get("metadata", {})
        )


@dataclass
class ExportMetadata:
    """导出元数据"""
    export_id: str
    format: ExportFormat
    exported_at: float
    version: str
    checksum: str
    signature: Optional[str] = None
    encrypted: bool = False
    original_version: Optional[str] = None
    migration_notes: List[str] = field(default_factory=list)


@dataclass
class ImportValidationResult:
    """导入验证结果"""
    valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    migration_performed: bool = False
    migration_steps: List[str] = field(default_factory=list)


@dataclass
class BundleManifest:
    """打包清单"""
    bundle_id: str
    name: str
    workflows: List[Dict[str, Any]] = field(default_factory=list)
    assets: Dict[str, str] = field(default_factory=dict)  # name -> path
    created_at: float = field(default_factory=time.time)
    version: str = "24.0.0"
    metadata: Dict[str, Any] = field(default_factory=dict)


class WorkflowImportExport:
    """
    工作流导入导出系统

    支持功能:
    1. JSON导入/导出 - 完整保真JSON往返
    2. YAML导入/导出 - 完整保真YAML往返
    3. 二进制导出 - 压缩二进制格式(.rabai)
    4. 云端导入 - 直接从S3/HTTP导入
    5. 工作流打包 - 将多个工作流+资源打包为单一归档
    6. 加密 - 使用密码加密导出的工作流
    7. 数字签名 - 签名导出以验证真实性
    8. 版本迁移 - 自动迁移旧版本工作流格式
    9. 部分导出 - 仅导出选定的步骤子集
    10. 模板提取 - 将任何工作流提取为可重用模板
    11. 批量导出 - 导出所有工作流到目录
    12. 导入验证 - 导入前验证
    """

    CURRENT_VERSION = "24.0.0"
    SIGNING_KEY = "rabai_secret_key"  # 实际应使用更安全的密钥管理

    def __init__(self, data_dir: str = "./data"):
        self.data_dir = data_dir
        self.workflows: Dict[str, Workflow] = {}
        self._ensure_data_dir()

    def _ensure_data_dir(self):
        """确保数据目录存在"""
        os.makedirs(self.data_dir, exist_ok=True)

    # ========== 核心哈希和签名工具 ==========

    def _compute_checksum(self, data: bytes) -> str:
        """计算数据校验和"""
        return hashlib.sha256(data).hexdigest()

    def _sign_data(self, data: bytes) -> str:
        """对数据进行数字签名"""
        h = hashlib.sha256(data + self.SIGNING_KEY.encode())
        return base64.b64encode(h.digest()).decode()

    def _verify_signature(self, data: bytes, signature: str) -> bool:
        """验证数字签名"""
        expected = self._sign_data(data)
        return expected == signature

    # ========== 1. JSON 导入/导出 ==========

    def export_to_json(self, workflow: Workflow, pretty: bool = True) -> str:
        """导出工作流到JSON格式"""
        data = workflow.to_dict()
        data["_metadata"] = {
            "exported_at": time.time(),
            "format": ExportFormat.JSON.value,
            "version": self.CURRENT_VERSION,
            "checksum": self._compute_checksum(json.dumps(data, ensure_ascii=False).encode())
        }
        indent = 2 if pretty else None
        return json.dumps(data, ensure_ascii=False, indent=indent)

    def import_from_json(self, json_str: str, validate: bool = True) -> Workflow:
        """从JSON导入工作流"""
        if validate:
            validation = self.validate_import(json_str, ExportFormat.JSON)
            if not validation.valid:
                raise ValueError(f"Validation failed: {validation.errors}")

        data = json.loads(json_str)
        # 移除元数据
        if "_metadata" in data:
            del data["_metadata"]
        return self._migrate_and_create_workflow(data)

    def _migrate_and_create_workflow(self, data: Dict[str, Any]) -> Workflow:
        """迁移并创建工作流"""
        data = self._migrate_workflow_data(data)
        return Workflow.from_dict(data)

    # ========== 2. YAML 导入/导出 ==========

    def export_to_yaml(self, workflow: Workflow, yaml_path: str) -> None:
        """导出工作流到YAML文件"""
        try:
            import yaml
        except ImportError:
            # 如果没有pyyaml，使用JSON作为后备
            json_str = self.export_to_json(workflow)
            with open(yaml_path.replace('.yaml', '.json'), 'w', encoding='utf-8') as f:
                f.write(json_str)
            return

        data = workflow.to_dict()
        data["_metadata"] = {
            "exported_at": time.time(),
            "format": ExportFormat.YAML.value,
            "version": self.CURRENT_VERSION
        }
        with open(yaml_path, 'w', encoding='utf-8') as f:
            yaml.dump(data, f, allow_unicode=True, default_flow_style=False)

    def import_from_yaml(self, yaml_path: str, validate: bool = True) -> Workflow:
        """从YAML文件导入工作流"""
        try:
            import yaml
        except ImportError:
            raise ImportError("PyYAML not installed. Install with: pip install pyyaml")

        with open(yaml_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)

        if validate:
            validation = self.validate_import(data, ExportFormat.YAML)
            if not validation.valid:
                raise ValueError(f"Validation failed: {validation.errors}")

        if "_metadata" in data:
            del data["_metadata"]
        return self._migrate_and_create_workflow(data)

    # ========== 3. 二进制导出 (.rabai) ==========

    def export_to_binary(self, workflow: Workflow, output_path: str,
                         encrypt_password: Optional[str] = None) -> None:
        """导出工作流到压缩二进制格式(.rabai)"""
        data = workflow.to_dict()
        json_str = json.dumps(data, ensure_ascii=False)

        # 压缩
        compressed = zlib.compress(json_str.encode(), level=9)

        # 可选加密
        if encrypt_password:
            compressed = self._encrypt_data(compressed, encrypt_password)

        # 签名
        checksum = self._compute_checksum(compressed)
        signature = self._sign_data(compressed)

        # 写入二进制格式
        with open(output_path, 'wb') as f:
            # 魔数和版本
            f.write(b'RBAI')  # 魔数
            f.write(struct.pack('>H', 1))  # 版本
            # 标志
            flags = 0
            if encrypt_password:
                flags |= 1
            f.write(struct.pack('B', flags))
            # 校验和 (32字节)
            f.write(checksum.encode())
            # 签名 (44字节)
            f.write(signature.encode().ljust(44, b'\0'))
            # 数据长度 + 数据
            f.write(struct.pack('>I', len(compressed)))
            f.write(compressed)

    def import_from_binary(self, binary_path: str,
                           decrypt_password: Optional[str] = None) -> Workflow:
        """从二进制文件导入工作流"""
        with open(binary_path, 'rb') as f:
            # 读取魔数
            magic = f.read(4)
            if magic != b'RBAI':
                raise ValueError("Invalid .rabai file: bad magic number")

            # 读取版本
            version = struct.unpack('>H', f.read(2))[0]

            # 读取标志
            flags = struct.unpack('B', f.read(1))[0]
            encrypted = bool(flags & 1)

            # 读取校验和
            checksum = f.read(32).decode()

            # 读取签名
            signature = f.read(44).rstrip(b'\0').decode()

            # 读取数据
            data_len = struct.unpack('>I', f.read(4))[0]
            compressed = f.read(data_len)

        # 验证校验和
        actual_checksum = self._compute_checksum(compressed)
        if actual_checksum != checksum:
            raise ValueError("Checksum mismatch: file may be corrupted")

        # 验证签名
        if not self._verify_signature(compressed, signature):
            raise ValueError("Signature verification failed: file may be tampered")

        # 解密
        if encrypted:
            if not decrypt_password:
                raise ValueError("File is encrypted, password required")
            compressed = self._decrypt_data(compressed, decrypt_password)

        # 解压
        json_str = zlib.decompress(compressed)
        data = json.loads(json_str)

        return self._migrate_and_create_workflow(data)

    # ========== 4. 云端导入 (S3, HTTP) ==========

    def import_from_url(self, url: str, timeout: int = 30) -> Workflow:
        """从云URL导入工作流 (S3, HTTP)"""
        parsed = urlparse(url)

        if parsed.scheme == 's3':
            return self._import_from_s3(url, timeout)
        elif parsed.scheme in ('http', 'https'):
            return self._import_from_http(url, timeout)
        else:
            raise ValueError(f"Unsupported URL scheme: {parsed.scheme}")

    def _import_from_s3(self, s3_url: str, timeout: int) -> Workflow:
        """从S3导入"""
        try:
            import boto3
        except ImportError:
            raise ImportError("boto3 not installed. Install with: pip install boto3")

        # 解析 s3://bucket/key 格式
        parsed = urlparse(s3_url)
        bucket = parsed.netloc
        key = parsed.path.lstrip('/')

        # 使用boto3下载
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket, Key=key, ResponseTimeout=timeout)
        content = response['Body'].read()

        return self._import_from_bytes(content, s3_url)

    def _import_from_http(self, http_url: str, timeout: int) -> Workflow:
        """从HTTP导入"""
        import urllib.request

        req = urllib.request.Request(http_url, headers={'User-Agent': 'Rabai/24.0'})
        with urllib.request.urlopen(req, timeout=timeout) as response:
            content = response.read()

        return self._import_from_bytes(content, http_url)

    def _import_from_bytes(self, content: bytes, source: str) -> Workflow:
        """从字节内容导入"""
        # 检测格式
        if content[:4] == b'RBAI':
            return self.import_from_binary_bytes(content)
        elif content[:1] == b'{':
            return self._migrate_and_create_workflow(json.loads(content.decode('utf-8')))
        else:
            try:
                import yaml
                data = yaml.safe_load(content)
                return self._migrate_and_create_workflow(data)
            except:
                raise ValueError(f"Unknown content format from: {source}")

    def import_from_binary_bytes(self, data: bytes) -> Workflow:
        """从字节数据导入二进制工作流"""
        # 简化版：直接解析
        io_buf = io.BytesIO(data)
        io_buf.read(4)  # 魔数
        io_buf.read(2)  # 版本
        flags = struct.unpack('B', io_buf.read(1))[0]
        io_buf.read(32)  # checksum
        io_buf.read(44)  # signature
        data_len = struct.unpack('>I', io_buf.read(4))[0]
        compressed = io_buf.read(data_len)

        if flags & 1:
            raise ValueError("Encrypted binary import from bytes not supported")

        json_str = zlib.decompress(compressed)
        return self._migrate_and_create_workflow(json.loads(json_str))

    # ========== 5. 工作流打包 ==========

    def create_bundle(self, workflow_ids: List[str], bundle_name: str,
                      assets: Optional[Dict[str, str]] = None) -> BundleManifest:
        """创建工作流打包"""
        manifest = BundleManifest(
            bundle_id=str(uuid.uuid4()),
            name=bundle_name,
            created_at=time.time()
        )

        for wid in workflow_ids:
            if wid in self.workflows:
                w = self.workflows[wid]
                manifest.workflows.append(w.to_dict())

        if assets:
            manifest.assets = assets

        return manifest

    def export_bundle(self, manifest: BundleManifest, output_path: str) -> None:
        """导出打包文件"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # 写入清单
            manifest_path = os.path.join(tmpdir, "manifest.json")
            with open(manifest_path, 'w', encoding='utf-8') as f:
                json.dump(asdict(manifest), f, ensure_ascii=False, indent=2)

            # 复制资源文件
            assets_dir = os.path.join(tmpdir, "assets")
            os.makedirs(assets_dir, exist_ok=True)
            for name, path in manifest.assets.items():
                if os.path.exists(path):
                    shutil.copy(path, os.path.join(assets_dir, name))

            # 创建tar归档
            with tarfile.open(output_path, 'w:gz') as tar:
                tar.add(tmpdir, arcname='')

    def import_bundle(self, bundle_path: str) -> BundleManifest:
        """导入打包文件"""
        with tempfile.TemporaryDirectory() as tmpdir:
            with tarfile.open(bundle_path, 'r:gz') as tar:
                tar.extractall(tmpdir)

            # 读取清单
            manifest_path = os.path.join(tmpdir, "manifest.json")
            with open(manifest_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # 迁移每个工作流
            workflows = []
            for wdata in data.get("workflows", []):
                w = self._migrate_and_create_workflow(wdata)
                workflows.append(w.to_dict())
                self.workflows[w.workflow_id] = w

            data["workflows"] = workflows
            return BundleManifest(**data)

    # ========== 6. 加密功能 ==========

    def _encrypt_data(self, data: bytes, password: str) -> bytes:
        """加密数据"""
        from cryptography.fernet import Fernet
        import hashlib

        # 从密码派生密钥
        key_base = hashlib.sha256(password.encode()).digest()
        key = base64.urlsafe_b64encode(key_base)
        f = Fernet(key)
        return f.encrypt(data)

    def _decrypt_data(self, data: bytes, password: str) -> bytes:
        """解密数据"""
        from cryptography.fernet import Fernet
        import hashlib

        key_base = hashlib.sha256(password.encode()).digest()
        key = base64.urlsafe_b64encode(key_base)
        f = Fernet(key)
        return f.decrypt(data)

    def encrypt_workflow(self, workflow: Workflow, password: str) -> bytes:
        """加密工作流"""
        data = json.dumps(workflow.to_dict(), ensure_ascii=False).encode()
        return self._encrypt_data(data, password)

    def decrypt_workflow(self, encrypted_data: bytes, password: str) -> Workflow:
        """解密工作流"""
        data = self._decrypt_data(encrypted_data, password)
        return self._migrate_and_create_workflow(json.loads(data))

    # ========== 7. 数字签名 ==========

    def sign_workflow(self, workflow: Workflow) -> Dict[str, Any]:
        """签名工作流"""
        data = json.dumps(workflow.to_dict(), ensure_ascii=False).encode()
        signature = self._sign_data(data)
        checksum = self._compute_checksum(data)

        return {
            "workflow": workflow.to_dict(),
            "signature": signature,
            "checksum": checksum,
            "signed_at": time.time(),
            "version": self.CURRENT_VERSION
        }

    def verify_signed_workflow(self, signed_data: Dict[str, Any]) -> bool:
        """验证签名工作流"""
        workflow_data = signed_data.get("workflow", {})
        signature = signed_data.get("signature", "")
        checksum = signed_data.get("checksum", "")

        data = json.dumps(workflow_data, ensure_ascii=False).encode()

        # 验证校验和
        if self._compute_checksum(data) != checksum:
            return False

        # 验证签名
        return self._verify_signature(data, signature)

    # ========== 8. 版本迁移 ==========

    VERSION_MIGRATIONS = {
        "1.0.0": None,  # 初始版本无需迁移
        "2.0.0": "_migrate_v2_to_v3",
        "20.0.0": "_migrate_v20_to_v21",
        "21.0.0": "_migrate_v21_to_v22",
        "23.0.0": "_migrate_v23_to_v24",
    }

    def _migrate_workflow_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """迁移工作流数据到当前版本"""
        source_version = data.get("version", "1.0.0")

        if source_version == self.CURRENT_VERSION:
            return data

        migration_steps = []
        current = source_version

        while current != self.CURRENT_VERSION:
            next_version = self._get_next_version(current)
            if not next_version:
                raise ValueError(f"Cannot migrate from version {current}")

            migrator_name = self.VERSION_MIGRATIONS.get(current)
            if migrator_name:
                migrator = getattr(self, migrator_name)
                data = migrator(data)
                migration_steps.append(f"{current} -> {next_version}")
            current = next_version

        data["version"] = self.CURRENT_VERSION
        data["_migration_steps"] = migration_steps
        return data

    def _get_next_version(self, current: str) -> Optional[str]:
        """获取下一个版本"""
        order = ["1.0.0", "2.0.0", "20.0.0", "21.0.0", "22.0.0", "23.0.0", "24.0.0"]
        try:
            idx = order.index(current)
            return order[idx + 1] if idx + 1 < len(order) else None
        except ValueError:
            return None

    def _migrate_v2_to_v3(self, data: Dict) -> Dict:
        """迁移 v2 -> v3"""
        # 示例：添加新字段
        if "settings" not in data:
            data["settings"] = {}
        if "timeout" not in data["settings"]:
            data["settings"]["timeout"] = 30
        return data

    def _migrate_v20_to_v21(self, data: Dict) -> Dict:
        """迁移 v20 -> v21"""
        # 添加新元数据字段
        if "metadata" not in data:
            data["metadata"] = {}
        data["metadata"]["migrated_from_v20"] = True
        return data

    def _migrate_v21_to_v22(self, data: Dict) -> Dict:
        """迁移 v21 -> v22"""
        if "triggers" not in data:
            data["triggers"] = []
        return data

    def _migrate_v23_to_v24(self, data: Dict) -> Dict:
        """迁移 v23 -> v24"""
        # 当前版本迁移逻辑
        if "steps" in data:
            for step in data["steps"]:
                if isinstance(step, dict) and "timeout" not in step:
                    step["timeout"] = 30.0
                if isinstance(step, dict) and "retry" not in step:
                    step["retry"] = 0
        return data

    def auto_migrate(self, data: Dict[str, Any]) -> tuple[Dict[str, Any], List[str]]:
        """自动迁移数据，返回迁移后的数据和步骤列表"""
        original_version = data.get("version", "1.0.0")
        migrated = self._migrate_workflow_data(data.copy())
        steps = migrated.pop("_migration_steps", [])
        return migrated, steps

    # ========== 9. 部分导出 ==========

    def export_partial(self, workflow: Workflow, step_ids: List[str],
                       include_conditions: bool = True) -> Workflow:
        """导出部分工作流（仅包含指定的步骤）"""
        if not step_ids:
            raise ValueError("step_ids cannot be empty")

        # 过滤步骤
        selected_steps = []
        for step in workflow.steps:
            if isinstance(step, WorkflowStep) and step.step_id in step_ids:
                selected_steps.append(step)
            elif isinstance(step, dict) and step.get("step_id") in step_ids:
                selected_steps.append(step)

        # 创建部分工作流
        partial = Workflow(
            workflow_id=workflow.workflow_id + "_partial",
            name=workflow.name + " (Partial)",
            description=f"Partial export of {workflow.name} with steps: {', '.join(step_ids)}",
            version=workflow.version,
            steps=selected_steps,
            triggers=workflow.triggers if include_conditions else [],
            settings=workflow.settings.copy(),
            metadata={**workflow.metadata, "partial_export": True, "selected_steps": step_ids}
        )

        return partial

    def export_steps_range(self, workflow: Workflow, start_idx: int,
                           end_idx: int) -> Workflow:
        """导出步骤范围内的部分工作流"""
        if start_idx < 0 or end_idx >= len(workflow.steps):
            raise ValueError("Index out of range")

        step_ids = []
        for i in range(start_idx, end_idx + 1):
            step = workflow.steps[i]
            sid = step.step_id if isinstance(step, WorkflowStep) else step.get("step_id")
            step_ids.append(sid)

        return self.export_partial(workflow, step_ids)

    # ========== 10. 模板提取 ==========

    def extract_template(self, workflow: Workflow,
                         template_name: str,
                         clear_sensitive: bool = True) -> Workflow:
        """提取工作流为可重用模板"""
        template = Workflow(
            workflow_id=f"template_{uuid.uuid4().hex[:8]}",
            name=template_name,
            description=workflow.description,
            version=self.CURRENT_VERSION,
            steps=workflow.steps.copy(),
            triggers=[],  # 模板不包含触发器
            settings=workflow.settings.copy(),
            metadata={
                "is_template": True,
                "template_of": workflow.workflow_id,
                "template_of_name": workflow.name,
                "extracted_at": time.time()
            }
        )

        if clear_sensitive:
            # 清除敏感信息
            for step in template.steps:
                if isinstance(step, WorkflowStep):
                    # 清除包含password、token等字段
                    sensitive_keys = ["password", "token", "api_key", "secret", "credential"]
                    for key in list(step.params.keys()):
                        if any(s in key.lower() for s in sensitive_keys):
                            step.params[key] = "***REDACTED***"
                elif isinstance(step, dict):
                    sensitive_keys = ["password", "token", "api_key", "secret", "credential"]
                    for key in list(step.get("params", {}).keys()):
                        if any(s in key.lower() for s in sensitive_keys):
                            step["params"][key] = "***REDACTED***"

        return template

    def create_template_from_steps(self, step_definitions: List[Dict],
                                   template_name: str) -> Workflow:
        """从步骤定义创建模板"""
        steps = []
        for i, step_def in enumerate(step_definitions):
            step = WorkflowStep(
                step_id=f"step_{i+1}",
                action=step_def.get("action", "unknown"),
                params=step_def.get("params", {}),
                conditions=step_def.get("conditions", []),
                timeout=step_def.get("timeout", 30.0),
                retry=step_def.get("retry", 0)
            )
            steps.append(step)

        return Workflow(
            workflow_id=f"template_{uuid.uuid4().hex[:8]}",
            name=template_name,
            description=f"Template: {template_name}",
            version=self.CURRENT_VERSION,
            steps=steps,
            triggers=[],
            metadata={"is_template": True, "created_from_definitions": True}
        )

    # ========== 11. 批量导出 ==========

    def batch_export(self, output_dir: str,
                    format: ExportFormat = ExportFormat.JSON,
                    encrypt_password: Optional[str] = None) -> Dict[str, str]:
        """批量导出所有工作流到目录"""
        os.makedirs(output_dir, exist_ok=True)
        results = {}

        for wid, workflow in self.workflows.items():
            try:
                if format == ExportFormat.JSON:
                    filename = f"{workflow.name}_{wid}.json"
                    filepath = os.path.join(output_dir, filename)
                    with open(filepath, 'w', encoding='utf-8') as f:
                        f.write(self.export_to_json(workflow))

                elif format == ExportFormat.YAML:
                    filename = f"{workflow.name}_{wid}.yaml"
                    filepath = os.path.join(output_dir, filename)
                    self.export_to_yaml(workflow, filepath)

                elif format == ExportFormat.BINARY:
                    filename = f"{workflow.name}_{wid}.rabai"
                    filepath = os.path.join(output_dir, filename)
                    self.export_to_binary(workflow, filepath, encrypt_password)

                results[wid] = filepath

            except Exception as e:
                results[wid] = f"ERROR: {str(e)}"

        return results

    def batch_export_by_tags(self, tags: List[str], output_dir: str,
                            format: ExportFormat = ExportFormat.JSON) -> Dict[str, str]:
        """按标签批量导出"""
        tagged_ids = []
        for wid, workflow in self.workflows.items():
            workflow_tags = workflow.metadata.get("tags", [])
            if any(t in workflow_tags for t in tags):
                tagged_ids.append(wid)

        # 临时替换workflows
        original = self.workflows.copy()
        self.workflows = {k: v for k, v in self.workflows.items() if k in tagged_ids}
        results = self.batch_export(output_dir, format)
        self.workflows = original
        return results

    # ========== 12. 导入验证 ==========

    def validate_import(self, data: Any, format: ExportFormat) -> ImportValidationResult:
        """验证导入数据"""
        errors = []
        warnings = []
        migration_performed = False
        migration_steps = []

        # 基本结构检查
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except json.JSONDecodeError as e:
                errors.append(f"Invalid JSON: {e}")
                return ImportValidationResult(False, errors, warnings)

        if not isinstance(data, dict):
            errors.append("Data must be a dictionary")
            return ImportValidationResult(False, errors, warnings)

        # 必需字段检查
        required_fields = ["workflow_id", "name", "steps"]
        for field in required_fields:
            if field not in data:
                errors.append(f"Missing required field: {field}")

        # 版本检查和迁移
        version = data.get("version", "1.0.0")
        if version != self.CURRENT_VERSION:
            try:
                migrated_data, migration_steps = self.auto_migrate(data)
                migration_performed = True
                warnings.append(f"Migrated from {version} to {self.CURRENT_VERSION}")
            except Exception as e:
                errors.append(f"Migration failed: {e}")

        # 步骤验证
        steps = data.get("steps", [])
        if not isinstance(steps, list):
            errors.append("steps must be a list")
        else:
            for i, step in enumerate(steps):
                if not isinstance(step, dict):
                    warnings.append(f"Step {i} is not a dictionary")
                    continue
                if "step_id" not in step:
                    warnings.append(f"Step {i} missing step_id")
                if "action" not in step:
                    errors.append(f"Step {i} missing action")

        # 设置验证
        settings = data.get("settings", {})
        if not isinstance(settings, dict):
            errors.append("settings must be a dictionary")

        valid = len(errors) == 0
        return ImportValidationResult(valid, errors, warnings, migration_performed, migration_steps)

    def validate_batch_import(self, items: List[Any]) -> List[ImportValidationResult]:
        """批量验证导入项"""
        return [self.validate_import(item, ExportFormat.JSON) for item in items]

    def validate_workflow(self, workflow: Workflow,
                         level: ValidationLevel = ValidationLevel.STRICT) -> ImportValidationResult:
        """验证工作流对象"""
        data = workflow.to_dict()
        result = self.validate_import(data, ExportFormat.JSON)

        if level == ValidationLevel.MINIMAL:
            # 只检查必需字段
            result.errors = [e for e in result.errors if "missing" in e.lower()]

        elif level == ValidationLevel.LENIENT:
            # 将某些错误降级为警告
            new_errors = []
            for e in result.errors:
                if "missing" in e.lower() and "optional" not in e.lower():
                    result.warnings.append(e)
                else:
                    new_errors.append(e)
            result.errors = new_errors

        result.valid = len(result.errors) == 0
        return result

    # ========== 辅助方法 ==========

    def load_workflow(self, workflow_id: str) -> Optional[Workflow]:
        """加载工作流"""
        return self.workflows.get(workflow_id)

    def save_workflow(self, workflow: Workflow) -> None:
        """保存工作流"""
        self.workflows[workflow.workflow_id] = workflow

    def delete_workflow(self, workflow_id: str) -> bool:
        """删除工作流"""
        if workflow_id in self.workflows:
            del self.workflows[workflow_id]
            return True
        return False

    def list_workflows(self) -> List[Dict[str, Any]]:
        """列出所有工作流"""
        return [
            {
                "workflow_id": w.workflow_id,
                "name": w.name,
                "version": w.version,
                "step_count": len(w.steps)
            }
            for w in self.workflows.values()
        ]
