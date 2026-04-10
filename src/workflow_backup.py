"""
Workflow Backup and Restore System

A comprehensive backup and restore system for workflows with:
- Incremental and full backup
- Backup rotation (daily, weekly, monthly)
- Checksum verification
- AES-256 encryption
- Remote backup (S3, FTP, SCP)
- Disaster recovery with point-in-time restore
- Automatic scheduling and monitoring
"""

import os
import json
import hashlib
import shutil
import tarfile
import gzip
import zipfile
import logging
import threading
import time
import re
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Callable
from dataclasses import dataclass, field, asdict
from enum import Enum
from abc import ABC, abstractmethod
import schedule
import asyncio
import aiofiles

# Optional imports with graceful fallbacks
try:
    import boto3
    from botocore.exceptions import ClientError
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

try:
    import paramiko
    HAS_PARAMIKO = True
except ImportError:
    HAS_PARAMIKO = False

try:
    from cryptography.fernet import Fernet
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    HAS_CRYPTO = True
except ImportError:
    HAS_CRYPTO = False

try:
    import ftplib
    HAS_FTP = True
except ImportError:
    HAS_FTP = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BackupType(Enum):
    """Backup type enumeration"""
    FULL = "full"
    INCREMENTAL = "incremental"
    DIFFERENTIAL = "differential"


class BackupStatus(Enum):
    """Backup status enumeration"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    VERIFIED = "verified"


class RetentionPolicy(Enum):
    """Retention policy for backup rotation"""
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


@dataclass
class BackupMetadata:
    """Metadata for a backup"""
    backup_id: str
    backup_type: BackupType
    created_at: datetime
    workflow_ids: List[str]
    checksum: str
    encrypted: bool
    size_bytes: int
    retention_policy: RetentionPolicy
    version: str
    parent_backup_id: Optional[str] = None
    expires_at: Optional[datetime] = None
    tags: Dict[str, str] = field(default_factory=dict)
    status: BackupStatus = BackupStatus.PENDING
    error_message: Optional[str] = None

    def to_dict(self) -> Dict:
        data = asdict(self)
        data['backup_type'] = self.backup_type.value
        data['retention_policy'] = self.retention_policy.value
        data['status'] = self.status.value
        data['created_at'] = self.created_at.isoformat()
        if self.expires_at:
            data['expires_at'] = self.expires_at.isoformat()
        return data

    @classmethod
    def from_dict(cls, data: Dict) -> 'BackupMetadata':
        data['backup_type'] = BackupType(data['backup_type'])
        data['retention_policy'] = RetentionPolicy(data['retention_policy'])
        data['status'] = BackupStatus(data['status'])
        data['created_at'] = datetime.fromisoformat(data['created_at'])
        if data.get('expires_at'):
            data['expires_at'] = datetime.fromisoformat(data['expires_at'])
        return cls(**data)


@dataclass
class WorkflowSnapshot:
    """Snapshot of a workflow at a point in time"""
    workflow_id: str
    workflow_data: Dict[str, Any]
    checksum: str
    modified_at: datetime
    version: str


@dataclass
class BackupCatalog:
    """Catalog of all backups"""
    backups: List[BackupMetadata] = field(default_factory=list)
    workflows_last_backup: Dict[str, datetime] = field(default_factory=dict)
    workflows_checksums: Dict[str, str] = field(default_factory=dict)

    def add_backup(self, metadata: BackupMetadata):
        self.backups.append(metadata)
        self._update_workflow_tracking(metadata)

    def _update_workflow_tracking(self, metadata: BackupMetadata):
        for wf_id in metadata.workflow_ids:
            self.workflows_last_backup[wf_id] = metadata.created_at
            self.workflows_checksums[wf_id] = metadata.checksum

    def get_backup(self, backup_id: str) -> Optional[BackupMetadata]:
        for backup in self.backups:
            if backup.backup_id == backup_id:
                return backup
        return None

    def get_backups_by_type(self, backup_type: BackupType) -> List[BackupMetadata]:
        return [b for b in self.backups if b.backup_type == backup_type]

    def get_backups_by_policy(self, policy: RetentionPolicy) -> List[BackupMetadata]:
        return [b for b in self.backups if b.retention_policy == policy]

    def to_dict(self) -> Dict:
        return {
            'backups': [b.to_dict() for b in self.backups],
            'workflows_last_backup': {k: v.isoformat() for k, v in self.workflows_last_backup.items()},
            'workflows_checksums': self.workflows_checksums
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'BackupCatalog':
        cat = cls()
        cat.backups = [BackupMetadata.from_dict(b) for b in data.get('backups', [])]
        cat.workflows_last_backup = {
            k: datetime.fromisoformat(v) for k, v in data.get('workflows_last_backup', {}).items()
        }
        cat.workflows_checksums = data.get('workflows_checksums', {})
        return cat


class RemoteStorageBackend(ABC):
    """Abstract base class for remote storage backends"""

    @abstractmethod
    def connect(self) -> bool:
        pass

    @abstractmethod
    def disconnect(self):
        pass

    @abstractmethod
    def upload(self, local_path: str, remote_path: str) -> bool:
        pass

    @abstractmethod
    def download(self, remote_path: str, local_path: str) -> bool:
        pass

    @abstractmethod
    def list_backups(self, prefix: str = "") -> List[str]:
        pass

    @abstractmethod
    def delete(self, remote_path: str) -> bool:
        pass


class S3Backend(RemoteStorageBackend):
    """Amazon S3 storage backend"""

    def __init__(self, bucket: str, access_key: str = None, secret_key: str = None,
                 endpoint_url: str = None, region: str = "us-east-1"):
        if not HAS_BOTO3:
            raise ImportError("boto3 is required for S3 support. Install with: pip install boto3")

        self.bucket = bucket
        self.region = region
        self.access_key = access_key or os.environ.get('AWS_ACCESS_KEY_ID')
        self.secret_key = secret_key or os.environ.get('AWS_SECRET_ACCESS_KEY')
        self.endpoint_url = endpoint_url or os.environ.get('AWS_ENDPOINT_URL')

        self.s3_client = None
        self._connected = False

    def connect(self) -> bool:
        try:
            session = boto3.Session(
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region
            )
            self.s3_client = session.client('s3', endpoint_url=self.endpoint_url)
            self._connected = True
            logger.info(f"Connected to S3 bucket: {self.bucket}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to S3: {e}")
            return False

    def disconnect(self):
        self.s3_client = None
        self._connected = False
        logger.info("Disconnected from S3")

    def upload(self, local_path: str, remote_path: str) -> bool:
        if not self._connected:
            self.connect()
        try:
            self.s3_client.upload_file(local_path, self.bucket, remote_path)
            logger.info(f"Uploaded {local_path} to s3://{self.bucket}/{remote_path}")
            return True
        except ClientError as e:
            logger.error(f"Failed to upload to S3: {e}")
            return False

    def download(self, remote_path: str, local_path: str) -> bool:
        if not self._connected:
            self.connect()
        try:
            self.s3_client.download_file(self.bucket, remote_path, local_path)
            logger.info(f"Downloaded s3://{self.bucket}/{remote_path} to {local_path}")
            return True
        except ClientError as e:
            logger.error(f"Failed to download from S3: {e}")
            return False

    def list_backups(self, prefix: str = "") -> List[str]:
        if not self._connected:
            self.connect()
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            return [obj['Key'] for obj in response.get('Contents', [])]
        except ClientError as e:
            logger.error(f"Failed to list S3 objects: {e}")
            return []

    def delete(self, remote_path: str) -> bool:
        if not self._connected:
            self.connect()
        try:
            self.s3_client.delete_object(Bucket=self.bucket, Key=remote_path)
            logger.info(f"Deleted s3://{self.bucket}/{remote_path}")
            return True
        except ClientError as e:
            logger.error(f"Failed to delete from S3: {e}")
            return False


class FTPBackend(RemoteStorageBackend):
    """FTP storage backend"""

    def __init__(self, host: str, port: int = 21, username: str = None,
                 password: str = None, base_dir: str = "/"):
        if not HAS_FTP:
            raise ImportError("ftplib is required for FTP support")

        self.host = host
        self.port = port
        self.username = username or os.environ.get('FTP_USERNAME', 'anonymous')
        self.password = password or os.environ.get('FTP_PASSWORD', '')
        self.base_dir = base_dir
        self.ftp = None
        self._connected = False

    def connect(self) -> bool:
        try:
            self.ftp = ftplib.FTP()
            self.ftp.connect(self.host, self.port)
            self.ftp.login(self.username, self.password)
            if self.base_dir:
                self.ftp.cwd(self.base_dir)
            self._connected = True
            logger.info(f"Connected to FTP server: {self.host}:{self.port}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to FTP: {e}")
            return False

    def disconnect(self):
        if self.ftp:
            try:
                self.ftp.quit()
            except:
                self.ftp.close()
        self.ftp = None
        self._connected = False
        logger.info("Disconnected from FTP server")

    def upload(self, local_path: str, remote_path: str) -> bool:
        if not self._connected:
            self.connect()
        try:
            with open(local_path, 'rb') as f:
                self.ftp.storbinary(f"STOR {remote_path}", f)
            logger.info(f"Uploaded {local_path} to FTP://{self.host}/{remote_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to upload to FTP: {e}")
            return False

    def download(self, remote_path: str, local_path: str) -> bool:
        if not self._connected:
            self.connect()
        try:
            with open(local_path, 'wb') as f:
                self.ftp.retrbinary(f"RETR {remote_path}", f.write)
            logger.info(f"Downloaded FTP://{self.host}/{remote_path} to {local_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to download from FTP: {e}")
            return False

    def list_backups(self, prefix: str = "") -> List[str]:
        if not self._connected:
            self.connect()
        try:
            files = []
            self.ftp.retrlines(f"LIST {prefix}", files.append)
            return [line.split()[-1] for line in files if line]
        except Exception as e:
            logger.error(f"Failed to list FTP files: {e}")
            return []

    def delete(self, remote_path: str) -> bool:
        if not self._connected:
            self.connect()
        try:
            self.ftp.delete(remote_path)
            logger.info(f"Deleted FTP://{self.host}/{remote_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete from FTP: {e}")
            return False


class SCPBackend(RemoteStorageBackend):
    """SCP/SFTP storage backend"""

    def __init__(self, host: str, port: int = 22, username: str = None,
                 password: str = None, key_file: str = None, remote_path: str = "/"):
        if not HAS_PARAMIKO:
            raise ImportError("paramiko is required for SCP support. Install with: pip install paramiko")

        self.host = host
        self.port = port
        self.username = username or os.environ.get('SCP_USERNAME')
        self.password = password or os.environ.get('SCP_PASSWORD')
        self.key_file = key_file or os.environ.get('SCP_KEY_FILE')
        self.remote_path = remote_path
        self.ssh = None
        self.scp = None
        self._connected = False

    def connect(self) -> bool:
        try:
            self.ssh = paramiko.SSHClient()
            self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            connect_kwargs = {
                'hostname': self.host,
                'port': self.port,
                'username': self.username,
            }

            if self.key_file:
                connect_kwargs['key_filename'] = self.key_file
            elif self.password:
                connect_kwargs['password'] = self.password

            self.ssh.connect(**connect_kwargs)
            self.scp = self.ssh.open_sftp()
            self._connected = True
            logger.info(f"Connected to SCP server: {self.host}:{self.port}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to SCP: {e}")
            return False

    def disconnect(self):
        if self.scp:
            self.scp.close()
        if self.ssh:
            self.ssh.close()
        self.scp = None
        self.ssh = None
        self._connected = False
        logger.info("Disconnected from SCP server")

    def upload(self, local_path: str, remote_path: str) -> bool:
        if not self._connected:
            self.connect()
        try:
            remote_full_path = f"{self.remote_path}/{remote_path}"
            self.scp.put(local_path, remote_full_path)
            logger.info(f"Uploaded {local_path} to SCP://{self.host}/{remote_full_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to upload via SCP: {e}")
            return False

    def download(self, remote_path: str, local_path: str) -> bool:
        if not self._connected:
            self.connect()
        try:
            remote_full_path = f"{self.remote_path}/{remote_path}"
            self.scp.get(remote_full_path, local_path)
            logger.info(f"Downloaded SCP://{self.host}/{remote_full_path} to {local_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to download via SCP: {e}")
            return False

    def list_backups(self, prefix: str = "") -> List[str]:
        if not self._connected:
            self.connect()
        try:
            files = []
            self.scp.listdir(f"{self.remote_path}/{prefix}")
            return files
        except Exception as e:
            logger.error(f"Failed to list SCP files: {e}")
            return []

    def delete(self, remote_path: str) -> bool:
        if not self._connected:
            self.connect()
        try:
            remote_full_path = f"{self.remote_path}/{remote_path}"
            self.scp.remove(remote_full_path)
            logger.info(f"Deleted SCP://{self.host}/{remote_full_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete via SCP: {e}")
            return False


class EncryptionManager:
    """Handles AES-256 encryption for backups"""

    def __init__(self, password: str = None):
        self.password = password or os.environ.get('BACKUP_ENCRYPTION_KEY')
        self._cipher = None
        self._key = None

    def _derive_key(self, password: str) -> bytes:
        """Derive AES key from password using PBKDF2"""
        if not HAS_CRYPTO:
            raise ImportError("cryptography is required for encryption. Install with: pip install cryptography")

        salt = b'rabai_backup_salt_v1'
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        return kdf.derive(password.encode())

    def initialize(self, password: str = None):
        """Initialize the encryption manager with a password"""
        pwd = password or self.password
        if not pwd:
            raise ValueError("Encryption password is required")
        self._key = self._derive_key(pwd)

    def encrypt(self, data: bytes) -> bytes:
        """Encrypt data using AES-256-GCM"""
        if not self._key:
            self.initialize()

        if not HAS_CRYPTO:
            raise ImportError("cryptography is required for encryption")

        aesgcm = AESGCM(self._key)
        nonce = os.urandom(12)
        ciphertext = aesgcm.encrypt(nonce, data, None)
        return nonce + ciphertext

    def decrypt(self, encrypted_data: bytes) -> bytes:
        """Decrypt data using AES-256-GCM"""
        if not self._key:
            raise ValueError("Encryption not initialized. Call initialize() first.")

        if not HAS_CRYPTO:
            raise ImportError("cryptography is required for decryption")

        nonce = encrypted_data[:12]
        ciphertext = encrypted_data[12:]
        aesgcm = AESGCM(self._key)
        return aesgcm.decrypt(nonce, ciphertext, None)


class ChecksumCalculator:
    """Calculate and verify checksums for backup integrity"""

    @staticmethod
    def calculate_file_checksum(file_path: str, algorithm: str = 'sha256') -> str:
        """Calculate checksum of a file"""
        hash_obj = hashlib.new(algorithm)
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                hash_obj.update(chunk)
        return hash_obj.hexdigest()

    @staticmethod
    def calculate_data_checksum(data: bytes, algorithm: str = 'sha256') -> str:
        """Calculate checksum of data bytes"""
        hash_obj = hashlib.new(algorithm)
        hash_obj.update(data)
        return hash_obj.hexdigest()

    @staticmethod
    def verify_file_integrity(file_path: str, expected_checksum: str,
                              algorithm: str = 'sha256') -> bool:
        """Verify file integrity against expected checksum"""
        actual = ChecksumCalculator.calculate_file_checksum(file_path, algorithm)
        return actual == expected_checksum

    @staticmethod
    def calculate_manifest_checksum(manifest: Dict) -> str:
        """Calculate checksum of a backup manifest"""
        manifest_json = json.dumps(manifest, sort_keys=True)
        return ChecksumCalculator.calculate_data_checksum(manifest_json.encode())


class BackupRotationManager:
    """Manages backup rotation based on retention policies"""

    def __init__(self, retention: Dict[str, int] = None):
        """
        Initialize rotation manager

        Args:
            retention: Dict with keys 'daily', 'weekly', 'monthly' and int values
                      for number of backups to keep
        """
        self.retention = retention or {
            'daily': 7,
            'weekly': 4,
            'monthly': 12
        }

    def get_backups_to_delete(self, backups: List[BackupMetadata]) -> List[BackupMetadata]:
        """Determine which backups should be deleted based on rotation policy"""
        to_delete = []
        now = datetime.now()

        daily_backups = [b for b in backups
                        if b.retention_policy == RetentionPolicy.DAILY
                        and b.status == BackupStatus.COMPLETED]
        weekly_backups = [b for b in backups
                         if b.retention_policy == RetentionPolicy.WEEKLY
                         and b.status == BackupStatus.COMPLETED]
        monthly_backups = [b for b in backups
                          if b.retention_policy == RetentionPolicy.MONTHLY
                          and b.status == BackupStatus.COMPLETED]

        # Keep daily backups from last N days
        daily_cutoff = now - timedelta(days=self.retention['daily'])
        to_delete.extend([b for b in daily_backups if b.created_at < daily_cutoff])

        # Keep weekly backups from last N weeks
        weekly_cutoff = now - timedelta(weeks=self.retention['weekly'])
        to_delete.extend([b for b in weekly_backups if b.created_at < weekly_cutoff])

        # Keep monthly backups from last N months
        monthly_cutoff = now - timedelta(days=30 * self.retention['monthly'])
        to_delete.extend([b for b in monthly_backups if b.created_at < monthly_cutoff])

        # Remove duplicates
        seen = set()
        unique_to_delete = []
        for b in to_delete:
            if b.backup_id not in seen:
                seen.add(b.backup_id)
                unique_to_delete.append(b)

        return unique_to_delete


class WorkflowBackup:
    """
    Comprehensive backup and restore system for workflows.

    Features:
    - Incremental and full backup
    - Backup rotation with retention policies
    - Checksum verification
    - AES-256 encryption
    - Remote backup (S3, FTP, SCP)
    - Disaster recovery with point-in-time restore
    - Automatic scheduling
    - Backup monitoring with alerts
    """

    VERSION = "1.0.0"

    def __init__(self,
                 workflows_dir: str = None,
                 backup_dir: str = None,
                 catalog_path: str = None,
                 encryption_password: str = None,
                 remote_backend: RemoteStorageBackend = None,
                 retention: Dict[str, int] = None,
                 check_interval: int = 30):
        """
        Initialize the backup system.

        Args:
            workflows_dir: Directory containing workflow files
            backup_dir: Directory to store backups locally
            catalog_path: Path to the backup catalog file
            encryption_password: Password for AES-256 encryption
            remote_backend: Remote storage backend (S3, FTP, SCP)
            retention: Retention policy for backup rotation
            check_interval: Interval in seconds for disk space checks
        """
        self.workflows_dir = Path(workflows_dir or os.path.join(
            os.path.dirname(os.path.dirname(__file__)), 'workflows'))
        self.backup_dir = Path(backup_dir or os.path.join(
            os.path.dirname(os.path.dirname(__file__)), 'backups'))
        self.catalog_path = Path(catalog_path or self.backup_dir / 'catalog.json')
        self.check_interval = check_interval

        # Create directories if they don't exist
        self.backup_dir.mkdir(parents=True, exist_ok=True)

        # Initialize components
        self.encryption = EncryptionManager(encryption_password)
        self.rotation_manager = BackupRotationManager(retention)
        self.remote_backend = remote_backend
        self.catalog = self._load_catalog()

        # Monitoring
        self._monitoring_active = False
        self._monitoring_thread = None
        self._alert_callbacks: List[Callable] = []

        # Scheduler
        self._scheduler_active = False
        self._scheduler_thread = None

    def _load_catalog(self) -> BackupCatalog:
        """Load backup catalog from disk"""
        if self.catalog_path.exists():
            try:
                with open(self.catalog_path, 'r') as f:
                    data = json.load(f)
                return BackupCatalog.from_dict(data)
            except Exception as e:
                logger.error(f"Failed to load catalog: {e}")
        return BackupCatalog()

    def _save_catalog(self):
        """Save backup catalog to disk"""
        try:
            self.catalog_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.catalog_path, 'w') as f:
                json.dump(self.catalog.to_dict(), f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save catalog: {e}")

    def _generate_backup_id(self) -> str:
        """Generate unique backup ID"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f"backup_{timestamp}"

    def _get_workflow_files(self) -> List[Path]:
        """Get all workflow files in the workflows directory"""
        return list(self.workflows_dir.glob('*.json'))

    def _calculate_workflow_checksum(self, workflow_path: Path) -> str:
        """Calculate checksum of a workflow file"""
        return ChecksumCalculator.calculate_file_checksum(str(workflow_path))

    def _load_workflow(self, workflow_path: Path) -> Dict[str, Any]:
        """Load workflow data from file"""
        with open(workflow_path, 'r') as f:
            return json.load(f)

    def _get_changed_workflows(self) -> List[Tuple[Path, str, str]]:
        """
        Get workflows that have changed since last backup.

        Returns:
            List of tuples: (workflow_path, current_checksum, last_checksum)
        """
        changed = []
        for wf_path in self._get_workflow_files():
            current_checksum = self._calculate_workflow_checksum(wf_path)
            wf_id = wf_path.stem
            last_checksum = self.catalog.workflows_checksums.get(wf_id)

            if last_checksum is None or current_checksum != last_checksum:
                changed.append((wf_path, current_checksum, last_checksum or ''))

        return changed

    def _create_backup_archive(self,
                               workflows: List[Dict[str, Any]],
                               backup_id: str,
                               workflow_paths: List[Path],
                               encrypt: bool = False) -> Tuple[str, str, int]:
        """
        Create a backup archive containing workflows.

        Returns:
            Tuple of (archive_path, checksum, size_bytes)
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        archive_name = f"{backup_id}_{timestamp}.tar.gz"
        archive_path = self.backup_dir / archive_name

        # Create manifest
        manifest = {
            'backup_id': backup_id,
            'version': self.VERSION,
            'created_at': datetime.now().isoformat(),
            'workflows': []
        }

        # Write workflows to temp directory
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)

            for i, (wf_data, wf_path) in enumerate(zip(workflows, workflow_paths)):
                wf_filename = f"workflow_{i}.json"
                wf_file_path = tmp_path / wf_filename
                with open(wf_file_path, 'w') as f:
                    json.dump(wf_data, f, indent=2)

                manifest['workflows'].append({
                    'filename': wf_filename,
                    'original_name': wf_path.name,
                    'checksum': self._calculate_workflow_checksum(wf_path),
                    'workflow_id': wf_path.stem
                })

            # Write manifest
            manifest_path = tmp_path / 'manifest.json'
            with open(manifest_path, 'w') as f:
                json.dump(manifest, f, indent=2)

            # Create tar.gz archive
            with tarfile.open(archive_path, 'w:gz') as tar:
                tar.add(tmp_path, arcname='backup')

            # Encrypt if requested
            if encrypt:
                if not HAS_CRYPTO:
                    raise ImportError("cryptography is required for encryption")
                self.encryption.initialize()
                with open(archive_path, 'rb') as f:
                    data = f.read()
                encrypted_data = self.encryption.encrypt(data)
                encrypted_path = archive_path.with_suffix('.enc')
                with open(encrypted_path, 'wb') as f:
                    f.write(encrypted_data)
                archive_path.unlink()
                archive_path = encrypted_path

        size = archive_path.stat().st_size
        checksum = ChecksumCalculator.calculate_file_checksum(str(archive_path))

        return str(archive_path), checksum, size

    def _extract_backup_archive(self,
                                archive_path: str,
                                extract_dir: str,
                                decrypt: bool = False) -> Dict:
        """Extract backup archive to a directory"""
        archive_path = Path(archive_path)

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)

            # Decrypt if encrypted
            if decrypt:
                if not HAS_CRYPTO:
                    raise ImportError("cryptography is required for decryption")
                self.encryption.initialize()
                with open(archive_path, 'rb') as f:
                    encrypted_data = f.read()
                data = self.encryption.decrypt(encrypted_data)
                decrypted_path = tmp_path / 'backup.tar.gz'
                with open(decrypted_path, 'wb') as f:
                    f.write(data)
                archive_path = decrypted_path

            # Extract archive
            with tarfile.open(archive_path, 'r:gz') as tar:
                tar.extractall(tmp_path)

            # Read manifest
            backup_dir = tmp_path / 'backup'
            manifest_path = backup_dir / 'manifest.json'
            with open(manifest_path, 'r') as f:
                manifest = json.load(f)

            # Copy workflows to extract directory
            extract_path = Path(extract_dir)
            extract_path.mkdir(parents=True, exist_ok=True)

            for wf_info in manifest['workflows']:
                wf_src = backup_dir / wf_info['filename']
                wf_dst = extract_path / wf_info['original_name']
                shutil.copy2(wf_src, wf_dst)

            return manifest

    def full_backup(self,
                   encrypt: bool = False,
                   upload_remote: bool = True,
                   retention_policy: RetentionPolicy = RetentionPolicy.DAILY,
                   tags: Dict[str, str] = None) -> Optional[BackupMetadata]:
        """
        Create a full backup of all workflows.

        Args:
            encrypt: Whether to encrypt the backup
            upload_remote: Whether to upload to remote storage
            retention_policy: Retention policy for this backup
            tags: Optional tags for the backup

        Returns:
            BackupMetadata if successful, None otherwise
        """
        backup_id = self._generate_backup_id()
        logger.info(f"Starting full backup: {backup_id}")

        try:
            metadata = BackupMetadata(
                backup_id=backup_id,
                backup_type=BackupType.FULL,
                created_at=datetime.now(),
                workflow_ids=[],
                checksum='',
                encrypted=encrypt,
                size_bytes=0,
                retention_policy=retention_policy,
                version=self.VERSION,
                tags=tags or {},
                status=BackupStatus.IN_PROGRESS
            )

            # Load all workflows
            workflows = []
            workflow_paths = []
            for wf_path in self._get_workflow_files():
                workflows.append(self._load_workflow(wf_path))
                workflow_paths.append(wf_path)
                metadata.workflow_ids.append(wf_path.stem)

            if not workflows:
                logger.warning("No workflows found to backup")
                return None

            # Create archive
            archive_path, checksum, size = self._create_backup_archive(
                workflows, backup_id, workflow_paths, encrypt)

            metadata.checksum = checksum
            metadata.size_bytes = size
            metadata.status = BackupStatus.COMPLETED

            # Verify backup
            if not self._verify_backup_integrity(archive_path, checksum, encrypt):
                metadata.status = BackupStatus.FAILED
                metadata.error_message = "Backup verification failed"
                logger.error(f"Backup verification failed: {backup_id}")
                return metadata

            metadata.status = BackupStatus.VERIFIED

            # Upload to remote if configured
            if upload_remote and self.remote_backend:
                remote_path = f"backups/{backup_id}/{Path(archive_path).name}"
                if self.remote_backend.upload(archive_path, remote_path):
                    metadata.tags['remote_path'] = remote_path
                else:
                    logger.warning(f"Failed to upload backup to remote: {backup_id}")

            # Apply rotation policy
            self._apply_rotation()

            # Update catalog
            self.catalog.add_backup(metadata)
            self._save_catalog()

            logger.info(f"Full backup completed: {backup_id} ({size} bytes)")
            return metadata

        except Exception as e:
            logger.error(f"Full backup failed: {e}")
            return BackupMetadata(
                backup_id=backup_id,
                backup_type=BackupType.FULL,
                created_at=datetime.now(),
                workflow_ids=[],
                checksum='',
                encrypted=encrypt,
                size_bytes=0,
                retention_policy=retention_policy,
                version=self.VERSION,
                tags={},
                status=BackupStatus.FAILED,
                error_message=str(e)
            )

    def incremental_backup(self,
                          encrypt: bool = False,
                          upload_remote: bool = True,
                          retention_policy: RetentionPolicy = RetentionPolicy.DAILY,
                          tags: Dict[str, str] = None) -> Optional[BackupMetadata]:
        """
        Create an incremental backup of changed workflows since last backup.

        Args:
            encrypt: Whether to encrypt the backup
            upload_remote: Whether to upload to remote storage
            retention_policy: Retention policy for this backup
            tags: Optional tags for the backup

        Returns:
            BackupMetadata if successful, None otherwise
        """
        backup_id = self._generate_backup_id()
        logger.info(f"Starting incremental backup: {backup_id}")

        try:
            # Find changed workflows
            changed = self._get_changed_workflows()

            if not changed:
                logger.info("No workflows have changed since last backup")
                return None

            metadata = BackupMetadata(
                backup_id=backup_id,
                backup_type=BackupType.INCREMENTAL,
                created_at=datetime.now(),
                workflow_ids=[],
                checksum='',
                encrypted=encrypt,
                size_bytes=0,
                retention_policy=retention_policy,
                version=self.VERSION,
                tags=tags or {},
                status=BackupStatus.IN_PROGRESS
            )

            # Get last backup for reference
            last_backup = self.catalog.backups[-1] if self.catalog.backups else None
            if last_backup:
                metadata.parent_backup_id = last_backup.backup_id

            # Load changed workflows
            workflows = []
            workflow_paths = []
            for wf_path, current_checksum, last_checksum in changed:
                workflows.append(self._load_workflow(wf_path))
                workflow_paths.append(wf_path)
                metadata.workflow_ids.append(wf_path.stem)
                logger.info(f"Changed workflow: {wf_path.name} ({last_checksum[:8]}... -> {current_checksum[:8]}...)")

            # Create archive
            archive_path, checksum, size = self._create_backup_archive(
                workflows, backup_id, workflow_paths, encrypt)

            metadata.checksum = checksum
            metadata.size_bytes = size
            metadata.status = BackupStatus.COMPLETED

            # Verify backup
            if not self._verify_backup_integrity(archive_path, checksum, encrypt):
                metadata.status = BackupStatus.FAILED
                metadata.error_message = "Backup verification failed"
                logger.error(f"Backup verification failed: {backup_id}")
                return metadata

            metadata.status = BackupStatus.VERIFIED

            # Upload to remote if configured
            if upload_remote and self.remote_backend:
                remote_path = f"backups/{backup_id}/{Path(archive_path).name}"
                if self.remote_backend.upload(archive_path, remote_path):
                    metadata.tags['remote_path'] = remote_path

            # Apply rotation policy
            self._apply_rotation()

            # Update catalog
            self.catalog.add_backup(metadata)
            self._save_catalog()

            logger.info(f"Incremental backup completed: {backup_id} ({len(changed)} workflows)")
            return metadata

        except Exception as e:
            logger.error(f"Incremental backup failed: {e}")
            return BackupMetadata(
                backup_id=backup_id,
                backup_type=BackupType.INCREMENTAL,
                created_at=datetime.now(),
                workflow_ids=[],
                checksum='',
                encrypted=encrypt,
                size_bytes=0,
                retention_policy=retention_policy,
                version=self.VERSION,
                tags={},
                status=BackupStatus.FAILED,
                error_message=str(e)
            )

    def _verify_backup_integrity(self,
                                 archive_path: str,
                                 expected_checksum: str,
                                 encrypted: bool = False) -> bool:
        """Verify backup integrity using checksums"""
        try:
            if encrypted:
                # For encrypted files, we verify after decryption
                if not HAS_CRYPTO:
                    return False
                self.encryption.initialize()
                with open(archive_path, 'rb') as f:
                    encrypted_data = f.read()
                data = self.encryption.decrypt(encrypted_data)

                # Calculate checksum of decrypted data
                actual = ChecksumCalculator.calculate_data_checksum(data)
                return actual == expected_checksum
            else:
                return ChecksumCalculator.verify_file_integrity(
                    archive_path, expected_checksum)
        except Exception as e:
            logger.error(f"Backup verification error: {e}")
            return False

    def verify_backup(self, backup_id: str) -> bool:
        """
        Verify the integrity of a specific backup.

        Args:
            backup_id: ID of the backup to verify

        Returns:
            True if backup is valid, False otherwise
        """
        metadata = self.catalog.get_backup(backup_id)
        if not metadata:
            logger.error(f"Backup not found: {backup_id}")
            return False

        # Find local backup file
        backup_files = list(self.backup_dir.glob(f"{backup_id}*"))
        if not backup_files:
            logger.error(f"Backup file not found: {backup_id}")
            return False

        is_valid = self._verify_backup_integrity(
            str(backup_files[0]), metadata.checksum, metadata.encrypted)

        if is_valid:
            metadata.status = BackupStatus.VERIFIED
            self._save_catalog()
            logger.info(f"Backup verified: {backup_id}")
        else:
            logger.warning(f"Backup verification failed: {backup_id}")

        return is_valid

    def restore_backup(self,
                       backup_id: str,
                       restore_dir: str = None,
                       workflow_ids: List[str] = None,
                       decrypt: bool = False) -> bool:
        """
        Restore workflows from a backup.

        Args:
            backup_id: ID of the backup to restore
            restore_dir: Directory to restore to (default: workflows_dir)
            workflow_ids: Specific workflow IDs to restore (None for all)
            decrypt: Whether the backup is encrypted

        Returns:
            True if restore successful, False otherwise
        """
        metadata = self.catalog.get_backup(backup_id)
        if not metadata:
            logger.error(f"Backup not found: {backup_id}")
            return False

        # Find backup file
        backup_files = list(self.backup_dir.glob(f"{backup_id}*"))
        if not backup_files and self.remote_backend:
            # Try to download from remote
            remote_path = metadata.tags.get('remote_path')
            if remote_path:
                local_path = self.backup_dir / Path(remote_path).name
                if self.remote_backend.download(remote_path, str(local_path)):
                    backup_files = [local_path]

        if not backup_files:
            logger.error(f"Backup file not found: {backup_id}")
            return False

        try:
            restore_path = Path(restore_dir or self.workflows_dir)

            # Extract backup
            manifest = self._extract_backup_archive(
                str(backup_files[0]), str(restore_path), decrypt or metadata.encrypted)

            # Filter specific workflows if requested
            if workflow_ids:
                for wf_info in manifest['workflows']:
                    if wf_info['workflow_id'] not in workflow_ids:
                        wf_file = restore_path / wf_info['original_name']
                        if wf_file.exists():
                            wf_file.unlink()

            logger.info(f"Restored backup: {backup_id}")
            return True

        except Exception as e:
            logger.error(f"Restore failed: {e}")
            return False

    def disaster_recovery(self,
                         target_datetime: datetime,
                         restore_dir: str = None) -> bool:
        """
        Perform disaster recovery to a point in time.

        Finds the most recent backup before or at target_datetime and restores it.

        Args:
            target_datetime: Point in time to restore to
            restore_dir: Directory to restore to

        Returns:
            True if recovery successful, False otherwise
        """
        # Find applicable backups
        applicable_backups = [
            b for b in self.catalog.backups
            if b.created_at <= target_datetime and b.status == BackupStatus.VERIFIED
        ]

        if not applicable_backups:
            logger.error(f"No backups found before {target_datetime}")
            return False

        # Sort by creation time, most recent first
        applicable_backups.sort(key=lambda b: b.created_at, reverse=True)

        # Try to restore from the most recent backup
        for backup in applicable_backups:
            if self.restore_backup(backup.backup_id, restore_dir):
                logger.info(f"Disaster recovery completed to {target_datetime}")
                return True

        logger.error("Disaster recovery failed")
        return False

    def get_backup_history(self,
                          workflow_id: str = None,
                          backup_type: BackupType = None,
                          limit: int = 100) -> List[BackupMetadata]:
        """
        Get backup history, optionally filtered.

        Args:
            workflow_id: Filter by workflow ID
            backup_type: Filter by backup type
            limit: Maximum number of backups to return

        Returns:
            List of BackupMetadata
        """
        results = self.catalog.backups

        if workflow_id:
            results = [b for b in results if workflow_id in b.workflow_ids]

        if backup_type:
            results = [b for b in results if b.backup_type == backup_type]

        results.sort(key=lambda b: b.created_at, reverse=True)
        return results[:limit]

    def list_backups(self,
                     show_expired: bool = False) -> List[BackupMetadata]:
        """
        List all backups in the catalog.

        Args:
            show_expired: Include expired backups

        Returns:
            List of BackupMetadata
        """
        if show_expired:
            return self.catalog.backups

        return [b for b in self.catalog.backups
                if b.expires_at is None or b.expires_at > datetime.now()]

    def delete_backup(self, backup_id: str, delete_remote: bool = True) -> bool:
        """
        Delete a backup.

        Args:
            backup_id: ID of backup to delete
            delete_remote: Also delete from remote storage if present

        Returns:
            True if deletion successful
        """
        metadata = self.catalog.get_backup(backup_id)
        if not metadata:
            logger.error(f"Backup not found: {backup_id}")
            return False

        try:
            # Delete local file
            backup_files = list(self.backup_dir.glob(f"{backup_id}*"))
            for f in backup_files:
                f.unlink()

            # Delete remote if present
            if delete_remote and self.remote_backend:
                remote_path = metadata.tags.get('remote_path')
                if remote_path:
                    self.remote_backend.delete(remote_path)

            # Remove from catalog
            self.catalog.backups = [b for b in self.catalog.backups
                                   if b.backup_id != backup_id]
            self._save_catalog()

            logger.info(f"Deleted backup: {backup_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to delete backup: {e}")
            return False

    def _apply_rotation(self):
        """Apply rotation policy to manage backup retention"""
        to_delete = self.rotation_manager.get_backups_to_delete(self.catalog.backups)

        for backup in to_delete:
            self.delete_backup(backup.backup_id)

        if to_delete:
            logger.info(f"Rotation: deleted {len(to_delete)} old backups")

    def create_versioned_backup(self,
                               version_tag: str,
                               encrypt: bool = True,
                               upload_remote: bool = True) -> Optional[BackupMetadata]:
        """
        Create a versioned backup for cross-version compatibility.

        Args:
            version_tag: Version identifier (e.g., 'v1.0', 'v2.1')
            encrypt: Whether to encrypt the backup
            upload_remote: Whether to upload to remote storage

        Returns:
            BackupMetadata if successful
        """
        metadata = self.full_backup(
            encrypt=encrypt,
            upload_remote=upload_remote,
            retention_policy=RetentionPolicy.MONTHLY,
            tags={'version_tag': version_tag}
        )

        if metadata:
            metadata.tags['version_tag'] = version_tag
            self._save_catalog()

        return metadata

    def restore_versioned_backup(self,
                                version_tag: str,
                                restore_dir: str = None) -> bool:
        """
        Restore a versioned backup.

        Args:
            version_tag: Version tag to restore
            restore_dir: Directory to restore to

        Returns:
            True if restore successful
        """
        versioned_backups = [
            b for b in self.catalog.backups
            if b.tags.get('version_tag') == version_tag
        ]

        if not versioned_backups:
            logger.error(f"No backup found for version: {version_tag}")
            return False

        # Use the most recent backup for this version
        latest = max(versioned_backups, key=lambda b: b.created_at)
        return self.restore_backup(latest.backup_id, restore_dir)

    # Monitoring Methods

    def add_alert_callback(self, callback: Callable[[str, Dict], None]):
        """Add a callback for backup monitoring alerts"""
        self._alert_callbacks.append(callback)

    def _trigger_alert(self, alert_type: str, data: Dict):
        """Trigger an alert to all registered callbacks"""
        for callback in self._alert_callbacks:
            try:
                callback(alert_type, data)
            except Exception as e:
                logger.error(f"Alert callback failed: {e}")

    def check_disk_space(self) -> Dict[str, Any]:
        """
        Check available disk space for backups.

        Returns:
            Dict with disk space information
        """
        try:
            stat = shutil.disk_usage(self.backup_dir)
            percent_used = (stat.used / stat.total) * 100

            return {
                'total_bytes': stat.total,
                'used_bytes': stat.used,
                'free_bytes': stat.free,
                'percent_used': percent_used,
                'low_space': percent_used > 90
            }
        except Exception as e:
            logger.error(f"Failed to check disk space: {e}")
            return {'error': str(e)}

    def _monitoring_loop(self):
        """Background monitoring loop"""
        while self._monitoring_active:
            # Check disk space
            space = self.check_disk_space()
            if space.get('low_space'):
                self._trigger_alert('low_disk_space', space)

            # Check backup health
            recent_backups = self.get_backup_history(limit=5)
            if recent_backups:
                latest = recent_backups[0]
                hours_since = (datetime.now() - latest.created_at).total_seconds() / 3600

                if latest.status == BackupStatus.FAILED:
                    self._trigger_alert('backup_failed', {
                        'backup_id': latest.backup_id,
                        'error': latest.error_message
                    })
                elif hours_since > 48:
                    self._trigger_alert('no_recent_backup', {
                        'hours_since_last_backup': hours_since
                    })

            time.sleep(self.check_interval)

    def start_monitoring(self):
        """Start background monitoring"""
        if self._monitoring_active:
            return

        self._monitoring_active = True
        self._monitoring_thread = threading.Thread(target=self._monitoring_loop)
        self._monitoring_thread.daemon = True
        self._monitoring_thread.start()
        logger.info("Backup monitoring started")

    def stop_monitoring(self):
        """Stop background monitoring"""
        self._monitoring_active = False
        if self._monitoring_thread:
            self._monitoring_thread.join(timeout=5)
        logger.info("Backup monitoring stopped")

    # Scheduling Methods

    def schedule_full_backup(self,
                            time_str: str = "02:00",
                            encrypt: bool = True):
        """
        Schedule daily full backup.

        Args:
            time_str: Time to run backup (HH:MM format)
            encrypt: Whether to encrypt backups
        """
        def job():
            self.full_backup(encrypt=encrypt)

        schedule.every().day.at(time_str).do(job)
        logger.info(f"Scheduled full backup daily at {time_str}")

    def schedule_incremental_backup(self,
                                   interval_hours: int = 6,
                                   encrypt: bool = True):
        """
        Schedule incremental backups.

        Args:
            interval_hours: Hours between incremental backups
            encrypt: Whether to encrypt backups
        """
        def job():
            self.incremental_backup(encrypt=encrypt)

        schedule.every(interval_hours).hours.do(job)
        logger.info(f"Scheduled incremental backup every {interval_hours} hours")

    def schedule_weekly_full_backup(self,
                                    day: str = "sunday",
                                    time_str: str = "03:00",
                                    encrypt: bool = True):
        """
        Schedule weekly full backup.

        Args:
            day: Day of week (monday, tuesday, etc.)
            time_str: Time to run backup (HH:MM format)
            encrypt: Whether to encrypt backups
        """
        def job():
            self.full_backup(
                encrypt=encrypt,
                retention_policy=RetentionPolicy.WEEKLY
            )

        getattr(schedule.every(), day).at(time_str).do(job)
        logger.info(f"Scheduled weekly full backup on {day} at {time_str}")

    def _scheduler_loop(self):
        """Background scheduler loop"""
        while self._scheduler_active:
            schedule.run_pending()
            time.sleep(60)

    def start_scheduler(self):
        """Start the backup scheduler"""
        if self._scheduler_active:
            return

        self._scheduler_active = True
        self._scheduler_thread = threading.Thread(target=self._scheduler_loop)
        self._scheduler_thread.daemon = True
        self._scheduler_thread.start()
        logger.info("Backup scheduler started")

    def stop_scheduler(self):
        """Stop the backup scheduler"""
        self._scheduler_active = False
        if self._scheduler_thread:
            self._scheduler_thread.join(timeout=5)
        logger.info("Backup scheduler stopped")

    def get_catalog_info(self) -> Dict[str, Any]:
        """
        Get information about the backup catalog.

        Returns:
            Dict with catalog statistics
        """
        backups = self.catalog.backups
        return {
            'total_backups': len(backups),
            'full_backups': len([b for b in backups if b.backup_type == BackupType.FULL]),
            'incremental_backups': len([b for b in backups if b.backup_type == BackupType.INCREMENTAL]),
            'verified_backups': len([b for b in backups if b.status == BackupStatus.VERIFIED]),
            'failed_backups': len([b for b in backups if b.status == BackupStatus.FAILED]),
            'encrypted_backups': len([b for b in backups if b.encrypted]),
            'total_size_bytes': sum(b.size_bytes for b in backups),
            'last_backup': max((b.created_at for b in backups), default=None),
            'versioned_backups': len(set(b.tags.get('version_tag') for b in backups if b.tags.get('version_tag')))
        }

    def cleanup(self):
        """Cleanup resources and stop background tasks"""
        self.stop_monitoring()
        self.stop_scheduler()
        if self.remote_backend:
            self.remote_backend.disconnect()
        logger.info("WorkflowBackup cleanup completed")


# Factory function for creating backup system with remote storage
def create_backup_system(backup_type: str = None, **kwargs) -> WorkflowBackup:
    """
    Create a WorkflowBackup instance with specified remote backend.

    Args:
        backup_type: Type of remote backend ('s3', 'ftp', 'scp')
        **kwargs: Arguments for the backend

    Returns:
        WorkflowBackup instance
    """
    remote_backend = None

    if backup_type:
        if backup_type == 's3':
            remote_backend = S3Backend(
                bucket=kwargs.get('bucket'),
                access_key=kwargs.get('access_key'),
                secret_key=kwargs.get('secret_key'),
                endpoint_url=kwargs.get('endpoint_url'),
                region=kwargs.get('region', 'us-east-1')
            )
        elif backup_type == 'ftp':
            remote_backend = FTPBackend(
                host=kwargs.get('host'),
                port=kwargs.get('port', 21),
                username=kwargs.get('username'),
                password=kwargs.get('password'),
                base_dir=kwargs.get('base_dir', '/')
            )
        elif backup_type == 'scp':
            remote_backend = SCPBackend(
                host=kwargs.get('host'),
                port=kwargs.get('port', 22),
                username=kwargs.get('username'),
                password=kwargs.get('password'),
                key_file=kwargs.get('key_file'),
                remote_path=kwargs.get('remote_path', '/')
            )

    return WorkflowBackup(remote_backend=remote_backend, **kwargs)


# Convenience functions
def quick_backup(workflows_dir: str = None, backup_dir: str = None) -> bool:
    """Quick full backup of all workflows"""
    backup = WorkflowBackup(workflows_dir=workflows_dir, backup_dir=backup_dir)
    result = backup.full_backup()
    return result is not None and result.status == BackupStatus.VERIFIED


def quick_restore(backup_id: str, workflows_dir: str = None) -> bool:
    """Quick restore from a backup"""
    backup = WorkflowBackup(workflows_dir=workflows_dir)
    return backup.restore_backup(backup_id)
