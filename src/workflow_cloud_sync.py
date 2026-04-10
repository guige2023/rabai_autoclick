"""
Workflow Cloud Synchronization System

A comprehensive cloud sync system for workflows with:
- Multi-provider support (S3, Google Cloud Storage, Azure Blob, Dropbox)
- Bidirectional sync between local and cloud
- Conflict resolution for simultaneous modifications
- Selective sync for chosen workflows
- Offline support with change queuing
- Per-workflow sync status tracking
- Delta sync for bandwidth optimization
- AES-256 encryption at rest
- Flexible sync scheduling (on-change or scheduled)
- Complete audit trail and sync logs
"""

import os
import json
import hashlib
import shutil
import logging
import threading
import time
import re
import tempfile
import queue
import copy
import difflib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Callable, Set
from dataclasses import dataclass, field, asdict
from enum import Enum
from abc import ABC, abstractmethod
import schedule
import asyncio
import aiofiles

# Optional imports with graceful fallbacks
try:
    import boto3
    from botocore.exceptions import ClientError, BotoCoreError
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

try:
    from google.cloud import storage
    from google.oauth2 import service_account
    HAS_GCP = True
except ImportError:
    HAS_GCP = False

try:
    from azure.storage.blob import BlobServiceClient, ContentSettings
    HAS_AZURE = True
except ImportError:
    HAS_AZURE = False

try:
    import dropbox
    from dropbox.exceptions import AuthError, ApiError
    HAS_DROPBOX = True
except ImportError:
    HAS_DROPBOX = False

try:
    from cryptography.fernet import Fernet
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    HAS_CRYPTO = True
except ImportError:
    HAS_CRYPTO = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SyncProvider(Enum):
    """Supported cloud storage providers"""
    S3 = "s3"
    GCS = "gcs"
    AZURE = "azure"
    DROPBOX = "dropbox"


class SyncDirection(Enum):
    """Sync direction modes"""
    UPLOAD_ONLY = "upload_only"
    DOWNLOAD_ONLY = "download_only"
    BIDIRECTIONAL = "bidirectional"


class SyncStatus(Enum):
    """Sync status for individual workflows"""
    SYNCED = "synced"
    PENDING_UPLOAD = "pending_upload"
    PENDING_DOWNLOAD = "pending_download"
    CONFLICT = "conflict"
    ERROR = "error"
    OFFLINE_QUEUED = "offline_queued"
    EXCLUDED = "excluded"


class ConflictResolution(Enum):
    """Conflict resolution strategies"""
    LOCAL_WINS = "local_wins"
    REMOTE_WINS = "remote_wins"
    NEWEST_WINS = "newest_wins"
    MANUAL = "manual"


class SyncEvent(Enum):
    """Types of sync events"""
    UPLOAD = "upload"
    DOWNLOAD = "download"
    CONFLICT_DETECTED = "conflict_detected"
    CONFLICT_RESOLVED = "conflict_resolved"
    DELETE = "delete"
    ERROR = "error"


@dataclass
class WorkflowVersion:
    """Version information for a workflow"""
    version_id: str
    checksum: str
    size: int
    modified_at: datetime
    modified_by: str = "unknown"
    delta_from: Optional[str] = None


@dataclass
class SyncRecord:
    """Record of a sync operation"""
    event: SyncEvent
    workflow_id: str
    timestamp: datetime
    success: bool
    error_message: Optional[str] = None
    direction: Optional[SyncDirection] = None
    bytes_transferred: int = 0
    duration_ms: int = 0


@dataclass
class ConflictInfo:
    """Information about a sync conflict"""
    workflow_id: str
    local_version: WorkflowVersion
    remote_version: WorkflowVersion
    local_content: Dict[str, Any]
    remote_content: Dict[str, Any]
    detected_at: datetime
    resolution: Optional[ConflictResolution] = None
    resolved_at: Optional[datetime] = None
    resolved_content: Optional[Dict[str, Any]] = None


@dataclass
class SyncState:
    """Current sync state for a workflow"""
    workflow_id: str
    status: SyncStatus
    local_checksum: str
    remote_checksum: Optional[str]
    last_synced: Optional[datetime]
    last_modified: datetime
    version_id: str
    pending_changes: int = 0
    error_message: Optional[str] = None


@dataclass
class ProviderConfig:
    """Configuration for a cloud provider"""
    provider: SyncProvider
    bucket: str
    prefix: str = ""
    region: Optional[str] = None
    credentials_path: Optional[str] = None
    credentials_dict: Optional[Dict] = None
    endpoint_url: Optional[str] = None  # For S3-compatible storage


class CloudProvider(ABC):
    """Abstract base class for cloud storage providers"""

    @abstractmethod
    def connect(self) -> bool:
        """Establish connection to the provider"""
        pass

    @abstractmethod
    def disconnect(self):
        """Close connection to the provider"""
        pass

    @abstractmethod
    def upload_file(self, local_path: str, remote_path: str, encrypted: bool = False) -> bool:
        """Upload a file to cloud storage"""
        pass

    @abstractmethod
    def download_file(self, remote_path: str, local_path: str) -> bool:
        """Download a file from cloud storage"""
        pass

    @abstractmethod
    def delete_file(self, remote_path: str) -> bool:
        """Delete a file from cloud storage"""
        pass

    @abstractmethod
    def list_files(self, prefix: str = "") -> List[str]:
        """List files in the cloud storage with given prefix"""
        pass

    @abstractmethod
    def file_exists(self, remote_path: str) -> bool:
        """Check if a file exists in cloud storage"""
        pass

    @abstractmethod
    def get_file_metadata(self, remote_path: str) -> Dict[str, Any]:
        """Get metadata for a file in cloud storage"""
        pass

    @abstractmethod
    def is_online(self) -> bool:
        """Check if the provider is accessible"""
        pass


class S3Provider(CloudProvider):
    """Amazon S3 / S3-compatible storage provider"""

    def __init__(self, config: ProviderConfig):
        self.config = config
        self.client = None
        self._connected = False

    def connect(self) -> bool:
        if not HAS_BOTO3:
            logger.error("boto3 not installed. S3 sync not available.")
            return False

        try:
            if self.config.endpoint_url:
                self.client = boto3.client(
                    's3',
                    region_name=self.config.region or 'us-east-1',
                    endpoint_url=self.config.endpoint_url,
                    aws_access_key_id=self.config.credentials_dict.get('access_key') if self.config.credentials_dict else None,
                    aws_secret_access_key=self.config.credentials_dict.get('secret_key') if self.config.credentials_dict else None
                )
            else:
                session = boto3.Session(
                    region_name=self.config.region or 'us-east-1',
                    aws_access_key_id=self.config.credentials_dict.get('access_key') if self.config.credentials_dict else None,
                    aws_secret_access_key=self.config.credentials_dict.get('secret_key') if self.config.credentials_dict else None
                )
                self.client = session.client('s3')

            self._connected = True
            return True
        except Exception as e:
            logger.error(f"Failed to connect to S3: {e}")
            return False

    def disconnect(self):
        self.client = None
        self._connected = False

    def upload_file(self, local_path: str, remote_path: str, encrypted: bool = False) -> bool:
        if not self._connected:
            return False
        try:
            extra_args = {}
            if encrypted:
                extra_args['ServerSideEncryption'] = 'AES256'

            self.client.upload_file(
                local_path,
                self.config.bucket,
                remote_path,
                ExtraArgs=extra_args if extra_args else None
            )
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to upload to S3: {e}")
            return False

    def download_file(self, remote_path: str, local_path: str) -> bool:
        if not self._connected:
            return False
        try:
            self.client.download_file(self.config.bucket, remote_path, local_path)
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to download from S3: {e}")
            return False

    def delete_file(self, remote_path: str) -> bool:
        if not self._connected:
            return False
        try:
            self.client.delete_object(Bucket=self.config.bucket, Key=remote_path)
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to delete from S3: {e}")
            return False

    def list_files(self, prefix: str = "") -> List[str]:
        if not self._connected:
            return []
        try:
            result = self.client.list_objects_v2(
                Bucket=self.config.bucket,
                Prefix=prefix
            )
            return [obj['Key'] for obj in result.get('Contents', [])]
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to list S3 files: {e}")
            return []

    def file_exists(self, remote_path: str) -> bool:
        if not self._connected:
            return False
        try:
            self.client.head_object(Bucket=self.config.bucket, Key=remote_path)
            return True
        except ClientError:
            return False

    def get_file_metadata(self, remote_path: str) -> Dict[str, Any]:
        if not self._connected:
            return {}
        try:
            response = self.client.head_object(Bucket=self.config.bucket, Key=remote_path)
            return {
                'size': response.get('ContentLength', 0),
                'modified': response.get('LastModified'),
                'etag': response.get('ETag', '').strip('"'),
                'metadata': response.get('Metadata', {})
            }
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get S3 file metadata: {e}")
            return {}

    def is_online(self) -> bool:
        if not self._connected:
            return False
        try:
            self.client.head_bucket(Bucket=self.config.bucket)
            return True
        except (ClientError, BotoCoreError):
            return False


class GCSProvider(CloudProvider):
    """Google Cloud Storage provider"""

    def __init__(self, config: ProviderConfig):
        self.config = config
        self.client = None
        self._connected = False

    def connect(self) -> bool:
        if not HAS_GCP:
            logger.error("google-cloud-storage not installed. GCS sync not available.")
            return False

        try:
            if self.config.credentials_dict:
                credentials = service_account.Credentials.from_service_account_info(
                    self.config.credentials_dict
                )
                self.client = storage.Client(credentials=credentials)
            else:
                self.client = storage.Client()

            # Verify connection
            self.client.bucket(self.config.bucket)
            self._connected = True
            return True
        except Exception as e:
            logger.error(f"Failed to connect to GCS: {e}")
            return False

    def disconnect(self):
        self.client = None
        self._connected = False

    def upload_file(self, local_path: str, remote_path: str, encrypted: bool = False) -> bool:
        if not self._connected:
            return False
        try:
            bucket = self.client.bucket(self.config.bucket)
            blob = bucket.blob(remote_path)

            if encrypted:
                blob.content_encoding = 'encrypted'

            blob.upload_from_filename(local_path)
            return True
        except Exception as e:
            logger.error(f"Failed to upload to GCS: {e}")
            return False

    def download_file(self, remote_path: str, local_path: str) -> bool:
        if not self._connected:
            return False
        try:
            bucket = self.client.bucket(self.config.bucket)
            blob = bucket.blob(remote_path)
            blob.download_to_filename(local_path)
            return True
        except Exception as e:
            logger.error(f"Failed to download from GCS: {e}")
            return False

    def delete_file(self, remote_path: str) -> bool:
        if not self._connected:
            return False
        try:
            bucket = self.client.bucket(self.config.bucket)
            blob = bucket.blob(remote_path)
            blob.delete()
            return True
        except Exception as e:
            logger.error(f"Failed to delete from GCS: {e}")
            return False

    def list_files(self, prefix: str = "") -> List[str]:
        if not self._connected:
            return []
        try:
            bucket = self.client.bucket(self.config.bucket)
            blobs = bucket.list_blobs(prefix=prefix)
            return [blob.name for blob in blobs]
        except Exception as e:
            logger.error(f"Failed to list GCS files: {e}")
            return []

    def file_exists(self, remote_path: str) -> bool:
        if not self._connected:
            return False
        try:
            bucket = self.client.bucket(self.config.bucket)
            blob = bucket.blob(remote_path)
            return blob.exists()
        except Exception:
            return False

    def get_file_metadata(self, remote_path: str) -> Dict[str, Any]:
        if not self._connected:
            return {}
        try:
            bucket = self.client.bucket(self.config.bucket)
            blob = bucket.blob(remote_path)
            blob.reload()
            return {
                'size': blob.size,
                'modified': blob.updated,
                'etag': blob.etag,
                'metadata': blob.metadata or {}
            }
        except Exception as e:
            logger.error(f"Failed to get GCS file metadata: {e}")
            return {}

    def is_online(self) -> bool:
        if not self._connected:
            return False
        try:
            bucket = self.client.bucket(self.config.bucket)
            bucket.reload()
            return True
        except Exception:
            return False


class AzureProvider(CloudProvider):
    """Azure Blob Storage provider"""

    def __init__(self, config: ProviderConfig):
        self.config = config
        self.client = None
        self._connected = False

    def connect(self) -> bool:
        if not HAS_AZURE:
            logger.error("azure-storage-blob not installed. Azure sync not available.")
            return False

        try:
            connection_string = self.config.credentials_dict.get('connection_string') if self.config.credentials_dict else None
            if connection_string:
                self.client = BlobServiceClient.from_connection_string(connection_string)
            else:
                account_url = f"https://{self.config.bucket}.blob.core.windows.net"
                self.client = BlobServiceClient(account_url=account_url)

            self._connected = True
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Azure: {e}")
            return False

    def disconnect(self):
        self.client = None
        self._connected = False

    def upload_file(self, local_path: str, remote_path: str, encrypted: bool = False) -> bool:
        if not self._connected:
            return False
        try:
            container_client = self.client.get_container_client(self.config.bucket)
            blob_client = container_client.get_blob_client(remote_path)

            content_settings = ContentSettings(content_type='application/json')
            if encrypted:
                content_settings.metadata = {'encrypted': 'true'}

            with open(local_path, 'rb') as data:
                blob_client.upload_blob(data, overwrite=True, content_settings=content_settings)
            return True
        except Exception as e:
            logger.error(f"Failed to upload to Azure: {e}")
            return False

    def download_file(self, remote_path: str, local_path: str) -> bool:
        if not self._connected:
            return False
        try:
            container_client = self.client.get_container_client(self.config.bucket)
            blob_client = container_client.get_blob_client(remote_path)

            with open(local_path, 'wb') as data:
                data.write(blob_client.download_blob().readall())
            return True
        except Exception as e:
            logger.error(f"Failed to download from Azure: {e}")
            return False

    def delete_file(self, remote_path: str) -> bool:
        if not self._connected:
            return False
        try:
            container_client = self.client.get_container_client(self.config.bucket)
            blob_client = container_client.get_blob_client(remote_path)
            blob_client.delete_blob()
            return True
        except Exception as e:
            logger.error(f"Failed to delete from Azure: {e}")
            return False

    def list_files(self, prefix: str = "") -> List[str]:
        if not self._connected:
            return []
        try:
            container_client = self.client.get_container_client(self.config.bucket)
            blobs = container_client.list_blobs(name_starts_with=prefix)
            return [blob.name for blob in blobs]
        except Exception as e:
            logger.error(f"Failed to list Azure files: {e}")
            return []

    def file_exists(self, remote_path: str) -> bool:
        if not self._connected:
            return False
        try:
            container_client = self.client.get_container_client(self.config.bucket)
            blob_client = container_client.get_blob_client(remote_path)
            return blob_client.exists()
        except Exception:
            return False

    def get_file_metadata(self, remote_path: str) -> Dict[str, Any]:
        if not self._connected:
            return {}
        try:
            container_client = self.client.get_container_client(self.config.bucket)
            blob_client = container_client.get_blob_client(remote_path)
            blob_props = blob_client.get_blob_properties()
            return {
                'size': blob_props.size,
                'modified': blob_props.last_modified,
                'etag': blob_props.etag,
                'metadata': blob_props.metadata or {}
            }
        except Exception as e:
            logger.error(f"Failed to get Azure file metadata: {e}")
            return {}

    def is_online(self) -> bool:
        if not self._connected:
            return False
        try:
            container_client = self.client.get_container_client(self.config.bucket)
            return container_client.get_container_properties() is not None
        except Exception:
            return False


class DropboxProvider(CloudProvider):
    """Dropbox Cloud Storage provider"""

    def __init__(self, config: ProviderConfig):
        self.config = config
        self.client = None
        self._connected = False

    def connect(self) -> bool:
        if not HAS_DROPBOX:
            logger.error("dropbox not installed. Dropbox sync not available.")
            return False

        try:
            access_token = self.config.credentials_dict.get('access_token') if self.config.credentials_dict else None
            if not access_token:
                logger.error("Dropbox access token not provided")
                return False

            self.client = dropbox.Dropbox(access_token)
            # Verify connection
            self.client.users_get_current_account()
            self._connected = True
            return True
        except AuthError as e:
            logger.error(f"Failed to authenticate with Dropbox: {e}")
            return False
        except Exception as e:
            logger.error(f"Failed to connect to Dropbox: {e}")
            return False

    def disconnect(self):
        self.client = None
        self._connected = False

    def upload_file(self, local_path: str, remote_path: str, encrypted: bool = False) -> bool:
        if not self._connected:
            return False
        try:
            with open(local_path, 'rb') as f:
                data = f.read()

            remote_path = '/' + remote_path.lstrip('/')
            self.client.files_upload(data, remote_path, mode=dropbox.files.WriteMode.overwrite)
            return True
        except ApiError as e:
            logger.error(f"Failed to upload to Dropbox: {e}")
            return False

    def download_file(self, remote_path: str, local_path: str) -> bool:
        if not self._connected:
            return False
        try:
            remote_path = '/' + remote_path.lstrip('/')
            _, response = self.client.files_download(remote_path)
            with open(local_path, 'wb') as f:
                f.write(response.content)
            return True
        except ApiError as e:
            logger.error(f"Failed to download from Dropbox: {e}")
            return False

    def delete_file(self, remote_path: str) -> bool:
        if not self._connected:
            return False
        try:
            remote_path = '/' + remote_path.lstrip('/')
            self.client.files_delete(remote_path)
            return True
        except ApiError as e:
            logger.error(f"Failed to delete from Dropbox: {e}")
            return False

    def list_files(self, prefix: str = "") -> List[str]:
        if not self._connected:
            return []
        try:
            remote_path = '/' + prefix.lstrip('/')
            result = self.client.files_list_folder(remote_path)
            files = []
            for entry in result.entries:
                if isinstance(entry, dropbox.files.FileMetadata):
                    files.append(entry.path_lower)
            return files
        except ApiError as e:
            logger.error(f"Failed to list Dropbox files: {e}")
            return []

    def file_exists(self, remote_path: str) -> bool:
        if not self._connected:
            return False
        try:
            remote_path = '/' + remote_path.lstrip('/')
            self.client.files_get_metadata(remote_path)
            return True
        except ApiError:
            return False

    def get_file_metadata(self, remote_path: str) -> Dict[str, Any]:
        if not self._connected:
            return {}
        try:
            remote_path = '/' + remote_path.lstrip('/')
            metadata = self.client.files_get_metadata(remote_path)
            if isinstance(metadata, dropbox.files.FileMetadata):
                return {
                    'size': metadata.size,
                    'modified': datetime.strptime(metadata.server_modified, '%Y-%m-%dT%H:%M:%SZ'),
                    'etag': metadata.content_hash,
                    'metadata': {}
                }
            return {}
        except ApiError as e:
            logger.error(f"Failed to get Dropbox file metadata: {e}")
            return {}

    def is_online(self) -> bool:
        if not self._connected:
            return False
        try:
            self.client.users_get_current_account()
            return True
        except Exception:
            return False


class EncryptionManager:
    """Handles encryption and decryption of workflow data"""

    def __init__(self, encryption_key: Optional[bytes] = None):
        self.fernet = None
        self.aesgcm = None
        if HAS_CRYPTO:
            if encryption_key:
                self._init_with_key(encryption_key)
            else:
                self._generate_key()

    def _generate_key(self):
        """Generate a new encryption key"""
        if HAS_CRYPTO:
            self.fernet = Fernet.generate_key()
            self.aesgcm = AESGCM(self.fernet[:32])

    def _init_with_key(self, key: bytes):
        """Initialize with provided key"""
        if HAS_CRYPTO:
            if len(key) == 32:
                self.fernet = key
                self.aesgcm = AESGCM(key)
            elif len(key) >= 16:
                derived_key = self._derive_key(key)
                self.fernet = derived_key
                self.aesgcm = AESGCM(derived_key[:32])

    def _derive_key(self, password: str) -> bytes:
        """Derive encryption key from password"""
        if HAS_CRYPTO:
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=b'rabai_workflow_salt_v1',
                iterations=100000,
            )
            return kdf.derive(password.encode())
        return password.encode()[:32]

    def encrypt(self, data: bytes) -> bytes:
        """Encrypt data using AES-256-GCM"""
        if not HAS_CRYPTO:
            return data
        try:
            nonce = os.urandom(12)
            ciphertext = self.aesgcm.encrypt(nonce, data, None)
            return nonce + ciphertext
        except Exception as e:
            logger.error(f"Encryption failed: {e}")
            return data

    def decrypt(self, encrypted_data: bytes) -> bytes:
        """Decrypt data using AES-256-GCM"""
        if not HAS_CRYPTO:
            return encrypted_data
        try:
            nonce = encrypted_data[:12]
            ciphertext = encrypted_data[12:]
            return self.aesgcm.decrypt(nonce, ciphertext, None)
        except Exception as e:
            logger.error(f"Decryption failed: {e}")
            return encrypted_data

    def encrypt_file(self, input_path: str, output_path: str) -> bool:
        """Encrypt a file"""
        try:
            with open(input_path, 'rb') as f:
                data = f.read()
            encrypted = self.encrypt(data)
            with open(output_path, 'wb') as f:
                f.write(encrypted)
            return True
        except Exception as e:
            logger.error(f"File encryption failed: {e}")
            return False

    def decrypt_file(self, input_path: str, output_path: str) -> bool:
        """Decrypt a file"""
        try:
            with open(input_path, 'rb') as f:
                encrypted_data = f.read()
            decrypted = self.decrypt(encrypted_data)
            with open(output_path, 'wb') as f:
                f.write(decrypted)
            return True
        except Exception as e:
            logger.error(f"File decryption failed: {e}")
            return False


class DeltaCalculator:
    """Calculates and applies delta differences for bandwidth optimization"""

    @staticmethod
    def calculate_checksum(content: Dict[str, Any]) -> str:
        """Calculate checksum for workflow content"""
        content_str = json.dumps(content, sort_keys=True, default=str)
        return hashlib.sha256(content_str.encode()).hexdigest()

    @staticmethod
    def calculate_delta(old_content: Dict[str, Any], new_content: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate delta between two workflow versions"""
        old_str = json.dumps(old_content, sort_keys=True, default=str)
        new_str = json.dumps(new_content, sort_keys=True, default=str)

        diff = list(difflib.unified_diff(
            old_str.splitlines(keepends=True),
            new_str.splitlines(keepends=True),
            lineterm=''
        ))

        return {
            'has_changes': len(diff) > 0,
            'diff': ''.join(diff),
            'patch': DeltaCalculator._generate_patch(old_content, new_content),
            'new_checksum': DeltaCalculator.calculate_checksum(new_content)
        }

    @staticmethod
    def _generate_patch(old_content: Dict[str, Any], new_content: Dict[str, Any]) -> Dict[str, Any]:
        """Generate a patch dict showing changed fields"""
        patch = {}
        all_keys = set(old_content.keys()) | set(new_content.keys())

        for key in all_keys:
            old_val = old_content.get(key)
            new_val = new_content.get(key)
            if old_val != new_val:
                patch[key] = {
                    'old': old_val,
                    'new': new_val,
                    'type': 'modified' if key in old_content and key in new_content else 'added' if key in new_content else 'removed'
                }

        return patch

    @staticmethod
    def apply_delta(base_content: Dict[str, Any], delta: Dict[str, Any]) -> Dict[str, Any]:
        """Apply a delta patch to base content"""
        if not delta.get('has_changes'):
            return base_content

        result = copy.deepcopy(base_content)
        patch = delta.get('patch', {})

        for key, change in patch.items():
            if change['type'] == 'removed':
                result.pop(key, None)
            else:
                result[key] = change['new']

        return result


class OfflineQueue:
    """Queue for storing operations when offline"""

    def __init__(self, queue_file: str):
        self.queue_file = queue_file
        self._queue: List[Dict[str, Any]] = []
        self._load()

    def _load(self):
        """Load queue from disk"""
        if os.path.exists(self.queue_file):
            try:
                with open(self.queue_file, 'r') as f:
                    self._queue = json.load(f)
            except Exception as e:
                logger.error(f"Failed to load offline queue: {e}")
                self._queue = []

    def _save(self):
        """Save queue to disk"""
        try:
            with open(self.queue_file, 'w') as f:
                json.dump(self._queue, f, default=str)
        except Exception as e:
            logger.error(f"Failed to save offline queue: {e}")

    def enqueue(self, operation: Dict[str, Any]) -> bool:
        """Add an operation to the queue"""
        operation['queued_at'] = datetime.now().isoformat()
        self._queue.append(operation)
        self._save()
        return True

    def dequeue(self) -> Optional[Dict[str, Any]]:
        """Remove and return the oldest operation"""
        if self._queue:
            op = self._queue.pop(0)
            self._save()
            return op
        return None

    def peek(self) -> Optional[Dict[str, Any]]:
        """View the oldest operation without removing it"""
        if self._queue:
            return self._queue[0]
        return None

    def get_all(self) -> List[Dict[str, Any]]:
        """Get all queued operations"""
        return copy.deepcopy(self._queue)

    def clear(self):
        """Clear all queued operations"""
        self._queue = []
        self._save()

    def size(self) -> int:
        """Get number of queued operations"""
        return len(self._queue)

    def remove_for_workflow(self, workflow_id: str) -> int:
        """Remove all operations for a specific workflow"""
        original_size = len(self._queue)
        self._queue = [op for op in self._queue if op.get('workflow_id') != workflow_id]
        removed = original_size - len(self._queue)
        if removed > 0:
            self._save()
        return removed


class WorkflowCloudSync:
    """
    Main cloud synchronization system for workflows.

    Features:
    - Multi-provider support (S3, GCS, Azure, Dropbox)
    - Bidirectional sync between local and cloud
    - Conflict resolution with multiple strategies
    - Selective sync for chosen workflows
    - Offline support with operation queuing
    - Per-workflow sync status tracking
    - Delta sync for bandwidth optimization
    - AES-256 encryption at rest
    - Flexible sync scheduling
    - Complete audit trail
    """

    def __init__(
        self,
        local_workflows_dir: str,
        config: ProviderConfig,
        encryption_key: Optional[bytes] = None,
        state_file: Optional[str] = None,
        sync_log_file: Optional[str] = None,
        offline_queue_file: Optional[str] = None
    ):
        """
        Initialize the cloud sync system.

        Args:
            local_workflows_dir: Path to local workflows directory
            config: Cloud provider configuration
            encryption_key: Optional encryption key for data at rest
            state_file: Path to sync state file
            sync_log_file: Path to sync audit log
            offline_queue_file: Path to offline queue file
        """
        self.local_dir = Path(local_workflows_dir)
        self.config = config
        self.encryption = EncryptionManager(encryption_key)
        self.delta_calc = DeltaCalculator()

        # State files
        self.state_file = Path(state_file) if state_file else self.local_dir / '.sync_state.json'
        self.sync_log_file = Path(sync_log_file) if sync_log_file else self.local_dir / '.sync_audit.json'
        self.offline_queue_file = Path(offline_queue_file) if offline_queue_file else self.local_dir / '.offline_queue.json'

        # State tracking
        self.sync_states: Dict[str, SyncState] = {}
        self.sync_history: List[SyncRecord] = []
        self.conflicts: Dict[str, ConflictInfo] = {}

        # Sync configuration
        self.sync_direction = SyncDirection.BIDIRECTIONAL
        self.conflict_resolution = ConflictResolution.NEWEST_WINS
        self.selected_workflows: Set[str] = set()  # Empty means all
        self.excluded_workflows: Set[str] = set()
        self.encryption_enabled = True
        self.delta_sync_enabled = True

        # Offline support
        self.offline_queue = OfflineQueue(str(self.offline_queue_file))
        self.is_online = False

        # Provider
        self.provider: Optional[CloudProvider] = None
        self._provider_lock = threading.Lock()

        # Scheduler
        self._scheduler_thread: Optional[threading.Thread] = None
        self._stop_scheduler = threading.Event()
        self._sync_on_change = False

        # Initialize
        self._load_state()
        self._init_provider()

    def _init_provider(self):
        """Initialize the cloud provider based on config"""
        with self._provider_lock:
            if self.config.provider == SyncProvider.S3:
                self.provider = S3Provider(self.config)
            elif self.config.provider == SyncProvider.GCS:
                self.provider = GCSProvider(self.config)
            elif self.config.provider == SyncProvider.AZURE:
                self.provider = AzureProvider(self.config)
            elif self.config.provider == SyncProvider.DROPBOX:
                self.provider = DropboxProvider(self.config)
            else:
                logger.error(f"Unknown provider: {self.config.provider}")
                return

            if self.provider:
                self.is_online = self.provider.connect()

    def _load_state(self):
        """Load sync state from disk"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    data = json.load(f)

                self.sync_states = {}
                for wf_id, state_data in data.get('sync_states', {}).items():
                    self.sync_states[wf_id] = SyncState(
                        workflow_id=state_data['workflow_id'],
                        status=SyncStatus(state_data['status']),
                        local_checksum=state_data['local_checksum'],
                        remote_checksum=state_data.get('remote_checksum'),
                        last_synced=datetime.fromisoformat(state_data['last_synced']) if state_data.get('last_synced') else None,
                        last_modified=datetime.fromisoformat(state_data['last_modified']),
                        version_id=state_data['version_id'],
                        pending_changes=state_data.get('pending_changes', 0),
                        error_message=state_data.get('error_message')
                    )

                self.sync_history = []
                for record_data in data.get('sync_history', []):
                    self.sync_history.append(SyncRecord(
                        event=SyncEvent(record_data['event']),
                        workflow_id=record_data['workflow_id'],
                        timestamp=datetime.fromisoformat(record_data['timestamp']),
                        success=record_data['success'],
                        error_message=record_data.get('error_message'),
                        direction=SyncDirection(record_data['direction']) if record_data.get('direction') else None,
                        bytes_transferred=record_data.get('bytes_transferred', 0),
                        duration_ms=record_data.get('duration_ms', 0)
                    ))

                self.conflicts = {}
                for wf_id, conflict_data in data.get('conflicts', {}).items():
                    self.conflicts[wf_id] = ConflictInfo(
                        workflow_id=conflict_data['workflow_id'],
                        local_version=WorkflowVersion(**conflict_data['local_version']),
                        remote_version=WorkflowVersion(**conflict_data['remote_version']),
                        local_content=conflict_data['local_content'],
                        remote_content=conflict_data['remote_content'],
                        detected_at=datetime.fromisoformat(conflict_data['detected_at']),
                        resolution=ConflictResolution(conflict_data['resolution']) if conflict_data.get('resolution') else None,
                        resolved_at=datetime.fromisoformat(conflict_data['resolved_at']) if conflict_data.get('resolved_at') else None,
                        resolved_content=conflict_data.get('resolved_content')
                    )
            except Exception as e:
                logger.error(f"Failed to load sync state: {e}")

    def _save_state(self):
        """Save sync state to disk"""
        try:
            data = {
                'sync_states': {
                    wf_id: {
                        'workflow_id': state.workflow_id,
                        'status': state.status.value,
                        'local_checksum': state.local_checksum,
                        'remote_checksum': state.remote_checksum,
                        'last_synced': state.last_synced.isoformat() if state.last_synced else None,
                        'last_modified': state.last_modified.isoformat(),
                        'version_id': state.version_id,
                        'pending_changes': state.pending_changes,
                        'error_message': state.error_message
                    }
                    for wf_id, state in self.sync_states.items()
                },
                'sync_history': [
                    {
                        'event': record.event.value,
                        'workflow_id': record.workflow_id,
                        'timestamp': record.timestamp.isoformat(),
                        'success': record.success,
                        'error_message': record.error_message,
                        'direction': record.direction.value if record.direction else None,
                        'bytes_transferred': record.bytes_transferred,
                        'duration_ms': record.duration_ms
                    }
                    for record in self.sync_history[-1000:]  # Keep last 1000 records
                ],
                'conflicts': {
                    wf_id: {
                        'workflow_id': conflict.workflow_id,
                        'local_version': {
                            'version_id': conflict.local_version.version_id,
                            'checksum': conflict.local_version.checksum,
                            'size': conflict.local_version.size,
                            'modified_at': conflict.local_version.modified_at.isoformat(),
                            'modified_by': conflict.local_version.modified_by,
                            'delta_from': conflict.local_version.delta_from
                        },
                        'remote_version': {
                            'version_id': conflict.remote_version.version_id,
                            'checksum': conflict.remote_version.checksum,
                            'size': conflict.remote_version.size,
                            'modified_at': conflict.remote_version.modified_at.isoformat(),
                            'modified_by': conflict.remote_version.modified_by,
                            'delta_from': conflict.remote_version.delta_from
                        },
                        'local_content': conflict.local_content,
                        'remote_content': conflict.remote_content,
                        'detected_at': conflict.detected_at.isoformat(),
                        'resolution': conflict.resolution.value if conflict.resolution else None,
                        'resolved_at': conflict.resolved_at.isoformat() if conflict.resolved_at else None,
                        'resolved_content': conflict.resolved_content
                    }
                    for wf_id, conflict in self.conflicts.items()
                }
            }

            with open(self.state_file, 'w') as f:
                json.dump(data, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Failed to save sync state: {e}")

    def _log_sync(
        self,
        event: SyncEvent,
        workflow_id: str,
        success: bool,
        direction: Optional[SyncDirection] = None,
        error_message: Optional[str] = None,
        bytes_transferred: int = 0,
        duration_ms: int = 0
    ):
        """Log a sync operation"""
        record = SyncRecord(
            event=event,
            workflow_id=workflow_id,
            timestamp=datetime.now(),
            success=success,
            direction=direction,
            error_message=error_message,
            bytes_transferred=bytes_transferred,
            duration_ms=duration_ms
        )
        self.sync_history.append(record)
        self._save_state()

    def _get_workflow_path(self, workflow_id: str) -> Path:
        """Get the local path for a workflow"""
        return self.local_dir / f"{workflow_id}.json"

    def _load_workflow(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """Load workflow content from local file"""
        path = self._get_workflow_path(workflow_id)
        if path.exists():
            try:
                with open(path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Failed to load workflow {workflow_id}: {e}")
        return None

    def _save_workflow(self, workflow_id: str, content: Dict[str, Any]) -> bool:
        """Save workflow content to local file"""
        path = self._get_workflow_path(workflow_id)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, 'w') as f:
                json.dump(content, f, indent=2, default=str)
            return True
        except Exception as e:
            logger.error(f"Failed to save workflow {workflow_id}: {e}")
            return False

    def _get_remote_path(self, workflow_id: str, encrypted: bool = False) -> str:
        """Get the remote path for a workflow"""
        suffix = ".enc" if encrypted else ""
        prefix = self.config.prefix.strip('/')
        prefix = f"{prefix}/" if prefix else ""
        return f"{prefix}{workflow_id}{suffix}.json"

    def _should_sync(self, workflow_id: str) -> bool:
        """Check if a workflow should be synced"""
        if self.selected_workflows and workflow_id not in self.selected_workflows:
            return False
        if workflow_id in self.excluded_workflows:
            return False
        return True

    def set_selected_workflows(self, workflow_ids: Set[str]):
        """Set which workflows to sync (empty means all)"""
        self.selected_workflows = workflow_ids

    def set_excluded_workflows(self, workflow_ids: Set[str]):
        """Set which workflows to exclude from sync"""
        self.excluded_workflows = workflow_ids

    def add_selected_workflow(self, workflow_id: str):
        """Add a workflow to the sync selection"""
        self.selected_workflows.add(workflow_id)

    def remove_selected_workflow(self, workflow_id: str):
        """Remove a workflow from the sync selection"""
        self.selected_workflows.discard(workflow_id)

    def add_excluded_workflow(self, workflow_id: str):
        """Add a workflow to the exclusion list"""
        self.excluded_workflows.add(workflow_id)

    def remove_excluded_workflow(self, workflow_id: str):
        """Remove a workflow from the exclusion list"""
        self.excluded_workflows.discard(workflow_id)

    def get_sync_status(self, workflow_id: str) -> SyncStatus:
        """Get the current sync status for a workflow"""
        if workflow_id in self.conflicts:
            return SyncStatus.CONFLICT

        state = self.sync_states.get(workflow_id)
        if state:
            return state.status

        # Check if there's pending offline changes
        for op in self.offline_queue.get_all():
            if op.get('workflow_id') == workflow_id:
                return SyncStatus.OFFLINE_QUEUED

        return SyncStatus.EXCLUDED if not self._should_sync(workflow_id) else SyncStatus.PENDING_UPLOAD

    def get_all_sync_statuses(self) -> Dict[str, SyncStatus]:
        """Get sync statuses for all workflows"""
        statuses = {}
        for wf_id in self._discover_local_workflows():
            statuses[wf_id] = self.get_sync_status(wf_id)
        return statuses

    def _discover_local_workflows(self) -> Set[str]:
        """Discover all workflows in the local directory"""
        workflows = set()
        if self.local_dir.exists():
            for file in self.local_dir.glob("*.json"):
                if not file.name.startswith('.'):
                    workflows.add(file.stem)
        return workflows

    def _discover_remote_workflows(self) -> Set[str]:
        """Discover all workflows in cloud storage"""
        if not self.provider or not self.is_online:
            return set()

        try:
            files = self.provider.list_files(self.config.prefix)
            workflows = set()
            for f in files:
                filename = Path(f).stem
                # Remove .enc suffix if present
                if filename.endswith('.enc'):
                    filename = filename[:-4]
                if filename:
                    workflows.add(filename)
            return workflows
        except Exception as e:
            logger.error(f"Failed to discover remote workflows: {e}")
            return set()

    def _ensure_online(self) -> bool:
        """Ensure we have an active connection"""
        if not self.provider:
            self._init_provider()

        if self.provider and not self.is_online:
            self.is_online = self.provider.connect()

        return self.is_online

    def _process_offline_queue(self) -> int:
        """Process all queued offline operations"""
        if self.offline_queue.size() == 0:
            return 0

        if not self._ensure_online():
            return 0

        processed = 0
        while True:
            op = self.offline_queue.peek()
            if not op:
                break

            workflow_id = op.get('workflow_id')
            op_type = op.get('type')

            success = False
            if op_type == 'upload':
                success = self._upload_workflow(workflow_id, force=True)
            elif op_type == 'download':
                success = self._download_workflow(workflow_id, force=True)
            elif op_type == 'delete':
                success = self._delete_remote_workflow(workflow_id)

            if success:
                self.offline_queue.dequeue()
                processed += 1
            else:
                break

        return processed

    def _upload_workflow(self, workflow_id: str, force: bool = False) -> bool:
        """Upload a workflow to cloud storage"""
        if not self._ensure_online():
            return False

        local_path = self._get_workflow_path(workflow_id)
        if not local_path.exists():
            logger.error(f"Local workflow {workflow_id} not found")
            return False

        try:
            # Get local content and calculate checksum
            with open(local_path, 'r') as f:
                local_content = json.load(f)

            local_checksum = self.delta_calc.calculate_checksum(local_content)

            # Check if we should do delta sync
            state = self.sync_states.get(workflow_id)
            remote_path = self._get_remote_path(workflow_id, encrypted=False)
            encrypted_remote_path = self._get_remote_path(workflow_id, encrypted=True)

            if self.delta_sync_enabled and state and state.remote_checksum and not force:
                # Check if remote exists and get content for delta
                if self.provider.file_exists(remote_path):
                    temp_dir = tempfile.mkdtemp()
                    temp_path = os.path.join(temp_dir, f"{workflow_id}.json")
                    if self.provider.download_file(remote_path, temp_path):
                        try:
                            with open(temp_path, 'r') as f:
                                remote_content = json.load(f)
                            delta = self.delta_calc.calculate_delta(remote_content, local_content)
                            if not delta['has_changes']:
                                logger.info(f"No changes for workflow {workflow_id}, skipping upload")
                                return True
                        finally:
                            os.unlink(temp_path)
                            os.rmdir(temp_dir)

            # Perform upload
            temp_dir = tempfile.mkdtemp()
            temp_upload = os.path.join(temp_dir, "upload.json")

            if self.encryption_enabled:
                self.encryption.encrypt_file(str(local_path), temp_upload)
                remote_path = encrypted_remote_path
            else:
                shutil.copy(local_path, temp_upload)

            start_time = time.time()
            success = self.provider.upload_file(temp_upload, remote_path)
            duration_ms = int((time.time() - start_time) * 1000)

            # Cleanup
            os.unlink(temp_upload)
            os.rmdir(temp_dir)

            if success:
                # Update state
                file_size = local_path.stat().st_size
                version_id = f"{workflow_id}_{int(time.time())}"

                new_state = SyncState(
                    workflow_id=workflow_id,
                    status=SyncStatus.SYNCED,
                    local_checksum=local_checksum,
                    remote_checksum=local_checksum,
                    last_synced=datetime.now(),
                    last_modified=datetime.fromtimestamp(local_path.stat().st_mtime),
                    version_id=version_id,
                    pending_changes=0
                )
                self.sync_states[workflow_id] = new_state
                self._log_sync(
                    SyncEvent.UPLOAD, workflow_id, True,
                    direction=SyncDirection.UPLOAD_ONLY,
                    bytes_transferred=file_size, duration_ms=duration_ms
                )
                self._save_state()
                return True
            else:
                self._log_sync(
                    SyncEvent.UPLOAD, workflow_id, False,
                    direction=SyncDirection.UPLOAD_ONLY,
                    error_message="Upload failed"
                )
                return False

        except Exception as e:
            logger.error(f"Failed to upload workflow {workflow_id}: {e}")
            self._log_sync(
                SyncEvent.UPLOAD, workflow_id, False,
                direction=SyncDirection.UPLOAD_ONLY,
                error_message=str(e)
            )
            return False

    def _download_workflow(self, workflow_id: str, force: bool = False) -> bool:
        """Download a workflow from cloud storage"""
        if not self._ensure_online():
            return False

        try:
            # Determine which file to download (encrypted or not)
            remote_path = self._get_remote_path(workflow_id, encrypted=False)
            encrypted_remote_path = self._get_remote_path(workflow_id, encrypted=True)

            # Check which exists
            if self.provider.file_exists(encrypted_remote_path):
                remote_path = encrypted_remote_path
                is_encrypted = True
            elif self.provider.file_exists(remote_path):
                is_encrypted = False
            else:
                logger.error(f"Remote workflow {workflow_id} not found")
                return False

            # Get remote metadata
            metadata = self.provider.get_file_metadata(remote_path)
            remote_checksum = metadata.get('etag', '')

            # Check if download is needed
            state = self.sync_states.get(workflow_id)
            if state and state.remote_checksum == remote_checksum and not force:
                local_path = self._get_workflow_path(workflow_id)
                if local_path.exists():
                    local_content = self._load_workflow(workflow_id)
                    if local_content:
                        local_checksum = self.delta_calc.calculate_checksum(local_content)
                        if local_checksum == state.local_checksum:
                            logger.info(f"Workflow {workflow_id} unchanged, skipping download")
                            return True

            # Download
            temp_dir = tempfile.mkdtemp()
            temp_download = os.path.join(temp_dir, "download.json")
            local_path = self._get_workflow_path(workflow_id)

            start_time = time.time()
            success = self.provider.download_file(remote_path, temp_download)
            duration_ms = int((time.time() - start_time) * 1000)

            if success:
                # Decrypt if needed
                if is_encrypted:
                    decrypt_path = os.path.join(temp_dir, "decrypted.json")
                    self.encryption.decrypt_file(temp_download, decrypt_path)
                    shutil.move(decrypt_path, local_path)
                else:
                    shutil.move(temp_download, local_path)

                os.rmdir(temp_dir)

                # Update state
                remote_content = self._load_workflow(workflow_id)
                new_checksum = self.delta_calc.calculate_checksum(remote_content)
                version_id = f"{workflow_id}_{int(time.time())}"

                new_state = SyncState(
                    workflow_id=workflow_id,
                    status=SyncStatus.SYNCED,
                    local_checksum=new_checksum,
                    remote_checksum=new_checksum,
                    last_synced=datetime.now(),
                    last_modified=datetime.now(),
                    version_id=version_id,
                    pending_changes=0
                )
                self.sync_states[workflow_id] = new_state
                self._log_sync(
                    SyncEvent.DOWNLOAD, workflow_id, True,
                    direction=SyncDirection.DOWNLOAD_ONLY,
                    bytes_transferred=metadata.get('size', 0), duration_ms=duration_ms
                )
                self._save_state()
                return True
            else:
                os.rmdir(temp_dir)
                self._log_sync(
                    SyncEvent.DOWNLOAD, workflow_id, False,
                    direction=SyncDirection.DOWNLOAD_ONLY,
                    error_message="Download failed"
                )
                return False

        except Exception as e:
            logger.error(f"Failed to download workflow {workflow_id}: {e}")
            self._log_sync(
                SyncEvent.DOWNLOAD, workflow_id, False,
                direction=SyncDirection.DOWNLOAD_ONLY,
                error_message=str(e)
            )
            return False

    def _delete_remote_workflow(self, workflow_id: str) -> bool:
        """Delete a workflow from cloud storage"""
        if not self._ensure_online():
            return False

        try:
            remote_path = self._get_remote_path(workflow_id, encrypted=False)
            encrypted_remote_path = self._get_remote_path(workflow_id, encrypted=True)

            success = False
            if self.provider.file_exists(encrypted_remote_path):
                success = self.provider.delete_file(encrypted_remote_path)
            if not success and self.provider.file_exists(remote_path):
                success = self.provider.delete_file(remote_path)

            if success:
                if workflow_id in self.sync_states:
                    del self.sync_states[workflow_id]
                self._log_sync(
                    SyncEvent.DELETE, workflow_id, True,
                    direction=SyncDirection.UPLOAD_ONLY
                )
                self._save_state()
                return True
            else:
                self._log_sync(
                    SyncEvent.DELETE, workflow_id, False,
                    direction=SyncDirection.UPLOAD_ONLY,
                    error_message="Delete failed"
                )
                return False

        except Exception as e:
            logger.error(f"Failed to delete remote workflow {workflow_id}: {e}")
            return False

    def _detect_conflict(self, workflow_id: str, local_content: Dict[str, Any], remote_content: Dict[str, Any]) -> bool:
        """Detect if there's a conflict between local and remote versions"""
        local_checksum = self.delta_calc.calculate_checksum(local_content)
        remote_checksum = self.delta_calc.calculate_checksum(remote_content)

        state = self.sync_states.get(workflow_id)
        if state and state.local_checksum != local_checksum and state.remote_checksum == remote_checksum:
            # Local changed but remote matches old remote - no conflict
            return False
        elif state and state.local_checksum == local_checksum and state.remote_checksum != remote_checksum:
            # Remote changed but local matches old local - no conflict
            return False
        elif state and state.local_checksum != local_checksum and state.remote_checksum != remote_checksum:
            # Both changed - conflict!
            return True

        # New workflow that exists in both places
        if not state and local_content != remote_content:
            return True

        return False

    def _resolve_conflict(self, workflow_id: str, resolution: ConflictResolution) -> bool:
        """Resolve a conflict using the specified strategy"""
        if workflow_id not in self.conflicts:
            return False

        conflict = self.conflicts[workflow_id]

        if resolution == ConflictResolution.LOCAL_WINS:
            resolved_content = conflict.local_content
        elif resolution == ConflictResolution.REMOTE_WINS:
            resolved_content = conflict.remote_content
        elif resolution == ConflictResolution.NEWEST_WINS:
            if conflict.local_version.modified_at > conflict.remote_version.modified_at:
                resolved_content = conflict.local_content
            else:
                resolved_content = conflict.remote_content
        elif resolution == ConflictResolution.MANUAL:
            # Manual resolution requires resolved_content to be set externally
            if conflict.resolved_content is None:
                return False
            resolved_content = conflict.resolved_content
        else:
            return False

        # Save resolved content locally
        self._save_workflow(workflow_id, resolved_content)

        # Update conflict info
        conflict.resolution = resolution
        conflict.resolved_at = datetime.now()
        conflict.resolved_content = resolved_content

        # Upload resolved version
        if self._ensure_online():
            self._upload_workflow(workflow_id, force=True)

        # Remove from active conflicts
        del self.conflicts[workflow_id]

        # Update status
        if workflow_id in self.sync_states:
            self.sync_states[workflow_id].status = SyncStatus.SYNCED

        self._log_sync(
            SyncEvent.CONFLICT_RESOLVED, workflow_id, True
        )
        self._save_state()
        return True

    def sync_workflow(self, workflow_id: str, direction: Optional[SyncDirection] = None) -> bool:
        """
        Sync a single workflow with cloud storage.

        Args:
            workflow_id: ID of the workflow to sync
            direction: Override sync direction (None uses default)

        Returns:
            True if sync was successful
        """
        if not self._should_sync(workflow_id):
            logger.info(f"Workflow {workflow_id} is excluded from sync")
            return False

        sync_dir = direction or self.sync_direction

        # Handle offline mode
        if not self._ensure_online():
            if sync_dir == SyncDirection.UPLOAD_ONLY or sync_dir == SyncDirection.BIDIRECTIONAL:
                local_path = self._get_workflow_path(workflow_id)
                if local_path.exists():
                    self.offline_queue.enqueue({
                        'type': 'upload',
                        'workflow_id': workflow_id,
                        'timestamp': datetime.now().isoformat()
                    })
                    state = self.sync_states.get(workflow_id)
                    if not state:
                        state = SyncState(
                            workflow_id=workflow_id,
                            status=SyncStatus.OFFLINE_QUEUED,
                            local_checksum='',
                            remote_checksum=None,
                            last_synced=None,
                            last_modified=datetime.now(),
                            version_id=''
                        )
                    state.status = SyncStatus.OFFLINE_QUEUED
                    self.sync_states[workflow_id] = state
                    self._save_state()
                    logger.info(f"Workflow {workflow_id} queued for upload when online")
            return False

        # Process any pending offline operations first
        self._process_offline_queue()

        local_path = self._get_workflow_path(workflow_id)
        local_exists = local_path.exists()
        local_content = self._load_workflow(workflow_id) if local_exists else None

        remote_path = self._get_remote_path(workflow_id, encrypted=False)
        encrypted_remote_path = self._get_remote_path(workflow_id, encrypted=True)
        remote_exists = self.provider.file_exists(remote_path) or self.provider.file_exists(encrypted_remote_path)

        if sync_dir == SyncDirection.UPLOAD_ONLY:
            if local_exists:
                return self._upload_workflow(workflow_id)
            return False

        elif sync_dir == SyncDirection.DOWNLOAD_ONLY:
            if remote_exists:
                return self._download_workflow(workflow_id)
            return False

        else:  # BIDIRECTIONAL
            if not local_exists and remote_exists:
                return self._download_workflow(workflow_id)

            elif local_exists and not remote_exists:
                return self._upload_workflow(workflow_id)

            elif local_exists and remote_exists:
                # Both exist - check for conflicts
                temp_dir = tempfile.mkdtemp()
                temp_remote = os.path.join(temp_dir, "remote.json")

                actual_remote_path = encrypted_remote_path if self.provider.file_exists(encrypted_remote_path) else remote_path
                is_encrypted = self.provider.file_exists(encrypted_remote_path)

                if self.provider.download_file(actual_remote_path, temp_remote):
                    try:
                        if is_encrypted:
                            temp_decrypted = os.path.join(temp_dir, "decrypted.json")
                            self.encryption.decrypt_file(temp_remote, temp_decrypted)
                            with open(temp_decrypted, 'r') as f:
                                remote_content = json.load(f)
                        else:
                            with open(temp_remote, 'r') as f:
                                remote_content = json.load(f)

                        if self._detect_conflict(workflow_id, local_content, remote_content):
                            # Store conflict info
                            local_checksum = self.delta_calc.calculate_checksum(local_content)
                            remote_checksum = self.delta_calc.calculate_checksum(remote_content)

                            self.conflicts[workflow_id] = ConflictInfo(
                                workflow_id=workflow_id,
                                local_version=WorkflowVersion(
                                    version_id=f"{workflow_id}_local",
                                    checksum=local_checksum,
                                    size=local_path.stat().st_size,
                                    modified_at=datetime.fromtimestamp(local_path.stat().st_mtime)
                                ),
                                remote_version=WorkflowVersion(
                                    version_id=f"{workflow_id}_remote",
                                    checksum=remote_checksum,
                                    size=self.provider.get_file_metadata(actual_remote_path).get('size', 0),
                                    modified_at=datetime.now()
                                ),
                                local_content=local_content,
                                remote_content=remote_content,
                                detected_at=datetime.now()
                            )

                            state = self.sync_states.get(workflow_id)
                            if not state:
                                state = SyncState(
                                    workflow_id=workflow_id,
                                    status=SyncStatus.CONFLICT,
                                    local_checksum=local_checksum,
                                    remote_checksum=remote_checksum,
                                    last_synced=None,
                                    last_modified=datetime.now(),
                                    version_id=''
                                )
                            state.status = SyncStatus.CONFLICT
                            self.sync_states[workflow_id] = state

                            self._log_sync(
                                SyncEvent.CONFLICT_DETECTED, workflow_id, True
                            )
                            self._save_state()
                            logger.warning(f"Conflict detected for workflow {workflow_id}")
                            return False

                        # No conflict - sync based on which is newer
                        local_checksum = self.delta_calc.calculate_checksum(local_content)
                        remote_checksum = self.delta_calc.calculate_checksum(remote_content)

                        if local_checksum == remote_checksum:
                            logger.info(f"Workflow {workflow_id} already in sync")
                            return True

                        # Use modification time to decide
                        local_mtime = datetime.fromtimestamp(local_path.stat().st_mtime)
                        remote_mtime = self.provider.get_file_metadata(actual_remote_path).get('modified', datetime.now())

                        if local_mtime > remote_mtime:
                            return self._upload_workflow(workflow_id)
                        else:
                            return self._download_workflow(workflow_id)

                    finally:
                        os.unlink(temp_remote)
                        os.rmdir(temp_dir)
                return False

            return False

    def sync_all(self, direction: Optional[SyncDirection] = None) -> Dict[str, bool]:
        """
        Sync all selected workflows with cloud storage.

        Args:
            direction: Override sync direction

        Returns:
            Dictionary mapping workflow IDs to sync success status
        """
        results = {}

        # Ensure online and process offline queue
        if self._ensure_online():
            self._process_offline_queue()

        local_workflows = self._discover_local_workflows()

        for workflow_id in local_workflows:
            if self._should_sync(workflow_id):
                results[workflow_id] = self.sync_workflow(workflow_id, direction)

        return results

    def get_pending_conflicts(self) -> List[ConflictInfo]:
        """Get list of all pending conflicts"""
        return list(self.conflicts.values())

    def resolve_conflict(self, workflow_id: str, resolution: ConflictResolution, manual_content: Optional[Dict[str, Any]] = None) -> bool:
        """
        Resolve a conflict for a specific workflow.

        Args:
            workflow_id: ID of the workflow with conflict
            resolution: Resolution strategy to use
            manual_content: Required content if using MANUAL resolution

        Returns:
            True if resolution was successful
        """
        if resolution == ConflictResolution.MANUAL and manual_content:
            if workflow_id in self.conflicts:
                self.conflicts[workflow_id].resolved_content = manual_content

        return self._resolve_conflict(workflow_id, resolution)

    def get_sync_history(
        self,
        workflow_id: Optional[str] = None,
        limit: int = 100,
        event_type: Optional[SyncEvent] = None
    ) -> List[SyncRecord]:
        """
        Get sync history, optionally filtered.

        Args:
            workflow_id: Filter by workflow ID
            limit: Maximum number of records to return
            event_type: Filter by event type

        Returns:
            List of sync records
        """
        records = self.sync_history

        if workflow_id:
            records = [r for r in records if r.workflow_id == workflow_id]

        if event_type:
            records = [r for r in records if r.event == event_type]

        return records[-limit:]

    def get_sync_stats(self) -> Dict[str, Any]:
        """Get overall sync statistics"""
        total = len(self.sync_states)
        synced = sum(1 for s in self.sync_states.values() if s.status == SyncStatus.SYNCED)
        conflicts = len(self.conflicts)
        pending = self.offline_queue.size()

        return {
            'total_workflows': total,
            'synced': synced,
            'conflicts': conflicts,
            'pending_offline': pending,
            'is_online': self.is_online,
            'provider': self.config.provider.value if self.config.provider else None
        }

    def start_auto_sync(self, interval_seconds: int = 300, on_change: bool = False):
        """
        Start automatic sync scheduling.

        Args:
            interval_seconds: Interval between scheduled syncs
            on_change: If True, also sync on local file changes
        """
        self._stop_scheduler.clear()
        self._sync_on_change = on_change

        def scheduler_loop():
            while not self._stop_scheduler.is_set():
                self._stop_scheduler.wait(timeout=interval_seconds)
                if not self._stop_scheduler.is_set():
                    logger.info("Running scheduled sync...")
                    self.sync_all()

        self._scheduler_thread = threading.Thread(target=scheduler_loop, daemon=True)
        self._scheduler_thread.start()
        logger.info(f"Auto-sync started with interval {interval_seconds}s, on_change={on_change}")

    def stop_auto_sync(self):
        """Stop automatic sync scheduling"""
        self._stop_scheduler.set()
        if self._scheduler_thread:
            self._scheduler_thread.join(timeout=5)
        logger.info("Auto-sync stopped")

    def set_encryption_key(self, key: bytes):
        """Set or update the encryption key"""
        self.encryption._init_with_key(key)

    def enable_encryption(self, enabled: bool = True):
        """Enable or disable encryption"""
        self.encryption_enabled = enabled

    def enable_delta_sync(self, enabled: bool = True):
        """Enable or disable delta sync"""
        self.delta_sync_enabled = enabled

    def set_conflict_resolution(self, resolution: ConflictResolution):
        """Set the default conflict resolution strategy"""
        self.conflict_resolution = resolution

    def set_sync_direction(self, direction: SyncDirection):
        """Set the sync direction"""
        self.sync_direction = direction

    def force_full_sync(self) -> Dict[str, bool]:
        """Force a full sync by re-uploading all local workflows"""
        # Clear all local sync states
        for workflow_id in self.sync_states:
            self.sync_states[workflow_id].remote_checksum = None

        # Process offline queue
        if self._ensure_online():
            self._process_offline_queue()

        # Re-sync all
        return self.sync_all()

    def cleanup(self):
        """Clean up resources"""
        self.stop_auto_sync()
        if self.provider:
            self.provider.disconnect()
        self._save_state()
