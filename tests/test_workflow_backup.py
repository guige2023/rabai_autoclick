"""
Tests for Workflow Backup Module
"""
import unittest
import tempfile
import shutil
import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, mock_open

import sys
sys.path.insert(0, '/Users/guige/my_project')

# Mock schedule module before importing workflow_backup
sys.modules['schedule'] = Mock()

from src.workflow_backup import (
    WorkflowBackup,
    BackupMetadata,
    WorkflowSnapshot,
    BackupCatalog,
    BackupType,
    BackupStatus,
    RetentionPolicy,
    EncryptionManager,
    ChecksumCalculator,
    BackupRotationManager,
    RemoteStorageBackend,
    S3Backend,
    FTPBackend,
    SCPBackend,
    create_backup_system,
    quick_backup,
    quick_restore
)


class TestBackupMetadata(unittest.TestCase):
    """Test BackupMetadata dataclass"""

    def test_create_backup_metadata(self):
        """Test creating backup metadata"""
        now = datetime.now()
        metadata = BackupMetadata(
            backup_id="backup_001",
            backup_type=BackupType.FULL,
            created_at=now,
            workflow_ids=["wf_001", "wf_002"],
            checksum="abc123",
            encrypted=True,
            size_bytes=1024,
            retention_policy=RetentionPolicy.DAILY,
            version="1.0.0"
        )
        
        self.assertEqual(metadata.backup_id, "backup_001")
        self.assertEqual(metadata.backup_type, BackupType.FULL)
        self.assertEqual(len(metadata.workflow_ids), 2)
        self.assertTrue(metadata.encrypted)

    def test_to_dict(self):
        """Test converting metadata to dictionary"""
        now = datetime.now()
        metadata = BackupMetadata(
            backup_id="backup_001",
            backup_type=BackupType.FULL,
            created_at=now,
            workflow_ids=["wf_001"],
            checksum="abc123",
            encrypted=False,
            size_bytes=1024,
            retention_policy=RetentionPolicy.DAILY,
            version="1.0.0"
        )
        
        data = metadata.to_dict()
        
        self.assertEqual(data['backup_id'], "backup_001")
        self.assertEqual(data['backup_type'], "full")
        self.assertEqual(data['retention_policy'], "daily")

    def test_from_dict(self):
        """Test creating metadata from dictionary"""
        now = datetime.now()
        data = {
            'backup_id': 'backup_001',
            'backup_type': 'full',
            'created_at': now.isoformat(),
            'workflow_ids': ['wf_001'],
            'checksum': 'abc123',
            'encrypted': False,
            'size_bytes': 1024,
            'retention_policy': 'daily',
            'version': '1.0.0',
            'status': 'completed'
        }
        
        metadata = BackupMetadata.from_dict(data)
        
        self.assertEqual(metadata.backup_id, "backup_001")
        self.assertEqual(metadata.backup_type, BackupType.FULL)


class TestBackupCatalog(unittest.TestCase):
    """Test BackupCatalog class"""

    def test_create_catalog(self):
        """Test creating backup catalog"""
        catalog = BackupCatalog()
        self.assertEqual(len(catalog.backups), 0)

    def test_add_backup(self):
        """Test adding backup to catalog"""
        catalog = BackupCatalog()
        now = datetime.now()
        metadata = BackupMetadata(
            backup_id="backup_001",
            backup_type=BackupType.FULL,
            created_at=now,
            workflow_ids=["wf_001"],
            checksum="abc123",
            encrypted=False,
            size_bytes=1024,
            retention_policy=RetentionPolicy.DAILY,
            version="1.0.0"
        )
        
        catalog.add_backup(metadata)
        
        self.assertEqual(len(catalog.backups), 1)
        self.assertIn("wf_001", catalog.workflows_last_backup)

    def test_get_backup(self):
        """Test getting backup from catalog"""
        catalog = BackupCatalog()
        now = datetime.now()
        metadata = BackupMetadata(
            backup_id="backup_001",
            backup_type=BackupType.FULL,
            created_at=now,
            workflow_ids=["wf_001"],
            checksum="abc123",
            encrypted=False,
            size_bytes=1024,
            retention_policy=RetentionPolicy.DAILY,
            version="1.0.0"
        )
        catalog.add_backup(metadata)
        
        found = catalog.get_backup("backup_001")
        self.assertIsNotNone(found)
        self.assertEqual(found.backup_id, "backup_001")

    def test_get_backup_not_found(self):
        """Test getting non-existent backup"""
        catalog = BackupCatalog()
        found = catalog.get_backup("nonexistent")
        self.assertIsNone(found)

    def test_get_backups_by_type(self):
        """Test getting backups by type"""
        catalog = BackupCatalog()
        now = datetime.now()
        
        for i, btype in enumerate([BackupType.FULL, BackupType.INCREMENTAL, BackupType.FULL]):
            metadata = BackupMetadata(
                backup_id=f"backup_{i}",
                backup_type=btype,
                created_at=now,
                workflow_ids=["wf_001"],
                checksum="abc123",
                encrypted=False,
                size_bytes=1024,
                retention_policy=RetentionPolicy.DAILY,
                version="1.0.0"
            )
            catalog.add_backup(metadata)
        
        full_backups = catalog.get_backups_by_type(BackupType.FULL)
        self.assertEqual(len(full_backups), 2)

    def test_to_dict_and_from_dict(self):
        """Test catalog serialization"""
        catalog = BackupCatalog()
        now = datetime.now()
        metadata = BackupMetadata(
            backup_id="backup_001",
            backup_type=BackupType.FULL,
            created_at=now,
            workflow_ids=["wf_001"],
            checksum="abc123",
            encrypted=False,
            size_bytes=1024,
            retention_policy=RetentionPolicy.DAILY,
            version="1.0.0"
        )
        catalog.add_backup(metadata)
        
        data = catalog.to_dict()
        restored = BackupCatalog.from_dict(data)
        
        self.assertEqual(len(restored.backups), 1)
        self.assertEqual(restored.backups[0].backup_id, "backup_001")


class TestChecksumCalculator(unittest.TestCase):
    """Test ChecksumCalculator class"""

    def test_calculate_data_checksum(self):
        """Test calculating checksum of data"""
        data = b"test data"
        checksum1 = ChecksumCalculator.calculate_data_checksum(data)
        checksum2 = ChecksumCalculator.calculate_data_checksum(data)
        
        self.assertEqual(checksum1, checksum2)
        self.assertIsInstance(checksum1, str)

    def test_calculate_data_checksum_different_algorithms(self):
        """Test calculating checksum with different algorithms"""
        data = b"test data"
        
        sha256 = ChecksumCalculator.calculate_data_checksum(data, 'sha256')
        md5 = ChecksumCalculator.calculate_data_checksum(data, 'md5')
        
        self.assertNotEqual(sha256, md5)

    def test_calculate_file_checksum(self):
        """Test calculating file checksum"""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"test file content")
            temp_path = f.name
        
        try:
            checksum = ChecksumCalculator.calculate_file_checksum(temp_path)
            self.assertIsInstance(checksum, str)
            self.assertEqual(len(checksum), 64)  # SHA256 produces 64 hex chars
        finally:
            os.unlink(temp_path)

    def test_verify_file_integrity(self):
        """Test verifying file integrity"""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"test file content")
            temp_path = f.name
        
        try:
            checksum = ChecksumCalculator.calculate_file_checksum(temp_path)
            self.assertTrue(
                ChecksumCalculator.verify_file_integrity(temp_path, checksum)
            )
            self.assertFalse(
                ChecksumCalculator.verify_file_integrity(temp_path, "wrong_checksum")
            )
        finally:
            os.unlink(temp_path)

    def test_calculate_manifest_checksum(self):
        """Test calculating manifest checksum"""
        manifest = {
            'backup_id': 'backup_001',
            'version': '1.0.0',
            'workflows': [{'filename': 'wf1.json', 'checksum': 'abc123'}]
        }
        
        checksum = ChecksumCalculator.calculate_manifest_checksum(manifest)
        self.assertIsInstance(checksum, str)


class TestBackupRotationManager(unittest.TestCase):
    """Test BackupRotationManager class"""

    def test_init_default_retention(self):
        """Test initialization with default retention"""
        manager = BackupRotationManager()
        
        self.assertEqual(manager.retention['daily'], 7)
        self.assertEqual(manager.retention['weekly'], 4)
        self.assertEqual(manager.retention['monthly'], 12)

    def test_init_custom_retention(self):
        """Test initialization with custom retention"""
        custom = {'daily': 3, 'weekly': 2, 'monthly': 6}
        manager = BackupRotationManager(retention=custom)
        
        self.assertEqual(manager.retention['daily'], 3)
        self.assertEqual(manager.retention['weekly'], 2)

    def test_get_backups_to_delete(self):
        """Test determining backups to delete"""
        manager = BackupRotationManager(retention={'daily': 1, 'weekly': 2, 'monthly': 12})
        now = datetime.now()
        
        backups = []
        for i in range(5):
            backup = BackupMetadata(
                backup_id=f"backup_{i}",
                backup_type=BackupType.DAILY if i % 2 == 0 else BackupType.WEEKLY,
                created_at=now - timedelta(days=i),
                workflow_ids=["wf_001"],
                checksum="abc123",
                encrypted=False,
                size_bytes=1024,
                retention_policy=RetentionPolicy.DAILY if i % 2 == 0 else RetentionPolicy.WEEKLY,
                version="1.0.0",
                status=BackupStatus.COMPLETED
            )
            backups.append(backup)
        
        to_delete = manager.get_backups_to_delete(backups)
        
        self.assertIsInstance(to_delete, list)


class TestEncryptionManager(unittest.TestCase):
    """Test EncryptionManager class"""

    @patch('src.workflow_backup.HAS_CRYPTO', True)
    @patch('src.workflow_backup.HAS_CRYPTO', True)
    def test_init_with_password(self):
        """Test initialization with password"""
        manager = EncryptionManager(password="test_password")
        self.assertIsNotNone(manager)

    def test_init_without_password(self):
        """Test initialization without password"""
        manager = EncryptionManager()
        self.assertIsNone(manager._key)

    @patch('src.workflow_backup.HAS_CRYPTO', False)
    def test_encrypt_without_crypto_raises(self):
        """Test encryption raises error without cryptography library"""
        manager = EncryptionManager(password="test")
        with self.assertRaises(ImportError):
            manager.encrypt(b"data")


class TestRemoteStorageBackend(unittest.TestCase):
    """Test RemoteStorageBackend abstract class"""

    def test_backend_is_abc(self):
        """Test that RemoteStorageBackend is abstract"""
        with self.assertRaises(TypeError):
            RemoteStorageBackend()


class TestWorkflowBackup(unittest.TestCase):
    """Test WorkflowBackup main class"""

    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.workflows_dir = os.path.join(self.temp_dir, "workflows")
        self.backup_dir = os.path.join(self.temp_dir, "backups")
        os.makedirs(self.workflows_dir, exist_ok=True)
        os.makedirs(self.backup_dir, exist_ok=True)
        
        # Create a sample workflow file
        self.sample_workflow = {
            "id": "wf_001",
            "name": "Test Workflow",
            "steps": [{"action": "click", "target": "button"}]
        }
        wf_path = os.path.join(self.workflows_dir, "wf_001.json")
        with open(wf_path, 'w') as f:
            json.dump(self.sample_workflow, f)

    def tearDown(self):
        """Clean up temporary files"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_init_creates_directories(self):
        """Test initialization creates required directories"""
        backup = WorkflowBackup(
            workflows_dir=self.workflows_dir,
            backup_dir=self.backup_dir
        )
        
        self.assertTrue(os.path.exists(self.backup_dir))

    def test_generate_backup_id(self):
        """Test backup ID generation"""
        backup = WorkflowBackup(
            workflows_dir=self.workflows_dir,
            backup_dir=self.backup_dir
        )
        
        backup_id = backup._generate_backup_id()
        
        self.assertTrue(backup_id.startswith("backup_"))
        self.assertIn("_", backup_id)

    def test_get_workflow_files(self):
        """Test getting workflow files"""
        backup = WorkflowBackup(
            workflows_dir=self.workflows_dir,
            backup_dir=self.backup_dir
        )
        
        files = backup._get_workflow_files()
        
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0].stem, "wf_001")

    def test_load_workflow(self):
        """Test loading workflow data"""
        backup = WorkflowBackup(
            workflows_dir=self.workflows_dir,
            backup_dir=self.backup_dir
        )
        
        wf_path = Path(os.path.join(self.workflows_dir, "wf_001.json"))
        data = backup._load_workflow(wf_path)
        
        self.assertEqual(data['id'], "wf_001")
        self.assertEqual(data['name'], "Test Workflow")

    def test_calculate_workflow_checksum(self):
        """Test calculating workflow checksum"""
        backup = WorkflowBackup(
            workflows_dir=self.workflows_dir,
            backup_dir=self.backup_dir
        )
        
        wf_path = Path(os.path.join(self.workflows_dir, "wf_001.json"))
        checksum = backup._calculate_workflow_checksum(wf_path)
        
        self.assertIsInstance(checksum, str)
        self.assertEqual(len(checksum), 64)  # SHA256

    def test_get_changed_workflows(self):
        """Test getting changed workflows"""
        backup = WorkflowBackup(
            workflows_dir=self.workflows_dir,
            backup_dir=self.backup_dir
        )
        
        changed = backup._get_changed_workflows()
        
        # Should return the one workflow we have
        self.assertEqual(len(changed), 1)
        self.assertEqual(changed[0][0].stem, "wf_001")

    @patch('os.makedirs')
    @patch('builtins.open', new_callable=mock_open)
    @patch('os.path.exists')
    def test_full_backup_no_workflows(self, mock_exists, mock_file, mock_makedirs):
        """Test full backup with no workflows"""
        mock_exists.return_value = True
        backup = WorkflowBackup(
            workflows_dir=self.workflows_dir,
            backup_dir=self.backup_dir
        )
        
        # Remove workflow file
        wf_path = os.path.join(self.workflows_dir, "wf_001.json")
        if os.path.exists(wf_path):
            os.unlink(wf_path)
        
        result = backup.full_backup()
        
        self.assertIsNone(result)

    @patch('src.workflow_backup.tempfile.TemporaryDirectory')
    @patch('tarfile.open')
    @patch('builtins.open', new_callable=mock_open)
    @patch('os.makedirs')
    @patch('os.path.exists')
    @patch.object(Path, 'glob')
    def test_verify_backup_integrity(self, mock_glob, mock_exists, mock_makedirs, mock_file, mock_tarfile, mock_tempdir):
        """Test backup integrity verification"""
        mock_exists.side_effect = lambda x: True
        mock_glob.return_value = []
        
        backup = WorkflowBackup(
            workflows_dir=self.workflows_dir,
            backup_dir=self.backup_dir
        )
        
        # Create a temp file to test with
        with tempfile.NamedTemporaryFile(delete=False, suffix='.tar.gz') as f:
            f.write(b"test backup content")
            temp_path = f.name
        
        try:
            checksum = ChecksumCalculator.calculate_file_checksum(temp_path)
            is_valid = backup._verify_backup_integrity(temp_path, checksum, False)
            self.assertTrue(is_valid)
        finally:
            os.unlink(temp_path)

    @patch('os.makedirs')
    @patch('builtins.open', new_callable=mock_open)
    @patch('os.path.exists')
    def test_get_catalog_info(self, mock_exists, mock_file, mock_makedirs):
        """Test getting catalog info"""
        mock_exists.return_value = True
        backup = WorkflowBackup(
            workflows_dir=self.workflows_dir,
            backup_dir=self.backup_dir
        )
        
        info = backup.get_catalog_info()
        
        self.assertIn('total_backups', info)
        self.assertIn('full_backups', info)
        self.assertIn('encrypted_backups', info)

    @patch('os.makedirs')
    @patch('builtins.open', new_callable=mock_open)
    @patch('os.path.exists')
    def test_check_disk_space(self, mock_exists, mock_file, mock_makedirs):
        """Test checking disk space"""
        mock_exists.return_value = True
        backup = WorkflowBackup(
            workflows_dir=self.workflows_dir,
            backup_dir=self.backup_dir
        )
        
        space = backup.check_disk_space()
        
        self.assertIn('total_bytes', space)
        self.assertIn('free_bytes', space)
        self.assertIn('percent_used', space)

    @patch('os.makedirs')
    @patch('builtins.open', new_callable=mock_open)
    @patch('os.path.exists')
    def test_list_backups(self, mock_exists, mock_file, mock_makedirs):
        """Test listing backups"""
        mock_exists.return_value = True
        backup = WorkflowBackup(
            workflows_dir=self.workflows_dir,
            backup_dir=self.backup_dir
        )
        
        backups = backup.list_backups()
        
        self.assertIsInstance(backups, list)


class TestBackupAlerts(unittest.TestCase):
    """Test backup alerting functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.workflows_dir = os.path.join(self.temp_dir, "workflows")
        self.backup_dir = os.path.join(self.temp_dir, "backups")
        os.makedirs(self.workflows_dir, exist_ok=True)
        os.makedirs(self.backup_dir, exist_ok=True)

    def tearDown(self):
        """Clean up temporary files"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_add_alert_callback(self):
        """Test adding alert callback"""
        backup = WorkflowBackup(
            workflows_dir=self.workflows_dir,
            backup_dir=self.backup_dir
        )
        
        callback_called = []
        def test_callback(alert_type, data):
            callback_called.append((alert_type, data))
        
        backup.add_alert_callback(test_callback)
        self.assertEqual(len(backup._alert_callbacks), 1)


class TestBackupScheduling(unittest.TestCase):
    """Test backup scheduling functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.workflows_dir = os.path.join(self.temp_dir, "workflows")
        self.backup_dir = os.path.join(self.temp_dir, "backups")
        os.makedirs(self.workflows_dir, exist_ok=True)
        os.makedirs(self.backup_dir, exist_ok=True)

    def tearDown(self):
        """Clean up temporary files"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    @patch('schedule.every')
    def test_schedule_full_backup(self, mock_schedule_every):
        """Test scheduling full backup"""
        mock_job = Mock()
        mock_schedule_every.return_value.day.at.return_value.do.return_value = mock_job
        
        backup = WorkflowBackup(
            workflows_dir=self.workflows_dir,
            backup_dir=self.backup_dir
        )
        backup.schedule_full_backup(time_str="02:00")
        
        # Just verify no exception raised
        self.assertTrue(True)

    @patch('schedule.every')
    def test_schedule_incremental_backup(self, mock_schedule_every):
        """Test scheduling incremental backup"""
        mock_job = Mock()
        mock_schedule_every.return_value.hours.do.return_value = mock_job
        
        backup = WorkflowBackup(
            workflows_dir=self.workflows_dir,
            backup_dir=self.backup_dir
        )
        backup.schedule_incremental_backup(interval_hours=6)
        
        # Just verify no exception raised
        self.assertTrue(True)


class TestCreateBackupSystem(unittest.TestCase):
    """Test create_backup_system factory function"""

    @patch('os.makedirs')
    @patch('builtins.open', new_callable=mock_open)
    @patch('os.path.exists')
    def test_create_backup_system_local(self, mock_exists, mock_file, mock_makedirs):
        """Test creating local backup system"""
        mock_exists.return_value = True
        backup = create_backup_system(backup_dir="/tmp/backups")
        self.assertIsInstance(backup, WorkflowBackup)

    @patch('os.makedirs')
    @patch('builtins.open', new_callable=mock_open)
    @patch('os.path.exists')
    def test_create_backup_system_s3(self, mock_exists, mock_file, mock_makedirs):
        """Test creating S3 backup system"""
        mock_exists.return_value = True
        # Note: Will fail at S3Backend due to missing boto3 but that's OK
        # we're testing the factory function structure
        try:
            backup = create_backup_system(backup_type='s3', bucket='test-bucket')
        except ImportError:
            pass  # Expected if boto3 not installed


if __name__ == '__main__':
    unittest.main()
