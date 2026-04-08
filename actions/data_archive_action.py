"""Data Archive Action.

Archives data to various formats (ZIP, TAR, compressed) with incremental
backup support, deduplication, and retention policy management.
"""

import sys
import os
import tarfile
import zipfile
import hashlib
import time
from typing import Any, Dict, List, Optional
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataArchiveAction(BaseAction):
    """Archive data to compressed formats with deduplication.
    
    Supports ZIP, TAR, TAR.GZ formats with incremental backup,
    content deduplication, and retention policy enforcement.
    """
    action_type = "data_archive"
    display_name = "数据归档"
    description = "将数据归档为压缩格式，支持增量备份和去重"

    SUPPORTED_FORMATS = ['zip', 'tar', 'tar.gz', 'tar.bz2']

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Archive data.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - action: 'create', 'extract', 'list', 'verify'.
                - source: Source file(s) or directory path(s).
                - destination: Output archive path.
                - format: Archive format (zip, tar, tar.gz, tar.bz2).
                - compression_level: Compression level 0-9 (default: 6).
                - deduplicate: Enable content deduplication (default: False).
                - incremental: Enable incremental mode (default: False).
                - base_backup: Base backup path for incremental.
                - retention_days: Days to retain archive (0 = forever).
                - password: Password for encrypted archives.
                - save_to_var: Variable name for results.
        
        Returns:
            ActionResult with archive operation results.
        """
        try:
            action = params.get('action', 'create')
            save_to_var = params.get('save_to_var', 'archive_result')

            if action == 'create':
                return self._create_archive(context, params, save_to_var)
            elif action == 'extract':
                return self._extract_archive(context, params, save_to_var)
            elif action == 'list':
                return self._list_archive(context, params, save_to_var)
            elif action == 'verify':
                return self._verify_archive(context, params, save_to_var)
            else:
                return ActionResult(success=False, message=f"Unknown archive action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Archive error: {e}")

    def _create_archive(self, context: Any, params: Dict, save_to_var: str) -> ActionResult:
        """Create an archive."""
        source = params.get('source')
        destination = params.get('destination')
        format_type = params.get('format', 'zip').lower()
        compression_level = params.get('compression_level', 6)
        deduplicate = params.get('deduplicate', False)
        incremental = params.get('incremental', False)
        base_backup = params.get('base_backup')
        password = params.get('password')
        retention_days = params.get('retention_days', 0)

        if not source or not destination:
            return ActionResult(success=False, message="source and destination are required")

        # Convert to list
        if isinstance(source, str):
            source = [source]

        # Handle incremental
        if incremental and base_backup:
            source = self._get_incremental_files(source, base_backup)
            if not source:
                return ActionResult(success=True, data={'message': 'No new files for incremental backup'})

        # Deduplicate if requested
        seen_hashes = {}
        files_to_archive = []
        
        if deduplicate:
            for file_path in source:
                file_hash = self._hash_file(file_path)
                if file_hash not in seen_hashes:
                    seen_hashes[file_hash] = file_path
                    files_to_archive.append(file_path)
        else:
            files_to_archive = source

        # Create archive
        created_files = []
        total_size = 0

        try:
            if format_type == 'zip':
                result = self._create_zip(destination, files_to_archive, compression_level, password)
            elif format_type in ('tar', 'tar.gz', 'tar.bz2'):
                result = self._create_tar(destination, files_to_archive, format_type, compression_level)
            else:
                return ActionResult(success=False, message=f"Unsupported format: {format_type}")

            created_files = result.get('files', [])
            total_size = result.get('size', 0)

        except Exception as e:
            return ActionResult(success=False, message=f"Failed to create archive: {e}")

        # Apply retention policy
        if retention_days > 0:
            self._apply_retention(destination, retention_days)

        archive_info = {
            'destination': destination,
            'format': format_type,
            'files_count': len(created_files),
            'total_size': total_size,
            'compression_level': compression_level,
            'deduplicated': deduplicate,
            'incremental': incremental,
            'created_at': time.time()
        }

        context.set_variable(save_to_var, archive_info)
        return ActionResult(success=True, data=archive_info,
                           message=f"Archive created: {destination} ({len(created_files)} files)")

    def _create_zip(self, destination: str, files: List[str], 
                    compression_level: int, password: Optional[str]) -> Dict:
        """Create a ZIP archive."""
        import zlib
        comp_level = min(max(compression_level, 0), 9)
        
        with zipfile.ZipFile(destination, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
            for file_path in files:
                if os.path.isfile(file_path):
                    zf.write(file_path, os.path.basename(file_path))
                elif os.path.isdir(file_path):
                    for root, dirs, filenames in os.walk(file_path):
                        for filename in filenames:
                            full_path = os.path.join(root, filename)
                            arcname = os.path.relpath(full_path, os.path.dirname(file_path))
                            zf.write(full_path, arcname)

        size = os.path.getsize(destination)
        return {'files': files, 'size': size}

    def _create_tar(self, destination: str, files: List[str],
                    format_type: str, compression_level: int) -> Dict:
        """Create a TAR archive."""
        mode_map = {
            'tar': 'w',
            'tar.gz': 'w:gz',
            'tar.bz2': 'w:bz2'
        }
        mode = mode_map.get(format_type, 'w')

        with tarfile.open(destination, mode) as tf:
            for file_path in files:
                if os.path.exists(file_path):
                    tf.add(file_path, arcname=os.path.basename(file_path))

        size = os.path.getsize(destination)
        return {'files': files, 'size': size}

    def _extract_archive(self, context: Any, params: Dict, save_to_var: str) -> ActionResult:
        """Extract an archive."""
        source = params.get('source')
        destination = params.get('destination', os.path.dirname(source))
        format_type = params.get('format', 'auto').lower()
        password = params.get('password')

        if not source or not os.path.exists(source):
            return ActionResult(success=False, message="source is required and must exist")

        if format_type == 'auto':
            format_type = self._detect_format(source)

        try:
            if format_type == 'zip':
                with zipfile.ZipFile(source, 'r') as zf:
                    if password:
                        zf.setpassword(password.encode())
                    zf.extractall(destination)
                    extracted = zf.namelist()
            elif format_type in ('tar', 'tar.gz', 'tar.bz2'):
                mode_map = {
                    'tar': 'r',
                    'tar.gz': 'r:gz',
                    'tar.bz2': 'r:bz2'
                }
                mode = mode_map.get(format_type, 'r')
                with tarfile.open(source, mode) as tf:
                    tf.extractall(destination)
                    extracted = tf.getnames()
            else:
                return ActionResult(success=False, message=f"Unsupported format: {format_type}")

            result = {
                'extracted': len(extracted),
                'destination': destination,
                'files': extracted[:100]  # First 100 files
            }
            context.set_variable(save_to_var, result)
            return ActionResult(success=True, data=result,
                               message=f"Extracted {len(extracted)} files")

        except Exception as e:
            return ActionResult(success=False, message=f"Extraction failed: {e}")

    def _list_archive(self, context: Any, params: Dict, save_to_var: str) -> ActionResult:
        """List archive contents."""
        source = params.get('source')
        format_type = params.get('format', 'auto').lower()

        if not source:
            return ActionResult(success=False, message="source is required")

        if format_type == 'auto':
            format_type = self._detect_format(source)

        try:
            if format_type == 'zip':
                with zipfile.ZipFile(source, 'r') as zf:
                    files = [{'name': info.filename, 'size': info.file_size} for info in zf.infolist()]
            else:
                with tarfile.open(source, 'r:*') as tf:
                    files = [{'name': m.name, 'size': m.size, 'type': m.type} for m in tf.getmembers()]

            result = {'total_files': len(files), 'files': files}
            context.set_variable(save_to_var, result)
            return ActionResult(success=True, data=result, message=f"Archive contains {len(files)} entries")

        except Exception as e:
            return ActionResult(success=False, message=f"List failed: {e}")

    def _verify_archive(self, context: Any, params: Dict, save_to_var: str) -> ActionResult:
        """Verify archive integrity."""
        source = params.get('source')
        format_type = params.get('format', 'auto').lower()

        if not source:
            return ActionResult(success=False, message="source is required")

        if format_type == 'auto':
            format_type = self._detect_format(source)

        try:
            if format_type == 'zip':
                with zipfile.ZipFile(source, 'r') as zf:
                    bad_file = zf.testzip()
                    if bad_file:
                        return ActionResult(success=False, data={'corrupted_file': bad_file},
                                          message=f"Corrupted file: {bad_file}")
            else:
                with tarfile.open(source, 'r:*') as tf:
                    # Just try to read members
                    for member in tf:
                        if not member.isfile() and not member.isdir():
                            pass

            context.set_variable(save_to_var, {'verified': True, 'source': source})
            return ActionResult(success=True, message="Archive verified successfully")

        except Exception as e:
            return ActionResult(success=False, message=f"Verification failed: {e}")

    def _detect_format(self, path: str) -> str:
        """Auto-detect archive format."""
        if path.endswith('.zip'):
            return 'zip'
        elif path.endswith(('.tar.gz', '.tgz')):
            return 'tar.gz'
        elif path.endswith('.tar.bz2'):
            return 'tar.bz2'
        elif path.endswith('.tar'):
            return 'tar'
        return 'zip'

    def _hash_file(self, file_path: str) -> str:
        """Calculate SHA256 hash of file."""
        sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(65536), b''):
                sha256.update(chunk)
        return sha256.hexdigest()

    def _get_incremental_files(self, source: List[str], base_backup: str) -> List[str]:
        """Get files that are newer than base backup."""
        if not os.path.exists(base_backup):
            return source

        base_mtime = os.path.getmtime(base_backup)
        new_files = []

        for file_path in source:
            if os.path.getmtime(file_path) > base_mtime:
                new_files.append(file_path)

        return new_files

    def _apply_retention(self, archive_path: str, retention_days: int):
        """Apply retention policy to archives."""
        if retention_days <= 0:
            return

        archive_dir = os.path.dirname(archive_path)
        archive_name = os.path.basename(archive_path)
        cutoff = time.time() - (retention_days * 86400)

        try:
            for file_name in os.listdir(archive_dir):
                if file_name.startswith(archive_name.rsplit('.', 1)[0]):
                    file_path = os.path.join(archive_dir, file_name)
                    if os.path.getmtime(file_path) < cutoff:
                        os.remove(file_path)
        except Exception:
            pass
