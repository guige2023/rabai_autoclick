"""Compression action module for RabAI AutoClick.

Provides advanced compression operations:
- CompressionZipAction: Create zip archive with options
- CompressionUnzipAction: Extract zip with options
- CompressionTarAction: Create tar archive
- CompressionuntarAction: Extract tar archive
- CompressionGzipAction: Gzip compress file
- CompressionGunzipAction: Gunzip decompress file
- CompressionBzip2Action: Bzip2 compress file
- CompressionSevenZipAction: Create 7z archive
- CompressionListAction: List archive contents
- CompressionInfoAction: Get archive info
"""

import os
import subprocess
import tarfile
import zipfile
import gzip
import bz2
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CompressionZipAction(BaseAction):
    """Create zip archive with options."""
    action_type = "compression_zip"
    display_name = "创建ZIP压缩包"
    description = "创建带选项的ZIP压缩包"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute zip.

        Args:
            context: Execution context.
            params: Dict with source, output, compression_level, include_hidden.

        Returns:
            ActionResult with output path.
        """
        source = params.get('source', '')
        output = params.get('output', '')
        compression_level = params.get('compression_level', 6)
        include_hidden = params.get('include_hidden', False)

        valid, msg = self.validate_type(source, str, 'source')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(output, str, 'output')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_source = context.resolve_value(source)
            resolved_output = context.resolve_value(output)
            resolved_level = context.resolve_value(compression_level)
            resolved_hidden = context.resolve_value(include_hidden)

            if not os.path.exists(resolved_source):
                return ActionResult(
                    success=False,
                    message=f"源路径不存在: {resolved_source}"
                )

            is_dir = os.path.isdir(resolved_source)

            if is_dir:
                with zipfile.ZipFile(resolved_output, 'w', zipfile.ZIP_DEFLATED, compresslevel=int(resolved_level)) as zf:
                    for root, dirs, files in os.walk(resolved_source):
                        if not resolved_hidden:
                            dirs[:] = [d for d in dirs if not d.startswith('.')]
                            files = [f for f in files if not f.startswith('.')]

                        for file in files:
                            file_path = os.path.join(root, file)
                            arcname = os.path.relpath(file_path, os.path.dirname(resolved_source))
                            zf.write(file_path, arcname)
            else:
                with zipfile.ZipFile(resolved_output, 'w', zipfile.ZIP_DEFLATED, compresslevel=int(resolved_level)) as zf:
                    zf.write(resolved_source, os.path.basename(resolved_source))

            size = os.path.getsize(resolved_output)

            return ActionResult(
                success=True,
                message=f"ZIP已创建: {resolved_output} ({size} bytes)",
                data={'path': resolved_output, 'size': size}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建ZIP失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['source', 'output']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'compression_level': 6, 'include_hidden': False}


class CompressionUnzipAction(BaseAction):
    """Extract zip with options."""
    action_type = "compression_unzip"
    display_name = "解压ZIP文件"
    description = "解压ZIP文件到目录"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute unzip.

        Args:
            context: Execution context.
            params: Dict with archive, output_dir, pattern.

        Returns:
            ActionResult indicating success.
        """
        archive = params.get('archive', '')
        output_dir = params.get('output_dir', '')
        pattern = params.get('pattern', '')

        valid, msg = self.validate_type(archive, str, 'archive')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_archive = context.resolve_value(archive)
            resolved_output = context.resolve_value(output_dir) if output_dir else os.path.splitext(resolved_archive)[0]

            if not os.path.exists(resolved_archive):
                return ActionResult(
                    success=False,
                    message=f"压缩包不存在: {resolved_archive}"
                )

            os.makedirs(resolved_output, exist_ok=True)

            with zipfile.ZipFile(resolved_archive, 'r') as zf:
                if pattern:
                    names = [n for n in zf.namelist() if pattern in n]
                else:
                    names = zf.namelist()

                zf.extractall(resolved_output)

            return ActionResult(
                success=True,
                message=f"已解压 {len(names)} 个文件到 {resolved_output}",
                data={'output_dir': resolved_output, 'files': len(names)}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"解压失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['archive']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_dir': '', 'pattern': ''}


class CompressionTarAction(BaseAction):
    """Create tar archive."""
    action_type = "compression_tar"
    display_name = "创建TAR压缩包"
    description = "创建tar归档文件"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute tar.

        Args:
            context: Execution context.
            params: Dict with source, output, compression, include_hidden.

        Returns:
            ActionResult with output path.
        """
        source = params.get('source', '')
        output = params.get('output', '')
        compression = params.get('compression', 'none')
        include_hidden = params.get('include_hidden', False)

        valid, msg = self.validate_type(source, str, 'source')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(output, str, 'output')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_source = context.resolve_value(source)
            resolved_output = context.resolve_value(output)
            resolved_compression = context.resolve_value(compression)
            resolved_hidden = context.resolve_value(include_hidden)

            if not os.path.exists(resolved_source):
                return ActionResult(
                    success=False,
                    message=f"源路径不存在: {resolved_source}"
                )

            mode_map = {
                'none': 'w',
                'gz': 'w:gz',
                'gzip': 'w:gz',
                'bz2': 'w:bz2',
                'xz': 'w:xz'
            }
            mode = mode_map.get(resolved_compression, 'w')

            with tarfile.open(resolved_output, mode) as tf:
                if os.path.isdir(resolved_source):
                    for root, dirs, files in os.walk(resolved_source):
                        if not resolved_hidden:
                            dirs[:] = [d for d in dirs if not d.startswith('.')]
                            files = [f for f in files if not f.startswith('.')]

                        for file in files:
                            file_path = os.path.join(root, file)
                            arcname = os.path.relpath(file_path, os.path.dirname(resolved_source))
                            tf.add(file_path, arcname)
                else:
                    tf.add(resolved_source, os.path.basename(resolved_source))

            size = os.path.getsize(resolved_output)

            return ActionResult(
                success=True,
                message=f"TAR已创建: {resolved_output} ({size} bytes)",
                data={'path': resolved_output, 'size': size}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建TAR失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['source', 'output']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'compression': 'none', 'include_hidden': False}


class CompressionUntarAction(BaseAction):
    """Extract tar archive."""
    action_type = "compression_untar"
    display_name = "解压TAR文件"
    description = "解压tar归档文件"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute untar.

        Args:
            context: Execution context.
            params: Dict with archive, output_dir.

        Returns:
            ActionResult indicating success.
        """
        archive = params.get('archive', '')
        output_dir = params.get('output_dir', '')

        valid, msg = self.validate_type(archive, str, 'archive')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_archive = context.resolve_value(archive)
            resolved_output = context.resolve_value(output_dir) if output_dir else '.'

            if not os.path.exists(resolved_archive):
                return ActionResult(
                    success=False,
                    message=f"压缩包不存在: {resolved_archive}"
                )

            os.makedirs(resolved_output, exist_ok=True)

            with tarfile.open(resolved_archive, 'r:*') as tf:
                members = tf.getmembers()
                tf.extractall(resolved_output)

            return ActionResult(
                success=True,
                message=f"已解压 {len(members)} 个文件到 {resolved_output}",
                data={'output_dir': resolved_output, 'files': len(members)}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"解压TAR失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['archive']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_dir': ''}


class CompressionGzipAction(BaseAction):
    """Gzip compress file."""
    action_type = "compression_gzip"
    display_name = "GZIP压缩"
    description = "使用GZIP压缩文件"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute gzip.

        Args:
            context: Execution context.
            params: Dict with file_path, output_path.

        Returns:
            ActionResult with output path.
        """
        file_path = params.get('file_path', '')
        output_path = params.get('output_path', '')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_input = context.resolve_value(file_path)
            resolved_output = context.resolve_value(output_path) if output_path else f"{resolved_input}.gz"

            if not os.path.exists(resolved_input):
                return ActionResult(
                    success=False,
                    message=f"文件不存在: {resolved_input}"
                )

            with open(resolved_input, 'rb') as f_in:
                with gzip.open(resolved_output, 'wb') as f_out:
                    f_out.writelines(f_in)

            orig_size = os.path.getsize(resolved_input)
            comp_size = os.path.getsize(resolved_output)
            ratio = (1 - comp_size / orig_size) * 100 if orig_size > 0 else 0

            return ActionResult(
                success=True,
                message=f"GZIP压缩: {orig_size} -> {comp_size} bytes ({ratio:.1f}% 压缩)",
                data={'output': resolved_output, 'orig_size': orig_size, 'comp_size': comp_size}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"GZIP压缩失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_path': ''}


class CompressionGunzipAction(BaseAction):
    """Gunzip decompress file."""
    action_type = "compression_gunzip"
    display_name = "GZIP解压"
    description = "解压GZIP文件"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute gunzip.

        Args:
            context: Execution context.
            params: Dict with file_path, output_path.

        Returns:
            ActionResult with output path.
        """
        file_path = params.get('file_path', '')
        output_path = params.get('output_path', '')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_input = context.resolve_value(file_path)
            base_name = resolved_input
            if resolved_input.endswith('.gz'):
                base_name = resolved_input[:-3]
            resolved_output = context.resolve_value(output_path) if output_path else base_name

            if not os.path.exists(resolved_input):
                return ActionResult(
                    success=False,
                    message=f"文件不存在: {resolved_input}"
                )

            with gzip.open(resolved_input, 'rb') as f_in:
                with open(resolved_output, 'wb') as f_out:
                    f_out.writelines(f_in)

            size = os.path.getsize(resolved_output)

            return ActionResult(
                success=True,
                message=f"GZIP解压: {resolved_output} ({size} bytes)",
                data={'output': resolved_output, 'size': size}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"GZIP解压失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_path': ''}


class CompressionBzip2Action(BaseAction):
    """Bzip2 compress file."""
    action_type = "compression_bzip2"
    display_name = "BZIP2压缩"
    description = "使用BZIP2压缩文件"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute bzip2.

        Args:
            context: Execution context.
            params: Dict with file_path, output_path.

        Returns:
            ActionResult with output path.
        """
        file_path = params.get('file_path', '')
        output_path = params.get('output_path', '')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_input = context.resolve_value(file_path)
            resolved_output = context.resolve_value(output_path) if output_path else f"{resolved_input}.bz2"

            if not os.path.exists(resolved_input):
                return ActionResult(
                    success=False,
                    message=f"文件不存在: {resolved_input}"
                )

            with open(resolved_input, 'rb') as f_in:
                with bz2.open(resolved_output, 'wb') as f_out:
                    f_out.writelines(f_in)

            orig_size = os.path.getsize(resolved_input)
            comp_size = os.path.getsize(resolved_output)
            ratio = (1 - comp_size / orig_size) * 100 if orig_size > 0 else 0

            return ActionResult(
                success=True,
                message=f"BZIP2压缩: {orig_size} -> {comp_size} bytes ({ratio:.1f}% 压缩)",
                data={'output': resolved_output, 'orig_size': orig_size, 'comp_size': comp_size}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"BZIP2压缩失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_path': ''}


class CompressionSevenZipAction(BaseAction):
    """Create 7z archive."""
    action_type = "compression_seven_zip"
    display_name = "创建7z压缩包"
    description = "创建7z压缩包"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute 7z.

        Args:
            context: Execution context.
            params: Dict with source, output, compression_level, password.

        Returns:
            ActionResult with output path.
        """
        source = params.get('source', '')
        output = params.get('output', '')
        compression_level = params.get('compression_level', 5)
        password = params.get('password', '')

        valid, msg = self.validate_type(source, str, 'source')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(output, str, 'output')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_source = context.resolve_value(source)
            resolved_output = context.resolve_value(output)
            resolved_level = context.resolve_value(compression_level)
            resolved_pwd = context.resolve_value(password) if password else ''

            if not os.path.exists(resolved_source):
                return ActionResult(
                    success=False,
                    message=f"源路径不存在: {resolved_source}"
                )

            cmd = ['7z', 'a', '-y']

            if resolved_pwd:
                cmd.append(f'-p{resolved_pwd}')

            cmd.extend([f'-mx={resolved_level}', resolved_output, resolved_source])

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

            if result.returncode != 0:
                return ActionResult(
                    success=False,
                    message=f"7z创建失败: {result.stderr}"
                )

            size = os.path.getsize(resolved_output)

            return ActionResult(
                success=True,
                message=f"7z已创建: {resolved_output} ({size} bytes)",
                data={'path': resolved_output, 'size': size}
            )
        except FileNotFoundError:
            return ActionResult(
                success=False,
                message="7z未安装: brew install p7zip"
            )
        except subprocess.TimeoutExpired:
            return ActionResult(
                success=False,
                message="7z创建超时"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"7z创建失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['source', 'output']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'compression_level': 5, 'password': ''}


class CompressionListAction(BaseAction):
    """List archive contents."""
    action_type = "compression_list"
    display_name = "列出压缩包内容"
    description = "列出压缩包内的文件"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list.

        Args:
            context: Execution context.
            params: Dict with archive, output_var.

        Returns:
            ActionResult with file list.
        """
        archive = params.get('archive', '')
        output_var = params.get('output_var', 'archive_contents')

        valid, msg = self.validate_type(archive, str, 'archive')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_archive = context.resolve_value(archive)

            if not os.path.exists(resolved_archive):
                return ActionResult(
                    success=False,
                    message=f"压缩包不存在: {resolved_archive}"
                )

            files = []
            ext = os.path.splitext(resolved_archive)[1].lower()

            if ext == '.zip':
                with zipfile.ZipFile(resolved_archive, 'r') as zf:
                    for info in zf.infolist():
                        files.append({
                            'name': info.filename,
                            'size': info.file_size,
                            'compressed_size': info.compress_size,
                            'date_time': info.date_time
                        })

            elif ext in ('.tar', '.tar.gz', '.tgz', '.tar.bz2', '.tar.xz'):
                mode = 'r:*'
                if ext.endswith('.gz'):
                    mode = 'r:gz'
                elif ext.endswith('.bz2'):
                    mode = 'r:bz2'

                with tarfile.open(resolved_archive, mode) as tf:
                    for info in tf.getmembers():
                        files.append({
                            'name': info.name,
                            'size': info.size,
                            'type': 'dir' if info.isdir() else 'file',
                            'mtime': info.mtime
                        })

            elif ext == '.gz':
                return ActionResult(
                    success=True,
                    message="GZIP单文件压缩包",
                    data={'files': [{'name': resolved_archive, 'type': 'gzip'}]}
                )

            context.set(output_var, files)

            return ActionResult(
                success=True,
                message=f"列出 {len(files)} 个文件",
                data={'count': len(files), 'files': files, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列出压缩包内容失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['archive']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'archive_contents'}


class CompressionInfoAction(BaseAction):
    """Get archive info."""
    action_type = "compression_info"
    display_name = "获取压缩包信息"
    description = "获取压缩包的详细信息"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute info.

        Args:
            context: Execution context.
            params: Dict with archive, output_var.

        Returns:
            ActionResult with archive info.
        """
        archive = params.get('archive', '')
        output_var = params.get('output_var', 'archive_info')

        valid, msg = self.validate_type(archive, str, 'archive')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_archive = context.resolve_value(archive)

            if not os.path.exists(resolved_archive):
                return ActionResult(
                    success=False,
                    message=f"压缩包不存在: {resolved_archive}"
                )

            stat = os.stat(resolved_archive)
            ext = os.path.splitext(resolved_archive)[1].lower()

            info = {
                'path': resolved_archive,
                'size': stat.st_size,
                'modified': stat.st_mtime,
                'format': ext.lstrip('.'),
                'is_archive': True
            }

            # Count files
            if ext == '.zip':
                with zipfile.ZipFile(resolved_archive, 'r') as zf:
                    info['file_count'] = len(zf.namelist())
                    info['total_size'] = sum(i.file_size for i in zf.infolist())

            elif ext.startswith('.tar'):
                mode = 'r:*'
                if 'gz' in ext:
                    mode = 'r:gz'
                elif 'bz2' in ext:
                    mode = 'r:bz2'

                with tarfile.open(resolved_archive, mode) as tf:
                    members = tf.getmembers()
                    info['file_count'] = len(members)
                    info['total_size'] = sum(m.size for m in members if not m.isdir())

            context.set(output_var, info)

            return ActionResult(
                success=True,
                message=f"压缩包信息: {info['size']} bytes, {info.get('file_count', '?')} files",
                data=info
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取压缩包信息失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['archive']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'archive_info'}
