"""Hash action module for RabAI AutoClick.

Provides cryptographic hash operations:
- HashMd5Action: Calculate MD5 hash
- HashSha256Action: Calculate SHA256 hash
- HashSha512Action: Calculate SHA512 hash
- HashHmacAction: Calculate HMAC
- HashBlake2Action: Calculate BLAKE2 hash
- HashPasswordAction: Hash password
- HashVerifyAction: Verify hash
- HashFileAction: Calculate file hash
"""

import hashlib
import hmac
import os
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class HashMd5Action(BaseAction):
    """Calculate MD5 hash."""
    action_type = "hash_md5"
    display_name = "MD5哈希"
    description = "计算MD5哈希值"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute MD5.

        Args:
            context: Execution context.
            params: Dict with data, encoding, output_var.

        Returns:
            ActionResult with hash.
        """
        data = params.get('data', '')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'hash_md5')

        valid, msg = self.validate_type(data, str, 'data')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_data = context.resolve_value(data)
            resolved_enc = context.resolve_value(encoding)

            hash_val = hashlib.md5(str(resolved_data).encode(resolved_enc)).hexdigest()
            context.set(output_var, hash_val)

            return ActionResult(
                success=True,
                message=f"MD5: {hash_val}",
                data={'hash': hash_val, 'algorithm': 'md5', 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"MD5计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'encoding': 'utf-8', 'output_var': 'hash_md5'}


class HashSha256Action(BaseAction):
    """Calculate SHA256 hash."""
    action_type = "hash_sha256"
    display_name = "SHA256哈希"
    description = "计算SHA256哈希值"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute SHA256.

        Args:
            context: Execution context.
            params: Dict with data, encoding, output_var.

        Returns:
            ActionResult with hash.
        """
        data = params.get('data', '')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'hash_sha256')

        valid, msg = self.validate_type(data, str, 'data')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_data = context.resolve_value(data)
            resolved_enc = context.resolve_value(encoding)

            hash_val = hashlib.sha256(str(resolved_data).encode(resolved_enc)).hexdigest()
            context.set(output_var, hash_val)

            return ActionResult(
                success=True,
                message=f"SHA256: {hash_val[:16]}...",
                data={'hash': hash_val, 'algorithm': 'sha256', 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"SHA256计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'encoding': 'utf-8', 'output_var': 'hash_sha256'}


class HashSha512Action(BaseAction):
    """Calculate SHA512 hash."""
    action_type = "hash_sha512"
    display_name = "SHA512哈希"
    description = "计算SHA512哈希值"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute SHA512.

        Args:
            context: Execution context.
            params: Dict with data, encoding, output_var.

        Returns:
            ActionResult with hash.
        """
        data = params.get('data', '')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'hash_sha512')

        valid, msg = self.validate_type(data, str, 'data')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_data = context.resolve_value(data)
            resolved_enc = context.resolve_value(encoding)

            hash_val = hashlib.sha512(str(resolved_data).encode(resolved_enc)).hexdigest()
            context.set(output_var, hash_val)

            return ActionResult(
                success=True,
                message=f"SHA512: {hash_val[:16]}...",
                data={'hash': hash_val, 'algorithm': 'sha512', 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"SHA512计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'encoding': 'utf-8', 'output_var': 'hash_sha512'}


class HashHmacAction(BaseAction):
    """Calculate HMAC."""
    action_type = "hash_hmac"
    display_name = "HMAC哈希"
    description = "计算HMAC"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HMAC.

        Args:
            context: Execution context.
            params: Dict with data, key, algorithm, encoding, output_var.

        Returns:
            ActionResult with HMAC.
        """
        data = params.get('data', '')
        key = params.get('key', '')
        algorithm = params.get('algorithm', 'sha256')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'hash_hmac')

        valid, msg = self.validate_type(data, str, 'data')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_data = context.resolve_value(data)
            resolved_key = context.resolve_value(key)
            resolved_algo = context.resolve_value(algorithm)
            resolved_enc = context.resolve_value(encoding)

            algo_map = {
                'md5': 'md5', 'sha1': 'sha1', 'sha256': 'sha256',
                'sha384': 'sha384', 'sha512': 'sha512'
            }
            algo = algo_map.get(resolved_algo, 'sha256')

            hash_val = hmac.new(
                resolved_key.encode(resolved_enc),
                str(resolved_data).encode(resolved_enc),
                getattr(hashlib, algo)
            ).hexdigest()

            context.set(output_var, hash_val)

            return ActionResult(
                success=True,
                message=f"HMAC-{resolved_algo.upper()}: {hash_val[:16]}...",
                data={'hash': hash_val, 'algorithm': resolved_algo, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HMAC计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'algorithm': 'sha256', 'encoding': 'utf-8', 'output_var': 'hash_hmac'}


class HashBlake2Action(BaseAction):
    """Calculate BLAKE2 hash."""
    action_type = "hash_blake2"
    display_name = "BLAKE2哈希"
    description = "计算BLAKE2哈希值"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute BLAKE2.

        Args:
            context: Execution context.
            params: Dict with data, digest_size, key, encoding, output_var.

        Returns:
            ActionResult with hash.
        """
        data = params.get('data', '')
        digest_size = params.get('digest_size', 32)
        key = params.get('key', '')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'hash_blake2')

        valid, msg = self.validate_type(data, str, 'data')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_data = context.resolve_value(data)
            resolved_size = context.resolve_value(digest_size)
            resolved_key = context.resolve_value(key) if key else None
            resolved_enc = context.resolve_value(encoding)

            if resolved_key:
                hash_val = hashlib.blake2b(
                    str(resolved_data).encode(resolved_enc),
                    key=resolved_key.encode(resolved_enc),
                    digest_size=int(resolved_size)
                ).hexdigest()
            else:
                hash_val = hashlib.blake2b(
                    str(resolved_data).encode(resolved_enc),
                    digest_size=int(resolved_size)
                ).hexdigest()

            context.set(output_var, hash_val)

            return ActionResult(
                success=True,
                message=f"BLAKE2b: {hash_val[:16]}...",
                data={'hash': hash_val, 'algorithm': 'blake2b', 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"BLAKE2计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'digest_size': 32, 'key': '', 'encoding': 'utf-8', 'output_var': 'hash_blake2'}


class HashPasswordAction(BaseAction):
    """Hash password."""
    action_type = "hash_password"
    display_name = "密码哈希"
    description = "使用bcrypt哈希密码"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute password hash.

        Args:
            context: Execution context.
            params: Dict with password, rounds, output_var.

        Returns:
            ActionResult with hash.
        """
        password = params.get('password', '')
        rounds = params.get('rounds', 12)
        output_var = params.get('output_var', 'password_hash')

        valid, msg = self.validate_type(password, str, 'password')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import bcrypt

            resolved_pwd = context.resolve_value(password)
            resolved_rounds = context.resolve_value(rounds)

            salt = bcrypt.gensalt(rounds=int(resolved_rounds))
            hashed = bcrypt.hashpw(resolved_pwd.encode('utf-8'), salt).decode('utf-8')

            context.set(output_var, hashed)

            return ActionResult(
                success=True,
                message=f"密码已哈希",
                data={'hash': hashed, 'output_var': output_var}
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="bcrypt未安装: pip install bcrypt"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"密码哈希失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['password']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'rounds': 12, 'output_var': 'password_hash'}


class HashVerifyAction(BaseAction):
    """Verify hash."""
    action_type = "hash_verify"
    display_name = "验证哈希"
    description = "验证密码哈希"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute verify.

        Args:
            context: Execution context.
            params: Dict with password, hash, output_var.

        Returns:
            ActionResult with verification result.
        """
        password = params.get('password', '')
        hash_val = params.get('hash', '')
        output_var = params.get('output_var', 'hash_valid')

        valid, msg = self.validate_type(password, str, 'password')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(hash_val, str, 'hash')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import bcrypt

            resolved_pwd = context.resolve_value(password)
            resolved_hash = context.resolve_value(hash_val)

            valid = bcrypt.checkpw(resolved_pwd.encode('utf-8'), resolved_hash.encode('utf-8'))
            context.set(output_var, valid)

            return ActionResult(
                success=True,
                message=f"密码验证: {'通过' if valid else '失败'}",
                data={'valid': valid, 'output_var': output_var}
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="bcrypt未安装"
            )
        except Exception as e:
            context.set(output_var, False)
            return ActionResult(
                success=False,
                message=f"密码验证失败: {str(e)}",
                data={'valid': False, 'output_var': output_var}
            )

    def get_required_params(self) -> List[str]:
        return ['password', 'hash']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'hash_valid'}


class HashFileAction(BaseAction):
    """Calculate file hash."""
    action_type = "hash_file"
    display_name = "文件哈希"
    description = "计算文件哈希值"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute file hash.

        Args:
            context: Execution context.
            params: Dict with file_path, algorithm, output_var.

        Returns:
            ActionResult with hash.
        """
        file_path = params.get('file_path', '')
        algorithm = params.get('algorithm', 'sha256')
        output_var = params.get('output_var', 'file_hash')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import hashlib

            resolved_path = context.resolve_value(file_path)
            resolved_algo = context.resolve_value(algorithm)

            if not os.path.exists(resolved_path):
                return ActionResult(
                    success=False,
                    message=f"文件不存在: {resolved_path}"
                )

            algo_map = {
                'md5': hashlib.md5, 'sha1': hashlib.sha1,
                'sha256': hashlib.sha256, 'sha512': hashlib.sha512
            }

            if resolved_algo not in algo_map:
                return ActionResult(
                    success=False,
                    message=f"不支持的算法: {resolved_algo}"
                )

            hasher = algo_map[resolved_algo]()

            with open(resolved_path, 'rb') as f:
                for chunk in iter(lambda: f.read(8192), b''):
                    hasher.update(chunk)

            hash_val = hasher.hexdigest()
            context.set(output_var, hash_val)

            return ActionResult(
                success=True,
                message=f"文件{resolved_algo}: {hash_val[:16]}...",
                data={'hash': hash_val, 'algorithm': resolved_algo, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"文件哈希计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'algorithm': 'sha256', 'output_var': 'file_hash'}
