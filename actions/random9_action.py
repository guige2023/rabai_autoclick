"""Extended random operations module for RabAI AutoClick.

Provides additional random operations:
- RandomGaussAction: Gaussian/normal distribution
- RandomExpovariateAction: Exponential distribution
- RandomGammavariateAction: Gamma distribution
- RandomBetavariateAction: Beta distribution
- RandomParetovariateAction: Pareto distribution
- RandomWeibullvariateAction: Weibull distribution
- RandomBytesAction: Generate random bytes
- RandomPasswordAction: Generate secure random password
"""

from typing import Any, Dict, List

import random
import string
import secrets

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RandomGaussAction(BaseAction):
    """Generate Gaussian (normal) distributed random number."""
    action_type = "random_gauss"
    display_name = "高斯分布随机数"
    description = "生成正态分布随机数"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Gaussian random.

        Args:
            context: Execution context.
            params: Dict with mu (mean), sigma (std dev), output_var.

        Returns:
            ActionResult with Gaussian random number.
        """
        mu = params.get('mu', 0.0)
        sigma = params.get('sigma', 1.0)
        output_var = params.get('output_var', 'random_gauss')

        valid, msg = self.validate_type(mu, (int, float), 'mu')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_mu = context.resolve_value(mu)
            resolved_sigma = context.resolve_value(sigma)

            if resolved_sigma <= 0:
                return ActionResult(success=False, message="sigma必须大于0")

            result = random.gauss(resolved_mu, resolved_sigma)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"高斯随机数: {result:.6f}",
                data={
                    'value': result,
                    'mu': resolved_mu,
                    'sigma': resolved_sigma,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"高斯随机数生成失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'mu': 0.0, 'sigma': 1.0, 'output_var': 'random_gauss'}


class RandomExpovariateAction(BaseAction):
    """Generate exponentially distributed random number."""
    action_type = "random_expovariate"
    display_name = "指数分布随机数"
    description = "生成指数分布随机数"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute exponential random.

        Args:
            context: Execution context.
            params: Dict with lambda_param, output_var.

        Returns:
            ActionResult with exponential random number.
        """
        lambda_param = params.get('lambda_param', 1.0)
        output_var = params.get('output_var', 'random_exp')

        try:
            resolved_lambda = context.resolve_value(lambda_param)

            if resolved_lambda <= 0:
                return ActionResult(success=False, message="lambda必须大于0")

            result = random.expovariate(resolved_lambda)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"指数分布随机数: {result:.6f}",
                data={
                    'value': result,
                    'lambda': resolved_lambda,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"指数分布随机数生成失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'lambda_param': 1.0, 'output_var': 'random_exp'}


class RandomGammavariateAction(BaseAction):
    """Generate gamma distributed random number."""
    action_type = "random_gamma"
    display_name = "Gamma分布随机数"
    description = "生成Gamma分布随机数"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute gamma random.

        Args:
            context: Execution context.
            params: Dict with alpha, beta, output_var.

        Returns:
            ActionResult with gamma random number.
        """
        alpha = params.get('alpha', 1.0)
        beta = params.get('beta', 1.0)
        output_var = params.get('output_var', 'random_gamma')

        try:
            resolved_alpha = context.resolve_value(alpha)
            resolved_beta = context.resolve_value(beta)

            if resolved_alpha <= 0 or resolved_beta <= 0:
                return ActionResult(success=False, message="alpha和beta必须大于0")

            result = random.gammavariate(resolved_alpha, resolved_beta)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"Gamma分布随机数: {result:.6f}",
                data={
                    'value': result,
                    'alpha': resolved_alpha,
                    'beta': resolved_beta,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Gamma分布随机数生成失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'alpha': 1.0, 'beta': 1.0, 'output_var': 'random_gamma'}


class RandomBetavariateAction(BaseAction):
    """Generate beta distributed random number."""
    action_type = "random_beta"
    display_name = "Beta分布随机数"
    description = "生成Beta分布随机数"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute beta random.

        Args:
            context: Execution context.
            params: Dict with alpha, beta, output_var.

        Returns:
            ActionResult with beta random number.
        """
        alpha = params.get('alpha', 1.0)
        beta = params.get('beta', 1.0)
        output_var = params.get('output_var', 'random_beta')

        try:
            resolved_alpha = context.resolve_value(alpha)
            resolved_beta = context.resolve_value(beta)

            if resolved_alpha <= 0 or resolved_beta <= 0:
                return ActionResult(success=False, message="alpha和beta必须大于0")

            result = random.betavariate(resolved_alpha, resolved_beta)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"Beta分布随机数: {result:.6f}",
                data={
                    'value': result,
                    'alpha': resolved_alpha,
                    'beta': resolved_beta,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Beta分布随机数生成失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'alpha': 1.0, 'beta': 1.0, 'output_var': 'random_beta'}


class RandomParetovariateAction(BaseAction):
    """Generate Pareto distributed random number."""
    action_type = "random_pareto"
    display_name = "Pareto分布随机数"
    description = "生成Pareto分布随机数"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Pareto random.

        Args:
            context: Execution context.
            params: Dict with alpha (shape), output_var.

        Returns:
            ActionResult with Pareto random number.
        """
        alpha = params.get('alpha', 1.0)
        output_var = params.get('output_var', 'random_pareto')

        try:
            resolved_alpha = context.resolve_value(alpha)

            if resolved_alpha <= 0:
                return ActionResult(success=False, message="alpha必须大于0")

            result = random.paretovariate(resolved_alpha)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"Pareto分布随机数: {result:.6f}",
                data={
                    'value': result,
                    'alpha': resolved_alpha,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Pareto分布随机数生成失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'alpha': 1.0, 'output_var': 'random_pareto'}


class RandomWeibullvariateAction(BaseAction):
    """Generate Weibull distributed random number."""
    action_type = "random_weibull"
    display_name = "Weibull分布随机数"
    description = "生成Weibull分布随机数"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Weibull random.

        Args:
            context: Execution context.
            params: Dict with alpha (scale), beta (shape), output_var.

        Returns:
            ActionResult with Weibull random number.
        """
        alpha = params.get('alpha', 1.0)
        beta = params.get('beta', 1.0)
        output_var = params.get('output_var', 'random_weibull')

        try:
            resolved_alpha = context.resolve_value(alpha)
            resolved_beta = context.resolve_value(beta)

            if resolved_alpha <= 0 or resolved_beta <= 0:
                return ActionResult(success=False, message="alpha和beta必须大于0")

            result = random.weibullvariate(resolved_alpha, resolved_beta)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"Weibull分布随机数: {result:.6f}",
                data={
                    'value': result,
                    'alpha': resolved_alpha,
                    'beta': resolved_beta,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Weibull分布随机数生成失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'alpha': 1.0, 'beta': 1.0, 'output_var': 'random_weibull'}


class RandomBytesAction(BaseAction):
    """Generate random bytes."""
    action_type = "random_bytes"
    display_name = "随机字节"
    description = "生成随机字节序列"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute random bytes.

        Args:
            context: Execution context.
            params: Dict with length, output_var.

        Returns:
            ActionResult with random bytes.
        """
        length = params.get('length', 32)
        output_var = params.get('output_var', 'random_bytes')

        valid, msg = self.validate_type(length, int, 'length')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_length = context.resolve_value(length)

            if resolved_length <= 0 or resolved_length > 10000:
                return ActionResult(success=False, message="length必须在1-10000之间")

            result = secrets.token_bytes(resolved_length)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"随机字节生成: {resolved_length} bytes",
                data={
                    'length': resolved_length,
                    'hex': result.hex(),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"随机字节生成失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'length': 32, 'output_var': 'random_bytes'}


class RandomPasswordAction(BaseAction):
    """Generate secure random password."""
    action_type = "random_password"
    display_name = "随机密码"
    description = "生成安全随机密码"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute password generation.

        Args:
            context: Execution context.
            params: Dict with length, use_upper, use_lower, use_digits, use_special, output_var.

        Returns:
            ActionResult with generated password.
        """
        length = params.get('length', 16)
        use_upper = params.get('use_upper', True)
        use_lower = params.get('use_lower', True)
        use_digits = params.get('use_digits', True)
        use_special = params.get('use_special', False)
        output_var = params.get('output_var', 'random_password')

        valid, msg = self.validate_type(length, int, 'length')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_length = context.resolve_value(length)

            if resolved_length < 4:
                return ActionResult(success=False, message="密码长度至少为4")

            chars = ''
            if context.resolve_value(use_upper):
                chars += string.ascii_uppercase
            if context.resolve_value(use_lower):
                chars += string.ascii_lowercase
            if context.resolve_value(use_digits):
                chars += string.digits
            if context.resolve_value(use_special):
                chars += string.punctuation

            if not chars:
                return ActionResult(success=False, message="至少选择一种字符类型")

            # Use secrets for cryptographic security
            result = ''.join(secrets.choice(chars) for _ in range(resolved_length))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"密码生成: {len(result)} characters",
                data={
                    'password': result,
                    'length': resolved_length,
                    'charset_size': len(chars),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"密码生成失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'length': 16,
            'use_upper': True,
            'use_lower': True,
            'use_digits': True,
            'use_special': False,
            'output_var': 'random_password'
        }
