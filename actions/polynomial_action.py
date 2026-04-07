"""Polynomial action module for RabAI AutoClick.

Provides polynomial operations:
- PolynomialEvalAction: Evaluate polynomial at a point
- PolynomialAddAction: Add polynomials
- PolynomialSubtractAction: Subtract polynomials
- PolynomialMultiplyAction: Multiply polynomials
- PolynomialDerivativeAction: Compute derivative
- PolynomialIntegralAction: Compute indefinite integral
- PolynomialRootsAction: Find polynomial roots (Newton-Raphson)
"""

from typing import Any, Dict, List, Optional, Tuple

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PolynomialEvalAction(BaseAction):
    """Evaluate polynomial at a given value."""
    action_type = "polynomial_eval"
    display_name = "多项式求值"
    description = "计算多项式在指定点的值"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Evaluate polynomial at x.

        Args:
            context: Execution context.
            params: Dict with coefficients, x, output_var.
                coefficients: List of coefficients [a_n, ..., a_1, a_0]
                x: Value at which to evaluate

        Returns:
            ActionResult with evaluated value.
        """
        coefficients = params.get('coefficients', [])
        x = params.get('x', 0)
        output_var = params.get('output_var', 'poly_eval_result')

        valid, msg = self.validate_type(coefficients, list, 'coefficients')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_coeffs = context.resolve_value(coefficients)
            resolved_x = context.resolve_value(x)

            if not resolved_coeffs:
                return ActionResult(success=False, message="系数列表不能为空")

            result = 0.0
            for i, coef in enumerate(resolved_coeffs):
                result += coef * (resolved_x ** i)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"多项式求值完成: P({resolved_x}) = {result}",
                data={
                    'value': result,
                    'x': resolved_x,
                    'coefficients': resolved_coeffs,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"多项式求值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['coefficients', 'x']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'poly_eval_result'}


class PolynomialAddAction(BaseAction):
    """Add two polynomials."""
    action_type = "polynomial_add"
    display_name = "多项式加法"
    description = "两个多项式相加"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Add polynomials.

        Args:
            context: Execution context.
            params: Dict with poly1, poly2, output_var.

        Returns:
            ActionResult with sum polynomial.
        """
        poly1 = params.get('poly1', [])
        poly2 = params.get('poly2', [])
        output_var = params.get('output_var', 'poly_sum')

        valid, msg = self.validate_type(poly1, list, 'poly1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(poly2, list, 'poly2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_p1 = context.resolve_value(poly1)
            resolved_p2 = context.resolve_value(poly2)

            max_len = max(len(resolved_p1), len(resolved_p2))
            result = [0.0] * max_len

            for i in range(len(resolved_p1)):
                result[i] += resolved_p1[i]
            for i in range(len(resolved_p2)):
                result[i] += resolved_p2[i]

            # Remove trailing zeros
            while len(result) > 1 and abs(result[-1]) < 1e-12:
                result.pop()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"多项式加法完成: {result}",
                data={
                    'result': result,
                    'poly1': resolved_p1,
                    'poly2': resolved_p2,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"多项式加法失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['poly1', 'poly2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'poly_sum'}


class PolynomialSubtractAction(BaseAction):
    """Subtract two polynomials."""
    action_type = "polynomial_subtract"
    display_name = "多项式减法"
    description = "两个多项式相减"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Subtract polynomials.

        Args:
            context: Execution context.
            params: Dict with poly1, poly2, output_var.

        Returns:
            ActionResult with difference polynomial.
        """
        poly1 = params.get('poly1', [])
        poly2 = params.get('poly2', [])
        output_var = params.get('output_var', 'poly_diff')

        valid, msg = self.validate_type(poly1, list, 'poly1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(poly2, list, 'poly2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_p1 = context.resolve_value(poly1)
            resolved_p2 = context.resolve_value(poly2)

            max_len = max(len(resolved_p1), len(resolved_p2))
            result = [0.0] * max_len

            for i in range(len(resolved_p1)):
                result[i] += resolved_p1[i]
            for i in range(len(resolved_p2)):
                result[i] -= resolved_p2[i]

            # Remove trailing zeros
            while len(result) > 1 and abs(result[-1]) < 1e-12:
                result.pop()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"多项式减法完成: {result}",
                data={
                    'result': result,
                    'poly1': resolved_p1,
                    'poly2': resolved_p2,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"多项式减法失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['poly1', 'poly2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'poly_diff'}


class PolynomialMultiplyAction(BaseAction):
    """Multiply two polynomials."""
    action_type = "polynomial_multiply"
    display_name = "多项式乘法"
    description = "两个多项式相乘"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Multiply polynomials.

        Args:
            context: Execution context.
            params: Dict with poly1, poly2, output_var.

        Returns:
            ActionResult with product polynomial.
        """
        poly1 = params.get('poly1', [])
        poly2 = params.get('poly2', [])
        output_var = params.get('output_var', 'poly_product')

        valid, msg = self.validate_type(poly1, list, 'poly1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(poly2, list, 'poly2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_p1 = context.resolve_value(poly1)
            resolved_p2 = context.resolve_value(poly2)

            if not resolved_p1 or not resolved_p2:
                return ActionResult(success=False, message="两个多项式都不能为空")

            result = [0.0] * (len(resolved_p1) + len(resolved_p2) - 1)

            for i, a in enumerate(resolved_p1):
                for j, b in enumerate(resolved_p2):
                    result[i + j] += a * b

            # Remove trailing zeros
            while len(result) > 1 and abs(result[-1]) < 1e-12:
                result.pop()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"多项式乘法完成: {result}",
                data={
                    'result': result,
                    'degree': len(result) - 1,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"多项式乘法失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['poly1', 'poly2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'poly_product'}


class PolynomialDerivativeAction(BaseAction):
    """Compute derivative of polynomial."""
    action_type = "polynomial_derivative"
    display_name = "多项式导数"
    description = "计算多项式导数"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Compute derivative.

        Args:
            context: Execution context.
            params: Dict with coefficients, output_var.

        Returns:
            ActionResult with derivative polynomial.
        """
        coefficients = params.get('coefficients', [])
        output_var = params.get('output_var', 'poly_derivative')

        valid, msg = self.validate_type(coefficients, list, 'coefficients')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_coeffs = context.resolve_value(coefficients)

            if len(resolved_coeffs) < 2:
                context.set(output_var, [0.0])
                return ActionResult(
                    success=True,
                    message="导数为常数0",
                    data={'result': [0.0], 'output_var': output_var}
                )

            result = [resolved_coeffs[i] * i for i in range(1, len(resolved_coeffs))]

            # Remove trailing zeros
            while len(result) > 1 and abs(result[-1]) < 1e-12:
                result.pop()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"多项式导数: {result}",
                data={
                    'result': result,
                    'degree': len(result) - 1 if result else 0,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"多项式求导失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['coefficients']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'poly_derivative'}


class PolynomialIntegralAction(BaseAction):
    """Compute indefinite integral of polynomial."""
    action_type = "polynomial_integral"
    display_name = "多项式积分"
    description = "计算多项式不定积分"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Compute indefinite integral.

        Args:
            context: Execution context.
            params: Dict with coefficients, constant, output_var.

        Returns:
            ActionResult with integral polynomial.
        """
        coefficients = params.get('coefficients', [])
        constant = params.get('constant', 0.0)
        output_var = params.get('output_var', 'poly_integral')

        valid, msg = self.validate_type(coefficients, list, 'coefficients')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_coeffs = context.resolve_value(coefficients)
            resolved_constant = context.resolve_value(constant)

            result = [resolved_constant]
            for i, coef in enumerate(resolved_coeffs):
                result.append(coef / (i + 1))

            # Remove trailing zeros
            while len(result) > 1 and abs(result[-1]) < 1e-12:
                result.pop()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"多项式积分: {result} + C",
                data={
                    'result': result,
                    'constant': resolved_constant,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"多项式积分失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['coefficients']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'constant': 0.0, 'output_var': 'poly_integral'}


class PolynomialRootsAction(BaseAction):
    """Find roots of polynomial using Newton-Raphson."""
    action_type = "polynomial_roots"
    display_name = "多项式求根"
    description = "使用牛顿法求多项式根"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Find roots.

        Args:
            context: Execution context.
            params: Dict with coefficients, initial_guess, tolerance, max_iter, output_var.

        Returns:
            ActionResult with found roots.
        """
        coefficients = params.get('coefficients', [])
        initial_guess = params.get('initial_guess', 0.0)
        tolerance = params.get('tolerance', 1e-10)
        max_iter = params.get('max_iter', 100)
        output_var = params.get('output_var', 'poly_roots')

        valid, msg = self.validate_type(coefficients, list, 'coefficients')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_coeffs = context.resolve_value(coefficients)
            resolved_guess = context.resolve_value(initial_guess)
            resolved_tol = context.resolve_value(tolerance)
            resolved_max_iter = context.resolve_value(max_iter)

            if len(resolved_coeffs) < 2:
                return ActionResult(success=False, message="需要至少2个系数才能求根")

            # Derivative coefficients
            deriv_coeffs = [resolved_coeffs[i] * i for i in range(1, len(resolved_coeffs))]

            def eval_poly(coeffs, x):
                result = 0.0
                for i, c in enumerate(coeffs):
                    result += c * (x ** i)
                return result

            roots = []
            current_guess = complex(resolved_guess, 0)

            for _ in range(int(resolved_max_iter)):
                f_val = eval_poly(resolved_coeffs, current_guess)
                if abs(f_val) < resolved_tol:
                    break

                f_prime_val = eval_poly(deriv_coeffs, current_guess)
                if abs(f_prime_val) < 1e-15:
                    current_guess += complex(0.1, 0.1)
                    continue

                current_guess = current_guess - f_val / f_prime_val

            real_part = round(current_guess.real, 12)
            imag_part = round(current_guess.imag, 12)

            if abs(imag_part) < 1e-8:
                result = real_part if abs(real_part) > 1e-10 else 0.0
            else:
                result = complex(real_part, imag_part)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"多项式根: {result}",
                data={
                    'root': result,
                    'iterations': resolved_max_iter,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"多项式求根失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['coefficients']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'initial_guess': 0.0,
            'tolerance': 1e-10,
            'max_iter': 100,
            'output_var': 'poly_roots'
        }
