"""Statistics action module for RabAI AutoClick.

Provides statistics operations:
- StatsMeanAction: Calculate mean
- StatsMedianAction: Calculate median
- StatsModeAction: Calculate mode
- StatsStdDevAction: Calculate standard deviation
- StatsVarianceAction: Calculate variance
- StatsPercentileAction: Calculate percentile
"""

import statistics
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class StatsMeanAction(BaseAction):
    """Calculate mean."""
    action_type = "stats_mean"
    display_name = "计算均值"
    description = "计算列表均值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute mean.

        Args:
            context: Execution context.
            params: Dict with list_var, output_var.

        Returns:
            ActionResult with mean.
        """
        list_var = params.get('list_var', '')
        output_var = params.get('output_var', 'mean_result')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(list_var)

            items = context.get(resolved_var)
            if not isinstance(items, (list, tuple)):
                items = list(items)

            if len(items) == 0:
                return ActionResult(
                    success=False,
                    message="空列表无法计算均值"
                )

            result = statistics.mean(items)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"均值: {result}",
                data={
                    'count': len(items),
                    'mean': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算均值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'mean_result'}


class StatsMedianAction(BaseAction):
    """Calculate median."""
    action_type = "stats_median"
    display_name = "计算中位数"
    description = "计算列表中位数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute median.

        Args:
            context: Execution context.
            params: Dict with list_var, output_var.

        Returns:
            ActionResult with median.
        """
        list_var = params.get('list_var', '')
        output_var = params.get('output_var', 'median_result')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(list_var)

            items = context.get(resolved_var)
            if not isinstance(items, (list, tuple)):
                items = list(items)

            if len(items) == 0:
                return ActionResult(
                    success=False,
                    message="空列表无法计算中位数"
                )

            result = statistics.median(items)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"中位数: {result}",
                data={
                    'count': len(items),
                    'median': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算中位数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'median_result'}


class StatsModeAction(BaseAction):
    """Calculate mode."""
    action_type = "stats_mode"
    display_name = "计算众数"
    description = "计算列表众数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute mode.

        Args:
            context: Execution context.
            params: Dict with list_var, output_var.

        Returns:
            ActionResult with mode.
        """
        list_var = params.get('list_var', '')
        output_var = params.get('output_var', 'mode_result')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(list_var)

            items = context.get(resolved_var)
            if not isinstance(items, (list, tuple)):
                items = list(items)

            if len(items) == 0:
                return ActionResult(
                    success=False,
                    message="空列表无法计算众数"
                )

            result = statistics.mode(items)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"众数: {result}",
                data={
                    'count': len(items),
                    'mode': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算众数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'mode_result'}


class StatsStdDevAction(BaseAction):
    """Calculate standard deviation."""
    action_type = "stats_stdev"
    display_name = "计算标准差"
    description = "计算列表标准差"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute stdev.

        Args:
            context: Execution context.
            params: Dict with list_var, output_var.

        Returns:
            ActionResult with stdev.
        """
        list_var = params.get('list_var', '')
        output_var = params.get('output_var', 'stdev_result')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(list_var)

            items = context.get(resolved_var)
            if not isinstance(items, (list, tuple)):
                items = list(items)

            if len(items) < 2:
                return ActionResult(
                    success=False,
                    message="列表至少需要2个元素计算标准差"
                )

            result = statistics.stdev(items)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"标准差: {result}",
                data={
                    'count': len(items),
                    'stdev': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算标准差失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'stdev_result'}


class StatsVarianceAction(BaseAction):
    """Calculate variance."""
    action_type = "stats_variance"
    display_name = "计算方差"
    description = "计算列表方差"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute variance.

        Args:
            context: Execution context.
            params: Dict with list_var, output_var.

        Returns:
            ActionResult with variance.
        """
        list_var = params.get('list_var', '')
        output_var = params.get('output_var', 'variance_result')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(list_var)

            items = context.get(resolved_var)
            if not isinstance(items, (list, tuple)):
                items = list(items)

            if len(items) < 2:
                return ActionResult(
                    success=False,
                    message="列表至少需要2个元素计算方差"
                )

            result = statistics.variance(items)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"方差: {result}",
                data={
                    'count': len(items),
                    'variance': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算方差失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'variance_result'}


class StatsPercentileAction(BaseAction):
    """Calculate percentile."""
    action_type = "stats_percentile"
    display_name = "计算百分位数"
    description = "计算列表百分位数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute percentile.

        Args:
            context: Execution context.
            params: Dict with list_var, percentile, output_var.

        Returns:
            ActionResult with percentile.
        """
        list_var = params.get('list_var', '')
        percentile = params.get('percentile', 50)
        output_var = params.get('output_var', 'percentile_result')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(list_var)
            resolved_p = float(context.resolve_value(percentile))

            items = context.get(resolved_var)
            if not isinstance(items, (list, tuple)):
                items = list(items)

            if len(items) == 0:
                return ActionResult(
                    success=False,
                    message="空列表无法计算百分位数"
                )

            if resolved_p < 0 or resolved_p > 100:
                return ActionResult(
                    success=False,
                    message="百分位数必须在0-100之间"
                )

            result = statistics.quantiles(items, n=100)[int(resolved_p) - 1] if resolved_p != 0 and resolved_p != 100 else (min(items) if resolved_p == 0 else max(items))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"百分位数 {resolved_p}: {result}",
                data={
                    'count': len(items),
                    'percentile': resolved_p,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算百分位数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var', 'percentile']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'percentile_result'}
