"""
Tests for workflow_aws_cloudwatch module
"""
import sys
sys.path.insert(0, '/Users/guige/my_project')

import unittest
from unittest.mock import Mock, patch, MagicMock, PropertyMock
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import types

# Create mock boto3 module before importing workflow_aws_cloudwatch
mock_boto3 = types.ModuleType('boto3')
mock_boto3.Session = MagicMock()
mock_boto3.client = MagicMock()

# Create mock botocore exceptions
mock_boto3_exceptions = types.ModuleType('botocore.exceptions')
mock_boto3_exceptions.ClientError = Exception
mock_boto3_exceptions.BotoCoreError = Exception

# Create mock botocore config
mock_boto3_config = types.ModuleType('botocore.config')
mock_boto3_config.Config = MagicMock()

sys.modules['boto3'] = mock_boto3
sys.modules['botocore'] = types.ModuleType('botocore')
sys.modules['botocore.exceptions'] = mock_boto3_exceptions
sys.modules['botocore.config'] = mock_boto3_config

# Now we can import the module
from src.workflow_aws_cloudwatch import (
    CloudWatchIntegration,
    MetricStatistic,
    AlarmState,
    AlarmActionType,
    LogFormat,
    SyntheticsRunStatus,
    InsightRuleState,
    SLIType,
    MetricData,
    AlarmConfig,
    DashboardWidget,
    LogQuery,
    CanaryConfig,
    ContributorInsightRule,
    ServiceLevelIndicator,
    EmbeddedMetricSpec,
    BOTO3_AVAILABLE,
)


class TestMetricStatistic(unittest.TestCase):
    """Test MetricStatistic enum"""

    def test_metric_statistic_values(self):
        self.assertEqual(MetricStatistic.SAMPLE_COUNT.value, "SampleCount")
        self.assertEqual(MetricStatistic.AVERAGE.value, "Average")
        self.assertEqual(MetricStatistic.SUM.value, "Sum")
        self.assertEqual(MetricStatistic.MINIMUM.value, "Minimum")
        self.assertEqual(MetricStatistic.MAXIMUM.value, "Maximum")


class TestAlarmState(unittest.TestCase):
    """Test AlarmState enum"""

    def test_alarm_state_values(self):
        self.assertEqual(AlarmState.OK.value, "OK")
        self.assertEqual(AlarmState.ALARM.value, "ALARM")
        self.assertEqual(AlarmState.INSUFFICIENT_DATA.value, "INSUFFICIENT_DATA")


class TestAlarmActionType(unittest.TestCase):
    """Test AlarmActionType enum"""

    def test_alarm_action_type_values(self):
        self.assertEqual(AlarmActionType.SNS.value, "sns")
        self.assertEqual(AlarmActionType.AUTO_SCALING.value, "autoscaling")
        self.assertEqual(AlarmActionType.EC2.value, "ec2")


class TestLogFormat(unittest.TestCase):
    """Test LogFormat enum"""

    def test_log_format_values(self):
        self.assertEqual(LogFormat.JSON.value, "json")
        self.assertEqual(LogFormat.PLAIN_TEXT.value, "plaintext")
        self.assertEqual(LogFormat.RAW.value, "raw")


class TestSyntheticsRunStatus(unittest.TestCase):
    """Test SyntheticsRunStatus enum"""

    def test_synthetics_run_status_values(self):
        self.assertEqual(SyntheticsRunStatus.RUNNING.value, "RUNNING")
        self.assertEqual(SyntheticsRunStatus.PASSED.value, "PASSED")
        self.assertEqual(SyntheticsRunStatus.FAILED.value, "FAILED")


class TestInsightRuleState(unittest.TestCase):
    """Test InsightRuleState enum"""

    def test_insight_rule_state_values(self):
        self.assertEqual(InsightRuleState.ENABLED.value, "ENABLED")
        self.assertEqual(InsightRuleState.DISABLED.value, "DISABLED")
        self.assertEqual(InsightRuleState.DELETED.value, "DELETED")


class TestSLIType(unittest.TestCase):
    """Test SLIType enum"""

    def test_sli_type_values(self):
        self.assertEqual(SLIType.AVAILABILITY.value, "availability")
        self.assertEqual(SLIType.LATENCY.value, "latency")
        self.assertEqual(SLIType.THROUGHPUT.value, "throughput")
        self.assertEqual(SLIType.ERROR_RATE.value, "error_rate")


class TestMetricData(unittest.TestCase):
    """Test MetricData dataclass"""

    def test_metric_data_defaults(self):
        metric = MetricData(
            metric_name="CPUUtilization",
            value=75.5
        )
        self.assertEqual(metric.metric_name, "CPUUtilization")
        self.assertEqual(metric.value, 75.5)
        self.assertEqual(metric.unit, "None")
        self.assertEqual(metric.dimensions, {})
        self.assertIsNone(metric.statistics)
        self.assertIsNone(metric.timestamp)

    def test_metric_data_custom(self):
        timestamp = datetime(2024, 1, 1, 12, 0, 0)
        metric = MetricData(
            metric_name="RequestCount",
            value=1000.0,
            timestamp=timestamp,
            unit="Count",
            dimensions={"Service": "API"},
            statistics={"Average": 50.0, "Sum": 5000.0}
        )
        self.assertEqual(metric.unit, "Count")
        self.assertEqual(metric.dimensions, {"Service": "API"})
        self.assertEqual(metric.statistics["Average"], 50.0)


class TestAlarmConfig(unittest.TestCase):
    """Test AlarmConfig dataclass"""

    def test_alarm_config_defaults(self):
        config = AlarmConfig(
            alarm_name="HighCPUAlarm",
            metric_name="CPUUtilization",
            namespace="AWS/EC2",
            threshold=80.0,
            comparison_operator="GreaterThanThreshold"
        )
        self.assertEqual(config.alarm_name, "HighCPUAlarm")
        self.assertEqual(config.period, 60)
        self.assertEqual(config.evaluation_periods, 1)
        self.assertEqual(config.statistic, "Average")
        self.assertEqual(config.alarm_actions, [])
        self.assertEqual(config.treat_missing_data, "missing")

    def test_alarm_config_custom(self):
        config = AlarmConfig(
            alarm_name="HighCPUAlarm",
            metric_name="CPUUtilization",
            namespace="AWS/EC2",
            threshold=80.0,
            comparison_operator="GreaterThanThreshold",
            period=300,
            evaluation_periods=3,
            alarm_actions=["arn:aws:sns:us-east-1:123456789012:my-topic"],
            dimensions={"InstanceId": "i-123"}
        )
        self.assertEqual(config.period, 300)
        self.assertEqual(config.evaluation_periods, 3)
        self.assertEqual(len(config.alarm_actions), 1)
        self.assertEqual(config.dimensions, {"InstanceId": "i-123"})


class TestDashboardWidget(unittest.TestCase):
    """Test DashboardWidget dataclass"""

    def test_dashboard_widget_defaults(self):
        widget = DashboardWidget(
            widget_type="metric",
            title="My Widget"
        )
        self.assertEqual(widget.widget_type, "metric")
        self.assertEqual(widget.title, "My Widget")
        self.assertEqual(widget.width, 6)
        self.assertEqual(widget.height, 6)
        self.assertEqual(widget.properties, {})

    def test_dashboard_widget_custom(self):
        widget = DashboardWidget(
            widget_type="log",
            title="Error Logs",
            width=12,
            height=8,
            properties={"query": "fields @message"}
        )
        self.assertEqual(widget.width, 12)
        self.assertEqual(widget.height, 8)
        self.assertEqual(widget.properties["query"], "fields @message")


class TestLogQuery(unittest.TestCase):
    """Test LogQuery dataclass"""

    def test_log_query_defaults(self):
        query = LogQuery(
            query_string="fields @timestamp, @message",
            log_group_name="/aws/lambda/my-function"
        )
        self.assertEqual(query.query_string, "fields @timestamp, @message")
        self.assertEqual(query.log_group_name, "/aws/lambda/my-function")
        self.assertEqual(query.limit, 1000)
        self.assertIsNone(query.start_time)
        self.assertIsNone(query.end_time)

    def test_log_query_custom(self):
        start = datetime(2024, 1, 1, 0, 0, 0)
        end = datetime(2024, 1, 2, 0, 0, 0)
        query = LogQuery(
            query_string="fields @message",
            log_group_name="/aws/lambda/my-function",
            start_time=start,
            end_time=end,
            limit=500
        )
        self.assertEqual(query.start_time, start)
        self.assertEqual(query.end_time, end)
        self.assertEqual(query.limit, 500)


class TestCanaryConfig(unittest.TestCase):
    """Test CanaryConfig dataclass"""

    def test_canary_config_defaults(self):
        config = CanaryConfig(
            name="my-canary",
            execution_role_arn="arn:aws:iam::123456789012:role/my-role",
            handler="exports.handler",
            code_bucket="my-bucket",
            code_key="canary.zip"
        )
        self.assertEqual(config.name, "my-canary")
        self.assertEqual(config.schedule_expression, "rate(5 minutes)")
        self.assertEqual(config.runtime_version, "syn-nodejs-puppeteer-6.0")
        self.assertEqual(config.failure_retention_period, 31)
        self.assertEqual(config.success_retention_period, 31)

    def test_canary_config_custom(self):
        config = CanaryConfig(
            name="my-canary",
            execution_role_arn="arn:aws:iam::123456789012:role/my-role",
            handler="exports.handler",
            code_bucket="my-bucket",
            code_key="canary.zip",
            schedule_expression="rate(1 minute)",
            runtime_version="syn-nodejs-puppeteer-7.0"
        )
        self.assertEqual(config.schedule_expression, "rate(1 minute)")
        self.assertEqual(config.runtime_version, "syn-nodejs-puppeteer-7.0")


class TestContributorInsightRule(unittest.TestCase):
    """Test ContributorInsightRule dataclass"""

    def test_contributor_insight_rule_defaults(self):
        rule = ContributorInsightRule(
            rule_name="my-rule",
            log_group_name="/aws/lambda/my-function",
            schema={"something": "value"}
        )
        self.assertEqual(rule.rule_name, "my-rule")
        self.assertEqual(rule.log_group_name, "/aws/lambda/my-function")
        self.assertEqual(rule.schema, {"something": "value"})
        self.assertEqual(rule.tags, {})

    def test_contributor_insight_rule_with_tags(self):
        rule = ContributorInsightRule(
            rule_name="my-rule",
            log_group_name="/aws/lambda/my-function",
            schema={"something": "value"},
            tags={"Environment": "Production"}
        )
        self.assertEqual(rule.tags, {"Environment": "Production"})


class TestServiceLevelIndicator(unittest.TestCase):
    """Test ServiceLevelIndicator dataclass"""

    def test_service_level_indicator_defaults(self):
        sli = ServiceLevelIndicator(
            name="my-sli",
            sli_type=SLIType.AVAILABILITY,
            metric_name="SuccessRate",
            namespace="MyApp",
            target=99.9
        )
        self.assertEqual(sli.name, "my-sli")
        self.assertEqual(sli.sli_type, SLIType.AVAILABILITY)
        self.assertEqual(sli.target, 99.9)
        self.assertEqual(sli.period, 60)
        self.assertEqual(sli.dimensions, {})


class TestEmbeddedMetricSpec(unittest.TestCase):
    """Test EmbeddedMetricSpec dataclass"""

    def test_embedded_metric_spec(self):
        spec = EmbeddedMetricSpec(
            namespace="MyApp/Metrics",
            metric_name="RequestLatency",
            dimensions=["Service", "Region"],
            metrics=[
                {"Name": "Latency", "Unit": "Milliseconds"}
            ]
        )
        self.assertEqual(spec.namespace, "MyApp/Metrics")
        self.assertEqual(spec.metric_name, "RequestLatency")
        self.assertEqual(len(spec.dimensions), 2)


class TestCloudWatchIntegrationInit(unittest.TestCase):
    """Test CloudWatchIntegration initialization"""

    def test_init_with_defaults(self):
        with patch('src.workflow_aws_cloudwatch.BOTO3_AVAILABLE', False):
            integration = CloudWatchIntegration()
            self.assertEqual(integration.region_name, "us-east-1")
            self.assertIsNone(integration.profile_name)
            self.assertIsNone(integration.endpoint_url)

    def test_init_with_custom_params(self):
        with patch('src.workflow_aws_cloudwatch.BOTO3_AVAILABLE', False):
            integration = CloudWatchIntegration(
                region_name="us-west-2",
                profile_name="my-profile",
                endpoint_url="http://localhost:4566"
            )
            self.assertEqual(integration.region_name, "us-west-2")
            self.assertEqual(integration.profile_name, "my-profile")
            self.assertEqual(integration.endpoint_url, "http://localhost:4566")


class TestCloudWatchIntegrationMetrics(unittest.TestCase):
    """Test CloudWatchIntegration metrics operations"""

    def setUp(self):
        self.mock_cloudwatch = MagicMock()
        self.mock_cloudwatch.put_metric_data.return_value = {}
        self.mock_cloudwatch.get_metric_data.return_value = {
            'MetricDataResults': []
        }
        self.mock_cloudwatch.get_metric_statistics.return_value = {
            'Datapoints': []
        }
        self.mock_cloudwatch.list_metrics.return_value = {
            'Metrics': []
        }
        self.mock_cloudwatch.get_paginator.return_value.paginate.return_value = [
            {'Metrics': []}
        ]

        with patch('src.workflow_aws_cloudwatch.BOTO3_AVAILABLE', True):
            self.integration = CloudWatchIntegration()
            self.integration._clients["cloudwatch"] = self.mock_cloudwatch

    def test_put_metric_data_single(self):
        metric = MetricData(
            metric_name="CPUUtilization",
            value=75.5,
            unit="Percent"
        )

        result = self.integration.put_metric_data(
            namespace="AWS/EC2",
            metrics=[metric]
        )

        self.assertEqual(result, {})
        self.mock_cloudwatch.put_metric_data.assert_called_once()

    def test_put_metric_data_multiple(self):
        metrics = [
            MetricData(metric_name="CPUUtilization", value=75.5, unit="Percent"),
            MetricData(metric_name="MemoryUtilization", value=50.0, unit="Percent")
        ]

        result = self.integration.put_metric_data(
            namespace="System/Linux",
            metrics=metrics
        )

        self.mock_cloudwatch.put_metric_data.assert_called_once()
        call_args = self.mock_cloudwatch.put_metric_data.call_args
        self.assertEqual(len(call_args.kwargs['MetricData']), 2)

    def test_put_metric_data_with_dimensions(self):
        metric = MetricData(
            metric_name="RequestCount",
            value=1000.0,
            unit="Count",
            dimensions={"Service": "API", "Region": "us-east-1"}
        )

        self.integration.put_metric_data(
            namespace="MyApp",
            metrics=[metric]
        )

        call_args = self.mock_cloudwatch.put_metric_data.call_args
        metric_data = call_args.kwargs['MetricData'][0]
        self.assertEqual(len(metric_data['Dimensions']), 2)

    def test_put_metric_data_with_statistics(self):
        metric = MetricData(
            metric_name="Latency",
            value=0.0,
            statistics={
                "SampleCount": 100,
                "Sum": 5000.0,
                "Minimum": 10.0,
                "Maximum": 100.0,
                "Average": 50.0
            }
        )

        self.integration.put_metric_data(
            namespace="MyApp",
            metrics=[metric]
        )

        call_args = self.mock_cloudwatch.put_metric_data.call_args
        metric_data = call_args.kwargs['MetricData'][0]
        self.assertIn('StatisticValues', metric_data)

    def test_put_metric_data_without_client(self):
        with patch('src.workflow_aws_cloudwatch.BOTO3_AVAILABLE', True):
            integration = CloudWatchIntegration()
            integration._clients = {"cloudwatch": None}

            with self.assertRaises(RuntimeError) as context:
                integration.put_metric_data(
                    namespace="MyApp",
                    metrics=[MetricData(metric_name="Test", value=1.0)]
                )
            self.assertIn("not initialized", str(context.exception))

    def test_get_metric_data(self):
        metric_queries = [
            {
                'Id': 'm1',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/EC2',
                        'MetricName': 'CPUUtilization'
                    },
                    'Period': 300,
                    'Stat': 'Average'
                }
            }
        ]
        start_time = datetime(2024, 1, 1, 0, 0, 0)
        end_time = datetime(2024, 1, 2, 0, 0, 0)

        result = self.integration.get_metric_data(
            metric_queries=metric_queries,
            start_time=start_time,
            end_time=end_time
        )

        self.mock_cloudwatch.get_metric_data.assert_called_once()

    def test_get_metric_statistics(self):
        start_time = datetime(2024, 1, 1, 0, 0, 0)
        end_time = datetime(2024, 1, 2, 0, 0, 0)

        result = self.integration.get_metric_statistics(
            namespace="AWS/EC2",
            metric_name="CPUUtilization",
            start_time=start_time,
            end_time=end_time,
            period=300,
            statistics=["Average", "Minimum", "Maximum"]
        )

        self.mock_cloudwatch.get_metric_statistics.assert_called_once()

    def test_get_metric_statistics_with_dimensions(self):
        start_time = datetime(2024, 1, 1, 0, 0, 0)
        end_time = datetime(2024, 1, 2, 0, 0, 0)
        dimensions = [{"Name": "InstanceId", "Value": "i-123"}]

        result = self.integration.get_metric_statistics(
            namespace="AWS/EC2",
            metric_name="CPUUtilization",
            start_time=start_time,
            end_time=end_time,
            period=300,
            statistics=["Average"],
            dimensions=dimensions
        )

        call_args = self.mock_cloudwatch.get_metric_statistics.call_args
        self.assertIn('Dimensions', call_args.kwargs)

    def test_list_metrics(self):
        result = self.integration.list_metrics(
            namespace="AWS/EC2",
            metric_name="CPUUtilization"
        )

        self.mock_cloudwatch.get_paginator.assert_called()

    def test_list_metrics_with_filters(self):
        dimensions = [{"Name": "InstanceId", "Value": "i-123"}]

        result = self.integration.list_metrics(
            namespace="AWS/EC2",
            metric_name="CPUUtilization",
            dimensions=dimensions
        )

        self.mock_cloudwatch.get_paginator.assert_called()


class TestCloudWatchIntegrationAlarms(unittest.TestCase):
    """Test CloudWatchIntegration alarm operations"""

    def setUp(self):
        self.mock_cloudwatch = MagicMock()
        self.mock_cloudwatch.put_metric_alarm.return_value = {}
        self.mock_cloudwatch.describe_alarms.return_value = {
            'MetricAlarms': []
        }
        self.mock_cloudwatch.delete_alarms.return_value = {}
        self.mock_cloudwatch.get_paginator.return_value.paginate.return_value = [
            {'MetricAlarms': []}
        ]

        with patch('src.workflow_aws_cloudwatch.BOTO3_AVAILABLE', True):
            self.integration = CloudWatchIntegration()
            self.integration._clients["cloudwatch"] = self.mock_cloudwatch

    def test_put_metric_alarm_basic(self):
        config = AlarmConfig(
            alarm_name="HighCPUAlarm",
            metric_name="CPUUtilization",
            namespace="AWS/EC2",
            threshold=80.0,
            comparison_operator="GreaterThanThreshold"
        )

        result = self.integration.put_metric_alarm(config)

        self.mock_cloudwatch.put_metric_alarm.assert_called_once()

    def test_put_metric_alarm_with_actions(self):
        config = AlarmConfig(
            alarm_name="HighCPUAlarm",
            metric_name="CPUUtilization",
            namespace="AWS/EC2",
            threshold=80.0,
            comparison_operator="GreaterThanThreshold",
            alarm_actions=["arn:aws:sns:us-east-1:123456789012:my-topic"]
        )

        result = self.integration.put_metric_alarm(config)

        call_args = self.mock_cloudwatch.put_metric_alarm.call_args
        self.assertIn('AlarmActions', call_args.kwargs)

    def test_put_metric_alarm_without_client(self):
        with patch('src.workflow_aws_cloudwatch.BOTO3_AVAILABLE', True):
            integration = CloudWatchIntegration()
            integration._clients = {"cloudwatch": None}

            config = AlarmConfig(
                alarm_name="HighCPUAlarm",
                metric_name="CPUUtilization",
                namespace="AWS/EC2",
                threshold=80.0,
                comparison_operator="GreaterThanThreshold"
            )

            with self.assertRaises(RuntimeError) as context:
                integration.put_metric_alarm(config)
            self.assertIn("not initialized", str(context.exception))

    def test_describe_alarms_by_names(self):
        alarm_names = ["Alarm1", "Alarm2"]

        result = self.integration.describe_alarms(alarm_names=alarm_names)

        self.mock_cloudwatch.get_paginator.assert_called()

    def test_describe_alarms_by_prefix(self):
        result = self.integration.describe_alarms(alarm_prefix="HighCPU")

        self.mock_cloudwatch.get_paginator.assert_called()

    def test_describe_alarms_by_state(self):
        result = self.integration.describe_alarms(state_value="ALARM")

        self.mock_cloudwatch.get_paginator.assert_called()

    def test_delete_alarms(self):
        alarm_names = ["Alarm1", "Alarm2"]

        result = self.integration.delete_alarms(alarm_names)

        self.mock_cloudwatch.delete_alarms.assert_called_once()

    def test_set_alarm_state(self):
        result = self.integration.set_alarm_state(
            alarm_name="TestAlarm",
            state_value="ALARM",
            state_reason="Testing"
        )

        self.mock_cloudwatch.set_alarm_state.assert_called_once()


class TestCloudWatchIntegrationDashboards(unittest.TestCase):
    """Test CloudWatchIntegration dashboard operations"""

    def setUp(self):
        self.mock_cloudwatch = MagicMock()
        self.mock_cloudwatch.put_dashboard.return_value = {}
        self.mock_cloudwatch.get_dashboard.return_value = {
            'DashboardEntries': [],
            'DashboardBody': '{}'
        }
        self.mock_cloudwatch.get_paginator.return_value.paginate.return_value = [
            {'DashboardEntries': []}
        ]
        self.mock_cloudwatch.delete_dashboards.return_value = {}

        with patch('src.workflow_aws_cloudwatch.BOTO3_AVAILABLE', True):
            self.integration = CloudWatchIntegration()
            self.integration._clients["cloudwatch"] = self.mock_cloudwatch

    def test_create_dashboard_with_body(self):
        dashboard_body = '{"widgets": []}'

        result = self.integration.create_dashboard(
            dashboard_name="MyDashboard",
            dashboard_body=dashboard_body
        )

        self.mock_cloudwatch.put_dashboard.assert_called_once()

    def test_create_dashboard_with_widgets(self):
        widgets = [
            DashboardWidget(
                widget_type="metric",
                title="CPU",
                width=6,
                height=6,
                properties={}
            )
        ]

        result = self.integration.create_dashboard(
            dashboard_name="MyDashboard",
            widgets=widgets
        )

        self.mock_cloudwatch.put_dashboard.assert_called_once()

    def test_get_dashboard(self):
        result = self.integration.get_dashboard(dashboard_name="MyDashboard")

        self.mock_cloudwatch.get_dashboard.assert_called_once()

    def test_list_dashboards(self):
        result = self.integration.list_dashboards()

        self.mock_cloudwatch.get_paginator.assert_called()

    def test_delete_dashboard(self):
        result = self.integration.delete_dashboard(dashboard_name="MyDashboard")

        self.mock_cloudwatch.delete_dashboards.assert_called_once()

    def test_update_dashboard(self):
        widgets = [
            DashboardWidget(
                widget_type="metric",
                title="CPU",
                width=6,
                height=6,
                properties={}
            )
        ]

        result = self.integration.update_dashboard(
            dashboard_name="MyDashboard",
            widgets=widgets
        )

        self.mock_cloudwatch.put_dashboard.assert_called()


class TestCloudWatchIntegrationLogs(unittest.TestCase):
    """Test CloudWatchIntegration Logs operations"""

    def setUp(self):
        self.mock_logs = MagicMock()
        self.mock_logs.create_log_group.return_value = {}
        self.mock_logs.describe_log_groups.return_value = {
            'logGroups': []
        }
        self.mock_logs.delete_log_group.return_value = {}
        self.mock_logs.put_log_events.return_value = {}
        self.mock_logs.create_log_stream.return_value = {}
        self.mock_logs.filter_log_events.return_value = {
            'events': []
        }
        self.mock_logs.start_query.return_value = {'queryId': 'query-123'}
        self.mock_logs.get_query_results.return_value = {
            'results': []
        }
        self.mock_logs.describe_resource_policies.return_value = []
        self.mock_logs.put_resource_policy.return_value = {}

        with patch('src.workflow_aws_cloudwatch.BOTO3_AVAILABLE', True):
            self.integration = CloudWatchIntegration()
            self.integration._clients["logs"] = self.mock_logs

    def test_create_log_group_basic(self):
        result = self.integration.create_log_group(log_group_name="/my/log/group")

        self.mock_logs.create_log_group.assert_called_once()

    def test_create_log_group_with_retention(self):
        result = self.integration.create_log_group(
            log_group_name="/my/log/group",
            retention_days=7
        )

        self.mock_logs.create_log_group.assert_called_once()
        call_args = self.mock_logs.create_log_group.call_args
        self.assertIn('retentionInDays', call_args.kwargs)

    def test_create_log_group_with_kms(self):
        result = self.integration.create_log_group(
            log_group_name="/my/log/group",
            kms_key_id="arn:aws:kms:us-east-1:123456789012:key/my-key"
        )

        self.mock_logs.create_log_group.assert_called_once()
        call_args = self.mock_logs.create_log_group.call_args
        self.assertIn('kmsKeyId', call_args.kwargs)

    def test_create_log_group_with_tags(self):
        result = self.integration.create_log_group(
            log_group_name="/my/log/group",
            tags={"Environment": "Production"}
        )

        self.mock_logs.create_log_group.assert_called_once()

    def test_delete_log_group(self):
        result = self.integration.delete_log_group(log_group_name="/my/log/group")

        self.mock_logs.delete_log_group.assert_called_once()

    def test_describe_log_groups(self):
        result = self.integration.describe_log_groups(log_group_name_prefix="/aws")

        self.mock_logs.get_paginator.assert_called_once()

    def test_put_log_events(self):
        result = self.integration.put_log_events(
            log_group_name="/my/log/group",
            log_stream_name="my-stream",
            log_events=[{"timestamp": 1234567890, "message": "test message"}]
        )

        self.mock_logs.put_log_events.assert_called_once()

    def test_put_log_events_with_sequence_token(self):
        result = self.integration.put_log_events(
            log_group_name="/my/log/group",
            log_stream_name="my-stream",
            log_events=[{"timestamp": 1234567890, "message": "test message"}],
            sequence_token="abc123"
        )

        self.mock_logs.put_log_events.assert_called_once()

    def test_create_log_stream(self):
        result = self.integration.create_log_stream(
            log_group_name="/my/log/group",
            log_stream_name="my-stream"
        )

        self.mock_logs.create_log_stream.assert_called_once()

    def test_filter_log_events(self):
        result = self.integration.filter_log_events(
            log_group_name="/my/log/group",
            filter_pattern="ERROR"
        )

        self.mock_logs.get_paginator.assert_called()

    def test_filter_log_events_with_time_range(self):
        # filter_log_events expects start_time and end_time as integers (milliseconds)
        start_time = int(datetime(2024, 1, 1, 0, 0, 0).timestamp() * 1000)
        end_time = int(datetime(2024, 1, 2, 0, 0, 0).timestamp() * 1000)

        result = self.integration.filter_log_events(
            log_group_name="/my/log/group",
            filter_pattern="ERROR",
            start_time=start_time,
            end_time=end_time
        )

        self.mock_logs.get_paginator.assert_called()

    def test_start_query(self):
        result = self.integration.start_query(
            log_group_name="/my/log/group",
            query_string="fields @timestamp, @message",
            start_time=datetime(2024, 1, 1, 0, 0, 0),
            end_time=datetime(2024, 1, 2, 0, 0, 0)
        )

        self.mock_logs.start_query.assert_called_once()

    def test_get_query_results(self):
        result = self.integration.get_query_results(query_id="query-123")

        self.mock_logs.get_query_results.assert_called_once()


class TestCloudWatchIntegrationEvents(unittest.TestCase):
    """Test CloudWatchIntegration Events/EventBridge operations"""

    def setUp(self):
        self.mock_events = MagicMock()
        self.mock_events.put_rule.return_value = {
            'RuleArn': 'arn:aws:events:us-east-1:123456789012:rule/my-rule'
        }
        self.mock_events.put_targets.return_value = {}
        self.mock_events.remove_targets.return_value = {}
        self.mock_events.delete_rule.return_value = {}
        self.mock_events.describe_rules.return_value = {'Rules': []}
        self.mock_events.put_events.return_value = {'Entries': []}
        self.mock_events.create_event_bus.return_value = {}

        with patch('src.workflow_aws_cloudwatch.BOTO3_AVAILABLE', True):
            self.integration = CloudWatchIntegration()
            self.integration._clients["events"] = self.mock_events

    def test_put_rule(self):
        result = self.integration.put_rule(
            name="my-rule",
            event_pattern={"source": ["aws.ec2"]}
        )

        self.mock_events.put_rule.assert_called_once()

    def test_put_rule_with_schedule(self):
        result = self.integration.put_rule(
            name="my-rule",
            schedule_expression="rate(5 minutes)"
        )

        self.mock_events.put_rule.assert_called_once()
        call_args = self.mock_events.put_rule.call_args
        self.assertIn('ScheduleExpression', call_args.kwargs)

    def test_put_targets(self):
        result = self.integration.put_targets(
            rule_name="my-rule",
            targets=[{"Id": "target-1", "Arn": "arn:aws:lambda:us-east-1:123456789012:function:my-function"}]
        )

        self.mock_events.put_targets.assert_called_once()

    def test_remove_targets(self):
        result = self.integration.remove_targets(
            rule_name="my-rule",
            target_ids=["target-1"]
        )

        self.mock_events.remove_targets.assert_called_once()

    def test_delete_rule(self):
        result = self.integration.delete_rule(name="my-rule")

        self.mock_events.delete_rule.assert_called_once()

    def test_put_events(self):
        result = self.integration.put_events(
            entries=[{"Source": "my-app", "DetailType": "my-event", "Detail": "{}"}]
        )

        self.mock_events.put_events.assert_called_once()

    def test_create_event_bus(self):
        result = self.integration.create_event_bus(
            name="my-bus"
        )

        self.mock_events.create_event_bus.assert_called_once()


class TestCloudWatchIntegrationSynthetics(unittest.TestCase):
    """Test CloudWatchIntegration Synthetics canary operations"""

    def setUp(self):
        self.mock_lambda = MagicMock()
        self.mock_s3 = MagicMock()
        self.mock_iam = MagicMock()
        self.mock_cloudwatch = MagicMock()

        self.mock_cloudwatch.get_paginator.return_value.paginate.return_value = [
            {'Canaries': []}
        ]
        self.mock_cloudwatch.get_canary_runs.return_value = {'CanaryRuns': []}
        self.mock_cloudwatch.put_canary.return_value = {}
        self.mock_cloudwatch.start_canary.return_value = {}
        self.mock_cloudwatch.stop_canary.return_value = {}
        self.mock_cloudwatch.delete_canary.return_value = {}

        with patch('src.workflow_aws_cloudwatch.BOTO3_AVAILABLE', True):
            self.integration = CloudWatchIntegration()
            self.integration._clients["lambda"] = self.mock_lambda
            self.integration._clients["s3"] = self.mock_s3
            self.integration._clients["iam"] = self.mock_iam
            self.integration._clients["cloudwatch"] = self.mock_cloudwatch

    def test_create_canary(self):
        config = CanaryConfig(
            name="my-canary",
            execution_role_arn="arn:aws:iam::123456789012:role/my-role",
            handler="exports.handler",
            code_bucket="my-bucket",
            code_key="canary.zip"
        )

        result = self.integration.create_canary(config=config)

        self.mock_cloudwatch.put_canary.assert_called_once()

    def test_start_canary(self):
        result = self.integration.start_canary(name="my-canary")

        self.mock_cloudwatch.start_canary.assert_called()

    def test_stop_canary(self):
        result = self.integration.stop_canary(name="my-canary")

        self.mock_cloudwatch.stop_canary.assert_called()

    def test_delete_canary(self):
        result = self.integration.delete_canary(name="my-canary")

        self.mock_cloudwatch.delete_canary.assert_called()

    def test_get_canary_runs(self):
        result = self.integration.get_canary_runs(name="my-canary")

        self.mock_cloudwatch.get_paginator.assert_called()


class TestCloudWatchIntegrationInsightRules(unittest.TestCase):
    """Test CloudWatchIntegration Contributor Insights operations"""

    def setUp(self):
        self.mock_cloudwatch = MagicMock()
        self.mock_cloudwatch.put_insight_rule.return_value = {}
        self.mock_cloudwatch.enable_insight_rules.return_value = {}
        self.mock_cloudwatch.disable_insight_rules.return_value = {}
        self.mock_cloudwatch.delete_insight_rules.return_value = {}
        self.mock_cloudwatch.describe_insight_rules.return_value = {'InsightRules': []}
        self.mock_cloudwatch.get_insight_rule_report.return_value = {'InsightRules': []}

        with patch('src.workflow_aws_cloudwatch.BOTO3_AVAILABLE', True):
            self.integration = CloudWatchIntegration()
            self.integration._clients["cloudwatch"] = self.mock_cloudwatch

    def test_create_insight_rule(self):
        rule = ContributorInsightRule(
            rule_name="my-rule",
            log_group_name="/aws/lambda/my-function",
            schema={"something": "value"}
        )

        result = self.integration.create_insight_rule(rule=rule)

        self.mock_cloudwatch.put_insight_rule.assert_called()

    def test_enable_insight_rule(self):
        result = self.integration.enable_insight_rule(rule_name="my-rule")

        self.mock_cloudwatch.enable_insight_rules.assert_called()

    def test_disable_insight_rule(self):
        result = self.integration.disable_insight_rule(rule_name="my-rule")

        self.mock_cloudwatch.disable_insight_rules.assert_called()

    def test_delete_insight_rule(self):
        result = self.integration.delete_insight_rule(rule_name="my-rule")

        self.mock_cloudwatch.delete_insight_rules.assert_called()

    def test_describe_insight_rules(self):
        result = self.integration.describe_insight_rules()

        self.mock_cloudwatch.describe_insight_rules.assert_called()

    def test_get_insight_rule_report(self):
        result = self.integration.get_insight_rule_report(rule_name="my-rule")

        self.mock_cloudwatch.get_insight_rule_report.assert_called()


class TestBoto3Availability(unittest.TestCase):
    """Test BOTO3_AVAILABLE flag"""

    def test_boto3_available_flag(self):
        # This tests the module-level flag is importable
        import src.workflow_aws_cloudwatch as cw_module
        self.assertTrue(hasattr(cw_module, 'BOTO3_AVAILABLE'))


if __name__ == '__main__':
    unittest.main()
