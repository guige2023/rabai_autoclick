"""
Tests for workflow_aws_budgets module
"""
import sys
sys.path.insert(0, '/Users/guige/my_project')

import unittest
from unittest.mock import Mock, patch, MagicMock, PropertyMock
import json
import time
import types
from datetime import datetime

# Create mock boto3 module before importing workflow_aws_budgets
mock_boto3 = types.ModuleType('boto3')
mock_session = MagicMock()
mock_boto3.Session = MagicMock(return_value=mock_session)
mock_boto3.client = MagicMock()

# Create mock botocore exceptions
mock_boto3_exceptions = types.ModuleType('botocore.exceptions')
mock_boto3_exceptions.ClientError = Exception
mock_boto3_exceptions.BotoCoreError = Exception

sys.modules['boto3'] = mock_boto3
sys.modules['botocore'] = types.ModuleType('botocore')
sys.modules['botocore.exceptions'] = mock_boto3_exceptions

# Now import the module
from src.workflow_aws_budgets import (
    BudgetsIntegration,
    BudgetType,
    TimeUnit,
    NotificationType,
    Subscriber,
    Notification,
    BudgetAction,
    AutoAdjustConfig,
    CostFilter,
)


class TestBudgetType(unittest.TestCase):
    """Test BudgetType enum"""
    def test_budget_type_values(self):
        self.assertEqual(BudgetType.COST.value, "COST")
        self.assertEqual(BudgetType.USAGE.value, "USAGE")
        self.assertEqual(BudgetType.RI_UTILIZATION.value, "RI_UTILIZATION")
        self.assertEqual(BudgetType.RI_COVERAGE.value, "RI_COVERAGE")
        self.assertEqual(BudgetType.SAVINGS_PLANS_UTILIZATION.value, "SAVINGS_PLANS_UTILIZATION")
        self.assertEqual(BudgetType.SAVINGS_PLANS_COVERAGE.value, "SAVINGS_PLANS_COVERAGE")


class TestTimeUnit(unittest.TestCase):
    """Test TimeUnit enum"""
    def test_time_unit_values(self):
        self.assertEqual(TimeUnit.DAILY.value, "DAILY")
        self.assertEqual(TimeUnit.WEEKLY.value, "WEEKLY")
        self.assertEqual(TimeUnit.MONTHLY.value, "MONTHLY")
        self.assertEqual(TimeUnit.QUARTERLY.value, "QUARTERLY")
        self.assertEqual(TimeUnit.ANNUALLY.value, "ANNUALLY")


class TestNotificationType(unittest.TestCase):
    """Test NotificationType enum"""
    def test_notification_type_values(self):
        self.assertEqual(NotificationType.ACTUAL.value, "ACTUAL")
        self.assertEqual(NotificationType.FORECASTED.value, "FORECASTED")
        self.assertEqual(NotificationType.THRESHOLD_BREACH.value, "THRESHOLD_BREACH")


class TestSubscriber(unittest.TestCase):
    """Test Subscriber dataclass"""
    def test_subscriber_defaults(self):
        subscriber = Subscriber(address="test@example.com", subscription_type="EMAIL")
        self.assertEqual(subscriber.address, "test@example.com")
        self.assertEqual(subscriber.subscription_type, "EMAIL")

    def test_subscriber_to_dict(self):
        subscriber = Subscriber(address="test@example.com", subscription_type="EMAIL")
        result = subscriber.to_dict()
        self.assertEqual(result["address"], "test@example.com")
        self.assertEqual(result["subscriptionType"], "EMAIL")


class TestNotification(unittest.TestCase):
    """Test Notification dataclass"""
    def test_notification_defaults(self):
        notification = Notification(
            threshold=80.0,
            notification_type=NotificationType.ACTUAL,
            comparison_operator="GREATER_THAN"
        )
        self.assertEqual(notification.threshold, 80.0)
        self.assertEqual(notification.notification_type, NotificationType.ACTUAL)
        self.assertEqual(notification.comparison_operator, "GREATER_THAN")
        self.assertEqual(notification.threshold_type, "PERCENTAGE")
        self.assertEqual(notification.subscribers, [])

    def test_notification_with_subscribers(self):
        subscriber = Subscriber(address="test@example.com", subscription_type="EMAIL")
        notification = Notification(
            threshold=80.0,
            notification_type=NotificationType.ACTUAL,
            comparison_operator="GREATER_THAN",
            subscribers=[subscriber]
        )
        self.assertEqual(len(notification.subscribers), 1)

    def test_notification_to_dict(self):
        subscriber = Subscriber(address="test@example.com", subscription_type="EMAIL")
        notification = Notification(
            threshold=80.0,
            notification_type=NotificationType.ACTUAL,
            comparison_operator="GREATER_THAN",
            subscribers=[subscriber]
        )
        result = notification.to_dict()
        self.assertEqual(result["threshold"], 80.0)
        self.assertEqual(result["notificationType"], "ACTUAL")
        self.assertEqual(result["comparisonOperator"], "GREATER_THAN")
        self.assertEqual(result["thresholdType"], "PERCENTAGE")
        self.assertEqual(len(result["subscribers"]), 1)


class TestBudgetAction(unittest.TestCase):
    """Test BudgetAction dataclass"""
    def test_budget_action_defaults(self):
        action = BudgetAction(
            action_threshold=80.0,
            definition={"IamActionDefinition": {}},
            execution_role_arn="arn:aws:iam::123456789012:role/TestRole",
            action_type="APPLY_IAM_POLICY",
            notification_type=NotificationType.ACTUAL
        )
        self.assertEqual(action.action_threshold, 80.0)
        self.assertEqual(action.action_type, "APPLY_IAM_POLICY")

    def test_budget_action_to_dict(self):
        action = BudgetAction(
            action_threshold=80.0,
            definition={"IamActionDefinition": {}},
            execution_role_arn="arn:aws:iam::123456789012:role/TestRole",
            action_type="APPLY_IAM_POLICY",
            notification_type=NotificationType.ACTUAL
        )
        result = action.to_dict()
        self.assertEqual(result["actionThreshold"]["actionThresholdValue"], 80.0)
        self.assertEqual(result["actionThreshold"]["actionThresholdType"], "PERCENTAGE")
        self.assertEqual(result["actionType"], "APPLY_IAM_POLICY")


class TestAutoAdjustConfig(unittest.TestCase):
    """Test AutoAdjustConfig dataclass"""
    def test_auto_adjust_config_defaults(self):
        config = AutoAdjustConfig(auto_adjust_type="FORECAST")
        self.assertEqual(config.auto_adjust_type, "FORECAST")
        self.assertEqual(config.lookback_period_days, 12)

    def test_auto_adjust_config_to_dict(self):
        config = AutoAdjustConfig(
            auto_adjust_type="HISTORICAL",
            lookback_period_days=6
        )
        result = config.to_dict()
        self.assertEqual(result["autoAdjustType"], "HISTORICAL")
        self.assertEqual(result["historicalOptions"]["lookbackConfiguration"]["lookbackPeriodDays"], 6)


class TestCostFilter(unittest.TestCase):
    """Test CostFilter dataclass"""
    def test_cost_filter_defaults(self):
        cost_filter = CostFilter(name="Service", values=["EC2", "S3"])
        self.assertEqual(cost_filter.name, "Service")
        self.assertEqual(cost_filter.values, ["EC2", "S3"])

    def test_cost_filter_to_dict(self):
        cost_filter = CostFilter(name="Service", values=["EC2", "S3"])
        result = cost_filter.to_dict()
        self.assertEqual(result, {"Service": ["EC2", "S3"]})


class TestBudgetsIntegration(unittest.TestCase):
    """Test BudgetsIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_budgets_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()
        self.mock_sns_client = MagicMock()
        self.mock_iam_client = MagicMock()

        mock_session.client = MagicMock(side_effect=lambda service, **kwargs: {
            "budgets": self.mock_budgets_client,
            "cloudwatch": self.mock_cloudwatch_client,
            "sns": self.mock_sns_client,
            "iam": self.mock_iam_client,
        }.get(service, MagicMock()))

    def test_create_budget(self):
        """Test create_budget method"""
        with patch('src.workflow_aws_budgets.boto3.Session', return_value=mock_session):
            integration = BudgetsIntegration(region="us-east-1")
            integration.budgets_client = self.mock_budgets_client

            self.mock_budgets_client.create_budget.return_value = {"ResponseMetadata": {}}

            result = integration.create_budget(
                budget_name="test-budget",
                budget_limit=1000.0,
                time_unit=TimeUnit.MONTHLY,
                budget_type=BudgetType.COST
            )

            self.assertEqual(result, {"ResponseMetadata": {}})
            self.mock_budgets_client.create_budget.assert_called_once()

    def test_create_budget_with_filters(self):
        """Test create_budget with cost filters"""
        with patch('src.workflow_aws_budgets.boto3.Session', return_value=mock_session):
            integration = BudgetsIntegration(region="us-east-1")
            integration.budgets_client = self.mock_budgets_client

            self.mock_budgets_client.create_budget.return_value = {"ResponseMetadata": {}}

            cost_filter = CostFilter(name="Service", values=["EC2"])

            result = integration.create_budget(
                budget_name="test-budget",
                budget_limit=1000.0,
                time_unit=TimeUnit.MONTHLY,
                budget_type=BudgetType.COST,
                cost_filters=[cost_filter]
            )

            self.mock_budgets_client.create_budget.assert_called_once()
            call_args = self.mock_budgets_client.create_budget.call_args
            self.assertIn("CostFilters", call_args.kwargs["Budget"])

    def test_get_budget(self):
        """Test get_budget method"""
        with patch('src.workflow_aws_budgets.boto3.Session', return_value=mock_session):
            integration = BudgetsIntegration(region="us-east-1")
            integration.budgets_client = self.mock_budgets_client

            self.mock_budgets_client.describe_budget.return_value = {"Budget": {"BudgetName": "test"}}

            result = integration.get_budget("test-budget")

            self.assertEqual(result, {"Budget": {"BudgetName": "test"}})
            self.mock_budgets_client.describe_budget.assert_called_once_with(BudgetName="test-budget")

    def test_list_budgets(self):
        """Test list_budgets method"""
        with patch('src.workflow_aws_budgets.boto3.Session', return_value=mock_session):
            integration = BudgetsIntegration(region="us-east-1")
            integration.budgets_client = self.mock_budgets_client

            mock_paginator = MagicMock()
            mock_paginator.paginate.return_value = [{"Budgets": [{"BudgetName": "budget1"}, {"BudgetName": "budget2"}]}]
            self.mock_budgets_client.get_paginator.return_value = mock_paginator

            result = integration.list_budgets()

            self.assertEqual(len(result), 2)
            self.assertEqual(result[0]["BudgetName"], "budget1")
            self.assertEqual(result[1]["BudgetName"], "budget2")

    def test_update_budget(self):
        """Test update_budget method"""
        with patch('src.workflow_aws_budgets.boto3.Session', return_value=mock_session):
            integration = BudgetsIntegration(region="us-east-1")
            integration.budgets_client = self.mock_budgets_client

            self.mock_budgets_client.update_budget.return_value = {"ResponseMetadata": {}}

            result = integration.update_budget(
                budget_name="test-budget",
                budget_limit=2000.0
            )

            self.assertEqual(result, {"ResponseMetadata": {}})
            self.mock_budgets_client.update_budget.assert_called_once()

    def test_delete_budget(self):
        """Test delete_budget method"""
        with patch('src.workflow_aws_budgets.boto3.Session', return_value=mock_session):
            integration = BudgetsIntegration(region="us-east-1")
            integration.budgets_client = self.mock_budgets_client

            self.mock_budgets_client.delete_budget.return_value = {"ResponseMetadata": {}}

            result = integration.delete_budget("test-budget")

            self.assertEqual(result, {"ResponseMetadata": {}})
            self.mock_budgets_client.delete_budget.assert_called_once_with(BudgetName="test-budget")

    def test_create_cost_budget(self):
        """Test create_cost_budget method"""
        with patch('src.workflow_aws_budgets.boto3.Session', return_value=mock_session):
            integration = BudgetsIntegration(region="us-east-1")
            integration.budgets_client = self.mock_budgets_client

            self.mock_budgets_client.create_budget.return_value = {"ResponseMetadata": {}}

            result = integration.create_cost_budget(
                budget_name="cost-budget",
                budget_limit=500.0
            )

            self.mock_budgets_client.create_budget.assert_called_once()

    def test_create_usage_budget(self):
        """Test create_usage_budget method"""
        with patch('src.workflow_aws_budgets.boto3.Session', return_value=mock_session):
            integration = BudgetsIntegration(region="us-east-1")
            integration.budgets_client = self.mock_budgets_client

            self.mock_budgets_client.create_budget.return_value = {"ResponseMetadata": {}}

            result = integration.create_usage_budget(
                budget_name="usage-budget",
                budget_limit=100.0,
                usage_unit="Hrs"
            )

            self.mock_budgets_client.create_budget.assert_called_once()

    def test_create_ri_utilization_budget(self):
        """Test create_ri_utilization_budget method"""
        with patch('src.workflow_aws_budgets.boto3.Session', return_value=mock_session):
            integration = BudgetsIntegration(region="us-east-1")
            integration.budgets_client = self.mock_budgets_client

            self.mock_budgets_client.create_budget.return_value = {"ResponseMetadata": {}}

            result = integration.create_ri_utilization_budget(
                budget_name="ri-util-budget",
                threshold=70.0
            )

            self.mock_budgets_client.create_budget.assert_called_once()

    def test_create_ri_coverage_budget(self):
        """Test create_ri_coverage_budget method"""
        with patch('src.workflow_aws_budgets.boto3.Session', return_value=mock_session):
            integration = BudgetsIntegration(region="us-east-1")
            integration.budgets_client = self.mock_budgets_client

            self.mock_budgets_client.create_budget.return_value = {"ResponseMetadata": {}}

            result = integration.create_ri_coverage_budget(
                budget_name="ri-cov-budget",
                threshold=80.0
            )

            self.mock_budgets_client.create_budget.assert_called_once()

    def test_create_savings_plans_utilization_budget(self):
        """Test create_savings_plans_utilization_budget method"""
        with patch('src.workflow_aws_budgets.boto3.Session', return_value=mock_session):
            integration = BudgetsIntegration(region="us-east-1")
            integration.budgets_client = self.mock_budgets_client

            self.mock_budgets_client.create_budget.return_value = {"ResponseMetadata": {}}

            result = integration.create_savings_plans_utilization_budget(
                budget_name="sp-util-budget",
                threshold=75.0
            )

            self.mock_budgets_client.create_budget.assert_called_once()

    def test_create_savings_plans_coverage_budget(self):
        """Test create_savings_plans_coverage_budget method"""
        with patch('src.workflow_aws_budgets.boto3.Session', return_value=mock_session):
            integration = BudgetsIntegration(region="us-east-1")
            integration.budgets_client = self.mock_budgets_client

            self.mock_budgets_client.create_budget.return_value = {"ResponseMetadata": {}}

            result = integration.create_savings_plans_coverage_budget(
                budget_name="sp-cov-budget",
                threshold=85.0
            )

            self.mock_budgets_client.create_budget.assert_called_once()

    def test_create_notification_with_subscribers(self):
        """Test create_notification_with_subscribers method"""
        with patch('src.workflow_aws_budgets.boto3.Session', return_value=mock_session):
            integration = BudgetsIntegration(region="us-east-1")
            integration.budgets_client = self.mock_budgets_client

            self.mock_budgets_client.create_notification.return_value = {"ResponseMetadata": {}}

            subscriber = Subscriber(address="test@example.com", subscription_type="EMAIL")
            notification = Notification(
                threshold=80.0,
                notification_type=NotificationType.ACTUAL,
                comparison_operator="GREATER_THAN",
                subscribers=[subscriber]
            )

            result = integration.create_notification_with_subscribers("test-budget", notification)

            self.assertEqual(result, {"ResponseMetadata": {}})
            self.mock_budgets_client.create_notification.assert_called_once()

    def test_add_subscriber(self):
        """Test add_subscriber method"""
        with patch('src.workflow_aws_budgets.boto3.Session', return_value=mock_session):
            integration = BudgetsIntegration(region="us-east-1")
            integration.budgets_client = self.mock_budgets_client

            self.mock_budgets_client.create_subscriber.return_value = {"ResponseMetadata": {}}

            subscriber = Subscriber(address="test@example.com", subscription_type="EMAIL")
            notification = Notification(
                threshold=80.0,
                notification_type=NotificationType.ACTUAL,
                comparison_operator="GREATER_THAN"
            )

            result = integration.add_subscriber("test-budget", notification, subscriber)

            self.assertEqual(result, {"ResponseMetadata": {}})
            self.mock_budgets_client.create_subscriber.assert_called_once()

    def test_remove_subscriber(self):
        """Test remove_subscriber method"""
        with patch('src.workflow_aws_budgets.boto3.Session', return_value=mock_session):
            integration = BudgetsIntegration(region="us-east-1")
            integration.budgets_client = self.mock_budgets_client

            self.mock_budgets_client.delete_subscriber.return_value = {"ResponseMetadata": {}}

            subscriber = Subscriber(address="test@example.com", subscription_type="EMAIL")
            notification = Notification(
                threshold=80.0,
                notification_type=NotificationType.ACTUAL,
                comparison_operator="GREATER_THAN"
            )

            result = integration.remove_subscriber("test-budget", notification, subscriber)

            self.assertEqual(result, {"ResponseMetadata": {}})
            self.mock_budgets_client.delete_subscriber.assert_called_once()

    def test_create_email_subscriber(self):
        """Test create_email_subscriber method"""
        with patch('src.workflow_aws_budgets.boto3.Session', return_value=mock_session):
            integration = BudgetsIntegration(region="us-east-1")

            result = integration.create_email_subscriber("test@example.com")

            self.assertEqual(result.address, "test@example.com")
            self.assertEqual(result.subscription_type, "EMAIL")

    def test_create_sns_subscriber(self):
        """Test create_sns_subscriber method"""
        with patch('src.workflow_aws_budgets.boto3.Session', return_value=mock_session):
            integration = BudgetsIntegration(region="us-east-1")

            result = integration.create_sns_subscriber("arn:aws:sns:us-east-1:123456789012:topic")

            self.assertEqual(result.address, "arn:aws:sns:us-east-1:123456789012:topic")
            self.assertEqual(result.subscription_type, "SNS")

    def test_create_threshold_alert(self):
        """Test create_threshold_alert method"""
        with patch('src.workflow_aws_budgets.boto3.Session', return_value=mock_session):
            integration = BudgetsIntegration(region="us-east-1")
            integration.budgets_client = self.mock_budgets_client

            self.mock_budgets_client.create_notification.return_value = {"ResponseMetadata": {}}

            result = integration.create_threshold_alert(
                budget_name="test-budget",
                threshold=80.0
            )

            self.assertEqual(result.threshold, 80.0)
            self.assertEqual(result.notification_type, NotificationType.ACTUAL)

    def test_create_budget_action(self):
        """Test create_budget_action method"""
        with patch('src.workflow_aws_budgets.boto3.Session', return_value=mock_session):
            integration = BudgetsIntegration(region="us-east-1")
            integration.budgets_client = self.mock_budgets_client
            integration.iam_client = self.mock_iam_client

            self.mock_budgets_client.create_budget_action.return_value = {"ResponseMetadata": {}}
            self.mock_iam_client.get_role.return_value = {
                "Role": {"Arn": "arn:aws:iam::123456789012:role/TestRole"}
            }

            subscriber = Subscriber(address="test@example.com", subscription_type="EMAIL")

            result = integration.create_budget_action(
                budget_name="test-budget",
                action_threshold=80.0,
                action_type="APPLY_IAM_POLICY",
                notification_type=NotificationType.ACTUAL,
                subscribers=[subscriber]
            )

            self.assertEqual(result, {"ResponseMetadata": {}})
            self.mock_budgets_client.create_budget_action.assert_called_once()

    def test_describe_budget_actions(self):
        """Test describe_budget_actions method"""
        with patch('src.workflow_aws_budgets.boto3.Session', return_value=mock_session):
            integration = BudgetsIntegration(region="us-east-1")
            integration.budgets_client = self.mock_budgets_client

            self.mock_budgets_client.describe_budget_actions.return_value = {
                "Actions": [{"ActionId": "action-1"}]
            }

            result = integration.describe_budget_actions("test-budget")

            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]["ActionId"], "action-1")

    def test_update_budget_action(self):
        """Test update_budget_action method"""
        with patch('src.workflow_aws_budgets.boto3.Session', return_value=mock_session):
            integration = BudgetsIntegration(region="us-east-1")
            integration.budgets_client = self.mock_budgets_client

            self.mock_budgets_client.update_budget_action.return_value = {"ResponseMetadata": {}}

            result = integration.update_budget_action(
                budget_name="test-budget",
                action_id="action-1"
            )

            self.assertEqual(result, {"ResponseMetadata": {}})
            self.mock_budgets_client.update_budget_action.assert_called_once()

    def test_delete_budget_action(self):
        """Test delete_budget_action method"""
        with patch('src.workflow_aws_budgets.boto3.Session', return_value=mock_session):
            integration = BudgetsIntegration(region="us-east-1")
            integration.budgets_client = self.mock_budgets_client

            self.mock_budgets_client.delete_budget_action.return_value = {"ResponseMetadata": {}}

            result = integration.delete_budget_action("test-budget", "action-1")

            self.assertEqual(result, {"ResponseMetadata": {}})
            self.mock_budgets_client.delete_budget_action.assert_called_once()

    def test_create_auto_adjusting_budget(self):
        """Test create_auto_adjusting_budget method"""
        with patch('src.workflow_aws_budgets.boto3.Session', return_value=mock_session):
            integration = BudgetsIntegration(region="us-east-1")
            integration.budgets_client = self.mock_budgets_client

            self.mock_budgets_client.create_budget.return_value = {"ResponseMetadata": {}}

            result = integration.create_auto_adjusting_budget(
                budget_name="auto-budget",
                auto_adjust_type="FORECAST",
                budget_limit=1000.0
            )

            self.mock_budgets_client.create_budget.assert_called_once()

    def test_create_historical_auto_adjusting_budget(self):
        """Test create_historical_auto_adjusting_budget method"""
        with patch('src.workflow_aws_budgets.boto3.Session', return_value=mock_session):
            integration = BudgetsIntegration(region="us-east-1")
            integration.budgets_client = self.mock_budgets_client

            self.mock_budgets_client.create_budget.return_value = {"ResponseMetadata": {}}

            result = integration.create_historical_auto_adjusting_budget(
                budget_name="historical-auto-budget",
                budget_limit=1000.0
            )

            self.mock_budgets_client.create_budget.assert_called_once()

    def test_create_forecast_auto_adjusting_budget(self):
        """Test create_forecast_auto_adjusting_budget method"""
        with patch('src.workflow_aws_budgets.boto3.Session', return_value=mock_session):
            integration = BudgetsIntegration(region="us-east-1")
            integration.budgets_client = self.mock_budgets_client

            self.mock_budgets_client.create_budget.return_value = {"ResponseMetadata": {}}

            result = integration.create_forecast_auto_adjusting_budget(
                budget_name="forecast-auto-budget",
                budget_limit=1000.0
            )

            self.mock_budgets_client.create_budget.assert_called_once()

    def test_get_budget_report(self):
        """Test get_budget_report method"""
        with patch('src.workflow_aws_budgets.boto3.Session', return_value=mock_session):
            integration = BudgetsIntegration(region="us-east-1")
            integration.budgets_client = self.mock_budgets_client

            self.mock_budgets_client.describe_budget.return_value = {"Budget": {"BudgetName": "test"}}
            self.mock_budgets_client.describe_budget_spend.return_value = {"BudgetSpend": {}}
            self.mock_budgets_client.describe_budget_history.return_value = {"BudgetHistoryList": []}

            result = integration.get_budget_report("test-budget")

            self.assertIn("budget", result)
            self.assertIn("spend", result)
            self.assertIn("history", result)

    def test_create_sns_topic(self):
        """Test create_sns_topic method"""
        with patch('src.workflow_aws_budgets.boto3.Session', return_value=mock_session):
            integration = BudgetsIntegration(region="us-east-1")
            integration.sns_client = self.mock_sns_client

            self.mock_sns_client.create_topic.return_value = {
                "TopicArn": "arn:aws:sns:us-east-1:123456789012:test-topic"
            }

            result = integration.create_sns_topic("test-topic")

            self.assertEqual(result, "arn:aws:sns:us-east-1:123456789012:test-topic")
            self.mock_sns_client.create_topic.assert_called_once()


if __name__ == '__main__':
    unittest.main()
