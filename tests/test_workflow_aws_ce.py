"""
Tests for workflow_aws_ce module
"""
import sys
sys.path.insert(0, '/Users/guige/my_project')

import unittest
from unittest.mock import Mock, patch, MagicMock, PropertyMock
import json
import time
import types
from datetime import datetime

# Create mock boto3 module before importing workflow_aws_ce
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
from src.workflow_aws_ce import (
    CostExplorerIntegration,
    CostMetricType,
    GranularityType,
    AnomalyDetectionMode,
    CostExplorerConfig,
    CostQueryConfig,
    CostForecastConfig,
    ReservationCoverageConfig,
    ReservationRecommendationConfig,
    SavingsPlansRecommendationConfig,
    AnomalyDetectionConfig,
    CostCategoryConfig,
)


class TestCostMetricType(unittest.TestCase):
    """Test CostMetricType enum"""
    def test_cost_metric_type_values(self):
        self.assertEqual(CostMetricType.BLENDED_COST.value, "BlendedCost")
        self.assertEqual(CostMetricType.UNBLENDED_COST.value, "UnblendedCost")
        self.assertEqual(CostMetricType.AMORTIZED_COST.value, "AmortizedCost")
        self.assertEqual(CostMetricType.NET_AMORTIZED_COST.value, "NetAmortizedCost")
        self.assertEqual(CostMetricType.USAGE_QUANTITY.value, "UsageQuantity")


class TestGranularityType(unittest.TestCase):
    """Test GranularityType enum"""
    def test_granularity_type_values(self):
        self.assertEqual(GranularityType.DAILY.value, "DAILY")
        self.assertEqual(GranularityType.MONTHLY.value, "MONTHLY")
        self.assertEqual(GranularityType.HOURLY.value, "HOURLY")


class TestAnomalyDetectionMode(unittest.TestCase):
    """Test AnomalyDetectionMode enum"""
    def test_anomaly_detection_mode_values(self):
        self.assertEqual(AnomalyDetectionMode.ENTIRE_ORGANIZATION.value, "ENTIRE_ORGANIZATION")
        self.assertEqual(AnomalyDetectionMode.SPECIFIC_ACCOUNTS.value, "SPECIFIC_ACCOUNTS")
        self.assertEqual(AnomalyDetectionMode.SPECIFIC_SERVICES.value, "SPECIFIC_SERVICES")


class TestCostExplorerConfig(unittest.TestCase):
    """Test CostExplorerConfig dataclass"""
    def test_cost_explorer_config_defaults(self):
        config = CostExplorerConfig()
        self.assertEqual(config.region_name, "us-east-1")
        self.assertIsNone(config.aws_access_key_id)
        self.assertIsNone(config.aws_secret_access_key)

    def test_cost_explorer_config_full(self):
        config = CostExplorerConfig(
            region_name="us-west-2",
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            profile_name="test-profile"
        )
        self.assertEqual(config.region_name, "us-west-2")
        self.assertEqual(config.aws_access_key_id, "test-key")
        self.assertEqual(config.profile_name, "test-profile")


class TestCostQueryConfig(unittest.TestCase):
    """Test CostQueryConfig dataclass"""
    def test_cost_query_config_defaults(self):
        config = CostQueryConfig(
            time_period_start="2024-01-01",
            time_period_end="2024-01-31"
        )
        self.assertEqual(config.time_period_start, "2024-01-01")
        self.assertEqual(config.time_period_end, "2024-01-31")
        self.assertEqual(config.granularity, GranularityType.DAILY)
        self.assertEqual(config.metrics, [CostMetricType.BLENDED_COST])

    def test_cost_query_config_full(self):
        config = CostQueryConfig(
            time_period_start="2024-01-01",
            time_period_end="2024-01-31",
            granularity=GranularityType.MONTHLY,
            metrics=[CostMetricType.BLENDED_COST, CostMetricType.USAGE_QUANTITY],
            group_by=["SERVICE", "REGION"]
        )
        self.assertEqual(config.granularity, GranularityType.MONTHLY)
        self.assertEqual(len(config.metrics), 2)
        self.assertEqual(len(config.group_by), 2)


class TestCostForecastConfig(unittest.TestCase):
    """Test CostForecastConfig dataclass"""
    def test_cost_forecast_config_defaults(self):
        config = CostForecastConfig(
            time_period_start="2024-01-01",
            time_period_end="2024-01-31"
        )
        self.assertEqual(config.metric, CostMetricType.BLENDED_COST)
        self.assertEqual(config.prediction_interval_level, 85)

    def test_cost_forecast_config_full(self):
        config = CostForecastConfig(
            time_period_start="2024-01-01",
            time_period_end="2024-01-31",
            metric=CostMetricType.UNBLENDED_COST,
            prediction_interval_level=90
        )
        self.assertEqual(config.metric, CostMetricType.UNBLENDED_COST)
        self.assertEqual(config.prediction_interval_level, 90)


class TestReservationCoverageConfig(unittest.TestCase):
    """Test ReservationCoverageConfig dataclass"""
    def test_reservation_coverage_config_defaults(self):
        config = ReservationCoverageConfig(
            time_period_start="2024-01-01",
            time_period_end="2024-01-31"
        )
        self.assertEqual(config.granularity, GranularityType.MONTHLY)


class TestReservationRecommendationConfig(unittest.TestCase):
    """Test ReservationRecommendationConfig dataclass"""
    def test_reservation_recommendation_config_defaults(self):
        config = ReservationRecommendationConfig()
        self.assertEqual(config.service, "Amazon EC2")
        self.assertIsNone(config.account_id)

    def test_reservation_recommendation_config_full(self):
        config = ReservationRecommendationConfig(
            service="Amazon RDS",
            account_id="123456789012"
        )
        self.assertEqual(config.service, "Amazon RDS")
        self.assertEqual(config.account_id, "123456789012")


class TestSavingsPlansRecommendationConfig(unittest.TestCase):
    """Test SavingsPlansRecommendationConfig dataclass"""
    def test_savings_plans_recommendation_config_defaults(self):
        config = SavingsPlansRecommendationConfig()
        self.assertEqual(config.SavingsPlansType, "General")
        self.assertEqual(config.term, "ONE_YEAR")
        self.assertEqual(config.payment_option, "NO_UPFRONT")


class TestAnomalyDetectionConfig(unittest.TestCase):
    """Test AnomalyDetectionConfig dataclass"""
    def test_anomaly_detection_config_defaults(self):
        config = AnomalyDetectionConfig()
        self.assertEqual(config.mode, AnomalyDetectionMode.ENTIRE_ORGANIZATION)
        self.assertEqual(config.threshold, 1.0)
        self.assertEqual(config.frequency, "DAILY")

    def test_anomaly_detection_config_full(self):
        config = AnomalyDetectionConfig(
            mode=AnomalyDetectionMode.SPECIFIC_ACCOUNTS,
            account_ids=["123456789012"],
            threshold=2.0,
            frequency="WEEKLY"
        )
        self.assertEqual(config.mode, AnomalyDetectionMode.SPECIFIC_ACCOUNTS)
        self.assertEqual(len(config.account_ids), 1)


class TestCostCategoryConfig(unittest.TestCase):
    """Test CostCategoryConfig dataclass"""
    def test_cost_category_config_defaults(self):
        config = CostCategoryConfig(
            name="test-category",
            cost_category_name="TestCategory"
        )
        self.assertEqual(config.rule_version, "CostCategoryExpression.v1")
        self.assertEqual(config.rules, [])


class TestCostExplorerIntegration(unittest.TestCase):
    """Test CostExplorerIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_ce_client = MagicMock()
        self.mock_org_client = MagicMock()

    def test_initialization_with_config(self):
        """Test initialization with CostExplorerConfig"""
        config = CostExplorerConfig(region_name="us-west-2")
        with patch('src.workflow_aws_ce.boto3.client', return_value=self.mock_ce_client):
            integration = CostExplorerIntegration(config=config)
            self.assertEqual(integration.config.region_name, "us-west-2")

    def test_get_cost_and_usage(self):
        """Test get_cost_and_usage method"""
        with patch('src.workflow_aws_ce.boto3.client', return_value=self.mock_ce_client):
            self.mock_ce_client.get_cost_and_usage.return_value = {
                "ResultsByTime": [
                    {
                        "TimePeriod": {"Start": "2024-01-01", "End": "2024-01-02"},
                        "Groups": [],
                        "Total": {}
                    }
                ]
            }

            config = CostExplorerConfig()
            integration = CostExplorerIntegration(config=config)
            integration._client = self.mock_ce_client

            query_config = CostQueryConfig(
                time_period_start="2024-01-01",
                time_period_end="2024-01-31"
            )

            result = integration.get_cost_and_usage(query_config)

            self.assertTrue(result["success"])
            self.assertEqual(len(result["data"]), 1)

    def test_get_cost_and_usage_with_error(self):
        """Test get_cost_and_usage method with error"""
        with patch('src.workflow_aws_ce.boto3.client', return_value=self.mock_ce_client):
            self.mock_ce_client.get_cost_and_usage.side_effect = Exception("Test error")

            config = CostExplorerConfig()
            integration = CostExplorerIntegration(config=config)
            integration._client = self.mock_ce_client

            query_config = CostQueryConfig(
                time_period_start="2024-01-01",
                time_period_end="2024-01-31"
            )

            result = integration.get_cost_and_usage(query_config)

            self.assertFalse(result["success"])
            self.assertIn("error", result)

    def test_get_cost_breakdown_by_service(self):
        """Test get_cost_breakdown_by_service method"""
        with patch('src.workflow_aws_ce.boto3.client', return_value=self.mock_ce_client):
            self.mock_ce_client.get_cost_and_usage.return_value = {
                "ResultsByTime": [
                    {
                        "TimePeriod": {"Start": "2024-01-01", "End": "2024-01-31"},
                        "Groups": [
                            {
                                "Keys": ["Amazon EC2"],
                                "Metrics": {
                                    "BlendedCost": {"Amount": "100.50", "Unit": "USD"},
                                    "UsageQuantity": {"Amount": "50", "Unit": "Hrs"}
                                }
                            }
                        ],
                        "Total": {}
                    }
                ]
            }

            config = CostExplorerConfig()
            integration = CostExplorerIntegration(config=config)
            integration._client = self.mock_ce_client

            result = integration.get_cost_breakdown_by_service("2024-01-01", "2024-01-31")

            self.assertTrue(result["success"])
            self.assertIn("service_costs", result)
            self.assertEqual(len(result["service_costs"]), 1)

    def test_get_cost_breakdown_by_region(self):
        """Test get_cost_breakdown_by_region method"""
        with patch('src.workflow_aws_ce.boto3.client', return_value=self.mock_ce_client):
            self.mock_ce_client.get_cost_and_usage.return_value = {
                "ResultsByTime": [
                    {
                        "TimePeriod": {"Start": "2024-01-01", "End": "2024-01-31"},
                        "Groups": [
                            {
                                "Keys": ["us-east-1"],
                                "Metrics": {
                                    "BlendedCost": {"Amount": "200.00", "Unit": "USD"}
                                }
                            }
                        ],
                        "Total": {}
                    }
                ]
            }

            config = CostExplorerConfig()
            integration = CostExplorerIntegration(config=config)
            integration._client = self.mock_ce_client

            result = integration.get_cost_breakdown_by_region("2024-01-01", "2024-01-31")

            self.assertTrue(result["success"])
            self.assertIn("region_costs", result)

    def test_get_cost_breakdown_by_linked_account(self):
        """Test get_cost_breakdown_by_linked_account method"""
        with patch('src.workflow_aws_ce.boto3.client', return_value=self.mock_ce_client):
            self.mock_ce_client.get_cost_and_usage.return_value = {
                "ResultsByTime": [
                    {
                        "TimePeriod": {"Start": "2024-01-01", "End": "2024-01-31"},
                        "Groups": [
                            {
                                "Keys": ["123456789012"],
                                "Metrics": {
                                    "BlendedCost": {"Amount": "500.00", "Unit": "USD"},
                                    "UnblendedCost": {"Amount": "490.00", "Unit": "USD"}
                                }
                            }
                        ],
                        "Total": {}
                    }
                ]
            }

            config = CostExplorerConfig()
            integration = CostExplorerIntegration(config=config)
            integration._client = self.mock_ce_client

            result = integration.get_cost_breakdown_by_linked_account("2024-01-01", "2024-01-31")

            self.assertTrue(result["success"])
            self.assertIn("account_costs", result)

    def test_get_cost_forecast(self):
        """Test get_cost_forecast method"""
        with patch('src.workflow_aws_ce.boto3.client', return_value=self.mock_ce_client):
            self.mock_ce_client.get_cost_forecast.return_value = {
                "ForecastResultsByTime": [
                    {
                        "TimePeriod": {"Start": "2024-02-01", "End": "2024-02-28"},
                        "MeanValue": "1500.00",
                        "PredictionIntervalLowerBound": "1000.00",
                        "PredictionIntervalUpperBound": "2000.00"
                    }
                ]
            }

            config = CostExplorerConfig()
            integration = CostExplorerIntegration(config=config)
            integration._client = self.mock_ce_client

            forecast_config = CostForecastConfig(
                time_period_start="2024-02-01",
                time_period_end="2024-02-28"
            )

            result = integration.get_cost_forecast(forecast_config)

            self.assertTrue(result["success"])
            self.assertEqual(len(result["forecast_results"]), 1)

    def test_get_cost_forecast_with_error(self):
        """Test get_cost_forecast method with error"""
        with patch('src.workflow_aws_ce.boto3.client', return_value=self.mock_ce_client):
            self.mock_ce_client.get_cost_forecast.side_effect = Exception("Test error")

            config = CostExplorerConfig()
            integration = CostExplorerIntegration(config=config)
            integration._client = self.mock_ce_client

            forecast_config = CostForecastConfig(
                time_period_start="2024-02-01",
                time_period_end="2024-02-28"
            )

            result = integration.get_cost_forecast(forecast_config)

            self.assertFalse(result["success"])
            self.assertIn("error", result)

    def test_get_reservation_coverage(self):
        """Test get_reservation_coverage method"""
        with patch('src.workflow_aws_ce.boto3.client', return_value=self.mock_ce_client):
            self.mock_ce_client.get_reservation_coverage.return_value = {
                "CoveragesByTime": [
                    {
                        "TimePeriod": {"Start": "2024-01-01", "End": "2024-01-31"},
                        "Coverage": {
                            "CoverageHours": {"CoveragePercentage": "75.5"},
                            "CoverageNormalizedUnits": {"CoveragePercentage": "80.0"}
                        }
                    }
                ],
                "Total": {
                    "CoverageHours": {"CoveragePercentage": "75.5"}
                }
            }

            config = CostExplorerConfig()
            integration = CostExplorerIntegration(config=config)
            integration._client = self.mock_ce_client

            coverage_config = ReservationCoverageConfig(
                time_period_start="2024-01-01",
                time_period_end="2024-01-31"
            )

            result = integration.get_reservation_coverage(coverage_config)

            self.assertTrue(result["success"])
            self.assertIn("coverage_by_time", result)

    def test_get_reservation_recommendations(self):
        """Test get_reservation_recommendations method"""
        with patch('src.workflow_aws_ce.boto3.client', return_value=self.mock_ce_client):
            self.mock_ce_client.get_reservation_recommendations.return_value = {
                "ReservationRecommendations": [
                    {
                        "Service": "Amazon EC2",
                        "Recommendation": {"RecommendedInstance": "m5.xlarge"}
                    }
                ]
            }

            config = CostExplorerConfig()
            integration = CostExplorerIntegration(config=config)
            integration._client = self.mock_ce_client

            rec_config = ReservationRecommendationConfig(service="Amazon EC2")

            result = integration.get_reservation_recommendations(rec_config)

            self.assertTrue(result["success"])
            self.assertEqual(len(result["recommendations"]), 1)

    def test_get_savings_plans_recommendations(self):
        """Test get_savings_plans_recommendations method"""
        with patch('src.workflow_aws_ce.boto3.client', return_value=self.mock_ce_client):
            self.mock_ce_client.get_savings_plans_recommendation.return_value = {
                "SavingsPlansRecommendations": [
                    {
                        "SavingsPlansType": "General",
                        "Term": "ONE_YEAR",
                        "PaymentOption": "NO_UPFRONT"
                    }
                ]
            }

            config = CostExplorerConfig()
            integration = CostExplorerIntegration(config=config)
            integration._client = self.mock_ce_client

            sp_config = SavingsPlansRecommendationConfig()

            result = integration.get_savings_plans_recommendations(sp_config)

            self.assertTrue(result["success"])
            self.assertEqual(len(result["recommendations"]), 1)

    def test_get_cost_anomaly_detection(self):
        """Test get_cost_anomaly_detection method"""
        with patch('src.workflow_aws_ce.boto3.client', return_value=self.mock_ce_client):
            self.mock_ce_client.get_cost_anomalies.return_value = {
                "Anomalies": [
                    {
                        "AnomalyId": "anomaly-1",
                        "Service": "Amazon EC2",
                        "Region": "us-east-1",
                        "ActualSpend": {"Amount": "500", "Unit": "USD"},
                        "ExpectedSpend": {"Amount": "200", "Unit": "USD"},
                        "Impact": {"Amount": "300", "Unit": "USD"}
                    }
                ]
            }

            config = CostExplorerConfig()
            integration = CostExplorerIntegration(config=config)
            integration._client = self.mock_ce_client

            result = integration.get_cost_anomaly_detection("2024-01-01", "2024-01-31")

            self.assertTrue(result["success"])
            self.assertEqual(result["total_count"], 1)

    def test_create_anomaly_subscription(self):
        """Test create_anomaly_subscription method"""
        with patch('src.workflow_aws_ce.boto3.client', return_value=self.mock_ce_client):
            self.mock_ce_client.create_anomaly_subscription.return_value = {
                "AnomalySubscription": {"SubscriptionName": "test-sub"},
                "SubscriptionArn": "arn:aws:ce::123456789012:anomaly-subscription/test"
            }

            config = CostExplorerConfig()
            integration = CostExplorerIntegration(config=config)
            integration._client = self.mock_ce_client

            sub_config = AnomalyDetectionConfig()

            result = integration.create_anomaly_subscription(sub_config, "test@example.com")

            self.assertTrue(result["success"])
            self.assertIn("subscription", result)

    def test_get_tag_values(self):
        """Test get_tag_values method"""
        with patch('src.workflow_aws_ce.boto3.client', return_value=self.mock_ce_client):
            self.mock_ce_client.get_tags.return_value = {
                "Tags": [
                    {"Key": "Environment", "Value": "Production"}
                ]
            }

            config = CostExplorerConfig()
            integration = CostExplorerIntegration(config=config)
            integration._client = self.mock_ce_client

            result = integration.get_tag_values("Environment", "2024-01-01", "2024-01-31")

            self.assertTrue(result["success"])
            self.assertEqual(result["tag_key"], "Environment")

    def test_get_cost_by_tag(self):
        """Test get_cost_by_tag method"""
        with patch('src.workflow_aws_ce.boto3.client', return_value=self.mock_ce_client):
            self.mock_ce_client.get_cost_and_usage.return_value = {
                "ResultsByTime": [
                    {
                        "TimePeriod": {"Start": "2024-01-01", "End": "2024-01-31"},
                        "Groups": [
                            {
                                "Keys": ["Production"],
                                "Metrics": {
                                    "BlendedCost": {"Amount": "1000.00", "Unit": "USD"}
                                }
                            }
                        ],
                        "Total": {}
                    }
                ]
            }

            config = CostExplorerConfig()
            integration = CostExplorerIntegration(config=config)
            integration._client = self.mock_ce_client

            result = integration.get_cost_by_tag("Environment", "2024-01-01", "2024-01-31")

            self.assertTrue(result["success"])
            self.assertIn("tag_costs", result)

    def test_create_cost_category(self):
        """Test create_cost_category method"""
        with patch('src.workflow_aws_ce.boto3.client', return_value=self.mock_ce_client):
            self.mock_ce_client.create_cost_category.return_value = {
                "CostCategory": {"Name": "TestCategory"},
                "CostCategoryArn": "arn:aws:ce::123456789012:cost-category/TestCategory"
            }

            config = CostExplorerConfig()
            integration = CostExplorerIntegration(config=config)
            integration._client = self.mock_ce_client

            cat_config = CostCategoryConfig(
                name="test",
                cost_category_name="TestCategory",
                rules=[{"Value": "Production", "Rule": {}}]
            )

            result = integration.create_cost_category(cat_config)

            self.assertTrue(result["success"])
            self.assertIn("cost_category", result)

    def test_get_cost_category(self):
        """Test get_cost_category method"""
        with patch('src.workflow_aws_ce.boto3.client', return_value=self.mock_ce_client):
            self.mock_ce_client.describe_cost_categories.return_value = {
                "CostCategoryNames": ["TestCategory"]
            }

            config = CostExplorerConfig()
            integration = CostExplorerIntegration(config=config)
            integration._client = self.mock_ce_client

            result = integration.get_cost_category("TestCategory")

            self.assertTrue(result["success"])
            self.assertEqual(len(result["cost_categories"]), 1)

    def test_get_cost_by_cost_category(self):
        """Test get_cost_by_cost_category method"""
        with patch('src.workflow_aws_ce.boto3.client', return_value=self.mock_ce_client):
            self.mock_ce_client.get_cost_and_usage.return_value = {
                "ResultsByTime": [
                    {
                        "TimePeriod": {"Start": "2024-01-01", "End": "2024-01-31"},
                        "Groups": [
                            {
                                "Keys": ["Production"],
                                "Metrics": {
                                    "BlendedCost": {"Amount": "1000.00", "Unit": "USD"},
                                    "AmortizedCost": {"Amount": "950.00", "Unit": "USD"}
                                }
                            }
                        ],
                        "Total": {}
                    }
                ]
            }

            config = CostExplorerConfig()
            integration = CostExplorerIntegration(config=config)
            integration._client = self.mock_ce_client

            result = integration.get_cost_by_cost_category("Environment", "2024-01-01", "2024-01-31")

            self.assertTrue(result["success"])
            self.assertIn("category_costs", result)

    def test_generate_visualization_data(self):
        """Test generate_visualization_data method"""
        with patch('src.workflow_aws_ce.boto3.client', return_value=self.mock_ce_client):
            self.mock_ce_client.get_cost_and_usage.return_value = {
                "ResultsByTime": [
                    {
                        "TimePeriod": {"Start": "2024-01-01", "End": "2024-01-02"},
                        "Groups": [
                            {
                                "Keys": ["Amazon EC2"],
                                "Metrics": {
                                    "BlendedCost": {"Amount": "100.50", "Unit": "USD"},
                                    "UsageQuantity": {"Amount": "50", "Unit": "Hrs"}
                                }
                            }
                        ],
                        "Total": {}
                    }
                ]
            }

            config = CostExplorerConfig()
            integration = CostExplorerIntegration(config=config)
            integration._client = self.mock_ce_client

            result = integration.generate_visualization_data("2024-01-01", "2024-01-31")

            self.assertTrue(result["success"])
            self.assertIn("visualization", result)
            self.assertIn("summary", result["visualization"])
            self.assertIn("time_series", result["visualization"])

    def test_get_organization_cost_data(self):
        """Test get_organization_cost_data method"""
        with patch('src.workflow_aws_ce.boto3.client', return_value=self.mock_ce_client):
            self.mock_ce_client.get_cost_and_usage.return_value = {
                "ResultsByTime": [
                    {
                        "TimePeriod": {"Start": "2024-01-01", "End": "2024-01-31"},
                        "Groups": [
                            {
                                "Keys": ["123456789012"],
                                "Metrics": {
                                    "BlendedCost": {"Amount": "500.00", "Unit": "USD"},
                                    "UnblendedCost": {"Amount": "490.00", "Unit": "USD"}
                                }
                            }
                        ],
                        "Total": {}
                    }
                ]
            }
            self.mock_org_client.get_paginator.return_value.paginate.return_value = [
                {"Accounts": [{"Id": "123456789012", "Name": "TestAccount", "Status": "ACTIVE"}]}
            ]

            config = CostExplorerConfig()
            integration = CostExplorerIntegration(config=config)
            integration._client = self.mock_ce_client
            integration._org_client = self.mock_org_client

            result = integration.get_organization_cost_data("2024-01-01", "2024-01-31")

            self.assertTrue(result["success"])
            self.assertIn("organization_accounts", result)
            self.assertIn("cost_by_account", result)

    def test_build_filter_expression(self):
        """Test _build_filter_expression method"""
        config = CostExplorerConfig()
        integration = CostExplorerIntegration(config=config)

        # Test simple filter
        simple_filter = integration._build_filter_expression({"Service": "Amazon EC2"})
        self.assertEqual(simple_filter, {"Dimensions": {"Key": "Service", "Values": ["Amazon EC2"]}})

        # Test list filter (OR conditions)
        list_filter = integration._build_filter_expression({"Service": ["Amazon EC2", "Amazon RDS"]})
        self.assertIn("Or", list_filter)

        # Test multiple filters (AND conditions)
        multi_filter = integration._build_filter_expression({
            "Service": "Amazon EC2",
            "Region": "us-east-1"
        })
        self.assertIn("And", multi_filter)

    def test_close_and_context_manager(self):
        """Test close method and context manager"""
        config = CostExplorerConfig()
        integration = CostExplorerIntegration(config=config)
        integration._client = self.mock_ce_client
        integration._org_client = self.mock_org_client

        # Test close
        integration.close()
        self.assertIsNone(integration._client)
        self.assertIsNone(integration._org_client)

        # Test context manager
        with CostExplorerIntegration(config=config) as ce:
            ce._client = self.mock_ce_client
            ce._org_client = self.mock_org_client
        # After exit, clients should be closed
        self.assertIsNone(ce._client)


if __name__ == '__main__':
    unittest.main()
