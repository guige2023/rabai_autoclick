"""
AWS Cost Explorer Integration Module for Workflow System

Implements a CostExplorerIntegration class with:
1. Cost data: Get cost and usage data
2. Cost breakdown: By service, region, linked account
3. Cost forecast: Future cost prediction
4. Reservation coverage: RI coverage analysis
5. Reservation recommendations: RI purchase recommendations
6. Savings Plans: Savings Plans recommendations
7. Anomaly detection: Cost anomaly detection
8. Tags: Tag tracking and allocation
9. Cost categories: Define cost categories
10. Visualizations: Cost visualization data

Commit: 'feat(aws-ce): add AWS Cost Explorer with cost data, breakdown, forecast, RI coverage, recommendations, Savings Plans, anomaly detection, tags, cost categories, visualizations'
"""

import uuid
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional, Set, Union, Awaitable
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum
import copy
import threading
import hashlib
import base64

try:
    import boto3
    from botocore.exceptions import ClientError, BotoCoreError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    boto3 = None
    ClientError = None
    BotoCoreError = None


logger = logging.getLogger(__name__)


class CostMetricType(Enum):
    """Cost metric types for Cost Explorer."""
    BLENDED_COST = "BlendedCost"
    UNBLENDED_COST = "UnblendedCost"
    AMORTIZED_COST = "AmortizedCost"
    NET_AMORTIZED_COST = "NetAmortizedCost"
    USAGE_QUANTITY = "UsageQuantity"


class GranularityType(Enum):
    """Time granularity for cost data."""
    DAILY = "DAILY"
    MONTHLY = "MONTHLY"
    HOURLY = "HOURLY"


class AnomalyDetectionMode(Enum):
    """Anomaly detection subscription modes."""
    ENTIRE_ORGANIZATION = "ENTIRE_ORGANIZATION"
    SPECIFIC_ACCOUNTS = "SPECIFIC_ACCOUNTS"
    SPECIFIC_SERVICES = "SPECIFIC_SERVICES"


@dataclass
class CostExplorerConfig:
    """Configuration for Cost Explorer connection."""
    region_name: str = "us-east-1"
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    profile_name: Optional[str] = None


@dataclass
class CostQueryConfig:
    """Configuration for cost queries."""
    time_period_start: str
    time_period_end: str
    granularity: GranularityType = GranularityType.DAILY
    metrics: List[CostMetricType] = field(default_factory=lambda: [CostMetricType.BLENDED_COST])
    group_by: List[str] = field(default_factory=list)
    filter_dimensions: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CostForecastConfig:
    """Configuration for cost forecasts."""
    time_period_start: str
    time_period_end: str
    metric: CostMetricType = CostMetricType.BLENDED_COST
    prediction_interval_level: int = 85


@dataclass
class ReservationCoverageConfig:
    """Configuration for reservation coverage queries."""
    time_period_start: str
    time_period_end: str
    granularity: GranularityType = GranularityType.MONTHLY
    filter_dimensions: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ReservationRecommendationConfig:
    """Configuration for reservation recommendations."""
    service: str = "Amazon EC2"
    account_id: Optional[str] = None
    filter_dimensions: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SavingsPlansRecommendationConfig:
    """Configuration for Savings Plans recommendations."""
    SavingsPlansType: str = "General"
    term: str = "ONE_YEAR"
    payment_option: str = "NO_UPFRONT"
    account_id: Optional[str] = None


@dataclass
class AnomalyDetectionConfig:
    """Configuration for cost anomaly detection."""
    mode: AnomalyDetectionMode = AnomalyDetectionMode.ENTIRE_ORGANIZATION
    account_ids: List[str] = field(default_factory=list)
    service_whitelist: List[str] = field(default_factory=list)
    service_blacklist: List[str] = field(default_factory=list)
    threshold: float = 1.0
    frequency: str = "DAILY"


@dataclass
class CostCategoryConfig:
    """Configuration for cost categories."""
    name: str
    cost_category_name: str
    rule_version: str = "CostCategoryExpression.v1"
    rules: List[Dict[str, Any]] = field(default_factory=list)


class CostExplorerIntegration:
    """
    AWS Cost Explorer Integration.
    
    Provides comprehensive AWS cost management including:
    - Cost and usage data retrieval
    - Cost breakdown by service, region, linked account
    - Cost forecasting with prediction intervals
    - Reservation coverage analysis
    - Reservation purchase recommendations
    - Savings Plans recommendations
    - Cost anomaly detection
    - Tag-based cost allocation
    - Cost category management
    - Cost visualization data generation
    """
    
    def __init__(self, config: Optional[CostExplorerConfig] = None):
        """Initialize the Cost Explorer integration."""
        self.config = config or CostExplorerConfig()
        self._client = None
        self._org_client = None
        self._lock = threading.Lock()
        self._session_cache = {}
        
    @property
    def client(self):
        """Get or create Cost Explorer client."""
        if self._client is None:
            with self._lock:
                if self._client is None:
                    if BOTO3_AVAILABLE:
                        kwargs = {"region_name": self.config.region_name}
                        if self.config.aws_access_key_id:
                            kwargs["aws_access_key_id"] = self.config.aws_access_key_id
                        if self.config.aws_secret_access_key:
                            kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key
                        if self.config.aws_session_token:
                            kwargs["aws_session_token"] = self.config.aws_session_token
                        if self.config.profile_name:
                            kwargs["profile_name"] = self.config.profile_name
                        self._client = boto3.client("ce", **kwargs)
                    else:
                        raise ImportError("boto3 is required for Cost Explorer integration")
        return self._client
    
    @property
    def org_client(self):
        """Get or create Organizations client for cross-account queries."""
        if self._org_client is None:
            with self._lock:
                if self._org_client is None:
                    if BOTO3_AVAILABLE:
                        kwargs = {"region_name": self.config.region_name}
                        if self.config.aws_access_key_id:
                            kwargs["aws_access_key_id"] = self.config.aws_access_key_id
                        if self.config.aws_secret_access_key:
                            kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key
                        if self.config.aws_session_token:
                            kwargs["aws_session_token"] = self.config.aws_session_token
                        if self.config.profile_name:
                            kwargs["profile_name"] = self.config.profile_name
                        self._org_client = boto3.client("organizations", **kwargs)
                    else:
                        raise ImportError("boto3 is required for Cost Explorer Organizations integration")
        return self._org_client

    def get_cost_and_usage(
        self,
        query_config: CostQueryConfig
    ) -> Dict[str, Any]:
        """
        Get cost and usage data from AWS Cost Explorer.
        
        Args:
            query_config: Configuration for the cost query including time period,
                         granularity, metrics, and filters.
                         
        Returns:
            Dictionary containing cost and usage data with results grouped
            according to the query configuration.
        """
        try:
            params = {
                "TimePeriod": {
                    "Start": query_config.time_period_start,
                    "End": query_config.time_period_end
                },
                "Granularity": query_config.granularity.value,
                "Metrics": [m.value for m in query_config.metrics]
            }
            
            if query_config.group_by:
                params["GroupBy"] = [
                    {"Type": "DIMENSION", "Key": g} for g in query_config.group_by
                ]
            
            if query_config.filter_dimensions:
                params["Filter"] = self._build_filter_expression(query_config.filter_dimensions)
            
            response = self.client.get_cost_and_usage(**params)
            
            return {
                "success": True,
                "data": response.get("ResultsByTime", []),
                "metadata": {
                    "time_period_start": query_config.time_period_start,
                    "time_period_end": query_config.time_period_end,
                    "granularity": query_config.granularity.value,
                    "metrics": [m.value for m in query_config.metrics]
                }
            }
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get cost and usage: {e}")
            return {
                "success": False,
                "error": str(e),
                "data": []
            }
    
    def get_cost_breakdown_by_service(
        self,
        start_date: str,
        end_date: str,
        granularity: GranularityType = GranularityType.MONTHLY
    ) -> Dict[str, Any]:
        """
        Get cost breakdown by AWS service.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            granularity: Time granularity for the data
            \n
        Returns:
            Dictionary with costs organized by service.
        """
        query_config = CostQueryConfig(
            time_period_start=start_date,
            time_period_end=end_date,
            granularity=granularity,
            metrics=[CostMetricType.BLENDED_COST, CostMetricType.USAGE_QUANTITY],
            group_by=["SERVICE"]
        )
        
        result = self.get_cost_and_usage(query_config)
        
        if result["success"]:
            service_costs = []
            for period in result["data"]:
                services = {}
                for group in period.get("Groups", []):
                    service_name = group["Keys"][0] if group["Keys"] else "Unknown"
                    services[service_name] = {
                        "amount": group["Metrics"].get("BlendedCost", {}).get("Amount", "0"),
                        "unit": group["Metrics"].get("BlendedCost", {}).get("Unit", "USD"),
                        "usage_amount": group["Metrics"].get("UsageQuantity", {}).get("Amount", "0"),
                        "usage_unit": group["Metrics"].get("UsageQuantity", {}).get("Unit", "N/A")
                    }
                service_costs.append({
                    "time_period": period["TimePeriod"],
                    "services": services,
                    "total": period.get("Total", {})
                })
            result["service_costs"] = service_costs
        
        return result
    
    def get_cost_breakdown_by_region(
        self,
        start_date: str,
        end_date: str,
        granularity: GranularityType = GranularityType.MONTHLY
    ) -> Dict[str, Any]:
        """
        Get cost breakdown by AWS region.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            granularity: Time granularity for the data
            \n
        Returns:
            Dictionary with costs organized by region.
        """
        query_config = CostQueryConfig(
            time_period_start=start_date,
            time_period_end=end_date,
            granularity=granularity,
            metrics=[CostMetricType.BLENDED_COST],
            group_by=["REGION"]
        )
        
        result = self.get_cost_and_usage(query_config)
        
        if result["success"]:
            region_costs = []
            for period in result["data"]:
                regions = {}
                for group in period.get("Groups", []):
                    region_name = group["Keys"][0] if group["Keys"] else "Unknown"
                    regions[region_name] = {
                        "amount": group["Metrics"].get("BlendedCost", {}).get("Amount", "0"),
                        "unit": group["Metrics"].get("BlendedCost", {}).get("Unit", "USD")
                    }
                region_costs.append({
                    "time_period": period["TimePeriod"],
                    "regions": regions,
                    "total": period.get("Total", {})
                })
            result["region_costs"] = region_costs
        
        return result
    
    def get_cost_breakdown_by_linked_account(
        self,
        start_date: str,
        end_date: str,
        granularity: GranularityType = GranularityType.MONTHLY,
        account_ids: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Get cost breakdown by linked account.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            granularity: Time granularity for the data
            account_ids: Optional list of account IDs to filter
            \n
        Returns:
            Dictionary with costs organized by linked account.
        """
        filters = {}
        if account_ids:
            filters["LinkedAccount"] = account_ids
        
        query_config = CostQueryConfig(
            time_period_start=start_date,
            time_period_end=end_date,
            granularity=granularity,
            metrics=[CostMetricType.BLENDED_COST, CostMetricType.UNBLENDED_COST],
            group_by=["LINKED_ACCOUNT"],
            filter_dimensions=filters
        )
        
        result = self.get_cost_and_usage(query_config)
        
        if result["success"]:
            account_costs = []
            for period in result["data"]:
                accounts = {}
                for group in period.get("Groups", []):
                    account_id = group["Keys"][0] if group["Keys"] else "Unknown"
                    accounts[account_id] = {
                        "blended_cost": group["Metrics"].get("BlendedCost", {}).get("Amount", "0"),
                        "unblended_cost": group["Metrics"].get("UnblendedCost", {}).get("Amount", "0"),
                        "unit": group["Metrics"].get("BlendedCost", {}).get("Unit", "USD")
                    }
                account_costs.append({
                    "time_period": period["TimePeriod"],
                    "accounts": accounts,
                    "total": period.get("Total", {})
                })
            result["account_costs"] = account_costs
        
        return result
    
    def get_cost_forecast(
        self,
        forecast_config: CostForecastConfig
    ) -> Dict[str, Any]:
        """
        Get cost forecast for future prediction.
        
        Args:
            forecast_config: Configuration for the forecast including time period,
                           metric type, and prediction interval level.
                           \n
        Returns:
            Dictionary containing forecasted costs with prediction intervals.
        """
        try:
            params = {
                "TimePeriod": {
                    "Start": forecast_config.time_period_start,
                    "End": forecast_config.time_period_end
                },
                "Metric": forecast_config.metric.value,
                "PredictionIntervalLevel": forecast_config.prediction_interval_level
            }
            
            response = self.client.get_cost_forecast(**params)
            
            return {
                "success": True,
                "forecast_results": response.get("ForecastResultsByTime", []),
                "metadata": {
                    "metric": forecast_config.metric.value,
                    "prediction_interval_level": forecast_config.prediction_interval_level
                }
            }
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get cost forecast: {e}")
            return {
                "success": False,
                "error": str(e),
                "forecast_results": []
            }
    
    def get_reservation_coverage(
        self,
        coverage_config: ReservationCoverageConfig
    ) -> Dict[str, Any]:
        """
        Get reservation coverage analysis.
        
        Args:
            coverage_config: Configuration for coverage query including time period,
                           granularity, and filters.
                           \n
        Returns:
            Dictionary containing reservation coverage statistics by service,
                           instance type, and time period.
        """
        try:
            params = {
                "TimePeriod": {
                    "Start": coverage_config.time_period_start,
                    "End": coverage_config.time_period_end
                },
                "Granularity": coverage_config.granularity.value
            }
            
            if coverage_config.filter_dimensions:
                params["Filter"] = self._build_filter_expression(coverage_config.filter_dimensions)
            
            response = self.client.get_reservation_coverage(**params)
            
            return {
                "success": True,
                "coverage_by_time": response.get("CoveragesByTime", []),
                "total": response.get("Total", {}),
                "metadata": {
                    "time_period_start": coverage_config.time_period_start,
                    "time_period_end": coverage_config.time_period_end,
                    "granularity": coverage_config.granularity.value
                }
            }
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get reservation coverage: {e}")
            return {
                "success": False,
                "error": str(e),
                "coverage_by_time": [],
                "total": {}
            }
    
    def get_reservation_recommendations(
        self,
        recommendation_config: ReservationRecommendationConfig
    ) -> Dict[str, Any]:
        """
        Get reservation purchase recommendations.
        
        Args:
            recommendation_config: Configuration for recommendations including
                                 service type and optional filters.
                                 \n
        Returns:
            Dictionary containing RI purchase recommendations with expected
            savings and utilization metrics.
        """
        try:
            params = {
                "Service": recommendation_config.service
            }
            
            if recommendation_config.account_id:
                params["AccountId"] = recommendation_config.account_id
            
            if recommendation_config.filter_dimensions:
                params["Filter"] = self._build_filter_expression(recommendation_config.filter_dimensions)
            
            response = self.client.get_reservation_recommendations(**params)
            
            return {
                "success": True,
                "recommendations": response.get("ReservationRecommendations", []),
                "metadata": {
                    "service": recommendation_config.service,
                    "account_id": recommendation_config.account_id
                }
            }
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get reservation recommendations: {e}")
            return {
                "success": False,
                "error": str(e),
                "recommendations": []
            }
    
    def get_savings_plans_recommendations(
        self,
        sp_config: SavingsPlansRecommendationConfig
    ) -> Dict[str, Any]:
        """
        Get Savings Plans recommendations.
        
        Args:
            sp_config: Configuration for Savings Plans recommendations including
                      Savings Plans type, term, and payment option.
                      \n
        Returns:
            Dictionary containing Savings Plans recommendations with estimated
            savings and coverage metrics.
        """
        try:
            params = {
                "SavingsPlansType": sp_config.SavingsPlansType,
                "Term": sp_config.term,
                "PaymentOption": sp_config.payment_option
            }
            
            if sp_config.account_id:
                params["AccountId"] = sp_config.account_id
            
            response = self.client.get_savings_plans_recommendation(**params)
            
            return {
                "success": True,
                "recommendations": response.get("SavingsPlansRecommendations", []),
                "metadata": {
                    "savings_plans_type": sp_config.SavingsPlansType,
                    "term": sp_config.term,
                    "payment_option": sp_config.payment_option,
                    "account_id": sp_config.account_id
                }
            }
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get Savings Plans recommendations: {e}")
            return {
                "success": False,
                "error": str(e),
                "recommendations": []
            }
    
    def get_cost_anomaly_detection(
        self,
        start_date: str,
        end_date: str
    ) -> Dict[str, Any]:
        """
        Get cost anomaly detection results.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            \n
        Returns:
            Dictionary containing detected cost anomalies with impact metrics.
        """
        try:
            params = {
                "StartDate": start_date,
                "EndDate": end_date,
                "MaxResults": 100
            }
            
            response = self.client.get_cost_anomalies(**params)
            
            anomalies = response.get("Anomalies", [])
            
            formatted_anomalies = []
            for anomaly in anomalies:
                formatted_anomalies.append({
                    "anomaly_id": anomaly.get("AnomalyId"),
                    "service": anomaly.get("Service"),
                    "region": anomaly.get("Region"),
                    "cost_type": anomaly.get("CostType"),
                    "actual_spend": anomaly.get("ActualSpend", {}),
                    "expected_spend": anomaly.get("ExpectedSpend", {}),
                    "impact": anomaly.get("Impact", {}),
                    "anomaly_period": anomaly.get("AnomalyPeriod", {}),
                    "monitoring_account_id": anomaly.get("MonitoringAccountId")
                })
            
            return {
                "success": True,
                "anomalies": formatted_anomalies,
                "total_count": len(formatted_anomalies),
                "metadata": {
                    "start_date": start_date,
                    "end_date": end_date
                }
            }
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get cost anomalies: {e}")
            return {
                "success": False,
                "error": str(e),
                "anomalies": []
            }
    
    def create_anomaly_subscription(
        self,
        subscription_config: AnomalyDetectionConfig,
        subscriber_email: str
    ) -> Dict[str, Any]:
        """
        Create a cost anomaly detection subscription.
        
        Args:
            subscription_config: Configuration for the anomaly detection subscription
            subscriber_email: Email address to receive anomaly alerts
            \n
        Returns:
            Dictionary containing the created subscription details.
        """
        try:
            params = {
                "AnomalySubscription": {
                    "SubscriptionName": f"cost-anomaly-subscription-{uuid.uuid4().hex[:8]}",
                    "Threshold": subscription_config.threshold,
                    "Frequency": subscription_config.frequency,
                    "MonitorArnList": [],
                    "Subscribers": [
                        {
                            "Address": subscriber_email,
                            "Type": "EMAIL"
                        }
                    ]
                }
            }
            
            if subscription_config.mode == AnomalyDetectionMode.ENTIRE_ORGANIZATION:
                params["AnomalySubscription"]["AccountScope"] = "PAYER"
            elif subscription_config.mode == AnomalyDetectionMode.SPECIFIC_ACCOUNTS:
                params["AnomalySubscription"]["AccountScope"] = {
                    "AccountScope": subscription_config.account_ids
                }
            
            if subscription_config.service_whitelist:
                params["AnomalySubscription"]["ServiceResourceScope"] = {
                    "ServiceType": subscription_config.service_whitelist
                }
            
            if subscription_config.service_blacklist:
                params["AnomalySubscription"]["ResourceScope"] = {
                    "ServiceExcludeList": subscription_config.service_blacklist
                }
            
            response = self.client.create_anomaly_subscription(**params)
            
            return {
                "success": True,
                "subscription": response.get("AnomalySubscription", {}),
                "subscription_arn": response.get("SubscriptionArn")
            }
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to create anomaly subscription: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def get_tag_values(
        self,
        tag_key: str,
        start_date: str,
        end_date: str
    ) -> Dict[str, Any]:
        """
        Get tag values for cost allocation tracking.
        
        Args:
            tag_key: The tag key to retrieve values for
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            \n
        Returns:
            Dictionary containing tag values with associated costs.
        """
        try:
            params = {
                "TimePeriod": {
                    "Start": start_date,
                    "End": end_date
                },
                "TagKey": tag_key
            }
            
            response = self.client.get_tags(**params)
            
            return {
                "success": True,
                "tag_key": tag_key,
                "tags": response.get("Tags", []),
                "metadata": {
                    "start_date": start_date,
                    "end_date": end_date
                }
            }
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get tag values: {e}")
            return {
                "success": False,
                "error": str(e),
                "tags": []
            }
    
    def get_cost_by_tag(
        self,
        tag_key: str,
        start_date: str,
        end_date: str,
        granularity: GranularityType = GranularityType.MONTHLY
    ) -> Dict[str, Any]:
        """
        Get cost breakdown by tag for cost allocation.
        
        Args:
            tag_key: The tag key to group costs by
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            granularity: Time granularity for the data
            \n
        Returns:
            Dictionary with costs organized by tag values.
        """
        query_config = CostQueryConfig(
            time_period_start=start_date,
            time_period_end=end_date,
            granularity=granularity,
            metrics=[CostMetricType.BLENDED_COST],
            group_by=[tag_key]
        )
        
        result = self.get_cost_and_usage(query_config)
        
        if result["success"]:
            tag_costs = []
            for period in result["data"]:
                tags = {}
                for group in period.get("Groups", []):
                    tag_value = group["Keys"][0] if group["Keys"] else "Untagged"
                    tags[tag_value] = {
                        "amount": group["Metrics"].get("BlendedCost", {}).get("Amount", "0"),
                        "unit": group["Metrics"].get("BlendedCost", {}).get("Unit", "USD")
                    }
                tag_costs.append({
                    "time_period": period["TimePeriod"],
                    "tags": tags,
                    "total": period.get("Total", {})
                })
            result["tag_costs"] = tag_costs
        
        return result
    
    def create_cost_category(
        self,
        category_config: CostCategoryConfig
    ) -> Dict[str, Any]:
        """
        Create a cost category for organizing costs.
        
        Args:
            category_config: Configuration for the cost category including
                           name, rules, and rule version.
                           \n
        Returns:
            Dictionary containing the created cost category details.
        """
        try:
            params = {
                "Name": category_config.cost_category_name,
                "RuleVersion": category_config.rule_version,
                "Rules": category_config.rules
            }
            
            response = self.client.create_cost_category(**params)
            
            return {
                "success": True,
                "cost_category": response.get("CostCategory", {}),
                "cost_category_arn": response.get("CostCategoryArn")
            }
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to create cost category: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def get_cost_category(
        self,
        cost_category_name: str
    ) -> Dict[str, Any]:
        """
        Get details of a cost category.
        
        Args:
            cost_category_name: Name of the cost category to retrieve
            \n
        Returns:
            Dictionary containing cost category details and rules.
        """
        try:
            params = {
                "Name": cost_category_name
            }
            
            response = self.client.describe_cost_categories(**params)
            
            categories = response.get("CostCategoryNames", [])
            
            return {
                "success": True,
                "cost_categories": categories,
                "metadata": {
                    "cost_category_name": cost_category_name
                }
            }
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get cost category: {e}")
            return {
                "success": False,
                "error": str(e),
                "cost_categories": []
            }
    
    def get_cost_by_cost_category(
        self,
        cost_category_name: str,
        start_date: str,
        end_date: str,
        granularity: GranularityType = GranularityType.MONTHLY
    ) -> Dict[str, Any]:
        """
        Get costs organized by cost category.
        
        Args:
            cost_category_name: Name of the cost category to group by
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            granularity: Time granularity for the data
            \n
        Returns:
            Dictionary with costs organized by cost category values.
        """
        query_config = CostQueryConfig(
            time_period_start=start_date,
            time_period_end=end_date,
            granularity=granularity,
            metrics=[CostMetricType.BLENDED_COST, CostMetricType.AMORTIZED_COST],
            group_by=["COST_CATEGORY"],
            filter_dimensions={"CostCategory": cost_category_name}
        )
        
        result = self.get_cost_and_usage(query_config)
        
        if result["success"]:
            category_costs = []
            for period in result["data"]:
                categories = {}
                for group in period.get("Groups", []):
                    category_value = group["Keys"][0] if group["Keys"] else "Uncategorized"
                    categories[category_value] = {
                        "blended_cost": group["Metrics"].get("BlendedCost", {}).get("Amount", "0"),
                        "amortized_cost": group["Metrics"].get("AmortizedCost", {}).get("Amount", "0"),
                        "unit": group["Metrics"].get("BlendedCost", {}).get("Unit", "USD")
                    }
                category_costs.append({
                    "time_period": period["TimePeriod"],
                    "categories": categories,
                    "total": period.get("Total", {})
                })
            result["category_costs"] = category_costs
        
        return result
    
    def generate_visualization_data(
        self,
        start_date: str,
        end_date: str,
        group_by维度: List[str] = None
    ) -> Dict[str, Any]:
        """
        Generate cost visualization data for dashboards.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            group_by维度: List of dimensions to group by for visualization
            \n
        Returns:
            Dictionary containing formatted data suitable for cost
            visualization including time series data and summary metrics.
        """
        if group_by维度 is None:
            group_by维度 = ["SERVICE"]
        
        query_config = CostQueryConfig(
            time_period_start=start_date,
            time_period_end=end_date,
            granularity=GranularityType.DAILY,
            metrics=[CostMetricType.BLENDED_COST, CostMetricType.USAGE_QUANTITY],
            group_by=group_by维度
        )
        
        result = self.get_cost_and_usage(query_config)
        
        if result["success"]:
            visualization = {
                "time_series": [],
                "summary": {
                    "total_cost": 0.0,
                    "total_usage": 0.0,
                    "period_count": 0,
                    "service_count": 0
                },
                "top_services": [],
                "daily_trends": []
            }
            
            all_services = set()
            daily_totals = defaultdict(float)
            
            for period in result["data"]:
                time_period = period["TimePeriod"]["Start"]
                
                for group in period.get("Groups", []):
                    service = group["Keys"][0] if group["Keys"] else "Unknown"
                    all_services.add(service)
                    
                    cost_amount = float(group["Metrics"].get("BlendedCost", {}).get("Amount", "0"))
                    usage_amount = float(group["Metrics"].get("UsageQuantity", {}).get("Amount", "0"))
                    
                    visualization["summary"]["total_cost"] += cost_amount
                    visualization["summary"]["total_usage"] += usage_amount
                    daily_totals[time_period] += cost_amount
                    
                    visualization["time_series"].append({
                        "timestamp": time_period,
                        "service": service,
                        "cost": cost_amount,
                        "usage": usage_amount
                    })
            
            service_costs = defaultdict(float)
            for period in result["data"]:
                for group in period.get("Groups", []):
                    service = group["Keys"][0] if group["Keys"] else "Unknown"
                    cost_amount = float(group["Metrics"].get("BlendedCost", {}).get("Amount", "0"))
                    service_costs[service] += cost_amount
            
            visualization["top_services"] = sorted(
                [{"service": s, "cost": c} for s, c in service_costs.items()],
                key=lambda x: x["cost"],
                reverse=True
            )[:10]
            
            visualization["daily_trends"] = [
                {"date": date, "cost": cost}
                for date, cost in sorted(daily_totals.items())
            ]
            
            visualization["summary"]["period_count"] = len(daily_totals)
            visualization["summary"]["service_count"] = len(all_services)
            visualization["metadata"] = {
                "start_date": start_date,
                "end_date": end_date,
                "group_by": group_by维度
            }
            
            result["visualization"] = visualization
        
        return result
    
    def get_organization_cost_data(
        self,
        start_date: str,
        end_date: str,
        granularity: GranularityType = GranularityType.MONTHLY
    ) -> Dict[str, Any]:
        """
        Get cost data across an AWS Organization.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            granularity: Time granularity for the data
            \n
        Returns:
            Dictionary containing cost data organized by account with
            organizational totals and breakdowns.
        """
        try:
            org_accounts = []
            try:
                paginator = self.org_client.get_paginator("list_accounts")
                for page in paginator.paginate():
                    for account in page.get("Accounts", []):
                        org_accounts.append({
                            "account_id": account["Id"],
                            "account_name": account["Name"],
                            "status": account["Status"]
                        })
            except (ClientError, BotoCoreError) as e:
                logger.warning(f"Could not fetch organization accounts: {e}")
            
            query_config = CostQueryConfig(
                time_period_start=start_date,
                time_period_end=end_date,
                granularity=granularity,
                metrics=[CostMetricType.BLENDED_COST, CostMetricType.UNBLENDED_COST],
                group_by=["LINKED_ACCOUNT"]
            )
            
            account_result = self.get_cost_breakdown_by_linked_account(
                start_date, end_date, granularity
            )
            
            return {
                "success": True,
                "organization_accounts": org_accounts,
                "cost_by_account": account_result.get("account_costs", []),
                "total_accounts": len(org_accounts),
                "metadata": {
                    "start_date": start_date,
                    "end_date": end_date,
                    "granularity": granularity.value
                }
            }
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get organization cost data: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def _build_filter_expression(
        self,
        filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Build Cost Explorer filter expression from simple dictionary.
        
        Args:
            filters: Dictionary of filter dimensions
            \n
        Returns:
            Cost Explorer filter expression format.
        """
        if not filters:
            return {}
        
        and_conditions = []
        for key, value in filters.items():
            if isinstance(value, list):
                or_conditions = [{"Dimensions": {"Key": key, "Values": [v]}} for v in value]
                and_conditions.append({"Or": or_conditions})
            else:
                and_conditions.append({"Dimensions": {"Key": key, "Values": [value]}})
        
        if len(and_conditions) == 1:
            return and_conditions[0]
        else:
            return {"And": and_conditions}
    
    def close(self):
        """Close the Cost Explorer clients."""
        with self._lock:
            self._client = None
            self._org_client = None
            self._session_cache.clear()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False
