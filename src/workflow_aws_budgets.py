"""
AWS Budgets Integration

Comprehensive AWS Budgets management including:
- Budget management (create/manage)
- Budget actions (create/manage)
- Notifications configuration
- Support for cost, usage, RI, Savings Plans budgets
- Threshold alerts
- Email and SNS subscribers
- Cost allocation tags and budget reports
- Auto-adjusting budgets
- Budget forecasting
- CloudWatch integration (metrics and alarms)
"""

import boto3
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class BudgetType(Enum):
    """Budget type enumeration"""
    COST = "COST"
    USAGE = "USAGE"
    RI_UTILIZATION = "RI_UTILIZATION"
    RI_COVERAGE = "RI_COVERAGE"
    SAVINGS_PLANS_UTILIZATION = "SAVINGS_PLANS_UTILIZATION"
    SAVINGS_PLANS_COVERAGE = "SAVINGS_PLANS_COVERAGE"


class TimeUnit(Enum):
    """Budget time unit enumeration"""
    DAILY = "DAILY"
    WEEKLY = "WEEKLY"
    MONTHLY = "MONTHLY"
    QUARTERLY = "QUARTERLY"
    ANNUALLY = "ANNUALLY"


class NotificationType(Enum):
    """Notification type enumeration"""
    ACTUAL = "ACTUAL"
    FORECASTED = "FORECASTED"
    THRESHOLD_BREACH = "THRESHOLD_BREACH"


@dataclass
class Subscriber:
    """Budget subscriber configuration"""
    address: str
    subscription_type: str  # EMAIL, SNS
    
    def to_dict(self) -> Dict[str, str]:
        return {
            "address": self.address,
            "subscriptionType": self.subscription_type
        }


@dataclass
class Notification:
    """Budget notification configuration"""
    threshold: float
    notification_type: NotificationType
    comparison_operator: str
    subscribers: List[Subscriber] = field(default_factory=list)
    threshold_type: str = "PERCENTAGE"
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "threshold": self.threshold,
            "notificationType": self.notification_type.value,
            "comparisonOperator": self.comparison_operator,
            "thresholdType": self.threshold_type,
            "subscribers": [s.to_dict() for s in self.subscribers]
        }


@dataclass
class BudgetAction:
    """Budget action configuration"""
    action_threshold: float
    definition: Dict[str, Any]
    execution_role_arn: str
    action_type: str
    notification_type: NotificationType
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "actionThreshold": {
                "actionThresholdValue": self.action_threshold,
                "actionThresholdType": "PERCENTAGE"
            },
            "definition": self.definition,
            "executionRoleArn": self.execution_role_arn,
            "actionType": self.action_type,
            "notificationType": self.notification_type.value
        }


@dataclass
class AutoAdjustConfig:
    """Auto-adjusting budget configuration"""
    auto_adjust_type: str  # FORECAST, HISTORICAL
    lookback_period_days: int = 12
    historical_options: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        config = {
            "autoAdjustType": self.auto_adjust_type,
            "historicalOptions": {
                "lookbackConfiguration": {
                    "lookbackPeriodDays": self.lookback_period_days
                }
            }
        }
        return config


@dataclass
class CostFilter:
    """Cost allocation filter"""
    name: str
    values: List[str]
    
    def to_dict(self) -> Dict[str, List[str]]:
        return {self.name: self.values}


class BudgetsIntegration:
    """
    AWS Budgets Integration Class
    
    Provides comprehensive management of AWS Budgets including:
    - Budget CRUD operations
    - Budget actions for automated responses
    - Notifications and subscribers
    - Support for multiple budget types (cost, usage, RI, Savings Plans)
    - Auto-adjusting budgets
    - Budget forecasting
    - CloudWatch integration
    """
    
    def __init__(self, profile_name: Optional[str] = None, region: str = "us-east-1"):
        """
        Initialize the Budgets Integration
        
        Args:
            profile_name: AWS profile name for boto3 session
            region: AWS region (default: us-east-1)
        """
        session = boto3.Session(profile_name=profile_name)
        self.budgets_client = session.client("budgets", region_name=region)
        self.cloudwatch_client = session.client("cloudwatch", region_name=region)
        self.sns_client = session.client("sns", region_name=region)
        self.iam_client = session.client("iam", region_name=region)
        self.region = region
        
    # =========================================================================
    # Budget Management
    # =========================================================================
    
    def create_budget(
        self,
        budget_name: str,
        budget_limit: float,
        time_unit: TimeUnit,
        budget_type: BudgetType,
        cost_filters: Optional[List[CostFilter]] = None,
        cost_types: Optional[Dict[str, bool]] = None,
        auto_adjust_config: Optional[AutoAdjustConfig] = None,
        notifications: Optional[List[Notification]] = None
    ) -> Dict[str, Any]:
        """
        Create a new budget
        
        Args:
            budget_name: Name of the budget
            budget_limit: Spending limit amount
            time_unit: Time unit (DAILY, WEEKLY, MONTHLY, etc.)
            budget_type: Type of budget (COST, USAGE, RI_*, SAVINGS_PLANS_*)
            cost_filters: Optional list of cost filters
            cost_types: Optional cost types configuration
            auto_adjust_config: Optional auto-adjusting configuration
            notifications: Optional list of notifications
            
        Returns:
            Dict containing the created budget details
        """
        budget = {
            "BudgetName": budget_name,
            "BudgetLimit": {
                "Amount": str(budget_limit),
                "Unit": "USD"
            },
            "TimeUnit": time_unit.value,
            "BudgetType": budget_type.value
        }
        
        if cost_filters:
            budget["CostFilters"] = {
                f.name: f.values for f in cost_filters
            }
            
        if cost_types:
            budget["CostTypes"] = cost_types
            
        if auto_adjust_config:
            budget["AutoAdjustData"] = auto_adjust_config.to_dict()
            
        params = {"Budget": budget}
        
        if notifications:
            params["NotificationsWithSubscribers"] = [
                {"Notification": n.to_dict(), "Subscribers": [s.to_dict() for s in n.subscribers]}
                for n in notifications
            ]
            
        response = self.budgets_client.create_budget(**params)
        logger.info(f"Created budget: {budget_name}")
        return response
    
    def get_budget(self, budget_name: str) -> Dict[str, Any]:
        """Get details of a specific budget"""
        return self.budgets_client.describe_budget(BudgetName=budget_name)
    
    def list_budgets(self, max_results: int = 100) -> List[Dict[str, Any]]:
        """List all budgets"""
        budgets = []
        paginator = self.budgets_client.get_paginator("describe_budgets")
        
        for page in paginator.paginate(MaxResults=max_results):
            budgets.extend(page.get("Budgets", []))
            
        return budgets
    
    def update_budget(
        self,
        budget_name: str,
        budget_limit: Optional[float] = None,
        cost_filters: Optional[List[CostFilter]] = None,
        cost_types: Optional[Dict[str, bool]] = None,
        auto_adjust_config: Optional[AutoAdjustConfig] = None
    ) -> Dict[str, Any]:
        """Update an existing budget"""
        new_budget = {}
        
        if budget_limit is not None:
            new_budget["BudgetLimit"] = {
                "Amount": str(budget_limit),
                "Unit": "USD"
            }
            
        if cost_filters:
            new_budget["CostFilters"] = {
                f.name: f.values for f in cost_filters
            }
            
        if cost_types:
            new_budget["CostTypes"] = cost_types
            
        if auto_adjust_config:
            new_budget["AutoAdjustData"] = auto_adjust_config.to_dict()
            
        response = self.budgets_client.update_budget(
            BudgetName=budget_name,
            NewBudget=new_budget
        )
        logger.info(f"Updated budget: {budget_name}")
        return response
    
    def delete_budget(self, budget_name: str) -> Dict[str, Any]:
        """Delete a budget"""
        response = self.budgets_client.delete_budget(BudgetName=budget_name)
        logger.info(f"Deleted budget: {budget_name}")
        return response
    
    # =========================================================================
    # Budget Actions
    # =========================================================================
    
    def create_budget_action(
        self,
        budget_name: str,
        action_threshold: float,
        action_type: str,
        notification_type: NotificationType,
        subscribers: List[Subscriber],
        definition: Optional[Dict[str, Any]] = None,
        execution_role_arn: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a budget action
        
        Args:
            budget_name: Name of the budget
            action_threshold: Threshold for action trigger
            action_type: Type of action (APPLY_IAM_POLICY, RUN_SSM_DOCUMENTS, CREATE_IAM_POLICY)
            notification_type: Type of notification
            subscribers: List of subscribers to notify
            definition: Action definition details
            execution_role_arn: IAM role ARN for action execution
        """
        if definition is None:
            definition = {
                "IamActionDefinition": {
                    "PolicyId": "root",
                    "PolicyName": "BudgetLimitPolicy",
                    "Roles": []
                }
            }
            
        if execution_role_arn is None:
            execution_role_arn = self._get_default_execution_role()
            
        params = {
            "BudgetName": budget_name,
            "Notification": {
                "NotificationType": notification_type.value,
                "ComparisonOperator": "GREATER_THAN",
                "Threshold": action_threshold,
                "ThresholdType": "PERCENTAGE"
            },
            "ActionThreshold": {
                "ActionThresholdValue": action_threshold,
                "ActionThresholdType": "PERCENTAGE"
            },
            "ActionType": action_type,
            "Definition": definition,
            "ExecutionRoleArn": execution_role_arn,
            "Subscribers": [s.to_dict() for s in subscribers]
        }
        
        response = self.budgets_client.create_budget_action(**params)
        logger.info(f"Created budget action for: {budget_name}")
        return response
    
    def describe_budget_actions(self, budget_name: str) -> List[Dict[str, Any]]:
        """Describe all actions for a budget"""
        response = self.budgets_client.describe_budget_actions(BudgetName=budget_name)
        return response.get("Actions", [])
    
    def update_budget_action(
        self,
        budget_name: str,
        action_id: str,
        new_definition: Optional[Dict[str, Any]] = None,
        new_subscribers: Optional[List[Subscriber]] = None
    ) -> Dict[str, Any]:
        """Update an existing budget action"""
        params = {
            "BudgetName": budget_name,
            "ActionId": action_id
        }
        
        if new_definition:
            params["NewDefinition"] = new_definition
            
        if new_subscribers:
            params["NewSubscribers"] = [s.to_dict() for s in new_subscribers]
            
        response = self.budgets_client.update_budget_action(**params)
        logger.info(f"Updated budget action: {action_id}")
        return response
    
    def delete_budget_action(self, budget_name: str, action_id: str) -> Dict[str, Any]:
        """Delete a budget action"""
        response = self.budgets_client.delete_budget_action(
            BudgetName=budget_name,
            ActionId=action_id
        )
        logger.info(f"Deleted budget action: {action_id}")
        return response
    
    def _get_default_execution_role(self) -> str:
        """Get or create default execution role for budget actions"""
        role_name = "AWSBudgetsExecutionRole"
        try:
            role = self.iam_client.get_role(RoleName=role_name)
            return role["Role"]["Arn"]
        except self.iam_client.exceptions.NoSuchEntityException:
            trust_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "budgets.amazonaws.com"},
                        "Action": "sts:AssumeRole"
                    }
                ]
            }
            self.iam_client.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=str(trust_policy),
                Description="Execution role for AWS Budgets actions"
            )
            return f"arn:aws:iam::*:role/{role_name}"
    
    # =========================================================================
    # Notifications & Subscribers
    # =========================================================================
    
    def create_notification_with_subscribers(
        self,
        budget_name: str,
        notification: Notification
    ) -> Dict[str, Any]:
        """Create a notification with subscribers for an existing budget"""
        params = {
            "BudgetName": budget_name,
            "Notification": notification.to_dict(),
            "Subscribers": [s.to_dict() for s in notification.subscribers]
        }
        
        response = self.budgets_client.create_notification(**params)
        logger.info(f"Created notification for budget: {budget_name}")
        return response
    
    def add_subscriber(
        self,
        budget_name: str,
        notification: Notification,
        subscriber: Subscriber
    ) -> Dict[str, Any]:
        """Add a subscriber to an existing notification"""
        response = self.budgets_client.create_subscriber(
            BudgetName=budget_name,
            Notification=notification.to_dict(),
            Subscriber=subscriber.to_dict()
        )
        logger.info(f"Added subscriber {subscriber.address} to budget: {budget_name}")
        return response
    
    def remove_subscriber(
        self,
        budget_name: str,
        notification: Notification,
        subscriber: Subscriber
    ) -> Dict[str, Any]:
        """Remove a subscriber from a notification"""
        response = self.budgets_client.delete_subscriber(
            BudgetName=budget_name,
            Notification=notification.to_dict(),
            Subscriber=subscriber.to_dict()
        )
        logger.info(f"Removed subscriber from budget: {budget_name}")
        return response
    
    def update_notification(
        self,
        budget_name: str,
        old_notification: Notification,
        new_notification: Notification
    ) -> Dict[str, Any]:
        """Update an existing notification"""
        response = self.budgets_client.update_notification(
            BudgetName=budget_name,
            OldNotification=old_notification.to_dict(),
            NewNotification=new_notification.to_dict()
        )
        logger.info(f"Updated notification for budget: {budget_name}")
        return response
    
    # =========================================================================
    # Cost Budgets
    # =========================================================================
    
    def create_cost_budget(
        self,
        budget_name: str,
        budget_limit: float,
        time_unit: TimeUnit = TimeUnit.MONTHLY,
        cost_filters: Optional[List[CostFilter]] = None,
        notifications: Optional[List[Notification]] = None
    ) -> Dict[str, Any]:
        """Create a cost budget"""
        return self.create_budget(
            budget_name=budget_name,
            budget_limit=budget_limit,
            time_unit=time_unit,
            budget_type=BudgetType.COST,
            cost_filters=cost_filters,
            notifications=notifications
        )
    
    def create_linked_account_cost_budget(
        self,
        budget_name: str,
        budget_limit: float,
        account_id: str,
        time_unit: TimeUnit = TimeUnit.MONTHLY
    ) -> Dict[str, Any]:
        """Create a cost budget for a linked account"""
        return self.create_cost_budget(
            budget_name=budget_name,
            budget_limit=budget_limit,
            time_unit=time_unit,
            cost_filters=[CostFilter(name="LinkedAccount", values=[account_id])]
        )
    
    # =========================================================================
    # Usage Budgets
    # =========================================================================
    
    def create_usage_budget(
        self,
        budget_name: str,
        budget_limit: float,
        usage_unit: str,
        time_unit: TimeUnit = TimeUnit.MONTHLY,
        service: Optional[str] = None,
        operation: Optional[str] = None,
        notifications: Optional[List[Notification]] = None
    ) -> Dict[str, Any]:
        """Create a usage budget"""
        cost_filters = []
        if service:
            cost_filters.append(CostFilter(name="Service", values=[service]))
        if operation:
            cost_filters.append(CostFilter(name="Operation", values=[operation]))
            
        return self.create_budget(
            budget_name=budget_name,
            budget_limit=budget_limit,
            time_unit=time_unit,
            budget_type=BudgetType.USAGE,
            cost_filters=cost_filters if cost_filters else None,
            notifications=notifications
        )
    
    # =========================================================================
    # RI Budgets
    # =========================================================================
    
    def create_ri_utilization_budget(
        self,
        budget_name: str,
        threshold: float,
        service: Optional[str] = None,
        notifications: Optional[List[Notification]] = None
    ) -> Dict[str, Any]:
        """Create an RI utilization budget"""
        cost_filters = []
        if service:
            cost_filters.append(CostFilter(name="Service", values=[service]))
            
        return self.create_budget(
            budget_name=budget_name,
            budget_limit=100.0,  # RI budgets are always percentage
            time_unit=TimeUnit.MONTHLY,
            budget_type=BudgetType.RI_UTILIZATION,
            cost_filters=cost_filters if cost_filters else None,
            notifications=notifications
        )
    
    def create_ri_coverage_budget(
        self,
        budget_name: str,
        threshold: float,
        service: Optional[str] = None,
        notifications: Optional[List[Notification]] = None
    ) -> Dict[str, Any]:
        """Create an RI coverage budget"""
        cost_filters = []
        if service:
            cost_filters.append(CostFilter(name="Service", values=[service]))
            
        return self.create_budget(
            budget_name=budget_name,
            budget_limit=100.0,
            time_unit=TimeUnit.MONTHLY,
            budget_type=BudgetType.RI_COVERAGE,
            cost_filters=cost_filters if cost_filters else None,
            notifications=notifications
        )
    
    # =========================================================================
    # Savings Plans Budgets
    # =========================================================================
    
    def create_savings_plans_utilization_budget(
        self,
        budget_name: str,
        threshold: float,
        service: Optional[str] = None,
        linked_account: Optional[str] = None,
        notifications: Optional[List[Notification]] = None
    ) -> Dict[str, Any]:
        """Create a Savings Plans utilization budget"""
        cost_filters = []
        if service:
            cost_filters.append(CostFilter(name="Service", values=[service]))
        if linked_account:
            cost_filters.append(CostFilter(name="LinkedAccount", values=[linked_account]))
            
        return self.create_budget(
            budget_name=budget_name,
            budget_limit=100.0,
            time_unit=TimeUnit.MONTHLY,
            budget_type=BudgetType.SAVINGS_PLANS_UTILIZATION,
            cost_filters=cost_filters if cost_filters else None,
            notifications=notifications
        )
    
    def create_savings_plans_coverage_budget(
        self,
        budget_name: str,
        threshold: float,
        service: Optional[str] = None,
        linked_account: Optional[str] = None,
        notifications: Optional[List[Notification]] = None
    ) -> Dict[str, Any]:
        """Create a Savings Plans coverage budget"""
        cost_filters = []
        if service:
            cost_filters.append(CostFilter(name="Service", values=[service]))
        if linked_account:
            cost_filters.append(CostFilter(name="LinkedAccount", values=[linked_account]))
            
        return self.create_budget(
            budget_name=budget_name,
            budget_limit=100.0,
            time_unit=TimeUnit.MONTHLY,
            budget_type=BudgetType.SAVINGS_PLANS_COVERAGE,
            cost_filters=cost_filters if cost_filters else None,
            notifications=notifications
        )
    
    # =========================================================================
    # Threshold Alerts
    # =========================================================================
    
    def create_threshold_alert(
        self,
        budget_name: str,
        threshold: float,
        notification_type: NotificationType = NotificationType.ACTUAL,
        subscribers: Optional[List[Subscriber]] = None
    ) -> Notification:
        """Create a threshold alert notification"""
        if subscribers is None:
            subscribers = []
            
        notification = Notification(
            threshold=threshold,
            notification_type=notification_type,
            comparison_operator="GREATER_THAN",
            subscribers=subscribers
        )
        
        self.create_notification_with_subscribers(budget_name, notification)
        return notification
    
    def create_escalation_alert(
        self,
        budget_name: str,
        thresholds: List[float],
        notification_type: NotificationType = NotificationType.ACTUAL
    ) -> List[Notification]:
        """Create escalation alerts at multiple thresholds"""
        notifications = []
        operators = ["GREATER_THAN", "GREATER_THAN", "GREATER_THAN", "LESS_THAN"]
        
        for i, threshold in enumerate(thresholds):
            notification = Notification(
                threshold=threshold,
                notification_type=notification_type,
                comparison_operator=operators[i % len(operators)],
                subscribers=[]
            )
            self.create_notification_with_subscribers(budget_name, notification)
            notifications.append(notification)
            
        return notifications
    
    # =========================================================================
    # Email Subscribers
    # =========================================================================
    
    def create_email_subscriber(self, address: str) -> Subscriber:
        """Create an email subscriber"""
        return Subscriber(
            address=address,
            subscription_type="EMAIL"
        )
    
    def setup_email_alert(
        self,
        budget_name: str,
        email_addresses: List[str],
        threshold: float,
        notification_type: NotificationType = NotificationType.ACTUAL
    ) -> Notification:
        """Setup email alerts for a budget"""
        subscribers = [self.create_email_subscriber(email) for email in email_addresses]
        
        notification = Notification(
            threshold=threshold,
            notification_type=notification_type,
            comparison_operator="GREATER_THAN",
            subscribers=subscribers
        )
        
        self.create_notification_with_subscribers(budget_name, notification)
        return notification
    
    # =========================================================================
    # SNS Subscribers
    # =========================================================================
    
    def create_sns_subscriber(self, topic_arn: str) -> Subscriber:
        """Create an SNS subscriber"""
        return Subscriber(
            address=topic_arn,
            subscription_type="SNS"
        )
    
    def setup_sns_alert(
        self,
        budget_name: str,
        sns_topic_arn: str,
        threshold: float,
        notification_type: NotificationType = NotificationType.ACTUAL
    ) -> Notification:
        """Setup SNS alerts for a budget"""
        subscriber = self.create_sns_subscriber(sns_topic_arn)
        
        notification = Notification(
            threshold=threshold,
            notification_type=notification_type,
            comparison_operator="GREATER_THAN",
            subscribers=[subscriber]
        )
        
        self.create_notification_with_subscribers(budget_name, notification)
        return notification
    
    def create_sns_topic(self, name: str, tags: Optional[List[Dict[str, str]]] = None) -> str:
        """Create an SNS topic for budget alerts"""
        response = self.sns_client.create_topic(Name=name)
        topic_arn = response["TopicArn"]
        
        if tags:
            self.sns_client.tag_resource(ResourceArn=topic_arn, Tags=tags)
            
        logger.info(f"Created SNS topic: {name}")
        return topic_arn
    
    # =========================================================================
    # Reports
    # =========================================================================
    
    def get_budget_report(self, budget_name: str) -> Dict[str, Any]:
        """Get detailed report for a budget"""
        budget = self.get_budget(budget_name)
        
        try:
            spend = self.budgets_client.describe_budget_spend(BudgetName=budget_name)
        except Exception:
            spend = {}
            
        try:
            history = self.budgets_client.describe_budget_history(
                BudgetName=budget_name,
                TimePeriod={
                    "Start": (datetime.now() - timedelta(days=30)).isoformat(),
                    "End": datetime.now().isoformat()
                }
            )
        except Exception:
            history = {}
            
        return {
            "budget": budget,
            "spend": spend,
            "history": history
        }
    
    def get_cost allocation_tags_report(self) -> List[Dict[str, Any]]:
        """Get cost allocation tags report"""
        ce_client = boto3.Session().client("ce", region_name=self.region)
        
        response = ce_client.get_tags(
            TimePeriod={
                "Start": (datetime.now() - timedelta(days=30)).isoformat(),
                "End": datetime.now().isoformat()
            },
            Granularity="MONTHLY",
            GroupBy=[
                {"Key": "TAG", "Type": "COST_ALLOCATION"}
            ]
        )
        
        return response.get("Tags", [])
    
    def get_cost_forecast(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """Get cost forecast for a date range"""
        ce_client = boto3.Session().client("ce", region_name=self.region)
        
        response = ce_client.get_cost_and_forecast(
            TimePeriod={
                "Start": start_date,
                "End": end_date
            },
            Granularity="MONTHLY",
            Metrics=["UNBLENDED_COST"]
        )
        
        return response
    
    # =========================================================================
    # Auto-Adjusting Budgets
    # =========================================================================
    
    def create_auto_adjusting_budget(
        self,
        budget_name: str,
        auto_adjust_type: str,
        budget_limit: float,
        time_unit: TimeUnit = TimeUnit.MONTHLY,
        lookback_period_days: int = 12
    ) -> Dict[str, Any]:
        """
        Create an auto-adjusting budget
        
        Args:
            budget_name: Name of the budget
            auto_adjust_type: FORECAST or HISTORICAL
            budget_limit: Base budget limit
            time_unit: Time unit for the budget
            lookback_period_days: Days to look back for HISTORICAL type
        """
        auto_adjust_config = AutoAdjustConfig(
            auto_adjust_type=auto_adjust_type,
            lookback_period_days=lookback_period_days
        )
        
        return self.create_budget(
            budget_name=budget_name,
            budget_limit=budget_limit,
            time_unit=time_unit,
            budget_type=BudgetType.COST,
            auto_adjust_config=auto_adjust_config
        )
    
    def create_historical_auto_adjusting_budget(
        self,
        budget_name: str,
        budget_limit: float,
        lookback_period_days: int = 12,
        time_unit: TimeUnit = TimeUnit.MONTHLY
    ) -> Dict[str, Any]:
        """Create an auto-adjusting budget based on historical data"""
        return self.create_auto_adjusting_budget(
            budget_name=budget_name,
            auto_adjust_type="HISTORICAL",
            budget_limit=budget_limit,
            time_unit=time_unit,
            lookback_period_days=lookback_period_days
        )
    
    def create_forecast_auto_adjusting_budget(
        self,
        budget_name: str,
        budget_limit: float,
        time_unit: TimeUnit = TimeUnit.MONTHLY
    ) -> Dict[str, Any]:
        """Create an auto-adjusting budget based on forecasted data"""
        return self.create_auto_adjusting_budget(
            budget_name=budget_name,
            auto_adjust_type="FORECAST",
            budget_limit=budget_limit,
            time_unit=time_unit
        )
    
    # =========================================================================
    # Budget Forecasting
    # =========================================================================
    
    def get_budget_forecast(
        self,
        budget_name: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get forecasted spend for a budget"""
        if start_date is None:
            start_date = datetime.now().strftime("%Y-%m-%d")
        if end_date is None:
            end_date = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
            
        try:
            spend = self.budgets_client.describe_budget_spend(BudgetName=budget_name)
            budget_details = self.get_budget(budget_name)
            
            budget_limit = float(budget_details.get("Budget", {}).get("BudgetLimit", {}).get("Amount", 0))
            
            forecast_data = {
                "budget_name": budget_name,
                "budget_limit": budget_limit,
                "forecast_period": {"start": start_date, "end": end_date},
                "estimated_forecast": spend.get("BudgetSpend", {}),
                "status": "calculated"
            }
            
            return forecast_data
            
        except Exception as e:
            logger.error(f"Error getting budget forecast: {e}")
            raise
    
    def calculate_budget_projection(
        self,
        budget_name: str,
        days_ahead: int = 30
    ) -> Dict[str, Any]:
        """Calculate projected spend based on current burn rate"""
        budget = self.get_budget(budget_name)
        budget_limit = float(budget.get("Budget", {}).get("BudgetLimit", {}).get("Amount", 0))
        
        end_date = (datetime.now() + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
        
        try:
            ce_client = boto3.Session().client("ce", region_name=self.region)
            current_spend = ce_client.get_cost_and_usage(
                TimePeriod={
                    "Start": datetime.now().strftime("%Y-%m-%d"),
                    "End": datetime.now().strftime("%Y-%m-%d")
                },
                Granularity="DAILY",
                Metrics=["UNBLENDED_COST"]
            )
            
            daily_avg = 0
            if current_spend.get("ResultsByTime"):
                for result in current_spend.get("ResultsByTime", []):
                    for amount in result.get("Amounts", []):
                        daily_avg += float(amount.get("Amount", 0))
                        
            projected_total = daily_avg * days_ahead
            days_remaining = days_ahead
            burn_rate = budget_limit / days_remaining if days_remaining > 0 else 0
            
            return {
                "budget_name": budget_name,
                "budget_limit": budget_limit,
                "projected_spend": projected_total,
                "daily_average": daily_avg,
                "burn_rate": burn_rate,
                "days_remaining": days_remaining,
                "status": "on_track" if projected_total < budget_limit else "over_budget"
            }
            
        except Exception as e:
            logger.error(f"Error calculating budget projection: {e}")
            raise
    
    # =========================================================================
    # CloudWatch Integration
    # =========================================================================
    
    def publish_budget_metric(
        self,
        budget_name: str,
        metric_name: str,
        value: float,
        unit: str = "None"
    ) -> Dict[str, Any]:
        """
        Publish a budget metric to CloudWatch
        
        Args:
            budget_name: Name of the budget
            metric_name: Metric name to publish
            value: Metric value
            unit: CloudWatch unit
        """
        response = self.cloudwatch_client.put_metric_data(
            Namespace="AWS/Budgets",
            MetricData=[
                {
                    "MetricName": metric_name,
                    "Dimensions": [
                        {"Name": "BudgetName", "Value": budget_name}
                    ],
                    "Value": value,
                    "Unit": unit
                }
            ]
        )
        logger.info(f"Published metric {metric_name} for budget: {budget_name}")
        return response
    
    def create_budget_alarm(
        self,
        alarm_name: str,
        budget_name: str,
        threshold: float,
        comparison_operator: str = "GreaterThanThreshold",
        evaluation_periods: int = 1,
        period: int = 300,
        statistic: str = "Average"
    ) -> Dict[str, Any]:
        """
        Create a CloudWatch alarm for budget metrics
        
        Args:
            alarm_name: Name of the alarm
            budget_name: Associated budget name
            threshold: Alarm threshold
            comparison_operator: Comparison operator
            evaluation_periods: Number of evaluation periods
            period: Period in seconds
            statistic: CloudWatch statistic
        """
        response = self.cloudwatch_client.put_metric_alarm(
            AlarmName=alarm_name,
            AlarmDescription=f"CloudWatch alarm for budget: {budget_name}",
            Namespace="AWS/Budgets",
            MetricName="BudgetThresholdBreached",
            Dimensions=[
                {"Name": "BudgetName", "Value": budget_name}
            ],
            Threshold=threshold,
            ComparisonOperator=comparison_operator,
            EvaluationPeriods=evaluation_periods,
            Period=period,
            Statistic=statistic,
            ActionsEnabled=True
        )
        logger.info(f"Created CloudWatch alarm: {alarm_name}")
        return response
    
    def link_budget_to_alarm(
        self,
        alarm_name: str,
        budget_name: str,
        sns_topic_arn: Optional[str] = None
    ) -> Dict[str, Any]:
        """Link a budget notification to a CloudWatch alarm"""
        if sns_topic_arn:
            response = self.cloudwatch_client.put_metric_alarm(
                AlarmName=alarm_name,
                AlarmActions=[sns_topic_arn]
            )
        return {"alarm_name": alarm_name, "budget_name": budget_name, "linked": True}
    
    def get_budget_metrics(
        self,
        budget_name: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        period: int = 3600
    ) -> List[Dict[str, Any]]:
        """
        Get CloudWatch metrics for a budget
        
        Args:
            budget_name: Name of the budget
            start_time: Start time for metrics
            end_time: End time for metrics
            period: Period in seconds
        """
        if start_time is None:
            start_time = datetime.now() - timedelta(days=7)
        if end_time is None:
            end_time = datetime.now()
            
        response = self.cloudwatch_client.get_metric_statistics(
            Namespace="AWS/Budgets",
            MetricName="BudgetThresholdBreached",
            StartTime=start_time,
            EndTime=end_time,
            Period=period,
            Statistics=["Average", "Maximum", "Minimum"],
            Dimensions=[
                {"Name": "BudgetName", "Value": budget_name}
            ]
        )
        
        return response.get("Datapoints", [])
    
    def create_composite_alarm(
        self,
        alarm_name: str,
        alarm_rule: str,
        actions_enabled: bool = True
    ) -> Dict[str, Any]:
        """Create a composite CloudWatch alarm based on budget metrics"""
        response = self.cloudwatch_client.put_composite_alarm(
            AlarmName=alarm_name,
            AlarmRule=alarm_rule,
            ActionsEnabled=actions_enabled
        )
        logger.info(f"Created composite alarm: {alarm_name}")
        return response
    
    # =========================================================================
    # Utility Methods
    # =========================================================================
    
    def validate_budget_name(self, budget_name: str) -> bool:
        """Validate budget name format"""
        if not budget_name or len(budget_name) > 100:
            return False
        return True
    
    def estimate_monthly_budget(
        self,
        daily_cost: float,
        buffer_percentage: float = 10.0
    ) -> float:
        """Estimate monthly budget based on daily cost with buffer"""
        base_monthly = daily_cost * 30
        buffer = base_monthly * (buffer_percentage / 100)
        return base_monthly + buffer
    
    def calculate_threshold_values(
        self,
        budget_limit: float,
        thresholds: List[float]
    ) -> List[Dict[str, float]]:
        """Calculate actual threshold values from percentages"""
        return [
            {
                "percentage": t,
                "amount": budget_limit * (t / 100)
            }
            for t in thresholds
        ]
    
    def get_all_budget_alerts(self) -> List[Dict[str, Any]]:
        """Get all budget alerts across all budgets"""
        budgets = self.list_budgets()
        alerts = []
        
        for budget in budgets:
            budget_name = budget.get("BudgetName")
            try:
                notifications = self.budgets_client.describe_notifications(
                    BudgetName=budget_name
                )
                alerts.extend([
                    {
                        "budget_name": budget_name,
                        "notification": n
                    }
                    for n in notifications.get("Notifications", [])
                ])
            except Exception as e:
                logger.warning(f"Could not get notifications for {budget_name}: {e}")
                
        return alerts
    
    def export_budget_config(self, budget_name: str) -> Dict[str, Any]:
        """Export full budget configuration"""
        budget = self.get_budget(budget_name)
        
        try:
            notifications = self.budgets_client.describe_notifications(
                BudgetName=budget_name
            )
        except Exception:
            notifications = {}
            
        try:
            actions = self.describe_budget_actions(budget_name)
        except Exception:
            actions = []
            
        return {
            "budget": budget,
            "notifications": notifications,
            "actions": actions
        }


# =============================================================================
# Convenience Functions
# =============================================================================

def create_standard_budget(
    budget_name: str,
    monthly_limit: float,
    email_subscribers: Optional[List[str]] = None
) -> BudgetsIntegration:
    """
    Create a standard monthly cost budget with email alerts
    
    Args:
        budget_name: Name for the budget
        monthly_limit: Monthly spending limit
        email_subscribers: List of email addresses for alerts
        
    Returns:
        BudgetsIntegration instance with created budget
    """
    integration = BudgetsIntegration()
    
    notifications = []
    if email_subscribers:
        subscribers = [
            Subscriber(address=email, subscription_type="EMAIL")
            for email in email_subscribers
        ]
        notification = Notification(
            threshold=80.0,
            notification_type=NotificationType.ACTUAL,
            comparison_operator="GREATER_THAN",
            subscribers=subscribers
        )
        notifications.append(notification)
        
    integration.create_budget(
        budget_name=budget_name,
        budget_limit=monthly_limit,
        time_unit=TimeUnit.MONTHLY,
        budget_type=BudgetType.COST,
        notifications=notifications
    )
    
    return integration


def create_linked_account_budgets(
    account_id: str,
    service: str,
    monthly_limit: float,
    email_subscribers: List[str]
) -> Dict[str, Any]:
    """
    Create budgets for a linked account with service-level granularity
    
    Args:
        account_id: Linked account ID
        service: AWS service name
        monthly_limit: Monthly spending limit
        email_subscribers: List of email addresses
        
    Returns:
        Dict with created budget information
    """
    integration = BudgetsIntegration()
    
    cost_filters = [
        CostFilter(name="LinkedAccount", values=[account_id]),
        CostFilter(name="Service", values=[service])
    ]
    
    subscribers = [
        Subscriber(address=email, subscription_type="EMAIL")
        for email in email_subscribers
    ]
    
    notification = Notification(
        threshold=80.0,
        notification_type=NotificationType.ACTUAL,
        comparison_operator="GREATER_THAN",
        subscribers=subscribers
    )
    
    budget_name = f"{service}-{account_id}-budget"
    
    integration.create_budget(
        budget_name=budget_name,
        budget_limit=monthly_limit,
        time_unit=TimeUnit.MONTHLY,
        budget_type=BudgetType.COST,
        cost_filters=cost_filters,
        notifications=[notification]
    )
    
    return {
        "budget_name": budget_name,
        "account_id": account_id,
        "service": service,
        "monthly_limit": monthly_limit
    }
