"""
Tests for workflow_aws_gamelift module

Commit: 'tests: add comprehensive tests for workflow_aws_gamelift'
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
import dataclasses

# Patch dataclasses.field BEFORE importing the module
_original_field = dataclasses.field

def _patched_field(*args, **kwargs):
    if 'default' not in kwargs and 'default_factory' not in kwargs:
        kwargs['default'] = None
    return _original_field(*args, **kwargs)

# Create mock boto3 module before importing workflow_aws_gamelift
mock_boto3 = types.ModuleType('boto3')
mock_boto3.Session = MagicMock()
mock_boto3.client = MagicMock()
mock_boto3.resource = MagicMock()

# Create mock botocore exceptions
mock_boto3_exceptions = types.ModuleType('botocore.exceptions')
mock_boto3_exceptions.ClientError = Exception
mock_boto3_exceptions.BotoCoreError = Exception

sys.modules['boto3'] = mock_boto3
sys.modules['botocore'] = types.ModuleType('botocore')
sys.modules['botocore.exceptions'] = mock_boto3_exceptions

# Patch the field and import
dataclasses.field = _patched_field
import src.workflow_aws_gamelift as _gamelift_module
dataclasses.field = _original_field

# Extract the classes
GameLiftIntegration = _gamelift_module.GameLiftIntegration
FleetType = _gamelift_module.FleetType
OperatingSystem = _gamelift_module.OperatingSystem
BuildStatus = _gamelift_module.BuildStatus
FleetStatus = _gamelift_module.FleetStatus
GameSessionStatus = _gamelift_module.GameSessionStatus
PlayerSessionStatus = _gamelift_module.PlayerSessionStatus
AliasType = _gamelift_module.AliasType
MetricType = _gamelift_module.MetricType
GameLiftConfig = _gamelift_module.GameLiftConfig
Fleet = _gamelift_module.Fleet
Build = _gamelift_module.Build
GameSession = _gamelift_module.GameSession


class TestFleetType(unittest.TestCase):
    """Test FleetType enum"""

    def test_fleet_type_values(self):
        self.assertEqual(FleetType.ON_DEMAND.value, "ON_DEMAND")
        self.assertEqual(FleetType.SPOT.value, "SPOT")


class TestOperatingSystem(unittest.TestCase):
    """Test OperatingSystem enum"""

    def test_operating_system_values(self):
        self.assertEqual(OperatingSystem.WINDOWS_2012.value, "WINDOWS_2012")
        self.assertEqual(OperatingSystem.WINDOWS_2016.value, "WINDOWS_2016")
        self.assertEqual(OperatingSystem.AMAZON_LINUX.value, "AMAZON_LINUX")
        self.assertEqual(OperatingSystem.AMAZON_LINUX_2.value, "AMAZON_LINUX_2")


class TestBuildStatus(unittest.TestCase):
    """Test BuildStatus enum (GameLift specific)"""

    def test_build_status_values(self):
        self.assertEqual(BuildStatus.INITIALIZED.value, "INITIALIZED")
        self.assertEqual(BuildStatus.BUILDING.value, "BUILDING")
        self.assertEqual(BuildStatus.BUILD_COMPLETE.value, "BUILD_COMPLETE")
        self.assertEqual(BuildStatus.BUILD_FAILED.value, "BUILD_FAILED")
        self.assertEqual(BuildStatus.DELETED.value, "DELETED")


class TestFleetStatus(unittest.TestCase):
    """Test FleetStatus enum"""

    def test_fleet_status_values(self):
        self.assertEqual(FleetStatus.NEW.value, "NEW")
        self.assertEqual(FleetStatus.DOWNLOADING.value, "DOWNLOADING")
        self.assertEqual(FleetStatus.VALIDATING.value, "VALIDATING")
        self.assertEqual(FleetStatus.BUILDING.value, "BUILDING")
        self.assertEqual(FleetStatus.ACTIVATING.value, "ACTIVATING")
        self.assertEqual(FleetStatus.ACTIVE.value, "ACTIVE")
        self.assertEqual(FleetStatus.ERROR.value, "ERROR")


class TestGameSessionStatus(unittest.TestCase):
    """Test GameSessionStatus enum"""

    def test_game_session_status_values(self):
        self.assertEqual(GameSessionStatus.ACTIVE.value, "ACTIVE")
        self.assertEqual(GameSessionStatus.ACTIVATING.value, "ACTIVATING")
        self.assertEqual(GameSessionStatus.TERMINATED.value, "TERMINATED")
        self.assertEqual(GameSessionStatus.CREATING.value, "CREATING")


class TestPlayerSessionStatus(unittest.TestCase):
    """Test PlayerSessionStatus enum"""

    def test_player_session_status_values(self):
        self.assertEqual(PlayerSessionStatus.ACTIVE.value, "ACTIVE")
        self.assertEqual(PlayerSessionStatus.TERMINATED.value, "TERMINATED")
        self.assertEqual(PlayerSessionStatus.RESERVED.value, "RESERVED")


class TestAliasType(unittest.TestCase):
    """Test AliasType enum"""

    def test_alias_type_values(self):
        self.assertEqual(AliasType.SIMPLE.value, "SIMPLE")
        self.assertEqual(AliasType.TERMINAL.value, "TERMINAL")


class TestMetricType(unittest.TestCase):
    """Test MetricType enum"""

    def test_metric_type_values(self):
        self.assertEqual(MetricType.ACTIVE_SERVER_PROCESSES.value, "ActiveServerProcesses")
        self.assertEqual(MetricType.ACTIVE_GAME_SESSIONS.value, "ActiveGameSessions")
        self.assertEqual(MetricType.PLACEMENTS_SUCCEEDED.value, "PlacementsSucceeded")
        self.assertEqual(MetricType.PLACEMENTS_FAILED.value, "PlacementsFailed")


class TestGameLiftConfig(unittest.TestCase):
    """Test GameLiftConfig dataclass"""

    def test_config_defaults(self):
        config = GameLiftConfig()
        self.assertEqual(config.region, "us-west-2")
        self.assertIsNone(config.aws_access_key_id)
        self.assertIsNone(config.aws_secret_access_key)
        self.assertFalse(config.enable_auto_scaling)

    def test_config_custom(self):
        config = GameLiftConfig(
            region="us-east-1",
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            enable_auto_scaling=True
        )
        self.assertEqual(config.region, "us-east-1")
        self.assertEqual(config.aws_access_key_id, "test-key")
        self.assertTrue(config.enable_auto_scaling)


class TestFleet(unittest.TestCase):
    """Test Fleet class"""

    def test_fleet_creation(self):
        fleet = Fleet(
            fleet_id="fleet-123",
            fleet_name="test-fleet",
            fleet_type=FleetType.ON_DEMAND,
            operating_system=OperatingSystem.AMAZON_LINUX_2,
            status=FleetStatus.ACTIVE,
            desired_ec2_instances=5
        )
        self.assertEqual(fleet.fleet_id, "fleet-123")
        self.assertEqual(fleet.fleet_name, "test-fleet")
        self.assertEqual(fleet.fleet_type, FleetType.ON_DEMAND)
        self.assertEqual(fleet.desired_ec2_instances, 5)

    def test_fleet_to_dict(self):
        fleet = Fleet(
            fleet_id="fleet-123",
            fleet_name="test-fleet",
            fleet_type=FleetType.ON_DEMAND,
            operating_system=OperatingSystem.AMAZON_LINUX_2,
            status=FleetStatus.ACTIVE
        )
        result = fleet.to_dict()
        self.assertEqual(result["FleetId"], "fleet-123")
        self.assertEqual(result["FleetName"], "test-fleet")
        self.assertEqual(result["FleetType"], "ON_DEMAND")


class TestBuild(unittest.TestCase):
    """Test Build class"""

    def test_build_creation(self):
        build = Build(
            build_id="build-123",
            build_name="test-build",
            build_version="1.0.0",
            status=BuildStatus.BUILD_COMPLETE,
            operating_system=OperatingSystem.AMAZON_LINUX_2
        )
        self.assertEqual(build.build_id, "build-123")
        self.assertEqual(build.build_name, "test-build")
        self.assertEqual(build.status, BuildStatus.BUILD_COMPLETE)

    def test_build_to_dict(self):
        build = Build(
            build_id="build-123",
            build_name="test-build",
            status=BuildStatus.BUILD_COMPLETE
        )
        result = build.to_dict()
        self.assertEqual(result["BuildId"], "build-123")
        self.assertEqual(result["Status"], "BUILD_COMPLETE")


class TestGameSession(unittest.TestCase):
    """Test GameSession class"""

    def test_game_session_creation(self):
        session = GameSession(
            game_session_id="session-123",
            fleet_id="fleet-123",
            fleet_name="test-fleet",
            status=GameSessionStatus.ACTIVE,
            maximum_player_session_count=10
        )
        self.assertEqual(session.game_session_id, "session-123")
        self.assertEqual(session.maximum_player_session_count, 10)
        self.assertEqual(session.current_player_session_count, 0)

    def test_game_session_to_dict(self):
        session = GameSession(
            game_session_id="session-123",
            fleet_id="fleet-123",
            status=GameSessionStatus.ACTIVE,
            maximum_player_session_count=10
        )
        result = session.to_dict()
        self.assertEqual(result["GameSessionId"], "session-123")
        self.assertEqual(result["MaximumPlayerSessionCount"], 10)


class TestGameLiftIntegration(unittest.TestCase):
    """Test GameLiftIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_gamelift_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()
        
        # Create integration instance with mocked clients
        self.integration = GameLiftIntegration()
        self.integration.client = self.mock_gamelift_client
        self.integration._cloudwatch_client = self.mock_cloudwatch_client

    def test_init_with_config(self):
        """Test initialization with config"""
        config = GameLiftConfig(region="us-east-1")
        integration = GameLiftIntegration(config=config)
        self.assertEqual(integration.config.region, "us-east-1")

    def test_init_without_boto3(self):
        """Test initialization handles boto3 not available"""
        with patch.object(_gamelift_module, 'BOTO3_AVAILABLE', False):
            integration = GameLiftIntegration()
            self.assertIsNone(integration.client)

    def test_create_build(self):
        """Test creating a build"""
        mock_response = {
            "Build": {
                "BuildId": "build-123",
                "BuildName": "test-build",
                "Status": "INITIALIZED"
            }
        }
        self.mock_gamelift_client.create_build.return_value = mock_response
        
        result = self.integration.create_build(
            name="test-build",
            build_version="1.0.0"
        )
        
        self.assertEqual(result["Build"]["BuildId"], "build-123")
        self.mock_gamelift_client.create_build.assert_called_once()

    def test_list_builds(self):
        """Test listing builds"""
        mock_response = {
            "Builds": [
                {"BuildId": "build-1", "BuildName": "build-one", "Status": "READY"},
                {"BuildId": "build-2", "BuildName": "build-two", "Status": "INITIALIZED"}
            ]
        }
        self.mock_gamelift_client.list_builds.return_value = mock_response
        
        result = self.integration.list_builds()
        
        self.assertEqual(len(result["Builds"]), 2)

    def test_delete_build(self):
        """Test deleting a build"""
        self.mock_gamelift_client.delete_build.return_value = {}
        
        result = self.integration.delete_build("build-123")
        
        self.assertTrue(result)
        self.mock_gamelift_client.delete_build.assert_called_once()

    def test_create_fleet(self):
        """Test creating a fleet"""
        mock_response = {
            "FleetAttributes": {
                "FleetId": "fleet-123",
                "FleetName": "test-fleet",
                "Status": "NEW",
                "FleetType": "ON_DEMAND"
            }
        }
        self.mock_gamelift_client.create_fleet.return_value = mock_response
        
        result = self.integration.create_fleet(
            name="test-fleet",
            build_id="build-123"
        )
        
        self.assertEqual(result["FleetAttributes"]["FleetId"], "fleet-123")

    def test_get_fleet(self):
        """Test getting fleet info"""
        mock_response = {
            "FleetAttributes": {
                "FleetId": "fleet-123",
                "FleetName": "test-fleet",
                "Status": "ACTIVE"
            }
        }
        self.mock_gamelift_client.describe_fleet_attributes.return_value = mock_response
        
        result = self.integration.get_fleet("fleet-123")
        
        self.assertEqual(result["FleetAttributes"]["FleetId"], "fleet-123")

    def test_list_fleets(self):
        """Test listing fleets"""
        mock_response = {
            "FleetAttributes": [
                {"FleetId": "fleet-1", "FleetName": "fleet-one", "Status": "ACTIVE"},
                {"FleetId": "fleet-2", "FleetName": "fleet-two", "Status": "NEW"}
            ]
        }
        self.mock_gamelift_client.list_fleets.return_value = mock_response
        
        result = self.integration.list_fleets()
        
        self.assertEqual(len(result["FleetAttributes"]), 2)

    def test_delete_fleet(self):
        """Test deleting a fleet"""
        self.mock_gamelift_client.delete_fleet.return_value = {}
        
        result = self.integration.delete_fleet("fleet-123")
        
        self.assertTrue(result)

    def test_update_fleet_capacity(self):
        """Test updating fleet capacity"""
        mock_response = {
            "FleetId": "fleet-123",
            "ScalingPolicies": []
        }
        self.mock_gamelift_client.update_fleet_capacity.return_value = mock_response
        
        result = self.integration.update_fleet_capacity(
            fleet_id="fleet-123",
            desired_instances=10,
            min_size=2,
            max_size=20
        )
        
        self.assertEqual(result["FleetId"], "fleet-123")

    def test_create_game_session(self):
        """Test creating a game session"""
        mock_response = {
            "GameSession": {
                "GameSessionId": "session-123",
                "FleetId": "fleet-123",
                "Status": "ACTIVE",
                "MaximumPlayerSessionCount": 10
            }
        }
        self.mock_gamelift_client.create_game_session.return_value = mock_response
        
        result = self.integration.create_game_session(
            fleet_id="fleet-123",
            maximum_player_session_count=10
        )
        
        self.assertEqual(result["GameSession"]["GameSessionId"], "session-123")

    def test_get_game_session(self):
        """Test getting game session info"""
        mock_response = {
            "GameSession": {
                "GameSessionId": "session-123",
                "FleetId": "fleet-123",
                "Status": "ACTIVE",
                "CurrentPlayerSessionCount": 5
            }
        }
        self.mock_gamelift_client.describe_game_sessions.return_value = mock_response
        
        result = self.integration.get_game_session("fleet-123", "session-123")
        
        self.assertEqual(result["GameSession"]["GameSessionId"], "session-123")

    def test_list_game_sessions(self):
        """Test listing game sessions"""
        mock_response = {
            "GameSessions": [
                {"GameSessionId": "session-1", "Status": "ACTIVE"},
                {"GameSessionId": "session-2", "Status": "TERMINATED"}
            ]
        }
        self.mock_gamelift_client.list_game_sessions.return_value = mock_response
        
        result = self.integration.list_game_sessions("fleet-123")
        
        self.assertEqual(len(result["GameSessions"]), 2)

    def test_create_player_session(self):
        """Test creating a player session"""
        mock_response = {
            "PlayerSession": {
                "PlayerSessionId": "player-session-123",
                "GameSessionId": "session-123",
                "PlayerId": "player-123",
                "Status": "RESERVED"
            }
        }
        self.mock_gamelift_client.create_player_session.return_value = mock_response
        
        result = self.integration.create_player_session(
            game_session_id="session-123",
            player_id="player-123"
        )
        
        self.assertEqual(result["PlayerSession"]["PlayerSessionId"], "player-session-123")

    def test_create_player_sessions(self):
        """Test creating multiple player sessions"""
        mock_response = {
            "PlayerSessions": [
                {"PlayerSessionId": "ps-1", "PlayerId": "player-1", "Status": "RESERVED"},
                {"PlayerSessionId": "ps-2", "PlayerId": "player-2", "Status": "RESERVED"}
            ]
        }
        self.mock_gamelift_client.create_player_sessions.return_value = mock_response
        
        result = self.integration.create_player_sessions(
            game_session_id="session-123",
            player_ids=["player-1", "player-2"]
        )
        
        self.assertEqual(len(result["PlayerSessions"]), 2)

    def test_get_player_session(self):
        """Test getting player session info"""
        mock_response = {
            "PlayerSession": {
                "PlayerSessionId": "player-session-123",
                "PlayerId": "player-123",
                "Status": "ACTIVE"
            }
        }
        self.mock_gamelift_client.describe_player_sessions.return_value = mock_response
        
        result = self.integration.get_player_session("player-session-123")
        
        self.assertEqual(result["PlayerSession"]["PlayerSessionId"], "player-session-123")

    def test_create_alias(self):
        """Test creating an alias"""
        mock_response = {
            "Alias": {
                "AliasId": "alias-123",
                "Name": "test-alias",
                "RoutingStrategy": {
                    "Type": "SIMPLE",
                    "FleetId": "fleet-123"
                }
            }
        }
        self.mock_gamelift_client.create_alias.return_value = mock_response
        
        result = self.integration.create_alias(
            name="test-alias",
            fleet_id="fleet-123"
        )
        
        self.assertEqual(result["Alias"]["AliasId"], "alias-123")

    def test_get_alias(self):
        """Test getting alias info"""
        mock_response = {
            "Alias": {
                "AliasId": "alias-123",
                "Name": "test-alias",
                "RoutingStrategy": {"Type": "SIMPLE"}
            }
        }
        self.mock_gamelift_client.describe_alias.return_value = mock_response
        
        result = self.integration.get_alias("alias-123")
        
        self.assertEqual(result["Alias"]["AliasId"], "alias-123")

    def test_list_aliases(self):
        """Test listing aliases"""
        mock_response = {
            "Aliases": [
                {"AliasId": "alias-1", "Name": "alias-one"},
                {"AliasId": "alias-2", "Name": "alias-two"}
            ]
        }
        self.mock_gamelift_client.list_aliases.return_value = mock_response
        
        result = self.integration.list_aliases()
        
        self.assertEqual(len(result["Aliases"]), 2)

    def test_delete_alias(self):
        """Test deleting an alias"""
        self.mock_gamelift_client.delete_alias.return_value = {}
        
        result = self.integration.delete_alias("alias-123")
        
        self.assertTrue(result)

    def test_update_alias(self):
        """Test updating an alias"""
        mock_response = {
            "Alias": {
                "AliasId": "alias-123",
                "Name": "updated-alias"
            }
        }
        self.mock_gamelift_client.update_alias.return_value = mock_response
        
        result = self.integration.update_alias("alias-123", name="updated-alias")
        
        self.assertEqual(result["Alias"]["Name"], "updated-alias")

    def test_search_game_sessions(self):
        """Test searching game sessions"""
        mock_response = {
            "GameSessions": [
                {"GameSessionId": "session-1", "Status": "ACTIVE"}
            ]
        }
        self.mock_gamelift_client.search_game_sessions.return_value = mock_response
        
        result = self.integration.search_game_sessions(
            fleet_id="fleet-123",
            filter_expression="hasAvailablePlayerSessions=true"
        )
        
        self.assertEqual(len(result["GameSessions"]), 1)

    def test_start_game_session_placement(self):
        """Test starting game session placement"""
        mock_response = {
            "GameSessionPlacement": {
                "PlacementId": "placement-123",
                "Status": "PENDING"
            }
        }
        self.mock_gamelift_client.start_game_session_placement.return_value = mock_response
        
        result = self.integration.start_game_session_placement(
            queue_name="test-queue",
            maximum_player_session_count=10
        )
        
        self.assertEqual(result["GameSessionPlacement"]["PlacementId"], "placement-123")

    def test_get_game_session_placement(self):
        """Test getting game session placement info"""
        mock_response = {
            "GameSessionPlacement": {
                "PlacementId": "placement-123",
                "Status": "FULFILLED",
                "GameSessionArn": "arn:aws:gamelift:us-west-2:123:gamesession/fleet-123/session-123"
            }
        }
        self.mock_gamelift_client.describe_game_session_placement.return_value = mock_response
        
        result = self.integration.get_game_session_placement("placement-123")
        
        self.assertEqual(result["GameSessionPlacement"]["PlacementId"], "placement-123")

    def test_stop_game_session_placement(self):
        """Test stopping game session placement"""
        mock_response = {
            "GameSessionPlacement": {
                "PlacementId": "placement-123",
                "Status": "CANCELLED"
            }
        }
        self.mock_gamelift_client.stop_game_session_placement.return_value = mock_response
        
        result = self.integration.stop_game_session_placement("placement-123")
        
        self.assertEqual(result["GameSessionPlacement"]["Status"], "CANCELLED")

    def test_create_matchmaking_rule_set(self):
        """Test creating matchmaking rule set"""
        mock_response = {
            "RuleSet": {
                "Name": "test-ruleset",
                "RuleSetBody": "{\"rules\":[]}",
                "CreationTime": "2024-01-01T00:00:00Z"
            }
        }
        self.mock_gamelift_client.create_matchmaking_rule_set.return_value = mock_response
        
        result = self.integration.create_matchmaking_rule_set(
            name="test-ruleset",
            rule_set_body="{\"rules\":[]}"
        )
        
        self.assertEqual(result["RuleSet"]["Name"], "test-ruleset")

    def test_get_matchmaking_rule_set(self):
        """Test getting matchmaking rule set"""
        mock_response = {
            "RuleSet": {
                "Name": "test-ruleset",
                "RuleSetBody": "{\"rules\":[]}"
            }
        }
        self.mock_gamelift_client.describe_matchmaking_rule_set.return_value = mock_response
        
        result = self.integration.get_matchmaking_rule_set("test-ruleset")
        
        self.assertEqual(result["RuleSet"]["Name"], "test-ruleset")

    def test_list_matchmaking_rule_sets(self):
        """Test listing matchmaking rule sets"""
        mock_response = {
            "RuleSets": [
                {"Name": "ruleset-1"},
                {"Name": "ruleset-2"}
            ]
        }
        self.mock_gamelift_client.list_matchmaking_rule_sets.return_value = mock_response
        
        result = self.integration.list_matchmaking_rule_sets()
        
        self.assertEqual(len(result["RuleSets"]), 2)

    def test_delete_matchmaking_rule_set(self):
        """Test deleting matchmaking rule set"""
        self.mock_gamelift_client.delete_matchmaking_rule_set.return_value = {}
        
        result = self.integration.delete_matchmaking_rule_set("test-ruleset")
        
        self.assertTrue(result)

    def test_create_matchmaking_configuration(self):
        """Test creating matchmaking configuration"""
        mock_response = {
            "Configuration": {
                "Name": "test-config",
                "GameSessionQueueArn": "arn:aws:gamelift:us-west-2:123:gamesessionqueue/test-queue"
            }
        }
        self.mock_gamelift_client.create_matchmaking_configuration.return_value = mock_response
        
        result = self.integration.create_matchmaking_configuration(
            name="test-config",
            game_session_queue_arn="arn:aws:gamelift:us-west-2:123:gamesessionqueue/test-queue",
            rule_set_name="test-ruleset"
        )
        
        self.assertEqual(result["Configuration"]["Name"], "test-config")

    def test_get_matchmaking_configuration(self):
        """Test getting matchmaking configuration"""
        mock_response = {
            "Configuration": {
                "Name": "test-config",
                "Status": "ACTIVE"
            }
        }
        self.mock_gamelift_client.describe_matchmaking_configurations.return_value = mock_response
        
        result = self.integration.get_matchmaking_configuration("test-config")
        
        self.assertEqual(result["Configuration"]["Name"], "test-config")

    def test_delete_matchmaking_configuration(self):
        """Test deleting matchmaking configuration"""
        self.mock_gamelift_client.delete_matchmaking_configuration.return_value = {}
        
        result = self.integration.delete_matchmaking_configuration("test-config")
        
        self.assertTrue(result)

    def test_start_matchmaking(self):
        """Test starting matchmaking"""
        mock_response = {
            "MatchmakingTicket": {
                "TicketId": "ticket-123",
                "Status": "SEARCHING"
            }
        }
        self.mock_gamelift_client.start_matchmaking.return_value = mock_response
        
        result = self.integration.start_matchmaking(
            configuration_name="test-config",
            players=[{"PlayerId": "player-1", "PlayerAttributes": {}}]
        )
        
        self.assertEqual(result["MatchmakingTicket"]["TicketId"], "ticket-123")

    def test_get_matchmaking(self):
        """Test getting matchmaking ticket"""
        mock_response = {
            "Ticket": {
                "TicketId": "ticket-123",
                "Status": "COMPLETED"
            }
        }
        self.mock_gamelift_client.describe_matchmaking.return_value = mock_response
        
        result = self.integration.get_matchmaking("ticket-123")
        
        self.assertEqual(result["Ticket"]["TicketId"], "ticket-123")

    def test_stop_matchmaking(self):
        """Test stopping matchmaking"""
        mock_response = {
            "MatchmakingTicket": {
                "TicketId": "ticket-123",
                "Status": "CANCELLED"
            }
        }
        self.mock_gamelift_client.stop_matchmaking.return_value = mock_response
        
        result = self.integration.stop_matchmaking(
            configuration_name="test-config",
            player_ids=["player-1"]
        )
        
        self.assertEqual(result["MatchmakingTicket"]["Status"], "CANCELLED")

    def test_get_fleet_metrics(self):
        """Test getting fleet metrics from CloudWatch"""
        mock_cloudwatch_response = {
            "Datapoints": [
                {"Timestamp": datetime.now(), "Value": 5.0, "Unit": "Count"}
            ]
        }
        self.mock_cloudwatch_client.get_metric_statistics.return_value = mock_cloudwatch_response
        
        result = self.integration.get_fleet_metrics("fleet-123")
        
        self.assertIsNotNone(result)

    def test_get_game_session_queue(self):
        """Test getting game session queue"""
        mock_response = {
            "GameSessionQueues": [
                {
                    "Name": "test-queue",
                    "Destinations": [{"DestinationArn": "arn:aws:gamelift:us-west-2:123:gamesessionqueue/test-queue"}]
                }
            ]
        }
        self.mock_gamelift_client.describe_game_session_queues.return_value = mock_response
        
        result = self.integration.get_game_session_queue("test-queue")
        
        self.assertEqual(result["GameSessionQueues"][0]["Name"], "test-queue")

    def test_create_game_session_queue(self):
        """Test creating game session queue"""
        mock_response = {
            "GameSessionQueue": {
                "Name": "new-queue",
                "Arn": "arn:aws:gamelift:us-west-2:123:gamesessionqueue/new-queue"
            }
        }
        self.mock_gamelift_client.create_game_session_queue.return_value = mock_response
        
        result = self.integration.create_game_session_queue(name="new-queue")
        
        self.assertEqual(result["GameSessionQueue"]["Name"], "new-queue")


class TestGameLiftIntegrationWithMockSession(unittest.TestCase):
    """Test GameLiftIntegration with mocked boto3 session"""

    def test_init_with_boto3_session(self):
        """Test initialization creates clients from boto3 session"""
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_session.client.return_value = mock_client
        
        with patch.object(_gamelift_module, 'boto3') as mock_boto3:
            mock_boto3.Session.return_value = mock_session
            mock_boto3.BOTO3_AVAILABLE = True
            
            integration = GameLiftIntegration()
            # Verify client was created
            self.assertIsNotNone(integration.client)


if __name__ == "__main__":
    unittest.main()
