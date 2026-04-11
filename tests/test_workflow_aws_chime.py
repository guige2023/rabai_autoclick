"""
Tests for workflow_aws_chime module
"""
import sys
sys.path.insert(0, '/Users/guige/my_project')

import unittest
from unittest.mock import Mock, patch, MagicMock, PropertyMock
import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Any
import types

# Create mock boto3 module before importing workflow_aws_chime
mock_boto3 = types.ModuleType('boto3')
mock_boto3.Session = MagicMock()
mock_boto3.client = MagicMock()

# Create mock botocore exceptions
mock_boto3_exceptions = types.ModuleType('botocore.exceptions')
mock_boto3_exceptions.ClientError = Exception

sys.modules['boto3'] = mock_boto3
sys.modules['botocore'] = types.ModuleType('botocore')
sys.modules['botocore.exceptions'] = mock_boto3_exceptions

# Now we can import the module
from src.workflow_aws_chime import (
    ChimeIntegration,
    MeetingInfo,
    AttendeeInfo,
    ChannelInfo,
)


class TestMeetingInfo(unittest.TestCase):
    """Test MeetingInfo dataclass"""

    def test_meeting_info_creation(self):
        meeting = MeetingInfo(
            meeting_id="meeting-123",
            meeting_arn="arn:aws:chime:us-east-1:123456789:meeting/abc-123",
            external_meeting_id="ext-123",
            media_region="us-east-1"
        )
        self.assertEqual(meeting.meeting_id, "meeting-123")
        self.assertEqual(meeting.media_region, "us-east-1")

    def test_meeting_info_defaults(self):
        meeting = MeetingInfo(
            meeting_id="meeting-123",
            meeting_arn="arn:aws:chime:...meeting-123"
        )
        self.assertIsNone(meeting.external_meeting_id)
        self.assertEqual(meeting.media_region, "us-east-1")


class TestAttendeeInfo(unittest.TestCase):
    """Test AttendeeInfo dataclass"""

    def test_attendee_info_creation(self):
        attendee = AttendeeInfo(
            attendee_id="attendee-123",
            attendee_arn="arn:aws:chime:...attendee/abc-123",
            external_user_id="user-123",
            join_token="token-xyz"
        )
        self.assertEqual(attendee.attendee_id, "attendee-123")
        self.assertEqual(attendee.join_token, "token-xyz")


class TestChannelInfo(unittest.TestCase):
    """Test ChannelInfo dataclass"""

    def test_channel_info_creation(self):
        channel = ChannelInfo(
            channel_arn="arn:aws:chime:...channel/abc-123",
            channel_name="test-channel",
            channel_type="STANDARD",
            moderation_mode="UNMODERATED"
        )
        self.assertEqual(channel.channel_name, "test-channel")
        self.assertEqual(channel.channel_type, "STANDARD")


class TestChimeIntegration(unittest.TestCase):
    """Test ChimeIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_session = MagicMock()
        self.mock_chime_client = MagicMock()
        self.mock_chime_sdk_meetings = MagicMock()
        self.mock_chime_sdk_messaging = MagicMock()
        self.mock_cloudwatch_client = MagicMock()

        mock_boto3.Session.return_value = self.mock_session
        self.mock_session.client.side_effect = [
            self.mock_chime_client,
            self.mock_chime_sdk_meetings,
            self.mock_chime_sdk_messaging,
            self.mock_cloudwatch_client,
        ]

    def test_integration_initialization(self):
        """Test ChimeIntegration initialization"""
        integration = ChimeIntegration(
            region_name="us-east-1"
        )
        self.assertEqual(integration.region_name, "us-east-1")

    def test_integration_with_credentials(self):
        """Test ChimeIntegration with explicit credentials"""
        integration = ChimeIntegration(
            region_name="us-west-2",
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            aws_session_token="test-token"
        )
        self.assertEqual(integration.region_name, "us-west-2")

    # =========================================================================
    # Meeting Management Tests
    # =========================================================================

    def test_create_meeting(self):
        """Test creating a meeting"""
        integration = ChimeIntegration()
        self.mock_chime_sdk_meetings.create_meeting.return_value = {
            "Meeting": {
                "MeetingId": "meeting-123",
                "MeetingArn": "arn:aws:chime:us-east-1:123456789:meeting/m-123",
                "CreatedTimestamp": "2024-01-01T00:00:00Z"
            }
        }

        result = integration.create_meeting(
            external_meeting_id="ext-123",
            media_region="us-east-1"
        )

        self.assertEqual(result.meeting_id, "meeting-123")
        self.assertEqual(result.external_meeting_id, "ext-123")

    def test_create_meeting_with_notifications(self):
        """Test creating a meeting with notifications"""
        integration = ChimeIntegration()
        self.mock_chime_sdk_meetings.create_meeting.return_value = {
            "Meeting": {
                "MeetingId": "meeting-123",
                "MeetingArn": "arn:aws:chime:...meeting/m-123",
                "CreatedTimestamp": "2024-01-01T00:00:00Z"
            }
        }

        notifications = {"SNSTopicArn": "arn:aws:sns:...topic-123"}
        result = integration.create_meeting(
            external_meeting_id="ext-123",
            notifications_config=notifications
        )

        call_kwargs = self.mock_chime_sdk_meetings.create_meeting.call_args[1]
        self.assertEqual(call_kwargs["NotificationsConfiguration"], notifications)

    def test_get_meeting(self):
        """Test getting a meeting"""
        integration = ChimeIntegration()
        integration._meetings["meeting-123"] = MeetingInfo(
            meeting_id="meeting-123",
            meeting_arn="arn:aws:chime:...meeting/m-123"
        )

        result = integration.get_meeting("meeting-123")

        self.assertIsNotNone(result)
        self.assertEqual(result.meeting_id, "meeting-123")

    def test_get_meeting_not_in_cache(self):
        """Test getting a meeting not in cache"""
        integration = ChimeIntegration()
        self.mock_chime_sdk_meetings.get_meeting.return_value = {
            "Meeting": {
                "MeetingId": "meeting-456",
                "MeetingArn": "arn:aws:chime:...meeting/m-456",
                "MediaRegion": "us-west-2",
                "CreatedTimestamp": "2024-01-01T00:00:00Z"
            }
        }

        result = integration.get_meeting("meeting-456")

        self.assertIsNotNone(result)
        self.assertEqual(result.meeting_id, "meeting-456")

    def test_get_meeting_not_found(self):
        """Test getting a non-existent meeting"""
        integration = ChimeIntegration()
        self.mock_chime_sdk_meetings.get_meeting.side_effect = Exception("Not found")

        result = integration.get_meeting("nonexistent")

        self.assertIsNone(result)

    def test_delete_meeting(self):
        """Test deleting a meeting"""
        integration = ChimeIntegration()
        integration._meetings["meeting-123"] = MeetingInfo(
            meeting_id="meeting-123",
            meeting_arn="arn:aws:chime:...meeting/m-123"
        )
        self.mock_chime_sdk_meetings.delete_meeting.return_value = {}

        result = integration.delete_meeting("meeting-123")

        self.assertTrue(result)
        self.assertNotIn("meeting-123", integration._meetings)

    def test_delete_meeting_error(self):
        """Test deleting a meeting with error"""
        integration = ChimeIntegration()
        self.mock_chime_sdk_meetings.delete_meeting.side_effect = Exception("Delete failed")

        result = integration.delete_meeting("meeting-123")

        self.assertFalse(result)

    def test_list_meetings(self):
        """Test listing meetings"""
        integration = ChimeIntegration()
        self.mock_chime_sdk_meetings.list_meetings.return_value = {
            "Meetings": [
                {
                    "MeetingId": "meeting-1",
                    "MeetingArn": "arn:aws:chime:...meeting/1",
                    "ExternalMeetingId": "ext-1",
                    "MediaRegion": "us-east-1",
                    "CreatedTimestamp": "2024-01-01T00:00:00Z"
                },
                {
                    "MeetingId": "meeting-2",
                    "MeetingArn": "arn:aws:chime:...meeting/2",
                    "ExternalMeetingId": "ext-2",
                    "MediaRegion": "us-west-2",
                    "CreatedTimestamp": "2024-01-01T00:00:00Z"
                }
            ]
        }

        result = integration.list_meetings()

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].meeting_id, "meeting-1")

    # =========================================================================
    # Attendee Management Tests
    # =========================================================================

    def test_create_attendee(self):
        """Test creating an attendee"""
        integration = ChimeIntegration()
        self.mock_chime_sdk_meetings.create_attendee.return_value = {
            "Attendee": {
                "AttendeeId": "attendee-123",
                "AttendeeArn": "arn:aws:chime:...attendee/a-123",
                "JoinToken": "token-xyz"
            }
        }

        result = integration.create_attendee(
            meeting_id="meeting-123",
            external_user_id="user-123"
        )

        self.assertEqual(result.attendee_id, "attendee-123")
        self.assertEqual(result.join_token, "token-xyz")
        self.assertIn("meeting-123", integration._attendees)

    def test_get_attendee(self):
        """Test getting an attendee"""
        integration = ChimeIntegration()
        self.mock_chime_sdk_meetings.get_attendee.return_value = {
            "Attendee": {
                "AttendeeId": "attendee-123",
                "AttendeeArn": "arn:aws:chime:...attendee/a-123",
                "ExternalUserId": "user-123"
            }
        }

        result = integration.get_attendee("meeting-123", "attendee-123")

        self.assertIsNotNone(result)
        self.assertEqual(result.attendee_id, "attendee-123")

    def test_get_attendee_not_found(self):
        """Test getting a non-existent attendee"""
        integration = ChimeIntegration()
        self.mock_chime_sdk_meetings.get_attendee.side_effect = Exception("Not found")

        result = integration.get_attendee("meeting-123", "nonexistent")

        self.assertIsNone(result)

    def test_list_attendees(self):
        """Test listing attendees"""
        integration = ChimeIntegration()
        self.mock_chime_sdk_meetings.list_attendees.return_value = {
            "Attendees": [
                {
                    "AttendeeId": "attendee-1",
                    "AttendeeArn": "arn:aws:chime:...attendee/1",
                    "ExternalUserId": "user-1"
                },
                {
                    "AttendeeId": "attendee-2",
                    "AttendeeArn": "arn:aws:chime:...attendee/2",
                    "ExternalUserId": "user-2"
                }
            ]
        }

        result = integration.list_attendees("meeting-123")

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].attendee_id, "attendee-1")

    def test_delete_attendee(self):
        """Test deleting an attendee"""
        integration = ChimeIntegration()
        integration._attendees["meeting-123"] = [
            AttendeeInfo(
                attendee_id="attendee-123",
                attendee_arn="arn:aws:chime:...attendee/a-123"
            )
        ]
        self.mock_chime_sdk_meetings.delete_attendee.return_value = {}

        result = integration.delete_attendee("meeting-123", "attendee-123")

        self.assertTrue(result)

    def test_delete_attendee_not_found(self):
        """Test deleting a non-existent attendee"""
        integration = ChimeIntegration()
        self.mock_chime_sdk_meetings.delete_attendee.side_effect = Exception("Not found")

        result = integration.delete_attendee("meeting-123", "nonexistent")

        self.assertFalse(result)

    # =========================================================================
    # Messaging Tests
    # =========================================================================

    def test_create_channel(self):
        """Test creating a channel"""
        integration = ChimeIntegration()
        self.mock_chime_sdk_messaging.create_channel.return_value = {
            "Channel": {
                "ChannelArn": "arn:aws:chime:...channel/c-123",
                "Name": "test-channel",
                "ChannelType": "STANDARD",
                "Mode": "UNRESTRICTED"
            }
        }

        result = integration.create_channel(
            chime_app_instance_arn="arn:aws:chime:...app-instance/ai-123",
            channel_name="test-channel"
        )

        self.assertEqual(result.channel_arn, "arn:aws:chime:...channel/c-123")
        self.assertEqual(result.channel_name, "test-channel")

    def test_create_channel_with_privacy(self):
        """Test creating a private channel"""
        integration = ChimeIntegration()
        self.mock_chime_sdk_messaging.create_channel.return_value = {
            "Channel": {
                "ChannelArn": "arn:aws:chime:...channel/c-123",
                "Name": "private-channel",
                "ChannelType": "STANDARD",
                "Mode": "RESTRICTED"
            }
        }

        result = integration.create_channel(
            chime_app_instance_arn="arn:aws:chime:...app-instance/ai-123",
            channel_name="private-channel",
            privacy="PRIVATE"
        )

        call_kwargs = self.mock_chime_sdk_messaging.create_channel.call_args[1]
        self.assertEqual(call_kwargs["Privacy"], "PRIVATE")

    def test_list_channels(self):
        """Test listing channels"""
        integration = ChimeIntegration()
        self.mock_chime_sdk_messaging.list_channels.return_value = {
            "Channels": [
                {
                    "ChannelArn": "arn:aws:chime:...channel/1",
                    "Name": "channel-1",
                    "ChannelType": "STANDARD",
                    "Mode": "UNRESTRICTED"
                },
                {
                    "ChannelArn": "arn:aws:chime:...channel/2",
                    "Name": "channel-2",
                    "ChannelType": "STANDARD",
                    "Mode": "UNMODERATED"
                }
            ]
        }

        result = integration.list_channels("arn:aws:chime:...app-instance/ai-123")

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].channel_name, "channel-1")

    def test_send_message(self):
        """Test sending a message"""
        integration = ChimeIntegration()
        self.mock_chime_sdk_messaging.send_message.return_value = {
            "MessageId": "msg-123",
            "ResponseMetadata": {"Date": "2024-01-01T00:00:00Z"}
        }

        result = integration.send_message(
            channel_arn="arn:aws:chime:...channel/c-123",
            content="Hello everyone!",
            sender_arn="arn:aws:chime:...user/u-123"
        )

        self.assertEqual(result["message_id"], "msg-123")

    def test_list_messages(self):
        """Test listing messages"""
        integration = ChimeIntegration()
        self.mock_chime_sdk_messaging.list_messages.return_value = {
            "ChannelMessages": [
                {
                    "MessageId": "msg-1",
                    "Content": "Hello",
                    "Sender": {"Arn": "arn:aws:chime:...user/u-1"},
                    "CreatedTimestamp": "2024-01-01T00:00:00Z",
                    "Type": "STANDARD"
                },
                {
                    "MessageId": "msg-2",
                    "Content": "Hi there",
                    "Sender": {"Arn": "arn:aws:chime:...user/u-2"},
                    "CreatedTimestamp": "2024-01-01T00:01:00Z",
                    "Type": "STANDARD"
                }
            ]
        }

        result = integration.list_messages("arn:aws:chime:...channel/c-123")

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["message_id"], "msg-1")

    # =========================================================================
    # Bot Integration Tests
    # =========================================================================

    def test_create_bot(self):
        """Test creating a bot"""
        integration = ChimeIntegration()
        self.mock_chime_client.create_bot.return_value = {
            "Bot": {
                "BotId": "bot-123",
                "BotArn": "arn:aws:chime:...bot/b-123",
                "DisplayName": "Test Bot",
                "Disabled": False,
                "CreatedTimestamp": "2024-01-01T00:00:00Z"
            }
        }

        result = integration.create_bot(
            chime_app_instance_arn="arn:aws:chime:...app-instance/ai-123",
            display_name="Test Bot"
        )

        self.assertEqual(result["bot_id"], "bot-123")
        self.assertEqual(result["display_name"], "Test Bot")

    def test_list_bots(self):
        """Test listing bots"""
        integration = ChimeIntegration()
        self.mock_chime_client.list_bots.return_value = {
            "Bot": [
                {
                    "BotId": "bot-1",
                    "BotArn": "arn:aws:chime:...bot/1",
                    "DisplayName": "Bot 1",
                    "Disabled": False
                },
                {
                    "BotId": "bot-2",
                    "BotArn": "arn:aws:chime:...bot/2",
                    "DisplayName": "Bot 2",
                    "Disabled": True
                }
            ]
        }

        result = integration.list_bots("arn:aws:chime:...app-instance/ai-123")

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["bot_id"], "bot-1")

    def test_update_bot(self):
        """Test updating a bot"""
        integration = ChimeIntegration()
        self.mock_chime_client.update_bot.return_value = {}

        result = integration.update_bot("bot-123", disabled=True)

        self.assertTrue(result)

    def test_delete_bot(self):
        """Test deleting a bot"""
        integration = ChimeIntegration()
        self.mock_chime_client.delete_bot.return_value = {}

        result = integration.delete_bot("bot-123")

        self.assertTrue(result)

    def test_get_bot_presence(self):
        """Test getting bot presence"""
        integration = ChimeIntegration()
        self.mock_chime_client.get_bot.return_value = {
            "Bot": {
                "BotId": "bot-123",
                "DisplayName": "Test Bot",
                "Disabled": False
            }
        }

        result = integration.get_bot_presence("bot-123")

        self.assertEqual(result["bot_id"], "bot-123")
        self.assertEqual(result["status"], "enabled")

    # =========================================================================
    # Voice Connector Tests
    # =========================================================================

    def test_create_voice_connector(self):
        """Test creating a voice connector"""
        integration = ChimeIntegration()
        self.mock_chime_client.create_voice_connector.return_value = {
            "VoiceConnector": {
                "VoiceConnectorId": "vc-123",
                "Arn": "arn:aws:chime:...voice-connector/vc-123",
                "Name": "TestVC",
                "AwsRegion": "us-east-1",
                "RequireEncryption": True,
                "CreatedTimestamp": "2024-01-01T00:00:00Z"
            }
        }

        result = integration.create_voice_connector(
            name="TestVC",
            aws_region="us-east-1",
            require_encryption=True
        )

        self.assertEqual(result["voice_connector_id"], "vc-123")
        self.assertTrue(result["require_encryption"])

    def test_get_voice_connector(self):
        """Test getting a voice connector"""
        integration = ChimeIntegration()
        self.mock_chime_client.get_voice_connector.return_value = {
            "VoiceConnector": {
                "VoiceConnectorId": "vc-123",
                "Arn": "arn:aws:chime:...voice-connector/vc-123",
                "Name": "TestVC",
                "AwsRegion": "us-east-1",
                "RequireEncryption": True
            }
        }

        result = integration.get_voice_connector("vc-123")

        self.assertIsNotNone(result)
        self.assertEqual(result["voice_connector_id"], "vc-123")

    def test_get_voice_connector_not_found(self):
        """Test getting a non-existent voice connector"""
        integration = ChimeIntegration()
        self.mock_chime_client.get_voice_connector.side_effect = Exception("Not found")

        result = integration.get_voice_connector("nonexistent")

        self.assertIsNone(result)

    def test_list_voice_connectors(self):
        """Test listing voice connectors"""
        integration = ChimeIntegration()
        self.mock_chime_client.list_voice_connectors.return_value = {
            "VoiceConnectors": [
                {
                    "VoiceConnectorId": "vc-1",
                    "Arn": "arn:aws:chime:...voice-connector/1",
                    "Name": "VC1",
                    "AwsRegion": "us-east-1",
                    "RequireEncryption": True
                },
                {
                    "VoiceConnectorId": "vc-2",
                    "Arn": "arn:aws:chime:...voice-connector/2",
                    "Name": "VC2",
                    "AwsRegion": "us-west-2",
                    "RequireEncryption": False
                }
            ]
        }

        result = integration.list_voice_connectors()

        self.assertEqual(len(result), 2)

    def test_update_voice_connector(self):
        """Test updating a voice connector"""
        integration = ChimeIntegration()
        self.mock_chime_client.update_voice_connector.return_value = {}

        result = integration.update_voice_connector(
            voice_connector_id="vc-123",
            name="UpdatedVC",
            require_encryption=False
        )

        self.assertTrue(result)

    def test_delete_voice_connector(self):
        """Test deleting a voice connector"""
        integration = ChimeIntegration()
        self.mock_chime_client.delete_voice_connector.return_value = {}

        result = integration.delete_voice_connector("vc-123")

        self.assertTrue(result)

    def test_create_voice_connector_termination(self):
        """Test creating voice connector termination"""
        integration = ChimeIntegration()
        self.mock_chime_client.create_voice_connector_termination.return_value = {
            "Termination": {
                "CidrAllowList": ["10.0.0.0/24"],
                "CpsLimit": 10,
                "Disabled": False
            }
        }

        result = integration.create_voice_connector_termination(
            voice_connector_id="vc-123",
            cidr_allow_list=["10.0.0.0/24"],
            cps_limit=10
        )

        self.assertEqual(result["cps_limit"], 10)

    # =========================================================================
    # Media Capture Pipeline Tests
    # =========================================================================

    def test_create_media_capture_pipeline(self):
        """Test creating a media capture pipeline"""
        integration = ChimeIntegration()
        mock_sts = MagicMock()
        mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}
        self.mock_session.client.side_effect = lambda service: mock_sts if service == "sts" else self.mock_chime_client

        self.mock_chime_client.create_media_capture_pipeline.return_value = {
            "MediaCapturePipeline": {
                "MediaPipelineId": "pipeline-123",
                "MediaPipelineArn": "arn:aws:chime:...pipeline/p-123",
                "Status": "Initializing",
                "CreatedTimestamp": "2024-01-01T00:00:00Z"
            }
        }

        result = integration.create_media_capture_pipeline(
            meeting_id="meeting-123",
            s3_bucket_arn="arn:aws:s3:::bucket-name"
        )

        self.assertEqual(result["pipeline_id"], "pipeline-123")

    def test_get_media_capture_pipeline(self):
        """Test getting a media capture pipeline"""
        integration = ChimeIntegration()
        self.mock_chime_client.get_media_capture_pipeline.return_value = {
            "MediaCapturePipeline": {
                "MediaPipelineId": "pipeline-123",
                "MediaPipelineArn": "arn:aws:chime:...pipeline/p-123",
                "Status": "Active",
                "CreatedTimestamp": "2024-01-01T00:00:00Z"
            }
        }

        result = integration.get_media_capture_pipeline("pipeline-123")

        self.assertIsNotNone(result)
        self.assertEqual(result["pipeline_id"], "pipeline-123")

    def test_list_media_capture_pipelines(self):
        """Test listing media capture pipelines"""
        integration = ChimeIntegration()
        self.mock_chime_client.list_media_capture_pipelines.return_value = {
            "MediaCapturePipelines": [
                {
                    "MediaPipelineId": "pipeline-1",
                    "MediaPipelineArn": "arn:aws:chime:...pipeline/1",
                    "Status": "Active"
                }
            ]
        }

        result = integration.list_media_capture_pipelines()

        self.assertEqual(len(result), 1)

    def test_delete_media_capture_pipeline(self):
        """Test deleting a media capture pipeline"""
        integration = ChimeIntegration()
        self.mock_chime_client.delete_media_capture_pipeline.return_value = {}

        result = integration.delete_media_capture_pipeline("pipeline-123")

        self.assertTrue(result)

    # =========================================================================
    # Media Insights Configuration Tests
    # =========================================================================

    def test_create_media_insights_configuration(self):
        """Test creating media insights configuration"""
        integration = ChimeIntegration()
        self.mock_chime_client.create_media_insights_configuration.return_value = {
            "MediaInsightsConfiguration": {
                "MediaInsightsConfigurationArn": "arn:aws:chime:...mi/c-123",
                "MediaInsightsType": "VoiceAnalytics",
                "PostCallAnalysisSettings": {"Enabled": True}
            }
        }

        result = integration.create_media_insights_configuration(
            name="test-config",
            media_insights_type="VoiceAnalytics",
            post_call_analysis_enabled=True
        )

        self.assertEqual(result["media_insights_type"], "VoiceAnalytics")
        self.assertTrue(result["post_call_analysis_enabled"])

    def test_get_media_insights_configuration(self):
        """Test getting media insights configuration"""
        integration = ChimeIntegration()
        self.mock_chime_client.get_media_insights_configuration.return_value = {
            "MediaInsightsConfiguration": {
                "MediaInsightsConfigurationArn": "arn:aws:chime:...mi/c-123",
                "MediaInsightsType": "VoiceAnalytics"
            }
        }

        result = integration.get_media_insights_configuration(
            "arn:aws:chime:...mi/c-123"
        )

        self.assertIsNotNone(result)
        self.assertEqual(result["media_insights_type"], "VoiceAnalytics")

    # =========================================================================
    # Chat Room Tests
    # =========================================================================

    def test_create_chat_room(self):
        """Test creating a chat room"""
        integration = ChimeIntegration()
        self.mock_chime_sdk_messaging.create_channel.return_value = {
            "Channel": {
                "ChannelArn": "arn:aws:chime:...channel/room-123",
                "Name": "test-room",
                "ChannelType": "STANDARD",
                "Mode": "UNRESTRICTED"
            }
        }

        result = integration.create_channel(
            chime_app_instance_arn="arn:aws:chime:...app-instance/ai-123",
            channel_name="test-room"
        )

        self.assertEqual(result.channel_name, "test-room")

    def test_list_chat_rooms(self):
        """Test listing chat rooms"""
        integration = ChimeIntegration()
        self.mock_chime_sdk_messaging.list_channels.return_value = {
            "Channels": [
                {
                    "ChannelArn": "arn:aws:chime:...channel/room-1",
                    "Name": "room-1",
                    "ChannelType": "STANDARD",
                    "Mode": "UNRESTRICTED"
                }
            ]
        }

        result = integration.list_channels("arn:aws:chime:...app-instance/ai-123")

        self.assertGreater(len(result), 0)

    # =========================================================================
    # Team Tests
    # =========================================================================

    def test_create_team(self):
        """Test creating a team"""
        integration = ChimeIntegration()
        self.mock_chime_client.create_team.return_value = {
            "Team": {
                "TeamId": "team-123",
                "Arn": "arn:aws:chime:...team/t-123",
                "Name": "Test Team",
                "CreatedTimestamp": "2024-01-01T00:00:00Z"
            }
        }

        result = integration.create_team(
            chime_app_instance_arn="arn:aws:chime:...app-instance/ai-123",
            name="Test Team"
        )

        self.assertEqual(result["team_id"], "team-123")
        self.assertEqual(result["name"], "Test Team")

    def test_list_teams(self):
        """Test listing teams"""
        integration = ChimeIntegration()
        self.mock_chime_client.list_teams.return_value = {
            "Team": [
                {
                    "TeamId": "team-1",
                    "Arn": "arn:aws:chime:...team/1",
                    "Name": "Team 1"
                },
                {
                    "TeamId": "team-2",
                    "Arn": "arn:aws:chime:...team/2",
                    "Name": "Team 2"
                }
            ]
        }

        result = integration.list_teams(
            chime_app_instance_arn="arn:aws:chime:...app-instance/ai-123"
        )

        self.assertEqual(len(result), 2)

    # =========================================================================
    # CloudWatch Metrics Tests
    # =========================================================================

    def test_put_metric_data(self):
        """Test putting CloudWatch metric data"""
        integration = ChimeIntegration()
        self.mock_cloudwatch_client.put_metric_data.return_value = {}

        result = integration.put_meeting_metric(
            meeting_id="meeting-123",
            metric_name="TestMetric",
            value=1.0,
            unit="Count"
        )

        self.assertTrue(result)
        self.mock_cloudwatch_client.put_metric_data.assert_called_once()


if __name__ == "__main__":
    unittest.main()
