"""
AWS Chime SDK Integration

Provides comprehensive integration with AWS Chime services including:
- Meeting management (create/manage meetings)
- Attendee management (add/remove attendees)
- Channel messaging
- Bot integration
- Voice connector management
- Media capture pipeline
- Media insights configuration
- Chat room management
- Team management
- CloudWatch integration for metrics
"""

import boto3
import json
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from botocore.exceptions import ClientError


@dataclass
class MeetingInfo:
    """Stores meeting details."""
    meeting_id: str
    meeting_arn: str
    external_meeting_id: Optional[str] = None
    media_region: str = "us-east-1"
    created_at: Optional[str] = None
    meeting_host_id: Optional[str] = None


@dataclass
class AttendeeInfo:
    """Stores attendee details."""
    attendee_id: str
    attendee_arn: str
    external_user_id: Optional[str] = None
    join_token: Optional[str] = None


@dataclass
class ChannelInfo:
    """Stores channel details."""
    channel_arn: str
    channel_name: str
    channel_type: str = "STANDARD"
    moderation_mode: str = "UNMODERATED"


class ChimeIntegration:
    """
    AWS Chime SDK Integration class.

    Provides methods for managing meetings, attendees, messaging,
    bots, voice connectors, media capture, insights, chat rooms,
    teams, and CloudWatch metrics.
    """

    def __init__(
        self,
        region_name: str = "us-east-1",
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
    ):
        """
        Initialize the Chime integration.

        Args:
            region_name: AWS region for Chime services
            aws_access_key_id: AWS access key (uses env if not provided)
            aws_secret_access_key: AWS secret key (uses env if not provided)
            aws_session_token: AWS session token (uses env if not provided)
        """
        self.region_name = region_name

        # Initialize AWS clients
        session_kwargs = {
            "region_name": region_name,
        }
        if aws_access_key_id:
            session_kwargs["aws_access_key_id"] = aws_access_key_id
        if aws_secret_access_key:
            session_kwargs["aws_secret_access_key"] = aws_secret_access_key
        if aws_session_token:
            session_kwargs["aws_session_token"] = aws_session_token

        self.session = boto3.Session(**session_kwargs)

        # Chime SDK clients
        self.chime_client = self.session.client("chime", region_name=region_name)
        self.chime_sdk_meetings = self.session.client("chime-sdk-meetings", region_name=region_name)
        self.chime_sdk_messaging = self.session.client("chime-sdk-messaging", region_name=region_name)
        self.cloudwatch_client = self.session.client("cloudwatch", region_name=region_name)

        # Store active meetings and attendees
        self._meetings: Dict[str, MeetingInfo] = {}
        self._attendees: Dict[str, List[AttendeeInfo]] = {}

    # =========================================================================
    # MEETING MANAGEMENT
    # =========================================================================

    def create_meeting(
        self,
        external_meeting_id: Optional[str] = None,
        media_region: str = "us-east-1",
        client_request_token: Optional[str] = None,
        notifications_config: Optional[Dict] = None,
    ) -> MeetingInfo:
        """
        Create a new Amazon Chime SDK meeting.

        Args:
            external_meeting_id: External identifier for the meeting
            media_region: AWS region for media placement
            client_request_token: Unique token for idempotent requests
            notifications_config: SNS notification configuration

        Returns:
            MeetingInfo object with meeting details
        """
        if client_request_token is None:
            client_request_token = str(uuid.uuid4())

        kwargs = {
            "ClientRequestToken": client_request_token,
            "MediaRegion": media_region,
        }

        if external_meeting_id:
            kwargs["ExternalMeetingId"] = external_meeting_id

        if notifications_config:
            kwargs["NotificationsConfiguration"] = notifications_config

        try:
            response = self.chime_sdk_meetings.create_meeting(**kwargs)
            meeting = response["Meeting"]

            meeting_info = MeetingInfo(
                meeting_id=meeting["MeetingId"],
                meeting_arn=meeting["MeetingArn"],
                external_meeting_id=external_meeting_id,
                media_region=media_region,
                created_at=meeting.get("CreatedTimestamp"),
            )

            self._meetings[meeting_info.meeting_id] = meeting_info
            return meeting_info

        except ClientError as e:
            raise Exception(f"Failed to create meeting: {e}")

    def get_meeting(self, meeting_id: str) -> Optional[MeetingInfo]:
        """
        Get details of an existing meeting.

        Args:
            meeting_id: The Chime meeting ID

        Returns:
            MeetingInfo if found, None otherwise
        """
        # Check local cache first
        if meeting_id in self._meetings:
            return self._meetings[meeting_id]

        try:
            response = self.chime_sdk_meetings.get_meeting(MeetingId=meeting_id)
            meeting = response["Meeting"]

            meeting_info = MeetingInfo(
                meeting_id=meeting["MeetingId"],
                meeting_arn=meeting["MeetingArn"],
                media_region=meeting.get("MediaRegion"),
                created_at=meeting.get("CreatedTimestamp"),
            )

            self._meetings[meeting_id] = meeting_info
            return meeting_info

        except ClientError:
            return None

    def delete_meeting(self, meeting_id: str) -> bool:
        """
        Delete a meeting.

        Args:
            meeting_id: The Chime meeting ID

        Returns:
            True if successful, False otherwise
        """
        try:
            self.chime_sdk_meetings.delete_meeting(MeetingId=meeting_id)
            if meeting_id in self._meetings:
                del self._meetings[meeting_id]
            if meeting_id in self._attendees:
                del self._attendees[meeting_id]
            return True
        except ClientError:
            return False

    def list_meetings(self, max_results: int = 50) -> List[MeetingInfo]:
        """
        List all meetings.

        Args:
            max_results: Maximum number of results to return

        Returns:
            List of MeetingInfo objects
        """
        try:
            response = self.chime_sdk_meetings.list_meetings(MaxResults=max_results)
            meetings = []

            for meeting in response.get("Meetings", []):
                meeting_info = MeetingInfo(
                    meeting_id=meeting["MeetingId"],
                    meeting_arn=meeting["MeetingArn"],
                    external_meeting_id=meeting.get("ExternalMeetingId"),
                    media_region=meeting.get("MediaRegion"),
                    created_at=meeting.get("CreatedTimestamp"),
                )
                meetings.append(meeting_info)
                self._meetings[meeting_info.meeting_id] = meeting_info

            return meetings

        except ClientError as e:
            raise Exception(f"Failed to list meetings: {e}")

    # =========================================================================
    # ATTENDEE MANAGEMENT
    # =========================================================================

    def create_attendee(
        self,
        meeting_id: str,
        external_user_id: Optional[str] = None,
    ) -> AttendeeInfo:
        """
        Add an attendee to a meeting.

        Args:
            meeting_id: The Chime meeting ID
            external_user_id: External identifier for the attendee

        Returns:
            AttendeeInfo object with attendee details and join token
        """
        kwargs = {
            "MeetingId": meeting_id,
        }

        if external_user_id:
            kwargs["ExternalUserId"] = external_user_id

        try:
            response = self.chime_sdk_meetings.create_attendee(**kwargs)
            attendee = response["Attendee"]

            attendee_info = AttendeeInfo(
                attendee_id=attendee["AttendeeId"],
                attendee_arn=attendee["AttendeeArn"],
                external_user_id=external_user_id,
                join_token=attendee.get("JoinToken"),
            )

            if meeting_id not in self._attendees:
                self._attendees[meeting_id] = []
            self._attendees[meeting_id].append(attendee_info)

            return attendee_info

        except ClientError as e:
            raise Exception(f"Failed to create attendee: {e}")

    def get_attendee(self, meeting_id: str, attendee_id: str) -> Optional[AttendeeInfo]:
        """
        Get details of a meeting attendee.

        Args:
            meeting_id: The Chime meeting ID
            attendee_id: The attendee ID

        Returns:
            AttendeeInfo if found, None otherwise
        """
        try:
            response = self.chime_sdk_meetings.get_attendee(
                MeetingId=meeting_id,
                AttendeeId=attendee_id,
            )
            attendee = response["Attendee"]

            return AttendeeInfo(
                attendee_id=attendee["AttendeeId"],
                attendee_arn=attendee["AttendeeArn"],
                external_user_id=attendee.get("ExternalUserId"),
            )

        except ClientError:
            return None

    def list_attendees(self, meeting_id: str) -> List[AttendeeInfo]:
        """
        List all attendees in a meeting.

        Args:
            meeting_id: The Chime meeting ID

        Returns:
            List of AttendeeInfo objects
        """
        try:
            response = self.chime_sdk_meetings.list_attendees(
                MeetingId=meeting_id,
                MaxResults=100,
            )

            attendees = []
            for attendee in response.get("Attendees", []):
                attendee_info = AttendeeInfo(
                    attendee_id=attendee["AttendeeId"],
                    attendee_arn=attendee["AttendeeArn"],
                    external_user_id=attendee.get("ExternalUserId"),
                )
                attendees.append(attendee_info)

            self._attendees[meeting_id] = attendees
            return attendees

        except ClientError as e:
            raise Exception(f"Failed to list attendees: {e}")

    def delete_attendee(self, meeting_id: str, attendee_id: str) -> bool:
        """
        Remove an attendee from a meeting.

        Args:
            meeting_id: The Chime meeting ID
            attendee_id: The attendee ID

        Returns:
            True if successful, False otherwise
        """
        try:
            self.chime_sdk_meetings.delete_attendee(
                MeetingId=meeting_id,
                AttendeeId=attendee_id,
            )

            if meeting_id in self._attendees:
                self._attendees[meeting_id] = [
                    a for a in self._attendees[meeting_id]
                    if a.attendee_id != attendee_id
                ]

            return True
        except ClientError:
            return False

    # =========================================================================
    # MESSAGING (CHANNEL MESSAGING)
    # =========================================================================

    def create_channel(
        self,
        chime_app_instance_arn: str,
        channel_name: str,
        channel_type: str = "STANDARD",
        mode: str = "UNRESTRICTED",
        privacy: str = "PUBLIC",
    ) -> ChannelInfo:
        """
        Create a channel for messaging.

        Args:
            chime_app_instance_arn: ARN of the Chime app instance
            channel_name: Name of the channel
            channel_type: Type of channel (STANDARD or TRUSTED)
            mode: Channel mode (UNRESTRICTED, RESTRICTED, etc.)
            privacy: Channel privacy (PUBLIC or PRIVATE)

        Returns:
            ChannelInfo object with channel details
        """
        try:
            response = self.chime_sdk_messaging.create_channel(
                AppInstanceArn=chime_app_instance_arn,
                Name=channel_name,
                ChannelType=channel_type,
                Mode=mode,
                Privacy=privacy,
            )

            channel = response["Channel"]

            return ChannelInfo(
                channel_arn=channel["ChannelArn"],
                channel_name=channel["Name"],
                channel_type=channel.get("ChannelType", channel_type),
                moderation_mode=channel.get("Mode", mode),
            )

        except ClientError as e:
            raise Exception(f"Failed to create channel: {e}")

    def list_channels(self, chime_app_instance_arn: str) -> List[ChannelInfo]:
        """
        List channels in an app instance.

        Args:
            chime_app_instance_arn: ARN of the Chime app instance

        Returns:
            List of ChannelInfo objects
        """
        try:
            response = self.chime_sdk_messaging.list_channels(
                AppInstanceArn=chime_app_instance_arn,
            )

            channels = []
            for channel in response.get("Channels", []):
                channel_info = ChannelInfo(
                    channel_arn=channel["ChannelArn"],
                    channel_name=channel["Name"],
                    channel_type=channel.get("ChannelType", "STANDARD"),
                    moderation_mode=channel.get("Mode", "UNRESTRICTED"),
                )
                channels.append(channel_info)

            return channels

        except ClientError as e:
            raise Exception(f"Failed to list channels: {e}")

    def send_message(
        self,
        channel_arn: str,
        content: str,
        sender_arn: str,
        persistence: str = "PERSISTENT",
        type_: str = "STANDARD",
    ) -> Dict:
        """
        Send a message to a channel.

        Args:
            channel_arn: ARN of the channel
            content: Message content
            sender_arn: ARN of the sender
            persistence: Message persistence (PERSISTENT or SURROGATE)
            type_: Message type (STANDARD or CONTROL)

        Returns:
            Message metadata including message ID
        """
        try:
            response = self.chime_sdk_messaging.send_message(
                ChannelArn=channel_arn,
                Content=content,
                SenderArn=sender_arn,
                Persistence=persistence,
                Type=type_,
            )

            return {
                "message_id": response["MessageId"],
                "channel_arn": channel_arn,
                "timestamp": response.get("ResponseMetadata", {}).get("Date"),
            }

        except ClientError as e:
            raise Exception(f"Failed to send message: {e}")

    def list_messages(self, channel_arn: str, max_results: int = 50) -> List[Dict]:
        """
        List messages in a channel.

        Args:
            channel_arn: ARN of the channel
            max_results: Maximum number of messages to return

        Returns:
            List of message objects
        """
        try:
            response = self.chime_sdk_messaging.list_messages(
                ChannelArn=channel_arn,
                MaxResults=max_results,
            )

            messages = []
            for msg in response.get("ChannelMessages", []):
                messages.append({
                    "message_id": msg["MessageId"],
                    "content": msg.get("Content"),
                    "sender": msg.get("Sender", {}).get("Arn"),
                    "timestamp": msg.get("CreatedTimestamp"),
                    "type": msg.get("Type"),
                })

            return messages

        except ClientError as e:
            raise Exception(f"Failed to list messages: {e}")

    # =========================================================================
    # BOT INTEGRATION
    # =========================================================================

    def create_bot(
        self,
        chime_app_instance_arn: str,
        display_name: str,
        domain: Optional[str] = None,
    ) -> Dict:
        """
        Create a bot for channel integration.

        Args:
            chime_app_instance_arn: ARN of the Chime app instance
            display_name: Display name for the bot
            domain: Domain for the bot

        Returns:
            Bot details including bot ARN and ID
        """
        try:
            kwargs = {
                "AppInstanceArn": chime_app_instance_arn,
                "DisplayName": display_name,
            }

            if domain:
                kwargs["Domain"] = domain

            response = self.chime_client.create_bot(**kwargs)
            bot = response["Bot"]

            return {
                "bot_id": bot["BotId"],
                "bot_arn": bot["BotArn"],
                "display_name": bot["DisplayName"],
                "disabled": bot.get("Disabled", False),
                "created_at": bot.get("CreatedTimestamp"),
            }

        except ClientError as e:
            raise Exception(f"Failed to create bot: {e}")

    def list_bots(self, chime_app_instance_arn: str) -> List[Dict]:
        """
        List bots in an app instance.

        Args:
            chime_app_instance_arn: ARN of the Chime app instance

        Returns:
            List of bot objects
        """
        try:
            response = self.chime_client.list_bots(
                AppInstanceArn=chime_app_instance_arn,
            )

            bots = []
            for bot in response.get("Bot", []):
                bots.append({
                    "bot_id": bot["BotId"],
                    "bot_arn": bot["BotArn"],
                    "display_name": bot["DisplayName"],
                    "disabled": bot.get("Disabled", False),
                })

            return bots

        except ClientError as e:
            raise Exception(f"Failed to list bots: {e}")

    def update_bot(self, bot_id: str, disabled: bool = False) -> bool:
        """
        Update a bot's status.

        Args:
            bot_id: The bot ID
            disabled: Whether the bot is disabled

        Returns:
            True if successful, False otherwise
        """
        try:
            self.chime_client.update_bot(
                BotId=bot_id,
                Disabled=disabled,
            )
            return True
        except ClientError:
            return False

    def delete_bot(self, bot_id: str) -> bool:
        """
        Delete a bot.

        Args:
            bot_id: The bot ID

        Returns:
            True if successful, False otherwise
        """
        try:
            self.chime_client.delete_bot(BotId=bot_id)
            return True
        except ClientError:
            return False

    def get_bot_presence(self, bot_id: str) -> Dict:
        """
        Get bot presence information.

        Args:
            bot_id: The bot ID

        Returns:
            Presence information
        """
        try:
            response = self.chime_client.get_bot(BotId=bot_id)
            bot = response["Bot"]

            return {
                "bot_id": bot["BotId"],
                "display_name": bot["DisplayName"],
                "disabled": bot.get("Disabled", False),
                "status": "disabled" if bot.get("Disabled") else "enabled",
            }

        except ClientError as e:
            raise Exception(f"Failed to get bot presence: {e}")

    # =========================================================================
    # VOICE CONNECTOR MANAGEMENT
    # =========================================================================

    def create_voice_connector(
        self,
        name: str,
        aws_region: str = "us-east-1",
        require_encryption: bool = True,
    ) -> Dict:
        """
        Create a Voice Connector for PSTN audio.

        Args:
            name: Name of the Voice Connector
            aws_region: AWS region for the Voice Connector
            require_encryption: Whether to require encryption

        Returns:
            Voice Connector details
        """
        try:
            response = self.chime_client.create_voice_connector(
                Name=name,
                AwsRegion=aws_region,
                RequireEncryption=require_encryption,
            )

            vc = response["VoiceConnector"]

            return {
                "voice_connector_id": vc["VoiceConnectorId"],
                "arn": vc["Arn"],
                "name": vc["Name"],
                "aws_region": vc["AwsRegion"],
                "require_encryption": vc["RequireEncryption"],
                "created_at": vc.get("CreatedTimestamp"),
            }

        except ClientError as e:
            raise Exception(f"Failed to create voice connector: {e}")

    def get_voice_connector(self, voice_connector_id: str) -> Optional[Dict]:
        """
        Get Voice Connector details.

        Args:
            voice_connector_id: The Voice Connector ID

        Returns:
            Voice Connector details if found, None otherwise
        """
        try:
            response = self.chime_client.get_voice_connector(
                VoiceConnectorId=voice_connector_id,
            )

            vc = response["VoiceConnector"]

            return {
                "voice_connector_id": vc["VoiceConnectorId"],
                "arn": vc["Arn"],
                "name": vc["Name"],
                "aws_region": vc["AwsRegion"],
                "require_encryption": vc["RequireEncryption"],
                "created_at": vc.get("CreatedTimestamp"),
            }

        except ClientError:
            return None

    def list_voice_connectors(self) -> List[Dict]:
        """
        List all Voice Connectors.

        Returns:
            List of Voice Connector details
        """
        try:
            response = self.chime_client.list_voice_connectors()

            connectors = []
            for vc in response.get("VoiceConnectors", []):
                connectors.append({
                    "voice_connector_id": vc["VoiceConnectorId"],
                    "arn": vc["Arn"],
                    "name": vc["Name"],
                    "aws_region": vc["AwsRegion"],
                    "require_encryption": vc["RequireEncryption"],
                })

            return connectors

        except ClientError as e:
            raise Exception(f"Failed to list voice connectors: {e}")

    def update_voice_connector(
        self,
        voice_connector_id: str,
        name: Optional[str] = None,
        require_encryption: Optional[bool] = None,
    ) -> bool:
        """
        Update a Voice Connector.

        Args:
            voice_connector_id: The Voice Connector ID
            name: New name for the Voice Connector
            require_encryption: Whether to require encryption

        Returns:
            True if successful, False otherwise
        """
        try:
            kwargs = {"VoiceConnectorId": voice_connector_id}

            if name is not None:
                kwargs["Name"] = name
            if require_encryption is not None:
                kwargs["RequireEncryption"] = require_encryption

            self.chime_client.update_voice_connector(**kwargs)
            return True

        except ClientError:
            return False

    def delete_voice_connector(self, voice_connector_id: str) -> bool:
        """
        Delete a Voice Connector.

        Args:
            voice_connector_id: The Voice Connector ID

        Returns:
            True if successful, False otherwise
        """
        try:
            self.chime_client.delete_voice_connector(
                VoiceConnectorId=voice_connector_id,
            )
            return True
        except ClientError:
            return False

    def create_voice_connector_termination(
        self,
        voice_connector_id: str,
        cidr_allow_list: List[str],
        cps_limit: int = 1,
        disabled: bool = False,
    ) -> Dict:
        """
        Create termination settings for a Voice Connector.

        Args:
            voice_connector_id: The Voice Connector ID
            cidr_allow_list: List of allowed CIDR blocks
            cps_limit: Calls per second limit
            disabled: Whether termination is disabled

        Returns:
            Termination settings
        """
        try:
            response = self.chime_client.create_voice_connector_termination(
                VoiceConnectorId=voice_connector_id,
                CidrAllowList=cidr_allow_list,
                CpsLimit=cps_limit,
                Disabled=disabled,
            )

            termination = response["Termination"]

            return {
                "cidr_allow_list": termination["CidrAllowList"],
                "cps_limit": termination["CpsLimit"],
                "disabled": termination["Disabled"],
            }

        except ClientError as e:
            raise Exception(f"Failed to create voice connector termination: {e}")

    # =========================================================================
    # MEDIA CAPTURE PIPELINE
    # =========================================================================

    def create_media_capture_pipeline(
        self,
        meeting_id: str,
        s3_bucket_arn: str,
        source_type: str = "ChimeSdkMeeting",
        capture_type: str = "AudioWithSip",
        enabled: bool = True,
    ) -> Dict:
        """
        Create a media capture pipeline.

        Args:
            meeting_id: The Chime meeting ID to capture
            s3_bucket_arn: ARN of the S3 bucket for storage
            source_type: Source type for capture
            capture_type: Type of media to capture
            enabled: Whether the pipeline is enabled

        Returns:
            Media capture pipeline details
        """
        try:
            response = self.chime_client.create_media_capture_pipeline(
                SourceType=source_type,
                SourceResourceArn=f"arn:aws:chime::{self.session.client('sts').get_caller_identity()['Account']}:meeting/{meeting_id}",
                SinkType="S3Bucket",
                SinkArn=s3_bucket_arn,
                ClientRequestToken=str(uuid.uuid4()),
                Enabled=enabled,
            )

            pipeline = response["MediaCapturePipeline"]

            return {
                "pipeline_id": pipeline["MediaPipelineId"],
                "pipeline_arn": pipeline["MediaPipelineArn"],
                "meeting_id": meeting_id,
                "s3_bucket_arn": s3_bucket_arn,
                "status": pipeline.get("Status", "Initializing"),
                "created_at": pipeline.get("CreatedTimestamp"),
            }

        except ClientError as e:
            raise Exception(f"Failed to create media capture pipeline: {e}")

    def get_media_capture_pipeline(self, pipeline_id: str) -> Optional[Dict]:
        """
        Get media capture pipeline details.

        Args:
            pipeline_id: The pipeline ID

        Returns:
            Pipeline details if found, None otherwise
        """
        try:
            response = self.chime_client.get_media_capture_pipeline(
                MediaPipelineId=pipeline_id,
            )

            pipeline = response["MediaCapturePipeline"]

            return {
                "pipeline_id": pipeline["MediaPipelineId"],
                "pipeline_arn": pipeline["MediaPipelineArn"],
                "status": pipeline.get("Status"),
                "created_at": pipeline.get("CreatedTimestamp"),
            }

        except ClientError:
            return None

    def list_media_capture_pipelines(self) -> List[Dict]:
        """
        List all media capture pipelines.

        Returns:
            List of media capture pipeline details
        """
        try:
            response = self.chime_client.list_media_capture_pipelines()

            pipelines = []
            for pipeline in response.get("MediaCapturePipelines", []):
                pipelines.append({
                    "pipeline_id": pipeline["MediaPipelineId"],
                    "pipeline_arn": pipeline["MediaPipelineArn"],
                    "status": pipeline.get("Status"),
                    "created_at": pipeline.get("CreatedTimestamp"),
                })

            return pipelines

        except ClientError as e:
            raise Exception(f"Failed to list media capture pipelines: {e}")

    def delete_media_capture_pipeline(self, pipeline_id: str) -> bool:
        """
        Delete a media capture pipeline.

        Args:
            pipeline_id: The pipeline ID

        Returns:
            True if successful, False otherwise
        """
        try:
            self.chime_client.delete_media_capture_pipeline(
                MediaPipelineId=pipeline_id,
            )
            return True
        except ClientError:
            return False

    # =========================================================================
    # MEDIA INSIGHTS CONFIGURATION
    # =========================================================================

    def create_media_insights_configuration(
        self,
        name: str,
        media_insights_type: str = "VoiceAnalytics",
        post_call_analysis_enabled: bool = False,
    ) -> Dict:
        """
        Create a media insights configuration.

        Args:
            name: Name of the configuration
            media_insights_type: Type of media insights (VoiceAnalytics, etc.)
            post_call_analysis_enabled: Enable post-call analysis

        Returns:
            Media insights configuration details
        """
        try:
            configuration = {
                "MediaInsightsType": media_insights_type,
            }

            if post_call_analysis_enabled:
                configuration["PostCallAnalysisSettings"] = {
                    "Enabled": True,
                }

            response = self.chime_client.create_media_insights_configuration(
                MediaInsightsConfiguration=configuration,
            )

            config = response["MediaInsightsConfiguration"]

            return {
                "arn": config["MediaInsightsConfigurationArn"],
                "media_insights_type": config["MediaInsightsType"],
                "post_call_analysis_enabled": config.get("PostCallAnalysisSettings", {}).get("Enabled", False),
            }

        except ClientError as e:
            raise Exception(f"Failed to create media insights configuration: {e}")

    def get_media_insights_configuration(self, arn: str) -> Optional[Dict]:
        """
        Get media insights configuration details.

        Args:
            arn: The configuration ARN

        Returns:
            Configuration details if found, None otherwise
        """
        try:
            response = self.chime_client.get_media_insights_configuration(
                MediaInsightsConfigurationArn=arn,
            )

            config = response["MediaInsightsConfiguration"]

            return {
                "arn": config["MediaInsightsConfigurationArn"],
                "media_insights_type": config["MediaInsightsType"],
                "post_call_analysis_enabled": config.get("PostCallAnalysisSettings", {}).get("Enabled", False),
            }

        except ClientError:
            return None

    def list_media_insights_configurations(self) -> List[Dict]:
        """
        List all media insights configurations.

        Returns:
            List of media insights configuration details
        """
        try:
            response = self.chime_client.list_media_insights_configurations()

            configs = []
            for config in response.get("MediaInsightsConfigurations", []):
                configs.append({
                    "arn": config["MediaInsightsConfigurationArn"],
                    "media_insights_type": config["MediaInsightsType"],
                })

            return configs

        except ClientError as e:
            raise Exception(f"Failed to list media insights configurations: {e}")

    # =========================================================================
    # CHAT ROOM MANAGEMENT
    # =========================================================================

    def create_chat_room(
        self,
        chime_app_instance_arn: str,
        name: str,
        description: Optional[str] = None,
    ) -> Dict:
        """
        Create a chat room.

        Args:
            chime_app_instance_arn: ARN of the Chime app instance
            name: Name of the chat room
            description: Optional description

        Returns:
            Chat room details
        """
        try:
            kwargs = {
                "AppInstanceArn": chime_app_instance_arn,
                "Name": name,
            }

            if description:
                kwargs["Description"] = description

            response = self.chime_sdk_messaging.create_channel(
                AppInstanceArn=chime_app_instance_arn,
                Name=name,
                ChannelType="STANDARD",
                Mode="UNRESTRICTED",
                Privacy="PRIVATE" if description else "PUBLIC",
            )

            channel = response["Channel"]

            return {
                "chat_room_id": channel["ChannelArn"].split("/")[-1],
                "channel_arn": channel["ChannelArn"],
                "name": channel["Name"],
                "created_at": channel.get("CreatedTimestamp"),
            }

        except ClientError as e:
            raise Exception(f"Failed to create chat room: {e}")

    def list_chat_rooms(self, chime_app_instance_arn: str) -> List[Dict]:
        """
        List chat rooms in an app instance.

        Args:
            chime_app_instance_arn: ARN of the Chime app instance

        Returns:
            List of chat room details
        """
        try:
            response = self.chime_sdk_messaging.list_channels(
                AppInstanceArn=chime_app_instance_arn,
                ChannelType="STANDARD",
            )

            rooms = []
            for channel in response.get("Channels", []):
                rooms.append({
                    "channel_arn": channel["ChannelArn"],
                    "name": channel["Name"],
                    "mode": channel.get("Mode"),
                })

            return rooms

        except ClientError as e:
            raise Exception(f"Failed to list chat rooms: {e}")

    def update_chat_room(
        self,
        channel_arn: str,
        name: Optional[str] = None,
        mode: Optional[str] = None,
    ) -> bool:
        """
        Update a chat room.

        Args:
            channel_arn: ARN of the channel
            name: New name for the chat room
            mode: New mode for the chat room

        Returns:
            True if successful, False otherwise
        """
        try:
            kwargs = {"ChannelArn": channel_arn}

            if name:
                kwargs["Name"] = name
            if mode:
                kwargs["Mode"] = mode

            self.chime_sdk_messaging.update_channel(**kwargs)
            return True

        except ClientError:
            return False

    def delete_chat_room(self, channel_arn: str) -> bool:
        """
        Delete a chat room.

        Args:
            channel_arn: ARN of the channel

        Returns:
            True if successful, False otherwise
        """
        try:
            self.chime_sdk_messaging.delete_channel(ChannelArn=channel_arn)
            return True
        except ClientError:
            return False

    # =========================================================================
    # TEAM MANAGEMENT
    # =========================================================================

    def create_team(
        self,
        chime_app_instance_arn: str,
        name: str,
    ) -> Dict:
        """
        Create a team.

        Args:
            chime_app_instance_arn: ARN of the Chime app instance
            name: Name of the team

        Returns:
            Team details
        """
        try:
            response = self.chime_client.create_team(
                AppInstanceArn=chime_app_instance_arn,
                Name=name,
            )

            team = response["Team"]

            return {
                "team_id": team["TeamId"],
                "arn": team["Arn"],
                "name": team["Name"],
                "created_at": team.get("CreatedTimestamp"),
            }

        except ClientError as e:
            raise Exception(f"Failed to create team: {e}")

    def list_teams(self, chime_app_instance_arn: str) -> List[Dict]:
        """
        List teams in an app instance.

        Args:
            chime_app_instance_arn: ARN of the Chime app instance

        Returns:
            List of team details
        """
        try:
            response = self.chime_client.list_teams(
                AppInstanceArn=chime_app_instance_arn,
            )

            teams = []
            for team in response.get("Team", []):
                teams.append({
                    "team_id": team["TeamId"],
                    "arn": team["Arn"],
                    "name": team["Name"],
                    "created_at": team.get("CreatedTimestamp"),
                })

            return teams

        except ClientError as e:
            raise Exception(f"Failed to list teams: {e}")

    def create_team_member(
        self,
        team_id: str,
        user_id: str,
        role: str = "Member",
    ) -> Dict:
        """
        Add a member to a team.

        Args:
            team_id: The team ID
            user_id: The user ID to add
            role: Role for the member (Member or Admin)

        Returns:
            Team membership details
        """
        try:
            response = self.chime_client.create_team_membership(
                TeamId=team_id,
                UserId=user_id,
                Role=role,
            )

            membership = response["TeamMembership"]

            return {
                "team_id": membership["TeamId"],
                "user_id": membership["UserId"],
                "role": membership["Role"],
                "status": membership.get("Status"),
            }

        except ClientError as e:
            raise Exception(f"Failed to create team member: {e}")

    def list_team_members(self, team_id: str) -> List[Dict]:
        """
        List members of a team.

        Args:
            team_id: The team ID

        Returns:
            List of team member details
        """
        try:
            response = self.chime_client.list_team_memberships(TeamId=team_id)

            members = []
            for membership in response.get("TeamMemberships", []):
                members.append({
                    "team_id": membership["TeamId"],
                    "user_id": membership["UserId"],
                    "role": membership["Role"],
                    "status": membership.get("Status"),
                })

            return members

        except ClientError as e:
            raise Exception(f"Failed to list team members: {e}")

    def delete_team_member(self, team_id: str, user_id: str) -> bool:
        """
        Remove a member from a team.

        Args:
            team_id: The team ID
            user_id: The user ID to remove

        Returns:
            True if successful, False otherwise
        """
        try:
            self.chime_client.delete_team_membership(
                TeamId=team_id,
                UserId=user_id,
            )
            return True
        except ClientError:
            return False

    def delete_team(self, team_id: str) -> bool:
        """
        Delete a team.

        Args:
            team_id: The team ID

        Returns:
            True if successful, False otherwise
        """
        try:
            self.chime_client.delete_team(TeamId=team_id)
            return True
        except ClientError:
            return False

    # =========================================================================
    # CLOUDWATCH INTEGRATION
    # =========================================================================

    def put_meeting_metric(
        self,
        meeting_id: str,
        metric_name: str,
        value: float,
        unit: str = "Count",
    ) -> bool:
        """
        Publish a meeting-related metric to CloudWatch.

        Args:
            meeting_id: The meeting ID
            metric_name: Name of the metric
            value: Metric value
            unit: CloudWatch unit (Count, Seconds, etc.)

        Returns:
            True if successful, False otherwise
        """
        try:
            self.cloudwatch_client.put_metric_data(
                Namespace="AWS/Chime",
                MetricData=[
                    {
                        "MetricName": metric_name,
                        "Dimensions": [
                            {"Name": "MeetingId", "Value": meeting_id},
                        ],
                        "Value": value,
                        "Unit": unit,
                    },
                ],
            )
            return True
        except ClientError:
            return False

    def put_messaging_metric(
        self,
        channel_arn: str,
        metric_name: str,
        value: float,
        unit: str = "Count",
    ) -> bool:
        """
        Publish a messaging-related metric to CloudWatch.

        Args:
            channel_arn: The channel ARN
            metric_name: Name of the metric
            value: Metric value
            unit: CloudWatch unit

        Returns:
            True if successful, False otherwise
        """
        try:
            self.cloudwatch_client.put_metric_data(
                Namespace="AWS/Chime",
                MetricData=[
                    {
                        "MetricName": metric_name,
                        "Dimensions": [
                            {"Name": "ChannelArn", "Value": channel_arn},
                        ],
                        "Value": value,
                        "Unit": unit,
                    },
                ],
            )
            return True
        except ClientError:
            return False

    def get_meeting_metrics(
        self,
        meeting_id: str,
        start_time: datetime,
        end_time: datetime,
        period: int = 60,
    ) -> Dict:
        """
        Get CloudWatch metrics for a meeting.

        Args:
            meeting_id: The meeting ID
            start_time: Start of the time range
            end_time: End of the time range
            period: Period in seconds

        Returns:
            CloudWatch metrics data
        """
        try:
            response = self.cloudwatch_client.get_metric_statistics(
                Namespace="AWS/Chime",
                MetricName="MeetingParticipants",
                StartTime=start_time,
                EndTime=end_time,
                Period=period,
                Statistics=["Average", "Maximum", "Minimum"],
                Dimensions=[
                    {"Name": "MeetingId", "Value": meeting_id},
                ],
            )

            return {
                "meeting_id": meeting_id,
                "data_points": response.get("Datapoints", []),
            }

        except ClientError as e:
            raise Exception(f"Failed to get meeting metrics: {e}")

    def get_messaging_metrics(
        self,
        channel_arn: str,
        start_time: datetime,
        end_time: datetime,
        period: int = 60,
    ) -> Dict:
        """
        Get CloudWatch metrics for messaging.

        Args:
            channel_arn: The channel ARN
            start_time: Start of the time range
            end_time: End of the time range
            period: Period in seconds

        Returns:
            CloudWatch metrics data
        """
        try:
            response = self.cloudwatch_client.get_metric_statistics(
                Namespace="AWS/Chime",
                MetricName="MessageCount",
                StartTime=start_time,
                EndTime=end_time,
                Period=period,
                Statistics=["Sum", "Average"],
                Dimensions=[
                    {"Name": "ChannelArn", "Value": channel_arn},
                ],
            )

            return {
                "channel_arn": channel_arn,
                "data_points": response.get("Datapoints", []),
            }

        except ClientError as e:
            raise Exception(f"Failed to get messaging metrics: {e}")

    def create_dashboard(self, dashboard_name: str) -> bool:
        """
        Create a CloudWatch dashboard for Chime metrics.

        Args:
            dashboard_name: Name of the dashboard

        Returns:
            True if successful, False otherwise
        """
        dashboard_body = {
            "widgets": [
                {
                    "type": "metric",
                    "properties": {
                        "title": "Active Meetings",
                        "metrics": [
                            ["AWS/Chime", "ActiveMeetings"],
                        ],
                        "period": 300,
                        "stat": "Average",
                    },
                },
                {
                    "type": "metric",
                    "properties": {
                        "title": "Meeting Participants",
                        "metrics": [
                            ["AWS/Chime", "MeetingParticipants"],
                        ],
                        "period": 300,
                        "stat": "Average",
                    },
                },
                {
                    "type": "metric",
                    "properties": {
                        "title": "Messages Sent",
                        "metrics": [
                            ["AWS/Chime", "MessageCount"],
                        ],
                        "period": 300,
                        "stat": "Sum",
                    },
                },
            ],
        }

        try:
            self.cloudwatch_client.put_dashboard(
                DashboardName=dashboard_name,
                DashboardBody=json.dumps(dashboard_body),
            )
            return True
        except ClientError:
            return False

    def set_alarm(
        self,
        alarm_name: str,
        metric_name: str,
        threshold: float,
        comparison_operator: str = "GreaterThanThreshold",
        period: int = 300,
        evaluation_periods: int = 1,
    ) -> bool:
        """
        Create a CloudWatch alarm for Chime metrics.

        Args:
            alarm_name: Name of the alarm
            metric_name: Metric to monitor
            threshold: Threshold value
            comparison_operator: Comparison operator
            period: Period in seconds
            evaluation_periods: Number of evaluation periods

        Returns:
            True if successful, False otherwise
        """
        try:
            self.cloudwatch_client.put_metric_alarm(
                AlarmName=alarm_name,
                Namespace="AWS/Chime",
                MetricName=metric_name,
                Threshold=threshold,
                ComparisonOperator=comparison_operator,
                Period=period,
                EvaluationPeriods=evaluation_periods,
                Statistic="Average",
            )
            return True
        except ClientError:
            return False
