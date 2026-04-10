"""
Tests for the multi-channel notification system.
"""

import sys
import unittest
from unittest.mock import patch, MagicMock, Mock, call
from datetime import datetime, time as dt_time

sys.path.insert(0, '/Users/guige/my_project')

from src.notification_system import (
    NotificationSystem,
    NotificationPriority,
    NotificationChannel,
    NotificationTemplate,
    NotificationRule,
    DoNotDisturbSchedule,
    RetryConfig
)


class TestNotificationTemplate(unittest.TestCase):
    """Tests for NotificationTemplate."""

    def test_template_render_basic(self):
        """Test basic template rendering."""
        template = NotificationTemplate(
            name="test",
            subject="Hello {name}",
            body="Welcome {name}, you have {count} messages",
            variables=["name", "count"]
        )
        result = template.render(name="Alice", count=5)

        self.assertEqual(result["subject"], "Hello Alice")
        self.assertEqual(result["body"], "Welcome Alice, you have 5 messages")

    def test_template_render_missing_variable(self):
        """Test template rendering with missing variables."""
        template = NotificationTemplate(
            name="test",
            subject="Hello {name}",
            body="Message for {name}",
            variables=["name"]
        )
        result = template.render(name="Bob")

        self.assertEqual(result["subject"], "Hello Bob")
        self.assertEqual(result["body"], "Message for Bob")


class TestDoNotDisturbSchedule(unittest.TestCase):
    """Tests for DoNotDisturbSchedule."""

    def test_dnd_disabled(self):
        """Test DND when disabled."""
        schedule = DoNotDisturbSchedule(enabled=False)
        self.assertFalse(schedule.is_active())

    @patch('src.notification_system.datetime')
    def test_dnd_active_overnight(self, mock_datetime):
        """Test overnight DND range (e.g., 22:00 to 08:00)."""
        schedule = DoNotDisturbSchedule(
            enabled=True,
            start_time="22:00",
            end_time="08:00"
        )

        # Test time at 23:00 (should be in DND)
        mock_datetime.now.return_value = datetime(2024, 1, 1, 23, 0)
        self.assertTrue(schedule.is_active())

        # Test time at 03:00 (should be in DND)
        mock_datetime.now.return_value = datetime(2024, 1, 1, 3, 0)
        self.assertTrue(schedule.is_active())

        # Test time at 12:00 (should NOT be in DND)
        mock_datetime.now.return_value = datetime(2024, 1, 1, 12, 0)
        self.assertFalse(schedule.is_active())

    @patch('src.notification_system.datetime')
    def test_dnd_active_same_day(self, mock_datetime):
        """Test same-day DND range (e.g., 08:00 to 18:00)."""
        schedule = DoNotDisturbSchedule(
            enabled=True,
            start_time="08:00",
            end_time="18:00"
        )

        # Test time at 12:00 (should be in DND)
        mock_datetime.now.return_value = datetime(2024, 1, 1, 12, 0)
        self.assertTrue(schedule.is_active())

        # Test time at 06:00 (should NOT be in DND)
        mock_datetime.now.return_value = datetime(2024, 1, 1, 6, 0)
        self.assertFalse(schedule.is_active())

        # Test time at 20:00 (should NOT be in DND)
        mock_datetime.now.return_value = datetime(2024, 1, 1, 20, 0)
        self.assertFalse(schedule.is_active())


class TestRetryConfig(unittest.TestCase):
    """Tests for RetryConfig."""

    def test_default_values(self):
        """Test default retry configuration."""
        config = RetryConfig()
        self.assertEqual(config.max_attempts, 3)
        self.assertEqual(config.base_delay, 1.0)
        self.assertEqual(config.exponential_base, 2.0)
        self.assertEqual(config.max_delay, 60.0)


class TestNotificationSystem(unittest.TestCase):
    """Tests for NotificationSystem."""

    def setUp(self):
        """Set up test fixtures."""
        self.system = NotificationSystem()

    def test_init_default_templates(self):
        """Test that default templates are initialized."""
        self.assertIn("workflow_complete", self.system.templates)
        self.assertIn("workflow_failed", self.system.templates)
        self.assertIn("workflow_started", self.system.templates)
        self.assertIn("alert", self.system.templates)

    def test_add_template(self):
        """Test adding a custom template."""
        template = NotificationTemplate(
            name="custom",
            subject="Custom: {var}",
            body="Custom body {var}",
            variables=["var"]
        )
        self.system.add_template(template)
        self.assertEqual(self.system.templates["custom"], template)

    def test_add_rule(self):
        """Test adding a notification rule."""
        rule = NotificationRule(
            name="test_rule",
            condition=lambda ctx: ctx.get("value", 0) > 10,
            channels=[NotificationChannel.EMAIL]
        )
        self.system.add_rule(rule)
        self.assertIn(rule, self.system.rules)

    def test_dnd_schedule_setting(self):
        """Test setting DND schedule."""
        schedule = DoNotDisturbSchedule(enabled=True, start_time="23:00", end_time="07:00")
        self.system.set_dnd_schedule(schedule)
        self.assertEqual(self.system.dnd_schedule, schedule)

    def test_retry_config_setting(self):
        """Test setting retry configuration."""
        config = RetryConfig(max_attempts=5, base_delay=2.0)
        self.system.set_retry_config(config)
        self.assertEqual(self.system.retry_config.max_attempts, 5)

    def test_get_channels_for_priority_high(self):
        """Test channel selection for HIGH priority."""
        channels = self.system._get_channels_for_priority(NotificationPriority.HIGH)
        self.assertIn(NotificationChannel.DESKTOP, channels)
        self.assertIn(NotificationChannel.EMAIL, channels)
        self.assertIn(NotificationChannel.TELEGRAM, channels)
        self.assertIn(NotificationChannel.SMS, channels)

    def test_get_channels_for_priority_normal(self):
        """Test channel selection for NORMAL priority."""
        channels = self.system._get_channels_for_priority(NotificationPriority.NORMAL)
        self.assertIn(NotificationChannel.DESKTOP, channels)
        self.assertIn(NotificationChannel.EMAIL, channels)
        self.assertIn(NotificationChannel.TELEGRAM, channels)
        self.assertNotIn(NotificationChannel.SMS, channels)

    def test_get_channels_for_priority_low(self):
        """Test channel selection for LOW priority."""
        channels = self.system._get_channels_for_priority(NotificationPriority.LOW)
        self.assertIn(NotificationChannel.EMAIL, channels)
        self.assertEqual(len(channels), 1)

    def test_should_deliver_no_rule(self):
        """Test delivery check with no rule."""
        result = self.system._should_deliver(None, {"key": "value"})
        self.assertTrue(result)

    def test_should_deliver_matching_rule(self):
        """Test delivery check with matching rule."""
        rule = NotificationRule(
            name="test",
            condition=lambda ctx: ctx.get("value", 0) > 5,
            channels=[NotificationChannel.EMAIL]
        )
        result = self.system._should_deliver(rule, {"value": 10})
        self.assertTrue(result)

    def test_should_deliver_non_matching_rule(self):
        """Test delivery check with non-matching rule."""
        rule = NotificationRule(
            name="test",
            condition=lambda ctx: ctx.get("value", 0) > 5,
            channels=[NotificationChannel.EMAIL]
        )
        result = self.system._should_deliver(rule, {"value": 3})
        self.assertFalse(result)

    def test_retry_delay_calculation(self):
        """Test exponential backoff delay calculation."""
        self.system.retry_config = RetryConfig(base_delay=1.0, exponential_base=2.0, max_delay=60.0)

        # Attempt 0: 1.0 * 2^0 = 1.0
        self.assertAlmostEqual(self.system._get_retry_delay(0), 1.0)
        # Attempt 1: 1.0 * 2^1 = 2.0
        self.assertAlmostEqual(self.system._get_retry_delay(1), 2.0)
        # Attempt 2: 1.0 * 2^2 = 4.0
        self.assertAlmostEqual(self.system._get_retry_delay(2), 4.0)
        # Attempt 10 would exceed max_delay
        self.assertEqual(self.system._get_retry_delay(10), 60.0)

    def test_log_delivery(self):
        """Test delivery logging."""
        self.system._log_delivery("email", "Test Subject", True)
        self.assertEqual(len(self.system.delivery_history), 1)
        self.assertEqual(self.system.delivery_history[0]["channel"], "email")
        self.assertTrue(self.system.delivery_history[0]["success"])

    def test_get_delivery_history(self):
        """Test retrieving delivery history."""
        self.system._log_delivery("email", "Test 1", True)
        self.system._log_delivery("telegram", "Test 2", False, "Connection timeout")

        history = self.system.get_delivery_history()
        self.assertEqual(len(history), 2)
        self.assertEqual(history[0]["channel"], "email")
        self.assertEqual(history[1]["channel"], "telegram")

    def test_clear_history(self):
        """Test clearing delivery history."""
        self.system._log_delivery("email", "Test", True)
        self.system.clear_history()
        self.assertEqual(len(self.system.delivery_history), 0)


class TestDesktopNotification(unittest.TestCase):
    """Tests for desktop notification functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.system = NotificationSystem()

    @patch('src.notification_system.subprocess.run')
    @patch('src.notification_system.DoNotDisturbSchedule.is_active')
    def test_send_desktop_notification_success(self, mock_dnd_active, mock_run):
        """Test successful desktop notification."""
        mock_dnd_active.return_value = False
        mock_run.return_value = MagicMock(returncode=0)

        result = self.system.send_desktop_notification("Test Title", "Test Message")

        self.assertTrue(result)
        mock_run.assert_called_once()
        # Check that osascript was called with -e flag and the script contains display notification
        call_args = mock_run.call_args[0][0]
        self.assertEqual(call_args[0], "osascript")
        self.assertEqual(call_args[1], "-e")
        self.assertIn("display notification", call_args[2])

    @patch('src.notification_system.subprocess.run')
    @patch('src.notification_system.DoNotDisturbSchedule.is_active')
    def test_send_desktop_notification_blocked_by_dnd(self, mock_dnd_active, mock_run):
        """Test desktop notification blocked by DND."""
        mock_dnd_active.return_value = True

        result = self.system.send_desktop_notification("Test Title", "Test Message")

        self.assertFalse(result)
        mock_run.assert_not_called()

    @patch('src.notification_system.subprocess.run')
    @patch('src.notification_system.DoNotDisturbSchedule.is_active')
    def test_send_desktop_notification_failure(self, mock_dnd_active, mock_run):
        """Test desktop notification failure."""
        mock_dnd_active.return_value = False
        mock_run.side_effect = Exception("Command failed")

        result = self.system.send_desktop_notification("Test Title", "Test Message")

        self.assertFalse(result)


class TestEmailNotification(unittest.TestCase):
    """Tests for email notification functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.system = NotificationSystem(config={
            "email": {
                "smtp_host": "smtp.test.com",
                "smtp_port": 587,
                "smtp_user": "user",
                "smtp_password": "pass",
                "from_addr": "from@test.com"
            }
        })

    @patch('src.notification_system.smtplib.SMTP')
    @patch('src.notification_system.DoNotDisturbSchedule.is_active')
    def test_send_email_success(self, mock_dnd_active, mock_smtp):
        """Test successful email sending."""
        mock_dnd_active.return_value = False
        mock_server = MagicMock()
        mock_smtp.return_value.__enter__.return_value = mock_server

        result = self.system.send_email("to@test.com", "Test Subject", "Test Body")

        self.assertTrue(result)
        mock_server.send_message.assert_called_once()

    @patch('src.notification_system.DoNotDisturbSchedule.is_active')
    def test_send_email_blocked_by_dnd(self, mock_dnd_active):
        """Test email blocked by DND."""
        mock_dnd_active.return_value = True

        result = self.system.send_email("to@test.com", "Test Subject", "Test Body")

        self.assertFalse(result)


class TestTelegramNotification(unittest.TestCase):
    """Tests for Telegram notification functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.system = NotificationSystem(config={
            "telegram": {
                "bot_token": "test_token",
                "chat_id": "test_chat_id"
            }
        })

    @patch('src.notification_system.urlopen')
    @patch('src.notification_system.DoNotDisturbSchedule.is_active')
    def test_send_telegram_success(self, mock_dnd_active, mock_urlopen):
        """Test successful Telegram message."""
        mock_dnd_active.return_value = False
        mock_response = MagicMock()
        mock_response.__enter__.return_value.read.return_value = b'{"ok": true}'
        mock_urlopen.return_value = mock_response

        result = self.system.send_telegram("Test message")

        self.assertTrue(result)

    @patch('src.notification_system.DoNotDisturbSchedule.is_active')
    def test_send_telegram_missing_config(self, mock_dnd_active):
        """Test Telegram with missing configuration."""
        system = NotificationSystem(config={})  # No telegram config
        mock_dnd_active.return_value = False

        result = system.send_telegram("Test message")

        self.assertFalse(result)


class TestDiscordNotification(unittest.TestCase):
    """Tests for Discord notification functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.system = NotificationSystem(config={
            "discord": {
                "webhook_url": "https://discord.com/api/webhooks/test",
                "username": "Test Bot"
            }
        })

    @patch('src.notification_system.urlopen')
    @patch('src.notification_system.DoNotDisturbSchedule.is_active')
    def test_send_discord_success(self, mock_dnd_active, mock_urlopen):
        """Test successful Discord message."""
        mock_dnd_active.return_value = False
        mock_response = MagicMock()
        mock_response.__enter__.return_value.read.return_value = b'{"id": "123"}'
        mock_urlopen.return_value = mock_response

        result = self.system.send_discord("Test message")

        self.assertTrue(result)

    @patch('src.notification_system.DoNotDisturbSchedule.is_active')
    def test_send_discord_missing_webhook(self, mock_dnd_active):
        """Test Discord with missing webhook URL."""
        system = NotificationSystem(config={"discord": {}})
        mock_dnd_active.return_value = False

        result = system.send_discord("Test message")

        self.assertFalse(result)


class TestSlackNotification(unittest.TestCase):
    """Tests for Slack notification functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.system = NotificationSystem(config={
            "slack": {
                "webhook_url": "https://hooks.slack.com/services/test",
                "username": "Test Bot"
            }
        })

    @patch('src.notification_system.urlopen')
    @patch('src.notification_system.DoNotDisturbSchedule.is_active')
    def test_send_slack_success(self, mock_dnd_active, mock_urlopen):
        """Test successful Slack message."""
        mock_dnd_active.return_value = False
        mock_response = MagicMock()
        mock_response.__enter__.return_value.read.return_value = b'ok'
        mock_urlopen.return_value = mock_response

        result = self.system.send_slack("Test message")

        self.assertTrue(result)

    @patch('src.notification_system.urlopen')
    @patch('src.notification_system.DoNotDisturbSchedule.is_active')
    def test_send_slack_with_blocks(self, mock_dnd_active, mock_urlopen):
        """Test Slack message with blocks."""
        mock_dnd_active.return_value = False
        mock_response = MagicMock()
        mock_response.__enter__.return_value.read.return_value = b'ok'
        mock_urlopen.return_value = mock_response

        blocks = [{"type": "section", "text": {"type": "plain_text", "text": "Hello"}}]
        result = self.system.send_slack("Test message", blocks=blocks)

        self.assertTrue(result)


class TestWebhookNotification(unittest.TestCase):
    """Tests for generic webhook notification functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.system = NotificationSystem()

    @patch('src.notification_system.urlopen')
    @patch('src.notification_system.DoNotDisturbSchedule.is_active')
    def test_send_webhook_success(self, mock_dnd_active, mock_urlopen):
        """Test successful webhook call."""
        mock_dnd_active.return_value = False
        mock_response = MagicMock()
        mock_response.__enter__.return_value.read.return_value = b'success'
        mock_urlopen.return_value = mock_response

        result = self.system.send_webhook(
            "https://example.com/webhook",
            {"message": "test"}
        )

        self.assertTrue(result)

    @patch('src.notification_system.DoNotDisturbSchedule.is_active')
    def test_send_webhook_blocked_by_dnd(self, mock_dnd_active):
        """Test webhook blocked by DND."""
        mock_dnd_active.return_value = True

        result = self.system.send_webhook(
            "https://example.com/webhook",
            {"message": "test"}
        )

        self.assertFalse(result)


class TestSMSNotification(unittest.TestCase):
    """Tests for Twilio SMS notification functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.system = NotificationSystem(config={
            "twilio": {
                "account_sid": "AC123",
                "auth_token": "token123",
                "from_number": "+15555555555"
            }
        })

    @patch('src.notification_system.urlopen')
    @patch('src.notification_system.DoNotDisturbSchedule.is_active')
    def test_send_sms_success(self, mock_dnd_active, mock_urlopen):
        """Test successful SMS sending."""
        mock_dnd_active.return_value = False
        mock_response = MagicMock()
        mock_response.__enter__.return_value.read.return_value = b'{"sid": "SM123"}'
        mock_urlopen.return_value = mock_response

        result = self.system.send_sms("+15555555556", "Test SMS")

        self.assertTrue(result)

    def test_send_sms_missing_config(self):
        """Test SMS with missing configuration."""
        system = NotificationSystem(config={})  # No twilio config

        result = system.send_sms("+15555555556", "Test SMS")

        self.assertFalse(result)


class TestUnifiedSend(unittest.TestCase):
    """Tests for the unified send method."""

    def setUp(self):
        """Set up test fixtures."""
        self.system = NotificationSystem()

    @patch.object(NotificationSystem, 'send_email')
    @patch.object(NotificationSystem, 'send_telegram')
    @patch.object(NotificationSystem, 'send_discord')
    def test_send_with_channels(self, mock_discord, mock_telegram, mock_email):
        """Test send with specified channels."""
        mock_email.return_value = True
        mock_telegram.return_value = True
        mock_discord.return_value = True

        result = self.system.send(
            "Test message",
            channels=[NotificationChannel.EMAIL, NotificationChannel.TELEGRAM]
        )

        self.assertTrue(result["email"])
        self.assertTrue(result["telegram"])
        mock_discord.assert_not_called()

    @patch.object(NotificationSystem, 'send_desktop_notification')
    def test_send_with_priority(self, mock_desktop):
        """Test send respects priority-based channels."""
        mock_desktop.return_value = True

        # For LOW priority, only EMAIL should be used by default
        result = self.system.send(
            "Test message",
            title="Test",
            priority=NotificationPriority.LOW,
            to_email="test@example.com"
        )

        # Desktop should NOT be called for LOW priority
        mock_desktop.assert_not_called()


class TestSendFromTemplate(unittest.TestCase):
    """Tests for template-based sending."""

    def setUp(self):
        """Set up test fixtures."""
        self.system = NotificationSystem()

    @patch.object(NotificationSystem, 'send')
    def test_send_from_template(self, mock_send):
        """Test sending from a named template."""
        mock_send.return_value = {"email": True}

        result = self.system.send_from_template(
            "workflow_complete",
            workflow_name="Test Workflow",
            timestamp="2024-01-01 12:00"
        )

        self.assertTrue(result["email"])
        mock_send.assert_called_once()
        call_kwargs = mock_send.call_args[1]
        self.assertIn("Test Workflow", call_kwargs["message"])
        self.assertIn("2024-01-01 12:00", call_kwargs["message"])

    def test_send_from_missing_template(self):
        """Test sending from non-existent template."""
        result = self.system.send_from_template("nonexistent")
        # When template doesn't exist, all channel results should be False
        for channel_result in result.values():
            self.assertFalse(channel_result)


class TestRetryWithBackoff(unittest.TestCase):
    """Tests for retry with exponential backoff."""

    def setUp(self):
        """Set up test fixtures."""
        self.system = NotificationSystem(config={
            "email": {
                "smtp_host": "smtp.test.com",
                "smtp_port": 587,
                "smtp_user": "user",
                "smtp_password": "pass",
                "from_addr": "from@test.com"
            }
        })

    @patch('src.notification_system.time.sleep')
    @patch('src.notification_system.smtplib.SMTP')
    @patch('src.notification_system.DoNotDisturbSchedule.is_active')
    def test_retry_succeeds_on_third_attempt(self, mock_dnd_active, mock_smtp, mock_sleep):
        """Test retry succeeds after initial failures."""
        mock_dnd_active.return_value = False
        mock_server = MagicMock()
        mock_smtp.return_value.__enter__.return_value = mock_server

        # First two calls fail, third succeeds
        mock_server.send_message.side_effect = [
            Exception("Connection error"),
            Exception("Timeout"),
            None  # Success
        ]

        result = self.system.send_email("to@test.com", "Subject", "Body")

        self.assertTrue(result)
        self.assertEqual(mock_server.send_message.call_count, 3)
        # Should have slept twice (between attempts)
        self.assertEqual(mock_sleep.call_count, 2)

    @patch('src.notification_system.time.sleep')
    @patch('src.notification_system.smtplib.SMTP')
    @patch('src.notification_system.DoNotDisturbSchedule.is_active')
    def test_retry_all_fail(self, mock_dnd_active, mock_smtp, mock_sleep):
        """Test all retry attempts fail."""
        mock_dnd_active.return_value = False
        mock_server = MagicMock()
        mock_smtp.return_value.__enter__.return_value = mock_server
        mock_server.send_message.side_effect = Exception("Always fails")

        result = self.system.send_email("to@test.com", "Subject", "Body")

        self.assertFalse(result)
        # Should have retried max_attempts times
        self.assertEqual(mock_server.send_message.call_count, 3)


class TestDNDExemptChannels(unittest.TestCase):
    """Tests for DND exempt channels."""

    def test_exempt_channel_bypasses_dnd(self):
        """Test that exempt channels bypass DND."""
        schedule = DoNotDisturbSchedule(
            enabled=True,
            start_time="00:00",
            end_time="23:59",  # All day
            exempt_channels=["telegram"]
        )

        # The exempt_channels list should contain telegram
        self.assertIn("telegram", schedule.exempt_channels)
        self.assertNotIn("email", schedule.exempt_channels)

    def test_is_dnd_active_respects_exempt_channels(self):
        """Test that _is_dnd_active checks exempt channels."""
        system = NotificationSystem()
        system.dnd_schedule = DoNotDisturbSchedule(
            enabled=True,
            start_time="00:00",
            end_time="23:59",
            exempt_channels=["telegram"]
        )

        # Telegram should be exempt (bypass DND)
        self.assertFalse(system._is_dnd_active(NotificationChannel.TELEGRAM))
        # Other channels should not be exempt
        self.assertTrue(system._is_dnd_active(NotificationChannel.EMAIL))


if __name__ == "__main__":
    unittest.main()
