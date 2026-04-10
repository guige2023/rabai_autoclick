"""
Tests for workflow_aws_comprehend module

Comprehensive tests for ComprehendIntegration class covering:
- Entity recognition
- Sentiment analysis
- Topic modeling
- Key phrase extraction
- Language detection
- PII detection
- Custom classification
- Events detection
- Flywheel integration
- CloudWatch metrics
"""
import sys
sys.path.insert(0, '/Users/guige/my_project')

import unittest
from unittest.mock import Mock, patch, MagicMock, PropertyMock
import json
import time
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
import types

# Create mock boto3 module before importing workflow module
mock_boto3 = types.ModuleType('boto3')
mock_boto3.Session = MagicMock()
mock_boto3.client = MagicMock()

sys.modules['boto3'] = mock_boto3

# Import the module
import src.workflow_aws_comprehend as comprehend_module

ComprehendIntegration = comprehend_module.ComprehendIntegration
CloudWatchMetrics = comprehend_module.CloudWatchMetrics
SentimentType = comprehend_module.SentimentType
EntityType = comprehend_module.EntityType
PIIEntityType = comprehend_module.PIIEntityType
Entity = comprehend_module.Entity
KeyPhrase = comprehend_module.KeyPhrase
SentimentResult = comprehend_module.SentimentResult
AspectSentiment = comprehend_module.AspectSentiment
LanguageDetected = comprehend_module.LanguageDetected
PIIDetectionResult = comprehend_module.PIIDetectionResult
TopicResult = comprehend_module.TopicResult
KeyPhraseResult = comprehend_module.KeyPhraseResult
CustomClassificationResult = comprehend_module.CustomClassificationResult
EventDetectionResult = comprehend_module.EventDetectionResult


class TestSentimentType(unittest.TestCase):
    """Test SentimentType enum"""

    def test_sentiment_values(self):
        self.assertEqual(SentimentType.POSITIVE.value, "POSITIVE")
        self.assertEqual(SentimentType.NEGATIVE.value, "NEGATIVE")
        self.assertEqual(SentimentType.NEUTRAL.value, "NEUTRAL")
        self.assertEqual(SentimentType.MIXED.value, "MIXED")


class TestEntityType(unittest.TestCase):
    """Test EntityType enum"""

    def test_entity_type_values(self):
        self.assertEqual(EntityType.PERSON.value, "PERSON")
        self.assertEqual(EntityType.LOCATION.value, "LOCATION")
        self.assertEqual(EntityType.ORGANIZATION.value, "ORGANIZATION")
        self.assertEqual(EntityType.DATE.value, "DATE")
        self.assertEqual(EntityType.OTHER.value, "OTHER")


class TestPIIEntityType(unittest.TestCase):
    """Test PIIEntityType enum"""

    def test_pii_entity_type_values(self):
        self.assertEqual(PIIEntityType.NAME.value, "NAME")
        self.assertEqual(PIIEntityType.SSN.value, "SSN")
        self.assertEqual(PIIEntityType.EMAIL.value, "EMAIL")
        self.assertEqual(PIIEntityType.PHONE.value, "PHONE")


class TestEntity(unittest.TestCase):
    """Test Entity dataclass"""

    def test_entity_creation(self):
        entity = Entity(
            text="John",
            entity_type="PERSON",
            score=0.99,
            begin_offset=0,
            end_offset=4
        )
        self.assertEqual(entity.text, "John")
        self.assertEqual(entity.entity_type, "PERSON")
        self.assertEqual(entity.score, 0.99)


class TestKeyPhrase(unittest.TestCase):
    """Test KeyPhrase dataclass"""

    def test_key_phrase_creation(self):
        phrase = KeyPhrase(text="artificial intelligence", score=0.95)
        self.assertEqual(phrase.text, "artificial intelligence")
        self.assertEqual(phrase.score, 0.95)


class TestSentimentResult(unittest.TestCase):
    """Test SentimentResult dataclass"""

    def test_sentiment_result_creation(self):
        result = SentimentResult(
            sentiment=SentimentType.POSITIVE,
            sentiment_score={"Positive": 0.8, "Negative": 0.1, "Neutral": 0.05, "Mixed": 0.05}
        )
        self.assertEqual(result.sentiment, SentimentType.POSITIVE)
        self.assertEqual(result.sentiment_score["Positive"], 0.8)


class TestAspectSentiment(unittest.TestCase):
    """Test AspectSentiment dataclass"""

    def test_aspect_sentiment_creation(self):
        sentiment = AspectSentiment(
            aspect="food quality",
            sentiment=SentimentType.POSITIVE,
            confidence_scores={"Positive": 0.9, "Negative": 0.05, "Neutral": 0.05, "Mixed": 0.0}
        )
        self.assertEqual(sentiment.aspect, "food quality")
        self.assertEqual(sentiment.sentiment, SentimentType.POSITIVE)


class TestLanguageDetected(unittest.TestCase):
    """Test LanguageDetected dataclass"""

    def test_language_detected_creation(self):
        lang = LanguageDetected(language_code="en", score=0.99)
        self.assertEqual(lang.language_code, "en")
        self.assertEqual(lang.score, 0.99)


class TestPIIDetectionResult(unittest.TestCase):
    """Test PIIDetectionResult dataclass"""

    def test_pii_detection_result_creation(self):
        result = PIIDetectionResult(
            entity_type="NAME",
            score=0.95,
            begin_offset=0,
            end_offset=15,
            text="John Doe"
        )
        self.assertEqual(result.entity_type, "NAME")
        self.assertEqual(result.text, "John Doe")


class TestTopicResult(unittest.TestCase):
    """Test TopicResult dataclass"""

    def test_topic_result_creation(self):
        result = TopicResult(topic_arn="arn:aws:comprehend:us-east-1:123456789012:topic/test", score=0.85)
        self.assertEqual(result.topic_arn, "arn:aws:comprehend:us-east-1:123456789012:topic/test")


class TestKeyPhraseResult(unittest.TestCase):
    """Test KeyPhraseResult dataclass"""

    def test_key_phrase_result_creation(self):
        result = KeyPhraseResult(text="machine learning", score=0.92)
        self.assertEqual(result.text, "machine learning")


class TestCustomClassificationResult(unittest.TestCase):
    """Test CustomClassificationResult dataclass"""

    def test_custom_classification_result_creation(self):
        result = CustomClassificationResult(class_name="positive", score=0.88)
        self.assertEqual(result.class_name, "positive")
        self.assertEqual(result.score, 0.88)


class TestEventDetectionResult(unittest.TestCase):
    """Test EventDetectionResult dataclass"""

    def test_event_detection_result_creation(self):
        result = EventDetectionResult(event_type="VERB_EVENT", score=0.75, span=(0, 10))
        self.assertEqual(result.event_type, "VERB_EVENT")
        self.assertEqual(result.span, (0, 10))


class TestCloudWatchMetrics(unittest.TestCase):
    """Test CloudWatchMetrics class"""

    def setUp(self):
        self.mock_cloudwatch_client = MagicMock()
        self.boto3_patcher = patch('boto3.client')
        self.mock_boto3_client = self.boto3_patcher.start()
        self.mock_boto3_client.return_value = self.mock_cloudwatch_client

    def tearDown(self):
        self.boto3_patcher.stop()

    def test_init_with_default_namespace(self):
        metrics = CloudWatchMetrics()
        self.assertEqual(metrics.namespace, "AWS/Comprehend")
        self.assertEqual(metrics.metrics_data, [])

    def test_init_with_custom_namespace(self):
        metrics = CloudWatchMetrics(namespace="Custom/Namespace")
        self.assertEqual(metrics.namespace, "Custom/Namespace")

    def test_record_entity_recognition(self):
        metrics = CloudWatchMetrics()
        metrics.record_entity_recognition(entity_count=5, latency_ms=100.0)
        self.assertEqual(len(metrics.metrics_data), 2)
        self.assertEqual(metrics.metrics_data[0]["MetricName"], "EntityRecognitionCount")
        self.assertEqual(metrics.metrics_data[1]["MetricName"], "EntityRecognitionLatency")

    def test_record_sentiment_analysis(self):
        metrics = CloudWatchMetrics()
        metrics.record_sentiment_analysis(latency_ms=50.0)
        self.assertEqual(len(metrics.metrics_data), 2)
        self.assertEqual(metrics.metrics_data[0]["MetricName"], "SentimentAnalysisCount")

    def test_record_topic_modeling(self):
        metrics = CloudWatchMetrics()
        metrics.record_topic_modeling(topic_count=10, latency_ms=500.0)
        self.assertEqual(len(metrics.metrics_data), 2)
        self.assertEqual(metrics.metrics_data[0]["MetricName"], "TopicModelingCount")

    def test_record_key_phrase_extraction(self):
        metrics = CloudWatchMetrics()
        metrics.record_key_phrase_extraction(phrase_count=8, latency_ms=75.0)
        self.assertEqual(len(metrics.metrics_data), 2)

    def test_record_language_detection(self):
        metrics = CloudWatchMetrics()
        metrics.record_language_detection(latency_ms=25.0)
        self.assertEqual(len(metrics.metrics_data), 2)

    def test_record_pii_detection(self):
        metrics = CloudWatchMetrics()
        metrics.record_pii_detection(pii_count=3, latency_ms=60.0)
        self.assertEqual(len(metrics.metrics_data), 2)

    def test_record_custom_classification(self):
        metrics = CloudWatchMetrics()
        metrics.record_custom_classification(latency_ms=120.0)
        self.assertEqual(len(metrics.metrics_data), 2)

    def test_record_event_detection(self):
        metrics = CloudWatchMetrics()
        metrics.record_event_detection(event_count=2, latency_ms=45.0)
        self.assertEqual(len(metrics.metrics_data), 2)

    def test_flush_with_data(self):
        metrics = CloudWatchMetrics()
        metrics.record_entity_recognition(entity_count=5, latency_ms=100.0)
        metrics.flush()
        self.mock_cloudwatch_client.put_metric_data.assert_called_once()
        self.assertEqual(len(metrics.metrics_data), 0)

    def test_flush_without_data(self):
        metrics = CloudWatchMetrics()
        metrics.flush()
        self.mock_cloudwatch_client.put_metric_data.assert_not_called()


class TestComprehendIntegration(unittest.TestCase):
    """Test ComprehendIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_comprehend_client = MagicMock()
        
        # Patch boto3.client
        self.boto3_patcher = patch('boto3.client')
        self.mock_boto3_client = self.boto3_patcher.start()
        self.mock_boto3_client.return_value = self.mock_comprehend_client

    def tearDown(self):
        self.boto3_patcher.stop()

    def test_init_with_defaults(self):
        """Test initialization with defaults"""
        integration = ComprehendIntegration()
        self.assertEqual(integration.region_name, "us-east-1")
        self.assertIsNone(integration.flywheel_arn)
        self.assertIsNone(integration.custom_classifier_arn)
        self.assertTrue(integration.enable_cloudwatch)

    def test_init_with_custom_params(self):
        """Test initialization with custom parameters"""
        integration = ComprehendIntegration(
            region_name="us-west-2",
            endpoint_url="https://vpce-xxx.comprehend.us-west-2.vpce.amazonaws.com",
            flywheel_arn="arn:aws:comprehend:us-west-2:123456789012:flywheel/test",
            custom_classifier_arn="arn:aws:comprehend:us-west-2:123456789012:document-classifier/test",
            enable_cloudwatch=False
        )
        self.assertEqual(integration.region_name, "us-west-2")
        self.assertEqual(integration.flywheel_arn, "arn:aws:comprehend:us-west-2:123456789012:flywheel/test")
        self.assertFalse(integration.enable_cloudwatch)


class TestEntityRecognition(unittest.TestCase):
    """Test entity recognition methods"""

    def setUp(self):
        self.mock_comprehend_client = MagicMock()
        self.boto3_patcher = patch('boto3.client')
        self.mock_boto3_client = self.boto3_patcher.start()
        self.mock_boto3_client.return_value = self.mock_comprehend_client
        self.integration = ComprehendIntegration()

    def tearDown(self):
        self.boto3_patcher.stop()

    def test_detect_entities_success(self):
        """Test successful entity detection"""
        self.mock_comprehend_client.detect_entities.return_value = {
            "Entities": [
                {"Text": "John", "Type": "PERSON", "Score": 0.99, "BeginOffset": 0, "EndOffset": 4},
                {"Text": "San Francisco", "Type": "LOCATION", "Score": 0.95, "BeginOffset": 10, "EndOffset": 23}
            ]
        }
        
        result = self.integration.detect_entities("John lives in San Francisco", "en")
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].text, "John")
        self.assertEqual(result[0].entity_type, "PERSON")
        self.assertEqual(result[1].text, "San Francisco")
        self.assertEqual(result[1].entity_type, "LOCATION")

    def test_detect_entities_error(self):
        """Test entity detection with error"""
        self.mock_comprehend_client.detect_entities.side_effect = Exception("API error")
        
        with self.assertRaises(Exception):
            self.integration.detect_entities("John lives in San Francisco", "en")

    def test_detect_entities_batch_success(self):
        """Test successful batch entity detection"""
        self.mock_comprehend_client.batch_detect_entities.return_value = {
            "ResultList": [
                {
                    "Entities": [
                        {"Text": "John", "Type": "PERSON", "Score": 0.99, "BeginOffset": 0, "EndOffset": 4}
                    ]
                },
                {
                    "Entities": [
                        {"Text": "Amazon", "Type": "ORGANIZATION", "Score": 0.90, "BeginOffset": 0, "EndOffset": 6}
                    ]
                }
            ]
        }
        
        result = self.integration.detect_entities_batch(["John lives here", "Amazon is great"], "en")
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0][0].text, "John")
        self.assertEqual(result[1][0].text, "Amazon")


class TestSentimentAnalysis(unittest.TestCase):
    """Test sentiment analysis methods"""

    def setUp(self):
        self.mock_comprehend_client = MagicMock()
        self.boto3_patcher = patch('boto3.client')
        self.mock_boto3_client = self.boto3_patcher.start()
        self.mock_boto3_client.return_value = self.mock_comprehend_client
        self.integration = ComprehendIntegration()

    def tearDown(self):
        self.boto3_patcher.stop()

    def test_detect_sentiment_success(self):
        """Test successful sentiment detection"""
        self.mock_comprehend_client.detect_sentiment.return_value = {
            "Sentiment": "POSITIVE",
            "SentimentScore": {
                "Positive": 0.85,
                "Negative": 0.05,
                "Neutral": 0.07,
                "Mixed": 0.03
            }
        }
        
        result = self.integration.detect_sentiment("I love this product!", "en")
        
        self.assertEqual(result.sentiment, SentimentType.POSITIVE)
        self.assertEqual(result.sentiment_score["Positive"], 0.85)

    def test_detect_sentiment_error(self):
        """Test sentiment detection with error"""
        self.mock_comprehend_client.detect_sentiment.side_effect = Exception("API error")
        
        with self.assertRaises(Exception):
            self.integration.detect_sentiment("I love this product!", "en")

    def test_detect_sentiment_batch_success(self):
        """Test successful batch sentiment detection"""
        self.mock_comprehend_client.batch_detect_sentiment.return_value = {
            "ResultList": [
                {"Sentiment": "POSITIVE", "SentimentScore": {"Positive": 0.9}},
                {"Sentiment": "NEGATIVE", "SentimentScore": {"Negative": 0.8}}
            ]
        }
        
        result = self.integration.detect_sentiment_batch(["Great!", "Terrible!"], "en")
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].sentiment, SentimentType.POSITIVE)
        self.assertEqual(result[1].sentiment, SentimentType.NEGATIVE)

    def test_detect_aspect_sentiment_success(self):
        """Test successful aspect-based sentiment detection"""
        self.mock_comprehend_client.detect_targeted_sentiment.return_value = {
            "TargetedSentimentEntities": [
                {
                    "Type": "ASPECT",
                    "Text": "food",
                    "Sentiment": "POSITIVE",
                    "SentimentScore": {"Positive": 0.9, "Negative": 0.05, "Neutral": 0.05, "Mixed": 0.0}
                }
            ]
        }
        
        result = self.integration.detect_aspect_sentiment("The food was delicious", "food", "en")
        
        self.assertEqual(result.aspect, "food")
        self.assertEqual(result.sentiment, SentimentType.POSITIVE)

    def test_detect_aspect_sentiment_no_result(self):
        """Test aspect sentiment with no matching aspect"""
        self.mock_comprehend_client.detect_targeted_sentiment.return_value = {
            "TargetedSentimentEntities": []
        }
        
        result = self.integration.detect_aspect_sentiment("The food was delicious", "service", "en")
        
        self.assertEqual(result.aspect, "service")
        self.assertEqual(result.sentiment, SentimentType.NEUTRAL)


class TestKeyPhraseExtraction(unittest.TestCase):
    """Test key phrase extraction methods"""

    def setUp(self):
        self.mock_comprehend_client = MagicMock()
        self.boto3_patcher = patch('boto3.client')
        self.mock_boto3_client = self.boto3_patcher.start()
        self.mock_boto3_client.return_value = self.mock_comprehend_client
        self.integration = ComprehendIntegration()

    def tearDown(self):
        self.boto3_patcher.stop()

    def test_extract_key_phrases_success(self):
        """Test successful key phrase extraction"""
        self.mock_comprehend_client.detect_key_phrases.return_value = {
            "KeyPhrases": [
                {"Text": "artificial intelligence", "Score": 0.95},
                {"Text": "machine learning", "Score": 0.90}
            ]
        }
        
        result = self.integration.extract_key_phrases("Artificial intelligence and machine learning are related fields", "en")
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].text, "artificial intelligence")
        self.assertEqual(result[0].score, 0.95)

    def test_extract_key_phrases_error(self):
        """Test key phrase extraction with error"""
        self.mock_comprehend_client.detect_key_phrases.side_effect = Exception("API error")
        
        with self.assertRaises(Exception):
            self.integration.extract_key_phrases("Test text", "en")

    def test_extract_key_phrases_batch_success(self):
        """Test successful batch key phrase extraction"""
        self.mock_comprehend_client.batch_detect_key_phrases.return_value = {
            "ResultList": [
                {"KeyPhrases": [{"Text": "AI", "Score": 0.95}]},
                {"KeyPhrases": [{"Text": "ML", "Score": 0.90}]}
            ]
        }
        
        result = self.integration.extract_key_phrases_batch(["AI is great", "ML is useful"], "en")
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0][0].text, "AI")


class TestLanguageDetection(unittest.TestCase):
    """Test language detection methods"""

    def setUp(self):
        self.mock_comprehend_client = MagicMock()
        self.boto3_patcher = patch('boto3.client')
        self.mock_boto3_client = self.boto3_patcher.start()
        self.mock_boto3_client.return_value = self.mock_comprehend_client
        self.integration = ComprehendIntegration()

    def tearDown(self):
        self.boto3_patcher.stop()

    def test_detect_language_success(self):
        """Test successful language detection"""
        self.mock_comprehend_client.detect_dominant_language.return_value = {
            "Languages": [
                {"LanguageCode": "en", "Score": 0.99},
                {"LanguageCode": "es", "Score": 0.01}
            ]
        }
        
        result = self.integration.detect_language("This is English text")
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].language_code, "en")
        self.assertEqual(result[0].score, 0.99)

    def test_detect_language_error(self):
        """Test language detection with error"""
        self.mock_comprehend_client.detect_dominant_language.side_effect = Exception("API error")
        
        with self.assertRaises(Exception):
            self.integration.detect_language("Test text")


class TestPIIDetection(unittest.TestCase):
    """Test PII detection methods"""

    def setUp(self):
        self.mock_comprehend_client = MagicMock()
        self.boto3_patcher = patch('boto3.client')
        self.mock_boto3_client = self.boto3_patcher.start()
        self.mock_boto3_client.return_value = self.mock_comprehend_client
        self.integration = ComprehendIntegration()

    def tearDown(self):
        self.boto3_patcher.stop()

    def test_detect_pii_success(self):
        """Test successful PII detection"""
        self.mock_comprehend_client.detect_pii_entities.return_value = {
            "Entities": [
                {"Type": "NAME", "Score": 0.95, "BeginOffset": 0, "EndOffset": 8},
                {"Type": "EMAIL", "Score": 0.90, "BeginOffset": 10, "EndOffset": 25}
            ]
        }
        
        result = self.integration.detect_pii("John Doe john@example.com", "en")
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].entity_type, "NAME")
        self.assertEqual(result[1].entity_type, "EMAIL")

    def test_detect_pii_with_labels_filter(self):
        """Test PII detection with labels filter"""
        self.mock_comprehend_client.detect_pii_entities.return_value = {
            "Entities": [
                {"Type": "NAME", "Score": 0.95, "BeginOffset": 0, "EndOffset": 8},
                {"Type": "EMAIL", "Score": 0.90, "BeginOffset": 10, "EndOffset": 25}
            ]
        }
        
        result = self.integration.detect_pii("John Doe john@example.com", "en", labels=["NAME"])
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].entity_type, "NAME")

    def test_detect_pii_error(self):
        """Test PII detection with error"""
        self.mock_comprehend_client.detect_pii_entities.side_effect = Exception("API error")
        
        with self.assertRaises(Exception):
            self.integration.detect_pii("John Doe john@example.com", "en")


class TestCustomClassification(unittest.TestCase):
    """Test custom classification methods"""

    def setUp(self):
        self.mock_comprehend_client = MagicMock()
        self.boto3_patcher = patch('boto3.client')
        self.mock_boto3_client = self.boto3_patcher.start()
        self.mock_boto3_client.return_value = self.mock_comprehend_client
        self.integration = ComprehendIntegration(
            custom_classifier_arn="arn:aws:comprehend:us-east-1:123456789012:document-classifier/test"
        )

    def tearDown(self):
        self.boto3_patcher.stop()

    def test_classify_custom_success(self):
        """Test successful custom classification"""
        self.mock_comprehend_client.classify_document.return_value = {
            "Classes": [
                {"Name": "positive", "Score": 0.85},
                {"Name": "negative", "Score": 0.15}
            ]
        }
        
        result = self.integration.classify_custom("This is a great product!")
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].class_name, "positive")
        self.assertEqual(result[0].score, 0.85)

    def test_classify_custom_no_arn(self):
        """Test custom classification without ARN"""
        integration = ComprehendIntegration()
        
        with self.assertRaises(ValueError):
            integration.classify_custom("This is a great product!")

    def test_classify_custom_error(self):
        """Test custom classification with error"""
        self.mock_comprehend_client.classify_document.side_effect = Exception("API error")
        
        with self.assertRaises(Exception):
            self.integration.classify_custom("This is a great product!")


class TestEventsDetection(unittest.TestCase):
    """Test events detection methods"""

    def setUp(self):
        self.mock_comprehend_client = MagicMock()
        self.boto3_patcher = patch('boto3.client')
        self.mock_boto3_client = self.boto3_patcher.start()
        self.mock_boto3_client.return_value = self.mock_comprehend_client
        self.integration = ComprehendIntegration()

    def tearDown(self):
        self.boto3_patcher.stop()

    def test_detect_events_success(self):
        """Test successful event detection"""
        self.mock_comprehend_client.detectsyntax.return_value = {
            "SyntaxTokens": [
                {"BeginOffset": 0, "EndOffset": 4, "PartOfSpeech": {"Tag": "VERB"}, "Score": 0.95},
                {"BeginOffset": 5, "EndOffset": 9, "PartOfSpeech": {"Tag": "NOUN"}, "Score": 0.90}
            ]
        }
        
        result = self.integration.detect_events("John runs fast", "en")
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].event_type, "VERB_EVENT")

    def test_detect_events_error(self):
        """Test event detection with error"""
        self.mock_comprehend_client.detectsyntax.side_effect = Exception("API error")
        
        with self.assertRaises(Exception):
            self.integration.detect_events("John runs fast", "en")


class TestFlywheelIntegration(unittest.TestCase):
    """Test flywheel integration methods"""

    def setUp(self):
        self.mock_comprehend_client = MagicMock()
        self.boto3_patcher = patch('boto3.client')
        self.mock_boto3_client = self.boto3_patcher.start()
        self.mock_boto3_client.return_value = self.mock_comprehend_client
        self.integration = ComprehendIntegration()

    def tearDown(self):
        self.boto3_patcher.stop()

    def test_create_flywheel_success(self):
        """Test successful flywheel creation"""
        self.mock_comprehend_client.create_flywheel.return_value = {
            "FlywheelArn": "arn:aws:comprehend:us-east-1:123456789012:flywheel/test-flywheel"
        }
        
        result = self.integration.create_flywheel(
            flywheel_name="test-flywheel",
            data_lake_bucket="my-bucket"
        )
        
        self.assertEqual(result["FlywheelArn"], "arn:aws:comprehend:us-east-1:123456789012:flywheel/test-flywheel")
        self.assertEqual(self.integration.flywheel_arn, result["FlywheelArn"])

    def test_create_flywheel_error(self):
        """Test flywheel creation with error"""
        self.mock_comprehend_client.create_flywheel.side_effect = Exception("API error")
        
        with self.assertRaises(Exception):
            self.integration.create_flywheel(
                flywheel_name="test-flywheel",
                data_lake_bucket="my-bucket"
            )

    def test_update_flywheel_success(self):
        """Test successful flywheel update"""
        self.integration.flywheel_arn = "arn:aws:comprehend:us-east-1:123456789012:flywheel/test-flywheel"
        self.mock_comprehend_client.update_flywheel.return_value = {
            "FlywheelArn": "arn:aws:comprehend:us-east-1:123456789012:flywheel/test-flywheel"
        }
        
        result = self.integration.update_flywheel()
        
        self.assertEqual(result["FlywheelArn"], self.integration.flywheel_arn)

    def test_update_flywheel_no_arn(self):
        """Test flywheel update without ARN"""
        integration = ComprehendIntegration()
        
        with self.assertRaises(ValueError):
            integration.update_flywheel()

    def test_get_flywheel_success(self):
        """Test successful flywheel retrieval"""
        self.integration.flywheel_arn = "arn:aws:comprehend:us-east-1:123456789012:flywheel/test-flywheel"
        self.mock_comprehend_client.describe_flywheel.return_value = {
            "FlywheelArn": self.integration.flywheel_arn,
            "FlywheelName": "test-flywheel",
            "Status": "ACTIVE"
        }
        
        result = self.integration.get_flywheel()
        
        self.assertEqual(result["FlywheelName"], "test-flywheel")

    def test_get_flywheel_no_arn(self):
        """Test flywheel retrieval without ARN"""
        integration = ComprehendIntegration()
        
        with self.assertRaises(ValueError):
            integration.get_flywheel()


if __name__ == '__main__':
    unittest.main()
