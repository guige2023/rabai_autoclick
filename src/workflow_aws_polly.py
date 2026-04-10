"""
AWS Polly (Text-to-Speech) Integration Module for Workflow System

Implements a PollyIntegration class with:
1. Text-to-speech: Convert text to speech
2. Speech synthesis: Synthesize speech from SSML
3. Voices: List available voices
4. Lexicons: Manage pronunciation lexicons
5. Speech marks: Get speech marks
6. Neural TTS: Neural voices
7. Long-form: Long-form audio generation
8. Voice engine: Engine selection (standard/neural)
9. SNS notifications: Async synthesis notifications
10. CloudWatch integration: Polly metrics

Commit: 'feat(aws-polly): add Amazon Polly with text-to-speech, SSML synthesis, voices, lexicons, speech marks, neural TTS, long-form audio, voice engine, SNS, CloudWatch'
"""

import uuid
import json
import threading
import time
import logging
import hashlib
import base64
from datetime import datetime
from typing import Dict, List, Callable, Any, Optional, Set, Union
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum
from io import BytesIO

try:
    import boto3
    from botocore.exceptions import (
        ClientError,
        BotoCoreError
    )
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    boto3 = None


logger = logging.getLogger(__name__)


class EngineType(Enum):
    """Polly voice engine types."""
    STANDARD = "standard"
    NEURAL = "neural"
    GENERATIVE = "generative"


class OutputFormat(Enum):
    """Polly audio output formats."""
    JSON = "json"
    MP3 = "mp3"
    OGG_VORBIS = "ogg_vorbis"
    PCM = "pcm"


class VoiceGender(Enum):
    """Voice gender types."""
    MALE = "Male"
    FEMALE = "Female"
    NEUTRAL = "Neutral"


class SpeechMarkType(Enum):
    """Types of speech marks."""
    SENTENCE = "sentence"
    SSML = "ssml"
    VISEME = "viseme"
    WORD = "word"


@dataclass
class Voice:
    """Polly voice information."""
    id: str
    name: str
    language_code: str
    language_name: Optional[str] = None
    gender: Optional[str] = None
    engine: Optional[str] = None
    supported_engines: List[str] = field(default_factory=list)


@dataclass
class SynthesisTask:
    """Polly synthesis task information."""
    task_id: str
    status: str
    output_uri: Optional[str] = None
    synthesis_request: Optional[Dict[str, Any]] = None
    creation_time: Optional[datetime] = None
    completion_time: Optional[datetime] = None
    error_message: Optional[str] = None
    audio_stream: Optional[bytes] = None


@dataclass
class Lexicon:
    """Polly pronunciation lexicon."""
    name: str
    content: str
    alphabet: str = "ipa"
    language_code: Optional[str] = None


@dataclass
class SpeechMark:
    """Speech mark information."""
    mark_type: str
    start: int
    end: int
    value: str


class PollyIntegration:
    """
    AWS Polly integration class for text-to-speech operations.
    
    Supports:
    - Text-to-speech conversion with multiple formats and engines
    - SSML-based speech synthesis
    - Voice listing and selection (standard, neural, generative)
    - Pronunciation lexicon management (PLS)
    - Speech marks extraction (word, sentence, viseme, ssml)
    - Long-form audio generation via async tasks
    - Engine selection (standard/neural/generative)
    - SNS notifications for async synthesis
    - CloudWatch metrics and monitoring
    """
    
    def __init__(
        self,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: str = "us-east-1",
        endpoint_url: Optional[str] = None,
        polly_client: Optional[Any] = None,
        sns_client: Optional[Any] = None,
        cloudwatch_client: Optional[Any] = None
    ):
        """
        Initialize Polly integration.
        
        Args:
            aws_access_key_id: AWS access key ID (uses boto3 credentials if None)
            aws_secret_access_key: AWS secret access key (uses boto3 credentials if None)
            region_name: AWS region name
            endpoint_url: Polly endpoint URL (for testing with LocalStack, etc.)
            polly_client: Pre-configured Polly client (overrides boto3 creation)
            sns_client: Pre-configured SNS client for notifications
            cloudwatch_client: Pre-configured CloudWatch client for metrics
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for Polly integration. Install with: pip install boto3")
        
        self.region_name = region_name
        self.endpoint_url = endpoint_url
        self._polly_client = polly_client
        self._sns_client = sns_client
        self._cloudwatch_client = cloudwatch_client
        self._cloudwatch_namespace = "Polly/Integration"
        self._lock = threading.RLock()
        
        session_kwargs = {
            "region_name": region_name
        }
        if aws_access_key_id and aws_secret_access_key:
            session_kwargs["aws_access_key_id"] = aws_access_key_id
            session_kwargs["aws_secret_access_key"] = aws_secret_access_key
        
        self._session = boto3.Session(**session_kwargs)
        
        self._metrics_buffer: List[Dict[str, Any]] = []
        self._metrics_lock = threading.Lock()
        self._active_tasks: Dict[str, SynthesisTask] = {}
        self._task_callbacks: Dict[str, Callable] = {}
    
    @property
    def polly_client(self):
        """Get or create Polly client."""
        if self._polly_client is None:
            kwargs = {"region_name": self.region_name}
            if self.endpoint_url:
                kwargs["endpoint_url"] = self.endpoint_url
            self._polly_client = self._session.client("polly", **kwargs)
        return self._polly_client
    
    @property
    def sns_client(self):
        """Get or create SNS client."""
        if self._sns_client is None:
            kwargs = {"region_name": self.region_name}
            if self.endpoint_url:
                kwargs["endpoint_url"] = self.endpoint_url
            self._sns_client = self._session.client("sns", **kwargs)
        return self._sns_client
    
    @property
    def cloudwatch_client(self):
        """Get or create CloudWatch client."""
        if self._cloudwatch_client is None:
            kwargs = {"region_name": self.region_name}
            if self.endpoint_url:
                kwargs["endpoint_url"] = self.endpoint_url
            self._cloudwatch_client = self._session.client("cloudwatch", **kwargs)
        return self._cloudwatch_client
    
    def _record_metric(self, metric_name: str, value: float, unit: str = "Count", dimensions: Dict[str, str] = None):
        """Record a metric for CloudWatch."""
        metric = {
            "MetricName": metric_name,
            "Value": value,
            "Unit": unit,
            "Timestamp": datetime.utcnow().isoformat()
        }
        if dimensions:
            metric["Dimensions"] = [{"Name": k, "Value": v} for k, v in dimensions.items()]
        
        with self._metrics_lock:
            self._metrics_buffer.append(metric)
    
    def flush_metrics(self):
        """Flush buffered metrics to CloudWatch."""
        with self._metrics_lock:
            if not self._metrics_buffer:
                return
            
            try:
                self.cloudwatch_client.put_metric_data(
                    Namespace=self._cloudwatch_namespace,
                    MetricData=self._metrics_buffer
                )
                logger.info(f"Flushed {len(self._metrics_buffer)} metrics to CloudWatch")
                self._metrics_buffer.clear()
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to flush metrics to CloudWatch: {e}")
    
    # =========================================================================
    # Text-to-Speech
    # =========================================================================
    
    def synthesize_speech(
        self,
        text: str,
        voice_id: str = "Joanna",
        output_format: str = "mp3",
        engine: str = "neural",
        language_code: Optional[str] = None,
        output_s3_bucket: Optional[str] = None,
        output_s3_key_prefix: Optional[str] = None,
        sns_topic_arn: Optional[str] = None,
        sample_rate: Optional[str] = None,
        text_type: str = "text",
        lexicon_names: Optional[List[str]] = None,
        speaking_style: Optional[str] = None,
        prosody: Optional[Dict[str, Any]] = None,
        whispered: bool = False,
        speech_mark_types: Optional[List[str]] = None,
        return_audio_stream: bool = False
    ) -> Dict[str, Any]:
        """
        Convert text to speech.
        
        Args:
            text: Text to synthesize (max 6000 bytes for standard, 150000 for neural)
            voice_id: Voice ID to use (e.g., 'Joanna', 'Matthew', 'Amy')
            output_format: Output format ('mp3', 'ogg_vorbis', 'pcm')
            engine: Engine to use ('standard', 'neural', 'generative')
            language_code: Language code (e.g., 'en-US')
            output_s3_bucket: S3 bucket for async synthesis output
            output_s3_key_prefix: S3 key prefix for output
            sns_topic_arn: SNS topic ARN for async task notifications
            sample_rate: Audio sample rate (e.g., '22050', '16000', '8000')
            text_type: 'text' or 'ssml'
            lexicon_names: List of lexicon names to apply
            speaking_style: Speaking style (e.g., 'conversation', 'news', 'customer-service')
            prosody: Prosody settings (pitch, rate, volume)
            whispered: Whether to produce whispered speech
            speech_mark_types: Types of speech marks to include
            return_audio_stream: Whether to return audio bytes directly
            
        Returns:
            Dictionary containing audio stream or task info
        """
        if len(text.encode('utf-8')) > 150000 and engine == "neural":
            logger.warning(f"Text size {len(text)} exceeds neural limit, using start_speech_synthesis_task")
            return self.start_speech_synthesis_task(
                text=text,
                voice_id=voice_id,
                output_format=output_format,
                engine=engine,
                language_code=language_code,
                output_s3_bucket=output_s3_bucket,
                output_s3_key_prefix=output_s3_key_prefix,
                sns_topic_arn=sns_topic_arn,
                sample_rate=sample_rate,
                text_type=text_type,
                lexicon_names=lexicon_names
            )
        
        try:
            params = {
                "Text": text,
                "VoiceId": voice_id,
                "OutputFormat": output_format,
                "Engine": engine
            }
            
            if language_code:
                params["LanguageCode"] = language_code
            
            if sample_rate:
                params["SampleRate"] = sample_rate
            
            if text_type == "ssml":
                params["TextType"] = "ssml"
            
            if lexicon_names:
                params["LexiconNames"] = lexicon_names
            
            if speaking_style:
                params["SpeechMarkTypes"] = ["ssml"] if text_type == "ssml" else []
                if text_type == "ssml" and speaking_style:
                    params["SpeechSynthesisTaskId"] = str(uuid.uuid4())
            
            if sns_topic_arn and output_s3_bucket:
                params["SnsTopicArn"] = sns_topic_arn
                params["OutputS3Bucket"] = output_s3_bucket
                if output_s3_key_prefix:
                    params["OutputS3KeyPrefix"] = output_s3_key_prefix
                task_id = str(uuid.uuid4())
                params["TaskId"] = task_id
                
                response = self.polly_client.start_speech_synthesis_task(**params)
                task = SynthesisTask(
                    task_id=task_id,
                    status=response.get("SynthesisTask", {}).get("TaskStatus", "scheduled"),
                    output_uri=response.get("SynthesisTask", {}).get("OutputUri"),
                    creation_time=datetime.utcnow()
                )
                self._active_tasks[task_id] = task
                self._record_metric("SynthesisTasksStarted", 1, "Count", {"Engine": engine})
                return {"task": task, "response": response}
            
            if speech_mark_types:
                params["SpeechMarkTypes"] = speech_mark_types
            
            response = self.polly_client.synthesize_speech(**params)
            
            audio_stream = response.get("AudioStream").read() if response.get("AudioStream") else None
            
            self._record_metric("SynthesizeSpeechRequests", 1, "Count", {
                "Engine": engine,
                "VoiceId": voice_id,
                "OutputFormat": output_format
            })
            
            if return_audio_stream:
                return {
                    "audio_stream": audio_stream,
                    "content_type": response.get("ContentType"),
                    "request_characters": response.get("RequestCharacters", 0)
                }
            
            return {
                "audio_stream": audio_stream,
                "content_type": response.get("ContentType"),
                "request_characters": response.get("RequestCharacters", 0)
            }
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to synthesize speech: {e}")
            raise
    
    def synthesize_speech_from_ssml(
        self,
        ssml_text: str,
        voice_id: str = "Joanna",
        output_format: str = "mp3",
        engine: str = "neural",
        language_code: Optional[str] = None,
        output_s3_bucket: Optional[str] = None,
        output_s3_key_prefix: Optional[str] = None,
        sns_topic_arn: Optional[str] = None,
        sample_rate: Optional[str] = None,
        return_audio_stream: bool = False
    ) -> Dict[str, Any]:
        """
        Synthesize speech from SSML markup.
        
        Args:
            ssml_text: SSML markup text
            voice_id: Voice ID to use
            output_format: Output format ('mp3', 'ogg_vorbis', 'pcm')
            engine: Engine to use ('standard', 'neural', 'generative')
            language_code: Language code
            output_s3_bucket: S3 bucket for async synthesis
            output_s3_key_prefix: S3 key prefix for output
            sns_topic_arn: SNS topic ARN for notifications
            sample_rate: Audio sample rate
            return_audio_stream: Whether to return audio bytes
            
        Returns:
            Dictionary containing audio or task info
        """
        return self.synthesize_speech(
            text=ssml_text,
            voice_id=voice_id,
            output_format=output_format,
            engine=engine,
            language_code=language_code,
            output_s3_bucket=output_s3_bucket,
            output_s3_key_prefix=output_s3_key_prefix,
            sns_topic_arn=sns_topic_arn,
            sample_rate=sample_rate,
            text_type="ssml",
            return_audio_stream=return_audio_stream
        )
    
    # =========================================================================
    # Voice Management
    # =========================================================================
    
    def list_voices(
        self,
        language_code: Optional[str] = None,
        engine: Optional[str] = None,
        include_additional_language_codes: bool = False
    ) -> List[Voice]:
        """
        List available Polly voices.
        
        Args:
            language_code: Filter by language code (e.g., 'en-US')
            engine: Filter by engine type ('standard', 'neural', 'generative')
            include_additional_language_codes: Include voices that can speak additional languages
            
        Returns:
            List of Voice objects
        """
        try:
            params = {}
            if language_code:
                params["LanguageCode"] = language_code
            
            if engine:
                params["Engine"] = engine
            
            response = self.polly_client.describe_voices(**params)
            
            voices = []
            for voice_data in response.get("Voices", []):
                voice = Voice(
                    id=voice_data.get("Id"),
                    name=voice_data.get("Name"),
                    language_code=voice_data.get("LanguageCode"),
                    language_name=voice_data.get("LanguageName"),
                    gender=voice_data.get("Gender"),
                    engine=voice_data.get("SupportedEngines", [None])[0] if voice_data.get("SupportedEngines") else None,
                    supported_engines=voice_data.get("SupportedEngines", [])
                )
                voices.append(voice)
            
            self._record_metric("ListVoicesRequests", 1, "Count", {"LanguageCode": language_code or "all"})
            return voices
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to list voices: {e}")
            raise
    
    def get_voice(
        self,
        voice_id: str,
        engine: Optional[str] = None
    ) -> Voice:
        """
        Get details for a specific voice.
        
        Args:
            voice_id: Voice ID
            engine: Engine to check support for
            
        Returns:
            Voice object
        """
        try:
            params = {"VoiceId": voice_id}
            if engine:
                params["Engine"] = engine
            
            response = self.polly_client.describe_voices(**params)
            
            if not response.get("Voices"):
                raise ValueError(f"Voice '{voice_id}' not found")
            
            voice_data = response["Voices"][0]
            return Voice(
                id=voice_data.get("Id"),
                name=voice_data.get("Name"),
                language_code=voice_data.get("LanguageCode"),
                language_name=voice_data.get("LanguageName"),
                gender=voice_data.get("Gender"),
                engine=voice_data.get("SupportedEngines", [None])[0] if voice_data.get("SupportedEngines") else None,
                supported_engines=voice_data.get("SupportedEngines", [])
            )
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get voice {voice_id}: {e}")
            raise
    
    def get_available_neural_voices(self, language_code: Optional[str] = None) -> List[Voice]:
        """
        Get list of neural voices.
        
        Args:
            language_code: Filter by language code
            
        Returns:
            List of neural Voice objects
        """
        return self.list_voices(
            language_code=language_code,
            engine="neural"
        )
    
    def get_available_generative_voices(self, language_code: Optional[str] = None) -> List[Voice]:
        """
        Get list of generative voices.
        
        Args:
            language_code: Filter by language code
            
        Returns:
            List of generative Voice objects
        """
        return self.list_voices(
            language_code=language_code,
            engine="generative"
        )
    
    # =========================================================================
    # Lexicon Management
    # =========================================================================
    
    def put_lexicon(
        self,
        name: str,
        content: str,
        language_code: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Store a pronunciation lexicon.
        
        Args:
            name: Lexicon name
            content: PLS lexicon content (XML)
            language_code: Language code for the lexicon
            
        Returns:
            Response from API
        """
        try:
            params = {
                "Name": name,
                "Content": content
            }
            
            response = self.polly_client.put_lexicon(**params)
            logger.info(f"Lexicon '{name}' stored successfully")
            self._record_metric("LexiconsStored", 1, "Count")
            return response
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to store lexicon '{name}': {e}")
            raise
    
    def get_lexicon(self, name: str) -> Lexicon:
        """
        Retrieve a pronunciation lexicon.
        
        Args:
            name: Lexicon name
            
        Returns:
            Lexicon object
        """
        try:
            response = self.polly_client.get_lexicon(Name=name)
            lexicon_data = response.get("Lexicon", {})
            
            return Lexicon(
                name=lexicon_data.get("Name"),
                content=lexicon_data.get("Content"),
                alphabet=lexicon_data.get("Alphabet", "ipa"),
                language_code=lexicon_data.get("LanguageCode")
            )
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get lexicon '{name}': {e}")
            raise
    
    def delete_lexicon(self, name: str) -> Dict[str, Any]:
        """
        Delete a pronunciation lexicon.
        
        Args:
            name: Lexicon name
            
        Returns:
            Response from API
        """
        try:
            response = self.polly_client.delete_lexicon(Name=name)
            logger.info(f"Lexicon '{name}' deleted")
            self._record_metric("LexiconsDeleted", 1, "Count")
            return response
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to delete lexicon '{name}': {e}")
            raise
    
    def list_lexicons(self) -> List[Lexicon]:
        """
        List all pronunciation lexicons.
        
        Returns:
            List of Lexicon objects
        """
        try:
            response = self.polly_client.list_lexicons()
            
            lexicons = []
            for lexicon_data in response.get("Lexicons", []):
                lexicon = Lexicon(
                    name=lexicon_data.get("Name"),
                    content=lexicon_data.get("Content", ""),
                    alphabet=lexicon_data.get("Alphabet", "ipa"),
                    language_code=lexicon_data.get("LanguageCode")
                )
                lexicons.append(lexicon)
            
            self._record_metric("ListLexiconsRequests", 1, "Count")
            return lexicons
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to list lexicons: {e}")
            raise
    
    # =========================================================================
    # Speech Marks
    # =========================================================================
    
    def get_speech_marks(
        self,
        text: str,
        output_format: str = "json",
        voice_id: str = "Joanna",
        engine: str = "neural",
        speech_mark_types: Optional[List[str]] = None,
        language_code: Optional[str] = None
    ) -> List[SpeechMark]:
        """
        Get speech marks for text.
        
        Args:
            text: Text to analyze
            output_format: Output format ('json', 'v2' is legacy)
            voice_id: Voice ID
            engine: Engine type
            speech_mark_types: Types of marks ('sentence', 'word', 'viseme', 'ssml')
            language_code: Language code
            
        Returns:
            List of SpeechMark objects
        """
        if speech_mark_types is None:
            speech_mark_types = ["word", "sentence", "viseme"]
        
        try:
            params = {
                "Text": text,
                "OutputFormat": "Json",
                "VoiceId": voice_id,
                "Engine": engine,
                "SpeechMarkTypes": speech_mark_types
            }
            
            if language_code:
                params["LanguageCode"] = language_code
            
            response = self.polly_client.synthesize_speech(**params)
            
            audio_stream = response.get("AudioStream")
            if audio_stream:
                marks_data = audio_stream.read().decode('utf-8')
                marks = []
                for line in marks_data.strip().split('\n'):
                    if line:
                        try:
                            mark_data = json.loads(line)
                            mark = SpeechMark(
                                mark_type=mark_data.get("type"),
                                start=mark_data.get("time", 0),
                                end=mark_data.get("time", 0) + mark_data.get("duration", 0),
                                value=mark_data.get("value", "")
                            )
                            marks.append(mark)
                        except json.JSONDecodeError:
                            continue
                
                self._record_metric("SpeechMarksRequests", 1, "Count", {"Engine": engine})
                return marks
            
            return []
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get speech marks: {e}")
            raise
    
    # =========================================================================
    # Long-Form / Async Tasks
    # =========================================================================
    
    def start_speech_synthesis_task(
        self,
        text: str,
        voice_id: str = "Joanna",
        output_format: str = "mp3",
        engine: str = "neural",
        language_code: Optional[str] = None,
        output_s3_bucket: str = None,
        output_s3_key_prefix: Optional[str] = None,
        sns_topic_arn: Optional[str] = None,
        sample_rate: Optional[str] = None,
        text_type: str = "text",
        lexicon_names: Optional[List[str]] = None
    ) -> SynthesisTask:
        """
        Start a long-form speech synthesis task.
        
        Args:
            text: Text to synthesize
            voice_id: Voice ID
            output_format: Output format
            engine: Engine type
            language_code: Language code
            output_s3_bucket: S3 bucket for output (required for long-form)
            output_s3_key_prefix: S3 key prefix
            sns_topic_arn: SNS topic for notifications
            sample_rate: Audio sample rate
            text_type: 'text' or 'ssml'
            lexicon_names: Lexicons to apply
            
        Returns:
            SynthesisTask object
        """
        if not output_s3_bucket:
            raise ValueError("output_s3_bucket is required for long-form synthesis")
        
        try:
            params = {
                "Text": text,
                "VoiceId": voice_id,
                "OutputFormat": output_format,
                "Engine": engine,
                "OutputS3BucketName": output_s3_bucket
            }
            
            if language_code:
                params["LanguageCode"] = language_code
            
            if sample_rate:
                params["SampleRate"] = sample_rate
            
            if text_type == "ssml":
                params["TextType"] = "ssml"
            
            if output_s3_key_prefix:
                params["OutputS3KeyPrefix"] = output_s3_key_prefix
            
            if sns_topic_arn:
                params["SnsTopicArn"] = sns_topic_arn
            
            if lexicon_names:
                params["LexiconNames"] = lexicon_names
            
            task_id = str(uuid.uuid4())
            params["TaskId"] = task_id
            
            response = self.polly_client.start_speech_synthesis_task(**params)
            task_data = response.get("SynthesisTask", {})
            
            task = SynthesisTask(
                task_id=task_id,
                status=task_data.get("TaskStatus", "scheduled"),
                output_uri=task_data.get("OutputUri"),
                synthesis_request=task_data.get("SynthesisRequest"),
                creation_time=task_data.get("CreationTime"),
                completion_time=task_data.get("CompletionTime"),
                error_message=task_data.get("TaskReason")
            )
            
            self._active_tasks[task_id] = task
            self._record_metric("LongFormSynthesisTasksStarted", 1, "Count", {"Engine": engine})
            logger.info(f"Started long-form synthesis task: {task_id}")
            
            return task
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to start speech synthesis task: {e}")
            raise
    
    def get_speech_synthesis_task(self, task_id: str) -> SynthesisTask:
        """
        Get status of a speech synthesis task.
        
        Args:
            task_id: Task ID
            
        Returns:
            SynthesisTask object
        """
        try:
            response = self.polly_client.get_speech_synthesis_task(TaskId=task_id)
            task_data = response.get("SynthesisTask", {})
            
            return SynthesisTask(
                task_id=task_id,
                status=task_data.get("TaskStatus", "unknown"),
                output_uri=task_data.get("OutputUri"),
                synthesis_request=task_data.get("SynthesisRequest"),
                creation_time=task_data.get("CreationTime"),
                completion_time=task_data.get("CompletionTime"),
                error_message=task_data.get("TaskReason")
            )
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get synthesis task {task_id}: {e}")
            raise
    
    def list_speech_synthesis_tasks(
        self,
        status: Optional[str] = None,
        max_results: int = 100
    ) -> List[SynthesisTask]:
        """
        List speech synthesis tasks.
        
        Args:
            status: Filter by status ('scheduled', 'inProgress', 'completed', 'failed')
            max_results: Maximum number of results
            
        Returns:
            List of SynthesisTask objects
        """
        try:
            params = {"MaxResults": max_results}
            if status:
                params["TaskStatus"] = status
            
            response = self.polly_client.list_speech_synthesis_tasks(**params)
            
            tasks = []
            for task_data in response.get("SynthesisTasks", []):
                task = SynthesisTask(
                    task_id=task_data.get("TaskId", ""),
                    status=task_data.get("TaskStatus", "unknown"),
                    output_uri=task_data.get("OutputUri"),
                    synthesis_request=task_data.get("SynthesisRequest"),
                    creation_time=task_data.get("CreationTime"),
                    completion_time=task_data.get("CompletionTime"),
                    error_message=task_data.get("TaskReason")
                )
                tasks.append(task)
            
            return tasks
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to list synthesis tasks: {e}")
            raise
    
    def delete_speech_synthesis_task(self, task_id: str) -> Dict[str, Any]:
        """
        Delete a speech synthesis task.
        
        Args:
            task_id: Task ID
            
        Returns:
            Response from API
        """
        try:
            response = self.polly_client.delete_speech_synthesis_task(TaskId=task_id)
            
            if task_id in self._active_tasks:
                del self._active_tasks[task_id]
            
            self._record_metric("SynthesisTasksDeleted", 1, "Count")
            return response
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to delete synthesis task {task_id}: {e}")
            raise
    
    # =========================================================================
    # SNS Notifications
    # =========================================================================
    
    def subscribe_to_synthesis_completion(
        self,
        sns_topic_arn: str,
        callback: Callable[[SynthesisTask], None]
    ) -> str:
        """
        Subscribe to synthesis task completion notifications.
        
        Args:
            sns_topic_arn: SNS topic ARN
            callback: Callback function receiving SynthesisTask
            
        Returns:
            Subscription ARN
        """
        subscription_id = str(uuid.uuid4())
        self._task_callbacks[subscription_id] = callback
        
        logger.info(f"Subscribed to synthesis completion: {subscription_id}")
        return subscription_id
    
    def unsubscribe_from_synthesis_completion(self, subscription_id: str):
        """
        Unsubscribe from synthesis notifications.
        
        Args:
            subscription_id: Subscription ID
        """
        if subscription_id in self._task_callbacks:
            del self._task_callbacks[subscription_id]
            logger.info(f"Unsubscribed from synthesis completion: {subscription_id}")
    
    def create_sns_topic_for_polly(
        self,
        topic_name: str,
        subscription_lambda_arn: Optional[str] = None
    ) -> str:
        """
        Create an SNS topic for Polly notifications.
        
        Args:
            topic_name: Topic name
            subscription_lambda_arn: Lambda ARN to subscribe
            
        Returns:
            Topic ARN
        """
        try:
            response = self.sns_client.create_topic(Name=topic_name)
            topic_arn = response.get("TopicArn")
            
            if subscription_lambda_arn:
                self.sns_client.subscribe(
                    TopicArn=topic_arn,
                    Protocol="lambda",
                    Endpoint=subscription_lambda_arn
                )
            
            self._record_metric("SNSTopicsCreated", 1, "Count")
            return topic_arn
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to create SNS topic: {e}")
            raise
    
    # =========================================================================
    # CloudWatch Metrics
    # =========================================================================
    
    def get_polly_metrics(
        self,
        metric_name: str,
        period: int = 60,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        statistics: List[str] = None
    ) -> Dict[str, Any]:
        """
        Get Polly CloudWatch metrics.
        
        Args:
            metric_name: Name of the metric
            period: Period in seconds
            start_time: Start time
            end_time: End time
            statistics: List of statistics ('SampleCount', 'Average', 'Sum', 'Minimum', 'Maximum')
            
        Returns:
            Metric data
        """
        if statistics is None:
            statistics = ["SampleCount", "Average"]
        
        if end_time is None:
            end_time = datetime.utcnow()
        if start_time is None:
            start_time = end_time - timedelta(hours=1)
        
        try:
            response = self.cloudwatch_client.get_metric_statistics(
                Namespace="AWS/Polly",
                MetricName=metric_name,
                StartTime=start_time,
                EndTime=end_time,
                Period=period,
                Statistics=statistics
            )
            
            return {
                "metric_name": metric_name,
                "period": period,
                "statistics": statistics,
                "data_points": response.get("Datapoints", [])
            }
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get Polly metrics: {e}")
            raise
    
    def enable_detailed_metrics(self) -> Dict[str, Any]:
        """
        Enable detailed CloudWatch metrics for Polly.
        
        Returns:
            Response from API
        """
        try:
            response = self.cloudwatch_client.put_metric_filter(
                FilterName="PollyDetailedMetrics",
                FilterPattern="",
                MetricTransformations=[
                    {
                        "MetricName": "SynthesizeSpeechRequests",
                        "MetricNamespace": "AWS/Polly",
                        "MetricValue": "1"
                    }
                ]
            )
            self._record_metric("DetailedMetricsEnabled", 1, "Count")
            return response
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to enable detailed metrics: {e}")
            raise
    
    def get_request_metrics(
        self,
        voice_id: Optional[str] = None,
        engine: Optional[str] = None,
        period: int = 300,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Get synthesis request metrics.
        
        Args:
            voice_id: Filter by voice ID
            engine: Filter by engine
            period: Period in seconds
            start_time: Start time
            end_time: End time
            
        Returns:
            Metrics data
        """
        if end_time is None:
            end_time = datetime.utcnow()
        if start_time is None:
            start_time = end_time - timedelta(hours=24)
        
        dimensions = []
        if voice_id:
            dimensions.append({"Name": "VoiceId", "Value": voice_id})
        if engine:
            dimensions.append({"Name": "Engine", "Value": engine})
        
        try:
            kwargs = {
                "Namespace": "AWS/Polly",
                "MetricName": "SynthesizeSpeechRequests",
                "StartTime": start_time,
                "EndTime": end_time,
                "Period": period,
                "Statistics": ["SampleCount", "Sum"]
            }
            
            if dimensions:
                kwargs["Dimensions"] = dimensions
            
            response = self.cloudwatch_client.get_metric_statistics(**kwargs)
            return response.get("Datapoints", [])
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get request metrics: {e}")
            raise
    
    def get_latency_metrics(
        self,
        engine: Optional[str] = None,
        period: int = 300,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Get latency metrics for Polly requests.
        
        Args:
            engine: Filter by engine
            period: Period in seconds
            start_time: Start time
            end_time: End time
            
        Returns:
            Latency metrics data
        """
        if end_time is None:
            end_time = datetime.utcnow()
        if start_time is None:
            start_time = end_time - timedelta(hours=24)
        
        kwargs = {
            "Namespace": "AWS/Polly",
            "MetricName": "SynthesisLatency",
            "StartTime": start_time,
            "EndTime": end_time,
            "Period": period,
            "Statistics": ["Average", "Minimum", "Maximum"]
        }
        
        if engine:
            kwargs["Dimensions"] = [{"Name": "Engine", "Value": engine}]
        
        try:
            response = self.cloudwatch_client.get_metric_statistics(**kwargs)
            return response.get("Datapoints", [])
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get latency metrics: {e}")
            raise
    
    # =========================================================================
    # Utility Methods
    # =========================================================================
    
    def save_audio_to_file(self, audio_stream: bytes, file_path: str):
        """
        Save audio stream to file.
        
        Args:
            audio_stream: Audio bytes
            file_path: Output file path
        """
        with open(file_path, 'wb') as f:
            f.write(audio_stream)
        logger.info(f"Saved audio to {file_path}")
    
    def audio_to_base64(self, audio_stream: bytes) -> str:
        """
        Convert audio stream to base64 string.
        
        Args:
            audio_stream: Audio bytes
            
        Returns:
            Base64 encoded string
        """
        return base64.b64encode(audio_stream).decode('utf-8')
    
    def base64_to_audio(self, base64_str: str) -> bytes:
        """
        Convert base64 string to audio stream.
        
        Args:
            base64_str: Base64 encoded audio
            
        Returns:
            Audio bytes
        """
        return base64.b64decode(base64_str)
    
    def get_supported_engines_for_voice(self, voice_id: str) -> List[str]:
        """
        Get supported engines for a voice.
        
        Args:
            voice_id: Voice ID
            
        Returns:
            List of supported engine types
        """
        voice = self.get_voice(voice_id)
        return voice.supported_engines
    
    def is_neural_available(self, voice_id: str) -> bool:
        """
        Check if neural engine is available for a voice.
        
        Args:
            voice_id: Voice ID
            
        Returns:
            True if neural is available
        """
        engines = self.get_supported_engines_for_voice(voice_id)
        return "neural" in engines
    
    def is_generative_available(self, voice_id: str) -> bool:
        """
        Check if generative engine is available for a voice.
        
        Args:
            voice_id: Voice ID
            
        Returns:
            True if generative is available
        """
        engines = self.get_supported_engines_for_voice(voice_id)
        return "generative" in engines
    
    def create_sample_lexicon(self, name: str, entries: Dict[str, str]) -> Lexicon:
        """
        Create a sample PLS lexicon from entries.
        
        Args:
            name: Lexicon name
            entries: Dictionary mapping words to pronunciations
            
        Returns:
            Lexicon object
        """
        lexicon_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<lexicon version="1.0" 
         xmlns="http://www.w3.org/2005/01/pronunciation-lexicon"
         alphabet="ipa"
         xml:lang="en-US">
  <lexeme>
    <grapheme></grapheme>
    <phoneme></phoneme>
  </lexeme>
</lexicon>"""
        
        graphemes = []
        phonemes = []
        for word, pronunciation in entries.items():
            graphemes.append(f"    <grapheme>{word}</grapheme>")
            phonemes.append(f"    <phoneme>{pronunciation}</phoneme>")
        
        full_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<lexicon version="1.0" 
         xmlns="http://www.w3.org/2005/01/pronunciation-lexicon"
         alphabet="ipa"
         xml:lang="en-US">
"""
        for word, pronunciation in entries.items():
            full_content += f"""  <lexeme>
    <grapheme>{word}</grapheme>
    <phoneme>{pronunciation}</phoneme>
  </lexeme>
"""
        full_content += "</lexicon>"
        
        return Lexicon(name=name, content=full_content, alphabet="ipa")


from datetime import timedelta


class PollyIntegrationError(Exception):
    """Custom exception for Polly integration errors."""
    pass
