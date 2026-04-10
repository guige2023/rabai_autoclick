"""
AWS Rekognition Integration Module for Workflow System

Implements a RekognitionIntegration class with:
1. Face detection: Detect faces in images
2. Face comparison: Compare faces
3. Face search: Search faces in collections
4. Label detection: Detect labels in images/video
5. Text detection: Detect text in images
6. Celebrity recognition: Recognize celebrities
7. Content moderation: Moderate content
8. Video analysis: Analyze videos
9. PPE detection: Personal protective equipment
10. CloudWatch integration: Recognition metrics

Commit: 'feat(aws-rekognition): add Amazon Rekognition with face detection, face comparison, face search, label detection, text detection, celebrity recognition, content moderation, video analysis, PPE detection, CloudWatch'
"""

import uuid
import json
import time
import logging
import hashlib
import subprocess
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional, Set, Union, Type, Awaitable
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum
import copy
import threading
import os
import re
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


class FaceDetectionAttributes(Enum):
    """Face detection attribute types."""
    DEFAULT = "DEFAULT"
    ALL = "ALL"


class FaceMatchThreshold(Enum):
    """Face match threshold levels."""
    LOW = 50
    MEDIUM = 70
    HIGH = 80
    VERY_HIGH = 90


class ImageOrientationCorrection(Enum):
    """Image orientation correction modes."""
    AUTO = "AUTO"
    MANUAL = "MANUAL"


class LabelDetectionMode(Enum):
    """Label detection mode types."""
    BASIC = "BASIC"
    ENHANCED = "ENHANCED"


class ContentModerationConfidence(Enum):
    """Content moderation confidence thresholds."""
    LOW = 0
    MEDIUM = 50
    HIGH = 75
    VERY_HIGH = 90


class PPEType(Enum):
    """Personal protective equipment types."""
    FACE_COVER = "FACE_COVER"
    HEAD_COVER = "HEAD_COVER"
    HAND_COVER = "HAND_COVER"
    FACE_PROTECTION = "FACE_PROTECTION"
    PROTECTIVE_BODYwear = "PROTECTIVE_BODYWEAR"


class VideoAnalysisStatus(Enum):
    """Video analysis job status."""
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    STOPPED = "STOPPED"


class CollectionStatus(Enum):
    """Collection status types."""
    ACTIVE = "ACTIVE"
    CREATING = "CREATING"
    DELETING = "DELETING"


@dataclass
class BoundingBox:
    """Bounding box coordinates for detected items."""
    Width: float
    Height: float
    Left: float
    Top: float


@dataclass
class FaceDetail:
    """Face detail information."""
    FaceId: str
    BoundingBox: BoundingBox
    Confidence: float
    AgeRange: Optional[Dict[str, int]] = None
    Gender: Optional[Dict[str, str]] = None
    Emotions: Optional[List[Dict[str, Any]]] = None
    Landmarks: Optional[List[Dict[str, Any]]] = None
    Pose: Optional[Dict[str, float]] = None
    Quality: Optional[Dict[str, float]] = None


@dataclass
class FaceSearchResult:
    """Face search result."""
    FaceId: str
    FaceMatches: List[Dict[str, Any]]
    Face: Optional[Dict[str, Any]] = None


@dataclass
class LabelDetection:
    """Label detection result."""
    Name: str
    Confidence: float
    Parents: List[Dict[str, str]]
    Categories: Optional[List[str]] = None


@dataclass
class TextDetection:
    """Text detection result."""
    DetectedText: str
    Type: str
    Confidence: float
    BoundingBox: BoundingBox


@dataclass
class CelebrityRecognition:
    """Celebrity recognition result."""
    Name: str
    Urls: List[str]
    Face: Dict[str, Any]
    Confidence: float
    MatchedBoundingBox: BoundingBox


@dataclass
class ModerationLabel:
    """Content moderation label."""
    Name: str
    Confidence: float
    ParentName: Optional[str] = None
    Categories: Optional[List[str]] = None


@dataclass
class PPEDetection:
    """PPE detection result."""
    Type: str
    BoundingBox: BoundingBox
    Confidence: float
    Covers: Dict[str, Any]


@dataclass
class VideoAnalysisResult:
    """Video analysis result."""
    JobId: str
    Status: str
    StartTime: datetime
    StopTime: Optional[datetime]
    FaceSearchResults: Optional[List[Dict[str, Any]]] = None
    LabelDetectionResults: Optional[List[Dict[str, Any]]] = None
    ContentModerationResults: Optional[List[Dict[str, Any]]] = None
    TextDetectionResults: Optional[List[Dict[str, Any]]] = None
    CelebrityRecognitionResults: Optional[List[Dict[str, Any]]] = None
    ErrorMessage: Optional[str] = None


@dataclass
class RecognitionMetrics:
    """Recognition metrics for CloudWatch."""
    Timestamp: datetime
    FaceDetections: int = 0
    FaceComparisons: int = 0
    FaceSearches: int = 0
    LabelDetections: int = 0
    TextDetections: int = 0
    CelebrityRecognitions: int = 0
    ModerationFlags: int = 0
    PPEDetections: int = 0
    VideoAnalyses: int = 0
    Errors: int = 0


class RekognitionIntegration:
    """
    AWS Rekognition integration for image and video analysis.
    
    Features:
    1. Face detection: Detect faces in images
    2. Face comparison: Compare faces
    3. Face search: Search faces in collections
    4. Label detection: Detect labels in images/video
    5. Text detection: Detect text in images
    6. Celebrity recognition: Recognize celebrities
    7. Content moderation: Moderate content
    8. Video analysis: Analyze videos
    9. PPE detection: Personal protective equipment
    10. CloudWatch integration: Recognition metrics
    """
    
    def __init__(
        self,
        region_name: str = "us-east-1",
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        enable_cloudwatch: bool = True,
        cloudwatch_namespace: str = "Rekognition/Metrics",
        **kwargs
    ):
        """
        Initialize Rekognition integration.
        
        Args:
            region_name: AWS region
            aws_access_key_id: AWS access key ID
            aws_secret_access_key: AWS secret access key
            aws_session_token: AWS session token
            enable_cloudwatch: Enable CloudWatch metrics
            cloudwatch_namespace: CloudWatch namespace for metrics
        """
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
        self.enable_cloudwatch = enable_cloudwatch
        self.cloudwatch_namespace = cloudwatch_namespace
        
        self._client = None
        self._cloudwatch_client = None
        self._collections: Dict[str, str] = {}
        self._metrics_buffer: List[RecognitionMetrics] = []
        self._lock = threading.Lock()
        
        if BOTO3_AVAILABLE:
            self._initialize_clients()
    
    def _initialize_clients(self):
        """Initialize AWS clients."""
        try:
            client_kwargs = {
                "region_name": self.region_name
            }
            if self.aws_access_key_id and self.aws_secret_access_key:
                client_kwargs["aws_access_key_id"] = self.aws_access_key_id
                client_kwargs["aws_secret_access_key"] = self.aws_secret_access_key
                if self.aws_session_token:
                    client_kwargs["aws_session_token"] = self.aws_session_token
            
            self._client = boto3.client("rekognition", **client_kwargs)
            
            if self.enable_cloudwatch:
                self._cloudwatch_client = boto3.client("cloudwatch", **client_kwargs)
            
            logger.info(f"Rekognition client initialized for region {self.region_name}")
        except Exception as e:
            logger.error(f"Failed to initialize Rekognition client: {e}")
    
    def _record_metric(self, metric_type: str, value: int = 1, error: bool = False):
        """Record a metric for CloudWatch."""
        if not self.enable_cloudwatch:
            return
        
        with self._lock:
            metric = RecognitionMetrics(
                Timestamp=datetime.utcnow(),
                **{metric_type: value, "Errors": 1 if error else 0}
            )
            self._metrics_buffer.append(metric)
            
            if len(self._metrics_buffer) >= 10:
                self._flush_metrics()
    
    def _flush_metrics(self):
        """Flush metrics to CloudWatch."""
        if not self._cloudwatch_client or not self._metrics_buffer:
            return
        
        try:
            metrics_data = []
            for metric in self._metrics_buffer:
                for field in [
                    "FaceDetections", "FaceComparisons", "FaceSearches",
                    "LabelDetections", "TextDetections", "CelebrityRecognitions",
                    "ModerationFlags", "PPEDetections", "VideoAnalyses", "Errors"
                ]:
                    value = getattr(metric, field, 0)
                    if value > 0:
                        metrics_data.append({
                            "MetricName": field,
                            "Value": value,
                            "Timestamp": metric.Timestamp,
                            "Unit": "Count"
                        })
            
            if metrics_data:
                self._cloudwatch_client.put_metric_data(
                    Namespace=self.cloudwatch_namespace,
                    MetricData=metrics_data
                )
            
            self._metrics_buffer.clear()
        except Exception as e:
            logger.error(f"Failed to flush metrics to CloudWatch: {e}")
    
    # ========================================================================
    # 1. Face Detection
    # ========================================================================
    
    def detect_faces(
        self,
        image_source: Union[str, bytes, Dict],
        attributes: List[str] = None,
        enable_cloudwatch: bool = True
    ) -> Dict[str, Any]:
        """
        Detect faces in an image.
        
        Args:
            image_source: Image source (S3 object, bytes, or base64)
            attributes: Face attributes to detect (DEFAULT, AGE, GENDER, etc.)
            enable_cloudwatch: Enable CloudWatch metrics
            
        Returns:
            Dictionary containing face details
        """
        if not BOTO3_AVAILABLE:
            logger.warning("boto3 not available, returning mock data")
            return self._mock_face_detection()
        
        try:
            image_payload = self._prepare_image(image_source)
            
            detect_args = {
                "Image": image_payload,
                "Attributes": attributes or ["DEFAULT"]
            }
            
            response = self._client.detect_faces(**detect_args)
            
            if enable_cloudwatch and self.enable_cloudwatch:
                self._record_metric("FaceDetections", len(response.get("FaceDetails", [])))
            
            return {
                "success": True,
                "face_details": response.get("FaceDetails", []),
                "orientation_corrected": response.get("OrientationCorrection"),
                "count": len(response.get("FaceDetails", []))
            }
        except ClientError as e:
            logger.error(f"Face detection failed: {e}")
            if enable_cloudwatch and self.enable_cloudwatch:
                self._record_metric("FaceDetections", error=True)
            return {"success": False, "error": str(e)}
    
    def _mock_face_detection(self) -> Dict[str, Any]:
        """Generate mock face detection results."""
        return {
            "success": True,
            "face_details": [
                {
                    "BoundingBox": {"Width": 0.5, "Height": 0.5, "Left": 0.25, "Top": 0.25},
                    "Confidence": 99.5,
                    "AgeRange": {"Low": 30, "High": 40},
                    "Gender": {"Value": "Male", "Confidence": 99.0},
                    "Emotions": [{"Type": "HAPPY", "Confidence": 95.0}]
                }
            ],
            "orientation_corrected": "ROTATE_0",
            "count": 1
        }
    
    # ========================================================================
    # 2. Face Comparison
    # ========================================================================
    
    def compare_faces(
        self,
        source_image: Union[str, bytes, Dict],
        target_image: Union[str, bytes, Dict],
        similarity_threshold: float = 80.0,
        enable_cloudwatch: bool = True
    ) -> Dict[str, Any]:
        """
        Compare faces between two images.
        
        Args:
            source_image: Source image
            target_image: Target image to compare against
            similarity_threshold: Minimum similarity threshold (0-100)
            enable_cloudwatch: Enable CloudWatch metrics
            
        Returns:
            Dictionary containing face match results
        """
        if not BOTO3_AVAILABLE:
            logger.warning("boto3 not available, returning mock data")
            return self._mock_face_comparison()
        
        try:
            source_payload = self._prepare_image(source_image)
            target_payload = self._prepare_image(target_image)
            
            response = self._client.compare_faces(
                SourceImage=source_payload,
                TargetImage=target_payload,
                SimilarityThreshold=similarity_threshold
            )
            
            matches = response.get("FaceMatches", [])
            unmatches = response.get("UnmatchedFaces", [])
            
            if enable_cloudwatch and self.enable_cloudwatch:
                self._record_metric("FaceComparisons")
            
            return {
                "success": True,
                "face_matches": matches,
                "unmatched_faces": unmatches,
                "source_face": response.get("SourceFace"),
                "match_count": len(matches),
                "similarity": matches[0].get("Similarity") if matches else 0
            }
        except ClientError as e:
            logger.error(f"Face comparison failed: {e}")
            if enable_cloudwatch and self.enable_cloudwatch:
                self._record_metric("FaceComparisons", error=True)
            return {"success": False, "error": str(e)}
    
    def _mock_face_comparison(self) -> Dict[str, Any]:
        """Generate mock face comparison results."""
        return {
            "success": True,
            "face_matches": [
                {"Similarity": 95.5, "Face": {"FaceId": "mock-face-id"}}
            ],
            "unmatched_faces": [],
            "source_face": {"FaceId": "mock-source-face-id"},
            "match_count": 1,
            "similarity": 95.5
        }
    
    # ========================================================================
    # 3. Face Search
    # ========================================================================
    
    def create_collection(
        self,
        collection_id: str,
        description: str = "",
        tags: Dict[str, str] = None,
        enable_cloudwatch: bool = True
    ) -> Dict[str, Any]:
        """
        Create a face collection.
        
        Args:
            collection_id: Unique collection identifier
            description: Collection description
            tags: Tags for the collection
            enable_cloudwatch: Enable CloudWatch metrics
            
        Returns:
            Dictionary containing collection creation result
        """
        if not BOTO3_AVAILABLE:
            logger.warning("boto3 not available, returning mock data")
            return {"success": True, "collection_id": collection_id}
        
        try:
            kwargs = {"CollectionId": collection_id}
            if description:
                kwargs["Description"] = description
            
            response = self._client.create_collection(**kwargs)
            
            self._collections[collection_id] = response.get("CollectionArn", "")
            
            if tags and not BOTO3_AVAILABLE:
                try:
                    self._client.tag_resource(
                        ResourceArn=response.get("CollectionArn"),
                        Tags=tags
                    )
                except Exception as tag_error:
                    logger.warning(f"Failed to tag collection: {tag_error}")
            
            return {
                "success": True,
                "collection_id": collection_id,
                "collection_arn": response.get("CollectionArn"),
                "status_code": response.get("StatusCode")
            }
        except ClientError as e:
            logger.error(f"Collection creation failed: {e}")
            return {"success": False, "error": str(e)}
    
    def index_faces(
        self,
        collection_id: str,
        image_source: Union[str, bytes, Dict],
        external_image_id: str = None,
        detection_attributes: List[str] = None,
        enable_cloudwatch: bool = True
    ) -> Dict[str, Any]:
        """
        Add faces to a collection.
        
        Args:
            collection_id: Collection ID
            image_source: Image source
            external_image_id: External image ID
            detection_attributes: Face attributes to detect
            enable_cloudwatch: Enable CloudWatch metrics
            
        Returns:
            Dictionary containing indexed face results
        """
        if not BOTO3_AVAILABLE:
            logger.warning("boto3 not available, returning mock data")
            return self._mock_index_faces(collection_id)
        
        try:
            image_payload = self._prepare_image(image_source)
            
            kwargs = {
                "CollectionId": collection_id,
                "Image": image_payload
            }
            
            if external_image_id:
                kwargs["ExternalImageId"] = external_image_id
            
            if detection_attributes:
                kwargs["DetectionAttributes"] = detection_attributes
            
            response = self._client.index_faces(**kwargs)
            
            face_records = response.get("FaceRecords", [])
            
            if enable_cloudwatch and self.enable_cloudwatch:
                self._record_metric("FaceSearches", len(face_records))
            
            return {
                "success": True,
                "face_records": face_records,
                "indexed_faces_count": len(face_records),
                "unindexed_faces": response.get("UnindexedFaces", [])
            }
        except ClientError as e:
            logger.error(f"Face indexing failed: {e}")
            if enable_cloudwatch and self.enable_cloudwatch:
                self._record_metric("FaceSearches", error=True)
            return {"success": False, "error": str(e)}
    
    def search_faces(
        self,
        collection_id: str,
        face_id: str = None,
        image_source: Union[str, bytes, Dict] = None,
        max_faces: int = 10,
        face_match_threshold: float = 80.0,
        enable_cloudwatch: bool = True
    ) -> Dict[str, Any]:
        """
        Search for faces in a collection.
        
        Args:
            collection_id: Collection ID
            face_id: Face ID to search for
            image_source: Image source (alternative to face_id)
            max_faces: Maximum number of faces to return
            face_match_threshold: Face match threshold (0-100)
            enable_cloudwatch: Enable CloudWatch metrics
            
        Returns:
            Dictionary containing search results
        """
        if not BOTO3_AVAILABLE:
            logger.warning("boto3 not available, returning mock data")
            return self._mock_face_search()
        
        try:
            kwargs = {
                "CollectionId": collection_id,
                "MaxFaces": max_faces,
                "FaceMatchThreshold": face_match_threshold
            }
            
            if face_id:
                kwargs["FaceId"] = face_id
            elif image_source:
                image_payload = self._prepare_image(image_source)
                kwargs["Image"] = image_payload
            else:
                return {"success": False, "error": "Either face_id or image_source required"}
            
            response = self._client.search_faces(**kwargs)
            
            face_matches = response.get("FaceMatches", [])
            
            if enable_cloudwatch and self.enable_cloudwatch:
                self._record_metric("FaceSearches", len(face_matches))
            
            return {
                "success": True,
                "face_matches": face_matches,
                "searched_face": response.get("SearchedFace"),
                "matched_faces_count": len(face_matches),
                "face_model_version": response.get("FaceModelVersion")
            }
        except ClientError as e:
            logger.error(f"Face search failed: {e}")
            if enable_cloudwatch and self.enable_cloudwatch:
                self._record_metric("FaceSearches", error=True)
            return {"success": False, "error": str(e)}
    
    def delete_collection(self, collection_id: str) -> Dict[str, Any]:
        """
        Delete a face collection.
        
        Args:
            collection_id: Collection ID to delete
            
        Returns:
            Dictionary containing deletion result
        """
        if not BOTO3_AVAILABLE:
            logger.warning("boto3 not available")
            return {"success": True, "collection_id": collection_id}
        
        try:
            self._client.delete_collection(CollectionId=collection_id)
            
            if collection_id in self._collections:
                del self._collections[collection_id]
            
            return {
                "success": True,
                "collection_id": collection_id,
                "status_code": 200
            }
        except ClientError as e:
            logger.error(f"Collection deletion failed: {e}")
            return {"success": False, "error": str(e)}
    
    def _mock_index_faces(self, collection_id: str) -> Dict[str, Any]:
        """Generate mock index faces results."""
        return {
            "success": True,
            "face_records": [
                {
                    "Face": {"FaceId": "mock-face-id", "Confidence": 99.0},
                    "FaceDetail": {"BoundingBox": {"Width": 0.5, "Height": 0.5, "Left": 0.25, "Top": 0.25}}
                }
            ],
            "indexed_faces_count": 1,
            "unindexed_faces": []
        }
    
    def _mock_face_search(self) -> Dict[str, Any]:
        """Generate mock face search results."""
        return {
            "success": True,
            "face_matches": [
                {
                    "Similarity": 95.0,
                    "Face": {
                        "FaceId": "mock-face-id",
                        "BoundingBox": {"Width": 0.5, "Height": 0.5, "Left": 0.25, "Top": 0.25}
                    }
                }
            ],
            "searched_face": {"FaceId": "mock-searched-face-id"},
            "matched_faces_count": 1,
            "face_model_version": "6.0"
        }
    
    # ========================================================================
    # 4. Label Detection
    # ========================================================================
    
    def detect_labels(
        self,
        image_source: Union[str, bytes, Dict],
        max_labels: int = 100,
        min_confidence: float = 50.0,
        detect_moderation_labels: bool = False,
        enable_cloudwatch: bool = True
    ) -> Dict[str, Any]:
        """
        Detect labels in an image.
        
        Args:
            image_source: Image source
            max_labels: Maximum number of labels to return
            min_confidence: Minimum confidence threshold
            detect_moderation_labels: Also detect moderation labels
            enable_cloudwatch: Enable CloudWatch metrics
            
        Returns:
            Dictionary containing label detection results
        """
        if not BOTO3_AVAILABLE:
            logger.warning("boto3 not available, returning mock data")
            return self._mock_label_detection()
        
        try:
            image_payload = self._prepare_image(image_source)
            
            kwargs = {
                "Image": image_payload,
                "MaxLabels": max_labels,
                "MinConfidence": min_confidence
            }
            
            if detect_moderation_labels:
                kwargs["Features"] = ["GENERAL_LABELS", "IMAGE_PROPERTIES"]
            
            response = self._client.detect_labels(**kwargs)
            
            labels = response.get("Labels", [])
            
            if enable_cloudwatch and self.enable_cloudwatch:
                self._record_metric("LabelDetections", len(labels))
            
            return {
                "success": True,
                "labels": labels,
                "label_count": len(labels),
                "image_properties": response.get("ImageProperties"),
                "moderation_labels": response.get("ModerationLabels", [])
            }
        except ClientError as e:
            logger.error(f"Label detection failed: {e}")
            if enable_cloudwatch and self.enable_cloudwatch:
                self._record_metric("LabelDetections", error=True)
            return {"success": False, "error": str(e)}
    
    def detect_labels_video(
        self,
        video_source: Union[str, Dict],
        s3_bucket: str = None,
        sns_topic_arn: str = None,
        sns_role_arn: str = None,
        min_confidence: float = 50.0,
        enable_cloudwatch: bool = True
    ) -> Dict[str, Any]:
        """
        Start label detection in a video.
        
        Args:
            video_source: Video source (S3 object or bytes)
            s3_bucket: S3 bucket for video
            sns_topic_arn: SNS topic ARN for notifications
            sns_role_arn: IAM role ARN for SNS
            min_confidence: Minimum confidence threshold
            enable_cloudwatch: Enable CloudWatch metrics
            
        Returns:
            Dictionary containing job ID and status
        """
        if not BOTO3_AVAILABLE:
            logger.warning("boto3 not available, returning mock data")
            return self._mock_video_label_detection()
        
        try:
            kwargs = {
                "MinConfidence": min_confidence
            }
            
            if isinstance(video_source, dict):
                kwargs["Video"] = video_source
            elif s3_bucket:
                kwargs["Video"] = {"S3Object": {"Bucket": s3_bucket, "Name": video_source}}
            else:
                return {"success": False, "error": "Either video_source dict or s3_bucket required"}
            
            if sns_topic_arn:
                kwargs["NotificationChannel"] = {
                    "SNSTopicArn": sns_topic_arn,
                    "RoleArn": sns_role_arn
                }
            
            response = self._client.start_label_detection(**kwargs)
            
            job_id = response.get("JobId")
            
            return {
                "success": True,
                "job_id": job_id,
                "status": "IN_PROGRESS"
            }
        except ClientError as e:
            logger.error(f"Video label detection start failed: {e}")
            return {"success": False, "error": str(e)}
    
    def _mock_label_detection(self) -> Dict[str, Any]:
        """Generate mock label detection results."""
        return {
            "success": True,
            "labels": [
                {
                    "Name": "Person",
                    "Confidence": 98.5,
                    "Parents": [{"Name": "Human"}]
                },
                {
                    "Name": "Building",
                    "Confidence": 92.3,
                    "Parents": [{"Name": "Structure"}]
                }
            ],
            "label_count": 2,
            "image_properties": {},
            "moderation_labels": []
        }
    
    def _mock_video_label_detection(self) -> Dict[str, Any]:
        """Generate mock video label detection results."""
        return {
            "success": True,
            "job_id": "mock-job-id-" + str(uuid.uuid4()),
            "status": "IN_PROGRESS"
        }
    
    # ========================================================================
    # 5. Text Detection
    # ========================================================================
    
    def detect_text(
        self,
        image_source: Union[str, bytes, Dict],
        filters: Dict[str, Any] = None,
        enable_cloudwatch: bool = True
    ) -> Dict[str, Any]:
        """
        Detect text in an image.
        
        Args:
            image_source: Image source
            filters: Text detection filters
            enable_cloudwatch: Enable CloudWatch metrics
            
        Returns:
            Dictionary containing text detection results
        """
        if not BOTO3_AVAILABLE:
            logger.warning("boto3 not available, returning mock data")
            return self._mock_text_detection()
        
        try:
            image_payload = self._prepare_image(image_source)
            
            kwargs = {"Image": image_payload}
            
            if filters:
                kwargs["Filters"] = filters
            
            response = self._client.detect_text(**kwargs)
            
            text_detections = response.get("TextDetections", [])
            
            if enable_cloudwatch and self.enable_cloudwatch:
                self._record_metric("TextDetections", len(text_detections))
            
            return {
                "success": True,
                "text_detections": text_detections,
                "text_count": len(text_detections)
            }
        except ClientError as e:
            logger.error(f"Text detection failed: {e}")
            if enable_cloudwatch and self.enable_cloudwatch:
                self._record_metric("TextDetections", error=True)
            return {"success": False, "error": str(e)}
    
    def detect_text_video(
        self,
        video_source: Union[str, Dict],
        s3_bucket: str = None,
        sns_topic_arn: str = None,
        sns_role_arn: str = None,
        enable_cloudwatch: bool = True
    ) -> Dict[str, Any]:
        """
        Start text detection in a video.
        
        Args:
            video_source: Video source
            s3_bucket: S3 bucket for video
            sns_topic_arn: SNS topic ARN for notifications
            sns_role_arn: IAM role ARN for SNS
            enable_cloudwatch: Enable CloudWatch metrics
            
        Returns:
            Dictionary containing job ID and status
        """
        if not BOTO3_AVAILABLE:
            logger.warning("boto3 not available, returning mock data")
            return {"success": True, "job_id": "mock-job-id", "status": "IN_PROGRESS"}
        
        try:
            kwargs = {}
            
            if isinstance(video_source, dict):
                kwargs["Video"] = video_source
            elif s3_bucket:
                kwargs["Video"] = {"S3Object": {"Bucket": s3_bucket, "Name": video_source}}
            else:
                return {"success": False, "error": "Either video_source dict or s3_bucket required"}
            
            if sns_topic_arn:
                kwargs["NotificationChannel"] = {
                    "SNSTopicArn": sns_topic_arn,
                    "RoleArn": sns_role_arn
                }
            
            response = self._client.start_text_detection(**kwargs)
            
            return {
                "success": True,
                "job_id": response.get("JobId"),
                "status": "IN_PROGRESS"
            }
        except ClientError as e:
            logger.error(f"Video text detection start failed: {e}")
            return {"success": False, "error": str(e)}
    
    def _mock_text_detection(self) -> Dict[str, Any]:
        """Generate mock text detection results."""
        return {
            "success": True,
            "text_detections": [
                {
                    "DetectedText": "SAMPLE TEXT",
                    "Type": "LINE",
                    "Confidence": 95.5,
                    "BoundingBox": {"Width": 0.8, "Height": 0.1, "Left": 0.1, "Top": 0.45}
                }
            ],
            "text_count": 1
        }
    
    # ========================================================================
    # 6. Celebrity Recognition
    # ========================================================================
    
    def recognize_celebrities(
        self,
        image_source: Union[str, bytes, Dict],
        enable_cloudwatch: bool = True
    ) -> Dict[str, Any]:
        """
        Recognize celebrities in an image.
        
        Args:
            image_source: Image source
            enable_cloudwatch: Enable CloudWatch metrics
            
        Returns:
            Dictionary containing celebrity recognition results
        """
        if not BOTO3_AVAILABLE:
            logger.warning("boto3 not available, returning mock data")
            return self._mock_celebrity_recognition()
        
        try:
            image_payload = self._prepare_image(image_source)
            
            response = self._client.recognize_celebrities(Image=image_payload)
            
            celebrities = response.get("CelebrityFaces", [])
            
            if enable_cloudwatch and self.enable_cloudwatch:
                self._record_metric("CelebrityRecognitions", len(celebrities))
            
            return {
                "success": True,
                "celebrity_faces": celebrities,
                "unrecognized_faces": response.get("UnrecognizedFaces", []),
                "celebrity_count": len(celebrities)
            }
        except ClientError as e:
            logger.error(f"Celebrity recognition failed: {e}")
            if enable_cloudwatch and self.enable_cloudwatch:
                self._record_metric("CelebrityRecognitions", error=True)
            return {"success": False, "error": str(e)}
    
    def recognize_celebrities_video(
        self,
        video_source: Union[str, Dict],
        s3_bucket: str = None,
        sns_topic_arn: str = None,
        sns_role_arn: str = None,
        enable_cloudwatch: bool = True
    ) -> Dict[str, Any]:
        """
        Start celebrity recognition in a video.
        
        Args:
            video_source: Video source
            s3_bucket: S3 bucket for video
            sns_topic_arn: SNS topic ARN for notifications
            sns_role_arn: IAM role ARN for SNS
            enable_cloudwatch: Enable CloudWatch metrics
            
        Returns:
            Dictionary containing job ID and status
        """
        if not BOTO3_AVAILABLE:
            logger.warning("boto3 not available, returning mock data")
            return {"success": True, "job_id": "mock-job-id", "status": "IN_PROGRESS"}
        
        try:
            kwargs = {}
            
            if isinstance(video_source, dict):
                kwargs["Video"] = video_source
            elif s3_bucket:
                kwargs["Video"] = {"S3Object": {"Bucket": s3_bucket, "Name": video_source}}
            else:
                return {"success": False, "error": "Either video_source dict or s3_bucket required"}
            
            if sns_topic_arn:
                kwargs["NotificationChannel"] = {
                    "SNSTopicArn": sns_topic_arn,
                    "RoleArn": sns_role_arn
                }
            
            response = self._client.start_celebrity_recognition(**kwargs)
            
            return {
                "success": True,
                "job_id": response.get("JobId"),
                "status": "IN_PROGRESS"
            }
        except ClientError as e:
            logger.error(f"Video celebrity recognition start failed: {e}")
            return {"success": False, "error": str(e)}
    
    def _mock_celebrity_recognition(self) -> Dict[str, Any]:
        """Generate mock celebrity recognition results."""
        return {
            "success": True,
            "celebrity_faces": [
                {
                    "Name": "John Doe",
                    "Urls": ["https://en.wikipedia.org/wiki/John_Doe"],
                    "Face": {
                        "BoundingBox": {"Width": 0.5, "Height": 0.5, "Left": 0.25, "Top": 0.25},
                        "Confidence": 99.0
                    },
                    "Confidence": 98.5
                }
            ],
            "unrecognized_faces": [],
            "celebrity_count": 1
        }
    
    # ========================================================================
    # 7. Content Moderation
    # ========================================================================
    
    def detect_moderation_labels(
        self,
        image_source: Union[str, bytes, Dict],
        min_confidence: float = 50.0,
        moderation_model_version: str = None,
        enable_cloudwatch: bool = True
    ) -> Dict[str, Any]:
        """
        Detect moderation labels in an image.
        
        Args:
            image_source: Image source
            min_confidence: Minimum confidence threshold
            moderation_model_version: Moderation model version
            enable_cloudwatch: Enable CloudWatch metrics
            
        Returns:
            Dictionary containing moderation detection results
        """
        if not BOTO3_AVAILABLE:
            logger.warning("boto3 not available, returning mock data")
            return self._mock_moderation_detection()
        
        try:
            image_payload = self._prepare_image(image_source)
            
            kwargs = {
                "Image": image_payload,
                "MinConfidence": min_confidence
            }
            
            if moderation_model_version:
                kwargs["ModerationModelVersion"] = moderation_model_version
            
            response = self._client.detect_moderation_labels(**kwargs)
            
            moderation_labels = response.get("ModerationLabels", [])
            
            if enable_cloudwatch and self.enable_cloudwatch:
                self._record_metric("ModerationFlags", len(moderation_labels))
            
            return {
                "success": True,
                "moderation_labels": moderation_labels,
                "moderation_count": len(moderation_labels),
                "model_version": response.get("ModerationModelVersion")
            }
        except ClientError as e:
            logger.error(f"Content moderation detection failed: {e}")
            if enable_cloudwatch and self.enable_cloudwatch:
                self._record_metric("ModerationFlags", error=True)
            return {"success": False, "error": str(e)}
    
    def detect_moderation_labels_video(
        self,
        video_source: Union[str, Dict],
        s3_bucket: str = None,
        sns_topic_arn: str = None,
        sns_role_arn: str = None,
        min_confidence: float = 50.0,
        enable_cloudwatch: bool = True
    ) -> Dict[str, Any]:
        """
        Start content moderation detection in a video.
        
        Args:
            video_source: Video source
            s3_bucket: S3 bucket for video
            sns_topic_arn: SNS topic ARN for notifications
            sns_role_arn: IAM role ARN for SNS
            min_confidence: Minimum confidence threshold
            enable_cloudwatch: Enable CloudWatch metrics
            
        Returns:
            Dictionary containing job ID and status
        """
        if not BOTO3_AVAILABLE:
            logger.warning("boto3 not available, returning mock data")
            return {"success": True, "job_id": "mock-job-id", "status": "IN_PROGRESS"}
        
        try:
            kwargs = {"MinConfidence": min_confidence}
            
            if isinstance(video_source, dict):
                kwargs["Video"] = video_source
            elif s3_bucket:
                kwargs["Video"] = {"S3Object": {"Bucket": s3_bucket, "Name": video_source}}
            else:
                return {"success": False, "error": "Either video_source dict or s3_bucket required"}
            
            if sns_topic_arn:
                kwargs["NotificationChannel"] = {
                    "SNSTopicArn": sns_topic_arn,
                    "RoleArn": sns_role_arn
                }
            
            response = self._client.start_content_moderation(**kwargs)
            
            return {
                "success": True,
                "job_id": response.get("JobId"),
                "status": "IN_PROGRESS"
            }
        except ClientError as e:
            logger.error(f"Video content moderation start failed: {e}")
            return {"success": False, "error": str(e)}
    
    def _mock_moderation_detection(self) -> Dict[str, Any]:
        """Generate mock moderation detection results."""
        return {
            "success": True,
            "moderation_labels": [
                {
                    "Name": "Suggestive",
                    "Confidence": 85.5,
                    "ParentName": "Inappropriate Content"
                }
            ],
            "moderation_count": 1,
            "model_version": "6.0"
        }
    
    # ========================================================================
    # 8. Video Analysis
    # ========================================================================
    
    def start_video_analysis(
        self,
        video_source: Union[str, Dict],
        s3_bucket: str = None,
        features: List[str] = None,
        sns_topic_arn: str = None,
        sns_role_arn: str = None,
        enable_cloudwatch: bool = True
    ) -> Dict[str, Any]:
        """
        Start video analysis with multiple features.
        
        Args:
            video_source: Video source
            s3_bucket: S3 bucket for video
            features: List of features (FACESEARCH, LABELS, TEXT, CELEBRITIES, etc.)
            sns_topic_arn: SNS topic ARN for notifications
            sns_role_arn: IAM role ARN for SNS
            enable_cloudwatch: Enable CloudWatch metrics
            
        Returns:
            Dictionary containing job ID and status
        """
        if not BOTO3_AVAILABLE:
            logger.warning("boto3 not available, returning mock data")
            return {"success": True, "job_id": "mock-video-job-id", "status": "IN_PROGRESS"}
        
        try:
            kwargs = {}
            
            if isinstance(video_source, dict):
                kwargs["Video"] = video_source
            elif s3_bucket:
                kwargs["Video"] = {"S3Object": {"Bucket": s3_bucket, "Name": video_source}}
            else:
                return {"success": False, "error": "Either video_source dict or s3_bucket required"}
            
            if features:
                kwargs["Features"] = features
            
            if sns_topic_arn:
                kwargs["NotificationChannel"] = {
                    "SNSTopicArn": sns_topic_arn,
                    "RoleArn": sns_role_arn
                }
            
            response = self._client.start_segment_detection(
                **kwargs
            ) if "SEGMENT" in features else self._client.start_label_detection(
                Video=kwargs.get("Video"),
                NotificationChannel=kwargs.get("NotificationChannel")
            )
            
            job_id = response.get("JobId")
            
            if enable_cloudwatch and self.enable_cloudwatch:
                self._record_metric("VideoAnalyses")
            
            return {
                "success": True,
                "job_id": job_id,
                "status": "IN_PROGRESS"
            }
        except ClientError as e:
            logger.error(f"Video analysis start failed: {e}")
            if enable_cloudwatch and self.enable_cloudwatch:
                self._record_metric("VideoAnalyses", error=True)
            return {"success": False, "error": str(e)}
    
    def get_video_analysis_results(
        self,
        job_id: str,
        feature_type: str = "LABEL"
    ) -> Dict[str, Any]:
        """
        Get video analysis results.
        
        Args:
            job_id: Job ID
            feature_type: Feature type (LABEL, FACE_SEARCH, CELEBRITY, CONTENT_MODERATION, TEXT)
            
        Returns:
            Dictionary containing video analysis results
        """
        if not BOTO3_AVAILABLE:
            logger.warning("boto3 not available, returning mock data")
            return {
                "success": True,
                "job_id": job_id,
                "status": "COMPLETED",
                "results": []
            }
        
        try:
            kwargs = {"JobId": job_id}
            
            if feature_type == "LABEL":
                response = self._client.get_label_detection(**kwargs)
            elif feature_type == "FACE_SEARCH":
                response = self._client.get_face_search(**kwargs)
            elif feature_type == "CELEBRITY":
                response = self._client.get_celebrity_recognition(**kwargs)
            elif feature_type == "CONTENT_MODERATION":
                response = self._client.get_content_moderation(**kwargs)
            elif feature_type == "TEXT":
                response = self._client.get_text_detection(**kwargs)
            else:
                return {"success": False, "error": f"Unknown feature type: {feature_type}"}
            
            return {
                "success": True,
                "job_id": job_id,
                "status": response.get("VideoMetadata", {}).get("Status"),
                "results": response.get(feature_type.upper() + "Detections", []),
                "next_token": response.get("NextToken")
            }
        except ClientError as e:
            logger.error(f"Get video analysis results failed: {e}")
            return {"success": False, "error": str(e)}
    
    def analyze_video_sync(
        self,
        video_source: Union[str, Dict],
        s3_bucket: str = None,
        features: List[str] = None,
        poll_interval: int = 5,
        max_wait_time: int = 300,
        enable_cloudwatch: bool = True
    ) -> Dict[str, Any]:
        """
        Analyze video synchronously (with polling).
        
        Args:
            video_source: Video source
            s3_bucket: S3 bucket for video
            features: List of features to analyze
            poll_interval: Polling interval in seconds
            max_wait_time: Maximum wait time in seconds
            enable_cloudwatch: Enable CloudWatch metrics
            
        Returns:
            Dictionary containing all analysis results
        """
        start_result = self.start_video_analysis(
            video_source=video_source,
            s3_bucket=s3_bucket,
            features=features or ["LABELS", "FACESEARCH", "CELEBRITIES"],
            enable_cloudwatch=enable_cloudwatch
        )
        
        if not start_result.get("success"):
            return start_result
        
        job_id = start_result.get("job_id")
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            results = self.get_video_analysis_results(job_id)
            
            if results.get("status") == "COMPLETED":
                return results
            elif results.get("status") == "FAILED":
                return {"success": False, "error": "Video analysis failed", "job_id": job_id}
            
            time.sleep(poll_interval)
        
        return {
            "success": False,
            "error": "Timeout waiting for video analysis",
            "job_id": job_id
        }
    
    # ========================================================================
    # 9. PPE Detection
    # ========================================================================
    
    def detect_ppe(
        self,
        image_source: Union[str, bytes, Dict],
        enable_cloudwatch: bool = True
    ) -> Dict[str, Any]:
        """
        Detect personal protective equipment in an image.
        
        Args:
            image_source: Image source
            enable_cloudwatch: Enable CloudWatch metrics
            
        Returns:
            Dictionary containing PPE detection results
        """
        if not BOTO3_AVAILABLE:
            logger.warning("boto3 not available, returning mock data")
            return self._mock_ppe_detection()
        
        try:
            image_payload = self._prepare_image(image_source)
            
            response = self._client.detect_protective_equipment(Image=image_payload)
            
            persons = response.get("Persons", [])
            summary = response.get("Summary", {})
            
            if enable_cloudwatch and self.enable_cloudwatch:
                self._record_metric("PPEDetections", len(persons))
            
            return {
                "success": True,
                "persons": persons,
                "summary": summary,
                "person_count": len(persons),
                "has_ppe_violations": summary.get("TotalviolationsWithBodyCover", 0) > 0
            }
        except ClientError as e:
            logger.error(f"PPE detection failed: {e}")
            if enable_cloudwatch and self.enable_cloudwatch:
                self._record_metric("PPEDetections", error=True)
            return {"success": False, "error": str(e)}
    
    def detect_ppe_video(
        self,
        video_source: Union[str, Dict],
        s3_bucket: str = None,
        sns_topic_arn: str = None,
        sns_role_arn: str = None,
        enable_cloudwatch: bool = True
    ) -> Dict[str, Any]:
        """
        Start PPE detection in a video.
        
        Args:
            video_source: Video source
            s3_bucket: S3 bucket for video
            sns_topic_arn: SNS topic ARN for notifications
            sns_role_arn: IAM role ARN for SNS
            enable_cloudwatch: Enable CloudWatch metrics
            
        Returns:
            Dictionary containing job ID and status
        """
        if not BOTO3_AVAILABLE:
            logger.warning("boto3 not available, returning mock data")
            return {"success": True, "job_id": "mock-ppe-job-id", "status": "IN_PROGRESS"}
        
        try:
            kwargs = {}
            
            if isinstance(video_source, dict):
                kwargs["Video"] = video_source
            elif s3_bucket:
                kwargs["Video"] = {"S3Object": {"Bucket": s3_bucket, "Name": video_source}}
            else:
                return {"success": False, "error": "Either video_source dict or s3_bucket required"}
            
            if sns_topic_arn:
                kwargs["NotificationChannel"] = {
                    "SNSTopicArn": sns_topic_arn,
                    "RoleArn": sns_role_arn
                }
            
            response = self._client.start_protective_equipment_detection(**kwargs)
            
            return {
                "success": True,
                "job_id": response.get("JobId"),
                "status": "IN_PROGRESS"
            }
        except ClientError as e:
            logger.error(f"Video PPE detection start failed: {e}")
            return {"success": False, "error": str(e)}
    
    def _mock_ppe_detection(self) -> Dict[str, Any]:
        """Generate mock PPE detection results."""
        return {
            "success": True,
            "persons": [
                {
                    "BoundingBox": {"Width": 0.3, "Height": 0.5, "Left": 0.35, "Top": 0.3},
                    "Confidence": 99.5,
                    "BodyParts": [
                        {
                            "Name": "HEAD",
                            "Confidence": 99.5,
                            "EquipmentDetections": [
                                {
                                    "Type": "FACE_COVER",
                                    "Confidence": 98.0,
                                    "Covers": {"Value": True, "Confidence": 95.0}
                                }
                            ]
                        }
                    ]
                }
            ],
            "summary": {
                "TotalDetected": 1,
                "PersonsWithNoPPE": 0,
                "PersonsWithCompletePPE": 1,
                "TotalviolationsWithBodyCover": 0,
                "TotalviolationsWithFaceCover": 0
            },
            "person_count": 1,
            "has_ppe_violations": False
        }
    
    # ========================================================================
    # 10. CloudWatch Integration
    # ========================================================================
    
    def put_metric_data(self, metrics: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Put custom metrics to CloudWatch.
        
        Args:
            metrics: List of metric data
            
        Returns:
            Dictionary containing operation result
        """
        if not self._cloudwatch_client:
            return {"success": False, "error": "CloudWatch client not initialized"}
        
        try:
            self._cloudwatch_client.put_metric_data(
                Namespace=self.cloudwatch_namespace,
                MetricData=metrics
            )
            
            return {"success": True, "metric_count": len(metrics)}
        except ClientError as e:
            logger.error(f"Put metric data failed: {e}")
            return {"success": False, "error": str(e)}
    
    def get_rekognition_metrics(
        self,
        start_time: datetime = None,
        end_time: datetime = None,
        period: int = 60,
        stat_type: str = "Average"
    ) -> Dict[str, Any]:
        """
        Get Rekognition metrics from CloudWatch.
        
        Args:
            start_time: Start time for metrics
            end_time: End time for metrics
            period: Metric period in seconds
            stat_type: Statistic type (Average, Sum, Maximum, Minimum)
            
        Returns:
            Dictionary containing metric data
        """
        if not self._cloudwatch_client:
            return {"success": False, "error": "CloudWatch client not initialized"}
        
        try:
            end_time = end_time or datetime.utcnow()
            start_time = start_time or end_time - timedelta(hours=1)
            
            metric_names = [
                "FaceDetections", "FaceComparisons", "FaceSearches",
                "LabelDetections", "TextDetections", "CelebrityRecognitions",
                "ModerationFlags", "PPEDetections", "VideoAnalyses", "Errors"
            ]
            
            results = {}
            
            for metric_name in metric_names:
                response = self._cloudwatch_client.get_metric_statistics(
                    Namespace=self.cloudwatch_namespace,
                    MetricName=metric_name,
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=period,
                    Statistics=[stat_type]
                )
                
                results[metric_name] = response.get("Datapoints", [])
            
            return {
                "success": True,
                "metrics": results,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat()
            }
        except ClientError as e:
            logger.error(f"Get rekognition metrics failed: {e}")
            return {"success": False, "error": str(e)}
    
    def create_dashboard(self, dashboard_name: str = "RekognitionDashboard") -> Dict[str, Any]:
        """
        Create a CloudWatch dashboard for Rekognition metrics.
        
        Args:
            dashboard_name: Dashboard name
            
        Returns:
            Dictionary containing dashboard creation result
        """
        if not self._cloudwatch_client:
            return {"success": False, "error": "CloudWatch client not initialized"}
        
        try:
            widget_config = {
                "widgets": [
                    {
                        "type": "metric",
                        "properties": {
                            "metrics": [
                                [self.cloudwatch_namespace, "FaceDetections", {"stat": "Sum"}],
                                [".", "FaceComparisons", {"stat": "Sum"}],
                                [".", "FaceSearches", {"stat": "Sum"}],
                                [".", "LabelDetections", {"stat": "Sum"}]
                            ],
                            "period": 300,
                            "stat": "Sum",
                            "region": self.region_name,
                            "title": "Rekognition Detections"
                        }
                    },
                    {
                        "type": "metric",
                        "properties": {
                            "metrics": [
                                [self.cloudwatch_namespace, "Errors", {"stat": "Sum"}],
                                [".", "ModerationFlags", {"stat": "Sum"}],
                                [".", "PPEDetections", {"stat": "Sum"}]
                            ],
                            "period": 300,
                            "stat": "Sum",
                            "region": self.region_name,
                            "title": "Rekognition Alerts"
                        }
                    }
                ]
            }
            
            dashboard_body = json.dumps(widget_config)
            
            self._cloudwatch_client.put_dashboard(
                DashboardName=dashboard_name,
                DashboardBody=dashboard_body
            )
            
            return {
                "success": True,
                "dashboard_name": dashboard_name,
                "dashboard_url": f"https://{self.region_name}.console.aws.amazon.com/cloudwatch/home?region={self.region_name}#dashboards:name={dashboard_name}"
            }
        except ClientError as e:
            logger.error(f"Create dashboard failed: {e}")
            return {"success": False, "error": str(e)}
    
    def set_alarm(
        self,
        alarm_name: str,
        metric_name: str,
        threshold: float,
        comparison_operator: str = "GreaterThanThreshold",
        evaluation_periods: int = 1,
        period: int = 300,
        sns_topic_arn: str = None
    ) -> Dict[str, Any]:
        """
        Create a CloudWatch alarm for Rekognition metrics.
        
        Args:
            alarm_name: Alarm name
            metric_name: Metric name
            threshold: Alarm threshold
            comparison_operator: Comparison operator
            evaluation_periods: Number of evaluation periods
            period: Period in seconds
            sns_topic_arn: SNS topic ARN for alarm notifications
            
        Returns:
            Dictionary containing alarm creation result
        """
        if not self._cloudwatch_client:
            return {"success": False, "error": "CloudWatch client not initialized"}
        
        try:
            kwargs = {
                "AlarmName": alarm_name,
                "MetricName": metric_name,
                "Namespace": self.cloudwatch_namespace,
                "Threshold": threshold,
                "ComparisonOperator": comparison_operator,
                "EvaluationPeriods": evaluation_periods,
                "Period": period,
                "Statistic": "Sum"
            }
            
            if sns_topic_arn:
                kwargs["AlarmActions"] = [sns_topic_arn]
            
            self._cloudwatch_client.put_metric_alarm(**kwargs)
            
            return {
                "success": True,
                "alarm_name": alarm_name,
                "alarm_arn": f"arn:aws:cloudwatch:{self.region_name}::alarm:{alarm_name}"
            }
        except ClientError as e:
            logger.error(f"Create alarm failed: {e}")
            return {"success": False, "error": str(e)}
    
    # ========================================================================
    # Utility Methods
    # ========================================================================
    
    def _prepare_image(self, image_source: Union[str, bytes, Dict]) -> Dict[str, Any]:
        """
        Prepare image payload for Rekognition API.
        
        Args:
            image_source: Image source (S3 object, bytes, or base64)
            
        Returns:
            Dictionary containing image payload
        """
        if isinstance(image_source, dict):
            return image_source
        elif isinstance(image_source, str):
            if image_source.startswith("s3://"):
                parts = image_source.replace("s3://", "").split("/")
                bucket = parts[0]
                key = "/".join(parts[1:])
                return {"S3Object": {"Bucket": bucket, "Name": key}}
            elif os.path.isfile(image_source):
                with open(image_source, "rb") as f:
                    image_bytes = f.read()
                return {"Bytes": base64.b64encode(image_bytes).decode()}
            else:
                return {"Bytes": base64.b64decode(image_source)}
        elif isinstance(image_source, bytes):
            return {"Bytes": base64.b64encode(image_source).decode()}
        else:
            raise ValueError(f"Invalid image source type: {type(image_source)}")
    
    def list_collections(self) -> Dict[str, Any]:
        """
        List all face collections.
        
        Returns:
            Dictionary containing collection list
        """
        if not BOTO3_AVAILABLE:
            return {"success": True, "collections": list(self._collections.keys())}
        
        try:
            response = self._client.list_collections(MaxResults=100)
            
            collections = response.get("CollectionIds", [])
            
            return {
                "success": True,
                "collections": collections,
                "count": len(collections)
            }
        except ClientError as e:
            logger.error(f"List collections failed: {e}")
            return {"success": False, "error": str(e)}
    
    def describe_collection(self, collection_id: str) -> Dict[str, Any]:
        """
        Describe a face collection.
        
        Args:
            collection_id: Collection ID
            
        Returns:
            Dictionary containing collection details
        """
        if not BOTO3_AVAILABLE:
            return {"success": True, "collection_id": collection_id, "face_count": 0}
        
        try:
            response = self._client.describe_collection(CollectionId=collection_id)
            
            return {
                "success": True,
                "collection_id": collection_id,
                "face_count": response.get("FaceCount", 0),
                "collection_arn": response.get("CollectionARN"),
                "creation_timestamp": response.get("CreationTimestamp"),
                "status": response.get("Status")
            }
        except ClientError as e:
            logger.error(f"Describe collection failed: {e}")
            return {"success": False, "error": str(e)}
    
    def list_faces(
        self,
        collection_id: str,
        max_results: int = 100,
        next_token: str = None
    ) -> Dict[str, Any]:
        """
        List faces in a collection.
        
        Args:
            collection_id: Collection ID
            max_results: Maximum number of results
            next_token: Pagination token
            
        Returns:
            Dictionary containing face list
        """
        if not BOTO3_AVAILABLE:
            return {"success": True, "faces": [], "face_count": 0}
        
        try:
            kwargs = {"CollectionId": collection_id, "MaxResults": max_results}
            
            if next_token:
                kwargs["NextToken"] = next_token
            
            response = self._client.list_faces(**kwargs)
            
            return {
                "success": True,
                "faces": response.get("Faces", []),
                "face_count": len(response.get("Faces", [])),
                "next_token": response.get("NextToken")
            }
        except ClientError as e:
            logger.error(f"List faces failed: {e}")
            return {"success": False, "error": str(e)}
    
    def delete_faces(self, collection_id: str, face_ids: List[str]) -> Dict[str, Any]:
        """
        Delete faces from a collection.
        
        Args:
            collection_id: Collection ID
            face_ids: List of face IDs to delete
            
        Returns:
            Dictionary containing deletion result
        """
        if not BOTO3_AVAILABLE:
            return {"success": True, "deleted_face_ids": face_ids}
        
        try:
            response = self._client.delete_faces(
                CollectionId=collection_id,
                FaceIds=face_ids
            )
            
            return {
                "success": True,
                "deleted_face_ids": response.get("DeletedFaces", []),
                "failed_face_ids": response.get("FailedFaces", [])
            }
        except ClientError as e:
            logger.error(f"Delete faces failed: {e}")
            return {"success": False, "error": str(e)}
    
    def get_segment_detection(
        self,
        job_id: str,
        segment_types: List[str] = None
    ) -> Dict[str, Any]:
        """
        Get segment detection results.
        
        Args:
            job_id: Job ID
            segment_types: List of segment types (TECHNICAL_CUES, SHOT, SCENE)
            
        Returns:
            Dictionary containing segment detection results
        """
        if not BOTO3_AVAILABLE:
            return {"success": True, "job_id": job_id, "segments": []}
        
        try:
            kwargs = {"JobId": job_id}
            
            if segment_types:
                kwargs["SegmentTypes"] = segment_types
            
            response = self._client.get_segment_detection(**kwargs)
            
            return {
                "success": True,
                "job_id": job_id,
                "segments": response.get("Segments", []),
                "segment_details": response.get("AudioMetadata", [])
            }
        except ClientError as e:
            logger.error(f"Get segment detection failed: {e}")
            return {"success": False, "error": str(e)}
    
    def close(self):
        """Close and cleanup resources."""
        if self._metrics_buffer:
            self._flush_metrics()
        
        self._client = None
        self._cloudwatch_client = None
        
        logger.info("Rekognition integration closed")
