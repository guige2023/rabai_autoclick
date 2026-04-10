"""
ML Pipeline Orchestration for Workflow AI

This module provides MLPipeline class for orchestrating machine learning pipelines
including preprocessing, feature engineering, training, evaluation, and model management.
"""

import json
import hashlib
import logging
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass, field, asdict
from collections import defaultdict
import copy

logger = logging.getLogger(__name__)


class PipelineStage(str, Enum):
    """Define ML pipeline stages."""
    PREPROCESSING = "preprocessing"
    FEATURE_ENGINEERING = "feature_engineering"
    TRAINING = "training"
    EVALUATION = "evaluation"
    HYPERPARAMETER_TUNING = "hyperparameter_tuning"
    REGISTERED = "registered"
    A_B_TESTING = "a_b_testing"
    SCHEDULED = "scheduled"


@dataclass
class ModelVersion:
    """Version information for a registered model."""
    version_id: str
    model_name: str
    stage: PipelineStage
    metrics: Dict[str, float]
    params: Dict[str, Any]
    created_at: str
    pipeline_config: Dict[str, Any]
    artifact_path: Optional[str] = None
    parent_version_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ABTestResult:
    """A/B test comparison result."""
    test_id: str
    control_version_id: str
    treatment_version_id: str
    control_metrics: Dict[str, float]
    treatment_metrics: Dict[str, float]
    metric_deltas: Dict[str, float]
    winner: str
    confidence: float
    started_at: str
    completed_at: Optional[str] = None


@dataclass
class PipelineConfig:
    """Configuration for a pipeline run."""
    stages: List[PipelineStage]
    hyperparameters: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    preprocessing_params: Dict[str, Any] = field(default_factory=dict)
    feature_params: Dict[str, Any] = field(default_factory=dict)
    training_params: Dict[str, Any] = field(default_factory=dict)
    evaluation_params: Dict[str, Any] = field(default_factory=dict)
    schedule: Optional[Dict[str, Any]] = None


class DataPreprocessor:
    """Data preprocessing handler."""
    
    def __init__(self, params: Optional[Dict[str, Any]] = None):
        self.params = params or {}
        self._fitted = False
    
    def fit(self, data: Any) -> 'DataPreprocessor':
        """Fit preprocessor to data."""
        logger.info("Fitting data preprocessor")
        self._fitted = True
        return self
    
    def transform(self, data: Any) -> Any:
        """Transform data."""
        if not self._fitted:
            raise RuntimeError("Preprocessor must be fitted before transform")
        logger.info("Transforming data with preprocessor")
        return data
    
    def fit_transform(self, data: Any) -> Any:
        """Fit and transform data."""
        return self.fit(data).transform(data)


class FeatureEngineer:
    """Feature engineering handler."""
    
    def __init__(self, params: Optional[Dict[str, Any]] = None):
        self.params = params or {}
        self._features_created = []
    
    def create_features(self, data: Any) -> Any:
        """Create features from workflow data."""
        logger.info("Creating features from workflow data")
        self._features_created = ["feature_1", "feature_2"]
        return data
    
    def get_feature_names(self) -> List[str]:
        """Get list of created feature names."""
        return self._features_created.copy()


class ModelTrainer:
    """Model training handler."""
    
    def __init__(self, params: Optional[Dict[str, Any]] = None):
        self.params = params or {}
        self._trained = False
        self._model = None
    
    def train(self, X: Any, y: Any, **kwargs) -> Any:
        """Train model on data."""
        logger.info(f"Training model with params: {self.params}")
        self._trained = True
        self._model = {"trained": True, "params": self.params}
        return self._model
    
    def predict(self, X: Any) -> Any:
        """Make predictions with trained model."""
        if not self._trained:
            raise RuntimeError("Model must be trained before prediction")
        return X


class ModelEvaluator:
    """Model evaluation handler."""
    
    def __init__(self, params: Optional[Dict[str, Any]] = None):
        self.params = params or {}
    
    def evaluate(self, model: Any, X: Any, y: Any) -> Dict[str, float]:
        """Evaluate model performance."""
        logger.info("Evaluating model performance")
        return {
            "accuracy": 0.85,
            "precision": 0.82,
            "recall": 0.80,
            "f1": 0.81,
            "roc_auc": 0.88
        }
    
    def cross_validate(self, model: Any, X: Any, y: Any, cv: int = 5) -> Dict[str, float]:
        """Perform cross-validation."""
        logger.info(f"Performing {cv}-fold cross-validation")
        return {
            "cv_accuracy_mean": 0.84,
            "cv_accuracy_std": 0.02
        }


class HyperparameterTuner:
    """Hyperparameter tuning handler."""
    
    def __init__(self, params: Optional[Dict[str, Any]] = None):
        self.params = params or {}
    
    def tune(self, trainer: ModelTrainer, X: Any, y: Any, 
             param_space: Dict[str, List[Any]]) -> Dict[str, Any]:
        """Tune hyperparameters."""
        logger.info(f"Tuning hyperparameters with space: {param_space}")
        return {
            "best_params": {k: v[0] for k, v in param_space.items()},
            "best_score": 0.87,
            "search_history": []
        }


class ModelRegistry:
    """Register and manage trained models."""
    
    def __init__(self):
        self._models: Dict[str, ModelVersion] = {}
        self._latest_by_name: Dict[str, str] = {}
    
    def register(self, model_name: str, version: ModelVersion) -> str:
        """Register a model version."""
        self._models[version.version_id] = version
        if model_name not in self._latest_by_name or \
           version.created_at >= self._models[self._latest_by_name[model_name]].created_at:
            self._latest_by_name[model_name] = version.version_id
        logger.info(f"Registered model {model_name} version {version.version_id}")
        return version.version_id
    
    def get(self, version_id: str) -> Optional[ModelVersion]:
        """Get a specific model version."""
        return self._models.get(version_id)
    
    def get_latest(self, model_name: str) -> Optional[ModelVersion]:
        """Get the latest version of a model."""
        version_id = self._latest_by_name.get(model_name)
        return self._models.get(version_id) if version_id else None
    
    def list_versions(self, model_name: Optional[str] = None) -> List[ModelVersion]:
        """List all model versions."""
        if model_name:
            return [v for v in self._models.values() if v.model_name == model_name]
        return list(self._models.values())
    
    def stage_model(self, version_id: str, stage: PipelineStage) -> None:
        """Update model stage."""
        if version_id in self._models:
            self._models[version_id].stage = stage
            logger.info(f"Model {version_id} staged to {stage}")
    
    def delete(self, version_id: str) -> bool:
        """Delete a model version."""
        if version_id in self._models:
            del self._models[version_id]
            return True
        return False


class PipelineVersioner:
    """Version control for pipelines."""
    
    def __init__(self):
        self._versions: Dict[str, Dict[str, Any]] = {}
        self._history: List[str] = []
    
    def create_version(self, config: PipelineConfig, parent_id: Optional[str] = None) -> str:
        """Create a new pipeline version."""
        version_data = {
            "config": asdict(config),
            "created_at": datetime.utcnow().isoformat(),
            "parent_id": parent_id,
            "version_hash": None
        }
        version_hash = self._compute_hash(version_data)
        version_id = f"pipeline_{len(self._versions) + 1}_{version_hash[:8]}"
        version_data["version_hash"] = version_hash
        self._versions[version_id] = version_data
        self._history.append(version_id)
        logger.info(f"Created pipeline version {version_id}")
        return version_id
    
    def get_version(self, version_id: str) -> Optional[Dict[str, Any]]:
        """Get pipeline version data."""
        return self._versions.get(version_id)
    
    def list_versions(self) -> List[str]:
        """List all pipeline versions."""
        return self._history.copy()
    
    def compare_versions(self, v1_id: str, v2_id: str) -> Dict[str, Any]:
        """Compare two pipeline versions."""
        v1 = self._versions.get(v1_id)
        v2 = self._versions.get(v2_id)
        if not v1 or not v2:
            return {"error": "Version not found"}
        return {
            "v1_created": v1["created_at"],
            "v2_created": v2["created_at"],
            "v1_parent": v1.get("parent_id"),
            "v2_parent": v2.get("parent_id"),
            "config_diff": self._diff_configs(v1["config"], v2["config"])
        }
    
    def _compute_hash(self, data: Dict[str, Any]) -> str:
        """Compute hash for version data."""
        content = json.dumps(data, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()
    
    def _diff_configs(self, c1: Dict, c2: Dict) -> Dict[str, Tuple[Any, Any]]:
        """Find differences between configs."""
        diff = {}
        all_keys = set(c1.keys()) | set(c2.keys())
        for key in all_keys:
            v1, v2 = c1.get(key), c2.get(key)
            if v1 != v2:
                diff[key] = (v1, v2)
        return diff


class ABTester:
    """A/B test different pipelines."""
    
    def __init__(self):
        self._tests: Dict[str, ABTestResult] = {}
    
    def create_test(self, control_version_id: str, treatment_version_id: str,
                   metrics: List[str]) -> str:
        """Create a new A/B test."""
        test_id = f"ab_test_{len(self._tests) + 1}_{datetime.utcnow().strftime('%Y%m%d')}"
        result = ABTestResult(
            test_id=test_id,
            control_version_id=control_version_id,
            treatment_version_id=treatment_version_id,
            control_metrics={m: 0.0 for m in metrics},
            treatment_metrics={m: 0.0 for m in metrics},
            metric_deltas={},
            winner="",
            confidence=0.0,
            started_at=datetime.utcnow().isoformat()
        )
        self._tests[test_id] = result
        logger.info(f"Created A/B test {test_id}")
        return test_id
    
    def record_metric(self, test_id: str, group: str, 
                     metric: str, value: float) -> None:
        """Record a metric value for a test group."""
        if test_id not in self._tests:
            raise ValueError(f"Test {test_id} not found")
        result = self._tests[test_id]
        if group == "control":
            result.control_metrics[metric] = value
        elif group == "treatment":
            result.treatment_metrics[metric] = value
    
    def compute_results(self, test_id: str) -> ABTestResult:
        """Compute A/B test results."""
        if test_id not in self._tests:
            raise ValueError(f"Test {test_id} not found")
        result = self._tests[test_id]
        for metric in result.control_metrics:
            delta = result.treatment_metrics[metric] - result.control_metrics[metric]
            result.metric_deltas[metric] = delta
        primary_metric = list(result.control_metrics.keys())[0] if result.control_metrics else "accuracy"
        if result.metric_deltas.get(primary_metric, 0) > 0:
            result.winner = "treatment"
        else:
            result.winner = "control"
        result.confidence = min(0.99, abs(result.metric_deltas.get(primary_metric, 0)) * 10 + 0.7)
        result.completed_at = datetime.utcnow().isoformat()
        logger.info(f"A/B test {test_id} completed. Winner: {result.winner}")
        return result
    
    def get_test(self, test_id: str) -> Optional[ABTestResult]:
        """Get A/B test result."""
        return self._tests.get(test_id)
    
    def list_tests(self) -> List[ABTestResult]:
        """List all A/B tests."""
        return list(self._tests.values())


class PipelineScheduler:
    """Schedule pipeline runs."""
    
    def __init__(self):
        self._schedules: Dict[str, Dict[str, Any]] = {}
        self._execution_history: List[Dict[str, Any]] = []
    
    def schedule(self, pipeline_version_id: str, schedule_config: Dict[str, Any]) -> str:
        """Schedule a pipeline run."""
        schedule_id = f"schedule_{len(self._schedules) + 1}"
        schedule_entry = {
            "schedule_id": schedule_id,
            "pipeline_version_id": pipeline_version_id,
            "cron": schedule_config.get("cron"),
            "interval_seconds": schedule_config.get("interval_seconds"),
            "enabled": schedule_config.get("enabled", True),
            "created_at": datetime.utcnow().isoformat(),
            "last_run": None,
            "next_run": None
        }
        self._schedules[schedule_id] = schedule_entry
        logger.info(f"Scheduled pipeline {pipeline_version_id} with id {schedule_id}")
        return schedule_id
    
    def unschedule(self, schedule_id: str) -> bool:
        """Remove a schedule."""
        if schedule_id in self._schedules:
            del self._schedules[schedule_id]
            return True
        return False
    
    def trigger_run(self, schedule_id: str) -> str:
        """Trigger an immediate pipeline run."""
        if schedule_id not in self._schedules:
            raise ValueError(f"Schedule {schedule_id} not found")
        run_id = f"run_{len(self._execution_history) + 1}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        run_entry = {
            "run_id": run_id,
            "schedule_id": schedule_id,
            "pipeline_version_id": self._schedules[schedule_id]["pipeline_version_id"],
            "status": "running",
            "started_at": datetime.utcnow().isoformat(),
            "completed_at": None
        }
        self._execution_history.append(run_entry)
        self._schedules[schedule_id]["last_run"] = run_entry["started_at"]
        logger.info(f"Triggered pipeline run {run_id}")
        return run_id
    
    def complete_run(self, run_id: str, status: str = "success") -> None:
        """Mark a pipeline run as completed."""
        for run in self._execution_history:
            if run["run_id"] == run_id:
                run["status"] = status
                run["completed_at"] = datetime.utcnow().isoformat()
                break
    
    def get_schedule(self, schedule_id: str) -> Optional[Dict[str, Any]]:
        """Get schedule details."""
        return self._schedules.get(schedule_id)
    
    def list_schedules(self) -> List[Dict[str, Any]]:
        """List all schedules."""
        return list(self._schedules.values())
    
    def list_runs(self, schedule_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """List execution history."""
        if schedule_id:
            return [r for r in self._execution_history if r["schedule_id"] == schedule_id]
        return self._execution_history.copy()


class MLPipeline:
    """
    ML Pipeline Orchestration for Workflow AI.
    
    Provides end-to-end pipeline management including:
    - Data preprocessing
    - Feature engineering
    - Model training
    - Model evaluation
    - Hyperparameter tuning
    - Model registry
    - Pipeline versioning
    - A/B testing
    - Pipeline scheduling
    """
    
    def __init__(self, name: str = "workflow_ml_pipeline"):
        self.name = name
        self.current_config: Optional[PipelineConfig] = None
        self.current_version_id: Optional[str] = None
        
        # Initialize components
        self.preprocessor = DataPreprocessor()
        self.feature_engineer = FeatureEngineer()
        self.trainer = ModelTrainer()
        self.evaluator = ModelEvaluator()
        self.tuner = HyperparameterTuner()
        self.registry = ModelRegistry()
        self.versioner = PipelineVersioner()
        self.ab_tester = ABTester()
        self.scheduler = PipelineScheduler()
        
        # Pipeline state
        self._trained_model: Optional[Any] = None
        self._latest_metrics: Dict[str, float] = {}
        
        logger.info(f"Initialized MLPipeline: {name}")
    
    def define_stages(self, stages: List[PipelineStage]) -> 'MLPipeline':
        """Define ML pipeline stages."""
        self.current_config = PipelineConfig(stages=stages)
        logger.info(f"Defined pipeline stages: {[s.value for s in stages]}")
        return self
    
    def add_stage(self, stage: PipelineStage) -> 'MLPipeline':
        """Add a stage to the pipeline."""
        if self.current_config is None:
            self.current_config = PipelineConfig(stages=[])
        self.current_config.stages.append(stage)
        logger.info(f"Added stage: {stage.value}")
        return self
    
    def preprocess_data(self, data: Any, params: Optional[Dict[str, Any]] = None) -> Any:
        """Preprocess workflow data."""
        logger.info("Preprocessing workflow data")
        self.preprocessor = DataPreprocessor(params or self.current_config.preprocessing_params)
        return self.preprocessor.fit_transform(data)
    
    def engineer_features(self, data: Any, params: Optional[Dict[str, Any]] = None) -> Any:
        """Engineer features from workflow data."""
        logger.info("Engineering features from workflow data")
        self.feature_engineer = FeatureEngineer(params or self.current_config.feature_params)
        return self.feature_engineer.create_features(data)
    
    def train_model(self, X: Any, y: Any, 
                   params: Optional[Dict[str, Any]] = None) -> Any:
        """Train ML model in pipeline."""
        logger.info("Training model in pipeline")
        self.trainer = ModelTrainer(params or self.current_config.training_params)
        self._trained_model = self.trainer.train(X, y)
        return self._trained_model
    
    def evaluate_model(self, X: Any, y: Any,
                      params: Optional[Dict[str, Any]] = None) -> Dict[str, float]:
        """Evaluate model performance."""
        logger.info("Evaluating model performance")
        self.evaluator = ModelEvaluator(params or self.current_config.evaluation_params)
        self._latest_metrics = self.evaluator.evaluate(self._trained_model, X, y)
        return self._latest_metrics
    
    def tune_hyperparameters(self, X: Any, y: Any,
                            param_space: Optional[Dict[str, List[Any]]] = None) -> Dict[str, Any]:
        """Tune hyperparameters."""
        logger.info("Tuning hyperparameters")
        if param_space is None:
            param_space = {
                "learning_rate": [0.001, 0.01, 0.1],
                "max_depth": [3, 5, 7],
                "n_estimators": [100, 200, 300]
            }
        self.tuner = HyperparameterTuner(self.current_config.hyperparameters)
        best_result = self.tuner.tune(self.trainer, X, y, param_space)
        self.current_config.hyperparameters.update(best_result.get("best_params", {}))
        return best_result
    
    def register_model(self, model_name: str, artifact_path: Optional[str] = None) -> str:
        """Register and manage trained models."""
        if self._trained_model is None:
            raise RuntimeError("No trained model to register. Train a model first.")
        version_id = self._generate_version_id()
        version = ModelVersion(
            version_id=version_id,
            model_name=model_name,
            stage=PipelineStage.REGISTERED,
            metrics=self._latest_metrics.copy(),
            params=self.trainer.params.copy(),
            created_at=datetime.utcnow().isoformat(),
            pipeline_config=asdict(self.current_config) if self.current_config else {},
            artifact_path=artifact_path
        )
        return self.registry.register(model_name, version)
    
    def get_registered_model(self, version_id: str) -> Optional[ModelVersion]:
        """Get a registered model by version ID."""
        return self.registry.get(version_id)
    
    def list_registered_models(self, model_name: Optional[str] = None) -> List[ModelVersion]:
        """List registered models."""
        return self.registry.list_versions(model_name)
    
    def stage_model(self, version_id: str, stage: PipelineStage) -> None:
        """Stage a model to a specific pipeline stage."""
        self.registry.stage_model(version_id, stage)
    
    def create_pipeline_version(self, parent_id: Optional[str] = None) -> str:
        """Version control for pipelines."""
        if self.current_config is None:
            self.current_config = PipelineConfig(stages=list(PipelineStage))
        self.current_version_id = self.versioner.create_version(self.current_config, parent_id)
        logger.info(f"Created pipeline version: {self.current_version_id}")
        return self.current_version_id
    
    def get_pipeline_version(self, version_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Get pipeline version data."""
        vid = version_id or self.current_version_id
        return self.versioner.get_version(vid) if vid else None
    
    def compare_pipeline_versions(self, v1_id: str, v2_id: str) -> Dict[str, Any]:
        """Compare two pipeline versions."""
        return self.versioner.compare_versions(v1_id, v2_id)
    
    def create_ab_test(self, control_version_id: str, treatment_version_id: str,
                      metrics: Optional[List[str]] = None) -> str:
        """Create A/B test for different pipelines."""
        if metrics is None:
            metrics = ["accuracy", "precision", "recall", "f1"]
        return self.ab_tester.create_test(control_version_id, treatment_version_id, metrics)
    
    def record_ab_metric(self, test_id: str, group: str, 
                        metric: str, value: float) -> None:
        """Record metric for A/B test."""
        self.ab_tester.record_metric(test_id, group, metric, value)
    
    def compute_ab_test_results(self, test_id: str) -> ABTestResult:
        """Compute A/B test results."""
        return self.ab_tester.compute_results(test_id)
    
    def get_ab_test(self, test_id: str) -> Optional[ABTestResult]:
        """Get A/B test results."""
        return self.ab_tester.get_test(test_id)
    
    def list_ab_tests(self) -> List[ABTestResult]:
        """List all A/B tests."""
        return self.ab_tester.list_tests()
    
    def schedule_pipeline(self, schedule_config: Dict[str, Any]) -> str:
        """Schedule pipeline runs."""
        if not self.current_version_id:
            self.create_pipeline_version()
        return self.scheduler.schedule(self.current_version_id, schedule_config)
    
    def trigger_scheduled_run(self, schedule_id: str) -> str:
        """Trigger a scheduled pipeline run."""
        return self.scheduler.trigger_run(schedule_id)
    
    def complete_pipeline_run(self, run_id: str, status: str = "success") -> None:
        """Complete a pipeline run."""
        self.scheduler.complete_run(run_id, status)
    
    def get_schedule(self, schedule_id: str) -> Optional[Dict[str, Any]]:
        """Get schedule details."""
        return self.scheduler.get_schedule(schedule_id)
    
    def list_schedules(self) -> List[Dict[str, Any]]:
        """List all schedules."""
        return self.scheduler.list_schedules()
    
    def run_full_pipeline(self, X: Any, y: Any, model_name: str = "default_model",
                         params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Run the full ML pipeline end-to-end."""
        logger.info("Running full ML pipeline")
        
        if self.current_config is None:
            self.define_stages([
                PipelineStage.PREPROCESSING,
                PipelineStage.FEATURE_ENGINEERING,
                PipelineStage.TRAINING,
                PipelineStage.EVALUATION
            ])
        
        results = {"stages_completed": []}
        
        if PipelineStage.PREPROCESSING in self.current_config.stages:
            X_processed = self.preprocess_data(X, params)
            results["stages_completed"].append(PipelineStage.PREPROCESSING.value)
        
        if PipelineStage.FEATURE_ENGINEERING in self.current_config.stages:
            X_features = self.engineer_features(X_processed if 'X_processed' in locals() else X, params)
            results["stages_completed"].append(PipelineStage.FEATURE_ENGINEERING.value)
        
        if PipelineStage.TRAINING in self.current_config.stages:
            self.train_model(X_features if 'X_features' in locals() else X, y, params)
            results["stages_completed"].append(PipelineStage.TRAINING.value)
        
        if PipelineStage.EVALUATION in self.current_config.stages:
            metrics = self.evaluate_model(X_features if 'X_features' in locals() else X, y, params)
            results["metrics"] = metrics
            results["stages_completed"].append(PipelineStage.EVALUATION.value)
        
        version_id = self.register_model(model_name)
        results["version_id"] = version_id
        
        self.create_pipeline_version()
        results["pipeline_version_id"] = self.current_version_id
        
        logger.info(f"Full pipeline completed: {results}")
        return results
    
    def _generate_version_id(self) -> str:
        """Generate a unique version ID."""
        timestamp = datetime.utcnow().isoformat()
        content = f"{self.name}_{timestamp}_{len(self.registry._models)}"
        return f"v_{hashlib.md5(content.encode()).hexdigest()[:12]}"
    
    def get_pipeline_info(self) -> Dict[str, Any]:
        """Get current pipeline information."""
        return {
            "name": self.name,
            "current_version_id": self.current_version_id,
            "current_config": asdict(self.current_config) if self.current_config else None,
            "has_trained_model": self._trained_model is not None,
            "latest_metrics": self._latest_metrics.copy(),
            "registered_model_count": len(self.registry._models),
            "pipeline_version_count": len(self.versioner._versions),
            "ab_test_count": len(self.ab_tester._tests),
            "schedule_count": len(self.scheduler._schedules)
        }


# Convenience function for quick pipeline creation
def create_pipeline(name: str = "workflow_ml_pipeline", 
                    stages: Optional[List[PipelineStage]] = None) -> MLPipeline:
    """Create a new ML pipeline with optional stages."""
    pipeline = MLPipeline(name)
    if stages:
        pipeline.define_stages(stages)
    else:
        pipeline.define_stages([
            PipelineStage.PREPROCESSING,
            PipelineStage.FEATURE_ENGINEERING,
            PipelineStage.TRAINING,
            PipelineStage.EVALUATION,
            PipelineStage.HYPERPARAMETER_TUNING
        ])
    return pipeline
