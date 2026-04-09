"""
Data ML Pipeline Action Module

Provides machine learning pipeline orchestration with data preprocessing,
feature engineering, model training, evaluation, and inference stages.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class PipelineStage(Enum):
    """ML pipeline stages."""

    DATA_LOADING = "data_loading"
    PREPROCESSING = "preprocessing"
    FEATURE_ENGINEERING = "feature_engineering"
    TRAIN = "train"
    EVALUATE = "evaluate"
    INFERENCE = "inference"


class ModelStatus(Enum):
    """Model training status."""

    UNTRAINED = "untrained"
    TRAINING = "training"
    TRAINED = "trained"
    EVALUATED = "evaluated"
    FAILED = "failed"


@dataclass
class PipelineStep:
    """A step in an ML pipeline."""

    step_id: str
    stage: PipelineStage
    handler: Callable[..., Any]
    input_key: str = ""
    output_key: str = ""
    config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineResult:
    """Result of a pipeline step."""

    step_id: str
    success: bool
    output: Any = None
    duration_ms: float = 0.0
    error: Optional[str] = None


@dataclass
class MLModel:
    """A trained ML model."""

    model_id: str
    name: str
    model_type: str
    version: int
    status: ModelStatus
    metrics: Dict[str, float] = field(default_factory=dict)
    trained_at: Optional[float] = None
    config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MLPipelineConfig:
    """Configuration for ML pipeline."""

    default_timeout: float = 300.0
    enable_caching: bool = True
    save_models: bool = True
    evaluation_metrics: List[str] = field(default_factory=lambda: ["accuracy", "loss"])


class DataMLPipelineAction:
    """
    ML pipeline action for model training and inference.

    Features:
    - Multi-stage pipeline orchestration
    - Data preprocessing and feature engineering
    - Model training and evaluation
    - Pipeline step caching
    - Model versioning
    - Metric tracking
    - Inference serving

    Usage:
        pipeline = DataMLPipelineAction(config)
        
        pipeline.add_step(PipelineStage.DATA_LOADING, data_loader)
        pipeline.add_step(PipelineStage.PREPROCESSING, preprocessor)
        pipeline.add_step(PipelineStage.TRAIN, trainer)
        
        model = await pipeline.train(data)
        predictions = await pipeline.predict(model, test_data)
    """

    def __init__(self, config: Optional[MLPipelineConfig] = None):
        self.config = config or MLPipelineConfig()
        self._steps: List[PipelineStep] = []
        self._models: Dict[str, MLModel] = {}
        self._cache: Dict[str, Any] = {}
        self._stats = {
            "pipelines_executed": 0,
            "steps_completed": 0,
            "steps_failed": 0,
            "models_trained": 0,
        }

    def add_step(
        self,
        stage: PipelineStage,
        handler: Callable[..., Any],
        input_key: str = "",
        output_key: str = "",
    ) -> PipelineStep:
        """Add a step to the pipeline."""
        step_id = f"step_{uuid.uuid4().hex[:8]}"
        step = PipelineStep(
            step_id=step_id,
            stage=stage,
            handler=handler,
            input_key=input_key,
            output_key=output_key,
        )
        self._steps.append(step)
        return step

    async def execute(
        self,
        input_data: Any,
        start_stage: Optional[PipelineStage] = None,
    ) -> Dict[str, Any]:
        """Execute the pipeline."""
        self._stats["pipelines_executed"] += 1
        context: Dict[str, Any] = {"input": input_data}
        start_idx = 0

        if start_stage:
            for i, step in enumerate(self._steps):
                if step.stage == start_stage:
                    start_idx = i
                    break

        for step in self._steps[start_idx:]:
            cache_key = f"{step.step_id}:{hash(str(input_data))}"
            if self.config.enable_caching and cache_key in self._cache:
                logger.info(f"Using cached result for {step.stage.value}")
                context[step.output_key or step.stage.value] = self._cache[cache_key]
                continue

            result = await self._execute_step(step, context)
            self._stats["steps_completed"] += 1

            if not result.success:
                self._stats["steps_failed"] += 1
                logger.error(f"Step {step.stage.value} failed: {result.error}")
                break

            if result.output:
                key = step.output_key or step.stage.value
                context[key] = result.output

                if self.config.enable_caching:
                    self._cache[cache_key] = result.output

        return context

    async def _execute_step(
        self,
        step: PipelineStep,
        context: Dict[str, Any],
    ) -> PipelineResult:
        """Execute a single pipeline step."""
        result = PipelineResult(step_id=step.step_id, success=True)
        start_time = time.time()

        try:
            input_data = context.get(step.input_key) if step.input_key else context.get("input")

            if asyncio.iscoroutinefunction(step.handler):
                output = await asyncio.wait_for(
                    step.handler(input_data, **step.config),
                    timeout=self.config.default_timeout,
                )
            else:
                output = step.handler(input_data, **step.config)

            result.output = output
            result.success = True

        except asyncio.TimeoutError:
            result.success = False
            result.error = f"Step timed out after {self.config.default_timeout}s"
        except Exception as e:
            result.success = False
            result.error = str(e)

        result.duration_ms = (time.time() - start_time) * 1000
        return result

    async def train(
        self,
        training_data: Any,
        model_name: str,
        model_type: str = "generic",
    ) -> MLModel:
        """Train a model through the pipeline."""
        model_id = f"model_{uuid.uuid4().hex[:12]}"

        model = MLModel(
            model_id=model_id,
            name=model_name,
            model_type=model_type,
            version=1,
            status=ModelStatus.TRAINING,
        )
        self._models[model_id] = model

        context = await self.execute(training_data)

        model.status = ModelStatus.TRAINED
        model.trained_at = time.time()
        self._stats["models_trained"] += 1

        return model

    async def predict(
        self,
        model_id: str,
        input_data: Any,
    ) -> Any:
        """Run inference with a trained model."""
        model = self._models.get(model_id)
        if model is None:
            raise ValueError(f"Model not found: {model_id}")

        if model.status != ModelStatus.TRAINED:
            raise ValueError(f"Model {model_id} is not trained (status: {model.status.value})")

        predict_step = None
        for step in self._steps:
            if step.stage == PipelineStage.INFERENCE:
                predict_step = step
                break

        if predict_step is None:
            raise ValueError("No inference step defined in pipeline")

        context = {"input": input_data}
        result = await self._execute_step(predict_step, context)

        if not result.success:
            raise RuntimeError(f"Inference failed: {result.error}")

        return result.output

    def register_model(
        self,
        model: MLModel,
    ) -> None:
        """Register a trained model."""
        self._models[model.model_id] = model

    def get_model(self, model_id: str) -> Optional[MLModel]:
        """Get a model by ID."""
        return self._models.get(model_id)

    def update_model_metrics(
        self,
        model_id: str,
        metrics: Dict[str, float],
    ) -> None:
        """Update model evaluation metrics."""
        model = self._models.get(model_id)
        if model:
            model.metrics.update(metrics)
            model.status = ModelStatus.EVALUATED

    def get_stats(self) -> Dict[str, Any]:
        """Get ML pipeline statistics."""
        return {
            **self._stats.copy(),
            "total_steps": len(self._steps),
            "total_models": len(self._models),
            "cache_size": len(self._cache),
        }


async def demo_ml_pipeline():
    """Demonstrate ML pipeline."""
    config = MLPipelineConfig()
    pipeline = DataMLPipelineAction(config)

    async def load_data(data):
        await asyncio.sleep(0.05)
        return [{"features": [1, 2, 3], "label": 0}, {"features": [4, 5, 6], "label": 1}]

    async def preprocess(data):
        await asyncio.sleep(0.05)
        return data

    async def train(data):
        await asyncio.sleep(0.05)
        return {"weights": [0.1, 0.2, 0.3], "bias": 0.5}

    pipeline.add_step(PipelineStage.DATA_LOADING, load_data, output_key="raw_data")
    pipeline.add_step(PipelineStage.PREPROCESSING, preprocess, input_key="raw_data", output_key="processed_data")
    pipeline.add_step(PipelineStage.TRAIN, train, input_key="processed_data", output_key="model")

    result = await pipeline.execute([1, 2, 3])
    print(f"Pipeline output keys: {list(result.keys())}")
    print(f"Stats: {pipeline.get_stats()}")


if __name__ == "__main__":
    asyncio.run(demo_ml_pipeline())
