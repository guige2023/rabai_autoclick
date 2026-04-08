"""HuggingFace API action module for RabAI AutoClick.

Provides HuggingFace Hub operations including inference API,
model downloads, and dataset access.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class HuggingFaceInferenceAction(BaseAction):
    """Execute inference via HuggingFace Inference API.

    Supports text generation, embeddings, and other HF tasks.
    """
    action_type = "huggingface_inference"
    display_name = "HuggingFace推理"
    description = "HuggingFace API推理"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HuggingFace inference.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: HuggingFace API key
                - model: Model name or ID
                - task: Task type (text-generation, feature-extraction, etc.)
                - inputs: Input text or data
                - parameters: Optional task parameters

        Returns:
            ActionResult with inference results.
        """
        api_key = params.get('api_key') or os.environ.get('HF_API_KEY')
        model = params.get('model', '')
        task = params.get('task', '')
        inputs = params.get('inputs', '')
        parameters = params.get('parameters', {})

        if not api_key:
            return ActionResult(success=False, message="HF_API_KEY is required")
        if not model:
            return ActionResult(success=False, message="model is required")

        try:
            from huggingface_hub import InferenceClient
        except ImportError:
            return ActionResult(success=False, message="huggingface_hub not installed. Run: pip install huggingface_hub")

        client = InferenceClient(model=model, token=api_key)
        start = time.time()
        try:
            if task == 'text-generation' or not task:
                text = client.text_generation(inputs, **parameters)
                duration = time.time() - start
                return ActionResult(
                    success=True, message="Text generation completed",
                    data={'text': text, 'model': model}, duration=duration
                )
            elif task == 'feature-extraction':
                features = client.feature_extraction(inputs)
                duration = time.time() - start
                return ActionResult(
                    success=True, message="Feature extraction completed",
                    data={'features': features.tolist() if hasattr(features, 'tolist') else features, 'model': model}, duration=duration
                )
            elif task == 'fill-mask':
                result = client.fill_mask(inputs, **parameters)
                duration = time.time() - start
                return ActionResult(
                    success=True, message="Fill mask completed",
                    data={'predictions': result, 'model': model}, duration=duration
                )
            elif task == 'question-answering':
                result = client.question_answering(inputs, **parameters)
                duration = time.time() - start
                return ActionResult(
                    success=True, message="QA completed",
                    data={'answer': result}, duration=duration
                )
            elif task == 'summarization':
                result = client.summarization(inputs, **parameters)
                duration = time.time() - start
                return ActionResult(
                    success=True, message="Summarization completed",
                    data={'summary': result, 'model': model}, duration=duration
                )
            elif task == 'translation':
                result = client.translation(inputs, **parameters)
                duration = time.time() - start
                return ActionResult(
                    success=True, message="Translation completed",
                    data={'translation': result, 'model': model}, duration=duration
                )
            else:
                return ActionResult(success=False, message=f"Unsupported task: {task}")
        except Exception as e:
            return ActionResult(success=False, message=f"HuggingFace inference error: {str(e)}")


class HuggingFaceDatasetAction(BaseAction):
    """Load and access HuggingFace datasets."""
    action_type = "huggingface_dataset"
    display_name = "HuggingFace数据集"
    description = "HuggingFace数据集加载"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Load HuggingFace dataset.

        Args:
            context: Execution context.
            params: Dict with keys:
                - name: Dataset name (e.g. 'glue', 'squad')
                - split: Dataset split ('train', 'test', 'validation')
                - config: Optional config name
                - streaming: Use streaming mode

        Returns:
            ActionResult with dataset info.
        """
        name = params.get('name', '')
        split = params.get('split', 'train')
        config = params.get('config', None)
        streaming = params.get('streaming', False)

        if not name:
            return ActionResult(success=False, message="name is required")

        try:
            from datasets import load_dataset
        except ImportError:
            return ActionResult(success=False, message="datasets package not installed. Run: pip install datasets")

        start = time.time()
        try:
            dataset = load_dataset(name, config, split=split, streaming=streaming)
            num_samples = len(list(dataset.take(10))) if streaming else len(dataset)
            duration = time.time() - start
            return ActionResult(
                success=True, message=f"Loaded dataset with {num_samples} samples",
                data={
                    'name': name,
                    'split': split,
                    'num_samples': num_samples,
                    'features': list(dataset.features.keys()) if hasattr(dataset, 'features') else None,
                },
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Dataset load error: {str(e)}")


class HuggingFaceModelInfoAction(BaseAction):
    """Get information about HuggingFace models."""
    action_type = "huggingface_model_info"
    display_name = "HuggingFace模型信息"
    description = "HuggingFace模型信息查询"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get model metadata.

        Args:
            context: Execution context.
            params: Dict with keys:
                - model: Model ID (e.g. 'gpt2', 'bert-base-uncased')
                - api_key: HF API key (optional for public models)

        Returns:
            ActionResult with model metadata.
        """
        model = params.get('model', '')
        api_key = params.get('api_key') or os.environ.get('HF_API_KEY')

        if not model:
            return ActionResult(success=False, message="model is required")

        try:
            from huggingface_hub import model_info
        except ImportError:
            return ActionResult(success=False, message="huggingface_hub not installed")

        start = time.time()
        try:
            info = model_info(model, token=api_key)
            duration = time.time() - start
            return ActionResult(
                success=True, message="Model info retrieved",
                data={
                    'id': info.id,
                    'modelId': info.modelId,
                    'sha': info.sha,
                    'pipeline_tag': info.pipeline_tag,
                    'tags': info.tags,
                    'downloads': info.downloads,
                    'likes': info.likes,
                },
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Model info error: {str(e)}")
