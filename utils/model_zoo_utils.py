"""
Model registry and model zoo utilities.

Provides model loading, saving, and registry functionality
for managing trained models.
"""
from __future__ import annotations

import json
import os
import pickle
from typing import Any, Callable, Dict, List, Optional, Type

import numpy as np


class ModelRegistry:
    """Registry for models and their configurations."""

    def __init__(self, name: str = "model_zoo"):
        self.name = name
        self._models = {}
        self._configs = {}

    def register(self, name: str, model_class: Type = None, config: dict = None):
        """
        Register a model.

        Args:
            name: Model name
            model_class: Model class
            config: Model configuration
        """
        def decorator(cls_or_fn):
            self._models[name] = cls_or_fn
            if config:
                self._configs[name] = config
            return cls_or_fn

        if model_class is not None:
            self._models[name] = model_class
            if config:
                self._configs[name] = config
            return model_class
        return decorator

    def get(self, name: str) -> Optional[Type]:
        """Get registered model class."""
        return self._models.get(name)

    def list_models(self) -> List[str]:
        """List all registered models."""
        return list(self._models.keys())

    def get_config(self, name: str) -> Optional[dict]:
        """Get model configuration."""
        return self._configs.get(name)


_model_registry = ModelRegistry()


def register_model(name: str, config: dict = None):
    """Decorator to register a model."""
    return _model_registry.register(name, config=config)


def get_model(name: str):
    """Get a registered model."""
    return _model_registry.get(name)


def list_models():
    """List all registered models."""
    return _model_registry.list_models()


class ModelCheckpoint:
    """Model checkpoint manager."""

    def __init__(
        self,
        checkpoint_dir: str,
        save_best_only: bool = True,
        maximize_metric: bool = True,
        metric_name: str = "val_loss",
    ):
        self.checkpoint_dir = checkpoint_dir
        self.save_best_only = save_best_only
        self.maximize_metric = maximize_metric
        self.metric_name = metric_name
        self.best_value = float("-inf") if maximize_metric else float("inf")
        self.checkpoints = []
        os.makedirs(checkpoint_dir, exist_ok=True)

    def save(
        self, model_state: Dict, metric_value: float, epoch: int, filename: str = None
    ) -> Optional[str]:
        """
        Save checkpoint if metric improved.

        Args:
            model_state: Model state dict
            metric_value: Current metric value
            epoch: Current epoch
            filename: Custom filename

        Returns:
            Path to saved checkpoint or None
        """
        if self.save_best_only:
            if (self.maximize_metric and metric_value <= self.best_value) or (
                not self.maximize_metric and metric_value >= self.best_value
            ):
                return None
        self.best_value = metric_value
        if filename is None:
            filename = f"checkpoint_epoch{epoch}_{self.metric_name}{metric_value:.4f}.pt"
        filepath = os.path.join(self.checkpoint_dir, filename)
        with open(filepath, "wb") as f:
            pickle.dump({"state": model_state, "metric": metric_value, "epoch": epoch}, f)
        self.checkpoints.append({"path": filepath, "epoch": epoch, "metric": metric_value})
        return filepath

    def load(self, filepath: str) -> Dict:
        """Load checkpoint."""
        with open(filepath, "rb") as f:
            return pickle.load(f)

    def load_best(self) -> Optional[Dict]:
        """Load best checkpoint."""
        if not self.checkpoints:
            return None
        best = min(self.checkpoints, key=lambda x: x["metric"] if not self.maximize_metric else -x["metric"])
        return self.load(best["path"])


class ModelLoader:
    """Load models from various formats."""

    @staticmethod
    def load_pickle(filepath: str) -> Any:
        """Load model from pickle file."""
        with open(filepath, "rb") as f:
            return pickle.load(f)

    @staticmethod
    def load_json(filepath: str) -> Dict:
        """Load model config from JSON."""
        with open(filepath, "r") as f:
            return json.load(f)

    @staticmethod
    def load_numpy(filepath: str) -> np.ndarray:
        """Load numpy array."""
        return np.load(filepath)

    @staticmethod
    def load_onnx(filepath: str) -> Any:
        """Load ONNX model (placeholder)."""
        raise NotImplementedError("ONNX loading requires onnxruntime")

    @staticmethod
    def load_tensorflow(filepath: str) -> Any:
        """Load TensorFlow model (placeholder)."""
        raise NotImplementedError("TensorFlow loading requires tensorflow package")

    @staticmethod
    def load_torch(filepath: str) -> Any:
        """Load PyTorch model (placeholder)."""
        raise NotImplementedError("PyTorch loading requires torch package")


class ModelSaver:
    """Save models in various formats."""

    @staticmethod
    def save_pickle(obj: Any, filepath: str) -> None:
        """Save model as pickle."""
        with open(filepath, "wb") as f:
            pickle.dump(obj, f)

    @staticmethod
    def save_json(obj: Dict, filepath: str) -> None:
        """Save model config as JSON."""
        with open(filepath, "w") as f:
            json.dump(obj, f, indent=2)

    @staticmethod
    def save_numpy(arr: np.ndarray, filepath: str) -> None:
        """Save numpy array."""
        np.save(filepath, arr)


class ModelConfig:
    """Model configuration container."""

    def __init__(self, **kwargs):
        self._config = kwargs

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        return self._config.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """Set configuration value."""
        self._config[key] = value

    def update(self, **kwargs) -> None:
        """Update configuration."""
        self._config.update(kwargs)

    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return self._config.copy()

    def __repr__(self) -> str:
        return f"ModelConfig({self._config})"


class ModelSummary:
    """Generate model summary information."""

    def __init__(self, model: Any):
        self.model = model
        self._layers = []
        self._total_params = 0
        self._trainable_params = 0

    def add_layer(self, name: str, output_shape: tuple, num_params: int, trainable: bool = True):
        """Add a layer to the summary."""
        self._layers.append(
            {
                "name": name,
                "output_shape": output_shape,
                "num_params": num_params,
                "trainable": trainable,
            }
        )
        self._total_params += num_params
        if trainable:
            self._trainable_params += num_params

    def summary(self) -> str:
        """Generate text summary."""
        lines = ["Model Summary", "=" * 50]
        lines.append(f"{'Layer':<30} {'Output Shape':<20} {'Params':<12}")
        lines.append("-" * 50)
        for layer in self._layers:
            shape_str = str(layer["output_shape"])
            lines.append(f"{layer['name']:<30} {shape_str:<20} {layer['num_params']:<12}")
        lines.append("-" * 50)
        lines.append(f"{'Total params':<30} {self._total_params:,}")
        lines.append(f"{'Trainable params':<30} {self._trainable_params:,}")
        lines.append(f"{'Non-trainable params':<30} {self._total_params - self._trainable_params:,}")
        return "\n".join(lines)


def count_parameters(model: Any) -> Dict[str, int]:
    """
    Count model parameters.

    Args:
        model: Model with layers or named_parameters

    Returns:
        Dictionary with parameter counts
    """
    if hasattr(model, "named_parameters"):
        total = 0
        trainable = 0
        for name, param in model.named_parameters():
            n = param.size if hasattr(param, "size") else np.prod(param.shape)
            total += n
            if param.requires_grad:
                trainable += n
        return {"total": total, "trainable": trainable, "non_trainable": total - trainable}
    return {"total": 0, "trainable": 0, "non_trainable": 0}


def get_model_size_mb(model: Any) -> float:
    """
    Estimate model size in megabytes.

    Args:
        model: Model object

    Returns:
        Size in MB
    """
    if hasattr(model, "state_dict"):
        state = model.state_dict()
        size_bytes = sum(v.nbytes if hasattr(v, "nbytes") else np.prod(v.shape) * 4 for v in state.values())
        return size_bytes / (1024 * 1024)
    return 0.0


class PretrainedModelLoader:
    """Load pretrained models from various sources."""

    _model_urls = {
        "resnet18": "https://download.pytorch.org/models/resnet18-f37072fd.pth",
        "resnet50": "https://download.pytorch.org/models/resnet50-0676ba61.pth",
        "vgg16": "https://download.pytorch.org/models/vgg16-397923af.pth",
        "bert-base": "https://huggingface.co/bert-base-uncased/resolve/main/pytorch_model.bin",
    }

    @classmethod
    def load_pretrained(cls, model_name: str, save_dir: str = None) -> Optional[Dict]:
        """
        Load pretrained model weights.

        Args:
            model_name: Name of pretrained model
            save_dir: Directory to cache weights

        Returns:
            Model weights dict or None
        """
        if model_name not in cls._model_urls:
            return None
        url = cls._model_urls[model_name]
        if save_dir:
            os.makedirs(save_dir, exist_ok=True)
            filepath = os.path.join(save_dir, f"{model_name}.pth")
            if os.path.exists(filepath):
                return ModelLoader.load_pickle(filepath)
        return None


def clone_model(model: Any) -> Any:
    """
    Create a deep copy of a model.

    Args:
        model: Model to clone

    Returns:
        Cloned model
    """
    import copy
    return copy.deepcopy(model)


def freeze_layers(model: Any, layer_names: List[str]) -> None:
    """
    Freeze specified layers in model.

    Args:
        model: Model with named_parameters
        layer_names: List of layer names to freeze
    """
    if hasattr(model, "named_parameters"):
        for name, param in model.named_parameters():
            for layer_name in layer_names:
                if layer_name in name:
                    param.requires_grad = False


def unfreeze_layers(model: Any, layer_names: List[str]) -> None:
    """
    Unfreeze specified layers in model.

    Args:
        model: Model with named_parameters
        layer_names: List of layer names to unfreeze
    """
    if hasattr(model, "named_parameters"):
        for name, param in model.named_parameters():
            for layer_name in layer_names:
                if layer_name in name:
                    param.requires_grad = True
