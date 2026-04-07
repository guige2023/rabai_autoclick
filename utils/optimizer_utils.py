"""
Neural network optimizer utilities.

Provides implementations of common gradient descent optimizers
including SGD, Adam, RMSprop, Adagrad, and momentum-based methods.
"""
from __future__ import annotations

from typing import Callable, Dict, Sequence, Tuple

import numpy as np


class Optimizer:
    """Base optimizer class."""

    def __init__(self, lr: float = 0.01):
        self.lr = lr

    def update(self, params: np.ndarray, grads: np.ndarray, state: Dict) -> Tuple[np.ndarray, Dict]:
        raise NotImplementedError


class SGD(Optimizer):
    """Stochastic Gradient Descent optimizer."""

    def __init__(self, lr: float = 0.01, momentum: float = 0.0, nesterov: bool = False):
        super().__init__(lr)
        self.momentum = momentum
        self.nesterov = nesterov

    def update(self, params: np.ndarray, grads: np.ndarray, state: Dict) -> Tuple[np.ndarray, Dict]:
        velocity = state.get("velocity", np.zeros_like(params))
        if self.momentum > 0:
            velocity = self.momentum * velocity - self.lr * grads
            if self.nesterov:
                grads = grads + self.momentum * velocity
            else:
                grads = velocity
        else:
            grads = -self.lr * grads
        new_params = params + grads
        state["velocity"] = velocity
        return new_params, state


class Adam(Optimizer):
    """Adam optimizer with adaptive learning rates."""

    def __init__(
        self,
        lr: float = 0.001,
        beta1: float = 0.9,
        beta2: float = 0.999,
        eps: float = 1e-8,
    ):
        super().__init__(lr)
        self.beta1 = beta1
        self.beta2 = beta2
        self.eps = eps

    def update(self, params: np.ndarray, grads: np.ndarray, state: Dict) -> Tuple[np.ndarray, Dict]:
        t = state.get("t", 0) + 1
        m = state.get("m", np.zeros_like(params))
        v = state.get("v", np.zeros_like(params))

        m = self.beta1 * m + (1 - self.beta1) * grads
        v = self.beta2 * v + (1 - self.beta2) * (grads ** 2)

        m_hat = m / (1 - self.beta1 ** t)
        v_hat = v / (1 - self.beta2 ** t)

        update = self.lr * m_hat / (np.sqrt(v_hat) + self.eps)
        new_params = params - update

        state.update({"t": t, "m": m, "v": v})
        return new_params, state


class RMSprop(Optimizer):
    """RMSprop optimizer with adaptive learning rates."""

    def __init__(self, lr: float = 0.01, rho: float = 0.9, eps: float = 1e-8):
        super().__init__(lr)
        self.rho = rho
        self.eps = eps

    def update(self, params: np.ndarray, grads: np.ndarray, state: Dict) -> Tuple[np.ndarray, Dict]:
        cache = state.get("cache", np.zeros_like(params))
        cache = self.rho * cache + (1 - self.rho) * (grads ** 2)
        update = grads / (np.sqrt(cache) + self.eps)
        new_params = params - self.lr * update
        state["cache"] = cache
        return new_params, state


class Adagrad(Optimizer):
    """Adagrad optimizer with per-parameter learning rates."""

    def __init__(self, lr: float = 0.01, eps: float = 1e-8):
        super().__init__(lr)
        self.eps = eps

    def update(self, params: np.ndarray, grads: np.ndarray, state: Dict) -> Tuple[np.ndarray, Dict]:
        grad_sum_sq = state.get("grad_sum_sq", np.zeros_like(params))
        grad_sum_sq += grads ** 2
        update = grads / (np.sqrt(grad_sum_sq) + self.eps)
        new_params = params - self.lr * update
        state["grad_sum_sq"] = grad_sum_sq
        return new_params, state


class AdamW(Optimizer):
    """AdamW optimizer with weight decay regularization."""

    def __init__(
        self,
        lr: float = 0.001,
        beta1: float = 0.9,
        beta2: float = 0.999,
        eps: float = 1e-8,
        wd: float = 0.01,
    ):
        super().__init__(lr)
        self.beta1 = beta1
        self.beta2 = beta2
        self.eps = eps
        self.wd = wd

    def update(self, params: np.ndarray, grads: np.ndarray, state: Dict) -> Tuple[np.ndarray, Dict]:
        t = state.get("t", 0) + 1
        m = state.get("m", np.zeros_like(params))
        v = state.get("v", np.zeros_like(params))

        m = self.beta1 * m + (1 - self.beta1) * grads
        v = self.beta2 * v + (1 - self.beta2) * (grads ** 2)

        m_hat = m / (1 - self.beta1 ** t)
        v_hat = v / (1 - self.beta2 ** t)

        update = self.lr * m_hat / (np.sqrt(v_hat) + self.eps) + self.wd * params
        new_params = params - update

        state.update({"t": t, "m": m, "v": v})
        return new_params, state


class GradientClipping:
    """Gradient clipping wrapper."""

    def __init__(self, clip_value: float = 1.0, norm_type: float = None):
        self.clip_value = clip_value
        self.norm_type = norm_type

    def clip(self, grads: np.ndarray) -> np.ndarray:
        if self.norm_type is not None:
            total_norm = np.linalg.norm(grads, ord=self.norm_type)
            clip_coef = self.clip_value / (total_norm + 1e-6)
            if clip_coef < 1:
                return grads * clip_coef
            return grads
        return np.clip(grads, -self.clip_value, self.clip_value)


def get_optimizer(name: str, lr: float = 0.01, **kwargs) -> Optimizer:
    """
    Get optimizer by name.

    Args:
        name: Optimizer name (sgd, adam, rmsprop, adagrad, adamw)
        lr: Learning rate (default: 0.01)
        **kwargs: Additional optimizer-specific arguments

    Returns:
        Optimizer instance

    Example:
        >>> opt = get_optimizer('adam', lr=0.001)
        >>> opt = get_optimizer('sgd', lr=0.1, momentum=0.9)
    """
    optimizers = {
        "sgd": SGD,
        "adam": Adam,
        "rmsprop": RMSprop,
        "adagrad": Adagrad,
        "adamw": AdamW,
    }
    name = name.lower()
    if name not in optimizers:
        raise ValueError(f"Unknown optimizer: {name}. Available: {list(optimizers.keys())}")
    return optimizers[name](lr=lr, **kwargs)


class LearningRateScheduler:
    """Learning rate scheduler base class."""

    def __init__(self, optimizer: Optimizer):
        self.optimizer = optimizer
        self.initial_lr = optimizer.lr

    def step(self, epoch: int) -> float:
        raise NotImplementedError


class StepLR(LearningRateScheduler):
    """Decays learning rate by gamma every step_size epochs."""

    def __init__(self, optimizer: Optimizer, step_size: int, gamma: float = 0.1):
        super().__init__(optimizer)
        self.step_size = step_size
        self.gamma = gamma

    def step(self, epoch: int) -> float:
        if epoch > 0 and epoch % self.step_size == 0:
            self.optimizer.lr *= self.gamma
        return self.optimizer.lr


class CosineAnnealingLR(LearningRateScheduler):
    """Cosine annealing learning rate schedule."""

    def __init__(self, optimizer: Optimizer, T_max: int, eta_min: float = 0):
        super().__init__(optimizer)
        self.T_max = T_max
        self.eta_min = eta_min

    def step(self, epoch: int) -> float:
        self.optimizer.lr = self.eta_min + (self.initial_lr - self.eta_min) * (1 + np.cos(np.pi * epoch / self.T_max)) / 2
        return self.optimizer.lr


class WarmupScheduler(LearningRateScheduler):
    """Linear warmup then constant learning rate."""

    def __init__(self, optimizer: Optimizer, warmup_epochs: int, constant_epochs: int = None):
        super().__init__(optimizer)
        self.warmup_epochs = warmup_epochs
        self.constant_epochs = constant_epochs or 0

    def step(self, epoch: int) -> float:
        if epoch < self.warmup_epochs:
            self.optimizer.lr = self.initial_lr * (epoch + 1) / self.warmup_epochs
        elif self.constant_epochs and epoch >= self.warmup_epochs + self.constant_epochs:
            pass
        return self.optimizer.lr
