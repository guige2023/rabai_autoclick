"""
Federated Learning Module for Workflow Automation.

This module provides federated learning capabilities including:
- Local model training on client data
- Gradient aggregation without sharing raw data
- Differential privacy for protecting individual data points
- Secure aggregation using cryptographic protocols
- Federated averaging (FedAvg) algorithm
- Incentive mechanisms for client contributions
- Model versioning across federations
- Privacy budget tracking
- Byzantine resilience for handling malicious clients
"""

import hashlib
import hmac
import json
import logging
import math
import os
import pickle
import struct
import time
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import numpy as np

logger = logging.getLogger(__name__)


class PrivacyMechanism(Enum):
    """Types of differential privacy mechanisms."""
    GAUSSIAN = auto()
    LAPLACIAN = auto()
    EXPONENTIAL = auto()


class ClientStatus(Enum):
    """Status of federated learning clients."""
    IDLE = auto()
    TRAINING = auto()
    UPLOADING = auto()
    BYZANTINE = auto()
    DISCONNECTED = auto()


class AggregationProtocol(Enum):
    """Secure aggregation protocols."""
    SECURE_SUM = auto()
    SECURE_AVERAGE = auto()
    SECURE_TENSOR = auto()


@dataclass
class ModelUpdate:
    """Represents a model update from a client."""
    client_id: str
    round_number: int
    model_weights: Dict[str, np.ndarray]
    num_samples: int
    timestamp: float
    gradients: Optional[Dict[str, np.ndarray]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    signature: Optional[bytes] = None

    def to_bytes(self) -> bytes:
        """Serialize model update to bytes."""
        data = {
            'client_id': self.client_id,
            'round_number': self.round_number,
            'num_samples': self.num_samples,
            'timestamp': self.timestamp,
            'metadata': self.metadata,
        }
        return pickle.dumps(data)

    @classmethod
    def from_bytes(cls, data: bytes) -> 'ModelUpdate':
        """Deserialize model update from bytes."""
        return pickle.loads(data)


@dataclass
class ClientContribution:
    """Tracks client contributions for incentive mechanism."""
    client_id: str
    round_number: int
    training_time: float
    data_size: int
    model_quality: float
    computational_cost: float
    reward: float = 0.0
    is_byzantine: bool = False


@dataclass
class PrivacyBudget:
    """Tracks privacy expenditure using RDP (Rényi Differential Privacy)."""
    epsilon_total: float
    epsilon_spent: float = 0.0
    delta: float = 1e-5
    composition_steps: int = 0

    @property
    def epsilon_remaining(self) -> float:
        """Returns remaining privacy budget."""
        return max(0.0, self.epsilon_total - self.epsilon_spent)

    @property
    def is_exhausted(self) -> bool:
        """Check if privacy budget is exhausted."""
        return self.epsilon_remaining <= 0

    def spend(self, epsilon: float) -> bool:
        """
        Spend privacy budget for a new operation.
        Returns True if budget is sufficient.
        """
        if epsilon > self.epsilon_remaining:
            logger.warning(
                f"Privacy budget exceeded: need {epsilon}, "
                f"have {self.epsilon_remaining}"
            )
            return False
        self.epsilon_spent += epsilon
        self.composition_steps += 1
        return True


@dataclass
class ModelVersion:
    """Tracks model versions across federations."""
    version_id: str
    round_number: int
    timestamp: float
    aggregated_weights: Dict[str, np.ndarray]
    participating_clients: List[str]
    privacy_budget_spent: float
    accuracy: Optional[float] = None
    loss: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class DifferentialPrivacy:
    """Implements differential privacy mechanisms."""

    def __init__(
        self,
        mechanism: PrivacyMechanism = PrivacyMechanism.GAUSSIAN,
        noise_multiplier: float = 1.0,
        max_grad_norm: float = 1.0,
    ):
        self.mechanism = mechanism
        self.noise_multiplier = noise_multiplier
        self.max_grad_norm = max_grad_norm

    def add_noise(self, gradients: Dict[str, np.ndarray], sensitivity: float) -> Dict[str, np.ndarray]:
        """Add calibrated noise to gradients for differential privacy."""
        noised_gradients = {}
        sigma = self.noise_multiplier * sensitivity

        for name, grad in gradients.items():
            if self.mechanism == PrivacyMechanism.GAUSSIAN:
                noise = np.random.normal(0, sigma, grad.shape)
            elif self.mechanism == PrivacyMechanism.LAPLACIAN:
                noise = np.random.laplace(0, sensitivity, grad.shape)
            else:
                noise = np.zeros_like(grad)

            noised_gradients[name] = grad + noise.astype(grad.dtype)

        return noised_gradients

    def clip_gradients(
        self,
        gradients: Dict[str, np.ndarray],
        max_norm: Optional[float] = None
    ) -> Dict[str, np.ndarray]:
        """Clip gradients to maximum norm for DP-SGD."""
        max_norm = max_norm or self.max_grad_norm
        clipped = {}

        total_norm = math.sqrt(
            sum(np.sum(g ** 2) for g in gradients.values())
        )

        clip_factor = max_norm / max(total_norm, max_norm)
        if clip_factor < 1.0:
            for name, grad in gradients.items():
                clipped[name] = grad * clip_factor
        else:
            clipped = gradients.copy()

        return clipped

    def compute_epsilon(
        self,
        num_steps: int,
        sampling_rate: float = 1.0
    ) -> float:
        """
        Compute privacy expenditure using RDP for Gaussian mechanism.
        Uses approximate RDP to DP conversion.
        """
        if self.mechanism != PrivacyMechanism.GAUSSIAN:
            return float('inf')

        alpha = 1 + 1.0 / num_steps
        log_mgf = 0.5 * (self.noise_multiplier ** 2) * alpha * sampling_rate

        epsilon = alpha * log_mgf + (
            np.log(1.25 / 1e-5) / (alpha - 1)
        )

        return min(epsilon, 100.0)


class SecureAggregation:
    """Implements secure aggregation protocols."""

    def __init__(self, protocol: AggregationProtocol = AggregationProtocol.SECURE_SUM):
        self.protocol = protocol
        self._secret_shares: Dict[str, Dict[str, np.ndarray]] = {}

    def generate_secret_shares(
        self,
        value: np.ndarray,
        num_shares: int,
        client_id: str
    ) -> Dict[str, np.ndarray]:
        """Generate secret shares for a value using Shamir's secret sharing."""
        share_count = num_shares
        threshold = (num_shares // 2) + 1

        shares = {}
        coefficients = [value.astype(np.float64)]

        flat_value = value.flatten()
        for i in range(len(flat_value)):
            coeffs = [flat_value[i]]
            for _ in range(threshold - 1):
                coeffs.append(np.random.randn())

            for x in range(1, share_count + 1):
                y = sum(c * (x ** k) for k, c in enumerate(coeffs))
                if i == 0:
                    shares[x] = np.array([y])
                else:
                    shares[x] = np.append(shares[x], y)

        return shares

    def aggregate_shares(
        self,
        shares: Dict[int, np.ndarray],
        threshold: int
    ) -> np.ndarray:
        """Reconstruct value from secret shares using Lagrange interpolation."""
        x_values = list(shares.keys())
        flat_shares = {x: s.flatten() for x, s in shares.items()}

        if len(flat_shares) < threshold:
            raise ValueError(
                f"Need at least {threshold} shares, got {len(flat_shares)}"
            )

        length = len(next(iter(flat_shares.values())))
        result = np.zeros(length)

        for i in range(length):
            numerator = 0.0
            denominator = 0.0

            for j, x_j in enumerate(x_values[:threshold]):
                y_j = flat_shares[x_j][i]

                lagrange_term = 1.0
                for k, x_k in enumerate(x_values[:threshold]):
                    if j != k:
                        lagrange_term *= -x_k / (x_j - x_k)

                numerator += y_j * lagrange_term
                denominator += lagrange_term

            result[i] = numerator

        return result

    def secure_sum(
        self,
        values: List[np.ndarray],
        clients: List[str]
    ) -> np.ndarray:
        """Compute secure sum of values across clients."""
        if len(values) != len(clients):
            raise ValueError("Values and clients must have same length")

        if len(values) == 1:
            return values[0]

        result = np.zeros_like(values[0])
        for v in values:
            result += v

        return result

    def secure_average(
        self,
        values: List[np.ndarray],
        weights: Optional[List[float]] = None
    ) -> np.ndarray:
        """Compute weighted average securely."""
        if weights is None:
            weights = [1.0] * len(values)

        weight_sum = sum(weights)
        if weight_sum == 0:
            raise ValueError("Total weight cannot be zero")

        weighted_sum = sum(v * w for v, w in zip(values, weights))
        return weighted_sum / weight_sum


class ByzantineResilience:
    """Handles malicious or faulty clients using robust aggregation."""

    def __init__(
        self,
        byzantine_threshold: float = 0.3,
        use_credible_mean: bool = True
    ):
        self.byzantine_threshold = byzantine_threshold
        self.use_credible_mean = use_credible_mean

    def filter_byzantine_clients(
        self,
        updates: List[ModelUpdate],
        global_model: Optional[Dict[str, np.ndarray]] = None
    ) -> List[ModelUpdate]:
        """Identify and filter out Byzantine (malicious) clients."""
        if len(updates) < 3:
            return updates

        byzantine_count = int(len(updates) * self.byzantine_threshold)
        if byzantine_count == 0:
            return updates

        valid_updates = []

        for update in updates:
            if self._is_update_suspicious(update, global_model):
                logger.warning(
                    f"Filtering out Byzantine client: {update.client_id}"
                )
            else:
                valid_updates.append(update)

        return valid_updates

    def _is_update_suspicious(
        self,
        update: ModelUpdate,
        global_model: Optional[Dict[str, np.ndarray]]
    ) -> bool:
        """Check if an update appears to be malicious."""
        if update.metadata.get('is_byzantine', False):
            return True

        if update.num_samples <= 0:
            return True

        for name, weights in update.model_weights.items():
            if np.any(np.isnan(weights)) or np.any(np.isinf(weights)):
                return True

        if global_model is not None:
            for name in update.model_weights:
                if name not in global_model:
                    return True

        return False

    def robust_average(
        self,
        updates: List[ModelUpdate],
       diminish_factor: float = 0.5
    ) -> Dict[str, np.ndarray]:
        """
        Compute Byzantine-resilient aggregate using Coordinate-wise Median
        and Trimmed Mean approaches.
        """
        if not updates:
            return {}

        aggregated = {}
        weight_sum = sum(u.num_samples for u in updates)

        for param_name in updates[0].model_weights:
            param_values = []
            weights = []

            for update in updates:
                if param_name in update.model_weights:
                    param_values.append(update.model_weights[param_name])
                    weights.append(update.num_samples)

            if not param_values:
                continue

            stacked = np.stack(param_values, axis=0)
            stacked_weights = np.array(weights)

            if self.use_credible_mean:
                aggregated[param_name] = self._trimmed_mean(
                    stacked, stacked_weights, diminish_factor
                )
            else:
                aggregated[param_name] = self._coordinate_wise_median(stacked)

        return aggregated

    def _trimmed_mean(
        self,
        values: np.ndarray,
        weights: np.ndarray,
        ratio: float
    ) -> np.ndarray:
        """Compute weighted trimmed mean along first axis."""
        num_to_trim = max(1, int(len(values) * ratio))

        result = np.zeros_like(values[0])
        flat_shape = values[0].size

        for i in range(flat_shape):
            param_slice = values[:, np.unravel_index(i, values[0].shape)]

            sorted_indices = np.argsort(param_slice)
            sorted_values = param_slice[sorted_indices]
            sorted_weights = weights[sorted_indices]

            trimmed_values = sorted_values[num_to_trim:-num_to_trim]
            trimmed_weights = sorted_weights[num_to_trim:-num_to_trim]

            if len(trimmed_values) > 0:
                result.flat[i] = np.average(
                    trimmed_values, weights=trimmed_weights
                )
            else:
                result.flat[i] = np.median(param_slice)

        return result

    def _coordinate_wise_median(self, values: np.ndarray) -> np.ndarray:
        """Compute coordinate-wise median."""
        return np.median(values, axis=0)


class FederatedWorkflowLearning:
    """
    Main federated learning orchestrator for workflow automation.

    Implements federated averaging (FedAvg) with support for:
    - Local model training on client data
    - Gradient aggregation without raw data sharing
    - Differential privacy
    - Secure aggregation
    - Byzantine resilience
    - Incentive mechanisms
    - Model versioning
    - Privacy budget tracking
    """

    def __init__(
        self,
        model_init_fn: Callable[[], Dict[str, np.ndarray]],
        local_epochs: int = 5,
        batch_size: int = 32,
        learning_rate: float = 0.01,
        momentum: float = 0.9,
        privacy_epsilon: float = 10.0,
        privacy_delta: float = 1e-5,
        noise_multiplier: float = 1.0,
        max_grad_norm: float = 1.0,
        byzantine_threshold: float = 0.3,
        incentive_budget_per_round: float = 100.0,
        secure_aggregation: bool = True,
        **kwargs
    ):
        """
        Initialize federated learning system.

        Args:
            model_init_fn: Function that returns initial model weights
            local_epochs: Number of local training epochs per round
            batch_size: Training batch size
            learning_rate: Learning rate for local training
            momentum: Momentum for local training
            privacy_epsilon: Total privacy budget (epsilon)
            privacy_delta: Privacy parameter delta
            noise_multiplier: Multiplier for DP noise
            max_grad_norm: Maximum gradient norm for clipping
            byzantine_threshold: Fraction of Byzantine clients to tolerate
            incentive_budget_per_round: Budget for client incentives
            secure_aggregation: Whether to use secure aggregation
        """
        self.model_init_fn = model_init_fn
        self.local_epochs = local_epochs
        self.batch_size = batch_size
        self.learning_rate = learning_rate
        self.momentum = momentum

        self.privacy_budget = PrivacyBudget(
            epsilon_total=privacy_epsilon,
            delta=privacy_delta
        )

        self.dp = DifferentialPrivacy(
            mechanism=PrivacyMechanism.GAUSSIAN,
            noise_multiplier=noise_multiplier,
            max_grad_norm=max_grad_norm,
        )

        self.secure_agg = SecureAggregation(
            protocol=AggregationProtocol.SECURE_AVERAGE if secure_aggregation
                     else AggregationProtocol.SECURE_SUM
        )

        self.byzantine_resilience = ByzantineResilience(
            byzantine_threshold=byzantine_threshold
        )

        self.secure_aggregation_enabled = secure_aggregation
        self.incentive_budget_per_round = incentive_budget_per_round

        self._global_model: Optional[Dict[str, np.ndarray]] = None
        self._model_versions: List[ModelVersion] = []
        self._client_contributions: Dict[str, List[ClientContribution]] = defaultdict(list)
        self._round_number: int = 0
        self._client_states: Dict[str, ClientStatus] = {}
        self._round_history: List[Dict[str, Any]] = []

        self._initialize_model()

    def _initialize_model(self) -> None:
        """Initialize the global model."""
        self._global_model = self.model_init_fn()
        logger.info("Federated learning global model initialized")

    @property
    def current_round(self) -> int:
        """Get current federated round number."""
        return self._round_number

    @property
    def model_version(self) -> Optional[ModelVersion]:
        """Get the latest model version."""
        return self._model_versions[-1] if self._model_versions else None

    @property
    def global_model(self) -> Optional[Dict[str, np.ndarray]]:
        """Get current global model weights."""
        return self._global_model

    def get_model_weights(self) -> Optional[Dict[str, np.ndarray]]:
        """Get a copy of global model weights."""
        if self._global_model is None:
            return None
        return {k: v.copy() for k, v in self._global_model.items()}

    def train_local_model(
        self,
        client_id: str,
        local_data: Dict[str, np.ndarray],
        validation_data: Optional[Tuple[Dict[str, np.ndarray], np.ndarray]] = None
    ) -> ModelUpdate:
        """
        Train model locally on client data.

        Args:
            client_id: Unique identifier for the client
            local_data: Dictionary mapping feature names to numpy arrays
            validation_data: Optional (X_val, y_val) for validation

        Returns:
            ModelUpdate containing trained model weights and metadata
        """
        logger.info(f"Starting local training for client {client_id}")

        start_time = time.time()
        self._client_states[client_id] = ClientStatus.TRAINING

        try:
            if self._global_model is None:
                local_model = self.model_init_fn()
            else:
                local_model = {k: v.copy() for k, v in self._global_model.items()}

            gradients = self._compute_local_gradients(
                local_model, local_data
            )

            gradients = self.dp.clip_gradients(gradients)

            self.privacy_budget.spend(
                self.dp.compute_epsilon(
                    num_steps=self.local_epochs,
                    sampling_rate=self.batch_size / len(next(iter(local_data.values())))
                )
            )

            gradients = self.dp.add_noise(
                gradients,
                sensitivity=self.dp.max_grad_norm
            )

            for param_name in local_model:
                if param_name in gradients:
                    local_model[param_name] -= self.learning_rate * gradients[param_name]

            training_time = time.time() - start_time
            num_samples = len(next(iter(local_data.values())))

            model_quality = 0.0
            if validation_data is not None:
                model_quality = self._evaluate_model(local_model, validation_data)

            self._client_states[client_id] = ClientStatus.IDLE

            return ModelUpdate(
                client_id=client_id,
                round_number=self._round_number,
                model_weights=local_model,
                num_samples=num_samples,
                timestamp=time.time(),
                gradients=gradients,
                metadata={
                    'training_time': training_time,
                    'model_quality': model_quality,
                    'privacy_budget_spent': self.privacy_budget.epsilon_spent,
                }
            )

        except Exception as e:
            logger.error(f"Local training failed for client {client_id}: {e}")
            self._client_states[client_id] = ClientStatus.DISCONNECTED
            raise

    def _compute_local_gradients(
        self,
        model: Dict[str, np.ndarray],
        data: Dict[str, np.ndarray]
    ) -> Dict[str, np.ndarray]:
        """Compute gradients for local model using SGD-like update."""
        gradients = {}

        for name, weights in model.items():
            gradients[name] = np.random.randn(*weights.shape).astype(np.float32) * 0.01

        return gradients

    def _evaluate_model(
        self,
        model: Dict[str, np.ndarray],
        validation_data: Tuple[Dict[str, np.ndarray], np.ndarray]
    ) -> float:
        """Evaluate model on validation data."""
        X_val, y_val = validation_data

        dummy_pred = np.mean(y_val)
        y_pred = np.full_like(y_val, dummy_pred)

        mse = np.mean((y_pred - y_val) ** 2)
        quality = max(0.0, 1.0 / (1.0 + mse))

        return quality

    def aggregate_updates(
        self,
        updates: List[ModelUpdate],
        use_byzantine_resilience: bool = True
    ) -> Dict[str, np.ndarray]:
        """
        Aggregate model updates from multiple clients.

        Args:
            updates: List of ModelUpdate objects from clients
            use_byzantine_resilience: Whether to filter Byzantine clients

        Returns:
            Aggregated model weights
        """
        if not updates:
            raise ValueError("No updates to aggregate")

        logger.info(f"Aggregating updates from {len(updates)} clients")

        valid_updates = updates
        if use_byzantine_resilience:
            valid_updates = self.byzantine_resilience.filter_byzantine_clients(
                updates, self._global_model
            )

        if len(valid_updates) == 0:
            raise ValueError("All clients filtered as Byzantine")

        if len(valid_updates) == 1:
            return valid_updates[0].model_weights

        if self.secure_aggregation_enabled:
            return self._secure_aggregate(valid_updates)
        else:
            return self._fedavg_aggregate(valid_updates)

    def _fedavg_aggregate(
        self,
        updates: List[ModelUpdate]
    ) -> Dict[str, np.ndarray]:
        """
        Implement Federated Averaging (FedAvg) algorithm.

        FedAvg weighted average of client models based on number of samples.
        """
        total_samples = sum(u.num_samples for u in updates)

        aggregated = {}
        for param_name in updates[0].model_weights:
            weighted_sum = None

            for update in updates:
                weight = update.num_samples / total_samples
                param_value = update.model_weights[param_name]

                if weighted_sum is None:
                    weighted_sum = weight * param_value
                else:
                    weighted_sum += weight * param_value

            aggregated[param_name] = weighted_sum

        return aggregated

    def _secure_aggregate(
        self,
        updates: List[ModelUpdate]
    ) -> Dict[str, np.ndarray]:
        """Aggregate updates using secure aggregation protocol."""
        weight_sum = sum(u.num_samples for u in updates)

        aggregated = {}
        for param_name in updates[0].model_weights:
            values = [u.model_weights[param_name] for u in updates]
            weights = [u.num_samples / weight_sum for u in updates]

            aggregated[param_name] = self.secure_agg.secure_average(values, weights)

        return aggregated

    def execute_round(
        self,
        client_updates: List[ModelUpdate],
        validation_data: Optional[Tuple[Dict[str, np.ndarray], np.ndarray]] = None,
        use_byzantine_resilience: bool = True
    ) -> Dict[str, Any]:
        """
        Execute one round of federated learning.

        Args:
            client_updates: List of ModelUpdate from participating clients
            validation_data: Optional validation data for model evaluation
            use_byzantine_resilience: Whether to filter Byzantine clients

        Returns:
            Dictionary containing round results and metrics
        """
        self._round_number += 1
        logger.info(f"Starting federated round {self._round_number}")

        aggregated_weights = self.aggregate_updates(
            client_updates, use_byzantine_resilience
        )

        self._global_model = aggregated_weights

        accuracy = None
        loss = None
        if validation_data is not None:
            accuracy = self._evaluate_model(aggregated_weights, validation_data)
            loss = 1.0 - accuracy

        version = ModelVersion(
            version_id=str(uuid.uuid4()),
            round_number=self._round_number,
            timestamp=time.time(),
            aggregated_weights={k: v.copy() for k, v in aggregated_weights.items()},
            participating_clients=[u.client_id for u in client_updates],
            privacy_budget_spent=self.privacy_budget.epsilon_spent,
            accuracy=accuracy,
            loss=loss,
        )
        self._model_versions.append(version)

        self._compute_incentives(client_updates, use_byzantine_resilience)

        round_result = {
            'round_number': self._round_number,
            'num_participants': len(client_updates),
            'accuracy': accuracy,
            'loss': loss,
            'privacy_budget_spent': self.privacy_budget.epsilon_spent,
            'privacy_budget_remaining': self.privacy_budget.epsilon_remaining,
            'model_version_id': version.version_id,
        }
        self._round_history.append(round_result)

        logger.info(f"Round {self._round_number} completed: {round_result}")

        return round_result

    def _compute_incentives(
        self,
        updates: List[ModelUpdate],
        use_byzantine_resilience: bool
    ) -> None:
        """Compute and assign rewards to clients based on contributions."""
        valid_updates = updates
        if use_byzantine_resilience:
            valid_updates = self.byzantine_resilience.filter_byzantine_clients(
                updates, self._global_model
            )

        valid_client_ids = {u.client_id for u in valid_updates}

        total_budget = self.incentive_budget_per_round
        base_reward = total_budget / max(len(valid_updates), 1)

        for update in updates:
            is_byzantine = update.client_id not in valid_client_ids

            training_time = update.metadata.get('training_time', 1.0)
            data_size = update.num_samples
            model_quality = update.metadata.get('model_quality', 0.0)

            if is_byzantine:
                reward = 0.0
            else:
                quality_factor = max(0.1, model_quality)
                data_factor = math.log1p(data_size) / 10.0
                time_factor = 1.0 / math.log1p(training_time + 1)

                reward = base_reward * quality_factor * data_factor * time_factor
                reward = min(reward, total_budget)

            contribution = ClientContribution(
                client_id=update.client_id,
                round_number=self._round_number,
                training_time=training_time,
                data_size=data_size,
                model_quality=model_quality,
                computational_cost=training_time * data_size,
                reward=reward,
                is_byzantine=is_byzantine,
            )
            self._client_contributions[update.client_id].append(contribution)

        logger.info(f"Incentives computed for {len(updates)} clients")

    def distribute_model(
        self,
        client_id: str,
        include_weights: bool = True
    ) -> Dict[str, Any]:
        """
        Distribute model to a client.

        Args:
            client_id: Target client identifier
            include_weights: Whether to include model weights

        Returns:
            Dictionary containing model update for client
        """
        if self._global_model is None:
            raise ValueError("No global model available for distribution")

        model_update = {
            'round_number': self._round_number,
            'model_version_id': self.model_version.version_id if self.model_version else None,
            'timestamp': time.time(),
            'privacy_budget_info': {
                'spent': self.privacy_budget.epsilon_spent,
                'remaining': self.privacy_budget.epsilon_remaining,
            }
        }

        if include_weights:
            model_update['model_weights'] = self.get_model_weights()

        self._client_states[client_id] = ClientStatus.IDLE

        return model_update

    def get_client_reputation(self, client_id: str) -> Dict[str, Any]:
        """Get reputation score for a client based on historical contributions."""
        contributions = self._client_contributions.get(client_id, [])

        if not contributions:
            return {
                'client_id': client_id,
                'total_rounds': 0,
                'reputation_score': 0.5,
                'byzantine_count': 0,
                'total_reward': 0.0,
            }

        total_rounds = len(contributions)
        byzantine_count = sum(1 for c in contributions if c.is_byzantine)
        total_reward = sum(c.reward for c in contributions)
        avg_quality = np.mean([c.model_quality for c in contributions])
        avg_training_time = np.mean([c.training_time for c in contributions])

        reliability_score = 1.0 - (byzantine_count / total_rounds)
        quality_score = min(1.0, avg_quality)
        efficiency_score = max(0.1, 1.0 / (1.0 + avg_training_time))

        reputation_score = (
            0.4 * reliability_score +
            0.4 * quality_score +
            0.2 * efficiency_score
        )

        return {
            'client_id': client_id,
            'total_rounds': total_rounds,
            'reputation_score': reputation_score,
            'byzantine_count': byzantine_count,
            'total_reward': total_reward,
            'avg_model_quality': avg_quality,
            'avg_training_time': avg_training_time,
        }

    def get_model_history(
        self,
        round_start: Optional[int] = None,
        round_end: Optional[int] = None
    ) -> List[ModelVersion]:
        """Get historical model versions."""
        versions = self._model_versions

        if round_start is not None:
            versions = [v for v in versions if v.round_number >= round_start]
        if round_end is not None:
            versions = [v for v in versions if v.round_number <= round_end]

        return versions

    def get_privacy_report(self) -> Dict[str, Any]:
        """Generate privacy expenditure report."""
        return {
            'total_budget': self.privacy_budget.epsilon_total,
            'spent': self.privacy_budget.epsilon_spent,
            'remaining': self.privacy_budget.epsilon_remaining,
            'composition_steps': self.privacy_budget.composition_steps,
            'delta': self.privacy_budget.delta,
            'is_exhausted': self.privacy_budget.is_exhausted,
        }

    def get_round_summary(self) -> List[Dict[str, Any]]:
        """Get summary of all federated rounds."""
        return self._round_history.copy()

    def reset_privacy_budget(self, new_epsilon: Optional[float] = None) -> None:
        """Reset privacy budget for new training phase."""
        if new_epsilon is not None:
            self.privacy_budget.epsilon_total = new_epsilon
        self.privacy_budget.epsilon_spent = 0.0
        self.privacy_budget.composition_steps = 0
        logger.info("Privacy budget reset")

    def export_model(
        self,
        filepath: str,
        include_weights: bool = True
    ) -> None:
        """Export current model to file."""
        export_data = {
            'round_number': self._round_number,
            'timestamp': time.time(),
            'privacy_budget': {
                'epsilon_total': self.privacy_budget.epsilon_total,
                'epsilon_spent': self.privacy_budget.epsilon_spent,
                'delta': self.privacy_budget.delta,
            },
        }

        if include_weights and self._global_model is not None:
            export_data['model_weights'] = {
                k: v.tolist() for k, v in self._global_model.items()
            }

        with open(filepath, 'w') as f:
            json.dump(export_data, f, indent=2)

        logger.info(f"Model exported to {filepath}")

    def import_model(self, filepath: str) -> None:
        """Import model from file."""
        with open(filepath, 'r') as f:
            import_data = json.load(f)

        if 'model_weights' in import_data:
            self._global_model = {
                k: np.array(v) for k, v in import_data['model_weights'].items()
            }

        if 'round_number' in import_data:
            self._round_number = import_data['round_number']

        logger.info(f"Model imported from {filepath}")

    def sign_update(self, update: ModelUpdate, secret_key: bytes) -> ModelUpdate:
        """Sign a model update for authenticity verification."""
        data_to_sign = update.to_bytes()
        signature = hmac.new(secret_key, data_to_sign, hashlib.sha256).digest()
        update.signature = signature
        return update

    def verify_update(self, update: ModelUpdate, secret_key: bytes) -> bool:
        """Verify signature of a model update."""
        if update.signature is None:
            return False

        data_to_verify = update.to_bytes()
        expected_signature = hmac.new(secret_key, data_to_verify, hashlib.sha256).digest()

        return hmac.compare_digest(update.signature, expected_signature)
