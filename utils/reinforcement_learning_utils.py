"""
Reinforcement learning utilities.

Provides policy gradient, Q-learning, and RL helper functions.
"""
from __future__ import annotations

from typing import Callable, List, Optional, Tuple

import numpy as np


class ReplayBuffer:
    """Experience replay buffer for off-policy RL."""

    def __init__(self, capacity: int):
        self.capacity = capacity
        self.buffer = []
        self.position = 0

    def push(
        self, state: np.ndarray, action: int, reward: float, next_state: np.ndarray, done: bool
    ) -> None:
        """Add experience to buffer."""
        if len(self.buffer) < self.capacity:
            self.buffer.append(None)
        self.buffer[self.position] = (state, action, reward, next_state, done)
        self.position = (self.position + 1) % self.capacity

    def sample(self, batch_size: int) -> List:
        """Sample random batch."""
        return random.sample(self.buffer, batch_size)

    def __len__(self) -> int:
        return len(self.buffer)


class PrioritizedReplayBuffer(ReplayBuffer):
    """Prioritized experience replay buffer."""

    def __init__(self, capacity: int, alpha: float = 0.6):
        super().__init__(capacity)
        self.alpha = alpha
        self.priorities = np.zeros(capacity)
        self.position = 0

    def push(self, state, action, reward, next_state, done, td_error: float = 1.0):
        """Add experience with priority."""
        priority = td_error ** self.alpha
        if len(self.buffer) < self.capacity:
            self.buffer.append(None)
            self.priorities[len(self.buffer) - 1] = priority
        else:
            self.priorities[self.position] = priority
        self.buffer[self.position] = (state, action, reward, next_state, done)
        self.position = (self.position + 1) % self.capacity

    def sample(self, batch_size: int, beta: float = 0.4) -> Tuple[List, np.ndarray]:
        """Sample batch with prioritization."""
        probs = self.priorities[: len(self.buffer)]
        probs = probs / probs.sum()
        indices = np.random.choice(len(self.buffer), batch_size, p=probs)
        weights = (len(self.buffer) * probs[indices]) ** (-beta)
        weights = weights / weights.max()
        batch = [self.buffer[i] for i in indices]
        return batch, weights, indices


class OrnsteinUhlenbeckProcess:
    """OU noise for continuous action exploration."""

    def __init__(self, size: int, theta: float = 0.15, sigma: float = 0.3):
        self.size = size
        self.theta = theta
        self.sigma = sigma
        self.state = np.zeros(size)

    def reset(self) -> np.ndarray:
        """Reset noise state."""
        self.state = np.zeros(self.size)
        return self.state

    def sample(self) -> np.ndarray:
        """Sample next noise value."""
        self.state += -self.theta * self.state + self.sigma * np.random.randn(self.size)
        return self.state


def compute_advantages(
    rewards: np.ndarray, values: np.ndarray, gamma: float = 0.99, gae_lambda: float = 0.95
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Compute GAE (Generalized Advantage Estimation).

    Args:
        rewards: Reward sequence (T,)
        values: Value estimates (T+1,)
        gamma: Discount factor
        gae_lambda: GAE lambda parameter

    Returns:
        Tuple of (advantages, returns)

    Example:
        >>> rewards = np.array([0, 0, 1, 0, 0])
        >>> values = np.array([0, 0.5, 0.8, 0.3, 0])
        >>> adv, ret = compute_advantages(rewards, values)
    """
    T = len(rewards)
    advantages = np.zeros(T)
    last_gae = 0
    for t in reversed(range(T)):
        delta = rewards[t] + gamma * values[t + 1] - values[t]
        advantages[t] = last_gae = delta + gamma * gae_lambda * last_gae
    returns = advantages + values[:T]
    return advantages, returns


def policy_gradient_loss(
    log_probs: np.ndarray, advantages: np.ndarray
) -> float:
    """
    Compute policy gradient loss.

    Args:
        log_probs: Log probabilities of actions
        advantages: Advantage estimates

    Returns:
        Policy loss
    """
    return -np.mean(log_probs * advantages)


def q_learning_update(
    q_table: np.ndarray, state: int, action: int, reward: float, next_state: int, alpha: float, gamma: float, done: bool
) -> float:
    """
    Q-learning update rule.

    Args:
        q_table: Q-value table
        state: Current state
        action: Action taken
        reward: Reward received
        next_state: Next state
        alpha: Learning rate
        gamma: Discount factor
        done: Whether episode ended

    Returns:
        TD error
    """
    current_q = q_table[state, action]
    if done:
        target_q = reward
    else:
        target_q = reward + gamma * np.max(q_table[next_state])
    td_error = target_q - current_q
    q_table[state, action] += alpha * td_error
    return td_error


def sarsa_update(
    q_table: np.ndarray, state: int, action: int, reward: float, next_state: int, next_action: int, alpha: float, gamma: float, done: bool
) -> float:
    """
    SARSA update rule.

    Args:
        q_table: Q-value table
        state: Current state
        action: Action taken
        reward: Reward received
        next_state: Next state
        next_action: Next action
        alpha: Learning rate
        gamma: Discount factor
        done: Whether episode ended

    Returns:
        TD error
    """
    current_q = q_table[state, action]
    if done:
        target_q = reward
    else:
        target_q = reward + gamma * q_table[next_state, next_action]
    td_error = target_q - current_q
    q_table[state, action] += alpha * td_error
    return td_error


def clipped_surrogate_loss(
    old_log_probs: np.ndarray, new_log_probs: np.ndarray, advantages: np.ndarray, epsilon: float = 0.2
) -> float:
    """
    PPO clipped surrogate loss.

    Args:
        old_log_probs: Old policy log probabilities
        new_log_probs: New policy log probabilities
        advantages: Advantage estimates
        epsilon: Clipping parameter

    Returns:
        Clipped surrogate loss
    """
    ratio = np.exp(new_log_probs - old_log_probs)
    surr1 = ratio * advantages
    surr2 = np.clip(ratio, 1 - epsilon, 1 + epsilon) * advantages
    return -np.mean(np.minimum(surr1, surr2))


class SoftActorCritic:
    """Soft Actor-Critic algorithm."""

    def __init__(
        self,
        state_dim: int,
        action_dim: int,
        hidden_dim: int = 256,
        alpha: float = 0.2,
        gamma: float = 0.99,
        tau: float = 0.005,
    ):
        self.alpha = alpha
        self.gamma = gamma
        self.tau = tau
        self.action_dim = action_dim
        self.q1_network = self._build_network(state_dim + action_dim, hidden_dim, 1)
        self.q2_network = self._build_network(state_dim + action_dim, hidden_dim, 1)
        self.target_q1_network = self._copy_network(self.q1_network)
        self.target_q2_network = self._copy_network(self.q2_network)
        self.policy_network = self._build_network(state_dim, hidden_dim, action_dim)

    def _build_network(self, input_dim: int, hidden_dim: int, output_dim: int) -> dict:
        """Build simple feedforward network."""
        return {
            "w1": np.random.randn(input_dim, hidden_dim) * np.sqrt(2.0 / input_dim),
            "b1": np.zeros(hidden_dim),
            "w2": np.random.randn(hidden_dim, hidden_dim) * np.sqrt(2.0 / hidden_dim),
            "b2": np.zeros(hidden_dim),
            "w_out": np.random.randn(hidden_dim, output_dim) * 0.01,
            "b_out": np.zeros(output_dim),
        }

    def _copy_network(self, network: dict) -> dict:
        """Create a copy of network weights."""
        return {k: v.copy() for k, v in network.items()}

    def _forward(self, network: dict, x: np.ndarray) -> np.ndarray:
        """Forward pass through network."""
        h = np.tanh(x @ network["w1"] + network["b1"])
        h = np.tanh(h @ network["w2"] + network["b2"])
        return h @ network["w_out"] + network["b_out"]

    def _soft_update(self, target: dict, source: dict) -> None:
        """Soft update of target network."""
        for key in target:
            target[key] = (1 - self.tau) * target[key] + self.tau * source[key]


class DQN:
    """Deep Q-Network."""

    def __init__(
        self,
        state_dim: int,
        action_dim: int,
        hidden_dim: int = 128,
        lr: float = 0.001,
        gamma: float = 0.99,
        epsilon: float = 1.0,
        epsilon_decay: float = 0.995,
        epsilon_min: float = 0.01,
    ):
        self.action_dim = action_dim
        self.gamma = gamma
        self.epsilon = epsilon
        self.epsilon_decay = epsilon_decay
        self.epsilon_min = epsilon_min
        self.network = self._build_network(state_dim, hidden_dim, action_dim)
        self.target_network = self._copy_network(self.network)
        self.lr = lr

    def _build_network(self, input_dim: int, hidden_dim: int, output_dim: int) -> dict:
        return {
            "w1": np.random.randn(input_dim, hidden_dim) * np.sqrt(2.0 / input_dim),
            "b1": np.zeros(hidden_dim),
            "w2": np.random.randn(hidden_dim, hidden_dim) * np.sqrt(2.0 / hidden_dim),
            "b2": np.zeros(hidden_dim),
            "w_out": np.random.randn(hidden_dim, output_dim) * 0.01,
            "b_out": np.zeros(output_dim),
        }

    def _copy_network(self, network: dict) -> dict:
        return {k: v.copy() for k, v in network.items()}

    def _forward(self, network: dict, x: np.ndarray) -> np.ndarray:
        h = np.maximum(0, x @ network["w1"] + network["b1"])
        h = np.maximum(0, h @ network["w2"] + network["b2"])
        return h @ network["w_out"] + network["b_out"]

    def select_action(self, state: np.ndarray, epsilon: float = None) -> int:
        """Epsilon-greedy action selection."""
        if epsilon is None:
            epsilon = self.epsilon
        if np.random.rand() < epsilon:
            return np.random.randint(self.action_dim)
        q_values = self._forward(self.network, state)
        return int(np.argmax(q_values))

    def update(self, states: np.ndarray, actions: np.ndarray, rewards: np.ndarray, next_states: np.ndarray, dones: np.ndarray) -> float:
        """Update DQN."""
        current_q = self._forward(self.network, states)
        with np.errstate(divide='ignore', invalid='ignore'):
            next_q = self._forward(self.target_network, next_states)
            max_next_q = np.max(next_q, axis=1)
            target_q = rewards + (1 - dones) * self.gamma * max_next_q
        target_q = np.where(np.isfinite(target_q), target_q, 0)
        loss = np.mean((current_q[np.arange(len(actions)), actions] - target_q) ** 2)
        self.epsilon = max(self.epsilon_min, self.epsilon * self.epsilon_decay)
        return float(loss)


def epsilon_greedy(q_values: np.ndarray, epsilon: float) -> int:
    """
    Epsilon-greedy action selection.

    Args:
        q_values: Q-values for each action
        epsilon: Exploration probability

    Returns:
        Selected action index
    """
    if np.random.rand() < epsilon:
        return np.random.randint(len(q_values))
    return int(np.argmax(q_values))


def boltzmann_exploration(q_values: np.ndarray, temperature: float = 1.0) -> int:
    """
    Boltzmann exploration.

    Args:
        q_values: Q-values for each action
        temperature: Temperature parameter

    Returns:
        Selected action index
    """
    exp_q = np.exp(q_values / temperature)
    probs = exp_q / exp_q.sum()
    return np.random.choice(len(q_values), p=probs)


class MultiArmedBandit:
    """Multi-armed bandit."""

    def __init__(self, n_arms: int, reward_probs: np.ndarray = None):
        self.n_arms = n_arms
        if reward_probs is None:
            self.reward_probs = np.random.rand(n_arms)
        else:
            self.reward_probs = reward_probs

    def pull(self, arm: int) -> float:
        """Pull arm and get reward."""
        return float(np.random.rand() < self.reward_probs[arm])

    def epsilon_greedy(self, counts: np.ndarray, values: np.ndarray, epsilon: float) -> int:
        """Select arm using epsilon-greedy."""
        if np.random.rand() < epsilon:
            return np.random.randint(self.n_arms)
        return int(np.argmax(values))

    def ucb1(self, counts: np.ndarray, values: np.ndarray, t: int) -> int:
        """UCB1 action selection."""
        ucb_values = values + np.sqrt(2 * np.log(t + 1) / (counts + 1e-10))
        return int(np.argmax(ucb_values))

    def thompson_sampling(self, alpha: np.ndarray, beta: np.ndarray) -> int:
        """Thompson sampling with Beta posterior."""
        samples = np.random.beta(alpha, beta)
        return int(np.argmax(samples))
