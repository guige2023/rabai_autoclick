"""
Generative model utilities.

Provides utilities for VAE, GAN, and diffusion models
including loss functions, sampling, and generation helpers.
"""
from __future__ import annotations

from typing import Callable, Optional, Tuple

import numpy as np


def reparameterize(mu: np.ndarray, log_var: np.ndarray) -> np.ndarray:
    """
    Reparameterization trick for VAE.

    Args:
        mu: Mean of the latent distribution
        log_var: Log variance of the latent distribution

    Returns:
        Sample from the latent distribution

    Example:
        >>> mu = np.zeros(10)
        >>> log_var = np.zeros(10)
        >>> z = reparameterize(mu, log_var)
        >>> z.shape
        (10,)
    """
    std = np.exp(0.5 * log_var)
    eps = np.random.randn(*std.shape)
    return mu + std * eps


def kl_divergence_gaussian(mu: np.ndarray, log_var: np.ndarray) -> float:
    """
    KL divergence between Gaussian and standard normal.

    Args:
        mu: Mean of the Gaussian
        log_var: Log variance of the Gaussian

    Returns:
        KL divergence
    """
    return -0.5 * np.sum(1 + log_var - mu ** 2 - np.exp(log_var))


def vae_loss(
    x_recon: np.ndarray,
    x: np.ndarray,
    mu: np.ndarray,
    log_var: np.ndarray,
    beta: float = 1.0,
) -> Tuple[float, float, float]:
    """
    VAE loss (reconstruction + KL divergence).

    Args:
        x_recon: Reconstructed input
        x: Original input
        mu: Latent mean
        log_var: Latent log variance
        beta: Weight for KL term (beta-VAE)

    Returns:
        Tuple of (total_loss, recon_loss, kl_loss)
    """
    recon_loss = np.mean((x_recon - x) ** 2)
    kl_loss = kl_divergence_gaussian(mu, log_var)
    total_loss = recon_loss + beta * kl_loss
    return total_loss, recon_loss, kl_loss


def wasserstein_loss(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """
    Wasserstein loss for WGAN.

    Args:
        y_true: Ground truth (1 for real, -1 for fake)
        y_pred: Discriminator output

    Returns:
        Wasserstein loss
    """
    return np.mean(y_true * y_pred)


def gradient_penalty(
    discriminator: Callable,
    real_data: np.ndarray,
    fake_data: np.ndarray,
    batch_size: int,
) -> float:
    """
    Gradient penalty for WGAN-GP.

    Args:
        discriminator: Discriminator function
        real_data: Real data samples
        fake_data: Fake data samples
        batch_size: Batch size

    Returns:
        Gradient penalty
    """
    alpha = np.random.rand(batch_size, *real_data.shape[1:])
    interpolated = alpha * real_data + (1 - alpha) * fake_data
    interpolated.requires_grad = True
    grad = discriminator(interpolated)
    grad = grad.reshape(batch_size, -1)
    grad_norm = np.linalg.norm(grad, axis=1)
    penalty = np.mean((grad_norm - 1) ** 2)
    return penalty


class NoiseSampler:
    """Sample noise for generative models."""

    def __init__(self, distribution: str = "normal", seed: int = None):
        self.distribution = distribution
        if seed is not None:
            np.random.seed(seed)

    def sample(self, shape: Tuple[int, ...]) -> np.ndarray:
        """
        Sample noise.

        Args:
            shape: Shape of noise to sample

        Returns:
            Noise array
        """
        if self.distribution == "normal":
            return np.random.randn(*shape)
        elif self.distribution == "uniform":
            return np.random.uniform(-1, 1, shape)
        elif self.distribution == "truncated":
            return np.random.randn(*shape) * 0.7
        return np.random.randn(*shape)


class GANMonitor:
    """Monitor GAN training progress."""

    def __init__(self, latent_dim: int, n_samples: int = 5):
        self.latent_dim = latent_dim
        self.n_samples = n_samples
        self.fixed_noise = np.random.randn(n_samples, latent_dim)
        self.history = {"g_loss": [], "d_loss": [], "grad_pen": []}

    def update(self, g_loss: float, d_loss: float, grad_pen: float = None) -> None:
        """Update history with new losses."""
        self.history["g_loss"].append(g_loss)
        self.history["d_loss"].append(d_loss)
        if grad_pen is not None:
            self.history["grad_pen"].append(grad_pen)

    def get_fixed_noise(self) -> np.ndarray:
        """Get fixed noise for consistent monitoring."""
        return self.fixed_noise

    def should_save(self, epoch: int, save_interval: int = 10) -> bool:
        """Check if model should be saved at this epoch."""
        return epoch % save_interval == 0


class DiffusionSchedule:
    """Noise schedule for diffusion models."""

    def __init__(
        self,
        n_steps: int = 1000,
        beta_start: float = 0.0001,
        beta_end: float = 0.02,
        schedule_type: str = "linear",
    ):
        self.n_steps = n_steps
        self.beta_start = beta_start
        self.beta_end = beta_end
        self.schedule_type = schedule_type
        self.betas = self._create_schedule()

    def _create_schedule(self) -> np.ndarray:
        """Create beta schedule."""
        if self.schedule_type == "linear":
            return np.linspace(self.beta_start, self.beta_end, self.n_steps)
        elif self.schedule_type == "cosine":
            t = np.arange(self.n_steps + 1)
            s = 0.008
            alphas_cumprod = np.cos(((t / self.n_steps) + s) / (1 + s) * np.pi * 0.5) ** 2
            alphas_cumprod = alphas_cumprod / alphas_cumprod[0]
            betas = 1 - (alphas_cumprod[1:] / alphas_cumprod[:-1])
            return np.clip(betas, 0.0001, 0.02)
        return np.linspace(self.beta_start, self.beta_end, self.n_steps)

    def get_alpha_bar(self, t: int) -> float:
        """Get cumulative product of (1 - beta) up to t."""
        return np.prod(1 - self.betas[:t])


def add_noise(x: np.ndarray, t: int, schedule: DiffusionSchedule) -> Tuple[np.ndarray, np.ndarray]:
    """
    Add noise to data at timestep t.

    Args:
        x: Clean data
        t: Timestep
        schedule: Diffusion schedule

    Returns:
        Tuple of (noisy_x, noise)
    """
    noise = np.random.randn(*x.shape)
    alpha_bar = schedule.get_alpha_bar(t)
    noisy = np.sqrt(alpha_bar) * x + np.sqrt(1 - alpha_bar) * noise
    return noisy, noise


def denoise_step(
    x_t: np.ndarray, t: int, model: Callable, schedule: DiffusionSchedule
) -> np.ndarray:
    """
    Single denoising step (DDPM sampling).

    Args:
        x_t: Noisy data at timestep t
        t: Current timestep
        model: Denoising model
        schedule: Diffusion schedule

    Returns:
        Denoised data at timestep t-1
    """
    beta_t = schedule.betas[t]
    alpha_t = 1 - beta_t
    alpha_bar_t = schedule.get_alpha_bar(t)
    alpha_bar_t_1 = schedule.get_alpha_bar(t - 1) if t > 0 else 1.0
    predicted_noise = model(x_t, t)
    mean = (1 / np.sqrt(alpha_t)) * (x_t - (beta_t / np.sqrt(1 - alpha_bar_t)) * predicted_noise)
    if t > 0:
        variance = beta_t * (1 - alpha_bar_t_1) / (1 - alpha_bar_t)
        noise = np.random.randn(*x_t.shape)
        return mean + np.sqrt(variance) * noise
    return mean


def generate_with_diffusion(
    model: Callable, schedule: DiffusionSchedule, latent_dim: int, n_samples: int
) -> np.ndarray:
    """
    Generate samples using diffusion model.

    Args:
        model: Denoising model
        schedule: Diffusion schedule
        latent_dim: Dimension of latent space
        n_samples: Number of samples to generate

    Returns:
        Generated samples
    """
    x = np.random.randn(n_samples, latent_dim)
    for t in range(schedule.n_steps - 1, -1, -1):
        x = denoise_step(x, t, model, schedule)
    return x


class VAE:
    """Variational Autoencoder template."""

    def __init__(
        self,
        input_dim: int,
        latent_dim: int,
        encoder_layers: list,
        decoder_layers: list,
    ):
        self.input_dim = input_dim
        self.latent_dim = latent_dim
        self.encoder_layers = encoder_layers
        self.decoder_layers = decoder_layers
        self._init_weights()

    def _init_weights(self) -> None:
        """Initialize network weights."""
        pass

    def encode(self, x: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        """Encode input to latent distribution."""
        raise NotImplementedError

    def decode(self, z: np.ndarray) -> np.ndarray:
        """Decode latent to reconstruction."""
        raise NotImplementedError

    def forward(self, x: np.ndarray) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Forward pass returning reconstruction, mu, log_var."""
        mu, log_var = self.encode(x)
        z = reparameterize(mu, log_var)
        x_recon = self.decode(z)
        return x_recon, mu, log_var

    def loss(self, x: np.ndarray) -> Tuple[float, float, float]:
        """Compute VAE loss."""
        x_recon, mu, log_var = self.forward(x)
        return vae_loss(x_recon, x, mu, log_var)


class GlowBlock:
    """Glow flow-based model block."""

    def __init__(
        self,
        channels: int,
        hidden_channels: int,
        num_layers: int = 3,
    ):
        self.channels = channels
        self.hidden_channels = hidden_channels
        self.num_layers = num_layers
        self.actnorm_scale = np.ones(channels)

    def forward(self, x: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        """
        Forward pass through Glow block.

        Returns:
            Tuple of (output, log_det)
        """
        log_det = np.sum(np.log(np.abs(self.actnorm_scale)) + 0.5)
        return x * self.actnorm_scale, log_det

    def inverse(self, z: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        """Inverse pass."""
        log_det = -np.sum(np.log(np.abs(self.actnorm_scale)) + 0.5)
        return z / self.actnorm_scale, log_det


def nll_loss_gaussian(y_true: np.ndarray, y_pred: np.ndarray, std: float = 1.0) -> float:
    """
    Negative log-likelihood for Gaussian distribution.

    Args:
        y_true: Ground truth
        y_pred: Predicted mean
        std: Standard deviation

    Returns:
        Negative log-likelihood
    """
    return np.mean(0.5 * ((y_true - y_pred) / std) ** 2 + np.log(std + 1e-10))


def energy_based_loss(
    energy: np.ndarray, pos_weight: float = 1.0, neg_weight: float = 1.0
) -> float:
    """
    Energy-based model loss.

    Args:
        energy: Model energy values
        pos_weight: Weight for positive samples
        neg_weight: Weight for negative samples

    Returns:
        Energy loss
    """
    positive_loss = pos_weight * np.mean(energy[energy > 0] ** 2)
    negative_loss = neg_weight * np.mean(np.maximum(0, 1 - energy[energy < 0]) ** 2)
    return positive_loss + negative_loss
