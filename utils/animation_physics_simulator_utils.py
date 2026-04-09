"""
Animation physics simulator utilities.

This module provides physics-based animation utilities including
spring dynamics, velocity, acceleration, and physical simulation.
"""

from __future__ import annotations

import math
from typing import Tuple, Optional, Dict, Any
from dataclasses import dataclass, field


# Type aliases
Vector2D = Tuple[float, float]


@dataclass
class SpringConfig:
    """Configuration for spring physics."""
    stiffness: float = 200.0  # Spring stiffness (N/m)
    damping: float = 10.0  # Damping coefficient (N*s/m)
    mass: float = 1.0  # Mass (kg)


@dataclass
class PhysicsState:
    """State of a physics simulation."""
    position: float
    velocity: float
    acceleration: float = 0.0
    time: float = 0.0


@dataclass
class PhysicsSimulationResult:
    """Result of physics simulation."""
    final_position: float
    final_velocity: float
    final_time: float
    settled: bool
    iterations: int


@dataclass
class VelocityProfile2D:
    """2D velocity profile."""
    vx: float
    vy: float

    @property
    def speed(self) -> float:
        return math.sqrt(self.vx ** 2 + self.vy ** 2)

    @property
    def angle(self) -> float:
        return math.atan2(self.vy, self.vx)

    def scale(self, factor: float) -> "VelocityProfile2D":
        return VelocityProfile2D(vx=self.vx * factor, vy=self.vy * factor)

    def magnitude(self) -> float:
        return self.speed


def simulate_spring_damping(
    initial_position: float,
    target_position: float,
    initial_velocity: float,
    config: SpringConfig,
    dt: float = 0.016,
    max_time: float = 5.0,
    tolerance: float = 0.001,
) -> PhysicsSimulationResult:
    """
    Simulate spring-damper system to target position.

    Args:
        initial_position: Starting position.
        target_position: Target equilibrium position.
        initial_velocity: Starting velocity.
        config: Spring physics configuration.
        dt: Time step in seconds.
        max_time: Maximum simulation time.
        tolerance: Position tolerance for settling.

    Returns:
        PhysicsSimulationResult with final state.
    """
    state = PhysicsState(
        position=initial_position,
        velocity=initial_velocity,
        time=0.0,
    )

    iterations = 0
    settled = False

    while state.time < max_time:
        # Spring force: F = -k * (x - target)
        displacement = state.position - target_position
        spring_force = -config.stiffness * displacement

        # Damping force: F = -c * v
        damping_force = -config.damping * state.velocity

        # Total force and acceleration
        total_force = spring_force + damping_force
        acceleration = total_force / config.mass

        # Update state
        state.velocity += acceleration * dt
        state.position += state.velocity * dt
        state.time += dt
        state.acceleration = acceleration

        iterations += 1

        # Check if settled
        if abs(state.velocity) < tolerance and abs(displacement) < tolerance:
            settled = True
            break

    return PhysicsSimulationResult(
        final_position=state.position,
        final_velocity=state.velocity,
        final_time=state.time,
        settled=settled,
        iterations=iterations,
    )


def compute_spring_parameters(
    target_duration: float,
    damping_ratio: float = 0.7,
    displacement: float = 1.0,
) -> SpringConfig:
    """
    Compute spring parameters for a desired animation duration.

    Args:
        target_duration: Desired settling time in seconds.
        damping_ratio: Damping ratio (0 = undamped, 1 = critically damped).
        displacement: Expected displacement.

    Returns:
        SpringConfig with computed parameters.
    """
    # For critically damped: c = 2 * sqrt(k * m)
    # Using damping_ratio: c = damping_ratio * 2 * sqrt(k * m)
    # Settling time approximation: t ~ 4.6 / (damping_ratio * omega_n)
    # where omega_n = sqrt(k/m)

    omega_n = 4.6 / (target_duration * damping_ratio) if damping_ratio > 0 else 4.6 / target_duration
    stiffness = omega_n ** 2  # Assuming mass = 1
    damping = 2 * damping_ratio * omega_n

    return SpringConfig(stiffness=stiffness, damping=damping, mass=1.0)


def apply_friction(
    velocity: float,
    friction_coefficient: float,
    dt: float,
) -> float:
    """
    Apply friction to velocity.

    Args:
        velocity: Current velocity.
        friction_coefficient: Friction coefficient.
        dt: Time step.

    Returns:
        Updated velocity after friction.
    """
    friction_force = friction_coefficient * velocity
    return velocity - friction_force * dt


def apply_gravity(
    velocity: float,
    gravity: float = 980.0,
    dt: float = 0.016,
) -> Tuple[float, float]:
    """
    Apply gravity to velocity and position.

    Args:
        velocity: Current velocity.
        gravity: Gravity acceleration (px/s^2).
        dt: Time step.

    Returns:
        Tuple of (updated_velocity, position_change).
    """
    new_velocity = velocity + gravity * dt
    position_change = velocity * dt + 0.5 * gravity * dt * dt
    return new_velocity, position_change


def compute_momentum(mass: float, velocity: float) -> float:
    """Compute momentum (p = mv)."""
    return mass * velocity


def compute_kinetic_energy(mass: float, velocity: float) -> float:
    """Compute kinetic energy (KE = 0.5 * m * v^2)."""
    return 0.5 * mass * velocity ** 2


def compute_elastic_potential_energy(
    stiffness: float,
    displacement: float,
) -> float:
    """Compute elastic potential energy (PE = 0.5 * k * x^2)."""
    return 0.5 * stiffness * displacement ** 2


def simulate_bounce(
    position: float,
    velocity: float,
    restitution: float,
    gravity: float = 980.0,
    ground_y: float = 0.0,
    dt: float = 0.016,
    max_bounces: int = 10,
) -> Tuple[float, float, int]:
    """
    Simulate a bouncing object.

    Args:
        position: Initial position.
        velocity: Initial velocity.
        restitution: Bounce coefficient (0-1).
        gravity: Gravity acceleration.
        ground_y: Ground position.
        dt: Time step.
        max_bounces: Maximum number of bounces.

    Returns:
        Tuple of (final_position, final_velocity, bounce_count).
    """
    pos = position
    vel = velocity
    bounce_count = 0
    iterations = 0
    max_iterations = 1000

    while pos > ground_y and iterations < max_iterations and bounce_count < max_bounces:
        # Apply gravity
        vel += gravity * dt
        pos += vel * dt
        iterations += 1

        # Check for bounce
        if pos <= ground_y:
            pos = ground_y
            vel = -vel * restitution
            bounce_count += 1

    return pos, vel, bounce_count
