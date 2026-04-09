"""Spring physics utilities for RabAI AutoClick.

Provides:
- Spring/damper system simulation
- Natural and damped oscillation
- Spring-based animations and interpolation
"""

from typing import Tuple, Optional, Callable
import math


class Spring:
    """A damped spring system."""

    def __init__(
        self,
        stiffness: float = 200.0,
        damping: float = 10.0,
        mass: float = 1.0,
    ):
        """Initialize spring.

        Args:
            stiffness: Spring constant (N/m).
            damping: Damping coefficient (Ns/m).
            mass: Mass of object (kg).
        """
        self.stiffness = stiffness
        self.damping = damping
        self.mass = mass
        self.position = 0.0
        self.velocity = 0.0
        self.target = 0.0

    def set_target(self, target: float) -> None:
        """Set spring target position."""
        self.target = target

    def update(self, dt: float) -> float:
        """Update spring by dt seconds.

        Args:
            dt: Time step in seconds.

        Returns:
            Current position.
        """
        displacement = self.position - self.target
        force = -self.stiffness * displacement - self.damping * self.velocity
        acceleration = force / self.mass
        self.velocity += acceleration * dt
        self.position += self.velocity * dt
        return self.position

    def is_at_rest(self, tolerance: float = 1e-3) -> bool:
        """Check if spring has settled."""
        return (
            abs(self.position - self.target) < tolerance
            and abs(self.velocity) < tolerance
        )

    def reset(self, position: float = 0.0, velocity: float = 0.0) -> None:
        """Reset spring state."""
        self.position = position
        self.velocity = velocity
        self.target = position


class Spring2D:
    """2D spring system for x/y coordinates."""

    def __init__(
        self,
        stiffness: float = 200.0,
        damping: float = 10.0,
        mass: float = 1.0,
    ):
        """Initialize 2D spring."""
        self.spring_x = Spring(stiffness, damping, mass)
        self.spring_y = Spring(stiffness, damping, mass)

    def set_target(self, x: float, y: float) -> None:
        """Set target position."""
        self.spring_x.set_target(x)
        self.spring_y.set_target(y)

    def update(self, dt: float) -> Tuple[float, float]:
        """Update spring."""
        return (self.spring_x.update(dt), self.spring_y.update(dt))

    def get_position(self) -> Tuple[float, float]:
        """Get current position."""
        return (self.spring_x.position, self.spring_y.position)

    def get_velocity(self) -> Tuple[float, float]:
        """Get current velocity."""
        return (self.spring_x.velocity, self.spring_y.velocity)

    def is_at_rest(self, tolerance: float = 1e-3) -> bool:
        """Check if at rest."""
        return self.spring_x.is_at_rest(tolerance) and self.spring_y.is_at_rest(tolerance)

    def reset(self, x: float = 0.0, y: float = 0.0) -> None:
        """Reset state."""
        self.spring_x.reset(x)
        self.spring_y.reset(y)


def spring_interpolate(
    current: float,
    target: float,
    velocity: float,
    stiffness: float = 200.0,
    damping: float = 10.0,
    mass: float = 1.0,
    dt: float = 1.0 / 60.0,
) -> Tuple[float, float]:
    """Single-step spring interpolation.

    Args:
        current: Current position.
        target: Target position.
        velocity: Current velocity.
        stiffness: Spring stiffness.
        damping: Damping coefficient.
        mass: Mass.
        dt: Time step.

    Returns:
        (new_position, new_velocity).
    """
    disp = current - target
    force = -stiffness * disp - damping * velocity
    acc = force / mass
    new_vel = velocity + acc * dt
    new_pos = current + new_vel * dt
    return new_pos, new_vel


def spring_value(
    start: float,
    end: float,
    t: float,
    stiffness: float = 100.0,
    damping: float = 10.0,
) -> float:
    """Compute spring value at time t.

    Uses analytical solution for damped harmonic oscillator.

    Args:
        start: Start value.
        end: End value.
        t: Time since start.
        stiffness: Spring constant.
        damping: Damping ratio.

    Returns:
        Value at time t.
    """
    omega0 = math.sqrt(stiffness)
    zeta = damping / (2 * omega0)

    if zeta < 1.0:  # Underdamped
        omega1 = omega0 * math.sqrt(1 - zeta * zeta)
        envelope = math.exp(-zeta * omega0 * t)
        val = 1 - envelope * (
            math.cos(omega1 * t) +
            (zeta * omega0 / omega1) * math.sin(omega1 * t)
        )
    elif zeta == 1.0:  # Critically damped
        val = 1 - (1 + omega0 * t) * math.exp(-omega0 * t)
    else:  # Overdamped
        r1 = -omega0 * (zeta + math.sqrt(zeta * zeta - 1))
        r2 = -omega0 * (zeta - math.sqrt(zeta * zeta - 1))
        val = 1 - (r2 * math.exp(r1 * t) - r1 * math.exp(r2 * t)) / (r2 - r1)

    return start + (end - start) * val


def simulate_spring(
    start_pos: float,
    end_pos: float,
    stiffness: float = 100.0,
    damping: float = 8.0,
    dt: float = 1.0 / 60.0,
    max_time: float = 5.0,
    tolerance: float = 1e-3,
) -> Tuple[list, list]:
    """Simulate spring motion fully to rest.

    Args:
        start_pos: Initial position.
        end_pos: Target position.
        stiffness: Spring stiffness.
        damping: Damping coefficient.
        dt: Time step.
        max_time: Maximum simulation time.
        tolerance: Rest tolerance.

    Returns:
        (times, positions) lists.
    """
    times = [0.0]
    positions = [start_pos]
    pos = start_pos
    vel = 0.0
    t = 0.0

    while t < max_time:
        pos, vel = spring_interpolate(
            pos, end_pos, vel, stiffness, damping, 1.0, dt
        )
        t += dt
        times.append(t)
        positions.append(pos)
        if abs(pos - end_pos) < tolerance and abs(vel) < tolerance:
            break

    return times, positions


def critically_damped_position(
    start: float,
    end: float,
    t: float,
    duration: float,
) -> float:
    """Critically damped interpolation (smoothest approach).

    Args:
        start: Start value.
        end: End value.
        t: Current time.
        duration: Total duration.

    Returns:
        Position at time t.
    """
    if duration <= 0:
        return end
    tau = max(0.0, min(1.0, t / duration))
    # Critically damped: (1 + omega*t) * exp(-omega*t)
    omega = 5.0  # Tune for feel
    return start + (end - start) * (1 - (1 + omega * tau) * math.exp(-omega * tau))
