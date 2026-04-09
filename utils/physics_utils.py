"""Physics simulation utilities for RabAI AutoClick.

Provides:
- Basic physics: velocity, acceleration, momentum
- Projectile motion
- Collision detection (circle-circle, circle-rect, AABB)
- Simple rigid body simulation
"""

from typing import List, Tuple, Optional, Dict
import math


class PhysicsBody:
    """A simple physics body."""

    def __init__(
        self,
        x: float = 0.0,
        y: float = 0.0,
        vx: float = 0.0,
        vy: float = 0.0,
        mass: float = 1.0,
        radius: float = 10.0,
        restitution: float = 0.8,
    ):
        """Initialize physics body.

        Args:
            x, y: Position.
            vx, vy: Velocity.
            mass: Mass.
            radius: Collision radius.
            restitution: Bounciness (0 = inelastic, 1 = elastic).
        """
        self.x = x
        self.y = y
        self.vx = vx
        self.vy = vy
        self.mass = mass
        self.radius = radius
        self.restitution = restitution
        self.ax = 0.0
        self.ay = 0.0

    def apply_force(self, fx: float, fy: float) -> None:
        """Apply force to body."""
        self.ax += fx / self.mass
        self.ay += fy / self.mass

    def update(self, dt: float) -> None:
        """Update body by dt seconds."""
        self.vx += self.ax * dt
        self.vy += self.ay * dt
        self.x += self.vx * dt
        self.y += self.vy * dt
        self.ax = 0.0
        self.ay = 0.0

    def kinetic_energy(self) -> float:
        """Compute kinetic energy."""
        v_sq = self.vx * self.vx + self.vy * self.vy
        return 0.5 * self.mass * v_sq

    def momentum(self) -> Tuple[float, float]:
        """Return momentum vector."""
        return (self.mass * self.vx, self.mass * self.vy)


def projectile_motion(
    v0: float,
    angle: float,
    height: float = 0.0,
    g: float = 9.81,
) -> Tuple[float, float]:
    """Calculate projectile range and max height.

    Args:
        v0: Initial velocity.
        angle: Launch angle in radians.
        height: Initial height.
        g: Gravity.

    Returns:
        (range, max_height).
    """
    vx = v0 * math.cos(angle)
    vy = v0 * math.sin(angle)
    time_of_flight = (vy + math.sqrt(vy * vy + 2 * g * height)) / g
    range_dist = vx * time_of_flight
    max_h = height + vy * vy / (2 * g)
    return (range_dist, max_h)


def projectile_position(
    v0: float,
    angle: float,
    t: float,
    height: float = 0.0,
    g: float = 9.81,
) -> Tuple[float, float]:
    """Get projectile position at time t.

    Returns:
        (x, y) position.
    """
    vx = v0 * math.cos(angle)
    vy = v0 * math.sin(angle)
    x = vx * t
    y = height + vy * t - 0.5 * g * t * t
    return (x, y)


def collision_circle_circle(
    x1: float, y1: float, r1: float,
    x2: float, y2: float, r2: float,
) -> bool:
    """Check circle-circle collision."""
    dx = x2 - x1
    dy = y2 - y1
    dist_sq = dx * dx + dy * dy
    rad_sum = r1 + r2
    return dist_sq <= rad_sum * rad_sum


def collision_circle_rect(
    cx: float, cy: float, radius: float,
    rx: float, ry: float, rw: float, rh: float,
) -> bool:
    """Check circle-rectangle collision."""
    closest_x = max(rx, min(cx, rx + rw))
    closest_y = max(ry, min(cy, ry + rh))
    dx = cx - closest_x
    dy = cy - closest_y
    return dx * dx + dy * dy <= radius * radius


def collision_aabb(
    ax: float, ay: float, aw: float, ah: float,
    bx: float, by: float, bw: float, bh: float,
) -> bool:
    """Check AABB (axis-aligned bounding box) collision."""
    return ax < bx + bw and ax + aw > bx and ay < by + bh and ay + ah > by


def resolve_collision_circle(
    body1: PhysicsBody,
    body2: PhysicsBody,
) -> Optional[Tuple[float, float]]:
    """Resolve elastic collision between two circles.

    Returns:
        (nx, ny) collision normal, or None if no collision.
    """
    dx = body2.x - body1.x
    dy = body2.y - body1.y
    dist_sq = dx * dx + dy * dy
    rad_sum = body1.radius + body2.radius

    if dist_sq >= rad_sum * rad_sum:
        return None

    dist = math.sqrt(dist_sq)
    if dist < 1e-10:
        return (1.0, 0.0)

    nx, ny = dx / dist, dy / dist

    # Relative velocity
    dvx = body1.vx - body2.vx
    dvy = body1.vy - body2.vy
    dvn = dvx * nx + dvy * ny

    # Don't resolve if velocities are separating
    if dvn > 0:
        return (nx, ny)

    e = min(body1.restitution, body2.restitution)
    m1, m2 = body1.mass, body2.mass
    j = -(1 + e) * dvn / (1 / m1 + 1 / m2)

    body1.vx += j * nx / m1
    body1.vy += j * ny / m1
    body2.vx -= j * nx / m2
    body2.vy -= j * ny / m2

    # Separate overlapping bodies
    overlap = rad_sum - dist
    total_mass = m1 + m2
    body1.x -= overlap * nx * m2 / total_mass
    body1.y -= overlap * ny * m2 / total_mass
    body2.x += overlap * nx * m1 / total_mass
    body2.y += overlap * ny * m1 / total_mass

    return (nx, ny)


def gravity_force(
    mass: float,
    g: float = 9.81,
) -> Tuple[float, float]:
    """Compute gravitational force."""
    return (0.0, -mass * g)


def friction_force(
    vx: float,
    vy: float,
    mu: float,
    normal: float = 1.0,
) -> Tuple[float, float]:
    """Compute kinetic friction force."""
    speed = math.sqrt(vx * vx + vy * vy)
    if speed < 1e-10:
        return (0.0, 0.0)
    force_mag = mu * abs(normal)
    return (-force_mag * vx / speed, -force_mag * vy / speed)


def angular_velocity(
    vx: float,
    vy: float,
    pivot_x: float,
    pivot_y: float,
    x: float,
    y: float,
) -> float:
    """Compute angular velocity around a pivot point."""
    rx = x - pivot_x
    ry = y - pivot_y
    return (rx * vy - ry * vx) / (rx * rx + ry * ry)


def simple_gravity_simulation(
    bodies: List[PhysicsBody],
    dt: float,
    g: float = 9.81,
) -> List[PhysicsBody]:
    """Run simple gravity simulation for one step.

    Args:
        bodies: List of physics bodies.
        dt: Time step.
        g: Gravity strength.

    Returns:
        Updated bodies.
    """
    for body in bodies:
        body.apply_force(0.0, -body.mass * g)
        body.update(dt)
    return bodies


def terminal_velocity(
    mass: float,
    drag_coeff: float,
    area: float,
    fluid_density: float = 1.225,
) -> float:
    """Compute terminal velocity for falling object."""
    return math.sqrt(2 * mass * 9.81 / (drag_coeff * area * fluid_density))


def impulse(
    body: PhysicsBody,
    fx: float,
    fy: float,
    dt: float,
) -> None:
    """Apply impulse to body."""
    body.vx += fx * dt / body.mass
    body.vy += fy * dt / body.mass


def damped_oscillator(
    x0: float,
    v0: float,
    t: float,
    k: float,
    c: float,
    m: float,
) -> float:
    """Damped harmonic oscillator position at time t.

    Args:
        x0: Initial position.
        v0: Initial velocity.
        t: Time.
        k: Spring constant.
        c: Damping coefficient.
        m: Mass.

    Returns:
        Position at time t.
    """
    omega0 = math.sqrt(k / m)
    zeta = c / (2 * math.sqrt(k * m))

    if zeta < 1.0:  # Underdamped
        omega1 = omega0 * math.sqrt(1 - zeta * zeta)
        A = x0
        B = (v0 + zeta * omega0 * x0) / omega1
        return math.exp(-zeta * omega0 * t) * (
            A * math.cos(omega1 * t) + B * math.sin(omega1 * t)
        )
    elif zeta == 1.0:  # Critically damped
        omega = omega0
        return (x0 + (v0 + omega * x0) * t) * math.exp(-omega * t)
    else:  # Overdamped
        r1 = -omega0 * (zeta + math.sqrt(zeta * zeta - 1))
        r2 = -omega0 * (zeta - math.sqrt(zeta * zeta - 1))
        A = (x0 * r2 - v0) / (r2 - r1)
        B = (v0 - x0 * r1) / (r2 - r1)
        return A * math.exp(r1 * t) + B * math.exp(r2 * t)
