"""Transformation matrix utilities for RabAI AutoClick.

Provides:
- 2D and 3D transformation matrices
- Matrix composition and decomposition
- Common transformations (translate, rotate, scale)
- Homogeneous coordinates
"""

from typing import List, Tuple, Optional
import math


Matrix3 = List[List[float]]
Matrix4 = List[List[float]]


def identity3() -> Matrix3:
    """Create 3x3 identity matrix."""
    return [
        [1, 0, 0],
        [0, 1, 0],
        [0, 0, 1],
    ]


def identity4() -> Matrix4:
    """Create 4x4 identity matrix."""
    return [
        [1, 0, 0, 0],
        [0, 1, 0, 0],
        [0, 0, 1, 0],
        [0, 0, 0, 1],
    ]


def mat3_mul(a: Matrix3, b: Matrix3) -> Matrix3:
    """Multiply two 3x3 matrices."""
    result: Matrix3 = [[0.0] * 3 for _ in range(3)]
    for i in range(3):
        for j in range(3):
            for k in range(3):
                result[i][j] += a[i][k] * b[k][j]
    return result


def mat4_mul(a: Matrix4, b: Matrix4) -> Matrix4:
    """Multiply two 4x4 matrices."""
    result: Matrix4 = [[0.0] * 4 for _ in range(4)]
    for i in range(4):
        for j in range(4):
            for k in range(4):
                result[i][j] += a[i][k] * b[k][j]
    return result


def mat3_transform_point(m: Matrix3, x: float, y: float) -> Tuple[float, float]:
    """Transform 2D point by 3x3 matrix (homogeneous)."""
    wx = m[0][0] * x + m[0][1] * y + m[0][2]
    wy = m[1][0] * x + m[1][1] * y + m[1][2]
    w = m[2][0] * x + m[2][1] * y + m[2][2]
    if abs(w) < 1e-10:
        return (wx, wy)
    return (wx / w, wy / w)


def mat4_transform_point(m: Matrix4, x: float, y: float, z: float) -> Tuple[float, float, float]:
    """Transform 3D point by 4x4 matrix (homogeneous)."""
    wx = m[0][0] * x + m[0][1] * y + m[0][2] * z + m[0][3]
    wy = m[1][0] * x + m[1][1] * y + m[1][2] * z + m[1][3]
    wz = m[2][0] * x + m[2][1] * y + m[2][2] * z + m[2][3]
    w = m[3][0] * x + m[3][1] * y + m[3][2] * z + m[3][3]
    if abs(w) < 1e-10:
        return (wx, wy, wz)
    return (wx / w, wy / w, wz / w)


def translation_matrix2d(dx: float, dy: float) -> Matrix3:
    """Create 2D translation matrix."""
    return [
        [1, 0, dx],
        [0, 1, dy],
        [0, 0, 1],
    ]


def rotation_matrix2d(angle: float) -> Matrix3:
    """Create 2D rotation matrix (radians)."""
    c = math.cos(angle)
    s = math.sin(angle)
    return [
        [c, -s, 0],
        [s, c, 0],
        [0, 0, 1],
    ]


def scale_matrix2d(sx: float, sy: Optional[float] = None) -> Matrix3:
    """Create 2D scale matrix."""
    if sy is None:
        sy = sx
    return [
        [sx, 0, 0],
        [0, sy, 0],
        [0, 0, 1],
    ]


def shear_matrix2d(shx: float, shy: float) -> Matrix3:
    """Create 2D shear matrix."""
    return [
        [1, shx, 0],
        [shy, 1, 0],
        [0, 0, 1],
    ]


def translation_matrix3d(dx: float, dy: float, dz: float) -> Matrix4:
    """Create 3D translation matrix."""
    return [
        [1, 0, 0, dx],
        [0, 1, 0, dy],
        [0, 0, 1, dz],
        [0, 0, 0, 1],
    ]


def rotation_x_matrix3d(angle: float) -> Matrix4:
    """Create 3D rotation matrix around X axis."""
    c = math.cos(angle)
    s = math.sin(angle)
    return [
        [1, 0, 0, 0],
        [0, c, -s, 0],
        [0, s, c, 0],
        [0, 0, 0, 1],
    ]


def rotation_y_matrix3d(angle: float) -> Matrix4:
    """Create 3D rotation matrix around Y axis."""
    c = math.cos(angle)
    s = math.sin(angle)
    return [
        [c, 0, s, 0],
        [0, 1, 0, 0],
        [-s, 0, c, 0],
        [0, 0, 0, 1],
    ]


def rotation_z_matrix3d(angle: float) -> Matrix4:
    """Create 3D rotation matrix around Z axis."""
    c = math.cos(angle)
    s = math.sin(angle)
    return [
        [c, -s, 0, 0],
        [s, c, 0, 0],
        [0, 0, 1, 0],
        [0, 0, 0, 1],
    ]


def scale_matrix3d(sx: float, sy: float, sz: float) -> Matrix4:
    """Create 3D scale matrix."""
    return [
        [sx, 0, 0, 0],
        [0, sy, 0, 0],
        [0, 0, sz, 0],
        [0, 0, 0, 1],
    ]


def perspective_matrix(
    fov: float,
    aspect: float,
    near: float,
    far: float,
) -> Matrix4:
    """Create perspective projection matrix."""
    f = 1.0 / math.tan(fov / 2)
    return [
        [f / aspect, 0, 0, 0],
        [0, f, 0, 0],
        [0, 0, (far + near) / (near - far), (2 * far * near) / (near - far)],
        [0, 0, -1, 0],
    ]


def look_at_matrix(
    eye: Tuple[float, float, float],
    target: Tuple[float, float, float],
    up: Tuple[float, float, float] = (0, 1, 0),
) -> Matrix4:
    """Create look-at view matrix."""
    fx = target[0] - eye[0]
    fy = target[1] - eye[1]
    fz = target[2] - eye[2]
    flen = math.sqrt(fx * fx + fy * fy + fz * fz)
    fx /= flen
    fy /= flen
    fz /= flen

    rx = fy * up[2] - fz * up[1]
    ry = fz * up[0] - fx * up[2]
    rz = fx * up[1] - fy * up[0]
    rlen = math.sqrt(rx * rx + ry * ry + rz * rz)
    rx /= rlen
    ry /= rlen
    rz /= rlen

    ux = ry * fz - rz * fy
    uy = rz * fx - rx * fz
    uz = rx * fy - ry * fx

    return [
        [rx, ux, -fx, 0],
        [ry, uy, -fy, 0],
        [rz, uz, -fz, 0],
        [-rx * eye[0] - ry * eye[1] - rz * eye[2],
         -ux * eye[0] - uy * eye[1] - uz * eye[2],
         fx * eye[0] + fy * eye[1] + fz * eye[2],
         1],
    ]


def decompose_matrix2d(m: Matrix3) -> Tuple[float, float, float, float, float, float]:
    """Decompose 2D matrix into (translation, rotation, scale).

    Returns:
        (tx, ty, rotation, sx, sy, shear)
    """
    tx = m[0][2]
    ty = m[1][2]
    sx = math.sqrt(m[0][0] * m[0][0] + m[1][0] * m[1][0])
    sy = math.sqrt(m[0][1] * m[0][1] + m[1][1] * m[1][1])
    rotation = math.atan2(m[1][0], m[0][0])
    shear = math.atan2(m[0][1], m[1][1]) - math.pi / 2
    return (tx, ty, rotation, sx, sy, shear)


def invert_matrix3(m: Matrix3) -> Optional[Matrix3]:
    """Invert 3x3 matrix."""
    det = (m[0][0] * (m[1][1] * m[2][2] - m[1][2] * m[2][1])
           - m[0][1] * (m[1][0] * m[2][2] - m[1][2] * m[2][0])
           + m[0][2] * (m[1][0] * m[2][1] - m[1][1] * m[2][0]))
    if abs(det) < 1e-10:
        return None
    inv_det = 1.0 / det
    return [
        [
            (m[1][1] * m[2][2] - m[1][2] * m[2][1]) * inv_det,
            (m[0][2] * m[2][1] - m[0][1] * m[2][2]) * inv_det,
            (m[0][1] * m[1][2] - m[0][2] * m[1][1]) * inv_det,
        ],
        [
            (m[1][2] * m[2][0] - m[1][0] * m[2][2]) * inv_det,
            (m[0][0] * m[2][2] - m[0][2] * m[2][0]) * inv_det,
            (m[0][2] * m[1][0] - m[0][0] * m[1][2]) * inv_det,
        ],
        [
            (m[1][0] * m[2][1] - m[1][1] * m[2][0]) * inv_det,
            (m[0][1] * m[2][0] - m[0][0] * m[2][1]) * inv_det,
            (m[0][0] * m[1][1] - m[0][1] * m[1][0]) * inv_det,
        ],
    ]
