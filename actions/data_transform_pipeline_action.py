"""Data Transform Pipeline Action.

Chains data transformations into reusable pipelines.
"""
from typing import Any, Callable, Dict, List, Optional, TypeVar
from dataclasses import dataclass, field


T = TypeVar("T")


@dataclass
class Transform:
    name: str
    fn: Callable[[Any], Any]
    description: str = ""
    enabled: bool = True


class DataTransformPipelineAction:
    """Chains data transformations."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.transforms: List[Transform] = []

    def add_transform(
        self,
        name: str,
        fn: Callable[[Any], Any],
        description: str = "",
    ) -> "DataTransformPipelineAction":
        self.transforms.append(Transform(name=name, fn=fn, description=description))
        return self

    def transform(self, data: Any) -> Any:
        result = data
        for t in self.transforms:
            if not t.enabled:
                continue
            result = t.fn(result)
        return result

    def enable(self, name: str) -> bool:
        for t in self.transforms:
            if t.name == name:
                t.enabled = True
                return True
        return False

    def disable(self, name: str) -> bool:
        for t in self.transforms:
            if t.name == name:
                t.enabled = False
                return True
        return False

    def clone(self, new_name: str) -> "DataTransformPipelineAction":
        new_pipe = DataTransformPipelineAction(new_name)
        new_pipe.transforms = list(self.transforms)
        return new_pipe
