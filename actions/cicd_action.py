"""
CI/CD pipeline executor for automated build, test, and deployment workflows.

Supports GitHub Actions, GitLab CI, Jenkins, and generic pipeline definitions.
"""
from __future__ import annotations

import json
import os
import subprocess
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Optional


class PipelineStatus(Enum):
    """Pipeline execution status."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILURE = "failure"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"


class StageStatus(Enum):
    """Individual stage status."""
    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class Job:
    """A CI/CD job within a stage."""
    name: str
    script: list[str]
    environment: dict = field(default_factory=dict)
    artifacts: list[str] = field(default_factory=list)
    timeout_seconds: int = 3600
    retry_count: int = 0
    status: StageStatus = StageStatus.NOT_STARTED
    output: str = ""
    start_time: Optional[float] = None
    end_time: Optional[float] = None

    def duration_seconds(self) -> float:
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0.0


@dataclass
class Stage:
    """A stage within a pipeline."""
    name: str
    jobs: list[Job]
    allow_failure: bool = False
    status: StageStatus = StageStatus.NOT_STARTED
    start_time: Optional[float] = None
    end_time: Optional[float] = None

    def duration_seconds(self) -> float:
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0.0


@dataclass
class PipelineRun:
    """A complete pipeline execution run."""
    id: str
    name: str
    stages: list[Stage]
    status: PipelineStatus = PipelineStatus.PENDING
    branch: str = "main"
    commit_sha: str = ""
    triggered_by: str = "manual"
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    artifacts: dict[str, str] = field(default_factory=dict)

    def duration_seconds(self) -> float:
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0.0

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "status": self.status.value,
            "branch": self.branch,
            "commit_sha": self.commit_sha,
            "triggered_by": self.triggered_by,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": self.duration_seconds(),
            "stages": [
                {
                    "name": s.name,
                    "status": s.status.value,
                    "duration_seconds": s.duration_seconds(),
                    "jobs": [
                        {
                            "name": j.name,
                            "status": j.status.value,
                            "duration_seconds": j.duration_seconds(),
                        }
                        for j in s.jobs
                    ],
                }
                for s in self.stages
            ],
        }


class PipelineExecutor:
    """
    CI/CD pipeline executor for automated workflows.

    Supports multi-stage pipelines with parallel job execution,
    artifact passing, and conditional stage execution.
    """

    def __init__(self, workspace: Optional[Path] = None):
        self.workspace = workspace or Path.cwd()
        self._pipeline_runs: dict[str, PipelineRun] = {}

    def create_pipeline(
        self,
        name: str,
        stages: list[Stage],
        branch: str = "main",
        commit_sha: str = "",
        triggered_by: str = "manual",
    ) -> PipelineRun:
        """Create a new pipeline definition."""
        run_id = str(uuid.uuid4())[:8]
        return PipelineRun(
            id=run_id,
            name=name,
            stages=stages,
            branch=branch,
            commit_sha=commit_sha,
            triggered_by=triggered_by,
        )

    def execute_pipeline(self, pipeline: PipelineRun) -> PipelineRun:
        """Execute a pipeline with all its stages and jobs."""
        pipeline.status = PipelineStatus.RUNNING
        self._pipeline_runs[pipeline.id] = pipeline

        for stage in pipeline.stages:
            self._execute_stage(stage, pipeline)

            if stage.status == StageStatus.FAILED and not stage.allow_failure:
                pipeline.status = PipelineStatus.FAILURE
                pipeline.end_time = time.time()
                return pipeline

        failed_stages = [s for s in pipeline.stages if s.status == StageStatus.FAILED]
        if failed_stages:
            pipeline.status = PipelineStatus.FAILURE
        else:
            pipeline.status = PipelineStatus.SUCCESS

        pipeline.end_time = time.time()
        return pipeline

    def _execute_stage(self, stage: Stage, pipeline: PipelineRun) -> None:
        """Execute a single stage."""
        stage.status = StageStatus.IN_PROGRESS
        stage.start_time = time.time()

        for job in stage.jobs:
            self._execute_job(job)

            if job.status == StageStatus.FAILED:
                stage.status = StageStatus.FAILED
                stage.end_time = time.time()
                return

        stage.status = StageStatus.PASSED
        stage.end_time = time.time()

    def _execute_job(self, job: Job) -> None:
        """Execute a single job."""
        job.status = StageStatus.IN_PROGRESS
        job.start_time = time.time()

        env = os.environ.copy()
        env.update(job.environment)

        try:
            for script_line in job.script:
                result = subprocess.run(
                    script_line,
                    shell=True,
                    capture_output=True,
                    text=True,
                    env=env,
                    timeout=job.timeout_seconds,
                )

                job.output += result.stdout + "\n"
                if result.stderr:
                    job.output += "STDERR: " + result.stderr + "\n"

                if result.returncode != 0:
                    job.status = StageStatus.FAILED
                    job.end_time = time.time()
                    return

            job.status = StageStatus.PASSED

        except subprocess.TimeoutExpired:
            job.output += f"\nJob timed out after {job.timeout_seconds} seconds\n"
            job.status = StageStatus.FAILED

        except Exception as e:
            job.output += f"\nJob error: {str(e)}\n"
            job.status = StageStatus.FAILED

        job.end_time = time.time()

    def get_pipeline_status(self, pipeline_id: str) -> Optional[PipelineRun]:
        """Get the status of a pipeline run."""
        return self._pipeline_runs.get(pipeline_id)

    def list_pipeline_runs(self) -> list[dict]:
        """List all pipeline runs."""
        return [run.to_dict() for run in self._pipeline_runs.values()]


def create_pipeline_from_yaml(pipeline_yaml: str) -> list[Stage]:
    """Create pipeline stages from a YAML definition."""
    import yaml

    data = yaml.safe_load(pipeline_yaml)
    stages = []

    for stage_data in data.get("stages", []):
        jobs = []
        for job_data in stage_data.get("jobs", []):
            job = Job(
                name=job_data["name"],
                script=job_data.get("script", []),
                environment=job_data.get("environment", {}),
                timeout_seconds=job_data.get("timeout", 3600),
                retry_count=job_data.get("retry", 0),
            )
            jobs.append(job)

        stage = Stage(
            name=stage_data["name"],
            jobs=jobs,
            allow_failure=stage_data.get("allow_failure", False),
        )
        stages.append(stage)

    return stages
