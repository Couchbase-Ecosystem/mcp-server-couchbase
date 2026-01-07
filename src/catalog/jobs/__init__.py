"""
Jobs module for catalog schema inference.

Provides parallel execution and job queue management for INFER queries.
"""

from .executor import InferenceResult, ParallelInferenceExecutor
from .queue import InferenceJob, InferenceJobQueue, JobPriority, JobStatus

__all__ = [
    "InferenceJob",
    "InferenceJobQueue",
    "InferenceResult",
    "JobPriority",
    "JobStatus",
    "ParallelInferenceExecutor",
]
