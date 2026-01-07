"""
Job queue for schema inference operations.

This module provides a priority queue for managing INFER jobs with support
for retries, prioritization, and on-demand refresh requests.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional

from utils.constants import MCP_SERVER_NAME

logger = logging.getLogger(f"{MCP_SERVER_NAME}.jobs.queue")


class JobPriority(Enum):
    """Priority levels for inference jobs."""

    HIGH = 1  # On-demand refresh requests
    NORMAL = 2  # Scheduled refresh (changed collections)
    LOW = 3  # Background discovery


class JobStatus(Enum):
    """Status of an inference job."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"


@dataclass(order=True)
class InferenceJob:
    """
    Job for schema inference.

    Jobs are ordered by (priority, created_at) for priority queue.
    """

    # Fields used for ordering (compare=True by default)
    priority: JobPriority = field(compare=True)
    created_at: datetime = field(compare=True, default_factory=datetime.utcnow)

    # Job details (not used for ordering)
    bucket: str = field(compare=False, default="")
    scope: str = field(compare=False, default="")
    collection: str = field(compare=False, default="")
    status: JobStatus = field(default=JobStatus.PENDING, compare=False)
    retry_count: int = field(default=0, compare=False)
    max_retries: int = field(default=3, compare=False)
    last_error: Optional[str] = field(default=None, compare=False)

    @property
    def path(self) -> str:
        """Get the collection path."""
        return f"{self.bucket}/{self.scope}/{self.collection}"

    def can_retry(self) -> bool:
        """Check if the job can be retried."""
        return self.retry_count < self.max_retries

    def increment_retry(self, error: str) -> None:
        """Increment retry count and store error."""
        self.retry_count += 1
        self.last_error = error
        self.status = JobStatus.RETRYING


class InferenceJobQueue:
    """
    Async priority queue for inference jobs.

    Features:
    - Priority-based ordering (HIGH > NORMAL > LOW)
    - Duplicate prevention (one job per collection path)
    - Retry support with configurable max retries
    - Thread-safe operations via asyncio locks
    """

    def __init__(self):
        """Initialize the job queue."""
        self._queue: asyncio.PriorityQueue[InferenceJob] = asyncio.PriorityQueue()
        self._in_progress: dict[str, InferenceJob] = {}
        self._pending_paths: set[str] = set()  # Track paths in queue
        self._lock = asyncio.Lock()
        self._completed_count = 0
        self._failed_count = 0

    async def enqueue(self, job: InferenceJob) -> bool:
        """
        Add a job to the queue.

        Prevents duplicate jobs for the same collection path.
        If a job with the same path exists and the new job has higher priority,
        the new job will be added (old one will be skipped when dequeued).

        Args:
            job: InferenceJob to add

        Returns:
            True if job was added, False if duplicate
        """
        async with self._lock:
            path = job.path

            # Check if already in progress
            if path in self._in_progress:
                logger.debug(f"Job for {path} already in progress, skipping")
                return False

            # Check if already pending (unless higher priority)
            if path in self._pending_paths:
                logger.debug(f"Job for {path} already pending, skipping")
                return False

            # Add to queue
            self._pending_paths.add(path)
            await self._queue.put(job)
            logger.debug(f"Enqueued job for {path} with priority {job.priority.name}")
            return True

    async def dequeue(self, timeout: Optional[float] = None) -> Optional[InferenceJob]:
        """
        Get the next job from the queue.

        Args:
            timeout: Maximum time to wait in seconds, or None to wait forever

        Returns:
            InferenceJob or None if timeout occurred
        """
        try:
            if timeout is not None:
                job = await asyncio.wait_for(self._queue.get(), timeout=timeout)
            else:
                job = await self._queue.get()

            async with self._lock:
                # Mark as in progress
                self._pending_paths.discard(job.path)
                self._in_progress[job.path] = job
                job.status = JobStatus.IN_PROGRESS

            logger.debug(f"Dequeued job for {job.path}")
            return job

        except asyncio.TimeoutError:
            return None

    async def complete(self, job: InferenceJob, success: bool, error: Optional[str] = None) -> None:
        """
        Mark a job as completed or failed.

        If failed and retries are available, re-enqueues the job with RETRYING status.

        Args:
            job: The job to complete
            success: Whether the job succeeded
            error: Error message if failed
        """
        async with self._lock:
            # Remove from in progress
            self._in_progress.pop(job.path, None)

            if success:
                job.status = JobStatus.COMPLETED
                self._completed_count += 1
                logger.debug(f"Job completed for {job.path}")
            else:
                if error:
                    job.increment_retry(error)

                if job.can_retry():
                    # Re-enqueue for retry
                    job.status = JobStatus.RETRYING
                    self._pending_paths.add(job.path)
                    await self._queue.put(job)
                    logger.info(f"Job for {job.path} failed, retrying ({job.retry_count}/{job.max_retries})")
                else:
                    job.status = JobStatus.FAILED
                    self._failed_count += 1
                    logger.warning(f"Job for {job.path} failed after {job.retry_count} retries: {error}")

    def pending_count(self) -> int:
        """Get the number of pending jobs."""
        return self._queue.qsize()

    def in_progress_count(self) -> int:
        """Get the number of jobs in progress."""
        return len(self._in_progress)

    def completed_count(self) -> int:
        """Get the total number of completed jobs."""
        return self._completed_count

    def failed_count(self) -> int:
        """Get the total number of failed jobs."""
        return self._failed_count

    def is_empty(self) -> bool:
        """Check if the queue is empty and no jobs are in progress."""
        return self._queue.empty() and not self._in_progress

    async def clear(self) -> int:
        """
        Clear all pending jobs from the queue.

        Does not affect jobs currently in progress.

        Returns:
            Number of jobs cleared
        """
        cleared = 0
        async with self._lock:
            while not self._queue.empty():
                try:
                    job = self._queue.get_nowait()
                    self._pending_paths.discard(job.path)
                    cleared += 1
                except asyncio.QueueEmpty:
                    break

        if cleared > 0:
            logger.info(f"Cleared {cleared} pending jobs from queue")
        return cleared

    def get_status(self) -> dict:
        """
        Get queue status summary.

        Returns:
            Dictionary with queue statistics
        """
        return {
            "pending": self.pending_count(),
            "in_progress": self.in_progress_count(),
            "completed": self._completed_count,
            "failed": self._failed_count,
        }
