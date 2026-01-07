"""
Parallel inference executor for schema collection.

This module provides a parallel executor that runs INFER queries concurrently
with controlled concurrency using asyncio.Semaphore.
"""

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, Optional

from acouchbase.bucket import AsyncBucket
from acouchbase.cluster import AsyncCluster

from catalog.schema import SchemaCollection, parse_infer_output
from utils.constants import MCP_SERVER_NAME
from utils.connection import connect_to_bucket_async

logger = logging.getLogger(f"{MCP_SERVER_NAME}.jobs.executor")

# Default concurrency limit for parallel INFER queries
DEFAULT_CONCURRENCY = 5


@dataclass
class InferenceResult:
    """Result of a schema inference operation."""

    bucket: str
    scope: str
    collection: str
    schema: Optional[SchemaCollection]
    indexes: list[dict[str, Any]]
    document_count: int
    error: Optional[Exception] = None

    @property
    def path(self) -> str:
        """Get the collection path."""
        return f"{self.bucket}/{self.scope}/{self.collection}"

    @property
    def success(self) -> bool:
        """Check if inference was successful."""
        return self.error is None and self.schema is not None


@dataclass
class InferenceTask:
    """Task definition for inference execution."""

    bucket: str
    scope: str
    collection: str


class ParallelInferenceExecutor:
    """
    Execute INFER queries in parallel with concurrency control.

    Uses asyncio.Semaphore to limit the number of concurrent INFER queries
    to avoid overloading the query service.

    Usage:
        executor = ParallelInferenceExecutor(cluster, concurrency=5)
        results = await executor.execute_batch(tasks)
    """

    def __init__(
        self,
        cluster: AsyncCluster,
        concurrency: int = DEFAULT_CONCURRENCY,
    ):
        """
        Initialize the parallel executor.

        Args:
            cluster: AsyncCluster connection
            concurrency: Maximum number of concurrent INFER queries
        """
        self._cluster = cluster
        self._concurrency = concurrency
        self._semaphore = asyncio.Semaphore(concurrency)

    async def execute_batch(
        self,
        tasks: list[InferenceTask],
    ) -> list[InferenceResult]:
        """
        Execute INFER on multiple collections in parallel.

        Args:
            tasks: List of InferenceTask objects defining collections to infer

        Returns:
            List of InferenceResult objects (same order as input tasks)
        """
        if not tasks:
            return []

        logger.info(f"Starting parallel inference for {len(tasks)} collections (concurrency={self._concurrency})")

        # Create coroutines for all tasks
        coroutines = [
            self._infer_with_semaphore(task)
            for task in tasks
        ]

        # Execute all in parallel (respecting semaphore)
        results = await asyncio.gather(*coroutines, return_exceptions=False)

        # Count successes and failures
        successes = sum(1 for r in results if r.success)
        failures = len(results) - successes
        logger.info(f"Parallel inference completed: {successes} succeeded, {failures} failed")

        return results

    async def _infer_with_semaphore(self, task: InferenceTask) -> InferenceResult:
        """
        Execute single INFER with semaphore control.

        Args:
            task: InferenceTask defining the collection to infer

        Returns:
            InferenceResult with schema or error
        """
        async with self._semaphore:
            return await self._execute_inference(task)

    async def _execute_inference(self, task: InferenceTask) -> InferenceResult:
        """
        Execute INFER query for a single collection.

        Args:
            task: InferenceTask defining the collection to infer

        Returns:
            InferenceResult with schema, indexes, and document count
        """
        path = f"{task.bucket}/{task.scope}/{task.collection}"

        try:
            logger.debug(f"Starting inference for {path}")

            # Get bucket
            bucket = connect_to_bucket_async(self._cluster, task.bucket)

            # Get document count
            doc_count = await self._get_document_count(bucket, task.scope, task.collection)

            # Run INFER query
            raw_schema = await self._run_infer_query(bucket, task.scope, task.collection)

            # Parse schema
            schema_collection = parse_infer_output(raw_schema)

            # Get indexes
            indexes = await self._get_index_definitions(task.bucket, task.scope, task.collection)

            logger.debug(f"Inference completed for {path}: {len(schema_collection)} variants, {doc_count} docs")

            return InferenceResult(
                bucket=task.bucket,
                scope=task.scope,
                collection=task.collection,
                schema=schema_collection,
                indexes=indexes,
                document_count=doc_count,
            )

        except Exception as e:
            logger.warning(f"Inference failed for {path}: {e}")
            return InferenceResult(
                bucket=task.bucket,
                scope=task.scope,
                collection=task.collection,
                schema=None,
                indexes=[],
                document_count=0,
                error=e,
            )

    async def _get_document_count(
        self,
        bucket: AsyncBucket,
        scope_name: str,
        collection_name: str,
    ) -> int:
        """Get the document count for a collection."""
        try:
            scope = bucket.scope(name=scope_name)
            count_query = f"SELECT RAW COUNT(*) FROM `{collection_name}`"
            count_result = scope.query(count_query)
            async for row in count_result:
                return row
            return 0
        except Exception as e:
            logger.warning(f"Error getting document count for {scope_name}.{collection_name}: {e}")
            return 0

    async def _run_infer_query(
        self,
        bucket: AsyncBucket,
        scope_name: str,
        collection_name: str,
    ) -> list[dict[str, Any]]:
        """Run INFER query on a collection."""
        try:
            scope = bucket.scope(name=scope_name)

            # First check if there are any documents
            count_query = f"SELECT RAW COUNT(*) FROM `{collection_name}` LIMIT 1"
            count_result = scope.query(count_query)
            doc_count = 0
            async for row in count_result:
                doc_count = row

            if doc_count == 0:
                logger.debug(f"Skipping INFER for {scope_name}.{collection_name} (empty)")
                return []

            # Run INFER
            query = f"INFER `{collection_name}`"
            result = scope.query(query)
            schema_list = []
            async for row in result:
                schema_list.append(row)

            # INFER returns a list, flatten if needed
            return schema_list[0] if schema_list else []

        except Exception as e:
            logger.error(f"Error running INFER for {scope_name}.{collection_name}: {e}")
            return []

    async def _get_index_definitions(
        self,
        bucket_name: str,
        scope_name: str,
        collection_name: str,
    ) -> list[dict[str, Any]]:
        """Get index definitions for a collection."""
        try:
            query = (
                f"SELECT meta().id, i.name, i.index_key, i.metadata.definition "
                f"FROM system:indexes as i "
                f"WHERE i.bucket_id = '{bucket_name}' "
                f"AND i.scope_id = '{scope_name}' "
                f"AND i.keyspace_id = '{collection_name}'"
            )
            result = await self._cluster.query(query)
            indexes = []
            async for row in result:
                indexes.append(row)
            # Sort by name for consistency
            return sorted(indexes, key=lambda idx: idx.get("name", ""))
        except Exception as e:
            logger.warning(f"Error fetching indexes for {bucket_name}.{scope_name}.{collection_name}: {e}")
            return []
