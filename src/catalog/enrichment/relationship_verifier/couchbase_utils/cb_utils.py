"""Minimal helpers for Couchbase schema discovery."""

from __future__ import annotations

import logging
import random
import time
from contextlib import suppress
from datetime import timedelta
from typing import Any

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.kv_range_scan import RangeScan, SamplingScan
from couchbase.management.buckets import BucketType, CreateBucketSettings
from couchbase.options import (
    ClusterOptions,
    ExistsOptions,
    QueryOptions,
    RemoveOptions,
    ScanOptions,
    UpsertOptions,
)

__all__ = ["CB"]

logger = logging.getLogger(__name__)


class CB:
    """Minimal Couchbase helper with an internal cluster handle."""

    UPSERT_RETRY_ATTEMPTS = 3
    UPSERT_RETRY_BACKOFF_SECONDS = 0.5
    UPSERT_RETRY_JITTER_MAX_SECONDS = 0.2

    def __init__(self) -> None:
        self.__cluster: Cluster | None = None

    @classmethod
    def from_cluster(cls, cluster: Cluster) -> CB:
        """Create a helper instance that reuses an existing cluster connection."""
        instance = cls()
        instance.__cluster = cluster
        return instance

    @property
    def cluster(self) -> Cluster:
        """Return the underlying Couchbase cluster handle."""
        if self.__cluster is None:
            raise RuntimeError(
                "Cluster is not connected. Call connect_to_cluster first."
            )
        return self.__cluster

    def connect_to_cluster(
        self,
        connection_string: str,
        username: str,
        password: str,
        timeout_seconds: int = 30,
    ) -> None:
        """Connect to a Couchbase cluster and cache the connection."""
        authenticator = PasswordAuthenticator(username, password)
        options = ClusterOptions(authenticator)
        options.apply_profile("wan_development")
        cluster = Cluster.connect(connection_string, options)
        cluster.wait_until_ready(timedelta(seconds=timeout_seconds))
        self.__cluster = cluster

    def get_all_namespaces(self, timeout_seconds: int = 30) -> list[list[str]]:
        """Return all namespaces as [[bucket, scope, collection], ...]."""
        if self.__cluster is None:
            raise RuntimeError(
                "Cluster is not connected. Call connect_to_cluster first."
            )

        # Use management APIs to avoid system keyspace shape differences across versions.
        namespaces: list[list[str]] = []
        bucket_manager = self.__cluster.buckets()
        all_buckets = bucket_manager.get_all_buckets(
            timeout=timedelta(seconds=timeout_seconds)
        )
        bucket_names: list[str] = []

        if isinstance(all_buckets, dict):
            bucket_names = [name for name in all_buckets if isinstance(name, str)]
        else:
            for bucket_info in all_buckets:
                name = getattr(bucket_info, "name", None)
                if name is None and isinstance(bucket_info, dict):
                    name = bucket_info.get("name")
                if isinstance(name, str):
                    bucket_names.append(name)

        for bucket_name in bucket_names:
            bucket = self.__cluster.bucket(bucket_name)
            scopes = bucket.collections().get_all_scopes(
                timeout=timedelta(seconds=timeout_seconds)
            )
            for scope in scopes:
                for collection in scope.collections:
                    namespaces.append([bucket_name, scope.name, collection.name])

        namespaces.sort()
        return namespaces

    def get_inferred_schema(
        self,
        bucket_name: str,
        scope_name: str,
        collection_name: str,
        timeout_seconds: int = 60,
    ) -> list[dict[str, Any]]:
        """Return INFER output for a bucket.scope.collection."""
        keyspace = ".".join(
            (
                self.__quote_identifier(bucket_name),
                self.__quote_identifier(scope_name),
                self.__quote_identifier(collection_name),
            )
        )
        statement = f"INFER {keyspace};"
        rows = self.__run_query(statement, timeout_seconds)

        schemas: list[dict[str, Any]] = []
        for row in rows:
            if isinstance(row, dict):
                schemas.append(row)
            elif isinstance(row, list):
                for item in row:
                    if isinstance(item, dict):
                        schemas.append(item)

        return schemas

    def run_query(self, statement: str, timeout_seconds: int = 30) -> list[Any]:
        """Run a raw SQL++ query and return all rows."""
        return self.__run_query(statement, timeout_seconds)

    def get_collection_document_count(
        self,
        bucket_name: str,
        scope_name: str,
        collection_name: str,
        timeout_seconds: int = 30,
    ) -> int:
        """Return number of documents in a collection."""
        keyspace = ".".join(
            (
                self.__quote_identifier(bucket_name),
                self.__quote_identifier(scope_name),
                self.__quote_identifier(collection_name),
            )
        )
        rows = self.__run_query(
            f"SELECT RAW COUNT(1) FROM {keyspace};",
            timeout_seconds,
        )
        if not rows:
            return 0
        return int(rows[0])

    def create_scope(
        self,
        bucket_name: str,
        scope_name: str,
        timeout_seconds: int = 30,
    ) -> None:
        """Create a new scope in a bucket."""
        statement = (
            f"CREATE SCOPE {self.__quote_identifier(bucket_name)}"
            f".{self.__quote_identifier(scope_name)};"
        )
        self.__run_query(statement, timeout_seconds)

    def create_bucket(
        self,
        bucket_name: str,
        timeout_seconds: int = 60,
    ) -> None:
        """Create a new bucket in the cluster with standard settings."""
        if self.__cluster is None:
            raise RuntimeError(
                "Cluster is not connected. Call connect_to_cluster first."
            )

        try:
            bucket_manager = self.__cluster.buckets()

            settings = CreateBucketSettings(
                name=bucket_name,
                bucket_type=BucketType.COUCHBASE,
                ram_quota_mb=256,
                num_replicas=0,
            )
            bucket_manager.create_bucket(
                settings,
                timeout=timedelta(seconds=timeout_seconds),
            )
        except Exception:
            # Bucket may already exist, which is fine
            pass

    def delete_bucket(
        self,
        bucket_name: str,
        timeout_seconds: int = 60,
    ) -> None:
        """Delete a bucket from the cluster and wait for removal."""
        if self.__cluster is None:
            raise RuntimeError(
                "Cluster is not connected. Call connect_to_cluster first."
            )

        bucket_manager = self.__cluster.buckets()
        # Bucket may already be absent, which is fine.
        with suppress(Exception):
            bucket_manager.drop_bucket(
                bucket_name,
                timeout=timedelta(seconds=timeout_seconds),
            )

        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            all_buckets = bucket_manager.get_all_buckets(
                timeout=timedelta(seconds=timeout_seconds)
            )
            if bucket_name not in all_buckets:
                return
            time.sleep(0.5)

        raise TimeoutError(
            f"Bucket {bucket_name!r} was not deleted within {timeout_seconds} seconds"
        )

    def delete_scope(
        self,
        bucket_name: str,
        scope_name: str,
        timeout_seconds: int = 30,
    ) -> None:
        """Delete a scope from a bucket."""
        statement = (
            f"DROP SCOPE {self.__quote_identifier(bucket_name)}"
            f".{self.__quote_identifier(scope_name)};"
        )
        self.__run_query(statement, timeout_seconds)

    def create_collection(
        self,
        bucket_name: str,
        scope_name: str,
        collection_name: str,
        timeout_seconds: int = 30,
    ) -> None:
        """Create a new collection in a scope."""
        statement = (
            f"CREATE COLLECTION {self.__quote_identifier(bucket_name)}"
            f".{self.__quote_identifier(scope_name)}"
            f".{self.__quote_identifier(collection_name)};"
        )
        self.__run_query(statement, timeout_seconds)

    def delete_collection(
        self,
        bucket_name: str,
        scope_name: str,
        collection_name: str,
        timeout_seconds: int = 30,
    ) -> None:
        """Delete a collection from a scope."""
        statement = (
            f"DROP COLLECTION {self.__quote_identifier(bucket_name)}"
            f".{self.__quote_identifier(scope_name)}"
            f".{self.__quote_identifier(collection_name)};"
        )
        self.__run_query(statement, timeout_seconds)

    def upsert_document(
        self,
        bucket_name: str,
        scope_name: str,
        collection_name: str,
        document_id: str,
        document_body: Any,
        timeout_seconds: int = 30,
    ) -> None:
        """Insert or update a document in a collection."""
        collection = self.__get_collection(bucket_name, scope_name, collection_name)
        last_error: Exception | None = None

        for attempt in range(1, self.UPSERT_RETRY_ATTEMPTS + 1):
            try:
                collection.upsert(
                    document_id,
                    document_body,
                    UpsertOptions(timeout=timedelta(seconds=timeout_seconds)),
                )
                return
            except Exception as error:
                if not self._is_transient_upsert_error(error):
                    raise
                last_error = error
                if attempt >= self.UPSERT_RETRY_ATTEMPTS:
                    break
                sleep_seconds = self.UPSERT_RETRY_BACKOFF_SECONDS + random.uniform(
                    0,
                    self.UPSERT_RETRY_JITTER_MAX_SECONDS,
                )
                logger.debug(
                    "Retrying upsert after transient KV error (attempt %s/%s): %s.%s.%s key=%s error=%s",
                    attempt + 1,
                    self.UPSERT_RETRY_ATTEMPTS,
                    bucket_name,
                    scope_name,
                    collection_name,
                    document_id,
                    error,
                )
                time.sleep(sleep_seconds)

        if last_error is not None:
            raise last_error
        raise RuntimeError("Upsert failed without an exception")

    def remove_document(
        self,
        bucket_name: str,
        scope_name: str,
        collection_name: str,
        document_id: str,
        timeout_seconds: int = 30,
    ) -> None:
        """Remove a document from a collection by id."""
        collection = self.__get_collection(bucket_name, scope_name, collection_name)
        collection.remove(
            document_id,
            RemoveOptions(timeout=timedelta(seconds=timeout_seconds)),
        )

    def scan_collection_documents(
        self,
        bucket_name: str,
        scope_name: str,
        collection_name: str,
        *,
        limit: int | None = None,
        timeout_seconds: int = 60,
        batch_item_limit: int = 200,
    ) -> list[tuple[str, Any]]:
        """Scan documents in a collection using KV range scan."""
        collection = self.__get_collection(bucket_name, scope_name, collection_name)
        scan_options = ScanOptions(
            timeout=timedelta(seconds=timeout_seconds),
            ids_only=False,
            batch_item_limit=batch_item_limit,
            concurrency=4,
        )
        scan_results = collection.scan(RangeScan(), scan_options)

        rows: list[tuple[str, Any]] = []
        for scan_result in scan_results:
            document_id = getattr(scan_result, "id", None)
            if document_id is None:
                document_id = getattr(scan_result, "key", None)
            value = getattr(scan_result, "value", None)
            if document_id is None:
                continue
            rows.append((str(document_id), value))
            if limit is not None and len(rows) >= limit:
                break

        return rows

    def sample_collection_documents(
        self,
        bucket_name: str,
        scope_name: str,
        collection_name: str,
        *,
        limit: int,
        seed: int | None = None,
        timeout_seconds: int = 60,
    ) -> list[tuple[str, Any]]:
        """Sample documents in a collection using KV sampling scan."""
        if limit <= 0:
            return []

        collection = self.__get_collection(bucket_name, scope_name, collection_name)
        scan_options = ScanOptions(
            timeout=timedelta(seconds=timeout_seconds),
            ids_only=False,
            batch_item_limit=min(limit, 200),
            concurrency=4,
        )
        scan_results = collection.scan(
            SamplingScan(limit=limit, seed=seed), scan_options
        )

        rows: list[tuple[str, Any]] = []
        for scan_result in scan_results:
            document_id = getattr(scan_result, "id", None)
            value = getattr(scan_result, "value", None)
            rows.append((str(document_id), value))

        return rows

    def document_exists(
        self,
        bucket_name: str,
        scope_name: str,
        collection_name: str,
        document_id: str,
        timeout_seconds: int = 30,
    ) -> bool:
        """Return whether a document exists by id in a collection."""
        collection = self.__get_collection(bucket_name, scope_name, collection_name)
        exists_result = collection.exists(
            document_id,
            ExistsOptions(timeout=timedelta(seconds=timeout_seconds)),
        )
        return bool(getattr(exists_result, "exists", False))

    def __run_query(self, statement: str, timeout_seconds: int) -> list[Any]:
        if self.__cluster is None:
            raise RuntimeError(
                "Cluster is not connected. Call connect_to_cluster first."
            )

        result = self.__cluster.query(
            statement,
            QueryOptions(timeout=timedelta(seconds=timeout_seconds)),
        )
        return list(result.rows())

    def __get_collection(
        self, bucket_name: str, scope_name: str, collection_name: str
    ) -> Any:
        if self.__cluster is None:
            raise RuntimeError(
                "Cluster is not connected. Call connect_to_cluster first."
            )

        bucket = self.__cluster.bucket(bucket_name)
        scope = bucket.scope(scope_name)
        return scope.collection(collection_name)

    @staticmethod
    def _is_transient_upsert_error(error: Exception) -> bool:
        message = str(error).lower()
        transient_markers = (
            "request_canceled",
            "scope not found",
            "collection not found",
            "keyspace not found",
            "manifest",
            "not ready",
        )
        return any(marker in message for marker in transient_markers)

    @staticmethod
    def __quote_identifier(identifier: str) -> str:
        safe_identifier = identifier.replace("`", "``")
        return f"`{safe_identifier}`"
