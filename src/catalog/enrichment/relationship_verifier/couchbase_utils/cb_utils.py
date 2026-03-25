"""Minimal helpers for Couchbase schema discovery."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, QueryOptions, RemoveOptions, UpsertOptions

__all__ = ["CB"]


class CB:
    """Minimal Couchbase helper with an internal cluster handle."""

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
        collection.upsert(
            document_id,
            document_body,
            UpsertOptions(timeout=timedelta(seconds=timeout_seconds)),
        )

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
    def __quote_identifier(identifier: str) -> str:
        safe_identifier = identifier.replace("`", "``")
        return f"`{safe_identifier}`"
