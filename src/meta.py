import logging
from typing import Any, Dict, List
from couchbase.cluster import Cluster
from couchbase.bucket import Bucket
from couchbase.management.buckets import BucketSettings
from couchbase.management.search import SearchIndex
from couchbase.management.queries import QueryIndex
from couchbase.exceptions import CouchbaseException

# Configure logging for this module
meta_logger = logging.getLogger(__name__)
meta_logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
if not meta_logger.handlers:
    meta_logger.addHandler(handler)


def _get_cluster_info(cluster: Cluster) -> Dict[str, Any]:
    """Helper function to get basic cluster diagnostics."""
    try:
        diag = cluster.diagnostics()
        # Convert diagnostics report to a simpler dict
        report = {
            'id': diag.id,
            'sdk': diag.sdk,
            'state': diag.state.name,
            'endpoints': {}
        }
        for service_type, endpoints in diag.endpoints.items():
             report['endpoints'][service_type.name] = [
                 {'id': ep.id, 'state': ep.state.name, 'local': ep.local, 'remote': ep.remote, 'last_activity': str(ep.last_activity)}
                 for ep in endpoints
             ]
        return report
    except CouchbaseException as e:
        meta_logger.error(f"Error getting cluster info: {type(e).__name__} - {e}")
        raise # Re-raise for the tool wrapper to handle
    except Exception as e: # Catch potential other errors during dict creation
        meta_logger.error(f"Unexpected error processing cluster info: {type(e).__name__} - {e}")
        raise


def _get_bucket_info(bucket: Bucket) -> Dict[str, Any]:
    """Helper function to get bucket settings."""
    try:
        # Need the bucket manager associated with the cluster
        bm = bucket.bucket_manager()
        settings = bm.get_bucket(bucket.name) # Get settings for the specific bucket instance

        # Convert BucketSettings to a dictionary
        return {
            'name': settings.name,
            'bucket_type': settings.bucket_type.value,
            'ram_quota_mb': settings.ram_quota_mb,
            'num_replicas': settings.num_replicas,
            'replica_indexes': settings.replica_indexes,
            'flush_enabled': settings.flush_enabled,
            'max_ttl': settings.max_ttl,
            'compression_mode': settings.compression_mode.value,
            'minimum_durability_level': str(settings.minimum_durability_level), # Convert enum/object to string
            'storage_backend': settings.storage_backend.value,
            'eviction_policy': settings.eviction_policy.value,
            'conflict_resolution_type': settings.conflict_resolution_type.value,
        }
    except CouchbaseException as e:
        meta_logger.error(f"Error getting bucket info for {bucket.name}: {type(e).__name__} - {e}")
        raise
    except Exception as e: # Catch potential other errors during dict creation
        meta_logger.error(f"Unexpected error processing bucket info: {type(e).__name__} - {e}")
        raise


def _list_fts_indexes(cluster: Cluster) -> List[Dict[str, Any]]:
    """Helper function to list all Full-Text Search (FTS) indexes."""
    try:
        index_manager = cluster.search_indexes()
        indexes = index_manager.get_all_indexes()
        # Convert SearchIndex objects to dictionaries
        result_list = []
        for index in indexes:
            result_list.append({
                'name': index.name,
                'type': index.type,
                'source_name': index.source_name,
                'uuid': index.uuid,
                'params': index.params, # These can be complex dicts
                'source_params': index.source_params,
                'plan_params': index.plan_params,
                'source_uuid': index.source_uuid,
            })
        return result_list
    except CouchbaseException as e:
        meta_logger.error(f"Error listing FTS indexes: {type(e).__name__} - {e}")
        raise
    except Exception as e: # Catch potential other errors during dict creation
        meta_logger.error(f"Unexpected error processing FTS indexes: {type(e).__name__} - {e}")
        raise


def _list_n1ql_indexes(cluster: Cluster, bucket_name: str) -> List[Dict[str, Any]]:
    """Helper function to list N1QL (Query) indexes for a specific bucket."""
    try:
        index_manager = cluster.query_indexes()
        indexes = index_manager.get_all_indexes(bucket_name=bucket_name)
        # Convert QueryIndex objects to dictionaries
        result_list = []
        for index in indexes:
            result_list.append({
                'name': index.name,
                'keyspace': index.keyspace_id, # Corrected attribute name
                'namespace': index.namespace_id, # Corrected attribute name
                'keys': index.index_key, # Corrected attribute name
                'condition': index.condition,
                'state': index.state,
                'type': index.type.value, # e.g., 'gsi'
                'is_primary': index.is_primary,
                'partition': index.partition
            })
        return result_list
    except CouchbaseException as e:
        meta_logger.error(f"Error listing N1QL indexes for bucket {bucket_name}: {type(e).__name__} - {e}")
        raise
    except Exception as e: # Catch potential other errors during dict creation
        meta_logger.error(f"Unexpected error processing N1QL indexes: {type(e).__name__} - {e}")
        raise 