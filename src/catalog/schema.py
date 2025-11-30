"""
Inverted Index Schema Storage for Couchbase Collections

Supports multiple schema variants per collection (common in document DBs).
Each INFER output may return multiple top-level schemas representing different
document shapes in the collection.

Storage format per variant:
    {
        "field.path": {
            "string": ["sample1", "sample2"],
            "number": [1, 2, 3]
        }
    }

Merging strategy:
- Each schema variant from INFER is kept separate
- When merging two INFER outputs, match variants 1:1 based on 70%+ path similarity
- Matched variants merge samples; unmatched variants are added as new
- Example: run1={s1,s2,s3}, run2={s2,s4,s5} → final={s1,s2,s3,s4,s5}
"""

from typing import Any, Dict, List, Optional, Set, Tuple
from collections import defaultdict
import copy


class SchemaVariant:
    """
    Inverted index for a single schema variant (one document shape).
    
    Structure: path -> type -> samples[]
    
    Example:
        {
            "company.name": {"string": ["TechNova", "Acme"]},
            "company.departments[].name": {"string": ["Engineering", "Marketing"]},
        }
    """
    
    MAX_SAMPLES_PER_TYPE = 10
    
    def __init__(self, index: Optional[Dict[str, Dict[str, List[Any]]]] = None, variant_id: Optional[str] = None):
        """
        Initialize a schema variant.
        
        Args:
            index: Optional existing index to load from
            variant_id: Optional identifier (e.g., from INFER's Flavor field)
        """
        self._index: Dict[str, Dict[str, List[Any]]] = index or {}
        self.variant_id = variant_id
        self.doc_count = 0  # Number of documents matching this variant
    
    def add_field(self, path: str, field_type: str, samples: List[Any] = None):
        """Add or update a field in the index."""
        if not path:
            return
            
        field_type = self._normalize_type(field_type)
        
        if path not in self._index:
            self._index[path] = {}
        
        if field_type not in self._index[path]:
            self._index[path][field_type] = []
        
        if samples:
            existing = self._index[path][field_type]
            for sample in samples:
                if isinstance(sample, (str, int, float, bool)) or sample is None:
                    if sample not in existing and len(existing) < self.MAX_SAMPLES_PER_TYPE:
                        existing.append(sample)
    
    def get_paths(self) -> Set[str]:
        """Get all field paths in this variant."""
        return set(self._index.keys())
    
    def get_path_type_pairs(self) -> Set[Tuple[str, str]]:
        """Get all (path, type) pairs for similarity comparison."""
        pairs = set()
        for path, type_map in self._index.items():
            for field_type in type_map.keys():
                pairs.add((path, field_type))
        return pairs
    
    def similarity(self, other: 'SchemaVariant') -> float:
        """
        Calculate similarity between two schema variants based on path overlap.
        Uses Jaccard similarity on paths (ignoring types for simplicity).
        
        Returns:
            Float between 0.0 and 1.0 representing similarity
        """
        self_paths = self.get_paths()
        other_paths = other.get_paths()
        
        if not self_paths and not other_paths:
            return 1.0
        if not self_paths or not other_paths:
            return 0.0
        
        intersection = self_paths & other_paths
        union = self_paths | other_paths
        
        return len(intersection) / len(union) if union else 0.0
    
    def merge_samples(self, other: 'SchemaVariant'):
        """
        Merge samples from another variant into this one.
        Adds new samples without overwriting existing ones.
        """
        for path, type_map in other._index.items():
            for field_type, samples in type_map.items():
                self.add_field(path, field_type, samples)
        
        self.doc_count += other.doc_count
    
    def to_dict(self) -> Dict[str, Dict[str, List[Any]]]:
        """Export the index as a dictionary."""
        return copy.deepcopy(self._index)
    
    @staticmethod
    def _normalize_type(type_name: str) -> str:
        """Normalize type names to consistent format."""
        type_map = {
            "integer": "number",
            "int": "number",
            "float": "number",
            "double": "number",
            "bool": "boolean",
            "str": "string",
            "dict": "object",
            "list": "array",
        }
        return type_map.get(type_name.lower(), type_name.lower())
    
    def __len__(self) -> int:
        return len(self._index)
    
    def __repr__(self) -> str:
        return f"SchemaVariant({len(self._index)} paths, {self.doc_count} docs, id={self.variant_id})"


class SchemaCollection:
    """
    Collection of schema variants for a single Couchbase collection.
    
    Maintains multiple distinct schema patterns found in documents.
    Supports merging with 70% similarity-based 1:1 matching.
    
    Example:
        Run 1 INFER returns: {s1, s2, s3}
        Run 2 INFER returns: {s2', s4, s5}  (s2' is similar to s2)
        After merge: {s1, s2_merged, s3, s4, s5}
    """
    
    SIMILARITY_THRESHOLD = 0.70  # 70% path overlap required to merge
    
    def __init__(self, variants: Optional[List[SchemaVariant]] = None):
        self._variants: List[SchemaVariant] = variants or []
    
    def add_variant(self, variant: SchemaVariant):
        """Add a new schema variant to the collection."""
        if len(variant) > 0:  # Only add non-empty variants
            self._variants.append(variant)
    
    def get_variants(self) -> List[SchemaVariant]:
        """Get all schema variants."""
        return self._variants
    
    def merge(self, other: 'SchemaCollection'):
        """
        Merge another SchemaCollection into this one using 1:1 matching.
        
        Algorithm:
        1. For each new variant, find the best matching existing variant (highest similarity)
        2. If similarity >= 70%, merge samples into existing variant (mark as matched)
        3. Otherwise, add as a new variant
        4. Each existing variant can only be matched once (1:1 matching)
        """
        # Track which existing variants have been matched
        matched_existing = set()
        
        for new_variant in other._variants:
            best_match_idx = None
            best_similarity = 0.0
            
            # Find best matching unmatched existing variant
            for i, existing_variant in enumerate(self._variants):
                if i in matched_existing:
                    continue  # Skip already matched variants
                    
                sim = existing_variant.similarity(new_variant)
                if sim > best_similarity:
                    best_similarity = sim
                    best_match_idx = i
            
            if best_match_idx is not None and best_similarity >= self.SIMILARITY_THRESHOLD:
                # Merge into existing variant
                self._variants[best_match_idx].merge_samples(new_variant)
                matched_existing.add(best_match_idx)
            else:
                # Add as new variant
                self._variants.append(copy.deepcopy(new_variant))
    
    def to_dict(self) -> List[Dict[str, Any]]:
        """
        Export all variants as a list of dictionaries.
        
        Returns:
            List of variant dicts, each with 'fields', 'doc_count', 'variant_id'
        """
        result = []
        for i, variant in enumerate(self._variants):
            result.append({
                "variant_id": variant.variant_id or f"variant_{i}",
                "doc_count": variant.doc_count,
                "fields": variant.to_dict()
            })
        return result
    
    def to_flat_dict(self) -> Dict[str, Dict[str, List[Any]]]:
        """
        Export all variants merged into a single flat dictionary.
        Useful for simple queries that don't need variant separation.
        """
        merged = SchemaVariant()
        for variant in self._variants:
            merged.merge_samples(variant)
        return merged.to_dict()
    
    @classmethod
    def from_dict(cls, data: List[Dict[str, Any]]) -> 'SchemaCollection':
        """Load a SchemaCollection from serialized format."""
        collection = cls()
        for variant_data in data:
            variant = SchemaVariant(
                index=variant_data.get("fields", {}),
                variant_id=variant_data.get("variant_id")
            )
            variant.doc_count = variant_data.get("doc_count", 0)
            collection.add_variant(variant)
        return collection
    
    def __len__(self) -> int:
        return len(self._variants)
    
    def __repr__(self) -> str:
        total_paths = sum(len(v) for v in self._variants)
        return f"SchemaCollection({len(self._variants)} variants, {total_paths} total paths)"


def parse_infer_output(infer_output: List[Dict[str, Any]]) -> SchemaCollection:
    """
    Parse Couchbase INFER output into a SchemaCollection.
    
    Each top-level schema in INFER output becomes a separate variant.
    
    Args:
        infer_output: List of schema dictionaries returned by INFER
        
    Returns:
        SchemaCollection with variants for each schema pattern
    """
    collection = SchemaCollection()
    
    # Handle nested list structure from INFER
    schemas = []
    for item in infer_output:
        if isinstance(item, list):
            schemas.extend(item)
        else:
            schemas.append(item)
    
    # Create a variant for each top-level schema
    for schema_node in schemas:
        variant = SchemaVariant()
        
        # Extract metadata
        if "#docs" in schema_node:
            variant.doc_count = schema_node["#docs"]
        if "Flavor" in schema_node:
            variant.variant_id = schema_node["Flavor"] or None
        
        # Parse the schema structure
        _traverse_schema(schema_node, "", variant, sample_docs=[])
        
        # Only add non-empty variants
        collection.add_variant(variant)
    
    return collection


def _traverse_schema(
    node: Dict[str, Any], 
    current_path: str, 
    variant: SchemaVariant,
    sample_docs: List[Any]
):
    """
    Recursively traverse schema node and populate the variant.
    
    Args:
        node: Current schema node from INFER output
        current_path: Dot-notation path to current node
        variant: SchemaVariant to populate
        sample_docs: Sample documents/values at this level
    """
    if not isinstance(node, dict):
        return
    
    # Skip internal metadata fields
    if current_path == "~meta" or current_path.endswith(".~meta"):
        return
    
    # Get type(s) for this node
    node_types = _get_node_types(node)
    
    # Get samples
    node_samples = _get_node_samples(node)
    effective_samples = node_samples if node_samples else sample_docs
    
    # Extract scalar samples
    scalar_samples = _extract_scalar_samples(effective_samples)
    
    # Add leaf fields only (skip containers like object/array)
    if current_path and node_types:
        for node_type in node_types:
            if node_type not in ("object", "array"):
                typed_samples = _filter_samples_by_type(scalar_samples, node_type)
                variant.add_field(current_path, node_type, typed_samples)
    
    # Recurse into properties (for objects)
    if "properties" in node:
        for prop_name, prop_data in node["properties"].items():
            if prop_name == "~meta":
                continue
            new_path = f"{current_path}.{prop_name}" if current_path else prop_name
            prop_samples = _extract_property_values(prop_name, effective_samples)
            _traverse_schema(prop_data, new_path, variant, prop_samples)
    
    # Recurse into array items
    if "items" in node:
        items_data = node["items"]
        array_path = f"{current_path}[]" if current_path else "[]"
        flattened_samples = _flatten_arrays(effective_samples)
        
        if isinstance(items_data, dict):
            _traverse_schema(items_data, array_path, variant, flattened_samples)
        elif isinstance(items_data, list):
            # Tuple-style array with different types at each position
            for item in items_data:
                if isinstance(item, dict):
                    _traverse_schema(item, array_path, variant, flattened_samples)


def _get_node_types(node: Dict[str, Any]) -> List[str]:
    """Extract type(s) from a schema node."""
    types = []
    if "type" in node:
        dtype = node["type"]
        if isinstance(dtype, list):
            types.extend(dtype)
        elif isinstance(dtype, str):
            types.append(dtype)
    else:
        if "properties" in node:
            types.append("object")
        elif "items" in node:
            types.append("array")
    return types


def _get_node_samples(node: Dict[str, Any]) -> List[Any]:
    """Extract samples from a schema node."""
    if "samples" not in node:
        return []
    samples = node["samples"]
    return [samples] if not isinstance(samples, list) else samples


def _extract_scalar_samples(samples: List[Any]) -> List[Any]:
    """Extract scalar values from samples."""
    scalars = []
    seen = set()
    
    for sample in samples:
        if isinstance(sample, (str, int, float, bool)) or sample is None:
            key = (type(sample).__name__, sample)
            if key not in seen:
                seen.add(key)
                scalars.append(sample)
        elif isinstance(sample, list):
            for item in sample:
                if isinstance(item, (str, int, float, bool)) or item is None:
                    key = (type(item).__name__, item)
                    if key not in seen:
                        seen.add(key)
                        scalars.append(item)
    return scalars[:10]


def _filter_samples_by_type(samples: List[Any], target_type: str) -> List[Any]:
    """Filter samples to only those matching the target type."""
    type_checkers = {
        "string": lambda x: isinstance(x, str),
        "number": lambda x: isinstance(x, (int, float)) and not isinstance(x, bool),
        "boolean": lambda x: isinstance(x, bool),
        "null": lambda x: x is None,
    }
    checker = type_checkers.get(target_type)
    return [s for s in samples if checker(s)] if checker else samples


def _extract_property_values(prop_name: str, samples: List[Any]) -> List[Any]:
    """Extract values for a specific property from sample documents."""
    if not samples:
        return []
    
    values = []
    for sample in samples:
        if isinstance(sample, dict) and prop_name in sample:
            values.append(sample[prop_name])
        elif isinstance(sample, list):
            for item in sample:
                if isinstance(item, dict) and prop_name in item:
                    values.append(item[prop_name])
    return values


def _flatten_arrays(samples: List[Any]) -> List[Any]:
    """Flatten array samples to get individual items."""
    if not samples:
        return []
    
    flattened = []
    for sample in samples:
        if isinstance(sample, list):
            flattened.extend(sample)
        else:
            flattened.append(sample)
    return flattened


def merge_schema_collections(
    existing: SchemaCollection, 
    new: SchemaCollection
) -> SchemaCollection:
    """
    Merge two schema collections with 70% similarity 1:1 matching.
    
    Args:
        existing: Current schema collection
        new: New schema collection from latest INFER
        
    Returns:
        Merged schema collection
    """
    result = SchemaCollection([copy.deepcopy(v) for v in existing.get_variants()])
    result.merge(new)
    return result


# Backward compatibility alias
SchemaIndex = SchemaVariant


# Example usage and testing
if __name__ == "__main__":
    # Simulate INFER output with 3 schema variants
    infer_run1 = [[
        {
            "#docs": 100,
            "Flavor": "company_full",
            "properties": {
                "company": {
                    "properties": {
                        "name": {"type": "string", "samples": ["TechNova"]},
                        "departments": {
                            "type": "array",
                            "items": {
                                "properties": {
                                    "name": {"type": "string", "samples": ["Engineering"]}
                                }
                            }
                        }
                    }
                }
            }
        },
        {
            "#docs": 50,
            "Flavor": "company_simple",
            "properties": {
                "company": {
                    "properties": {
                        "name": {"type": "string", "samples": ["Acme"]}
                    }
                }
            }
        },
        {
            "#docs": 25,
            "Flavor": "temp_data",
            "properties": {
                "temp": {
                    "type": "array",
                    "items": [{"type": "number"}, {"type": "string"}],
                    "samples": [[1, "abc"]]
                }
            }
        }
    ]]
    
    # Parse first run
    collection1 = parse_infer_output(infer_run1)
    print("=== First INFER Run ===")
    print(f"Collection: {collection1}")
    for i, v in enumerate(collection1.get_variants()):
        print(f"\nVariant {i} ({v.variant_id}): {v.doc_count} docs")
        for path, types in v.to_dict().items():
            print(f"  {path}: {types}")
    
    # Simulate second INFER run with overlapping and new schemas
    infer_run2 = [[
        {
            "#docs": 120,
            "Flavor": "company_full",  # Should match first variant (>70% similar)
            "properties": {
                "company": {
                    "properties": {
                        "name": {"type": "string", "samples": ["GlobalTech"]},
                        "departments": {
                            "type": "array",
                            "items": {
                                "properties": {
                                    "name": {"type": "string", "samples": ["Marketing", "Sales"]}
                                }
                            }
                        }
                    }
                }
            }
        },
        {
            "#docs": 30,
            "Flavor": "order_data",  # New variant (no match)
            "properties": {
                "order": {
                    "properties": {
                        "id": {"type": "string", "samples": ["ORD-001"]},
                        "total": {"type": "number", "samples": [99.99]}
                    }
                }
            }
        }
    ]]
    
    collection2 = parse_infer_output(infer_run2)
    print("\n=== Second INFER Run ===")
    print(f"Collection: {collection2}")
    
    # Merge: {s1, s2, s3} + {s1', s4} -> {s1_merged, s2, s3, s4}
    merged = merge_schema_collections(collection1, collection2)
    print("\n=== After Merge ===")
    print(f"Merged Collection: {merged}")
    for i, v in enumerate(merged.get_variants()):
        print(f"\nVariant {i} ({v.variant_id}): {v.doc_count} docs")
        for path, types in v.to_dict().items():
            print(f"  {path}: {types}")
