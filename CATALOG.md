# Catalog System Architecture

## Overview

The Couchbase MCP Server includes an intelligent catalog system that automatically discovers, maintains, and enriches database schema information. This catalog system enhances the AI's ability to write accurate and optimized SQL++ queries by providing comprehensive context about your database structure, including collections, fields, indexes, and relationships.

**Key capability**: The catalog handles **heterogeneous document collections** by storing multiple schema variants per collection. Each variant represents a distinct document shape, allowing accurate representation of the diverse document structures common in NoSQL databases.

## Table of Contents

- [System Architecture](#system-architecture)
- [Core Components](#core-components)
- [Threading Model](#threading-model)
- [Data Flow](#data-flow)
- [Catalog Lifecycle](#catalog-lifecycle)
- [Schema Storage Format](#schema-storage-format)
- [Enrichment Process](#enrichment-process)
- [Integration with MCP Server](#integration-with-mcp-server)
- [File Locations](#file-locations)
- [Development and Testing](#development-and-testing)

---

## System Architecture

The catalog system operates on a **two-thread architecture** with clear separation of concerns:

```
┌─────────────────────────────────────────────────────────────┐
│                    MCP Server (Main Thread)                  │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  FastMCP Server                                        │ │
│  │  - Tool handling (query, CRUD operations)              │ │
│  │  - Session management                                  │ │
│  │  - Client communication                                │ │
│  └────────────────────────────────────────────────────────┘ │
│                            │                                 │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  Enrichment Component (Async Cron Task)                │ │
│  │  - Polls Store every 2 minutes for enrichment needs    │ │
│  │  - Uses MCP Sampling to request LLM enrichment         │ │
│  │  - Runs in MCP server's async event loop              │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                            │
                            ├─── Reads/Writes
                            ↓
              ┌──────────────────────────┐
              │   Thread-Safe Store      │
              │   (Global Singleton)     │
              │   - Database schema      │
              │   - Enriched prompts     │
              │   - Enrichment flag      │
              │   - Schema hash          │
              │   - Persisted to disk    │
              └──────────────────────────┘
                            ↑
                            ├─── Reads/Writes
                            │
┌─────────────────────────────────────────────────────────────┐
│              Catalog Worker (Background Thread)              │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  Independent async event loop                          │ │
│  │  - Own database connection                             │ │
│  │  - Runs every 5 minutes                                │ │
│  │  - Collects schema via INFER queries                   │ │
│  │  - Collects index definitions                          │ │
│  │  - Detects schema changes via hashing                  │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

---

## Core Components

### 1. Catalog Manager (`catalog_manager.py`)

**Purpose**: Thread lifecycle management

**Responsibilities**:

- Start/stop the catalog background thread
- Check thread status
- Provide clean startup/shutdown hooks for MCP server

**Key Functions**:

```python
start_catalog_thread()   # Start background worker
stop_catalog_thread()    # Graceful shutdown
is_catalog_thread_running()  # Status check
```

### 2. Catalog Worker (`catalog/worker.py`)

**Purpose**: Background schema collection

**Responsibilities**:

- Maintain separate database connection
- Run its own asyncio event loop
- Collect schema information every 5 minutes
- Execute INFER queries on all collections
- Retrieve index definitions from system:indexes
- Merge new schema data with existing data
- Compute schema hash for change detection
- Signal enrichment when changes detected

**Key Features**:

- **Refresh Interval**: 300 seconds (5 minutes)
- **Multi-Variant Merging**: Preserves all schema variants using 70% similarity matching
- **Change Detection**: SHA-256 hash comparison
- **Empty Collection Skipping**: Skips INFER for collections with no documents
- **Resilient**: Continues operating even if connection fails
- **Ordered**: Sorts buckets/scopes/collections for consistency

**Main Functions**:

```python
catalog_worker_loop(stop_event)  # Entry point, creates event loop
_catalog_worker_async(stop_event)  # Async main loop
_collect_buckets_scopes_collections(cluster, existing_data)  # Schema collection
_infer_collection_schema(bucket, scope, collection)  # INFER query execution
_get_index_definitions(cluster, bucket, scope, collection)  # Index retrieval
_compute_schema_hash(schema_data)  # Hash for change detection
```

**Schema Merging in Worker**:

```python
# Parse new INFER output into SchemaCollection
new_schema_collection = parse_infer_output(raw_schema)

# Load existing schema if present
if existing_schema_list:
    existing_schema_collection = SchemaCollection.from_dict(existing_schema_list)
    existing_schema_collection.merge(new_schema_collection)  # 70% similarity
    schema = existing_schema_collection.to_dict()
else:
    schema = new_schema_collection.to_dict()
```

### 3. Schema Module (`catalog/schema.py`)

**Purpose**: Multi-variant schema storage for heterogeneous document collections

**Responsibilities**:

- Parse Couchbase INFER output into schema variants
- Store multiple schema variants per collection (handles document heterogeneity)
- Merge schema variants using 70% similarity-based 1:1 matching
- Export schemas in various formats

**Key Classes**:

#### `SchemaVariant`

Represents a single document shape (schema pattern) found in a collection:

```python
{
    "company.name": {"string": ["TechNova", "Acme"]},
    "company.departments[].name": {"string": ["Engineering", "Marketing"]},
}
```

#### `SchemaCollection`

Manages multiple schema variants for a collection:

```python
# Collection with 3 document shapes
[
    {"variant_id": "company_full", "doc_count": 100, "fields": {...}},
    {"variant_id": "company_simple", "doc_count": 50, "fields": {...}},
    {"variant_id": "temp_data", "doc_count": 25, "fields": {...}}
]
```

**Key Functions**:

- `parse_infer_output()`: Converts INFER results to SchemaCollection
- `merge_schema_collections()`: Combines schemas with 70% similarity matching
- `SchemaVariant.similarity()`: Jaccard similarity for variant matching
- `SchemaCollection.merge()`: 1:1 variant matching and merging

**Merging Strategy**:

```
Run 1 INFER returns: {s1, s2, s3}
Run 2 INFER returns: {s2', s4, s5}  (s2' is 70%+ similar to s2)
After merge: {s1, s2_merged, s3, s4, s5}
```

**Features**:

- Path notation with `[]` for arrays
- Multiple types per path (handles type variance)
- Sample value storage (up to 10 per type)
- 70% Jaccard similarity threshold for variant matching
- Type normalization (integer → number, etc.)

### 4. Catalog Store (`catalog/store/store.py`)

**Purpose**: Thread-safe, persistent data store

**Responsibilities**:

- Store database schema information
- Store enriched prompts from LLM
- Track schema hash for change detection
- Maintain enrichment flags
- Persist state to disk

**Storage Location**: `~/.couchbase_mcp/catalog_state.json`

**Key Methods**:

```python
add_database_info(info)      # Store schema data (persisted)
get_database_info()          # Retrieve schema
add_prompt(prompt)           # Store enriched prompt
get_prompt()                 # Retrieve prompt
set_needs_enrichment(bool)   # Set/clear enrichment flag (persisted)
get_needs_enrichment()       # Check if enrichment needed
set_schema_hash(hash)        # Store hash for change detection (persisted)
get_schema_hash()            # Retrieve current hash
to_dict() / from_dict()      # Serialize/deserialize state
```

**Thread Safety**:

- All methods use `threading.Lock()` for safe concurrent access
- Global singleton uses double-checked locking pattern with `_store_init_lock`

**Persistence**: Automatically saves state to JSON file on updates to database_info, schema_hash, and needs_enrichment

### 5. Enrichment System (`catalog/enrichment/catalog_enrichment.py`)

**Purpose**: LLM-powered schema enrichment

**Responsibilities**:

- Periodically check Store for enrichment needs
- Request LLM analysis via MCP Sampling
- Generate human-readable schema descriptions
- Identify relationships between collections
- Provide query optimization hints
- Store enriched prompts for AI context

**Check Interval**: Polling every 120 seconds (2 minutes)

**Key Functions**:

```python
start_enrichment_cron(session)  # Start enrichment cron task
stop_enrichment_cron()          # Stop enrichment task
is_enrichment_cron_running()    # Check if task is running
run_enrichment_cron(session)    # Main cron loop
_check_and_enrich_catalog(session)  # Check flag and enrich if needed
_request_llm_enrichment(session, db_info)  # Call LLM via sampling
_build_enrichment_prompt(db_info)  # Build prompt for LLM
```

**Enrichment Prompt Includes**:

- Request for collection descriptions
- Request for field descriptions
- Request for relationship identification
- Index analysis for optimization hints
- Sample value analysis for data patterns

---

## Threading Model

### Main Thread (MCP Server)

- **Framework**: FastMCP (async/await)
- **Event Loop**: asyncio (managed by FastMCP)
- **Responsibilities**:
  - Handle incoming MCP requests
  - Execute tools (queries, CRUD operations)
  - Manage client sessions
  - Run enrichment cron task (polls every 2 minutes)

### Background Thread (Catalog Worker)

- **Framework**: Standard Python threading
- **Event Loop**: Separate asyncio event loop
- **Responsibilities**:
  - Maintain independent DB connection
  - Collect schema information
  - Detect changes
  - Update Store

### Why Two Threads?

1. **Isolation**: Background schema collection doesn't block MCP request handling
2. **Reliability**: Worker failures don't crash MCP server
3. **Performance**: Long-running INFER queries don't delay user queries
4. **Separate Connection**: Worker maintains its own DB connection to avoid interference
5. **Independent Lifecycle**: Worker can be started/stopped independently

---

## Data Flow

### 1. Schema Collection Flow

```
┌──────────────────┐
│  Catalog Worker  │
│  (Every 5 min)   │
└────────┬─────────┘
         │
         ├─→ Connect to cluster
         │
         ├─→ Get all buckets (sorted by name)
         │
         ├─→ For each bucket:
         │   ├─→ Get all scopes (sorted by name)
         │   │
         │   ├─→ For each scope:
         │   │   ├─→ Get all collections (sorted by name)
         │   │   │
         │   │   ├─→ For each collection:
         │   │   │   ├─→ Check document count (skip if empty)
         │   │   │   ├─→ Run INFER query
         │   │   │   ├─→ Parse INFER output → SchemaCollection (variants)
         │   │   │   ├─→ Load existing SchemaCollection if present
         │   │   │   ├─→ Merge variants (70% similarity 1:1 matching)
         │   │   │   ├─→ Get index definitions
         │   │   │   └─→ Store collection data
         │   │   │
         │   │   └─→ Store scope data
         │   │
         │   └─→ Store bucket data
         │
         ├─→ Compute schema hash (SHA-256)
         │
         ├─→ Compare with previous hash
         │
         └─→ If changed:
             ├─→ Update Store.database_info
             ├─→ Update Store.schema_hash
             └─→ Set Store.needs_enrichment = True
```

**Variant Merging Algorithm**:

```
For each new variant from INFER:
  1. Find best matching existing variant (highest Jaccard similarity)
  2. If similarity >= 70%:
     - Merge samples into existing variant
     - Mark existing variant as matched (1:1 matching)
  3. Otherwise:
     - Add as new variant to collection
```

### 2. Enrichment Flow

```
┌────────────────────┐
│ Enrichment Task    │
│ (Cron: every 2min) │
└─────────┬──────────┘
          │
          ├─→ Check Store.get_needs_enrichment()
          │   ├─→ If False: sleep 2 minutes, repeat
          │   └─→ If True: continue to enrichment
          │
          ├─→ Get database_info from Store
          │
          ├─→ Build enrichment prompt:
          │   ├─→ Include schema structure
          │   ├─→ Include field samples
          │   ├─→ Include index definitions
          │   └─→ Request descriptions & relationships
          │
          ├─→ Call MCP Sampling API:
          │   └─→ session.create_message(prompt)
          │
          ├─→ Receive LLM response:
          │   └─→ Enriched schema descriptions
          │
          ├─→ Store enriched prompt:
          │   └─→ Store.add_prompt(enriched_prompt)
          │
          ├─→ Clear enrichment flag:
          │   └─→ Store.set_needs_enrichment(False)
          │
          └─→ Sleep 2 minutes, repeat
```

### 3. Query Tool Usage Flow

```
┌──────────────┐
│  MCP Client  │
│  (e.g. AI)   │
└──────┬───────┘
       │
       ├─→ "Write a query to find all users"
       │
       ↓
┌──────────────┐
│ MCP Server   │
│ Query Tool   │
└──────┬───────┘
       │
       ├─→ Get enriched prompt from Store
       │   (provides context about schema)
       │
       ├─→ AI uses context to write query:
       │   SELECT * FROM users WHERE ...
       │
       ├─→ Execute query on cluster
       │
       └─→ Return results to client
```

---

## Catalog Lifecycle

### 1. MCP Server Startup

```python
# 1. MCP Server initialization
main() → app_lifespan()

# 2. Start catalog background thread
start_catalog_thread()
  ↓
  Creates daemon thread
  ↓
  Thread executes: catalog_worker_loop(stop_event)
  ↓
  Creates new asyncio event loop in thread
  ↓
  Runs: _catalog_worker_async(stop_event)

# 3. MCP Server caches session (stdio only)
list_tools() → cache session
  ↓
  Start enrichment task
  ↓
  start_enrichment_cron(session)
  ↓
  Creates async task: run_enrichment_cron()
```

### 2. Normal Operation

```python
# Background Thread (every 5 min):
while not stop_event:
    - Collect schema from cluster
    - Merge with existing schema
    - Detect changes via hash
    - Update Store if changed
    - Set needs_enrichment = True
    - Sleep for 5 minutes

# MCP Server Thread (cron, every 2 min):
while True:
    - Check needs_enrichment flag
    - If True:
        - Request LLM enrichment via sampling
        - Store enriched prompt
        - Set needs_enrichment = False
    - Sleep for 2 minutes
```

### 3. MCP Server Shutdown

```python
# 1. Stop enrichment task
await stop_enrichment_cron()
  ↓
  Cancel async task
  ↓
  Wait for cancellation

# 2. Stop catalog thread
stop_catalog_thread()
  ↓
  Set stop_event
  ↓
  Wait for thread (timeout: 310s)
  ↓
  Thread closes cluster connection
  ↓
  Thread cleans up event loop

# 3. Close main cluster connection
app_context.cluster.close()
```

---

## Schema Storage Format

### Database Info Structure

```json
{
  "buckets": {
    "travel-sample": {
      "name": "travel-sample",
      "scopes": {
        "inventory": {
          "name": "inventory",
          "collections": {
            "airline": {
              "name": "airline",
              "schema": [
                {
                  "variant_id": "airline_full",
                  "doc_count": 187,
                  "fields": {
                    "id": {"number": [10, 137, 1191]},
                    "name": {"string": ["40-Mile Air", "Alaska Airlines"]},
                    "iata": {"string": ["Q5", "AS", "AA"]},
                    "country": {"string": ["United States", "France"]}
                  }
                },
                {
                  "variant_id": "airline_minimal",
                  "doc_count": 23,
                  "fields": {
                    "id": {"number": [500, 501]},
                    "name": {"string": ["Regional Air"]}
                  }
                }
              ],
              "indexes": [
                {
                  "id": ""
                  "name": "def_inventory_airline_primary",
                  "index_key": [],
                  "metadata":{"definition": ""},
                  "state": "online"
                }
              ]
            }
          }
        }
      }
    }
  }
}
```

### Schema Variant Format

Each collection stores a **list of schema variants**, where each variant represents a distinct document shape found in the collection.

**Variant Structure**:

```json
{
  "variant_id": "company_full",    // Identifier (from INFER Flavor or auto-generated)
  "doc_count": 100,                // Number of documents matching this shape
  "fields": {                      // Inverted index of field paths
    "company.name": {"string": ["TechNova", "Acme"]},
    "company.departments[].name": {"string": ["Engineering"]}
  }
}
```

**Path Notation**:

- Nested fields: `company.address.city`
- Array elements: `users[].addresses[].street`
- Root fields: `name`, `age`

**Type Storage**:

- Each path maps to types found at that path
- Each type stores sample values (up to 10)
- Supports multiple types per path (type variance within a variant)

**Example with Multiple Types**:

```json
{
  "order.items[].quantity": {
    "number": [1, 2, 5, 10],
    "string": ["1", "2"]
  },
  "order.items[].product.name": {
    "string": ["Laptop", "Mouse", "Keyboard"]
  }
}
```

### Why Multiple Variants?

Document databases like Couchbase often contain heterogeneous documents in the same collection:

- Different document types (e.g., "order" vs "return" in an orders collection)
- Schema evolution over time
- Optional fields present in some documents

The variant system captures this reality:

```
Collection: orders
├── variant_0 (order_complete): 500 docs - {id, items, total, shipping}
├── variant_1 (order_minimal): 200 docs - {id, items}
└── variant_2 (return_request): 50 docs - {id, original_order_id, reason}
```

---

## Enrichment Process

### What Gets Enriched?

The LLM receives the complete schema structure (including all variants) and is asked to provide:

1. **Collection Descriptions**: Purpose and content of each collection
2. **Variant Analysis**: Different document shapes within collections
3. **Field Descriptions**: Meaning and usage of important fields
4. **Relationships**: Foreign keys, references between collections
5. **Patterns**: Naming conventions, data patterns
6. **Index Analysis**: How indexes affect query optimization
7. **Sample Analysis**: Data formats, status codes, patterns

### Enrichment Prompt Example

```
# Database Schema Analysis Request

Please analyze the following Couchbase database schema and provide:
1. A brief description for each bucket, scope, and collection
2. Field descriptions for important fields in each collection
3. Relationships between collections (foreign keys, references, etc.)
4. Any patterns or conventions you observe

Format your response as a structured prompt that can be used by an AI 
assistant to help users write better SQL++ queries.

## Database Schema:

{...full schema with variants, samples and indexes...}

## Important Instructions:
- Analyze the 'indexes' field to understand optimized fields
- Analyze the 'samples' field to understand data values and formats
- Note different schema variants (document shapes) within collections
- Identify potential join keys
- Suggest optimal query patterns based on indexes
```

### How Enrichment is Used

When a user asks the AI to write a query, the enriched prompt is included in the context:

```
User: "Find all flights from San Francisco to New York"

Context (from enriched prompt):
- Collection 'route' contains flight route information
- Field 'sourceairport' contains departure airport codes
- Field 'destinationairport' contains arrival airport codes
- Index 'idx_route_src_dst' optimizes queries on these fields
- Sample values show airport codes are 3-letter IATA codes

AI generates optimized query:
SELECT * FROM route 
WHERE sourceairport = 'SFO' 
  AND destinationairport IN ['JFK', 'EWR', 'LGA']
```

---

## Integration with MCP Server

### Startup Integration

```python
# mcp_server.py

@asynccontextmanager
async def app_lifespan(server: FastMCP):
    """Initialize MCP server and start catalog thread"""
    
    # 1. Start catalog background thread
    logger.info("Starting catalog background thread")
    start_catalog_thread()
    
    # 2. Create app context
    app_context = AppContext(...)
    
    try:
        yield app_context
    finally:
        # 3. Stop enrichment (async)
        logger.info("Stopping catalog enrichment cron")
        await stop_enrichment_cron()
        
        # 4. Stop catalog thread
        logger.info("Stopping catalog background thread")
        stop_catalog_thread()
        
        # 5. Close connections
        app_context.cluster.close()

class MCPServer(FastMCP):
    async def list_tools(self):
        """Cache session and start enrichment (stdio only)"""
        global _cached_session
        
        if self._transport == "stdio" and _cached_session is None:
            ctx = self.get_context()
            _cached_session = ctx.session
            
            # Start enrichment in background
            start_enrichment_cron(_cached_session)
        
        return await super().list_tools()
```

### Tool Usage Integration

Tools like `query_scope` can access enriched prompts:

```python
from catalog.store.store import get_catalog_store

def query_scope_tool(bucket, scope, query):
    """Execute query with catalog context"""
    
    # Get enriched context
    store = get_catalog_store()
    enriched_prompt = store.get_prompt()
    
    # AI can use enriched_prompt for better query generation
    # (This happens at the LLM level, not in the tool itself)
    
    # Execute query
    result = execute_query(bucket, scope, query)
    return result
```

---

## File Locations

### Source Files

```
src/
├── catalog_manager.py              # Thread lifecycle management
├── catalog/
│   ├── __init__.py
│   ├── worker.py                   # Background schema collection with variant merging
│   ├── schema.py                   # Multi-variant schema storage (SchemaVariant, SchemaCollection)
│   ├── verify.py                   # CLI testing/verification script for catalog system
│   ├── verify_schema.py            # Schema parsing test with sample INFER output
│   ├── store/
│   │   ├── __init__.py             # Store module exports
│   │   └── store.py                # Thread-safe persistent data store
│   └── enrichment/
│       ├── __init__.py             # Enrichment module exports
│       └── catalog_enrichment.py   # Polling-based LLM enrichment cron (every 2 min)
```

### Runtime Files

```
~/.couchbase_mcp/
└── catalog_state.json              # Persisted catalog state
```

### State File Format

```json
{
  "database_info": {
    "buckets": {
      "my-bucket": {
        "name": "my-bucket",
        "scopes": {
          "my-scope": {
            "collections": {
              "my-collection": {
                "name": "my-collection",
                "schema": [
                  {"variant_id": "variant_0", "doc_count": 100, "fields": {...}},
                  {"variant_id": "variant_1", "doc_count": 50, "fields": {...}}
                ],
                "indexes": [...]
              }
            }
          }
        }
      }
    }
  },
  "prompt": "...enriched prompt from LLM...",
  "schema_hash": "abc123...",
  "needs_enrichment": false
}
```

---

## Key Design Decisions

### 1. Why Separate Thread Instead of Async Task?

**Decision**: Use background thread with its own event loop

**Rationale**:

- Independent database connection (doesn't interfere with MCP operations)
- Isolation: Worker failures don't affect MCP server
- Long-running INFER queries don't block MCP request handling
- Can use blocking operations if needed
- Clean separation of concerns

### 2. Why Multi-Variant Schema with Similarity Merging?

**Decision**: Store multiple schema variants per collection and merge using 70% Jaccard similarity

**Rationale**:

- Document databases contain heterogeneous documents (different shapes in same collection)
- INFER returns multiple top-level schemas representing different document patterns
- 70% similarity threshold balances between:
  - Merging truly similar variants (accumulating samples over time)
  - Preserving distinct document shapes
- 1:1 matching prevents a single variant from absorbing multiple new variants
- Provides accurate representation of real collection contents

### 3. Why Hash-Based Change Detection?

**Decision**: Compute SHA-256 hash of schema for change detection

**Rationale**:

- Efficient: Simple string comparison vs deep object diff
- Reliable: Any schema change triggers enrichment
- Deterministic: Same schema always produces same hash
- Fast: O(1) comparison instead of O(n) traversal

### 4. Why Polling-Based Enrichment?

**Decision**: Use periodic polling (every 2 minutes) instead of event-driven signaling

**Rationale**:

- **Simplicity**: No cross-thread event coordination complexity
- **Reliability**: Polling ensures enrichment is eventually triggered even if signals are missed
- **Decoupled**: Worker and enrichment operate independently without shared event objects
- **Reasonable Latency**: 2-minute max delay is acceptable for schema enrichment
- **Thread-Safe**: Simple flag check avoids asyncio Event issues across threads

### 5. Why Store Enriched Prompts Instead of Schema Annotations?

**Decision**: Store complete enriched prompt from LLM

**Rationale**:

- Flexible: LLM can structure response as needed
- Complete: Includes descriptions, relationships, hints
- Reusable: Can be directly used in AI context
- Evolvable: Prompt format can change without schema changes

## Future Enhancements

Potential improvements to the catalog system:

1. **Incremental Updates**: Only INFER changed collections
2. **Event-driven sampling**: Instead periodic pooling use event driven pattern for sampling.
3. **Parallel Schema Inference**: Currently, the worker processes collections sequentially. Implementing parallel execution using `asyncio.gather` with a concurrency limit (e.g., `asyncio.Semaphore(5)`) would significantly speed up the catalog refresh cycle.
4. **Job Queue for Inference**: Decouple the scheduling from execution by using an internal job queue. This allows "spinning up" inference jobs for specific collections efficiently, handling retries for failed jobs, and prioritizing active collections.
5. **On-Demand Granular Refresh**: Add capability to trigger schema inference for a specific bucket, scope, or collection on demand, useful when the user knows a schema has changed.

---

## Summary

The catalog system provides a sophisticated, automated approach to database schema management that:

- **Automatically discovers** database structure via background collection
- **Captures document heterogeneity** with multi-variant schema storage
- **Intelligently merges** schema variants using 70% similarity matching
- **Detects changes** efficiently using hash-based comparison
- **Enriches data** with LLM-generated descriptions and insights
- **Provides context** to AI for better query generation
- **Operates independently** without blocking MCP operations
- **Persists state** for fast startup and resilience
- **Scales efficiently** to large databases

This architecture enables the MCP server to provide rich, contextual information to AI assistants, resulting in more accurate queries, better suggestions, and improved user experience.
