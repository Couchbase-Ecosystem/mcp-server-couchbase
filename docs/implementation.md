# Code-Level Walkthrough

## File Structure (What You Added)

```
src/catalog/
├── store/
│   ├── __init__.py
│   └── store.py          ← MODIFIED: Added CollectionMetadata
├── events/               ← NEW FOLDER
│   ├── __init__.py
│   └── bridge.py         ← NEW: ThreadToAsyncBridge
├── jobs/                 ← NEW FOLDER
│   ├── __init__.py
│   ├── executor.py       ← NEW: ParallelInferenceExecutor
│   └── queue.py          ← NEW: InferenceJobQueue
├── worker.py             ← MODIFIED: Incremental + Parallel
└── enrichment/
    └── catalog_enrichment.py  ← MODIFIED: Event-driven

src/tools/
├── __init__.py           ← MODIFIED: Register new tools
└── catalog.py            ← NEW: 4 new MCP tools

docs/
└── CATALOG_ENHANCEMENTS.md  ← NEW: Documentation
```

---

## Enhancement 1: Incremental Updates

### File: `src/catalog/store/store.py`

**What we added:** A way to track each collection's state

```python
@dataclass
class CollectionMetadata:
    """Metadata for tracking collection changes for incremental updates."""

    bucket: str                    # e.g., "travel-sample"
    scope: str                     # e.g., "_default"
    collection: str                # e.g., "airlines"
    schema_hash: str               # Hash of the schema (to detect changes)
    last_infer_time: str           # When we last ran INFER
    document_count: int | None     # How many documents (to detect changes)

    @property
    def path(self) -> str:
        """Get the collection path in bucket/scope/collection format."""
        return f"{self.bucket}/{self.scope}/{self.collection}"
        # Returns: "travel-sample/_default/airlines"
```

**Visual:**

```
┌─────────────────────────────────────────────────────────────┐
│                    CollectionMetadata                        │
├─────────────────────────────────────────────────────────────┤
│  bucket: "travel-sample"                                     │
│  scope: "_default"                                           │
│  collection: "airlines"                                      │
│  schema_hash: "abc123..."     ← Used to detect schema change │
│  last_infer_time: "2024-01-07T10:30:00"                     │
│  document_count: 1500         ← Used to detect data change   │
└─────────────────────────────────────────────────────────────┘
```

In the Store class, we added these methods:

```python
class Store:
    def __init__(self):
        # ... existing fields ...
        self.collection_metadata: dict[str, CollectionMetadata] = {}  # NEW!
        #                         ↑
        #    Key = "bucket/scope/collection"
        #    Value = CollectionMetadata object

    def get_collection_metadata(self, path: str) -> CollectionMetadata | None:
        """Get metadata for a specific collection."""
        with self._lock:  # Thread-safe (prevents data corruption)
            return self.collection_metadata.get(path)

    def set_collection_metadata(self, metadata: CollectionMetadata) -> None:
        """Save metadata for a collection."""
        with self._lock:
            self.collection_metadata[metadata.path] = metadata
            self._save_state()  # Persist to disk
```

---

### File: `src/catalog/worker.py`

The decision logic - should we refresh this collection?

```python
def _needs_refresh(
    stored_metadata: CollectionMetadata | None,  # What we have saved
    current_doc_count: int | None,                # Current count from DB
) -> bool:
    """
    Returns True if we should run INFER on this collection.
    Returns False if we can skip it (no changes).
    """

    # Case 1: New collection we've never seen
    if stored_metadata is None:
        return True  # Must refresh!

    # Case 2: Too old (over 1 hour)
    last_infer = datetime.fromisoformat(stored_metadata.last_infer_time)
    age_seconds = (datetime.utcnow() - last_infer).total_seconds()

    if age_seconds > 3600:  # 1 hour = 3600 seconds
        return True  # Force refresh!

    # Case 3: Document count changed significantly (10% or more)
    if stored_metadata.document_count == 0 and current_doc_count > 0:
        return True  # Was empty, now has data!

    change_ratio = abs(current_doc_count - stored_metadata.document_count) / stored_metadata.document_count
    if change_ratio >= 0.10:  # 10% change
        return True  # Significant change!

    # No refresh needed
    return False
```

**Visual flow:**

```
                    Collection: "airlines"
                           │
                           ▼
              ┌────────────────────────┐
              │  Do we have metadata?  │
              └────────────────────────┘
                    │           │
                   No          Yes
                    │           │
                    ▼           ▼
              ┌─────────┐  ┌──────────────────┐
              │ REFRESH │  │ Is it > 1 hour   │
              └─────────┘  │ since last INFER?│
                           └──────────────────┘
                                │         │
                               Yes        No
                                │         │
                                ▼         ▼
                          ┌─────────┐  ┌──────────────────┐
                          │ REFRESH │  │ Did doc count    │
                          └─────────┘  │ change by ≥10%?  │
                                       └──────────────────┘
                                            │         │
                                           Yes        No
                                            │         │
                                            ▼         ▼
                                      ┌─────────┐  ┌──────┐
                                      │ REFRESH │  │ SKIP │
                                      └─────────┘  └──────┘
```

---

## Enhancement 2: Event-Driven (ThreadToAsyncBridge)

### File: `src/catalog/events/bridge.py`

**The Problem:**

```
┌─────────────────────┐         ┌─────────────────────┐
│  WORKER THREAD      │         │  MAIN THREAD        │
│  (Background)       │   ???   │  (Async/Await)      │
│                     │ ──────► │                     │
│  Runs in a loop     │         │  Enrichment waits   │
│  Detects changes    │         │  for signal         │
└─────────────────────┘         └─────────────────────┘

Problem: These are DIFFERENT threads!
         They can't share asyncio.Event directly.
```

**The Solution: ThreadToAsyncBridge**

```python
class ThreadToAsyncBridge:
    """
    Bridge for signaling from a background thread to an async event loop.

    Think of it as a doorbell that works across threads.
    """

    def __init__(self):
        self._thread_event = threading.Event()  # For thread-side
        self._async_event = None                 # For async-side
        self._target_loop = None                 # The async event loop

    def set_target_loop(self, loop):
        """
        Called by enrichment when it starts.
        "Hey bridge, here's my event loop - send signals here"
        """
        self._target_loop = loop
        self._async_event = asyncio.Event()

    def signal_from_thread(self):
        """
        Called by WORKER THREAD when schema changes.
        "Ring the doorbell!"
        """
        self._thread_event.set()

        # This is the magic! Safely schedule on the other thread's loop
        if self._target_loop and self._async_event:
            self._target_loop.call_soon_threadsafe(self._async_event.set)
            #                 ↑
            #    "Schedule this function to run on the target loop"
            #    This is THREAD-SAFE (won't corrupt data)

    async def wait_for_signal(self, timeout=None):
        """
        Called by ENRICHMENT (async) to wait for signal.
        "Wait by the door for the doorbell"
        """
        try:
            await asyncio.wait_for(self._async_event.wait(), timeout=timeout)
            self._async_event.clear()  # Reset for next signal
            return True   # "Doorbell rang!"
        except asyncio.TimeoutError:
            return False  # "Timed out, no doorbell"
```

**Visual:**

```
WORKER THREAD                           MAIN THREAD (Async)
─────────────                           ───────────────────

                                        bridge.set_target_loop(loop)
                                              │
                                              ▼
                                        ┌─────────────────┐
                                        │ Enrichment:     │
                                        │ await bridge.   │
                                        │ wait_for_signal │
                                        │                 │
                                        │ (sleeping...)   │
                                        └─────────────────┘

Schema changed!                               │
      │                                       │
      ▼                                       │
bridge.signal_from_thread()                   │
      │                                       │
      │  call_soon_threadsafe ───────────────►│
      │  (crosses thread boundary safely)     │
                                              ▼
                                        ┌─────────────────┐
                                        │ WAKE UP!        │
                                        │ Signal received │
                                        │ Do enrichment   │
                                        └─────────────────┘
```

---

### File: `src/catalog/enrichment/catalog_enrichment.py`

**Before (Polling):**

```python
async def run_enrichment_cron(session):
    while True:
        await _check_and_enrich_catalog(session)
        await asyncio.sleep(120)  # Sleep 2 minutes, then check again
        #     ↑ WASTEFUL! Checking even when nothing changed
```

**After (Event-Driven):**

```python
async def run_enrichment_cron(session):
    # Set up the bridge
    bridge = get_enrichment_bridge()
    bridge.set_target_loop(asyncio.get_running_loop())

    while True:
        # Wait for signal (with 60s timeout for shutdown checks)
        signaled = await bridge.wait_for_signal(timeout=60.0)
        #                 ↑
        #    Sleeps here using ZERO CPU until signal arrives!

        if signaled:
            # Worker told us schema changed - do enrichment NOW
            await _check_and_enrich_catalog(session)
```

---

## Enhancement 3: Parallel Execution

### File: `src/catalog/jobs/executor.py`

**The concept:**

```
SEQUENTIAL (Before):          PARALLEL (After):
────────────────────          ─────────────────

Task 1 ████████               Task 1 ████████
       ↓                      Task 2 ████████
Task 2 ████████               Task 3 ████████
       ↓                      Task 4 ████████
Task 3 ████████               Task 5 ████████
       ↓
Task 4 ████████               Time: 1 unit
       ↓
Task 5 ████████

Time: 5 units
```

**The code:**

```python
@dataclass
class InferenceTask:
    """What collection to run INFER on."""
    bucket: str
    scope: str
    collection: str


@dataclass
class InferenceResult:
    """Result of running INFER."""
    bucket: str
    scope: str
    collection: str
    schema: SchemaCollection | None  # The schema we found
    indexes: list                     # Index definitions
    document_count: int
    error: Exception | None = None    # If something went wrong

    @property
    def success(self) -> bool:
        return self.error is None and self.schema is not None


class ParallelInferenceExecutor:
    """Run multiple INFER queries at the same time."""

    def __init__(self, cluster, concurrency=5):
        self._cluster = cluster
        self._semaphore = asyncio.Semaphore(concurrency)
        #               ↑
        #    "Only allow 5 concurrent operations"
        #    Like a bouncer at a club

    async def execute_batch(self, tasks: list[InferenceTask]) -> list[InferenceResult]:
        """Run INFER on all tasks in parallel."""

        # Create a coroutine for each task
        coroutines = [
            self._infer_with_semaphore(task)
            for task in tasks
        ]

        # Run all at once (but semaphore limits to 5 concurrent)
        results = await asyncio.gather(*coroutines)
        #              ↑
        #    "Run all these coroutines and wait for ALL to finish"

        return results

    async def _infer_with_semaphore(self, task):
        """Run single INFER, but respect the concurrency limit."""

        async with self._semaphore:  # "Acquire a slot" (blocks if 5 already running)
            #     ↑
            #     Waits here if 5 tasks are already running
            #     When one finishes, this one starts

            return await self._execute_inference(task)

        # When we exit this block, we "release the slot"
```

**Visual of Semaphore:**

```
Semaphore(5) = 5 slots available

Time 0:  [Task1] [Task2] [Task3] [Task4] [Task5]  ← All 5 slots used
         Task6, Task7, Task8 waiting...

Time 1:  [Task6] [Task2] [Task3] [Task4] [Task5]  ← Task1 done, Task6 starts
         Task7, Task8 waiting...

Time 2:  [Task6] [Task7] [Task3] [Task4] [Task5]  ← Task2 done, Task7 starts
         Task8 waiting...

...and so on
```

---

### File: `src/catalog/worker.py` (Modified)

How the worker uses parallel execution:

```python
async def _collect_buckets_scopes_collections(cluster, existing_database_info, incremental=True):

    # PHASE 1: Discovery - Find what needs refresh
    collections_to_refresh = []      # Tasks to run
    collections_skipped_data = {}    # Cached data to reuse

    for bucket in all_buckets:
        for scope in bucket.scopes:
            for collection in scope.collections:
                path = f"{bucket.name}/{scope.name}/{collection.name}"

                # Get current document count
                current_doc_count = await _get_document_count(...)

                # Check if we need to refresh
                stored_metadata = store.get_collection_metadata(path)

                if not _needs_refresh(stored_metadata, current_doc_count):
                    # SKIP - use cached data
                    collections_skipped_data[path] = existing_data
                else:
                    # REFRESH - add to task list
                    collections_to_refresh.append(
                        InferenceTask(bucket=..., scope=..., collection=...)
                    )

    # PHASE 2: Parallel Execution
    if collections_to_refresh:
        executor = ParallelInferenceExecutor(cluster, concurrency=5)
        results = await executor.execute_batch(collections_to_refresh)
        #        ↑
        #   Runs all INFER queries in parallel (max 5 at a time)

        # Process results
        for result in results:
            if result.success:
                inference_results[result.path] = {
                    "schema": result.schema.to_dict(),
                    "indexes": result.indexes,
                }
                # Update metadata for next time
                store.set_collection_metadata(CollectionMetadata(...))

    # PHASE 3: Build final result
    # Combine refreshed data + skipped data
    ...
```

**Visual of the 3 phases:**

```
┌─────────────────────────────────────────────────────────────────┐
│ PHASE 1: DISCOVERY                                               │
│                                                                  │
│   Collection 1 → Check → No change → SKIP (use cache)           │
│   Collection 2 → Check → No change → SKIP (use cache)           │
│   Collection 3 → Check → CHANGED!  → ADD TO REFRESH LIST        │
│   Collection 4 → Check → No change → SKIP (use cache)           │
│   Collection 5 → Check → NEW!      → ADD TO REFRESH LIST        │
│                                                                  │
│   Result: [Task3, Task5] need refresh                            │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ PHASE 2: PARALLEL EXECUTION                                      │
│                                                                  │
│   ParallelInferenceExecutor.execute_batch([Task3, Task5])       │
│                                                                  │
│   Task3 ████████  }  Running in parallel                        │
│   Task5 ████████  }                                              │
│                                                                  │
│   Results: [Result3, Result5]                                    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ PHASE 3: BUILD RESULT                                            │
│                                                                  │
│   Final data = {                                                 │
│     Collection 1: cached_data,     (from skip)                   │
│     Collection 2: cached_data,     (from skip)                   │
│     Collection 3: fresh_data,      (from refresh)                │
│     Collection 4: cached_data,     (from skip)                   │
│     Collection 5: fresh_data,      (from refresh)                │
│   }                                                              │
└─────────────────────────────────────────────────────────────────┘
```

---

## Enhancement 4: Job Queue

### File: `src/catalog/jobs/queue.py`

```python
class JobPriority(Enum):
    """Priority levels - lower number = higher priority."""
    HIGH = 1      # User requested "refresh now!"
    NORMAL = 2    # Regular scheduled refresh
    LOW = 3       # Background discovery


class JobStatus(Enum):
    """What state is the job in?"""
    PENDING = "pending"          # Waiting in queue
    IN_PROGRESS = "in_progress"  # Currently running
    COMPLETED = "completed"      # Done successfully
    FAILED = "failed"            # Failed after all retries
    RETRYING = "retrying"        # Failed, will retry


@dataclass(order=True)  # Makes jobs sortable by priority
class InferenceJob:
    """A job to run INFER on a collection."""

    # These fields are used for sorting (priority queue)
    priority: JobPriority
    created_at: datetime

    # Job details
    bucket: str
    scope: str
    collection: str
    status: JobStatus = JobStatus.PENDING
    retry_count: int = 0
    max_retries: int = 3  # Try up to 3 times if fails
```

**Priority Queue Visual:**

```
┌─────────────────────────────────────────┐
│            JOB QUEUE                     │
│                                          │
│  ┌──────────────────────────────────┐   │
│  │ Priority: HIGH                    │   │  ← Processed first!
│  │ Collection: users                 │   │
│  │ (User requested refresh)          │   │
│  └──────────────────────────────────┘   │
│                  ↓                       │
│  ┌──────────────────────────────────┐   │
│  │ Priority: NORMAL                  │   │  ← Processed second
│  │ Collection: orders                │   │
│  │ (Scheduled refresh)               │   │
│  └──────────────────────────────────┘   │
│                  ↓                       │
│  ┌──────────────────────────────────┐   │
│  │ Priority: NORMAL                  │   │  ← Processed third
│  │ Collection: products              │   │
│  │ (Scheduled refresh)               │   │
│  └──────────────────────────────────┘   │
│                                          │
└─────────────────────────────────────────┘
```

**The Queue Class:**

```python
class InferenceJobQueue:
    """Async priority queue for inference jobs."""

    def __init__(self):
        self._queue = asyncio.PriorityQueue()  # Sorts by priority automatically
        self._in_progress = {}                  # Jobs currently running
        self._pending_paths = set()             # Paths in queue (no duplicates)

    async def enqueue(self, job: InferenceJob) -> bool:
        """Add a job to the queue."""

        path = job.path

        # Prevent duplicates
        if path in self._in_progress or path in self._pending_paths:
            return False  # Already queued or running

        self._pending_paths.add(path)
        await self._queue.put(job)
        return True

    async def dequeue(self, timeout=None) -> InferenceJob | None:
        """Get the next job (highest priority first)."""

        job = await self._queue.get()  # Blocks until job available

        self._pending_paths.remove(job.path)
        self._in_progress[job.path] = job
        job.status = JobStatus.IN_PROGRESS

        return job

    async def complete(self, job: InferenceJob, success: bool, error=None):
        """Mark job as done."""

        del self._in_progress[job.path]

        if success:
            job.status = JobStatus.COMPLETED
        else:
            # Failed - should we retry?
            if job.retry_count < job.max_retries:
                job.retry_count += 1
                job.status = JobStatus.RETRYING
                await self._queue.put(job)  # Back in queue!
            else:
                job.status = JobStatus.FAILED  # Gave up
```

**Retry Flow:**

```
Job fails (attempt 1)
        │
        ▼
retry_count < 3? ──Yes──► Re-queue job (retry_count = 1)
        │                        │
        No                       ▼
        │                 Job fails (attempt 2)
        ▼                        │
  FAILED (give up)               ▼
                          retry_count < 3? ──Yes──► Re-queue job (retry_count = 2)
                                │                        │
                                No                       ▼
                                │                 Job fails (attempt 3)
                                ▼                        │
                          FAILED (give up)               ▼
                                                  retry_count < 3? ──No──► FAILED
```

---

## New MCP Tools

### File: `src/tools/catalog.py`

```python
def get_catalog_status(ctx: Context) -> dict:
    """
    Tool: "How's the catalog doing?"

    Returns stats like:
    - How many collections indexed
    - When was last refresh
    - Is enrichment needed
    """
    store = get_catalog_store()
    database_info = store.get_database_info()

    # Count collections
    collection_count = 0
    for bucket in database_info.get("buckets", {}).values():
        for scope in bucket.get("scopes", {}).values():
            collection_count += len(scope.get("collections", {}))

    return {
        "status": "active",
        "statistics": {
            "collections_indexed": collection_count,
        },
        "enrichment": {
            "needs_enrichment": store.get_needs_enrichment(),
        },
        "last_full_refresh": store.get_last_full_refresh(),
    }


def get_collection_schema_from_catalog(ctx, bucket_name, scope_name, collection_name):
    """
    Tool: "Give me the cached schema for this collection"

    Fast! Reads from cache instead of running INFER.
    """
    store = get_catalog_store()
    database_info = store.get_database_info()

    # Navigate to the collection
    collection_data = (
        database_info
        .get("buckets", {})
        .get(bucket_name, {})
        .get("scopes", {})
        .get(scope_name, {})
        .get("collections", {})
        .get(collection_name)
    )

    if collection_data:
        return {"status": "success", "schema": collection_data["schema"]}
    else:
        return {"status": "not_found"}


def refresh_collection_schema(ctx, bucket_name, scope_name, collection_name):
    """
    Tool: "Refresh this collection's schema RIGHT NOW"

    Creates a HIGH priority job.
    """
    job = InferenceJob(
        priority=JobPriority.HIGH,  # Skip the line!
        bucket=bucket_name,
        scope=scope_name,
        collection=collection_name,
    )

    queue = _get_job_queue()
    asyncio.create_task(queue.enqueue(job))

    return {"status": "queued", "priority": "HIGH"}


def get_enriched_database_context(ctx):
    """
    Tool: "Give me the AI-friendly database description"

    Returns the enriched prompt generated by LLM.
    """
    store = get_catalog_store()
    prompt = store.get_prompt()

    if prompt:
        return {"status": "available", "enriched_prompt": prompt}
    else:
        return {"status": "not_available"}
```

---

## Complete Data Flow

```
┌──────────────────────────────────────────────────────────────────────────┐
│                              MCP SERVER                                   │
│                                                                           │
│  ┌─────────────────────────────────────────────────────────────────────┐ │
│  │                         MAIN THREAD                                  │ │
│  │                                                                      │ │
│  │   AI Request ──► Tool Handler ──► Response                          │ │
│  │                       │                                              │ │
│  │                       ├── get_catalog_status()                       │ │
│  │                       ├── get_collection_schema_from_catalog()       │ │
│  │                       ├── refresh_collection_schema()                │ │
│  │                       └── get_enriched_database_context()            │ │
│  │                                                                      │ │
│  │   ┌────────────────────────────────────────────────────────────┐    │ │
│  │   │ ENRICHMENT TASK                                             │    │ │
│  │   │                                                             │    │ │
│  │   │   bridge.set_target_loop(loop)                              │    │ │
│  │   │        │                                                    │    │ │
│  │   │        ▼                                                    │    │ │
│  │   │   await bridge.wait_for_signal() ◄──── Signal from worker   │    │ │
│  │   │        │                                                    │    │ │
│  │   │        ▼                                                    │    │ │
│  │   │   _check_and_enrich_catalog()                               │    │ │
│  │   │        │                                                    │    │ │
│  │   │        ▼                                                    │    │ │
│  │   │   store.add_prompt(enriched_text)                           │    │ │
│  │   └────────────────────────────────────────────────────────────┘    │ │
│  └─────────────────────────────────────────────────────────────────────┘ │
│                                    ▲                                      │
│                                    │ ThreadToAsyncBridge                  │
│                                    │ (signal)                             │
│  ┌─────────────────────────────────┼───────────────────────────────────┐ │
│  │                    WORKER THREAD │                                   │ │
│  │                                                                      │ │
│  │   Every 5 minutes:                                                   │ │
│  │                                                                      │ │
│  │   ┌─────────────────────────────────────────────────────────────┐   │ │
│  │   │ PHASE 1: Discovery                                           │   │ │
│  │   │                                                              │   │ │
│  │   │   For each collection:                                       │   │ │
│  │   │     metadata = store.get_collection_metadata(path)           │   │ │
│  │   │     doc_count = await _get_document_count()                  │   │ │
│  │   │                                                              │   │ │
│  │   │     if _needs_refresh(metadata, doc_count):                  │   │ │
│  │   │       collections_to_refresh.append(task)                    │   │ │
│  │   │     else:                                                    │   │ │
│  │   │       collections_skipped.append(cached_data)                │   │ │
│  │   └─────────────────────────────────────────────────────────────┘   │ │
│  │                          │                                           │ │
│  │                          ▼                                           │ │
│  │   ┌─────────────────────────────────────────────────────────────┐   │ │
│  │   │ PHASE 2: Parallel Execution                                  │   │ │
│  │   │                                                              │   │ │
│  │   │   executor = ParallelInferenceExecutor(concurrency=5)        │   │ │
│  │   │   results = await executor.execute_batch(collections)        │   │ │
│  │   │                                                              │   │ │
│  │   │   ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐    │   │ │
│  │   │   │ INFER  │ │ INFER  │ │ INFER  │ │ INFER  │ │ INFER  │    │   │ │
│  │   │   │ Coll 1 │ │ Coll 2 │ │ Coll 3 │ │ Coll 4 │ │ Coll 5 │    │   │ │
│  │   │   └────────┘ └────────┘ └────────┘ └────────┘ └────────┘    │   │ │
│  │   │        All running in parallel (max 5 at a time)             │   │ │
│  │   └─────────────────────────────────────────────────────────────┘   │ │
│  │                          │                                           │ │
│  │                          ▼                                           │ │
│  │   ┌─────────────────────────────────────────────────────────────┐   │ │
│  │   │ PHASE 3: Update Store                                        │   │ │
│  │   │                                                              │   │ │
│  │   │   for result in results:                                     │   │ │
│  │   │     store.set_collection_metadata(metadata)                  │   │ │
│  │   │                                                              │   │ │
│  │   │   store.add_database_info(combined_data)                     │   │ │
│  │   │                                                              │   │ │
│  │   │   if schema_changed:                                         │   │ │
│  │   │     store.set_needs_enrichment(True)                         │   │ │
│  │   │     bridge.signal_from_thread() ────────────────────────►    │   │ │
│  │   └─────────────────────────────────────────────────────────────┘   │ │
│  └─────────────────────────────────────────────────────────────────────┘ │
│                                    │                                      │
│                                    ▼                                      │
│                        ┌───────────────────────┐                          │
│                        │        STORE          │                          │
│                        │                       │                          │
│                        │ • database_info       │                          │
│                        │ • collection_metadata │                          │
│                        │ • enriched_prompt     │                          │
│                        │ • schema_hash         │                          │
│                        │ • needs_enrichment    │                          │
│                        │                       │                          │
│                        │ Persisted to:         │                          │
│                        │ ~/.couchbase_mcp/     │                          │
│                        │   catalog_state.json  │                          │
│                        └───────────────────────┘                          │
└──────────────────────────────────────────────────────────────────────────┘
```
