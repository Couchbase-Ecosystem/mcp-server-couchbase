# MCP Server Couchbase Architecture

## Part 1: What is MCP?

**MCP = Model Context Protocol**

Think of it like this:

```
┌─────────────────┐         ┌─────────────────┐         ┌─────────────────┐
│                 │         │                 │         │                 │
│   AI Assistant  │ ◄─────► │   MCP Server    │ ◄─────► │    Couchbase    │
│   (e.g. Claude) │         │   (Our Code)    │         │    Database     │
│                 │         │                 │         │                 │
└─────────────────┘         └─────────────────┘         └─────────────────┘
     "Hey, show me              Translates               Actual data
      all users"                requests                 lives here
```

In simple words:
- AI assistants (like Claude) don't know how to talk to databases directly
- Our MCP Server acts as a translator/middleman
- AI says "show me users" → MCP Server runs the actual SQL query → returns results to AI

---

## Part 2: What are "Tools" in MCP?

Tools are like functions the AI can call. Think of them as buttons:

```
┌──────────────────────────────────────────────────────┐
│                    MCP Server                         │
│                                                       │
│   [get_buckets]  [run_query]  [get_document]         │
│                                                       │
│   [list_indexes] [get_schema] [delete_document]      │
│                                                       │
│   When AI needs data, it "presses" one of these      │
└──────────────────────────────────────────────────────┘
```

**Example flow:**

1. User asks AI: "What buckets do I have?"
2. AI thinks: "I need to call get_buckets tool"
3. MCP Server runs the actual Couchbase query
4. Returns: `["travel-sample", "beer-sample"]`
5. AI responds: "You have 2 buckets: travel-sample and beer-sample"

---

## Part 3: What is the Catalog?

The Catalog is like a cache/memory of your database structure.

**Without Catalog:**
Every time AI asks about schema:
- → Run INFER query (slow, 5-10 seconds)
- → Wait...
- → Get result

**With Catalog:**
Catalog already has the schema saved:
- → Just read from memory (instant!)

**What the Catalog stores:**
- All your buckets, scopes, collections
- Schema (what fields each collection has)
- Indexes (which queries are optimized)

---

## Part 4: The Catalog Architecture (Before Your Changes)

```
┌─────────────────────────────────────────────────────────────┐
│                        MCP SERVER                            │
│                                                              │
│  ┌────────────────────┐      ┌────────────────────────────┐ │
│  │  Main Thread       │      │  Background Worker Thread  │ │
│  │  (Handles AI       │      │  (Runs every 5 minutes)    │ │
│  │   requests)        │      │                            │ │
│  │                    │      │  1. Connect to Couchbase   │ │
│  │  Tools like:       │      │  2. Get ALL collections    │ │
│  │  - run_query       │      │  3. Run INFER on EACH one  │ │
│  │  - get_document    │      │  4. Save to Store          │ │
│  └────────────────────┘      └────────────────────────────┘ │
│              │                           │                   │
│              │         ┌─────────────────┘                   │
│              ▼         ▼                                     │
│        ┌──────────────────────┐                              │
│        │       STORE          │  ← Shared memory             │
│        │  (Schema cache)      │     (like a global variable) │
│        │  - database_info     │                              │
│        │  - enriched_prompt   │                              │
│        └──────────────────────┘                              │
└─────────────────────────────────────────────────────────────┘
```

**Problem with this approach:**
- Worker refreshes ALL collections every 5 min (wasteful)
- Enrichment checks every 2 min if anything changed (polling = wasteful)
- Collections processed one-by-one (slow)

---

## Part 5: Your Enhancements (What You Did!)

### Enhancement 1: Incremental Updates

**Before:** Refresh ALL 100 collections every 5 minutes
**After:** Only refresh collections that actually changed

```
BEFORE:                              AFTER:
─────────────────────────────       ─────────────────────────────
Collection 1 → INFER (5 sec)        Collection 1 → Skip (no change)
Collection 2 → INFER (5 sec)        Collection 2 → Skip (no change)
Collection 3 → INFER (5 sec)        Collection 3 → INFER (changed!)
Collection 4 → INFER (5 sec)        Collection 4 → Skip (no change)
...                                 ...
Total: 500 seconds                  Total: 5 seconds
```

**How do we know if it changed?**
- We store document count for each collection
- If count changed by 10% or more → refresh
- If older than 1 hour → refresh anyway

---

### Enhancement 2: Event-Driven (No More Polling)

**Before (Polling):**
```
Enrichment system:
  Loop forever:
    Sleep 2 minutes
    Check: "Did schema change?"
    If yes → do enrichment
    If no → sleep again
This is wasteful - checking every 2 min even if nothing changed!
```

**After (Event-Driven):**
```
Enrichment system:
  Wait for signal...
  (sleeping, using no CPU)

Worker: *detects schema change*
Worker: "Hey Enrichment! Wake up!"

Enrichment: *wakes up immediately*
Enrichment: "Got it, processing now!"
```

**Simple analogy:**
- Polling = Checking your mailbox every 2 minutes
- Event-Driven = Doorbell rings when mail arrives

---

### Enhancement 3: Parallel Execution

**Before (Sequential):**
```
Time →
┌──────────┬──────────┬──────────┬──────────┬──────────┐
│ Coll 1   │ Coll 2   │ Coll 3   │ Coll 4   │ Coll 5   │
│ 5 sec    │ 5 sec    │ 5 sec    │ 5 sec    │ 5 sec    │
└──────────┴──────────┴──────────┴──────────┴──────────┘
                    Total: 25 seconds
```

**After (Parallel with limit of 5):**
```
Time →
┌──────────┐
│ Coll 1   │
├──────────┤
│ Coll 2   │  All 5 running
├──────────┤  at the same time!
│ Coll 3   │
├──────────┤
│ Coll 4   │
├──────────┤
│ Coll 5   │
└──────────┘
Total: 5 seconds (5x faster!)
```

**Why limit to 5?**
- Running 100 queries at once would crash the database
- 5 is a safe number that's fast but doesn't overload

---

### Enhancement 4: Job Queue

**Before:** Tight coupling - worker directly runs INFER
**After:** Worker creates "jobs", separate executor processes them

```
┌─────────────┐     ┌─────────────────┐     ┌─────────────┐
│   WORKER    │ ──► │   JOB QUEUE     │ ──► │  EXECUTOR   │
│             │     │                 │     │             │
│ "Collection │     │ ┌─────────────┐ │     │ Processes   │
│  3 changed" │     │ │ Job: Coll 3 │ │     │ jobs in     │
│             │     │ │ Priority: H │ │     │ parallel    │
└─────────────┘     │ └─────────────┘ │     └─────────────┘
                    │ ┌─────────────┐ │
                    │ │ Job: Coll 7 │ │
                    │ │ Priority: N │ │
                    │ └─────────────┘ │
                    └─────────────────┘
```

**Benefits:**
- **Priorities:** User requests (HIGH) go before scheduled (NORMAL)
- **Retries:** If a job fails, it automatically retries (up to 3 times)
- **No duplicates:** Won't queue same collection twice

---

## Part 6: New Tools You Added

4 new buttons for the AI to press:

| Tool | What it does |
|------|--------------|
| `get_catalog_status` | "How's the catalog doing?" - shows stats |
| `get_collection_schema_from_catalog` | Get schema from cache (fast!) |
| `refresh_collection_schema` | "Refresh this collection NOW" (urgent) |
| `get_enriched_database_context` | Get AI-friendly database description |

---

## Part 7: Complete Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                           MCP SERVER                                 │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │                    MAIN THREAD                                │   │
│  │                                                               │   │
│  │   AI Request → Tool Handler → Response                        │   │
│  │                                                               │   │
│  │   ┌─────────────────────────────────────────────────────┐    │   │
│  │   │ ENRICHMENT (waits for signal, no polling)           │    │   │
│  │   │                                                      │    │   │
│  │   │   Waiting... ──► Signal! ──► Generate AI description │    │   │
│  │   └─────────────────────────────────────────────────────┘    │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                              ▲                                       │
│                              │ Signal (instant!)                     │
│                              │                                       │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │                 BACKGROUND WORKER THREAD                      │   │
│  │                                                               │   │
│  │   Every 5 min:                                                │   │
│  │   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐      │   │
│  │   │ DISCOVERY   │ ─► │ JOB QUEUE   │ ─► │ PARALLEL    │      │   │
│  │   │             │    │             │    │ EXECUTOR    │      │   │
│  │   │ "Which      │    │ Only queues │    │             │      │   │
│  │   │ collections │    │ changed     │    │ Runs 5 at   │      │   │
│  │   │ changed?"   │    │ collections │    │ a time      │      │   │
│  │   └─────────────┘    └─────────────┘    └─────────────┘      │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                              │                                       │
│                              ▼                                       │
│                    ┌──────────────────┐                              │
│                    │      STORE       │                              │
│                    │   (Shared Cache) │                              │
│                    │                  │                              │
│                    │ • database_info  │                              │
│                    │ • metadata       │                              │
│                    │ • enriched text  │                              │
│                    └──────────────────┘                              │
└─────────────────────────────────────────────────────────────────────┘
```
