# Lessons Learned

Key insights from building the Data Lake Explorer Streamlit application.

---

## Streamlit Widget State Management

### Deleting widget keys does NOT reset widgets

When a widget has a `key` parameter, Streamlit tracks its identity via that key. **Deleting the key from `st.session_state` does not reset the widget** because the frontend component persists across reruns and Streamlit restores the value from the frontend.

```python
# WRONG: Widget will retain its previous frontend value
del st.session_state["widget_filter_client"]

# CORRECT: Set the key to the desired value
st.session_state["widget_filter_client"] = ["Fivetran"]
```

Reference: [Widget behavior docs](https://docs.streamlit.io/develop/concepts/architecture/widget-behavior)

### The `default` parameter conflicts when a key exists in session state

With key-based identity, if a widget's key already exists in `st.session_state`, passing `default=` raises `StreamlitAPIException: "widget was created with a default value but also had its value set via the Session State API"`. This commonly happens when a callback (e.g., click-to-filter) pre-sets the widget key before the widget renders. The fix is to conditionally omit `default`:

```python
ms_kwargs = dict(key=widget_key, on_change=callback, args=(...))
if widget_key not in st.session_state:
    ms_kwargs["default"] = default_vals
st.sidebar.multiselect(label, options, **ms_kwargs)
```

### Widget keys cannot be set after the widget is instantiated

Attempting to write to `st.session_state[widget_key]` **after** the corresponding widget has been rendered in the current script run raises `StreamlitAPIException`. Widget keys can only be modified:

- Before the widget command runs in the script
- In a callback function (callbacks execute before the script reruns)

---

## Streamlit v2 BidiComponent (Custom Components)

### Callback execution order is the key to everything

Streamlit's order of operations on rerun:

1. Widget values in `st.session_state` are updated
2. **Callback functions execute** (before the script body)
3. The page script reruns top-to-bottom

This means `on_<key>_change` callbacks are the correct place to modify widget state, since they run before any widget is instantiated.

### Use `_ts: Date.now()` to guarantee unique trigger values

`setTriggerValue` may not fire the `on_change` callback if the value is structurally identical to the previous one. Including a timestamp ensures every click produces a unique value:

```javascript
setTriggerValue('selected_node', {
    nodeId: nodeId,
    nodeType: nodeType,
    _ts: Date.now()  // Guarantees uniqueness
});
```

On the Python side, use the timestamp for deduplication to avoid reprocessing stale triggers:

```python
ts = info.get("_ts", 0)
last_ts = st.session_state.get("_last_click_ts", 0)
if ts <= last_ts:
    return  # Already processed
st.session_state["_last_click_ts"] = ts
```

### `setTriggerValue` vs `setStateValue`

| | `setTriggerValue` | `setStateValue` |
|---|---|---|
| Persistence | Consumed after one rerun (resets to null) | Persists across reruns |
| Use case | One-time events (clicks, submissions) | Persistent state (selections, toggles) |
| Callback | `on_<key>_change` fires on each new trigger | `on_<key>_change` fires when value changes |

For click-to-filter, `setTriggerValue` with `_ts` is the right choice: each click is a one-time event that should be processed once.

### Reading trigger values: result object vs callback

The official Streamlit docs show two patterns:

```python
# Pattern 1: Read from result (post-mount)
result = my_component(on_clicked_change=lambda: None)
if result.clicked:
    process(result.clicked)

# Pattern 2: Callback (pre-script)
def on_click():
    state = st.session_state["my_component"]
    process(state.clicked)
result = my_component(on_clicked_change=on_click)
```

**Pattern 1** is simpler but cannot modify widget state (widgets already rendered). **Pattern 2** is required when you need to update sidebar filters or other widgets based on the trigger, since callbacks run before widgets are instantiated.

---

## Streamlit `st.rerun()` Pitfalls

### Avoid `st.rerun()` when callbacks suffice

`st.rerun()` adds complexity and can cause subtle timing issues. If you can accomplish the state update in a callback (which runs before the script), you don't need `st.rerun()` at all. The script will naturally render with the updated state.

### `st.rerun()` does not guarantee widget key deletion takes effect

Even if you delete a widget key and call `st.rerun()`, the frontend widget may restore its value on the next run. This is because widget identity (with a key) is tied to the frontend component, not the Python session state.

---

## Architecture Patterns

### Never pre-set a widget key via the Session State API in the script body

Setting `st.session_state[widget_key] = value` in the script body before calling
the widget (e.g., `st.multiselect(..., key=widget_key)`) causes a conflict on
the very first user interaction. Streamlit warns:

> "The widget with key 'X' was created with a default value but also had its
> value set via the Session State API."

Even without `default`, pre-setting the widget key in the script body and then
calling the widget causes the first interaction to be silently discarded.
Conditional guards (`if key not in st.session_state`) don't help because after
the first render the key always exists.

The **only** safe places to write to a widget key are:
1. **In a callback** (`on_change` or BidiComponent `on_<key>_change`) — callbacks
   run before widgets are instantiated.
2. **Never in the script body** before the widget command.

### Dual-key pattern for widget persistence across pages

Use two keys per filter to survive page navigation:

- `persist_filter_<name>`: Stores the intended value, never tied to a widget
- `widget_filter_<name>`: The actual widget key, only written by the widget itself

The correct pattern uses `default=` sourced from the persist key, plus an
`on_change` callback to sync the widget value back to the persist key:

```python
def _sync_filter(persist_key, widget_key):
    st.session_state[persist_key] = st.session_state[widget_key]

persisted = st.session_state.get(persist_key, [])
default_vals = [v for v in persisted if v in options]

values[name] = st.sidebar.multiselect(
    label, options, default=default_vals,
    key=widget_key,
    on_change=_sync_filter,
    args=(persist_key, widget_key),
)
st.session_state[persist_key] = values[name]
```

When programmatically updating filters (e.g., from a click callback that runs
before widget instantiation), set **both** keys:
```python
st.session_state[persist_key] = new_val
st.session_state[widget_key] = new_val
```

### Node type mapping must be explicit

When building a network graph with multiple node types, every node dict must include a `nodeType` field. The JS click handler uses this to tell Python what kind of node was clicked. Missing `nodeType` results in `'unknown'` which doesn't match any filter key.

```python
# Every node type needs explicit nodeType
nodes.append({
    "id": warehouse, "label": warehouse,
    "nodeType": "warehouse",  # Required for click-to-filter
    ...
})
```

### vis.js network: nodeTypeMap for click handler

Pass a `nodeTypeMap` dict from Python to JS so the click handler can look up any node's type without searching the nodes array:

```python
node_type_map = {n["id"]: n.get("nodeType", "unknown") for n in nodes}
component_data = {"nodes": nodes, "edges": edges, "nodeTypeMap": node_type_map}
```

```javascript
const nodeType = nodeTypeMap[nodeId] || 'unknown';
```

---

## Streamlit in Snowflake (SiS) Considerations

### Sample data fallback for local development

When running locally without a Snowflake session, load sample CSV data so the app is functional for development and testing:

```python
try:
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()
except Exception:
    session = None
```

### `snowflake.yml` must list all files

Every file referenced by the app (Python modules, SVG icons, CSS) must be listed in `snowflake.yml` under the stage artifacts. Missing files will cause import errors or broken assets when deployed.

---

## SVG Icon Sourcing for Brand Logos

### Source priority for open-source brand SVGs

1. **Simple Icons** (`simpleicons.org`) - Best source. Monochrome, single-path, 24x24 viewBox. Perfect for rendering as white silhouettes. Check both `master` and `develop` branches on GitHub, and newer npm versions (`npm pack simple-icons@latest`) which may have icons not yet on either branch.
2. **Gilbarbara/logos** - Good fallback. Has square `-icon` suffix variants (e.g., `amplitude-icon.svg` at 256x256) that work well in circular frames. Avoid base names which are often wide wordmark logos (e.g., 512x107) that look terrible in circles.
3. **Simple Icons CDN** (`cdn.simpleicons.org`) - Unreliable; HEAD requests may fail. Use GitHub raw URLs instead.

### Gilbarbara wordmark vs icon variants

Gilbarbara logos come in two forms: base (often wide wordmarks) and `-icon` suffix (square). Always check for the `-icon` variant first. Example: `amplitude.svg` is 512x107 (wordmark), `amplitude-icon.svg` is 256x256 (square logo mark). Some `-icon` variants exist but have zero `<path>` elements (e.g., `heap-icon.svg` uses only `<rect>` and `<circle>`), making them unusable for path-based renderers.

### Most niche data tools have no open-source SVG icons

Of 257 classified tools, only ~104 have brand SVG icons from open-source repos. The remaining ~150 are niche ETL, BI, or enterprise tools (Wherescape, Alooma, Attunity, Panoply, etc.) that don't appear in any major icon repository. The letter-abbreviation fallback is essential for coverage.

### `IDENTIFIER()` does not support string concatenation for schema creation

`CREATE SCHEMA IF NOT EXISTS IDENTIFIER($DB_NAME || '.APP')` fails because `IDENTIFIER()` expects a single object name, not a dot-qualified path built via concatenation. The fix is to `USE DATABASE IDENTIFIER($DB_NAME)` first, then `CREATE SCHEMA IF NOT EXISTS APP` without `IDENTIFIER()`.

### `GRANT USAGE ON STREAMLIT` requires the app to exist first

You cannot grant access to a Streamlit app in the same SQL setup script that runs before `snow streamlit deploy`. The Streamlit object is created by the deploy command, not by SQL. Move the `GRANT USAGE ON STREAMLIT` to the deploy script, after `snow streamlit deploy` succeeds.

---

## Adding a New Node Type to a vis.js Network Graph

### Use composite IDs to avoid node collisions across parents

When the same name can appear under multiple parents (e.g., "PUBLIC" schema exists in multiple databases), the node ID must encode the parent context. Use `"DB.SCHEMA"` as the node ID while displaying just the schema name as the label:

```python
schema_id = f"{database}.{schema_name}"
nodes.append({"id": schema_id, "label": schema_name, ...})
```

### Chain-based edge topology scales better than hardcoded branches

With N hideable node types, hardcoded if/elif branches for edge creation grow combinatorially. A generic chain approach builds an ordered list of visible node IDs per row and creates edges between consecutive pairs:

```python
chain = []
if not hide_clients: chain.append(client)
if not hide_warehouses: chain.append(warehouse)
if not hide_databases: chain.append(database)
if not hide_schemas and schema_id: chain.append(schema_id)

for i in range(len(chain) - 1):
    _add_edge(chain[i], chain[i + 1], ...)
```

This handles any combination of hidden node types with zero branching logic.

### Distinguish SVG polygon shapes by point count

Both hexagons and diamonds use `<polygon>` SVG elements. In JavaScript, distinguish them by counting the space-separated coordinate pairs in the `points` attribute: 6 points = hexagon, 4 points = diamond.

```javascript
var pts = svg.match(/points="([^"]+)"/);
if (pts && pts[1].split(/\s+/).length >= 6) return 'schema';  // hexagon
return 'warehouse';  // diamond
```

### `pyvis` is only used for its bundled `vis-network.min.js`

The app imports `pyvis` solely to locate the vis.js library file bundled inside the package. If `pyvis` is missing, the network graph fails at component build time, not at render time. Ensure it's in your local environment even though it's not a direct runtime dependency.

---

## Snowflake SQL Performance

### Split-join pattern for OR conditions in large tables

When joining on multiple columns with OR conditions (e.g., match on `client_app_id` OR `application`), Snowflake's query optimizer may flag it as an "Inefficient join condition" that forces a cartesian product. The fix is to split the single OR-join into separate LEFT JOINs, then combine results:

```sql
-- INEFFICIENT: OR in join forces cartesian evaluation
SELECT ... FROM raw_sessions rs
LEFT JOIN classification c
    ON (c.source_field = 'client_app_id' AND rs.client_app_id ILIKE c.pattern)
    OR (c.source_field = 'application' AND rs.application ILIKE c.pattern)

-- BETTER: Split into two joins, then pick best match
matched_by_app_id AS (
    SELECT rs.*, c.display_name AS dn_app_id, c.priority AS p_app_id
    FROM raw_sessions rs
    LEFT JOIN classification c
        ON c.source_field = 'client_app_id' AND rs.client_app_id ILIKE c.pattern
),
matched_by_application AS (
    SELECT m.*, c.display_name AS dn_app, c.priority AS p_app
    FROM matched_by_app_id m
    LEFT JOIN classification c
        ON c.source_field = 'application' AND m.application ILIKE c.pattern
),
classified AS (
    SELECT ...,
        CASE WHEN COALESCE(p_app_id, 999) <= COALESCE(p_app, 999) 
             THEN dn_app_id ELSE dn_app END AS display_name
    FROM matched_by_application
)
```

This "split-join" approach converts two OR branches into two sequential joins, each with a single equality predicate on `source_field`. The optimizer can now use equality filters efficiently, avoiding the cartesian product. For accounts with high query volumes (millions of rows in `query_history`), this can reduce execution time dramatically.

### Stored procedure `CREATE OR ALTER` deploys to the current database/schema context

`CREATE OR ALTER PROCEDURE` creates the procedure in whatever `DATABASE.SCHEMA` is active in the session — not in the database where a previous version existed. If your Snowflake session context drifts (e.g., Cortex Code defaults to `SNOWFLAKE_INTELLIGENCE.AGENTS`), you'll silently create a duplicate procedure in the wrong schema while the old version keeps running. Always run `USE DATABASE` / `USE SCHEMA` immediately before `CREATE OR ALTER PROCEDURE`.

### `SPLIT_PART` returns empty string, not NULL, for missing parts

`SPLIT_PART(value, '.', 2)` returns `''` (empty string) — not NULL — when the delimiter isn't found. This means naive concatenation like `SPLIT_PART(x, '.', 1) || '.' || SPLIT_PART(x, '.', 2)` produces `'TOM.'` instead of just `'TOM'` when the input is a bare name with no dots.

Guard with a CASE:

```sql
CASE WHEN SPLIT_PART(objectName, '.', 2) != ''
     THEN SPLIT_PART(objectName, '.', 1) || '.' || SPLIT_PART(objectName, '.', 2)
     ELSE NULL
END AS schema_name
```

This commonly affects `access_history.direct_objects_accessed` where `objectName` can be a bare database name (no schema qualifier) for metadata-level access like `SHOW` or `DESCRIBE` commands.

### App-qualified function names in `access_history` are not table references

Snowflake's `access_history.direct_objects_accessed` includes application-qualified function calls (e.g., `BENCH_V2!SPCS_GET_LOGS`, `TB_REC_SERVICE_DEMO_PREDICT!FORWARD`) alongside normal `DB.SCHEMA.TABLE` references. These use `APP_NAME!FUNCTION_NAME` format with an exclamation mark instead of dots.

When you `SPLIT_PART(objectName, '.', 1)` on these, the entire string lands in the database column (since there are no dots), creating bogus nodes in the network graph (e.g., "Schema: BENCH_V2!SPCS_GET_LOGS"). The fix is to exclude them in the WHERE clause:

```sql
AND t.VALUE:objectName::VARCHAR NOT LIKE '%!%'
```

These entries come from SPCS log/event retrieval, performance explorer queries, anomaly insights setup, and other native app function invocations. They represent function calls, not data access, so excluding them is semantically correct.

### `SYSTEM$LOG_INFO` / `SYSTEM$LOG_ERROR` for stored procedure observability

Snowflake SQL stored procedures support `SYSTEM$LOG_INFO()`, `SYSTEM$LOG_ERROR()`, etc. for structured logging to an event table. Key requirements:

1. **Event table**: Must be configured at the account level (`ALTER ACCOUNT SET EVENT_TABLE = '<db.schema.table>'`). Without it, log calls succeed silently but nothing is persisted.
2. **LOG_LEVEL**: Set on the procedure via `ALTER PROCEDURE ... SET LOG_LEVEL = INFO` to control which severity levels are captured.
3. **EXCEPTION block**: Wrap the procedure body in `BEGIN ... EXCEPTION WHEN OTHER THEN ... END` to log errors before re-raising. Use `:SQLCODE` and `:SQLERRM` (with colon prefix) to access error details.
4. **Variable references in SQL**: Inside `SELECT ... INTO :var` and string concatenation, variables declared in `DECLARE` must use the colon prefix (`:row_count`, `:result_msg`).

### `SYSTEM$SET_RETURN_VALUE` is a side-effect function — it cannot run inside a stored procedure

`SYSTEM$SET_RETURN_VALUE` sets the return value visible in `TASK_HISTORY()`. It **must** be called in the task execution context (the task body), not inside a stored procedure invoked by the task. Attempts to call it from within a procedure fail with:

- **Bare statement**: `SYSTEM$SET_RETURN_VALUE(:msg);` → "cannot be executed as a statement in Snowscript"
- **SELECT wrapper**: `SELECT SYSTEM$SET_RETURN_VALUE(:msg);` → "contains a function with side effects"
- **CALL wrapper**: `CALL SYSTEM$SET_RETURN_VALUE(:msg);` → same side-effects error (still inside a procedure context)

The solution is to call the procedure from the task body, capture its return value, and then call `SYSTEM$SET_RETURN_VALUE` at the task level. Use `EXECUTE IMMEDIATE` with `$$` delimiters to embed a multi-statement anonymous block as a single task body:

```sql
CREATE OR ALTER TASK my_task
  SCHEDULE = '...'
AS
  EXECUTE IMMEDIATE
  $$
  DECLARE
    result VARCHAR;
  BEGIN
    CALL my_procedure() INTO :result;
    CALL SYSTEM$SET_RETURN_VALUE(:result);
  END;
  $$;
```

The `EXECUTE IMMEDIATE` + `$$` pattern is necessary because `CREATE TASK ... AS` expects a single SQL statement, and the anonymous `DECLARE`/`BEGIN`/`END` block contains semicolons that confuse the parser without dollar-quote delimiters.
