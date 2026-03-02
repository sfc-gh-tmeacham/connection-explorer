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

### The `default` parameter is ignored when a key exists in session state

With key-based identity (v1.50.0+), if a widget's key already exists in `st.session_state`, the `default` parameter is silently ignored. The widget always uses the session state value.

```python
# If st.session_state["my_key"] == [], this default is ignored:
st.multiselect("Label", options, default=["A", "B"], key="my_key")
# Widget will show [] not ["A", "B"]
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

### Dual-key pattern for widget persistence across pages

Use two keys per filter to survive page navigation:

- `persist_filter_<name>`: Stores the intended value, never tied to a widget
- `widget_filter_<name>`: The actual widget key, may be absent on other pages

On the page with the widget:
```python
persisted = st.session_state.get(persist_key, [])
default_vals = [v for v in persisted if v in options]
values[name] = st.sidebar.multiselect(label, options, default=default_vals, key=widget_key)
st.session_state[persist_key] = values[name]
```

When programmatically updating filters (e.g., from a click callback), set **both** keys:
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
