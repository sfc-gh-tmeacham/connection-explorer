# Data Lake Explorer — Improvements

- [ ] **Local Snowflake Connectivity** — The app can't connect to Snowflake when running locally. It silently falls back to hardcoded sample data. Adding `Session.builder.configs()` support (reading from `~/.snowflake/connections.toml` or env vars) would make local development actually useful.

- [ ] **SQL Data Pipeline Is Fragile** — The stored procedure (`snowflake_data_set_up.sql`) does a `TRUNCATE` then `INSERT` with a massive 3-way join across `sessions`, `query_history`, and `access_history`. If the insert fails mid-way, you're left with an empty table. This should use a swap pattern (write to a temp table, then `ALTER TABLE ... SWAP WITH`).

- [x] **Client Classification CASE/WHEN Block Is Unmaintainable** — 70+ hardcoded `ILIKE` patterns for classifying client applications. Adding a new tool means editing the stored procedure. This should be a lookup/mapping table instead.

- [x] **Single-File App at 1190 Lines** — All logic (data loading, graph building, chart rendering, theme detection, CSS, JavaScript) lives in one file. Splitting into modules (e.g., `data.py`, `network.py`, `charts.py`, `theme.py`) would improve maintainability.

- [ ] **No Tests** — Zero test coverage. The data processing, filtering, and client classification logic are all testable but untested.

- [ ] **Theme Detection Is Overly Complex** — There's both a Python-side theme detector (`get_theme_colors`) and a JavaScript polling loop that runs every 200ms to sync PyVis with Streamlit's theme. The JS alone is ~160 lines of DOM manipulation. This could be simplified.

- [ ] **`process_dataframe` Silently Drops Rows** — `df.query("ACCESS_COUNT > 20")` uses a hardcoded threshold that's separate from the user-facing "Access Count Limit" filter. Users have no visibility into this hidden cutoff.

- [ ] **Missing Date/Time Context** — The data covers 30 days but there's no date column exposed in the UI. Users can't see trends over time or filter by date range.

- [x] **No Node Images for Clients** — Databases and warehouses get custom icons, but client application nodes are rendered as default vis.js nodes. Adding icons for major clients (Tableau, dbt, Python, etc.) would improve the visualization.

- [ ] **Caching Could Cause Stale Data** — `load_data()` has a 5-minute TTL, but `process_dataframe()` has no TTL at all (cached forever until manual refresh). If underlying data changes, processed results go stale.

- [ ] **No Error Boundaries Around the Network Graph** — If `build_network_html` fails or produces invalid HTML, the whole page breaks. The function is ~400 lines with string manipulation on generated HTML.

- [ ] **CRON Task Hardcodes `COMPUTE_WH`** — The warehouse in the SQL task definition should be parameterized or documented more prominently.

- [ ] **No Pagination or Virtual Scrolling** — With large datasets, the network graph loads all nodes at once, which can be slow. The "Graph Node Limit" dropdown mitigates this but is crude.
