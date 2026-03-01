# Data Lake Explorer — Improvements

## Done

- [x] **Client Classification CASE/WHEN Block Is Unmaintainable** — Replaced 70+ hardcoded `ILIKE` patterns with a Python lookup table in `components/client_mappings.py`.

- [x] **Single-File App at 1190 Lines** — Split into 8 modules under `components/` (theme, assets, data, client_mappings, setup, charts, network) plus `streamlit_app.py` entry point. Google-style docstrings added to all files.

- [x] **No Node Images for Clients** — `generate_client_icon_uri` in `client_mappings.py` creates unique SVG icons for every client application.

- [x] **Theme Detection Is Overly Complex** — The 160-line JS polling loop is gone. Migrated to Streamlit Components v2 which uses native CSS variables (`--st-text-color`, `--st-background-color`) for automatic theme sync. Python-side `get_theme_colors` remains but is minimal.

- [x] **No Error Boundaries Around the Network Graph** — The old `build_network_html` string-manipulation approach is gone. The graph now uses structured JSON passed to a Components v2 mount function, eliminating the class of HTML/JS injection failures.

## Open

- [ ] **Local Snowflake Connectivity** — The app falls back to hardcoded sample data when running locally. Adding `Session.builder.configs()` support (reading from `~/.snowflake/connections.toml` or env vars) would make local development useful.

- [ ] **SQL Data Pipeline Is Fragile** — The stored procedure (`snowflake_data_set_up.sql`) does a `TRUNCATE` then `INSERT` with a massive 3-way join across `sessions`, `query_history`, and `access_history`. If the insert fails mid-way, you get an empty table. Should use a swap pattern (write to temp table, then `ALTER TABLE ... SWAP WITH`).

- [ ] **No Tests** — Zero test coverage. The data processing, filtering, and client classification logic are all testable but untested.

- [ ] **`process_dataframe` Silently Drops Rows** — `df.query("ACCESS_COUNT > 20")` in `components/data.py:330` uses a hardcoded threshold separate from the user-facing "Access Count Limit" filter. Users have no visibility into this hidden cutoff.

- [ ] **Missing Date/Time Context** — The data covers 30 days but there is no date column exposed in the UI. Users cannot see trends over time or filter by date range.

- [ ] **Caching Could Cause Stale Data** — `load_data()` has a 5-minute TTL, but `process_dataframe()` has no TTL at all (cached forever until manual refresh). If underlying data changes, processed results go stale.

- [ ] **CRON Task Hardcodes `COMPUTE_WH`** — The warehouse in the SQL task definition should be parameterized or documented more prominently.

- [ ] **No Pagination or Virtual Scrolling** — With large datasets, the network graph loads all nodes at once, which can be slow. The "Graph Node Limit" dropdown mitigates this but is crude.
