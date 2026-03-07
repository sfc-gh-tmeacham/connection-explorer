# Data Lake Explorer — Improvements

## Done

- [x] **Client Classification CASE/WHEN Block Is Unmaintainable** — Replaced 70+ hardcoded `ILIKE` patterns with a Python lookup table in `components/client_mappings.py`.

- [x] **Single-File App at 1190 Lines** — Split into 8 modules under `components/` (theme, assets, data, client_mappings, setup, charts, network) plus `streamlit_app.py` entry point. Google-style docstrings added to all files.

- [x] **No Node Images for Clients** — `generate_client_icon_uri` in `client_mappings.py` creates unique SVG icons for every client application.

- [x] **Theme Detection Is Overly Complex** — The 160-line JS polling loop is gone. Migrated to Streamlit Components v2 which uses native CSS variables (`--st-text-color`, `--st-background-color`) for automatic theme sync. Python-side `get_theme_colors` remains but is minimal.

- [x] **No Error Boundaries Around the Network Graph** — The old `build_network_html` string-manipulation approach is gone. The graph now uses structured JSON passed to a Components v2 mount function, eliminating the class of HTML/JS injection failures.

- [x] **SQL Data Pipeline Is Fragile** — Changed from `TRUNCATE` + `INSERT` to `INSERT OVERWRITE`, which is atomic. No risk of empty table if the insert fails mid-way.

- [x] **CRON Task Hardcodes `COMPUTE_WH`** — Warehouse is now parameterized via `$WH_NAME` variable in `snowflake_data_set_up.sql`.

- [x] **Local Snowflake Connectivity** — Added `.streamlit/secrets.toml` support with PAT (Personal Access Token) authentication for local development.

- [x] **`process_dataframe` Silently Drops Rows** — Removed the hardcoded `ACCESS_COUNT > 20` filter. Users now have full control via the UI's "Access Count Limit" filter.

## Open

- [ ] **No Tests** — Zero test coverage. The data processing, filtering, and client classification logic are all testable but untested.

- [ ] **Missing Date/Time Context** — The data covers 30 days but there is no date column exposed in the UI. Users cannot see trends over time or filter by date range.

- [ ] **Caching Could Cause Stale Data** — `load_data()` has a 5-minute TTL, but `process_dataframe()` has no TTL at all (cached forever until manual refresh). If underlying data changes, processed results go stale.

