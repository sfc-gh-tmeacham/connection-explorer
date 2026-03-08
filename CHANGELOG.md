# Changelog

All notable changes to the Connection Explorer project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [0.3.0] - 2026-03-08

### Added
- Combine R/W toggle (default: on) merges read/write edges into a single blue line per node pair
- Charts page organized into tabs with Material icons (Bar Charts, Heatmaps, Treemap, Sankey)
- Edge deduplication lesson in docs/lessons_learned.md

### Changed
- Widened toggle column layout from [5, 4.5, 0.5] to [3, 6.5, 0.5] to fit 6 checkboxes
- Fixed app name in lessons_learned.md to "Connection Explorer"

## [0.2.0] - 2026-03-07

### Added
- Edge deduplication: at most 2 edges (1 read + 1 write) per node pair
- Unit test suite covering 16 pure functions across 5 component modules
- Project documentation, lessons learned, and README with screenshot
- Deploy scripts, SQL setup, and Snowflake infrastructure configuration
- 257 client classifications with SVG brand icons

### Changed
- Refactored app into modular architecture with Streamlit Components v2 network graph
- Removed upstream files superseded by fork restructuring

## [0.1.0] - 2026-03-06

### Added
- Initial fork from data-lake-explorer with renamed project
- Interactive network graph with vis.js force-directed layout
- Sidebar filters: client, warehouse, database, schema, direction
- Multi-select sidebar filters with persistent state
- Hide Warehouses, Hide Clients, Hide Databases, Hide Schemas toggles
- Cluster Databases toggle to group schemas under parent databases
- Fullscreen mode for network graph
- Bar charts (Client, Database, Schema, Warehouse by access count)
- Client x Database heatmap
- Hierarchical treemap (Client -> Database -> Schema -> Direction)
- Sankey flow diagrams (Client -> Warehouse -> Database -> Schema)
- Graph Node Limit selector
- Customer deployment support with account_usage tables
- Dark/light theme support for pyvis loading bar
- Public read-only repo policy and metadata
- Uninstall scripts
