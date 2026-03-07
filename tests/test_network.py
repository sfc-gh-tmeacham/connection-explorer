"""Tests for components.network — edge aggregation, node stats, log scale, tooltips, clusters."""

import pandas as pd

from components.network import (
    _aggregate_edges,
    _compute_node_stats,
    _log_scale,
    _build_tooltip,
    _assign_cluster,
)


class TestAggregateEdges:
    def test_sums_access_counts(self, sample_df):
        result = _aggregate_edges(sample_df)
        # Two Tableau-read-DB_A-WH1 rows (100 + 80) should merge
        mask = (
            (result["DATABASE"] == "DB_A")
            & (result["CLIENT"] == "Tableau")
            & (result["DIRECTION"] == "read")
        )
        assert result.loc[mask, "ACCESS_COUNT"].iloc[0] == 180

    def test_preserves_columns(self, sample_df):
        result = _aggregate_edges(sample_df)
        for col in ["DATABASE", "SCHEMA_NAME", "WAREHOUSE", "CLIENT", "DIRECTION", "ACCESS_COUNT"]:
            assert col in result.columns

    def test_no_duplicates(self, sample_df):
        result = _aggregate_edges(sample_df)
        group_cols = ["DATABASE", "SCHEMA_NAME", "WAREHOUSE", "CLIENT", "DIRECTION"]
        assert not result.duplicated(subset=group_cols).any()


class TestComputeNodeStats:
    def test_returns_dict(self, aggregated_df):
        stats = _compute_node_stats(aggregated_df)
        assert isinstance(stats, dict)

    def test_has_expected_nodes(self, aggregated_df):
        stats = _compute_node_stats(aggregated_df)
        for node in ["DB_A", "DB_B", "WH1", "WH2", "Tableau", "dbt", "Power BI"]:
            assert node in stats, f"Missing node: {node}"

    def test_read_write_split(self, aggregated_df):
        stats = _compute_node_stats(aggregated_df)
        tableau = stats["Tableau"]
        assert tableau["read"] == 180
        assert tableau["write"] == 0
        assert tableau["total"] == 180

    def test_connections_tracked(self, aggregated_df):
        stats = _compute_node_stats(aggregated_df)
        assert "DB_A" in stats["Tableau"]["connections"]


class TestLogScale:
    def test_zero_returns_out_min(self):
        assert _log_scale(0, 1, 1000, 10, 100) == 10

    def test_min_equals_max_returns_out_min(self):
        assert _log_scale(50, 100, 100, 10, 100) == 10

    def test_mid_value(self):
        result = _log_scale(500, 1, 1000, 10, 100)
        assert 10 < result < 100

    def test_max_value_near_out_max(self):
        result = _log_scale(1000, 1, 1000, 10, 100)
        assert result > 90


class TestBuildTooltip:
    def test_contains_node_info(self):
        stats = {"total": 1000, "read": 800, "write": 200, "connections": {"WH1": 500}}
        tip = _build_tooltip("DB_A", "Database", stats, "MY_ORG", "MY_ACCT")
        assert "Database: DB_A" in tip
        assert "MY_ORG" in tip
        assert "1,000" in tip

    def test_top_connections(self):
        stats = {
            "total": 100, "read": 100, "write": 0,
            "connections": {"A": 50, "B": 30, "C": 20, "D": 10},
        }
        tip = _build_tooltip("X", "Client", stats, "O", "A")
        assert "A: 50" in tip
        assert "B: 30" in tip
        assert "C: 20" in tip
        # D should not appear (top 3 only)
        assert "D: 10" not in tip

    def test_empty_stats(self):
        tip = _build_tooltip("X", "Warehouse", {}, "O", "A")
        assert "Warehouse: X" in tip
        assert "Total Access: 0" in tip


class TestAssignCluster:
    def test_raw_layer(self):
        assert _assign_cluster("RAW_EVENTS_DB") == "Raw Layer"

    def test_clean_layer(self):
        assert _assign_cluster("CLEANED_CRM_DB") == "Clean Layer"

    def test_integrated_is_clean(self):
        assert _assign_cluster("INTEGRATED_DB") == "Clean Layer"

    def test_gold_layer(self):
        assert _assign_cluster("FINANCE_MART_DB") == "Gold Layer"

    def test_unclustered(self):
        assert _assign_cluster("SANDBOX_DB") == ""

    def test_ml_unclustered(self):
        assert _assign_cluster("ML_FEATURES_DB") == ""
