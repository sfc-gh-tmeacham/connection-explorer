"""Tests for components.data — apply_filters and related pure functions."""

import pandas as pd

from components.data import apply_filters


class TestApplyFilters:
    """Tests for apply_filters()."""

    def test_no_filters_returns_above_threshold(self, sample_df):
        """With no dimension filters, only the access_count threshold applies."""
        result = apply_filters(sample_df, (), (), (), (), "", (), 0)
        assert len(result) == len(sample_df)

    def test_access_count_threshold(self, sample_df):
        """Rows at or below the threshold are excluded (exclusive >)."""
        result = apply_filters(sample_df, (), (), (), (), "", (), 100)
        assert all(result["ACCESS_COUNT"] > 100)

    def test_database_filter(self, sample_df):
        result = apply_filters(sample_df, ("DB_A",), (), (), (), "", (), 0)
        assert set(result["DATABASE"].unique()) == {"DB_A"}

    def test_warehouse_filter(self, sample_df):
        result = apply_filters(sample_df, (), (), ("WH2",), (), "", (), 0)
        assert set(result["WAREHOUSE"].unique()) == {"WH2"}

    def test_client_filter(self, sample_df):
        result = apply_filters(sample_df, (), (), (), ("Tableau",), "", (), 0)
        assert set(result["CLIENT"].unique()) == {"Tableau"}

    def test_direction_filter(self, sample_df):
        result = apply_filters(sample_df, (), (), (), (), "", ("write",), 0)
        assert set(result["DIRECTION"].unique()) == {"write"}

    def test_org_filter(self, sample_df):
        result = apply_filters(sample_df, (), (), (), (), "ORG", (), 0)
        assert len(result) == len(sample_df)

    def test_org_filter_no_match(self, sample_df):
        result = apply_filters(sample_df, (), (), (), (), "NONEXISTENT", (), 0)
        assert len(result) == 0

    def test_combined_filters(self, sample_df):
        result = apply_filters(sample_df, ("DB_A",), (), (), ("Tableau",), "", ("read",), 0)
        assert all(result["DATABASE"] == "DB_A")
        assert all(result["CLIENT"] == "Tableau")
        assert all(result["DIRECTION"] == "read")

    def test_empty_dataframe(self):
        empty = pd.DataFrame(columns=[
            "ORGANIZATION_NAME", "ACCOUNT_NAME", "DATABASE", "SCHEMA_NAME",
            "WAREHOUSE", "CLIENT", "DIRECTION", "ACCESS_COUNT",
        ])
        result = apply_filters(empty, (), (), (), (), "", (), 0)
        assert result.empty

    def test_schema_filter(self, sample_df):
        result = apply_filters(sample_df, (), ("DB_A.S1",), (), (), "", (), 0)
        assert set(result["SCHEMA_NAME"].unique()) == {"DB_A.S1"}
