"""Tests for components.charts — chart data prep and Plotly figure builders."""

import pandas as pd
import plotly.graph_objects as go

from components.charts import _short_schema, prepare_chart_data, _build_bar_chart, _build_sankey


class TestShortSchema:
    def test_qualified(self):
        assert _short_schema("DATABASE.SCHEMA") == "SCHEMA"

    def test_unqualified(self):
        assert _short_schema("SCHEMA") == "SCHEMA"

    def test_empty(self):
        assert _short_schema("") == ""

    def test_multi_dot(self):
        assert _short_schema("A.B.C") == "C"


class TestPrepareChartData:
    def test_returns_aggregated(self, sample_df):
        result = prepare_chart_data(sample_df, "CLIENT", top_n=10)
        assert "Client" in result.columns
        assert "Direction" in result.columns
        assert "Access Count" in result.columns

    def test_empty_input(self):
        empty = pd.DataFrame()
        result = prepare_chart_data(empty, "CLIENT")
        assert result.empty

    def test_top_n_limits(self, sample_df):
        result = prepare_chart_data(sample_df, "CLIENT", top_n=2)
        assert result["Client"].nunique() <= 2

    def test_direction_normalized(self, sample_df):
        result = prepare_chart_data(sample_df, "CLIENT")
        assert set(result["Direction"].unique()).issubset({"Read", "Write"})


class TestBuildBarChart:
    def test_returns_figure(self, sample_df):
        data = prepare_chart_data(sample_df, "DATABASE")
        fig = _build_bar_chart(data, "DATABASE", "rgba(0,0,0,0.1)")
        assert isinstance(fig, go.Figure)
        assert len(fig.data) > 0

    def test_empty_returns_none(self):
        empty = pd.DataFrame()
        assert _build_bar_chart(empty, "DATABASE", "rgba(0,0,0,0.1)") is None

    def test_stacked_mode(self, sample_df):
        data = prepare_chart_data(sample_df, "DATABASE")
        fig = _build_bar_chart(data, "DATABASE", "rgba(0,0,0,0.1)")
        assert fig.layout.barmode == "stack"


class TestBuildSankey:
    def test_read_sankey(self, sample_df):
        fig = _build_sankey(sample_df, "read")
        assert isinstance(fig, go.Figure)
        assert len(fig.data) > 0
        assert isinstance(fig.data[0], go.Sankey)

    def test_no_matching_direction_returns_none(self, sample_df):
        # Filter to only writes, then ask for a direction that doesn't exist
        writes_only = sample_df[sample_df["DIRECTION"] == "write"]
        assert _build_sankey(writes_only, "read") is None

    def test_write_sankey(self, sample_df):
        fig = _build_sankey(sample_df, "write")
        assert isinstance(fig, go.Figure)
