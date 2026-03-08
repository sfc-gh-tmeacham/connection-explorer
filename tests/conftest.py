"""Shared fixtures for unit tests."""

import pandas as pd
import pytest


@pytest.fixture()
def sample_df() -> pd.DataFrame:
    """Minimal DataFrame matching the app's standard 8-column schema."""
    return pd.DataFrame({
        "ORGANIZATION_NAME": ["ORG"] * 6,
        "ACCOUNT_NAME": ["ACCT"] * 6,
        "DATABASE": ["DB_A", "DB_A", "DB_B", "DB_B", "DB_A", "DB_B"],
        "SCHEMA_NAME": [
            "DB_A.S1", "DB_A.S1", "DB_B.S2", "DB_B.S2", "DB_A.S1", "DB_B.S2",
        ],
        "WAREHOUSE": ["WH1", "WH1", "WH2", "WH2", "WH1", "WH2"],
        "CLIENT": ["Tableau", "dbt", "Power BI", "Looker", "Tableau", "dbt"],
        "DIRECTION": ["read", "write", "read", "read", "read", "write"],
        "ACCESS_COUNT": [100, 200, 150, 50, 80, 300],
    })


@pytest.fixture()
def aggregated_df() -> pd.DataFrame:
    """Pre-aggregated DataFrame for network edge tests."""
    return pd.DataFrame({
        "DATABASE": ["DB_A", "DB_A", "DB_B"],
        "SCHEMA_NAME": ["DB_A.S1", "DB_A.S1", "DB_B.S2"],
        "WAREHOUSE": ["WH1", "WH1", "WH2"],
        "CLIENT": ["Tableau", "dbt", "Power BI"],
        "DIRECTION": ["read", "write", "read"],
        "ACCESS_COUNT": [180, 200, 150],
    })
