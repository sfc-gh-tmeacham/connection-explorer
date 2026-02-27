"""Altair bar chart preparation and rendering."""

import altair as alt
import pandas as pd
import streamlit as st

from components.theme import SNOWFLAKE_BLUE, STAR_BLUE, get_theme_colors


@st.cache_data(show_spinner=False)
def prepare_chart_data(df: pd.DataFrame, column: str, top_n: int = 10) -> pd.DataFrame:
    """Prepare aggregated data for charts."""
    if df.empty:
        return pd.DataFrame()

    return (
        df[[column, "ACCESS_COUNT"]]
        .groupby(column)["ACCESS_COUNT"]
        .sum()
        .reset_index()
        .rename(columns={column: column.title(), "ACCESS_COUNT": "Access Count"})
        .sort_values("Access Count", ascending=False)
        .head(top_n)
    )


def _configure_chart(chart: alt.Chart, text_color: str, grid_color: str, domain_color: str) -> alt.Chart:
    """Apply standard Snowflake theme configuration to chart."""
    return (chart
        .configure_axis(labelFont="Lato", titleFont="Lato", labelFontSize=12, titleFontSize=12,
                       labelColor=text_color, titleColor=text_color, gridColor=grid_color,
                       domainColor=domain_color, tickColor=domain_color, labelPadding=6, titlePadding=10)
        .configure_legend(labelFont="Lato", titleFont="Lato", labelColor=text_color, titleColor=text_color)
        .configure_title(font="Lato", color=text_color)
        .configure_view(strokeWidth=0)
        .configure(background="transparent"))


def render_bar_charts(df: pd.DataFrame) -> None:
    if df.empty:
        st.warning("Apply different filters or load more data to see charts.")
        return

    is_dark, text_color = get_theme_colors()
    bar_color = STAR_BLUE if is_dark else SNOWFLAKE_BLUE
    grid_color = "rgba(255,255,255,0.18)" if is_dark else "rgba(0,0,0,0.10)"
    domain_color = "rgba(255,255,255,0.28)" if is_dark else "rgba(0,0,0,0.25)"

    def bar_chart(column, width=400):
        data = prepare_chart_data(df, column)
        if data.empty:
            return
        chart = (
            alt.Chart(data)
            .mark_bar(color=bar_color, cornerRadiusEnd=4)
            .encode(
                x=alt.X("Access Count:Q", title="Access Count"),
                y=alt.Y(f"{column.title()}:N", sort="-x", title=column.title()),
                tooltip=[alt.Tooltip(column.title() + ":N", title=column.title()),
                        alt.Tooltip("Access Count:Q", title="Access Count", format=",")],
            )
            .properties(width=width, height=350,
                       title=alt.TitleParams(text=f"Access Count by {column.title()}",
                                            fontSize=16, fontWeight="bold", color=text_color, font="Lato"))
        )
        st.altair_chart(_configure_chart(chart, text_color, grid_color, domain_color), use_container_width=True)

    col21, col22 = st.columns(2)
    with col21:
        bar_chart("CLIENT")
    with col22:
        bar_chart("DATABASE")
    bar_chart("WAREHOUSE", width=800)
