"""Weather ETL Pipeline Dashboard — Streamlit app."""

from __future__ import annotations

import os
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots
from sqlalchemy import create_engine, text

# ── connection strings ────────────────────────────────────────────────────────
WEATHER_DSN = os.getenv(
    "WEATHER_DSN", "postgresql://airflow:airflow@localhost:5434/weather"
)
AIRFLOW_DSN = os.getenv(
    "AIRFLOW_DSN", "postgresql://airflow:airflow@localhost:5434/airflow"
)

CITY_COLORS = {
    "Karachi": "#EF553B",
    "Lahore": "#00CC96",
    "Islamabad": "#636EFA",
    "Peshawar": "#FFA15A",
    "Quetta": "#AB63FA",
}

st.set_page_config(
    page_title="Weather ETL Dashboard",
    page_icon="🌤",
    layout="wide",
)


# ── helpers ───────────────────────────────────────────────────────────────────
@st.cache_resource
def _engine(dsn: str):
    return create_engine(dsn)


@st.cache_data(ttl=60)
def query(dsn: str, sql: str) -> pd.DataFrame:
    try:
        with _engine(dsn).connect() as conn:
            return pd.read_sql(text(sql), conn)
    except Exception as exc:
        return pd.DataFrame({"error": [str(exc)]})


def _ok(df: pd.DataFrame) -> bool:
    return "error" not in df.columns and not df.empty


# ── page header ───────────────────────────────────────────────────────────────
st.title("🌤 Weather ETL Pipeline Dashboard")
st.caption(
    f"Auto-refreshes every 60 s · Last rendered: "
    f"{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"
)

col_refresh, _ = st.columns([1, 9])
with col_refresh:
    if st.button("↻ Refresh now"):
        st.cache_data.clear()
        st.rerun()

st.divider()

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — PIPELINE STATUS
# ═══════════════════════════════════════════════════════════════════════════════
st.subheader("🔄 Pipeline Status — last 5 DAG runs")

pipeline_sql = """
SELECT
    ti.run_id,
    ti.task_id,
    ti.state,
    ti.start_date,
    ti.end_date,
    ROUND(ti.duration::numeric, 1) AS duration_s
FROM task_instance ti
WHERE ti.dag_id = 'weather_etl_pipeline'
  AND ti.run_id IN (
      SELECT run_id
      FROM (
          SELECT DISTINCT run_id, MAX(start_date) AS run_start
          FROM task_instance
          WHERE dag_id = 'weather_etl_pipeline'
          GROUP BY run_id
      ) ranked
      ORDER BY run_start DESC
      LIMIT 5
  )
ORDER BY ti.start_date DESC, ti.task_id;
"""

pipeline_df = query(AIRFLOW_DSN, pipeline_sql)

if _ok(pipeline_df):
    task_order = ["ingest_weather_data", "validate_raw_data", "dbt_run", "dbt_test"]
    pivot = (
        pipeline_df.pivot(index="run_id", columns="task_id", values="state")
        .reindex(columns=[t for t in task_order if t in pipeline_df["task_id"].unique()])
        .sort_index(ascending=False)
    )

    def _color(val):
        colours = {
            "success": "background-color:#1a4a1a; color:#90ee90",
            "failed": "background-color:#4a1a1a; color:#ff9090",
            "running": "background-color:#3a3a00; color:#ffff80",
            "skipped": "background-color:#2a2a2a; color:#aaaaaa",
            "upstream_failed": "background-color:#4a1a1a; color:#ff9090",
        }
        return colours.get(str(val).lower(), "")

    styled = pivot.style.map(_color)
    st.dataframe(styled, width="stretch")

    # duration bar for the most recent run (by actual start time)
    latest_run = (
        pipeline_df.dropna(subset=["start_date"])
        .groupby("run_id")["start_date"]
        .max()
        .idxmax()
    )
    latest = pipeline_df[pipeline_df["run_id"] == latest_run].copy()
    latest["duration_s"] = latest["duration_s"].fillna(0)
    fig_dur = px.bar(
        latest,
        x="task_id",
        y="duration_s",
        color="state",
        color_discrete_map={
            "success": "#2ecc71",
            "failed": "#e74c3c",
            "running": "#f39c12",
            "skipped": "#95a5a6",
            "upstream_failed": "#c0392b",
        },
        labels={"task_id": "Task", "duration_s": "Duration (s)"},
        title=f"Task durations — {latest_run}",
    )
    fig_dur.update_layout(showlegend=True, height=280)
    st.plotly_chart(fig_dur, width="stretch")

    broken = pipeline_df[pipeline_df["state"].isin(["failed", "upstream_failed"])]
    root_failures = pipeline_df[pipeline_df["state"] == "failed"]

    if not root_failures.empty:
        bad_tasks = root_failures["task_id"].unique().tolist()
        downstream = pipeline_df[
            (pipeline_df["state"] == "upstream_failed")
            & (~pipeline_df["task_id"].isin(bad_tasks))
        ]["task_id"].unique().tolist()

        msg = (
            f"**Root failure:** `{'`, `'.join(bad_tasks)}` is failing in every run."
        )
        if downstream:
            msg += (
                f"  \n**Blocked downstream:** `{'`, `'.join(downstream)}` "
                f"shows `upstream_failed` because it can't run until the above is fixed."
            )
        st.error(msg)
    elif not broken.empty:
        st.warning("Some tasks are blocked by upstream failures.")
    else:
        st.success("All tasks succeeded in the last 5 runs.")
else:
    st.warning("Could not reach the Airflow metadata DB. Check that the containers are running.")
    if "error" in pipeline_df.columns:
        st.code(pipeline_df["error"].iloc[0])

st.divider()

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — DATA OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════════
st.subheader("📋 Raw Data Overview")

overview_sql = """
SELECT
    COUNT(*)                        AS total_rows,
    COUNT(DISTINCT city)            AS cities,
    MIN(date)                       AS earliest_date,
    MAX(date)                       AS latest_date,
    MAX(fetched_at)                 AS last_fetched,
    COUNT(*) FILTER (WHERE temp_max IS NULL OR temp_min IS NULL) AS rows_with_nulls
FROM raw.weather_data;
"""

overview = query(WEATHER_DSN, overview_sql)

if _ok(overview):
    row = overview.iloc[0]
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Total rows", f"{int(row['total_rows']):,}")
    c2.metric("Cities", int(row["cities"]))
    c3.metric("Earliest date", str(row["earliest_date"]))
    c4.metric("Latest date", str(row["latest_date"]))
    c5.metric("Last fetched", str(row["last_fetched"])[:16])
    c6.metric("Rows w/ nulls", int(row["rows_with_nulls"]))
else:
    st.warning("Could not reach the weather database.")
    if "error" in overview.columns:
        st.code(overview["error"].iloc[0])

st.divider()

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — TEMPERATURE TRENDS
# ═══════════════════════════════════════════════════════════════════════════════
st.subheader("🌡 Temperature Trends by City")

temp_sql = """
SELECT
    city, date,
    temp_max AS temp_max_c,
    temp_min AS temp_min_c,
    (temp_max + temp_min) / 2.0 AS temp_avg_c
FROM raw.weather_data
WHERE temp_max IS NOT NULL AND temp_min IS NOT NULL
ORDER BY city, date;
"""

temp_df = query(WEATHER_DSN, temp_sql)

if _ok(temp_df):
    tab_max, tab_min, tab_avg = st.tabs(["Max temp", "Min temp", "Avg temp"])

    for tab, col, label in [
        (tab_max, "temp_max_c", "Max Temperature (°C)"),
        (tab_min, "temp_min_c", "Min Temperature (°C)"),
        (tab_avg, "temp_avg_c", "Avg Temperature (°C)"),
    ]:
        with tab:
            fig = px.line(
                temp_df,
                x="date",
                y=col,
                color="city",
                color_discrete_map=CITY_COLORS,
                markers=True,
                labels={"date": "Date", col: label, "city": "City"},
            )
            fig.update_layout(height=380, legend_title_text="City")
            st.plotly_chart(fig, width="stretch")
else:
    st.info("No temperature data available yet.")

st.divider()

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — CITY COMPARISON
# ═══════════════════════════════════════════════════════════════════════════════
st.subheader("🏙 City Comparison (all-time averages)")

city_sql = """
SELECT
    city,
    ROUND(AVG(temp_max)::numeric, 2)        AS avg_temp_max_c,
    ROUND(AVG(temp_min)::numeric, 2)        AS avg_temp_min_c,
    MAX(temp_max)                            AS all_time_high_c,
    MIN(temp_min)                            AS all_time_low_c,
    ROUND(AVG(precipitation)::numeric, 2)   AS avg_rain_mm,
    SUM(precipitation)                       AS total_rain_mm,
    ROUND(AVG(wind_speed_max)::numeric, 2)  AS avg_wind_kmh,
    MAX(wind_speed_max)                      AS peak_wind_kmh
FROM raw.weather_data
WHERE temp_max IS NOT NULL AND temp_min IS NOT NULL
GROUP BY city
ORDER BY avg_temp_max_c DESC;
"""

city_df = query(WEATHER_DSN, city_sql)

if _ok(city_df):
    col_left, col_right = st.columns(2)

    with col_left:
        fig_temp = go.Figure()
        fig_temp.add_bar(
            name="Avg Max (°C)",
            x=city_df["city"],
            y=city_df["avg_temp_max_c"],
            marker_color=[CITY_COLORS.get(c, "#888") for c in city_df["city"]],
        )
        fig_temp.add_bar(
            name="Avg Min (°C)",
            x=city_df["city"],
            y=city_df["avg_temp_min_c"],
            marker_color=[CITY_COLORS.get(c, "#888") for c in city_df["city"]],
            opacity=0.5,
        )
        fig_temp.update_layout(
            title="Average Max & Min Temperature",
            barmode="group",
            height=340,
            yaxis_title="°C",
        )
        st.plotly_chart(fig_temp, width="stretch")

    with col_right:
        fig_rain = px.bar(
            city_df,
            x="city",
            y="total_rain_mm",
            color="city",
            color_discrete_map=CITY_COLORS,
            labels={"total_rain_mm": "Total Rainfall (mm)", "city": "City"},
            title="Total Rainfall",
        )
        fig_rain.update_layout(height=340, showlegend=False)
        st.plotly_chart(fig_rain, width="stretch")

    st.dataframe(
        city_df.rename(columns={
            "city": "City",
            "avg_temp_max_c": "Avg Max °C",
            "avg_temp_min_c": "Avg Min °C",
            "all_time_high_c": "Record High °C",
            "all_time_low_c": "Record Low °C",
            "avg_rain_mm": "Avg Rain mm",
            "total_rain_mm": "Total Rain mm",
            "avg_wind_kmh": "Avg Wind km/h",
            "peak_wind_kmh": "Peak Wind km/h",
        }),
        width="stretch",
        hide_index=True,
    )
else:
    st.info("No city comparison data available yet.")

st.divider()

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — PRECIPITATION & WIND
# ═══════════════════════════════════════════════════════════════════════════════
st.subheader("🌧 Precipitation & Wind Speed")

weather_sql = """
SELECT city, date, precipitation AS precipitation_mm, wind_speed_max AS wind_speed_kmh
FROM raw.weather_data
ORDER BY city, date;
"""

weather_df = query(WEATHER_DSN, weather_sql)

if _ok(weather_df):
    city_sel = st.selectbox(
        "Select city",
        options=sorted(weather_df["city"].unique()),
        key="precip_city",
    )
    city_data = weather_df[weather_df["city"] == city_sel]

    fig_pw = make_subplots(specs=[[{"secondary_y": True}]])
    fig_pw.add_trace(
        go.Bar(
            x=city_data["date"],
            y=city_data["precipitation_mm"],
            name="Precipitation (mm)",
            marker_color="#636EFA",
            opacity=0.7,
        ),
        secondary_y=False,
    )
    fig_pw.add_trace(
        go.Scatter(
            x=city_data["date"],
            y=city_data["wind_speed_kmh"],
            name="Wind speed (km/h)",
            mode="lines+markers",
            line=dict(color="#EF553B", width=2),
        ),
        secondary_y=True,
    )
    fig_pw.update_yaxes(title_text="Precipitation (mm)", secondary_y=False)
    fig_pw.update_yaxes(title_text="Wind speed (km/h)", secondary_y=True)
    fig_pw.update_layout(
        title=f"{city_sel} — Precipitation & Wind",
        height=380,
        legend=dict(orientation="h", y=-0.2),
    )
    st.plotly_chart(fig_pw, width="stretch")
else:
    st.info("No precipitation/wind data available yet.")
