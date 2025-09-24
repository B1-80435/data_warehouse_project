import sqlite3
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

DB = "logs.db"

@st.cache_data
def get_tables():
    conn = sqlite3.connect(DB)
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'logs_%';").fetchall()]
    conn.close()

    return tables 

@st.cache_data
def get_filter_options(table):
    conn = sqlite3.connect(DB)
    query = f"""
        SELECT DISTINCT 
            TRIM(chip_id) AS chip_id,
            TRIM(design_block) AS design_block,
            TRIM(team) AS team,
            TRIM(impact_score) AS impact_score,
            TRIM(test_condition) AS test_condition
            FROM {table}
        WHERE chip_id IS NOT NULL 
           OR design_block IS NOT NULL 
           OR team IS NOT NULL 
           OR impact_score IS NOT NULL 
           OR test_condition IS NOT NULL; """
    
    df = pd.read_sql_query(query, conn)
    conn.close()

    # Clean duplicates / normalize values
    for col in df.columns:
        if col in df:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .str.upper()         # optional → make case-insensitive
                .replace("NAN", "")  # clean up NaN text if it appears
            )
    return df


def build_where_clause(filters):
    clauses = []
    params = []
    for col, vals in filters.items():
        if vals:
            placeholders = ",".join(["?"] * len(vals))
            clauses.append(f"LOWER({col}) IN ({placeholders})")
            params.extend([v.lower() for v in vals])
    where_caluse = " WHERE " + " AND ".join(clauses) if clauses else ""
    return where_caluse, params 

def load_filtered_data(conn, table, filters, limit=500000):
    where_clause, params = build_where_clause(filters)
    limit_clause = f"LIMIT {limit}" if limit else ""
    query = f"SELECT * FROM {table} {where_clause} {limit_clause};"
    return pd.read_sql_query(query, conn, params=params or None)

# Streamlit COnfiguration

st.set_page_config(page_title="Log Analytics Dashbaord", layout="wide")
st.sidebar.title("Log Dashboard")
conn = sqlite3.connect(DB)

tables = get_tables()

selected_table = st.sidebar.selectbox("Select Log Source", tables)

# Sidebar filters (dynamic based on available cols)

filter_cols = ["chip_id", "design_block", "team", "impact_score", "test_condition"]

filters_df = get_filter_options(selected_table)
active_filters = {}

for col in filter_cols:
    if col in filters_df.columns:
        options = sorted(filters_df[col].dropna().str.upper().unique().tolist())
        selected = st.sidebar.multiselect(f"Filter by {col}", options)
        if selected:
            active_filters[col] = selected


df = load_filtered_data(conn, selected_table, active_filters)

st.title(f"📊 Log Analytics Dashboard: {selected_table}")

if not df.empty:
    c1,c2,c3,c4 = st.columns(4)
    with c1:
        st.metric("Total logs", f"{len(df):,}")
    with c2:
        st.metric("Unique Chips", f"{df['chip_id'].nunique():,}" if "chip_id" in df else "N/A")
    with c3:
        st.metric("Blocks impacted", f"{df['design_block'].nunique():,}" if "design_block" in df else "N/A")
    with c4:
        st.metric("Teams Impacted", f"{df['team'].nunique():,}" if "team" in df else "N/A")
    

# Show active filters
if active_filters:
    st.write("### Active Filters")
    st.write(", ".join([f"{col}: {', '.join(vals)}" for col, vals in active_filters.items()]))

if not df.empty:
    

    if "design_block" in df.columns:
        st.subheader("Top impacted Design Blocks")
        top_blocks = df['design_block'].value_counts().reset_index()
        top_blocks.columns = ["design_block", "count"]
        fig = px.bar(top_blocks.head(10), x="design_block", y="count",text="count",
                     title="Top 10 impacted Design Blocks", color="count")
        fig.update_traces(textposition = "outside")
        st.plotly_chart(fig, use_container_width=True)

    if "team" in df.columns:
        st.subheader("Logs by Team.")
        team_counts = df['team'].value_counts().reset_index()
        team_counts.columns = ["team", "count"]
        fig = px.bar(team_counts.head(10), x='team', y='count', text='count',
                     title='Top 10 Teams by Logs.', color='count')
        fig.update_traces(textposition='outside')
        st.plotly_chart(fig, use_container_width=True)
        
    if "impact_score" in df.columns:
        st.subheader("Impact Score Distribution")
        fig = px.pie(df, names="impact_score", title="Impact Score Breakdown")
        st.plotly_chart(fig, use_container_width=True)

    # Time trend (check if Date/Timestamp exists)
    date_col = None
    for c in ["Date", "Timestamp", "Time"]:
        if c in df.columns:
            date_col = c
            break

    if date_col:
        st.subheader(f"Log Trend over {date_col}")
        trend = df.groupby(date_col).size().reset_index(name="count")
        fig = px.line(trend, x=date_col, y="count", title=f"Log over {date_col}")
        st.plotly_chart(fig, use_container_width=True)

    #  Heatmap
    if "design_block" in df.columns and "team" in df.columns:
        st.subheader("Heatmap: Logs by Block v/s Team")
        pivot = df.pivot_table(index='design_block', columns='team', aggfunc='size', fill_value=0)
        fig = px.imshow(pivot, aspect='auto', color_continuous_scale='Reds',
                        title="Log Heatmap")
        st.plotly_chart(fig, use_container_width=True)

    # Error categorization trend
    if 'failure_type' in df.columns and date_col:
        st.subheader("Log Categorization Trend")
        trend_cat = df.groupby([date_col, 'failure_type']).size().reset_index(name='count')
        fig = px.area(trend_cat, x=date_col, y='count', color='failure_type',
                      title='Log Trend by Failure Type', groupnorm='fraction')
        st.plotly_chart(fig, use_container_width=True)

    # Anomaly detection
    if date_col:
        st.subheader("Log Spike Detection")
        trend['rolling_avg'] = trend['count'].rolling(window=3, center=True).mean()
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=trend[date_col], y=trend['count'],
                                 mode = "lines+markers", name="Daily Logs"))
        fig.add_trace(go.Scatter(x=trend[date_col], y=trend['rolling_avg'],
                                 mode='lines', name='3-day Rolling AVG'))
        fig.update_layout(title="Anomaly Detection in Log Trend")
        st.plotly_chart(fig, use_container_width=True)

    # # Pareto Chart
    if "design_block" in df.columns:
        st.subheader("Pareto Analysis of Log (80/20 Rule)")
        pareto = df.groupby("design_block").size().reset_index(name='count').sort_values(by='count', ascending=False)
        pareto['cum_pct'] = pareto['count'].cumsum() / pareto['count'].sum() * 100

        fig = go.Figure()
        fig.add_bar(x=pareto['design_block'], y=pareto['count'], name='Logs')
        fig.add_scatter(x=pareto['design_block'], y=pareto['cum_pct'], name='Cumulative %', yaxis='y2')

        fig.update_layout(title="Logs by Design Block", yaxis=dict(title="Logs Count"),
                          yaxis2 = dict(title="Cumulative %", overlaying='y', side='right', range=[0,110]))
        st.plotly_chart(fig, use_container_width=True)


# Show raw logs
st.subheader("Raw Logs")
st.dataframe(df.head(50))

# Download option

if not df.empty:
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button("Download filtered logs as CSV", csv, "logs_filtered.csv", "text/csv")

if df.empty:
    st.warning("No Logs found for selected Filters.")
    st.stop()

conn.close()
