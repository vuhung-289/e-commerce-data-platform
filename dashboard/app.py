"""
E-commerce Daily Dashboard
Streamlit app connected to BigQuery — displays metrics from fact_daily_summary
"""
import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from google.cloud import bigquery
from datetime import datetime, timedelta

# ── Config ──────────────────────────────────────────────────────────────────
GCP_PROJECT = os.getenv("GCP_PROJECT_ID", "ecommerce-data-platform-492602")
CREDENTIALS_PATH = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS",
    os.path.join(os.path.dirname(__file__), "..", "airflow", "include", "gcp-credentials.json")
)

# Set credentials if file exists
if os.path.exists(CREDENTIALS_PATH):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH

st.set_page_config(
    page_title="E-commerce Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ──────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

    .main .block-container {
        padding-top: 1.5rem;
        padding-bottom: 1rem;
        max-width: 1400px;
    }

    /* Header */
    .dashboard-header {
        background: linear-gradient(135deg, #667EEA 0%, #764BA2 100%);
        padding: 1.8rem 2rem;
        border-radius: 16px;
        margin-bottom: 1.5rem;
        box-shadow: 0 8px 32px rgba(102, 126, 234, 0.25);
    }
    .dashboard-header h1 {
        color: white; margin: 0; font-size: 1.8rem; font-weight: 800;
        letter-spacing: -0.5px;
    }
    .dashboard-header p {
        color: rgba(255,255,255,0.85); margin: 0.3rem 0 0; font-size: 0.95rem;
    }

    /* KPI Cards */
    .kpi-card {
        background: linear-gradient(145deg, #1A1F36 0%, #232946 100%);
        border: 1px solid rgba(102, 126, 234, 0.15);
        padding: 1.2rem 1.4rem;
        border-radius: 14px;
        text-align: center;
        transition: transform 0.2s, box-shadow 0.2s;
        box-shadow: 0 4px 15px rgba(0,0,0,0.2);
    }
    .kpi-card:hover {
        transform: translateY(-3px);
        box-shadow: 0 8px 25px rgba(102, 126, 234, 0.2);
    }
    .kpi-label {
        font-size: 0.78rem; color: #8892B0; text-transform: uppercase;
        letter-spacing: 1.2px; font-weight: 600; margin-bottom: 0.4rem;
    }
    .kpi-value {
        font-size: 1.7rem; font-weight: 800; margin: 0.2rem 0;
        background: linear-gradient(135deg, #667EEA, #764BA2);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    }
    .kpi-delta-up { color: #48BB78; font-size: 0.82rem; font-weight: 600; }
    .kpi-delta-down { color: #FC8181; font-size: 0.82rem; font-weight: 600; }
    .kpi-delta-neutral { color: #8892B0; font-size: 0.82rem; font-weight: 600; }

    /* Section titles */
    .section-title {
        font-size: 1.15rem; font-weight: 700; color: #CCD6F6;
        margin: 1.5rem 0 0.8rem; padding-bottom: 0.5rem;
        border-bottom: 2px solid rgba(102, 126, 234, 0.3);
    }

    /* Chart containers */
    .chart-container {
        background: linear-gradient(145deg, #1A1F36 0%, #232946 100%);
        border: 1px solid rgba(102, 126, 234, 0.1);
        border-radius: 14px; padding: 1.2rem;
        box-shadow: 0 4px 15px rgba(0,0,0,0.15);
        margin-bottom: 1rem;
    }

    /* Sidebar */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0E1117 0%, #1A1F36 100%);
    }

    /* Hide default streamlit elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}

    /* Data table styling */
    .stDataFrame { border-radius: 10px; overflow: hidden; }
</style>
""", unsafe_allow_html=True)


# ── Data Loading ────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_fact_daily_summary():
    """Load fact_daily_summary from BigQuery, cached for 5 minutes."""
    client = bigquery.Client(project=GCP_PROJECT)
    query = f"""
        SELECT *
        FROM `{GCP_PROJECT}.marts.fact_daily_summary`
        ORDER BY order_date DESC
    """
    df = client.query(query).to_dataframe()
    df["order_date"] = pd.to_datetime(df["order_date"])
    return df


@st.cache_data(ttl=300)
def load_staging_transactions():
    """Load detailed transactions for drill-down."""
    client = bigquery.Client(project=GCP_PROJECT)
    query = f"""
        SELECT order_date, platform, category, order_status,
               total_amount_vnd, payment_method, customer_city
        FROM `{GCP_PROJECT}.staging.stg_transactions`
        ORDER BY order_date DESC
        LIMIT 10000
    """
    df = client.query(query).to_dataframe()
    df["order_date"] = pd.to_datetime(df["order_date"])
    return df


# ── Helper Functions ────────────────────────────────────────────────────────
CHART_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter", color="#CCD6F6", size=12),
    margin=dict(l=40, r=20, t=40, b=40),
    legend=dict(
        bgcolor="rgba(0,0,0,0)", borderwidth=0,
        font=dict(size=11, color="#8892B0"),
    ),
    xaxis=dict(gridcolor="rgba(102,126,234,0.08)", zeroline=False),
    yaxis=dict(gridcolor="rgba(102,126,234,0.08)", zeroline=False),
    hoverlabel=dict(
        bgcolor="#1A1F36", bordercolor="#667EEA",
        font=dict(color="#FAFAFA", size=12),
    ),
)

COLORS = {
    "primary": "#667EEA",
    "secondary": "#764BA2",
    "success": "#48BB78",
    "warning": "#F6AD55",
    "danger": "#FC8181",
    "info": "#63B3ED",
    "shopee": "#EE4D2D",
    "tiki": "#1A94FF",
    "lazada": "#A855F7",
    "sendo": "#27AE60",
}


def format_vnd(value):
    if value >= 1e9:
        return f"{value/1e9:.1f}B"
    if value >= 1e6:
        return f"{value/1e6:.1f}M"
    if value >= 1e3:
        return f"{value/1e3:.0f}K"
    return f"{value:,.0f}"


def format_usd(value):
    if value >= 1e6:
        return f"${value/1e6:.1f}M"
    if value >= 1e3:
        return f"${value/1e3:.1f}K"
    return f"${value:,.0f}"


def calc_delta(current, previous):
    if previous == 0:
        return 0, "neutral"
    pct = ((current - previous) / abs(previous)) * 100
    direction = "up" if pct > 0 else ("down" if pct < 0 else "neutral")
    return pct, direction


def render_kpi(label, value, delta_pct, direction, prefix="", suffix=""):
    arrow = "↑" if direction == "up" else ("↓" if direction == "down" else "→")
    delta_class = f"kpi-delta-{direction}"
    sign = "+" if delta_pct > 0 else ""
    return f"""
    <div class="kpi-card">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{prefix}{value}{suffix}</div>
        <div class="{delta_class}">{arrow} {sign}{delta_pct:.1f}% vs previous day</div>
    </div>
    """


# ── Main App ────────────────────────────────────────────────────────────────
def main():
    # Header
    st.markdown("""
    <div class="dashboard-header">
        <h1>E-commerce Analytics Dashboard</h1>
        <p>Real-time insights from BigQuery • Powered by dbt + Airflow</p>
    </div>
    """, unsafe_allow_html=True)

    # Load data
    try:
        df = load_fact_daily_summary()
    except Exception as e:
        st.error(f"Cannot connect to BigQuery: {e}")
        st.info("Check `GOOGLE_APPLICATION_CREDENTIALS` and GCP Project ID")
        st.stop()

    if df.empty:
        st.warning("No data in `fact_daily_summary`. Run the pipeline first!")
        st.stop()

    # ── Sidebar ─────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("### Filters")

        min_date = df["order_date"].min().date()
        max_date = df["order_date"].max().date()

        date_range = st.date_input(
            "Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )

        if len(date_range) == 2:
            start_date, end_date = date_range
        else:
            start_date, end_date = min_date, max_date

        # Filter
        mask = (df["order_date"].dt.date >= start_date) & (df["order_date"].dt.date <= end_date)
        filtered = df[mask].sort_values("order_date")

        st.markdown("---")
        st.markdown(f"**{len(filtered)} days of data**")
        st.markdown(f"**Updated:** {datetime.now().strftime('%H:%M %d/%m/%Y')}")

        if st.button("Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

        st.markdown("---")
        st.markdown("""
        <div style="text-align:center; color:#8892B0; font-size:0.8rem;">
            <p>Built with ❤️</p>
            <p>Streamlit • BigQuery • dbt</p>
        </div>
        """, unsafe_allow_html=True)

    if filtered.empty:
        st.warning("No data in the selected date range.")
        st.stop()

    # ── KPI Section ─────────────────────────────────────────────────────
    latest = filtered.iloc[-1]
    if len(filtered) >= 2:
        previous = filtered.iloc[-2]
    else:
        previous = latest

    gmv_delta, gmv_dir = calc_delta(latest["gmv_vnd"], previous["gmv_vnd"])
    usd_delta, usd_dir = calc_delta(latest["gmv_usd"], previous["gmv_usd"])
    orders_delta, orders_dir = calc_delta(latest["total_orders"], previous["total_orders"])
    aov_delta, aov_dir = calc_delta(latest["avg_order_value_vnd"], previous["avg_order_value_vnd"])
    cancel_delta, cancel_dir = calc_delta(latest["cancellation_rate_pct"], previous["cancellation_rate_pct"])
    # For cancel rate, direction meaning is inverted (lower is better)
    cancel_dir = "up" if cancel_dir == "down" else ("down" if cancel_dir == "up" else "neutral")

    cols = st.columns(5)
    kpis = [
        ("GMV (VND)", format_vnd(latest["gmv_vnd"]), gmv_delta, gmv_dir, "", " ₫"),
        ("GMV (USD)", format_usd(latest["gmv_usd"]), usd_delta, usd_dir, "", ""),
        ("Orders", f"{int(latest['total_orders']):,}", orders_delta, orders_dir, "", ""),
        ("AOV", format_vnd(latest["avg_order_value_vnd"]), aov_delta, aov_dir, "", " ₫"),
        ("Cancel Rate", f"{latest['cancellation_rate_pct']:.1f}", cancel_delta, cancel_dir, "", "%"),
    ]

    for col, (label, value, delta, direction, prefix, suffix) in zip(cols, kpis):
        with col:
            st.markdown(render_kpi(label, value, delta, direction, prefix, suffix), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Row 1: GMV Trend + Platform Breakdown ───────────────────────────
    st.markdown('<div class="section-title">GMV Trend & Platform Breakdown</div>', unsafe_allow_html=True)

    col1, col2 = st.columns([3, 2])

    with col1:
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)

        fig_gmv = make_subplots(specs=[[{"secondary_y": True}]])

        # GMV VND (bar)
        fig_gmv.add_trace(
            go.Bar(
                x=filtered["order_date"], y=filtered["gmv_vnd"],
                name="GMV (VND)",
                marker=dict(
                    color=filtered["gmv_vnd"],
                    colorscale=[[0, "#667EEA"], [1, "#764BA2"]],
                    cornerradius=4,
                ),
                opacity=0.7,
                hovertemplate="<b>%{x|%d/%m}</b><br>GMV: %{y:,.0f} VND<extra></extra>",
            ),
            secondary_y=False,
        )

        # GMV USD (line)
        fig_gmv.add_trace(
            go.Scatter(
                x=filtered["order_date"], y=filtered["gmv_usd"],
                name="GMV (USD)", mode="lines+markers",
                line=dict(color=COLORS["success"], width=2.5),
                marker=dict(size=5),
                hovertemplate="<b>%{x|%d/%m}</b><br>GMV: $%{y:,.0f}<extra></extra>",
            ),
            secondary_y=True,
        )

        # Highlight event days
        event_days = filtered[filtered["has_major_ecommerce_event"] == True]
        if not event_days.empty:
            fig_gmv.add_trace(
                go.Scatter(
                    x=event_days["order_date"], y=event_days["gmv_vnd"],
                    name="Event Days", mode="markers",
                    marker=dict(color=COLORS["warning"], size=12, symbol="star",
                                line=dict(width=1.5, color="#fff")),
                    hovertemplate="<b>⭐ Event Day</b><br>%{x|%d/%m/%Y}<extra></extra>",
                ),
                secondary_y=False,
            )

        fig_gmv.update_layout(
            **CHART_LAYOUT, title="Daily GMV", height=380,
            barmode="overlay",
        )
        fig_gmv.update_yaxes(title_text="VND", secondary_y=False, gridcolor="rgba(102,126,234,0.08)")
        fig_gmv.update_yaxes(title_text="USD", secondary_y=True, gridcolor="rgba(0,0,0,0)")

        st.plotly_chart(fig_gmv, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)

        platform_cols = ["orders_shopee", "orders_tiki", "orders_lazada", "orders_sendo"]
        platform_labels = ["Shopee", "Tiki", "Lazada", "Sendo"]
        platform_colors = [COLORS["shopee"], COLORS["tiki"], COLORS["lazada"], COLORS["sendo"]]

        platform_totals = [filtered[c].sum() for c in platform_cols]

        fig_platform = go.Figure(data=[
            go.Pie(
                labels=platform_labels,
                values=platform_totals,
                hole=0.55,
                marker=dict(colors=platform_colors, line=dict(color="#0E1117", width=2)),
                textinfo="percent+label",
                textfont=dict(size=12, color="#FAFAFA"),
                hovertemplate="<b>%{label}</b><br>Orders: %{value:,}<br>%{percent}<extra></extra>",
            )
        ])

        fig_platform.update_layout(
            **CHART_LAYOUT, title="Platform Breakdown", height=380,
            showlegend=False,
            annotations=[dict(
                text=f"<b>{sum(platform_totals):,}</b><br>orders",
                x=0.5, y=0.5, font_size=16, font_color="#CCD6F6",
                showarrow=False,
            )],
        )

        st.plotly_chart(fig_platform, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # ── Row 2: Orders Trend + Exchange Rate ─────────────────────────────
    st.markdown('<div class="section-title">Orders & USD/VND Exchange Rate</div>', unsafe_allow_html=True)

    col3, col4 = st.columns(2)

    with col3:
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)

        fig_orders = go.Figure()

        fig_orders.add_trace(go.Scatter(
            x=filtered["order_date"], y=filtered["total_orders"],
            name="Total Orders", fill="tozeroy",
            line=dict(color=COLORS["primary"], width=2),
            fillcolor="rgba(102, 126, 234, 0.15)",
            hovertemplate="<b>%{x|%d/%m}</b><br>Total: %{y:,}<extra></extra>",
        ))

        fig_orders.add_trace(go.Scatter(
            x=filtered["order_date"], y=filtered["revenue_orders"],
            name="Revenue Orders", fill="tozeroy",
            line=dict(color=COLORS["success"], width=2),
            fillcolor="rgba(72, 187, 120, 0.1)",
            hovertemplate="<b>%{x|%d/%m}</b><br>Revenue: %{y:,}<extra></extra>",
        ))

        fig_orders.add_trace(go.Scatter(
            x=filtered["order_date"], y=filtered["cancelled_orders"],
            name="Cancelled",
            line=dict(color=COLORS["danger"], width=2, dash="dot"),
            hovertemplate="<b>%{x|%d/%m}</b><br>Cancelled: %{y:,}<extra></extra>",
        ))

        fig_orders.update_layout(**CHART_LAYOUT, title="Order Trend", height=360)
        st.plotly_chart(fig_orders, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with col4:
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)

        fig_fx = go.Figure()

        fig_fx.add_trace(go.Scatter(
            x=filtered["order_date"], y=filtered["usd_sell_rate"],
            name="USD/VND Sell", mode="lines+markers",
            line=dict(color=COLORS["warning"], width=2.5),
            marker=dict(size=5),
            fill="tozeroy",
            fillcolor="rgba(246, 173, 85, 0.1)",
            hovertemplate="<b>%{x|%d/%m}</b><br>Sell: %{y:,.0f} VND<extra></extra>",
        ))

        fig_fx.update_layout(**CHART_LAYOUT, title="USD/VND Rate (Vietcombank)", height=360)
        fig_fx.update_yaxes(tickformat=",")
        st.plotly_chart(fig_fx, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # ── Row 3: Cancel Rate + News Correlation ───────────────────────────
    st.markdown('<div class="section-title">Cancel/Return Rate & News Correlation</div>', unsafe_allow_html=True)

    col5, col6 = st.columns(2)

    with col5:
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)

        fig_rates = go.Figure()

        fig_rates.add_trace(go.Scatter(
            x=filtered["order_date"], y=filtered["cancellation_rate_pct"],
            name="Cancel Rate %", fill="tozeroy",
            line=dict(color=COLORS["danger"], width=2),
            fillcolor="rgba(252, 129, 129, 0.1)",
            hovertemplate="<b>%{x|%d/%m}</b><br>Cancel: %{y:.1f}%<extra></extra>",
        ))

        fig_rates.add_trace(go.Scatter(
            x=filtered["order_date"], y=filtered["return_rate_pct"],
            name="Return Rate %",
            line=dict(color=COLORS["warning"], width=2, dash="dash"),
            hovertemplate="<b>%{x|%d/%m}</b><br>Return: %{y:.1f}%<extra></extra>",
        ))

        fig_rates.update_layout(**CHART_LAYOUT, title="Cancel & Return Rate", height=360)
        st.plotly_chart(fig_rates, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with col6:
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)

        fig_news = make_subplots(specs=[[{"secondary_y": True}]])

        fig_news.add_trace(
            go.Bar(
                x=filtered["order_date"], y=filtered["news_ecommerce_articles"],
                name="E-commerce News",
                marker=dict(color=COLORS["info"], cornerradius=3),
                opacity=0.6,
                hovertemplate="<b>%{x|%d/%m}</b><br>Articles: %{y}<extra></extra>",
            ),
            secondary_y=False,
        )

        fig_news.add_trace(
            go.Scatter(
                x=filtered["order_date"], y=filtered["gmv_vnd"],
                name="GMV (VND)", mode="lines+markers",
                line=dict(color=COLORS["primary"], width=2),
                marker=dict(size=4),
                hovertemplate="<b>%{x|%d/%m}</b><br>GMV: %{y:,.0f}<extra></extra>",
            ),
            secondary_y=True,
        )

        fig_news.update_layout(**CHART_LAYOUT, title="News vs GMV", height=360)
        fig_news.update_yaxes(title_text="Articles", secondary_y=False, gridcolor="rgba(102,126,234,0.08)")
        fig_news.update_yaxes(title_text="GMV (VND)", secondary_y=True, gridcolor="rgba(0,0,0,0)")

        st.plotly_chart(fig_news, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # ── Row 4: Top Category + Payment Method ────────────────────────────
    st.markdown('<div class="section-title">Additional Insights</div>', unsafe_allow_html=True)

    col7, col8 = st.columns(2)

    with col7:
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        st.markdown("##### Top Category by Day")

        display_df = filtered[["order_date", "top_category_by_gmv", "top_payment_method",
                               "unique_customers"]].copy()
        display_df["order_date"] = display_df["order_date"].dt.strftime("%d/%m/%Y")
        display_df.columns = ["Date", "Top Category", "Top Payment", "Customers"]

        st.dataframe(
            display_df.reset_index(drop=True),
            use_container_width=True,
            height=300,
        )
        st.markdown('</div>', unsafe_allow_html=True)

    with col8:
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)

        # Stacked bar: Platform orders per day
        platform_data = []
        for col_name, label, color in [
            ("orders_shopee", "Shopee", COLORS["shopee"]),
            ("orders_tiki", "Tiki", COLORS["tiki"]),
            ("orders_lazada", "Lazada", COLORS["lazada"]),
            ("orders_sendo", "Sendo", COLORS["sendo"]),
        ]:
            platform_data.append(go.Bar(
                x=filtered["order_date"], y=filtered[col_name],
                name=label, marker_color=color,
                hovertemplate=f"<b>{label}</b><br>" + "%{x|%d/%m}: %{y:,} orders<extra></extra>",
            ))

        fig_stacked = go.Figure(data=platform_data)
        fig_stacked.update_layout(
            **CHART_LAYOUT, title="Orders by Platform/Day",
            height=340, barmode="stack",
        )
        st.plotly_chart(fig_stacked, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # ── Raw Data ────────────────────────────────────────────────────────
    with st.expander("View Raw Data (fact_daily_summary)", expanded=False):
        show_df = filtered.copy()
        show_df["order_date"] = show_df["order_date"].dt.strftime("%d/%m/%Y")
        st.dataframe(show_df, use_container_width=True, height=400)

        csv = show_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download CSV", csv,
            "fact_daily_summary.csv", "text/csv",
            use_container_width=True,
        )

    # ── Footer ──────────────────────────────────────────────────────────
    st.markdown("""
    <div style="text-align:center; padding:2rem 0 1rem; color:#4A5568; font-size:0.8rem;">
        <p>E-commerce Analytics Dashboard • Data pipeline: Airflow + dbt + BigQuery</p>
        <p>Built with Streamlit & Plotly</p>
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
