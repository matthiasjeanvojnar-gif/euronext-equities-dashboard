"""
Euronext Equities Dashboard
============================
Internal market monitoring tool.
Bloomberg-inspired dark theme, data-centric, table-first.
"""

import os
import datetime

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

from process_data import (
    parse_euronext_excel,
    compute_market_summary,
    compute_group_summary,
)
from fx_utils import get_fx_rate
from storage_utils import (
    ensure_dirs,
    save_market_snapshot,
    save_group_snapshot,
    load_market_history,
    load_group_history,
    aggregate_time,
)
from download_utils import (
    download_latest_snapshot,
    is_valid_xlsx,
    LATEST_FILE,
    DATA_DIR,
    ARCHIVE_DIR,
)

# ═══════════════════════════════════════════════════════════════════════════
# Page config & theme
# ═══════════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="Euronext Equities Monitor",
    page_icon="◼",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Bloomberg-inspired dark CSS
st.markdown("""
<style>
    /* ── Base ── */
    .stApp {
        background-color: #0a0e17;
        color: #c8cdd3;
    }
    section[data-testid="stSidebar"] {
        background-color: #0d1117;
        border-right: 1px solid #1c2333;
    }
    section[data-testid="stSidebar"] .stMarkdown p,
    section[data-testid="stSidebar"] .stMarkdown label,
    section[data-testid="stSidebar"] .stSelectbox label,
    section[data-testid="stSidebar"] .stRadio label,
    section[data-testid="stSidebar"] .stNumberInput label {
        color: #8b949e !important;
    }

    /* ── Headers ── */
    h1, h2, h3, h4 { color: #e6edf3 !important; font-weight: 600 !important; }
    h1 { font-size: 1.6rem !important; letter-spacing: 0.02em; }
    h2 { font-size: 1.15rem !important; border-bottom: 1px solid #1c2333; padding-bottom: 6px; }
    h3 { font-size: 1.0rem !important; color: #8b949e !important; }

    /* ── KPI metrics ── */
    [data-testid="stMetric"] {
        background: #0d1117;
        border: 1px solid #1c2333;
        border-radius: 4px;
        padding: 12px 16px;
    }
    [data-testid="stMetricLabel"] { color: #8b949e !important; font-size: 0.75rem !important; text-transform: uppercase; letter-spacing: 0.06em; }
    [data-testid="stMetricValue"] { color: #58a6ff !important; font-size: 1.3rem !important; font-weight: 700 !important; font-family: 'JetBrains Mono', 'SF Mono', 'Consolas', monospace !important; }

    /* ── Tables ── */
    .stDataFrame { border: 1px solid #1c2333; border-radius: 4px; }
    .stDataFrame [data-testid="StyledDataFrame"] { font-size: 0.82rem; }

    /* ── Buttons ── */
    .stButton > button {
        background: #1f6feb;
        color: #fff;
        border: none;
        font-weight: 600;
        letter-spacing: 0.03em;
        border-radius: 4px;
        padding: 0.5rem 1.5rem;
        transition: background 0.15s;
    }
    .stButton > button:hover { background: #388bfd; }

    /* ── Dividers ── */
    hr { border-color: #1c2333 !important; }

    /* ── Expander ── */
    details { border: 1px solid #1c2333 !important; border-radius: 4px; }

    /* ── Status pill ── */
    .status-pill {
        display: inline-block;
        padding: 3px 10px;
        border-radius: 3px;
        font-size: 0.75rem;
        font-weight: 600;
        letter-spacing: 0.04em;
        text-transform: uppercase;
    }
    .status-new { background: #1a3a2a; color: #3fb950; }
    .status-same { background: #1a2433; color: #58a6ff; }
    .status-first { background: #2d1a00; color: #d29922; }

    /* ── Compact padding ── */
    .block-container { padding-top: 1.5rem; padding-bottom: 1rem; }

    /* ── Time display ── */
    .time-label {
        font-size: 0.7rem;
        color: #6e7681;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        margin-bottom: 2px;
    }
    .time-value {
        font-size: 0.95rem;
        color: #c8cdd3;
        font-family: 'JetBrains Mono', 'SF Mono', monospace;
    }
</style>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════
# Plotly theme
# ═══════════════════════════════════════════════════════════════════════════

PLOTLY_LAYOUT = dict(
    paper_bgcolor="#0a0e17",
    plot_bgcolor="#0d1117",
    font=dict(color="#8b949e", size=11, family="JetBrains Mono, SF Mono, Consolas, monospace"),
    margin=dict(l=50, r=20, t=40, b=40),
    xaxis=dict(gridcolor="#1c2333", zerolinecolor="#1c2333"),
    yaxis=dict(gridcolor="#1c2333", zerolinecolor="#1c2333"),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=10)),
    colorway=["#58a6ff", "#3fb950", "#d29922", "#f85149", "#bc8cff",
              "#39d2c0", "#ff7b72", "#79c0ff", "#d2a8ff", "#ffa657"],
)


def apply_plotly_theme(fig):
    fig.update_layout(**PLOTLY_LAYOUT)
    return fig


# ═══════════════════════════════════════════════════════════════════════════
# Session state init
# ═══════════════════════════════════════════════════════════════════════════

def init_state():
    defaults = {
        "snapshot_status": None,       # "first" | "new" | "same"
        "last_snapshot_time": None,
        "df": None,
        "snapshot_time": None,
        "market_summary": None,
        "group_summary": None,
        "latest_trade": None,
        "refresh_time": None,
        "fx_info": None,
        "download_method": None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

init_state()


# ═══════════════════════════════════════════════════════════════════════════
# Core refresh logic (uses download_utils pipeline)
# ═══════════════════════════════════════════════════════════════════════════

def refresh_data(fx_info: dict):
    """Download, validate, parse, compute, store."""
    status_area = st.empty()

    def _progress(msg: str):
        status_area.info(f"⟳ {msg}")

    # ── 1. Download with validation ──
    _progress("Starting download…")
    result = download_latest_snapshot(progress_callback=_progress)

    if not result.ok:
        status_area.empty()
        st.error(result.error or "Download failed.")
        return

    if result.method == "cache":
        st.warning("⚠ " + (result.error or "Using last available valid snapshot."))

    filepath = result.filepath
    st.session_state["download_method"] = result.method

    # ── 2. Final validation gate (belt & suspenders) ──
    if not is_valid_xlsx(filepath):
        status_area.empty()
        st.error(
            "Downloaded file is not a valid Excel (.xlsx) file. "
            "The response may have been an HTML page or redirect."
        )
        return

    # ── 3. Parse ──
    _progress("Processing data…")
    try:
        df, snapshot_time = parse_euronext_excel(filepath)
    except Exception as e:
        status_area.empty()
        st.error(f"Parsing error: {e}")
        return

    if df.empty:
        status_area.empty()
        st.error("Parsed file contains no instrument data.")
        return

    if snapshot_time is None:
        snapshot_time = datetime.datetime.now()

    # Snapshot status
    prev = st.session_state.get("last_snapshot_time")
    if prev is None:
        st.session_state["snapshot_status"] = "first"
    elif prev == snapshot_time:
        st.session_state["snapshot_status"] = "same"
    else:
        st.session_state["snapshot_status"] = "new"
    st.session_state["last_snapshot_time"] = snapshot_time

    # ── 4. Compute ──
    fx_rate = fx_info["rate"]
    market_summary = compute_market_summary(df, fx_rate)
    group_summary = compute_group_summary(market_summary)

    # Latest trade
    if "last_trade_mic_time_parsed" in df.columns:
        valid = df["last_trade_mic_time_parsed"].dropna()
        latest_trade = valid.max() if len(valid) > 0 else None
    else:
        latest_trade = None

    # ── 5. Store history ──
    try:
        save_market_snapshot(snapshot_time, market_summary, fx_rate, latest_trade)
        save_group_snapshot(snapshot_time, group_summary)
    except Exception:
        pass  # non-critical

    # ── 6. Update state ──
    st.session_state["df"] = df
    st.session_state["snapshot_time"] = snapshot_time
    st.session_state["market_summary"] = market_summary
    st.session_state["group_summary"] = group_summary
    st.session_state["latest_trade"] = latest_trade
    st.session_state["refresh_time"] = datetime.datetime.now()
    st.session_state["fx_info"] = fx_info

    status_area.empty()
    method_label = {"direct": "Direct HTTP", "playwright": "Browser export", "cache": "Cached file"}
    st.success(f"✓ Data loaded via {method_label.get(result.method, result.method)}")


# ═══════════════════════════════════════════════════════════════════════════
# Formatting helpers
# ═══════════════════════════════════════════════════════════════════════════

def fmt_number(n, decimals=0):
    if pd.isna(n) or n is None:
        return "—"
    if abs(n) >= 1_000_000_000:
        return f"{n/1_000_000_000:,.{decimals}f}B"
    if abs(n) >= 1_000_000:
        return f"{n/1_000_000:,.{decimals}f}M"
    if abs(n) >= 1_000:
        return f"{n/1_000:,.{decimals}f}K"
    return f"{n:,.{decimals}f}"


def fmt_ts(ts):
    if ts is None or (isinstance(ts, float) and np.isnan(ts)):
        return "—"
    if isinstance(ts, datetime.datetime):
        return ts.strftime("%Y-%m-%d %H:%M:%S CET")
    return str(ts)


def status_pill(status):
    cls_map = {"first": "status-first", "new": "status-new", "same": "status-same"}
    label_map = {"first": "First Snapshot", "new": "New Snapshot", "same": "No New Data"}
    cls = cls_map.get(status, "status-same")
    label = label_map.get(status, status or "—")
    return f'<span class="status-pill {cls}">{label}</span>'


# ═══════════════════════════════════════════════════════════════════════════
# Filter helpers
# ═══════════════════════════════════════════════════════════════════════════

def apply_scope_filter(df: pd.DataFrame, scope: str) -> pd.DataFrame:
    if scope == "Core":
        return df[df["market_group"] == "Core"]
    elif scope == "Core + Growth":
        return df[df["market_group"].isin(["Core", "Growth"])]
    return df


# ═══════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("### ◼ EURONEXT MONITOR")
    st.markdown("---")

    # Refresh button
    refresh_clicked = st.button("⟳  Refresh Data", use_container_width=True, type="primary")

    st.markdown("---")
    st.markdown("##### FILTERS")

    scope = st.radio("Scope", ["Core", "Core + Growth", "All"], index=2, horizontal=True)

    # Market filter (populated after data load)
    market_options = []
    if st.session_state["market_summary"] is not None:
        market_options = sorted(st.session_state["market_summary"]["market"].unique().tolist())
    selected_markets = st.multiselect("Markets", market_options, default=market_options)

    time_agg = st.selectbox("Time Aggregation", ["Snapshot", "Hourly", "Daily", "Weekly"], index=0)

    st.markdown("---")
    st.markdown("##### FX — EUR/NOK")

    fx_mode = st.radio("FX Mode", ["Auto", "Manual"], horizontal=True)
    manual_fx = None
    if fx_mode == "Manual":
        manual_fx = st.number_input("EUR per 1 NOK", value=0.0875, format="%.6f", step=0.0001)

    fx_info = get_fx_rate(fx_mode, manual_fx)

    st.markdown(f"""
    <div style="font-size:0.78rem; color:#6e7681; margin-top:6px;">
        Rate: <span style="color:#58a6ff;">{fx_info['rate']:.6f}</span><br>
        Date: {fx_info['date']}<br>
        Source: {fx_info['source']}
    </div>
    """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════
# TRIGGER REFRESH
# ═══════════════════════════════════════════════════════════════════════════

if refresh_clicked:
    refresh_data(fx_info)


# ═══════════════════════════════════════════════════════════════════════════
# HEADER
# ═══════════════════════════════════════════════════════════════════════════

st.markdown("# ◼ EURONEXT EQUITIES MONITOR")

# Time bar
col_t1, col_t2, col_t3, col_t4 = st.columns(4)

with col_t1:
    st.markdown(f"""
    <div class="time-label">Snapshot Time (Euronext)</div>
    <div class="time-value">{fmt_ts(st.session_state.get('snapshot_time'))}</div>
    """, unsafe_allow_html=True)

with col_t2:
    st.markdown(f"""
    <div class="time-label">Latest Trade in Snapshot</div>
    <div class="time-value">{fmt_ts(st.session_state.get('latest_trade'))}</div>
    """, unsafe_allow_html=True)

with col_t3:
    st.markdown(f"""
    <div class="time-label">Dashboard Refreshed</div>
    <div class="time-value">{fmt_ts(st.session_state.get('refresh_time'))}</div>
    """, unsafe_allow_html=True)

with col_t4:
    status = st.session_state.get("snapshot_status")
    st.markdown(f"""
    <div class="time-label">Status</div>
    <div style="margin-top:2px;">{status_pill(status)}</div>
    """, unsafe_allow_html=True)

st.markdown("---")


# ═══════════════════════════════════════════════════════════════════════════
# MAIN CONTENT
# ═══════════════════════════════════════════════════════════════════════════

if st.session_state["df"] is None:
    st.markdown("""
    <div style="text-align:center; padding:60px 20px; color:#6e7681;">
        <div style="font-size:2rem; margin-bottom:12px;">◼</div>
        <div style="font-size:1.1rem; margin-bottom:6px;">No data loaded</div>
        <div style="font-size:0.85rem;">Click <b>Refresh Data</b> in the sidebar to download the latest Euronext snapshot.</div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()


# ── Apply filters ──
ms = st.session_state["market_summary"].copy()
ms = apply_scope_filter(ms, scope)
if selected_markets:
    ms = ms[ms["market"].isin(selected_markets)]

gs = compute_group_summary(ms)

# Recompute pct_share after filter
total_eur = ms["turnover_eur"].sum()
ms["pct_share"] = (ms["turnover_eur"] / total_eur * 100).round(2) if total_eur > 0 else 0.0


# ═══════════════════════════════════════════════════════════════════════════
# KPIs
# ═══════════════════════════════════════════════════════════════════════════

k1, k2, k3, k4 = st.columns(4)
k1.metric("Total Volume", fmt_number(ms["volume"].sum()))
k2.metric("Total Turnover (EUR)", fmt_number(ms["turnover_eur"].sum(), 1))

# Oslo NOK
oslo_rows = ms[ms["currency"] == "NOK"]
oslo_native = oslo_rows["turnover_native"].sum() if len(oslo_rows) > 0 else 0
k3.metric("Oslo Turnover (NOK)", fmt_number(oslo_native, 1))

total_instruments = ms["instruments"].sum() if "instruments" in ms.columns else 0
k4.metric("Instruments", f"{int(total_instruments):,}")


# ═══════════════════════════════════════════════════════════════════════════
# CURRENT SNAPSHOT — Tables
# ═══════════════════════════════════════════════════════════════════════════

st.markdown("## Current Snapshot")

tab_market, tab_group = st.tabs(["By Market", "By Group"])

with tab_market:
    display_ms = ms[["market", "market_group", "volume", "turnover_native", "turnover_eur", "pct_share"]].copy()
    display_ms.columns = ["Market", "Group", "Volume", "Turnover (Native)", "Turnover (EUR)", "% Share"]
    st.dataframe(
        display_ms.style.format({
            "Volume": "{:,.0f}",
            "Turnover (Native)": "{:,.0f}",
            "Turnover (EUR)": "{:,.0f}",
            "% Share": "{:.2f}%",
        }),
        use_container_width=True,
        hide_index=True,
        height=min(400, 36 + 35 * len(display_ms)),
    )

with tab_group:
    display_gs = gs[["market_group", "volume", "turnover_eur"]].copy()
    display_gs.columns = ["Group", "Volume", "Turnover (EUR)"]
    st.dataframe(
        display_gs.style.format({
            "Volume": "{:,.0f}",
            "Turnover (EUR)": "{:,.0f}",
        }),
        use_container_width=True,
        hide_index=True,
    )


# ═══════════════════════════════════════════════════════════════════════════
# CURRENT SNAPSHOT — Charts
# ═══════════════════════════════════════════════════════════════════════════

st.markdown("## Market Breakdown")

ch1, ch2 = st.columns(2)

with ch1:
    fig = px.bar(
        ms.sort_values("turnover_eur", ascending=True),
        x="turnover_eur", y="market", orientation="h",
        title="Turnover (EUR) by Market",
        color="market_group",
    )
    apply_plotly_theme(fig)
    fig.update_layout(height=max(300, 28 * len(ms)), showlegend=True, yaxis_title="")
    st.plotly_chart(fig, use_container_width=True)

with ch2:
    fig = px.bar(
        ms.sort_values("volume", ascending=True),
        x="volume", y="market", orientation="h",
        title="Volume by Market",
        color="market_group",
    )
    apply_plotly_theme(fig)
    fig.update_layout(height=max(300, 28 * len(ms)), showlegend=True, yaxis_title="")
    st.plotly_chart(fig, use_container_width=True)

# Group pie
fig_pie = px.pie(
    gs, values="turnover_eur", names="market_group",
    title="Turnover Share by Group",
    hole=0.45,
)
apply_plotly_theme(fig_pie)
fig_pie.update_layout(height=350)
fig_pie.update_traces(textinfo="label+percent", textfont_size=12)
st.plotly_chart(fig_pie, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════
# HISTORICAL
# ═══════════════════════════════════════════════════════════════════════════

st.markdown("---")
st.markdown("## Historical Trends")
st.caption("Charts improve as more snapshots are collected over time.")

mkt_hist = load_market_history()
grp_hist = load_group_history()

if mkt_hist is not None and len(mkt_hist) > 0:
    # Apply scope filter to history
    mkt_hist = apply_scope_filter(mkt_hist, scope)
    if selected_markets:
        mkt_hist = mkt_hist[mkt_hist["market"].isin(selected_markets)]

    # Total aggregation
    total_hist = mkt_hist.groupby("snapshot_time", as_index=False).agg(
        volume=("volume", "sum"),
        turnover_eur=("turnover_eur", "sum"),
    )
    total_hist = aggregate_time(total_hist, time_agg, ["volume", "turnover_eur"])

    h1, h2 = st.columns(2)

    with h1:
        if not total_hist.empty:
            fig = px.line(total_hist, x="snapshot_time", y="volume", title="Total Volume")
            apply_plotly_theme(fig)
            fig.update_traces(line=dict(width=2))
            fig.update_layout(height=320)
            st.plotly_chart(fig, use_container_width=True)

    with h2:
        if not total_hist.empty:
            fig = px.line(total_hist, x="snapshot_time", y="turnover_eur", title="Total Turnover (EUR)")
            apply_plotly_theme(fig)
            fig.update_traces(line=dict(width=2, color="#3fb950"))
            fig.update_layout(height=320)
            st.plotly_chart(fig, use_container_width=True)

    # By market over time
    by_mkt = aggregate_time(mkt_hist, time_agg, ["volume", "turnover_eur"], group_col="market")
    if not by_mkt.empty:
        st.markdown("### By Market")
        bm1, bm2 = st.columns(2)
        with bm1:
            fig = px.line(by_mkt, x="snapshot_time", y="turnover_eur", color="market", title="Turnover by Market")
            apply_plotly_theme(fig)
            fig.update_layout(height=380)
            st.plotly_chart(fig, use_container_width=True)
        with bm2:
            fig = px.line(by_mkt, x="snapshot_time", y="volume", color="market", title="Volume by Market")
            apply_plotly_theme(fig)
            fig.update_layout(height=380)
            st.plotly_chart(fig, use_container_width=True)

    # By group over time
    if grp_hist is not None and len(grp_hist) > 0:
        grp_hist_f = apply_scope_filter(grp_hist, scope)
        by_grp = aggregate_time(grp_hist_f, time_agg, ["volume", "turnover_eur"], group_col="market_group")
        if not by_grp.empty:
            st.markdown("### By Group")
            bg1, bg2 = st.columns(2)
            with bg1:
                fig = px.line(by_grp, x="snapshot_time", y="turnover_eur", color="market_group", title="Turnover by Group")
                apply_plotly_theme(fig)
                fig.update_layout(height=350)
                st.plotly_chart(fig, use_container_width=True)
            with bg2:
                fig = px.line(by_grp, x="snapshot_time", y="volume", color="market_group", title="Volume by Group")
                apply_plotly_theme(fig)
                fig.update_layout(height=350)
                st.plotly_chart(fig, use_container_width=True)

    n_snapshots = mkt_hist["snapshot_time"].nunique()
    st.caption(f"Historical data: {n_snapshots} snapshot(s) stored.")
else:
    st.info("No historical data yet. Refresh multiple times to build history.")


# ═══════════════════════════════════════════════════════════════════════════
# RAW DATA
# ═══════════════════════════════════════════════════════════════════════════

st.markdown("---")

with st.expander("Raw Cleaned Data"):
    df_display = st.session_state["df"]
    if df_display is not None:
        st.dataframe(df_display, use_container_width=True, height=400)

with st.expander("Historical Market Data"):
    if mkt_hist is not None:
        st.dataframe(mkt_hist, use_container_width=True, height=400)
    else:
        st.write("No historical data.")

with st.expander("Historical Group Data"):
    if grp_hist is not None:
        st.dataframe(grp_hist, use_container_width=True, height=400)
    else:
        st.write("No historical data.")


# ═══════════════════════════════════════════════════════════════════════════
# Footer
# ═══════════════════════════════════════════════════════════════════════════

st.markdown("---")
st.markdown("""
<div style="text-align:center; color:#30363d; font-size:0.7rem; padding:10px 0;">
    EURONEXT EQUITIES MONITOR — Internal Use Only — Data: live.euronext.com
</div>
""", unsafe_allow_html=True)
