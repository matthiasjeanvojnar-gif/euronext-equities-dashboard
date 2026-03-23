"""
Storage Utilities — Historical snapshot persistence with Parquet.
"""

import os
import datetime
import pandas as pd
import numpy as np

DATA_DIR = "data"
MARKET_HIST = os.path.join(DATA_DIR, "history_market.parquet")
GROUP_HIST = os.path.join(DATA_DIR, "history_group.parquet")


def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(os.path.join(DATA_DIR, "archive"), exist_ok=True)


# ---------------------------------------------------------------------------
# Save / Load
# ---------------------------------------------------------------------------

def save_market_snapshot(
    snapshot_time: datetime.datetime,
    market_summary: pd.DataFrame,
    fx_rate: float,
    latest_trade: datetime.datetime | None,
):
    """Append market-level snapshot to history (dedup by snapshot_time+market)."""
    ensure_dirs()
    records = market_summary.copy()
    records["snapshot_time"] = snapshot_time
    records["fx_rate"] = fx_rate
    records["latest_trade"] = latest_trade

    keep_cols = [
        "snapshot_time", "market", "market_group", "volume",
        "turnover_native", "turnover_eur", "fx_rate", "latest_trade",
    ]
    for c in keep_cols:
        if c not in records.columns:
            records[c] = np.nan
    records = records[keep_cols]

    if os.path.exists(MARKET_HIST):
        existing = pd.read_parquet(MARKET_HIST)
        combined = pd.concat([existing, records], ignore_index=True)
        combined = combined.drop_duplicates(subset=["snapshot_time", "market"], keep="last")
    else:
        combined = records

    combined.to_parquet(MARKET_HIST, index=False)
    return combined


def save_group_snapshot(
    snapshot_time: datetime.datetime,
    group_summary: pd.DataFrame,
):
    """Append group-level snapshot to history."""
    ensure_dirs()
    records = group_summary.copy()
    records["snapshot_time"] = snapshot_time

    keep_cols = ["snapshot_time", "market_group", "volume", "turnover_eur"]
    for c in keep_cols:
        if c not in records.columns:
            records[c] = np.nan
    records = records[keep_cols]

    if os.path.exists(GROUP_HIST):
        existing = pd.read_parquet(GROUP_HIST)
        combined = pd.concat([existing, records], ignore_index=True)
        combined = combined.drop_duplicates(subset=["snapshot_time", "market_group"], keep="last")
    else:
        combined = records

    combined.to_parquet(GROUP_HIST, index=False)
    return combined


def load_market_history() -> pd.DataFrame | None:
    if os.path.exists(MARKET_HIST):
        df = pd.read_parquet(MARKET_HIST)
        df["snapshot_time"] = pd.to_datetime(df["snapshot_time"])
        return df.sort_values("snapshot_time")
    return None


def load_group_history() -> pd.DataFrame | None:
    if os.path.exists(GROUP_HIST):
        df = pd.read_parquet(GROUP_HIST)
        df["snapshot_time"] = pd.to_datetime(df["snapshot_time"])
        return df.sort_values("snapshot_time")
    return None


# ---------------------------------------------------------------------------
# Time aggregation
# ---------------------------------------------------------------------------

def aggregate_time(df: pd.DataFrame, freq: str, value_cols: list[str], group_col: str | None = None) -> pd.DataFrame:
    """Aggregate historical data by time frequency.

    freq: "Snapshot" | "Hourly" | "Daily" | "Weekly"
    """
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    if freq == "Snapshot":
        return df

    freq_map = {"Hourly": "h", "Daily": "D", "Weekly": "W"}
    pd_freq = freq_map.get(freq, "D")

    df["period"] = df["snapshot_time"].dt.floor(pd_freq) if pd_freq != "W" else df["snapshot_time"].dt.to_period("W").dt.start_time

    group_keys = ["period"]
    if group_col and group_col in df.columns:
        group_keys.append(group_col)

    agg_dict = {c: "sum" for c in value_cols if c in df.columns}
    if not agg_dict:
        return df

    # For time aggregation, take the MAX of each period (latest snapshot)
    # to avoid double-counting intra-period snapshots
    agg_dict = {c: "max" for c in value_cols if c in df.columns}

    result = df.groupby(group_keys, as_index=False).agg(agg_dict)
    result = result.rename(columns={"period": "snapshot_time"})
    return result.sort_values("snapshot_time")
