"""
Data Processing — Parse Euronext Live equities Excel export.

File layout:
  Row 1  → column headers
  Row 2  → "European Equities" label (skip)
  Row 3  → snapshot timestamp
  Row 4  → secondary label (skip)
  Row 5+ → instrument data
"""

import re
import datetime
import pandas as pd
import numpy as np


# ---------------------------------------------------------------------------
# Column name normalisation map
# ---------------------------------------------------------------------------

COL_MAP = {
    "name": "name",
    "isin": "isin",
    "symbol": "symbol",
    "market": "market",
    "currency": "currency",
    "open": "open",
    "high": "high",
    "low": "low",
    "last": "last",
    "last date/time": "last_datetime",
    "volume": "volume",
    "turnover": "turnover",
    "mic": "mic",
    "trading date/time": "trading_datetime",
    "last trade mic time": "last_trade_mic_time",
}


# ---------------------------------------------------------------------------
# Market grouping
# ---------------------------------------------------------------------------

CORE_MARKETS = {
    "euronext paris",
    "euronext amsterdam",
    "euronext brussels",
    "euronext dublin",
    "euronext lisbon",
    "euronext milan",
    "oslo børs",
    "oslo bors",
    "euronext oslo",
}


def classify_market(market: str) -> str:
    """Return market_group label."""
    ml = market.strip().lower()
    if ml in CORE_MARKETS:
        return "Core"
    if "growth" in ml:
        return "Growth"
    return "Extended"


# ---------------------------------------------------------------------------
# Timestamp extraction
# ---------------------------------------------------------------------------

def extract_snapshot_time(filepath: str) -> datetime.datetime | None:
    """Read row 3 of the Excel to extract the snapshot timestamp."""
    try:
        raw = pd.read_excel(filepath, header=None, nrows=4, engine="openpyxl")
        # Row index 2 (0-based) contains the timestamp
        for idx in [2, 1, 3]:
            cell = str(raw.iloc[idx, 0]) if idx < len(raw) else ""
            ts = _parse_timestamp(cell)
            if ts is not None:
                return ts
        # Try all cells in first 4 rows
        for r in range(min(4, len(raw))):
            for c in range(min(5, raw.shape[1])):
                cell = str(raw.iloc[r, c])
                ts = _parse_timestamp(cell)
                if ts is not None:
                    return ts
    except Exception:
        pass
    return None


def _parse_timestamp(text: str) -> datetime.datetime | None:
    """Try multiple formats to parse a timestamp string."""
    text = text.strip()
    if not text or text == "nan":
        return None

    # Common Euronext patterns
    patterns = [
        r"(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2})",
        r"(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})",
        r"(\d{2}-\d{2}-\d{4}\s+\d{2}:\d{2}:\d{2})",
        r"(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2})",
        r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})",
    ]
    fmts = [
        "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%d-%m-%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%Y-%m-%dT%H:%M:%S",
    ]
    for pat, fmt in zip(patterns, fmts):
        m = re.search(pat, text)
        if m:
            try:
                return datetime.datetime.strptime(m.group(1), fmt)
            except ValueError:
                continue

    # Pandas fallback
    try:
        return pd.to_datetime(text, dayfirst=True).to_pydatetime()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Main parser
# ---------------------------------------------------------------------------

def parse_euronext_excel(filepath: str) -> tuple[pd.DataFrame, datetime.datetime | None]:
    """Parse the Euronext equities export.

    Returns (dataframe, snapshot_time).
    """
    snapshot_time = extract_snapshot_time(filepath)

    # Read with row 0 as header, then skip metadata rows
    df = pd.read_excel(filepath, header=0, engine="openpyxl")

    # Drop first 3 data rows (indices 0-2) which are metadata
    # But we need to be adaptive — find where real data starts
    df = _drop_metadata_rows(df)

    # Normalise column names
    df.columns = [_normalise_col(c) for c in df.columns]

    # Ensure critical columns
    for col in ["volume", "turnover", "market"]:
        if col not in df.columns:
            df[col] = np.nan if col != "market" else "Unknown"

    # Clean numeric
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype(np.int64)
    df["turnover"] = pd.to_numeric(df["turnover"], errors="coerce").fillna(0.0)

    # Currency
    if "currency" not in df.columns:
        df["currency"] = "EUR"
    df["currency"] = df["currency"].astype(str).str.strip().str.upper()

    # Market group
    df["market"] = df["market"].astype(str).str.strip()
    df["market_group"] = df["market"].apply(classify_market)

    # Parse last trade time
    if "last_trade_mic_time" in df.columns:
        df["last_trade_mic_time_parsed"] = pd.to_datetime(
            df["last_trade_mic_time"], errors="coerce", dayfirst=True
        )
    elif "last_datetime" in df.columns:
        df["last_trade_mic_time_parsed"] = pd.to_datetime(
            df["last_datetime"], errors="coerce", dayfirst=True
        )
    elif "trading_datetime" in df.columns:
        df["last_trade_mic_time_parsed"] = pd.to_datetime(
            df["trading_datetime"], errors="coerce", dayfirst=True
        )
    else:
        df["last_trade_mic_time_parsed"] = pd.NaT

    # Drop rows with no market/ISIN (leftover junk)
    if "isin" in df.columns:
        df = df[df["isin"].notna() & (df["isin"].astype(str).str.len() > 5)]
    else:
        df = df[df["market"].astype(str).str.len() > 2]

    df = df.reset_index(drop=True)
    return df, snapshot_time


def _drop_metadata_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Drop the metadata rows that sit between the header and actual data."""
    # Strategy: find first row where at least 3 columns have non-null usable data
    # Typically rows 0-2 are metadata ("European Equities", timestamp, secondary label)
    drop_count = 0
    for i in range(min(5, len(df))):
        row = df.iloc[i]
        # Check if this row looks like instrument data (has an ISIN-like value somewhere)
        row_str = " ".join(str(v) for v in row.values)
        # ISIN pattern: 2 letters + 10 alphanumeric
        if re.search(r"[A-Z]{2}[A-Z0-9]{10}", row_str):
            break
        drop_count = i + 1
    if drop_count > 0:
        df = df.iloc[drop_count:].reset_index(drop=True)
    return df


def _normalise_col(name: str) -> str:
    """Lowercase, strip, map to canonical name."""
    n = str(name).strip().lower()
    return COL_MAP.get(n, n.replace(" ", "_").replace("/", "_"))


# ---------------------------------------------------------------------------
# Aggregation helpers
# ---------------------------------------------------------------------------

def compute_market_summary(df: pd.DataFrame, fx_rate: float) -> pd.DataFrame:
    """Aggregate by market."""
    agg = (
        df.groupby(["market", "market_group", "currency"], as_index=False)
        .agg(
            volume=("volume", "sum"),
            turnover_native=("turnover", "sum"),
            instruments=("volume", "size"),
        )
    )
    agg["turnover_eur"] = agg.apply(
        lambda r: r["turnover_native"] * fx_rate if r["currency"] == "NOK" else r["turnover_native"],
        axis=1,
    )
    total_eur = agg["turnover_eur"].sum()
    agg["pct_share"] = (agg["turnover_eur"] / total_eur * 100).round(2) if total_eur > 0 else 0.0
    return agg.sort_values("turnover_eur", ascending=False).reset_index(drop=True)


def compute_group_summary(market_summary: pd.DataFrame) -> pd.DataFrame:
    """Aggregate by market_group."""
    return (
        market_summary.groupby("market_group", as_index=False)
        .agg(
            volume=("volume", "sum"),
            turnover_eur=("turnover_eur", "sum"),
            instruments=("instruments", "sum"),
        )
        .sort_values("turnover_eur", ascending=False)
        .reset_index(drop=True)
    )
