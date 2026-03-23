"""
FX Utilities — EUR/NOK rate fetching and fallback logic.
"""

import datetime
import requests
import streamlit as st


# ---------------------------------------------------------------------------
# Public API — ECB Statistical Data Warehouse (free, no key)
# Returns EUR/NOK daily reference rate.
# ---------------------------------------------------------------------------

ECB_URL = (
    "https://data-api.ecb.europa.eu/service/data/EXR/"
    "D.NOK.EUR.SP00.A?lastNObservations=5&format=csvdata"
)

FALLBACK_RATE = 0.0875  # ~11.43 NOK per EUR → 1/11.43


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_ecb_eurnok() -> tuple[float | None, str | None]:
    """Fetch latest ECB EUR/NOK rate.

    Returns (eur_per_nok, date_str) or (None, None) on failure.
    The ECB quotes NOK per 1 EUR; we invert to get EUR per 1 NOK.
    """
    try:
        resp = requests.get(ECB_URL, timeout=15)
        resp.raise_for_status()
        lines = resp.text.strip().splitlines()
        if len(lines) < 2:
            return None, None
        # CSV: last line is most recent observation
        last_line = lines[-1]
        parts = last_line.split(",")
        # Find OBS_VALUE and TIME_PERIOD columns from header
        header = lines[0].split(",")
        time_idx = header.index("TIME_PERIOD") if "TIME_PERIOD" in header else None
        obs_idx = header.index("OBS_VALUE") if "OBS_VALUE" in header else None
        if time_idx is None or obs_idx is None:
            return None, None
        nok_per_eur = float(parts[obs_idx])
        eur_per_nok = 1.0 / nok_per_eur
        date_str = parts[time_idx]
        return eur_per_nok, date_str
    except Exception:
        return None, None


def get_fx_rate(mode: str, manual_rate: float | None = None) -> dict:
    """Return FX info dict.

    Parameters
    ----------
    mode : "Auto" | "Manual"
    manual_rate : EUR per 1 NOK (used when mode == "Manual")

    Returns
    -------
    dict with keys: rate, date, source, ok
    """
    if mode == "Manual" and manual_rate is not None and manual_rate > 0:
        return {
            "rate": manual_rate,
            "date": datetime.date.today().isoformat(),
            "source": "Manual input",
            "ok": True,
        }

    rate, date_str = fetch_ecb_eurnok()
    if rate is not None:
        return {
            "rate": rate,
            "date": date_str,
            "source": "ECB (auto)",
            "ok": True,
        }

    # Fallback
    return {
        "rate": FALLBACK_RATE,
        "date": "N/A",
        "source": "Hardcoded fallback",
        "ok": False,
    }
