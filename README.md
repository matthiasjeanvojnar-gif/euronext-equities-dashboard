# Euronext Equities Monitor

Internal market monitoring dashboard for Euronext Live equities data.  
Bloomberg-inspired dark theme, data-centric, table-first design.

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run the dashboard
streamlit run app.py
```

The app opens at `http://localhost:8501`.

## Usage

1. Click **⟳ Refresh Data** in the sidebar to download the latest Euronext snapshot
2. The dashboard auto-processes and displays current market data
3. Each refresh appends to historical storage — charts improve over time
4. Use sidebar filters (Scope, Markets, Time Aggregation) to slice the data

## FX Handling

- **Auto mode**: Fetches daily EUR/NOK rate from the ECB API
- **Manual mode**: Enter your own EUR per 1 NOK rate in the sidebar

## Project Structure

```
├── app.py              # Main Streamlit dashboard
├── process_data.py     # Excel parsing, cleaning, aggregation
├── fx_utils.py         # EUR/NOK FX rate fetching
├── storage_utils.py    # Parquet-based historical persistence
├── requirements.txt    # Python dependencies
├── .streamlit/
│   └── config.toml     # Dark theme configuration
└── data/               # Auto-created on first run
    ├── latest_equities.xlsx
    ├── archive/
    ├── history_market.parquet
    └── history_group.parquet
```

## Data Source

All data comes from: https://live.euronext.com/en/products/equities/list  
Excel export downloaded automatically on each refresh.

## Notes

- Historical data is stored in Parquet format (fast, compact)
- Duplicate snapshots are automatically de-duplicated
- If download fails, the app falls back to the last cached file
- Time aggregation (Hourly/Daily/Weekly) uses the max value per period
