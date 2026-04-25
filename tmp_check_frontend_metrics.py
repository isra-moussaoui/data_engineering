import sys
from pathlib import Path

root = Path(__file__).resolve().parent
sys.path.insert(0, str(root / "frontEnd-Streamlit"))

from services.db import (
    fetch_dashboard_overview,
    fetch_batch_series,
    fetch_batch_snapshot,
)

print("overview", fetch_dashboard_overview())
snapshot = fetch_batch_snapshot(limit=20)
print("snapshot_pairs", snapshot["currency_pair"].tolist())
print("gbpusd_rows_30d", len(fetch_batch_series("GBP/USD", 30)))
print("usdjpy_rows_30d", len(fetch_batch_series("USD/JPY", 30)))
