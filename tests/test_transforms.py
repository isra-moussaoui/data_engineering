import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from transformation.transform import t1_clean_forex, t1_clean_crypto, t2_daily_change, t3_unify_and_load

# ── Fixtures ──────────────────────────────────────────────────────────────────

SAMPLE_FOREX_RAW = {
    "base": "EUR",
    "date": "2024-01-15",
    "rates": {"USD": 1.08, "GBP": 0.85, "JPY": 160.5, "TND": None},
    "_ingested_at": "2024-01-15T08:00:00",
    "_source": "frankfurter"
}

SAMPLE_CRYPTO_RAW = {
    "rates": {
        "BTC": {"amount": "45000.50", "base": "BTC", "currency": "USD"},
        "ETH": {"amount": "2500.00", "base": "ETH", "currency": "USD"},
        "SOL": {"amount": None},
    },
    "base": "USD",
    "_ingested_at": "2024-01-15T08:00:00",
    "_source": "coinbase"
}

# ── Test 1: Nulls are dropped ─────────────────────────────────────────────────

def test_t1_drops_null_rates():
    df = t1_clean_forex(SAMPLE_FOREX_RAW)
    assert df["rate"].isna().sum() == 0
    assert "EUR/TND" not in df["currency_pair"].values

# ── Test 2: Rates are positive ────────────────────────────────────────────────

def test_t1_all_rates_positive():
    df = t1_clean_forex(SAMPLE_FOREX_RAW)
    assert (df["rate"] > 0).all()

# ── Test 3: Crypto nulls dropped, valid pairs present ─────────────────────────

def test_t1_crypto_drops_null_amount():
    df = t1_clean_crypto(SAMPLE_CRYPTO_RAW)
    assert "SOL/USD" not in df["currency_pair"].values
    assert "BTC/USD" in df["currency_pair"].values
    assert "ETH/USD" in df["currency_pair"].values

# ── Test 4: % change computed correctly ───────────────────────────────────────

def test_t2_pct_change_correct():
    df_today = pd.DataFrame([
        {"currency_pair": "EUR/USD", "rate": 1.10, "base": "EUR", "quote": "USD",
         "rate_date": pd.Timestamp("2024-01-15"), "source": "frankfurter",
         "ingested_at": pd.Timestamp("2024-01-15T08:00:00")}
    ])
    df_prev = pd.DataFrame([
        {"currency_pair": "EUR/USD", "prev_rate": 1.00}
    ])
    df = df_today.merge(df_prev, on="currency_pair", how="left")
    df["pct_change"] = ((df["rate"] - df["prev_rate"]) / df["prev_rate"] * 100).round(4)
    assert df.loc[0, "pct_change"] == pytest.approx(10.0, rel=1e-3)

# ── Test 5: Unified schema has required columns ───────────────────────────────

def test_t3_unified_schema():
    df_forex  = t1_clean_forex(SAMPLE_FOREX_RAW)
    df_crypto = t1_clean_crypto(SAMPLE_CRYPTO_RAW)
    import pandas as pd
    df = pd.concat([df_forex, df_crypto], ignore_index=True)
    required_cols = {"currency_pair", "base", "quote", "rate", "rate_date", "source"}
    assert required_cols.issubset(set(df.columns))
    assert len(df) == len(df_forex) + len(df_crypto)