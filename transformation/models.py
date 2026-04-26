# Database table definitions
from sqlalchemy import Column, Date, DateTime, Index, Integer, Numeric, String, func
from sqlalchemy.orm import declarative_base


Base = declarative_base()


class RawCryptoStream(Base):
    __tablename__ = "raw_crypto_stream"
    __table_args__ = (Index("idx_raw_stream_coin_time", "coin", "event_time"),)

    id = Column(Integer, primary_key=True)
    coin = Column(String(10), nullable=False)
    pair = Column(String(20), nullable=False)
    price_usd = Column(Numeric(18, 6), nullable=False)
    event_time = Column(DateTime(timezone=True), nullable=False)
    source = Column(String(50))
    ingested_at = Column(DateTime(timezone=True), server_default=func.now())


class CryptoStreamEnriched(Base):
    __tablename__ = "crypto_stream_enriched"
    __table_args__ = (Index("idx_enriched_coin_time", "coin", "event_time"),)

    id = Column(Integer, primary_key=True)
    coin = Column(String(10), nullable=False)
    pair = Column(String(20), nullable=False)
    price_usd = Column(Numeric(18, 6), nullable=False)
    vwap_1min = Column(Numeric(18, 6))
    pct_from_vwap = Column(Numeric(10, 4))
    event_time = Column(DateTime(timezone=True), nullable=False)
    processed_at = Column(DateTime(timezone=True), server_default=func.now())


class UnifiedRate(Base):
    __tablename__ = "unified_rates"

    id = Column(Integer, primary_key=True)
    currency_pair = Column(String(20))
    base = Column(String(10))
    quote = Column(String(10))
    rate = Column(Numeric(18, 6))
    prev_rate = Column(Numeric(18, 6))
    pct_change = Column(Numeric(10, 4))
    rate_date = Column(Date)
    source = Column(String(50))
    ingested_at = Column(DateTime(timezone=False))
    transformed_at = Column(DateTime(timezone=False))
