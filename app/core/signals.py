"""
Trading signal generation and indicator utilities.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List

import pandas as pd


@dataclass
class Signal:
    date: pd.Timestamp
    price: float
    action: str
    confidence: float
    reasons: List[str]


def _compute_rsi(series: pd.Series, window: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / window, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / window, adjust=False).mean()
    rs = avg_gain / (avg_loss + 1e-9)
    return 100 - (100 / (1 + rs))


def compute_indicators(data: pd.DataFrame) -> pd.DataFrame:
    df = data.copy()
    df["ma_fast"] = df["close"].rolling(window=10, min_periods=10).mean()
    df["ma_slow"] = df["close"].rolling(window=30, min_periods=30).mean()
    df["ema_fast"] = df["close"].ewm(span=12, adjust=False).mean()
    df["ema_slow"] = df["close"].ewm(span=26, adjust=False).mean()
    df["macd"] = df["ema_fast"] - df["ema_slow"]
    df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
    df["macd_hist"] = df["macd"] - df["macd_signal"]
    df["rsi"] = _compute_rsi(df["close"])
    df["vol_ma"] = df["volume"].rolling(window=20, min_periods=20).mean()
    return df


def generate_trade_signals(data: pd.DataFrame) -> pd.DataFrame:
    df = compute_indicators(data)
    df["buy_signal"] = (
        (df["ma_fast"] > df["ma_slow"])
        & (df["ma_fast"].shift(1) <= df["ma_slow"].shift(1))
        & (df["macd_hist"] > 0)
        & (df["rsi"] < 65)
    )
    df["sell_signal"] = (
        (df["ma_fast"] < df["ma_slow"])
        & (df["ma_fast"].shift(1) >= df["ma_slow"].shift(1))
        & (df["macd_hist"] < 0)
        & (df["rsi"] > 35)
    )
    df["signal"] = None
    df.loc[df["buy_signal"], "signal"] = "买入"
    df.loc[df["sell_signal"], "signal"] = "卖出"

    df["confidence"] = 0.0
    df.loc[df["buy_signal"], "confidence"] = df.loc[df["buy_signal"], "rsi"].apply(
        lambda r: max(0.4, min(0.9, (70 - r) / 40))
    )
    df.loc[df["sell_signal"], "confidence"] = df.loc[df["sell_signal"], "rsi"].apply(
        lambda r: max(0.4, min(0.9, (r - 30) / 40))
    )

    df["reasons"] = [[] for _ in range(len(df))]

    def _append_reason(mask: pd.Series, reason: str) -> None:
        df.loc[mask, "reasons"] = df.loc[mask, "reasons"].apply(lambda arr: arr + [reason])

    _append_reason(df["buy_signal"], "短期均线上穿长期均线")
    _append_reason(df["buy_signal"] & (df["macd_hist"] > 0), "MACD动能转正")
    _append_reason(df["buy_signal"] & (df["rsi"] < 40), "RSI处于相对低位")

    _append_reason(df["sell_signal"], "短期均线下穿长期均线")
    _append_reason(df["sell_signal"] & (df["macd_hist"] < 0), "MACD动能转弱")
    _append_reason(df["sell_signal"] & (df["rsi"] > 60), "RSI处于相对高位")

    return df


def summarize_signals(df: pd.DataFrame) -> dict:
    latest = df.dropna(subset=["signal"]).tail(5)
    buy_points = latest[latest["signal"] == "买入"]
    sell_points = latest[latest["signal"] == "卖出"]

    summary = {
        "recent_buys": [
            Signal(
                date=row["date"],
                price=row["close"],
                action=row["signal"],
                confidence=float(row["confidence"]),
                reasons=list(row["reasons"]),
            )
            for _, row in buy_points.iterrows()
        ],
        "recent_sells": [
            Signal(
                date=row["date"],
                price=row["close"],
                action=row["signal"],
                confidence=float(row["confidence"]),
                reasons=list(row["reasons"]),
            )
            for _, row in sell_points.iterrows()
        ],
    }

    latest_close = df.iloc[-1]["close"]
    ma_fast = df.iloc[-1]["ma_fast"]
    ma_slow = df.iloc[-1]["ma_slow"]
    macd = df.iloc[-1]["macd"]
    rsi = df.iloc[-1]["rsi"]

    trend = "震荡"
    if pd.notna(ma_fast) and pd.notna(ma_slow):
        if ma_fast > ma_slow:
            trend = "上升"
        elif ma_fast < ma_slow:
            trend = "下降"

    momentum = "趋弱" if macd < 0 else "转强"

    summary["market_view"] = {
        "latest_price": float(latest_close),
        "trend": trend,
        "momentum": momentum,
        "rsi": float(rsi) if pd.notna(rsi) else None,
    }
    return summary

