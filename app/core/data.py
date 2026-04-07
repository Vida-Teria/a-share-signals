"""
Data access utilities for A-share equities.

This module provides a single entry-point `load_stock_history` that tries to
retrieve historical OHLCV data via Akshare. If the runtime environment lacks
network connectivity or the Akshare dependency, it falls back to loading a
local CSV sample when available.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd


class DataUnavailableError(RuntimeError):
    """Raised when no data source is available for the requested symbol."""


@dataclass(frozen=True)
class HistoryRequest:
    symbol: str
    start: Optional[dt.date] = None
    end: Optional[dt.date] = None
    adjust: str = "qfq"  # 前复权 keeps continuity for long periods

    def normalized(self) -> "HistoryRequest":
        today = dt.date.today()
        default_start = today - dt.timedelta(days=365)
        return HistoryRequest(
            symbol=self.symbol,
            start=self.start or default_start,
            end=self.end or today,
            adjust=self.adjust,
        )


def _try_akshare(req: HistoryRequest) -> Optional[pd.DataFrame]:
    try:
        import akshare as ak  # type: ignore
    except ModuleNotFoundError:
        return None

    try:
        raw = ak.stock_zh_a_hist(
            symbol=req.symbol,
            period="daily",
            start_date=req.start.strftime("%Y%m%d"),
            end_date=req.end.strftime("%Y%m%d"),
            adjust=req.adjust,
        )
    except Exception:
        return None

    if raw.empty:
        return None

    data = raw.rename(
        columns={
            "日期": "date",
            "开盘": "open",
            "最高": "high",
            "最低": "low",
            "收盘": "close",
            "成交量": "volume",
            "成交额": "amount",
        }
    )
    data["date"] = pd.to_datetime(data["date"])
    numeric_cols = ["open", "high", "low", "close", "volume", "amount"]
    data[numeric_cols] = data[numeric_cols].apply(pd.to_numeric, errors="coerce")
    data = data.dropna(subset=["close"])
    data = data.sort_values("date").reset_index(drop=True)
    data.attrs["source"] = "akshare"
    data.attrs["sample_range"] = (
        data["date"].min().date().isoformat(),
        data["date"].max().date().isoformat(),
    )
    return data


def _try_local_sample(req: HistoryRequest, base_path: Path) -> Optional[pd.DataFrame]:
    csv_path = base_path / f"{req.symbol}.csv"
    if not csv_path.exists():
        csv_path = base_path / "sample.csv"
        if not csv_path.exists():
            return None

    try:
        data = pd.read_csv(csv_path, parse_dates=["date"])
    except Exception:
        return None

    if data.empty or "date" not in data:
        return None

    data = data.sort_values("date").reset_index(drop=True)

    available_start = data["date"].min().date()
    available_end = data["date"].max().date()

    start = max(req.start, available_start)
    end = min(req.end, available_end)
    window = data[(data["date"].dt.date >= start) & (data["date"].dt.date <= end)]

    if window.empty:
        window = data

    window = window.reset_index(drop=True)
    window.attrs["source"] = "local_sample"
    window.attrs["sample_range"] = (
        available_start.isoformat(),
        available_end.isoformat(),
    )
    return window


def load_stock_history(
    symbol: str,
    start: Optional[dt.date] = None,
    end: Optional[dt.date] = None,
    adjust: str = "qfq",
    sample_dir: Path | None = None,
) -> pd.DataFrame:
    """
    Load historical OHLCV data for the given A-share symbol.

    The function prioritizes live data via Akshare, and only uses the bundled
    CSV sample when live retrieval fails. A `DataUnavailableError` is raised if
    no data source succeeds.
    """

    request = HistoryRequest(symbol, start, end, adjust).normalized()

    live = _try_akshare(request)
    if live is not None:
        return live

    sample_base = sample_dir or Path(__file__).resolve().parent.parent / "resources"
    offline = _try_local_sample(request, sample_base)
    if offline is not None:
        return offline

    raise DataUnavailableError(
        f"无法获取 {symbol} 的行情数据。请确认网络可用或提供本地样例数据。"
    )
