"""
Data access utilities for A-share equities.

This module provides a single entry-point `load_stock_history` that tries to
retrieve historical OHLCV data via Akshare. If the runtime environment lacks
network connectivity or the Akshare dependency, it falls back to loading a
local CSV sample when available.
"""

from __future__ import annotations

import datetime as dt
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd
import requests


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


def _market_code(symbol: str) -> Optional[str]:
    if symbol.startswith(("5", "6", "9")):
        return f"1.{symbol}"  # Shanghai
    if symbol.startswith(("0", "2", "3", "4", "8")):
        return f"0.{symbol}"  # Shenzhen & Beijing boards
    return None


_ADJUST_MAP = {
    "qfq": 1,
    "hfq": 2,
    "none": 0,
    "": 0,
    None: 0,
}


def _yf_ticker(symbol: str) -> Optional[str]:
    if symbol.startswith(("5", "6", "9")):
        return f"{symbol}.SS"
    if symbol.startswith(("0", "2", "3")):
        return f"{symbol}.SZ"
    if symbol.startswith(("4", "8")):
        return f"{symbol}.BJ"
    return None


def _try_eastmoney(req: HistoryRequest) -> Optional[pd.DataFrame]:
    secid = _market_code(req.symbol)
    if secid is None:
        return None

    adjust_code = _ADJUST_MAP.get((req.adjust or "").lower(), 0)
    beg = req.start.strftime("%Y%m%d")
    end = req.end.strftime("%Y%m%d")
    url = (
        "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        f"?secid={secid}&fields1=f1,f2,f3,f4,f5,f6"
        "&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60"
        f"&klt=101&fqt={adjust_code}&beg={beg}&end={end}"
    )

    try:
        session = requests.Session()
        session.trust_env = False  # ignore potentially broken corporate proxy settings
        resp = session.get(
            url,
            timeout=8,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; AShareSignalBot/1.0)",
                "Referer": "https://quote.eastmoney.com/",
            },
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception:
        return None

    klines = payload.get("data", {}).get("klines") or []
    if not klines:
        return None

    records = []
    for line in klines:
        parts = line.split(",")
        if len(parts) < 7:
            continue
        date_str, open_, close, high, low, volume, amount = parts[:7]
        records.append(
            {
                "date": pd.to_datetime(date_str),
                "open": float(open_),
                "close": float(close),
                "high": float(high),
                "low": float(low),
                # Eastmoney volume is in lots; convert to shares for consistency.
                "volume": float(volume) * 100,
                "amount": float(amount),
            }
        )

    if not records:
        return None

    data = pd.DataFrame(records).dropna(subset=["close"])
    data = data.sort_values("date").reset_index(drop=True)
    data.attrs["source"] = "eastmoney"
    data.attrs["sample_range"] = (
        data["date"].min().date().isoformat(),
        data["date"].max().date().isoformat(),
    )
    return data


def _try_yfinance(req: HistoryRequest) -> Optional[pd.DataFrame]:
    ticker = _yf_ticker(req.symbol)
    if ticker is None:
        return None

    try:
        import yfinance as yf  # type: ignore
    except ModuleNotFoundError:
        return None

    # yfinance uses a shared requests session; disable inherited proxy config
    try:
        yf.utils.get_yf_session().trust_env = False  # type: ignore[attr-defined]
    except Exception:
        pass

    proxy_keys = [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "NO_PROXY",
        "no_proxy",
    ]
    saved_env = {key: os.environ.get(key) for key in proxy_keys}
    for key in proxy_keys:
        os.environ.pop(key, None)

    try:
        history = yf.Ticker(ticker).history(
            start=req.start,
            end=req.end + dt.timedelta(days=1),
            interval="1d",
            auto_adjust=False,
            actions=False,
        )
    except Exception:
        history = None
    finally:
        for key, value in saved_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    if history is None:
        return None

    if history.empty:
        return None

    if isinstance(history.columns, pd.MultiIndex):
        history.columns = history.columns.get_level_values(0)

    df = history.reset_index().rename(
        columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        }
    )
    close_series = df.get("close")
    if close_series is None and "Close" in df:
        close_series = df["Close"]
    if req.adjust and req.adjust.lower() in {"qfq", "hfq"} and "adj_close" in df:
        close_series = df["adj_close"]

    if close_series is None:
        return None

    df = df.assign(close=close_series).dropna(subset=["close"])
    df["amount"] = df["close"] * df["volume"].fillna(0)
    df = df[["date", "open", "high", "low", "close", "volume", "amount"]]
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    df.attrs["source"] = "yfinance"
    df.attrs["sample_range"] = (
        df["date"].min().date().isoformat(),
        df["date"].max().date().isoformat(),
    )
    return df


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

    east = _try_eastmoney(request)
    if east is not None:
        return east

    yf_data = _try_yfinance(request)
    if yf_data is not None:
        return yf_data

    sample_base = sample_dir or Path(__file__).resolve().parent.parent / "resources"
    offline = _try_local_sample(request, sample_base)
    if offline is not None:
        return offline

    raise DataUnavailableError(
        f"无法获取 {symbol} 的行情数据。请确认网络可用或提供本地样例数据。"
    )
