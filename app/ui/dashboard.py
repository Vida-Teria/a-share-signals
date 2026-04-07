"""
Streamlit dashboard for the A-share trading assistant.
"""

from __future__ import annotations

import datetime as dt

import plotly.graph_objects as go
import streamlit as st

from app.core.data import DataUnavailableError, load_stock_history
from app.core.signals import generate_trade_signals, summarize_signals


def _render_header() -> None:
    st.set_page_config(page_title="A股买卖点助手", layout="centered")
    st.markdown(
        """
        <style>
        [data-testid="stAppViewContainer"]{
            padding-top:1rem;
        }
        @media (max-width: 768px){
            [data-testid="stSidebar"]{
                width:100% !important;
                position:relative;
                border-right:none;
            }
            [data-testid="stSidebar"] section{
                padding:1rem 1.25rem;
            }
            [data-testid="stSidebarNav"]{
                display:none;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.title("A股买卖点助手")
    st.caption(
        "输入股票代码（如 `600519` 或 `000001`），系统将基于趋势与动量指标给出近期买卖点建议。"
    )


def _render_sidebar() -> dict:
    st.sidebar.header("参数设置")
    symbol = st.sidebar.text_input("股票代码", value="600519")
    days_back = st.sidebar.slider("回溯天数", min_value=90, max_value=720, value=240, step=30)
    adjust = st.sidebar.selectbox("复权方式", options=["qfq", "hfq", "None"], index=0)
    start = dt.date.today() - dt.timedelta(days=days_back)
    end = dt.date.today()
    return {
        "symbol": symbol.strip(),
        "start": start,
        "end": end,
        "adjust": None if adjust == "None" else adjust,
    }


def _plot_with_signals(df):
    figure = go.Figure(
        data=[
            go.Candlestick(
                x=df["date"],
                open=df["open"],
                high=df["high"],
                low=df["low"],
                close=df["close"],
                name="K线",
            )
        ]
    )

    buys = df[df["buy_signal"]]
    sells = df[df["sell_signal"]]
    figure.add_trace(
        go.Scatter(
            x=buys["date"],
            y=buys["close"],
            mode="markers",
            marker=dict(symbol="triangle-up", size=12, color="#1f77b4"),
            name="买入信号",
        )
    )
    figure.add_trace(
        go.Scatter(
            x=sells["date"],
            y=sells["close"],
            mode="markers",
            marker=dict(symbol="triangle-down", size=12, color="#d62728"),
            name="卖出信号",
        )
    )

    figure.update_layout(
        margin=dict(l=48, r=24, t=32, b=32),
        height=520,
        xaxis_title="交易日",
        yaxis_title="股价 (元)",
        xaxis_rangeslider_visible=False,
    )
    return figure


def _render_summary(summary: dict) -> None:
    st.subheader("市场综述")
    cols = st.columns(3)
    cols[0].metric("最新收盘", f"{summary['market_view']['latest_price']:.2f} 元")
    cols[1].metric("趋势", summary["market_view"]["trend"])
    rsi = summary["market_view"]["rsi"]
    cols[2].metric("RSI(14)", f"{rsi:.1f}" if rsi is not None else "暂无数据")

    st.markdown("---")
    left, right = st.columns(2)

    with left:
        st.markdown("#### 最近买入信号")
        buys = summary["recent_buys"]
        if not buys:
            st.info("近5个信号窗口内无买入信号。")
        else:
            for signal in buys:
                st.write(
                    f"**{signal.date.date()}** · {signal.price:.2f} 元 · 置信度 {signal.confidence:.0%}"
                )
                st.caption("；".join(signal.reasons))

    with right:
        st.markdown("#### 最近卖出信号")
        sells = summary["recent_sells"]
        if not sells:
            st.info("近5个信号窗口内无卖出信号。")
        else:
            for signal in sells:
                st.write(
                    f"**{signal.date.date()}** · {signal.price:.2f} 元 · 置信度 {signal.confidence:.0%}"
                )
                st.caption("；".join(signal.reasons))


def run() -> None:
    _render_header()
    params = _render_sidebar()

    if not params["symbol"]:
        st.warning("请输入股票代码，例如 `600519`。")
        return

    try:
        history = load_stock_history(
            symbol=params["symbol"],
            start=params["start"],
            end=params["end"],
            adjust=params["adjust"] or "qfq",
        )
    except DataUnavailableError as exc:
        st.error(str(exc))
        st.stop()

    source = history.attrs.get("source")
    if source == "local_sample":
        start, end = history.attrs.get("sample_range", ("未知", "未知"))
        st.info(
            f"当前使用的是内置样例数据，时间范围 {start} 至 {end}。如需实时行情，请在可联网环境下运行。"
        )
    elif source == "akshare":
        _, end = history.attrs.get("sample_range", ("未知", "未知"))
        st.success(f"行情数据来自 Akshare，已更新至 {end}。")
    elif source == "eastmoney":
        _, end = history.attrs.get("sample_range", ("未知", "未知"))
        st.success(f"行情数据来自东方财富，已更新至 {end}。")

    with st.spinner("正在计算信号..."):
        result = generate_trade_signals(history)
        summary = summarize_signals(result)

    chart = _plot_with_signals(result)
    st.plotly_chart(chart, use_container_width=True)

    _render_summary(summary)

    st.markdown("---")
    st.markdown(
        "数据源优先使用 Akshare 实时行情，若当前环境离线，请在 `app/resources` 目录下放置相应代码的历史数据 CSV。"
    )
    st.caption("本工具仅供学习研究，信号不构成投资建议。")


if __name__ == "__main__":
    run()

