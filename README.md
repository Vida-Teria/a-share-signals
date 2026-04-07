# A股买卖点可视化助手

基于 Streamlit 的轻量工具，输入 A 股股票代码即可获取近阶段的趋势、动量指标以及自动计算的买卖信号。默认通过 [Akshare](https://akshare.xyz/) 拉取最新行情，离线场景下会回落到项目内提供的示例数据。

## 功能特性

- 输入股票代码与回溯区间，自动拉取日线级别数据。
- 计算 MA、EMA、MACD、RSI 等指标并给出买卖点提示。
- 以 Plotly K 线图展示信号点，附带最近信号摘要与趋势分析。
- 支持在 `app/resources` 下放置自定义 CSV，便于离线或自有数据源。

## 快速开始

1. 创建虚拟环境并安装依赖：

   ```bash
   python -m venv .venv
   .venv\Scripts\activate  # Windows
   pip install -r requirements.txt
   ```

2. 运行 Streamlit 应用：

   ```bash
   streamlit run streamlit_app.py
   ```

3. 打开浏览器访问终端输出的地址，输入股票代码（例如 `600519`），即可查看信号分析。

## 离线数据

若当前环境无法访问外部网络，可将本地历史数据 CSV 文件放置于 `app/resources` 目录，文件名使用对应股票代码（如 `600519.csv`），列格式应包含：

```
date,open,high,low,close,volume,amount
```

项目已附带 `sample.csv` 作为示例。

## 声明

本工具仅供学习研究使用，任何信号或分析不构成投资建议。请结合自身情况谨慎决策。

