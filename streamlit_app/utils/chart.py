import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import yfinance as yf


@st.cache_data(ttl=3600)
def _fetch_ohlc(yf_ticker: str) -> pd.DataFrame:
    df = yf.Ticker(yf_ticker).history(period="30d", interval="1d")
    return df.reset_index()


def render_candlestick(yf_ticker: str, title: str | None = None) -> None:
    if not yf_ticker:
        st.info("No yfinance ticker configured for this symbol. Set it in the Config page.")
        return

    with st.spinner(f"Loading chart for {yf_ticker}…"):
        df = _fetch_ohlc(yf_ticker)

    if df.empty:
        st.warning(f"No historical data found for **{yf_ticker}**.")
        return

    fig = go.Figure(
        data=[
            go.Candlestick(
                x=df["Date"],
                open=df["Open"],
                high=df["High"],
                low=df["Low"],
                close=df["Close"],
                increasing_line_color="#1B5E20",
                decreasing_line_color="#B71C1C",
            )
        ]
    )
    fig.update_layout(
        title=title or f"{yf_ticker} — Last 30 Days",
        yaxis_title="Price (₹)",
        height=420,
        xaxis_rangeslider_visible=False,
        margin=dict(l=0, r=0, t=40, b=0),
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True)
