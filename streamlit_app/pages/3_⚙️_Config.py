import streamlit as st
import pandas as pd
import json
from utils.config import load_config, save_config

st.set_page_config(page_title="Config", page_icon="⚙️", layout="wide")
st.title("⚙️ Config")
st.caption("Manage symbols, lot sizes, and yfinance ticker mappings.")

st.info(
    "Changes apply immediately to your session. "
    "Download the JSON and commit it to keep changes permanently.",
    icon="ℹ️",
)

config = load_config()
df = pd.DataFrame(config["symbols"])

edited = st.data_editor(
    df,
    num_rows="dynamic",
    use_container_width=True,
    column_config={
        "nfo_symbol": st.column_config.TextColumn(
            "NFO Symbol",
            help="Symbol used in Breeze NFO/BFO API calls (e.g. NIFTY, TCS)",
            required=True,
        ),
        "nse_symbol": st.column_config.TextColumn(
            "NSE Symbol",
            help="NSE display symbol (usually the same as NFO symbol for stocks)",
        ),
        "yf_ticker": st.column_config.TextColumn(
            "yfinance Ticker",
            help="Yahoo Finance ticker for the candlestick chart. "
            "Stocks: TCS.NS | Indices: ^NSEI (NIFTY), ^NSEBANK (BANKNIFTY)",
        ),
        "lot_size": st.column_config.NumberColumn(
            "Lot Size",
            help="Breeze F&O lot size for this symbol",
            min_value=1,
            step=1,
            required=True,
        ),
    },
)

col1, col2 = st.columns(2)

with col1:
    if st.button("💾 Apply Changes", type="primary"):
        records = edited.dropna(subset=["nfo_symbol"]).to_dict(orient="records")
        save_config({"symbols": records})
        st.success("Config updated for this session.")
        st.rerun()

with col2:
    records = edited.dropna(subset=["nfo_symbol"]).to_dict(orient="records")
    updated_json = json.dumps({"symbols": records}, indent=2)
    st.download_button(
        "⬇️ Download config.json",
        data=updated_json.encode("utf-8"),
        file_name="config.json",
        mime="application/json",
        help="Download and commit this file to streamlit_app/config.json to persist changes.",
    )

st.divider()
st.subheader("yfinance Ticker Reference")
st.markdown("""
| Symbol type | Example | yfinance ticker |
|---|---|---|
| NSE stock | TCS | `TCS.NS` |
| Nifty 50 index | NIFTY | `^NSEI` |
| Bank Nifty index | BANKNIFTY | `^NSEBANK` |
| Fin Nifty index | FINNIFTY | `^CNXFIN` |
| Midcap Nifty | MIDCPNIFTY | `^NSEMDCP50` |
""")
