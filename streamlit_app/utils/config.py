import json
import streamlit as st
from pathlib import Path

_CONFIG_PATH = Path(__file__).parent.parent / "config.json"


def load_config() -> dict:
    """Load config from session_state, initialising from config.json on first call."""
    if "config" not in st.session_state:
        with open(_CONFIG_PATH) as f:
            st.session_state["config"] = json.load(f)
    return st.session_state["config"]


def save_config(config: dict) -> None:
    """Persist updated config into session_state."""
    st.session_state["config"] = config


def get_symbols() -> list[str]:
    return [s["nfo_symbol"] for s in load_config()["symbols"]]


def get_symbol_info(nfo_symbol: str) -> dict | None:
    for s in load_config()["symbols"]:
        if s["nfo_symbol"] == nfo_symbol:
            return s
    return None
