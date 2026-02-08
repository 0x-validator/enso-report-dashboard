"""
ENSO Foundation Report Dashboard
=================================
Live Streamlit dashboard replicating the R report.
Fetches all data from public APIs on each visit (cached 10 min).

Sections:
  1. Treasury Holdings (on-chain wallets, vesting, staking, rewards)
  2. Market Makers Holdings (manual input, added to totals)
  3. Vesting Contracts Detail
  4. Staked Positions Detail
  5. Spot Exchange Volumes (14 exchanges)
  6. Perpetual Exchange Volumes (11 exchanges)
  7. Selling Plan

Run locally:  streamlit run report_dashboard.py
"""

import os
import time
from datetime import datetime, timezone, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="ENSO Foundation Report",
    page_icon="📊",
    layout="wide",
)

# ── Constants ────────────────────────────────────────────────────────────────
ENSO_CONTRACT = "0x699F088b5DddcAFB7c4824db5B10B57B37cB0C66"
STAKING_CONTRACT = "0x22Ad2a46d317C5eDF6c01fea16d4399C912E9A01"
DECIMALS = 18
ETHERSCAN_URL = "https://api.etherscan.io/v2/api"
GOLDSKY_URL = "https://api.goldsky.com/api/public/project_cmgrrbljx1rpt01wnczmq0ayf/subgraphs/tge/0.0.2/gn"
RPC_URL = "https://eth.llamarpc.com"
BSC_RPC_URL = "https://bsc-dataseed.binance.org/"
BSC_ENSO_CONTRACT = "0xfeb339236d25d3e415f280189bc7c2fbab6ae9ef"
COINGECKO_URL = "https://api.coingecko.com/api/v3"

FOUNDATION_WALLETS = {
    "treasury": {"addr": "0x715b1ddf5d6da6846eadb72d3d6f9d93148d0bb0", "type": "liquid"},
    "vesting_contract": {"addr": "0x4110d73ff4d4fe45af2762df2205ad95c8c9679b", "type": "vesting"},
    "operational": {"addr": "0xd782d294bc3e8b1a32ec9283b02893fd932d3ece", "type": "liquid"},
    "vesting_operational_1": {"addr": "0x3dea6f0f4d3fcd9a706e8b6b0750ab2f57dec17a", "type": "vesting"},
    "vesting_operational_2": {"addr": "0x332e5b70e451bdeebd36b5d3442827aa52a42f80", "type": "vesting"},
    "vesting_operational_3": {"addr": "0xa3314896ae22caf4bfdcb0bd4c3cabce324d8f3e", "type": "vesting"},
}

BINANCE_OBLIGATION = 1_750_000
BINANCE_DUE_DATE = "2026-03-27"

VESTING_SCHEDULES = [
    {"name": "Vesting Contract", "start": 1760434200, "end": 1823506200, "total": 14_605_000},
    {"name": "Operational 1", "start": 1760434200, "end": 1886578200, "total": 4_020_000},
    {"name": "Operational 2", "start": 1760434200, "end": 1774690200, "total": 1_230_000},
    {"name": "Operational 3", "start": 1760434200, "end": 1773394200, "total": 1_750_000},
]


# ── Helpers ──────────────────────────────────────────────────────────────────
def short_addr(addr: str) -> str:
    return f"{addr[:6]}...{addr[-4:]}"


def safe_get(url: str, params: dict | None = None, headers: dict | None = None,
             timeout: int = 15) -> dict | list | None:
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=timeout)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


def safe_post(url: str, json_body: dict, timeout: int = 15) -> dict | None:
    try:
        resp = requests.post(url, json=json_body, timeout=timeout)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


# ── On-chain data functions ──────────────────────────────────────────────────
def get_token_balance(wallet: str, api_key: str) -> float:
    data = safe_get(ETHERSCAN_URL, params={
        "chainid": "1", "module": "account", "action": "tokenbalance",
        "contractaddress": ENSO_CONTRACT, "address": wallet,
        "tag": "latest", "apikey": api_key,
    })
    if data and data.get("status") == "1":
        return int(data["result"]) / 10**DECIMALS
    return 0.0


def get_bsc_token_balance(wallet: str) -> float:
    addr_padded = wallet.lower().replace("0x", "").zfill(64)
    call_data = "0x70a08231" + addr_padded
    body = {
        "jsonrpc": "2.0", "id": 1, "method": "eth_call",
        "params": [{"to": BSC_ENSO_CONTRACT, "data": call_data}, "latest"],
    }
    data = safe_post(BSC_RPC_URL, body)
    if data and data.get("result") and data["result"] != "0x":
        try:
            return int(data["result"], 16) / 10**DECIMALS
        except (ValueError, TypeError):
            pass
    return 0.0


def get_vesting_info(contract: str, api_key: str) -> dict:
    balance = get_token_balance(contract, api_key)
    data = safe_get(ETHERSCAN_URL, params={
        "chainid": "1", "module": "proxy", "action": "eth_call",
        "to": contract, "data": "0xfbccedae",
        "tag": "latest", "apikey": api_key,
    })
    vested_unclaimed = 0.0
    if data and data.get("result") and not any(
        x in str(data["result"]).lower() for x in ["invalid", "error", "revert"]
    ):
        try:
            vested_unclaimed = int(data["result"], 16) / 10**DECIMALS
        except (ValueError, TypeError):
            pass
    locked = max(0, balance - vested_unclaimed)
    return {
        "balance": balance,
        "vested_unclaimed": vested_unclaimed,
        "locked": locked,
    }


def get_staked_positions(owner: str) -> list[dict]:
    query = '{ positions(where:{owner:"%s"}) { id expiry deposit } }' % owner.lower()
    data = safe_post(GOLDSKY_URL, {"query": query})
    if not data or not data.get("data", {}).get("positions"):
        return []
    now = int(datetime.now(timezone.utc).timestamp())
    positions = []
    for p in data["data"]["positions"]:
        deposit = int(p["deposit"]) / 10**DECIMALS
        expiry = int(p["expiry"])
        positions.append({
            "id": int(p["id"]),
            "deposit": deposit,
            "expiry_ts": expiry,
            "expiry_utc": datetime.fromtimestamp(expiry, tz=timezone.utc),
            "is_expired": expiry <= now,
        })
    return positions


def get_position_rewards(position_id: int) -> float:
    id_hex = hex(position_id)[2:].zfill(64)
    call_data = "0x61c02efb" + id_hex
    body = {
        "jsonrpc": "2.0", "id": 1, "method": "eth_call",
        "params": [{"to": STAKING_CONTRACT, "data": call_data}, "latest"],
    }
    data = safe_post(RPC_URL, body)
    if data and data.get("result") and data["result"] != "0x":
        try:
            return int(data["result"], 16) / 10**DECIMALS
        except (ValueError, TypeError):
            pass
    return 0.0


def get_enso_price(cg_key: str) -> float | None:
    data = safe_get(f"{COINGECKO_URL}/simple/price",
                    params={"ids": "enso", "vs_currencies": "usd"},
                    headers={"x-cg-demo-api-key": cg_key})
    if data and "enso" in data:
        return data["enso"].get("usd")
    return None


def get_circulating_supply() -> dict:
    data = safe_get("http://api.enso.finance/api/v1/enso-token/circulating-supply")
    if data:
        return {
            "circulating": float(data.get("circulatingSupply", 0)),
            "total": float(data.get("totalSupply", 0)),
        }
    return {"circulating": 0, "total": 0}


def calculate_vesting_projection(target_date_str: str = "2026-03-27") -> int:
    target_ts = int(datetime.strptime(target_date_str, "%Y-%m-%d").replace(
        tzinfo=timezone.utc).timestamp())
    now_ts = int(datetime.now(timezone.utc).timestamp())
    total = 0
    for v in VESTING_SCHEDULES:
        rate = v["total"] / (v["end"] - v["start"])
        effective_end = min(target_ts, v["end"])
        if effective_end > now_ts:
            total += rate * (effective_end - now_ts)
    return round(total)


# ── Exchange volume functions ────────────────────────────────────────────────
def _rolling_7day(daily_volumes: list[tuple], exchange: str) -> dict | None:
    """Calculate 7-day rolling volume from (date, quote_volume) tuples."""
    if len(daily_volumes) < 7:
        return None
    today = datetime.now(timezone.utc).date()
    filtered = [(d, v) for d, v in daily_volumes if d < today]
    filtered.sort(key=lambda x: x[0])
    if len(filtered) < 7:
        return None
    current = filtered[-7:]
    cur_vol = sum(v for _, v in current)
    wow = None
    if len(filtered) >= 14:
        prev = filtered[-14:-7]
        prev_vol = sum(v for _, v in prev)
        if prev_vol > 0:
            wow = ((cur_vol - prev_vol) / prev_vol) * 100
    return {"exchange": exchange, "volume_7d": cur_vol, "avg_daily": cur_vol / 7, "wow_pct": wow}


def _parse_binance_klines(data: list) -> list[tuple]:
    rows = []
    for k in data:
        ts = datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc).date()
        rows.append((ts, float(k[7])))
    return rows


def get_spot_volume_binance() -> dict | None:
    data = safe_get("https://api.binance.com/api/v3/klines",
                    params={"symbol": "ENSOUSDT", "interval": "1d", "limit": "30"})
    return _rolling_7day(_parse_binance_klines(data), "Binance") if data else None


def get_spot_volume_bybit() -> dict | None:
    data = safe_get("https://api.bybit.com/v5/market/kline",
                    params={"category": "spot", "symbol": "ENSOUSDT", "interval": "D", "limit": "30"})
    if not data or "result" not in data:
        return None
    rows = []
    for k in data["result"].get("list", []):
        ts = datetime.fromtimestamp(int(k[0]) / 1000, tz=timezone.utc).date()
        rows.append((ts, float(k[6])))
    return _rolling_7day(rows, "Bybit")


def get_spot_volume_okx() -> dict | None:
    data = safe_get("https://www.okx.com/api/v5/market/candles",
                    params={"instId": "ENSO-USDT", "bar": "1D", "limit": "30"})
    if not data or "data" not in data:
        return None
    rows = []
    for k in data["data"]:
        ts = datetime.fromtimestamp(int(k[0]) / 1000, tz=timezone.utc).date()
        rows.append((ts, float(k[7]) if len(k) > 7 else float(k[6])))
    return _rolling_7day(rows, "OKX")


def get_spot_volume_bitget() -> dict | None:
    data = safe_get("https://api.bitget.com/api/v2/spot/market/candles",
                    params={"symbol": "ENSOUSDT", "granularity": "1day", "limit": "30"})
    if not data or "data" not in data:
        return None
    rows = []
    for k in data["data"]:
        ts = datetime.fromtimestamp(int(k[0]) / 1000, tz=timezone.utc).date()
        rows.append((ts, float(k[6]) if len(k) > 6 else float(k[5])))
    return _rolling_7day(rows, "Bitget")


def get_spot_volume_gate() -> dict | None:
    data = safe_get("https://api.gateio.ws/api/v4/spot/candlesticks",
                    params={"currency_pair": "ENSO_USDT", "interval": "1d", "limit": "30"})
    if not data or not isinstance(data, list):
        return None
    rows = []
    for k in data:
        ts = datetime.fromtimestamp(int(k[0]), tz=timezone.utc).date()
        qv = float(k[1]) * float(k[2]) if len(k) > 2 else float(k[6]) if len(k) > 6 else 0
        if len(k) > 6:
            qv = float(k[6])
        rows.append((ts, qv))
    return _rolling_7day(rows, "Gate.io")


def get_spot_volume_kraken() -> dict | None:
    data = safe_get("https://api.kraken.com/0/public/OHLC",
                    params={"pair": "ENSOUSD", "interval": "1440"})
    if not data or "result" not in data:
        return None
    for key in data["result"]:
        if key == "last":
            continue
        rows = []
        for k in data["result"][key]:
            ts = datetime.fromtimestamp(int(k[0]), tz=timezone.utc).date()
            rows.append((ts, float(k[6])))
        return _rolling_7day(rows, "Kraken")
    return None


def get_spot_volume_bitmart() -> dict | None:
    data = safe_get("https://api-cloud.bitmart.com/spot/quotation/v3/klines",
                    params={"symbol": "ENSO_USDT", "step": "1440", "limit": "30"})
    if not data or "data" not in data:
        return None
    rows = []
    for k in data["data"]:
        ts = datetime.fromtimestamp(int(k[0]), tz=timezone.utc).date()
        rows.append((ts, float(k[6]) if len(k) > 6 else float(k[5])))
    return _rolling_7day(rows, "BitMart")


def get_spot_volume_phemex() -> dict | None:
    now_ts = int(datetime.now(timezone.utc).timestamp())
    data = safe_get("https://api.phemex.com/exchange/public/md/v2/kline",
                    params={"symbol": "sENSOUSDT", "resolution": "86400",
                            "limit": "30", "to": str(now_ts)})
    if not data or "data" not in data or "rows" not in data["data"]:
        return None
    rows = []
    for k in data["data"]["rows"]:
        ts = datetime.fromtimestamp(int(k[0]), tz=timezone.utc).date()
        qv = float(k[7]) / 10000 if len(k) > 7 else float(k[5]) / 10000
        rows.append((ts, qv))
    return _rolling_7day(rows, "Phemex")


def get_spot_volume_kucoin() -> dict | None:
    now_ts = int(datetime.now(timezone.utc).timestamp())
    start = now_ts - 30 * 86400
    data = safe_get("https://api.kucoin.com/api/v1/market/candles",
                    params={"type": "1day", "symbol": "ENSO-USDT",
                            "startAt": str(start), "endAt": str(now_ts)})
    if not data or "data" not in data:
        return None
    rows = []
    for k in data["data"]:
        ts = datetime.fromtimestamp(int(k[0]), tz=timezone.utc).date()
        rows.append((ts, float(k[6]) if len(k) > 6 else float(k[5])))
    return _rolling_7day(rows, "KuCoin")


def get_spot_volume_bingx() -> dict | None:
    data = safe_get("https://open-api.bingx.com/openApi/spot/v2/market/kline",
                    params={"symbol": "ENSO-USDT", "interval": "1d", "limit": "30"})
    if not data or "data" not in data:
        return None
    rows = []
    for k in data["data"]:
        ts_val = k.get("time") or k.get("openTime") or k[0]
        ts = datetime.fromtimestamp(int(ts_val) / 1000, tz=timezone.utc).date()
        qv = float(k.get("quoteVolume", 0) or k.get("volume", 0))
        rows.append((ts, qv))
    return _rolling_7day(rows, "BingX")


def get_spot_volume_mexc() -> dict | None:
    data = safe_get("https://api.mexc.com/api/v3/klines",
                    params={"symbol": "ENSOUSDT", "interval": "1d", "limit": "30"})
    if not data or not isinstance(data, list):
        return None
    rows = []
    for k in data:
        ts = datetime.fromtimestamp(int(k[0]) / 1000, tz=timezone.utc).date()
        rows.append((ts, float(k[7])))
    return _rolling_7day(rows, "MEXC")


def _get_krw_to_usd() -> float:
    data = safe_get("https://api.exchangerate-api.com/v4/latest/USD")
    if data and "rates" in data and "KRW" in data["rates"]:
        return 1.0 / data["rates"]["KRW"]
    return 1.0 / 1450


def get_spot_volume_upbit() -> dict | None:
    data = safe_get("https://api.upbit.com/v1/candles/days",
                    params={"market": "KRW-ENSO", "count": "30"})
    if not data or not isinstance(data, list):
        return None
    krw_rate = _get_krw_to_usd()
    rows = []
    for k in data:
        ts = datetime.fromisoformat(k["candle_date_time_utc"]).date()
        qv = float(k.get("candle_acc_trade_price", 0)) * krw_rate
        rows.append((ts, qv))
    return _rolling_7day(rows, "Upbit")


def get_spot_volume_bithumb() -> dict | None:
    data = safe_get(f"https://api.bithumb.com/public/candlestick/ENSO_KRW/24h")
    if not data or data.get("status") != "0000" or "data" not in data:
        return None
    krw_rate = _get_krw_to_usd()
    rows = []
    for k in data["data"]:
        if isinstance(k, list) and len(k) >= 6:
            ts = datetime.fromtimestamp(int(k[0]) / 1000, tz=timezone.utc).date()
            qv = float(k[5]) * krw_rate
            rows.append((ts, qv))
    return _rolling_7day(rows, "Bithumb")


def get_spot_volume_cointr() -> dict | None:
    data = safe_get("https://api.cointr.com/api/v2/spot/market/candles",
                    params={"symbol": "ENSOUSDT", "granularity": "1day", "limit": "30"})
    if not data or "data" not in data:
        return None
    rows = []
    for k in data["data"]:
        ts = datetime.fromtimestamp(int(k[0]) / 1000, tz=timezone.utc).date()
        rows.append((ts, float(k[7]) if len(k) > 7 else float(k[5])))
    return _rolling_7day(rows, "CoinTR")


# ── Perpetual volume functions ───────────────────────────────────────────────
def get_perp_volume_binance() -> dict | None:
    data = safe_get("https://fapi.binance.com/fapi/v1/klines",
                    params={"symbol": "ENSOUSDT", "interval": "1d", "limit": "30"})
    oi_data = safe_get("https://fapi.binance.com/fapi/v1/openInterest",
                       params={"symbol": "ENSOUSDT"})
    oi = float(oi_data.get("openInterest", 0)) if oi_data else 0
    if not data:
        return None
    result = _rolling_7day(_parse_binance_klines(data), "Binance")
    if result:
        result["open_interest"] = oi
    return result


def get_perp_volume_bybit() -> dict | None:
    data = safe_get("https://api.bybit.com/v5/market/kline",
                    params={"category": "linear", "symbol": "ENSOUSDT", "interval": "D", "limit": "30"})
    oi_data = safe_get("https://api.bybit.com/v5/market/tickers",
                       params={"category": "linear", "symbol": "ENSOUSDT"})
    oi = 0
    if oi_data and "result" in oi_data:
        tickers = oi_data["result"].get("list", [])
        if tickers:
            oi = float(tickers[0].get("openInterest", 0))
    if not data or "result" not in data:
        return None
    rows = []
    for k in data["result"].get("list", []):
        ts = datetime.fromtimestamp(int(k[0]) / 1000, tz=timezone.utc).date()
        rows.append((ts, float(k[6])))
    result = _rolling_7day(rows, "Bybit")
    if result:
        result["open_interest"] = oi
    return result


def get_perp_volume_gate() -> dict | None:
    data = safe_get("https://api.gateio.ws/api/v4/futures/usdt/candlesticks",
                    params={"contract": "ENSO_USDT", "interval": "1d", "limit": "30"})
    oi_data = safe_get("https://api.gateio.ws/api/v4/futures/usdt/contracts/ENSO_USDT")
    oi = float(oi_data.get("position_size", 0)) if oi_data and isinstance(oi_data, dict) else 0
    if not data or not isinstance(data, list):
        return None
    rows = []
    for k in data:
        ts = datetime.fromtimestamp(int(k["t"]), tz=timezone.utc).date()
        rows.append((ts, float(k.get("sum", 0))))
    result = _rolling_7day(rows, "Gate.io")
    if result:
        result["open_interest"] = oi
    return result


def get_perp_volume_okx() -> dict | None:
    data = safe_get("https://www.okx.com/api/v5/market/candles",
                    params={"instId": "ENSO-USDT-SWAP", "bar": "1D", "limit": "30"})
    oi_data = safe_get("https://www.okx.com/api/v5/public/open-interest",
                       params={"instId": "ENSO-USDT-SWAP"})
    oi = 0
    if oi_data and "data" in oi_data and oi_data["data"]:
        oi = float(oi_data["data"][0].get("oi", 0))
    if not data or "data" not in data:
        return None
    rows = []
    for k in data["data"]:
        ts = datetime.fromtimestamp(int(k[0]) / 1000, tz=timezone.utc).date()
        rows.append((ts, float(k[7]) if len(k) > 7 else float(k[6])))
    result = _rolling_7day(rows, "OKX")
    if result:
        result["open_interest"] = oi
    return result


def get_perp_volume_bitget() -> dict | None:
    data = safe_get("https://api.bitget.com/api/v2/mix/market/candles",
                    params={"productType": "USDT-FUTURES", "symbol": "ENSOUSDT",
                            "granularity": "1D", "limit": "30"})
    oi_data = safe_get("https://api.bitget.com/api/v2/mix/market/ticker",
                       params={"productType": "USDT-FUTURES", "symbol": "ENSOUSDT"})
    oi = 0
    if oi_data and "data" in oi_data and oi_data["data"]:
        oi_list = oi_data["data"] if isinstance(oi_data["data"], list) else [oi_data["data"]]
        if oi_list:
            oi = float(oi_list[0].get("openInterest", 0) or 0)
    if not data or "data" not in data:
        return None
    rows = []
    for k in data["data"]:
        ts = datetime.fromtimestamp(int(k[0]) / 1000, tz=timezone.utc).date()
        rows.append((ts, float(k[6]) if len(k) > 6 else float(k[5])))
    result = _rolling_7day(rows, "Bitget")
    if result:
        result["open_interest"] = oi
    return result


def get_perp_volume_bingx() -> dict | None:
    data = safe_get("https://open-api.bingx.com/openApi/swap/v3/quote/klines",
                    params={"symbol": "ENSO-USDT", "interval": "1d", "limit": "30"})
    oi_data = safe_get("https://open-api.bingx.com/openApi/swap/v2/quote/ticker",
                       params={"symbol": "ENSO-USDT"})
    oi = 0
    if oi_data and "data" in oi_data:
        d = oi_data["data"]
        if isinstance(d, list) and d:
            oi = float(d[0].get("openInterest", 0) or 0)
        elif isinstance(d, dict):
            oi = float(d.get("openInterest", 0) or 0)
    if not data or "data" not in data:
        return None
    rows = []
    for k in data["data"]:
        ts_val = k.get("time") or k.get("openTime") or 0
        ts = datetime.fromtimestamp(int(ts_val) / 1000, tz=timezone.utc).date()
        qv = float(k.get("quoteVolume", 0) or k.get("volume", 0))
        rows.append((ts, qv))
    result = _rolling_7day(rows, "BingX")
    if result:
        result["open_interest"] = oi
    return result


def get_perp_volume_mexc() -> dict | None:
    data = safe_get(f"https://contract.mexc.com/api/v1/contract/kline/ENSO_USDT",
                    params={"interval": "Day1", "limit": "30"})
    oi_data = safe_get("https://contract.mexc.com/api/v1/contract/ticker",
                       params={"symbol": "ENSO_USDT"})
    oi = 0
    if oi_data and "data" in oi_data:
        oi = float(oi_data["data"].get("holdVol", 0) or 0)
    if not data or "data" not in data:
        return None
    klines = data["data"]
    if isinstance(klines, dict):
        klines = klines.get("klineList", klines.get("data", []))
    if not isinstance(klines, list):
        return None
    rows = []
    for k in klines:
        if isinstance(k, dict):
            ts = datetime.fromtimestamp(int(k.get("time", 0)), tz=timezone.utc).date()
            rows.append((ts, float(k.get("vol", 0))))
    result = _rolling_7day(rows, "MEXC")
    if result:
        result["open_interest"] = oi
    return result


def get_perp_volume_phemex() -> dict | None:
    now_ts = int(datetime.now(timezone.utc).timestamp())
    data = safe_get("https://api.phemex.com/exchange/public/md/v2/kline",
                    params={"symbol": "ENSOUSDT", "resolution": "86400",
                            "limit": "30", "to": str(now_ts)})
    oi = 0
    if not data or "data" not in data or "rows" not in data["data"]:
        return None
    rows = []
    for k in data["data"]["rows"]:
        ts = datetime.fromtimestamp(int(k[0]), tz=timezone.utc).date()
        qv = float(k[4]) / 10000 if len(k) > 4 else 0
        rows.append((ts, qv))
    result = _rolling_7day(rows, "Phemex")
    if result:
        result["open_interest"] = oi
    return result


def get_perp_volume_kucoin() -> dict | None:
    now_ms = int(datetime.now(timezone.utc).timestamp()) * 1000
    start_ms = now_ms - 30 * 86400 * 1000
    data = safe_get("https://api-futures.kucoin.com/api/v1/kline/query",
                    params={"symbol": "ENSOUSDTM", "granularity": "1440",
                            "from": str(start_ms), "to": str(now_ms)})
    oi_data = safe_get("https://api-futures.kucoin.com/api/v1/contracts/ENSOUSDTM")
    oi = 0
    if oi_data and "data" in oi_data:
        oi = float(oi_data["data"].get("openInterest", 0) or 0)
    if not data or "data" not in data:
        return None
    rows = []
    for k in data["data"]:
        ts = datetime.fromtimestamp(int(k[0]) / 1000, tz=timezone.utc).date()
        rows.append((ts, float(k[6]) if len(k) > 6 else float(k[5])))
    result = _rolling_7day(rows, "KuCoin")
    if result:
        result["open_interest"] = oi
    return result


def get_perp_volume_bitmart() -> dict | None:
    data = safe_get("https://api-cloud.bitmart.com/contract/public/kline",
                    params={"symbol": "ENSOUSDT", "step": "1440", "limit": "30"})
    oi = 0
    if not data or "data" not in data:
        return None
    klines = data["data"].get("klines", data["data"]) if isinstance(data["data"], dict) else data["data"]
    if not isinstance(klines, list):
        return None
    rows = []
    for k in klines:
        if isinstance(k, dict):
            ts = datetime.fromtimestamp(int(k.get("timestamp", 0)), tz=timezone.utc).date()
            rows.append((ts, float(k.get("quote_volume", 0) or k.get("vol", 0))))
    result = _rolling_7day(rows, "BitMart")
    if result:
        result["open_interest"] = oi
    return result


def get_perp_volume_lbank() -> dict | None:
    data = safe_get("https://lbkperp.lbank.com/cfd/openApi/v1/pub/kline",
                    params={"productGroup": "SwapU", "symbol": "ensousdt",
                            "interval": "day1", "size": "30"})
    oi_data = safe_get("https://lbkperp.lbank.com/cfd/openApi/v1/pub/ticker",
                       params={"productGroup": "SwapU", "symbol": "ensousdt"})
    oi = 0
    if oi_data and "data" in oi_data:
        d = oi_data["data"]
        if isinstance(d, list) and d:
            oi = float(d[0].get("holdVol", 0) or 0)
        elif isinstance(d, dict):
            oi = float(d.get("holdVol", 0) or 0)
    if not data or "data" not in data:
        return None
    rows = []
    klines = data["data"]
    if not isinstance(klines, list):
        return None
    for k in klines:
        if isinstance(k, list) and len(k) > 6:
            ts = datetime.fromtimestamp(int(k[0]) / 1000, tz=timezone.utc).date()
            rows.append((ts, float(k[6])))
    result = _rolling_7day(rows, "LBank")
    if result:
        result["open_interest"] = oi
    return result


# ── Cached data aggregators ──────────────────────────────────────────────────
@st.cache_data(ttl=600, show_spinner=False)
def load_holdings(api_key: str):
    """Fetch all foundation holdings from on-chain."""
    results = {"liquid": 0, "liquid_bsc": 0, "vested_unclaimed": 0, "locked_vesting": 0,
               "staked_total": 0, "staked_expired": 0, "staked_locked": 0,
               "rewards": 0, "wallets": {}, "positions": []}

    for name, info in FOUNDATION_WALLETS.items():
        if info["type"] == "vesting":
            vi = get_vesting_info(info["addr"], api_key)
            results["vested_unclaimed"] += vi["vested_unclaimed"]
            results["locked_vesting"] += vi["locked"]
            results["wallets"][name] = {
                "address": info["addr"], "type": "vesting",
                "balance": vi["balance"],
                "vested_unclaimed": vi["vested_unclaimed"],
                "locked": vi["locked"],
            }
        else:
            bal = get_token_balance(info["addr"], api_key)
            bsc_bal = get_bsc_token_balance(info["addr"])
            results["liquid"] += bal
            results["liquid_bsc"] += bsc_bal
            wallet_data = {"address": info["addr"], "type": "liquid",
                           "balance": bal, "bsc_balance": bsc_bal}

            if name == "treasury":
                positions = get_staked_positions(info["addr"])
                total_rewards = 0
                for p in positions:
                    r = get_position_rewards(p["id"])
                    p["rewards"] = r
                    total_rewards += r
                    time.sleep(0.2)
                staked_total = sum(p["deposit"] for p in positions)
                staked_expired = sum(p["deposit"] for p in positions if p["is_expired"])
                staked_locked = sum(p["deposit"] for p in positions if not p["is_expired"])
                results["staked_total"] = staked_total
                results["staked_expired"] = staked_expired
                results["staked_locked"] = staked_locked
                results["rewards"] = total_rewards
                results["positions"] = positions
                wallet_data["staked"] = staked_total
                wallet_data["positions_count"] = len(positions)

            results["wallets"][name] = wallet_data
        time.sleep(0.2)

    results["total_sellable"] = (results["liquid"] + results["liquid_bsc"]
                                  + results["vested_unclaimed"]
                                  + results["staked_expired"] + results["rewards"])
    results["total_holdings"] = (results["liquid"] + results["liquid_bsc"]
                                  + results["vested_unclaimed"]
                                  + results["locked_vesting"] + results["staked_total"]
                                  + results["rewards"])
    return results


@st.cache_data(ttl=600, show_spinner=False)
def load_spot_volumes():
    fetchers = [
        get_spot_volume_binance, get_spot_volume_bybit, get_spot_volume_okx,
        get_spot_volume_bitget, get_spot_volume_gate, get_spot_volume_kraken,
        get_spot_volume_bitmart, get_spot_volume_phemex, get_spot_volume_kucoin,
        get_spot_volume_bingx, get_spot_volume_mexc, get_spot_volume_upbit,
        get_spot_volume_bithumb, get_spot_volume_cointr,
    ]
    results = []
    for fn in fetchers:
        try:
            r = fn()
            if r:
                results.append(r)
        except Exception:
            pass
        time.sleep(0.15)
    return results


@st.cache_data(ttl=600, show_spinner=False)
def load_perp_volumes():
    fetchers = [
        get_perp_volume_binance, get_perp_volume_bybit, get_perp_volume_gate,
        get_perp_volume_okx, get_perp_volume_bitget, get_perp_volume_bingx,
        get_perp_volume_mexc, get_perp_volume_phemex, get_perp_volume_kucoin,
        get_perp_volume_bitmart, get_perp_volume_lbank,
    ]
    results = []
    for fn in fetchers:
        try:
            r = fn()
            if r:
                results.append(r)
        except Exception:
            pass
        time.sleep(0.15)
    return results


# ── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("ENSO Foundation Report")
    st.caption("Live report")
    st.divider()

    try:
        api_key = st.secrets["ETHERSCAN_API_KEY"]
    except (KeyError, FileNotFoundError):
        api_key = os.getenv("ETHERSCAN_API_KEY", "")
    eth_key = st.text_input("Etherscan API Key", value=api_key, type="password")

    try:
        cg_key_default = st.secrets["COINGECKO_API_KEY"]
    except (KeyError, FileNotFoundError):
        cg_key_default = os.getenv("COINGECKO_API_KEY", "")
    cg_key = st.text_input("CoinGecko API Key", value=cg_key_default, type="password")

    if not eth_key:
        st.warning("Enter Etherscan API key.")
        st.stop()

    st.divider()
    st.markdown("**Market Makers Holdings**")
    mm_amber = st.number_input(
        "Amber",
        min_value=0, value=0, step=10000,
        help="ENSO tokens held by Amber. Added to Foundation totals.",
    )
    mm_jpeg = st.number_input(
        "Jpeg",
        min_value=0, value=0, step=10000,
        help="ENSO tokens held by Jpeg. Added to Foundation totals.",
    )
    mm_holdings = mm_amber + mm_jpeg

    st.divider()
    refresh = st.button("🔄 Refresh all data", use_container_width=True)
    if refresh:
        st.cache_data.clear()

# ── Load data ────────────────────────────────────────────────────────────────
with st.spinner("Fetching on-chain holdings..."):
    holdings = load_holdings(eth_key)

with st.spinner("Fetching exchange volumes..."):
    spot_vols = load_spot_volumes()
    perp_vols = load_perp_volumes()

enso_price = get_enso_price(cg_key)
supply = get_circulating_supply()
vesting_proj = calculate_vesting_projection(BINANCE_DUE_DATE)
now_dt = datetime.now(timezone.utc)

# Adjust totals with MM holdings
total_holdings_adj = holdings["total_holdings"] + mm_holdings
total_sellable_adj = holdings["total_sellable"]

# ── Header ───────────────────────────────────────────────────────────────────
st.markdown("## ENSO Foundation Report")
st.caption(f"Generated {now_dt.strftime('%B %d, %Y at %H:%M UTC')}"
           + (f" · ENSO Price: **${enso_price:.4f}**" if enso_price else ""))

# ── KPI row ──────────────────────────────────────────────────────────────────
k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Total Holdings", f"{total_holdings_adj:,.0f}")
k2.metric("Total Sellable", f"{total_sellable_adj:,.0f}")
k3.metric("Circulating Supply", f"{supply['circulating']:,.0f}" if supply["circulating"] else "N/A")
if enso_price:
    k4.metric("Holdings Value", f"${total_holdings_adj * enso_price:,.0f}")
else:
    k4.metric("ENSO Price", "N/A")
total_spot_daily = sum(v["avg_daily"] for v in spot_vols)
k5.metric("Avg Daily Spot Vol", f"${total_spot_daily:,.0f}")
total_perp_daily = sum(v["avg_daily"] for v in perp_vols)
k6.metric("Avg Daily Perp Vol", f"${total_perp_daily:,.0f}")

st.divider()

# ── Tabs ─────────────────────────────────────────────────────────────────────
tabs = st.tabs([
    "🏦 Treasury Holdings", "📜 Vesting Detail", "🔒 Staked Positions",
    "📊 Spot Volumes", "📈 Perp Volumes",
])

# ═══════════════ TAB 1: Treasury Holdings ═══════════════════════════════════
with tabs[0]:
    col1, col2 = st.columns([1, 1])

    with col1:
        st.markdown("#### Holdings Breakdown")
        breakdown = {
            "Liquid — ETH": holdings["liquid"],
            "Liquid — BSC": holdings["liquid_bsc"],
            "Vested (claimable)": holdings["vested_unclaimed"],
            "Staked (expired)": holdings["staked_expired"],
            "Rewards (claimable)": holdings["rewards"],
            "MM — Amber": mm_amber,
            "MM — Jpeg": mm_jpeg,
            "Staked (locked)": holdings["staked_locked"],
            "Locked (unvested)": holdings["locked_vesting"],
        }
        bd_df = pd.DataFrame([
            {"Category": k, "Amount": v, "Sellable": k not in ("Staked (locked)", "Locked (unvested)", "MM — Amber", "MM — Jpeg")}
            for k, v in breakdown.items() if v > 0
        ])
        if not bd_df.empty:
            bd_df["Formatted"] = bd_df["Amount"].apply(lambda x: f"{x:,.0f}")
            st.dataframe(
                bd_df[["Category", "Formatted", "Sellable"]].rename(
                    columns={"Formatted": "ENSO Tokens"}),
                use_container_width=True, hide_index=True,
            )

        st.markdown("---")
        st.markdown(f"**Total Sellable: {total_sellable_adj:,.0f} ENSO**")
        st.caption("Sellable = Liquid (ETH + BSC) + Vested + Staked Expired + Rewards")

        # Binance obligation
        st.markdown("---")
        st.markdown("#### Binance Obligation")
        st.markdown(f"- **Due:** {BINANCE_DUE_DATE}")
        st.markdown(f"- **Amount:** {BINANCE_OBLIGATION:,.0f} ENSO")
        st.markdown(f"- **Vesting until then:** ~{vesting_proj:,.0f} ENSO")
        net_after = total_sellable_adj - BINANCE_OBLIGATION + vesting_proj
        st.markdown(f"- **Net sellable after obligation:** {net_after:,.0f} ENSO")

    with col2:
        st.markdown("#### Holdings Distribution")
        sellable_items = {k: v for k, v in breakdown.items() if v > 0}
        fig = px.pie(
            names=list(sellable_items.keys()),
            values=list(sellable_items.values()),
            hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig.update_traces(textposition="inside", textinfo="percent+label",
                          hovertemplate="%{label}<br>%{value:,.0f} ENSO<extra></extra>")
        fig.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

        if supply["circulating"]:
            st.markdown("#### Supply Context")
            st.markdown(f"- Circulating: **{supply['circulating']:,.0f}** ENSO")
            st.markdown(f"- Total Supply: **{supply['total']:,.0f}** ENSO")

    # Wallet breakdown
    st.markdown("#### Wallet Breakdown")
    wallet_rows = []
    for name, w in holdings["wallets"].items():
        row = {"Wallet": name.replace("_", " ").title(), "Address": short_addr(w["address"])}
        if w["type"] == "vesting":
            row["ETH Balance"] = f"{w['balance']:,.0f}"
            row["BSC Balance"] = ""
            row["Claimable"] = f"{w['vested_unclaimed']:,.0f}"
            row["Locked"] = f"{w['locked']:,.0f}"
        else:
            bsc = w.get("bsc_balance", 0)
            row["ETH Balance"] = f"{w['balance']:,.0f}"
            row["BSC Balance"] = f"{bsc:,.0f}" if bsc > 0 else ""
            row["Claimable"] = ""
            row["Locked"] = ""
        wallet_rows.append(row)
    st.dataframe(pd.DataFrame(wallet_rows), use_container_width=True, hide_index=True)

# ═══════════════ TAB 2: Vesting Detail ══════════════════════════════════════
with tabs[1]:
    st.markdown("#### Vesting Contracts")
    for name, w in holdings["wallets"].items():
        if w["type"] != "vesting":
            continue
        st.markdown(f"**{name.replace('_', ' ').title()}** — `{w['address']}`")
        vc1, vc2, vc3 = st.columns(3)
        vc1.metric("Claimable", f"{w['vested_unclaimed']:,.0f}")
        vc2.metric("Locked", f"{w['locked']:,.0f}")
        vc3.metric("Contract Balance", f"{w['balance']:,.0f}")
        if w["balance"] > 0:
            pct_vested = w["vested_unclaimed"] / w["balance"] * 100
            st.progress(min(pct_vested / 100, 1.0), text=f"{pct_vested:.1f}% vested")
        st.markdown("---")

    st.markdown("#### Vesting Projection")
    st.markdown(f"Additional tokens vesting until **{BINANCE_DUE_DATE}**: **{vesting_proj:,.0f} ENSO**")

    proj_rows = []
    now_ts = int(now_dt.timestamp())
    target_ts = int(datetime.strptime(BINANCE_DUE_DATE, "%Y-%m-%d").replace(
        tzinfo=timezone.utc).timestamp())
    for v in VESTING_SCHEDULES:
        rate = v["total"] / (v["end"] - v["start"])
        eff = min(target_ts, v["end"])
        will_vest = max(0, rate * (eff - now_ts)) if eff > now_ts else 0
        proj_rows.append({
            "Contract": v["name"],
            "Total Allocation": f"{v['total']:,.0f}",
            "Will Vest": f"{will_vest:,.0f}",
            "Daily Rate": f"{rate * 86400:,.0f}",
        })
    st.dataframe(pd.DataFrame(proj_rows), use_container_width=True, hide_index=True)

# ═══════════════ TAB 3: Staked Positions ════════════════════════════════════
with tabs[2]:
    positions = holdings["positions"]
    if not positions:
        st.info("No staking positions found.")
    else:
        st.markdown(f"#### Treasury Staking — {len(positions)} positions")
        mc1, mc2, mc3, mc4 = st.columns(4)
        mc1.metric("Total Staked", f"{holdings['staked_total']:,.0f}")
        mc2.metric("Expired (claimable)", f"{holdings['staked_expired']:,.0f}")
        mc3.metric("Locked", f"{holdings['staked_locked']:,.0f}")
        mc4.metric("Rewards Available", f"{holdings['rewards']:,.0f}")

        pos_df = pd.DataFrame(positions)
        pos_df["status"] = pos_df["is_expired"].apply(lambda x: "Expired" if x else "Locked")
        pos_df["deposit_fmt"] = pos_df["deposit"].apply(lambda x: f"{x:,.0f}")
        pos_df["rewards_fmt"] = pos_df["rewards"].apply(lambda x: f"{x:,.0f}")

        st.dataframe(
            pos_df[["id", "deposit_fmt", "rewards_fmt", "expiry_utc", "status"]].rename(columns={
                "id": "Position ID", "deposit_fmt": "Staked (ENSO)",
                "rewards_fmt": "Rewards", "expiry_utc": "Expiry", "status": "Status",
            }),
            use_container_width=True, hide_index=True, height=400,
        )

        # Locked positions by expiry
        locked_pos = [p for p in positions if not p["is_expired"]]
        if locked_pos:
            ldf = pd.DataFrame(locked_pos)
            ldf["expiry_date"] = ldf["expiry_utc"].apply(lambda x: x.date())
            agg = ldf.groupby("expiry_date")["deposit"].sum().reset_index()
            agg.sort_values("expiry_date", inplace=True)
            fig_lock = px.bar(agg, x="expiry_date", y="deposit",
                              title="Locked Staked Tokens by Expiry Date",
                              labels={"deposit": "ENSO", "expiry_date": "Unlock Date"},
                              color_discrete_sequence=["#8b5cf6"])
            fig_lock.update_traces(hovertemplate="%{x}<br>%{y:,.0f} ENSO<extra></extra>",
                                   texttemplate="%{y:,.0f}", textposition="outside")
            fig_lock.update_layout(yaxis_tickformat=",.", height=350)
            st.plotly_chart(fig_lock, use_container_width=True)

# ═══════════════ TAB 4: Spot Volumes ════════════════════════════════════════
with tabs[3]:
    st.markdown("#### Exchange Average Daily Volume (Last 7 Days)")
    if enso_price:
        st.caption(f"ENSO Spot Price: **${enso_price:.4f}**")

    if not spot_vols:
        st.warning("Could not fetch spot volume data.")
    else:
        spot_df = pd.DataFrame(spot_vols)
        spot_df.sort_values("volume_7d", ascending=False, inplace=True)

        if enso_price and enso_price > 0:
            spot_df["avg_daily_enso"] = spot_df["avg_daily"] / enso_price
        else:
            spot_df["avg_daily_enso"] = 0

        fmt_spot = spot_df.copy()
        fmt_spot["avg_daily"] = fmt_spot["avg_daily"].apply(lambda x: f"${x:,.0f}")
        fmt_spot["volume_7d"] = fmt_spot["volume_7d"].apply(lambda x: f"${x:,.0f}")
        fmt_spot["avg_daily_enso"] = fmt_spot["avg_daily_enso"].apply(lambda x: f"{x:,.0f}")
        fmt_spot["wow_pct"] = fmt_spot["wow_pct"].apply(
            lambda x: f"{x:+.1f}%" if pd.notna(x) else "N/A")

        st.dataframe(
            fmt_spot[["exchange", "avg_daily", "avg_daily_enso", "volume_7d", "wow_pct"]].rename(columns={
                "exchange": "Exchange", "avg_daily": "Avg Daily (USD)",
                "avg_daily_enso": "Avg Daily (ENSO)", "volume_7d": "7-Day Total (USD)",
                "wow_pct": "WoW Change",
            }),
            use_container_width=True, hide_index=True,
        )

        # Bar chart
        fig_spot = px.bar(spot_df, x="exchange", y="avg_daily",
                          title="Average Daily Spot Volume by Exchange (USD)",
                          labels={"avg_daily": "USD", "exchange": ""},
                          color_discrete_sequence=["#3b82f6"])
        fig_spot.update_traces(hovertemplate="%{x}<br>$%{y:,.0f}<extra></extra>")
        fig_spot.update_layout(yaxis_tickformat=",.", height=380)
        st.plotly_chart(fig_spot, use_container_width=True)

        st.metric("Total Avg Daily Spot Volume", f"${total_spot_daily:,.0f}")

# ═══════════════ TAB 5: Perp Volumes ════════════════════════════════════════
with tabs[4]:
    st.markdown("#### Perpetuals Average Daily Volume (Last 7 Days)")

    if not perp_vols:
        st.warning("Could not fetch perpetuals volume data.")
    else:
        perp_df = pd.DataFrame(perp_vols)
        perp_df.sort_values("volume_7d", ascending=False, inplace=True)

        fmt_perp = perp_df.copy()
        fmt_perp["avg_daily"] = fmt_perp["avg_daily"].apply(lambda x: f"${x:,.0f}")
        fmt_perp["volume_7d"] = fmt_perp["volume_7d"].apply(lambda x: f"${x:,.0f}")
        if "open_interest" in fmt_perp.columns:
            fmt_perp["open_interest"] = fmt_perp["open_interest"].apply(
                lambda x: f"{x:,.0f}" if x else "N/A")
        else:
            fmt_perp["open_interest"] = "N/A"
        fmt_perp["wow_pct"] = fmt_perp["wow_pct"].apply(
            lambda x: f"{x:+.1f}%" if pd.notna(x) else "N/A")

        st.dataframe(
            fmt_perp[["exchange", "avg_daily", "volume_7d", "open_interest", "wow_pct"]].rename(columns={
                "exchange": "Exchange", "avg_daily": "Avg Daily Vol (USD)",
                "volume_7d": "7-Day Total (USD)",
                "open_interest": "Open Interest", "wow_pct": "WoW Change",
            }),
            use_container_width=True, hide_index=True,
        )

        fig_perp = px.bar(perp_df, x="exchange", y="avg_daily",
                          title="Average Daily Perpetuals Volume by Exchange (USD)",
                          labels={"avg_daily": "USD", "exchange": ""},
                          color_discrete_sequence=["#22c55e"])
        fig_perp.update_traces(hovertemplate="%{x}<br>$%{y:,.0f}<extra></extra>")
        fig_perp.update_layout(yaxis_tickformat=",.", height=380)
        st.plotly_chart(fig_perp, use_container_width=True)

        st.metric("Total Avg Daily Perp Volume", f"${total_perp_daily:,.0f}")

# ── Footer ───────────────────────────────────────────────────────────────────
st.divider()
st.caption(
    f"ENSO Foundation Report · Generated {now_dt.strftime('%Y-%m-%d %H:%M UTC')} · "
    f"Data cached for 10 minutes"
)
