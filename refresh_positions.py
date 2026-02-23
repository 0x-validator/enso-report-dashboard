"""
Refresh enso_positions.csv from on-chain Etherscan event logs.

Standalone script used by GitHub Actions to keep the committed CSV
up-to-date so the dashboard always starts with fresh position data.

Usage:  python refresh_positions.py
Requires ETHERSCAN_API_KEY env var.
"""

import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

# ── Constants (mirror report_dashboard.py) ───────────────────────────────────
STAKING_CONTRACT = "0x22Ad2a46d317C5eDF6c01fea16d4399C912E9A01"
ETHERSCAN_URL = "https://api.etherscan.io/v2/api"
DECIMALS = 18

TOPIC_POSITION_CREATED = "0x34e49ed13d7eb52832aff120e7482f7b6e7e0328254ca90ee5834a845a87c3b2"
TOPIC_FUNDS_DEPOSITED  = "0xed2de103da084463a1b2895568d352fd796dfd1d033c0e8ee9fabe73a6715389"
TOPIC_FUNDS_WITHDRAWN  = "0xd66662c0ded9e58fd31d5e44944bcfd07ffc15e6927ecc1382e7941cb7bd24c4"
TOPIC_TRANSFER         = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


# ── Helpers ──────────────────────────────────────────────────────────────────
def _decode_word(data: str, index: int) -> int:
    start = 2 + index * 64
    return int(data[start : start + 64], 16)


def _fetch_staking_logs(api_key: str, topic0: str) -> list[dict]:
    all_logs: list[dict] = []
    from_block = 0
    while True:
        resp = requests.get(ETHERSCAN_URL, params={
            "chainid": "1", "module": "logs", "action": "getLogs",
            "address": STAKING_CONTRACT, "topic0": topic0,
            "fromBlock": from_block, "toBlock": "latest", "apikey": api_key,
        }, timeout=30)
        if resp.status_code != 200:
            break
        data = resp.json()
        if not data or data.get("status") != "1" or not data.get("result"):
            break
        logs = data["result"]
        all_logs.extend(logs)
        if len(logs) >= 1000:
            from_block = int(logs[-1]["blockNumber"], 16) + 1
            time.sleep(0.25)
        else:
            break
    return all_logs


def refresh_positions(api_key: str) -> pd.DataFrame | None:
    pc_logs = _fetch_staking_logs(api_key, TOPIC_POSITION_CREATED)
    dep_logs = _fetch_staking_logs(api_key, TOPIC_FUNDS_DEPOSITED)
    wth_logs = _fetch_staking_logs(api_key, TOPIC_FUNDS_WITHDRAWN)
    tr_logs = _fetch_staking_logs(api_key, TOPIC_TRANSFER)

    if not pc_logs:
        return None

    now = int(datetime.now(timezone.utc).timestamp())
    positions: dict[int, dict] = {}

    for log in pc_logs:
        pid = int(log["topics"][1], 16)
        expiry = int(log["data"][2:66], 16)
        validator = bytes.fromhex(log["topics"][2][2:]).rstrip(b"\x00").decode("utf-8", errors="replace")
        positions[pid] = {
            "position_id": pid, "expiry_ts": expiry, "validator": validator,
            "owner": None, "net_deposited": 0.0, "stake": 0.0,
        }

    for log in tr_logs:
        to_addr = "0x" + log["topics"][2][-40:]
        token_id = int(log["topics"][3], 16)
        if token_id in positions:
            positions[token_id]["owner"] = to_addr

    for log in dep_logs:
        pid = int(log["topics"][1], 16)
        if pid in positions:
            positions[pid]["net_deposited"] += _decode_word(log["data"], 0) / 10**DECIMALS
            positions[pid]["stake"] += _decode_word(log["data"], 1) / 10**DECIMALS

    for log in wth_logs:
        pid = int(log["topics"][1], 16)
        if pid in positions:
            positions[pid]["net_deposited"] -= _decode_word(log["data"], 0) / 10**DECIMALS

    df = pd.DataFrame(positions.values())
    df = df[df["net_deposited"] > 0].copy()
    df["expiry_utc"] = pd.to_datetime(df["expiry_ts"], unit="s", utc=True).dt.strftime("%Y-%m-%d %H:%M")
    df["unlock_remaining"] = df["expiry_ts"].apply(
        lambda t: "UNLOCKED" if t <= now else f"{(t - now) // 86400}d {((t - now) % 86400) // 3600}h"
    )
    df = df.sort_values("stake", ascending=False)
    return df[["position_id", "expiry_ts", "expiry_utc", "unlock_remaining",
                "validator", "owner", "net_deposited", "stake"]]


def main():
    api_key = os.environ.get("ETHERSCAN_API_KEY", "")
    if not api_key:
        print("ERROR: ETHERSCAN_API_KEY not set")
        sys.exit(1)

    print("Fetching staking positions from Etherscan...")
    df = refresh_positions(api_key)

    if df is None or df.empty:
        print("ERROR: No positions returned from Etherscan")
        sys.exit(1)

    out_path = Path(__file__).resolve().parent / "enso_positions.csv"
    df.to_csv(str(out_path), index=False)
    print(f"Saved {len(df)} positions to {out_path}")


if __name__ == "__main__":
    main()
