"""
ENSO Foundation — Telegram Treasury Report Bot
===============================================
Self-contained script that fetches treasury holdings, vesting, staking,
and rewards data, then sends a formatted summary to Telegram.

Usage:
  python telegram_report.py

Cron (every Monday at 9 AM EST):
  0 14 * * 1  cd /path/to/Foundation_Dashboard && python telegram_report.py
"""

import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd
import requests
from dotenv import load_dotenv
from eth_abi import encode as abi_encode, decode as abi_decode

# ── Load environment ─────────────────────────────────────────────────────────
load_dotenv()

ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

if not ETHERSCAN_API_KEY:
    sys.exit("Error: ETHERSCAN_API_KEY not set in .env")
if not TELEGRAM_BOT_TOKEN:
    sys.exit("Error: TELEGRAM_BOT_TOKEN not set in .env")
if not TELEGRAM_CHAT_ID:
    sys.exit("Error: TELEGRAM_CHAT_ID not set in .env")

# ── Constants (from report_dashboard.py) ──────────────────────────────────────
ENSO_CONTRACT = "0x699F088b5DddcAFB7c4824db5B10B57B37cB0C66"
STAKING_CONTRACT = "0x22Ad2a46d317C5eDF6c01fea16d4399C912E9A01"
DECIMALS = 18
ETHERSCAN_URL = "https://api.etherscan.io/v2/api"
BSC_RPC_URL = "https://bsc-dataseed.binance.org/"
BSC_ENSO_CONTRACT = "0xfeb339236d25d3e415f280189bc7c2fbab6ae9ef"
MULTICALL3 = "0xcA11bde05977b3631167028862bE2a173976CA11"
RPC_URLS = ["https://eth.llamarpc.com", "https://1rpc.io/eth",
            "https://cloudflare-eth.com"]

FOUNDATION_WALLETS = {
    "treasury": {"addr": "0x715b1ddf5d6da6846eadb72d3d6f9d93148d0bb0", "type": "liquid"},
    "vesting_contract": {"addr": "0x4110d73ff4d4fe45af2762df2205ad95c8c9679b", "type": "vesting"},
    "operational": {"addr": "0xd782d294bc3e8b1a32ec9283b02893fd932d3ece", "type": "liquid"},
    "vesting_operational_1": {"addr": "0x3dea6f0f4d3fcd9a706e8b6b0750ab2f57dec17a", "type": "vesting"},
    "vesting_operational_2": {"addr": "0x332e5b70e451bdeebd36b5d3442827aa52a42f80", "type": "vesting"},
    "vesting_operational_3": {"addr": "0xa3314896ae22caf4bfdcb0bd4c3cabce324d8f3e", "type": "vesting"},
}

MM_DEFAULTS = {"amber": 330_940, "jpeg": 537_202}
FOUNDATION_STAKING_ADDR = "0x715b1ddf5d6da6846eadb72d3d6f9d93148d0bb0"

REWARDS_SCHEDULE = [
    ("2025-11-14", 629532.04), ("2025-12-14", 606812.70), ("2026-01-14", 573129.66),
    ("2026-02-14", 531316.33), ("2026-03-14", 484654.38), ("2026-04-14", 436452.71),
    ("2026-05-14", 389669.23), ("2026-06-14", 346635.34), ("2026-07-14", 308914.21),
    ("2026-08-14", 277292.38), ("2026-09-14", 251877.93), ("2026-10-14", 232264.45),
    ("2026-11-14", 217717.58), ("2026-12-14", 207348.92), ("2027-01-14", 200254.63),
    ("2027-02-14", 195609.57), ("2027-03-14", 192718.63), ("2027-04-14", 191033.29),
    ("2027-05-14", 190144.57), ("2027-06-14", 189762.60), ("2027-07-14", 189691.36),
    ("2027-08-14", 189803.97), ("2027-09-14", 190021.68), ("2027-10-14", 190297.54),
    ("2027-11-14", 190604.61), ("2027-12-14", 190928.03), ("2028-01-14", 191259.87),
    ("2028-02-14", 191596.08), ("2028-03-14", 191934.64), ("2028-04-14", 192274.60),
    ("2028-05-14", 192615.50), ("2028-06-14", 192957.15), ("2028-07-14", 193299.47),
    ("2028-08-14", 193642.41), ("2028-09-14", 193985.98), ("2028-10-14", 194330.16),
    ("2028-11-14", 194674.95), ("2028-12-14", 195020.35), ("2029-01-14", 195366.36),
    ("2029-02-14", 195712.99), ("2029-03-14", 196060.24), ("2029-04-14", 196408.10),
    ("2029-05-14", 196756.57), ("2029-06-14", 197105.67), ("2029-07-14", 197455.38),
    ("2029-08-14", 197805.72), ("2029-09-14", 198156.68), ("2029-10-14", 198508.26),
    ("2029-11-14", 198860.46), ("2029-12-14", 199213.29), ("2030-01-14", 199566.74),
    ("2030-02-14", 199920.82), ("2030-03-14", 200275.53), ("2030-04-14", 200630.87),
    ("2030-05-14", 200986.84), ("2030-06-14", 201343.44), ("2030-07-14", 201700.67),
    ("2030-08-14", 202058.54), ("2030-09-14", 202417.04), ("2030-10-14", 202776.18),
    ("2030-11-14", 203135.96), ("2030-12-14", 203496.37), ("2031-01-14", 203857.43),
    ("2031-02-14", 204219.12), ("2031-03-14", 204581.46), ("2031-04-14", 204944.43),
    ("2031-05-14", 205308.06), ("2031-06-14", 205672.32), ("2031-07-14", 206037.24),
    ("2031-08-14", 206402.80), ("2031-09-14", 206769.01), ("2031-10-14", 207135.87),
    ("2031-11-14", 207503.38), ("2031-12-14", 207871.54), ("2032-01-14", 208240.36),
    ("2032-02-14", 208609.83), ("2032-03-14", 208979.96), ("2032-04-14", 209350.74),
    ("2032-05-14", 209722.18), ("2032-06-14", 210094.28), ("2032-07-14", 210467.04),
    ("2032-08-14", 210840.46), ("2032-09-14", 211214.54), ("2032-10-14", 211589.29),
    ("2032-11-14", 211964.70), ("2032-12-14", 212340.78), ("2033-01-14", 212717.53),
    ("2033-02-14", 213094.94), ("2033-03-14", 213473.03), ("2033-04-14", 213851.78),
    ("2033-05-14", 214231.21), ("2033-06-14", 214611.31), ("2033-07-14", 214992.08),
    ("2033-08-14", 215373.53), ("2033-09-14", 215755.66), ("2033-10-14", 216138.46),
    ("2033-11-14", 216521.95), ("2033-12-14", 216906.11), ("2034-01-14", 217290.95),
    ("2034-02-14", 217676.48), ("2034-03-14", 218062.70), ("2034-04-14", 218449.59),
    ("2034-05-14", 218837.18), ("2034-06-14", 219225.45), ("2034-07-14", 219614.41),
    ("2034-08-14", 220004.06), ("2034-09-14", 220394.40), ("2034-10-14", 220785.44),
    ("2034-11-14", 221177.17), ("2034-12-14", 221569.59), ("2035-01-14", 221962.71),
    ("2035-02-14", 222356.53), ("2035-03-14", 222751.04), ("2035-04-14", 223146.26),
    ("2035-05-14", 223542.18), ("2035-06-14", 223938.80), ("2035-07-14", 224336.12),
    ("2035-08-14", 224734.15), ("2035-09-14", 225132.88), ("2035-10-14", 225532.33),
]

TOPIC_POSITION_CREATED = "0x34e49ed13d7eb52832aff120e7482f7b6e7e0328254ca90ee5834a845a87c3b2"
TOPIC_FUNDS_DEPOSITED  = "0xed2de103da084463a1b2895568d352fd796dfd1d033c0e8ee9fabe73a6715389"
TOPIC_FUNDS_WITHDRAWN  = "0xd66662c0ded9e58fd31d5e44944bcfd07ffc15e6927ecc1382e7941cb7bd24c4"
TOPIC_TRANSFER         = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


# ── Helpers ──────────────────────────────────────────────────────────────────
def safe_get(url, params=None, headers=None, timeout=15):
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=timeout)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


def safe_post(url, json_body, timeout=15):
    try:
        resp = requests.post(url, json=json_body, timeout=timeout)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


def _decode_word(data, index):
    start = 2 + index * 64
    return int(data[start : start + 64], 16)


def _fetch_staking_logs(topic0):
    all_logs = []
    from_block = 0
    while True:
        data = safe_get(ETHERSCAN_URL, params={
            "chainid": "1", "module": "logs", "action": "getLogs",
            "address": STAKING_CONTRACT, "topic0": topic0,
            "fromBlock": from_block, "toBlock": "latest",
            "apikey": ETHERSCAN_API_KEY,
        }, timeout=30)
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


def fmt(value):
    return f"{value:,.0f}"


# ── On-chain data functions ──────────────────────────────────────────────────
def get_token_balance(wallet):
    data = safe_get(ETHERSCAN_URL, params={
        "chainid": "1", "module": "account", "action": "tokenbalance",
        "contractaddress": ENSO_CONTRACT, "address": wallet,
        "tag": "latest", "apikey": ETHERSCAN_API_KEY,
    })
    if data and data.get("status") == "1":
        return int(data["result"]) / 10**DECIMALS
    return 0.0


def get_bsc_token_balance(wallet):
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


def get_vesting_info(contract):
    balance = get_token_balance(contract)
    data = safe_get(ETHERSCAN_URL, params={
        "chainid": "1", "module": "proxy", "action": "eth_call",
        "to": contract, "data": "0xfbccedae",
        "tag": "latest", "apikey": ETHERSCAN_API_KEY,
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
    return {"balance": balance, "vested_unclaimed": vested_unclaimed, "locked": locked}


def get_staked_positions(owner):
    now = int(datetime.now(timezone.utc).timestamp())
    script_dir = os.path.dirname(os.path.abspath(__file__))
    for csv_path in [
        os.path.join(script_dir, "enso_positions.csv"),
        os.path.join(script_dir, "..", "Staking", "enso_positions.csv"),
    ]:
        if os.path.exists(csv_path):
            try:
                df = pd.read_csv(csv_path)
                fdn = df[df["owner"].str.lower() == owner.lower()]
                if not fdn.empty:
                    positions = []
                    for _, row in fdn.iterrows():
                        expiry = int(row["expiry_ts"])
                        positions.append({
                            "id": int(row["position_id"]),
                            "deposit": float(row["net_deposited"]),
                            "stake": float(row["stake"]),
                            "expiry_ts": expiry,
                            "is_expired": expiry <= now,
                        })
                    return positions
            except Exception:
                pass
    return []


def get_all_position_rewards(position_ids):
    if not position_ids:
        return {}
    calls = []
    staking_addr = bytes.fromhex(STAKING_CONTRACT[2:])
    for pid in position_ids:
        call_data = bytes.fromhex("61c02efb") + pid.to_bytes(32, "big")
        calls.append((staking_addr, True, call_data))
    encoded = "0x82ad56cb" + abi_encode(["(address,bool,bytes)[]"], [calls]).hex()
    body = {
        "jsonrpc": "2.0", "id": 1, "method": "eth_call",
        "params": [{"to": MULTICALL3, "data": encoded}, "latest"],
    }
    for rpc in RPC_URLS:
        data = safe_post(rpc, body, timeout=30)
        if not data or not data.get("result") or data["result"] == "0x":
            continue
        try:
            raw = bytes.fromhex(data["result"][2:])
            decoded = abi_decode(["(bool,bytes)[]"], raw)[0]
            rewards = {}
            for i, (success, ret_data) in enumerate(decoded):
                pid = position_ids[i]
                if success and len(ret_data) >= 32:
                    rewards[pid] = int.from_bytes(ret_data[:32], "big") / 10**DECIMALS
                else:
                    rewards[pid] = 0.0
            return rewards
        except Exception:
            continue
    return {pid: 0.0 for pid in position_ids}


# ── Holdings aggregator ──────────────────────────────────────────────────────
def load_holdings():
    results = {
        "liquid": 0, "liquid_bsc": 0, "vested_unclaimed": 0, "locked_vesting": 0,
        "staked_total": 0, "staked_expired": 0, "staked_locked": 0,
        "rewards": 0, "positions": [],
    }
    for name, info in FOUNDATION_WALLETS.items():
        if info["type"] == "vesting":
            vi = get_vesting_info(info["addr"])
            results["vested_unclaimed"] += vi["vested_unclaimed"]
            results["locked_vesting"] += vi["locked"]
        else:
            bal = get_token_balance(info["addr"])
            bsc_bal = get_bsc_token_balance(info["addr"])
            results["liquid"] += bal
            results["liquid_bsc"] += bsc_bal
            if name == "treasury":
                positions = get_staked_positions(info["addr"])
                rewards_map = get_all_position_rewards([p["id"] for p in positions])
                total_rewards = 0
                for p in positions:
                    p["rewards"] = rewards_map.get(p["id"], 0.0)
                    total_rewards += p["rewards"]
                results["staked_total"] = sum(p["deposit"] for p in positions)
                results["staked_expired"] = sum(p["deposit"] for p in positions if p["is_expired"])
                results["staked_locked"] = sum(p["deposit"] for p in positions if not p["is_expired"])
                results["rewards"] = total_rewards
                results["positions"] = positions
        time.sleep(0.2)

    mm_holdings = sum(MM_DEFAULTS.values())
    results["mm_holdings"] = mm_holdings
    results["total_sellable"] = (results["liquid"] + results["liquid_bsc"]
                                  + results["vested_unclaimed"]
                                  + results["staked_expired"] + results["rewards"])
    results["total_holdings"] = (results["liquid"] + results["liquid_bsc"]
                                  + results["vested_unclaimed"]
                                  + results["locked_vesting"] + results["staked_total"]
                                  + results["rewards"] + mm_holdings)
    return results


# ── Staking rewards projection (next 3 months) ──────────────────────────────
def load_staking_rewards_projection():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    pos_df = None
    for c in [
        os.path.join(script_dir, "enso_positions.csv"),
        os.path.join(script_dir, "..", "Staking", "enso_positions.csv"),
    ]:
        if os.path.exists(c):
            try:
                pos_df = pd.read_csv(c)
                break
            except Exception:
                pass
    if pos_df is None:
        return None

    try:
        fdn = pos_df[pos_df["owner"].str.lower() == FOUNDATION_STAKING_ADDR.lower()]
        fdn_stake_weight = fdn["stake"].sum()
        total_stake_weight = pos_df["stake"].sum()
        if total_stake_weight == 0:
            return None
        fdn_share = fdn_stake_weight / total_stake_weight

        today = datetime.now(timezone.utc).date()
        future = [(d, m) for d, m in REWARDS_SCHEDULE
                   if datetime.strptime(d, "%Y-%m-%d").date() >= today]
        if not future:
            return None

        # Next 3 months only
        next_3 = future[:3]
        rows = []
        for date_str, pool in next_3:
            reward = pool * 0.80 * fdn_share
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            rows.append({
                "label": dt.strftime("%b %Y"),
                "reward": reward,
            })
        return rows
    except Exception:
        return None


# ── Build Telegram message ───────────────────────────────────────────────────
def build_message():
    print("Fetching on-chain holdings...")
    h = load_holdings()

    print("Computing rewards projection...")
    rewards_proj = load_staking_rewards_projection()

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    lines = [
        "<b>ENSO Foundation — Treasury Report</b>",
        today,
        "",
        f"<b>Total Holdings:</b>  {fmt(h['total_holdings'])} ENSO",
        f"<b>Total Sellable:</b>  {fmt(h['total_sellable'])} ENSO",
        "",
        "<b>Holdings Breakdown:</b>",
        f"  Liquid (ETH):        {fmt(h['liquid'])}",
        f"  Liquid (BSC):        {fmt(h['liquid_bsc'])}",
        f"  Vested Unclaimed:    {fmt(h['vested_unclaimed'])}",
        f"  Staked (Expired):    {fmt(h['staked_expired'])}",
        f"  Staked (Locked):     {fmt(h['staked_locked'])}",
        f"  Staking Rewards:     {fmt(h['rewards'])}",
        f"  Locked Unvested:     {fmt(h['locked_vesting'])}",
        f"  Market Makers:       {fmt(h['mm_holdings'])}",
    ]

    if rewards_proj:
        lines.append("")
        lines.append("<b>Expected Staking Rewards (next 3 months):</b>")
        for r in rewards_proj:
            lines.append(f"  {r['label']}:  {fmt(r['reward'])} ENSO")

    return "\n".join(lines)


# ── Send to Telegram ────────────────────────────────────────────────────────
def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
    }
    resp = requests.post(url, json=payload, timeout=15)
    resp.raise_for_status()
    result = resp.json()
    if not result.get("ok"):
        sys.exit(f"Telegram API error: {result}")
    print("Message sent successfully.")


if __name__ == "__main__":
    message = build_message()
    print("---")
    print(message)
    print("---")
    print("Sending to Telegram...")
    send_telegram(message)
