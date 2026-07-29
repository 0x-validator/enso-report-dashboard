"""Read LIVE on-chain claimable staking rewards for Foundation positions."""
import os, sys, time
import pandas as pd, requests
from eth_abi import encode as abi_encode, decode as abi_decode

STAKING_CONTRACT = "0x22Ad2a46d317C5eDF6c01fea16d4399C912E9A01"
MULTICALL3 = "0xcA11bde05977b3631167028862bE2a173976CA11"
ETHERSCAN_URL = "https://api.etherscan.io/v2/api"
RPC_URLS = ["https://ethereum-rpc.publicnode.com", "https://eth.drpc.org", "https://1rpc.io/eth"]
DECIMALS = 18
FOUNDATION_STAKING_ADDR = "0x715b1ddf5d6da6846eadb72d3d6f9d93148d0bb0"
REWARDS_SELECTOR = "61c02efb"  # rewards(uint256 positionId)

TOPIC_POSITION_CREATED = "0x34e49ed13d7eb52832aff120e7482f7b6e7e0328254ca90ee5834a845a87c3b2"
TOPIC_TRANSFER         = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def post(url, body):
    try:
        r = requests.post(url, json=body, timeout=30)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print("  rpc err", url, e)
    return None


def fetch_logs(api_key, topic0):
    out, frm = [], 0
    while True:
        r = requests.get(ETHERSCAN_URL, params={
            "chainid": "1", "module": "logs", "action": "getLogs",
            "address": STAKING_CONTRACT, "topic0": topic0,
            "fromBlock": frm, "toBlock": "latest", "apikey": api_key}, timeout=30)
        d = r.json()
        if d.get("status") != "1" or not d.get("result"):
            break
        logs = d["result"]; out.extend(logs)
        if len(logs) >= 1000:
            frm = int(logs[-1]["blockNumber"], 16) + 1; time.sleep(0.25)
        else:
            break
    return out


def foundation_position_ids(api_key):
    """Determine which position NFTs are currently owned by the Foundation."""
    pc = fetch_logs(api_key, TOPIC_POSITION_CREATED)
    tr = fetch_logs(api_key, TOPIC_TRANSFER)
    pids = {int(l["topics"][1], 16) for l in pc}
    owner = {}
    for l in tr:  # chronological; last transfer wins
        tid = int(l["topics"][3], 16)
        to = "0x" + l["topics"][2][-40:]
        if tid in pids:
            owner[tid] = to.lower()
    return sorted(p for p, o in owner.items() if o == FOUNDATION_STAKING_ADDR.lower())


def read_rewards(position_ids):
    calls = []
    staking = bytes.fromhex(STAKING_CONTRACT[2:])
    for pid in position_ids:
        calls.append((staking, True, bytes.fromhex(REWARDS_SELECTOR) + pid.to_bytes(32, "big")))
    encoded = "0x82ad56cb" + abi_encode(["(address,bool,bytes)[]"], [calls]).hex()
    body = {"jsonrpc": "2.0", "id": 1, "method": "eth_call",
            "params": [{"to": MULTICALL3, "data": encoded}, "latest"]}
    for rpc in RPC_URLS:
        d = post(rpc, body)
        if not d or not d.get("result") or d["result"] == "0x":
            print("  no result from", rpc, d.get("error") if d else "")
            continue
        raw = bytes.fromhex(d["result"][2:])
        decoded = abi_decode(["(bool,bytes)[]"], raw)[0]
        out = {}
        for i, (ok, ret) in enumerate(decoded):
            out[position_ids[i]] = (int.from_bytes(ret[:32], "big") / 10**DECIMALS) if ok and len(ret) >= 32 else None
        print("  source RPC:", rpc)
        return out
    return {}


def main():
    api_key = os.environ.get("ETHERSCAN_API_KEY", "")
    src = "on-chain ownership (Etherscan logs)"
    if api_key:
        ids = foundation_position_ids(api_key)
    else:
        print("No ETHERSCAN_API_KEY -> using position IDs from enso_positions.csv")
        df = pd.read_csv(os.path.join(os.path.dirname(__file__), "enso_positions.csv"))
        df["owner"] = df["owner"].str.lower()
        ids = sorted(df[df["owner"] == FOUNDATION_STAKING_ADDR.lower()]["position_id"].astype(int).tolist())
        src = "CSV snapshot ownership"
    print(f"Foundation positions ({src}): {len(ids)} -> {ids}")
    if not ids:
        return
    rewards = read_rewards(ids)
    total = 0.0; ok = 0
    print("\n  position_id     claimable ENSO")
    for pid in ids:
        v = rewards.get(pid)
        if v is None:
            print(f"  {pid:>10}     <call failed>")
        else:
            print(f"  {pid:>10}   {v:>16,.4f}")
            total += v; ok += 1
    print(f"\n  Positions read OK: {ok}/{len(ids)}")
    print(f"  TOTAL CLAIMABLE (live, latest block): {total:,.4f} ENSO")


if __name__ == "__main__":
    main()
