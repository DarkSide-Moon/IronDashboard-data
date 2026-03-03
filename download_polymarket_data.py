# -*- coding: utf-8 -*-
"""
Polymarket 数据下载
- 每个事件保存为一个 CSV（以 polymarket 的 slug 命名），宽格式：第一列 datetime，后续列为各结果
- 首次运行：全量下载历史数据（interval=max，约每10分钟一个点）
- 再次运行：增量下载最近24小时（interval=1d，约每1分钟一个点）并合并

datetime 列均为北京时间（UTC+8），格式：YYYY-MM-DD HH:MM:SS

注意：两次增量运行的间隔不能超过 24 小时，否则会出现数据缺口。
      若间隔已超过 24 小时，请先执行全量更新，再恢复增量运行。

运行命令：
  全量更新（首次或间隔超过24h时使用，覆盖全部历史数据，约每10分钟一个点）：
      python download_polymarket_data.py --refresh

  增量更新（日常使用，下载最近24小时新数据并合并，约每1分钟一个点）：
      python download_polymarket_data.py
"""

import sys
import os
import json
import time
import argparse
import requests
import pandas as pd

sys.stdout.reconfigure(encoding='utf-8')

# ─── 配置 ────────────────────────────────────────────────────────────────────

GAMMA_API = "https://gamma-api.polymarket.com"  # Polymarket 的事件元数据接口，查询某个事件下有哪些结果，每个结果对应哪个 token
CLOB_API  = "https://clob.polymarket.com"  # Polymarket 的订单簿接口，查询某个 token 的历史价格（概率）
DATA_DIR  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "polymarket_data")

EVENTS = [
    {"slug": "fed-decision-in-march-885",                        "title": "Fed Decision (March 2026)"},
    {"slug": "will-the-iranian-regime-fall-by-june-30",          "title": "Iranian Regime Fall by Jun 30"},
    {"slug": "will-iran-close-the-strait-of-hormuz-by-2027",     "title": "Iran Close Strait of Hormuz"},
    {"slug": "us-x-iran-ceasefire-by",                           "title": "US x Iran Ceasefire"},
    {"slug": "us-iran-nuclear-deal-before-2027",                  "title": "US-Iran Nuclear Deal before 2027"},
    {"slug": "who-will-be-next-supreme-leader-of-iran-515",      "title": "Next Supreme Leader of Iran"},
    {"slug": "what-will-the-usisrael-target-in-iran-by-march-31","title": "US/Israel Target in Iran by Mar 31"},
    {"slug": "usisrael-strikes-iran-on",                         "title": "US/Israel Strikes Iran On"},
    {"slug": "will-crude-oil-cl-hit-by-end-of-march",            "title": "Crude Oil Hit Price by Mar 31"},
    {"slug": "us-seizes-an-iran-linked-oil-tanker-by-march-7",   "title": "US Seizes Iran Oil Tanker by Mar 7"},
    {"slug": "march-inflation-us-annual",                         "title": "March Inflation US - Annual"},
]

MARKETS_CSV = os.path.join(DATA_DIR, "markets.csv")

# ─── API ──────────────────────────────────────────────────────────────────────

def fetch_event_markets(slug):
    """从 Gamma API 获取事件下所有活跃市场的 token_id 和列名。"""
    resp = requests.get(f"{GAMMA_API}/events", params={"slug": slug}, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if not data:
        return []

    event = data[0]
    all_markets = event.get("markets", [])
    active = [m for m in all_markets if m.get("active") and not m.get("closed")]
    if not active:
        active = all_markets

    is_binary = len(active) == 1
    results = []
    for market in active:
        token_ids_raw = market.get("clobTokenIds", "[]")
        token_ids = json.loads(token_ids_raw) if isinstance(token_ids_raw, str) else token_ids_raw
        if not token_ids:
            continue
        # 二元事件（仅Yes/No）用 "Yes" 作为列名；多结果事件用完整问题文本
        if is_binary:
            col_name = "Yes"
        else:
            col_name = market.get("question", "").strip().rstrip("?")
        results.append({
            "event_slug":  slug,
            "event_title": event.get("title", slug),
            "column_name": col_name,
            "token_id":    token_ids[0],   # Yes token
        })
    return results


def fetch_price_history(token_id, interval="max", fidelity=1, retries=3, retry_delay=5):
    """
    从 CLOB API 获取价格历史。
    interval=max, fidelity=1  →  全量历史，约每10分钟一个点
    interval=1d,  fidelity=1  →  最近24小时，约每1分钟一个点
    网络错误时自动重试 retries 次，每次间隔 retry_delay 秒。
    """
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(
                f"{CLOB_API}/prices-history",
                params={"market": token_id, "interval": interval, "fidelity": fidelity},
                timeout=20
            )
            resp.raise_for_status()
            return resp.json().get("history", [])
        except requests.exceptions.RequestException as e:
            if attempt < retries:
                print(f" [retry {attempt}/{retries}] {e}")
                time.sleep(retry_delay)
            else:
                print(f" [failed] {e}")
                return []

# ─── 宽格式 CSV 工具 ──────────────────────────────────────────────────────────

def _event_csv_path(slug):
    return os.path.join(DATA_DIR, f"{slug}.csv")


def _histories_to_wide(market_list, histories):
    """
    将多个 token 的历史数据合并为一个宽格式 DataFrame。
    datetime 取整到分钟后作为对齐键，外连接合并，缺失值向前填充。
    """
    dfs = []
    for market, history in zip(market_list, histories):
        if not history:
            continue
        col = market["column_name"]
        df = pd.DataFrame(history)
        df["datetime"] = (pd.to_datetime(df["t"], unit="s", utc=True)
                          .dt.tz_convert("Asia/Shanghai")
                          .dt.round("min"))
        df = df[["datetime", "p"]].rename(columns={"p": col})
        df = df.drop_duplicates(subset="datetime")
        dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    combined = dfs[0]
    for df in dfs[1:]:
        combined = combined.merge(df, on="datetime", how="outer")

    combined = combined.sort_values("datetime").reset_index(drop=True)
    combined = combined.ffill()
    return combined


def get_event_latest_ts(slug):
    """返回事件 CSV 中最新一行的 Unix 时间戳，若文件不存在则返回 0。"""
    path = _event_csv_path(slug)
    if not os.path.exists(path):
        return 0
    df = pd.read_csv(path, encoding="utf-8-sig", usecols=["datetime"])
    if df.empty:
        return 0
    latest = pd.to_datetime(df["datetime"]).dt.tz_localize("Asia/Shanghai").max()
    return int(latest.timestamp())


def save_event_data(slug, market_list, histories):
    """将新数据与已有事件 CSV 合并去重后保存，返回保存后的总行数。"""
    new_wide = _histories_to_wide(market_list, histories)
    if new_wide.empty:
        return 0

    path = _event_csv_path(slug)
    if os.path.exists(path):
        existing = pd.read_csv(path, encoding="utf-8-sig")
        existing["datetime"] = pd.to_datetime(existing["datetime"]).dt.tz_localize("Asia/Shanghai")
        combined = pd.concat([existing, new_wide], ignore_index=True)
    else:
        combined = new_wide

    combined = combined.drop_duplicates(subset=["datetime"])
    combined = combined.sort_values("datetime").reset_index(drop=True)
    combined = combined.ffill()

    # 保存时去掉时区标识，统一存北京时间字符串
    combined["datetime"] = combined["datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
    combined.to_csv(path, index=False, encoding="utf-8-sig")
    return len(combined)


def load_event_data(slug):
    """读取事件宽格式 CSV，返回 DataFrame（datetime 列为北京时间）。"""
    path = _event_csv_path(slug)
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path, encoding="utf-8-sig")
    df["datetime"] = pd.to_datetime(df["datetime"]).dt.tz_localize("Asia/Shanghai")
    return df

# ─── 主流程 ───────────────────────────────────────────────────────────────────

def load_or_fetch_markets():
    """读取已有 markets.csv，或重新查询 Gamma API。"""
    if os.path.exists(MARKETS_CSV):
        df = pd.read_csv(MARKETS_CSV, encoding="utf-8-sig")
        return df.to_dict("records")

    print("[markets] querying Gamma API ...")
    all_markets = []
    for event in EVENTS:
        markets = fetch_event_markets(event["slug"])
        all_markets.extend(markets)
        print(f"  {event['slug'][:45]} -> {len(markets)} outcomes")

    df = pd.DataFrame(all_markets)
    df.to_csv(MARKETS_CSV, index=False, encoding="utf-8-sig")
    print(f"[markets] saved {len(all_markets)} tokens to markets.csv\n")
    return all_markets


def download_all(market_list, refresh=False):
    """
    按事件分组，对每个事件：
    - 无本地 CSV 或 refresh=True：全量下载（interval=max, fidelity=1）
    - 有本地 CSV：增量下载最近24小时（interval=1d, fidelity=1）
    """
    # 按 event_slug 分组
    events_map = {}
    for m in market_list:
        slug = m["event_slug"]
        events_map.setdefault(slug, []).append(m)

    total = len(events_map)
    stats = {"new": 0, "incremental": 0, "fetched_pts": 0}

    for idx, (slug, event_markets) in enumerate(events_map.items(), 1):
        event_title = event_markets[0]["event_title"]
        latest_ts   = get_event_latest_ts(slug)
        force_full  = refresh or (latest_ts == 0)
        mode        = "FULL" if force_full else "INCR"

        print(f"\n[{idx}/{total}] [{mode}] {event_title}")

        if force_full and refresh:
            path = _event_csv_path(slug)
            if os.path.exists(path):
                os.remove(path)

        histories = []
        for i, market in enumerate(event_markets, 1):
            col = market["column_name"]
            interval = "max" if force_full else "1d"
            print(f"  [{i}/{len(event_markets)}] {col[:50]} ...", end=" ", flush=True)
            history = fetch_price_history(market["token_id"], interval=interval, fidelity=1)
            histories.append(history)
            print(f"{len(history)} pts")
            stats["fetched_pts"] += len(history)

        total_rows = save_event_data(slug, event_markets, histories)
        print(f"  -> {slug}.csv  ({total_rows} rows)")

        if force_full:
            stats["new"] += 1
        else:
            stats["incremental"] += 1

    return stats


def print_summary(market_list):
    """打印各事件 CSV 的摘要信息。"""
    # 按 slug 分组取事件标题
    title_map = {m["event_slug"]: m["event_title"] for m in market_list}

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    for event in EVENTS:
        slug  = event["slug"]
        title = title_map.get(slug, event["title"])
        df    = load_event_data(slug)
        if df.empty:
            print(f"\n{title}  -- no data")
            continue

        n_rows   = len(df)
        dt_min   = str(df["datetime"].min())[:16]
        dt_max   = str(df["datetime"].max())[:16]
        print(f"\n{title}  ({n_rows} rows, {dt_min} ~ {dt_max})")

        for col in df.columns[1:]:   # 跳过 datetime
            series = df[col].dropna()
            if series.empty:
                print(f"  {col[:55]}  no data")
            else:
                print(f"  {col[:55]}  latest={series.iloc[-1]*100:.1f}%")


def main():
    parser = argparse.ArgumentParser(description="Polymarket Data Downloader")
    parser.add_argument(
        "--refresh", action="store_true",
        help="强制重新全量下载（覆盖旧历史数据）"
    )
    args = parser.parse_args()

    os.makedirs(DATA_DIR, exist_ok=True)
    print("=" * 60)
    print("Polymarket Data Downloader")
    if args.refresh:
        print("  全量更新")
    print("=" * 60 + "\n")

    if args.refresh and os.path.exists(MARKETS_CSV):
        os.remove(MARKETS_CSV)

    market_list = load_or_fetch_markets()
    stats = download_all(market_list, refresh=args.refresh)

    print(f"\n[done] full={stats['new']}  incremental={stats['incremental']}  "
          f"fetched={stats['fetched_pts']} pts")

    print_summary(market_list)


if __name__ == "__main__":
    main()
