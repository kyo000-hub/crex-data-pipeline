#!/usr/bin/env python3
"""
CREX e-Stat Phase 0: API疎通テスト
半導体・GDP・CPI・労働力調査の統計表を検索し、接続を確認する
"""
import os
import json
import requests
from datetime import datetime

API_KEY = os.environ.get("ESTAT_API_KEY", "")
BASE = "https://api.e-stat.go.jp/rest/3.0/app/json"

def search(keyword, limit=5):
    r = requests.get(f"{BASE}/getStatsList", params={
        "appId": API_KEY, "searchWord": keyword, "limit": limit
    }, timeout=30)
    r.raise_for_status()
    tables = r.json().get("GET_STATS_LIST", {}).get("DATALIST_INF", {}).get("TABLE_INF", [])
    return tables if isinstance(tables, list) else [tables]

def get_title(t):
    title = t.get("TITLE", "")
    return title.get("$", "") if isinstance(title, dict) else title

print("=" * 60)
print("CREX e-Stat API 疎通テスト (Phase 0)")
print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)

tests = [
    ("生産動態統計 半導体", "半導体の出荷額・生産量"),
    ("国民経済計算 国内総生産", "GDP統計"),
    ("消費者物価指数 全国", "CPI"),
    ("労働力調査 完全失業率", "失業率"),
    ("経済センサス 製造業", "経済センサス"),
]

results = []
for keyword, label in tests:
    try:
        tables = search(keyword)
        print(f"\n✅ {label}: {len(tables)}件")
        for t in tables[:3]:
            print(f"   {t.get('@id','')}: {get_title(t)[:60]}")
        results.append({"label": label, "keyword": keyword, "count": len(tables), "status": "OK"})
    except Exception as e:
        print(f"\n❌ {label}: {e}")
        results.append({"label": label, "keyword": keyword, "count": 0, "status": str(e)})

# 結果をJSONで保存
os.makedirs("data/raw", exist_ok=True)
with open("data/raw/phase0_result.json", "w", encoding="utf-8") as f:
    json.dump({
        "timestamp": datetime.now().isoformat(),
        "api_key_prefix": API_KEY[:8] + "...",
        "results": results
    }, f, ensure_ascii=False, indent=2)

ok = sum(1 for r in results if r["status"] == "OK")
print(f"\n{'=' * 60}")
print(f"結果: {ok}/{len(tests)} テスト成功")
print(f"{'=' * 60}")

if ok < len(tests):
    exit(1)
