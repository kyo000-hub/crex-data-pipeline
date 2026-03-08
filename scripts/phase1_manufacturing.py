#!/usr/bin/env python3
"""
CREX e-Stat Phase 1: 生産動態統計（製造業品目別）
"""
import os, json, csv, time, requests
from datetime import datetime

API_KEY = os.environ.get("ESTAT_API_KEY", "")
BASE = "https://api.e-stat.go.jp/rest/3.0/app/json"
OUTPUT_DIR = "data/raw/manufacturing"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def search_tables(keyword, limit=100):
    r = requests.get(f"{BASE}/getStatsList", params={
        "appId": API_KEY, "searchWord": keyword, "limit": limit
    }, timeout=30)
    r.raise_for_status()
    tables = r.json().get("GET_STATS_LIST", {}).get("DATALIST_INF", {}).get("TABLE_INF", [])
    return tables if isinstance(tables, list) else [tables]

def get_stats_data(stats_id, limit=100000):
    r = requests.get(f"{BASE}/getStatsData", params={
        "appId": API_KEY, "statsDataId": stats_id, "limit": limit
    }, timeout=120)
    r.raise_for_status()
    return r.json()

def get_title(t):
    title = t.get("TITLE", "")
    return title.get("$", "") if isinstance(title, dict) else title

print("=" * 60)
print("CREX e-Stat Phase 1: 生産動態統計")
print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)

search_queries = [
    "生産動態統計 年報 化学工業",
    "生産動態統計 年報 鉄鋼",
    "生産動態統計 年報 機械",
    "生産動態統計 年報 電子部品",
    "生産動態統計 年報 繊維工業",
    "生産動態統計 年報 紙パルプ",
    "生産動態統計 年報 窯業",
    "生産動態統計 年報 非鉄金属",
    "生産動態統計 年報 ゴム製品",
    "生産動態統計 年報 プラスチック",
]

all_tables = []
for query in search_queries:
    try:
        tables = search_tables(query, limit=30)
        all_tables.extend(tables)
        print(f"  {query}: {len(tables)}件")
        time.sleep(0.5)
    except Exception as e:
        print(f"  ❌ {query}: {e}")

print(f"\n対象テーブル合計: {len(all_tables)}件")

summary = []
MAX_TABLES = 15

for i, table in enumerate(all_tables[:MAX_TABLES]):
    table_id = table.get("@id", "")
    title = get_title(table)
    print(f"\n[{i+1}/{min(len(all_tables), MAX_TABLES)}] {table_id}: {title[:50]}")

    try:
        data = get_stats_data(table_id)
        stat = data.get("GET_STATS_DATA", {}).get("STATISTICAL_DATA", {})

        class_objs = stat.get("CLASS_INF", {}).get("CLASS_OBJ", [])
        if not isinstance(class_objs, list):
            class_objs = [class_objs]
        class_map = {}
        for obj in class_objs:
            oid = obj.get("@id", "")
            classes = obj.get("CLASS", [])
            if not isinstance(classes, list):
                classes = [classes]
            class_map[oid] = {c.get("@code", ""): c.get("@name", "") for c in classes}

        values = stat.get("DATA_INF", {}).get("VALUE", [])
        if not isinstance(values, list):
            values = [values]

        if not values:
            print(f"  ⚠️ データなし")
            continue

        rows = []
        for v in values:
            row = {"value": v.get("$", "")}
            for key, val in v.items():
                if key.startswith("@"):
                    clean = key.lstrip("@")
                    row[clean] = val
                    if clean in class_map and val in class_map[clean]:
                        row[f"{clean}_label"] = class_map[clean][val]
            rows.append(row)

        filename = f"{OUTPUT_DIR}/{table_id}.csv"
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

        print(f"  ✅ {len(rows)}行 → {filename}")
        summary.append({"table_id": table_id, "title": title, "rows": len(rows), "status": "OK"})
        time.sleep(1)

    except Exception as e:
        print(f"  ❌ エラー: {e}")
        summary.append({"table_id": table_id, "title": title, "rows": 0, "status": str(e)})

with open(f"{OUTPUT_DIR}/_summary.json", "w", encoding="utf-8") as f:
    json.dump({
        "timestamp": datetime.now().isoformat(),
        "total_tables_found": len(all_tables),
        "processed": len(summary),
        "success": sum(1 for s in summary if s["status"] == "OK"),
        "total_rows": sum(s["rows"] for s in summary),
        "tables": summary
    }, f, ensure_ascii=False, indent=2)

ok = sum(1 for s in summary if s["status"] == "OK")
total_rows = sum(s["rows"] for s in summary)
print(f"\n{'=' * 60}")
print(f"Phase 1 完了: {ok}/{len(summary)}テーブル成功, {total_rows}行取得")
print(f"{'=' * 60}")
