#!/usr/bin/env python3
"""
CREX e-Stat Phase 1: 生産動態統計（製造業品目別）
品目別の出荷額・生産量・生産額を年次で取得し、CSVに保存する
"""
import os
import json
import csv
import time
import requests
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
    }, timeout=60)
    r.raise_for_status()
    return r.json()

def get_title(t):
    title = t.get("TITLE", "")
    return title.get("$", "") if isinstance(title, dict) else title

def extract_class_info(stat_data):
    """メタ情報（分類軸）を抽出"""
    class_objs = stat_data.get("STATISTICAL_DATA", {}).get("CLASS_INF", {}).get("CLASS_OBJ", [])
    if not isinstance(class_objs, list):
        class_objs = [class_objs]
    
    class_map = {}
    for obj in class_objs:
        obj_id = obj.get("@id", "")
        classes = obj.get("CLASS", [])
        if not isinstance(classes, list):
            classes = [classes]
        class_map[obj_id] = {
            "name": obj.get("@name", ""),
            "items": {c.get("@code", ""): c.get("@name", "") for c in classes}
        }
    return class_map

def extract_values(stat_data, class_map):
    """データ値を抽出してリスト化"""
    values = stat_data.get("STATISTICAL_DATA", {}).get("DATA_INF", {}).get("VALUE", [])
    if not isinstance(values, list):
        values = [values]
    
    rows = []
    for v in values:
        row = {"value": v.get("$", ""), "unit": v.get("@unit", "")}
        # 各分類軸のコードを名前に変換
        for key, val in v.items():
            if key.startswith("@") and key not in ("@unit",):
                clean_key = key.lstrip("@")
                row[f"{clean_key}_code"] = val
                if clean_key in class_map:
                    row[f"{clean_key}_name"] = class_map[clean_key]["items"].get(val, val)
        rows.append(row)
    return rows

print("=" * 60)
print("CREX e-Stat Phase 1: 生産動態統計")
print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)

# ── Step 1: 生産動態統計の年報テーブルを検索 ──
search_queries = [
    "生産動態統計 年報 化学",
    "生産動態統計 年報 鉄鋼",
    "生産動態統計 年報 機械",
    "生産動態統計 年報 電気",
    "生産動態統計 年報 繊維",
    "生産動態統計 年報 紙",
    "生産動態統計 年報 窯業",
    "生産動態統計 年報 非鉄金属",
    "生産動態統計 年報 ゴム",
    "生産動態統計 年報 プラスチック",
]

all_tables = []
for query in search_queries:
    try:
        tables = search_tables(query, limit=50)
        # 年報の時系列表のみフィルター
        filtered = [t for t in tables if "時系列" in get_title(t) or "年報" in get_title(t)]
        all_tables.extend(filtered)
        print(f"  {query}: {len(filtered)}件（検索{len(tables)}件中）")
        time.sleep(0.5)  # API負荷軽減
    except Exception as e:
        print(f"  ❌ {query}: {e}")

print(f"\n対象テーブル合計: {len(all_tables)}件")

# ── Step 2: 各テーブルからデータを取得 ──
summary = []
error_count = 0
MAX_TABLES = 20  # 初回は20テーブルまで

for i, table in enumerate(all_tables[:MAX_TABLES]):
    table_id = table.get("@id", "")
    title = get_title(table)
    
    print(f"\n[{i+1}/{min(len(all_tables), MAX_TABLES)}] {table_id}: {title[:50]}")
    
    try:
        data = get_stats_data(table_id)
        class_map = extract_class_info(data)
        rows = extract_values(data, class_map)
        
        if not rows:
            print(f"  ⚠️ データなし")
            continue
        
        # CSVに保存
        filename = f"{OUTPUT_DIR}/{table_id}.csv"
        fieldnames = list(rows[0].keys())
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        
        print(f"  ✅ {len(rows)}行 → {filename}")
        summary.append({
            "table_id": table_id,
            "title": title,
            "rows": len(rows),
            "columns": len(fieldnames),
            "status": "OK"
        })
        
        time.sleep(1)  # API負荷軽減
        
    except Exception as e:
        print(f"  ❌ エラー: {e}")
        error_count += 1
        summary.append({
            "table_id": table_id,
            "title": title,
            "rows": 0,
            "columns": 0,
            "status": str(e)
        })

# ── Step 3: サマリー保存 ──
with open(f"{OUTPUT_DIR}/_summary.json", "w", encoding="utf-8") as f:
    json.dump({
        "timestamp": datetime.now().isoformat(),
        "total_tables": len(all_tables),
        "processed": len(summary),
        "success": sum(1 for s in summary if s["status"] == "OK"),
        "errors": error_count,
        "total_rows": sum(s["rows"] for s in summary),
        "tables": summary
    }, f, ensure_ascii=False, indent=2)

ok = sum(1 for s in summary if s["status"] == "OK")
total_rows = sum(s["rows"] for s in summary)
print(f"\n{'=' * 60}")
print(f"Phase 1 完了: {ok}/{len(summary)}テーブル成功, {total_rows}行取得")
print(f"{'=' * 60}")
