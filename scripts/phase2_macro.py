#!/usr/bin/env python3
"""
CREX e-Stat Phase 2: マクロ統計取得
GDP・CPI・失業率・賃金・人口等の主要マクロ指標を取得
"""
import os
import json
import csv
import time
import requests
from datetime import datetime

API_KEY = os.environ.get("ESTAT_API_KEY", "")
BASE = "https://api.e-stat.go.jp/rest/3.0/app/json"
OUTPUT_DIR = "data/raw/macro"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def search_tables(keyword, limit=20):
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

def extract_and_save(stat_data, table_id, filename):
    """データを抽出してCSVに保存"""
    class_objs = stat_data.get("GET_STATS_DATA", {}).get("STATISTICAL_DATA", {}).get("CLASS_INF", {}).get("CLASS_OBJ", [])
    if not isinstance(class_objs, list):
        class_objs = [class_objs]
    
    class_map = {}
    for obj in class_objs:
        obj_id = obj.get("@id", "")
        classes = obj.get("CLASS", [])
        if not isinstance(classes, list):
            classes = [classes]
        class_map[obj_id] = {c.get("@code", ""): c.get("@name", "") for c in classes}
    
    values = stat_data.get("GET_STATS_DATA", {}).get("STATISTICAL_DATA", {}).get("DATA_INF", {}).get("VALUE", [])
    if not isinstance(values, list):
        values = [values]
    
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
    
    if rows:
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
    
    return len(rows)

print("=" * 60)
print("CREX e-Stat Phase 2: マクロ統計取得")
print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)

# ── 取得対象のマクロ統計 ──
macro_targets = [
    {
        "name": "消費者物価指数（CPI）",
        "search": "消費者物価指数 全国 年平均",
        "crex_id": "cpi",
        "filter_title": ["年平均", "総合"]
    },
    {
        "name": "労働力調査（完全失業率）",
        "search": "労働力調査 基本集計 全国 年平均",
        "crex_id": "unemployment",
        "filter_title": ["年平均", "完全失業"]
    },
    {
        "name": "毎月勤労統計（賃金）",
        "search": "毎月勤労統計 全国 年平均 現金給与",
        "crex_id": "avg-wage",
        "filter_title": ["現金給与", "年"]
    },
    {
        "name": "人口推計",
        "search": "人口推計 各年10月1日 総人口",
        "crex_id": "population",
        "filter_title": ["総人口", "年齢"]
    },
    {
        "name": "家計調査（消費支出）",
        "search": "家計調査 二人以上 年 消費支出",
        "crex_id": "consumer-spending",
        "filter_title": ["消費支出", "年"]
    },
    {
        "name": "住宅着工統計",
        "search": "住宅着工統計 新設住宅 利用関係別",
        "crex_id": "housing-starts",
        "filter_title": ["新設住宅"]
    },
    {
        "name": "鉱工業指数（生産指数）",
        "search": "鉱工業指数 生産指数 年",
        "crex_id": "iip",
        "filter_title": ["生産指数"]
    },
    {
        "name": "商業動態統計（小売販売額）",
        "search": "商業動態統計 小売業 販売額 年",
        "crex_id": "retail-sales",
        "filter_title": ["小売", "販売額"]
    },
]

summary = []
for target in macro_targets:
    print(f"\n■ {target['name']}")
    
    try:
        tables = search_tables(target["search"])
        
        # タイトルでフィルター
        matched = tables
        for kw in target.get("filter_title", []):
            matched = [t for t in matched if kw in get_title(t)]
        
        if not matched:
            matched = tables[:1]  # フィルターで0件の場合は先頭1件
        
        if not matched:
            print(f"  ⚠️ テーブルが見つかりません")
            summary.append({"name": target["name"], "status": "NOT_FOUND", "rows": 0})
            continue
        
        table = matched[0]
        table_id = table.get("@id", "")
        title = get_title(table)
        print(f"  対象: {table_id} - {title[:60]}")
        
        data = get_stats_data(table_id)
        filename = f"{OUTPUT_DIR}/{target['crex_id']}.csv"
        rows = extract_and_save(data, table_id, filename)
        
        print(f"  ✅ {rows}行 → {filename}")
        summary.append({
            "name": target["name"],
            "crex_id": target["crex_id"],
            "table_id": table_id,
            "title": title,
            "rows": rows,
            "status": "OK"
        })
        
        time.sleep(1)
        
    except Exception as e:
        print(f"  ❌ {e}")
        summary.append({"name": target["name"], "status": str(e), "rows": 0})

# サマリー保存
with open(f"{OUTPUT_DIR}/_summary.json", "w", encoding="utf-8") as f:
    json.dump({
        "timestamp": datetime.now().isoformat(),
        "targets": len(macro_targets),
        "success": sum(1 for s in summary if s.get("status") == "OK"),
        "total_rows": sum(s.get("rows", 0) for s in summary),
        "results": summary
    }, f, ensure_ascii=False, indent=2)

ok = sum(1 for s in summary if s.get("status") == "OK")
total_rows = sum(s.get("rows", 0) for s in summary)
print(f"\n{'=' * 60}")
print(f"Phase 2 完了: {ok}/{len(macro_targets)}指標成功, {total_rows}行取得")
print(f"{'=' * 60}")
