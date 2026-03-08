#!/usr/bin/env python3
"""
CREX e-Stat Phase 3: データ加工
raw CSVをCREXのI3ページ用JSONに変換する
出力: data/processed/ 以下に指標別JSONファイル
"""
import os, json, csv, re
from datetime import datetime

RAW_DIR = "data/raw"
OUT_DIR = "data/processed"
os.makedirs(OUT_DIR, exist_ok=True)

def parse_time_to_year(time_str, time_label=""):
    """e-Statのtime値を西暦年に変換"""
    # "1991000000" → "1991", "2023100000" → "2023"
    if time_str and len(time_str) >= 4:
        y = time_str[:4]
        if y.isdigit() and 1900 <= int(y) <= 2100:
            return int(y)
    # time_labelから抽出: "1991年", "平成25年" etc
    m = re.search(r'(\d{4})年', str(time_label))
    if m:
        return int(m.group(1))
    # 平成→西暦変換
    m = re.search(r'平成(\d+)年', str(time_label))
    if m:
        return 1988 + int(m.group(1))
    m = re.search(r'令和(\d+)年', str(time_label))
    if m:
        return 2018 + int(m.group(1))
    return None

def safe_float(v):
    """安全にfloatに変換"""
    if v is None or v == "" or v == "-" or v == "…" or v == "x" or v == "***":
        return None
    try:
        return float(str(v).replace(",", ""))
    except:
        return None

# ═══════════════════════════════════════
# 1. 人口推計 → population.json
# ═══════════════════════════════════════
print("■ 人口推計")
try:
    with open(f"{RAW_DIR}/macro/population.csv", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    # 総人口（cat01=000, cat02=000(総数), cat03=001(1975年)〜）全国のみ
    total_pop = {}
    for r in rows:
        cat01_label = r.get("cat01_label", "")
        cat02_label = r.get("cat02_label", "")
        area_label = r.get("area_label", "")
        
        if "人口・総数" in cat01_label and "総数" in cat02_label and area_label == "全国":
            year = parse_time_to_year(r.get("time", ""), r.get("time_label", ""))
            val = safe_float(r.get("value"))
            if year and val:
                total_pop[year] = val  # 千人単位
    
    pop_series = [{"year": y, "value": v, "unit": "千人"} for y, v in sorted(total_pop.items())]
    
    output = {
        "indicator_id": "population",
        "indicator_name": "総人口",
        "unit": "千人",
        "source": "e-Stat 人口推計",
        "updated": datetime.now().strftime("%Y-%m-%d"),
        "data": pop_series
    }
    with open(f"{OUT_DIR}/population.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"  ✅ {len(pop_series)}年分 → population.json")
except Exception as e:
    print(f"  ❌ {e}")

# ═══════════════════════════════════════
# 2. 住宅着工統計 → housing-starts.json
# ═══════════════════════════════════════
print("■ 住宅着工統計")
try:
    with open(f"{RAW_DIR}/macro/housing-starts.csv", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    # 全国合計のみ抽出
    national = {}
    for r in rows:
        area_label = r.get("area_label", "")
        if area_label == "全国":
            year = parse_time_to_year(r.get("time", ""), r.get("time_label", ""))
            val = safe_float(r.get("value"))
            if year and val:
                if year not in national:
                    national[year] = 0
                national[year] += val
    
    hs_series = [{"year": y, "value": v, "unit": "戸"} for y, v in sorted(national.items())]
    
    output = {
        "indicator_id": "housing-starts",
        "indicator_name": "新設住宅着工戸数",
        "unit": "戸",
        "source": "e-Stat 住宅着工統計",
        "updated": datetime.now().strftime("%Y-%m-%d"),
        "data": hs_series
    }
    with open(f"{OUT_DIR}/housing-starts.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"  ✅ {len(hs_series)}年分 → housing-starts.json")
except Exception as e:
    print(f"  ❌ {e}")

# ═══════════════════════════════════════
# 3. 小売販売額 → retail-sales.json
# ═══════════════════════════════════════
print("■ 小売販売額")
try:
    with open(f"{RAW_DIR}/macro/retail-sales.csv", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    # 全国の年次データのみ（time_labelに「年」を含み「月」を含まない）
    national = {}
    for r in rows:
        time_label = r.get("time_label", "")
        # 年次データのみ（四半期・月次除外）
        if "月" in time_label or "～" in time_label or "〜" in time_label:
            continue
        year = parse_time_to_year(r.get("time", ""), time_label)
        val = safe_float(r.get("value"))
        area_label = r.get("area_label", "")
        if year and val and year not in national:
            national[year] = val
    
    rs_series = [{"year": y, "value": v, "unit": "10億円"} for y, v in sorted(national.items())]
    
    output = {
        "indicator_id": "retail-sales",
        "indicator_name": "小売業販売額",
        "unit": "10億円",
        "source": "e-Stat 商業動態統計",
        "updated": datetime.now().strftime("%Y-%m-%d"),
        "data": rs_series
    }
    with open(f"{OUT_DIR}/retail-sales.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"  ✅ {len(rs_series)}年分 → retail-sales.json")
except Exception as e:
    print(f"  ❌ {e}")

# ═══════════════════════════════════════
# 4. 平均賃金 → avg-wage.json
# ═══════════════════════════════════════
print("■ 平均賃金")
try:
    with open(f"{RAW_DIR}/macro/avg-wage.csv", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    # 全国・5人以上・TL(産業計)のみ
    wages = {}
    for r in rows:
        area_label = r.get("area_label", "")
        cat03_label = r.get("cat03_label", "")
        if area_label == "全国" and "5人以上" in cat03_label:
            year = parse_time_to_year(r.get("time", ""), r.get("time_label", ""))
            val = safe_float(r.get("value"))
            if year and val and year not in wages:
                wages[year] = val
    
    wage_series = [{"year": y, "value": v, "unit": "指数"} for y, v in sorted(wages.items())]
    
    output = {
        "indicator_id": "avg-wage",
        "indicator_name": "賃金指数（現金給与総額）",
        "unit": "指数（平成17年=100）",
        "source": "e-Stat 毎月勤労統計",
        "updated": datetime.now().strftime("%Y-%m-%d"),
        "data": wage_series
    }
    with open(f"{OUT_DIR}/avg-wage.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"  ✅ {len(wage_series)}年分 → avg-wage.json")
except Exception as e:
    print(f"  ❌ {e}")

# ═══════════════════════════════════════
# 5. 製造業品目別データ → manufacturing.json
# ═══════════════════════════════════════
print("■ 製造業品目別データ")
try:
    mfg_dir = f"{RAW_DIR}/manufacturing"
    all_items = []
    
    for filename in os.listdir(mfg_dir):
        if not filename.endswith(".csv"):
            continue
        
        filepath = os.path.join(mfg_dir, filename)
        table_id = filename.replace(".csv", "")
        
        with open(filepath, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        if not rows:
            continue
        
        # 品目別に集計
        items = {}
        for r in rows:
            cat01_label = r.get("cat01_label", "")  # 生産/受入/出荷等
            cat02_label = r.get("cat02_label", "")  # 品目名
            val = safe_float(r.get("value"))
            
            if cat02_label and val is not None:
                key = cat02_label
                if key not in items:
                    items[key] = {"name": key, "category": cat01_label, "value": val, "table_id": table_id}
                # 最初の値を保持
        
        for item in items.values():
            all_items.append(item)
    
    output = {
        "indicator_id": "manufacturing-products",
        "indicator_name": "製造業品目別データ",
        "source": "e-Stat 生産動態統計",
        "updated": datetime.now().strftime("%Y-%m-%d"),
        "total_items": len(all_items),
        "items": all_items[:500]  # 上限500品目
    }
    with open(f"{OUT_DIR}/manufacturing.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"  ✅ {len(all_items)}品目 → manufacturing.json")
except Exception as e:
    print(f"  ❌ {e}")

# ═══════════════════════════════════════
# 6. 全指標のインデックス → index.json
# ═══════════════════════════════════════
print("■ インデックス生成")
try:
    index = {
        "updated": datetime.now().strftime("%Y-%m-%d"),
        "indicators": []
    }
    
    for filename in os.listdir(OUT_DIR):
        if filename == "index.json" or not filename.endswith(".json"):
            continue
        with open(os.path.join(OUT_DIR, filename), encoding="utf-8") as f:
            data = json.load(f)
        
        entry = {
            "id": data.get("indicator_id", ""),
            "name": data.get("indicator_name", ""),
            "unit": data.get("unit", ""),
            "source": data.get("source", ""),
            "file": filename,
        }
        if "data" in data:
            entry["years"] = len(data["data"])
            if data["data"]:
                entry["latest_year"] = data["data"][-1].get("year")
                entry["latest_value"] = data["data"][-1].get("value")
        elif "total_items" in data:
            entry["total_items"] = data["total_items"]
        
        index["indicators"].append(entry)
    
    with open(f"{OUT_DIR}/index.json", "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)
    print(f"  ✅ {len(index['indicators'])}指標 → index.json")
except Exception as e:
    print(f"  ❌ {e}")

print(f"\n{'=' * 60}")
print(f"Phase 3 完了: data/processed/ にJSONファイルを出力")
print(f"{'=' * 60}")
