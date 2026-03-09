#!/usr/bin/env python3
"""
CREX データ集約スクリプト v2
processed JSON + raw CSV → Googleスプレッドシート用CSV
修正: processed JSONを活用してマクロデータの精度向上
"""
import os, csv, re, json, glob
from datetime import datetime
from collections import defaultdict

RAW = "data/raw"
PROCESSED = "data/processed"
OUT = "data/gsheet"
os.makedirs(OUT, exist_ok=True)

def parse_year(time_str, time_label=""):
    if time_str and len(str(time_str)) >= 4:
        y = str(time_str)[:4]
        if y.isdigit() and 1900 <= int(y) <= 2100:
            return int(y)
    for pattern, offset in [(r'(\d{4})年', 0), (r'令和(\d+)年', 2018), (r'平成(\d+)年', 1988), (r'昭和(\d+)年', 1925)]:
        m = re.search(pattern, str(time_label))
        if m: return int(m.group(1)) + offset
    return None

def safe_float(v):
    if v is None or str(v).strip() in ("", "-", "…", "x", "***", "X", "－"):
        return None
    try: return float(str(v).replace(",", ""))
    except: return None

def read_csv(path):
    try:
        with open(path, encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except: return []

def label(row, key):
    return row.get(f"{key}_label", row.get(key, ""))

def write_csv(filepath, headers, rows):
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow({h: r.get(h, "") for h in headers})
    print(f"  ✅ {os.path.basename(filepath)}: {len(rows)}行")

def read_json(path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except: return None

print("=" * 70)
print("CREX データ集約 v2")
print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)

# ════════════════════════════════════════════════════════════
# B. マクロ時系列 — processed JSONから読み込み（精度高い）
# ════════════════════════════════════════════════════════════
print("\n■ B. マクロ時系列（processed JSONから）")
macro_rows = []
macro_headers = ["year", "indicator_id", "indicator_name", "value", "unit", "source_id", "notes"]

# processed JSONから読み込み
json_mappings = [
    ("population.json", "population", "総人口", "千人", "estat-population"),
    ("housing-starts.json", "housing-starts", "住宅着工戸数", "戸", "estat-housing"),
    ("retail-sales.json", "retail-sales", "小売販売額", "10億円", "estat-retail"),
    ("avg-wage.json", "avg-wage", "賃金指数", "指数", "estat-wage"),
]

for json_file, ind_id, ind_name, unit, src_id in json_mappings:
    data = read_json(os.path.join(PROCESSED, json_file))
    if data and "data" in data:
        for d in data["data"]:
            macro_rows.append({
                "year": d["year"], "indicator_id": ind_id,
                "indicator_name": ind_name, "value": d["value"],
                "unit": unit, "source_id": src_id, "notes": ""
            })
        print(f"  {ind_id}: {len(data['data'])}年分")

# raw CSVから追加指標を直接抽出
# 失業率
print("  失業率を検索中...")
for csv_file in sorted(glob.glob(os.path.join(RAW, "macro", "*.csv"))):
    rows = read_csv(csv_file)
    if not rows: continue
    
    # 完全失業率を含むテーブルか確認
    sample_text = " ".join(str(v) for r in rows[:200] for v in r.values())
    if "完全失業率" not in sample_text: continue
    
    unemp = {}
    for r in rows:
        all_text = " ".join(str(v) for v in r.values())
        if "完全失業率" not in all_text: continue
        
        # 年平均データのみ（月次除外）
        tl = str(r.get("time_label", ""))
        time_val = str(r.get("time", ""))
        
        # 年次データ判定: time_labelに「月」がない、または「年平均」を含む
        if "月" in tl and "年平均" not in tl: continue
        
        year = parse_year(time_val, tl)
        val = safe_float(r.get("value"))
        if year and val and 0.5 < val < 20 and year not in unemp:
            unemp[year] = val
    
    if unemp:
        for y, v in sorted(unemp.items()):
            macro_rows.append({
                "year": y, "indicator_id": "unemployment",
                "indicator_name": "完全失業率", "value": v,
                "unit": "%", "source_id": "estat-labor", "notes": ""
            })
        print(f"  unemployment: {len(unemp)}年分")
        break

# CPI（消費者物価指数）
print("  CPIを検索中...")
for csv_file in sorted(glob.glob(os.path.join(RAW, "macro", "*.csv"))):
    rows = read_csv(csv_file)
    if not rows: continue
    
    sample_text = " ".join(str(v) for r in rows[:200] for v in r.values())
    if "物価" not in sample_text and "CPI" not in sample_text: continue
    
    cpi = {}
    for r in rows:
        all_text = " ".join(str(v) for v in r.values())
        # 総合指数のみ
        if "総合" not in all_text: continue
        
        tl = str(r.get("time_label", ""))
        if "月" in tl and "年平均" not in tl: continue
        
        year = parse_year(r.get("time", ""), tl)
        val = safe_float(r.get("value"))
        area = label(r, "area")
        
        if year and val and 50 < val < 200:
            if area and "全国" not in area: continue
            if year not in cpi:
                cpi[year] = val
    
    if cpi:
        for y, v in sorted(cpi.items()):
            macro_rows.append({
                "year": y, "indicator_id": "cpi",
                "indicator_name": "消費者物価指数", "value": v,
                "unit": "指数", "source_id": "estat-cpi", "notes": ""
            })
        print(f"  cpi: {len(cpi)}年分")
        break

# 鉱工業指数
print("  鉱工業指数を検索中...")
for csv_file in sorted(glob.glob(os.path.join(RAW, "macro", "*.csv"))):
    rows = read_csv(csv_file)
    if not rows: continue
    
    sample_text = " ".join(str(v) for r in rows[:100] for v in r.values())
    if "鉱工業" not in sample_text and "生産指数" not in sample_text: continue
    
    iip = {}
    for r in rows:
        all_text = " ".join(str(v) for v in r.values())
        if "鉱工業" not in all_text and "製造工業" not in all_text: continue
        
        tl = str(r.get("time_label", ""))
        if "月" in tl and "年" not in tl: continue
        
        year = parse_year(r.get("time", ""), tl)
        val = safe_float(r.get("value"))
        if year and val and 30 < val < 200 and year not in iip:
            iip[year] = val
    
    if iip:
        for y, v in sorted(iip.items()):
            macro_rows.append({
                "year": y, "indicator_id": "iip",
                "indicator_name": "鉱工業生産指数", "value": v,
                "unit": "指数", "source_id": "estat-iip", "notes": ""
            })
        print(f"  iip: {len(iip)}年分")
        break

# ソート
macro_rows.sort(key=lambda x: (x["indicator_id"], x["year"]))
write_csv(os.path.join(OUT, "B_macro_annual.csv"), macro_headers, macro_rows)

# ════════════════════════════════════════════════════════════
# C. 業界データサマリー
# ════════════════════════════════════════════════════════════
print("\n■ C. 業界データサマリー")
industry_headers = ["directory", "table_id", "total_rows", "year_range", "categories", "source_id"]
industry_summary = []

dir_source_map = {
    "manufacturing": "estat-manufacturing", "census": "estat-census",
    "corporate": "estat-corp-finance", "trade": "estat-trade",
    "wage-structure": "estat-wage-structure", "employment": "estat-employment",
    "national-census": "estat-national-census", "service": "estat-service",
    "medical": "estat-medical", "agriculture": "estat-agri",
    "construction": "estat-construction", "energy": "estat-energy",
    "education": "estat-school", "transport": "estat-transport",
    "safety": "estat-crime", "ict": "estat-ict", "rd": "estat-rd",
}

for subdir in sorted(os.listdir(RAW)):
    dir_path = os.path.join(RAW, subdir)
    if not os.path.isdir(dir_path): continue
    src_id = dir_source_map.get(subdir, f"estat-{subdir}")
    
    for csv_file in sorted(glob.glob(os.path.join(dir_path, "*.csv"))):
        table_id = os.path.splitext(os.path.basename(csv_file))[0]
        rows = read_csv(csv_file)
        if not rows: continue
        
        years = set()
        for r in rows:
            y = parse_year(r.get("time", ""), r.get("time_label", ""))
            if y: years.add(y)
        
        cats = []
        for col in list(rows[0].keys()):
            if col.endswith("_label"):
                vals = set(str(r.get(col, "")) for r in rows[:1000] if r.get(col))
                if 1 < len(vals) < 50:
                    cats.append(f"{col}({len(vals)})")
        
        industry_summary.append({
            "directory": subdir, "table_id": table_id,
            "total_rows": len(rows),
            "year_range": f"{min(years)}-{max(years)}" if years else "—",
            "categories": " / ".join(cats[:5]), "source_id": src_id,
        })

write_csv(os.path.join(OUT, "C_industry_summary.csv"), industry_headers, industry_summary)

# ════════════════════════════════════════════════════════════
# C-1. 製造業品目別
# ════════════════════════════════════════════════════════════
print("\n■ C-1. 製造業品目別")
mfg_headers = ["table_id", "category", "product_name", "value", "source_id"]
mfg_rows = []

mfg_dir = os.path.join(RAW, "manufacturing")
if os.path.isdir(mfg_dir):
    for csv_file in sorted(glob.glob(os.path.join(mfg_dir, "*.csv"))):
        table_id = os.path.splitext(os.path.basename(csv_file))[0]
        rows = read_csv(csv_file)
        seen = set()
        for r in rows:
            cat = label(r, "cat01")
            product = label(r, "cat02")
            val = safe_float(r.get("value"))
            key = f"{cat}:{product}"
            if product and val is not None and key not in seen:
                seen.add(key)
                mfg_rows.append({"table_id": table_id, "category": cat,
                    "product_name": product, "value": val, "source_id": "estat-manufacturing"})

write_csv(os.path.join(OUT, "C1_manufacturing_products.csv"), mfg_headers, mfg_rows)

# ════════════════════════════════════════════════════════════
# 国勢調査 産業別就業者
# ════════════════════════════════════════════════════════════
print("\n■ 国勢調査 産業別就業者")
census_headers = ["industry_category", "year", "value", "unit", "gender", "source_id"]
census_rows = []

nc_dir = os.path.join(RAW, "national-census")
if os.path.isdir(nc_dir):
    for csv_file in sorted(glob.glob(os.path.join(nc_dir, "*.csv"))):
        rows = read_csv(csv_file)
        for r in rows:
            industry = ""
            for k in ["cat01_label", "cat02_label", "cat03_label"]:
                v = str(r.get(k, ""))
                if len(v) > 2 and any(x in v for x in ["業", "製造", "建設", "卸売", "小売", "金融", "情報", "運輸", "医療", "教育", "農", "漁", "鉱", "電気", "ガス", "サービス", "宿泊", "飲食"]):
                    industry = v
                    break
            if not industry: continue
            
            year = parse_year(r.get("time", ""), r.get("time_label", ""))
            val = safe_float(r.get("value"))
            if year and val is not None:
                gender = ""
                for k in ["cat01_label", "cat02_label"]:
                    v = str(r.get(k, ""))
                    if v in ("男", "女", "総数"): gender = v
                census_rows.append({"industry_category": industry, "year": year,
                    "value": val, "unit": "人", "gender": gender or "総数",
                    "source_id": "estat-national-census"})

write_csv(os.path.join(OUT, "national_census_industry.csv"), census_headers, census_rows)

# ════════════════════════════════════════════════════════════
# 経済センサス
# ════════════════════════════════════════════════════════════
print("\n■ 経済センサス")
ec_headers = ["industry_code", "industry_name", "indicator", "value", "year", "source_id"]
ec_rows = []

ec_dir = os.path.join(RAW, "census")
if os.path.isdir(ec_dir):
    for csv_file in sorted(glob.glob(os.path.join(ec_dir, "*.csv"))):
        rows = read_csv(csv_file)
        for r in rows:
            ind_name = ""
            ind_code = ""
            for k in sorted(r.keys()):
                if k.endswith("_label"):
                    v = str(r.get(k, ""))
                    code_key = k.replace("_label", "")
                    code = str(r.get(code_key, ""))
                    if len(v) > 2 and any(x in v for x in ["業", "製造", "卸売", "小売", "建設", "運輸", "金融", "医療", "サービス"]):
                        ind_name = v; ind_code = code; break
            if not ind_name: continue
            
            val = safe_float(r.get("value"))
            if val is None: continue
            tab = label(r, "tab")
            year = parse_year(r.get("time", ""), r.get("time_label", ""))
            ec_rows.append({"industry_code": ind_code, "industry_name": ind_name,
                "indicator": tab or "—", "value": val, "year": year or "—",
                "source_id": "estat-census"})

write_csv(os.path.join(OUT, "economic_census.csv"), ec_headers, ec_rows)

# ════════════════════════════════════════════════════════════
# 賃金構造
# ════════════════════════════════════════════════════════════
print("\n■ 賃金構造基本統計")
wage_headers = ["industry_or_category", "indicator", "value", "year", "source_id"]
wage_rows = []

ws_dir = os.path.join(RAW, "wage-structure")
if os.path.isdir(ws_dir):
    for csv_file in sorted(glob.glob(os.path.join(ws_dir, "*.csv")))[:3]:
        rows = read_csv(csv_file)
        for r in rows[:5000]:
            cat_labels = []
            for k in sorted(r.keys()):
                if k.endswith("_label"):
                    v = str(r.get(k, ""))
                    if v and len(v) < 40: cat_labels.append(v)
            val = safe_float(r.get("value"))
            if val is None: continue
            tab = label(r, "tab")
            year = parse_year(r.get("time", ""), r.get("time_label", ""))
            wage_rows.append({"industry_or_category": " / ".join(cat_labels[:3]),
                "indicator": tab or "—", "value": val, "year": year or "—",
                "source_id": "estat-wage-structure"})

write_csv(os.path.join(OUT, "wage_structure.csv"), wage_headers, wage_rows)

# ════════════════════════════════════════════════════════════
# 法人企業統計
# ════════════════════════════════════════════════════════════
print("\n■ 法人企業統計")
corp_headers = ["industry_or_category", "indicator", "value", "year", "source_id"]
corp_rows = []

corp_dir = os.path.join(RAW, "corporate")
if os.path.isdir(corp_dir):
    for csv_file in sorted(glob.glob(os.path.join(corp_dir, "*.csv")))[:5]:
        rows = read_csv(csv_file)
        for r in rows[:5000]:
            cat_labels = []
            for k in sorted(r.keys()):
                if k.endswith("_label"):
                    v = str(r.get(k, ""))
                    if v and len(v) < 40: cat_labels.append(v)
            val = safe_float(r.get("value"))
            if val is None: continue
            tab = label(r, "tab")
            year = parse_year(r.get("time", ""), r.get("time_label", ""))
            corp_rows.append({"industry_or_category": " / ".join(cat_labels[:3]),
                "indicator": tab or "—", "value": val, "year": year or "—",
                "source_id": "estat-corporate"})

write_csv(os.path.join(OUT, "corporate_finance.csv"), corp_headers, corp_rows)

# ════════════════════════════════════════════════════════════
# サービス産業
# ════════════════════════════════════════════════════════════
print("\n■ サービス産業動向")
svc_headers = ["industry_or_category", "indicator", "value", "year_month", "source_id"]
svc_rows = []

svc_dir = os.path.join(RAW, "service")
if os.path.isdir(svc_dir):
    for csv_file in sorted(glob.glob(os.path.join(svc_dir, "*.csv")))[:3]:
        rows = read_csv(csv_file)
        for r in rows[:5000]:
            cat_labels = []
            for k in sorted(r.keys()):
                if k.endswith("_label"):
                    v = str(r.get(k, ""))
                    if v and len(v) < 40: cat_labels.append(v)
            val = safe_float(r.get("value"))
            if val is None: continue
            tab = label(r, "tab")
            time_val = r.get("time", "")
            svc_rows.append({"industry_or_category": " / ".join(cat_labels[:3]),
                "indicator": tab or "—", "value": val, "year_month": time_val,
                "source_id": "estat-service"})

write_csv(os.path.join(OUT, "service_industry.csv"), svc_headers, svc_rows)

# ════════════════════════════════════════════════════════════
# 医療
# ════════════════════════════════════════════════════════════
print("\n■ 医療統計")
med_headers = ["category", "indicator", "value", "year", "source_id"]
med_rows = []

med_dir = os.path.join(RAW, "medical")
if os.path.isdir(med_dir):
    for csv_file in sorted(glob.glob(os.path.join(med_dir, "*.csv"))):
        rows = read_csv(csv_file)
        for r in rows[:3000]:
            cat_labels = []
            for k in sorted(r.keys()):
                if k.endswith("_label"):
                    v = str(r.get(k, ""))
                    if v and len(v) < 40: cat_labels.append(v)
            val = safe_float(r.get("value"))
            if val is None: continue
            tab = label(r, "tab")
            year = parse_year(r.get("time", ""), r.get("time_label", ""))
            med_rows.append({"category": " / ".join(cat_labels[:3]),
                "indicator": tab or "—", "value": val, "year": year or "—",
                "source_id": "estat-medical"})

write_csv(os.path.join(OUT, "medical.csv"), med_headers, med_rows)

# ════════════════════════════════════════════════════════════
# サマリー
# ════════════════════════════════════════════════════════════
summary = {"timestamp": datetime.now().isoformat(), "files": {}}
for f in sorted(os.listdir(OUT)):
    if f.endswith(".csv"):
        path = os.path.join(OUT, f)
        with open(path) as fh: lines = sum(1 for _ in fh) - 1
        size = os.path.getsize(path)
        summary["files"][f] = {"rows": lines, "size_kb": round(size/1024, 1)}

with open(os.path.join(OUT, "_summary.json"), "w", encoding="utf-8") as f:
    json.dump(summary, f, ensure_ascii=False, indent=2)

print(f"\n{'=' * 70}")
print(f"集約完了 v2: {len(summary['files'])} CSVファイル → {OUT}/")
for fname, info in summary["files"].items():
    print(f"  {fname}: {info['rows']}行 ({info['size_kb']}KB)")
print(f"{'=' * 70}")
