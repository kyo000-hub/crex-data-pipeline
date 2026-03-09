#!/usr/bin/env python3
"""
CREX データ集約スクリプト
raw CSV (300万行) → Googleスプレッドシート用CSV (集約済み)
出力: data/gsheet/ 以下にB〜E用のCSVを生成
"""
import os, csv, re, json, glob
from datetime import datetime
from collections import defaultdict

RAW = "data/raw"
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

print("=" * 70)
print("CREX データ集約: raw CSV → Googleスプレッドシート用CSV")
print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)

# ════════════════════════════════════════════════════════════
# B. マクロ時系列
# ════════════════════════════════════════════════════════════
print("\n■ B. マクロ時系列")
macro_rows = []
today = datetime.now().strftime("%Y-%m-%d")

# 各ディレクトリのCSVを走査してマクロ指標を抽出
macro_configs = [
    # (ディレクトリ, ファイルパターン, indicator_id, indicator_name, 値フィルタ条件, unit, source_id)
    ("macro", "population", "population", "総人口", {"cat01_label": "人口・総数", "cat02_label": "総数", "area_label": "全国"}, "千人", "estat-population"),
    ("macro", "housing", "housing-starts", "住宅着工戸数", {"area_label": "全国"}, "戸", "estat-housing"),
    ("macro", "retail", "retail-sales", "小売販売額", {}, "10億円", "estat-retail"),
    ("macro", "avg-wage", "avg-wage", "賃金指数", {"area_label": "全国", "cat03_label": "5人以上"}, "指数", "estat-wage"),
]

for subdir, file_pattern, ind_id, ind_name, filters, unit, src_id in macro_configs:
    dir_path = os.path.join(RAW, subdir)
    if not os.path.isdir(dir_path):
        continue
    
    for csv_file in glob.glob(os.path.join(dir_path, "*.csv")):
        if file_pattern and file_pattern not in os.path.basename(csv_file).lower():
            continue
        
        rows = read_csv(csv_file)
        yearly = {}
        
        for r in rows:
            # フィルタ適用
            skip = False
            for key, target in filters.items():
                val = str(r.get(key, ""))
                if target and target not in val:
                    skip = True
                    break
            if skip:
                continue
            
            # 月次データを除外（年次のみ）
            tl = r.get("time_label", "")
            if "月" in str(tl) and "年" in str(tl) and len(str(tl)) > 5:
                continue
            
            year = parse_year(r.get("time", ""), tl)
            val = safe_float(r.get("value"))
            
            if year and val is not None:
                if year not in yearly:
                    yearly[year] = val
        
        for y, v in sorted(yearly.items()):
            macro_rows.append({
                "year": y, "indicator_id": ind_id, "indicator_name": ind_name,
                "value": v, "unit": unit, "source_id": src_id, "notes": ""
            })
        
        if yearly:
            break  # 最初にヒットしたファイルのみ

# 労働力調査（失業率）- 特殊処理
for csv_file in glob.glob(os.path.join(RAW, "macro", "*.csv")):
    rows = read_csv(csv_file)
    if not rows: continue
    
    # 完全失業率を含むか確認
    has_unemp = any("完全失業率" in str(v) for r in rows[:100] for v in r.values())
    if not has_unemp: continue
    
    yearly = {}
    for r in rows:
        all_vals = " ".join(str(v) for v in r.values())
        if "完全失業率" not in all_vals: continue
        
        tl = r.get("time_label", "")
        if "月" in str(tl): continue
        
        year = parse_year(r.get("time", ""), tl)
        val = safe_float(r.get("value"))
        if year and val and 0 < val < 30 and year not in yearly:
            yearly[year] = val
    
    for y, v in sorted(yearly.items()):
        macro_rows.append({
            "year": y, "indicator_id": "unemployment", "indicator_name": "完全失業率",
            "value": v, "unit": "%", "source_id": "estat-labor", "notes": ""
        })
    if yearly: break

# 鉱工業指数
for csv_file in sorted(glob.glob(os.path.join(RAW, "macro", "*.csv"))):
    rows = read_csv(csv_file)
    if not rows: continue
    
    has_iip = any("生産" in str(v) and "指数" in str(v) for r in rows[:50] for v in r.values())
    if not has_iip: continue
    
    yearly = {}
    for r in rows:
        all_vals = " ".join(str(v) for v in r.values())
        if "鉱工業" not in all_vals and "生産" not in all_vals: continue
        
        tl = r.get("time_label", "")
        if "月" in str(tl): continue
        
        year = parse_year(r.get("time", ""), tl)
        val = safe_float(r.get("value"))
        if year and val and 10 < val < 200 and year not in yearly:
            yearly[year] = val
    
    for y, v in sorted(yearly.items()):
        macro_rows.append({
            "year": y, "indicator_id": "iip", "indicator_name": "鉱工業生産指数",
            "value": v, "unit": "指数", "source_id": "estat-iip", "notes": ""
        })
    if yearly: break

macro_headers = ["year", "indicator_id", "indicator_name", "value", "unit", "source_id", "notes"]
write_csv(os.path.join(OUT, "B_macro_annual.csv"), macro_headers, macro_rows)

# ════════════════════════════════════════════════════════════
# C. 業界データ（全ディレクトリのサマリー）
# ════════════════════════════════════════════════════════════
print("\n■ C. 業界データサマリー")
industry_headers = ["directory", "table_id", "table_title", "total_rows", "year_range", "categories", "source_id"]
industry_summary = []

dir_source_map = {
    "manufacturing": "estat-manufacturing",
    "census": "estat-census",
    "corporate": "estat-corp-finance",
    "trade": "estat-trade",
    "wage-structure": "estat-wage-structure",
    "employment": "estat-employment",
    "national-census": "estat-national-census",
    "service": "estat-service",
    "medical": "estat-medical",
    "agriculture": "estat-agri",
    "construction": "estat-construction",
    "energy": "estat-energy",
    "education": "estat-school",
    "transport": "estat-transport",
    "safety": "estat-crime",
    "ict": "estat-ict",
    "rd": "estat-rd",
}

for subdir in sorted(os.listdir(RAW)):
    dir_path = os.path.join(RAW, subdir)
    if not os.path.isdir(dir_path): continue
    
    src_id = dir_source_map.get(subdir, f"estat-{subdir}")
    
    for csv_file in sorted(glob.glob(os.path.join(dir_path, "*.csv"))):
        table_id = os.path.splitext(os.path.basename(csv_file))[0]
        rows = read_csv(csv_file)
        if not rows: continue
        
        # 年範囲
        years = set()
        for r in rows:
            y = parse_year(r.get("time", ""), r.get("time_label", ""))
            if y: years.add(y)
        
        # カテゴリ情報
        cats = []
        for col in list(rows[0].keys()):
            if col.endswith("_label"):
                vals = set(str(r.get(col, "")) for r in rows[:1000] if r.get(col))
                if 1 < len(vals) < 50:
                    cats.append(f"{col}({len(vals)}種)")
        
        industry_summary.append({
            "directory": subdir,
            "table_id": table_id,
            "table_title": "",  # タイトルはraw CSVにない
            "total_rows": len(rows),
            "year_range": f"{min(years)}-{max(years)}" if years else "—",
            "categories": " / ".join(cats[:5]),
            "source_id": src_id,
        })

write_csv(os.path.join(OUT, "C_industry_summary.csv"), industry_headers, industry_summary)

# ════════════════════════════════════════════════════════════
# 製造業品目別データ（C-1用）
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
                mfg_rows.append({
                    "table_id": table_id,
                    "category": cat,
                    "product_name": product,
                    "value": val,
                    "source_id": "estat-manufacturing",
                })

write_csv(os.path.join(OUT, "C1_manufacturing_products.csv"), mfg_headers, mfg_rows)

# ════════════════════════════════════════════════════════════
# 国勢調査 産業別就業者（全業界に配布可能）
# ════════════════════════════════════════════════════════════
print("\n■ 国勢調査 産業別就業者")
census_headers = ["industry_category", "year", "value", "unit", "gender", "source_id"]
census_rows = []

nc_dir = os.path.join(RAW, "national-census")
if os.path.isdir(nc_dir):
    for csv_file in sorted(glob.glob(os.path.join(nc_dir, "*.csv"))):
        rows = read_csv(csv_file)
        for r in rows:
            # 産業分類ラベルを探す
            industry = ""
            for k in ["cat01_label", "cat02_label", "cat03_label"]:
                v = str(r.get(k, ""))
                if any(x in v for x in ["製造業", "建設業", "卸売", "小売", "金融", "情報通信", "運輸", "医療", "教育", "農業", "漁業", "鉱業", "電気", "ガス", "サービス"]):
                    industry = v
                    break
            
            if not industry: continue
            
            year = parse_year(r.get("time", ""), r.get("time_label", ""))
            val = safe_float(r.get("value"))
            
            if year and val is not None:
                gender = ""
                for k in ["cat01_label", "cat02_label"]:
                    v = str(r.get(k, ""))
                    if v in ("男", "女", "総数"):
                        gender = v
                
                census_rows.append({
                    "industry_category": industry,
                    "year": year,
                    "value": val,
                    "unit": "人",
                    "gender": gender or "総数",
                    "source_id": "estat-national-census",
                })

write_csv(os.path.join(OUT, "national_census_industry.csv"), census_headers, census_rows)

# ════════════════════════════════════════════════════════════
# 経済センサス（産業別）
# ════════════════════════════════════════════════════════════
print("\n■ 経済センサス")
ec_headers = ["industry_code", "industry_name", "indicator", "value", "year", "source_id"]
ec_rows = []

ec_dir = os.path.join(RAW, "census")
if os.path.isdir(ec_dir):
    for csv_file in sorted(glob.glob(os.path.join(ec_dir, "*.csv"))):
        rows = read_csv(csv_file)
        for r in rows:
            # 産業分類を探す
            ind_code = ""
            ind_name = ""
            for k in sorted(r.keys()):
                if k.endswith("_label"):
                    v = str(r.get(k, ""))
                    code_key = k.replace("_label", "")
                    code = str(r.get(code_key, ""))
                    if len(v) > 2 and any(x in v for x in ["業", "製造", "卸売", "小売", "建設", "運輸", "金融", "医療"]):
                        ind_name = v
                        ind_code = code
                        break
            
            if not ind_name: continue
            
            val = safe_float(r.get("value"))
            if val is None: continue
            
            # 表章項目
            tab = label(r, "tab")
            year = parse_year(r.get("time", ""), r.get("time_label", ""))
            
            ec_rows.append({
                "industry_code": ind_code,
                "industry_name": ind_name,
                "indicator": tab or "—",
                "value": val,
                "year": year or "—",
                "source_id": "estat-census",
            })

write_csv(os.path.join(OUT, "economic_census.csv"), ec_headers, ec_rows)

# ════════════════════════════════════════════════════════════
# 賃金構造（産業別）
# ════════════════════════════════════════════════════════════
print("\n■ 賃金構造基本統計")
wage_headers = ["industry_or_category", "indicator", "value", "year", "source_id"]
wage_rows = []

ws_dir = os.path.join(RAW, "wage-structure")
if os.path.isdir(ws_dir):
    for csv_file in sorted(glob.glob(os.path.join(ws_dir, "*.csv")))[:3]:
        rows = read_csv(csv_file)
        for r in rows[:5000]:  # 上限
            cat_labels = []
            for k in sorted(r.keys()):
                if k.endswith("_label"):
                    v = str(r.get(k, ""))
                    if v and len(v) < 40:
                        cat_labels.append(v)
            
            val = safe_float(r.get("value"))
            if val is None: continue
            
            tab = label(r, "tab")
            year = parse_year(r.get("time", ""), r.get("time_label", ""))
            
            wage_rows.append({
                "industry_or_category": " / ".join(cat_labels[:3]),
                "indicator": tab or "—",
                "value": val,
                "year": year or "—",
                "source_id": "estat-wage-structure",
            })

write_csv(os.path.join(OUT, "wage_structure.csv"), wage_headers, wage_rows)

# ════════════════════════════════════════════════════════════
# サマリー
# ════════════════════════════════════════════════════════════
summary = {
    "timestamp": datetime.now().isoformat(),
    "files": {}
}
for f in sorted(os.listdir(OUT)):
    if f.endswith(".csv"):
        path = os.path.join(OUT, f)
        with open(path) as fh:
            lines = sum(1 for _ in fh) - 1
        size = os.path.getsize(path)
        summary["files"][f] = {"rows": lines, "size_kb": round(size/1024, 1)}
        
with open(os.path.join(OUT, "_summary.json"), "w", encoding="utf-8") as f:
    json.dump(summary, f, ensure_ascii=False, indent=2)

print(f"\n{'=' * 70}")
print(f"集約完了: {len(summary['files'])} CSVファイル → {OUT}/")
for fname, info in summary["files"].items():
    print(f"  {fname}: {info['rows']}行 ({info['size_kb']}KB)")
print(f"{'=' * 70}")
