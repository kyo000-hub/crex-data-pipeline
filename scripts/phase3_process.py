#!/usr/bin/env python3
"""
CREX Phase 3: 汎用データ加工スクリプト
data/raw/ 以下の全CSVを走査し、data/processed/ にJSON出力する。
各ディレクトリ（macro/census/trade/corporate/manufacturing等）を自動検出。
"""
import os, json, csv, re, glob
from datetime import datetime
from collections import defaultdict

RAW_DIR = "data/raw"
OUT_DIR = "data/processed"
os.makedirs(OUT_DIR, exist_ok=True)

# ════════════════════════════════════════════════════════════
# ユーティリティ
# ════════════════════════════════════════════════════════════
def parse_time_to_year(time_str, time_label=""):
    if time_str and len(str(time_str)) >= 4:
        y = str(time_str)[:4]
        if y.isdigit() and 1900 <= int(y) <= 2100:
            return int(y)
    m = re.search(r'(\d{4})年', str(time_label))
    if m: return int(m.group(1))
    m = re.search(r'平成(\d+)年', str(time_label))
    if m: return 1988 + int(m.group(1))
    m = re.search(r'令和(\d+)年', str(time_label))
    if m: return 2018 + int(m.group(1))
    m = re.search(r'昭和(\d+)年', str(time_label))
    if m: return 1925 + int(m.group(1))
    return None

def safe_float(v):
    if v is None or str(v).strip() in ("", "-", "…", "x", "***", "X", "－", "…"):
        return None
    try:
        return float(str(v).replace(",", "").replace("，", ""))
    except:
        return None

def read_csv_safe(filepath):
    try:
        with open(filepath, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return list(reader)
    except Exception as e:
        print(f"    ⚠️ CSV読み込みエラー: {e}")
        return []

def get_label(row, key):
    """ラベル列があればそれを、なければコード値を返す"""
    return row.get(f"{key}_label", row.get(key, ""))

def save_json(data, filename):
    filepath = os.path.join(OUT_DIR, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return filepath

# ════════════════════════════════════════════════════════════
# 汎用CSV→サマリー変換
# ════════════════════════════════════════════════════════════
def summarize_csv(filepath, table_id=""):
    """任意のCSVを読み込み、構造を分析してサマリーを返す"""
    rows = read_csv_safe(filepath)
    if not rows:
        return None
    
    cols = list(rows[0].keys())
    
    # 時系列データかどうか判定
    has_time = any(c in cols for c in ["time", "time_label"])
    has_area = any(c in cols for c in ["area", "area_label"])
    
    # 分類軸を特定（cat01, cat02, tab等）
    cat_cols = [c for c in cols if re.match(r'(cat\d+|tab)$', c)]
    label_cols = [c for c in cols if re.match(r'(cat\d+|tab)_label$', c)]
    
    # 各分類軸のユニーク値数
    axis_info = {}
    for lc in label_cols:
        vals = set(str(r.get(lc, "")) for r in rows if r.get(lc))
        axis_info[lc] = {"count": len(vals), "samples": sorted(list(vals))[:10]}
    
    # 年の範囲
    years = set()
    for r in rows:
        y = parse_time_to_year(r.get("time", ""), r.get("time_label", ""))
        if y: years.add(y)
    
    # 地域
    areas = set()
    for r in rows:
        a = r.get("area_label", "")
        if a: areas.add(a)
    
    # 値の統計
    values = [safe_float(r.get("value")) for r in rows]
    valid_values = [v for v in values if v is not None]
    
    return {
        "table_id": table_id,
        "file": os.path.basename(filepath),
        "total_rows": len(rows),
        "columns": cols,
        "has_time": has_time,
        "has_area": has_area,
        "years": sorted(years) if years else [],
        "year_range": f"{min(years)}-{max(years)}" if years else "—",
        "areas": sorted(areas)[:5] if areas else [],
        "area_count": len(areas),
        "axis_info": axis_info,
        "value_count": len(valid_values),
        "value_range": f"{min(valid_values):.1f}-{max(valid_values):.1f}" if valid_values else "—",
    }

# ════════════════════════════════════════════════════════════
# ディレクトリ別の加工処理
# ════════════════════════════════════════════════════════════

def process_macro(subdir):
    """マクロ統計の加工（年次時系列を抽出）"""
    results = []
    dir_path = os.path.join(RAW_DIR, subdir)
    if not os.path.isdir(dir_path):
        return results
    
    for csv_file in glob.glob(os.path.join(dir_path, "*.csv")):
        table_id = os.path.splitext(os.path.basename(csv_file))[0]
        rows = read_csv_safe(csv_file)
        if not rows:
            continue
        
        summary = summarize_csv(csv_file, table_id)
        if summary:
            results.append(summary)
    
    return results

def process_industry_dir(subdir):
    """業界系ディレクトリの加工"""
    results = []
    dir_path = os.path.join(RAW_DIR, subdir)
    if not os.path.isdir(dir_path):
        return results
    
    for csv_file in glob.glob(os.path.join(dir_path, "*.csv")):
        table_id = os.path.splitext(os.path.basename(csv_file))[0]
        summary = summarize_csv(csv_file, table_id)
        if summary:
            results.append(summary)
    
    return results

def extract_time_series(rows, value_filter=None, area_filter="全国"):
    """行データから年次時系列を抽出"""
    series = {}
    for r in rows:
        # 地域フィルタ
        if area_filter:
            area = r.get("area_label", "")
            if area and area_filter not in area:
                continue
        
        # 値フィルタ（特定のカテゴリのみ）
        if value_filter:
            match = False
            for key, target in value_filter.items():
                val = get_label(r, key)
                if target in str(val):
                    match = True
                    break
            if not match:
                continue
        
        year = parse_time_to_year(r.get("time", ""), r.get("time_label", ""))
        val = safe_float(r.get("value"))
        
        if year and val is not None:
            if year not in series or val > 0:
                series[year] = val
    
    return [{"year": y, "value": v} for y, v in sorted(series.items())]

# ════════════════════════════════════════════════════════════
# メイン処理
# ════════════════════════════════════════════════════════════
print("=" * 70)
print("CREX Phase 3: 汎用データ加工")
print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)

# 全ディレクトリを走査
all_dirs = []
for item in os.listdir(RAW_DIR):
    full = os.path.join(RAW_DIR, item)
    if os.path.isdir(full):
        all_dirs.append(item)

print(f"\n検出ディレクトリ: {sorted(all_dirs)}")

# ── 1. 各ディレクトリのサマリー生成 ──
master_index = {
    "updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
    "directories": {},
    "total_files": 0,
    "total_rows": 0,
}

for subdir in sorted(all_dirs):
    dir_path = os.path.join(RAW_DIR, subdir)
    csv_files = glob.glob(os.path.join(dir_path, "*.csv"))
    
    if not csv_files:
        continue
    
    print(f"\n■ {subdir}/ ({len(csv_files)} CSVファイル)")
    
    dir_summary = {
        "file_count": len(csv_files),
        "total_rows": 0,
        "tables": [],
    }
    
    for csv_file in sorted(csv_files):
        table_id = os.path.splitext(os.path.basename(csv_file))[0]
        summary = summarize_csv(csv_file, table_id)
        
        if summary:
            dir_summary["tables"].append(summary)
            dir_summary["total_rows"] += summary["total_rows"]
            print(f"  ✅ {table_id}: {summary['total_rows']}行, {summary['year_range']}, 地域{summary['area_count']}")
    
    master_index["directories"][subdir] = dir_summary
    master_index["total_files"] += len(csv_files)
    master_index["total_rows"] += dir_summary["total_rows"]

# ── 2. マクロ指標の時系列JSON生成 ──
print(f"\n■ マクロ時系列JSON生成")

# 人口推計
pop_file = os.path.join(RAW_DIR, "macro", "population.csv")
if os.path.exists(pop_file):
    rows = read_csv_safe(pop_file)
    total_pop = {}
    for r in rows:
        cat01 = get_label(r, "cat01")
        cat02 = get_label(r, "cat02")
        area = get_label(r, "area")
        if "総数" in cat01 and "総数" in cat02 and area == "全国":
            y = parse_time_to_year(r.get("time", ""), r.get("time_label", ""))
            v = safe_float(r.get("value"))
            if y and v: total_pop[y] = v
    
    if total_pop:
        save_json({
            "indicator_id": "population", "indicator_name": "総人口",
            "unit": "千人", "source": "e-Stat 人口推計",
            "updated": datetime.now().strftime("%Y-%m-%d"),
            "data": [{"year": y, "value": v} for y, v in sorted(total_pop.items())]
        }, "population.json")
        print(f"  ✅ population.json ({len(total_pop)}年)")

# 住宅着工
hs_files = glob.glob(os.path.join(RAW_DIR, "macro", "*.csv"))
for f in hs_files:
    if "housing" in os.path.basename(f).lower():
        rows = read_csv_safe(f)
        national = {}
        for r in rows:
            if get_label(r, "area") == "全国":
                y = parse_time_to_year(r.get("time", ""), r.get("time_label", ""))
                v = safe_float(r.get("value"))
                if y and v:
                    national[y] = national.get(y, 0) + v
        if national:
            save_json({
                "indicator_id": "housing-starts", "indicator_name": "新設住宅着工戸数",
                "unit": "戸", "source": "e-Stat 住宅着工統計",
                "updated": datetime.now().strftime("%Y-%m-%d"),
                "data": [{"year": y, "value": v} for y, v in sorted(national.items())]
            }, "housing-starts.json")
            print(f"  ✅ housing-starts.json ({len(national)}年)")
        break

# 小売販売額
for f in glob.glob(os.path.join(RAW_DIR, "macro", "*.csv")):
    if "retail" in os.path.basename(f).lower():
        rows = read_csv_safe(f)
        national = {}
        for r in rows:
            tl = r.get("time_label", "")
            if "月" in tl or "～" in tl or "〜" in tl:
                continue
            y = parse_time_to_year(r.get("time", ""), tl)
            v = safe_float(r.get("value"))
            if y and v and y not in national:
                national[y] = v
        if national:
            save_json({
                "indicator_id": "retail-sales", "indicator_name": "小売業販売額",
                "unit": "10億円", "source": "e-Stat 商業動態統計",
                "updated": datetime.now().strftime("%Y-%m-%d"),
                "data": [{"year": y, "value": v} for y, v in sorted(national.items())]
            }, "retail-sales.json")
            print(f"  ✅ retail-sales.json ({len(national)}年)")
        break

# 賃金指数
for f in glob.glob(os.path.join(RAW_DIR, "macro", "*.csv")):
    if "wage" in os.path.basename(f).lower() or "avg" in os.path.basename(f).lower():
        rows = read_csv_safe(f)
        wages = {}
        for r in rows:
            area = get_label(r, "area")
            cat03 = get_label(r, "cat03")
            if area == "全国" and "5人以上" in cat03:
                y = parse_time_to_year(r.get("time", ""), r.get("time_label", ""))
                v = safe_float(r.get("value"))
                if y and v and y not in wages:
                    wages[y] = v
        if wages:
            save_json({
                "indicator_id": "avg-wage", "indicator_name": "賃金指数（現金給与総額）",
                "unit": "指数（H17=100）", "source": "e-Stat 毎月勤労統計",
                "updated": datetime.now().strftime("%Y-%m-%d"),
                "data": [{"year": y, "value": v} for y, v in sorted(wages.items())]
            }, "avg-wage.json")
            print(f"  ✅ avg-wage.json ({len(wages)}年)")
        break

# ── 3. 新規：労働力調査（失業率） ──
labor_dir = os.path.join(RAW_DIR, "macro")
for f in glob.glob(os.path.join(labor_dir, "*.csv")):
    rows = read_csv_safe(f)
    if not rows:
        continue
    # 完全失業率を含むテーブルを探す
    has_unemployment = False
    for r in rows[:50]:
        for k, v in r.items():
            if "完全失業率" in str(v):
                has_unemployment = True
                break
    if not has_unemployment:
        continue
    
    unemp = {}
    for r in rows:
        # 年次データ・全国のみ
        labels = " ".join(str(v) for v in r.values())
        if "完全失業率" in labels:
            y = parse_time_to_year(r.get("time", ""), r.get("time_label", ""))
            v = safe_float(r.get("value"))
            if y and v and 0 < v < 30:  # 失業率は0-30%の範囲
                if y not in unemp:
                    unemp[y] = v
    
    if unemp:
        save_json({
            "indicator_id": "unemployment", "indicator_name": "完全失業率",
            "unit": "%", "source": "e-Stat 労働力調査",
            "updated": datetime.now().strftime("%Y-%m-%d"),
            "data": [{"year": y, "value": v} for y, v in sorted(unemp.items())]
        }, "unemployment.json")
        print(f"  ✅ unemployment.json ({len(unemp)}年)")
        break

# ── 4. 全ディレクトリのCSVファイルごとのサマリーJSON ──
print(f"\n■ ディレクトリ別サマリーJSON生成")

for subdir in sorted(all_dirs):
    dir_path = os.path.join(RAW_DIR, subdir)
    csv_files = [f for f in glob.glob(os.path.join(dir_path, "*.csv"))]
    
    if not csv_files:
        continue
    
    dir_data = {
        "directory": subdir,
        "updated": datetime.now().strftime("%Y-%m-%d"),
        "file_count": len(csv_files),
        "tables": [],
    }
    
    total_rows_dir = 0
    for csv_file in sorted(csv_files):
        table_id = os.path.splitext(os.path.basename(csv_file))[0]
        rows = read_csv_safe(csv_file)
        if not rows:
            continue
        
        # 各テーブルの基本情報
        cols = list(rows[0].keys())
        years = set()
        categories = defaultdict(set)
        
        for r in rows:
            y = parse_time_to_year(r.get("time", ""), r.get("time_label", ""))
            if y: years.add(y)
            for c in cols:
                if c.endswith("_label"):
                    v = str(r.get(c, ""))
                    if v and len(v) < 50:
                        categories[c].add(v)
        
        table_info = {
            "table_id": table_id,
            "rows": len(rows),
            "columns": cols,
            "year_range": f"{min(years)}-{max(years)}" if years else "—",
            "year_count": len(years),
            "categories": {k: {"count": len(v), "samples": sorted(list(v))[:15]} 
                          for k, v in categories.items()},
        }
        dir_data["tables"].append(table_info)
        total_rows_dir += len(rows)
    
    dir_data["total_rows"] = total_rows_dir
    
    if dir_data["tables"]:
        save_json(dir_data, f"_dir_{subdir}.json")
        print(f"  ✅ _dir_{subdir}.json ({len(dir_data['tables'])}テーブル, {total_rows_dir}行)")

# ── 5. マスターインデックス ──
print(f"\n■ マスターインデックス生成")

# processed JSONの一覧
processed_files = []
for f in sorted(os.listdir(OUT_DIR)):
    if f.endswith(".json") and f != "index.json":
        filepath = os.path.join(OUT_DIR, f)
        size = os.path.getsize(filepath)
        processed_files.append({
            "file": f,
            "size_kb": round(size / 1024, 1),
        })

master_index["processed_files"] = processed_files
master_index["processed_count"] = len(processed_files)

save_json(master_index, "index.json")
print(f"  ✅ index.json (ディレクトリ{len(master_index['directories'])}個, ファイル{master_index['total_files']}個, {master_index['total_rows']}行)")

# ── サマリー ──
print(f"\n{'=' * 70}")
print(f"Phase 3 完了")
print(f"  入力: {master_index['total_files']} CSVファイル, {master_index['total_rows']:,}行")
print(f"  出力: {len(processed_files)} JSONファイル → {OUT_DIR}/")
print(f"{'=' * 70}")
