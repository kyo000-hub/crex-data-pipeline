#!/usr/bin/env python3
"""
CREX マクロ指標 直接取得スクリプト
テーブルIDを直接指定して、確実に正しいデータを取得する。
出力: data/gsheet/B_macro_annual.csv
"""
import os, csv, re, json, requests, time
from datetime import datetime

API_KEY = os.environ.get("ESTAT_API_KEY", "")
BASE = "https://api.e-stat.go.jp/rest/3.0/app/json"
OUT = "data/gsheet"
os.makedirs(OUT, exist_ok=True)

def get_data(stats_id, limit=100000):
    r = requests.get(f"{BASE}/getStatsData", params={
        "appId": API_KEY, "statsDataId": stats_id, "limit": limit
    }, timeout=120)
    r.raise_for_status()
    stat = r.json().get("GET_STATS_DATA", {}).get("STATISTICAL_DATA", {})
    
    class_objs = stat.get("CLASS_INF", {}).get("CLASS_OBJ", [])
    if not isinstance(class_objs, list): class_objs = [class_objs]
    cmap = {}
    for obj in class_objs:
        oid = obj.get("@id", "")
        classes = obj.get("CLASS", [])
        if not isinstance(classes, list): classes = [classes]
        cmap[oid] = {c.get("@code", ""): c.get("@name", "") for c in classes}
    
    values = stat.get("DATA_INF", {}).get("VALUE", [])
    if not isinstance(values, list): values = [values] if values else []
    
    rows = []
    for v in values:
        row = {"value": v.get("$", "")}
        for key, val in v.items():
            if key.startswith("@"):
                clean = key.lstrip("@")
                row[clean] = val
                if clean in cmap and val in cmap[clean]:
                    row[f"{clean}_label"] = cmap[clean][val]
        rows.append(row)
    return rows

def parse_year(time_str, time_label=""):
    if time_str and len(str(time_str)) >= 4:
        y = str(time_str)[:4]
        if y.isdigit() and 1900 <= int(y) <= 2100: return int(y)
    for p, o in [(r'(\d{4})年', 0), (r'令和(\d+)年', 2018), (r'平成(\d+)年', 1988)]:
        m = re.search(p, str(time_label))
        if m: return int(m.group(1)) + o
    return None

def sf(v):
    if v is None or str(v).strip() in ("", "-", "…", "x", "***"): return None
    try: return float(str(v).replace(",", ""))
    except: return None

print("=" * 60)
print("CREX マクロ指標 直接取得")
print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)

macro_rows = []

# ══════════════════════════════════════
# 1. 総人口（人口推計 テーブルID: 0000150002）
# ══════════════════════════════════════
print("\n■ 総人口")
try:
    rows = get_data("0000150002")
    pop = {}
    for r in rows:
        c1 = r.get("cat01_label", "")
        c2 = r.get("cat02_label", "")
        area = r.get("area_label", "")
        if "総数" in c1 and "総数" in c2 and area == "全国":
            y = parse_year(r.get("time", ""), r.get("time_label", ""))
            v = sf(r.get("value"))
            if y and v and v > 1000: pop[y] = v
    for y, v in sorted(pop.items()):
        macro_rows.append({"year":y,"indicator_id":"population","indicator_name":"総人口","value":v,"unit":"千人","source_id":"estat-population","notes":""})
    print(f"  ✅ {len(pop)}年分")
    time.sleep(1)
except Exception as e:
    print(f"  ❌ {e}")
    # フォールバック: 別のテーブルIDを試す
    try:
        rows = get_data("0003046410")
        pop = {}
        for r in rows:
            c1 = r.get("cat01_label", "")
            if "総人口" in c1 or "総数" in c1:
                y = parse_year(r.get("time", ""), r.get("time_label", ""))
                v = sf(r.get("value"))
                if y and v and v > 10000: pop[y] = v
        for y, v in sorted(pop.items()):
            macro_rows.append({"year":y,"indicator_id":"population","indicator_name":"総人口","value":v,"unit":"千人","source_id":"estat-population","notes":""})
        print(f"  ✅ フォールバック: {len(pop)}年分")
    except Exception as e2:
        print(f"  ❌ フォールバックも失敗: {e2}")

# ══════════════════════════════════════
# 2. CPI（消費者物価指数）
# ══════════════════════════════════════
print("\n■ CPI")
try:
    # 消費者物価指数の年平均・全国・総合
    rows = get_data("0003427113", limit=50000)
    if not rows:
        rows = get_data("0003143513", limit=50000)
    
    cpi = {}
    for r in rows:
        labels = " ".join(str(v) for v in r.values())
        # 総合指数・全国を探す
        if "総合" not in labels: continue
        area = r.get("area_label", "")
        if area and "全国" not in area: continue
        
        tl = str(r.get("time_label", ""))
        if "月" in tl and "年平均" not in tl: continue
        
        y = parse_year(r.get("time", ""), tl)
        v = sf(r.get("value"))
        if y and v and 50 < v < 200 and y not in cpi:
            cpi[y] = v
    
    for y, v in sorted(cpi.items()):
        macro_rows.append({"year":y,"indicator_id":"cpi","indicator_name":"消費者物価指数","value":v,"unit":"指数","source_id":"estat-cpi","notes":""})
    print(f"  ✅ {len(cpi)}年分")
    time.sleep(1)
except Exception as e:
    print(f"  ❌ {e}")

# ══════════════════════════════════════
# 3. 完全失業率
# ══════════════════════════════════════
print("\n■ 完全失業率")
try:
    rows = get_data("0003005865", limit=5000)
    unemp = {}
    for r in rows:
        labels = " ".join(str(v) for v in r.values())
        if "完全失業率" not in labels: continue
        
        tl = str(r.get("time_label", ""))
        # 年平均のみ
        if "月" in tl and "年平均" not in tl: continue
        
        y = parse_year(r.get("time", ""), tl)
        v = sf(r.get("value"))
        if y and v and 0.5 < v < 15 and y not in unemp:
            unemp[y] = v
    
    for y, v in sorted(unemp.items()):
        macro_rows.append({"year":y,"indicator_id":"unemployment","indicator_name":"完全失業率","value":v,"unit":"%","source_id":"estat-labor","notes":""})
    print(f"  ✅ {len(unemp)}年分")
    time.sleep(1)
except Exception as e:
    print(f"  ❌ {e}")

# ══════════════════════════════════════
# 4. 住宅着工戸数
# ══════════════════════════════════════
print("\n■ 住宅着工戸数")
try:
    rows = get_data("0004023580", limit=5000)
    hs = {}
    for r in rows:
        area = r.get("area_label", "")
        if area and "全国" not in area: continue
        y = parse_year(r.get("time", ""), r.get("time_label", ""))
        v = sf(r.get("value"))
        if y and v and v > 100:
            hs[y] = hs.get(y, 0) + v
    
    for y, v in sorted(hs.items()):
        macro_rows.append({"year":y,"indicator_id":"housing-starts","indicator_name":"住宅着工戸数","value":v,"unit":"戸","source_id":"estat-housing","notes":""})
    print(f"  ✅ {len(hs)}年分")
    time.sleep(1)
except Exception as e:
    print(f"  ❌ {e}")

# ══════════════════════════════════════
# 5. 小売販売額
# ══════════════════════════════════════
print("\n■ 小売販売額")
try:
    rows = get_data("0003147742", limit=1000)
    retail = {}
    for r in rows:
        tl = str(r.get("time_label", ""))
        if "月" in tl or "～" in tl or "〜" in tl: continue
        y = parse_year(r.get("time", ""), tl)
        v = sf(r.get("value"))
        if y and v and y not in retail:
            retail[y] = v
    
    for y, v in sorted(retail.items()):
        macro_rows.append({"year":y,"indicator_id":"retail-sales","indicator_name":"小売販売額","value":v,"unit":"10億円","source_id":"estat-retail","notes":""})
    print(f"  ✅ {len(retail)}年分")
    time.sleep(1)
except Exception as e:
    print(f"  ❌ {e}")

# ══════════════════════════════════════
# 6. 賃金指数（既存のavg-wage.jsonから）
# ══════════════════════════════════════
print("\n■ 賃金指数")
try:
    wage_json = "data/processed/avg-wage.json"
    if os.path.exists(wage_json):
        with open(wage_json, encoding="utf-8") as f:
            data = json.load(f)
        for d in data.get("data", []):
            macro_rows.append({"year":d["year"],"indicator_id":"avg-wage","indicator_name":"賃金指数","value":d["value"],"unit":"指数","source_id":"estat-wage","notes":""})
        print(f"  ✅ {len(data.get('data',[]))}年分（JSONから）")
    else:
        # APIから直接取得
        rows = get_data("0003411563", limit=50000)
        wages = {}
        for r in rows:
            area = r.get("area_label", "")
            if area and "全国" not in area: continue
            y = parse_year(r.get("time", ""), r.get("time_label", ""))
            v = sf(r.get("value"))
            if y and v and y not in wages:
                wages[y] = v
        for y, v in sorted(wages.items()):
            macro_rows.append({"year":y,"indicator_id":"avg-wage","indicator_name":"賃金指数","value":v,"unit":"指数","source_id":"estat-wage","notes":""})
        print(f"  ✅ {len(wages)}年分（APIから）")
except Exception as e:
    print(f"  ❌ {e}")

# ══════════════════════════════════════
# 7. 鉱工業生産指数
# ══════════════════════════════════════
print("\n■ 鉱工業生産指数")
try:
    rows = get_data("0003272944", limit=30000)
    iip = {}
    for r in rows:
        labels = " ".join(str(v) for v in r.values())
        if "鉱工業" not in labels and "製造工業" not in labels: continue
        
        tl = str(r.get("time_label", ""))
        if "月" in tl: continue  # 年次のみ
        
        y = parse_year(r.get("time", ""), tl)
        v = sf(r.get("value"))
        if y and v and 30 < v < 200 and y not in iip:
            iip[y] = v
    
    for y, v in sorted(iip.items()):
        macro_rows.append({"year":y,"indicator_id":"iip","indicator_name":"鉱工業生産指数","value":v,"unit":"指数","source_id":"estat-iip","notes":""})
    print(f"  ✅ {len(iip)}年分")
except Exception as e:
    print(f"  ❌ {e}")

# ══════════════════════════════════════
# 8. 県内GDP（ボーナス: テーブル0000010103から）
# ══════════════════════════════════════
print("\n■ 県内GDP（都道府県別）")
try:
    # 既にraw CSVにあるので読み込み
    gdp_file = os.path.join("data/raw/macro", "0000010103.csv")
    if os.path.exists(gdp_file):
        with open(gdp_file, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            gdp_rows = list(reader)
        
        gdp = {}
        for r in gdp_rows:
            c1 = r.get("cat01_label", "")
            area = r.get("area_label", "")
            if "県内総生産額" in c1 and "全国" in area:
                y = parse_year(r.get("time", ""), r.get("time_label", ""))
                v = sf(r.get("value"))
                if y and v and y not in gdp:
                    gdp[y] = v
        
        for y, v in sorted(gdp.items()):
            macro_rows.append({"year":y,"indicator_id":"gdp-prefectural","indicator_name":"県内総生産額（全国）","value":v,"unit":"百万円","source_id":"estat-gdp","notes":"県民経済計算"})
        print(f"  ✅ {len(gdp)}年分")
    else:
        print(f"  ⚠️ ファイルなし")
except Exception as e:
    print(f"  ❌ {e}")

# ══════════════════════════════════════
# ソート & 保存
# ══════════════════════════════════════
macro_rows.sort(key=lambda x: (x["indicator_id"], x["year"]))
headers = ["year", "indicator_id", "indicator_name", "value", "unit", "source_id", "notes"]

filepath = os.path.join(OUT, "B_macro_annual.csv")
with open(filepath, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=headers)
    w.writeheader()
    for r in macro_rows:
        w.writerow({h: r.get(h, "") for h in headers})

# 指標別の集計
from collections import Counter
counts = Counter(r["indicator_id"] for r in macro_rows)

print(f"\n{'=' * 60}")
print(f"完了: {len(macro_rows)}行 → {filepath}")
for ind, cnt in sorted(counts.items()):
    print(f"  {ind}: {cnt}年分")
print(f"{'=' * 60}")
