#!/usr/bin/env python3
"""
CREX 汎用e-Stat取得スクリプト
設定ファイル（SOURCES dict）に全統計の検索条件を定義し、
ソースIDを指定して任意の統計を取得する。

Usage:
  python scripts/fetch_all.py                    # 全ソース取得
  python scripts/fetch_all.py estat-census       # 特定ソースのみ
  python scripts/fetch_all.py estat-census estat-trade  # 複数指定
"""
import os, sys, json, csv, time, requests, re
from datetime import datetime

API_KEY = os.environ.get("ESTAT_API_KEY", "")
BASE = "https://api.e-stat.go.jp/rest/3.0/app/json"
RAW_DIR = "data/raw"
LOG_FILE = "data/raw/_fetch_log.json"

# ════════════════════════════════════════════════════════════
# 全統計ソース定義
# ════════════════════════════════════════════════════════════
SOURCES = {
    # ── 取得済み（修正版） ──
    "estat-population": {
        "name": "人口推計",
        "search": ["人口推計 年齢 男女別 総人口"],
        "stat_id": "00200524",
        "dir": "macro",
        "limit": 100000,
        "timeout": 60,
    },
    "estat-cpi": {
        "name": "消費者物価指数",
        "search": ["消費者物価指数 全国 年平均 総合"],
        "stat_id": "00200571",
        "dir": "macro",
        "limit": 50000,
        "timeout": 120,
    },
    "estat-wage": {
        "name": "毎月勤労統計",
        "search": ["毎月勤労統計 全国 月間現金給与総額"],
        "stat_id": "00450011",
        "dir": "macro",
        "limit": 100000,
        "timeout": 60,
    },
    "estat-housing": {
        "name": "住宅着工統計",
        "search": ["住宅着工統計 新設住宅 利用関係別"],
        "stat_id": "00600120",
        "dir": "macro",
        "limit": 100000,
        "timeout": 60,
    },
    "estat-retail": {
        "name": "商業動態統計",
        "search": ["商業動態統計 小売業 販売額"],
        "stat_id": "00550060",
        "dir": "macro",
        "limit": 100000,
        "timeout": 60,
    },
    # ── Phase 2 修正 ──
    "estat-labor": {
        "name": "労働力調査（失業率）",
        "search": ["労働力調査 基本集計 全国 完全失業率", "労働力調査 完全失業率 年平均"],
        "stat_id": "00200531",
        "dir": "macro",
        "limit": 50000,
        "timeout": 60,
    },
    "estat-iip": {
        "name": "鉱工業指数",
        "search": ["鉱工業指数 生産指数", "鉱工業 生産 出荷 在庫"],
        "stat_id": "00550040",
        "dir": "macro",
        "limit": 50000,
        "timeout": 60,
    },
    "estat-household": {
        "name": "家計調査",
        "search": ["家計調査 二人以上の世帯 消費支出 年"],
        "stat_id": "00200573",
        "dir": "macro",
        "limit": 30000,
        "timeout": 120,
    },
    # ── 経済センサス ──
    "estat-census-activity": {
        "name": "経済センサス-活動調査",
        "search": ["経済センサス 活動調査 産業別 事業所数 従業者数"],
        "stat_id": "00200553",
        "dir": "census",
        "limit": 100000,
        "timeout": 120,
    },
    "estat-census-basic": {
        "name": "経済センサス-基礎調査",
        "search": ["経済センサス 基礎調査 産業別 事業所数"],
        "stat_id": "00200552",
        "dir": "census",
        "limit": 100000,
        "timeout": 120,
    },
    # ── 貿易統計 ──
    "estat-trade": {
        "name": "貿易統計",
        "search": ["貿易統計 概況品別 輸出 輸入 年", "貿易統計 品別国別 年"],
        "stat_id": "00350300",
        "dir": "trade",
        "limit": 100000,
        "timeout": 120,
    },
    # ── 法人企業統計 ──
    "estat-corp-finance": {
        "name": "法人企業統計",
        "search": ["法人企業統計 産業別 売上高 営業利益", "法人企業統計 年次別調査"],
        "stat_id": "00550010",
        "dir": "corporate",
        "limit": 100000,
        "timeout": 120,
    },
    # ── 製造業 ──
    "estat-manufacturing": {
        "name": "生産動態統計",
        "search": [
            "生産動態統計 年報 化学工業", "生産動態統計 年報 鉄鋼",
            "生産動態統計 年報 電子部品", "生産動態統計 年報 機械",
            "生産動態統計 年報 繊維工業", "生産動態統計 年報 紙パルプ",
            "生産動態統計 年報 窯業", "生産動態統計 年報 非鉄金属",
            "生産動態統計 年報 ゴム製品", "生産動態統計 年報 プラスチック",
            "生産動態統計 年報 食料品", "生産動態統計 年報 石油",
        ],
        "stat_id": "00550020",
        "dir": "manufacturing",
        "limit": 100000,
        "timeout": 120,
        "max_tables": 30,
    },
    # ── 賃金構造 ──
    "estat-wage-structure": {
        "name": "賃金構造基本統計",
        "search": ["賃金構造基本統計 産業別 きまって支給する現金給与額", "賃金構造基本統計 産業 年齢"],
        "stat_id": "00450091",
        "dir": "wage-structure",
        "limit": 100000,
        "timeout": 120,
    },
    # ── 雇用 ──
    "estat-employment-trend": {
        "name": "雇用動向調査",
        "search": ["雇用動向調査 産業別 入職率 離職率"],
        "stat_id": "00450012",
        "dir": "employment",
        "limit": 50000,
        "timeout": 60,
    },
    "estat-employment-structure": {
        "name": "就業構造基本調査",
        "search": ["就業構造基本調査 産業別 就業者"],
        "stat_id": "00200532",
        "dir": "employment",
        "limit": 100000,
        "timeout": 120,
    },
    # ── 国勢調査 ──
    "estat-national-census": {
        "name": "国勢調査",
        "search": ["国勢調査 産業別 就業者数", "国勢調査 産業 職業"],
        "stat_id": "00200521",
        "dir": "national-census",
        "limit": 100000,
        "timeout": 120,
    },
    # ── サービス業 ──
    "estat-service": {
        "name": "サービス産業動向調査",
        "search": ["サービス産業動向調査 売上高 事業従事者"],
        "stat_id": "00200544",
        "dir": "service",
        "limit": 50000,
        "timeout": 60,
    },
    "estat-ict": {
        "name": "情報通信業基本調査",
        "search": ["情報通信業基本調査 売上高 従業者"],
        "stat_id": "00200549",
        "dir": "ict",
        "limit": 50000,
        "timeout": 60,
    },
    # ── 科学技術 ──
    "estat-rd": {
        "name": "科学技術研究調査",
        "search": ["科学技術研究調査 産業別 研究費 研究者数"],
        "stat_id": "00200550",
        "dir": "rd",
        "limit": 50000,
        "timeout": 60,
    },
    # ── 医療・福祉 ──
    "estat-medical": {
        "name": "医療施設調査",
        "search": ["医療施設調査 病院 診療所 施設数"],
        "stat_id": "00450022",
        "dir": "medical",
        "limit": 50000,
        "timeout": 60,
    },
    "estat-patient": {
        "name": "患者調査",
        "search": ["患者調査 傷病分類別 患者数"],
        "stat_id": "00450023",
        "dir": "medical",
        "limit": 50000,
        "timeout": 60,
    },
    "estat-care": {
        "name": "介護サービス施設調査",
        "search": ["介護サービス施設 事業所数"],
        "stat_id": "00450028",
        "dir": "medical",
        "limit": 50000,
        "timeout": 60,
    },
    "estat-pharma": {
        "name": "薬事工業生産動態統計",
        "search": ["薬事工業生産動態統計 医薬品 生産額"],
        "stat_id": "00450100",
        "dir": "medical",
        "limit": 50000,
        "timeout": 60,
    },
    "estat-medical-cost": {
        "name": "社会医療診療行為別統計",
        "search": ["社会医療診療行為別統計 診療行為"],
        "stat_id": "00450398",
        "dir": "medical",
        "limit": 50000,
        "timeout": 60,
    },
    # ── 農林水産 ──
    "estat-agri-census": {
        "name": "農林業センサス",
        "search": ["農林業センサス 経営体数 農家数"],
        "stat_id": "00500209",
        "dir": "agriculture",
        "limit": 100000,
        "timeout": 120,
    },
    "estat-fishery-census": {
        "name": "漁業センサス",
        "search": ["漁業センサス 経営体"],
        "stat_id": "00500210",
        "dir": "agriculture",
        "limit": 50000,
        "timeout": 60,
    },
    "estat-crop": {
        "name": "作物統計",
        "search": ["作物統計 作付面積 収穫量"],
        "stat_id": "00500215",
        "dir": "agriculture",
        "limit": 50000,
        "timeout": 60,
    },
    # ── 運輸・観光 ──
    "estat-transport-auto": {
        "name": "自動車輸送統計",
        "search": ["自動車輸送統計 貨物 旅客"],
        "stat_id": "00600020",
        "dir": "transport",
        "limit": 50000,
        "timeout": 60,
    },
    "estat-port": {
        "name": "港湾統計",
        "search": ["港湾統計 入港船舶 貨物量"],
        "stat_id": "00600050",
        "dir": "transport",
        "limit": 50000,
        "timeout": 60,
    },
    "estat-railway": {
        "name": "鉄道輸送統計",
        "search": ["鉄道輸送統計 旅客 貨物"],
        "stat_id": "00600070",
        "dir": "transport",
        "limit": 50000,
        "timeout": 60,
    },
    "estat-hotel": {
        "name": "宿泊旅行統計",
        "search": ["宿泊旅行統計 宿泊者数 都道府県別"],
        "stat_id": "00600150",
        "dir": "transport",
        "limit": 50000,
        "timeout": 60,
    },
    # ── 建設 ──
    "estat-construction-order": {
        "name": "建設工事受注動態統計",
        "search": ["建設工事受注動態統計 受注高"],
        "stat_id": "00600330",
        "dir": "construction",
        "limit": 50000,
        "timeout": 60,
    },
    "estat-building-starts": {
        "name": "建築着工統計",
        "search": ["建築着工統計 着工建築物 床面積"],
        "stat_id": "00600340",
        "dir": "construction",
        "limit": 50000,
        "timeout": 60,
    },
    # ── エネルギー ──
    "estat-energy": {
        "name": "資源エネルギー統計",
        "search": ["資源エネルギー統計 生産 消費"],
        "stat_id": "00550150",
        "dir": "energy",
        "limit": 50000,
        "timeout": 60,
    },
    "estat-oil": {
        "name": "石油製品需給動態統計",
        "search": ["石油製品需給動態統計 販売量"],
        "stat_id": "00550170",
        "dir": "energy",
        "limit": 50000,
        "timeout": 60,
    },
    # ── 教育 ──
    "estat-school": {
        "name": "学校基本調査",
        "search": ["学校基本調査 学校数 生徒数 教員数"],
        "stat_id": "00400001",
        "dir": "education",
        "limit": 50000,
        "timeout": 60,
    },
    # ── 企業 ──
    "estat-econ-structure": {
        "name": "経済構造実態調査",
        "search": ["経済構造実態調査 産業別 売上高"],
        "stat_id": None,
        "dir": "corporate",
        "limit": 50000,
        "timeout": 60,
    },
    "estat-corp-activity": {
        "name": "企業活動基本調査",
        "search": ["企業活動基本調査 売上高 研究開発"],
        "stat_id": None,
        "dir": "corporate",
        "limit": 50000,
        "timeout": 60,
    },
    "estat-sme": {
        "name": "中小企業実態基本調査",
        "search": ["中小企業実態基本調査 売上高"],
        "stat_id": None,
        "dir": "corporate",
        "limit": 50000,
        "timeout": 60,
    },
    "estat-overseas": {
        "name": "海外事業活動基本調査",
        "search": ["海外事業活動基本調査 現地法人 売上高"],
        "stat_id": None,
        "dir": "corporate",
        "limit": 50000,
        "timeout": 60,
    },
    # ── 家計・物価 ──
    "estat-household-structure": {
        "name": "全国家計構造調査",
        "search": ["全国家計構造調査 所得 消費"],
        "stat_id": "00200564",
        "dir": "macro",
        "limit": 50000,
        "timeout": 60,
    },
    "estat-individual-biz": {
        "name": "個人企業経済調査",
        "search": ["個人企業経済調査 売上高"],
        "stat_id": "00200565",
        "dir": "corporate",
        "limit": 30000,
        "timeout": 60,
    },
    "estat-retail-price": {
        "name": "小売物価統計調査",
        "search": ["小売物価統計調査 主要品目 小売価格"],
        "stat_id": "00200572",
        "dir": "macro",
        "limit": 50000,
        "timeout": 60,
    },
    # ── 通信 ──
    "estat-ict-usage": {
        "name": "通信利用動向調査",
        "search": ["通信利用動向調査 インターネット利用率"],
        "stat_id": None,
        "dir": "ict",
        "limit": 30000,
        "timeout": 60,
    },
    # ── 法人景気 ──
    "estat-bsi": {
        "name": "法人企業景気予測調査",
        "search": ["法人企業景気予測調査 業況判断"],
        "stat_id": "00550200",
        "dir": "corporate",
        "limit": 30000,
        "timeout": 60,
    },
    # ── 環境・安全 ──
    "estat-environment": {
        "name": "環境統計",
        "search": ["環境統計 廃棄物 CO2 排出量"],
        "stat_id": None,
        "dir": "environment",
        "limit": 30000,
        "timeout": 60,
    },
    "estat-crime": {
        "name": "犯罪統計",
        "search": ["犯罪統計 刑法犯 認知件数"],
        "stat_id": None,
        "dir": "safety",
        "limit": 30000,
        "timeout": 60,
    },
}

# ════════════════════════════════════════════════════════════
# API関数
# ════════════════════════════════════════════════════════════
def search_tables(keyword, limit=50):
    """e-Stat APIで統計表を検索"""
    params = {"appId": API_KEY, "searchWord": keyword, "limit": limit}
    r = requests.get(f"{BASE}/getStatsList", params=params, timeout=30)
    r.raise_for_status()
    data = r.json().get("GET_STATS_LIST", {}).get("DATALIST_INF", {})
    tables = data.get("TABLE_INF", [])
    return tables if isinstance(tables, list) else [tables] if tables else []

def search_by_stat_id(stat_id, limit=50):
    """統計IDで直接検索"""
    params = {"appId": API_KEY, "statsCode": stat_id, "limit": limit}
    r = requests.get(f"{BASE}/getStatsList", params=params, timeout=30)
    r.raise_for_status()
    data = r.json().get("GET_STATS_LIST", {}).get("DATALIST_INF", {})
    tables = data.get("TABLE_INF", [])
    return tables if isinstance(tables, list) else [tables] if tables else []

def get_stats_data(stats_id, limit=100000, timeout=60):
    """統計データを取得"""
    r = requests.get(f"{BASE}/getStatsData", params={
        "appId": API_KEY, "statsDataId": stats_id, "limit": limit
    }, timeout=timeout)
    r.raise_for_status()
    return r.json()

def get_title(t):
    title = t.get("TITLE", "")
    return title.get("$", "") if isinstance(title, dict) else str(title)

def extract_data(raw_json):
    """統計データからクラス情報と値を抽出"""
    stat = raw_json.get("GET_STATS_DATA", {}).get("STATISTICAL_DATA", {})
    
    # クラス情報
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
    
    # 値
    values = stat.get("DATA_INF", {}).get("VALUE", [])
    if not isinstance(values, list):
        values = [values] if values else []
    
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
    
    return rows

def save_csv(rows, filepath):
    """行データをCSVに保存"""
    if not rows:
        return 0
    fieldnames = list(rows[0].keys())
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)

# ════════════════════════════════════════════════════════════
# メイン取得ロジック
# ════════════════════════════════════════════════════════════
def fetch_source(source_id, config):
    """1つのソースを取得"""
    name = config["name"]
    out_dir = os.path.join(RAW_DIR, config["dir"])
    os.makedirs(out_dir, exist_ok=True)
    
    max_tables = config.get("max_tables", 5)
    limit = config.get("limit", 50000)
    timeout = config.get("timeout", 60)
    
    result = {
        "source_id": source_id,
        "name": name,
        "timestamp": datetime.now().isoformat(),
        "tables_found": 0,
        "tables_processed": 0,
        "tables_success": 0,
        "total_rows": 0,
        "files": [],
        "errors": [],
    }
    
    # Step 1: テーブル検索
    all_tables = []
    for query in config["search"]:
        try:
            tables = search_tables(query, limit=30)
            all_tables.extend(tables)
            time.sleep(0.5)
        except Exception as e:
            result["errors"].append(f"Search '{query}': {e}")
    
    # stat_idでも検索（検索でヒットしない場合のフォールバック）
    if not all_tables and config.get("stat_id"):
        try:
            all_tables = search_by_stat_id(config["stat_id"], limit=50)
        except Exception as e:
            result["errors"].append(f"StatID search: {e}")
    
    # 重複除去
    seen_ids = set()
    unique_tables = []
    for t in all_tables:
        tid = t.get("@id", "")
        if tid not in seen_ids:
            seen_ids.add(tid)
            unique_tables.append(t)
    
    result["tables_found"] = len(unique_tables)
    
    if not unique_tables:
        result["errors"].append("No tables found")
        return result
    
    # Step 2: データ取得
    for i, table in enumerate(unique_tables[:max_tables]):
        table_id = table.get("@id", "")
        title = get_title(table)
        result["tables_processed"] += 1
        
        try:
            raw = get_stats_data(table_id, limit=limit, timeout=timeout)
            rows = extract_data(raw)
            
            if not rows:
                continue
            
            filepath = os.path.join(out_dir, f"{table_id}.csv")
            saved = save_csv(rows, filepath)
            
            result["tables_success"] += 1
            result["total_rows"] += saved
            result["files"].append({
                "table_id": table_id,
                "title": title[:80],
                "rows": saved,
                "file": filepath,
            })
            
            time.sleep(1)  # API負荷軽減
            
        except requests.exceptions.Timeout:
            result["errors"].append(f"{table_id}: Timeout ({timeout}s)")
        except Exception as e:
            result["errors"].append(f"{table_id}: {str(e)[:100]}")
    
    return result

# ════════════════════════════════════════════════════════════
# エントリーポイント
# ════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 70)
    print("CREX 汎用e-Statデータ取得")
    print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"APIキー: {API_KEY[:8]}...")
    print("=" * 70)
    
    # コマンドライン引数でソースIDを指定可能
    if len(sys.argv) > 1:
        target_ids = sys.argv[1:]
    else:
        target_ids = list(SOURCES.keys())
    
    # 存在チェック
    for sid in target_ids:
        if sid not in SOURCES:
            print(f"❌ 不明なソースID: {sid}")
            print(f"   有効なID: {', '.join(sorted(SOURCES.keys()))}")
            sys.exit(1)
    
    print(f"\n対象: {len(target_ids)}ソース")
    print("-" * 70)
    
    all_results = []
    total_rows = 0
    total_success = 0
    total_errors = 0
    
    for idx, source_id in enumerate(target_ids, 1):
        config = SOURCES[source_id]
        print(f"\n[{idx}/{len(target_ids)}] {source_id}: {config['name']}")
        
        result = fetch_source(source_id, config)
        all_results.append(result)
        
        total_rows += result["total_rows"]
        total_success += result["tables_success"]
        
        if result["tables_success"] > 0:
            print(f"  ✅ {result['tables_success']}/{result['tables_processed']}テーブル成功, {result['total_rows']}行")
            for f in result["files"][:3]:
                print(f"     {f['table_id']}: {f['title'][:50]} ({f['rows']}行)")
            if len(result["files"]) > 3:
                print(f"     ...他{len(result['files'])-3}ファイル")
        else:
            print(f"  ⚠️ データなし (検索{result['tables_found']}件)")
        
        if result["errors"]:
            total_errors += len(result["errors"])
            for err in result["errors"][:2]:
                print(f"  ❌ {err[:80]}")
    
    # ログ保存
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    log = {
        "timestamp": datetime.now().isoformat(),
        "targets": len(target_ids),
        "total_success": total_success,
        "total_rows": total_rows,
        "total_errors": total_errors,
        "results": all_results,
    }
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        json.dump(log, f, ensure_ascii=False, indent=2)
    
    print(f"\n{'=' * 70}")
    print(f"完了: {total_success}テーブル成功, {total_rows}行取得, {total_errors}エラー")
    print(f"ログ: {LOG_FILE}")
    print(f"{'=' * 70}")
