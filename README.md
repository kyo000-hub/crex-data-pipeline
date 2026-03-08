# CREX Data Pipeline

CREX経済データプラットフォームのデータ取得パイプライン。
GitHub Actionsで定期実行し、e-Stat等の公的統計APIからデータを自動取得・蓄積する。

## ディレクトリ構成

```
crex-data-pipeline/
├── .github/workflows/
│   └── fetch-estat.yml      # GitHub Actions ワークフロー
├── scripts/
│   ├── phase0_test.py       # API疎通テスト
│   ├── phase1_manufacturing.py  # 生産動態統計（製造業品目別）
│   └── phase2_macro.py      # マクロ統計（GDP/CPI/失業率等）
├── data/
│   ├── raw/                 # 取得した生データ（CSV/JSON）
│   │   ├── manufacturing/   # Phase 1: 生産動態統計
│   │   └── macro/           # Phase 2: マクロ統計
│   └── processed/           # 加工済みデータ（将来）
├── mappings/                # 品目コード→CREX業界マッピング（将来）
└── README.md
```

## 使い方

### 手動実行
1. GitHubリポジトリの「Actions」タブ
2. 「CREX e-Stat データ取得」ワークフロー選択
3. 「Run workflow」→ フェーズを選択して実行

### 自動実行
- 毎月1日と15日の午前3時（JST）に全フェーズが自動実行

## フェーズ

| フェーズ | 内容 | 取得指標数 |
|---------|------|-----------|
| Phase 0 | API疎通テスト | — |
| Phase 1 | 生産動態統計（製造業品目別出荷額・生産量） | 3 |
| Phase 2 | マクロ統計（CPI/失業率/賃金/人口/住宅着工等） | 8 |

## セットアップ

1. リポジトリのSettings → Secrets → Actions に `ESTAT_API_KEY` を登録
2. Actionsタブで手動実行してテスト
