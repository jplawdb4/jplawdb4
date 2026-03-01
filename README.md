# jplawdb4

Japanese Law Database for AI Agents — 日本法令データベース（AIエージェント向け）

## Overview

AI エージェント（Claude Code 等）が日本の税法・法令を高速に検索・参照するためのローカルデータベースシステム。

## Status

🚧 Under construction

## フォルダ構成

| フォルダ | 内容 | quickstart |
|---------|------|-----------|
| `law/` | 条文（法令）24法令 | `law/quickstart.txt` |
| `hanketsu/` | 判決990件・裁決255件 | `hanketsu/quickstart.txt` |
| `tsutatsu/` | 通達 | `tsutatsu/quickstart.txt` |
| `paper/` | 論文（OECD-BEPS等） | `paper/quickstart.txt` |
| `guide/` | NTA手引き（申告書記載要領等） | `guide/quickstart.txt` |
| `beppyo/` | 別表 | `beppyo/quickstart.txt` |
| `accounting/` | 会計 | `accounting/quickstart.txt` |
| `qa/` | Q&A | `qa/quickstart.txt` |
| `treaty/` | 租税条約 | `treaty/quickstart.txt` |
| `その他/` | DR成果HTML（分析レポート等） | `その他/quickstart.txt` |

## DR成果HTMLの保存ルール

ClaudeCodeが生成するディープリサーチ成果物HTMLは `その他/html/` に保存する。

```
その他/
├── quickstart.txt   # AI向けガイド
├── index.json       # 機械可読インデックス（全HTML一覧+メタ）
├── index.html       # ブラウザ用ナビ
└── html/            # HTML本体
    └── {topic}_{YYYYMMDD}.html
```
