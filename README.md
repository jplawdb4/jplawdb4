# jplawdb4

Japanese Law Database for AI Agents — 日本法令データベース（AIエージェント向け）

## Overview

AI エージェント（Claude Code 等）が日本の税法・法令を高速に検索・参照するためのローカルデータベースシステム。

## Status

🚧 Under construction

## フォルダ構成

| フォルダ | 内容 | quickstart |
|---------|------|-----------|
| `law/` | 条文（法令）27法令 | `law/quickstart.txt` |
| `hanketsu/` | 判決990件・裁決255件 | `hanketsu/quickstart.txt` |
| `tsutatsu/` | 通達 | `tsutatsu/quickstart.txt` |
| `paper/` | 論文（OECD-BEPS等） | `paper/quickstart.txt` |
| `guide/` | NTA手引き（申告書記載要領等） | `guide/quickstart.txt` |
| `beppyo/` | 別表 | `beppyo/quickstart.txt` |
| `accounting/` | 会計 | `accounting/quickstart.txt` |
| `qa/` | Q&A | `qa/quickstart.txt` |
| `treaty/` | 租税条約 | `treaty/quickstart.txt` |

## DR成果HTMLの扱い

ClaudeCode が生成するディープリサーチ成果物 HTML は、現在は公開リポジトリの外で private に管理する。
