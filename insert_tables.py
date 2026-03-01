#!/usr/bin/env python3
"""
insert_tables.py - 条文ファイルに e-Gov API の表データを挿入する

使い方:
  python3 insert_tables.py --dry-run          # 変更なし・確認のみ
  python3 insert_tables.py --lawdir sozei_tokubetsu  # 特定法令のみ
  python3 insert_tables.py                    # 全件実行
  python3 insert_tables.py --force            # 挿入済みも再処理
"""

import argparse
import json
import logging
import re
import subprocess
import time
import urllib.parse
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional

import requests
import tiktoken

# ─── 定数 ─────────────────────────────────────────────────────────────────
JPLAWDB4    = Path("/home/user/jplawdb4")
LAW_TEXT    = JPLAWDB4 / "law" / "text"
CACHE_DIR   = JPLAWDB4 / ".insert_tables_cache"
MAX_TOKENS  = 9999
RATE_LIMIT  = 1.5   # 秒
ENC         = tiktoken.get_encoding("cl100k_base")

TABLE_MARKER  = "<!--TABLE_INSERTED-->"
TABLE_REF_PAT = re.compile(r'次の表|左の表|下の表')
P_BLOCK_PAT   = re.compile(r'^\[p\d+[^\]]*\]', re.MULTILINE)
CHUNK_PAT     = re.compile(r'^--- split \d+/\d+ of (.+?)\.txt ---')

API_V2 = "https://laws.e-gov.go.jp/api/2"
API_V1 = "https://laws.e-gov.go.jp/api/1"

# 法令ディレクトリ → law_id（v2 API 用）
LAW_ID_MAP = {
    "sozei_tokubetsu":        "332AC0000000026",
    "sozokuzei":              "325AC0000000073",
    "shotokuzei":             "340AC0000000033",
    "hojinzei":               "340AC0000000034",
    "sozei_tokubetsu_seirei": "332CO0000000043",
    "hojinzei_seirei":        "340CO0000000097",
    "shotokuzei_seirei":      "340CO0000000096",
    "shohizei":               "363AC0000000108",
    "shohizei_seirei":        "363CO0000000360",
    "kokuzei_tsusoku":        "337AC0000000066",
    "kaishahou":              "417AC0000000086",
    "shouhou":                "232AC0000000048",
}

# 省令など law_id が不定 → law_num で v1 API
LAW_NUM_MAP = {
    "sozei_tokubetsu_kisoku": "昭和三十二年大蔵省令第十五号",
    "hojinzei_kisoku":        "昭和四十年大蔵省令第十二号",
    "shotokuzei_kisoku":      "昭和四十年大蔵省令第十一号",
    "shohizei_kisoku":        "昭和六十三年大蔵省令第五十三号",
    "kokuzei_tsusoku_kisoku": "昭和三十七年大蔵省令第二十八号",
    "sozokuzei_kisoku":       "昭和二十五年大蔵省令第十七号",
    "sozokuzei_seirei":       "昭和二十五年政令第七十一号",
    "kaishahou_kisoku":       "平成十八年法務省令第十二号",
    "kaishahou_seirei":       "平成十七年政令第三百六十四号",
    "shouhou_kisoku":         "平成十四年法務省令第二十二号",
}


# ─── Phase 1: スキャン ────────────────────────────────────────────────────

def parse_header(text: str) -> tuple[str, str, str]:
    """ファイルテキストから (article_id, egov_id, law_num) を抽出"""
    article_id = egov_id = law_num = ""
    for line in text.split("\n")[:10]:
        if m := re.search(r'article:\s*([\w\-]+)', line):
            article_id = m.group(1)
        if m := re.search(r'egov_id:\s*([A-Z0-9a-z]+)', line):
            egov_id = m.group(1)
        if m := re.search(r'law_num:\s*([^/\n]+)', line):
            law_num = m.group(1).strip()
    return article_id, egov_id, law_num


def scan_target_files(lawdir_filter: Optional[str] = None,
                      file_filter: Optional[str] = None) -> list[dict]:
    """「次の表」等を含む条文ファイルをスキャン"""
    results = []
    search_dirs = []

    if lawdir_filter:
        d = LAW_TEXT / lawdir_filter
        if d.exists():
            search_dirs = [d]
    else:
        search_dirs = [p for p in LAW_TEXT.iterdir() if p.is_dir()]

    for law_dir in sorted(search_dirs):
        lawdir = law_dir.name
        for fpath in sorted(law_dir.glob("*.txt")):
            if file_filter and fpath.name != file_filter:
                continue
            text = fpath.read_text(encoding="utf-8")
            if not TABLE_REF_PAT.search(text):
                continue

            already_done = TABLE_MARKER in text
            is_chunk = bool(CHUNK_PAT.match(text))

            if is_chunk:
                m = CHUNK_PAT.match(text)
                base_stem = m.group(1)
                base_path = fpath.parent / f"{base_stem}.txt"
                base_text = base_path.read_text(encoding="utf-8") if base_path.exists() else ""
                article_id, egov_id, law_num = parse_header(base_text)
            else:
                article_id, egov_id, law_num = parse_header(text)
                base_stem = fpath.stem

            results.append({
                "path":         fpath,
                "lawdir":       lawdir,
                "article_id":   article_id,
                "egov_id":      egov_id,
                "law_num":      law_num,
                "is_chunk":     is_chunk,
                "already_done": already_done,
            })

    return results


# ─── Phase 2: API 取得・キャッシュ ───────────────────────────────────────

class LawDataCache:
    def __init__(self):
        CACHE_DIR.mkdir(exist_ok=True)
        self._mem: dict[str, object] = {}
        self._last_req = 0.0

    def _wait(self):
        elapsed = time.time() - self._last_req
        if elapsed < RATE_LIMIT:
            time.sleep(RATE_LIMIT - elapsed)
        self._last_req = time.time()

    def get_json(self, law_id: str) -> Optional[dict]:
        """v2 API: JSON 全文ツリーを返す"""
        if law_id in self._mem:
            return self._mem[law_id]
        cache = CACHE_DIR / f"{law_id}.json"
        if cache.exists():
            data = json.loads(cache.read_text(encoding="utf-8"))
            self._mem[law_id] = data
            return data
        self._wait()
        try:
            r = requests.get(f"{API_V2}/law_data/{law_id}", timeout=30)
            r.raise_for_status()
            tree = r.json().get("law_full_text")
            if tree:
                cache.write_text(json.dumps(tree, ensure_ascii=False), encoding="utf-8")
                self._mem[law_id] = tree
                return tree
        except Exception as e:
            logging.warning(f"v2 API failed ({law_id}): {e}")
        return None

    def get_xml(self, law_num: str) -> Optional[str]:
        """v1 API: XML 全文テキストを返す"""
        key = "v1_" + urllib.parse.quote(law_num, safe="")
        cache = CACHE_DIR / f"{key}.xml"
        if cache.exists():
            return cache.read_text(encoding="utf-8")
        self._wait()
        try:
            url = f"{API_V1}/lawdata/{urllib.parse.quote(law_num)}"
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            xml_text = r.text
            cache.write_text(xml_text, encoding="utf-8")
            return xml_text
        except Exception as e:
            logging.warning(f"v1 API failed ({law_num}): {e}")
        return None


def resolve_source(lawdir: str, egov_id: str, law_num: str) -> tuple[str, str]:
    """
    Returns (id_or_num, mode)
      mode: "v2" → get_json(id), "v1" → get_xml(num), "unknown"
    """
    if egov_id:
        return egov_id, "v2"
    if lawdir in LAW_ID_MAP:
        return LAW_ID_MAP[lawdir], "v2"
    if lawdir in LAW_NUM_MAP:
        return LAW_NUM_MAP[lawdir], "v1"
    if law_num:
        return law_num, "v1"
    return "", "unknown"


# ─── Phase 3: テーブル抽出・変換 ─────────────────────────────────────────

def article_id_to_api_num(article_id: str) -> str:
    """'66-6' → '66_6'（e-Gov Article.Num 形式）"""
    # チャンクサフィックス (_2 等) は article_id に含まれない
    return article_id.replace("-", "_")


def _text_of(node: dict | list | str) -> str:
    """JSON ノードからテキストを再帰的に抽出"""
    if isinstance(node, str):
        return node
    if isinstance(node, dict):
        # Ruby タグ: ルビ読み（Rt）は除外
        parts = []
        for child in node.get("children", []):
            if isinstance(child, dict) and child.get("tag") == "Rt":
                continue
            parts.append(_text_of(child))
        return "".join(parts)
    if isinstance(node, list):
        return "".join(_text_of(c) for c in node)
    return ""


def _collect(node: dict | list | str, tag: str) -> list[dict]:
    """指定タグの全ノードを再帰収集"""
    out = []
    if isinstance(node, dict):
        if node.get("tag") == tag:
            out.append(node)
        else:
            for child in node.get("children", []):
                out.extend(_collect(child, tag))
    elif isinstance(node, list):
        for item in node:
            out.extend(_collect(item, tag))
    return out


def _find_article(node: dict | list | str, num: str) -> Optional[dict]:
    """JSON ツリーから Article.Num == num のノードを探す"""
    if isinstance(node, dict):
        if node.get("tag") == "Article" and node.get("attr", {}).get("Num") == num:
            return node
        for child in node.get("children", []):
            r = _find_article(child, num)
            if r:
                return r
    elif isinstance(node, list):
        for item in node:
            r = _find_article(item, num)
            if r:
                return r
    return None


def _ts_to_grid(ts_node: dict) -> list[list[str]]:
    """TableStruct (JSON) → 行列リスト（rowspan 展開済み）"""
    rows_raw = []
    for table in (ts_node.get("children") or []):
        if not isinstance(table, dict) or table.get("tag") != "Table":
            continue
        for row in (table.get("children") or []):
            if not isinstance(row, dict) or row.get("tag") != "TableRow":
                continue
            cells = []
            for col in (row.get("children") or []):
                if not isinstance(col, dict) or col.get("tag") != "TableColumn":
                    continue
                texts = []
                for sent in _collect(col, "Sentence"):
                    t = _text_of(sent).strip()
                    if t:
                        texts.append(t)
                cell = " ".join(texts)
                rowspan = int((col.get("attr") or {}).get("rowspan", 1))
                cells.append((cell, rowspan))
            if cells:
                rows_raw.append(cells)
    return _expand_rowspan(rows_raw)


def _expand_rowspan(rows_raw: list[list[tuple[str, int]]]) -> list[list[str]]:
    """rowspan を展開してフラットなグリッドを生成"""
    if not rows_raw:
        return []
    num_cols = max(len(r) for r in rows_raw)
    grid = []
    pending: dict[int, tuple[int, str]] = {}  # col_idx → (残り行数, セルテキスト)

    for raw_row in rows_raw:
        row_out = []
        raw_idx = 0
        for col_idx in range(num_cols):
            if col_idx in pending:
                remain, text = pending[col_idx]
                row_out.append(text)
                if remain - 1 > 0:
                    pending[col_idx] = (remain - 1, text)
                else:
                    del pending[col_idx]
            elif raw_idx < len(raw_row):
                cell_text, rowspan = raw_row[raw_idx]
                row_out.append(cell_text)
                if rowspan > 1:
                    pending[col_idx] = (rowspan - 1, cell_text)
                raw_idx += 1
            else:
                row_out.append("")
        grid.append(row_out)
    return grid


def grid_to_markdown(grid: list[list[str]]) -> str:
    """行列リスト → Markdown テーブル文字列"""
    if not grid:
        return ""
    # セル内の | と改行を置換（Markdown テーブル制約）
    cleaned = [
        [cell.replace("|", "｜").replace("\n", " ") for cell in row]
        for row in grid
    ]
    num_cols = max(len(r) for r in cleaned)
    col_w = [0] * num_cols
    for row in cleaned:
        for i, cell in enumerate(row):
            if i < num_cols:
                col_w[i] = max(col_w[i], len(cell))
    col_w = [max(w, 3) for w in col_w]  # 最低幅 3

    lines = []
    for ri, row in enumerate(cleaned):
        cells = []
        for i in range(num_cols):
            cell = row[i] if i < len(row) else ""
            cells.append(cell.ljust(col_w[i]))
        lines.append("| " + " | ".join(cells) + " |")
        if ri == 0:
            lines.append("| " + " | ".join("-" * w for w in col_w) + " |")
    return "\n" + "\n".join(lines) + "\n"


def extract_tables_json(law_tree: dict, article_id: str) -> list[list[list[str]]]:
    """JSON ツリーから指定条のテーブルグリッドリストを返す"""
    api_num = article_id_to_api_num(article_id)
    article_node = _find_article(law_tree, api_num)
    if not article_node:
        logging.debug(f"  Article {api_num} not found in JSON tree")
        return []
    return [_ts_to_grid(ts) for ts in _collect(article_node, "TableStruct")]


def extract_tables_xml(xml_text: str, article_id: str) -> list[list[list[str]]]:
    """v1 XML テキストから指定条のテーブルグリッドリストを返す"""
    api_num = article_id_to_api_num(article_id)
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        logging.warning(f"  XML parse error: {e}")
        return []
    article_el = root.find(f".//Article[@Num='{api_num}']")
    if article_el is None:
        logging.debug(f"  Article {api_num} not found in XML")
        return []
    grids = []
    for ts_el in article_el.iter("TableStruct"):
        grid = _ts_xml_to_grid(ts_el)
        if grid:
            grids.append(grid)
    return grids


def _ts_xml_to_grid(ts_el) -> list[list[str]]:
    """ElementTree の TableStruct → 行列リスト"""
    rows_raw = []
    table_el = ts_el.find("Table")
    if table_el is None:
        return []
    for row_el in table_el.findall("TableRow"):
        cells = []
        for col_el in row_el.findall("TableColumn"):
            texts = []
            for sent_el in col_el.iter("Sentence"):
                t = ET.tostring(sent_el, method="text", encoding="unicode").strip()
                if t:
                    texts.append(t)
            cell = " ".join(texts)
            rowspan = int(col_el.get("rowspan", 1))
            cells.append((cell, rowspan))
        if cells:
            rows_raw.append(cells)
    return _expand_rowspan(rows_raw)


# ─── Phase 4: 挿入 ───────────────────────────────────────────────────────

def find_insertion_points(text: str) -> list[int]:
    """
    「次の表」参照のある [pN] ブロックの末尾位置（改行直前）を返す。
    複数テーブル参照に対応。
    """
    block_starts = [(m.start(), m.end()) for m in P_BLOCK_PAT.finditer(text)]
    points = []
    for i, (start, _) in enumerate(block_starts):
        end = block_starts[i + 1][0] if i + 1 < len(block_starts) else len(text)
        block_text = text[start:end]
        if TABLE_REF_PAT.search(block_text):
            # ブロック末尾の空白を除いた位置
            tail = block_text.rstrip()
            points.append(start + len(tail))
    return points


def insert_tables_to_file(
    fpath: Path,
    grids: list[list[list[str]]],
    dry_run: bool,
) -> str:
    """
    ファイルにテーブルを挿入。冪等性あり。
    Returns: "inserted" | "already_done" | "no_tables" | "no_points"
    """
    text = fpath.read_text(encoding="utf-8")

    if TABLE_MARKER in text:
        return "already_done"

    if not grids:
        return "no_tables"

    points = find_insertion_points(text)
    if not points:
        return "no_points"

    n = min(len(points), len(grids))
    new_text = text

    # 後ろから挿入（位置ずれを防ぐ）
    for i in range(n - 1, -1, -1):
        pos = points[i]
        md = grid_to_markdown(grids[i])
        new_text = new_text[:pos] + md + new_text[pos:]

    # 冪等マーカーをヘッダー末尾（最初の空行後）に追加
    lines = new_text.split("\n")
    for j, line in enumerate(lines):
        if line.strip() == "" and j > 0:
            lines.insert(j + 1, TABLE_MARKER)
            break
    new_text = "\n".join(lines)

    if not dry_run:
        fpath.write_text(new_text, encoding="utf-8")

    return "inserted"


# ─── Phase 5: トークン超過対応 ────────────────────────────────────────────

def handle_overflow(fpath: Path) -> bool:
    """9,999 トークン超なら split_oversized.py を呼び出す"""
    tokens = len(ENC.encode(fpath.read_text(encoding="utf-8")))
    if tokens <= MAX_TOKENS:
        return False
    logging.info(f"  overflow: {fpath.name} ({tokens:,} tok) → splitting")
    result = subprocess.run(
        ["python3", str(JPLAWDB4 / "split_oversized.py"), "--db", "law"],
        capture_output=True, text=True, cwd=str(JPLAWDB4)
    )
    if result.returncode != 0:
        logging.error(f"  split_oversized.py failed: {result.stderr[:300]}")
    return True


# ─── メイン ──────────────────────────────────────────────────────────────

def process_file(
    entry: dict,
    cache: LawDataCache,
    dry_run: bool,
    errors: list,
) -> str:
    fpath     = entry["path"]
    lawdir    = entry["lawdir"]
    article_id = entry["article_id"]

    if not article_id:
        errors.append(f"{fpath.name}: article_id 不明")
        return "error_no_article"

    src, mode = resolve_source(lawdir, entry["egov_id"], entry["law_num"])
    if mode == "unknown":
        errors.append(f"{fpath.name}: law_id/law_num 不明")
        return "error_no_source"

    # 法令データ取得
    if mode == "v2":
        law_data = cache.get_json(src)
        if not law_data:
            errors.append(f"{fpath.name}: v2 API 失敗 ({src})")
            return "error_api"
        grids = extract_tables_json(law_data, article_id)
    else:
        xml_text = cache.get_xml(src)
        if not xml_text:
            errors.append(f"{fpath.name}: v1 API 失敗 ({src})")
            return "error_api"
        grids = extract_tables_xml(xml_text, article_id)

    if not grids:
        errors.append(f"{fpath.name}: テーブルなし (article={article_id})")
        return "error_no_tables"

    status = insert_tables_to_file(fpath, grids, dry_run)
    logging.info(f"  {fpath.name}: {status} ({len(grids)} table(s))")

    if status == "inserted" and not dry_run:
        handle_overflow(fpath)

    return status


def main():
    parser = argparse.ArgumentParser(description="insert_tables.py")
    parser.add_argument("--dry-run",  action="store_true", help="変更なし・確認のみ")
    parser.add_argument("--lawdir",   help="特定の法令ディレクトリ名")
    parser.add_argument("--file",     help="特定ファイル名")
    parser.add_argument("--force",    action="store_true", help="挿入済みも再処理")
    parser.add_argument("--log-file", default="insert_tables.log")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(args.log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )

    # Phase 1
    logging.info("=== Phase 1: スキャン ===")
    targets = scan_target_files(args.lawdir, args.file)
    logging.info(f"対象: {len(targets)} ファイル")

    if args.force:
        # マーカーを削除してから再処理
        for entry in targets:
            text = entry["path"].read_text(encoding="utf-8")
            if TABLE_MARKER in text:
                entry["path"].write_text(
                    text.replace(TABLE_MARKER + "\n", "").replace(TABLE_MARKER, ""),
                    encoding="utf-8"
                )
                entry["already_done"] = False

    # Phase 2–4
    logging.info("=== Phase 2-4: 取得・変換・挿入 ===")
    cache  = LawDataCache()
    errors: list[str] = []
    stats: dict[str, int] = {}

    for entry in targets:
        if entry["already_done"] and not args.force:
            stats["already_done"] = stats.get("already_done", 0) + 1
            continue
        status = process_file(entry, cache, args.dry_run, errors)
        stats[status] = stats.get(status, 0) + 1

    # Phase 6
    if not args.dry_run:
        logging.info("=== Phase 6: 整合性確認 ===")
        subprocess.run(
            ["python3", str(JPLAWDB4 / "verify_integrity.py")],
            cwd=str(JPLAWDB4)
        )

    # サマリー
    logging.info("\n=== 結果 ===")
    for k, v in sorted(stats.items()):
        logging.info(f"  {k}: {v}")
    if errors:
        logging.warning(f"\nエラー ({len(errors)} 件):")
        for e in errors:
            logging.warning(f"  {e}")


if __name__ == "__main__":
    main()
