#!/usr/bin/env python3
"""
build_taiyounensuu.py
減価償却資産の耐用年数等に関する省令（昭和四十年大蔵省令第十五号）を
law/text/taiyounensuu_kisoku/ に追加する。

- 第一条〜第六条: 通常の条文ファイル (1.txt 〜 6.txt)
- 別表第一〜第十一: 耐用年数表 (beppyo_1.txt 〜 beppyo_11.txt)
"""

import json
import re
import subprocess
from pathlib import Path
from datetime import datetime

import tiktoken

# ─── 定数 ───────────────────────────────────────────────────────────────────

JPLAWDB4  = Path("/home/user/jplawdb4")
OUT_DIR   = JPLAWDB4 / "law" / "text" / "taiyounensuu_kisoku"
JSON_PATH = Path("/tmp/taiyou.json")
API_URL   = "https://laws.e-gov.go.jp/api/2/law_data/340M50000040015"

LAWDIR    = "taiyounensuu_kisoku"
LAW_NAME  = "減価償却資産の耐用年数等に関する省令"
LAW_NUM   = "昭和四十年大蔵省令第十五号"
EGOV_ID   = "340M50000040015"
AS_OF     = "2025-04-01"   # 最終改正施行日

MAX_TOKENS = 9999
ENC = tiktoken.get_encoding("cl100k_base")


# ─── ユーティリティ ──────────────────────────────────────────────────────────

def get_text(node) -> str:
    """ノードからテキストを再帰的に取得"""
    if isinstance(node, str):
        return node
    result = ""
    for c in node.get("children", []):
        if isinstance(c, (str, dict)):
            result += get_text(c)
    return result


def count_tokens(text: str) -> int:
    return len(ENC.encode(text))


def write_file(path: Path, content: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    tok = count_tokens(content)
    print(f"  wrote {path.name} ({tok:,} tok)")
    return tok


# ─── テキスト抽出 ────────────────────────────────────────────────────────────

def para_id_tag(para_node) -> str:
    """Paragraph → [pN] タグ文字列"""
    attr = para_node.get("attr", {})
    num = attr.get("Num", "")
    return f"[p{num}]"


def item_tag(ancestors: list) -> str:
    """Item/Subitem 階層 → [pN-iM-sK] タグ文字列"""
    parts = []
    for a in ancestors:
        tag = a.get("tag", "")
        num = a.get("attr", {}).get("Num", "")
        if tag == "Paragraph":
            parts.append(f"p{num}")
        elif tag.startswith("Item"):
            # Item, Subitem1..4
            depth = 0 if tag == "Item" else int(re.search(r'\d+', tag).group()) if re.search(r'\d+', tag) else 0
            prefix = "i" if depth == 0 else "s"
            parts.append(f"{prefix}{num}")
    return "[" + "-".join(parts) + "]"


def extract_paragraph_lines(para_node, ancestors=None) -> list[str]:
    """Paragraph ノード → 行リスト"""
    if ancestors is None:
        ancestors = []
    lines = []
    tag = para_node.get("tag", "")
    attr = para_node.get("attr", {})
    num = attr.get("Num", "")

    if tag == "Paragraph":
        p_tag = f"[p{num}]"
        # ParagraphSentence のテキスト
        para_text = ""
        items_lines = []
        for c in para_node.get("children", []):
            if isinstance(c, dict):
                ct = c.get("tag", "")
                if ct == "ParagraphSentence":
                    para_text = get_text(c).strip()
                elif ct.startswith("Item"):
                    items_lines.extend(
                        extract_paragraph_lines(c, ancestors + [para_node])
                    )
        if para_text:
            lines.append(f"{p_tag} {para_text}")
        lines.extend(items_lines)

    elif tag == "Item":
        itag = f"[p{ancestors[-1].get('attr',{}).get('Num','')}-i{num}]" if ancestors else f"[i{num}]"
        item_text = ""
        sub_lines = []
        for c in para_node.get("children", []):
            if isinstance(c, dict):
                ct = c.get("tag", "")
                if ct == "ItemTitle":
                    item_text += get_text(c).strip() + " "
                elif ct == "ItemSentence":
                    item_text += get_text(c).strip()
                elif ct.startswith("Subitem") or ct == "Item":
                    sub_lines.extend(
                        extract_paragraph_lines(c, ancestors + [para_node])
                    )
        if item_text.strip():
            lines.append(f"{itag} {item_text.strip()}")
        lines.extend(sub_lines)

    elif tag.startswith("Subitem"):
        depth = re.search(r'\d+', tag)
        depth = int(depth.group()) if depth else 1
        p_num = ""
        i_num = ""
        for a in ancestors:
            t = a.get("tag", "")
            n = a.get("attr", {}).get("Num", "")
            if t == "Paragraph":
                p_num = n
            elif t == "Item":
                i_num = n
        stag = f"[p{p_num}-i{i_num}-s{num}]" if p_num and i_num else f"[s{num}]"
        item_text = ""
        sub_lines = []
        for c in para_node.get("children", []):
            if isinstance(c, dict):
                ct = c.get("tag", "")
                if ct.endswith("Title"):
                    item_text += get_text(c).strip() + " "
                elif ct.endswith("Sentence"):
                    item_text += get_text(c).strip()
                elif ct.startswith("Subitem"):
                    sub_lines.extend(
                        extract_paragraph_lines(c, ancestors + [para_node])
                    )
        if item_text.strip():
            lines.append(f"{stag} {item_text.strip()}")
        lines.extend(sub_lines)

    return lines


def article_to_text(article_node) -> tuple[str, str]:
    """Article ノード → (article_id, 本文テキスト)"""
    attr = article_node.get("attr", {})
    num = attr.get("Num", "")
    title_text = ""
    para_lines = []

    for c in article_node.get("children", []):
        if isinstance(c, dict):
            ct = c.get("tag", "")
            if ct == "ArticleTitle":
                title_text = get_text(c).strip()
            elif ct == "Paragraph":
                para_lines.extend(extract_paragraph_lines(c))

    body = "\n".join(para_lines)
    return num, title_text, body


# ─── テーブル変換 ────────────────────────────────────────────────────────────

def expand_rowspan(rows_raw: list[list[tuple]]) -> list[list[str]]:
    """rowspan を展開して均一な行列に変換"""
    grid: list[list[str]] = []
    rowspan_map: dict[int, tuple[int, str]] = {}  # col → (remaining, text)

    for row in rows_raw:
        new_row = []
        col_idx = 0
        cell_iter = iter(row)

        while True:
            # rowspan_map にある列は補完
            if col_idx in rowspan_map:
                remaining, text = rowspan_map[col_idx]
                new_row.append(text)
                if remaining > 1:
                    rowspan_map[col_idx] = (remaining - 1, text)
                else:
                    del rowspan_map[col_idx]
                col_idx += 1
                continue
            # 次のセルを取得
            try:
                rs, text = next(cell_iter)
            except StopIteration:
                break
            new_row.append(text)
            if rs > 1:
                rowspan_map[col_idx] = (rs - 1, text)
            col_idx += 1

        # 残 rowspan_map
        while col_idx in rowspan_map:
            remaining, text = rowspan_map[col_idx]
            new_row.append(text)
            if remaining > 1:
                rowspan_map[col_idx] = (remaining - 1, text)
            else:
                del rowspan_map[col_idx]
            col_idx += 1

        grid.append(new_row)

    return grid


def table_struct_to_grid(ts_node) -> list[list[str]]:
    """TableStruct ノード → 行列リスト"""
    rows_raw = []

    def walk_table(node):
        tag = node.get("tag", "")
        if tag == "TableRow":
            row = []
            for c in node.get("children", []):
                if isinstance(c, dict) and c.get("tag") == "TableColumn":
                    rs = int(c.get("attr", {}).get("rowspan", 1))
                    text = get_text(c).strip().replace("\n", " ")
                    row.append((rs, text))
            rows_raw.append(row)
        else:
            for c in node.get("children", []):
                if isinstance(c, dict):
                    walk_table(c)

    walk_table(ts_node)
    return expand_rowspan(rows_raw)


def grid_to_markdown(grid: list[list[str]]) -> str:
    """行列リスト → Markdown テーブル文字列"""
    if not grid:
        return ""
    n_cols = max(len(r) for r in grid)
    lines = []
    for i, row in enumerate(grid):
        # 列数を揃える
        padded = row + [""] * (n_cols - len(row))
        lines.append("| " + " | ".join(padded) + " |")
        if i == 0:
            lines.append("| " + " | ".join(["---"] * n_cols) + " |")
    return "\n".join(lines)


def appdx_table_to_markdown(appdx_node) -> tuple[str, str]:
    """AppdxTable ノード → (title, Markdown テーブル)"""
    title = ""
    for c in appdx_node.get("children", []):
        if isinstance(c, dict) and c.get("tag") == "AppdxTableTitle":
            title = get_text(c).strip()
            break

    # TableStruct を複数持つ場合もある
    md_parts = []

    def walk_appdx(node):
        tag = node.get("tag", "")
        if tag == "TableStruct":
            # TableStructTitle があれば小見出し
            ts_title = ""
            for c in node.get("children", []):
                if isinstance(c, dict) and c.get("tag") == "TableStructTitle":
                    ts_title = get_text(c).strip()
                    break
            grid = table_struct_to_grid(node)
            md = grid_to_markdown(grid)
            if ts_title:
                md_parts.append(f"### {ts_title}\n{md}")
            else:
                md_parts.append(md)
        else:
            for c in node.get("children", []):
                if isinstance(c, dict):
                    walk_appdx(c)

    walk_appdx(appdx_node)
    return title, "\n\n".join(md_parts)


# ─── ファイル生成 ────────────────────────────────────────────────────────────

HEADER_TMPL = "law: {law_name} ({lawdir})\nlaw_type: rule / law_num: {law_num} / egov_id: {egov_id} / as_of: {as_of}"


def build_article_file(article_node) -> Path | None:
    """Article → .txt ファイル生成"""
    num, title, body = article_to_text(article_node)
    if not num or not body.strip():
        return None

    # 附則（SupplProvision 内の Article）は除外
    # → walk 側でフィルタするので、ここでは num チェックのみ
    file_stem = num.replace("_", "-")  # "1", "2", ..., "6"
    out_path = OUT_DIR / f"{file_stem}.txt"

    header = HEADER_TMPL.format(
        law_name=LAW_NAME, lawdir=LAWDIR,
        law_num=LAW_NUM, egov_id=EGOV_ID, as_of=AS_OF
    )
    content = f"{header}\narticle: {num} / title: {title}\n\n{body}\n"
    write_file(out_path, content)
    return out_path


def build_beppyo_file(appdx_node, idx: int) -> Path:
    """AppdxTable → beppyo_N.txt ファイル生成"""
    title, md = appdx_table_to_markdown(appdx_node)

    file_stem = f"beppyo-{idx}"
    out_path = OUT_DIR / f"{file_stem}.txt"

    header = HEADER_TMPL.format(
        law_name=LAW_NAME, lawdir=LAWDIR,
        law_num=LAW_NUM, egov_id=EGOV_ID, as_of=AS_OF
    )
    content = f"{header}\narticle: {file_stem} / title: {title}\n\n{md}\n"
    write_file(out_path, content)
    return out_path


# ─── メイン ──────────────────────────────────────────────────────────────────

def main():
    # JSON 取得（キャッシュ優先）
    if JSON_PATH.exists():
        print(f"キャッシュ使用: {JSON_PATH}")
        data = json.loads(JSON_PATH.read_text(encoding="utf-8"))
    else:
        import urllib.request
        print(f"API 取得中: {API_URL}")
        with urllib.request.urlopen(API_URL) as resp:
            raw = resp.read().decode("utf-8")
        JSON_PATH.write_text(raw, encoding="utf-8")
        data = json.loads(raw)

    law = data["law_full_text"]

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"\n出力先: {OUT_DIR}")

    # Phase 1: 本則 Article (第一条〜第六条) 抽出
    print("\n--- 条文 ---")
    article_files = []

    def walk_main_articles(node, in_suppl=False):
        tag = node.get("tag", "")
        if tag == "SupplProvision":
            # 附則に入ったら停止
            return
        if tag == "Article" and not in_suppl:
            num = node.get("attr", {}).get("Num", "")
            # 数字のみの番号（1〜6）のみ対象
            if num.isdigit() and 1 <= int(num) <= 6:
                path = build_article_file(node)
                if path:
                    article_files.append(path)
        else:
            for c in node.get("children", []):
                if isinstance(c, dict):
                    walk_main_articles(c, in_suppl)

    walk_main_articles(law)

    # Phase 2: AppdxTable (別表第一〜第十一) 抽出
    print("\n--- 別表 ---")
    beppyo_files = []
    beppyo_idx = 0

    def walk_appdx_tables(node):
        nonlocal beppyo_idx
        tag = node.get("tag", "")
        if tag == "AppdxTable":
            beppyo_idx += 1
            path = build_beppyo_file(node, beppyo_idx)
            beppyo_files.append(path)
        else:
            for c in node.get("children", []):
                if isinstance(c, dict):
                    walk_appdx_tables(c)

    walk_appdx_tables(law)

    # Phase 3: トークン超過ファイルを split_oversized.py で分割
    print("\n--- トークン超過チェック ---")
    oversized = []
    for p in list(article_files) + list(beppyo_files):
        if p.exists():
            tok = count_tokens(p.read_text(encoding="utf-8"))
            if tok > MAX_TOKENS:
                oversized.append((p, tok))
                print(f"  超過: {p.name} ({tok:,} tok)")

    if oversized:
        print("split_oversized.py を実行...")
        result = subprocess.run(
            ["python3", str(JPLAWDB4 / "split_oversized.py"), "--db", "law"],
            capture_output=True, text=True, cwd=str(JPLAWDB4)
        )
        if result.returncode != 0:
            print("  ERROR:", result.stderr[:300])
        else:
            print("  分割完了")

    # Phase 4: verify_integrity
    print("\n--- 整合性確認 ---")
    result = subprocess.run(
        ["python3", str(JPLAWDB4 / "verify_integrity.py"), "--db", "law"],
        capture_output=True, text=True, cwd=str(JPLAWDB4)
    )
    # taiyounensuu_kisoku 関連のみ表示
    for line in result.stdout.splitlines():
        if "taiyou" in line or "PASS" in line or "ISSUE" in line or "ERROR" in line or "❌" in line or "🎉" in line:
            print(" ", line)

    print(f"\n✅ 完了: {len(article_files)}条文 + {len(beppyo_files)}別表")


if __name__ == "__main__":
    main()
