#!/usr/bin/env python3
"""
build_beppyo_laws.py
消費税法・所得税法・法人税法の 別表（AppdxTable）を
law/text/{lawdir}/beppyo-N.txt として生成する。

- 既存の条文ファイルはそのまま（変更しない）
- 附則別表（SupplProvisionAppdxTable）はスキップ
"""

import json
import subprocess
from pathlib import Path

import tiktoken

JPLAWDB4   = Path("/home/user/jplawdb4")
CACHE_DIR  = JPLAWDB4 / ".insert_tables_cache"
MAX_TOKENS = 9999
ENC = tiktoken.get_encoding("cl100k_base")

# ─── 対象法令の定義 ────────────────────────────────────────────────────────────

LAWS = [
    {
        "lawdir":   "shohizei",
        "law_name": "消費税法",
        "law_type": "act",
        "law_num":  "昭和六十三年法律第百八号",
        "egov_id":  "363AC0000000108",
        "as_of":    "2025-12-27",
    },
    {
        "lawdir":   "shotokuzei",
        "law_name": "所得税法",
        "law_type": "act",
        "law_num":  "昭和四十年法律第三十三号",
        "egov_id":  "340AC0000000033",
        "as_of":    "2025-12-27",
    },
    {
        "lawdir":   "hojinzei",
        "law_name": "法人税法",
        "law_type": "act",
        "law_num":  "昭和四十年法律第三十四号",
        "egov_id":  "340AC0000000034",
        "as_of":    "2025-12-27",
    },
]


# ─── ユーティリティ ────────────────────────────────────────────────────────────

def get_text(node) -> str:
    if isinstance(node, str):
        return node
    result = ""
    for c in node.get("children", []):
        if isinstance(c, (str, dict)):
            result += get_text(c)
    return result


def count_tokens(text: str) -> int:
    return len(ENC.encode(text))


def write_file(path: Path, content: str) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    tok = count_tokens(content)
    print(f"  wrote {path.name} ({tok:,} tok)")
    return tok


# ─── テーブル変換 ────────────────────────────────────────────────────────────

def expand_rowspan(rows_raw: list) -> list:
    grid = []
    rowspan_map: dict = {}  # col → (remaining, text)

    for row in rows_raw:
        new_row = []
        col_idx = 0
        cell_iter = iter(row)

        while True:
            if col_idx in rowspan_map:
                remaining, text = rowspan_map[col_idx]
                new_row.append(text)
                if remaining > 1:
                    rowspan_map[col_idx] = (remaining - 1, text)
                else:
                    del rowspan_map[col_idx]
                col_idx += 1
                continue
            try:
                rs, text = next(cell_iter)
            except StopIteration:
                break
            new_row.append(text)
            if rs > 1:
                rowspan_map[col_idx] = (rs - 1, text)
            col_idx += 1

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


def table_struct_to_grid(ts_node) -> list:
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


def grid_to_markdown(grid: list) -> str:
    if not grid:
        return ""
    n_cols = max(len(r) for r in grid)
    lines = []
    for i, row in enumerate(grid):
        padded = row + [""] * (n_cols - len(row))
        lines.append("| " + " | ".join(padded) + " |")
        if i == 0:
            lines.append("| " + " | ".join(["---"] * n_cols) + " |")
    return "\n".join(lines)


def extract_item_lines(node, depth=0) -> list:
    """Item/Subitem ノード → インデント付きテキスト行リスト"""
    lines = []
    tag = node.get("tag", "")
    indent = "  " * depth

    if tag == "Item":
        item_title = ""
        item_text = ""
        sub_lines = []
        for c in node.get("children", []):
            if not isinstance(c, dict): continue
            ct = c.get("tag", "")
            if ct == "ItemTitle":
                item_title = get_text(c).strip()
            elif ct == "ItemSentence":
                item_text = get_text(c).strip()
            elif ct.startswith("Subitem") or ct == "Item":
                sub_lines.extend(extract_item_lines(c, depth + 1))
        if item_title or item_text:
            lines.append(f"{indent}{item_title} {item_text}".rstrip())
        lines.extend(sub_lines)

    elif tag.startswith("Subitem"):
        sub_title = ""
        sub_text = ""
        sub_sub_lines = []
        for c in node.get("children", []):
            if not isinstance(c, dict): continue
            ct = c.get("tag", "")
            if ct.endswith("Title"):
                sub_title = get_text(c).strip()
            elif ct.endswith("Sentence"):
                sub_text = get_text(c).strip()
            elif ct.startswith("Subitem") or ct == "Item":
                sub_sub_lines.extend(extract_item_lines(c, depth + 1))
        if sub_title or sub_text:
            lines.append(f"{indent}{sub_title} {sub_text}".rstrip())
        lines.extend(sub_sub_lines)

    return lines


def appdx_table_to_markdown(appdx_node, title_tag="AppdxTableTitle") -> tuple:
    """AppdxTable ノード → (title, Markdown / テキスト)

    TableStruct があればMarkdownテーブル、Item列挙のみなら箇条書きテキストとして返す。
    """
    title = ""
    related_art = ""
    for c in appdx_node.get("children", []):
        if isinstance(c, dict) and c.get("tag") == title_tag:
            title = get_text(c).strip()
        elif isinstance(c, dict) and c.get("tag") == "RelatedArticleNum":
            related_art = get_text(c).strip()

    md_parts = []

    def walk_appdx(node):
        tag = node.get("tag", "")
        if tag == "TableStruct":
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
        elif tag == "Item":
            lines = extract_item_lines(node, depth=0)
            if lines:
                md_parts.append("\n".join(lines))
        else:
            for c in node.get("children", []):
                if isinstance(c, dict):
                    walk_appdx(c)

    if related_art:
        md_parts.append(related_art)
    walk_appdx(appdx_node)
    return title, "\n\n".join(md_parts)


# ─── 法令ごとの別表生成 ───────────────────────────────────────────────────────

def build_beppyo_for_law(law_info: dict):
    lawdir   = law_info["lawdir"]
    law_name = law_info["law_name"]
    law_type = law_info["law_type"]
    law_num  = law_info["law_num"]
    egov_id  = law_info["egov_id"]
    as_of    = law_info["as_of"]
    out_dir  = JPLAWDB4 / "law" / "text" / lawdir

    print(f"\n{'='*60}")
    print(f"【{law_name}】({lawdir})")
    print(f"{'='*60}")

    # キャッシュ読み込み
    cache_file = CACHE_DIR / f"{egov_id}.json"
    if not cache_file.exists():
        import urllib.request
        api_url = f"https://laws.e-gov.go.jp/api/2/law_data/{egov_id}"
        print(f"API取得中: {api_url}")
        with urllib.request.urlopen(api_url) as resp:
            raw = json.loads(resp.read().decode("utf-8"))
        root = raw.get("law_full_text", raw)
        cache_file.write_text(json.dumps(root, ensure_ascii=False))

    root = json.loads(cache_file.read_text())

    # ヘッダーテンプレート
    header = (
        f"law: {law_name} ({lawdir})\n"
        f"law_type: {law_type} / law_num: {law_num} / egov_id: {egov_id} / as_of: {as_of}"
    )

    # AppdxTable を順番に収集（附則内はスキップ）
    appdx_tables = []

    def walk(node, in_suppl=False):
        tag = node.get("tag", "")
        if tag == "SupplProvision":
            return  # 附則に入ったら停止
        if tag == "AppdxTable" and not in_suppl:
            appdx_tables.append(node)
        else:
            for c in node.get("children", []):
                if isinstance(c, dict):
                    walk(c, in_suppl)

    walk(root)
    print(f"  本則別表: {len(appdx_tables)}件")

    if not appdx_tables:
        print("  別表なし → スキップ")
        return []

    created = []
    for idx, appdx_node in enumerate(appdx_tables, start=1):
        title, md = appdx_table_to_markdown(appdx_node, "AppdxTableTitle")
        if not md.strip():
            print(f"  beppyo-{idx}: テーブルデータなし → スキップ")
            continue

        file_stem = f"beppyo-{idx}"
        out_path = out_dir / f"{file_stem}.txt"

        # 既存チェック（強制再書き込み不要なのでスキップ）
        if out_path.exists():
            print(f"  {out_path.name} 既存 → 上書き")  # 修正のため常に書き込む

        content = f"{header}\narticle: {file_stem} / title: {title}\n\n{md}\n"
        write_file(out_path, content)
        created.append(out_path)

    return created


# ─── メイン ──────────────────────────────────────────────────────────────────

def main():
    all_created = []

    for law_info in LAWS:
        files = build_beppyo_for_law(law_info)
        all_created.extend(files)

    # トークン超過チェック
    print(f"\n{'='*60}")
    print("トークン超過チェック")
    oversized = []
    for p in all_created:
        if p.exists():
            tok = count_tokens(p.read_text(encoding="utf-8"))
            if tok > MAX_TOKENS:
                oversized.append((p, tok))
                print(f"  ⚠️  超過: {p.name} ({tok:,} tok)")

    if oversized:
        print("\nsplit_oversized.py を実行...")
        result = subprocess.run(
            ["python3", str(JPLAWDB4 / "split_oversized.py"), "--db", "law"],
            capture_output=True, text=True, cwd=str(JPLAWDB4)
        )
        if result.returncode != 0:
            print("  ERROR:", result.stderr[:300])
        else:
            print("  分割完了")
            for line in result.stdout.splitlines():
                if "split" in line.lower() or "oversized" in line.lower():
                    print("  ", line)
    else:
        print("  全ファイル 9,999 tok 以内 ✓")

    # 整合性確認
    print(f"\n{'='*60}")
    print("整合性確認")
    result = subprocess.run(
        ["python3", str(JPLAWDB4 / "verify_integrity.py"), "--db", "law"],
        capture_output=True, text=True, cwd=str(JPLAWDB4)
    )
    for line in result.stdout.splitlines():
        if any(kw in line for kw in ["shohizei", "shotokuzei", "hojinzei", "PASS", "ISSUE", "ERROR", "🎉", "❌"]):
            print(" ", line)

    total = len(all_created)
    print(f"\n✅ 完了: 別表 {total}件生成")


if __name__ == "__main__":
    main()
