#!/usr/bin/env python3
"""jplawdb4: 不要メタデータ削除 + 既存分割アンドゥ + 再分割"""
import os
import re
import sys
import argparse
from pathlib import Path

import tiktoken

JPLAWDB4 = Path("/home/user/jplawdb4")
ENC = tiktoken.get_encoding("cl100k_base")

DB_TEXT_DIRS = {
    "law": "law/text",
    "tsutatsu": "tsutatsu/text",
    "hanketsu": "hanketsu/text",
    "qa": "qa/text",
    "guide": "guide/text",
    "paper": "paper/text",
    "treaty": "treaty/text",
    "accounting": "accounting/text",
    "beppyo": "beppyo/text",
}


def count_tokens(text: str) -> int:
    return len(ENC.encode(text))


# ─── Unsplit: チャンクファイルを元ファイルにマージ ───

def unsplit_file(base_path: Path):
    """_2, _3, ... _N を元ファイルにマージし、チャンクファイルを削除。
    Returns: (was_unsplit, chunks_removed)"""
    text = base_path.read_text(encoding="utf-8")

    # split注記があるか確認
    m = re.search(r'\n--- split 1/(\d+) → next: .+\.txt ---\n?$', text)
    if not m:
        return False, 0

    total = int(m.group(1))
    # split注記を除去
    merged = text[:m.start()] + "\n"

    stem = base_path.stem
    parent = base_path.parent
    suffix = base_path.suffix
    removed = 0

    for i in range(2, total + 1):
        chunk_path = parent / f"{stem}_{i}{suffix}"
        if not chunk_path.exists():
            print(f"  WARNING: missing chunk {chunk_path.name}")
            continue
        chunk_text = chunk_path.read_text(encoding="utf-8")
        # 先頭のsplit注記を除去
        chunk_text = re.sub(r'^--- split \d+/\d+ of .+\.txt ---\n', '', chunk_text)
        merged += chunk_text
        chunk_path.unlink()
        removed += 1

    base_path.write_text(merged, encoding="utf-8")
    return True, removed


# ─── DB別トリミング関数 ───

def trim_law(text: str) -> str:
    lines = text.split("\n")
    out = []
    for line in lines:
        # url: 行を削除
        if line.startswith("url: https://jplawdb"):
            continue
        # 空の egov_id を除去
        line = line.replace(" / egov_id:  /", " /")
        out.append(line)
    return "\n".join(out)


def trim_accounting_guide(text: str) -> str:
    lines = text.split("\n")
    out = []
    for line in lines:
        if line.startswith("url: https://jplawdb"):
            continue
        if line.startswith("section: "):
            continue
        out.append(line)
    return "\n".join(out)


def trim_tsutatsu(text: str) -> str:
    lines = text.split("\n")
    out = []
    for line in lines:
        if line.startswith("url: https://jplawdb"):
            continue
        if line.startswith("snapshot: "):
            continue
        # item行から / id: X 部分を削除
        if line.startswith("item: "):
            line = re.sub(r' / id: [^ /]+', '', line)
        out.append(line)
    return "\n".join(out)


def trim_yaml_fields(text: str, fields_to_remove: set, remove_pid_all: bool = False) -> str:
    """YAML形式ファイル（---区切り）から指定フィールドを削除"""
    lines = text.split("\n")
    out = []
    in_yaml = False
    skip_next_indent = False  # source: の次のindented行をスキップ

    for i, line in enumerate(lines):
        if line.strip() == "---":
            in_yaml = not in_yaml
            skip_next_indent = False
            out.append(line)
            continue

        if skip_next_indent:
            if line.startswith("  "):
                continue  # indented continuation → skip
            else:
                skip_next_indent = False

        if in_yaml:
            # フィールド名を取得
            m = re.match(r'^(\w[\w_]*):', line)
            if m:
                field = m.group(1)
                if field in fields_to_remove:
                    # source: は次の indented 行もスキップ
                    if field == "source":
                        skip_next_indent = True
                    continue
                # pid: "all" の特別処理
                if remove_pid_all and field == "pid":
                    val = line.split(":", 1)[1].strip().strip('"').strip("'")
                    if val == "all":
                        continue

        out.append(line)
    return "\n".join(out)


def trim_treaty(text: str) -> str:
    fields = {"url", "prev", "next", "section_id", "page_start", "page_end"}
    return trim_yaml_fields(text, fields, remove_pid_all=True)


def trim_paper(text: str) -> str:
    fields = {"url", "prev", "next", "section_id", "page_start", "page_end", "para_count"}
    return trim_yaml_fields(text, fields)


def trim_qa(text: str) -> str:
    lines = text.split("\n")
    out = []

    # Phase 1: ヘッダー部のフィールド削除
    in_header = True
    header_done = False
    body_lines = []

    for line in lines:
        if in_header:
            if line.strip() == "---":
                in_header = False
                header_done = True
                out.append(line)
                continue
            if line.startswith("doc_title: "):
                continue
            if line.startswith("source_kind: "):
                continue
            out.append(line)
        else:
            body_lines.append(line)

    if not header_done:
        return text  # ヘッダーが見つからない

    # Phase 2: パンくずリスト削除
    if body_lines:
        first_line = body_lines[0] if body_lines else ""
        remove_count = 0

        if first_line.startswith("法令等"):
            remove_count = 4
        elif first_line.startswith("税の情報"):
            remove_count = 4
        elif first_line.startswith("国税庁等について"):
            remove_count = 5

        if remove_count > 0 and len(body_lines) > remove_count:
            body_lines = body_lines[remove_count:]

    out.extend(body_lines)
    return "\n".join(out)


def trim_hanketsu(text: str) -> str:
    lines = text.split("\n")
    out = []
    in_yaml = False
    skip_source_indent = False
    skip_laws_empty = False

    for i, line in enumerate(lines):
        if line.strip() == "---":
            in_yaml = not in_yaml
            skip_source_indent = False
            out.append(line)
            continue

        if skip_source_indent:
            if line.startswith("  "):
                continue
            else:
                skip_source_indent = False

        if in_yaml:
            # summary_status, summary_source
            if line.startswith("summary_status:"):
                continue
            if line.startswith("summary_source:"):
                continue
            # source: + indented origin:
            if line.startswith("source:"):
                skip_source_indent = True
                continue
            # laws: [] (空配列のみ)
            if line.strip() == "laws: []":
                continue

        out.append(line)
    return "\n".join(out)


# ─── メイン処理 ───

TRIM_FUNCS = {
    "law": trim_law,
    "accounting": trim_accounting_guide,
    "guide": trim_accounting_guide,
    "tsutatsu": trim_tsutatsu,
    "treaty": trim_treaty,
    "paper": trim_paper,
    "qa": trim_qa,
    "hanketsu": trim_hanketsu,
}


def process_db(base: Path, db: str, rel_dir: str, dry_run: bool):
    """1 DB を処理: unsplit → trim → 統計"""
    text_dir = base / rel_dir
    if not text_dir.is_dir():
        return None

    trim_func = TRIM_FUNCS.get(db)
    stats = {
        "files": 0,
        "unsplit": 0,
        "chunks_removed": 0,
        "trimmed": 0,
        "tokens_before": 0,
        "tokens_after": 0,
    }

    for root, dirs, files in os.walk(text_dir):
        for fname in sorted(files):
            if not fname.endswith(".txt"):
                continue

            fpath = Path(root) / fname

            # split注記で始まるファイルはチャンク → スキップ（unsplit時に処理される）
            try:
                first_line = fpath.open(encoding="utf-8").readline()
            except:
                continue
            if first_line.startswith("--- split "):
                continue
            stats["files"] += 1

            # Step 1: unsplit
            was_unsplit, removed = unsplit_file(fpath)
            if was_unsplit:
                stats["unsplit"] += 1
                stats["chunks_removed"] += removed

            # Step 2: trim
            if trim_func:
                text = fpath.read_text(encoding="utf-8")
                tok_before = count_tokens(text)
                stats["tokens_before"] += tok_before

                trimmed = trim_func(text)
                tok_after = count_tokens(trimmed)
                stats["tokens_after"] += tok_after

                if trimmed != text:
                    stats["trimmed"] += 1
                    if not dry_run:
                        fpath.write_text(trimmed, encoding="utf-8")
            else:
                text = fpath.read_text(encoding="utf-8")
                tok = count_tokens(text)
                stats["tokens_before"] += tok
                stats["tokens_after"] += tok

    return stats


def main():
    parser = argparse.ArgumentParser(description="jplawdb4 metadata trimmer + resplitter")
    parser.add_argument("--dry-run", action="store_true", help="トークン削減量のみ表示")
    parser.add_argument("--db", choices=list(DB_TEXT_DIRS.keys()), help="特定DBのみ")
    args = parser.parse_args()

    print(f"=== jplawdb4 Metadata Trim {'(DRY RUN)' if args.dry_run else ''} ===\n")

    total_stats = {
        "files": 0, "unsplit": 0, "chunks_removed": 0,
        "trimmed": 0, "tokens_before": 0, "tokens_after": 0,
    }

    for db, rel_dir in DB_TEXT_DIRS.items():
        if args.db and db != args.db:
            continue

        stats = process_db(JPLAWDB4, db, rel_dir, args.dry_run)
        if stats is None:
            continue

        saved = stats["tokens_before"] - stats["tokens_after"]
        pct = (saved / stats["tokens_before"] * 100) if stats["tokens_before"] > 0 else 0

        print(f"  {db}: {stats['files']:,} files")
        if stats["unsplit"] > 0:
            print(f"    unsplit: {stats['unsplit']} files, {stats['chunks_removed']} chunks removed")
        if stats["trimmed"] > 0:
            print(f"    trimmed: {stats['trimmed']:,} files, -{saved:,} tokens ({pct:.1f}%)")

        for k in total_stats:
            total_stats[k] += stats[k]

    total_saved = total_stats["tokens_before"] - total_stats["tokens_after"]
    pct = (total_saved / total_stats["tokens_before"] * 100) if total_stats["tokens_before"] > 0 else 0
    print(f"\n{'='*50}")
    print(f"Total: {total_stats['files']:,} files")
    print(f"  Unsplit: {total_stats['unsplit']} files ({total_stats['chunks_removed']} chunks removed)")
    print(f"  Trimmed: {total_stats['trimmed']:,} files")
    print(f"  Tokens: {total_stats['tokens_before']:,} → {total_stats['tokens_after']:,} (-{total_saved:,}, {pct:.1f}%)")

    if not args.dry_run and total_stats["unsplit"] > 0:
        print(f"\n--- Re-splitting oversized files ---")
        # split_oversized の関数を直接インポート
        sys.path.insert(0, str(JPLAWDB4))
        from split_oversized import scan_and_split, verify, MAX_TOKENS
        split_stats = scan_and_split(JPLAWDB4, args.db)
        print(f"\nRe-split: {split_stats['split']} files → {split_stats['chunks_created']} chunks")
        print(f"\n--- Verification ---")
        violations = verify(JPLAWDB4, args.db)
        if violations:
            sys.exit(1)


if __name__ == "__main__":
    main()
