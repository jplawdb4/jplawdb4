#!/usr/bin/env python3
"""jplawdb4: 9,500トークン超過ファイルの分割スクリプト"""
import os
import re
import sys
import argparse
from pathlib import Path

import tiktoken

JPLAWDB4 = Path("/home/user/jplawdb4")
MAX_TOKENS = 9999
TARGET_TOKENS = 9900  # split注記マージン
MIN_CHUNK_TOKENS = 200  # これ未満のチャンクは前チャンクにマージ
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

HEADER_TYPE = {
    "law": "blank", "tsutatsu": "blank", "hanketsu": "blank",
    "qa": "dashes", "guide": "blank",
    "paper": "yaml_or_blank", "treaty": "yaml_or_blank",
    "accounting": "blank", "beppyo": "blank",
}


def count_tokens(text: str) -> int:
    return len(ENC.encode(text))


def extract_header(text: str, db: str):
    """ヘッダーと本文を分離。(header_text, body_text) を返す"""
    htype = HEADER_TYPE.get(db, "blank")
    lines = text.split("\n")

    if htype == "dashes":
        for i, line in enumerate(lines):
            if line.strip() == "---" and i > 0:
                header = "\n".join(lines[:i + 1]) + "\n"
                body = "\n".join(lines[i + 1:])
                return header, body
        return "", text

    elif htype == "yaml_or_blank":
        if lines and lines[0].strip() == "---":
            for i in range(1, len(lines)):
                if lines[i].strip() == "---":
                    header = "\n".join(lines[:i + 1]) + "\n"
                    body = "\n".join(lines[i + 1:])
                    return header, body
            return "", text
        else:
            for i, line in enumerate(lines):
                if line.strip() == "" and i > 0:
                    header = "\n".join(lines[:i + 1]) + "\n"
                    body = "\n".join(lines[i + 1:])
                    return header, body
            return "", text

    else:  # "blank"
        for i, line in enumerate(lines):
            if line.strip() == "" and i > 0:
                header = "\n".join(lines[:i + 1]) + "\n"
                body = "\n".join(lines[i + 1:])
                return header, body
        return "", text


def get_line_boundaries(body: str):
    """行末位置のリストを返す（フォールバック用）"""
    boundaries = []
    pos = 0
    for line in body.split("\n"):
        pos += len(line) + 1  # +1 for \n
        if pos < len(body):
            boundaries.append(pos)
    return boundaries


def find_boundaries(body: str, db: str):
    """本文中の分割可能ポイント（文字位置リスト）を返す。
    3段階のフォールバック付き。"""

    if db in ("law", "tsutatsu", "hanketsu", "accounting", "guide"):
        # Level 1: [pN] マーカー
        b1 = [m.start() for m in re.finditer(r"^\[p\d+\]", body, re.MULTILINE)
               if m.start() > 0]
        # Level 2: [pN-iM] マーカー
        b2 = [m.start() for m in re.finditer(r"^\[p\d+-i\d+\]", body, re.MULTILINE)
               if m.start() > 0]
        # Level 3: 行単位
        b3 = get_line_boundaries(body)
        return b1, b2, b3

    elif db in ("paper", "treaty"):
        # Level 1: ## セクションヘッダー
        b1 = [m.start() for m in re.finditer(r"^##\s", body, re.MULTILINE)
               if m.start() > 0]
        # Level 2: 空行
        b2 = [m.start() + 1 for m in re.finditer(r"\n\n", body)
               if m.start() > 0]
        # Level 3: 行単位
        b3 = get_line_boundaries(body)
        return b1, b2, b3

    else:  # qa, etc.
        # Level 1: 空行
        b1 = [m.start() + 1 for m in re.finditer(r"\n\n", body)
               if m.start() > 0]
        # Level 2: 行単位
        b2 = get_line_boundaries(body)
        b3 = b2
        return b1, b2, b3


def greedy_split_body(body: str, budget: int, boundary_levels):
    """本文をbudget以内のチャンクに分割。
    boundary_levels: (coarse, medium, fine) の3段階境界リスト。
    超過チャンクは次のレベルの境界で再分割。"""

    def split_at_level(text: str, start_offset: int, level_idx: int):
        """指定レベルの境界でテキストを分割"""
        if level_idx >= len(boundary_levels):
            # 最終フォールバック: そのまま返す（verifyで検出）
            return [text]

        # この text の範囲に該当する境界を計算
        all_bounds = boundary_levels[level_idx]
        # start_offset 基準の境界を、text ローカル座標に変換
        local_bounds = sorted(set(
            b - start_offset for b in all_bounds
            if start_offset < b < start_offset + len(text)
        ))

        if not local_bounds:
            # このレベルに境界がない → 次のレベルへ
            return split_at_level(text, start_offset, level_idx + 1)

        chunks = []
        pos = 0
        while pos < len(text):
            best_end = None
            for b in local_bounds:
                if b <= pos:
                    continue
                chunk_text = text[pos:b]
                tok = count_tokens(chunk_text)
                if tok <= budget:
                    best_end = b
                else:
                    break

            if best_end is None:
                # budget内に収まる境界がない
                next_bounds = [b for b in local_bounds if b > pos]
                if next_bounds:
                    # 最小の境界で切って、超過チャンクは次レベルで再分割
                    best_end = next_bounds[0]
                    chunk = text[pos:best_end]
                    if count_tokens(chunk) > budget:
                        sub_chunks = split_at_level(chunk, start_offset + pos, level_idx + 1)
                        chunks.extend(sub_chunks)
                        pos = best_end
                        continue
                else:
                    # 残り全部 → 次レベルで再分割
                    remainder = text[pos:]
                    if count_tokens(remainder) > budget:
                        sub_chunks = split_at_level(remainder, start_offset + pos, level_idx + 1)
                        chunks.extend(sub_chunks)
                    else:
                        chunks.append(remainder)
                    pos = len(text)  # prevent duplicate in post-loop check
                    break

            chunks.append(text[pos:best_end])
            pos = best_end

        if pos < len(text):
            remainder = text[pos:]
            if remainder.strip():
                chunks.append(remainder)

        return chunks

    return split_at_level(body, 0, 0)


def merge_small_chunks(chunks, min_tokens=MIN_CHUNK_TOKENS):
    """微小チャンクを前のチャンクにマージ"""
    if len(chunks) <= 1:
        return chunks
    merged = [chunks[0]]
    for chunk in chunks[1:]:
        if count_tokens(chunk) < min_tokens and merged:
            merged[-1] = merged[-1] + chunk
        else:
            merged.append(chunk)
    return merged


def rebalance_to_two(body: str, header: str, stem: str, boundary_levels):
    """3チャンク以上→2チャンクへのリバランス。
    全境界レベルから最もバランスの良い2分割点を探索。
    Returns: [part1, part2] or None if impossible."""
    note1 = f"\n--- split 1/2 → next: {stem}_2.txt ---\n"
    note2 = f"--- split 2/2 of {stem}.txt ---\n"
    header_tok = count_tokens(header)
    note1_tok = count_tokens(note1)
    note2_tok = count_tokens(note2)

    # 5トークンの安全マージン（トークナイゼーション境界効果）
    budget1 = MAX_TOKENS - header_tok - note1_tok - 5  # chunk1のbody予算
    budget2 = MAX_TOKENS - note2_tok - 5               # chunk2のbody予算

    if budget1 < 100 or budget2 < 100:
        return None

    for level_bounds in boundary_levels:
        bounds = sorted(b for b in level_bounds if 0 < b < len(body))
        if not bounds:
            continue

        best_split = None
        best_diff = float('inf')

        for b in bounds:
            p1_tok = count_tokens(body[:b])
            p2_tok = count_tokens(body[b:])
            if p1_tok <= budget1 and p2_tok <= budget2:
                diff = abs(p1_tok - p2_tok)
                if diff < best_diff:
                    best_diff = diff
                    best_split = b

        if best_split is not None:
            return [body[:best_split], body[best_split:]]

    return None


def split_file(fpath: Path, db: str, dry_run=False):
    """1ファイルを分割。Returns: (was_split, num_chunks)"""
    text = fpath.read_text(encoding="utf-8")
    tokens = count_tokens(text)

    if tokens <= MAX_TOKENS:
        return False, 1

    header, body = extract_header(text, db)
    boundary_levels = find_boundaries(body, db)
    stem = fpath.stem

    header_tokens = count_tokens(header)
    note_overhead = 50
    budget = TARGET_TOKENS - header_tokens - note_overhead

    if budget < 500:
        # ヘッダーが巨大 → 最小化
        header_lines = header.split("\n")
        header = "\n".join(header_lines[:2]) + "\n"
        header_tokens = count_tokens(header)
        budget = TARGET_TOKENS - header_tokens - note_overhead

    # 全体がTARGET内なら分割不要
    if count_tokens(header + body) <= TARGET_TOKENS:
        return False, 1

    # 本文を分割
    body_chunks = greedy_split_body(body, budget, boundary_levels)

    # 微小チャンクをマージ
    body_chunks = merge_small_chunks(body_chunks)

    if len(body_chunks) <= 1:
        return False, 1

    # 3チャンク以上で2チャンクに収まる場合はリバランス
    if len(body_chunks) >= 3:
        two = rebalance_to_two(body, header, stem, boundary_levels)
        if two is not None:
            body_chunks = two

    total = len(body_chunks)

    # split注記付きの最終チャンクを生成
    final_chunks = []
    for i, chunk_body in enumerate(body_chunks):
        n = i + 1
        if n == 1:
            note = f"\n--- split 1/{total} → next: {stem}_2.txt ---\n"
            final_chunks.append(header + chunk_body + note)
        else:
            note = f"--- split {n}/{total} of {stem}.txt ---\n"
            final_chunks.append(note + chunk_body)

    if dry_run:
        print(f"  → {len(final_chunks)} chunks: ", end="")
        for i, c in enumerate(final_chunks):
            print(f"[{i+1}]{count_tokens(c)}tok ", end="")
        print()
        return True, len(final_chunks)

    # ファイル書き出し
    parent = fpath.parent
    suffix = fpath.suffix

    fpath.write_text(final_chunks[0], encoding="utf-8")
    for i, chunk in enumerate(final_chunks[1:], start=2):
        new_path = parent / f"{stem}_{i}{suffix}"
        new_path.write_text(chunk, encoding="utf-8")

    return True, len(final_chunks)


def scan_and_split(base: Path, target_db=None, dry_run=False):
    """全DBをスキャンし、超過ファイルを分割"""
    stats = {"scanned": 0, "oversized": 0, "split": 0, "chunks_created": 0}

    for db, rel_dir in DB_TEXT_DIRS.items():
        if target_db and db != target_db:
            continue
        text_dir = base / rel_dir
        if not text_dir.is_dir():
            continue

        db_oversized = 0
        for root, dirs, files in os.walk(text_dir):
            for fname in sorted(files):
                if not fname.endswith(".txt"):
                    continue

                fpath = Path(root) / fname
                text = fpath.read_text(encoding="utf-8")

                # split注記で始まるファイルはチャンク → スキップ
                if text.startswith("--- split "):
                    continue
                tokens = count_tokens(text)
                stats["scanned"] += 1

                if tokens > MAX_TOKENS:
                    stats["oversized"] += 1
                    db_oversized += 1
                    rel = fpath.relative_to(base)
                    print(f"  {db}: {rel} ({tokens:,} tokens)", end="")

                    was_split, num_chunks = split_file(fpath, db, dry_run)
                    if was_split:
                        stats["split"] += 1
                        stats["chunks_created"] += num_chunks
                    if not dry_run:
                        print(f" → {num_chunks} chunks")

        if db_oversized > 0:
            print(f"  [{db}] {db_oversized} oversized files")

    return stats


def verify(base: Path, target_db=None):
    """分割後の全ファイルが MAX_TOKENS 以下か検証"""
    violations = []
    total = 0

    for db, rel_dir in DB_TEXT_DIRS.items():
        if target_db and db != target_db:
            continue
        text_dir = base / rel_dir
        if not text_dir.is_dir():
            continue

        for root, dirs, files in os.walk(text_dir):
            for fname in sorted(files):
                if not fname.endswith(".txt"):
                    continue
                fpath = Path(root) / fname
                text = fpath.read_text(encoding="utf-8")
                tokens = count_tokens(text)
                total += 1

                if tokens > MAX_TOKENS:
                    rel = fpath.relative_to(base)
                    violations.append((rel, tokens))

    print(f"Verified: {total} files")
    if violations:
        print(f"VIOLATIONS: {len(violations)} files still over {MAX_TOKENS} tokens:")
        for rel, tok in sorted(violations, key=lambda x: -x[1]):
            print(f"  {tok:>6,} tok  {rel}")
    else:
        print(f"ALL PASS: every file ≤ {MAX_TOKENS} tokens")

    return violations


def main():
    parser = argparse.ArgumentParser(description="jplawdb4 token-limit splitter")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--db", choices=list(DB_TEXT_DIRS.keys()))
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()

    if args.verify:
        violations = verify(JPLAWDB4, args.db)
        sys.exit(1 if violations else 0)

    print(f"=== jplawdb4 Token Split (max={MAX_TOKENS}, target={TARGET_TOKENS}) ===\n")
    stats = scan_and_split(JPLAWDB4, args.db, args.dry_run)

    print(f"\nScanned: {stats['scanned']:,} files")
    print(f"Oversized: {stats['oversized']:,} files")
    if not args.dry_run:
        print(f"Split: {stats['split']:,} files → {stats['chunks_created']:,} chunks")


if __name__ == "__main__":
    main()
