#!/usr/bin/env python3
"""jplawdb4: 分割・統合・トリミング後の包括的整合性検証"""
import os
import re
import sys
from pathlib import Path
from collections import defaultdict

import tiktoken

JPLAWDB4 = Path("/home/user/jplawdb4")
MAX_TOKENS = 9999
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

# トリミング対象フィールド（各DBで削除されているべきもの）
TRIM_CHECKS = {
    "law": [r"^url:"],
    "accounting": [r"^url:", r"^section:"],
    "guide": [r"^url:", r"^section:"],
    "tsutatsu": [r"^url:", r"^snapshot:"],
    "treaty": [r"^url:", r"^prev:", r"^next:", r"^section_id:", r"^page_start:", r"^page_end:"],
    "paper": [r"^url:", r"^prev:", r"^next:", r"^section_id:", r"^page_start:", r"^page_end:", r"^para_count:"],
    "qa": [r"^doc_title:", r"^source_kind:"],
    "hanketsu": [r"^summary_status:", r"^summary_source:"],
}


def count_tokens(text: str) -> int:
    return len(ENC.encode(text))


def verify_all():
    issues = defaultdict(list)
    stats = {
        "total_files": 0,
        "base_files": 0,
        "chunk_files": 0,
        "over_limit": 0,
        "empty_files": 0,
        "tiny_files": 0,  # < 50 tokens
        "orphan_chunks": 0,
        "missing_chunks": 0,
        "annotation_mismatch": 0,
        "double_split": 0,
        "trim_violations": 0,
        "split_note_remnants": 0,
    }
    db_stats = {}

    for db, rel_dir in DB_TEXT_DIRS.items():
        text_dir = JPLAWDB4 / rel_dir
        if not text_dir.is_dir():
            continue

        db_info = {"total": 0, "base": 0, "chunks": 0, "over": 0, "issues": 0}

        # 全ファイルを走査
        all_files = {}
        for root, dirs, files in os.walk(text_dir):
            for fname in sorted(files):
                if not fname.endswith(".txt"):
                    continue
                fpath = Path(root) / fname
                rel = fpath.relative_to(JPLAWDB4)
                all_files[str(rel)] = fpath

        # ファイルをベース/チャンクに分類（split注記の有無で判定）
        base_files = {}
        chunk_files = defaultdict(dict)  # {base_stem: {N: path}}

        # まず全ファイルを読み込み、split注記でチャンクかどうか判定
        file_texts = {}
        for rel_str, fpath in all_files.items():
            try:
                text = fpath.read_text(encoding="utf-8")
            except Exception as e:
                issues["read_error"].append(f"{rel_str}: {e}")
                text = ""
            file_texts[rel_str] = text

        for rel_str, fpath in all_files.items():
            fname = fpath.name
            text = file_texts.get(rel_str, "")
            stats["total_files"] += 1
            db_info["total"] += 1

            # チャンク判定: ファイル先頭に "--- split N/M of" がある（_2以降のチャンク）
            is_chunk = bool(re.match(r"--- split \d+/\d+ of ", text))

            if is_chunk:
                # ファイル名から_N部分を抽出
                m = re.match(r"^(.+)_(\d+)\.txt$", fname)
                if m:
                    base_stem = m.group(1)
                    chunk_n = int(m.group(2))
                    chunk_files[str(fpath.parent / base_stem)][chunk_n] = fpath
                    stats["chunk_files"] += 1
                    db_info["chunks"] += 1

                    # 二重分割チェック (_2_2.txt 等)
                    if re.search(r"_\d+_\d+\.txt$", fname):
                        stats["double_split"] += 1
                        issues["double_split"].append(str(rel_str))
                else:
                    # split注記があるのに_N.txtパターンでない → 異常
                    issues["annotation_mismatch"].append(
                        f"{rel_str}: has split annotation but filename doesn't match _N.txt pattern"
                    )
                    stats["annotation_mismatch"] += 1
            else:
                base_stem = fpath.stem
                base_files[str(fpath.parent / base_stem)] = fpath
                stats["base_files"] += 1
                db_info["base"] += 1

        # === CHECK 1: トークン数上限 ===（既読テキストを再利用）
        for rel_str, fpath in all_files.items():
            text = file_texts.get(rel_str, "")
            if not text and rel_str not in file_texts:
                continue

            tokens = count_tokens(text)

            if tokens > MAX_TOKENS:
                stats["over_limit"] += 1
                db_info["over"] += 1
                issues["over_limit"].append(f"{rel_str} ({tokens:,} tok)")

            if len(text.strip()) == 0:
                stats["empty_files"] += 1
                issues["empty_files"].append(str(rel_str))

            if tokens < 50 and len(text.strip()) > 0:
                stats["tiny_files"] += 1
                issues["tiny_files"].append(f"{rel_str} ({tokens} tok)")

        # === CHECK 2: 孤立チャンク（ベースファイル無し） ===
        for base_key, chunks in chunk_files.items():
            if base_key not in base_files:
                stats["orphan_chunks"] += 1
                for n, p in sorted(chunks.items()):
                    rel = p.relative_to(JPLAWDB4)
                    issues["orphan_chunks"].append(str(rel))

        # === CHECK 3: チャンク連続性 + split注記整合性 ===
        for base_key, base_fpath in base_files.items():
            base_rel = str(base_fpath.relative_to(JPLAWDB4))
            base_text = file_texts.get(base_rel, "")

            if base_key not in chunk_files:
                # 分割されていないファイル → split注記が残っていないかチェック
                if re.search(r"--- split \d+/\d+", base_text):
                    stats["split_note_remnants"] += 1
                    issues["split_note_remnants"].append(base_rel)
                continue

            chunks = chunk_files[base_key]
            chunk_nums = sorted(chunks.keys())

            m = re.search(r"--- split 1/(\d+)", base_text)
            if m:
                expected_total = int(m.group(1))
                expected_chunks = list(range(2, expected_total + 1))

                # チャンクファイルの番号と比較
                if chunk_nums != expected_chunks:
                    stats["missing_chunks"] += 1
                    rel = base_fpath.relative_to(JPLAWDB4)
                    issues["missing_chunks"].append(
                        f"{rel}: expected {expected_chunks}, found {chunk_nums}"
                    )

                # 各チャンクのsplit注記チェック
                for n, cpath in sorted(chunks.items()):
                    c_rel = str(cpath.relative_to(JPLAWDB4))
                    ctext = file_texts.get(c_rel, "")
                    cm = re.search(r"--- split (\d+)/(\d+)", ctext)
                    if cm:
                        noted_n = int(cm.group(1))
                        noted_total = int(cm.group(2))
                        if noted_n != n or noted_total != expected_total:
                            stats["annotation_mismatch"] += 1
                            rel = cpath.relative_to(JPLAWDB4)
                            issues["annotation_mismatch"].append(
                                f"{rel}: file is _{n}, note says {noted_n}/{noted_total}, expected {n}/{expected_total}"
                            )
                    else:
                        stats["annotation_mismatch"] += 1
                        rel = cpath.relative_to(JPLAWDB4)
                        issues["annotation_mismatch"].append(
                            f"{rel}: no split annotation found"
                        )
            else:
                # チャンクがあるのにベースにsplit注記がない
                stats["annotation_mismatch"] += 1
                rel = base_fpath.relative_to(JPLAWDB4)
                issues["annotation_mismatch"].append(
                    f"{rel}: has chunks {chunk_nums} but no split annotation in base"
                )

        # === CHECK 4: トリミング検証（ベースファイルのみ、先頭30行） ===
        if db in TRIM_CHECKS:
            patterns = TRIM_CHECKS[db]
            sample_count = 0
            violation_count = 0
            for base_key, base_fpath in base_files.items():
                b_rel = str(base_fpath.relative_to(JPLAWDB4))
                text = file_texts.get(b_rel, "")
                header_lines = text.split("\n")[:30]
                for pattern in patterns:
                    for line in header_lines:
                        if re.match(pattern, line.strip()):
                            violation_count += 1
                            rel = base_fpath.relative_to(JPLAWDB4)
                            issues["trim_violations"].append(
                                f"{rel}: still has '{line.strip()[:60]}'"
                            )
                            break
                sample_count += 1

            if violation_count > 0:
                stats["trim_violations"] += violation_count
                db_info["issues"] += violation_count

        db_stats[db] = db_info

    # === レポート出力 ===
    print("=" * 70)
    print("jplawdb4 包括的整合性検証レポート")
    print("=" * 70)

    print(f"\n📊 ファイル統計:")
    print(f"  総ファイル数:     {stats['total_files']:,}")
    print(f"  ベースファイル:   {stats['base_files']:,}")
    print(f"  チャンクファイル: {stats['chunk_files']:,}")

    print(f"\n📊 DB別内訳:")
    print(f"  {'DB':<12} {'Total':>6} {'Base':>6} {'Chunk':>6} {'Over':>5}")
    print(f"  {'-'*12} {'-'*6} {'-'*6} {'-'*6} {'-'*5}")
    for db, info in sorted(db_stats.items()):
        print(f"  {db:<12} {info['total']:>6} {info['base']:>6} {info['chunks']:>6} {info['over']:>5}")

    print(f"\n{'='*70}")
    print(f"🔍 検証結果:")
    print(f"{'='*70}")

    checks = [
        ("over_limit", "トークン上限超過 (>9,999)", "CRITICAL"),
        ("empty_files", "空ファイル", "WARNING"),
        ("tiny_files", "極小ファイル (<50tok)", "WARNING"),
        ("orphan_chunks", "孤立チャンク (ベース無し)", "CRITICAL"),
        ("missing_chunks", "欠損チャンク (連続性)", "CRITICAL"),
        ("annotation_mismatch", "split注記不整合", "CRITICAL"),
        ("double_split", "二重分割 (_N_M.txt)", "CRITICAL"),
        ("split_note_remnants", "未分割ファイルにsplit注記残存", "WARNING"),
        ("trim_violations", "トリミング漏れ", "WARNING"),
    ]

    all_pass = True
    for key, label, severity in checks:
        count = stats.get(key, 0)
        if count > 0:
            mark = "❌" if severity == "CRITICAL" else "⚠️"
            print(f"\n  {mark} {label}: {count}件")
            for item in issues[key][:20]:  # 最大20件表示
                print(f"     {item}")
            if len(issues[key]) > 20:
                print(f"     ... and {len(issues[key]) - 20} more")
            if severity == "CRITICAL":
                all_pass = False
        else:
            print(f"  ✅ {label}: 0件")

    print(f"\n{'='*70}")
    if all_pass:
        print("🎉 ALL CRITICAL CHECKS PASSED")
    else:
        print("🚨 CRITICAL ISSUES FOUND — 要修正")
    print(f"{'='*70}")

    return 0 if all_pass else 1


if __name__ == "__main__":
    sys.exit(verify_all())
