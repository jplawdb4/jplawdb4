#!/usr/bin/env python3
"""jplawdb4: DB間フィールド名統一スクリプト

Phase 2: qa ディレクトリ名統一（sozoku/souzoku, zoyo/zouyo）
Phase 3: フィールド名統一（source_page→source_url, url→source_url, tax_types→topics）
"""
import os
import re
import sys
import shutil
import argparse
from pathlib import Path

JPLAWDB4 = Path("/home/user/jplawdb4")


def unify_qa_dirs(base: Path, dry_run=False):
    """Phase 2: qa ディレクトリ名の揺らぎを統一"""
    merges = [
        ("bunshokaito_souzoku", "bunshokaito_sozoku"),
        ("bunshokaito_zouyo", "bunshokaito_zoyo"),
    ]
    stats = {"moved": 0, "doc_code_fixed": 0}

    for src_name, dst_name in merges:
        src_dir = base / "qa" / "text" / src_name
        dst_dir = base / "qa" / "text" / dst_name

        if not src_dir.is_dir():
            continue

        if not dst_dir.is_dir():
            if not dry_run:
                dst_dir.mkdir(parents=True, exist_ok=True)

        for fname in sorted(os.listdir(src_dir)):
            if not fname.endswith(".txt"):
                continue

            src_path = src_dir / fname
            dst_path = dst_dir / fname

            if dst_path.exists():
                print(f"  WARNING: {dst_name}/{fname} already exists, skipping")
                continue

            # doc_code修正
            text = src_path.read_text(encoding="utf-8")
            if f"doc_code: {src_name}" in text:
                text = text.replace(f"doc_code: {src_name}", f"doc_code: {dst_name}")
                stats["doc_code_fixed"] += 1

            if dry_run:
                print(f"  MOVE: {src_name}/{fname} → {dst_name}/{fname}")
            else:
                dst_path.write_text(text, encoding="utf-8")
                src_path.unlink()
                print(f"  MOVE: {src_name}/{fname} → {dst_name}/{fname}")

            stats["moved"] += 1

        # 空ディレクトリ削除
        if not dry_run and src_dir.is_dir():
            remaining = list(src_dir.iterdir())
            if not remaining:
                src_dir.rmdir()
                print(f"  RMDIR: {src_name}/")

    return stats


def unify_tsutatsu_source(base: Path, dry_run=False):
    """Phase 3-A: tsutatsu source_page → source_url"""
    text_dir = base / "tsutatsu" / "text"
    stats = {"files": 0, "changed": 0}

    for root, dirs, files in os.walk(text_dir):
        for fname in sorted(files):
            if not fname.endswith(".txt"):
                continue

            fpath = Path(root) / fname
            text = fpath.read_text(encoding="utf-8")
            stats["files"] += 1

            if "source_page:" in text:
                new_text = text.replace("source_page:", "source_url:")
                stats["changed"] += 1
                if not dry_run:
                    fpath.write_text(new_text, encoding="utf-8")

    return stats


def unify_saiketsu_fields(base: Path, dry_run=False):
    """Phase 3-B/C: saiketsu url → source_url, tax_types → topics"""
    saiketsu_dir = base / "hanketsu" / "text" / "saiketsu"
    stats = {"files": 0, "url_changed": 0, "tax_types_changed": 0}

    if not saiketsu_dir.is_dir():
        return stats

    for fname in sorted(os.listdir(saiketsu_dir)):
        if not fname.endswith(".txt"):
            continue

        fpath = saiketsu_dir / fname
        text = fpath.read_text(encoding="utf-8")
        stats["files"] += 1
        changed = False

        # url: → source_url: (YAML行)
        if re.search(r"^url:", text, re.MULTILINE):
            text = re.sub(r"^url:", "source_url:", text, flags=re.MULTILINE)
            stats["url_changed"] += 1
            changed = True

        # tax_types: → topics: (YAML行)
        if re.search(r"^tax_types:", text, re.MULTILINE):
            text = re.sub(r"^tax_types:", "topics:", text, flags=re.MULTILINE)
            stats["tax_types_changed"] += 1
            changed = True

        if changed and not dry_run:
            fpath.write_text(text, encoding="utf-8")

    return stats


def main():
    parser = argparse.ArgumentParser(description="jplawdb4 field name unifier")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    mode = " (DRY RUN)" if args.dry_run else ""
    print(f"=== jplawdb4 Field Unification{mode} ===\n")

    # Phase 2: qa ディレクトリ統合
    print("Phase 2: qa directory merge")
    qa_stats = unify_qa_dirs(JPLAWDB4, args.dry_run)
    print(f"  → {qa_stats['moved']} files moved, {qa_stats['doc_code_fixed']} doc_codes fixed\n")

    # Phase 3-A: tsutatsu source_page → source_url
    print("Phase 3-A: tsutatsu source_page → source_url")
    ts_stats = unify_tsutatsu_source(JPLAWDB4, args.dry_run)
    print(f"  → {ts_stats['changed']}/{ts_stats['files']} files changed\n")

    # Phase 3-B/C: saiketsu url → source_url, tax_types → topics
    print("Phase 3-B/C: saiketsu url → source_url, tax_types → topics")
    sk_stats = unify_saiketsu_fields(JPLAWDB4, args.dry_run)
    print(f"  → url: {sk_stats['url_changed']}, tax_types: {sk_stats['tax_types_changed']} / {sk_stats['files']} files\n")

    # Summary
    print("=" * 50)
    total = qa_stats["moved"] + ts_stats["changed"] + sk_stats["url_changed"] + sk_stats["tax_types_changed"]
    print(f"Total changes: {total}")


if __name__ == "__main__":
    main()
