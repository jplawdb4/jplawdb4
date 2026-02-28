#!/usr/bin/env python3
"""jplawdb4 Step 3.5: shard/shards_index内のURL書き換え"""
import json
import os
import re
import sys

DST = "/home/user/jplawdb4"

OLD_BASE = "https://jplawdb.github.io/html-preview/"

def rewrite_qa_guide_accounting_shards():
    """qa/guide/accounting: TSV内のtext_url→相対パス変換、enhanced_url列削除"""
    configs = [
        {
            "db": "qa",
            "old_db": "ai-nta-qa-db",
            # 列: id source_kind doc_code doc_title item_id item_title snippet url text_url source_url
            # url列 = enhanced URL → 削除
            # text_url列 → 相対パス
            # source_url列 = NTA外部URL → そのまま
            "drop_cols": ["url"],  # enhanced URL
            "rewrite_cols": {"text_url": lambda v, db="qa": url_to_relative(v, "ai-nta-qa-db", db)},
        },
        {
            "db": "guide",
            "old_db": "ai-nta-guide-db",
            # 列: doc_code item_id title snippet text_url enhanced_url
            "drop_cols": ["enhanced_url"],
            "rewrite_cols": {"text_url": lambda v, db="guide": url_to_relative(v, "ai-nta-guide-db", db)},
        },
        {
            "db": "accounting",
            "old_db": "ai-accounting-db",
            # 列: doc_code item_id title snippet text_url enhanced_url
            "drop_cols": ["enhanced_url"],
            "rewrite_cols": {"text_url": lambda v, db="accounting": url_to_relative(v, "ai-accounting-db", db)},
        },
    ]

    for cfg in configs:
        shard_dir = os.path.join(DST, cfg["db"], "shards")
        count = 0
        for fname in sorted(os.listdir(shard_dir)):
            if not fname.startswith("shard-") or not fname.endswith(".txt"):
                continue
            fpath = os.path.join(shard_dir, fname)
            count += rewrite_tsv(fpath, cfg["drop_cols"], cfg["rewrite_cols"])
        print(f"  {cfg['db']}: {count} shard files rewritten")


def url_to_relative(url, old_db_name, new_db_name):
    """絶対URL → 相対パス (text/{code}/{id}.txt)"""
    # https://jplawdb.github.io/html-preview/ai-nta-qa-db/text/bunshokaito_gensen/1549-01.txt
    # → text/bunshokaito_gensen/1549-01.txt
    prefix = f"{OLD_BASE}{old_db_name}/"
    if url.startswith(prefix):
        return url[len(prefix):]
    return url  # 外部URLはそのまま


def rewrite_tsv(fpath, drop_cols, rewrite_cols):
    """TSVファイルの列を書き換え/削除"""
    with open(fpath, "r", encoding="utf-8") as f:
        lines = f.readlines()
    if not lines:
        return 0

    header = lines[0].rstrip("\n").split("\t")
    drop_indices = set()
    for col in drop_cols:
        if col in header:
            drop_indices.add(header.index(col))

    rewrite_indices = {}
    for col, func in rewrite_cols.items():
        if col in header:
            rewrite_indices[header.index(col)] = func

    new_lines = []
    # Rewrite header
    new_header = [h for i, h in enumerate(header) if i not in drop_indices]
    new_lines.append("\t".join(new_header) + "\n")

    # Rewrite data lines
    for line in lines[1:]:
        fields = line.rstrip("\n").split("\t")
        new_fields = []
        for i, field in enumerate(fields):
            if i in drop_indices:
                continue
            if i in rewrite_indices:
                field = rewrite_indices[i](field)
            new_fields.append(field)
        new_lines.append("\t".join(new_fields) + "\n")

    with open(fpath, "w", encoding="utf-8") as f:
        f.writelines(new_lines)
    return 1


def rewrite_paper_treaty_shards():
    """paper/treaty: core列の packs/ → text/ 置換"""
    targets = [
        ("paper/shards/oecd-tpg-2022", "shard-"),
        ("paper/shards/nta-tp-audit", "shard-"),
        ("paper/shards/oecd-beps", "shard-"),
        ("treaty/shards", "shard-"),
    ]

    for rel_dir, prefix in targets:
        shard_dir = os.path.join(DST, rel_dir)
        count = 0
        for fname in sorted(os.listdir(shard_dir)):
            if not fname.startswith(prefix) or not fname.endswith(".txt"):
                continue
            fpath = os.path.join(shard_dir, fname)
            with open(fpath, "r", encoding="utf-8") as f:
                content = f.read()
            # core列の packs/ → text/
            new_content = content.replace("\tpacks/", "\ttext/")
            if new_content != content:
                with open(fpath, "w", encoding="utf-8") as f:
                    f.write(new_content)
                count += 1
        print(f"  {rel_dir}: {count} shard files (packs→text)")

    # latin TSVも同様にcore列を書き換え
    for rel_dir in ["paper/shards/oecd-tpg-2022", "paper/shards/nta-tp-audit",
                     "paper/shards/oecd-beps", "treaty/shards"]:
        shard_dir = os.path.join(DST, rel_dir)
        count = 0
        for fname in sorted(os.listdir(shard_dir)):
            if not fname.startswith("latin-") or not fname.endswith(".tsv"):
                continue
            fpath = os.path.join(shard_dir, fname)
            with open(fpath, "r", encoding="utf-8") as f:
                content = f.read()
            new_content = content.replace("\tpacks/", "\ttext/")
            if new_content != content:
                with open(fpath, "w", encoding="utf-8") as f:
                    f.write(new_content)
                count += 1
        if count > 0:
            print(f"  {rel_dir}: {count} latin TSV files (packs→text)")


def rewrite_shards_index_json():
    """shards_index.json内のbase_url削除、fileパス修正"""
    targets = [
        "qa/shards/index.json",
        "treaty/shards/index.json",
        "accounting/shards/index.json",
    ]

    for rel_path in targets:
        fpath = os.path.join(DST, rel_path)
        if not os.path.exists(fpath):
            print(f"  SKIP: {rel_path} not found")
            continue

        with open(fpath, "r", encoding="utf-8") as f:
            data = json.load(f)

        # base_url削除
        if "base_url" in data:
            del data["base_url"]

        # enhanced関連の削除
        for key in list(data.keys()):
            if "enhanced" in key.lower():
                del data[key]

        # shards配列内のfileパス修正
        if "shards" in data:
            for shard in data["shards"]:
                if "file" in shard:
                    # "data/shards/shard-00.txt" → "shards/shard-00.txt"
                    shard["file"] = shard["file"].replace("data/shards/", "shards/")
                # URL列があれば削除
                for key in ["url", "enhanced_url"]:
                    if key in shard:
                        del shard[key]

        # docs配列内のURL修正
        if "docs" in data:
            for doc in data["docs"]:
                for key in list(doc.keys()):
                    if "enhanced" in key.lower() or "resolve_lite" in key.lower():
                        del doc[key]
                    elif key == "url" and isinstance(doc[key], str) and OLD_BASE in doc[key]:
                        del doc[key]

        with open(fpath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"  {rel_path}: rewritten")


def main():
    print("=== Step 3.5: shard/shards_index URL書き換え ===")
    print()
    print("[1] qa/guide/accounting shards (TSV URL → 相対パス, enhanced_url列削除)")
    rewrite_qa_guide_accounting_shards()
    print()
    print("[2] paper/treaty shards (packs/ → text/)")
    rewrite_paper_treaty_shards()
    print()
    print("[3] shards_index.json (base_url削除, fileパス修正)")
    rewrite_shards_index_json()
    print()
    print("=== 完了 ===")


if __name__ == "__main__":
    main()
