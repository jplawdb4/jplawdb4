#!/usr/bin/env python3
"""Update all quickstart.txt files with lean shard routing tables."""
import os, json

BASE = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(BASE, "_shard_routing.json")) as f:
    R = json.load(f)

MARKER_START = "\n## Shard Routing (lean)\n"

def append_routing(qs_path, routing_text):
    """Append routing table to quickstart, replacing old one if exists."""
    with open(qs_path) as f:
        content = f.read()
    # Remove old routing section if present
    if MARKER_START.strip() in content:
        idx = content.index(MARKER_START.strip())
        content = content[:idx].rstrip()
    content = content.rstrip() + "\n" + MARKER_TEXT_SEP + routing_text + "\n"
    with open(qs_path, "w") as f:
        f.write(content)

ROUTING_TEXT_SEP = MARKER_START

# ========== QA ==========
def update_qa():
    lines = [MARKER_START.strip(), "",
             "以下のリーンshardはゴミフィールド除去済み（snippet/id/source_kind/doc_title/source_url削除）。",
             "フィールド: doc_code, item_id, item_title",
             "パス導出: qa/text/{doc_code}/{item_id}.txt",
             ""]
    for s in R["qa"]:
        dcs = ", ".join(s["doc_codes"])
        topics = ", ".join(s["topics"])
        lines.append(f'{s["shard"]} ({s["count"]}件): {dcs}')
        lines.append(f'  Topics: {topics}')
    text = "\n".join(lines)
    qs = os.path.join(BASE, "qa/quickstart.txt")
    with open(qs) as f:
        content = f.read()
    if MARKER_START.strip() in content:
        content = content[:content.index(MARKER_START.strip())].rstrip()
    with open(qs, "w") as f:
        f.write(content + "\n\n" + text + "\n")
    print(f"  qa/quickstart.txt updated ({len(R['qa'])} shards)")

# ========== HANKETSU (houjinzei + saiketsu embed) ==========
def update_hanketsu():
    lines = [MARKER_START.strip(), "",
             "### houjinzei shards（変更なし）",
             "フィールド: id, date, court, title, topics, keywords, laws",
             "パス導出: hanketsu/text/houjinzei/{id}.txt",
             ""]
    for s in R["hanketsu_houjinzei"]:
        topics = ", ".join(s["topics"])
        lines.append(f'{s["shard"]} ({s["count"]}件): ID {s["id_range"]}')
        lines.append(f'  Topics: {topics}')

    lines.append("")
    lines.append("### saiketsu 全件インデックス（shard廃止・quickstart内蔵）")
    lines.append(f"全{R['saiketsu_count']}件。パス導出: hanketsu/text/saiketsu/{{id}}.txt")
    lines.append("")
    lines.append(R["saiketsu_embed"])

    text = "\n".join(lines)
    qs = os.path.join(BASE, "hanketsu/quickstart.txt")
    with open(qs) as f:
        content = f.read()
    if MARKER_START.strip() in content:
        content = content[:content.index(MARKER_START.strip())].rstrip()
    with open(qs, "w") as f:
        f.write(content + "\n\n" + text + "\n")
    print(f"  hanketsu/quickstart.txt updated (houjinzei {len(R['hanketsu_houjinzei'])} shards + saiketsu {R['saiketsu_count']} embedded)")

# ========== TREATY ==========
def update_treaty():
    lines = [MARKER_START.strip(), "",
             "リーンshard（page_start/page_end除去）。",
             "フィールド: id, pid, snippet, core",
             ""]
    for s in R["treaty"]:
        countries = ", ".join(s["countries"][:5])
        if len(s["countries"]) > 5:
            countries += f" 他{len(s['countries'])-5}か国"
        topics = ", ".join(s["topics"])
        lines.append(f'{s["shard"]} ({s["count"]}件): {countries}')
        lines.append(f'  Topics: {topics}')
    text = "\n".join(lines)
    qs = os.path.join(BASE, "treaty/quickstart.txt")
    with open(qs) as f:
        content = f.read()
    if MARKER_START.strip() in content:
        content = content[:content.index(MARKER_START.strip())].rstrip()
    with open(qs, "w") as f:
        f.write(content + "\n\n" + text + "\n")
    print(f"  treaty/quickstart.txt updated ({len(R['treaty'])} shards)")

# ========== ACCOUNTING ==========
def update_accounting():
    lines = [MARKER_START.strip(), "",
             "リーンshard（snippet/text_url除去）。",
             "フィールド: doc_code, item_id, title",
             "パス導出: accounting/text/{doc_code}/{item_id}.txt",
             ""]
    for s in R["accounting"]:
        dcs = ", ".join(s["doc_codes"])
        topics = ", ".join(s["topics"])
        lines.append(f'{s["shard"]} ({s["count"]}件): {dcs}')
        lines.append(f'  Topics: {topics}')
    text = "\n".join(lines)
    qs = os.path.join(BASE, "accounting/quickstart.txt")
    with open(qs) as f:
        content = f.read()
    if MARKER_START.strip() in content:
        content = content[:content.index(MARKER_START.strip())].rstrip()
    with open(qs, "w") as f:
        f.write(content + "\n\n" + text + "\n")
    print(f"  accounting/quickstart.txt updated ({len(R['accounting'])} shards)")

# ========== GUIDE ==========
def update_guide():
    lines = [MARKER_START.strip(), "",
             "リーンshard（snippet除去）。",
             "フィールド: doc_code, item_id, title",
             "パス導出: guide/text/{doc_code}/{item_id}.txt",
             ""]
    for s in R["guide"]:
        dcs = ", ".join(s["doc_codes"])
        topics = ", ".join(s["topics"])
        lines.append(f'{s["shard"]} ({s["count"]}件): {dcs}')
        lines.append(f'  Topics: {topics}')
    text = "\n".join(lines)
    qs = os.path.join(BASE, "guide/quickstart.txt")
    with open(qs) as f:
        content = f.read()
    if MARKER_START.strip() in content:
        content = content[:content.index(MARKER_START.strip())].rstrip()
    with open(qs, "w") as f:
        f.write(content + "\n\n" + text + "\n")
    print(f"  guide/quickstart.txt updated ({len(R['guide'])} shards)")

# ========== PAPER ==========
def update_paper():
    lines = [MARKER_START.strip(), "",
             "リーンshard（page_start/page_end除去）。",
             "フィールド: id, pid, snippet, core",
             ""]
    for sub in ["oecd-beps", "oecd-tpg-2022", "nta-tp-audit"]:
        if sub not in R["paper"]: continue
        lines.append(f"### {sub}")
        for s in R["paper"][sub]:
            topics = ", ".join(s["topics"])
            lines.append(f'  {s["shard"]} ({s["count"]}件): {topics}')
        lines.append("")
    text = "\n".join(lines)
    qs = os.path.join(BASE, "paper/quickstart.txt")
    with open(qs) as f:
        content = f.read()
    if MARKER_START.strip() in content:
        content = content[:content.index(MARKER_START.strip())].rstrip()
    with open(qs, "w") as f:
        f.write(content + "\n\n" + text + "\n")
    total = sum(len(R["paper"][s]) for s in R["paper"])
    print(f"  paper/quickstart.txt updated ({total} shards)")

if __name__ == "__main__":
    print("=== Updating quickstart.txt files ===")
    update_qa()
    update_hanketsu()
    update_treaty()
    update_accounting()
    update_guide()
    update_paper()
    print("\nDone.")
