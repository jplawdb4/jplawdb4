#!/usr/bin/env python3
"""
Lean shard generator for jplawdb4.
Removes redundant/garbage fields from all shard files.
Generates topic routing data for quickstart updates.
"""
import os, json, re, glob, math
from collections import defaultdict, Counter

try:
    import tiktoken
    enc = tiktoken.get_encoding("cl100k_base")
    def tok(text): return len(enc.encode(text))
except ImportError:
    def tok(text): return len(text.encode()) // 3  # rough fallback

BASE = os.path.dirname(os.path.abspath(__file__))
MAX_TOK = 9500  # margin below 10K

def extract_topics(texts, top_n=8):
    stop = {'の','に','を','は','が','と','で','た','て','する','から','まで',
            'について','における','による','された','される','および','又は',
            'ある','その','この','もの','こと','ため','等','及び','場合','規定',
            '関する','係る','対する','取扱','課税','所得','税額','金額','法人税',
            '所得税','消費税','相続税','について','における','に関する','に係る'}
    freq = Counter()
    for t in texts:
        for w in re.findall(r'[\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ff]{2,6}', t):
            if w not in stop: freq[w] += 1
    return [w for w,_ in freq.most_common(top_n)]

def split_write(records, outdir, header, prefix="shard"):
    """Split records into ≤MAX_TOK shard files. Returns list of (filename, records)."""
    shards = []
    cur, cur_tok = [], tok(header + "\n")
    for r in records:
        rt = tok(r + "\n")
        if cur and cur_tok + rt > MAX_TOK:
            shards.append(cur)
            cur, cur_tok = [], tok(header + "\n")
        cur.append(r)
        cur_tok += rt
    if cur: shards.append(cur)

    written = []
    fmt = f"{{:0{max(2, len(str(len(shards)-1)))}d}}"
    for i, shard in enumerate(shards):
        fname = f"{prefix}-{fmt.format(i)}.txt"
        path = os.path.join(outdir, fname)
        with open(path, "w") as f:
            f.write(header + "\n" + "\n".join(shard) + "\n")
        written.append((fname, shard))
    return written

def remove_old_shards(directory):
    """Remove shard-*.txt files only (preserve index.json, latin-*)."""
    for f in os.listdir(directory):
        if f.startswith("shard-") and f.endswith(".txt"):
            os.remove(os.path.join(directory, f))

# ========== QA ==========
def process_qa():
    print("=== qa ===")
    d = os.path.join(BASE, "qa/shards")
    recs = defaultdict(list)
    for f in sorted(glob.glob(os.path.join(d, "shard-*.txt"))):
        with open(f) as fh:
            for i, line in enumerate(fh):
                if i == 0: continue
                p = line.strip().split("\t")
                if len(p) < 6: continue
                recs[p[2]].append((p[4], f"{p[2]}\t{p[4]}\t{p[5]}"))

    # Sort by doc_code then item_id
    ordered = []
    for dc in sorted(recs):
        for item_id, rec in sorted(recs[dc], key=lambda x: x[0]):
            ordered.append(rec)

    remove_old_shards(d)
    header = "doc_code\titem_id\titem_title"
    written = split_write(ordered, d, header)

    routing = []
    for fname, shard in written:
        dcs = sorted(set(r.split("\t")[0] for r in shard))
        titles = [r.split("\t")[2] for r in shard]
        topics = extract_topics(titles)
        routing.append({"shard": fname, "count": len(shard),
                        "doc_codes": dcs, "topics": topics})

    print(f"  {len(ordered)} records → {len(written)} shards")
    return routing

# ========== HANKETSU/HOUJINZEI (keep as-is, extract topics only) ==========
def process_houjinzei():
    print("=== hanketsu/houjinzei ===")
    d = os.path.join(BASE, "hanketsu/shards/houjinzei")
    routing = []
    total = 0
    for f in sorted(glob.glob(os.path.join(d, "shard-*.txt"))):
        recs = []
        with open(f) as fh:
            for i, line in enumerate(fh):
                if i == 0: continue
                recs.append(line.strip())
        total += len(recs)
        kws = [r.split("\t")[5] for r in recs if len(r.split("\t")) > 5 and r.split("\t")[5]]
        topics = extract_topics(kws)
        ids = [r.split("\t")[0] for r in recs if r]
        routing.append({"shard": os.path.basename(f), "count": len(recs),
                        "id_range": f"{ids[0]}〜{ids[-1]}" if ids else "",
                        "topics": topics})
    print(f"  {total} records, {len(routing)} shards (unchanged)")
    return routing

# ========== SAIKETSU (lean for quickstart embed) ==========
def process_saiketsu():
    print("=== saiketsu ===")
    d = os.path.join(BASE, "hanketsu/shards/saiketsu")
    recs = []
    for f in sorted(glob.glob(os.path.join(d, "shard-*.txt"))):
        with open(f) as fh:
            for i, line in enumerate(fh):
                if i == 0: continue
                p = line.strip().split("\t")
                if len(p) < 4: continue
                recs.append(f"{p[0]}\t{p[2]}\t{p[3]}")

    # Don't delete old shards yet (quickstart embed is separate step)
    lean_text = "id\tdate_iso\ttax_types\n" + "\n".join(recs)
    lean_tok = tok(lean_text)
    print(f"  {len(recs)} records, {lean_tok} tok (will embed in quickstart)")
    return recs, lean_text

# ========== TREATY ==========
def process_treaty():
    print("=== treaty ===")
    d = os.path.join(BASE, "treaty/shards")
    recs = []
    for f in sorted(glob.glob(os.path.join(d, "shard-*.txt"))):
        with open(f) as fh:
            for i, line in enumerate(fh):
                if i == 0: continue
                p = line.strip().split("\t")
                if len(p) < 6: continue
                # keep: id, pid, snippet, core (drop page_start, page_end)
                recs.append(f"{p[0]}\t{p[1]}\t{p[4]}\t{p[5]}")

    remove_old_shards(d)
    header = "id\tpid\tsnippet\tcore"
    written = split_write(recs, d, header)

    routing = []
    for fname, shard in written:
        countries = sorted(set(r.split("\t")[0].split("--")[0] for r in shard if "--" in r.split("\t")[0]))
        snippets = [r.split("\t")[2] for r in shard]
        topics = extract_topics(snippets)
        routing.append({"shard": fname, "count": len(shard),
                        "countries": countries, "topics": topics})

    print(f"  {len(recs)} records → {len(written)} shards")
    return routing

# ========== ACCOUNTING ==========
def process_accounting():
    print("=== accounting ===")
    d = os.path.join(BASE, "accounting/shards")
    recs = []
    for f in sorted(glob.glob(os.path.join(d, "shard-*.txt"))):
        with open(f) as fh:
            for i, line in enumerate(fh):
                if i == 0: continue
                p = line.strip().split("\t")
                if len(p) < 3: continue
                # keep: doc_code, item_id, title (drop snippet, text_url)
                recs.append(f"{p[0]}\t{p[1]}\t{p[2]}")

    remove_old_shards(d)
    header = "doc_code\titem_id\ttitle"
    written = split_write(recs, d, header)

    routing = []
    for fname, shard in written:
        dcs = sorted(set(r.split("\t")[0] for r in shard))
        titles = [r.split("\t")[2] for r in shard]
        topics = extract_topics(titles)
        routing.append({"shard": fname, "count": len(shard),
                        "doc_codes": dcs, "topics": topics})

    print(f"  {len(recs)} records → {len(written)} shards")
    return routing

# ========== GUIDE ==========
def process_guide():
    print("=== guide ===")
    d = os.path.join(BASE, "guide/shards")
    recs = []
    for f in sorted(glob.glob(os.path.join(d, "shard-*.txt"))):
        with open(f) as fh:
            for i, line in enumerate(fh):
                if i == 0: continue
                p = line.strip().split("\t")
                if len(p) < 3: continue
                recs.append(f"{p[0]}\t{p[1]}\t{p[2]}")

    remove_old_shards(d)
    header = "doc_code\titem_id\ttitle"
    written = split_write(recs, d, header)

    routing = []
    for fname, shard in written:
        dcs = sorted(set(r.split("\t")[0] for r in shard))
        titles = [r.split("\t")[2] for r in shard]
        topics = extract_topics(titles)
        routing.append({"shard": fname, "count": len(shard),
                        "doc_codes": dcs, "topics": topics})

    print(f"  {len(recs)} records → {len(written)} shards")
    return routing

# ========== PAPER ==========
def process_paper():
    print("=== paper ===")
    all_routing = {}
    for sub in ["oecd-beps", "oecd-tpg-2022", "nta-tp-audit"]:
        d = os.path.join(BASE, f"paper/shards/{sub}")
        if not os.path.isdir(d): continue

        recs = []
        for f in sorted(glob.glob(os.path.join(d, "shard-*.txt"))):
            with open(f) as fh:
                for i, line in enumerate(fh):
                    if i == 0: continue
                    p = line.strip().split("\t")
                    if len(p) < 6: continue
                    recs.append(f"{p[0]}\t{p[1]}\t{p[4]}\t{p[5]}")

        remove_old_shards(d)
        header = "id\tpid\tsnippet\tcore"
        written = split_write(recs, d, header)

        routing = []
        for fname, shard in written:
            snippets = [r.split("\t")[2] for r in shard]
            topics = extract_topics(snippets)
            routing.append({"shard": fname, "count": len(shard), "topics": topics})

        all_routing[sub] = routing
        print(f"  {sub}: {len(recs)} records → {len(written)} shards")

    return all_routing

# ========== MAIN ==========
if __name__ == "__main__":
    result = {
        "qa": process_qa(),
        "hanketsu_houjinzei": process_houjinzei(),
        "treaty": process_treaty(),
        "accounting": process_accounting(),
        "guide": process_guide(),
        "paper": process_paper(),
    }
    sk_recs, sk_text = process_saiketsu()
    result["saiketsu_embed"] = sk_text
    result["saiketsu_count"] = len(sk_recs)

    out = os.path.join(BASE, "_shard_routing.json")
    with open(out, "w") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"\nRouting data → {out}")
