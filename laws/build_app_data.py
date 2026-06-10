#!/usr/bin/env python3
"""e-Gov法令XML → SPA用構造化JSON"""
import xml.etree.ElementTree as ET
import json, os

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "app", "data")
os.makedirs(OUT, exist_ok=True)

LAWS = [
    dict(slug="kokuzei_tsusoku", xml="/tmp/ntsuusoku.xml"),
    dict(slug="hojinzei",        xml="/tmp/houjinzei.xml"),
    dict(slug="shotokuzei",      xml="/tmp/shotokuzei.xml"),
    dict(slug="sozei_tokuso",    xml="/tmp/sotokuso.xml"),
]

def text_of(el):
    parts = []
    def walk(e):
        if e.tag == "Rt":
            return
        if e.text:
            parts.append(e.text)
        for c in e:
            walk(c)
            if c.tail:
                parts.append(c.tail)
    walk(el)
    return "".join(parts)

def sentences(parent):
    return "".join(text_of(s) for s in parent.findall(".//Sentence"))

def table(ts):
    rows = []
    for row in ts.findall(".//TableRow"):
        rows.append([text_of(c) for c in row.findall("TableColumn")])
    return rows

def items(parent, depth=1):
    tags = {1: "Item", 2: "Subitem1", 3: "Subitem2", 4: "Subitem3"}
    tag = tags.get(depth)
    if tag is None:
        return []
    out = []
    for it in parent.findall(tag):
        te = it.find(f"{tag}Title")
        se = it.find(f"{tag}Sentence")
        o = dict(t=text_of(te) if te is not None else "",
                 b=sentences(se) if se is not None else "")
        sub = items(it, depth + 1)
        if sub: o["sub"] = sub
        tb = [table(ts) for ts in it.findall("TableStruct")]
        if tb: o["tbl"] = tb
        out.append(o)
    return out

def article(art):
    num = art.get("Num", "")
    cap = art.find("ArticleCaption")
    ttl = art.find("ArticleTitle")
    paras = []
    for p in art.findall("Paragraph"):
        pn = p.find("ParagraphNum")
        ps = p.find("ParagraphSentence")
        o = dict(n=text_of(pn) if pn is not None else "",
                 b=sentences(ps) if ps is not None else "")
        its = items(p)
        if its: o["items"] = its
        tb = [table(ts) for ts in p.findall("TableStruct")]
        if tb: o["tbl"] = tb
        paras.append(o)
    return dict(id=num.replace(":", "_"),
                t=text_of(ttl) if ttl is not None else "",
                c=text_of(cap) if cap is not None else "",
                paras=paras)

for cfg in LAWS:
    tree = ET.parse(cfg["xml"])
    law = tree.getroot().find(".//Law")
    body = law.find("LawBody")
    title = text_of(body.find("LawTitle"))
    law_num = text_of(law.find("LawNum"))
    main = body.find("MainProvision")

    chapters = []
    def collect(ch, prefix=""):
        ti = ch.find("ChapterTitle")
        label = (prefix + text_of(ti)) if ti is not None else prefix
        secs = []
        direct = [article(a) for a in ch.findall("Article")]
        if direct:
            secs.append(dict(label="", arts=direct))
        for sec in ch.findall("Section"):
            sti = sec.find("SectionTitle")
            secs.append(dict(label=text_of(sti) if sti is not None else "",
                             arts=[article(a) for a in sec.findall(".//Article")]))
        chapters.append(dict(label=label, secs=secs))

    parts = main.findall("Part")
    if parts:
        for pt in parts:
            pl = text_of(pt.find("PartTitle"))
            for ch in pt.findall("Chapter"):
                collect(ch, prefix=pl + "　")
    else:
        for ch in main.findall("Chapter"):
            collect(ch)
        if not chapters:
            chapters.append(dict(label=title, secs=[dict(label="", arts=[article(a) for a in main.findall(".//Article")])]))

    data = dict(slug=cfg["slug"], title=title, num=law_num, chapters=chapters)
    fn = os.path.join(OUT, cfg["slug"] + ".json")
    with open(fn, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    n = sum(len(s["arts"]) for c in chapters for s in c["secs"])
    print(f"{title}: {len(chapters)}章 {n}条 -> {os.path.getsize(fn)//1024}KB")

# 法令一覧メタ
meta = []
for cfg in LAWS:
    d = json.load(open(os.path.join(OUT, cfg["slug"] + ".json"), encoding="utf-8"))
    meta.append(dict(slug=d["slug"], title=d["title"], num=d["num"],
                     chapters=len(d["chapters"]),
                     articles=sum(len(s["arts"]) for c in d["chapters"] for s in c["secs"])))
json.dump(meta, open(os.path.join(OUT, "meta.json"), "w", encoding="utf-8"), ensure_ascii=False)
print("meta.json done")
