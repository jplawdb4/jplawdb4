#!/usr/bin/env python3
"""e-Gov法令XML → Lawzilla風静的条文サイトジェネレーター"""
import xml.etree.ElementTree as ET
import html as H
import os, re, json

OUT = os.path.dirname(os.path.abspath(__file__))

LAWS = [
    dict(slug="kokuzei_tsusoku", xml="/tmp/ntsuusoku.xml",  note=""),
    dict(slug="hojinzei",        xml="/tmp/houjinzei.xml",  note=""),
    dict(slug="shotokuzei",      xml="/tmp/shotokuzei.xml", note=""),
    dict(slug="sozei_tokuso",    xml="/tmp/sotokuso.xml",   note=""),
]

KANSUJI = "〇一二三四五六七八九"

def text_of(el):
    """Sentence等のテキスト抽出（ルビのRtは除外）"""
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

def render_sentences(parent, cls=""):
    out = []
    for s in parent.findall(".//Sentence"):
        out.append(H.escape(text_of(s)))
    return "".join(out)

def render_table(ts):
    rows = []
    for row in ts.findall(".//TableRow"):
        cells = []
        for col in row.findall("TableColumn"):
            cells.append(f"<td>{H.escape(text_of(col))}</td>")
        rows.append("<tr>" + "".join(cells) + "</tr>")
    if not rows:
        return ""
    return '<div class="tbl-wrap"><table class="law-tbl">' + "".join(rows) + "</table></div>"

def render_items(parent, depth=1):
    """号・イロハ等の再帰描画"""
    tags = {1: "Item", 2: "Subitem1", 3: "Subitem2", 4: "Subitem3"}
    tag = tags.get(depth)
    if tag is None:
        return ""
    out = []
    for it in parent.findall(tag):
        title_el = it.find(f"{tag}Title")
        sent_el = it.find(f"{tag}Sentence")
        title = H.escape(text_of(title_el)) if title_el is not None else ""
        body = ""
        if sent_el is not None:
            body = "".join(H.escape(text_of(s)) for s in sent_el.findall(".//Sentence"))
        sub = render_items(it, depth + 1)
        tbls = "".join(render_table(ts) for ts in it.findall("TableStruct"))
        out.append(f'<div class="item d{depth}"><span class="it">{title}</span><span class="ib">{body}</span>{sub}{tbls}</div>')
    return "".join(out)

def render_article(art):
    num = art.get("Num", "")
    cap_el = art.find("ArticleCaption")
    ttl_el = art.find("ArticleTitle")
    cap = H.escape(text_of(cap_el)) if cap_el is not None else ""
    ttl = H.escape(text_of(ttl_el)) if ttl_el is not None else ""
    aid = "a" + num.replace(":", "-")
    paras = []
    for p in art.findall("Paragraph"):
        pnum_el = p.find("ParagraphNum")
        pnum = text_of(pnum_el) if pnum_el is not None else ""
        ps = p.find("ParagraphSentence")
        body = ""
        if ps is not None:
            body = "".join(H.escape(text_of(s)) for s in ps.findall(".//Sentence"))
        items = render_items(p)
        tbls = "".join(render_table(ts) for ts in p.findall("TableStruct"))
        badge = f'<span class="pn">{H.escape(pnum)}</span>' if pnum else '<span class="pn pn1">1</span>'
        paras.append(f'<div class="para">{badge}<div class="pb">{body}{items}{tbls}</div></div>')
    return aid, ttl, cap, f'''<article class="art" id="{aid}">
<div class="art-h"><span class="art-t">{ttl}</span>{f'<span class="art-c">{cap}</span>' if cap else ""}</div>
{"".join(paras)}
</article>'''

CSS = """
:root{--zil-bg:#f5f7f3;--surface:#fff;--p:#00AEC6;--pd:#008193;--p25:#E8FCFF;--navy:#005870;
--ink:#121921;--ink2:#4d5a66;--ink3:#8b97a1;--line:#e3e8e2;--line2:#cfd7d0;--yellow:#FCE48A;
--sans:"Noto Sans JP","Hiragino Sans",sans-serif}
*{box-sizing:border-box}
body{margin:0;background:var(--zil-bg);color:var(--ink);font-family:var(--sans);font-size:15px;line-height:1.95;-webkit-font-smoothing:antialiased}
::selection{background:rgba(0,174,198,.18)}
a{color:var(--pd)}
.topbar{background:var(--surface);border-bottom:1px solid var(--line);box-shadow:0 1px 4px rgba(18,25,33,.06);position:sticky;top:0;z-index:20}
.topbar-in{width:min(100%,1240px);margin:0 auto;padding:10px 20px;display:flex;align-items:center;gap:14px}
.brand{display:flex;align-items:center;gap:9px;font-weight:900;font-size:.98rem;color:var(--navy);text-decoration:none;letter-spacing:.04em}
.brand .mark{width:28px;height:28px;border-radius:7px;background:linear-gradient(135deg,var(--p),var(--pd));color:#fff;font-size:.78rem;font-weight:900;display:inline-flex;align-items:center;justify-content:center}
.brand small{font-size:.68rem;font-weight:500;color:var(--ink3)}
.search{margin-left:auto;display:flex;align-items:center;gap:8px}
.search input{font-family:var(--sans);font-size:.82rem;border:1px solid var(--line2);border-radius:7px;padding:7px 12px;width:240px;background:var(--zil-bg);outline:none;transition:border-color .2s}
.search input:focus{border-color:var(--p);background:#fff}
.wrap{width:min(100%,1240px);margin:0 auto;display:grid;grid-template-columns:280px 1fr;gap:0;align-items:start}
.side{position:sticky;top:53px;height:calc(100vh - 53px);overflow-y:auto;padding:18px 8px 40px 20px;border-right:1px solid var(--line);background:rgba(255,255,255,.5)}
.side h2{font-size:.72rem;font-weight:700;letter-spacing:.2em;text-transform:uppercase;color:var(--ink3);margin:14px 0 6px}
.side a{display:block;font-size:.8rem;color:var(--ink2);text-decoration:none;padding:3px 10px;border-radius:6px;line-height:1.6}
.side a:hover{background:var(--p25);color:var(--pd)}
.side a.ch{font-weight:700;color:var(--navy);margin-top:6px}
.side a.cur{background:var(--p25);color:var(--pd);font-weight:700}
.side .sec-l{padding-left:22px}
.side .art-l{padding-left:34px;font-size:.76rem}
.content{padding:26px 26px 100px 34px;min-width:0}
.law-head{margin:0 0 6px}
.law-head h1{font-size:1.5rem;font-weight:900;color:var(--navy);margin:0;line-height:1.5}
.law-head .num{font-size:.78rem;color:var(--ink3);font-weight:500}
.crumb{font-size:.74rem;color:var(--ink3);margin:10px 0 18px}
.crumb a{color:var(--pd);text-decoration:none}
.ch-title{display:flex;align-items:center;gap:10px;margin:34px 0 14px}
.ch-title::before{content:"";width:5px;height:22px;border-radius:3px;background:var(--p)}
.ch-title h2{font-size:1.1rem;font-weight:900;color:var(--navy);margin:0}
.sec-title{font-size:.95rem;font-weight:700;color:var(--pd);margin:26px 0 8px;border-bottom:2px solid var(--p25);padding-bottom:4px}
.art{background:var(--surface);border:1px solid var(--line);border-radius:10px;padding:18px 22px 16px;margin:0 0 12px;scroll-margin-top:70px}
.art:target{border-color:var(--p);box-shadow:0 0 0 3px rgba(0,174,198,.15)}
.art-h{display:flex;align-items:baseline;gap:10px;flex-wrap:wrap;margin-bottom:8px}
.art-t{font-weight:900;color:var(--navy);font-size:1rem}
.art-c{font-size:.8rem;color:var(--pd);background:var(--p25);border-radius:4px;padding:1px 10px;font-weight:700}
.para{display:flex;gap:10px;margin:7px 0}
.pn{flex:0 0 auto;min-width:24px;height:24px;border-radius:6px;background:var(--zil-bg);border:1px solid var(--line);color:var(--ink2);font-size:.72rem;font-weight:700;display:flex;align-items:center;justify-content:center;margin-top:5px;padding:0 5px}
.pn1{opacity:.35}
.pb{font-size:.9rem;min-width:0}
.item{display:flex;gap:8px;margin:4px 0;font-size:.88rem;flex-wrap:wrap}
.item .it{flex:0 0 auto;font-weight:700;color:var(--pd);min-width:1.4em}
.item .ib{flex:1;min-width:16em}
.item.d2{margin-left:1.6em}.item.d3{margin-left:3.2em}.item.d4{margin-left:4.8em}
.item.d2 .it,.item.d3 .it,.item.d4 .it{color:var(--ink2)}
.tbl-wrap{overflow-x:auto;margin:8px 0;width:100%}
.law-tbl{border-collapse:collapse;font-size:.8rem;background:#fff}
.law-tbl td{border:1px solid var(--line2);padding:6px 10px;vertical-align:top;min-width:6em}
.pager{display:flex;justify-content:space-between;gap:12px;margin:34px 0 0}
.pager a{flex:1;background:var(--surface);border:1px solid var(--line);border-radius:10px;padding:14px 18px;text-decoration:none;color:var(--navy);font-weight:700;font-size:.86rem;transition:border-color .2s,box-shadow .2s}
.pager a:hover{border-color:var(--p);box-shadow:0 6px 18px -10px rgba(0,88,112,.3)}
.pager a.next{text-align:right}
.pager .dir{display:block;font-size:.66rem;font-weight:700;letter-spacing:.18em;color:var(--ink3);text-transform:uppercase}
.note{font-size:.76rem;color:var(--ink3);margin-top:26px;border-top:1px dashed var(--line2);padding-top:12px}
.hit{background:var(--yellow)}
.hidden{display:none}
.menu-btn{display:none}
@media(max-width:900px){
.wrap{grid-template-columns:1fr}
.side{position:fixed;left:0;top:53px;width:min(82vw,300px);background:#fff;z-index:15;transform:translateX(-105%);transition:transform .25s;box-shadow:8px 0 30px rgba(0,0,0,.12)}
.side.open{transform:none}
.menu-btn{display:inline-flex;align-items:center;justify-content:center;border:1px solid var(--line2);background:#fff;border-radius:7px;width:34px;height:34px;font-size:1rem;color:var(--navy);cursor:pointer}
.search input{width:130px}
.content{padding:20px 16px 80px}
}
"""

JS = """
const q=document.getElementById('q');
if(q){q.addEventListener('input',()=>{
const v=q.value.trim();
document.querySelectorAll('.art').forEach(a=>{
if(!v){a.classList.remove('hidden');return}
a.classList.toggle('hidden',!a.textContent.includes(v));
});
});}
const mb=document.getElementById('menu');
if(mb){mb.addEventListener('click',()=>document.getElementById('side').classList.toggle('open'));}
"""

def page(title, law_title, law_num, side_html, body_html, root_rel="../"):
    return f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{H.escape(title)} | JPLAWDB4 条文ライブラリ</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Noto+Sans+JP:wght@400;500;700;900&display=swap" rel="stylesheet">
<style>{CSS}</style>
</head>
<body>
<nav class="topbar"><div class="topbar-in">
<button class="menu-btn" id="menu">☰</button>
<a class="brand" href="{root_rel}index.html"><span class="mark">法</span>JPLAWDB4<small>｜条文ライブラリ</small></a>
<div class="search"><input id="q" type="search" placeholder="このページ内の条文を検索…"></div>
</div></nav>
<div class="wrap">
<aside class="side" id="side">{side_html}</aside>
<main class="content">{body_html}</main>
</div>
<script>{JS}</script>
</body>
</html>"""

def build_law(cfg):
    tree = ET.parse(cfg["xml"])
    law = tree.getroot().find(".//Law")
    law_num = text_of(law.find("LawNum"))
    body = law.find("LawBody")
    title = text_of(body.find("LawTitle"))
    main = body.find("MainProvision")

    # 章の列挙（編がある場合は編→章）
    chapters = []  # (label, [(seclabel|None, [articles])])
    def collect_chapter(ch, prefix=""):
        ti = ch.find("ChapterTitle")
        label = (prefix + text_of(ti)) if ti is not None else prefix
        groups = []
        direct = ch.findall("Article")
        if direct:
            groups.append((None, direct))
        for sec in ch.findall("Section"):
            sti = sec.find("SectionTitle")
            slabel = text_of(sti) if sti is not None else ""
            arts = sec.findall(".//Article")
            groups.append((slabel, arts))
        chapters.append((label, groups))

    parts = main.findall("Part")
    if parts:
        for pt in parts:
            pl = text_of(pt.find("PartTitle"))
            for ch in pt.findall("Chapter"):
                collect_chapter(ch, prefix=pl + "　")
    else:
        for ch in main.findall("Chapter"):
            collect_chapter(ch)
        if not chapters:
            arts = main.findall(".//Article")
            chapters.append((title, [(None, arts)]))

    slug = cfg["slug"]
    outdir = os.path.join(OUT, slug)
    os.makedirs(outdir, exist_ok=True)
    npages = len(chapters)

    # サイドバー（全章リンク）生成関数
    def sidebar(cur_idx, art_anchors):
        out = ['<h2>章</h2>']
        for i, (label, _) in enumerate(chapters):
            cls = "ch cur" if i == cur_idx else "ch"
            out.append(f'<a class="{cls}" href="ch{i+1:02d}.html">{H.escape(label)}</a>')
            if i == cur_idx:
                for aid, attl, acap in art_anchors:
                    nm = f"{attl}{('　' + acap) if acap else ''}"
                    out.append(f'<a class="art-l" href="#{aid}">{H.escape(nm[:30])}</a>')
        return "".join(out)

    chmeta = []
    for i, (label, groups) in enumerate(chapters):
        anchors = []
        chunks = [f'<div class="law-head"><h1>{H.escape(title)}</h1><span class="num">{H.escape(law_num)}</span></div>',
                  f'<div class="crumb"><a href="../index.html">条文ライブラリ</a> › <a href="index.html">{H.escape(title)}</a> › {H.escape(label)}</div>',
                  f'<div class="ch-title"><h2>{H.escape(label)}</h2></div>']
        n_arts = 0
        for slabel, arts in groups:
            if slabel:
                chunks.append(f'<div class="sec-title">{H.escape(slabel)}</div>')
            for art in arts:
                aid, attl, acap, htmlart = render_article(art)
                anchors.append((aid, attl, acap))
                chunks.append(htmlart)
                n_arts += 1
        pager = ['<div class="pager">']
        if i > 0:
            pager.append(f'<a href="ch{i:02d}.html"><span class="dir">← 前の章</span>{H.escape(chapters[i-1][0])}</a>')
        if i < npages - 1:
            pager.append(f'<a class="next" href="ch{i+2:02d}.html"><span class="dir">次の章 →</span>{H.escape(chapters[i+1][0])}</a>')
        pager.append('</div>')
        chunks.append("".join(pager))
        chunks.append('<div class="note">出典: e-Gov法令検索（法令API）。本則のみ掲載（附則・別表は省略）。表示は機械変換によるもので、正文は官報・e-Govを確認してください。</div>')
        htmlpage = page(f"{title} {label}", title, law_num, sidebar(i, anchors), "".join(chunks))
        with open(os.path.join(outdir, f"ch{i+1:02d}.html"), "w", encoding="utf-8") as f:
            f.write(htmlpage)
        chmeta.append(dict(label=label, file=f"ch{i+1:02d}.html", arts=n_arts))

    # 法令トップ（目次）
    toc = [f'<div class="law-head"><h1>{H.escape(title)}</h1><span class="num">{H.escape(law_num)}</span></div>',
           f'<div class="crumb"><a href="../index.html">条文ライブラリ</a> › {H.escape(title)}</div>',
           '<div class="ch-title"><h2>目次</h2></div>']
    for m in chmeta:
        toc.append(f'<article class="art"><div class="art-h"><span class="art-t"><a href="{m["file"]}" style="text-decoration:none;color:inherit">{H.escape(m["label"])}</a></span><span class="art-c">{m["arts"]}条</span></div></article>')
    toc.append('<div class="note">出典: e-Gov法令検索（法令API）。本則のみ掲載（附則・別表は省略）。</div>')
    side = '<h2>章</h2>' + "".join(f'<a class="ch" href="{m["file"]}">{H.escape(m["label"])}</a>' for m in chmeta)
    with open(os.path.join(outdir, "index.html"), "w", encoding="utf-8") as f:
        f.write(page(title, title, law_num, side, "".join(toc)))

    total = sum(m["arts"] for m in chmeta)
    print(f"{title}: {npages}章 {total}条")
    return dict(slug=slug, title=title, num=law_num, chapters=npages, articles=total)

meta = [build_law(c) for c in LAWS]
with open(os.path.join(OUT, "laws.json"), "w", encoding="utf-8") as f:
    json.dump(meta, f, ensure_ascii=False, indent=1)
print("done")
