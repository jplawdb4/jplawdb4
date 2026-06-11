#!/usr/bin/env python3
"""CFC縦串ビルダー v3: 法・施行令・施行規則・通達の4層ノードモデル
- 4層すべて同一のノードアドレス体系（ln66_6.1.1 / 通達は ln66_6-1）
- 参照解決: 漢数字＋算用数字、措置法/措置法令/措置法規則の略称、イロハ枝、通達相互参照
- 委任文言（政令で定める/財務省令で定める）の検出とマーキング
- 4層横断の逆参照（縦串パネルの基盤）
"""
import xml.etree.ElementTree as ET
import json, os, re, html as H

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "app", "data2")
os.makedirs(OUT, exist_ok=True)

KD = {'〇':0,'一':1,'二':2,'三':3,'四':4,'五':5,'六':6,'七':7,'八':8,'九':9}
IROHA = "イロハニホヘトチリヌルヲワカヨタレソツネナラムウヰノオクヤマケフコエテ"
ZEN = str.maketrans('０１２３４５６７８９', '0123456789')

def kanji2int(s):
    s = s.translate(ZEN)
    if s.isdigit():
        return int(s)
    total = 0; cur = 0
    for ch in s:
        if ch == '千': total += (cur or 1) * 1000; cur = 0
        elif ch == '百': total += (cur or 1) * 100; cur = 0
        elif ch == '十': total += (cur or 1) * 10; cur = 0
        elif ch in KD: cur = KD[ch]
        else: return None
    return total + cur

def kanji_branch(s):
    parts = s.split('の')
    nums = [kanji2int(p) for p in parts]
    if any(n is None for n in nums): return None
    return '_'.join(str(n) for n in nums)

def text_of(el):
    parts = []
    def walk(e):
        if e.tag == 'Rt': return
        if e.text: parts.append(e.text)
        for c in e:
            walk(c)
            if c.tail: parts.append(c.tail)
    walk(el)
    return ''.join(parts)

def sentences(parent):
    return ''.join(text_of(s) for s in parent.findall('.//Sentence'))

# ---------- XML → フラットノード ----------

def extract_nodes(xml, ranges, prefix_label):
    tree = ET.parse(xml)
    law = tree.getroot().find('.//Law')
    body = law.find('LawBody')
    law_num = text_of(law.find('LawNum'))
    title = text_of(body.find('LawTitle'))
    nodes = []

    def key(s):
        ps = re.split(r'[_:]', s)
        if not all(p.isdigit() for p in ps): return None
        return tuple(int(p) for p in ps)
    def in_range(num):
        k = key(num)
        if k is None: return False
        for s, e in ranges:
            a, b = key(s), key(e)
            L = max(len(k), len(a), len(b))
            pad = lambda t: t + (0,)*(L-len(t))
            if pad(a) <= pad(k) <= pad(b): return True
        return False
    def kanji_label(artnum):
        ps = artnum.split('_')
        lab = '第' + ps[0] + '条'
        for p in ps[1:]: lab += 'の' + p
        return lab
    def tables_of(el):
        out = []
        for ts in el.findall('TableStruct'):
            rows = []
            for row in ts.findall('.//TableRow'):
                rows.append([text_of(c) for c in row.findall('TableColumn')])
            if rows: out.append(rows)
        return out
    def walk_items(parent, base_addr, base_ka, art, depth=1):
        tags = {1:'Item', 2:'Subitem1', 3:'Subitem2', 4:'Subitem3'}
        tag = tags.get(depth)
        if not tag: return
        for idx, it in enumerate(parent.findall(tag), 1):
            te = it.find(f'{tag}Title')
            tt = text_of(te) if te is not None else ''
            if depth == 1:
                part = kanji_branch(tt) or str(idx)
                ka = base_ka + f'第{part.replace("_","の")}号'
            elif depth == 2:
                i = IROHA.find(tt) + 1 if tt and tt in IROHA else idx
                part = str(i); ka = base_ka + tt
            else:
                m = re.match(r'[（(]?([0-9０-９]+)', tt)
                part = (m.group(1).translate(ZEN) if m else str(idx)); ka = base_ka + tt
            addr = base_addr + '.' + part
            se = it.find(f'{tag}Sentence')
            nodes.append(dict(type=tag, address=addr, ka=ka, title=tt,
                              sentence=sentences(se) if se is not None else '',
                              art=art, tables=tables_of(it)))
            walk_items(it, addr, ka, art, depth+1)

    for a in body.find('MainProvision').iter('Article'):
        num = a.get('Num', '')
        if not in_range(num): continue
        art = num.replace(':', '_')
        addr0 = 'ln' + art
        cap = a.find('ArticleCaption')
        lab = kanji_label(art)
        nodes.append(dict(type='Article', address=addr0, ka=lab, title=lab,
                          sentence='', art=art, caption=text_of(cap) if cap is not None else '', tables=[]))
        for pi, p in enumerate(a.findall('Paragraph'), 1):
            pn = p.find('ParagraphNum')
            pidx = kanji2int(text_of(pn)) if pn is not None and text_of(pn) else pi
            pidx = pidx or pi
            addr = f'{addr0}.{pidx}'
            ka = lab + f'第{pidx}項'
            ps = p.find('ParagraphSentence')
            nodes.append(dict(type='Paragraph', address=addr, ka=ka, title='',
                              sentence=sentences(ps) if ps is not None else '',
                              art=art, tables=tables_of(p)))
            walk_items(p, addr, ka, art)
    return title, law_num, nodes

# ---------- 通達HTML → フラットノード ----------

TSU_NUM = re.compile(r'^(\d+(?:の\d+)*[-−‐](?:\d+(?:の\d+)*))')

def tsu_addr(numstr):
    """'66の6-1' → ('66_6-1', '66の6')  (address部, 対象条)"""
    numstr = numstr.replace('−', '-').replace('‐', '-')
    base, seq = numstr.split('-', 1)
    return base.replace('の', '_') + '-' + seq.replace('の', '_'), base

def extract_tsutatsu(files):
    from bs4 import BeautifulSoup
    nodes = []
    groups = []
    for path, glabel in files:
        s = open(path, encoding='utf-8', errors='replace').read()
        soup = BeautifulSoup(s, 'html.parser')
        body = soup.find('div', id='bodyArea') or soup
        cur = None      # 現在の通達 art id
        cur_title = ''  # 直前の h2
        pseq = 0
        garts = []
        for elem in body.find_all(['h2', 'p', 'ol', 'ul']):
            if elem.name == 'h2':
                cur_title = elem.get_text(strip=True)
                continue
            text = elem.get_text(' ', strip=True)
            if not text: continue
            m = TSU_NUM.match(text)
            if m:
                num = m.group(1).replace('−', '-').replace('‐', '-')
                addrpart, base = tsu_addr(num)
                cur = addrpart
                pseq = 1
                garts.append(cur)
                nodes.append(dict(type='Article', address='ln'+cur, ka=num, title=num,
                                  sentence='', art=cur, caption=cur_title.strip('（）'), tables=[],
                                  target=base))
                bodytext = text[m.end():].strip()
                nodes.append(dict(type='Paragraph', address=f'ln{cur}.{pseq}', ka=num,
                                  title='', sentence=bodytext, art=cur, tables=[]))
            elif cur:
                pseq += 1
                ttl = '注' if text.startswith('（注') or text.startswith('(注') else ''
                nodes.append(dict(type='Paragraph', address=f'ln{cur}.{pseq}', ka=num,
                                  title=ttl, sentence=text, art=cur, tables=[]))
        groups.append(dict(label=glabel, arts=garts))
    return nodes, groups

# ---------- 参照解決 ----------

KN = '(?:[〇一二三四五六七八九十百千]+|[0-9０-９]+)'
LAWNAMES = {
    '法人税法施行令': ('ext', '340CO0000000097'),
    '法人税法施行規則': ('ext', '340M50000040012'),
    '法人税法': ('ext', '340AC0000000034'),
    '所得税法施行令': ('ext', '340CO0000000096'),
    '所得税法': ('ext', '340AC0000000033'),
    '国税通則法': ('ext', '337AC0000000066'),
    '租税特別措置法施行令': ('rei', None),
    '租税特別措置法施行規則': ('kis', None),
    '租税特別措置法': ('hou', None),
    '措置法施行令': ('rei', None),
    '措置法施行規則': ('kis', None),
    '措置法令': ('rei', None),
    '措置法規則': ('kis', None),
    '措置法': ('hou', None),
}
LAW_ALT = '|'.join(sorted(LAWNAMES, key=len, reverse=True))
REF = re.compile(
    '(?P<law>' + LAW_ALT + r')?'
    r'(?P<hourei>(?<![法令規])法|(?<![法政省命條'  '])令|規則)?'
    r'第(?P<art>' + KN + r')条(?P<artbr>(?:の' + KN + r')*)'
    r'(?:第(?P<para>' + KN + r')項)?'
    r'(?:第(?P<item>' + KN + r')号(?P<itembr>(?:の' + KN + r')*))?'
    r'(?P<iroha>[イロハニホヘトチリヌル])?'
    r'|(?P<rel>前条|次条|前項|次項|前各項|前' + KN + r'項|前号|次号)'
    r'(?:第(?P<rpara>' + KN + r')項)?'
    r'(?:第(?P<ritem>' + KN + r')号)?'
)
TSUREF = re.compile(r'(?<![\d－‐−-])(\d+(?:の\d+)+[-−‐]\d+(?:の\d+)*)(?![\d）]*[-−‐])')
DELEG = re.compile(r'政令で定める|財務省令で定める')

LZID = {'hou': '332AC0000000026', 'rei': '332CO0000000043', 'kis': '332M50000040015'}

class Resolver:
    def __init__(self, law_key, known, art_seq=None, self_linkable=True):
        self.lk = law_key; self.known = known
        self.art_seq = art_seq or []
        self.self_linkable = self_linkable
        self.refs = []

    def ctx(self, addr):
        m = re.match(r'ln([0-9_\-]+)(?:\.(\d+))?(?:\.([0-9_]+))?', addr)
        return m.group(1), m.group(2), m.group(3)

    def resolve(self, mseg, src_addr):
        art, para, item = self.ctx(src_addr)
        g = mseg.groupdict()
        law_key = self.lk
        ext_id = None
        explicit = False
        if g['law']:
            kind, _id = LAWNAMES[g['law']]
            explicit = True
            if kind == 'ext': ext_id = _id
            else: law_key = kind
        elif g['hourei']:
            hk = g['hourei']
            law_key = 'hou' if hk == '法' else ('rei' if hk == '令' else 'kis')
            explicit = True
        if g['art']:
            if not explicit and not self.self_linkable:
                return None
            n = kanji2int(g['art'])
            if n is None: return None
            tgt = str(n)
            for b in (g['artbr'] or '').split('の')[1:]:
                bn = kanji2int(b)
                if bn is None: return None
                tgt += '_' + str(bn)
            addr = 'ln' + tgt
            if g['para']: addr += '.' + str(kanji2int(g['para']))
            if g['item']:
                if not g['para']: addr += '.1'
                ib = str(kanji2int(g['item']))
                for b in (g['itembr'] or '').split('の')[1:]:
                    ib += '_' + str(kanji2int(b))
                addr += '.' + ib
            if g['iroha']:
                i = IROHA.find(g['iroha'])
                if i >= 0 and g['item']: addr += '.' + str(i+1)
            if ext_id: return ('ext', ext_id, addr)
            return (law_key, addr, None)
        rel = g['rel']
        if not rel: return None
        if not self.self_linkable: return None
        if law_key != self.lk: return None
        def num_in(s):
            m = re.search('(' + KN + ')', s)
            return kanji2int(m.group(1)) if m else 1
        if rel in ('前条', '次条'):
            if art in self.art_seq:
                i = self.art_seq.index(art) + (1 if rel == '次条' else -1)
                if 0 <= i < len(self.art_seq): addr = 'ln' + self.art_seq[i]
                else: return None
            elif '_' not in art and art.isdigit():
                addr = 'ln' + str(int(art) + (1 if rel == '次条' else -1))
            else: return None
        elif rel in ('前項', '次項'):
            if not para: return None
            p = int(para) + (1 if rel == '次項' else -1)
            if p < 1: return None
            addr = f'ln{art}.{p}'
        elif rel.startswith('前') and rel.endswith('項'):
            if not para: return None
            n = num_in(rel[1:-1]) if rel != '前各項' else int(para) - 1
            p = max(1, int(para) - n)
            addr = f'ln{art}.{p}'
        elif rel in ('前号', '次号'):
            if not item or not para or '_' in item: return None
            i = int(item) + (1 if rel == '次号' else -1)
            if i < 1: return None
            addr = f'ln{art}.{para}.{i}'
        else:
            return None
        if g['rpara']: addr += '.' + str(kanji2int(g['rpara']))
        if g['ritem']: addr += '.' + str(kanji2int(g['ritem']))
        return (law_key, addr, None)

    def linkify(self, text, src_addr, deleg_in=()):
        """REF・通達相互参照・委任文言をまとめてマークアップ"""
        cands = []
        for m in REF.finditer(text):
            cands.append((m.start(), m.end(), 'ref', m))
        if 'tsu' in self.known:
            for m in TSUREF.finditer(text):
                cands.append((m.start(), m.end(), 'tsu', m))
        for layer in deleg_in:
            pat = '政令で定める' if layer == 'rei' else '財務省令で定める'
            for m in re.finditer(pat, text):
                cands.append((m.start(), m.end(), 'deleg:'+layer, m))
        cands.sort(key=lambda c: (c[0], -(c[1]-c[0])))
        out = []; pos = 0
        for st, en, kind, m in cands:
            if st < pos: continue
            label = text[st:en]
            seg = H.escape(text[pos:st])
            if kind == 'ref':
                r = self.resolve(m, src_addr)
                if r:
                    lk, addr, sub = r
                    if lk == 'ext':
                        out.append(seg)
                        q = f'?n={sub}&amp;mode=only' if sub else ''
                        out.append(f'<a class="law" target="_blank" rel="noopener" href="https://lawzilla.jp/law/{addr}{q}">{H.escape(label)}</a>')
                        pos = en; continue
                    known = self.known.get(lk, {})
                    if addr in known or ('.' in addr and addr.split('.')[0] in known):
                        tgt = addr if addr in known else addr.split('.')[0]
                        self.refs.append((src_addr, lk, tgt, addr))
                        out.append(seg)
                        out.append(f'<a class="self" href="javascript:void(0)" data-link="{lk}@{addr}">{H.escape(label)}</a>')
                        pos = en; continue
                    lz = LZID.get(lk)
                    if lz:
                        out.append(seg)
                        out.append(f'<a class="law" target="_blank" rel="noopener" href="https://lawzilla.jp/law/{lz}?n={addr}&amp;mode=only">{H.escape(label)}</a>')
                        pos = en; continue
                out.append(H.escape(text[pos:en])); pos = en
            elif kind == 'tsu':
                num = m.group(1).replace('−', '-').replace('‐', '-')
                ap, _ = tsu_addr(num)
                addr = 'ln' + ap
                if addr in self.known.get('tsu', {}):
                    if self.lk != 'tsu' or addr != 'ln' + src_addr[2:].split('.')[0]:
                        self.refs.append((src_addr, 'tsu', addr, addr))
                    out.append(seg)
                    out.append(f'<a class="self tsu" href="javascript:void(0)" data-link="tsu@{addr}">{H.escape(label)}</a>')
                    pos = en
                else:
                    out.append(H.escape(text[pos:en])); pos = en
            else:
                layer = kind.split(':')[1]
                out.append(seg)
                out.append(f'<span class="deleg" data-layer="{layer}">{H.escape(label)}</span>')
                pos = en
        out.append(H.escape(text[pos:]))
        return ''.join(out)

def kakko_html(text, resolver, src_addr, deleg_in=()):
    if text.count('（') != text.count('）'):
        return resolver.linkify(text, src_addr, deleg_in)
    out = []; level = 0; buf = []
    def flush():
        if buf:
            out.append(resolver.linkify(''.join(buf), src_addr, deleg_in))
            buf.clear()
    for ch in text:
        if ch == '（':
            flush(); level += 1
            cls = 'kakko' if level == 1 else f'kakko-{level}'
            out.append(f'<span class="{cls}">（')
        elif ch == '）':
            flush(); out.append('）</span>'); level = max(0, level-1)
        else:
            buf.append(ch)
    flush()
    return ''.join(out)

# ---------- 4層の抽出 ----------

TSU_DIR = "/mnt/d/Users/PC/Desktop/CFC税制/40_DB_RAG/RAG_System/通達"

t_hou, n_hou, nd_hou = extract_nodes('/tmp/sotokuso.xml', [("66_6","66_9_5"),("40_4","40_6")], '法')
t_rei, n_rei, nd_rei = extract_nodes('/tmp/sotochirei.xml', [("39_14","39_20_9"),("25_19","25_27")], '令')
t_kis, n_kis, nd_kis = extract_nodes('/tmp/sotokisoku.xml', [("18_20","18_20_2"),("22_11","22_11_3")], '規')
nd_tsu, grp_tsu = extract_tsutatsu([
    (os.path.join(TSU_DIR, '66_6_utf8.htm'), '第66条の6〜第66条の9関係'),
    (os.path.join(TSU_DIR, '66_9_2_utf8.htm'), '第66条の9の2〜第66条の9の5関係'),
])

known = {
    'hou': set(n['address'] for n in nd_hou),
    'rei': set(n['address'] for n in nd_rei),
    'kis': set(n['address'] for n in nd_kis),
    'tsu': set(n['address'] for n in nd_tsu),
}

GROUPS = {
 'cfc_hou': [("内国法人の外国関係会社に係る所得の課税の特例","66_6","66_9"),
             ("特殊関係株主等である内国法人に係る特例","66_9_2","66_9_5"),
             ("居住者の外国関係会社に係る特例","40_4","40_6")],
 'cfc_rei': [("外国関係会社の判定等","39_14","39_14_3"),
             ("適用対象金額の計算","39_15","39_15"),
             ("課税対象金額・部分課税対象金額等","39_16","39_17_5"),
             ("二重課税調整・申告","39_18","39_20_9"),
             ("居住者に係る規定","25_19","25_27")],
 'cfc_kis': [("内国法人に係る規定","22_11","22_11_3"),
             ("居住者に係る規定","18_20","18_20_2")],
}

def build_xml_law(law_key, slug, title, law_num, nodes, deleg_layers):
    art_seq = [n['art'] for n in nodes if n['type'] == 'Article']
    res = Resolver(law_key, known, art_seq)
    arts = []; order = []; nmap = {}
    for n in nodes:
        order.append(n['address'])
        h = kakko_html(n['sentence'], res, n['address'], deleg_layers) if n['sentence'] else ''
        ent = dict(t=n['type'], ka=n['ka'], ti=n['title'], h=h, art=n['art'])
        if n.get('caption'): ent['c'] = n['caption']
        if n['tables']: ent['tbl'] = n['tables']
        nmap[n['address']] = ent
        if n['type'] == 'Article':
            arts.append(dict(id=n['art'], address=n['address'], label=n['ka'], c=n.get('caption','')))
    groups = []
    def key(s): return tuple(int(p) for p in s.split('_'))
    def pad(t, L): return t + (0,)*(L-len(t))
    for label, s, e in GROUPS[slug]:
        sel = []
        for a in arts:
            k, ks, ke = key(a['id']), key(s), key(e)
            L = max(len(k), len(ks), len(ke))
            if pad(ks,L) <= pad(k,L) <= pad(ke,L): sel.append(a['id'])
        if sel: groups.append(dict(label=label, arts=sel))
    grouped = set(x for g in groups for x in g['arts'])
    rest = [a['id'] for a in arts if a['id'] not in grouped]
    if rest: groups.append(dict(label='その他', arts=rest))
    return dict(slug=slug, key=law_key, title=title, num=law_num, lawzilla=LZID.get(law_key,''),
                arts=arts, groups=groups, order=order, nodes=nmap), res.refs

def build_tsu(nodes, groups):
    res = Resolver('tsu', known, self_linkable=False)
    arts = []; order = []; nmap = {}
    for n in nodes:
        order.append(n['address'])
        h = kakko_html(n['sentence'], res, n['address']) if n['sentence'] else ''
        ent = dict(t=n['type'], ka=n['ka'], ti=n['title'], h=h, art=n['art'])
        if n.get('caption'): ent['c'] = n['caption']
        if n.get('target'): ent['tg'] = n['target']
        nmap[n['address']] = ent
        if n['type'] == 'Article':
            arts.append(dict(id=n['art'], address=n['address'], label=n['ka'], c=n.get('caption','')))
    return dict(slug='cfc_tsu', key='tsu', title='租税特別措置法関係通達（法人税編・CFC関係）',
                num='昭和50年2月14日直法2-2（最終改正まで反映）', lawzilla='',
                arts=arts, groups=groups, order=order, nodes=nmap), res.refs

hou, r_hou = build_xml_law('hou', 'cfc_hou', t_hou+'（CFC税制抜粋）', n_hou, nd_hou, ('rei','kis'))
rei, r_rei = build_xml_law('rei', 'cfc_rei', t_rei+'（CFC税制抜粋）', n_rei, nd_rei, ('kis',))
kis, r_kis = build_xml_law('kis', 'cfc_kis', t_kis+'（CFC税制抜粋）', n_kis, nd_kis, ())
tsu, r_tsu = build_tsu(nd_tsu, grp_tsu)

# 通達→法条の対象マッピング（番号プレフィクス 66の6-1 → 法66_6）も参照として追加
for a in tsu['arts']:
    base = a['id'].split('-')[0]  # '66_6'
    tgt = 'ln' + base
    if tgt in known['hou']:
        r_tsu.append((a['address'], 'hou', tgt, tgt))

# 逆参照: 4層横断
laws = {'hou': hou, 'rei': rei, 'kis': kis, 'tsu': tsu}
back = {k: {} for k in laws}
PFX = {'hou':'法','rei':'令','kis':'規','tsu':'措通'}
for src_law, refs in (('hou',r_hou),('rei',r_rei),('kis',r_kis),('tsu',r_tsu)):
    for src_addr, dst_law, dst_addr, _full in refs:
        if src_addr.split('.')[0] == dst_addr.split('.')[0] and src_law == dst_law:
            continue  # 同一条内の自己参照は逆引きに載せない
        nm = laws[src_law]['nodes'].get(src_addr)
        ka = PFX[src_law] + (nm['ka'] if nm else src_addr)
        b = back[dst_law].setdefault(dst_addr, [])
        if not any(x['addr'] == src_addr and x['law'] == src_law for x in b):
            b.append(dict(law=src_law, addr=src_addr, ka=ka))
for k in laws:
    laws[k]['backlinks'] = back[k]

for k, d in laws.items():
    fn = os.path.join(OUT, d['slug'] + '.json')
    json.dump(d, open(fn, 'w', encoding='utf-8'), ensure_ascii=False, separators=(',',':'))
meta = [dict(slug=d['slug'], key=d['key'], title=d['title'], num=d['num'], articles=len(d['arts']))
        for d in (hou, rei, kis, tsu)]
json.dump(meta, open(os.path.join(OUT, 'meta.json'), 'w', encoding='utf-8'), ensure_ascii=False)

for k, d in laws.items():
    print(f"{d['slug']}: {len(d['arts'])}条/項目 {len(d['order'])}ノード backlinked={len(d['backlinks'])} "
          f"{os.path.getsize(os.path.join(OUT, d['slug']+'.json'))//1024}KB")
print('refs:', {k: len(r) for k, r in (('hou',r_hou),('rei',r_rei),('kis',r_kis),('tsu',r_tsu))})
