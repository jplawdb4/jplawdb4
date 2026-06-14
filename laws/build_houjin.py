#!/usr/bin/env python3
"""CFC縦串ビルダー v4
v3（4層ノードモデル・参照解決・委任検出・逆参照）に加えて:
- 定義語辞書: 「X」という型 ＋ 用語の意義列挙型 の2形式を機械抽出
- 全層本文への定義語マーキング（span.term data-def）
- 外部法令参照のプレビュー辞書 ext_preview.json（政令等のホバー対応）
"""
import xml.etree.ElementTree as ET
import json, os, re, html as H

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "app_houjin", "data2")
os.makedirs(OUT, exist_ok=True)

KD = {'〇':0,'一':1,'二':2,'三':3,'四':4,'五':5,'六':6,'七':7,'八':8,'九':9}
IROHA = "イロハニホヘトチリヌルヲワカヨタレソツネナラムウヰノオクヤマケフコエテ"
ZEN = str.maketrans('０１２３４５６７８９', '0123456789')

def kanji2int(s):
    s = s.translate(ZEN)
    if s.isdigit(): return int(s)
    total = 0; cur = 0
    for ch in s:
        if ch == '千': total += (cur or 1)*1000; cur = 0
        elif ch == '百': total += (cur or 1)*100; cur = 0
        elif ch == '十': total += (cur or 1)*10; cur = 0
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
    cols = parent.findall('.//Column')
    if cols:
        return '　'.join(''.join(text_of(x) for x in c.findall('.//Sentence')) for c in cols)
    return ''.join(text_of(s) for s in parent.findall('.//Sentence'))

def kanji_label(artnum):
    ps = artnum.split('_')
    lab = '第' + ps[0] + '条'
    for p in ps[1:]: lab += 'の' + p
    return lab

# ---------- XML → フラットノード ----------

def extract_nodes(xml, ranges):
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
    numstr = numstr.replace('−', '-').replace('‐', '-')
    base, seq = numstr.split('-', 1)
    return base.replace('の', '_') + '-' + seq.replace('の', '_'), base

def extract_tsutatsu_xml(xmlpath, prefixes):
    """tax_tsutatsu 独自XML（<items><item id raw_id title><paragraphs><paragraph><text>）"""
    tree = ET.parse(xmlpath); root = tree.getroot()
    items_el = root.find('items')
    nodes = []; garts = []
    for it in items_el.findall('item'):
        raw = (it.get('raw_id', '') or '').replace('−','-').replace('‐','-')
        if not any(raw.startswith(p) for p in prefixes): continue
        iid = it.get('id', '')
        art = iid.replace('-', '_')
        title = it.get('title', '')
        addr0 = 'ln' + art
        parts = art.split('_')
        target = '_'.join(parts[:3]) if len(parts) >= 3 else '_'.join(parts)
        nodes.append(dict(type='Article', address=addr0, ka=raw, title=raw,
                          sentence='', art=art, caption=title, tables=[], target=target))
        garts.append(art)
        ps = it.find('paragraphs'); seq = 0
        if ps is not None:
            for p in ps.findall('paragraph'):
                te = p.find('text')
                txt = ''.join(te.itertext()).strip() if te is not None else ''
                if not txt: continue
                seq += 1
                ttl = '注' if txt.lstrip().startswith(('（注', '(注', '注')) else ''
                nodes.append(dict(type='Paragraph', address=f'{addr0}.{seq}', ka=raw,
                                  title=ttl, sentence=txt, art=art, tables=[]))
    groups = [dict(label='措通 第66条の4関係', arts=garts)]
    return nodes, groups

# ---------- 参照解決 ----------

KN = '(?:[〇一二三四五六七八九十百千]+|[0-9０-９]+)'
LAWNAMES = {
    '租税特別措置法施行令': ('ext', '332CO0000000043'),
    '租税特別措置法施行規則': ('ext', '332M50000040015'),
    '租税特別措置法': ('ext', '332AC0000000026'),
    '措置法施行令': ('ext', '332CO0000000043'),
    '措置法施行規則': ('ext', '332M50000040015'),
    '措置法': ('ext', '332AC0000000026'),
    '所得税法施行令': ('ext', '340CO0000000096'),
    '所得税法': ('ext', '340AC0000000033'),
    '国税通則法': ('ext', '337AC0000000066'),
    '法人税法施行令': ('rei', None),
    '法人税法施行規則': ('kis', None),
    '法人税法': ('hou', None),
}
LAW_ALT = '|'.join(sorted(LAWNAMES, key=len, reverse=True))
REF = re.compile(
    '(?P<law>' + LAW_ALT + r')?'
    r'(?P<hourei>(?<![法令規])法|(?<![法政省命條'  '])令|規則)?'
    r'第(?P<art>' + KN + r')条(?P<artbr>(?:の' + KN + r')*)'
    r'(?:第(?P<para>' + KN + r')項)?'
    r'(?:第(?P<item>' + KN + r')号(?P<itembr>(?:の' + KN + r')*))?'
    r'(?P<iroha>[イロハニホヘトチリヌル])?'
    r'|(?P<rel>前条|次条|前項|次項|前各項|前' + KN + r'項|前号|次号|同条|同項|同号)'
    r'(?:第(?P<rpara>' + KN + r')項)?'
    r'(?:第(?P<ritem>' + KN + r')号)?'
    r'(?P<riroha>[イロハニホヘトチリヌル])?'
    r'|(?<![条則])第(?P<spara>' + KN + r')項'
    r'(?:第(?P<sitem>' + KN + r')号(?P<sbr>(?:の' + KN + r')*))?'
    r'(?P<siroha>[イロハニホヘトチリヌル])?'
    r'|(?<![項条])第(?P<sitem2>' + KN + r')号(?P<sbr2>(?:の' + KN + r')*)'
    r'(?P<siroha2>[イロハニホヘトチリヌル])?'
)
TSUREF = re.compile(r'(?<![\d－‐−-])(\d+(?:の\d+)+[-−‐]\d+(?:の\d+)*)(?![\d）]*[-−‐])')
DELEG = re.compile(r'政令で定める|財務省令で定める')
LZID = {'hou': '340AC0000000034', 'rei': '340CO0000000097', 'kis': '340M50000040012'}

# 定義語（pass1で構築 → pass2で参照）
TERM_DEFS = {}   # term -> [(law_key, addr)]
TERM_RE = None   # pass2で構築

def pick_def(term, cur_lk, cur_art):
    cands = TERM_DEFS.get(term, [])
    for lk, addr in cands:
        if lk == cur_lk and addr.split('.')[0] == 'ln' + cur_art: return (lk, addr)
    for lk, addr in cands:
        if lk == cur_lk: return (lk, addr)
    for order in ('hou', 'rei', 'kis', 'tsu'):
        for lk, addr in cands:
            if lk == order: return (lk, addr)
    return cands[0] if cands else None

class Resolver:
    def __init__(self, law_key, known, art_seq=None, self_linkable=True):
        self.lk = law_key; self.known = known
        self.art_seq = art_seq or []
        self.self_linkable = self_linkable
        self.refs = []
        self.ext_refs = set()  # (lawid, addr)
        self.ana = None        # 直近の任意参照（同項・同号・列挙連鎖用）
        self.ana_named = None  # 直近の条名指し参照（同条用）

    def begin_node(self):
        self.ana = None
        self.ana_named = None
        self.qdepth = 0  # 読替規定等の引用「」深度

    def _upd_ana(self, r, named=False):
        lk, addr, sub = r
        a = (sub or addr)
        ps = a[2:].split('.') if a.startswith('ln') else []
        if not ps: return
        ent = dict(ext=addr if lk == 'ext' else None,
                   lk=None if lk == 'ext' else lk,
                   art=ps[0], para=ps[1] if len(ps) > 1 else None,
                   item=ps[2] if len(ps) > 2 else None)
        self.ana = ent
        if named: self.ana_named = ent

    def ctx(self, addr):
        m = re.match(r'ln([0-9_\-]+)(?:\.(\d+))?(?:\.([0-9_]+))?', addr)
        return m.group(1), m.group(2), m.group(3)

    CONN = re.compile(r'^(?:から|まで|及び|又は|並びに|若しくは|、|\s)*$')

    def resolve(self, mseg, src_addr, gap=None):
        art, para, item = self.ctx(src_addr)
        g = mseg.groupdict()
        law_key = self.lk; ext_id = None; explicit = False
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
            if not explicit and not self.self_linkable: return None
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
        # 列挙・範囲（から/まで/及び/又は等）で直前の参照に連なるか
        chained = (gap is not None and len(gap) <= 10 and self.CONN.match(gap)
                   and self.ana and self.ana.get('art'))
        # 裸の項参照（第三項／第三項第二号イ）
        if g.get('spara'):
            p = str(kanji2int(g['spara']))
            ib = None
            if g.get('sitem'):
                ib = str(kanji2int(g['sitem']))
                for b in (g.get('sbr') or '').split('の')[1:]:
                    ib += '_' + str(kanji2int(b))
                if g.get('siroha'):
                    i = IROHA.find(g['siroha'])
                    if i >= 0: ib += '.' + str(i+1)
            if chained:
                # 直前に参照された条の項（例: 法人税法69条第一項から第三項まで）
                a = self.ana
                addr = 'ln' + a['art'] + '.' + p + (('.' + ib) if ib else '')
                if a.get('ext'): return ('ext', a['ext'], addr)
                tlk = a['lk']
                if addr in self.known.get(tlk, ()) or addr.split('.')[0] in self.known.get(tlk, ()):
                    return (tlk, addr, None)
                return None
            if not self.self_linkable: return None
            addr = f'ln{art}.{p}' + (('.' + ib) if ib else '')
            return (self.lk, addr, None) if addr in self.known.get(self.lk, ()) else None
        # 裸の号参照（第二号／第二号の二イ）
        if g.get('sitem2'):
            ib = str(kanji2int(g['sitem2']))
            for b in (g.get('sbr2') or '').split('の')[1:]:
                ib += '_' + str(kanji2int(b))
            if g.get('siroha2'):
                i = IROHA.find(g['siroha2'])
                if i >= 0: ib += '.' + str(i+1)
            if chained and self.ana.get('para'):
                a = self.ana
                addr = 'ln' + a['art'] + '.' + a['para'] + '.' + ib
                if a.get('ext'): return ('ext', a['ext'], addr)
                tlk = a['lk']
                if addr in self.known.get(tlk, ()) or addr.split('.')[0] in self.known.get(tlk, ()):
                    return (tlk, addr, None)
                return None
            if not self.self_linkable: return None
            if not para: return None
            addr = f'ln{art}.{para}.{ib}'
            return (self.lk, addr, None) if addr in self.known.get(self.lk, ()) else None
        rel = g['rel']
        if not rel: return None
        # 同条・同項・同号: 直前の参照（照応）から解決。他法令(ext)への照応も可
        if rel in ('同条', '同項', '同号'):
            a = self.ana_named if rel == '同条' else self.ana
            if not a or not a.get('art'): return None
            t_art = a['art']; t_para = None; t_item = None
            if rel == '同条':
                if g['rpara']: t_para = str(kanji2int(g['rpara']))
                if g['ritem']:
                    if not t_para: return None
                    t_item = str(kanji2int(g['ritem']))
            elif rel == '同項':
                t_para = a.get('para')
                if not t_para: return None
                if g['ritem']: t_item = str(kanji2int(g['ritem']))
            else:  # 同号
                t_para = a.get('para'); t_item = a.get('item')
                if not t_para or not t_item: return None
            addr = 'ln' + t_art
            if t_para: addr += '.' + t_para
            if t_item:
                addr += '.' + t_item
                if g.get('riroha'):
                    i = IROHA.find(g['riroha'])
                    if i >= 0: addr += '.' + str(i+1)
            if a.get('ext'):
                return ('ext', a['ext'], addr)
            tlk = a['lk']
            if addr in self.known.get(tlk, ()) or addr.split('.')[0] in self.known.get(tlk, ()):
                return (tlk, addr, None)
            return None
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
            addr = f'ln{art}.{max(1, int(para) - n)}'
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
        cands = []
        for m in REF.finditer(text):
            cands.append((m.start(), m.end(), 0, 'ref', m))
        if 'tsu' in self.known:
            for m in TSUREF.finditer(text):
                cands.append((m.start(), m.end(), 1, 'tsu', m))
        for layer in deleg_in:
            pat = '政令で定める' if layer == 'rei' else '財務省令で定める'
            for m in re.finditer(pat, text):
                cands.append((m.start(), m.end(), 2, 'deleg:'+layer, m))
        if TERM_RE is not None:
            art = src_addr[2:].split('.')[0]
            for m in TERM_RE.finditer(text):
                st, en = m.start(), m.end()
                # 定義部そのもの（「X」という）はスキップ
                if st > 0 and text[st-1] == '「' and en < len(text) and text[en] == '」':
                    continue
                d = pick_def(m.group(0), self.lk, art)
                if not d: continue
                if d[0] == self.lk and d[1] == src_addr: continue
                cands.append((st, en, 3, 'term:' + d[0] + '@' + d[1], m))
        cands.sort(key=lambda c: (c[0], c[2], -(c[1]-c[0])))
        # 各位置の引用「」深度（ノード横断で持ち越し）
        qd = []; d = getattr(self, 'qdepth', 0)
        for ch in text:
            qd.append(d)
            if ch == '「': d += 1
            elif ch == '」': d = max(0, d - 1)
        self.qdepth = d
        out = []; pos = 0
        prev_ref_end = None
        for st, en, _pri, kind, m in cands:
            if st < pos: continue
            label = text[st:en]
            seg = H.escape(text[pos:st])
            in_quote = qd[st] > 0 if st < len(qd) else False
            if kind == 'ref':
                gap = text[prev_ref_end:st] if prev_ref_end is not None else None
                r = self.resolve(m, src_addr, gap)
                prev_ref_end = en
                if r and not in_quote:
                    self._upd_ana(r, named=bool(m.group('art')) or m.group('rel') in ('前条', '次条'))
                    lk, addr, sub = r
                    if lk == 'ext':
                        self.ext_refs.add((addr, sub) if sub else (addr, ''))
                        out.append(seg)
                        q = f'?n={sub}&amp;mode=only' if sub else ''
                        dl = f' data-ext="{addr}@{sub}"' if sub else ''
                        out.append(f'<a class="law"{dl} target="_blank" rel="noopener" href="https://lawzilla.jp/law/{addr}{q}">{H.escape(label)}</a>')
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
                        self.ext_refs.add((lz, addr))
                        out.append(seg)
                        out.append(f'<a class="law" data-ext="{lz}@{addr}" target="_blank" rel="noopener" href="https://lawzilla.jp/law/{lz}?n={addr}&amp;mode=only">{H.escape(label)}</a>')
                        pos = en; continue
                out.append(H.escape(text[pos:en])); pos = en
            elif kind == 'tsu':
                num = m.group(1).replace('−', '-').replace('‐', '-')
                ap, _b = tsu_addr(num)
                addr = 'ln' + ap
                if addr in self.known.get('tsu', {}):
                    if self.lk != 'tsu' or addr.split('.')[0] != 'ln' + src_addr[2:].split('.')[0]:
                        self.refs.append((src_addr, 'tsu', addr, addr))
                    out.append(seg)
                    out.append(f'<a class="self tsu" href="javascript:void(0)" data-link="tsu@{addr}">{H.escape(label)}</a>')
                else:
                    out.append(H.escape(text[pos:en]))
                pos = en
            elif kind.startswith('deleg:'):
                layer = kind.split(':')[1]
                out.append(seg)
                trail = text[en:en+14]
                out.append(f'<span class="deleg" data-layer="{layer}" data-t="{H.escape(trail)}">{H.escape(label)}</span>')
                pos = en
            else:  # term
                dl = kind.split(':', 1)[1]
                out.append(seg)
                out.append(f'<span class="term" data-def="{dl}">{H.escape(label)}</span>')
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

# ---------- 定義語抽出 ----------

DEF_BRACKET = re.compile(r'「([^「」]{2,30})」と(?:いう|いい|総称する)')
STOP_TERMS = {'当該', 'その他', 'もの', 'こと', '場合', '金額', '割合', '法人', '株式', '日'}

def harvest_terms(law_key, nodes):
    """Shape2: 「X」という ／ Shape3: 用語の意義の列挙号"""
    found = []
    def_para = None
    for n in nodes:
        s = n['sentence']
        if not s: continue
        for m in DEF_BRACKET.finditer(s):
            t = m.group(1)
            if t in STOP_TERMS or len(t) < 2: continue
            found.append((t, n['address']))
        if n['type'] == 'Paragraph' and '用語の意義' in s:
            def_para = n['address']
        elif def_para and n['type'] == 'Item' and n['address'].startswith(def_para + '.'):
            head = re.split(r'[　 ]', n['sentence'], 1)[0]
            if 2 <= len(head) <= 20 and not head.startswith('次に') and head not in STOP_TERMS:
                found.append((head, n['address']))
        elif n['type'] == 'Paragraph':
            if def_para and not n['address'].startswith(def_para.rsplit('.', 1)[0]):
                def_para = None
    for t, addr in found:
        TERM_DEFS.setdefault(t, [])
        if not any(lk == law_key and a == addr for lk, a in TERM_DEFS[t]):
            TERM_DEFS[t].append((law_key, addr))

# ---------- 4層抽出（pass1） ----------

XMLDIR = "/mnt/d/Users/PC/Desktop/法令等xml"

import json as _j
_S=_j.load(open('/tmp/hj_set.json'))
t_hou, n_hou, nd_hou = extract_nodes(XMLDIR+'/法人税法.xml', [(a,a) for a in _S['hou']])
t_rei, n_rei, nd_rei = extract_nodes(XMLDIR+'/法人税法施行令.xml', [(a,a) for a in _S['rei']])
t_kis, n_kis, nd_kis = extract_nodes(XMLDIR+'/法人税法施行規則.xml', [(a,a) for a in _S['kis']])
nd_tsu, grp_tsu = extract_tsutatsu_xml(XMLDIR+'/法人税基本通達.xml', [])

known = {
    'hou': set(n['address'] for n in nd_hou),
    'rei': set(n['address'] for n in nd_rei),
    'kis': set(n['address'] for n in nd_kis),
    'tsu': set(n['address'] for n in nd_tsu),
}

harvest_terms('hou', nd_hou)
harvest_terms('rei', nd_rei)
harvest_terms('kis', nd_kis)
TERM_RE = re.compile('|'.join(re.escape(t) for t in sorted(TERM_DEFS, key=len, reverse=True)))

GROUPS = {
 'hj_hou': [("CFC・TP・FTCが参照する法人税法条文","1","999999")],
 'hj_rei': [("参照される施行令条文","1","999999")],
 'hj_kis': [("参照される施行規則条文","1","999999")],
}

def build_xml_law(law_key, slug, title, law_num, nodes, deleg_layers):
    art_seq = [n['art'] for n in nodes if n['type'] == 'Article']
    res = Resolver(law_key, known, art_seq)
    arts = []; order = []; nmap = {}
    for n in nodes:
        order.append(n['address'])
        if n['type'] in ('Article', 'Paragraph'):
            res.begin_node()  # 照応は項単位（柱書→号→イへ文脈継続）
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
                arts=arts, groups=groups, order=order, nodes=nmap), res

def build_tsu(nodes, groups):
    res = Resolver('tsu', known, self_linkable=False)
    arts = []; order = []; nmap = {}
    for n in nodes:
        order.append(n['address'])
        if n['type'] == 'Article':
            res.begin_node()  # 通達は1通達単位で文脈継続（注を含む）
        h = kakko_html(n['sentence'], res, n['address']) if n['sentence'] else ''
        ent = dict(t=n['type'], ka=n['ka'], ti=n['title'], h=h, art=n['art'])
        if n.get('caption'): ent['c'] = n['caption']
        if n.get('target'): ent['tg'] = n['target']
        nmap[n['address']] = ent
        if n['type'] == 'Article':
            arts.append(dict(id=n['art'], address=n['address'], label=n['ka'], c=n.get('caption','')))
    return dict(slug='hj_tsu', key='tsu', title='（通達は制度別アプリを参照）',
                num='国税庁HTML版（snapshot 2025-06-28）', lawzilla='',
                arts=arts, groups=groups, order=order, nodes=nmap), res

hou, res_hou = build_xml_law('hou', 'hj_hou', t_hou+'（CFC・TP・FTC関連条文）', n_hou, nd_hou, ('rei','kis'))
rei, res_rei = build_xml_law('rei', 'hj_rei', t_rei+'（CFC・TP・FTC関連条文）', n_rei, nd_rei, ('kis',))
kis, res_kis = build_xml_law('kis', 'hj_kis', t_kis+'（CFC・TP・FTC関連条文）', n_kis, nd_kis, ())
tsu, res_tsu = build_tsu(nd_tsu, grp_tsu)

r_hou, r_rei, r_kis, r_tsu = res_hou.refs, res_rei.refs, res_kis.refs, res_tsu.refs
for a in tsu['arts']:
    if 'ln69' in known['hou']: r_tsu.append((a['address'], 'hou', 'ln69', 'ln69'))

laws = {'hou': hou, 'rei': rei, 'kis': kis, 'tsu': tsu}
back = {k: {} for k in laws}
PFX = {'hou':'法','rei':'令','kis':'規','tsu':'措通'}
for src_law, refs in (('hou',r_hou),('rei',r_rei),('kis',r_kis),('tsu',r_tsu)):
    for src_addr, dst_law, dst_addr, _full in refs:
        if src_addr.split('.')[0] == dst_addr.split('.')[0] and src_law == dst_law: continue
        nm = laws[src_law]['nodes'].get(src_addr)
        ka = PFX[src_law] + (nm['ka'] if nm else src_addr)
        b = back[dst_law].setdefault(dst_addr, [])
        if not any(x['addr'] == src_addr and x['law'] == src_law for x in b):
            b.append(dict(law=src_law, addr=src_addr, ka=ka))
for k in laws:
    laws[k]['backlinks'] = back[k]
    # 定義語辞書（この法令で定義された語）
    laws[k]['terms'] = {t: [a for lk, a in v if lk == k] for t, v in TERM_DEFS.items() if any(lk == k for lk, _ in v)}

# ---------- 外部参照プレビュー辞書 ----------

EXT_XML = {
    '332AC0000000026': (XMLDIR+'/租税特別措置法.xml', '措法'),
    '332CO0000000043': (XMLDIR+'/租税特別措置法施行令.xml', '措令'),
    '332M50000040015': (XMLDIR+'/租税特別措置法施行規則.xml', '措規'),
    '340AC0000000033': (XMLDIR+'/所得税法.xml', '所得税法'),
    '337AC0000000066': (XMLDIR+'/国税通則法.xml', '国税通則法'),
    '340CO0000000096': (XMLDIR+'/所得税法施行令.xml', '所得税法施行令'),
}

ext_targets = {}
for res in (res_hou, res_rei, res_kis, res_tsu):
    for lawid, addr in res.ext_refs:
        if addr: ext_targets.setdefault(lawid, set()).add(addr)

def addr_parts(addr):
    ps = addr[2:].split('.')
    return ps[0], (ps[1] if len(ps) > 1 else None), (ps[2] if len(ps) > 2 else None)

preview = {}
for lawid, addrs in ext_targets.items():
    if lawid not in EXT_XML: continue
    path, disp = EXT_XML[lawid]
    if not os.path.exists(path): continue
    arts_needed = {addr_parts(a)[0] for a in addrs}
    artmap = {}
    tree = ET.parse(path)
    mp = tree.getroot().find('.//MainProvision')
    for a in mp.iter('Article'):
        num = a.get('Num', '').replace(':', '_')
        if num in arts_needed: artmap[num] = a
    for addr in addrs:
        art, para, item = addr_parts(addr)
        el = artmap.get(art)
        if el is None: continue
        ka = disp + kanji_label(art)
        cap = el.find('ArticleCaption')
        capt = text_of(cap) if cap is not None else ''
        paras = el.findall('Paragraph')
        tgt_p = None
        if para:
            ka += f'第{para}項'
            for pi, p in enumerate(paras, 1):
                pn = p.find('ParagraphNum')
                pidx = kanji2int(text_of(pn)) if pn is not None and text_of(pn) else pi
                if str(pidx or pi) == para: tgt_p = p; break
        else:
            tgt_p = paras[0] if paras else None
        if tgt_p is None: continue
        txt = ''
        if item:
            ka += f'第{item.replace("_","の")}号'
            for it in tgt_p.findall('Item'):
                tt = text_of(it.find('ItemTitle')) if it.find('ItemTitle') is not None else ''
                if kanji_branch(tt) == item:
                    se = it.find('ItemSentence')
                    txt = sentences(se) if se is not None else ''
                    break
        if not txt:
            ps = tgt_p.find('ParagraphSentence')
            txt = sentences(ps) if ps is not None else ''
        if txt:
            preview[f'{lawid}@{addr}'] = dict(ka=ka, c=capt, t=txt[:600])

json.dump(preview, open(os.path.join(OUT, 'ext_preview.json'), 'w', encoding='utf-8'),
          ensure_ascii=False, separators=(',', ':'))

# ---------- 委任実装の特定（「…に規定する政令で定める」定型句マッチング） ----------

import html as _H
IMPL_PAT = re.compile(r'data-link="(hou|rei)@(ln[0-9_\.\-]+)"[^>]*>[^<]*</a>(?:<[^>]+>|[（）]){0,4}に規定する(政令|財務省令)で定める')
DELEG_SPAN = re.compile(r'<span class="deleg" data-layer="(rei|kis)" data-t="([^"]*)">')

def _plain_after(html_s, idx, n=14):
    seg = re.sub(r'<[^>]+>', '', html_s[idx:idx+220])
    return _H.unescape(seg)[:n]

impl_map = {}  # (tgt_law, tgt_addr) -> [(src_law, src_addr, trail)]
for src_law in ('rei', 'kis'):
    for addr, nd in laws[src_law]['nodes'].items():
        h = nd['h']
        if 'に規定する' not in _H.unescape(re.sub(r'<[^>]+>', '', h[:0])) and 'data-link' not in h:
            continue
        for m in IMPL_PAT.finditer(h):
            tgt_law, tgt_addr, kind = m.group(1), m.group(2), m.group(3)
            layer_expected = 'rei' if kind == '政令' else 'kis'
            if src_law != layer_expected: continue
            trail = _plain_after(h, m.end())
            impl_map.setdefault((tgt_law, tgt_addr), []).append((src_law, addr, trail))

def _common_prefix(a, b):
    i = 0
    while i < min(len(a), len(b)) and a[i] == b[i]: i += 1
    return i

n_annot = 0
for lk in ('hou', 'rei'):
    for addr, nd in laws[lk]['nodes'].items():
        if 'class="deleg"' not in nd['h']: continue
        parts = addr.split('.')
        cand_addrs = [addr]
        if len(parts) > 2: cand_addrs.append('.'.join(parts[:2]))
        if len(parts) > 1: cand_addrs.append(parts[0])
        def annotate(m):
            global n_annot
            layer, t_esc = m.group(1), m.group(2)
            t = _H.unescape(t_esc)
            cands = []
            for ca in cand_addrs:
                cands += [c for c in impl_map.get((lk, ca), []) if c[0] == layer]
            if not cands: return m.group(0)
            scored = sorted(((_common_prefix(t, c[2]), c) for c in cands), key=lambda x: -x[0])
            matched = [c for sc, c in scored if sc >= 4]
            use = matched if matched else [c for sc, c in scored]
            seen = set(); keys = []
            for c in use:
                k2 = c[0] + '@' + c[1]
                if k2 not in seen:
                    seen.add(k2); keys.append(k2)
            if not keys: return m.group(0)
            n_annot += 1
            return f'<span class="deleg" data-layer="{layer}" data-t="{t_esc}" data-impl="{"|".join(keys[:6])}">'
        nd['h'] = DELEG_SPAN.sub(annotate, nd['h'])

print('impl targets:', len(impl_map), '| annotated deleg spans:', n_annot)

for k, d in laws.items():
    json.dump(d, open(os.path.join(OUT, d['slug'] + '.json'), 'w', encoding='utf-8'),
              ensure_ascii=False, separators=(',', ':'))
meta = [dict(slug=d['slug'], key=d['key'], title=d['title'], num=d['num'], articles=len(d['arts']))
        for d in (hou, rei, kis, tsu)]
json.dump(meta, open(os.path.join(OUT, 'meta.json'), 'w', encoding='utf-8'), ensure_ascii=False)

print('terms:', len(TERM_DEFS))
print('ext_preview entries:', len(preview), f"{os.path.getsize(os.path.join(OUT,'ext_preview.json'))//1024}KB")
for k, d in laws.items():
    nterm = sum(n['h'].count('class="term"') for n in d['nodes'].values())
    print(f"{d['slug']}: {len(d['arts'])}項目 {len(d['order'])}ノード term出現{nterm} "
          f"{os.path.getsize(os.path.join(OUT, d['slug']+'.json'))//1024}KB")
