#!/usr/bin/env python3
"""CFC条文 → Lawzilla型ノードモデルJSON
仕組みの再現:
- ノードアドレス体系 ln66_6 / ln66_6.1 / ln66_6.1.1 / ln66_6.1.1.1
- 括弧入れ子の <span class="kakko"> / "kakko-2" / ... 化
- 条文参照（絶対・相対）の data-link 解決
- InnerBackLink（逆参照）生成
"""
import xml.etree.ElementTree as ET
import json, os, re, html as H

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "app", "data2")
os.makedirs(OUT, exist_ok=True)

KD = {'〇':0,'一':1,'二':2,'三':3,'四':4,'五':5,'六':6,'七':7,'八':8,'九':9}
IROHA = "イロハニホヘトチリヌルヲワカヨタレソツネナラムウヰノオクヤマケフコエテ"

def kanji2int(s):
    total = 0; cur = 0
    for ch in s:
        if ch == '千': total += (cur or 1) * 1000; cur = 0
        elif ch == '百': total += (cur or 1) * 100; cur = 0
        elif ch == '十': total += (cur or 1) * 10; cur = 0
        elif ch in KD: cur = KD[ch]
        else: return None
    return total + cur

def kanji_branch(s):
    """'六十六' or '六十六'... 「X条のY」のXY部 → '66' / '66_6'"""
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

# ---------- フラットノード抽出 ----------

def item_addr_part(title):
    """号タイトル 一/二/一の二 → '1'/'2'/'1_2'"""
    b = kanji_branch(title)
    return b

def extract_nodes(xml, ranges, law_key):
    """XML → ノード列 [{type,address,ka,title,sentence,art,tables}]"""
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
                part = item_addr_part(tt) or str(idx)
                ka = base_ka + f'第{part.replace("_","の")}号'
            elif depth == 2:
                i = IROHA.find(tt) + 1 if tt and tt in IROHA else idx
                part = str(i)
                ka = base_ka + tt
            else:
                m = re.match(r'[（(]?(\d+)', tt)
                part = m.group(1) if m else str(idx)
                ka = base_ka + tt
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
        ka0 = ('法' if law_key == 'cfc_hou' else '令') + lab
        nodes.append(dict(type='Article', address=addr0, ka=lab, title=text_of(a.find('ArticleTitle')) if a.find('ArticleTitle') is not None else lab,
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

# ---------- 参照解決 ----------

K = '[〇一二三四五六七八九十百千]+'
LAWNAMES = {
    '法人税法施行令': ('ext', '340CO0000000097'),
    '法人税法': ('ext', '340AC0000000034'),
    '所得税法施行令': ('ext', '340CO0000000096'),
    '所得税法': ('ext', '340AC0000000033'),
    '国税通則法': ('ext', '337AC0000000066'),
    '租税特別措置法施行令': ('rei', '332CO0000000043'),
    '租税特別措置法': ('hou', '332AC0000000026'),
}
REF = re.compile(
    '(?P<law>' + '|'.join(LAWNAMES) + r')?'
    r'(?P<hourei>法|令)?'
    r'第(?P<art>' + K + r')条(?P<artbr>(?:の' + K + r')*)'
    r'(?:第(?P<para>' + K + r')項)?'
    r'(?:第(?P<item>' + K + r')号(?P<itembr>(?:の' + K + r')*))?'
    r'|(?P<rel>前条|次条|前項|次項|前各項|前' + K + r'項|前号|次号|前各号|前' + K + r'号)'
    r'(?:第(?P<rpara>' + K + r')項)?'
    r'(?:第(?P<ritem>' + K + r')号)?'
)

class Resolver:
    """law_key: 'hou'/'rei'。known: {'hou':set(addr),'rei':set(addr)}"""
    def __init__(self, law_key, known, art_seq=None):
        self.lk = law_key; self.known = known
        self.art_seq = art_seq or []  # 条IDの文書順
        self.refs = []  # (src_addr, dst_lawkey, dst_addr)

    def ctx(self, addr):
        # ln66_6.2.1.1 -> art='66_6', para='2', item='1'
        m = re.match(r'ln([0-9_]+)(?:\.(\d+))?(?:\.([0-9_]+))?', addr)
        return m.group(1), m.group(2), m.group(3)

    def resolve(self, mseg, src_addr):
        art, para, item = self.ctx(src_addr)
        g = mseg.groupdict()
        law_key = self.lk
        ext_id = None
        if g['law']:
            kind, _id = LAWNAMES[g['law']]
            if kind == 'ext':
                ext_id = _id
            else:
                law_key = kind
        elif g['hourei']:
            # 施行令の中の「法第…」→ 措置法 / 法の中の「令第…」(まれ)
            law_key = 'hou' if g['hourei'] == '法' else 'rei'
        if g['art']:
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
            if ext_id:
                return ('ext', ext_id, addr)
            return (law_key, addr, None)
        rel = g['rel']
        if not rel: return None
        if law_key != self.lk: return None  # 「法第」+相対は無い
        def num_in(s):
            m = re.search('(' + K + ')', s)
            return kanji2int(m.group(1)) if m else 1
        if rel in ('前条', '次条'):
            if art in self.art_seq:
                i = self.art_seq.index(art) + (1 if rel == '次条' else -1)
                if 0 <= i < len(self.art_seq):
                    addr = 'ln' + self.art_seq[i]
                else:
                    return None
            elif '_' not in art:
                n = int(art) + (1 if rel == '次条' else -1)
                addr = f'ln{n}'
            else:
                return None
        elif rel == '前項' or rel == '次項':
            if not para: return None
            p = int(para) + (1 if rel == '次項' else -1)
            if p < 1: return None
            addr = f'ln{art}.{p}'
        elif rel.startswith('前') and rel.endswith('項'):
            if not para: return None
            n = num_in(rel[1:-1]) if rel != '前各項' else int(para) - 1
            p = int(para) - n
            if p < 1: p = 1
            addr = f'ln{art}.{p}'
        elif rel in ('前号', '次号'):
            if not item or not para or '_' in item: return None
            i = int(item) + (1 if rel == '次号' else -1)
            if i < 1: return None
            addr = f'ln{art}.{para}.{i}'
        else:
            return None  # 前各号・前X号は文脈依存が強いので非リンク
        if g['rpara']: addr += '.' + str(kanji2int(g['rpara']))
        if g['ritem']: addr += '.' + str(kanji2int(g['ritem']))
        return (law_key, addr, None)

    def linkify(self, text, src_addr):
        """テキスト断片内の参照をaタグ化（エスケープ込み）"""
        out = []
        pos = 0
        for m in REF.finditer(text):
            r = self.resolve(m, src_addr)
            label = m.group(0)
            ok = False
            if r:
                lk, addr, _ = r
                if lk == 'ext':
                    out.append(H.escape(text[pos:m.start()]))
                    q = f'?n={_}&amp;mode=only' if _ else ''
                    out.append(f'<a class="law" target="_blank" rel="noopener" href="https://lawzilla.jp/law/{addr}{q}">{H.escape(label)}</a>')
                    ok = True
                elif addr in self.known.get(lk, ()) or ('.' in addr and addr.split('.')[0] in self.known.get(lk, ())):
                    base = addr if addr in self.known.get(lk, ()) else addr.split('.')[0]
                    self.refs.append((src_addr, lk, addr if addr in self.known.get(lk, ()) else base))
                    out.append(H.escape(text[pos:m.start()]))
                    out.append(f'<a class="self" href="javascript:void(0)" data-link="{lk}@{addr}">{H.escape(label)}</a>')
                    ok = True
                else:
                    # 同一法令だが抜粋範囲外 → Lawzilla本体へフォールバック
                    lzid = {'hou': '332AC0000000026', 'rei': '332CO0000000043'}.get(lk)
                    if lzid:
                        out.append(H.escape(text[pos:m.start()]))
                        out.append(f'<a class="law" target="_blank" rel="noopener" href="https://lawzilla.jp/law/{lzid}?n={addr}&amp;mode=only">{H.escape(label)}</a>')
                        ok = True
            if not ok:
                out.append(H.escape(text[pos:m.end()]))
            else:
                pass
            pos = m.end()
        out.append(H.escape(text[pos:]))
        return ''.join(out)

def kakko_html(text, resolver, src_addr):
    """全角（）の入れ子を kakko / kakko-2 ... スパン化し、テキスト断片は参照リンク化"""
    out = []
    level = 0
    buf = []
    def flush():
        if buf:
            out.append(resolver.linkify(''.join(buf), src_addr))
            buf.clear()
    ok = text.count('（') == text.count('）')
    if not ok:
        return resolver.linkify(text, src_addr)
    for ch in text:
        if ch == '（':
            flush()
            level += 1
            cls = 'kakko' if level == 1 else f'kakko-{level}'
            out.append(f'<span class="{cls}">（')
        elif ch == '）':
            flush()
            out.append('）</span>')
            level = max(0, level - 1)
        else:
            buf.append(ch)
    flush()
    return ''.join(out)

# ---------- ビルド ----------

HOU_RANGES = [("66_6", "66_9_5"), ("40_4", "40_6")]
REI_RANGES = [("39_14", "39_20_9"), ("25_19", "25_27")]
GROUPS = {
 'cfc_hou': [
    ("内国法人の外国関係会社に係る所得の課税の特例", "66_6", "66_9"),
    ("特殊関係株主等である内国法人に係る特例", "66_9_2", "66_9_5"),
    ("居住者の外国関係会社に係る特例", "40_4", "40_6"),
 ],
 'cfc_rei': [
    ("外国関係会社の判定等", "39_14", "39_14_3"),
    ("適用対象金額の計算", "39_15", "39_15"),
    ("課税対象金額・部分課税対象金額等", "39_16", "39_17_5"),
    ("二重課税調整・申告", "39_18", "39_20_9"),
    ("居住者に係る規定", "25_19", "25_27"),
 ],
}

t_hou, n_hou, nodes_hou = extract_nodes('/tmp/sotokuso.xml', HOU_RANGES, 'cfc_hou')
t_rei, n_rei, nodes_rei = extract_nodes('/tmp/sotochirei.xml', REI_RANGES, 'cfc_rei')

known = {'hou': set(n['address'] for n in nodes_hou),
         'rei': set(n['address'] for n in nodes_rei)}

def build(law_key_short, slug, title, law_num, nodes, lawzilla_id):
    art_seq = [n['art'] for n in nodes if n['type'] == 'Article']
    res = Resolver(law_key_short, known, art_seq)
    arts = []
    order = []
    nmap = {}
    for n in nodes:
        order.append(n['address'])
        html_body = kakko_html(n['sentence'], res, n['address']) if n['sentence'] else ''
        ent = dict(t=n['type'], ka=n['ka'], ti=n['title'], h=html_body, art=n['art'])
        if n.get('caption'): ent['c'] = n['caption']
        if n['tables']: ent['tbl'] = n['tables']
        nmap[n['address']] = ent
        if n['type'] == 'Article':
            arts.append(dict(id=n['art'], address=n['address'], label=n['ka'], c=n.get('caption','')))
    groups = []
    def key(s): return tuple(int(p) for p in s.split('_'))
    for label, s, e in GROUPS[slug]:
        sel = [a for a in arts if key(s) <= key(a['id']) + (0,)*(3-len(key(a['id']))) and key(a['id']) + (0,)*(3-len(key(a['id']))) <= key(e) + (0,)*(3-len(key(e)))]
        # 簡易: 範囲判定をパディングで
        def pad(t, L): return t + (0,)*(L-len(t))
        sel = []
        for a in arts:
            k, ks, ke = key(a['id']), key(s), key(e)
            L = max(len(k), len(ks), len(ke))
            if pad(ks, L) <= pad(k, L) <= pad(ke, L): sel.append(a['id'])
        if sel: groups.append(dict(label=label, arts=sel))
    grouped = set(x for g in groups for x in g['arts'])
    rest = [a['id'] for a in arts if a['id'] not in grouped]
    if rest: groups.append(dict(label='その他', arts=rest))
    return dict(slug=slug, key=law_key_short, title=title, num=law_num,
                lawzilla=lawzilla_id, arts=arts, groups=groups,
                order=order, nodes=nmap), res.refs

hou, refs_hou = build('hou', 'cfc_hou', t_hou + '（CFC税制抜粋）', n_hou, nodes_hou, '332AC0000000026')
rei, refs_rei = build('rei', 'cfc_rei', t_rei + '（CFC税制抜粋）', n_rei, nodes_rei, '332CO0000000043')

# InnerBackLink: dst -> [ {law, addr, ka} ]
back = {'hou': {}, 'rei': {}}
all_nodes = {'hou': hou['nodes'], 'rei': rei['nodes']}
for src_law, refs in (('hou', refs_hou), ('rei', refs_rei)):
    for src_addr, dst_law, dst_addr in refs:
        # 自己参照（同一条内）もLawzillaは載せるが、条単位で集約
        ka = all_nodes[src_law][src_addr]['ka'] if src_addr in all_nodes[src_law] else src_addr
        ka = ('法' if src_law == 'hou' else '令') + ka
        b = back[dst_law].setdefault(dst_addr, [])
        ent = dict(law=src_law, addr=src_addr, ka=ka)
        if not any(x['addr'] == src_addr and x['law'] == src_law for x in b):
            b.append(ent)
hou['backlinks'] = back['hou']
rei['backlinks'] = back['rei']

json.dump(hou, open(os.path.join(OUT, 'cfc_hou.json'), 'w', encoding='utf-8'), ensure_ascii=False, separators=(',', ':'))
json.dump(rei, open(os.path.join(OUT, 'cfc_rei.json'), 'w', encoding='utf-8'), ensure_ascii=False, separators=(',', ':'))
meta = [dict(slug='cfc_hou', key='hou', title=hou['title'], num=hou['num'], articles=len(hou['arts'])),
        dict(slug='cfc_rei', key='rei', title=rei['title'], num=rei['num'], articles=len(rei['arts']))]
json.dump(meta, open(os.path.join(OUT, 'meta.json'), 'w', encoding='utf-8'), ensure_ascii=False)

print(f"hou: {len(hou['arts'])}条 {len(hou['order'])}ノード refs={len(refs_hou)} backlinked={len(hou['backlinks'])}")
print(f"rei: {len(rei['arts'])}条 {len(rei['order'])}ノード refs={len(refs_rei)} backlinked={len(rei['backlinks'])}")
for f in os.listdir(OUT):
    print(f, os.path.getsize(os.path.join(OUT, f))//1024, 'KB')
