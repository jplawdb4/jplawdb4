#!/usr/bin/env python3
"""CFC税制関連条文だけを抽出してSPA用JSONを生成"""
import xml.etree.ElementTree as ET
import json, os, re

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "app", "data")
os.makedirs(OUT, exist_ok=True)

import importlib.util
_spec = importlib.util.spec_from_file_location("bad", os.path.join(os.path.dirname(os.path.abspath(__file__)), "build_app_data.py"))
# build_app_data はトップレベルで全法令を生成するため import せず、関数定義部のみ exec する
_src = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "build_app_data.py"), encoding="utf-8").read()
_defs = _src.split("for cfg in LAWS:")[0]
exec(compile(_defs, "build_app_data_defs", "exec"))

def in_range(num, start, end):
    """Num='66_6_2' を (66,6,2) として start<=x<=end 判定"""
    def key(s):
        parts = re.split(r"[_:]", s)
        if not all(p.isdigit() for p in parts):
            return None
        return tuple(int(p) for p in parts)
    k, a, b = key(num), key(start), key(end)
    if k is None:
        return False
    # 比較のため長さを揃える（不足は0埋め）
    L = max(len(k), len(a), len(b))
    pad = lambda t: t + (0,) * (L - len(t))
    return pad(a) <= pad(k) <= pad(b)

def extract(xml, slug, title, ranges, groups):
    """ranges: [(start,end)] 該当条文を抽出し、groups: [(label, start, end)] で章立て"""
    tree = ET.parse(xml)
    law = tree.getroot().find(".//Law")
    body = law.find("LawBody")
    law_num = text_of(law.find("LawNum"))
    arts = []
    for a in body.find("MainProvision").iter("Article"):
        num = a.get("Num", "")
        if any(in_range(num, s, e) for s, e in ranges):
            arts.append(article(a))
    chapters = []
    used = set()
    for label, s, e in groups:
        sel = [a for a in arts if in_range(a["id"], s, e) and a["id"] not in used]
        used.update(a["id"] for a in sel)
        if sel:
            chapters.append(dict(label=label, secs=[dict(label="", arts=sel)]))
    rest = [a for a in arts if a["id"] not in used]
    if rest:
        chapters.append(dict(label="その他", secs=[dict(label="", arts=rest)]))
    data = dict(slug=slug, title=title, num=law_num, chapters=chapters)
    fn = os.path.join(OUT, slug + ".json")
    json.dump(data, open(fn, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))
    n = len(arts)
    print(f"{title}: {len(chapters)}グループ {n}条 -> {os.path.getsize(fn)//1024}KB")
    return dict(slug=slug, title=title, num=law_num, chapters=len(chapters), articles=n)

meta = []
# 措置法: 66の6〜66の9の5（内国法人CFC）＋40の4〜40の6（居住者CFC）
meta.append(extract(
    "/tmp/sotokuso.xml", "cfc_hou", "租税特別措置法（CFC税制抜粋）",
    ranges=[("66_6", "66_9_5"), ("40_4", "40_6")],
    groups=[
        ("内国法人の外国関係会社に係る所得の課税の特例（66条の6〜66条の9）", "66_6", "66_9"),
        ("特殊関係株主等である内国法人に係る特例（66条の9の2〜66条の9の5）", "66_9_2", "66_9_5"),
        ("居住者の外国関係会社に係る特例（40条の4〜40条の6）", "40_4", "40_6"),
    ]))
# 施行令: 39の14〜39の20の9＋25の19〜25の27（居住者側）
meta.append(extract(
    "/tmp/sotochirei.xml", "cfc_rei", "租税特別措置法施行令（CFC税制抜粋）",
    ranges=[("39_14", "39_20_9"), ("25_19", "25_27")],
    groups=[
        ("外国関係会社の判定等（39条の14〜39条の14の3）", "39_14", "39_14_3"),
        ("適用対象金額の計算（39条の15）", "39_15", "39_15"),
        ("課税対象金額・部分課税対象金額等（39条の16〜39条の17の5）", "39_16", "39_17_5"),
        ("二重課税調整・申告（39条の18〜39条の20の9）", "39_18", "39_20_9"),
        ("居住者に係る規定（25条の19〜25条の27）", "25_19", "25_27"),
    ]))

json.dump(meta, open(os.path.join(OUT, "meta.json"), "w", encoding="utf-8"), ensure_ascii=False)
print("meta.json done (CFC only)")
