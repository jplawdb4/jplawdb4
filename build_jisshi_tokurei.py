#!/usr/bin/env python3
"""
build_jisshi_tokurei.py — 実施特例法関係（法令3本＋通達）を jplawdb4 に収録

使い方:
  python3 build_jisshi_tokurei.py              # 全件実行
  python3 build_jisshi_tokurei.py --law-only   # 法令のみ
  python3 build_jisshi_tokurei.py --dry-run    # 確認のみ
"""

import argparse
import json
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional

import tiktoken

JPLAWDB4   = Path("/home/user/jplawdb4")
CACHE_DIR  = JPLAWDB4 / ".insert_tables_cache"
MAX_TOKENS = 9999
ENC        = tiktoken.get_encoding("cl100k_base")
RATE_LIMIT = 1.5

API_V2 = "https://laws.e-gov.go.jp/api/2"
API_V1 = "https://laws.e-gov.go.jp/api/1"

# ── 対象法令 ──────────────────────────────────────────────────────

LAWS = [
    {
        "lawdir":   "jisshi_tokurei",
        "law_name": "租税条約等の実施に伴う所得税法、法人税法及び地方税法の特例等に関する法律",
        "law_type": "act",
        "law_num":  "昭和四十四年法律第四十六号",
        "egov_id":  "344AC0000000046",
        "as_of":    "2026-01-01",
        "api_mode": "v2",
    },
    {
        "lawdir":   "jisshi_tokurei_seirei",
        "law_name": "租税条約等の実施に伴う所得税法、法人税法及び地方税法の特例等に関する法律施行令",
        "law_type": "order",
        "law_num":  "昭和六十二年政令第三百三十五号",
        "egov_id":  "362CO0000000335",
        "as_of":    "2026-01-01",
        "api_mode": "v2",
    },
    {
        "lawdir":   "jisshi_tokurei_kisoku",
        "law_name": "租税条約等の実施に伴う所得税法、法人税法及び地方税法の特例等に関する法律の施行に関する省令",
        "law_type": "rule",
        "law_num":  "昭和四十四年大蔵省・自治省令第一号",
        "egov_id":  "",
        "as_of":    "2026-01-01",
        "api_mode": "v1",
    },
]


# ── ユーティリティ ────────────────────────────────────────────────

def count_tokens(text: str) -> int:
    return len(ENC.encode(text))


def get_text(node) -> str:
    """JSON ノードからテキストを再帰的に抽出（Rt=ルビ読みは除外）"""
    if isinstance(node, str):
        return node
    if isinstance(node, dict):
        if node.get("tag") == "Rt":
            return ""
        return "".join(get_text(c) for c in node.get("children", []))
    if isinstance(node, list):
        return "".join(get_text(c) for c in node)
    return ""


def num_to_filename(num_str: str) -> str:
    """Article.Num '3_2' → ファイル名 '3-2'"""
    return num_str.replace("_", "-")


_last_req = 0.0


def rate_limit_wait():
    global _last_req
    elapsed = time.time() - _last_req
    if elapsed < RATE_LIMIT:
        time.sleep(RATE_LIMIT - elapsed)
    _last_req = time.time()


# ── e-Gov API 取得 ───────────────────────────────────────────────

def fetch_law_v2(egov_id: str) -> Optional[dict]:
    """v2 API で JSON ツリーを取得（キャッシュ付き）"""
    CACHE_DIR.mkdir(exist_ok=True)
    cache = CACHE_DIR / f"{egov_id}.json"
    if cache.exists():
        print(f"  キャッシュ使用: {cache.name}")
        return json.loads(cache.read_text(encoding="utf-8"))
    rate_limit_wait()
    url = f"{API_V2}/law_data/{egov_id}"
    print(f"  API取得: {url}")
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        tree = data.get("law_full_text", data)
        cache.write_text(json.dumps(tree, ensure_ascii=False), encoding="utf-8")
        return tree
    except Exception as e:
        print(f"  v2 API失敗: {e}")
        return None


def fetch_law_v1(law_num: str) -> Optional[str]:
    """v1 API で XML テキストを取得（キャッシュ付き）"""
    CACHE_DIR.mkdir(exist_ok=True)
    key = "v1_" + urllib.parse.quote(law_num, safe="")
    cache = CACHE_DIR / f"{key}.xml"
    if cache.exists():
        print(f"  キャッシュ使用: {cache.name}")
        return cache.read_text(encoding="utf-8")
    rate_limit_wait()
    url = f"{API_V1}/lawdata/{urllib.parse.quote(law_num)}"
    print(f"  API取得: {url}")
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            xml_text = resp.read().decode("utf-8")
        cache.write_text(xml_text, encoding="utf-8")
        return xml_text
    except Exception as e:
        print(f"  v1 API失敗: {e}")
        return None


# ── JSON パーサー (v2 API) ───────────────────────────────────────

def extract_articles_json(tree: dict) -> list:
    """MainProvision 内の全 Article を解析。
    Returns: [(num, title, paragraphs), ...]
    """
    # MainProvision を探す
    main_prov = _find_node(tree, "MainProvision")
    if not main_prov:
        print("  WARNING: MainProvision not found")
        return []

    articles = []
    _collect_articles(main_prov, articles)
    return articles


def _find_node(node, tag: str):
    if isinstance(node, dict):
        if node.get("tag") == tag:
            return node
        for c in node.get("children", []):
            r = _find_node(c, tag)
            if r:
                return r
    elif isinstance(node, list):
        for item in node:
            r = _find_node(item, tag)
            if r:
                return r
    return None


def _collect_articles(node, results: list):
    """SupplProvision を除き Article を収集"""
    if isinstance(node, dict):
        if node.get("tag") == "SupplProvision":
            return
        if node.get("tag") == "Article":
            art = _parse_article_json(node)
            if art:
                results.append(art)
        else:
            for c in node.get("children", []):
                _collect_articles(c, results)
    elif isinstance(node, list):
        for item in node:
            _collect_articles(item, results)


def _parse_article_json(node: dict) -> Optional[tuple]:
    """Article ノード → (num, title, paragraphs)"""
    num = node.get("attr", {}).get("Num", "")
    if not num:
        return None

    title = ""
    paragraphs = []

    for c in node.get("children", []):
        if not isinstance(c, dict):
            continue
        tag = c.get("tag", "")
        if tag == "ArticleTitle":
            title = get_text(c).strip()
        elif tag == "Paragraph":
            para = _parse_paragraph_json(c)
            if para:
                paragraphs.append(para)

    return (num, title, paragraphs)


def _parse_paragraph_json(node: dict) -> Optional[tuple]:
    """Paragraph → (para_num, para_text, items)"""
    para_num = node.get("attr", {}).get("Num", "1")
    para_text = ""
    items = []

    for c in node.get("children", []):
        if not isinstance(c, dict):
            continue
        tag = c.get("tag", "")
        if tag == "ParagraphSentence":
            para_text = get_text(c).strip()
        elif tag == "Item":
            item = _parse_item_json(c)
            if item:
                items.append(item)

    return (para_num, para_text, items)


def _parse_item_json(node: dict) -> Optional[tuple]:
    """Item → (item_num, item_title, item_text, subitems)"""
    item_num = node.get("attr", {}).get("Num", "")
    item_title = ""
    item_text = ""
    subitems = []

    for c in node.get("children", []):
        if not isinstance(c, dict):
            continue
        tag = c.get("tag", "")
        if tag == "ItemTitle":
            item_title = get_text(c).strip()
        elif tag == "ItemSentence":
            item_text = get_text(c).strip()
        elif tag.startswith("Subitem"):
            _collect_subitems_json(c, subitems, depth=1)

    return (item_num, item_title, item_text, subitems)


def _collect_subitems_json(node: dict, results: list, depth: int):
    """Subitem を再帰的に収集"""
    tag = node.get("tag", "")
    if not tag.startswith("Subitem"):
        return

    sub_num = node.get("attr", {}).get("Num", "")
    sub_title = ""
    sub_text = ""

    for c in node.get("children", []):
        if not isinstance(c, dict):
            continue
        ct = c.get("tag", "")
        if ct.endswith("Title"):
            sub_title = get_text(c).strip()
        elif ct.endswith("Sentence"):
            sub_text = get_text(c).strip()
        elif ct.startswith("Subitem"):
            _collect_subitems_json(c, results, depth + 1)

    results.append((depth, sub_num, sub_title, sub_text))


# ── XML パーサー (v1 API) ────────────────────────────────────────

def extract_articles_xml(xml_text: str) -> list:
    """v1 XML の MainProvision 内 Article を解析"""
    root = ET.fromstring(xml_text)

    # MainProvision を探す
    mp = root.find(".//MainProvision")
    if mp is None:
        print("  WARNING: MainProvision not found in XML")
        return []

    articles = []
    for art_el in mp.iter("Article"):
        # SupplProvision 内の Article を除外
        parent = _xml_parent_map(root).get(art_el)
        if parent is not None and _is_under_suppl(art_el, root):
            continue
        art = _parse_article_xml(art_el)
        if art:
            articles.append(art)

    return articles


def _xml_parent_map(root):
    """ElementTree の親マップをキャッシュ付きで生成"""
    if not hasattr(_xml_parent_map, "_cache"):
        _xml_parent_map._cache = {}
    rid = id(root)
    if rid not in _xml_parent_map._cache:
        _xml_parent_map._cache[rid] = {c: p for p in root.iter() for c in p}
    return _xml_parent_map._cache[rid]


def _is_under_suppl(el, root):
    """要素が SupplProvision 配下かどうか"""
    pmap = _xml_parent_map(root)
    current = el
    while current in pmap:
        current = pmap[current]
        if current.tag == "SupplProvision":
            return True
    return False


def _parse_article_xml(art_el) -> Optional[tuple]:
    """XML Article → (num, title, paragraphs)"""
    num = art_el.get("Num", "")
    if not num:
        return None

    title_el = art_el.find("ArticleTitle")
    title = _xml_text(title_el) if title_el is not None else ""

    paragraphs = []
    for para_el in art_el.findall("Paragraph"):
        para = _parse_paragraph_xml(para_el)
        if para:
            paragraphs.append(para)

    return (num, title, paragraphs)


def _parse_paragraph_xml(para_el) -> Optional[tuple]:
    """XML Paragraph → (para_num, para_text, items)"""
    para_num = para_el.get("Num", "1")

    ps_el = para_el.find("ParagraphSentence")
    para_text = _xml_text(ps_el) if ps_el is not None else ""

    items = []
    for item_el in para_el.findall("Item"):
        item = _parse_item_xml(item_el)
        if item:
            items.append(item)

    return (para_num, para_text, items)


def _parse_item_xml(item_el) -> Optional[tuple]:
    """XML Item → (item_num, item_title, item_text, subitems)"""
    item_num = item_el.get("Num", "")

    title_el = item_el.find("ItemTitle")
    item_title = _xml_text(title_el) if title_el is not None else ""

    sent_el = item_el.find("ItemSentence")
    item_text = _xml_text(sent_el) if sent_el is not None else ""

    subitems = []
    for sub_el in item_el:
        if sub_el.tag.startswith("Subitem"):
            _collect_subitems_xml(sub_el, subitems, depth=1)

    return (item_num, item_title, item_text, subitems)


def _collect_subitems_xml(el, results: list, depth: int):
    """XML Subitem を再帰的に収集"""
    if not el.tag.startswith("Subitem"):
        return

    sub_num = el.get("Num", "")
    sub_title = ""
    sub_text = ""

    for c in el:
        if c.tag.endswith("Title"):
            sub_title = _xml_text(c)
        elif c.tag.endswith("Sentence"):
            sub_text = _xml_text(c)
        elif c.tag.startswith("Subitem"):
            _collect_subitems_xml(c, results, depth + 1)

    results.append((depth, sub_num, sub_title, sub_text))


def _xml_text(el) -> str:
    """ElementTree 要素からテキスト抽出（Rt 除外）"""
    if el is None:
        return ""
    parts = []
    for node in el.iter():
        if node.tag == "Rt":
            continue
        if node.text:
            parts.append(node.text)
        if node.tail and node is not el:
            # Rt の tail は含めない
            parent = None
            for p in el.iter():
                if node in list(p):
                    parent = p
                    break
            if parent is not None and parent.tag == "Rt":
                continue
            parts.append(node.tail)
    return "".join(parts).strip()


# ── テキスト生成 ─────────────────────────────────────────────────

def format_article(num: str, title: str, paragraphs: list, law_info: dict) -> str:
    """1条分のテキストファイル内容を生成"""
    filename = num_to_filename(num)

    # ヘッダー
    lines = [
        f"law: {law_info['law_name']} ({law_info['lawdir']})",
    ]
    meta = f"law_type: {law_info['law_type']} / law_num: {law_info['law_num']}"
    if law_info.get("egov_id"):
        meta += f" / egov_id: {law_info['egov_id']}"
    meta += f" / as_of: {law_info['as_of']}"
    lines.append(meta)
    lines.append(f"article: {filename} / title: {title}")
    lines.append("")  # 空行でヘッダー終了

    # 本文
    multi_para = len(paragraphs) > 1

    for para_num_str, para_text, items in paragraphs:
        pn = int(para_num_str) if para_num_str.isdigit() else 1

        # 項テキスト
        if para_text:
            prefix = f"[p{pn}]"
            if multi_para:
                lines.append(f"{prefix} {pn} {para_text}")
            else:
                lines.append(f"{prefix} {para_text}")

        # 号
        for item_num, item_title, item_text, subitems in items:
            in_str = item_num if item_num else "?"
            marker = f"[p{pn}-i{in_str}]"
            text = f"{item_title} {item_text}".strip() if item_title else item_text
            lines.append(f"{marker} {text}")

            # サブアイテム
            for depth, sub_num, sub_title, sub_text in subitems:
                sn = sub_num if sub_num else "?"
                marker = f"[p{pn}-i{in_str}-s{sn}]"
                text = f"{sub_title} {sub_text}".strip() if sub_title else sub_text
                lines.append(f"{marker} {text}")

    return "\n".join(lines) + "\n"


# ── 法令ビルド ───────────────────────────────────────────────────

def build_law(law_info: dict, dry_run: bool = False) -> list:
    """1法令の全条文ファイルを生成。Returns: [created_paths]"""
    lawdir   = law_info["lawdir"]
    api_mode = law_info["api_mode"]
    out_dir  = JPLAWDB4 / "law" / "text" / lawdir

    print(f"\n{'='*60}")
    print(f"【{law_info['law_name']}】({lawdir})")
    print(f"{'='*60}")

    # API取得
    if api_mode == "v2":
        tree = fetch_law_v2(law_info["egov_id"])
        if not tree:
            print("  ERROR: API取得失敗")
            return []
        articles = extract_articles_json(tree)
    else:
        xml_text = fetch_law_v1(law_info["law_num"])
        if not xml_text:
            print("  ERROR: API取得失敗")
            return []
        articles = extract_articles_xml(xml_text)

    print(f"  本則条文: {len(articles)}条")

    if dry_run:
        for num, title, paragraphs in articles:
            fn = num_to_filename(num)
            text = format_article(num, title, paragraphs, law_info)
            tok = count_tokens(text)
            over = " ⚠️ OVER" if tok > MAX_TOKENS else ""
            print(f"    {fn}.txt ({tok:,} tok){over}")
        return []

    # ファイル生成
    out_dir.mkdir(parents=True, exist_ok=True)
    created = []

    for num, title, paragraphs in articles:
        fn = num_to_filename(num)
        text = format_article(num, title, paragraphs, law_info)
        tok = count_tokens(text)

        fpath = out_dir / f"{fn}.txt"
        fpath.write_text(text, encoding="utf-8")

        over = " ⚠️ OVER" if tok > MAX_TOKENS else ""
        print(f"    {fn}.txt ({tok:,} tok){over}")
        created.append(fpath)

    return created


# ── 通達ビルド ───────────────────────────────────────────────────

NTA_TSUTATSU_INDEX = "https://www.nta.go.jp/law/tsutatsu/kobetsu/jisshi/index.htm"

def build_tsutatsu(dry_run: bool = False) -> list:
    """NTA サイトから実施特例法関係通達を取得"""
    out_dir = JPLAWDB4 / "tsutatsu" / "text" / "jisshi_tokurei_tsutatsu"

    print(f"\n{'='*60}")
    print("【実施特例法関係通達】")
    print(f"{'='*60}")

    # NTA通達インデックスページを取得
    print(f"  NTA通達インデックス取得中...")

    try:
        req = urllib.request.Request(
            NTA_TSUTATSU_INDEX,
            headers={"User-Agent": "jplawdb4-builder/1.0"}
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  WARNING: NTA通達取得失敗: {e}")
        print("  通達目次ページの URL が不正の可能性があります。")
        print("  手動で NTA サイトを確認し、正しい URL を設定してください。")
        print("  スキップします。")
        return []

    # 通達ページへのリンクを抽出
    links = re.findall(r'href="([^"]*\.htm)"', html)
    tsutatsu_links = [l for l in links if "jisshi" in l.lower() or "tokurei" in l.lower()]

    if not tsutatsu_links:
        print("  WARNING: 通達リンクが見つかりません。NTA URL構造が変更された可能性。")
        print("  スキップします。")
        return []

    print(f"  通達ページ: {len(tsutatsu_links)}件")

    if dry_run:
        for link in tsutatsu_links[:10]:
            print(f"    {link}")
        return []

    out_dir.mkdir(parents=True, exist_ok=True)
    created = []

    for link in tsutatsu_links:
        time.sleep(RATE_LIMIT)
        full_url = urllib.parse.urljoin(NTA_TSUTATSU_INDEX, link)

        try:
            req = urllib.request.Request(
                full_url,
                headers={"User-Agent": "jplawdb4-builder/1.0"}
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                page_html = resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            print(f"    SKIP {link}: {e}")
            continue

        # HTMLからテキスト抽出（簡易）
        items = parse_tsutatsu_page(page_html, full_url)
        for item_id, title, body in items:
            header = (
                f"doc: 実施特例法関係通達 (jisshi_tokurei_tsutatsu)\n"
                f"item: {item_id} / title: {title}\n"
                f"source_url: {full_url}\n"
            )
            content = header + "\n" + body + "\n"
            fpath = out_dir / f"{item_id}.txt"
            fpath.write_text(content, encoding="utf-8")
            tok = count_tokens(content)
            print(f"    {item_id}.txt ({tok:,} tok)")
            created.append(fpath)

    return created


def parse_tsutatsu_page(html: str, url: str) -> list:
    """NTA通達HTMLページから (item_id, title, body) のリストを抽出"""
    # HTMLタグ除去（簡易版）
    text = re.sub(r'<br\s*/?>', '\n', html, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'&nbsp;', ' ', text)
    text = re.sub(r'&lt;', '<', text)
    text = re.sub(r'&gt;', '>', text)
    text = re.sub(r'&amp;', '&', text)
    text = re.sub(r'\r\n', '\n', text)

    # 通達番号パターンで分割
    # 例: "（趣旨）" の前に "3の2－1" のような番号
    pattern = re.compile(
        r'^[\s　]*(\d+(?:の\d+)*(?:－\d+(?:の\d+)*)?)\s*[（(]([^）)]+)[）)]\s*$',
        re.MULTILINE
    )

    matches = list(pattern.finditer(text))
    if not matches:
        # フォールバック: ページ全体を1アイテムとして
        # URLからitem_idを推測
        fname = url.rstrip("/").split("/")[-1].replace(".htm", "")
        body = "\n".join(line.strip() for line in text.split("\n")
                        if line.strip() and not line.strip().startswith("Copyright"))
        if body.strip():
            return [(fname, fname, f"[p1] {body}")]
        return []

    items = []
    for i, m in enumerate(matches):
        raw_num = m.group(1)
        title = m.group(2)
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        body_raw = text[start:end].strip()

        # item_id変換: 「の」→「-」、「－」→「-」
        item_id = raw_num.replace("の", "-").replace("－", "-")

        # [p1] マーカー付与
        body_lines = []
        para_num = 1
        for line in body_raw.split("\n"):
            line = line.strip()
            if not line:
                continue
            if line.startswith("Copyright") or line.startswith("ページの先頭"):
                continue
            body_lines.append(f"[p{para_num}] {line}")
            para_num += 1

        if body_lines:
            items.append((item_id, title, "\n".join(body_lines)))

    return items


# ── メイン ───────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="build_jisshi_tokurei.py")
    parser.add_argument("--dry-run", action="store_true", help="確認のみ")
    parser.add_argument("--law-only", action="store_true", help="法令のみ（通達スキップ）")
    args = parser.parse_args()

    all_created = []

    # Phase 1: 法令（3本）
    for law_info in LAWS:
        files = build_law(law_info, dry_run=args.dry_run)
        all_created.extend(files)

    # Phase 2: 通達
    if not args.law_only:
        tsutatsu_files = build_tsutatsu(dry_run=args.dry_run)
        all_created.extend(tsutatsu_files)

    if args.dry_run:
        print(f"\n=== DRY RUN 完了 ===")
        return

    # Phase 3: トークン超過チェック＆分割
    print(f"\n{'='*60}")
    print("トークン超過チェック")
    oversized = []
    for p in all_created:
        if p.exists():
            tok = count_tokens(p.read_text(encoding="utf-8"))
            if tok > MAX_TOKENS:
                oversized.append((p, tok))
                print(f"  ⚠️  超過: {p.name} ({tok:,} tok)")

    if oversized:
        print("\nsplit_oversized.py を実行...")
        for db in ("law", "tsutatsu"):
            result = subprocess.run(
                ["python3", str(JPLAWDB4 / "split_oversized.py"), "--db", db],
                capture_output=True, text=True, cwd=str(JPLAWDB4)
            )
            if result.returncode != 0:
                print(f"  ERROR ({db}): {result.stderr[:300]}")
            else:
                for line in result.stdout.splitlines():
                    if "split" in line.lower() or "oversized" in line.lower() or "chunk" in line.lower():
                        print(f"  {line}")
    else:
        print("  全ファイル 9,999 tok 以内 ✓")

    # Phase 4: 整合性確認
    print(f"\n{'='*60}")
    print("整合性確認")
    result = subprocess.run(
        ["python3", str(JPLAWDB4 / "verify_integrity.py")],
        capture_output=True, text=True, cwd=str(JPLAWDB4)
    )
    for line in result.stdout.splitlines():
        if any(kw in line for kw in ["jisshi", "PASS", "ISSUE", "ERROR", "🎉", "❌", "law", "tsutatsu"]):
            print(f"  {line}")

    # サマリー
    law_count = sum(1 for p in all_created if "law/text/" in str(p))
    tsu_count = sum(1 for p in all_created if "tsutatsu/text/" in str(p))
    print(f"\n{'='*60}")
    print(f"✅ 完了: 法令 {law_count}条文 + 通達 {tsu_count}項目")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
