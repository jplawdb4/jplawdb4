import html
import pathlib
import re

import pdfplumber

PDF = pathlib.Path("nta_gensen_aramashi2024_12.pdf")
OUT = pathlib.Path("withholding_tax_treaty_rates_nta_20260531.html")


def clean_cell(value):
    if value is None:
        return ""
    value = value.replace("\b", "")
    value = re.sub(r"[ \u3000]+", " ", value)
    value = re.sub(r" *\n *", "\n", value)
    return value.strip()


def clean_country(value):
    value = clean_cell(value)
    lines = []
    for line in value.splitlines():
        line = re.sub(r"(?<=[ァ-ヶ一-龥]) (?=[ァ-ヶ一-龥])", "", line)
        line = re.sub(r"(?<=[ァ-ヶ一-龥]) (?=[ァ-ヶ一-龥])", "", line)
        lines.append(line.strip())
    return "\n".join(line for line in lines if line)


def cell_html(value):
    value = clean_cell(value)
    return "<br>".join(html.escape(line) for line in value.splitlines())


def country_html(value):
    value = clean_country(value)
    return "<br>".join(html.escape(line) for line in value.splitlines())


def collect_rows(raw):
    rows = []
    for page in raw:
        for table in page["tables"]:
            if not table or not table[0]:
                continue
            first = "".join(clean_cell(c) for c in table[0] if c)
            if "国・ 地 域 名" not in first and "国・地域名" not in first:
                continue
            for row in table[2:]:
                if len(row) < 5:
                    continue
                if not clean_cell(row[0]) and not clean_cell(row[1]):
                    continue
                rows.append(
                    {
                        "page": page["page"],
                        "country": row[0],
                        "interest": row[1],
                        "dividend": row[2],
                        "royalty": row[3],
                        "note": row[4],
                    }
                )
    return rows


def extract_raw():
    raw = []
    with pdfplumber.open(PDF) as pdf:
        for idx in range(55, 67):
            raw.append({"page": idx + 1, "tables": pdf.pages[idx].extract_tables()})
    return raw


def main():
    raw = extract_raw()
    rows = collect_rows(raw)
    table_rows = "\n".join(
        f"""
        <tr>
          <td class="country">{country_html(row["country"])}</td>
          <td>{cell_html(row["interest"])}</td>
          <td>{cell_html(row["dividend"])}</td>
          <td>{cell_html(row["royalty"])}</td>
          <td class="note">{cell_html(row["note"])}</td>
          <td class="page">PDF p.{row["page"]}</td>
        </tr>"""
        for row in rows
    )

    css = """
:root{--ink:#172033;--muted:#657084;--line:#d8dde8;--bg:#f4f6fa;--paper:#fff;--head:#123242;--sub:#eef6f8}
*{box-sizing:border-box}
body{margin:0;font-family:"Yu Gothic",YuGothic,Meiryo,system-ui,sans-serif;color:var(--ink);background:var(--bg);line-height:1.65}
header{background:var(--head);color:#fff;padding:28px 22px 22px;border-bottom:5px solid #8bb8c6}
.wrap,main{max-width:1280px;margin:0 auto}
main{padding:0 16px 56px}
h1{margin:0 0 8px;font-size:clamp(24px,3vw,38px);line-height:1.25;letter-spacing:0}
.subtitle{margin:0;color:#d7e8ee}
section{background:var(--paper);border:1px solid var(--line);border-radius:8px;margin:18px 0;padding:20px}
h2{margin:0 0 10px;font-size:20px;line-height:1.35}
p{margin:0 0 10px}
a{color:#15546b}
.meta{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;margin-top:14px}
.meta div{border:1px solid var(--line);border-radius:6px;padding:10px 12px;background:#fbfcfe}
.label{display:block;color:var(--muted);font-size:12px}
.tools{display:flex;gap:10px;flex-wrap:wrap;margin-top:12px}
input[type="search"]{width:min(100%,480px);min-height:38px;border:1px solid var(--line);border-radius:6px;padding:6px 10px;font:inherit}
.count{color:var(--muted);font-size:14px;align-self:center}
.table-wrap{overflow:auto;border:1px solid var(--line);border-radius:8px;background:#fff}
table{width:100%;border-collapse:separate;border-spacing:0;min-width:980px;font-size:14px}
th,td{border-right:1px solid var(--line);border-bottom:1px solid var(--line);padding:9px 10px;vertical-align:top}
th:last-child,td:last-child{border-right:0}
tbody tr:last-child td{border-bottom:0}
thead th{position:sticky;top:0;background:#e8f0f3;z-index:1;text-align:center;font-weight:700}
td:not(.country):not(.note):not(.page){text-align:center;white-space:nowrap}
.country{width:220px;font-weight:700;background:#fbfcfe}
.note{width:390px}
.page{width:78px;color:var(--muted);font-size:12px;text-align:center;white-space:nowrap}
.source-note{background:#fff8e4;border-color:#e7d392}
mark{background:#fff0a8;padding:0 1px}
@media(max-width:760px){section{padding:16px}.meta{grid-template-columns:1fr}table{font-size:13px;min-width:900px}}
@media print{body{background:#fff}header,.tools{display:none}section{border:0;padding:0}.table-wrap{border:0;overflow:visible}thead th{position:static}table{font-size:10px;min-width:0}}
"""
    script = """
const q = document.getElementById('q');
const rows = Array.from(document.querySelectorAll('tbody tr'));
const count = document.getElementById('count');
function norm(s){ return s.toLowerCase().replace(/\\s+/g,''); }
function applyFilter(){
  const term = norm(q.value);
  let shown = 0;
  rows.forEach(row => {
    const ok = !term || norm(row.innerText).includes(term);
    row.style.display = ok ? '' : 'none';
    if (ok) shown++;
  });
  count.textContent = `${shown} / ${rows.length}件`;
}
q.addEventListener('input', applyFilter);
applyFilter();
"""
    doc = f"""<!doctype html>
<html lang="ja">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>各国への支払の源泉税率（租税条約） | テキストHTML</title>
  <style>{css}</style>
</head>
<body>
  <header>
    <div class="wrap">
      <h1>各国への支払の源泉税率（租税条約）</h1>
      <p class="subtitle">国税庁「令和7年版 源泉徴収のあらまし」第10 参考表のテキストHTML化</p>
    </div>
  </header>
  <main>
    <section>
      <h2>このHTMLについて</h2>
      <p>国税庁PDFの「各国への支払の源泉税率（租税条約）」を、検索・コピーしやすいHTMLテーブルに変換したものです。表の列は、国・地域名、限度税率（利子、配当、使用料）、備考、PDFページです。</p>
      <p>税率適用の判断では、各行の脚注、末尾注、租税条約本文、議定書、届出手続、国内法税率との比較を必ず確認してください。</p>
      <div class="meta">
        <div><span class="label">一次ソース</span><a href="https://www.nta.go.jp/publication/pamph/gensen/aramashi2024/index.htm">国税庁 令和7年版 源泉徴収のあらまし</a></div>
        <div><span class="label">対象PDF</span><a href="https://www.nta.go.jp/publication/pamph/gensen/aramashi2024/pdf/12.pdf">第10 非居住者又は外国法人に支払う所得の源泉徴収事務</a></div>
        <div><span class="label">対象ページ</span>冊子ページ327-338 / PDF page 56-67</div>
        <div><span class="label">抽出件数</span>{len(rows)}件</div>
      </div>
    </section>

    <section class="source-note">
      <h2>注記</h2>
      <p>PDFから抽出した本文テキストをもとにHTML化しています。原典PDFのレイアウト上、同じセル内の注記は改行で保持しています。最終判断ではリンク先の国税庁PDFと各租税条約本文を確認してください。</p>
    </section>

    <section>
      <h2>税率表</h2>
      <div class="tools">
        <input id="q" type="search" placeholder="国名・税率・備考を検索">
        <span id="count" class="count"></span>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th rowspan="2">国・地域名</th>
              <th colspan="3">限度税率</th>
              <th rowspan="2">備考</th>
              <th rowspan="2">出所</th>
            </tr>
            <tr>
              <th>利子（％）</th>
              <th>配当（％）</th>
              <th>使用料（％）</th>
            </tr>
          </thead>
          <tbody>{table_rows}
          </tbody>
        </table>
      </div>
    </section>
  </main>
  <script>{script}</script>
</body>
</html>
"""
    OUT.write_text(doc, encoding="utf-8")
    print(f"wrote {OUT} rows={len(rows)} bytes={len(doc.encode('utf-8'))}")


if __name__ == "__main__":
    main()
