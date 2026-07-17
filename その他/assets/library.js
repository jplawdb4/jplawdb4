(() => {
  "use strict";

  const state = {
    items: [],
    category: "すべて",
    query: "",
    sort: "newest",
    includeLegacy: false
  };

  const dom = {
    form: document.querySelector("#finder"),
    search: document.querySelector("#librarySearch"),
    sort: document.querySelector("#librarySort"),
    categories: document.querySelector("#categoryFilters"),
    legacy: document.querySelector("#includeLegacy"),
    count: document.querySelector("#resultCount"),
    countNote: document.querySelector("#resultNote"),
    featured: document.querySelector("#featured"),
    cards: document.querySelector("#cards"),
    updated: document.querySelector("#updatedAt"),
    status: document.querySelector("#loadStatus")
  };

  function escapeHtml(value) {
    return String(value ?? "").replace(/[&<>"']/g, (character) => ({
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      '"': "&quot;",
      "'": "&#39;"
    })[character]);
  }

  function itemUrl(item) {
    return item.file.split("/").map(encodeURIComponent).join("/");
  }

  function normalize(value) {
    return String(value ?? "")
      .normalize("NFKC")
      .toLocaleLowerCase("ja")
      .replace(/\s+/g, "");
  }

  function searchable(item) {
    return normalize([
      item.title,
      item.short_summary,
      item.summary,
      item.category,
      item.format,
      item.family,
      item.version_label,
      ...(item.tags || []),
      ...(item.laws || [])
    ].join(" "));
  }

  function categories() {
    const values = [...new Set(state.items.map((item) => item.category).filter(Boolean))];
    const priority = ["計算ツール", "法人税", "消費税", "国際課税", "法令マップ", "DB・運用"];
    return values.sort((a, b) => {
      const ai = priority.indexOf(a);
      const bi = priority.indexOf(b);
      if (ai < 0 && bi < 0) return a.localeCompare(b, "ja");
      if (ai < 0) return 1;
      if (bi < 0) return -1;
      return ai - bi;
    });
  }

  function renderFilters() {
    const values = ["すべて", ...categories()];
    dom.categories.replaceChildren(
      ...values.map((category) => {
        const button = document.createElement("button");
        button.type = "button";
        button.className = "filter-chip";
        button.dataset.category = category;
        button.setAttribute("aria-pressed", String(state.category === category));
        button.textContent = category;
        return button;
      })
    );
  }

  function filteredItems() {
    const query = normalize(state.query);
    return state.items
      .filter((item) => item.published !== false)
      .filter((item) => state.includeLegacy || item.status !== "legacy")
      .filter((item) => state.category === "すべて" || item.category === state.category)
      .filter((item) => !query || searchable(item).includes(query))
      .sort((a, b) => {
        if (state.sort === "title") return a.title.localeCompare(b.title, "ja");
        if (state.sort === "oldest") return a.date.localeCompare(b.date);
        return b.date.localeCompare(a.date) || a.title.localeCompare(b.title, "ja");
      });
  }

  function badges(item) {
    const output = [];
    if (item.status === "legacy") output.push('<span class="badge legacy">旧版</span>');
    else if (item.status === "current") output.push('<span class="badge current">現行</span>');
    if (item.version_label) output.push(`<span class="badge">${escapeHtml(item.version_label)}</span>`);
    return output.join("");
  }

  function card(item) {
    const summary = item.short_summary || item.summary || "";
    const date = item.date.replaceAll("-", ".");
    return `
      <article class="card" data-family="${escapeHtml(item.family || "")}">
        <div class="card-head">
          <span class="card-kind">${escapeHtml(item.category)} · ${escapeHtml(item.format)}</span>
          <div class="badges">${badges(item)}</div>
        </div>
        <h3><a href="${itemUrl(item)}">${escapeHtml(item.title)}</a></h3>
        <p class="card-summary">${escapeHtml(summary)}</p>
        <footer class="card-footer">
          <div class="card-meta">
            <span>${escapeHtml(date)} 公開</span>
            <strong title="${escapeHtml(item.legal_as_of || "")}">${escapeHtml(item.legal_as_of || item.family_label || "公開時点")}</strong>
          </div>
          <span class="card-arrow" aria-hidden="true">→</span>
        </footer>
      </article>`;
  }

  function featured(item) {
    if (!item) return "";
    return `
      <article class="featured">
        <div class="featured-visual" aria-hidden="true"></div>
        <div class="featured-body">
          <p class="featured-label">Recommended · ${escapeHtml(item.category)} · ${escapeHtml(item.version_label || "")}</p>
          <h3>${escapeHtml(item.title)}</h3>
          <p class="featured-summary">${escapeHtml(item.short_summary || item.summary)}</p>
          <a class="featured-action" href="${itemUrl(item)}">Webワークベンチを開く</a>
        </div>
      </article>`;
  }

  function render() {
    const items = filteredItems();
    const defaultView =
      !state.query &&
      state.category === "すべて" &&
      state.sort === "newest" &&
      !state.includeLegacy;
    const recommended = defaultView ? items.find((item) => item.recommended) : null;
    const cardItems = recommended ? items.filter((item) => item.id !== recommended.id) : items;

    dom.count.textContent = items.length + "件";
    dom.countNote.textContent = state.includeLegacy
      ? "旧版を含む公開成果物"
      : "現行・参照用の公開成果物";
    dom.featured.innerHTML = featured(recommended);
    dom.featured.hidden = !recommended;

    if (!cardItems.length) {
      dom.cards.innerHTML = '<p class="empty">条件に一致する成果物はありません。検索語または分類を変更してください。</p>';
    } else {
      dom.cards.innerHTML = cardItems.map(card).join("");
    }

    document.querySelectorAll(".filter-chip").forEach((button) => {
      button.setAttribute("aria-pressed", String(button.dataset.category === state.category));
    });
  }

  async function load() {
    try {
      const response = await fetch("./index.json", { cache: "no-store" });
      if (!response.ok) throw new Error("HTTP " + response.status);
      const data = await response.json();
      state.items = Array.isArray(data.items) ? data.items : [];
      dom.updated.textContent = String(data.updated || "").replaceAll("-", ".");
      renderFilters();
      render();
      dom.status.textContent = "";
    } catch (error) {
      dom.status.textContent = "成果物一覧を読み込めませんでした。再読み込みしてください。";
      dom.cards.innerHTML = '<p class="empty">index.json の読み込みに失敗しました。</p>';
      console.error(error);
    }
  }

  dom.search.addEventListener("input", () => {
    state.query = dom.search.value;
    render();
  });

  dom.sort.addEventListener("change", () => {
    state.sort = dom.sort.value;
    render();
  });

  dom.categories.addEventListener("click", (event) => {
    const button = event.target.closest("[data-category]");
    if (!button) return;
    state.category = button.dataset.category;
    render();
  });

  dom.legacy.addEventListener("change", () => {
    state.includeLegacy = dom.legacy.checked;
    render();
  });

  dom.form.addEventListener("submit", (event) => event.preventDefault());
  load();
})();
