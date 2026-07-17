(() => {
  "use strict";

  const PUBLIC_ROOT = "/jplawdb4/その他/";
  const INDEX_URL = PUBLIC_ROOT + "index.html";
  const REGISTRY_URL = PUBLIC_ROOT + "index.json";
  const APP_MODE = document.documentElement.dataset.jplawShell === "app";

  const ready = (callback) => {
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", callback, { once: true });
    } else {
      callback();
    }
  };

  const cleanText = (value) => String(value || "").replace(/\s+/g, " ").trim();

  function ensureMainLandmark() {
    let main = document.querySelector("main, [role='main']");
    if (!main) {
      main = document.querySelector(
        "article, .report, .report-container, .document, .content, .container, .page, .layout, .workspace"
      );
      if (main) main.setAttribute("role", "main");
    }
    if (!main) {
      main = document.body;
      main.setAttribute("role", "main");
    }
    if (!main.id) main.id = "jplaw-main";
    main.classList.add("jplaw-shell-main");

    if (!document.querySelector(".jplaw-skip-link")) {
      const skip = document.createElement("a");
      skip.className = "jplaw-skip-link";
      skip.href = "#" + main.id;
      skip.textContent = "本文へ移動";
      document.body.prepend(skip);
    }
    return main;
  }

  function ensureAccessibleNames() {
    const controls = document.querySelectorAll(
      "input:not([type='hidden']), select, textarea"
    );
    controls.forEach((control, index) => {
      if (
        control.getAttribute("aria-label") ||
        control.getAttribute("aria-labelledby") ||
        (control.id && document.querySelector(`label[for="${CSS.escape(control.id)}"]`)) ||
        control.closest("label")
      ) return;

      const field = control.closest(
        "tr, .field, .form-group, .input-group, .control, .row, [data-field]"
      );
      const source =
        field?.querySelector("label, th, .label, .field-label, dt, legend") ||
        control.previousElementSibling;
      let label = cleanText(source?.textContent);
      if (!label) label = cleanText(control.placeholder);
      if (!label) label = "入力項目 " + (index + 1);
      control.setAttribute("aria-label", label.slice(0, 120));
    });
  }

  function parentCanScroll(table) {
    let node = table.parentElement;
    while (node && node !== document.body) {
      const style = getComputedStyle(node);
      if (/(auto|scroll)/.test(style.overflowX)) return node;
      node = node.parentElement;
    }
    return null;
  }

  function upgradeTables() {
    document.querySelectorAll("table").forEach((table, index) => {
      if (
        table.closest(".form-paper, .print-stack, [data-jplaw-no-wrap]") ||
        table.closest(".jplaw-table-scroll")
      ) return;

      const existing = parentCanScroll(table);
      let wrapper = existing;
      if (!wrapper) {
        wrapper = document.createElement("div");
        table.parentNode.insertBefore(wrapper, table);
        wrapper.append(table);
      }
      wrapper.classList.add("jplaw-table-scroll");
      if (!wrapper.hasAttribute("tabindex")) wrapper.tabIndex = 0;
      if (!wrapper.hasAttribute("role")) wrapper.setAttribute("role", "region");
      if (!wrapper.hasAttribute("aria-label")) {
        const caption = cleanText(table.querySelector("caption")?.textContent);
        wrapper.setAttribute("aria-label", caption || "表 " + (index + 1) + "（横方向にスクロールできます）");
      }
      if (!wrapper.previousElementSibling?.classList.contains("jplaw-scroll-hint")) {
        const hint = document.createElement("p");
        hint.className = "jplaw-scroll-hint";
        hint.textContent = "表は横方向にスクロールできます";
        hint.hidden = table.scrollWidth <= wrapper.clientWidth + 2;
        wrapper.before(hint);
        const update = () => {
          hint.hidden = table.scrollWidth <= wrapper.clientWidth + 2;
        };
        window.addEventListener("resize", update, { passive: true });
      }
    });
  }

  function containWidePages() {
    if (APP_MODE) return;
    const viewport = document.documentElement.clientWidth;
    document.querySelectorAll(
      "body > .page, body > .sheet, body > .canvas, main > .page, main > .sheet, .structure-map"
    ).forEach((element, index) => {
      if (element.closest(".jplaw-wide-scroll")) return;
      if (element.scrollWidth <= viewport + 2 && element.getBoundingClientRect().right <= viewport + 2) return;
      const wrapper = document.createElement("div");
      wrapper.className = "jplaw-wide-scroll";
      wrapper.tabIndex = 0;
      wrapper.setAttribute("role", "region");
      wrapper.setAttribute("aria-label", "横長資料 " + (index + 1) + "（横方向にスクロールできます）");
      element.parentNode.insertBefore(wrapper, element);
      wrapper.append(element);
    });
  }

  function headingList() {
    const seen = new Set();
    return [...document.querySelectorAll("h2, h3")].filter((heading) => {
      if (heading.closest(".jplaw-toc-panel, .jplaw-public-tools")) return false;
      const text = cleanText(heading.textContent);
      if (!text || seen.has(text + heading.tagName)) return false;
      seen.add(text + heading.tagName);
      return true;
    });
  }

  function normalizeHeadingLevels() {
    const levelMap = new Map();
    let previousLevel = 1;
    document.querySelectorAll("h1, h2, h3, h4, h5, h6").forEach((heading, index) => {
      if (heading.closest(".jplaw-toc-panel, .jplaw-public-tools")) return;
      const nativeLevel = Number(heading.tagName.slice(1));
      for (const mappedLevel of [...levelMap.keys()]) {
        if (mappedLevel > nativeLevel) levelMap.delete(mappedLevel);
      }
      let accessibleLevel = levelMap.get(nativeLevel) || nativeLevel;
      if (index > 0 && accessibleLevel > previousLevel + 1) {
        accessibleLevel = previousLevel + 1;
        levelMap.set(nativeLevel, accessibleLevel);
      }
      if (accessibleLevel !== nativeLevel) {
        heading.setAttribute("aria-level", String(accessibleLevel));
      }
      previousLevel = accessibleLevel;
    });
  }

  function ensureHeadingIds(headings) {
    const occupied = new Set([...document.querySelectorAll("[id]")].map((node) => node.id));
    headings.forEach((heading, index) => {
      if (heading.id) return;
      let id = "section-" + (index + 1);
      while (occupied.has(id)) id += "-";
      heading.id = id;
      occupied.add(id);
    });
  }

  function buildReadingTools(main) {
    if (APP_MODE || document.querySelector(".jplaw-public-tools")) return;

    const headings = headingList();
    ensureHeadingIds(headings);

    const progress = document.createElement("div");
    progress.className = "jplaw-reading-progress";
    progress.setAttribute("aria-hidden", "true");
    document.body.append(progress);

    const tools = document.createElement("nav");
    tools.className = "jplaw-public-tools";
    tools.setAttribute("aria-label", "公開資料ナビゲーション");

    const indexLink = document.createElement("a");
    indexLink.className = "jplaw-tools-index";
    indexLink.href = INDEX_URL;
    indexLink.innerHTML = "<span>成果物一覧</span>";
    indexLink.setAttribute("aria-label", "成果物一覧へ戻る");
    tools.append(indexLink);

    let tocButton = null;
    let panel = null;
    let backdrop = null;
    let closeButton = null;

    if (headings.length >= 3) {
      tocButton = document.createElement("button");
      tocButton.type = "button";
      tocButton.textContent = "目次";
      tocButton.setAttribute("aria-expanded", "false");
      tocButton.setAttribute("aria-controls", "jplaw-toc-panel");
      tools.append(tocButton);

      backdrop = document.createElement("div");
      backdrop.className = "jplaw-toc-backdrop";
      backdrop.hidden = true;

      panel = document.createElement("aside");
      panel.className = "jplaw-toc-panel";
      panel.id = "jplaw-toc-panel";
      panel.setAttribute("aria-label", "文書目次");
      panel.setAttribute("aria-hidden", "true");

      const header = document.createElement("div");
      header.className = "jplaw-toc-header";
      const title = document.createElement("div");
      title.innerHTML = "<p>Document navigator</p><h2>この資料の目次</h2>";
      closeButton = document.createElement("button");
      closeButton.className = "jplaw-toc-close";
      closeButton.type = "button";
      closeButton.textContent = "×";
      closeButton.setAttribute("aria-label", "目次を閉じる");
      header.append(title, closeButton);

      const list = document.createElement("ol");
      list.className = "jplaw-toc-list";
      headings.forEach((heading) => {
        const item = document.createElement("li");
        item.className = heading.tagName === "H3" ? "level-3" : "level-2";
        const link = document.createElement("a");
        link.href = "#" + heading.id;
        link.textContent = cleanText(heading.textContent);
        item.append(link);
        list.append(item);
      });

      const neighbours = document.createElement("div");
      neighbours.className = "jplaw-neighbours";
      neighbours.id = "jplaw-neighbours";
      panel.append(header, list, neighbours);
      document.body.append(backdrop, panel);

      const close = (restoreFocus = true) => {
        panel.classList.remove("is-open");
        backdrop.classList.remove("is-open");
        panel.setAttribute("aria-hidden", "true");
        tocButton.setAttribute("aria-expanded", "false");
        backdrop.hidden = true;
        document.documentElement.style.overflow = "";
        if (restoreFocus) tocButton.focus();
      };
      const open = () => {
        backdrop.hidden = false;
        panel.classList.add("is-open");
        backdrop.classList.add("is-open");
        panel.setAttribute("aria-hidden", "false");
        tocButton.setAttribute("aria-expanded", "true");
        document.documentElement.style.overflow = "hidden";
        closeButton.focus();
      };
      tocButton.addEventListener("click", open);
      closeButton.addEventListener("click", () => close());
      backdrop.addEventListener("click", () => close());
      panel.addEventListener("click", (event) => {
        if (event.target.closest(".jplaw-toc-list a")) close(false);
      });
      document.addEventListener("keydown", (event) => {
        if (event.key === "Escape" && panel.classList.contains("is-open")) close();
      });

      if ("IntersectionObserver" in window) {
        const links = new Map(
          [...list.querySelectorAll("a")].map((link) => [link.hash.slice(1), link])
        );
        const observer = new IntersectionObserver((entries) => {
          entries.forEach((entry) => {
            if (!entry.isIntersecting) return;
            links.forEach((link) => link.removeAttribute("aria-current"));
            links.get(entry.target.id)?.setAttribute("aria-current", "location");
          });
        }, { rootMargin: "-16% 0px -72% 0px" });
        headings.forEach((heading) => observer.observe(heading));
      }

      enrichNeighbours(neighbours);
    }

    const topButton = document.createElement("button");
    topButton.type = "button";
    topButton.textContent = "↑";
    topButton.setAttribute("aria-label", "ページ先頭へ戻る");
    topButton.addEventListener("click", () => window.scrollTo({ top: 0, behavior: "smooth" }));
    tools.append(topButton);
    document.body.append(tools);

    const updateProgress = () => {
      const root = document.documentElement;
      const distance = Math.max(1, root.scrollHeight - root.clientHeight);
      progress.style.width = Math.min(100, Math.max(0, (root.scrollTop / distance) * 100)) + "%";
      topButton.hidden = root.scrollTop < 420;
    };
    updateProgress();
    window.addEventListener("scroll", updateProgress, { passive: true });
    window.addEventListener("resize", updateProgress, { passive: true });
  }

  async function enrichNeighbours(container) {
    try {
      const response = await fetch(REGISTRY_URL, { cache: "no-store" });
      if (!response.ok) return;
      const registry = await response.json();
      const currentPath = decodeURIComponent(location.pathname).replace(/^.*\/その他\//, "");
      const items = registry.items.filter((item) => item.published !== false && item.status !== "legacy");
      const currentIndex = items.findIndex((item) => decodeURIComponent(item.file) === currentPath);
      if (currentIndex < 0) return;
      const links = [
        ["前の資料", items[currentIndex - 1]],
        ["次の資料", items[currentIndex + 1]]
      ].filter(([, item]) => item);
      links.forEach(([label, item]) => {
        const link = document.createElement("a");
        link.href = PUBLIC_ROOT + item.file.split("/").map(encodeURIComponent).join("/").replace(/%2F/g, "/");
        link.innerHTML = `<small>${label}</small>${escapeHtml(item.title)}`;
        container.append(link);
      });
    } catch {
      // The document remains fully usable when opened directly from disk.
    }
  }

  function escapeHtml(value) {
    return String(value).replace(/[&<>"']/g, (character) => ({
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      '"': "&quot;",
      "'": "&#39;"
    })[character]);
  }

  function watchDynamicContent() {
    let scheduled = false;
    const observer = new MutationObserver((mutations) => {
      if (!mutations.some((mutation) => mutation.addedNodes.length)) return;
      if (scheduled) return;
      scheduled = true;
      requestAnimationFrame(() => {
        scheduled = false;
        ensureAccessibleNames();
        upgradeTables();
      });
    });
    observer.observe(document.body, { childList: true, subtree: true });
  }

  ready(() => {
    const main = ensureMainLandmark();
    ensureAccessibleNames();
    upgradeTables();
    containWidePages();
    normalizeHeadingLevels();
    buildReadingTools(main);
    watchDynamicContent();
  });
})();
