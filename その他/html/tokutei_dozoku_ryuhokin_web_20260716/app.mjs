import {
  calculateEdition,
  defaultInputState,
  directInputForFormula,
  displayDateRange,
  flattenInputs,
  formulaDependencies,
} from "./formula-engine.mjs";

const STORAGE_KEY = "jplawdb4.retained-earnings-tax-web.v2";
const MOBILE_BREAKPOINT = 820;
const COMPACT_BREAKPOINT = 1160;
const SURFACE_META = {
  main: { label: "別表三(一)", prefix: "m", rowKey: "main_rows" },
  f1: { label: "付表一", prefix: "f1", rowKey: "f1_rows" },
  f2: { label: "付表二", prefix: "f2", rowKey: "f2_rows" },
};

const ids = [
  "bootScreen", "yearSwitcher", "editionStamp", "applicableLabel", "monthBadge", "formulaBadge", "excelLink",
  "saveButton", "csvButton", "loadButton", "printButton", "printAllButton", "resetButton", "loadFileInput", "saveState",
  "resultGrid", "inputSearch", "expandInputsButton", "inputGroups", "surfaceTabs", "paperStage", "formPaper",
  "formCaption", "ledgerTitle", "formulaToggle", "ledgerBody", "sourceList", "auditLight", "auditVerdict",
  "checkCounter", "checkList", "traceName", "traceCard", "releaseFacts", "zoomOutput", "zoomInButton",
  "zoomOutButton", "fitButton", "confirmDialog", "confirmTitle", "confirmMessage", "toast", "printStack",
  "inputDossier", "documentDesk", "auditRail",
];
const dom = Object.fromEntries(ids.map((id) => [id, document.getElementById(id)]));

let model;
let manifest;
let currentEdition;
let currentSurface = "main";
let currentView = "form";
let calculation;
let stateByEdition = {};
const engagedEditions = new Set();
let zoom = 1;
let saveTimer;
let toastTimer;

const yen = new Intl.NumberFormat("ja-JP", { maximumFractionDigits: 0 });
const ratio = new Intl.NumberFormat("ja-JP", { minimumFractionDigits: 8, maximumFractionDigits: 8 });

function create(tag, className = "", text = "") {
  const element = document.createElement(tag);
  if (className) element.className = className;
  if (text !== "") element.textContent = text;
  return element;
}

function editionById(id) {
  return model.editions.find((edition) => edition.id === id);
}

function getState() {
  return stateByEdition[currentEdition.id];
}

function readStorage() {
  try {
    const parsed = JSON.parse(localStorage.getItem(STORAGE_KEY) || "{}");
    if (parsed && parsed.schema === model.schema && typeof parsed.states === "object") {
      stateByEdition = parsed.states;
      Object.entries(stateByEdition).forEach(([editionId, storedState]) => {
        const edition = editionById(editionId);
        if (!edition || !storedState || typeof storedState !== "object") return;
        const defaults = defaultInputState(edition);
        const keys = new Set([...Object.keys(defaults), ...Object.keys(storedState)]);
        const differsFromDefault = [...keys].some(
          (key) => String(storedState[key] ?? "") !== String(defaults[key] ?? "")
        );
        if (differsFromDefault) engagedEditions.add(editionId);
      });
    }
  } catch {
    stateByEdition = {};
  }
}

function writeStorage() {
  localStorage.setItem(STORAGE_KEY, JSON.stringify({ schema: model.schema, saved_at: new Date().toISOString(), states: stateByEdition }));
  dom.saveState.textContent = "端末内に保存済み";
  dom.saveState.classList.remove("is-saving");
}

function scheduleStorage() {
  dom.saveState.textContent = "保存中…";
  dom.saveState.classList.add("is-saving");
  clearTimeout(saveTimer);
  saveTimer = setTimeout(writeStorage, 280);
}

function showToast(message, error = false) {
  clearTimeout(toastTimer);
  dom.toast.textContent = message;
  dom.toast.classList.toggle("is-error", error);
  dom.toast.classList.add("is-visible");
  toastTimer = setTimeout(() => dom.toast.classList.remove("is-visible"), 3200);
}

function groupIntegerText(value) {
  const text = String(value ?? "").trim();
  if (!/^-?\d+$/.test(text)) return null;
  try {
    return yen.format(BigInt(text));
  } catch {
    const sign = text.startsWith("-") ? "-" : "";
    const digits = sign ? text.slice(1) : text;
    return sign + digits.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
  }
}

function formatAmount(value) {
  if (value === "" || value === null || value === undefined) return "—";
  if (typeof value === "string") return groupIntegerText(value) || value;
  if (typeof value === "bigint") return yen.format(value);
  return Number.isFinite(value) ? yen.format(Math.trunc(value)) : "—";
}

function formatRowValue(row, value) {
  if (value === "" || value === null || value === undefined) return "";
  if (String(row.format || "").includes("0.00000000")) {
    const number = Number(value);
    return Number.isFinite(number) ? ratio.format(number) : "#ERROR";
  }
  return formatAmount(value);
}

function rawInputValue(item, value) {
  if (item.kind === "amount") return groupIntegerText(value) || String(value ?? "");
  return value ?? "";
}

function parseAmount(value) {
  const normalized = String(value || "")
    .replace(/[,，\s]/g, "")
    .replace(/[−‐‑‒–—―]/g, "-")
    .replace(/[０-９]/g, (digit) => String.fromCharCode(digit.charCodeAt(0) - 0xfee0));
  return normalized === "" || normalized === "-" ? normalized : normalized;
}

function formulaRows(edition, surface = currentSurface) {
  return edition[SURFACE_META[surface].rowKey] || [];
}

function rowName(surface, row) {
  return row.name || SURFACE_META[surface].prefix + "_" + String(row.no).padStart(2, "0");
}

function allFormulaRows(edition) {
  return Object.keys(edition.geometry).flatMap((surface) =>
    formulaRows(edition, surface).map((row) => ({ surface, row, name: rowName(surface, row) }))
  );
}

function renderYearSwitcher() {
  dom.yearSwitcher.replaceChildren();
  for (const edition of model.editions) {
    const button = create("button", "year-button", "R" + edition.year);
    button.type = "button";
    button.dataset.edition = edition.id;
    button.title = edition.display;
    button.setAttribute("aria-label", edition.display + "へ切替");
    button.setAttribute("aria-current", edition.id === currentEdition.id ? "true" : "false");
    button.classList.toggle("is-active", edition.id === currentEdition.id);
    button.addEventListener("click", () => switchEdition(edition.id));
    dom.yearSwitcher.append(button);
  }
}

function rovingTabs(container, selector, activeButton) {
  for (const button of container.querySelectorAll(selector)) {
    const active = button === activeButton;
    button.tabIndex = active ? 0 : -1;
    button.setAttribute("aria-selected", String(active));
    button.classList.toggle("is-active", active);
  }
}

function wireRovingKeydown(event, buttons, activate) {
  if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
  event.preventDefault();
  const list = [...buttons];
  let index = list.indexOf(event.currentTarget);
  if (event.key === "Home") index = 0;
  else if (event.key === "End") index = list.length - 1;
  else index = (index + (event.key === "ArrowRight" ? 1 : -1) + list.length) % list.length;
  activate(list[index]);
  list[index].focus();
}

function renderSurfaceTabs() {
  dom.surfaceTabs.replaceChildren();
  const surfaces = Object.keys(currentEdition.geometry);
  for (const surface of surfaces) {
    const button = create("button", "surface-tab", SURFACE_META[surface].label);
    button.type = "button";
    button.role = "tab";
    button.id = "surfaceTab-" + surface;
    button.dataset.surface = surface;
    button.setAttribute("aria-controls", "formPaper");
    button.setAttribute("aria-selected", String(surface === currentSurface));
    button.tabIndex = surface === currentSurface ? 0 : -1;
    button.classList.toggle("is-active", surface === currentSurface);
    button.addEventListener("click", () => switchSurface(surface));
    button.addEventListener("keydown", (event) =>
      wireRovingKeydown(event, dom.surfaceTabs.querySelectorAll(".surface-tab"), (target) => switchSurface(target.dataset.surface))
    );
    dom.surfaceTabs.append(button);
  }
}

function renderEditionHeader() {
  dom.editionStamp.textContent = currentEdition.display;
  dom.applicableLabel.textContent = currentEdition.applicable_label;
  dom.monthBadge.textContent = (calculation.state.inp_months || "—") + "か月";
  dom.formulaBadge.textContent = allFormulaRows(currentEdition).length + " 数式";
  dom.excelLink.href = currentEdition.reference_workbook.href;
  dom.excelLink.title = "SHA-256 " + currentEdition.reference_workbook.sha256;
  document.title = currentEdition.display + "｜特定同族会社の留保金課税";
}

function renderResults() {
  const items = [
    ["留保金額に対する税額", calculation.summary.retained_tax, true],
    ["課税留保金額", calculation.summary.taxable_retained, false],
    ["留保控除額", calculation.summary.retention_allowance, false],
    ["住民税額", calculation.summary.municipal_tax, false],
  ];
  dom.resultGrid.replaceChildren();
  for (const [label, value, primary] of items) {
    const item = create("div", "result-item" + (primary ? " primary" : ""));
    item.append(create("span", "", label), create("strong", "", formatAmount(value)));
    dom.resultGrid.append(item);
  }
}

function inputId(name) {
  return "field-" + currentEdition.id + "-" + name;
}

function makeInputControl(item) {
  let control;
  const value = getState()[item.name];
  if (item.kind === "select") {
    control = create("select", "input-control");
    for (const optionValue of item.options || []) {
      const option = create("option", "", optionValue);
      option.value = optionValue;
      control.append(option);
    }
    control.value = value ?? item.default;
  } else {
    control = create("input", "input-control");
    control.type = item.kind === "date" ? "date" : "text";
    if (item.kind === "date") {
      control.min = "1900-01-01";
      control.max = "2100-12-31";
    }
    if (item.kind === "amount") {
      control.inputMode = item.allow_negative ? "decimal" : "numeric";
      control.autocomplete = "off";
    }
    control.value = rawInputValue(item, value);
    if (item.kind === "computed") control.readOnly = true;
  }
  control.id = inputId(item.name);
  control.dataset.field = item.name;
  control.dataset.kind = item.kind;
  control.setAttribute("aria-label", item.label);
  control.addEventListener("focus", () => {
    if (item.kind === "amount") control.value = String(getState()[item.name] ?? 0);
  });
  control.addEventListener("blur", () => {
    if (item.kind === "amount") control.value = rawInputValue(item, getState()[item.name]);
  });
  if (item.kind !== "computed") {
    control.addEventListener("input", (event) => updateField(item.name, item.kind, event.target.value, event.target));
    control.addEventListener("change", (event) => updateField(item.name, item.kind, event.target.value, event.target));
  }
  return control;
}

function renderInputs() {
  dom.inputGroups.replaceChildren();
  currentEdition.input_groups.forEach((group, index) => {
    const details = create("details", "input-group");
    details.open = index <= 1;
    const summary = create("summary");
    summary.append(create("span", "", group.title), create("span", "input-count", group.items.length + "項目"));
    details.append(summary);
    for (const item of group.items) {
      const row = create("div", "input-row");
      row.dataset.fieldRow = item.name;
      row.dataset.search = (item.label + " " + (item.desc || "") + " " + (item.source || "") + " " + (item.note || "")).toLowerCase();
      const copy = create("div", "input-copy");
      const label = create("label", "", item.label);
      label.htmlFor = inputId(item.name);
      copy.append(label);
      if (item.source) copy.append(create("span", "input-source", item.source));
      row.append(copy, makeInputControl(item));
      if (item.note) row.append(create("p", "input-note", item.note));
      details.append(row);
    }
    dom.inputGroups.append(details);
  });
}

function refreshInputValidity() {
  const failedFields = new Set(
    calculation.checks.filter((check) => !check.ok && check.level === "error" && check.field).map((check) => check.field)
  );
  for (const row of dom.inputGroups.querySelectorAll("[data-field-row]")) {
    row.classList.toggle("has-error", failedFields.has(row.dataset.fieldRow));
  }
  for (const control of dom.inputGroups.querySelectorAll("[data-field]")) {
    control.setAttribute("aria-invalid", String(failedFields.has(control.dataset.field)));
  }
  const monthControl = dom.inputGroups.querySelector('[data-field="inp_months"]');
  if (monthControl && document.activeElement !== monthControl) monthControl.value = calculation.state.inp_months || "";
}

function updateField(name, kind, rawValue, sourceElement = null) {
  engagedEditions.add(currentEdition.id);
  getState()[name] = kind === "amount" ? parseAmount(rawValue) : rawValue;
  const direct = sourceElement && sourceElement.classList.contains("form-direct-input");
  recalculate({ skipForm: direct });
  if (direct) {
    sourceElement.setAttribute("aria-invalid", String(hasFieldError(name)));
    const definition = flattenInputs(currentEdition).find((item) => item.name === name);
    const paired = dom.inputGroups.querySelector('[data-field="' + CSS.escape(name) + '"]');
    if (definition && paired && paired !== document.activeElement) paired.value = rawInputValue(definition, calculation.state[name]);
  }
  scheduleStorage();
}

function recalculate(options = {}) {
  calculation = calculateEdition(currentEdition, getState());
  stateByEdition[currentEdition.id] = calculation.state;
  renderEditionHeader();
  renderResults();
  if (!options.skipForm) renderForm();
  if (!options.skipLedger) renderLedger();
  renderAudit();
  refreshInputValidity();
  window.dispatchEvent(new CustomEvent("retained-tax-calculated", { detail: { edition: currentEdition.id, result: calculation } }));
}

function hasFieldError(name) {
  return calculation.checks.some((check) => !check.ok && check.level === "error" && check.field === name);
}

function boxStyle(element, box, geometry) {
  element.style.left = Number(box.left) / Number(geometry.page_width_pt) * 100 + "%";
  element.style.top = Number(box.top) / Number(geometry.page_height_pt) * 100 + "%";
  element.style.width = Number(box.width) / Number(geometry.page_width_pt) * 100 + "%";
  element.style.height = Number(box.height) / Number(geometry.page_height_pt) * 100 + "%";
}

function periodText() {
  const range = displayDateRange(calculation.state);
  if (!range) return "";
  const parts = range.split(" ～ ");
  return parts[0].replaceAll("-", "/") + "\n" + parts[1].replaceAll("-", "/");
}

function renderFormPaper(target, surface, printMode = false) {
  const geometry = currentEdition.geometry[surface];
  const rows = formulaRows(currentEdition, surface);
  const mobileReadOnly = !printMode && window.innerWidth <= MOBILE_BREAKPOINT;
  target.replaceChildren();
  target.className = printMode ? "form-paper print-paper" : "form-paper";
  target.dataset.surface = surface;
  target.style.setProperty("--paper-width", geometry.page_width_pt + "px");
  target.style.setProperty("--paper-height", geometry.page_height_pt + "px");
  target.style.setProperty("--form-scale", printMode ? "1" : String(zoom));

  const image = create("img", "form-background");
  image.src = currentEdition.assets[surface];
  image.alt = currentEdition.display + " " + SURFACE_META[surface].label + " 国税庁公式様式";
  image.draggable = false;
  target.append(image);

  const period = create("span", "form-header-value period", calculation.ready ? periodText() : "");
  boxStyle(period, geometry.period, geometry);
  target.append(period);
  const corp = create("span", "form-header-value corp", calculation.ready ? (calculation.state.inp_corp || "") : "");
  boxStyle(corp, geometry.corp, geometry);
  target.append(corp);

  for (const row of rows) {
    const box = geometry.rows[String(Number(row.no))];
    if (!box) continue;
    const name = rowName(surface, row);
    const directInput = directInputForFormula(row.formula);
    let overlay;
    if (!printMode && directInput) {
      overlay = create("input", "form-direct-input");
      overlay.type = "text";
      overlay.inputMode = "decimal";
      overlay.value = formatRowValue(row, calculation.state[directInput] ?? 0);
      overlay.dataset.field = directInput;
      overlay.setAttribute("aria-label", SURFACE_META[surface].label + " " + row.no + "欄 " + row.item);
      overlay.setAttribute("aria-invalid", String(hasFieldError(directInput)));
      if (currentEdition.has_f2 && surface === "f2" && calculation.state.inp_group !== "はい") overlay.readOnly = true;
      if (mobileReadOnly) {
        overlay.disabled = true;
        overlay.tabIndex = -1;
        overlay.title = "モバイル表示では下部ナビの「入力」から編集してください";
      }
      overlay.addEventListener("focus", () => { overlay.value = String(calculation.state[directInput] ?? 0); });
      overlay.addEventListener("input", () => updateField(directInput, "amount", overlay.value, overlay));
      overlay.addEventListener("blur", () => {
        overlay.value = formatRowValue(row, calculation.state[directInput] ?? 0);
        refreshFormPaperValues();
      });
    } else {
      const value = calculation.ready ? calculation.values[name] : "";
      overlay = printMode ? create("span", "form-value", formatRowValue(row, value)) : create("button", "form-value", formatRowValue(row, value));
      if (!printMode) {
        overlay.type = "button";
        overlay.dataset.formulaName = name;
        overlay.title = row.no + "欄 " + row.item + "\n" + row.formula;
        if (mobileReadOnly) {
          overlay.disabled = true;
          overlay.tabIndex = -1;
        } else {
          overlay.addEventListener("click", () => showTrace(name));
        }
      }
    }
    boxStyle(overlay, box, geometry);
    target.append(overlay);
  }

  if (surface === "main" && geometry.rows["9_inner"]) {
    const innerName = currentEdition.year === 8 ? "inp_m_B4_17_inner" : "inp_m9_inner";
    const inner = create("span", "form-value", calculation.ready ? formatAmount(calculation.state[innerName] || 0) : "");
    inner.dataset.innerName = innerName;
    boxStyle(inner, geometry.rows["9_inner"], geometry);
    target.append(inner);
  }
}

function refreshFormPaperValues() {
  const rows = formulaRows(currentEdition, currentSurface);
  const formulaRowsByName = new Map(rows.map((row) => [rowName(currentSurface, row), row]));
  const directRowsByField = new Map();
  for (const row of rows) {
    const field = directInputForFormula(row.formula);
    if (field) directRowsByField.set(field, row);
  }
  for (const overlay of dom.formPaper.querySelectorAll("[data-formula-name]")) {
    const row = formulaRowsByName.get(overlay.dataset.formulaName);
    if (row) {
      const value = calculation.ready ? calculation.values[overlay.dataset.formulaName] : "";
      overlay.textContent = formatRowValue(row, value);
    }
  }
  for (const overlay of dom.formPaper.querySelectorAll(".form-direct-input[data-field]")) {
    const field = overlay.dataset.field;
    const row = directRowsByField.get(field);
    if (row && overlay !== document.activeElement) {
      overlay.value = formatRowValue(row, calculation.state[field] ?? 0);
    }
    overlay.setAttribute("aria-invalid", String(hasFieldError(field)));
  }
  for (const inner of dom.formPaper.querySelectorAll("[data-inner-name]")) {
    inner.textContent = calculation.ready ? formatAmount(calculation.state[inner.dataset.innerName] || 0) : "";
  }
}

function renderForm() {
  renderFormPaper(dom.formPaper, currentSurface, false);
  dom.formPaper.setAttribute("aria-labelledby", "surfaceTab-" + currentSurface);
  const geometry = currentEdition.geometry[currentSurface];
  dom.formCaption.textContent = currentEdition.display + " " + SURFACE_META[currentSurface].label +
    "／公式原票 " + Number(geometry.page_width_pt).toFixed(2) + " × " + Number(geometry.page_height_pt).toFixed(2) +
    " pt。原票表示の直接入力欄は入力台帳と同期します。";
  dom.zoomOutput.value = Math.round(zoom * 100) + "%";
  dom.zoomOutput.textContent = Math.round(zoom * 100) + "%";
}

function renderLedger() {
  const meta = SURFACE_META[currentSurface];
  dom.ledgerTitle.textContent = currentEdition.display + " " + meta.label + " — 計算台帳";
  dom.ledgerBody.replaceChildren();
  for (const row of formulaRows(currentEdition)) {
    const name = rowName(currentSurface, row);
    const tr = create("tr");
    tr.dataset.formulaName = name;
    tr.append(create("td", "", String(row.no)));
    const item = create("td", "", row.item);
    if (row.note) item.append(create("small", "input-source", row.note));
    const rule = create("td");
    rule.append(create("div", "rule-copy", row.rule || ""), create("code", "formula-copy", row.formula));
    const valueCell = create("td");
    const button = create("button", "ledger-value-button", formatRowValue(row, calculation.values[name]));
    button.type = "button";
    button.addEventListener("click", () => showTrace(name));
    valueCell.append(button);
    const checkCell = create("td");
    checkCell.append(create("span", "row-check", calculation.exactValues[name] !== undefined ? "✓" : "!"));
    tr.append(item, rule, valueCell, checkCell);
    dom.ledgerBody.append(tr);
  }
  const table = dom.ledgerBody.closest("table");
  if (table) table.classList.toggle("show-formulas", dom.formulaToggle.checked);
}

function renderSources() {
  dom.sourceList.replaceChildren();
  for (const source of currentEdition.sources) {
    const item = create("li", "source-item");
    const body = create("div");
    const link = create("a", "", source.title);
    link.href = source.url;
    link.target = "_blank";
    link.rel = "noopener noreferrer";
    body.append(link, create("p", "", source.organization + "／" + (source.use || "年度別原票・記載要領")));
    if (source.sha256) body.append(create("span", "source-hash", "SHA-256 " + source.sha256));
    item.append(body);
    dom.sourceList.append(item);
  }
}

function renderAudit() {
  const failures = calculation.checks.filter((check) => !check.ok);
  const errors = failures.filter((check) => check.level === "error");
  const pending = !engagedEditions.has(currentEdition.id) && errors.length > 0;
  if (pending) {
    dom.auditLight.className = "audit-light";
    dom.auditVerdict.className = "audit-verdict is-pending";
    dom.auditVerdict.replaceChildren();
    const strong = create("strong", "", "入力待ち");
    strong.append(create("span", "", "READY TO CHECK"));
    dom.auditVerdict.append(
      strong,
      create("p", "", "事業年度、会社区分、判定結果などを入力すると、適用条件と計算欄を自動検算します。")
    );
    dom.checkCounter.textContent = "入力後に検算";
    dom.checkList.replaceChildren(create("div", "check-item", "入力台帳から必須項目の入力を始めてください。"));
    return;
  }
  const verdict = errors.length ? "FAIL" : "PASS";
  dom.auditLight.className = "audit-light " + (errors.length ? "is-fail" : "is-pass");
  dom.auditVerdict.className = "audit-verdict " + (errors.length ? "is-fail" : "is-pass");
  dom.auditVerdict.replaceChildren();
  const strong = create("strong", "", verdict);
  strong.append(create("span", "", (calculation.checks.length - failures.length) + "/" + calculation.checks.length + " CHECKS"));
  dom.auditVerdict.append(
    strong,
    create("p", "", errors.length ? errors.length + "件の入力・適用条件を修正してください。結果表示・保存・印刷は停止中です。" : "入力要件、年度適用、数式解決および欄間整合性に問題はありません。")
  );
  dom.checkCounter.textContent = failures.length + "件 要確認";
  dom.checkList.replaceChildren();

  const keyIds = ["date_start", "date_end", "date_order", "date_span", "edition", "special", "class", "group", "f2_period", "f2_total_20", "f2_total_22", "f2_ratio", "formula_resolution"];
  const passes = calculation.checks.filter((check) => check.ok && keyIds.includes(check.id));
  const visible = failures.length ? [...failures, ...passes.slice(-2)] : passes;
  const amountChecks = calculation.checks.filter((check) => /^(number|integer|nonnegative|maximum):/.test(check.id));
  if (amountChecks.length) {
    visible.push({
      id: "amount_aggregate",
      ok: amountChecks.every((check) => check.ok),
      level: "error",
      message: "金額入力要件 " + amountChecks.filter((check) => check.ok).length + "/" + amountChecks.length + "件",
      field: "",
    });
  }
  for (const check of visible) {
    const item = create("div", "check-item" + (check.ok ? "" : " is-fail"), check.message);
    if (!check.ok && check.field) {
      item.tabIndex = 0;
      item.role = "button";
      item.addEventListener("click", () => focusField(check.field));
      item.addEventListener("keydown", (event) => {
        if (["Enter", " "].includes(event.key)) {
          event.preventDefault();
          focusField(check.field);
        }
      });
    }
    dom.checkList.append(item);
  }
  renderReleaseFacts();
}

function renderReleaseFacts() {
  const facts = [
    ["MODEL", (manifest.model && manifest.model.sha256 || "—").slice(0, 16)],
    ["YEAR", currentEdition.id],
    ["INPUTS", String(flattenInputs(currentEdition).length)],
    ["FORMULAS", String(allFormulaRows(currentEdition).length)],
    ["SURFACES", String(Object.keys(currentEdition.geometry).length)],
    ["EXCEL", (currentEdition.reference_workbook.sha256 || "—").slice(0, 16)],
    ["BUILD", manifest.status || "BUILD"],
  ];
  dom.releaseFacts.replaceChildren();
  for (const [key, value] of facts) dom.releaseFacts.append(create("dt", "", key), create("dd", "", value));
}

function showTrace(name) {
  const found = allFormulaRows(currentEdition).find((entry) => entry.name === name);
  if (!found) return;
  dom.traceName.textContent = SURFACE_META[found.surface].label + " " + found.row.no + "欄";
  dom.traceCard.replaceChildren();
  const result = create("div", "trace-result");
  result.append(create("span", "", found.row.item), create("strong", "", formatRowValue(found.row, calculation.values[name])));
  dom.traceCard.append(result, create("code", "trace-formula", found.row.formula));
  const list = create("ul", "trace-deps");
  for (const dependency of formulaDependencies(found.row.formula)) {
    const item = create("li");
    const value = dependency === "$G$10" ? "端数処理ヘルパー" : calculation.env[dependency];
    item.append(create("code", "", dependency), create("span", "", typeof value === "number" || /^-?\d+$/.test(String(value ?? "")) ? formatAmount(value) : String(value ?? "未解決")));
    list.append(item);
  }
  if (!list.children.length) list.append(create("li", "", "直接値・定数式"));
  dom.traceCard.append(list);
}

function focusField(name) {
  setMobilePanel("inputs", { moveFocus: false });
  const control = dom.inputGroups.querySelector('[data-field="' + CSS.escape(name) + '"]');
  if (!control) return;
  const details = control.closest("details");
  if (details) details.open = true;
  const row = control.closest(".input-row");
  if (row) {
    row.classList.add("is-target");
    setTimeout(() => row.classList.remove("is-target"), 1800);
  }
  requestAnimationFrame(() => {
    control.scrollIntoView({ behavior: "smooth", block: "center" });
    control.focus({ preventScroll: true });
  });
}

function switchEdition(id, options = {}) {
  const edition = editionById(id);
  if (!edition) return;
  currentEdition = edition;
  if (!stateByEdition[id]) stateByEdition[id] = defaultInputState(edition);
  if (!(currentSurface in edition.geometry)) currentSurface = "main";
  renderYearSwitcher();
  renderSurfaceTabs();
  renderInputs();
  renderSources();
  recalculate();
  if (!options.silent) {
    scheduleStorage();
    showToast(edition.display + "へ切り替えました。");
  }
  requestAnimationFrame(fitForm);
}

function switchSurface(surface) {
  if (!(surface in currentEdition.geometry)) return;
  currentSurface = surface;
  const active = dom.surfaceTabs.querySelector('[data-surface="' + surface + '"]');
  rovingTabs(dom.surfaceTabs, ".surface-tab", active);
  renderForm();
  renderLedger();
  requestAnimationFrame(fitForm);
}

function switchView(view, options = {}) {
  currentView = view;
  const activeTab = document.querySelector('.view-tab[data-view="' + view + '"]');
  rovingTabs(document.querySelector(".view-tabs"), ".view-tab", activeTab);
  for (const panel of document.querySelectorAll(".view-panel")) {
    const active = panel.dataset.panel === view;
    if (!active && panel.contains(document.activeElement)) activeTab.focus();
    panel.classList.toggle("is-active", active);
    panel.hidden = !active;
    panel.inert = !active;
  }
  const formActive = view === "form";
  dom.surfaceTabs.hidden = !formActive;
  dom.surfaceTabs.inert = !formActive;
  if (options.focusTab) activeTab.focus();
  if (view === "form") requestAnimationFrame(fitForm);
}

function setZoom(next) {
  zoom = Math.max(0.42, Math.min(2.2, next));
  renderForm();
}

function fitForm() {
  if (currentView !== "form") return;
  const geometry = currentEdition.geometry[currentSurface];
  const available = Math.max(280, dom.paperStage.clientWidth - 72);
  setZoom(Math.min(1.25, available / Number(geometry.page_width_pt)));
}

function filterInputs(query) {
  const normalized = query.trim().toLowerCase();
  for (const details of dom.inputGroups.querySelectorAll("details")) {
    let visible = 0;
    for (const row of details.querySelectorAll(".input-row")) {
      const match = !normalized || row.dataset.search.includes(normalized);
      row.classList.toggle("is-hidden", !match);
      if (match) visible += 1;
    }
    details.hidden = visible === 0;
    if (normalized && visible) details.open = true;
  }
}

function requireReady(action) {
  if (calculation.ready) return true;
  showToast("別表二判定・必須区分・年度適用・入力整合を完了するまで" + action + "できません。", true);
  const first = calculation.checks.find((check) => !check.ok && check.level === "error" && check.field);
  if (first) focusField(first.field);
  return false;
}

function downloadJson() {
  if (!requireReady("保存")) return;
  const payload = {
    schema: model.schema,
    edition: currentEdition.id,
    edition_label: currentEdition.display,
    saved_at: new Date().toISOString(),
    inputs: getState(),
    exact_results: calculation.exactValues,
    calculation_sha256: manifest.model && manifest.model.sha256 || "",
  };
  const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json;charset=utf-8" });
  const link = document.createElement("a");
  link.href = URL.createObjectURL(blob);
  link.download = "tokutei_dozoku_ryuhokin_" + currentEdition.id.toLowerCase() + "_" + new Date().toISOString().slice(0, 10).replaceAll("-", "") + ".json";
  link.click();
  setTimeout(() => URL.revokeObjectURL(link.href), 0);
  showToast("入力・計算結果をJSONで保存しました。");
}

function csvCell(value) {
  return '"' + String(value ?? "").replaceAll('"', '""') + '"';
}

function buildCsvText() {
  const rows = [["区分", "帳票・入力群", "欄番号", "項目", "厳密値", "数式・転記元", "年度版"]];
  for (const group of currentEdition.input_groups) {
    for (const item of group.items) {
      rows.push([
        "入力",
        group.title,
        "",
        item.label,
        getState()[item.name] ?? "",
        item.source || item.desc || "",
        currentEdition.display,
      ]);
    }
  }
  for (const surface of Object.keys(currentEdition.geometry)) {
    for (const row of formulaRows(currentEdition, surface)) {
      rows.push([
        "計算",
        SURFACE_META[surface].label,
        row.no,
        row.item,
        calculation.exactValues[row.name] ?? "",
        row.formula || row.rule || "",
        currentEdition.display,
      ]);
    }
  }
  return "\uFEFF" + rows.map((row) => row.map(csvCell).join(",")).join("\r\n") + "\r\n";
}

function downloadCsv() {
  if (!requireReady("CSV出力")) return;
  const csv = buildCsvText();
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
  const link = document.createElement("a");
  link.href = URL.createObjectURL(blob);
  link.download = "tokutei_dozoku_ryuhokin_" + currentEdition.id.toLowerCase() + "_" + new Date().toISOString().slice(0, 10).replaceAll("-", "") + ".csv";
  link.click();
  setTimeout(() => URL.revokeObjectURL(link.href), 0);
  showToast("入力・厳密計算結果をCSVで保存しました。");
}

async function loadJson(file) {
  try {
    const payload = JSON.parse(await file.text());
    const edition = editionById(payload.edition);
    if (payload.schema !== model.schema || !edition || typeof payload.inputs !== "object") throw new Error("この再現版の保存データではありません。");
    if (payload.edition_label && payload.edition_label !== edition.display) throw new Error("年度IDと年度表示が一致しません。");
    stateByEdition[payload.edition] = { ...defaultInputState(edition), ...payload.inputs };
    engagedEditions.add(payload.edition);
    switchEdition(payload.edition, { silent: true });
    writeStorage();
    showToast(edition.display + "の入力を読み込みました。");
  } catch (error) {
    showToast("読込失敗：" + error.message, true);
  } finally {
    dom.loadFileInput.value = "";
  }
}

async function confirmAction(title, message) {
  dom.confirmTitle.textContent = title;
  dom.confirmMessage.textContent = message;
  dom.confirmDialog.showModal();
  return new Promise((resolve) => {
    dom.confirmDialog.addEventListener("close", () => resolve(dom.confirmDialog.returnValue === "confirm"), { once: true });
  });
}

async function resetInputs() {
  if (!(await confirmAction(currentEdition.display + "の入力を初期化しますか", "法人名を含む、この年度のブラウザ内入力データを初期値へ戻します。"))) return;
  stateByEdition[currentEdition.id] = defaultInputState(currentEdition);
  engagedEditions.delete(currentEdition.id);
  renderInputs();
  recalculate();
  writeStorage();
  showToast("入力を初期化しました。");
}

function printCurrent() {
  if (!requireReady("印刷")) return;
  document.body.classList.remove("print-all");
  window.print();
}

async function printAll() {
  if (!requireReady("印刷")) return;
  dom.printStack.replaceChildren();
  for (const surface of Object.keys(currentEdition.geometry)) {
    const paper = create("div", "form-paper print-paper");
    renderFormPaper(paper, surface, true);
    dom.printStack.append(paper);
  }
  document.body.classList.add("print-all");
  const images = [...dom.printStack.querySelectorAll("img")];
  await Promise.all(images.map((image) => new Promise((resolve) => {
    if (image.complete && image.naturalWidth > 0) {
      resolve();
      return;
    }
    image.addEventListener("load", resolve, { once: true });
    image.addEventListener("error", resolve, { once: true });
  })));
  window.print();
}

function mobileContainers() {
  return { inputs: dom.inputDossier, document: dom.documentDesk, audit: dom.auditRail };
}

function syncMobileInert() {
  const panel = document.body.dataset.mobilePanel || "document";
  const containers = mobileContainers();
  if (window.innerWidth <= MOBILE_BREAKPOINT) {
    for (const [name, element] of Object.entries(containers)) {
      const active = name === panel;
      element.inert = !active;
      element.setAttribute("aria-hidden", String(!active));
    }
  } else if (window.innerWidth <= COMPACT_BREAKPOINT) {
    const auditOpen = panel === "audit";
    dom.auditRail.inert = !auditOpen;
    dom.auditRail.setAttribute("aria-hidden", String(!auditOpen));
    for (const element of [dom.inputDossier, dom.documentDesk]) {
      element.inert = auditOpen;
      element.setAttribute("aria-hidden", String(auditOpen));
    }
  } else {
    for (const element of Object.values(containers)) {
      element.inert = false;
      element.setAttribute("aria-hidden", "false");
    }
  }
}

function setMobilePanel(panel, options = {}) {
  const button = document.querySelector('.mobile-dock [data-mobile-panel="' + panel + '"]');
  if (!button) return;
  const activeElement = document.activeElement;
  const nextContainer = mobileContainers()[panel];
  if (options.moveFocus !== false && activeElement && !nextContainer.contains(activeElement)) button.focus();
  document.body.dataset.mobilePanel = panel;
  for (const item of document.querySelectorAll(".mobile-dock [data-mobile-panel]")) {
    const active = item.dataset.mobilePanel === panel;
    item.classList.toggle("is-active", active);
    item.setAttribute("aria-pressed", String(active));
  }
  syncMobileInert();
}

function wireEvents() {
  for (const button of document.querySelectorAll(".view-tab")) {
    button.addEventListener("click", () => switchView(button.dataset.view));
    button.addEventListener("keydown", (event) =>
      wireRovingKeydown(event, document.querySelectorAll(".view-tab"), (target) => switchView(target.dataset.view))
    );
  }
  dom.formulaToggle.addEventListener("change", () => {
    const table = dom.ledgerBody.closest("table");
    if (table) table.classList.toggle("show-formulas", dom.formulaToggle.checked);
  });
  dom.inputSearch.addEventListener("input", () => filterInputs(dom.inputSearch.value));
  dom.expandInputsButton.addEventListener("click", () => {
    const details = [...dom.inputGroups.querySelectorAll("details:not([hidden])")];
    const expand = details.some((item) => !item.open);
    details.forEach((item) => { item.open = expand; });
    dom.expandInputsButton.textContent = expand ? "全折畳" : "全展開";
  });
  dom.saveButton.addEventListener("click", downloadJson);
  dom.csvButton.addEventListener("click", downloadCsv);
  dom.loadButton.addEventListener("click", () => dom.loadFileInput.click());
  dom.loadFileInput.addEventListener("change", () => {
    if (dom.loadFileInput.files[0]) loadJson(dom.loadFileInput.files[0]);
  });
  dom.resetButton.addEventListener("click", resetInputs);
  dom.printButton.addEventListener("click", printCurrent);
  dom.printAllButton.addEventListener("click", printAll);
  dom.zoomInButton.addEventListener("click", () => setZoom(zoom + 0.1));
  dom.zoomOutButton.addEventListener("click", () => setZoom(zoom - 0.1));
  dom.fitButton.addEventListener("click", fitForm);
  for (const button of document.querySelectorAll(".mobile-dock [data-mobile-panel]")) {
    button.addEventListener("click", () => setMobilePanel(button.dataset.mobilePanel));
  }
  window.addEventListener("afterprint", () => {
    document.body.classList.remove("print-all");
    dom.printStack.replaceChildren();
  });
  let resizeTimer;
  window.addEventListener("resize", () => {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(() => {
      syncMobileInert();
      if (currentView === "form") fitForm();
    }, 120);
  });
  document.addEventListener("keydown", (event) => {
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "s") {
      event.preventDefault();
      downloadJson();
    }
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "p") {
      event.preventDefault();
      printCurrent();
    }
  });
}

async function boot() {
  try {
    [model, manifest] = await Promise.all([
      fetch("data/model.json", { cache: "no-store" }).then((response) => {
        if (!response.ok) throw new Error("model.json HTTP " + response.status);
        return response.json();
      }),
      fetch("data/manifest.json", { cache: "no-store" }).then((response) => {
        if (!response.ok) throw new Error("manifest.json HTTP " + response.status);
        return response.json();
      }),
    ]);
    currentEdition = editionById("R8") || model.editions.at(-1);
    readStorage();
    if (!stateByEdition[currentEdition.id]) stateByEdition[currentEdition.id] = defaultInputState(currentEdition);
    wireEvents();
    switchEdition(currentEdition.id, { silent: true });
    switchView("form");
    setMobilePanel("document", { moveFocus: false });
    document.documentElement.dataset.ready = "true";
    window.retainedTaxApp = {
      get model() { return model; },
      get manifest() { return manifest; },
      get edition() { return currentEdition; },
      get state() { return structuredClone(getState()); },
      get result() { return calculation; },
      switchEdition,
      switchSurface,
      switchView,
      setMobilePanel,
      setField(name, value) {
        const definition = flattenInputs(currentEdition).find((item) => item.name === name);
        if (!definition) throw new Error("Unknown field " + name);
        updateField(name, definition.kind, value);
        return calculation;
      },
      audit() {
        return {
          ready: calculation.ready,
          checks: calculation.checks,
          errors: calculation.checks.filter((check) => !check.ok),
          formulaErrors: calculation.formulaErrors,
        };
      },
      recalculate() {
        recalculate();
        return calculation;
      },
      exportCsvText() {
        if (!calculation.ready) throw new Error("Calculation is not ready");
        return buildCsvText();
      },
    };
    requestAnimationFrame(() => {
      fitForm();
      setTimeout(() => dom.bootScreen.classList.add("is-hidden"), 180);
    });
  } catch (error) {
    console.error(error);
    dom.bootScreen.querySelector("p").textContent = "起動できません：" + error.message;
    dom.bootScreen.classList.add("is-error");
  }
}

boot();
