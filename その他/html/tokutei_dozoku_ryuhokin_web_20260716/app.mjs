import {
  calculateEdition,
  defaultInputState,
  directInputForFormula,
  displayDateRange,
  flattenInputs,
  formulaDependencies,
} from "./formula-engine.mjs";

const STORAGE_KEY = "jplawdb4.retained-earnings-tax-web.v1";
const MOBILE_BREAKPOINT = 820;
const SURFACE_META = {
  main: { label: "別表三(一)", prefix: "m", rowKey: "main_rows" },
  f1: { label: "付表一", prefix: "f1", rowKey: "f1_rows" },
  f2: { label: "付表二", prefix: "f2", rowKey: "f2_rows" },
};

const dom = Object.fromEntries([
  "bootScreen", "yearSwitcher", "editionStamp", "applicableLabel", "monthBadge", "formulaBadge", "excelLink",
  "saveButton", "loadButton", "printButton", "printAllButton", "resetButton", "loadFileInput", "saveState",
  "resultGrid", "inputSearch", "expandInputsButton", "inputGroups", "surfaceTabs", "paperStage", "formPaper",
  "formCaption", "ledgerTitle", "formulaToggle", "ledgerBody", "sourceList", "auditLight", "auditVerdict",
  "checkCounter", "checkList", "traceName", "traceCard", "releaseFacts", "zoomOutput", "zoomInButton",
  "zoomOutButton", "fitButton", "confirmDialog", "confirmTitle", "confirmMessage", "toast", "printStack",
].map((id) => [id, document.getElementById(id)]));

let model;
let manifest;
let currentEdition;
let currentSurface = "main";
let currentView = "form";
let calculation;
let stateByEdition = {};
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
    if (parsed?.schema === model.schema && typeof parsed.states === "object") stateByEdition = parsed.states;
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
  toastTimer = setTimeout(() => dom.toast.classList.remove("is-visible"), 2800);
}

function formatAmount(value) {
  const number = Number(value);
  return Number.isFinite(number) ? yen.format(Math.trunc(number)) : "—";
}

function formatRowValue(row, value) {
  const number = Number(value);
  if (!Number.isFinite(number)) return "#ERROR";
  if (String(row.format || "").includes("0.00000000")) return ratio.format(number);
  return formatAmount(number);
}

function rawInputValue(item, value) {
  if (item.kind === "amount") return Number.isFinite(Number(value)) ? yen.format(Number(value)) : String(value ?? "");
  return value ?? "";
}

function parseAmount(value) {
  const normalized = String(value || "")
    .replace(/[，,\s]/g, "")
    .replace(/[−―ー]/g, "-")
    .replace(/[０-９]/g, (digit) => String.fromCharCode(digit.charCodeAt(0) - 0xfee0));
  if (normalized === "" || normalized === "-") return normalized;
  const number = Number(normalized);
  return Number.isFinite(number) ? number : normalized;
}

function formulaRows(edition, surface = currentSurface) {
  return edition[SURFACE_META[surface].rowKey] || [];
}

function rowName(surface, row) {
  return row.name || `${SURFACE_META[surface].prefix}_${String(row.no).padStart(2, "0")}`;
}

function allFormulaRows(edition) {
  return Object.keys(edition.geometry).flatMap((surface) => formulaRows(edition, surface).map((row) => ({ surface, row, name: rowName(surface, row) })));
}

function renderYearSwitcher() {
  dom.yearSwitcher.replaceChildren();
  for (const edition of model.editions) {
    const button = create("button", "year-button", `R${edition.year}`);
    button.type = "button";
    button.dataset.edition = edition.id;
    button.title = edition.display;
    button.setAttribute("aria-label", `${edition.display}へ切替`);
    button.classList.toggle("is-active", edition.id === currentEdition.id);
    button.addEventListener("click", () => switchEdition(edition.id));
    dom.yearSwitcher.append(button);
  }
}

function renderSurfaceTabs() {
  dom.surfaceTabs.replaceChildren();
  for (const surface of Object.keys(currentEdition.geometry)) {
    const button = create("button", "surface-tab", SURFACE_META[surface].label);
    button.type = "button";
    button.role = "tab";
    button.dataset.surface = surface;
    button.classList.toggle("is-active", surface === currentSurface);
    button.setAttribute("aria-selected", String(surface === currentSurface));
    button.addEventListener("click", () => switchSurface(surface));
    dom.surfaceTabs.append(button);
  }
}

function renderEditionHeader() {
  dom.editionStamp.textContent = currentEdition.display;
  dom.applicableLabel.textContent = currentEdition.applicable_label;
  dom.monthBadge.textContent = `${calculation.state.inp_months || "—"}か月`;
  const formulaCount = allFormulaRows(currentEdition).length;
  dom.formulaBadge.textContent = `${formulaCount} 数式`;
  dom.excelLink.href = currentEdition.reference_workbook.href;
  dom.excelLink.title = `SHA-256 ${currentEdition.reference_workbook.sha256}`;
  document.title = `${currentEdition.display}｜特定同族会社の留保金課税`;
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
    const item = create("div", `result-item${primary ? " primary" : ""}`);
    item.append(create("span", "", label), create("strong", "", formatAmount(value)));
    dom.resultGrid.append(item);
  }
}

function inputId(name) {
  return `field-${currentEdition.id}-${name}`;
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
    if (item.kind === "date") control.type = "date";
    else if (item.kind === "text") control.type = "text";
    else control.type = "text";
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
    details.open = index === 0 || index === 1;
    const summary = create("summary");
    summary.append(create("span", "", group.title), create("span", "input-count", `${group.items.length}項目`));
    details.append(summary);
    for (const item of group.items) {
      const row = create("div", "input-row");
      row.dataset.fieldRow = item.name;
      row.dataset.search = `${item.label} ${item.desc || ""} ${item.source || ""} ${item.note || ""}`.toLowerCase();
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
  const failedFields = new Set(calculation.checks.filter((check) => !check.ok && check.level === "error" && check.field).map((check) => check.field));
  for (const row of dom.inputGroups.querySelectorAll("[data-field-row]")) row.classList.toggle("has-error", failedFields.has(row.dataset.fieldRow));
  for (const control of dom.inputGroups.querySelectorAll("[data-field]")) control.setAttribute("aria-invalid", String(failedFields.has(control.dataset.field)));
  const monthControl = dom.inputGroups.querySelector('[data-field="inp_months"]');
  if (monthControl && document.activeElement !== monthControl) monthControl.value = calculation.state.inp_months || "";
}

function updateField(name, kind, rawValue, sourceElement = null) {
  let value = rawValue;
  if (kind === "amount") value = parseAmount(rawValue);
  getState()[name] = value;
  const isDirectFormInput = sourceElement?.classList.contains("form-direct-input");
  recalculate({ skipForm: isDirectFormInput });
  if (isDirectFormInput) {
    sourceElement.setAttribute("aria-invalid", String(hasFieldError(name)));
    const definition = flattenInputs(currentEdition).find((item) => item.name === name);
    const pairedControl = dom.inputGroups.querySelector(`[data-field="${name}"]`);
    if (definition && pairedControl && pairedControl !== document.activeElement) {
      pairedControl.value = rawInputValue(definition, calculation.state[name]);
    }
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
  element.style.left = `${Number(box.left) / Number(geometry.page_width_pt) * 100}%`;
  element.style.top = `${Number(box.top) / Number(geometry.page_height_pt) * 100}%`;
  element.style.width = `${Number(box.width) / Number(geometry.page_width_pt) * 100}%`;
  element.style.height = `${Number(box.height) / Number(geometry.page_height_pt) * 100}%`;
}

function periodText() {
  const range = displayDateRange(calculation.state);
  if (!range) return "";
  const [start, end] = range.split(" ～ ");
  return `${start.replaceAll("-", "/")}\n${end.replaceAll("-", "/")}`;
}

function renderFormPaper(target, surface, printMode = false) {
  const geometry = currentEdition.geometry[surface];
  const rows = formulaRows(currentEdition, surface);
  target.replaceChildren();
  target.className = printMode ? "form-paper print-paper" : "form-paper";
  target.dataset.surface = surface;
  target.style.setProperty("--paper-width", `${geometry.page_width_pt}px`);
  target.style.setProperty("--paper-height", `${geometry.page_height_pt}px`);
  target.style.setProperty("--form-scale", printMode ? "1" : String(zoom));

  const image = create("img", "form-background");
  image.src = currentEdition.assets[surface];
  image.alt = `${currentEdition.display} ${SURFACE_META[surface].label} 国税庁公式様式`;
  image.draggable = false;
  target.append(image);

  const period = create("span", "form-header-value period", periodText());
  boxStyle(period, geometry.period, geometry);
  target.append(period);
  const corp = create("span", "form-header-value corp", calculation.state.inp_corp || "");
  boxStyle(corp, geometry.corp, geometry);
  target.append(corp);

  for (const row of rows) {
    const key = String(Number(row.no));
    const box = geometry.rows[key];
    if (!box) continue;
    const name = rowName(surface, row);
    const value = calculation.values[name];
    const directInput = directInputForFormula(row.formula);
    let overlay;
    if (!printMode && directInput) {
      overlay = create("input", "form-direct-input");
      overlay.type = "text";
      overlay.inputMode = "decimal";
      overlay.value = formatRowValue(row, calculation.state[directInput] ?? 0);
      overlay.dataset.field = directInput;
      overlay.setAttribute("aria-label", `${SURFACE_META[surface].label} ${row.no}欄 ${row.item}`);
      overlay.setAttribute("aria-invalid", String(hasFieldError(directInput)));
      if (currentEdition.has_f2 && surface === "f2" && calculation.state.inp_group !== "はい") overlay.readOnly = true;
      overlay.addEventListener("focus", () => { overlay.value = String(calculation.state[directInput] ?? 0); });
      overlay.addEventListener("input", () => updateField(directInput, "amount", overlay.value, overlay));
      overlay.addEventListener("blur", () => {
        overlay.value = formatRowValue(row, calculation.state[directInput] ?? 0);
        renderForm();
      });
    } else {
      overlay = printMode ? create("span", "form-value", formatRowValue(row, value)) : create("button", "form-value", formatRowValue(row, value));
      if (!printMode) {
        overlay.type = "button";
        overlay.dataset.formulaName = name;
        overlay.title = `${row.no}欄 ${row.item}\n${row.formula}`;
        overlay.addEventListener("click", () => showTrace(name));
      }
    }
    boxStyle(overlay, box, geometry);
    target.append(overlay);
  }

  if (surface === "main" && geometry.rows["9_inner"]) {
    const innerName = currentEdition.year === 8 ? "inp_m_B4_17_inner" : "inp_m9_inner";
    const inner = create("span", "form-value", formatAmount(calculation.state[innerName] || 0));
    boxStyle(inner, geometry.rows["9_inner"], geometry);
    target.append(inner);
  }
}

function renderForm() {
  renderFormPaper(dom.formPaper, currentSurface, false);
  const geometry = currentEdition.geometry[currentSurface];
  dom.formCaption.textContent = `${currentEdition.display} ${SURFACE_META[currentSurface].label}｜公式原票 ${Number(geometry.page_width_pt).toFixed(2)} × ${Number(geometry.page_height_pt).toFixed(2)} pt｜黄色表示の欄は原票から直接入力できます`;
  dom.zoomOutput.value = `${Math.round(zoom * 100)}%`;
  dom.zoomOutput.textContent = `${Math.round(zoom * 100)}%`;
}

function renderLedger() {
  const meta = SURFACE_META[currentSurface];
  dom.ledgerTitle.textContent = `${currentEdition.display} ${meta.label} — 計算台帳`;
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
    const valueButton = create("button", "ledger-value-button", formatRowValue(row, calculation.values[name]));
    valueButton.type = "button";
    valueButton.addEventListener("click", () => showTrace(name));
    valueCell.append(valueButton);
    const checkCell = create("td");
    checkCell.append(create("span", "row-check", Number.isFinite(Number(calculation.values[name])) ? "✓" : "!"));
    tr.append(item, rule, valueCell, checkCell);
    dom.ledgerBody.append(tr);
  }
  dom.ledgerBody.closest("table")?.classList.toggle("show-formulas", dom.formulaToggle.checked);
}

function renderSources() {
  dom.sourceList.replaceChildren();
  currentEdition.sources.forEach((source) => {
    const item = create("li", "source-item");
    const body = create("div");
    const link = create("a", "", source.title);
    link.href = source.url;
    link.target = "_blank";
    link.rel = "noopener noreferrer";
    body.append(link, create("p", "", `${source.organization}｜${source.use || "年度別原票・記載要領"}`));
    if (source.sha256) body.append(create("span", "source-hash", `SHA-256 ${source.sha256}`));
    item.append(body);
    dom.sourceList.append(item);
  });
}

function renderAudit() {
  const failures = calculation.checks.filter((check) => !check.ok);
  const errors = failures.filter((check) => check.level === "error");
  const warnings = failures.filter((check) => check.level === "warning");
  const verdict = errors.length ? "FAIL" : warnings.length ? "要確認" : "PASS";
  const verdictClass = errors.length ? "is-fail" : warnings.length ? "is-warning" : "is-pass";
  dom.auditLight.className = `audit-light ${errors.length ? "is-fail" : "is-pass"}`;
  dom.auditVerdict.className = `audit-verdict ${verdictClass}`;
  dom.auditVerdict.replaceChildren();
  const strong = create("strong", "", verdict);
  strong.append(create("span", "", `${calculation.checks.length - failures.length}/${calculation.checks.length} CHECKS`));
  dom.auditVerdict.append(strong, create("p", "", errors.length ? `${errors.length}件の入力・計算エラーを修正してください。` : warnings.length ? `${warnings.length}件の適用判定を確認してください。計算式自体は解決済みです。` : "入力規則、年度適用、数式解決および欄間整合性に問題はありません。"));
  dom.checkCounter.textContent = `${failures.length}件 要確認`;
  dom.checkList.replaceChildren();

  const keyPasses = calculation.checks.filter((check) => check.ok && ["date_start", "date_end", "date_order", "date_span", "edition", "special", "f2_total_20", "f2_total_22", "f2_ratio", "formula_resolution", "finite_outputs"].includes(check.id));
  const visibleChecks = failures.length ? [...failures, ...keyPasses.slice(-2)] : keyPasses;
  const amountChecks = calculation.checks.filter((check) => /^(number|integer|nonnegative|maximum):/.test(check.id));
  if (amountChecks.length) visibleChecks.push({ id: "amount_aggregate", ok: amountChecks.every((check) => check.ok), level: "error", message: `金額入力規則 ${amountChecks.filter((check) => check.ok).length}/${amountChecks.length}件`, field: "" });
  for (const check of visibleChecks) {
    const item = create("div", `check-item${check.ok ? "" : " is-fail"}${check.level === "warning" ? " is-warning" : ""}`, check.message);
    if (!check.ok && check.field) {
      item.tabIndex = 0;
      item.role = "button";
      item.addEventListener("click", () => focusField(check.field));
      item.addEventListener("keydown", (event) => { if (["Enter", " "].includes(event.key)) focusField(check.field); });
    }
    dom.checkList.append(item);
  }
  renderReleaseFacts();
}

function renderReleaseFacts() {
  const facts = [
    ["MODEL", manifest.model?.sha256?.slice(0, 16) || "—"],
    ["YEAR", currentEdition.id],
    ["INPUTS", String(flattenInputs(currentEdition).length)],
    ["FORMULAS", String(allFormulaRows(currentEdition).length)],
    ["SURFACES", String(Object.keys(currentEdition.geometry).length)],
    ["EXCEL", currentEdition.reference_workbook.sha256?.slice(0, 16) || "—"],
    ["BUILD", manifest.status || "BUILD"],
  ];
  dom.releaseFacts.replaceChildren();
  for (const [key, value] of facts) dom.releaseFacts.append(create("dt", "", key), create("dd", "", value));
}

function showTrace(name) {
  const found = allFormulaRows(currentEdition).find((entry) => entry.name === name);
  if (!found) return;
  const dependencies = formulaDependencies(found.row.formula);
  dom.traceName.textContent = `${SURFACE_META[found.surface].label} ${found.row.no}欄`;
  dom.traceCard.replaceChildren();
  const result = create("div", "trace-result");
  result.append(create("span", "", found.row.item), create("strong", "", formatRowValue(found.row, calculation.values[name])));
  dom.traceCard.append(result, create("code", "trace-formula", found.row.formula));
  const list = create("ul", "trace-deps");
  for (const dependency of dependencies) {
    const item = create("li");
    const value = dependency === "$G$10" ? "端数処理ヘルパー" : calculation.env[dependency];
    item.append(create("code", "", dependency), create("span", "", typeof value === "number" ? formatAmount(value) : String(value ?? "未解決")));
    list.append(item);
  }
  if (!dependencies.length) list.append(create("li", "", "直接値・定数式"));
  dom.traceCard.append(list);
}

function focusField(name) {
  const control = dom.inputGroups.querySelector(`[data-field="${CSS.escape(name)}"]`);
  if (!control) return;
  const details = control.closest("details");
  if (details) details.open = true;
  const row = control.closest(".input-row");
  row?.classList.add("is-target");
  setTimeout(() => row?.classList.remove("is-target"), 1800);
  control.scrollIntoView({ behavior: "smooth", block: "center" });
  control.focus({ preventScroll: true });
  if (window.innerWidth <= MOBILE_BREAKPOINT) setMobilePanel("inputs");
}

function switchEdition(id, options = {}) {
  const edition = editionById(id);
  if (!edition) return;
  currentEdition = edition;
  if (!stateByEdition[id]) stateByEdition[id] = defaultInputState(edition);
  currentSurface = currentSurface in edition.geometry ? currentSurface : "main";
  renderYearSwitcher();
  renderSurfaceTabs();
  renderInputs();
  renderSources();
  recalculate();
  if (!options.silent) {
    scheduleStorage();
    showToast(`${edition.display}へ切り替えました`);
  }
  requestAnimationFrame(fitForm);
}

function switchSurface(surface) {
  if (!(surface in currentEdition.geometry)) return;
  currentSurface = surface;
  renderSurfaceTabs();
  renderForm();
  renderLedger();
  requestAnimationFrame(fitForm);
}

function switchView(view) {
  currentView = view;
  document.querySelectorAll(".view-tab").forEach((button) => {
    const active = button.dataset.view === view;
    button.classList.toggle("is-active", active);
    button.setAttribute("aria-selected", String(active));
  });
  document.querySelectorAll(".view-panel").forEach((panel) => panel.classList.toggle("is-active", panel.dataset.panel === view));
  if (view === "form") requestAnimationFrame(fitForm);
}

function setZoom(next) {
  zoom = Math.max(.42, Math.min(2.2, next));
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

function downloadJson() {
  const payload = {
    schema: model.schema,
    edition: currentEdition.id,
    edition_label: currentEdition.display,
    saved_at: new Date().toISOString(),
    inputs: getState(),
    calculation_sha256: manifest.model?.sha256 || "",
  };
  const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json;charset=utf-8" });
  const link = document.createElement("a");
  link.href = URL.createObjectURL(blob);
  link.download = `tokutei_dozoku_ryuhokin_${currentEdition.id.toLowerCase()}_${new Date().toISOString().slice(0, 10).replaceAll("-", "")}.json`;
  link.click();
  setTimeout(() => URL.revokeObjectURL(link.href), 0);
  showToast("入力データをJSONで保存しました");
}

async function loadJson(file) {
  try {
    const payload = JSON.parse(await file.text());
    if (payload.schema !== model.schema || !editionById(payload.edition) || typeof payload.inputs !== "object") throw new Error("このアプリの保存データではありません");
    stateByEdition[payload.edition] = { ...defaultInputState(editionById(payload.edition)), ...payload.inputs };
    switchEdition(payload.edition, { silent: true });
    writeStorage();
    showToast(`${editionById(payload.edition).display}の入力を読み込みました`);
  } catch (error) {
    showToast(`読込失敗：${error.message}`, true);
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
  if (!(await confirmAction(`${currentEdition.display}の入力を初期化しますか`, "法人名を含む、この年度のブラウザ内入力データを初期値へ戻します。"))) return;
  stateByEdition[currentEdition.id] = defaultInputState(currentEdition);
  renderInputs();
  recalculate();
  writeStorage();
  showToast("入力を初期化しました");
}

function printCurrent() {
  document.body.classList.remove("print-all");
  window.print();
}

async function printAll() {
  dom.printStack.replaceChildren();
  for (const surface of Object.keys(currentEdition.geometry)) {
    const paper = create("div", "form-paper print-paper");
    renderFormPaper(paper, surface, true);
    dom.printStack.append(paper);
  }
  document.body.classList.add("print-all");
  const images = [...dom.printStack.querySelectorAll("img")];
  await Promise.all(images.map((image) => new Promise((resolve) => {
    if (image.complete && image.naturalWidth > 0) { resolve(); return; }
    image.addEventListener("load", resolve, { once: true });
    image.addEventListener("error", resolve, { once: true });
  })));
  window.print();
}

function setMobilePanel(panel) {
  document.body.dataset.mobilePanel = panel;
  document.querySelectorAll("[data-mobile-panel]").forEach((button) => button.classList.toggle("is-active", button.dataset.mobilePanel === panel));
}

function wireEvents() {
  document.querySelectorAll(".view-tab").forEach((button) => button.addEventListener("click", () => switchView(button.dataset.view)));
  dom.formulaToggle.addEventListener("change", () => dom.ledgerBody.closest("table")?.classList.toggle("show-formulas", dom.formulaToggle.checked));
  dom.inputSearch.addEventListener("input", () => filterInputs(dom.inputSearch.value));
  dom.expandInputsButton.addEventListener("click", () => {
    const details = [...dom.inputGroups.querySelectorAll("details:not([hidden])")];
    const expand = details.some((item) => !item.open);
    details.forEach((item) => { item.open = expand; });
    dom.expandInputsButton.textContent = expand ? "全折畳" : "全展開";
  });
  dom.saveButton.addEventListener("click", downloadJson);
  dom.loadButton.addEventListener("click", () => dom.loadFileInput.click());
  dom.loadFileInput.addEventListener("change", () => { if (dom.loadFileInput.files[0]) loadJson(dom.loadFileInput.files[0]); });
  dom.resetButton.addEventListener("click", resetInputs);
  dom.printButton.addEventListener("click", printCurrent);
  dom.printAllButton.addEventListener("click", printAll);
  dom.zoomInButton.addEventListener("click", () => setZoom(zoom + .1));
  dom.zoomOutButton.addEventListener("click", () => setZoom(zoom - .1));
  dom.fitButton.addEventListener("click", fitForm);
  document.querySelectorAll("[data-mobile-panel]").forEach((button) => button.addEventListener("click", () => setMobilePanel(button.dataset.mobilePanel)));
  window.addEventListener("afterprint", () => {
    document.body.classList.remove("print-all");
    dom.printStack.replaceChildren();
  });
  let resizeTimer;
  window.addEventListener("resize", () => {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(() => { if (currentView === "form") fitForm(); }, 160);
  });
  document.addEventListener("keydown", (event) => {
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "s") { event.preventDefault(); downloadJson(); }
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "p") { event.preventDefault(); printCurrent(); }
  });
}

async function boot() {
  try {
    [model, manifest] = await Promise.all([
      fetch("data/model.json", { cache: "no-store" }).then((response) => {
        if (!response.ok) throw new Error(`model.json HTTP ${response.status}`);
        return response.json();
      }),
      fetch("data/manifest.json", { cache: "no-store" }).then((response) => {
        if (!response.ok) throw new Error(`manifest.json HTTP ${response.status}`);
        return response.json();
      }),
    ]);
    currentEdition = editionById("R8") || model.editions.at(-1);
    readStorage();
    if (!stateByEdition[currentEdition.id]) stateByEdition[currentEdition.id] = defaultInputState(currentEdition);
    wireEvents();
    switchEdition(currentEdition.id, { silent: true });
    renderSources();
    setMobilePanel("document");
    document.documentElement.dataset.ready = "true";
    window.retainedTaxApp = {
      get model() { return model; },
      get manifest() { return manifest; },
      get edition() { return currentEdition; },
      get state() { return structuredClone(getState()); },
      get result() { return calculation; },
      switchEdition,
      switchSurface,
      setField(name, value) {
        const definition = flattenInputs(currentEdition).find((item) => item.name === name);
        if (!definition) throw new Error(`Unknown field ${name}`);
        updateField(name, definition.kind, value);
        return calculation;
      },
      audit() { return { checks: calculation.checks, errors: calculation.checks.filter((check) => !check.ok), formulaErrors: calculation.formulaErrors }; },
      recalculate() { recalculate(); return calculation; },
    };
    requestAnimationFrame(() => {
      fitForm();
      setTimeout(() => dom.bootScreen.classList.add("is-hidden"), 180);
    });
  } catch (error) {
    console.error(error);
    dom.bootScreen.querySelector("p").textContent = `起動できません：${error.message}`;
    dom.bootScreen.classList.add("is-error");
  }
}

boot();
