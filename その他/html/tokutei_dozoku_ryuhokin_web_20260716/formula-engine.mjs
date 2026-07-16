export class FormulaError extends Error {
  constructor(message, formula = "") {
    super(message);
    this.name = "FormulaError";
    this.formula = formula;
  }
}

export class MissingDependency extends FormulaError {
  constructor(name) {
    super(`Missing dependency: ${name}`);
    this.name = "MissingDependency";
    this.dependency = name;
  }
}

const AST_CACHE = new Map();

function tokenize(source) {
  const input = String(source || "").trim().replace(/^=/, "");
  const tokens = [];
  let index = 0;
  while (index < input.length) {
    const char = input[index];
    if (/\s/.test(char)) {
      index += 1;
      continue;
    }
    if (char === '"') {
      let value = "";
      index += 1;
      while (index < input.length) {
        if (input[index] === '"' && input[index + 1] === '"') {
          value += '"';
          index += 2;
          continue;
        }
        if (input[index] === '"') {
          index += 1;
          break;
        }
        value += input[index];
        index += 1;
      }
      tokens.push({ type: "string", value });
      continue;
    }
    const two = input.slice(index, index + 2);
    if ([">=", "<=", "<>"].includes(two)) {
      tokens.push({ type: "operator", value: two });
      index += 2;
      continue;
    }
    if (/[0-9.]/.test(char)) {
      const match = input.slice(index).match(/^(?:\d+(?:\.\d*)?|\.\d+)/);
      if (!match) throw new FormulaError(`Invalid number near ${input.slice(index)}`, source);
      tokens.push({ type: "number", value: Number(match[0]) });
      index += match[0].length;
      continue;
    }
    if (/[A-Za-z_$]/.test(char)) {
      const match = input.slice(index).match(/^[A-Za-z_$][A-Za-z0-9_.$]*/);
      if (!match) throw new FormulaError(`Invalid name near ${input.slice(index)}`, source);
      tokens.push({ type: "name", value: match[0] });
      index += match[0].length;
      continue;
    }
    if ("+-*/=><(),:%".includes(char)) {
      const type = "+-*/=><%".includes(char) ? "operator" : "punctuation";
      tokens.push({ type, value: char });
      index += 1;
      continue;
    }
    throw new FormulaError(`Unsupported token '${char}'`, source);
  }
  tokens.push({ type: "eof", value: "" });
  return tokens;
}

class Parser {
  constructor(formula) {
    this.formula = formula;
    this.tokens = tokenize(formula);
    this.index = 0;
  }

  current() {
    return this.tokens[this.index];
  }

  consume(value) {
    if (this.current().value !== value) {
      throw new FormulaError(`Expected '${value}', found '${this.current().value}'`, this.formula);
    }
    const token = this.current();
    this.index += 1;
    return token;
  }

  match(...values) {
    if (values.includes(this.current().value)) {
      const token = this.current();
      this.index += 1;
      return token;
    }
    return null;
  }

  parse() {
    const expression = this.comparison();
    if (this.current().type !== "eof") {
      throw new FormulaError(`Unexpected token '${this.current().value}'`, this.formula);
    }
    return expression;
  }

  comparison() {
    let node = this.additive();
    let operator;
    while ((operator = this.match("=", "<>", ">=", "<=", ">", "<"))) {
      node = { type: "binary", operator: operator.value, left: node, right: this.additive() };
    }
    return node;
  }

  additive() {
    let node = this.multiplicative();
    let operator;
    while ((operator = this.match("+", "-"))) {
      node = { type: "binary", operator: operator.value, left: node, right: this.multiplicative() };
    }
    return node;
  }

  multiplicative() {
    let node = this.unary();
    let operator;
    while ((operator = this.match("*", "/"))) {
      node = { type: "binary", operator: operator.value, left: node, right: this.unary() };
    }
    return node;
  }

  unary() {
    const operator = this.match("+", "-");
    if (operator) return { type: "unary", operator: operator.value, argument: this.unary() };
    return this.postfix();
  }

  postfix() {
    let node = this.primary();
    if (this.match(":")) node = { type: "range", start: node, end: this.primary() };
    while (this.match("%")) node = { type: "percent", argument: node };
    return node;
  }

  primary() {
    const token = this.current();
    if (token.type === "number" || token.type === "string") {
      this.index += 1;
      return { type: token.type, value: token.value };
    }
    if (token.type === "name") {
      this.index += 1;
      const name = token.value;
      if (this.match("(")) {
        const args = [];
        if (!this.match(")")) {
          do {
            args.push(this.comparison());
          } while (this.match(","));
          this.consume(")");
        }
        return { type: "call", name, args };
      }
      return { type: "name", value: name };
    }
    if (this.match("(")) {
      const node = this.comparison();
      this.consume(")");
      return node;
    }
    throw new FormulaError(`Unexpected token '${token.value}'`, this.formula);
  }
}

export function parseFormula(formula) {
  const key = String(formula || "");
  if (!AST_CACHE.has(key)) AST_CACHE.set(key, new Parser(key).parse());
  return AST_CACHE.get(key);
}

function numeric(value) {
  if (value === "" || value === null || value === undefined || value === false) return 0;
  if (value === true) return 1;
  const number = Number(value);
  return Number.isFinite(number) ? number : NaN;
}

function excelEqual(left, right) {
  if (typeof left === "string" || typeof right === "string") return String(left ?? "") === String(right ?? "");
  return numeric(left) === numeric(right);
}

function flatten(values) {
  return values.flatMap((value) => (Array.isArray(value) ? flatten(value) : [value]));
}

function roundDown(value, digits = 0) {
  const number = numeric(value);
  const places = Math.trunc(numeric(digits));
  if (places >= 0) {
    const factor = 10 ** places;
    return Math.trunc(number * factor) / factor;
  }
  const factor = 10 ** -places;
  return Math.trunc(number / factor) * factor;
}

function roundUp(value, digits = 0) {
  const number = numeric(value);
  const places = Math.trunc(numeric(digits));
  const away = (x) => (x >= 0 ? Math.ceil(x) : Math.floor(x));
  if (places >= 0) {
    const factor = 10 ** places;
    return away(number * factor) / factor;
  }
  const factor = 10 ** -places;
  return away(number / factor) * factor;
}

function expandRange(start, end, env) {
  if (start.type !== "name" || end.type !== "name") {
    throw new FormulaError("Ranges must use named values");
  }
  const left = start.value.match(/^(.*?)(\d+)$/);
  const right = end.value.match(/^(.*?)(\d+)$/);
  if (!left || !right || left[1] !== right[1]) throw new FormulaError(`Unsupported range ${start.value}:${end.value}`);
  const from = Number(left[2]);
  const to = Number(right[2]);
  const width = Math.max(left[2].length, right[2].length);
  const values = [];
  for (let index = from; index <= to; index += 1) {
    const name = `${left[1]}${String(index).padStart(width, "0")}`;
    if (!(name in env)) throw new MissingDependency(name);
    values.push(env[name]);
  }
  return values;
}

function evaluateAst(node, env, options) {
  switch (node.type) {
    case "number":
    case "string":
      return node.value;
    case "name": {
      const upper = node.value.toUpperCase();
      if (upper === "TRUE") return true;
      if (upper === "FALSE") return false;
      if (node.value === "$G$10") return options.resolveSpecial(node.value, env);
      if (!(node.value in env)) throw new MissingDependency(node.value);
      return env[node.value];
    }
    case "range":
      return expandRange(node.start, node.end, env);
    case "percent":
      return numeric(evaluateAst(node.argument, env, options)) / 100;
    case "unary": {
      const value = numeric(evaluateAst(node.argument, env, options));
      return node.operator === "-" ? -value : value;
    }
    case "binary": {
      const left = evaluateAst(node.left, env, options);
      const right = evaluateAst(node.right, env, options);
      switch (node.operator) {
        case "+": return numeric(left) + numeric(right);
        case "-": return numeric(left) - numeric(right);
        case "*": return numeric(left) * numeric(right);
        case "/": return numeric(right) === 0 ? NaN : numeric(left) / numeric(right);
        case "=": return excelEqual(left, right);
        case "<>": return !excelEqual(left, right);
        case ">": return numeric(left) > numeric(right);
        case "<": return numeric(left) < numeric(right);
        case ">=": return numeric(left) >= numeric(right);
        case "<=": return numeric(left) <= numeric(right);
        default: throw new FormulaError(`Unsupported operator ${node.operator}`);
      }
    }
    case "call": {
      const name = node.name.toUpperCase();
      if (name === "IF") {
        const condition = Boolean(evaluateAst(node.args[0], env, options));
        return evaluateAst(condition ? node.args[1] : node.args[2], env, options);
      }
      if (name === "AND") return node.args.every((arg) => Boolean(evaluateAst(arg, env, options)));
      if (name === "OR") return node.args.some((arg) => Boolean(evaluateAst(arg, env, options)));
      const args = node.args.map((arg) => evaluateAst(arg, env, options));
      const values = flatten(args).map(numeric);
      switch (name) {
        case "SUM": return values.reduce((sum, value) => sum + value, 0);
        case "MIN": return Math.min(...values);
        case "MAX": return Math.max(...values);
        case "ROUNDDOWN": return roundDown(args[0], args[1]);
        case "ROUNDUP": return roundUp(args[0], args[1]);
        case "QUOTIENT": return Math.trunc(numeric(args[0]) / numeric(args[1]));
        case "DATE": return Date.UTC(numeric(args[0]), numeric(args[1]) - 1, numeric(args[2]));
        default: throw new FormulaError(`Unsupported function ${node.name}`);
      }
    }
    default:
      throw new FormulaError(`Unsupported AST node ${node.type}`);
  }
}

export function evaluateFormula(formula, env, options = {}) {
  const settings = {
    resolveSpecial: options.resolveSpecial || ((name) => { throw new MissingDependency(name); }),
  };
  return evaluateAst(parseFormula(formula), env, settings);
}

export function formulaDependencies(formula) {
  const dependencies = new Set();
  const visit = (node) => {
    if (!node) return;
    if (node.type === "name" && !["TRUE", "FALSE"].includes(node.value.toUpperCase())) dependencies.add(node.value);
    if (node.type === "range") {
      const left = node.start.value.match(/^(.*?)(\d+)$/);
      const right = node.end.value.match(/^(.*?)(\d+)$/);
      if (left && right && left[1] === right[1]) {
        const width = Math.max(left[2].length, right[2].length);
        for (let i = Number(left[2]); i <= Number(right[2]); i += 1) dependencies.add(`${left[1]}${String(i).padStart(width, "0")}`);
      }
      return;
    }
    for (const value of Object.values(node)) {
      if (Array.isArray(value)) value.forEach(visit);
      else if (value && typeof value === "object") visit(value);
    }
  };
  visit(parseFormula(formula));
  return [...dependencies];
}

function parseDate(value) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  const match = String(value || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return NaN;
  const timestamp = Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3]));
  const date = new Date(timestamp);
  if (date.getUTCFullYear() !== Number(match[1]) || date.getUTCMonth() !== Number(match[2]) - 1 || date.getUTCDate() !== Number(match[3])) return NaN;
  return timestamp;
}

function formatIsoDate(timestamp) {
  if (!Number.isFinite(timestamp)) return "";
  return new Date(timestamp).toISOString().slice(0, 10);
}

function edate(timestamp, months) {
  const date = new Date(timestamp);
  const day = date.getUTCDate();
  const targetFirst = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + months, 1));
  const lastDay = new Date(Date.UTC(targetFirst.getUTCFullYear(), targetFirst.getUTCMonth() + 1, 0)).getUTCDate();
  return Date.UTC(targetFirst.getUTCFullYear(), targetFirst.getUTCMonth(), Math.min(day, lastDay));
}

function monthBoundary(start, months) {
  const shifted = edate(start, months);
  return new Date(shifted).getUTCDate() < new Date(start).getUTCDate() ? shifted : shifted - 86_400_000;
}

export function calculateMonthCount(startValue, endValue) {
  const start = parseDate(startValue);
  const end = parseDate(endValue);
  if (!Number.isFinite(start) || !Number.isFinite(end) || end < start || end > monthBoundary(start, 12)) return "";
  let months = 1;
  for (let number = 1; number <= 11; number += 1) {
    if (end > monthBoundary(start, number)) months += 1;
  }
  return Math.max(1, Math.min(12, months));
}

export function flattenInputs(edition) {
  return edition.input_groups.flatMap((group) => group.items);
}

export function defaultInputState(edition) {
  const state = {};
  for (const item of flattenInputs(edition)) state[item.name] = item.default ?? (item.kind === "amount" ? 0 : "");
  state.inp_start = edition.default_start;
  state.inp_end = edition.default_end;
  state.inp_months = calculateMonthCount(state.inp_start, state.inp_end);
  return state;
}

function tierTwoValue(edition, env) {
  const taxable = `m_${String(edition.taxable_row).padStart(2, "0")}`;
  const current = `m_${String(edition.current_retained_row).padStart(2, "0")}`;
  const allowance = `m_${String(edition.main_allowance_row).padStart(2, "0")}`;
  for (const dependency of [taxable, current, allowance, "m_01", "inp_months"]) {
    if (!(dependency in env)) throw new MissingDependency(dependency);
  }
  const preTruncation = Math.max(numeric(env[current]) - numeric(env[allowance]), 0);
  const taxableRemainder = preTruncation - numeric(env[taxable]);
  const annualLimit = numeric(env.inp_months) === 0 ? 0 : 100_000_000 * numeric(env.inp_months) / 12;
  const rawTier = Math.max(0, Math.min(numeric(env[taxable]) - numeric(env.m_01), annualLimit - numeric(env.m_01)));
  const rawRemainder = rawTier - roundDown(rawTier, -3);
  return rawRemainder > taxableRemainder ? roundUp(rawTier, -3) : roundDown(rawTier, -3);
}

function normalizeInputs(edition, inputState) {
  const env = {};
  const definitions = new Map(flattenInputs(edition).map((item) => [item.name, item]));
  for (const [name, definition] of definitions) {
    const raw = inputState[name] ?? definition.default ?? "";
    if (definition.kind === "amount") env[name] = raw === "" ? 0 : Number(raw);
    else if (definition.kind === "date") env[name] = parseDate(raw);
    else env[name] = raw;
  }
  env.inp_months = calculateMonthCount(inputState.inp_start, inputState.inp_end);
  return env;
}

function validateInputs(edition, state, env, values, formulaErrors) {
  const checks = [];
  const add = (id, ok, level, message, field = "") => checks.push({ id, ok: Boolean(ok), level, message, field });
  const start = parseDate(state.inp_start);
  const end = parseDate(state.inp_end);
  add("date_start", Number.isFinite(start), "error", "開始日が有効な日付です", "inp_start");
  add("date_end", Number.isFinite(end), "error", "終了日が有効な日付です", "inp_end");
  add("date_order", Number.isFinite(start) && Number.isFinite(end) && end >= start, "error", "終了日は開始日以後です", "inp_end");
  add("date_span", Number.isFinite(start) && Number.isFinite(end) && end <= monthBoundary(start, 12), "error", "事業年度は暦による12か月以内です", "inp_end");
  add("edition", Boolean(env.cfg_applicable), "warning", `${edition.display}の適用期間と一致します`, "inp_end");
  add("special", state.inp_special === "はい", "warning", "別表二の判定結果が特定同族会社です", "inp_special");

  for (const item of flattenInputs(edition)) {
    if (item.kind !== "amount") continue;
    const value = Number(state[item.name] ?? 0);
    add(`number:${item.name}`, Number.isFinite(value), "error", `${item.label}は数値です`, item.name);
    add(`integer:${item.name}`, Number.isInteger(value), "error", `${item.label}は整数（円）です`, item.name);
    if (!item.allow_negative) add(`nonnegative:${item.name}`, value >= 0, "error", `${item.label}は0以上です`, item.name);
    if (item.max) add(`maximum:${item.name}`, Math.abs(value) <= item.max, "error", `${item.label}は入力上限以内です`, item.name);
  }

  if (edition.has_f2 && state.inp_group === "はい") {
    const total20 = edition.year === 8 ? "inp_f2_b18_1_5_total" : "inp_f2_21";
    const total22 = edition.year === 8 ? "inp_f2_b18_1_6_total" : "inp_f2_23";
    add("f2_total_20", numeric(env[total20]) >= numeric(values.f2_20), "error", "付表二21欄の控除前合計額は20欄以上です", total20);
    add("f2_total_22", numeric(env[total22]) >= numeric(values.f2_22), "error", "付表二23欄の控除前合計額は22欄以上です", total22);
    add("f2_ratio", numeric(values.f2_24) >= 0 && numeric(values.f2_24) <= 1, "error", "付表二24欄の割合は0～100%です");
  }
  add("formula_resolution", formulaErrors.length === 0, "error", "全計算式を解決できました");
  add("finite_outputs", Object.values(values).every((value) => typeof value !== "number" || Number.isFinite(value)), "error", "計算結果に数値エラーがありません");
  return checks;
}

export function calculateEdition(edition, inputState) {
  const state = { ...defaultInputState(edition), ...inputState };
  state.inp_months = calculateMonthCount(state.inp_start, state.inp_end);
  const env = normalizeInputs(edition, state);
  env.inp_months = state.inp_months;
  const resolveSpecial = (name) => {
    if (name === "$G$10") return tierTwoValue(edition, env);
    throw new MissingDependency(name);
  };
  const formulaErrors = [];

  for (const [name, formula] of [
    ["cfg_applicable", edition.applicable_formula],
    ["cfg_resident_rate", edition.resident_rate_formula],
    ["cfg_donation_rate", edition.donation_rate_formula],
  ]) {
    try {
      env[name] = evaluateFormula(formula, env, { resolveSpecial });
    } catch (error) {
      formulaErrors.push({ name, formula, error: error.message });
      env[name] = false;
    }
  }

  const nodes = [
    ...edition.f2_rows,
    ...edition.f1_rows,
    ...edition.main_rows,
  ].map((row) => ({ ...row, name: row.name || `${row === undefined ? "x" : ""}` }));
  const unresolved = new Map(nodes.map((row) => [row.name, row]));
  let madeProgress = true;
  let passes = 0;
  while (unresolved.size && madeProgress && passes <= nodes.length + 4) {
    madeProgress = false;
    passes += 1;
    for (const [name, row] of [...unresolved.entries()]) {
      try {
        env[name] = evaluateFormula(row.formula, env, { resolveSpecial });
        unresolved.delete(name);
        madeProgress = true;
      } catch (error) {
        if (!(error instanceof MissingDependency)) {
          formulaErrors.push({ name, formula: row.formula, error: error.message });
          env[name] = NaN;
          unresolved.delete(name);
          madeProgress = true;
        }
      }
    }
  }
  for (const [name, row] of unresolved) {
    const missing = formulaDependencies(row.formula).filter((dependency) => !(dependency in env));
    formulaErrors.push({ name, formula: row.formula, error: `Unresolved dependencies: ${missing.join(", ")}` });
    env[name] = NaN;
  }

  const values = Object.fromEntries(Object.entries(env).filter(([name]) => /^(?:m|f1|f2)_\d{2}$/.test(name)));
  const checks = validateInputs(edition, state, env, values, formulaErrors);
  const key = (prefix, row) => `${prefix}_${String(row).padStart(2, "0")}`;
  const summary = {
    retained_tax: values.m_08,
    taxable_retained: values[key("m", edition.taxable_row)],
    retention_allowance: values[key("f1", edition.allowance_row)],
    municipal_tax: values[key("m", edition.municipal_row)],
  };
  return { state, env, values, summary, checks, formulaErrors, passes };
}

export function directInputForFormula(formula) {
  const text = String(formula || "").replace(/\s+/g, "");
  let match = text.match(/^=(inp_[A-Za-z0-9_]+)$/);
  if (match) return match[1];
  match = text.match(/^=IF\(inp_group="はい",(inp_[A-Za-z0-9_]+),0\)$/i);
  return match ? match[1] : "";
}

export function displayDateRange(state) {
  const start = parseDate(state.inp_start);
  const end = parseDate(state.inp_end);
  return Number.isFinite(start) && Number.isFinite(end) ? `${formatIsoDate(start)} ～ ${formatIsoDate(end)}` : "";
}
