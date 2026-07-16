export class FormulaError extends Error {
  constructor(message, formula = "") {
    super(message);
    this.name = "FormulaError";
    this.formula = formula;
  }
}

export class MissingDependency extends FormulaError {
  constructor(name) {
    super("Missing dependency: " + name);
    this.name = "MissingDependency";
    this.dependency = name;
  }
}

const AST_CACHE = new Map();
const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER);

function absBigInt(value) {
  return value < 0n ? -value : value;
}

function gcd(left, right) {
  let a = absBigInt(left);
  let b = absBigInt(right);
  while (b !== 0n) {
    const next = a % b;
    a = b;
    b = next;
  }
  return a || 1n;
}

function rational(numerator, denominator = 1n) {
  let n = BigInt(numerator);
  let d = BigInt(denominator);
  if (d === 0n) throw new FormulaError("Division by zero");
  if (d < 0n) {
    n = -n;
    d = -d;
  }
  const divisor = gcd(n, d);
  return Object.freeze({ n: n / divisor, d: d / divisor });
}

function isRational(value) {
  return Boolean(value) && typeof value === "object" && typeof value.n === "bigint" && typeof value.d === "bigint";
}

function decimalRational(value) {
  const text = String(value).trim();
  const match = text.match(/^([+-]?)(\d*)(?:\.(\d*))?(?:[eE]([+-]?\d+))?$/);
  if (!match || (!match[2] && !match[3])) throw new FormulaError("Invalid number: " + text);
  const exponent = Number(match[4] || 0);
  if (!Number.isInteger(exponent) || Math.abs(exponent) > 100) throw new FormulaError("Unsupported numeric exponent: " + text);
  const fraction = match[3] || "";
  const digits = (match[2] || "0") + fraction;
  let n = BigInt(digits || "0");
  if (match[1] === "-") n = -n;
  const scale = fraction.length - exponent;
  return scale >= 0 ? rational(n, 10n ** BigInt(scale)) : rational(n * (10n ** BigInt(-scale)), 1n);
}

function toRational(value) {
  if (isRational(value)) return value;
  if (value === "" || value === null || value === undefined || value === false) return rational(0n);
  if (value === true) return rational(1n);
  if (typeof value === "bigint") return rational(value);
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new FormulaError("Non-finite number");
    if (Number.isSafeInteger(value)) return rational(BigInt(value));
    return decimalRational(value.toString());
  }
  return decimalRational(value);
}

function add(left, right) {
  const a = toRational(left);
  const b = toRational(right);
  return rational(a.n * b.d + b.n * a.d, a.d * b.d);
}

function subtract(left, right) {
  const a = toRational(left);
  const b = toRational(right);
  return rational(a.n * b.d - b.n * a.d, a.d * b.d);
}

function multiply(left, right) {
  const a = toRational(left);
  const b = toRational(right);
  return rational(a.n * b.n, a.d * b.d);
}

function divide(left, right) {
  const a = toRational(left);
  const b = toRational(right);
  if (b.n === 0n) throw new FormulaError("Division by zero");
  return rational(a.n * b.d, a.d * b.n);
}

function compare(left, right) {
  const a = toRational(left);
  const b = toRational(right);
  const delta = a.n * b.d - b.n * a.d;
  return delta < 0n ? -1 : delta > 0n ? 1 : 0;
}

function truthy(value) {
  if (isRational(value)) return value.n !== 0n;
  if (Array.isArray(value)) return value.length > 0;
  return Boolean(value);
}

function excelEqual(left, right) {
  if (typeof left === "string" || typeof right === "string") return String(left ?? "") === String(right ?? "");
  try {
    return compare(left, right) === 0;
  } catch {
    return left === right;
  }
}

function canonical(value) {
  if (!isRational(value)) return String(value ?? "");
  return value.d === 1n ? value.n.toString() : value.n.toString() + "/" + value.d.toString();
}

function project(value) {
  if (Array.isArray(value)) return value.map(project);
  if (!isRational(value)) return value;
  if (value.d === 1n) {
    return absBigInt(value.n) <= MAX_SAFE ? Number(value.n) : value.n.toString();
  }
  const number = Number(value.n) / Number(value.d);
  return Number.isFinite(number) ? number : canonical(value);
}

function flatten(values) {
  return values.flatMap((value) => (Array.isArray(value) ? flatten(value) : [value]));
}

function integerTowardZero(value) {
  const number = toRational(value);
  return number.n / number.d;
}

function powerOfTen(places) {
  const integer = integerTowardZero(places);
  if (integer < -100n || integer > 100n) throw new FormulaError("Unsupported rounding precision");
  return integer;
}

function roundDownExact(value, digits = rational(0n)) {
  const number = toRational(value);
  const places = powerOfTen(digits);
  if (places >= 0n) {
    const factor = 10n ** places;
    return rational((number.n * factor) / number.d, factor);
  }
  const factor = 10n ** (-places);
  return rational((number.n / (number.d * factor)) * factor);
}

function divideAwayFromZero(numerator, denominator) {
  let quotient = numerator / denominator;
  if (numerator % denominator !== 0n) quotient += numerator > 0n ? 1n : -1n;
  return quotient;
}

function roundUpExact(value, digits = rational(0n)) {
  const number = toRational(value);
  const places = powerOfTen(digits);
  if (places >= 0n) {
    const factor = 10n ** places;
    return rational(divideAwayFromZero(number.n * factor, number.d), factor);
  }
  const factor = 10n ** (-places);
  return rational(divideAwayFromZero(number.n, number.d * factor) * factor);
}

function minExact(values) {
  if (!values.length) return rational(0n);
  return values.reduce((best, value) => (compare(value, best) < 0 ? toRational(value) : best), toRational(values[0]));
}

function maxExact(values) {
  if (!values.length) return rational(0n);
  return values.reduce((best, value) => (compare(value, best) > 0 ? toRational(value) : best), toRational(values[0]));
}

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
      if (!match) throw new FormulaError("Invalid number near " + input.slice(index), source);
      tokens.push({ type: "number", value: match[0] });
      index += match[0].length;
      continue;
    }
    if (/[A-Za-z_$]/.test(char)) {
      const match = input.slice(index).match(/^[A-Za-z_$][A-Za-z0-9_.$]*/);
      if (!match) throw new FormulaError("Invalid name near " + input.slice(index), source);
      tokens.push({ type: "name", value: match[0] });
      index += match[0].length;
      continue;
    }
    if ("+-*/=><(),:%".includes(char)) {
      tokens.push({ type: "+-*/=><%".includes(char) ? "operator" : "punctuation", value: char });
      index += 1;
      continue;
    }
    throw new FormulaError("Unsupported token '" + char + "'", source);
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
    if (this.current().value !== value) throw new FormulaError("Expected '" + value + "'", this.formula);
    const token = this.current();
    this.index += 1;
    return token;
  }

  match(...values) {
    if (!values.includes(this.current().value)) return null;
    const token = this.current();
    this.index += 1;
    return token;
  }

  parse() {
    const expression = this.comparison();
    if (this.current().type !== "eof") throw new FormulaError("Unexpected token '" + this.current().value + "'", this.formula);
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
    return operator ? { type: "unary", operator: operator.value, argument: this.unary() } : this.postfix();
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
    throw new FormulaError("Unexpected token '" + token.value + "'", this.formula);
  }
}

export function parseFormula(formula) {
  const key = String(formula || "");
  if (!AST_CACHE.has(key)) AST_CACHE.set(key, new Parser(key).parse());
  return AST_CACHE.get(key);
}

function expandRange(start, end, env) {
  if (start.type !== "name" || end.type !== "name") throw new FormulaError("Ranges must use named values");
  const left = start.value.match(/^(.*?)(\d+)$/);
  const right = end.value.match(/^(.*?)(\d+)$/);
  if (!left || !right || left[1] !== right[1]) throw new FormulaError("Unsupported range");
  const values = [];
  const width = Math.max(left[2].length, right[2].length);
  for (let index = Number(left[2]); index <= Number(right[2]); index += 1) {
    const name = left[1] + String(index).padStart(width, "0");
    if (!(name in env)) throw new MissingDependency(name);
    values.push(env[name]);
  }
  return values;
}

function safeDateArgument(value) {
  const integer = integerTowardZero(value);
  if (absBigInt(integer) > MAX_SAFE) throw new FormulaError("DATE argument outside safe range");
  return Number(integer);
}

function evaluateAst(node, env, options) {
  switch (node.type) {
    case "number": return decimalRational(node.value);
    case "string": return node.value;
    case "name": {
      const upper = node.value.toUpperCase();
      if (upper === "TRUE") return true;
      if (upper === "FALSE") return false;
      if (node.value === "$G$10") return options.resolveSpecial(node.value, env);
      if (!(node.value in env)) throw new MissingDependency(node.value);
      return env[node.value];
    }
    case "range": return expandRange(node.start, node.end, env);
    case "percent": return divide(evaluateAst(node.argument, env, options), rational(100n));
    case "unary": {
      const value = toRational(evaluateAst(node.argument, env, options));
      return node.operator === "-" ? rational(-value.n, value.d) : value;
    }
    case "binary": {
      const left = evaluateAst(node.left, env, options);
      const right = evaluateAst(node.right, env, options);
      switch (node.operator) {
        case "+": return add(left, right);
        case "-": return subtract(left, right);
        case "*": return multiply(left, right);
        case "/": return divide(left, right);
        case "=": return excelEqual(left, right);
        case "<>": return !excelEqual(left, right);
        case ">": return compare(left, right) > 0;
        case "<": return compare(left, right) < 0;
        case ">=": return compare(left, right) >= 0;
        case "<=": return compare(left, right) <= 0;
        default: throw new FormulaError("Unsupported operator " + node.operator);
      }
    }
    case "call": {
      const name = node.name.toUpperCase();
      if (name === "IF") {
        const condition = truthy(evaluateAst(node.args[0], env, options));
        return evaluateAst(condition ? node.args[1] : node.args[2], env, options);
      }
      if (name === "AND") return node.args.every((argument) => truthy(evaluateAst(argument, env, options)));
      if (name === "OR") return node.args.some((argument) => truthy(evaluateAst(argument, env, options)));
      const args = node.args.map((argument) => evaluateAst(argument, env, options));
      const values = flatten(args);
      switch (name) {
        case "SUM": return values.reduce((sum, value) => add(sum, value), rational(0n));
        case "MIN": return minExact(values);
        case "MAX": return maxExact(values);
        case "ROUNDDOWN": return roundDownExact(args[0], args[1] ?? rational(0n));
        case "ROUNDUP": return roundUpExact(args[0], args[1] ?? rational(0n));
        case "QUOTIENT": return rational(integerTowardZero(divide(args[0], args[1])));
        case "DATE": return rational(BigInt(Date.UTC(safeDateArgument(args[0]), safeDateArgument(args[1]) - 1, safeDateArgument(args[2]))));
        default: throw new FormulaError("Unsupported function " + node.name);
      }
    }
    default: throw new FormulaError("Unsupported AST node " + node.type);
  }
}

function exactify(value) {
  if (isRational(value) || typeof value === "boolean" || typeof value === "string") return value;
  if (Array.isArray(value)) return value.map(exactify);
  return toRational(value);
}

function evaluateFormulaExact(formula, env, options = {}) {
  const settings = {
    resolveSpecial: options.resolveSpecial || ((name) => { throw new MissingDependency(name); }),
  };
  return evaluateAst(parseFormula(formula), env, settings);
}

export function evaluateFormula(formula, env, options = {}) {
  const exactEnv = Object.fromEntries(Object.entries(env || {}).map(([name, value]) => [name, exactify(value)]));
  const value = evaluateFormulaExact(formula, exactEnv, options);
  return options.exact ? value : project(value);
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
        for (let index = Number(left[2]); index <= Number(right[2]); index += 1) {
          dependencies.add(left[1] + String(index).padStart(width, "0"));
        }
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

const MIN_SUPPORTED_DATE = Date.UTC(1900, 0, 1);
const MAX_SUPPORTED_DATE = Date.UTC(2100, 11, 31);

function dateInSupportedRange(timestamp) {
  return Number.isFinite(timestamp) && timestamp >= MIN_SUPPORTED_DATE && timestamp <= MAX_SUPPORTED_DATE;
}

function formatIsoDate(timestamp) {
  return Number.isFinite(timestamp) ? new Date(timestamp).toISOString().slice(0, 10) : "";
}

function edate(timestamp, months) {
  const date = new Date(timestamp);
  const day = date.getUTCDate();
  const target = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + months, 1));
  const lastDay = new Date(Date.UTC(target.getUTCFullYear(), target.getUTCMonth() + 1, 0)).getUTCDate();
  return Date.UTC(target.getUTCFullYear(), target.getUTCMonth(), Math.min(day, lastDay));
}

function monthBoundary(start, months) {
  const shifted = edate(start, months);
  return new Date(shifted).getUTCDate() < new Date(start).getUTCDate() ? shifted : shifted - 86_400_000;
}

export function calculateMonthCount(startValue, endValue) {
  const start = parseDate(startValue);
  const end = parseDate(endValue);
  if (!dateInSupportedRange(start) || !dateInSupportedRange(end) || end < start || end > monthBoundary(start, 12)) return "";
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
  const taxable = "m_" + String(edition.taxable_row).padStart(2, "0");
  const current = "m_" + String(edition.current_retained_row).padStart(2, "0");
  const allowance = "m_" + String(edition.main_allowance_row).padStart(2, "0");
  for (const dependency of [taxable, current, allowance, "m_01", "inp_months"]) {
    if (!(dependency in env)) throw new MissingDependency(dependency);
  }
  const zero = rational(0n);
  const preTruncation = maxExact([subtract(env[current], env[allowance]), zero]);
  const taxableRemainder = subtract(preTruncation, env[taxable]);
  const annualLimit = compare(env.inp_months, zero) === 0
    ? zero
    : divide(multiply(rational(100000000n), env.inp_months), rational(12n));
  const rawTier = maxExact([
    zero,
    minExact([subtract(env[taxable], env.m_01), subtract(annualLimit, env.m_01)]),
  ]);
  const truncated = roundDownExact(rawTier, rational(-3n));
  const rawRemainder = subtract(rawTier, truncated);
  return compare(rawRemainder, taxableRemainder) > 0 ? roundUpExact(rawTier, rational(-3n)) : truncated;
}

function normalizeInputs(edition, inputState) {
  const env = {};
  for (const definition of flattenInputs(edition)) {
    const raw = inputState[definition.name] ?? definition.default ?? "";
    if (definition.kind === "amount") {
      try { env[definition.name] = toRational(raw === "" ? 0 : raw); } catch { env[definition.name] = rational(0n); }
    } else if (definition.kind === "date") {
      const timestamp = parseDate(raw);
      env[definition.name] = rational(BigInt(Number.isFinite(timestamp) ? timestamp : 0));
    } else {
      env[definition.name] = raw;
    }
  }
  env.inp_months = rational(BigInt(calculateMonthCount(inputState.inp_start, inputState.inp_end) || 0));
  return env;
}

function addCheck(checks, id, ok, level, message, field = "") {
  checks.push({ id, ok: Boolean(ok), level, message, field });
}

function validAmount(raw) {
  try {
    return { ok: true, value: toRational(raw === "" ? 0 : raw) };
  } catch {
    return { ok: false, value: rational(0n) };
  }
}

function validateInputs(edition, state, env, values, formulaErrors) {
  const checks = [];
  const start = parseDate(state.inp_start);
  const end = parseDate(state.inp_end);
  const startInRange = dateInSupportedRange(start);
  const endInRange = dateInSupportedRange(end);
  addCheck(checks, "date_start", startInRange, "error", "開始日は1900-01-01から2100-12-31までの有効な日付で入力してください。", "inp_start");
  addCheck(checks, "date_end", endInRange, "error", "終了日は1900-01-01から2100-12-31までの有効な日付で入力してください。", "inp_end");
  addCheck(checks, "date_order", startInRange && endInRange && end >= start, "error", "終了日は開始日以後にしてください。", "inp_end");
  addCheck(checks, "date_span", startInRange && endInRange && end <= monthBoundary(start, 12), "error", "事業年度は暦による12か月以内にしてください。", "inp_end");
  addCheck(checks, "edition", truthy(env.cfg_applicable), "error", edition.display + "の適用期間と一致していません。", "inp_end");
  addCheck(checks, "special", state.inp_special === "はい", "error",
    state.inp_special === "いいえ" ? "対象外：別表三(一)は使用しません。" : "別表二の判定結果18を確認してください。", "inp_special");
  addCheck(checks, "class", ["該当", "非該当"].includes(state.inp_class), "error", "中小企業者等該当性を選択してください。", "inp_class");
  if (edition.has_f2) {
    addCheck(checks, "group", ["はい", "いいえ"].includes(state.inp_group), "error", "通算法人区分を選択してください。", "inp_group");
    addCheck(checks, "f2_period", truthy(env.cfg_f2_period_ok), "error", "付表二の制度開始日条件を満たしていません。", "inp_start");
  }

  for (const item of flattenInputs(edition)) {
    if (item.kind !== "amount") continue;
    const parsed = validAmount(state[item.name] ?? 0);
    addCheck(checks, "number:" + item.name, parsed.ok, "error", item.label + "は数値で入力してください。", item.name);
    addCheck(checks, "integer:" + item.name, parsed.ok && parsed.value.d === 1n, "error", item.label + "は整数（円）で入力してください。", item.name);
    if (!item.allow_negative) addCheck(checks, "nonnegative:" + item.name, parsed.ok && parsed.value.n >= 0n, "error", item.label + "は0以上で入力してください。", item.name);
    if (item.max) {
      const maximum = BigInt(String(item.max));
      addCheck(checks, "maximum:" + item.name, parsed.ok && absBigInt(parsed.value.n) <= maximum * parsed.value.d, "error", item.label + "は入力上限以下にしてください。", item.name);
    }
  }

  if (edition.has_f2 && state.inp_group === "はい") {
    const total20 = edition.year === 8 ? "inp_f2_b18_1_5_total" : "inp_f2_21";
    const total22 = edition.year === 8 ? "inp_f2_b18_1_6_total" : "inp_f2_23";
    addCheck(checks, "f2_total_20", compare(env[total20], values.f2_20) >= 0, "error", "付表二21欄の控除前合計額は20欄以上にしてください。", total20);
    addCheck(checks, "f2_total_22", compare(env[total22], values.f2_22) >= 0, "error", "付表二23欄の控除前合計額は22欄以上にしてください。", total22);
    addCheck(checks, "f2_ratio", compare(values.f2_24, rational(0n)) >= 0 && compare(values.f2_24, rational(1n)) <= 0, "error", "付表二24欄の割合は0～100％でなければなりません。");
  }
  addCheck(checks, "formula_resolution", formulaErrors.length === 0, "error", "全計算式を解決できませんでした。");
  return checks;
}

export function calculateEdition(edition, inputState) {
  const state = { ...defaultInputState(edition), ...inputState };
  state.inp_months = calculateMonthCount(state.inp_start, state.inp_end);
  const exactEnv = normalizeInputs(edition, state);
  const resolveSpecial = (name) => {
    if (name === "$G$10") return tierTwoValue(edition, exactEnv);
    throw new MissingDependency(name);
  };
  const formulaErrors = [];

  for (const [name, formula] of [
    ["cfg_applicable", edition.applicable_formula],
    ["cfg_f2_period_ok", edition.f2_period_formula || "=TRUE"],
    ["cfg_resident_rate", edition.resident_rate_formula],
    ["cfg_donation_rate", edition.donation_rate_formula],
  ]) {
    try {
      exactEnv[name] = evaluateFormulaExact(formula, exactEnv, { resolveSpecial });
    } catch (error) {
      formulaErrors.push({ name, formula, error: error.message });
      exactEnv[name] = false;
    }
  }

  const nodes = [...edition.f2_rows, ...edition.f1_rows, ...edition.main_rows];
  const unresolved = new Map(nodes.map((row) => [row.name, row]));
  let madeProgress = true;
  let passes = 0;
  while (unresolved.size && madeProgress && passes <= nodes.length + 4) {
    madeProgress = false;
    passes += 1;
    for (const [name, row] of [...unresolved.entries()]) {
      try {
        exactEnv[name] = evaluateFormulaExact(row.formula, exactEnv, { resolveSpecial });
        unresolved.delete(name);
        madeProgress = true;
      } catch (error) {
        if (!(error instanceof MissingDependency)) {
          formulaErrors.push({ name, formula: row.formula, error: error.message });
          exactEnv[name] = rational(0n);
          unresolved.delete(name);
          madeProgress = true;
        }
      }
    }
  }
  for (const [name, row] of unresolved) {
    const missing = formulaDependencies(row.formula).filter((dependency) => !(dependency in exactEnv));
    formulaErrors.push({ name, formula: row.formula, error: "Unresolved dependencies: " + missing.join(", ") });
    exactEnv[name] = rational(0n);
  }

  const exactRows = Object.fromEntries(Object.entries(exactEnv).filter(([name]) => /^(?:m|f1|f2)_\d{2}$/.test(name)));
  const checks = validateInputs(edition, state, exactEnv, exactRows, formulaErrors);
  const ready = checks.every((check) => check.level !== "error" || check.ok);
  const key = (prefix, row) => prefix + "_" + String(row).padStart(2, "0");
  const summaryExact = {
    retained_tax: exactRows.m_08,
    taxable_retained: exactRows[key("m", edition.taxable_row)],
    retention_allowance: exactRows[key("f1", edition.allowance_row)],
    municipal_tax: exactRows[key("m", edition.municipal_row)],
  };
  const values = Object.fromEntries(Object.entries(exactRows).map(([name, value]) => [name, project(value)]));
  const exactValues = Object.fromEntries(Object.entries(exactRows).map(([name, value]) => [name, canonical(value)]));
  const summary = Object.fromEntries(Object.entries(summaryExact).map(([name, value]) => [name, ready ? project(value) : ""]));
  const env = Object.fromEntries(Object.entries(exactEnv).map(([name, value]) => [name, project(value)]));
  env.cfg_calculation_ready = ready;
  return { state, env, values, exactValues, summary, checks, formulaErrors, passes, ready };
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
  return Number.isFinite(start) && Number.isFinite(end) ? formatIsoDate(start) + " ～ " + formatIsoDate(end) : "";
}
