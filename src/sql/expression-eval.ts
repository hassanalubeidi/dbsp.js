/**
 * SQL Expression Evaluation
 * ==========================
 * 
 * Functions for evaluating SQL expressions at runtime.
 * Supports arithmetic, functions, string operations, date/time, and more.
 * 
 * @module
 */

import type { AggregateArg } from './ast-types';

// ============ CANONICAL KEY GENERATION ============

/**
 * Generate a canonical string key for an aggregate expression.
 * Works with both:
 * - AggregateArg format (from parser/compiler): { type: 'function', functionName: 'IF', args: [...] }
 * - Raw node-sql-parser AST: { type: 'case', args: [...], else: ... }
 * 
 * This ensures the same expression produces the same key regardless of format,
 * enabling accurate matching between compiler-stored values and evaluator lookups.
 */
export function getCanonicalExprKey(expr: any): string {
  if (!expr) return 'null';
  
  // Handle star
  if (expr.type === 'star') return '*';
  
  // Handle column reference
  if (expr.type === 'column' || expr.type === 'column_ref') {
    const table = expr.table ? `${expr.table}.` : '';
    return `col:${table}${expr.column}`;
  }
  
  // Handle numbers
  if (expr.type === 'number') return `num:${expr.value}`;
  if (expr.type === 'expression' && expr.value !== undefined) return `num:${expr.value}`;
  
  // Handle strings
  if (expr.type === 'single_quote_string' || expr.type === 'string') return `str:${expr.value}`;
  if (expr.type === 'expression' && expr.stringValue !== undefined) return `str:${expr.stringValue}`;
  
  // Handle binary expressions (both formats)
  if (expr.type === 'binary_expr' || (expr.type === 'expression' && expr.operator && expr.left && expr.right)) {
    const op = expr.operator;
    const left = getCanonicalExprKey(expr.left);
    const right = getCanonicalExprKey(expr.right);
    return `(${left}${op}${right})`;
  }
  
  // Handle CASE expression (raw node-sql-parser format)
  // { type: 'case', args: [{ cond: ..., result: ... }, ...], else: ... }
  // Note: node-sql-parser sometimes includes ELSE as another WHEN with cond = true or null
  if (expr.type === 'case') {
    const parts: string[] = ['CASE'];
    let elseValue: any = expr.else;
    
    if (expr.args) {
      for (const arg of expr.args) {
        // Check if this is the ELSE clause: cond is null, true, or { type: 'bool', value: true }
        const isElseClause = (
          arg.cond === null ||
          arg.cond === undefined ||
          arg.cond === true ||
          (arg.cond?.type === 'bool' && arg.cond?.value === true)
        );
        
        if (isElseClause) {
          elseValue = arg.result;
          continue;
        }
        const cond = getCanonicalCondKey(arg.cond);
        const result = getCanonicalExprKey(arg.result);
        parts.push(`WHEN(${cond})THEN(${result})`);
      }
    }
    if (elseValue) {
      parts.push(`ELSE(${getCanonicalExprKey(elseValue)})`);
    }
    parts.push('END');
    return parts.join('');
  }
  
  // Handle IF function (parsed format for CASE)
  // { type: 'function', functionName: 'IF', args: [cond, then, else] }
  if (expr.type === 'function' && expr.functionName === 'IF' && expr.args?.length >= 2) {
    const cond = getCanonicalCondFromAggregateArg(expr.args[0]);
    const thenVal = getCanonicalExprKey(expr.args[1]);
    const elseVal = expr.args[2] ? getCanonicalExprKey(expr.args[2]) : 'num:0';
    return `CASEWHEN(${cond})THEN(${thenVal})ELSE(${elseVal})END`;
  }
  
  // Handle EQ/NE/GT/LT/GTE/LTE functions (from parseConditionToExpr)
  if (expr.type === 'function' && ['EQ', 'NE', 'GT', 'LT', 'GTE', 'LTE'].includes(expr.functionName || '')) {
    const opMap: Record<string, string> = { EQ: '=', NE: '!=', GT: '>', LT: '<', GTE: '>=', LTE: '<=' };
    const op = opMap[expr.functionName as string];
    if (op && expr.args?.length >= 2) {
      const left = getCanonicalExprKey(expr.args[0]);
      const right = getCanonicalExprKey(expr.args[1]);
      return `(${left}${op}${right})`;
    }
  }
  
  // Handle other functions
  if (expr.type === 'function' && expr.functionName) {
    const args = (expr.args || []).map(getCanonicalExprKey).join(',');
    return `fn:${expr.functionName}(${args})`;
  }
  
  // Handle node-sql-parser function calls
  if (expr.type === 'function' && (expr.name || expr.name?.name)) {
    const funcName = typeof expr.name === 'string' ? expr.name : expr.name?.name?.[0]?.value || '';
    const args = (expr.args?.value || []).map(getCanonicalExprKey).join(',');
    return `fn:${funcName}(${args})`;
  }
  
  // Handle aggregate function calls in CASE expressions
  if (expr.type === 'aggr_func') {
    const funcName = getFunctionName(expr);
    const argExpr = expr.args?.expr;
    if (argExpr?.type === 'star') {
      return `agg:${funcName}(*)`;
    } else if (argExpr?.type === 'case') {
      return `agg:${funcName}(${getCanonicalExprKey(argExpr)})`;
    } else if (argExpr?.type === 'column_ref') {
      return `agg:${funcName}(col:${argExpr.column})`;
    } else if (argExpr) {
      return `agg:${funcName}(${getCanonicalExprKey(argExpr)})`;
    }
    return `agg:${funcName}(*)`;
  }
  
  return JSON.stringify(expr);
}

/**
 * Get canonical key for a condition expression (used in CASE WHEN).
 * Handles both raw node-sql-parser and AggregateArg formats.
 */
function getCanonicalCondKey(cond: any): string {
  if (!cond) return 'true';
  
  // Binary comparison: { type: 'binary_expr', operator: '=', left: ..., right: ... }
  if (cond.type === 'binary_expr') {
    const op = cond.operator;
    if (['=', '!=', '<>', '>', '<', '>=', '<=', 'LIKE', 'NOT LIKE'].includes(op)) {
      const left = getCanonicalExprKey(cond.left);
      const right = getCanonicalExprKey(cond.right);
      return `(${left}${op}${right})`;
    }
    if (op === 'AND' || op === 'OR') {
      const left = getCanonicalCondKey(cond.left);
      const right = getCanonicalCondKey(cond.right);
      return `(${left}${op}${right})`;
    }
  }
  
  return getCanonicalExprKey(cond);
}

/**
 * Convert an AggregateArg condition (from parseConditionToExpr) back to canonical form.
 */
function getCanonicalCondFromAggregateArg(arg: AggregateArg): string {
  // EQ, NE, GT, LT, GTE, LTE functions
  if (arg.type === 'function' && arg.functionName && arg.args?.length === 2) {
    const opMap: Record<string, string> = { EQ: '=', NE: '!=', GT: '>', LT: '<', GTE: '>=', LTE: '<=' };
    const op = opMap[arg.functionName];
    if (op) {
      const left = getCanonicalExprKey(arg.args[0]);
      const right = getCanonicalExprKey(arg.args[1]);
      return `(${left}${op}${right})`;
    }
  }
  
  // AND represented as multiplication
  if (arg.type === 'expression' && arg.operator === '*' && arg.left && arg.right) {
    const left = getCanonicalCondFromAggregateArg(arg.left);
    const right = getCanonicalCondFromAggregateArg(arg.right);
    return `(${left}AND${right})`;
  }
  
  return getCanonicalExprKey(arg);
}

/**
 * Generate a canonical key for an aggregate function with its argument expression.
 * Used by the compiler to store and the evaluator to look up.
 */
export function getCanonicalAggregateKey(funcName: string, argExpr: any): string {
  const exprKey = argExpr ? getCanonicalExprKey(argExpr) : '*';
  return `${funcName.toUpperCase()}:${exprKey}`;
}

/**
 * Extract the function name from an aggregate function expression.
 * Handles both string and object name formats from node-sql-parser.
 */
export function getFunctionName(expr: any): string {
  if (!expr?.name) return '';
  if (typeof expr.name === 'string') return expr.name.toUpperCase();
  if (expr.name?.name?.[0]?.value) return expr.name.name[0].value.toUpperCase();
  if (expr.name?.toUpperCase) return expr.name.toUpperCase();
  return '';
}

// ============ NUMERIC EXPRESSION EVALUATION ============

/**
 * Evaluate an aggregate expression against a row.
 * Supports: columns, arithmetic operators, and functions (ABS, SIGN, IF, etc.)
 */
export function evaluateAggregateExpr(expr: AggregateArg, row: any): number {
  switch (expr.type) {
    case 'star':
      return 1;
    case 'column':
      return Number(row[expr.column!]) || 0;
    case 'expression':
      if (expr.value !== undefined) {
        return expr.value;
      }
      if (expr.operator && expr.left && expr.right) {
        const left = evaluateAggregateExpr(expr.left, row);
        const right = evaluateAggregateExpr(expr.right, row);
        switch (expr.operator) {
          case '+': return left + right;
          case '-': return left - right;
          case '*': return left * right;
          case '/': return right !== 0 ? left / right : 0;
          case '%': return right !== 0 ? left % right : 0;
          default: return 0;
        }
      }
      return 0;
    case 'function':
      // Built-in functions for aggregation expressions
      return evaluateFunctionExpr(expr, row);
    case 'between':
      // BETWEEN evaluates to 1 (true) or 0 (false)
      if (expr.left && expr.low && expr.high) {
        const val = evaluateAggregateExpr(expr.left, row);
        const low = evaluateAggregateExpr(expr.low, row);
        const high = evaluateAggregateExpr(expr.high, row);
        return (val >= low && val <= high) ? 1 : 0;
      }
      return 0;
    case 'scalar_subquery':
      // Scalar subquery values should be injected into the row by the scalar_join operator
      // Look for a pre-computed value in the row (with _scalar_ prefix or the subquery alias)
      // First, check if the expression has been marked with a specific alias during extraction
      if ((expr as any)._scalarAlias) {
        const alias = (expr as any)._scalarAlias;
        const val = row[alias];
        if (val !== undefined && val !== null) {
          return Number(val) || 0;
        }
      }
      
      // Fallback: Look for the first _scalar_inline_ key (for inline scalar subqueries in CASE)
      const scalarKeys = Object.keys(row).filter(k => k.startsWith('_scalar_inline_'));
      if (scalarKeys.length > 0) {
        // All inline scalar subqueries in the same CASE typically refer to the same subquery
        // Just use the first one's value
        const val = row[scalarKeys[0]];
        if (val !== undefined && val !== null) {
          return Number(val) || 0;
        }
      }
      
      // If still not found, try any _scalar_ prefixed key
      for (const key of Object.keys(row)) {
        if (key.startsWith('_scalar_') || key === '_scalarValue_') {
          const val = row[key];
          if (val !== undefined && val !== null) {
            return Number(val) || 0;
          }
        }
      }
      
      // If not found, return 0 as fallback (subquery value wasn't injected yet)
      return 0;
    default:
      return 0;
  }
}

/**
 * Evaluate a function expression (ABS, SIGN, IF, COALESCE, etc.)
 * Returns a numeric value.
 */
function evaluateFunctionExpr(expr: AggregateArg, row: any): number {
  const funcName = expr.functionName?.toUpperCase() || '';
  const args = expr.args || [];
  
  switch (funcName) {
    case 'ABS': {
      const val = args[0] ? evaluateAggregateExpr(args[0], row) : 0;
      return Math.abs(val);
    }
    case 'SIGN': {
      const val = args[0] ? evaluateAggregateExpr(args[0], row) : 0;
      return Math.sign(val);
    }
    case 'FLOOR': {
      const val = args[0] ? evaluateAggregateExpr(args[0], row) : 0;
      return Math.floor(val);
    }
    case 'CEIL':
    case 'CEILING': {
      const val = args[0] ? evaluateAggregateExpr(args[0], row) : 0;
      return Math.ceil(val);
    }
    case 'ROUND': {
      const val = args[0] ? evaluateAggregateExpr(args[0], row) : 0;
      const decimals = args[1] ? evaluateAggregateExpr(args[1], row) : 0;
      const factor = Math.pow(10, decimals);
      return Math.round(val * factor) / factor;
    }
    case 'SQRT': {
      const val = args[0] ? evaluateAggregateExpr(args[0], row) : 0;
      return Math.sqrt(val);
    }
    case 'POWER':
    case 'POW': {
      const base = args[0] ? evaluateAggregateExpr(args[0], row) : 0;
      const exp = args[1] ? evaluateAggregateExpr(args[1], row) : 1;
      return Math.pow(base, exp);
    }
    case 'LOG': {
      // LOG(x) = natural log, LOG(base, x) = log base
      if (args.length === 1) {
        const val = args[0] ? evaluateAggregateExpr(args[0], row) : 1;
        return val > 0 ? Math.log(val) : 0;
      } else {
        const base = args[0] ? evaluateAggregateExpr(args[0], row) : Math.E;
        const val = args[1] ? evaluateAggregateExpr(args[1], row) : 1;
        return val > 0 && base > 0 ? Math.log(val) / Math.log(base) : 0;
      }
    }
    case 'LOG10': {
      const val = args[0] ? evaluateAggregateExpr(args[0], row) : 1;
      return val > 0 ? Math.log10(val) : 0;
    }
    case 'LOG2': {
      const val = args[0] ? evaluateAggregateExpr(args[0], row) : 1;
      return val > 0 ? Math.log2(val) : 0;
    }
    case 'LN': {
      const val = args[0] ? evaluateAggregateExpr(args[0], row) : 1;
      return val > 0 ? Math.log(val) : 0;
    }
    case 'EXP': {
      const val = args[0] ? evaluateAggregateExpr(args[0], row) : 0;
      return Math.exp(val);
    }
    case 'MOD': {
      const a = args[0] ? evaluateAggregateExpr(args[0], row) : 0;
      const b = args[1] ? evaluateAggregateExpr(args[1], row) : 1;
      return b !== 0 ? a % b : 0;
    }
    case 'GREATEST': {
      // GREATEST(a, b, c, ...) - returns maximum value
      const values = args.map(arg => evaluateAggregateExpr(arg, row));
      return values.length > 0 ? Math.max(...values) : 0;
    }
    case 'LEAST': {
      // LEAST(a, b, c, ...) - returns minimum value
      const values = args.map(arg => evaluateAggregateExpr(arg, row));
      return values.length > 0 ? Math.min(...values) : 0;
    }
    case 'TRUNCATE':
    case 'TRUNC': {
      // TRUNC(x, [decimals]) - truncate towards zero
      const val = args[0] ? evaluateAggregateExpr(args[0], row) : 0;
      const decimals = args[1] ? evaluateAggregateExpr(args[1], row) : 0;
      const factor = Math.pow(10, decimals);
      return Math.trunc(val * factor) / factor;
    }
    case 'PI': {
      return Math.PI;
    }
    case 'RANDOM':
    case 'RAND': {
      return Math.random();
    }
    case 'SIN': {
      const val = args[0] ? evaluateAggregateExpr(args[0], row) : 0;
      return Math.sin(val);
    }
    case 'COS': {
      const val = args[0] ? evaluateAggregateExpr(args[0], row) : 0;
      return Math.cos(val);
    }
    case 'TAN': {
      const val = args[0] ? evaluateAggregateExpr(args[0], row) : 0;
      return Math.tan(val);
    }
    case 'ASIN': {
      const val = args[0] ? evaluateAggregateExpr(args[0], row) : 0;
      return Math.asin(Math.max(-1, Math.min(1, val)));
    }
    case 'ACOS': {
      const val = args[0] ? evaluateAggregateExpr(args[0], row) : 0;
      return Math.acos(Math.max(-1, Math.min(1, val)));
    }
    case 'ATAN': {
      const val = args[0] ? evaluateAggregateExpr(args[0], row) : 0;
      return Math.atan(val);
    }
    case 'ATAN2': {
      const y = args[0] ? evaluateAggregateExpr(args[0], row) : 0;
      const x = args[1] ? evaluateAggregateExpr(args[1], row) : 1;
      return Math.atan2(y, x);
    }
    case 'DEGREES': {
      const val = args[0] ? evaluateAggregateExpr(args[0], row) : 0;
      return val * (180 / Math.PI);
    }
    case 'RADIANS': {
      const val = args[0] ? evaluateAggregateExpr(args[0], row) : 0;
      return val * (Math.PI / 180);
    }
    case 'COALESCE': {
      // Return first non-null, non-zero value
      for (const arg of args) {
        const val = evaluateAggregateExpr(arg, row);
        if (val !== 0 && !isNaN(val)) return val;
      }
      return 0;
    }
    case 'IF':
    case 'IIF': {
      // IF(condition, trueVal, falseVal)
      // condition is treated as truthy if > 0
      const condition = args[0] ? evaluateAggregateExpr(args[0], row) : 0;
      const trueVal = args[1] ? evaluateAggregateExpr(args[1], row) : 1;
      const falseVal = args[2] ? evaluateAggregateExpr(args[2], row) : 0;
      return condition > 0 ? trueVal : falseVal;
    }
    case 'NULLIF': {
      // NULLIF(a, b) returns null (0) if a == b, else a
      const a = args[0] ? evaluateAggregateExpr(args[0], row) : 0;
      const b = args[1] ? evaluateAggregateExpr(args[1], row) : 0;
      return a === b ? 0 : a;
    }
    case 'EQ':
    case 'EQUALS': {
      // EQ(a, b) returns 1 if a == b, else 0
      // Handles both numeric and string comparison
      const a = args[0] ? evaluateExprValue(args[0], row) : 0;
      const b = args[1] ? evaluateExprValue(args[1], row) : 0;
      return a === b ? 1 : 0;
    }
    case 'NE':
    case 'NOTEQUALS': {
      const a = args[0] ? evaluateExprValue(args[0], row) : 0;
      const b = args[1] ? evaluateExprValue(args[1], row) : 0;
      return a !== b ? 1 : 0;
    }
    case 'GT': {
      const a = args[0] ? evaluateAggregateExpr(args[0], row) : 0;
      const b = args[1] ? evaluateAggregateExpr(args[1], row) : 0;
      return a > b ? 1 : 0;
    }
    case 'GTE': {
      const a = args[0] ? evaluateAggregateExpr(args[0], row) : 0;
      const b = args[1] ? evaluateAggregateExpr(args[1], row) : 0;
      return a >= b ? 1 : 0;
    }
    case 'LT': {
      const a = args[0] ? evaluateAggregateExpr(args[0], row) : 0;
      const b = args[1] ? evaluateAggregateExpr(args[1], row) : 0;
      return a < b ? 1 : 0;
    }
    case 'LTE': {
      const a = args[0] ? evaluateAggregateExpr(args[0], row) : 0;
      const b = args[1] ? evaluateAggregateExpr(args[1], row) : 0;
      return a <= b ? 1 : 0;
    }
    case 'OR': {
      // OR(a, b) returns 1 if either is truthy
      const a = args[0] ? evaluateAggregateExpr(args[0], row) : 0;
      const b = args[1] ? evaluateAggregateExpr(args[1], row) : 0;
      return (a > 0 || b > 0) ? 1 : 0;
    }
    case 'AND': {
      // AND(a, b) returns 1 if both are truthy
      const a = args[0] ? evaluateAggregateExpr(args[0], row) : 0;
      const b = args[1] ? evaluateAggregateExpr(args[1], row) : 0;
      return (a > 0 && b > 0) ? 1 : 0;
    }
    case 'NOT': {
      const a = args[0] ? evaluateAggregateExpr(args[0], row) : 0;
      return a > 0 ? 0 : 1;
    }
    default:
      console.warn(`[DBSP SQL] Unknown function: ${funcName}`);
      return 0;
  }
}

// ============ GENERIC VALUE EVALUATION ============

/**
 * Evaluate an expression to a raw value (string, number, or null)
 * Used for comparisons that might involve strings or null handling
 */
function evaluateExprValue(expr: AggregateArg, row: any): any {
  switch (expr.type) {
    case 'column':
      return row[expr.column!];
    case 'expression':
      // Check for string literal first
      if (expr.stringValue !== undefined) {
        return expr.stringValue;
      }
      if (expr.value !== undefined) {
        return expr.value;
      }
      // For arithmetic, return as number
      return evaluateAggregateExpr(expr, row);
    case 'function':
      // Handle functions that return any type (COALESCE, NULLIF)
      return evaluateFunctionExprGeneric(expr, row);
    default:
      return evaluateAggregateExpr(expr, row);
  }
}

/**
 * Evaluate a function expression that may return any type (not just number)
 * Used for COALESCE, NULLIF, string functions, etc. in SELECT projections
 */
export function evaluateFunctionExprGeneric(expr: AggregateArg, row: any): any {
  const funcName = expr.functionName?.toUpperCase() || '';
  const args = expr.args || [];
  
  switch (funcName) {
    // ============ NULL HANDLING ============
    case 'COALESCE': {
      // Return first non-null, non-undefined value
      for (const arg of args) {
        const val = evaluateExprValue(arg, row);
        if (val !== null && val !== undefined) {
          return val;
        }
      }
      return null;
    }
    case 'NULLIF': {
      // NULLIF(a, b) returns null if a == b, else a
      const a = evaluateExprValue(args[0], row);
      const b = evaluateExprValue(args[1], row);
      return a === b ? null : a;
    }
    
    // ============ STRING FUNCTIONS ============
    case 'UPPER': {
      const val = args[0] ? evaluateExprValue(args[0], row) : '';
      return val == null ? null : String(val).toUpperCase();
    }
    case 'LOWER': {
      const val = args[0] ? evaluateExprValue(args[0], row) : '';
      return val == null ? null : String(val).toLowerCase();
    }
    case 'CONCAT': {
      // CONCAT(a, b, ...) - concatenate all arguments
      let result = '';
      for (const arg of args) {
        const val = evaluateExprValue(arg, row);
        if (val != null) {
          result += String(val);
        }
      }
      return result;
    }
    case 'CONCAT_WS': {
      // CONCAT_WS(separator, a, b, ...) - concatenate with separator
      if (args.length === 0) return '';
      const separator = args[0] ? String(evaluateExprValue(args[0], row) ?? '') : '';
      const parts: string[] = [];
      for (let i = 1; i < args.length; i++) {
        const val = evaluateExprValue(args[i], row);
        if (val != null) {
          parts.push(String(val));
        }
      }
      return parts.join(separator);
    }
    case 'SUBSTR':
    case 'SUBSTRING': {
      // SUBSTR(str, start, [length]) - 1-indexed like SQL
      const str = args[0] ? String(evaluateExprValue(args[0], row) ?? '') : '';
      const start = args[1] ? Number(evaluateExprValue(args[1], row)) : 1;
      const length = args[2] ? Number(evaluateExprValue(args[2], row)) : undefined;
      // SQL is 1-indexed, JS is 0-indexed
      const jsStart = start - 1;
      return length !== undefined ? str.substring(jsStart, jsStart + length) : str.substring(jsStart);
    }
    case 'LENGTH':
    case 'LEN':
    case 'CHAR_LENGTH':
    case 'CHARACTER_LENGTH': {
      const val = args[0] ? evaluateExprValue(args[0], row) : '';
      return val == null ? null : String(val).length;
    }
    case 'TRIM': {
      const val = args[0] ? evaluateExprValue(args[0], row) : '';
      return val == null ? null : String(val).trim();
    }
    case 'LTRIM': {
      const val = args[0] ? evaluateExprValue(args[0], row) : '';
      return val == null ? null : String(val).trimStart();
    }
    case 'RTRIM': {
      const val = args[0] ? evaluateExprValue(args[0], row) : '';
      return val == null ? null : String(val).trimEnd();
    }
    case 'REPLACE': {
      // REPLACE(str, from, to)
      const str = args[0] ? String(evaluateExprValue(args[0], row) ?? '') : '';
      const from = args[1] ? String(evaluateExprValue(args[1], row) ?? '') : '';
      const to = args[2] ? String(evaluateExprValue(args[2], row) ?? '') : '';
      return str.split(from).join(to); // Replace all occurrences
    }
    case 'LEFT': {
      // LEFT(str, n) - first n characters
      const str = args[0] ? String(evaluateExprValue(args[0], row) ?? '') : '';
      const n = args[1] ? Number(evaluateExprValue(args[1], row)) : 0;
      return str.substring(0, n);
    }
    case 'RIGHT': {
      // RIGHT(str, n) - last n characters
      const str = args[0] ? String(evaluateExprValue(args[0], row) ?? '') : '';
      const n = args[1] ? Number(evaluateExprValue(args[1], row)) : 0;
      return str.substring(Math.max(0, str.length - n));
    }
    case 'LPAD': {
      // LPAD(str, len, pad) - left pad to length
      const str = args[0] ? String(evaluateExprValue(args[0], row) ?? '') : '';
      const len = args[1] ? Number(evaluateExprValue(args[1], row)) : 0;
      const pad = args[2] ? String(evaluateExprValue(args[2], row) ?? ' ') : ' ';
      return str.padStart(len, pad);
    }
    case 'RPAD': {
      // RPAD(str, len, pad) - right pad to length
      const str = args[0] ? String(evaluateExprValue(args[0], row) ?? '') : '';
      const len = args[1] ? Number(evaluateExprValue(args[1], row)) : 0;
      const pad = args[2] ? String(evaluateExprValue(args[2], row) ?? ' ') : ' ';
      return str.padEnd(len, pad);
    }
    case 'REVERSE': {
      const val = args[0] ? evaluateExprValue(args[0], row) : '';
      return val == null ? null : String(val).split('').reverse().join('');
    }
    case 'POSITION':
    case 'STRPOS':
    case 'INSTR': {
      // POSITION(substr IN str) or STRPOS(str, substr)
      // Returns 1-indexed position, 0 if not found
      const str = args[0] ? String(evaluateExprValue(args[0], row) ?? '') : '';
      const substr = args[1] ? String(evaluateExprValue(args[1], row) ?? '') : '';
      const idx = str.indexOf(substr);
      return idx === -1 ? 0 : idx + 1;
    }
    case 'SPLIT_PART': {
      // SPLIT_PART(str, delimiter, part) - 1-indexed
      const str = args[0] ? String(evaluateExprValue(args[0], row) ?? '') : '';
      const delimiter = args[1] ? String(evaluateExprValue(args[1], row) ?? '') : '';
      const partNum = args[2] ? Number(evaluateExprValue(args[2], row)) : 1;
      const parts = str.split(delimiter);
      // SQL is 1-indexed
      return parts[partNum - 1] ?? '';
    }
    case 'REPEAT': {
      // REPEAT(str, n)
      const str = args[0] ? String(evaluateExprValue(args[0], row) ?? '') : '';
      const n = args[1] ? Math.max(0, Math.floor(Number(evaluateExprValue(args[1], row)))) : 0;
      return str.repeat(n);
    }
    case 'INITCAP': {
      // INITCAP - capitalize first letter of each word
      const val = args[0] ? evaluateExprValue(args[0], row) : '';
      if (val == null) return null;
      return String(val).toLowerCase().replace(/\b\w/g, c => c.toUpperCase());
    }
    case 'PRINTF':
    case 'FORMAT': {
      // PRINTF(format, arg1, arg2, ...) - SQLite style formatted string
      // Supports: %s (string), %d (integer), %f (float), %c (char), %% (literal %)
      // Also: %e (exponential), %g (general), %x/%X (hex), %o (octal), %b (binary)
      // Width and precision: %10s, %.2f, %10.2f, %-10s (left align)
      if (args.length === 0) return '';
      const format = args[0] ? String(evaluateExprValue(args[0], row) ?? '') : '';
      const values = args.slice(1).map(arg => evaluateExprValue(arg, row));
      
      let valueIndex = 0;
      return format.replace(/%(-?)(\d+)?(?:\.(\d+))?([sdfeEgGxXobci%])/g, 
        (match, leftAlign, width, precision, specifier) => {
          if (specifier === '%') return '%';
          if (valueIndex >= values.length) return match;
          
          const val = values[valueIndex++];
          const w = width ? parseInt(width, 10) : 0;
          const p = precision ? parseInt(precision, 10) : undefined;
          const left = leftAlign === '-';
          
          let result: string;
          switch (specifier) {
            case 's':
              result = val == null ? 'NULL' : String(val);
              if (p !== undefined) result = result.slice(0, p);
              break;
            case 'd':
            case 'i':
              result = String(Math.floor(Number(val) || 0));
              break;
            case 'f':
              result = p !== undefined 
                ? (Number(val) || 0).toFixed(p)
                : String(Number(val) || 0);
              break;
            case 'e':
              result = (Number(val) || 0).toExponential(p ?? 6);
              break;
            case 'E':
              result = (Number(val) || 0).toExponential(p ?? 6).toUpperCase();
              break;
            case 'g':
            case 'G':
              result = String(Number(val) || 0);
              break;
            case 'x':
              result = Math.floor(Number(val) || 0).toString(16);
              break;
            case 'X':
              result = Math.floor(Number(val) || 0).toString(16).toUpperCase();
              break;
            case 'o':
              result = Math.floor(Number(val) || 0).toString(8);
              break;
            case 'b':
              result = Math.floor(Number(val) || 0).toString(2);
              break;
            case 'c':
              // Character from code point
              const code = Math.floor(Number(val) || 0);
              result = code > 0 ? String.fromCharCode(code) : '';
              break;
            default:
              result = String(val);
          }
          
          // Apply width padding
          if (w > 0 && result.length < w) {
            const padding = ' '.repeat(w - result.length);
            result = left ? result + padding : padding + result;
          }
          
          return result;
        }
      );
    }
    case 'GLOB': {
      // GLOB(pattern, string) - Unix glob-style pattern matching (case-sensitive)
      // * matches any sequence, ? matches any single character
      // [...] matches character class, [^...] matches negated character class
      const pattern = args[0] ? String(evaluateExprValue(args[0], row) ?? '') : '';
      const str = args[1] ? String(evaluateExprValue(args[1], row) ?? '') : '';
      
      // Convert glob pattern to regex
      let regexPattern = '';
      let i = 0;
      while (i < pattern.length) {
        const char = pattern[i];
        switch (char) {
          case '*':
            regexPattern += '.*';
            break;
          case '?':
            regexPattern += '.';
            break;
          case '[':
            // Character class
            let j = i + 1;
            let classStr = '[';
            if (pattern[j] === '^' || pattern[j] === '!') {
              classStr += '^';
              j++;
            }
            while (j < pattern.length && pattern[j] !== ']') {
              classStr += pattern[j];
              j++;
            }
            classStr += ']';
            regexPattern += classStr;
            i = j;
            break;
          // Escape special regex characters
          case '.':
          case '+':
          case '^':
          case '$':
          case '|':
          case '(':
          case ')':
          case '{':
          case '}':
          case '\\':
            regexPattern += '\\' + char;
            break;
          default:
            regexPattern += char;
        }
        i++;
      }
      
      try {
        const regex = new RegExp('^' + regexPattern + '$');
        return regex.test(str) ? 1 : 0;
      } catch {
        return 0;
      }
    }
    case 'HEX': {
      // HEX(value) - Return hexadecimal representation
      const val = args[0] ? evaluateExprValue(args[0], row) : null;
      if (val == null) return null;
      
      if (typeof val === 'number') {
        return Math.floor(val).toString(16).toUpperCase();
      }
      // For strings, return hex of each character code
      const str = String(val);
      return [...str].map(c => c.charCodeAt(0).toString(16).toUpperCase().padStart(2, '0')).join('');
    }
    case 'CHAR': {
      // CHAR(n1, n2, ...) - Return string from Unicode code points
      const chars = args.map(arg => {
        const code = Number(evaluateExprValue(arg, row) || 0);
        return code > 0 ? String.fromCharCode(code) : '';
      });
      return chars.join('');
    }
    case 'UNICODE': {
      // UNICODE(str) - Return code point of first character
      const str = args[0] ? String(evaluateExprValue(args[0], row) ?? '') : '';
      return str.length > 0 ? str.charCodeAt(0) : null;
    }
    case 'ZEROBLOB': {
      // ZEROBLOB(n) - Return blob of n zero bytes (represented as string of zeros)
      const n = args[0] ? Math.max(0, Math.floor(Number(evaluateExprValue(args[0], row)))) : 0;
      return '\x00'.repeat(n);
    }
    case 'TYPEOF': {
      // TYPEOF(expr) - Return type of expression as string
      const val = args[0] ? evaluateExprValue(args[0], row) : null;
      if (val === null || val === undefined) return 'null';
      if (typeof val === 'number') {
        return Number.isInteger(val) ? 'integer' : 'real';
      }
      if (typeof val === 'string') return 'text';
      if (typeof val === 'boolean') return 'integer';
      return 'blob';
    }
    
    // ============ DATE/TIME FUNCTIONS ============
    case 'TUMBLE_START': {
      // TUMBLE_START(timestamp, interval) - returns the start of the tumbling window
      const ts = evaluateExprValue(args[0], row);
      const interval = args[1]?.stringValue || args[1]?.value || 3600000; // default 1 hour in ms
      return tumbleStart(ts, interval);
    }
    case 'TUMBLE_END': {
      // TUMBLE_END(timestamp, interval) - returns the end of the tumbling window
      const ts = evaluateExprValue(args[0], row);
      const interval = args[1]?.stringValue || args[1]?.value || 3600000;
      return tumbleEnd(ts, interval);
    }
    case 'NOW':
    case 'CURRENT_TIMESTAMP': {
      // NOW() / CURRENT_TIMESTAMP - returns current timestamp as ISO string
      return new Date().toISOString();
    }
    case 'CURRENT_DATE': {
      // CURRENT_DATE - returns current date as YYYY-MM-DD
      return new Date().toISOString().slice(0, 10);
    }
    case 'CURRENT_TIME': {
      // CURRENT_TIME - returns current time as HH:MM:SS
      return new Date().toISOString().slice(11, 19);
    }
    case 'DATE_TRUNC': {
      // DATE_TRUNC(unit, timestamp) - truncate to specified precision
      const unit = args[0] ? String(evaluateExprValue(args[0], row)).toLowerCase() : 'day';
      const ts = args[1] ? evaluateExprValue(args[1], row) : null;
      if (ts == null) return null;
      
      const date = new Date(ts);
      if (isNaN(date.getTime())) return null;
      
      switch (unit) {
        case 'year':
          return `${date.getUTCFullYear()}-01-01 00:00:00`;
        case 'month':
          return `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, '0')}-01 00:00:00`;
        case 'day':
          return date.toISOString().slice(0, 10) + ' 00:00:00';
        case 'hour':
          return date.toISOString().slice(0, 13) + ':00:00';
        case 'minute':
          return date.toISOString().slice(0, 16) + ':00';
        case 'second':
          return date.toISOString().slice(0, 19);
        default:
          return date.toISOString().slice(0, 10) + ' 00:00:00';
      }
    }
    case 'EXTRACT': {
      // EXTRACT(unit FROM timestamp)
      // node-sql-parser may give us different structures
      const unit = args[0]?.stringValue?.toLowerCase() || args[0]?.column?.toLowerCase() || 'day';
      const ts = args[1] ? evaluateExprValue(args[1], row) : null;
      if (ts == null) return null;
      
      const date = new Date(ts);
      if (isNaN(date.getTime())) return null;
      
      switch (unit) {
        case 'year': return date.getUTCFullYear();
        case 'month': return date.getUTCMonth() + 1;
        case 'day': return date.getUTCDate();
        case 'hour': return date.getUTCHours();
        case 'minute': return date.getUTCMinutes();
        case 'second': return date.getUTCSeconds();
        case 'dow':
        case 'dayofweek': return date.getUTCDay();
        case 'doy':
        case 'dayofyear': {
          const start = new Date(Date.UTC(date.getUTCFullYear(), 0, 0));
          const diff = date.getTime() - start.getTime();
          return Math.floor(diff / (1000 * 60 * 60 * 24));
        }
        case 'week': {
          const start = new Date(Date.UTC(date.getUTCFullYear(), 0, 1));
          const diff = date.getTime() - start.getTime();
          return Math.ceil(diff / (1000 * 60 * 60 * 24 * 7));
        }
        case 'quarter': return Math.floor(date.getUTCMonth() / 3) + 1;
        case 'epoch': return Math.floor(date.getTime() / 1000);
        default: return null;
      }
    }
    case 'DATE_ADD':
    case 'DATEADD': {
      // DATE_ADD(timestamp, interval) or DATE_ADD(timestamp, amount, unit)
      const ts = args[0] ? evaluateExprValue(args[0], row) : null;
      if (ts == null) return null;
      
      const date = new Date(ts);
      if (isNaN(date.getTime())) return null;
      
      // Try to parse interval
      const intervalVal = args[1];
      if (intervalVal?.stringValue) {
        const ms = parseIntervalToMs(intervalVal.stringValue);
        return new Date(date.getTime() + ms).toISOString();
      } else if (args[1] && args[2]) {
        // DATE_ADD(ts, 1, 'day')
        const amount = Number(evaluateExprValue(args[1], row));
        const unit = String(evaluateExprValue(args[2], row)).toLowerCase();
        const ms = parseIntervalToMs(`${amount} ${unit}`);
        return new Date(date.getTime() + ms).toISOString();
      }
      return ts;
    }
    case 'DATE_SUB':
    case 'DATESUB': {
      // DATE_SUB(timestamp, interval)
      const ts = args[0] ? evaluateExprValue(args[0], row) : null;
      if (ts == null) return null;
      
      const date = new Date(ts);
      if (isNaN(date.getTime())) return null;
      
      const intervalVal = args[1];
      if (intervalVal?.stringValue) {
        const ms = parseIntervalToMs(intervalVal.stringValue);
        return new Date(date.getTime() - ms).toISOString();
      } else if (args[1] && args[2]) {
        const amount = Number(evaluateExprValue(args[1], row));
        const unit = String(evaluateExprValue(args[2], row)).toLowerCase();
        const ms = parseIntervalToMs(`${amount} ${unit}`);
        return new Date(date.getTime() - ms).toISOString();
      }
      return ts;
    }
    case 'DATEDIFF': {
      // DATEDIFF(ts1, ts2) - difference in days
      const ts1 = args[0] ? evaluateExprValue(args[0], row) : null;
      const ts2 = args[1] ? evaluateExprValue(args[1], row) : null;
      if (ts1 == null || ts2 == null) return null;
      
      const date1 = new Date(ts1);
      const date2 = new Date(ts2);
      if (isNaN(date1.getTime()) || isNaN(date2.getTime())) return null;
      
      const diffMs = date1.getTime() - date2.getTime();
      return Math.floor(diffMs / (1000 * 60 * 60 * 24));
    }
    case 'TO_TIMESTAMP': {
      // TO_TIMESTAMP(epoch_seconds) or TO_TIMESTAMP(string)
      const val = args[0] ? evaluateExprValue(args[0], row) : null;
      if (val == null) return null;
      
      if (typeof val === 'number') {
        return new Date(val * 1000).toISOString();
      }
      return new Date(val).toISOString();
    }
    case 'TO_DATE': {
      // TO_DATE(string) - convert to date string
      const val = args[0] ? evaluateExprValue(args[0], row) : null;
      if (val == null) return null;
      
      const date = new Date(val);
      if (isNaN(date.getTime())) return null;
      return date.toISOString().slice(0, 10);
    }
    
    // ============ SQLITE DATE/TIME FUNCTIONS ============
    case 'DATE': {
      // DATE(timestring, modifier, ...) - Returns the date in YYYY-MM-DD format
      // Supports: 'now', ISO date strings, Unix timestamps, and modifiers
      const timestring = args[0] ? evaluateExprValue(args[0], row) : 'now';
      let date = parseSQLiteTimestring(timestring);
      if (!date) return null;
      
      // Apply modifiers
      for (let i = 1; i < args.length; i++) {
        const modifier = args[i] ? String(evaluateExprValue(args[i], row)) : '';
        date = applySQLiteModifier(date, modifier);
        if (!date) return null;
      }
      
      return formatSQLiteDate(date);
    }
    case 'TIME': {
      // TIME(timestring, modifier, ...) - Returns the time in HH:MM:SS format
      const timestring = args[0] ? evaluateExprValue(args[0], row) : 'now';
      let date = parseSQLiteTimestring(timestring);
      if (!date) return null;
      
      // Apply modifiers
      for (let i = 1; i < args.length; i++) {
        const modifier = args[i] ? String(evaluateExprValue(args[i], row)) : '';
        date = applySQLiteModifier(date, modifier);
        if (!date) return null;
      }
      
      return formatSQLiteTime(date);
    }
    case 'DATETIME': {
      // DATETIME(timestring, modifier, ...) - Returns the datetime in YYYY-MM-DD HH:MM:SS format
      const timestring = args[0] ? evaluateExprValue(args[0], row) : 'now';
      let date = parseSQLiteTimestring(timestring);
      if (!date) return null;
      
      // Apply modifiers
      for (let i = 1; i < args.length; i++) {
        const modifier = args[i] ? String(evaluateExprValue(args[i], row)) : '';
        date = applySQLiteModifier(date, modifier);
        if (!date) return null;
      }
      
      return formatSQLiteDateTime(date);
    }
    case 'STRFTIME': {
      // STRFTIME(format, timestring, modifier, ...) - Returns a formatted date/time string
      const format = args[0] ? String(evaluateExprValue(args[0], row)) : '%Y-%m-%d %H:%M:%S';
      const timestring = args[1] ? evaluateExprValue(args[1], row) : 'now';
      let date = parseSQLiteTimestring(timestring);
      if (!date) return null;
      
      // Apply modifiers
      for (let i = 2; i < args.length; i++) {
        const modifier = args[i] ? String(evaluateExprValue(args[i], row)) : '';
        date = applySQLiteModifier(date, modifier);
        if (!date) return null;
      }
      
      return formatSQLiteStrftime(format, date);
    }
    case 'JULIANDAY': {
      // JULIANDAY(timestring, modifier, ...) - Returns the Julian day number
      const timestring = args[0] ? evaluateExprValue(args[0], row) : 'now';
      let date = parseSQLiteTimestring(timestring);
      if (!date) return null;
      
      // Apply modifiers
      for (let i = 1; i < args.length; i++) {
        const modifier = args[i] ? String(evaluateExprValue(args[i], row)) : '';
        date = applySQLiteModifier(date, modifier);
        if (!date) return null;
      }
      
      return calculateJulianDay(date);
    }
    case 'UNIXEPOCH': {
      // UNIXEPOCH(timestring, modifier, ...) - Returns the Unix timestamp (seconds since 1970-01-01)
      const timestring = args[0] ? evaluateExprValue(args[0], row) : 'now';
      let date = parseSQLiteTimestring(timestring);
      if (!date) return null;
      
      // Apply modifiers
      for (let i = 1; i < args.length; i++) {
        const modifier = args[i] ? String(evaluateExprValue(args[i], row)) : '';
        date = applySQLiteModifier(date, modifier);
        if (!date) return null;
      }
      
      return Math.floor(date.getTime() / 1000);
    }
    
    // ============ TYPE CONVERSION ============
    case 'CAST': {
      // CAST(val AS type) - handled separately in compileSelect usually
      const val = args[0] ? evaluateExprValue(args[0], row) : null;
      const targetType = args[1]?.stringValue?.toUpperCase() || 'VARCHAR';
      
      if (val == null) return null;
      
      switch (targetType) {
        case 'INT':
        case 'INTEGER':
        case 'BIGINT':
          return Math.floor(Number(val)) || 0;
        case 'FLOAT':
        case 'DOUBLE':
        case 'DECIMAL':
        case 'NUMERIC':
          return Number(val) || 0;
        case 'VARCHAR':
        case 'TEXT':
        case 'STRING':
          return String(val);
        case 'BOOLEAN':
        case 'BOOL':
          return Boolean(val);
        default:
          return val;
      }
    }
    
    default:
      // For other functions, delegate to numeric evaluator
      return evaluateFunctionExpr(expr, row);
  }
}

// ============ HELPER FUNCTIONS ============

/**
 * Parse an interval string to milliseconds
 * Supports: "1 hour", "30 minutes", "1 day", etc.
 */
function parseIntervalToMs(interval: string | number): number {
  if (typeof interval === 'number') return interval;
  
  const match = interval.match(/(\d+)\s*(hour|minute|second|day|week|month|year)s?/i);
  if (!match) return 3600000; // default 1 hour
  
  const value = parseInt(match[1], 10);
  const unit = match[2].toLowerCase();
  
  switch (unit) {
    case 'second': return value * 1000;
    case 'minute': return value * 60 * 1000;
    case 'hour': return value * 60 * 60 * 1000;
    case 'day': return value * 24 * 60 * 60 * 1000;
    case 'week': return value * 7 * 24 * 60 * 60 * 1000;
    case 'month': return value * 30 * 24 * 60 * 60 * 1000; // approximate
    case 'year': return value * 365 * 24 * 60 * 60 * 1000; // approximate
    default: return 3600000;
  }
}

/**
 * TUMBLE_START: Get the start of the tumbling window for a timestamp
 */
function tumbleStart(timestamp: any, interval: string | number): string {
  const ts = typeof timestamp === 'string' ? new Date(timestamp).getTime() : timestamp;
  const intervalMs = parseIntervalToMs(interval);
  
  // Align to the window boundary
  const windowStart = Math.floor(ts / intervalMs) * intervalMs;
  
  return new Date(windowStart).toISOString().slice(0, 19).replace('T', ' ');
}

/**
 * TUMBLE_END: Get the end of the tumbling window for a timestamp
 */
function tumbleEnd(timestamp: any, interval: string | number): string {
  const ts = typeof timestamp === 'string' ? new Date(timestamp).getTime() : timestamp;
  const intervalMs = parseIntervalToMs(interval);
  
  // Align to the window boundary and add interval
  const windowStart = Math.floor(ts / intervalMs) * intervalMs;
  const windowEnd = windowStart + intervalMs;
  
  return new Date(windowEnd).toISOString().slice(0, 19).replace('T', ' ');
}

/**
 * Get a string representation of an aggregate expression (for alias generation)
 */
export function getExprString(expr: AggregateArg): string {
  switch (expr.type) {
    case 'star':
      return 'star';
    case 'column':
      return expr.column || '';
    case 'expression':
      if (expr.value !== undefined) {
        return String(expr.value);
      }
      if (expr.operator && expr.left && expr.right) {
        return `${getExprString(expr.left)}_${expr.operator}_${getExprString(expr.right)}`;
      }
      return 'expr';
    case 'function':
      const funcName = expr.functionName || 'fn';
      const argsStr = (expr.args || []).map(getExprString).join('_');
      return `${funcName}_${argsStr}`;
    default:
      return 'unknown';
  }
}

// ============ CASE EXPRESSION EVALUATION ============

/**
 * Interface for CASE column (post-aggregation)
 */
interface CaseColumnDef {
  type: 'case';
  conditions: { when: any; then: any }[];
  else?: any;
  alias?: string;
}

/**
 * Evaluate a CASE WHEN expression against a row.
 * This is used for post-aggregation CASE expressions that reference aggregate aliases.
 * 
 * Example:
 *   CASE WHEN COUNT(*) > 0 THEN SUM(x) / COUNT(*) ELSE 0 END AS avg
 *   
 * @param caseCol - The parsed CASE column definition
 * @param row - The aggregated row (containing aggregate values like totalRFQs, filledRFQs, etc.)
 * @returns The evaluated result
 */
export function evaluateCaseColumn(caseCol: CaseColumnDef, row: any): any {
  // Evaluate each WHEN condition
  for (const condition of caseCol.conditions) {
    const whenResult = evaluateConditionExpr(condition.when, row);
    if (whenResult) {
      return evaluateValueExpr(condition.then, row);
    }
  }
  
  // No condition matched - return ELSE value (or 0 if no ELSE)
  if (caseCol.else !== undefined) {
    return evaluateValueExpr(caseCol.else, row);
  }
  return 0;
}

/**
 * Evaluate a condition expression (for CASE WHEN conditions)
 * Returns true/false based on the condition
 */
function evaluateConditionExpr(expr: any, row: any): boolean {
  if (!expr) return false;
  
  // Binary comparison: a > b, a = b, a != b, etc.
  if (expr.type === 'binary_expr') {
    const left = evaluateValueExpr(expr.left, row);
    const right = evaluateValueExpr(expr.right, row);
    
    switch (expr.operator) {
      case '=': return left === right;
      case '!=': 
      case '<>': return left !== right;
      case '>': return Number(left) > Number(right);
      case '>=': return Number(left) >= Number(right);
      case '<': return Number(left) < Number(right);
      case '<=': return Number(left) <= Number(right);
      case 'AND': return evaluateConditionExpr(expr.left, row) && evaluateConditionExpr(expr.right, row);
      case 'OR': return evaluateConditionExpr(expr.left, row) || evaluateConditionExpr(expr.right, row);
      default: return false;
    }
  }
  
  // Single value that's truthy/falsy
  const val = evaluateValueExpr(expr, row);
  return !!val;
}

/**
 * Evaluate a value expression that may contain:
 * - Column references (to aggregate aliases)
 * - Literal values
 * - Arithmetic expressions
 * - Aggregate function calls (resolved to their aliases)
 */
function evaluateValueExpr(expr: any, row: any): any {
  if (expr === null || expr === undefined) return 0;
  
  // Literal number
  if (typeof expr === 'number') return expr;
  
  // Literal string (used as column reference)
  if (typeof expr === 'string') return row[expr] ?? expr;
  
  // Column reference: { type: 'column_ref', column: 'totalRFQs' }
  if (expr.type === 'column_ref') {
    return row[expr.column] ?? 0;
  }
  
  // Number literal: { type: 'number', value: 123 }
  if (expr.type === 'number') {
    return expr.value;
  }
  
  // String literal: { type: 'single_quote_string', value: 'FILLED' }
  if (expr.type === 'single_quote_string' || expr.type === 'string') {
    return expr.value;
  }
  
  // Binary expression: a + b, a * b, a / b
  if (expr.type === 'binary_expr') {
    const left = evaluateValueExpr(expr.left, row);
    const right = evaluateValueExpr(expr.right, row);
    
    
    switch (expr.operator) {
      case '+': return Number(left) + Number(right);
      case '-': return Number(left) - Number(right);
      case '*': return Number(left) * Number(right);
      case '/': return Number(right) !== 0 ? Number(left) / Number(right) : 0;
      case '%': return Number(right) !== 0 ? Number(left) % Number(right) : 0;
      default: return 0;
    }
  }
  
  // Aggregate function reference: { type: 'aggr_func', name: 'COUNT', args: { expr: { type: 'star' } } }
  // These should have been pre-computed and stored in the row with a canonical key
  if (expr.type === 'aggr_func') {
    const funcName = getFunctionName(expr);
    const argExpr = expr.args?.expr;
    
    // CRITICAL: For aggregates with function arguments (SUM(ABS(col))), we need to find
    // the correct pre-computed value. The canonical key should work, but if the same
    // aggregate appears both as an explicit SELECT column and extracted from CASE,
    // there may be value conflicts. We prefer explicit aliases when available.
    
    // Build the inner column name for SUM(ABS(col)) pattern
    let innerColName = '';
    if (argExpr?.type === 'function') {
      const fnName = argExpr.name?.name?.[0]?.value?.toUpperCase() || argExpr.name?.toUpperCase() || '';
      if (fnName && argExpr.args?.value?.[0]?.type === 'column_ref') {
        innerColName = argExpr.args.value[0].column;
      }
    } else if (argExpr?.type === 'column_ref') {
      innerColName = argExpr.column;
    }
    
    // Look for explicit SELECT column aliases that might contain the aggregate value
    // These aliases are user-defined and typically have meaningful names
    const explicitAliases = Object.keys(row).filter(k => {
      // Skip internal/canonical keys
      if (k.includes(':') || k.startsWith('_') || k.startsWith('sum_') || k.startsWith('count_') ||
          k.startsWith('avg_') || k.startsWith('min_') || k.startsWith('max_')) {
        return false;
      }
      // Keep keys that look like user aliases
      return true;
    });
    
    // Try to match explicit alias to this aggregate based on column name
    if (innerColName && explicitAliases.length > 0) {
      const innerLower = innerColName.toLowerCase();
      for (const alias of explicitAliases) {
        // Check if alias contains the column name (e.g., "sectorNotional" contains "notional")
        if (alias.toLowerCase().includes(innerLower)) {
          const aliasValue = row[alias];
          if (aliasValue !== undefined && typeof aliasValue === 'number') {
            return aliasValue;
          }
        }
      }
    }
    
    // Try the canonical key
    const canonicalKey = getCanonicalAggregateKey(funcName, argExpr);
    if (row[canonicalKey] !== undefined) {
      return row[canonicalKey];
    }
    
    // Fallback: try simpler patterns for backwards compatibility
    let argKey = '*';
    if (argExpr?.type === 'column_ref') {
      argKey = argExpr.column;
    } else if (argExpr?.type === 'star') {
      argKey = '*';
    } else if (argExpr?.type === 'function') {
      // SUM(ABS(col)) - look for existing aliases that might match
      const fnName = argExpr.name?.name?.[0]?.value || argExpr.name || '';
      if (fnName && argExpr.args?.value?.[0]?.type === 'column_ref') {
        const innerCol = argExpr.args.value[0].column;
        argKey = `${fnName}_${innerCol}`;
      }
    }
    
    const possibleKeys = [
      `${funcName}:${argKey}`,                    // e.g., "COUNT:*"
      `${funcName}:star`,                         // e.g., "COUNT:star" (for *)
      `${funcName.toLowerCase()}_${argKey === '*' ? 'star' : argKey}`, // e.g., "count_star"
      `${funcName.toLowerCase()}_star`,           // e.g., "count_star"
      expr.as,                                    // Explicit alias
    ].filter(Boolean);
    
    for (const key of possibleKeys) {
      if (key && row[key] !== undefined) {
        return row[key];
      }
    }
    
    // Also try direct function name
    if (row[funcName] !== undefined) return row[funcName];
    if (row[funcName.toLowerCase()] !== undefined) return row[funcName.toLowerCase()];
    
    return 0;
  }
  
  // Nested CASE expression
  if (expr.type === 'case') {
    return evaluateCaseColumn({
      type: 'case',
      conditions: (expr.args || []).map((arg: any) => ({
        when: arg.cond,
        then: arg.result,
      })),
      else: expr.else,
    }, row);
  }
  
  // Scalar subquery - look up by its assigned alias
  // Check for our marker that stores the alias (set by compiler)
  if (expr._scalarAlias) {
    const val = row[expr._scalarAlias];
    // console.log('[evaluateValueExpr] scalar via _scalarAlias:', expr._scalarAlias, '=', val, 'row keys:', Object.keys(row));
    if (val !== undefined) {
      return val;
    }
  }
  
  // Also check if this is a subquery structure from node-sql-parser
  if (expr.ast && expr.ast.type === 'select') {
    // Try to find by _scalarAlias first
    if (expr._scalarAlias && row[expr._scalarAlias] !== undefined) {
      return row[expr._scalarAlias];
    }
    // Fallback: Try to find any _scalar_inline_N value
    for (const key of Object.keys(row)) {
      if (key.startsWith('_scalar_inline_')) {
        // console.log('[evaluateValueExpr] scalar subquery found via fallback:', key, '=', row[key]);
        return row[key];
      }
    }
    // Scalar subquery value not found - may be expected if not yet injected
    return 0;
  }
  
  return 0;
}

// ============ SQLITE DATE/TIME HELPER FUNCTIONS ============

/**
 * Parse a SQLite timestring into a Date object.
 * Supports various formats:
 * - 'now' - current date/time
 * - 'YYYY-MM-DD' - date only
 * - 'YYYY-MM-DD HH:MM:SS' - date and time
 * - 'YYYY-MM-DD HH:MM:SS.SSS' - date and time with milliseconds
 * - 'YYYY-MM-DDTHH:MM:SS' - ISO 8601 format
 * - 'YYYY-MM-DDTHH:MM:SSZ' - ISO 8601 with Z suffix
 * - Unix timestamp (number or string of digits)
 * - Julian day number (number > 1000000)
 */
function parseSQLiteTimestring(timestring: any): Date | null {
  if (timestring == null) return null;
  
  const str = String(timestring).trim();
  
  // 'now' means current time
  if (str.toLowerCase() === 'now') {
    return new Date();
  }
  
  // Check if it's a numeric timestamp
  if (/^\d+(\.\d+)?$/.test(str)) {
    const num = parseFloat(str);
    // Julian day if very large (> 2000000 means after ~500 AD)
    // Unix epoch is around 2440587.5 in Julian days
    if (num > 1000000) {
      // Might be a Julian day number
      if (num > 2000000 && num < 3000000) {
        // Treat as Julian day
        return julianDayToDate(num);
      }
      // Otherwise treat as Unix timestamp in seconds
      return new Date(num * 1000);
    }
    // Small numbers are probably Unix timestamps
    return new Date(num * 1000);
  }
  
  // Try parsing as ISO date/datetime
  // Handle SQLite format 'YYYY-MM-DD HH:MM:SS' (space instead of T)
  const normalized = str.replace(' ', 'T');
  const date = new Date(normalized);
  
  if (!isNaN(date.getTime())) {
    return date;
  }
  
  // Try just the date part
  const dateMatch = str.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (dateMatch) {
    const [, year, month, day] = dateMatch;
    const timeMatch = str.match(/(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d+))?)?/);
    if (timeMatch) {
      const [, hour, minute, second = '0', ms = '0'] = timeMatch;
      return new Date(Date.UTC(
        parseInt(year, 10),
        parseInt(month, 10) - 1,
        parseInt(day, 10),
        parseInt(hour, 10),
        parseInt(minute, 10),
        parseInt(second, 10),
        parseInt(ms.padEnd(3, '0').slice(0, 3), 10)
      ));
    }
    return new Date(Date.UTC(
      parseInt(year, 10),
      parseInt(month, 10) - 1,
      parseInt(day, 10)
    ));
  }
  
  // Try just time 'HH:MM:SS'
  const timeOnlyMatch = str.match(/^(\d{2}):(\d{2}):(\d{2})/);
  if (timeOnlyMatch) {
    const [, hour, minute, second] = timeOnlyMatch;
    // Use Unix epoch date for time-only values
    return new Date(Date.UTC(
      1970, 0, 1,
      parseInt(hour, 10),
      parseInt(minute, 10),
      parseInt(second, 10)
    ));
  }
  
  return null;
}

/**
 * Apply a SQLite date modifier to a Date object.
 * Modifiers include:
 * - '+N days', '-N months', '+N years', etc.
 * - 'start of month', 'start of year', 'start of day'
 * - 'weekday N' (0 = Sunday, 6 = Saturday)
 * - 'localtime', 'utc'
 * - 'unixepoch' (treat input as Unix timestamp)
 * - 'julianday' (treat input as Julian day)
 * - 'subsec' (high-precision subseconds)
 */
function applySQLiteModifier(date: Date, modifier: string): Date | null {
  if (!date || !modifier) return date;
  
  const mod = modifier.trim().toLowerCase();
  
  // +N unit or -N unit
  const offsetMatch = mod.match(/^([+-])(\d+(?:\.\d+)?)\s*(second|minute|hour|day|month|year)s?$/);
  if (offsetMatch) {
    const [, sign, amount, unit] = offsetMatch;
    const value = parseFloat(amount) * (sign === '-' ? -1 : 1);
    const result = new Date(date.getTime());
    
    switch (unit) {
      case 'second':
        result.setUTCSeconds(result.getUTCSeconds() + Math.floor(value));
        result.setUTCMilliseconds(result.getUTCMilliseconds() + Math.round((value % 1) * 1000));
        break;
      case 'minute':
        result.setUTCMinutes(result.getUTCMinutes() + Math.floor(value));
        result.setUTCSeconds(result.getUTCSeconds() + Math.round((value % 1) * 60));
        break;
      case 'hour':
        result.setUTCHours(result.getUTCHours() + Math.floor(value));
        result.setUTCMinutes(result.getUTCMinutes() + Math.round((value % 1) * 60));
        break;
      case 'day':
        result.setUTCDate(result.getUTCDate() + Math.floor(value));
        result.setUTCHours(result.getUTCHours() + Math.round((value % 1) * 24));
        break;
      case 'month':
        result.setUTCMonth(result.getUTCMonth() + Math.floor(value));
        break;
      case 'year':
        result.setUTCFullYear(result.getUTCFullYear() + Math.floor(value));
        break;
    }
    return result;
  }
  
  // start of month, start of year, start of day
  if (mod === 'start of month') {
    return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1, 0, 0, 0, 0));
  }
  if (mod === 'start of year') {
    return new Date(Date.UTC(date.getUTCFullYear(), 0, 1, 0, 0, 0, 0));
  }
  if (mod === 'start of day') {
    return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate(), 0, 0, 0, 0));
  }
  
  // weekday N (find next occurrence of that weekday)
  const weekdayMatch = mod.match(/^weekday\s+(\d)$/);
  if (weekdayMatch) {
    const targetDay = parseInt(weekdayMatch[1], 10);
    const result = new Date(date.getTime());
    const currentDay = result.getUTCDay();
    let daysToAdd = targetDay - currentDay;
    if (daysToAdd <= 0) daysToAdd += 7;
    result.setUTCDate(result.getUTCDate() + daysToAdd);
    return result;
  }
  
  // localtime - convert from UTC to local (for display purposes, we keep UTC internally)
  if (mod === 'localtime') {
    // In a browser context, we don't really have access to timezone data
    // Just return the date as-is for now
    return date;
  }
  
  // utc - treat as UTC (default behavior)
  if (mod === 'utc') {
    return date;
  }
  
  // unixepoch - interpret the input as Unix epoch seconds
  // This modifier is handled at parse time, not apply time
  if (mod === 'unixepoch') {
    return date;
  }
  
  // auto - automatically detect format (SQLite 3.37.0+)
  if (mod === 'auto') {
    return date;
  }
  
  // Unknown modifier - return unchanged
  return date;
}

/**
 * Format a Date as SQLite DATE format: YYYY-MM-DD
 */
function formatSQLiteDate(date: Date): string {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  const day = String(date.getUTCDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

/**
 * Format a Date as SQLite TIME format: HH:MM:SS
 */
function formatSQLiteTime(date: Date): string {
  const hour = String(date.getUTCHours()).padStart(2, '0');
  const minute = String(date.getUTCMinutes()).padStart(2, '0');
  const second = String(date.getUTCSeconds()).padStart(2, '0');
  return `${hour}:${minute}:${second}`;
}

/**
 * Format a Date as SQLite DATETIME format: YYYY-MM-DD HH:MM:SS
 */
function formatSQLiteDateTime(date: Date): string {
  return `${formatSQLiteDate(date)} ${formatSQLiteTime(date)}`;
}

/**
 * Format a Date using SQLite strftime format specifiers.
 * Supported specifiers:
 * %d - day of month: 01-31
 * %f - fractional seconds: SS.SSS
 * %H - hour: 00-23
 * %j - day of year: 001-366
 * %J - Julian day number
 * %m - month: 01-12
 * %M - minute: 00-59
 * %s - seconds since 1970-01-01
 * %S - seconds: 00-59
 * %w - day of week: 0-6 (Sunday = 0)
 * %W - week of year: 00-53
 * %Y - year: 0000-9999
 * %% - literal %
 */
function formatSQLiteStrftime(format: string, date: Date): string {
  return format.replace(/%([dfHjJmMsSWwY%])/g, (match, specifier) => {
    switch (specifier) {
      case 'd': return String(date.getUTCDate()).padStart(2, '0');
      case 'f': {
        const secs = String(date.getUTCSeconds()).padStart(2, '0');
        const ms = String(date.getUTCMilliseconds()).padStart(3, '0');
        return `${secs}.${ms}`;
      }
      case 'H': return String(date.getUTCHours()).padStart(2, '0');
      case 'j': {
        const start = new Date(Date.UTC(date.getUTCFullYear(), 0, 0));
        const diff = date.getTime() - start.getTime();
        const dayOfYear = Math.floor(diff / (1000 * 60 * 60 * 24));
        return String(dayOfYear).padStart(3, '0');
      }
      case 'J': return String(calculateJulianDay(date));
      case 'm': return String(date.getUTCMonth() + 1).padStart(2, '0');
      case 'M': return String(date.getUTCMinutes()).padStart(2, '0');
      case 's': return String(Math.floor(date.getTime() / 1000));
      case 'S': return String(date.getUTCSeconds()).padStart(2, '0');
      case 'w': return String(date.getUTCDay());
      case 'W': {
        // Week of year (ISO style, Monday is first day)
        const start = new Date(Date.UTC(date.getUTCFullYear(), 0, 1));
        const diff = date.getTime() - start.getTime();
        const dayOfYear = Math.floor(diff / (1000 * 60 * 60 * 24));
        const week = Math.floor((dayOfYear + start.getUTCDay()) / 7);
        return String(week).padStart(2, '0');
      }
      case 'Y': return String(date.getUTCFullYear()).padStart(4, '0');
      case '%': return '%';
      default: return match;
    }
  });
}

/**
 * Calculate the Julian day number for a Date.
 * The Julian day is the continuous count of days since the beginning
 * of the Julian Period on January 1, 4713 BC (proleptic Julian calendar).
 */
function calculateJulianDay(date: Date): number {
  // Convert to Julian day
  // Formula from astronomical algorithms
  const year = date.getUTCFullYear();
  const month = date.getUTCMonth() + 1;
  const day = date.getUTCDate();
  const hour = date.getUTCHours();
  const minute = date.getUTCMinutes();
  const second = date.getUTCSeconds();
  const ms = date.getUTCMilliseconds();
  
  // Adjust for months January and February
  const a = Math.floor((14 - month) / 12);
  const y = year + 4800 - a;
  const m = month + 12 * a - 3;
  
  // Julian day number
  let jd = day + Math.floor((153 * m + 2) / 5) + 365 * y + Math.floor(y / 4);
  
  // Adjust for Gregorian calendar
  jd = jd - Math.floor(y / 100) + Math.floor(y / 400) - 32045;
  
  // Add fractional day for time
  const fracDay = (hour - 12) / 24 + minute / 1440 + second / 86400 + ms / 86400000;
  
  return jd + fracDay;
}

/**
 * Convert a Julian day number back to a Date object.
 */
function julianDayToDate(jd: number): Date {
  // Algorithm from astronomical algorithms
  const z = Math.floor(jd + 0.5);
  const f = (jd + 0.5) - z;
  
  let a: number;
  if (z < 2299161) {
    a = z;
  } else {
    const alpha = Math.floor((z - 1867216.25) / 36524.25);
    a = z + 1 + alpha - Math.floor(alpha / 4);
  }
  
  const b = a + 1524;
  const c = Math.floor((b - 122.1) / 365.25);
  const d = Math.floor(365.25 * c);
  const e = Math.floor((b - d) / 30.6001);
  
  const day = b - d - Math.floor(30.6001 * e);
  const month = e < 14 ? e - 1 : e - 13;
  const year = month > 2 ? c - 4716 : c - 4715;
  
  // Extract time from fractional day
  const totalSeconds = f * 86400 + 12 * 3600; // Add 12 hours (Julian day starts at noon)
  const hours = Math.floor(totalSeconds / 3600) % 24;
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = Math.floor(totalSeconds % 60);
  const ms = Math.round((totalSeconds - Math.floor(totalSeconds)) * 1000);
  
  return new Date(Date.UTC(year, month - 1, day, hours, minutes, seconds, ms));
}


