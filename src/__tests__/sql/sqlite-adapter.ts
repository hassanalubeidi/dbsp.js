/**
 * SQLLogicTest Adapter
 * ====================
 * 
 * This module provides an adapter to run SQLLogicTest test cases against the DBSP SQL compiler.
 * SQLLogicTest is a widely-used SQL testing framework created by SQLite that tests
 * SQL correctness by comparing query results against expected outputs.
 * 
 * Test Format:
 * - statement ok/error: Execute a statement (CREATE TABLE, INSERT, etc.)
 * - query <type> <label>: Execute a query and compare results
 * - hash-threshold <n>: Hash results if more than n values
 * 
 * @see https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki
 */

import { SQLCompiler } from '../../sql/sql-compiler';
import { ZSet } from '../../internals/zset';

export interface SQLLogicTestCase {
  type: 'statement' | 'query';
  sql: string;
  expected: 'ok' | 'error' | string[];
  resultType?: string; // I, R, T for integers, reals, text
  sortMode?: 'nosort' | 'rowsort' | 'valuesort';
  label?: string;
}

export interface SQLLogicTestResult {
  passed: boolean;
  actual: string[] | null;
  expected: string[] | 'ok' | 'error';
  error?: string;
}

/**
 * Parse a SQLLogicTest file content into test cases
 */
export function parseSQLLogicTest(content: string): SQLLogicTestCase[] {
  const tests: SQLLogicTestCase[] = [];
  const lines = content.split('\n');
  let i = 0;
  
  while (i < lines.length) {
    const line = lines[i].trim();
    
    // Skip empty lines and comments
    if (!line || line.startsWith('#')) {
      i++;
      continue;
    }
    
    // Parse 'statement ok' or 'statement error'
    if (line.startsWith('statement ')) {
      const expected = line.includes('error') ? 'error' : 'ok';
      i++;
      
      // Collect SQL until empty line or next directive
      let sql = '';
      while (i < lines.length && lines[i].trim() && !lines[i].startsWith('statement ') && !lines[i].startsWith('query ')) {
        sql += lines[i] + '\n';
        i++;
      }
      
      tests.push({
        type: 'statement',
        sql: sql.trim(),
        expected,
      });
      continue;
    }
    
    // Parse 'query <type> <label>' or 'query <type> <sortmode>'
    if (line.startsWith('query ')) {
      const parts = line.split(/\s+/);
      const resultType = parts[1] || 'T';
      const sortMode = parts[2] as 'nosort' | 'rowsort' | 'valuesort' | undefined;
      const label = parts[3];
      i++;
      
      // Collect SQL until ----
      let sql = '';
      while (i < lines.length && lines[i].trim() !== '----') {
        sql += lines[i] + '\n';
        i++;
      }
      i++; // Skip ----
      
      // Collect expected results until empty line
      const expected: string[] = [];
      while (i < lines.length && lines[i].trim()) {
        expected.push(lines[i].trim());
        i++;
      }
      
      tests.push({
        type: 'query',
        sql: sql.trim(),
        expected,
        resultType,
        sortMode,
        label,
      });
      continue;
    }
    
    // Skip unknown directives
    i++;
  }
  
  return tests;
}

/**
 * Run a single SQLLogicTest case against the DBSP compiler
 */
export function runSQLLogicTest(test: SQLLogicTestCase, existingTables: Map<string, any[]>): SQLLogicTestResult {
  if (test.type === 'statement') {
    return runStatement(test, existingTables);
  } else {
    return runQuery(test, existingTables);
  }
}

function runStatement(test: SQLLogicTestCase, existingTables: Map<string, any[]>): SQLLogicTestResult {
  try {
    // Handle INSERT statements by parsing and storing data
    if (test.sql.toUpperCase().startsWith('INSERT ')) {
      const parsed = parseInsert(test.sql);
      if (parsed) {
        const tableData = existingTables.get(parsed.table) || [];
        tableData.push(parsed.values);
        existingTables.set(parsed.table, tableData);
      }
      return { passed: test.expected === 'ok', actual: null, expected: test.expected };
    }
    
    // Handle CREATE TABLE by just noting the table exists
    if (test.sql.toUpperCase().startsWith('CREATE TABLE')) {
      const match = test.sql.match(/CREATE TABLE\s+(\w+)/i);
      if (match) {
        existingTables.set(match[1], []);
      }
      return { passed: test.expected === 'ok', actual: null, expected: test.expected };
    }
    
    // For other statements, try to compile
    const compiler = new SQLCompiler();
    compiler.compile(test.sql);
    return { passed: test.expected === 'ok', actual: null, expected: test.expected };
  } catch (e) {
    return { 
      passed: test.expected === 'error', 
      actual: null, 
      expected: test.expected,
      error: (e as Error).message 
    };
  }
}

function runQuery(test: SQLLogicTestCase, existingTables: Map<string, any[]>): SQLLogicTestResult {
  try {
    // Build SQL with table creation and view
    let fullSQL = '';
    
    // Create tables
    for (const [tableName, _] of existingTables) {
      // Infer schema from data
      const tableData = existingTables.get(tableName);
      if (tableData && tableData.length > 0) {
        const firstRow = tableData[0];
        const columns = Object.keys(firstRow).map(col => {
          const val = firstRow[col];
          const type = typeof val === 'number' 
            ? (Number.isInteger(val) ? 'INT' : 'DECIMAL')
            : 'VARCHAR';
          return `${col} ${type}`;
        }).join(', ');
        fullSQL += `CREATE TABLE ${tableName} (${columns});\n`;
      }
    }
    
    // Add query as a view
    fullSQL += `CREATE VIEW result AS ${test.sql};`;
    
    const compiler = new SQLCompiler();
    const result = compiler.compile(fullSQL);
    
    // Collect results
    const queryResults: string[] = [];
    const view = result.views.result;
    if (view) {
      // Subscribe to output
      view.output((zset: ZSet<any>) => {
        for (const [row, weight] of zset.entries()) {
          if (weight > 0) {
            // Flatten row values to array
            const values = Object.values(row).map(v => String(v ?? 'NULL'));
            queryResults.push(...values);
          }
        }
      });
      
      // Step the circuit with data
      const inputMap = new Map<string, ZSet<any>>();
      for (const [tableName, tableData] of existingTables) {
        if (tableData.length > 0) {
          inputMap.set(tableName, ZSet.fromValues(tableData));
        }
      }
      result.circuit.step(inputMap);
    }
    
    // Sort results if required
    let sortedResults = queryResults;
    if (test.sortMode === 'rowsort' || test.sortMode === 'valuesort') {
      sortedResults = [...queryResults].sort();
    }
    
    // Compare with expected
    const expectedArr = test.expected as string[];
    let sortedExpected = expectedArr;
    if (test.sortMode === 'rowsort' || test.sortMode === 'valuesort') {
      sortedExpected = [...expectedArr].sort();
    }
    
    const passed = arraysEqual(sortedResults, sortedExpected);
    
    return {
      passed,
      actual: sortedResults,
      expected: test.expected,
    };
  } catch (e) {
    return {
      passed: false,
      actual: null,
      expected: test.expected,
      error: (e as Error).message,
    };
  }
}

function parseInsert(sql: string): { table: string; values: Record<string, any> } | null {
  // Simple INSERT parser: INSERT INTO table (cols) VALUES (vals)
  const match = sql.match(/INSERT INTO\s+(\w+)\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)/i);
  if (!match) return null;
  
  const table = match[1];
  const cols = match[2].split(',').map(c => c.trim());
  const vals = match[3].split(',').map(v => {
    const trimmed = v.trim();
    // Parse value: number, string, or null
    if (trimmed.toUpperCase() === 'NULL') return null;
    if (/^-?\d+$/.test(trimmed)) return parseInt(trimmed, 10);
    if (/^-?\d+\.\d+$/.test(trimmed)) return parseFloat(trimmed);
    // Remove quotes
    return trimmed.replace(/^['"]|['"]$/g, '');
  });
  
  const values: Record<string, any> = {};
  for (let i = 0; i < cols.length; i++) {
    values[cols[i]] = vals[i];
  }
  
  return { table, values };
}

function arraysEqual(a: string[], b: string[]): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

/**
 * Run all tests from a SQLLogicTest file content
 */
export function runAllTests(content: string): { 
  total: number; 
  passed: number; 
  failed: number; 
  results: SQLLogicTestResult[] 
} {
  const tests = parseSQLLogicTest(content);
  const existingTables = new Map<string, any[]>();
  const results: SQLLogicTestResult[] = [];
  let passed = 0;
  let failed = 0;
  
  for (const test of tests) {
    const result = runSQLLogicTest(test, existingTables);
    results.push(result);
    if (result.passed) {
      passed++;
    } else {
      failed++;
    }
  }
  
  return { total: tests.length, passed, failed, results };
}

