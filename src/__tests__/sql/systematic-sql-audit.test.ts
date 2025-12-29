/**
 * Systematic SQL Compliance Audit
 * Organized by SQL Standard Categories
 */

import { describe, it, expect } from 'vitest';
import { SQLCompiler, SQLParser } from '../../sql/sql-compiler';
import { ZSet } from '../../internals/zset';

// Helper to compile SQL using the SQLCompiler
function compileSQL(sql: string) {
  const compiler = new SQLCompiler();
  return compiler.compile(sql);
}

// Helper to execute query and get results
function executeQuery(sql: string, data: Record<string, any[]>): any[] {
  const { circuit, tables, views } = compileSQL(sql);
  const results: any[] = [];
  
  // Get the first (and usually only) view
  const viewName = Object.keys(views)[0];
  if (viewName) {
    views[viewName].output((zset: ZSet<any>) => {
      for (const [row, weight] of zset.entries()) {
        if (weight > 0) results.push(row);
      }
    });
  }

  // Combine all table data into a single ZSet for input
  const inputMap = new Map<string, ZSet<any>>();
  for (const [tableName, rows] of Object.entries(data)) {
    if (tables[tableName]) {
      inputMap.set(tableName, ZSet.fromValues(rows));
    }
  }
  circuit.step(inputMap);

  return results;
}

// ============================================================================
// CATEGORY 1: COMPARISON OPERATORS
// ============================================================================
describe('Systematic: Comparison Operators', () => {
  const numData = { t: [{ id: 1, a: 10, b: 5 }, { id: 2, a: 5, b: 5 }, { id: 3, a: 3, b: 5 }] };
  
  it('= equal (column to column)', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, a INT, b INT); CREATE VIEW query AS SELECT * FROM t WHERE a = b;`, numData);
    expect(r.length).toBe(1);
  });
  
  it('<> not equal (column to column)', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, a INT, b INT); CREATE VIEW query AS SELECT * FROM t WHERE a <> b;`, numData);
    expect(r.length).toBe(2);
  });
  
  it('!= not equal alt (column to column)', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, a INT, b INT); CREATE VIEW query AS SELECT * FROM t WHERE a != b;`, numData);
    expect(r.length).toBe(2);
  });
  
  it('< less than (column to column)', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, a INT, b INT); CREATE VIEW query AS SELECT * FROM t WHERE a < b;`, numData);
    expect(r.length).toBe(1);
  });
  
  it('<= less or equal (column to column)', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, a INT, b INT); CREATE VIEW query AS SELECT * FROM t WHERE a <= b;`, numData);
    expect(r.length).toBe(2);
  });
  
  it('> greater than (column to column)', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, a INT, b INT); CREATE VIEW query AS SELECT * FROM t WHERE a > b;`, numData);
    expect(r.length).toBe(1);
  });
  
  it('>= greater or equal (column to column)', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, a INT, b INT); CREATE VIEW query AS SELECT * FROM t WHERE a >= b;`, numData);
    expect(r.length).toBe(2);
  });
});

// ============================================================================
// CATEGORY 2: LOGICAL OPERATORS
// ============================================================================
describe('Systematic: Logical Operators', () => {
  const data = { t: [
    { id: 1, a: 1, b: 1 },
    { id: 2, a: 1, b: 0 },
    { id: 3, a: 0, b: 1 },
    { id: 4, a: 0, b: 0 }
  ]};
  
  it('AND', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, a INT, b INT); CREATE VIEW query AS SELECT * FROM t WHERE a = 1 AND b = 1;`, data);
    expect(r.length).toBe(1);
  });
  
  it('OR', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, a INT, b INT); CREATE VIEW query AS SELECT * FROM t WHERE a = 1 OR b = 1;`, data);
    expect(r.length).toBe(3);
  });
  
  it('NOT with comparison', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, a INT, b INT); CREATE VIEW query AS SELECT * FROM t WHERE NOT a = 1;`, data);
    expect(r.length).toBe(2);
  });
  
  it('NOT with AND (parenthesized expr)', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, a INT, b INT); CREATE VIEW query AS SELECT * FROM t WHERE NOT (a = 1 AND b = 1);`, data);
    expect(r.length).toBe(3);
  });
  
  it('Complex: (A AND B) OR (C AND D)', () => {
    const data2 = { t: [
      { id: 1, a: 1, b: 1, c: 0, d: 0 },
      { id: 2, a: 0, b: 0, c: 1, d: 1 },
      { id: 3, a: 0, b: 0, c: 0, d: 0 }
    ]};
    const r = executeQuery(`CREATE TABLE t (id INT, a INT, b INT, c INT, d INT); CREATE VIEW query AS SELECT * FROM t WHERE (a = 1 AND b = 1) OR (c = 1 AND d = 1);`, data2);
    expect(r.length).toBe(2);
  });
});

// ============================================================================
// CATEGORY 3: ARITHMETIC OPERATORS
// ============================================================================
describe('Systematic: Arithmetic Operators', () => {
  const data = { t: [{ id: 1, a: 10, b: 3 }] };
  
  it('+ addition', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, a INT, b INT); CREATE VIEW query AS SELECT id, a + b AS result FROM t;`, data);
    expect(r[0]?.result).toBe(13);
  });
  
  it('- subtraction', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, a INT, b INT); CREATE VIEW query AS SELECT id, a - b AS result FROM t;`, data);
    expect(r[0]?.result).toBe(7);
  });
  
  it('* multiplication', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, a INT, b INT); CREATE VIEW query AS SELECT id, a * b AS result FROM t;`, data);
    expect(r[0]?.result).toBe(30);
  });
  
  it('/ division', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, a INT, b INT); CREATE VIEW query AS SELECT id, a / b AS result FROM t;`, data);
    expect(r[0]?.result).toBeCloseTo(3.33, 1);
  });
  
  it('% modulo', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, a INT, b INT); CREATE VIEW query AS SELECT id, a % b AS result FROM t;`, data);
    expect(r[0]?.result).toBe(1);
  });
  
  it('unary - (negation)', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, a INT, b INT); CREATE VIEW query AS SELECT id, -a AS result FROM t;`, data);
    expect(r[0]?.result).toBe(-10);
  });
  
  it('unary + (positive)', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, a INT, b INT); CREATE VIEW query AS SELECT id, +a AS result FROM t;`, data);
    expect(r[0]?.result).toBe(10);
  });
  
  it('parentheses for precedence', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, a INT, b INT); CREATE VIEW query AS SELECT id, (a + b) * 2 AS result FROM t;`, data);
    expect(r[0]?.result).toBe(26);
  });
  
  it('expression in WHERE (a * b > value)', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, a INT, b INT); CREATE VIEW query AS SELECT * FROM t WHERE a * b > 30;`, data);
    // a=10, b=3 -> 30, not > 30, so only id=1 (10*3=30) would need a=10,b=4 or similar
    // Actually with a=10, b=3 -> 30 which is NOT > 30
    expect(r.length).toBe(0); // 10*3=30 is not > 30
  });
  
  it('expression in WHERE (price * qty > 50)', () => {
    const data2 = { t: [
      { id: 1, price: 10, qty: 5 },   // 50 - not > 50
      { id: 2, price: 20, qty: 3 },   // 60 - > 50
      { id: 3, price: 5, qty: 8 },    // 40 - not > 50
    ]};
    const r = executeQuery(`CREATE TABLE t (id INT, price INT, qty INT); CREATE VIEW query AS SELECT * FROM t WHERE price * qty > 50;`, data2);
    expect(r.length).toBe(1);
    expect(r[0]?.id).toBe(2);
  });
});

// ============================================================================
// CATEGORY 4: PREDICATES
// ============================================================================
describe('Systematic: Predicates', () => {
  it('BETWEEN with integers', () => {
    const data = { t: [{ id: 1, v: 5 }, { id: 2, v: 10 }, { id: 3, v: 15 }] };
    const r = executeQuery(`CREATE TABLE t (id INT, v INT); CREATE VIEW query AS SELECT * FROM t WHERE v BETWEEN 5 AND 10;`, data);
    expect(r.length).toBe(2);
  });
  
  it('NOT BETWEEN', () => {
    const data = { t: [{ id: 1, v: 5 }, { id: 2, v: 10 }, { id: 3, v: 15 }] };
    const r = executeQuery(`CREATE TABLE t (id INT, v INT); CREATE VIEW query AS SELECT * FROM t WHERE v NOT BETWEEN 5 AND 10;`, data);
    expect(r.length).toBe(1);
  });
  
  it('IN with numbers', () => {
    const data = { t: [{ id: 1, v: 1 }, { id: 2, v: 2 }, { id: 3, v: 3 }] };
    const r = executeQuery(`CREATE TABLE t (id INT, v INT); CREATE VIEW query AS SELECT * FROM t WHERE v IN (1, 3);`, data);
    expect(r.length).toBe(2);
  });
  
  it('NOT IN with numbers', () => {
    const data = { t: [{ id: 1, v: 1 }, { id: 2, v: 2 }, { id: 3, v: 3 }] };
    const r = executeQuery(`CREATE TABLE t (id INT, v INT); CREATE VIEW query AS SELECT * FROM t WHERE v NOT IN (1, 3);`, data);
    expect(r.length).toBe(1);
  });
  
  it('IN with strings', () => {
    const data = { t: [{ id: 1, s: 'a' }, { id: 2, s: 'b' }, { id: 3, s: 'c' }] };
    const r = executeQuery(`CREATE TABLE t (id INT, s VARCHAR); CREATE VIEW query AS SELECT * FROM t WHERE s IN ('a', 'c');`, data);
    expect(r.length).toBe(2);
  });
  
  it('LIKE with %', () => {
    const data = { t: [{ id: 1, s: 'apple' }, { id: 2, s: 'banana' }, { id: 3, s: 'apricot' }] };
    const r = executeQuery(`CREATE TABLE t (id INT, s VARCHAR); CREATE VIEW query AS SELECT * FROM t WHERE s LIKE 'ap%';`, data);
    expect(r.length).toBe(2);
  });
  
  it('LIKE with _', () => {
    const data = { t: [{ id: 1, s: 'cat' }, { id: 2, s: 'cut' }, { id: 3, s: 'cart' }] };
    const r = executeQuery(`CREATE TABLE t (id INT, s VARCHAR); CREATE VIEW query AS SELECT * FROM t WHERE s LIKE 'c_t';`, data);
    expect(r.length).toBe(2);
  });
  
  it('NOT LIKE', () => {
    const data = { t: [{ id: 1, s: 'apple' }, { id: 2, s: 'banana' }] };
    const r = executeQuery(`CREATE TABLE t (id INT, s VARCHAR); CREATE VIEW query AS SELECT * FROM t WHERE s NOT LIKE 'a%';`, data);
    expect(r.length).toBe(1);
  });
  
  it('IS NULL', () => {
    const data = { t: [{ id: 1, v: 10 }, { id: 2, v: null }] };
    const r = executeQuery(`CREATE TABLE t (id INT, v INT); CREATE VIEW query AS SELECT * FROM t WHERE v IS NULL;`, data);
    expect(r.length).toBe(1);
  });
  
  it('IS NOT NULL', () => {
    const data = { t: [{ id: 1, v: 10 }, { id: 2, v: null }] };
    const r = executeQuery(`CREATE TABLE t (id INT, v INT); CREATE VIEW query AS SELECT * FROM t WHERE v IS NOT NULL;`, data);
    expect(r.length).toBe(1);
  });
});

// ============================================================================
// CATEGORY 5: AGGREGATE FUNCTIONS
// ============================================================================
describe('Systematic: Aggregate Functions', () => {
  const data = { t: [
    { id: 1, g: 'A', v: 10 },
    { id: 2, g: 'A', v: 20 },
    { id: 3, g: 'B', v: 30 },
    { id: 4, g: 'B', v: 40 },
    { id: 5, g: 'B', v: null }
  ]};
  
  it('COUNT(*)', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, g VARCHAR, v INT); CREATE VIEW query AS SELECT COUNT(*) AS cnt FROM t;`, data);
    expect(r[0]?.cnt).toBe(5);
  });
  
  it('COUNT(column) ignores NULL', () => {
    // COUNT(column) should ignore NULL values - only 4 non-null values
    const r = executeQuery(`CREATE TABLE t (id INT, g VARCHAR, v INT); CREATE VIEW query AS SELECT COUNT(v) AS cnt FROM t;`, data);
    expect(r[0]?.cnt).toBe(4);
  });
  
  it('COUNT(DISTINCT column)', () => {
    const data2 = { t: [{ id: 1, v: 'A' }, { id: 2, v: 'A' }, { id: 3, v: 'B' }] };
    const r = executeQuery(`CREATE TABLE t (id INT, v VARCHAR); CREATE VIEW query AS SELECT g, COUNT(DISTINCT v) AS cnt FROM t GROUP BY g;`, 
      { t: [{ id: 1, g: 'X', v: 'A' }, { id: 2, g: 'X', v: 'A' }, { id: 3, g: 'X', v: 'B' }] });
    console.log('COUNT(DISTINCT) with GROUP BY:', r);
    expect(r[0]?.cnt).toBe(2);
  });
  
  it('SUM', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, g VARCHAR, v INT); CREATE VIEW query AS SELECT SUM(v) AS total FROM t;`, data);
    expect(r[0]?.total).toBe(100);
  });
  
  it('AVG ignores NULL', () => {
    // AVG should ignore NULL values - (10+20+30+40)/4 = 25
    const r = executeQuery(`CREATE TABLE t (id INT, g VARCHAR, v INT); CREATE VIEW query AS SELECT AVG(v) AS avg FROM t;`, data);
    expect(r[0]?.avg).toBe(25);
  });
  
  it('MIN ignores NULL', () => {
    // MIN should ignore NULL values - min of 10,20,30,40 = 10
    const r = executeQuery(`CREATE TABLE t (id INT, g VARCHAR, v INT); CREATE VIEW query AS SELECT MIN(v) AS min FROM t;`, data);
    expect(r[0]?.min).toBe(10);
  });
  
  it('MAX', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, g VARCHAR, v INT); CREATE VIEW query AS SELECT MAX(v) AS max FROM t;`, data);
    expect(r[0]?.max).toBe(40);
  });
  
  it('GROUP BY single column', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, g VARCHAR, v INT); CREATE VIEW query AS SELECT g, SUM(v) AS total FROM t GROUP BY g;`, data);
    expect(r.length).toBe(2);
    expect(r.find(x => x.g === 'A')?.total).toBe(30);
    expect(r.find(x => x.g === 'B')?.total).toBe(70);
  });
  
  it('GROUP BY multiple columns', () => {
    const data2 = { t: [
      { id: 1, g1: 'A', g2: 'X', v: 10 },
      { id: 2, g1: 'A', g2: 'Y', v: 20 },
      { id: 3, g1: 'B', g2: 'X', v: 30 }
    ]};
    const r = executeQuery(`CREATE TABLE t (id INT, g1 VARCHAR, g2 VARCHAR, v INT); CREATE VIEW query AS SELECT g1, g2, SUM(v) AS total FROM t GROUP BY g1, g2;`, data2);
    expect(r.length).toBe(3);
  });
  
  it('HAVING', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, g VARCHAR, v INT); CREATE VIEW query AS SELECT g, SUM(v) AS total FROM t GROUP BY g HAVING SUM(v) > 50;`, data);
    expect(r.length).toBe(1);
    expect(r[0]?.g).toBe('B');
  });
});

// ============================================================================
// CATEGORY 6: STRING FUNCTIONS
// ============================================================================
describe('Systematic: String Functions', () => {
  const data = { t: [{ id: 1, s: 'Hello World' }] };
  
  it('UPPER', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, s VARCHAR); CREATE VIEW query AS SELECT id, UPPER(s) AS result FROM t;`, data);
    expect(r[0]?.result).toBe('HELLO WORLD');
  });
  
  it('LOWER', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, s VARCHAR); CREATE VIEW query AS SELECT id, LOWER(s) AS result FROM t;`, data);
    expect(r[0]?.result).toBe('hello world');
  });
  
  it('LENGTH', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, s VARCHAR); CREATE VIEW query AS SELECT id, LENGTH(s) AS result FROM t;`, data);
    expect(r[0]?.result).toBe(11);
  });
  
  it('TRIM', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, s VARCHAR); CREATE VIEW query AS SELECT id, TRIM(s) AS result FROM t;`, { t: [{ id: 1, s: '  test  ' }] });
    expect(r[0]?.result).toBe('test');
  });
  
  it('LTRIM', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, s VARCHAR); CREATE VIEW query AS SELECT id, LTRIM(s) AS result FROM t;`, { t: [{ id: 1, s: '  test' }] });
    expect(r[0]?.result).toBe('test');
  });
  
  it('RTRIM', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, s VARCHAR); CREATE VIEW query AS SELECT id, RTRIM(s) AS result FROM t;`, { t: [{ id: 1, s: 'test  ' }] });
    expect(r[0]?.result).toBe('test');
  });
  
  it('CONCAT', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, s VARCHAR); CREATE VIEW query AS SELECT id, CONCAT(s, '!') AS result FROM t;`, data);
    console.log('CONCAT result:', r);
    expect(r[0]?.result).toBe('Hello World!');
  });
  
  it('SUBSTR/SUBSTRING', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, s VARCHAR); CREATE VIEW query AS SELECT id, SUBSTR(s, 1, 5) AS result FROM t;`, data);
    console.log('SUBSTR result:', r);
    expect(r[0]?.result).toBe('Hello');
  });
  
  it('REPLACE', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, s VARCHAR); CREATE VIEW query AS SELECT id, REPLACE(s, 'World', 'Universe') AS result FROM t;`, data);
    console.log('REPLACE result:', r);
    expect(r[0]?.result).toBe('Hello Universe');
  });
  
  it('LEFT', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, s VARCHAR); CREATE VIEW query AS SELECT id, LEFT(s, 5) AS result FROM t;`, data);
    console.log('LEFT result:', r);
    expect(r[0]?.result).toBe('Hello');
  });
  
  it('RIGHT', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, s VARCHAR); CREATE VIEW query AS SELECT id, RIGHT(s, 5) AS result FROM t;`, data);
    console.log('RIGHT result:', r);
    expect(r[0]?.result).toBe('World');
  });
});

// ============================================================================
// CATEGORY 7: NUMERIC FUNCTIONS
// ============================================================================
describe('Systematic: Numeric Functions', () => {
  it('ABS', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, v INT); CREATE VIEW query AS SELECT id, ABS(v) AS result FROM t;`, { t: [{ id: 1, v: -10 }] });
    expect(r[0]?.result).toBe(10);
  });
  
  it('ROUND', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, v DECIMAL); CREATE VIEW query AS SELECT id, ROUND(v, 2) AS result FROM t;`, { t: [{ id: 1, v: 3.14159 }] });
    console.log('ROUND result:', r);
    expect(r[0]?.result).toBeCloseTo(3.14, 2);
  });
  
  it('FLOOR', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, v DECIMAL); CREATE VIEW query AS SELECT id, FLOOR(v) AS result FROM t;`, { t: [{ id: 1, v: 3.7 }] });
    expect(r[0]?.result).toBe(3);
  });
  
  it('CEIL/CEILING', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, v DECIMAL); CREATE VIEW query AS SELECT id, CEIL(v) AS result FROM t;`, { t: [{ id: 1, v: 3.2 }] });
    expect(r[0]?.result).toBe(4);
  });
  
  it('SIGN', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, v INT); CREATE VIEW query AS SELECT id, SIGN(v) AS result FROM t;`, { t: [{ id: 1, v: -10 }] });
    console.log('SIGN result:', r);
    expect(r[0]?.result).toBe(-1);
  });
  
  it('POWER', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, v INT); CREATE VIEW query AS SELECT id, POWER(v, 2) AS result FROM t;`, { t: [{ id: 1, v: 3 }] });
    console.log('POWER result:', r);
    expect(r[0]?.result).toBe(9);
  });
  
  it('SQRT', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, v INT); CREATE VIEW query AS SELECT id, SQRT(v) AS result FROM t;`, { t: [{ id: 1, v: 16 }] });
    console.log('SQRT result:', r);
    expect(r[0]?.result).toBe(4);
  });
  
  it('MOD function', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, a INT, b INT); CREATE VIEW query AS SELECT id, MOD(a, b) AS result FROM t;`, { t: [{ id: 1, a: 10, b: 3 }] });
    console.log('MOD result:', r);
    expect(r[0]?.result).toBe(1);
  });
});

// ============================================================================
// CATEGORY 8: NULL HANDLING FUNCTIONS
// ============================================================================
describe('Systematic: NULL Handling', () => {
  it('COALESCE with 2 args', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, v INT); CREATE VIEW query AS SELECT id, COALESCE(v, 0) AS result FROM t;`, { t: [{ id: 1, v: null }] });
    expect(r[0]?.result).toBe(0);
  });
  
  it('COALESCE with 3 args', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, a INT, b INT, c INT); CREATE VIEW query AS SELECT id, COALESCE(a, b, c) AS result FROM t;`, { t: [{ id: 1, a: null, b: null, c: 5 }] });
    expect(r[0]?.result).toBe(5);
  });
  
  it('NULLIF', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, v INT); CREATE VIEW query AS SELECT id, NULLIF(v, 0) AS result FROM t;`, { t: [{ id: 1, v: 0 }, { id: 2, v: 5 }] });
    console.log('NULLIF result:', r);
    expect(r.find(x => x.id === 1)?.result).toBeNull();
    expect(r.find(x => x.id === 2)?.result).toBe(5);
  });
  
  it('IFNULL / NVL equivalent', () => {
    // Using COALESCE as IFNULL equivalent
    const r = executeQuery(`CREATE TABLE t (id INT, v INT); CREATE VIEW query AS SELECT id, COALESCE(v, -1) AS result FROM t;`, { t: [{ id: 1, v: null }] });
    expect(r[0]?.result).toBe(-1);
  });
});

// ============================================================================
// CATEGORY 9: CASE EXPRESSIONS
// ============================================================================
describe('Systematic: CASE Expressions', () => {
  it('Simple CASE with aggregate', () => {
    const data = { t: [{ id: 1, v: 10 }, { id: 2, v: 20 }] };
    const r = executeQuery(`CREATE TABLE t (id INT, v INT); CREATE VIEW query AS SELECT CASE WHEN SUM(v) > 25 THEN 'high' ELSE 'low' END AS level FROM t;`, data);
    console.log('CASE with aggregate:', r);
    expect(r[0]?.level).toBe('high');
  });
  
  it('CASE in aggregation context', () => {
    const data = { t: [{ id: 1, s: 'A', v: 10 }, { id: 2, s: 'B', v: 20 }, { id: 3, s: 'A', v: 5 }] };
    const r = executeQuery(`CREATE TABLE t (id INT, s VARCHAR, v INT); CREATE VIEW query AS SELECT s, SUM(CASE WHEN v > 10 THEN 1 ELSE 0 END) AS high_count FROM t GROUP BY s;`, data);
    console.log('CASE in SUM:', r);
    expect(r.find(x => x.s === 'A')?.high_count).toBe(0);
    expect(r.find(x => x.s === 'B')?.high_count).toBe(1);
  });
  
  it('Multiple WHEN clauses', () => {
    const data = { t: [{ id: 1, v: 90 }, { id: 2, v: 75 }, { id: 3, v: 50 }] };
    const r = executeQuery(`CREATE TABLE t (id INT, v INT); CREATE VIEW query AS 
      SELECT id, v, SUM(1) AS cnt,
        CASE WHEN MAX(v) >= 90 THEN 'A' 
             WHEN MAX(v) >= 70 THEN 'B'
             ELSE 'C' END AS grade 
      FROM t GROUP BY id, v;`, data);
    console.log('Multiple WHEN:', r);
    expect(r.length).toBe(3);
  });
  
  it('Simple CASE without aggregation', () => {
    const data = { t: [{ id: 1, v: 1 }, { id: 2, v: 2 }] };
    const r = executeQuery(`CREATE TABLE t (id INT, v INT); CREATE VIEW query AS 
      SELECT id, CASE v WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END AS word FROM t;`, data);
    console.log('Simple CASE (no agg):', r);
  });
});

// ============================================================================
// CATEGORY 10: JOIN TYPES
// ============================================================================
describe('Systematic: JOIN Types', () => {
  const left = { a: [{ id: 1, v: 'X' }, { id: 2, v: 'Y' }] };
  const right = { b: [{ id: 1, w: 100 }, { id: 3, w: 300 }] };
  const both = { ...left, ...right };
  
  it('INNER JOIN', () => {
    const r = executeQuery(`CREATE TABLE a (id INT, v VARCHAR); CREATE TABLE b (id INT, w INT); CREATE VIEW query AS SELECT a.id, v, w FROM a INNER JOIN b ON a.id = b.id;`, both);
    expect(r.length).toBe(1);
    expect(r[0]?.id).toBe(1);
  });
  
  it('LEFT JOIN / LEFT OUTER JOIN', () => {
    const r = executeQuery(`CREATE TABLE a (id INT, v VARCHAR); CREATE TABLE b (id INT, w INT); CREATE VIEW query AS SELECT a.id, v, w FROM a LEFT JOIN b ON a.id = b.id;`, both);
    expect(r.length).toBe(2);
  });
  
  it('RIGHT JOIN', () => {
    const r = executeQuery(`CREATE TABLE a (id INT, v VARCHAR); CREATE TABLE b (id INT, w INT); CREATE VIEW query AS SELECT b.id, v, w FROM a RIGHT JOIN b ON a.id = b.id;`, both);
    console.log('RIGHT JOIN:', r);
  });
  
  it('FULL OUTER JOIN', () => {
    const r = executeQuery(`CREATE TABLE a (id INT, v VARCHAR); CREATE TABLE b (id INT, w INT); CREATE VIEW query AS SELECT a.id, b.id, v, w FROM a FULL OUTER JOIN b ON a.id = b.id;`, both);
    console.log('FULL OUTER JOIN:', r);
  });
  
  it('CROSS JOIN', () => {
    const r = executeQuery(`CREATE TABLE a (id INT, v VARCHAR); CREATE TABLE b (id INT, w INT); CREATE VIEW query AS SELECT a.id AS a_id, b.id AS b_id FROM a CROSS JOIN b;`, both);
    console.log('CROSS JOIN:', r);
    // Should be 2 * 2 = 4 rows
  });
  
  it('Self JOIN with table alias projection', () => {
    // Self join with alias.column in SELECT
    const data = { t: [{ id: 1, parent: null, name: 'Root' }, { id: 2, parent: 1, name: 'Child' }] };
    const r = executeQuery(`CREATE TABLE t (id INT, parent INT, name VARCHAR); CREATE VIEW query AS SELECT c.name AS child, p.name AS parent FROM t c JOIN t p ON c.parent = p.id;`, data);
    expect(r.length).toBe(1);
    expect(r[0]?.child).toBe('Child');
    expect(r[0]?.parent).toBe('Root');
  });
});

// ============================================================================
// CATEGORY 11: SUBQUERIES
// ============================================================================
describe('Systematic: Subqueries', () => {
  it('Scalar subquery in SELECT (with aggregation)', () => {
    const data = { t: [{ id: 1, v: 10 }, { id: 2, v: 20 }] };
    const r = executeQuery(`CREATE TABLE t (id INT, v INT); CREATE VIEW query AS 
      SELECT id, v, (SELECT SUM(v) FROM t) AS total FROM t GROUP BY id, v;`, data);
    console.log('Scalar subquery SELECT:', r);
  });
  
  it('IN (SELECT ...)', () => {
    const data = { a: [{ id: 1 }, { id: 2 }, { id: 3 }], b: [{ id: 1 }, { id: 3 }] };
    const r = executeQuery(`CREATE TABLE a (id INT); CREATE TABLE b (id INT); CREATE VIEW query AS SELECT * FROM a WHERE id IN (SELECT id FROM b);`, data);
    expect(r.length).toBe(2);
  });
  
  it('NOT IN (SELECT ...)', () => {
    const data = { a: [{ id: 1 }, { id: 2 }, { id: 3 }], b: [{ id: 1 }, { id: 3 }] };
    const r = executeQuery(`CREATE TABLE a (id INT); CREATE TABLE b (id INT); CREATE VIEW query AS SELECT * FROM a WHERE id NOT IN (SELECT id FROM b);`, data);
    console.log('NOT IN subquery:', r);
  });
  
  it('EXISTS (SELECT ...)', () => {
    const data = { a: [{ id: 1, ref: 10 }, { id: 2, ref: 20 }], b: [{ id: 10 }] };
    const r = executeQuery(`CREATE TABLE a (id INT, ref INT); CREATE TABLE b (id INT); CREATE VIEW query AS SELECT * FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.id = a.ref);`, data);
    expect(r.length).toBe(1);
  });
  
  it('NOT EXISTS', () => {
    const data = { a: [{ id: 1, ref: 10 }, { id: 2, ref: 20 }], b: [{ id: 10 }] };
    const r = executeQuery(`CREATE TABLE a (id INT, ref INT); CREATE TABLE b (id INT); CREATE VIEW query AS SELECT * FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.id = a.ref);`, data);
    console.log('NOT EXISTS:', r);
  });
  
  it('Derived table (subquery in FROM)', () => {
    const data = { t: [{ id: 1, g: 'A', v: 10 }, { id: 2, g: 'A', v: 20 }, { id: 3, g: 'B', v: 30 }] };
    const r = executeQuery(`CREATE TABLE t (id INT, g VARCHAR, v INT); CREATE VIEW query AS 
      SELECT sub.g, sub.total FROM (SELECT g, SUM(v) AS total FROM t GROUP BY g) AS sub WHERE sub.total > 25;`, data);
    console.log('Derived table:', r);
    expect(r.length).toBe(2);
  });
});

// ============================================================================
// CATEGORY 12: SET OPERATIONS
// ============================================================================
describe('Systematic: Set Operations', () => {
  it('UNION (removes duplicates)', () => {
    const data = { a: [{ v: 1 }, { v: 2 }], b: [{ v: 2 }, { v: 3 }] };
    const r = executeQuery(`CREATE TABLE a (v INT); CREATE TABLE b (v INT); CREATE VIEW query AS SELECT v FROM a UNION SELECT v FROM b;`, data);
    console.log('UNION:', r);
  });
  
  it('UNION ALL (keeps duplicates)', () => {
    const data = { a: [{ v: 1 }, { v: 2 }], b: [{ v: 2 }, { v: 3 }] };
    const r = executeQuery(`CREATE TABLE a (v INT); CREATE TABLE b (v INT); CREATE VIEW query AS SELECT v FROM a UNION ALL SELECT v FROM b;`, data);
    console.log('UNION ALL:', r);
  });
  
  it('INTERSECT', () => {
    const data = { a: [{ v: 1 }, { v: 2 }, { v: 3 }], b: [{ v: 2 }, { v: 3 }, { v: 4 }] };
    const r = executeQuery(`CREATE TABLE a (v INT); CREATE TABLE b (v INT); CREATE VIEW query AS SELECT v FROM a INTERSECT SELECT v FROM b;`, data);
    console.log('INTERSECT:', r);
  });
  
  it('EXCEPT', () => {
    const data = { a: [{ v: 1 }, { v: 2 }, { v: 3 }], b: [{ v: 2 }, { v: 3 }] };
    const r = executeQuery(`CREATE TABLE a (v INT); CREATE TABLE b (v INT); CREATE VIEW query AS SELECT v FROM a EXCEPT SELECT v FROM b;`, data);
    console.log('EXCEPT:', r);
  });
});

// ============================================================================
// CATEGORY 13: WINDOW FUNCTIONS
// ============================================================================
describe('Systematic: Window Functions', () => {
  const data = { t: [
    { id: 1, g: 'A', v: 10 },
    { id: 2, g: 'A', v: 20 },
    { id: 3, g: 'B', v: 30 },
    { id: 4, g: 'B', v: 40 }
  ]};
  
  it('ROW_NUMBER()', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, g VARCHAR, v INT); CREATE VIEW query AS SELECT id, ROW_NUMBER() OVER (ORDER BY v) AS rn FROM t;`, data);
    expect(r.length).toBe(4);
  });
  
  it('ROW_NUMBER() with PARTITION BY', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, g VARCHAR, v INT); CREATE VIEW query AS SELECT id, g, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) AS rn FROM t;`, data);
    expect(r.length).toBe(4);
  });
  
  it('RANK()', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, g VARCHAR, v INT); CREATE VIEW query AS SELECT id, RANK() OVER (ORDER BY v) AS rnk FROM t;`, data);
    expect(r.length).toBe(4);
  });
  
  it('DENSE_RANK()', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, g VARCHAR, v INT); CREATE VIEW query AS SELECT id, DENSE_RANK() OVER (ORDER BY v) AS drnk FROM t;`, data);
    expect(r.length).toBe(4);
  });
  
  it('LAG()', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, g VARCHAR, v INT); CREATE VIEW query AS SELECT id, LAG(v) OVER (ORDER BY id) AS prev FROM t;`, data);
    expect(r.length).toBe(4);
  });
  
  it('LEAD()', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, g VARCHAR, v INT); CREATE VIEW query AS SELECT id, LEAD(v) OVER (ORDER BY id) AS next FROM t;`, data);
    expect(r.length).toBe(4);
  });
  
  it('SUM() OVER', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, g VARCHAR, v INT); CREATE VIEW query AS SELECT id, SUM(v) OVER (PARTITION BY g) AS group_sum FROM t;`, data);
    expect(r.length).toBe(4);
  });
  
  it('AVG() OVER', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, g VARCHAR, v INT); CREATE VIEW query AS SELECT id, AVG(v) OVER (PARTITION BY g) AS group_avg FROM t;`, data);
    expect(r.length).toBe(4);
  });
  
  it('NTILE()', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, g VARCHAR, v INT); CREATE VIEW query AS SELECT id, NTILE(2) OVER (ORDER BY v) AS bucket FROM t;`, data);
    expect(r.length).toBe(4);
  });
  
  it('Rolling SUM with ROWS BETWEEN', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, g VARCHAR, v INT); CREATE VIEW query AS SELECT id, SUM(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS rolling FROM t;`, data);
    console.log('Rolling SUM:', r);
    expect(r.length).toBe(4);
  });
});

// ============================================================================
// CATEGORY 14: ORDER BY / LIMIT
// ============================================================================
describe('Systematic: ORDER BY / LIMIT', () => {
  const data = { t: [{ id: 3, v: 30 }, { id: 1, v: 10 }, { id: 2, v: 20 }] };
  
  it('ORDER BY ASC (default)', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, v INT); CREATE VIEW query AS SELECT * FROM t ORDER BY v;`, data);
    expect(r[0]?.v).toBe(10);
  });
  
  it('ORDER BY DESC', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, v INT); CREATE VIEW query AS SELECT * FROM t ORDER BY v DESC;`, data);
    expect(r[0]?.v).toBe(30);
  });
  
  it('ORDER BY multiple columns', () => {
    const data2 = { t: [{ id: 1, a: 1, b: 2 }, { id: 2, a: 1, b: 1 }, { id: 3, a: 2, b: 1 }] };
    const r = executeQuery(`CREATE TABLE t (id INT, a INT, b INT); CREATE VIEW query AS SELECT * FROM t ORDER BY a, b;`, data2);
    expect(r[0]?.id).toBe(2); // a=1, b=1 comes first
  });
  
  it('ORDER BY mixed directions', () => {
    const data2 = { t: [{ id: 1, a: 1, b: 2 }, { id: 2, a: 1, b: 1 }, { id: 3, a: 2, b: 1 }] };
    const r = executeQuery(`CREATE TABLE t (id INT, a INT, b INT); CREATE VIEW query AS SELECT * FROM t ORDER BY a ASC, b DESC;`, data2);
    expect(r[0]?.id).toBe(1); // a=1, b=2 (b DESC)
  });
  
  it('LIMIT', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, v INT); CREATE VIEW query AS SELECT * FROM t LIMIT 2;`, data);
    expect(r.length).toBe(2);
  });
  
  it('ORDER BY with LIMIT', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, v INT); CREATE VIEW query AS SELECT * FROM t ORDER BY v DESC LIMIT 1;`, data);
    expect(r.length).toBe(1);
    expect(r[0]?.v).toBe(30);
  });
  
  it('LIMIT 0', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, v INT); CREATE VIEW query AS SELECT * FROM t LIMIT 0;`, data);
    expect(r.length).toBe(0);
  });
  
  it('ORDER BY column alias', () => {
    const r = executeQuery(`CREATE TABLE t (id INT, v INT); CREATE VIEW query AS SELECT id, v AS value FROM t ORDER BY value;`, data);
    expect(r[0]?.value).toBe(10);
  });
  
  it('ORDER BY ordinal position', () => {
    // ORDER BY 2 (second column)
    const r = executeQuery(`CREATE TABLE t (id INT, v INT); CREATE VIEW query AS SELECT id, v FROM t ORDER BY 2;`, data);
    expect(r[0]?.v).toBe(10);
  });
});

// ============================================================================
// CATEGORY 15: DISTINCT
// ============================================================================
describe('Systematic: DISTINCT', () => {
  it('SELECT DISTINCT single column', () => {
    const data = { t: [{ id: 1, v: 'A' }, { id: 2, v: 'A' }, { id: 3, v: 'B' }] };
    const r = executeQuery(`CREATE TABLE t (id INT, v VARCHAR); CREATE VIEW query AS SELECT DISTINCT v FROM t;`, data);
    expect(r.length).toBe(2);
  });
  
  it('SELECT DISTINCT multiple columns', () => {
    const data = { t: [{ id: 1, a: 'X', b: 1 }, { id: 2, a: 'X', b: 1 }, { id: 3, a: 'X', b: 2 }] };
    const r = executeQuery(`CREATE TABLE t (id INT, a VARCHAR, b INT); CREATE VIEW query AS SELECT DISTINCT a, b FROM t;`, data);
    expect(r.length).toBe(2);
  });
  
  it('SELECT DISTINCT with ORDER BY', () => {
    const data = { t: [{ id: 1, v: 'B' }, { id: 2, v: 'A' }, { id: 3, v: 'B' }] };
    const r = executeQuery(`CREATE TABLE t (id INT, v VARCHAR); CREATE VIEW query AS SELECT DISTINCT v FROM t ORDER BY v;`, data);
    expect(r.length).toBe(2);
    expect(r[0]?.v).toBe('A');
  });
});

