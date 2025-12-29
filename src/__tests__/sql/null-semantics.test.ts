/**
 * Three-Valued NULL Logic Tests
 * ==============================
 * 
 * Tests for SQL's three-valued logic where NULL represents "unknown".
 * In WHERE clauses, NULL comparisons filter out rows.
 * 
 * Key rules:
 * - NULL = anything → NULL (false in WHERE)
 * - NULL <> anything → NULL (false in WHERE)
 * - NULL AND TRUE → NULL (false in WHERE)
 * - NULL AND FALSE → FALSE
 * - NULL OR TRUE → TRUE
 * - NULL OR FALSE → NULL (false in WHERE)
 * - NOT NULL → NULL (false in WHERE)
 */

import { describe, it, expect } from 'vitest';
import { SQLCompiler } from '../../sql/sql-compiler';
import { ZSet } from '../../internals/zset';

// Standard test helper following existing patterns
function compileSQL(sql: string) {
  const compiler = new SQLCompiler();
  return compiler.compile(sql);
}

describe('Three-Valued NULL Logic', () => {
  // Helper to execute a SQL query and get results
  function executeQuery(sql: string, data: Record<string, any[]>) {
    const { circuit, tables, views } = compileSQL(sql);
    
    // Get view results
    const viewName = Object.keys(views)[0];
    const view = views[viewName];
    const results: ZSet<any>[] = [];
    view.output((zset) => results.push(zset as ZSet<any>));
    
    // Step with initial data
    const inputMap = new Map<string, ZSet<any>>();
    for (const [tableName, rows] of Object.entries(data)) {
      const zset = ZSet.fromValues(rows);
      inputMap.set(tableName.toLowerCase(), zset);
    }
    
    circuit.step(inputMap);
    
    // Flatten results
    const allRows: any[] = [];
    for (const zset of results) {
      for (const [row, weight] of zset.entries()) {
        if (weight > 0) {
          allRows.push(row);
        }
      }
    }
    return allRows;
  }

  describe('NULL comparisons', () => {
    it('should filter out rows where column = value and column is NULL', () => {
      const sql = `
        CREATE TABLE t (id INT, value INT);
        CREATE VIEW v AS SELECT * FROM t WHERE value = 10;
      `;
      const results = executeQuery(sql, {
        t: [
          { id: 1, value: 10 },
          { id: 2, value: null },
          { id: 3, value: 20 }
        ]
      });
      expect(results.length).toBe(1);
      expect(results[0].id).toBe(1);
    });

    it('should filter out rows where column != value and column is NULL', () => {
      const sql = `
        CREATE TABLE t (id INT, value INT);
        CREATE VIEW v AS SELECT * FROM t WHERE value != 10;
      `;
      const results = executeQuery(sql, {
        t: [
          { id: 1, value: 10 },
          { id: 2, value: null },
          { id: 3, value: 20 }
        ]
      });
      expect(results.length).toBe(1);
      expect(results[0].id).toBe(3);
    });

    it('should filter out rows where column < value and column is NULL', () => {
      const sql = `
        CREATE TABLE t (id INT, value INT);
        CREATE VIEW v AS SELECT * FROM t WHERE value < 15;
      `;
      const results = executeQuery(sql, {
        t: [
          { id: 1, value: 10 },
          { id: 2, value: null },
          { id: 3, value: 20 }
        ]
      });
      expect(results.length).toBe(1);
      expect(results[0].id).toBe(1);
    });

    it('should filter out rows where column > value and column is NULL', () => {
      const sql = `
        CREATE TABLE t (id INT, value INT);
        CREATE VIEW v AS SELECT * FROM t WHERE value > 15;
      `;
      const results = executeQuery(sql, {
        t: [
          { id: 1, value: 10 },
          { id: 2, value: null },
          { id: 3, value: 20 }
        ]
      });
      expect(results.length).toBe(1);
      expect(results[0].id).toBe(3);
    });
  });

  describe('Column-to-column comparisons with NULL', () => {
    it('should filter out rows where either column is NULL', () => {
      const sql = `
        CREATE TABLE t (id INT, a INT, b INT);
        CREATE VIEW v AS SELECT * FROM t WHERE a = b;
      `;
      const results = executeQuery(sql, {
        t: [
          { id: 1, a: 10, b: 10 },
          { id: 2, a: null, b: 10 },
          { id: 3, a: 10, b: null },
          { id: 4, a: null, b: null }
        ]
      });
      expect(results.length).toBe(1);
      expect(results[0].id).toBe(1);
    });
  });

  describe('NULL with BETWEEN', () => {
    it('should filter out rows where column is NULL in BETWEEN', () => {
      const sql = `
        CREATE TABLE t (id INT, value INT);
        CREATE VIEW v AS SELECT * FROM t WHERE value BETWEEN 5 AND 15;
      `;
      const results = executeQuery(sql, {
        t: [
          { id: 1, value: 10 },
          { id: 2, value: null },
          { id: 3, value: 20 }
        ]
      });
      expect(results.length).toBe(1);
      expect(results[0].id).toBe(1);
    });
  });

  describe('NULL with IN', () => {
    it('should filter out rows where column is NULL in IN clause', () => {
      const sql = `
        CREATE TABLE t (id INT, value INT);
        CREATE VIEW v AS SELECT * FROM t WHERE value IN (10, 20, 30);
      `;
      const results = executeQuery(sql, {
        t: [
          { id: 1, value: 10 },
          { id: 2, value: null },
          { id: 3, value: 15 }
        ]
      });
      expect(results.length).toBe(1);
      expect(results[0].id).toBe(1);
    });
  });

  describe('NULL with LIKE', () => {
    it('should filter out rows where column is NULL in LIKE', () => {
      const sql = `
        CREATE TABLE t (id INT, name TEXT);
        CREATE VIEW v AS SELECT * FROM t WHERE name LIKE 'A%';
      `;
      const results = executeQuery(sql, {
        t: [
          { id: 1, name: 'Alice' },
          { id: 2, name: null },
          { id: 3, name: 'Bob' }
        ]
      });
      expect(results.length).toBe(1);
      expect(results[0].id).toBe(1);
    });
  });

  describe('IS NULL and IS NOT NULL', () => {
    it('should find NULL values with IS NULL', () => {
      const sql = `
        CREATE TABLE t (id INT, value INT);
        CREATE VIEW v AS SELECT * FROM t WHERE value IS NULL;
      `;
      const results = executeQuery(sql, {
        t: [
          { id: 1, value: 10 },
          { id: 2, value: null },
          { id: 3, value: 20 }
        ]
      });
      expect(results.length).toBe(1);
      expect(results[0].id).toBe(2);
    });

    it('should find non-NULL values with IS NOT NULL', () => {
      const sql = `
        CREATE TABLE t (id INT, value INT);
        CREATE VIEW v AS SELECT * FROM t WHERE value IS NOT NULL;
      `;
      const results = executeQuery(sql, {
        t: [
          { id: 1, value: 10 },
          { id: 2, value: null },
          { id: 3, value: 20 }
        ]
      });
      expect(results.length).toBe(2);
      const ids = results.map((r: any) => r.id).sort();
      expect(ids).toEqual([1, 3]);
    });
  });

  describe('NULL with AND', () => {
    it('should handle NULL AND TRUE (filter out)', () => {
      const sql = `
        CREATE TABLE t (id INT, a INT, b INT);
        CREATE VIEW v AS SELECT * FROM t WHERE a = 10 AND b = 20;
      `;
      const results = executeQuery(sql, {
        t: [
          { id: 1, a: 10, b: 20 },    // TRUE AND TRUE = TRUE (include)
          { id: 2, a: null, b: 20 },  // NULL AND TRUE = NULL (exclude)
          { id: 3, a: 10, b: null },  // TRUE AND NULL = NULL (exclude)
          { id: 4, a: 5, b: 20 }      // FALSE AND TRUE = FALSE (exclude)
        ]
      });
      expect(results.length).toBe(1);
      expect(results[0].id).toBe(1);
    });
  });

  describe('NULL with OR', () => {
    it('should handle NULL OR TRUE (include)', () => {
      const sql = `
        CREATE TABLE t (id INT, a INT, b INT);
        CREATE VIEW v AS SELECT * FROM t WHERE a = 10 OR b = 20;
      `;
      const results = executeQuery(sql, {
        t: [
          { id: 1, a: 10, b: 5 },     // TRUE OR FALSE = TRUE (include)
          { id: 2, a: null, b: 20 },  // NULL OR TRUE = TRUE (include)
          { id: 3, a: 10, b: null },  // TRUE OR NULL = TRUE (include)
          { id: 4, a: null, b: null } // NULL OR NULL = NULL (exclude)
        ]
      });
      expect(results.length).toBe(3);
      const ids = results.map((r: any) => r.id).sort();
      expect(ids).toEqual([1, 2, 3]);
    });
  });

  describe('NULL in aggregations', () => {
    it('should ignore NULL in COUNT(column)', () => {
      const sql = `
        CREATE TABLE t (value INT);
        CREATE VIEW v AS SELECT COUNT(value) AS cnt FROM t;
      `;
      const results = executeQuery(sql, {
        t: [
          { value: 10 },
          { value: null },
          { value: 20 }
        ]
      });
      expect(results.length).toBe(1);
      expect(results[0].cnt).toBe(2); // NULL is not counted
    });

    it('should count all rows with COUNT(*)', () => {
      const sql = `
        CREATE TABLE t (value INT);
        CREATE VIEW v AS SELECT COUNT(*) AS cnt FROM t;
      `;
      const results = executeQuery(sql, {
        t: [
          { value: 10 },
          { value: null },
          { value: 20 }
        ]
      });
      expect(results.length).toBe(1);
      expect(results[0].cnt).toBe(3); // COUNT(*) includes NULL rows
    });

    it('should ignore NULL in SUM', () => {
      const sql = `
        CREATE TABLE t (value INT);
        CREATE VIEW v AS SELECT SUM(value) AS total FROM t;
      `;
      const results = executeQuery(sql, {
        t: [
          { value: 10 },
          { value: null },
          { value: 20 }
        ]
      });
      expect(results.length).toBe(1);
      expect(results[0].total).toBe(30); // NULL is ignored
    });

    it('should ignore NULL in AVG', () => {
      const sql = `
        CREATE TABLE t (value INT);
        CREATE VIEW v AS SELECT AVG(value) AS avg FROM t;
      `;
      const results = executeQuery(sql, {
        t: [
          { value: 10 },
          { value: null },
          { value: 20 }
        ]
      });
      expect(results.length).toBe(1);
      expect(results[0].avg).toBe(15); // (10 + 20) / 2 = 15
    });

    it('should ignore NULL in MIN', () => {
      const sql = `
        CREATE TABLE t (value INT);
        CREATE VIEW v AS SELECT MIN(value) AS minimum FROM t;
      `;
      const results = executeQuery(sql, {
        t: [
          { value: 10 },
          { value: null },
          { value: 5 },
          { value: 20 }
        ]
      });
      expect(results.length).toBe(1);
      expect(results[0].minimum).toBe(5);
    });

    it('should ignore NULL in MAX', () => {
      const sql = `
        CREATE TABLE t (value INT);
        CREATE VIEW v AS SELECT MAX(value) AS maximum FROM t;
      `;
      const results = executeQuery(sql, {
        t: [
          { value: 10 },
          { value: null },
          { value: 5 },
          { value: 20 }
        ]
      });
      expect(results.length).toBe(1);
      expect(results[0].maximum).toBe(20);
    });
  });

  describe('COALESCE with NULL', () => {
    it('should return first non-null value', () => {
      const sql = `
        CREATE TABLE t (a INT, b INT, c INT);
        CREATE VIEW v AS SELECT COALESCE(a, b, c) AS result FROM t;
      `;
      const results = executeQuery(sql, {
        t: [
          { a: 1, b: 2, c: 3 },      // returns 1
          { a: null, b: 2, c: 3 },   // returns 2
          { a: null, b: null, c: 3 } // returns 3
        ]
      });
      expect(results.length).toBe(3);
      const values = results.map((r: any) => r.result).sort();
      expect(values).toEqual([1, 2, 3]);
    });
  });
});


