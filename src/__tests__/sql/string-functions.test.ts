/**
 * String Function Tests
 * =====================
 * 
 * Tests for SQLite-compatible string functions including PRINTF, GLOB, HEX, CHAR, etc.
 */

import { describe, it, expect } from 'vitest';
import { SQLCompiler } from '../../sql/sql-compiler';
import { ZSet } from '../../internals/zset';

// Standard test helper following existing patterns
function compileSQL(sql: string) {
  const compiler = new SQLCompiler();
  return compiler.compile(sql);
}

describe('SQLite String Functions', () => {
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

  describe('PRINTF/FORMAT function', () => {
    it('should format string with %s', () => {
      const sql = `
        CREATE TABLE t (name TEXT);
        CREATE VIEW v AS SELECT PRINTF('Hello, %s!', name) AS greeting FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ name: 'World' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].greeting).toBe('Hello, World!');
    });

    it('should format integers with %d', () => {
      const sql = `
        CREATE TABLE t (value INT);
        CREATE VIEW v AS SELECT PRINTF('Value: %d', value) AS result FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ value: 42 }]
      });
      expect(results.length).toBe(1);
      expect(results[0].result).toBe('Value: 42');
    });

    it('should format floats with %f', () => {
      const sql = `
        CREATE TABLE t (value DECIMAL);
        CREATE VIEW v AS SELECT PRINTF('Price: %.2f', value) AS result FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ value: 19.99 }]
      });
      expect(results.length).toBe(1);
      expect(results[0].result).toBe('Price: 19.99');
    });

    it('should format with width padding', () => {
      const sql = `
        CREATE TABLE t (value INT);
        CREATE VIEW v AS SELECT PRINTF('%5d', value) AS result FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ value: 42 }]
      });
      expect(results.length).toBe(1);
      expect(results[0].result).toBe('   42');
    });

    it('should format with left align', () => {
      const sql = `
        CREATE TABLE t (value INT);
        CREATE VIEW v AS SELECT PRINTF('%-5d', value) AS result FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ value: 42 }]
      });
      expect(results.length).toBe(1);
      expect(results[0].result).toBe('42   ');
    });

    it('should escape percent with %%', () => {
      const sql = `
        CREATE TABLE t (value INT);
        CREATE VIEW v AS SELECT PRINTF('%d%%', value) AS result FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ value: 100 }]
      });
      expect(results.length).toBe(1);
      expect(results[0].result).toBe('100%');
    });

    it('should format multiple arguments', () => {
      const sql = `
        CREATE TABLE t (name TEXT, age INT);
        CREATE VIEW v AS SELECT PRINTF('%s is %d years old', name, age) AS result FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ name: 'Alice', age: 30 }]
      });
      expect(results.length).toBe(1);
      expect(results[0].result).toBe('Alice is 30 years old');
    });

    it('should format hex with %x', () => {
      const sql = `
        CREATE TABLE t (value INT);
        CREATE VIEW v AS SELECT PRINTF('%x', value) AS result FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ value: 255 }]
      });
      expect(results.length).toBe(1);
      expect(results[0].result).toBe('ff');
    });

    it('should format hex uppercase with %X', () => {
      const sql = `
        CREATE TABLE t (value INT);
        CREATE VIEW v AS SELECT PRINTF('%X', value) AS result FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ value: 255 }]
      });
      expect(results.length).toBe(1);
      expect(results[0].result).toBe('FF');
    });
  });

  describe('GLOB function', () => {
    it('should match with * wildcard', () => {
      const sql = `
        CREATE TABLE t (filename TEXT);
        CREATE VIEW v AS SELECT filename, GLOB('*.txt', filename) AS matches FROM t;
      `;
      const results = executeQuery(sql, {
        t: [
          { filename: 'document.txt' },
          { filename: 'image.png' },
          { filename: 'notes.txt' }
        ]
      });
      expect(results.length).toBe(3);
      const txt1 = results.find((r: any) => r.filename === 'document.txt');
      const png = results.find((r: any) => r.filename === 'image.png');
      const txt2 = results.find((r: any) => r.filename === 'notes.txt');
      expect(txt1.matches).toBe(1);
      expect(png.matches).toBe(0);
      expect(txt2.matches).toBe(1);
    });

    it('should match with ? wildcard', () => {
      const sql = `
        CREATE TABLE t (code TEXT);
        CREATE VIEW v AS SELECT code, GLOB('A??', code) AS matches FROM t;
      `;
      const results = executeQuery(sql, {
        t: [
          { code: 'ABC' },
          { code: 'AB' },
          { code: 'ABCD' }
        ]
      });
      expect(results.length).toBe(3);
      const abc = results.find((r: any) => r.code === 'ABC');
      const ab = results.find((r: any) => r.code === 'AB');
      const abcd = results.find((r: any) => r.code === 'ABCD');
      expect(abc.matches).toBe(1);
      expect(ab.matches).toBe(0);
      expect(abcd.matches).toBe(0);
    });

    it('should match with character class', () => {
      const sql = `
        CREATE TABLE t (chr TEXT);
        CREATE VIEW v AS SELECT chr, GLOB('[abc]', chr) AS matches FROM t;
      `;
      const results = executeQuery(sql, {
        t: [
          { chr: 'a' },
          { chr: 'b' },
          { chr: 'd' }
        ]
      });
      expect(results.length).toBe(3);
      const a = results.find((r: any) => r.chr === 'a');
      const b = results.find((r: any) => r.chr === 'b');
      const d = results.find((r: any) => r.chr === 'd');
      expect(a.matches).toBe(1);
      expect(b.matches).toBe(1);
      expect(d.matches).toBe(0);
    });

    it('should be case sensitive', () => {
      const sql = `
        CREATE TABLE t (name TEXT);
        CREATE VIEW v AS SELECT name, GLOB('Test*', name) AS matches FROM t;
      `;
      const results = executeQuery(sql, {
        t: [
          { name: 'Testing' },
          { name: 'testing' },
          { name: 'TEST' }
        ]
      });
      expect(results.length).toBe(3);
      const testing = results.find((r: any) => r.name === 'Testing');
      const lowerTesting = results.find((r: any) => r.name === 'testing');
      const upperTest = results.find((r: any) => r.name === 'TEST');
      expect(testing.matches).toBe(1);
      expect(lowerTesting.matches).toBe(0);
      expect(upperTest.matches).toBe(0);
    });
  });

  describe('HEX function', () => {
    it('should convert number to hex', () => {
      const sql = `
        CREATE TABLE t (value INT);
        CREATE VIEW v AS SELECT HEX(value) AS hex FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ value: 255 }]
      });
      expect(results.length).toBe(1);
      expect(results[0].hex).toBe('FF');
    });

    it('should convert string to hex codes', () => {
      const sql = `
        CREATE TABLE t (value TEXT);
        CREATE VIEW v AS SELECT HEX(value) AS hex FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ value: 'AB' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].hex).toBe('4142'); // A=0x41, B=0x42
    });
  });

  describe('CHAR function', () => {
    it('should convert code points to characters', () => {
      const sql = `
        CREATE TABLE t (id INT);
        CREATE VIEW v AS SELECT CHAR(65, 66, 67) AS chars FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ id: 1 }]
      });
      expect(results.length).toBe(1);
      expect(results[0].chars).toBe('ABC');
    });
  });

  describe('UNICODE function', () => {
    it('should return code point of first character', () => {
      const sql = `
        CREATE TABLE t (chr TEXT);
        CREATE VIEW v AS SELECT UNICODE(chr) AS code FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ chr: 'A' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].code).toBe(65);
    });
  });

  describe('TYPEOF function', () => {
    it('should return integer for integer values', () => {
      const sql = `
        CREATE TABLE t (value INT);
        CREATE VIEW v AS SELECT TYPEOF(value) AS tp FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ value: 42 }]
      });
      expect(results.length).toBe(1);
      expect(results[0].tp).toBe('integer');
    });

    it('should return real for float values', () => {
      const sql = `
        CREATE TABLE t (value DECIMAL);
        CREATE VIEW v AS SELECT TYPEOF(value) AS tp FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ value: 3.14 }]
      });
      expect(results.length).toBe(1);
      expect(results[0].tp).toBe('real');
    });

    it('should return text for string values', () => {
      const sql = `
        CREATE TABLE t (value TEXT);
        CREATE VIEW v AS SELECT TYPEOF(value) AS tp FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ value: 'hello' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].tp).toBe('text');
    });

    it('should return null for null values', () => {
      const sql = `
        CREATE TABLE t (value TEXT);
        CREATE VIEW v AS SELECT TYPEOF(value) AS tp FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ value: null }]
      });
      expect(results.length).toBe(1);
      expect(results[0].tp).toBe('null');
    });
  });

  describe('INSTR function', () => {
    it('should find substring position', () => {
      const sql = `
        CREATE TABLE t (str TEXT, sub TEXT);
        CREATE VIEW v AS SELECT INSTR(str, sub) AS pos FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ str: 'hello world', sub: 'world' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].pos).toBe(7); // 1-indexed
    });

    it('should return 0 if not found', () => {
      const sql = `
        CREATE TABLE t (str TEXT, sub TEXT);
        CREATE VIEW v AS SELECT INSTR(str, sub) AS pos FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ str: 'hello world', sub: 'xyz' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].pos).toBe(0);
    });
  });

  describe('Other string functions', () => {
    it('should handle UPPER', () => {
      const sql = `
        CREATE TABLE t (str TEXT);
        CREATE VIEW v AS SELECT UPPER(str) AS result FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ str: 'hello' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].result).toBe('HELLO');
    });

    it('should handle LOWER', () => {
      const sql = `
        CREATE TABLE t (str TEXT);
        CREATE VIEW v AS SELECT LOWER(str) AS result FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ str: 'HELLO' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].result).toBe('hello');
    });

    it('should handle LENGTH', () => {
      const sql = `
        CREATE TABLE t (str TEXT);
        CREATE VIEW v AS SELECT LENGTH(str) AS len FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ str: 'hello' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].len).toBe(5);
    });

    it('should handle SUBSTR', () => {
      const sql = `
        CREATE TABLE t (str TEXT);
        CREATE VIEW v AS SELECT SUBSTR(str, 2, 3) AS result FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ str: 'hello' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].result).toBe('ell');
    });

    it('should handle TRIM', () => {
      const sql = `
        CREATE TABLE t (str TEXT);
        CREATE VIEW v AS SELECT TRIM(str) AS result FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ str: '  hello  ' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].result).toBe('hello');
    });

    it('should handle REPLACE', () => {
      const sql = `
        CREATE TABLE t (str TEXT);
        CREATE VIEW v AS SELECT REPLACE(str, 'o', '0') AS result FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ str: 'hello world' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].result).toBe('hell0 w0rld');
    });

    it('should handle CONCAT', () => {
      const sql = `
        CREATE TABLE t (a TEXT, b TEXT);
        CREATE VIEW v AS SELECT CONCAT(a, b) AS result FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ a: 'hello', b: 'world' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].result).toBe('helloworld');
    });

    it('should handle REVERSE', () => {
      const sql = `
        CREATE TABLE t (str TEXT);
        CREATE VIEW v AS SELECT REVERSE(str) AS result FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ str: 'hello' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].result).toBe('olleh');
    });

    it('should handle REPEAT', () => {
      const sql = `
        CREATE TABLE t (str TEXT, n INT);
        CREATE VIEW v AS SELECT REPEAT(str, n) AS result FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ str: 'ab', n: 3 }]
      });
      expect(results.length).toBe(1);
      expect(results[0].result).toBe('ababab');
    });

    it('should handle COALESCE', () => {
      const sql = `
        CREATE TABLE t (a TEXT, b TEXT);
        CREATE VIEW v AS SELECT COALESCE(a, b) AS result FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ a: null, b: 'fallback' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].result).toBe('fallback');
    });
  });
});

