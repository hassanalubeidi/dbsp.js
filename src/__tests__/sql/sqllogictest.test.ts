/**
 * SQLLogicTest Tests
 * ==================
 * 
 * These tests use the SQLLogicTest format to verify SQL compliance.
 * Based on the SQLite SQLLogicTest framework.
 */

import { describe, it, expect } from 'vitest';
import { parseSQLLogicTest, runSQLLogicTest, runAllTests } from './sqlite-adapter';

// Sample SQLLogicTest content for basic tests
const BASIC_SELECT_TESTS = `
# Basic SELECT tests

statement ok
CREATE TABLE t1 (a INT, b INT, c INT)

statement ok
INSERT INTO t1 (a, b, c) VALUES (1, 2, 3)

statement ok
INSERT INTO t1 (a, b, c) VALUES (4, 5, 6)

query III rowsort
SELECT a, b, c FROM t1
----
1
2
3
4
5
6
`;

const AGGREGATE_TESTS = `
# Aggregate function tests

statement ok
CREATE TABLE numbers (n INT)

statement ok
INSERT INTO numbers (n) VALUES (10)

statement ok
INSERT INTO numbers (n) VALUES (20)

statement ok
INSERT INTO numbers (n) VALUES (30)

query I nosort
SELECT SUM(n) FROM numbers
----
60

query I nosort
SELECT COUNT(*) FROM numbers
----
3

query I nosort
SELECT MAX(n) FROM numbers
----
30

query I nosort
SELECT MIN(n) FROM numbers
----
10
`;

const WHERE_CLAUSE_TESTS = `
# WHERE clause tests

statement ok
CREATE TABLE items (id INT, value INT, category VARCHAR)

statement ok
INSERT INTO items (id, value, category) VALUES (1, 100, 'A')

statement ok
INSERT INTO items (id, value, category) VALUES (2, 200, 'B')

statement ok
INSERT INTO items (id, value, category) VALUES (3, 150, 'A')

query II rowsort
SELECT id, value FROM items WHERE category = 'A'
----
1
100
3
150

query I nosort
SELECT SUM(value) FROM items WHERE value > 100
----
350
`;

const GROUP_BY_TESTS = `
# GROUP BY tests

statement ok
CREATE TABLE sales (product VARCHAR, amount INT)

statement ok
INSERT INTO sales (product, amount) VALUES ('apple', 10)

statement ok
INSERT INTO sales (product, amount) VALUES ('banana', 20)

statement ok
INSERT INTO sales (product, amount) VALUES ('apple', 15)

statement ok
INSERT INTO sales (product, amount) VALUES ('banana', 25)

query TI rowsort
SELECT product, SUM(amount) FROM sales GROUP BY product
----
apple
25
banana
45
`;

const JOIN_TESTS = `
# JOIN tests

statement ok
CREATE TABLE customers (id INT, name VARCHAR)

statement ok
CREATE TABLE orders (order_id INT, customer_id INT, amount INT)

statement ok
INSERT INTO customers (id, name) VALUES (1, 'Alice')

statement ok
INSERT INTO customers (id, name) VALUES (2, 'Bob')

statement ok
INSERT INTO orders (order_id, customer_id, amount) VALUES (101, 1, 50)

statement ok
INSERT INTO orders (order_id, customer_id, amount) VALUES (102, 1, 75)

statement ok
INSERT INTO orders (order_id, customer_id, amount) VALUES (103, 2, 100)

query TI rowsort
SELECT c.name, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id
----
Alice
50
Alice
75
Bob
100
`;

describe('SQLLogicTest Adapter', () => {
  describe('Parser', () => {
    it('should parse statement ok/error directives', () => {
      const content = `
statement ok
CREATE TABLE t1 (a INT)

statement error
DROP TABLE nonexistent
`;
      const tests = parseSQLLogicTest(content);
      expect(tests).toHaveLength(2);
      expect(tests[0].type).toBe('statement');
      expect(tests[0].expected).toBe('ok');
      expect(tests[1].type).toBe('statement');
      expect(tests[1].expected).toBe('error');
    });

    it('should parse query directives with expected results', () => {
      const content = `
query I nosort
SELECT 1
----
1
`;
      const tests = parseSQLLogicTest(content);
      expect(tests).toHaveLength(1);
      expect(tests[0].type).toBe('query');
      expect(tests[0].resultType).toBe('I');
      expect(tests[0].sortMode).toBe('nosort');
      expect(tests[0].expected).toEqual(['1']);
    });
  });

  describe('Basic SELECT', () => {
    it('should run basic SELECT tests', () => {
      const result = runAllTests(BASIC_SELECT_TESTS);
      
      // Log failures for debugging
      result.results.forEach((r, i) => {
        if (!r.passed) {
          console.log(`Test ${i} failed:`, r);
        }
      });
      
      expect(result.failed).toBe(0);
    });
  });

  describe('Aggregate Functions', () => {
    it('should run aggregate tests', () => {
      const result = runAllTests(AGGREGATE_TESTS);
      
      // Log failures for debugging
      result.results.forEach((r, i) => {
        if (!r.passed) {
          console.log(`Aggregate test ${i} failed:`, r);
        }
      });
      
      // Target 95% pass rate
      expect(result.passed).toBeGreaterThanOrEqual(Math.floor(result.total * 0.95));
    });
  });

  describe('WHERE Clauses', () => {
    it('should run WHERE clause tests', () => {
      const result = runAllTests(WHERE_CLAUSE_TESTS);
      
      result.results.forEach((r, i) => {
        if (!r.passed) {
          console.log(`WHERE test ${i} failed:`, r);
        }
      });
      
      // Target 95% pass rate
      expect(result.passed).toBeGreaterThanOrEqual(Math.floor(result.total * 0.95));
    });
  });

  describe('GROUP BY', () => {
    it('should run GROUP BY tests', () => {
      const result = runAllTests(GROUP_BY_TESTS);
      
      result.results.forEach((r, i) => {
        if (!r.passed) {
          console.log(`GROUP BY test ${i} failed:`, r);
        }
      });
      
      // Target 95% pass rate
      expect(result.passed).toBeGreaterThanOrEqual(Math.floor(result.total * 0.95));
    });
  });

  describe('JOIN', () => {
    it('should run JOIN tests', () => {
      const result = runAllTests(JOIN_TESTS);
      
      result.results.forEach((r, i) => {
        if (!r.passed) {
          console.log(`JOIN test ${i} failed:`, r);
        }
      });
      
      // Target 95% pass rate
      expect(result.passed).toBeGreaterThanOrEqual(Math.floor(result.total * 0.95));
    });
  });
});

describe('SQLLogicTest: select1 (Sample from SQLite)', () => {
  // This is a subset of the actual SQLite select1.test file
  const SELECT1_TESTS = `
statement ok
CREATE TABLE t1(a INT, b INT, c INT, d INT, e INT)

statement ok
INSERT INTO t1 (a, b, c, d, e) VALUES (1, 2, 3, 4, 5)

statement ok
INSERT INTO t1 (a, b, c, d, e) VALUES (6, 7, 8, 9, 10)

statement ok
INSERT INTO t1 (a, b, c, d, e) VALUES (11, 12, 13, 14, 15)

query I nosort
SELECT a FROM t1 WHERE a = 1
----
1

query I nosort
SELECT a FROM t1 WHERE a > 1 AND a < 11
----
6

query II rowsort
SELECT a, b FROM t1 WHERE a = 1 OR a = 6
----
1
2
6
7

query I nosort
SELECT SUM(a) FROM t1
----
18

query I nosort
SELECT COUNT(*) FROM t1 WHERE a > 5
----
2

query II rowsort
SELECT a, a + b FROM t1
----
1
3
6
13
11
23
`;

  it('should pass select1 sample tests', () => {
    const result = runAllTests(SELECT1_TESTS);
    
    result.results.forEach((r, i) => {
      if (!r.passed && r.error) {
        console.log(`select1 test ${i} error:`, r.error);
      } else if (!r.passed) {
        console.log(`select1 test ${i} failed:`, 
          'expected:', r.expected, 
          'actual:', r.actual);
      }
    });
    
    // Track progress - target 95% pass rate
    const passRate = result.passed / result.total;
    console.log(`SQLLogicTest select1: ${result.passed}/${result.total} (${(passRate * 100).toFixed(1)}%)`);
    
    expect(passRate).toBeGreaterThanOrEqual(0.95);
  });
});

