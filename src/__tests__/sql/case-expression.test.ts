/**
 * CASE Expression Tests
 * =====================
 * 
 * Comprehensive tests for CASE WHEN expressions in SQL aggregation queries.
 * Tests post-aggregation CASE expressions that reference computed aggregates.
 */

import { describe, it, expect } from 'vitest';
import { SQLCompiler, SQLParser } from '../../sql/sql-compiler';
import { ZSet } from '../../internals/zset';
import { getCanonicalAggregateKey } from '../../sql/expression-eval';

// Standard test helper following existing patterns
function compileSQL(sql: string) {
  const compiler = new SQLCompiler();
  return compiler.compile(sql);
}

describe('CASE Expression in Aggregations', () => {
  
  describe('Basic CASE with COUNT(*)', () => {
    
    it('should compute hit rate from COUNT(*) and SUM', () => {
      const sql = `
        CREATE TABLE rfqs (id INT, status TEXT);
        CREATE VIEW stats AS 
         SELECT 
           COUNT(*) AS total,
           SUM(CASE WHEN status = 'FILLED' THEN 1 ELSE 0 END) AS filled,
           CASE WHEN COUNT(*) > 0 
                THEN SUM(CASE WHEN status = 'FILLED' THEN 1 ELSE 0 END) * 1.0 / COUNT(*)
                ELSE 0 END AS hitRate
         FROM rfqs
      `;
      
      const { circuit, tables, views } = compileSQL(sql);
      
      const results: ZSet<any>[] = [];
      views.stats.output((zset) => {
        results.push(zset as ZSet<any>);
      });
      
      circuit.step(new Map([
        ['rfqs', ZSet.fromValues([
          { id: 1, status: 'FILLED' },
          { id: 2, status: 'FILLED' },
          { id: 3, status: 'REJECTED' },
          { id: 4, status: 'FILLED' },
          { id: 5, status: 'REJECTED' },
        ])]
      ]));
      
      expect(results).toHaveLength(1);
      const row = results[0].values()[0];
      expect(row.total).toBe(5);
      expect(row.filled).toBe(3);
      expect(row.hitRate).toBeCloseTo(0.6, 5);
    });
    
    it('should handle single row', () => {
      const sql = `
        CREATE TABLE single (id INT, value INT);
        CREATE VIEW single_stats AS 
         SELECT 
           COUNT(*) AS cnt,
           SUM(value) AS total,
           CASE WHEN COUNT(*) > 0 THEN SUM(value) / COUNT(*) ELSE 0 END AS avg
         FROM single
      `;
      
      const { circuit, views } = compileSQL(sql);
      
      const results: ZSet<any>[] = [];
      views.single_stats.output((zset) => {
        results.push(zset as ZSet<any>);
      });
      
      circuit.step(new Map([
        ['single', ZSet.fromValues([{ id: 1, value: 100 }])]
      ]));
      
      expect(results).toHaveLength(1);
      const row = results[0].values()[0];
      expect(row.cnt).toBe(1);
      expect(row.total).toBe(100);
      expect(row.avg).toBe(100);
    });
  });
  
  describe('CASE with different comparison operators', () => {
    
    it('should handle > operator', () => {
      const sql = `
        CREATE TABLE items (id INT, amount INT);
        CREATE VIEW gt_test AS 
         SELECT 
           SUM(amount) AS total,
           CASE WHEN SUM(amount) > 100 THEN 1 ELSE 0 END AS isHigh
         FROM items
      `;
      
      const { circuit, views } = compileSQL(sql);
      
      const results: ZSet<any>[] = [];
      views.gt_test.output((zset) => {
        results.push(zset as ZSet<any>);
      });
      
      circuit.step(new Map([
        ['items', ZSet.fromValues([
          { id: 1, amount: 50 },
          { id: 2, amount: 75 },
        ])]
      ]));
      
      const row = results[0].values()[0];
      expect(row.total).toBe(125);
      expect(row.isHigh).toBe(1);
    });
    
    it('should handle < operator', () => {
      const sql = `
        CREATE TABLE items2 (id INT, amount INT);
        CREATE VIEW lt_test AS 
         SELECT 
           SUM(amount) AS total,
           CASE WHEN SUM(amount) < 100 THEN 1 ELSE 0 END AS isLow
         FROM items2
      `;
      
      const { circuit, views } = compileSQL(sql);
      
      const results: ZSet<any>[] = [];
      views.lt_test.output((zset) => {
        results.push(zset as ZSet<any>);
      });
      
      circuit.step(new Map([
        ['items2', ZSet.fromValues([
          { id: 1, amount: 30 },
          { id: 2, amount: 20 },
        ])]
      ]));
      
      const row = results[0].values()[0];
      expect(row.total).toBe(50);
      expect(row.isLow).toBe(1);
    });
    
    it('should handle >= operator', () => {
      const sql = `
        CREATE TABLE items3 (id INT, amount INT);
        CREATE VIEW gte_test AS 
         SELECT 
           SUM(amount) AS total,
           CASE WHEN SUM(amount) >= 100 THEN 1 ELSE 0 END AS isOk
         FROM items3
      `;
      
      const { circuit, views } = compileSQL(sql);
      
      const results: ZSet<any>[] = [];
      views.gte_test.output((zset) => {
        results.push(zset as ZSet<any>);
      });
      
      circuit.step(new Map([
        ['items3', ZSet.fromValues([
          { id: 1, amount: 50 },
          { id: 2, amount: 50 },
        ])]
      ]));
      
      const row = results[0].values()[0];
      expect(row.total).toBe(100);
      expect(row.isOk).toBe(1);
    });
    
    it('should handle = operator', () => {
      const sql = `
        CREATE TABLE items5 (id INT, amount INT);
        CREATE VIEW eq_test AS 
         SELECT 
           COUNT(*) AS cnt,
           CASE WHEN COUNT(*) = 2 THEN 1 ELSE 0 END AS isPair
         FROM items5
      `;
      
      const { circuit, views } = compileSQL(sql);
      
      const results: ZSet<any>[] = [];
      views.eq_test.output((zset) => {
        results.push(zset as ZSet<any>);
      });
      
      circuit.step(new Map([
        ['items5', ZSet.fromValues([
          { id: 1, amount: 10 },
          { id: 2, amount: 20 },
        ])]
      ]));
      
      const row = results[0].values()[0];
      expect(row.cnt).toBe(2);
      expect(row.isPair).toBe(1);
    });
  });
  
  describe('CASE with arithmetic expressions', () => {
    
    it('should compute ratio with division', () => {
      const sql = `
        CREATE TABLE orders (id INT, filled INT);
        CREATE VIEW ratio_test AS 
         SELECT 
           COUNT(*) AS total,
           SUM(filled) AS filledCount,
           CASE WHEN COUNT(*) > 0 
                THEN SUM(filled) * 100 / COUNT(*) 
                ELSE 0 END AS fillPercent
         FROM orders
      `;
      
      const { circuit, views } = compileSQL(sql);
      
      const results: ZSet<any>[] = [];
      views.ratio_test.output((zset) => {
        results.push(zset as ZSet<any>);
      });
      
      circuit.step(new Map([
        ['orders', ZSet.fromValues([
          { id: 1, filled: 1 },
          { id: 2, filled: 1 },
          { id: 3, filled: 0 },
          { id: 4, filled: 1 },
        ])]
      ]));
      
      const row = results[0].values()[0];
      expect(row.fillPercent).toBe(75);
    });
    
    it('should protect against division by zero with CASE', () => {
      const sql = `
        CREATE TABLE div_test (id INT, value INT, divisor INT);
        CREATE VIEW div_zero AS 
         SELECT 
           SUM(value) AS total,
           SUM(divisor) AS divSum,
           CASE WHEN SUM(divisor) > 0 
                THEN SUM(value) / SUM(divisor)
                ELSE 0 END AS ratio
         FROM div_test
      `;
      
      const { circuit, views } = compileSQL(sql);
      
      const results: ZSet<any>[] = [];
      views.div_zero.output((zset) => {
        results.push(zset as ZSet<any>);
      });
      
      circuit.step(new Map([
        ['div_test', ZSet.fromValues([
          { id: 1, value: 100, divisor: 0 },
          { id: 2, value: 200, divisor: 0 },
        ])]
      ]));
      
      const row = results[0].values()[0];
      expect(row.divSum).toBe(0);
      expect(row.ratio).toBe(0); // Protected by CASE
    });
  });
  
  describe('Multiple CASE columns', () => {
    
    it('should evaluate multiple CASE expressions independently', () => {
      const sql = `
        CREATE TABLE multi (id INT, status TEXT, amount INT);
        CREATE VIEW multi_case AS 
         SELECT 
           COUNT(*) AS cnt,
           SUM(amount) AS total,
           CASE WHEN COUNT(*) > 5 THEN 1 ELSE 0 END AS isMany,
           CASE WHEN SUM(amount) > 1000 THEN 1 ELSE 0 END AS isHighValue,
           CASE WHEN COUNT(*) > 0 THEN SUM(amount) / COUNT(*) ELSE 0 END AS avgAmount
         FROM multi
      `;
      
      const { circuit, views } = compileSQL(sql);
      
      const results: ZSet<any>[] = [];
      views.multi_case.output((zset) => {
        results.push(zset as ZSet<any>);
      });
      
      circuit.step(new Map([
        ['multi', ZSet.fromValues([
          { id: 1, status: 'A', amount: 100 },
          { id: 2, status: 'B', amount: 200 },
          { id: 3, status: 'A', amount: 300 },
        ])]
      ]));
      
      const row = results[0].values()[0];
      expect(row.isMany).toBe(0); // 3 is not > 5
      expect(row.isHighValue).toBe(0); // 600 is not > 1000
      expect(row.avgAmount).toBe(200);
    });
  });
  
  describe('CASE with GROUP BY', () => {
    
    it('should compute CASE per group', () => {
      const sql = `
        CREATE TABLE grouped (category TEXT, amount INT);
        CREATE VIEW grouped_case AS 
         SELECT 
           category,
           COUNT(*) AS cnt,
           SUM(amount) AS total,
           CASE WHEN SUM(amount) > 100 THEN 1 ELSE 0 END AS isHigh
         FROM grouped
         GROUP BY category
      `;
      
      const { circuit, views } = compileSQL(sql);
      
      const results: any[][] = [];
      views.grouped_case.integrate().output((zset) => {
        results.push((zset as ZSet<any>).values());
      });
      
      circuit.step(new Map([
        ['grouped', ZSet.fromValues([
          { category: 'A', amount: 50 },
          { category: 'A', amount: 75 },
          { category: 'B', amount: 30 },
          { category: 'B', amount: 20 },
        ])]
      ]));
      
      expect(results[0]).toHaveLength(2);
      
      const catA = results[0].find((r: any) => r.category === 'A');
      const catB = results[0].find((r: any) => r.category === 'B');
      
      expect(catA?.total).toBe(125);
      expect(catA?.isHigh).toBe(1);
      expect(catB?.total).toBe(50);
      expect(catB?.isHigh).toBe(0);
    });
    
    it('should compute hit rate per group', () => {
      const sql = `
        CREATE TABLE rfqs_grouped (counterparty TEXT, status TEXT);
        CREATE VIEW cp_stats AS 
         SELECT 
           counterparty,
           COUNT(*) AS total,
           SUM(CASE WHEN status = 'FILLED' THEN 1 ELSE 0 END) AS filled,
           CASE WHEN COUNT(*) > 0 
                THEN SUM(CASE WHEN status = 'FILLED' THEN 1 ELSE 0 END) * 100 / COUNT(*)
                ELSE 0 END AS hitRatePct
         FROM rfqs_grouped
         GROUP BY counterparty
      `;
      
      const { circuit, views } = compileSQL(sql);
      
      const results: any[][] = [];
      views.cp_stats.integrate().output((zset) => {
        results.push((zset as ZSet<any>).values());
      });
      
      circuit.step(new Map([
        ['rfqs_grouped', ZSet.fromValues([
          { counterparty: 'JPM', status: 'FILLED' },
          { counterparty: 'JPM', status: 'FILLED' },
          { counterparty: 'JPM', status: 'REJECTED' },
          { counterparty: 'GS', status: 'FILLED' },
          { counterparty: 'GS', status: 'REJECTED' },
          { counterparty: 'GS', status: 'REJECTED' },
          { counterparty: 'GS', status: 'REJECTED' },
        ])]
      ]));
      
      expect(results[0]).toHaveLength(2);
      
      const jpm = results[0].find((r: any) => r.counterparty === 'JPM');
      const gs = results[0].find((r: any) => r.counterparty === 'GS');
      
      // JPM: 2/3 = 66.67%
      expect(jpm?.hitRatePct).toBeCloseTo(66.67, 1);
      // GS: 1/4 = 25%
      expect(gs?.hitRatePct).toBe(25);
    });
  });
  
  describe('Edge cases', () => {
    
    it('should handle all rows matching CASE condition (100% hit rate)', () => {
      const sql = `
        CREATE TABLE all_match (id INT, status TEXT);
        CREATE VIEW all_filled AS 
         SELECT 
           COUNT(*) AS total,
           SUM(CASE WHEN status = 'FILLED' THEN 1 ELSE 0 END) AS filled,
           CASE WHEN COUNT(*) > 0 
                THEN SUM(CASE WHEN status = 'FILLED' THEN 1 ELSE 0 END) * 100 / COUNT(*)
                ELSE 0 END AS hitRate
         FROM all_match
      `;
      
      const { circuit, views } = compileSQL(sql);
      
      const results: ZSet<any>[] = [];
      views.all_filled.output((zset) => {
        results.push(zset as ZSet<any>);
      });
      
      circuit.step(new Map([
        ['all_match', ZSet.fromValues([
          { id: 1, status: 'FILLED' },
          { id: 2, status: 'FILLED' },
          { id: 3, status: 'FILLED' },
        ])]
      ]));
      
      const row = results[0].values()[0];
      expect(row.hitRate).toBe(100);
    });
    
    it('should handle no rows matching (0% hit rate)', () => {
      const sql = `
        CREATE TABLE none_match (id INT, status TEXT);
        CREATE VIEW none_filled AS 
         SELECT 
           COUNT(*) AS total,
           SUM(CASE WHEN status = 'FILLED' THEN 1 ELSE 0 END) AS filled,
           CASE WHEN COUNT(*) > 0 
                THEN SUM(CASE WHEN status = 'FILLED' THEN 1 ELSE 0 END) * 100 / COUNT(*)
                ELSE 0 END AS hitRate
         FROM none_match
      `;
      
      const { circuit, views } = compileSQL(sql);
      
      const results: ZSet<any>[] = [];
      views.none_filled.output((zset) => {
        results.push(zset as ZSet<any>);
      });
      
      circuit.step(new Map([
        ['none_match', ZSet.fromValues([
          { id: 1, status: 'REJECTED' },
          { id: 2, status: 'REJECTED' },
        ])]
      ]));
      
      const row = results[0].values()[0];
      expect(row.hitRate).toBe(0);
    });
    
    it('should handle large numbers', () => {
      const sql = `
        CREATE TABLE big_nums (id INT, amount INT);
        CREATE VIEW big_test AS 
         SELECT 
           SUM(amount) AS total,
           CASE WHEN SUM(amount) > 1000000000 THEN 1 ELSE 0 END AS isHuge
         FROM big_nums
      `;
      
      const { circuit, views } = compileSQL(sql);
      
      const results: ZSet<any>[] = [];
      views.big_test.output((zset) => {
        results.push(zset as ZSet<any>);
      });
      
      circuit.step(new Map([
        ['big_nums', ZSet.fromValues([
          { id: 1, amount: 500000000 },
          { id: 2, amount: 700000000 },
        ])]
      ]));
      
      const row = results[0].values()[0];
      expect(row.total).toBe(1200000000);
      expect(row.isHuge).toBe(1);
    });
    
    it('should handle negative numbers', () => {
      const sql = `
        CREATE TABLE negatives (id INT, pnl INT);
        CREATE VIEW pnl_test AS 
         SELECT 
           SUM(pnl) AS totalPnL,
           CASE WHEN SUM(pnl) < 0 THEN 1 ELSE 0 END AS isLoss
         FROM negatives
      `;
      
      const { circuit, views } = compileSQL(sql);
      
      const results: ZSet<any>[] = [];
      views.pnl_test.output((zset) => {
        results.push(zset as ZSet<any>);
      });
      
      circuit.step(new Map([
        ['negatives', ZSet.fromValues([
          { id: 1, pnl: -100 },
          { id: 2, pnl: 50 },
          { id: 3, pnl: -75 },
        ])]
      ]));
      
      const row = results[0].values()[0];
      expect(row.totalPnL).toBe(-125);
      expect(row.isLoss).toBe(1);
    });
    
    it('should handle zero values correctly', () => {
      const sql = `
        CREATE TABLE zeros (id INT, value INT);
        CREATE VIEW zero_test AS 
         SELECT 
           SUM(value) AS total,
           CASE WHEN SUM(value) = 0 THEN 1 ELSE 0 END AS isZero
         FROM zeros
      `;
      
      const { circuit, views } = compileSQL(sql);
      
      const results: ZSet<any>[] = [];
      views.zero_test.output((zset) => {
        results.push(zset as ZSet<any>);
      });
      
      circuit.step(new Map([
        ['zeros', ZSet.fromValues([
          { id: 1, value: 0 },
          { id: 2, value: 0 },
        ])]
      ]));
      
      const row = results[0].values()[0];
      expect(row.total).toBe(0);
      expect(row.isZero).toBe(1);
    });
  });
  
  describe('Canonical Key Matching', () => {
    it('should generate matching canonical keys for parsed and raw CASE expressions', () => {
      // Test case: what key does the compiler generate from parsed AggregateArg?
      const parsedCaseExpr = {
        type: 'function',
        functionName: 'IF',
        args: [
          { type: 'function', functionName: 'EQ', args: [{ type: 'column', column: 'status' }, { type: 'expression', stringValue: 'FILLED' }] },
          { type: 'expression', value: 1 },
          { type: 'expression', value: 0 }
        ]
      };
      const compilerKey = getCanonicalAggregateKey('SUM', parsedCaseExpr);
      
      // Test case: what key does the evaluator generate from raw node-sql-parser AST?
      // Note: ELSE clause may be in expr.else or as a WHEN with cond = true
      const rawCaseExpr = {
        type: 'case',
        args: [
          { cond: { type: 'binary_expr', operator: '=', left: { type: 'column_ref', column: 'status' }, right: { type: 'single_quote_string', value: 'FILLED' } }, result: { type: 'number', value: 1 } },
          { cond: true, result: { type: 'number', value: 0 } }, // ELSE as WHEN with cond = true
        ],
        else: null
      };
      const evaluatorKey = getCanonicalAggregateKey('SUM', rawCaseExpr);
      
      expect(compilerKey).toBe(evaluatorKey);
    });
    
    it('should correctly distinguish between similar CASE expressions with different THEN values', () => {
      const caseWithThen1 = {
        type: 'case',
        args: [
          { cond: { type: 'binary_expr', operator: '=', left: { type: 'column_ref', column: 'status' }, right: { type: 'single_quote_string', value: 'FILLED' } }, result: { type: 'number', value: 1 } },
          { cond: true, result: { type: 'number', value: 0 } },
        ],
      };
      const caseWithThenNotional = {
        type: 'case',
        args: [
          { cond: { type: 'binary_expr', operator: '=', left: { type: 'column_ref', column: 'status' }, right: { type: 'single_quote_string', value: 'FILLED' } }, result: { type: 'column_ref', column: 'notional' } },
          { cond: true, result: { type: 'number', value: 0 } },
        ],
      };
      
      const key1 = getCanonicalAggregateKey('SUM', caseWithThen1);
      const key2 = getCanonicalAggregateKey('SUM', caseWithThenNotional);
      
      expect(key1).not.toBe(key2);
      expect(key1).toContain('num:1');
      expect(key2).toContain('col:notional');
    });
  });
  
  describe('Real-world scenarios', () => {
    
    it('should compute RFQ statistics like CreditTradingPage', () => {
      const sql = `
        CREATE TABLE rfqs_real (rfqId INT, status TEXT, notional INT, spreadCapture INT);
        CREATE VIEW rfq_stats AS 
         SELECT 
           COUNT(*) AS totalRFQs,
           SUM(CASE WHEN status = 'FILLED' THEN 1 ELSE 0 END) AS filledRFQs,
           SUM(CASE WHEN status = 'REJECTED' THEN 1 ELSE 0 END) AS rejectedRFQs,
           CASE WHEN COUNT(*) > 0 
                THEN SUM(CASE WHEN status = 'FILLED' THEN 1 ELSE 0 END) * 100 / COUNT(*) 
                ELSE 0 END AS hitRate,
           SUM(CASE WHEN status = 'FILLED' THEN notional ELSE 0 END) AS totalNotional
         FROM rfqs_real
      `;
      
      const { circuit, views } = compileSQL(sql);
      
      const results: ZSet<any>[] = [];
      views.rfq_stats.output((zset) => {
        results.push(zset as ZSet<any>);
      });
      
      circuit.step(new Map([
        ['rfqs_real', ZSet.fromValues([
          { rfqId: 1, status: 'FILLED', notional: 1000000, spreadCapture: 50 },
          { rfqId: 2, status: 'FILLED', notional: 2000000, spreadCapture: 75 },
          { rfqId: 3, status: 'REJECTED', notional: 500000, spreadCapture: 0 },
          { rfqId: 4, status: 'FILLED', notional: 1500000, spreadCapture: 60 },
          { rfqId: 5, status: 'REJECTED', notional: 750000, spreadCapture: 0 },
        ])]
      ]));
      
      const row = results[0].values()[0];
      expect(row.totalRFQs).toBe(5);
      expect(row.filledRFQs).toBe(3);
      expect(row.rejectedRFQs).toBe(2);
      expect(row.hitRate).toBe(60); // 3/5 * 100 = 60 (exact)
      expect(row.totalNotional).toBe(4500000);
    });
    
    it('should compute counterparty stats with GROUP BY', () => {
      const sql = `
        CREATE TABLE cp_rfqs (counterparty TEXT, status TEXT, notional INT);
        CREATE VIEW cp_analytics AS 
         SELECT 
           counterparty,
           COUNT(*) AS rfqCount,
           SUM(CASE WHEN status = 'FILLED' THEN 1 ELSE 0 END) AS filledCount,
           SUM(notional) AS totalNotional,
           CASE WHEN COUNT(*) > 0 
                THEN SUM(CASE WHEN status = 'FILLED' THEN 1 ELSE 0 END) * 100 / COUNT(*) 
                ELSE 0 END AS hitRate
         FROM cp_rfqs
         GROUP BY counterparty
      `;
      
      const { circuit, views } = compileSQL(sql);
      
      const results: any[][] = [];
      views.cp_analytics.integrate().output((zset) => {
        results.push((zset as ZSet<any>).values());
      });
      
      circuit.step(new Map([
        ['cp_rfqs', ZSet.fromValues([
          { counterparty: 'JPM', status: 'FILLED', notional: 1000000 },
          { counterparty: 'JPM', status: 'FILLED', notional: 2000000 },
          { counterparty: 'JPM', status: 'REJECTED', notional: 500000 },
          { counterparty: 'GS', status: 'REJECTED', notional: 1000000 },
          { counterparty: 'GS', status: 'REJECTED', notional: 1500000 },
          { counterparty: 'MS', status: 'FILLED', notional: 750000 },
        ])]
      ]));
      
      expect(results[0]).toHaveLength(3);
      
      const jpm = results[0].find((r: any) => r.counterparty === 'JPM');
      expect(jpm?.hitRate).toBeCloseTo(66.67, 1); // 2/3 * 100 = 66.67
      
      const gs = results[0].find((r: any) => r.counterparty === 'GS');
      expect(gs?.hitRate).toBe(0);
      
      const ms = results[0].find((r: any) => r.counterparty === 'MS');
      expect(ms?.hitRate).toBe(100);
    });
  });
  
  describe('Incremental updates', () => {
    
    it('should update CASE expression results when data changes', () => {
      const sql = `
        CREATE TABLE orders (id INT, status TEXT);
        CREATE VIEW order_stats AS 
         SELECT 
           COUNT(*) AS total,
           SUM(CASE WHEN status = 'FILLED' THEN 1 ELSE 0 END) AS filled,
           CASE WHEN COUNT(*) > 0 
                THEN SUM(CASE WHEN status = 'FILLED' THEN 1 ELSE 0 END) * 100 / COUNT(*)
                ELSE 0 END AS fillRate
         FROM orders
      `;
      
      const { circuit, views } = compileSQL(sql);
      
      // For global aggregations, use integrate() to get cumulative state
      const results: any[][] = [];
      views.order_stats.integrate().output((zset) => {
        results.push((zset as ZSet<any>).values());
      });
      
      // Initial data: 50% fill rate
      circuit.step(new Map([
        ['orders', ZSet.fromValues([
          { id: 1, status: 'FILLED' },
          { id: 2, status: 'REJECTED' },
        ])]
      ]));
      
      let row = results[results.length - 1][0];
      expect(row.fillRate).toBe(50);
      
      // Add more filled orders: should increase rate
      circuit.step(new Map([
        ['orders', ZSet.fromValues([
          { id: 3, status: 'FILLED' },
          { id: 4, status: 'FILLED' },
        ])]
      ]));
      
      row = results[results.length - 1][0];
      expect(row.total).toBe(4);
      expect(row.filled).toBe(3);
      expect(row.fillRate).toBe(75);
    });
  });
  
  describe('Aggregates wrapping CASE expressions', () => {
    it('should compute SUM(CASE WHEN condition THEN value ELSE 0 END)', () => {
      const result = compileSQL(`
        CREATE TABLE trades (id INT, pnl DECIMAL, direction VARCHAR, side VARCHAR);
        CREATE VIEW aligned_pnl AS
        SELECT 
          SUM(CASE WHEN (side = 'LONG' AND direction = 'LONG')
                   OR (side = 'SHORT' AND direction = 'SHORT')
               THEN pnl ELSE 0 END) AS alignedPnL,
          SUM(CASE WHEN (side = 'LONG' AND direction = 'SHORT')
                   OR (side = 'SHORT' AND direction = 'LONG')
               THEN pnl ELSE 0 END) AS misalignedPnL
        FROM trades
      `);
      
      const results: any[] = [];
      result.views.aligned_pnl.output((zset) => {
        for (const [row, weight] of zset.entries()) {
          if (weight > 0) results.push([row, weight]);
        }
      });
      
      // Push data
      result.circuit.step(new Map([
        ['trades', ZSet.fromValues([
          { id: 1, pnl: 1000, direction: 'LONG', side: 'LONG' },   // aligned
          { id: 2, pnl: 500, direction: 'SHORT', side: 'LONG' },  // misaligned
          { id: 3, pnl: 2000, direction: 'SHORT', side: 'SHORT' }, // aligned
          { id: 4, pnl: 300, direction: 'LONG', side: 'SHORT' },  // misaligned
        ])]
      ]));
      
      expect(results.length).toBeGreaterThan(0);
      const row = results[results.length - 1][0];
      
      // Aligned = 1000 + 2000 = 3000
      expect(row.alignedPnL).toBe(3000);
      // Misaligned = 500 + 300 = 800
      expect(row.misalignedPnL).toBe(800);
    });
  });
  
  describe('Aggregates only inside CASE expressions', () => {
    it('should compute risk-adjusted metrics with no top-level aggregates', () => {
      // This SQL pattern has NO top-level aggregates - all aggregates are inside CASE
      const result = compileSQL(`
        CREATE TABLE positions (id INT, pnl DECIMAL, dv01 DECIMAL, notional DECIMAL);
        CREATE VIEW risk_metrics AS
        SELECT 
          CASE WHEN SUM(ABS(dv01)) > 0 
               THEN SUM(pnl) / SUM(ABS(dv01)) 
               ELSE 0 END AS pnlPerDV01,
          CASE WHEN SUM(ABS(notional)) > 0 
               THEN (SUM(pnl) / SUM(ABS(notional))) * 10000 
               ELSE 0 END AS pnlPerNotional
        FROM positions
      `);
      
      const results: any[] = [];
      result.views.risk_metrics.output((zset) => {
        for (const [row, weight] of zset.entries()) {
          if (weight > 0) results.push([row, weight]);
        }
      });
      
      // Push data
      result.circuit.step(new Map([
        ['positions', ZSet.fromValues([
          { id: 1, pnl: 1000, dv01: 50, notional: 1000000 },
          { id: 2, pnl: -500, dv01: 25, notional: 500000 },
          { id: 3, pnl: 2000, dv01: 100, notional: 2000000 },
        ])]
      ]));
      
      // Verify results
      expect(results.length).toBeGreaterThan(0);
      const row = results[results.length - 1][0];
      
      // Total PnL = 1000 - 500 + 2000 = 2500
      // Total |DV01| = 50 + 25 + 100 = 175
      // pnlPerDV01 = 2500 / 175 = 14.2857...
      expect(row.pnlPerDV01).toBeCloseTo(14.29, 1);
      
      // Total |notional| = 1000000 + 500000 + 2000000 = 3500000
      // pnlPerNotional = (2500 / 3500000) * 10000 = 7.14 bps
      expect(row.pnlPerNotional).toBeCloseTo(7.14, 1);
    });
    
    it('should return ELSE value when aggregate condition is zero', () => {
      const result = compileSQL(`
        CREATE TABLE positions (id INT, pnl DECIMAL, dv01 DECIMAL);
        CREATE VIEW zero_test AS
        SELECT 
          CASE WHEN SUM(ABS(dv01)) > 0 
               THEN SUM(pnl) / SUM(ABS(dv01)) 
               ELSE 0 END AS pnlPerDV01
        FROM positions
      `);
      
      const results: any[] = [];
      result.views.zero_test.output((zset) => {
        for (const [row, weight] of zset.entries()) {
          if (weight > 0) results.push([row, weight]);
        }
      });
      
      // Push data with all zero dv01
      result.circuit.step(new Map([
        ['positions', ZSet.fromValues([
          { id: 1, pnl: 1000, dv01: 0 },
          { id: 2, pnl: -500, dv01: 0 },
        ])]
      ]));
      
      // Should return 0 (the ELSE value) since SUM(ABS(dv01)) = 0
      expect(results.length).toBeGreaterThan(0);
      const row = results[results.length - 1][0];
      expect(row.pnlPerDV01).toBe(0);
    });
  });
});
