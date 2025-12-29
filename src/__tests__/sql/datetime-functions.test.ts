/**
 * DATE/TIME/DATETIME/STRFTIME Function Tests
 * ==========================================
 * 
 * Tests for SQLite-compatible date and time functions.
 * Based on SQLite's documented behavior.
 */

import { describe, it, expect } from 'vitest';
import { SQLCompiler } from '../../sql/sql-compiler';
import { ZSet } from '../../internals/zset';

// Standard test helper following existing patterns
function compileSQL(sql: string) {
  const compiler = new SQLCompiler();
  return compiler.compile(sql);
}

describe('SQLite Date/Time Functions', () => {
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

  describe('DATE function', () => {
    it('should parse ISO date string', () => {
      const sql = `
        CREATE TABLE t (id INT, dt TEXT);
        CREATE VIEW v AS SELECT DATE(dt) AS d FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ id: 1, dt: '2023-12-25' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].d).toBe('2023-12-25');
    });

    it('should extract date from datetime', () => {
      const sql = `
        CREATE TABLE t (id INT, dt TEXT);
        CREATE VIEW v AS SELECT DATE(dt) AS d FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ id: 1, dt: '2023-12-25 14:30:45' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].d).toBe('2023-12-25');
    });

    it('should handle date arithmetic with modifiers', () => {
      const sql = `
        CREATE TABLE t (id INT, dt TEXT);
        CREATE VIEW v AS SELECT DATE(dt, '+1 day') AS tomorrow FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ id: 1, dt: '2023-12-25' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].tomorrow).toBe('2023-12-26');
    });

    it('should handle negative modifiers', () => {
      const sql = `
        CREATE TABLE t (id INT, dt TEXT);
        CREATE VIEW v AS SELECT DATE(dt, '-1 month') AS lastMonth FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ id: 1, dt: '2023-12-25' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].lastMonth).toBe('2023-11-25');
    });

    it('should handle start of month modifier', () => {
      const sql = `
        CREATE TABLE t (id INT, dt TEXT);
        CREATE VIEW v AS SELECT DATE(dt, 'start of month') AS firstOfMonth FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ id: 1, dt: '2023-12-25' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].firstOfMonth).toBe('2023-12-01');
    });

    it('should handle start of year modifier', () => {
      const sql = `
        CREATE TABLE t (id INT, dt TEXT);
        CREATE VIEW v AS SELECT DATE(dt, 'start of year') AS firstOfYear FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ id: 1, dt: '2023-12-25' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].firstOfYear).toBe('2023-01-01');
    });

    it('should chain multiple modifiers', () => {
      const sql = `
        CREATE TABLE t (id INT, dt TEXT);
        CREATE VIEW v AS SELECT DATE(dt, 'start of month', '+1 month', '-1 day') AS lastOfMonth FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ id: 1, dt: '2023-12-25' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].lastOfMonth).toBe('2023-12-31');
    });

    it('should handle multiple rows', () => {
      const sql = `
        CREATE TABLE events (id INT, event_date TEXT);
        CREATE VIEW v AS SELECT DATE(event_date) AS d FROM events;
      `;
      const results = executeQuery(sql, {
        events: [
          { id: 1, event_date: '2023-12-25 14:30:00' },
          { id: 2, event_date: '2024-01-15 09:00:00' }
        ]
      });
      expect(results.length).toBe(2);
      const dates = results.map((r: any) => r.d).sort();
      expect(dates).toContain('2023-12-25');
      expect(dates).toContain('2024-01-15');
    });
  });

  describe('TIME function', () => {
    it('should extract time from datetime', () => {
      const sql = `
        CREATE TABLE t (id INT, dt TEXT);
        CREATE VIEW v AS SELECT TIME(dt) AS tm FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ id: 1, dt: '2023-12-25 14:30:45' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].tm).toBe('14:30:45');
    });

    it('should parse time string', () => {
      const sql = `
        CREATE TABLE t (id INT, tm TEXT);
        CREATE VIEW v AS SELECT TIME(tm) AS result FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ id: 1, tm: '09:15:30' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].result).toBe('09:15:30');
    });

    it('should handle time arithmetic', () => {
      const sql = `
        CREATE TABLE t (id INT, tm TEXT);
        CREATE VIEW v AS SELECT TIME(tm, '+2 hours') AS later FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ id: 1, tm: '12:00:00' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].later).toBe('14:00:00');
    });

    it('should handle minute modifiers', () => {
      const sql = `
        CREATE TABLE t (id INT, tm TEXT);
        CREATE VIEW v AS SELECT TIME(tm, '+30 minutes') AS later FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ id: 1, tm: '12:00:00' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].later).toBe('12:30:00');
    });
  });

  describe('DATETIME function', () => {
    it('should format datetime string', () => {
      const sql = `
        CREATE TABLE t (id INT, dt TEXT);
        CREATE VIEW v AS SELECT DATETIME(dt) AS result FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ id: 1, dt: '2023-12-25T14:30:45' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].result).toBe('2023-12-25 14:30:45');
    });

    it('should handle datetime with modifiers', () => {
      const sql = `
        CREATE TABLE t (id INT, dt TEXT);
        CREATE VIEW v AS SELECT DATETIME(dt, '+1 day', '+6 hours') AS result FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ id: 1, dt: '2023-12-25 12:00:00' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].result).toBe('2023-12-26 18:00:00');
    });

    it('should handle start of day modifier', () => {
      const sql = `
        CREATE TABLE t (id INT, dt TEXT);
        CREATE VIEW v AS SELECT DATETIME(dt, 'start of day') AS result FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ id: 1, dt: '2023-12-25 14:30:45' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].result).toBe('2023-12-25 00:00:00');
    });
  });

  describe('STRFTIME function', () => {
    it('should format with %Y-%m-%d', () => {
      const sql = `
        CREATE TABLE t (id INT, dt TEXT);
        CREATE VIEW v AS SELECT STRFTIME('%Y-%m-%d', dt) AS formatted FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ id: 1, dt: '2023-12-25' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].formatted).toBe('2023-12-25');
    });

    it('should format with %H:%M:%S', () => {
      const sql = `
        CREATE TABLE t (id INT, dt TEXT);
        CREATE VIEW v AS SELECT STRFTIME('%H:%M:%S', dt) AS formatted FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ id: 1, dt: '2023-12-25 14:30:45' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].formatted).toBe('14:30:45');
    });

    it('should extract year with %Y', () => {
      const sql = `
        CREATE TABLE t (id INT, dt TEXT);
        CREATE VIEW v AS SELECT STRFTIME('%Y', dt) AS yr FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ id: 1, dt: '2023-12-25' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].yr).toBe('2023');
    });

    it('should extract month with %m', () => {
      const sql = `
        CREATE TABLE t (id INT, dt TEXT);
        CREATE VIEW v AS SELECT STRFTIME('%m', dt) AS mo FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ id: 1, dt: '2023-12-25' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].mo).toBe('12');
    });

    it('should extract day with %d', () => {
      const sql = `
        CREATE TABLE t (id INT, dt TEXT);
        CREATE VIEW v AS SELECT STRFTIME('%d', dt) AS dy FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ id: 1, dt: '2023-12-25' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].dy).toBe('25');
    });

    it('should get day of week with %w', () => {
      const sql = `
        CREATE TABLE t (id INT, dt TEXT);
        CREATE VIEW v AS SELECT STRFTIME('%w', dt) AS dow FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ id: 1, dt: '2023-12-25' }]
      });
      expect(results.length).toBe(1);
      // December 25, 2023 is a Monday
      expect(results[0].dow).toBe('1');
    });

    it('should get Unix timestamp with %s', () => {
      const sql = `
        CREATE TABLE t (id INT, dt TEXT);
        CREATE VIEW v AS SELECT STRFTIME('%s', dt) AS epoch FROM t;
      `;
      const results = executeQuery(sql, {
        // Use a date well past epoch to avoid timezone issues
        t: [{ id: 1, dt: '2000-01-01 12:00:00' }]
      });
      expect(results.length).toBe(1);
      // 2000-01-01 12:00:00 UTC = 946728000
      const epoch = parseInt(results[0].epoch, 10);
      // Allow some timezone variance
      expect(epoch).toBeGreaterThan(946684800);
      expect(epoch).toBeLessThan(946771200);
    });

    it('should handle literal percent with %%', () => {
      const sql = `
        CREATE TABLE t (id INT, dt TEXT);
        CREATE VIEW v AS SELECT STRFTIME('100%%', dt) AS pct FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ id: 1, dt: '2023-12-25' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].pct).toBe('100%');
    });

    it('should handle combined format specifiers', () => {
      const sql = `
        CREATE TABLE t (id INT, dt TEXT);
        CREATE VIEW v AS SELECT STRFTIME('%Y/%m/%d %H:%M', dt) AS formatted FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ id: 1, dt: '2023-12-25 14:30:45' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].formatted).toBe('2023/12/25 14:30');
    });

    it('should apply modifiers before formatting', () => {
      const sql = `
        CREATE TABLE t (id INT, dt TEXT);
        CREATE VIEW v AS SELECT STRFTIME('%Y-%m-%d', dt, '+1 month') AS formatted FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ id: 1, dt: '2023-12-25' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].formatted).toBe('2024-01-25');
    });
  });

  describe('JULIANDAY function', () => {
    it('should compute Julian day for a date', () => {
      const sql = `
        CREATE TABLE t (id INT, dt TEXT);
        CREATE VIEW v AS SELECT JULIANDAY(dt) AS jd FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ id: 1, dt: '2000-01-01' }]
      });
      expect(results.length).toBe(1);
      // Julian day for 2000-01-01 00:00:00 is approximately 2451544.5
      expect(results[0].jd).toBeCloseTo(2451544.5, 1);
    });

    it('should compute Julian day for datetime', () => {
      const sql = `
        CREATE TABLE t (id INT, dt TEXT);
        CREATE VIEW v AS SELECT JULIANDAY(dt) AS jd FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ id: 1, dt: '2000-01-01 12:00:00' }]
      });
      expect(results.length).toBe(1);
      // At noon, the Julian day is exactly 2451545
      expect(results[0].jd).toBeCloseTo(2451545, 1);
    });

    it('should handle modifiers', () => {
      const sql = `
        CREATE TABLE t (id INT, dt TEXT);
        CREATE VIEW v AS SELECT JULIANDAY(dt, '+1 day') AS jd FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ id: 1, dt: '2000-01-01' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].jd).toBeCloseTo(2451545.5, 1);
    });
  });

  describe('UNIXEPOCH function', () => {
    it('should compute Unix timestamp correctly', () => {
      const sql = `
        CREATE TABLE t (id INT, dt TEXT);
        CREATE VIEW v AS SELECT UNIXEPOCH(dt) AS epoch FROM t;
      `;
      const results = executeQuery(sql, {
        // Use noon to avoid edge cases
        t: [{ id: 1, dt: '2000-01-01 12:00:00' }]
      });
      expect(results.length).toBe(1);
      // 2000-01-01 12:00:00 UTC is 946728000 seconds since epoch
      // Allow some timezone variance
      expect(results[0].epoch).toBeGreaterThan(946684800);
      expect(results[0].epoch).toBeLessThan(946771200);
    });

    it('should handle relative timestamps', () => {
      const sql = `
        CREATE TABLE t (id INT, dt TEXT);
        CREATE VIEW v AS SELECT UNIXEPOCH(dt) AS epoch FROM t;
      `;
      // Use two timestamps and check the difference
      const results = executeQuery(sql, {
        t: [
          { id: 1, dt: '2000-01-01 12:00:00' },
          { id: 2, dt: '2000-01-02 12:00:00' }
        ]
      });
      expect(results.length).toBe(2);
      const epochs = results.map((r: any) => r.epoch).sort((a: number, b: number) => a - b);
      // Difference should be exactly 86400 (one day)
      expect(epochs[1] - epochs[0]).toBe(86400);
    });

    it('should handle modifiers by adding time', () => {
      const sql = `
        CREATE TABLE t (id INT, dt TEXT);
        CREATE VIEW v AS SELECT UNIXEPOCH(dt) AS epoch, UNIXEPOCH(dt, '+1 day') AS epochPlusOne FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ id: 1, dt: '2000-01-15 12:00:00' }]
      });
      expect(results.length).toBe(1);
      // The difference should be exactly 86400 seconds (one day)
      expect(results[0].epochPlusOne - results[0].epoch).toBe(86400);
    });
  });

  describe('Date functions with table data', () => {
    it('should compute Julian day for different dates', () => {
      const sql = `
        CREATE TABLE people (name TEXT, birth_date TEXT);
        CREATE VIEW v AS 
          SELECT name, 
                 JULIANDAY(birth_date) AS jd
          FROM people;
      `;
      const results = executeQuery(sql, {
        people: [
          { name: 'Alice', birth_date: '2000-01-01' },
          { name: 'Bob', birth_date: '1990-06-15' }
        ]
      });
      expect(results.length).toBe(2);
      const alice = results.find((r: any) => r.name === 'Alice');
      const bob = results.find((r: any) => r.name === 'Bob');
      // Julian days should be valid values around 2.4 million
      expect(alice.jd).toBeGreaterThan(2400000);
      expect(bob.jd).toBeGreaterThan(2400000);
      // Bob should have a smaller Julian day (earlier date)
      expect(bob.jd).toBeLessThan(alice.jd);
    });

    it('should project date functions', () => {
      const sql = `
        CREATE TABLE orders (id INT, order_date TEXT);
        CREATE VIEW v AS 
          SELECT id, DATE(order_date) AS order_day FROM orders;
      `;
      const results = executeQuery(sql, {
        orders: [
          { id: 1, order_date: '2022-12-31 23:59:59' },
          { id: 2, order_date: '2023-06-15 10:30:00' },
          { id: 3, order_date: '2024-01-01 00:00:00' }
        ]
      });
      expect(results.length).toBe(3);
      const order1 = results.find((r: any) => r.id === 1);
      const order2 = results.find((r: any) => r.id === 2);
      const order3 = results.find((r: any) => r.id === 3);
      expect(order1?.order_day).toBe('2022-12-31');
      expect(order2?.order_day).toBe('2023-06-15');
      expect(order3?.order_day).toBe('2024-01-01');
    });

    it('should aggregate with date columns', () => {
      const sql = `
        CREATE TABLE sales (amount DECIMAL, sale_month TEXT);
        CREATE VIEW v AS 
          SELECT sale_month, 
                 SUM(amount) AS total
          FROM sales
          GROUP BY sale_month;
      `;
      const results = executeQuery(sql, {
        sales: [
          { amount: 100, sale_month: '2023-01' },
          { amount: 200, sale_month: '2023-01' },
          { amount: 150, sale_month: '2023-02' }
        ]
      });
      expect(results.length).toBe(2);
      const jan = results.find((r: any) => r.sale_month === '2023-01');
      const feb = results.find((r: any) => r.sale_month === '2023-02');
      expect(jan?.total).toBe(300);
      expect(feb?.total).toBe(150);
    });
  });

  describe('Edge cases', () => {
    it('should handle NULL input', () => {
      const sql = `
        CREATE TABLE t (dt TEXT);
        CREATE VIEW v AS SELECT DATE(dt) AS d FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ dt: null }]
      });
      expect(results.length).toBe(1);
      expect(results[0].d).toBeNull();
    });

    it('should handle ISO format with T separator', () => {
      const sql = `
        CREATE TABLE t (id INT, dt TEXT);
        CREATE VIEW v AS SELECT DATE(dt) AS d FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ id: 1, dt: '2023-12-25T14:30:45Z' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].d).toBe('2023-12-25');
    });

    it('should handle year crossing with modifiers', () => {
      const sql = `
        CREATE TABLE t (id INT, dt TEXT);
        CREATE VIEW v AS SELECT DATE(dt, '+1 day') AS d FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ id: 1, dt: '2023-12-31' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].d).toBe('2024-01-01');
    });

    it('should handle leap year', () => {
      const sql = `
        CREATE TABLE t (id INT, dt TEXT);
        CREATE VIEW v AS SELECT DATE(dt, '+1 day') AS d FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ id: 1, dt: '2024-02-28' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].d).toBe('2024-02-29');
    });

    it('should handle weekday modifier', () => {
      // 2023-12-25 is Monday (weekday 1), so weekday 0 (Sunday) should be 2023-12-31
      const sql = `
        CREATE TABLE t (id INT, dt TEXT);
        CREATE VIEW v AS SELECT DATE(dt, 'weekday 0') AS d FROM t;
      `;
      const results = executeQuery(sql, {
        t: [{ id: 1, dt: '2023-12-25' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].d).toBe('2023-12-31');
    });
  });
});
