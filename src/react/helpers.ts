/**
 * DBSP Helper Utilities
 * =====================
 * 
 * Convenience functions for common DBSP patterns.
 */

import type { ViewStats } from '../core/types';

// ═══════════════════════════════════════════════════════════════════════════════
// SQL QUERY BUILDERS - Type-safe query construction
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Build a SELECT query with optional clauses.
 * 
 * @example
 * ```tsx
 * const query = buildSelect({
 *   columns: ['id', 'name', 'SUM(amount) as total'],
 *   from: 'orders',
 *   where: "status = 'pending'",
 *   groupBy: ['name'],
 *   orderBy: 'total DESC',
 *   limit: 10
 * });
 * // "SELECT id, name, SUM(amount) as total FROM orders WHERE status = 'pending' GROUP BY name ORDER BY total DESC LIMIT 10"
 * ```
 */
export function buildSelect(opts: {
  columns: string[] | '*';
  from: string;
  where?: string;
  groupBy?: string[];
  having?: string;
  orderBy?: string;
  limit?: number;
  offset?: number;
}): string {
  const cols = opts.columns === '*' ? '*' : opts.columns.join(', ');
  let sql = `SELECT ${cols} FROM ${opts.from}`;
  
  if (opts.where) sql += ` WHERE ${opts.where}`;
  if (opts.groupBy?.length) sql += ` GROUP BY ${opts.groupBy.join(', ')}`;
  if (opts.having) sql += ` HAVING ${opts.having}`;
  if (opts.orderBy) sql += ` ORDER BY ${opts.orderBy}`;
  if (opts.limit !== undefined) sql += ` LIMIT ${opts.limit}`;
  if (opts.offset !== undefined) sql += ` OFFSET ${opts.offset}`;
  
  return sql;
}


// ═══════════════════════════════════════════════════════════════════════════════
// PERFORMANCE UTILITIES - Analyze and format stats
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Performance classification for a view.
 */
export type PerformanceLevel = 'fast' | 'good' | 'slow' | 'bottleneck';

/**
 * Classify view performance based on update time.
 * 
 * @example
 * ```tsx
 * const level = getPerformanceLevel(view.stats);
 * // Returns 'fast', 'good', 'slow', or 'bottleneck'
 * ```
 */
export function getPerformanceLevel(stats: ViewStats): PerformanceLevel {
  const ms = stats.lastFullProcessMs ?? stats.lastUpdateMs;
  if (ms < 1) return 'fast';
  if (ms < 5) return 'good';
  if (ms < 10) return 'slow';
  return 'bottleneck';
}

/**
 * Format milliseconds for display.
 * 
 * @example
 * ```tsx
 * formatMs(0.5)    // "500μs"
 * formatMs(1.5)    // "1.50ms"
 * formatMs(1500)   // "1.50s"
 * ```
 */
export function formatMs(ms: number): string {
  if (ms < 1) return `${Math.round(ms * 1000)}μs`;
  if (ms < 1000) return `${ms.toFixed(2)}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
}

/**
 * Format bytes for display.
 * 
 * @example
 * ```tsx
 * formatBytes(1024)       // "1.0 KB"
 * formatBytes(1048576)    // "1.0 MB"
 * ```
 */
export function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
  return `${(bytes / 1024 / 1024 / 1024).toFixed(1)} GB`;
}


// ═══════════════════════════════════════════════════════════════════════════════
// TYPE UTILITIES - Common type definitions
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Extract the row type from a source handle.
 * 
 * @example
 * ```tsx
 * const orders = useDBSPSource<Order>({ name: 'orders', key: 'id' });
 * type OrderRow = SourceRow<typeof orders>;  // Order
 * ```
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type SourceRow<S> = S extends { push: (rows: infer T | (infer T)[]) => void } ? T : never;

/**
 * Extract the result type from a view handle.
 * 
 * @example
 * ```tsx
 * const totals = useDBSPView(orders, "SELECT status, SUM(amount) FROM orders GROUP BY status");
 * type TotalRow = ViewResult<typeof totals>;  // { status: string; sum_amount: number }
 * ```
 */
export type ViewResult<V> = V extends { results: (infer T)[] } ? T : never;


