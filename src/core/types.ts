/**
 * DBSP Core Types
 * ================
 * 
 * This module defines the public API for DBSP's incremental computation engine.
 * 
 * ## Quick Start
 * 
 * ```tsx
 * // 1. Create a data source
 * const orders = useDBSPSource<Order>({ name: 'orders', key: 'orderId' });
 * 
 * // 2. Push data to it
 * orders.push({ orderId: 1, amount: 100, status: 'pending' });
 * 
 * // 3. Create SQL views that update incrementally
 * const pendingOrders = useDBSPView(orders, "SELECT * FROM orders WHERE status = 'pending'");
 * const totalByStatus = useDBSPView(orders, "SELECT status, SUM(amount) FROM orders GROUP BY status");
 * 
 * // 4. Views can feed into other views! (chained views)
 * const highValuePending = useDBSPView(pendingOrders, 
 *   "SELECT * FROM pendingOrders WHERE amount > 1000"
 * );
 * 
 * // Views automatically update when source data changes!
 * ```
 * 
 * @module
 */

// ═══════════════════════════════════════════════════════════════════════════════
// STREAM API - Unified interface for sources and views (both produce delta streams)
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Internal interface for anything that can be used as a data stream input.
 * Both sources and views implement this, enabling views to chain into other views.
 * 
 * @internal This is used by useDBSPView to accept either sources or views as inputs.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export interface DBSPStreamHandle<T extends Record<string, any>> {
  /** Stream name (used as table name in SQL) */
  readonly name: string;
  
  /** True when the stream is ready to be queried */
  readonly ready: boolean;
  
  /** @internal Unique identity - changes when source is recreated (for cache invalidation) */
  readonly _identity: string;
  
  /** @internal Schema string for SQL compilation */
  _getSchema(): string | null;
  
  /** 
   * @internal Key function for row identity.
   * Uses `any` parameter to enable bivariant function assignment.
   * Without this, TypeScript's contravariance for function parameters would prevent
   * DBSPStreamHandle<SpecificType> from being assignable to DBSPStreamHandle<Record<string, any>>.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  _getKeyFn(): (row: any) => string;
  
  /** @internal Current integrated data (full state) */
  _getData(): Map<string, T>;
  
  /** @internal Subscribe to delta changes */
  _subscribe(callback: (delta: Array<[T, number]>) => void): () => void;
}

// ═══════════════════════════════════════════════════════════════════════════════
// SOURCE API - Create and manage data sources
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Options for creating a DBSP data source.
 * 
 * @example Basic usage
 * ```tsx
 * const orders = useDBSPSource<Order>({
 *   name: 'orders',   // Table name in SQL queries
 *   key: 'orderId',   // Primary key for upserts
 * });
 * ```
 * 
 * @example High-volume streaming data
 * ```tsx
 * const trades = useDBSPSource<Trade>({
 *   name: 'trades',
 *   key: 'tradeId',
 *   maxRows: 10000,  // Keep only last 10K rows to prevent memory explosion
 * });
 * ```
 */
export interface DBSPSourceOptions<T> {
  /**
   * Table name used in SQL queries.
   * 
   * @example
   * ```tsx
   * const orders = useDBSPSource({ name: 'orders', key: 'id' });
   * // Now you can query: "SELECT * FROM orders WHERE ..."
   * ```
   */
  name: string;
  
  /**
   * Primary key for upserting rows.
   * 
   * Can be:
   * - A single field name: `'orderId'`
   * - Multiple fields: `['customerId', 'productId']` (composite key)
   * - A function: `(row) => \`\${row.a}:\${row.b}\`` (custom key)
   */
  key: keyof T | (keyof T)[] | ((row: T) => string);
  
  /**
   * Maximum rows to keep in memory (oldest evicted first).
   * 
   * **IMPORTANT**: Set this for high-volume streaming data to prevent
   * memory exhaustion. Leave undefined for small reference datasets.
   * 
   * @example
   * ```tsx
   * // High-volume: keep only recent data
   * const events = useDBSPSource({ name: 'events', key: 'id', maxRows: 50000 });
   * 
   * // Reference data: keep everything
   * const products = useDBSPSource({ name: 'products', key: 'sku' });
   * ```
   */
  maxRows?: number;
  
  /**
   * Enable console logging for debugging.
   * @default false
   */
  debug?: boolean;
  
  /**
   * Advanced: Configure backpressure handling for bursty data.
   * Most users don't need this.
   */
  freshness?: FreshnessConfig;
  
  /**
   * Advanced: Offload processing to a Web Worker.
   * Use for extreme throughput (100K+ rows/second).
   */
  worker?: WorkerConfig;
}

/**
 * Handle returned by useDBSPSource for interacting with a data source.
 * 
 * Sources implement `DBSPStreamHandle`, so they can be used as inputs to views.
 * 
 * @example
 * ```tsx
 * const orders = useDBSPSource<Order>({ name: 'orders', key: 'orderId' });
 * 
 * // Push initial data or updates
 * orders.push([{ orderId: 1, amount: 100 }, { orderId: 2, amount: 200 }]);
 * 
 * // Update a specific row (upsert by key)
 * orders.push({ orderId: 1, amount: 150 });
 * 
 * // Remove a row
 * orders.remove(1);
 * 
 * // Clear all data
 * orders.clear();
 * ```
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export interface DBSPSourceHandle<T extends Record<string, any>> extends DBSPStreamHandle<T> {
  /** Current row count in the source */
  readonly totalRows: number;
  
  /** Performance statistics */
  readonly stats: SourceStats;
  
  /**
   * Add or update rows. Existing rows with matching keys are replaced.
   * 
   * @param rows - Single row or array of rows to upsert
   * 
   * @example
   * ```tsx
   * // Single row
   * orders.push({ orderId: 1, amount: 100 });
   * 
   * // Batch (more efficient)
   * orders.push([order1, order2, order3]);
   * ```
   */
  push(rows: T | T[]): void;
  
  /**
   * Remove rows by their key value(s).
   * 
   * @param keyValues - Key value(s) to remove
   * 
   * @example
   * ```tsx
   * orders.remove(1);           // Remove order with id=1
   * orders.remove(1, 2, 3);     // Remove multiple
   * ```
   */
  remove(...keyValues: unknown[]): void;
  
  /** Clear all data from this source */
  clear(): void;
}


// ═══════════════════════════════════════════════════════════════════════════════
// VIEW API - Create SQL views over sources
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Options for creating a DBSP view.
 * 
 * @example Basic aggregation
 * ```tsx
 * const totals = useDBSPView(orders, 
 *   "SELECT status, SUM(amount) AS total FROM orders GROUP BY status",
 *   { outputKey: 'status' }  // Key for the output rows
 * );
 * ```
 * 
 * @example Time-series with bounded memory
 * ```tsx
 * const recentPrices = useDBSPView(prices,
 *   "SELECT * FROM prices ORDER BY ts DESC LIMIT 100",
 *   { maxRows: 100 }  // Keep only 100 most recent
 * );
 * ```
 */
export interface DBSPViewOptions {
  /**
   * Name for this view (used as table name when this view feeds into another view).
   * 
   * If not provided, a name is auto-generated. You MUST provide a name if you
   * want to use this view as input to another view.
   * 
   * @example
   * ```tsx
   * // Named view that can be used as input
   * const pending = useDBSPView(orders, 
   *   "SELECT * FROM orders WHERE status = 'pending'",
   *   { name: 'pending' }
   * );
   * 
   * // Now use 'pending' as a table name in another view
   * const highValue = useDBSPView(pending,
   *   "SELECT * FROM pending WHERE amount > 1000"
   * );
   * ```
   */
  name?: string;
  
  /**
   * Primary key for output rows.
   * 
   * Use this when your output has a different shape than the input,
   * like in aggregations where the group-by column becomes the key.
   * 
   * @example
   * ```tsx
   * // Input: orders with orderId
   * // Output: aggregation with status as the key
   * const byStatus = useDBSPView(orders,
   *   "SELECT status, COUNT(*) FROM orders GROUP BY status",
   *   { outputKey: 'status' }
   * );
   * ```
   */
  outputKey?: string | string[] | ((row: Record<string, unknown>) => string);
  
  /**
   * Maximum rows to keep in the view result set.
   * 
   * Use with `ORDER BY ... LIMIT` queries to prevent unbounded growth.
   * When exceeded, oldest rows are pruned (FIFO).
   */
  maxRows?: number;
  
  /**
   * Join behavior for multi-source queries.
   * 
   * - `'append-only'` (default): 3000x faster, only tracks count (no row storage)
   * - `'full'`: Stores joined rows in memory (up to maxResults)
   * - `'full-indexed'`: Stores joined rows in IndexedDB (unlimited, disk-backed)
   * 
   * @default 'append-only'
   */
  joinMode?: JoinMode;
  
  /**
   * Maximum joined results to store in 'full' mode.
   * Prevents memory explosion with high-cardinality joins.
   * 
   * @default 10000
   */
  maxResults?: number;
  
  /** Enable console logging for debugging */
  debug?: boolean;
  
  /**
   * Advanced: Offload SQL circuit execution to a Web Worker.
   * 
   * When enabled, the SQL parsing, circuit compilation, and incremental
   * computation all happen in a separate thread, keeping the main thread
   * free for UI updates.
   * 
   * Best for:
   * - Complex queries with many operators
   * - High-frequency updates (>100 updates/second)
   * - Large result sets
   * 
   * @example
   * ```tsx
   * const heavy = useDBSPView(source, complexQuery, {
   *   worker: { enabled: true }
   * });
   * ```
   */
  worker?: WorkerConfig;
}

/**
 * Handle returned by useDBSPView for reading query results.
 * 
 * Views implement `DBSPStreamHandle`, so they can be used as inputs to other views!
 * This enables powerful view chaining: source → view → view → view
 * 
 * @example Basic usage
 * ```tsx
 * const pendingOrders = useDBSPView(orders, "SELECT * FROM orders WHERE status = 'pending'");
 * 
 * // Read results
 * console.log(pendingOrders.results);  // Array of matching rows
 * console.log(pendingOrders.count);    // Number of results
 * 
 * // Check performance
 * console.log(pendingOrders.stats.lastUpdateMs);  // Last query time in ms
 * ```
 * 
 * @example Chained views (view as input to another view)
 * ```tsx
 * // First view: filter pending orders
 * const pending = useDBSPView(orders, "SELECT * FROM orders WHERE status = 'pending'", { name: 'pending' });
 * 
 * // Second view: aggregate pending orders by customer
 * const pendingByCustomer = useDBSPView(pending, 
 *   "SELECT customerId, SUM(amount) as total FROM pending GROUP BY customerId"
 * );
 * ```
 */
export interface DBSPViewHandle<TOut extends Record<string, unknown>> extends DBSPStreamHandle<TOut> {
  /** Current query results (automatically updates when source data changes) */
  readonly results: TOut[];
  
  /** Number of results (always accurate, even when storage is limited) */
  readonly count: number;
  
  /** Performance and memory statistics */
  readonly stats: ViewStats;
  
  /**
   * Get paginated results (only for 'full-indexed' join mode).
   * Returns empty array for other modes.
   */
  getResultsPage?(offset: number, limit: number): Promise<TOut[]>;
  
  /**
   * Get all results from IndexedDB (only for 'full-indexed' join mode).
   * @warning May cause memory issues for very large result sets!
   */
  getAllResults?(): Promise<TOut[]>;
}


// ═══════════════════════════════════════════════════════════════════════════════
// STATISTICS TYPES - Performance monitoring
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Statistics for a data source.
 */
export interface SourceStats {
  /** Time of last push operation (ms) */
  lastUpdateMs: number;
  /** Total number of push operations */
  totalUpdates: number;
  /** Current row count */
  totalRows: number;
  /** Average push time (ms) */
  avgUpdateMs: number;
  
  // Backpressure stats (only when freshness config is used)
  /** Current buffer size */
  bufferSize?: number;
  /** Maximum buffer capacity */
  bufferCapacity?: number;
  /** Buffer utilization (0-1) */
  bufferUtilization?: number;
  /** Current lag in ms */
  lagMs?: number;
  /** Rows dropped due to buffer overflow */
  droppedOverflow?: number;
  /** Rows dropped due to staleness */
  droppedStale?: number;
  /** Total rows dropped */
  totalDropped?: number;
  /** True if processing is falling behind */
  isLagging?: boolean;
}

/**
 * Statistics for a view.
 */
export interface ViewStats {
  // ─────── Core Metrics ───────
  /** Last circuit step time (ms) - SQL execution time */
  lastUpdateMs: number;
  /** Total number of updates processed */
  totalUpdates: number;
  /** Average update time (ms) */
  avgUpdateMs: number;
  
  // ─────── Detailed Timing ───────
  /** Full processing time including output integration (ms) */
  lastFullProcessMs?: number;
  /** Average full processing time (ms) */
  avgFullProcessMs?: number;
  /** Output callback time (ms) */
  lastOutputMs?: number;
  /** Pure circuit time without output (ms) */
  lastCircuitMs?: number;
  /** Time spent pruning old rows (ms) */
  lastPruneMs?: number;
  
  // ─────── Row Counts ───────
  /** Rows processed in last delta */
  lastDeltaRows?: number;
  /** Rows pruned in last update */
  lastPrunedRows?: number;
  /** Current row count */
  currentRowCount?: number;
  /** Peak row count seen */
  peakRowCount?: number;
  
  // ─────── Memory ───────
  /** Estimated memory usage (bytes) */
  estimatedMemoryBytes?: number;
  /** Peak memory usage (bytes) */
  peakMemoryBytes?: number;
  /** Memory growth rate (bytes/second) */
  memoryGrowthRate?: number;
  /** When memory tracking started */
  memoryTrackingStartMs?: number;
  
  // ─────── Limits ───────
  /** True if result limit was reached (full join mode) */
  resultLimitReached?: boolean;
  /** Max results configured */
  maxResults?: number;
  /** Max rows configured */
  maxRows?: number;
  /** Storage type: 'memory' | 'indexeddb' | 'count-only' */
  storageType?: 'memory' | 'indexeddb' | 'count-only';
}


// ═══════════════════════════════════════════════════════════════════════════════
// ADVANCED CONFIGURATION - For power users
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Join optimization mode.
 * 
 * - `'append-only'`: Fastest (3000x), only tracks count, no row storage
 * - `'full'`: Stores results in memory (limited by maxResults)
 * - `'full-indexed'`: Stores results in IndexedDB (unlimited, disk-backed)
 */
export type JoinMode = 'append-only' | 'full' | 'full-indexed';

/**
 * Backpressure handling for high-volume streams.
 * 
 * When data arrives faster than it can be processed, this config
 * determines how to handle the overflow.
 */
export interface FreshnessConfig {
  /** Maximum pending messages before dropping */
  maxBufferSize?: number;
  /** Maximum message age (ms) before dropping as stale */
  maxMessageAgeMs?: number;
  /** Maximum rows to process per batch */
  maxBatchSize?: number;
  /** How often to process batches (ms) */
  processingIntervalMs?: number;
  /** Callback when data is dropped */
  onDrop?: (count: number, reason: 'overflow' | 'stale') => void;
}

/**
 * Web Worker configuration for CPU-intensive processing.
 * 
 * Use when processing 100K+ rows per second and main thread
 * responsiveness is critical.
 */
export interface WorkerConfig {
  /** Enable Web Worker for this source */
  enabled: boolean;
  /** Number of worker threads (default: 1) */
  poolSize?: number;
  /** Batch size for worker messages (default: 5000) */
  batchSize?: number;
}
