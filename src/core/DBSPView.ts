/**
 * DBSPView - Platform-Agnostic Incremental SQL View
 * ==================================================
 * 
 * This is the core view implementation that handles all SQL computation.
 * It's framework-agnostic - React integration is in react/useDBSPView.ts.
 * 
 * ## Features
 * - Incremental SQL execution via DBSP circuits
 * - Optimized hash-indexed JOINs (50-100x faster than naive)
 * - Window functions (LAG, LEAD, ROW_NUMBER, SUM, AVG, etc.)
 * - Memory management with maxRows pruning
 * - View chaining (views can be inputs to other views)
 * 
 * ## Usage (Vanilla JS)
 * 
 * ```ts
 * import { DBSPView } from 'dbsp/core';
 * 
 * const view = new DBSPView({
 *   sources: [ordersSource],
 *   query: "SELECT * FROM orders WHERE status = 'pending'",
 * });
 * 
 * // Subscribe to changes
 * view.subscribe(state => {
 *   console.log('Results:', state.results);
 * });
 * 
 * // Cleanup
 * view.dispose();
 * ```
 * 
 * ## Usage (React - via react/useDBSPView.ts)
 * 
 * ```tsx
 * import { useDBSPView } from 'dbsp/react';
 * 
 * const pending = useDBSPView(orders, "SELECT * FROM orders WHERE status = 'pending'");
 * ```
 */

import type { DBSPStreamHandle, DBSPViewOptions, ViewStats, JoinMode } from './types';
import { ZSet } from '../internals/zset';
import { SQLCompiler } from '../sql/sql-compiler';
import { Circuit, StreamHandle } from '../internals/circuit';
import { OptimizedJoinState, AppendOnlyJoinState } from '../joins/optimized-join';
import { JoinResultStorage } from '../joins/join-storage';
import { dbspStore } from './store';
import { parseJoinProjection, createJoinProjector, type JoinProjection } from '../sql/join-projection';
import { dbspRegistry, type OperatorInfo } from './registry';

// ============ TYPES ============

export interface DBSPViewConfig<TOut extends Record<string, unknown>> {
  /** Array of source streams (sources or other views) */
  sources: DBSPStreamHandle<Record<string, unknown>>[];
  /** SQL query string */
  query: string;
  /** View name (used as table name when chaining) */
  name?: string;
  /** Output key for row identity */
  outputKey?: string | string[] | ((row: TOut) => string);
  /** Join mode for multi-source queries */
  joinMode?: JoinMode;
  /** Maximum results for 'full' join mode */
  maxResults?: number;
  /** Maximum rows to keep */
  maxRows?: number;
  /** Enable debug logging */
  debug?: boolean;
  /** Unique ID for this view (for IndexedDB) */
  viewId?: string;
}

export interface DBSPViewState<TOut> {
  results: TOut[];
  count: number;
  stats: ViewStats;
  ready: boolean;
}

type ViewListener<TOut> = (state: DBSPViewState<TOut>) => void;

interface JoinConfig {
  leftSource: string;
  rightSource: string;
  leftJoinKey: string;
  rightJoinKey: string;
  projection: JoinProjection;
  projector: (left: Record<string, unknown>, right: Record<string, unknown>) => Record<string, unknown>;
}

// ============ JOIN DETECTION ============

function detectJoin(query: string, sourceNames: string[]): JoinConfig | null {
  if (sourceNames.length !== 2) return null;
  
  const upperQuery = query.toUpperCase();
  if (!upperQuery.includes(' JOIN ')) return null;
  
  const onMatch = query.match(/\bON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)/i);
  if (!onMatch) return null;
  
  const [, table1, col1, table2, col2] = onMatch;
  const [leftName, rightName] = sourceNames;
  
  let leftJoinKey: string;
  let rightJoinKey: string;
  
  if (table1.toLowerCase() === leftName.toLowerCase()) {
    leftJoinKey = col1;
    rightJoinKey = col2;
  } else if (table2.toLowerCase() === leftName.toLowerCase()) {
    leftJoinKey = col2;
    rightJoinKey = col1;
  } else {
    return null;
  }
  
  const projection = parseJoinProjection(query, leftName, rightName);
  const projector = createJoinProjector<Record<string, unknown>, Record<string, unknown>, Record<string, unknown>>(projection);
  
  return {
    leftSource: leftName,
    rightSource: rightName,
    leftJoinKey,
    rightJoinKey,
    projection,
    projector,
  };
}

// ============ DBSP VIEW CLASS ============

/**
 * Platform-agnostic incremental SQL view.
 * 
 * Handles all SQL computation logic without any framework dependencies.
 * Use with React via useDBSPView, or directly in vanilla JS.
 */
// Instance counter for debugging
let dbspViewInstanceCounter = 0;

export class DBSPView<
  TIn extends Record<string, unknown>,
  TOut extends Record<string, unknown> = TIn
> implements DBSPStreamHandle<TOut> {
  
  // ============ CONFIGURATION ============
  private config: DBSPViewConfig<TOut>;
  private sourceNames: string[];
  private joinConfig: JoinConfig | null;
  private isJoinQuery: boolean;
  private _instanceId: number;
  
  // ============ STATE ============
  private _ready = false;
  private _name: string;
  
  // Circuit state (for non-JOIN queries)
  private circuit: Circuit | null = null;
  private circuitViews: Record<string, StreamHandle<unknown>> = {};
  private integratedData = new Map<string, { row: TOut; weight: number; index: number }>();
  private resultsArray: TOut[] = [];
  private freeIndices: number[] = [];
  private cachedResults: TOut[] | null = null;
  
  // Join state
  private joinState: AppendOnlyJoinState<Record<string, unknown>, Record<string, unknown>> | 
                     OptimizedJoinState<Record<string, unknown>, Record<string, unknown>> | null = null;
  private joinResults: { results: TOut[]; count: number; dirty: boolean } = { 
    results: [], count: 0, dirty: true 
  };
  // Track which join result keys have been emitted to downstream views (for incremental delta propagation)
  private emittedJoinKeys = new Set<string>();
  private indexedStorage: JoinResultStorage<Record<string, unknown>, Record<string, unknown>> | null = null;
  
  // Batched join queue
  private pendingJoinUpdates: { left: Record<string, unknown>[]; right: Record<string, unknown>[] } = { left: [], right: [] };
  private joinQueueTimeout: ReturnType<typeof setTimeout> | null = null;
  private readonly JOIN_BATCH_INTERVAL_MS = 16;
  
  // Stats
  private statsData = {
    lastUpdateMs: 0,
    totalUpdates: 0,
    avgUpdateMs: 0,
    updateTimes: new Float64Array(100),
    updateTimesIndex: 0,
    updateTimesCount: 0,
    lastFullProcessMs: 0,
    avgFullProcessMs: 0,
    fullProcessTimes: new Float64Array(100),
    fullProcessIndex: 0,
    fullProcessCount: 0,
    lastOutputMs: 0,
    lastCircuitMs: 0,
    lastPruneMs: 0,
    lastPrunedRows: 0,
    lastDeltaRows: 0,
    currentRowCount: 0,
    peakRowCount: 0,
    estimatedMemoryBytes: 0,
    peakMemoryBytes: 0,
    memoryTrackingStartMs: Date.now(),
    lastMemoryBytes: 0,
    memoryGrowthRate: 0,
    memorySamples: new Float64Array(60),
    memorySamplesIndex: 0,
    memorySamplesCount: 0,
    lastMemorySampleTime: Date.now(),
  };
  
  // Subscribers
  private listeners = new Set<ViewListener<TOut>>();
  private unsubscribes: Array<() => void> = [];
  private downstreamSubscribers = new Set<(delta: Array<[TOut, number]>) => void>();
  
  // Schema (for view chaining)
  private schema: string | null = null;
  
  // ============ CONSTRUCTOR ============
  
  constructor(config: DBSPViewConfig<TOut>) {
    this.config = config;
    this._instanceId = ++dbspViewInstanceCounter;
    this._name = config.name || `view_${config.viewId || Math.random().toString(36).slice(2)}`;
    this.sourceNames = config.sources.map(s => s.name);
    this.joinConfig = detectJoin(config.query, this.sourceNames);
    this.isJoinQuery = this.joinConfig !== null;
    
    // Initialize asynchronously
    this.initialize();
  }
  
  // ============ PUBLIC API ============
  
  /** View name (used as table name when chaining) */
  get name(): string {
    return this._name;
  }
  
  /** Unique identity - changes when view is recreated */
  get _identity(): string {
    return `${this._name}:${this._instanceId}`;
  }
  
  /** True when view is ready to be queried */
  get ready(): boolean {
    return this._ready;
  }
  
  /** Current results */
  get results(): TOut[] {
    return this.getResults();
  }
  
  /** Current row count */
  get count(): number {
    return this.getCount();
  }
  
  /** Performance statistics */
  get stats(): ViewStats {
    return this.getStats();
  }
  
  /**
   * Subscribe to view state changes.
   */
  subscribe(listener: ViewListener<TOut>): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }
  
  /**
   * Get current state snapshot.
   */
  getState(): DBSPViewState<TOut> {
    return {
      results: this.getResults(),
      count: this.getCount(),
      stats: this.getStats(),
      ready: this._ready,
    };
  }
  
  /**
   * Get paginated results (for 'full-indexed' mode).
   */
  async getResultsPage(offset: number, limit: number): Promise<TOut[]> {
    if (this.config.joinMode !== 'full-indexed' || !this.indexedStorage) {
      return this.getResults().slice(offset, offset + limit);
    }
    
    const rawResults = await this.indexedStorage.getPage(offset, limit);
    return rawResults.map(([left, right]) => {
      return this.joinConfig 
        ? this.joinConfig.projector(left, right) as TOut 
        : { ...left, ...right } as TOut;
    });
  }
  
  /**
   * Get all results from IndexedDB (for 'full-indexed' mode).
   */
  async getAllResults(): Promise<TOut[]> {
    if (this.config.joinMode !== 'full-indexed' || !this.indexedStorage) {
      return this.getResults();
    }
    
    const rawResults = await this.indexedStorage.getAllResults();
    return rawResults.map(([left, right]) => {
      return this.joinConfig 
        ? this.joinConfig.projector(left, right) as TOut 
        : { ...left, ...right } as TOut;
    });
  }
  
  /**
   * Cleanup resources.
   */
  dispose(): void {
    // Unsubscribe from sources
    for (const unsubscribe of this.unsubscribes) {
      unsubscribe();
    }
    this.unsubscribes = [];
    
    // Clear join queue
    if (this.joinQueueTimeout) {
      clearTimeout(this.joinQueueTimeout);
      this.joinQueueTimeout = null;
    }
    
    // Clear join state
    if (this.joinState) {
      this.joinState.clear();
      this.joinState = null;
    }
    
    // Clear IndexedDB storage
    if (this.indexedStorage) {
      this.indexedStorage.dispose().catch(console.error);
      this.indexedStorage = null;
    }
    
    // Clear circuit
    this.circuit = null;
    this.circuitViews = {};
    
    // Clear listeners
    this.listeners.clear();
    this.downstreamSubscribers.clear();
    
    // Unregister from central registry
    dbspRegistry.unregister(this._identity);
  }
  
  // ============ STREAM INTERFACE (for view chaining) ============
  
  _getSchema(): string | null {
    return this.schema;
  }
  
  _getKeyFn(): (row: TOut) => string {
    return this.getOutputKey.bind(this);
  }
  
  _getData(): Map<string, TOut> {
    const dataMap = new Map<string, TOut>();
    
    if (this.isJoinQuery) {
      for (const row of this.getResults()) {
        const key = this.getOutputKey(row);
        dataMap.set(key, row);
      }
    } else {
      for (const [key, entry] of this.integratedData) {
        dataMap.set(key, entry.row);
      }
    }
    
    return dataMap;
  }
  
  _subscribe(callback: (delta: Array<[TOut, number]>) => void): () => void {
    this.downstreamSubscribers.add(callback);
    return () => this.downstreamSubscribers.delete(callback);
  }
  
  // ============ PRIVATE METHODS ============
  
  private getOutputKey(row: TOut): string {
    const { outputKey } = this.config;
    if (outputKey) {
      if (typeof outputKey === 'function') {
        return outputKey(row);
      }
      const keys = Array.isArray(outputKey) ? outputKey : [outputKey];
      return keys.map(k => String(row[k])).join('::');
    }
    return JSON.stringify(row);
  }
  
  private getResults(): TOut[] {
    const { joinMode, maxRows } = this.config;
    
    if (this.isJoinQuery) {
      if (!this.joinState) return [];
      
      if (joinMode === 'full-indexed') {
        return []; // Results are in IndexedDB
      } else if (joinMode === 'full') {
        if (this.joinResults.dirty && this.joinConfig) {
          const state = this.joinState as OptimizedJoinState<Record<string, unknown>, Record<string, unknown>>;
          const rawResults = state.getResults();
          const combined = rawResults.map(([left, right]) => {
            return this.joinConfig!.projector(left, right) as TOut;
          });
          this.joinResults.results = combined;
          this.joinResults.count = combined.length;
          this.joinResults.dirty = false;
          
          // Infer schema from first result (needed for view chaining)
          if (!this.schema && combined.length > 0) {
            this.schema = this.inferSchema(combined[0]);
          }
        }
        return this.joinResults.results;
      } else {
        return []; // Append-only mode: count-only
      }
    }
    
    // Circuit-based results
    if (!this.circuit) return [];
    
    if (this.cachedResults === null) {
      this.cachedResults = [];
      for (const item of this.resultsArray) {
        if (item !== undefined) {
          this.cachedResults.push(item);
        }
      }
    }
    
    const rawResults = this.cachedResults;
    if (maxRows && rawResults.length > maxRows) {
      return rawResults.slice(-maxRows);
    }
    return rawResults;
  }
  
  private getCount(): number {
    const { joinMode } = this.config;
    
    if (this.isJoinQuery) {
      if (joinMode === 'full-indexed' && this.indexedStorage) {
        return this.indexedStorage.count;
      }
      return this.joinState?.count || 0;
    }
    
    return this.integratedData.size;
  }
  
  private getStats(): ViewStats {
    const { joinMode, maxResults, maxRows } = this.config;
    
    const resultLimitReached = joinMode === 'full' && 
      this.joinState && 
      'isResultLimitReached' in this.joinState &&
      (this.joinState as OptimizedJoinState<Record<string, unknown>, Record<string, unknown>>).isResultLimitReached;
    
    return {
      lastUpdateMs: this.statsData.lastUpdateMs,
      totalUpdates: this.statsData.totalUpdates,
      avgUpdateMs: this.statsData.avgUpdateMs,
      lastFullProcessMs: this.statsData.lastFullProcessMs,
      avgFullProcessMs: this.statsData.avgFullProcessMs,
      lastOutputMs: this.statsData.lastOutputMs,
      lastCircuitMs: this.statsData.lastCircuitMs,
      lastPruneMs: this.statsData.lastPruneMs,
      lastPrunedRows: this.statsData.lastPrunedRows,
      lastDeltaRows: this.statsData.lastDeltaRows,
      currentRowCount: this.statsData.currentRowCount,
      peakRowCount: this.statsData.peakRowCount,
      estimatedMemoryBytes: this.statsData.estimatedMemoryBytes,
      peakMemoryBytes: this.statsData.peakMemoryBytes,
      memoryGrowthRate: this.statsData.memoryGrowthRate,
      memoryTrackingStartMs: this.statsData.memoryTrackingStartMs,
      resultLimitReached: resultLimitReached || undefined,
      maxResults: joinMode === 'full' ? maxResults : undefined,
      maxRows: maxRows,
      storageType: joinMode === 'full-indexed' ? 'indexeddb' : (joinMode === 'full' ? 'memory' : 'count-only'),
    };
  }
  
  private inferSchema(row: TOut): string {
    const columns: string[] = [];
    for (const [colKey, value] of Object.entries(row)) {
      let type = 'VARCHAR';
      if (typeof value === 'number') {
        type = Number.isInteger(value) ? 'INT' : 'DECIMAL';
      } else if (typeof value === 'boolean') {
        type = 'BOOLEAN';
      }
      columns.push(`${colKey} ${type}`);
    }
    return columns.join(', ');
  }
  
  /**
   * Infer the schema for a JOIN result from source schemas + projection.
   * This allows downstream views to chain immediately, without waiting for results.
   */
  private inferJoinSchema(
    leftSource: DBSPStreamHandle<Record<string, unknown>>,
    rightSource: DBSPStreamHandle<Record<string, unknown>>
  ): string | null {
    const leftSchema = leftSource._getSchema();
    const rightSchema = rightSource._getSchema();
    
    if (!leftSchema || !rightSchema) {
      return null;
    }
    
    if (!this.joinConfig) {
      return null;
    }
    
    const projection = this.joinConfig.projection;
    
    // Parse schemas into column maps
    const parseSchema = (schema: string): Map<string, string> => {
      const cols = new Map<string, string>();
      for (const part of schema.split(',')) {
        const trimmed = part.trim();
        const match = trimmed.match(/^(\w+)\s+(\w+)$/);
        if (match) {
          cols.set(match[1], match[2]);
        }
      }
      return cols;
    };
    
    const leftCols = parseSchema(leftSchema);
    const rightCols = parseSchema(rightSchema);
    
    // Build output columns according to projection
    const outputCols: string[] = [];
    
    // If leftSelectAll, include all left columns
    if (projection.leftSelectAll) {
      for (const [name, type] of leftCols) {
        outputCols.push(`${name} ${type}`);
      }
    }
    
    // If rightSelectAll, include all right columns
    if (projection.rightSelectAll) {
      for (const [name, type] of rightCols) {
        // Skip if already added from left (name collision)
        if (!projection.leftSelectAll || !leftCols.has(name)) {
          outputCols.push(`${name} ${type}`);
        }
      }
    }
    
    // Add explicit mappings with their aliases
    for (const mapping of projection.mappings) {
      const sourceCols = mapping.table === projection.leftTable ? leftCols : rightCols;
      const type = sourceCols.get(mapping.column) || 'VARCHAR';
      outputCols.push(`${mapping.alias} ${type}`);
    }
    
    return outputCols.join(', ');
  }
  
  private notifyListeners(): void {
    const state = this.getState();
    for (const listener of this.listeners) {
      listener(state);
    }
  }
  
  private notifyDownstream(entries: Array<[TOut, number]>): void {
    for (const callback of this.downstreamSubscribers) {
      callback(entries);
    }
  }
  
  // ============ INITIALIZATION ============
  
  private async initialize(): Promise<void> {
    const { sources } = this.config;
    
    // Wait for all sources to be ready
    const waitForSources = (): Promise<void> => {
      return new Promise((resolve) => {
        let attempts = 0;
        const check = () => {
          attempts++;
          const readyStates = sources.map(s => ({ name: s.name, ready: s.ready }));
          if (sources.every(s => s.ready)) {
            console.log(`[DBSPView:${this._name}] All sources ready after ${attempts} checks:`, readyStates);
            resolve();
          } else {
            if (attempts % 100 === 0) {
              console.log(`[DBSPView:${this._name}] Waiting for sources (attempt ${attempts}):`, readyStates);
            }
            setTimeout(check, 10);
          }
        };
        check();
      });
    };
    
    await waitForSources();
    
    // Initialize based on query type - MUST check return value!
    let initialized = false;
    if (this.isJoinQuery) {
      initialized = this.initJoin();
    } else {
      initialized = this.initCircuit();
    }
    
    if (!initialized) {
      console.error(`[DBSPView:${this._name}] Failed to initialize - sources may not have schemas yet`);
      // Retry after a short delay
      await new Promise(resolve => setTimeout(resolve, 50));
      if (this.isJoinQuery) {
        initialized = this.initJoin();
      } else {
        initialized = this.initCircuit();
      }
      
      if (!initialized) {
        console.error(`[DBSPView:${this._name}] Initialization failed after retry`);
        return;
      }
    }
    
    console.log(`[DBSPView:${this._name}] Initialized successfully, subscribing to ${sources.length} sources`);
    
    // Subscribe to sources
    for (const source of sources) {
      const unsubscribe = source._subscribe((delta) => {
        if (this.isJoinQuery) {
          this.queueJoinDelta(source.name, delta as Array<[Record<string, unknown>, number]>);
        } else {
          this.processCircuitDelta(source.name, delta as Array<[Record<string, unknown>, number]>);
        }
      });
      this.unsubscribes.push(unsubscribe);
    }
    
    // Load initial data
    for (const source of sources) {
      const data = source._getData();
      console.log(`[DBSPView:${this._name}] Loading initial data from "${source.name}": ${data.size} rows`);
      if (data.size > 0) {
        const entries: Array<[Record<string, unknown>, number]> = [];
        for (const [, row] of data) {
          entries.push([row, 1]);
        }
        if (this.isJoinQuery) {
          this.queueJoinDelta(source.name, entries);
        } else {
          this.processCircuitDelta(source.name, entries);
        }
      }
    }
    
    console.log(`[DBSPView:${this._name}] Initialization complete, current count: ${this.getCount()}`);
    this._ready = true;
    
    // Register with central registry for auto-visualization
    this.registerWithRegistry();
    
    this.notifyListeners();
    dbspStore.notifyChange();
  }
  
  /**
   * Register this view with the central registry
   */
  private registerWithRegistry(): void {
    const { sources, query, joinMode } = this.config;
    
    // Parse operators from SQL query
    const operators = this.parseOperators(query);
    
    dbspRegistry.register({
      id: this._identity,
      name: this._name,
      type: 'view',
      query,
      sourceIds: sources.map(s => s._identity),
      sourceNames: sources.map(s => s.name),
      stats: this.getStats(),
      operators,
      ready: this._ready,
      isJoin: this.isJoinQuery,
      joinMode: joinMode,
      rowCount: this.getCount(),
      getStats: () => this.getStats(),
      getRowCount: () => this.getCount(),
      isReady: () => this._ready,
      getData: () => this.getResults() as Record<string, unknown>[],
    });
  }
  
  /**
   * Parse SQL query to extract operator information
   */
  private parseOperators(query: string): OperatorInfo[] {
    const operators: OperatorInfo[] = [];
    const upperQuery = query.toUpperCase();
    
    // SCAN operator for each source
    for (const sourceName of this.sourceNames) {
      operators.push({
        type: 'scan',
        sqlClause: `FROM ${sourceName}`,
        circuitOp: 'input',
        complexity: 'O(|Δ|)',
      });
    }
    
    // Detect JOIN
    if (upperQuery.includes(' JOIN ')) {
      const joinMatch = query.match(/JOIN\s+\w+\s+(?:\w+\s+)?ON\s+[^)]+/i);
      operators.push({
        type: 'join',
        sqlClause: joinMatch ? joinMatch[0] : 'JOIN ...',
        circuitOp: this.isJoinQuery ? 'hash_join' : 'nested_loop',
        complexity: 'O(|ΔL| + |ΔR|)',
        details: this.joinConfig ? `ON ${this.joinConfig.leftJoinKey} = ${this.joinConfig.rightJoinKey}` : undefined,
      });
    }
    
    // Detect WHERE (filter)
    if (upperQuery.includes(' WHERE ')) {
      const whereMatch = query.match(/WHERE\s+.+?(?=GROUP|ORDER|LIMIT|HAVING|$)/is);
      operators.push({
        type: 'filter',
        sqlClause: whereMatch ? whereMatch[0].trim() : 'WHERE ...',
        circuitOp: 'filter',
        complexity: 'O(|Δ|)',
      });
    }
    
    // Detect GROUP BY (aggregate)
    if (upperQuery.includes('GROUP BY')) {
      const groupMatch = query.match(/GROUP\s+BY\s+[^)]+?(?=HAVING|ORDER|LIMIT|$)/is);
      operators.push({
        type: 'aggregate',
        sqlClause: groupMatch ? groupMatch[0].trim() : 'GROUP BY ...',
        circuitOp: 'reduce',
        complexity: 'O(|Δ groups|)',
      });
    }
    
    // Detect aggregate functions (without GROUP BY = global aggregate)
    if (!upperQuery.includes('GROUP BY')) {
      const aggFuncs = ['SUM(', 'COUNT(', 'AVG(', 'MIN(', 'MAX('];
      const hasAgg = aggFuncs.some(fn => upperQuery.includes(fn));
      if (hasAgg) {
        operators.push({
          type: 'aggregate',
          sqlClause: 'Global aggregation',
          circuitOp: 'reduce',
          complexity: 'O(1)',
        });
      }
    }
    
    // Detect window functions
    if (upperQuery.includes(' OVER ') || upperQuery.includes(' OVER(')) {
      const windowMatch = query.match(/\w+\s*\([^)]*\)\s+OVER\s*\([^)]+\)/i);
      operators.push({
        type: 'window',
        sqlClause: windowMatch ? windowMatch[0] : 'WINDOW FUNCTION',
        circuitOp: 'window',
        complexity: 'O(|partition|)',
      });
    }
    
    // Detect ORDER BY (sort)
    if (upperQuery.includes('ORDER BY') && !upperQuery.includes(' OVER ')) {
      const orderMatch = query.match(/ORDER\s+BY\s+.+?(?=LIMIT|$)/is);
      operators.push({
        type: 'sort',
        sqlClause: orderMatch ? orderMatch[0].trim() : 'ORDER BY ...',
        circuitOp: 'sort',
        complexity: 'O(n log n)',
      });
    }
    
    // Detect LIMIT
    if (upperQuery.includes('LIMIT')) {
      const limitMatch = query.match(/LIMIT\s+\d+/i);
      operators.push({
        type: 'limit',
        sqlClause: limitMatch ? limitMatch[0] : 'LIMIT ...',
        circuitOp: 'take',
        complexity: 'O(1)',
      });
    }
    
    // Detect DISTINCT
    if (upperQuery.includes('SELECT DISTINCT')) {
      operators.push({
        type: 'distinct',
        sqlClause: 'SELECT DISTINCT',
        circuitOp: 'distinct',
        complexity: 'O(|Δ|)',
      });
    }
    
    // Detect UNION/INTERSECT/EXCEPT
    for (const setOp of ['UNION', 'INTERSECT', 'EXCEPT']) {
      if (upperQuery.includes(` ${setOp} `)) {
        operators.push({
          type: 'union',
          sqlClause: setOp,
          circuitOp: setOp.toLowerCase(),
          complexity: 'O(|ΔL| + |ΔR|)',
        });
      }
    }
    
    // PROJECT is always present (SELECT columns)
    const selectMatch = query.match(/SELECT\s+(?:DISTINCT\s+)?(.+?)\s+FROM/is);
    if (selectMatch) {
      const projection = selectMatch[1].trim();
      if (projection !== '*') {
        operators.push({
          type: 'project',
          sqlClause: `SELECT ${projection.length > 50 ? projection.slice(0, 50) + '...' : projection}`,
          circuitOp: 'map',
          complexity: 'O(|Δ|)',
        });
      }
    }
    
    return operators;
  }
  
  // ============ CIRCUIT PROCESSING ============
  
  private initCircuit(): boolean {
    const { sources, query, debug } = this.config;
    
    // Check all sources have schemas
    const schemas: Array<{ name: string; schema: string }> = [];
    for (const source of sources) {
      const schema = source._getSchema();
      if (!schema) {
        console.warn(`[DBSPView:${this._name}] Source "${source.name}" has no schema yet (ready=${source.ready})`);
        return false;
      }
      schemas.push({ name: source.name, schema });
    }
    
    try {
      const tableDefs = schemas.map(s => `CREATE TABLE ${s.name} (${s.schema});`).join('\n');
      const sql = `${tableDefs}\nCREATE VIEW result AS ${query};`;
      
      if (debug) {
        console.log('[DBSPView] Compiling SQL:', sql);
      }
      
      const compiler = new SQLCompiler();
      const { circuit, views } = compiler.compile(sql);
      
      this.circuit = circuit;
      this.circuitViews = views;
      
      // Listen for output
      views['result'].output((delta) => {
        this.handleCircuitOutput(delta as ZSet<TOut>);
      });
      
      if (debug) {
        console.log('[DBSPView] Circuit initialized with sources:', schemas.map(s => s.name));
      }
      
      return true;
    } catch (err) {
      console.error('[DBSPView] Failed to compile SQL:', err);
      return false;
    }
  }
  
  private handleCircuitOutput(zset: ZSet<TOut>): void {
    const outputStart = performance.now();
    
    if (!zset || typeof zset.entries !== 'function') {
      console.warn('[DBSPView] Received invalid delta:', zset);
      return;
    }
    
    let hasChanges = false;
    const deltaEntries: Array<[TOut, number]> = [];
    const { maxRows } = this.config;
    
    for (const [row, weight] of zset.entries()) {
      const rowKey = this.getOutputKey(row);
      const existing = this.integratedData.get(rowKey);
      const oldWeight = existing?.weight || 0;
      const newWeight = oldWeight + weight;
      
      const wasPresent = oldWeight > 0;
      const isPresent = newWeight > 0;
      
      if (!wasPresent && isPresent) {
        // INSERT
        let idx: number;
        if (this.freeIndices.length > 0) {
          idx = this.freeIndices.pop()!;
          this.resultsArray[idx] = row;
        } else {
          idx = this.resultsArray.length;
          this.resultsArray.push(row);
        }
        this.integratedData.set(rowKey, { row, weight: newWeight, index: idx });
        deltaEntries.push([row, 1]);
        hasChanges = true;
        
        // Infer schema from first row
        if (!this.schema) {
          this.schema = this.inferSchema(row);
        }
      } else if (wasPresent && !isPresent) {
        // DELETE
        const idx = existing!.index;
        // @ts-expect-error - tombstone
        this.resultsArray[idx] = undefined;
        this.freeIndices.push(idx);
        this.integratedData.delete(rowKey);
        deltaEntries.push([existing!.row, -1]);
        hasChanges = true;
      } else if (wasPresent && isPresent) {
        // UPDATE
        const idx = existing!.index;
        deltaEntries.push([existing!.row, -1]);
        deltaEntries.push([row, 1]);
        this.resultsArray[idx] = row;
        this.integratedData.set(rowKey, { row, weight: newWeight, index: idx });
        hasChanges = true;
      }
    }
    
    // Notify downstream subscribers
    if (deltaEntries.length > 0) {
      this.notifyDownstream(deltaEntries);
    }
    
    // Prune old data if maxRows is specified
    const pruneStart = performance.now();
    let prunedRows = 0;
    
    if (maxRows && this.integratedData.size > maxRows) {
      const toRemove = this.integratedData.size - maxRows;
      
      for (let i = 0; i < this.resultsArray.length && prunedRows < toRemove; i++) {
        const row = this.resultsArray[i];
        if (row !== undefined) {
          const rowKey = this.getOutputKey(row);
          const entry = this.integratedData.get(rowKey);
          if (entry && entry.index === i) {
            // @ts-expect-error - tombstone
            this.resultsArray[i] = undefined;
            this.freeIndices.push(i);
            this.integratedData.delete(rowKey);
            prunedRows++;
            hasChanges = true;
          }
        }
      }
    }
    
    const pruneElapsed = performance.now() - pruneStart;
    this.statsData.lastPruneMs = pruneElapsed;
    this.statsData.lastPrunedRows = prunedRows;
    this.statsData.currentRowCount = this.integratedData.size;
    this.statsData.peakRowCount = Math.max(this.statsData.peakRowCount, this.integratedData.size);
    
    // Memory estimation
    const AVG_ROW_SIZE_BYTES = 200;
    const currentMemory = this.integratedData.size * AVG_ROW_SIZE_BYTES;
    this.statsData.estimatedMemoryBytes = currentMemory;
    this.statsData.peakMemoryBytes = Math.max(this.statsData.peakMemoryBytes, currentMemory);
    
    const outputElapsed = performance.now() - outputStart;
    this.statsData.lastOutputMs = outputElapsed;
    
    if (hasChanges) {
      this.cachedResults = null;
      this.notifyListeners();
      dbspStore.notifyChange();
    }
  }
  
  private processCircuitDelta(sourceName: string, entries: Array<[Record<string, unknown>, number]>): void {
    if (!this.circuit) return;
    
    const start = performance.now();
    
    const delta = ZSet.fromEntries(entries);
    const inputMap = new Map<string, ZSet<unknown>>();
    
    // Initialize all sources with zero
    for (const source of this.config.sources) {
      inputMap.set(source.name, ZSet.zero());
    }
    
    // Set the actual delta
    inputMap.set(sourceName, delta as ZSet<unknown>);
    
    this.statsData.lastOutputMs = 0;
    this.circuit.step(inputMap);
    
    const elapsed = performance.now() - start;
    const circuitOnlyTime = elapsed - this.statsData.lastOutputMs;
    
    this.statsData.lastDeltaRows = entries.length;
    this.statsData.lastCircuitMs = circuitOnlyTime;
    this.updateStats(circuitOnlyTime, elapsed);
  }
  
  // ============ JOIN PROCESSING ============
  
  private initJoin(): boolean {
    const { sources, joinMode = 'append-only', maxResults = 10_000, viewId, debug } = this.config;
    
    if (!this.joinConfig) return false;
    
    const leftSource = sources.find(s => s.name === this.joinConfig!.leftSource);
    const rightSource = sources.find(s => s.name === this.joinConfig!.rightSource);
    
    const leftJoinKeyFn = (row: Record<string, unknown>) => String(row[this.joinConfig!.leftJoinKey]);
    const rightJoinKeyFn = (row: Record<string, unknown>) => String(row[this.joinConfig!.rightJoinKey]);
    
    // Eagerly infer schema from source schemas + projection
    // This is needed for view chaining - downstream views need schema immediately
    if (!this.schema && leftSource && rightSource) {
      this.schema = this.inferJoinSchema(leftSource, rightSource);
    }
    
    if (joinMode === 'full-indexed') {
      if (!leftSource || !rightSource) return false;
      
      const storage = new JoinResultStorage<Record<string, unknown>, Record<string, unknown>>(viewId || this._name);
      this.indexedStorage = storage;
      
      this.joinState = new AppendOnlyJoinState(
        leftJoinKeyFn,
        rightJoinKeyFn,
        false,
        (left, right, joinKey) => {
          if (this.indexedStorage) {
            this.indexedStorage.add(left, right, joinKey);
          }
        }
      );
      
      storage.init().then(() => {
        if (debug) {
          console.log(`[DBSPView] 💾 FULL-INDEXED JOIN mode (IndexedDB)`);
        }
      }).catch(console.error);
      
    } else if (joinMode === 'full') {
      if (!leftSource || !rightSource) return false;
      
      const leftKeyFn = leftSource._getKeyFn();
      const rightKeyFn = rightSource._getKeyFn();
      
      this.joinState = new OptimizedJoinState(
        leftKeyFn,
        rightKeyFn,
        leftJoinKeyFn,
        rightJoinKeyFn,
        { maxResults }
      );
      
      if (debug) {
        console.log(`[DBSPView] 📋 FULL JOIN mode (stores results)`);
      }
    } else {
      this.joinState = new AppendOnlyJoinState(
        leftJoinKeyFn,
        rightJoinKeyFn,
        false
      );
      
      if (debug) {
        console.log(`[DBSPView] ⚡ APPEND-ONLY JOIN (count-only, fastest)`);
      }
    }
    
    return true;
  }
  
  private queueJoinDelta(sourceName: string, entries: Array<[Record<string, unknown>, number]>): void {
    if (!this.joinConfig) return;
    
    const isLeft = sourceName === this.joinConfig.leftSource;
    
    for (const [row, weight] of entries) {
      if (weight > 0) {
        if (isLeft) {
          this.pendingJoinUpdates.left.push(row);
        } else {
          this.pendingJoinUpdates.right.push(row);
        }
      }
    }
    
    if (!this.joinQueueTimeout) {
      this.joinQueueTimeout = setTimeout(() => {
        this.joinQueueTimeout = null;
        this.processJoinQueue();
      }, this.JOIN_BATCH_INTERVAL_MS);
    }
  }
  
  private processJoinQueue(): void {
    if (!this.joinState) return;
    
    const pending = this.pendingJoinUpdates;
    if (pending.left.length === 0 && pending.right.length === 0) return;
    
    const start = performance.now();
    const totalRows = pending.left.length + pending.right.length;
    
    // OPTIMIZATION: Only compute new matches from the PENDING data
    // Instead of iterating ALL join results (O(n) where n=75,000+), we only check
    // potential new matches from the incoming delta (O(delta * matches) which is small)
    const deltaEntries: Array<[TOut, number]> = [];
    
    if (this.config.joinMode === 'full' && this.joinConfig) {
      const state = this.joinState as OptimizedJoinState<Record<string, unknown>, Record<string, unknown>>;
      
      // Step 1: For new RIGHT rows, check against EXISTING left rows (before inserting right)
      for (const rightRow of pending.right) {
        const joinKey = String(rightRow[this.joinConfig.rightJoinKey] ?? '');
        // Get existing left rows that match
        const matchingLefts = state.getLeftByKey(joinKey);
        for (const leftRow of matchingLefts) {
          const projectedRow = this.joinConfig.projector(leftRow, rightRow) as TOut;
          const key = this.getOutputKey(projectedRow);
          if (!this.emittedJoinKeys.has(key)) {
            this.emittedJoinKeys.add(key);
            deltaEntries.push([projectedRow, 1]);
          }
        }
      }
      
      // Step 2: Insert right rows into state (so left rows can match them)
      this.joinState.batchInsertRight(pending.right);
      
      // Step 3: For new LEFT rows, check against ALL right rows (existing + newly inserted)
      for (const leftRow of pending.left) {
        const joinKey = String(leftRow[this.joinConfig.leftJoinKey] ?? '');
        // Get all right rows that match (including newly inserted ones)
        const matchingRights = state.getRightByKey(joinKey);
        for (const rightRow of matchingRights) {
          const projectedRow = this.joinConfig.projector(leftRow, rightRow) as TOut;
          const key = this.getOutputKey(projectedRow);
          if (!this.emittedJoinKeys.has(key)) {
            this.emittedJoinKeys.add(key);
            deltaEntries.push([projectedRow, 1]);
          }
        }
      }
      
      // Step 4: Insert left rows into state (for future batches)
      this.joinState.batchInsertLeft(pending.left);
    } else {
      // Non-full join modes: just insert (no downstream propagation)
      this.joinState.batchInsertRight(pending.right);
      this.joinState.batchInsertLeft(pending.left);
    }
    
    this.pendingJoinUpdates = { left: [], right: [] };
    this.joinResults.dirty = true;
    
    // Eagerly infer schema for view chaining (only once, when first results come in)
    if (!this.schema && this.joinConfig && this.config.joinMode === 'full') {
      const state = this.joinState as OptimizedJoinState<Record<string, unknown>, Record<string, unknown>>;
      const rawResults = state.getResults();
      if (rawResults.length > 0) {
        const firstRow = this.joinConfig.projector(rawResults[0][0], rawResults[0][1]) as TOut;
        this.schema = this.inferSchema(firstRow);
      }
    }
    
    const elapsed = performance.now() - start;
    this.updateStats(elapsed, elapsed);
    
    if (this.config.debug) {
      console.log(`[DBSPView:JOIN] Batch: ${totalRows} rows in ${elapsed.toFixed(2)}ms, ${deltaEntries.length} new matches`);
    }
    
    // Notify downstream views of the delta
    if (deltaEntries.length > 0) {
      this.notifyDownstream(deltaEntries);
    }
    
    this.notifyListeners();
    dbspStore.notifyChange();
  }
  
  // ============ STATS ============
  
  private updateStats(circuitTime: number, fullTime: number): void {
    this.statsData.lastUpdateMs = circuitTime;
    this.statsData.lastFullProcessMs = fullTime;
    this.statsData.totalUpdates++;
    
    this.statsData.updateTimes[this.statsData.updateTimesIndex] = circuitTime;
    this.statsData.updateTimesIndex = (this.statsData.updateTimesIndex + 1) % 100;
    this.statsData.updateTimesCount = Math.min(this.statsData.updateTimesCount + 1, 100);
    
    let sum = 0;
    for (let i = 0; i < this.statsData.updateTimesCount; i++) {
      sum += this.statsData.updateTimes[i];
    }
    this.statsData.avgUpdateMs = this.statsData.updateTimesCount > 0 
      ? sum / this.statsData.updateTimesCount 
      : 0;
    
    this.statsData.fullProcessTimes[this.statsData.fullProcessIndex] = fullTime;
    this.statsData.fullProcessIndex = (this.statsData.fullProcessIndex + 1) % 100;
    this.statsData.fullProcessCount = Math.min(this.statsData.fullProcessCount + 1, 100);
    
    let fullSum = 0;
    for (let i = 0; i < this.statsData.fullProcessCount; i++) {
      fullSum += this.statsData.fullProcessTimes[i];
    }
    this.statsData.avgFullProcessMs = this.statsData.fullProcessCount > 0 
      ? fullSum / this.statsData.fullProcessCount 
      : 0;
  }
}

