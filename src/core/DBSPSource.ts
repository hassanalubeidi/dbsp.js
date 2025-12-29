/**
 * DBSPSource - Platform-Agnostic Data Source
 * ===========================================
 * 
 * This is the core source implementation that handles data ingestion.
 * It's framework-agnostic - React integration is in react/useDBSPSource.ts.
 * 
 * ## Features
 * - Automatic upserts (same key → replace)
 * - Memory management with maxRows pruning
 * - Chunked async processing for large batches
 * - Silent eviction (aggregates preserved)
 * 
 * ## Usage (Vanilla JS)
 * 
 * ```ts
 * import { DBSPSource } from 'dbsp/core';
 * 
 * const orders = new DBSPSource({
 *   name: 'orders',
 *   key: 'orderId',
 * });
 * 
 * orders.push({ orderId: 1, amount: 100 });
 * orders.subscribe(delta => console.log('Changes:', delta));
 * ```
 * 
 * ## Usage (React - via react/useDBSPSource.ts)
 * 
 * ```tsx
 * const orders = useDBSPSource({ name: 'orders', key: 'orderId' });
 * orders.push({ orderId: 1, amount: 100 });
 * ```
 */

import type { DBSPStreamHandle, SourceStats, FreshnessConfig } from './types';
import { FreshnessQueue } from '../internals/freshness-queue';
import { dbspStore } from './store';
import { dbspRegistry } from './registry';

// ============ TYPES ============

export interface DBSPSourceConfig<T extends Record<string, unknown>> {
  /** Table name (SQL identifier) */
  name: string;
  /** Primary key for upserts */
  key: keyof T | (keyof T)[] | ((row: T) => string);
  /** Maximum rows to keep */
  maxRows?: number;
  /** Enable debug logging */
  debug?: boolean;
  /** Backpressure configuration */
  freshness?: FreshnessConfig;
}

export interface DBSPSourceState<T> {
  totalRows: number;
  stats: SourceStats;
  ready: boolean;
}

type SourceListener<T> = (state: DBSPSourceState<T>) => void;

// ============ DBSP SOURCE CLASS ============

/**
 * Platform-agnostic data source.
 * 
 * Handles data ingestion without any framework dependencies.
 * Use with React via useDBSPSource, or directly in vanilla JS.
 */
// Instance counter for debugging
let dbspSourceInstanceCounter = 0;

export class DBSPSource<T extends Record<string, unknown>> implements DBSPStreamHandle<T> {
  
  // ============ CONFIGURATION ============
  private config: DBSPSourceConfig<T>;
  private _instanceId: number;
  
  /** Unique instance ID - used to detect when sources are recreated */
  get instanceId(): number { return this._instanceId; }
  
  // ============ STATE ============
  private dataMap = new Map<string, T>();
  private insertionOrder: string[] = [];
  private _ready = false;
  private schema: string | null = null;
  
  // ============ SUBSCRIBERS ============
  private listeners = new Set<SourceListener<T>>();
  private deltaSubscribers = new Set<(delta: Array<[T, number]>) => void>();
  
  // ============ STATS ============
  private statsData = {
    lastUpdateMs: 0,
    totalUpdates: 0,
    totalRows: 0,
    avgUpdateMs: 0,
    updateTimes: new Float64Array(100),
    updateTimesIndex: 0,
    updateTimesCount: 0,
  };
  
  // ============ CHUNKED PROCESSING ============
  private readonly CHUNK_SIZE = 2000;
  private readonly LARGE_BATCH_THRESHOLD = 5000;
  private chunkQueue: T[][] = [];
  private isProcessingChunks = false;
  
  // ============ FRESHNESS QUEUE ============
  private freshnessQueue: FreshnessQueue<{ type: 'push' | 'remove'; data: T | T[] | unknown[] }> | null = null;
  private freshnessInterval: ReturnType<typeof setInterval> | null = null;
  private droppedOverflow = 0;
  private droppedStale = 0;
  
  // ============ CONSTRUCTOR ============
  
  constructor(config: DBSPSourceConfig<T>) {
    this.config = config;
    this._instanceId = ++dbspSourceInstanceCounter;
    
    console.log(`[DBSPSource:${config.name}] Created instance #${this._instanceId}`);
    
    // Validate name
    if (!config.name || !/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(config.name)) {
      throw new Error(`DBSPSource: 'name' must be a valid SQL identifier (got: "${config.name}")`);
    }
    
    // Initialize freshness queue if configured
    if (config.freshness) {
      this.freshnessQueue = new FreshnessQueue(
        config.freshness.maxBufferSize ?? 10000,
        config.freshness.maxMessageAgeMs,
        (count, reason) => {
          if (reason === 'overflow') {
            this.droppedOverflow += count;
          } else {
            this.droppedStale += count;
          }
          if (config.freshness?.onDrop) config.freshness.onDrop(count, reason);
          if (config.debug) {
            console.log(`[DBSPSource:${config.name}] Dropped ${count} messages: ${reason}`);
          }
        },
        config.debug
      );
      
      // Start processing loop
      const intervalMs = config.freshness.processingIntervalMs ?? 16;
      this.freshnessInterval = setInterval(() => this.processQueue(), intervalMs);
    }
    
    // Register with central registry for auto-visualization
    dbspRegistry.register({
      id: this._identity,
      name: config.name,
      type: 'source',
      sourceIds: [],
      sourceNames: [],
      stats: this.stats,
      operators: [],
      ready: this._ready,
      rowCount: 0,
      getStats: () => this.stats,
      getRowCount: () => this.totalRows,
      isReady: () => this._ready,
      getData: () => Array.from(this.dataMap.values()),
    });
  }
  
  // ============ PUBLIC API ============
  
  /** Source name (used as table name in SQL) */
  get name(): string {
    return this.config.name;
  }
  
  /** Unique identity - changes when source is recreated */
  get _identity(): string {
    return `${this.config.name}:${this._instanceId}`;
  }
  
  /** True when source has received data */
  get ready(): boolean {
    // Debug: Uncomment to trace ready checks
    // console.log(`[DBSPSource:${this.config.name}] ready check: ${this._ready} (instance #${this._instanceId})`);
    return this._ready;
  }
  
  /** Current row count */
  get totalRows(): number {
    return this.dataMap.size;
  }
  
  /** Performance statistics */
  get stats(): SourceStats {
    const baseStats: SourceStats = {
      lastUpdateMs: this.statsData.lastUpdateMs,
      totalUpdates: this.statsData.totalUpdates,
      totalRows: this.dataMap.size,
      avgUpdateMs: this.statsData.avgUpdateMs,
    };
    
    if (this.freshnessQueue) {
      const queueStats = this.freshnessQueue.getStats();
      baseStats.bufferSize = queueStats.size;
      baseStats.bufferCapacity = queueStats.capacity;
      baseStats.bufferUtilization = queueStats.utilization;
      baseStats.lagMs = queueStats.lagMs;
      baseStats.droppedOverflow = this.droppedOverflow;
      baseStats.droppedStale = this.droppedStale;
      baseStats.totalDropped = this.droppedOverflow + this.droppedStale;
      baseStats.isLagging = queueStats.lagMs > 100;
    }
    
    return baseStats;
  }
  
  /**
   * Add or update rows.
   */
  push(rows: T | T[]): void {
    const rowArray = Array.isArray(rows) ? rows : [rows];
    if (rowArray.length === 0) return;
    
    // Infer schema on first push
    if (!this.schema) {
      this.schema = this.inferSchema(rowArray[0]);
      this._ready = true;
      console.log(`[DBSPSource:${this.config.name}] First push - schema inferred, ready=true, instance=${this._instanceId}`);
    }
    
    // Queue or process immediately
    if (this.freshnessQueue) {
      this.freshnessQueue.enqueue({ type: 'push', data: rowArray });
    } else {
      this.processDataSync(rowArray, false);
    }
  }
  
  /**
   * Remove rows by key.
   */
  remove(...keyValues: unknown[]): void {
    if (this.freshnessQueue) {
      this.freshnessQueue.enqueue({ type: 'remove', data: keyValues });
    } else {
      const keyStr = keyValues.join('::');
      const existing = this.dataMap.get(keyStr);
      if (existing) {
        this.processDataSync([existing], true);
      }
    }
  }
  
  /**
   * Clear all data.
   */
  clear(): void {
    const entries: Array<[T, number]> = [];
    
    for (const [, row] of this.dataMap) {
      entries.push([row, -1]);
    }
    
    this.dataMap.clear();
    this.insertionOrder = [];
    
    if (this.freshnessQueue) {
      this.freshnessQueue.clear();
      this.droppedOverflow = 0;
      this.droppedStale = 0;
    }
    
    if (entries.length > 0) {
      this.notifyDeltaSubscribers(entries);
    }
    
    // Reset stats
    this.statsData.lastUpdateMs = 0;
    this.statsData.totalUpdates = 0;
    this.statsData.totalRows = 0;
    this.statsData.avgUpdateMs = 0;
    this.statsData.updateTimesIndex = 0;
    this.statsData.updateTimesCount = 0;
    this.statsData.updateTimes.fill(0);
    
    this.notifyListeners();
    dbspStore.notifyChange();
    
    if (this.config.debug) {
      console.log(`[DBSPSource:${this.config.name}] Cleared`);
    }
  }
  
  /**
   * Subscribe to state changes.
   */
  subscribe(listener: SourceListener<T>): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }
  
  /**
   * Get current state.
   */
  getState(): DBSPSourceState<T> {
    return {
      totalRows: this.dataMap.size,
      stats: this.stats,
      ready: this._ready,
    };
  }
  
  /**
   * Cleanup resources.
   */
  dispose(): void {
    // Unregister from central registry
    dbspRegistry.unregister(this._identity);
    
    if (this.freshnessInterval) {
      clearInterval(this.freshnessInterval);
      this.freshnessInterval = null;
    }
    this.listeners.clear();
    this.deltaSubscribers.clear();
    this.dataMap.clear();
    this.insertionOrder = [];
    this.chunkQueue = [];
  }
  
  // ============ STREAM INTERFACE ============
  
  _getSchema(): string | null {
    return this.schema;
  }
  
  _getKeyFn(): (row: T) => string {
    return this.getKey.bind(this);
  }
  
  _getData(): Map<string, T> {
    return this.dataMap;
  }
  
  _subscribe(callback: (delta: Array<[T, number]>) => void): () => void {
    this.deltaSubscribers.add(callback);
    return () => this.deltaSubscribers.delete(callback);
  }
  
  // ============ PRIVATE METHODS ============
  
  private getKey(row: T): string {
    const { key } = this.config;
    if (typeof key === 'function') {
      return key(row);
    }
    const keys = Array.isArray(key) ? key : [key];
    return keys.map(k => String(row[k])).join('::');
  }
  
  private inferSchema(row: T): string {
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
  
  private notifyListeners(): void {
    const state = this.getState();
    for (const listener of this.listeners) {
      listener(state);
    }
  }
  
  private notifyDeltaSubscribers(entries: Array<[T, number]>): void {
    for (const callback of this.deltaSubscribers) {
      callback(entries);
    }
  }
  
  private updateStats(elapsed: number): void {
    this.statsData.lastUpdateMs = elapsed;
    this.statsData.totalUpdates++;
    this.statsData.totalRows = this.dataMap.size;
    
    this.statsData.updateTimes[this.statsData.updateTimesIndex] = elapsed;
    this.statsData.updateTimesIndex = (this.statsData.updateTimesIndex + 1) % 100;
    this.statsData.updateTimesCount = Math.min(this.statsData.updateTimesCount + 1, 100);
    
    let sum = 0;
    for (let i = 0; i < this.statsData.updateTimesCount; i++) {
      sum += this.statsData.updateTimes[i];
    }
    this.statsData.avgUpdateMs = this.statsData.updateTimesCount > 0 
      ? sum / this.statsData.updateTimesCount 
      : 0;
  }
  
  private processChunk(chunk: T[], isRemove: boolean): number {
    const entries: Array<[T, number]> = [];
    const { maxRows, debug, name } = this.config;
    
    for (const row of chunk) {
      const rowKey = this.getKey(row);
      
      if (isRemove) {
        const existing = this.dataMap.get(rowKey);
        if (existing) {
          this.dataMap.delete(rowKey);
          const idx = this.insertionOrder.indexOf(rowKey);
          if (idx !== -1) this.insertionOrder.splice(idx, 1);
          entries.push([existing, -1]);
        }
      } else {
        const existing = this.dataMap.get(rowKey);
        
        if (existing) {
          entries.push([existing, -1]);
          const idx = this.insertionOrder.indexOf(rowKey);
          if (idx !== -1) this.insertionOrder.splice(idx, 1);
        }
        
        entries.push([row, 1]);
        this.dataMap.set(rowKey, row);
        this.insertionOrder.push(rowKey);
      }
    }
    
    if (entries.length > 0) {
      this.notifyDeltaSubscribers(entries);
    }
    
    // Silent eviction
    if (maxRows && this.dataMap.size > maxRows) {
      const toEvict = this.dataMap.size - maxRows;
      for (let i = 0; i < toEvict && this.insertionOrder.length > 0; i++) {
        const oldestKey = this.insertionOrder.shift()!;
        this.dataMap.delete(oldestKey);
      }
      if (debug && toEvict > 0) {
        console.log(`[DBSPSource:${name}] Silently evicted ${toEvict} rows`);
      }
    }
    
    return entries.length;
  }
  
  private async processChunksAsync(): Promise<void> {
    if (this.isProcessingChunks) return;
    this.isProcessingChunks = true;
    
    const totalStart = performance.now();
    let totalProcessed = 0;
    
    while (this.chunkQueue.length > 0) {
      const chunk = this.chunkQueue.shift()!;
      const processed = this.processChunk(chunk, false);
      totalProcessed += processed;
      
      // Yield to main thread
      await new Promise(resolve => setTimeout(resolve, 0));
      
      this.notifyListeners();
      dbspStore.notifyChange();
    }
    
    const elapsed = performance.now() - totalStart;
    this.updateStats(elapsed);
    
    if (this.config.debug && totalProcessed > 0) {
      console.log(`[DBSPSource:${this.config.name}] Chunked: ${totalProcessed} entries in ${elapsed.toFixed(2)}ms`);
    }
    
    this.isProcessingChunks = false;
  }
  
  private processDataSync(rows: T[], isRemove: boolean): void {
    // Large batches use chunked async
    if (rows.length > this.LARGE_BATCH_THRESHOLD && !isRemove) {
      if (this.config.debug) {
        console.log(`[DBSPSource:${this.config.name}] Large batch (${rows.length}) - using chunked async`);
      }
      
      for (let i = 0; i < rows.length; i += this.CHUNK_SIZE) {
        this.chunkQueue.push(rows.slice(i, i + this.CHUNK_SIZE));
      }
      
      this.processChunksAsync();
      return;
    }
    
    // Small batches - sync processing
    const start = performance.now();
    this.processChunk(rows, isRemove);
    const elapsed = performance.now() - start;
    this.updateStats(elapsed);
    
    if (this.config.debug) {
      console.log(`[DBSPSource:${this.config.name}] Processed ${rows.length} rows in ${elapsed.toFixed(2)}ms`);
    }
    
    this.notifyListeners();
    dbspStore.notifyChange();
  }
  
  private async processQueue(): Promise<void> {
    if (!this.freshnessQueue) return;
    
    const maxBatchSize = this.config.freshness?.maxBatchSize ?? 500;
    const messages = await this.freshnessQueue.dequeue(maxBatchSize, 1);
    
    if (messages.length === 0) return;
    
    const start = performance.now();
    const entries: Array<[T, number]> = [];
    
    for (const msg of messages) {
      const { type, data } = msg.data;
      
      if (type === 'push') {
        const rows = Array.isArray(data) ? data as T[] : [data as T];
        for (const row of rows) {
          const rowKey = this.getKey(row);
          const existing = this.dataMap.get(rowKey);
          
          if (existing) {
            entries.push([existing, -1]);
            const idx = this.insertionOrder.indexOf(rowKey);
            if (idx !== -1) this.insertionOrder.splice(idx, 1);
          }
          entries.push([row, 1]);
          this.dataMap.set(rowKey, row);
          this.insertionOrder.push(rowKey);
        }
      } else if (type === 'remove') {
        const keys = data as unknown[];
        for (const keyVal of keys) {
          const keyStr = String(keyVal);
          const existing = this.dataMap.get(keyStr);
          if (existing) {
            entries.push([existing, -1]);
            this.dataMap.delete(keyStr);
            const idx = this.insertionOrder.indexOf(keyStr);
            if (idx !== -1) this.insertionOrder.splice(idx, 1);
          }
        }
      }
    }
    
    if (entries.length > 0) {
      this.notifyDeltaSubscribers(entries);
    }
    
    // Silent eviction
    const { maxRows, debug, name } = this.config;
    if (maxRows && this.dataMap.size > maxRows) {
      const toEvict = this.dataMap.size - maxRows;
      for (let i = 0; i < toEvict && this.insertionOrder.length > 0; i++) {
        const oldestKey = this.insertionOrder.shift()!;
        this.dataMap.delete(oldestKey);
      }
      if (debug && toEvict > 0) {
        console.log(`[DBSPSource:${name}] Silently evicted ${toEvict} rows`);
      }
    }
    
    const elapsed = performance.now() - start;
    this.updateStats(elapsed);
    
    if (this.config.debug) {
      console.log(`[DBSPSource:${this.config.name}] Async: ${messages.length} messages in ${elapsed.toFixed(2)}ms`);
    }
    
    this.notifyListeners();
    dbspStore.notifyChange();
  }
}

