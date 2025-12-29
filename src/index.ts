/**
 * DBSP: Database Stream Processor
 * ================================
 * 
 * A TypeScript implementation of DBSP for incremental view maintenance.
 * Based on the paper "DBSP: Automatic Incremental View Maintenance for Rich Query Languages"
 * 
 * ## Quick Start (React)
 * 
 * ```tsx
 * import { useDBSPSource, useDBSPView } from './dbsp/react';
 * 
 * const orders = useDBSPSource<Order>({ name: 'orders', key: 'orderId' });
 * const pending = useDBSPView(orders, "SELECT * FROM orders WHERE status = 'pending'");
 * ```
 * 
 * ## Core Concepts
 * 
 * - **ZSet**: A set with integer weights (multiset representation)
 * - **Stream**: An infinite sequence of values over time
 * - **Circuit**: A dataflow graph of operators
 * - **Operators**: Transform streams (filter, map, join, aggregate)
 * 
 * @module
 */

// ═══════════════════════════════════════════════════════════════════════════════
// REACT API (Recommended for React apps)
// ═══════════════════════════════════════════════════════════════════════════════

export { useDBSPSource, useDBSPView } from './react';
export type {
  DBSPSourceOptions,
  DBSPSourceHandle,
  DBSPViewOptions,
  DBSPViewHandle,
  DBSPStreamHandle,
  FreshnessConfig,
  SourceStats,
  ViewStats,
  JoinMode,
} from './core/types';


// ═══════════════════════════════════════════════════════════════════════════════
// CORE CLASSES (Platform-agnostic - for vanilla JS, Node.js, or any framework)
// ═══════════════════════════════════════════════════════════════════════════════

export { DBSPSource, DBSPView, DBSPStore, dbspStore } from './core';
export type { DBSPSourceConfig, DBSPSourceState, DBSPViewConfig, DBSPViewState, StoreListener } from './core';


// ═══════════════════════════════════════════════════════════════════════════════
// CORE PRIMITIVES
// ═══════════════════════════════════════════════════════════════════════════════

// Z-Sets (multisets with weights)
export { 
  ZSet,
  IndexedZSet, 
  joinFilter, 
  joinFilterMap, 
  joinWithIndex, 
  antiJoin, 
  semiJoin 
} from './internals/zset';

// Streams
export * from './internals/stream';

// Operators
export * from './internals/operators';

// Circuits
export * from './internals/circuit';

// SQL Compiler
export * from './sql';


// ═══════════════════════════════════════════════════════════════════════════════
// OPTIMIZED DATA STRUCTURES
// ═══════════════════════════════════════════════════════════════════════════════

// High-performance join implementations
export { 
  OptimizedJoinState, 
  AppendOnlyJoinState,
  benchmarkJoin,
} from './joins/optimized-join';

// Join result storage (IndexedDB)
export {
  JoinResultStorage,
  clearAllJoinResults,
  deleteJoinDatabase,
} from './joins/join-storage';

// Advanced join types (ASOF, semi, anti)
export {
  AsofJoinState,
  StatePrunedJoinState,
  IncrementalSemiJoinState,
  IncrementalAntiJoinState,
} from './joins/advanced-joins';

// Optimized window function data structures
export {
  MonotonicDeque,
  RunningAggregate,
  IncrementalWindowState,
  PartitionedWindowState,
} from './internals/window-state';
export type { WindowFunctionSpec } from './internals/window-state';


// ═══════════════════════════════════════════════════════════════════════════════
// UTILITIES (for advanced usage)
// ═══════════════════════════════════════════════════════════════════════════════

// Freshness queue (for streaming data freshness)
export { FreshnessQueue, CircularBuffer } from './internals/freshness-queue';
export type { StreamMessage } from './internals/freshness-queue';
