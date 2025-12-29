/**
 * DBSP Internals
 * ===============
 * 
 * Core implementation details of DBSP. These modules are used internally
 * and can be used directly for advanced use cases.
 * 
 * @module
 */

// Z-Sets (multisets with weights)
export { 
  ZSet,
  IndexedZSet, 
  joinFilter, 
  joinFilterMap, 
  joinWithIndex, 
  antiJoin, 
  semiJoin 
} from './zset';

// Streams
export * from './stream';

// Operators
export * from './operators';

// Circuits
export * from './circuit';

// Freshness queue (for streaming data freshness)
export { FreshnessQueue, CircularBuffer } from './freshness-queue';
export type { StreamMessage } from './freshness-queue';

// Window function state
export {
  MonotonicDeque,
  RunningAggregate,
  IncrementalWindowState,
  PartitionedWindowState,
} from './window-state';
export type { WindowFunctionSpec } from './window-state';

