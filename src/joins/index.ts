/**
 * DBSP Join Implementations
 * ==========================
 * 
 * High-performance join implementations for DBSP.
 * 
 * ## Available Join Types
 * 
 * - **OptimizedJoinState**: Full join with result storage (memory-limited)
 * - **AppendOnlyJoinState**: Fast append-only join (3000x faster, count-only)
 * - **AsofJoinState**: ASOF join for time-series data
 * - **IncrementalSemiJoinState**: Semi-join (exists filter)
 * - **IncrementalAntiJoinState**: Anti-join (not exists filter)
 * 
 * @module
 */

// High-performance join implementations
export { 
  OptimizedJoinState, 
  AppendOnlyJoinState,
  benchmarkJoin,
} from './optimized-join';

// Join result storage (IndexedDB)
export {
  JoinResultStorage,
  clearAllJoinResults,
  deleteJoinDatabase,
} from './join-storage';

// Advanced join types (ASOF, semi, anti)
export {
  AsofJoinState,
  StatePrunedJoinState,
  IncrementalSemiJoinState,
  IncrementalAntiJoinState,
} from './advanced-joins';


