/**
 * DBSP React Bindings
 * ====================
 * 
 * Incremental SQL computation for React applications.
 * 
 * ## Quick Start
 * 
 * ```tsx
 * import { useDBSPSource, useDBSPView } from 'dbsp/react';
 * 
 * function OrderDashboard() {
 *   // 1. Create a data source (your "table")
 *   const orders = useDBSPSource<Order>({ name: 'orders', key: 'orderId' });
 *   
 *   // 2. Query it with SQL (updates incrementally!)
 *   const pending = useDBSPView(orders, "SELECT * FROM orders WHERE status = 'pending'");
 *   const byStatus = useDBSPView(orders, 
 *     "SELECT status, SUM(amount) as total FROM orders GROUP BY status",
 *     { outputKey: 'status' }
 *   );
 *   
 *   // 3. Push data - all views update automatically!
 *   useEffect(() => {
 *     fetchOrders().then(orders.push);
 *   }, []);
 *   
 *   return <div>{pending.count} pending orders</div>;
 * }
 * ```
 * 
 * ## Core Concepts
 * 
 * - **Source** (`useDBSPSource`): An in-memory table you can push data to
 * - **View** (`useDBSPView`): A SQL query that updates incrementally when source changes
 * - **View Chaining**: Views can feed into other views for composable data pipelines
 * - **Dynamic Filters**: Use `interpolateSQL` + `useMemo` for $param syntax
 * 
 * ## Features
 * 
 * - **JOINs**: Multi-table joins with automatic hash indexing (50-100x faster)
 * - **Aggregations**: GROUP BY with SUM, COUNT, AVG, MIN, MAX
 * - **Window Functions**: LAG, LEAD, RANK, NTILE, rolling aggregates
 * - **Memory Management**: `maxRows` option prevents unbounded growth
 * - **Adaptive Throttling**: Automatic FPS adjustment based on render time
 * 
 * @module
 */

// ═══════════════════════════════════════════════════════════════════════════════
// CORE API
// ═══════════════════════════════════════════════════════════════════════════════

export { useDBSPSource } from './useDBSPSource';
export { useDBSPView } from './useDBSPView';
export { useDBSPRegistry, useRegistryStats, useGraphLayout, getNodeEdges, getLineage, getDescendants } from './useDBSPRegistry';
export type { UseDBSPRegistryResult } from './useDBSPRegistry';

// Types for configuring sources and views
export type {
  DBSPSourceOptions,
  DBSPSourceHandle,
  DBSPViewOptions,
  DBSPViewHandle,
  DBSPStreamHandle,  // Unified interface - both sources and views can be inputs
  SourceStats,
  ViewStats,
  JoinMode,
  FreshnessConfig,
} from '../core/types';


// ═══════════════════════════════════════════════════════════════════════════════
// SQL UTILITIES
// ═══════════════════════════════════════════════════════════════════════════════

export { interpolateSQL } from './sql';

export type { SQLParamValue, SQLParams, FilterValue, FilterRecord } from './sql';


// ═══════════════════════════════════════════════════════════════════════════════
// EXTERNAL STORE (Advanced)
// ═══════════════════════════════════════════════════════════════════════════════

export {
  dbspStore,              // Singleton store for manual control
  useDBSPStoreVersion,    // Hook to subscribe to store updates
} from './store';


// ═══════════════════════════════════════════════════════════════════════════════
// UTILITIES
// ═══════════════════════════════════════════════════════════════════════════════

export {
  buildSelect,
  getPerformanceLevel,
  formatMs,
  formatBytes,
  type PerformanceLevel,
  type SourceRow,
  type ViewResult,
} from './helpers';


// ═══════════════════════════════════════════════════════════════════════════════
// CORE CLASSES (for advanced/non-React use)
// ═══════════════════════════════════════════════════════════════════════════════

export { DBSPSource, DBSPView, DBSPStore, DBSPRegistry, dbspRegistry } from '../core';
export type { 
  DBSPSourceConfig, 
  DBSPSourceState, 
  DBSPViewConfig, 
  DBSPViewState, 
  StoreListener,
  RegistryEntry,
  RegistryEdge,
  RegistryGraph,
  OperatorInfo,
  OperatorType,
} from '../core';
