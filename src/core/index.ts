/**
 * DBSP Core - Platform-Agnostic API
 * ==================================
 * 
 * This module exports the core DBSP classes that work without any framework.
 * Use these directly in Node.js, vanilla JS, or with any UI framework.
 * 
 * ## Quick Start (Vanilla JS)
 * 
 * ```ts
 * import { DBSPSource, DBSPView, dbspStore } from 'dbsp/core';
 * 
 * // Create a source
 * const orders = new DBSPSource({
 *   name: 'orders',
 *   key: 'orderId',
 * });
 * 
 * // Create a view
 * const pending = new DBSPView({
 *   sources: [orders],
 *   query: "SELECT * FROM orders WHERE status = 'pending'",
 * });
 * 
 * // Subscribe to changes
 * pending.subscribe(state => {
 *   console.log('Pending orders:', state.results);
 * });
 * 
 * // Push data
 * orders.push({ orderId: 1, amount: 100, status: 'pending' });
 * 
 * // Cleanup
 * pending.dispose();
 * orders.dispose();
 * ```
 * 
 * ## For React
 * 
 * Use the React hooks instead:
 * ```tsx
 * import { useDBSPSource, useDBSPView } from 'dbsp/react';
 * ```
 * 
 * @module
 */

// ============ CORE CLASSES ============

export { DBSPSource } from './DBSPSource';
export type { DBSPSourceConfig, DBSPSourceState } from './DBSPSource';

export { DBSPView } from './DBSPView';
export type { DBSPViewConfig, DBSPViewState } from './DBSPView';

export { DBSPStore, dbspStore } from './store';
export type { StoreListener } from './store';

export { DBSPRegistry, dbspRegistry } from './registry';
export type { RegistryEntry, RegistryEdge, RegistryGraph, OperatorInfo, OperatorType } from './registry';

// ============ TYPES ============

export type {
  // Stream interface (unified for sources and views)
  DBSPStreamHandle,
  
  // Source types
  DBSPSourceOptions,
  DBSPSourceHandle,
  SourceStats,
  
  // View types  
  DBSPViewOptions,
  DBSPViewHandle,
  ViewStats,
  
  // Configuration
  JoinMode,
  FreshnessConfig,
  WorkerConfig,
} from './types';
