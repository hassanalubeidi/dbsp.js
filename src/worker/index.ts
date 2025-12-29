/**
 * DBSP Worker Module
 * ==================
 * 
 * Web Worker for running DBSP data processing off the main thread.
 * 
 * This module contains:
 * - `dbsp-source.worker.ts` - Worker for useDBSPSource offloading
 * 
 * Usage:
 * Workers are loaded lazily via Vite's worker import syntax when enabled:
 * 
 * ```tsx
 * const source = useDBSPSource<Order>({
 *   name: 'orders',
 *   key: 'orderId',
 *   worker: { enabled: true, batchSize: 5000 }
 * });
 * ```
 * 
 * When worker mode is enabled, heavy data processing (insertions, updates,
 * eviction) runs in a background thread to keep the main thread responsive.
 * 
 * @module
 */

// Worker is imported directly via Vite's worker syntax, e.g.:
// import Worker from './dbsp-source.worker?worker'
// No direct exports needed from this index file.
