/**
 * useDBSPSource - React Hook for DBSP Data Sources
 * ================================================
 * 
 * Thin React wrapper around the platform-agnostic DBSPSource class.
 * All logic is in core/DBSPSource.ts - this just adds React integration.
 * 
 * ## Basic Usage
 * 
 * ```tsx
 * const orders = useDBSPSource<Order>({ 
 *   name: 'orders',
 *   key: 'orderId'
 * });
 * 
 * orders.push({ orderId: 1, amount: 100 });
 * ```
 * 
 * ## Memory Limits
 * 
 * ```tsx
 * const trades = useDBSPSource<Trade>({
 *   name: 'trades',
 *   key: 'tradeId',
 *   maxRows: 50000,  // Evict oldest when limit reached
 * });
 * ```
 */

import { useRef, useMemo, useEffect } from 'react';
import type { DBSPSourceOptions, DBSPSourceHandle, SourceStats } from '../core/types';
import { DBSPSource, type DBSPSourceConfig } from '../core/DBSPSource';
import { useDBSPStoreVersion } from './store';

// ============ HOOK ============

/**
 * Create a reactive data source for SQL queries.
 * 
 * React optimizations:
 * - Uses useSyncExternalStore for efficient subscription
 * - Caches stats objects to prevent unnecessary re-renders
 * - Uses getters/functions that read from ref to avoid stale closures
 * 
 * @param options - Source configuration
 * @returns Source handle with push, remove, clear methods
 */
export function useDBSPSource<T extends Record<string, unknown>>(
  options: DBSPSourceOptions<T>
): DBSPSourceHandle<T> {
  const { name, key, maxRows, debug, freshness } = options;
  
  // Create stable config
  const config = useMemo((): DBSPSourceConfig<T> => ({
    name,
    key,
    maxRows,
    debug,
    freshness,
  }), [name, key, maxRows, debug, freshness]);
  
  // Create source instance - stored in ref to survive re-renders
  const sourceRef = useRef<DBSPSource<T> | null>(null);
  
  if (!sourceRef.current) {
    sourceRef.current = new DBSPSource<T>(config);
    console.log(`[useDBSPSource:${name}] Created new source`);
        }
        
  // Recreate source when config changes
  const prevConfigRef = useRef(config);
  useEffect(() => {
    if (prevConfigRef.current !== config) {
      console.log(`[useDBSPSource:${name}] Config changed, recreating source`);
      sourceRef.current?.dispose();
      sourceRef.current = new DBSPSource<T>(config);
      prevConfigRef.current = config;
        }
  }, [config, name]);
  
  // Ensure source exists on mount/remount (handles StrictMode)
  useEffect(() => {
    if (!sourceRef.current) {
      sourceRef.current = new DBSPSource<T>(config);
      console.log(`[useDBSPSource:${name}] Recreated source after StrictMode remount`);
    }
  });
  
  // Cleanup on unmount - MUST set ref to null so StrictMode remount creates a new source
  useEffect(() => {
    return () => {
      sourceRef.current?.dispose();
      sourceRef.current = null;
    };
  }, []);
  
  // Subscribe to store updates
  const storeVersion = useDBSPStoreVersion();
  
  // ============ REACT OPTIMIZATIONS ============
  
  // Cache totalRows - reads from current source
  const totalRows = useMemo(() => {
    void storeVersion; // Dependency
    return sourceRef.current?.totalRows ?? 0;
  }, [storeVersion]);
  
  // Cache stats object
  const lastStatsRef = useRef<{ version: number; totalUpdates: number; stats: SourceStats } | null>(null);
  
  const stats = useMemo((): SourceStats => {
    void storeVersion;
    
    const source = sourceRef.current;
    if (!source) return { lastUpdateMs: 0, totalUpdates: 0, totalRows: 0, avgUpdateMs: 0 };
    
    const newStats = source.stats;
    const currentTotalUpdates = newStats.totalUpdates;
    
    if (lastStatsRef.current && 
        lastStatsRef.current.totalUpdates === currentTotalUpdates &&
        lastStatsRef.current.version === storeVersion - 1) {
      lastStatsRef.current.version = storeVersion;
      return lastStatsRef.current.stats;
    }
    
    lastStatsRef.current = { version: storeVersion, totalUpdates: currentTotalUpdates, stats: newStats };
    return newStats;
  }, [storeVersion]);
  
  // ============ RETURN ============
  // CRITICAL: All methods/getters must use sourceRef.current, NOT a captured variable!
  // This avoids stale closure issues with React StrictMode and HMR.
  
  return {
    // Public interface - all use sourceRef.current to get CURRENT source
    name,
    push: (rows: T | T[]) => sourceRef.current?.push(rows),
    remove: (...keyValues: unknown[]) => sourceRef.current?.remove(...keyValues),
    clear: () => sourceRef.current?.clear(),
    totalRows,
    get ready() { return sourceRef.current?.ready ?? false; },
    stats,
    
    // Stream interface - all use sourceRef.current
    get _identity() { return sourceRef.current?._identity ?? `${name}:pending`; },
    _getSchema: () => sourceRef.current?._getSchema() ?? null,
    _getKeyFn: () => sourceRef.current?._getKeyFn() ?? ((row: T) => JSON.stringify(row)),
    _getData: () => sourceRef.current?._getData() ?? new Map(),
    _subscribe: (callback: (delta: Array<[T, number]>) => void) => {
      return sourceRef.current?._subscribe(callback) ?? (() => {});
    },
  };
}
