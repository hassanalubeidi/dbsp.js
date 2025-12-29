/**
 * useDBSPServerView - Server-Side SQL View Hook
 * ==============================================
 * 
 * Same interface as useDBSPView, but runs SQL on the DBSP server.
 * Only the computed results are streamed to the client.
 * 
 * ## Benefits
 * 
 * - Heavy SQL computation on server
 * - Reduced client memory (only results, not source data)
 * - Shared computation (multiple clients, same view = single computation)
 * - Works on low-power devices
 * 
 * ## Usage
 * 
 * ```tsx
 * // Same interface as useDBSPView!
 * const sectorPnL = useDBSPServerView(
 *   ['rfqs', 'positions'],  // Source NAMES (not handles)
 *   `SELECT sector, SUM(notional) as total FROM positions GROUP BY sector`
 * );
 * 
 * console.log(sectorPnL.results);  // Computed on server, streamed here
 * ```
 */

import { useEffect, useRef, useState, useId, useMemo, useDeferredValue } from 'react';
import { useDBSPServerConnection } from './useDBSPServerConnection';
import type { ViewStats } from '../core/types';
import type { ViewSnapshotMessage, ViewDeltaMessage } from './protocol';

// ═══════════════════════════════════════════════════════════════════════════════
// TYPES
// ═══════════════════════════════════════════════════════════════════════════════

export interface DBSPServerViewOptions {
  /** View name for debugging */
  name?: string;
  /** Join mode (server-side) */
  joinMode?: 'append-only' | 'full' | 'full-indexed';
  /** Max rows to keep */
  maxRows?: number;
  /** Max results for joins */
  maxResults?: number;
  /** Enable debug logging */
  debug?: boolean;
}

export interface DBSPServerViewHandle<T extends Record<string, unknown>> {
  /** Current results (streamed from server) */
  results: T[];
  /** Total count */
  count: number;
  /** Performance stats */
  stats: ViewStats;
  /** Whether connected to server */
  connected: boolean;
  /** Error message if any */
  error: string | null;
}

// ═══════════════════════════════════════════════════════════════════════════════
// HOOK
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Create a server-side SQL view.
 * 
 * @param sources - Source NAMES (strings), not handles
 * @param query - SQL query to execute on server
 * @param options - View options
 */
export function useDBSPServerView<T extends Record<string, unknown>>(
  sources: string | string[],
  query: string,
  options: DBSPServerViewOptions = {}
): DBSPServerViewHandle<T> {
  const viewId = useId();
  const { connected, subscribeView } = useDBSPServerConnection();
  
  // Normalize sources
  const sourceArray = useMemo(() => 
    Array.isArray(sources) ? sources : [sources],
    [sources]
  );
  
  // State
  const [results, setResults] = useState<T[]>([]);
  const [count, setCount] = useState(0);
  const [stats, setStats] = useState<ViewStats>({
    lastUpdateMs: 0,
    totalUpdates: 0,
    avgUpdateMs: 0,
  });
  const [error, setError] = useState<string | null>(null);
  
  // Track results by key for delta application
  const resultsMapRef = useRef<Map<string, T>>(new Map());
  
  // Key function - use first field or generate
  const getKey = (row: T): string => {
    const keys = Object.keys(row);
    if (keys.length > 0) {
      return String(row[keys[0]]);
    }
    return JSON.stringify(row);
  };
  
  // Handle snapshot
  const handleSnapshot = (msg: ViewSnapshotMessage) => {
    if (options.debug) {
      console.log(`[useDBSPServerView:${options.name || viewId}] Snapshot: ${msg.count} rows`);
    }
    
    // Reset and populate map
    resultsMapRef.current.clear();
    for (const row of msg.results as T[]) {
      const key = getKey(row);
      resultsMapRef.current.set(key, row);
    }
    
    setResults(msg.results as T[]);
    setCount(msg.count);
    setStats({
      lastUpdateMs: msg.stats.lastUpdateMs,
      totalUpdates: msg.stats.totalUpdates,
      avgUpdateMs: msg.stats.avgUpdateMs,
    });
    setError(null);
  };
  
  // Handle delta
  const handleDelta = (msg: ViewDeltaMessage) => {
    if (options.debug) {
      console.log(`[useDBSPServerView:${options.name || viewId}] Delta: ${msg.delta.length} changes`);
    }
    
    // Apply delta to map
    for (const [row, weight] of msg.delta as Array<[T, number]>) {
      const key = getKey(row);
      if (weight > 0) {
        resultsMapRef.current.set(key, row);
      } else {
        resultsMapRef.current.delete(key);
      }
    }
    
    // Update results array
    setResults(Array.from(resultsMapRef.current.values()));
    setCount(msg.count);
    setStats({
      lastUpdateMs: msg.stats.lastUpdateMs,
      totalUpdates: msg.stats.totalUpdates,
      avgUpdateMs: msg.stats.avgUpdateMs,
    });
  };
  
  // Handle error
  const handleError = (errorMsg: string) => {
    console.error(`[useDBSPServerView:${options.name || viewId}] Error:`, errorMsg);
    setError(errorMsg);
  };
  
  // Subscribe when connected
  useEffect(() => {
    if (!connected) return;
    
    if (options.debug) {
      console.log(`[useDBSPServerView:${options.name || viewId}] Subscribing...`);
    }
    
    const unsubscribe = subscribeView(
      viewId.replace(/:/g, '_'),
      query,
      sourceArray,
      {
        onSnapshot: handleSnapshot,
        onDelta: handleDelta,
        onError: handleError,
        joinMode: options.joinMode,
        maxRows: options.maxRows,
        maxResults: options.maxResults,
      }
    );
    
    return () => {
      if (options.debug) {
        console.log(`[useDBSPServerView:${options.name || viewId}] Unsubscribing...`);
      }
      unsubscribe();
      resultsMapRef.current.clear();
    };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [connected, query, JSON.stringify(sourceArray), options.joinMode, options.maxRows, options.maxResults]);
  
  // Deferred values for non-blocking updates
  const deferredResults = useDeferredValue(results);
  const deferredCount = useDeferredValue(count);
  
  return {
    results: deferredResults,
    count: deferredCount,
    stats,
    connected,
    error,
  };
}



