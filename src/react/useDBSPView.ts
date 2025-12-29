/**
 * useDBSPView - React Hook for Incremental SQL Views
 * ==================================================
 * 
 * Thin React wrapper around the platform-agnostic DBSPView class.
 * All logic is in core/DBSPView.ts - this just adds React integration.
 * 
 * ## Basic Usage
 * 
 * ```tsx
 * const pending = useDBSPView(orders, 
 *   "SELECT * FROM orders WHERE status = 'pending'"
 * );
 * 
 * console.log(pending.results);  // Array of matching rows
 * console.log(pending.count);    // Number of results
 * ```
 * 
 * ## View Chaining
 * 
 * Views can feed into other views:
 * 
 * ```tsx
 * const pending = useDBSPView(orders, 
 *   "SELECT * FROM orders WHERE status = 'pending'",
 *   { name: 'pending' }
 * );
 * 
 * const pendingByCustomer = useDBSPView(pending,
 *   "SELECT customerId, SUM(amount) as total FROM pending GROUP BY customerId"
 * );
 * ```
 * 
 * ## Joins
 * 
 * ```tsx
 * const enriched = useDBSPView([orders, customers],
 *   `SELECT o.*, c.name FROM orders o JOIN customers c ON o.customerId = c.id`
 * );
 * ```
 */

import { useRef, useMemo, useEffect, useId, useDeferredValue } from 'react';
import type { DBSPViewOptions, DBSPViewHandle, ViewStats } from '../core/types';
import { DBSPView, type DBSPViewConfig } from '../core/DBSPView';
import { useDBSPStoreVersion } from './store';

// ============ TYPES ============

/**
 * Minimal interface for stream-like objects that can be used as view inputs.
 * 
 * This uses a structural type without the problematic method signatures that
 * cause contravariance issues. TypeScript's function parameter contravariance
 * means that DBSPStreamHandle<SpecificType> would not be assignable to 
 * DBSPStreamHandle<Record<string, any>> due to _getKeyFn() signature differences.
 * 
 * By only specifying the properties we actually need for type checking and
 * using `any` for method signatures, we allow any stream handle to be passed.
 */
interface AnyStream {
  readonly name: string;
  readonly ready: boolean;
  readonly _identity: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  _getSchema(): any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  _getKeyFn(): any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  _getData(): any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  _subscribe(callback: any): () => void;
}

type StreamOrStreams = AnyStream | AnyStream[];

// ============ HOOK ============

/**
 * Create an incremental SQL view over one or more data streams.
 * 
 * React optimizations:
 * - Uses useSyncExternalStore for efficient subscription
 * - Uses useDeferredValue to prevent blocking interactions
 * - Uses getters/functions that read from ref to avoid stale closures
 * 
 * @param sources - Single stream or array of streams
 * @param query - SQL query string  
 * @param options - View options
 * @returns View handle with results, count, and stream interface
 */
export function useDBSPView<
  TIn extends Record<string, unknown>,
  TOut extends Record<string, unknown> = TIn
>(
  sources: StreamOrStreams,
  query: string,
  options: DBSPViewOptions = {}
): DBSPViewHandle<TOut> {
  const viewId = useId();
  
  // Normalize sources
  const sourceArray = Array.isArray(sources) ? sources : [sources];
  // CRITICAL: Use _identity, not just name, so views are recreated when sources are recreated
  const sourceKey = sourceArray.map(s => s._identity).join('::');
  
  // Store sources in ref to always have latest references
  const sourcesRef = useRef(sourceArray);
  sourcesRef.current = sourceArray;
  
  // Create stable config
  const config = useMemo((): DBSPViewConfig<TOut> => ({
    sources: sourcesRef.current,
    query,
    name: options.name,
    outputKey: options.outputKey as DBSPViewConfig<TOut>['outputKey'],
    joinMode: options.joinMode,
    maxResults: options.maxResults,
    maxRows: options.maxRows,
    debug: options.debug,
    viewId: viewId.replace(/:/g, '_'),
  }), [sourceKey, query, options.name, options.joinMode, options.maxResults, options.maxRows, options.debug, viewId]);
  
  // Create view instance - stored in ref to survive re-renders
  const viewRef = useRef<DBSPView<TIn, TOut> | null>(null);
  
  if (!viewRef.current) {
    viewRef.current = new DBSPView<TIn, TOut>(config);
  }
  
  // Recreate view when config changes
  const prevConfigRef = useRef(config);
  useEffect(() => {
    if (prevConfigRef.current !== config) {
      viewRef.current?.dispose();
      viewRef.current = new DBSPView<TIn, TOut>(config);
      prevConfigRef.current = config;
    }
  }, [config]);
        
  // Ensure view exists on mount/remount (handles StrictMode)
  useEffect(() => {
    if (!viewRef.current) {
      viewRef.current = new DBSPView<TIn, TOut>(config);
      console.log(`[useDBSPView] Recreated view after StrictMode remount`);
    }
  });
  
  // Cleanup on unmount - MUST set ref to null so StrictMode remount creates a new view
  useEffect(() => {
    return () => {
      viewRef.current?.dispose();
      viewRef.current = null;
    };
  }, []);
  
  // Subscribe to store updates (triggers re-render when data changes)
  const storeVersion = useDBSPStoreVersion();
  
  // ============ REACT OPTIMIZATIONS ============
  
  // Cache results array to prevent new arrays on every render
  const lastResultsRef = useRef<{ version: number; results: TOut[] }>({
    version: -1,
    results: [],
  });
  
  const results = useMemo(() => {
    void storeVersion; // Dependency
    
    if (lastResultsRef.current.version === storeVersion) {
      return lastResultsRef.current.results;
    }
    
    const view = viewRef.current;
    const newResults = view?.results ?? [];
    lastResultsRef.current = { version: storeVersion, results: newResults };
    return newResults;
  }, [storeVersion]);
  
  // Cache count
  const lastCountRef = useRef<{ version: number; count: number }>({ version: -1, count: 0 });
  
  const count = useMemo(() => {
    void storeVersion;
    
    const view = viewRef.current;
    const newCount = view?.count ?? 0;
    if (lastCountRef.current.count !== newCount || lastCountRef.current.version !== storeVersion) {
      lastCountRef.current = { version: storeVersion, count: newCount };
    }
    return newCount;
  }, [storeVersion]);
  
  // Cache stats object
  const lastStatsRef = useRef<{ version: number; totalUpdates: number; stats: ViewStats } | null>(null);
  
  const defaultStats: ViewStats = { lastUpdateMs: 0, totalUpdates: 0, avgUpdateMs: 0 };
  
  const stats = useMemo((): ViewStats => {
    void storeVersion;
    
    const view = viewRef.current;
    if (!view) return defaultStats;
    
    const newStats = view.stats;
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
    
  // Deferred values for non-blocking updates
  const deferredResults = useDeferredValue(results);
  const deferredCount = useDeferredValue(count);
  
  // ============ RETURN ============
  // CRITICAL: All methods/getters must use viewRef.current, NOT a captured variable!
  // This avoids stale closure issues with React StrictMode and HMR.
  
  const viewName = options.name || `view_${viewId.replace(/:/g, '_')}`;
  
  return {
    // Public interface - uses viewRef.current
    results: deferredResults,
    count: deferredCount,
    stats,
    getResultsPage: async (offset: number, limit: number) => {
      return viewRef.current?.getResultsPage(offset, limit) ?? [];
    },
    getAllResults: async () => {
      return viewRef.current?.getAllResults() ?? [];
    },
    
    // Stream interface - all use viewRef.current
    name: viewName,
    get ready() { return viewRef.current?.ready ?? false; },
    get _identity() { return viewRef.current?._identity ?? `${viewName}:pending`; },
    _getSchema: () => viewRef.current?._getSchema() ?? null,
    _getKeyFn: () => viewRef.current?._getKeyFn() ?? ((row: TOut) => JSON.stringify(row)),
    _getData: () => viewRef.current?._getData() ?? new Map(),
    _subscribe: (callback: (delta: Array<[TOut, number]>) => void) => {
      return viewRef.current?._subscribe(callback) ?? (() => {});
    },
  };
}
