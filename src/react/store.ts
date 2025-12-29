/**
 * React Integration for DBSPStore
 * ================================
 * 
 * This re-exports the platform-agnostic store and adds React-specific hooks.
 * 
 * @example
 * ```tsx
 * import { useDBSPStoreVersion, dbspStore } from 'dbsp/react';
 * 
 * function MyComponent() {
 *   const version = useDBSPStoreVersion();
 *   // Component re-renders when version changes
 * }
 * ```
 */

import { useSyncExternalStore } from 'react';
import { dbspStore } from '../core/store';

// Re-export store for direct access
export { dbspStore } from '../core/store';
export type { StoreListener } from '../core/store';

/**
 * React hook to subscribe to DBSP store updates.
 * 
 * Use this instead of useState for data version tracking.
 * All components using this hook will re-render together when data changes.
 * 
 * @returns Current snapshot version (use as dependency for useMemo)
 * 
 * @example
 * ```tsx
 * function MyView() {
 *   const storeVersion = useDBSPStoreVersion();
 *   
 *   const results = useMemo(() => {
 *     // Recompute when store updates
 *     return expensiveComputation();
 *   }, [storeVersion]);
 * }
 * ```
 */
export function useDBSPStoreVersion(): number {
  return useSyncExternalStore(
    dbspStore.subscribe,
    dbspStore.getSnapshot,
    dbspStore.getServerSnapshot
  );
}
