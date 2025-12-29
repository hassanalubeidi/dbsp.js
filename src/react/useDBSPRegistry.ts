/**
 * useDBSPRegistry - React Hook for DBSP Registry
 * ================================================
 * 
 * This hook provides React integration with the central DBSP registry,
 * enabling automatic visualization of all data sources and views.
 * 
 * ## Features
 * 
 * - Automatic discovery of all sources and views
 * - Live updates when sources/views are added/removed
 * - Dependency graph for visualization
 * - Real-time stats access
 * 
 * ## Usage
 * 
 * ```tsx
 * import { useDBSPRegistry } from 'dbsp/react';
 * 
 * function DataFlowViz() {
 *   const { sources, views, graph, refresh } = useDBSPRegistry();
 *   
 *   return (
 *     <div>
 *       <h3>Sources ({sources.length})</h3>
 *       {sources.map(s => <div key={s.id}>{s.name}</div>)}
 *       
 *       <h3>Views ({views.length})</h3>
 *       {views.map(v => <div key={v.id}>{v.name}</div>)}
 *     </div>
 *   );
 * }
 * ```
 */

import { useSyncExternalStore, useCallback, useMemo, useState, useEffect } from 'react';
import { dbspRegistry, type RegistryEntry, type RegistryGraph, type RegistryEdge } from '../core/registry';

// ============ TYPES ============

export interface UseDBSPRegistryResult {
  /** All registered sources */
  sources: RegistryEntry[];
  /** All registered views */
  views: RegistryEntry[];
  /** Complete dependency graph */
  graph: RegistryGraph;
  /** Current registry version (changes on add/remove) */
  version: number;
  /** Refresh stats for all entries */
  refresh: () => RegistryEntry[];
  /** Get entry by ID */
  getEntry: (id: string) => RegistryEntry | undefined;
  /** Topologically sorted entries (sources first, then views in dependency order) */
  sorted: RegistryEntry[];
  /** Debug info */
  debug: { sources: number; views: number; version: number };
}

// ============ HOOK ============

export interface UseDBSPRegistryOptions {
  /** 
   * Refresh interval for live stats updates (in ms).
   * Set to 0 to disable polling.
   * Default: 0 (no polling - only reacts to structure changes)
   */
  refreshIntervalMs?: number;
}

/**
 * React hook for accessing the DBSP registry.
 * 
 * Automatically subscribes to registry changes and provides
 * the current state of all sources and views.
 * 
 * @param options.refreshIntervalMs - Polling interval for live stats (0 = disabled)
 */
export function useDBSPRegistry(options: UseDBSPRegistryOptions = {}): UseDBSPRegistryResult {
  const { refreshIntervalMs = 0 } = options;
  
  // Subscribe to registry changes using useSyncExternalStore
  const version = useSyncExternalStore(
    dbspRegistry.subscribe,
    dbspRegistry.getSnapshot,
    dbspRegistry.getServerSnapshot
  );
  
  // Tick counter for live stats refresh (triggers re-render without structural changes)
  const [tick, setTick] = useState(0);
  
  // Polling for live stats updates
  useEffect(() => {
    if (refreshIntervalMs <= 0) return;
    
    const interval = setInterval(() => {
      setTick(t => t + 1);
    }, refreshIntervalMs);
    
    return () => clearInterval(interval);
  }, [refreshIntervalMs]);
  
  // Combined version + tick for memoization dependencies
  const refreshKey = `${version}-${tick}`;
  
  // Memoize getEntry
  const getEntry = useCallback((id: string) => {
    return dbspRegistry.getEntry(id);
  }, []);
  
  // Memoize refresh function
  const refresh = useCallback(() => {
    return dbspRegistry.getAllEntries();
  }, []);
  
  // Compute derived values when version or tick changes
  const sources = useMemo(() => {
    return dbspRegistry.getSources();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [refreshKey]);
  
  const views = useMemo(() => {
    return dbspRegistry.getViews();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [refreshKey]);
  
  const graph = useMemo(() => {
    return dbspRegistry.getGraph();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [refreshKey]);
  
  const sorted = useMemo(() => {
    return dbspRegistry.getTopologicalOrder();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [refreshKey]);
  
  const debug = useMemo(() => {
    return dbspRegistry.getDebugInfo();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [refreshKey]);
  
  return {
    sources,
    views,
    graph,
    version,
    refresh,
    getEntry,
    sorted,
    debug,
  };
}

// ============ UTILITY HOOKS ============

/**
 * Hook to get live stats for a specific entry.
 * 
 * Stats are refreshed every interval (default 100ms).
 */
export function useRegistryStats(entryId: string, refreshIntervalMs = 100): {
  stats: RegistryEntry['stats'] | null;
  rowCount: number;
  ready: boolean;
} {
  const entry = useSyncExternalStore(
    useCallback((callback) => {
      const interval = setInterval(callback, refreshIntervalMs);
      const unsubscribe = dbspRegistry.subscribe(callback);
      return () => {
        clearInterval(interval);
        unsubscribe();
      };
    }, [refreshIntervalMs]),
    useCallback(() => {
      const e = dbspRegistry.getEntry(entryId);
      return e ? {
        stats: e.getStats(),
        rowCount: e.getRowCount(),
        ready: e.isReady(),
      } : null;
    }, [entryId]),
    useCallback(() => null, [])
  );
  
  return entry ?? { stats: null, rowCount: 0, ready: false };
}

/**
 * Hook to compute layout positions for the dependency graph.
 * 
 * Uses a simple Dagre-like algorithm to compute X/Y positions.
 */
export function useGraphLayout(graph: RegistryGraph): {
  positions: Map<string, { x: number; y: number }>;
  width: number;
  height: number;
} {
  return useMemo(() => {
    const { nodes, edges } = graph;
    const positions = new Map<string, { x: number; y: number }>();
    
    if (nodes.length === 0) {
      return { positions, width: 0, height: 0 };
    }
    
    // Compute depth of each node (0 = sources, 1+ = views)
    const depths = new Map<string, number>();
    const childrenOf = new Map<string, string[]>();
    
    // Build adjacency list
    for (const edge of edges) {
      if (!childrenOf.has(edge.from)) {
        childrenOf.set(edge.from, []);
      }
      childrenOf.get(edge.from)!.push(edge.to);
    }
    
    // Compute depths via BFS
    const queue: string[] = [];
    for (const node of nodes) {
      if (node.type === 'source') {
        depths.set(node.id, 0);
        queue.push(node.id);
      }
    }
    
    while (queue.length > 0) {
      const current = queue.shift()!;
      const currentDepth = depths.get(current) ?? 0;
      
      const children = childrenOf.get(current) || [];
      for (const child of children) {
        const existingDepth = depths.get(child) ?? -1;
        if (currentDepth + 1 > existingDepth) {
          depths.set(child, currentDepth + 1);
          queue.push(child);
        }
      }
    }
    
    // Group nodes by depth
    const byDepth = new Map<number, RegistryEntry[]>();
    for (const node of nodes) {
      const depth = depths.get(node.id) ?? 0;
      if (!byDepth.has(depth)) {
        byDepth.set(depth, []);
      }
      byDepth.get(depth)!.push(node);
    }
    
    // Layout constants
    const NODE_WIDTH = 280;
    const NODE_HEIGHT = 100;
    const HORIZONTAL_GAP = 320;
    const VERTICAL_GAP = 120;
    const PADDING = 50;
    
    // Compute positions
    const maxDepth = Math.max(...Array.from(depths.values()), 0);
    let maxY = 0;
    
    for (let depth = 0; depth <= maxDepth; depth++) {
      const nodesAtDepth = byDepth.get(depth) || [];
      const x = PADDING + depth * HORIZONTAL_GAP;
      
      for (let i = 0; i < nodesAtDepth.length; i++) {
        const y = PADDING + i * VERTICAL_GAP;
        positions.set(nodesAtDepth[i].id, { x, y });
        maxY = Math.max(maxY, y);
      }
    }
    
    return {
      positions,
      width: PADDING * 2 + (maxDepth + 1) * HORIZONTAL_GAP + NODE_WIDTH,
      height: maxY + NODE_HEIGHT + PADDING,
    };
  }, [graph]);
}

// ============ HELPERS ============

/**
 * Get edges connecting to a specific node.
 */
export function getNodeEdges(graph: RegistryGraph, nodeId: string): {
  incoming: RegistryEdge[];
  outgoing: RegistryEdge[];
} {
  return {
    incoming: graph.edges.filter(e => e.to === nodeId),
    outgoing: graph.edges.filter(e => e.from === nodeId),
  };
}

/**
 * Get lineage of a node (all ancestors).
 */
export function getLineage(graph: RegistryGraph, nodeId: string): string[] {
  const lineage = new Set<string>();
  const queue = [nodeId];
  
  while (queue.length > 0) {
    const current = queue.shift()!;
    for (const edge of graph.edges) {
      if (edge.to === current && !lineage.has(edge.from)) {
        lineage.add(edge.from);
        queue.push(edge.from);
      }
    }
  }
  
  return Array.from(lineage);
}

/**
 * Get descendants of a node (all children recursively).
 */
export function getDescendants(graph: RegistryGraph, nodeId: string): string[] {
  const descendants = new Set<string>();
  const queue = [nodeId];
  
  while (queue.length > 0) {
    const current = queue.shift()!;
    for (const edge of graph.edges) {
      if (edge.from === current && !descendants.has(edge.to)) {
        descendants.add(edge.to);
        queue.push(edge.to);
      }
    }
  }
  
  return Array.from(descendants);
}

