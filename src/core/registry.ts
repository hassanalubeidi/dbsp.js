/**
 * DBSPRegistry - Central Registry for DBSP Sources and Views
 * ===========================================================
 * 
 * This singleton registry automatically tracks all DBSPSource and DBSPView
 * instances, enabling automatic visualization of data flow graphs.
 * 
 * ## Features
 * 
 * - Auto-registration of sources and views
 * - Dependency graph construction
 * - Live stats access
 * - Operator introspection
 * 
 * ## Usage
 * 
 * ```tsx
 * import { dbspRegistry } from 'dbsp/core';
 * 
 * // Get current graph
 * const { nodes, edges } = dbspRegistry.getGraph();
 * 
 * // Subscribe to changes
 * const unsubscribe = dbspRegistry.subscribe(() => {
 *   console.log('Registry updated!');
 * });
 * ```
 */

import type { SourceStats, ViewStats } from './types';

// ============ TYPES ============

/**
 * Operator types that can be extracted from SQL/circuits
 */
export type OperatorType = 
  | 'scan'      // Table scan (FROM)
  | 'filter'    // WHERE clause
  | 'project'   // SELECT columns
  | 'join'      // JOIN operation
  | 'aggregate' // GROUP BY aggregation
  | 'window'    // Window function
  | 'sort'      // ORDER BY
  | 'limit'     // LIMIT clause
  | 'distinct'  // DISTINCT
  | 'union'     // UNION/INTERSECT/EXCEPT
  | 'subquery'; // Subquery

/**
 * Information about a single operator in a view's execution plan
 */
export interface OperatorInfo {
  /** Operator type */
  type: OperatorType;
  /** SQL clause that generated this operator (e.g., "WHERE status = 'FILLED'") */
  sqlClause: string;
  /** Circuit operator name (e.g., "filter", "map", "join") */
  circuitOp: string;
  /** Complexity class (e.g., "O(|Δ|)", "O(|L|+|R|)") */
  complexity: string;
  /** Additional details */
  details?: string;
}

/**
 * Registry entry for a source or view
 */
export interface RegistryEntry {
  /** Unique identifier */
  id: string;
  /** Display name */
  name: string;
  /** Entry type */
  type: 'source' | 'view';
  /** SQL query (for views only) */
  query?: string;
  /** IDs of parent sources/views this depends on */
  sourceIds: string[];
  /** Names of parent sources/views */
  sourceNames: string[];
  /** Current statistics */
  stats: SourceStats | ViewStats;
  /** Parsed operators (for views) */
  operators: OperatorInfo[];
  /** Creation timestamp */
  createdAt: number;
  /** Whether the entry is ready */
  ready: boolean;
  /** Join mode (for views with joins) */
  joinMode?: string;
  /** Whether this is a join query */
  isJoin?: boolean;
  /** Row count */
  rowCount: number;
  /** Function to get current stats */
  getStats: () => SourceStats | ViewStats;
  /** Function to get current row count */
  getRowCount: () => number;
  /** Function to check if ready */
  isReady: () => boolean;
  /** Function to get data (for preview) - returns array of rows */
  getData?: () => Record<string, unknown>[];
}

/**
 * Edge in the dependency graph
 */
export interface RegistryEdge {
  /** Source entry ID */
  from: string;
  /** Target entry ID */
  to: string;
  /** Edge type */
  type: 'data' | 'chain';
}

/**
 * Complete dependency graph
 */
export interface RegistryGraph {
  nodes: RegistryEntry[];
  edges: RegistryEdge[];
}

// ============ REGISTRY CLASS ============

/**
 * Central registry for tracking all DBSP sources and views.
 * 
 * This enables automatic visualization of data flow graphs.
 */
export class DBSPRegistry {
  private entries = new Map<string, RegistryEntry>();
  private entriesByName = new Map<string, string>(); // name -> id mapping for deduplication
  private listeners = new Set<() => void>();
  private _version = 0;
  
  // ============ PUBLIC API ============
  
  /**
   * Current version number (increments on any change)
   */
  get version(): number {
    return this._version;
  }
  
  /**
   * Register a source or view
   * 
   * If an entry with the same NAME already exists, the old entry is replaced.
   * This handles React re-renders and StrictMode correctly.
   */
  register(entry: Omit<RegistryEntry, 'createdAt'>): void {
    // Deduplicate by name: if an entry with this name exists, remove it first
    const existingId = this.entriesByName.get(entry.name);
    if (existingId && existingId !== entry.id) {
      this.entries.delete(existingId);
    }
    
    const fullEntry: RegistryEntry = {
      ...entry,
      createdAt: Date.now(),
    };
    
    this.entries.set(entry.id, fullEntry);
    this.entriesByName.set(entry.name, entry.id);
    this._version++;
    this.notifyListeners();
  }
  
  /**
   * Update an existing entry's dynamic properties
   */
  update(id: string, updates: Partial<Pick<RegistryEntry, 'stats' | 'ready' | 'rowCount' | 'operators'>>): void {
    const entry = this.entries.get(id);
    if (entry) {
      Object.assign(entry, updates);
      this._version++;
      // Don't notify on every stats update - too noisy
      // Listeners should poll using getSnapshot
    }
  }
  
  /**
   * Unregister a source or view
   */
  unregister(id: string): void {
    const entry = this.entries.get(id);
    if (entry) {
      this.entries.delete(id);
      // Clean up name mapping if it still points to this id
      if (this.entriesByName.get(entry.name) === id) {
        this.entriesByName.delete(entry.name);
      }
      this._version++;
      this.notifyListeners();
    }
  }
  
  /**
   * Get a specific entry
   */
  getEntry(id: string): RegistryEntry | undefined {
    return this.entries.get(id);
  }
  
  /**
   * Get all sources
   */
  getSources(): RegistryEntry[] {
    return Array.from(this.entries.values()).filter(e => e.type === 'source');
  }
  
  /**
   * Get all views
   */
  getViews(): RegistryEntry[] {
    return Array.from(this.entries.values()).filter(e => e.type === 'view');
  }
  
  /**
   * Get all entries
   */
  getAllEntries(): RegistryEntry[] {
    return Array.from(this.entries.values());
  }
  
  /**
   * Build the complete dependency graph
   */
  getGraph(): RegistryGraph {
    const nodes = this.getAllEntries();
    const edges: RegistryEdge[] = [];
    
    // Build ID lookup for faster access
    const idByName = new Map<string, string>();
    for (const entry of nodes) {
      idByName.set(entry.name, entry.id);
    }
    
    // Build edges from sourceIds
    for (const entry of nodes) {
      for (const sourceId of entry.sourceIds) {
        // Check if sourceId is actually an ID or a name
        let fromId = sourceId;
        if (!this.entries.has(sourceId)) {
          // Try to find by name
          const foundId = idByName.get(sourceId);
          if (foundId) {
            fromId = foundId;
          } else {
            continue; // Skip if source not found
          }
        }
        
        edges.push({
          from: fromId,
          to: entry.id,
          type: entry.type === 'view' && this.entries.get(fromId)?.type === 'view' ? 'chain' : 'data',
        });
      }
    }
    
    return { nodes, edges };
  }
  
  /**
   * Get topologically sorted nodes (sources first, then views in dependency order)
   */
  getTopologicalOrder(): RegistryEntry[] {
    const nodes = this.getAllEntries();
    const visited = new Set<string>();
    const result: RegistryEntry[] = [];
    
    const visit = (entry: RegistryEntry) => {
      if (visited.has(entry.id)) return;
      visited.add(entry.id);
      
      // Visit dependencies first
      for (const sourceId of entry.sourceIds) {
        const source = this.entries.get(sourceId);
        if (source) {
          visit(source);
        }
      }
      
      result.push(entry);
    };
    
    for (const node of nodes) {
      visit(node);
    }
    
    return result;
  }
  
  /**
   * Subscribe to registry changes
   */
  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };
  
  /**
   * Get snapshot version (for React useSyncExternalStore)
   */
  getSnapshot = (): number => {
    return this._version;
  };
  
  /**
   * Get server snapshot (for SSR)
   */
  getServerSnapshot = (): number => {
    return this._version;
  };
  
  /**
   * Clear all entries (for testing)
   */
  clear(): void {
    this.entries.clear();
    this.entriesByName.clear();
    this._version++;
    this.notifyListeners();
  }
  
  /**
   * Get debug info
   */
  getDebugInfo(): { sources: number; views: number; version: number } {
    return {
      sources: this.getSources().length,
      views: this.getViews().length,
      version: this._version,
    };
  }
  
  // ============ PRIVATE ============
  
  private notifyListeners(): void {
    for (const listener of this.listeners) {
      listener();
    }
  }
}

// Singleton instance
export const dbspRegistry = new DBSPRegistry();

