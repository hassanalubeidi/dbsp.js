/**
 * DBSPStore - Platform-Agnostic State Manager with Adaptive Throttling
 * =====================================================================
 * 
 * This is the core store that coordinates all DBSP updates.
 * It's framework-agnostic - React integration is in react/hooks.ts.
 * 
 * ## Adaptive Throttling
 * 
 * The throttle rate adjusts based on actual render performance:
 * - If renders take 16ms → 60fps (max)
 * - If renders take 50ms → 20fps
 * - If renders take 100ms → 10fps
 * 
 * This ensures smooth UI while maximizing responsiveness.
 * 
 * ## Usage (Vanilla JS)
 * 
 * ```ts
 * import { dbspStore } from 'dbsp/core';
 * 
 * // Subscribe to changes
 * const unsubscribe = dbspStore.subscribe(() => {
 *   console.log('Store updated:', dbspStore.getSnapshot());
 * });
 * 
 * // Get current version
 * const version = dbspStore.getSnapshot();
 * ```
 * 
 * ## Usage (React - via react/hooks.ts)
 * 
 * ```tsx
 * import { useDBSPStoreVersion } from 'dbsp/react';
 * 
 * function MyComponent() {
 *   const version = useDBSPStoreVersion();
 *   // Component re-renders when version changes
 * }
 * ```
 */

export type StoreListener = () => void;

// Frame rate limits
const MIN_THROTTLE_MS = 16;   // 60fps max
const MAX_THROTTLE_MS = 200;  // 5fps min (safety floor)

/**
 * Central store for coordinating DBSP updates.
 * 
 * Instead of each source/view triggering independent updates,
 * all changes flow through this store, enabling:
 * 1. Batched updates (all components render together)
 * 2. Adaptive throttling (based on actual render performance)
 * 3. Framework-agnostic design (React, Vue, vanilla all work)
 */
export class DBSPStore {
  // Subscribers (from any framework or vanilla JS)
  private listeners = new Set<StoreListener>();
  
  // Throttle state - ADAPTIVE based on render performance
  private pendingNotify = false;
  private lastNotifyTime = 0;
  private throttleTimer: ReturnType<typeof setTimeout> | null = null;
  
  // Adaptive throttle tracking
  private renderTimes: number[] = [];
  private _currentThrottleMs = MIN_THROTTLE_MS;
  private lastRenderStart = 0;
  
  // Snapshot version - when this changes, all subscribers are notified
  private _snapshotVersion = 0;
  
  // FPS update callback (for UI display)
  private fpsUpdateListeners = new Set<(fps: number, avgRenderMs: number) => void>();
  
  // ============ PUBLIC API ============
  
  /**
   * Current snapshot version.
   * Use this to determine if state has changed.
   */
  get snapshotVersion(): number {
    return this._snapshotVersion;
  }
  
  /**
   * Current adaptive throttle interval in ms.
   */
  get currentThrottleMs(): number {
    return this._currentThrottleMs;
  }
  
  /**
   * Subscribe to store changes.
   * Returns an unsubscribe function.
   * 
   * @example
   * ```ts
   * const unsubscribe = dbspStore.subscribe(() => {
   *   console.log('Data changed!');
   * });
   * 
   * // Later...
   * unsubscribe();
   * ```
   */
  subscribe = (listener: StoreListener): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };
  
  /**
   * Get current snapshot version.
   * Compatible with React's useSyncExternalStore.
   */
  getSnapshot = (): number => {
    return this._snapshotVersion;
  };
  
  /**
   * Server snapshot (for SSR).
   * Compatible with React's useSyncExternalStore.
   */
  getServerSnapshot = (): number => {
    return this._snapshotVersion;
  };
  
  /**
   * Notify the store that data has changed.
   * Triggers a throttled update to all subscribers.
   * 
   * Call this from sources and views when their data changes.
   */
  notifyChange(): void {
    if (this.pendingNotify) return;
    this.pendingNotify = true;
    
    const now = Date.now();
    const timeSinceLastNotify = now - this.lastNotifyTime;
    
    if (timeSinceLastNotify >= this._currentThrottleMs) {
      // Enough time has passed - notify immediately via microtask
      this.lastNotifyTime = now;
      queueMicrotask(() => {
        this.pendingNotify = false;
        this.lastRenderStart = performance.now();
        this.notifyListeners();
      });
    } else {
      // Schedule for later
      if (this.throttleTimer) return;
      
      this.throttleTimer = setTimeout(() => {
        this.throttleTimer = null;
        this.pendingNotify = false;
        this.lastNotifyTime = Date.now();
        this.lastRenderStart = performance.now();
        this.notifyListeners();
      }, this._currentThrottleMs - timeSinceLastNotify);
    }
  }
  
  // ============ ADAPTIVE THROTTLE ============
  
  /**
   * Record how long a render cycle took.
   * Call this after the UI has finished updating.
   * 
   * The store uses this to adaptively adjust the throttle rate.
   */
  recordRenderTime(ms: number): void {
    this.renderTimes.push(ms);
    
    // Keep last 10 samples
    if (this.renderTimes.length > 10) {
      this.renderTimes.shift();
    }
    
    // Calculate new throttle based on render time
    this.updateThrottle();
  }
  
  private updateThrottle(): void {
    if (this.renderTimes.length === 0) return;
    
    // Use 90th percentile of render times (handle outliers)
    const sorted = [...this.renderTimes].sort((a, b) => a - b);
    const p90Index = Math.floor(sorted.length * 0.9);
    const p90RenderTime = sorted[p90Index];
    
    // Throttle should be at least the render time + small buffer
    // This ensures we don't start a new render before the last one finishes
    const targetThrottle = Math.ceil(p90RenderTime * 1.2); // 20% buffer
    
    // Clamp to bounds
    this._currentThrottleMs = Math.max(
      MIN_THROTTLE_MS,
      Math.min(MAX_THROTTLE_MS, targetThrottle)
    );
    
    // Notify FPS listeners
    const fps = Math.round(1000 / this._currentThrottleMs);
    const avgRenderMs = this.renderTimes.reduce((a, b) => a + b, 0) / this.renderTimes.length;
    for (const listener of this.fpsUpdateListeners) {
      listener(fps, avgRenderMs);
    }
  }
  
  private notifyListeners(): void {
    this._snapshotVersion++;
    // Notify all subscribers - they will all update together
    for (const listener of this.listeners) {
      listener();
    }
    
    // Schedule render time measurement
    // In browser, use requestAnimationFrame for accurate timing
    if (typeof requestAnimationFrame !== 'undefined') {
      requestAnimationFrame(() => {
        const renderTime = performance.now() - this.lastRenderStart;
        this.recordRenderTime(renderTime);
      });
    }
  }
  
  // ============ FPS MONITORING ============
  
  /**
   * Subscribe to FPS updates (for UI display).
   */
  onFpsUpdate(callback: (fps: number, avgRenderMs: number) => void): () => void {
    this.fpsUpdateListeners.add(callback);
    return () => this.fpsUpdateListeners.delete(callback);
  }
  
  // ============ DEBUG ============
  
  /**
   * Get current store statistics for debugging.
   */
  getStats() {
    const avgRenderTime = this.renderTimes.length > 0
      ? this.renderTimes.reduce((a, b) => a + b, 0) / this.renderTimes.length
      : 0;
    
    return {
      listeners: this.listeners.size,
      snapshotVersion: this._snapshotVersion,
      currentThrottleMs: this._currentThrottleMs,
      currentFps: Math.round(1000 / this._currentThrottleMs),
      avgRenderTimeMs: avgRenderTime.toFixed(2),
      renderSamples: this.renderTimes.length,
    };
  }
  
  /**
   * Reset the store (for testing).
   */
  reset(): void {
    this._snapshotVersion = 0;
    this.renderTimes = [];
    this._currentThrottleMs = MIN_THROTTLE_MS;
    this.pendingNotify = false;
    if (this.throttleTimer) {
      clearTimeout(this.throttleTimer);
      this.throttleTimer = null;
    }
  }
}

// Singleton instance - shared across the application
export const dbspStore = new DBSPStore();

