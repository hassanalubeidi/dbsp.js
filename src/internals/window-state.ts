/**
 * Optimized Window Function State
 * ================================
 * 
 * High-performance data structures for SQL window functions.
 * These enable O(1) per-row processing instead of O(n) re-scanning.
 * 
 * ## Data Structures
 * 
 * ### MonotonicDeque - For MIN/MAX (O(1) amortized)
 * 
 * Classic algorithm: elements that can never be the answer are pruned early.
 * For MIN: if new element X is smaller than existing element Y, then Y can
 * never be the minimum (X is both smaller AND will stay longer).
 * 
 * ### RunningAggregate - For SUM/AVG/COUNT (O(1))
 * 
 * Uses a ring buffer to track the sliding window. Maintains running totals
 * by subtracting the outgoing value and adding the incoming value.
 * 
 * ### PartitionedWindowState - Multi-partition management
 * 
 * Manages separate window state per partition key. Used when SQL has
 * `PARTITION BY` in the window specification.
 * 
 * ## When These Are Used
 * 
 * The SQL compiler automatically uses these optimized structures when:
 * - Window has ROWS BETWEEN n PRECEDING AND CURRENT ROW
 * - Window uses SUM, AVG, COUNT, MIN, MAX, LAG, or LEAD
 * 
 * Falls back to O(n) for RANK, DENSE_RANK, NTILE (require full partition).
 * 
 * ## References
 * 
 * - "Sliding-Window Aggregation Algorithms" - various CS papers
 * - Apache Flink window internals
 * - Materialize differential dataflow
 */

// ============ MONOTONIC DEQUE FOR MIN/MAX ============

/**
 * Monotonic Deque for O(1) amortized sliding window MIN/MAX
 * 
 * Key insight: Elements that can never be the answer are pruned immediately.
 * For MIN: If a new element is smaller than existing elements, those existing
 * elements can never be the minimum (since the new element is both smaller AND newer).
 */
export class MonotonicDeque {
  private deque: { value: number; idx: number }[] = [];
  private windowSize: number;
  private currentIdx = 0;
  private mode: 'min' | 'max';
  
  constructor(windowSize: number, mode: 'min' | 'max') {
    this.windowSize = windowSize;
    this.mode = mode;
  }
  
  /**
   * Add a value and get the current MIN/MAX in O(1) amortized time
   */
  add(value: number): number {
    // Pop elements that can never be the answer
    // For MIN: pop larger elements (they can never be minimum since we're newer AND smaller)
    // For MAX: pop smaller elements
    const shouldPop = this.mode === 'min'
      ? (existing: number) => existing >= value
      : (existing: number) => existing <= value;
    
    while (this.deque.length > 0 && shouldPop(this.deque[this.deque.length - 1].value)) {
      this.deque.pop();
    }
    
    // Add new element
    this.deque.push({ value, idx: this.currentIdx });
    
    // Remove elements outside window
    while (this.deque.length > 0 && this.deque[0].idx <= this.currentIdx - this.windowSize) {
      this.deque.shift();
    }
    
    this.currentIdx++;
    
    // Front of deque is always the answer
    return this.deque[0]?.value ?? value;
  }
  
  /**
   * Get current MIN/MAX without adding
   */
  current(): number | null {
    return this.deque[0]?.value ?? null;
  }
  
  /**
   * Reset state
   */
  reset(): void {
    this.deque = [];
    this.currentIdx = 0;
  }
  
  /**
   * Get window size
   */
  getWindowSize(): number {
    return this.windowSize;
  }
}

// ============ RUNNING AGGREGATE FOR SUM/AVG/COUNT ============

/**
 * Running Aggregate for O(1) sliding window SUM/AVG/COUNT
 * 
 * Uses a ring buffer to track values in the window.
 * When the window slides, we subtract the leaving value and add the entering value.
 */
export class RunningAggregate {
  private buffer: (number | null)[];
  private bufferIdx = 0;
  private windowSize: number;
  private sum = 0;
  private count = 0;
  private nonNullCount = 0;
  
  constructor(windowSize: number) {
    this.windowSize = windowSize;
    this.buffer = new Array(windowSize).fill(null);
  }
  
  /**
   * Add a value to the window, returns new aggregate values
   */
  add(value: number | null): { sum: number; avg: number | null; count: number } {
    // Remove the value that's leaving the window
    const leaving = this.buffer[this.bufferIdx];
    if (leaving !== null) {
      this.sum -= leaving;
      this.nonNullCount--;
    }
    if (this.count === this.windowSize) {
      // Buffer was full, so we're replacing
    } else {
      this.count++;
    }
    
    // Add the new value
    this.buffer[this.bufferIdx] = value;
    if (value !== null) {
      this.sum += value;
      this.nonNullCount++;
    }
    
    // Move buffer pointer
    this.bufferIdx = (this.bufferIdx + 1) % this.windowSize;
    
    return {
      sum: this.sum,
      avg: this.nonNullCount > 0 ? this.sum / this.nonNullCount : null,
      count: this.count,
    };
  }
  
  /**
   * Get current values without adding
   */
  current(): { sum: number; avg: number | null; count: number } {
    return {
      sum: this.sum,
      avg: this.nonNullCount > 0 ? this.sum / this.nonNullCount : null,
      count: this.count,
    };
  }
  
  /**
   * Reset state
   */
  reset(): void {
    this.buffer = new Array(this.windowSize).fill(null);
    this.bufferIdx = 0;
    this.sum = 0;
    this.count = 0;
    this.nonNullCount = 0;
  }
}

// ============ INCREMENTAL WINDOW STATE ============

/**
 * Combined window state that maintains optimized data structures
 * for all window functions in a partition.
 */
export interface WindowFunctionSpec {
  type: 'SUM' | 'AVG' | 'COUNT' | 'MIN' | 'MAX' | 'LAG' | 'LEAD' | 'ROW_NUMBER';
  column?: string;
  frameSize: number;  // For ROWS BETWEEN n PRECEDING AND CURRENT ROW
  offset?: number;    // For LAG/LEAD
  alias: string;
}

export class IncrementalWindowState {
  private specs: WindowFunctionSpec[];
  private minDeques: Map<string, MonotonicDeque> = new Map();
  private maxDeques: Map<string, MonotonicDeque> = new Map();
  private runningAggs: Map<string, RunningAggregate> = new Map();
  
  // For LAG/LEAD, we need to maintain ordered rows
  private orderedRows: any[] = [];
  private rowIndex = 0;
  
  constructor(specs: WindowFunctionSpec[]) {
    this.specs = specs;
    
    // Initialize data structures for each spec
    for (const spec of specs) {
      switch (spec.type) {
        case 'MIN':
          this.minDeques.set(spec.alias, new MonotonicDeque(spec.frameSize, 'min'));
          break;
        case 'MAX':
          this.maxDeques.set(spec.alias, new MonotonicDeque(spec.frameSize, 'max'));
          break;
        case 'SUM':
        case 'AVG':
        case 'COUNT':
          this.runningAggs.set(spec.alias, new RunningAggregate(spec.frameSize));
          break;
      }
    }
  }
  
  /**
   * Process a new row and compute all window function values
   * 
   * Returns the row augmented with window function results.
   * Complexity: O(1) for each function (amortized for MIN/MAX)
   */
  processRow(row: Record<string, any>): Record<string, any> {
    const result = { ...row };
    
    for (const spec of this.specs) {
      switch (spec.type) {
        case 'MIN': {
          const deque = this.minDeques.get(spec.alias)!;
          const value = Number(row[spec.column!]) || 0;
          result[spec.alias] = deque.add(value);
          break;
        }
        
        case 'MAX': {
          const deque = this.maxDeques.get(spec.alias)!;
          const value = Number(row[spec.column!]) || 0;
          result[spec.alias] = deque.add(value);
          break;
        }
        
        case 'SUM': {
          const agg = this.runningAggs.get(spec.alias)!;
          const value = Number(row[spec.column!]) || 0;
          const { sum } = agg.add(value);
          result[spec.alias] = sum;
          break;
        }
        
        case 'AVG': {
          const agg = this.runningAggs.get(spec.alias)!;
          const value = Number(row[spec.column!]) || 0;
          const { avg } = agg.add(value);
          result[spec.alias] = avg;
          break;
        }
        
        case 'COUNT': {
          const agg = this.runningAggs.get(spec.alias)!;
          const { count } = agg.add(1);
          result[spec.alias] = count;
          break;
        }
        
        case 'ROW_NUMBER': {
          this.rowIndex++;
          result[spec.alias] = this.rowIndex;
          break;
        }
        
        case 'LAG': {
          const offset = spec.offset ?? 1;
          const idx = this.orderedRows.length - offset;
          result[spec.alias] = idx >= 0 ? this.orderedRows[idx][spec.column!] : null;
          // Store row for LAG (only when needed)
          this.orderedRows.push(row);
          break;
        }
        
        case 'LEAD': {
          // LEAD is tricky - we can't compute it until future rows arrive
          // For now, return null and let the caller handle backfilling
          result[spec.alias] = null;
          break;
        }
      }
    }
    
    // Prune old rows for LAG/LEAD - only if we have LAG specs
    const hasLag = this.specs.some(s => s.type === 'LAG');
    if (hasLag && this.orderedRows.length > 0) {
      const maxOffset = Math.max(...this.specs.filter(s => s.type === 'LAG').map(s => s.offset ?? 1));
      // Keep only what we need for the largest LAG offset
      if (this.orderedRows.length > maxOffset + 10) {
        this.orderedRows = this.orderedRows.slice(-maxOffset - 5);
      }
    }
    
    return result;
  }
  
  /**
   * Reset all state
   */
  reset(): void {
    for (const deque of this.minDeques.values()) deque.reset();
    for (const deque of this.maxDeques.values()) deque.reset();
    for (const agg of this.runningAggs.values()) agg.reset();
    this.orderedRows = [];
    this.rowIndex = 0;
  }
}

// ============ PARTITION-AWARE WINDOW STATE ============

/**
 * Manages window state across multiple partitions
 */
export class PartitionedWindowState {
  private partitions: Map<string, IncrementalWindowState> = new Map();
  private specs: WindowFunctionSpec[];
  private partitionKeyFn: (row: any) => string;
  
  constructor(
    specs: WindowFunctionSpec[],
    partitionKeyFn: (row: any) => string
  ) {
    this.specs = specs;
    this.partitionKeyFn = partitionKeyFn;
  }
  
  /**
   * Process a row, automatically routing to correct partition
   */
  processRow(row: Record<string, any>): Record<string, any> {
    const partitionKey = this.partitionKeyFn(row);
    
    if (!this.partitions.has(partitionKey)) {
      this.partitions.set(partitionKey, new IncrementalWindowState(this.specs));
    }
    
    return this.partitions.get(partitionKey)!.processRow(row);
  }
  
  /**
   * Reset all partitions
   */
  reset(): void {
    this.partitions.clear();
  }
  
  /**
   * Get number of partitions (for monitoring)
   */
  partitionCount(): number {
    return this.partitions.size;
  }
}

