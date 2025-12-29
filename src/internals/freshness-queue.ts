/**
 * Freshness Queue for DBSP
 * 
 * Provides data freshness guarantees for streaming data via:
 * - Circular buffer that drops oldest items when full
 * - Age-based filtering to drop stale messages
 * - Lag tracking metrics
 */

// ============ TYPES ============

export interface StreamMessage<T> {
  /** Unique sequence number for ordering */
  sequence: number;
  /** The data payload */
  data: T;
  /** Timestamp when message was created */
  timestamp: number;
  /** Optional message ID for deduplication */
  messageId?: string;
}

// ============ CIRCULAR BUFFER ============

/**
 * Fixed-size circular buffer that maintains freshness by dropping oldest items
 * when capacity is reached. Guarantees O(1) operations.
 */
export class CircularBuffer<T> {
  private buffer: (T | undefined)[];
  private head = 0;
  private tail = 0;
  private count = 0;
  private droppedCount = 0;
  private capacity: number;
  private onDrop?: (item: T) => void;
  
  constructor(capacity: number, onDrop?: (item: T) => void) {
    if (capacity < 1) throw new Error('Capacity must be at least 1');
    this.capacity = capacity;
    this.onDrop = onDrop;
    this.buffer = new Array(capacity);
  }
  
  push(item: T): boolean {
    let dropped = false;
    
    if (this.count === this.capacity) {
      const droppedItem = this.buffer[this.tail];
      if (droppedItem !== undefined && this.onDrop) {
        this.onDrop(droppedItem);
      }
      this.tail = (this.tail + 1) % this.capacity;
      this.droppedCount++;
      dropped = true;
    } else {
      this.count++;
    }
    
    this.buffer[this.head] = item;
    this.head = (this.head + 1) % this.capacity;
    
    return dropped;
  }
  
  pop(): T | undefined {
    if (this.count === 0) return undefined;
    
    const item = this.buffer[this.tail];
    this.buffer[this.tail] = undefined;
    this.tail = (this.tail + 1) % this.capacity;
    this.count--;
    
    return item;
  }
  
  peek(): T | undefined {
    if (this.count === 0) return undefined;
    return this.buffer[this.tail];
  }
  
  peekNewest(): T | undefined {
    if (this.count === 0) return undefined;
    const newestIdx = (this.head - 1 + this.capacity) % this.capacity;
    return this.buffer[newestIdx];
  }
  
  size(): number { return this.count; }
  isEmpty(): boolean { return this.count === 0; }
  isFull(): boolean { return this.count === this.capacity; }
  getCapacity(): number { return this.capacity; }
  getDroppedCount(): number { return this.droppedCount; }
  getUtilization(): number { return this.count / this.capacity; }
  
  clear(): void {
    this.buffer = new Array(this.capacity);
    this.head = 0;
    this.tail = 0;
    this.count = 0;
  }
  
  toArray(): T[] {
    const result: T[] = [];
    let idx = this.tail;
    for (let i = 0; i < this.count; i++) {
      const item = this.buffer[idx];
      if (item !== undefined) result.push(item);
      idx = (idx + 1) % this.capacity;
    }
    return result;
  }
}

// ============ FRESHNESS QUEUE ============

/**
 * Queue that maintains data freshness by:
 * 1. Using circular buffer to drop oldest when full
 * 2. Dropping messages older than maxAgeMs
 * 3. Tracking lag metrics
 */
export class FreshnessQueue<T> {
  private buffer: CircularBuffer<StreamMessage<T>>;
  private sequence = 0;
  private pendingResolvers: Array<() => void> = [];
  private droppedStale = 0;
  private droppedOverflow = 0;
  private capacity: number;
  private maxAgeMs: number | undefined;
  private onDropCallback?: (dropped: number, reason: 'overflow' | 'stale') => void;
  private debug: boolean;
  
  constructor(
    capacity: number,
    maxAgeMs: number | undefined,
    onDrop?: (dropped: number, reason: 'overflow' | 'stale') => void,
    debug = false
  ) {
    this.capacity = capacity;
    this.maxAgeMs = maxAgeMs;
    this.onDropCallback = onDrop;
    this.debug = debug;
    this.buffer = new CircularBuffer(capacity, () => {
      this.droppedOverflow++;
    });
  }
  
  enqueue(data: T, messageId?: string): number {
    const seq = this.sequence++;
    const message: StreamMessage<T> = {
      sequence: seq,
      data,
      timestamp: Date.now(),
      messageId,
    };
    
    const wasDropped = this.buffer.push(message);
    
    if (wasDropped && this.onDropCallback) {
      this.onDropCallback(1, 'overflow');
    }
    
    if (this.debug && wasDropped) {
      console.log(`[FreshnessQueue] Dropped oldest for freshness, capacity ${this.capacity}`);
    }
    
    while (this.pendingResolvers.length > 0) {
      const resolver = this.pendingResolvers.shift();
      resolver?.();
    }
    
    return seq;
  }
  
  enqueueBatch(items: T[], messageIdPrefix?: string): number[] {
    const sequences: number[] = [];
    for (let i = 0; i < items.length; i++) {
      const msgId = messageIdPrefix ? `${messageIdPrefix}_${i}` : undefined;
      sequences.push(this.enqueue(items[i], msgId));
    }
    return sequences;
  }
  
  async dequeue(maxCount: number, timeoutMs?: number): Promise<StreamMessage<T>[]> {
    if (this.buffer.isEmpty()) {
      await this.waitForData(timeoutMs);
    }
    
    const now = Date.now();
    const messages: StreamMessage<T>[] = [];
    let staleDropped = 0;
    
    while (messages.length < maxCount && !this.buffer.isEmpty()) {
      const msg = this.buffer.pop();
      if (!msg) break;
      
      if (this.maxAgeMs !== undefined && (now - msg.timestamp) > this.maxAgeMs) {
        staleDropped++;
        this.droppedStale++;
        continue;
      }
      
      messages.push(msg);
    }
    
    if (staleDropped > 0) {
      if (this.onDropCallback) {
        this.onDropCallback(staleDropped, 'stale');
      }
      if (this.debug) {
        console.log(`[FreshnessQueue] Dropped ${staleDropped} stale messages`);
      }
    }
    
    messages.sort((a, b) => a.sequence - b.sequence);
    
    return messages;
  }
  
  getLag(): number {
    const oldest = this.buffer.peek();
    if (!oldest) return 0;
    return Date.now() - oldest.timestamp;
  }
  
  isLagging(thresholdMs: number): boolean {
    return this.getLag() > thresholdMs;
  }
  
  dropStale(ageMs?: number): number {
    const maxAge = ageMs ?? this.maxAgeMs;
    if (maxAge === undefined) return 0;
    
    const now = Date.now();
    let dropped = 0;
    
    while (!this.buffer.isEmpty()) {
      const oldest = this.buffer.peek();
      if (!oldest) break;
      
      if ((now - oldest.timestamp) > maxAge) {
        this.buffer.pop();
        dropped++;
        this.droppedStale++;
      } else {
        break;
      }
    }
    
    if (dropped > 0 && this.onDropCallback) {
      this.onDropCallback(dropped, 'stale');
    }
    
    return dropped;
  }
  
  private waitForData(timeoutMs?: number): Promise<void> {
    if (!this.buffer.isEmpty()) {
      return Promise.resolve();
    }
    
    return new Promise((resolve) => {
      this.pendingResolvers.push(resolve);
      
      if (timeoutMs !== undefined) {
        setTimeout(() => {
          const idx = this.pendingResolvers.indexOf(resolve);
          if (idx >= 0) {
            this.pendingResolvers.splice(idx, 1);
            resolve();
          }
        }, timeoutMs);
      }
    });
  }
  
  size(): number { return this.buffer.size(); }
  getCapacity(): number { return this.buffer.getCapacity(); }
  getUtilization(): number { return this.buffer.getUtilization(); }
  getSequence(): number { return this.sequence; }
  getDroppedStale(): number { return this.droppedStale; }
  getDroppedOverflow(): number { return this.droppedOverflow; }
  getTotalDropped(): number { return this.droppedStale + this.droppedOverflow; }
  
  clear(): void {
    this.buffer.clear();
  }
  
  getStats(): {
    size: number;
    capacity: number;
    utilization: number;
    lagMs: number;
    droppedStale: number;
    droppedOverflow: number;
    totalDropped: number;
  } {
    return {
      size: this.size(),
      capacity: this.getCapacity(),
      utilization: this.getUtilization(),
      lagMs: this.getLag(),
      droppedStale: this.droppedStale,
      droppedOverflow: this.droppedOverflow,
      totalDropped: this.getTotalDropped(),
    };
  }
}

