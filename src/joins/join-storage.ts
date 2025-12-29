/**
 * IndexedDB Storage for Join Results
 * 
 * Streams join results to IndexedDB to avoid memory explosion.
 * Provides paginated access and proper cleanup on unmount.
 */

const DB_NAME = 'dbsp-join-results';
const DB_VERSION = 1;

export interface JoinResultEntry<TLeft, TRight> {
  id: number; // Auto-increment key
  viewId: string;
  left: TLeft;
  right: TRight;
  joinKey: string;
  timestamp: number;
}

/**
 * IndexedDB storage manager for join results
 */
export class JoinResultStorage<
  TLeft extends Record<string, unknown>,
  TRight extends Record<string, unknown>
> {
  private db: IDBDatabase | null = null;
  private dbPromise: Promise<IDBDatabase> | null = null;
  private viewId: string;
  private resultCount = 0;
  private isCleared = false;
  
  // Batch write buffer for performance
  private writeBuffer: Array<{ left: TLeft; right: TRight; joinKey: string }> = [];
  private flushTimeout: ReturnType<typeof setTimeout> | null = null;
  private flushPromise: Promise<void> | null = null;
  
  // Configuration
  private batchSize = 500; // Flush every 500 results
  private flushIntervalMs = 100; // Or every 100ms
  
  constructor(viewId: string) {
    this.viewId = viewId;
  }
  
  /**
   * Initialize the database connection
   */
  async init(): Promise<void> {
    if (this.db) return;
    if (this.dbPromise) {
      await this.dbPromise;
      return;
    }
    
    this.dbPromise = new Promise((resolve, reject) => {
      const request = indexedDB.open(DB_NAME, DB_VERSION);
      
      request.onerror = () => {
        console.error('[JoinResultStorage] Failed to open IndexedDB:', request.error);
        reject(request.error);
      };
      
      request.onsuccess = () => {
        this.db = request.result;
        resolve(this.db);
      };
      
      request.onupgradeneeded = (event) => {
        const db = (event.target as IDBOpenDBRequest).result;
        
        // Create object store if it doesn't exist
        if (!db.objectStoreNames.contains('results')) {
          const store = db.createObjectStore('results', { 
            keyPath: 'id', 
            autoIncrement: true 
          });
          
          // Index by viewId for efficient cleanup and queries
          store.createIndex('viewId', 'viewId', { unique: false });
          
          // Compound index for viewId + id (for pagination)
          store.createIndex('viewId_id', ['viewId', 'id'], { unique: false });
        }
      };
    });
    
    await this.dbPromise;
  }
  
  /**
   * Add a join result (buffered for performance)
   */
  add(left: TLeft, right: TRight, joinKey: string): void {
    if (this.isCleared) return;
    
    this.writeBuffer.push({ left, right, joinKey });
    this.resultCount++;
    
    // Flush if buffer is full
    if (this.writeBuffer.length >= this.batchSize) {
      this.flush();
    } else if (!this.flushTimeout) {
      // Schedule flush
      this.flushTimeout = setTimeout(() => {
        this.flush();
      }, this.flushIntervalMs);
    }
  }
  
  /**
   * Flush buffered results to IndexedDB
   */
  async flush(): Promise<void> {
    if (this.flushTimeout) {
      clearTimeout(this.flushTimeout);
      this.flushTimeout = null;
    }
    
    if (this.writeBuffer.length === 0 || !this.db || this.isCleared) return;
    
    // Take current buffer
    const toFlush = this.writeBuffer;
    this.writeBuffer = [];
    
    // Wait for any pending flush
    if (this.flushPromise) {
      await this.flushPromise;
    }
    
    this.flushPromise = this.doFlush(toFlush);
    await this.flushPromise;
    this.flushPromise = null;
  }
  
  private async doFlush(
    entries: Array<{ left: TLeft; right: TRight; joinKey: string }>
  ): Promise<void> {
    if (!this.db || this.isCleared) return;
    
    return new Promise((resolve, reject) => {
      const tx = this.db!.transaction('results', 'readwrite');
      const store = tx.objectStore('results');
      
      const timestamp = Date.now();
      
      for (const entry of entries) {
        const record: Omit<JoinResultEntry<TLeft, TRight>, 'id'> = {
          viewId: this.viewId,
          left: entry.left,
          right: entry.right,
          joinKey: entry.joinKey,
          timestamp,
        };
        store.add(record);
      }
      
      tx.oncomplete = () => resolve();
      tx.onerror = () => {
        console.error('[JoinResultStorage] Flush failed:', tx.error);
        reject(tx.error);
      };
    });
  }
  
  /**
   * Get total result count
   */
  get count(): number {
    return this.resultCount;
  }
  
  /**
   * Get paginated results
   */
  async getPage(offset: number, limit: number): Promise<[TLeft, TRight][]> {
    // Ensure all pending writes are flushed first
    await this.flush();
    
    if (!this.db) return [];
    
    return new Promise((resolve, reject) => {
      const tx = this.db!.transaction('results', 'readonly');
      const store = tx.objectStore('results');
      const index = store.index('viewId');
      
      const results: [TLeft, TRight][] = [];
      let skipped = 0;
      
      const request = index.openCursor(IDBKeyRange.only(this.viewId));
      
      request.onsuccess = (event) => {
        const cursor = (event.target as IDBRequest<IDBCursorWithValue>).result;
        
        if (!cursor) {
          resolve(results);
          return;
        }
        
        if (skipped < offset) {
          skipped++;
          cursor.continue();
          return;
        }
        
        if (results.length < limit) {
          const entry = cursor.value as JoinResultEntry<TLeft, TRight>;
          results.push([entry.left, entry.right]);
          cursor.continue();
        } else {
          resolve(results);
        }
      };
      
      request.onerror = () => reject(request.error);
    });
  }
  
  /**
   * Get all results (careful with memory!)
   */
  async getAllResults(): Promise<[TLeft, TRight][]> {
    await this.flush();
    
    if (!this.db) return [];
    
    return new Promise((resolve, reject) => {
      const tx = this.db!.transaction('results', 'readonly');
      const store = tx.objectStore('results');
      const index = store.index('viewId');
      
      const request = index.getAll(IDBKeyRange.only(this.viewId));
      
      request.onsuccess = () => {
        const entries = request.result as JoinResultEntry<TLeft, TRight>[];
        resolve(entries.map(e => [e.left, e.right]));
      };
      
      request.onerror = () => reject(request.error);
    });
  }
  
  /**
   * Clear all results for this view
   */
  async clear(): Promise<void> {
    this.isCleared = true;
    
    // Clear pending writes
    if (this.flushTimeout) {
      clearTimeout(this.flushTimeout);
      this.flushTimeout = null;
    }
    this.writeBuffer = [];
    this.resultCount = 0;
    
    // Wait for any pending flush
    if (this.flushPromise) {
      await this.flushPromise;
    }
    
    if (!this.db) return;
    
    return new Promise((resolve, reject) => {
      const tx = this.db!.transaction('results', 'readwrite');
      const store = tx.objectStore('results');
      const index = store.index('viewId');
      
      // Delete all entries for this viewId
      const request = index.openKeyCursor(IDBKeyRange.only(this.viewId));
      
      request.onsuccess = (event) => {
        const cursor = (event.target as IDBRequest<IDBCursor>).result;
        if (cursor) {
          store.delete(cursor.primaryKey);
          cursor.continue();
        }
      };
      
      tx.oncomplete = () => {
        this.isCleared = false; // Allow new writes
        resolve();
      };
      tx.onerror = () => reject(tx.error);
    });
  }
  
  /**
   * Close database connection and cleanup
   */
  async dispose(): Promise<void> {
    await this.clear();
    
    if (this.db) {
      this.db.close();
      this.db = null;
    }
    this.dbPromise = null;
  }
}

/**
 * Static utility to clear ALL join results from IndexedDB
 * Useful for debugging or full reset
 */
export async function clearAllJoinResults(): Promise<void> {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION);
    
    request.onsuccess = () => {
      const db = request.result;
      const tx = db.transaction('results', 'readwrite');
      const store = tx.objectStore('results');
      
      store.clear();
      
      tx.oncomplete = () => {
        db.close();
        resolve();
      };
      tx.onerror = () => {
        db.close();
        reject(tx.error);
      };
    };
    
    request.onerror = () => reject(request.error);
  });
}

/**
 * Delete the entire database (nuclear option)
 */
export async function deleteJoinDatabase(): Promise<void> {
  return new Promise((resolve, reject) => {
    const request = indexedDB.deleteDatabase(DB_NAME);
    request.onsuccess = () => resolve();
    request.onerror = () => reject(request.error);
  });
}


