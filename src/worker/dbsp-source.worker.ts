/**
 * DBSP Source Web Worker
 * 
 * Offloads heavy data processing from the main thread.
 * Handles:
 * - Data ingestion and storage
 * - Delta computation
 * - Eviction when maxRows exceeded
 * 
 * Communication Protocol:
 * - Main → Worker: { type: 'push' | 'remove' | 'clear' | 'init', ... }
 * - Worker → Main: { type: 'delta' | 'stats' | 'ready', ... }
 */

// ============ TYPES ============

interface WorkerConfig {
  name: string;
  maxRows?: number;
}

interface PushMessage {
  type: 'push';
  rows: Record<string, unknown>[];
  keys: string[];
}

interface RemoveMessage {
  type: 'remove';
  keys: string[];
}

interface ClearMessage {
  type: 'clear';
}

interface InitMessage {
  type: 'init';
  config: WorkerConfig;
}

interface GetDataMessage {
  type: 'getData';
}

type WorkerMessage = PushMessage | RemoveMessage | ClearMessage | InitMessage | GetDataMessage;

interface DeltaResponse {
  type: 'delta';
  entries: Array<[Record<string, unknown>, number]>;
  stats: {
    processingTimeMs: number;
    rowCount: number;
    evictedCount: number;
  };
}

interface StatsResponse {
  type: 'stats';
  rowCount: number;
}

interface ReadyResponse {
  type: 'ready';
}

interface DataResponse {
  type: 'data';
  rows: Array<[string, Record<string, unknown>]>;
}

type WorkerResponse = DeltaResponse | StatsResponse | ReadyResponse | DataResponse;

// ============ STATE ============

let config: WorkerConfig = { name: 'unknown' };
const dataMap = new Map<string, Record<string, unknown>>();
const insertionOrder: string[] = [];

// ============ HANDLERS ============

function handleInit(msg: InitMessage): ReadyResponse {
  config = msg.config;
  dataMap.clear();
  insertionOrder.length = 0;
  return { type: 'ready' };
}

function handlePush(msg: PushMessage): DeltaResponse {
  const start = performance.now();
  const entries: Array<[Record<string, unknown>, number]> = [];
  
  for (let i = 0; i < msg.rows.length; i++) {
    const row = msg.rows[i];
    const key = msg.keys[i];
    
    const existing = dataMap.get(key);
    
    if (existing) {
      // Emit -1 for old version
      entries.push([existing, -1]);
      // Remove old position from insertion order
      const idx = insertionOrder.indexOf(key);
      if (idx !== -1) insertionOrder.splice(idx, 1);
    }
    
    // Add new version
    entries.push([row, 1]);
    dataMap.set(key, row);
    insertionOrder.push(key);
  }
  
  // SILENT EVICTION: Remove oldest rows without emitting deltas
  // This preserves aggregate correctness
  let evictedCount = 0;
  if (config.maxRows && dataMap.size > config.maxRows) {
    const toEvict = dataMap.size - config.maxRows;
    for (let i = 0; i < toEvict && insertionOrder.length > 0; i++) {
      const oldestKey = insertionOrder.shift()!;
      dataMap.delete(oldestKey);
      evictedCount++;
    }
  }
  
  const processingTimeMs = performance.now() - start;
  
  return {
    type: 'delta',
    entries,
    stats: {
      processingTimeMs,
      rowCount: dataMap.size,
      evictedCount,
    },
  };
}

function handleRemove(msg: RemoveMessage): DeltaResponse {
  const start = performance.now();
  const entries: Array<[Record<string, unknown>, number]> = [];
  
  for (const key of msg.keys) {
    const existing = dataMap.get(key);
    if (existing) {
      entries.push([existing, -1]);
      dataMap.delete(key);
      const idx = insertionOrder.indexOf(key);
      if (idx !== -1) insertionOrder.splice(idx, 1);
    }
  }
  
  const processingTimeMs = performance.now() - start;
  
  return {
    type: 'delta',
    entries,
    stats: {
      processingTimeMs,
      rowCount: dataMap.size,
      evictedCount: 0,
    },
  };
}

function handleClear(): DeltaResponse {
  const start = performance.now();
  const entries: Array<[Record<string, unknown>, number]> = [];
  
  for (const [, row] of dataMap) {
    entries.push([row, -1]);
  }
  
  dataMap.clear();
  insertionOrder.length = 0;
  
  const processingTimeMs = performance.now() - start;
  
  return {
    type: 'delta',
    entries,
    stats: {
      processingTimeMs,
      rowCount: 0,
      evictedCount: 0,
    },
  };
}

function handleGetData(): DataResponse {
  return {
    type: 'data',
    rows: Array.from(dataMap.entries()),
  };
}

// ============ MESSAGE HANDLER ============

self.onmessage = (event: MessageEvent<WorkerMessage>) => {
  const msg = event.data;
  let response: WorkerResponse;
  
  switch (msg.type) {
    case 'init':
      response = handleInit(msg);
      break;
    case 'push':
      response = handlePush(msg);
      break;
    case 'remove':
      response = handleRemove(msg);
      break;
    case 'clear':
      response = handleClear();
      break;
    case 'getData':
      response = handleGetData();
      break;
    default:
      console.error('[DBSP Worker] Unknown message type:', msg);
      return;
  }
  
  self.postMessage(response);
};

// Signal ready
self.postMessage({ type: 'ready' });


