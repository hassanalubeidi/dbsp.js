/**
 * DBSP View Web Worker
 * 
 * Offloads SQL circuit execution to a separate thread.
 * This keeps the main thread free for UI updates while complex
 * incremental computations happen in the background.
 * 
 * Communication Protocol:
 * - Main → Worker: init, delta, getData, destroy
 * - Worker → Main: ready, results, error
 */

import { Circuit, StreamHandle } from '../internals/circuit';
import { SQLCompiler } from '../sql/sql-compiler';
import { ZSet } from '../internals/zset';

// ============ MESSAGE TYPES ============

interface SourceSchema {
  name: string;
  schema: string;
}

interface InitMessage {
  type: 'init';
  query: string;
  sources: SourceSchema[];
  outputKey?: string | string[];
}

interface DeltaMessage {
  type: 'delta';
  sourceName: string;
  entries: Array<[Record<string, unknown>, number]>;
}

interface GetDataMessage {
  type: 'getData';
}

interface DestroyMessage {
  type: 'destroy';
}

type WorkerMessage = InitMessage | DeltaMessage | GetDataMessage | DestroyMessage;

interface ReadyResponse {
  type: 'ready';
}

interface ResultsResponse {
  type: 'results';
  results: Record<string, unknown>[];
  count: number;
  stats: {
    processingTimeMs: number;
    deltaRows: number;
    outputRows: number;
  };
}

interface ErrorResponse {
  type: 'error';
  message: string;
}

type WorkerResponse = ReadyResponse | ResultsResponse | ErrorResponse;

// ============ STATE ============

let circuit: Circuit | null = null;
let inputStreams: Record<string, StreamHandle<Record<string, unknown>>> = {};
let integratedData: Map<string, Record<string, unknown>> = new Map();
let outputKeyFn: ((row: Record<string, unknown>) => string) | null = null;
let isInitialized = false;

// ============ OUTPUT KEY FUNCTION ============

function createOutputKeyFn(outputKey?: string | string[]): (row: Record<string, unknown>) => string {
  if (!outputKey) {
    return (row) => JSON.stringify(row);
  }
  if (typeof outputKey === 'string') {
    return (row) => String(row[outputKey]);
  }
  // Array of keys
  return (row) => outputKey.map(k => String(row[k])).join('::');
}

// ============ HANDLERS ============

function handleInit(msg: InitMessage): WorkerResponse {
  try {
    const start = performance.now();
    
    // Build SQL with CREATE TABLE statements for each source
    let fullSql = '';
    for (const source of msg.sources) {
      fullSql += `CREATE TABLE ${source.name} (${source.schema});\n`;
    }
    
    // Add the actual query as a view
    fullSql += `CREATE VIEW result AS ${msg.query};`;
    
    // Compile SQL to circuit
    const compiler = new SQLCompiler();
    const compiled = compiler.compile(fullSql);
    
    circuit = compiled.circuit;
    inputStreams = compiled.tables;
    
    // Set up output key function
    outputKeyFn = createOutputKeyFn(msg.outputKey);
    
    // Subscribe to output
    const outputView = compiled.views['result'];
    if (!outputView) {
      return { type: 'error', message: 'Failed to compile query - no output view created' };
    }
    
    // Integrate output to get full state and subscribe to updates
    // output() method is on StreamHandle, takes a callback
    const integratedOutput = outputView.integrate();
    integratedOutput.output((delta: ZSet<Record<string, unknown>>) => {
      // Apply delta to integrated data
      for (const [row, weight] of delta.entries()) {
        const key = outputKeyFn!(row);
        if (weight > 0) {
          integratedData.set(key, row);
        } else {
          integratedData.delete(key);
        }
      }
    });
    
    integratedData.clear();
    isInitialized = true;
    
    const elapsed = performance.now() - start;
    console.log(`[DBSP View Worker] Initialized in ${elapsed.toFixed(2)}ms`);
    
    return { type: 'ready' };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return { type: 'error', message: `Init failed: ${message}` };
  }
}

function handleDelta(msg: DeltaMessage): WorkerResponse {
  if (!isInitialized || !circuit) {
    return { type: 'error', message: 'Worker not initialized' };
  }
  
  try {
    const start = performance.now();
    
    if (!inputStreams[msg.sourceName]) {
      return { type: 'error', message: `Unknown source: ${msg.sourceName}` };
    }
    
    // Create ZSet from delta entries
    const zset = ZSet.fromEntries(msg.entries);
    
    // Create delta map with zero ZSets for all inputs
    const deltas = new Map<string, ZSet<Record<string, unknown>>>();
    for (const name of Object.keys(inputStreams)) {
      deltas.set(name, ZSet.zero());
    }
    // Set the actual delta for this source
    deltas.set(msg.sourceName, zset);
    
    // Step the circuit with the deltas
    circuit.step(deltas as Map<string, unknown>);
    
    const elapsed = performance.now() - start;
    
    // Get current results
    const results = Array.from(integratedData.values());
    
    return {
      type: 'results',
      results,
      count: results.length,
      stats: {
        processingTimeMs: elapsed,
        deltaRows: msg.entries.length,
        outputRows: results.length,
      },
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return { type: 'error', message: `Delta processing failed: ${message}` };
  }
}

function handleGetData(): WorkerResponse {
  if (!isInitialized) {
    return { type: 'error', message: 'Worker not initialized' };
  }
  
  const results = Array.from(integratedData.values());
  return {
    type: 'results',
    results,
    count: results.length,
    stats: {
      processingTimeMs: 0,
      deltaRows: 0,
      outputRows: results.length,
    },
  };
}

function handleDestroy(): void {
  circuit = null;
  inputStreams = {};
  integratedData.clear();
  isInitialized = false;
}

// ============ MESSAGE HANDLER ============

self.onmessage = (event: MessageEvent<WorkerMessage>) => {
  const msg = event.data;
  let response: WorkerResponse | null = null;
  
  switch (msg.type) {
    case 'init':
      response = handleInit(msg);
      break;
    case 'delta':
      response = handleDelta(msg);
      break;
    case 'getData':
      response = handleGetData();
      break;
    case 'destroy':
      handleDestroy();
      break;
    default:
      response = { type: 'error', message: `Unknown message type: ${(msg as any).type}` };
  }
  
  if (response) {
    self.postMessage(response);
  }
};

// Signal ready
self.postMessage({ type: 'ready' });

