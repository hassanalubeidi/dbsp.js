/**
 * DBSP Server Engine
 * ==================
 * 
 * Node.js server that runs DBSP transformations and streams results to clients.
 * Connects to a data source (like credit-server) and processes SQL queries.
 * 
 * ## Architecture
 * 
 * ```
 * credit-server (8766) ──▶ DBSPServer (8767) ──▶ Browser Clients
 *      Raw Data              SQL Processing        Computed Results
 * ```
 * 
 * ## Usage
 * 
 * ```ts
 * import { DBSPServer } from 'dbsp/server';
 * 
 * const server = new DBSPServer({
 *   port: 8767,
 *   dataSourceUrl: 'ws://localhost:8766',
 *   sources: [
 *     { name: 'rfqs', key: 'rfqId', dataStream: 'rfq' },
 *     { name: 'positions', key: 'positionId', dataStream: 'position' },
 *   ],
 * });
 * 
 * await server.start();
 * ```
 */

import { WebSocket, WebSocketServer } from 'ws';
import { DBSPSource } from '../core/DBSPSource';
import { DBSPView } from '../core/DBSPView';
import type { DBSPStreamHandle } from '../core/types';
import type {
  DBSPServerConfig,
  ClientMessage,
  ServerMessage,
  SourceConfig,
  ViewSnapshotMessage,
  ViewDeltaMessage,
} from './protocol';

// ═══════════════════════════════════════════════════════════════════════════════
// TYPES
// ═══════════════════════════════════════════════════════════════════════════════

interface ClientConnection {
  id: string;
  ws: WebSocket;
  subscriptions: Map<string, ViewSubscription>;
}

interface ViewSubscription {
  viewId: string;
  view: DBSPView<Record<string, unknown>, Record<string, unknown>>;
  unsubscribe: () => void;
}

// ═══════════════════════════════════════════════════════════════════════════════
// DBSP SERVER
// ═══════════════════════════════════════════════════════════════════════════════

export class DBSPServer {
  private config: DBSPServerConfig;
  private wss: WebSocketServer | null = null;
  private dataSourceWs: WebSocket | null = null;
  private sources = new Map<string, DBSPSource<Record<string, unknown>>>();
  private clients = new Map<string, ClientConnection>();
  private clientIdCounter = 0;
  private isConnectedToDataSource = false;

  constructor(config: DBSPServerConfig) {
    this.config = config;
  }

  // ============ PUBLIC API ============

  async start(): Promise<void> {
    console.log(`\n[DBSPServer] Starting server on port ${this.config.port}...`);

    // 1. Create sources
    this.initializeSources();

    // 2. Connect to data source
    await this.connectToDataSource();

    // 3. Start WebSocket server for clients
    this.startWebSocketServer();

    console.log(`[DBSPServer] Server ready. Listening on ws://localhost:${this.config.port}`);
  }

  stop(): void {
    console.log('[DBSPServer] Stopping server...');

    // Close all client connections
    for (const client of this.clients.values()) {
      client.ws.close();
    }
    this.clients.clear();

    // Close data source connection
    if (this.dataSourceWs) {
      this.dataSourceWs.close();
      this.dataSourceWs = null;
    }

    // Close WebSocket server
    if (this.wss) {
      this.wss.close();
      this.wss = null;
    }

    // Dispose sources
    for (const source of this.sources.values()) {
      source.dispose();
    }
    this.sources.clear();

    console.log('[DBSPServer] Server stopped.');
  }

  // ============ INITIALIZATION ============

  private initializeSources(): void {
    console.log(`[DBSPServer] Initializing ${this.config.sources.length} sources...`);

    for (const sourceConfig of this.config.sources) {
      const source = new DBSPSource<Record<string, unknown>>({
        name: sourceConfig.name,
        key: sourceConfig.key as string | string[],
        maxRows: sourceConfig.maxRows,
        debug: this.config.debug,
      });

      this.sources.set(sourceConfig.name, source);
      console.log(`  ✓ Source "${sourceConfig.name}" (key: ${JSON.stringify(sourceConfig.key)})`);
    }
  }

  private connectToDataSource(): Promise<void> {
    return new Promise((resolve, reject) => {
      console.log(`[DBSPServer] Connecting to data source: ${this.config.dataSourceUrl}`);

      this.dataSourceWs = new WebSocket(this.config.dataSourceUrl);

      this.dataSourceWs.on('open', () => {
        console.log('[DBSPServer] Connected to data source.');
        this.isConnectedToDataSource = true;
        resolve();
      });

      this.dataSourceWs.on('message', (data: Buffer | ArrayBuffer | Buffer[]) => {
        this.handleDataSourceMessage(data.toString());
      });

      this.dataSourceWs.on('close', () => {
        console.log('[DBSPServer] Disconnected from data source.');
        this.isConnectedToDataSource = false;
        // TODO: Implement reconnection logic
      });

      this.dataSourceWs.on('error', (err: Error) => {
        console.error('[DBSPServer] Data source error:', err.message);
        reject(err);
      });

      // Timeout after 10 seconds
      setTimeout(() => {
        if (!this.isConnectedToDataSource) {
          reject(new Error('Timeout connecting to data source'));
        }
      }, 10000);
    });
  }

  private startWebSocketServer(): void {
    this.wss = new WebSocketServer({ port: this.config.port });

    this.wss.on('connection', (ws: WebSocket) => {
      const clientId = `client_${++this.clientIdCounter}`;
      console.log(`[DBSPServer] Client connected: ${clientId}`);

      const client: ClientConnection = {
        id: clientId,
        ws,
        subscriptions: new Map(),
      };

      this.clients.set(clientId, client);

      // Send server info
      this.send(ws, {
        type: 'server-info',
        sources: Array.from(this.sources.keys()),
        version: '1.0.0',
        clientCount: this.clients.size,
      });

      ws.on('message', (data: Buffer | ArrayBuffer | Buffer[]) => {
        this.handleClientMessage(client, data.toString());
      });

      ws.on('close', () => {
        console.log(`[DBSPServer] Client disconnected: ${clientId}`);
        this.handleClientDisconnect(client);
      });

      ws.on('error', (err: Error) => {
        console.error(`[DBSPServer] Client error (${clientId}):`, err.message);
      });
    });
  }

  // ============ DATA SOURCE HANDLING ============

  private handleDataSourceMessage(rawData: string): void {
    try {
      const msg = JSON.parse(rawData);

      // Map data source message types to our sources
      switch (msg.type) {
        case 'rfq-snapshot':
          this.pushToSource('rfqs', msg.data);
          break;

        case 'position-snapshot':
          this.pushToSource('positions', msg.data);
          break;

        case 'signal-snapshot':
          this.pushToSource('signals', msg.data);
          break;

        case 'price-snapshot':
          this.pushToSource('prices', msg.data);
          break;

        case 'watchlist-snapshot':
          this.pushToSource('watchlist', msg.data);
          break;

        case 'alert-snapshot':
          this.pushToSource('alerts', msg.data);
          break;

        case 'leaderboard-snapshot':
          this.pushToSource('leaderboard', msg.data);
          break;

        case 'fx-snapshot':
          this.pushToSource('fx', msg.data);
          break;

        case 'benchmark-snapshot':
          this.pushToSource('benchmarks', msg.data);
          break;

        case 'delta':
          // Handle delta updates
          const deltas = msg.data;
          if (deltas.rfqs?.length) this.pushToSource('rfqs', deltas.rfqs);
          if (deltas.positions?.length) {
            // Handle position updates (which have op + row structure)
            const positionRows = deltas.positions.map((p: { op: string; row: Record<string, unknown> } | Record<string, unknown>) => 
              'row' in p ? p.row : p
            );
            this.pushToSource('positions', positionRows);
          }
          if (deltas.signals?.length) this.pushToSource('signals', deltas.signals);
          if (deltas.prices?.length) this.pushToSource('prices', deltas.prices);
          if (deltas.fx?.length) this.pushToSource('fx', deltas.fx);
          if (deltas.benchmarks?.length) this.pushToSource('benchmarks', deltas.benchmarks);
          if (deltas.alerts?.length) this.pushToSource('alerts', deltas.alerts);
          if (deltas.leaderboard?.length) this.pushToSource('leaderboard', deltas.leaderboard);
          break;
      }
    } catch (err) {
      console.error('[DBSPServer] Error parsing data source message:', err);
    }
  }

  private pushToSource(name: string, data: Record<string, unknown>[]): void {
    const source = this.sources.get(name);
    if (source) {
      source.push(data);
      if (this.config.debug) {
        console.log(`[DBSPServer] Pushed ${data.length} rows to "${name}"`);
      }
    }
  }

  // ============ CLIENT MESSAGE HANDLING ============

  private handleClientMessage(client: ClientConnection, rawData: string): void {
    try {
      const msg: ClientMessage = JSON.parse(rawData);

      switch (msg.type) {
        case 'subscribe-view':
          this.handleSubscribeView(client, msg);
          break;

        case 'unsubscribe-view':
          this.handleUnsubscribeView(client, msg.viewId);
          break;

        case 'get-snapshot':
          this.handleGetSnapshot(client, msg.viewId);
          break;

        case 'ping':
          this.send(client.ws, {
            type: 'pong',
            timestamp: msg.timestamp,
            serverTime: Date.now(),
          });
          break;

        default:
          console.warn(`[DBSPServer] Unknown message type from ${client.id}`);
      }
    } catch (err) {
      console.error(`[DBSPServer] Error handling client message (${client.id}):`, err);
      this.send(client.ws, {
        type: 'error',
        code: 'INTERNAL_ERROR',
        message: 'Failed to process message',
      });
    }
  }

  private handleSubscribeView(
    client: ClientConnection,
    msg: { viewId: string; query: string; sources: string[]; options?: Record<string, unknown> }
  ): void {
    const { viewId, query, sources: sourceNames, options } = msg;

    console.log(`[DBSPServer] Client ${client.id} subscribing to view ${viewId}`);
    console.log(`  Query: ${query.substring(0, 100)}...`);
    console.log(`  Sources: ${sourceNames.join(', ')}`);

    // Validate sources exist
    const viewSources: DBSPStreamHandle<Record<string, unknown>>[] = [];
    for (const name of sourceNames) {
      const source = this.sources.get(name);
      if (!source) {
        this.send(client.ws, {
          type: 'error',
          viewId,
          code: 'SOURCE_NOT_FOUND',
          message: `Source "${name}" not found. Available: ${Array.from(this.sources.keys()).join(', ')}`,
        });
        return;
      }
      viewSources.push(source);
    }

    try {
      // Create the view
      const view = new DBSPView<Record<string, unknown>, Record<string, unknown>>({
        sources: viewSources,
        query,
        name: options?.name as string | undefined,
        joinMode: options?.joinMode as 'append-only' | 'full' | 'full-indexed' | undefined,
        maxRows: options?.maxRows as number | undefined,
        maxResults: options?.maxResults as number | undefined,
        debug: this.config.debug,
      });

      // Subscribe to view updates
      const unsubscribe = view._subscribe((delta) => {
        // Stream delta to client
        const deltaMsg: ViewDeltaMessage = {
          type: 'view-delta',
          viewId,
          delta,
          count: view.count,
          stats: {
            lastUpdateMs: view.stats.lastUpdateMs,
            totalUpdates: view.stats.totalUpdates,
            avgUpdateMs: view.stats.avgUpdateMs,
            currentRowCount: view.count,
          },
        };
        this.send(client.ws, deltaMsg);
      });

      // Store subscription
      const subscription: ViewSubscription = { viewId, view, unsubscribe };
      client.subscriptions.set(viewId, subscription);

      // Send confirmation
      this.send(client.ws, {
        type: 'view-subscribed',
        viewId,
        availableSources: Array.from(this.sources.keys()),
      });

      // Wait for view to be ready, then send initial snapshot
      this.waitForViewAndSendSnapshot(client, viewId, view);

    } catch (err) {
      console.error(`[DBSPServer] Failed to create view ${viewId}:`, err);
      this.send(client.ws, {
        type: 'error',
        viewId,
        code: 'INVALID_SQL',
        message: err instanceof Error ? err.message : 'Failed to compile SQL',
      });
    }
  }

  private async waitForViewAndSendSnapshot(
    client: ClientConnection,
    viewId: string,
    view: DBSPView<Record<string, unknown>, Record<string, unknown>>
  ): Promise<void> {
    // Poll for view to be ready
    let attempts = 0;
    while (!view.ready && attempts < 100) {
      await new Promise(resolve => setTimeout(resolve, 50));
      attempts++;
    }

    if (!view.ready) {
      console.warn(`[DBSPServer] View ${viewId} not ready after ${attempts * 50}ms`);
      return;
    }

    // Send initial snapshot
    const snapshot: ViewSnapshotMessage = {
      type: 'view-snapshot',
      viewId,
      results: view.results,
      count: view.count,
      stats: {
        lastUpdateMs: view.stats.lastUpdateMs,
        totalUpdates: view.stats.totalUpdates,
        avgUpdateMs: view.stats.avgUpdateMs,
        currentRowCount: view.count,
      },
    };

    this.send(client.ws, snapshot);
    console.log(`[DBSPServer] Sent snapshot for view ${viewId}: ${view.count} rows`);
  }

  private handleUnsubscribeView(client: ClientConnection, viewId: string): void {
    const subscription = client.subscriptions.get(viewId);
    if (subscription) {
      subscription.unsubscribe();
      subscription.view.dispose();
      client.subscriptions.delete(viewId);
      console.log(`[DBSPServer] Client ${client.id} unsubscribed from view ${viewId}`);
    }
  }

  private handleGetSnapshot(client: ClientConnection, viewId: string): void {
    const subscription = client.subscriptions.get(viewId);
    if (!subscription) {
      this.send(client.ws, {
        type: 'error',
        viewId,
        code: 'SUBSCRIPTION_FAILED',
        message: 'View not subscribed',
      });
      return;
    }

    const { view } = subscription;
    const snapshot: ViewSnapshotMessage = {
      type: 'view-snapshot',
      viewId,
      results: view.results,
      count: view.count,
      stats: {
        lastUpdateMs: view.stats.lastUpdateMs,
        totalUpdates: view.stats.totalUpdates,
        avgUpdateMs: view.stats.avgUpdateMs,
        currentRowCount: view.count,
      },
    };

    this.send(client.ws, snapshot);
  }

  private handleClientDisconnect(client: ClientConnection): void {
    // Clean up all subscriptions
    for (const subscription of client.subscriptions.values()) {
      subscription.unsubscribe();
      subscription.view.dispose();
    }
    client.subscriptions.clear();
    this.clients.delete(client.id);
  }

  // ============ HELPERS ============

  private send(ws: WebSocket, msg: ServerMessage): void {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(msg));
    }
  }
}



