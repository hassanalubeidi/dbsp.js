/**
 * useDBSPServerConnection - Shared WebSocket Connection to DBSP Server
 * =====================================================================
 * 
 * Manages a single WebSocket connection to the DBSP server, shared across
 * all useDBSPServerView hooks in the application.
 * 
 * ## Usage
 * 
 * ```tsx
 * // In your app root, wrap with provider
 * import { DBSPServerProvider } from 'dbsp/server';
 * 
 * function App() {
 *   return (
 *     <DBSPServerProvider url="ws://localhost:8767">
 *       <MyComponent />
 *     </DBSPServerProvider>
 *   );
 * }
 * ```
 */

import { createContext, useContext, useEffect, useRef, useState, useCallback, type ReactNode } from 'react';
import type { ClientMessage, ServerMessage, ViewSnapshotMessage, ViewDeltaMessage } from './protocol';

// ═══════════════════════════════════════════════════════════════════════════════
// TYPES
// ═══════════════════════════════════════════════════════════════════════════════

export interface DBSPServerConnectionState {
  /** Whether we're connected to the DBSP server */
  connected: boolean;
  /** Available source names on the server */
  availableSources: string[];
  /** Subscribe to a view - returns unsubscribe function */
  subscribeView: (
    viewId: string,
    query: string,
    sources: string[],
    options: {
      onSnapshot: (msg: ViewSnapshotMessage) => void;
      onDelta: (msg: ViewDeltaMessage) => void;
      onError: (error: string) => void;
      joinMode?: 'append-only' | 'full' | 'full-indexed';
      maxRows?: number;
      maxResults?: number;
    }
  ) => () => void;
  /** Send a message to the server */
  send: (msg: ClientMessage) => void;
}

// ═══════════════════════════════════════════════════════════════════════════════
// CONTEXT
// ═══════════════════════════════════════════════════════════════════════════════

const DBSPServerContext = createContext<DBSPServerConnectionState | null>(null);

export function useDBSPServerConnection(): DBSPServerConnectionState {
  const ctx = useContext(DBSPServerContext);
  if (!ctx) {
    throw new Error('useDBSPServerConnection must be used within DBSPServerProvider');
  }
  return ctx;
}

// ═══════════════════════════════════════════════════════════════════════════════
// PROVIDER
// ═══════════════════════════════════════════════════════════════════════════════

interface DBSPServerProviderProps {
  /** WebSocket URL of the DBSP server */
  url: string;
  /** Reconnect delay in ms */
  reconnectDelay?: number;
  /** Children */
  children: ReactNode;
}

export function DBSPServerProvider({ url, reconnectDelay = 3000, children }: DBSPServerProviderProps) {
  const wsRef = useRef<WebSocket | null>(null);
  const [connected, setConnected] = useState(false);
  const [availableSources, setAvailableSources] = useState<string[]>([]);
  
  // Message queue for messages sent before connection is ready
  const pendingMessagesRef = useRef<ClientMessage[]>([]);
  
  // View subscriptions: viewId -> callbacks
  const viewCallbacksRef = useRef<Map<string, {
    onSnapshot: (msg: ViewSnapshotMessage) => void;
    onDelta: (msg: ViewDeltaMessage) => void;
    onError: (error: string) => void;
  }>>(new Map());

  // Connect/reconnect logic
  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return;

    console.log(`[DBSPServerProvider] Connecting to ${url}...`);
    const ws = new WebSocket(url);
    wsRef.current = ws;

    ws.onopen = () => {
      console.log('[DBSPServerProvider] Connected to DBSP server');
      setConnected(true);
      
      // Send any pending messages
      for (const msg of pendingMessagesRef.current) {
        console.log('[DBSPServerProvider] Sending queued message:', msg.type);
        ws.send(JSON.stringify(msg));
      }
      pendingMessagesRef.current = [];
    };

    ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data) as ServerMessage;
        
        if (msg.type === 'server-info') {
          setAvailableSources(msg.sources);
          console.log('[DBSPServerProvider] Server info:', msg);
        } else if (msg.type === 'view-subscribed') {
          console.log(`[DBSPServerProvider] Subscribed to view ${msg.viewId}`);
        } else if (msg.type === 'view-snapshot') {
          const callbacks = viewCallbacksRef.current.get(msg.viewId);
          if (callbacks) {
            callbacks.onSnapshot(msg);
          }
        } else if (msg.type === 'view-delta') {
          const callbacks = viewCallbacksRef.current.get(msg.viewId);
          if (callbacks) {
            callbacks.onDelta(msg);
          }
        } else if (msg.type === 'error') {
          console.error('[DBSPServerProvider] Server error:', msg);
          if (msg.viewId) {
            const callbacks = viewCallbacksRef.current.get(msg.viewId);
            if (callbacks) {
              callbacks.onError(msg.message);
            }
          }
        } else if (msg.type === 'pong') {
          // Handle pong if needed
        }
      } catch (err) {
        console.error('[DBSPServerProvider] Failed to parse message:', err);
      }
    };

    ws.onclose = () => {
      console.log('[DBSPServerProvider] Disconnected from DBSP server');
      setConnected(false);
      wsRef.current = null;
      
      // Reconnect after delay
      setTimeout(connect, reconnectDelay);
    };

    ws.onerror = (err) => {
      console.error('[DBSPServerProvider] WebSocket error:', err);
    };
  }, [url, reconnectDelay]);

  // Initial connection
  useEffect(() => {
    connect();
    return () => {
      wsRef.current?.close();
    };
  }, [connect]);

  // Send message helper - queues if not connected
  const send = useCallback((msg: ClientMessage) => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      console.log('[DBSPServerProvider] Sending:', msg.type);
      wsRef.current.send(JSON.stringify(msg));
    } else {
      console.log('[DBSPServerProvider] Queuing message (not connected yet):', msg.type);
      pendingMessagesRef.current.push(msg);
    }
  }, []);

  // Subscribe to a view
  const subscribeView = useCallback((
    viewId: string,
    query: string,
    sources: string[],
    options: {
      onSnapshot: (msg: ViewSnapshotMessage) => void;
      onDelta: (msg: ViewDeltaMessage) => void;
      onError: (error: string) => void;
      joinMode?: 'append-only' | 'full' | 'full-indexed';
      maxRows?: number;
      maxResults?: number;
    }
  ) => {
    console.log(`[DBSPServerProvider] Subscribing to view: ${viewId}, sources: ${sources.join(', ')}`);
    
    // Store callbacks
    viewCallbacksRef.current.set(viewId, {
      onSnapshot: options.onSnapshot,
      onDelta: options.onDelta,
      onError: options.onError,
    });

    // Send subscribe message
    send({
      type: 'subscribe-view',
      viewId,
      query,
      sources,
      options: {
        joinMode: options.joinMode,
        maxRows: options.maxRows,
        maxResults: options.maxResults,
      },
    });

    // Return unsubscribe function
    return () => {
      viewCallbacksRef.current.delete(viewId);
      send({
        type: 'unsubscribe-view',
        viewId,
      });
    };
  }, [send]);

  const value: DBSPServerConnectionState = {
    connected,
    availableSources,
    subscribeView,
    send,
  };

  return (
    <DBSPServerContext.Provider value={value}>
      {children}
    </DBSPServerContext.Provider>
  );
}

