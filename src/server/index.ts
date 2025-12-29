/**
 * DBSP Server Module
 * ==================
 * 
 * Server-side DBSP processing with streaming to clients.
 * 
 * ## Server Usage (Node.js)
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
 * 
 * ## Client Usage (React)
 * 
 * ```tsx
 * import { DBSPServerProvider, useDBSPServerView } from 'dbsp/server';
 * 
 * function App() {
 *   return (
 *     <DBSPServerProvider url="ws://localhost:8767">
 *       <Dashboard />
 *     </DBSPServerProvider>
 *   );
 * }
 * 
 * function Dashboard() {
 *   // SQL runs on server, only results stream to client
 *   const sectorPnL = useDBSPServerView(
 *     ['positions'],
 *     `SELECT sector, SUM(notional) as total FROM positions GROUP BY sector`
 *   );
 * 
 *   return <div>{sectorPnL.results.map(r => ...)}</div>;
 * }
 * ```
 */

// Server-side (Node.js)
export { DBSPServer } from './DBSPServer';

// Client-side (React)
export { DBSPServerProvider, useDBSPServerConnection } from './useDBSPServerConnection';
export { useDBSPServerView, type DBSPServerViewHandle, type DBSPServerViewOptions } from './useDBSPServerView';

// Protocol types
export type {
  ClientMessage,
  ServerMessage,
  DBSPServerConfig,
  SourceConfig,
  ViewSnapshotMessage,
  ViewDeltaMessage,
} from './protocol';

