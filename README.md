# dbsp.js

> `@dbsp/core` on npm

Incremental SQL for JavaScript. Query your data with SQL, get microsecond updates.

```tsx
const orders = useDBSPSource<Order>({ name: 'orders', key: 'id' });
const pending = useDBSPView(orders, "SELECT * FROM orders WHERE status = 'pending'");
// Updates in O(Δ) instead of O(N) - even with millions of rows
```

## Install

```bash
npm install @dbsp/core
```

## Quick Start (React)

```tsx
import { useDBSPSource, useDBSPView } from '@dbsp/core/react';

function Dashboard() {
  const orders = useDBSPSource<Order>({ name: 'orders', key: 'orderId' });
  const pending = useDBSPView(orders, "SELECT * FROM orders WHERE status = 'pending'");
  
  // Load initial data
  useEffect(() => {
    fetchOrders().then(orders.push);
  }, []);
  
  // All ways to modify data:
  const handleNewOrder = (order: Order) => {
    orders.push(order);              // Insert single row
  };
  
  const handleBulkImport = (rows: Order[]) => {
    orders.push(rows);               // Insert/upsert multiple rows
  };
  
  const handleUpdateOrder = (order: Order) => {
    orders.push(order);              // Upsert - same key = update
  };
  
  const handleDeleteOrder = (orderId: string) => {
    orders.remove(orderId);          // Delete by primary key
  };
  
  const handleDeleteMultiple = (ids: string[]) => {
    orders.remove(...ids);           // Delete multiple by keys
  };
  
  const handleClearAll = () => {
    orders.clear();                  // Delete all rows
  };
  
  return (
    <div>
      <h2>{pending.count} pending orders</h2>
      <p>Total rows: {orders.totalRows}</p>
      
      <button onClick={() => handleNewOrder({ orderId: 'new', status: 'pending', amount: 100 })}>
        Add Order
      </button>
      <button onClick={() => handleDeleteOrder('new')}>Delete Order</button>
      <button onClick={handleClearAll}>Clear All</button>
    </div>
  );
}
```

## Vanilla JS / Node.js

Use DBSP without React - works in Node.js, browsers, or any framework:

```ts
import { DBSPSource, DBSPView } from '@dbsp/core/core';

// Create a source
const orders = new DBSPSource({ name: 'orders', key: 'orderId' });

// Create a view
const pending = new DBSPView({
  sources: [orders],
  query: "SELECT * FROM orders WHERE status = 'pending'"
});

// Subscribe to changes
pending.subscribe(state => {
  console.log('Pending orders:', state.results);
  console.log('Count:', state.count);
});

// === All ways to modify data ===

// Insert single row
orders.push({ orderId: 1, status: 'pending', amount: 100 });

// Insert multiple rows
orders.push([
  { orderId: 2, status: 'pending', amount: 200 },
  { orderId: 3, status: 'shipped', amount: 300 }
]);

// Update (upsert) - same key overwrites
orders.push({ orderId: 1, status: 'shipped', amount: 100 });

// Delete by key
orders.remove(2);

// Delete multiple
orders.remove(1, 3);

// Clear all
orders.clear();

// Check state
console.log('Total rows:', orders.totalRows);

// Cleanup
pending.dispose();
orders.dispose();
```

## Server-Side (Node.js)

Run DBSP on a server and stream results to clients. Useful for:
- Offloading computation from browsers
- Sharing computed views across clients
- Processing high-volume data streams

```ts
import { DBSPServer } from '@dbsp/core/server';

const server = new DBSPServer({
  port: 8767,
  dataSourceUrl: 'ws://localhost:8766',  // Your data source
  sources: [
    { name: 'orders', key: 'orderId', dataStream: 'orders' },
    { name: 'customers', key: 'customerId', dataStream: 'customers' },
  ],
});

await server.start();
// Clients can now connect and subscribe to views
```

**Client connection:**
```tsx
import { DBSPServerProvider, useDBSPServerView } from '@dbsp/core/server';

function App() {
  return (
    <DBSPServerProvider url="ws://localhost:8767">
      <Dashboard />
    </DBSPServerProvider>
  );
}

function Dashboard() {
  const pending = useDBSPServerView(
    'pending-orders',
    "SELECT * FROM orders WHERE status = 'pending'",
    ['orders']
  );
  
  return <div>{pending.count} pending</div>;
}
```

## View Chaining

Views can feed into other views for composable pipelines:

```tsx
const orders = useDBSPSource<Order>({ name: 'orders', key: 'orderId' });

// First view: filter pending
const pending = useDBSPView(orders, "SELECT * FROM orders WHERE status = 'pending'");

// Chain: aggregate pending orders
const pendingByCustomer = useDBSPView(pending,
  "SELECT customerId, COUNT(*) as count, SUM(amount) as total FROM orders GROUP BY customerId",
  { outputKey: 'customerId' }
);

// Chain: top customers by pending amount
const topCustomers = useDBSPView(pendingByCustomer,
  "SELECT * FROM orders ORDER BY total DESC LIMIT 10"
);
```

## Joins

```tsx
const orders = useDBSPSource<Order>({ name: 'orders', key: 'orderId' });
const customers = useDBSPSource<Customer>({ name: 'customers', key: 'customerId' });

// Join multiple sources
const enriched = useDBSPView(
  [orders, customers],
  `SELECT o.*, c.name, c.email 
   FROM orders o 
   JOIN customers c ON o.customerId = c.customerId`,
  { joinMode: 'full' }  // 'append-only' for insert-only streams (faster)
);
```

## Extending DBSP

### Custom Operators with ZSets

Build custom operators using the low-level Z-set primitives:

```ts
import { ZSet } from '@dbsp/core/internals';

// Create a weighted set
const zset = new ZSet<User>(user => user.id);
zset.insert({ id: '1', name: 'Alice' }, 1);   // +1 = insert
zset.insert({ id: '2', name: 'Bob' }, 1);
zset.insert({ id: '1', name: 'Alice' }, -1);  // -1 = delete

// ZSet operations
const filtered = zset.filter(u => u.name.startsWith('B'));
const mapped = zset.map(u => ({ ...u, upper: u.name.toUpperCase() }));
const combined = zset1.add(zset2);  // Union
const diff = zset1.subtract(zset2); // Difference
```

### Custom Circuits

Build custom streaming circuits:

```ts
import { Circuit } from '@dbsp/core/internals';

const circuit = new Circuit();

// Define inputs
const ordersInput = circuit.input<Order>('orders', o => o.orderId);
const customersInput = circuit.input<Customer>('customers', c => c.customerId);

// Build pipeline
const filtered = ordersInput.filter(o => o.amount > 100);
const joined = filtered.join(
  customersInput,
  o => o.customerId,
  c => c.customerId
);

// Output
joined.output(results => console.log(results.values()));

// Process data
circuit.step(new Map([
  ['orders', ordersDelta],
  ['customers', customersDelta]
]));
```

### Window Functions

Use optimized window function state:

```ts
import { PartitionedWindowState, MonotonicDeque, RunningAggregate } from '@dbsp/core/internals';

// O(1) running SUM/AVG/COUNT
const runningSum = new RunningAggregate();
runningSum.add(100);  // sum = 100
runningSum.add(50);   // sum = 150
runningSum.remove(100); // sum = 50

// O(1) amortized MIN/MAX over sliding window
const minDeque = new MonotonicDeque((a, b) => a - b);
minDeque.push(5);
minDeque.push(3);
minDeque.push(7);
console.log(minDeque.peek()); // 3
```

## SQL Support

```sql
-- Filtering & projection
SELECT id, name FROM users WHERE active = true

-- Aggregation
SELECT department, COUNT(*), AVG(salary) FROM employees GROUP BY department

-- Joins
SELECT o.*, c.name FROM orders o JOIN customers c ON o.customerId = c.id

-- Window functions
SELECT *, 
  LAG(price) OVER (PARTITION BY symbol ORDER BY ts) as prevPrice,
  AVG(price) OVER (PARTITION BY symbol ORDER BY ts ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as ma10
FROM prices

-- Ranking
SELECT *, RANK() OVER (ORDER BY score DESC) as rank FROM leaderboard

-- Subqueries
SELECT * FROM orders WHERE customerId IN (SELECT id FROM vip_customers)
SELECT * FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customerId = c.id)
```

## API Reference

### useDBSPSource (React)

```tsx
const source = useDBSPSource<T>({
  name: string,           // SQL table name
  key: string | string[], // Primary key
  maxRows?: number        // Memory limit (FIFO eviction)
});

source.push(rows)         // Insert/upsert rows
source.remove(...keys)    // Delete by key
source.clear()            // Clear all
source.totalRows          // Current count
```

### useDBSPView (React)

```tsx
const view = useDBSPView(
  source,                 // Source, View, or array for joins
  sqlQuery,               // SQL string
  { 
    outputKey?: string,   // Key for output deduplication
    maxRows?: number,     // Result limit
    joinMode?: 'append-only' | 'full' | 'full-indexed'
  }
);

view.results              // Result array
view.count                // Row count
view.ready                // Whether view is initialized
view.stats                // { lastUpdateMs, totalUpdates, avgUpdateMs }
```

### DBSPSource / DBSPView (Vanilla JS)

```ts
const source = new DBSPSource({ name, key, maxRows?, debug? });
source.push(rows);
source.subscribe(state => { /* state.totalRows, state.delta */ });
source.dispose();

const view = new DBSPView({ sources, query, joinMode?, maxRows? });
view.subscribe(state => { /* state.results, state.count */ });
view.dispose();
```

## Imports

```tsx
// React hooks
import { useDBSPSource, useDBSPView } from '@dbsp/core/react';

// Vanilla JS classes
import { DBSPSource, DBSPView } from '@dbsp/core/core';

// SQL compiler
import { SQLCompiler, SQLParser } from '@dbsp/core/sql';

// Low-level primitives
import { ZSet, Circuit, StreamHandle } from '@dbsp/core/internals';

// Server (Node.js)
import { DBSPServer, DBSPServerProvider, useDBSPServerView } from '@dbsp/core/server';

// Optimized joins
import { OptimizedJoinState, AppendOnlyJoinState } from '@dbsp/core/joins';
```

## Development

```bash
npm install     # Install dependencies
npm run build   # Compile TypeScript
npm run dev     # Watch mode
npm run test    # Run tests
npm run typecheck  # Type check without emitting
```

## How It Works

DBSP uses **Z-sets** (weighted multisets) where:
- Weight `+1` = insert
- Weight `-1` = delete  
- Weight `0` = no change

When data changes, only the delta (Δ) propagates through operators:

| Operation | Traditional | DBSP |
|-----------|-------------|------|
| Filter    | O(N)        | O(Δ) |
| Map       | O(N)        | O(Δ) |
| Join      | O(N×M)      | O(Δ) |
| GROUP BY  | O(N)        | O(Δ) |

## Documentation

📚 **[View Full Documentation](https://hassanalubeidi.github.io/dbsp.js/)** - Interactive docs with examples, deep dives, and API reference.

The docs site covers:
- How Z-Sets enable incremental computation
- Differentiation & Integration operators
- Bilinear join optimizations
- Window function implementations
- SQL compiler internals
- Memory management strategies

To run the docs site locally:

```bash
cd docs
npm install
npm run dev
```

## License

MIT
