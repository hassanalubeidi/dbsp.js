/**
 * Benchmark Data Generation Module
 * 
 * Provides types and utilities for DBSP performance benchmarks.
 * Generates realistic order/customer datasets for testing.
 */

import { ZSet } from '../internals/zset';

// ============ TYPES ============

export interface Order {
  id: number;
  orderId: number;  // Alias for id
  customerId: number;
  productId: number;
  quantity: number;
  price: number;
  amount: number;  // Alias for price
  region: string;
  status: 'pending' | 'shipped' | 'delivered';
  timestamp: number;
}

export interface Customer {
  customerId: number;
  name: string;
  region: string;
  tier: 'bronze' | 'silver' | 'gold' | 'platinum';
}

export interface BenchmarkDataset {
  orders: Order[];
  customers: Customer[];
}

export interface DeltaBatch<T = Order> {
  inserts: T[];
  updates: T[];
  deletes: T[];
  totalChanges: number;
}

export interface JoinStats {
  leftSize: number;
  rightSize: number;
  resultSize: number;
  timeMs: number;
}

// ============ DATA GENERATION ============

const REGIONS = ['NA', 'EU', 'APAC', 'LATAM'];
const TIERS: Customer['tier'][] = ['bronze', 'silver', 'gold', 'platinum'];
const STATUSES: Order['status'][] = ['pending', 'shipped', 'delivered'];

/**
 * Generate a random integer between min and max (inclusive)
 */
function randomInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

/**
 * Generate a benchmark dataset with orders and customers
 */
export function generateDataset(
  orderCount: number,
  customerCount: number,
  productCount: number
): BenchmarkDataset {
  // Generate customers
  const customers: Customer[] = [];
  for (let i = 0; i < customerCount; i++) {
    customers.push({
      customerId: i,
      name: `Customer_${i}`,
      region: REGIONS[i % REGIONS.length],
      tier: TIERS[i % TIERS.length],
    });
  }

  // Generate orders
  const orders: Order[] = [];
  for (let i = 0; i < orderCount; i++) {
    const customerId = randomInt(0, customerCount - 1);
    const price = randomInt(10, 1000);
    orders.push({
      id: i,
      orderId: i,
      customerId,
      productId: randomInt(0, productCount - 1),
      quantity: randomInt(1, 100),
      price,
      amount: price,
      region: customers[customerId].region,
      status: STATUSES[randomInt(0, STATUSES.length - 1)],
      timestamp: Date.now() - randomInt(0, 30 * 24 * 60 * 60 * 1000), // Last 30 days
    });
  }

  return { orders, customers };
}

/**
 * Generate a delta (batch of changes) for orders
 * @param existingOrders - Current orders
 * @param deltaPercent - Percentage of existing orders to change (0-100)
 * @param startOrderId - Starting order ID for new inserts
 */
export function generateOrderDelta(
  existingOrders: Order[],
  deltaPercent: number,
  startOrderId: number
): DeltaBatch {
  const totalDelta = Math.floor(existingOrders.length * (deltaPercent / 100));
  const insertCount = Math.max(1, Math.floor(totalDelta * 0.4));
  const updateCount = Math.max(1, Math.floor(totalDelta * 0.4));
  const deleteCount = Math.max(0, totalDelta - insertCount - updateCount);

  const maxOrderId = startOrderId;

  // Generate inserts
  const inserts: Order[] = [];
  for (let i = 0; i < insertCount; i++) {
    const customerId = randomInt(0, 9999);  // Assume up to 10k customers
    const price = randomInt(10, 1000);
    const id = maxOrderId + i;
    inserts.push({
      id,
      orderId: id,
      customerId,
      productId: randomInt(0, 4999),  // Assume up to 5k products
      quantity: randomInt(1, 100),
      price,
      amount: price,
      region: REGIONS[customerId % REGIONS.length],
      status: STATUSES[randomInt(0, STATUSES.length - 1)],
      timestamp: Date.now(),
    });
  }

  // Generate updates (modify existing orders)
  const updates: Order[] = [];
  const availableForUpdate = [...existingOrders];
  for (let i = 0; i < Math.min(updateCount, availableForUpdate.length); i++) {
    const idx = randomInt(0, availableForUpdate.length - 1);
    const original = availableForUpdate.splice(idx, 1)[0];
    const newPrice = randomInt(10, 1000);
    updates.push({
      ...original,
      price: newPrice,
      amount: newPrice,
      quantity: randomInt(1, 100),
      status: STATUSES[randomInt(0, STATUSES.length - 1)],
    });
  }

  // Generate deletes (pick existing orders to delete)
  const deletes: Order[] = [];
  for (let i = 0; i < Math.min(deleteCount, availableForUpdate.length); i++) {
    const idx = randomInt(0, availableForUpdate.length - 1);
    deletes.push(availableForUpdate.splice(idx, 1)[0]);
  }

  const totalChanges = inserts.length + updates.length + deletes.length;
  return { inserts, updates, deletes, totalChanges };
}

// ============ ZSET CONVERSION ============

const orderKey = (o: Order): string => `order_${o.orderId}`;
const customerKey = (c: Customer): string => `customer_${c.customerId}`;

/**
 * Convert orders array to ZSet
 */
export function ordersToZSet(orders: Order[]): ZSet<Order> {
  return ZSet.fromValues(orders, orderKey);
}

/**
 * Convert customers array to ZSet
 */
export function customersToZSet(customers: Customer[]): ZSet<Customer> {
  return ZSet.fromValues(customers, customerKey);
}

/**
 * Convert delta batch to ZSet (inserts + updates with weight +1, deletes with weight -1)
 */
export function deltaToZSet(delta: DeltaBatch): ZSet<Order> {
  const entries: [Order, number][] = [];
  
  // Inserts get +1
  for (const order of delta.inserts) {
    entries.push([order, 1]);
  }
  
  // Updates: old value -1, new value +1
  // Since we only have the new value, we add +1 for updates
  for (const order of delta.updates) {
    entries.push([order, 1]);
  }
  
  // Deletes get -1
  for (const order of delta.deletes) {
    entries.push([order, -1]);
  }
  
  return ZSet.fromEntries(entries, orderKey);
}

// ============ JOIN UTILITIES ============

/**
 * Hash join implementation for benchmarking
 */
export function hashJoin<T, U, K>(
  left: T[],
  right: U[],
  leftKey: (t: T) => K,
  rightKey: (u: U) => K
): [T, U][] {
  // Build hash table on right side
  const rightIndex = new Map<string, U[]>();
  for (const r of right) {
    const key = String(rightKey(r));
    if (!rightIndex.has(key)) {
      rightIndex.set(key, []);
    }
    rightIndex.get(key)!.push(r);
  }

  // Probe with left side
  const results: [T, U][] = [];
  for (const l of left) {
    const key = String(leftKey(l));
    const matches = rightIndex.get(key);
    if (matches) {
      for (const r of matches) {
        results.push([l, r]);
      }
    }
  }

  return results;
}

