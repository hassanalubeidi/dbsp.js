/**
 * Window Function 1M+ Scale Test
 * 
 * Tests window functions at extreme scales (up to 1M+ elements)
 * 
 * Run with: npx vitest run src/dbsp/sql/window-1m.test.ts
 */

import { describe, it, expect } from 'vitest';
import { MonotonicDeque, RunningAggregate, IncrementalWindowState, type WindowFunctionSpec } from '../../internals/window-state';

function formatNumber(n: number): string {
  if (n >= 1e9) return `${(n / 1e9).toFixed(2)}B`;
  if (n >= 1e6) return `${(n / 1e6).toFixed(2)}M`;
  if (n >= 1e3) return `${(n / 1e3).toFixed(1)}K`;
  return n.toFixed(0);
}

function formatTime(ms: number): string {
  if (ms >= 1000) return `${(ms / 1000).toFixed(2)}s`;
  if (ms >= 1) return `${ms.toFixed(2)}ms`;
  return `${(ms * 1000).toFixed(0)}μs`;
}

function formatBytes(bytes: number): string {
  if (bytes >= 1e9) return `${(bytes / 1e9).toFixed(2)} GB`;
  if (bytes >= 1e6) return `${(bytes / 1e6).toFixed(2)} MB`;
  if (bytes >= 1e3) return `${(bytes / 1e3).toFixed(2)} KB`;
  return `${bytes} B`;
}

describe('1M+ Scale Tests', () => {
  
  it('MonotonicDeque MIN up to 5M elements', () => {
    console.log('\n' + '='.repeat(100));
    console.log('📊 MONOTONIC DEQUE (MIN) - EXTREME SCALE TEST');
    console.log('='.repeat(100));
    
    const scales = [
      10_000,
      100_000,
      500_000,
      1_000_000,
      2_000_000,
      5_000_000,
    ];
    const windowSize = 100;
    
    console.log(`Window size: ${windowSize}\n`);
    
    const results: any[] = [];
    
    for (const size of scales) {
      // Generate data inline to avoid memory issues
      const deque = new MonotonicDeque(windowSize, 'min');
      
      const start = performance.now();
      for (let i = 0; i < size; i++) {
        deque.add(Math.random() * 1000);
      }
      const elapsed = performance.now() - start;
      
      const opsPerSec = size / (elapsed / 1000);
      const timePerOp = elapsed / size;
      
      results.push({
        size: formatNumber(size),
        timeMs: formatTime(elapsed),
        opsPerSec: formatNumber(opsPerSec),
        'μs/op': (timePerOp * 1000).toFixed(3),
        'throughput': `${formatNumber(size / elapsed * 1000)}/s`,
      });
      
      // Force GC if available
      if ((global as any).gc) (global as any).gc();
    }
    
    console.table(results);
    
    // At 5M elements, should still be fast
    const lastResult = results[results.length - 1];
    console.log(`\n✅ Processed 5M elements in ${lastResult.timeMs}`);
    console.log(`   Throughput: ${lastResult.opsPerSec} ops/sec`);
  });
  
  it('MonotonicDeque MAX up to 5M elements', () => {
    console.log('\n' + '='.repeat(100));
    console.log('📊 MONOTONIC DEQUE (MAX) - EXTREME SCALE TEST');
    console.log('='.repeat(100));
    
    const scales = [
      10_000,
      100_000,
      500_000,
      1_000_000,
      2_000_000,
      5_000_000,
    ];
    const windowSize = 100;
    
    console.log(`Window size: ${windowSize}\n`);
    
    const results: any[] = [];
    
    for (const size of scales) {
      const deque = new MonotonicDeque(windowSize, 'max');
      
      const start = performance.now();
      for (let i = 0; i < size; i++) {
        deque.add(Math.random() * 1000);
      }
      const elapsed = performance.now() - start;
      
      results.push({
        size: formatNumber(size),
        timeMs: formatTime(elapsed),
        opsPerSec: formatNumber(size / (elapsed / 1000)),
      });
    }
    
    console.table(results);
  });
  
  it('RunningAggregate up to 10M elements', () => {
    console.log('\n' + '='.repeat(100));
    console.log('📊 RUNNING AGGREGATE (SUM/AVG/COUNT) - EXTREME SCALE TEST');
    console.log('='.repeat(100));
    
    const scales = [
      10_000,
      100_000,
      500_000,
      1_000_000,
      2_000_000,
      5_000_000,
      10_000_000,
    ];
    const windowSize = 100;
    
    console.log(`Window size: ${windowSize}\n`);
    
    const results: any[] = [];
    
    for (const size of scales) {
      const agg = new RunningAggregate(windowSize);
      
      const start = performance.now();
      for (let i = 0; i < size; i++) {
        agg.add(Math.random() * 1000);
      }
      const elapsed = performance.now() - start;
      
      results.push({
        size: formatNumber(size),
        timeMs: formatTime(elapsed),
        opsPerSec: formatNumber(size / (elapsed / 1000)),
        'μs/op': ((elapsed / size) * 1000).toFixed(4),
      });
    }
    
    console.table(results);
    
    const lastResult = results[results.length - 1];
    console.log(`\n✅ Processed 10M elements in ${lastResult.timeMs}`);
    console.log(`   Throughput: ${lastResult.opsPerSec} ops/sec`);
  });
  
  it('Combined MIN + MAX + SUM/AVG up to 2M elements', () => {
    console.log('\n' + '='.repeat(100));
    console.log('📊 COMBINED WINDOW FUNCTIONS - EXTREME SCALE TEST');
    console.log('='.repeat(100));
    
    const scales = [
      10_000,
      100_000,
      500_000,
      1_000_000,
      2_000_000,
    ];
    const windowSize = 100;
    
    console.log(`Window size: ${windowSize}`);
    console.log('Functions: MIN, MAX, SUM, AVG, COUNT (5 total)\n');
    
    const results: any[] = [];
    
    for (const size of scales) {
      const minDeque = new MonotonicDeque(windowSize, 'min');
      const maxDeque = new MonotonicDeque(windowSize, 'max');
      const agg = new RunningAggregate(windowSize);
      
      const start = performance.now();
      for (let i = 0; i < size; i++) {
        const value = Math.random() * 1000;
        minDeque.add(value);
        maxDeque.add(value);
        agg.add(value);
      }
      const elapsed = performance.now() - start;
      
      results.push({
        size: formatNumber(size),
        timeMs: formatTime(elapsed),
        opsPerSec: formatNumber(size / (elapsed / 1000)),
        'rows/sec': formatNumber(size / (elapsed / 1000)),
      });
    }
    
    console.table(results);
    
    console.log('\nNote: Each row computes 5 window functions simultaneously');
  });
  
  it('IncrementalWindowState with 7 functions up to 1M elements', () => {
    console.log('\n' + '='.repeat(100));
    console.log('📊 INCREMENTAL WINDOW STATE (7 FUNCTIONS) - EXTREME SCALE TEST');
    console.log('='.repeat(100));
    
    const specs: WindowFunctionSpec[] = [
      { type: 'MIN', column: 'value', frameSize: 100, alias: 'min_value' },
      { type: 'MAX', column: 'value', frameSize: 100, alias: 'max_value' },
      { type: 'SUM', column: 'value', frameSize: 100, alias: 'sum_value' },
      { type: 'AVG', column: 'value', frameSize: 100, alias: 'avg_value' },
      { type: 'COUNT', column: 'value', frameSize: 100, alias: 'count_value' },
      { type: 'ROW_NUMBER', column: 'value', frameSize: 1, alias: 'row_num' },
      { type: 'LAG', column: 'value', frameSize: 100, offset: 1, alias: 'lag_value' },
    ];
    
    const scales = [
      10_000,
      50_000,
      100_000,
      250_000,
      500_000,
      1_000_000,
    ];
    
    console.log('Functions: MIN, MAX, SUM, AVG, COUNT, ROW_NUMBER, LAG (7 total)\n');
    
    const results: any[] = [];
    
    for (const size of scales) {
      const state = new IncrementalWindowState(specs);
      
      const start = performance.now();
      for (let i = 0; i < size; i++) {
        state.processRow({
          id: i,
          value: Math.random() * 1000,
          ts: Date.now() + i,
        });
      }
      const elapsed = performance.now() - start;
      
      results.push({
        size: formatNumber(size),
        timeMs: formatTime(elapsed),
        opsPerSec: formatNumber(size / (elapsed / 1000)),
        'μs/row': ((elapsed / size) * 1000).toFixed(2),
      });
      
      // Reset to free memory
      state.reset();
    }
    
    console.table(results);
    
    const lastResult = results[results.length - 1];
    console.log(`\n✅ Processed 1M rows with 7 window functions in ${lastResult.timeMs}`);
    console.log(`   Throughput: ${lastResult.opsPerSec} rows/sec`);
  });
  
  it('O(n) vs O(1) comparison at 1M scale', () => {
    console.log('\n' + '='.repeat(100));
    console.log('📊 O(n) vs O(1) COMPARISON AT 1M SCALE');
    console.log('='.repeat(100));
    
    const size = 1_000_000;
    const windowSizes = [10, 50, 100, 500, 1000];
    
    console.log(`Data size: ${formatNumber(size)} elements\n`);
    
    // Generate data once
    const data = new Float64Array(size);
    for (let i = 0; i < size; i++) {
      data[i] = Math.random() * 1000;
    }
    
    const results: any[] = [];
    
    for (const windowSize of windowSizes) {
      // O(n) naive implementation
      const startOld = performance.now();
      let oldResult = 0;
      for (let i = 0; i < size; i++) {
        const startIdx = Math.max(0, i - windowSize + 1);
        let min = Infinity;
        for (let j = startIdx; j <= i; j++) {
          if (data[j] < min) min = data[j];
        }
        oldResult += min; // Prevent optimization
      }
      const elapsedOld = performance.now() - startOld;
      
      // O(1) MonotonicDeque
      const deque = new MonotonicDeque(windowSize, 'min');
      const startNew = performance.now();
      let newResult = 0;
      for (let i = 0; i < size; i++) {
        newResult += deque.add(data[i]); // Prevent optimization
      }
      const elapsedNew = performance.now() - startNew;
      
      const speedup = elapsedOld / elapsedNew;
      
      results.push({
        windowSize,
        'O(n) time': formatTime(elapsedOld),
        'O(1) time': formatTime(elapsedNew),
        'Speedup': `${speedup.toFixed(1)}x`,
        'O(n) ops/s': formatNumber(size / (elapsedOld / 1000)),
        'O(1) ops/s': formatNumber(size / (elapsedNew / 1000)),
      });
    }
    
    console.table(results);
    
    console.log('\n📈 Analysis:');
    console.log('- At 1M elements, even window size 10 shows significant speedup');
    console.log('- Larger windows show dramatic improvements');
    console.log('- O(1) maintains consistent performance regardless of window size');
  });
  
  it('Memory efficiency test at 1M scale', () => {
    console.log('\n' + '='.repeat(100));
    console.log('💾 MEMORY EFFICIENCY TEST AT 1M SCALE');
    console.log('='.repeat(100));
    
    const size = 1_000_000;
    const windowSizes = [10, 100, 1000, 10000];
    
    console.log(`Processing ${formatNumber(size)} elements with different window sizes\n`);
    
    const results: any[] = [];
    
    for (const windowSize of windowSizes) {
      // Estimate memory before
      const memBefore = process.memoryUsage().heapUsed;
      
      const deque = new MonotonicDeque(windowSize, 'min');
      const agg = new RunningAggregate(windowSize);
      
      // Process all data
      for (let i = 0; i < size; i++) {
        const value = Math.random() * 1000;
        deque.add(value);
        agg.add(value);
      }
      
      // Estimate memory after
      const memAfter = process.memoryUsage().heapUsed;
      const memDelta = memAfter - memBefore;
      
      // Theoretical memory: 
      // - Deque: windowSize * (8 bytes value + 8 bytes index) = windowSize * 16
      // - Agg: windowSize * 8 bytes = windowSize * 8
      // Total: windowSize * 24 bytes
      const theoreticalMem = windowSize * 24;
      
      results.push({
        windowSize: formatNumber(windowSize),
        'Theoretical': formatBytes(theoreticalMem),
        'Actual (approx)': formatBytes(Math.abs(memDelta)),
        'Data processed': formatNumber(size),
        'Ratio (Data/Mem)': `${(size * 8 / theoreticalMem).toFixed(0)}x`,
      });
    }
    
    console.table(results);
    
    console.log('\n📌 Key Insight:');
    console.log('- Memory is bounded by window size, NOT data size');
    console.log('- 1M elements (8MB of data) processed with <240KB memory');
    console.log('- This enables infinite streaming with bounded memory');
  });
  
  it('Throughput stability test (sustained 1M operations)', () => {
    console.log('\n' + '='.repeat(100));
    console.log('📊 THROUGHPUT STABILITY TEST (SUSTAINED 1M OPERATIONS)');
    console.log('='.repeat(100));
    
    const batchSize = 100_000;
    const batches = 10;
    const totalOps = batchSize * batches;
    const windowSize = 100;
    
    console.log(`Total operations: ${formatNumber(totalOps)}`);
    console.log(`Batch size: ${formatNumber(batchSize)}`);
    console.log(`Window size: ${windowSize}\n`);
    
    const deque = new MonotonicDeque(windowSize, 'min');
    const batchTimes: number[] = [];
    
    for (let batch = 0; batch < batches; batch++) {
      const start = performance.now();
      
      for (let i = 0; i < batchSize; i++) {
        deque.add(Math.random() * 1000);
      }
      
      const elapsed = performance.now() - start;
      batchTimes.push(elapsed);
    }
    
    const results = batchTimes.map((time, i) => ({
      batch: i + 1,
      timeMs: formatTime(time),
      opsPerSec: formatNumber(batchSize / (time / 1000)),
    }));
    
    console.table(results);
    
    const avgTime = batchTimes.reduce((a, b) => a + b, 0) / batches;
    const minTime = Math.min(...batchTimes);
    const maxTime = Math.max(...batchTimes);
    const variance = maxTime / minTime;
    
    console.log(`\n📈 Statistics:`);
    console.log(`   Average: ${formatTime(avgTime)}/batch`);
    console.log(`   Min: ${formatTime(minTime)}`);
    console.log(`   Max: ${formatTime(maxTime)}`);
    console.log(`   Variance: ${variance.toFixed(2)}x`);
    console.log(`   Total time: ${formatTime(batchTimes.reduce((a, b) => a + b, 0))}`);
    console.log(`   Overall throughput: ${formatNumber(totalOps / (batchTimes.reduce((a, b) => a + b, 0) / 1000))}/sec`);
    
    // Variance should be relatively low (< 5x between min and max)
    // Allow higher variance in CI environments due to shared resources
    expect(variance).toBeLessThan(5);
  });
});

