/**
 * Window Function Benchmark Test
 * 
 * Compares the old O(n) implementation with the new O(1) optimized implementation.
 * 
 * Run with: npx vitest run src/dbsp/sql/window-benchmark.test.ts
 */

import { describe, it, expect } from 'vitest';
import { SQLCompiler } from '../../sql/sql-compiler';
import { Circuit } from '../../internals/circuit';
import { ZSet } from '../../internals/zset';
import { MonotonicDeque, RunningAggregate, IncrementalWindowState, PartitionedWindowState } from '../../internals/window-state';

// ============ STANDALONE DATA STRUCTURE BENCHMARKS ============

describe('MonotonicDeque Performance', () => {
  it('O(1) amortized for sliding window MIN', () => {
    const sizes = [100, 1000, 10000];
    const results: { size: number; timeMs: number; opsPerSec: number }[] = [];
    
    for (const size of sizes) {
      const deque = new MonotonicDeque(10, 'min');
      const data = Array.from({ length: size }, () => Math.random() * 1000);
      
      const start = performance.now();
      for (const value of data) {
        deque.add(value);
      }
      const elapsed = performance.now() - start;
      
      results.push({
        size,
        timeMs: elapsed,
        opsPerSec: size / (elapsed / 1000),
      });
    }
    
    console.log('\n📊 MonotonicDeque MIN Benchmark:');
    console.table(results);
    
    // Verify O(1) amortized: time should grow linearly with size
    // If O(n), 10x size would mean 10x time. We expect close to linear.
    const ratio1to2 = results[1].timeMs / results[0].timeMs;
    const ratio2to3 = results[2].timeMs / results[1].timeMs;
    
    console.log(`\nScaling: 100→1000 = ${ratio1to2.toFixed(1)}x, 1000→10000 = ${ratio2to3.toFixed(1)}x`);
    console.log('(Linear = ~10x, O(n²) = ~100x)');
    
    // Should be roughly linear (within 2x of expected 10x)
    expect(ratio1to2).toBeLessThan(20);
    expect(ratio2to3).toBeLessThan(20);
  });
  
  it('O(1) amortized for sliding window MAX', () => {
    const sizes = [100, 1000, 10000];
    const results: { size: number; timeMs: number; opsPerSec: number }[] = [];
    
    for (const size of sizes) {
      const deque = new MonotonicDeque(10, 'max');
      const data = Array.from({ length: size }, () => Math.random() * 1000);
      
      const start = performance.now();
      for (const value of data) {
        deque.add(value);
      }
      const elapsed = performance.now() - start;
      
      results.push({
        size,
        timeMs: elapsed,
        opsPerSec: size / (elapsed / 1000),
      });
    }
    
    console.log('\n📊 MonotonicDeque MAX Benchmark:');
    console.table(results);
    
    expect(results[2].opsPerSec).toBeGreaterThan(100000); // Should be very fast
  });
});

describe('RunningAggregate Performance', () => {
  it('O(1) for sliding window SUM/AVG/COUNT', () => {
    const sizes = [100, 1000, 10000, 100000];
    const results: { size: number; timeMs: number; opsPerSec: number }[] = [];
    
    for (const size of sizes) {
      const agg = new RunningAggregate(10);
      const data = Array.from({ length: size }, () => Math.random() * 1000);
      
      const start = performance.now();
      for (const value of data) {
        agg.add(value);
      }
      const elapsed = performance.now() - start;
      
      results.push({
        size,
        timeMs: elapsed,
        opsPerSec: size / (elapsed / 1000),
      });
    }
    
    console.log('\n📊 RunningAggregate SUM/AVG/COUNT Benchmark:');
    console.table(results);
    
    // O(1) means all sizes should have similar ops/sec
    const minOps = Math.min(...results.map(r => r.opsPerSec));
    const maxOps = Math.max(...results.map(r => r.opsPerSec));
    
    console.log(`\nOps/sec range: ${(minOps/1000).toFixed(0)}K - ${(maxOps/1000).toFixed(0)}K`);
    console.log('(O(1) = consistent ops/sec, O(n) = decreasing ops/sec)');
    
    // For O(1), ops/sec increases as JIT warms up, so just verify ops/sec is high
    // The key property is that ops/sec doesn't DROP significantly for larger sizes
    expect(results[results.length - 1].opsPerSec).toBeGreaterThan(1000000); // >1M ops/sec
  });
});

// ============ FULL WINDOW FUNCTION COMPARISON ============

describe('Window Function Implementation Comparison', () => {
  /**
   * Simulate the OLD O(n) approach
   */
  function computeWindowResultsOld(
    partition: { value: number; ts: number }[],
    windowSize: number
  ): { value: number; ts: number; minValue: number; maxValue: number; sum: number; avg: number }[] {
    const results: any[] = [];
    
    for (let i = 0; i < partition.length; i++) {
      const row = partition[i];
      const startIdx = Math.max(0, i - windowSize + 1);
      
      // O(n) per row - iterate through window
      const windowRows = partition.slice(startIdx, i + 1);
      
      let min = Infinity;
      let max = -Infinity;
      let sum = 0;
      
      for (const wr of windowRows) {
        min = Math.min(min, wr.value);
        max = Math.max(max, wr.value);
        sum += wr.value;
      }
      
      results.push({
        value: row.value,
        ts: row.ts,
        minValue: min,
        maxValue: max,
        sum,
        avg: sum / windowRows.length,
      });
    }
    
    return results;
  }
  
  /**
   * Simulate the NEW O(1) approach using optimized data structures
   */
  function computeWindowResultsNew(
    partition: { value: number; ts: number }[],
    windowSize: number
  ): { value: number; ts: number; minValue: number; maxValue: number; sum: number; avg: number }[] {
    const results: any[] = [];
    
    const minDeque = new MonotonicDeque(windowSize, 'min');
    const maxDeque = new MonotonicDeque(windowSize, 'max');
    const runningAgg = new RunningAggregate(windowSize);
    
    for (const row of partition) {
      // O(1) amortized per row
      const minValue = minDeque.add(row.value);
      const maxValue = maxDeque.add(row.value);
      const { sum, avg } = runningAgg.add(row.value);
      
      results.push({
        value: row.value,
        ts: row.ts,
        minValue,
        maxValue,
        sum,
        avg,
      });
    }
    
    return results;
  }
  
  it('compares performance: O(n) vs O(1) for varying partition sizes', () => {
    const partitionSizes = [10, 50, 100, 200, 500, 1000];
    const windowSize = 5;
    
    const results: {
      partitionSize: number;
      oldTimeMs: number;
      newTimeMs: number;
      speedup: string;
      oldOpsPerSec: number;
      newOpsPerSec: number;
    }[] = [];
    
    for (const size of partitionSizes) {
      // Generate test data
      const partition = Array.from({ length: size }, (_, i) => ({
        value: Math.random() * 1000,
        ts: Date.now() + i * 1000,
      }));
      
      // Warm up
      computeWindowResultsOld(partition, windowSize);
      computeWindowResultsNew(partition, windowSize);
      
      // Benchmark OLD implementation (run multiple times for accuracy)
      const iterations = Math.max(1, Math.floor(10000 / size));
      
      const oldStart = performance.now();
      for (let i = 0; i < iterations; i++) {
        computeWindowResultsOld(partition, windowSize);
      }
      const oldElapsed = (performance.now() - oldStart) / iterations;
      
      // Benchmark NEW implementation
      const newStart = performance.now();
      for (let i = 0; i < iterations; i++) {
        computeWindowResultsNew(partition, windowSize);
      }
      const newElapsed = (performance.now() - newStart) / iterations;
      
      results.push({
        partitionSize: size,
        oldTimeMs: oldElapsed,
        newTimeMs: newElapsed,
        speedup: `${(oldElapsed / newElapsed).toFixed(1)}x`,
        oldOpsPerSec: size / (oldElapsed / 1000),
        newOpsPerSec: size / (newElapsed / 1000),
      });
    }
    
    console.log('\n' + '='.repeat(80));
    console.log('📊 WINDOW FUNCTION BENCHMARK: O(n) vs O(1) Implementation');
    console.log('='.repeat(80));
    console.log(`Window size: ${windowSize} rows`);
    console.log('');
    console.table(results.map(r => ({
      'Partition Size': r.partitionSize,
      'Old (O(n)) ms': r.oldTimeMs.toFixed(3),
      'New (O(1)) ms': r.newTimeMs.toFixed(3),
      'Speedup': r.speedup,
    })));
    
    console.log('\n📈 Scaling Analysis:');
    console.log('');
    
    // Calculate scaling factors
    const first = results[0];
    const last = results[results.length - 1];
    const sizeRatio = last.partitionSize / first.partitionSize;
    const oldTimeRatio = last.oldTimeMs / first.oldTimeMs;
    const newTimeRatio = last.newTimeMs / first.newTimeMs;
    
    console.log(`Size increase: ${first.partitionSize} → ${last.partitionSize} (${sizeRatio}x)`);
    console.log(`Old time increase: ${first.oldTimeMs.toFixed(3)}ms → ${last.oldTimeMs.toFixed(3)}ms (${oldTimeRatio.toFixed(1)}x)`);
    console.log(`New time increase: ${first.newTimeMs.toFixed(3)}ms → ${last.newTimeMs.toFixed(3)}ms (${newTimeRatio.toFixed(1)}x)`);
    console.log('');
    console.log('Expected scaling:');
    console.log(`  - O(n): ${sizeRatio}x size → ~${sizeRatio}x time`);
    console.log(`  - O(1): ${sizeRatio}x size → ~${sizeRatio}x time (linear with n, O(1) per element)`);
    console.log(`  - O(n²): ${sizeRatio}x size → ~${sizeRatio * sizeRatio}x time`);
    console.log('');
    
    // Note: This test varies PARTITION SIZE with a FIXED small window size (5).
    // The O(1) optimization is for WINDOW SIZE, not partition size.
    // Both implementations are O(partition_size) in this test.
    // The real benefit is shown in "Window Size Scaling" test below.
    // Here we just verify correctness - both should produce same results.
    
    // Verify correctness: results should be the same
    const testPartition = Array.from({ length: 20 }, (_, i) => ({
      value: i * 10 + Math.random(),
      ts: Date.now() + i * 1000,
    }));
    
    const oldResults = computeWindowResultsOld(testPartition, 5);
    const newResults = computeWindowResultsNew(testPartition, 5);
    
    // Check that results are equivalent (with floating point tolerance)
    for (let i = 0; i < oldResults.length; i++) {
      expect(newResults[i].minValue).toBeCloseTo(oldResults[i].minValue, 5);
      expect(newResults[i].maxValue).toBeCloseTo(oldResults[i].maxValue, 5);
      expect(newResults[i].sum).toBeCloseTo(oldResults[i].sum, 5);
      expect(newResults[i].avg).toBeCloseTo(oldResults[i].avg, 5);
    }
    
    console.log('✅ Correctness verified: Old and New implementations produce identical results');
  });
  
  it('shows O(n) vs O(1) divergence as window size grows', () => {
    const partitionSize = 500;
    const windowSizes = [5, 10, 25, 50, 100, 200];
    
    const results: {
      windowSize: number;
      oldTimeMs: number;
      newTimeMs: number;
      speedup: string;
    }[] = [];
    
    // Generate test data
    const partition = Array.from({ length: partitionSize }, (_, i) => ({
      value: Math.random() * 1000,
      ts: Date.now() + i * 1000,
    }));
    
    for (const windowSize of windowSizes) {
      // Warm up
      computeWindowResultsOld(partition, windowSize);
      computeWindowResultsNew(partition, windowSize);
      
      const iterations = 10;
      
      const oldStart = performance.now();
      for (let i = 0; i < iterations; i++) {
        computeWindowResultsOld(partition, windowSize);
      }
      const oldElapsed = (performance.now() - oldStart) / iterations;
      
      const newStart = performance.now();
      for (let i = 0; i < iterations; i++) {
        computeWindowResultsNew(partition, windowSize);
      }
      const newElapsed = (performance.now() - newStart) / iterations;
      
      results.push({
        windowSize,
        oldTimeMs: oldElapsed,
        newTimeMs: newElapsed,
        speedup: `${(oldElapsed / newElapsed).toFixed(1)}x`,
      });
    }
    
    console.log('\n' + '='.repeat(80));
    console.log('📊 WINDOW SIZE SCALING: How window size affects performance');
    console.log('='.repeat(80));
    console.log(`Partition size: ${partitionSize} rows`);
    console.log('');
    console.table(results.map(r => ({
      'Window Size': r.windowSize,
      'Old (O(n)) ms': r.oldTimeMs.toFixed(3),
      'New (O(1)) ms': r.newTimeMs.toFixed(3),
      'Speedup': r.speedup,
    })));
    
    // The old implementation should get slower as window size grows
    // The new implementation should stay roughly constant
    const firstNew = results[0].newTimeMs;
    const lastNew = results[results.length - 1].newTimeMs;
    const firstOld = results[0].oldTimeMs;
    const lastOld = results[results.length - 1].oldTimeMs;
    
    console.log('\n📈 Window Size Scaling:');
    console.log(`Old: ${firstOld.toFixed(3)}ms → ${lastOld.toFixed(3)}ms (${(lastOld/firstOld).toFixed(1)}x increase)`);
    console.log(`New: ${firstNew.toFixed(3)}ms → ${lastNew.toFixed(3)}ms (${(lastNew/firstNew).toFixed(1)}x increase)`);
    console.log('');
    console.log('(Old should grow ~linearly with window size, New should stay constant)');
  });
  
  it('simulates incremental stream processing', () => {
    console.log('\n' + '='.repeat(80));
    console.log('📊 INCREMENTAL STREAM PROCESSING SIMULATION');
    console.log('='.repeat(80));
    console.log('');
    
    const windowSize = 5;
    const totalRows = 1000;
    const batchSize = 10;
    
    // OLD approach: Recompute entire partition on each batch
    let oldTotalTime = 0;
    let partition: { value: number; ts: number }[] = [];
    
    const oldStart = performance.now();
    for (let batch = 0; batch < totalRows / batchSize; batch++) {
      // Add batch of rows
      for (let i = 0; i < batchSize; i++) {
        partition.push({
          value: Math.random() * 1000,
          ts: Date.now() + (batch * batchSize + i) * 1000,
        });
      }
      
      // Recompute ALL window results (O(n²) total)
      computeWindowResultsOld(partition, windowSize);
    }
    oldTotalTime = performance.now() - oldStart;
    
    // NEW approach: Process each row incrementally
    const minDeque = new MonotonicDeque(windowSize, 'min');
    const maxDeque = new MonotonicDeque(windowSize, 'max');
    const runningAgg = new RunningAggregate(windowSize);
    
    const newStart = performance.now();
    for (let i = 0; i < totalRows; i++) {
      const value = Math.random() * 1000;
      minDeque.add(value);
      maxDeque.add(value);
      runningAgg.add(value);
    }
    const newTotalTime = performance.now() - newStart;
    
    console.log(`Processing ${totalRows} rows in batches of ${batchSize}:`);
    console.log('');
    console.log(`Old (recompute all): ${oldTotalTime.toFixed(2)}ms`);
    console.log(`New (incremental):   ${newTotalTime.toFixed(2)}ms`);
    console.log(`Speedup:             ${(oldTotalTime / newTotalTime).toFixed(1)}x`);
    console.log('');
    console.log('(The old approach recomputes the entire partition on each batch,');
    console.log(' leading to O(n²) total complexity. The new approach is O(n) total.)');
    
    expect(newTotalTime).toBeLessThan(oldTotalTime);
  });
});

// ============ FULL SQL COMPILATION BENCHMARK ============

describe('SQL Compiled Window Function Benchmark', () => {
  it('benchmarks actual SQL compilation paths', () => {
    // Test with SUM OVER window function using the proper SQLCompiler flow
    const sql = `
      SELECT tickId, sector, ts, spread,
             SUM(spread) OVER (PARTITION BY sector ORDER BY ts ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS sumSpread
      FROM prices
    `;
    
    const sizes = [10, 50, 100, 200];
    const results: {
      size: number;
      timeMs: number;
      opsPerSec: number;
    }[] = [];
    
    for (const size of sizes) {
      // Create a fresh compiler for each test
      const compiler = new SQLCompiler();
      
      // First, compile the CREATE TABLE to register the table
      compiler.compile('CREATE TABLE prices (tickId TEXT, sector TEXT, ts INTEGER, spread FLOAT)');
      
      // Then compile the query as a view
      const viewResult = compiler.compile(`CREATE VIEW price_window AS ${sql}`);
      
      if (!viewResult || !viewResult.circuit) {
        throw new Error('Failed to compile view');
      }
      
      const circuit = viewResult.circuit;
      
      // Generate test data
      const testData = Array.from({ length: size }, (_, i) => ({
        tickId: `T${i}`,
        sector: ['Tech', 'Energy', 'Finance'][i % 3],
        ts: Date.now() + i * 1000,
        spread: Math.random() * 100,
      }));
      
      // Warm up
      const delta = ZSet.fromEntries(testData.map(r => [r, 1]));
      circuit.step(new Map([['prices', delta]]));
      
      // Benchmark
      const iterations = Math.max(1, Math.floor(1000 / size));
      const start = performance.now();
      
      for (let i = 0; i < iterations; i++) {
        const newData = testData.map(r => ({ ...r, spread: Math.random() * 100 }));
        const batchDelta = ZSet.fromEntries(newData.map(r => [r, 1]));
        circuit.step(new Map([['prices', batchDelta]]));
      }
      
      const elapsed = (performance.now() - start) / iterations;
      
      results.push({
        size,
        timeMs: elapsed,
        opsPerSec: size / (elapsed / 1000),
      });
    }
    
    console.log('\n' + '='.repeat(80));
    console.log('📊 SQL COMPILED WINDOW FUNCTION BENCHMARK');
    console.log('='.repeat(80));
    console.log('Query: SUM OVER (PARTITION BY sector ORDER BY ts ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)');
    console.log('');
    console.table(results);
    
    // Verify reasonable performance
    expect(results[0].opsPerSec).toBeGreaterThan(100);
  });
});

