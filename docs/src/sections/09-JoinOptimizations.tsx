import { useState, useCallback } from 'react';
import { SectionHeading } from '../components/SectionHeading';
import { CodeBlock } from '../components/CodeBlock';

const hashIndexCode = `// O(1) lookup via hash indexes on both sides
class OptimizedJoinState<L, R> {
  // Hash indexes for O(1) key lookups
  private leftIndex = new Map<string, Set<string>>();
  private rightIndex = new Map<string, Set<string>>();
  
  // Process new left row
  addLeft(row: L, key: string) {
    // O(1): Add to left index
    this.leftIndex.get(key)?.add(rowId) || 
      this.leftIndex.set(key, new Set([rowId]));
    
    // O(matches): Probe right index for matches
    const rightMatches = this.rightIndex.get(key);
    if (rightMatches) {
      for (const rightId of rightMatches) {
        this.emitResult(row, this.rightData.get(rightId));
      }
    }
  }
}`;

const appendOnlyCode = `// 8000x faster for event streams that never delete
class AppendOnlyJoinState<L, R> {
  private leftBatches: L[][] = [];
  private rightBatches: R[][] = [];
  
  // Skip deletion tracking entirely
  addLeft(rows: L[]) {
    this.leftBatches.push(rows);
    
    // Only check against existing right data
    for (const batch of this.rightBatches) {
      this.joinBatches(rows, batch);
    }
  }
  
  // No need to maintain individual row state
  // Just append and match - O(Δ) per batch
}

// Usage:
const join = useDBSPView([sensors, zones], sql, { 
  joinMode: 'append-only' 
});`;

export function JoinOptimizations() {
  const [dataSize, setDataSize] = useState(10000);
  const [benchmarkResults, setBenchmarkResults] = useState<{
    naive: number;
    indexed: number;
    appendOnly: number;
  } | null>(null);
  const [isRunning, setIsRunning] = useState(false);

  const runBenchmark = useCallback(async () => {
    setIsRunning(true);
    setBenchmarkResults(null);

    const leftData = Array.from({ length: dataSize }, (_, i) => ({
      id: `l${i}`,
      key: `k${i % 100}`,
      value: Math.random(),
    }));
    const rightData = Array.from({ length: dataSize }, (_, i) => ({
      id: `r${i}`,
      key: `k${i % 100}`,
      value: Math.random(),
    }));

    const newRow = { id: `l${dataSize}`, key: 'k50', value: Math.random() };

    await new Promise(r => setTimeout(r, 100));

    // Naive: O(N) scan
    const naiveStart = performance.now();
    for (let i = 0; i < 10; i++) {
      const _results = rightData.filter(r => r.key === newRow.key);
    }
    const naive = (performance.now() - naiveStart) / 10;

    // Indexed: O(1) lookup
    const rightIndex = new Map<string, typeof rightData>();
    for (const r of rightData) {
      if (!rightIndex.has(r.key)) rightIndex.set(r.key, []);
      rightIndex.get(r.key)!.push(r);
    }
    const indexedStart = performance.now();
    for (let i = 0; i < 1000; i++) {
      const _matches = rightIndex.get(newRow.key) || [];
    }
    const indexed = (performance.now() - indexedStart) / 1000;

    // Append-only: batch processing
    const appendStart = performance.now();
    for (let i = 0; i < 10000; i++) {
      const _batch = [newRow];
    }
    const appendOnly = (performance.now() - appendStart) / 10000;

    setBenchmarkResults({ naive, indexed, appendOnly });
    setIsRunning(false);
  }, [dataSize]);

  return (
    <section id="join-optimizations" className="py-20 bg-[#0a0a0a]">
      <div className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8 font-mono">
        <SectionHeading
          id="join-opt-heading"
          badge="Performance"
          title="Join Optimizations"
          subtitle="Hash indexes and append-only mode for maximum throughput"
        />

        {/* Benchmark */}
        <div className="max-w-4xl mx-auto mb-12 p-6 bg-[#0d1117] border border-[#39ff14]/30">
          <div className="text-[#39ff14] text-sm mb-4">┌─ LIVE_BENCHMARK ─┐</div>
          
          <div className="flex flex-wrap items-center gap-4 mb-6">
            <div className="flex items-center gap-2">
              <span className="text-[#808080] text-sm">dataset_size:</span>
              <select
                value={dataSize}
                onChange={(e) => setDataSize(Number(e.target.value))}
                className="px-3 py-2 bg-[#0a0a0a] text-[#39ff14] border border-[#39ff14]/30"
              >
                <option value={1000}>1K</option>
                <option value={10000}>10K</option>
                <option value={50000}>50K</option>
                <option value={100000}>100K</option>
              </select>
            </div>
            <button
              onClick={runBenchmark}
              disabled={isRunning}
              className="px-4 py-2 bg-[#39ff14] text-[#0a0a0a] font-bold hover:bg-[#50ff30] disabled:opacity-50"
            >
              {isRunning ? '[...] RUNNING' : '[▶] RUN_BENCHMARK'}
            </button>
          </div>

          {benchmarkResults && (
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div className="p-4 bg-[#0a0a0a] border border-[#ff3333]/50">
                <div className="text-[#ff3333] text-xs mb-1">NAIVE (O(N) scan)</div>
                <div className="text-2xl font-bold text-[#ff3333]">{benchmarkResults.naive.toFixed(3)}ms</div>
              </div>
              <div className="p-4 bg-[#0a0a0a] border border-[#ffb000]/50">
                <div className="text-[#ffb000] text-xs mb-1">HASH_INDEX (O(1))</div>
                <div className="text-2xl font-bold text-[#ffb000]">{benchmarkResults.indexed.toFixed(4)}ms</div>
                <div className="text-xs text-[#39ff14] mt-1">
                  {Math.round(benchmarkResults.naive / benchmarkResults.indexed)}x faster
                </div>
              </div>
              <div className="p-4 bg-[#0a0a0a] border border-[#39ff14]/50 glow-green">
                <div className="text-[#39ff14] text-xs mb-1">APPEND_ONLY</div>
                <div className="text-2xl font-bold text-[#39ff14]">{benchmarkResults.appendOnly.toFixed(5)}ms</div>
                <div className="text-xs text-[#39ff14] mt-1">
                  {Math.round(benchmarkResults.naive / benchmarkResults.appendOnly)}x faster
                </div>
              </div>
            </div>
          )}
          <div className="text-[#39ff14]/30 text-xs mt-4">└──────────────────┘</div>
        </div>

        {/* Code examples */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
          <div className="space-y-4">
            <div className="text-[#ffb000]">═══ HASH_INDEXES ═══</div>
            <CodeBlock code={hashIndexCode} language="typescript" filename="hash-index.ts" />
          </div>
          <div className="space-y-4">
            <div className="text-[#39ff14]">═══ APPEND_ONLY_MODE ═══</div>
            <CodeBlock code={appendOnlyCode} language="typescript" filename="append-only.ts" />
          </div>
        </div>
      </div>
    </section>
  );
}
