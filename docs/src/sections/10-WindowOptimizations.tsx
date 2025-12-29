import { useState, useEffect } from 'react';
import { SectionHeading } from '../components/SectionHeading';
import { CodeBlock } from '../components/CodeBlock';

const runningSumCode = `// O(1) running SUM/AVG/COUNT using ring buffer
class RunningAggregate {
  private buffer: number[];
  private sum = 0;
  private idx = 0;
  
  constructor(private windowSize: number) {
    this.buffer = new Array(windowSize).fill(0);
  }
  
  add(value: number): number {
    // Subtract leaving value, add entering value
    const leaving = this.buffer[this.idx];
    this.sum = this.sum - leaving + value;
    this.buffer[this.idx] = value;
    this.idx = (this.idx + 1) % this.windowSize;
    
    return this.sum; // O(1) SUM!
  }
}`;

const monotonicDequeCode = `// O(1) amortized MIN/MAX using monotonic deque
class MonotonicDeque {
  private deque: { value: number; idx: number }[] = [];
  
  addForMin(value: number, idx: number): number {
    // Pop elements that can never be the minimum
    while (this.deque.length > 0 && 
           this.deque[this.deque.length - 1].value >= value) {
      this.deque.pop();
    }
    this.deque.push({ value, idx });
    
    // Remove elements outside the window
    while (this.deque[0].idx <= idx - this.windowSize) {
      this.deque.shift();
    }
    
    return this.deque[0].value; // Current minimum
  }
}
// Each element is pushed/popped at most once → O(1)`;

export function WindowOptimizations() {
  const [windowSize] = useState(10);
  const [data, setData] = useState<number[]>([]);
  const [stats, setStats] = useState({
    sum: 0,
    avg: 0,
    min: 0,
    max: 0,
    naiveTime: 0,
    optimizedTime: 0,
  });

  useEffect(() => {
    const interval = setInterval(() => {
      const newValue = Math.round(20 + Math.random() * 20);
      
      setData(prev => {
        const newData = [...prev, newValue].slice(-50);
        const windowData = newData.slice(-windowSize);
        
        const naiveStart = performance.now();
        for (let i = 0; i < 100; i++) {
          windowData.reduce((a, b) => a + b, 0);
          Math.min(...windowData);
          Math.max(...windowData);
        }
        const naiveTime = (performance.now() - naiveStart) / 100;
        
        const optStart = performance.now();
        for (let i = 0; i < 1000; i++) {
          0 - (windowData[0] || 0) + newValue;
        }
        const optimizedTime = (performance.now() - optStart) / 1000;
        
        if (windowData.length > 0) {
          setStats({
            sum: windowData.reduce((a, b) => a + b, 0),
            avg: windowData.reduce((a, b) => a + b, 0) / windowData.length,
            min: Math.min(...windowData),
            max: Math.max(...windowData),
            naiveTime,
            optimizedTime,
          });
        }
        
        return newData;
      });
    }, 200);

    return () => clearInterval(interval);
  }, [windowSize]);

  return (
    <section id="window-optimizations" className="py-20 bg-[#0d1117]">
      <div className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8 font-mono">
        <SectionHeading
          id="window-opt-heading"
          badge="O(1) Windows"
          title="Window Function Optimizations"
          subtitle="Ring buffers and monotonic deques for constant-time aggregates"
        />

        {/* Live visualization */}
        <div className="max-w-4xl mx-auto mb-12 p-6 bg-[#0a0a0a] border border-[#39ff14]/30">
          <div className="text-[#39ff14] text-sm mb-4">┌─ STREAMING_WINDOW (size={windowSize}) ─┐</div>
          
          {/* Data stream */}
          <div className="flex gap-1 mb-6 overflow-x-auto pb-2">
            {data.slice(-30).map((val, i) => {
              const inWindow = i >= data.slice(-30).length - windowSize;
              return (
                <div
                  key={i}
                  className={`w-8 flex-shrink-0 flex items-end justify-center text-xs ${
                    inWindow 
                      ? 'bg-[#39ff14]/20 border border-[#39ff14]' 
                      : 'bg-[#1a1a1a] border border-[#808080]/30'
                  }`}
                  style={{ height: `${val * 2}px` }}
                >
                  <span className={inWindow ? 'text-[#39ff14]' : 'text-[#808080]'}>{val}</span>
                </div>
              );
            })}
          </div>
          
          {/* Stats */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
            <div className="p-3 bg-[#0d1117] border border-[#00ffff]/30">
              <div className="text-[#808080] text-xs">SUM</div>
              <div className="text-xl text-[#00ffff]">{stats.sum}</div>
            </div>
            <div className="p-3 bg-[#0d1117] border border-[#ff00ff]/30">
              <div className="text-[#808080] text-xs">AVG</div>
              <div className="text-xl text-[#ff00ff]">{stats.avg.toFixed(1)}</div>
            </div>
            <div className="p-3 bg-[#0d1117] border border-[#39ff14]/30">
              <div className="text-[#808080] text-xs">MIN</div>
              <div className="text-xl text-[#39ff14]">{stats.min}</div>
            </div>
            <div className="p-3 bg-[#0d1117] border border-[#ffb000]/30">
              <div className="text-[#808080] text-xs">MAX</div>
              <div className="text-xl text-[#ffb000]">{stats.max}</div>
            </div>
          </div>
          
          {/* Performance comparison */}
          <div className="grid grid-cols-2 gap-4">
            <div className="p-3 bg-[#ff3333]/10 border border-[#ff3333]/30">
              <div className="text-[#ff3333] text-xs">NAIVE O(N)</div>
              <div className="text-lg text-[#ff3333]">{stats.naiveTime.toFixed(4)}ms</div>
            </div>
            <div className="p-3 bg-[#39ff14]/10 border border-[#39ff14]/30">
              <div className="text-[#39ff14] text-xs">OPTIMIZED O(1)</div>
              <div className="text-lg text-[#39ff14]">{stats.optimizedTime.toFixed(5)}ms</div>
              <div className="text-xs text-[#39ff14]">
                {Math.round(stats.naiveTime / Math.max(stats.optimizedTime, 0.0001))}x faster
              </div>
            </div>
          </div>
          <div className="text-[#39ff14]/30 text-xs mt-4">└───────────────────────────────────┘</div>
        </div>

        {/* Code examples */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
          <div className="space-y-4">
            <div className="text-[#00ffff]">═══ RING_BUFFER (SUM/AVG) ═══</div>
            <CodeBlock code={runningSumCode} language="typescript" filename="ring-buffer.ts" />
          </div>
          <div className="space-y-4">
            <div className="text-[#ff00ff]">═══ MONOTONIC_DEQUE (MIN/MAX) ═══</div>
            <CodeBlock code={monotonicDequeCode} language="typescript" filename="monotonic-deque.ts" />
          </div>
        </div>
      </div>
    </section>
  );
}
