import { useState, useEffect } from 'react';
import { SectionHeading } from '../components/SectionHeading';
import { CodeBlock } from '../components/CodeBlock';

const freshnessQueueCode = `// Circular buffer with overflow handling
class FreshnessQueue<T> {
  private buffer: (T | null)[];
  private head = 0;
  private tail = 0;
  private size = 0;
  
  constructor(
    private capacity: number,
    private onDrop?: (item: T) => void
  ) {
    this.buffer = new Array(capacity).fill(null);
  }
  
  push(item: T): void {
    if (this.size === this.capacity) {
      // Buffer full: drop oldest message
      const dropped = this.buffer[this.head];
      if (dropped && this.onDrop) {
        this.onDrop(dropped);
      }
      this.head = (this.head + 1) % this.capacity;
      this.size--;
    }
    
    this.buffer[this.tail] = item;
    this.tail = (this.tail + 1) % this.capacity;
    this.size++;
  }
}`;

const maxRowsCode = `// FIFO eviction with configurable max rows
const source = useDBSPSource<SensorReading>({
  name: 'sensors',
  key: 'sensorId',
  maxRows: 10000, // Keep last 10K rows
});

// Internally:
class EvictionManager {
  private insertOrder: string[] = [];
  
  onInsert(key: string) {
    this.insertOrder.push(key);
    
    while (this.insertOrder.length > this.maxRows) {
      const oldest = this.insertOrder.shift()!;
      this.emit({ key: oldest, weight: -1 }); // Delete
    }
  }
}`;

export function MemoryManagement() {
  const [bufferSize] = useState(20);
  const [buffer, setBuffer] = useState<number[]>([]);
  const [droppedCount, setDroppedCount] = useState(0);
  const [totalPushed, setTotalPushed] = useState(0);

  useEffect(() => {
    const interval = setInterval(() => {
      const newValue = Math.round(Math.random() * 100);
      
      setBuffer(prev => {
        const newBuffer = [...prev, newValue];
        if (newBuffer.length > bufferSize) {
          setDroppedCount(d => d + 1);
          return newBuffer.slice(-bufferSize);
        }
        return newBuffer;
      });
      setTotalPushed(t => t + 1);
    }, 100);

    return () => clearInterval(interval);
  }, [bufferSize]);

  return (
    <section id="memory" className="py-20 bg-[#0d1117]">
      <div className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8 font-mono">
        <SectionHeading
          id="memory-heading"
          badge="Production Ready"
          title="Memory Management"
          subtitle="Bounded memory usage for infinite streams"
        />

        {/* Freshness Queue visualization */}
        <div className="max-w-4xl mx-auto mb-12 p-6 bg-[#0a0a0a] border border-[#39ff14]/30">
          <div className="flex items-center justify-between mb-4">
            <span className="text-[#39ff14]">┌─ FRESHNESS_QUEUE (size={bufferSize}) ─┐</span>
            <div className="flex items-center gap-4 text-xs">
              <span className="text-[#808080]">dropped: <span className="text-[#ff3333]">{droppedCount}</span></span>
              <span className="text-[#808080]">total: <span className="text-[#39ff14]">{totalPushed}</span></span>
            </div>
          </div>
          
          {/* Buffer visualization */}
          <div className="flex gap-1 mb-6 overflow-x-auto pb-2">
            {buffer.map((val, i) => {
              const isNewest = i === buffer.length - 1;
              const isOldest = i === 0 && buffer.length === bufferSize;
              return (
                <div
                  key={i}
                  className={`w-10 h-10 flex items-center justify-center text-xs transition-all ${
                    isNewest 
                      ? 'bg-[#39ff14] text-[#0a0a0a]' 
                      : isOldest
                        ? 'bg-[#ff3333]/20 border border-[#ff3333] text-[#ff3333]'
                        : 'bg-[#1a1a1a] border border-[#39ff14]/20 text-[#808080]'
                  }`}
                >
                  {val}
                </div>
              );
            })}
            {buffer.length < bufferSize && (
              Array.from({ length: bufferSize - buffer.length }).map((_, i) => (
                <div key={`empty-${i}`} className="w-10 h-10 border border-dashed border-[#808080]/30" />
              ))
            )}
          </div>
          
          {/* Legend */}
          <div className="flex gap-6 text-xs text-[#808080]">
            <div className="flex items-center gap-2">
              <div className="w-4 h-4 bg-[#39ff14]" />
              <span>NEWEST</span>
            </div>
            <div className="flex items-center gap-2">
              <div className="w-4 h-4 bg-[#ff3333]/20 border border-[#ff3333]" />
              <span>NEXT_TO_DROP</span>
            </div>
            <div className="flex items-center gap-2">
              <div className="w-4 h-4 border border-dashed border-[#808080]/30" />
              <span>EMPTY_SLOT</span>
            </div>
          </div>
          <div className="text-[#39ff14]/30 text-xs mt-4">└────────────────────────────────────┘</div>
        </div>

        {/* Code examples */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 mb-12">
          <div className="space-y-4">
            <div className="text-[#00ffff]">═══ FRESHNESS_QUEUE ═══</div>
            <CodeBlock code={freshnessQueueCode} language="typescript" filename="freshness-queue.ts" />
          </div>
          <div className="space-y-4">
            <div className="text-[#ff00ff]">═══ MAX_ROWS_EVICTION ═══</div>
            <CodeBlock code={maxRowsCode} language="typescript" filename="eviction.ts" />
          </div>
        </div>

        {/* Key points */}
        <div className="max-w-3xl mx-auto p-6 bg-[#0a0a0a] border border-[#ffb000]/30">
          <div className="text-[#ffb000] mb-4">┌─ MEMORY_GUARANTEES ─┐</div>
          <ul className="space-y-2 text-sm text-[#808080]">
            <li className="flex items-start gap-2">
              <span className="text-[#39ff14]">[✓]</span>
              <span><span className="text-[#e0e0e0]">Bounded memory</span> — Never exceed configured limits</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-[#39ff14]">[✓]</span>
              <span><span className="text-[#e0e0e0]">FIFO eviction</span> — Oldest data removed first</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-[#39ff14]">[✓]</span>
              <span><span className="text-[#e0e0e0]">Backpressure handling</span> — Graceful degradation under load</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-[#39ff14]">[✓]</span>
              <span><span className="text-[#e0e0e0]">Delta propagation</span> — Evictions emit proper delete deltas</span>
            </li>
          </ul>
          <div className="text-[#ffb000]/30 text-xs mt-4">└─────────────────────┘</div>
        </div>
      </div>
    </section>
  );
}
