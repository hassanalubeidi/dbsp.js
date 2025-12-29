import { useState } from 'react';
import { SectionHeading } from '../components/SectionHeading';
import { CodeBlock } from '../components/CodeBlock';

interface ZSetEntry {
  value: string;
  weight: number;
}

const zsetCode = `// Z-Set: A set with integer weights
class ZSet<T> {
  private data = new Map<string, { value: T; weight: number }>();

  // Insert adds weight (default: +1)
  insert(value: T, weight: number = 1) {
    const existing = this.data.get(key)?.weight ?? 0;
    const newWeight = existing + weight;
    
    if (newWeight === 0) {
      this.data.delete(key); // Weight 0 = not present
    } else {
      this.data.set(key, { value, weight: newWeight });
    }
  }
}

// Examples:
// Insert: zset.insert({ id: 1 }, +1)  // Add row
// Delete: zset.insert({ id: 1 }, -1)  // Remove row
// Update: zset.insert(oldRow, -1)     // -1 for old
//         zset.insert(newRow, +1)     // +1 for new`;

export function ZSets() {
  const [entries, setEntries] = useState<ZSetEntry[]>([
    { value: 'sensor-a', weight: 1 },
    { value: 'sensor-b', weight: 1 },
    { value: 'sensor-c', weight: 1 },
  ]);
  const [pendingDelta, setPendingDelta] = useState<ZSetEntry | null>(null);
  const [deltaHistory, setDeltaHistory] = useState<ZSetEntry[]>([]);

  const applyDelta = (value: string, weight: number) => {
    const delta: ZSetEntry = { value, weight };
    setPendingDelta(delta);
    setDeltaHistory(prev => [...prev.slice(-4), delta]);
    
    setTimeout(() => {
      setEntries(prev => {
        const existing = prev.find(e => e.value === value);
        if (existing) {
          const newWeight = existing.weight + weight;
          if (newWeight === 0) {
            return prev.filter(e => e.value !== value);
          }
          return prev.map(e => e.value === value ? { ...e, weight: newWeight } : e);
        }
        if (weight > 0) {
          return [...prev, { value, weight }];
        }
        return prev;
      });
      setPendingDelta(null);
    }, 500);
  };

  const addSensor = () => {
    const id = `sensor-${String.fromCharCode(97 + entries.length + Math.floor(Math.random() * 10))}`;
    applyDelta(id, +1);
  };

  const removeSensor = (value: string) => {
    applyDelta(value, -1);
  };

  return (
    <section id="zsets" className="py-20 bg-[#0a0a0a]">
      <div className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8 font-mono">
        <SectionHeading
          id="zsets-heading"
          badge="Core Concept"
          title="Z-Sets: Data with Weights"
          subtitle="The mathematical foundation that makes incremental computation possible"
        />

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 mb-12">
          {/* Visual representation */}
          <div className="space-y-6">
            {/* Current state */}
            <div className="p-6 bg-[#0d1117] border border-[#39ff14]/30">
              <div className="text-[#39ff14] text-sm mb-4">┌─ CURRENT_ZSET_STATE ─┐</div>
              <div className="space-y-2">
                {entries.map((entry) => (
                  <div
                    key={entry.value}
                    className="flex items-center justify-between p-3 bg-[#0a0a0a] border border-[#39ff14]/20 group"
                  >
                    <div className="flex items-center gap-3">
                      <span className={`w-10 h-8 flex items-center justify-center font-bold ${
                        entry.weight > 0 ? 'bg-[#39ff14] text-[#0a0a0a]' : 'bg-[#ff3333] text-[#0a0a0a]'
                      }`}>
                        {entry.weight > 0 ? '+' : ''}{entry.weight}
                      </span>
                      <span className="text-[#e0e0e0]">{entry.value}</span>
                    </div>
                    <button
                      onClick={() => removeSensor(entry.value)}
                      className="px-2 py-1 border border-[#ff3333]/50 text-[#ff3333] text-xs opacity-0 group-hover:opacity-100 transition-opacity hover:bg-[#ff3333]/10"
                    >
                      [-1] DELETE
                    </button>
                  </div>
                ))}
                {entries.length === 0 && (
                  <div className="text-center py-8 text-[#808080]">$ Empty Z-Set (∅)</div>
                )}
              </div>
              <button
                onClick={addSensor}
                className="mt-4 w-full px-4 py-2 bg-[#39ff14] text-[#0a0a0a] font-bold hover:bg-[#50ff30] transition-colors"
              >
                [+1] INSERT_NEW_SENSOR
              </button>
              <div className="text-[#39ff14]/30 text-xs mt-4">└─────────────────────┘</div>
            </div>

            {/* Pending delta */}
            {pendingDelta && (
              <div className="p-4 bg-[#ffb000]/10 border border-[#ffb000] animate-pulse">
                <div className="text-xs text-[#ffb000] mb-1">$ APPLYING_DELTA...</div>
                <div className="flex items-center gap-2">
                  <span className={`font-bold ${pendingDelta.weight > 0 ? 'text-[#39ff14]' : 'text-[#ff3333]'}`}>
                    Δ = {pendingDelta.weight > 0 ? '+' : ''}{pendingDelta.weight}
                  </span>
                  <span className="text-[#e0e0e0]">{pendingDelta.value}</span>
                </div>
              </div>
            )}

            {/* Delta history */}
            <div className="p-6 bg-[#0d1117] border border-[#00ffff]/30">
              <div className="text-[#00ffff] text-sm mb-4">┌─ DELTA_HISTORY ─┐</div>
              <div className="space-y-2">
                {deltaHistory.length === 0 ? (
                  <div className="text-center py-4 text-[#808080] text-sm">
                    $ Click buttons to see deltas...
                    <span className="inline-block w-2 h-4 bg-[#39ff14] animate-pulse ml-1" />
                  </div>
                ) : (
                  deltaHistory.map((delta, i) => (
                    <div
                      key={i}
                      className={`flex items-center gap-2 p-2 text-sm ${
                        delta.weight > 0 ? 'bg-[#39ff14]/10 border-l-2 border-[#39ff14]' : 'bg-[#ff3333]/10 border-l-2 border-[#ff3333]'
                      }`}
                    >
                      <span className={`font-bold ${delta.weight > 0 ? 'text-[#39ff14]' : 'text-[#ff3333]'}`}>
                        Δ = {delta.weight > 0 ? '+' : ''}{delta.weight}
                      </span>
                      <span className="text-[#808080]">{delta.value}</span>
                    </div>
                  ))
                )}
              </div>
              <div className="text-[#00ffff]/30 text-xs mt-4">└─────────────────┘</div>
            </div>
          </div>

          {/* Code explanation */}
          <div className="space-y-6">
            <CodeBlock code={zsetCode} language="typescript" filename="zset.ts" />
            
            <div className="p-6 bg-[#0d1117] border border-[#39ff14]/30">
              <div className="text-[#39ff14] text-sm mb-4">┌─ WHY_ZSETS_MATTER ─┐</div>
              <ul className="space-y-3 text-sm text-[#808080]">
                <li className="flex items-start gap-2">
                  <span className="text-[#39ff14]">[✓]</span>
                  <span><span className="text-[#e0e0e0]">Unified representation</span> — Data and changes use the same structure</span>
                </li>
                <li className="flex items-start gap-2">
                  <span className="text-[#39ff14]">[✓]</span>
                  <span><span className="text-[#e0e0e0]">Abelian group</span> — We can add/subtract Z-sets mathematically</span>
                </li>
                <li className="flex items-start gap-2">
                  <span className="text-[#39ff14]">[✓]</span>
                  <span><span className="text-[#e0e0e0]">Weight semantics</span> — +1 = insert, -1 = delete, 0 = absent</span>
                </li>
                <li className="flex items-start gap-2">
                  <span className="text-[#39ff14]">[✓]</span>
                  <span><span className="text-[#e0e0e0]">Multiset support</span> — Weights &gt;1 represent duplicates</span>
                </li>
              </ul>
              <div className="text-[#39ff14]/30 text-xs mt-4">└────────────────────┘</div>
            </div>
          </div>
        </div>

        {/* Formula explanation */}
        <div className="max-w-3xl mx-auto text-center">
          <div className="p-6 bg-[#0d1117] border border-[#00ffff]/30">
            <div className="text-[#00ffff] text-sm mb-4">┌─ THE_UPDATE_FORMULA ─┐</div>
            <div className="flex items-center justify-center gap-4 text-2xl mb-4">
              <span className="text-[#808080]">DB[t]</span>
              <span className="text-[#808080]">=</span>
              <span className="text-[#808080]">DB[t-1]</span>
              <span className="text-[#39ff14] text-glow-green">+</span>
              <span className="text-[#39ff14] text-glow-green">ΔDB[t]</span>
            </div>
            <p className="text-sm text-[#808080]">
              The current database state is the previous state plus the delta. 
              <span className="text-[#39ff14]"> This is just Z-set addition!</span>
            </p>
            <div className="text-[#00ffff]/30 text-xs mt-4">└──────────────────────┘</div>
          </div>
        </div>
      </div>
    </section>
  );
}
