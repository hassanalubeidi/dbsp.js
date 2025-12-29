import { useState, useEffect } from 'react';
import { SectionHeading } from '../components/SectionHeading';

interface JoinEntry {
  side: 'left' | 'right';
  key: string;
  value: string;
  isNew?: boolean;
}

export function BilinearJoins() {
  const [leftData, setLeftData] = useState<JoinEntry[]>([
    { side: 'left', key: 'k1', value: 'sensor-a' },
    { side: 'left', key: 'k2', value: 'sensor-b' },
  ]);
  const [rightData, setRightData] = useState<JoinEntry[]>([
    { side: 'right', key: 'k1', value: 'Zone A' },
    { side: 'right', key: 'k2', value: 'Zone B' },
  ]);
  const [results, setResults] = useState<{ left: string; right: string; key: string }[]>([]);
  const [highlightedResults, setHighlightedResults] = useState<Set<string>>(new Set());

  useEffect(() => {
    const newResults: { left: string; right: string; key: string }[] = [];
    const highlighted = new Set<string>();
    
    for (const l of leftData) {
      for (const r of rightData) {
        if (l.key === r.key) {
          const id = `${l.value}-${r.value}`;
          newResults.push({ left: l.value, right: r.value, key: l.key });
          if (l.isNew || r.isNew) {
            highlighted.add(id);
          }
        }
      }
    }
    
    setResults(newResults);
    setHighlightedResults(highlighted);
  }, [leftData, rightData]);

  const addLeft = () => {
    const key = `k${Math.floor(Math.random() * 3) + 1}`;
    const entry: JoinEntry = {
      side: 'left',
      key,
      value: `sensor-${String.fromCharCode(97 + leftData.length)}`,
      isNew: true,
    };
    setLeftData(prev => [...prev, entry]);
    setTimeout(() => {
      setLeftData(prev => prev.map(e => ({ ...e, isNew: false })));
    }, 1000);
  };

  const addRight = () => {
    const key = `k${Math.floor(Math.random() * 3) + 1}`;
    const entry: JoinEntry = {
      side: 'right',
      key,
      value: `Zone-${String.fromCharCode(65 + rightData.length)}`,
      isNew: true,
    };
    setRightData(prev => [...prev, entry]);
    setTimeout(() => {
      setRightData(prev => prev.map(e => ({ ...e, isNew: false })));
    }, 1000);
  };

  const clear = () => {
    setLeftData([
      { side: 'left', key: 'k1', value: 'sensor-a' },
      { side: 'left', key: 'k2', value: 'sensor-b' },
    ]);
    setRightData([
      { side: 'right', key: 'k1', value: 'Zone A' },
      { side: 'right', key: 'k2', value: 'Zone B' },
    ]);
  };

  return (
    <section id="bilinear-joins" className="py-20 bg-[#0d1117]">
      <div className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8 font-mono">
        <SectionHeading
          id="joins-heading"
          badge="Bilinear Operators"
          title="The Incremental Join Formula"
          subtitle="How DBSP processes JOINs in O(Δ) time"
        />

        {/* The formula */}
        <div className="max-w-4xl mx-auto mb-12">
          <div className="p-8 bg-[#0a0a0a] border border-[#39ff14]/50 glow-green">
            <div className="text-center mb-4">
              <span className="text-sm text-[#808080]">$ From the DBSP paper (Section 5):</span>
            </div>
            <div className="flex flex-wrap items-center justify-center gap-2 text-lg md:text-2xl mb-6">
              <span className="text-[#39ff14]">Δ(A ⋈ B)</span>
              <span className="text-[#808080]">=</span>
              <span className="text-[#ffb000]">(ΔA ⋈ ΔB)</span>
              <span className="text-[#808080]">+</span>
              <span className="text-[#00ffff]">(A<sub>prev</sub> ⋈ ΔB)</span>
              <span className="text-[#808080]">+</span>
              <span className="text-[#ff00ff]">(ΔA ⋈ B<sub>prev</sub>)</span>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm text-center">
              <div className="p-3 bg-[#ffb000]/10 border border-[#ffb000]/30">
                <span className="text-[#ffb000]">ΔA ⋈ ΔB</span>
                <p className="text-[#808080] mt-1">New × New (tiny)</p>
              </div>
              <div className="p-3 bg-[#00ffff]/10 border border-[#00ffff]/30">
                <span className="text-[#00ffff]">A<sub>prev</sub> ⋈ ΔB</span>
                <p className="text-[#808080] mt-1">Existing × New (indexed)</p>
              </div>
              <div className="p-3 bg-[#ff00ff]/10 border border-[#ff00ff]/30">
                <span className="text-[#ff00ff]">ΔA ⋈ B<sub>prev</sub></span>
                <p className="text-[#808080] mt-1">New × Existing (indexed)</p>
              </div>
            </div>
          </div>
        </div>

        {/* Interactive visualization */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-12">
          {/* Left table */}
          <div className="p-6 bg-[#0a0a0a] border border-[#00ffff]/30">
            <div className="flex items-center justify-between mb-4">
              <span className="text-[#00ffff]">┌─ TABLE_A ─┐</span>
              <button
                onClick={addLeft}
                className="px-3 py-1 bg-[#00ffff] text-[#0a0a0a] text-sm font-bold hover:bg-[#50ffff]"
              >
                [+] ADD
              </button>
            </div>
            <div className="space-y-2">
              {leftData.map((entry, i) => (
                <div
                  key={i}
                  className={`flex items-center justify-between p-3 ${
                    entry.isNew
                      ? 'bg-[#00ffff]/20 border border-[#00ffff] animate-pulse'
                      : 'bg-[#1a1a1a] border border-[#00ffff]/20'
                  }`}
                >
                  <span className="text-[#e0e0e0]">{entry.value}</span>
                  <span className="px-2 py-0.5 bg-[#00ffff]/20 text-[#00ffff] text-xs">
                    {entry.key}
                  </span>
                </div>
              ))}
            </div>
            <div className="text-[#00ffff]/30 text-xs mt-4">└────────────┘</div>
          </div>

          {/* Join results */}
          <div className="p-6 bg-[#0a0a0a] border border-[#39ff14]/50">
            <div className="flex items-center justify-between mb-4">
              <span className="text-[#39ff14]">┌─ A ⋈ B ─┐</span>
              <span className="text-xs text-[#808080]">ON key</span>
            </div>
            <div className="space-y-2 max-h-64 overflow-y-auto">
              {results.length === 0 ? (
                <div className="text-center py-8 text-[#808080]">$ No matches</div>
              ) : (
                results.map((result, i) => {
                  const id = `${result.left}-${result.right}`;
                  const isHighlighted = highlightedResults.has(id);
                  return (
                    <div
                      key={i}
                      className={`flex items-center justify-between p-3 ${
                        isHighlighted
                          ? 'bg-[#39ff14]/20 border border-[#39ff14]'
                          : 'bg-[#1a1a1a] border border-[#39ff14]/20'
                      }`}
                    >
                      <div className="flex items-center gap-2 text-sm">
                        <span className="text-[#00ffff]">{result.left}</span>
                        <span className="text-[#808080]">×</span>
                        <span className="text-[#ff00ff]">{result.right}</span>
                      </div>
                      <span className="px-2 py-0.5 bg-[#39ff14]/20 text-[#39ff14] text-xs">
                        {result.key}
                      </span>
                    </div>
                  );
                })
              )}
            </div>
            <div className="text-[#39ff14]/30 text-xs mt-4">└──────────┘</div>
          </div>

          {/* Right table */}
          <div className="p-6 bg-[#0a0a0a] border border-[#ff00ff]/30">
            <div className="flex items-center justify-between mb-4">
              <span className="text-[#ff00ff]">┌─ TABLE_B ─┐</span>
              <button
                onClick={addRight}
                className="px-3 py-1 bg-[#ff00ff] text-[#0a0a0a] text-sm font-bold hover:bg-[#ff50ff]"
              >
                [+] ADD
              </button>
            </div>
            <div className="space-y-2">
              {rightData.map((entry, i) => (
                <div
                  key={i}
                  className={`flex items-center justify-between p-3 ${
                    entry.isNew
                      ? 'bg-[#ff00ff]/20 border border-[#ff00ff] animate-pulse'
                      : 'bg-[#1a1a1a] border border-[#ff00ff]/20'
                  }`}
                >
                  <span className="px-2 py-0.5 bg-[#ff00ff]/20 text-[#ff00ff] text-xs">
                    {entry.key}
                  </span>
                  <span className="text-[#e0e0e0]">{entry.value}</span>
                </div>
              ))}
            </div>
            <div className="text-[#ff00ff]/30 text-xs mt-4">└────────────┘</div>
          </div>
        </div>

        <div className="text-center mb-12">
          <button
            onClick={clear}
            className="px-4 py-2 border border-[#ff3333]/50 text-[#ff3333] hover:bg-[#ff3333]/10"
          >
            [X] RESET
          </button>
        </div>

        {/* Key insight */}
        <div className="max-w-3xl mx-auto p-6 bg-[#0a0a0a] border border-[#ffb000]/50">
          <div className="text-[#ffb000] mb-4">┌─ KEY_INSIGHT ─┐</div>
          <p className="text-[#808080] mb-4">
            Instead of computing full <span className="text-[#e0e0e0]">A ⋈ B</span> (O(|A| × |B|)), we:
          </p>
          <ol className="space-y-2 text-sm text-[#808080] list-decimal list-inside">
            <li>Maintain <span className="text-[#39ff14]">hash indexes</span> on both tables</li>
            <li>When new row arrives, <span className="text-[#39ff14]">probe the opposite index</span></li>
            <li>Only produce <span className="text-[#39ff14]">new matching pairs</span></li>
          </ol>
          <p className="mt-4 text-[#808080]">
            Result: <span className="text-[#39ff14]">O(Δ)</span> per update instead of O(N²)
          </p>
          <div className="text-[#ffb000]/30 text-xs mt-4">└───────────────┘</div>
        </div>
      </div>
    </section>
  );
}
