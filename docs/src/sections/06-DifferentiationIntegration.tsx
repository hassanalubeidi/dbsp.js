import { useState } from 'react';
import { SectionHeading } from '../components/SectionHeading';

interface TimeSlice {
  time: number;
  snapshot: string[];
  delta: { value: string; weight: number }[];
}

export function DifferentiationIntegration() {
  const [currentTime, setCurrentTime] = useState(0);
  
  const timeline: TimeSlice[] = [
    { time: 0, snapshot: [], delta: [] },
    { time: 1, snapshot: ['A'], delta: [{ value: 'A', weight: +1 }] },
    { time: 2, snapshot: ['A', 'B'], delta: [{ value: 'B', weight: +1 }] },
    { time: 3, snapshot: ['A', 'B', 'C'], delta: [{ value: 'C', weight: +1 }] },
    { time: 4, snapshot: ['A', 'C'], delta: [{ value: 'B', weight: -1 }] },
    { time: 5, snapshot: ['A', 'C', 'D'], delta: [{ value: 'D', weight: +1 }] },
  ];

  const current = timeline[currentTime];
  const prev = timeline[Math.max(0, currentTime - 1)];

  return (
    <section id="diff-int" className="py-20 bg-[#0d1117]">
      <div className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8 font-mono">
        <SectionHeading
          id="diff-int-heading"
          badge="Core Operators"
          title="Differentiation & Integration"
          subtitle="The two fundamental operators that make incremental computation work"
        />

        {/* Time slider */}
        <div className="max-w-2xl mx-auto mb-12">
          <div className="flex items-center justify-center gap-4 mb-4">
            {timeline.map((_, i) => (
              <button
                key={i}
                onClick={() => setCurrentTime(i)}
                className={`w-12 h-12 font-bold transition-all ${
                  i === currentTime
                    ? 'bg-[#39ff14] text-[#0a0a0a] glow-green'
                    : 'bg-[#1a1a1a] text-[#808080] hover:text-[#39ff14] border border-[#39ff14]/30'
                }`}
              >
                t{i}
              </button>
            ))}
          </div>
          <div className="text-center text-sm text-[#808080]">
            $ Select a time step to view state
            <span className="inline-block w-2 h-4 bg-[#39ff14] animate-pulse ml-1" />
          </div>
        </div>

        {/* Main visualization */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-12">
          {/* Snapshot at t-1 */}
          <div className="p-6 bg-[#0a0a0a] border border-[#808080]/30">
            <div className="flex items-center justify-between mb-4">
              <span className="text-[#808080] text-sm">┌─ SNAPSHOT[t-1] ─┐</span>
              <span className="text-xs text-[#808080]">PREV_STATE</span>
            </div>
            <div className="flex flex-wrap gap-2 min-h-20">
              {currentTime === 0 ? (
                <span className="text-[#808080]">∅ (empty)</span>
              ) : (
                prev.snapshot.map(item => (
                  <div
                    key={item}
                    className="px-4 py-2 bg-[#1a1a1a] border border-[#808080]/30 text-[#e0e0e0]"
                  >
                    {item}
                  </div>
                ))
              )}
            </div>
            <div className="text-[#808080]/30 text-xs mt-4">└──────────────────┘</div>
          </div>

          {/* Delta */}
          <div className="p-6 bg-[#0a0a0a] border border-[#39ff14]/50">
            <div className="flex items-center justify-between mb-4">
              <span className="text-[#39ff14] text-sm">┌─ Δ[t] ─┐</span>
              <span className="text-xs text-[#39ff14]">CHANGE</span>
            </div>
            <div className="flex flex-wrap gap-2 min-h-20">
              {current.delta.length === 0 ? (
                <span className="text-[#808080]">∅ (no_changes)</span>
              ) : (
                current.delta.map((d, i) => (
                  <div
                    key={i}
                    className={`px-4 py-2 flex items-center gap-2 ${
                      d.weight > 0
                        ? 'bg-[#39ff14]/20 text-[#39ff14] border border-[#39ff14]/50'
                        : 'bg-[#ff3333]/20 text-[#ff3333] border border-[#ff3333]/50'
                    }`}
                  >
                    <span className="font-bold">{d.weight > 0 ? '+' : ''}{d.weight}</span>
                    <span>{d.value}</span>
                  </div>
                ))
              )}
            </div>
            <div className="text-[#39ff14]/30 text-xs mt-4">└─────────┘</div>
          </div>

          {/* Snapshot at t */}
          <div className="p-6 bg-[#0a0a0a] border border-[#00ffff]/50">
            <div className="flex items-center justify-between mb-4">
              <span className="text-[#00ffff] text-sm">┌─ SNAPSHOT[t] ─┐</span>
              <span className="text-xs text-[#00ffff]">CURRENT</span>
            </div>
            <div className="flex flex-wrap gap-2 min-h-20">
              {current.snapshot.length === 0 ? (
                <span className="text-[#808080]">∅ (empty)</span>
              ) : (
                current.snapshot.map(item => (
                  <div
                    key={item}
                    className={`px-4 py-2 transition-all ${
                      current.delta.some(d => d.value === item && d.weight > 0)
                        ? 'bg-[#39ff14] text-[#0a0a0a] animate-pulse'
                        : 'bg-[#1a1a1a] border border-[#00ffff]/30 text-[#e0e0e0]'
                    }`}
                  >
                    {item}
                  </div>
                ))
              )}
            </div>
            <div className="text-[#00ffff]/30 text-xs mt-4">└─────────────────┘</div>
          </div>
        </div>

        {/* Operators explanation */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-8 mb-12">
          {/* Differentiate */}
          <div className="p-6 bg-[#0a0a0a] border border-[#ff00ff]/30">
            <div className="flex items-center gap-3 mb-4">
              <span className="w-12 h-12 flex items-center justify-center bg-[#ff00ff] text-[#0a0a0a] font-bold text-xl">
                D
              </span>
              <div>
                <h4 className="text-[#ff00ff]">DIFFERENTIATE</h4>
                <p className="text-sm text-[#808080]">Compute changes between snapshots</p>
              </div>
            </div>
            <div className="p-4 bg-[#0d1117] border border-[#ff00ff]/20 text-center mb-4">
              <span className="text-[#ff00ff]">D(</span>
              <span className="text-[#e0e0e0]">stream</span>
              <span className="text-[#ff00ff]">)[t]</span>
              <span className="text-[#808080]"> = </span>
              <span className="text-[#e0e0e0]">stream[t]</span>
              <span className="text-[#39ff14]"> - </span>
              <span className="text-[#e0e0e0]">stream[t-1]</span>
            </div>
            <p className="text-sm text-[#808080]">
              Takes snapshots → outputs <span className="text-[#e0e0e0]">deltas</span> (what changed)
            </p>
          </div>

          {/* Integrate */}
          <div className="p-6 bg-[#0a0a0a] border border-[#00ffff]/30">
            <div className="flex items-center gap-3 mb-4">
              <span className="w-12 h-12 flex items-center justify-center bg-[#00ffff] text-[#0a0a0a] font-bold text-xl">
                I
              </span>
              <div>
                <h4 className="text-[#00ffff]">INTEGRATE</h4>
                <p className="text-sm text-[#808080]">Accumulate changes into state</p>
              </div>
            </div>
            <div className="p-4 bg-[#0d1117] border border-[#00ffff]/20 text-center mb-4">
              <span className="text-[#00ffff]">I(</span>
              <span className="text-[#e0e0e0]">deltas</span>
              <span className="text-[#00ffff]">)[t]</span>
              <span className="text-[#808080]"> = </span>
              <span className="text-[#e0e0e0]">Σ deltas[0..t]</span>
            </div>
            <p className="text-sm text-[#808080]">
              Takes deltas → outputs <span className="text-[#e0e0e0]">running sum</span> (current state)
            </p>
          </div>
        </div>

        {/* Key theorem */}
        <div className="max-w-3xl mx-auto p-6 bg-[#0a0a0a] border border-[#39ff14]/50 glow-green">
          <div className="text-[#39ff14] text-sm mb-4 text-center">┌─ FUNDAMENTAL_THEOREM ─┐</div>
          <div className="flex items-center justify-center gap-4 text-xl mb-4 flex-wrap">
            <span className="text-[#ff00ff]">D</span>
            <span className="text-[#808080]">∘</span>
            <span className="text-[#00ffff]">I</span>
            <span className="text-[#808080]">=</span>
            <span className="text-[#e0e0e0]">identity</span>
            <span className="text-[#808080] mx-4">&&</span>
            <span className="text-[#00ffff]">I</span>
            <span className="text-[#808080]">∘</span>
            <span className="text-[#ff00ff]">D</span>
            <span className="text-[#808080]">=</span>
            <span className="text-[#e0e0e0]">identity</span>
          </div>
          <p className="text-sm text-[#808080] text-center">
            D and I are <span className="text-[#39ff14]">inverses</span>. This guarantees correctness of incremental computation.
          </p>
          <div className="text-[#39ff14]/30 text-xs mt-4 text-center">└───────────────────────┘</div>
        </div>
      </div>
    </section>
  );
}
