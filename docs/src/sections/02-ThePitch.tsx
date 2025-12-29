import { useState, useEffect } from 'react';

export function ThePitch() {
  const [eventCount, setEventCount] = useState(0);
  const [isStreaming, setIsStreaming] = useState(true);

  useEffect(() => {
    if (!isStreaming) return;
    const interval = setInterval(() => {
      setEventCount(prev => prev + 1);
    }, 100);
    return () => clearInterval(interval);
  }, [isStreaming]);

  return (
    <section id="the-pitch" className="relative py-20 bg-[#0a0a0a] overflow-hidden">
      {/* Grid background */}
      <div className="absolute inset-0 opacity-5">
        <div className="absolute inset-0" style={{
          backgroundImage: `
            linear-gradient(rgba(57, 255, 20, 0.3) 1px, transparent 1px),
            linear-gradient(90deg, rgba(57, 255, 20, 0.3) 1px, transparent 1px)
          `,
          backgroundSize: '40px 40px',
        }} />
      </div>
      <div className="relative z-10 max-w-5xl mx-auto px-4 sm:px-6 lg:px-8 font-mono">
        
        {/* The Vision */}
        <div className="text-center mb-16">
          <div className="text-[#00ffff] text-sm mb-4 tracking-widest">THE VISION</div>
          <h2 className="text-3xl md:text-4xl font-bold text-[#e0e0e0] mb-6">
            What if <span className="text-[#00ffff]">Apache Flink</span> ran in your{' '}
            <span className="text-[#39ff14]">browser</span>?
          </h2>
          <p className="text-[#808080] text-lg max-w-2xl mx-auto mb-8">
            Real-time streaming analytics. Live aggregations. SQL queries. 
            <br />
            <span className="text-[#39ff14]">Zero backend. Zero latency. Zero cost.</span>
          </p>
          
          {/* Mini live demo */}
          <div className="inline-flex items-center gap-4 px-6 py-3 bg-[#0d1117] border border-[#39ff14]/50">
            <div className={`w-3 h-3 rounded-full ${isStreaming ? 'bg-[#39ff14] animate-pulse' : 'bg-[#808080]'}`} />
            <span className="text-[#e0e0e0]">
              Events processed: <span className="text-[#39ff14] font-bold tabular-nums">{eventCount.toLocaleString()}</span>
            </span>
            <span className="text-[#808080]">|</span>
            <span className="text-[#808080] text-sm">← This is running in your browser right now</span>
          </div>
        </div>

        {/* Why client-side? */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-16">
          <div className="p-5 bg-[#0d1117] border border-[#39ff14]/30">
            <div className="text-3xl mb-3">⚡</div>
            <div className="text-[#39ff14] font-bold text-lg mb-2">Microsecond latency</div>
            <div className="text-[#808080] text-sm">
              Server round-trip: ~100ms
              <br />
              Browser compute: <span className="text-[#39ff14]">~50µs</span>
            </div>
          </div>
          <div className="p-5 bg-[#0d1117] border border-[#00ffff]/30">
            <div className="text-3xl mb-3">🔒</div>
            <div className="text-[#00ffff] font-bold text-lg mb-2">Private by default</div>
            <div className="text-[#808080] text-sm">
              Data never leaves the device.
              <br />
              <span className="text-[#00ffff]">GDPR compliance is trivial.</span>
            </div>
          </div>
          <div className="p-5 bg-[#0d1117] border border-[#ffb000]/30">
            <div className="text-3xl mb-3">📴</div>
            <div className="text-[#ffb000] font-bold text-lg mb-2">Works offline</div>
            <div className="text-[#808080] text-sm">
              In a field. On a plane. In a tunnel.
              <br />
              <span className="text-[#ffb000]">Zero network dependency.</span>
            </div>
          </div>
          <div className="p-5 bg-[#0d1117] border border-[#ff00ff]/30">
            <div className="text-3xl mb-3">💰</div>
            <div className="text-[#ff00ff] font-bold text-lg mb-2">$0 infrastructure</div>
            <div className="text-[#808080] text-sm">
              Each user is their own compute cluster.
              <br />
              <span className="text-[#ff00ff]">Scale to millions, pay nothing.</span>
            </div>
          </div>
        </div>

        {/* The catch */}
        <div className="max-w-3xl mx-auto text-center mb-12">
          <div className="text-[#ff3333] text-sm mb-4">THE CATCH</div>
          <h3 className="text-2xl font-bold text-[#e0e0e0] mb-4">
            But browsers are <span className="text-[#ff3333]">single-threaded</span>...
          </h3>
          <p className="text-[#808080] mb-6">
            Naive queries scan your entire dataset on every update. 
            With 100K rows, that freezes your UI at 60fps.
          </p>
          
          {/* O(N) vs O(Δ) visual */}
          <div className="grid grid-cols-2 gap-4 max-w-lg mx-auto">
            <div className="p-4 bg-[#ff3333]/10 border border-[#ff3333]/30">
              <div className="text-[#ff3333] font-bold mb-2">❌ Naive</div>
              <div className="text-2xl font-bold text-[#ff3333]">O(N)</div>
              <div className="text-xs text-[#808080]">scan everything</div>
            </div>
            <div className="p-4 bg-[#39ff14]/10 border border-[#39ff14]/30 glow-green">
              <div className="text-[#39ff14] font-bold mb-2">✓ DBSP</div>
              <div className="text-2xl font-bold text-[#39ff14]">O(Δ)</div>
              <div className="text-xs text-[#808080]">just the change</div>
            </div>
          </div>
        </div>

        {/* The solution - one liner */}
        <div className="max-w-3xl mx-auto p-6 bg-[#0d1117] border border-[#39ff14] glow-green text-center">
          <div className="text-[#39ff14] text-sm mb-2">THE SOLUTION</div>
          <div className="text-xl text-[#e0e0e0] mb-4">
            <span className="text-[#39ff14] font-bold">Incremental View Maintenance (IVM)</span>
          </div>
          <p className="text-[#808080] text-sm mb-4">
            Instead of re-running queries from scratch, DBSP remembers previous results 
            and only computes what changed. Same technique used by Flink, Materialize, and RisingWave.
          </p>
          <div className="text-[#39ff14] font-bold">
            result<sub>new</sub> = result<sub>old</sub> + query(Δ)
          </div>
        </div>

        {/* CTA */}
        <div className="text-center mt-12">
          <div className="text-[#808080] mb-4">Ready to try it?</div>
          <a
            href="#quickstart"
            className="inline-block px-8 py-3 bg-[#39ff14] text-[#0a0a0a] font-bold hover:bg-[#50ff30] transition-all glow-green text-lg"
          >
            ▶ GET STARTED IN 30 SECONDS
          </a>
        </div>
      </div>
    </section>
  );
}

