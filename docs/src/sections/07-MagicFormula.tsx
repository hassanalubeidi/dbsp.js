import { SectionHeading } from '../components/SectionHeading';
import { CodeBlock } from '../components/CodeBlock';

const linearOperatorCode = `// LINEAR operators work directly on deltas!
// No need to track previous state.

function filter(predicate: (row) => boolean) {
  return (delta: ZSet) => {
    // Just filter the delta, not the whole dataset
    return delta.filter(predicate);
  };
}

function map(transform: (row) => newRow) {
  return (delta: ZSet) => {
    // Just map the delta
    return delta.map(transform);
  };
}

// Both are O(Δ) - only touch the changed rows!`;

export function MagicFormula() {
  return (
    <section id="magic-formula" className="py-20 bg-[#0a0a0a]">
      <div className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8 font-mono">
        <SectionHeading
          id="magic-heading"
          badge="The Magic"
          title="The Incremental View Maintenance Formula"
          subtitle="How any query can be made incremental"
        />

        {/* Main formula */}
        <div className="max-w-4xl mx-auto mb-16">
          <div className="p-8 bg-[#0d1117] border border-[#39ff14]/50 glow-green">
            <div className="text-center mb-6">
              <span className="text-sm text-[#808080]">$ For any query Q, its incremental version is:</span>
            </div>
            <div className="flex items-center justify-center gap-3 text-2xl md:text-4xl mb-6 flex-wrap">
              <span className="text-[#39ff14] text-glow-green">Q<sup>Δ</sup></span>
              <span className="text-[#808080]">=</span>
              <span className="text-[#ff00ff]">D</span>
              <span className="text-[#808080]">∘</span>
              <span className="text-[#e0e0e0]">Q</span>
              <span className="text-[#808080]">∘</span>
              <span className="text-[#00ffff]">I</span>
            </div>
            <div className="text-center text-[#808080] text-sm">
              # Differentiate the output of Q applied to the integrated input
            </div>
          </div>
        </div>

        {/* Visual pipeline */}
        <div className="max-w-4xl mx-auto mb-16">
          <div className="text-[#39ff14] text-center mb-8">═══ THE_INCREMENTAL_PIPELINE ═══</div>
          <div className="flex flex-wrap items-center justify-center gap-4">
            <div className="p-4 bg-[#0d1117] border border-[#39ff14]/50">
              <div className="text-xs text-[#39ff14] mb-1">INPUT</div>
              <div className="font-bold text-[#e0e0e0]">Δ input</div>
            </div>

            <span className="text-[#39ff14]">→</span>

            <div className="p-4 bg-[#0d1117] border border-[#00ffff]/50">
              <div className="text-xs text-[#00ffff] mb-1">OP</div>
              <div className="font-bold text-[#00ffff]">I</div>
            </div>

            <span className="text-[#39ff14]">→</span>

            <div className="p-4 bg-[#0d1117] border border-[#e0e0e0]/30">
              <div className="text-xs text-[#808080] mb-1">QUERY</div>
              <div className="font-bold text-[#e0e0e0]">Q</div>
            </div>

            <span className="text-[#39ff14]">→</span>

            <div className="p-4 bg-[#0d1117] border border-[#ff00ff]/50">
              <div className="text-xs text-[#ff00ff] mb-1">OP</div>
              <div className="font-bold text-[#ff00ff]">D</div>
            </div>

            <span className="text-[#39ff14]">→</span>

            <div className="p-4 bg-[#0d1117] border border-[#39ff14]/50">
              <div className="text-xs text-[#39ff14] mb-1">OUTPUT</div>
              <div className="font-bold text-[#e0e0e0]">Δ output</div>
            </div>
          </div>
        </div>

        {/* Linear operators */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 mb-12">
          <div className="space-y-6">
            <div className="text-[#39ff14] text-lg">═══ LINEAR_OPERATORS_ARE_FREE ═══</div>
            <p className="text-[#808080]">
              Some operators are <span className="text-[#39ff14]">linear</span>, meaning they distribute 
              over Z-set addition:
            </p>
            <div className="p-4 bg-[#0d1117] border border-[#39ff14]/30 text-center">
              <span className="text-[#39ff14]">Q<sub>linear</sub></span>
              <span className="text-[#808080]">(a + b) = </span>
              <span className="text-[#39ff14]">Q<sub>linear</sub></span>
              <span className="text-[#808080]">(a) + </span>
              <span className="text-[#39ff14]">Q<sub>linear</sub></span>
              <span className="text-[#808080]">(b)</span>
            </div>
            <p className="text-[#808080]">
              Apply them <span className="text-[#e0e0e0]">directly to deltas</span> - no I or D needed:
            </p>
            <div className="p-4 bg-[#0d1117] border border-[#39ff14]/50 text-center glow-green">
              <span className="text-[#39ff14]">Q<sub>linear</sub><sup>Δ</sup></span>
              <span className="text-[#808080]"> = </span>
              <span className="text-[#39ff14]">Q<sub>linear</sub></span>
            </div>
          </div>

          <CodeBlock code={linearOperatorCode} language="typescript" filename="linear-operators.ts" />
        </div>

        {/* Operator classification */}
        <div className="max-w-4xl mx-auto">
          <div className="text-[#39ff14] text-center mb-8">═══ OPERATOR_CLASSIFICATIONS ═══</div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            {/* Linear operators */}
            <div className="p-6 bg-[#0a0a0a] border border-[#39ff14]/50">
              <div className="text-[#39ff14] mb-4 flex items-center gap-2">
                <span className="px-2 py-0.5 bg-[#39ff14] text-[#0a0a0a] text-xs font-bold">✓</span>
                LINEAR (O(Δ))
              </div>
              <ul className="space-y-2 text-sm">
                <li className="flex items-center gap-2 text-[#808080]">
                  <span className="text-[#39ff14]">filter</span>
                  <span>— WHERE clause</span>
                </li>
                <li className="flex items-center gap-2 text-[#808080]">
                  <span className="text-[#39ff14]">map</span>
                  <span>— SELECT expressions</span>
                </li>
                <li className="flex items-center gap-2 text-[#808080]">
                  <span className="text-[#39ff14]">flatMap</span>
                  <span>— UNNEST / LATERAL</span>
                </li>
                <li className="flex items-center gap-2 text-[#808080]">
                  <span className="text-[#39ff14]">union</span>
                  <span>— UNION ALL</span>
                </li>
              </ul>
            </div>

            {/* Bilinear operators */}
            <div className="p-6 bg-[#0a0a0a] border border-[#ffb000]/50">
              <div className="text-[#ffb000] mb-4 flex items-center gap-2">
                <span className="px-2 py-0.5 bg-[#ffb000] text-[#0a0a0a] text-xs font-bold">≈</span>
                BILINEAR (Needs State)
              </div>
              <ul className="space-y-2 text-sm">
                <li className="flex items-center gap-2 text-[#808080]">
                  <span className="text-[#ffb000]">join</span>
                  <span>— JOIN ... ON</span>
                </li>
                <li className="flex items-center gap-2 text-[#808080]">
                  <span className="text-[#ffb000]">aggregate</span>
                  <span>— GROUP BY + SUM/AVG</span>
                </li>
                <li className="flex items-center gap-2 text-[#808080]">
                  <span className="text-[#ffb000]">distinct</span>
                  <span>— SELECT DISTINCT</span>
                </li>
                <li className="flex items-center gap-2 text-[#808080]">
                  <span className="text-[#ffb000]">window</span>
                  <span>— Window functions</span>
                </li>
              </ul>
            </div>
          </div>
          
          <p className="mt-6 text-center text-sm text-[#808080]">
            # Bilinear operators maintain state but still process <span className="text-[#39ff14]">only deltas</span>
          </p>
        </div>
      </div>
    </section>
  );
}
