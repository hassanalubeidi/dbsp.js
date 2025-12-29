import { useState } from 'react';
import { SectionHeading } from '../components/SectionHeading';
import { CodeBlock } from '../components/CodeBlock';

interface CircuitNode {
  id: string;
  type: 'source' | 'filter' | 'map' | 'join' | 'groupby' | 'aggregate' | 'output';
  label: string;
  isLinear?: boolean;
}

interface CircuitEdge {
  from: string;
  to: string;
}

const examples: Record<string, { sql: string; nodes: CircuitNode[]; edges: CircuitEdge[] }> = {
  simple: {
    sql: `SELECT sensorId, temperature
FROM sensors
WHERE temperature > 35`,
    nodes: [
      { id: 'src', type: 'source', label: 'sensors' },
      { id: 'filter', type: 'filter', label: 'temp > 35', isLinear: true },
      { id: 'map', type: 'map', label: 'SELECT cols', isLinear: true },
      { id: 'out', type: 'output', label: 'Result' },
    ],
    edges: [
      { from: 'src', to: 'filter' },
      { from: 'filter', to: 'map' },
      { from: 'map', to: 'out' },
    ],
  },
  join: {
    sql: `SELECT s.sensorId, z.name
FROM sensors s
JOIN zones z ON s.zoneId = z.zoneId
WHERE s.temperature > 35`,
    nodes: [
      { id: 'src1', type: 'source', label: 'sensors' },
      { id: 'src2', type: 'source', label: 'zones' },
      { id: 'filter', type: 'filter', label: 'temp > 35', isLinear: true },
      { id: 'join', type: 'join', label: 'JOIN ON zoneId' },
      { id: 'map', type: 'map', label: 'SELECT cols', isLinear: true },
      { id: 'out', type: 'output', label: 'Result' },
    ],
    edges: [
      { from: 'src1', to: 'filter' },
      { from: 'filter', to: 'join' },
      { from: 'src2', to: 'join' },
      { from: 'join', to: 'map' },
      { from: 'map', to: 'out' },
    ],
  },
  aggregate: {
    sql: `SELECT zoneId, AVG(temperature) as avg_temp
FROM sensors
GROUP BY zoneId`,
    nodes: [
      { id: 'src', type: 'source', label: 'sensors' },
      { id: 'groupby', type: 'groupby', label: 'GROUP BY zoneId' },
      { id: 'agg', type: 'aggregate', label: 'AVG(temp)' },
      { id: 'out', type: 'output', label: 'Result' },
    ],
    edges: [
      { from: 'src', to: 'groupby' },
      { from: 'groupby', to: 'agg' },
      { from: 'agg', to: 'out' },
    ],
  },
};

const nodeColors: Record<string, { bg: string; border: string; text: string }> = {
  source: { bg: 'bg-[#00ffff]/10', border: 'border-[#00ffff]/50', text: 'text-[#00ffff]' },
  filter: { bg: 'bg-[#39ff14]/10', border: 'border-[#39ff14]/50', text: 'text-[#39ff14]' },
  map: { bg: 'bg-[#39ff14]/10', border: 'border-[#39ff14]/50', text: 'text-[#39ff14]' },
  join: { bg: 'bg-[#ffb000]/10', border: 'border-[#ffb000]/50', text: 'text-[#ffb000]' },
  groupby: { bg: 'bg-[#ff00ff]/10', border: 'border-[#ff00ff]/50', text: 'text-[#ff00ff]' },
  aggregate: { bg: 'bg-[#ff00ff]/10', border: 'border-[#ff00ff]/50', text: 'text-[#ff00ff]' },
  output: { bg: 'bg-[#39ff14]/10', border: 'border-[#39ff14]', text: 'text-[#39ff14]' },
};

export function SQLCompiler() {
  const [selectedExample, setSelectedExample] = useState<'simple' | 'join' | 'aggregate'>('simple');
  const example = examples[selectedExample];

  return (
    <section id="sql-compiler" className="py-20 bg-[#0a0a0a]">
      <div className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8 font-mono">
        <SectionHeading
          id="sql-heading"
          badge="Under the Hood"
          title="The SQL Compiler"
          subtitle="How SQL queries become DBSP circuits"
        />

        {/* Example selector */}
        <div className="flex flex-wrap items-center justify-center gap-4 mb-12">
          {Object.entries({ simple: 'SIMPLE_FILTER', join: 'JOIN_QUERY', aggregate: 'GROUP_BY' }).map(([key, label]) => (
            <button
              key={key}
              onClick={() => setSelectedExample(key as 'simple' | 'join' | 'aggregate')}
              className={`px-4 py-2 font-bold transition-all ${
                selectedExample === key
                  ? 'bg-[#39ff14] text-[#0a0a0a]'
                  : 'border border-[#39ff14]/30 text-[#39ff14] hover:bg-[#39ff14]/10'
              }`}
            >
              [{label}]
            </button>
          ))}
        </div>

        {/* SQL to Circuit */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 mb-12">
          {/* SQL */}
          <div className="p-6 bg-[#0d1117] border border-[#00ffff]/30">
            <div className="flex items-center justify-between mb-4">
              <span className="text-[#00ffff]">┌─ SQL_QUERY ─┐</span>
              <span className="text-xs text-[#808080]">INPUT</span>
            </div>
            <CodeBlock code={example.sql} language="sql" />
            <div className="text-[#00ffff]/30 text-xs mt-4">└─────────────┘</div>
          </div>

          {/* Circuit */}
          <div className="p-6 bg-[#0d1117] border border-[#39ff14]/30">
            <div className="flex items-center justify-between mb-4">
              <span className="text-[#39ff14]">┌─ DBSP_CIRCUIT ─┐</span>
              <span className="text-xs text-[#808080]">OUTPUT</span>
            </div>
            <div className="flex flex-wrap gap-3 items-center justify-center">
              {example.nodes.map((node, i) => {
                const colors = nodeColors[node.type];
                return (
                  <div key={node.id} className="flex items-center gap-2">
                    <div className={`px-4 py-2 ${colors.bg} border ${colors.border} ${colors.text}`}>
                      <div className="text-xs opacity-60">{node.type.toUpperCase()}</div>
                      <div className="font-bold">{node.label}</div>
                      {node.isLinear && (
                        <div className="text-xs text-[#39ff14]">[LINEAR]</div>
                      )}
                    </div>
                    {i < example.nodes.length - 1 && (
                      <span className="text-[#39ff14]">→</span>
                    )}
                  </div>
                );
              })}
            </div>
            <div className="text-[#39ff14]/30 text-xs mt-4">└─────────────────┘</div>
          </div>
        </div>

        {/* Key insight */}
        <div className="max-w-3xl mx-auto p-6 bg-[#0d1117] border border-[#ffb000]/30">
          <div className="text-[#ffb000] mb-4">┌─ COMPILER_OPTIMIZATIONS ─┐</div>
          <ul className="space-y-2 text-sm text-[#808080]">
            <li className="flex items-start gap-2">
              <span className="text-[#39ff14]">[1]</span>
              <span><span className="text-[#e0e0e0]">Linear operators</span> (filter, map) applied directly to deltas</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-[#39ff14]">[2]</span>
              <span><span className="text-[#e0e0e0]">Bilinear operators</span> (join, aggregate) use incremental formulas</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-[#39ff14]">[3]</span>
              <span><span className="text-[#e0e0e0]">Hash indexes</span> maintained automatically for join keys</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-[#39ff14]">[4]</span>
              <span><span className="text-[#e0e0e0]">Operator fusion</span> where possible to reduce intermediate allocations</span>
            </li>
          </ul>
          <div className="text-[#ffb000]/30 text-xs mt-4">└──────────────────────────┘</div>
        </div>
      </div>
    </section>
  );
}
