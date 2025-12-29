import { useState } from 'react';
import { SectionHeading } from '../components/SectionHeading';
import { CodeBlock } from '../components/CodeBlock';

interface Row {
  id: string;
  value: number;
}

export function DeepDive() {
  const [rows, setRows] = useState<Row[]>([
    { id: 'row-1', value: 10 },
    { id: 'row-2', value: 25 },
    { id: 'row-3', value: 40 },
  ]);
  const [changes, setChanges] = useState<{ type: 'insert' | 'delete'; row: Row }[]>([]);
  const [runningSum, setRunningSum] = useState(75);

  const addRow = () => {
    const newRow = { id: `row-${rows.length + 1}`, value: Math.floor(Math.random() * 50) + 10 };
    setRows([...rows, newRow]);
    setRunningSum(prev => prev + newRow.value);
    setChanges(prev => [...prev.slice(-4), { type: 'insert', row: newRow }]);
  };

  const removeRow = (row: Row) => {
    setRows(rows.filter(r => r.id !== row.id));
    setRunningSum(prev => prev - row.value);
    setChanges(prev => [...prev.slice(-4), { type: 'delete', row }]);
  };

  const traditionalSum = rows.reduce((acc, r) => acc + r.value, 0);

  const joinCode = `// How DBSP handles JOINs incrementally
// When a new sensor reading arrives:

function onNewSensorReading(reading) {
  // 1. Index lookup - O(1)
  const zone = zoneIndex.get(reading.zoneId);
  
  // 2. If it matches, emit the joined result
  if (zone) {
    emit({ ...reading, zoneName: zone.name });
  }
  
  // 3. Store for future zone additions
  sensorIndex.set(reading.id, reading);
}

// We NEVER re-scan the entire sensors table!`;

  return (
    <section id="deep-dive" className="relative py-20 bg-[#0a0a0a] overflow-hidden">
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
      <div className="relative z-10 max-w-6xl mx-auto px-4 sm:px-6 lg:px-8 font-mono">
        <SectionHeading
          id="deep-dive-heading"
          badge="Under The Hood"
          title="How DBSP Works"
          subtitle="The clever trick that makes everything fast (no PhD required)"
        />

        {/* The Core Idea */}
        <div className="max-w-4xl mx-auto mb-16">
          <div className="p-6 bg-[#0d1117] border border-[#39ff14]/50 glow-green">
            <div className="text-[#39ff14] text-lg mb-4">💡 THE CORE IDEA</div>
            <p className="text-[#e0e0e0] text-lg mb-4">
              Instead of re-running your query on the entire dataset every time something changes,
              DBSP <span className="text-[#39ff14] font-bold">remembers the previous result</span> and 
              <span className="text-[#39ff14] font-bold"> only calculates what changed</span>.
            </p>
            <div className="p-4 bg-[#0a0a0a] border border-[#808080]/30">
              <div className="text-[#808080] text-sm mb-2">Think of it like:</div>
              <div className="text-[#e0e0e0]">
                📊 You have a spreadsheet with 100,000 rows and a SUM formula.
                <br />
                ❌ <span className="text-[#ff3333]">Traditional:</span> Excel recalculates by scanning all 100,000 rows every keystroke.
                <br />
                ✅ <span className="text-[#39ff14]">DBSP:</span> When you change one cell, it just adds/subtracts the difference.
              </div>
            </div>
          </div>
        </div>

        {/* Interactive Demo */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 mb-16">
          {/* Live example */}
          <div className="space-y-4">
            <div className="text-[#39ff14] text-lg">═══ TRY IT: SUM(value) ═══</div>
            
            <div className="p-4 bg-[#0d1117] border border-[#39ff14]/30">
              <div className="text-[#808080] text-sm mb-3">Your Data:</div>
              <div className="space-y-2">
                {rows.map(row => (
                  <div key={row.id} className="flex items-center justify-between p-2 bg-[#0a0a0a] border border-[#808080]/20 group">
                    <span className="text-[#e0e0e0]">{row.id}: <span className="text-[#00ffff]">{row.value}</span></span>
                    <button
                      onClick={() => removeRow(row)}
                      className="text-[#ff3333] text-xs opacity-0 group-hover:opacity-100 border border-[#ff3333]/50 px-2 py-1 hover:bg-[#ff3333]/10"
                    >
                      DELETE
                    </button>
                  </div>
                ))}
              </div>
              <button
                onClick={addRow}
                className="w-full mt-3 px-4 py-2 bg-[#39ff14] text-[#0a0a0a] font-bold hover:bg-[#50ff30]"
              >
                + ADD ROW
              </button>
            </div>

            {/* Results comparison */}
            <div className="grid grid-cols-2 gap-4">
              <div className="p-4 bg-[#ff3333]/10 border border-[#ff3333]/30">
                <div className="text-[#ff3333] text-xs mb-1">Traditional O(N)</div>
                <div className="text-2xl font-bold text-[#ff3333]">{traditionalSum}</div>
                <div className="text-xs text-[#808080] mt-1">Scans {rows.length} rows</div>
              </div>
              <div className="p-4 bg-[#39ff14]/10 border border-[#39ff14]/30 glow-green">
                <div className="text-[#39ff14] text-xs mb-1">DBSP O(1)</div>
                <div className="text-2xl font-bold text-[#39ff14]">{runningSum}</div>
                <div className="text-xs text-[#808080] mt-1">Just 1 operation</div>
              </div>
            </div>

            {/* Change log */}
            <div className="p-4 bg-[#0d1117] border border-[#00ffff]/30">
              <div className="text-[#00ffff] text-sm mb-2">Change Log:</div>
              {changes.length === 0 ? (
                <div className="text-[#808080] text-sm">Click buttons to see changes...</div>
              ) : (
                <div className="space-y-1">
                  {changes.map((c, i) => (
                    <div key={i} className={`text-sm ${c.type === 'insert' ? 'text-[#39ff14]' : 'text-[#ff3333]'}`}>
                      {c.type === 'insert' ? '+' : '-'} {c.row.id}: {c.row.value}
                      <span className="text-[#808080]"> → sum {c.type === 'insert' ? '+' : '-'}= {c.row.value}</span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>

          {/* Code comparison */}
          <div className="space-y-4">
            <div className="text-[#ff3333] text-lg">═══ THE CODE ═══</div>
            <CodeBlock 
              code={`// ❌ Traditional: Re-scan everything
function getSum(allRows) {
  let sum = 0;
  for (const row of allRows) {  // O(N)
    sum += row.value;
  }
  return sum;
}

// With 100K rows = 100K operations per update!`}
              language="typescript" 
              filename="traditional.ts" 
            />
            <CodeBlock 
              code={`// ✅ DBSP: Only process the change
let runningSum = 75;  // Cached result

function onRowInserted(newRow) {
  runningSum += newRow.value;  // O(1)!
}

function onRowDeleted(oldRow) {
  runningSum -= oldRow.value;  // O(1)!
}

// With 100K rows = still just 1 operation!`}
              language="typescript" 
              filename="dbsp.ts" 
            />
          </div>
        </div>

        {/* What about complex queries? */}
        <div className="max-w-4xl mx-auto mb-16">
          <div className="text-[#00ffff] text-lg mb-6 text-center">═══ COMPLEX QUERIES ═══</div>
          
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
            <div className="p-4 bg-[#0d1117] border border-[#39ff14]/30">
              <div className="text-[#39ff14] text-lg mb-2">WHERE</div>
              <p className="text-[#808080] text-sm">
                New row arrives → check if it passes filter → done!
              </p>
              <div className="text-[#39ff14] text-sm mt-2">O(1)</div>
            </div>
            <div className="p-4 bg-[#0d1117] border border-[#39ff14]/30">
              <div className="text-[#39ff14] text-lg mb-2">GROUP BY</div>
              <p className="text-[#808080] text-sm">
                Keep running total per group. Update just that group.
              </p>
              <div className="text-[#39ff14] text-sm mt-2">O(1)</div>
            </div>
            <div className="p-4 bg-[#0d1117] border border-[#ffb000]/30">
              <div className="text-[#ffb000] text-lg mb-2">JOIN</div>
              <p className="text-[#808080] text-sm">
                Index both tables. New row → probe the other index.
              </p>
              <div className="text-[#ffb000] text-sm mt-2">O(matches)</div>
            </div>
          </div>
        </div>

        {/* JOIN explanation */}
        <div className="max-w-4xl mx-auto mb-16">
          <div className="text-[#ffb000] text-lg mb-6">═══ HOW JOINS WORK ═══</div>
          
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
            <div className="space-y-4">
              <p className="text-[#e0e0e0]">
                JOINs are the trickiest part. Traditional databases re-match everything.
                DBSP uses <span className="text-[#39ff14]">hash indexes</span>:
              </p>
              
              <div className="p-4 bg-[#0d1117] border border-[#808080]/30">
                <div className="text-[#e0e0e0] mb-3">When a new sensor reading comes in:</div>
                <ol className="space-y-2 text-sm text-[#808080]">
                  <li>1. <span className="text-[#39ff14]">Index lookup</span> - Find matching zone (O(1))</li>
                  <li>2. <span className="text-[#39ff14]">Emit result</span> - Only if there's a match</li>
                  <li>3. <span className="text-[#39ff14]">Store for later</span> - In case a zone is added</li>
                </ol>
              </div>
              
              <div className="p-4 bg-[#0d1117] border border-[#39ff14]/50 glow-green">
                <div className="text-[#39ff14] text-sm mb-2">💡 Key Insight:</div>
                <p className="text-[#e0e0e0] text-sm">
                  We maintain indexes on both sides. When data arrives on 
                  <span className="text-[#00ffff]"> either side</span>, we probe the 
                  <span className="text-[#ff00ff]"> other side's index</span>.
                  This is how real databases work — but incrementally!
                </p>
              </div>
            </div>
            
            <CodeBlock code={joinCode} language="typescript" filename="incremental-join.ts" />
          </div>
        </div>

        {/* Supported operators */}
        <div className="max-w-4xl mx-auto">
          <div className="text-[#39ff14] text-lg mb-6 text-center">═══ SUPPORTED SQL ═══</div>
          
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            {[
              { name: 'SELECT', desc: 'Pick columns' },
              { name: 'WHERE', desc: 'Filter rows' },
              { name: 'JOIN', desc: 'Combine tables' },
              { name: 'GROUP BY', desc: 'Aggregate' },
              { name: 'ORDER BY', desc: 'Sort results' },
              { name: 'DISTINCT', desc: 'Remove dupes' },
              { name: 'LIMIT', desc: 'Paginate' },
              { name: 'Subqueries', desc: 'Nested queries' },
            ].map(op => (
              <div key={op.name} className="p-3 bg-[#0d1117] border border-[#39ff14]/30 text-center">
                <div className="text-[#39ff14] font-bold">{op.name}</div>
                <div className="text-[#808080] text-xs">{op.desc}</div>
              </div>
            ))}
          </div>
          
          <p className="text-center text-[#808080] text-sm mt-6">
            All standard SQL operations work incrementally. Write SQL, get real-time updates.
          </p>
        </div>
      </div>
    </section>
  );
}

