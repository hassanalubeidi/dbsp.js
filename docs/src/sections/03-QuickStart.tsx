import { useState, useCallback } from 'react';
import { CodeBlock } from '../components/CodeBlock';
import { SectionHeading } from '../components/SectionHeading';

interface SensorReading {
  sensorId: string;
  location: string;
  temperature: number;
  humidity: number;
}

// Simulated hooks for the interactive demo (mirrors real API)
function useDemoDBSP() {
  const [data, setData] = useState<SensorReading[]>([]);
  const [stats, setStats] = useState({ totalReadings: 0, lastUpdateMs: 0 });

  const push = useCallback((reading: SensorReading) => {
    const start = performance.now();
    setData(prev => [...prev, reading].slice(-20));
    setStats(prev => ({
      totalReadings: prev.totalReadings + 1,
      lastUpdateMs: performance.now() - start,
    }));
  }, []);

  const clear = useCallback(() => {
    setData([]);
    setStats({ totalReadings: 0, lastUpdateMs: 0 });
  }, []);

  // Compute alerts (simulates useDBSPView)
  const alerts = data.filter(r => r.temperature > 35);

  return { data, alerts, stats, push, clear };
}

const installCode = `npm install @dbsp/react`;

const simpleExample = `import { useDBSPSource, useDBSPView } from '@dbsp/react';

function Dashboard() {
  // 1. Create a data source (like a database table)
  const sensors = useDBSPSource<SensorReading>({
    name: 'sensors',
    key: 'sensorId',
  });

  // 2. Query with SQL — view updates automatically!
  const alerts = useDBSPView(sensors,
    "SELECT * FROM sensors WHERE temperature > 35"
  );

  // 3. Push data from any source (WebSocket, API, user input)
  const addReading = () => {
    sensors.push({
      sensorId: \`sensor-\${Date.now()}\`,
      location: 'Field A',
      temperature: 30 + Math.random() * 15,
      humidity: 50 + Math.random() * 30,
    });
  };

  return (
    <div>
      <button onClick={addReading}>Add Reading</button>
      <p>Total: {sensors.count}</p>
      <p>Alerts: {alerts.count}</p>
      <ul>
        {alerts.data.map(a => (
          <li key={a.sensorId}>{a.temperature}°C</li>
        ))}
      </ul>
    </div>
  );
}`;

const locations = ['Field A', 'Field B', 'Greenhouse', 'Storage', 'Office'];

export function QuickStart() {
  const { alerts, stats, push, clear } = useDemoDBSP();

  const addReading = () => {
    push({
      sensorId: `sensor-${Math.random().toString(36).slice(2, 6)}`,
      location: locations[Math.floor(Math.random() * locations.length)],
      temperature: Math.round((25 + Math.random() * 20) * 10) / 10,
      humidity: Math.round((40 + Math.random() * 40) * 10) / 10,
    });
  };

  const addBurst = () => {
    for (let i = 0; i < 10; i++) {
      setTimeout(() => addReading(), i * 50);
    }
  };

  return (
    <section id="quickstart" className="relative py-20 bg-[#0a0a0a] overflow-hidden">
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
      <div className="relative z-10 max-w-6xl mx-auto px-4 sm:px-6 lg:px-8">
        <SectionHeading
          id="quickstart-heading"
          badge="Quick Start"
          title="Get started in 30 seconds"
          subtitle="Install, create a source, write SQL. That's it."
        />

        {/* Step 1: Install */}
        <div className="max-w-4xl mx-auto mb-12">
          <div className="flex items-center gap-3 mb-4">
            <span className="flex items-center justify-center w-8 h-8 rounded-full bg-[#39ff14] text-[#0a0a0a] font-bold text-sm">1</span>
            <span className="text-[#e0e0e0] font-mono text-lg">Install the package</span>
          </div>
          <CodeBlock code={installCode} language="bash" filename="terminal" />
        </div>

        {/* Step 2: Code */}
        <div className="max-w-4xl mx-auto mb-12">
          <div className="flex items-center gap-3 mb-4">
            <span className="flex items-center justify-center w-8 h-8 rounded-full bg-[#39ff14] text-[#0a0a0a] font-bold text-sm">2</span>
            <span className="text-[#e0e0e0] font-mono text-lg">Write your first real-time view</span>
          </div>
          <CodeBlock
            code={simpleExample}
            language="tsx"
            filename="Dashboard.tsx"
            showLineNumbers
          />
        </div>

        {/* Step 3: Try it */}
        <div className="max-w-4xl mx-auto">
          <div className="flex items-center gap-3 mb-4">
            <span className="flex items-center justify-center w-8 h-8 rounded-full bg-[#39ff14] text-[#0a0a0a] font-bold text-sm">3</span>
            <span className="text-[#e0e0e0] font-mono text-lg">Try it live (interactive demo below)</span>
          </div>
          
          <div className="p-6 bg-[#0d1117] border border-[#39ff14]/30 font-mono">
            {/* Controls */}
            <div className="flex flex-wrap gap-3 mb-6">
              <button
                onClick={addReading}
                className="px-4 py-2 bg-[#39ff14] text-[#0a0a0a] font-bold hover:bg-[#50ff30] transition-colors"
              >
                sensors.push(reading)
              </button>
              <button
                onClick={addBurst}
                className="px-4 py-2 border border-[#39ff14]/50 text-[#39ff14] font-bold hover:bg-[#39ff14]/10 transition-colors"
              >
                push × 10
              </button>
              <button
                onClick={clear}
                className="px-4 py-2 border border-[#ff3333]/50 text-[#ff3333] font-bold hover:bg-[#ff3333]/10 transition-colors"
              >
                clear()
              </button>
            </div>

            {/* Stats */}
            <div className="grid grid-cols-3 gap-4 mb-6">
              <div className="p-4 bg-[#0a0a0a] border border-[#39ff14]/30">
                <div className="text-xs text-[#808080] mb-1">sensors.count</div>
                <div className="text-2xl font-bold text-[#39ff14] tabular-nums">{stats.totalReadings}</div>
              </div>
              <div className="p-4 bg-[#0a0a0a] border border-[#ffb000]/30">
                <div className="text-xs text-[#808080] mb-1">alerts.count</div>
                <div className="text-2xl font-bold text-[#ffb000] tabular-nums">{alerts.length}</div>
              </div>
              <div className="p-4 bg-[#0a0a0a] border border-[#00ffff]/30">
                <div className="text-xs text-[#808080] mb-1">lastUpdate</div>
                <div className="text-2xl font-bold text-[#00ffff] tabular-nums">{stats.lastUpdateMs.toFixed(2)}ms</div>
              </div>
            </div>

            {/* Alerts list */}
            <div className="p-4 bg-[#0a0a0a] border border-[#ffb000]/30 max-h-48 overflow-y-auto">
              <div className="text-sm text-[#808080] mb-3">
                alerts.data <span className="text-[#ffb000]">// WHERE temperature &gt; 35</span>
              </div>
              
              {alerts.length === 0 ? (
                <div className="text-center py-6 text-[#808080]">
                  No alerts yet. Add some readings with temp &gt; 35°C
                </div>
              ) : (
                <div className="space-y-2">
                  {alerts.map((reading, i) => (
                    <div
                      key={i}
                      className="flex items-center justify-between p-2 bg-[#ffb000]/10 border border-[#ffb000]/20"
                    >
                      <span className="text-[#e0e0e0]">{reading.sensorId}</span>
                      <span className="text-[#ffb000] font-bold">{reading.temperature.toFixed(1)}°C</span>
                    </div>
                  ))}
                </div>
              )}
            </div>

            {/* Explanation */}
            <div className="mt-6 p-4 bg-[#39ff14]/10 border border-[#39ff14]/30 text-sm">
              <div className="text-[#39ff14] font-bold mb-2">💡 What's happening:</div>
              <ul className="text-[#e0e0e0] space-y-1">
                <li>• Each <code className="text-[#39ff14]">push()</code> adds a row to the source</li>
                <li>• The SQL view <code className="text-[#ffb000]">WHERE temperature &gt; 35</code> updates automatically</li>
                <li>• DBSP only checks the <span className="text-[#39ff14]">new row</span>, not the entire dataset</li>
                <li>• That's why updates take <span className="text-[#00ffff]">&lt;1ms</span> even with thousands of rows</li>
              </ul>
            </div>
          </div>
        </div>

        {/* Next steps */}
        <div className="max-w-4xl mx-auto mt-12 text-center">
          <div className="text-[#808080] mb-4">Want to see something more impressive?</div>
          <a
            href="#farm-dashboard"
            className="inline-block px-6 py-3 border-2 border-[#39ff14] text-[#39ff14] font-bold hover:bg-[#39ff14]/10 transition-all font-mono"
          >
            🌾 See the Farm Dashboard with JOINs & GROUP BY →
          </a>
        </div>
      </div>
    </section>
  );
}

