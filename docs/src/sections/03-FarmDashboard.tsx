import { useState, useEffect, useCallback, useMemo } from 'react';
import { SectionHeading } from '../components/SectionHeading';
import { CodeBlock } from '../components/CodeBlock';

// ASCII Art
const FARM_ASCII = `
    \\|/     _____
   --*--   /|   |\\    🌾🌾🌾
    /|\\   /_|___|_\\   🌽🌽🌽
          |  ___  |   🍅🍅🍅
    🌻    | |   | |   🌱🌱🌱
`;

const TRACTOR_ASCII = `
     __
  __/  \\___
 |  ____   |
 '-|    |-(_)
   |____|
`;

const WEATHER_ICONS: Record<string, string> = {
  hot: '☀️ ',
  normal: '🌤️',
  drought: '💧',
};

// Types
interface Sensor {
  sensorId: string;
  zoneId: string;
  temperature: number;
  humidity: number;
  soilMoisture: number;
  timestamp: number;
}

interface Zone {
  zoneId: string;
  name: string;
  crop: string;
  emoji: string;
  area: number;
}

interface Alert {
  sensorId: string;
  zoneName: string;
  crop: string;
  emoji: string;
  temperature: number;
  soilMoisture: number;
  type: 'heat' | 'drought';
}

interface ZoneStats {
  zoneId: string;
  name: string;
  crop: string;
  emoji: string;
  avgTemp: number;
  avgHumidity: number;
  avgMoisture: number;
  sensorCount: number;
  status: 'good' | 'warning' | 'critical';
}

// Static zone data with emojis
const zones: Zone[] = [
  { zoneId: 'z1', name: 'North Field', crop: 'Wheat', emoji: '🌾', area: 50 },
  { zoneId: 'z2', name: 'South Field', crop: 'Corn', emoji: '🌽', area: 75 },
  { zoneId: 'z3', name: 'Greenhouse A', crop: 'Tomatoes', emoji: '🍅', area: 10 },
  { zoneId: 'z4', name: 'Orchard', crop: 'Apples', emoji: '🍎', area: 30 },
];

function generateSensor(): Sensor {
  const zone = zones[Math.floor(Math.random() * zones.length)];
  return {
    sensorId: `S${Math.random().toString(36).slice(2, 6).toUpperCase()}`,
    zoneId: zone.zoneId,
    temperature: Math.round((15 + Math.random() * 30) * 10) / 10,
    humidity: Math.round((30 + Math.random() * 50) * 10) / 10,
    soilMoisture: Math.round((20 + Math.random() * 60) * 10) / 10,
    timestamp: Date.now(),
  };
}

export function FarmDashboard() {
  const [sensors, setSensors] = useState<Sensor[]>([]);
  const [isStreaming, setIsStreaming] = useState(false);
  const [streamRate, setStreamRate] = useState(200);
  const [stats, setStats] = useState({
    totalSensors: 0,
    alertCount: 0,
    joinTimeMs: 0,
    aggTimeMs: 0,
  });

  // Simulated SQL JOIN
  const alerts = useMemo(() => {
    const start = performance.now();
    const result: Alert[] = [];
    
    for (const sensor of sensors) {
      const zone = zones.find(z => z.zoneId === sensor.zoneId);
      if (zone && (sensor.temperature > 35 || sensor.soilMoisture < 25)) {
        result.push({
          sensorId: sensor.sensorId,
          zoneName: zone.name,
          crop: zone.crop,
          emoji: zone.emoji,
          temperature: sensor.temperature,
          soilMoisture: sensor.soilMoisture,
          type: sensor.temperature > 35 ? 'heat' : 'drought',
        });
      }
    }
    
    const elapsed = performance.now() - start;
    if (elapsed > 0.01) {
      setStats(prev => ({ ...prev, joinTimeMs: elapsed }));
    }
    
    return result.slice(-10);
  }, [sensors]);

  // Simulated SQL GROUP BY with status
  const zoneStats = useMemo(() => {
    const start = performance.now();
    const grouped = new Map<string, { temps: number[]; humidities: number[]; moistures: number[]; count: number }>();
    
    for (const sensor of sensors) {
      const existing = grouped.get(sensor.zoneId) || { temps: [], humidities: [], moistures: [], count: 0 };
      existing.temps.push(sensor.temperature);
      existing.humidities.push(sensor.humidity);
      existing.moistures.push(sensor.soilMoisture);
      existing.count++;
      grouped.set(sensor.zoneId, existing);
    }
    
    const result: ZoneStats[] = zones.map(zone => {
      const data = grouped.get(zone.zoneId);
      if (!data || data.count === 0) {
        return { 
          zoneId: zone.zoneId, name: zone.name, crop: zone.crop, emoji: zone.emoji,
          avgTemp: 0, avgHumidity: 0, avgMoisture: 0, sensorCount: 0, status: 'good' as const
        };
      }
      const avgTemp = data.temps.reduce((a, b) => a + b, 0) / data.count;
      const avgMoisture = data.moistures.reduce((a, b) => a + b, 0) / data.count;
      let status: 'good' | 'warning' | 'critical' = 'good';
      if (avgTemp > 35 || avgMoisture < 25) status = 'critical';
      else if (avgTemp > 30 || avgMoisture < 35) status = 'warning';
      
      return {
        zoneId: zone.zoneId,
        name: zone.name,
        crop: zone.crop,
        emoji: zone.emoji,
        avgTemp,
        avgHumidity: data.humidities.reduce((a, b) => a + b, 0) / data.count,
        avgMoisture,
        sensorCount: data.count,
        status,
      };
    });
    
    const elapsed = performance.now() - start;
    if (elapsed > 0.01) {
      setStats(prev => ({ ...prev, aggTimeMs: elapsed }));
    }
    
    return result;
  }, [sensors]);

  useEffect(() => {
    if (!isStreaming) return;
    
    const interval = setInterval(() => {
      const newSensor = generateSensor();
      setSensors(prev => {
        const newSensors = [...prev, newSensor].slice(-500);
        setStats(s => ({ ...s, totalSensors: s.totalSensors + 1, alertCount: alerts.length }));
        return newSensors;
      });
    }, streamRate);

    return () => clearInterval(interval);
  }, [isStreaming, streamRate, alerts.length]);

  const addSensor = useCallback(() => {
    const newSensor = generateSensor();
    setSensors(prev => [...prev, newSensor].slice(-500));
    setStats(s => ({ ...s, totalSensors: s.totalSensors + 1 }));
  }, []);

  const clear = useCallback(() => {
    setSensors([]);
    setStats({ totalSensors: 0, alertCount: 0, joinTimeMs: 0, aggTimeMs: 0 });
  }, []);

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'critical': return 'text-[#ff3333]';
      case 'warning': return 'text-[#ffb000]';
      default: return 'text-[#39ff14]';
    }
  };

  const getStatusBorder = (status: string) => {
    switch (status) {
      case 'critical': return 'border-[#ff3333]/50';
      case 'warning': return 'border-[#ffb000]/50';
      default: return 'border-[#39ff14]/50';
    }
  };

  return (
    <section id="farm-dashboard" className="relative py-20 bg-[#0a0a0a] overflow-hidden">
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
      <div className="relative z-10 max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 font-mono">
        <SectionHeading
          id="farm-heading"
          badge="Live Demo"
          title="Smart Farm Dashboard"
          subtitle="Real-time crop monitoring powered by incremental SQL"
        />

        {/* Farm ASCII Art Header */}
        <div className="max-w-4xl mx-auto mb-8 p-4 bg-[#0a0a0a] border border-[#39ff14]/30">
          <pre className="text-[#39ff14] text-xs md:text-sm text-center leading-tight">
{`
    \\|/         🌾 SMARTFARM v2.0 🌾         \\|/
   --*--    ═══════════════════════════    --*--
    /|\\     Sensor Network: ONLINE         /|\\
            Zones: ${zones.length} | Crops: ${zones.map(z => z.emoji).join('')}
`}
          </pre>
        </div>

        {/* Control Panel */}
        <div className="max-w-4xl mx-auto mb-8 p-4 bg-[#0a0a0a] border border-[#00ffff]/30">
          <div className="text-[#00ffff] text-sm mb-4">
            ┌─ CONTROL_PANEL ──────────────────────────────────────────────┐
          </div>
          <div className="flex flex-wrap items-center justify-center gap-4">
            <button
              onClick={addSensor}
              className="px-4 py-2 bg-[#39ff14] text-[#0a0a0a] font-bold hover:bg-[#50ff30] transition-colors"
            >
              🌡️ [+] ADD_READING
            </button>
            <button
              onClick={() => setIsStreaming(!isStreaming)}
              className={`px-4 py-2 font-bold transition-colors ${
                isStreaming 
                  ? 'bg-[#ff3333] text-[#0a0a0a] hover:bg-[#ff5555]' 
                  : 'border border-[#39ff14] text-[#39ff14] hover:bg-[#39ff14]/10'
              }`}
            >
              {isStreaming ? '🛑 [■] STOP_SENSORS' : '📡 [▶] START_SENSORS'}
            </button>
            <div className="flex items-center gap-2 text-[#808080]">
              <span>RATE:</span>
              <select
                value={streamRate}
                onChange={(e) => setStreamRate(Number(e.target.value))}
                className="px-3 py-2 bg-[#0a0a0a] text-[#39ff14] border border-[#39ff14]/30"
              >
                <option value={500}>🐢 2/sec</option>
                <option value={200}>🚶 5/sec</option>
                <option value={100}>🏃 10/sec</option>
                <option value={50}>🚀 20/sec</option>
              </select>
            </div>
            <button
              onClick={clear}
              className="px-4 py-2 border border-[#ff3333]/50 text-[#ff3333] font-bold hover:bg-[#ff3333]/10 transition-colors"
            >
              🗑️ [X] CLEAR
            </button>
          </div>
          <div className="text-[#00ffff]/30 text-xs mt-4">
            └──────────────────────────────────────────────────────────────┘
          </div>
        </div>

        {/* Stats Dashboard */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8">
          <div className="p-4 bg-[#0a0a0a] border border-[#39ff14]/30">
            <div className="text-[#808080] text-xs">📊 READINGS</div>
            <div className="text-3xl font-bold text-[#39ff14] tabular-nums">{stats.totalSensors}</div>
            <div className="text-[#39ff14]/50 text-xs mt-1">
              {'█'.repeat(Math.min(10, Math.floor(stats.totalSensors / 50)))}{'░'.repeat(10 - Math.min(10, Math.floor(stats.totalSensors / 50)))}
            </div>
          </div>
          <div className="p-4 bg-[#0a0a0a] border border-[#ffb000]/30">
            <div className="text-[#808080] text-xs">🚨 ALERTS</div>
            <div className="text-3xl font-bold text-[#ffb000] tabular-nums">{alerts.length}</div>
            <div className="text-[#ffb000]/50 text-xs mt-1">
              {alerts.length > 0 ? '⚠️ ACTION NEEDED' : '✓ ALL CLEAR'}
            </div>
          </div>
          <div className="p-4 bg-[#0a0a0a] border border-[#00ffff]/30">
            <div className="text-[#808080] text-xs">⚡ JOIN_TIME</div>
            <div className="text-3xl font-bold text-[#00ffff] tabular-nums">{stats.joinTimeMs.toFixed(2)}<span className="text-sm">ms</span></div>
            <div className="text-[#00ffff]/50 text-xs mt-1">sensors ⋈ zones</div>
          </div>
          <div className="p-4 bg-[#0a0a0a] border border-[#ff00ff]/30">
            <div className="text-[#808080] text-xs">📈 AGG_TIME</div>
            <div className="text-3xl font-bold text-[#ff00ff] tabular-nums">{stats.aggTimeMs.toFixed(2)}<span className="text-sm">ms</span></div>
            <div className="text-[#ff00ff]/50 text-xs mt-1">GROUP BY zone</div>
          </div>
        </div>

        {/* Main Grid */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Zone Status - GROUP BY */}
          <div className="p-6 bg-[#0a0a0a] border border-[#00ffff]/30">
            <div className="flex items-center justify-between mb-4">
              <span className="text-[#00ffff]">
                ┌─ 🗺️ ZONE_STATUS ─────────────────────────┐
              </span>
            </div>
            <pre className="text-[#808080] text-xs mb-4">
{`   ZONE          │ CROP  │ TEMP  │ H2O  │ STATUS
  ────────────────┼───────┼───────┼──────┼────────`}
            </pre>
            <div className="space-y-1">
              {zoneStats.map((zone) => (
                <div
                  key={zone.zoneId}
                  className={`flex items-center p-2 bg-[#0d1117] border ${getStatusBorder(zone.status)} transition-all`}
                >
                  <div className="w-8 text-center text-lg">{zone.emoji}</div>
                  <div className="flex-1 grid grid-cols-5 gap-2 text-sm">
                    <span className="text-[#e0e0e0] truncate">{zone.name}</span>
                    <span className="text-[#808080]">{zone.crop}</span>
                    <span className={zone.avgTemp > 35 ? 'text-[#ff3333]' : zone.avgTemp > 30 ? 'text-[#ffb000]' : 'text-[#39ff14]'}>
                      {zone.avgTemp.toFixed(1)}°C
                    </span>
                    <span className={zone.avgMoisture < 25 ? 'text-[#ff3333]' : zone.avgMoisture < 35 ? 'text-[#ffb000]' : 'text-[#39ff14]'}>
                      {zone.avgMoisture.toFixed(0)}%💧
                    </span>
                    <span className={`${getStatusColor(zone.status)} font-bold`}>
                      {zone.status === 'critical' ? '🔴 CRIT' : zone.status === 'warning' ? '🟡 WARN' : '🟢 OK'}
                    </span>
                  </div>
                </div>
              ))}
            </div>
            <div className="text-[#00ffff]/30 text-xs mt-4">
              └─────────────────────────────────────────────┘
            </div>
          </div>

          {/* Alerts - JOIN */}
          <div className="p-6 bg-[#0a0a0a] border border-[#ffb000]/30">
            <div className="flex items-center justify-between mb-4">
              <span className="text-[#ffb000]">
                ┌─ 🚨 CROP_ALERTS (sensors ⋈ zones) ────────┐
              </span>
            </div>
            
            {alerts.length === 0 ? (
              <div className="text-center py-12">
                <pre className="text-[#39ff14] text-xs">
{`
     🌱 ALL CROPS HEALTHY 🌱
    
    ╔═══════════════════════╗
    ║  No alerts detected   ║
    ║  Sensors operating    ║
    ║  normally             ║
    ╚═══════════════════════╝
`}
                </pre>
                <div className="text-[#808080] text-sm mt-4">
                  $ Start streaming to monitor crops...
                  <span className="inline-block w-2 h-4 bg-[#39ff14] animate-pulse ml-1" />
                </div>
              </div>
            ) : (
              <div className="space-y-2 max-h-80 overflow-y-auto">
                {alerts.map((alert, i) => (
                  <div
                    key={`${alert.sensorId}-${i}`}
                    className={`p-3 delta-row ${
                      alert.type === 'heat' 
                        ? 'bg-[#ff3333]/10 border-l-4 border-[#ff3333]' 
                        : 'bg-[#00ffff]/10 border-l-4 border-[#00ffff]'
                    }`}
                  >
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-2">
                        <span className="text-lg">{alert.emoji}</span>
                        <span className={`px-2 py-0.5 text-xs font-bold ${
                          alert.type === 'heat' 
                            ? 'bg-[#ff3333] text-[#0a0a0a]' 
                            : 'bg-[#00ffff] text-[#0a0a0a]'
                        }`}>
                          {alert.type === 'heat' ? '🔥 HEAT' : '💧 DRY'}
                        </span>
                        <span className="text-[#e0e0e0] text-sm">{alert.zoneName}</span>
                      </div>
                      <div className="text-right text-sm">
                        <div className={alert.type === 'heat' ? 'text-[#ff3333]' : 'text-[#00ffff]'}>
                          {alert.type === 'heat' ? `${alert.temperature.toFixed(1)}°C` : `${alert.soilMoisture.toFixed(0)}% 💧`}
                        </div>
                        <div className="text-[#808080] text-xs">{alert.sensorId}</div>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            )}
            <div className="text-[#ffb000]/30 text-xs mt-4">
              └──────────────────────────────────────────────┘
            </div>
          </div>
        </div>

        {/* Sensor Stream */}
        <div className="mt-6 p-4 bg-[#0a0a0a] border border-[#39ff14]/20">
          <div className="text-[#39ff14] text-sm mb-2">
            ─── 📡 SENSOR_STREAM ──────────────────────────────────────────
          </div>
          <div className="h-20 overflow-hidden text-xs">
            {sensors.slice(-6).map((s, i) => {
              const zone = zones.find(z => z.zoneId === s.zoneId);
              const isHot = s.temperature > 35;
              const isDry = s.soilMoisture < 25;
              return (
                <div key={i} className={`${isHot ? 'text-[#ff3333]' : isDry ? 'text-[#00ffff]' : 'text-[#39ff14]/70'}`}>
                  [{new Date(s.timestamp).toISOString().slice(11, 23)}] {zone?.emoji || '📍'} {s.sensorId.padEnd(6)} 
                  │ {s.zoneId} │ {s.temperature.toFixed(1).padStart(5)}°C │ {s.humidity.toFixed(0).padStart(3)}% 💨 │ {s.soilMoisture.toFixed(0).padStart(3)}% 💧
                  {isHot && ' ⚠️ HOT!'}
                  {isDry && ' ⚠️ DRY!'}
                </div>
              );
            })}
          </div>
        </div>

        {/* SQL Queries */}
        <div className="mt-8 p-6 bg-[#0a0a0a] border border-[#39ff14]/30">
          <div className="text-[#39ff14] text-sm mb-4">
            ┌─ 📋 SQL_QUERIES ─────────────────────────────────────────────┐
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6 text-sm">
            <div>
              <div className="text-[#00ffff] mb-2">$ cat zone_stats.sql</div>
              <CodeBlock 
                code={`-- 📊 Zone Statistics
SELECT 
  zone_id,
  zone_name,
  crop_type,
  AVG(temperature) as avg_temp,
  AVG(soil_moisture) as avg_moisture,
  COUNT(*) as readings
FROM sensors
GROUP BY zone_id`}
                language="sql"
              />
            </div>
            <div>
              <div className="text-[#ffb000] mb-2">$ cat crop_alerts.sql</div>
              <CodeBlock 
                code={`-- 🚨 Crop Alerts (JOIN)
SELECT 
  s.sensor_id,
  z.zone_name,
  z.crop_type,
  s.temperature,
  s.soil_moisture
FROM sensors s
JOIN zones z ON s.zone_id = z.zone_id
WHERE s.temperature > 35 
   OR s.soil_moisture < 25`}
                language="sql"
              />
            </div>
          </div>
          <div className="text-[#39ff14]/30 text-xs mt-4">
            └─────────────────────────────────────────────────────────────┘
          </div>
        </div>
      </div>
    </section>
  );
}
