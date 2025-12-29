import { useState, useEffect } from 'react';

// Simulated reading generator
function generateReading() {
  return {
    sensorId: `sensor-${Math.floor(Math.random() * 10)}`,
    temperature: 20 + Math.random() * 25,
    humidity: 40 + Math.random() * 40,
    timestamp: Date.now(),
  };
}

export function Hero() {
  const [readings, setReadings] = useState<ReturnType<typeof generateReading>[]>([]);
  const [alertCount, setAlertCount] = useState(0);
  const [totalCount, setTotalCount] = useState(0);
  const [lastUpdateMs, setLastUpdateMs] = useState(0);
  
  // Animation states
  const [typedText, setTypedText] = useState('');
  const [strikethroughCount, setStrikethroughCount] = useState(0);
  const [showMicro, setShowMicro] = useState(false);
  const [microScale, setMicroScale] = useState(0);
  const [showLightning, setShowLightning] = useState(false);
  const [animationPhase, setAnimationPhase] = useState<'typing' | 'pause' | 'striking' | 'pause2' | 'micro-reveal' | 'done'>('typing');
  
  const baseText = 'Real-time SQL views that update in ';
  const milliLetters = ['m', 'i', 'l', 'l', 'i'];

  // Main animation orchestrator
  useEffect(() => {
    let timeout: NodeJS.Timeout;
    let interval: NodeJS.Timeout;
    
    if (animationPhase === 'typing') {
      const fullText = baseText + 'milliseconds';
      let i = 0;
      interval = setInterval(() => {
        if (i <= fullText.length) {
          setTypedText(fullText.slice(0, i));
          i++;
        } else {
          clearInterval(interval);
          setAnimationPhase('pause');
        }
      }, 17); // 2x faster typing
      return () => clearInterval(interval);
    }
    
    if (animationPhase === 'pause') {
      timeout = setTimeout(() => {
        setAnimationPhase('striking');
      }, 500); // 2x faster pause
      return () => clearTimeout(timeout);
    }
    
    if (animationPhase === 'striking') {
      // Strike through letter by letter with dramatic timing
      let letterIndex = 0;
      interval = setInterval(() => {
        if (letterIndex < milliLetters.length) {
          setStrikethroughCount(letterIndex + 1);
          letterIndex++;
        } else {
          clearInterval(interval);
          setAnimationPhase('pause2');
        }
      }, 75); // 2x faster strikethrough
      return () => clearInterval(interval);
    }
    
    if (animationPhase === 'pause2') {
      timeout = setTimeout(() => {
        setAnimationPhase('micro-reveal');
      }, 200); // 2x faster pause
      return () => clearTimeout(timeout);
    }
    
    if (animationPhase === 'micro-reveal') {
      // Dramatic micro reveal with scale animation
      setShowMicro(true);
      
      // Animate scale from 0 to 1 with overshoot
      let scale = 0;
      interval = setInterval(() => {
        scale += 0.3; // 2x faster scale
        if (scale >= 1.2) {
          setMicroScale(1);
          clearInterval(interval);
          setTimeout(() => {
            setShowLightning(true);
            setAnimationPhase('done');
          }, 100); // Keep original for dramatic effect
        } else {
          setMicroScale(scale);
        }
      }, 15); // 2x faster interval
      return () => clearInterval(interval);
    }
  }, [animationPhase]);

  // Data stream
  useEffect(() => {
    const interval = setInterval(() => {
      const reading = generateReading();
      const start = performance.now();
      
      setReadings(prev => {
        const newReadings = [...prev, reading].slice(-50);
        const alerts = newReadings.filter(r => r.temperature > 35);
        setAlertCount(alerts.length);
        return newReadings;
      });
      
      setTotalCount(prev => prev + 1);
      setLastUpdateMs(performance.now() - start);
    }, 100);

    return () => clearInterval(interval);
  }, []);

  const sqlQuery = "SELECT * FROM sensors WHERE temp > 35";

  // Render the dramatically animated tagline
  const renderTagline = () => {
    // During typing phase
    if (animationPhase === 'typing') {
      return (
        <>
          <span className="text-[#e0e0e0]">{typedText}</span>
          <span className="inline-block w-3 h-6 bg-[#39ff14] animate-pulse ml-1" />
        </>
      );
    }
    
    // After typing, show full text with animation
    return (
      <>
        <span className="text-[#e0e0e0]">{baseText}</span>
        
        {/* The "milli" part with letter-by-letter strikethrough */}
        <span className="relative inline-block">
          {/* Original milli letters with progressive strikethrough */}
          {!showMicro && milliLetters.map((letter, index) => (
            <span
              key={index}
              className={`relative inline-block transition-all duration-150 ${
                index < strikethroughCount 
                  ? 'text-[#ff3333]' 
                  : 'text-[#e0e0e0]'
              }`}
              style={{
                textDecoration: index < strikethroughCount ? 'line-through' : 'none',
                textDecorationThickness: '3px',
                textDecorationColor: '#ff3333',
                transform: index < strikethroughCount ? 'translateY(2px)' : 'none',
                opacity: showMicro ? 0 : 1,
              }}
            >
              {letter}
              {/* Red slash effect on each struck letter */}
              {index < strikethroughCount && (
                <span 
                  className="absolute inset-0 flex items-center justify-center text-[#ff3333] font-bold pointer-events-none"
                  style={{
                    animation: 'slashAppear 0.15s ease-out',
                  }}
                >
                </span>
              )}
            </span>
          ))}
          
          {/* The dramatic "micro" reveal */}
          {showMicro && (
            <span
              className="inline-block text-[#39ff14] font-bold"
              style={{
                transform: `scale(${microScale})`,
                textShadow: microScale >= 1 
                  ? '0 0 10px #39ff14, 0 0 20px #39ff14, 0 0 40px #39ff14, 0 0 80px #39ff14' 
                  : 'none',
                transition: 'text-shadow 0.3s ease-out',
              }}
            >
              micro
            </span>
          )}
        </span>
        
        <span className="text-[#e0e0e0]">seconds</span>
        
        {/* Lightning bolt with dramatic entrance */}
        {showLightning && (
          <span 
            className="inline-block ml-2 text-2xl"
            style={{
              animation: 'lightningStrike 0.5s ease-out',
            }}
          >
            ⚡
          </span>
        )}
        
        {/* Blinking cursor during animation */}
        {animationPhase !== 'done' && (
          <span className="inline-block w-3 h-6 bg-[#39ff14] animate-pulse ml-1" />
        )}
      </>
    );
  };

  return (
    <section id="hero" className="relative min-h-screen flex items-center justify-center overflow-hidden pt-16">
      {/* Keyframe animations */}
      <style>{`
        @keyframes slashAppear {
          0% { opacity: 0; transform: scale(1.5); }
          100% { opacity: 1; transform: scale(1); }
        }
        
        @keyframes lightningStrike {
          0% { 
            opacity: 0; 
            transform: translateY(-20px) scale(2);
            filter: brightness(3);
          }
          50% {
            opacity: 1;
            transform: translateY(5px) scale(1.2);
            filter: brightness(2);
          }
          100% { 
            opacity: 1; 
            transform: translateY(0) scale(1);
            filter: brightness(1);
          }
        }
        
        @keyframes glowPulse {
          0%, 100% { 
            text-shadow: 0 0 10px #39ff14, 0 0 20px #39ff14, 0 0 40px #39ff14;
          }
          50% { 
            text-shadow: 0 0 20px #39ff14, 0 0 40px #39ff14, 0 0 80px #39ff14, 0 0 120px #39ff14;
          }
        }
      `}</style>
      
      {/* Background */}
      <div className="absolute inset-0 bg-[#0a0a0a]" />
      
      {/* Grid overlay */}
      <div className="absolute inset-0 opacity-5">
        <div className="absolute inset-0" style={{
          backgroundImage: `
            linear-gradient(rgba(57, 255, 20, 0.3) 1px, transparent 1px),
            linear-gradient(90deg, rgba(57, 255, 20, 0.3) 1px, transparent 1px)
          `,
          backgroundSize: '40px 40px',
        }} />
      </div>

      {/* Content */}
      <div className="relative z-10 max-w-6xl mx-auto px-4 sm:px-6 lg:px-8 py-20 font-mono">
        <div className="text-center mb-12 stagger-children">
          
          {/* ASCII Logo */}
          <div className="mb-8 overflow-x-auto">
            <pre 
              className="text-[#39ff14] text-[10px] sm:text-sm leading-tight inline-block text-left text-glow-green"
              style={{ fontFamily: "'JetBrains Mono', monospace" }}
            >
{`██████╗ ██████╗ ███████╗██████╗        ██╗███████╗
██╔══██╗██╔══██╗██╔════╝██╔══██╗       ██║██╔════╝
██║  ██║██████╔╝███████╗██████╔╝       ██║███████╗
██║  ██║██╔══██╗╚════██║██╔═══╝   ██   ██║╚════██║
██████╔╝██████╔╝███████║██║       ╚█████╔╝███████║
╚═════╝ ╚═════╝ ╚══════╝╚═╝        ╚════╝ ╚══════╝`}
            </pre>
            <div className="mt-4 text-sm sm:text-base text-[#808080]">
              <span className="text-[#39ff14]">▸</span> Incremental SQL for Real-time Applications
            </div>
            <div className="flex flex-wrap justify-center gap-3 mt-3 text-xs sm:text-sm">
              <span className="text-[#00ffff]">React</span>
              <span className="text-[#808080]">•</span>
              <span className="text-[#00ffff]">TypeScript</span>
              <span className="text-[#808080]">•</span>
              <span className="text-[#ffb000]">O(Δ)</span>
              <span className="text-[#808080]">•</span>
              <span className="text-[#ff00ff]">SQL-powered</span>
            </div>
          </div>

          {/* Main tagline with DRAMATIC animation */}
          <div className="text-xl md:text-2xl text-[#808080] max-w-3xl mx-auto mb-8 min-h-[4rem]">
            <span className="text-[#39ff14]">$ </span>
            {renderTagline()}
          </div>

          {/* Feature badges */}
          <div className="flex flex-wrap justify-center gap-4 mb-8 text-sm">
            <div className="px-4 py-2 bg-[#39ff14]/10 border border-[#39ff14]/50 text-[#39ff14]">
              ⚡ O(Δ) Updates
            </div>
            <div className="px-4 py-2 bg-[#00ffff]/10 border border-[#00ffff]/50 text-[#00ffff]">
              🔗 Incremental JOINs
            </div>
            <div className="px-4 py-2 bg-[#ff00ff]/10 border border-[#ff00ff]/50 text-[#ff00ff]">
              📊 Live Aggregations
            </div>
            <div className="px-4 py-2 bg-[#ffb000]/10 border border-[#ffb000]/50 text-[#ffb000]">
              🎯 React Hooks
            </div>
          </div>

          {/* CTA buttons */}
          <div className="flex flex-wrap items-center justify-center gap-4 mb-12">
            <a
              href="#quickstart"
              className="px-8 py-3 bg-[#39ff14] text-[#0a0a0a] font-bold hover:bg-[#50ff30] transition-all glow-green text-lg"
            >
              ▶ QUICK_START
            </a>
            <a
              href="#farm-dashboard"
              className="px-8 py-3 border-2 border-[#39ff14] text-[#39ff14] font-bold hover:bg-[#39ff14]/10 transition-all text-lg"
            >
              🌾 LIVE_DEMO
            </a>
          </div>
        </div>

        {/* Live stats with subtle SQL */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 max-w-4xl mx-auto mb-8">
          {/* Total readings */}
          <div className="p-4 bg-[#0a0a0a] border border-[#39ff14]/30">
            <div className="text-[#808080] text-xs mb-1">┌─ TOTAL_EVENTS ─┐</div>
            <div className="text-4xl font-bold text-[#39ff14] tabular-nums text-glow-green">
              {totalCount.toLocaleString().padStart(6, '0')}
            </div>
            <div className="flex items-center gap-2 text-xs text-[#808080] mt-2">
              <span className="w-2 h-2 bg-[#39ff14] animate-pulse" />
              STREAMING @ 10/sec
            </div>
            <div className="text-[#39ff14]/30 text-xs mt-2">└────────────────┘</div>
          </div>

          {/* Alert count with SQL */}
          <div className="p-4 bg-[#0a0a0a] border border-[#ffb000]/30">
            <div className="text-[#808080] text-xs mb-1">┌─ FILTERED ─┐</div>
            <div className="text-4xl font-bold text-[#ffb000] tabular-nums text-glow-amber">
              {String(alertCount).padStart(6, '0')}
            </div>
            <div className="text-[10px] text-[#ffb000]/60 mt-2 font-normal">
              WHERE temp &gt; 35
            </div>
            <div className="text-[#ffb000]/30 text-xs mt-2">└─────────────┘</div>
          </div>

          {/* Update time */}
          <div className="p-4 bg-[#0a0a0a] border border-[#00ffff]/30">
            <div className="text-[#808080] text-xs mb-1">┌─ LATENCY ─┐</div>
            <div className="text-4xl font-bold text-[#00ffff] tabular-nums text-glow-cyan">
              {lastUpdateMs.toFixed(2)}<span className="text-xl">ms</span>
            </div>
            <div className="text-xs text-[#808080] mt-2">
              ⚡ O(Δ) not O(N)
            </div>
            <div className="text-[#00ffff]/30 text-xs mt-2">└───────────┘</div>
          </div>
        </div>

        {/* Live stream preview with elegant SQL */}
        <div className="max-w-4xl mx-auto">
          <div className="flex items-center justify-between text-xs mb-2">
            <span className="text-[#39ff14]">─── 📡 LIVE_DATA_STREAM ───</span>
            <span className="text-[#ffb000]/50 italic">{sqlQuery}</span>
          </div>
          <div className="bg-[#0a0a0a] border border-[#39ff14]/20 p-3 h-28 overflow-hidden text-xs">
            {readings.slice(-8).map((r, i) => {
              const isAlert = r.temperature > 35;
              return (
                <div key={i} className={`${isAlert ? 'text-[#ffb000]' : 'text-[#39ff14]/70'} delta-row`}>
                  <span className="text-[#808080]">[{new Date(r.timestamp).toISOString().slice(11, 23)}]</span>
                  {' '}
                  <span className="text-[#00ffff]">{r.sensorId.padEnd(10)}</span>
                  {' │ '}
                  <span className={isAlert ? 'text-[#ffb000]' : 'text-[#39ff14]'}>
                    temp={r.temperature.toFixed(1).padStart(5)}°C
                  </span>
                  {' │ '}
                  <span className="text-[#ff00ff]">
                    humidity={r.humidity.toFixed(1).padStart(5)}%
                  </span>
                  {isAlert && <span className="text-[#ffb000]"> ⚠️ ALERT</span>}
                </div>
              );
            })}
          </div>
        </div>

        {/* Scroll indicator */}
        <div className="text-center mt-12">
          <div className="text-[#39ff14]/50 text-sm animate-bounce">
            ↓ SCROLL_FOR_MORE ↓
          </div>
        </div>
      </div>
    </section>
  );
}
