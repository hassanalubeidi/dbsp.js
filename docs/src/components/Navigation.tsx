import { useState, useEffect } from 'react';

const navItems = [
  { id: 'hero', label: 'home', shortcut: '1' },
  { id: 'the-pitch', label: 'why', shortcut: '2' },
  { id: 'quickstart', label: 'start', shortcut: '3' },
  { id: 'farm-dashboard', label: 'demo', shortcut: '4' },
  { id: 'deep-dive', label: 'how', shortcut: '5' },
  { id: 'api-reference', label: 'api', shortcut: '6' },
];

export function Navigation() {
  const [activeSection, setActiveSection] = useState('hero');
  const [isScrolled, setIsScrolled] = useState(false);

  useEffect(() => {
    const handleScroll = () => {
      setIsScrolled(window.scrollY > 50);
      
      // Find active section
      const sections = navItems.map(item => document.getElementById(item.id));
      const scrollPos = window.scrollY + 100;
      
      for (let i = sections.length - 1; i >= 0; i--) {
        const section = sections[i];
        if (section && section.offsetTop <= scrollPos) {
          setActiveSection(navItems[i].id);
          break;
        }
      }
    };

    window.addEventListener('scroll', handleScroll);
    return () => window.removeEventListener('scroll', handleScroll);
  }, []);

  return (
    <nav className={`fixed top-0 left-0 right-0 z-50 transition-all duration-300 border-b ${
      isScrolled 
        ? 'bg-[#0d1117]/95 backdrop-blur-sm border-[#39ff14]/30' 
        : 'bg-transparent border-transparent'
    }`}>
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between h-14">
          {/* Logo - Terminal style */}
          <a href="#hero" className="flex items-center gap-2 group border-none">
            <span className="text-[#39ff14] text-lg font-mono">
              <span className="opacity-60">$</span> dbsp.js
            </span>
            <span className="w-2 h-5 bg-[#39ff14] animate-pulse" />
          </a>

          {/* Nav Links - Terminal tabs */}
          <div className="hidden md:flex items-center font-mono">
            <span className="text-[#808080] mr-2">├</span>
            {navItems.map((item, index) => (
              <a
                key={item.id}
                href={`#${item.id}`}
                className={`px-3 py-1 text-sm transition-all border-none ${
                  activeSection === item.id
                    ? 'text-[#0d1117] bg-[#39ff14]'
                    : 'text-[#808080] hover:text-[#39ff14] hover:bg-[#39ff14]/10'
                }`}
              >
                <span className="opacity-50 mr-1">[{item.shortcut}]</span>
                {item.label}
                {index < navItems.length - 1 && (
                  <span className="ml-3 text-[#39ff14]/30">│</span>
                )}
              </a>
            ))}
            <span className="text-[#808080] ml-2">┤</span>
          </div>

          {/* GitHub Link */}
          <a
            href="https://github.com/hassanlubeidi/dbsp-js"
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-2 px-3 py-1 text-sm font-mono text-[#00ffff] hover:bg-[#00ffff]/10 transition-colors border border-[#00ffff]/30 hover:border-[#00ffff]"
          >
            <span className="opacity-60">→</span>
            github
          </a>
        </div>
      </div>
    </nav>
  );
}
