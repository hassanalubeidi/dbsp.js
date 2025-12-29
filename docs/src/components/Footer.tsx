export function Footer() {
  return (
    <footer className="bg-[#0a0a0a] border-t border-[#39ff14]/20 py-12 mt-20 font-mono">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        {/* ASCII art divider */}
        <div className="text-[#39ff14]/30 text-xs mb-8 text-center overflow-hidden">
          ╔{'═'.repeat(70)}╗
        </div>

        <div className="grid grid-cols-1 md:grid-cols-4 gap-8">
          {/* Logo & Description */}
          <div className="md:col-span-2">
            <div className="flex items-center gap-2 mb-4">
              <span className="text-[#39ff14] text-xl text-glow-green">
                {'>'} dbsp.js_
              </span>
            </div>
            <p className="text-[#808080] text-sm max-w-md leading-relaxed">
              # Incremental view maintenance for React.
              <br />
              # Based on the DBSP paper by VMware Research.
              <br />
              # Build real-time dashboards that scale.
            </p>
          </div>

          {/* Links */}
          <div>
            <h4 className="text-[#39ff14] mb-4 text-sm">┌─ RESOURCES ─┐</h4>
            <ul className="space-y-2 text-sm">
              <li>
                <a href="#quickstart" className="text-[#808080] hover:text-[#39ff14] transition-colors border-none">
                  → quick_start
                </a>
              </li>
              <li>
                <a href="#api-reference" className="text-[#808080] hover:text-[#39ff14] transition-colors border-none">
                  → api_reference
                </a>
              </li>
              <li>
                <a 
                  href="https://arxiv.org/abs/2203.16684" 
                  target="_blank" 
                  rel="noopener noreferrer"
                  className="text-[#00ffff] hover:text-[#50ffff] transition-colors border-none"
                >
                  → dbsp_paper ↗
                </a>
              </li>
            </ul>
          </div>

          <div>
            <h4 className="text-[#39ff14] mb-4 text-sm">┌─ COMMUNITY ─┐</h4>
            <ul className="space-y-2 text-sm">
              <li>
                <a 
                  href="https://github.com/user/dbsp.js" 
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-[#00ffff] hover:text-[#50ffff] transition-colors border-none"
                >
                  → github ↗
                </a>
              </li>
              <li>
                <a 
                  href="https://www.npmjs.com/package/@dbsp/core" 
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-[#00ffff] hover:text-[#50ffff] transition-colors border-none"
                >
                  → npm ↗
                </a>
              </li>
            </ul>
          </div>
        </div>

        {/* Bottom bar */}
        <div className="mt-8 text-center">
          <div className="text-[#39ff14]/30 text-xs mb-4 overflow-hidden">
            ╚{'═'.repeat(70)}╝
          </div>
          <div className="text-[#808080] text-xs">
            <span className="text-[#39ff14]">$</span> echo "Built with dbsp.js • MIT License • $(date)"
          </div>
          <div className="text-[#39ff14]/50 text-xs mt-1">
            EOF
          </div>
        </div>
      </div>
    </footer>
  );
}
