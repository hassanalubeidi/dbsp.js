import { useState, useEffect, useMemo } from 'react';
import { codeToHtml, bundledLanguages } from 'shiki';

interface CodeBlockProps {
  code: string;
  language?: string;
  filename?: string;
  showLineNumbers?: boolean;
}

export function CodeBlock({ code, language = 'typescript', filename, showLineNumbers = false }: CodeBlockProps) {
  const [copied, setCopied] = useState(false);
  const [highlightedHtml, setHighlightedHtml] = useState<string | null>(null);

  const trimmedCode = useMemo(() => code.trim(), [code]);
  const lines = useMemo(() => trimmedCode.split('\n'), [trimmedCode]);

  // Map common language names
  const langMap: Record<string, string> = {
    'tsx': 'tsx',
    'typescript': 'typescript',
    'ts': 'typescript',
    'javascript': 'javascript',
    'js': 'javascript',
    'sql': 'sql',
    'json': 'json',
    'bash': 'bash',
    'shell': 'bash',
  };

  const shikiLang = langMap[language] || 'typescript';

  useEffect(() => {
    let cancelled = false;

    async function highlight() {
      try {
        // Check if language is supported
        if (!(shikiLang in bundledLanguages)) {
          setHighlightedHtml(null);
          return;
        }

        const html = await codeToHtml(trimmedCode, {
          lang: shikiLang,
          theme: 'github-dark',
        });
        
        if (!cancelled) {
          setHighlightedHtml(html);
        }
      } catch (err) {
        console.warn('Syntax highlighting failed:', err);
        if (!cancelled) {
          setHighlightedHtml(null);
        }
      }
    }

    highlight();
    return () => { cancelled = true; };
  }, [trimmedCode, shikiLang]);

  const handleCopy = async () => {
    await navigator.clipboard.writeText(trimmedCode);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="relative group font-mono">
      {/* Terminal window header */}
      {filename && (
        <div className="flex items-center gap-2 px-3 py-2 bg-[#1a1a1a] border border-[#39ff14]/30 border-b-0">
          <span className="w-3 h-3 rounded-full bg-[#ff5f56]" />
          <span className="w-3 h-3 rounded-full bg-[#ffbd2e]" />
          <span className="w-3 h-3 rounded-full bg-[#27ca40]" />
          <span className="ml-2 text-[#808080] text-xs">{filename}</span>
          <span className="ml-auto text-[#39ff14]/50 text-xs uppercase">{language}</span>
        </div>
      )}

      {/* Code content */}
      <div className={`relative bg-[#0a0a0a] border border-[#39ff14]/30 ${filename ? 'border-t-0' : ''}`}>
        {highlightedHtml ? (
          <div 
            className="shiki-wrapper overflow-x-auto text-sm [&_pre]:!bg-transparent [&_pre]:p-4 [&_pre]:m-0 [&_code]:!text-sm"
            dangerouslySetInnerHTML={{ __html: highlightedHtml }}
          />
        ) : (
          <pre className="p-4 overflow-x-auto text-sm">
            <code>
              {showLineNumbers ? (
                lines.map((line, i) => (
                  <div key={i} className="flex">
                    <span className="w-8 text-[#39ff14]/30 text-right pr-4 select-none flex-shrink-0">
                      {String(i + 1).padStart(2, '0')}
                    </span>
                    <span className="text-[#e0e0e0]">{line || ' '}</span>
                  </div>
                ))
              ) : (
                <span className="text-[#e0e0e0]">{trimmedCode}</span>
              )}
            </code>
          </pre>
        )}

        {/* Copy button */}
        <button
          onClick={handleCopy}
          className="absolute top-3 right-3 px-2 py-1 text-xs bg-[#1a1a1a] border border-[#39ff14]/30 text-[#808080] hover:text-[#39ff14] hover:border-[#39ff14] opacity-0 group-hover:opacity-100 transition-all"
          aria-label="Copy code"
        >
          {copied ? '✓ COPIED' : '⎘ COPY'}
        </button>
      </div>
    </div>
  );
}

// Inline SQL code block for smaller snippets
export function SQLCode({ children }: { children: string }) {
  const [html, setHtml] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    codeToHtml(children.trim(), { lang: 'sql', theme: 'github-dark' })
      .then(result => {
        if (!cancelled) setHtml(result);
      })
      .catch(() => {});
    return () => { cancelled = true; };
  }, [children]);

  if (html) {
    return (
      <span 
        className="inline [&_pre]:!inline [&_pre]:!bg-transparent [&_pre]:!p-0 [&_code]:!text-sm"
        dangerouslySetInnerHTML={{ __html: html }}
      />
    );
  }

  return <code className="text-[#39ff14] font-mono">{children}</code>;
}
