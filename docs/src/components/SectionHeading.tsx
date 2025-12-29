interface SectionHeadingProps {
  id: string;
  badge?: string;
  title: string;
  subtitle?: string;
}

export function SectionHeading({ id, badge, title, subtitle }: SectionHeadingProps) {
  return (
    <div className="text-center mb-12">
      {badge && (
        <div className="inline-block px-4 py-1.5 bg-[#39ff14]/20 border border-[#39ff14]/50 text-[#39ff14] text-sm font-mono mb-4 rounded">
          {badge}
        </div>
      )}
      <h2 
        id={id} 
        className="text-2xl md:text-4xl font-bold text-[#e0e0e0] mb-4 scroll-mt-20"
      >
        {title}
      </h2>
      {subtitle && (
        <p className="text-base text-[#a0a0a0] max-w-2xl mx-auto font-sans">
          {subtitle}
        </p>
      )}
    </div>
  );
}
