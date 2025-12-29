import { useState } from 'react';
import { CodeBlock } from '../components/CodeBlock';
import { SectionHeading } from '../components/SectionHeading';

interface Param {
  name: string;
  type: string;
  description: string;
  required?: boolean;
  default?: string;
}

interface APIDoc {
  name: string;
  signature: string;
  description: string;
  params: Param[];
  returns: { type: string; description: string };
  examples: { title: string; code: string }[];
  notes?: string[];
}

const coreHooks: APIDoc[] = [
  {
    name: 'useDBSPSource',
    signature: 'useDBSPSource<T>(options): DBSPSource<T>',
    description: 'Creates a reactive data source (like a database table) that automatically tracks insertions, updates, and deletions. Data pushed to this source can be queried using SQL.',
    params: [
      { 
        name: 'name', 
        type: 'string', 
        description: 'Unique identifier for this source. Used as the table name in SQL queries.', 
        required: true 
      },
      { 
        name: 'key', 
        type: 'keyof T', 
        description: 'Field to use as primary key. Duplicate keys will update existing rows.', 
        required: true 
      },
      { 
        name: 'schema', 
        type: 'Schema<T>', 
        description: 'Optional type hints for columns. Helps with aggregation type inference.' 
      },
      { 
        name: 'maxRows', 
        type: 'number', 
        description: 'Maximum rows to keep. Oldest rows evicted first (FIFO).',
        default: 'unlimited'
      },
      { 
        name: 'worker', 
        type: '{ enabled: boolean }', 
        description: 'Enable Web Worker mode for heavy data processing.',
        default: '{ enabled: false }'
      },
    ],
    returns: {
      type: 'DBSPSource<T>',
      description: 'Object with methods to manipulate the data source.'
    },
    examples: [
      {
        title: 'Basic usage',
        code: `const sensors = useDBSPSource<SensorReading>({
  name: 'sensors',
  key: 'sensorId',
});

// Insert single row
sensors.push({ sensorId: 's1', temp: 25 });

// Insert multiple rows
sensors.push([
  { sensorId: 's2', temp: 28 },
  { sensorId: 's3', temp: 32 }
]);

// Update (upsert - same key overwrites)
sensors.push({ sensorId: 's1', temp: 30 });

// Delete by key
sensors.remove('s1');

// Delete multiple
sensors.remove('s2', 's3');

// Clear all
sensors.clear();`
      },
    ],
    notes: [
      'Key field must be unique across all rows',
      'Push with same key will update the existing row',
    ]
  },
  {
    name: 'useDBSPView',
    signature: 'useDBSPView<T, R>(sources, sql, options?): DBSPView<R>',
    description: 'Creates a SQL view that automatically updates when the underlying source(s) change. Supports full SQL including JOINs, aggregations, window functions, subqueries, and set operations.',
    params: [
      { 
        name: 'sources', 
        type: 'DBSPSource | DBSPSource[]', 
        description: 'One or more data sources to query. For JOINs, pass an array.', 
        required: true 
      },
      { 
        name: 'sql', 
        type: 'string', 
        description: 'SQL query. Table names should match source "name" fields.', 
        required: true 
      },
      { 
        name: 'joinMode', 
        type: '"standard" | "append-only"', 
        description: 'Use "append-only" for event streams where data is never deleted.',
        default: '"standard"'
      },
    ],
    returns: {
      type: 'DBSPView<R>',
      description: 'Object containing the query results that updates automatically.'
    },
    examples: [
      {
        title: 'Filter',
        code: `const hot = useDBSPView(sensors,
  "SELECT * FROM sensors WHERE temp > 35"
);`
      },
      {
        title: 'JOIN',
        code: `const alerts = useDBSPView([sensors, zones],
  \`SELECT s.*, z.name as zoneName
   FROM sensors s
   JOIN zones z ON s.zoneId = z.zoneId
   WHERE s.temp > 35\`
);`
      },
      {
        title: 'Aggregation with HAVING',
        code: `const stats = useDBSPView(sensors,
  \`SELECT zoneId, AVG(temp) as avgTemp, COUNT(*) as cnt
   FROM sensors 
   GROUP BY zoneId
   HAVING COUNT(*) > 5\`
);`
      },
      {
        title: 'Window Function',
        code: `const withMovingAvg = useDBSPView(sensors,
  \`SELECT *, 
    AVG(temp) OVER (
      PARTITION BY zoneId 
      ORDER BY timestamp 
      ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    ) as movingAvg
   FROM sensors\`
);`
      }
    ],
    notes: [
      'Views update automatically — no manual refresh needed',
      'Use backticks for multi-line SQL',
    ]
  },
];

const returnTypes = [
  {
    name: 'DBSPSource<T>',
    description: 'Returned by useDBSPSource. Represents a mutable data collection.',
    properties: [
      { name: 'push(row)', description: 'Insert a new row (or update if key exists)' },
      { name: 'remove(...keys)', description: 'Remove rows by their keys' },
      { name: 'clear()', description: 'Remove all rows' },
      { name: 'totalRows', description: 'Number of rows in the source' },
      { name: 'ready', description: 'Whether the source is initialized' },
      { name: 'stats', description: 'Performance metrics (lastUpdateMs, totalUpdates, etc.)' },
    ]
  },
  {
    name: 'DBSPView<R>',
    description: 'Returned by useDBSPView. A read-only query result.',
    properties: [
      { name: 'results', description: 'Array of rows matching the query' },
      { name: 'count', description: 'Number of rows in the result' },
      { name: 'stats', description: 'Performance metrics (lastUpdateMs, totalUpdates, etc.)' },
      { name: 'ready', description: 'Whether the view is initialized' },
    ]
  }
];

// Comprehensive SQL reference organized by category
const sqlCategories = [
  {
    name: 'SELECT Clauses',
    color: '#39ff14',
    items: [
      { feature: 'SELECT columns', example: 'SELECT id, name, price', description: 'Select specific columns' },
      { feature: 'SELECT *', example: 'SELECT * FROM orders', description: 'Select all columns' },
      { feature: 'Aliases (AS)', example: 'SELECT price * qty AS total', description: 'Rename columns in output' },
      { feature: 'DISTINCT', example: 'SELECT DISTINCT category', description: 'Remove duplicate rows' },
      { feature: 'Expressions', example: 'SELECT price * 1.1 AS withTax', description: 'Arithmetic expressions (+, -, *, /)' },
      { feature: 'CASE WHEN', example: 'SELECT CASE WHEN temp > 30 THEN \'hot\' ELSE \'ok\' END', description: 'Conditional expressions' },
      { feature: 'CAST', example: 'SELECT CAST(price AS INT)', description: 'Type conversions' },
      { feature: 'Functions', example: 'SELECT COALESCE(name, \'Unknown\')', description: 'Built-in functions' },
    ]
  },
  {
    name: 'Aggregate Functions',
    color: '#ffb000',
    items: [
      { feature: 'COUNT(*)', example: 'SELECT COUNT(*) FROM orders', description: 'Count all rows' },
      { feature: 'COUNT(column)', example: 'SELECT COUNT(orderId)', description: 'Count non-null values' },
      { feature: 'COUNT(DISTINCT)', example: 'SELECT COUNT(DISTINCT customerId)', description: 'Count unique values' },
      { feature: 'SUM', example: 'SELECT SUM(amount)', description: 'Sum of values' },
      { feature: 'AVG', example: 'SELECT AVG(price)', description: 'Average of values' },
      { feature: 'MIN', example: 'SELECT MIN(timestamp)', description: 'Minimum value' },
      { feature: 'MAX', example: 'SELECT MAX(score)', description: 'Maximum value' },
      { feature: 'Expressions in aggregates', example: 'SELECT SUM(price * quantity)', description: 'Complex aggregate arguments' },
      { feature: 'CASE in aggregates', example: 'SELECT SUM(CASE WHEN status=\'DONE\' THEN 1 ELSE 0 END)', description: 'Conditional counting' },
    ]
  },
  {
    name: 'Window Functions',
    color: '#00ffff',
    items: [
      { feature: 'ROW_NUMBER()', example: 'ROW_NUMBER() OVER (ORDER BY id)', description: 'Sequential row numbering' },
      { feature: 'LAG()', example: 'LAG(price, 1) OVER (ORDER BY date)', description: 'Previous row value' },
      { feature: 'LEAD()', example: 'LEAD(price, 1) OVER (ORDER BY date)', description: 'Next row value' },
      { feature: 'Aggregate OVER', example: 'SUM(amount) OVER (PARTITION BY customer)', description: 'Running aggregates' },
      { feature: 'PARTITION BY', example: 'OVER (PARTITION BY category)', description: 'Group window by column' },
      { feature: 'ORDER BY in OVER', example: 'OVER (ORDER BY timestamp DESC)', description: 'Order within window' },
      { feature: 'ROWS BETWEEN', example: 'ROWS BETWEEN 5 PRECEDING AND CURRENT ROW', description: 'Sliding window frame' },
      { feature: 'RANGE BETWEEN', example: 'RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW', description: 'Value-based window frame' },
    ]
  },
  {
    name: 'JOINs',
    color: '#ff00ff',
    items: [
      { feature: 'INNER JOIN', example: 'FROM a JOIN b ON a.id = b.aId', description: 'Matching rows from both tables' },
      { feature: 'LEFT JOIN', example: 'FROM a LEFT JOIN b ON a.id = b.aId', description: 'All from left, matching from right' },
      { feature: 'RIGHT JOIN', example: 'FROM a RIGHT JOIN b ON a.id = b.aId', description: 'All from right, matching from left' },
      { feature: 'FULL JOIN', example: 'FROM a FULL JOIN b ON a.id = b.aId', description: 'All rows from both tables' },
      { feature: 'CROSS JOIN', example: 'FROM a CROSS JOIN b', description: 'Cartesian product' },
      { feature: 'Multi-table', example: 'FROM a JOIN b ON ... JOIN c ON ...', description: 'Join multiple tables' },
    ]
  },
  {
    name: 'WHERE Conditions',
    color: '#39ff14',
    items: [
      { feature: 'Comparison', example: 'WHERE price > 100', description: 'Operators: =, !=, <>, <, >, <=, >=' },
      { feature: 'AND / OR', example: 'WHERE a > 1 AND b < 10', description: 'Logical operators' },
      { feature: 'NOT', example: 'WHERE NOT active', description: 'Negation' },
      { feature: 'BETWEEN', example: 'WHERE price BETWEEN 10 AND 50', description: 'Range check' },
      { feature: 'IN (values)', example: 'WHERE status IN (\'active\', \'pending\')', description: 'Match any in list' },
      { feature: 'IN (subquery)', example: 'WHERE id IN (SELECT id FROM other)', description: 'Subquery match' },
      { feature: 'LIKE', example: 'WHERE name LIKE \'%sensor%\'', description: 'Pattern matching' },
      { feature: 'IS NULL', example: 'WHERE deletedAt IS NULL', description: 'Null check' },
      { feature: 'IS NOT NULL', example: 'WHERE email IS NOT NULL', description: 'Not null check' },
      { feature: 'EXISTS', example: 'WHERE EXISTS (SELECT 1 FROM orders WHERE ...)', description: 'Subquery exists check' },
    ]
  },
  {
    name: 'Grouping & Ordering',
    color: '#ffb000',
    items: [
      { feature: 'GROUP BY', example: 'GROUP BY category', description: 'Group rows for aggregation' },
      { feature: 'GROUP BY multiple', example: 'GROUP BY category, region', description: 'Multi-column grouping' },
      { feature: 'HAVING', example: 'HAVING COUNT(*) > 5', description: 'Filter groups by aggregate' },
      { feature: 'ORDER BY', example: 'ORDER BY price DESC', description: 'Sort results' },
      { feature: 'ORDER BY multiple', example: 'ORDER BY category, price DESC', description: 'Multi-column sort' },
      { feature: 'LIMIT', example: 'LIMIT 10', description: 'Limit result count' },
      { feature: 'OFFSET', example: 'LIMIT 10 OFFSET 20', description: 'Skip rows (pagination)' },
    ]
  },
  {
    name: 'Set Operations',
    color: '#00ffff',
    items: [
      { feature: 'UNION', example: 'SELECT ... UNION SELECT ...', description: 'Combine results (deduplicated)' },
      { feature: 'UNION ALL', example: 'SELECT ... UNION ALL SELECT ...', description: 'Combine results (with duplicates)' },
      { feature: 'EXCEPT', example: 'SELECT ... EXCEPT SELECT ...', description: 'Rows in first but not second' },
      { feature: 'EXCEPT ALL', example: 'SELECT ... EXCEPT ALL SELECT ...', description: 'Except with duplicates' },
      { feature: 'INTERSECT', example: 'SELECT ... INTERSECT SELECT ...', description: 'Rows in both queries' },
      { feature: 'INTERSECT ALL', example: 'SELECT ... INTERSECT ALL SELECT ...', description: 'Intersect with duplicates' },
    ]
  },
  {
    name: 'Built-in Functions',
    color: '#ff00ff',
    items: [
      { feature: 'COALESCE', example: 'COALESCE(name, \'default\')', description: 'First non-null value' },
      { feature: 'NULLIF', example: 'NULLIF(a, b)', description: 'Returns null if a = b' },
      { feature: 'ABS', example: 'ABS(value)', description: 'Absolute value' },
      { feature: 'IF', example: 'IF(cond, then, else)', description: 'Conditional value' },
    ]
  },
];

export function APIReference() {
  const [expandedApi, setExpandedApi] = useState<string | null>('useDBSPSource');
  const [expandedCategory, setExpandedCategory] = useState<string | null>('SELECT Clauses');

  return (
    <section id="api-reference" className="relative py-20 bg-[#0a0a0a] overflow-hidden">
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
      <div className="relative z-10 max-w-5xl mx-auto px-4 sm:px-6 lg:px-8 font-mono">
        
        <SectionHeading
          id="api-heading"
          badge="Reference"
          title="API Documentation"
          subtitle="Complete reference for DBSP React hooks and SQL support"
        />

        {/* Core Hooks */}
        <div className="mb-16">
          <div className="text-[#39ff14] text-lg mb-6">═══ REACT HOOKS ═══</div>
          
          <div className="space-y-4">
            {coreHooks.map((api) => (
              <div
                key={api.name}
                className="bg-[#0d1117] border border-[#39ff14]/30 overflow-hidden"
              >
                {/* Header */}
                <button
                  onClick={() => setExpandedApi(expandedApi === api.name ? null : api.name)}
                  className="w-full p-4 text-left hover:bg-[#39ff14]/5 transition-colors"
                >
                  <div className="flex items-center justify-between">
                    <div>
                      <span className="text-[#00ffff] text-lg font-bold">{api.name}</span>
                      <code className="text-sm text-[#808080] ml-3">{api.signature}</code>
                    </div>
                    <span className="text-[#39ff14] text-xl">
                      {expandedApi === api.name ? '−' : '+'}
                    </span>
                  </div>
                  <p className="text-[#a0a0a0] mt-2 text-sm">{api.description}</p>
                </button>

                {/* Expanded content */}
                {expandedApi === api.name && (
                  <div className="px-4 pb-4 space-y-6 border-t border-[#39ff14]/20 bg-[#0a0a0a]">
                    
                    {/* Parameters */}
                    <div className="pt-4">
                      <div className="text-[#ffb000] text-sm mb-3">┌─ PARAMETERS ─┐</div>
                      <div className="space-y-3">
                        {api.params.map((param) => (
                          <div key={param.name} className="p-3 bg-[#0d1117] border-l-2 border-[#39ff14]/50">
                            <div className="flex items-center gap-3 mb-1">
                              <code className="text-[#e0e0e0] font-bold">{param.name}</code>
                              <code className="text-xs text-[#ff00ff] bg-[#ff00ff]/10 px-2 py-0.5">{param.type}</code>
                              {param.required && (
                                <span className="text-xs text-[#ffb000]">required</span>
                              )}
                            </div>
                            <p className="text-sm text-[#808080]">{param.description}</p>
                            {param.default && (
                              <p className="text-xs text-[#808080] mt-1">
                                Default: <code className="text-[#39ff14]">{param.default}</code>
                              </p>
                            )}
                          </div>
                        ))}
                      </div>
                    </div>

                    {/* Returns */}
                    <div>
                      <div className="text-[#00ffff] text-sm mb-3">┌─ RETURNS ─┐</div>
                      <div className="p-3 bg-[#0d1117] border-l-2 border-[#00ffff]/50">
                        <code className="text-[#00ffff]">{api.returns.type}</code>
                        <span className="text-[#808080] ml-3">{api.returns.description}</span>
                      </div>
                    </div>

                    {/* Examples */}
                    <div>
                      <div className="text-[#39ff14] text-sm mb-3">┌─ EXAMPLES ─┐</div>
                      <div className="space-y-4">
                        {api.examples.map((ex, i) => (
                          <div key={i}>
                            <p className="text-xs text-[#808080] mb-2">{ex.title}</p>
                            <CodeBlock code={ex.code} language="typescript" />
                          </div>
                        ))}
                      </div>
                    </div>

                    {/* Notes */}
                    {api.notes && (
                      <div className="p-3 bg-[#ffb000]/10 border border-[#ffb000]/30">
                        <div className="text-[#ffb000] text-sm mb-2">💡 Tips</div>
                        <ul className="text-sm text-[#e0e0e0] space-y-1">
                          {api.notes.map((note, i) => (
                            <li key={i}>• {note}</li>
                          ))}
                        </ul>
                      </div>
                    )}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>

        {/* Return Types */}
        <div className="mb-16">
          <div className="text-[#00ffff] text-lg mb-6">═══ RETURN TYPES ═══</div>
          
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {returnTypes.map((obj) => (
              <div key={obj.name} className="p-4 bg-[#0d1117] border border-[#00ffff]/30">
                <div className="text-[#00ffff] font-bold mb-2">{obj.name}</div>
                <p className="text-sm text-[#808080] mb-4">{obj.description}</p>
                <div className="space-y-2">
                  {obj.properties.map((prop) => (
                    <div key={prop.name} className="flex items-start gap-3 text-sm">
                      <code className="text-[#39ff14] min-w-[120px]">.{prop.name}</code>
                      <span className="text-[#808080]">{prop.description}</span>
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* SQL Reference - Categorized */}
        <div className="mb-8">
          <div className="text-[#ff00ff] text-lg mb-6">═══ SUPPORTED SQL ═══</div>
          <p className="text-[#808080] text-sm mb-6">
            DBSP supports comprehensive SQL syntax. Click a category to explore.
          </p>
          
          <div className="space-y-3">
            {sqlCategories.map((cat) => (
              <div 
                key={cat.name}
                className="bg-[#0d1117] border overflow-hidden"
                style={{ borderColor: `${cat.color}50` }}
              >
                <button
                  onClick={() => setExpandedCategory(expandedCategory === cat.name ? null : cat.name)}
                  className="w-full p-4 text-left hover:bg-white/5 transition-colors flex items-center justify-between"
                >
                  <span className="font-bold" style={{ color: cat.color }}>{cat.name}</span>
                  <div className="flex items-center gap-3">
                    <span className="text-[#808080] text-sm">{cat.items.length} features</span>
                    <span style={{ color: cat.color }}>
                      {expandedCategory === cat.name ? '−' : '+'}
                    </span>
                  </div>
                </button>
                
                {expandedCategory === cat.name && (
                  <div className="border-t bg-[#0a0a0a] p-4" style={{ borderColor: `${cat.color}30` }}>
                    <div className="overflow-x-auto">
                      <table className="w-full text-sm">
                        <thead>
                          <tr className="text-left">
                            <th className="pb-2 pr-4 text-[#808080] font-normal">Feature</th>
                            <th className="pb-2 pr-4 text-[#808080] font-normal">Example</th>
                            <th className="pb-2 text-[#808080] font-normal">Description</th>
                          </tr>
                        </thead>
                        <tbody>
                          {cat.items.map((item, i) => (
                            <tr key={i} className="border-t border-[#333]">
                              <td className="py-2 pr-4 font-bold" style={{ color: cat.color }}>
                                {item.feature}
                              </td>
                              <td className="py-2 pr-4">
                                <code className="text-[#a0a0a0] text-xs">{item.example}</code>
                              </td>
                              <td className="py-2 text-[#808080]">{item.description}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>

        {/* Quick Examples */}
        <div className="mt-16">
          <div className="text-[#39ff14] text-lg mb-6">═══ REAL-WORLD EXAMPLES ═══</div>
          
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div>
              <div className="text-[#ffb000] text-sm mb-2">Rolling Average (Window)</div>
              <CodeBlock 
                code={`const withRolling = useDBSPView(trades,
  \`SELECT *,
    AVG(price) OVER (
      PARTITION BY symbol
      ORDER BY timestamp
      ROWS BETWEEN 10 PRECEDING 
        AND CURRENT ROW
    ) as rollingAvg
   FROM trades\`
);`}
                language="sql"
              />
            </div>
            
            <div>
              <div className="text-[#00ffff] text-sm mb-2">Conditional Aggregation</div>
              <CodeBlock 
                code={`const summary = useDBSPView(orders,
  \`SELECT 
    customerId,
    COUNT(*) as totalOrders,
    SUM(CASE WHEN status='DONE' 
        THEN 1 ELSE 0 END) as completed,
    SUM(amount) as totalSpent
   FROM orders
   GROUP BY customerId
   HAVING COUNT(*) > 5\`
);`}
                language="sql"
              />
            </div>
            
            <div>
              <div className="text-[#ff00ff] text-sm mb-2">Multi-Table JOIN</div>
              <CodeBlock 
                code={`const report = useDBSPView(
  [orders, customers, products],
  \`SELECT 
    c.name as customer,
    p.title as product,
    o.quantity,
    o.quantity * p.price as total
   FROM orders o
   JOIN customers c ON o.custId = c.id
   JOIN products p ON o.prodId = p.id
   WHERE o.status = 'PENDING'\`
);`}
                language="sql"
              />
            </div>
            
            <div>
              <div className="text-[#39ff14] text-sm mb-2">Subquery with EXISTS</div>
              <CodeBlock 
                code={`const activeCustomers = useDBSPView(
  [customers, orders],
  \`SELECT * FROM customers c
   WHERE EXISTS (
     SELECT 1 FROM orders o
     WHERE o.customerId = c.id
       AND o.date > '2024-01-01'
   )\`
);`}
                language="sql"
              />
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
