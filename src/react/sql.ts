/**
 * interpolateSQL - SQL Parameter Interpolation Utility
 * ====================================================
 * 
 * A simple utility to inject filter values into SQL queries using $param syntax.
 * Use with `useMemo` + `useDBSPView` for dynamic filtering.
 * 
 * ## Usage Pattern
 * 
 * ```tsx
 * import { useDBSPView, interpolateSQL } from 'dbsp/react';
 * 
 * function FilteredTable() {
 *   const rfqs = useDBSPSource<RFQ>({ name: 'rfqs', key: 'rfqId' });
 *   
 *   // Local filter state
 *   const [sector, setSector] = useState('ALL');
 *   const [minNotional, setMinNotional] = useState(0);
 *   
 *   // Interpolate filters into SQL (memoized)
 *   const query = useMemo(() => interpolateSQL(
 *     `SELECT * FROM rfqs 
 *      WHERE ($sector = 'ALL' OR sector = $sector)
 *        AND notional >= $minNotional`,
 *     { sector, minNotional }
 *   ), [sector, minNotional]);
 *   
 *   // Create the view
 *   const filtered = useDBSPView(rfqs, query, { outputKey: 'rfqId' });
 *   
 *   return (
 *     <div>
 *       <select value={sector} onChange={e => setSector(e.target.value)}>
 *         <option value="ALL">All Sectors</option>
 *         <option value="TECH">Technology</option>
 *       </select>
 *       
 *       <p>{filtered.count} matching RFQs</p>
 *     </div>
 *   );
 * }
 * ```
 * 
 * ## The ($param = 'ALL' OR column = $param) Pattern
 * 
 * This pattern allows "show all" functionality:
 * - When filter is 'ALL': `('ALL' = 'ALL')` → true → all rows pass
 * - When filter is 'TECH': `('TECH' = 'ALL')` → false → checks `column = 'TECH'`
 * 
 * @module
 */

// ═══════════════════════════════════════════════════════════════════════════════
// TYPES
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Parameter values can be strings, numbers, booleans, or arrays (for IN clauses)
 */
export type SQLParamValue = string | number | boolean | null | Array<string | number>;

/**
 * Record of parameter name -> value
 */
export type SQLParams = Record<string, SQLParamValue>;

// Legacy aliases for backwards compatibility
export type FilterValue = SQLParamValue;
export type FilterRecord = SQLParams;

// ═══════════════════════════════════════════════════════════════════════════════
// SQL PARAMETER INTERPOLATION
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Interpolate parameter values into a SQL query.
 * 
 * Use with `useMemo` to create dynamic SQL queries that update when params change.
 * 
 * ## Supported Syntax
 * 
 * - `$param` - Simple value substitution
 * - `($param = 'ALL' OR column = $param)` - "Show all" pattern
 * - `column IN ($arrayParam)` - Array expansion for IN clauses
 * 
 * ## Examples
 * 
 * ```tsx
 * // Simple filter
 * interpolateSQL("SELECT * FROM t WHERE status = $status", { status: 'ACTIVE' })
 * // → "SELECT * FROM t WHERE status = 'ACTIVE'"
 * 
 * // "Show all" pattern
 * interpolateSQL("SELECT * FROM t WHERE ($s = 'ALL' OR sector = $s)", { s: 'ALL' })
 * // → "SELECT * FROM t WHERE ('ALL' = 'ALL' OR sector = 'ALL')"
 * // The literal comparison 'ALL' = 'ALL' evaluates to true, so all rows pass
 * 
 * // Numeric filter
 * interpolateSQL("SELECT * FROM t WHERE amount >= $min", { min: 1000 })
 * // → "SELECT * FROM t WHERE amount >= 1000"
 * 
 * // Array for IN clause
 * interpolateSQL("SELECT * FROM t WHERE id IN ($ids)", { ids: [1, 2, 3] })
 * // → "SELECT * FROM t WHERE id IN (1, 2, 3)"
 * ```
 * 
 * @param query - SQL query with $param placeholders
 * @param params - Parameter values to interpolate
 * @returns SQL query with values substituted
 */
export function interpolateSQL(query: string, params: SQLParams): string {
  let result = query;
  
  // Sort keys by length (longest first) to avoid partial replacements
  // e.g., $minNotional should be replaced before $min
  const sortedKeys = Object.keys(params).sort((a, b) => b.length - a.length);
  
  for (const key of sortedKeys) {
    const value = params[key];
    const placeholder = `$${key}`;
    
    if (!result.includes(placeholder)) continue;
    
    // Handle array values (for IN clauses)
    if (Array.isArray(value)) {
      const escaped = value.map(v => 
        typeof v === 'string' ? `'${v.replace(/'/g, "''")}'` : String(v)
      ).join(', ');
      result = result.split(placeholder).join(escaped);
    }
    // Handle null
    else if (value === null) {
      result = result.split(placeholder).join('NULL');
    }
    // Handle strings
    else if (typeof value === 'string') {
      // Escape single quotes in strings
      const escaped = `'${value.replace(/'/g, "''")}'`;
      result = result.split(placeholder).join(escaped);
    }
    // Handle numbers and booleans
    else {
      result = result.split(placeholder).join(String(value));
    }
  }
  
  return result;
}

