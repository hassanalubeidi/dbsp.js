/**
 * Join Projection - SQL Column Mapping for JOIN Results
 * ======================================================
 * 
 * Handles SELECT clause column aliasing for JOIN queries.
 * This is framework-agnostic and can be used by any runtime.
 * 
 * ## The Problem
 * 
 * When executing a JOIN like:
 * ```sql
 * SELECT rfqs.*, signals.model AS signal_model, signals.confidence AS signal_confidence
 * FROM rfqs JOIN signals ON rfqs.issuer = signals.issuer
 * ```
 * 
 * The raw join produces tuples: `[rfqRow, signalRow]`
 * 
 * We need to combine them into a single object with proper aliases:
 * ```typescript
 * { ...rfqRow, signal_model: signalRow.model, signal_confidence: signalRow.confidence }
 * ```
 * 
 * ## Usage
 * 
 * ```typescript
 * import { parseJoinProjection, createJoinProjector } from './join-projection';
 * 
 * const projection = parseJoinProjection(
 *   "SELECT a.*, b.name AS b_name FROM a JOIN b ON a.id = b.id",
 *   "a", "b"
 * );
 * 
 * const projector = createJoinProjector(projection);
 * 
 * // Combine join results
 * const result = projector(leftRow, rightRow);
 * ```
 * 
 * @module
 */

// ═══════════════════════════════════════════════════════════════════════════════
// TYPES
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Column mapping from source column to output alias
 */
export interface ColumnMapping {
  /** Source table name (lowercase) */
  table: string;
  /** Source column name */
  column: string;
  /** Output alias (defaults to column name if not specified) */
  alias: string;
}

/**
 * Complete projection spec for a JOIN query
 */
export interface JoinProjection {
  /** Left table name (lowercase) */
  leftTable: string;
  /** Right table name (lowercase) */
  rightTable: string;
  /** Whether left table uses SELECT * */
  leftSelectAll: boolean;
  /** Whether right table uses SELECT * */
  rightSelectAll: boolean;
  /** Explicit column mappings from SELECT clause */
  mappings: ColumnMapping[];
}

// ═══════════════════════════════════════════════════════════════════════════════
// PARSING
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Parse a SQL SELECT clause to extract column projections for a JOIN.
 * 
 * Handles:
 * - `table.*` - Include all columns from table
 * - `table.column AS alias` - Map column to alias
 * - `table.column` - Use column name as-is
 * 
 * @param query - Full SQL query string
 * @param leftTable - Name of the left (first) table in the JOIN
 * @param rightTable - Name of the right (second) table in the JOIN
 * @returns JoinProjection spec
 */
export function parseJoinProjection(
  query: string,
  leftTable: string,
  rightTable: string
): JoinProjection {
  const leftLower = leftTable.toLowerCase();
  const rightLower = rightTable.toLowerCase();
  
  const result: JoinProjection = {
    leftTable: leftLower,
    rightTable: rightLower,
    leftSelectAll: false,
    rightSelectAll: false,
    mappings: [],
  };
  
  // Extract SELECT clause (between SELECT and FROM)
  const selectMatch = query.match(/SELECT\s+([\s\S]+?)\s+FROM\s+/i);
  if (!selectMatch) {
    // No SELECT clause found - default to all columns from both tables
    result.leftSelectAll = true;
    result.rightSelectAll = true;
    return result;
  }
  
  const selectClause = selectMatch[1];
  
  // Split by comma (handling nested parentheses for functions)
  const columns = splitSelectColumns(selectClause);
  
  for (const col of columns) {
    const trimmed = col.trim();
    
    // Check for table.* pattern
    const starMatch = trimmed.match(/^(\w+)\.\*$/);
    if (starMatch) {
      const table = starMatch[1].toLowerCase();
      if (table === leftLower) result.leftSelectAll = true;
      else if (table === rightLower) result.rightSelectAll = true;
      continue;
    }
    
    // Check for table.column AS alias pattern
    const aliasMatch = trimmed.match(/(\w+)\.(\w+)\s+AS\s+(\w+)/i);
    if (aliasMatch) {
      const [, table, column, alias] = aliasMatch;
      result.mappings.push({
        table: table.toLowerCase(),
        column,
        alias,
      });
      continue;
    }
    
    // Check for table.column pattern (no alias - use column name)
    const colMatch = trimmed.match(/^(\w+)\.(\w+)$/);
    if (colMatch) {
      const [, table, column] = colMatch;
      result.mappings.push({
        table: table.toLowerCase(),
        column,
        alias: column, // No alias - use column name
      });
    }
  }
  
  return result;
}

/**
 * Split SELECT columns by comma, handling nested parentheses
 */
function splitSelectColumns(selectClause: string): string[] {
  const columns: string[] = [];
  let depth = 0;
  let current = '';
  
  for (const char of selectClause) {
    if (char === '(') {
      depth++;
      current += char;
    } else if (char === ')') {
      depth--;
      current += char;
    } else if (char === ',' && depth === 0) {
      if (current.trim()) {
        columns.push(current.trim());
      }
      current = '';
    } else {
      current += char;
    }
  }
  
  if (current.trim()) {
    columns.push(current.trim());
  }
  
  return columns;
}

// ═══════════════════════════════════════════════════════════════════════════════
// PROJECTION FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Create a projector function that combines left and right rows into a single result.
 * 
 * This is the main function to use - it returns a reusable function that
 * efficiently applies the projection to each join result pair.
 * 
 * @param projection - JoinProjection spec from parseJoinProjection
 * @returns Function that takes (leftRow, rightRow) and returns combined result
 */
export function createJoinProjector<TLeft extends Record<string, unknown>, TRight extends Record<string, unknown>, TOut extends Record<string, unknown>>(
  projection: JoinProjection
): (left: TLeft, right: TRight) => TOut {
  // Pre-compute which mappings apply to which table for efficiency
  const leftMappings = projection.mappings.filter(m => m.table === projection.leftTable);
  const rightMappings = projection.mappings.filter(m => m.table === projection.rightTable);
  
  return (left: TLeft, right: TRight): TOut => {
    const result: Record<string, unknown> = {};
    
    // Process left table
    if (projection.leftSelectAll) {
      // Copy all columns from left (without table prefix)
      Object.assign(result, left);
    }
    
    // Process right table
    if (projection.rightSelectAll) {
      // Copy all columns from right (without table prefix)
      Object.assign(result, right);
    }
    
    // Apply explicit left table mappings
    for (const mapping of leftMappings) {
      if (mapping.column in left) {
        result[mapping.alias] = left[mapping.column];
      }
    }
    
    // Apply explicit right table mappings
    for (const mapping of rightMappings) {
      if (mapping.column in right) {
        result[mapping.alias] = right[mapping.column];
      }
    }
    
    return result as TOut;
  };
}

/**
 * Apply join projection to a single pair of rows.
 * 
 * For better performance when processing many rows, use createJoinProjector
 * to create a reusable function instead.
 * 
 * @param left - Left row from join
 * @param right - Right row from join
 * @param projection - JoinProjection spec
 * @returns Combined result with proper aliases
 */
export function applyJoinProjection<TOut extends Record<string, unknown>>(
  left: Record<string, unknown>,
  right: Record<string, unknown>,
  projection: JoinProjection
): TOut {
  const projector = createJoinProjector<Record<string, unknown>, Record<string, unknown>, TOut>(projection);
  return projector(left, right);
}

// ═══════════════════════════════════════════════════════════════════════════════
// CONVENIENCE FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Extract table names from a JOIN query.
 * 
 * @param query - SQL query with JOIN
 * @returns Tuple of [leftTable, rightTable] or null if not a join
 */
export function extractJoinTables(query: string): [string, string] | null {
  // Match: FROM leftTable JOIN rightTable ON ...
  const joinMatch = query.match(/FROM\s+(\w+)\s+(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+|CROSS\s+)?JOIN\s+(\w+)/i);
  if (joinMatch) {
    return [joinMatch[1], joinMatch[2]];
  }
  return null;
}

/**
 * Quick check if a query contains a JOIN
 */
export function isJoinQuery(query: string): boolean {
  return /\bJOIN\b/i.test(query);
}

/**
 * Parse and create a projector in one step.
 * 
 * Convenience function that combines parseJoinProjection and createJoinProjector.
 * 
 * @param query - Full SQL query
 * @param leftTable - Left table name
 * @param rightTable - Right table name
 * @returns Projector function or null if not a valid join query
 */
export function createProjectorFromQuery<TLeft extends Record<string, unknown>, TRight extends Record<string, unknown>, TOut extends Record<string, unknown>>(
  query: string,
  leftTable: string,
  rightTable: string
): (left: TLeft, right: TRight) => TOut {
  const projection = parseJoinProjection(query, leftTable, rightTable);
  return createJoinProjector(projection);
}

