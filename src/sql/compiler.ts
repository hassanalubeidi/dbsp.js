/**
 * SQL Compiler
 * =============
 * 
 * Compiles parsed SQL AST into DBSP circuits.
 * 
 * Key insights from DBSP paper:
 * - Filter is a LINEAR operator, so filter^Δ = filter (works directly on deltas)
 * - Map/Projection is LINEAR, so map^Δ = map
 * - Join is BILINEAR: requires special handling for incrementality
 * - Aggregations require integration to maintain full state
 * 
 * @module
 */

import { Circuit, StreamHandle } from '../internals/circuit';
import { ZSet } from '../internals/zset';
import { PartitionedWindowState, type WindowFunctionSpec } from '../internals/window-state';

import type {
  Query,
  SelectQuery,
  SetOperationQuery,
  WithQuery,
  SimpleColumn,
  AggregateColumn,
  ExpressionColumn,
  FunctionColumn,
  WindowColumn,
  ScalarSubqueryColumn,
  WhereCondition,
  ComparisonCondition,
  AndCondition,
  OrCondition,
  BetweenCondition,
  InCondition,
  IsNullCondition,
  IsNotNullCondition,
  NotCondition,
  LikeCondition,
  RegexpCondition,
  ExistsCondition,
  ExpressionComparison,
  JoinInfo,
  AggregateArg,
} from './ast-types';

import { SQLParser } from './parser';
import { evaluateAggregateExpr, evaluateFunctionExprGeneric, getExprString, evaluateCaseColumn, getCanonicalAggregateKey } from './expression-eval';

// ============ COMPILE RESULT ============

export interface CompileResult {
  circuit: Circuit;
  tables: Record<string, StreamHandle<any>>;
  views: Record<string, StreamHandle<any>>;
}

// ============ SQL COMPILER CLASS ============

/**
 * SQLCompiler: Compiles parsed SQL into DBSP circuits
 * 
 * Key insights from DBSP paper:
 * - Filter is a LINEAR operator, so filter^Δ = filter (works directly on deltas)
 * - Map/Projection is LINEAR, so map^Δ = map
 * - Join is BILINEAR: requires special handling for incrementality
 * - Aggregations require integration to maintain full state
 */
export class SQLCompiler {
  /**
   * Compile SQL string to a DBSP circuit
   */
  compile(sql: string): CompileResult {
    const parser = new SQLParser();
    const ast = parser.parse(sql);
    
    const circuit = new Circuit();
    const tables: Record<string, StreamHandle<any>> = {};
    const views: Record<string, StreamHandle<any>> = {};
    
    // First pass: Create inputs for all tables
    for (const stmt of ast.statements) {
      if (stmt.type === 'CREATE_TABLE') {
        const keyFn = (row: any) => JSON.stringify(row);
        tables[stmt.tableName] = circuit.input(stmt.tableName, keyFn);
      }
    }
    
    // Second pass: Create views
    // NOTE: Views can reference other views (nested queries)
    // We pass both tables AND previously-created views as available sources
    for (const stmt of ast.statements) {
      if (stmt.type === 'CREATE_VIEW') {
        // Merge tables and views so views can reference other views
        const sources = { ...tables, ...views };
        const view = this.compileQuery(stmt.query, sources, circuit);
        if (view) {
          views[stmt.viewName] = view;
        }
      }
    }
    
    return { circuit, tables, views };
  }

  private compileQuery(
    query: Query,
    tables: Record<string, StreamHandle<any>>,
    circuit: Circuit
  ): StreamHandle<any> | null {
    // Handle WITH clause (CTEs)
    if (query.type === 'WITH') {
      const withQuery = query as WithQuery;
      // Create a new tables map that includes CTEs
      const tablesWithCTEs = { ...tables };
      
      // Process each CTE in order (later CTEs can reference earlier ones)
      for (const cte of withQuery.ctes) {
        const cteResult = this.compileQuery(cte.query, tablesWithCTEs, circuit);
        if (cteResult) {
          tablesWithCTEs[cte.name] = cteResult;
        }
      }
      
      // Compile the main query with CTEs available
      return this.compileQuery(withQuery.query, tablesWithCTEs, circuit);
    }
    
    if (query.type === 'UNION' || query.type === 'EXCEPT' || query.type === 'INTERSECT') {
      return this.compileSetOperation(query as SetOperationQuery, tables, circuit);
    }
    return this.compileSelect(query as SelectQuery, tables, circuit);
  }

  private compileSelect(
    query: SelectQuery,
    tables: Record<string, StreamHandle<any>>,
    circuit: Circuit
  ): StreamHandle<any> | null {
    let stream: StreamHandle<any> | null = null;
    
    // Build alias map for table references
    const aliasMap: Record<string, string> = {};  // alias -> actual table name
    
    // Handle FROM clause (with derived table support)
    if (query.fromRef?.derivedTable) {
      // Derived table in FROM: (SELECT ...) AS alias
      const derivedResult = this.compileQuery(query.fromRef.derivedTable, tables, circuit);
      if (!derivedResult) {
        console.error('Failed to compile derived table in FROM clause');
        return null;
      }
      // Create a virtual table for the derived table
      const alias = query.fromRef.alias || '_derived_';
      const tablesWithDerived = { ...tables, [alias]: derivedResult };
      stream = derivedResult;
      // Use the extended tables for joins
      Object.assign(tables, tablesWithDerived);
    } else {
      // Regular table reference
      const tableName = query.from;
      stream = tables[tableName];
      if (query.fromRef?.alias) {
        aliasMap[query.fromRef.alias] = tableName;
      }
    }
    
    // Handle multiple JOINs (supports 3+ tables!)
    if (query.joins && query.joins.length > 0) {
      // Pass the left table alias (from the FROM clause) for proper column prefixing in self-joins
      const leftTableAlias = query.fromRef?.alias;
      stream = this.compileMultipleJoins(stream, query.joins, tables, circuit, aliasMap, leftTableAlias);
    } else if (query.join) {
      // Legacy single join support
      stream = this.compileJoin(query, tables);
    }
    
    if (!stream) {
      console.error(`Table ${query.from} not found`);
      return null;
    }
    
    // Apply WHERE clause filter (LINEAR operator - works on deltas directly!)
    if (query.where) {
      // Check for IN subquery conditions
      const inSubqueries = this.findInSubqueries(query.where);
      // Check for EXISTS subquery conditions
      const existsSubqueries = this.findExistsSubqueries(query.where);
      
      const hasSubqueries = inSubqueries.length > 0 || existsSubqueries.length > 0;
      
      if (hasSubqueries) {
        // Handle IN subqueries via semi-join
        for (const inCond of inSubqueries) {
          const result = this.compileInSubquery(stream, inCond, tables, circuit);
          if (result) {
            stream = result;
          }
        }
        
        // Handle EXISTS subqueries via semi-join
        for (const existsCond of existsSubqueries) {
          const result = this.compileExistsSubquery(stream, existsCond, tables, circuit);
          if (result) {
            stream = result;
          }
        }
        
        // Apply remaining non-subquery predicates
        const nonSubqueryWhere = this.removeSubqueries(query.where);
        if (nonSubqueryWhere) {
          const predicate = this.compileWhere(nonSubqueryWhere);
          stream = stream.filter(predicate);
        }
      } else {
        const predicate = this.compileWhere(query.where);
        stream = stream.filter(predicate);
      }
    }
    
    // Handle aggregations
    // Check for both top-level aggregates AND CASE expressions (which may contain aggregates)
    const hasAggregates = query.columns.some(
      c => c !== '*' && typeof c === 'object' && (c.type === 'aggregate' || c.type === 'case')
    );
    
    // Check for window functions
    const windowCols = query.columns.filter(
      (c): c is WindowColumn => c !== '*' && typeof c === 'object' && c.type === 'window'
    );
    const hasWindowFunctions = windowCols.length > 0;
    
    if (hasWindowFunctions) {
      // Window functions require special processing
      // Use optimized O(1) algorithms when possible
      const canOptimize = this.canUseOptimizedWindow(windowCols);
      console.log(`[SQL] Window functions: ${windowCols.map(w => w.function).join(', ')}, canOptimize: ${canOptimize}`);
      if (canOptimize) {
        stream = this.compileWindowFunctionsOptimized(stream, query, windowCols, circuit);
      } else {
        stream = this.compileWindowFunctions(stream, query, windowCols, circuit);
      }
    } else if (hasAggregates) {
      stream = this.compileAggregation(stream, query, circuit, tables);
    } else {
      // Apply column projection
      // Note: Even with SELECT *, we may have additional columns with aliases (e.g., SELECT rfqs.*, signals.direction AS signal_direction)
      const hasSelectAll = query.columns.includes('*');
      const simpleCols = query.columns
        .filter((c): c is SimpleColumn => c !== '*' && typeof c === 'object' && c.type === 'column');
      const funcCols = query.columns
        .filter((c): c is FunctionColumn => c !== '*' && typeof c === 'object' && c.type === 'function');
      const exprCols = query.columns
        .filter((c): c is ExpressionColumn => c !== '*' && typeof c === 'object' && c.type === 'expression');
      const scalarSubqueryCols = query.columns
        .filter((c): c is ScalarSubqueryColumn => c !== '*' && typeof c === 'object' && c.type === 'scalar_subquery');
      type LiteralCol = { type: 'literal'; value: number | string | boolean; alias?: string };
      const literalCols = query.columns
        .filter((c): c is LiteralCol => c !== '*' && typeof c === 'object' && c.type === 'literal');
      
      // Pre-compile scalar subqueries (for uncorrelated subqueries, we compute once)
      const scalarSubqueryValues = new Map<string, { stream: StreamHandle<any>; value: any }>();
      for (const col of scalarSubqueryCols) {
        const alias = col.alias || '_scalar_';
        // Compile the subquery using available tables
        const subqueryStream = this.compileQuery(col.query, tables, circuit);
        if (subqueryStream) {
          scalarSubqueryValues.set(alias, { stream: subqueryStream, value: null });
          // Listen for updates to the scalar subquery
          subqueryStream.output((zset: ZSet<any>) => {
            const entries = zset.entries();
            if (entries.length > 0) {
              const [row] = entries[0];
              // Get the first column value (scalar subqueries return single value)
              const keys = Object.keys(row);
              scalarSubqueryValues.get(alias)!.value = keys.length > 0 ? row[keys[0]] : null;
            }
          });
        }
      }
      
      // Need to project if:
      // 1. No SELECT * (selective columns only), or
      // 2. SELECT * with additional columns that need aliases (e.g., SELECT *, col AS alias)
      const needsProjection = !hasSelectAll || simpleCols.length > 0 || funcCols.length > 0 || exprCols.length > 0 || scalarSubqueryCols.length > 0 || literalCols.length > 0;
      
      if (needsProjection) {
        stream = stream.map((row: any) => {
          // If SELECT *, start with all row columns; otherwise, start with empty result
          const result: any = hasSelectAll ? { ...row } : {};
          
          // Simple columns (with possible aliases)
          for (const col of simpleCols) {
            // Try prefixed column first (for self-joins), then fall back to unprefixed
            const prefixedName = col.table ? `${col.table}.${col.name}` : col.name;
            const value = row[prefixedName] !== undefined ? row[prefixedName] : row[col.name];
            result[col.alias || col.name] = value;
          }
            // Function columns (COALESCE, NULLIF, etc.)
            for (const col of funcCols) {
              const alias = col.alias || `${col.function.toLowerCase()}_result`;
              const funcName = col.function.toUpperCase();
              
              if (funcName === 'COALESCE') {
                // Return first non-null value
                let value = null;
                if (col.argExprs) {
                  for (const argExpr of col.argExprs) {
                    // Evaluate each argument
                    const rawVal = argExpr.type === 'column' 
                      ? row[argExpr.column!]
                      : argExpr.stringValue ?? argExpr.value;
                    if (rawVal !== null && rawVal !== undefined) {
                      value = rawVal;
                      break;
                    }
                  }
                }
                result[alias] = value;
              } else if (funcName === 'NULLIF') {
                // NULLIF(a, b) - return null if a == b, else a
                if (col.argExprs && col.argExprs.length >= 2) {
                  const a = col.argExprs[0].type === 'column' 
                    ? row[col.argExprs[0].column!]
                    : col.argExprs[0].stringValue ?? col.argExprs[0].value;
                  const b = col.argExprs[1].type === 'column'
                    ? row[col.argExprs[1].column!]
                    : col.argExprs[1].stringValue ?? col.argExprs[1].value;
                  result[alias] = a === b ? null : a;
                }
              } else {
                // Other functions: use numeric evaluator
                if (col.argExprs && col.argExprs.length > 0) {
                  result[alias] = evaluateFunctionExprGeneric({
                    type: 'function',
                    functionName: funcName,
                    args: col.argExprs,
                  }, row);
                }
              }
            }
            // Expression columns (arithmetic: price * quantity)
            for (const col of exprCols) {
              const alias = col.alias || '_expr_';
              result[alias] = this.evaluateExpression(col, row);
            }
            // Scalar subquery columns
            for (const col of scalarSubqueryCols) {
              const alias = col.alias || '_scalar_';
              const subqueryData = scalarSubqueryValues.get(alias);
              result[alias] = subqueryData?.value ?? null;
            }
            // Literal columns (constant values like "1 AS is_active", "0 AS count")
            for (const col of literalCols) {
              const alias = col.alias || '_literal_';
              result[alias] = col.value;
            }
            return result;
          });
        }
    }
    
    // Apply DISTINCT if specified (remove duplicate rows)
    if (query.distinct && stream) {
      // Use a Set to track seen row keys for deduplication
      const seenRows = new Set<string>();
      
      stream = circuit.addStatefulOperator<any, any>(
        `distinct_${stream.id}`,
        [stream.id],
        (inputs: ZSet<any>[]) => {
          const delta = inputs[0];
          const outputEntries: [any, number][] = [];
          
          for (const [row, weight] of delta.entries()) {
            const rowKey = JSON.stringify(row);
            
            if (weight > 0) {
              // Insert: only emit if we haven't seen this row before
              if (!seenRows.has(rowKey)) {
                seenRows.add(rowKey);
                outputEntries.push([row, 1]);
              }
            } else {
              // Delete: remove from seen set and emit deletion
              if (seenRows.has(rowKey)) {
                seenRows.delete(rowKey);
                outputEntries.push([row, -1]);
              }
            }
          }
          
          return ZSet.fromEntries(outputEntries);
        },
        () => seenRows.clear()
      );
    }
    
    // Apply ORDER BY with LIMIT/OFFSET
    // OPTIMIZED: Use bounded state to prevent O(n) growth
    if ((query.orderBy && query.orderBy.length > 0) || query.limit !== undefined || query.offset !== undefined) {
      const orderBy = query.orderBy || [];
      const limit = query.limit;
      const offset = query.offset || 0;
      
      // Resolve ordinal positions to actual column names
      // e.g., ORDER BY 1 -> ORDER BY first_column
      const resolvedOrderBy = orderBy.map(ord => {
        if (ord.ordinal !== undefined && ord.ordinal > 0) {
          // 1-indexed: get the nth column from SELECT
          const colIdx = ord.ordinal - 1;
          if (colIdx < query.columns.length) {
            const col = query.columns[colIdx];
            if (col === '*') {
              // Can't resolve ordinal for *
              return ord;
            }
            if (typeof col === 'object') {
              // Use alias if present, otherwise use column name
              const colName = (col as any).alias || (col as any).name || '';
              return { column: colName, direction: ord.direction };
            }
          }
        }
        return ord;
      });
      
      // OPTIMIZATION: Only store what we need + buffer for deletions
      // Keep 3x the limit to handle out-of-order arrivals and deletions
      const stateLimit = limit ? Math.max(limit * 3, 500) : 10000;
      
      // State: bounded sorted array + key lookup
      let sortedState: any[] = [];
      const keyToRow = new Map<string, any>();
      
      // Comparison function (memoized)
      const compare = (a: any, b: any): number => {
        for (const ord of resolvedOrderBy) {
          const aVal = a[ord.column];
          const bVal = b[ord.column];
          const cmp = aVal < bVal ? -1 : aVal > bVal ? 1 : 0;
          if (cmp !== 0) {
            return ord.direction === 'DESC' ? -cmp : cmp;
          }
        }
        return 0;
      };
      
      // Binary search for insertion point - O(log n)
      const findInsertIndex = (arr: any[], row: any): number => {
        let low = 0, high = arr.length;
        while (low < high) {
          const mid = (low + high) >>> 1;
          if (compare(arr[mid], row) < 0) {
            low = mid + 1;
          } else {
            high = mid;
          }
        }
        return low;
      };
      
      // Previous output for differential computation
      let previousOutput: any[] = [];
      
      stream = circuit.addStatefulOperator<any, any>(
        `orderby_limit_${stream.id}`,
        [stream.id],
        (inputs: ZSet<any>[]) => {
          const delta = inputs[0];
          const outputEntries: [any, number][] = [];
          
          // Process delta - use binary search for O(log n) insert/delete
          for (const [row, weight] of delta.entries()) {
            const rowKey = JSON.stringify(row);
            
            if (weight > 0) {
              // Insert: find position via binary search
              if (!keyToRow.has(rowKey)) {
                const idx = findInsertIndex(sortedState, row);
                
                // Only insert if it's in the top stateLimit or we have room
                if (idx < stateLimit || sortedState.length < stateLimit) {
                  sortedState.splice(idx, 0, row);
                  keyToRow.set(rowKey, row);
                  
                  // Evict if over limit (remove worst element)
                  if (sortedState.length > stateLimit) {
                    const evicted = sortedState.pop()!;
                    keyToRow.delete(JSON.stringify(evicted));
                  }
                }
              }
            } else {
              // Delete: find and remove
              if (keyToRow.has(rowKey)) {
                keyToRow.delete(rowKey);
                // Find in sorted array - O(log n) search + O(n) splice (but n is bounded)
                const idx = sortedState.findIndex(r => JSON.stringify(r) === rowKey);
                if (idx !== -1) {
                  sortedState.splice(idx, 1);
                }
              }
            }
          }
          
          // Get current output with offset/limit applied
          const start = offset;
          const end = limit !== undefined ? offset + limit : sortedState.length;
          const currentOutput = sortedState.slice(start, end);
          
          // Compute differential output - compare with previous
          const currentKeys = new Set<string>();
          for (const row of currentOutput) {
            currentKeys.add(JSON.stringify(row));
          }
          
          const previousKeys = new Set<string>();
          for (const row of previousOutput) {
            previousKeys.add(JSON.stringify(row));
          }
          
          // Removed rows
          for (const row of previousOutput) {
            const key = JSON.stringify(row);
            if (!currentKeys.has(key)) {
              outputEntries.push([row, -1]);
            }
          }
          
          // Added rows
          for (const row of currentOutput) {
            const key = JSON.stringify(row);
            if (!previousKeys.has(key)) {
              outputEntries.push([row, 1]);
            }
          }
          
          // Update previous for next iteration
          previousOutput = currentOutput;
          
          return ZSet.fromEntries(outputEntries);
        },
        () => {
          sortedState = [];
          keyToRow.clear();
          previousOutput = [];
        }
      );
    }
    
    // Apply QUALIFY clause (filter on window function results)
    // This is applied AFTER window functions are computed
    if (query.qualify && stream) {
      const qualifyPredicate = this.compileWhere(query.qualify);
      stream = stream.filter(qualifyPredicate);
    }
    
    return stream;
  }
  
  /**
   * Compile window functions (LAG, LEAD, rolling aggregates)
   * These require maintaining state across all rows
   */
  private compileWindowFunctions(
    stream: StreamHandle<any>,
    query: SelectQuery,
    windowCols: WindowColumn[],
    circuit: Circuit
  ): StreamHandle<any> {
    // Get simple columns that should be passed through
    const simpleCols = query.columns
      .filter((c): c is SimpleColumn => c !== '*' && typeof c === 'object' && c.type === 'column');
    
    // State for window functions
    // For each partition, we store ordered rows
    const partitionData = new Map<string, any[]>();
    // Store LAST computed results per partition (for differential output)
    const lastResults = new Map<string, any[]>();
    
    // Capture 'this' for use inside callback
    const self = this;
    
    // Pre-compute partition key function for efficiency
    const partitionBy = windowCols[0].partitionBy;
    const getPartitionKey = partitionBy 
      ? (row: any) => partitionBy.map(col => String(row[col] ?? '')).join('::')
      : () => '_all_';
    
    // Pre-compute order info
    const orderBy = windowCols[0].orderBy;
    const orderCol = orderBy?.[0]?.column;
    const orderDir = orderBy?.[0]?.direction === 'DESC' ? -1 : 1;
    
    return circuit.addStatefulOperator<any, any>(
      `window_${stream.id}`,
      [stream.id],
      (inputs: ZSet<any>[]) => {
        const delta = inputs[0];
        
        // BATCH PROCESSING: Collect all rows by partition first
        // This avoids O(n²) recomputation - we only compute ONCE per partition per batch
        const affectedPartitions = new Set<string>();
        
        for (const [row, count] of delta.entries()) {
          const partitionKey = getPartitionKey(row);
          affectedPartitions.add(partitionKey);
          
          // Get or create partition
          if (!partitionData.has(partitionKey)) {
            partitionData.set(partitionKey, []);
          }
          const partition = partitionData.get(partitionKey)!;
          
          if (count > 0) {
            // INSERT: Add row to partition
            partition.push(row);
          } else {
            // DELETE: Remove row from partition
            // Find by matching all fields (since we don't have a primary key here)
            const rowKey = JSON.stringify(row);
            const idx = partition.findIndex(r => JSON.stringify(r) === rowKey);
            if (idx !== -1) {
              partition.splice(idx, 1);
            }
          }
        }
        
        // Now compute window results ONCE per affected partition
        const outputEntries: [any, number][] = [];
        
        for (const partitionKey of affectedPartitions) {
          const partition = partitionData.get(partitionKey)!;
          
          // Sort partition if needed (do this ONCE, not per row)
          if (orderCol) {
            partition.sort((a, b) => {
              const aVal = a[orderCol];
              const bVal = b[orderCol];
              let cmp: number;
              if (typeof aVal === 'number' && typeof bVal === 'number') {
                cmp = aVal - bVal;
              } else {
                cmp = String(aVal ?? '').localeCompare(String(bVal ?? ''));
              }
              return orderDir * cmp;
            });
          }
          
          // Compute window results ONCE for the whole partition
          const newResults = self.computeWindowResults(partition, windowCols, simpleCols);
          
          // OPTIMIZATION: Use differential output - only emit changed rows
          // This is critical for NTILE where most rows stay in the same quartile
          const prevResults = lastResults.get(partitionKey) || [];
          
          // Build lookup map for previous results (by primary key or row content)
          const prevMap = new Map<string, any>();
          for (const oldResult of prevResults) {
            // Use outputKey if available, otherwise full serialization
            const key = JSON.stringify(oldResult);
            prevMap.set(key, oldResult);
          }
          
          // Build lookup for new results
          const newMap = new Map<string, any>();
          for (const newResult of newResults) {
            const key = JSON.stringify(newResult);
            newMap.set(key, newResult);
          }
          
          // Emit only the differences
          // Deletions: in prev but not in new
          for (const [key, oldResult] of prevMap) {
            if (!newMap.has(key)) {
              outputEntries.push([oldResult, -1]);
            }
          }
          
          // Insertions: in new but not in prev
          for (const [key, newResult] of newMap) {
            if (!prevMap.has(key)) {
              outputEntries.push([newResult, 1]);
            }
          }
          
          // Store for next time
          lastResults.set(partitionKey, newResults);
        }
        
        return ZSet.fromEntries(outputEntries);
      },
      () => {
        partitionData.clear();
        lastResults.clear();
      }
    );
  }
  
  /**
   * OPTIMIZED: Compile window functions using O(1) algorithms
   * 
   * Uses:
   * - Monotonic Deque for MIN/MAX (O(1) amortized)
   * - Running totals for SUM/AVG/COUNT (O(1))
   * - Direct indexing for LAG/LEAD (O(1))
   * 
   * This is used for "streaming-friendly" window functions that only
   * look at PRECEDING rows (not FOLLOWING).
   */
  private compileWindowFunctionsOptimized(
    stream: StreamHandle<any>,
    query: SelectQuery,
    windowCols: WindowColumn[],
    circuit: Circuit
  ): StreamHandle<any> {
    // Get simple columns that should be passed through
    const simpleCols = query.columns
      .filter((c): c is SimpleColumn => c !== '*' && typeof c === 'object' && c.type === 'column');
    
    // Get column names for fast key generation
    const simpleColNames = simpleCols.map(c => c.alias || c.name);
    
    // Convert window columns to specs
    const specs: WindowFunctionSpec[] = windowCols.map(wc => {
      const frame = wc.frame || { type: 'ROWS', start: { type: 'UNBOUNDED' }, end: { type: 'CURRENT' } };
      let frameSize = 100; // Default reasonable frame (not 1000!)
      
      if (frame.start.type === 'PRECEDING' && frame.start.offset !== undefined) {
        frameSize = frame.start.offset + 1; // +1 to include current row
      }
      
      return {
        type: wc.function as WindowFunctionSpec['type'],
        column: wc.args[0]?.column,
        frameSize,
        offset: wc.args[1]?.value ?? 1,
        alias: wc.alias || `${wc.function.toLowerCase()}_result`,
      };
    });
    
    // Create partitioned state - use PartitionedWindowState directly (handles all partitioning)
    const partitionKeyFn = windowCols[0].partitionBy
      ? (row: any) => windowCols[0].partitionBy!.map(col => String(row[col] ?? '')).join('::')
      : () => '_all_';
    
    // Single state that handles all partitions
    const windowState = new PartitionedWindowState(specs, partitionKeyFn);
    
    return circuit.addStatefulOperator<any, any>(
      `window_opt_${stream.id}`,
      [stream.id],
      (inputs: ZSet<any>[]) => {
        const delta = inputs[0];
        const outputEntries: [any, number][] = [];
        
        for (const [row, count] of delta.entries()) {
          if (count <= 0) continue;
          
          // Process through optimized window state - O(1) per function!
          const result = windowState.processRow(row);
          
          // Build final result with only requested columns (avoid unnecessary object creation)
          const finalResult: any = {};
          for (const colName of simpleColNames) {
            finalResult[colName] = result[colName] ?? row[colName];
          }
          for (const spec of specs) {
            finalResult[spec.alias] = result[spec.alias];
          }
          
          // Emit only the new result - no retraction needed for append-only streaming
          // The integrated view handles deduplication if needed
          outputEntries.push([finalResult, 1]);
        }
        
        return ZSet.fromEntries(outputEntries);
      },
      () => {
        windowState.reset();
      }
    );
  }
  
  /**
   * Check if window functions can use optimized O(1) algorithms
   */
  private canUseOptimizedWindow(windowCols: WindowColumn[]): boolean {
    for (const wc of windowCols) {
      // Optimized algorithms only work for:
      // - SUM, AVG, COUNT, MIN, MAX with ROWS BETWEEN n PRECEDING AND CURRENT ROW (with ORDER BY)
      // - LAG without default value (with ORDER BY - uses ordered row buffer for previous rows)
      // - ROW_NUMBER (without ORDER BY - just a counter)
      // Note: RANK, DENSE_RANK, NTILE require full partition knowledge - use fallback
      // Note: LEAD requires FUTURE rows, so can't be optimized in streaming mode
      // Note: LAG with default value needs full partition knowledge to apply default correctly
      
      const supportedFunctions = ['SUM', 'AVG', 'COUNT', 'MIN', 'MAX', 'LAG', 'ROW_NUMBER'];
      if (!supportedFunctions.includes(wc.function)) {
        return false;
      }
      
      // LAG with default value (3 args) needs non-optimized path
      if (wc.function === 'LAG' && wc.args.length >= 3) {
        return false;
      }
      
      // ROW_NUMBER with ORDER BY requires maintaining sorted partition - can't optimize
      // (We'd need to re-assign all row numbers when a row is inserted in the middle)
      if (wc.function === 'ROW_NUMBER' && wc.orderBy && wc.orderBy.length > 0) {
        console.log(`[SQL] ROW_NUMBER with ORDER BY - using full partition approach`);
        return false;
      }
      
      // Check frame - must end at CURRENT ROW (no FOLLOWING)
      const frame = wc.frame;
      if (frame) {
        if (frame.end.type === 'FOLLOWING') {
          return false; // Can't optimize FOLLOWING
        }
      }
    }
    
    return true;
  }
  
  /**
   * Compute window function results for all rows in a partition
   */
  private computeWindowResults(
    partition: any[],
    windowCols: WindowColumn[],
    simpleCols: SimpleColumn[]
  ): any[] {
    const results: any[] = [];
    
    for (let i = 0; i < partition.length; i++) {
      const row = partition[i];
      const result: any = {};
      
      // Copy simple columns
      for (const col of simpleCols) {
        // Try prefixed column first (for self-joins), then fall back to unprefixed
        const prefixedName = col.table ? `${col.table}.${col.name}` : col.name;
        const value = row[prefixedName] !== undefined ? row[prefixedName] : row[col.name];
        result[col.alias || col.name] = value;
      }
      
      // Compute each window function
      for (const wc of windowCols) {
        const alias = wc.alias || `${wc.function.toLowerCase()}_result`;
        
        switch (wc.function) {
          case 'LAG': {
            // LAG(col, offset, default)
            const col = wc.args[0]?.column || '';
            const offset = wc.args[1]?.value ?? 1;
            // Default value: use undefined check to distinguish between "no default" and "default = null"
            const hasDefault = wc.args.length >= 3;
            const defaultVal = hasDefault ? wc.args[2]?.value : null;
            
            const prevIdx = i - offset;
            result[alias] = prevIdx >= 0 ? partition[prevIdx][col] : defaultVal;
            break;
          }
          
          case 'LEAD': {
            // LEAD(col, offset, default)
            const col = wc.args[0]?.column || '';
            const offset = wc.args[1]?.value ?? 1;
            const defaultVal = wc.args[2]?.value ?? null;
            
            const nextIdx = i + offset;
            result[alias] = nextIdx < partition.length ? partition[nextIdx][col] : defaultVal;
            break;
          }
          
          case 'ROW_NUMBER': {
            result[alias] = i + 1;
            break;
          }
          
          case 'RANK': {
            // RANK: Returns rank with gaps for ties
            // Find position of first row with same ORDER BY values
            const orderBy = wc.orderBy || [];
            const currRow = partition[i];
            
            // If no rows before have the same values, rank is position + 1
            // If some rows have same values, find the first one and use its position + 1
            let firstSameIdx = i;
            for (let j = 0; j < i; j++) {
              let same = true;
              for (const ord of orderBy) {
                if (partition[j][ord.column] !== currRow[ord.column]) {
                  same = false;
                  break;
                }
              }
              if (same) {
                firstSameIdx = j;
                break;
              }
            }
            
            result[alias] = firstSameIdx + 1;
            break;
          }
          
          case 'DENSE_RANK': {
            // DENSE_RANK: Returns rank without gaps for ties
            // Count distinct ORDER BY value groups up to and including current row
            const orderBy = wc.orderBy || [];
            
            let denseRank = 1;
            let prevValues: any = null;
            
            for (let j = 0; j <= i; j++) {
              const r = partition[j];
              
              // Check if values changed from previous
              if (prevValues !== null) {
                let changed = false;
                for (const ord of orderBy) {
                  if (r[ord.column] !== prevValues[ord.column]) {
                    changed = true;
                    break;
                  }
                }
                if (changed) {
                  denseRank++;
                }
              }
              
              // Save current values for next comparison
              prevValues = {};
              for (const ord of orderBy) {
                prevValues[ord.column] = r[ord.column];
              }
            }
            
            result[alias] = denseRank;
            break;
          }
          
          case 'PERCENT_RANK': {
            // PERCENT_RANK: (rank - 1) / (total_rows - 1)
            // Relative rank within the partition
            if (partition.length === 1) {
              result[alias] = 0;
            } else {
              const orderBy = wc.orderBy || [];
              if (i > 0) {
                // Find actual rank (position of first row with same values)
                let actualRank = i + 1;
                for (let j = 0; j < i; j++) {
                  let same = true;
                  for (const ord of orderBy) {
                    if (partition[j][ord.column] !== partition[i][ord.column]) {
                      same = false;
                      break;
                    }
                  }
                  if (same) {
                    actualRank = j + 1;
                    break;
                  }
                }
                result[alias] = (actualRank - 1) / (partition.length - 1);
              } else {
                result[alias] = 0;
              }
            }
            break;
          }
          
          case 'CUME_DIST': {
            // CUME_DIST: Cumulative distribution
            // Number of rows with value <= current / total rows
            const orderBy = wc.orderBy || [];
            let countLessOrEqual = 0;
            
            for (let j = 0; j < partition.length; j++) {
              let lessOrEqual = true;
              for (const ord of orderBy) {
                const cmp = partition[j][ord.column] < partition[i][ord.column] ? -1 :
                           partition[j][ord.column] > partition[i][ord.column] ? 1 : 0;
                const directedCmp = ord.direction === 'DESC' ? -cmp : cmp;
                if (directedCmp > 0) {
                  lessOrEqual = false;
                  break;
                }
              }
              if (lessOrEqual) countLessOrEqual++;
            }
            
            result[alias] = countLessOrEqual / partition.length;
            break;
          }
          
          case 'NTILE': {
            // NTILE(n): Divides rows into n buckets
            const n = wc.args[0]?.value || 4;
            const bucketSize = Math.ceil(partition.length / n);
            result[alias] = Math.floor(i / bucketSize) + 1;
            break;
          }
          
          case 'FIRST_VALUE': {
            // FIRST_VALUE: First value in the partition/frame
            const col = wc.args[0]?.column || '';
            const frame = wc.frame || { type: 'ROWS', start: { type: 'UNBOUNDED' }, end: { type: 'CURRENT' } };
            
            let startIdx = 0;
            if (frame.start.type === 'PRECEDING' && frame.start.offset !== undefined) {
              startIdx = Math.max(0, i - frame.start.offset);
            } else if (frame.start.type === 'CURRENT') {
              startIdx = i;
            }
            
            result[alias] = partition[startIdx]?.[col] ?? null;
            break;
          }
          
          case 'LAST_VALUE': {
            // LAST_VALUE: Last value in the partition/frame
            const col = wc.args[0]?.column || '';
            const frame = wc.frame || { type: 'ROWS', start: { type: 'UNBOUNDED' }, end: { type: 'CURRENT' } };
            
            let endIdx = i;
            if (frame.end.type === 'FOLLOWING' && frame.end.offset !== undefined) {
              endIdx = Math.min(partition.length - 1, i + frame.end.offset);
            } else if (frame.end.type === 'UNBOUNDED') {
              endIdx = partition.length - 1;
            }
            
            result[alias] = partition[endIdx]?.[col] ?? null;
            break;
          }
          
          case 'SUM':
          case 'AVG':
          case 'COUNT':
          case 'MIN':
          case 'MAX': {
            // Rolling aggregate with frame
            const col = wc.args[0]?.column || '';
            const frame = wc.frame || { type: 'ROWS', start: { type: 'UNBOUNDED' }, end: { type: 'CURRENT' } };
            
            // Compute frame bounds
            let startIdx = 0;
            let endIdx = i;
            
            if (frame.start.type === 'PRECEDING' && frame.start.offset !== undefined) {
              startIdx = Math.max(0, i - frame.start.offset);
            } else if (frame.start.type === 'CURRENT') {
              startIdx = i;
            } else if (frame.start.type === 'UNBOUNDED') {
              startIdx = 0;
            }
            
            if (frame.end.type === 'FOLLOWING' && frame.end.offset !== undefined) {
              endIdx = Math.min(partition.length - 1, i + frame.end.offset);
            } else if (frame.end.type === 'CURRENT') {
              endIdx = i;
            } else if (frame.end.type === 'UNBOUNDED') {
              endIdx = partition.length - 1;
            }
            
            // Compute aggregate over frame
            const frameRows = partition.slice(startIdx, endIdx + 1);
            let aggResult: number | null = null;
            
            switch (wc.function) {
              case 'SUM':
                aggResult = frameRows.reduce((sum, r) => sum + (Number(r[col]) || 0), 0);
                break;
              case 'AVG':
                const total = frameRows.reduce((sum, r) => sum + (Number(r[col]) || 0), 0);
                aggResult = frameRows.length > 0 ? total / frameRows.length : null;
                break;
              case 'COUNT':
                aggResult = frameRows.length;
                break;
              case 'MIN':
                aggResult = frameRows.reduce((min, r) => {
                  const val = Number(r[col]);
                  return min === null || val < min ? val : min;
                }, null as number | null);
                break;
              case 'MAX':
                aggResult = frameRows.reduce((max, r) => {
                  const val = Number(r[col]);
                  return max === null || val > max ? val : max;
                }, null as number | null);
                break;
            }
            
            result[alias] = aggResult;
            break;
          }
        }
      }
      
      results.push(result);
    }
    
    return results;
  }

  /**
   * Compile multiple JOINs (supports 3+ tables)
   * Each join is applied in sequence, building up the result
   */
  private compileMultipleJoins(
    initialStream: StreamHandle<any> | null,
    joins: JoinInfo[],
    tables: Record<string, StreamHandle<any>>,
    circuit: Circuit,
    aliasMap: Record<string, string>,
    leftTableAlias?: string  // Alias for the initial (left) table
  ): StreamHandle<any> | null {
    if (!initialStream) return null;
    
    let result = initialStream;
    let currentLeftAlias = leftTableAlias;
    
    for (const join of joins) {
      // Get the table to join with
      let rightTable: StreamHandle<any> | null = null;
      
      if (join.derivedTable) {
        // Derived table in JOIN: JOIN (SELECT ...) AS alias
        rightTable = this.compileQuery(join.derivedTable, tables, circuit);
      } else {
        // Check for alias first, then direct table name
        const tableName = join.tableAlias ? (aliasMap[join.tableAlias] || join.table) : join.table;
        rightTable = tables[tableName] || tables[join.table];
        
        // Register alias
        if (join.tableAlias) {
          aliasMap[join.tableAlias] = join.table;
        }
      }
      
      if (!rightTable) {
        console.error(`Join table ${join.table} not found`);
        return null;
      }
      
      // Determine right alias
      const rightAlias = join.tableAlias || join.table;
      
      // Apply the join with aliases for proper column prefixing
      const joinResult = this.compileSingleJoin(result, rightTable, join, circuit, currentLeftAlias, rightAlias);
      if (!joinResult) return null;
      result = joinResult;
      
      // For subsequent joins, the current result becomes the left side
      // We don't need to track alias anymore since columns are already prefixed
      currentLeftAlias = undefined;
    }
    
    return result;
  }
  
  /**
   * Compile a single JOIN with support for:
   * - Multiple conditions (composite keys)
   * - Non-equi joins (>, <, >=, <=, BETWEEN)
   * - All join types (INNER, LEFT, RIGHT, FULL, CROSS)
   */
  private compileSingleJoin(
    left: StreamHandle<any>,
    right: StreamHandle<any>,
    join: JoinInfo,
    circuit: Circuit,
    leftAlias?: string,
    rightAlias?: string
  ): StreamHandle<any> | null {
    // Helper to merge tuple [left, right] into flat object
    // For self-joins, prefix columns with table alias to avoid collisions
    // Handles null values for outer joins (LEFT/RIGHT/FULL)
    const mergeTuple = (joined: StreamHandle<[any, any]>): StreamHandle<any> => {
      return joined.map(([l, r]: [any, any]) => {
        const result: Record<string, any> = {};
        
        // Add left columns - prefix with alias if present
        // l can be null for RIGHT JOIN with no match
        if (l != null) {
          for (const [key, value] of Object.entries(l)) {
            if (leftAlias) {
              result[`${leftAlias}.${key}`] = value;
            }
            result[key] = value;  // Also keep unprefixed for backwards compat
          }
        }
        
        // Add right columns - prefix with alias if present
        // r can be null for LEFT JOIN with no match
        if (r != null) {
          for (const [key, value] of Object.entries(r)) {
            if (rightAlias) {
              result[`${rightAlias}.${key}`] = value;
            }
            result[key] = value;  // Also keep unprefixed (overwrites if collision)
          }
        }
        
        return result;
      });
    };
    
    // Handle CROSS JOIN specially (no ON condition)
    if (join.type === 'CROSS') {
      // Cross join is just a cartesian product
      const joined = left.join(right, () => '_all_', () => '_all_');
      return mergeTuple(joined);
    }
    
    // Check for multiple conditions or non-equi join
    const hasMultipleConditions = join.conditions && join.conditions.length > 1;
    const hasNonEquiCondition = join.conditions?.some(c => c.operator !== '=');
    
    if (hasNonEquiCondition || hasMultipleConditions) {
      // Use filter-based join for complex conditions
      return this.compileComplexJoin(left, right, join, circuit);
    }
    
    // Simple equi-join: use the efficient hash-based join
    const leftKeyFn = (row: any) => row[join.leftColumn];
    const rightKeyFn = (row: any) => row[join.rightColumn];
    
    // For different join types, we need to merge the tuple result
    let joined: StreamHandle<[any, any]>;
    switch (join.type) {
      case 'INNER':
        joined = left.join(right, leftKeyFn, rightKeyFn);
        break;
      case 'LEFT':
        joined = left.leftJoin(right, leftKeyFn, rightKeyFn);
        break;
      case 'RIGHT':
        joined = right.leftJoin(left, rightKeyFn, leftKeyFn);
        break;
      case 'FULL':
        joined = left.fullJoin(right, leftKeyFn, rightKeyFn);
        break;
      default:
        joined = left.join(right, leftKeyFn, rightKeyFn);
        break;
    }
    
    return mergeTuple(joined);
  }
  
  /**
   * Compile complex joins with multiple conditions or non-equi operators
   * This uses a nested-loop approach with filtering
   */
  private compileComplexJoin(
    left: StreamHandle<any>,
    right: StreamHandle<any>,
    join: JoinInfo,
    _circuit: Circuit  // Reserved for future use (e.g., stateful non-equi joins)
  ): StreamHandle<any> {
    const conditions = join.conditions || [{
      leftColumn: join.leftColumn,
      rightColumn: join.rightColumn,
      operator: '=' as const,
    }];
    
    // Determine which table is the "left" table and "right" table by alias
    const leftTableAlias = join.leftTable || '';
    const rightTableAlias = join.tableAlias || join.rightTable || join.table;
    
    // Build a filter function that checks all conditions
    const matchesFn = (leftRow: any, rightRow: any): boolean => {
      for (const cond of conditions) {
        // Determine which row to read leftColumn from based on leftTable
        // If leftTable matches the right table alias, use rightRow
        const isLeftFromRightTable = cond.leftTable === rightTableAlias || 
                                     cond.leftTable === join.table;
        const leftVal = isLeftFromRightTable 
          ? rightRow[cond.leftColumn] 
          : leftRow[cond.leftColumn];
        
        // If rightLiteral is set, use it; otherwise use rightRow[rightColumn]
        let rightVal: any;
        if (cond.rightLiteral !== undefined) {
          rightVal = cond.rightLiteral;
        } else {
          // Determine which row to read rightColumn from based on rightTable
          const isRightFromLeftTable = cond.rightTable === leftTableAlias;
          rightVal = isRightFromLeftTable 
            ? leftRow[cond.rightColumn] 
            : rightRow[cond.rightColumn];
        }
        
        switch (cond.operator) {
          case '=':
            if (leftVal !== rightVal) return false;
            break;
          case '!=':
            if (leftVal === rightVal) return false;
            break;
          case '<':
            if (!(leftVal < rightVal)) return false;
            break;
          case '>':
            if (!(leftVal > rightVal)) return false;
            break;
          case '<=':
            if (!(leftVal <= rightVal)) return false;
            break;
          case '>=':
            if (!(leftVal >= rightVal)) return false;
            break;
          case 'BETWEEN':
            const highVal = rightRow[cond.betweenHigh!];
            if (!(leftVal >= rightVal && leftVal <= highVal)) return false;
            break;
        }
      }
      return true;
    };
    
    // Helper to merge tuple [left, right] into flat object
    const mergeTuple = (joined: StreamHandle<[any, any]>): StreamHandle<any> => {
      return joined.map(([l, r]: [any, any]) => ({ ...l, ...r }));
    };
    
    // Use joinFilter for non-equi joins
    // First, find the first equi-condition to use as the join key (optimization)
    const equiCondition = conditions.find(c => c.operator === '=');
    
    if (equiCondition) {
      // Use equi-join + filter for efficiency
      const leftKeyFn = (row: any) => row[equiCondition.leftColumn];
      const rightKeyFn = (row: any) => row[equiCondition.rightColumn];
      
      const joined = left.joinFilter(
        right,
        leftKeyFn,
        rightKeyFn,
        matchesFn
      );
      return mergeTuple(joined);
    } else {
      // Pure non-equi join - need cross join + filter
      // This is O(n*m) but unavoidable for non-equi conditions without equi-key
      const crossJoined = left.join(right, () => '_all_', () => '_all_');
      const filtered = crossJoined.filter(([leftRow, rightRow]: [any, any]) => matchesFn(leftRow, rightRow));
      return mergeTuple(filtered);
    }
  }
  
  private compileJoin(
    query: SelectQuery,
    tables: Record<string, StreamHandle<any>>
  ): StreamHandle<any> | null {
    const leftTable = tables[query.from];
    const rightTable = query.join ? tables[query.join.table] : null;
    
    if (!leftTable || !rightTable || !query.join) {
      console.error('Join tables not found');
      return null;
    }
    
    // For legacy join, use simplified single join (circuit not needed)
    return this.compileSingleJoin(leftTable, rightTable, query.join, null as any);
  }

  /**
   * Recursively extract aggregate functions from an expression
   * This handles aggregates inside CASE conditions and arithmetic
   */
  /**
   * Serialize an expression to a canonical string format for HAVING matching.
   * This must match how the parser serializes expressions.
   */
  private serializeExpressionForHaving(expr: any): string {
    if (!expr) return '';
    
    if (expr.type === 'column') {
      return expr.column || '';
    }
    
    if (expr.type === 'number' || expr.type === 'literal') {
      return String(expr.value);
    }
    
    if (expr.type === 'expression') {
      // Binary expression from our parser: { left, operator, right }
      const left = this.serializeExpressionForHaving(expr.left);
      const right = this.serializeExpressionForHaving(expr.right);
      return `(${left} ${expr.operator} ${right})`;
    }
    
    if (expr.type === 'function') {
      const fnName = expr.functionName || 'fn';
      const args = (expr.args || []).map((a: any) => this.serializeExpressionForHaving(a)).join(', ');
      return `${fnName}(${args})`;
    }
    
    return '';
  }

  private extractAggregatesFromExpr(expr: any, aggregates: AggregateColumn[], seen: Set<string>): void {
    if (!expr || typeof expr !== 'object') return;
    
    // Check if this is an aggregate function
    if (expr.type === 'aggr_func' || expr.type === 'aggregate') {
      const funcName = typeof expr.name === 'string' 
        ? expr.name.toUpperCase()
        : expr.name?.name?.[0]?.value?.toUpperCase() || expr.function?.toUpperCase() || '';
      
      if (['SUM', 'COUNT', 'AVG', 'MIN', 'MAX'].includes(funcName)) {
        // Build a unique key for this aggregate to avoid duplicates
        const argKey = JSON.stringify(expr.args || expr.over);
        const key = `${funcName}:${argKey}`;
        
        if (!seen.has(key)) {
          seen.add(key);
          
          // Parse the argument expression
          const argExpr = this.parseAggregateArg(expr);
          const alias = `${funcName.toLowerCase()}_${seen.size}`;
          
          aggregates.push({
            type: 'aggregate',
            function: funcName as any,
            args: ['*'], // Will be overridden by argExpr
            argExpr,
            alias,
          });
        }
      }
    }
    
    // Recurse into sub-expressions
    if (expr.left) this.extractAggregatesFromExpr(expr.left, aggregates, seen);
    if (expr.right) this.extractAggregatesFromExpr(expr.right, aggregates, seen);
    
    // Handle args which can be array, object with value/expr, or undefined
    if (expr.args) {
      if (Array.isArray(expr.args)) {
        for (const arg of expr.args) {
          this.extractAggregatesFromExpr(arg, aggregates, seen);
          // Also check arg.cond and arg.result for CASE clauses
          if (arg?.cond) this.extractAggregatesFromExpr(arg.cond, aggregates, seen);
          if (arg?.result) this.extractAggregatesFromExpr(arg.result, aggregates, seen);
        }
      } else if (typeof expr.args === 'object') {
        // args might be { expr: ..., value: [...] }
        if (expr.args.expr) this.extractAggregatesFromExpr(expr.args.expr, aggregates, seen);
        if (expr.args.value && Array.isArray(expr.args.value)) {
          for (const val of expr.args.value) {
            this.extractAggregatesFromExpr(val, aggregates, seen);
          }
        }
      }
    }
    
    if (expr.conditions) {
      for (const cond of expr.conditions) {
        if (cond.when) this.extractAggregatesFromExpr(cond.when, aggregates, seen);
        if (cond.then) this.extractAggregatesFromExpr(cond.then, aggregates, seen);
      }
    }
    if (expr.else) this.extractAggregatesFromExpr(expr.else, aggregates, seen);
  }
  
  /**
   * Parse aggregate argument from node-sql-parser format
   */
  private parseAggregateArg(expr: any): AggregateArg | undefined {
    if (!expr?.args?.expr) return undefined;
    
    const argExpr = expr.args.expr;
    
    // Handle function like ABS(column)
    if (argExpr.type === 'function') {
      const funcName = argExpr.name?.name?.[0]?.value || argExpr.name;
      const innerArg = argExpr.args?.value?.[0]?.column || argExpr.args?.value?.[0]?.value;
      return {
        type: 'function',
        functionName: funcName,
        args: innerArg ? [{ type: 'column', column: innerArg }] : [],
      };
    }
    
    // Handle binary expression like (a + b)
    if (argExpr.type === 'binary_expr') {
      return {
        type: 'expression',
        operator: argExpr.operator,
        left: this.parseExprToAggregateArg(argExpr.left),
        right: this.parseExprToAggregateArg(argExpr.right),
      };
    }
    
    // Handle column reference
    if (argExpr.type === 'column_ref') {
      return { type: 'column', column: argExpr.column };
    }
    
    // Handle star
    if (argExpr.type === 'star') {
      return { type: 'star' };
    }
    
    return undefined;
  }
  
  private parseExprToAggregateArg(expr: any): AggregateArg {
    if (!expr) return { type: 'star' };
    
    if (expr.type === 'column_ref') {
      return { type: 'column', column: expr.column };
    }
    if (expr.type === 'number') {
      return { type: 'expression', value: expr.value };
    }
    if (expr.type === 'binary_expr') {
      return {
        type: 'expression',
        operator: expr.operator,
        left: this.parseExprToAggregateArg(expr.left),
        right: this.parseExprToAggregateArg(expr.right),
      };
    }
    if (expr.type === 'function') {
      const funcName = expr.name?.name?.[0]?.value || expr.name;
      const argValue = expr.args?.value?.[0];
      return {
        type: 'function',
        functionName: funcName,
        args: argValue ? [this.parseExprToAggregateArg(argValue)] : [],
      };
    }
    
    return { type: 'star' };
  }

  private compileAggregation(
    stream: StreamHandle<any>,
    query: SelectQuery,
    circuit: Circuit,
    tables: Record<string, StreamHandle<any>> = {}
  ): StreamHandle<any> {
    // Find top-level aggregate functions
    const aggregates = query.columns.filter(
      (c): c is AggregateColumn => c !== '*' && typeof c === 'object' && c.type === 'aggregate'
    );
    
    // Find simple columns (for GROUP BY output)
    const simpleColumns = query.columns.filter(
      (c): c is SimpleColumn => c !== '*' && typeof c === 'object' && c.type === 'column'
    );
    
    // Find literal columns (constant values like "0 AS avgSlippage")
    type LiteralCol = { type: 'literal'; value: number | string | boolean; alias?: string };
    const literalColumns = query.columns.filter(
      (c): c is LiteralCol => c !== '*' && typeof c === 'object' && c.type === 'literal'
    );
    
    // Find scalar subquery columns - CRITICAL for queries like:
    // SELECT sector, SUM(notional), (SELECT SUM(notional) FROM positions) AS total FROM positions GROUP BY sector
    const scalarSubqueryCols = query.columns.filter(
      (c): c is ScalarSubqueryColumn => c !== '*' && typeof c === 'object' && c.type === 'scalar_subquery'
    );
    
    // Pre-compile scalar subqueries
    // ARCHITECTURE: Scalar subqueries produce global values that need to be joined with every output row.
    // We compile them as separate streams and join them with the aggregation result.
    const scalarSubqueryStreams = new Map<string, StreamHandle<any>>();
    
    // 1. Extract top-level scalar subquery columns
    for (const col of scalarSubqueryCols) {
      const alias = col.alias || '_scalar_';
      const subqueryStream = this.compileQuery(col.query, tables, circuit);
      if (subqueryStream) {
        scalarSubqueryStreams.set(alias, subqueryStream.integrate());
      }
    }
    
    // 2. Extract scalar subqueries from inside CASE expressions
    // The raw AST has objects with {ast: {type: 'select', ...}} for scalar subqueries
    let scalarCounter = 0;
    const extractScalarSubqueriesFromExpr = (expr: any): void => {
      if (!expr || typeof expr !== 'object') return;
      
      // Check for scalar subquery: has an 'ast' property with type 'select'
      if (expr.ast && expr.ast.type === 'select') {
        const alias = `_scalar_inline_${scalarCounter++}`;
        const parser = new SQLParser();
        try {
          const parsedQuery = parser.parseQuery(expr.ast);
          const subqueryStream = this.compileQuery(parsedQuery, tables, circuit);
          if (subqueryStream) {
            scalarSubqueryStreams.set(alias, subqueryStream.integrate());
            // Mark the expression so we can look it up later
            expr._scalarAlias = alias;
          }
        } catch (e) {
          console.warn('Failed to parse inline scalar subquery:', e);
        }
        // Don't recurse into the subquery's own structure
        return;
      }
      
      // Recursively extract from binary expressions  
      if (expr.left) extractScalarSubqueriesFromExpr(expr.left);
      if (expr.right) extractScalarSubqueriesFromExpr(expr.right);
      
      // Handle arrays of arguments
      if (Array.isArray(expr.args)) {
        for (const arg of expr.args) {
          extractScalarSubqueriesFromExpr(arg);
        }
      } else if (expr.args?.expr) {
        extractScalarSubqueriesFromExpr(expr.args.expr);
      }
      
      if (expr.expr) extractScalarSubqueriesFromExpr(expr.expr);
    };
    
    // Extract from CASE column conditions and results
    for (const caseCol of query.columns) {
      if (caseCol !== '*' && typeof caseCol === 'object' && caseCol.type === 'case') {
        if (caseCol.conditions) {
          for (const cond of caseCol.conditions) {
            if (cond.when) extractScalarSubqueriesFromExpr(cond.when);
            if (cond.then) extractScalarSubqueriesFromExpr(cond.then);
          }
        }
        if (caseCol.else) {
          extractScalarSubqueriesFromExpr(caseCol.else);
        }
      }
    }
    
    // Find CASE columns (post-aggregation expressions)
    type CaseColumn = { type: 'case'; conditions: { when: any; then: any }[]; else?: any; alias?: string };
    const caseColumns = query.columns.filter(
      (c): c is CaseColumn => c !== '*' && typeof c === 'object' && c.type === 'case'
    );
    
    // CRITICAL: Also extract aggregates from INSIDE CASE expressions
    // For SQL like: SELECT CASE WHEN SUM(x) > 0 THEN SUM(y) / SUM(x) ELSE 0 END
    // We need to compute SUM(x) and SUM(y) as separate aggregates
    const seen = new Set<string>();
    for (const agg of aggregates) {
      const key = `${agg.function}:${JSON.stringify(agg.args)}`;
      seen.add(key);
    }
    for (const caseCol of caseColumns) {
      // Extract from each condition's when and then clauses
      if (caseCol.conditions) {
        for (const cond of caseCol.conditions) {
          this.extractAggregatesFromExpr(cond.when, aggregates, seen);
          this.extractAggregatesFromExpr(cond.then, aggregates, seen);
        }
      }
      // Extract from else clause
      if (caseCol.else) {
        this.extractAggregatesFromExpr(caseCol.else, aggregates, seen);
      }
    }
    
    const groupByColumns = query.groupBy || [];
    const hasGroupBy = groupByColumns.length > 0;
    
    // Only skip if there are truly no aggregates (including those in CASE)
    if (aggregates.length === 0 && caseColumns.length === 0) {
      return stream;
    }
    
    if (hasGroupBy) {
      return this.compileGroupByAggregation(stream, aggregates, simpleColumns, groupByColumns, query, circuit, caseColumns, literalColumns, scalarSubqueryStreams);
    } else {
      return this.compileGlobalAggregation(stream, aggregates, circuit, caseColumns, literalColumns, scalarSubqueryStreams);
    }
  }

  private compileGroupByAggregation(
    stream: StreamHandle<any>,
    aggregates: AggregateColumn[],
    simpleColumns: SimpleColumn[],
    groupByColumns: string[],
    query: SelectQuery,
    circuit: Circuit,
    caseColumns: { type: 'case'; conditions: { when: any; then: any }[]; else?: any; alias?: string }[] = [],
    literalColumns: { type: 'literal'; value: number | string | boolean; alias?: string }[] = [],
    scalarSubqueryStreams: Map<string, StreamHandle<any>> = new Map()
  ): StreamHandle<any> {
    // Create key function for grouping
    const getGroupKey = (row: any): string => {
      return groupByColumns.map(col => String(row[col] ?? '')).join('::');
    };
    
    // Build aggregate alias map for HAVING
    const aggregateAliasMap = new Map<string, string>();
    for (const agg of aggregates) {
      const col = agg.args[0];
      const argName = col === '*' ? '*' : col;
      const exprStr = agg.argExpr ? getExprString(agg.argExpr) : (col === '*' ? 'star' : col);
      const alias = agg.alias || `${agg.function.toLowerCase()}_${exprStr}`;
      
      // Map "SUM:amount" -> "total" (the alias)
      aggregateAliasMap.set(`${agg.function}:${argName}`, alias);
      
      // Also add serialized expression form for HAVING clauses that use expressions
      // e.g., "SUM:(unrealizedPnL + realizedPnL)" -> "totalPnL"
      if (agg.argExpr && agg.argExpr.type === 'expression') {
        const serialized = this.serializeExpressionForHaving(agg.argExpr);
        if (serialized) {
          aggregateAliasMap.set(`${agg.function}:${serialized}`, alias);
        }
      }
    }
    
    // Compile HAVING predicate if present
    const havingPredicate = query.having 
      ? this.compileHaving(query.having, aggregateAliasMap)
      : null;
    
    // State: Map of groupKey -> { sum, count, rows (for min/max), distinctSets (for COUNT DISTINCT) }
    type GroupState = {
      sum: Map<string, number>;  // per aggregate column
      count: number;
      min: Map<string, number>;
      max: Map<string, number>;
      distinctSets: Map<string, Map<string, number>>;  // For COUNT(DISTINCT col): alias -> (value -> weight)
    };
    
    const groupStates = new Map<string, GroupState>();
    const previousResults = new Map<string, any>(); // Previous output per group
    
    const aggregatedStream = circuit.addStatefulOperator(
      `groupby_agg_${stream.id}`,
      [stream.id],
      (inputs: ZSet<any>[]) => {
        const delta = inputs[0];
        const affectedGroups = new Set<string>();
        
        // Apply delta to group states
        for (const [row, weight] of delta.entries()) {
          const groupKey = getGroupKey(row);
          affectedGroups.add(groupKey);
          
          // Get or create group state
          let state = groupStates.get(groupKey);
          if (!state) {
            state = {
              sum: new Map(),
              count: 0,
              min: new Map(),
              max: new Map(),
              distinctSets: new Map(),
            };
            groupStates.set(groupKey, state);
          }
          
          // Update count
          state.count += weight;
          
          // Update sums and track values for min/max
          for (const agg of aggregates) {
            const col = agg.args[0];
            // Use expression if available, otherwise fall back to column name
            const exprStr = agg.argExpr ? getExprString(agg.argExpr) : (col === '*' ? 'star' : col);
            const alias = agg.alias || `${agg.function.toLowerCase()}_${exprStr}`;
            
            // Check if the column value is NULL (for proper SQL NULL handling)
            // Note: For complex expressions (CASE, binary expr, functions), we evaluate first
            // since the "col" might be a placeholder like '_case_' or '_expr_'
            let rawVal: any;
            let isNullValue: boolean;
            
            if (col === '*') {
              rawVal = 1;
              isNullValue = false;
            } else if (agg.argExpr && (agg.argExpr.type === 'function' || agg.argExpr.type === 'expression' || agg.argExpr.type === 'between')) {
              // For complex expressions (CASE/IF, binary expr, BETWEEN), evaluate first
              rawVal = evaluateAggregateExpr(agg.argExpr, row);
              isNullValue = rawVal === null || rawVal === undefined;
            } else if (agg.argExpr?.type === 'column') {
              // For column references, check the actual column value
              rawVal = row[agg.argExpr.column!];
              isNullValue = rawVal === null || rawVal === undefined;
            } else {
              // Fallback: check the original column name
              rawVal = row[col];
              isNullValue = rawVal === null || rawVal === undefined;
            }
            
            if (agg.function === 'SUM') {
              // SUM ignores NULL values
              if (!isNullValue) {
                const currentSum = state.sum.get(alias) || 0;
                // Use pre-evaluated rawVal for complex expressions
                const value = (agg.argExpr && (agg.argExpr.type === 'function' || agg.argExpr.type === 'expression' || agg.argExpr.type === 'between'))
                  ? rawVal
                  : (agg.argExpr 
                      ? evaluateAggregateExpr(agg.argExpr, row)
                      : (col === '*' ? 1 : (Number(row[col]) || 0)));
                state.sum.set(alias, currentSum + value * weight);
              }
            } else if (agg.function === 'COUNT') {
              if (agg.distinct) {
                // COUNT(DISTINCT col) - track distinct values with their weights
                let value: any;
                if (col === '*') {
                  value = JSON.stringify(row);
                } else if (agg.argExpr?.type === 'column') {
                  value = row[agg.argExpr.column!];
                } else {
                  value = row[col];
                }
                
                // COUNT(DISTINCT) ignores NULL values
                if (value !== null && value !== undefined) {
                  const valueKey = String(value);
                  
                  let distinctSet = state.distinctSets.get(alias);
                  if (!distinctSet) {
                    distinctSet = new Map();
                    state.distinctSets.set(alias, distinctSet);
                  }
                  
                  const currentWeight = distinctSet.get(valueKey) || 0;
                  const newWeight = currentWeight + weight;
                  
                  if (newWeight <= 0) {
                    distinctSet.delete(valueKey);
                  } else {
                    distinctSet.set(valueKey, newWeight);
                  }
                }
              } else {
                // Regular COUNT - COUNT(*) counts all, COUNT(column) ignores NULL
                if (col === '*' || !isNullValue) {
                  const currentSum = state.sum.get(alias) || 0;
                  state.sum.set(alias, currentSum + weight);
                }
              }
            } else if (agg.function === 'AVG') {
              // For AVG, we track sum and count separately (ignoring NULL values)
              if (!isNullValue) {
                const sumKey = `_sum_for_avg_${alias}`;
                const countKey = `_count_for_avg_${alias}`;
                const currentSum = state.sum.get(sumKey) || 0;
                const currentCount = state.sum.get(countKey) || 0;
                // Use pre-evaluated rawVal for complex expressions
                const value = (agg.argExpr && (agg.argExpr.type === 'function' || agg.argExpr.type === 'expression' || agg.argExpr.type === 'between'))
                  ? rawVal
                  : (agg.argExpr 
                      ? evaluateAggregateExpr(agg.argExpr, row)
                      : (col === '*' ? 1 : (Number(row[col]) || 0)));
                state.sum.set(sumKey, currentSum + value * weight);
                state.sum.set(countKey, currentCount + weight);
              }
            } else if (agg.function === 'MIN' || agg.function === 'MAX') {
              // Track all values for min/max (ignoring NULL values)
              if (!isNullValue) {
                // Use pre-evaluated rawVal for complex expressions
                const value = (agg.argExpr && (agg.argExpr.type === 'function' || agg.argExpr.type === 'expression' || agg.argExpr.type === 'between'))
                  ? rawVal
                  : (agg.argExpr 
                      ? evaluateAggregateExpr(agg.argExpr, row)
                      : (col === '*' ? 1 : (Number(row[col]) || 0)));
                // Simple tracking: just keep running min/max (note: doesn't handle deletes perfectly)
                if (weight > 0) {
                  if (agg.function === 'MIN') {
                    const currentMin = state.min.get(alias);
                    if (currentMin === undefined || value < currentMin) {
                      state.min.set(alias, value);
                    }
                  } else {
                    const currentMax = state.max.get(alias);
                    if (currentMax === undefined || value > currentMax) {
                      state.max.set(alias, value);
                    }
                  }
                }
              }
            }
          }
        }
        
        // Build output delta: for each affected group, emit:
        // - Remove previous aggregated row (weight -1)
        // - Add new aggregated row (weight +1)
        const outputEntries: [any, number][] = [];
        
        for (const groupKey of affectedGroups) {
          const state = groupStates.get(groupKey);
          if (!state) continue;
          
          // Remove previous result if it existed AND it passed HAVING
          const prevResult = previousResults.get(groupKey);
          const prevPassedHaving = prevResult ? (!havingPredicate || havingPredicate(prevResult)) : false;
          if (prevResult && prevPassedHaving) {
            outputEntries.push([prevResult, -1]);
          }
          
          // If count <= 0, group is deleted
          if (state.count <= 0) {
            groupStates.delete(groupKey);
            previousResults.delete(groupKey);
            continue;
          }
          
          // Build new aggregated row
          const newRow: any = {};
          
          // Add group by columns
          // Parse group key back to values (we need a sample row)
          const keyParts = groupKey.split('::');
          for (let i = 0; i < groupByColumns.length; i++) {
            newRow[groupByColumns[i]] = keyParts[i];
          }
          
          // Add simple columns that match group by
          for (const col of simpleColumns) {
            if (groupByColumns.includes(col.name)) {
              const idx = groupByColumns.indexOf(col.name);
              newRow[col.alias || col.name] = keyParts[idx];
            }
          }
          
          // Add aggregates
          for (const agg of aggregates) {
            const col = agg.args[0];
            const exprStr = agg.argExpr ? getExprString(agg.argExpr) : (col === '*' ? 'star' : col);
            const alias = agg.alias || `${agg.function.toLowerCase()}_${exprStr}`;
            
            switch (agg.function) {
              case 'COUNT':
                if (agg.distinct) {
                  // COUNT(DISTINCT) - count unique values in the distinctSet
                  const distinctSet = state.distinctSets.get(alias);
                  newRow[alias] = distinctSet ? distinctSet.size : 0;
                } else {
                  newRow[alias] = state.sum.get(alias) || 0;
                }
                break;
              case 'SUM':
                newRow[alias] = state.sum.get(alias) || 0;
                break;
              case 'AVG': {
                const sumKey = `_sum_for_avg_${alias}`;
                const countKey = `_count_for_avg_${alias}`;
                const sum = state.sum.get(sumKey) || 0;
                const count = state.sum.get(countKey) || 0;
                newRow[alias] = count > 0 ? sum / count : 0;
                break;
              }
              case 'MIN':
                newRow[alias] = state.min.get(alias) ?? 0;
                break;
              case 'MAX':
                newRow[alias] = state.max.get(alias) ?? 0;
                break;
              default:
                newRow[alias] = 0;
            }
          }
          
          // Build aggregate alias map for CASE expression evaluation
          // This maps aggregate patterns to their computed values using canonical keys
          // e.g., "COUNT:*" -> value of count_star
          const aggValueMap: Record<string, any> = { ...newRow };
          
          // Add aggregate keys so CASE expressions can find them
          for (const agg of aggregates) {
            const col = agg.args[0];
            const exprStr = agg.argExpr ? getExprString(agg.argExpr) : (col === '*' ? 'star' : col);
            const alias = agg.alias || `${agg.function.toLowerCase()}_${exprStr}`;
            const val = newRow[alias];
            
            // Store by canonical key (works with both parsed and raw AST formats)
            const canonicalKey = getCanonicalAggregateKey(agg.function, agg.argExpr || (col === '*' ? { type: 'star' } : { type: 'column', column: col }));
            aggValueMap[canonicalKey] = val;
            
            // Also store by common patterns for backwards compatibility
            aggValueMap[`${agg.function}:${col}`] = val;
            aggValueMap[`${agg.function}:star`] = val;
            aggValueMap[`${agg.function.toLowerCase()}_${exprStr}`] = val;
            if (agg.alias) {
              aggValueMap[agg.alias] = val;
            }
          }
          
          // Evaluate CASE columns (post-aggregation expressions)
          for (const caseCol of caseColumns) {
            if (caseCol.alias) {
              newRow[caseCol.alias] = evaluateCaseColumn(caseCol, aggValueMap);
            }
          }
          
          // Add literal columns (constant values like "0 AS avgSlippage")
          for (const lit of literalColumns) {
            if (lit.alias) {
              newRow[lit.alias] = lit.value;
            }
          }
          
          // NOTE: Scalar subquery values will be added via post-processing map
          // because they're not available during aggregation output
          
          // Store new result
          previousResults.set(groupKey, newRow);
          
          // Apply HAVING filter if present
          // Only emit +1 if the row passes HAVING
          const passesHaving = !havingPredicate || havingPredicate(newRow);
          if (passesHaving) {
            outputEntries.push([newRow, 1]);
          }
        }
        
        // Return output delta as ZSet
        const outputKeyFn = (row: any) => JSON.stringify(row);
        return ZSet.fromEntries(outputEntries, outputKeyFn);
      },
      () => {
        groupStates.clear();
        previousResults.clear();
      }
    );
    
    // POST-PROCESSING: Join aggregation results with scalar subquery values
    // The scalar_join operator receives BOTH the aggregation stream AND scalar streams
    // It maintains cumulative scalar values and applies them to each aggregation row
    // CRITICAL: CASE expressions that depend on scalar subqueries must be re-evaluated here
    if (scalarSubqueryStreams.size > 0) {
      // Convert map to array for indexed access
      const scalarEntries = Array.from(scalarSubqueryStreams.entries());
      const scalarStreamIds = scalarEntries.map(([, s]) => s.id);
      const allInputIds = [aggregatedStream.id, ...scalarStreamIds];
      
      // State: accumulated scalar values (persists across steps)
      const scalarValues = new Map<string, any>();
      
      // Track CASE columns and aggregates that may need re-evaluation with scalar values
      const caseCols = caseColumns;
      const aggs = aggregates;
      
      return circuit.addStatefulOperator<any, any>(
        `scalar_join_${aggregatedStream.id}`,
        allInputIds,
        (inputs: ZSet<any>[]) => {
          const aggDelta = inputs[0];
          
          // Process scalar subquery deltas FIRST (they come in inputs[1], inputs[2], etc.)
          for (let i = 0; i < scalarEntries.length; i++) {
            const [alias] = scalarEntries[i];
            const scalarDelta = inputs[i + 1];
            if (scalarDelta) {
              for (const [row, weight] of scalarDelta.entries()) {
                if (weight > 0) {
                  const keys = Object.keys(row);
                  if (keys.length > 0) {
                    scalarValues.set(alias, row[keys[0]]);
                  }
                }
              }
            }
          }
          
          // Now add scalar values to each aggregation row
          const outputEntries: [any, number][] = [];
          for (const [row, weight] of aggDelta.entries()) {
            const result = { ...row };
            
            // Add all scalar values to the row
            for (const [alias] of scalarEntries) {
              result[alias] = scalarValues.get(alias) ?? null;
            }
            
            // Re-evaluate CASE columns that may depend on scalar subqueries
            // Build enhanced aggValueMap with:
            // 1. All current row values (including aggregates under their aliases)
            // 2. Canonical keys for aggregates (so CASE can find them by raw function)
            // 3. Scalar subquery values
            const aggValueMap: Record<string, any> = { ...result };
            
            // Add scalar values
            for (const [alias, val] of scalarValues) {
              aggValueMap[alias] = val;
            }
            
            // Add canonical keys for aggregates that are in the current aggregates list
            for (const agg of aggs) {
              const col = agg.args[0];
              const exprStr = agg.argExpr ? getExprString(agg.argExpr) : (col === '*' ? 'star' : col);
              const alias = agg.alias || `${agg.function.toLowerCase()}_${exprStr}`;
              const val = result[alias];
              if (val !== undefined) {
                // Add canonical key
                const canonicalKey = getCanonicalAggregateKey(agg.function, agg.argExpr || (col === '*' ? { type: 'star' } : { type: 'column', column: col }));
                aggValueMap[canonicalKey] = val;
                // Also add common patterns
                aggValueMap[`${agg.function}:${col}`] = val;
                aggValueMap[`${agg.function.toLowerCase()}_${exprStr}`] = val;
              }
            }
            
            // Re-evaluate each CASE column with the scalar values available
            for (const caseCol of caseCols) {
              if (caseCol.alias) {
                result[caseCol.alias] = evaluateCaseColumn(caseCol, aggValueMap);
              }
            }
            
            outputEntries.push([result, weight]);
          }
          
          return ZSet.fromEntries(outputEntries, (r: any) => JSON.stringify(r));
        },
        () => {
          scalarValues.clear();
        }
      );
    }
    
    return aggregatedStream;
  }

  private compileGlobalAggregation(
    stream: StreamHandle<any>,
    aggregates: AggregateColumn[],
    circuit: Circuit,
    caseColumns: { type: 'case'; conditions: { when: any; then: any }[]; else?: any; alias?: string }[] = [],
    literalColumns: { type: 'literal'; value: number | string | boolean; alias?: string }[] = [],
    scalarSubqueryStreams: Map<string, StreamHandle<any>> = new Map()
  ): StreamHandle<any> {
    // State for global aggregation
    const globalState: {
      sum: Map<string, number>;
      count: number;
      min: Map<string, number>;
      max: Map<string, number>;
      allValues: Map<string, number[]>;
    } = {
      sum: new Map(),
      count: 0,
      min: new Map(),
      max: new Map(),
      allValues: new Map(),
    };
    
    // Previous result for delta computation
    let previousResult: Record<string, any> | null = null;
    
    // Helper to evaluate expression for a row, returning null if the value is null
    const evaluateExpr = (agg: AggregateColumn, row: any): number | null => {
      if (agg.argExpr) {
        return evaluateAggregateExpr(agg.argExpr, row);
      }
      const col = agg.args[0];
      if (col === '*') return 1;
      const rawVal = row[col];
      if (rawVal === null || rawVal === undefined) return null;
      return Number(rawVal) || 0;
    };
    
    // Helper to check if value is null
    // For complex expressions (CASE, function, binary expr), evaluate first
    const isNull = (agg: AggregateColumn, row: any): boolean => {
      const col = agg.args[0];
      if (col === '*') return false;
      
      // For complex expressions, evaluate to check for null
      if (agg.argExpr && (agg.argExpr.type === 'function' || agg.argExpr.type === 'expression' || agg.argExpr.type === 'between')) {
        const val = evaluateAggregateExpr(agg.argExpr, row);
        return val === null || val === undefined;
      }
      if (agg.argExpr?.type === 'column') {
        const rawVal = row[agg.argExpr.column!];
        return rawVal === null || rawVal === undefined;
      }
      
      const rawVal = row[col];
      return rawVal === null || rawVal === undefined;
    };
    
    // Helper to get alias for aggregate
    const getAlias = (agg: AggregateColumn): string => {
      const col = agg.args[0];
      const exprStr = agg.argExpr ? getExprString(agg.argExpr) : (col === '*' ? 'star' : col);
      return agg.alias || `${agg.function.toLowerCase()}_${exprStr}`;
    };
    
    const aggregatedStream = circuit.addStatefulOperator(
      `global_agg_${stream.id}`,
      [stream.id],
      (inputs: ZSet<any>[]) => {
        const delta = inputs[0];
        const outputEntries: [Record<string, any>, number][] = [];
        
        // Emit previous result with weight -1 if it exists
        if (previousResult !== null && globalState.count > 0) {
          outputEntries.push([{ ...previousResult }, -1]);
        }
        
        // Apply delta to global state
        for (const [row, weight] of delta.entries()) {
          // Update count (always for COUNT(*))
          globalState.count += weight;
          
          // Update aggregates
          for (const agg of aggregates) {
            const alias = getAlias(agg);
            const valueIsNull = isNull(agg, row);
            const value = evaluateExpr(agg, row);
            
            switch (agg.function) {
              case 'COUNT':
                // COUNT(*) counts all rows, COUNT(column) ignores NULL
                if (!valueIsNull) {
                  globalState.sum.set(alias, (globalState.sum.get(alias) || 0) + weight);
                }
                break;
                
              case 'SUM':
                // SUM ignores NULL values
                if (!valueIsNull && value !== null) {
                  globalState.sum.set(alias, (globalState.sum.get(alias) || 0) + value * weight);
                }
                break;
                
              case 'AVG': {
                // AVG ignores NULL values - track sum and count separately
                if (!valueIsNull && value !== null) {
                  const sumKey = `_sum_for_avg_${alias}`;
                  const countKey = `_count_for_avg_${alias}`;
                  globalState.sum.set(sumKey, (globalState.sum.get(sumKey) || 0) + value * weight);
                  globalState.sum.set(countKey, (globalState.sum.get(countKey) || 0) + weight);
                }
                break;
              }
              
              case 'MIN':
              case 'MAX': {
                // MIN/MAX ignore NULL values
                if (!valueIsNull && value !== null) {
                  const allVals = globalState.allValues.get(alias) || [];
                  if (weight > 0) {
                    for (let i = 0; i < weight; i++) {
                      allVals.push(value);
                    }
                  } else {
                    for (let i = 0; i < Math.abs(weight); i++) {
                      const idx = allVals.indexOf(value);
                      if (idx !== -1) {
                        allVals.splice(idx, 1);
                      }
                    }
                  }
                  globalState.allValues.set(alias, allVals);
                }
                break;
              }
            }
          }
        }
        
        // Only emit result if we have data
        if (globalState.count > 0) {
          // Build new result row
          const newRow: Record<string, any> = {};
          
          for (const agg of aggregates) {
            const alias = getAlias(agg);
            
            switch (agg.function) {
              case 'COUNT':
              case 'SUM':
                newRow[alias] = globalState.sum.get(alias) || 0;
                break;
                
              case 'AVG': {
                const sumKey = `_sum_for_avg_${alias}`;
                const countKey = `_count_for_avg_${alias}`;
                const sum = globalState.sum.get(sumKey) || 0;
                const count = globalState.sum.get(countKey) || 0;
                newRow[alias] = count > 0 ? sum / count : 0;
                break;
              }
              
              case 'MIN': {
                const allVals = globalState.allValues.get(alias) || [];
                newRow[alias] = allVals.length > 0 ? Math.min(...allVals) : 0;
                break;
              }
              
              case 'MAX': {
                const allVals = globalState.allValues.get(alias) || [];
                newRow[alias] = allVals.length > 0 ? Math.max(...allVals) : 0;
                break;
              }
            }
          }
          
          // Build aggregate alias map for CASE expression evaluation using canonical keys
          const aggValueMap: Record<string, any> = { ...newRow };
          
          // Add aggregate keys so CASE expressions can find them
          for (const agg of aggregates) {
            const alias = getAlias(agg);
            const col = agg.args[0];
            const exprStr = agg.argExpr ? getExprString(agg.argExpr) : (col === '*' ? 'star' : col);
            const val = newRow[alias];
            
            // Store by canonical key (works with both parsed and raw AST formats)
            const canonicalKey = getCanonicalAggregateKey(agg.function, agg.argExpr || (col === '*' ? { type: 'star' } : { type: 'column', column: col }));
            aggValueMap[canonicalKey] = val;
            
            // Also store by common patterns for backwards compatibility
            aggValueMap[`${agg.function}:${col}`] = val;
            aggValueMap[`${agg.function}:star`] = val;
            aggValueMap[`${agg.function.toLowerCase()}_${exprStr}`] = val;
            if (agg.alias) {
              aggValueMap[agg.alias] = val;
            }
          }
          
          // Evaluate CASE columns (post-aggregation expressions)
          for (const caseCol of caseColumns) {
            if (caseCol.alias) {
              newRow[caseCol.alias] = evaluateCaseColumn(caseCol, aggValueMap);
            }
          }
          
          // Add literal columns (constant values like "0 AS avgSlippage")
          for (const lit of literalColumns) {
            if (lit.alias) {
              newRow[lit.alias] = lit.value;
            }
          }
          
          // NOTE: Scalar subquery values will be added via post-processing map
          // because they're not available during aggregation output
          
          // Emit new result with weight +1
          outputEntries.push([newRow, 1]);
          previousResult = newRow;
        } else {
          // All data deleted
          previousResult = null;
        }
        
        // Return delta as ZSet
        const outputKeyFn = (row: any) => JSON.stringify(row);
        return ZSet.fromEntries(outputEntries, outputKeyFn);
      },
      () => {
        // Reset state
        globalState.sum.clear();
        globalState.count = 0;
        globalState.min.clear();
        globalState.max.clear();
        globalState.allValues.clear();
        previousResult = null;
      }
    );
    
    // POST-PROCESSING: Join aggregation results with scalar subquery values
    if (scalarSubqueryStreams.size > 0) {
      const scalarEntries = Array.from(scalarSubqueryStreams.entries());
      const scalarStreamIds = scalarEntries.map(([, s]) => s.id);
      const allInputIds = [aggregatedStream.id, ...scalarStreamIds];
      
      const scalarValues = new Map<string, any>();
      const caseCols = caseColumns;
      
      return circuit.addStatefulOperator<any, any>(
        `scalar_join_global_${aggregatedStream.id}`,
        allInputIds,
        (inputs: ZSet<any>[]) => {
          const aggDelta = inputs[0];
          
          // Process scalar subquery deltas FIRST
          for (let i = 0; i < scalarEntries.length; i++) {
            const [alias] = scalarEntries[i];
            const scalarDelta = inputs[i + 1];
            if (scalarDelta) {
              for (const [row, weight] of scalarDelta.entries()) {
                if (weight > 0) {
                  const keys = Object.keys(row);
                  if (keys.length > 0) {
                    scalarValues.set(alias, row[keys[0]]);
                  }
                }
              }
            }
          }
          
          // Add scalar values and re-evaluate CASE columns
          const outputEntries: [any, number][] = [];
          for (const [row, weight] of aggDelta.entries()) {
            const result = { ...row };
            
            // Add all scalar values
            for (const [alias] of scalarEntries) {
              result[alias] = scalarValues.get(alias) ?? null;
            }
            
            // Re-evaluate CASE columns with scalar values
            const aggValueMap: Record<string, any> = { ...result };
            for (const [alias, val] of scalarValues) {
              aggValueMap[alias] = val;
            }
            
            // Add canonical keys for aggregates
            for (const agg of aggregates) {
              const col = agg.args[0];
              const exprStr = agg.argExpr ? getExprString(agg.argExpr) : (col === '*' ? 'star' : col);
              const alias = agg.alias || `${agg.function.toLowerCase()}_${exprStr}`;
              const val = result[alias];
              if (val !== undefined) {
                const canonicalKey = getCanonicalAggregateKey(agg.function, agg.argExpr || (col === '*' ? { type: 'star' } : { type: 'column', column: col }));
                aggValueMap[canonicalKey] = val;
                aggValueMap[`${agg.function}:${col}`] = val;
                aggValueMap[`${agg.function.toLowerCase()}_${exprStr}`] = val;
              }
            }
            
            for (const caseCol of caseCols) {
              if (caseCol.alias) {
                result[caseCol.alias] = evaluateCaseColumn(caseCol, aggValueMap);
              }
            }
            
            outputEntries.push([result, weight]);
          }
          
          return ZSet.fromEntries(outputEntries, (r: any) => JSON.stringify(r));
        },
        () => {
          scalarValues.clear();
        }
      );
    }
    
    return aggregatedStream;
  }

  /**
   * Compile set operations: UNION, EXCEPT, INTERSECT
   */
  private compileSetOperation(
    query: SetOperationQuery,
    tables: Record<string, StreamHandle<any>>,
    circuit: Circuit
  ): StreamHandle<any> | null {
    const left = this.compileSelect(query.left, tables, circuit);
    const right = this.compileSelect(query.right, tables, circuit);
    
    if (!left || !right) {
      return null;
    }
    
    let result: StreamHandle<any>;
    
    switch (query.type) {
      case 'UNION':
        result = left.union(right);
        break;
        
      case 'EXCEPT':
        result = left.subtract(right);
        break;
        
      case 'INTERSECT':
        result = left.intersect(right);
        break;
        
      default:
        result = left.union(right);
    }
    
    if (!query.all) {
      // Without ALL, apply distinct (set semantics vs bag semantics)
      return result.distinct();
    }
    
    return result;
  }

  private compileWhere(where: WhereCondition): (row: any) => boolean {
    switch (where.type) {
      case 'COMPARISON':
        return this.compileComparison(where);
      case 'AND':
        return this.compileAnd(where);
      case 'OR':
        return this.compileOr(where);
      case 'BETWEEN':
        return this.compileBetween(where);
      case 'IN':
        return this.compileIn(where);
      case 'IS_NULL':
        return this.compileIsNull(where);
      case 'IS_NOT_NULL':
        return this.compileIsNotNull(where);
      case 'NOT':
        return this.compileNot(where);
      case 'LIKE':
        return this.compileLike(where);
      case 'REGEXP':
        return this.compileRegexp(where);
      case 'EXISTS':
        console.warn('compileWhere called with EXISTS - should use compileExistsSubquery');
        return () => false;
      case 'AGGREGATE_COMPARISON':
        throw new Error('AGGREGATE_COMPARISON should be handled via compileHaving');
      case 'EXPRESSION_COMPARISON':
        return this.compileExpressionComparison(where);
    }
  }
  
  /**
   * Compile an expression comparison (e.g., price * qty > 50)
   */
  private compileExpressionComparison(cond: ExpressionComparison): (row: any) => boolean {
    return (row: any) => {
      const leftValue = evaluateAggregateExpr(cond.leftExpr, row);
      const rightValue = cond.rightExpr 
        ? evaluateAggregateExpr(cond.rightExpr, row) 
        : Number(cond.value);
      
      switch (cond.operator) {
        case '=': return leftValue === rightValue;
        case '!=':
        case '<>': return leftValue !== rightValue;
        case '<': return leftValue < rightValue;
        case '>': return leftValue > rightValue;
        case '<=': return leftValue <= rightValue;
        case '>=': return leftValue >= rightValue;
        default: return false;
      }
    };
  }
  
  /**
   * Compile REGEXP condition
   */
  private compileRegexp(regexp: RegexpCondition): (row: any) => boolean {
    const { column, pattern, caseInsensitive } = regexp;
    const flags = caseInsensitive ? 'i' : '';
    
    // Pre-compile the regex for efficiency
    let regex: RegExp;
    try {
      regex = new RegExp(pattern, flags);
    } catch (e) {
      console.error(`Invalid regex pattern: ${pattern}`);
      return () => false;
    }
    
    return (row: any) => {
      const value = row[column];
      if (value == null) return false;
      return regex.test(String(value));
    };
  }

  /**
   * Evaluate an arithmetic expression column (e.g., price * quantity)
   */
  private evaluateExpression(expr: ExpressionColumn, row: any): number {
    const left = this.evaluateExprOperand(expr.left, row);
    const right = this.evaluateExprOperand(expr.right, row);
    
    switch (expr.operator) {
      case '+': return left + right;
      case '-': return left - right;
      case '*': return left * right;
      case '/': return right !== 0 ? left / right : 0;
      case '%': return right !== 0 ? left % right : 0;
      default: return 0;
    }
  }

  /**
   * Evaluate an operand in an expression (can be column ref, literal, or nested expression)
   */
  private evaluateExprOperand(operand: any, row: any): number {
    if (!operand) return 0;
    
    if (operand.type === 'column_ref') {
      return Number(row[operand.column]) || 0;
    } else if (operand.type === 'number') {
      return operand.value || 0;
    } else if (operand.type === 'binary_expr') {
      // Nested expression: evaluate recursively
      const left = this.evaluateExprOperand(operand.left, row);
      const right = this.evaluateExprOperand(operand.right, row);
      switch (operand.operator) {
        case '+': return left + right;
        case '-': return left - right;
        case '*': return left * right;
        case '/': return right !== 0 ? left / right : 0;
        case '%': return right !== 0 ? left % right : 0;
        default: return 0;
      }
    }
    
    return 0;
  }

  /**
   * Compile a HAVING condition into a predicate function
   */
  private compileHaving(
    having: WhereCondition, 
    aggregateAliasMap: Map<string, string>
  ): (row: any) => boolean {
    switch (having.type) {
      case 'AGGREGATE_COMPARISON': {
        const key = `${having.aggregateFunc}:${having.aggregateArg}`;
        const alias = aggregateAliasMap.get(key) || key;
        
        return (row: any) => {
          const rowValue = row[alias];
          const compareValue = having.value;
          
          switch (having.operator) {
            case '=': return rowValue === compareValue;
            case '!=': return rowValue !== compareValue;
            case '<': return rowValue < compareValue;
            case '>': return rowValue > compareValue;
            case '<=': return rowValue <= compareValue;
            case '>=': return rowValue >= compareValue;
            default: return false;
          }
        };
      }
      case 'AND':
        return (row: any) => having.conditions.every(
          c => this.compileHaving(c, aggregateAliasMap)(row)
        );
      case 'OR':
        return (row: any) => having.conditions.some(
          c => this.compileHaving(c, aggregateAliasMap)(row)
        );
      default:
        return this.compileWhere(having);
    }
  }

  private compileComparison(cond: ComparisonCondition): (row: any) => boolean {
    // Handle literal-to-literal comparison (e.g., 'ALL' = 'ALL')
    // This is used for the pattern: ($param = 'ALL' OR column = $param)
    if (cond.leftLiteral !== undefined) {
      // Both sides are known at compile time - pre-evaluate
      const leftValue = cond.leftLiteral;
      const rightValue = cond.value;
      
      let result: boolean;
      switch (cond.operator) {
        case '=': result = leftValue === rightValue; break;
        case '!=':
        case '<>': result = leftValue !== rightValue; break;
        case '<': result = leftValue < rightValue; break;
        case '>': result = leftValue > rightValue; break;
        case '<=': result = leftValue <= rightValue; break;
        case '>=': result = leftValue >= rightValue; break;
        default: result = false;
      }
      
      // Return a constant function - no need to check row
      return () => result;
    }
    
    // Column-to-column comparison (e.g., WHERE a = b)
    if (cond.rightColumn !== undefined) {
      return (row: any) => {
        const leftValue = row[cond.column];
        const rightValue = row[cond.rightColumn!];
        
        // Three-valued NULL logic: NULL compared with anything is NULL (false in WHERE context)
        if (leftValue === null || leftValue === undefined || 
            rightValue === null || rightValue === undefined) {
          return false; // NULL comparisons filter out rows
        }
        
        switch (cond.operator) {
          case '=': return leftValue === rightValue;
          case '!=':
          case '<>': return leftValue !== rightValue;
          case '<': return leftValue < rightValue;
          case '>': return leftValue > rightValue;
          case '<=': return leftValue <= rightValue;
          case '>=': return leftValue >= rightValue;
          default: return false;
        }
      };
    }
    
    // Normal column-to-value comparison
    return (row: any) => {
      const rowValue = row[cond.column];
      const compareValue = cond.value;
      
      // Three-valued NULL logic: NULL compared with anything is NULL (false in WHERE context)
      if (rowValue === null || rowValue === undefined) {
        return false; // NULL comparisons filter out rows
      }
      
      switch (cond.operator) {
        case '=': return rowValue === compareValue;
        case '!=':
        case '<>': return rowValue !== compareValue;
        case '<': return rowValue < compareValue;
        case '>': return rowValue > compareValue;
        case '<=': return rowValue <= compareValue;
        case '>=': return rowValue >= compareValue;
        default: return false;
      }
    };
  }

  /**
   * Compile AND condition with three-valued NULL logic.
   * NULL AND TRUE = NULL (false in WHERE)
   * NULL AND FALSE = FALSE
   */
  private compileAnd(cond: AndCondition): (row: any) => boolean {
    const predicates = cond.conditions.map(c => this.compileWhere(c));
    return (row: any) => {
      // In three-valued logic, AND is true only if ALL conditions are true
      // If any is false, result is false
      // If any is null and none are false, result is null (treated as false in WHERE)
      for (const p of predicates) {
        if (!p(row)) return false;
      }
      return true;
    };
  }

  /**
   * Compile OR condition with three-valued NULL logic.
   * NULL OR TRUE = TRUE
   * NULL OR FALSE = NULL (false in WHERE)
   */
  private compileOr(cond: OrCondition): (row: any) => boolean {
    const predicates = cond.conditions.map(c => this.compileWhere(c));
    return (row: any) => {
      // In three-valued logic, OR is true if ANY condition is true
      for (const p of predicates) {
        if (p(row)) return true;
      }
      return false;
    };
  }

  private compileBetween(cond: BetweenCondition): (row: any) => boolean {
    return (row: any) => {
      const value = row[cond.column];
      
      // Three-valued NULL logic: NULL BETWEEN x AND y is NULL (false)
      if (value === null || value === undefined) {
        return false;
      }
      
      const result = value >= cond.low && value <= cond.high;
      return cond.not ? !result : result;
    };
  }

  /**
   * Find all IN subquery conditions in a WHERE clause
   */
  private findInSubqueries(where: WhereCondition): InCondition[] {
    const results: InCondition[] = [];
    
    if (where.type === 'IN' && (where as InCondition).subquery) {
      results.push(where as InCondition);
    } else if (where.type === 'AND') {
      for (const cond of (where as AndCondition).conditions) {
        results.push(...this.findInSubqueries(cond));
      }
    } else if (where.type === 'OR') {
      for (const cond of (where as OrCondition).conditions) {
        results.push(...this.findInSubqueries(cond));
      }
    }
    
    return results;
  }
  
  /**
   * Find all EXISTS subquery conditions in a WHERE clause
   */
  private findExistsSubqueries(where: WhereCondition): ExistsCondition[] {
    const results: ExistsCondition[] = [];
    
    if (where.type === 'EXISTS') {
      results.push(where as ExistsCondition);
    } else if (where.type === 'AND') {
      for (const cond of (where as AndCondition).conditions) {
        results.push(...this.findExistsSubqueries(cond));
      }
    } else if (where.type === 'OR') {
      for (const cond of (where as OrCondition).conditions) {
        results.push(...this.findExistsSubqueries(cond));
      }
    }
    
    return results;
  }
  
  /**
   * Remove IN and EXISTS subquery conditions from a WHERE clause
   */
  private removeSubqueries(where: WhereCondition): WhereCondition | null {
    if (where.type === 'IN' && (where as InCondition).subquery) {
      return null;
    }
    if (where.type === 'EXISTS') {
      return null;
    }
    
    if (where.type === 'AND') {
      const remaining = (where as AndCondition).conditions
        .map(c => this.removeSubqueries(c))
        .filter((c): c is WhereCondition => c !== null);
      
      if (remaining.length === 0) return null;
      if (remaining.length === 1) return remaining[0];
      return { type: 'AND', conditions: remaining };
    }
    
    if (where.type === 'OR') {
      const remaining = (where as OrCondition).conditions
        .map(c => this.removeSubqueries(c))
        .filter((c): c is WhereCondition => c !== null);
      
      if (remaining.length === 0) return null;
      if (remaining.length === 1) return remaining[0];
      return { type: 'OR', conditions: remaining };
    }
    
    return where;
  }

  private compileIn(cond: InCondition): (row: any) => boolean {
    if (cond.subquery) {
      console.warn('compileIn called with subquery - should use compileInSubquery');
      return () => false;
    }
    
    const valueSet = new Set(cond.values);
    return (row: any) => {
      const value = row[cond.column];
      
      // Three-valued NULL logic: NULL IN (...) is NULL (false in WHERE)
      if (value === null || value === undefined) {
        return false;
      }
      
      const result = valueSet.has(value);
      return cond.not ? !result : result;
    };
  }

  /**
   * Compile EXISTS subquery as a semi-join
   */
  private compileExistsSubquery(
    stream: StreamHandle<any>,
    cond: ExistsCondition,
    tables: Record<string, StreamHandle<any>>,
    circuit: Circuit
  ): StreamHandle<any> | null {
    const subTableName = cond.subquery.table;
    const leftColumn = cond.subquery.leftColumn;
    const rightColumn = cond.subquery.rightColumn;
    
    const subTable = tables[subTableName];
    if (!subTable) {
      console.error(`EXISTS subquery table ${subTableName} not found`);
      return stream;
    }
    
    const leftKeyFn = (row: any) => String(row[leftColumn]);
    const rightKeyFn = (row: any) => String(row[rightColumn]);
    
    // State for tracking
    const rightKeys = new Map<string, number>();
    const leftValues = new Map<string, any[]>();
    const previousResults = new Map<string, any>();
    
    return circuit.addStatefulOperator<any, any>(
      `exists_subquery_${stream.id}_${subTableName}`,
      [stream.id, subTable.id],
      (inputs: ZSet<any>[]) => {
        const leftDelta = inputs[0];
        const rightDelta = inputs[1];
        const outputEntries: [any, number][] = [];
        
        // Process right delta first (subquery table)
        for (const [row, weight] of rightDelta.entries()) {
          const key = rightKeyFn(row);
          const prevCount = rightKeys.get(key) || 0;
          const newCount = prevCount + weight;
          
          if (newCount > 0) {
            rightKeys.set(key, newCount);
          } else {
            rightKeys.delete(key);
          }
          
          const wasPresent = prevCount > 0;
          const isPresent = newCount > 0;
          
          if (wasPresent !== isPresent) {
            const matchingLeftRows = leftValues.get(key) || [];
            for (const leftRow of matchingLeftRows) {
              const leftKey = JSON.stringify(leftRow);
              if (isPresent) {
                if (!previousResults.has(leftKey)) {
                  previousResults.set(leftKey, leftRow);
                  outputEntries.push([leftRow, 1]);
                }
              } else {
                if (previousResults.has(leftKey)) {
                  previousResults.delete(leftKey);
                  outputEntries.push([leftRow, -1]);
                }
              }
            }
          }
        }
        
        // Process left delta
        for (const [row, weight] of leftDelta.entries()) {
          const key = leftKeyFn(row);
          const leftKey = JSON.stringify(row);
          
          if (weight > 0) {
            let keyRows = leftValues.get(key);
            if (!keyRows) {
              keyRows = [];
              leftValues.set(key, keyRows);
            }
            keyRows.push(row);
            
            if (rightKeys.has(key)) {
              if (!previousResults.has(leftKey)) {
                previousResults.set(leftKey, row);
                outputEntries.push([row, 1]);
              }
            }
          } else {
            const keyRows = leftValues.get(key);
            if (keyRows) {
              const idx = keyRows.findIndex(r => JSON.stringify(r) === leftKey);
              if (idx !== -1) {
                keyRows.splice(idx, 1);
              }
            }
            
            if (previousResults.has(leftKey)) {
              previousResults.delete(leftKey);
              outputEntries.push([row, -1]);
            }
          }
        }
        
        return ZSet.fromEntries(outputEntries);
      },
      () => {
        rightKeys.clear();
        leftValues.clear();
        previousResults.clear();
      }
    );
  }

  /**
   * Compile IN subquery as a semi-join
   */
  private compileInSubquery(
    stream: StreamHandle<any>,
    cond: InCondition,
    tables: Record<string, StreamHandle<any>>,
    circuit: Circuit
  ): StreamHandle<any> | null {
    if (!cond.subquery) return stream;
    
    const subTableName = cond.subquery.table;
    const subColumn = cond.subquery.column;
    const leftColumn = cond.column;
    
    const subTable = tables[subTableName];
    if (!subTable) {
      console.error(`Subquery table ${subTableName} not found`);
      return stream;
    }
    
    const leftKeyFn = (row: any) => String(row[leftColumn]);
    const rightKeyFn = (row: any) => String(row[subColumn]);
    
    // State for tracking right keys
    const rightKeys = new Map<string, number>(); // key -> count
    const leftValues = new Map<string, any[]>(); // key -> rows
    const previousResults = new Map<string, any>(); // key -> row
    
    return circuit.addStatefulOperator<any, any>(
      `in_subquery_${stream.id}_${cond.subquery.table}`,
      [stream.id, subTable.id],
      (inputs: ZSet<any>[]) => {
        const leftDelta = inputs[0];
        const rightDelta = inputs[1];
        const outputEntries: [any, number][] = [];
        
        // Process right delta first (subquery table)
        for (const [row, weight] of rightDelta.entries()) {
          const key = rightKeyFn(row);
          const prevCount = rightKeys.get(key) || 0;
          const newCount = prevCount + weight;
          
          if (newCount > 0) {
            rightKeys.set(key, newCount);
          } else {
            rightKeys.delete(key);
          }
          
          // If key existence changed, update matching left rows
          const wasPresent = prevCount > 0;
          const isPresent = newCount > 0;
          
          if (wasPresent !== isPresent) {
            const matchingLeftRows = leftValues.get(key) || [];
            for (const leftRow of matchingLeftRows) {
              const leftKey = JSON.stringify(leftRow);
              if (isPresent) {
                // Key now exists - add to results
                if (!previousResults.has(leftKey)) {
                  previousResults.set(leftKey, leftRow);
                  outputEntries.push([leftRow, 1]);
                }
              } else {
                // Key no longer exists - remove from results
                if (previousResults.has(leftKey)) {
                  previousResults.delete(leftKey);
                  outputEntries.push([leftRow, -1]);
                }
              }
            }
          }
        }
        
        // Process left delta
        for (const [row, weight] of leftDelta.entries()) {
          const key = leftKeyFn(row);
          const leftKey = JSON.stringify(row);
          
          if (weight > 0) {
            // Insert
            let keyRows = leftValues.get(key);
            if (!keyRows) {
              keyRows = [];
              leftValues.set(key, keyRows);
            }
            keyRows.push(row);
            
            // Check if right key exists
            if (rightKeys.has(key)) {
              if (!previousResults.has(leftKey)) {
                previousResults.set(leftKey, row);
                outputEntries.push([row, 1]);
              }
            }
          } else {
            // Delete
            const keyRows = leftValues.get(key);
            if (keyRows) {
              const idx = keyRows.findIndex(r => JSON.stringify(r) === leftKey);
              if (idx !== -1) {
                keyRows.splice(idx, 1);
              }
            }
            
            // Remove from results if present
            if (previousResults.has(leftKey)) {
              previousResults.delete(leftKey);
              outputEntries.push([row, -1]);
            }
          }
        }
        
        return ZSet.fromEntries(outputEntries);
      },
      () => {
        rightKeys.clear();
        leftValues.clear();
        previousResults.clear();
      }
    );
  }

  private compileIsNull(cond: IsNullCondition): (row: any) => boolean {
    return (row: any) => row[cond.column] === null || row[cond.column] === undefined;
  }

  private compileIsNotNull(cond: IsNotNullCondition): (row: any) => boolean {
    return (row: any) => row[cond.column] !== null && row[cond.column] !== undefined;
  }

  private compileNot(cond: NotCondition): (row: any) => boolean {
    const innerPredicate = this.compileWhere(cond.condition);
    return (row: any) => !innerPredicate(row);
  }

  private compileLike(cond: LikeCondition): (row: any) => boolean {
    // Convert SQL LIKE pattern to regex
    const pattern = cond.pattern
      .replace(/%/g, '.*')
      .replace(/_/g, '.');
    const regex = new RegExp(`^${pattern}$`, 'i');
    
    return (row: any) => {
      const value = row[cond.column];
      
      // Three-valued NULL logic: NULL LIKE pattern is NULL (false in WHERE)
      if (value === null || value === undefined) {
        return false;
      }
      
      const result = typeof value === 'string' && regex.test(value);
      return cond.not ? !result : result;
    };
  }
}

