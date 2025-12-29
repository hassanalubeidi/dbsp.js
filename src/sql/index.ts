/**
 * SQL Module
 * ===========
 * 
 * SQL to DBSP compiler that converts SQL statements into incremental streaming circuits.
 * 
 * ## Quick Start
 * 
 * ```ts
 * import { SQLCompiler } from './sql';
 * 
 * const compiler = new SQLCompiler();
 * const { circuit, tables, views } = compiler.compile(`
 *   CREATE TABLE orders (id INT, amount DECIMAL, status VARCHAR);
 *   CREATE VIEW pending AS SELECT * FROM orders WHERE status = 'pending';
 * `);
 * ```
 * 
 * @module
 */

// Re-export all AST types
export * from './ast-types';

// Re-export expression evaluation functions
export { evaluateAggregateExpr, evaluateFunctionExprGeneric, getExprString } from './expression-eval';

// Re-export parser
export { SQLParser } from './parser';

// Re-export compiler
export { SQLCompiler, type CompileResult } from './compiler';

// Re-export join projection utilities
export {
  parseJoinProjection,
  createJoinProjector,
  applyJoinProjection,
  extractJoinTables,
  isJoinQuery,
  createProjectorFromQuery,
  type JoinProjection,
  type ColumnMapping,
} from './join-projection';
