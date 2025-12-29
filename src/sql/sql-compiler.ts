/**
 * SQL to DBSP Compiler
 * =====================
 * 
 * This file re-exports from the modular SQL compiler components for backwards compatibility.
 * 
 * The SQL compiler has been split into:
 * - `ast-types.ts` - Type definitions for the SQL AST
 * - `expression-eval.ts` - Expression evaluation functions
 * - `parser.ts` - SQLParser class
 * - `compiler.ts` - SQLCompiler class
 * 
 * You can import directly from this file or from the individual modules.
 * 
 * @module
 */

// Re-export everything from the split modules
export * from './ast-types';
export * from './expression-eval';
export * from './parser';
export * from './compiler';
