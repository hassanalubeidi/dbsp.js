/**
 * SQL AST Type Definitions
 * =========================
 * 
 * Type definitions for the SQL Abstract Syntax Tree used by the DBSP SQL compiler.
 * These types represent parsed SQL statements, conditions, columns, and queries.
 * 
 * @module
 */

// ============ TABLE DEFINITIONS ============

export interface ColumnDef {
  name: string;
  type: string;
}

export interface CreateTableStatement {
  type: 'CREATE_TABLE';
  tableName: string;
  columns: ColumnDef[];
}

// ============ WHERE CONDITION TYPES ============

export interface ComparisonCondition {
  type: 'COMPARISON';
  column: string;
  operator: '=' | '!=' | '<>' | '<' | '>' | '<=' | '>=';
  value: string | number | boolean;
  /** When left side is a literal (not a column), this holds the literal value */
  leftLiteral?: string | number | boolean;
  /** When right side is a column (not a literal), this holds the column name */
  rightColumn?: string;
}

export interface AndCondition {
  type: 'AND';
  conditions: WhereCondition[];
}

export interface OrCondition {
  type: 'OR';
  conditions: WhereCondition[];
}

export interface BetweenCondition {
  type: 'BETWEEN';
  column: string;
  low: number;
  high: number;
  /** True if this is a NOT BETWEEN */
  not?: boolean;
}

export interface InCondition {
  type: 'IN';
  column: string;
  values: (string | number)[];
  // For IN (SELECT ...) subqueries
  subquery?: {
    table: string;
    column: string;
  };
  /** True if this is a NOT IN */
  not?: boolean;
}

export interface ExistsCondition {
  type: 'EXISTS';
  subquery: {
    table: string;
    leftColumn: string;  // Column from outer table
    rightColumn: string; // Column from subquery table
    leftTable?: string;  // Optional table name for the left column
  };
}

export interface IsNullCondition {
  type: 'IS_NULL';
  column: string;
}

export interface IsNotNullCondition {
  type: 'IS_NOT_NULL';
  column: string;
}

export interface NotCondition {
  type: 'NOT';
  condition: WhereCondition;
}

export interface LikeCondition {
  type: 'LIKE';
  column: string;
  pattern: string;
  /** True if this is a NOT LIKE */
  not?: boolean;
}

// Regular expression match (REGEXP, RLIKE, ~)
export interface RegexpCondition {
  type: 'REGEXP';
  column: string;
  pattern: string;
  caseInsensitive?: boolean;  // For ~* operator
}

// HAVING condition that references aggregate functions
export interface AggregateComparison {
  type: 'AGGREGATE_COMPARISON';
  aggregateFunc: string;  // e.g., 'SUM', 'COUNT'
  aggregateArg: string;   // e.g., 'amount', '*'
  operator: ComparisonCondition['operator'];
  value: number;
}

// Condition with expression on left side (e.g., price * qty > 50)
export interface ExpressionComparison {
  type: 'EXPRESSION_COMPARISON';
  leftExpr: AggregateArg;  // The expression to evaluate
  operator: ComparisonCondition['operator'];
  value: number | string | boolean;
  rightExpr?: AggregateArg;  // Optional: for expression-to-expression comparison
}

export type WhereCondition = 
  | ComparisonCondition 
  | AndCondition 
  | OrCondition 
  | BetweenCondition
  | InCondition
  | IsNullCondition
  | IsNotNullCondition
  | NotCondition
  | LikeCondition
  | RegexpCondition
  | AggregateComparison
  | ExpressionComparison
  | ExistsCondition;

// ============ COLUMN TYPES ============

export interface SimpleColumn {
  type: 'column';
  name: string;
  alias?: string;
  /** Table alias/name when column is qualified (e.g., t.name) */
  table?: string;
}

// Expression argument for aggregates (can be column, *, or binary expression)
export interface AggregateArg {
  type: 'column' | 'star' | 'expression' | 'function' | 'between' | 'scalar_subquery';
  column?: string;
  table?: string;         // Table qualifier: table.column
  operator?: string;
  left?: AggregateArg;
  right?: AggregateArg;
  value?: number;  // For literal numbers
  stringValue?: string;  // For literal strings (e.g., 'FILLED', 'PENDING')
  // Function-specific fields
  functionName?: string;  // e.g., 'ABS', 'SIGN', 'IF', 'COALESCE', 'NULLIF'
  args?: AggregateArg[];  // Function arguments
  // BETWEEN-specific fields
  low?: AggregateArg;
  high?: AggregateArg;
  // Scalar subquery in expression: col / (SELECT SUM(col) FROM table)
  subquery?: Query;
}

export interface AggregateColumn {
  type: 'aggregate';
  function: 'COUNT' | 'SUM' | 'AVG' | 'MIN' | 'MAX';
  args: string[];  // Keep for backwards compatibility (simple column name)
  argExpr?: AggregateArg;  // New: full expression support
  alias?: string;
  distinct?: boolean;  // For COUNT(DISTINCT col)
}

export interface ExpressionColumn {
  type: 'expression';
  operator: string;
  left: any;
  right: any;
  alias?: string;
  /** True if this is a unary expression (e.g., -a, +a) */
  unary?: boolean;
}

export interface CaseColumn {
  type: 'case';
  conditions: { when: any; then: any }[];
  else?: any;
  alias?: string;
}

export interface FunctionColumn {
  type: 'function';
  function: string;
  args: string[];
  argExprs?: AggregateArg[];  // Full expression support for COALESCE, NULLIF etc.
  alias?: string;
}

export interface CastColumn {
  type: 'cast';
  expr: any;
  targetType: string;
  alias?: string;
}

// Window function (LAG, LEAD, ROW_NUMBER, etc. with OVER clause)
export interface WindowColumn {
  type: 'window';
  function: 'LAG' | 'LEAD' | 'ROW_NUMBER' | 'RANK' | 'DENSE_RANK' | 'FIRST_VALUE' | 'LAST_VALUE' | 'NTILE' | 'PERCENT_RANK' | 'CUME_DIST' | 'SUM' | 'AVG' | 'COUNT' | 'MIN' | 'MAX';
  args: AggregateArg[];           // Function arguments (e.g., column, offset, default)
  partitionBy?: string[];         // PARTITION BY columns
  orderBy?: { column: string; direction: 'ASC' | 'DESC' }[];  // ORDER BY columns
  frame?: {                       // ROWS/RANGE BETWEEN frame
    type: 'ROWS' | 'RANGE';
    start: { type: 'UNBOUNDED' | 'CURRENT' | 'PRECEDING' | 'FOLLOWING'; offset?: number };
    end: { type: 'UNBOUNDED' | 'CURRENT' | 'PRECEDING' | 'FOLLOWING'; offset?: number };
  };
  alias?: string;
}

// Scalar subquery in SELECT: (SELECT MAX(price) FROM prices)
export interface ScalarSubqueryColumn {
  type: 'scalar_subquery';
  query: Query;                   // The subquery
  alias?: string;
}

// Literal value in SELECT: 0 AS avgSlippage, 'UNKNOWN' AS default_status
export interface LiteralColumn {
  type: 'literal';
  value: number | string | boolean;
  alias?: string;
}

export type SelectColumn = SimpleColumn | AggregateColumn | ExpressionColumn | CaseColumn | FunctionColumn | CastColumn | WindowColumn | ScalarSubqueryColumn | LiteralColumn | '*';

// ============ JOIN TYPES ============

/** Single join condition (for equi-joins and non-equi joins) */
export interface JoinCondition {
  leftColumn: string;
  leftTable?: string;
  rightColumn: string;
  rightTable?: string;
  operator: '=' | '!=' | '<' | '>' | '<=' | '>=' | 'BETWEEN';
  // For BETWEEN conditions: leftColumn BETWEEN rightColumn AND betweenHigh
  betweenHigh?: string;
  betweenHighTable?: string;
  // For column-to-literal comparisons in join conditions (e.g., c.tier = 'premium')
  rightLiteral?: string | number | boolean;
}

export interface JoinInfo {
  type: 'INNER' | 'LEFT' | 'RIGHT' | 'FULL' | 'CROSS';
  table: string;           // Table name OR empty if using derived table
  tableAlias?: string;     // Alias for the table
  derivedTable?: Query;    // For derived tables (subqueries in FROM)
  // Legacy single-column support (for backwards compatibility)
  leftColumn: string;
  leftTable?: string;
  rightColumn: string;
  rightTable?: string;
  // NEW: Multiple join conditions support (composite keys, non-equi joins)
  conditions?: JoinCondition[];
}

export interface OrderByItem {
  column: string;
  direction: 'ASC' | 'DESC';
  /** Ordinal position (1-indexed) when ORDER BY 1, 2, etc. is used */
  ordinal?: number;
}

// ============ QUERY TYPES ============

/** Table reference in FROM clause - can be a table name or derived table */
export interface TableRef {
  table: string;           // Table name (empty if derived table)
  alias?: string;          // Table alias (AS alias)
  derivedTable?: Query;    // For subqueries in FROM: (SELECT ...) AS alias
}

/** Common Table Expression (CTE) for WITH clause */
export interface CTE {
  name: string;            // CTE name
  columns?: string[];      // Optional column list: WITH cte(a, b) AS (...)
  query: Query;            // The CTE query
}

export interface SelectQuery {
  type: 'SELECT';
  columns: SelectColumn[];
  from: string;                    // Legacy: table name
  fromRef?: TableRef;              // NEW: Full table reference with alias/derived
  joins?: JoinInfo[];              // NEW: Multiple joins (replaces single join?)
  join?: JoinInfo;                 // Legacy: single join (for backwards compat)
  where?: WhereCondition;
  groupBy?: string[];
  having?: WhereCondition;
  orderBy?: OrderByItem[];
  limit?: number;
  offset?: number;
  distinct?: boolean;              // SELECT DISTINCT
  qualify?: WhereCondition;        // NEW: QUALIFY clause (window function filtering)
}

/** WITH clause wrapper for CTEs */
export interface WithQuery {
  type: 'WITH';
  ctes: CTE[];
  query: Query;                    // The main query following the CTEs
}

export interface SetOperationQuery {
  type: 'UNION' | 'EXCEPT' | 'INTERSECT';
  left: SelectQuery;
  right: SelectQuery;
  all: boolean;  // For UNION ALL, EXCEPT ALL, INTERSECT ALL
}

// Keep UnionQuery as alias for backwards compatibility
export type UnionQuery = SetOperationQuery;

export type Query = SelectQuery | SetOperationQuery | WithQuery;

// ============ STATEMENT TYPES ============

export interface CreateViewStatement {
  type: 'CREATE_VIEW';
  viewName: string;
  query: Query;
}

export type SQLStatement = CreateTableStatement | CreateViewStatement;

export interface ParsedSQL {
  statements: SQLStatement[];
}


