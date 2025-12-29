/**
 * SQL Parser
 * ===========
 * 
 * Parses SQL statements using node-sql-parser and converts them to our AST format.
 * The parsed AST is then used by the SQLCompiler to build DBSP circuits.
 * 
 * @module
 */

import pkg from 'node-sql-parser';
const { Parser } = pkg;

import type {
  ColumnDef,
  CreateTableStatement,
  CreateViewStatement,
  ParsedSQL,
  SQLStatement,
  Query,
  SelectQuery,
  SetOperationQuery,
  WithQuery,
  CTE,
  TableRef,
  SelectColumn,
  SimpleColumn,
  AggregateColumn,
  ExpressionColumn,
  CaseColumn,
  FunctionColumn,
  CastColumn,
  WindowColumn,
  ScalarSubqueryColumn,
  LiteralColumn,
  AggregateArg,
  WhereCondition,
  ComparisonCondition,
  JoinInfo,
  JoinCondition,
  OrderByItem,
} from './ast-types';

/**
 * SQLParser: Parses SQL statements using node-sql-parser
 * and converts to our simplified AST format
 */
export class SQLParser {
  private parser: typeof Parser.prototype;

  constructor() {
    this.parser = new Parser();
  }

  /**
   * Parse SQL string into our AST format
   */
  parse(sql: string): ParsedSQL {
    const statements: SQLStatement[] = [];
    
    // Split by semicolons and parse each statement
    const sqlStatements = sql
      .split(';')
      .map(s => s.trim())
      .filter(s => s.length > 0);
    
    for (const stmt of sqlStatements) {
      const parsed = this.parseStatement(stmt);
      if (parsed) {
        statements.push(parsed);
      }
    }
    
    return { statements };
  }

  private parseStatement(sql: string): SQLStatement | null {
    try {
      // Use MySQL dialect for broader compatibility
      const ast = this.parser.astify(sql, { database: 'MySQL' });
      
      // Handle array of statements
      const stmt = Array.isArray(ast) ? ast[0] : ast;
      
      if (!stmt) return null;
      
      if (stmt.type === 'create') {
        const keyword = stmt.keyword as string;
        if (keyword === 'table') {
          return this.parseCreateTable(stmt);
        } else if (keyword === 'view') {
          return this.parseCreateView(stmt);
        }
      }
      
      return null;
    } catch (e) {
      // Check if this is a CREATE VIEW with UNION/EXCEPT/INTERSECT
      // node-sql-parser doesn't support this directly, so we need to parse manually
      const createViewMatch = sql.match(/^\s*CREATE\s+VIEW\s+(\w+)\s+AS\s+(.+)$/is);
      if (createViewMatch) {
        const viewName = createViewMatch[1];
        const selectSql = createViewMatch[2].trim();
        
        try {
          // Try to parse the SELECT part separately
          const selectAst = this.parser.astify(selectSql, { database: 'MySQL' });
          const selectStmt = Array.isArray(selectAst) ? selectAst[0] : selectAst;
          
          if (selectStmt && selectStmt.type === 'select') {
            const query = this.parseQuery(selectStmt);
            return {
              type: 'CREATE_VIEW',
              viewName,
              query,
            };
          }
        } catch (innerError) {
          console.error('Failed to parse CREATE VIEW with set operation:', innerError);
        }
      }
      
      console.error('Parse error:', e);
      return null;
    }
  }

  private parseCreateTable(stmt: any): CreateTableStatement {
    const tableName = stmt.table?.[0]?.table || '';
    const columns: ColumnDef[] = [];
    
    if (stmt.create_definitions) {
      for (const def of stmt.create_definitions) {
        if (def.resource === 'column') {
          columns.push({
            name: def.column?.column || '',
            type: this.normalizeType(def.definition?.dataType || ''),
          });
        }
      }
    }
    
    return {
      type: 'CREATE_TABLE',
      tableName,
      columns,
    };
  }

  private parseCreateView(stmt: any): CreateViewStatement {
    const viewName = stmt.view?.view || '';
    const query = this.parseQuery(stmt.select);
    
    return {
      type: 'CREATE_VIEW',
      viewName,
      query,
    };
  }

  public parseQuery(select: any): Query {
    // Check for WITH clause (CTEs)
    if (select.with) {
      const ctes: CTE[] = [];
      for (const cte of select.with) {
        const cteName = cte.name?.value || cte.name || '';
        const cteColumns = cte.columns?.map((c: any) => c.column || c.value || c) as string[] | undefined;
        const cteQuery = this.parseQuery(cte.stmt?.ast || cte.stmt);
        ctes.push({
          name: cteName,
          columns: cteColumns,
          query: cteQuery,
        });
      }
      
      // Parse the main query after WITH
      const mainQuery = this.parseQueryWithoutWith(select);
      
      return {
        type: 'WITH',
        ctes,
        query: mainQuery,
      } as WithQuery;
    }
    
    return this.parseQueryWithoutWith(select);
  }
  
  private parseQueryWithoutWith(select: any): SelectQuery | SetOperationQuery {
    // Check for set operations (UNION, EXCEPT, INTERSECT)
    if (select._next && select.set_op) {
      const setOp = select.set_op.toLowerCase();
      let type: SetOperationQuery['type'];
      let all = false;
      
      if (setOp === 'union' || setOp === 'union all') {
        type = 'UNION';
        all = setOp === 'union all';
      } else if (setOp === 'except' || setOp === 'except all') {
        type = 'EXCEPT';
        all = setOp === 'except all';
      } else if (setOp === 'intersect' || setOp === 'intersect all') {
        type = 'INTERSECT';
        all = setOp === 'intersect all';
      } else {
        // Unknown set operation, treat as UNION
        type = 'UNION';
      }
      
      return {
        type,
        left: this.parseSelectQuery(select),
        right: this.parseSelectQuery(select._next),
        all,
      };
    }
    
    return this.parseSelectQuery(select);
  }

  private parseSelectQuery(select: any): SelectQuery {
    // Parse columns (including aggregates, expressions, etc.)
    const columns: SelectColumn[] = [];
    if (select.columns === '*') {
      columns.push('*');
    } else if (Array.isArray(select.columns)) {
      for (const col of select.columns) {
        const parsedCol = this.parseColumn(col);
        if (parsedCol) {
          columns.push(parsedCol);
        }
      }
    }
    
    // Parse FROM (first table) with alias and derived table support
    const fromClause = select.from?.[0];
    let from = '';
    let fromRef: TableRef | undefined;
    
    if (fromClause) {
      if (fromClause.expr?.ast) {
        // Derived table (subquery in FROM): (SELECT ...) AS alias
        fromRef = {
          table: '',
          alias: fromClause.as || undefined,
          derivedTable: this.parseQuery(fromClause.expr.ast),
        };
        from = fromClause.as || '_derived_';
      } else {
        // Regular table reference
        from = fromClause.table || '';
        fromRef = {
          table: fromClause.table || '',
          alias: fromClause.as || undefined,
        };
      }
    }
    
    // Parse JOINs (support multiple joins!)
    const joins: JoinInfo[] = [];
    let join: JoinInfo | undefined;
    
    if (select.from && select.from.length > 1) {
      for (let i = 1; i < select.from.length; i++) {
        const joinClause = select.from[i];
        if (joinClause.join || joinClause.table) {
          const joinInfo = this.parseJoinClause(joinClause);
          joins.push(joinInfo);
          
          // For backwards compatibility, set join to the first join
          if (i === 1) {
            join = joinInfo;
          }
        }
      }
    }
    
    // Parse WHERE
    let where: WhereCondition | undefined;
    if (select.where) {
      where = this.parseWhere(select.where);
    }
    
    // Parse GROUP BY
    let groupBy: string[] | undefined;
    if (select.groupby?.columns) {
      groupBy = select.groupby.columns.map((col: any) => col.column);
    }
    
    // Parse HAVING
    let having: WhereCondition | undefined;
    if (select.having) {
      having = this.parseWhere(select.having);
    }
    
    // Parse ORDER BY
    let orderBy: OrderByItem[] | undefined;
    if (select.orderby) {
      orderBy = select.orderby.map((item: any) => {
        let column = item.expr?.column || '';
        let ordinal: number | undefined;
        
        // Handle ordinal position (ORDER BY 1, 2, etc.)
        if (item.expr?.type === 'number') {
          ordinal = item.expr.value;
          column = ''; // Will be resolved from SELECT list
        }
        
        return {
          column,
          direction: item.type?.toUpperCase() === 'DESC' ? 'DESC' : 'ASC',
          ordinal,
        };
      });
    }
    
    // Parse LIMIT and OFFSET
    let limit: number | undefined;
    let offset: number | undefined;
    if (select.limit) {
      // node-sql-parser can store limit in different structures
      if (Array.isArray(select.limit.value)) {
        // LIMIT with OFFSET: value is [offset, limit] or just [limit]
        if (select.limit.value.length === 2) {
          offset = select.limit.value[0]?.value;
          limit = select.limit.value[1]?.value;
        } else {
          limit = select.limit.value[0]?.value;
        }
      } else {
        limit = select.limit.value;
      }
    }
    
    // Parse DISTINCT
    // node-sql-parser sets select.distinct to 'DISTINCT' when DISTINCT is used
    const distinct = select.distinct === 'DISTINCT' || select.distinct === true;
    
    // Parse QUALIFY (window function filtering) - if present
    // node-sql-parser may store this differently, check for it
    let qualify: WhereCondition | undefined;
    if (select.qualify) {
      qualify = this.parseWhere(select.qualify);
    }
    
    return {
      type: 'SELECT',
      columns,
      from,
      fromRef,
      joins: joins.length > 0 ? joins : undefined,
      join,
      where,
      groupBy,
      having,
      orderBy,
      limit,
      offset,
      distinct,
      qualify,
    };
  }
  
  /**
   * Parse a JOIN clause into JoinInfo
   * Supports multiple join conditions (composite keys) and non-equi joins
   */
  private parseJoinClause(joinClause: any): JoinInfo {
    const joinStr = (joinClause.join || '').toUpperCase();
    const joinType: JoinInfo['type'] = 
      joinStr.includes('CROSS') ? 'CROSS' :
      joinStr.includes('LEFT') ? 'LEFT' :
      joinStr.includes('RIGHT') ? 'RIGHT' :
      joinStr.includes('FULL') ? 'FULL' : 'INNER';
    
    // Handle derived table in JOIN
    let table = '';
    let tableAlias: string | undefined;
    let derivedTable: Query | undefined;
    
    if (joinClause.expr?.ast) {
      // JOIN (SELECT ...) AS alias ON ...
      derivedTable = this.parseQuery(joinClause.expr.ast);
      tableAlias = joinClause.as || undefined;
      table = '';
    } else {
      table = joinClause.table || '';
      tableAlias = joinClause.as || undefined;
    }
    
    // Parse join conditions
    const conditions = this.parseJoinConditions(joinClause.on);
    
    // For backwards compatibility, extract first condition's columns
    const firstCond = conditions[0] || { leftColumn: '', rightColumn: '', operator: '=' as const };
    
    // Preserve conditions array if:
    // 1. Multiple conditions (composite key), OR
    // 2. Single condition with non-equi operator
    const hasNonEquiOperator = conditions.some(c => c.operator !== '=');
    const shouldPreserveConditions = conditions.length > 1 || hasNonEquiOperator;
    
    return {
      type: joinType,
      table,
      tableAlias,
      derivedTable,
      leftColumn: firstCond.leftColumn,
      leftTable: firstCond.leftTable,
      rightColumn: firstCond.rightColumn,
      rightTable: firstCond.rightTable,
      conditions: shouldPreserveConditions ? conditions : undefined,
    };
  }
  
  /**
   * Parse JOIN ON conditions (supports AND for composite keys and non-equi operators)
   */
  private parseJoinConditions(onClause: any): JoinCondition[] {
    const conditions: JoinCondition[] = [];
    
    if (!onClause) {
      return conditions;
    }
    
    // Check for AND (composite key)
    if (onClause.type === 'binary_expr' && onClause.operator === 'AND') {
      conditions.push(...this.parseJoinConditions(onClause.left));
      conditions.push(...this.parseJoinConditions(onClause.right));
      return conditions;
    }
    
    // Single condition
    if (onClause.type === 'binary_expr') {
      const operator = onClause.operator as JoinCondition['operator'];
      
      if (operator === 'BETWEEN') {
        // a.col BETWEEN b.low AND b.high
        const betweenValues = onClause.right?.value || [];
        conditions.push({
          leftColumn: onClause.left?.column || '',
          leftTable: onClause.left?.table || undefined,
          rightColumn: betweenValues[0]?.column || '',
          rightTable: betweenValues[0]?.table || undefined,
          operator: 'BETWEEN',
          betweenHigh: betweenValues[1]?.column || '',
          betweenHighTable: betweenValues[1]?.table || undefined,
        });
      } else {
        // Check if right side is a literal (string, number) or a column
        const rightType = onClause.right?.type;
        const isRightLiteral = rightType === 'string' || rightType === 'single_quote_string' || 
                               rightType === 'number' || rightType === 'bool';
        
        conditions.push({
          leftColumn: onClause.left?.column || '',
          leftTable: onClause.left?.table || undefined,
          rightColumn: isRightLiteral ? '' : (onClause.right?.column || ''),
          rightTable: isRightLiteral ? undefined : (onClause.right?.table || undefined),
          rightLiteral: isRightLiteral ? onClause.right?.value : undefined,
          operator: ['=', '!=', '<', '>', '<=', '>='].includes(operator) ? operator : '=',
        });
      }
    } else if (onClause.left && onClause.right) {
      // Simple equi-join (older format) - check for literals
      const rightType = onClause.right?.type;
      const isRightLiteral = rightType === 'string' || rightType === 'single_quote_string' || 
                             rightType === 'number' || rightType === 'bool';
      
      conditions.push({
        leftColumn: onClause.left?.column || '',
        leftTable: onClause.left?.table || undefined,
        rightColumn: isRightLiteral ? '' : (onClause.right?.column || ''),
        rightTable: isRightLiteral ? undefined : (onClause.right?.table || undefined),
        rightLiteral: isRightLiteral ? onClause.right?.value : undefined,
        operator: '=',
      });
    }
    
    return conditions;
  }

  private parseColumn(col: any): SelectColumn | null {
    // Check for scalar subquery: (SELECT ... FROM ...)
    if (col.expr?.type === 'select' || col.expr?.ast) {
      const subqueryAst = col.expr.ast || col.expr;
      return {
        type: 'scalar_subquery',
        query: this.parseQuery(subqueryAst),
        alias: col.as || undefined,
      } as ScalarSubqueryColumn;
    }
    
    if (col.expr?.type === 'column_ref') {
      if (col.expr.column === '*') {
        return '*';
      }
      return {
        type: 'column',
        name: col.expr.column,
        alias: col.as || undefined,
        table: col.expr.table || undefined,
      } as SimpleColumn;
    } else if (col.expr?.type === 'star') {
      return '*';
    } else if (col.expr?.type === 'aggr_func' && col.expr?.over) {
      // Aggregate with OVER clause (rolling aggregate): SUM(...) OVER (...) - CHECK BEFORE plain aggr_func!
      return this.parseWindowFunction(col.expr, col.as);
    } else if (col.expr?.type === 'aggr_func') {
      // Aggregate function (COUNT, SUM, AVG, MIN, MAX) without OVER
      const funcName = col.expr.name.toUpperCase();
      const args: string[] = [];
      let argExpr: AggregateArg | undefined;
      
      // Check for DISTINCT modifier: COUNT(DISTINCT col)
      // node-sql-parser puts it in args.distinct
      const isDistinct = col.expr.args?.distinct === 'DISTINCT' || col.expr.args?.distinct === true;
      
      if (col.expr.args?.expr?.type === 'star') {
        args.push('*');
        argExpr = { type: 'star' };
      } else if (col.expr.args?.expr?.type === 'column_ref') {
        const colName = col.expr.args.expr.column;
        args.push(colName);
        argExpr = { type: 'column', column: colName };
      } else if (col.expr.args?.expr?.type === 'binary_expr') {
        // Complex expression inside aggregate: SUM(amount * quantity)
        argExpr = this.parseAggregateExpr(col.expr.args.expr);
        // For backwards compatibility, create a placeholder arg name
        args.push('_expr_');
      } else if (col.expr.args?.expr?.type === 'case') {
        // CASE WHEN inside aggregate: SUM(CASE WHEN status='FILLED' THEN 1 ELSE 0 END)
        argExpr = this.parseAggregateExpr(col.expr.args.expr);
        args.push('_case_');
      } else if (col.expr.args?.expr?.type === 'function') {
        // Function inside aggregate: SUM(ABS(col)), COUNT(LENGTH(str))
        argExpr = this.parseAggregateExpr(col.expr.args.expr);
        args.push('_func_');
      }
      
      return {
        type: 'aggregate',
        function: funcName as AggregateColumn['function'],
        args,
        argExpr,
        alias: col.as || undefined,
        distinct: isDistinct || undefined,
      } as AggregateColumn;
    } else if (col.expr?.type === 'binary_expr') {
      // Arithmetic expression
      return {
        type: 'expression',
        operator: col.expr.operator,
        left: col.expr.left,
        right: col.expr.right,
        alias: col.as || undefined,
      } as ExpressionColumn;
    } else if (col.expr?.type === 'case') {
      // CASE WHEN expression
      const conditions = col.expr.args?.map((arg: any) => ({
        when: arg.cond,
        then: arg.result,
      })) || [];
      return {
        type: 'case',
        conditions,
        else: col.expr.else,
        alias: col.as || undefined,
      } as CaseColumn;
    } else if (col.expr?.type === 'function' && col.expr?.over) {
      // Window function with OVER clause: LAG, LEAD, ROW_NUMBER, etc. - CHECK BEFORE regular function!
      return this.parseWindowFunction(col.expr, col.as);
    } else if (col.expr?.type === 'function') {
      // Regular function (COALESCE, NULLIF, etc.) - NO OVER clause
      const args: string[] = [];
      const argExprs: AggregateArg[] = [];
      if (col.expr.args?.value) {
        for (const arg of col.expr.args.value) {
          if (arg.type === 'column_ref') {
            args.push(arg.column);
          }
          // Parse full expression for generic function support
          argExprs.push(this.parseAggregateExpr(arg));
        }
      }
      return {
        type: 'function',
        function: col.expr.name?.name?.[0]?.value?.toUpperCase() || col.expr.name?.toUpperCase() || '',
        args,
        argExprs,
        alias: col.as || undefined,
      } as FunctionColumn;
    } else if (col.expr?.type === 'cast') {
      return {
        type: 'cast',
        expr: col.expr.expr,
        targetType: col.expr.target?.dataType || '',
        alias: col.as || undefined,
      } as CastColumn;
    } else if (col.expr?.type === 'number') {
      // Literal number: 0 AS avgSlippage, 123 AS fixed_value
      return {
        type: 'literal',
        value: col.expr.value,
        alias: col.as || undefined,
      } as LiteralColumn;
    } else if (col.expr?.type === 'string' || col.expr?.type === 'single_quote_string') {
      // Literal string: 'UNKNOWN' AS default_status
      return {
        type: 'literal',
        value: col.expr.value,
        alias: col.as || undefined,
      } as LiteralColumn;
    } else if (col.expr?.type === 'unary_expr') {
      // Unary expression: -a, +a
      const operator = col.expr.operator;
      const operand = col.expr.expr;
      
      // For unary minus/plus, create an expression column
      // -a is equivalent to 0 - a, +a is equivalent to 0 + a
      if (operator === '-' || operator === '+') {
        return {
          type: 'expression',
          operator: operator,
          left: { type: 'number', value: 0 },  // Left operand is 0 for unary
          right: operand,
          alias: col.as || undefined,
          unary: true,  // Mark as unary expression
        } as ExpressionColumn;
      }
    }
    
    return null;
  }
  
  /**
   * Parse a window function (LAG, LEAD, SUM OVER, etc.)
   */
  private parseWindowFunction(expr: any, alias?: string): WindowColumn {
    // Get function name - handle both string and object formats
    let funcName: string = '';
    if (typeof expr.name === 'string') {
      funcName = expr.name.toUpperCase();
    } else if (expr.name?.name?.[0]?.value) {
      funcName = expr.name.name[0].value.toUpperCase();
    }
    const func = funcName as WindowColumn['function'];
    
    // Parse function arguments
    const args: AggregateArg[] = [];
    if (expr.args?.value) {
      for (const arg of expr.args.value) {
        args.push(this.parseAggregateExpr(arg));
      }
    } else if (expr.args?.expr) {
      // aggr_func style: args is { expr: ... }
      args.push(this.parseAggregateExpr(expr.args.expr));
    }
    
    // Parse OVER clause - may be nested under as_window_specification
    const over = expr.over?.as_window_specification?.window_specification || expr.over;
    
    // Parse PARTITION BY
    let partitionBy: string[] | undefined;
    if (over?.partitionby) {
      partitionBy = over.partitionby
        .filter((p: any) => p.expr?.type === 'column_ref' || p.type === 'column_ref')
        .map((p: any) => p.expr?.column || p.column);
    }
    
    // Parse ORDER BY
    let orderBy: WindowColumn['orderBy'];
    if (over?.orderby) {
      orderBy = over.orderby
        .filter((o: any) => o.expr?.type === 'column_ref')
        .map((o: any) => ({
          column: o.expr.column,
          direction: (o.type?.toUpperCase() || 'ASC') as 'ASC' | 'DESC',
        }));
    }
    
    // Parse window frame (ROWS/RANGE BETWEEN)
    let frame: WindowColumn['frame'];
    if (over?.window_frame_clause) {
      const wf = over.window_frame_clause;
      
      const parseFrameBound = (bound: any): { type: 'UNBOUNDED' | 'CURRENT' | 'PRECEDING' | 'FOLLOWING'; offset?: number } => {
        if (!bound) return { type: 'CURRENT' };
        
        // Handle string format: "2 PRECEDING", "current row"
        if (typeof bound === 'string' || bound.value) {
          const str = (typeof bound === 'string' ? bound : bound.value).toLowerCase();
          if (str.includes('current')) {
            return { type: 'CURRENT' };
          }
          if (str.includes('unbounded')) {
            return { type: 'UNBOUNDED' };
          }
          const match = str.match(/(\d+)\s*(preceding|following)/i);
          if (match) {
            return { type: match[2].toUpperCase() as 'PRECEDING' | 'FOLLOWING', offset: parseInt(match[1], 10) };
          }
        }
        
        // Handle object format
        if (bound.type === 'origin' && bound.value) {
          const val = bound.value.toLowerCase();
          if (val.includes('current')) return { type: 'CURRENT' };
        }
        if (bound.type === 'number' && typeof bound.value === 'string') {
          const match = bound.value.match(/(\d+)\s*(preceding|following)/i);
          if (match) {
            return { type: match[2].toUpperCase() as 'PRECEDING' | 'FOLLOWING', offset: parseInt(match[1], 10) };
          }
        }
        if (bound.type === 'preceding' || bound.preceding !== undefined) {
          const offset = bound.value ?? bound.preceding ?? 1;
          return { type: 'PRECEDING', offset: typeof offset === 'number' ? offset : parseInt(offset, 10) };
        }
        if (bound.type === 'following' || bound.following !== undefined) {
          const offset = bound.value ?? bound.following ?? 1;
          return { type: 'FOLLOWING', offset: typeof offset === 'number' ? offset : parseInt(offset, 10) };
        }
        if (bound.type === 'current row' || bound.type === 'current' || bound.current_row) {
          return { type: 'CURRENT' };
        }
        if (bound.type === 'unbounded preceding' || bound.type === 'unbounded' || bound.unbounded) {
          return { type: 'UNBOUNDED' };
        }
        
        return { type: 'CURRENT' };
      };
      
      // Handle binary_expr format: { type: 'binary_expr', operator: 'BETWEEN', left: {...}, right: {...} }
      if (wf.type === 'binary_expr' && wf.operator === 'BETWEEN') {
        const frameType = (wf.left?.value || 'rows').toUpperCase() as 'ROWS' | 'RANGE';
        const bounds = wf.right?.value || [];
        frame = {
          type: frameType,
          start: parseFrameBound(bounds[0]),
          end: parseFrameBound(bounds[1]),
        };
      } else {
        frame = {
          type: (wf.type?.toUpperCase() || 'ROWS') as 'ROWS' | 'RANGE',
          start: parseFrameBound(wf.start || wf.expr?.start),
          end: parseFrameBound(wf.end || wf.expr?.end),
        };
      }
    }
    
    return {
      type: 'window',
      function: func,
      args,
      partitionBy,
      orderBy,
      frame,
      alias,
    };
  }

  /**
   * Parse an expression inside an aggregate function
   * Handles: column_ref, number, binary_expr (arithmetic), function calls, scalar subqueries
   */
  parseAggregateExpr(expr: any): AggregateArg {
    // Handle scalar subquery in expression: col / (SELECT SUM(x) FROM t)
    if (expr.type === 'select' || expr.ast) {
      const subqueryAst = expr.ast || expr;
      return {
        type: 'scalar_subquery',
        subquery: this.parseQuery(subqueryAst),
      };
    }
    
    if (expr.type === 'column_ref') {
      return { type: 'column', column: expr.column, table: expr.table || undefined };
    } else if (expr.type === 'number') {
      return { type: 'expression', value: expr.value };
    } else if (expr.type === 'single_quote_string' || expr.type === 'string') {
      // String literal: 'FILLED', 'PENDING', etc.
      return { type: 'expression', stringValue: expr.value };
    } else if (expr.type === 'binary_expr') {
      return {
        type: 'expression',
        operator: expr.operator,
        left: this.parseAggregateExpr(expr.left),
        right: this.parseAggregateExpr(expr.right),
      };
    } else if (expr.type === 'star') {
      return { type: 'star' };
    } else if (expr.type === 'function') {
      // Function call: ABS(col), IF(cond, a, b), etc.
      const funcName = expr.name?.name?.[0]?.value || expr.name || '';
      const funcArgs: AggregateArg[] = [];
      
      // Parse function arguments
      if (expr.args?.value) {
        for (const arg of expr.args.value) {
          funcArgs.push(this.parseAggregateExpr(arg));
        }
      }
      
      return {
        type: 'function',
        functionName: funcName,
        args: funcArgs,
      };
    } else if (expr.type === 'case') {
      // CASE WHEN condition THEN value ELSE value END
      // Convert to IF() for simplicity
      // node-sql-parser structure: { type: 'case', args: [{ cond, result, type: 'when' }], else: elseExpr }
      const whenClause = expr.args?.[0];
      if (whenClause) {
        const condition = this.parseConditionToExpr(whenClause.cond);
        const thenVal = this.parseAggregateExpr(whenClause.result);
        // The else clause is in expr.else, not expr.args[1]
        const elseVal = expr.else 
          ? this.parseAggregateExpr(expr.else)
          : { type: 'expression' as const, value: 0 };
        
        return {
          type: 'function',
          functionName: 'IF',
          args: [condition, thenVal, elseVal],
        };
      }
      return { type: 'expression', value: 0 };
    } else if (expr.type === 'interval') {
      // INTERVAL '1 hour', INTERVAL 30 MINUTE, etc.
      // Convert to string for use with TUMBLE_START/TUMBLE_END
      const value = expr.expr?.value || 1;
      const unit = expr.unit || 'HOUR';
      return {
        type: 'expression',
        stringValue: `${value} ${unit}`,
      };
    }
    // Default: treat as column
    return { type: 'column', column: expr.column || expr.value || '' };
  }
  
  /**
   * Parse a condition into a numeric expression (1 for true, 0 for false)
   * Used for CASE WHEN to IF() conversion
   */
  private parseConditionToExpr(cond: any): AggregateArg {
    if (cond.type === 'binary_expr') {
      // Comparison: col > 100, status = 'FILLED', etc.
      const op = cond.operator;
      if (['=', '=='].includes(op)) {
        return {
          type: 'function',
          functionName: 'EQ',
          args: [
            this.parseAggregateExpr(cond.left),
            this.parseAggregateExpr(cond.right),
          ],
        };
      }
      if (['!=', '<>'].includes(op)) {
        return {
          type: 'function',
          functionName: 'NE',
          args: [
            this.parseAggregateExpr(cond.left),
            this.parseAggregateExpr(cond.right),
          ],
        };
      }
      if (op === '>') {
        return {
          type: 'function',
          functionName: 'GT',
          args: [
            this.parseAggregateExpr(cond.left),
            this.parseAggregateExpr(cond.right),
          ],
        };
      }
      if (op === '<') {
        return {
          type: 'function',
          functionName: 'LT',
          args: [
            this.parseAggregateExpr(cond.left),
            this.parseAggregateExpr(cond.right),
          ],
        };
      }
      if (op === '>=') {
        return {
          type: 'function',
          functionName: 'GTE',
          args: [
            this.parseAggregateExpr(cond.left),
            this.parseAggregateExpr(cond.right),
          ],
        };
      }
      if (op === '<=') {
        return {
          type: 'function',
          functionName: 'LTE',
          args: [
            this.parseAggregateExpr(cond.left),
            this.parseAggregateExpr(cond.right),
          ],
        };
      }
      // AND/OR
      if (cond.operator === 'AND') {
        return {
          type: 'expression',
          operator: '*',  // AND as multiplication
          left: this.parseConditionToExpr(cond.left),
          right: this.parseConditionToExpr(cond.right),
        };
      }
      if (cond.operator === 'OR') {
        // OR: max(a, b) or a + b - a*b
        return {
          type: 'function',
          functionName: 'OR',
          args: [
            this.parseConditionToExpr(cond.left),
            this.parseConditionToExpr(cond.right),
          ],
        };
      }
      // BETWEEN: col BETWEEN low AND high
      if (cond.operator === 'BETWEEN') {
        // node-sql-parser: { left: col, right: { type: 'expr_list', value: [low, high] } }
        return {
          type: 'between',
          left: this.parseAggregateExpr(cond.left),
          low: this.parseAggregateExpr(cond.right?.value?.[0] || { type: 'number', value: 0 }),
          high: this.parseAggregateExpr(cond.right?.value?.[1] || { type: 'number', value: 0 }),
        };
      }
    }
    // Default: return as-is (assume numeric)
    return this.parseAggregateExpr(cond);
  }

  private parseWhere(whereAst: any): WhereCondition {
    // Handle BETWEEN and NOT BETWEEN
    if (whereAst.type === 'binary_expr' && (whereAst.operator === 'BETWEEN' || whereAst.operator === 'NOT BETWEEN')) {
      return {
        type: 'BETWEEN',
        column: whereAst.left?.column || '',
        low: whereAst.right?.value?.[0]?.value || 0,
        high: whereAst.right?.value?.[1]?.value || 0,
        not: whereAst.operator === 'NOT BETWEEN',
      };
    }
    
    // Handle IN and NOT IN (with literal values or subquery)
    if (whereAst.type === 'binary_expr' && (whereAst.operator === 'IN' || whereAst.operator === 'NOT IN')) {
      const column = whereAst.left?.column || '';
      const isNot = whereAst.operator === 'NOT IN';
      
      // Check if this is a subquery: right.value[0].ast exists
      const rightValues = whereAst.right?.value || [];
      if (rightValues.length === 1 && rightValues[0]?.ast) {
        // This is an IN (SELECT ...) subquery
        const subAst = rightValues[0].ast;
        const subTable = subAst.from?.[0]?.table || '';
        const subColumn = subAst.columns?.[0]?.expr?.column || '';
        
        return {
          type: 'IN',
          column,
          values: [], // No literal values for subquery
          subquery: {
            table: subTable,
            column: subColumn,
          },
          not: isNot,
        };
      }
      
      // Literal values
      const values = rightValues.map((v: any) => v.value).filter((v: any) => v !== undefined);
      return {
        type: 'IN',
        column,
        values,
        not: isNot,
      };
    }
    
    // Handle IS NULL
    if (whereAst.type === 'binary_expr' && whereAst.operator === 'IS') {
      if (whereAst.right?.type === 'null') {
        return {
          type: 'IS_NULL',
          column: whereAst.left?.column || '',
        };
      }
    }
    
    // Handle IS NOT NULL (operator is 'IS NOT')
    if (whereAst.type === 'binary_expr' && whereAst.operator === 'IS NOT') {
      if (whereAst.right?.type === 'null') {
        return {
          type: 'IS_NOT_NULL',
          column: whereAst.left?.column || '',
        };
      }
    }
    
    // Handle NOT
    if (whereAst.type === 'unary_expr' && whereAst.operator === 'NOT') {
      return {
        type: 'NOT',
        condition: this.parseWhere(whereAst.expr),
      };
    }
    
    // Handle LIKE and NOT LIKE
    if (whereAst.type === 'binary_expr' && (whereAst.operator === 'LIKE' || whereAst.operator === 'NOT LIKE')) {
      return {
        type: 'LIKE',
        column: whereAst.left?.column || '',
        pattern: whereAst.right?.value || '',
        not: whereAst.operator === 'NOT LIKE',
      };
    }
    
    // Handle REGEXP, RLIKE, ~, ~*
    if (whereAst.type === 'binary_expr' && 
        (whereAst.operator === 'REGEXP' || whereAst.operator === 'RLIKE' || 
         whereAst.operator === '~' || whereAst.operator === '~*')) {
      return {
        type: 'REGEXP',
        column: whereAst.left?.column || '',
        pattern: whereAst.right?.value || '',
        caseInsensitive: whereAst.operator === '~*',
      };
    }
    
    // Handle NOT as a function (e.g., NOT (a = 1 AND b = 1))
    // node-sql-parser treats NOT with parenthesized expressions as a function call
    if (whereAst.type === 'function') {
      const funcName = typeof whereAst.name === 'string' 
        ? whereAst.name.toUpperCase() 
        : whereAst.name?.name?.[0]?.value?.toUpperCase();
      
      if (funcName === 'NOT') {
        // NOT function with an expression argument
        const innerExpr = whereAst.args?.value?.[0];
        if (innerExpr) {
          return {
            type: 'NOT',
            condition: this.parseWhere(innerExpr),
          };
        }
      }
      
      if (funcName === 'EXISTS') {
        const subAst = whereAst.args?.value?.[0]?.ast;
        if (subAst) {
          const subTable = subAst.from?.[0]?.table || '';
          
          // Parse the subquery WHERE to find the correlation condition
          // e.g., WHERE customers.id = orders.customer_id
          let leftColumn = '';
          let rightColumn = '';
          let leftTable = '';
          
          if (subAst.where?.type === 'binary_expr' && subAst.where?.operator === '=') {
            const left = subAst.where.left;
            const right = subAst.where.right;
            
            // Determine which side is from the subquery table and which from the outer
            if (left?.table === subTable || !left?.table) {
              rightColumn = left?.column || '';
              leftColumn = right?.column || '';
              leftTable = right?.table || '';
            } else {
              leftColumn = left?.column || '';
              leftTable = left?.table || '';
              rightColumn = right?.column || '';
            }
          }
          
          return {
            type: 'EXISTS',
            subquery: {
              table: subTable,
              leftColumn,
              rightColumn,
              leftTable,
            },
          };
        }
      }
    }
    
    if (whereAst.type === 'binary_expr') {
      if (whereAst.operator === 'AND') {
        return {
          type: 'AND',
          conditions: [
            this.parseWhere(whereAst.left),
            this.parseWhere(whereAst.right),
          ],
        };
      } else if (whereAst.operator === 'OR') {
        return {
          type: 'OR',
          conditions: [
            this.parseWhere(whereAst.left),
            this.parseWhere(whereAst.right),
          ],
        };
      } else {
        // Check if left side is an aggregate function (for HAVING)
        if (whereAst.left?.type === 'aggr_func') {
          const funcName = whereAst.left.name?.toUpperCase() || '';
          let argCol = '*';
          const argExpr = whereAst.left.args?.expr;
          
          if (argExpr?.type === 'column_ref') {
            argCol = argExpr.column;
          } else if (argExpr?.type === 'star') {
            argCol = '*';
          } else if (argExpr?.type === 'binary_expr') {
            // Handle expressions like (unrealizedPnL + realizedPnL)
            // Serialize to a canonical string format
            argCol = this.serializeExpressionForHaving(argExpr);
          } else if (argExpr?.type === 'function') {
            // Handle function calls like ABS(notional)
            const fnName = argExpr.name?.name?.[0]?.value || argExpr.name || 'fn';
            const fnArg = argExpr.args?.value?.[0]?.column || 'arg';
            argCol = `${fnName}(${fnArg})`;
          }
          
          const operator = whereAst.operator as ComparisonCondition['operator'];
          let value = 0;
          if (whereAst.right?.type === 'number') {
            value = whereAst.right.value;
          }
          return {
            type: 'AGGREGATE_COMPARISON',
            aggregateFunc: funcName,
            aggregateArg: argCol,
            operator,
            value,
          };
        }
        
        // Regular comparison operator
        const operator = whereAst.operator as ComparisonCondition['operator'];
        let value: string | number | boolean = '';
        let column = '';
        let leftLiteral: string | number | boolean | undefined = undefined;
        
        // Handle left side - could be a column reference, literal, or expression
        if (whereAst.left?.type === 'binary_expr') {
          // Left side is an expression (e.g., price * qty > 50)
          const leftExpr = this.parseAggregateExpr(whereAst.left);
          
          // Get right side value
          let rightValue: number | string | boolean = 0;
          let rightExpr: AggregateArg | undefined = undefined;
          
          if (whereAst.right?.type === 'number') {
            rightValue = whereAst.right.value;
          } else if (whereAst.right?.type === 'string' || whereAst.right?.type === 'single_quote_string') {
            rightValue = whereAst.right.value;
          } else if (whereAst.right?.type === 'binary_expr') {
            rightExpr = this.parseAggregateExpr(whereAst.right);
          }
          
          return {
            type: 'EXPRESSION_COMPARISON' as const,
            leftExpr,
            operator,
            value: rightValue,
            rightExpr,
          };
        } else if (whereAst.left?.type === 'column_ref') {
          column = whereAst.left.column || '';
        } else if (whereAst.left?.type === 'string' || whereAst.left?.type === 'single_quote_string') {
          // Left side is a string literal (e.g., 'ALL' = 'ALL')
          leftLiteral = whereAst.left.value;
        } else if (whereAst.left?.type === 'number') {
          leftLiteral = whereAst.left.value;
        } else if (whereAst.left?.type === 'double_quote_string') {
          leftLiteral = whereAst.left.value;
        } else {
          // Fallback for column reference
          column = whereAst.left?.column || '';
        }
        
        // Handle right side
        let rightColumn: string | undefined = undefined;
        if (whereAst.right?.type === 'string' || whereAst.right?.type === 'single_quote_string') {
          value = whereAst.right.value;
        } else if (whereAst.right?.type === 'number') {
          value = whereAst.right.value;
        } else if (whereAst.right?.type === 'bool') {
          value = whereAst.right.value;
        } else if (whereAst.right?.type === 'double_quote_string') {
          value = whereAst.right.value;
        } else if (whereAst.right?.type === 'column_ref') {
          // Right side is a column reference (column-to-column comparison)
          rightColumn = whereAst.right.column || '';
        }
        
        return {
          type: 'COMPARISON',
          column,
          operator,
          value,
          leftLiteral,
          rightColumn,
        };
      }
    }
    
    // Default fallback
    return {
      type: 'COMPARISON',
      column: '',
      operator: '=',
      value: '',
    };
  }

  /**
   * Serialize a binary expression to a canonical string format for HAVING matching.
   * This must match how the compiler stores aggregate keys.
   */
  private serializeExpressionForHaving(expr: any): string {
    if (!expr) return '';
    
    if (expr.type === 'column_ref') {
      return expr.column || '';
    }
    
    if (expr.type === 'number') {
      return String(expr.value);
    }
    
    if (expr.type === 'binary_expr') {
      const left = this.serializeExpressionForHaving(expr.left);
      const right = this.serializeExpressionForHaving(expr.right);
      return `(${left} ${expr.operator} ${right})`;
    }
    
    if (expr.type === 'function') {
      const fnName = expr.name?.name?.[0]?.value || expr.name || 'fn';
      const fnArg = expr.args?.value?.[0]?.column || 'arg';
      return `${fnName}(${fnArg})`;
    }
    
    return '';
  }

  private normalizeType(type: string): string {
    const upper = type.toUpperCase();
    if (upper.startsWith('VARCHAR')) return 'VARCHAR';
    if (upper.startsWith('INT')) return 'INT';
    if (upper.startsWith('DECIMAL')) return 'DECIMAL';
    if (upper.startsWith('TIMESTAMP')) return 'TIMESTAMP';
    return upper;
  }
}


