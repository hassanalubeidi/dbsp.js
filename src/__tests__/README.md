# DBSP Test Suite

Organized test suite for the DBSP (Database Stream Processor) implementation.

## Structure

```
__tests__/
├── unit/           # Fast unit tests for core primitives
├── sql/            # SQL parser and compiler tests
├── joins/          # Join-related tests
├── performance/    # Performance and benchmark tests
└── integration/    # Integration and example tests
```

## Test Categories

### `unit/` - Core Primitives
Fast-running unit tests for foundational DBSP concepts.

| File | Description |
|------|-------------|
| `zset.test.ts` | ZSet operations (add, subtract, filter, map, join) |
| `stream.test.ts` | Stream operations and lifting |
| `circuit.test.ts` | Circuit building and execution |
| `operators.test.ts` | Operator implementations (integrate, differentiate) |

**Run:** `npm test -- __tests__/unit`

### `sql/` - SQL Compiler
Tests for SQL parsing and compilation into DBSP circuits.

| File | Description |
|------|-------------|
| `compiler.test.ts` | Full SQL compiler test suite (parser, SELECT, JOIN, GROUP BY, window functions, etc.) |

**Run:** `npm test -- __tests__/sql`

### `joins/` - Join Operations
Tests for various join strategies and optimizations.

| File | Description |
|------|-------------|
| `advanced.test.ts` | ASOF joins, semi-joins, anti-joins |
| `optimized.test.ts` | Optimized join state implementations |
| `optimization.test.ts` | Join optimization strategies |
| `deep-benchmark.test.ts` | Deep join performance analysis |

**Run:** `npm test -- __tests__/joins`

### `performance/` - Performance Tests
Benchmarks and performance regression tests. **Note:** These may be slow.

| File | Description |
|------|-------------|
| `regression.test.ts` | Performance regression tests (O(delta) verification) |
| `memory.test.ts` | Memory leak detection tests |
| `benchmarks.test.ts` | Full performance benchmarks |
| `sql-benchmarks.test.ts` | SQL-specific benchmarks |
| `window-benchmarks.test.ts` | Window function benchmarks |
| `window-stress.test.ts` | Window function stress tests |
| `window-scale.test.ts` | Window functions at 1M+ scale |

**Run:** `npm test -- __tests__/performance`  
**Run 1M benchmarks:** `FULL_BENCHMARK=true npm test -- __tests__/performance`

### `integration/` - Integration Tests
End-to-end tests and example verification.

| File | Description |
|------|-------------|
| `examples.test.ts` | Verifies example code works correctly |

**Run:** `npm test -- __tests__/integration`

## Running Tests

```bash
# Run all tests
npm test

# Run specific category
npm test -- __tests__/unit
npm test -- __tests__/sql
npm test -- __tests__/joins
npm test -- __tests__/performance
npm test -- __tests__/integration

# Run single file
npm test -- __tests__/unit/zset.test.ts

# Watch mode
npm test -- --watch

# With coverage
npm test -- --coverage
```

## Performance Test Configuration

Performance tests can run in two modes:

1. **Fast mode (default):** Smaller datasets for quick CI runs
2. **Full benchmark mode:** Full 1M+ datasets for thorough benchmarking

```bash
# Fast mode (default)
npm test -- __tests__/performance

# Full benchmark mode
FULL_BENCHMARK=true npm test -- __tests__/performance
```

## Test Philosophy

1. **Unit tests** should be fast and isolated
2. **SQL tests** verify correctness of SQL compilation
3. **Join tests** ensure join optimizations work correctly
4. **Performance tests** verify O(delta) complexity holds
5. **Integration tests** verify real-world usage patterns

