/**
 * Signal-Aligned Trades Tests
 * ============================
 * 
 * Tests for SELECT with table prefixes and AS aliases (e.g., SELECT table.*, other.col AS alias).
 * These tests ensure column aliasing works correctly after JOINs.
 */

import { describe, it, expect } from 'vitest';
import { SQLCompiler } from '../../sql/compiler';
import { ZSet } from '../../internals/zset';

describe('Signal-Aligned Trades', () => {
  
  it('should correctly apply AS aliases with SELECT table.*', () => {
    // This tests the core issue: SELECT rfqs.*, signals.direction AS signal_direction
    // The alias should be applied even when there's a wildcard in the column list
    const rfqs = [
      { rfqId: 1, issuer: 'AAPL', side: 'BID', notional: 1000000, status: 'FILLED' },
      { rfqId: 2, issuer: 'MSFT', side: 'OFFER', notional: 2000000, status: 'FILLED' },
    ];
    
    const signals = [
      { signalId: 1, issuer: 'AAPL', direction: 'LONG', confidence: 0.8 },
      { signalId: 2, issuer: 'MSFT', direction: 'SHORT', confidence: 0.9 },
    ];
    
    const sql = `
      CREATE TABLE rfqs (rfqId INT, issuer TEXT, side TEXT, notional INT, status TEXT);
      CREATE TABLE signals (signalId INT, issuer TEXT, direction TEXT, confidence DECIMAL);
      CREATE VIEW rfqSignals AS
        SELECT rfqs.*, signals.direction AS signal_direction, signals.confidence AS signal_confidence
        FROM rfqs
        JOIN signals ON rfqs.issuer = signals.issuer;
    `;
    
    const compiler = new SQLCompiler();
    const { circuit, views } = compiler.compile(sql);
    
    const results: any[][] = [];
    views.rfqSignals.integrate().output((zset) => {
      results.push((zset as ZSet<any>).values());
    });
    
    circuit.step(new Map([
      ['rfqs', ZSet.fromValues(rfqs)],
      ['signals', ZSet.fromValues(signals)],
    ]));
    
    const joined = results[0] || [];
    expect(joined.length).toBe(2);
    
    // Check that aliases are correctly applied
    const aapl = joined.find((r: any) => r.issuer === 'AAPL');
    expect(aapl).toBeDefined();
    expect(aapl.signal_direction).toBe('LONG');
    expect(aapl.signal_confidence).toBe(0.8);
    
    const msft = joined.find((r: any) => r.issuer === 'MSFT');
    expect(msft).toBeDefined();
    expect(msft.signal_direction).toBe('SHORT');
    expect(msft.signal_confidence).toBe(0.9);
  });
  
  it('should filter using aliased columns after join', () => {
    const rfqs = [
      { rfqId: 1, issuer: 'AAPL', side: 'BID', notional: 1000000 },
      { rfqId: 2, issuer: 'MSFT', side: 'OFFER', notional: 2000000 },
    ];
    
    const signals = [
      { signalId: 1, issuer: 'AAPL', direction: 'LONG', confidence: 0.8 },
      { signalId: 2, issuer: 'MSFT', direction: 'SHORT', confidence: 0.9 },
    ];
    
    // Step 1: Create joined data with aliases
    const joinSQL = `
      CREATE TABLE rfqs (rfqId INT, issuer TEXT, side TEXT, notional INT);
      CREATE TABLE signals (signalId INT, issuer TEXT, direction TEXT, confidence DECIMAL);
      CREATE VIEW rfqSignals AS
        SELECT rfqs.*, signals.direction AS signal_direction, signals.confidence AS signal_confidence
        FROM rfqs
        JOIN signals ON rfqs.issuer = signals.issuer;
    `;
    
    const compiler1 = new SQLCompiler();
    const result1 = compiler1.compile(joinSQL);
    
    const joinResults: any[] = [];
    result1.views.rfqSignals.integrate().output((zset) => {
      joinResults.push(...(zset as ZSet<any>).values());
    });
    
    result1.circuit.step(new Map([
      ['rfqs', ZSet.fromValues(rfqs)],
      ['signals', ZSet.fromValues(signals)],
    ]));
    
    expect(joinResults.length).toBe(2);
    expect(joinResults[0].signal_direction).toBeDefined();
    
    // Step 2: Filter using the aliased columns
    const filterSQL = `
      CREATE TABLE rfqSignals (rfqId INT, issuer TEXT, side TEXT, notional INT, signal_direction TEXT, signal_confidence DECIMAL);
      CREATE VIEW aligned AS
        SELECT * FROM rfqSignals
        WHERE signal_confidence > 0.6
          AND ((side = 'BID' AND signal_direction = 'LONG')
               OR (side = 'OFFER' AND signal_direction = 'SHORT'));
    `;
    
    const compiler2 = new SQLCompiler();
    const result2 = compiler2.compile(filterSQL);
    
    const filterResults: any[] = [];
    result2.views.aligned.integrate().output((zset) => {
      filterResults.push(...(zset as ZSet<any>).values());
    });
    
    result2.circuit.step(new Map([
      ['rfqSignals', ZSet.fromValues(joinResults)],
    ]));
    
    // Both should match:
    // - AAPL: side=BID, signal_direction=LONG, confidence=0.8 > 0.6 ✓
    // - MSFT: side=OFFER, signal_direction=SHORT, confidence=0.9 > 0.6 ✓
    expect(filterResults.length).toBe(2);
    expect(filterResults.some((r: any) => r.issuer === 'AAPL')).toBe(true);
    expect(filterResults.some((r: any) => r.issuer === 'MSFT')).toBe(true);
  });
  
  it('should handle OR conditions correctly', () => {
    const data = [
      { id: 1, side: 'BID', direction: 'LONG' },
      { id: 2, side: 'OFFER', direction: 'SHORT' },
      { id: 3, side: 'BID', direction: 'SHORT' },
      { id: 4, side: 'OFFER', direction: 'LONG' },
    ];
    
    const sql = `
      CREATE TABLE t (id INT, side TEXT, direction TEXT);
      CREATE VIEW matched AS
        SELECT * FROM t
        WHERE (side = 'BID' AND direction = 'LONG')
           OR (side = 'OFFER' AND direction = 'SHORT');
    `;
    
    const compiler = new SQLCompiler();
    const { circuit, views } = compiler.compile(sql);
    
    const results: any[][] = [];
    views.matched.integrate().output((zset) => {
      results.push((zset as ZSet<any>).values());
    });
    
    circuit.step(new Map([
      ['t', ZSet.fromValues(data)],
    ]));
    
    const matched = results[0] || [];
    
    // Should match id 1 (BID+LONG) and id 2 (OFFER+SHORT)
    expect(matched.length).toBe(2);
    expect(matched.some((r: any) => r.id === 1)).toBe(true);
    expect(matched.some((r: any) => r.id === 2)).toBe(true);
  });
  
  it('should work with combined join + filter in single query', () => {
    const rfqs = [
      { rfqId: 1, issuer: 'AAPL', side: 'BID', notional: 1000000, status: 'FILLED' },
      { rfqId: 2, issuer: 'MSFT', side: 'OFFER', notional: 2000000, status: 'FILLED' },
      { rfqId: 3, issuer: 'JPM', side: 'BID', notional: 3000000, status: 'FILLED' },
      { rfqId: 4, issuer: 'GS', side: 'OFFER', notional: 4000000, status: 'FILLED' },
    ];
    
    const signals = [
      { signalId: 1, issuer: 'AAPL', direction: 'LONG', confidence: 0.8 },   // MATCH: BID + LONG
      { signalId: 2, issuer: 'MSFT', direction: 'SHORT', confidence: 0.9 },  // MATCH: OFFER + SHORT
      { signalId: 3, issuer: 'JPM', direction: 'SHORT', confidence: 0.7 },   // NO: BID + SHORT mismatch
      { signalId: 4, issuer: 'GS', direction: 'LONG', confidence: 0.5 },     // NO: confidence too low
    ];
    
    const sql = `
      CREATE TABLE rfqs (rfqId INT, issuer TEXT, side TEXT, notional INT, status TEXT);
      CREATE TABLE signals (signalId INT, issuer TEXT, direction TEXT, confidence DECIMAL);
      CREATE VIEW aligned AS
        SELECT rfqs.*, signals.direction AS signal_direction, signals.confidence AS signal_confidence
        FROM rfqs
        JOIN signals ON rfqs.issuer = signals.issuer
        WHERE signals.confidence > 0.6
          AND ((rfqs.side = 'BID' AND signals.direction = 'LONG')
               OR (rfqs.side = 'OFFER' AND signals.direction = 'SHORT'));
    `;
    
    const compiler = new SQLCompiler();
    const { circuit, views } = compiler.compile(sql);
    
    const results: any[][] = [];
    views.aligned.integrate().output((zset) => {
      results.push((zset as ZSet<any>).values());
    });
    
    circuit.step(new Map([
      ['rfqs', ZSet.fromValues(rfqs)],
      ['signals', ZSet.fromValues(signals)],
    ]));
    
    const aligned = results[0] || [];
    expect(aligned.length).toBe(2);
    expect(aligned.some((r: any) => r.issuer === 'AAPL')).toBe(true);
    expect(aligned.some((r: any) => r.issuer === 'MSFT')).toBe(true);
  });
});
