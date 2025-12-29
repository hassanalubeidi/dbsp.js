/**
 * View Chaining Tests
 * ====================
 * 
 * Tests that downstream views correctly chain from upstream join views.
 * This is the actual pattern used in CreditTradingPage.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { DBSPSource } from '../../core/DBSPSource';
import { DBSPView } from '../../core/DBSPView';

describe('View Chaining from Join Views', () => {
  let rfqsSource: DBSPSource<any>;
  let signalsSource: DBSPSource<any>;
  
  beforeEach(() => {
    // Create sources like useDBSPSource does
    rfqsSource = new DBSPSource({
      name: 'rfqs',
      key: 'rfqId',
    });
    
    signalsSource = new DBSPSource({
      name: 'signals',
      key: 'signalId',
    });
  });
  
  afterEach(() => {
    rfqsSource.dispose();
    signalsSource.dispose();
  });
  
  it('should chain a filter view from a join view', async () => {
    // Push initial data
    const rfqs = [
      { rfqId: 1, issuer: 'AAPL', side: 'BID', notional: 1000000 },
      { rfqId: 2, issuer: 'MSFT', side: 'OFFER', notional: 2000000 },
      { rfqId: 3, issuer: 'JPM', side: 'BID', notional: 3000000 },
    ];
    
    const signals = [
      { signalId: 1, issuer: 'AAPL', direction: 'LONG', confidence: 0.8 },
      { signalId: 2, issuer: 'MSFT', direction: 'SHORT', confidence: 0.9 },
      { signalId: 3, issuer: 'JPM', direction: 'SHORT', confidence: 0.7 },
    ];
    
    rfqsSource.push(rfqs);
    signalsSource.push(signals);
    
    // Wait for sources to be ready
    await new Promise(resolve => setTimeout(resolve, 50));
    
    console.log('\n=== SOURCE STATUS ===');
    console.log(`rfqs ready: ${rfqsSource.ready}, schema: ${rfqsSource._getSchema()}`);
    console.log(`signals ready: ${signalsSource.ready}, schema: ${signalsSource._getSchema()}`);
    
    // Create the join view (like rfqsWithSignals)
    const joinView = new DBSPView<any, any>({
      sources: [rfqsSource, signalsSource],
      query: `SELECT rfqs.*, signals.direction AS signal_direction, signals.confidence AS signal_confidence
              FROM rfqs
              JOIN signals ON rfqs.issuer = signals.issuer`,
      name: 'rfqSignals',
      joinMode: 'full',
      outputKey: 'rfqId',
    });
    
    // Wait for join view to initialize
    await new Promise(resolve => setTimeout(resolve, 100));
    
    console.log('\n=== JOIN VIEW STATUS ===');
    console.log(`joinView ready: ${joinView.ready}`);
    console.log(`joinView schema: ${joinView._getSchema()}`);
    console.log(`joinView count: ${joinView.count}`);
    console.log(`joinView results:`, joinView.results.map(r => ({
      rfqId: r.rfqId,
      issuer: r.issuer,
      side: r.side,
      signal_direction: r.signal_direction,
      signal_confidence: r.signal_confidence,
    })));
    
    // Now create the chained filter view (like signalAlignedTradesView)
    const filterView = new DBSPView<any, any>({
      sources: [joinView],
      query: `SELECT * FROM rfqSignals
              WHERE signal_confidence > 0.6
                AND ((side = 'BID' AND signal_direction = 'LONG')
                     OR (side = 'OFFER' AND signal_direction = 'SHORT'))`,
      name: 'alignedTrades',
      outputKey: 'rfqId',
    });
    
    // Wait for filter view to initialize
    await new Promise(resolve => setTimeout(resolve, 100));
    
    console.log('\n=== FILTER VIEW STATUS ===');
    console.log(`filterView ready: ${filterView.ready}`);
    console.log(`filterView count: ${filterView.count}`);
    console.log(`filterView results:`, filterView.results);
    
    // Assertions (BEFORE dispose!)
    expect(joinView.count).toBe(3);
    expect(filterView.count).toBe(2); // AAPL (BID+LONG) and MSFT (OFFER+SHORT)
    
    // Cleanup
    filterView.dispose();
    joinView.dispose();
  });
  
  it('should project rfq_notional correctly in positionsWithRFQs scenario', async () => {
    // Create position and rfq sources
    const positionsSource = new DBSPSource<any>({
      name: 'positions',
      key: 'positionId',
    });
    
    const rfqsSource2 = new DBSPSource<any>({
      name: 'rfqs2',
      key: 'rfqId',
    });
    
    // Push data
    positionsSource.push([
      { positionId: 1, bondId: 'ABC', desk: 'HY Trading', notional: 1000000 },
      { positionId: 2, bondId: 'DEF', desk: 'IG Flow', notional: 2000000 },
    ]);
    
    rfqsSource2.push([
      { rfqId: 1, bondId: 'ABC', counterparty: 'JPM', status: 'FILLED', notional: 5000000 },
      { rfqId: 2, bondId: 'DEF', counterparty: 'GS', status: 'PENDING', notional: 3000000 },
    ]);
    
    console.log('\n=== POS+RFQ SOURCES ===');
    console.log(`positions: ${positionsSource.totalRows}, rfqs: ${rfqsSource2.totalRows}`);
    
    // Create the join view like positionsWithRFQs
    const joinView = new DBSPView<any, any>({
      sources: [positionsSource, rfqsSource2],
      query: `SELECT positions.*, rfqs2.counterparty AS rfq_counterparty, rfqs2.status AS rfq_status, rfqs2.notional AS rfq_notional
              FROM positions
              JOIN rfqs2 ON positions.bondId = rfqs2.bondId`,
      name: 'posRFQs',
      joinMode: 'full',
      outputKey: 'positionId',
    });
    
    await new Promise(resolve => setTimeout(resolve, 100));
    
    console.log('\n=== JOIN RESULTS ===');
    console.log(`count: ${joinView.count}`);
    joinView.results.forEach(r => {
      console.log(`  positionId=${r.positionId}, desk=${r.desk}, notional=${r.notional}, rfq_notional=${r.rfq_notional}, rfq_counterparty=${r.rfq_counterparty}`);
    });
    
    // Verify that rfq_notional is correctly projected
    expect(joinView.count).toBe(2);
    const firstRow = joinView.results.find(r => r.positionId === 1);
    expect(firstRow).toBeDefined();
    expect(firstRow?.notional).toBe(1000000);        // From positions
    expect(firstRow?.rfq_notional).toBe(5000000);    // From rfqs
    expect(firstRow?.rfq_counterparty).toBe('JPM');  // From rfqs
    expect(firstRow?.rfq_status).toBe('FILLED');     // From rfqs
    
    // Cleanup
    joinView.dispose();
    positionsSource.dispose();
    rfqsSource2.dispose();
  });
  
  it('should chain aggregation from join view (executionQuality pattern)', async () => {
    // Create position and rfq sources
    const positionsSource = new DBSPSource<any>({
      name: 'positions',
      key: 'positionId',
    });
    
    const rfqsSource3 = new DBSPSource<any>({
      name: 'rfqs3',
      key: 'rfqId',
    });
    
    // Push data
    positionsSource.push([
      { positionId: 1, bondId: 'ABC', desk: 'HY Trading', notional: 1000000 },
      { positionId: 2, bondId: 'DEF', desk: 'HY Trading', notional: 2000000 },
      { positionId: 3, bondId: 'GHI', desk: 'IG Flow', notional: 3000000 },
    ]);
    
    rfqsSource3.push([
      { rfqId: 1, bondId: 'ABC', counterparty: 'JPM', status: 'FILLED', notional: 5000000 },
      { rfqId: 2, bondId: 'DEF', counterparty: 'GS', status: 'FILLED', notional: 3000000 },
      { rfqId: 3, bondId: 'GHI', counterparty: 'MS', status: 'PENDING', notional: 4000000 },
    ]);
    
    console.log('\n=== EXEC QUALITY TEST - SOURCES ===');
    console.log(`positions: ${positionsSource.totalRows}, rfqs: ${rfqsSource3.totalRows}`);
    
    // Create the join view like positionsWithRFQs
    const joinView = new DBSPView<any, any>({
      sources: [positionsSource, rfqsSource3],
      query: `SELECT positions.*, rfqs3.counterparty AS rfq_counterparty, rfqs3.status AS rfq_status, rfqs3.notional AS rfq_notional
              FROM positions
              JOIN rfqs3 ON positions.bondId = rfqs3.bondId`,
      name: 'posRFQs',
      joinMode: 'full',
      outputKey: 'positionId',
    });
    
    await new Promise(resolve => setTimeout(resolve, 100));
    
    console.log('\n=== JOIN VIEW ===');
    console.log(`count: ${joinView.count}`);
    joinView.results.forEach(r => {
      console.log(`  desk=${r.desk}, rfq_notional=${r.rfq_notional}, rfq_status=${r.rfq_status}`);
    });
    
    // Now chain the aggregation view (like executionQualityView)
    const aggView = new DBSPView<any, any>({
      sources: [joinView],
      query: `SELECT 
                desk,
                COUNT(*) AS trades,
                SUM(ABS(rfq_notional)) AS totalNotional,
                SUM(CASE WHEN rfq_status = 'FILLED' THEN 1 ELSE 0 END) AS filledRFQs,
                CASE WHEN COUNT(*) > 0 
                     THEN SUM(CASE WHEN rfq_status = 'FILLED' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) 
                     ELSE 0 END AS executionRate
               FROM posRFQs
               GROUP BY desk`,
      name: 'execQuality',
      outputKey: 'desk',
    });
    
    await new Promise(resolve => setTimeout(resolve, 100));
    
    console.log('\n=== AGGREGATION VIEW ===');
    console.log(`count: ${aggView.count}`);
    aggView.results.forEach(r => {
      console.log(`  desk=${r.desk}, trades=${r.trades}, totalNotional=${r.totalNotional}, executionRate=${r.executionRate}`);
    });
    
    // Verify aggregation results
    expect(aggView.count).toBe(2);
    
    const hyTrading = aggView.results.find(r => r.desk === 'HY Trading');
    expect(hyTrading).toBeDefined();
    expect(hyTrading?.trades).toBe(2);
    expect(hyTrading?.totalNotional).toBe(8000000); // 5M + 3M
    expect(hyTrading?.executionRate).toBeCloseTo(1.0, 2); // Both FILLED
    
    const igFlow = aggView.results.find(r => r.desk === 'IG Flow');
    expect(igFlow).toBeDefined();
    expect(igFlow?.trades).toBe(1);
    expect(igFlow?.totalNotional).toBe(4000000);
    expect(igFlow?.executionRate).toBeCloseTo(0.0, 2); // PENDING
    
    // Cleanup
    aggView.dispose();
    joinView.dispose();
    positionsSource.dispose();
    rfqsSource3.dispose();
  });
  
  it('should handle scalar subqueries in GROUP BY (concentration pattern)', async () => {
    // This tests the pattern used by concentrationHHIView
    const positionsSource = new DBSPSource<any>({
      name: 'positions',
      key: 'positionId',
    });
    
    positionsSource.push([
      { positionId: 1, sector: 'Financials', notional: 1000000 },
      { positionId: 2, sector: 'Financials', notional: 2000000 },
      { positionId: 3, sector: 'Energy', notional: 3000000 },
      { positionId: 4, sector: 'Consumer', notional: 4000000 },
    ]);
    
    console.log('\n=== SCALAR SUBQUERY TEST ===');
    console.log(`positions: ${positionsSource.totalRows}`);
    
    // Create the view with scalar subquery (like topConcentrationView)
    const concentrationView = new DBSPView<any, any>({
      sources: [positionsSource],
      query: `SELECT 
                sector,
                SUM(ABS(notional)) AS sectorNotional,
                (SELECT SUM(ABS(notional)) FROM positions) AS totalNotional
              FROM positions
              GROUP BY sector`,
      name: 'concentration',
      outputKey: 'sector',
    });
    
    await new Promise(resolve => setTimeout(resolve, 150));
    
    console.log('\n=== CONCENTRATION RESULTS ===');
    console.log(`count: ${concentrationView.count}`);
    concentrationView.results.forEach(r => {
      console.log(`  sector=${r.sector}, sectorNotional=${r.sectorNotional}, totalNotional=${r.totalNotional}`);
    });
    
    // Verify results
    expect(concentrationView.count).toBe(3); // 3 sectors
    
    const financials = concentrationView.results.find(r => r.sector === 'Financials');
    expect(financials).toBeDefined();
    expect(financials?.sectorNotional).toBe(3000000); // 1M + 2M
    expect(financials?.totalNotional).toBe(10000000); // Total: 1M + 2M + 3M + 4M
    
    const energy = concentrationView.results.find(r => r.sector === 'Energy');
    expect(energy?.sectorNotional).toBe(3000000);
    expect(energy?.totalNotional).toBe(10000000);
    
    const consumer = concentrationView.results.find(r => r.sector === 'Consumer');
    expect(consumer?.sectorNotional).toBe(4000000);
    expect(consumer?.totalNotional).toBe(10000000);
    
    // Cleanup
    concentrationView.dispose();
    positionsSource.dispose();
  });
  
  it('should handle scalar subqueries inside CASE expressions (topConcentration pattern)', async () => {
    // This tests the pattern used by topConcentrationView
    const positionsSource = new DBSPSource<any>({
      name: 'positions',
      key: 'positionId',
    });
    
    positionsSource.push([
      { positionId: 1, sector: 'Financials', notional: 1000000 },
      { positionId: 2, sector: 'Financials', notional: 2000000 },
      { positionId: 3, sector: 'Energy', notional: 3000000 },
      { positionId: 4, sector: 'Consumer', notional: 4000000 },
    ]);
    
    console.log('\n=== CASE + SCALAR SUBQUERY TEST ===');
    
    // Create the view with scalar subquery inside CASE
    const shareView = new DBSPView<any, any>({
      sources: [positionsSource],
      query: `SELECT 
                sector,
                SUM(ABS(notional)) AS sectorNotional,
                CASE WHEN (SELECT SUM(ABS(notional)) FROM positions) > 0 
                     THEN SUM(ABS(notional)) / (SELECT SUM(ABS(notional)) FROM positions)
                     ELSE 0 END AS share
              FROM positions
              GROUP BY sector
              ORDER BY share DESC`,
      name: 'sectorShare',
      outputKey: 'sector',
    });
    
    await new Promise(resolve => setTimeout(resolve, 150));
    
    console.log('\n=== SHARE RESULTS ===');
    console.log(`count: ${shareView.count}`);
    shareView.results.forEach(r => {
      console.log(`  sector=${r.sector}, sectorNotional=${r.sectorNotional}, share=${r.share}`);
    });
    
    // Verify results
    expect(shareView.count).toBe(3);
    
    const consumer = shareView.results.find(r => r.sector === 'Consumer');
    expect(consumer?.sectorNotional).toBe(4000000);
    // Consumer share = 4M / 10M = 0.4
    expect(consumer?.share).toBeCloseTo(0.4, 2);
    
    const financials = shareView.results.find(r => r.sector === 'Financials');
    expect(financials?.sectorNotional).toBe(3000000);
    // Financials share = 3M / 10M = 0.3
    expect(financials?.share).toBeCloseTo(0.3, 2);
    
    // Cleanup
    shareView.dispose();
    positionsSource.dispose();
  });
  
  it('should handle HAVING with SUM(a + b) > 0 (profitableDesks pattern)', async () => {
    // This tests the pattern used by profitableDesks view
    const positionsSource = new DBSPSource<any>({
      name: 'positions',
      key: 'positionId',
    });
    
    positionsSource.push([
      { positionId: 1, desk: 'HY Trading', unrealizedPnL: 1000, realizedPnL: 500, notional: 1000000 },
      { positionId: 2, desk: 'HY Trading', unrealizedPnL: -200, realizedPnL: 300, notional: 2000000 },
      { positionId: 3, desk: 'IG Flow', unrealizedPnL: -500, realizedPnL: -1000, notional: 3000000 },
      { positionId: 4, desk: 'Credit', unrealizedPnL: 2000, realizedPnL: 1000, notional: 4000000 },
    ]);
    
    await new Promise(resolve => setTimeout(resolve, 50));
    
    console.log('\n=== PROFITABLE DESKS TEST ===');
    console.log(`positions: ${positionsSource.totalRows}`);
    
    const profitableDesksView = new DBSPView<any, any>({
      sources: [positionsSource],
      query: `SELECT desk,
                     SUM(unrealizedPnL + realizedPnL) AS totalPnL,
                     SUM(notional) AS notional,
                     COUNT(*) AS tradeCount
              FROM positions
              GROUP BY desk
              HAVING SUM(unrealizedPnL + realizedPnL) > 0`,
      name: 'profitableDesks',
      outputKey: 'desk',
    });
    
    await new Promise(resolve => setTimeout(resolve, 100));
    
    console.log('\n=== RESULTS ===');
    console.log(`count: ${profitableDesksView.count}`);
    profitableDesksView.results.forEach(r => {
      console.log(`  desk=${r.desk}, totalPnL=${r.totalPnL}, notional=${r.notional}, tradeCount=${r.tradeCount}`);
    });
    
    // Verify results
    // HY Trading: (1000+500) + (-200+300) = 1500 + 100 = 1600 > 0 ✓
    // IG Flow: (-500) + (-1000) = -1500 < 0 ✗
    // Credit: (2000+1000) = 3000 > 0 ✓
    expect(profitableDesksView.count).toBe(2);
    
    const hyTrading = profitableDesksView.results.find(r => r.desk === 'HY Trading');
    expect(hyTrading).toBeDefined();
    expect(hyTrading?.totalPnL).toBe(1600);
    expect(hyTrading?.tradeCount).toBe(2);
    
    const credit = profitableDesksView.results.find(r => r.desk === 'Credit');
    expect(credit).toBeDefined();
    expect(credit?.totalPnL).toBe(3000);
    expect(credit?.tradeCount).toBe(1);
    
    const igFlow = profitableDesksView.results.find(r => r.desk === 'IG Flow');
    expect(igFlow).toBeUndefined(); // Should be filtered out by HAVING
    
    // Cleanup
    profitableDesksView.dispose();
    positionsSource.dispose();
  });
  
  it('should handle deltas correctly', async () => {
    // Initial data
    rfqsSource.push([
      { rfqId: 1, issuer: 'AAPL', side: 'BID', notional: 1000000 },
    ]);
    signalsSource.push([
      { signalId: 1, issuer: 'AAPL', direction: 'LONG', confidence: 0.8 },
    ]);
    
    await new Promise(resolve => setTimeout(resolve, 50));
    
    // Create views
    const joinView = new DBSPView<any, any>({
      sources: [rfqsSource, signalsSource],
      query: `SELECT rfqs.*, signals.direction AS signal_direction, signals.confidence AS signal_confidence
              FROM rfqs
              JOIN signals ON rfqs.issuer = signals.issuer`,
      name: 'rfqSignals',
      joinMode: 'full',
      outputKey: 'rfqId',
    });
    
    await new Promise(resolve => setTimeout(resolve, 100));
    
    const filterView = new DBSPView<any, any>({
      sources: [joinView],
      query: `SELECT * FROM rfqSignals
              WHERE signal_confidence > 0.6
                AND ((side = 'BID' AND signal_direction = 'LONG')
                     OR (side = 'OFFER' AND signal_direction = 'SHORT'))`,
      name: 'alignedTrades',
      outputKey: 'rfqId',
    });
    
    await new Promise(resolve => setTimeout(resolve, 100));
    
    console.log('\n=== BEFORE DELTA ===');
    console.log(`joinView count: ${joinView.count}`);
    console.log(`filterView count: ${filterView.count}`);
    
    expect(joinView.count).toBe(1);
    expect(filterView.count).toBe(1); // AAPL matches
    
    // Now push a delta - new RFQ that should match
    rfqsSource.push([
      { rfqId: 2, issuer: 'MSFT', side: 'OFFER', notional: 2000000 },
    ]);
    signalsSource.push([
      { signalId: 2, issuer: 'MSFT', direction: 'SHORT', confidence: 0.9 },
    ]);
    
    // Wait for delta to propagate
    await new Promise(resolve => setTimeout(resolve, 200));
    
    console.log('\n=== AFTER DELTA ===');
    console.log(`joinView count: ${joinView.count}`);
    console.log(`filterView count: ${filterView.count}`);
    console.log(`filterView results:`, filterView.results);
    
    // Assertions (BEFORE dispose!)
    expect(joinView.count).toBe(2);
    expect(filterView.count).toBe(2); // Both AAPL and MSFT should match now
    
    // Cleanup
    filterView.dispose();
    joinView.dispose();
  });
});

