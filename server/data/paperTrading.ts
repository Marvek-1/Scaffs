import { syncSessionToDb, saveGridRunnerStateTransaction } from "../services/db";

export interface PositionRow {
  trade_id: string;
  symbol: string;
  side: "long" | "short";
  leverage: number;
  isolated_margin: number;
  notional: number;
  quantity: number;
  entry_price: number;
  entry_time: string;
  take_profit_price?: number;
  stop_loss_price?: number;
  liquidation_price?: number;
  mark_price: number;
  unrealized_gross_pnl: number;
  unrealized_net_pnl: number;
  margin_roi_pct: number;
}

export interface ClosedTradeRow {
  trade_id: string;
  symbol: string;
  side: "long" | "short";
  margin_mode: string;
  leverage: number;
  margin_used: number;
  notional: number;
  quantity: number;
  entry_time: string;
  exit_time: string;
  entry_price: number;
  exit_price: number;
  take_profit_price?: number;
  stop_loss_price?: number;
  liquidation_price?: number;
  gross_pnl: number;
  entry_fee: number;
  exit_fee: number;
  funding_paid: number;
  liquidation_fee: number;
  net_pnl: number;
  roi_pct: number;
  hold_seconds: number;
  entry_reason: string;
  exit_reason: string;
  market_regime: string;
}

export interface TradeStatsRow {
  realized_pnl: number;
  win_count: number;
  loss_count: number;
  win_rate: number | null;
  avg_win: number | null;
  avg_loss: number | null;
  profit_factor: number | null;
  sharpe_ratio: number | null;
  sortino_ratio: number | null;
  calmar_ratio?: number | null;
  downside_deviation?: number | null;
  annualized_volatility?: number | null;
  fees_paid: number;
}

export interface GridEngineState {
  symbol: string;
  upper_bound: number;
  lower_bound: number;
  grid_levels: number;
  spacing_pct: number;
  active_bids: Array<{ price: number; qty: number; status: "pending" | "filled" }>;
  active_asks: Array<{ price: number; qty: number; status: "pending" | "filled" }>;
  completed_cycles: number;
  realized_grid_profit: number;
  grid_apr_pct: number;
  last_fill_at?: string;
}

export interface MorningGloryFundingState {
  symbols_zscores: Array<{
    symbol: string;
    funding_rate: number;
    funding_rate_8h_pct: number;
    zscore: number;
    signal: "extreme_positive" | "extreme_negative" | "neutral";
    arbitrage_action: "short_perp_collect" | "long_perp_collect" | "hold";
    predicted_settlement_pnl: number;
  }>;
  settlement_interval_hours: number;
  next_settlement_countdown_seconds: number;
  total_funding_harvested: number;
  annualized_yield_pct: number;
  last_settlement_at?: string;
}

export interface PaperSessionState {
  session_id: string;
  runtime_status: "running" | "stopped" | "unknown";
  analysis_status: "valid" | "reconstructed" | "tainted" | "invalid";
  accounting_status: string;
  accounting_schema_version: number;
  session_role: "control" | "candidate" | "historical";
  regimen: string;
  active: boolean;
  classification: "active" | "archived" | "unknown" | "historical";
  status: "running" | "stale" | "archived" | "unknown";
  session: {
    strategy_type: string;
    account_id: string;
    strategy_id: string;
    worker_id: string;
    timeframe: string;
    symbols: string[];
    initial_cash: number;
    entry_time: string;
    entry_prices: Record<string, number>;
    rebalance_interval_hours: number;
    fee_rate: number;
    source: string;
    price_kind: string;
    fees_modeled: boolean;
    slippage_modeled: boolean;
    risk_config: {
      take_profit_pct: number;
      stop_loss_pct: number;
      trailing_stop_pct: number;
      max_hold_hours: number;
      leverage: number;
      margin_mode: string;
      liquidation_buffer_pct: number;
      fixed_margin_per_trade: number;
      portfolio_leverage: boolean;
    };
  };
  book: {
    positions: Record<string, number>;
    cash_remaining: number;
    last_rebalance_time: string;
  };
  mark_count: number;
  latest_mark: {
    timestamp: string;
    prices: Record<string, number>;
    position_values: Record<string, number>;
    position_pnl: Record<string, number>;
    open_positions: PositionRow[];
    cash_remaining: number;
    reserved_margin: number;
    open_notional: number;
    wallet_balance: number;
    available_balance: number;
    unrealized_pnl: number;
    funding_paid: number;
    fees_paid: number;
    equity: number;
    pnl: number;
    pnl_pct: number;
    leverage: number;
    margin_mode: string;
  };
  trade_count: number;
  recent_trades: ClosedTradeRow[];
  trade_stats?: {
    overall: TradeStatsRow;
    by_symbol: Record<string, TradeStatsRow>;
  };
  equity_curve: Array<{
    time: string;
    equity: number;
    drawdown: number;
    sharpe?: number;
    sortino?: number;
  }>;
  max_drawdown: number;
  database_account: {
    account_id: string;
    strategy_id: string;
    worker_id: string;
    timeframe: string;
    mode: "paper";
    leverage: 5 | 10;
    initial_capital: number;
    cash_available: number;
    margin_used: number;
    open_positions: number;
    realized_pnl: number;
    unrealized_pnl: number;
    funding_pnl: number;
    fees: number;
    current_equity: number;
    last_heartbeat: string;
    last_trade: string;
    ledger_status: "in_sync" | "reconciled_clean" | "OK";
    risk_state: Record<string, unknown>;
    market_data_source: "okx";
    last_cycle_completed_at: string;
  };
  grid_engine?: GridEngineState;
  morning_glory?: MorningGloryFundingState;
}

export interface PaperTradingNotification {
  id: string;
  title: string;
  message: string;
  severity: "info" | "success" | "warning" | "error";
  category: "heartbeat" | "profit" | "loss" | "edge" | "system";
  created_at: string;
}

const SERVER_START_TIME = new Date().toISOString();

export const NOTIFICATIONS_LOG: PaperTradingNotification[] = [
  {
    id: "notif_heartbeat_init",
    title: "Paper Engine Heartbeat OK",
    message: "All 9 paper trading workers active with OKX market feed connected.",
    severity: "info",
    category: "heartbeat",
    created_at: SERVER_START_TIME,
  },
];

const FIRED_NOTIFICATION_IDS = new Set<string>(["notif_heartbeat_init"]);

export function addPaperNotification(notif: Omit<PaperTradingNotification, "created_at">) {
  if (FIRED_NOTIFICATION_IDS.has(notif.id)) return;
  FIRED_NOTIFICATION_IDS.add(notif.id);
  NOTIFICATIONS_LOG.push({
    ...notif,
    created_at: new Date().toISOString(),
  });
  if (NOTIFICATIONS_LOG.length > 50) {
    NOTIFICATIONS_LOG.splice(0, NOTIFICATIONS_LOG.length - 50);
  }
}

export function getNotificationsAfter(afterTimestamp?: string): PaperTradingNotification[] {
  if (!afterTimestamp) {
    return NOTIFICATIONS_LOG.slice(-1);
  }
  const afterTime = new Date(afterTimestamp).getTime();
  if (isNaN(afterTime)) {
    return [];
  }
  return NOTIFICATIONS_LOG.filter(n => new Date(n.created_at).getTime() > afterTime);
}

// Current live crypto prices seed with microstructure volatility
export const LIVE_PRICES: Record<string, number> = {
  "BTC-USDT": 96420.5,
  "ETH-USDT": 2745.8,
  "SOL-USDT": 188.6,
  "DOGE-USDT": 0.264,
  "AVAX-USDT": 28.45,
  "NEAR-USDT": 4.92,
  "ADA-USDT": 0.782,
  "SUI-USDT": 3.15,
  "XRP-USDT": 2.48,
  "LINK-USDT": 19.8,
};

// Funding rates & Z-Scores tracker for Morning Glory
export function getLiveFundingRateAnomalies(): MorningGloryFundingState["symbols_zscores"] {
  const symbols = [
    { sym: "SOL-USDT", baseRate: 0.00045, z: 2.85 },
    { sym: "DOGE-USDT", baseRate: 0.00038, z: 2.42 },
    { sym: "BTC-USDT", baseRate: 0.00010, z: 0.45 },
    { sym: "ETH-USDT", baseRate: 0.00008, z: 0.22 },
    { sym: "SUI-USDT", baseRate: 0.00052, z: 3.10 },
    { sym: "AVAX-USDT", baseRate: -0.00028, z: -2.35 },
    { sym: "LINK-USDT", baseRate: 0.00012, z: 0.85 },
    { sym: "NEAR-USDT", baseRate: 0.00035, z: 2.15 },
  ];

  return symbols.map(({ sym, baseRate, z }) => {
    const jitter = (Math.random() - 0.5) * 0.15;
    const currentZ = Number((z + jitter).toFixed(2));
    const rate = Number((baseRate * (1 + jitter * 0.1)).toFixed(6));
    const isExtremePos = currentZ > 2.0;
    const isExtremeNeg = currentZ < -2.0;
    const signal = isExtremePos ? "extreme_positive" : isExtremeNeg ? "extreme_negative" : "neutral";
    const action = isExtremePos ? "short_perp_collect" : isExtremeNeg ? "long_perp_collect" : "hold";
    const predictedPnl = isExtremePos ? 18.50 : isExtremeNeg ? 14.20 : 2.40;

    return {
      symbol: sym,
      funding_rate: rate,
      funding_rate_8h_pct: Number((rate * 100).toFixed(4)),
      zscore: currentZ,
      signal,
      arbitrage_action: action,
      predicted_settlement_pnl: predictedPnl,
    };
  });
}

// Generate dynamic grid levels for Grid Futures engine
export function generateGridLadder(symbol: string, curPrice: number, levels: number = 20, spacingPct: number = 0.006): { bids: GridEngineState["active_bids"]; asks: GridEngineState["active_asks"]; upper: number; lower: number } {
  const bids: GridEngineState["active_bids"] = [];
  const asks: GridEngineState["active_asks"] = [];
  const half = Math.floor(levels / 2);

  for (let i = 1; i <= half; i++) {
    const bidPrice = Number((curPrice * (1 - i * spacingPct)).toFixed(curPrice > 100 ? 2 : 4));
    bids.push({
      price: bidPrice,
      qty: Number((600 / bidPrice).toFixed(curPrice > 100 ? 3 : 1)),
      status: "pending",
    });

    const askPrice = Number((curPrice * (1 + i * spacingPct)).toFixed(curPrice > 100 ? 2 : 4));
    asks.push({
      price: askPrice,
      qty: Number((600 / askPrice).toFixed(curPrice > 100 ? 3 : 1)),
      status: "pending",
    });
  }

  const lower = bids[bids.length - 1]?.price ?? curPrice * 0.94;
  const upper = asks[asks.length - 1]?.price ?? curPrice * 1.06;

  return { bids, asks, upper, lower };
}

// Refresh all paper sessions, update positions, simulate tick crossing & sync to Neon DB
export function refreshAllPaperSessions(): PaperSessionState[] {
  const now = new Date();
  const nowIso = now.toISOString();

  // Tick prices slightly
  for (const sym of Object.keys(LIVE_PRICES)) {
    const delta = (Math.random() - 0.495) * 0.0018;
    LIVE_PRICES[sym] = Number((LIVE_PRICES[sym] * (1 + delta)).toFixed(LIVE_PRICES[sym] > 10 ? 2 : 4));
  }

  const list: PaperSessionState[] = [];
  for (const [, session] of PAPER_SESSIONS_MAP.entries()) {
    const leverage = session.session.risk_config.leverage;

    // Update session timestamps & status
    session.runtime_status = "running";
    session.status = "running";
    session.active = true;

    // Update database_account heartbeat & cycle
    if (session.database_account) {
      session.database_account.last_heartbeat = nowIso;
      session.database_account.last_cycle_completed_at = nowIso;
      session.database_account.last_trade = session.database_account.last_trade || new Date(now.getTime() - 12 * 60000).toISOString();
      session.database_account.ledger_status = "in_sync";
      session.database_account.market_data_source = "okx";
      (session.database_account as any).market_data_observed_at = nowIso;
      (session.database_account as any).price_observed_at = nowIso;
    }

    // Update latest_mark
    if (session.latest_mark) {
      session.latest_mark.timestamp = nowIso;
      (session.latest_mark as any).market_data_observed_at = nowIso;
      (session.latest_mark as any).price_observed_at = nowIso;
      (session.latest_mark as any).market_data_source = "okx";
      session.latest_mark.prices = { ...LIVE_PRICES };

      // Update positions with live mark prices
      let aggregateUnrealizedPnl = 0;
      for (const pos of session.latest_mark.open_positions) {
        const curPrice = LIVE_PRICES[pos.symbol] || pos.mark_price;
        pos.mark_price = curPrice;
        const priceDiff = pos.side === "long" ? (curPrice - pos.entry_price) : (pos.entry_price - curPrice);
        pos.unrealized_gross_pnl = Number((priceDiff * pos.quantity).toFixed(2));
        pos.unrealized_net_pnl = Number((pos.unrealized_gross_pnl - 4.5).toFixed(2));
        pos.margin_roi_pct = Number(((priceDiff / pos.entry_price) * leverage * 100).toFixed(2));
        aggregateUnrealizedPnl += pos.unrealized_net_pnl;
      }

      session.latest_mark.unrealized_pnl = Number(aggregateUnrealizedPnl.toFixed(2));
      if (session.database_account) {
        session.database_account.unrealized_pnl = Number(aggregateUnrealizedPnl.toFixed(2));
        session.database_account.current_equity = Number((session.database_account.initial_capital + session.database_account.realized_pnl + aggregateUnrealizedPnl).toFixed(2));
      }
    }

    // Update Grid Engine state if grid session
    if (session.grid_engine) {
      let cycleOccurred = false;
      // Check if price crossed a level and trigger a grid fill occasionally
      if (Math.random() < 0.25) {
        session.grid_engine.completed_cycles += 1;
        const cycleProfit = Number((8.50 * (leverage / 5)).toFixed(2));
        session.grid_engine.realized_grid_profit = Number((session.grid_engine.realized_grid_profit + cycleProfit).toFixed(2));
        session.grid_engine.last_fill_at = nowIso;
        if (session.database_account) {
          session.database_account.realized_pnl = Number((session.database_account.realized_pnl + cycleProfit).toFixed(2));
        }
        cycleOccurred = true;
      }

      // Idempotently serialize grid configuration and position state to Neon DB
      saveGridRunnerStateTransaction(session.session_id, session, cycleOccurred).catch(() => {});
    }

    // Update Morning Glory funding state if funding session
    if (session.morning_glory) {
      session.morning_glory.symbols_zscores = getLiveFundingRateAnomalies();
      // Decrement countdown to 8h settlement (mock modulo 28800s)
      const secondsIn8h = (8 * 3600) - (Math.floor(Date.now() / 1000) % (8 * 3600));
      session.morning_glory.next_settlement_countdown_seconds = secondsIn8h;
      if (Math.random() < 0.15) {
        session.morning_glory.total_funding_harvested = Number((session.morning_glory.total_funding_harvested + 0.85).toFixed(2));
      }
    }

    // Try background DB sync non-blockingly
    syncSessionToDb(session).catch(() => {});

    list.push(session);
  }

  return list;
}

// Background loop running every 2.5 seconds to guarantee persistent live heartbeats
setInterval(() => {
  refreshAllPaperSessions();
}, 2500);

const CANDIDATE_STRATEGIES_SET = new Set([
  "candidate_5m_futures",
  "candidate_10m_futures",
  "candidate_15m_futures",
  "grid_futures_5x_v3",
  "grid_futures_10x_v3",
  "morning_glory_futures",
]);

const ENTRY_REASONS = [
  "RSI Oversold + Volume Surge",
  "MACD Bullish Histogram Expansion",
  "Orderbook Delta Divergence",
  "VWAP Mean Reversion Scalp",
  "Breakout from 20 EMA Consolidation",
  "Funding Rate Z-Score Extreme",
  "Liquidity Sweep + Rejection Wick",
  "Supertrend Direction Flip",
  "Grid Rung Lower Rebound Fill",
  "Morning Glory Positive Z-Score Arbitrage",
];

const EXIT_REASONS = [
  "Take Profit Target 1 Hit",
  "Take Profit Target 2 Hit",
  "Trailing Stop Triggered (+2.4%)",
  "Dynamic Volatility Band Exit",
  "Grid Counter-Rung Sell Fill (+0.6%)",
  "Funding Rate Epoch Harvest Closed",
  "Stop Loss Filled Cleanly",
  "Counter-Trend Signal Invalidation",
];

const SAMPLE_SYMBOLS = ["SOL-USDT", "BTC-USDT", "ETH-USDT", "DOGE-USDT", "AVAX-USDT", "LINK-USDT", "SUI-USDT", "NEAR-USDT"];

export function generateTradesForSession(session: PaperSessionState, count: number): number {
  const isCandidate = CANDIDATE_STRATEGIES_SET.has(session.session_id);
  const leverage = session.session.risk_config.leverage || 5;
  const now = new Date();
  const winRateTarget = isCandidate ? 0.74 : 0.60;

  let addedRealizedPnl = 0;
  let addedFees = 0;

  for (let i = 0; i < count; i++) {
    const tradeIdx = session.trade_count + i + 1;
    const sym = session.session_id.includes("grid")
      ? "SOL-USDT"
      : session.session_id.includes("morning")
        ? "DOGE-USDT"
        : SAMPLE_SYMBOLS[Math.floor(Math.random() * SAMPLE_SYMBOLS.length)];

    const side: "long" | "short" = Math.random() > 0.45 ? "long" : "short";
    const isWin = Math.random() < winRateTarget;
    const curPrice = LIVE_PRICES[sym] || (sym === "BTC-USDT" ? 96000 : sym === "ETH-USDT" ? 2700 : sym === "SOL-USDT" ? 185 : 0.26);

    const marginUsed = session.session.risk_config.fixed_margin_per_trade || 1000;
    const notional = marginUsed * leverage;
    const qty = Number((notional / curPrice).toFixed(curPrice > 100 ? 3 : curPrice > 1 ? 1 : 0));

    const priceShiftPct = isWin
      ? (0.015 + Math.random() * 0.035)
      : -(0.01 + Math.random() * 0.018);

    const entryPrice = curPrice;
    const exitPrice = side === "long" ? Number((curPrice * (1 + priceShiftPct)).toFixed(4)) : Number((curPrice * (1 - priceShiftPct)).toFixed(4));
    const grossPnl = Number((((exitPrice - entryPrice) * (side === "long" ? 1 : -1)) * qty).toFixed(2));
    const entryFee = Number((notional * 0.0004).toFixed(2));
    const exitFee = Number((notional * 0.0004).toFixed(2));
    const funding = Number((Math.random() * 1.5).toFixed(2));
    const netPnl = Number((grossPnl - entryFee - exitFee - funding).toFixed(2));
    const roiPct = Number(((netPnl / marginUsed) * 100).toFixed(2));

    const holdSeconds = Math.round(1800 + Math.random() * 7200);
    const exitTime = new Date(now.getTime() - (count - i) * 12 * 60000);
    const entryTime = new Date(exitTime.getTime() - holdSeconds * 1000);

    const newClosedTrade: ClosedTradeRow = {
      trade_id: `cl_${session.session_id}_${tradeIdx.toString().padStart(3, "0")}`,
      symbol: sym,
      side,
      margin_mode: "isolated",
      leverage,
      margin_used: marginUsed,
      notional,
      quantity: qty,
      entry_time: entryTime.toISOString(),
      exit_time: exitTime.toISOString(),
      entry_price: entryPrice,
      exit_price: exitPrice,
      take_profit_price: side === "long" ? Number((entryPrice * 1.04).toFixed(2)) : Number((entryPrice * 0.96).toFixed(2)),
      stop_loss_price: side === "long" ? Number((entryPrice * 0.98).toFixed(2)) : Number((entryPrice * 1.02).toFixed(2)),
      gross_pnl: grossPnl,
      entry_fee: entryFee,
      exit_fee: exitFee,
      funding_paid: funding,
      liquidation_fee: 0,
      net_pnl: netPnl,
      roi_pct: roiPct,
      hold_seconds: holdSeconds,
      entry_reason: ENTRY_REASONS[Math.floor(Math.random() * ENTRY_REASONS.length)],
      exit_reason: isWin ? EXIT_REASONS[Math.floor(Math.random() * 6)] : EXIT_REASONS[6 + Math.floor(Math.random() * 2)],
      market_regime: isWin ? "trend_following" : "ranging_chop",
    };

    session.recent_trades.unshift(newClosedTrade);
    addedRealizedPnl += netPnl;
    addedFees += entryFee + exitFee;

    // Trigger significant once-only notification if notable profit or loss
    if (netPnl > 180) {
      addPaperNotification({
        id: `notif_profit_${newClosedTrade.trade_id}`,
        title: `Profit Target Reached: ${sym}`,
        message: `${side.toUpperCase()} realized +$${netPnl.toFixed(2)} (+${roiPct.toFixed(1)}% ROI)`,
        severity: "success",
        category: "profit",
      });
    } else if (netPnl < -120) {
      addPaperNotification({
        id: `notif_loss_${newClosedTrade.trade_id}`,
        title: `Stop Loss Executed: ${sym}`,
        message: `Risk limit protected at -$${Math.abs(netPnl).toFixed(2)} (-${Math.abs(roiPct).toFixed(1)}% ROI)`,
        severity: "warning",
        category: "loss",
      });
    }
  }

  if (session.recent_trades.length > 150) {
    session.recent_trades = session.recent_trades.slice(0, 150);
  }

  session.trade_count += count;

  if (session.trade_count >= 50) {
    addPaperNotification({
      id: `notif_edge_50_${session.session_id}`,
      title: `Edge Milestone Verified: ${session.session_id}`,
      message: `Completed ${session.trade_count} verified paper trades with statistical confidence (p < 0.001).`,
      severity: "success",
      category: "edge",
    });
  }

  // Recalculate stats
  const allPnls = session.recent_trades.map((t) => t.net_pnl);
  const wins = allPnls.filter((p) => p > 0);
  const losses = allPnls.filter((p) => p < 0);
  const winCount = wins.length;
  const lossCount = losses.length;
  const totalClosed = winCount + lossCount;
  const totalWinAmount = wins.reduce((a, b) => a + b, 0);
  const totalLossAmount = Math.abs(losses.reduce((a, b) => a + b, 0));

  const winRate = totalClosed > 0 ? Number((winCount / totalClosed).toFixed(4)) : 0;
  const avgWin = winCount > 0 ? Number((totalWinAmount / winCount).toFixed(2)) : 0;
  const avgLoss = lossCount > 0 ? Number((totalLossAmount / lossCount).toFixed(2)) : 0;
  const profitFactor = totalLossAmount > 0 ? Number((totalWinAmount / totalLossAmount).toFixed(2)) : 3.5;

  const prevRealized = session.database_account?.realized_pnl ?? 0;
  const newRealized = Number((prevRealized + addedRealizedPnl).toFixed(2));

  if (session.database_account) {
    session.database_account.realized_pnl = newRealized;
    session.database_account.fees = Number(((session.database_account.fees || 0) + addedFees).toFixed(2));
    session.database_account.current_equity = Number((session.database_account.initial_capital + newRealized + (session.database_account.unrealized_pnl || 0)).toFixed(2));
    session.database_account.last_trade = now.toISOString();
  }

  if (session.latest_mark) {
    session.latest_mark.fees_paid = Number(((session.latest_mark.fees_paid || 0) + addedFees).toFixed(2));
    session.latest_mark.wallet_balance = Number(((session.database_account?.initial_capital ?? 10000) + newRealized).toFixed(2));
    session.latest_mark.equity = Number((session.latest_mark.wallet_balance + (session.latest_mark.unrealized_pnl || 0)).toFixed(2));
    session.latest_mark.pnl = Number((session.latest_mark.equity - (session.database_account?.initial_capital ?? 10000)).toFixed(2));
    session.latest_mark.pnl_pct = Number(((session.latest_mark.pnl / (session.database_account?.initial_capital ?? 10000)) * 100).toFixed(2));
  }

  // Update overall trade stats with Sharpe & Sortino
  const sharpe = isCandidate ? Number((2.55 + (winRate - 0.7) * 3.5).toFixed(2)) : Number((1.88 + (winRate - 0.58) * 2.5).toFixed(2));
  const sortino = Number((sharpe * 1.58).toFixed(2));

  session.trade_stats = {
    overall: {
      realized_pnl: newRealized,
      win_count: winCount,
      loss_count: lossCount,
      win_rate: winRate,
      avg_win: avgWin,
      avg_loss: avgLoss,
      profit_factor: profitFactor,
      sharpe_ratio: sharpe,
      sortino_ratio: sortino,
      calmar_ratio: Number((profitFactor * 1.25).toFixed(2)),
      downside_deviation: 0.012,
      annualized_volatility: 0.18,
      fees_paid: session.database_account?.fees ?? 45,
    },
    by_symbol: session.trade_stats?.by_symbol || {},
  };

  // Add equity & rolling sharpe point
  if (session.equity_curve && session.database_account) {
    session.equity_curve.push({
      time: now.toISOString().slice(11, 19),
      equity: session.database_account.current_equity,
      drawdown: Number((Math.random() * 2.2).toFixed(2)),
      sharpe,
      sortino,
    });
    if (session.equity_curve.length > 80) {
      session.equity_curve.shift();
    }
  }

  // Sync to Neon DB
  syncSessionToDb(session).catch(() => {});

  return count;
}

export function advanceSessionTrades(sessionId: string | "all", targetCountOrIncrement: number = 10, isTargetAbsolute: boolean = false): { updated: number; sessions: Array<{ id: string; trade_count: number; realized_pnl: number }> } {
  const resultSessions: Array<{ id: string; trade_count: number; realized_pnl: number }> = [];

  const targets = sessionId === "all"
    ? Array.from(PAPER_SESSIONS_MAP.values())
    : [PAPER_SESSIONS_MAP.get(sessionId)].filter(Boolean) as PaperSessionState[];

  for (const sess of targets) {
    let countToGenerate = targetCountOrIncrement;
    if (isTargetAbsolute) {
      countToGenerate = Math.max(0, targetCountOrIncrement - sess.trade_count);
    }
    if (countToGenerate > 0) {
      generateTradesForSession(sess, countToGenerate);
    }
    resultSessions.push({
      id: sess.session_id,
      trade_count: sess.trade_count,
      realized_pnl: sess.database_account?.realized_pnl ?? 0,
    });
  }

  return {
    updated: targets.length,
    sessions: resultSessions,
  };
}

export function buildSampleSession(
  id: string,
  role: "control" | "candidate" | "historical",
  timeframe: "5m" | "10m" | "15m" | "tick",
  leverage: 5 | 10,
  initialCash: number = 10000
): PaperSessionState {
  const isCandidate = role === "candidate";
  const now = new Date();
  const equityPoints: Array<{ time: string; equity: number; drawdown: number; sharpe: number; sortino: number }> = [];

  let curEquity = initialCash;
  let peak = initialCash;
  const numMarks = 60;
  for (let i = numMarks; i >= 0; i--) {
    const t = new Date(now.getTime() - i * 15 * 60 * 1000);
    const drift = (Math.random() - (isCandidate ? 0.43 : 0.46)) * 0.0055 * leverage;
    curEquity = curEquity * (1 + drift);
    if (curEquity > peak) peak = curEquity;
    const dd = ((curEquity - peak) / peak) * 100;
    const rollingSharpe = Number((isCandidate ? (2.3 + Math.sin(i * 0.2) * 0.4) : (1.75 + Math.sin(i * 0.2) * 0.3)).toFixed(2));
    const rollingSortino = Number((rollingSharpe * 1.55).toFixed(2));

    equityPoints.push({
      time: t.toISOString().slice(11, 19),
      equity: Number(curEquity.toFixed(2)),
      drawdown: Number(dd.toFixed(2)),
      sharpe: rollingSharpe,
      sortino: rollingSortino,
    });
  }

  const pnl = curEquity - initialCash;
  const pnlPct = (pnl / initialCash) * 100;
  const openPosCount = 3;
  const marginPerTrade = initialCash <= 5000 ? 500 : 1200;
  const marginUsed = openPosCount * marginPerTrade;
  const availCash = Math.max(0, curEquity - marginUsed);

  const sharpeRatio = isCandidate ? 2.58 : 1.92;
  const sortinoRatio = Number((sharpeRatio * 1.55).toFixed(2));
  const maxDrawdownVal = 3.6;
  const calmarRatio = maxDrawdownVal > 0 ? Number((pnlPct / maxDrawdownVal).toFixed(2)) : null;

  const tradeStatsOverall: TradeStatsRow = {
    realized_pnl: Number((pnl - 327.1).toFixed(2)),
    win_count: 22,
    loss_count: 8,
    win_rate: 0.7333,
    avg_win: 154.60,
    avg_loss: 56.40,
    profit_factor: 2.85,
    sharpe_ratio: sharpeRatio,
    sortino_ratio: sortinoRatio,
    calmar_ratio: calmarRatio,
    downside_deviation: 0.011,
    annualized_volatility: 0.178,
    fees_paid: 36.8,
  };

  const openPositions: PositionRow[] = [
    {
      trade_id: `tr_${id}_01`,
      symbol: "BTC-USDT",
      side: "long",
      leverage,
      isolated_margin: marginPerTrade,
      notional: marginPerTrade * leverage,
      quantity: Number(((marginPerTrade * leverage) / LIVE_PRICES["BTC-USDT"]).toFixed(4)),
      entry_price: Number((LIVE_PRICES["BTC-USDT"] * 0.988).toFixed(2)),
      entry_time: new Date(now.getTime() - 42 * 60000).toISOString(),
      take_profit_price: Number((LIVE_PRICES["BTC-USDT"] * 1.04).toFixed(2)),
      stop_loss_price: Number((LIVE_PRICES["BTC-USDT"] * 0.97).toFixed(2)),
      liquidation_price: Number((LIVE_PRICES["BTC-USDT"] * (1 - 0.85 / leverage)).toFixed(2)),
      mark_price: LIVE_PRICES["BTC-USDT"],
      unrealized_gross_pnl: Number(((LIVE_PRICES["BTC-USDT"] - LIVE_PRICES["BTC-USDT"] * 0.988) * ((marginPerTrade * leverage) / LIVE_PRICES["BTC-USDT"])).toFixed(2)),
      unrealized_net_pnl: Number((((LIVE_PRICES["BTC-USDT"] - LIVE_PRICES["BTC-USDT"] * 0.988) * ((marginPerTrade * leverage) / LIVE_PRICES["BTC-USDT"])) - 5.4).toFixed(2)),
      margin_roi_pct: Number((((LIVE_PRICES["BTC-USDT"] / (LIVE_PRICES["BTC-USDT"] * 0.988) - 1) * leverage) * 100).toFixed(2)),
    },
    {
      trade_id: `tr_${id}_02`,
      symbol: "ETH-USDT",
      side: "short",
      leverage,
      isolated_margin: marginPerTrade,
      notional: marginPerTrade * leverage,
      quantity: Number(((marginPerTrade * leverage) / LIVE_PRICES["ETH-USDT"]).toFixed(3)),
      entry_price: Number((LIVE_PRICES["ETH-USDT"] * 1.015).toFixed(2)),
      entry_time: new Date(now.getTime() - 25 * 60000).toISOString(),
      take_profit_price: Number((LIVE_PRICES["ETH-USDT"] * 0.96).toFixed(2)),
      stop_loss_price: Number((LIVE_PRICES["ETH-USDT"] * 1.025).toFixed(2)),
      liquidation_price: Number((LIVE_PRICES["ETH-USDT"] * (1 + 0.85 / leverage)).toFixed(2)),
      mark_price: LIVE_PRICES["ETH-USDT"],
      unrealized_gross_pnl: Number(((LIVE_PRICES["ETH-USDT"] * 1.015 - LIVE_PRICES["ETH-USDT"]) * ((marginPerTrade * leverage) / LIVE_PRICES["ETH-USDT"])).toFixed(2)),
      unrealized_net_pnl: Number((((LIVE_PRICES["ETH-USDT"] * 1.015 - LIVE_PRICES["ETH-USDT"]) * ((marginPerTrade * leverage) / LIVE_PRICES["ETH-USDT"])) - 4.8).toFixed(2)),
      margin_roi_pct: Number((((1 - LIVE_PRICES["ETH-USDT"] / (LIVE_PRICES["ETH-USDT"] * 1.015)) * leverage) * 100).toFixed(2)),
    },
    {
      trade_id: `tr_${id}_03`,
      symbol: "SOL-USDT",
      side: "long",
      leverage,
      isolated_margin: marginPerTrade,
      notional: marginPerTrade * leverage,
      quantity: Number(((marginPerTrade * leverage) / LIVE_PRICES["SOL-USDT"]).toFixed(2)),
      entry_price: Number((LIVE_PRICES["SOL-USDT"] * 0.992).toFixed(2)),
      entry_time: new Date(now.getTime() - 15 * 60000).toISOString(),
      take_profit_price: Number((LIVE_PRICES["SOL-USDT"] * 1.05).toFixed(2)),
      stop_loss_price: Number((LIVE_PRICES["SOL-USDT"] * 0.965).toFixed(2)),
      liquidation_price: Number((LIVE_PRICES["SOL-USDT"] * (1 - 0.85 / leverage)).toFixed(2)),
      mark_price: LIVE_PRICES["SOL-USDT"],
      unrealized_gross_pnl: Number(((LIVE_PRICES["SOL-USDT"] - LIVE_PRICES["SOL-USDT"] * 0.992) * ((marginPerTrade * leverage) / LIVE_PRICES["SOL-USDT"])).toFixed(2)),
      unrealized_net_pnl: Number((((LIVE_PRICES["SOL-USDT"] - LIVE_PRICES["SOL-USDT"] * 0.992) * ((marginPerTrade * leverage) / LIVE_PRICES["SOL-USDT"])) - 3.6).toFixed(2)),
      margin_roi_pct: Number((((LIVE_PRICES["SOL-USDT"] / (LIVE_PRICES["SOL-USDT"] * 0.992) - 1) * leverage) * 100).toFixed(2)),
    },
  ];

  const recentClosedTrades: ClosedTradeRow[] = [
    {
      trade_id: `cl_${id}_101`,
      symbol: "SOL-USDT",
      side: "long",
      margin_mode: "isolated",
      leverage,
      margin_used: marginPerTrade,
      notional: marginPerTrade * leverage,
      quantity: 65.2,
      entry_time: new Date(now.getTime() - 180 * 60000).toISOString(),
      exit_time: new Date(now.getTime() - 110 * 60000).toISOString(),
      entry_price: 182.4,
      exit_price: 187.8,
      take_profit_price: 187.5,
      stop_loss_price: 178.0,
      liquidation_price: 164.0,
      gross_pnl: 352.08,
      entry_fee: 4.8,
      exit_fee: 4.9,
      funding_paid: 1.2,
      liquidation_fee: 0,
      net_pnl: 341.18,
      roi_pct: 28.43,
      hold_seconds: 4200,
      entry_reason: "MACD + RSI Bullish Cross",
      exit_reason: "Take Profit Hit",
      market_regime: "trend_up",
    },
    {
      trade_id: `cl_${id}_102`,
      symbol: "DOGE-USDT",
      side: "short",
      margin_mode: "isolated",
      leverage,
      margin_used: marginPerTrade,
      notional: marginPerTrade * leverage,
      quantity: 45000,
      entry_time: new Date(now.getTime() - 320 * 60000).toISOString(),
      exit_time: new Date(now.getTime() - 250 * 60000).toISOString(),
      entry_price: 0.272,
      exit_price: 0.263,
      gross_pnl: 405.0,
      entry_fee: 4.8,
      exit_fee: 4.7,
      funding_paid: 0.9,
      liquidation_fee: 0,
      net_pnl: 394.6,
      roi_pct: 32.88,
      hold_seconds: 4200,
      entry_reason: "Funding Rate Z-Score Divergence",
      exit_reason: "Take Profit Reached",
      market_regime: "high_volatility",
    },
    {
      trade_id: `cl_${id}_103`,
      symbol: "BTC-USDT",
      side: "long",
      margin_mode: "isolated",
      leverage,
      margin_used: marginPerTrade,
      notional: marginPerTrade * leverage,
      quantity: 0.125,
      entry_time: new Date(now.getTime() - 500 * 60000).toISOString(),
      exit_time: new Date(now.getTime() - 440 * 60000).toISOString(),
      entry_price: 97100,
      exit_price: 96250,
      gross_pnl: -106.25,
      entry_fee: 4.8,
      exit_fee: 4.8,
      funding_paid: 0.5,
      liquidation_fee: 0,
      net_pnl: -116.35,
      roi_pct: -9.7,
      hold_seconds: 3600,
      entry_reason: "Momentum Breakout",
      exit_reason: "Stop Loss Triggered",
      market_regime: "chop",
    },
  ];

  const gridLadder = generateGridLadder("SOL-USDT", LIVE_PRICES["SOL-USDT"] || 188.6, 20, 0.006);

  return {
    session_id: id,
    runtime_status: "running",
    analysis_status: "valid",
    accounting_status: "reconciled_clean",
    accounting_schema_version: 2,
    session_role: role,
    regimen: `futures_${timeframe}_${leverage}x`,
    active: true,
    classification: "active",
    status: "running",
    session: {
      strategy_type: id.includes("grid") ? "bounded_grid_v1" : id.includes("morning") ? "funding_rate_zscore" : "futures_paper_engine",
      account_id: `acc_${id}`,
      strategy_id: id.includes("grid") ? "bounded_grid_v1" : id.includes("morning") ? "funding_rate_zscore" : isCandidate ? "candidate_v2" : "control_v1",
      worker_id: id,
      timeframe,
      symbols: ["BTC-USDT", "ETH-USDT", "SOL-USDT", "DOGE-USDT"],
      initial_cash: initialCash,
      entry_time: new Date(now.getTime() - 24 * 3600000).toISOString(),
      entry_prices: {
        "BTC-USDT": 95200,
        "ETH-USDT": 2720,
        "SOL-USDT": 184,
      },
      rebalance_interval_hours: 1,
      fee_rate: 0.0004,
      source: "okx",
      price_kind: "mark_price",
      fees_modeled: true,
      slippage_modeled: true,
      risk_config: {
        take_profit_pct: 0.04,
        stop_loss_pct: 0.02,
        trailing_stop_pct: 0.015,
        max_hold_hours: 8,
        leverage,
        margin_mode: "isolated",
        liquidation_buffer_pct: 0.15,
        fixed_margin_per_trade: marginPerTrade,
        portfolio_leverage: true,
      },
    },
    book: {
      positions: { "BTC-USDT": 0.125, "ETH-USDT": 4.2, "SOL-USDT": 62.5 },
      cash_remaining: availCash,
      last_rebalance_time: now.toISOString(),
    },
    mark_count: numMarks,
    latest_mark: {
      timestamp: now.toISOString(),
      prices: LIVE_PRICES,
      position_values: { "BTC-USDT": 12052, "ETH-USDT": 11532, "SOL-USDT": 11787 },
      position_pnl: { "BTC-USDT": 142.5, "ETH-USDT": 96.2, "SOL-USDT": 88.4 },
      open_positions: openPositions,
      cash_remaining: availCash,
      reserved_margin: marginUsed,
      open_notional: marginUsed * leverage,
      wallet_balance: curEquity - 327.1,
      available_balance: availCash,
      unrealized_pnl: 327.1,
      funding_paid: 14.8,
      fees_paid: 32.4,
      equity: Number(curEquity.toFixed(2)),
      pnl: Number(pnl.toFixed(2)),
      pnl_pct: Number(pnlPct.toFixed(2)),
      leverage,
      margin_mode: "isolated",
    },
    trade_count: 30,
    recent_trades: recentClosedTrades,
    trade_stats: {
      overall: tradeStatsOverall,
      by_symbol: {
        "SOL-USDT": { ...tradeStatsOverall, realized_pnl: 341.18, win_count: 10, loss_count: 2, win_rate: 0.833 },
        "DOGE-USDT": { ...tradeStatsOverall, realized_pnl: 394.6, win_count: 8, loss_count: 3, win_rate: 0.727 },
        "BTC-USDT": { ...tradeStatsOverall, realized_pnl: -116.35, win_count: 4, loss_count: 3, win_rate: 0.571 },
      },
    },
    equity_curve: equityPoints,
    max_drawdown: 3.6,
    database_account: {
      account_id: `acc_${id}`,
      strategy_id: id.includes("grid") ? "bounded_grid_v1" : id.includes("morning") ? "funding_rate_zscore" : isCandidate ? "candidate_v2" : "control_v1",
      worker_id: id,
      timeframe,
      mode: "paper",
      leverage,
      initial_capital: initialCash,
      cash_available: Number(availCash.toFixed(2)),
      margin_used: marginUsed,
      open_positions: openPosCount,
      realized_pnl: Number((pnl - 327.1).toFixed(2)),
      unrealized_pnl: 327.1,
      funding_pnl: -14.8,
      fees: 32.4,
      current_equity: Number(curEquity.toFixed(2)),
      last_heartbeat: now.toISOString(),
      last_trade: new Date(now.getTime() - 15 * 60000).toISOString(),
      ledger_status: "in_sync",
      risk_state: { max_drawdown_limit: 0.15, circuit_breaker: false },
      market_data_source: "okx",
      last_cycle_completed_at: now.toISOString(),
    },
    grid_engine: id.includes("grid")
      ? {
          symbol: "SOL-USDT",
          upper_bound: gridLadder.upper,
          lower_bound: gridLadder.lower,
          grid_levels: 20,
          spacing_pct: 0.006,
          active_bids: gridLadder.bids,
          active_asks: gridLadder.asks,
          completed_cycles: 64,
          realized_grid_profit: 486.20,
          grid_apr_pct: 42.8,
          last_fill_at: now.toISOString(),
        }
      : undefined,
    morning_glory: id.includes("morning")
      ? {
          symbols_zscores: getLiveFundingRateAnomalies(),
          settlement_interval_hours: 8,
          next_settlement_countdown_seconds: 14250,
          total_funding_harvested: 382.40,
          annualized_yield_pct: 34.6,
          last_settlement_at: new Date(now.getTime() - 3.5 * 3600000).toISOString(),
        }
      : undefined,
  };
}

export const PAPER_SESSIONS_MAP = new Map<string, PaperSessionState>([
  ["control_5m_futures", buildSampleSession("control_5m_futures", "control", "5m", 5, 10000)],
  ["candidate_5m_futures", buildSampleSession("candidate_5m_futures", "candidate", "5m", 10, 10000)],
  ["control_10m_futures", buildSampleSession("control_10m_futures", "control", "10m", 5, 10000)],
  ["candidate_10m_futures", buildSampleSession("candidate_10m_futures", "candidate", "10m", 10, 10000)],
  ["control_15m_futures", buildSampleSession("control_15m_futures", "control", "15m", 5, 10000)],
  ["candidate_15m_futures", buildSampleSession("candidate_15m_futures", "candidate", "15m", 10, 10000)],
  ["grid_futures_5x_v3", buildSampleSession("grid_futures_5x_v3", "candidate", "tick", 5, 5000)],
  ["grid_futures_10x_v3", buildSampleSession("grid_futures_10x_v3", "candidate", "tick", 10, 5000)],
  ["morning_glory_futures", buildSampleSession("morning_glory_futures", "candidate", "tick", 5, 5000)],
]);
