//! 15m vs 5m arbitrage for BTC, ETH, SOL, XRP. Trade only during the last 5 minutes of each 15m market (overlap with 5m).
//! Per-symbol price-to-beat tolerance; all symbols' price feeds and arb loops run in parallel via WebSocket.

use crate::api::PolymarketApi;
use crate::chainlink::run_chainlink_multi_poller;
use crate::config::Config;
use crate::discovery::{
    current_15m_period_start, current_5m_period_start, is_last_5min_of_15m, MarketDiscovery,
};
use crate::models::{PartialPosition, TradeRecord};
use crate::rtds::PriceCacheMulti;
use crate::ws::{run_market_ws, PricesSnapshot};
use anyhow::Result;
use chrono::Utc;
use log::{error, info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration};

const MARKET_15M_DURATION_SECS: i64 = 15 * 60;
const RESOLUTION_INITIAL_DELAY_SECS: u64 = 60;
const LIVE_PRICE_POLL_MS: u64 = 10;
const OVERLAP_POLL_SECS: u64 = 5;
const WAIT_FOR_PRICE_POLL_SECS: u64 = 10;
const NO_ARB_STATUS_LOG_SECS: u64 = 30;

pub struct ArbStrategy {
    api: Arc<PolymarketApi>,
    config: Config,
    discovery: MarketDiscovery,
    price_cache_15: PriceCacheMulti,
    price_cache_5: PriceCacheMulti,
}

impl ArbStrategy {
    pub fn new(api: Arc<PolymarketApi>, config: Config) -> Self {
        Self {
            discovery: MarketDiscovery::new(api.clone()),
            api,
            config,
            price_cache_15: Arc::new(RwLock::new(HashMap::new())),
            price_cache_5: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Wait until we're in the last 5 minutes of the 15m market and have both markets + both price-to-beats for this symbol.
    /// Fetches 15m and 5m markets in parallel.
    async fn wait_for_overlap_and_prices(
        &self,
        symbol: &str,
    ) -> Result<(
        String,
        String,
        String,
        String,
        String,
        String,
        i64,
        i64,
        f64,
        f64,
    )> {
        loop {
            let now = Utc::now().timestamp();
            let period_15 = current_15m_period_start();
            let period_5 = current_5m_period_start();

            if !is_last_5min_of_15m(now, period_15) {
                sleep(Duration::from_secs(OVERLAP_POLL_SECS)).await;
                continue;
            }

            let (cid_15, cid_5) = {
                let m15 = self.discovery.get_15m_market(symbol, period_15);
                let m5 = self.discovery.get_5m_market(symbol, period_5);
                let (r15, r5) = tokio::try_join!(m15, m5)?;
                let cid_15 = match r15 {
                    Some((cid, _)) => cid,
                    None => {
                        warn!("15m {} market not found for period {}. Retrying.", symbol, period_15);
                        sleep(Duration::from_secs(OVERLAP_POLL_SECS)).await;
                        continue;
                    }
                };
                let cid_5 = match r5 {
                    Some((cid, _)) => cid,
                    None => {
                        warn!("5m {} market not found for period {}. Retrying.", symbol, period_5);
                        sleep(Duration::from_secs(OVERLAP_POLL_SECS)).await;
                        continue;
                    }
                };
                (cid_15, cid_5)
            };

            let (price_15, price_5) = {
                let c15 = self.price_cache_15.read().await;
                let c5 = self.price_cache_5.read().await;
                let p15 = c15.get(symbol).and_then(|m| m.get(&period_15).copied());
                let p5 = c5.get(symbol).and_then(|m| m.get(&period_5).copied());
                (p15, p5)
            };

            let (price_15, price_5) = match (price_15, price_5) {
                (Some(a), Some(b)) => (a, b),
                _ => {
                    info!(
                        "{}: waiting for price-to-beat 15m={:?}, 5m={:?}",
                        symbol.to_uppercase(), price_15, price_5
                    );
                    sleep(Duration::from_secs(WAIT_FOR_PRICE_POLL_SECS)).await;
                    continue;
                }
            };

            let tolerance = self.config.strategy.price_to_beat_tolerance_for(symbol);
            if (price_15 - price_5).abs() > tolerance {
                info!(
                    "{}: |15m - 5m| price-to-beat = {:.6} > tolerance {:.6} USD; skipping.",
                    symbol.to_uppercase(), (price_15 - price_5).abs(), tolerance
                );
                sleep(Duration::from_secs(OVERLAP_POLL_SECS)).await;
                continue;
            }

            let (t15_up, t15_down, t5_up, t5_down) = {
                let tok15 = self.discovery.get_market_tokens(&cid_15);
                let tok5 = self.discovery.get_market_tokens(&cid_5);
                let ((t15_up, t15_down), (t5_up, t5_down)) = tokio::try_join!(tok15, tok5)?;
                (t15_up, t15_down, t5_up, t5_down)
            };

            info!(
                "{} overlap active: 15m period {} (P2B {:.4}), 5m period {} (P2B {:.4}), tolerance {:.6}",
                symbol.to_uppercase(), period_15, price_15, period_5, price_5, tolerance
            );
            return Ok((
                cid_15, cid_5, t15_up, t15_down, t5_up, t5_down,
                period_15, period_5, price_15, price_5,
            ));
        }
    }

    /// Run one overlap window for one symbol: subscribe to its four tokens via WebSocket; when sum of asks < threshold, place both legs.
    /// Returns list of trades placed (for resolution, PnL, redeem).
    #[allow(clippy::too_many_arguments)]
    async fn run_overlap_round(
        &self,
        symbol: &str,
        cid_15: &str,
        cid_5: &str,
        t15_up: &str,
        t15_down: &str,
        t5_up: &str,
        t5_down: &str,
        period_15: i64,
        period_5: i64,
    ) -> Result<(Vec<TradeRecord>, Vec<PartialPosition>)> {
        let prices: PricesSnapshot = Arc::new(RwLock::new(HashMap::new()));
        let asset_ids = vec![
            t15_up.to_string(),
            t15_down.to_string(),
            t5_up.to_string(),
            t5_down.to_string(),
        ];
        let ws_url = self.config.polymarket.ws_url.clone();
        let prices_clone = Arc::clone(&prices);
        let symbol_ws = symbol.to_string();
        let ws_handle = tokio::spawn(async move {
            if let Err(e) = run_market_ws(&ws_url, asset_ids, prices_clone).await {
                warn!("{} overlap WebSocket exited: {}", symbol_ws.to_uppercase(), e);
            }
        });

        let threshold = self.config.strategy.sum_threshold;
        let shares = self.config.strategy.arb_shares.clone();
        let interval_secs = self.config.strategy.trade_interval_secs;
        let simulation = self.config.strategy.simulation_mode;
        let sym_upper = symbol.to_uppercase();

        let mut last_trade_at: Option<std::time::Instant> = None;
        let mut last_status_log: Option<std::time::Instant> = None;
        let mut trades: Vec<TradeRecord> = Vec::new();
        let mut partials: Vec<PartialPosition> = Vec::new();

        while Utc::now().timestamp() < period_15 + MARKET_15M_DURATION_SECS {
            let snap = prices.read().await;
            let ask_15_up = snap.get(t15_up).and_then(|p| p.ask);
            let ask_15_down = snap.get(t15_down).and_then(|p| p.ask);
            let ask_5_up = snap.get(t5_up).and_then(|p| p.ask);
            let ask_5_down = snap.get(t5_down).and_then(|p| p.ask);
            drop(snap);

            if let Some(t) = last_trade_at {
                if t.elapsed().as_secs() < interval_secs {
                    sleep(Duration::from_millis(LIVE_PRICE_POLL_MS)).await;
                    continue;
                }
            }

            let sum_up_down = match (ask_15_up, ask_5_down) {
                (Some(a), Some(b)) => Some(a + b),
                _ => None,
            };
            let sum_down_up = match (ask_15_down, ask_5_up) {
                (Some(a), Some(b)) => Some(a + b),
                _ => None,
            };

            let (leg1_token, leg1_price, leg2_token, leg2_price, leg1_cid, leg1_outcome, leg2_cid, leg2_outcome) =
                if sum_up_down.map(|s| s < threshold).unwrap_or(false) {
                    (
                        t15_up, ask_15_up.unwrap(), t5_down, ask_5_down.unwrap(),
                        cid_15, "Up", cid_5, "Down",
                    )
                } else if sum_down_up.map(|s| s < threshold).unwrap_or(false) {
                    (
                        t15_down, ask_15_down.unwrap(), t5_up, ask_5_up.unwrap(),
                        cid_15, "Down", cid_5, "Up",
                    )
                } else {
                    let should_log = last_status_log
                        .map(|t| t.elapsed().as_secs() >= NO_ARB_STATUS_LOG_SECS)
                        .unwrap_or(true);
                    if should_log {
                        let sum_ud = sum_up_down.map(|s| format!("{:.4}", s)).unwrap_or_else(|| "?".to_string());
                        let sum_du = sum_down_up.map(|s| format!("{:.4}", s)).unwrap_or_else(|| "?".to_string());
                        let a15u = ask_15_up.map(|a| format!("{:.4}", a)).unwrap_or_else(|| "?".to_string());
                        let a15d = ask_15_down.map(|a| format!("{:.4}", a)).unwrap_or_else(|| "?".to_string());
                        let a5u = ask_5_up.map(|a| format!("{:.4}", a)).unwrap_or_else(|| "?".to_string());
                        let a5d = ask_5_down.map(|a| format!("{:.4}", a)).unwrap_or_else(|| "?".to_string());
                        info!(
                            "{} overlap: no arb yet | 15m Up={} Down={} 5m Up={} Down={} | sum(Up+Down)={} sum(Down+Up)={} threshold={}",
                            sym_upper, a15u, a15d, a5u, a5d, sum_ud, sum_du, threshold
                        );
                        last_status_log = Some(std::time::Instant::now());
                    }
                    sleep(Duration::from_millis(LIVE_PRICE_POLL_MS)).await;
                    continue;
                };

            if simulation {
                info!(
                    "[SIM] {} arb would place: 15m {} @ {:.4} + 5m {} @ {:.4} (sum {:.4} < {})",
                    sym_upper, leg1_outcome, leg1_price, leg2_outcome, leg2_price, leg1_price + leg2_price, threshold
                );
                last_trade_at = Some(std::time::Instant::now());
                last_status_log = None;
                let size_f64: f64 = shares.parse().unwrap_or(0.0);
                trades.push(TradeRecord {
                    symbol: symbol.to_string(),
                    period_15,
                    period_5,
                    cid_15: cid_15.to_string(),
                    cid_5: cid_5.to_string(),
                    leg1_token: leg1_token.to_string(),
                    leg1_price,
                    leg1_cid: leg1_cid.to_string(),
                    leg1_outcome: leg1_outcome.to_string(),
                    leg2_token: leg2_token.to_string(),
                    leg2_price,
                    leg2_cid: leg2_cid.to_string(),
                    leg2_outcome: leg2_outcome.to_string(),
                    size: size_f64,
                });
                sleep(Duration::from_millis(LIVE_PRICE_POLL_MS)).await;
                continue;
            }

            let size_f64: f64 = shares.parse().unwrap_or(0.0);

            // Place both legs in parallel for faster execution when arb opportunity appears
            let (r1, r2) = tokio::join!(
                self.api.place_market_order(leg1_token, size_f64, "BUY", Some("FAK")),
                self.api.place_market_order(leg2_token, size_f64, "BUY", Some("FAK")),
            );

            match (&r1, &r2) {
                (Ok(res1), Ok(res2)) => {
                    let id1 = res1.order_id.as_deref().unwrap_or("");
                    let id2 = res2.order_id.as_deref().unwrap_or("");
                    info!(
                        "{} arb placed: 15m {} @ {:.4} ({}), 5m {} @ {:.4} ({}), next in {}s",
                        sym_upper, leg1_outcome, leg1_price, id1, leg2_outcome, leg2_price, id2, interval_secs
                    );
                    last_trade_at = Some(std::time::Instant::now());
                    last_status_log = None;
                    let size_f64: f64 = shares.parse().unwrap_or(0.0);
                    trades.push(TradeRecord {
                        symbol: symbol.to_string(),
                        period_15,
                        period_5,
                        cid_15: cid_15.to_string(),
                        cid_5: cid_5.to_string(),
                        leg1_token: leg1_token.to_string(),
                        leg1_price,
                        leg1_cid: leg1_cid.to_string(),
                        leg1_outcome: leg1_outcome.to_string(),
                        leg2_token: leg2_token.to_string(),
                        leg2_price,
                        leg2_cid: leg2_cid.to_string(),
                        leg2_outcome: leg2_outcome.to_string(),
                        size: size_f64,
                    });
                }
                (Ok(_), Err(e)) => {
                    warn!("{} arb leg2 place failed (leg1 filled): {}", sym_upper, e);
                    let size_f64: f64 = shares.parse().unwrap_or(0.0);
                    partials.push(PartialPosition {
                        symbol: symbol.to_string(),
                        cid: cid_15.to_string(),
                        outcome: leg1_outcome.to_string(),
                        size: size_f64,
                    });
                }
                (Err(e), Ok(_)) => {
                    warn!("{} arb leg1 place failed (leg2 filled): {}", sym_upper, e);
                    let size_f64: f64 = shares.parse().unwrap_or(0.0);
                    partials.push(PartialPosition {
                        symbol: symbol.to_string(),
                        cid: cid_5.to_string(),
                        outcome: leg2_outcome.to_string(),
                        size: size_f64,
                    });
                }
                (Err(e), Err(_)) => {
                    warn!("{} arb leg1 place failed: {}", sym_upper, e);
                }
            }

            sleep(Duration::from_millis(LIVE_PRICE_POLL_MS)).await;
        }

        ws_handle.abort();
        info!(
            "{} overlap window ended (period {}), {} complete arb(s), {} partial(s).",
            sym_upper, period_15, trades.len(), partials.len()
        );
        Ok((trades, partials))
    }

    /// Poll until markets are closed/resolved, compute PnL, redeem winning tokens. Updates cumulative_pnl.
    /// Also redeems partial positions and sweeps all redeemable positions via API.
    async fn resolve_and_redeem(
        api: Arc<PolymarketApi>,
        config: &Config,
        cid_15: &str,
        cid_5: &str,
        trades: Vec<TradeRecord>,
        partials: Vec<PartialPosition>,
        cumulative_pnl: Arc<RwLock<f64>>,
    ) -> Result<()> {
        if trades.is_empty() && partials.is_empty() {
            return Ok(());
        }
        let poll_interval = config.strategy.resolution_poll_interval_secs;
        let auto_redeem = config.strategy.auto_redeem;
        let proxy = config.polymarket.proxy_wallet_address.as_deref();
        info!("Resolution: waiting {}s, then polling every {}s until markets close (no timeout).", RESOLUTION_INITIAL_DELAY_SECS, poll_interval);
        sleep(Duration::from_secs(RESOLUTION_INITIAL_DELAY_SECS)).await;

        let started = std::time::Instant::now();
        let mut period_pnl = 0.0f64;
        let mut m15_resolved = None;
        let mut m5_resolved = None;

        let sym = trades
            .first()
            .map(|t| t.symbol.to_uppercase())
            .or_else(|| partials.first().map(|p| p.symbol.to_uppercase()))
            .unwrap_or_else(|| "?".to_string());
        let mut poll_count = 0u32;

        loop {
            poll_count += 1;
            let m15 = api.get_market(cid_15).await.ok();
            let m5 = api.get_market(cid_5).await.ok();
            let (closed_15, winner_15) = m15.as_ref().map(|m| (m.closed, m.tokens.iter().find(|t| t.winner).map(|t| (t.token_id.as_str(), t.outcome.as_str())))).unwrap_or((false, None));
            let (closed_5, winner_5) = m5.as_ref().map(|m| (m.closed, m.tokens.iter().find(|t| t.winner).map(|t| (t.token_id.as_str(), t.outcome.as_str())))).unwrap_or((false, None));
            let elapsed = started.elapsed().as_secs();
            let w15 = winner_15.map(|(_, o)| o).unwrap_or("?");
            let w5 = winner_5.map(|(_, o)| o).unwrap_or("?");
            info!(
                "{} resolution poll #{} ({}s): 15m closed={} winner={} 5m closed={} winner={}",
                sym, poll_count, elapsed, closed_15, w15, closed_5, w5
            );

            if closed_15 && closed_5 && winner_15.is_some() && winner_5.is_some() {
                m15_resolved = m15;
                m5_resolved = m5;
                info!("{} markets resolved. Computing PnL and redeeming.", sym);
                break;
            }
            sleep(Duration::from_secs(poll_interval)).await;
        }

        let (winner_15, winner_5) = match (m15_resolved.as_ref(), m5_resolved.as_ref()) {
            (Some(m15), Some(m5)) => (
                m15.tokens.iter().find(|t| t.winner).map(|t| (t.token_id.as_str(), t.outcome.as_str())),
                m5.tokens.iter().find(|t| t.winner).map(|t| (t.token_id.as_str(), t.outcome.as_str())),
            ),
            _ => unreachable!("loop breaks only when both resolved"),
        };

        let (win_token_15, win_token_5, outcome_15, outcome_5) = match (winner_15, winner_5) {
            (Some((t15, o15)), Some((t5, o5))) => (t15, t5, o15, o5),
            _ => return Ok(()),
        };

        let period_15 = trades.first().map(|t| t.period_15).unwrap_or(0_i64);

        let mut total_deposit = 0.0;
        let mut total_payout = 0.0;
        let mut wins_both = 0usize;
        let mut wins_one = 0usize;
        let mut losses = 0usize;

        for trade in &trades {
            let cost = (trade.leg1_price + trade.leg2_price) * trade.size;
            total_deposit += cost;
            // We bought 15m one + 5m opposite. Check which legs won ($1 each).
            let we_won_15m = win_token_15 == trade.leg1_token || win_token_15 == trade.leg2_token;
            let we_won_5m = win_token_5 == trade.leg1_token || win_token_5 == trade.leg2_token;
            let payout = trade.size * ((we_won_15m as i32 + we_won_5m as i32) as f64);
            total_payout += payout;
            let pnl = payout - cost;
            period_pnl += pnl;

            match (we_won_15m, we_won_5m) {
                (true, true) => wins_both += 1,
                (true, false) | (false, true) => wins_one += 1,
                (false, false) => losses += 1,
            }

            let result_msg = match (we_won_15m, we_won_5m) {
                (true, true) => "Won both legs",
                (true, false) => "Won 15m leg",
                (false, true) => "Won 5m leg",
                (false, false) => "Lost both legs",
            };
            info!(
                "  {} #{}: {} | cost={:.2}, payout={:.2}, PnL={:.2}",
                sym, wins_both + wins_one + losses, result_msg, cost, payout, pnl
            );

            // Redeem winning tokens (one or both legs). Retry until success.
            if auto_redeem && proxy.is_some() && !config.strategy.simulation_mode {
                if we_won_15m {
                    let out = if win_token_15 == trade.leg1_token { &trade.leg1_outcome } else { &trade.leg2_outcome };
                    loop {
                        match api.redeem_tokens(trade.cid_15.as_str(), "", out).await {
                            Ok(_) => {
                                info!("    Redeemed 15m {} tokens", out);
                                break;
                            }
                            Err(e) => {
                                warn!("  Redeem 15m {} failed: {}. Retrying in 30s.", out, e);
                                sleep(Duration::from_secs(30)).await;
                            }
                        }
                    }
                }
                if we_won_5m {
                    let out = if win_token_5 == trade.leg1_token { &trade.leg1_outcome } else { &trade.leg2_outcome };
                    loop {
                        match api.redeem_tokens(trade.cid_5.as_str(), "", out).await {
                            Ok(_) => {
                                info!("    Redeemed 5m {} tokens", out);
                                break;
                            }
                            Err(e) => {
                                warn!("  Redeem 5m {} failed: {}. Retrying in 30s.", out, e);
                                sleep(Duration::from_secs(30)).await;
                            }
                        }
                    }
                }
            }
        }

        // Redeem partial positions (single leg that won)
        for p in &partials {
            let won = (p.cid == *cid_15 && p.outcome == outcome_15) || (p.cid == *cid_5 && p.outcome == outcome_5);
            if won && auto_redeem && proxy.is_some() && !config.strategy.simulation_mode {
                loop {
                    match api.redeem_tokens(&p.cid, "", &p.outcome).await {
                        Ok(_) => {
                            info!("    Redeemed partial {} {} tokens ({} shares)", p.symbol.to_uppercase(), p.outcome, p.size);
                            break;
                        }
                        Err(e) => {
                            warn!("  Redeem partial {} failed: {}. Retrying in 30s.", p.outcome, e);
                            sleep(Duration::from_secs(30)).await;
                        }
                    }
                }
            }
        }

        // Sweep: redeem ALL remaining redeemable positions (catches any we missed)
        if auto_redeem && proxy.is_some() && !config.strategy.simulation_mode {
            if let Some(proxy_addr) = proxy {
                if let Ok(condition_ids) = api.get_redeemable_positions(proxy_addr).await {
                    if !condition_ids.is_empty() {
                        info!("Sweep: {} redeemable condition(s) remaining. Redeeming all.", condition_ids.len());
                        for cid in &condition_ids {
                            let m = match api.get_market(cid).await {
                                Ok(m) => m,
                                Err(e) => {
                                    warn!("Sweep: could not get market {}: {}", &cid[..cid.len().min(18)], e);
                                    continue;
                                }
                            };
                            let winner = m.tokens.iter().find(|t| t.winner).map(|t| t.outcome.as_str());
                            if let Some(outcome) = winner {
                                loop {
                                    match api.redeem_tokens(cid, "", outcome).await {
                                        Ok(_) => {
                                            info!("    Sweep redeemed condition {} (winner {})", &cid[..cid.len().min(18)], outcome);
                                            break;
                                        }
                                        Err(e) => {
                                            warn!("Sweep redeem failed: {}. Retrying in 30s.", e);
                                            sleep(Duration::from_secs(30)).await;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let mut cum = cumulative_pnl.write().await;
        *cum += period_pnl;
        info!(
            "{} PERIOD {} SUMMARY: Market Won 15m {} 5m {} | deposit={:.2}, payout={:.2} | trades={} ({} both, {} one, {} lost), partials={} | period PnL={:.2} | cumulative PnL={:.2}",
            sym, period_15, outcome_15, outcome_5, total_deposit, total_payout,
            trades.len(), wins_both, wins_one, losses, partials.len(), period_pnl, *cum
        );
        Ok(())
    }

    /// Per-symbol loop: wait for overlap + prices, run round, resolve/redeem/PnL, repeat.
    async fn run_symbol_loop(
        api: Arc<PolymarketApi>,
        config: Config,
        price_cache_15: PriceCacheMulti,
        price_cache_5: PriceCacheMulti,
        cumulative_pnl: Arc<RwLock<f64>>,
        symbol: String,
    ) -> Result<()> {
        let discovery = MarketDiscovery::new(api.clone());
        let strategy = Self {
            api: api.clone(),
            config: config.clone(),
            discovery,
            price_cache_15,
            price_cache_5,
        };
        loop {
            let (cid_15, cid_5, t15_up, t15_down, t5_up, t5_down, period_15, period_5, _p15, _p5) =
                strategy.wait_for_overlap_and_prices(&symbol).await?;
            match strategy
                .run_overlap_round(
                    &symbol,
                    &cid_15, &cid_5,
                    &t15_up, &t15_down, &t5_up, &t5_down,
                    period_15,
                    period_5,
                )
                .await
            {
                Ok((trades, partials)) => {
                    if !trades.is_empty() || !partials.is_empty() {
                        if let Err(e) = Self::resolve_and_redeem(
                            api.clone(),
                            &config,
                            &cid_15,
                            &cid_5,
                            trades,
                            partials,
                            cumulative_pnl.clone(),
                        )
                        .await
                        {
                            error!("{} resolve/redeem error: {}", symbol.to_uppercase(), e);
                        }
                    }
                }
                Err(e) => {
                    error!("{} overlap round error: {}", symbol.to_uppercase(), e);
                }
            }
            sleep(Duration::from_secs(5)).await;
        }
    }

    pub async fn run(&self) -> Result<()> {
        let symbols = &self.config.strategy.symbols;
        info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        info!("15m vs 5m arbitrage (symbols: {:?}) — overlap window, parallel WS", symbols);
        info!("   Price-to-beat: RTDS Chainlink (all symbols in one WS); per-symbol tolerance");
        info!("   Place both legs when sum of asks < {}; next arb after {}s cooldown.", self.config.strategy.sum_threshold, self.config.strategy.trade_interval_secs);
        info!("   Post-arb: poll resolution every {}s, auto_redeem={}", self.config.strategy.resolution_poll_interval_secs, self.config.strategy.auto_redeem);
        info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        let cumulative_pnl: Arc<RwLock<f64>> = Arc::new(RwLock::new(0.0));
        let rtds_url = self.config.polymarket.rtds_ws_url.clone();
        let cache_15 = Arc::clone(&self.price_cache_15);
        let cache_5 = Arc::clone(&self.price_cache_5);
        let symbols_rtds = symbols.clone();
        if let Err(e) = run_chainlink_multi_poller(rtds_url, symbols_rtds, cache_15, cache_5).await {
            warn!("RTDS Chainlink poller start: {}", e);
        }
        sleep(Duration::from_secs(2)).await;

        let mut handles = Vec::new();
        for symbol in symbols.clone() {
            let api = Arc::clone(&self.api);
            let config = self.config.clone();
            let price_cache_15 = Arc::clone(&self.price_cache_15);
            let price_cache_5 = Arc::clone(&self.price_cache_5);
            let cumulative_pnl = Arc::clone(&cumulative_pnl);
            handles.push(tokio::spawn(async move {
                if let Err(e) = Self::run_symbol_loop(api, config, price_cache_15, price_cache_5, cumulative_pnl, symbol.clone()).await {
                    error!("Symbol loop {} failed: {}", symbol, e);
                }
            }));
        }
        futures_util::future::try_join_all(handles).await?;
        Ok(())
    }
}
