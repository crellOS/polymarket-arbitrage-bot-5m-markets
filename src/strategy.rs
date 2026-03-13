//! Soccer match trading: buy high-priced team + Draw when sum < threshold;
//! monitor via WebSocket; risk management (sell team, buy opposite) or profit exit (sell both).

use crate::api::PolymarketApi;
use crate::config::Config;
use crate::models::Position;
use crate::soccer::{SoccerDiscovery, SoccerMatch, SoccerOutcome};
use crate::ws::{run_market_ws, PricesSnapshot};
use chrono::Utc;
use anyhow::Result;
use log::{error, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{RwLock, Semaphore};
use tokio::time::{sleep, Duration};

const LIVE_PRICE_POLL_MS: u64 = 100;
const LIVE_PRICE_LOG_INTERVAL_SECS: u64 = 1;
/// Wait before first position check. Polymarket Data API can take 15–30s to index new positions.
const POSITION_CHECK_DELAY_SECS: u64 = 10;
/// Wait before each retry position check. Do several retries before placing new orders.
const POSITION_RETRY_DELAY_SECS: u64 = 5;
const MIN_ORDER_USD: f64 = 1.0;

#[derive(Debug, Clone)]
pub struct HoldingPosition {
    pub match_slug: String,
    pub team_token_id: String,
    pub team_outcome: SoccerOutcome,
    pub draw_token_id: String,
    pub buy_price_team: f64,
    pub buy_price_draw: f64,
    pub size: f64,
}

pub struct SoccerStrategy {
    api: Arc<PolymarketApi>,
    config: Config,
    discovery: SoccerDiscovery,
}

impl SoccerStrategy {
    pub fn new(api: Arc<PolymarketApi>, config: Config) -> Self {
        Self {
            discovery: SoccerDiscovery::new(api.clone()),
            api,
            config,
        }
    }

    pub async fn run(&self) -> Result<()> {
        info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        info!("Soccer match trading bot");
        info!(
            "  Buy when (team + draw) < {} | Risk threshold: {}% | Profit exit: >= {}",
            self.config.strategy.buy_threshold,
            self.config.strategy.risk_reduce_threshold * 100.0,
            self.config.strategy.sell_profit_threshold
        );
        info!("  Poll: {}s | Cache refresh: {}s | Live window: {}m | Max concurrent: {} | Shares: {}",
            self.config.strategy.market_poll_interval_secs,
            self.config.strategy.market_refresh_interval_secs,
            self.config.strategy.live_window_minutes,
            self.config.strategy.max_concurrent_markets,
            self.config.strategy.shares);
        info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        // Log holding stats at startup (balance, allowance, positions)
        self.log_holding_stats().await;

        let last_trade_at: Arc<RwLock<Option<std::time::Instant>>> = Arc::new(RwLock::new(None));
        let mut cached_matches: Vec<SoccerMatch> = Vec::new();
        let cache_path = &self.config.strategy.market_cache_path;
        let refresh_secs = self.config.strategy.market_refresh_interval_secs;
        let live_window = self.config.strategy.live_window_minutes;
        let mut last_refresh = std::time::Instant::now(); // First iter: is_empty triggers fetch
        let max_concurrent = self.config.strategy.max_concurrent_markets as usize;
        let (semaphore, trading_ids) = if max_concurrent > 1 {
            (
                Arc::new(Semaphore::new(max_concurrent)),
                Arc::new(RwLock::new(HashSet::new())),
            )
        } else {
            (Arc::new(Semaphore::new(0)), Arc::new(RwLock::new(HashSet::new())))
        };

        loop {
            let should_refresh = last_refresh.elapsed().as_secs() >= refresh_secs || cached_matches.is_empty();
            if should_refresh {
                match self.discovery.fetch_sort_and_save(
                    &self.config.strategy.soccer_tag_ids,
                    self.config.strategy.market_fetch_limit,
                    cache_path,
                    live_window,
                ).await {
                    Ok(matches) => {
                        cached_matches = matches;
                        last_refresh = std::time::Instant::now();
                    }
                    Err(e) => {
                        warn!("Failed to fetch soccer matches: {}.", e);
                        if cached_matches.is_empty() {
                            if let Ok(matches) = SoccerDiscovery::load_cached_matches(cache_path) {
                                cached_matches = matches;
                                info!("Loaded {} matches from cache", cached_matches.len());
                            }
                        }
                    }
                }
            }

            // Periodic holding stats (same interval as market refresh)
            if should_refresh {
                self.log_holding_stats().await;
            }

            let tradable_matches = if cached_matches.is_empty() {
                Vec::new()
            } else {
                self.select_tradable_live_matches(
                    &cached_matches,
                    live_window,
                    max_concurrent,
                ).await
            };

            // Markets where we hold tokens (for monitoring/selling) - merge with tradable
            let (held_matches, held_positions_map) = self.select_matches_with_holdings(&cached_matches).await;

            let mut matches_to_run: Vec<SoccerMatch> = tradable_matches.clone();
            let mut seen = HashSet::new();
            for m in &tradable_matches {
                seen.insert(m.event_id.clone());
            }
            for held in &held_matches {
                if !seen.contains(&held.event_id) {
                    seen.insert(held.event_id.clone());
                    matches_to_run.push(held.clone());
                }
            }

            if !matches_to_run.is_empty() {
                if max_concurrent == 1 {
                    let live_match = matches_to_run.first().unwrap();
                    let initial = held_positions_map.get(&live_match.event_id).cloned();
                    info!("Live match: {} (game_start: {:?}){}", live_match.title, live_match.game_start,
                        if initial.is_some() { " [monitoring existing holdings]" } else { "" });
                    if let Err(e) = self.run_match_cycle(live_match.clone(), Arc::clone(&last_trade_at), initial).await {
                        error!("Match cycle error: {}", e);
                    }
                } else {
                    for live_match in &matches_to_run {
                        let ids = trading_ids.write().await;
                        if ids.contains(&live_match.event_id) {
                            continue;
                        }
                        drop(ids);

                        let permit = match semaphore.clone().try_acquire_owned() {
                            Ok(p) => p,
                            Err(_) => break,
                        };

                        let m_clone = live_match.clone();
                        let event_id = m_clone.event_id.clone();
                        let initial = held_positions_map.get(&event_id).cloned();
                        let api = self.api.clone();
                        let config = self.config.clone();
                        let last_ta = Arc::clone(&last_trade_at);
                        let trading_ids_clone = Arc::clone(&trading_ids);

                        trading_ids.write().await.insert(event_id.clone());

                        tokio::spawn(async move {
                            let _permit = permit;
                            if let Err(e) = Self::run_match_cycle_static(
                                api,
                                config,
                                m_clone,
                                last_ta,
                                initial,
                            ).await {
                                error!("Match cycle error: {}", e);
                            }
                            trading_ids_clone.write().await.remove(&event_id);
                        });

                        info!("Started trading: {} ({} active)", live_match.title, trading_ids.read().await.len());
                    }
                }
            } else {
                let live_skipped = self.discovery.select_live_matches(
                    &cached_matches,
                    live_window,
                    self.config.strategy.trading_start_minutes_before,
                    self.config.strategy.min_minutes_into_game,
                    10,
                );
                if !live_skipped.is_empty() {
                    info!("{} live match(es) in progress (skipped: sum >= buy_threshold {})", live_skipped.len(), self.config.strategy.buy_threshold);
                    for m in live_skipped.iter().take(5) {
                        let (ah, ad, aa) = tokio::join!(
                            self.api.get_best_ask(&m.home_market.yes_token_id),
                            self.api.get_best_ask(&m.draw_market.yes_token_id),
                            self.api.get_best_ask(&m.away_market.yes_token_id),
                        );
                        let (h, d, a) = (
                            ah.ok().flatten(),
                            ad.ok().flatten(),
                            aa.ok().flatten(),
                        );
                        let (h_str, d_str, a_str) = (
                            h.map(|v| format!("${:.2}", v)).unwrap_or_else(|| "N/A".into()),
                            d.map(|v| format!("${:.2}", v)).unwrap_or_else(|| "N/A".into()),
                            a.map(|v| format!("${:.2}", v)).unwrap_or_else(|| "N/A".into()),
                        );
                        let sum = match (h, d, a) {
                            (Some(hi), Some(di), Some(ai)) => Some((hi + di).max(ai + di)),
                            _ => None,
                        };
                        let sum_str = sum.map(|s| format!("{:.2}", s)).unwrap_or_else(|| "N/A".into());
                        let gs = m.game_start
                            .map(|dt| dt.format("%Y-%m-%d %H:%M UTC").to_string())
                            .unwrap_or_else(|| "?".into());
                        info!("  • {} @ {} | H {} D {} A {} | sum {}", m.title, gs, h_str, d_str, a_str, sum_str);
                    }
                }
                let upcoming = self.discovery.select_nearest_upcoming(&cached_matches, self.config.strategy.nearest_upcoming_count);
                if upcoming.is_empty() {
                    info!("No tradable matches. No upcoming in cache. Waiting {}s.", self.config.strategy.market_poll_interval_secs);
                } else {
                    let now = chrono::Utc::now();
                    info!("Current time: {} UTC", now.format("%Y-%m-%d %H:%M:%S"));
                    info!("Nearest upcoming ({}):", upcoming.len());
                    for u in upcoming {
                        let (gs, remaining) = u.game_start
                            .map(|dt| {
                                let r = (dt - now).to_std().unwrap_or_default();
                                let secs = r.as_secs();
                                let days = secs / 86400;
                                let hrs = (secs % 86400) / 3600;
                                let mins = (secs % 3600) / 60;
                                let rem = if days > 0 {
                                    format!("in {}d {}h {}m", days, hrs, mins)
                                } else if hrs > 0 {
                                    format!("in {}h {}m", hrs, mins)
                                } else {
                                    format!("in {}m", mins)
                                };
                                (dt.format("%Y-%m-%d %H:%M UTC").to_string(), rem)
                            })
                            .unwrap_or_else(|| ("?".into(), "?".into()));
                        info!("  • {} @ {} ({})", u.title, gs, remaining);
                    }
                    info!("Waiting {}s.", self.config.strategy.market_poll_interval_secs);
                }
            }

            sleep(Duration::from_secs(self.config.strategy.market_poll_interval_secs)).await;
        }
    }

    /// Log USDC balance, allowance, and position stats (for "not enough balance" diagnostics).
    async fn log_holding_stats(&self) {
        let wallet = match self.api.get_trading_wallet() {
            Ok(w) => w,
            Err(e) => {
                warn!("Could not get trading wallet for holding stats: {}", e);
                return;
            }
        };
        let (balance_res, positions_res) = tokio::join!(
            self.api.get_balance_allowance(&wallet),
            self.api.get_all_positions(&wallet),
        );
        let balance = balance_res.unwrap_or_default();
        let positions = positions_res.unwrap_or_default();
        let active: Vec<_> = positions.iter().filter(|p| p.size > 0.0 && !p.redeemable.unwrap_or(false)).collect();
        let total_val: f64 = active.iter()
            .map(|p| p.size * p.avg_price.unwrap_or(0.0))
            .sum();
        info!("📊 Holding stats | Wallet: {}..{} | USDC: ${:.2} | Allowance: ${:.2} | Positions: {} ({} active) | ~Value: ${:.2}",
            &wallet[..2.min(wallet.len())], &wallet[wallet.len().saturating_sub(6)..],
            balance.balance_usdc, balance.allowance_usdc,
            positions.len(), active.len(), total_val);
        for p in active.iter().take(5) {
            let title = p.title.as_deref().unwrap_or("?");
            let ev = p.event_id.as_deref().unwrap_or("?");
            info!("   • {} (event {}) | {} shares @ ${:.2}", title, ev, p.size, p.avg_price.unwrap_or(0.0));
        }
        if active.len() > 5 {
            info!("   ... and {} more", active.len() - 5);
        }
    }

    /// Select matches where we hold tokens (team+draw). Returns matches and reconstructed HoldingPosition per event.
    async fn select_matches_with_holdings(
        &self,
        cached_matches: &[SoccerMatch],
    ) -> (Vec<SoccerMatch>, HashMap<String, HoldingPosition>) {
        let wallet = match self.api.get_trading_wallet() {
            Ok(w) => w,
            Err(_) => return (Vec::new(), HashMap::new()),
        };
        let positions = match self.api.get_all_positions(&wallet).await {
            Ok(p) => p,
            Err(_) => return (Vec::new(), HashMap::new()),
        };
        let active: Vec<_> = positions
            .into_iter()
            .filter(|p| p.size > 0.0 && !p.redeemable.unwrap_or(false))
            .collect();
        let by_event: HashMap<String, Vec<Position>> = {
            let mut m = HashMap::new();
            for p in active {
                if let Some(eid) = p.event_id.as_ref() {
                    m.entry(eid.clone()).or_insert_with(Vec::new).push(p);
                }
            }
            m
        };
        let mut held_matches = Vec::new();
        let mut held_positions = HashMap::new();
        for m in cached_matches {
            let Some(positions) = by_event.get(&m.event_id) else { continue };
            let (draw_pos, team_pos) = {
                let mut draw = None;
                let mut team = None;
                for p in positions {
                    let slug = p.slug.as_deref().unwrap_or("");
                    if slug.to_lowercase().contains("draw") {
                        draw = Some(p);
                    } else {
                        team = Some(p);
                    }
                }
                (draw, team)
            };
            let (Some(draw_pos), Some(team_pos)) = (draw_pos, team_pos) else { continue };
            let team_outcome = if team_pos.token_id == m.home_market.yes_token_id {
                SoccerOutcome::HomeWin
            } else if team_pos.token_id == m.away_market.yes_token_id {
                SoccerOutcome::AwayWin
            } else {
                continue;
            };
            let holding = HoldingPosition {
                match_slug: m.slug.clone(),
                team_token_id: team_pos.token_id.clone(),
                team_outcome,
                draw_token_id: draw_pos.token_id.clone(),
                buy_price_team: team_pos.avg_price.unwrap_or(0.0),
                buy_price_draw: draw_pos.avg_price.unwrap_or(0.0),
                size: team_pos.size.min(draw_pos.size),
            };
            held_positions.insert(m.event_id.clone(), holding);
            held_matches.push(m.clone());
        }
        (held_matches, held_positions)
    }

    /// Select live matches where (higher team + draw) sum < buy_threshold, take up to max_count.
    async fn select_tradable_live_matches(
        &self,
        matches: &[SoccerMatch],
        live_window_minutes: i64,
        max_count: usize,
    ) -> Vec<SoccerMatch> {
        let live = self.discovery.select_live_matches(
            matches,
            live_window_minutes,
            self.config.strategy.trading_start_minutes_before,
            self.config.strategy.min_minutes_into_game,
            50,
        );
        let buy_threshold = self.config.strategy.buy_threshold;
        let window = chrono::Duration::minutes(live_window_minutes);
        let mut with_sum: Vec<(f64, SoccerMatch)> = Vec::new();
        for m in live {
            let (ah, ad, aa) = tokio::join!(
                self.api.get_best_ask(&m.home_market.yes_token_id),
                self.api.get_best_ask(&m.draw_market.yes_token_id),
                self.api.get_best_ask(&m.away_market.yes_token_id),
            );
            let (h, d, a) = (
                ah.ok().flatten(),
                ad.ok().flatten(),
                aa.ok().flatten(),
            );
            // If we can't fetch prices (placeholder filter or API error), skip the match.
            // Including it with sum 0.0 caused us to enter match cycles for games with sum >= threshold
            // (WebSocket showed real prices); we'd then exit after connecting, wasting resources.
            let sum = match (h, d, a) {
                (Some(hi), Some(di), Some(ai)) => {
                    let sum_home_draw = hi + di;
                    let sum_away_draw = ai + di;
                    sum_home_draw.max(sum_away_draw)
                }
                _ => continue,
            };
            if sum < buy_threshold {
                with_sum.push((sum, m.clone()));
            }
        }
        with_sum.sort_by(|a, b| {
            let a_end = a.1.game_start.unwrap_or(chrono::DateTime::<Utc>::MAX_UTC) + window;
            let b_end = b.1.game_start.unwrap_or(chrono::DateTime::<Utc>::MAX_UTC) + window;
            a_end.cmp(&b_end)
        });
        with_sum.into_iter().take(max_count).map(|(_, m)| m).collect()
    }

    /// Verify buy success by checking actual positions. Returns (has_team, has_draw).
    /// Waits 3–4s, fetches positions; if missing, retries once after 1s.
    async fn verify_buy_by_positions(
        api: &PolymarketApi,
        team_token_id: &str,
        draw_token_id: &str,
        expected_shares: f64,
    ) -> (bool, bool) {
        let wallet = match api.get_trading_wallet() {
            Ok(w) => w,
            Err(_) => return (false, false),
        };
        let positions = match api.get_all_positions(&wallet).await {
            Ok(p) => p,
            Err(_) => return (false, false),
        };
        let has_token = |token_id: &str| -> bool {
            positions.iter().any(|p| {
                let same = p.token_id == token_id;
                let enough = p.size >= expected_shares - 0.5; // small tolerance
                same && enough
            })
        };
        (has_token(team_token_id), has_token(draw_token_id))
    }

    /// Verify sell success by checking positions: we no longer hold >= expected of either token.
    /// Returns (sold_team, sold_draw). Only position polling — no SDK trust.
    async fn verify_sell_by_positions(
        api: &PolymarketApi,
        team_token_id: &str,
        draw_token_id: &str,
        expected_shares: f64,
    ) -> (bool, bool) {
        let wallet = match api.get_trading_wallet() {
            Ok(w) => w,
            Err(_) => return (false, false),
        };
        let positions = match api.get_all_positions(&wallet).await {
            Ok(p) => p,
            Err(_) => return (false, false),
        };
        let still_has = |token_id: &str| -> bool {
            positions.iter().any(|p| p.token_id == token_id && p.size >= expected_shares - 0.5)
        };
        let sold_team = !still_has(team_token_id);
        let sold_draw = !still_has(draw_token_id);
        (sold_team, sold_draw)
    }

    /// Verify risk exit by positions: team sold, opposite bought, draw still held.
    /// Returns (team_sold, opposite_bought, draw_ok). Only position polling — no SDK trust.
    async fn verify_risk_exit_by_positions(
        api: &PolymarketApi,
        sell_token_id: &str,
        buy_token_id: &str,
        draw_token_id: &str,
        expected_shares: f64,
    ) -> (bool, bool, bool) {
        let wallet = match api.get_trading_wallet() {
            Ok(w) => w,
            Err(_) => return (false, false, false),
        };
        let positions = match api.get_all_positions(&wallet).await {
            Ok(p) => p,
            Err(_) => return (false, false, false),
        };
        let has_token = |token_id: &str| -> bool {
            positions.iter().any(|p| p.token_id == token_id && p.size >= expected_shares - 0.5)
        };
        let still_has = |token_id: &str| -> bool {
            positions.iter().any(|p| p.token_id == token_id && p.size >= expected_shares - 0.5)
        };
        let team_sold = !still_has(sell_token_id);
        let opposite_bought = has_token(buy_token_id);
        let draw_ok = has_token(draw_token_id);
        (team_sold, opposite_bought, draw_ok)
    }

    async fn run_match_cycle_static(
        api: Arc<PolymarketApi>,
        config: Config,
        m: SoccerMatch,
        last_trade_at: Arc<RwLock<Option<std::time::Instant>>>,
        initial_holding: Option<HoldingPosition>,
    ) -> Result<()> {
        if m.closed {
            return Ok(());
        }

        let prices: PricesSnapshot = Arc::new(RwLock::new(HashMap::new()));
        let asset_ids = m.token_ids().to_vec();
        let ws_url = config.polymarket.ws_url.clone();
        let prices_clone = Arc::clone(&prices);

        let ws_handle = tokio::spawn(async move {
            if let Err(e) = run_market_ws(&ws_url, asset_ids, prices_clone).await {
                warn!("Match WebSocket exited: {}", e);
            }
        });

        sleep(Duration::from_secs(2)).await;

        let buy_threshold = config.strategy.buy_threshold;
        // When we have existing holdings, skip the "sum >= threshold" exit (we're monitoring to sell)
        if initial_holding.is_none() {
            let snap = prices.read().await;
            let ah = snap.get(&m.home_market.yes_token_id).and_then(|p| p.ask);
            let ad = snap.get(&m.draw_market.yes_token_id).and_then(|p| p.ask);
            let aa = snap.get(&m.away_market.yes_token_id).and_then(|p| p.ask);
            if let (Some(h), Some(d), Some(a)) = (ah, ad, aa) {
                info!(
                    "Live prices {} | Home Yes ${:.2} Draw Yes ${:.2} Away Yes ${:.2}",
                    m.title, h, d, a
                );
                let sum = (h + d).max(a + d);
                if sum >= buy_threshold {
                    info!(
                        "Skipping {}: live sum {:.2} >= buy_threshold {} — exiting match cycle",
                        m.title, sum, buy_threshold
                    );
                    ws_handle.abort();
                    return Ok(());
                }
            }
        }

        let mut holding: Option<HoldingPosition> = initial_holding;
        let mut last_price_log = std::time::Instant::now();
        let buy_threshold = config.strategy.buy_threshold;
        let live_window = chrono::Duration::minutes(config.strategy.live_window_minutes);
        let risk_threshold = config.strategy.risk_reduce_threshold;
        let sell_threshold = config.strategy.sell_profit_threshold;
        let shares_f: f64 = config.strategy.shares.parse().unwrap_or(100.0);
        let simulation = config.strategy.simulation_mode;
        let interval_secs = config.strategy.trade_interval_secs;

        loop {
            if m.closed {
                break;
            }
            let match_ended = if let Some(gs) = m.game_start {
                chrono::Utc::now() > gs + live_window
            } else {
                m.end_date.map(|ed| chrono::Utc::now() > ed).unwrap_or(false)
            };
            if match_ended {
                info!("Match {} ended", m.title);
                break;
            }
            let snap = prices.read().await;
            let ask_home = snap.get(&m.home_market.yes_token_id).and_then(|p| p.ask);
            let ask_draw = snap.get(&m.draw_market.yes_token_id).and_then(|p| p.ask);
            let ask_away = snap.get(&m.away_market.yes_token_id).and_then(|p| p.ask);
            drop(snap);

            if let (Some(h), Some(d), Some(a)) = (ask_home, ask_draw, ask_away) {
                if last_price_log.elapsed().as_secs() >= LIVE_PRICE_LOG_INTERVAL_SECS {
                    info!(
                        "Live prices: {} Home ${:.2} Draw ${:.2} Away ${:.2}",
                        m.title, h, d, a
                    );
                    last_price_log = std::time::Instant::now();
                }
            }

            if let Some(ref pos) = holding {
                let (team_ask, opp_ask) = match pos.team_outcome {
                    SoccerOutcome::HomeWin => (ask_home, ask_away),
                    SoccerOutcome::AwayWin => (ask_away, ask_home),
                    _ => continue,
                };
                let draw_ask = ask_draw;

                if let (Some(ta), Some(da)) = (team_ask, draw_ask) {
                    let current_sum = ta + da;
                    let buy_sum = pos.buy_price_team + pos.buy_price_draw;
                    let reduction = 1.0 - (current_sum / buy_sum);

                    if current_sum >= sell_threshold {
                        info!("Profit exit: {} sum {:.4} >= {} — selling both", m.title, current_sum, sell_threshold);
                        if !simulation {
                            let (sell_token, _) = match pos.team_outcome {
                                SoccerOutcome::HomeWin => (&m.home_market.yes_token_id, &m.away_market.yes_token_id),
                                SoccerOutcome::AwayWin => (&m.away_market.yes_token_id, &m.home_market.yes_token_id),
                                _ => continue,
                            };
                            let mut sold_team = false;
                            let mut sold_draw = false;
                            let mut leg_round = 0;
                            const LEG_ROUND_MAX: u32 = 3;

                            while (!sold_team || !sold_draw) && leg_round < LEG_ROUND_MAX {
                                leg_round += 1;
                                if !sold_team {
                                    let _ = api.place_market_order(sell_token, shares_f, "SELL", Some("FAK")).await;
                                }
                                if !sold_draw {
                                    let _ = api.place_market_order(&pos.draw_token_id, shares_f, "SELL", Some("FAK")).await;
                                }
                                sleep(Duration::from_secs(POSITION_CHECK_DELAY_SECS)).await;
                                let mut attempts = 0;
                                while attempts < 4 {
                                    let (st, sd) = Self::verify_sell_by_positions(
                                        api.as_ref(),
                                        sell_token,
                                        &pos.draw_token_id,
                                        shares_f,
                                    ).await;
                                    if st { sold_team = true; }
                                    if sd { sold_draw = true; }
                                    if sold_team && sold_draw {
                                        break;
                                    }
                                    attempts += 1;
                                    if attempts < 4 {
                                        info!("Sell position check attempt {}: team_sold={} draw_sold={}", attempts, sold_team, sold_draw);
                                        sleep(Duration::from_secs(POSITION_RETRY_DELAY_SECS)).await;
                                    }
                                }
                                if sold_team && sold_draw {
                                    break;
                                }
                                if leg_round < LEG_ROUND_MAX {
                                    info!("Sell position check incomplete for {} (team_sold={} draw_sold={}) — retrying leg round {}", m.title, sold_team, sold_draw, leg_round + 1);
                                }
                            }

                            if sold_team && sold_draw {
                                info!("Sold both for {} (verified by positions)", m.title);
                                holding = None;
                                *last_trade_at.write().await = Some(std::time::Instant::now());
                                break;
                            } else {
                                warn!("Sell position check failed for {} after {} rounds (team_sold={} draw_sold={}) — keeping holding, no SDK trust", m.title, leg_round, sold_team, sold_draw);
                            }
                        } else {
                            holding = None;
                            *last_trade_at.write().await = Some(std::time::Instant::now());
                            break;
                        }
                    } else if reduction >= risk_threshold {
                        info!(
                            "Risk management: {} price drop {:.1}% >= {:.1}% — sell team, buy opposite",
                            m.title, reduction * 100.0, risk_threshold * 100.0
                        );
                        if !simulation {
                            let (sell_token, buy_token) = match pos.team_outcome {
                                SoccerOutcome::HomeWin => (&m.home_market.yes_token_id, &m.away_market.yes_token_id),
                                SoccerOutcome::AwayWin => (&m.away_market.yes_token_id, &m.home_market.yes_token_id),
                                _ => continue,
                            };
                            let do_buy = opp_ask.unwrap_or(0.5) >= MIN_ORDER_USD / shares_f;
                            let mut team_sold = false;
                            let mut opposite_bought = false;
                            let mut draw_ok = true;
                            let mut leg_round = 0;
                            const LEG_ROUND_MAX: u32 = 3;

                            while leg_round < LEG_ROUND_MAX && !(team_sold && (!do_buy || opposite_bought) && draw_ok) {
                                leg_round += 1;
                                if !team_sold {
                                    let _ = api.place_market_order(sell_token, shares_f, "SELL", Some("FAK")).await;
                                }
                                if do_buy && !opposite_bought {
                                    let _ = api.place_market_order(buy_token, shares_f, "BUY", Some("FAK")).await;
                                }
                                sleep(Duration::from_secs(POSITION_CHECK_DELAY_SECS)).await;
                                let mut attempts = 0;
                                while attempts < 4 {
                                    let (ts, ob, dk) = Self::verify_risk_exit_by_positions(
                                        api.as_ref(),
                                        sell_token,
                                        buy_token,
                                        &pos.draw_token_id,
                                        shares_f,
                                    ).await;
                                    if ts { team_sold = true; }
                                    if ob { opposite_bought = true; }
                                    draw_ok = dk;
                                    if team_sold && (!do_buy || opposite_bought) && draw_ok {
                                        break;
                                    }
                                    attempts += 1;
                                    if attempts < 4 {
                                        info!("Risk exit position check attempt {}: team_sold={} opposite_bought={} draw_ok={}", attempts, team_sold, opposite_bought, draw_ok);
                                        sleep(Duration::from_secs(POSITION_RETRY_DELAY_SECS)).await;
                                    }
                                }
                                if team_sold && (!do_buy || opposite_bought) && draw_ok {
                                    break;
                                }
                                if leg_round < LEG_ROUND_MAX {
                                    info!("Risk exit position check incomplete for {} — retrying leg round {}", m.title, leg_round + 1);
                                }
                            }

                            if team_sold && (!do_buy || opposite_bought) && draw_ok {
                                if do_buy && opposite_bought {
                                    holding = Some(HoldingPosition {
                                        match_slug: m.slug.clone(),
                                        team_token_id: buy_token.to_string(),
                                        team_outcome: match &pos.team_outcome {
                                            SoccerOutcome::HomeWin => SoccerOutcome::AwayWin,
                                            SoccerOutcome::AwayWin => SoccerOutcome::HomeWin,
                                            x => x.clone(),
                                        },
                                        draw_token_id: pos.draw_token_id.clone(),
                                        buy_price_team: opp_ask.unwrap_or(0.5),
                                        buy_price_draw: pos.buy_price_draw,
                                        size: shares_f,
                                    });
                                    info!("Risk exit: transformed to (opposite + draw) for {} (verified by positions)", m.title);
                                } else {
                                    holding = None;
                                }
                                *last_trade_at.write().await = Some(std::time::Instant::now());
                                break;
                            } else {
                                warn!("Risk exit position check failed for {} after {} rounds (team_sold={} opposite_bought={} draw_ok={}) — keeping holding, no SDK trust", m.title, leg_round, team_sold, opposite_bought, draw_ok);
                            }
                        } else {
                            holding = None;
                            *last_trade_at.write().await = Some(std::time::Instant::now());
                            break;
                        }
                    }
                }
                sleep(Duration::from_millis(LIVE_PRICE_POLL_MS)).await;
                continue;
            }

            let game_started = m
                .game_start
                .map(|gs| chrono::Utc::now() >= gs)
                .unwrap_or(true);
            if !game_started {
                sleep(Duration::from_millis(LIVE_PRICE_POLL_MS)).await;
                continue;
            }

            if let Some(t) = *last_trade_at.read().await {
                if t.elapsed().as_secs() < interval_secs {
                    sleep(Duration::from_millis(LIVE_PRICE_POLL_MS)).await;
                    continue;
                }
            }

            let (ask_home, ask_draw, ask_away) = (
                ask_home.unwrap_or(1.0),
                ask_draw.unwrap_or(1.0),
                ask_away.unwrap_or(1.0),
            );

            let sum_home_draw = ask_home + ask_draw;
            let sum_away_draw = ask_away + ask_draw;

            let (team_token, team_price, team_outcome, sum) = if sum_home_draw >= sum_away_draw {
                (&m.home_market.yes_token_id, ask_home, SoccerOutcome::HomeWin, sum_home_draw)
            } else {
                (&m.away_market.yes_token_id, ask_away, SoccerOutcome::AwayWin, sum_away_draw)
            };

            if sum >= buy_threshold {
                sleep(Duration::from_millis(LIVE_PRICE_POLL_MS)).await;
                continue;
            }

            let min_price = MIN_ORDER_USD / shares_f;
            if team_price < min_price || ask_draw < min_price {
                info!("Skipping: leg price {:.4} < min {:.4} (min $1 order)", team_price.min(ask_draw), min_price);
                sleep(Duration::from_millis(LIVE_PRICE_POLL_MS)).await;
                continue;
            }

            if simulation {
                info!(
                    "[SIM] Would buy: team @ {:.4} + draw @ {:.4} ({} shares)",
                    team_price, ask_draw, shares_f
                );
                holding = Some(HoldingPosition {
                    match_slug: m.slug.clone(),
                    team_token_id: team_token.to_string(),
                    team_outcome,
                    draw_token_id: m.draw_market.yes_token_id.clone(),
                    buy_price_team: team_price,
                    buy_price_draw: ask_draw,
                    size: shares_f,
                });
                *last_trade_at.write().await = Some(std::time::Instant::now());
                sleep(Duration::from_millis(LIVE_PRICE_POLL_MS)).await;
                continue;
            }

            // Place both buy orders
            let (_r1, _r2) = tokio::join!(
                api.place_market_order(team_token, shares_f, "BUY", Some("FAK")),
                api.place_market_order(&m.draw_market.yes_token_id, shares_f, "BUY", Some("FAK")),
            );

            // Verify by checking positions: wait for Data API to index (can take 15-30s).
            // Do multiple check retries before ever placing retry orders (avoids double-buying).
            sleep(Duration::from_secs(POSITION_CHECK_DELAY_SECS)).await;
            let (mut has_team, mut has_draw) = Self::verify_buy_by_positions(
                api.as_ref(),
                team_token,
                &m.draw_market.yes_token_id,
                shares_f,
            ).await;

            let mut attempts = 1;
            while (!has_team || !has_draw) && attempts < 4 {
                info!("Position check attempt {}: team={} draw={} (Data API may be slow)", attempts, has_team, has_draw);
                sleep(Duration::from_secs(POSITION_RETRY_DELAY_SECS)).await;
                let (ht, hd) = Self::verify_buy_by_positions(
                    api.as_ref(),
                    team_token,
                    &m.draw_market.yes_token_id,
                    shares_f,
                ).await;
                has_team = has_team || ht;
                has_draw = has_draw || hd;
                attempts += 1;
            }

            if has_team && has_draw {
                info!("Bought team + draw for {} (verified by positions)", m.title);
                holding = Some(HoldingPosition {
                    match_slug: m.slug.clone(),
                    team_token_id: team_token.to_string(),
                    team_outcome,
                    draw_token_id: m.draw_market.yes_token_id.clone(),
                    buy_price_team: team_price,
                    buy_price_draw: ask_draw,
                    size: shares_f,
                });
                *last_trade_at.write().await = Some(std::time::Instant::now());
            } else if !has_team && !has_draw {
                // Both false: no position confirmation. Retry full buy and poll again (no SDK trust).
                let mut full_retries = 0;
                const FULL_RETRY_MAX: u32 = 2;
                const FULL_RETRY_DELAY_SECS: u64 = 10;
                while full_retries < FULL_RETRY_MAX {
                    full_retries += 1;
                    info!("Buy position check both false for {} — retrying full buy (round {})", m.title, full_retries);
                    let _ = api.place_market_order(team_token, shares_f, "BUY", Some("FAK")).await;
                    let _ = api.place_market_order(&m.draw_market.yes_token_id, shares_f, "BUY", Some("FAK")).await;
                    sleep(Duration::from_secs(FULL_RETRY_DELAY_SECS)).await;
                    for _ in 0..4 {
                        let (ht, hd) = Self::verify_buy_by_positions(api.as_ref(), team_token, &m.draw_market.yes_token_id, shares_f).await;
                        has_team = has_team || ht;
                        has_draw = has_draw || hd;
                        if has_team && has_draw {
                            break;
                        }
                        sleep(Duration::from_secs(POSITION_RETRY_DELAY_SECS)).await;
                    }
                    if has_team && has_draw {
                        info!("Bought team + draw for {} (verified after full retry)", m.title);
                        holding = Some(HoldingPosition {
                            match_slug: m.slug.clone(),
                            team_token_id: team_token.to_string(),
                            team_outcome,
                            draw_token_id: m.draw_market.yes_token_id.clone(),
                            buy_price_team: team_price,
                            buy_price_draw: ask_draw,
                            size: shares_f,
                        });
                        *last_trade_at.write().await = Some(std::time::Instant::now());
                        break;
                    }
                }
                if !has_team || !has_draw {
                    warn!("Buy position check failed for {} after {} full retries (team={} draw={}) — not setting holding, no SDK trust", m.title, full_retries, has_team, has_draw);
                }
            } else {
                // Partial: we have one leg but not the other. Retry the missing leg (don't trust SDK).
                let mut leg_retries = 0;
                const LEG_RETRY_MAX: u32 = 2;
                const LEG_RETRY_DELAY_SECS: u64 = 10;

                while (!has_team || !has_draw) && leg_retries < LEG_RETRY_MAX {
                    leg_retries += 1;
                    if !has_team {
                        info!("Retrying team buy for {} (position check showed team=false)", m.title);
                        let _ = api.place_market_order(team_token, shares_f, "BUY", Some("FAK")).await;
                    }
                    if !has_draw {
                        info!("Retrying draw buy for {} (position check showed draw=false)", m.title);
                        let _ = api.place_market_order(&m.draw_market.yes_token_id, shares_f, "BUY", Some("FAK")).await;
                    }
                    sleep(Duration::from_secs(LEG_RETRY_DELAY_SECS)).await;
                    let (ht, hd) = Self::verify_buy_by_positions(
                        api.as_ref(),
                        team_token,
                        &m.draw_market.yes_token_id,
                        shares_f,
                    ).await;
                    has_team = has_team || ht;
                    has_draw = has_draw || hd;
                }

                if has_team && has_draw {
                    info!("Bought team + draw for {} (verified after retry)", m.title);
                    holding = Some(HoldingPosition {
                        match_slug: m.slug.clone(),
                        team_token_id: team_token.to_string(),
                        team_outcome,
                        draw_token_id: m.draw_market.yes_token_id.clone(),
                        buy_price_team: team_price,
                        buy_price_draw: ask_draw,
                        size: shares_f,
                    });
                    *last_trade_at.write().await = Some(std::time::Instant::now());
                } else {
                    // Still partial after retries. Don't set holding — we'd try to sell tokens we don't have.
                    warn!(
                        "Partial position for {} after {} leg retries (team={} draw={}). Not setting holding — missing leg may need manual check.",
                        m.title, leg_retries, has_team, has_draw
                    );
                }
            }

            sleep(Duration::from_millis(LIVE_PRICE_POLL_MS)).await;
        }

        ws_handle.abort();
        Ok(())
    }

    async fn run_match_cycle(
        &self,
        m: SoccerMatch,
        last_trade_at: Arc<RwLock<Option<std::time::Instant>>>,
        initial_holding: Option<HoldingPosition>,
    ) -> Result<()> {
        Self::run_match_cycle_static(
            self.api.clone(),
            self.config.clone(),
            m,
            last_trade_at,
            initial_holding,
        ).await
    }
}
