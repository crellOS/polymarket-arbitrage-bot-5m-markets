//! Soccer match trading: buy high-priced team + Draw when sum < threshold;
//! monitor via WebSocket; risk management (sell team, buy opposite) or profit exit (sell both).

use crate::api::PolymarketApi;
use crate::config::Config;
use crate::soccer::{SoccerDiscovery, SoccerMatch, SoccerOutcome};
use crate::ws::{run_market_ws, PricesSnapshot};
use anyhow::Result;
use log::{error, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{RwLock, Semaphore};
use tokio::time::{sleep, Duration};

const LIVE_PRICE_POLL_MS: u64 = 200;
const LIVE_PRICE_LOG_INTERVAL_SECS: u64 = 5;
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

            let m = if cached_matches.is_empty() {
                None
            } else {
                self.discovery.select_live_match(
                    &cached_matches,
                    live_window,
                    self.config.strategy.trading_start_minutes_before,
                )
            };

            let max_concurrent = self.config.strategy.max_concurrent_markets as usize;

            if max_concurrent == 1 {
                match m {
                    Some(live_match) => {
                        info!("Live match: {} (game_start: {:?})", live_match.title, live_match.game_start);
                        if let Err(e) = self
                            .run_match_cycle(live_match.clone(), Arc::clone(&last_trade_at))
                            .await
                        {
                            error!("Match cycle error: {}", e);
                        }
                    }
                    None => {
                        info!("No live soccer matches (trading only when game is in progress). Waiting {}s.",
                            self.config.strategy.market_poll_interval_secs);
                    }
                }
            } else {
                let live_matches = self.discovery.select_live_matches(
                    &cached_matches,
                    live_window,
                    self.config.strategy.trading_start_minutes_before,
                    max_concurrent,
                );

                for live_match in &live_matches {
                    let ids = trading_ids.write().await;
                    if ids.contains(&live_match.event_id) {
                        continue;
                    }
                    drop(ids);

                    let permit = match semaphore.clone().try_acquire_owned() {
                        Ok(p) => p,
                        Err(_) => break,
                    };

                    let m_clone = (*live_match).clone();
                    let event_id = m_clone.event_id.clone();
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
                        ).await {
                            error!("Match cycle error: {}", e);
                        }
                        trading_ids_clone.write().await.remove(&event_id);
                    });

                    info!("Started trading: {} ({} active)", live_match.title, trading_ids.read().await.len());
                }

                if live_matches.is_empty() {
                    info!("No live soccer matches. Waiting {}s.", self.config.strategy.market_poll_interval_secs);
                }
            }

            sleep(Duration::from_secs(self.config.strategy.market_poll_interval_secs)).await;
        }
    }

    async fn run_match_cycle_static(
        api: Arc<PolymarketApi>,
        config: Config,
        m: SoccerMatch,
        last_trade_at: Arc<RwLock<Option<std::time::Instant>>>,
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

        {
            let snap = prices.read().await;
            let ah = snap.get(&m.home_market.yes_token_id).and_then(|p| p.ask);
            let ad = snap.get(&m.draw_market.yes_token_id).and_then(|p| p.ask);
            let aa = snap.get(&m.away_market.yes_token_id).and_then(|p| p.ask);
            if let (Some(h), Some(d), Some(a)) = (ah, ad, aa) {
                info!(
                    "Live prices {} | Home Yes ${:.2} Draw Yes ${:.2} Away Yes ${:.2}",
                    m.title, h, d, a
                );
            }
        }

        let mut holding: Option<HoldingPosition> = None;
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
                            let _ = api.place_market_order(sell_token, shares_f, "SELL", Some("FAK")).await;
                            let _ = api.place_market_order(&pos.draw_token_id, shares_f, "SELL", Some("FAK")).await;
                        }
                        holding = None;
                        *last_trade_at.write().await = Some(std::time::Instant::now());
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
                            let _ = api.place_market_order(sell_token, shares_f, "SELL", Some("FAK")).await;
                            let opp_price = opp_ask.unwrap_or(0.5);
                            if opp_price >= MIN_ORDER_USD / shares_f {
                                let _ = api.place_market_order(buy_token, shares_f, "BUY", Some("FAK")).await;
                            }
                        }
                        holding = None;
                        *last_trade_at.write().await = Some(std::time::Instant::now());
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

            let (r1, r2) = tokio::join!(
                api.place_market_order(team_token, shares_f, "BUY", Some("FAK")),
                api.place_market_order(&m.draw_market.yes_token_id, shares_f, "BUY", Some("FAK")),
            );

            match (&r1, &r2) {
                (Ok(_), Ok(_)) => {
                    info!("Bought team + draw for {}", m.title);
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
                }
                (Err(e), _) | (_, Err(e)) => {
                    warn!("Order failed: {}", e);
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
    ) -> Result<()> {
        Self::run_match_cycle_static(
            self.api.clone(),
            self.config.clone(),
            m,
            last_trade_at,
        ).await
    }
}
