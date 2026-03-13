use clap::Parser;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(author, version, about = "Polymarket Soccer Trading Bot")]
pub struct Args {
    #[arg(short, long, default_value = "config.json")]
    pub config: PathBuf,

    #[arg(long)]
    pub redeem: bool,

    #[arg(long, requires = "redeem")]
    pub condition_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub polymarket: PolymarketConfig,
    pub strategy: StrategyConfig,
}

/// Soccer match trading strategy: buy high-priced team + Draw when sum < threshold.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyConfig {
    /// Polymarket tag IDs for soccer leagues (e.g. 100977=UCL, 100350=soccer).
    #[serde(default = "default_soccer_tag_ids")]
    pub soccer_tag_ids: Vec<String>,
    /// Interval in seconds to poll for soccer matches (e.g. 60–300).
    #[serde(default = "default_market_poll_interval_secs")]
    pub market_poll_interval_secs: u64,
    /// Max event limit per tag when fetching matches.
    #[serde(default = "default_market_fetch_limit")]
    pub market_fetch_limit: u32,
    /// Buy when (high_team_ask + draw_ask) < this (e.g. 0.95).
    #[serde(default = "default_buy_threshold")]
    pub buy_threshold: f64,
    /// Trigger risk management when holding tokens' combined price drops by this fraction (e.g. 0.13 = 13%).
    #[serde(default = "default_risk_reduce_threshold")]
    pub risk_reduce_threshold: f64,
    /// Sell both tokens for profit when combined price >= this (e.g. 0.99).
    #[serde(default = "default_sell_profit_threshold")]
    pub sell_profit_threshold: f64,
    #[serde(default)]
    pub simulation_mode: bool,
    /// Shares per leg when buying (team + draw).
    #[serde(default = "default_shares")]
    pub shares: String,
    /// Seconds between placing trades (cooldown).
    #[serde(default = "default_trade_interval_secs")]
    pub trade_interval_secs: u64,
    /// Path for caching fetched market data (JSON file).
    #[serde(default = "default_market_cache_path")]
    pub market_cache_path: String,
    /// Interval in seconds to refresh market cache from API (e.g. 300 = 5 min).
    #[serde(default = "default_market_refresh_interval_secs")]
    pub market_refresh_interval_secs: u64,
    /// Minutes after game start to consider match "live" (e.g. 135 = 2h15 for soccer).
    #[serde(default = "default_live_window_minutes")]
    pub live_window_minutes: i64,
    /// Minutes before game_start to start match cycle (connect WS, get prices). Trading begins at game_start.
    #[serde(default = "default_trading_start_minutes_before")]
    pub trading_start_minutes_before: i64,
    /// For live matches: only consider if at least N minutes into the game (0 = from kickoff).
    /// Set to 45 to require game_start at least 45 mins before now (skip first 45 min).
    #[serde(default = "default_min_minutes_into_game")]
    pub min_minutes_into_game: i64,
    /// Max number of live markets to trade simultaneously (e.g. 1 or 2 to avoid overload).
    #[serde(default = "default_max_concurrent_markets")]
    pub max_concurrent_markets: u32,
    /// Number of nearest upcoming matches to display when no live matches (e.g. 5).
    #[serde(default = "default_nearest_upcoming_count")]
    pub nearest_upcoming_count: usize,
}

fn default_soccer_tag_ids() -> Vec<String> {
    vec!["100977".into(), "100350".into()] // UCL, soccer
}
fn default_market_poll_interval_secs() -> u64 {
    120 // 2 min
}
fn default_market_fetch_limit() -> u32 {
    50
}
fn default_buy_threshold() -> f64 {
    0.95
}
fn default_risk_reduce_threshold() -> f64 {
    0.13
}
fn default_sell_profit_threshold() -> f64 {
    0.99
}
fn default_shares() -> String {
    "100".to_string()
}
fn default_trade_interval_secs() -> u64 {
    60
}
fn default_market_cache_path() -> String {
    "markets_cache.json".to_string()
}
fn default_market_refresh_interval_secs() -> u64 {
    300 // 5 min
}
fn default_live_window_minutes() -> i64 {
    135 // 2h15 for soccer (90 + stoppage + half)
}
fn default_trading_start_minutes_before() -> i64 {
    5 // Enter match cycle 5 min before kickoff so we're ready at game_start
}
fn default_min_minutes_into_game() -> i64 {
    0 // 0 = from kickoff; 45 = skip first 45 min (require game_start at least 45 mins before now)
}
fn default_max_concurrent_markets() -> u32 {
    1 // Trade one market at a time by default
}
fn default_nearest_upcoming_count() -> usize {
    5
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolymarketConfig {
    pub gamma_api_url: String,
    pub clob_api_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub api_passphrase: Option<String>,
    pub private_key: Option<String>,
    pub proxy_wallet_address: Option<String>,
    pub signature_type: Option<u8>,
    #[serde(default)]
    pub rpc_url: Option<String>,
    #[serde(default = "default_ws_url")]
    pub ws_url: String,
}

fn default_ws_url() -> String {
    "wss://ws-subscriptions-clob.polymarket.com".to_string()
}

impl Default for Config {
    fn default() -> Self {
        Self {
            polymarket: PolymarketConfig {
                gamma_api_url: "https://gamma-api.polymarket.com".to_string(),
                clob_api_url: "https://clob.polymarket.com".to_string(),
                api_key: None,
                api_secret: None,
                api_passphrase: None,
                private_key: None,
                proxy_wallet_address: None,
                signature_type: None,
                rpc_url: None,
                ws_url: default_ws_url(),
            },
            strategy: StrategyConfig {
                soccer_tag_ids: default_soccer_tag_ids(),
                market_poll_interval_secs: default_market_poll_interval_secs(),
                market_fetch_limit: default_market_fetch_limit(),
                buy_threshold: default_buy_threshold(),
                risk_reduce_threshold: default_risk_reduce_threshold(),
                sell_profit_threshold: default_sell_profit_threshold(),
                simulation_mode: false,
                shares: default_shares(),
                trade_interval_secs: default_trade_interval_secs(),
                market_cache_path: default_market_cache_path(),
                market_refresh_interval_secs: default_market_refresh_interval_secs(),
                live_window_minutes: default_live_window_minutes(),
                trading_start_minutes_before: default_trading_start_minutes_before(),
                min_minutes_into_game: default_min_minutes_into_game(),
                max_concurrent_markets: default_max_concurrent_markets(),
                nearest_upcoming_count: default_nearest_upcoming_count(),
            },
        }
    }
}

impl Config {
    pub fn load(path: &PathBuf) -> anyhow::Result<Self> {
        if path.exists() {
            let content = std::fs::read_to_string(path)?;
            Ok(serde_json::from_str(&content)?)
        } else {
            let config = Config::default();
            let content = serde_json::to_string_pretty(&config)?;
            std::fs::write(path, content)?;
            Ok(config)
        }
    }
}
