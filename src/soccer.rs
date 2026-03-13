//! Soccer match discovery: fetch live/upcoming match events from Polymarket Gamma API.
//! Match events have 3 outcomes: Home team Yes, Draw Yes, Away team Yes.

use crate::api::PolymarketApi;
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;

/// Market within a soccer match event (Home win, Draw, or Away win).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SoccerMarket {
    pub condition_id: String,
    pub outcome: SoccerOutcome,
    pub yes_token_id: String,
    pub question: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SoccerOutcome {
    HomeWin,
    Draw,
    AwayWin,
}

/// A soccer match event with Home/Draw/Away markets.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SoccerMatch {
    pub event_id: String,
    pub slug: String,
    pub title: String,
    /// Game kickoff time (UTC). From Polymarket market.gameStartTime.
    #[serde(with = "chrono::serde::ts_seconds_option")]
    pub game_start: Option<DateTime<Utc>>,
    #[serde(with = "chrono::serde::ts_seconds_option")]
    pub start_date: Option<DateTime<Utc>>,
    #[serde(with = "chrono::serde::ts_seconds_option")]
    pub end_date: Option<DateTime<Utc>>,
    pub active: bool,
    pub closed: bool,
    pub home_market: SoccerMarket,
    pub draw_market: SoccerMarket,
    pub away_market: SoccerMarket,
}

#[derive(Debug, Deserialize)]
struct GammaEvent {
    id: String,
    slug: Option<String>,
    title: Option<String>,
    #[serde(rename = "startDate")]
    start_date: Option<String>,
    #[serde(rename = "endDate")]
    end_date: Option<String>,
    active: Option<bool>,
    closed: Option<bool>,
    markets: Option<Vec<GammaMarket>>,
}

#[derive(Debug, Deserialize)]
struct GammaMarket {
    #[serde(rename = "conditionId")]
    condition_id: Option<String>,
    slug: Option<String>,
    question: Option<String>,
    #[serde(rename = "clobTokenIds")]
    clob_token_ids: Option<serde_json::Value>,
    #[serde(rename = "gameStartTime")]
    game_start_time: Option<String>,
}

impl SoccerMatch {
    /// Get the "Yes" token IDs for each outcome (for orderbook/WebSocket).
    pub fn token_ids(&self) -> [String; 3] {
        [
            self.home_market.yes_token_id.clone(),
            self.draw_market.yes_token_id.clone(),
            self.away_market.yes_token_id.clone(),
        ]
    }
}

fn parse_clob_token_ids(val: &serde_json::Value) -> Option<String> {
    if let Some(arr) = val.as_array() {
        let first = arr.first()?;
        return first.as_str().map(String::from);
    }
    if let Some(s) = val.as_str() {
        let parsed: Vec<String> = serde_json::from_str(s).ok()?;
        return parsed.into_iter().next();
    }
    None
}

fn parse_datetime(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s).ok().map(|dt| dt.with_timezone(&Utc))
}

/// Parse gameStartTime from Polymarket (e.g. "2026-03-17 20:00:00+00").
fn parse_game_start(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%#z")
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

fn is_draw_market(slug: &str, question: &str) -> bool {
    let slug_lower = slug.to_lowercase();
    let q_lower = question.to_lowercase();
    slug_lower.contains("draw") || q_lower.contains("draw") || q_lower.contains(" end in a draw")
}

pub struct SoccerDiscovery {
    api: Arc<PolymarketApi>,
}

impl SoccerDiscovery {
    pub fn new(api: Arc<PolymarketApi>) -> Self {
        Self { api }
    }

    /// Fetch soccer match events from Gamma API. Filters for events with exactly 3 markets
    /// (Home, Draw, Away) and one containing "draw". Excludes matches that have already ended
    /// (game_start + live_window < now).
    pub async fn fetch_soccer_matches(
        &self,
        tag_ids: &[String],
        limit: u32,
        live_window_minutes: i64,
    ) -> Result<Vec<SoccerMatch>> {
        let mut all_matches = Vec::new();
        for tag_id in tag_ids {
            // Don't filter by closed=false: Polymarket may mark events closed at kickoff.
            let url = format!(
                "{}/events?tag_id={}&active=true&limit={}",
                self.api.gamma_url(),
                tag_id,
                limit
            );
            log::debug!("Fetching soccer events: {}", url);
            let values = self.api.fetch_gamma_events(&url).await?;
            for v in values {
                let ev: GammaEvent = match serde_json::from_value(v) {
                    Ok(e) => e,
                    Err(_) => continue,
                };
                if let Some(m) = Self::parse_match_event(ev) {
                    all_matches.push(m);
                }
            }
        }
        // Exclude matches that have already ended (now < game_start + live_window)
        let now = Utc::now();
        let window = chrono::Duration::minutes(live_window_minutes);
        all_matches.retain(|m| {
            m.game_start
                .map(|gs| now < gs + window)
                .unwrap_or(false)
        });
        // Deduplicate by event_id, then sort by game_start
        let mut seen = std::collections::HashSet::new();
        let mut unique: Vec<_> = all_matches
            .into_iter()
            .filter(|m| seen.insert(m.event_id.clone()))
            .collect();
        unique.sort_by(|a, b| {
            let a_gs = a.game_start.unwrap_or(chrono::DateTime::<Utc>::MAX_UTC);
            let b_gs = b.game_start.unwrap_or(chrono::DateTime::<Utc>::MAX_UTC);
            a_gs.cmp(&b_gs)
        });
        Ok(unique)
    }

    /// Fetch matches, sort by gameStartTime, save to file. Call every 5 min.
    /// Excludes past matches (game already ended).
    pub async fn fetch_sort_and_save(
        &self,
        tag_ids: &[String],
        limit: u32,
        cache_path: impl AsRef<Path>,
        live_window_minutes: i64,
    ) -> Result<Vec<SoccerMatch>> {
        let matches = self.fetch_soccer_matches(tag_ids, limit, live_window_minutes).await?;
        if !matches.is_empty() {
            let json = serde_json::to_string_pretty(&matches)?;
            std::fs::write(cache_path.as_ref(), json)?;
            log::info!("Cached {} matches to {:?}", matches.len(), cache_path.as_ref());
        }
        Ok(matches)
    }

    /// Load cached matches from file (for startup or when API is unavailable).
    pub fn load_cached_matches(path: impl AsRef<Path>) -> Result<Vec<SoccerMatch>> {
        let content = std::fs::read_to_string(path)?;
        Ok(serde_json::from_str(&content)?)
    }

    /// Select a match to trade: from trading_start_minutes_before kickoff until game ends.
    /// Trading starts at game_start. Returns the match ending soonest.
    pub fn select_live_match<'a>(
        &self,
        matches: &'a [SoccerMatch],
        live_window_minutes: i64,
        trading_start_minutes_before: i64,
    ) -> Option<&'a SoccerMatch> {
        self.select_live_matches(matches, live_window_minutes, trading_start_minutes_before, 0, 1)
            .into_iter()
            .next()
    }

    /// Select up to `limit` live matches, sorted by ending soonest.
    pub fn select_live_matches<'a>(
        &self,
        matches: &'a [SoccerMatch],
        live_window_minutes: i64,
        trading_start_minutes_before: i64,
        min_minutes_into_game: i64,
        limit: usize,
    ) -> Vec<&'a SoccerMatch> {
        let now = Utc::now();
        let window = chrono::Duration::minutes(live_window_minutes);
        let pre = chrono::Duration::minutes(trading_start_minutes_before);
        let min_in = chrono::Duration::minutes(min_minutes_into_game);
        // Include matches in the live window. Don't filter by !closed: Polymarket may mark events
        // closed at kickoff even when in-play trading is still open.
        // For live matches (now > gs): require now >= gs + min_minutes_into_game (skip first N min if set).
        let mut live: Vec<_> = matches
            .iter()
            .filter(|m| m.active)
            .filter(|m| {
                m.game_start.map(|gs| {
                    let in_window = now >= gs - pre && now <= gs + window;
                    let past_min = min_minutes_into_game == 0 || now <= gs || now >= gs + min_in;
                    in_window && past_min
                }).unwrap_or(false)
            })
            .collect();
        live.sort_by(|a, b| {
            let a_end = a.game_start.unwrap_or(chrono::DateTime::<Utc>::MAX_UTC) + window;
            let b_end = b.game_start.unwrap_or(chrono::DateTime::<Utc>::MAX_UTC) + window;
            a_end.cmp(&b_end)
        });
        live.into_iter().take(limit).collect()
    }

    /// Select up to `limit` nearest upcoming matches (game_start > now), sorted by game_start.
    pub fn select_nearest_upcoming<'a>(
        &self,
        matches: &'a [SoccerMatch],
        limit: usize,
    ) -> Vec<&'a SoccerMatch> {
        let now = Utc::now();
        matches
            .iter()
            .filter(|m| m.active && !m.closed)
            .filter(|m| m.game_start.map(|gs| now < gs).unwrap_or(false))
            .take(limit)
            .collect()
    }

    fn parse_match_event(ev: GammaEvent) -> Option<SoccerMatch> {
        let markets = ev.markets?;
        if markets.len() != 3 {
            return None;
        }
        let mut home = None;
        let mut draw = None;
        let mut away = None;
        let mut game_start: Option<DateTime<Utc>> = None;

        for m in markets.iter() {
            if game_start.is_none() {
                game_start = m.game_start_time.as_deref().and_then(parse_game_start);
            }
            let cid = m.condition_id.clone()?;
            let yes_token = m.clob_token_ids.as_ref().and_then(parse_clob_token_ids)?;
            let question = m.question.clone().unwrap_or_default();
            let slug = m.slug.as_deref().unwrap_or("");

            if is_draw_market(slug, &question) {
                draw = Some(SoccerMarket {
                    condition_id: cid,
                    outcome: SoccerOutcome::Draw,
                    yes_token_id: yes_token,
                    question,
                });
            } else {
                // Convention: first non-draw = home, second = away
                if home.is_none() {
                    home = Some(SoccerMarket {
                        condition_id: cid,
                        outcome: SoccerOutcome::HomeWin,
                        yes_token_id: yes_token,
                        question,
                    });
                } else {
                    away = Some(SoccerMarket {
                        condition_id: cid,
                        outcome: SoccerOutcome::AwayWin,
                        yes_token_id: yes_token,
                        question,
                    });
                }
            }
        }

        let (home_market, draw_market, away_market) = (home?, draw?, away?);

        Some(SoccerMatch {
            event_id: ev.id,
            slug: ev.slug.unwrap_or_default(),
            title: ev.title.unwrap_or_default(),
            start_date: ev.start_date.as_deref().and_then(parse_datetime),
            end_date: ev.end_date.as_deref().and_then(parse_datetime),
            game_start,
            active: ev.active.unwrap_or(false),
            closed: ev.closed.unwrap_or(false),
            home_market,
            draw_market,
            away_market,
        })
    }

    /// Select the current live or nearest upcoming match from the list.
    pub fn select_best_match<'a>(&self, matches: &'a [SoccerMatch]) -> Option<&'a SoccerMatch> {
        let now = Utc::now();
        let eligible: Vec<_> = matches
            .iter()
            .filter(|m| m.active && !m.closed)
            .filter(|m| {
                m.end_date.map(|ed| now < ed).unwrap_or(true)
            })
            .collect();
        if eligible.is_empty() {
            return None;
        }
        eligible.into_iter().min_by(|a, b| {
            let a_end = a.end_date.unwrap_or(chrono::DateTime::<Utc>::MAX_UTC);
            let b_end = b.end_date.unwrap_or(chrono::DateTime::<Utc>::MAX_UTC);
            a_end.cmp(&b_end)
        })
    }
}
