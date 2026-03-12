# Polymarket Soccer Trading Bot

A **Rust** bot for trading **soccer match** prediction markets on [Polymarket](https://polymarket.com). It fetches live soccer matches (UCL, EPL, La Liga, etc.), buys **high-priced team + Draw** when the combined ask is below a threshold, and monitors positions via WebSocket with **risk management** and **profit exit**.

---

## Features

- **Match discovery** — Fetches soccer events from Polymarket Gamma API (UCL, leagues, etc.) every 1–5 minutes.
- **Buy logic** — Buys the higher-priced team's "Yes" token + Draw "Yes" when `(team_ask + draw_ask) < 0.95`.
- **Real-time prices** — WebSocket subscription to the CLOB market channel for live bid/ask updates.
- **Risk management** — If holding tokens' combined price drops by 13% (configurable), sells the team token and buys the opposite team's token, keeping the Draw.
- **Profit exit** — Sells both tokens when combined price ≥ 0.99.
- **Simulation mode** — Run without placing real orders.

---

## How It Works

1. **Discovery** — Every 5 minutes (configurable), fetch soccer events from all `soccer_tag_ids`, filter for team-vs-team markets (3 outcomes: Home, Draw, Away), sort by `gameStartTime`, and save to `markets_cache.json`.
2. **Live only** — Trade only when a match is **live**: `gameStartTime <= now <= gameStartTime + live_window_minutes` (default 135 min = 2h15 for soccer). Uses Polymarket’s `gameStartTime` (UTC).
3. **Prices** — Subscribe to the 3 outcome tokens via WebSocket.
4. **Entry** — When `(high_team_ask + draw_ask) < buy_threshold`, buy both legs.
5. **Monitoring** — While holding: **profit exit** if sum ≥ 0.99; **risk exit** if price drops ≥ 13%.
6. **Repeat** — Until match ends (game_start + live_window) or closed.

---

## Requirements

- **Rust** 1.70+
- **Polymarket account** with API credentials and (for live trading) a funded proxy wallet
- **Polygon** network

---

## Installation

```bash
cargo build --release
```

Binary: `target/release/polymarket-arbitrage-bot`

---

## Configuration

Copy or create `config.json` in the project root:

```json
{
  "polymarket": {
    "gamma_api_url": "https://gamma-api.polymarket.com",
    "clob_api_url": "https://clob.polymarket.com",
    "api_key": "YOUR_API_KEY",
    "api_secret": "YOUR_API_SECRET",
    "api_passphrase": "YOUR_PASSPHRASE",
    "private_key": "YOUR_POLYGON_PRIVATE_KEY_HEX",
    "proxy_wallet_address": "0x...",
    "signature_type": 2,
    "ws_url": "wss://ws-subscriptions-clob.polymarket.com"
  },
  "strategy": {
    "soccer_tag_ids": ["100977", "100350"],
    "market_poll_interval_secs": 120,
    "market_fetch_limit": 50,
    "buy_threshold": 0.95,
    "risk_reduce_threshold": 0.13,
    "sell_profit_threshold": 0.99,
    "simulation_mode": true,
    "shares": "100",
    "trade_interval_secs": 60,
    "market_cache_path": "markets_cache.json",
    "market_refresh_interval_secs": 300,
    "live_window_minutes": 135
  }
}
```

| Field | Description |
|-------|-------------|
| `soccer_tag_ids` | Polymarket tag IDs for soccer leagues (UCL, EPL, etc.). |
| `market_poll_interval_secs` | Seconds between checks (e.g. 60–120). |
| `market_refresh_interval_secs` | Seconds between API fetch + cache update (default 300 = 5 min). |
| `market_cache_path` | Path to save/load market data (default `markets_cache.json`). |
| `live_window_minutes` | Minutes after kickoff to treat match as live (default 135 = 2h15). |
| `market_fetch_limit` | Max events per tag when fetching. |
| `buy_threshold` | Max (team + draw) ask to trigger buy (e.g. 0.95). |
| `risk_reduce_threshold` | Price drop fraction to trigger risk exit (e.g. 0.13 = 13%). |
| `sell_profit_threshold` | Combined price ≥ this to sell both for profit (e.g. 0.99). |
| `shares` | Shares per leg. |
| `simulation_mode` | If `true`, no real orders. |

---

## Usage

**Run the bot:**

```bash
cargo run --release
# or
./target/release/polymarket-arbitrage-bot
```

**Custom config:**

```bash
./target/release/polymarket-arbitrage-bot -c /path/to/config.json
```

**Redeem winning positions:**

```bash
./target/release/polymarket-arbitrage-bot --redeem
```

**Logging:** `RUST_LOG=info` or `RUST_LOG=debug`

---

## Disclaimer

This bot is for **educational and research purposes**. Prediction markets and trading carry risk. Use at your own risk and comply with your jurisdiction's laws and Polymarket's terms of service.
