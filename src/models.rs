use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    pub token_id: String,
    pub side: String,
    pub size: String,
    pub price: String,
    #[serde(rename = "type")]
    pub order_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderResponse {
    pub order_id: Option<String>,
    pub status: String,
    pub message: Option<String>,
}

/// Order status from GET /order/{id}. Used to verify fill after placing order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderStatusResponse {
    pub status: String,
    #[serde(rename = "original_size")]
    pub original_size: String,
    #[serde(rename = "size_matched")]
    pub size_matched: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedeemResponse {
    pub success: bool,
    pub message: Option<String>,
    pub transaction_hash: Option<String>,
    pub amount_redeemed: Option<String>,
}

/// Position from data-api.polymarket.com/positions (for holding stats and monitoring).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    #[serde(rename = "asset")]
    pub token_id: String,
    #[serde(rename = "conditionId")]
    pub condition_id: Option<String>,
    pub size: f64,
    #[serde(rename = "avgPrice")]
    pub avg_price: Option<f64>,
    #[serde(rename = "eventId")]
    pub event_id: Option<String>,
    #[serde(rename = "eventSlug")]
    pub event_slug: Option<String>,
    pub title: Option<String>,
    pub slug: Option<String>,
    pub outcome: Option<String>,
    pub redeemable: Option<bool>,
}

/// USDC balance and allowance for the trading wallet (for "not enough balance" diagnostics).
#[derive(Debug, Clone, Default)]
pub struct BalanceAllowance {
    pub balance_usdc: f64,
    pub allowance_usdc: f64,
}
