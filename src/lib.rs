#![allow(dead_code)]

pub mod grpc;
pub mod websockets;

use grpc::ob::{Summary, Level};
use websockets::{NewUpdate, connect_to_websockets};

use std::{
    error::Error
};

use tokio::sync::{
    mpsc,
    broadcast
};

use serde_json::json;

use crate::grpc::run_grpc;

async fn aggregator(mut rx: mpsc::UnboundedReceiver<NewUpdate>, tx: broadcast::Sender<Summary>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {

    let mut top: (Vec<Level>, Vec<Level>) = (Vec::new(), Vec::new());

    while let Some(update) = rx.recv().await {
        let new_bids: Vec<Level> = update.bids
            .iter()
            .map(|&[x, y]| Level {exchange: update.exchange.to_string(), price: x, amount: y})
            .collect();
        let new_asks: Vec<Level> = update.asks
            .iter()
            .map(|&[x, y]| Level {exchange: update.exchange.to_string(), price: x, amount: y})
            .collect();
        
        let mut merged_bids = top.0.clone();
        let mut merged_asks = top.1.clone();
        let old_bids = top.0.clone();
        let old_asks = top.1.clone();

        for x in &new_bids {
            if !merged_bids.contains(x) {
                merged_bids.push(x.clone());
            }
        }

        for x in &new_asks {
            if !merged_asks.contains(x) {
                merged_asks.push(x.clone());
            }
        }

        merged_bids.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap());
        merged_asks.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap());
        top.0 = merged_bids.iter().take(10).cloned().collect();
        top.1 = merged_asks.iter().take(10).cloned().collect();

        let spread = top.1[0].price - top.0[0].price;

        let summary = Summary {
            spread,
            bids: top.0.clone(),
            asks: top.1.clone()
        };

        // println!("{:?}", summary);

        if old_asks != top.1 || old_bids != top.0 {
            if tx.send(summary).is_err() {
                println!("Failed to send new update")
            };
        }
    }

    Ok(())
}

pub async fn start(symbols: String) -> Result<(), Box<dyn Error + Send + Sync>> {

    let (wbs_tx, wbs_rx) = mpsc::unbounded_channel();
    let (grpc_tx, _grpc_rx) = broadcast::channel(128);

    let url = "wss://ws.bitstamp.net";
    let url_2 = "wss://stream.binance.com:9443/ws";
    let bts_channel = format!("order_book_{symbols}");
    let bin_channel = format!("{symbols}@depth20@1000ms");

    let json_req = json!({
        "event": "bts:subscribe",
        "data": {
            "channel": bts_channel
        }
    });
    let json_req_2 = json!({
        "method": "SUBSCRIBE",
        "params": [
          bin_channel,
        ],
        "id": 1
    });

    let grpc = run_grpc(grpc_tx.clone());
    let binance = connect_to_websockets(url, json_req, wbs_tx.clone());
    let bitstamp = connect_to_websockets(url_2, json_req_2, wbs_tx.clone());
    let aggregator = aggregator(wbs_rx, grpc_tx);

    match tokio::try_join!(binance, bitstamp, aggregator, grpc) {
        Ok(_) => Ok(()),
        Err(e) => Err(e.into())
    }

}

#[cfg(test)]
mod tests {
    use crate::start;
    use std::error::Error;

    #[tokio::test]
    async fn todo_test() -> Result<(), Box<dyn Error + Send + Sync>> {
        start(format!("ethbtc")).await
    }
}