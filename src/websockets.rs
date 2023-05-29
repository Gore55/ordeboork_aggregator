use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures::{stream::StreamExt, SinkExt};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;

#[allow(non_snake_case)]
#[derive(Serialize, Deserialize)]
struct BinanceResponse {
    lastUpdateId: f64,
    bids:  Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

#[derive(Serialize, Deserialize)]
struct BitstampResponse {
    data: BitstampData
}

#[derive(Serialize, Deserialize)]
struct BitstampData {
    bids:  Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
    timestamp: String,
    microtimestamp: String
}

#[derive(Debug, Clone, Copy)]
pub enum Exchange {
    Binance,
    Bitstamp
}

impl Exchange {
    pub fn to_string(&self) -> String {
        match self{
            Self::Binance => format!("Binance"),
            Self::Bitstamp => format!("Bitstamp")
        } 
    }
}

#[allow(unused)]
#[derive(Debug)]
pub struct NewUpdate {
    pub exchange: Exchange,
    pub bids: Vec<[f64; 2]>,
    pub asks: Vec<[f64; 2]>
}

impl NewUpdate {
    fn from(exchange: &Exchange, res: Message) -> Self {
        let text = res.into_text().expect("This shouldn't fail");
        let (bids, asks) = match exchange {
            Exchange::Binance => {
                let response: BinanceResponse = serde_json::from_str(&text).unwrap();
                let bids: Vec<[f64; 2]> = response.bids
                    .into_iter()
                    .map(|[s1, s2]| {
                        [s1.parse::<f64>().unwrap(), s2.parse::<f64>().unwrap()]
                    })
                    .collect();
                
                let asks: Vec<[f64; 2]> = response.asks
                    .into_iter()
                    .map(|[s1, s2]| {
                        [s1.parse::<f64>().unwrap(), s2.parse::<f64>().unwrap()]
                    })
                    .collect();

                (bids, asks)
            },
            Exchange::Bitstamp => {
                let _response: BitstampResponse = serde_json::from_str(&text).unwrap();
                let bids_20: Vec<[String; 2]> = _response.data.bids.iter().take(20).cloned().collect();
                let asks_20: Vec<[String; 2]> = _response.data.asks.iter().take(20).cloned().collect();

                let bids: Vec<[f64; 2]> = bids_20.into_iter()
                    .map(|[s1, s2]| {
                        [s1.parse::<f64>().unwrap(), s2.parse::<f64>().unwrap()]
                    })
                    .collect();
            
                let asks: Vec<[f64; 2]> = asks_20.into_iter()
                    .map(|[s1, s2]| {
                        [s1.parse::<f64>().unwrap(), s2.parse::<f64>().unwrap()]
                    })
                    .collect();
                (bids, asks)
            }
        };

        Self {
            exchange: exchange.clone(),
            bids,
            asks
        }
    }
}

impl Exchange {
    fn from_url(url: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if url.find("binance").is_some() {
            Ok(Self::Binance)
        } else if url.find("bitstamp").is_some() {
            Ok(Self::Bitstamp)
        } else {
            Err("Not a valid exchange".into())
        }
    }
}

pub async fn connect_to_websockets(input: &str, req: serde_json::Value, tx: UnboundedSender<NewUpdate>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {

    let exchange = Exchange::from_url(input)?;
    let dest = url::Url::parse(input)?;

    let (mut ws_stream, _) = match connect_async(dest).await {
        Ok(ws_stream) => ws_stream,
        Err(e) => {
            return Err(e.into());
        }
    };

    let message = Message::text(req.to_string());
    
    match ws_stream.send(message).await {
        Ok(_) => {},
        Err(e) => {
            return Err(e.into())
        }
    };

    let mut first = true;
    while let Some(Ok(msg)) = ws_stream.next().await {
        
        if first {
            first = false;
            continue
        }

        if msg.is_ping() {

            if ws_stream.send(Message::Pong(Vec::new())).await.is_err() {
                println!("Failed to connect to the exchange");
            };

        } else {

            if msg.is_text() {
                let newupdate = NewUpdate::from(&exchange, msg);

                if tx.send(newupdate).is_err() {
                    println!("Failed to send new update")
                };
            }
        }
    };

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::sync::mpsc::unbounded_channel;

    #[tokio::test]
    async fn total_test() {

        let json_req = json!({
            "event": "bts:subscribe",
            "data": {
                "channel": "order_book_ethbtc"
            }
        });
        let json_req_2 = json!({
            "method": "SUBSCRIBE",
            "params": [
              "ethbtc@depth20@1000ms",
            ],
            "id": 1
        });
        
        let url = "wss://ws.bitstamp.net";
        let url_2 = "wss://stream.binance.com:9443/ws";

        let (tx, mut rx) = unbounded_channel();

        let a = connect_to_websockets(url, json_req, tx.clone());
        let b = connect_to_websockets(url_2, json_req_2, tx.clone());

        let c = tokio::spawn(async move {
            while let Some(update) = rx.recv().await {
                println!("{:?}", update);
            };
        });

        let _ = tokio::join!(a, b, c);

    }
}