pub mod ob {
    tonic::include_proto!("orderbook"); // The string specified here must match the proto package name
}

use futures::stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use std::pin::Pin;
use std::net::ToSocketAddrs;
use tokio::sync::broadcast::Sender;
use tokio_stream::{wrappers::BroadcastStream, StreamExt};
use tonic::{Request, Response, Status, transport::Server};

use ob::orderbook_aggregator_server::{OrderbookAggregator};
use ob::{Summary, Empty};

type OBResult<T> = Result<Response<T>, Status>;
type ResponseStream = Pin<Box<dyn Stream<Item = Result<Summary, Status>> + Send>>;

#[derive(Debug)]
pub struct OBServer {
    tx: Sender<Summary>
}

impl OBServer {
    pub fn new(tx: Sender<Summary>) -> Self {
        Self {
            tx
        }
    }
}

#[tonic::async_trait]
impl OrderbookAggregator for OBServer {
    type BookSummaryStream = ResponseStream;
    async fn book_summary(&self, empty: Request<Empty>) -> OBResult<Self::BookSummaryStream> {
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        println!("client connected from: {:?}", empty.remote_addr());
        let brx = self.tx.subscribe();

        tokio::spawn(async move {
            let mut rx_stream = BroadcastStream::new(brx);
            while let Some(item) = rx_stream.next().await {
                match item {
                    Ok(summary) => {
                        let result: Result<Summary, _> = Ok(summary);
                        if tx.send(result).await.is_err() {
                            break;
                        }
                    },
                    Err(e) => {
                        println!("Broadcast receive error {e}");
                        break;
                    }
                };
                
            }
            println!("Client disconnected");
        });
        
        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx))
        ))
    }
}

pub async fn run_grpc(tx: Sender<Summary>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let server = OBServer::new(tx);
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
    match Server::builder()
        .add_service(ob::orderbook_aggregator_server::OrderbookAggregatorServer::new(server))
        .serve("[::1]:50051".to_socket_addrs().unwrap().next().unwrap())
        .await {
            Ok(_) => Ok(()),
            Err(e) => Err(e.into())
        }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::net::ToSocketAddrs;
    use tokio::sync::broadcast;
    use tonic::transport::Server;
    use ob::orderbook_aggregator_client::OrderbookAggregatorClient;

    #[tokio::test]
    async fn grpc_test() {

        let (tx, _rx) = broadcast::channel(128);

        let server = OBServer {
            tx: tx.clone()
        };
        Server::builder()
            .add_service(ob::orderbook_aggregator_server::OrderbookAggregatorServer::new(server))
            .serve("[::1]:50051".to_socket_addrs().unwrap().next().unwrap())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_client() {
        println!("About to connect");
        let mut client = OrderbookAggregatorClient::connect("http://[::1]:50051").await.unwrap();
        println!("Create Stream");
        let stream = client.book_summary(Empty {}).await.unwrap().into_inner();

        let mut stream = stream.take(5);

        while let Some(item) = stream.next().await {
            println!("received: {:?}", item.unwrap());
        }

    }
}