use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use futures::SinkExt;
use hyper::server::Server;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Client, Error, Method, Request, Response, Uri};
use log::info;
use tokio::join;
use tokio::net::{TcpSocket, TcpStream};
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;

type RegistryRef = Arc<Mutex<HashMap<String, String>>>;

enum RequestError {
    Tungstenite(hyper_tungstenite::tungstenite::Error),
    Hyper(hyper::Error),
}

impl From<hyper::Error> for RequestError {
    fn from(from: hyper::Error) -> Self {
        RequestError::Hyper(from)
    }
}

impl From<hyper_tungstenite::tungstenite::Error> for RequestError {
    fn from(from: hyper_tungstenite::tungstenite::Error) -> Self {
        RequestError::Tungstenite(from)
    }
}

fn listen_discovery(registry: RegistryRef, redis_url: String, discovery_channel: String) -> JoinHandle<()> {
    let registry_ref = registry.clone();
    tokio::task::spawn(async move {
        info!("Connecting to redis instance");
        let client = redis::Client::open(redis_url).unwrap();
        let mut connection = client.get_connection().unwrap();
        let mut pub_sub = connection.as_pubsub();
        let _ = pub_sub.psubscribe(discovery_channel);

        loop {
            let msg = pub_sub.get_message().unwrap();
            let payload_raw: String = msg.get_payload().unwrap();
            let payload = payload_raw.replace("\"", "");
            let (cmd_and_process, address) = payload.split_once("/").unwrap();
            let (cmd, process_id) = cmd_and_process.split_once(",").unwrap();

            let address_string = String::from(address);
            let process_id_string = String::from(process_id);

            println!("{} {} -> {}", cmd, process_id, address);

            if cmd == "add" {
                // let (tx, rx): (Sender<()>, Receiver<()>) = tokio::sync::oneshot::channel();
                registry_ref.lock().unwrap().insert(process_id_string, address_string);
            } else if cmd == "remove" {
                let _ = registry_ref.lock().unwrap().remove(&process_id_string);
            }
        }
    })
}

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();

    let registry = Arc::new(Mutex::new(HashMap::new()));
    let redis_url = std::env::var("REDIS_URL").unwrap();
    let discovery_channel = std::env::var("DISCOVERY_CHANNEL").unwrap();

    let listener = std::net::TcpListener::bind("0.0.0.0:8000").unwrap();
    let server = Server::from_tcp(listener).unwrap();

    let registry_ref = registry.clone();
    let service = make_service_fn(move |_| {
        let registry_ref = registry_ref.clone();
        async {
            let response_service = service_fn(move |req| {
                let registry_ref = registry_ref.clone();
                async move { handle_request(registry_ref.clone(), req).await }
            });
            Ok::<_, hyper::Error>(response_service)
        }
    });
    let http_server = server.http1_only(true).serve(service);

    let registry_ref = registry.clone();
    let discovery = listen_discovery(registry_ref, redis_url, discovery_channel);

    let _ = join!(http_server, discovery);
}

async fn handle_request(registry: RegistryRef, request: Request<Body>) -> Result<Response<Body>, RequestError> {
    println!("{} {}", request.method(), request.uri());
    if hyper_tungstenite::is_upgrade_request(&request) {
        let result = handle_websocket(registry, request).await;

        result.map_err(|e| RequestError::Tungstenite(e)).map(|r| r)
    } else {
        let result = handle_http(registry, request).await;

        result.map_err(|e| RequestError::Hyper(e)).map(|r| r)
    }
}

// async fn handle_http() {

// }

async fn handle_http(registry: RegistryRef, request: Request<Body>) -> Result<Response<Body>, hyper::Error> {
    let uri = request.uri().to_string().clone();
    let uri_parts: Vec<&str> = uri.split("/").collect();
    let process_id = uri_parts.get(1).unwrap().to_string();
    let reg = registry.lock().unwrap();
    let address = reg.get(&process_id).cloned();

    let response = match address {
        Some(addr) => {
            let client = Client::new();
            let uri_str = format!("http://{}/{}", addr, request.uri());
            let uri = Uri::from_str(&uri_str).unwrap();
            let result = client.get(uri).await;

            result
        }
        None => {
            let proxy = reg.iter().nth(0);
            let result = match proxy {
                Some((_, value)) => {
                    let client = Client::new();
                    let uri_str = format!("http://{}/{}", value, request.uri());
                    let uri = Uri::from_str(&uri_str).unwrap();
                    let result = match request.method() == Method::POST {
                        true => {
                            let body = request.body();
                            let req = Request::post(uri).body(body).unwrap();
                            client.request(request).await
                        }
                        false => client.get(uri).await,
                    };

                    result
                }
                None => Ok(Response::default()),
            };

            result
        }
    };

    response
}
async fn handle_websocket(registry: RegistryRef, request: Request<Body>) -> Result<Response<Body>, hyper_tungstenite::tungstenite::error::Error> {
    let uri = request.uri().to_string().clone();
    let uri_parts: Vec<&str> = uri.split("/").collect();
    let process_id = uri_parts.get(1).unwrap().to_string();
    let room_id = uri_parts.get(2).unwrap().to_string();
    let address = registry.lock().unwrap().get(&process_id).cloned();

    match address {
        Some(addr) => {
            let (response, income_stream) = hyper_tungstenite::upgrade(request, None).unwrap();
            tokio::spawn(async move {
                let out_stream = TcpStream::connect(addr.clone()).await.unwrap();
                let url = format!("ws://{}{}{}", addr, process_id, room_id);
                let (mut out_stream, _response) = tokio_tungstenite::client_async(url, out_stream).await.unwrap();
                let mut income_stream = income_stream.await.unwrap();
                loop {
                    let reply = income_stream.next().await.unwrap().unwrap();
                    let _ = out_stream.send(reply).await.unwrap();
                    let message = out_stream.next().await.unwrap().unwrap();
                    let _ = income_stream.send(message).await;
                }
            });

            Ok(response)
        }
        None => Ok(Response::default()),
    }
}
