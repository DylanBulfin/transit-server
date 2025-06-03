use std::{
    collections::HashMap,
    convert::Infallible,
    future::{Ready, ready},
    io::Write,
    net::SocketAddr,
    sync::LazyLock,
    time::Duration,
};

use http_body_util::{BodyExt, Full, combinators::WithTrailers};
use hyper::{HeaderMap, Request, Response, body::Bytes, server::conn::http2, service::service_fn};
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::{TokioExecutor, TokioIo, TokioTimer},
};
use tokio::{net::TcpListener, sync::RwLock};
use tonic::transport::Channel;
use transit_server::{
    create_logger,
    error::ScheduleError,
    shared::db_transit::{LastUpdateRequest, schedule_client::ScheduleClient},
};

const GRPC_BASE_URL: &'static str = "http://localhost:50052";
const GRPC_URL_PATH: &'static str = "/db_transit.Schedule/GetSchedule";
const GRPC_FULL_URL: &'static str = "http://localhost:50052/db_transit.Schedule/GetSchedule";

const MAX_CACHE_ENTRIES: u32 = 20;

static GRPC_CLIENT: RwLock<Option<ScheduleClient<Channel>>> = RwLock::const_new(None);
static HTTP_CLIENT: LazyLock<Client<HttpConnector, Full<Bytes>>> = LazyLock::new(|| {
    Client::builder(TokioExecutor::new())
        .pool_timer(TokioTimer::new())
        .pool_idle_timeout(Duration::from_secs(120))
        .http2_only(true)
        .build_http()
});
static LAST_UPDATE: RwLock<u32> = RwLock::const_new(0);
static CACHED_SCHEDULE: LazyLock<RwLock<HashMap<Vec<u8>, (Vec<u8>, HeaderMap, HeaderMap)>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

type BodyType = WithTrailers<Full<Bytes>, Ready<Option<Result<HeaderMap, Infallible>>>>;

create_logger!("./cacher.log");

async fn check_cache_validity() {
    // Send request
    let req = LastUpdateRequest {};

    let mut mclient = GRPC_CLIENT.write().await;

    let mut clearing = false;

    if mclient.is_some() {
        let client = mclient.as_mut().unwrap();
        let resp = client.get_last_update(req).await;

        match resp {
            Ok(rsp) => {
                let timestamp = rsp.into_inner().timestamp;
                match timestamp {
                    Some(time) => {
                        if *LAST_UPDATE.read().await != time {
                            // Server reporting new timestamp, update timestamp and clear the cache
                            clearing = true;
                            *LAST_UPDATE.write().await = time;
                        }
                    }
                    None => {
                        // Server not reporting a timestamp, something has gone wrong. Reset
                        // everything
                        clearing = true;
                        *LAST_UPDATE.write().await = 0
                    }
                }
            }
            Err(_) => clearing = true,
        }
    } else {
        // GRPC Client not set up, clear the cache and move on
        clearing = true;
    }

    if clearing {
        warn(format!("Found an issue, clearing cache")).await;
        CACHED_SCHEDULE.write().await.clear();
    }
}

/// Converts request body into a vector of raw bytes
async fn decode_body(
    mut body: hyper::body::Incoming,
) -> Result<(Vec<u8>, HeaderMap), ScheduleError> {
    let mut v = Vec::new();
    let mut t = HeaderMap::new();

    while let Some(next) = body.frame().await {
        let frame = next?;
        if let Some(chunk) = frame.data_ref() {
            v.write_all(chunk)?;
        } else if let Some(trailer) = frame.trailers_ref() {
            for (hn, hv) in trailer.iter() {
                t.insert(hn, hv.clone());
            }
        }
    }

    Ok((v, t))
}

fn form_response(bvec: &Vec<u8>, headers: HeaderMap, trailers: HeaderMap) -> Response<BodyType> {
    let body = Full::new(Bytes::from_iter(bvec.into_iter().cloned()))
        .with_trailers(ready(Some(Ok(trailers))));
    let mut resp = Response::new(body);

    for (hn, hv) in headers.iter() {
        resp.headers_mut().insert(hn, hv.clone());
    }

    resp
}

async fn add_cached_value(key: Vec<u8>, bvec: Vec<u8>, headers: HeaderMap, trailers: HeaderMap) {
    info(format!("Adding new request to cache")).await;

    let mut cache = CACHED_SCHEDULE.write().await;

    if cache.len() >= MAX_CACHE_ENTRIES as usize {
        warn(format!("Cache is full, refusing to add another entry")).await;
        return;
    }

    cache.insert(key, (bvec, headers, trailers));
}

async fn serve_schedule(
    req: Request<hyper::body::Incoming>,
) -> Result<Response<BodyType>, ScheduleError> {
    if req.uri().path() != GRPC_URL_PATH {
        warn(format!(
            "Rejecting request to endpoint: {:?}",
            req.uri().path()
        ))
        .await;
        return Err(format!("Endpoint not supported: {:?}", req.uri().path()).into());
    }

    check_cache_validity().await;

    let req_headers = req.headers().clone();
    let (req_body, _) = decode_body(req.into_body()).await?;

    if let Some((bvec, headers, trailers)) = CACHED_SCHEDULE.read().await.get(&req_body) {
        info(format!("Cache hit found")).await;

        Ok(form_response(bvec, headers.clone(), trailers.clone()))
    } else {
        // Upstream response
        let mut upstream_req = hyper::Request::builder().method("POST").uri(GRPC_FULL_URL);

        for (hn, hv) in req_headers.iter() {
            upstream_req = upstream_req.header(hn, hv);
        }

        let upstream_req =
            upstream_req.body(Full::new(Bytes::from_iter(req_body.iter().cloned())))?;

        info(format!("Forwarding request upstream: {:?}", upstream_req)).await;

        let upstream_resp = HTTP_CLIENT.request(upstream_req).await?;

        let headers = upstream_resp.headers().clone();
        let (bvec, trailers) = decode_body(upstream_resp.into_body()).await?;

        // Cache value for next time
        add_cached_value(req_body, bvec.clone(), headers.clone(), trailers.clone()).await;

        Ok(form_response(&bvec, headers, trailers))
    }
}

async fn cacher_serve_loop() -> Result<(), ScheduleError> {
    let grpc_client = ScheduleClient::connect(GRPC_BASE_URL).await?;
    *(GRPC_CLIENT.write().await) = Some(grpc_client);

    // Set up server
    let addr: SocketAddr = "[::1]:50051".parse()?;

    // We create a TcpListener and bind it to [::1]:50051
    let listener = TcpListener::bind(addr).await?;

    // We start a loop to continuously accept incoming connections
    loop {
        let (stream, _) = listener.accept().await?;

        // Use an adapter to access something implementing `tokio::io` traits as if they implement
        // `hyper::rt` IO traits.
        let io = TokioIo::new(stream);

        // Spawn a tokio task to serve multiple connections concurrently
        tokio::task::spawn(async move {
            // Handle the connection from the client using HTTP/2 with an executor and pass any
            // HTTP requests received on that connection to the `hello` function
            if let Err(err) = http2::Builder::new(TokioExecutor::default())
                .serve_connection(io, service_fn(serve_schedule))
                .await
            {
                error(format!("Error serving connection: {}", err)).await;
            }
        });
    }
}

#[tokio::main]
async fn main() -> Result<(), ScheduleError> {
    loop {
        let (comp, err) = tokio::select! {
            server = cacher_serve_loop() => ("Cacher", server),
            logger = logger_loop() => ("Logger", logger)
        };

        error(format!("{} thread failed: {:?}", comp, err)).await;
    }
}
