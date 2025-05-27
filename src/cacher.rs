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
use hyper::{
    HeaderMap, Request, Response, Uri,
    body::Bytes,
    header::{CONTENT_TYPE, HOST, HeaderName, HeaderValue},
    server::conn::http2,
    service::service_fn,
};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::RwLock,
};
use transit_server::{
    error::ScheduleError,
    shared::db_transit::{ScheduleRequest, schedule_client::ScheduleClient},
};

static CACHED_SCHEDULE: LazyLock<RwLock<HashMap<Vec<u8>, (Bytes, HeaderMap)>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

async fn hello(
    mut req: Request<hyper::body::Incoming>,
) -> Result<
    Response<WithTrailers<Full<Bytes>, Ready<Option<Result<HeaderMap, Infallible>>>>>,
    ScheduleError,
> {
    let mut req_buf = Vec::new();
    while let Some(next) = req.frame().await {
        let frame = next?;
        if let Some(chunk) = frame.data_ref() {
            let _ = req_buf.write_all(chunk);
        }
    }

    if CACHED_SCHEDULE.read().await.contains_key(&req_buf) {
        println!("Cache hit");
        tokio::time::sleep(Duration::new(1, 0)).await;
        let map = CACHED_SCHEDULE.read().await;
        let (body, trailers) = map.get(&req_buf).unwrap();
        let rsp = Full::new(body.to_owned()).with_trailers(ready(Some(Ok(trailers.clone()))));

        return Ok(Response::new(rsp));
    }

    let executor = TokioExecutor::default();

    println!("{:?}\n{:?}\n{:?}", req.method(), req.headers(), req.uri());
    let uri: Uri = "http://localhost:50052/db_transit.Schedule/GetSchedule"
        .parse()
        .expect("Unable to parse url");

    let host = uri.host().expect("No host for localhost");
    let port = 50052;

    // Set up client
    let addr = format!("{}:{}", host, port);

    let client_stream = TcpStream::connect(addr).await?;
    let client_io = TokioIo::new(client_stream);
    let (mut sender, conn) = hyper::client::conn::http2::handshake(executor, client_io).await?;
    tokio::task::spawn(async move {
        if let Err(err) = conn.await {
            println!("Connection failed: {:?}", err);
        }
    });

    let authority = uri.authority().unwrap().clone();

    let mut headers: Vec<(HeaderName, HeaderValue)> = Vec::new();

    for header in req.headers() {
        headers.push((header.0.clone(), header.1.clone()));
    }

    let mut req = Request::builder()
        .method("POST")
        .uri(uri)
        .header(HOST, authority.as_str())
        .header(CONTENT_TYPE, "application/grpc")
        .body(Full::new(Bytes::from_iter(req_buf.clone().into_iter())))?;

    for header in headers.into_iter() {
        req.headers_mut().insert(header.0, header.1);
    }

    println!("{:?}", req);

    let mut res = sender.send_request(req).await?;

    println!("{:?}", res);

    let mut out_buf: Vec<u8> = Vec::new();
    let mut trailers: HeaderMap = HeaderMap::new();

    while let Some(next) = res.frame().await {
        let frame = next?;
        if let Some(chunk) = frame.data_ref() {
            out_buf.write_all(chunk)?;
        } else if let Some(t) = frame.trailers_ref() {
            for (hn, hv) in t {
                trailers.insert(hn.clone(), hv.clone());
            }
        }
    }

    let future = ready(Some(Ok(trailers.clone())));

    let bytes = Bytes::from_iter(out_buf.into_iter());

    // Cache response for next time
    CACHED_SCHEDULE
        .write()
        .await
        .insert(req_buf, (bytes.clone(), trailers));

    let body = Full::new(bytes);
    // Need to add trailers to response because client depends on status code in them
    let body = body.with_trailers(future);

    let mut rsp = Response::new(body);
    for (hn, hv) in res.headers() {
        rsp.headers_mut().insert(hn.clone(), hv.clone());
    }

    println!("{:?}", rsp.headers());

    Ok(rsp)
}

#[tokio::main]
async fn main() -> Result<(), ScheduleError> {
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
                .serve_connection(io, service_fn(hello))
                .await
            {
                eprintln!("Error serving connection: {}", err);
            }
        });
    }
}
