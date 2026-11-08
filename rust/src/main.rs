// Copyright 2026 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, bail};
use bytes::Bytes;
use clap::Parser;
use http_body_util::Full;
use hyper::{Response, server::conn::http1, service::service_fn};
use hyper_util::rt::{TokioIo, TokioTimer};
use s3s::{Body, auth::SimpleAuth, service::S3ServiceBuilder};
use std::{
    convert::Infallible,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{net::TcpListener, sync::Semaphore, task::JoinSet};
use zapfs_rs::{api::Api, store::Store};

#[derive(Parser)]
#[command(version, about = "Experimental single-node ZapFS S3 server")]
struct Args {
    #[arg(long, default_value = "./data")]
    data_dir: PathBuf,
    #[arg(long, default_value = "127.0.0.1:8082")]
    listen: SocketAddr,
    #[arg(long, default_value = "127.0.0.1:8085")]
    admin_listen: SocketAddr,
    #[arg(env = "ZAPFS_ACCESS_KEY_ID", hide_env_values = true)]
    access_key: String,
    #[arg(env = "ZAPFS_SECRET_ACCESS_KEY", hide_env_values = true)]
    secret_key: String,
    #[arg(long, default_value_t = 1_073_741_824)]
    max_object_bytes: u64,
    #[arg(long, default_value_t = 32)]
    max_connections: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "zapfs_rs=info".into()),
        )
        .init();
    let args = Args::parse();
    if !args.listen.ip().is_loopback() || !args.admin_listen.ip().is_loopback() {
        bail!("This first implementation binds only to loopback addresses");
    }
    if args.access_key.is_empty() || args.secret_key.is_empty() {
        bail!("Credentials must not be empty");
    }
    if args.max_connections == 0
        || args.max_connections > 1024
        || args.max_object_bytes == 0
        || args.max_object_bytes > i64::MAX as u64
    {
        bail!("Invalid resource limits");
    }
    let store =
        Arc::new(Store::open(&args.data_dir, args.max_object_bytes).context("open storage")?);
    let mut builder = S3ServiceBuilder::new(Api::new(store, args.secret_key.as_bytes()));
    builder.set_auth(SimpleAuth::from_single(args.access_key, args.secret_key));
    // With auth configured, s3s' default access policy rejects unsigned requests.
    let service = builder.build();
    let listener = TcpListener::bind(args.listen).await?;
    let admin = TcpListener::bind(args.admin_listen).await?;
    eprintln!("S3 endpoint: http://{}", listener.local_addr()?);
    eprintln!("Admin endpoint: http://{}", admin.local_addr()?);
    let requests = Arc::new(AtomicU64::new(0));
    let slots = Arc::new(Semaphore::new(args.max_connections));
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            result=listener.accept()=>{
                let (stream,_)=result?;
                // Reject excess connections instead of building an unbounded queue.
                let Ok(permit)=slots.clone().try_acquire_owned() else { drop(stream); continue };
                let service=service.clone(); let count=requests.clone();
                connections.spawn(async move {
                    let _permit=permit;
                    let handler=service_fn(move |req:hyper::Request<hyper::body::Incoming>| {
                        let service=service.clone(); let count=count.clone();
                        async move {
                            count.fetch_add(1,Ordering::Relaxed);
                            let mut response=service.call(req.map(Body::from)).await?;
                            response.headers_mut().insert("x-amz-request-id",uuid::Uuid::new_v4().to_string().parse().expect("UUID is a valid header"));
                            Ok::<_,s3s::HttpError>(response)
                        }
                    });
                    let mut protocol=http1::Builder::new();
                    protocol.keep_alive(false).max_headers(100).max_buf_size(32*1024)
                        .timer(TokioTimer::new()).header_read_timeout(Duration::from_secs(10));
                    let result=tokio::time::timeout(Duration::from_secs(60),protocol.serve_connection(TokioIo::new(stream),handler)).await;
                    if !matches!(result,Ok(Ok(()))) { tracing::debug!("S3 connection closed or timed out"); }
                });
            },
            result=admin.accept()=>{
                let (stream,_)=result?;
                let Ok(permit)=slots.clone().try_acquire_owned() else { drop(stream); continue };
                let count=requests.clone();
                connections.spawn(async move {
                    let _permit=permit;
                    let handler=service_fn(move |req:hyper::Request<hyper::body::Incoming>| {
                        let count=count.clone();
                        async move {
                            let (status,body,content_type)=match (req.method().as_str(),req.uri().path()) {
                                ("GET","/health")=>(200,"ready\n".to_owned(),"text/plain"),
                                ("GET","/metrics")=>(200,format!("# TYPE zapfs_s3_requests_total counter\nzapfs_s3_requests_total {}\n",count.load(Ordering::Relaxed)),"text/plain; version=0.0.4"),
                                _=>(501,"Admin credential lifecycle is not implemented in this slice\n".to_owned(),"text/plain"),
                            };
                            Ok::<_,Infallible>(Response::builder().status(status).header("content-type",content_type).body(Full::new(Bytes::from(body))).expect("static response"))
                        }
                    });
                    let mut protocol=http1::Builder::new();
                    protocol.keep_alive(false).max_headers(32).max_buf_size(16*1024).timer(TokioTimer::new()).header_read_timeout(Duration::from_secs(5));
                    let _=tokio::time::timeout(Duration::from_secs(10),protocol.serve_connection(TokioIo::new(stream),handler)).await;
                });
            },
            Some(_)=connections.join_next(),if !connections.is_empty()=>{},
            _=tokio::signal::ctrl_c()=>break,
        }
    }
    connections.abort_all();
    while connections.join_next().await.is_some() {}
    Ok(())
}
