//! 本地模拟站点 HTTP 服务：
//!
//! - 端口 base_port     -> 分页 HTML 站 (station-a)
//! - 端口 base_port + 1 -> JSON 站 (station-b)
//!
//! 两个服务共享同一份演示数据（同进程），分别监听。
use std::net::SocketAddr;

use clap::Parser;
use drainage_collector::mock::{router, MockWorld};

#[derive(Parser)]
#[command(name = "mock-server", about = "本地 HTML/JSON 模拟养护站")]
struct Args {
    /// HTML 站监听端口，JSON 站使用该端口 +1。
    #[arg(long, default_value_t = 9100)]
    base_port: u16,
    #[arg(long, default_value = "127.0.0.1")]
    bind: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let world = MockWorld::demo();

    let html_app = router(world.clone(), Box::leak("station-a".into()));
    let json_app = router(world.clone(), Box::leak("station-b".into()));

    let html_addr: SocketAddr = format!("{}:{}", args.bind, args.base_port).parse()?;
    let json_addr: SocketAddr = format!("{}:{}", args.bind, args.base_port + 1).parse()?;

    let html_listener = tokio::net::TcpListener::bind(html_addr).await?;
    let json_listener = tokio::net::TcpListener::bind(json_addr).await?;
    println!(
        "HTML 模拟站 station-a: http://{html_addr}\nJSON 模拟站 station-b: http://{json_addr}\n\
         故障注入: POST /inject/fail?times=N  POST /inject/retry-after?secs=N"
    );

    tokio::try_join!(
        async {
            axum::serve(html_listener, html_app).await?;
            Ok::<_, anyhow::Error>(())
        },
        async {
            axum::serve(json_listener, json_app).await?;
            Ok(())
        }
    )?;
    Ok(())
}
