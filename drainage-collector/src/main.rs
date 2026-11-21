use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

use drainage_collector::api::{router, AppState};
use drainage_collector::config::AppConfig;
use drainage_collector::engine::Engine;
use drainage_collector::fetch::Fetcher;
use drainage_collector::seed::{build_fetcher, seed_reference_data};
use drainage_collector::store::Store;

#[derive(Parser)]
#[command(
    name = "drainage-collector",
    version,
    about = "市政排水入流口下游设施采集服务"
)]
struct Cli {
    /// 配置文件路径（缺省使用内置本地演示配置）。
    #[arg(long, short, global = true)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// 启动 HTTP 服务；启动时自动续跑未完成轮次。
    Serve,
    /// 启动一个采集轮次（不启动服务，适合脚本化演示）。
    Start {
        entry_id: i64,
        /// full 或 incremental
        #[arg(long, default_value = "full")]
        mode: String,
        /// 等待轮次结束后退出。
        #[arg(long)]
        wait: bool,
    },
    /// 确认暂停一个进行中的轮次（停在当前进度，重启后仍保持暂停）。
    Pause { run_id: i64 },
    /// 继续已暂停的轮次（沿用原轮次与截止点）。
    Resume {
        run_id: i64,
        /// 继续后等待轮次结束。
        #[arg(long)]
        wait: bool,
    },
    /// 查询轮次进度。
    Progress { run_id: i64 },
    /// 查询某轮次已核实可达设施。
    Reachable { run_id: i64 },
    /// 查询某入口最近成功轮次的可达设施。
    EntryReachable { entry_id: i64 },
    /// 列出入流口。
    Entries,
    /// 列出站点。
    Stations,
}

const DEFAULT_CONFIG: &str = include_str!("../config/local.toml");

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,drainage_collector=debug".into()),
        )
        .init();

    let cli = Cli::parse();
    let cfg_text = match &cli.config {
        Some(p) => std::fs::read_to_string(p)?,
        None => DEFAULT_CONFIG.to_string(),
    };
    let cfg: AppConfig = toml::from_str(&cfg_text).context("解析配置失败")?;

    let store = Store::connect(&cfg.database.url).await?;
    seed_reference_data(&store, &cfg).await?;
    let fetcher: Arc<Fetcher> = build_fetcher(&cfg).await?;
    let engine = Engine::new(store.clone(), fetcher);

    match cli.command {
        Command::Serve => {
            // 进程重启后接着完成原轮次。
            let resumed = engine.resume_on_boot().await?;
            if !resumed.is_empty() {
                tracing::info!(?resumed, "已恢复未完成轮次并继续执行");
            }
            let addr: SocketAddr = cfg.server.listen.parse()?;
            let app = router(AppState {
                engine: engine.clone(),
                store: store.clone(),
            });
            let listener = tokio::net::TcpListener::bind(addr).await?;
            tracing::info!(%addr, "采集服务已启动");
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown_signal())
                .await?;
            Ok(())
        }
        Command::Start {
            entry_id,
            mode,
            wait,
        } => {
            let m = drainage_collector::model::RunMode::parse(&mode)?;
            let run_id = engine.start_run(entry_id, m).await?;
            println!("已启动轮次 run_id={run_id} mode={mode} entry_id={entry_id}");
            if wait {
                wait_for_finish(&engine, run_id).await?;
            }
            Ok(())
        }
        Command::Pause { run_id } => {
            engine.pause_run(run_id).await?;
            let p = engine.progress(run_id).await?;
            println!("{}", serde_json::to_string_pretty(&p)?);
            Ok(())
        }
        Command::Resume { run_id, wait } => {
            match engine.resume_run(run_id).await? {
                Some(id) => println!("已继续轮次 run_id={id}"),
                None => println!("轮次 {run_id} 未能继续"),
            }
            if wait {
                wait_for_finish(&engine, run_id).await?;
            }
            Ok(())
        }
        Command::Progress { run_id } => {
            let p = engine.progress(run_id).await?;
            println!("{}", serde_json::to_string_pretty(&p)?);
            Ok(())
        }
        Command::Reachable { run_id } => {
            let items = engine.reachable(run_id).await?;
            println!("{}", serde_json::to_string_pretty(&items)?);
            Ok(())
        }
        Command::EntryReachable { entry_id } => match engine.entry_reachable(entry_id).await? {
            Some((run_id, items)) => {
                println!("基于轮次 {run_id} 的已核实可达设施：");
                println!("{}", serde_json::to_string_pretty(&items)?);
                Ok(())
            }
            None => {
                println!("入口 {entry_id} 尚无成功轮次");
                Ok(())
            }
        },
        Command::Entries => {
            println!(
                "{}",
                serde_json::to_string_pretty(&store.list_entries().await?)?
            );
            Ok(())
        }
        Command::Stations => {
            println!(
                "{}",
                serde_json::to_string_pretty(&store.list_stations().await?)?
            );
            Ok(())
        }
    }
}

async fn wait_for_finish(engine: &Engine, run_id: i64) -> Result<()> {
    loop {
        let p = engine.progress(run_id).await?;
        if p.status != "running" {
            println!("轮次结束：status={} reachable={}", p.status, p.reachable);
            if p.status == "paused" {
                return Ok(());
            }
            if p.status == "failed" {
                std::process::exit(2);
            }
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("收到退出信号，停止接收新请求；未完成轮次将在下次启动时续跑");
}
