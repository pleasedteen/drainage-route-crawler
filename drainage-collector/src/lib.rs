//! 市政排水入流口下游设施采集服务。
//!
//! 模块概览：
//! - [`model`]：领域模型
//! - [`store`]：PostgreSQL / SQLite 双后端持久层（sqlx Any）
//! - [`boundary`]：站点访问边界（页键台账定位、自动跳转同源限制）
//! - [`fetch`]：每站频率/并发控制、退避重试、Retry-After 暂停
//! - [`engine`]：轮次编排、截止点固化、全量/增量遍历、断点续跑
//! - [`parse`]：分页 HTML 与 JSON 页面解析
//! - [`mock`]：本地 HTML / JSON 模拟站点
//! - [`api`]：值班员操作与查询的 HTTP 接口
//! - [`config`] / [`seed`]：配置加载与演示数据

pub mod api;
pub mod boundary;
pub mod config;
pub mod engine;
pub mod fetch;
pub mod mock;
pub mod model;
pub mod parse;
pub mod seed;
pub mod store;

pub use engine::Engine;
pub use store::Store;
