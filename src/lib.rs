pub mod backend;
pub mod commands;
pub mod config;
pub mod finalshell;
pub mod mcp;
pub mod model;
#[cfg(feature = "desktop")]
mod fonts;
#[cfg(feature = "desktop")]
mod native_drop;
#[cfg(feature = "desktop")]
pub mod ui;
#[cfg(feature = "desktop")]
mod view_state;
