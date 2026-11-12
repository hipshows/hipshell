// Windows 桌面版按 GUI 程序构建, 启动时不再弹出黑色控制台窗口.
#![cfg_attr(all(windows, feature = "desktop"), windows_subsystem = "windows")]

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use hipssh::{backend::Backend, config::ConfigStore};

fn main() {
    // 在启动任何工作线程前设置, 让菜单跟随界面主题, 避免原生菜单残留抢占文件框焦点.
    #[cfg(feature = "desktop")]
    unsafe {
        std::env::set_var("SLINT_NO_MUDA", "1");
    }
    if let Err(err) = run() {
        eprintln!("HiPShell: {err}");
        // 从桌面或 Finder 启动时没有终端可看, 界面模式的启动错误改用对话框提示.
        #[cfg(feature = "desktop")]
        if !std::env::args().skip(1).any(|c| matches!(c.as_str(), "--serve" | "--help" | "--version")) {
            rfd::MessageDialog::new()
                .set_title("HiPShell")
                .set_description(format!("{err:#}"))
                .set_level(rfd::MessageLevel::Error)
                .show();
        }
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let mut z_args: Vec<String> = std::env::args().skip(1).collect();
    attach_console(&z_args);
    // 唯一配置目录: macOS 在 Application Support, Windows 为程序旁的 config.
    let z_dir = ConfigStore::default_directory()?;
    let z_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(4)
        .enable_all()
        .build()?;
    if z_args.first().is_some_and(|c| c == "--version") {
        println!("HiPShell {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
    if z_args.first().is_some_and(|c| c == "--help") {
        println!(
            "HiPShell\n  hipssh [--import <文件或目录> [--allow-mcp]]\n  hipssh --serve    无界面运行, 只提供 MCP (HTTP 方式)"
        );
        return Ok(());
    }
    let z_store = Arc::new(ConfigStore::load(z_dir.clone())?);
    if let Some(z_index) = z_args.iter().position(|c| c == "--import") {
        let z_file = PathBuf::from(z_args.get(z_index + 1).context("--import 缺少文件或目录")?);
        let (z_connections, z_skipped) = z_store.import_finalshell(&z_file, None)?;
        for l_skip in &z_skipped {
            eprintln!("HiPShell: 跳过 {l_skip}");
        }
        if z_args.iter().any(|c| c == "--allow-mcp") {
            // 授权必须来自用户启动参数, 导入文件自身不能为 LLM 授权.
            for mut l_conn in z_connections {
                l_conn.mcp_allowed = true;
                z_store.save(l_conn, None)?;
            }
        }
        z_args.drain(z_index..=z_index + 1);
    }
    z_args.retain(|c| c != "--allow-mcp");
    anyhow::ensure!(
        z_args.is_empty() || z_args == ["--serve"],
        "不支持的启动参数, 使用 --help 查看用法"
    );
    let _lock = lock_instance(&z_dir)?;
    let z_backend = Backend::new(z_store);
    // AI 客户端按网址直连的 MCP 服务; 有界面时失败原因显示在设置页, 不影响其他功能.
    let z_mcp = z_runtime.block_on(z_backend.apply_mcp());
    if z_args == ["--serve"] {
        // 无界面模式只为提供 MCP, 未能启动即失败退出.
        let z_url = z_mcp.map_err(anyhow::Error::msg)?;
        eprintln!("HiPShell 后台已启动, MCP 网址: {z_url}");
        z_runtime.block_on(tokio::signal::ctrl_c())?;
        return Ok(());
    }
    #[cfg(feature = "desktop")]
    {
        let z_result = hipssh::ui::run(
            z_backend,
            z_runtime.handle().clone(),
        );
        // 窗口关闭即退出: 直接丢弃运行时会无限等待仍在执行的阻塞任务 (卡住的 DNS 解析、本地文件读写),
        // 进程留在后台; 最多等 1 秒, 余下的随进程结束.
        z_runtime.shutdown_timeout(Duration::from_secs(1));
        z_result
    }
    #[cfg(not(feature = "desktop"))]
    {
        let _ = z_backend;
        bail!("此构建不含界面, 请使用 --serve")
    }
}

// 单实例: 锁随进程释放, 防止同一配置目录运行两份会话管理器; 刚关闭的旧实例可能还在退出, 最多等 3 秒.
fn lock_instance(c_dir: &Path) -> Result<std::fs::File> {
    std::fs::create_dir_all(c_dir)?;
    let z_lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(c_dir.join("backend.lock"))?;
    for _ in 0..30 {
        if z_lock.try_lock().is_ok() {
            return Ok(z_lock);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    bail!("HiPShell 已在运行, 请使用现有窗口")
}

// GUI 子系统没有控制台; 命令行用法挂到启动它的终端, 让输出可见.
#[cfg(all(windows, feature = "desktop"))]
fn attach_console(c_args: &[String]) {
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn AttachConsole(c_process_id: u32) -> i32;
    }
    const ATTACH_PARENT_PROCESS: u32 = u32::MAX;
    if !c_args.is_empty() {
        unsafe {
            AttachConsole(ATTACH_PARENT_PROCESS);
        }
    }
}

#[cfg(not(all(windows, feature = "desktop")))]
fn attach_console(_: &[String]) {}
