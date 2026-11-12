use crate::{backend::{Backend, local_file_name, local_name_key}, model::Actor};
use serde_json::{Value, json};
use slint::{ComponentHandle, Model, ModelRc, SharedString, VecModel};
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use unicode_width::UnicodeWidthChar;
use zeroize::Zeroize;

// 最近浏览过的目录 ((会话, 路径) → 文件行, 时间列标题), 只在界面线程使用. 进入目录时先显示缓存,
// 同时照常实时读取并替换; 读取完成前不允许文件操作, 覆盖、删除等仍以服务器实际状态为准.
// 目录树收起时保留子树, 再展开先显示, 后台刷新后按路径合并.
// 文件列表最多缓存 128 个目录; 目录树最多显示 2000 个节点, 收起的子树另存最多 4000 个, 1 分钟内再展开直接用, 不重新读取.
const FILE_CACHE_DIRS: usize = 128;
const FILE_CACHE_ROWS: usize = 100_000;
// 文件列表缓存按文字估算的字节上限 (含目录树隐藏子树另计).
const FILE_CACHE_BYTES: usize = 32 * 1024 * 1024;
const MAX_TREE_NODES: usize = 2000;
// 一次上传或下载最多选择的项数, 每项一个任务, 需小于后端任务队列上限.
const MAX_TRANSFER_ITEMS: usize = 32;
// 终端选中复制的上限.
const MAX_COPY_BYTES: usize = 1024 * 1024;
// 保存与恢复窗口大小的下限 (逻辑像素), 低于它视为异常 (最小化、错误关闭等).
const MIN_WINDOW_WIDTH: f32 = 800.0;
const MIN_WINDOW_HEIGHT: f32 = 540.0;
const TREE_CACHE_TTL: Duration = Duration::from_secs(60);
thread_local! {
    // ((会话, 路径), 文件行, 时间列标题, 估算字节数)
    static FILE_CACHE: RefCell<std::collections::VecDeque<((String, String), Vec<FileRow>, SharedString, usize)>> = RefCell::default();
    static TREE_CACHE: RefCell<HashMap<(String, String), (Vec<DirectoryRow>, Instant)>> = RefCell::default();
    // 目录树各节点子目录最后一次实时读取成功的时间; 收起时随子树保存, 不因收起续期.
    static TREE_LOADED: RefCell<HashMap<(String, String), Instant>> = RefCell::default();
    static TREE_REQUEST: Cell<u64> = const { Cell::new(0) };
    // 刚上传完成的 (会话, 路径): 列表刷新后选中并定位; 批量上传时后完成的覆盖先完成的.
    static FILE_FOCUS: RefCell<Option<(String, String)>> = const { RefCell::new(None) };
    static CONFLICT_ANSWER: RefCell<Option<tokio::sync::oneshot::Sender<i32>>> = const { RefCell::new(None) };
    // 设置 → 外观 "显示分组": 服务器名称前是否加 [分组], 标签、快速连接、状态栏、任务等处统一.
    static SHOW_GROUP: Cell<bool> = const { Cell::new(true) };
    // 已见过的传输任务 (新出现的用户任务自动打开传输面板) 与面板中已清除的任务.
    static TRANSFER_SEEN: RefCell<std::collections::HashSet<String>> = RefCell::default();
    static TRANSFER_CLEARED: RefCell<std::collections::HashSet<String>> = RefCell::default();
}

pub(crate) fn invalidate_file_cache(c_ui: &AppWindow, c_session: &str) {
    FILE_CACHE.with_borrow_mut(|c| c.retain(|c| c.0.0 != c_session));
    TREE_CACHE.with_borrow_mut(|c| c.retain(|c, _| c.0 != c_session));
    if c_ui.get_active_session().as_str() == c_session {
        c_ui.set_file_request_generation(c_ui.get_file_request_generation().wrapping_add(1));
        c_ui.set_file_loading_path("".into());
        c_ui.set_tree_loading("".into());
        c_ui.set_remote_ready(false);
        c_ui.set_selected_remote("".into());
        c_ui.set_selected_files_count(0);
        c_ui.set_selected_file_paths("".into());
        TREE_REQUEST.with(|c| c.set(c.get().wrapping_add(1)));
    }
}

// 只让发生变化的目录失效 (上传、改名、删除等), 其他目录的缓存继续可用; 正在浏览该目录时重新读取.
// 其他标签的变化仍按整个会话失效, 切回时重新读取.
fn invalidate_dirs(c_ui: &AppWindow, c_session: &str, c_dirs: &[String]) {
    if c_ui.get_active_session().as_str() != c_session {
        c_ui.invoke_action("invalidate_files".into(), c_session.into());
        return;
    }
    // 收起的子树以祖先目录为键保存, 祖先的缓存中也含有变化的目录, 一并失效.
    let z_affects = |c_key: &str| c_dirs.iter().any(|c| c == c_key || c_key == "/" || c.starts_with(&format!("{}/", c_key.trim_end_matches('/'))));
    FILE_CACHE.with_borrow_mut(|c| c.retain(|c| c.0.0 != c_session || !c_dirs.contains(&c.0.1)));
    TREE_CACHE.with_borrow_mut(|c| c.retain(|c, _| c.0 != c_session || !z_affects(&c.1)));
    TREE_LOADED.with_borrow_mut(|c| c.retain(|c, _| c.0 != c_session || !c_dirs.contains(&c.1)));
    // 界面上已展开的变化目录 (包括正在浏览的) 收起并去掉旧子节点: 不依赖随后的列表读取成功, 用户切走或读取失败时,
    // 收起上级目录也不会把旧子树存回缓存. 正在浏览的目录在列表读取完成后会重新展开为最新内容.
    let mut z_nodes = c_ui.get_directory_tree().iter().collect::<Vec<_>>();
    let mut z_changed = false;
    for z_dir in c_dirs {
        let Some(i) = z_nodes.iter().position(|c| c.path.as_str() == z_dir && c.expanded) else { continue };
        let l_depth = z_nodes[i].depth;
        let l_end = z_nodes.iter().enumerate().skip(i + 1).find(|(_, c)| c.depth <= l_depth).map(|(k, _)| k).unwrap_or(z_nodes.len());
        z_nodes.drain(i + 1..l_end);
        z_nodes[i].expanded = false;
        z_changed = true;
    }
    if z_changed {
        c_ui.set_directory_tree(ModelRc::new(VecModel::from(z_nodes)));
    }
    // 废弃在途的旧读取: 变化前发出的列表、补全回包到达时不再写回旧内容, 随后重新读取.
    if c_dirs.iter().any(|c| c.as_str() == c_ui.get_browse_path().as_str()) {
        c_ui.set_file_request_generation(c_ui.get_file_request_generation().wrapping_add(1));
        c_ui.set_file_loading_path("".into());
        c_ui.set_remote_ready(false);
        c_ui.set_remote_listed_session("".into());
    }
    if c_dirs.iter().any(|c| c.as_str() == c_ui.get_tree_loading().as_str()) {
        c_ui.set_tree_loading("".into());
        TREE_REQUEST.with(|c| c.set(c.get().wrapping_add(1)));
    }
}

fn parent_dir(c_path: &str) -> String {
    match c_path.trim_end_matches('/').rsplit_once('/') {
        Some(("", _)) | None => "/".into(),
        Some((l_parent, _)) => l_parent.into(),
    }
}

fn cache_file_rows(c_ui: &AppWindow) {
    let z_key = (c_ui.get_active_session().to_string(), c_ui.get_browse_path().to_string());
    let z_rows = c_ui.get_files().iter().map(|c| FileRow { selected: false, ..c }).collect::<Vec<_>>();
    let z_label = c_ui.get_file_time_label();
    let z_bytes = z_rows
        .iter()
        .map(|c| 96 + c.path.len() + c.label.len() + c.name.len() + c.size_text.len() + c.kind_text.len() + c.modified_text.len() + c.permissions.len() + c.owner.len())
        .sum::<usize>();
    FILE_CACHE.with_borrow_mut(|c_cache| {
        c_cache.retain(|c| c.0 != z_key);
        c_cache.push_back((z_key, z_rows, z_label, z_bytes));
        // 目录数、总行数、估算字节数都有上限, 超出时先淘汰最久未看的目录.
        while c_cache.len() > FILE_CACHE_DIRS
            || (c_cache.len() > 1
                && (c_cache.iter().map(|c| c.1.len()).sum::<usize>() > FILE_CACHE_ROWS
                    || c_cache.iter().map(|c| c.3).sum::<usize>() > FILE_CACHE_BYTES))
        {
            c_cache.pop_front();
        }
    });
}

fn cached_file_rows(c_session: &str, c_path: &str) -> Option<(Vec<FileRow>, SharedString)> {
    FILE_CACHE.with_borrow(|c_cache| {
        c_cache.iter().find(|c| c.0.0 == c_session && c.0.1 == c_path).map(|c| (c.1.clone(), c.2.clone()))
    })
}

include!(concat!(env!("OUT_DIR"), "/app_ui.rs"));

// 视图只处理展示和用户输入, 会话、凭据、传输及权限全部由后台管理.
pub fn run(
    c_backend: Arc<Backend>,
    c_runtime: tokio::runtime::Handle,
) -> anyhow::Result<()> {
    let z_ui = AppWindow::new()?;
    // 对外显示主次版本, 如 Ver.1.0; 版本号只在 Cargo.toml 维护.
    z_ui.set_app_version(concat!("Ver.", env!("CARGO_PKG_VERSION_MAJOR"), ".", env!("CARGO_PKG_VERSION_MINOR")).into());
    let z_settings = c_runtime.block_on(c_backend.call("get_settings", json!({}), Actor::User))?;
    let z_dark = z_settings
        .get("dark_theme")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    z_ui.set_theme_index(if z_dark { 0 } else { 1 });
    z_ui.set_is_macos(cfg!(target_os = "macos"));
    restore_layout(&z_ui, z_settings.get("layout").unwrap_or(&Value::Null));
    // 恢复默认布局 (面板已在界面复位): 取消最大化, 窗口回到默认大小并在当前显示器居中, 保存的布局清为默认.
    let (z_backend, z_runtime, z_weak) = (c_backend.clone(), c_runtime.clone(), z_ui.as_weak());
    z_ui.on_reset_window(move || {
        let Some(z_ui) = z_weak.upgrade() else { return; };
        let z_window = z_ui.window();
        z_window.set_maximized(false);
        z_window.set_size(slint::LogicalSize::new(1240.0, 830.0));
        use slint::winit_030::WinitWindowAccessor;
        z_window.with_winit_window(|c_window| {
            let Some(l_monitor) = c_window.current_monitor() else { return; };
            let (l_origin, l_screen, l_scale) = (l_monitor.position(), l_monitor.size(), c_window.scale_factor());
            let l_x = l_origin.x + (l_screen.width as i32 - (1240.0 * l_scale) as i32).max(0) / 2;
            let l_y = l_origin.y + (l_screen.height as i32 - (830.0 * l_scale) as i32).max(0) / 2;
            c_window.set_outer_position(slint::winit_030::winit::dpi::PhysicalPosition::new(l_x, l_y));
        });
        // 分组展开状态不属于窗口布局, 恢复默认时保留.
        let z_default = json!({"width":0,"height":0,"maximized":false,"monitor_expanded":false,"monitor_width":260,"bottom_collapsed":true,"bottom_height":232,"bottom_page":0,"expanded_groups":current_expanded_groups(&z_ui)});
        match z_runtime.block_on(z_backend.call("set_layout", json!({"layout": z_default}), Actor::User)) {
            Ok(_) => notify(&z_ui, NOTE_OK, "已恢复默认布局"),
            Err(err) => notify(&z_ui, NOTE_ERROR, format!("保存布局失败: {err:#}")),
        }
    });
    // 关闭窗口先确认, 确认后保存窗口位置大小与面板布局 (下次启动恢复) 再退出.
    // macOS 用 ⌘Q 退出时不经过关闭事件, 事件循环结束后补存布局.
    let z_layout_saved = Arc::new(AtomicBool::new(false));
    let z_asking = Arc::new(AtomicBool::new(false));
    let (z_exit_runtime, z_exit_backend) = (c_runtime.clone(), c_backend.clone());
    let (z_backend, z_runtime, z_weak, z_saved_flag) = (c_backend.clone(), c_runtime.clone(), z_ui.as_weak(), z_layout_saved.clone());
    z_ui.window().on_close_requested(move || {
        // 确认框已打开时再点关闭不重复弹出.
        if !z_asking.swap(true, Ordering::AcqRel) {
            let (l_backend, l_runtime, l_weak, l_saved, l_asking) = (z_backend.clone(), z_runtime.clone(), z_weak.clone(), z_saved_flag.clone(), z_asking.clone());
            z_runtime.spawn(async move {
                let l_answer = rfd::AsyncMessageDialog::new()
                    .set_title("退出 HiPShell")
                    .set_level(rfd::MessageLevel::Info)
                    .set_description("确定退出 HiPShell? 所有连接会断开, 正在进行的传输和命令会中止.")
                    .set_buttons(rfd::MessageButtons::OkCancelCustom("退出".into(), "取消".into()))
                    .show()
                    .await;
                l_asking.store(false, Ordering::Release);
                if matches!(l_answer, rfd::MessageDialogResult::Ok | rfd::MessageDialogResult::Yes)
                    || matches!(&l_answer, rfd::MessageDialogResult::Custom(c) if c == "退出")
                {
                    let _ = l_weak.upgrade_in_event_loop(move |c_ui| {
                        save_layout(&c_ui, &l_runtime, &l_backend);
                        l_saved.store(true, Ordering::Release);
                        let _ = slint::quit_event_loop();
                    });
                }
            });
        }
        slint::CloseRequestResponse::KeepWindowShown
    });
    apply_mcp_settings(
        &z_ui,
        z_settings.get("mcp_enabled").and_then(Value::as_bool).unwrap_or(true),
        z_settings.get("mcp_port").and_then(Value::as_u64).unwrap_or(u64::from(crate::mcp::DEFAULT_PORT)),
        &c_runtime.block_on(c_backend.mcp_status()),
    );
    let z_backend = c_backend.clone();
    let z_runtime = c_runtime.clone();
    let z_weak = z_ui.as_weak();
    // 应用开关与端口: 后台立即启停或换端口, 结果回到设置页显示.
    z_ui.on_mcp_apply(move |c_enabled, c_port| {
        let Some(z_ui) = z_weak.upgrade() else { return; };
        let Some(z_port) = c_port.trim().parse::<u16>().ok().filter(|c| *c >= 1024) else {
            notify(&z_ui, NOTE_ERROR, "端口需在 1024-65535 之间");
            return;
        };
        let (z_backend, z_weak) = (z_backend.clone(), z_weak.clone());
        z_runtime.spawn(async move {
            let z_result = z_backend.set_mcp(c_enabled, z_port).await;
            let _ = z_weak.upgrade_in_event_loop(move |c_ui| {
                let z_status = match z_result {
                    Ok(l_status) => l_status,
                    Err(err) => {
                        notify(&c_ui, NOTE_ERROR, format!("{err:#}"));
                        return;
                    }
                };
                apply_mcp_settings(&c_ui, c_enabled, u64::from(z_port), &z_status);
                match &z_status {
                    Ok(_) => notify(&c_ui, NOTE_OK, "MCP 服务已启动"),
                    Err(_) if !c_enabled => notify(&c_ui, NOTE_OK, "MCP 服务已关闭"),
                    Err(err) => notify(&c_ui, NOTE_ERROR, err.clone()),
                }
            });
        });
    });
    // MCP 自检: 在后台向本机端口握手, 结果回到设置页.
    let z_runtime = c_runtime.clone();
    let z_weak = z_ui.as_weak();
    z_ui.on_mcp_self_check(move || {
        let Some(z_ui) = z_weak.upgrade() else { return; };
        let Ok(z_port) = z_ui.get_mcp_port().parse::<u16>() else { return; };
        z_ui.set_mcp_checking(true);
        let z_weak = z_weak.clone();
        z_runtime.spawn(async move {
            let (z_level, z_title, z_detail) = crate::mcp::self_check(z_port).await;
            let _ = z_weak.upgrade_in_event_loop(move |c_ui| {
                c_ui.set_mcp_checking(false);
                c_ui.set_mcp_check_level(z_level);
                c_ui.set_mcp_check_detail(z_detail.into());
                c_ui.set_mcp_check_title(z_title.into());
            });
        });
    });
    let z_font_dir = crate::fonts::directory(c_backend.data_dir());
    crate::fonts::register_downloaded(&z_font_dir);
    apply_fonts(&z_ui);
    // 状态栏按关键字区分错误提示, 错误保留更久并标红.
    let z_weak = z_ui.as_weak();
    z_ui.on_font_index(move |c_name| {
        z_weak.upgrade().and_then(|c| c.get_font_candidates().iter().position(|l| l == c_name)).map_or(-1, |c| c as i32)
    });
    let z_weak = z_ui.as_weak();
    z_ui.on_font_download(move |c_name| {
        let Some(z_ui) = z_weak.upgrade() else { return; };
        set_font_state(&z_ui, &c_name, 1);
        let z_weak = z_weak.clone();
        let z_dir = z_font_dir.clone();
        // 下载与解压是阻塞的外部命令, 放在独立线程, 完成后回到界面线程注册并选中.
        std::thread::spawn(move || {
            let z_result = crate::fonts::download(&c_name, &z_dir);
            let _ = z_weak.upgrade_in_event_loop(move |c_ui| match z_result {
                Ok(l_files) => {
                    crate::fonts::register_files(&l_files);
                    apply_fonts(&c_ui);
                    // 已选用但此前未安装的字体: 先清空再设回, 让终端按新注册的字体重新测量格子.
                    if c_ui.get_terminal_font() == c_name {
                        c_ui.set_terminal_font("".into());
                        c_ui.set_terminal_font(c_name.clone());
                    }
                    c_ui.set_font_draft_index(c_ui.invoke_font_index(c_name.clone()));
                    c_ui.set_font_draft(c_name.clone());
                    notify(&c_ui, NOTE_OK, format!("{c_name} 已下载, 点击应用即可使用"));
                }
                Err(err) => {
                    set_font_state(&c_ui, &c_name, 0);
                    notify(&c_ui, NOTE_ERROR, format!("{c_name} 下载失败: {err:#}"));
                }
            });
        });
    });
    let z_font = string(&z_settings, "terminal_font");
    if !z_font.is_empty() {
        z_ui.set_terminal_font(z_font.into());
    }
    z_ui.set_term_font_size(number(&z_settings, "terminal_font_size").clamp(9.0, 28.0) as f32);
    let z_backend = c_backend.clone();
    let z_runtime = c_runtime.clone();
    let z_weak = z_ui.as_weak();
    z_ui.on_terminal_font_changed(move |c_family, c_size| {
        dispatch(&z_runtime, &z_backend, z_weak.clone(), "set_terminal_font", json!({"family":c_family.as_str(),"size":c_size}));
    });
    z_ui.set_tab_show_group(z_settings.get("tab_show_group").and_then(Value::as_bool).unwrap_or(true));
    // 未设置或目录已不存在时默认下载到桌面; 用户更改后保存在 settings.json.
    let z_download_dir = Some(string(&z_settings, "download_dir")).filter(|c| !c.is_empty() && std::path::Path::new(c).is_dir()).map(str::to_owned);
    z_ui.set_download_dir(z_download_dir.unwrap_or_else(desktop_dir).into());
    SHOW_GROUP.set(z_ui.get_tab_show_group());
    // 连接管理列宽: 配置中个数不对时用默认; 拖动松开后整组保存.
    let z_columns = z_settings.get("connection_columns").and_then(Value::as_array).filter(|c| c.len() == 5)
        .map(|c| c.iter().map(|l| l.as_u64().unwrap_or(80) as i32).collect::<Vec<_>>())
        .unwrap_or_else(|| crate::config::CONNECTION_COLUMNS.iter().map(|c| *c as i32).collect());
    z_ui.set_conn_col_host(z_columns[0]);
    z_ui.set_conn_col_port(z_columns[1]);
    z_ui.set_conn_col_user(z_columns[2]);
    z_ui.set_conn_col_auth(z_columns[3]);
    z_ui.set_conn_col_proxy(z_columns[4]);
    let z_backend = c_backend.clone();
    let z_runtime = c_runtime.clone();
    let z_weak = z_ui.as_weak();
    z_ui.on_conn_columns_changed(move || {
        let Some(z_ui) = z_weak.upgrade() else { return; };
        let z_columns = [z_ui.get_conn_col_host(), z_ui.get_conn_col_port(), z_ui.get_conn_col_user(), z_ui.get_conn_col_auth(), z_ui.get_conn_col_proxy()];
        dispatch(&z_runtime, &z_backend, z_weak.clone(), "set_connection_columns", json!({"columns":z_columns}));
    });
    z_ui.set_terminal_history_limit(number(&z_settings, "terminal_scrollback") as i32);
    let z_backend = c_backend.clone();
    let z_runtime = c_runtime.clone();
    let z_weak = z_ui.as_weak();
    z_ui.on_terminal_history_changed(move |c_lines| {
        dispatch(&z_runtime, &z_backend, z_weak.clone(), "set_terminal_scrollback", json!({"lines":c_lines}));
    });
    let z_state = Arc::new(Mutex::new(Value::Null));
    // 已渲染的终端画面: (会话, 序号, 滚动偏移).
    let z_sequence = Arc::new(Mutex::new((String::new(), u64::MAX, -1)));
    let z_view_generation = Arc::new(AtomicU64::new(0));
    let z_views = Rc::new(RefCell::new(crate::view_state::SessionViews::default()));
    let z_weak = z_ui.as_weak();
    z_ui.on_conflict_decided(move |c_choice| {
        if let Some(z_ui) = z_weak.upgrade() {
            z_ui.set_conflict_open(false);
            CONFLICT_ANSWER.with_borrow_mut(|c| {
                if let Some(l_answer) = c.take() { let _ = l_answer.send(c_choice); }
            });
            z_ui.invoke_restore_terminal_view();
        }
    });
    let (z_theme_tx, mut z_theme_rx) = tokio::sync::watch::channel(z_dark);
    z_ui.on_theme_changed(move |c_index| {
        let _ = z_theme_tx.send(c_index == 0);
    });
    let z_backend = c_backend.clone();
    let z_weak = z_ui.as_weak();
    // 只保留最新主题选择并顺序保存, 避免快速切换时写入顺序颠倒或阻塞界面.
    c_runtime.spawn(async move {
        while z_theme_rx.changed().await.is_ok() {
            let z_dark = *z_theme_rx.borrow_and_update();
            if let Err(err) = z_backend
                .call("set_theme", json!({"dark_theme":z_dark}), Actor::User)
                .await
            {
                let z_message = format!("主题保存失败: {err}");
                let _ = z_weak.upgrade_in_event_loop(move |c_ui| notify(&c_ui, NOTE_ERROR, z_message));
            }
        }
    });
    let z_snapshot = z_state.clone();
    let z_weak = z_ui.as_weak();
    z_ui.on_monitor_selection_changed(move || {
        let Some(z_ui) = z_weak.upgrade() else {
            return;
        };
        let z_name = if z_ui.get_network_interface_index() > 0 {
            z_ui.get_network_interfaces()
                .row_data(z_ui.get_network_interface_index() as usize)
                .unwrap_or_default()
        } else {
            SharedString::default()
        };
        z_ui.set_network_interface_name(z_name);
        let Ok(z_state) = z_snapshot.lock() else {
            return;
        };
        let z_active = z_ui.get_active_session();
        let z_session = array(&z_state, "sessions")
            .iter()
            .find(|c| string(c, "session_id") == z_active.as_str());
        apply_monitor_processes(&z_ui, z_session);
        apply_monitor_history(&z_ui, z_session);
    });
    let z_answers = Arc::new(Mutex::new(Vec::<String>::new()));
    // 显示分组: 立即按上次状态重建各处服务器名称 (状态没变化时轮询不会重新应用), 再保存设置.
    let z_backend = c_backend.clone();
    let z_runtime = c_runtime.clone();
    let z_weak = z_ui.as_weak();
    let z_last_state = z_state.clone();
    let z_group_answers = z_answers.clone();
    z_ui.on_tab_show_group_changed(move |c_show| {
        SHOW_GROUP.set(c_show);
        if let Some(z_ui) = z_weak.upgrade() {
            let l_state = z_last_state.lock().unwrap().clone();
            if !l_state.is_null() {
                apply_state(&z_ui, &l_state, &z_group_answers);
            }
        }
        dispatch(&z_runtime, &z_backend, z_weak.clone(), "set_tab_show_group", json!({"show":c_show}));
    });
    let z_weak = z_ui.as_weak();
    z_ui.on_file_selection(move |c_file, c_toggle, c_range, c_context| {
        if let Some(z_ui) = z_weak.upgrade() {
            select_remote_files(&z_ui, c_file.path.as_str(), c_toggle, c_range, c_context);
        }
    });
    let z_weak = z_ui.as_weak();
    z_ui.on_file_command(move |c_action, c_file| {
        let Some(z_ui) = z_weak.upgrade() else {
            return;
        };
        if !z_ui.get_remote_ready() || z_ui.get_selected_files_count() != 1 {
            return;
        }
        z_ui.set_file_operation_error("".into());
        if c_action == "permissions" {
            let z_mode = c_file.mode as u32;
            let (z_owner, z_group) = c_file.owner.as_str().split_once('/').unwrap_or(("—", "—"));
            z_ui.set_permissions_owner(z_owner.into());
            z_ui.set_permissions_group(z_group.into());
            z_ui.set_owner_read(z_mode & 0o400 != 0);
            z_ui.set_owner_write(z_mode & 0o200 != 0);
            z_ui.set_owner_execute(z_mode & 0o100 != 0);
            z_ui.set_group_read(z_mode & 0o040 != 0);
            z_ui.set_group_write(z_mode & 0o020 != 0);
            z_ui.set_group_execute(z_mode & 0o010 != 0);
            z_ui.set_other_read(z_mode & 0o004 != 0);
            z_ui.set_other_write(z_mode & 0o002 != 0);
            z_ui.set_other_execute(z_mode & 0o001 != 0);
            z_ui.set_permissions_path(c_file.path);
            z_ui.set_permissions_session(z_ui.get_active_session());
            z_ui.set_permissions_editing(true);
        } else if c_action == "rename" {
            z_ui.set_rename_path(c_file.path);
            z_ui.set_rename_name(c_file.name);
            z_ui.set_rename_session(z_ui.get_active_session());
            z_ui.set_rename_editing(true);
        }
    });
    let z_weak = z_ui.as_weak();
    z_ui.on_terminal_selection(move |c_row, c_col, c_end_row, c_end_col, c_word| {
        if let Some(z_ui) = z_weak.upgrade() {
            select_terminal_text(&z_ui, c_row, c_col, c_end_row, c_end_col, c_word);
        }
    });
    z_ui.on_accepts_files(|c_data| c_data.has_file_paths());
    let z_weak = z_ui.as_weak();
    let z_backend = c_backend.clone();
    let z_runtime = c_runtime.clone();
    z_ui.on_drop_files(move |c_data| {
        let Some(z_ui) = z_weak.upgrade() else {
            return;
        };
        if !z_ui.get_remote_ready() {
            return;
        }
        let Ok(z_paths) = c_data.file_paths() else {
            notify(&z_ui, NOTE_ERROR, "无法读取拖拽文件路径");
            return;
        };
        let z_paths = z_paths
            .take(MAX_TRANSFER_ITEMS + 1)
            .map(std::path::Path::to_path_buf)
            .collect::<Vec<_>>();
        upload_paths(
            &z_runtime,
            &z_backend,
            z_weak.clone(),
            z_ui.get_active_session().to_string(),
            z_ui.get_browse_path().to_string(),
            z_paths,
        );
    });
    crate::native_drop::install(&z_ui);
    let (z_input_tx, mut z_input_rx) = tokio::sync::mpsc::channel::<(String, String, (u64, u64))>(256);
    // 各标签的输入代次: 重连、断开、开关终端时加一. 输入在界面线程入队时就记下代次, 与代次变更先后一致;
    // 之前排队未发出的输入 (如大段粘贴的剩余部分) 随之作废, 不会进入同一标签 ID 下的新终端.
    let z_input_epochs: InputEpochs = Arc::new(Mutex::new(HashMap::new()));
    let z_input: InputQueue = (z_input_tx, z_input_epochs.clone());
    let z_backend = c_backend.clone();
    let z_weak = z_ui.as_weak();
    let z_epochs = z_input_epochs.clone();
    // 输入先进总队列, 再按标签分发到各自的发送任务: 同一标签保持顺序并合并连续键入,
    // 某个标签发送受阻 (大量粘贴或服务器卡住) 时不影响其他标签. 空数据表示标签已关闭, 结束其发送任务.
    c_runtime.spawn(async move {
        let mut z_workers: HashMap<String, tokio::sync::mpsc::Sender<(String, (u64, u64))>> = HashMap::new();
        while let Some((z_session, z_data, z_epoch)) = z_input_rx.recv().await {
            z_workers.retain(|_, c| !c.is_closed());
            // 代次记录保留 (每个关闭的标签只占几十字节): 删除后会回到 0, 旧代次的剩余输入反而又变得有效.
            if z_data.is_empty() {
                z_workers.remove(&z_session);
                continue;
            }
            let z_worker = z_workers.entry(z_session.clone()).or_insert_with(|| {
                let (l_tx, l_rx) = tokio::sync::mpsc::channel::<(String, (u64, u64))>(256);
                tokio::spawn(terminal_input_worker(z_backend.clone(), z_weak.clone(), z_session.clone(), z_epochs.clone(), l_rx));
                l_tx
            });
            if z_worker.try_send((z_data, z_epoch)).is_err() {
                show_notice(z_weak.clone(), NOTE_WARN, "终端输入队列已满, 请稍后重试".into());
            }
        }
    });
    let z_weak = z_ui.as_weak();
    let z_tx = z_input.clone();
    let z_runtime = c_runtime.clone();
    let z_paste_state = z_state.clone();
    z_ui.on_terminal_input(move |c_data, c_pasted| {
        if let Some(z_ui) = z_weak.upgrade() {
            // 粘贴的换行统一为回车: Windows 剪贴板为 \r\n, 原样发送会让每行多执行一次空回车.
            let z_text = if c_pasted { c_data.replace("\r\n", "\r").replace('\n', "\r") } else { c_data.to_string() };
            // 多行粘贴先确认 (全屏程序如 vim 除外): 误粘贴的大段内容在 Shell 里可能逐行执行.
            let z_lines = z_text.trim_end_matches('\r').matches('\r').count() + 1;
            let z_confirm = c_pasted && z_lines > 1 && !z_ui.get_alternate_screen();
            // 发送时按当时的粘贴模式编码 (确认弹窗期间远端可能切换模式).
            let z_encode = move |c_ui: &AppWindow, c_text: String| if c_ui.get_bracketed_paste() && c_pasted {
                // 去掉内容里的结束标记, 防止粘贴文本提前结束粘贴模式后被当作按键执行.
                format!("\u{1b}[200~{}\u{1b}[201~", c_text.replace("\u{1b}[201~", ""))
            } else {
                c_text
            };
            let z_session = z_ui.get_active_session().to_string();
            if !z_confirm {
                if !enqueue_input(&z_tx, z_session, z_encode(&z_ui, z_text), 0) {
                    notify(&z_ui, NOTE_WARN, "终端输入队列已满, 请稍后重试");
                }
                return;
            }
            // 弹窗期间标签可能已重连、终端重开 (界面或 MCP 发起) 或切到别的标签: 记下会话代次、终端代次与输入代次,
            // 确认时不一致就不发送; 终端代次再随输入交给后台核对, 覆盖界面状态尚未刷新的间隙.
            let z_link = |c_state: &Mutex<Value>, c_id: &str| array(&c_state.lock().unwrap(), "sessions").iter()
                .find(|c| string(c, "session_id") == c_id)
                .map(|c| (number(c, "link"), c.get("terminal").and_then(Value::as_u64).unwrap_or(0)))
                .unwrap_or_default();
            let z_mark = (z_link(&z_paste_state, &z_session), z_tx.1.lock().unwrap().get(&z_session).copied().unwrap_or(0));
            let (z_tx, z_weak, z_paste_state) = (z_tx.clone(), z_weak.clone(), z_paste_state.clone());
            z_runtime.spawn(async move {
                let l_answer = rfd::AsyncMessageDialog::new()
                    .set_title("粘贴多行内容")
                    .set_level(rfd::MessageLevel::Warning)
                    .set_description(format!("要粘贴的内容有 {z_lines} 行, 在 Shell 中可能逐行执行. 确定粘贴到终端?"))
                    .set_buttons(rfd::MessageButtons::OkCancelCustom("粘贴".into(), "取消".into()))
                    .show()
                    .await;
                if matches!(l_answer, rfd::MessageDialogResult::Ok | rfd::MessageDialogResult::Yes)
                    || matches!(&l_answer, rfd::MessageDialogResult::Custom(c) if c == "粘贴")
                {
                    let _ = z_weak.upgrade_in_event_loop(move |c_ui| {
                        let l_mark = (z_link(&z_paste_state, &z_session), z_tx.1.lock().unwrap().get(&z_session).copied().unwrap_or(0));
                        if c_ui.get_active_session().as_str() != z_session || l_mark != z_mark {
                            notify(&c_ui, NOTE_WARN, "终端已切换或重新连接, 粘贴已取消");
                            return;
                        }
                        if !enqueue_input(&z_tx, z_session, z_encode(&c_ui, z_text), z_mark.0.1) {
                            notify(&c_ui, NOTE_WARN, "终端输入队列已满, 请稍后重试");
                        }
                    });
                }
            });
        }
    });
    let z_weak = z_ui.as_weak();
    let z_tx = z_input.clone();
    z_ui.on_terminal_wheel(move |c_lines, c_row, c_col| {
        let Some(z_ui) = z_weak.upgrade() else { return false; };
        let z_data = terminal_wheel(c_lines, c_row, c_col, z_ui.get_mouse_mode(), z_ui.get_application_cursor());
        if !enqueue_input(&z_tx, z_ui.get_active_session().to_string(), z_data, 0) {
            notify(&z_ui, NOTE_WARN, "终端输入队列已满, 请稍后重试");
        }
        true
    });
    let z_weak = z_ui.as_weak();
    let z_tx = z_input.clone();
    z_ui.on_terminal_key(move |c_text, c_control, c_alt, c_shift, c_meta| {
        let Some(z_ui) = z_weak.upgrade() else {
            return false;
        };
        // Slint 在 macOS 中把 Command 映射为 control, 真正的 Control 映射为 meta.
        let z_control = if cfg!(target_os = "macos") {
            c_meta
        } else {
            c_control
        };
        let z_command = if cfg!(target_os = "macos") {
            c_control
        } else {
            c_meta
        };
        let z_data = if z_command {
            // macOS 终端习惯: ⌘←/→ 行首行尾, ⌘⌫ 删除到行首, ⌘K 清屏; 其余 Command 组合留给界面快捷键.
            use slint::platform::Key;
            let z_key = |c_key: Key| c_text == SharedString::from(c_key);
            if !cfg!(target_os = "macos") || c_alt || z_control {
                None
            } else if z_key(Key::LeftArrow) {
                Some("\u{1}".to_owned())
            } else if z_key(Key::RightArrow) {
                Some("\u{5}".to_owned())
            } else if z_key(Key::Backspace) {
                Some("\u{15}".to_owned())
            } else if c_text.eq_ignore_ascii_case("k") {
                Some("\u{c}".to_owned())
            } else {
                None
            }
        } else {
            terminal_key(
                &c_text,
                z_control,
                c_alt,
                c_shift,
                z_ui.get_application_cursor(),
                z_ui.get_modified_keys(),
            )
        };
        let Some(z_data) = z_data else {
            return false;
        };
        if !enqueue_input(&z_tx, z_ui.get_active_session().to_string(), z_data, 0) {
            notify(&z_ui, NOTE_WARN, "终端输入队列已满, 请稍后重试");
        }
        true
    });
    let z_weak = z_ui.as_weak();
    let z_snapshot = z_state.clone();
    let (z_backend, z_runtime) = (c_backend.clone(), c_runtime.clone());
    z_ui.on_edit_connection(move |c_id| {
        let Some(z_ui) = z_weak.upgrade() else {
            return;
        };
        let z_state = z_snapshot.lock().unwrap();
        let z_connection = array(&z_state, "connections")
            .iter()
            .find(|c_item| string(c_item, "id") == c_id.as_str())
            .cloned()
            .unwrap_or(json!({}));
        z_ui.set_edit_id(c_id.clone());
        z_ui.set_edit_name(string(&z_connection, "name").into());
        z_ui.set_edit_host(string(&z_connection, "host").into());
        z_ui.set_edit_port(
            z_connection
                .get("port")
                .and_then(Value::as_u64)
                .unwrap_or(22)
                .to_string()
                .into(),
        );
        z_ui.set_edit_user(
            z_connection
                .get("username")
                .and_then(Value::as_str)
                .unwrap_or("root")
                .into(),
        );
        let z_group: SharedString = if c_id.is_empty() {
            z_ui.get_selected_connection_group()
        } else if string(&z_connection, "group").is_empty() {
            "默认".into()
        } else {
            string(&z_connection, "group").into()
        };
        z_ui.set_edit_group_index(z_ui.get_connection_groups().iter().position(|c| c == z_group).unwrap_or(0) as i32);
        z_ui.set_edit_group(z_group);
        z_ui.set_edit_auth(match string(&z_connection, "auth") {
            "private_key" => 1,
            "keyboard_interactive" => 2,
            _ => 0,
        });
        z_ui.set_edit_key_path(string(&z_connection, "private_key_path").into());
        z_ui.set_edit_mcp(boolean(&z_connection, "mcp_allowed"));
        z_ui.set_edit_proxy_id(string(&z_connection, "proxy_id").into());
        refresh_edit_proxy_choices(&z_ui);
        let z_admin = match string(&z_connection, "admin_password") { "login" => 1, "custom" => 2, _ => 0 };
        z_ui.set_edit_admin(z_admin);
        z_ui.set_edit_admin_saved(z_admin == 2);
        // 已有连接回填保存的密码, 密码框可用眼睛按钮查看; 只读本机连接文件, 很快, 直接同步取.
        let z_secrets = if c_id.is_empty() {
            Value::Null
        } else {
            z_runtime.block_on(z_backend.call("connection_secrets", json!({"id":c_id.as_str()}), Actor::User)).unwrap_or_default()
        };
        z_ui.set_edit_password(string(&z_secrets, "password").into());
        z_ui.set_edit_admin_password(string(&z_secrets, "admin_password").into());
        z_ui.set_editing(true);
    });
    let z_weak = z_ui.as_weak();
    let z_backend = c_backend.clone();
    let z_runtime = c_runtime.clone();
    z_ui.on_save_connection(move || {
        let Some(z_ui) = z_weak.upgrade() else { return; };
        let Ok(z_port) = z_ui.get_edit_port().parse::<u16>() else { z_ui.set_form_error("请输入有效端口 (1-65535)".into()); return; };
        let z_id = if z_ui.get_edit_id().is_empty() { uuid::Uuid::new_v4().to_string() } else { z_ui.get_edit_id().to_string() };
        let mut z_args = json!({"connection":{"id":z_id,"name":if z_ui.get_edit_name().trim().is_empty() { z_ui.get_edit_host().to_string() } else { z_ui.get_edit_name().to_string() },"host":z_ui.get_edit_host().to_string(),"port":z_port,"username":z_ui.get_edit_user().to_string(),"group":z_ui.get_edit_group().to_string(),"auth":match z_ui.get_edit_auth(){1=>"private_key",2=>"keyboard_interactive",_=>"password"},"private_key_path":if z_ui.get_edit_key_path().is_empty(){None}else{Some(z_ui.get_edit_key_path().to_string())},"mcp_allowed":z_ui.get_edit_mcp(),"proxy_id":Some(z_ui.get_edit_proxy_id().to_string()).filter(|c| !c.is_empty()),"admin_password":match z_ui.get_edit_admin(){1=>"login",2=>"custom",_=>"none"}}});
        if !z_ui.get_edit_password().is_empty() { z_args["password"] = Value::String(z_ui.get_edit_password().to_string()); }
        if z_ui.get_edit_admin() == 2 && !z_ui.get_edit_admin_password().is_empty() { z_args["admin_password"] = Value::String(z_ui.get_edit_admin_password().to_string()); }
        z_ui.set_edit_password("".into());
        z_ui.set_edit_admin_password("".into());
        dispatch(&z_runtime, &z_backend, z_weak.clone(), "save_connection", z_args);
    });
    let z_weak = z_ui.as_weak();
    let z_backend = c_backend.clone();
    let z_runtime = c_runtime.clone();
    z_ui.on_save_proxy(move || {
        let Some(z_ui) = z_weak.upgrade() else { return; };
        let Ok(z_port) = z_ui.get_proxy_edit_port().trim().parse::<u16>() else { z_ui.set_form_error("请输入有效端口 (1-65535)".into()); return; };
        let mut z_args = json!({"proxy":{"id":z_ui.get_proxy_edit_id().to_string(),"name":z_ui.get_proxy_edit_name().to_string(),"kind":if z_ui.get_proxy_edit_kind() == 1 { "http" } else { "socks5" },"host":z_ui.get_proxy_edit_host().to_string(),"port":z_port,"username":z_ui.get_proxy_edit_user().trim().to_string()}});
        // 密码留空保持原值; 去掉用户名时一并清除密码.
        if !z_ui.get_proxy_edit_password().is_empty() {
            z_args["password"] = Value::String(z_ui.get_proxy_edit_password().to_string());
        } else if z_ui.get_proxy_edit_user().trim().is_empty() {
            z_args["password"] = Value::String(String::new());
        }
        z_ui.set_proxy_edit_password("".into());
        dispatch(&z_runtime, &z_backend, z_weak.clone(), "save_proxy", z_args);
    });
    let z_weak = z_ui.as_weak();
    let z_backend = c_backend.clone();
    let z_runtime = c_runtime.clone();
    z_ui.on_list_drop(move |c_list, c_from, c_to| {
        let Some(z_ui) = z_weak.upgrade() else { return; };
        let z_request = match c_list.as_str() {
            "conn" => connection_drop(&z_ui, c_from as usize, c_to as usize),
            "cmdgroup" => {
                let mut l_groups = z_ui.get_command_groups().iter().map(|c| c.to_string()).collect::<Vec<_>>();
                move_item(&mut l_groups, c_from as usize, c_to as usize).then(|| ("reorder_commands", json!({"groups":l_groups})))
            }
            "cmd" => {
                // 只在当前分组内拖动: 把源命令放到目标位置的命令之前 (或分组最后一条之后), 其余分组顺序不变.
                let l_visible = z_ui.get_visible_commands().iter().map(|c| c.id.to_string()).collect::<Vec<_>>();
                let mut l_all = z_ui.get_quick_commands().iter().map(|c| c.id.to_string()).collect::<Vec<_>>();
                let (l_from, l_to) = (c_from as usize, c_to as usize);
                (l_from < l_visible.len() && l_to <= l_visible.len() && l_to != l_from && l_to != l_from + 1).then(|| {
                    l_all.retain(|c| *c != l_visible[l_from]);
                    let l_at = match l_visible.get(l_to) {
                        Some(l_next) => l_all.iter().position(|c| c == l_next).unwrap_or(l_all.len()),
                        None => l_all.iter().position(|c| *c == l_visible[l_visible.len() - 1]).map_or(l_all.len(), |c| c + 1),
                    };
                    l_all.insert(l_at, l_visible[l_from].clone());
                    ("reorder_commands", json!({"order":l_all}))
                })
            }
            "tab" => {
                // 先在界面上移动标签, 再通知后台按 before (新位置后一个标签) 调整顺序.
                let mut l_rows = z_ui.get_sessions().iter().collect::<Vec<_>>();
                let l_id = l_rows.get(c_from as usize).map(|c| c.id.clone()).unwrap_or_default();
                move_item(&mut l_rows, c_from as usize, c_to as usize).then(|| {
                    let l_at = l_rows.iter().position(|c| c.id == l_id).unwrap_or(0);
                    let l_before = l_rows.get(l_at + 1).map(|c| c.id.to_string()).unwrap_or_default();
                    z_ui.set_sessions(update_rows(z_ui.get_sessions(), l_rows));
                    ("move_tab", json!({"session_id":l_id.as_str(),"before":l_before}))
                })
            }
            _ => None,
        };
        if let Some((z_method, z_args)) = z_request {
            dispatch(&z_runtime, &z_backend, z_weak.clone(), z_method, z_args);
        }
    });
    let z_weak = z_ui.as_weak();
    let z_snapshot = z_state.clone();
    z_ui.on_edit_command(move |c_id| {
        let Some(z_ui) = z_weak.upgrade() else {
            return;
        };
        let z_state = z_snapshot.lock().unwrap();
        let z_command = array(&z_state, "quick_commands")
            .iter()
            .find(|c| string(c, "id") == c_id.as_str());
        z_ui.set_saved_command_id(c_id);
        z_ui.set_saved_command_name(z_command.map(|c| string(c, "name")).unwrap_or("").into());
        let z_group = z_command
            .map(|c| SharedString::from(string(c, "group")))
            .unwrap_or_else(|| z_ui.get_selected_command_group());
        let z_index = z_ui.get_command_groups().iter().position(|c| c == z_group).unwrap_or(0);
        z_ui.set_saved_command_group(z_group);
        z_ui.set_saved_command_group_index(z_index as i32);
        z_ui.set_saved_command_text(z_command.map(|c| string(c, "command")).unwrap_or("").into());
        z_ui.set_command_editing(true);
    });
    let z_weak = z_ui.as_weak();
    let z_backend = c_backend.clone();
    let z_runtime = c_runtime.clone();
    z_ui.on_save_command(move || {
        let Some(z_ui) = z_weak.upgrade() else { return; };
        let z_group = z_ui.get_command_groups().row_data(z_ui.get_saved_command_group_index().max(0) as usize).unwrap_or_else(|| "默认".into());
        let z_args = json!({"command":{"id":z_ui.get_saved_command_id().to_string(),"name":z_ui.get_saved_command_name().to_string(),"group":z_group.to_string(),"command":z_ui.get_saved_command_text().to_string()}});
        dispatch(&z_runtime,&z_backend,z_weak.clone(),"save_saved_command",z_args);
    });
    let z_weak = z_ui.as_weak();
    let z_backend = c_backend.clone();
    let z_runtime = c_runtime.clone();
    let z_auth_answers = z_answers.clone();
    let z_action_tx = z_input.clone();
    let z_view_sequence = z_sequence.clone();
    let z_view_snapshot = z_state.clone();
    let z_activation_generation = z_view_generation.clone();
    z_ui.on_action(move |c_method,c_arg| {
        let Some(z_ui) = z_weak.upgrade() else { return; };
        // 返回上级目录就地改写为读取父目录: 在本回调里再次 invoke_action 会被 Slint 忽略 (回调执行期间处理函数被暂时取出).
        let (c_method, c_arg) = if c_method == "remote_parent" {
            let l_path = z_ui.get_browse_path();
            let l_parent = l_path.trim_end_matches('/').rsplit_once('/').map(|(p, _)| if p.is_empty() { "/" } else { p }).unwrap_or("/");
            (SharedString::from("list_files"), SharedString::from(l_parent))
        } else {
            (c_method, c_arg)
        };
        let z_session = z_ui.get_active_session().to_string();
        let z_arg = c_arg.to_string();
        // 换连接或终端前作废该标签尚未发出的输入; 关闭标签时结束其发送任务.
        if matches!(c_method.as_str(), "connect_tab" | "reconnect" | "connect_tab_files" | "open_terminal" | "close_terminal" | "disconnect_keep" | "disconnect") {
            *z_action_tx.1.lock().unwrap().entry(z_arg.clone()).or_default() += 1;
            if c_method == "disconnect" {
                enqueue_input(&z_action_tx, z_arg.clone(), String::new(), 0);
            }
        }
        if c_method == "show_session" {
            if z_views.borrow_mut().activate(&z_ui, &z_arg) {
                TREE_CACHE.with_borrow_mut(|c| c.retain(|c, _| c.0 == z_arg));
                z_activation_generation.fetch_add(1, Ordering::Relaxed);
                z_ui.set_view_generation(z_ui.get_view_generation().wrapping_add(1));
                z_ui.invoke_restore_terminal_view();
                // 缓存只用于立即显示, 切回后必须重新取一页; 仍保留滚动偏移, 不把历史浏览推到底部.
                *z_view_sequence.lock().unwrap() = (String::new(), u64::MAX, -1);
                z_ui.invoke_terminal_refresh();
                if let Ok(l_state) = z_view_snapshot.try_lock() {
                    let l_session = array(&l_state, "sessions").iter().find(|c| string(c, "session_id") == z_arg);
                    apply_monitor_usage(&z_ui, l_session);
                    apply_monitor_history(&z_ui, l_session);
                    apply_monitor_processes(&z_ui, l_session);
                }
            }
            return;
        }
        if c_method == "retain_views" { z_views.borrow_mut().retain(&z_ui); return; }
        if c_method == "edit_tab_connection" {
            // 先取出连接 ID 并释放状态锁, 编辑回调内还要再锁同一份状态.
            let l_id = z_view_snapshot.lock().ok().and_then(|c| {
                array(&c, "sessions").iter().find(|l| string(l, "session_id") == z_arg).map(|l| string(l, "connection_id").to_owned())
            });
            match l_id.filter(|c| !c.is_empty()) {
                Some(l_id) => z_ui.invoke_edit_connection(l_id.into()),
                None => notify(&z_ui, NOTE_WARN, "该标签没有对应的连接配置"),
            }
            return;
        }
        if c_method == "tree_collapse_all" {
            // 与单个目录收起一样保留子树, 再展开根目录时直接恢复.
            let z_nodes = z_ui.get_directory_tree().iter().collect::<Vec<_>>();
            if let Some(l_root) = z_nodes.first().filter(|_| z_nodes.len() > 1) {
                let l_key = (z_session.clone(), l_root.path.to_string());
                let l_subtree = z_nodes[1..].to_vec();
                let l_loaded = TREE_LOADED.with_borrow(|c| c.get(&l_key).copied()).unwrap_or_else(tree_expired);
                TREE_CACHE.with_borrow_mut(|c| { c.insert(l_key, (l_subtree, l_loaded)); });
            }
            let z_root = z_ui.get_directory_tree().row_data(0).map(|c| DirectoryRow { expanded: false, ..c });
            z_ui.set_directory_tree(ModelRc::new(VecModel::from(z_root.into_iter().collect::<Vec<_>>())));
            z_ui.set_tree_scroll_y(0.0);
            return;
        }
        if c_method == "invalidate_files" || c_method == "discard_files" { z_views.borrow_mut().invalidate_files(&z_ui, &z_arg, c_method == "discard_files"); return; }
        if c_method == "select_command_group" {
            z_ui.set_selected_command_group(z_arg.into());
            z_ui.set_saved_command_id("".into());
            z_ui.set_command_text("".into());
            apply_visible_commands(&z_ui);
            return;
        }
        if c_method == "edit_command_group" || c_method == "edit_connection_group" {
            // 命令分组与连接分组共用同一个名称对话框, 由 group-target 决定保存到哪里.
            z_ui.set_group_target(if c_method == "edit_command_group" { "command" } else { "connection" }.into());
            z_ui.set_group_old_name(z_arg.clone().into());
            z_ui.set_group_name(z_arg.into());
            z_ui.set_group_editing(true);
            return;
        }
        if c_method == "toggle_connection_group" {
            let mut z_groups = z_ui.get_expanded_connection_groups().iter().collect::<Vec<_>>();
            if let Some(i) = z_groups.iter().position(|c| c.as_str() == z_arg) {
                z_groups.remove(i);
            } else {
                z_groups.push(z_arg.into());
            }
            z_ui.set_expanded_connection_groups(ModelRc::new(VecModel::from(z_groups)));
            apply_connection_tree(&z_ui);
            // 立即保存: macOS 用 ⌘Q 等方式退出时可能来不及走关闭时的布局保存.
            dispatch(&z_runtime, &z_backend, z_weak.clone(), "set_expanded_groups", json!({"groups":current_expanded_groups(&z_ui)}));
            return;
        }
        // 传输面板: 在系统文件管理器中显示本机文件或打开下载目录; 清除已结束的传输只在界面隐藏, 底部任务页仍保留.
        match c_method.as_str() {
            "reveal_local" => { reveal_local(&z_ui, &z_arg, true); return; }
            "open_download_dir" => { reveal_local(&z_ui, &z_arg, false); return; }
            "clear_transfers" => {
                TRANSFER_CLEARED.with_borrow_mut(|c| c.extend(z_ui.get_transfers().iter().filter(|l| l.state != 0).map(|l| l.id.to_string())));
                z_ui.set_transfers(update_rows(z_ui.get_transfers(), z_ui.get_transfers().iter().filter(|c| c.state == 0).collect()));
                z_ui.set_transfer_failed(false);
                return;
            }
            _ => {}
        }
        if matches!(c_method.as_str(), "terminal_enter" | "terminal_clear" | "terminal_interrupt") {
            let z_data = match c_method.as_str() { "terminal_clear" => "\u{c}", "terminal_interrupt" => "\u{3}", _ => "\r" };
            if !enqueue_input(&z_action_tx, z_session, z_data.into(), 0) { notify(&z_ui, NOTE_WARN, "终端输入队列已满, 请稍后重试"); }
            return;
        }
        if matches!(c_method.as_str(), "set_permissions" | "rename_file" | "delete_files" | "delete_files_fast" | "create_directory" | "create_file") {
            if z_ui.get_file_operation_busy() { return; }
            z_ui.set_file_operation_error("".into());
            z_ui.set_file_operation_busy(true);
        }
        let (z_method,z_args) = match c_method.as_str() {
            "connect" | "connect_files" => {
                let mut l_args = json!({"connection_id":z_arg,"rows":z_ui.get_terminal_rows(),"cols":z_ui.get_terminal_cols(),"files_only":c_method == "connect_files"});
                if z_ui.get_active_session_empty() && !z_session.is_empty() { l_args["tab_id"] = json!(z_session); }
                ("connect",l_args)
            }
            "new_tab" => ("new_tab", json!({})),
            "clear_recent_connections" => ("clear_recent_connections", json!({})),
            "remove_recent_connection" => ("clear_recent_connections", json!({"connection_id":z_arg})),
            "connect_tab" | "reconnect" | "connect_tab_files" => ("reconnect", json!({"session_id":z_arg,"only_disconnected":c_method != "reconnect","files_only":c_method == "connect_tab_files","rows":z_ui.get_terminal_rows(),"cols":z_ui.get_terminal_cols()})),
            "open_terminal" | "close_terminal" => (c_method.as_str(), json!({"session_id":z_arg})),
            "input_admin_password" => ("input_admin_password", json!({"session_id":&z_session})),
            "disconnect_keep" => ("disconnect", json!({"session_id":z_arg,"keep_tab":true})),
            "remove_connection" => ("remove_connection", json!({"connection_id":z_arg})),
            "focus_session" => ("focus_session",json!({"session_id":z_arg})),
            "disconnect" => ("disconnect",json!({"session_id":z_arg})),
            "trust_once" | "trust_permanent" | "trust_reject" => ("trust_host",json!({"session_id":z_arg,"decision":match c_method.as_str(){"trust_once"=>"once","trust_permanent"=>"permanent",_=>"reject"}})),
            "answer_auth" => {
                let l_answers = std::mem::take(&mut *z_auth_answers.lock().unwrap());
                z_ui.set_auth_session("".into());
                z_ui.set_prompts(ModelRc::default());
                z_ui.set_auth_generation(z_ui.get_auth_generation()+1);
                ("answer_auth",json!({"session_id":z_arg,"answers":l_answers}))
            }
            "run_command" => {
                if z_arg.trim().is_empty() { return; }
                ("run_command",json!({"session_id":z_session,"command":z_arg,"timeout_seconds":30,"max_output_bytes":1048576}))
            }
            "get_task" | "cancel_task" => {
                if c_method=="get_task" { z_ui.set_selected_task(z_arg.clone().into()); }
                (c_method.as_str(),json!({"task_id":z_arg}))
            }
            "list_files" => {
                // 同一目录仍在读取时忽略重复点击, 网络慢时不会排起一串相同请求.
                if z_ui.get_file_loading_path().as_str() == z_arg { return; }
                ("list_files",json!({"session_id":z_session,"path":z_arg}))
            }
            "file_details" => ("file_details",json!({"session_id":z_session,"token":z_arg.parse::<u64>().unwrap_or_default()})),
            "select_all_files" => {
                let mut l_rows = z_ui.get_files().iter().collect::<Vec<_>>();
                for l_file in &mut l_rows { l_file.selected = true; }
                apply_file_selection(&z_ui, l_rows);
                return;
            }
            "clear_file_selection" => {
                let mut l_rows = z_ui.get_files().iter().collect::<Vec<_>>();
                for l_file in &mut l_rows { l_file.selected = false; }
                apply_file_selection(&z_ui, l_rows);
                return;
            }
            // 目录树右键删除文件夹: 与文件列表共用确认框, 固定为快速删除 (rm -rf).
            "prepare_delete_dir" => {
                if !z_ui.get_remote_ready() || z_arg.is_empty() || z_arg == "/" { return; }
                z_ui.set_delete_paths(ModelRc::new(VecModel::from(vec![SharedString::from(z_arg)])));
                z_ui.set_delete_session(z_ui.get_active_session());
                z_ui.set_delete_fast(true);
                z_ui.set_file_operation_error("".into());
                z_ui.set_delete_editing(true);
                return;
            }
            "prepare_delete" => {
                if !z_ui.get_remote_ready() { return; }
                let l_selected = z_ui.get_files().iter().filter(|c| c.selected).collect::<Vec<_>>();
                if l_selected.is_empty() { notify(&z_ui, NOTE_WARN, "请选择要删除的文件"); return; }
                // 按键删除 (key): 单个文件走普通删除; 多选或含目录走快速删除, 非空目录也能删.
                let l_fast = z_arg == "fast" || (z_arg == "key" && (l_selected.len() > 1 || l_selected.iter().any(|c| c.directory)));
                let l_paths = l_selected.into_iter().map(|c| c.path).collect::<Vec<_>>();
                z_ui.set_delete_paths(ModelRc::new(VecModel::from(l_paths)));
                z_ui.set_delete_session(z_ui.get_active_session());
                z_ui.set_delete_fast(l_fast);
                z_ui.set_file_operation_error("".into());
                z_ui.set_delete_editing(true);
                return;
            }
            "delete_files" => (if z_ui.get_delete_fast() { "delete_files_fast" } else { "delete_files" },json!({"session_id":z_ui.get_delete_session().to_string(),"paths":z_ui.get_delete_paths().iter().map(|c|c.to_string()).collect::<Vec<_>>()})),
            "rename_file" => ("rename_file",json!({"session_id":z_ui.get_rename_session().to_string(),"path":z_arg,"new_name":z_ui.get_rename_name().to_string()})),
            "prepare_create_directory" | "prepare_create_file" => {
                if !z_ui.get_remote_ready() { return; }
                z_ui.set_create_directory(c_method == "prepare_create_directory");
                z_ui.set_create_session(z_ui.get_active_session());
                z_ui.set_create_parent(z_ui.get_browse_path());
                z_ui.set_create_name("".into());
                z_ui.set_file_operation_error("".into());
                z_ui.set_create_editing(true);
                return;
            }
            "create_directory" | "create_file" => (c_method.as_str(),json!({"session_id":z_ui.get_create_session().to_string(),"path":z_ui.get_create_parent().to_string(),"name":z_ui.get_create_name().to_string()})),
            "save_command_group" => ("save_command_group",json!({"old_name":z_ui.get_group_old_name().to_string(),"name":z_ui.get_group_name().to_string()})),
            "delete_command_group" => ("delete_command_group",json!({"name":z_arg})),
            "duplicate_file" => {
                // 副本名: 名称-副本N.扩展名, 取当前目录中第一个未被占用的序号.
                let l_names = z_ui.get_files().iter().map(|c| c.name.to_string()).collect::<std::collections::HashSet<_>>();
                let Some(l_file) = z_ui.get_files().iter().find(|c| c.path.as_str() == z_arg) else { return; };
                let (l_stem, l_ext) = match l_file.name.rsplit_once('.') {
                    Some((l_stem, l_ext)) if !l_file.directory && !l_stem.is_empty() => (l_stem.to_owned(), format!(".{l_ext}")),
                    _ => (l_file.name.to_string(), String::new()),
                };
                let Some(l_name) = (1..1000).map(|i| format!("{l_stem}-副本{i}{l_ext}")).find(|c| !l_names.contains(c)) else { return; };
                let l_parent = z_arg.rsplit_once('/').map(|c| c.0).unwrap_or("");
                ("copy_file",json!({"session_id":z_session,"path":z_arg,"new_path":format!("{l_parent}/{l_name}")}))
            }
            "save_connection_group" => ("save_connection_group",json!({"old_name":z_ui.get_group_old_name().to_string(),"name":z_ui.get_group_name().to_string()})),
            "delete_connection_group" => ("delete_connection_group",json!({"name":z_arg})),
            // 参数为 "连接 ID\n代理 ID" 或 "分组名\n代理 ID", 代理 ID 为空表示直连.
            "proxy_connection" | "proxy_group" => {
                let (l_target, l_proxy) = z_arg.split_once('\n').unwrap_or((z_arg.as_str(), ""));
                ("set_connection_proxy", if c_method == "proxy_group" {
                    json!({"group":l_target,"proxy_id":l_proxy})
                } else {
                    json!({"connection_ids":[l_target],"proxy_id":l_proxy})
                })
            }
            "delete_proxy" => {
                // 删除前确认, 并说明使用它的服务器会改为直连.
                let l_users = z_ui.get_connections().iter().filter(|c| c.proxy_id.as_str() == z_arg).count();
                let (l_runtime, l_backend, l_weak, l_id) = (z_runtime.clone(), z_backend.clone(), z_weak.clone(), z_arg.clone());
                z_runtime.spawn(async move {
                    let l_text = if l_users > 0 { format!("有 {l_users} 台服务器在使用这个代理, 删除后它们改为直连.") } else { "删除这个代理?".to_owned() };
                    let l_confirmed = rfd::AsyncMessageDialog::new()
                        .set_title("删除代理")
                        .set_level(rfd::MessageLevel::Warning)
                        .set_description(l_text)
                        .set_buttons(rfd::MessageButtons::OkCancelCustom("删除".into(), "取消".into()))
                        .show()
                        .await;
                    if matches!(l_confirmed, rfd::MessageDialogResult::Ok | rfd::MessageDialogResult::Yes)
                        || matches!(&l_confirmed, rfd::MessageDialogResult::Custom(c) if c == "删除")
                    {
                        dispatch(&l_runtime, &l_backend, l_weak, "delete_proxy", json!({"proxy_id":l_id}));
                    }
                });
                return;
            }
            "tree_toggle" => {
                toggle_directory_tree(&z_runtime, &z_backend, z_weak.clone(), z_arg);
                return;
            }
            "set_permissions" => {
                let l_mode = (u32::from(z_ui.get_owner_read()) << 8)
                    | (u32::from(z_ui.get_owner_write()) << 7) | (u32::from(z_ui.get_owner_execute()) << 6)
                    | (u32::from(z_ui.get_group_read()) << 5) | (u32::from(z_ui.get_group_write()) << 4) | (u32::from(z_ui.get_group_execute()) << 3)
                    | (u32::from(z_ui.get_other_read()) << 2) | (u32::from(z_ui.get_other_write()) << 1) | u32::from(z_ui.get_other_execute());
                ("set_permissions",json!({"session_id":z_ui.get_permissions_session().to_string(),"path":z_arg,"permissions":l_mode}))
            }
            "send_saved_command" | "delete_saved_command" => (c_method.as_str(),json!({"command_id":z_arg,"session_id":z_session})),
            "send_command" => ("send_command",json!({"session_id":z_session,"command":z_arg})),
            _ => return,
        };
        dispatch(&z_runtime,&z_backend,z_weak.clone(),z_method,z_args);
    });
    let z_auth_answers = z_answers.clone();
    z_ui.on_auth_edit(move |c_index, c_text| {
        if let Some(z_answer) = z_auth_answers.lock().unwrap().get_mut(c_index as usize) {
            z_answer.zeroize();
            *z_answer = c_text.to_string();
        }
    });
    let z_weak = z_ui.as_weak();
    let z_backend = c_backend.clone();
    let z_runtime = c_runtime.clone();
    z_ui.on_choose_path(move |c_kind| {
        let Some(z_ui) = z_weak.upgrade() else { return; };
        let z_kind = c_kind.to_string();
        if matches!(z_kind.as_str(), "upload" | "upload_folder" | "download" | "download_to") && !z_ui.get_remote_ready() { return; }
        let z_download_dir = z_ui.get_download_dir().to_string();
        let z_connection = z_ui.get_selected_connection().to_string();
        let z_group = z_ui.get_selected_connection_group().to_string();
        let z_session = z_ui.get_active_session().to_string();
        let z_selected_files = z_ui.get_files().iter().filter(|c| c.selected).collect::<Vec<_>>();
        let z_upload_target = z_ui.get_upload_target().to_string();
        let z_weak = z_weak.clone();
        let z_backend = z_backend.clone();
        z_runtime.spawn(async move {
            // 用户选文件时后台提前建立传输连接, 选完即可开始.
            if matches!(z_kind.as_str(), "upload" | "upload_folder" | "download" | "download_to") {
                let _ = z_backend.call("warm_transfer", json!({"session_id":z_session}), Actor::User).await;
            }
            let z_dialog = rfd::AsyncFileDialog::new().set_title(match z_kind.as_str() {
                "upload" => "选择要上传的文件",
                "upload_folder" => "选择要上传的文件夹",
                "download" | "download_to" => "选择下载保存位置",
                "download_dir" => "选择默认下载目录",
                "key" => "选择私钥文件",
                "export" => "导出所选连接",
                "export_all" => "选择全部连接的导出目录",
                "export_group" => "选择分组的导出目录",
                "import_folder" | "import_folder_all" => "选择连接配置目录",
                "import_commands" => "选择要导入的命令文件",
                "export_commands" => "导出全部命令",
                _ => "选择连接配置文件",
            });
            if matches!(z_kind.as_str(), "upload" | "upload_folder") {
                // 系统对话框不能同时选文件和文件夹 (Windows), 分成两个入口.
                let z_paths = if z_kind == "upload" { z_dialog.pick_files().await } else { z_dialog.pick_folders().await };
                if let Some(z_paths) = z_paths {
                    upload_paths(&tokio::runtime::Handle::current(), &z_backend, z_weak, z_session, z_upload_target, z_paths.into_iter().take(MAX_TRANSFER_ITEMS + 1).map(|c| c.path().to_path_buf()).collect());
                }
                return;
            }
            if matches!(z_kind.as_str(), "download" | "download_to") {
                if z_selected_files.is_empty() || z_selected_files.len() > MAX_TRANSFER_ITEMS {
                    show_notice(z_weak, NOTE_WARN, format!("一次请选择 1～{MAX_TRANSFER_ITEMS} 项下载")); return;
                }
                if z_selected_files.iter().any(|c| c.name.is_empty() || c.name == "." || c.name == ".." || c.name.contains('/') || c.name.chars().any(char::is_control)) {
                    show_notice(z_weak, NOTE_WARN, "下载项的名称不能包含路径或控制字符".into()); return;
                }
                // "下载" 直接存入下载目录 (默认桌面), 目录已不存在时退回选择位置; "下载到…" 总是选择位置.
                let z_default = (z_kind == "download" && !z_download_dir.is_empty())
                    .then(|| std::path::PathBuf::from(&z_download_dir))
                    .filter(|c| c.is_dir());
                // 直接存入下载目录且达到 3 项时先确认目录; "下载到…" 刚选过位置, 不再确认.
                if z_default.is_some() && z_selected_files.len() >= 3 &&!confirm_download(z_weak.clone(), z_selected_files.len()).await { return; }
                // 文件夹、多选和默认目录都存到目录下, 只有单个文件用另存为.
                let z_multiple = z_default.is_some() || z_selected_files.len() > 1 || z_selected_files[0].directory;
                let z_destination = match z_default {
                    Some(l_dir) => l_dir,
                    None => {
                        let l_picked = if z_multiple {
                            z_dialog.set_title("选择下载保存目录").pick_folder().await
                        } else {
                            z_dialog.set_file_name(local_file_name(&z_selected_files[0].name)).save_file().await
                        };
                        let Some(l_picked) = l_picked else { return; };
                        l_picked.path().to_path_buf()
                    }
                };
                // 单个文件的保存对话框已由系统确认替换; 存到目录时同名冲突统一询问一次, 文件夹覆盖即合并到已有文件夹.
                let mut z_overwrite = vec![!z_multiple; z_selected_files.len()];
                let mut z_skip = vec![false; z_selected_files.len()];
                let mut z_targets = z_selected_files
                    .iter()
                    .map(|c| if z_multiple { z_destination.as_path().join(local_file_name(&c.name)) } else { z_destination.as_path().to_path_buf() })
                    .collect::<Vec<_>>();
                if z_multiple {
                    // Windows 把非法字符换成下划线后, 不同远端文件 (如 a:b 与 a?b) 可能得到同一本机名, 批次内先去重.
                    let l_key = |c: &std::path::Path| local_name_key(&c.to_string_lossy());
                    let mut l_seen = std::collections::HashSet::new();
                    for z_target in &mut z_targets {
                        if !l_seen.insert(l_key(z_target)) {
                            let l_name = z_target.file_name().map(|c| c.to_string_lossy().into_owned()).unwrap_or_default();
                            let l_dir = z_destination.as_path().to_path_buf();
                            *z_target = l_dir.join(unique_name(&l_name, |c| l_seen.contains(&l_key(&l_dir.join(c))) || l_dir.join(c).exists()));
                            l_seen.insert(l_key(z_target));
                        }
                    }
                    let l_existing = z_targets.iter().map(|c| c.exists()).collect::<Vec<_>>();
                    let l_conflicts = z_targets.iter().zip(&l_existing).filter(|(_, c)| **c).filter_map(|(c, _)| c.file_name()).map(|c| c.to_string_lossy().into_owned()).collect::<Vec<_>>();
                    if !l_conflicts.is_empty() {
                        let Some(l_choice) = ask_conflict(z_weak.clone(), &l_conflicts).await else { return; };
                        match l_choice {
                            ConflictChoice::Overwrite => z_overwrite = l_existing,
                            ConflictChoice::Skip => z_skip = l_existing,
                            ConflictChoice::KeepBoth => {
                                for z_target in &mut z_targets {
                                    if z_target.exists() {
                                        let l_name = z_target.file_name().map(|c| c.to_string_lossy().into_owned()).unwrap_or_default();
                                        let l_dir = z_destination.as_path().to_path_buf();
                                        *z_target = l_dir.join(unique_name(&l_name, |c| l_dir.join(c).exists() || l_seen.contains(&l_key(&l_dir.join(c)))));
                                        l_seen.insert(l_key(z_target));
                                    }
                                }
                            }
                        }
                    }
                }
                let mut z_count = 0;
                let z_skipped = z_skip.iter().filter(|c| **c).count();
                for (i, (z_file, z_local)) in z_selected_files.iter().zip(z_targets).enumerate() {
                    if z_skip[i] { continue; }
                    let z_result = z_backend.call(if z_file.directory { "download_directory" } else { "download_file" },json!({"session_id":z_session,"local_path":z_local,"remote_path":z_file.path.to_string(),"overwrite":z_overwrite[i],"size_hint":z_file.size_bytes.parse::<u64>().ok()}),Actor::User).await;
                    if let Err(err) = z_result {
                        show_notice(z_weak, NOTE_ERROR, batch_error("下载", z_count, &err)); return;
                    }
                    z_count += 1;
                }
                let _ = z_weak.upgrade_in_event_loop(move |c_ui| notify(&c_ui, NOTE_OK, format!("已加入 {z_count} 个下载任务{}", if z_skipped > 0 { format!(", 已跳过 {z_skipped} 个同名文件") } else { String::new() })));
                return;
            }
            let z_path = match z_kind.as_str() {
                "export" => z_dialog.set_file_name(format!("{z_connection}_connect_config.json")).add_filter("连接配置", &["json"]).save_file().await,
                "export_commands" => z_dialog.set_file_name("hipshell_commands.json").add_filter("命令文件", &["json"]).save_file().await,
                "import_commands" => z_dialog.add_filter("命令文件", &["json"]).pick_file().await,
                "export_all" | "export_group" | "import_folder" | "import_folder_all" | "download_dir" => z_dialog.pick_folder().await,
                _ => z_dialog.pick_file().await,
            };
            let Some(z_path) = z_path else { return; };
            let z_path = z_path.path().to_string_lossy().into_owned();
            let z_call = match z_kind.as_str() {
                "import" | "import_folder" => Some(("import_finalshell",json!({"path":z_path,"group":z_group}))),
                // 顶部菜单导入: 保留文件中原有的分组.
                "import_all" | "import_folder_all" => Some(("import_finalshell",json!({"path":z_path}))),
                "export" => Some(("export_finalshell",json!({"connection_id":z_connection,"path":z_path,"include_password":true}))),
                "export_all" => Some(("export_finalshell",json!({"all":true,"path":z_path,"include_password":true}))),
                "export_group" => Some(("export_finalshell",json!({"all":true,"group":z_group,"path":z_path,"include_password":true}))),
                "import_commands" | "export_commands" => Some((z_kind.as_str(),json!({"path":z_path}))),
                "download_dir" => Some(("set_download_dir",json!({"path":z_path}))),
                _ => None,
            };
            if let Some((z_method,mut z_args)) = z_call {
                let z_result = z_backend.call(z_method,z_args.clone(),Actor::User).await;
                clear_secrets(&mut z_args);
                complete(z_weak,z_method.to_string(),z_result,None);
            } else {
                let _ = slint::invoke_from_event_loop(move || if let Some(z_ui) = z_weak.upgrade() {
                    if z_kind == "key" { z_ui.set_edit_key_path(z_path.into()); }
                });
            }
        });
    });
    let z_busy = Arc::new(AtomicBool::new(false));
    let z_poll_clock = Rc::new(Cell::new(Instant::now() - Duration::from_secs(1)));
    let z_dimensions = Rc::new(RefCell::new((String::new(), 0, 0)));
    // 输出唤醒遇到进行中的刷新时记下, 完成后立即补一次, 不退回定时器周期.
    let z_rerun = Arc::new(AtomicBool::new(false));
    let z_wake_backend = c_backend.clone();
    let z_wake_runtime = c_runtime.clone();
    let z_weak = z_ui.as_weak();
    // 定时器负责状态轮询、尺寸和滚动同步; 终端新输出另由后台通知立即唤醒.
    let z_tick = Rc::new(move |c_wake: bool| {
        let Some(z_ui) = z_weak.upgrade() else {
            return;
        };
        if z_busy.swap(true, Ordering::AcqRel) {
            if c_wake {
                z_rerun.store(true, Ordering::Release);
            }
            return;
        }
        let z_session = z_ui.get_active_session().to_string();
        let z_generation = z_view_generation.load(Ordering::Relaxed);
        let z_empty = z_ui.get_active_session_empty();
        let z_scroll = z_ui.get_terminal_scroll_offset();
        // 只在拖选过程中暂停读取画面; 已有选区时照常刷新, 由 apply_polled_terminal 决定是否保留选区.
        let z_selection = z_ui.get_selection_dragging();
        let z_selected_task = z_ui.get_selected_task().to_string();
        let z_size = (
            z_session.clone(),
            z_ui.get_terminal_rows(),
            z_ui.get_terminal_cols(),
        );
        let z_resize = !z_session.is_empty() && !z_empty && *z_dimensions.borrow() != z_size;
        if z_resize {
            // 终端可显示的列数与行数, 随窗口变化并同步给服务器.
            z_ui.set_terminal_size_text(format!("{} 列 · {} 行", z_size.2, z_size.1).into());
            *z_dimensions.borrow_mut() = z_size.clone();
            if z_scroll == 0 {
                z_sequence.lock().unwrap().0.clear();
            }
        }
        let (z_after_seq, z_read_view) = {
            let z_rendered = z_sequence.lock().unwrap();
            let z_following = z_scroll == 0;
            let z_read_view = z_following || z_rendered.0 != z_session || z_rendered.2 != z_scroll;
            let z_after_seq = if z_following
                && z_rendered.0 == z_session
                && z_rendered.2 == 0
                && z_rendered.1 != u64::MAX
            {
                Some(z_rendered.1)
            } else {
                None
            };
            (z_after_seq, z_read_view)
        };
        let z_poll_state = z_poll_clock.get().elapsed() >= Duration::from_millis(500);
        if z_poll_state {
            z_poll_clock.set(Instant::now());
        }
        if !z_poll_state && (z_session.is_empty() || z_empty) {
            z_busy.store(false, Ordering::Release);
            return;
        }
        let z_backend = c_backend.clone();
        let z_weak = z_weak.clone();
        let z_busy = z_busy.clone();
        let z_state = z_state.clone();
        let z_sequence = z_sequence.clone();
        let z_answers = z_answers.clone();
        let z_refresh_runtime = c_runtime.clone();
        let z_view_generation = z_view_generation.clone();
        let z_rerun = z_rerun.clone();
        c_runtime.spawn(async move {
            if z_resize {
                let _ = z_backend
                    .call(
                        "resize_terminal",
                        json!({"session_id":z_size.0,"rows":z_size.1,"cols":z_size.2}),
                        Actor::User,
                    )
                    .await;
            }
            let z_new_state = if z_poll_state {
                Some(z_backend.call("state", json!({}), Actor::User).await)
            } else {
                None
            };
            let z_terminal = if z_session.is_empty() || z_empty {
                None
            } else {
                let mut z_args =
                    json!({"session_id":z_session,"max_lines":200,"scroll_offset":z_scroll});
                if z_selection || !z_read_view { z_args["events_only"] = json!(true); }
                if let Some(z_seq) = z_after_seq {
                    z_args["after_seq"] = json!(z_seq);
                }
                Some(z_backend.call("read_terminal", z_args, Actor::User).await)
            };
            let z_task = if z_poll_state && !z_selected_task.is_empty() {
                Some(
                    z_backend
                        .call("get_task", json!({"task_id":z_selected_task}), Actor::User)
                        .await,
                )
            } else {
                None
            };
            let z_busy_fallback = z_busy.clone();
            if slint::invoke_from_event_loop(move || {
                // A→B→A 时会话ID仍相同, 也不能应用切换前取到的旧状态; 无论是否应用都要释放忙标志.
                if let Some(z_ui) = z_weak.upgrade().filter(|_| z_view_generation.load(Ordering::Relaxed) == z_generation) {
                    if let Some(z_result) = z_new_state {
                        apply_polled_state(&z_ui, z_result, &z_state, &z_answers, &z_refresh_runtime, &z_backend);
                    }
                    if let Some(Ok(z_value)) = z_terminal {
                        apply_polled_terminal(&z_ui, &z_value, &z_session, z_scroll, &z_sequence);
                    }
                    if let Some(Ok(z_value)) = z_task.filter(|_| z_ui.get_selected_task().as_str() == z_selected_task) {
                        let z_output = task_output(&z_ui, &z_value);
                        if z_ui.get_task_output().as_str() != z_output {
                            z_ui.set_task_output(z_output.into());
                        }
                    }
                }
                z_busy.store(false, Ordering::Release);
                if z_rerun.swap(false, Ordering::AcqRel) {
                    if let Some(l_ui) = z_weak.upgrade() { l_ui.invoke_terminal_refresh(); }
                }
            })
            .is_err()
            {
                z_busy_fallback.store(false, Ordering::Release);
            }
        });
    });
    let z_timer = slint::Timer::default();
    let z_timer_tick = z_tick.clone();
    z_timer.start(slint::TimerMode::Repeated, Duration::from_millis(25), move || z_timer_tick(false));
    z_ui.on_terminal_refresh(move || z_tick(true));
    let z_weak = z_ui.as_weak();
    // 回显到达即刷新; 唤醒间隔至少 8ms, 持续输出时由忙标志按实际处理速度限速, 慢机器自动降频.
    z_wake_runtime.spawn(async move {
        loop {
            z_wake_backend.terminal_changed.notified().await;
            if z_weak.upgrade_in_event_loop(|c_ui| c_ui.invoke_terminal_refresh()).is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(8)).await;
        }
    });
    z_ui.run()?;
    if !z_layout_saved.load(Ordering::Acquire) {
        save_layout(&z_ui, &z_exit_runtime, &z_exit_backend);
    }
    Ok(())
}

fn save_layout(c_ui: &AppWindow, c_runtime: &tokio::runtime::Handle, c_backend: &Arc<Backend>) {
    // 最大化时沿用上次保存的普通窗口位置大小, 取消最大化后回到原来的大小.
    let z_saved = c_runtime.block_on(c_backend.call("get_settings", json!({}), Actor::User)).ok().and_then(|c| c.get("layout").cloned()).unwrap_or(Value::Null);
    if let Err(err) = c_runtime.block_on(c_backend.call("set_layout", json!({"layout": current_layout(c_ui, &z_saved)}), Actor::User)) {
        eprintln!("保存窗口布局失败: {err:#}");
    }
}

// 启动时恢复上次的窗口与面板布局. 窗口在显示前设置, 位置落在已拔掉的显示器上时由 native_drop 移回屏幕内.
fn restore_layout(c_ui: &AppWindow, c_layout: &Value) {
    let z_number = |c_key: &str| c_layout.get(c_key).and_then(Value::as_f64);
    // 尺寸低于下限 (旧版本在最小化时保存等) 时整组忽略, 用默认窗口; 位置为最小化坐标时交给系统摆放.
    if let (Some(l_width), Some(l_height)) = (
        z_number("width").filter(|c| *c >= f64::from(MIN_WINDOW_WIDTH)),
        z_number("height").filter(|c| *c >= f64::from(MIN_WINDOW_HEIGHT)),
    ) {
        c_ui.window().set_size(slint::LogicalSize::new(l_width as f32, l_height as f32));
        if let (Some(l_x), Some(l_y)) = (z_number("x").filter(|c| *c > -30000.0), z_number("y").filter(|c| *c > -30000.0)) {
            c_ui.window().set_position(slint::PhysicalPosition::new(l_x as i32, l_y as i32));
        }
    }
    if boolean(c_layout, "maximized") {
        c_ui.window().set_maximized(true);
    }
    if c_layout.is_object() {
        c_ui.set_monitor_expanded(boolean(c_layout, "monitor_expanded"));
        c_ui.set_bottom_collapsed(boolean(c_layout, "bottom_collapsed"));
        c_ui.set_bottom_page(number(c_layout, "bottom_page") as i32);
    }
    if let Some(l_width) = z_number("monitor_width") { c_ui.set_monitor_width(l_width as f32); }
    let z_groups = array(c_layout, "expanded_groups").iter().filter_map(Value::as_str).map(SharedString::from).collect::<Vec<_>>();
    c_ui.set_expanded_connection_groups(ModelRc::new(VecModel::from(z_groups)));
    if let Some(l_height) = z_number("bottom_height") { c_ui.set_bottom_height(l_height as f32); }
}

// 关闭时的布局; 最大化时窗口位置大小沿用 c_saved (上次保存的普通窗口), 取消最大化后仍是原来的大小.
fn current_layout(c_ui: &AppWindow, c_saved: &Value) -> Value {
    let z_window = c_ui.window();
    let z_position = z_window.position();
    let z_size = z_window.size().to_logical(z_window.scale_factor());
    // 最小化时 (Windows 上位置为 -32000, 尺寸可能为 0) 或尺寸异常时不记录当前窗口, 沿用上次保存的位置大小与最大化状态.
    let z_abnormal = z_window.is_minimized() || z_position.x <= -30000 || z_position.y <= -30000
        || z_size.width < MIN_WINDOW_WIDTH || z_size.height < MIN_WINDOW_HEIGHT;
    let z_maximized = if z_abnormal && !z_window.is_maximized() { boolean(c_saved, "maximized") } else { z_window.is_maximized() };
    let (z_x, z_y, z_width, z_height) = if z_maximized || z_abnormal {
        (number(c_saved, "x"), number(c_saved, "y"), number(c_saved, "width"), number(c_saved, "height"))
    } else {
        (f64::from(z_position.x), f64::from(z_position.y), f64::from(z_size.width), f64::from(z_size.height))
    };
    json!({
        "x": z_x as i32, "y": z_y as i32, "width": z_width.round() as u32, "height": z_height.round() as u32, "maximized": z_maximized,
        "monitor_expanded": c_ui.get_monitor_expanded(), "monitor_width": c_ui.get_monitor_width().round() as u32,
        "bottom_collapsed": c_ui.get_bottom_collapsed(), "bottom_height": c_ui.get_bottom_height().round() as u32,
        "bottom_page": c_ui.get_bottom_page(), "expanded_groups": current_expanded_groups(c_ui),
    })
}

// 已删除或改名的分组不再记录; 分组列表尚未加载 (启动后立即退出) 时原样保留.
fn current_expanded_groups(c_ui: &AppWindow) -> Vec<String> {
    let z_groups = c_ui.get_connection_groups();
    c_ui.get_expanded_connection_groups()
        .iter()
        .filter(|c| z_groups.row_count() == 0 || z_groups.iter().any(|c_name| c_name == *c))
        .map(|c| c.to_string())
        .collect()
}

// 定时状态结果: 上传完成后让对应会话的文件列表失效; 新连接成功时读取一次目录.
fn apply_polled_state(
    c_ui: &AppWindow,
    c_result: anyhow::Result<Value>,
    c_state: &Mutex<Value>,
    c_answers: &Arc<Mutex<Vec<String>>>,
    c_runtime: &tokio::runtime::Handle,
    c_backend: &Arc<Backend>,
) {
    let z_value = match c_result {
        Ok(l_value) => l_value,
        Err(err) => {
            // 状态每 500ms 轮询一次, 同一错误不重复刷新, 让消息能按时淡出.
            let l_text = format!("状态读取失败: {err}");
            if c_ui.get_status().as_str() != l_text {
                notify(c_ui, NOTE_ERROR, l_text);
            }
            return;
        }
    };
    apply_mcp_status(c_ui, &z_value["mcp_status"]);
    let mut z_old = c_state.lock().unwrap();
    let z_active_connected = array(&z_value, "sessions").iter().any(|c| string(c, "session_id") == c_ui.get_active_session().as_str() && string(c, "status") == "connected");
    let z_needs_files = if z_active_connected {
        c_ui.get_remote_listed_session() != c_ui.get_active_session()
    } else {
        c_ui.get_remote_ready() || c_ui.get_files().row_count() > 0 || c_ui.get_directory_tree().row_count() > 0
            || !c_ui.get_file_loading_path().is_empty() || !c_ui.get_tree_loading().is_empty()
            || !c_ui.get_remote_listed_session().is_empty()
    };
    if *z_old == z_value && !z_needs_files {
        return;
    }
    let z_connected_ids = array(&z_value, "sessions").iter()
        .filter(|c| string(c, "status") == "connected")
        .map(|c| string(c, "session_id")).collect::<Vec<_>>();
    FILE_CACHE.with_borrow_mut(|c| c.retain(|c| z_connected_ids.contains(&c.0.0.as_str())));
    TREE_CACHE.with_borrow_mut(|c| c.retain(|c, _| z_connected_ids.contains(&c.0.as_str()) && c.0 == c_ui.get_active_session().as_str()));
    // 同一标签重连沿用会话 ID, 两次轮询之间完成的重连看不到断开; 会话代次变了就作废旧列表, 下面按原目录重新读取.
    for l_session in array(&z_value, "sessions") {
        let l_id = string(l_session, "session_id");
        if array(&z_old, "sessions").iter().any(|c| string(c, "session_id") == l_id && number(c, "link") != number(l_session, "link")) {
            // 后台标签保存的视图快照 (列表、已加载标记、选择) 一并作废, 切回时重新读取.
            c_ui.invoke_action("discard_files".into(), l_id.into());
        }
    }
    let mut z_failed = Vec::new();
    for c_task in array(&z_value, "tasks") {
        let l_kind = string(c_task, "kind");
        let l_status = string(c_task, "status");
        let l_was = |c_status: &str| array(&z_old, "tasks").iter().any(|c_old| string(c_old, "task_id") == string(c_task, "task_id") && string(c_old, "status") == c_status);
        if l_kind == "upload" && l_status == "completed" && !l_was("completed") {
            invalidate_dirs(c_ui, string(c_task, "session_id"), &[parent_dir(string(c_task, "label"))]);
            // AI 的上传不抢用户的选择; 用户已切到别的目录时也不跳转.
            if string(c_task, "source") != "mcp" && parent_dir(string(c_task, "label")) == c_ui.get_browse_path().as_str() {
                FILE_FOCUS.set(Some((string(c_task, "session_id").to_owned(), string(c_task, "label").to_owned())));
            }
        }
        // 文件夹上传涉及的子目录很多, 失败或取消时也可能已写入部分文件, 结束后让整个会话的目录缓存失效.
        if l_kind == "upload" && boolean(c_task, "directory") && matches!(l_status, "completed" | "failed" | "cancelled") && !l_was(l_status) {
            c_ui.invoke_action("invalidate_files".into(), string(c_task, "session_id").into());
        }
        // 传输失败后进度直接从状态栏消失, 不提示容易被当成已完成 (以为下载好了而删掉服务器上的文件), 必须弹窗告知.
        if matches!(l_kind, "upload" | "download") && l_status == "failed" && !l_was("failed") {
            let l_ai = if string(c_task, "source") == "mcp" { "[AI] " } else { "" };
            let (l_from, l_to) = if l_kind == "download" { ("label", "local") } else { ("local", "label") };
            z_failed.push(format!(
                "{l_ai}{}失败: {}\n目标位置: {}\n原因: {}",
                if l_kind == "download" { "下载" } else { "上传" },
                string(c_task, l_from),
                string(c_task, l_to),
                friendly_error(string(c_task, "error")),
            ));
        }
    }
    if !z_failed.is_empty() {
        notify(c_ui, NOTE_ERROR, format!("{} 个传输任务失败, 详见任务列表", z_failed.len()));
        alert_transfer_failures(c_runtime, z_failed);
    }
    // 状态栏分工: 左侧为事件消息 (含连接失败原因与重试), 中间只显示当前会话的简短状态, 右侧为传输.
    // 会话刚变为失败时在左侧提示一次.
    for l_session in array(&z_value, "sessions") {
        let l_reason = session_error(string(l_session, "status"));
        let l_id = string(l_session, "session_id");
        let l_was = array(&z_old, "sessions").iter().find(|c| string(c, "session_id") == l_id).map(|c| string(c, "status"));
        if !l_reason.is_empty() && l_was != Some(string(l_session, "status")) {
            notify_retry(c_ui, format!("{} 连接失败: {l_reason}", server_label(l_session)), "reconnect", l_id.into());
        }
    }
    apply_state(c_ui, &z_value, c_answers);
    let z_active = c_ui.get_active_session().to_string();
    let z_connected = array(&z_value, "sessions")
        .iter()
        .any(|c| string(c, "session_id") == z_active && string(c, "status") == "connected");
    if z_connected && c_ui.get_remote_listed_session().as_str() != z_active {
        c_ui.set_remote_listed_session(z_active.clone().into());
        dispatch(c_runtime, c_backend, c_ui.as_weak(), "list_files", json!({"session_id":z_active,"path":c_ui.get_browse_path().to_string()}));
    } else if !z_connected {
        // 断开后清空文件列表与目录树, 重新连接时按原目录重新读取.
        invalidate_file_cache(c_ui, &z_active);
        c_ui.set_remote_ready(false);
        c_ui.set_remote_listed_session(SharedString::default());
        c_ui.set_files(ModelRc::default());
        c_ui.set_directory_tree(ModelRc::default());
        c_ui.set_selected_remote(SharedString::default());
        c_ui.set_selected_files_count(0);
        c_ui.set_selected_file_paths(SharedString::default());
    }
    *z_old = z_value;
}

// 定时终端结果: 用户切换标签、滚动或正在选择时丢弃迟到画面; 序号未变不重复应用.
fn apply_polled_terminal(
    c_ui: &AppWindow,
    c_value: &Value,
    c_session: &str,
    c_scroll: i32,
    c_sequence: &Mutex<(String, u64, i32)>,
) {
    if c_ui.get_active_session().as_str() != c_session {
        return;
    }
    if let Some(z_text) = c_value.get("clipboard").and_then(Value::as_str) {
        c_ui.invoke_copy_text(z_text.into());
    }
    // 不刷新画面时 (翻看历史、拖选) 输入模式仍随远端更新, 按键和粘贴按最新模式编码.
    if boolean(c_value, "unchanged") && c_value.get("modified_keys").is_some() {
        c_ui.set_bracketed_paste(boolean(c_value, "bracketed_paste"));
        c_ui.set_application_cursor(boolean(c_value, "application_cursor"));
        c_ui.set_modified_keys(number(c_value, "modified_keys") as i32);
        c_ui.set_mouse_mode(number(c_value, "mouse") as i32);
    }
    if c_ui.get_terminal_scroll_offset() != c_scroll
        || boolean(c_value, "unchanged")
        || c_ui.get_selection_dragging()
    {
        return;
    }
    let z_seq = c_value.get("seq").and_then(Value::as_u64).unwrap_or(0);
    let mut z_old = c_sequence.lock().unwrap();
    if z_old.0 != c_session || z_old.2 != c_scroll || z_old.1 != z_seq {
        // 残留选区 (点击时轻微移动也会选中) 不能让画面停止刷新: 选区处文字未变 (如 tmux 状态栏走时) 时保留, 变了就取消.
        let z_spans = c_ui.get_selection_spans().iter().collect::<Vec<_>>();
        let z_before = (!z_spans.is_empty()).then(|| spans_text(&c_ui.get_terminal_text(), &z_spans));
        apply_terminal(c_ui, c_value);
        if z_before.is_some_and(|c| c != spans_text(&c_ui.get_terminal_text(), &z_spans)) {
            c_ui.set_selection_spans(ModelRc::default());
            c_ui.set_selected_terminal_text("".into());
        }
        *z_old = (c_session.to_owned(), z_seq, c_scroll);
    }
    drop(z_old);
    let z_actual_scroll = number(c_value, "scroll_offset") as i32;
    // 后台进入全屏程序或清空历史时, 旧偏移可能已经不存在; 按实际位置恢复, 避免继续误判为历史冻结.
    if z_actual_scroll != c_scroll {
        c_ui.set_terminal_scroll_offset(z_actual_scroll);
    }
}

// 选区各片段覆盖的文字, 只用于比较刷新前后选区处是否变化.
fn spans_text(c_text: &str, c_spans: &[SelectionSpan]) -> String {
    let z_lines = c_text.split('\n').collect::<Vec<_>>();
    c_spans
        .iter()
        .map(|c_span| {
            let (l_left, l_right) = (c_span.col.max(0) as usize, (c_span.col + c_span.cells).max(0) as usize);
            let mut l_col = 0;
            z_lines.get(c_span.row.max(0) as usize).map_or(String::new(), |c_line| {
                c_line.chars().filter(|c_char| {
                    let l_inside = (l_left..l_right).contains(&l_col);
                    l_col += c_char.width().unwrap_or(0);
                    l_inside
                }).collect()
            })
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn dispatch(
    c_runtime: &tokio::runtime::Handle,
    c_backend: &Arc<Backend>,
    c_weak: slint::Weak<AppWindow>,
    c_method: &str,
    mut c_args: Value,
) {
    let z_backend = c_backend.clone();
    let z_method = c_method.to_string();
    // 目录请求与其属性补全都记下所属列表的代数, 回包时已有更新的列表 (刷新、换目录、切换视图) 则丢弃.
    // 补全在列表回包处理中立即发出, 沿用该列表的代数.
    let z_list_context = if matches!(c_method, "list_files" | "file_details") {
        let Some(z_ui) = c_weak.upgrade() else { return; };
        let mut z_request = z_ui.get_file_request_generation();
        if c_method == "list_files" {
            // 自动刷新和点击目录共用入口, 都不能在旧列表上继续执行文件操作.
            let z_path = string(&c_args, "path");
            let z_changed = z_ui.get_browse_path().as_str() != z_path;
            let z_cached = z_changed.then(|| cached_file_rows(string(&c_args, "session_id"), z_path)).flatten();
            if z_changed { z_ui.set_file_scroll_y(0.0); }
            // 换目录且无缓存时清空旧目录的行; 同一目录刷新保留当前行, 避免闪空和滚动位置丢失.
            if let Some((l_rows, l_label)) = z_cached {
                z_ui.set_files(ModelRc::new(VecModel::from(l_rows)));
                z_ui.set_file_time_label(l_label);
            } else if z_changed {
                z_ui.set_files(ModelRc::default());
            }
            z_ui.set_browse_path(z_path.into());
            z_ui.set_file_loading_path(z_path.into());
            // 先定位再读取, 点击后目录树立即响应, 不等慢网络回包.
            scroll_tree_to(&z_ui, z_path);
            z_ui.set_remote_listed_session(string(&c_args, "session_id").into());
            z_ui.set_remote_ready(false);
            z_ui.set_selected_remote("".into());
            z_ui.set_selected_files_count(0);
            z_ui.set_selected_file_paths("".into());
            z_request = z_request.wrapping_add(1);
            z_ui.set_file_request_generation(z_request);
            // 后台据此把未展开的上级目录与列表一起读回, 目录树不用逐级等待.
            c_args["tree_expanded"] = z_ui.get_directory_tree().iter().filter(|c| c.expanded).map(|c| c.path.to_string()).collect::<Vec<_>>().into();
        }
        Some((z_ui.get_view_generation(), z_request))
    } else { None };
    c_runtime.spawn(async move {
        let mut z_result = z_backend.call(&z_method, c_args.clone(), Actor::User).await;
        if z_method == "list_files" {
            if let Ok(z_value) = &mut z_result {
                z_value["requested_path"] = c_args["path"].clone();
            }
        }
        // 修改可能部分成功后报错, 成败都使所在目录的旧缓存失效; 包括操作期间用户已切到别的标签.
        if matches!(z_method.as_str(), "set_permissions" | "copy_file" | "rename_file" | "delete_files" | "delete_files_fast" | "create_directory" | "create_file") {
            let z_session = string(&c_args, "session_id").to_owned();
            let mut z_dirs = if z_method.starts_with("create_") {
                vec![string(&c_args, "path").to_owned()]
            } else if z_method.starts_with("delete_") {
                array(&c_args, "paths").iter().filter_map(Value::as_str).map(parent_dir).collect()
            } else {
                vec![parent_dir(string(&c_args, "path"))]
            };
            z_dirs.sort();
            z_dirs.dedup();
            let _ = c_weak.upgrade_in_event_loop(move |c_ui| invalidate_dirs(&c_ui, &z_session, &z_dirs));
        }
        clear_secrets(&mut c_args);
        complete(c_weak, z_method, z_result, z_list_context);
    });
}

fn select_terminal_text(
    c_ui: &AppWindow,
    c_row: i32,
    c_col: i32,
    c_end_row: i32,
    c_end_col: i32,
    c_word: bool,
) {
    let z_text = c_ui.get_terminal_text();
    let z_lines = z_text.split('\n').collect::<Vec<_>>();
    let z_last_row = z_lines.len().saturating_sub(1);
    let z_cols = c_ui.get_terminal_display_cols().max(1) as usize;
    let z_wrapped = c_ui.get_terminal_wrapped_rows().iter().collect::<Vec<_>>();
    let mut z_start = (
        (c_row.max(0) as usize).min(z_last_row),
        (c_col.max(0) as usize).min(z_cols),
    );
    let mut z_end = (
        (c_end_row.max(0) as usize).min(z_last_row),
        (c_end_col.max(0) as usize).min(z_cols),
    );
    if c_word {
        let mut z_col = 0;
        let mut z_word_start = 0;
        let mut z_word_end = z_cols;
        for z_char in z_lines[z_start.0].chars() {
            let z_width = z_char.width().unwrap_or(0);
            if z_char.is_whitespace() {
                if z_col >= z_start.1 {
                    z_word_end = z_col;
                    break;
                }
                z_word_start = z_col + z_width;
            }
            z_col += z_width;
        }
        z_end = (z_start.0, z_word_end.min(z_col.max(z_start.1 + 1)));
        z_start.1 = z_word_start;
    }
    if z_start > z_end {
        std::mem::swap(&mut z_start, &mut z_end);
    }
    let mut z_selected = String::new();
    let mut z_spans = Vec::new();
    let mut z_truncated = false;
    for (i, z_line) in z_lines.iter().enumerate().take(z_end.0 + 1).skip(z_start.0) {
        let z_left = if i == z_start.0 { z_start.1 } else { 0 };
        let z_right = if i == z_end.0 { z_end.1 } else { z_cols };
        if z_right > z_left {
            z_spans.push(SelectionSpan {
                row: i as i32,
                col: z_left as i32,
                cells: (z_right - z_left) as i32,
            });
        }
        // 终端宽度处的自动折行属于同一行, 复制时不插入换行.
        if i > z_start.0 && z_selected.len() < MAX_COPY_BYTES && !z_wrapped.contains(&(i as i32 - 1)) {
            z_selected.push('\n');
        }
        let mut z_col = 0;
        let mut z_previous_selected = false;
        for z_char in z_line.chars() {
            let z_width = z_char.width().unwrap_or(0);
            let z_inside = if z_width == 0 {
                z_previous_selected
            } else {
                z_col < z_right && z_col + z_width > z_left
            };
            if z_inside {
                if z_selected.len() + z_char.len_utf8() > MAX_COPY_BYTES {
                    z_truncated = true;
                    break;
                }
                z_selected.push(z_char);
            }
            z_previous_selected = z_inside;
            z_col += z_width;
            if z_col > z_right {
                break;
            }
        }
    }
    c_ui.set_selection_spans(ModelRc::new(VecModel::from(z_spans)));
    c_ui.set_selected_terminal_text(z_selected.into());
    if z_truncated {
        notify(c_ui, NOTE_WARN, "所选文本最多复制 64 KB, 请分段复制");
    }
}

fn select_remote_files(
    c_ui: &AppWindow,
    c_path: &str,
    c_toggle: bool,
    c_range: bool,
    c_context: bool,
) {
    if !c_ui.get_remote_ready() {
        return;
    }
    let mut z_rows = c_ui.get_files().iter().collect::<Vec<_>>();
    let Some(z_target) = z_rows.iter().position(|c| c.path.as_str() == c_path) else {
        return;
    };
    if c_context && z_rows[z_target].selected {
        return;
    }
    let z_anchor = z_rows
        .iter()
        .position(|c| c.path == c_ui.get_selected_remote())
        .unwrap_or(z_target);
    for (i, z_file) in z_rows.iter_mut().enumerate() {
        z_file.selected = if c_range {
            (z_anchor.min(z_target)..=z_anchor.max(z_target)).contains(&i)
        } else if c_toggle {
            if i == z_target {
                !z_file.selected
            } else {
                z_file.selected
            }
        } else {
            i == z_target
        };
    }
    if !c_range {
        c_ui.set_selected_remote(c_path.into());
    }
    apply_file_selection(c_ui, z_rows);
}

fn apply_file_selection(c_ui: &AppWindow, c_rows: Vec<FileRow>) {
    let z_paths = c_rows
        .iter()
        .filter(|c| c.selected)
        .map(|c| c.path.as_str())
        .collect::<Vec<_>>();
    c_ui.set_selected_files_count(z_paths.len() as i32);
    c_ui.set_selected_file_paths(z_paths.join("\n").into());
    if z_paths.is_empty() {
        c_ui.set_selected_remote("".into());
    } else if c_ui.get_selected_remote().is_empty() {
        c_ui.set_selected_remote(z_paths[0].into());
    }
    if z_paths.len() > 1 {
        notify(c_ui, NOTE_INFO, format!("已选择 {} 项", z_paths.len()));
    }
    // 只更新行数据, 保留正在响应右键的行和菜单实例.
    let z_model = c_ui.get_files();
    if let Some(z_rows) = z_model.as_any().downcast_ref::<VecModel<FileRow>>() {
        for (i, z_file) in c_rows.into_iter().enumerate() {
            if z_rows
                .row_data(i)
                .is_some_and(|c| c.selected != z_file.selected)
            {
                z_rows.set_row_data(i, z_file);
            }
        }
    } else {
        c_ui.set_files(ModelRc::new(VecModel::from(c_rows)));
    }
}

fn upload_paths(
    c_runtime: &tokio::runtime::Handle,
    c_backend: &Arc<Backend>,
    c_weak: slint::Weak<AppWindow>,
    c_session: String,
    c_directory: String,
    c_paths: Vec<std::path::PathBuf>,
) {
    if c_paths.is_empty() || c_paths.len() > MAX_TRANSFER_ITEMS {
        show_notice(c_weak, NOTE_WARN, format!("一次请选择或拖入 1～{MAX_TRANSFER_ITEMS} 项"));
        return;
    }
    let z_backend = c_backend.clone();
    c_runtime.spawn(async move {
        // 拖入上传不经过选择框, 在这里提前建立传输连接; 已建好时立即返回.
        let _ = z_backend.call("warm_transfer", json!({"session_id":c_session}), Actor::User).await;
        for z_path in &c_paths {
            let Some(_) = z_path
                .file_name()
                .and_then(|c| c.to_str())
                .filter(|c| !c.is_empty() && !c.chars().any(char::is_control))
            else {
                show_notice(c_weak, NOTE_WARN, "文件名无效".into());
                return;
            };
            if !z_path.is_file() && !z_path.is_dir() {
                show_notice(c_weak, NOTE_WARN, "只能上传普通文件或文件夹".into());
                return;
            }
        }
        let mut z_names = c_paths
            .iter()
            .map(|c| c.file_name().and_then(|l| l.to_str()).unwrap_or_default().to_owned())
            .collect::<Vec<_>>();
        // 来自不同目录的同名文件在批次内也会互相冲突, 后者先改名; 之后对最终名称统一检查.
        let mut z_seen = std::collections::HashSet::new();
        for z_name in &mut z_names {
            if !z_seen.insert(z_name.clone()) {
                *z_name = unique_name(z_name, |c| z_seen.contains(c));
                z_seen.insert(z_name.clone());
            }
        }
        // 只查最终名称是否已存在 (并发查询), 不读整个目录; 同名冲突统一询问一次.
        // 检查失败时按无冲突处理: 任务不带覆盖许可, 由后台再次检查, 不会覆盖.
        show_notice(c_weak.clone(), NOTE_INFO, "正在检查目标目录中的同名文件...".into());
        let mut z_taken = z_backend
            .call("check_remote_names", json!({"session_id":c_session,"directory":c_directory,"names":z_names}), Actor::User)
            .await
            .map(|c| array(&c, "existing").iter().filter_map(Value::as_str).map(str::to_owned).collect::<std::collections::HashSet<_>>())
            .unwrap_or_default();
        let z_conflicts = z_names.iter().filter(|c| z_taken.contains(*c)).cloned().collect::<Vec<_>>();
        // 覆盖许可按文件记录: 只有确认框里列出的目标可以覆盖.
        let mut z_overwrite = vec![false; z_names.len()];
        let mut z_skip = vec![false; z_names.len()];
        if !z_conflicts.is_empty() {
            let Some(l_choice) = ask_conflict(c_weak.clone(), &z_conflicts).await else { return; };
            match l_choice {
                ConflictChoice::Overwrite => {
                    for (i, z_name) in z_names.iter().enumerate() {
                        z_overwrite[i] = z_conflicts.contains(z_name);
                    }
                }
                ConflictChoice::Skip => {
                    for (i, z_name) in z_names.iter().enumerate() {
                        z_skip[i] = z_conflicts.contains(z_name);
                    }
                }
                ConflictChoice::KeepBoth => {
                    // 保留两者时新名字也不能与已有文件重名: 此时才读取完整目录; 读取失败时仍由后台拒绝重名.
                    if let Ok(l_list) = z_backend.call("list_files", json!({"session_id":c_session,"path":c_directory}), Actor::User).await {
                        z_taken.extend(array(&l_list, "entries").iter().map(|l| string(l, "name").to_owned()));
                    }
                    for z_name in &mut z_names {
                        if z_taken.contains(z_name) {
                            *z_name = unique_name(z_name, |c| z_taken.contains(c) || z_seen.contains(c));
                            z_taken.insert(z_name.clone());
                        }
                    }
                }
            }
        }
        let mut z_count = 0;
        let z_skipped = z_skip.iter().filter(|c| **c).count();
        for (i, ((z_path, z_name), z_overwrite)) in c_paths.into_iter().zip(z_names).zip(z_overwrite).enumerate() {
            if z_skip[i] { continue; }
            let z_remote = format!("{}/{}", c_directory.trim_end_matches('/'), z_name);
            let z_result = z_backend
                .call(
                    if z_path.is_dir() { "upload_directory" } else { "upload_file" },
                    json!({"session_id":c_session,"local_path":z_path,"remote_path":z_remote,"overwrite":z_overwrite}),
                    Actor::User,
                )
                .await;
            if let Err(err) = z_result {
                show_notice(
                    c_weak,
                    NOTE_ERROR,
                    batch_error("上传", z_count, &err),
                );
                return;
            }
            z_count += 1;
        }
        let _ = c_weak.upgrade_in_event_loop(move |c_ui| {
            notify(&c_ui, NOTE_OK, format!("已加入 {z_count} 个上传任务{}", if z_skipped > 0 { format!(", 已跳过 {z_skipped} 个同名文件") } else { String::new() }))
        });
    });
}

fn toggle_directory_tree(
    c_runtime: &tokio::runtime::Handle,
    c_backend: &Arc<Backend>,
    c_weak: slint::Weak<AppWindow>,
    c_path: String,
) {
    let Some(z_ui) = c_weak.upgrade() else {
        return;
    };
    let mut z_nodes = z_ui.get_directory_tree().iter().collect::<Vec<_>>();
    let Some(z_index) = z_nodes.iter().position(|c| c.path.as_str() == c_path) else {
        return;
    };
    let z_depth = z_nodes[z_index].depth;
    if z_nodes[z_index].expanded {
        // 缓存展开后仍在刷新也允许收起, 迟到的结果不能重新撑开目录树.
        if !z_ui.get_tree_loading().is_empty() {
            z_ui.set_tree_loading("".into());
            TREE_REQUEST.with(|c| c.set(c.get().wrapping_add(1)));
        }
        z_nodes[z_index].expanded = false;
        let z_end = z_nodes
            .iter()
            .enumerate()
            .skip(z_index + 1)
            .find(|(_, c)| c.depth <= z_depth)
            .map(|(i, _)| i)
            .unwrap_or(z_nodes.len());
        let z_subtree = z_nodes.drain(z_index + 1..z_end).collect::<Vec<_>>();
        TREE_CACHE.with_borrow_mut(|c_cache| {
            // 只保留当前会话的子树, 内存随会话切换释放.
            let z_session = z_ui.get_active_session().to_string();
            c_cache.retain(|c, _| c.0 == z_session);
            // 隐藏子树和可见节点一样受限, 浏览很多目录时不会无限积累.
            if c_cache.values().map(|c| c.0.len()).sum::<usize>() + z_subtree.len() > 2 * MAX_TREE_NODES || c_cache.len() >= FILE_CACHE_DIRS {
                c_cache.clear();
            }
            let l_loaded = TREE_LOADED.with_borrow(|c| c.get(&(z_session.clone(), c_path.clone())).copied()).unwrap_or_else(tree_expired);
            c_cache.insert((z_session, c_path.clone()), (z_subtree, l_loaded));
        });
        z_ui.set_directory_tree(ModelRc::new(VecModel::from(z_nodes)));
        return;
    }
    if !z_ui.get_tree_loading().is_empty() { return; }
    if z_nodes.len() >= MAX_TREE_NODES || z_depth >= 32 {
        notify(&z_ui, NOTE_WARN, "目录树已达显示上限, 可直接输入目录路径");
        return;
    }
    let z_session = z_ui.get_active_session().to_string();
    // 收起前保留的子树先放回, 展开立即可见; 1 分钟内收起的直接使用, 更早的再在后台读取并按路径合并更新.
    if let Some((mut l_subtree, l_saved)) = TREE_CACHE.with_borrow_mut(|c| c.remove(&(z_session.clone(), c_path.clone()))) {
        l_subtree.truncate(MAX_TREE_NODES.saturating_sub(z_nodes.len()));
        z_nodes[z_index].expanded = true;
        z_nodes.splice(z_index + 1..z_index + 1, l_subtree);
        z_ui.set_directory_tree(ModelRc::new(VecModel::from(z_nodes)));
        if l_saved.elapsed() < TREE_CACHE_TTL {
            return;
        }
    }
    let z_generation = z_ui.get_view_generation();
    let z_request = TREE_REQUEST.with(|c| { let z_next = c.get().wrapping_add(1); c.set(z_next); z_next });
    z_ui.set_tree_loading(c_path.clone().into());
    let z_backend = c_backend.clone();
    c_runtime.spawn(async move {
        let z_result = z_backend
            .call(
                "list_files",
                json!({"session_id":z_session,"path":c_path,"dirs_only":true}),
                Actor::User,
            )
            .await;
        let _ = c_weak.upgrade_in_event_loop(move |c_ui| {
            if c_ui.get_active_session().as_str() != z_session
                || c_ui.get_view_generation() != z_generation
                || c_ui.get_tree_loading().as_str() != c_path
                || TREE_REQUEST.with(Cell::get) != z_request
            {
                return;
            }
            c_ui.set_tree_loading("".into());
            match z_result {
                Ok(z_value) => {
                    // 与文件列表回包共用合并逻辑: 按路径替换该目录的子节点, 两个入口的回包先后到达也不会重复插入.
                    refresh_directory_children(&c_ui, &c_path, array(&z_value, "entries"));
                    if c_ui.get_status_retry_arg().as_str() == c_path { c_ui.invoke_dismiss_retry("tree_toggle".into()); }
                    if c_ui.get_directory_tree().row_count() >= MAX_TREE_NODES || boolean(&z_value, "truncated") {
                        notify(&c_ui, NOTE_WARN, "目录树列表已截断, 可直接输入目录路径");
                    }
                }
                Err(err) => {
                    // 缓存只是预览, 刷新失败后撤回, 重试按钮应重新读取而非收起旧子树.
                    let mut z_nodes = c_ui.get_directory_tree().iter().collect::<Vec<_>>();
                    if let Some(i) = z_nodes.iter().position(|c| c.path.as_str() == c_path) {
                        let z_depth = z_nodes[i].depth;
                        let z_end = z_nodes.iter().enumerate().skip(i + 1).find(|(_, c)| c.depth <= z_depth).map(|(i, _)| i).unwrap_or(z_nodes.len());
                        z_nodes.drain(i + 1..z_end);
                        z_nodes[i].expanded = false;
                        c_ui.set_directory_tree(ModelRc::new(VecModel::from(z_nodes)));
                    }
                    notify_retry(&c_ui, format!("目录加载失败 (已自动重试): {}", friendly_error(&format!("{err:#}"))), "tree_toggle", c_path.into());
                }
            }
        });
    });
}

// 没有读取记录的子树视为已过期, 再展开时后台重新读取.
fn tree_expired() -> Instant {
    Instant::now().checked_sub(TREE_CACHE_TTL).unwrap_or_else(Instant::now)
}

fn refresh_directory_children(c_ui: &AppWindow, c_path: &str, c_entries: &[Value]) {
    TREE_LOADED.with_borrow_mut(|c| {
        // 只保留当前会话的记录, 数量随目录树节点上限.
        let z_session = c_ui.get_active_session().to_string();
        c.retain(|c, _| c.0 == z_session);
        if c.len() >= MAX_TREE_NODES { c.clear(); }
        c.insert((z_session, c_path.to_owned()), Instant::now());
    });
    let mut z_nodes = c_ui.get_directory_tree().iter().collect::<Vec<_>>();
    let Some(z_index) = z_nodes.iter().position(|c| c.path.as_str() == c_path) else { return; };
    let z_depth = z_nodes[z_index].depth + 1;
    let z_end = z_nodes.iter().enumerate().skip(z_index + 1)
        .find(|(_, c)| c.depth < z_depth).map(|(i, _)| i).unwrap_or(z_nodes.len());
    let z_previous = z_nodes.drain(z_index + 1..z_end).collect::<Vec<_>>();
    let z_available = MAX_TREE_NODES.saturating_sub(z_nodes.len());
    let mut z_children = Vec::new();
    for z_entry in c_entries.iter().filter(|c| boolean(c, "is_dir")) {
        if z_children.len() >= z_available { break; }
        let z_path = format!("{}/{}", c_path.trim_end_matches('/'), string(z_entry, "name"));
        if let Some(z_old_index) = z_previous.iter().position(|c| c.path.as_str() == z_path && c.depth == z_depth) {
            let z_old_end = z_previous.iter().enumerate().skip(z_old_index + 1)
                .find(|(_, c)| c.depth <= z_depth).map(|(i, _)| i).unwrap_or(z_previous.len());
            z_children.extend(z_previous[z_old_index..z_old_end].iter().take(z_available - z_children.len()).cloned());
        } else {
            z_children.push(DirectoryRow { path: z_path.into(), name: string(z_entry, "name").into(), depth: z_depth, expanded: false });
        }
    }
    z_nodes[z_index].expanded = true;
    z_nodes.splice(z_index + 1..z_index + 1, z_children);
    c_ui.set_directory_tree(ModelRc::new(VecModel::from(z_nodes)));
}

// 目录的行中心放到约 2/5 高度处, 上方留出父目录; 靠近两端时停在列表边界, 不露空白. 行高 25.
// ListView 新建行时会把偏移吸附到行边界, 任意偏移会被挪动, 所以按整行计算.
fn scroll_tree_to(c_ui: &AppWindow, c_path: &str) {
    let z_tree = c_ui.get_directory_tree();
    let Some(z_index) = z_tree.iter().position(|c| c.path == c_path) else { return; };
    c_ui.set_tree_scroll_y(row_scroll_offset(z_index, z_tree.row_count(), 25.0, c_ui.get_tree_view_height()));
}

// 选中刚上传完成的文件并定位; 只改选中和滚动, 不抢键盘焦点, 用户可能正在终端输入.
fn focus_uploaded_file(c_ui: &AppWindow, c_session: &str, c_path: &str) {
    if c_session != c_ui.get_active_session().as_str() { return; }
    let z_files = c_ui.get_files();
    let Some(z_index) = z_files.iter().position(|c| c.path == c_path) else { return; };
    select_remote_files(c_ui, c_path, false, false, false);
    c_ui.set_file_scroll_y(row_scroll_offset(z_index, z_files.row_count(), 26.0, c_ui.get_file_view_height()));
}

fn row_scroll_offset(c_index: usize, c_rows: usize, c_row_height: f32, c_view_height: f32) -> f32 {
    let z_above = ((c_view_height * 0.4 - c_row_height / 2.0) / c_row_height).round().max(0.0);
    let z_max = (c_rows as f32 * c_row_height - c_view_height).max(0.0);
    -((c_index as f32 - z_above) * c_row_height).clamp(0.0, z_max)
}

fn complete(c_weak: slint::Weak<AppWindow>, c_method: String, c_result: anyhow::Result<Value>, c_list_context: Option<(i32, i32)>) {
    let _ = slint::invoke_from_event_loop(move || {
        let Some(z_ui) = c_weak.upgrade() else {
            return;
        };
        if c_list_context.is_some_and(|c| c != (z_ui.get_view_generation(), z_ui.get_file_request_generation())) { return; }
        // 走到这里的是最新一次目录请求, 成败都结束加载提示.
        if c_method == "list_files" {
            z_ui.set_file_loading_path("".into());
        }
        match c_result {
            Ok(z_value) => {
                if matches!(
                    c_method.as_str(),
                    "set_permissions" | "rename_file" | "delete_files" | "delete_files_fast" | "create_directory" | "create_file"
                ) {
                    z_ui.set_file_operation_busy(false);
                }
                // 浏览、连接、轮询等由界面本身体现结果, 不再逐条提示“完成”.
                if !matches!(
                    c_method.as_str(),
                    "list_files" | "file_details" | "get_task" | "connect" | "reconnect" | "new_tab" | "focus_session" | "disconnect"
                        | "open_terminal" | "close_terminal" | "trust_host" | "answer_auth" | "run_command"
                        | "upload_file" | "download_file" | "upload_directory" | "download_directory" | "send_command" | "send_saved_command" | "export_finalshell" | "copy_file"
                        | "import_finalshell" | "set_terminal_scrollback" | "import_commands" | "export_commands" | "move_tab" | "set_expanded_groups" | "set_tab_show_group" | "set_connection_columns"
                ) {
                    notify(&z_ui, NOTE_OK, format!("{}完成", method_label(&c_method)));
                }
                match c_method.as_str() {
                    "save_connection" => z_ui.set_editing(false),
                    "clear_recent_connections" => {
                        // 带 connection_id 时只移除快速连接中的一条, 其余保持原顺序.
                        let l_rows = match z_value.get("connection_id").and_then(Value::as_str) {
                            Some(l_id) => z_ui.get_recent_connections().iter().filter(|c| c.id.as_str() != l_id).collect(),
                            None => Vec::new(),
                        };
                        z_ui.set_recent_connections(update_rows(z_ui.get_recent_connections(), l_rows));
                    }
                    "save_saved_command" => {
                        z_ui.set_command_editing(false);
                        z_ui.set_saved_command_id(string(&z_value["command"], "id").into());
                    }
                    "delete_saved_command" => z_ui.set_saved_command_id("".into()),
                    "save_command_group" => {
                        z_ui.set_group_editing(false);
                        z_ui.set_selected_command_group(z_ui.get_group_name());
                    }
                    "delete_command_group" => {
                        z_ui.set_group_editing(false);
                        z_ui.set_saved_command_id("".into());
                    }
                    "save_proxy" => {
                        z_ui.set_proxy_editing(false);
                    }
                    "delete_proxy" if number(&z_value, "direct") > 0.0 => {
                        notify(&z_ui, NOTE_OK, format!("代理已删除, {} 台服务器改为直连", number(&z_value, "direct") as usize));
                    }
                    "set_connection_proxy" => {
                        notify(&z_ui, NOTE_OK, format!("已为 {} 台服务器设置代理, 下次连接生效", number(&z_value, "count") as usize));
                    }
                    "save_connection_group" | "delete_connection_group" => {
                        z_ui.set_group_editing(false);
                        z_ui.set_selected_connection_group(if c_method == "save_connection_group" { z_ui.get_group_name() } else { "默认".into() });
                    }
                    "remove_connection" => z_ui.set_selected_connection("".into()),
                    "connect" | "reconnect" | "new_tab" | "focus_session" => {
                        if boolean(&z_value, "skipped") {
                            return;
                        }
                        // 重连回包较晚, 期间用户可能已切到别的标签: 不再切回, 以免后续输入落到别的服务器.
                        if c_method == "reconnect" && z_value.get("session_id").and_then(Value::as_str) != Some(z_ui.get_active_session().as_str()) {
                            return;
                        }
                        if let Some(z_id) = z_value.get("session_id").and_then(Value::as_str) {
                            if c_method != "focus_session" {
                                let mut z_sessions = z_ui.get_sessions().iter().collect::<Vec<_>>();
                                if !z_sessions.iter().any(|c| c.id.as_str() == z_id) {
                                    z_sessions.push(SessionRow { id: z_id.into(), label: if boolean(&z_value, "empty") { "快速连接".into() } else { "连接中".into() }, empty: boolean(&z_value, "empty"), ..Default::default() });
                                    z_ui.set_sessions(update_rows(z_ui.get_sessions(), z_sessions));
                                }
                            }
                            z_ui.invoke_action("show_session".into(), z_id.into());
                            if c_method != "focus_session" {
                                z_ui.set_active_session_empty(boolean(&z_value, "empty"));
                                z_ui.set_managing(false);
                                // 复用空白标签时 show_session 不会重新激活, 且激活时仍按空标签禁用输入; 状态更新后再交还焦点.
                                z_ui.invoke_restore_terminal_view();
                            }
                        }
                    }
                    "import_finalshell" => {
                        // 跳过的记录给出数量和第一条原因, 其余连接照常导入.
                        let l_count = array(&z_value, "connections").len();
                        let l_skipped = array(&z_value, "skipped");
                        match l_skipped.first().and_then(Value::as_str) {
                            Some(l_first) => notify(&z_ui, NOTE_WARN, format!("已导入 {l_count} 条连接, 跳过 {} 条 ({l_first})", l_skipped.len())),
                            None => notify(&z_ui, NOTE_OK, format!("已导入 {l_count} 条连接")),
                        }
                    }
                    "import_commands" => {
                        let l_skipped = number(&z_value, "skipped") as usize;
                        notify(&z_ui, if l_skipped > 0 { NOTE_WARN } else { NOTE_OK }, format!("已导入 {} 条命令{}", number(&z_value, "added") as usize, if l_skipped > 0 { format!(", 跳过 {l_skipped} 条 (已存在或无效)") } else { String::new() }));
                    }
                    "export_commands" => {
                        notify(&z_ui, NOTE_OK, format!("已导出 {} 条命令", number(&z_value, "count") as usize));
                    }
                    "set_terminal_scrollback" => {
                        notify(&z_ui, NOTE_OK, format!("终端历史已设为 {} 行, 新连接生效", number(&z_value, "lines") as u64));
                    }
                    "set_download_dir" => z_ui.set_download_dir(string(&z_value, "path").into()),
                    "export_finalshell" => {
                        notify(&z_ui, NOTE_OK, format!("已导出 {} 条连接", number(&z_value, "count") as usize));
                    }
                    "file_details" => {
                        if string(&z_value, "session_id") != z_ui.get_active_session().as_str()
                            || string(&z_value, "path") != z_ui.get_browse_path().as_str()
                        {
                            return;
                        }
                        let z_details = array(&z_value, "entries")
                            .iter()
                            .map(|c| (string(c, "name"), c))
                            .collect::<HashMap<_, _>>();
                        let z_files = z_ui.get_files();
                        for i in 0..z_files.row_count() {
                            let Some(mut l_row) = z_files.row_data(i) else { continue; };
                            let Some(l_item) = z_details.get(l_row.name.as_str()) else { continue; };
                            if let Some(l_owner) = l_item.get("owner").and_then(Value::as_str) {
                                l_row.owner = l_owner.into();
                            }
                            if let Some(l_text) = l_item.get("modified_text").and_then(Value::as_str).and_then(|c| c.get(..16)) {
                                l_row.modified_text = l_text.into();
                            }
                            z_files.set_row_data(i, l_row);
                        }
                        if !string(&z_value, "server_timezone").is_empty() {
                            z_ui.set_file_time_label("修改时间 (服务器)".into());
                        }
                        cache_file_rows(&z_ui);
                        if !string(&z_value, "time_hint").is_empty() {
                            notify(&z_ui, NOTE_WARN, string(&z_value, "time_hint"));
                        }
                    }
                    "list_files" => {
                        if string(&z_value, "session_id") != z_ui.get_active_session().as_str() {
                            return;
                        }
                        if string(&z_value, "requested_path") != z_ui.get_browse_path().as_str() {
                            return;
                        }
                        let z_path = string(&z_value, "path");
                        z_ui.set_browse_path(z_path.into());
                        z_ui.set_remote_listed_session(string(&z_value, "session_id").into());
                        z_ui.set_remote_ready(true);
                        // 已能正常浏览: 之前读取目录失败留在状态栏的错误不再有意义, 直接收起.
                        z_ui.invoke_dismiss_retry("list_files".into());
                        if z_ui.get_status_retry_arg().as_str() == z_path { z_ui.invoke_dismiss_retry("tree_toggle".into()); }
                        let z_timezone = string(&z_value, "server_timezone");
                        z_ui.set_file_time_label(if z_timezone.is_empty() {
                            "修改时间 (UTC)".into()
                        } else {
                            "修改时间 (服务器)".into()
                        });
                        z_ui.set_selected_remote("".into());
                        z_ui.set_selected_files_count(0);
                        z_ui.set_selected_file_paths("".into());
                        // 先写目录树并定位, 再填文件列表: 同一次回包内完成, 不再逐级等待.
                        if z_ui.get_directory_tree().row_count() == 0 {
                            z_ui.set_directory_tree(ModelRc::new(VecModel::from(vec![
                                DirectoryRow {
                                    path: "/".into(),
                                    name: "/".into(),
                                    depth: 0,
                                    expanded: false,
                                },
                            ])));
                        }
                        // 文件列表已经提供该目录的最新子目录, 不再接受较早发出的树回包.
                        if z_ui.get_tree_loading().as_str() == z_path {
                            z_ui.set_tree_loading("".into());
                            TREE_REQUEST.with(|c| c.set(c.get().wrapping_add(1)));
                        }
                        let z_ancestors = array(&z_value, "tree_ancestors");
                        for l_ancestor in z_ancestors {
                            refresh_directory_children(&z_ui, string(l_ancestor, "path"), array(l_ancestor, "entries"));
                        }
                        refresh_directory_children(&z_ui, z_path, array(&z_value, "entries"));
                        scroll_tree_to(&z_ui, z_path);
                        if z_ui.get_directory_tree().row_count() >= MAX_TREE_NODES || z_ancestors.iter().any(|c| boolean(c, "truncated")) {
                            notify(&z_ui, NOTE_WARN, "目录树列表已截断, 可直接输入目录路径");
                        }
                        let z_rows = array(&z_value, "entries")
                            .iter()
                            .map(|c_item| {
                                file_row(
                                    c_item,
                                    format!(
                                        "{}/{}",
                                        z_path.trim_end_matches('/'),
                                        string(c_item, "name")
                                    ),
                                )
                            })
                            .collect::<Vec<_>>();
                        z_ui.set_files(ModelRc::new(VecModel::from(z_rows)));
                        cache_file_rows(&z_ui);
                        if let Some((l_session, l_path)) = FILE_FOCUS.take() {
                            focus_uploaded_file(&z_ui, &l_session, &l_path);
                        }
                        // 属主与时间仍在后台查询: 取到后就地更新这两列, 不重读目录.
                        if let Some(l_token) = z_value.get("details_token").and_then(Value::as_u64) {
                            z_ui.invoke_action("file_details".into(), l_token.to_string().into());
                        }
                        if !string(&z_value, "time_hint").is_empty() {
                            notify(&z_ui, NOTE_WARN, string(&z_value, "time_hint"));
                        }
                        if boolean(&z_value, "truncated") {
                            notify(&z_ui, NOTE_WARN, "远端列表已达显示上限, 请进入更具体的目录");
                        }
                    }
                    "get_task" => {
                        if string(&z_value, "task_id") == z_ui.get_selected_task().as_str() {
                            z_ui.set_task_output(task_output(&z_ui, &z_value).into());
                        }
                    }
                    "set_permissions" => {
                        z_ui.set_permissions_editing(false);
                        if string(&z_value, "session_id") == z_ui.get_active_session().as_str() {
                            z_ui.invoke_action("list_files".into(), z_ui.get_browse_path());
                        }
                    }
                    "copy_file" => {
                        notify(&z_ui, NOTE_OK, format!("已创建副本 {}", string(&z_value, "path")));
                        if string(&z_value, "session_id") == z_ui.get_active_session().as_str() {
                            z_ui.invoke_action("list_files".into(), z_ui.get_browse_path());
                        }
                    }
                    "rename_file" => {
                        z_ui.set_rename_editing(false);
                        if string(&z_value, "session_id") == z_ui.get_active_session().as_str() {
                            z_ui.invoke_action("list_files".into(), z_ui.get_browse_path());
                        }
                    }
                    "create_directory" | "create_file" => {
                        z_ui.set_create_editing(false);
                        if string(&z_value, "session_id") == z_ui.get_active_session().as_str()
                            && string(&z_value, "directory") == z_ui.get_browse_path().as_str()
                        {
                            z_ui.invoke_action("list_files".into(), z_ui.get_browse_path());
                        }
                    }
                    "delete_files" | "delete_files_fast" => {
                        let z_results = array(&z_value, "results");
                        let z_failed = z_results
                            .iter()
                            .filter(|c| !boolean(c, "success"))
                            .collect::<Vec<_>>();
                        if z_failed.is_empty() {
                            z_ui.set_delete_editing(false);
                        } else {
                            z_ui.set_delete_paths(ModelRc::new(VecModel::from(
                                z_failed
                                    .iter()
                                    .map(|c| SharedString::from(string(c, "path")))
                                    .collect::<Vec<_>>(),
                            )));
                            let z_error = friendly_error(string(z_failed[0], "error"))
                                .chars()
                                .take(200)
                                .collect::<String>();
                            // 全部失败时不写"0 项已删除"; 失败项仍留在对话框列表中.
                            let z_done = z_results.len() - z_failed.len();
                            z_ui.set_file_operation_error(
                                if z_done == 0 {
                                    format!("删除失败: {z_error}")
                                } else {
                                    format!("{z_done} 项已删除, {} 项失败: {z_error}", z_failed.len())
                                }
                                .into(),
                            );
                        }
                        if string(&z_value, "session_id") == z_ui.get_active_session().as_str() {
                            // 正在浏览的目录 (或其上级) 已被删除时退回到被删目录的上级, 不再读取已不存在的路径.
                            let z_browse = z_ui.get_browse_path().to_string();
                            let z_target = z_results
                                .iter()
                                .filter(|c| boolean(c, "success"))
                                .map(|c| string(c, "path").trim_end_matches('/'))
                                .find(|c| !c.is_empty() && (z_browse == *c || z_browse.starts_with(&format!("{c}/"))))
                                .map(parent_dir)
                                .unwrap_or(z_browse);
                            z_ui.invoke_action("list_files".into(), z_target.into());
                        }
                    }
                    "run_command" => {
                        if let Some(z_id) = z_value.get("task_id").and_then(Value::as_str) {
                            z_ui.set_selected_task(z_id.into());
                            z_ui.set_task_output("命令已进入队列, 等待输出...".into());
                            z_ui.set_bottom_page(2);
                        }
                    }
                    _ => {}
                }
            }
            Err(err) => {
                // 补全失败不影响已显示的列表, 属主与时间保持数字 ID 与 UTC.
                if c_method == "file_details" { return; }
                // 带上完整原因链 (如"打开 SFTP 通道失败: 等待服务器响应超时"), 只显示最外层说明看不出真正原因.
                let z_message = format!("{}失败: {}", method_label(&c_method), friendly_error(&format!("{err:#}")));
                // 错误显示在仍打开的对话框里时不再发到状态栏, 免得对话框关闭后状态栏还留着同一条错误.
                let mut z_in_dialog = false;
                if matches!(c_method.as_str(), "save_connection" | "save_saved_command" | "save_command_group" | "save_connection_group" | "save_proxy") {
                    z_ui.set_form_error(z_message.chars().take(200).collect::<String>().into());
                    z_in_dialog = z_ui.get_editing() || z_ui.get_command_editing() || z_ui.get_group_editing() || z_ui.get_proxy_editing();
                }
                if matches!(
                    c_method.as_str(),
                    "set_permissions" | "rename_file" | "delete_files" | "delete_files_fast" | "create_directory" | "create_file"
                ) {
                    z_ui.set_file_operation_busy(false);
                    z_ui.set_file_operation_error(
                        z_message.chars().take(240).collect::<String>().into(),
                    );
                    z_in_dialog = z_ui.get_permissions_editing() || z_ui.get_rename_editing() || z_ui.get_delete_editing() || z_ui.get_create_editing();
                    // 失败也可能部分生效, 发出时已使所在目录失效 (列表变为未就绪); 正在浏览该目录时重新读取, 否则文件区一直不可用.
                    if !z_ui.get_remote_ready() && z_ui.get_file_loading_path().is_empty() {
                        z_ui.invoke_action("list_files".into(), z_ui.get_browse_path());
                    }
                }
                if c_method == "list_files" {
                    let z_session = z_ui.get_active_session().to_string();
                    let z_path = z_ui.get_browse_path().to_string();
                    z_ui.set_file_error(friendly_error(&format!("{err:#}")).into());
                    z_ui.set_file_error_key(format!("{z_session}\n{z_path}").into());
                    FILE_CACHE.with_borrow_mut(|c| c.retain(|c| c.0 != (z_session.clone(), z_path.clone())));
                    z_ui.set_files(ModelRc::default());
                    z_ui.set_remote_ready(false);
                    notify_retry(&z_ui, z_message, "list_files", z_ui.get_browse_path());
                } else if !z_in_dialog {
                    notify(&z_ui, NOTE_ERROR, z_message);
                }
            }
        }
    });
}

fn apply_state(c_ui: &AppWindow, c_state: &Value, c_answers: &Arc<Mutex<Vec<String>>>) {
    if c_ui.get_status().as_str() == "正在加载连接..." {
        notify(c_ui, NOTE_INFO, "就绪");
    }
    let z_proxies = array(c_state, "proxies");
    c_ui.set_proxies(update_rows(
        c_ui.get_proxies(),
        z_proxies
            .iter()
            .map(|c| ProxyRow {
                id: string(c, "id").into(),
                name: string(c, "name").into(),
                kind: string(c, "kind").into(),
                host: string(c, "host").into(),
                port: number(c, "port").to_string().into(),
                username: string(c, "username").into(),
                has_password: boolean(c, "has_password"),
                users: array(c_state, "connections").iter().filter(|l| string(l, "proxy_id") == string(c, "id")).count() as i32,
            })
            .collect::<Vec<_>>(),
    ));
    if c_ui.get_editing() {
        refresh_edit_proxy_choices(c_ui);
    }
    c_ui.set_connections(update_rows(
        c_ui.get_connections(),
        array(c_state, "connections")
            .iter()
            .map(|c_item| ConnectionRow {
                id: string(c_item, "id").into(),
                name: string(c_item, "name").into(),
                // 所选代理不存在 (如从别的电脑导入) 时明确标出, 连接时也会提示重新选择.
                proxy_id: string(c_item, "proxy_id").into(),
                // 名称只用于显示, 是否直连、当前选中哪个代理一律按 proxy_id 判断.
                proxy: match string(c_item, "proxy_id") {
                    "" => "直连",
                    l_id => z_proxies.iter().find(|c| string(c, "id") == l_id).map(|c| string(c, "name")).unwrap_or("代理已删除"),
                }
                .into(),
                auth: match string(c_item, "auth") { "private_key" => "私钥", "keyboard_interactive" => "交互", _ => "密码" }.into(),
                host: string(c_item, "host").into(),
                port: number(c_item, "port").to_string().into(),
                username: string(c_item, "username").into(),
                group: if string(c_item, "group").is_empty() {
                    "默认"
                } else {
                    string(c_item, "group")
                }
                .into(),
                label: server_label(c_item).into(),
            })
            .collect::<Vec<_>>(),
    ));
    // 分组按保存的顺序排列 (可拖动调整); 未记录的分组按名称排在后面, 默认分组未记录时置顶.
    let z_saved = array(&c_state["settings"], "connection_groups").iter().filter_map(Value::as_str).filter(|c| !c.is_empty()).collect::<Vec<_>>();
    let z_extra = array(c_state, "connections")
        .iter()
        .map(|c| string(c, "group"))
        .filter(|c| !c.is_empty() && *c != "默认" && !z_saved.contains(c))
        .collect::<std::collections::BTreeSet<_>>();
    let mut z_groups = Vec::<SharedString>::new();
    if !z_saved.contains(&"默认") {
        z_groups.push("默认".into());
    }
    for z_name in z_saved.into_iter().chain(z_extra) {
        if !z_groups.iter().any(|c| c.as_str() == z_name) {
            z_groups.push(z_name.into());
        }
    }
    c_ui.set_connection_groups(update_rows(c_ui.get_connection_groups(), z_groups));
    c_ui.set_mcp_servers(update_rows(
        c_ui.get_mcp_servers(),
        array(c_state, "connections")
            .iter()
            .filter(|c| boolean(c, "mcp_allowed"))
            .map(|c| format!("{} · {}@{}", server_label(c), string(c, "username"), string(c, "host")).into())
            .collect(),
    ));
    apply_connection_tree(c_ui);
    let z_connections = c_ui.get_connections();
    c_ui.set_recent_connections(update_rows(
        c_ui.get_recent_connections(),
        array(&c_state["settings"], "recent_connections")
            .iter()
            .filter_map(|c| {
                c.as_str().and_then(|c_id| {
                    z_connections
                        .iter()
                        .find(|c_conn| c_conn.id.as_str() == c_id)
                })
            })
            .take(20)
            .collect::<Vec<_>>(),
    ));
    c_ui.set_sessions(update_rows(
        c_ui.get_sessions(),
        array(c_state, "sessions")
            .iter()
            .map(|c_item| {
                let l_status = string(c_item, "status");
                let l_state = session_state(c_item);
                SessionRow {
                    id: string(c_item, "session_id").into(),
                    name: string(c_item, "name").into(),
                    // 标签名不含分组, 界面按设置在前面加 [分组].
                    label: match l_state {
                        _ if boolean(c_item, "empty") => "快速连接".into(),
                        2 => string(c_item, "name").into(),
                        3 => format!("{} · 仅文件", string(c_item, "name")).into(),
                        _ => format!("{} · {}", string(c_item, "name"), status_label(l_status)).into(),
                    },
                    group: match string(c_item, "group") {
                        _ if boolean(c_item, "empty") => "",
                        "" => "默认",
                        l_group => l_group,
                    }
                    .into(),
                    empty: boolean(c_item, "empty"),
                    connected: l_status == "connected",
                    state: l_state,
                }
            })
            .collect::<Vec<_>>(),
    ));
    let z_id = c_state
        .get("active_session_id")
        .and_then(Value::as_str)
        .unwrap_or("");
    c_ui.invoke_action("retain_views".into(), "".into());
    if c_ui.get_active_session().as_str() != z_id {
        c_ui.invoke_action("show_session".into(), z_id.into());
    }
    let z_active = c_ui.get_active_session();
    let z_session = array(c_state, "sessions")
        .iter()
        .find(|c_item| string(c_item, "session_id") == z_active.as_str());
    c_ui.set_active_session_empty(z_session.is_none_or(|c| boolean(c, "empty")));
    c_ui.set_active_connected(z_session.is_some_and(|c| string(c, "status") == "connected"));
    c_ui.set_active_state(z_session.map(session_state).unwrap_or(0));
    c_ui.set_active_connection_id(z_session.map(|c| string(c, "connection_id")).unwrap_or_default().into());
    c_ui.set_active_error(session_error(z_session.map(|c| string(c, "status")).unwrap_or_default()).into());
    c_ui.set_peer_status(
        z_session
            .map(|c| {
                if boolean(c, "empty") {
                    "快速连接".into()
                } else {
                    format!("{} · {}", server_label(c), status_label(string(c, "status")))
                }
            })
            .unwrap_or_else(|| "未连接".into())
            .into(),
    );
    // 侧栏标题: 当前标签的服务器名称与地址.
    let z_host = z_session
        .and_then(|c| array(c_state, "connections").iter().find(|l| string(l, "id") == string(c, "connection_id")))
        .map(|c| format!("{}@{}:{}", string(c, "username"), string(c, "host"), number(c, "port")))
        .unwrap_or_default();
    c_ui.set_monitor_server_name(z_session.filter(|c| !boolean(c, "empty")).map(server_label).unwrap_or_default().into());
    c_ui.set_monitor_server_host(z_host.into());
    c_ui.set_monitor_text(
        z_session
            .map(monitor_text)
            .unwrap_or_else(|| "尚未连接".into())
            .into(),
    );
    apply_monitor_history(c_ui, z_session);
    apply_monitor_processes(c_ui, z_session);
    apply_monitor_usage(c_ui, z_session);
    c_ui.set_quick_commands(update_rows(c_ui.get_quick_commands(),
        array(c_state, "quick_commands")
            .iter()
            .map(|c_item| QuickCommandRow {
                id: string(c_item, "id").into(),
                label: string(c_item, "name").into(),
                group: string(c_item, "group").into(),
                command: string(c_item, "command").into(),
            })
            .collect::<Vec<_>>(),
    ));
    c_ui.set_command_groups(update_rows(c_ui.get_command_groups(),
        array(c_state, "command_groups").iter().filter_map(Value::as_str).map(SharedString::from).collect(),
    ));
    if !c_ui.get_command_groups().iter().any(|c| c == c_ui.get_selected_command_group()) {
        c_ui.set_selected_command_group(c_ui.get_command_groups().row_data(0).unwrap_or_default());
    }
    apply_visible_commands(c_ui);
    if !string(c_state, "quick_commands_error").is_empty() {
        let l_text = format!("读取常用命令失败: {}", string(c_state, "quick_commands_error"));
        if c_ui.get_status().as_str() != l_text {
            notify(c_ui, NOTE_ERROR, l_text);
        }
    }
    c_ui.set_tasks(update_rows(c_ui.get_tasks(),
        array(c_state, "tasks")
            .iter()
            .map(|c_item| {
                let l_total = number(c_item, "total");
                let l_bytes = number(c_item, "bytes");
                let l_status = string(c_item, "status");
                let l_connection = array(c_state, "connections")
                    .iter()
                    .find(|c| string(c, "id") == string(c_item, "connection_id"));
                let l_machine = l_connection.map(server_label).unwrap_or_else(|| "连接已删除".into());
                let l_command = string(c_item, "kind") == "command";
                let l_exit = c_item.get("exit_code").and_then(Value::as_i64);
                let l_directory = boolean(c_item, "directory");
                let l_files = number(c_item, "files_total");
                let l_file_bytes = number(c_item, "file_bytes");
                let l_file_total = number(c_item, "file_total");
                TaskRow {
                    id: string(c_item, "task_id").into(),
                    // 传输只显示文件名 (方向由图标表示), 完整路径在右侧详情中.
                    title: if l_command {
                        string(c_item, "label").lines().next().unwrap_or_default().into()
                    } else {
                        string(c_item, "label").rsplit('/').next().unwrap_or_default().into()
                    },
                    server: l_machine.into(),
                    ai: string(c_item, "source") == "mcp",
                    // AI 任务显示发起的客户端名称, 多个 AI 同时工作时可区分.
                    source: match (string(c_item, "source"), string(c_item, "client")) {
                        ("mcp", "") => "LLM".into(),
                        ("mcp", l_client) => l_client.chars().take(16).collect::<String>().into(),
                        _ => "用户".into(),
                    },
                    kind: match string(c_item, "kind") { "upload" => 1, "download" => 2, _ => 0 },
                    // 0 进行中/排队, 1 成功, 2 失败 (含非零退出码), 3 已取消.
                    state: match l_status {
                        "completed" if l_exit.is_some_and(|c| c != 0) => 2,
                        "completed" => 1,
                        "failed" => 2,
                        "cancelled" => 3,
                        _ => 0,
                    },
                    status: match l_exit {
                        Some(l_code) if l_command => format!("{} · 退出码 {l_code}", status_label(l_status)),
                        _ => status_label(l_status).to_string(),
                    }
                    .into(),
                    detail: if l_command {
                        SharedString::default()
                    } else if l_status == "completed" {
                        format!(
                            "{}{}{} · {}",
                            if l_directory { format!("{l_files} 个文件 · ") } else { String::new() },
                            format_bytes(l_bytes),
                            if string(c_item, "stderr").is_empty() { "" } else { " · 有跳过项, 见详情" },
                            task_timing(c_item).unwrap_or_default(),
                        )
                        .into()
                    } else if l_directory && l_status == "preparing" && l_files == 0.0 {
                        "正在扫描文件夹".into()
                    } else if l_directory && l_status == "running" {
                        // 进度条为当前文件; 整个文件夹的进度放在末尾.
                        format!(
                            "{:.0}/{l_files} {} · {} / {} · {}/s · 总 {:.0}%",
                            (number(c_item, "files_done") + 1.0).min(l_files),
                            string(c_item, "current"),
                            format_bytes(l_file_bytes),
                            format_bytes(l_file_total),
                            format_bytes(number(c_item, "speed_bytes_per_sec")),
                            if l_total > 0.0 { (l_bytes / l_total).clamp(0.0, 1.0) * 100.0 } else { 0.0 },
                        )
                        .into()
                    } else if l_status == "retrying" {
                        format!(
                            "已保留 {}{} · 等待自动重连",
                            format_bytes(l_bytes),
                            if l_total > 0.0 { format!(" / {}", format_bytes(l_total)) } else { String::new() },
                        )
                        .into()
                    } else {
                        let l_speed = number(c_item, "speed_bytes_per_sec");
                        format!(
                            "{}{} · {}/s{}",
                            format_bytes(l_bytes),
                            if l_total > 0.0 { format!(" / {}", format_bytes(l_total)) } else { String::new() },
                            format_bytes(l_speed),
                            if l_status == "running" && l_speed > 0.0 && l_total > l_bytes { format!(" · 剩 {}", format_eta((l_total - l_bytes) / l_speed)) } else { String::new() },
                        )
                        .into()
                    },
                    ratio: if l_directory {
                        if l_status == "running" && l_file_total > 0.0 { (l_file_bytes / l_file_total).clamp(0.0, 1.0) as f32 } else { -1.0 }
                    } else if l_command || l_total <= 0.0 {
                        -1.0
                    } else {
                        (l_bytes / l_total).clamp(0.0, 1.0) as f32
                    },
                    cancellable: matches!(l_status, "queued" | "preparing" | "running" | "pending" | "retrying"),
                    local: string(c_item, "local").into(),
                }
            })
            .collect::<Vec<_>>(),
    ));
    apply_transfer_summary(c_ui, array(c_state, "tasks"));
    let z_trust = array(c_state, "sessions")
        .iter()
        .find(|c_item| c_item.get("trust").is_some_and(Value::is_object));
    c_ui.set_trust_session(
        z_trust
            .map(|c_item| string(c_item, "session_id"))
            .unwrap_or("")
            .into(),
    );
    if let Some(z_session) = z_trust {
        c_ui.set_trust_fingerprint(string(&z_session["trust"], "fingerprint").into());
        c_ui.set_trust_changed(boolean(&z_session["trust"], "changed"));
    }
    let z_auth = array(c_state, "sessions")
        .iter()
        .find(|c_item| !array(c_item, "auth_prompts").is_empty());
    let z_auth_id = z_auth
        .map(|c_item| string(c_item, "session_id"))
        .unwrap_or("");
    let z_prompts = z_auth
        .map(|c_item| array(c_item, "auth_prompts"))
        .unwrap_or(&[]);
    // 认证弹窗只在提示变化时重建, 避免刷新清空正在输入的密码.
    let z_old_prompts = c_ui.get_prompts();
    use slint::Model;
    let z_changed = c_ui.get_auth_session().as_str() != z_auth_id
        || z_old_prompts.row_count() != z_prompts.len()
        || z_prompts.iter().enumerate().any(|(i, c_item)| {
            z_old_prompts.row_data(i).is_none_or(|l_old| {
                l_old.prompt.as_str() != string(c_item, "prompt")
                    || l_old.echo != boolean(c_item, "echo")
            })
        });
    if z_changed {
        let mut z_answers = c_answers.lock().unwrap();
        for z_answer in z_answers.iter_mut() {
            z_answer.zeroize();
        }
        *z_answers = vec![String::new(); z_prompts.len()];
        c_ui.set_auth_generation(c_ui.get_auth_generation() + 1);
        c_ui.set_prompts(ModelRc::new(VecModel::from(
            z_prompts
                .iter()
                .map(|c_item| PromptRow {
                    prompt: string(c_item, "prompt").into(),
                    echo: boolean(c_item, "echo"),
                })
                .collect::<Vec<_>>(),
        )));
        c_ui.set_auth_session(z_auth_id.into());
    }
}

// 刷新字体候选 (仅已安装) 与可下载字体的状态.
fn apply_fonts(c_ui: &AppWindow) {
    let z_installed = crate::fonts::installed();
    c_ui.set_font_downloads(ModelRc::new(VecModel::from(
        crate::fonts::DOWNLOADS
            .iter()
            .map(|c| FontDownloadRow { name: c.0.into(), state: if z_installed.iter().any(|l| l == c.0) { 2 } else { 0 } })
            .collect::<Vec<_>>(),
    )));
    c_ui.set_font_candidates(ModelRc::new(VecModel::from(z_installed.into_iter().map(SharedString::from).collect::<Vec<_>>())));
}

fn set_font_state(c_ui: &AppWindow, c_name: &str, c_state: i32) {
    let z_model = c_ui.get_font_downloads();
    if let Some(z_index) = z_model.iter().position(|c| c.name == c_name) {
        z_model.set_row_data(z_index, FontDownloadRow { name: c_name.into(), state: c_state });
    }
}

fn apply_mcp_status(c_ui: &AppWindow, c_mcp: &Value) {
    c_ui.set_mcp_running(boolean(c_mcp, "running"));
    let z_now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |c| c.as_millis() as u64);
    let z_rows = array(c_mcp, "activity")
        .iter()
        .map(|c| {
            let l_seconds = z_now.saturating_sub(c.get("at_ms").and_then(Value::as_u64).unwrap_or(0)) / 1000;
            McpActivityRow {
                time: match l_seconds {
                    0..5 => "刚刚".into(),
                    5..60 => format!("{l_seconds} 秒前").into(),
                    60..3600 => format!("{} 分钟前", l_seconds / 60).into(),
                    _ => format!("{} 小时前", l_seconds / 3600).into(),
                },
                // 第一行 "动作 · 客户端", 第二行对象; 动作名带出在主终端还是独立执行.
                label: format!(
                    "{} · {}",
                    mcp_action(string(c, "method")).0,
                    match string(c, "client") { "" => "AI", l_client => client_name(l_client) }
                )
                .into(),
                detail: string(c, "detail").into(),
                info: string(c, "info").into(),
                error: string(c, "error").into(),
                risk: mcp_action(string(c, "method")).1,
                ok: boolean(c, "ok"),
            }
        })
        .collect::<Vec<_>>();
    c_ui.set_mcp_activity(update_rows(c_ui.get_mcp_activity(), z_rows));
}

// 客户端自报的名称常带 "-mcp-client" 等后缀 (如 codex-mcp-client), 侧栏只显示前面的简称.
fn client_name(c_name: &str) -> &str {
    let z_name = c_name.trim();
    ["-mcp-client", "_mcp_client", " mcp client", "-mcp", "-client"]
        .iter()
        .find_map(|c| z_name.len().checked_sub(c.len()).filter(|i| *i > 0 && z_name.is_char_boundary(*i) && z_name[*i..].eq_ignore_ascii_case(c)).map(|i| &z_name[..i]))
        .unwrap_or(z_name)
}

fn update_rows<T: Clone + PartialEq + 'static>(c_model: ModelRc<T>, c_rows: Vec<T>) -> ModelRc<T> {
    if let Some(z_model) = c_model.as_any().downcast_ref::<VecModel<T>>() {
        let z_len = c_rows.len();
        for (i, z_row) in c_rows.into_iter().enumerate() {
            if i >= z_model.row_count() {
                z_model.push(z_row);
            } else if z_model.row_data(i).as_ref() != Some(&z_row) {
                z_model.set_row_data(i, z_row);
            }
        }
        while z_model.row_count() > z_len {
            z_model.remove(z_model.row_count() - 1);
        }
        c_model
    } else {
        ModelRc::new(VecModel::from(c_rows))
    }
}

fn apply_connection_tree(c_ui: &AppWindow) {
    let z_expanded_groups = c_ui
        .get_expanded_connection_groups()
        .iter()
        .collect::<std::collections::HashSet<_>>();
    // 分组按 connection_groups 的顺序, 组内连接沿用后台给出的顺序.
    let z_connections = c_ui.get_connections().iter().collect::<Vec<_>>();
    let mut z_rows = Vec::new();
    for z_name in c_ui.get_connection_groups().iter() {
        let z_members = z_connections.iter().filter(|c| c.group == z_name).collect::<Vec<_>>();
        let z_expanded = z_expanded_groups.contains(&z_name);
        z_rows.push(ConnectionTreeRow {
            name: z_name.clone(),
            group: z_name.clone(),
            folder: true,
            count: z_members.len() as i32,
            expanded: z_expanded,
            ..Default::default()
        });
        if z_expanded {
            z_rows.extend(z_members.into_iter().map(|c| ConnectionTreeRow {
                id: c.id.clone(),
                name: c.name.clone(),
                auth: c.auth.clone(),
                proxy: c.proxy.clone(),
                proxy_id: c.proxy_id.clone(),
                host: c.host.clone(),
                port: c.port.clone(),
                username: c.username.clone(),
                group: z_name.clone(),
                ..Default::default()
            }));
        }
    }
    c_ui.set_connection_tree(update_rows(c_ui.get_connection_tree(), z_rows));
}

fn apply_terminal(c_ui: &AppWindow, c_value: &Value) {
    c_ui.set_bracketed_paste(boolean(c_value, "bracketed_paste"));
    c_ui.set_application_cursor(boolean(c_value, "application_cursor"));
    c_ui.set_modified_keys(number(c_value, "modified_keys") as i32);
    c_ui.set_hide_cursor(boolean(c_value, "hide_cursor"));
    c_ui.set_cursor_shape(number(c_value, "cursor_shape") as i32);
    c_ui.set_mouse_mode(number(c_value, "mouse") as i32);
    c_ui.set_terminal_text(string(c_value, "text").into());
    c_ui.set_terminal_view_offset(number(c_value, "scroll_offset") as i32);
    c_ui.set_terminal_history_lines(number(c_value, "scrollback_lines") as i32);
    c_ui.set_cursor_row(number(c_value, "cursor_row") as i32);
    c_ui.set_cursor_col(number(c_value, "cursor_col") as i32);
    c_ui.set_terminal_display_cols(number(c_value, "cols") as i32);
    c_ui.set_terminal_display_rows(number(c_value, "rows") as i32);
    c_ui.set_terminal_wrapped_rows(update_rows(c_ui.get_terminal_wrapped_rows(), array(c_value, "wrapped").iter().map(|c| c.as_i64().unwrap_or(-1) as i32).collect()));
    c_ui.set_alternate_screen(boolean(c_value, "alternate_screen"));
    // 默认前背景跟随主题, 远端ANSI显式颜色原样保留.
    let (z_fg, z_bg) = if c_ui.get_theme_index() == 1 {
        (
            slint::Color::from_rgb_u8(43, 49, 56),
            slint::Color::from_rgb_u8(247, 248, 250),
        )
    } else {
        (
            slint::Color::from_rgb_u8(220, 230, 240),
            slint::Color::from_rgb_u8(16, 21, 29),
        )
    };
    let mut z_runs = array(c_value, "runs")
        .iter()
        .map(|c_item| TerminalRun {
            row: number(c_item, "row") as i32,
            col: number(c_item, "col") as i32,
            cells: c_item
                .get("width")
                .and_then(Value::as_i64)
                .unwrap_or_else(|| string(c_item, "text").chars().count() as i64)
                .max(1) as i32,
            text: string(c_item, "text").into(),
            block: terminal_block(string(c_item, "text")),
            fg: parse_color(string(c_item, "fg"), z_fg),
            bg: parse_color(string(c_item, "bg"), z_bg),
            fg_default: boolean(c_item, "fg_default"),
            bg_default: boolean(c_item, "bg_default"),
            inverse: boolean(c_item, "inverse"),
            bold: boolean(c_item, "bold"),
            underline: boolean(c_item, "underline"),
            dim: boolean(c_item, "dim"),
            italic: boolean(c_item, "italic"),
        })
        .collect::<Vec<_>>();
    // 当前页与上下缓冲页按行列排序, 便于按行号对齐新旧数据.
    z_runs.sort_by_key(|c| (c.row, c.col));
    let z_count = z_runs.len();
    let z_model = c_ui.get_terminal_runs();
    if let Some(z_existing) = z_model.as_any().downcast_ref::<VecModel<TerminalRun>>() {
        // 行号固定, 滚动或新输出时先按行号对齐: 删掉移出顶部的行, 在前面插入新露出的行;
        // 其余位置逐个比较, 未变的片段保留文字组件和字形缓存.
        if let Some(z_first) = z_runs.first().map(|c| c.row) {
            while z_existing.row_data(0).is_some_and(|c| c.row < z_first) {
                z_existing.remove(0);
            }
            if let Some(z_old_first) = z_existing.row_data(0).map(|c| c.row) {
                for (i, z_run) in z_runs.iter().take_while(|c| c.row < z_old_first).enumerate() {
                    z_existing.insert(i, z_run.clone());
                }
            }
        }
        for (i, z_run) in z_runs.into_iter().enumerate() {
            if i >= z_existing.row_count() {
                z_existing.push(z_run);
            } else if z_existing.row_data(i).as_ref() != Some(&z_run) {
                z_existing.set_row_data(i, z_run);
            }
        }
        while z_existing.row_count() > z_count {
            z_existing.remove(z_existing.row_count() - 1);
        }
    } else {
        c_ui.set_terminal_runs(ModelRc::new(VecModel::from(z_runs)));
    }
}

fn terminal_block(c_text: &str) -> i32 {
    match c_text {
        "▀" => 3, "▄" => 12, "█" => 15, "▌" => 5, "▐" => 10,
        "▖" => 4, "▗" => 8, "▘" => 1, "▙" => 13, "▚" => 9,
        "▛" => 7, "▜" => 11, "▝" => 2, "▞" => 6, "▟" => 14,
        _ => 0,
    }
}

// 列表拖动: 把 c_from 项移到行间位置 c_to (0..=len); 位置不变时返回 false.
fn move_item<T>(c_items: &mut Vec<T>, c_from: usize, c_to: usize) -> bool {
    if c_from >= c_items.len() || c_to > c_items.len() || c_to == c_from || c_to == c_from + 1 {
        return false;
    }
    let z_item = c_items.remove(c_from);
    c_items.insert(if c_to > c_from { c_to - 1 } else { c_to }, z_item);
    true
}

// 连接管理拖动: 分组行只在分组之间排序; 连接行放到插入位置上方那一行所在的分组 (上方是分组行则放在该组最前).
fn connection_drop(c_ui: &AppWindow, c_from: usize, c_to: usize) -> Option<(&'static str, Value)> {
    let z_rows = c_ui.get_connection_tree().iter().collect::<Vec<_>>();
    let z_source = z_rows.get(c_from)?;
    if z_source.folder {
        let mut l_groups = z_rows.iter().filter(|c| c.folder).map(|c| c.group.to_string()).collect::<Vec<_>>();
        let l_from = z_rows[..c_from].iter().filter(|c| c.folder).count();
        let l_to = z_rows[..c_to.min(z_rows.len())].iter().filter(|c| c.folder).count();
        return move_item(&mut l_groups, l_from, l_to).then(|| ("reorder_connection_groups", json!({"groups":l_groups})));
    }
    if c_to == 0 || c_to == c_from || c_to == c_from + 1 || c_to > z_rows.len() {
        return None;
    }
    let z_above = &z_rows[c_to - 1];
    let z_connections = c_ui.get_connections().iter().collect::<Vec<_>>();
    let mut z_order = z_connections.iter().map(|c| c.id.to_string()).filter(|c| *c != z_source.id.as_str()).collect::<Vec<_>>();
    let z_at = if z_above.folder {
        z_connections.iter().filter(|c| c.id != z_source.id).position(|c| c.group == z_above.group).unwrap_or(z_order.len())
    } else {
        z_order.iter().position(|c| *c == z_above.id.as_str()).map_or(z_order.len(), |c| c + 1)
    };
    z_order.insert(z_at, z_source.id.to_string());
    Some(("move_connection", json!({"connection_id":z_source.id.as_str(),"group":z_above.group.as_str(),"order":z_order})))
}

fn apply_visible_commands(c_ui: &AppWindow) {
    let z_group = c_ui.get_selected_command_group();
    c_ui.set_visible_commands(update_rows(c_ui.get_visible_commands(),
        c_ui.get_quick_commands().iter().filter(|c| c.group == z_group).collect(),
    ));
}

fn file_row(c_item: &Value, c_path: String) -> FileRow {
    let z_dir = boolean(c_item, "is_dir");
    let z_name = string(c_item, "name");
    let z_kind = match string(c_item, "kind") {
        "directory" => "文件夹",
        "symlink" => "链接",
        _ => "文件",
    };
    let z_size = if z_dir {
        "—".into()
    } else {
        format_bytes(number(c_item, "size"))
    };
    FileRow {
        path: c_path.into(),
        label: format!("{z_kind} {z_name} {z_size}").into(),
        directory: z_dir,
        selected: false,
        name: z_name.into(),
        icon: if z_dir { 0 } else if z_kind == "链接" { 8 } else {
            match z_name.rsplit('.').next().unwrap_or("").to_ascii_lowercase().as_str() {
                "txt" | "md" | "log" | "csv" | "rst" => 3,
                "rs" | "py" | "js" | "ts" | "html" | "css" | "c" | "cpp" | "go" | "java" | "sh" | "bash" | "zsh" => 4,
                "json" | "toml" | "yaml" | "yml" | "ini" | "conf" | "config" | "xml" | "env" => 5,
                "zip" | "gz" | "xz" | "bz2" | "tar" | "tgz" | "7z" | "rar" | "zst" => 6,
                "png" | "jpg" | "jpeg" | "gif" | "webp" | "svg" | "ico" | "bmp" => 7,
                _ => 2,
            }
        },
        size_text: z_size.into(),
        // Slint 的 int 只有32位, 原始长度用十进制文本传递, 大于2GB的文件也不截断.
        size_bytes: c_item.get("size").and_then(Value::as_u64).map(|c| c.to_string()).unwrap_or_default().into(),
        kind_text: z_kind.into(),
        modified_text: c_item.get("modified_text").and_then(Value::as_str)
            .and_then(|c| c.get(..16)).map(str::to_owned)
            .unwrap_or_else(|| format_file_time(c_item.get("modified_time").and_then(Value::as_u64))).into(),
        permissions: c_item
            .get("permissions")
            .and_then(Value::as_str)
            .unwrap_or("—")
            .into(),
        mode: c_item
            .get("mode")
            .and_then(Value::as_u64)
            .map(|c| (c & 0o777) as i32)
            .unwrap_or_default(),
        owner: c_item
            .get("owner")
            .and_then(Value::as_str)
            .unwrap_or("—")
            .into(),
    }
}

fn format_file_time(c_seconds: Option<u64>) -> String {
    let Some(z_seconds) = c_seconds.filter(|c| *c <= 253_402_300_799) else {
        return "—".into();
    };
    // UTC公历换算, 不引入日期库; 纪元平移到3月, 使闰日落在每年的末尾.
    let z_days = (z_seconds / 86_400) as i64 + 719_468;
    let z_era = z_days / 146_097;
    let z_day_of_era = z_days - z_era * 146_097;
    let z_year_of_era = (z_day_of_era - z_day_of_era / 1_460 + z_day_of_era / 36_524
        - z_day_of_era / 146_096)
        / 365;
    let mut z_year = z_year_of_era + z_era * 400;
    let z_day_of_year =
        z_day_of_era - (365 * z_year_of_era + z_year_of_era / 4 - z_year_of_era / 100);
    let z_month_index = (5 * z_day_of_year + 2) / 153;
    let z_day = z_day_of_year - (153 * z_month_index + 2) / 5 + 1;
    let z_month = z_month_index + if z_month_index < 10 { 3 } else { -9 };
    z_year += i64::from(z_month <= 2);
    format!(
        "{z_year:04}-{z_month:02}-{z_day:02} {:02}:{:02}",
        z_seconds % 86_400 / 3_600,
        z_seconds % 3_600 / 60
    )
}

// 标签状态: 0 未连接, 1 连接中, 2 已连接, 3 仅文件 (终端未打开), 4 失败; 状态点与各处提示共用.
fn session_state(c_session: &Value) -> i32 {
    let z_status = string(c_session, "status");
    match z_status {
        "connected" if boolean(c_session, "shell") => 2,
        "connected" => 3,
        "empty" | "disconnected" => 0,
        "authentication_failed" => 4,
        _ if z_status.starts_with("error") => 4,
        _ => 1,
    }
}

// 服务器显示名: "[分组] 名称", 未分组归入默认; 标签、状态栏、侧栏、快速连接和任务统一使用.
// 编辑连接的代理下拉: 直连 + 当前代理列表, 位置与 ID 一一对应. 代理列表在编辑期间变化时按 ID 重新定位;
// 所选代理已不存在 (如从别的电脑导入) 时保留一项提示, 不默认改成直连.
fn refresh_edit_proxy_choices(c_ui: &AppWindow) {
    let z_selected = c_ui.get_edit_proxy_id();
    let mut z_ids = vec![SharedString::default()];
    let mut z_names = vec![SharedString::from("直连")];
    for l_proxy in c_ui.get_proxies().iter() {
        z_ids.push(l_proxy.id);
        z_names.push(l_proxy.name);
    }
    if !z_selected.is_empty() && !z_ids.contains(&z_selected) {
        z_ids.push(z_selected.clone());
        z_names.push("原代理已不存在, 请重新选择".into());
    }
    let z_index = z_ids.iter().position(|c| *c == z_selected).unwrap_or(0) as i32;
    c_ui.set_edit_proxy_ids(update_rows(c_ui.get_edit_proxy_ids(), z_ids));
    c_ui.set_edit_proxy_choices(update_rows(c_ui.get_edit_proxy_choices(), z_names));
    c_ui.set_edit_proxy_index(z_index);
}

// 服务器显示名: 按设置决定是否在名称前加 [分组].
fn server_label(c_item: &Value) -> String {
    if !SHOW_GROUP.get() {
        return string(c_item, "name").to_owned();
    }
    let z_group = string(c_item, "group");
    format!("[{}] {}", if z_group.is_empty() { "默认" } else { z_group }, string(c_item, "name"))
}

fn status_label(c_status: &str) -> &str {
    match c_status {
        "connected" => "已连接",
        "empty" => "快速连接",
        "connecting" => "连接中",
        "host_key_confirmation" => "确认指纹",
        "auth_required" => "等待认证",
        "authenticating" => "认证中",
        "authentication_failed" => "认证失败",
        "disconnected" => "已断开",
        "queued" | "pending" => "排队中",
        "preparing" => "准备中",
        "running" => "进行中",
        "retrying" => "等待恢复",
        "completed" => "已完成",
        "cancelled" => "已取消",
        "failed" => "失败",
        // 具体原因由状态栏左侧消息和终端区提示显示, 标签与会话状态只给简短状态.
        _ if c_status.starts_with("error") => "连接失败",
        _ => c_status,
    }
}

// 会话失败原因: error: 前缀为具体错误, authentication_failed 为服务器拒绝了凭据; 其他状态为空.
fn session_error(c_status: &str) -> String {
    match c_status {
        "authentication_failed" => "服务器拒绝了密码或私钥, 请检查凭据".into(),
        _ => c_status.strip_prefix("error: ").unwrap_or_default().into(),
    }
}

fn apply_monitor_usage(c_ui: &AppWindow, c_session: Option<&Value>) {
    let z_monitor = c_session
        .and_then(|c| c.get("monitor"))
        .unwrap_or(&Value::Null);
    let z_total = number(z_monitor, "memory_total_bytes");
    let z_used = number(z_monitor, "memory_used_bytes");
    let z_swap_total = number(z_monitor, "swap_total_bytes");
    let z_swap_used = number(z_monitor, "swap_used_bytes");
    let z_cache = number(z_monitor, "memory_cache_bytes");
    let z_cpu = number(z_monitor, "cpu_percent").clamp(0.0, 100.0);
    c_ui.set_cpu_ratio((z_cpu / 100.0) as f32);
    c_ui.set_memory_ratio((z_used / z_total.max(1.0)).clamp(0.0, 1.0) as f32);
    c_ui.set_swap_ratio((z_swap_used / z_swap_total.max(1.0)).clamp(0.0, 1.0) as f32);
    c_ui.set_cache_ratio((z_cache / z_total.max(1.0)).clamp(0.0, 1.0) as f32);
    c_ui.set_cpu_usage(
        if z_total > 0.0 {
            format!("{z_cpu:.1}%")
        } else {
            "—".into()
        }
        .into(),
    );
    c_ui.set_memory_usage(
        if z_total > 0.0 {
            format!(
                "{:.0}% · {} / {}",
                z_used / z_total * 100.0,
                format_bytes(z_used),
                format_bytes(z_total)
            )
        } else {
            "—".into()
        }
        .into(),
    );
    c_ui.set_swap_usage(
        if z_total == 0.0 {
            "—".into()
        } else if z_swap_total == 0.0 {
            "未启用".into()
        } else {
            format!(
                "{:.0}% · {} / {}",
                z_swap_used / z_swap_total * 100.0,
                format_bytes(z_swap_used),
                format_bytes(z_swap_total)
            )
        }
        .into(),
    );
    c_ui.set_cache_usage(
        if z_total > 0.0 {
            format!(
                "{:.0}% · {} / {}",
                z_cache / z_total * 100.0,
                format_bytes(z_cache),
                format_bytes(z_total)
            )
        } else {
            "—".into()
        }
        .into(),
    );
}

// 顶栏传输按钮与传输面板: 按钮显示进行中的上传/下载数、合计进度和失败标记; 面板列出未清除的传输任务.
// 用户新发起的传输自动打开面板, AI (MCP) 发起的只在列表和按钮上显示, 不打断用户.
fn apply_transfer_summary(c_ui: &AppWindow, c_tasks: &[Value]) {
    let z_transfers = c_tasks.iter().filter(|c| matches!(string(c, "kind"), "upload" | "download")).collect::<Vec<_>>();
    let z_running = z_transfers
        .iter()
        .filter(|c| matches!(string(c, "status"), "queued" | "preparing" | "running" | "pending" | "retrying"))
        .collect::<Vec<_>>();
    let z_new_user = TRANSFER_SEEN.with_borrow_mut(|c_seen| {
        let l_new = z_running.iter().any(|c| string(c, "source") != "mcp" && !c_seen.contains(string(c, "task_id")));
        // 只保留后台仍在的任务, 两个集合不随历史无限增长.
        *c_seen = z_transfers.iter().map(|c| string(c, "task_id").to_owned()).collect();
        TRANSFER_CLEARED.with_borrow_mut(|c| c.retain(|l| c_seen.contains(l)));
        l_new
    });
    let z_rows = TRANSFER_CLEARED.with_borrow(|c_cleared| {
        c_ui.get_tasks().iter().filter(|c| c.kind != 0 && !c_cleared.contains(c.id.as_str())).collect::<Vec<_>>()
    });
    c_ui.set_transfer_failed(z_rows.iter().any(|c| c.state == 2));
    c_ui.set_transfers(update_rows(c_ui.get_transfers(), z_rows));
    let z_uploads = z_running.iter().filter(|c| string(c, "kind") == "upload").collect::<Vec<_>>();
    c_ui.set_transfer_uploads(z_uploads.len() as i32);
    c_ui.set_transfer_downloads((z_running.len() - z_uploads.len()) as i32);
    // 各方向合计进度只算大小已知的任务; 该方向无任务或都未知时为 -1, 按钮不显示该方向的进度条.
    let z_progress = |c_kind: &str| {
        let (l_bytes, l_total) = z_running.iter().filter(|c| string(c, "kind") == c_kind && number(c, "total") > 0.0)
            .fold((0.0, 0.0), |c, l| (c.0 + number(l, "bytes"), c.1 + number(l, "total")));
        if l_total > 0.0 { (l_bytes / l_total).clamp(0.0, 1.0) as f32 } else { -1.0 }
    };
    c_ui.set_transfer_up_progress(z_progress("upload"));
    c_ui.set_transfer_down_progress(z_progress("download"));
    let z_speed = |c_kind: &str| {
        let l_running = z_running.iter().filter(|c| string(c, "kind") == c_kind && string(c, "status") == "running").collect::<Vec<_>>();
        if l_running.is_empty() { SharedString::default() } else { format!("{}/s", format_bytes(l_running.iter().map(|c| number(c, "speed_bytes_per_sec")).sum::<f64>())).into() }
    };
    c_ui.set_transfer_up_speed(z_speed("upload"));
    c_ui.set_transfer_down_speed(z_speed("download"));
    if z_new_user {
        c_ui.set_transfers_open(true);
    }
}

// 默认下载目录: 用户目录下的桌面, 没有桌面时用用户目录. Windows 开启 OneDrive 备份后桌面在 %OneDrive%\Desktop,
// 优先使用; 中文系统的"桌面"只是显示名, 磁盘上的文件夹仍叫 Desktop.
fn desktop_dir() -> String {
    let z_env = |c_key: &str| std::env::var_os(c_key).filter(|c| !c.is_empty()).map(std::path::PathBuf::from);
    let z_home = z_env(if cfg!(windows) { "USERPROFILE" } else { "HOME" }).unwrap_or_default();
    let z_candidates = if cfg!(windows) { [z_env("OneDrive").map(|c| c.join("Desktop")), Some(z_home.join("Desktop"))] } else { [Some(z_home.join("Desktop")), None] };
    z_candidates.into_iter().flatten().find(|c| c.is_dir()).unwrap_or(z_home).to_string_lossy().into_owned()
}

// 剩余时间: 1 分钟内按秒, 1 小时内按分秒, 更长按时分.
fn format_eta(c_seconds: f64) -> String {
    let z_seconds = c_seconds.ceil().min(359_999.0) as u64;
    match z_seconds {
        0..60 => format!("{z_seconds} 秒"),
        60..3600 => format!("{} 分 {} 秒", z_seconds / 60, z_seconds % 60),
        _ => format!("{} 小时 {} 分", z_seconds / 3600, z_seconds % 3600 / 60),
    }
}

// 在系统文件管理器中显示本机路径: c_select 为 true 时选中该文件 (已不存在则打开所在目录), 否则打开该目录.
fn reveal_local(c_ui: &AppWindow, c_path: &str, c_select: bool) {
    let z_path = std::path::Path::new(c_path);
    if c_path.is_empty() || !z_path.is_absolute() {
        return notify(c_ui, NOTE_WARN, "没有可打开的本机路径");
    }
    let z_select = c_select && z_path.exists();
    let z_target = if c_select && !z_select { z_path.parent().unwrap_or(z_path) } else { z_path };
    if !z_target.exists() {
        return notify(c_ui, NOTE_WARN, format!("本机路径已不存在: {}", z_target.display()));
    }
    #[cfg(target_os = "macos")]
    let z_result = {
        let mut l_command = std::process::Command::new("open");
        if z_select { l_command.arg("-R"); }
        l_command.arg(z_target).spawn()
    };
    #[cfg(windows)]
    let z_result = {
        // explorer 只认 /select,"路径" 这种写法, 按普通参数整体加引号会被忽略选中, 用原样参数.
        use std::os::windows::process::CommandExt;
        let mut l_command = std::process::Command::new("explorer");
        if z_select { l_command.raw_arg(format!("/select,\"{}\"", z_target.display())); } else { l_command.arg(z_target); }
        l_command.spawn()
    };
    #[cfg(not(any(target_os = "macos", windows)))]
    let z_result = std::process::Command::new("xdg-open").arg(if z_select { z_target.parent().unwrap_or(z_target) } else { z_target }).spawn();
    if let Err(err) = z_result {
        notify(c_ui, NOTE_ERROR, format!("打开文件管理器失败: {err}"));
    }
}

fn terminal_key(
    c_text: &SharedString,
    c_control: bool,
    c_alt: bool,
    c_shift: bool,
    c_application_cursor: bool,
    c_modified_keys: i32,
) -> Option<String> {
    use slint::platform::Key;
    let z_key = |c_key: Key| *c_text == SharedString::from(c_key);
    let z_modifier = 1 + i32::from(c_shift) + i32::from(c_alt) * 2 + i32::from(c_control) * 4;
    // 第 3 位为 Kitty 键盘协议 (Pi、Codex 等开启), 带修饰的按键统一编码为 CSI 码 u, 可区分 Shift+Enter 等组合.
    let z_kitty = c_modified_keys & 4 != 0;
    let z_kitty_code = |c_code: u32| Some(if z_modifier == 1 { format!("\u{1b}[{c_code}u") } else { format!("\u{1b}[{c_code};{z_modifier}u") });
    // xterm modifyOtherKeys: 模式1保留普通Ctrl快捷键; 模式2才编码Ctrl, 避免打断tmux前缀与编辑器操作.
    let z_extended = !z_kitty && (c_modified_keys == 2 && z_modifier > 1 || c_modified_keys == 1 && c_alt);
    // macOS 习惯 Option+←/→ 按单词移动, 发送 readline 通用的 ESC b / ESC f.
    if cfg!(target_os = "macos") && c_alt && !c_control && !c_shift && c_modified_keys == 0 {
        if z_key(Key::LeftArrow) {
            return Some("\u{1b}b".into());
        }
        if z_key(Key::RightArrow) {
            return Some("\u{1b}f".into());
        }
    }
    // 组合键产生的普通字符按文字输入: Windows 的 AltGr 即 Ctrl+Alt (如 @ { €), macOS 的 Option 用于输入特殊字符.
    // Slint 的方向键、功能键位于私用区, 不受影响.
    let z_printable = c_text.chars().count() == 1
        && c_text.chars().all(|c| !c.is_control() && !('\u{e000}'..='\u{f8ff}').contains(&c));
    if z_printable && c_alt && !z_extended {
        if cfg!(windows) && c_control && !c_text.chars().all(|c| c.is_ascii_alphanumeric()) {
            return None;
        }
        if cfg!(target_os = "macos") && !c_control {
            return None;
        }
    }
    for (z_arrow, z_code) in [
        (Key::UpArrow, "A"),
        (Key::DownArrow, "B"),
        (Key::RightArrow, "C"),
        (Key::LeftArrow, "D"),
        (Key::Home, "H"),
        (Key::End, "F"),
    ] {
        if z_key(z_arrow) {
            if z_modifier == 1
                && c_application_cursor
                && matches!(
                    z_arrow,
                    Key::UpArrow | Key::DownArrow | Key::RightArrow | Key::LeftArrow
                )
            {
                return Some(format!("\u{1b}O{z_code}"));
            }
            return Some(if z_modifier == 1 {
                format!("\u{1b}[{z_code}")
            } else {
                format!("\u{1b}[1;{z_modifier}{z_code}")
            });
        }
    }
    for (z_key_value, z_code) in [
        (Key::Insert, 2),
        (Key::Delete, 3),
        (Key::PageUp, 5),
        (Key::PageDown, 6),
    ] {
        if z_key(z_key_value) {
            return Some(if z_modifier == 1 {
                format!("\u{1b}[{z_code}~")
            } else {
                format!("\u{1b}[{z_code};{z_modifier}~")
            });
        }
    }
    for (z_key_value, z_code) in [
        (Key::F1, 11),
        (Key::F2, 12),
        (Key::F3, 13),
        (Key::F4, 14),
        (Key::F5, 15),
        (Key::F6, 17),
        (Key::F7, 18),
        (Key::F8, 19),
        (Key::F9, 20),
        (Key::F10, 21),
        (Key::F11, 23),
        (Key::F12, 24),
    ] {
        if z_key(z_key_value) {
            // F1-F4 为 SS3 P-S, 带修饰时按 xterm 写作 CSI 1;修饰 P-S.
            let z_letter = char::from_u32((z_code - 11) as u32 + 'P' as u32)?;
            return Some(if z_code <= 14 && z_modifier == 1 {
                format!("\u{1b}O{z_letter}")
            } else if z_code <= 14 {
                format!("\u{1b}[1;{z_modifier}{z_letter}")
            } else if z_modifier == 1 {
                format!("\u{1b}[{z_code}~")
            } else {
                format!("\u{1b}[{z_code};{z_modifier}~")
            });
        }
    }
    if z_key(Key::Backspace) {
        if z_kitty && z_modifier > 1 {
            return z_kitty_code(127);
        }
        return Some(if z_extended {
            format!("\u{1b}[27;{z_modifier};127~")
        } else if c_alt {
            "\u{1b}\u{7f}".into()
        } else if c_control {
            "\u{8}".into()
        } else { "\u{7f}".into() });
    }
    if z_key(Key::Escape) {
        return if z_kitty { z_kitty_code(27) } else { Some("\u{1b}".into()) };
    }
    if z_key(Key::Tab) {
        if z_kitty && z_modifier > 1 {
            return z_kitty_code(9);
        }
        return Some(if z_extended { format!("\u{1b}[27;{z_modifier};9~") }
            else { if c_shift { "\u{1b}[Z" } else { "\t" }.into() });
    }
    if z_key(Key::Return) || c_text.as_str() == "\r" || c_text.as_str() == "\n" {
        if z_kitty && z_modifier > 1 {
            return z_kitty_code(13);
        }
        // Ctrl+Enter 发送换行符 (同 Ctrl+J): Shell 中等同回车, Claude Code、Pi 等用作输入框换行.
        return Some(if z_extended {
            format!("\u{1b}[27;{z_modifier};13~")
        } else if c_alt { "\u{1b}\r".into() } else if c_control { "\n".into() } else { "\r".into() });
    }
    if z_kitty && (c_control || c_alt) && c_text.chars().count() == 1 {
        // Kitty 协议用未按 Shift 的键值, Shift 已在修饰位中; 符号按美式键位还原 (如 Ctrl+Shift+/ 的 ? 还原为 /).
        return c_text.chars().next().and_then(|c| {
            let l_base = if c_shift {
                "~!@#$%^&*()_+{}|:\"<>?".find(c).map(|i| "`1234567890-=[]\\;',./".as_bytes()[i] as char).unwrap_or(c)
            } else { c };
            z_kitty_code(l_base.to_ascii_lowercase() as u32)
        });
    }
    if z_extended && c_text.chars().count() == 1 {
        return c_text.chars().next().map(|c| format!("\u{1b}[27;{z_modifier};{}~", c as u32));
    }
    if c_control {
        if c_shift && c_text.eq_ignore_ascii_case("v") {
            return None;
        }
        if let Some(z_char) = c_text
            .chars()
            .next()
            .filter(|_| c_text.chars().count() == 1)
        {
            let z_ascii = z_char.to_ascii_uppercase() as u32;
            if (64..=95).contains(&z_ascii) {
                return Some(char::from_u32(z_ascii & 31)?.to_string());
            }
            // xterm 对数字和 / 的传统映射: Ctrl+2 为 NUL, Ctrl+3..7 为 ESC..US, Ctrl+8 为 DEL, Ctrl+/ 同 Ctrl+_.
            let z_code = match z_char {
                ' ' | '2' => Some(0x00),
                '3' => Some(0x1b),
                '4' => Some(0x1c),
                '5' => Some(0x1d),
                '6' => Some(0x1e),
                '7' | '/' => Some(0x1f),
                '8' => Some(0x7f),
                _ => None,
            };
            if let Some(z_code) = z_code {
                return Some(char::from(z_code).to_string());
            }
        }
    }
    if c_alt && c_text.chars().count() == 1 {
        return Some(format!("\u{1b}{c_text}"));
    }
    None
}

// 滚轮: 远端开启鼠标上报时发送滚轮事件 (tmux、vim mouse=a 等), 否则在全屏程序中按方向键滚动 (less、man 等).
fn terminal_wheel(c_lines: i32, c_row: i32, c_col: i32, c_mouse_mode: i32, c_application_cursor: bool) -> String {
    let z_up = c_lines > 0;
    let z_count = c_lines.unsigned_abs().min(10) as usize;
    let z_button = if z_up { 64 } else { 65 };
    let z_event = match c_mouse_mode {
        2 => format!("\u{1b}[<{z_button};{};{}M", c_col + 1, c_row + 1),
        // 传统编码每个坐标占一个字节; 只发 ASCII, 坐标超过 95 时取最大值.
        1 => format!(
            "\u{1b}[M{}{}{}",
            char::from(32 + z_button as u8),
            char::from(33 + c_col.clamp(0, 94) as u8),
            char::from(33 + c_row.clamp(0, 94) as u8)
        ),
        _ => format!("\u{1b}{}{}", if c_application_cursor { 'O' } else { '[' }, if z_up { 'A' } else { 'B' }),
    };
    z_event.repeat(z_count)
}

fn apply_monitor_history(c_ui: &AppWindow, c_session: Option<&Value>) {
    let z_monitor = c_session
        .and_then(|c| c.get("monitor"))
        .unwrap_or(&Value::Null);
    let z_interfaces = array(z_monitor, "interfaces");
    let z_selected = c_ui.get_network_interface_name();
    let z_index = z_interfaces
        .iter()
        .position(|c| string(c, "name") == z_selected.as_str());
    let mut z_names = vec![SharedString::from("全部网卡")];
    z_names.extend(z_interfaces.iter().map(|c| string(c, "name").into()));
    // 复用原模型逐行比较, 数据未变时不标脏, 避免软件渲染整窗重画.
    c_ui.set_network_interfaces(update_rows(c_ui.get_network_interfaces(), z_names));
    c_ui.set_network_interface_index(z_index.map(|c| c as i32 + 1).unwrap_or(0));
    if z_index.is_none() {
        c_ui.set_network_interface_name("".into());
    }
    let z_source = z_index.map(|c| &z_interfaces[c]).unwrap_or(z_monitor);
    let z_history = z_source.get("history").unwrap_or(&Value::Null);
    let z_sampled = number(z_monitor, "memory_total_bytes") > 0.0;
    c_ui.set_network_download_rate(
        if z_sampled {
            format!("{}/s", format_bytes(number(z_source, "rx_bytes_per_sec")))
        } else {
            "—".into()
        }
        .into(),
    );
    c_ui.set_network_upload_rate(
        if z_sampled {
            format!("{}/s", format_bytes(number(z_source, "tx_bytes_per_sec")))
        } else {
            "—".into()
        }
        .into(),
    );
    let z_rx = array(z_history, "rx_bytes_per_sec");
    let z_tx = array(z_history, "tx_bytes_per_sec");
    let z_len = z_rx.len().min(z_tx.len()).min(120);
    let z_rx = z_rx[z_rx.len() - z_len..].iter().map(|c| c.as_f64().unwrap_or(0.0)).collect::<Vec<_>>();
    let z_tx = z_tx[z_tx.len() - z_len..].iter().map(|c| c.as_f64().unwrap_or(0.0)).collect::<Vec<_>>();
    // 上下行共用同一峰值缩放, 两条曲线可直接比较高低.
    let z_peak = z_rx.iter().chain(&z_tx).copied().fold(1024.0, f64::max);
    let (z_rx_line, z_rx_area) = network_paths(&z_rx, z_peak);
    let (z_tx_line, z_tx_area) = network_paths(&z_tx, z_peak);
    c_ui.set_network_rx_line(z_rx_line.into());
    c_ui.set_network_rx_area(z_rx_area.into());
    c_ui.set_network_tx_line(z_tx_line.into());
    c_ui.set_network_tx_area(z_tx_area.into());
    c_ui.set_network_peak(if z_sampled { format!("峰值 {}/s", format_bytes(z_peak)) } else { String::new() }.into());
}

// 采样两两合并为 60 个点; 时间轴固定占满图框, 新采样从右侧进入, 未采样部分保持基线.
fn network_paths(c_values: &[f64], c_peak: f64) -> (String, String) {
    let z_normalized = c_values
        .chunks(2)
        .map(|c| (c.iter().copied().fold(0.0, f64::max) / c_peak).clamp(0.0, 1.0))
        .collect::<Vec<_>>();
    let mut z_line = String::with_capacity(1024);
    for z_x in 0usize..60 {
        let z_value = z_x
            .checked_sub(60 - z_normalized.len())
            .and_then(|i| z_normalized.get(i))
            .copied()
            .unwrap_or(0.0);
        z_line.push_str(&format!("{} {z_x} {:.2} ", if z_x == 0 { "M" } else { "L" }, 100.0 - z_value * 96.0));
    }
    let z_area = if z_normalized.is_empty() { String::new() } else { format!("{z_line}L 59 100 L 0 100 Z") };
    (z_line, z_area)
}

fn apply_monitor_processes(c_ui: &AppWindow, c_session: Option<&Value>) {
    let z_monitor = c_session
        .and_then(|c| c.get("monitor"))
        .unwrap_or(&Value::Null);
    let mut z_processes = array(z_monitor, "processes").iter().collect::<Vec<_>>();
    let (z_key, z_secondary) = if c_ui.get_process_sort() == 1 {
        ("memory_bytes", "cpu_percent")
    } else {
        ("cpu_percent", "memory_bytes")
    };
    z_processes.sort_by(|a, b| {
        number(b, z_key)
            .total_cmp(&number(a, z_key))
            .then_with(|| number(b, z_secondary).total_cmp(&number(a, z_secondary)))
            .then_with(|| number(a, "pid").total_cmp(&number(b, "pid")))
    });
    c_ui.set_processes(update_rows(
        c_ui.get_processes(),
        z_processes
            .into_iter()
            .take(128)
            .map(|c| ProcessRow {
                pid: number(c, "pid").clamp(0.0, i32::MAX as f64) as i32,
                name: string(c, "name").into(),
                // 按整机占比后, 多核机器上普通进程的数值很小, 10% 以下多保留一位小数.
                cpu_text: { let l_cpu = number(c, "cpu_percent"); if l_cpu < 10.0 { format!("{l_cpu:.2}%") } else { format!("{l_cpu:.1}%") } }.into(),
                memory_text: format_bytes(number(c, "memory_bytes")).into(),
            })
            .collect::<Vec<_>>(),
    ));
}

fn monitor_text(c_session: &Value) -> String {
    let Some(z_monitor) = c_session.get("monitor").filter(|c_item| c_item.is_object()) else {
        return "等待监控数据".into();
    };
    if !string(z_monitor, "error").is_empty() {
        return format!("监控暂不可用\n{}", string(z_monitor, "error"));
    }
    String::new()
}
// 任务详情: 先说明执行了什么 (命令或传输路径、服务器、来源、状态与用时), 再给出结果输出.
fn task_output(c_ui: &AppWindow, c_task: &Value) -> String {
    use std::fmt::Write as _;
    let z_id = string(c_task, "task_id");
    let z_row = c_ui.get_tasks().iter().find(|c| c.id.as_str() == z_id);
    let z_kind = string(c_task, "kind");
    let z_code = c_task.get("exit_code").and_then(Value::as_i64);
    let z_status = match string(c_task, "status") {
        "completed" if z_code.is_some_and(|c| c != 0) => "执行失败",
        "completed" if z_kind == "command" => "执行成功",
        l_status => status_label(l_status),
    };
    let mut z_output = String::new();
    match z_kind {
        "upload" => {
            let _ = writeln!(z_output, "上传  {}", string(c_task, "local"));
            let _ = writeln!(z_output, "到    {}", string(c_task, "label"));
        }
        "download" => {
            let _ = writeln!(z_output, "下载  {}", string(c_task, "label"));
            let _ = writeln!(z_output, "到    {}", string(c_task, "local"));
        }
        _ => {
            let _ = writeln!(z_output, "命令  {}", string(c_task, "label").replace('\n', "\n      "));
        }
    }
    if let Some(z_row) = &z_row {
        let _ = writeln!(z_output, "服务器  {}  ·  来源 {}", z_row.server, z_row.source);
    }
    let _ = write!(z_output, "状态  {z_status}");
    if let Some(z_code) = z_code {
        let _ = write!(z_output, "  ·  退出码 {z_code}");
    }
    if let Some(z_timing) = task_timing(c_task) {
        let _ = write!(z_output, "  ·  {z_timing}");
    }
    z_output.push('\n');
    if z_kind != "command" {
        let l_total = number(c_task, "total");
        let _ = writeln!(
            z_output,
            "大小  {}{}",
            format_bytes(number(c_task, "bytes")),
            if l_total > 0.0 { format!(" / {}", format_bytes(l_total)) } else { String::new() }
        );
        if boolean(c_task, "directory") {
            let _ = writeln!(z_output, "文件  {:.0} / {:.0}", number(c_task, "files_done"), number(c_task, "files_total"));
            if string(c_task, "status") == "running" {
                let _ = writeln!(z_output, "当前  {}", string(c_task, "current"));
            }
        }
        if string(c_task, "status") == "retrying" {
            z_output.push_str("正在自动重连, 恢复后从已保留进度继续; 可取消任务.\n");
        }
    }
    let mut z_section = |c_title: &str, c_text: &str| {
        if !c_text.is_empty() {
            let _ = write!(z_output, "\n[{c_title}]\n{}", c_text.trim_end_matches('\n'));
            z_output.push('\n');
        }
    };
    z_section("输出", string(c_task, "stdout"));
    z_section(if z_code == Some(0) { "提示输出" } else { "错误输出" }, string(c_task, "stderr"));
    z_section(if string(c_task, "status") == "retrying" { "恢复原因" } else { "错误" }, string(c_task, "error"));
    if z_kind == "command" && matches!(string(c_task, "status"), "completed") && string(c_task, "stdout").is_empty() && string(c_task, "stderr").is_empty() {
        z_output.push_str("\n(无输出)\n");
    }
    if boolean(c_task, "truncated") {
        z_output.push_str("\n输出已达显示上限.\n");
    }
    z_output
}
// 已开始的任务给出用时; 传输完成后附平均速度.
fn task_timing(c_task: &Value) -> Option<String> {
    let z_start = c_task.get("started_at_ms").and_then(Value::as_u64)?;
    let z_end = c_task.get("finished_at_ms").and_then(Value::as_u64)?;
    let z_ms = z_end.saturating_sub(z_start);
    let z_seconds = z_ms / 1000;
    let mut z_text = if z_seconds >= 60 {
        format!("用时 {}分{}秒", z_seconds / 60, z_seconds % 60)
    } else {
        format!("用时 {:.1} 秒", z_ms as f64 / 1000.0)
    };
    if string(c_task, "kind") != "command" && string(c_task, "status") == "completed" && z_ms > 0 {
        let _ = std::fmt::Write::write_fmt(&mut z_text, format_args!(" · 平均 {}/s", format_bytes(number(c_task, "bytes") * 1000.0 / z_ms as f64)));
    }
    Some(z_text)
}
fn array<'a>(c_value: &'a Value, c_key: &str) -> &'a [Value] {
    c_value
        .get(c_key)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
}
fn string<'a>(c_value: &'a Value, c_key: &str) -> &'a str {
    c_value.get(c_key).and_then(Value::as_str).unwrap_or("")
}
fn number(c_value: &Value, c_key: &str) -> f64 {
    c_value.get(c_key).and_then(Value::as_f64).unwrap_or(0.0)
}
fn boolean(c_value: &Value, c_key: &str) -> bool {
    c_value.get(c_key).and_then(Value::as_bool).unwrap_or(false)
}
fn format_bytes(c_bytes: f64) -> String {
    // 沿用1024进位的数值, 界面统一使用常见文件单位, 不让用户手动换算.
    if c_bytes >= 1099511627776.0 {
        format!("{:.1} TB", c_bytes / 1099511627776.0)
    } else if c_bytes >= 1073741824.0 {
        format!("{:.1} GB", c_bytes / 1073741824.0)
    } else if c_bytes >= 1048576.0 {
        format!("{:.1} MB", c_bytes / 1048576.0)
    } else if c_bytes >= 1024.0 {
        format!("{:.1} KB", c_bytes / 1024.0)
    } else {
        format!("{:.0} B", c_bytes)
    }
}
fn parse_color(c_text: &str, c_default: slint::Color) -> slint::Color {
    u32::from_str_radix(c_text.trim_start_matches('#'), 16)
        .ok()
        .map(|c_rgb| {
            slint::Color::from_rgb_u8((c_rgb >> 16) as u8, (c_rgb >> 8) as u8, c_rgb as u8)
        })
        .unwrap_or(c_default)
}
// MCP 侧栏的动作名与风险等级 (0 只读, 1 连接, 2 文件传输, 3 执行命令); 动作名说明在哪执行, 不展开具体命令.
fn mcp_action(c_method: &str) -> (&str, i32) {
    match c_method {
        "state" => ("查看状态", 0),
        "read_terminal" => ("读取主终端", 0),
        "list_files" => ("浏览远端目录", 0),
        "list_local_files" => ("浏览本机目录", 0),
        "get_task" => ("查询任务", 0),
        "cancel_task" => ("取消任务", 0),
        "connect" => ("连接", 1),
        "disconnect" => ("断开", 1),
        "reconnect" => ("重新连接", 1),
        "open_terminal" => ("打开终端", 1),
        "upload_file" => ("上传文件", 2),
        "download_file" => ("下载文件", 2),
        "upload_directory" => ("上传文件夹", 2),
        "download_directory" => ("下载文件夹", 2),
        "write_terminal" => ("输入主终端", 3),
        "input_admin_password" => ("输入管理员密码", 3),
        "run_command" => ("独立执行命令", 3),
        c_other => (method_label(c_other), 0),
    }
}
fn method_label(c_method: &str) -> &str {
    match c_method {
        "state" => "读取状态",
        "read_terminal" => "读取终端",
        "write_terminal" => "输入终端",
        "input_admin_password" => "输入管理员密码",
        "get_task" => "查询任务",
        "connect" => "连接",
        "open_terminal" => "重新连接终端",
        "close_terminal" => "关闭终端",
        "save_connection" => "保存连接",
        "save_saved_command" => "保存常用命令",
        "delete_saved_command" => "删除常用命令",
        "save_command_group" => "保存命令分组",
        "delete_command_group" => "删除命令分组",
        "save_connection_group" => "保存连接分组",
        "delete_connection_group" => "删除连接分组",
        "save_proxy" => "保存代理",
        "delete_proxy" => "删除代理",
        "set_connection_proxy" => "设置代理",
        "send_saved_command" => "发送常用命令",
        "send_command" => "发送命令",
        "list_local_files" => "浏览本地目录",
        "remove_connection" => "删除连接",
        "import_finalshell" => "导入",
        "export_finalshell" => "导出",
        "import_commands" => "导入命令",
        "export_commands" => "导出命令",
        "list_files" => "目录浏览",
        "set_permissions" => "修改权限",
        "rename_file" => "重命名",
        "create_directory" => "新建文件夹",
        "create_file" => "新建文件",
        "focus_session" => "切换标签",
        "set_terminal_font" => "保存终端字体",
        "set_terminal_scrollback" => "保存历史行数",
        "set_tab_show_group" => "保存标签分组显示",
        "set_download_dir" => "设置下载目录",
        "set_connection_columns" => "保存列宽",
        "delete_files" => "删除",
        "delete_files_fast" => "快速删除",
        "copy_file" => "创建副本",
        "upload_file" => "上传任务创建",
        "download_file" => "下载任务创建",
        "upload_directory" | "download_directory" => "文件夹传输任务创建",
        "run_command" => "命令任务创建",
        "trust_host" => "主机信任处理",
        "answer_auth" => "认证回答提交",
        "cancel_task" => "任务取消",
        "disconnect" => "断开",
        "new_tab" => "新建标签",
        "clear_recent_connections" => "删除快速连接记录",
        "move_tab" => "移动标签",
        "set_expanded_groups" => "保存分组展开状态",
        "reconnect" => "重新连接",
        _ => "操作",
    }
}
// 状态栏消息类别, 与界面 status-kind 一致.
const NOTE_INFO: i32 = 0;
const NOTE_OK: i32 = 1;
const NOTE_WARN: i32 = 2;
const NOTE_ERROR: i32 = 3;
fn notify(c_ui: &AppWindow, c_kind: i32, c_text: impl Into<SharedString>) {
    c_ui.invoke_notify(c_kind, c_text.into());
}
// 失败可直接重做时附带操作, 状态栏显示“重试”.
fn notify_retry(c_ui: &AppWindow, c_text: String, c_action: &str, c_arg: SharedString) {
    notify(c_ui, NOTE_ERROR, c_text);
    c_ui.set_status_retry(c_action.into());
    c_ui.set_status_retry_arg(c_arg);
}
enum ConflictChoice { Overwrite, KeepBoth, Skip }

// 原生确认框最多三个按钮, 使用现有界面对话框同时保留四种选择; 多个批次的询问依次显示.
async fn ask_conflict(c_weak: slint::Weak<AppWindow>, c_names: &[String]) -> Option<ConflictChoice> {
    let z_names = c_names.join("\n");
    match ask_in_ui(c_weak, move |c_ui| {
        c_ui.set_conflict_kind(0);
        c_ui.set_conflict_names(z_names.into());
    }).await? {
        1 => Some(ConflictChoice::Overwrite),
        2 => Some(ConflictChoice::KeepBoth),
        3 => Some(ConflictChoice::Skip),
        _ => None,
    }
}
// 下载多项到默认目录前确认目录 (界面内对话框, 与同名询问同一风格), 确认返回 true.
async fn confirm_download(c_weak: slint::Weak<AppWindow>, c_count: usize) -> bool {
    ask_in_ui(c_weak, move |c_ui| {
        c_ui.set_conflict_kind(1);
        c_ui.set_conflict_count(c_count as i32);
    }).await == Some(1)
}
// 界面内询问框共用一个应答通道: 全局串行, 多个批次的询问依次显示, 不会互相覆盖应答.
async fn ask_in_ui(c_weak: slint::Weak<AppWindow>, c_setup: impl FnOnce(&AppWindow) + Send + 'static) -> Option<i32> {
    static PROMPT: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _z_prompt = PROMPT.lock().await;
    let (z_tx, z_rx) = tokio::sync::oneshot::channel();
    c_weak.upgrade_in_event_loop(move |c_ui| {
        CONFLICT_ANSWER.with_borrow_mut(|c| *c = Some(z_tx));
        c_setup(&c_ui);
        c_ui.set_conflict_open(true);
    }).ok()?;
    z_rx.await.ok()
}
// 传输失败弹窗: 对话框打开期间新的失败先排队, 关闭后合并到下一个对话框, 批量失败不会叠出多个窗口.
fn alert_transfer_failures(c_runtime: &tokio::runtime::Handle, c_items: Vec<String>) {
    static PENDING: Mutex<(bool, Vec<String>)> = Mutex::new((false, Vec::new()));
    {
        let mut l_pending = PENDING.lock().unwrap();
        l_pending.1.extend(c_items);
        if std::mem::replace(&mut l_pending.0, true) {
            return;
        }
    }
    c_runtime.spawn(async {
        loop {
            let l_items = {
                let mut l_pending = PENDING.lock().unwrap();
                if l_pending.1.is_empty() {
                    l_pending.0 = false;
                    break;
                }
                std::mem::take(&mut l_pending.1)
            };
            let l_more = if l_items.len() > 5 { format!("\n\n… 另有 {} 个失败, 详见任务列表", l_items.len() - 5) } else { String::new() };
            rfd::AsyncMessageDialog::new()
                .set_title("传输失败")
                .set_level(rfd::MessageLevel::Error)
                .set_description(format!("{}{l_more}\n\n目标文件保持原样, 未完成的临时文件已清理 (网络中断的任务已先自动重连续传). 请查看原因后重新传输.", l_items[..l_items.len().min(5)].join("\n\n")))
                .set_buttons(rfd::MessageButtons::Ok)
                .show()
                .await;
        }
    });
}
// "名称 (1).扩展名" 形式的未占用名称.
fn unique_name(c_name: &str, c_taken: impl Fn(&str) -> bool) -> String {
    let (z_stem, z_ext) = match c_name.rfind('.') {
        Some(i) if i > 0 => (&c_name[..i], &c_name[i..]),
        _ => (c_name, ""),
    };
    (1..1000).map(|i| format!("{z_stem} ({i}){z_ext}")).find(|c| !c_taken(c)).unwrap_or_else(|| c_name.to_owned())
}
// 常见的服务器英文错误 (SFTP 状态、rm/mkdir 等命令输出) 换成中文, 其余原样保留; 只用于界面提示, MCP 仍返回原文.
// SFTP 状态错误会显示为"原因: 原因", 替换后合并为一处.
fn friendly_error(c_error: &str) -> String {
    const TEXTS: [(&str, &str); 18] = [
        ("Permission denied", "权限不足"),
        ("Operation not permitted", "操作不被允许"),
        ("No such file or directory", "文件或目录不存在"),
        ("No such file", "文件或目录不存在"),
        ("Directory not empty", "目录不为空"),
        ("File exists", "文件已存在"),
        ("Not a directory", "不是目录"),
        ("Is a directory", "是一个目录"),
        ("Read-only file system", "只读文件系统"),
        ("No space left on device", "磁盘空间不足"),
        ("Disk quota exceeded", "超出磁盘配额"),
        ("Device or resource busy", "设备或资源正忙"),
        ("Text file busy", "文件正在被使用"),
        ("File name too long", "文件名过长"),
        ("Too many levels of symbolic links", "符号链接层级过多"),
        ("Connection refused", "连接被拒绝"),
        ("Connection reset by peer", "连接被对方重置"),
        ("cannot remove", "无法删除"),
    ];
    let mut z_text = c_error.to_owned();
    for (l_from, l_to) in TEXTS {
        if z_text.contains(l_from) {
            z_text = z_text.replace(l_from, l_to).replace(&format!("{l_to}: {l_to}"), l_to);
        }
    }
    z_text
}
// 批量创建传输任务中途失败: 一个都没创建时不再显示"已加入 0 个".
fn batch_error(c_kind: &str, c_count: usize, c_err: &anyhow::Error) -> String {
    if c_count == 0 {
        format!("{c_kind}任务创建失败: {c_err}")
    } else {
        format!("已加入 {c_count} 个{c_kind}任务, 后续任务创建失败: {c_err}")
    }
}
// 单个标签的输入发送任务: 连续键入与粘贴合并为不超过 16 KiB 的块按序发送; 标签关闭后发送失败即结束.
async fn terminal_input_worker(
    c_backend: Arc<Backend>,
    c_weak: slint::Weak<AppWindow>,
    c_session: String,
    c_epochs: InputEpochs,
    mut c_rx: tokio::sync::mpsc::Receiver<(String, (u64, u64))>,
) {
    let mut z_next = None;
    loop {
        let (mut z_data, z_epoch) = match z_next.take() {
            Some(l_item) => l_item,
            None => match c_rx.recv().await {
                Some(l_item) => l_item,
                None => break,
            },
        };
        // 只合并同一代次的连续输入; 新代次的留到下一轮.
        while z_data.len() < 16384 {
            match c_rx.try_recv() {
                Ok((mut l_data, l_epoch)) if l_epoch == z_epoch => {
                    z_data.push_str(&l_data);
                    l_data.zeroize();
                }
                Ok(l_item) => {
                    z_next = Some(l_item);
                    break;
                }
                Err(_) => break,
            }
        }
        let mut z_offset = 0;
        while z_offset < z_data.len() {
            if c_epochs.lock().unwrap().get(&c_session).copied().unwrap_or(0) != z_epoch.0 {
                break;
            }
            let mut z_end = (z_offset + 16384).min(z_data.len());
            while !z_data.is_char_boundary(z_end) {
                z_end -= 1;
            }
            let mut l_args = json!({"session_id":c_session,"data":&z_data[z_offset..z_end]});
            // 带终端代次的输入 (确认后的粘贴) 由后台核对, 终端期间重开过就拒绝.
            if z_epoch.1 != 0 {
                l_args["terminal"] = json!(z_epoch.1);
            }
            let z_result = c_backend.call("write_terminal", l_args, Actor::User).await;
            if let Err(err) = z_result {
                show_notice(c_weak.clone(), NOTE_ERROR, format!("终端输入失败: {err}"));
                // 带终端代次的输入 (确认后的粘贴) 被拒多因终端已重开: 只丢弃这一段, 继续发送之后排队的按键;
                // 若实为标签已关闭, 下一条输入同样失败后再结束.
                if z_epoch.1 != 0 {
                    break;
                }
                // 其他错误 (如标签已关闭) 即结束本任务, 分发处随后清理; 再有输入时重新创建.
                z_data.zeroize();
                return;
            }
            z_offset = z_end;
            tokio::task::yield_now().await;
        }
        z_data.zeroize();
    }
}
// MCP 设置页: 开关、端口草稿与运行状态; 运行中时生成网址、JSON 配置和交给 AI 的安装说明.
fn apply_mcp_settings(c_ui: &AppWindow, c_enabled: bool, c_port: u64, c_status: &std::result::Result<String, String>) {
    c_ui.set_mcp_enabled(c_enabled);
    c_ui.set_mcp_port(c_port.to_string().into());
    c_ui.set_mcp_port_draft(c_port.to_string().into());
    // 开关或端口变了, 旧的自检结果不再适用.
    c_ui.set_mcp_check_title("".into());
    let Ok(z_url) = c_status else {
        c_ui.set_mcp_url("".into());
        c_ui.set_mcp_config("".into());
        c_ui.set_mcp_prompt("".into());
        c_ui.set_mcp_error(if c_enabled { c_status.clone().err().unwrap_or_default() } else { String::new() }.into());
        return;
    };
    let z_config = json!({"mcpServers":{"hipshell":{"type":"http","url":z_url}}});
    // 交给 AI 的一次性安装说明: 网址固定, 按常见客户端列出登记方式, 其余客户端用通用 JSON.
    let z_prompt = format!(
        "请把本机的 HiPShell 注册为你的 MCP 服务器 (HTTP 方式, 即 Streamable HTTP). 只需配置一次.\n\n\
         - 名称: hipshell\n- 网址: {z_url}\n- 不需要令牌、请求头或启动命令.\n\n\
         按你所在的客户端登记:\n\
         - Claude Code (含 Claude Desktop 的 Code): 所有项目可用请运行 claude mcp add --transport http --scope user hipshell {z_url}; \
         只给当前项目用则在项目根目录的 .mcp.json 写入下面的 JSON.\n\
         - Codex: 在 ~/.codex/config.toml 加入两行: [mcp_servers.hipshell] 与 url = \"{z_url}\".\n\
         - 其他客户端 (Cursor 等): 写入其 MCP 配置文件, 通用 JSON (mcpServers 格式):\n{}\n\n\
         登记后重新加载 MCP, 并调用 hipshell 的 state 工具确认能看到会话列表.\n\n\
         说明: 使用时 HiPShell 需要保持运行; 不需要服务器地址、账号或密码; \
         只有在 HiPShell 中勾选“允许 MCP 使用此连接”的服务器可以被操作. 具体用法在连接后的 instructions 中.",
        serde_json::to_string_pretty(&z_config).unwrap_or_default(),
    );
    c_ui.set_mcp_error("".into());
    c_ui.set_mcp_url(z_url.as_str().into());
    c_ui.set_mcp_config(z_config.to_string().into());
    c_ui.set_mcp_prompt(z_prompt.into());
}

// 标签 ID → 输入代次, 见终端输入分发处.
type InputEpochs = Arc<Mutex<HashMap<String, u64>>>;
// 终端输入总队列 (标签, 数据, (入队时的输入代次, 要求的终端代次)) 与代次表.
type InputQueue = (tokio::sync::mpsc::Sender<(String, String, (u64, u64))>, InputEpochs);

// 入队时记下该标签当前的输入代次; 空数据表示标签已关闭. 队列满时返回 false.
// c_terminal: 非 0 时只写入该代次的终端 (后台核对), 0 为不限.
fn enqueue_input(c_queue: &InputQueue, c_session: String, c_data: String, c_terminal: u64) -> bool {
    let z_epoch = c_queue.1.lock().unwrap().get(&c_session).copied().unwrap_or(0);
    c_queue.0.try_send((c_session, c_data, (z_epoch, c_terminal))).is_ok()
}

fn show_notice(c_weak: slint::Weak<AppWindow>, c_kind: i32, c_message: String) {
    let _ = slint::invoke_from_event_loop(move || {
        if let Some(z_ui) = c_weak.upgrade() {
            notify(&z_ui, c_kind, c_message);
        }
    });
}
fn clear_secrets(c_args: &mut Value) {
    if let Some(Value::String(z_secret)) = c_args.get_mut("password") {
        z_secret.zeroize();
    }
    if let Some(Value::Array(z_answers)) = c_args.get_mut("answers") {
        for z_answer in z_answers {
            if let Value::String(z_secret) = z_answer {
                z_secret.zeroize();
            }
        }
    }
}
