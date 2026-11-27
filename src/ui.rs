use crate::{
    backend::{Backend, local_file_name, local_name_key},
    i18n::{self, is_en, localize_error as friendly_error, t},
    model::Actor,
};
use serde_json::{Value, json};
use slint::{ComponentHandle, Model, ModelRc, SharedString, VecModel};
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    rc::Rc,
    sync::{
        Arc, Mutex, PoisonError,
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
    // 批量上传节流期间暂存的待补刷目录 ((会话, 路径); 路径为空表示整会话失效), 在在途列表回包或同目录上传结束后补刷.
    static PENDING_UPLOAD_REFRESH: RefCell<std::collections::HashSet<(String, String)>> = RefCell::default();
    static CONFLICT_ANSWER: RefCell<Option<tokio::sync::oneshot::Sender<i32>>> = const { RefCell::new(None) };
    // 设置 → 外观 "显示分组": 服务器名称前是否加 [分组], 标签、快速连接、状态栏、任务等处统一.
    static SHOW_GROUP: Cell<bool> = const { Cell::new(true) };
    // 已见过的传输任务 (新出现的用户任务自动打开传输面板) 与面板中已清除的任务.
    static TRANSFER_SEEN: RefCell<std::collections::HashSet<String>> = RefCell::default();
    static TRANSFER_CLEARED: RefCell<std::collections::HashSet<String>> = RefCell::default();
    // 上次格式化任务完成时间时的本地日期 (年, 月, 日); 跨午夜时触发重绘, 当天任务随之补上月日.
    static LAST_LOCAL_DAY: Cell<(i32, u32, u32)> = const { Cell::new((0, 0, 0)) };
    // 认证弹窗当前对应的后台请求标识, 提交回答时带回核对.
    static AUTH_REQUEST: RefCell<String> = RefCell::default();
    // 各会话当前所展示文件列表所属连接的代次: 对列表中的文件发起操作时带上, 后台据此拒绝已重连标签上的旧操作.
    static LISTED_LINK: RefCell<HashMap<String, u64>> = RefCell::default();
}

// 关闭某会话上打开的文件弹窗 (改权限、改名、新建、删除). 会话换成新连接后由调用方关闭: 新代次记下之前确认的操作
// 带着旧代次, 后台会拒绝; 之后才确认的只可能来自新列表上打开的弹窗.
fn close_file_dialogs(c_ui: &AppWindow, c_session: &str) {
    if c_ui.get_permissions_session().as_str() == c_session { c_ui.set_permissions_editing(false); }
    if c_ui.get_rename_session().as_str() == c_session { c_ui.set_rename_editing(false); }
    if c_ui.get_create_session().as_str() == c_session { c_ui.set_create_editing(false); }
    if c_ui.get_delete_session().as_str() == c_session { c_ui.set_delete_editing(false); }
}
// 为针对文件列表的操作带上列表所属连接的代次 (见 LISTED_LINK); 只能在界面线程调用.
fn attach_listed_link(c_args: &mut Value) {
    if c_args.get("_link").is_none()
        && let Some(l_link) = LISTED_LINK.with_borrow(|c| c.get(string(c_args, "session_id")).copied())
    {
        c_args["_link"] = json!(l_link);
    }
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
    if !c_path.starts_with('/') {
        return String::new();
    }
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

pub(crate) fn localize_file_time_label(c_label: &str) -> &'static str {
    match c_label {
        "修改时间 (UTC)" | "Modified (UTC)" => t("修改时间 (UTC)", "Modified (UTC)"),
        "修改时间 (服务器)" | "Modified (Server)" => t("修改时间 (服务器)", "Modified (Server)"),
        _ => t("修改时间", "Modified"),
    }
}

fn localize_file_kind(c_kind: &str) -> &'static str {
    match c_kind {
        "文件夹" | "Folder" => t("文件夹", "Folder"),
        "链接" | "Link" => t("链接", "Link"),
        _ => t("文件", "File"),
    }
}

pub(crate) fn localize_file_rows(c_rows: impl IntoIterator<Item = FileRow>) -> Vec<FileRow> {
    c_rows
        .into_iter()
        .map(|mut l_row| {
            let l_kind = localize_file_kind(l_row.kind_text.as_str());
            if l_row.kind_text.as_str() != l_kind {
                l_row.kind_text = l_kind.into();
                l_row.label = format!("{l_kind} {} {}", l_row.name, l_row.size_text).into();
            }
            l_row
        })
        .collect()
}

fn format_terminal_size(c_cols: i32, c_rows: i32) -> String {
    if is_en() {
        format!("{c_cols} × {c_rows}")
    } else {
        format!("{c_cols} 列 · {c_rows} 行")
    }
}

// MCP 自检代次: 切换语言或修改端口/开关时自增, 避免在途旧结果回写覆盖最新状态.
static MCP_CHECK_GEN: AtomicU64 = AtomicU64::new(0);

fn cached_file_rows(c_session: &str, c_path: &str) -> Option<(Vec<FileRow>, SharedString)> {
    FILE_CACHE.with_borrow(|c_cache| {
        c_cache
            .iter()
            .find(|c| c.0.0 == c_session && c.0.1 == c_path)
            .map(|c| (localize_file_rows(c.1.iter().cloned()), localize_file_time_label(c.2.as_str()).into()))
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
    let z_lang = i18n::from_code(string(&z_settings, "language"));
    i18n::set(z_lang);
    z_ui.set_language_index(z_lang as i32);
    let z_dark = z_settings
        .get("dark_theme")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    z_ui.set_theme_index(if z_dark { 0 } else { 1 });
    z_ui.set_is_macos(cfg!(target_os = "macos"));
    restore_layout(&z_ui, z_settings.get("layout").unwrap_or(&Value::Null));
    // 启动时同步把恢复后的监控侧栏展开状态告知后台 (监控仅在展开时运行), 避免异步无序到达.
    {
        let _l_guard = c_runtime.enter();
        c_backend.set_monitor_enabled(z_ui.get_monitor_expanded());
    }
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
            Ok(_) => notify(&z_ui, NOTE_OK, t("已恢复默认布局", "Layout reset to defaults")),
            Err(err) => notify(
                &z_ui,
                NOTE_ERROR,
                if is_en() { format!("Failed to save layout: {err:#}") } else { format!("保存布局失败: {err:#}") },
            ),
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
                let l_ok = confirm_dialog(
                    t("退出 HiPShell", "Quit HiPShell"),
                    t(
                        "确定退出 HiPShell? 所有连接会断开, 正在进行的传输和命令会中止.",
                        "Quit HiPShell? All sessions will disconnect and active transfers or commands will stop.",
                    ),
                    rfd::MessageLevel::Info,
                    t("退出", "Quit"),
                )
                .await;
                l_asking.store(false, Ordering::Release);
                if l_ok {
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
        let Some(z_port) = u16::try_from(c_port).ok().filter(|c| *c >= 1024) else {
            notify(&z_ui, NOTE_ERROR, t("端口需在 1024-65535 之间", "Port must be between 1024 and 65535"));
            return;
        };
        let (z_backend, z_weak) = (z_backend.clone(), z_weak.clone());
        z_runtime.spawn(async move {
            let z_result = z_backend.set_mcp(c_enabled, z_port).await;
            let _ = z_weak.upgrade_in_event_loop(move |c_ui| {
                let z_status = match z_result {
                    Ok(l_status) => l_status,
                    Err(err) => {
                        notify(&c_ui, NOTE_ERROR, friendly_error(&format!("{err:#}")));
                        return;
                    }
                };
                apply_mcp_settings(&c_ui, c_enabled, u64::from(z_port), &z_status);
                match &z_status {
                    Ok(_) => notify(&c_ui, NOTE_OK, t("MCP 服务已启动", "MCP server started")),
                    Err(_) if !c_enabled => notify(&c_ui, NOTE_OK, t("MCP 服务已关闭", "MCP server stopped")),
                    Err(err) => notify(&c_ui, NOTE_ERROR, friendly_error(err)),
                }
            });
        });
    });
    // MCP 自检: 在后台向本机端口握手, 结果回到设置页.
    let z_runtime = c_runtime.clone();
    let z_weak = z_ui.as_weak();
    z_ui.on_mcp_self_check(move || {
        let Some(z_ui) = z_weak.upgrade() else { return; };
        let Ok(z_port) = u16::try_from(z_ui.get_mcp_port()) else { return; };
        let z_gen = MCP_CHECK_GEN.fetch_add(1, Ordering::AcqRel).wrapping_add(1);
        z_ui.set_mcp_checking(true);
        let z_weak = z_weak.clone();
        z_runtime.spawn(async move {
            let (z_level, z_title, z_detail) = crate::mcp::self_check(z_port).await;
            let _ = z_weak.upgrade_in_event_loop(move |c_ui| {
                if MCP_CHECK_GEN.load(Ordering::Acquire) != z_gen {
                    return;
                }
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
                    notify(
                        &c_ui,
                        NOTE_OK,
                        if is_en() {
                            format!("{c_name} downloaded; click Apply to use")
                        } else {
                            format!("{c_name} 已下载, 点击应用即可使用")
                        },
                    );
                }
                Err(err) => {
                    set_font_state(&c_ui, &c_name, 0);
                    let l_err = friendly_error(&format!("{err:#}"));
                    notify(
                        &c_ui,
                        NOTE_ERROR,
                        if is_en() {
                            format!("Failed to download {c_name}: {l_err}")
                        } else {
                            format!("{c_name} 下载失败: {l_err}")
                        },
                    );
                }
            });
        });
    });
    let z_font = string(&z_settings, "terminal_font");
    z_ui.set_terminal_font(
        if z_font.is_empty() {
            if cfg!(target_os = "macos") { "Menlo" } else { "Consolas" }
        } else {
            z_font
        }
        .into(),
    );
    z_ui.set_term_font_size(number(&z_settings, "terminal_font_size").clamp(9.0, 28.0) as f32);
    let z_backend = c_backend.clone();
    let z_runtime = c_runtime.clone();
    let z_weak = z_ui.as_weak();
    z_ui.on_terminal_font_changed(move |c_family, c_size| {
        dispatch(&z_runtime, &z_backend, z_weak.clone(), "set_terminal_font", json!({"family":c_family.as_str(),"size":c_size}));
    });
    z_ui.set_tab_show_group(z_settings.get("tab_show_group").and_then(Value::as_bool).unwrap_or(true));
    // 已保存的目录可能被移除, 保留设置并在下载时创建.
    let z_download_dir = Some(string(&z_settings, "download_dir")).filter(|c| std::path::Path::new(c).is_absolute()).map(str::to_owned);
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
    let z_concurrency = (number(&z_settings, "transfer_concurrency") as u32).clamp(1, crate::config::MAX_TRANSFER_CONCURRENCY);
    let z_batch = (number(&z_settings, "transfer_batch") as u32).clamp(1, crate::config::MAX_TRANSFER_BATCH);
    z_ui.set_transfer_concurrency(z_concurrency as i32);
    z_ui.set_transfer_batch_limit(z_batch as i32);
    let (z_limits_tx, mut z_limits_rx) = tokio::sync::watch::channel((z_concurrency, z_batch));
    let z_weak = z_ui.as_weak();
    z_ui.on_transfer_limits_changed(move || {
        let Some(l_ui) = z_weak.upgrade() else { return; };
        let l_concurrency = (l_ui.get_transfer_concurrency().max(1) as u32).clamp(1, crate::config::MAX_TRANSFER_CONCURRENCY);
        let l_batch = (l_ui.get_transfer_batch_limit().max(1) as u32).clamp(1, crate::config::MAX_TRANSFER_BATCH);
        let _ = z_limits_tx.send((l_concurrency, l_batch));
    });
    let z_backend = c_backend.clone();
    let z_weak = z_ui.as_weak();
    // 合并快速点击或键盘输入后再顺序落盘并调整并发槽位, 避免频繁写配置文件.
    c_runtime.spawn(async move {
        while z_limits_rx.changed().await.is_ok() {
            tokio::time::sleep(Duration::from_millis(80)).await;
            let (l_concurrency, l_batch) = *z_limits_rx.borrow_and_update();
            if let Err(err) = z_backend
                .call("set_transfer_limits", json!({"concurrency":l_concurrency,"batch":l_batch}), Actor::User)
                .await
            {
                let l_err = friendly_error(&format!("{err}"));
                let l_message = if is_en() {
                    format!("Failed to save transfer settings: {l_err}")
                } else {
                    format!("传输设置保存失败: {l_err}")
                };
                let _ = z_weak.upgrade_in_event_loop(move |c_ui| notify(&c_ui, NOTE_ERROR, l_message));
            }
        }
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
                let z_err = friendly_error(&format!("{err}"));
                let z_message = if is_en() {
                    format!("Failed to save theme: {z_err}")
                } else {
                    format!("主题保存失败: {z_err}")
                };
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
    let z_snapshot = z_state.clone();
    let z_weak = z_ui.as_weak();
    let (z_backend, z_runtime) = (c_backend.clone(), c_runtime.clone());
    z_ui.on_monitor_toggled(move |c_expanded| {
        // 同步按点击顺序通知后台, 避免快速展开/收起时并发 spawn 乱序导致前后端开关反转.
        {
            let _l_guard = z_runtime.enter();
            z_backend.set_monitor_enabled(c_expanded);
        }
        let Some(z_ui) = z_weak.upgrade() else {
            return;
        };
        if !c_expanded {
            // 收起停流后立即归零折叠栏 MiniBar, 避免残留停流前的占用率.
            apply_monitor_usage(&z_ui, None);
            return;
        }
        let Ok(z_state) = z_snapshot.lock() else {
            return;
        };
        if z_state.is_null() {
            return;
        }
        let z_active = z_ui.get_active_session();
        let z_session = array(&z_state, "sessions")
            .iter()
            .find(|c| string(c, "session_id") == z_active.as_str());
        apply_mcp_status(&z_ui, &z_state["mcp_status"]);
        apply_monitor_history(&z_ui, z_session);
        apply_monitor_processes(&z_ui, z_session);
    });
    let z_answers = Arc::new(Mutex::new(Vec::<String>::new()));
    // 语言切换: 立即更新全局语言并按缓存状态重建文案, 再顺序落盘.
    let (z_lang_tx, mut z_lang_rx) = tokio::sync::watch::channel(z_lang);
    let z_weak = z_ui.as_weak();
    let z_lang_state = z_state.clone();
    let z_lang_answers = z_answers.clone();
    z_ui.on_language_changed(move |c_index| {
        let z_next = if c_index == 1 { i18n::Lang::En } else { i18n::Lang::Zh };
        i18n::set(z_next);
        if let Some(z_ui) = z_weak.upgrade() {
            let l_state = z_lang_state.lock().unwrap_or_else(PoisonError::into_inner).clone();
            refresh_language_ui(&z_ui, &l_state, &z_lang_answers);
        }
        let _ = z_lang_tx.send(z_next);
    });
    let z_backend = c_backend.clone();
    let z_weak = z_ui.as_weak();
    c_runtime.spawn(async move {
        while z_lang_rx.changed().await.is_ok() {
            let l_next = *z_lang_rx.borrow_and_update();
            if let Err(err) = z_backend
                .call("set_language", json!({"language": i18n::code(l_next)}), Actor::User)
                .await
            {
                let l_err = friendly_error(&format!("{err}"));
                let l_message = if is_en() {
                    format!("Failed to save language: {l_err}")
                } else {
                    format!("语言保存失败: {l_err}")
                };
                let _ = z_weak.upgrade_in_event_loop(move |c_ui| notify(&c_ui, NOTE_ERROR, l_message));
            }
        }
    });
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
        if !z_ui.get_remote_ready() || z_ui.get_modal_open() || z_ui.get_transfers_open() {
            return;
        }
        let Ok(z_paths) = c_data.file_paths() else {
            notify(&z_ui, NOTE_ERROR, t("无法读取拖拽文件路径", "Unable to read dropped file paths"));
            return;
        };
        let z_limit = z_ui.get_transfer_batch_limit().max(1) as usize;
        let z_paths = z_paths
            .take(z_limit + 1)
            .map(std::path::Path::to_path_buf)
            .collect::<Vec<_>>();
        let z_session = z_ui.get_active_session().to_string();
        let z_link = LISTED_LINK.with_borrow(|c| c.get(&z_session).copied());
        upload_paths(
            &z_runtime,
            &z_backend,
            z_weak.clone(),
            z_session,
            z_link,
            z_ui.get_browse_path().to_string(),
            z_paths,
            z_limit,
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
                show_notice(z_weak.clone(), NOTE_WARN, t("终端输入队列已满, 请稍后重试", "Terminal input queue full; try again").into());
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
                    notify(&z_ui, NOTE_WARN, t("终端输入队列已满, 请稍后重试", "Terminal input queue full; try again"));
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
                let l_desc = if is_en() {
                    format!("Paste {z_lines} lines into the terminal? Commands may run line by line.")
                } else {
                    format!("要粘贴的内容有 {z_lines} 行, 在 Shell 中可能逐行执行. 确定粘贴到终端?")
                };
                if confirm_dialog(
                    t("粘贴多行内容", "Paste Multiple Lines"),
                    l_desc,
                    rfd::MessageLevel::Warning,
                    t("粘贴", "Paste"),
                )
                .await
                {
                    let _ = z_weak.upgrade_in_event_loop(move |c_ui| {
                        let l_mark = (z_link(&z_paste_state, &z_session), z_tx.1.lock().unwrap().get(&z_session).copied().unwrap_or(0));
                        if c_ui.get_active_session().as_str() != z_session || l_mark != z_mark {
                            notify(&c_ui, NOTE_WARN, t("终端已切换或重新连接, 粘贴已取消", "Terminal changed or reconnected; paste cancelled"));
                            return;
                        }
                        if !enqueue_input(&z_tx, z_session, z_encode(&c_ui, z_text), z_mark.0.1) {
                            notify(&c_ui, NOTE_WARN, t("终端输入队列已满, 请稍后重试", "Terminal input queue full; try again"));
                        }
                    });
                }
            });
        }
    });
    let z_weak = z_ui.as_weak();
    let z_tx = z_input.clone();
    z_ui.on_terminal_wheel(move |c_lines, c_row, c_col, c_modifiers| {
        let Some(z_ui) = z_weak.upgrade() else { return false; };
        let z_data = terminal_wheel(c_lines, c_row, c_col, c_modifiers, z_ui.get_mouse_mode(), z_ui.get_application_cursor());
        if !enqueue_input(&z_tx, z_ui.get_active_session().to_string(), z_data, 0) {
            notify(&z_ui, NOTE_WARN, t("终端输入队列已满, 请稍后重试", "Terminal input queue full; try again"));
        }
        true
    });
    let z_weak = z_ui.as_weak();
    let z_tx = z_input.clone();
    z_ui.on_terminal_pointer(move |c_code, c_row, c_col, c_release| {
        let Some(z_ui) = z_weak.upgrade() else { return false; };
        let z_mode = z_ui.get_mouse_mode();
        if z_mode == 0 {
            return false;
        }
        let z_data = terminal_mouse(c_code, c_row, c_col, c_release, z_mode);
        enqueue_input(&z_tx, z_ui.get_active_session().to_string(), z_data, 0)
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
            notify(&z_ui, NOTE_WARN, t("终端输入队列已满, 请稍后重试", "Terminal input queue full; try again"));
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
        let Ok(z_port) = z_ui.get_edit_port().parse::<u16>() else { z_ui.set_form_error(t("请输入有效端口 (1-65535)", "Enter a valid port (1-65535)").into()); return; };
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
        let Ok(z_port) = z_ui.get_proxy_edit_port().trim().parse::<u16>() else { z_ui.set_proxy_form_error(t("请输入有效端口 (1-65535)", "Enter a valid port (1-65535)").into()); return; };
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
                // 从最近状态快照立即回填连接 ID、断开原因与监控元数据 (try_lock 避开 apply_state 重入死锁);
                // 新建标签尚未进入快照时清空旧标签的监控曲线、进程与 MiniBar, 防止残留上一服务器数据.
                if let Ok(l_state) = z_view_snapshot.try_lock()
                    && !l_state.is_null()
                {
                    if array(&l_state, "sessions").iter().any(|c| string(c, "session_id") == z_arg) {
                        apply_active_session(&z_ui, &l_state, &z_arg);
                    } else {
                        apply_monitor_usage(&z_ui, None);
                        if z_ui.get_monitor_expanded() {
                            apply_monitor_history(&z_ui, None);
                            apply_monitor_processes(&z_ui, None);
                        }
                    }
                }
                z_ui.invoke_terminal_refresh();
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
                None => notify(&z_ui, NOTE_WARN, t("该标签没有对应的连接配置", "This tab has no saved host configuration")),
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
        if c_method == "invalidate_files" || c_method == "discard_files" {
            // 会话已换成新连接: 针对旧列表打开的文件弹窗一并关闭, 不能在新连接上确认.
            if c_method == "discard_files" { close_file_dialogs(&z_ui, &z_arg); }
            z_views.borrow_mut().invalidate_files(&z_ui, &z_arg, c_method == "discard_files");
            return;
        }
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
            if !enqueue_input(&z_action_tx, z_session, z_data.into(), 0) {
                notify(&z_ui, NOTE_WARN, t("终端输入队列已满, 请稍后重试", "Terminal input queue full; try again"));
            }
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
            // 带上界面展示的指纹和提示, 后台核对后才采用, 弹窗期间连接已更换时不会误用到新连接.
            "trust_once" | "trust_permanent" | "trust_reject" => ("trust_host",json!({"session_id":z_arg,"fingerprint":z_ui.get_trust_fingerprint().as_str(),"decision":match c_method.as_str(){"trust_once"=>"once","trust_permanent"=>"permanent",_=>"reject"}})),
            "answer_auth" => {
                let l_answers = std::mem::take(&mut *z_auth_answers.lock().unwrap());
                let l_request = AUTH_REQUEST.with_borrow_mut(std::mem::take);
                z_ui.set_auth_session("".into());
                z_ui.set_prompts(ModelRc::default());
                z_ui.set_auth_generation(z_ui.get_auth_generation()+1);
                ("answer_auth",json!({"session_id":z_arg,"answers":l_answers,"auth_id":l_request}))
            }
            "run_command" => {
                if z_arg.trim().is_empty() { return; }
                ("run_command",json!({"session_id":z_session,"command":z_arg,"timeout_seconds":30,"max_output_bytes":1048576}))
            }
            "get_task" | "cancel_task" => {
                if c_method == "get_task" { z_ui.set_selected_task(z_arg.clone().into()); }
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
                if l_selected.is_empty() { notify(&z_ui, NOTE_WARN, t("请选择要删除的文件", "Select files to delete")); return; }
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
                let l_suffix = t("-副本", "-copy");
                let Some(l_name) = (1..1000).map(|i| format!("{l_stem}{l_suffix}{i}{l_ext}")).find(|c| !l_names.contains(c)) else { return; };
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
                    let l_text = if l_users > 0 {
                        if is_en() {
                            format!("{l_users} host(s) use this proxy and will switch to Direct.")
                        } else {
                            format!("有 {l_users} 台服务器在使用这个代理, 删除后它们改为直连.")
                        }
                    } else {
                        t("删除这个代理?", "Delete this proxy?").to_owned()
                    };
                    if confirm_dialog(
                        t("删除代理", "Delete Proxy"),
                        l_text,
                        rfd::MessageLevel::Warning,
                        t("删除", "Delete"),
                    )
                    .await
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
        static DIALOG_OPEN: AtomicBool = AtomicBool::new(false);
        struct DialogGuard(&'static AtomicBool);
        impl Drop for DialogGuard {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }
        let Some(z_ui) = z_weak.upgrade() else { return; };
        let z_kind = c_kind.to_string();
        if matches!(z_kind.as_str(), "upload" | "upload_folder" | "download" | "download_to") && !z_ui.get_remote_ready() { return; }
        let z_download_dir = z_ui.get_download_dir().to_string();
        // 需要弹出系统文件框的操作做并发防重, 避免无宿主非模态对话框在 Windows/macOS 上叠加打开.
        let z_needs_os_dialog = !(z_kind == "download" && !z_download_dir.is_empty());
        let z_dialog_guard = if z_needs_os_dialog {
            if DIALOG_OPEN.swap(true, Ordering::AcqRel) {
                return;
            }
            Some(DialogGuard(&DIALOG_OPEN))
        } else {
            None
        };
        let z_connection = z_ui.get_selected_connection().to_string();
        let z_group = z_ui.get_selected_connection_group().to_string();
        let z_session = z_ui.get_active_session().to_string();
        // 下载针对列表中选中的文件: 带上列表所属连接的代次, 期间标签已重连时后台拒绝.
        let z_listed_link = LISTED_LINK.with_borrow(|c| c.get(&z_session).copied());
        let z_selected_files = z_ui.get_files().iter().filter(|c| c.selected).collect::<Vec<_>>();
        let z_upload_target = z_ui.get_upload_target().to_string();
        let z_batch_limit = z_ui.get_transfer_batch_limit().max(1) as usize;
        let z_weak = z_weak.clone();
        let z_backend = z_backend.clone();
        z_runtime.spawn(async move {
            let mut z_dialog_guard = z_dialog_guard;
            // 用户选文件时后台提前建立传输连接, 选完即可开始.
            if matches!(z_kind.as_str(), "upload" | "upload_folder" | "download" | "download_to") {
                let _ = z_backend.call("warm_transfer", json!({"session_id":z_session}), Actor::User).await;
            }
            let z_dialog = rfd::AsyncFileDialog::new().set_title(match z_kind.as_str() {
                "upload" => t("选择要上传的文件", "Select Files to Upload"),
                "upload_folder" => t("选择要上传的文件夹", "Select Folders to Upload"),
                "download" | "download_to" => t("选择下载保存位置", "Select Download Location"),
                "download_dir" => t("选择默认下载目录", "Select Default Download Folder"),
                "key" => t("选择私钥文件", "Select Private Key"),
                "export" => t("导出所选连接", "Export Selected Host"),
                "export_all" => t("选择全部连接的导出目录", "Select Export Folder for All Hosts"),
                "export_group" => t("选择分组的导出目录", "Select Export Folder for Group"),
                "import_folder" | "import_folder_all" => t("选择连接配置目录", "Select Host Config Folder"),
                "import_commands" => t("选择要导入的命令文件", "Select Commands File to Import"),
                "export_commands" => t("导出全部命令", "Export All Commands"),
                _ => t("选择连接配置文件", "Select Host Config File"),
            });
            if matches!(z_kind.as_str(), "upload" | "upload_folder") {
                // 系统对话框不能同时选文件和文件夹 (Windows), 分成两个入口.
                let z_paths = if z_kind == "upload" { z_dialog.pick_files().await } else { z_dialog.pick_folders().await };
                drop(z_dialog_guard.take());
                if let Some(z_paths) = z_paths {
                    upload_paths(&tokio::runtime::Handle::current(), &z_backend, z_weak, z_session, z_listed_link, z_upload_target, z_paths.into_iter().take(z_batch_limit + 1).map(|c| c.path().to_path_buf()).collect(), z_batch_limit);
                }
                return;
            }
            if matches!(z_kind.as_str(), "download" | "download_to") {
                if z_selected_files.is_empty() || z_selected_files.len() > z_batch_limit {
                    show_notice(
                        z_weak,
                        NOTE_WARN,
                        if is_en() {
                            format!("Select 1 to {z_batch_limit} items to download")
                        } else {
                            format!("一次请选择 1～{z_batch_limit} 项下载")
                        },
                    );
                    return;
                }
                if z_selected_files.iter().any(|c| c.name.is_empty() || c.name == "." || c.name == ".." || c.name.contains('/') || c.name.chars().any(char::is_control)) {
                    show_notice(z_weak, NOTE_WARN, t("下载项的名称不能包含路径或控制字符", "Item name cannot contain path separators or control characters").into());
                    return;
                }
                // "下载" 使用默认目录, "下载到…" 由系统对话框选择位置.
                let z_default = (z_kind == "download" && !z_download_dir.is_empty())
                    .then(|| std::path::PathBuf::from(&z_download_dir));
                // 直接存入下载目录且达到 3 项时先确认目录 (绑定本次快照路径, 避免弹窗期间修改默认目录造成错位).
                if z_default.is_some() && z_selected_files.len() >= 3 && !confirm_download(z_weak.clone(), z_selected_files.len(), z_download_dir.clone()).await { return; }
                // 文件夹、多选和默认目录都存到目录下, 只有单个文件用另存为.
                let z_multiple = z_default.is_some() || z_selected_files.len() > 1 || z_selected_files[0].directory;
                let z_destination = match z_default {
                    Some(l_dir) => {
                        if let Err(err) = tokio::fs::create_dir_all(&l_dir).await {
                            show_notice(
                                z_weak,
                                NOTE_ERROR,
                                if is_en() {
                                    format!("Failed to create download folder: {} ({err})", l_dir.display())
                                } else {
                                    format!("创建下载目录失败: {} ({err})", l_dir.display())
                                },
                            );
                            return;
                        }
                        l_dir
                    }
                    None => {
                        let l_picked = if z_multiple {
                            z_dialog.set_title(t("选择下载保存目录", "Select Download Folder")).pick_folder().await
                        } else {
                            z_dialog.set_file_name(local_file_name(&z_selected_files[0].name)).save_file().await
                        };
                        drop(z_dialog_guard.take());
                        let Some(l_picked) = l_picked else { return; };
                        l_picked.path().to_path_buf()
                    }
                };
                // 单个文件的保存对话框已由系统确认替换; 存到目录时同名冲突统一询问一次, 文件夹覆盖即合并到已有文件夹.
                let mut z_overwrite = vec![!z_multiple; z_selected_files.len()];
                let mut z_skip = vec![false; z_selected_files.len()];
                let z_dirs = z_selected_files.iter().map(|c| c.directory).collect::<Vec<_>>();
                let mut z_targets = z_selected_files
                    .iter()
                    .map(|c| if z_multiple { z_destination.as_path().join(local_file_name(&c.name)) } else { z_destination.as_path().to_path_buf() })
                    .collect::<Vec<_>>();
                if z_multiple {
                    // 本地同名检查与去重移到阻塞线程, 使用 symlink_metadata 识别悬空符号链接.
                    let l_dest = z_destination.clone();
                    let l_dirs = z_dirs.clone();
                    let l_prep = tokio::task::spawn_blocking(move || {
                        let l_exists = |c: &std::path::Path| std::fs::symlink_metadata(c).is_ok();
                        let l_key = |c: &std::path::Path| local_name_key(&c.to_string_lossy());
                        let mut l_seen = std::collections::HashSet::new();
                        for (l_target, &l_is_dir) in z_targets.iter_mut().zip(&l_dirs) {
                            if !l_seen.insert(l_key(l_target)) {
                                let l_name = l_target.file_name().map(|c| c.to_string_lossy().into_owned()).unwrap_or_default();
                                *l_target = l_dest.join(unique_name(&l_name, l_is_dir, |c| l_seen.contains(&l_key(&l_dest.join(c))) || l_exists(&l_dest.join(c))));
                                l_seen.insert(l_key(l_target));
                            }
                        }
                        let l_existing = z_targets.iter().map(|c| l_exists(c)).collect::<Vec<_>>();
                        let l_conflicts = z_targets.iter().zip(&l_existing).filter(|(_, c)| **c).filter_map(|(c, _)| c.file_name()).map(|c| c.to_string_lossy().into_owned()).collect::<Vec<_>>();
                        (z_targets, l_seen, l_existing, l_conflicts)
                    })
                    .await;
                    let Ok((l_targets, l_seen, l_existing, l_conflicts)) = l_prep else {
                        show_notice(z_weak, NOTE_ERROR, t("检查本地下载目录异常", "Failed to inspect local download folder").into());
                        return;
                    };
                    z_targets = l_targets;
                    if !l_conflicts.is_empty() {
                        let l_ctx = if is_en() {
                            z_destination.display().to_string()
                        } else {
                            format!("下载到 {}", z_destination.display())
                        };
                        let Some(l_choice) = ask_conflict(z_weak.clone(), l_ctx, &l_conflicts).await else { return; };
                        match l_choice {
                            ConflictChoice::Overwrite => z_overwrite = l_existing,
                            ConflictChoice::Skip => z_skip = l_existing,
                            ConflictChoice::KeepBoth => {
                                let l_dest = z_destination.clone();
                                let l_dirs = z_dirs.clone();
                                let Ok(l_targets) = tokio::task::spawn_blocking(move || {
                                    let l_exists = |c: &std::path::Path| std::fs::symlink_metadata(c).is_ok();
                                    let l_key = |c: &std::path::Path| local_name_key(&c.to_string_lossy());
                                    let mut l_seen = l_seen;
                                    for (l_target, &l_is_dir) in z_targets.iter_mut().zip(&l_dirs) {
                                        if l_exists(l_target) {
                                            let l_name = l_target.file_name().map(|c| c.to_string_lossy().into_owned()).unwrap_or_default();
                                            *l_target = l_dest.join(unique_name(&l_name, l_is_dir, |c| l_exists(&l_dest.join(c)) || l_seen.contains(&l_key(&l_dest.join(c)))));
                                            l_seen.insert(l_key(l_target));
                                        }
                                    }
                                    z_targets
                                }).await else {
                                    show_notice(z_weak, NOTE_ERROR, t("计算下载文件名异常", "Failed to compute download filename").into());
                                    return;
                                };
                                z_targets = l_targets;
                            }
                        }
                    }
                }
                let mut z_count = 0;
                let mut z_failed = 0;
                let mut z_first_err: Option<anyhow::Error> = None;
                let z_skipped = z_skip.iter().filter(|c| **c).count();
                for (i, (z_file, z_local)) in z_selected_files.iter().zip(z_targets).enumerate() {
                    if z_skip[i] { continue; }
                    let z_result = z_backend.call(if z_file.directory { "download_directory" } else { "download_file" },json!({"session_id":z_session,"_link":z_listed_link,"local_path":z_local,"remote_path":z_file.path.to_string(),"overwrite":z_overwrite[i],"size_hint":z_file.size_bytes.parse::<u64>().ok()}),Actor::User).await;
                    if let Err(err) = z_result {
                        if is_fatal_batch_error(&err) {
                            show_notice(z_weak, NOTE_ERROR, batch_error(false, z_count, &err));
                            return;
                        }
                        z_failed += 1;
                        z_first_err.get_or_insert(err);
                        continue;
                    }
                    z_count += 1;
                }
                finish_transfer_batch(z_weak, false, z_count, z_failed, z_skipped, z_first_err);
                return;
            }
            let z_path = match z_kind.as_str() {
                "export" => z_dialog.set_file_name(format!("{z_connection}_connect_config.json")).add_filter(t("连接配置", "Host Config"), &["json"]).save_file().await,
                "export_commands" => z_dialog.set_file_name("hipshell_commands.json").add_filter(t("命令文件", "Commands"), &["json"]).save_file().await,
                "import_commands" => z_dialog.add_filter(t("命令文件", "Commands"), &["json"]).pick_file().await,
                "export_all" | "export_group" | "import_folder" | "import_folder_all" | "download_dir" => z_dialog.pick_folder().await,
                _ => z_dialog.pick_file().await,
            };
            drop(z_dialog_guard.take());
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
    // 尺寸同步失败 (如重连期间会话短暂不存在) 时置位, 下次刷新清掉已记录的尺寸重新同步, 避免界面与终端尺寸长期不一致.
    let z_resize_failed = Arc::new(AtomicBool::new(false));
    // 输出唤醒遇到进行中的刷新时记下, 完成后立即补一次, 不退回定时器周期.
    let z_rerun = Arc::new(AtomicBool::new(false));
    let z_wake_backend = c_backend.clone();
    let z_wake_runtime = c_runtime.clone();
    let z_weak = z_ui.as_weak();
    // 定时器负责状态轮询、尺寸和滚动同步; 终端新输出另由后台通知立即唤醒.
    // 状态轮询 (会话、任务、监控) 每 500 毫秒一次, 有自己的忙标志: 要读配置并持全局锁, 偶尔较慢时不挡住终端画面刷新.
    let z_state_busy = Arc::new(AtomicBool::new(false));
    let z_tick = Rc::new(move |c_wake: bool| {
        let Some(z_ui) = z_weak.upgrade() else {
            return;
        };
        if z_poll_clock.get().elapsed() >= Duration::from_millis(500) && !z_state_busy.swap(true, Ordering::AcqRel) {
            z_poll_clock.set(Instant::now());
            let z_generation = z_view_generation.load(Ordering::Relaxed);
            let z_selected_task = z_ui.get_selected_task().to_string();
            let (z_backend, z_weak, z_state_busy, z_state, z_answers, z_refresh_runtime, z_view_generation) =
                (c_backend.clone(), z_weak.clone(), z_state_busy.clone(), z_state.clone(), z_answers.clone(), c_runtime.clone(), z_view_generation.clone());
            c_runtime.spawn(async move {
                // 后台调用放进子任务: 万一 panic, 这里仍能释放忙标志.
                let z_calls = {
                    let (z_backend, z_selected_task) = (z_backend.clone(), z_selected_task.clone());
                    tokio::spawn(async move {
                        let z_new_state = z_backend.call("state", json!({}), Actor::User).await;
                        let z_task = if z_selected_task.is_empty() {
                            None
                        } else {
                            Some(z_backend.call("get_task", json!({"task_id":z_selected_task}), Actor::User).await)
                        };
                        (z_new_state, z_task)
                    })
                };
                let Ok((z_new_state, z_task)) = z_calls.await else {
                    z_state_busy.store(false, Ordering::Release);
                    return;
                };
                let z_busy_fallback = z_state_busy.clone();
                if slint::invoke_from_event_loop(move || {
                    // A→B→A 时会话ID仍相同, 也不能应用切换前取到的旧状态; 无论是否应用都要释放忙标志.
                    if let Some(z_ui) = z_weak.upgrade().filter(|_| z_view_generation.load(Ordering::Relaxed) == z_generation) {
                        apply_polled_state(&z_ui, z_new_state, &z_state, &z_answers, &z_refresh_runtime, &z_backend);
                        if let Some(Ok(z_value)) = z_task.filter(|_| z_ui.get_selected_task().as_str() == z_selected_task) {
                            let z_output = task_output(&z_ui, &z_value);
                            if z_ui.get_task_output().as_str() != z_output {
                                z_ui.set_task_output(z_output.into());
                            }
                        }
                    }
                    z_state_busy.store(false, Ordering::Release);
                })
                .is_err()
                {
                    z_busy_fallback.store(false, Ordering::Release);
                }
            });
        }
        if z_busy.swap(true, Ordering::AcqRel) {
            if c_wake {
                z_rerun.store(true, Ordering::Release);
            }
            return;
        }
        let z_session = z_ui.get_active_session().to_string();
        let z_generation = z_view_generation.load(Ordering::Relaxed);
        let z_empty = z_ui.get_active_session_empty();
        // 已断开或失败的离线标签不再每 25ms 轮询/同步终端尺寸, 重连切回在线后再重新同步.
        let z_offline = matches!(z_ui.get_active_state(), 0 | 4);
        if z_session.is_empty() || z_empty || z_offline {
            if z_offline {
                z_dimensions.borrow_mut().0.clear();
            }
            z_busy.store(false, Ordering::Release);
            return;
        }
        let z_scroll = z_ui.get_terminal_scroll_offset();
        // 只在拖选过程中暂停读取画面; 已有选区时照常刷新, 由 apply_polled_terminal 决定是否保留选区.
        let z_selection = z_ui.get_selection_dragging();
        let z_size = (
            z_session.clone(),
            z_ui.get_terminal_rows(),
            z_ui.get_terminal_cols(),
        );
        if z_resize_failed.swap(false, Ordering::AcqRel) {
            z_dimensions.borrow_mut().0.clear();
        }
        let z_resize = *z_dimensions.borrow() != z_size;
        if z_resize {
            // 终端可显示的列数与行数, 随窗口变化并同步给服务器.
            z_ui.set_terminal_size_text(format_terminal_size(z_size.2, z_size.1).into());
            *z_dimensions.borrow_mut() = z_size.clone();
            // 无论是否在翻看历史, 尺寸变化后都需要刷新一次视图与展示尺寸.
            z_sequence.lock().unwrap().0.clear();
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
        let mut z_args = json!({"session_id":z_session,"max_lines":200,"scroll_offset":z_scroll});
        if z_selection || !z_read_view { z_args["events_only"] = json!(true); }
        if let Some(z_seq) = z_after_seq {
            z_args["after_seq"] = json!(z_seq);
        }
        if z_resize
            && let Some(l_terminal) = c_backend.try_resize_and_read_terminal(
                &z_session,
                Some((z_size.1.clamp(1, 200) as u16, z_size.2.clamp(1, 400) as u16)),
                &z_args,
            )
        {
            match l_terminal {
                Ok(l_value) => {
                    if let Some(l_text) = l_value.get("clipboard").and_then(Value::as_str) {
                        z_ui.invoke_copy_text(l_text.into());
                    }
                    if let (Some(l_rows), Some(l_cols)) = (l_value.get("rows").and_then(Value::as_i64), l_value.get("cols").and_then(Value::as_i64))
                        && (l_rows, l_cols) != (i64::from(z_size.1.clamp(1, 200)), i64::from(z_size.2.clamp(1, 400)))
                    {
                        z_resize_failed.store(true, Ordering::Release);
                    }
                    apply_polled_terminal(&z_ui, &l_value, &z_session, z_scroll, &z_sequence);
                }
                Err(_) => {
                    z_resize_failed.store(true, Ordering::Release);
                }
            }
            z_busy.store(false, Ordering::Release);
            if z_rerun.swap(false, Ordering::AcqRel) {
                let l_weak = z_weak.clone();
                let _ = slint::invoke_from_event_loop(move || {
                    if let Some(l_ui) = l_weak.upgrade() { l_ui.invoke_terminal_refresh(); }
                });
            }
            return;
        }
        let z_backend = c_backend.clone();
        let z_weak = z_weak.clone();
        let z_busy = z_busy.clone();
        let z_sequence = z_sequence.clone();
        let z_view_generation = z_view_generation.clone();
        let z_rerun = z_rerun.clone();
        let z_resize_failed = z_resize_failed.clone();
        c_runtime.spawn(async move {
            // 后台调用放进子任务: 万一 panic, 这里仍能释放忙标志, 画面刷新不会就此停住.
            let z_calls = tokio::spawn(async move {
                if z_resize
                    && z_backend
                        .call("resize_terminal", json!({"session_id":z_size.0,"rows":z_size.1,"cols":z_size.2}), Actor::User)
                        .await
                        .is_err()
                {
                    z_resize_failed.store(true, Ordering::Release);
                }
                let z_terminal = z_backend.call("read_terminal", z_args, Actor::User).await;
                // 后台终端尺寸与界面不符 (同标签重连后新会话沿用了旧尺寸、调整落到了旧会话等) 时重新同步; 比较前按后台上限截取.
                // 轻量结果 (拖选、翻看历史) 也带尺寸, 同样能发现.
                if let Ok(l_value) = &z_terminal
                    && let (Some(l_rows), Some(l_cols)) = (l_value.get("rows").and_then(Value::as_i64), l_value.get("cols").and_then(Value::as_i64))
                    && (l_rows, l_cols) != (i64::from(z_size.1.clamp(1, 200)), i64::from(z_size.2.clamp(1, 400)))
                {
                    z_resize_failed.store(true, Ordering::Release);
                }
                z_terminal
            });
            let Ok(z_terminal) = z_calls.await else {
                z_busy.store(false, Ordering::Release);
                return;
            };
            let z_busy_fallback = z_busy.clone();
            if slint::invoke_from_event_loop(move || {
                // 远端程序 (vim、tmux) 的复制请求已从后台取走, 同一标签内画面因代次变化被丢弃时也要写入剪贴板, 否则就此丢失;
                // 已切到别的标签时不写入, 免得后台标签迟到的复制覆盖用户刚复制的内容.
                if let (Some(z_ui), Ok(z_value)) = (z_weak.upgrade(), &z_terminal)
                    && z_ui.get_active_session().as_str() == z_session
                    && let Some(z_text) = z_value.get("clipboard").and_then(Value::as_str)
                {
                    z_ui.invoke_copy_text(z_text.into());
                }
                // A→B→A 时会话ID仍相同, 也不能应用切换前取到的旧画面; 无论是否应用都要释放忙标志.
                if let Some(z_ui) = z_weak.upgrade().filter(|_| z_view_generation.load(Ordering::Relaxed) == z_generation)
                    && let Ok(z_value) = z_terminal
                {
                    apply_polled_terminal(&z_ui, &z_value, &z_session, z_scroll, &z_sequence);
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
            let l_text = if is_en() {
                format!("Failed to read state: {err}")
            } else {
                format!("状态读取失败: {err}")
            };
            if c_ui.get_status().as_str() != l_text {
                notify(c_ui, NOTE_ERROR, l_text);
            }
            return;
        }
    };
    if c_ui.get_monitor_expanded() {
        apply_mcp_status(c_ui, &z_value["mcp_status"]);
    } else {
        c_ui.set_mcp_running(boolean(&z_value["mcp_status"], "running"));
    }
    let z_prev = {
        let mut l_old = c_state.lock().unwrap_or_else(PoisonError::into_inner);
        let l_active_connected = array(&z_value, "sessions").iter().any(|c| string(c, "session_id") == c_ui.get_active_session().as_str() && string(c, "status") == "connected");
        let l_needs_files = if l_active_connected {
            c_ui.get_remote_listed_session() != c_ui.get_active_session()
        } else {
            c_ui.get_remote_ready() || c_ui.get_files().row_count() > 0 || c_ui.get_directory_tree().row_count() > 0
                || !c_ui.get_file_loading_path().is_empty() || !c_ui.get_tree_loading().is_empty()
                || !c_ui.get_remote_listed_session().is_empty()
        };
        let l_day_changed = !array(&z_value, "tasks").is_empty() && LAST_LOCAL_DAY.get() != local_today();
        if *l_old == z_value && !l_needs_files && !l_day_changed && PENDING_UPLOAD_REFRESH.with_borrow(|c| c.is_empty()) {
            return;
        }
        // 在调用任何界面回调前更新快照并释放锁, 避免同步重入 (如 show_session) 时锁冲突.
        std::mem::replace(&mut *l_old, z_value.clone())
    };
    let z_connected_ids = array(&z_value, "sessions").iter()
        .filter(|c| string(c, "status") == "connected")
        .map(|c| string(c, "session_id")).collect::<Vec<_>>();
    FILE_CACHE.with_borrow_mut(|c| c.retain(|c| z_connected_ids.contains(&c.0.0.as_str())));
    TREE_CACHE.with_borrow_mut(|c| c.retain(|c, _| z_connected_ids.contains(&c.0.as_str()) && c.0 == c_ui.get_active_session().as_str()));
    // 同一标签重连沿用会话 ID, 两次轮询之间完成的重连看不到断开; 会话代次变了就作废旧列表, 下面按原目录重新读取.
    for l_session in array(&z_value, "sessions") {
        let l_id = string(l_session, "session_id");
        if let Some(l_terminal) = l_session.get("terminal").and_then(Value::as_u64) {
            TERMINAL_GEN.lock().unwrap_or_else(PoisonError::into_inner).insert(l_id.to_owned(), l_terminal);
        }
        // 新连接的列表已先到达时已在那里作废并重建 (见 list_files 回包), 不再重复清空.
        if array(&z_prev, "sessions").iter().any(|c| string(c, "session_id") == l_id && number(c, "link") != number(l_session, "link"))
            && LISTED_LINK.with_borrow(|c| c.get(l_id).copied()) != l_session.get("link").and_then(Value::as_u64)
        {
            // 后台标签保存的视图快照 (列表、已加载标记、选择) 一并作废, 切回时重新读取.
            c_ui.invoke_action("discard_files".into(), l_id.into());
        }
    }
    let mut z_failed = Vec::new();
    let z_active_now = c_ui.get_active_session().to_string();
    let z_browse_now = c_ui.get_browse_path().to_string();
    let z_upload_running = |c_status: &str| matches!(c_status, "queued" | "preparing" | "running" | "retrying");
    // 仅统计当前目录的同目录在途上传 (不含 cancelling 和无关 MCP 相对路径任务); 节流时登记待补刷标记, 避免后续失败/取消导致漏刷.
    let z_more_uploads_here = array(&z_value, "tasks").iter().any(|c_task| {
        string(c_task, "kind") == "upload"
            && z_upload_running(string(c_task, "status"))
            && string(c_task, "session_id") == z_active_now
            && parent_dir(string(c_task, "label")) == z_browse_now
    });
    let z_more_rel_uploads = array(&z_value, "tasks").iter().any(|c_task| {
        string(c_task, "kind") == "upload"
            && z_upload_running(string(c_task, "status"))
            && string(c_task, "session_id") == z_active_now
            && parent_dir(string(c_task, "label")).is_empty()
    });
    for l_task in array(&z_value, "tasks") {
        let l_kind = string(l_task, "kind");
        let l_status = string(l_task, "status");
        let l_was = |c_status: &str| array(&z_prev, "tasks").iter().any(|c_old| string(c_old, "task_id") == string(l_task, "task_id") && string(c_old, "status") == c_status);
        if l_kind == "upload" && l_status == "completed" && !l_was("completed") {
            let l_sid = string(l_task, "session_id");
            let l_parent = parent_dir(string(l_task, "label"));
            if l_parent.is_empty() {
                // MCP 相对路径上传 (如 "a.txt") 落在远端主目录, 按会话失效缓存以触发刷新.
                if l_sid == z_active_now && z_more_rel_uploads && !c_ui.get_file_loading_path().is_empty() {
                    PENDING_UPLOAD_REFRESH.with_borrow_mut(|c| { c.insert((l_sid.to_owned(), String::new())); });
                } else {
                    c_ui.invoke_action("invalidate_files".into(), l_sid.into());
                }
            } else if l_sid == z_active_now && l_parent == z_browse_now && z_more_uploads_here && !c_ui.get_file_loading_path().is_empty() {
                FILE_CACHE.with_borrow_mut(|c| c.retain(|c| c.0.0 != l_sid || c.0.1 != l_parent));
                PENDING_UPLOAD_REFRESH.with_borrow_mut(|c| { c.insert((l_sid.to_owned(), l_parent.clone())); });
            } else {
                invalidate_dirs(c_ui, l_sid, &[l_parent.clone()]);
            }
            // AI 的上传不抢用户的选择; 必须同时核对会话与目录, 防止后台标签覆盖当前标签待定位文件.
            if string(l_task, "source") != "mcp" && l_sid == z_active_now && l_parent == z_browse_now {
                FILE_FOCUS.set(Some((l_sid.to_owned(), string(l_task, "label").to_owned())));
            }
        }
        // 文件夹上传涉及的子目录很多, 失败或取消时也可能已写入部分文件, 结束后让整个会话的目录缓存失效.
        if l_kind == "upload" && boolean(l_task, "directory") && matches!(l_status, "completed" | "failed" | "cancelled") && !l_was(l_status) {
            c_ui.invoke_action("invalidate_files".into(), string(l_task, "session_id").into());
        }
        // 传输失败后进度直接从状态栏消失, 不提示容易被当成已完成 (以为下载好了而删掉服务器上的文件), 必须弹窗告知.
        if matches!(l_kind, "upload" | "download") && l_status == "failed" && !l_was("failed") {
            let l_ai = if string(l_task, "source") == "mcp" { "[AI] " } else { "" };
            let (l_from, l_to) = if l_kind == "download" { ("label", "local") } else { ("local", "label") };
            let l_err = friendly_error(string(l_task, "error"));
            z_failed.push(if is_en() {
                format!(
                    "{l_ai}{} failed: {}\nDestination: {}\nReason: {l_err}",
                    if l_kind == "download" { "Download" } else { "Upload" },
                    string(l_task, l_from),
                    string(l_task, l_to),
                )
            } else {
                format!(
                    "{l_ai}{}失败: {}\n目标位置: {}\n原因: {l_err}",
                    if l_kind == "download" { "下载" } else { "上传" },
                    string(l_task, l_from),
                    string(l_task, l_to),
                )
            });
        }
    }
    // 冲刷被节流的待补刷目录: 在途列表已返回、或该目录已无在途上传、或已切走标签时立即补刷, 防止漏刷.
    let z_loading_now = !c_ui.get_file_loading_path().is_empty();
    let mut z_flush_refresh = Vec::new();
    PENDING_UPLOAD_REFRESH.with_borrow_mut(|c_pending| {
        c_pending.retain(|(l_sid, l_dir)| {
            let l_still_busy = l_sid == &z_active_now
                && z_loading_now
                && if l_dir.is_empty() { z_more_rel_uploads } else { l_dir == &z_browse_now && z_more_uploads_here };
            if l_still_busy {
                true
            } else {
                z_flush_refresh.push((l_sid.clone(), l_dir.clone()));
                false
            }
        });
    });
    for (l_sid, l_dir) in z_flush_refresh {
        if l_dir.is_empty() {
            c_ui.invoke_action("invalidate_files".into(), l_sid.into());
        } else {
            invalidate_dirs(c_ui, &l_sid, &[l_dir]);
        }
    }
    if !z_failed.is_empty() {
        notify(
            c_ui,
            NOTE_ERROR,
            if is_en() {
                format!("{} transfer(s) failed; see Tasks", z_failed.len())
            } else {
                format!("{} 个传输任务失败, 详见任务列表", z_failed.len())
            },
        );
        alert_transfer_failures(c_runtime, z_failed);
    }
    // 状态栏分工: 左侧为事件消息 (含连接失败原因与重试), 中间只显示当前会话的简短状态, 右侧为传输.
    // 会话刚变为失败时在左侧提示一次.
    for l_session in array(&z_value, "sessions") {
        let l_status = string(l_session, "status");
        let l_reason = session_error(l_status);
        let l_id = string(l_session, "session_id");
        let l_was = array(&z_prev, "sessions").iter().find(|c| string(c, "session_id") == l_id).map(|c| string(c, "status"));
        if !l_reason.is_empty() && l_was != Some(l_status) {
            let l_label = if l_status.starts_with("error: 连接已断开:") {
                t("连接已断开", "Disconnected")
            } else {
                t("连接失败", "Connection failed")
            };
            notify_retry(c_ui, format!("{} {l_label}: {l_reason}", server_label(l_session)), "reconnect", l_id.into());
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
    // 不刷新画面时 (翻看历史、拖选) 输入模式仍随远端更新, 按键和粘贴按最新模式编码.
    if boolean(c_value, "unchanged") && c_value.get("modified_keys").is_some() {
        c_ui.set_bracketed_paste(boolean(c_value, "bracketed_paste"));
        c_ui.set_application_cursor(boolean(c_value, "application_cursor"));
        c_ui.set_modified_keys(number(c_value, "modified_keys") as i32);
        c_ui.set_mouse_mode(number(c_value, "mouse") as i32);
        c_ui.set_mouse_tracking(number(c_value, "mouse_tracking") as i32);
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
    if matches!(c_method, "set_permissions" | "rename_file" | "delete_files" | "delete_files_fast" | "create_directory" | "create_file" | "copy_file" | "check_remote_names" | "file_details") {
        attach_listed_link(&mut c_args);
    }
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
        notify(c_ui, NOTE_WARN, t("所选文本最多复制 1 MB, 请分段复制", "Selection copy limit is 1 MB; copy in smaller chunks"));
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
        notify(
            c_ui,
            NOTE_INFO,
            if is_en() {
                format!("{} items selected", z_paths.len())
            } else {
                format!("已选择 {} 项", z_paths.len())
            },
        );
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

// c_link: 发起上传 (打开选择框或拖入) 时所展示列表的连接代次; 重名检查、覆盖确认后的提交都带上它,
// 期间标签已重连时后台拒绝, 覆盖许可不会用到新连接上.
fn upload_paths(
    c_runtime: &tokio::runtime::Handle,
    c_backend: &Arc<Backend>,
    c_weak: slint::Weak<AppWindow>,
    c_session: String,
    c_link: Option<u64>,
    c_directory: String,
    c_paths: Vec<std::path::PathBuf>,
    c_limit: usize,
) {
    if c_paths.is_empty() || c_paths.len() > c_limit {
        show_notice(
            c_weak,
            NOTE_WARN,
            if is_en() {
                format!("Select or drop 1 to {c_limit} items")
            } else {
                format!("一次请选择或拖入 1～{c_limit} 项")
            },
        );
        return;
    }
    let z_backend = c_backend.clone();
    c_runtime.spawn(async move {
        // 拖入上传不经过选择框, 在这里提前建立传输连接; 已建好时立即返回.
        let _ = z_backend.call("warm_transfer", json!({"session_id":c_session,"_link":c_link}), Actor::User).await;
        // 校验本地文件名与类型放到阻塞线程并缓存 is_dir, 避免慢速磁盘阻塞 Tokio 工作线程及重复 stat.
        let z_checked = tokio::task::spawn_blocking(move || -> std::result::Result<Vec<(std::path::PathBuf, String, bool)>, &'static str> {
            let mut l_items = Vec::with_capacity(c_paths.len());
            for l_path in c_paths {
                let Some(l_name) = l_path
                    .file_name()
                    .and_then(|c| c.to_str())
                    .filter(|c| !c.is_empty() && !c.chars().any(char::is_control))
                    .map(str::to_owned)
                else {
                    return Err(t("文件名无效", "Invalid filename"));
                };
                let Ok(l_meta) = std::fs::metadata(&l_path) else {
                    return Err(t("只能上传普通文件或文件夹", "Only regular files and folders can be uploaded"));
                };
                if !l_meta.is_file() && !l_meta.is_dir() {
                    return Err(t("只能上传普通文件或文件夹", "Only regular files and folders can be uploaded"));
                }
                l_items.push((l_path, l_name, l_meta.is_dir()));
            }
            Ok(l_items)
        })
        .await;
        let z_items = match z_checked {
            Ok(Ok(l_items)) => l_items,
            Ok(Err(l_msg)) => { show_notice(c_weak, NOTE_WARN, l_msg.into()); return; }
            Err(_) => { show_notice(c_weak, NOTE_ERROR, t("检查上传源文件异常", "Failed to inspect source files").into()); return; }
        };
        let z_paths = z_items.iter().map(|c| c.0.clone()).collect::<Vec<_>>();
        let z_is_dirs = z_items.iter().map(|c| c.2).collect::<Vec<_>>();
        let mut z_names = z_items.into_iter().map(|c| c.1).collect::<Vec<_>>();
        // 来自不同目录的同名文件在批次内也会互相冲突, 后者先改名; 之后对最终名称统一检查.
        let mut z_seen = std::collections::HashSet::new();
        for (z_name, &z_is_dir) in z_names.iter_mut().zip(&z_is_dirs) {
            if !z_seen.insert(z_name.clone()) {
                *z_name = unique_name(z_name, z_is_dir, |c| z_seen.contains(c));
                z_seen.insert(z_name.clone());
            }
        }
        // 只查最终名称是否已存在 (并发查询, 最多等待 6 秒), 不读整个目录; 仅超时时回退缓存, 明确断线/重连错误直接提示.
        // 任务仍不带覆盖许可, 由后台再次严格检查防误覆盖.
        show_notice(c_weak.clone(), NOTE_INFO, t("正在检查目标目录中的同名文件...", "Checking existing files...").into());
        let z_check = tokio::time::timeout(
            Duration::from_secs(6),
            z_backend.call("check_remote_names", json!({"session_id":c_session,"_link":c_link,"directory":c_directory,"names":z_names}), Actor::User),
        )
        .await;
        let mut z_taken = match z_check {
            Ok(Ok(l_value)) => array(&l_value, "existing").iter().filter_map(Value::as_str).map(str::to_owned).collect::<std::collections::HashSet<_>>(),
            Ok(Err(err)) => {
                let l_err = friendly_error(&format!("{err:#}"));
                show_notice(
                    c_weak,
                    NOTE_ERROR,
                    if is_en() {
                        format!("Failed to check remote folder: {l_err}")
                    } else {
                        format!("检查远端目录失败: {l_err}")
                    },
                );
                return;
            }
            Err(_) => {
                let (l_tx, l_rx) = tokio::sync::oneshot::channel();
                let (l_sess, l_dir) = (c_session.clone(), c_directory.clone());
                if c_weak.upgrade_in_event_loop(move |c_ui| {
                    let l_names = if let Some((l_rows, _)) = cached_file_rows(&l_sess, &l_dir) {
                        l_rows.into_iter().map(|c| c.name.to_string()).collect()
                    } else if c_ui.get_active_session().as_str() == l_sess && c_ui.get_browse_path().as_str() == l_dir && c_ui.get_remote_ready() {
                        c_ui.get_files().iter().map(|c| c.name.to_string()).collect()
                    } else {
                        Vec::new()
                    };
                    let _ = l_tx.send(l_names);
                }).is_ok() {
                    l_rx.await.unwrap_or_default().into_iter().collect()
                } else {
                    std::collections::HashSet::new()
                }
            }
        };
        let z_conflicts = z_names.iter().filter(|c| z_taken.contains(*c)).cloned().collect::<Vec<_>>();
        // 覆盖许可按文件记录: 只有确认框里列出的目标可以覆盖.
        let mut z_overwrite = vec![false; z_names.len()];
        let mut z_skip = vec![false; z_names.len()];
        if !z_conflicts.is_empty() {
            let l_ctx = if is_en() { c_directory.clone() } else { format!("上传到 {c_directory}") };
            let Some(l_choice) = ask_conflict(c_weak.clone(), l_ctx, &z_conflicts).await else { return; };
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
                    // 保留两者只需已有文件名: names_only 跳过属主/时间查询, 不顶掉当前目录的 file_details 补全槽位.
                    if let Ok(Ok(l_list)) = tokio::time::timeout(
                        Duration::from_secs(6),
                        z_backend.call("list_files", json!({"session_id":c_session,"_link":c_link,"path":c_directory,"names_only":true}), Actor::User),
                    ).await {
                        z_taken.extend(array(&l_list, "entries").iter().map(|l| string(l, "name").to_owned()));
                    }
                    for (z_name, &z_is_dir) in z_names.iter_mut().zip(&z_is_dirs) {
                        if z_taken.contains(z_name) {
                            *z_name = unique_name(z_name, z_is_dir, |c| z_taken.contains(c) || z_seen.contains(c));
                            z_taken.insert(z_name.clone());
                        }
                    }
                }
            }
        }
        let mut z_count = 0;
        let mut z_failed = 0;
        let mut z_first_err: Option<anyhow::Error> = None;
        let z_skipped = z_skip.iter().filter(|c| **c).count();
        for (i, (((z_path, z_name), z_overwrite), z_is_dir)) in z_paths.into_iter().zip(z_names).zip(z_overwrite).zip(z_is_dirs).enumerate() {
            if z_skip[i] { continue; }
            let z_remote = format!("{}/{}", c_directory.trim_end_matches('/'), z_name);
            let z_result = z_backend
                .call(
                    if z_is_dir { "upload_directory" } else { "upload_file" },
                    json!({"session_id":c_session,"_link":c_link,"local_path":z_path,"remote_path":z_remote,"overwrite":z_overwrite}),
                    Actor::User,
                )
                .await;
            if let Err(err) = z_result {
                if is_fatal_batch_error(&err) {
                    show_notice(c_weak, NOTE_ERROR, batch_error(true, z_count, &err));
                    return;
                }
                z_failed += 1;
                z_first_err.get_or_insert(err);
                continue;
            }
            z_count += 1;
        }
        finish_transfer_batch(c_weak, true, z_count, z_failed, z_skipped, z_first_err);
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
        notify(&z_ui, NOTE_WARN, t("目录树已达显示上限, 可直接输入目录路径", "Directory tree limit reached; enter a path directly"));
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
                        notify(&c_ui, NOTE_WARN, t("目录树列表已截断, 可直接输入目录路径", "Directory tree truncated; enter a path directly"));
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
                    let l_err = friendly_error(&format!("{err:#}"));
                    notify_retry(
                        &c_ui,
                        if is_en() {
                            format!("Failed to load folder (retried): {l_err}")
                        } else {
                            format!("目录加载失败 (已自动重试): {l_err}")
                        },
                        "tree_toggle",
                        c_path.into(),
                    );
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
    let mut z_previous_by_path = HashMap::new();
    for l_subtree in z_previous.chunk_by(|_, c| c.depth > z_depth) {
        z_previous_by_path.entry(l_subtree[0].path.as_str()).or_insert(l_subtree);
    }
    let z_available = MAX_TREE_NODES.saturating_sub(z_nodes.len());
    let mut z_children = Vec::new();
    for z_entry in c_entries.iter().filter(|c| boolean(c, "is_dir")) {
        if z_children.len() >= z_available { break; }
        let z_path = format!("{}/{}", c_path.trim_end_matches('/'), string(z_entry, "name"));
        if let Some(l_subtree) = z_previous_by_path.get(z_path.as_str()) {
            z_children.extend(l_subtree.iter().take(z_available - z_children.len()).cloned());
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

// 替换文件列表模型前后把滚动偏移收回到新行数对应的有效范围, 避免批量删除后 ListView 残留旧负偏移把首行裁切一半.
fn clamp_file_scroll(c_ui: &AppWindow, c_rows: usize) {
    let z_min = (c_ui.get_file_view_height() - c_rows as f32 * 26.0).min(0.0);
    let z_cur = c_ui.get_file_scroll_y();
    let z_clamped = z_cur.clamp(z_min, 0.0);
    if (z_clamped - z_cur).abs() > 0.01 {
        c_ui.set_file_scroll_y(z_clamped);
    }
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
                    "list_files" | "file_details" | "get_task" | "cancel_task" | "connect" | "reconnect" | "new_tab" | "focus_session" | "disconnect"
                        | "open_terminal" | "close_terminal" | "trust_host" | "answer_auth" | "run_command"
                        | "upload_file" | "download_file" | "upload_directory" | "download_directory" | "send_command" | "send_saved_command" | "export_finalshell" | "copy_file"
                        | "import_finalshell" | "set_terminal_scrollback" | "import_commands" | "export_commands" | "move_tab" | "set_expanded_groups" | "set_tab_show_group" | "set_connection_columns"
                ) {
                    notify(
                        &z_ui,
                        NOTE_OK,
                        if is_en() {
                            format!("{} done", method_label(&c_method))
                        } else {
                            format!("{}完成", method_label(&c_method))
                        },
                    );
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
                        let l_n = number(&z_value, "direct") as usize;
                        notify(
                            &z_ui,
                            NOTE_OK,
                            if is_en() {
                                format!("Proxy deleted; {l_n} host(s) switched to Direct")
                            } else {
                                format!("代理已删除, {l_n} 台服务器改为直连")
                            },
                        );
                    }
                    "set_connection_proxy" => {
                        let l_n = number(&z_value, "count") as usize;
                        notify(
                            &z_ui,
                            NOTE_OK,
                            if is_en() {
                                format!("Proxy updated for {l_n} host(s); takes effect next connect")
                            } else {
                                format!("已为 {l_n} 台服务器设置代理, 下次连接生效")
                            },
                        );
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
                            let z_empty = boolean(&z_value, "empty");
                            if c_method != "focus_session" {
                                let mut z_sessions = z_ui.get_sessions().iter().collect::<Vec<_>>();
                                if let Some(l_row) = z_sessions.iter_mut().find(|c| c.id.as_str() == z_id) {
                                    l_row.empty = z_empty;
                                    if !z_empty {
                                        l_row.state = 1;
                                    }
                                    z_ui.set_sessions(update_rows(z_ui.get_sessions(), z_sessions));
                                } else {
                                    z_sessions.push(SessionRow {
                                        id: z_id.into(),
                                        label: if z_empty {
                                            t("快速连接", "Quick Connect").into()
                                        } else {
                                            t("连接中", "Connecting").into()
                                        },
                                        empty: z_empty,
                                        state: if z_empty { 0 } else { 1 },
                                        ..Default::default()
                                    });
                                    z_ui.set_sessions(update_rows(z_ui.get_sessions(), z_sessions));
                                }
                            }
                            z_ui.invoke_action("show_session".into(), z_id.into());
                            if c_method != "focus_session" {
                                z_ui.set_active_session_empty(z_empty);
                                if !z_empty {
                                    z_ui.set_active_connected(false);
                                    z_ui.set_active_latency_ms(-1);
                                    z_ui.set_active_state(1);
                                    z_ui.set_active_error(SharedString::default());
                                    z_ui.set_active_error_detail(SharedString::default());
                                    z_ui.set_active_dropped(false);
                                    z_ui.set_peer_status(SharedString::default());
                                    // 发起重连瞬间立即禁用旧文件区交互与拖放, 不等下次 500ms 轮询.
                                    z_ui.set_remote_ready(false);
                                    z_ui.set_remote_listed_session(SharedString::default());
                                    apply_monitor_usage(&z_ui, None);
                                    if z_ui.get_monitor_expanded() {
                                        apply_monitor_history(&z_ui, None);
                                        apply_monitor_processes(&z_ui, None);
                                    }
                                }
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
                            Some(l_first) => {
                                let l_first = friendly_error(l_first);
                                notify(
                                    &z_ui,
                                    NOTE_WARN,
                                    if is_en() {
                                        format!("Imported {l_count} host(s), skipped {} ({l_first})", l_skipped.len())
                                    } else {
                                        format!("已导入 {l_count} 条连接, 跳过 {} 条 ({l_first})", l_skipped.len())
                                    },
                                );
                            }
                            None => notify(
                                &z_ui,
                                NOTE_OK,
                                if is_en() {
                                    format!("Imported {l_count} host(s)")
                                } else {
                                    format!("已导入 {l_count} 条连接")
                                },
                            ),
                        }
                    }
                    "import_commands" => {
                        let l_added = number(&z_value, "added") as usize;
                        let l_skipped = number(&z_value, "skipped") as usize;
                        let l_msg = if is_en() {
                            format!(
                                "Imported {l_added} command(s){}",
                                if l_skipped > 0 { format!(", skipped {l_skipped} (duplicate or invalid)") } else { String::new() }
                            )
                        } else {
                            format!(
                                "已导入 {l_added} 条命令{}",
                                if l_skipped > 0 { format!(", 跳过 {l_skipped} 条 (已存在或无效)") } else { String::new() }
                            )
                        };
                        notify(&z_ui, if l_skipped > 0 { NOTE_WARN } else { NOTE_OK }, l_msg);
                    }
                    "export_commands" => {
                        let l_n = number(&z_value, "count") as usize;
                        notify(&z_ui, NOTE_OK, if is_en() { format!("Exported {l_n} command(s)") } else { format!("已导出 {l_n} 条命令") });
                    }
                    "set_terminal_scrollback" => {
                        let l_lines = number(&z_value, "lines") as u64;
                        notify(
                            &z_ui,
                            NOTE_OK,
                            if is_en() {
                                format!("Scrollback set to {l_lines} lines (new sessions)")
                            } else {
                                format!("终端历史已设为 {l_lines} 行, 新连接生效")
                            },
                        );
                    }
                    "set_download_dir" => z_ui.set_download_dir(string(&z_value, "path").into()),
                    "export_finalshell" => {
                        let l_n = number(&z_value, "count") as usize;
                        notify(&z_ui, NOTE_OK, if is_en() { format!("Exported {l_n} host(s)") } else { format!("已导出 {l_n} 条连接") });
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
                            z_ui.set_file_time_label(t("修改时间 (服务器)", "Modified (Server)").into());
                        }
                        cache_file_rows(&z_ui);
                        if !string(&z_value, "time_hint").is_empty() {
                            notify(&z_ui, NOTE_WARN, friendly_error(string(&z_value, "time_hint")));
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
                        if let Some(l_link) = z_value.get("link").and_then(Value::as_u64) {
                            let l_session = string(&z_value, "session_id");
                            // 新连接的列表到达 (可能早于状态轮询发现重连): 先作废旧连接的文件列表、目录树、缓存和弹窗, 再按新列表重建.
                            // 旧树节点不会留到新代次下, 从它们发起的操作也就带不上新代次.
                            if LISTED_LINK.with_borrow_mut(|c| c.insert(l_session.to_owned(), l_link)).is_some_and(|c| c != l_link) {
                                z_ui.invoke_action("discard_files".into(), l_session.into());
                            }
                        }
                        z_ui.set_browse_path(z_path.into());
                        z_ui.set_remote_listed_session(string(&z_value, "session_id").into());
                        z_ui.set_remote_ready(true);
                        // 已能正常浏览: 之前读取目录失败留在状态栏的错误不再有意义, 直接收起.
                        z_ui.invoke_dismiss_retry("list_files".into());
                        if z_ui.get_status_retry_arg().as_str() == z_path { z_ui.invoke_dismiss_retry("tree_toggle".into()); }
                        let z_timezone = string(&z_value, "server_timezone");
                        z_ui.set_file_time_label(if z_timezone.is_empty() {
                            t("修改时间 (UTC)", "Modified (UTC)").into()
                        } else {
                            t("修改时间 (服务器)", "Modified (Server)").into()
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
                            notify(&z_ui, NOTE_WARN, t("目录树列表已截断, 可直接输入目录路径", "Directory tree truncated; enter a path directly"));
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
                        let z_row_count = z_rows.len();
                        clamp_file_scroll(&z_ui, z_row_count);
                        z_ui.set_files(ModelRc::new(VecModel::from(z_rows)));
                        clamp_file_scroll(&z_ui, z_row_count);
                        cache_file_rows(&z_ui);
                        let z_has_pending = PENDING_UPLOAD_REFRESH.with_borrow(|c| {
                            let l_sid = z_ui.get_active_session().to_string();
                            c.contains(&(l_sid.clone(), z_path.to_owned())) || c.contains(&(l_sid, String::new()))
                        });
                        if !z_has_pending && let Some((l_session, l_path)) = FILE_FOCUS.take() {
                            focus_uploaded_file(&z_ui, &l_session, &l_path);
                        }
                        // 属主与时间仍在后台查询: 取到后就地更新这两列, 不重读目录.
                        if let Some(l_token) = z_value.get("details_token").and_then(Value::as_u64) {
                            z_ui.invoke_action("file_details".into(), l_token.to_string().into());
                        }
                        if !string(&z_value, "time_hint").is_empty() {
                            notify(&z_ui, NOTE_WARN, friendly_error(string(&z_value, "time_hint")));
                        }
                        if boolean(&z_value, "truncated") {
                            notify(&z_ui, NOTE_WARN, t("远端列表已达显示上限, 请进入更具体的目录", "Remote listing truncated; open a subfolder"));
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
                        let l_path = string(&z_value, "path");
                        notify(&z_ui, NOTE_OK, if is_en() { format!("Duplicated as {l_path}") } else { format!("已创建副本 {l_path}") });
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
                                if is_en() {
                                    if z_done == 0 {
                                        format!("Delete failed: {z_error}")
                                    } else {
                                        format!("{z_done} deleted, {} failed: {z_error}", z_failed.len())
                                    }
                                } else if z_done == 0 {
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
                            z_ui.set_task_output(t("命令已进入队列, 等待输出...", "Command queued, waiting for output...").into());
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
                let z_err = friendly_error(&format!("{err:#}"));
                let z_message = if is_en() {
                    format!("{} failed: {z_err}", method_label(&c_method))
                } else {
                    format!("{}失败: {z_err}", method_label(&c_method))
                };
                // 错误显示在仍打开的对话框里时不再发到状态栏, 免得对话框关闭后状态栏还留着同一条错误.
                let mut z_in_dialog = false;
                if c_method == "save_proxy" {
                    z_ui.set_proxy_form_error(z_message.chars().take(200).collect::<String>().into());
                    z_in_dialog = z_ui.get_proxy_editing();
                } else if matches!(c_method.as_str(), "save_connection" | "save_saved_command" | "save_command_group" | "save_connection_group") {
                    z_ui.set_form_error(z_message.chars().take(200).collect::<String>().into());
                    z_in_dialog = z_ui.get_editing() || z_ui.get_command_editing() || z_ui.get_group_editing();
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
                    z_ui.set_file_error(z_err.into());
                    z_ui.set_file_error_key(format!("{z_session}\n{z_path}").into());
                    FILE_CACHE.with_borrow_mut(|c| c.retain(|c| c.0 != (z_session.clone(), z_path.clone())));
                    z_ui.set_file_scroll_y(0.0);
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

// 切换语言后立即重建 Rust 侧已写入界面的状态文本与列表数据.
fn refresh_language_ui(c_ui: &AppWindow, c_state: &Value, c_answers: &Arc<Mutex<Vec<String>>>) {
    if !c_state.is_null() {
        apply_state(c_ui, c_state, c_answers);
        apply_mcp_status(c_ui, &c_state["mcp_status"]);
    } else if c_ui.get_editing() {
        refresh_edit_proxy_choices(c_ui);
    }
    let z_status = c_ui.get_status();
    if matches!(z_status.as_str(), "就绪" | "Ready") {
        c_ui.set_status(t("就绪", "Ready").into());
    } else if !z_status.is_empty() && is_en() {
        c_ui.set_status(friendly_error(z_status.as_str()).into());
    }
    c_ui.set_file_time_label(localize_file_time_label(c_ui.get_file_time_label().as_str()).into());
    let z_files = c_ui.get_files();
    if z_files.row_count() > 0 {
        let l_rows = localize_file_rows(z_files.iter());
        c_ui.set_files(update_rows(z_files, l_rows));
    }
    if !c_ui.get_terminal_size_text().is_empty() {
        c_ui.set_terminal_size_text(
            format_terminal_size(c_ui.get_terminal_cols(), c_ui.get_terminal_rows()).into(),
        );
    }
    if !c_ui.get_mcp_url().is_empty() {
        c_ui.set_mcp_prompt(
            build_mcp_prompt(c_ui.get_mcp_url().as_str(), c_ui.get_mcp_config().as_str()).into(),
        );
    }
    MCP_CHECK_GEN.fetch_add(1, Ordering::AcqRel);
    c_ui.set_mcp_checking(false);
    c_ui.set_mcp_check_title(SharedString::default());
    c_ui.set_mcp_check_detail(SharedString::default());
    let z_selected_task = c_ui.get_selected_task();
    if !z_selected_task.is_empty() {
        c_ui.invoke_action("get_task".into(), z_selected_task);
    }
}

fn apply_state(c_ui: &AppWindow, c_state: &Value, c_answers: &Arc<Mutex<Vec<String>>>) {
    if matches!(c_ui.get_status().as_str(), "正在加载连接..." | "Loading hosts...") {
        notify(c_ui, NOTE_INFO, t("就绪", "Ready"));
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
                    "" => t("直连", "Direct"),
                    l_id => z_proxies.iter().find(|c| string(c, "id") == l_id).map(|c| string(c, "name")).unwrap_or(t("代理已删除", "Deleted proxy")),
                }
                .into(),
                auth: match string(c_item, "auth") {
                    "private_key" => t("私钥", "Key"),
                    "keyboard_interactive" => t("交互", "Interactive"),
                    _ => t("密码", "Password"),
                }
                .into(),
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
                        _ if boolean(c_item, "empty") => t("快速连接", "Quick Connect").into(),
                        2 => string(c_item, "name").into(),
                        3 => format!("{} · {}", string(c_item, "name"), t("仅文件", "Files Only")).into(),
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
    apply_active_session(c_ui, c_state, z_active.as_str());
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
        let l_err = string(c_state, "quick_commands_error");
        let l_text = if is_en() {
            format!("Failed to load commands: {l_err}")
        } else {
            format!("读取常用命令失败: {l_err}")
        };
        if c_ui.get_status().as_str() != l_text {
            notify(c_ui, NOTE_ERROR, l_text);
        }
    }
    let z_tasks = array(c_state, "tasks");
    let z_today = local_today();
    LAST_LOCAL_DAY.set(z_today);
    let z_task_connections = array(c_state, "connections").iter().map(|c| (string(c, "id"), c)).collect::<HashMap<_, _>>();
    c_ui.set_tasks(update_rows(c_ui.get_tasks(),
        z_tasks
            .iter()
            .map(|c_item| {
                let l_total = number(c_item, "total");
                let l_bytes = number(c_item, "bytes");
                let l_status = string(c_item, "status");
                let l_connection = z_task_connections.get(string(c_item, "connection_id")).copied();
                let l_machine = l_connection.map(server_label).unwrap_or_else(|| t("连接已删除", "Deleted host").into());
                let l_ratio = if l_total > 0.0 { (l_bytes / l_total).clamp(0.0, 1.0) } else { -1.0 };
                let l_pct = l_ratio.max(0.0) * 100.0;
                let l_speed = number(c_item, "speed_bytes_per_sec");
                let l_command = string(c_item, "kind") == "command";
                let l_exit = c_item.get("exit_code").and_then(Value::as_i64);
                let l_directory = boolean(c_item, "directory");
                let l_files = number(c_item, "files_total");
                let l_file_bytes = number(c_item, "file_bytes");
                let l_file_total = number(c_item, "file_total");
                let l_time: SharedString = if task_active(l_status) {
                    SharedString::default()
                } else {
                    c_item
                        .get("finished_at_ms")
                        .and_then(Value::as_u64)
                        .or_else(|| c_item.get("created_at_ms").and_then(Value::as_u64))
                        .map(|c| format_task_time(c, z_today))
                        .unwrap_or_default()
                        .into()
                };
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
                        ("mcp", l_client) => i18n::display_client(l_client).chars().take(16).collect::<String>().into(),
                        _ => t("用户", "User").into(),
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
                        Some(l_code) if l_command => format!("{} · {} {l_code}", status_label(l_status), t("退出码", "Exit")),
                        _ if !l_command && l_status == "running" && boolean(c_item, "main_link") => {
                            format!("{} · {}", status_label(l_status), t("主连接", "Main Link"))
                        }
                        _ => status_label(l_status).to_string(),
                    }
                    .into(),
                    detail: match l_status {
                        _ if l_command => SharedString::default(),
                        "completed" => format!(
                            "{}{}{}",
                            if l_directory {
                                if is_en() { format!("{l_files:.0} files · ") } else { format!("{l_files:.0} 个文件 · ") }
                            } else {
                                String::new()
                            },
                            format_bytes(l_bytes),
                            if number(c_item, "stderr_bytes") == 0.0 { "" } else { t(" · 有跳过项", " · skipped") },
                        ).into(),
                        "queued" | "preparing" if l_directory && l_files == 0.0 => SharedString::default(),
                        "queued" | "preparing" if l_directory => format!(
                            "{}{}",
                            if is_en() { format!("{l_files:.0} files") } else { format!("{l_files:.0} 个文件") },
                            if l_total > 0.0 { format!(" · {}", format_bytes(l_total)) } else { String::new() }
                        ).into(),
                        "queued" | "preparing" if l_total > 0.0 => format_bytes(l_total).into(),
                        "running" if l_directory => format!(
                            "{:.0}/{l_files:.0} · {}{l_pct:.0}%",
                            (number(c_item, "files_done") + number(c_item, "files_failed") + 1.0).min(l_files),
                            if l_speed > 0.0 { format!("{}/s · ", format_bytes(l_speed)) } else { String::new() },
                        ).into(),
                        "retrying" if l_total > 0.0 => format!("{l_pct:.0}%").into(),
                        "retrying" if l_bytes > 0.0 => format_bytes(l_bytes).into(),
                        "running" if l_speed > 0.0 && l_total > l_bytes => {
                            let l_eta = format_eta((l_total - l_bytes) / l_speed);
                            if is_en() {
                                format!("{l_pct:.0}% · {}/s · {l_eta} left", format_bytes(l_speed))
                            } else {
                                format!("{l_pct:.0}% · {}/s · 剩 {l_eta}", format_bytes(l_speed))
                            }
                            .into()
                        }
                        "running" if l_total > 0.0 => format!("{l_pct:.0}% · {}", format_bytes(l_total)).into(),
                        "running" if l_speed > 0.0 => format!("{} · {}/s", format_bytes(l_bytes), format_bytes(l_speed)).into(),
                        "running" => format_bytes(l_bytes).into(),
                        _ => SharedString::default(),
                    },
                    time: l_time,
                    ratio: if l_directory {
                        // 文件夹进度条与百分比统一按整体字节计算 (总字节未知或全 0 字节时回退单文件进度), 避免遇 0 字节文件抖动.
                        if l_status == "running" {
                            if l_total > 0.0 { l_ratio as f32 } else if l_file_total > 0.0 { (l_file_bytes / l_file_total).clamp(0.0, 1.0) as f32 } else { -1.0 }
                        } else {
                            -1.0
                        }
                    } else if l_command {
                        -1.0
                    } else {
                        l_ratio as f32
                    },
                    running_plain: l_status == "running" && !boolean(c_item, "main_link"),
                    cancellable: task_active(l_status) && l_status != "cancelling",
                    local: string(c_item, "local").into(),
                }
            })
            .collect::<Vec<_>>(),
    ));
    apply_transfer_summary(c_ui, z_tasks, &z_task_connections);
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
    // 认证弹窗只在提示变化时重建, 避免刷新清空正在输入的密码. 请求标识随回答带回, 换了一轮请求 (文字相同也算) 就重建.
    let z_request = z_auth.map(|c_item| string(c_item, "auth_id")).unwrap_or("");
    let z_old_prompts = c_ui.get_prompts();
    let z_changed = c_ui.get_auth_session().as_str() != z_auth_id
        || AUTH_REQUEST.with_borrow(|c| c != z_request)
        || z_old_prompts.row_count() != z_prompts.len()
        || z_prompts.iter().enumerate().any(|(i, c_item)| {
            z_old_prompts.row_data(i).is_none_or(|l_old| {
                l_old.prompt.as_str() != i18n::localize_auth_prompt(string(c_item, "prompt"))
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
                    prompt: i18n::localize_auth_prompt(string(c_item, "prompt")).into(),
                    echo: boolean(c_item, "echo"),
                })
                .collect::<Vec<_>>(),
        )));
        c_ui.set_auth_session(z_auth_id.into());
        AUTH_REQUEST.with_borrow_mut(|c| *c = z_request.to_owned());
    }
}

fn apply_active_session(c_ui: &AppWindow, c_state: &Value, c_active: &str) {
    let z_session = array(c_state, "sessions")
        .iter()
        .find(|c_item| string(c_item, "session_id") == c_active);
    let z_raw_status = z_session.map(|c| string(c, "status")).unwrap_or_default();
    let z_state_code = z_session.map(session_state).unwrap_or(0);
    let z_err = session_error(z_raw_status);
    c_ui.set_active_session_empty(z_session.is_none_or(|c| boolean(c, "empty")));
    c_ui.set_active_connected(z_raw_status == "connected");
    c_ui.set_active_state(z_state_code);
    c_ui.set_active_dropped(z_raw_status.starts_with("error: 连接已断开:"));
    c_ui.set_active_connection_id(z_session.map(|c| string(c, "connection_id")).unwrap_or_default().into());
    c_ui.set_active_latency_ms(
        z_session
            .filter(|c| string(c, "status") == "connected")
            .and_then(|c| c.get("latency_ms"))
            .and_then(Value::as_i64)
            .map(|c| c.clamp(0, i32::MAX as i64) as i32)
            .unwrap_or(-1),
    );
    if z_state_code == 3 && z_err.is_empty() && c_ui.get_terminal_text().starts_with("终端打开失败:") {
        c_ui.set_active_error(friendly_error(c_ui.get_terminal_text().trim()).into());
        c_ui.set_active_error_detail(SharedString::default());
    } else {
        c_ui.set_active_error(z_err.into());
        c_ui.set_active_error_detail(
            z_session
                .map(|c| i18n::localize_disconnect_detail(string(c, "disconnect_detail")))
                .unwrap_or_default()
                .into(),
        );
    }
    c_ui.set_peer_status(
        z_session
            .map(|c| {
                if boolean(c, "empty") {
                    t("快速连接", "Quick Connect").into()
                } else {
                    format!("{} · {}", server_label(c), status_label(string(c, "status")))
                }
            })
            .unwrap_or_else(|| t("未连接", "Not connected").into())
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
            .unwrap_or_else(|| t("尚未连接", "Not connected").into())
            .into(),
    );
    if c_ui.get_monitor_expanded() {
        apply_monitor_history(c_ui, z_session);
        apply_monitor_processes(c_ui, z_session);
    }
    apply_monitor_usage(c_ui, z_session);
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
                    0..5 => t("刚刚", "just now").into(),
                    5..60 => if is_en() { format!("{l_seconds}s ago") } else { format!("{l_seconds} 秒前") }.into(),
                    60..3600 => if is_en() { format!("{}m ago", l_seconds / 60) } else { format!("{} 分钟前", l_seconds / 60) }.into(),
                    _ => if is_en() { format!("{}h ago", l_seconds / 3600) } else { format!("{} 小时前", l_seconds / 3600) }.into(),
                },
                // 第一行 "动作 · 客户端", 第二行对象; 动作名带出在主终端还是独立执行.
                label: format!(
                    "{} · {}",
                    mcp_action(string(c, "method")).0,
                    match string(c, "client") { "" => "AI", l_client => i18n::display_client(client_name(l_client)) }
                )
                .into(),
                detail: i18n::localize_mcp_detail(string(c, "detail")).into(),
                info: i18n::localize_mcp_info(string(c, "method"), string(c, "info")).into(),
                error: friendly_error(string(c, "error")).into(),
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
    let z_text = string(c_value, "text");
    c_ui.set_bracketed_paste(boolean(c_value, "bracketed_paste"));
    c_ui.set_application_cursor(boolean(c_value, "application_cursor"));
    c_ui.set_modified_keys(number(c_value, "modified_keys") as i32);
    c_ui.set_hide_cursor(boolean(c_value, "hide_cursor"));
    c_ui.set_cursor_shape(number(c_value, "cursor_shape") as i32);
    c_ui.set_mouse_mode(number(c_value, "mouse") as i32);
    c_ui.set_mouse_tracking(number(c_value, "mouse_tracking") as i32);
    c_ui.set_terminal_text(z_text.into());
    if c_ui.get_active_state() == 3 {
        c_ui.set_active_error_detail(SharedString::default());
        if z_text.starts_with("终端打开失败:") {
            c_ui.set_active_error(friendly_error(z_text.trim()).into());
        } else {
            c_ui.set_active_error(SharedString::default());
        }
    }
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
    let z_raw_kind = string(c_item, "kind");
    let z_kind = match z_raw_kind {
        "directory" => t("文件夹", "Folder"),
        "symlink" => t("链接", "Link"),
        _ => t("文件", "File"),
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
        icon: if z_dir { 0 } else if z_raw_kind == "symlink" { 8 } else {
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

fn utc_datetime(c_seconds: u64) -> (i32, u32, u32, u32, u32, u32) {
    // UTC公历换算, 不引入日期库; 纪元平移到3月, 使闰日落在每年的末尾.
    let z_seconds = c_seconds.min(253_402_300_799);
    let z_days = (z_seconds / 86_400) as i64 + 719_468;
    let z_era = z_days.div_euclid(146_097);
    let z_day_of_era = z_days.rem_euclid(146_097);
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
    (
        z_year as i32,
        z_month as u32,
        z_day as u32,
        (z_seconds % 86_400 / 3_600) as u32,
        (z_seconds % 3_600 / 60) as u32,
        (z_seconds % 60) as u32,
    )
}

fn format_file_time(c_seconds: Option<u64>) -> String {
    let Some(z_seconds) = c_seconds.filter(|c| *c <= 253_402_300_799) else {
        return "—".into();
    };
    let (z_year, z_month, z_day, z_hour, z_minute, _) = utc_datetime(z_seconds);
    format!("{z_year:04}-{z_month:02}-{z_day:02} {z_hour:02}:{z_minute:02}")
}

// 将 Unix 秒换算为本机本地时间 (年, 月, 日, 时, 分, 秒); 系统调用失败或超界时回退 UTC.
fn local_datetime(c_seconds: u64) -> (i32, u32, u32, u32, u32, u32) {
    let z_seconds = c_seconds.min(253_402_300_799);
    #[cfg(all(unix, target_pointer_width = "64"))]
    {
        use std::ffi::{c_char, c_int, c_long};
        #[repr(C)]
        struct Tm {
            tm_sec: c_int,
            tm_min: c_int,
            tm_hour: c_int,
            tm_mday: c_int,
            tm_mon: c_int,
            tm_year: c_int,
            tm_wday: c_int,
            tm_yday: c_int,
            tm_isdst: c_int,
            tm_gmtoff: c_long,
            tm_zone: *const c_char,
        }
        unsafe extern "C" {
            fn localtime_r(c_time: *const c_long, c_out: *mut Tm) -> *mut Tm;
        }
        if let Ok(l_time) = c_long::try_from(z_seconds) {
            let mut l_tm = Tm {
                tm_sec: 0, tm_min: 0, tm_hour: 0, tm_mday: 0, tm_mon: 0, tm_year: 0,
                tm_wday: 0, tm_yday: 0, tm_isdst: 0, tm_gmtoff: 0, tm_zone: std::ptr::null(),
            };
            if !unsafe { localtime_r(&raw const l_time, &raw mut l_tm) }.is_null() {
                return (
                    l_tm.tm_year + 1900,
                    (l_tm.tm_mon + 1) as u32,
                    l_tm.tm_mday as u32,
                    l_tm.tm_hour as u32,
                    l_tm.tm_min as u32,
                    l_tm.tm_sec.clamp(0, 59) as u32,
                );
            }
        }
    }
    #[cfg(windows)]
    {
        #[repr(C)]
        struct SystemTime {
            year: u16,
            month: u16,
            day_of_week: u16,
            day: u16,
            hour: u16,
            minute: u16,
            second: u16,
            milliseconds: u16,
        }
        #[link(name = "kernel32")]
        unsafe extern "system" {
            fn SystemTimeToTzSpecificLocalTime(
                c_tz: *const std::ffi::c_void,
                c_utc: *const SystemTime,
                c_local: *mut SystemTime,
            ) -> i32;
        }
        let (l_year, l_month, l_day, l_hour, l_minute, l_second) = utc_datetime(z_seconds);
        if let Ok(l_year) = u16::try_from(l_year) {
            let l_utc = SystemTime {
                year: l_year, month: l_month as u16, day_of_week: 0, day: l_day as u16,
                hour: l_hour as u16, minute: l_minute as u16, second: l_second as u16, milliseconds: 0,
            };
            let mut l_local = SystemTime {
                year: 0, month: 0, day_of_week: 0, day: 0, hour: 0, minute: 0, second: 0, milliseconds: 0,
            };
            if unsafe { SystemTimeToTzSpecificLocalTime(std::ptr::null(), &raw const l_utc, &raw mut l_local) } != 0 {
                return (
                    i32::from(l_local.year),
                    u32::from(l_local.month),
                    u32::from(l_local.day),
                    u32::from(l_local.hour),
                    u32::from(l_local.minute),
                    u32::from(l_local.second),
                );
            }
        }
    }
    utc_datetime(z_seconds)
}

fn local_today() -> (i32, u32, u32) {
    let z_seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |c| c.as_secs());
    let (z_year, z_month, z_day, _, _, _) = local_datetime(z_seconds);
    (z_year, z_month, z_day)
}

// 已结束任务列表时间: 当天显示 HH:MM, 跨日显示 MM-DD HH:MM.
fn format_task_time(c_ms: u64, c_today: (i32, u32, u32)) -> String {
    if c_ms == 0 {
        return String::new();
    }
    let (z_year, z_month, z_day, z_hour, z_minute, _) = local_datetime(c_ms / 1000);
    if (z_year, z_month, z_day) == c_today {
        format!("{z_hour:02}:{z_minute:02}")
    } else {
        format!("{z_month:02}-{z_day:02} {z_hour:02}:{z_minute:02}")
    }
}

// 任务详情中的完整本地时间 (精确到秒).
fn format_local_timestamp(c_ms: u64) -> String {
    let (z_year, z_month, z_day, z_hour, z_minute, z_second) = local_datetime(c_ms / 1000);
    format!("{z_year:04}-{z_month:02}-{z_day:02} {z_hour:02}:{z_minute:02}:{z_second:02}")
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

// 编辑连接的代理下拉: 直连 + 当前代理列表, 位置与 ID 一一对应. 代理列表在编辑期间变化时按 ID 重新定位;
// 所选代理已不存在 (如从别的电脑导入) 时保留一项提示, 不默认改成直连.
fn refresh_edit_proxy_choices(c_ui: &AppWindow) {
    let z_selected = c_ui.get_edit_proxy_id();
    let mut z_ids = vec![SharedString::default()];
    let mut z_names = vec![SharedString::from(t("直连", "Direct"))];
    for l_proxy in c_ui.get_proxies().iter() {
        z_ids.push(l_proxy.id);
        z_names.push(l_proxy.name);
    }
    if !z_selected.is_empty() && !z_ids.contains(&z_selected) {
        z_ids.push(z_selected.clone());
        z_names.push(t("原代理已不存在, 请重新选择", "Proxy deleted; select another").into());
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
    format!("[{}] {}", i18n::display_group(z_group), string(c_item, "name"))
}

fn status_label(c_status: &str) -> &str {
    match c_status {
        "connected" => t("已连接", "Connected"),
        "empty" => t("快速连接", "Quick Connect"),
        "connecting" => t("连接中", "Connecting"),
        "host_key_confirmation" => t("确认指纹", "Verify Key"),
        "auth_required" => t("等待认证", "Auth Required"),
        "authenticating" => t("认证中", "Authenticating"),
        "authentication_failed" => t("认证失败", "Auth Failed"),
        "disconnected" => t("已断开", "Disconnected"),
        "queued" => t("排队中", "Queued"),
        "preparing" => t("准备中", "Preparing"),
        "running" => t("进行中", "Running"),
        "retrying" => t("等待恢复", "Retrying"),
        "cancelling" => t("正在取消", "Cancelling"),
        "completed" => t("已完成", "Completed"),
        "cancelled" => t("已取消", "Cancelled"),
        "failed" => t("失败", "Failed"),
        // 具体原因由状态栏左侧消息和终端区提示显示, 标签与会话状态只给简短状态.
        _ if c_status.starts_with("error: 连接已断开:") => t("已断开", "Disconnected"),
        _ if c_status.starts_with("error") => t("连接失败", "Failed"),
        _ => c_status,
    }
}

// 会话失败原因: error: 前缀为具体错误, authentication_failed 为服务器拒绝了凭据; 其他状态为空.
fn session_error(c_status: &str) -> String {
    match c_status {
        "authentication_failed" => t("服务器拒绝了密码或私钥, 请检查凭据", "Server rejected password or key; check credentials").into(),
        _ => {
            let l_msg = c_status.strip_prefix("error: ").unwrap_or_default();
            let l_raw = l_msg.strip_prefix("连接已断开: ").unwrap_or(l_msg);
            if l_raw.is_empty() { String::new() } else { friendly_error(l_raw) }
        }
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
            t("未启用", "Disabled").into()
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
fn apply_transfer_summary(c_ui: &AppWindow, c_tasks: &[Value], c_connections: &HashMap<&str, &Value>) {
    let mut z_names = HashMap::new();
    // 每个方向依次累计任务数、已传字节、已知总量和正在传输的速度.
    let mut z_totals = [(0, 0.0, 0.0, None); 2];
    let z_new_user = TRANSFER_SEEN.with_borrow_mut(|c_seen| {
        let l_previous = std::mem::take(c_seen);
        let mut l_new = false;
        for l_task in c_tasks {
            let i = match string(l_task, "kind") { "upload" => 0, "download" => 1, _ => continue };
            let l_id = string(l_task, "task_id");
            c_seen.insert(l_id.to_owned());
            // 传输面板只显示纯连接名称, 避免分组前缀挤掉主机名.
            if let Some(l_connection) = c_connections.get(string(l_task, "connection_id")) {
                z_names.insert(l_id, string(l_connection, "name"));
            }
            let l_status = string(l_task, "status");
            if !task_active(l_status) {
                continue;
            }
            l_new |= string(l_task, "source") != "mcp" && !l_previous.contains(l_id);
            let (l_count, l_bytes, l_total, l_speed) = &mut z_totals[i];
            *l_count += 1;
            let l_size = number(l_task, "total");
            if l_size > 0.0 {
                *l_bytes += number(l_task, "bytes");
                *l_total += l_size;
            }
            if l_status == "running" {
                *l_speed.get_or_insert(0.0) += number(l_task, "speed_bytes_per_sec");
            }
        }
        TRANSFER_CLEARED.with_borrow_mut(|c| c.retain(|l| c_seen.contains(l)));
        l_new
    });
    let mut z_rows = TRANSFER_CLEARED.with_borrow(|c_cleared| {
        c_ui.get_tasks().iter().filter(|c| c.kind != 0 && !c_cleared.contains(c.id.as_str())).collect::<Vec<_>>()
    });
    for l_row in &mut z_rows {
        if let Some(l_name) = z_names.get(l_row.id.as_str()) {
            l_row.server = (*l_name).into();
        }
    }
    c_ui.set_transfer_failed(z_rows.iter().any(|c| c.state == 2));
    c_ui.set_transfers(update_rows(c_ui.get_transfers(), z_rows));
    let [(z_up_count, z_up_bytes, z_up_total, z_up_speed), (z_down_count, z_down_bytes, z_down_total, z_down_speed)] = z_totals;
    let z_progress = |c_bytes: f64, c_total: f64| {
        if c_total > 0.0 { (c_bytes / c_total).clamp(0.0, 1.0) as f32 } else { -1.0 }
    };
    let z_speed = |c_speed: Option<f64>| c_speed.map(|c| format!("{}/s", format_bytes(c)).into()).unwrap_or_default();
    c_ui.set_transfer_uploads(z_up_count);
    c_ui.set_transfer_downloads(z_down_count);
    c_ui.set_transfer_up_progress(z_progress(z_up_bytes, z_up_total));
    c_ui.set_transfer_down_progress(z_progress(z_down_bytes, z_down_total));
    c_ui.set_transfer_up_speed(z_speed(z_up_speed));
    c_ui.set_transfer_down_speed(z_speed(z_down_speed));
    if z_new_user && !c_ui.get_modal_open() {
        c_ui.set_transfers_open(true);
    }
}

fn task_active(c_status: &str) -> bool {
    matches!(c_status, "queued" | "preparing" | "running" | "retrying" | "cancelling")
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
    if is_en() {
        match z_seconds {
            0..60 => format!("{z_seconds}s"),
            60..3600 => format!("{}m {}s", z_seconds / 60, z_seconds % 60),
            _ => format!("{}h {}m", z_seconds / 3600, z_seconds % 3600 / 60),
        }
    } else {
        match z_seconds {
            0..60 => format!("{z_seconds}秒"),
            60..3600 => format!("{}分{}秒", z_seconds / 60, z_seconds % 60),
            _ => format!("{}小时{}分", z_seconds / 3600, z_seconds % 3600 / 60),
        }
    }
}

// 在系统文件管理器中显示本机路径: c_select 为 true 时选中该文件 (已不存在则打开所在目录), 否则打开该目录.
fn reveal_local(c_ui: &AppWindow, c_path: &str, c_select: bool) {
    let z_path = std::path::Path::new(c_path);
    if c_path.is_empty() || !z_path.is_absolute() {
        return notify(c_ui, NOTE_WARN, t("没有可打开的本机路径", "No local path available"));
    }
    let z_select = c_select && z_path.exists();
    let z_target = if c_select && !z_select { z_path.parent().unwrap_or(z_path) } else { z_path };
    if !z_target.exists() {
        return notify(
            c_ui,
            NOTE_WARN,
            if is_en() {
                format!("Local path not found: {}", z_target.display())
            } else {
                format!("本机路径已不存在: {}", z_target.display())
            },
        );
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
        notify(
            c_ui,
            NOTE_ERROR,
            if is_en() {
                format!("Failed to open file manager: {err}")
            } else {
                format!("打开文件管理器失败: {err}")
            },
        );
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
    let mut z_chars = c_text.chars();
    let z_char = z_chars.next().filter(|_| z_chars.next().is_none())?;
    let z_key = |c_key: Key| z_char == char::from(c_key);
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
    let z_printable = !z_char.is_control() && !('\u{e000}'..='\u{f8ff}').contains(&z_char);
    if z_printable && c_alt && !z_extended {
        if cfg!(windows) && c_control && !z_char.is_ascii_alphanumeric() {
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
            if z_modifier == 1 && c_application_cursor {
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
            let z_letter = char::from(b'P' + (z_code - 11) as u8);
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
    if z_key(Key::Return) || matches!(z_char, '\r' | '\n') {
        if z_kitty && z_modifier > 1 {
            return z_kitty_code(13);
        }
        // Ctrl+Enter 发送换行符 (同 Ctrl+J): Shell 中等同回车, Claude Code、Pi 等用作输入框换行.
        return Some(if z_extended {
            format!("\u{1b}[27;{z_modifier};13~")
        } else if c_alt { "\u{1b}\r".into() } else if c_control { "\n".into() } else { "\r".into() });
    }
    if z_kitty && (c_control || c_alt) {
        // Kitty 协议用未按 Shift 的键值, Shift 已在修饰位中; 符号按美式键位还原 (如 Ctrl+Shift+/ 的 ? 还原为 /).
        let l_base = if c_shift {
            "~!@#$%^&*()_+{}|:\"<>?".find(z_char).map(|i| "`1234567890-=[]\\;',./".as_bytes()[i] as char).unwrap_or(z_char)
        } else { z_char };
        return z_kitty_code(l_base.to_ascii_lowercase() as u32);
    }
    if z_extended {
        return Some(format!("\u{1b}[27;{z_modifier};{}~", z_char as u32));
    }
    if c_control {
        if c_shift && c_text.eq_ignore_ascii_case("v") {
            return None;
        }
        let l_ascii = z_char.to_ascii_uppercase() as u32;
        if (64..=95).contains(&l_ascii) {
            return Some(char::from((l_ascii & 31) as u8).to_string());
        }
        // xterm 对数字和 / 的传统映射: Ctrl+2 为 NUL, Ctrl+3..7 为 ESC..US, Ctrl+8 为 DEL, Ctrl+/ 同 Ctrl+_.
        let l_code = match z_char {
            ' ' | '2' => Some(0x00),
            '3' => Some(0x1b),
            '4' => Some(0x1c),
            '5' => Some(0x1d),
            '6' => Some(0x1e),
            '7' | '/' => Some(0x1f),
            '8' => Some(0x7f),
            _ => None,
        };
        if let Some(l_code) = l_code {
            return Some(char::from(l_code).to_string());
        }
    }
    c_alt.then(|| format!("\u{1b}{z_char}"))
}

// 滚轮: 远端开启鼠标上报时发送滚轮事件 (tmux、vim mouse=a 等), 否则在全屏程序中按方向键滚动 (less、man 等).
fn terminal_wheel(c_lines: i32, c_row: i32, c_col: i32, c_modifiers: i32, c_mouse_mode: i32, c_application_cursor: bool) -> String {
    let z_up = c_lines > 0;
    let z_count = c_lines.unsigned_abs().min(10) as usize;
    let z_event = if c_mouse_mode != 0 {
        terminal_mouse(if z_up { 64 } else { 65 } | c_modifiers, c_row, c_col, false, c_mouse_mode)
    } else {
        format!("\u{1b}{}{}", if c_application_cursor { 'O' } else { '[' }, if z_up { 'A' } else { 'B' })
    };
    z_event.repeat(z_count)
}

fn terminal_mouse(c_code: i32, c_row: i32, c_col: i32, c_release: bool, c_mode: i32) -> String {
    if c_mode == 2 {
        return format!("\u{1b}[<{c_code};{};{}{}", c_col + 1, c_row + 1, if c_release { 'm' } else { 'M' });
    }
    // 传统格式用按钮 3 表示释放; ASCII 输入队列以 95 格为界. UTF-8 扩展允许更长的坐标.
    let z_code = if c_release { c_code | 3 } else { c_code };
    let z_limit = if c_mode == 3 { 2014 } else { 94 };
    let z_coord = |c_value: i32| char::from_u32(33 + c_value.clamp(0, z_limit) as u32).unwrap();
    format!("\u{1b}[M{}{}{}", char::from(32 + z_code as u8), z_coord(c_col), z_coord(c_row))
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
    let mut z_names = vec![SharedString::from(t("全部网卡", "All Interfaces"))];
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
    c_ui.set_network_peak(
        if z_sampled {
            if is_en() {
                format!("Peak {}/s", format_bytes(z_peak))
            } else {
                format!("峰值 {}/s", format_bytes(z_peak))
            }
        } else {
            String::new()
        }
        .into(),
    );
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
        return t("等待监控数据", "Waiting for monitor data").into();
    };
    if !string(z_monitor, "error").is_empty() {
        return format!("{}\n{}", t("监控暂不可用", "Monitor unavailable"), friendly_error(string(z_monitor, "error")));
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
        "completed" if z_code.is_some_and(|c| c != 0) => t("执行失败", "Failed"),
        "completed" if z_kind == "command" => t("执行成功", "Succeeded"),
        l_status => status_label(l_status),
    };
    let mut z_output = String::new();
    match z_kind {
        "upload" => {
            let _ = writeln!(z_output, "{}  {}", t("上传", "Upload"), string(c_task, "local"));
            let _ = writeln!(z_output, "{}  {}", t("到  ", "To    "), string(c_task, "label"));
        }
        "download" => {
            let _ = writeln!(z_output, "{}  {}", t("下载", "Download"), string(c_task, "label"));
            let _ = writeln!(z_output, "{}  {}", t("到  ", "To      "), string(c_task, "local"));
        }
        _ => {
            let _ = writeln!(z_output, "{}  {}", t("命令", "Command"), string(c_task, "label").replace('\n', "\n         "));
        }
    }
    if let Some(z_row) = &z_row {
        let _ = writeln!(
            z_output,
            "{}  {}  ·  {} {}",
            t("服务器", "Host"),
            z_row.server,
            t("来源", "Source"),
            z_row.source
        );
    }
    let _ = write!(z_output, "{}  {z_status}", t("状态", "Status"));
    if let Some(z_code) = z_code {
        let _ = write!(z_output, "  ·  {} {z_code}", t("退出码", "Exit"));
    }
    if let Some(z_timing) = task_timing(c_task) {
        let _ = write!(z_output, "  ·  {z_timing}");
    }
    z_output.push('\n');
    if let Some(z_at) = ["finished_at_ms", "started_at_ms", "created_at_ms"]
        .into_iter()
        .find_map(|c_key| c_task.get(c_key).and_then(Value::as_u64).filter(|c| *c > 0))
    {
        let _ = writeln!(z_output, "{}  {}", t("时间", "Time"), format_local_timestamp(z_at));
    }
    if z_kind != "command" {
        let l_total = number(c_task, "total");
        let _ = writeln!(
            z_output,
            "{}  {}{}",
            t("大小", "Size"),
            format_bytes(number(c_task, "bytes")),
            if l_total > 0.0 { format!(" / {}", format_bytes(l_total)) } else { String::new() }
        );
        if boolean(c_task, "directory") {
            let l_failed = number(c_task, "files_failed");
            let _ = writeln!(
                z_output,
                "{}  {:.0} / {:.0}{}",
                t("文件", "Files"),
                number(c_task, "files_done"),
                number(c_task, "files_total"),
                if l_failed > 0.0 {
                    if is_en() { format!("  ({l_failed:.0} failed)") } else { format!("  (失败 {l_failed:.0})") }
                } else {
                    String::new()
                },
            );
            if string(c_task, "status") == "running" {
                let _ = writeln!(z_output, "{}  {}", t("当前", "Current"), string(c_task, "current"));
            }
        }
        let l_retries = number(c_task, "transfer_retries");
        if l_retries > 0.0 {
            if is_en() {
                let _ = writeln!(z_output, "Retries  {l_retries:.0}");
            } else {
                let _ = writeln!(z_output, "自动重试  {l_retries:.0} 次");
            }
        }
        if string(c_task, "status") == "retrying" {
            z_output.push_str(t(
                "正在自动重连, 恢复后从已保留进度继续; 可取消任务.\n",
                "Reconnecting automatically; will resume from saved progress. You may cancel.\n",
            ));
        }
    }
    let mut z_section = |c_title: &str, c_text: &str| {
        if !c_text.is_empty() {
            let _ = write!(z_output, "\n[{c_title}]\n{}", c_text.trim_end_matches('\n'));
            z_output.push('\n');
        }
    };
    let z_stdout = if z_kind == "command" {
        i18n::localize_command_output(string(c_task, "stdout")).to_owned()
    } else {
        i18n::localize_transfer_output(string(c_task, "stdout"))
    };
    z_section(t("输出", "Output"), &z_stdout);
    let z_stderr = if z_kind == "command" {
        string(c_task, "stderr").to_owned()
    } else {
        i18n::localize_transfer_output(string(c_task, "stderr"))
    };
    z_section(
        if z_code == Some(0) || (z_kind != "command" && string(c_task, "status") == "completed") {
            t("提示输出", "Notice")
        } else {
            t("错误输出", "Stderr")
        },
        &z_stderr,
    );
    if string(c_task, "last_transfer_error") != string(c_task, "error") {
        let l_retry_err = friendly_error(string(c_task, "last_transfer_error"));
        z_section(t("最近一次重试原因", "Last Retry Error"), &l_retry_err);
    }
    let z_err = friendly_error(string(c_task, "error"));
    z_section(
        if string(c_task, "status") == "retrying" { t("恢复原因", "Recovery Reason") } else { t("错误", "Error") },
        &z_err,
    );
    if z_kind == "command" && matches!(string(c_task, "status"), "completed") && string(c_task, "stdout").is_empty() && string(c_task, "stderr").is_empty() {
        z_output.push_str(t("\n(无输出)\n", "\n(no output)\n"));
    }
    if boolean(c_task, "truncated") {
        z_output.push_str(t("\n输出已达显示上限.\n", "\nOutput truncated.\n"));
    }
    z_output
}
// 已开始的任务给出用时; 传输完成后附平均速度.
fn task_timing(c_task: &Value) -> Option<String> {
    let z_start = c_task.get("started_at_ms").and_then(Value::as_u64)?;
    let z_end = c_task.get("finished_at_ms").and_then(Value::as_u64)?;
    let z_ms = z_end.saturating_sub(z_start);
    let z_seconds = z_ms / 1000;
    let mut z_text = if is_en() {
        if z_seconds >= 60 {
            format!("{}m {}s", z_seconds / 60, z_seconds % 60)
        } else {
            format!("{:.1}s", z_ms as f64 / 1000.0)
        }
    } else if z_seconds >= 60 {
        format!("用时 {}分{}秒", z_seconds / 60, z_seconds % 60)
    } else {
        format!("用时 {:.1} 秒", z_ms as f64 / 1000.0)
    };
    if string(c_task, "kind") != "command" && string(c_task, "status") == "completed" && z_ms > 0 {
        let l_avg = format_bytes(number(c_task, "bytes") * 1000.0 / z_ms as f64);
        let _ = std::fmt::Write::write_fmt(&mut z_text, format_args!(" · {} {l_avg}/s", t("平均", "avg")));
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
        "state" => (t("查看状态", "Read State"), 0),
        "read_terminal" => (t("读取主终端", "Read Terminal"), 0),
        "list_files" => (t("浏览远端目录", "List Remote"), 0),
        "list_local_files" => (t("浏览本机目录", "List Local"), 0),
        "get_task" => (t("查询任务", "Check Task"), 0),
        "cancel_task" => (t("取消任务", "Cancel Task"), 0),
        "connect" => (t("连接", "Connect"), 1),
        "disconnect" => (t("断开", "Disconnect"), 1),
        "reconnect" => (t("重新连接", "Reconnect"), 1),
        "open_terminal" => (t("打开终端", "Open Terminal"), 1),
        "upload_file" => (t("上传文件", "Upload File"), 2),
        "download_file" => (t("下载文件", "Download File"), 2),
        "upload_directory" => (t("上传文件夹", "Upload Folder"), 2),
        "download_directory" => (t("下载文件夹", "Download Folder"), 2),
        "write_terminal" => (t("输入主终端", "Write Terminal"), 3),
        "input_admin_password" => (t("输入管理员密码", "Send sudo Password"), 3),
        "run_command" => (t("独立执行命令", "Run Detached"), 3),
        c_other => (method_label(c_other), 0),
    }
}
fn method_label(c_method: &str) -> &str {
    match c_method {
        "state" => t("读取状态", "Read state"),
        "read_terminal" => t("读取终端", "Read terminal"),
        "write_terminal" => t("输入终端", "Terminal input"),
        "input_admin_password" => t("输入管理员密码", "Send sudo password"),
        "get_task" => t("查询任务", "Check task"),
        "connect" => t("连接", "Connect"),
        "open_terminal" => t("重新连接终端", "Open terminal"),
        "close_terminal" => t("关闭终端", "Close terminal"),
        "save_connection" => t("保存连接", "Save host"),
        "save_saved_command" => t("保存常用命令", "Save command"),
        "delete_saved_command" => t("删除常用命令", "Delete command"),
        "save_command_group" => t("保存命令分组", "Save command group"),
        "delete_command_group" => t("删除命令分组", "Delete command group"),
        "save_connection_group" => t("保存连接分组", "Save host group"),
        "delete_connection_group" => t("删除连接分组", "Delete host group"),
        "save_proxy" => t("保存代理", "Save proxy"),
        "delete_proxy" => t("删除代理", "Delete proxy"),
        "set_connection_proxy" => t("设置代理", "Set proxy"),
        "send_saved_command" => t("发送常用命令", "Send command"),
        "send_command" => t("发送命令", "Send command"),
        "list_local_files" => t("浏览本地目录", "List local"),
        "remove_connection" => t("删除连接", "Delete host"),
        "import_finalshell" => t("导入", "Import"),
        "export_finalshell" => t("导出", "Export"),
        "import_commands" => t("导入命令", "Import commands"),
        "export_commands" => t("导出命令", "Export commands"),
        "list_files" => t("目录浏览", "List files"),
        "set_permissions" => t("修改权限", "Set permissions"),
        "rename_file" => t("重命名", "Rename"),
        "create_directory" => t("新建文件夹", "New folder"),
        "create_file" => t("新建文件", "New file"),
        "focus_session" => t("切换标签", "Switch tab"),
        "set_terminal_font" => t("保存终端字体", "Save terminal font"),
        "set_terminal_scrollback" => t("保存历史行数", "Save scrollback"),
        "set_tab_show_group" => t("保存标签分组显示", "Save group display"),
        "set_download_dir" => t("设置下载目录", "Set download folder"),
        "set_connection_columns" => t("保存列宽", "Save column widths"),
        "delete_files" => t("删除", "Delete"),
        "delete_files_fast" => t("快速删除", "Force delete"),
        "copy_file" => t("创建副本", "Duplicate"),
        "upload_file" => t("上传任务创建", "Queue upload"),
        "download_file" => t("下载任务创建", "Queue download"),
        "upload_directory" | "download_directory" => t("文件夹传输任务创建", "Queue folder transfer"),
        "run_command" => t("命令任务创建", "Queue command"),
        "trust_host" => t("主机信任处理", "Trust host"),
        "answer_auth" => t("认证回答提交", "Submit auth"),
        "cancel_task" => t("任务取消", "Cancel task"),
        "disconnect" => t("断开", "Disconnect"),
        "new_tab" => t("新建标签", "New tab"),
        "clear_recent_connections" => t("删除快速连接记录", "Clear recent hosts"),
        "move_tab" => t("移动标签", "Move tab"),
        "set_expanded_groups" => t("保存分组展开状态", "Save expanded groups"),
        "reconnect" => t("重新连接", "Reconnect"),
        _ => t("操作", "Action"),
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
async fn ask_conflict(c_weak: slint::Weak<AppWindow>, c_target: String, c_names: &[String]) -> Option<ConflictChoice> {
    let z_names = c_names.join("\n");
    match ask_in_ui(c_weak, move |c_ui| {
        c_ui.set_conflict_kind(0);
        c_ui.set_conflict_target(c_target.into());
        c_ui.set_conflict_names(z_names.into());
    }).await? {
        1 => Some(ConflictChoice::Overwrite),
        2 => Some(ConflictChoice::KeepBoth),
        3 => Some(ConflictChoice::Skip),
        _ => None,
    }
}
// 下载多项到默认目录前确认目录 (界面内对话框, 与同名询问同一风格), 确认返回 true.
async fn confirm_download(c_weak: slint::Weak<AppWindow>, c_count: usize, c_dir: String) -> bool {
    ask_in_ui(c_weak, move |c_ui| {
        c_ui.set_conflict_kind(1);
        c_ui.set_conflict_count(c_count as i32);
        c_ui.set_conflict_target(c_dir.into());
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
            let l_more = if l_items.len() > 5 {
                if is_en() {
                    format!("\n\n… and {} more; see Tasks", l_items.len() - 5)
                } else {
                    format!("\n\n… 另有 {} 个失败, 详见任务列表", l_items.len() - 5)
                }
            } else {
                String::new()
            };
            let l_footer = t(
                "目标文件保持原样, 未完成的临时文件已清理 (网络中断的任务已先自动重连续传). 请查看原因后重新传输.",
                "Destination files were left intact and temporary files were cleaned up (interrupted transfers were retried automatically). Check the reason and retry.",
            );
            rfd::AsyncMessageDialog::new()
                .set_title(t("传输失败", "Transfer Failed"))
                .set_level(rfd::MessageLevel::Error)
                .set_description(format!("{}{l_more}\n\n{l_footer}", l_items[..l_items.len().min(5)].join("\n\n")))
                .set_buttons(rfd::MessageButtons::Ok)
                .show()
                .await;
        }
    });
}
// "名称 (1).扩展名" 或文件夹 "名称 (1)" 形式的未占用名称.
fn unique_name(c_name: &str, c_directory: bool, c_taken: impl Fn(&str) -> bool) -> String {
    let (z_stem, z_ext) = match c_name.rfind('.') {
        Some(i) if !c_directory && i > 0 => (&c_name[..i], &c_name[i..]),
        _ => (c_name, ""),
    };
    (1..1000).map(|i| format!("{z_stem} ({i}){z_ext}")).find(|c| !c_taken(c)).unwrap_or_else(|| c_name.to_owned())
}
// 原生确认对话框: 统一处理自定义主按钮与系统 Ok/Yes 回传.
async fn confirm_dialog(
    c_title: &'static str,
    c_description: impl Into<String>,
    c_level: rfd::MessageLevel,
    c_ok: &'static str,
) -> bool {
    let z_answer = rfd::AsyncMessageDialog::new()
        .set_title(c_title)
        .set_level(c_level)
        .set_description(c_description.into())
        .set_buttons(rfd::MessageButtons::OkCancelCustom(c_ok.into(), t("取消", "Cancel").into()))
        .show()
        .await;
    matches!(z_answer, rfd::MessageDialogResult::Ok | rfd::MessageDialogResult::Yes)
        || matches!(&z_answer, rfd::MessageDialogResult::Custom(c) if c == c_ok)
}

fn is_fatal_batch_error(c_err: &anyhow::Error) -> bool {
    let z_text = format!("{c_err:#}");
    z_text.contains("会话已断开")
        || z_text.contains("会话不存在")
        || z_text.contains("标签已重新连接")
        || z_text.contains("任务队列已满")
}
// 批量创建传输任务中途失败: 一个都没创建时不再显示"已加入 0 个".
fn batch_error(c_upload: bool, c_count: usize, c_err: &anyhow::Error) -> String {
    let z_err = friendly_error(&format!("{c_err}"));
    if is_en() {
        let l_kind = if c_upload { "upload" } else { "download" };
        if c_count == 0 {
            format!("Failed to queue {l_kind}: {z_err}")
        } else {
            format!("Queued {c_count} {l_kind}(s), remaining failed: {z_err}")
        }
    } else {
        let l_kind = if c_upload { "上传" } else { "下载" };
        if c_count == 0 {
            format!("{l_kind}任务创建失败: {z_err}")
        } else {
            format!("已加入 {c_count} 个{l_kind}任务, 后续任务创建失败: {z_err}")
        }
    }
}
// 上传与下载共用批量排队汇总提示, 统一经过 friendly_error 处理错误文案.
fn finish_transfer_batch(
    c_weak: slint::Weak<AppWindow>,
    c_upload: bool,
    c_count: usize,
    c_failed: usize,
    c_skipped: usize,
    c_first_err: Option<anyhow::Error>,
) {
    if let Some(err) = c_first_err {
        let l_err = friendly_error(&format!("{err:#}"));
        let l_msg = if is_en() {
            let l_kind = if c_upload { "upload" } else { "download" };
            let l_cap = if c_upload { "Upload" } else { "Download" };
            let l_skip = if c_skipped > 0 { format!(", {c_skipped} skipped") } else { String::new() };
            if c_count == 0 {
                format!("{l_cap} failed to queue ({c_failed}{l_skip}): {l_err}")
            } else {
                format!("Queued {c_count} {l_kind}(s){l_skip}, {c_failed} failed: {l_err}")
            }
        } else {
            let l_kind = if c_upload { "上传" } else { "下载" };
            let l_skip = if c_skipped > 0 { format!(", 已跳过 {c_skipped} 项") } else { String::new() };
            if c_count == 0 {
                format!("{l_kind}任务创建失败 ({c_failed} 项{l_skip}): {l_err}")
            } else {
                format!("已加入 {c_count} 个{l_kind}任务{l_skip}, {c_failed} 项创建失败: {l_err}")
            }
        };
        show_notice(c_weak, NOTE_ERROR, l_msg);
        return;
    }
    let z_msg = if is_en() {
        let l_kind = if c_upload { "upload" } else { "download" };
        format!(
            "Queued {c_count} {l_kind}(s){}",
            if c_skipped > 0 { format!(", {c_skipped} skipped") } else { String::new() }
        )
    } else {
        let l_kind = if c_upload { "上传" } else { "下载" };
        format!(
            "已加入 {c_count} 个{l_kind}任务{}",
            if c_skipped > 0 { format!(", 已跳过 {c_skipped} 个同名文件") } else { String::new() }
        )
    };
    show_notice(c_weak, NOTE_OK, z_msg);
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
                let l_err = friendly_error(&format!("{err}"));
                show_notice(
                    c_weak.clone(),
                    NOTE_ERROR,
                    if is_en() { format!("Terminal input failed: {l_err}") } else { format!("终端输入失败: {l_err}") },
                );
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
fn build_mcp_prompt(c_url: &str, c_config: &str) -> String {
    let z_pretty = serde_json::from_str::<Value>(c_config)
        .ok()
        .and_then(|c| serde_json::to_string_pretty(&c).ok())
        .unwrap_or_else(|| c_config.to_owned());
    if is_en() {
        format!(
            "Please register my local HiPShell as an MCP server (Streamable HTTP). One-time setup.\n\n\
             - Name: hipshell\n- URL: {c_url}\n- No token, custom headers, or startup command required.\n\n\
             Configure for your client:\n\
             - Claude Code (including Claude Desktop Code): run `claude mcp add --transport http --scope user hipshell {c_url}` for all projects, \
             or write the JSON below into `.mcp.json` in the project root.\n\
             - Codex: add two lines to `~/.codex/config.toml`: `[mcp_servers.hipshell]` and `url = \"{c_url}\"`.\n\
             - Other clients (Cursor, etc.): add this JSON (`mcpServers` format) to your MCP config:\n{z_pretty}\n\n\
             Reload MCP and call `state` on `hipshell` to verify you can see the session list.\n\n\
             Notes: Keep HiPShell running while in use; no host IP, username, or password is needed; \
             only hosts with \"Allow AI (MCP) access to this host\" enabled in HiPShell can be accessed. Full usage is in the server instructions."
        )
    } else {
        format!(
            "请把本机的 HiPShell 注册为你的 MCP 服务器 (HTTP 方式, 即 Streamable HTTP). 只需配置一次.\n\n\
             - 名称: hipshell\n- 网址: {c_url}\n- 不需要令牌、请求头或启动命令.\n\n\
             按你所在的客户端登记:\n\
             - Claude Code (含 Claude Desktop 的 Code): 所有项目可用请运行 claude mcp add --transport http --scope user hipshell {c_url}; \
             只给当前项目用则在项目根目录的 .mcp.json 写入下面的 JSON.\n\
             - Codex: 在 ~/.codex/config.toml 加入两行: [mcp_servers.hipshell] 与 url = \"{c_url}\".\n\
             - 其他客户端 (Cursor 等): 写入其 MCP 配置文件, 通用 JSON (mcpServers 格式):\n{z_pretty}\n\n\
             登记后重新加载 MCP, 并调用 hipshell 的 state 工具确认能看到会话列表.\n\n\
             说明: 使用时 HiPShell 需要保持运行; 不需要服务器地址、账号或密码; \
             只有在 HiPShell 中勾选“允许 AI 操作此服务器”的服务器可以被操作. 具体用法在连接后的 instructions 中."
        )
    }
}
// MCP 设置页: 开关、端口草稿与运行状态; 运行中时生成网址、JSON 配置和交给 AI 的安装说明.
fn apply_mcp_settings(c_ui: &AppWindow, c_enabled: bool, c_port: u64, c_status: &std::result::Result<String, String>) {
    c_ui.set_mcp_enabled(c_enabled);
    c_ui.set_mcp_port(c_port as i32);
    c_ui.set_mcp_port_draft(c_port as i32);
    // 开关或端口变了, 作废在途自检并清空旧结论.
    MCP_CHECK_GEN.fetch_add(1, Ordering::AcqRel);
    c_ui.set_mcp_checking(false);
    c_ui.set_mcp_check_title("".into());
    c_ui.set_mcp_check_detail("".into());
    let Ok(z_url) = c_status else {
        c_ui.set_mcp_url("".into());
        c_ui.set_mcp_config("".into());
        c_ui.set_mcp_prompt("".into());
        c_ui.set_mcp_error(if c_enabled {
            c_status.as_ref().err().map(|c| friendly_error(c)).unwrap_or_default()
        } else {
            String::new()
        }.into());
        return;
    };
    let z_config = json!({"mcpServers":{"hipshell":{"type":"http","url":z_url}}}).to_string();
    let z_prompt = build_mcp_prompt(z_url, &z_config);
    c_ui.set_mcp_error("".into());
    c_ui.set_mcp_url(z_url.as_str().into());
    c_ui.set_mcp_config(z_config.into());
    c_ui.set_mcp_prompt(z_prompt.into());
}

// 标签 ID → 输入代次, 见终端输入分发处.
type InputEpochs = Arc<Mutex<HashMap<String, u64>>>;
// 终端输入总队列 (标签, 数据, (入队时的输入代次, 要求的终端代次)) 与代次表.
type InputQueue = (tokio::sync::mpsc::Sender<(String, String, (u64, u64))>, InputEpochs);

// 入队时记下该标签当前的输入代次; 空数据表示标签已关闭. 队列满时返回 false.
// c_terminal: 非 0 时只写入该代次的终端 (后台核对), 0 为不限.
// 不限代次的输入 (普通按键、粘贴、常用命令) 按最近一次状态轮询看到的后台终端代次绑定: MCP 或后台重开终端时,
// 界面自己的输入代次不会变化, 靠它让按旧终端编码、排队中的输入不进入新终端.
static TERMINAL_GEN: std::sync::LazyLock<Mutex<HashMap<String, u64>>> = std::sync::LazyLock::new(Default::default);
fn enqueue_input(c_queue: &InputQueue, c_session: String, c_data: String, c_terminal: u64) -> bool {
    let z_epoch = c_queue.1.lock().unwrap().get(&c_session).copied().unwrap_or(0);
    let c_terminal = if c_terminal == 0 { TERMINAL_GEN.lock().unwrap().get(&c_session).copied().unwrap_or(0) } else { c_terminal };
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
