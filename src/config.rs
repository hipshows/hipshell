use crate::model::{Actor, AdminPassword, AuthMethod, Connection};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    // 锁中毒只在持锁线程 panic 后出现, 数据仍可用, 统一直接取回而不逐处转换为错误.
    sync::{Mutex, PoisonError},
    time::{SystemTime, UNIX_EPOCH},
};
use zeroize::Zeroizing;

const MAX_FILE_SIZE: u64 = 1024 * 1024;
// 终端历史行数可选值; 每行约占 列数 x 32 字节, 1 万行 160 列约 50 MiB.
pub const TERMINAL_SCROLLBACK_CHOICES: [u32; 4] = [1000, 2000, 5000, 10000];
// 连接管理列表默认列宽 (逻辑像素): 主机、端口、用户、认证、代理.
pub const CONNECTION_COLUMNS: [u32; 5] = [130, 50, 80, 48, 80];

#[derive(Clone, Serialize, Deserialize, Default)]
struct HostKeys(HashMap<String, String>);

#[derive(Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AppSettings {
    pub dark_theme: bool,
    // 终端标签名前是否显示 [分组].
    pub tab_show_group: bool,
    // 下载目录; 空字符串表示用桌面.
    pub download_dir: String,
    // 连接管理列表的列宽 (主机、端口、用户、认证、代理), 拖动表头分隔线调整后立即保存; 名称列占剩余宽度.
    pub connection_columns: Vec<u32>,
    pub recent_connections: Vec<String>,
    // 空字符串表示使用平台默认字体.
    pub terminal_font: String,
    pub terminal_font_size: u32,
    // 连接分组列表 (按显示顺序), 用于保留尚无连接的空分组; 连接自身的分组字段仍是归属依据.
    pub connection_groups: Vec<String>,
    // 连接的显示顺序 (连接 ID), 拖动排序时整体更新; 未列出的连接按名称排在后面.
    pub connection_order: Vec<String>,
    // 每个标签保留的终端历史行数, 超出后覆盖最旧的内容; 新连接生效.
    pub terminal_scrollback: u32,
    // MCP 服务开关与本机端口; 端口固定, 客户端配置的网址才长期有效.
    pub mcp_enabled: bool,
    pub mcp_port: u16,
    // 上次关闭时的窗口与面板布局及连接分组展开状态, 启动时恢复.
    pub layout: WindowLayout,
}

// 窗口位置为物理像素 (多显示器、不同缩放下按桌面坐标定位), 尺寸为逻辑像素; 最大化时保留最大化前的位置大小.
// width 为 0 表示未保存过, 使用默认窗口.
#[derive(Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct WindowLayout {
    pub x: i32,
    pub y: i32,
    pub width: u32,
    pub height: u32,
    pub maximized: bool,
    pub monitor_expanded: bool,
    pub monitor_width: u32,
    pub bottom_collapsed: bool,
    pub bottom_height: u32,
    pub bottom_page: i32,
    // 连接管理中展开的分组名; 未列出的分组默认收起.
    pub expanded_groups: Vec<String>,
}

impl Default for WindowLayout {
    fn default() -> Self {
        Self { x: 0, y: 0, width: 0, height: 0, maximized: false, monitor_expanded: false, monitor_width: 260, bottom_collapsed: true, bottom_height: 232, bottom_page: 0, expanded_groups: Vec::new() }
    }
}

impl Default for AppSettings {
    fn default() -> Self {
        Self {
            dark_theme: true,
            tab_show_group: true,
            download_dir: String::new(),
            connection_columns: CONNECTION_COLUMNS.to_vec(),
            recent_connections: Vec::new(),
            terminal_font: String::new(),
            terminal_font_size: 13,
            connection_groups: Vec::new(),
            connection_order: Vec::new(),
            terminal_scrollback: 2000,
            mcp_enabled: true,
            mcp_port: crate::mcp::DEFAULT_PORT,
            layout: WindowLayout::default(),
        }
    }
}

// 代理管理中的一项; 连接通过 proxy_id 选用. 密码按连接密码同样的方式加密保存, 只在建立连接时解密.
#[derive(Clone, Serialize, Deserialize)]
pub struct Proxy {
    #[serde(default)]
    pub id: String,
    pub name: String,
    // socks5 或 http (HTTP CONNECT).
    pub kind: String,
    pub host: String,
    pub port: u16,
    #[serde(default)]
    pub username: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    password: String,
}

const MAX_PROXIES: usize = 64;

#[derive(Default)]
struct Volatile {
    host_keys: HostKeys,
    settings: AppSettings,
}

pub struct ConfigStore {
    directory: PathBuf,
    state: Mutex<Volatile>,
    write_lock: Mutex<()>,
    // 连接文件解析缓存: 路径 -> (修改时间, 大小, 内容). 状态每 500ms 刷新一次, 文件未变时不再重复读取和解析.
    file_cache: Mutex<HashMap<PathBuf, (Option<SystemTime>, u64, serde_json::Value)>>,
}

impl ConfigStore {
    // 唯一配置目录, GUI 与 MCP 共用:
    // macOS 按系统惯例放在 ~/Library/Application Support/HiPShell (应用包装在 /Applications, 包内不可写入);
    // Windows 为免安装版, 放在 exe 旁的 config.
    pub fn default_directory() -> Result<PathBuf> {
        #[cfg(target_os = "macos")]
        {
            let z_home = std::env::var_os("HOME").filter(|c| !c.is_empty()).context("无法确定用户目录")?;
            Ok(PathBuf::from(z_home).join("Library/Application Support/HiPShell"))
        }
        #[cfg(not(target_os = "macos"))]
        {
            let z_executable = std::env::current_exe().context("无法确定程序路径")?;
            Ok(z_executable.parent().context("程序路径没有父目录")?.join("config"))
        }
    }

    pub fn load(c_path: PathBuf) -> Result<Self> {
        fs::create_dir_all(&c_path).context("创建配置目录失败")?;
        let c_path = c_path.canonicalize().context("解析配置目录路径失败")?;
        set_directory_permissions(&c_path)?;
        let z_hosts_path = c_path.join("known_hosts.json");
        let z_host_keys = if z_hosts_path.exists() {
            read_json_limited(&z_hosts_path)?
        } else {
            HostKeys::default()
        };
        validate_host_keys(&z_host_keys)?;
        let z_settings_path = c_path.join("settings.json");
        let mut z_settings = if z_settings_path.exists() {
            read_json_limited(&z_settings_path)?
        } else {
            AppSettings::default()
        };
        normalize_recent(&mut z_settings);
        Ok(Self {
            directory: c_path,
            state: Mutex::new(Volatile {
                host_keys: z_host_keys,
                settings: z_settings,
            }),
            write_lock: Mutex::new(()),
            file_cache: Mutex::new(HashMap::new()),
        })
    }

    fn local_path(&self, c_id: &str) -> Result<Option<PathBuf>> {
        Ok(self.local_record(c_id)?.map(|c| c.0))
    }

    // 返回已解析内容, 读取连接时不必再次打开和解析同一文件.
    // 文件修改时间与大小都未变时直接返回缓存; 写入均为原子替换, 修改时间必然变化.
    fn read_cached(&self, c_path: &Path) -> Result<serde_json::Value> {
        let z_meta = fs::symlink_metadata(c_path).context("读取配置文件信息失败")?;
        let z_key = (z_meta.modified().ok(), z_meta.len());
        if let Some((l_time, l_len, l_value)) = self.file_cache.lock().unwrap_or_else(PoisonError::into_inner).get(c_path) {
            if (*l_time, *l_len) == z_key && l_time.is_some() {
                return Ok(l_value.clone());
            }
        }
        let z_value: serde_json::Value = read_json_limited(c_path)?;
        self.file_cache
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(c_path.to_owned(), (z_key.0, z_key.1, z_value.clone()));
        Ok(z_value)
    }

    // 每个连接一个 {id}_connect_config.json (FinalShell 格式 + hipshell 扩展字段), 不存在时返回 None.
    fn local_record(&self, c_id: &str) -> Result<Option<(PathBuf, serde_json::Value)>> {
        let z_path = native_path(&self.directory, c_id)?;
        if !z_path.exists() {
            return Ok(None);
        }
        let z_value = self.read_cached(&z_path)?;
        Ok(Some((z_path, z_value)))
    }

    pub fn settings(&self) -> Result<AppSettings> {
        let mut z_settings = self.state.lock().unwrap_or_else(PoisonError::into_inner)
            .settings
            .clone();
        z_settings
            .recent_connections
            .retain(|c_id| self.get(c_id).is_ok());
        Ok(z_settings)
    }

    pub fn set_dark_theme(&self, c_dark: bool) -> Result<()> {
        self.update_settings(|c| std::mem::replace(&mut c.dark_theme, c_dark) != c_dark)
    }

    pub fn set_tab_show_group(&self, c_show: bool) -> Result<()> {
        self.update_settings(|c| std::mem::replace(&mut c.tab_show_group, c_show) != c_show)
    }

    // 列宽限制在界面允许的拖动范围内 (40-400), 个数不对时恢复默认.
    pub fn set_connection_columns(&self, mut c_columns: Vec<u32>) -> Result<()> {
        if c_columns.len() != CONNECTION_COLUMNS.len() {
            c_columns = CONNECTION_COLUMNS.to_vec();
        }
        c_columns.iter_mut().for_each(|c| *c = (*c).clamp(40, 400));
        self.update_settings(|c| {
            let l_changed = c.connection_columns != c_columns;
            c.connection_columns = c_columns;
            l_changed
        })
    }

    // 空字符串清除默认目录; 否则须为已存在的本机绝对目录.
    pub fn set_download_dir(&self, c_path: &str) -> Result<()> {
        ensure!(c_path.is_empty() || (Path::new(c_path).is_absolute() && Path::new(c_path).is_dir()), "下载目录不存在或不是绝对路径");
        ensure!(c_path.len() <= 4096 && !c_path.contains('\0'), "下载目录路径无效");
        self.update_settings(|c| {
            let l_changed = c.download_dir != c_path;
            c.download_dir = c_path.to_owned();
            l_changed
        })
    }

    pub fn set_terminal_font(&self, c_family: &str, c_size: u32) -> Result<()> {
        self.update_settings(|c| {
            let l_changed = c.terminal_font != c_family || c.terminal_font_size != c_size;
            c.terminal_font = c_family.to_owned();
            c.terminal_font_size = c_size;
            l_changed
        })
    }

    pub fn set_terminal_scrollback(&self, c_lines: u32) -> Result<()> {
        ensure!(TERMINAL_SCROLLBACK_CHOICES.contains(&c_lines), "历史行数只能是 1000、2000、5000 或 10000");
        self.update_settings(|c| std::mem::replace(&mut c.terminal_scrollback, c_lines) != c_lines)
    }

    pub fn set_layout(&self, mut c_layout: WindowLayout) -> Result<()> {
        // 面板尺寸限制在界面允许的拖动范围内, 手改配置文件也不会得到异常布局.
        c_layout.monitor_width = c_layout.monitor_width.clamp(220, 520);
        c_layout.bottom_height = c_layout.bottom_height.clamp(150, 2000);
        c_layout.bottom_page = c_layout.bottom_page.clamp(0, 2);
        c_layout.expanded_groups.truncate(256);
        // 窗口低于界面最小尺寸 (800x540) 视为无效, 下次按默认大小打开.
        if c_layout.width < 800 || c_layout.height < 540 {
            (c_layout.width, c_layout.height) = (0, 0);
        }
        self.update_settings(|c| {
            let l_changed = c.layout != c_layout;
            c.layout = c_layout;
            l_changed
        })
    }

    // 分组展开状态在展开/收起时单独保存, 不必等关闭窗口.
    pub fn set_expanded_groups(&self, mut c_groups: Vec<String>) -> Result<()> {
        c_groups.truncate(256);
        self.update_settings(|c| {
            let l_changed = c.layout.expanded_groups != c_groups;
            c.layout.expanded_groups = c_groups;
            l_changed
        })
    }

    pub fn set_mcp(&self, c_enabled: bool, c_port: u16) -> Result<()> {
        // 1024 以下为系统保留端口, 普通用户无法监听.
        ensure!(c_port >= 1024, "端口需在 1024-65535 之间");
        self.update_settings(|c| {
            let l_changed = c.mcp_enabled != c_enabled || c.mcp_port != c_port;
            c.mcp_enabled = c_enabled;
            c.mcp_port = c_port;
            l_changed
        })
    }

    // 新建 (c_old 为空) 或重命名连接分组; 重命名会同步修改组内每条连接.
    pub fn save_connection_group(&self, c_old: &str, c_name: &str) -> Result<()> {
        let z_name = c_name.trim();
        ensure!(
            !z_name.is_empty() && z_name.chars().count() <= 64 && !z_name.chars().any(char::is_control),
            "分组名称无效"
        );
        let z_old = c_old.trim();
        let z_connections = self.list();
        let mut z_groups = self.settings()?.connection_groups;
        let z_exists = |c: &str| z_groups.iter().any(|l| l == c) || z_connections.iter().any(|l| l.group == c);
        if z_old.is_empty() {
            ensure!(!z_exists(z_name) && z_name != "默认", "分组名称重复");
            ensure!(z_groups.len() < 64, "连接分组最多 64 个");
            z_groups.push(z_name.to_owned());
        } else {
            ensure!(z_old != "默认", "默认分组不能改名");
            if z_old == z_name {
                return Ok(());
            }
            ensure!(!z_exists(z_name) && z_name != "默认", "分组名称重复");
            for mut l_conn in z_connections.into_iter().filter(|c| c.group == z_old) {
                l_conn.group = z_name.to_owned();
                self.save(l_conn, None)?;
            }
            z_groups.retain(|c| c != z_old);
            z_groups.push(z_name.to_owned());
        }
        self.update_settings(|c| {
            c.connection_groups = z_groups;
            true
        })
    }

    // 拖动排序分组: 保存完整的分组顺序 (含默认分组).
    pub fn reorder_connection_groups(&self, c_groups: Vec<String>) -> Result<()> {
        ensure!(c_groups.len() <= 65, "连接分组最多 64 个");
        let mut z_seen = HashSet::new();
        ensure!(c_groups.iter().all(|c| !c.trim().is_empty() && z_seen.insert(c.as_str())), "分组顺序无效");
        self.update_settings(|c| {
            c.connection_groups = c_groups;
            true
        })
    }

    // 拖动排序连接: 跨分组时先改连接所属分组, 再保存完整的连接顺序.
    pub fn move_connection(&self, c_id: &str, c_group: &str, c_order: Vec<String>) -> Result<()> {
        ensure!(c_order.len() <= crate::finalshell::MAX_CONNECTIONS, "连接顺序无效");
        let mut z_conn = self.get(c_id)?;
        if z_conn.group != c_group {
            ensure!(!c_group.trim().is_empty(), "分组名称无效");
            z_conn.group = c_group.to_owned();
            self.save(z_conn, None)?;
        }
        self.update_settings(|c| {
            c.connection_order = c_order;
            true
        })
    }

    // 删除分组时组内连接移到默认分组, 连接本身不删除.
    pub fn delete_connection_group(&self, c_name: &str) -> Result<()> {
        ensure!(c_name != "默认", "默认分组不能删除");
        for mut l_conn in self.list().into_iter().filter(|c| c.group == c_name) {
            l_conn.group = "默认".into();
            self.save(l_conn, None)?;
        }
        self.update_settings(|c| {
            let l_len = c.connection_groups.len();
            c.connection_groups.retain(|l| l != c_name);
            l_len != c.connection_groups.len()
        })
    }

    // 修改函数返回是否有变化; 无变化时不写文件.
    fn update_settings(&self, c_change: impl FnOnce(&mut AppSettings) -> bool) -> Result<()> {
        let _z_write = self.write_lock.lock().unwrap_or_else(PoisonError::into_inner);
        let mut z_state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let mut z_settings = z_state.settings.clone();
        if !c_change(&mut z_settings) {
            return Ok(());
        }
        write_json_atomic(&self.directory.join("settings.json"), &z_settings)?;
        z_state.settings = z_settings;
        Ok(())
    }

    pub fn record_recent(&self, c_id: &str) -> Result<()> {
        let _z_write = self.write_lock.lock().unwrap_or_else(PoisonError::into_inner);
        self.get(c_id)?;
        let mut z_state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let mut z_settings = z_state.settings.clone();
        z_settings.recent_connections.retain(|c_old| c_old != c_id);
        z_settings.recent_connections.insert(0, c_id.to_owned());
        z_settings.recent_connections.truncate(20);
        write_json_atomic(&self.directory.join("settings.json"), &z_settings)?;
        z_state.settings = z_settings;
        Ok(())
    }

    // c_id 为 None 时清空全部最近连接, 否则只移除该条; 无变化不写盘.
    pub fn clear_recent(&self, c_id: Option<&str>) -> Result<()> {
        let _z_write = self.write_lock.lock().unwrap_or_else(PoisonError::into_inner);
        let mut z_state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let z_recent = &z_state.settings.recent_connections;
        if !c_id.map_or(!z_recent.is_empty(), |c_id| z_recent.iter().any(|c| c == c_id)) {
            return Ok(());
        }
        let mut z_settings = z_state.settings.clone();
        match c_id {
            Some(l_id) => z_settings.recent_connections.retain(|c| c != l_id),
            None => z_settings.recent_connections.clear(),
        }
        write_json_atomic(&self.directory.join("settings.json"), &z_settings)?;
        z_state.settings = z_settings;
        Ok(())
    }

    pub fn list(&self) -> Vec<Connection> {
        let mut z_connections = Vec::new();
        let Ok(z_entries) = fs::read_dir(&self.directory) else {
            return z_connections;
        };
        let mut z_ids = HashSet::new();
        for z_entry in z_entries.flatten() {
            let z_path = z_entry.path();
            if z_path.extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            let Some(z_stem) = z_path.file_stem().and_then(|s| s.to_str()) else {
                continue;
            };
            if let Some(z_id) = z_stem.strip_suffix("_connect_config").filter(|c| uuid::Uuid::parse_str(c).is_ok()) {
                z_ids.insert(z_id.to_owned());
            }
        }
        for z_id in z_ids {
            if let Ok(z_conn) = self.get(&z_id) {
                z_connections.push(z_conn);
            }
        }
        let z_order = self.settings().map(|c| c.connection_order).unwrap_or_default();
        let z_position = |c_id: &str| z_order.iter().position(|c| c == c_id).unwrap_or(usize::MAX);
        z_connections.sort_by(|a, b| a.group.cmp(&b.group).then(z_position(&a.id).cmp(&z_position(&b.id))).then(a.name.cmp(&b.name)));
        z_connections
    }

    pub fn get(&self, c_id: &str) -> Result<Connection> {
        let (_, z_value) = self.local_record(c_id)?.context("连接配置不存在")?;
        let z_conn = connection_from_value(&z_value).context("读取连接配置失败")?;
        ensure!(z_conn.id == c_id, "连接 ID 与文件名不一致");
        Ok(z_conn)
    }

    pub fn save(&self, c_conn: Connection, c_secret: Option<&str>) -> Result<Connection> {
        self.save_internal(c_conn, c_secret, None, None)
    }

    // 编辑连接保存: c_admin 为单独设置的管理员密码, None 保持原值.
    pub fn save_edited(&self, c_conn: Connection, c_secret: Option<&str>, c_admin: Option<&str>) -> Result<Connection> {
        self.save_internal(c_conn, c_secret, c_admin, None)
    }

    pub(super) fn save_imported(
        &self,
        c_conn: Connection,
        c_original: &serde_json::Value,
        c_secret: Option<&str>,
    ) -> Result<Connection> {
        self.save_internal(c_conn, c_secret, None, Some(c_original))
    }

    fn save_internal(
        &self,
        mut c_conn: Connection,
        c_secret: Option<&str>,
        c_admin: Option<&str>,
        c_imported: Option<&serde_json::Value>,
    ) -> Result<Connection> {
        if c_conn.id.is_empty() {
            c_conn.id = uuid::Uuid::new_v4().to_string();
        }
        if c_conn.group.trim().is_empty() {
            c_conn.group = "默认".into();
        }
        c_conn.validate()?;
        let z_path = native_path(&self.directory, &c_conn.id)?;
        let _z_write = self.write_lock.lock().unwrap_or_else(PoisonError::into_inner);
        let z_local_path = self.local_path(&c_conn.id)?;
        let (z_existing, z_original, z_expected) = if let Some(z_path) = z_local_path.as_ref() {
            let z_bytes = read_limited_bytes(z_path)?;
            let z_value: serde_json::Value = serde_json::from_slice(&z_bytes)?;
            let z_conn = connection_from_value(&z_value)?;
            ensure!(z_conn.id == c_conn.id, "连接 ID 与文件名不一致");
            let z_original = z_value.get("conection_type").is_some().then_some(z_value);
            (
                Some(z_conn),
                z_original,
                Some(format!("{:x}", md5::compute(z_bytes))),
            )
        } else {
            (None, None, None)
        };
        let z_auth_changed = z_existing.as_ref().is_some_and(|c| c.auth != c_conn.auth);
        if z_existing.as_ref().is_some_and(|c| {
            c.host != c_conn.host
                || c.port != c_conn.port
                || c.username != c_conn.username
                || c.auth != c_conn.auth
                || c.private_key_path != c_conn.private_key_path
        }) {
            c_conn.mcp_allowed = false;
        }
        self.adopt_private_key(&mut c_conn)?;
        // 密码按 FinalShell 兼容格式写入连接文件; 未提供新密码时保留原值, 认证方式改变则清空旧密码.
        let z_file_password: Option<&str> = if z_auth_changed && c_secret.is_none() { Some("") } else { c_secret };
        let z_record = c_imported.or(z_original.as_ref());
        if c_conn.admin_password == AdminPassword::Custom {
            ensure!(c_admin.map_or_else(|| z_record.is_some_and(|c| !stored_admin(c).is_empty()), |c| !c.is_empty()), "请输入管理员密码");
            ensure!(c_admin.is_none_or(|c| c.len() <= 1024 && !c.contains(['\r', '\n', '\0'])), "管理员密码过长或含换行");
        }
        crate::finalshell::write_native_record(
            &z_path,
            z_record,
            &c_conn,
            z_file_password,
            c_admin,
            z_expected.as_deref(),
            z_local_path.is_some(),
        )?;
        Ok(c_conn)
    }

    // 私钥连接保存时把所选私钥复制为数据目录下 keys/<连接ID>.key, 配置只记这个相对路径:
    // 原文件被删除或移走后仍可登录, 拷贝整个数据目录到别的电脑也能直接使用. 再次选择新私钥时覆盖副本.
    // 原文件已不存在但副本还在时沿用副本; 两者都没有时保持原路径, 连接时提示文件不存在.
    fn adopt_private_key(&self, c_conn: &mut Connection) -> Result<()> {
        if c_conn.auth != AuthMethod::PrivateKey {
            return Ok(());
        }
        let Some(z_source) = c_conn.private_key_path.clone().filter(|c| !c.is_empty()) else {
            return Ok(());
        };
        let z_managed = managed_key_path(&c_conn.id);
        if z_source == z_managed {
            return Ok(());
        }
        let z_target = self.directory.join(&z_managed);
        let z_source_path = self.key_location(&z_source);
        if z_source_path.is_file() {
            let z_key = Zeroizing::new(read_limited_bytes(&z_source_path).context("读取私钥文件失败, 私钥须为 1 MiB 以内的普通文件")?);
            let z_dir = self.directory.join("keys");
            fs::create_dir_all(&z_dir).context("创建私钥目录失败")?;
            set_directory_permissions(&z_dir)?;
            write_bytes_atomic(&z_target, &z_key).context("复制私钥到软件数据目录失败")?;
        } else if !z_target.is_file() {
            return Ok(());
        }
        c_conn.private_key_path = Some(z_managed);
        Ok(())
    }

    // 连接使用的私钥文件: 优先用数据目录中的副本; 仍指向原文件的旧连接在此补做复制,
    // 复制失败 (如磁盘只读) 不影响本次登录, 直接用原文件.
    pub fn private_key_file(&self, c_conn: &Connection) -> Result<PathBuf> {
        let z_path = c_conn
            .private_key_path
            .as_deref()
            .filter(|c| !c.is_empty())
            .context("未选择私钥文件, 请编辑连接选择本地私钥")?;
        let z_managed = managed_key_path(&c_conn.id);
        if z_path == z_managed {
            return Ok(self.directory.join(z_managed));
        }
        let mut z_adopted = false;
        if let Ok(l_saved) = self.get(&c_conn.id) {
            if l_saved.private_key_path.as_deref() == Some(z_path) {
                match self.save(l_saved, None) {
                    Ok(l_conn) => z_adopted = l_conn.private_key_path.as_deref() == Some(z_managed.as_str()),
                    Err(err) => eprintln!("私钥复制到软件数据目录失败: {err:#}"),
                }
            }
        }
        Ok(if z_adopted { self.directory.join(z_managed) } else { self.key_location(z_path) })
    }

    // 相对路径 (如导出再导入的连接带着 keys/旧ID.key) 按数据目录解析, 不受程序当前工作目录影响.
    fn key_location(&self, c_path: &str) -> PathBuf {
        if Path::new(c_path).is_relative() { self.directory.join(c_path) } else { PathBuf::from(c_path) }
    }

    // 读取连接文件中保存的密码 (私钥连接为私钥口令).
    pub fn secret(&self, c_id: &str) -> Result<Option<Zeroizing<String>>> {
        let z_conn = self.get(c_id)?;
        if z_conn.auth == AuthMethod::KeyboardInteractive {
            return Ok(None);
        }
        let z_path = self.local_path(c_id)?.context("连接配置不存在")?;
        crate::finalshell::native_secret(&z_path, 0)
    }

    // 读取单独设置的管理员密码, 未设置时返回 None.
    pub fn admin_secret(&self, c_id: &str) -> Result<Option<Zeroizing<String>>> {
        let (_, z_value) = self.local_record(c_id)?.context("连接配置不存在")?;
        let z_encoded = stored_admin(&z_value);
        if z_encoded.is_empty() {
            return Ok(None);
        }
        Ok(Some(Zeroizing::new(crate::finalshell::decrypt_password(z_encoded)?)))
    }

    pub fn remove(&self, c_id: &str) -> Result<()> {
        let _z_write = self.write_lock.lock().unwrap_or_else(PoisonError::into_inner);
        self.get(c_id)?;
        let z_path = native_path(&self.directory, c_id)?;
        fs::remove_file(&z_path).context("删除连接配置失败")?;
        self.file_cache.lock().unwrap_or_else(PoisonError::into_inner).remove(&z_path);
        let mut z_state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let mut z_settings = z_state.settings.clone();
        z_settings.recent_connections.retain(|c_old| c_old != c_id);
        write_json_atomic(&self.directory.join("settings.json"), &z_settings)?;
        z_state.settings = z_settings;
        Ok(())
    }

    pub fn import_finalshell(&self, c_path: &Path, c_group: Option<&str>) -> Result<(Vec<Connection>, Vec<String>)> {
        crate::finalshell::import(self, c_path, c_group)
    }

    pub fn export_finalshell(
        &self,
        c_id: &str,
        c_path: &Path,
        c_include_password: bool,
    ) -> Result<()> {
        crate::finalshell::export(self, c_id, c_path, c_include_password)
    }

    pub fn export_all_finalshell(&self, c_path: &Path, c_include_password: bool, c_group: Option<&str>) -> Result<usize> {
        crate::finalshell::export_all(self, c_path, c_include_password, c_group)
    }

    pub(super) fn native_record(&self, c_id: &str) -> Result<Option<serde_json::Value>> {
        let z_path = self.local_path(c_id)?.context("连接配置不存在")?;
        Ok(Some(read_json_limited(&z_path)?))
    }

    // 代理列表保存在 proxies.json; 返回给界面时不含密码, 只标记是否已设置.
    pub fn proxies(&self) -> Result<Vec<Proxy>> {
        let z_path = self.directory.join("proxies.json");
        if !z_path.exists() {
            return Ok(Vec::new());
        }
        read_json_limited(&z_path)
    }

    pub fn proxy_has_password(c_proxy: &Proxy) -> bool {
        !c_proxy.password.is_empty()
    }

    // 新建 (id 为空) 或修改代理; 密码 None 保持原值, 空字符串清除.
    pub fn save_proxy(&self, mut c_proxy: Proxy, c_password: Option<&str>) -> Result<Proxy> {
        c_proxy.name = c_proxy.name.trim().to_owned();
        c_proxy.host = c_proxy.host.trim().to_owned();
        ensure!(!c_proxy.name.is_empty() && c_proxy.name.len() <= 64 && !c_proxy.name.chars().any(char::is_control), "代理名称为空或过长");
        // "直连"等是界面上的固定选项, 不能用作代理名称, 以免列表与菜单中混淆.
        ensure!(!matches!(c_proxy.name.as_str(), "直连" | "代理已删除"), "代理名称不能是「{}」", c_proxy.name);
        ensure!(matches!(c_proxy.kind.as_str(), "socks5" | "http"), "代理类型只支持 SOCKS5 或 HTTP");
        ensure!(
            !c_proxy.host.is_empty() && c_proxy.host.len() <= 255 && !c_proxy.host.chars().any(|c| c.is_control() || c.is_whitespace()),
            "代理地址无效"
        );
        ensure!(c_proxy.port != 0, "代理端口不能为 0");
        ensure!(c_proxy.username.len() <= 255 && !c_proxy.username.chars().any(char::is_control), "代理用户名无效");
        let _z_write = self.write_lock.lock().unwrap_or_else(PoisonError::into_inner);
        let mut z_proxies = self.proxies()?;
        ensure!(!z_proxies.iter().any(|c| c.name == c_proxy.name && c.id != c_proxy.id), "代理名称重复");
        let z_existing = z_proxies.iter().position(|c| !c_proxy.id.is_empty() && c.id == c_proxy.id);
        c_proxy.password = match c_password {
            Some("") => String::new(),
            Some(l_password) => {
                ensure!(l_password.len() <= 255, "代理密码过长");
                crate::finalshell::encrypt_password(l_password)?
            }
            None => z_existing.map(|i| z_proxies[i].password.clone()).unwrap_or_default(),
        };
        match z_existing {
            Some(i) => z_proxies[i] = c_proxy.clone(),
            None => {
                ensure!(c_proxy.id.is_empty(), "代理不存在");
                ensure!(z_proxies.len() < MAX_PROXIES, "最多保存 {MAX_PROXIES} 个代理");
                c_proxy.id = uuid::Uuid::new_v4().to_string();
                z_proxies.push(c_proxy.clone());
            }
        }
        write_json_atomic(&self.directory.join("proxies.json"), &z_proxies)?;
        Ok(c_proxy)
    }

    // 删除代理, 使用它的连接改回直连; 返回改回直连的连接数.
    pub fn delete_proxy(&self, c_id: &str) -> Result<usize> {
        {
            let _z_write = self.write_lock.lock().unwrap_or_else(PoisonError::into_inner);
            let mut z_proxies = self.proxies()?;
            let z_count = z_proxies.len();
            z_proxies.retain(|c| c.id != c_id);
            ensure!(z_proxies.len() < z_count, "代理不存在");
            write_json_atomic(&self.directory.join("proxies.json"), &z_proxies)?;
        }
        let z_ids = self.list().into_iter().filter(|c| c.proxy_id.as_deref() == Some(c_id)).map(|c| c.id).collect::<Vec<_>>();
        self.set_connection_proxy(&z_ids, None)
    }

    // 为一批连接选择代理 (None 为直连), 返回实际修改的连接数.
    pub fn set_connection_proxy(&self, c_ids: &[String], c_proxy: Option<&str>) -> Result<usize> {
        if let Some(l_proxy) = c_proxy {
            ensure!(self.proxies()?.iter().any(|c| c.id == l_proxy), "代理不存在, 请刷新后重新选择");
        }
        let mut z_changed = 0;
        for z_id in c_ids {
            let mut l_conn = self.get(z_id)?;
            if l_conn.proxy_id.as_deref() != c_proxy {
                l_conn.proxy_id = c_proxy.map(str::to_owned);
                self.save(l_conn, None)?;
                z_changed += 1;
            }
        }
        Ok(z_changed)
    }

    // 建立连接时取代理及解密后的密码.
    pub fn proxy_with_secret(&self, c_id: &str) -> Result<(Proxy, Option<Zeroizing<String>>)> {
        let z_proxy = self.proxies()?.into_iter().find(|c| c.id == c_id).context("连接所选的代理已删除, 请重新选择代理")?;
        let z_secret = if z_proxy.password.is_empty() {
            None
        } else {
            Some(Zeroizing::new(crate::finalshell::decrypt_password(&z_proxy.password).context("代理密码解析失败")?))
        };
        Ok((z_proxy, z_secret))
    }

    pub fn known_host(&self, c_host: &str, c_port: u16) -> Result<Option<String>> {
        let z_state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(z_state.host_keys.0.get(&host_key(c_host, c_port)).cloned())
    }

    pub fn trust_host(&self, c_host: &str, c_port: u16, c_key: &str) -> Result<()> {
        ensure!(
            !c_host.is_empty() && c_port != 0 && !c_key.is_empty(),
            "主机密钥参数无效"
        );
        let _z_write_guard = self.write_lock.lock().unwrap_or_else(PoisonError::into_inner);
        let mut z_host_keys = self.state.lock().unwrap_or_else(PoisonError::into_inner)
            .host_keys
            .clone();
        z_host_keys
            .0
            .insert(host_key(c_host, c_port), c_key.to_owned());
        write_json_atomic(&self.directory.join("known_hosts.json"), &z_host_keys)?;
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
            .host_keys = z_host_keys;
        Ok(())
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    pub fn record_operation(
        &self,
        c_actor: Actor,
        c_method: &str,
        c_result: &serde_json::Value,
    ) -> Result<()> {
        ensure!(
            !c_method.is_empty()
                && c_method.len() <= 64
                && c_method
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_'),
            "操作记录方法名无效"
        );
        let _z_write_guard = self.write_lock.lock().unwrap_or_else(PoisonError::into_inner);

        // 接受直接结果或 {success, result} 包装，只从成功结果挑选白名单字段。
        let z_payload = c_result.get("result").unwrap_or(c_result);
        let z_success = c_result
            .get("success")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true);
        let mut z_record = serde_json::Map::new();
        z_record.insert(
            "timestamp_ms".into(),
            serde_json::Value::from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
            ),
        );
        z_record.insert(
            "actor".into(),
            serde_json::Value::String(
                match c_actor {
                    Actor::User => "user",
                    Actor::Mcp => "mcp",
                }
                .into(),
            ),
        );
        z_record.insert("method".into(), serde_json::Value::String(c_method.into()));
        z_record.insert("success".into(), serde_json::Value::Bool(z_success));

        if z_success {
            insert_operation_id(&mut z_record, "task_id", z_payload.get("task_id"));
            insert_operation_id(&mut z_record, "session_id", z_payload.get("session_id"));
            let z_connection_id = z_payload
                .get("connection")
                .and_then(|v| v.get("id"))
                .or_else(|| z_payload.get("connection_id"));
            insert_operation_id(&mut z_record, "connection_id", z_connection_id);
        }
        let mut z_line =
            serde_json::to_vec(&serde_json::Value::Object(z_record)).context("编码操作记录失败")?;
        z_line.push(b'\n');
        ensure!(z_line.len() <= 2048, "单条操作记录超过 2 KiB 限制");

        const MAX_LOG: u64 = 512 * 1024;
        let z_current = self.directory.join("operations.jsonl");
        let z_previous = self.directory.join("operations.previous.jsonl");
        let z_current_size = checked_log_size(&z_current)?;
        let z_previous_size = checked_log_size(&z_previous)?;
        ensure!(z_current_size <= MAX_LOG, "当前操作记录超过容量上限");
        ensure!(z_previous_size <= MAX_LOG, "旧操作记录超过容量上限");
        if z_current_size + z_line.len() as u64 > MAX_LOG {
            if z_current_size > 0 {
                fs::rename(&z_current, &z_previous).context("滚动操作记录失败")?;
            }
            // 新日志采用独占创建，避免链接到配置目录外的文件。
            let mut z_options = fs::OpenOptions::new();
            z_options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                z_options.mode(0o600);
            }
            let mut z_file = z_options.open(&z_current).context("创建操作记录失败")?;
            z_file.write_all(&z_line).context("写入操作记录失败")?;
            z_file.sync_data().context("同步操作记录失败")?;
        } else {
            let mut z_options = fs::OpenOptions::new();
            z_options.append(true).create(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                z_options.mode(0o600);
            }
            let mut z_file = z_options.open(&z_current).context("打开操作记录失败")?;
            set_file_permissions(&z_current)?;
            z_file.write_all(&z_line).context("写入操作记录失败")?;
            z_file.sync_data().context("同步操作记录失败")?;
        }
        Ok(())
    }
}

fn insert_operation_id(
    c_record: &mut serde_json::Map<String, serde_json::Value>,
    c_field: &str,
    c_value: Option<&serde_json::Value>,
) {
    if let Some(z_id) = c_value
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty() && s.len() <= 128 && s.bytes().all(|b| b.is_ascii_graphic()))
    {
        c_record.insert(c_field.into(), serde_json::Value::String(z_id.into()));
    }
}

fn normalize_recent(c_settings: &mut AppSettings) {
    let mut z_seen = HashSet::new();
    c_settings
        .recent_connections
        .retain(|c_id| uuid::Uuid::parse_str(c_id).is_ok() && z_seen.insert(c_id.clone()));
    c_settings.recent_connections.truncate(20);
}

fn validate_host_keys(c_hosts: &HostKeys) -> Result<()> {
    for z_key in c_hosts.0.keys() {
        ensure!(z_key.len() <= 512, "known_hosts 键无效");
    }
    Ok(())
}

fn checked_log_size(c_path: &Path) -> Result<u64> {
    match fs::symlink_metadata(c_path) {
        Ok(z_meta) => {
            ensure!(z_meta.file_type().is_file(), "操作记录路径不是普通文件");
            Ok(z_meta.len())
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(err) => Err(err).context("读取操作记录元数据失败"),
    }
}

#[cfg(unix)]
fn set_file_permissions(c_path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(c_path, fs::Permissions::from_mode(0o600)).context("设置操作记录权限失败")
}
#[cfg(not(unix))]
fn set_file_permissions(_: &Path) -> Result<()> {
    Ok(())
}

// 私钥副本相对数据目录的位置, 使用 / 分隔, 各系统通用.
fn managed_key_path(c_id: &str) -> String {
    format!("keys/{c_id}.key")
}

fn host_key(c_host: &str, c_port: u16) -> String {
    format!("[{c_host}]:{c_port}")
}

fn native_path(c_directory: &Path, c_id: &str) -> Result<PathBuf> {
    let z_uuid = uuid::Uuid::parse_str(c_id).context("连接 ID 必须是 UUID")?;
    ensure!(
        z_uuid.to_string() == c_id.to_ascii_lowercase(),
        "连接 ID 格式无效"
    );
    Ok(c_directory.join(format!("{c_id}_connect_config.json")))
}

fn read_json_limited<T: for<'de> Deserialize<'de>>(c_path: &Path) -> Result<T> {
    let z_data = read_limited_bytes(c_path)?;
    serde_json::from_slice(&z_data).context("解析配置 JSON 失败")
}

fn read_limited_bytes(c_path: &Path) -> Result<Vec<u8>> {
    let z_meta = fs::symlink_metadata(c_path).context("读取配置文件信息失败")?;
    ensure!(z_meta.file_type().is_file(), "配置路径必须是普通文件");
    ensure!(z_meta.len() <= MAX_FILE_SIZE, "配置文件超过 1 MiB 限制");
    let z_file = fs::File::open(c_path).context("打开配置文件失败")?;
    let mut z_data = Vec::new();
    z_file
        .take(MAX_FILE_SIZE + 1)
        .read_to_end(&mut z_data)
        .context("读取配置文件失败")?;
    ensure!(
        z_data.len() as u64 <= MAX_FILE_SIZE,
        "配置文件超过 1 MiB 限制"
    );
    Ok(z_data)
}

fn connection_from_value(c_value: &serde_json::Value) -> Result<Connection> {
    let z_id = c_value
        .get("hipshell")
        .and_then(|c| c.get("id"))
        .and_then(serde_json::Value::as_str)
        .context("连接文件缺少 HiPShell 元数据, 请重新导入")?;
    crate::finalshell::native_record_connection(c_value, z_id)
}

// 连接文件中已加密的管理员密码, 未设置时为空.
fn stored_admin(c_value: &serde_json::Value) -> &str {
    c_value.get("hipshell").and_then(|c| c.get("admin_password")).and_then(serde_json::Value::as_str).unwrap_or_default()
}

fn write_json_atomic<T: Serialize>(c_path: &Path, c_value: &T) -> Result<()> {
    let z_bytes = serde_json::to_vec(c_value).context("编码配置 JSON 失败")?;
    write_bytes_atomic(c_path, &z_bytes)
}

// 先写同目录临时文件 (仅本人可读) 再改名替换, 中途失败不会留下半个文件.
fn write_bytes_atomic(c_path: &Path, z_bytes: &[u8]) -> Result<()> {
    ensure!(
        z_bytes.len() as u64 <= MAX_FILE_SIZE,
        "配置文件超过 1 MiB 限制"
    );
    let z_temp = c_path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
    let mut z_options = fs::OpenOptions::new();
    z_options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        z_options.mode(0o600);
    }
    let mut z_file = z_options.open(&z_temp).context("创建临时配置文件失败")?;
    let z_result = (|| -> Result<()> {
        z_file.write_all(z_bytes).context("写入临时配置文件失败")?;
        z_file.sync_all().context("同步临时配置文件失败")?;
        drop(z_file);
        fs::rename(&z_temp, c_path).context("替换配置文件失败")
    })();
    if z_result.is_err() {
        let _ = fs::remove_file(&z_temp);
    }
    z_result
}

#[cfg(unix)]
fn set_directory_permissions(c_path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(c_path, fs::Permissions::from_mode(0o700)).context("设置配置目录权限失败")
}
#[cfg(not(unix))]
fn set_directory_permissions(_: &Path) -> Result<()> {
    Ok(())
}
