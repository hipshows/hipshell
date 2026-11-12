use crate::{
    config::ConfigStore,
    model::{Actor, AdminPassword, AuthMethod, Connection},
};
use anyhow::{Context, Result, bail, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use russh::{
    ChannelMsg, Disconnect, client,
    keys::{self, HashAlg, PrivateKeyWithHashAlg, PublicKeyOrCertificate},
};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, VecDeque},
    io::SeekFrom,
    path::{Path, PathBuf},
    // 锁中毒只在持锁线程 panic 后出现, 数据仍可用, 统一直接取回而不逐处转换为错误.
    sync::{Arc, Mutex, PoisonError, Weak, atomic::{AtomicBool, AtomicU64, Ordering}},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::{Semaphore, mpsc, oneshot},
};
use zeroize::Zeroizing;

const MAX_SESSIONS: usize = 8;
const MAX_TASKS: usize = 200;
// 一个文件夹传输任务最多包含的条目数 (文件与子目录合计), 扫描时超出即报错, 不做部分传输.
const MAX_DIRECTORY_ITEMS: usize = 200_000;
// 文件夹任务详情中保留的跳过或失败条目数.
const MAX_DIRECTORY_NOTES: usize = 200;
const MAX_TABS: usize = 32;
const MAX_INPUT: usize = 16 * 1024;
// 独立命令输出的默认保留量 (MCP 默认按此返回, 避免一次塞满上下文); 界面可要求更多, 上限 MAX_COMMAND_OUTPUT.
const MAX_OUTPUT: usize = 65_536;
const MAX_COMMAND_OUTPUT: usize = 1024 * 1024;
// 同一会话所有上传的在途写请求总数 (每个 32 KiB, 确认后即释放): 专用连接 256 个 (8 MiB);
// 退回主连接时另用 64 个 (2 MiB) 的总额度, 多个上传合计也不拖慢终端回显.
const UPLOAD_SESSION_INFLIGHT: usize = 256;
const UPLOAD_MAIN_INFLIGHT: usize = 64;
// 传输遇到网络错误时连续自动重连续传的次数上限 (有新进度即重新计数).
const TRANSFER_RETRIES: u32 = 10;
// 传输用完的 SFTP 会话最多留这么久、这么多个 (与同时传输数一致); 过期的不再复用, 服务器也可能已回收.
const TRANSFER_IDLE: Duration = Duration::from_secs(60);
const TRANSFER_IDLE_MAX: usize = 3;
// 已结束任务保留的输出合计上限; 超出后从最早的任务开始释放输出, 任务记录保留.
const TASK_OUTPUT_BUDGET: usize = 32 * 1024 * 1024;
// 所有终端历史合计的内存预算 (每个字符格约 32 字节); 新终端的历史行数在设置值和剩余预算间取小, 至少 1000 行.
const TERMINAL_HISTORY_BUDGET: usize = 96 * 1024 * 1024;
// 单个目录最多列出的条目数, 日志等大目录也能完整显示; 删除上限与之一致.
const MAX_LIST_ENTRIES: usize = 20_000;
const MAX_TERMINAL_LINES: u16 = 200;
const MAX_TERMINAL_COLS: u16 = 400;
// 历史行数上限 (设置可选 1000-10000), 实际行数按创建终端时的设置.
const MAX_SCROLLBACK: usize = 10_000;
const MAX_TERMINAL_DECODED_CHUNK: usize = 32 * 1024;
const MAX_LIST_NAME_BYTES: usize = 2 * 1024 * 1024;
// 一次删除的项数上限, 与单个目录列出的条目上限一致.
const MAX_DELETE_ITEMS: usize = MAX_LIST_ENTRIES;
const MAX_MONITOR_HISTORY: usize = 120;

pub struct Backend {
    store: Arc<ConfigStore>,
    commands: crate::commands::CommandStore,
    state: Mutex<State>,
    // 传输与独立命令分开限流, 两个大文件传输不会挡住命令执行.
    transfer_slots: Arc<Semaphore>,
    command_slots: Arc<Semaphore>,
    // 终端有新输出时唤醒界面刷新; 只保留一个许可, 连续输出自然合并.
    pub(crate) terminal_changed: tokio::sync::Notify,
    // MCP 服务的监听任务与状态 (网址, 或未运行的原因); 异步锁让启动、换端口、关闭依次进行.
    mcp_http: tokio::sync::Mutex<(Option<tokio::task::JoinHandle<()>>, std::result::Result<String, String>)>,
}

#[derive(Default)]
struct State {
    sessions: HashMap<String, Arc<LiveSession>>,
    tabs: Vec<ConnectionTab>,
    tasks: HashMap<String, Arc<Mutex<Task>>>,
    active_session_id: Option<String>,
    mcp_running: bool,
    // MCP 会话标识 -> (客户端名称, 最近请求时间); 超时未请求即视为离线.
    mcp_clients: HashMap<String, (String, Instant)>,
    // 最近的 MCP 操作: (方法, 客户端, 摘要, 是否成功, 时间), 新的在前.
    // MCP 最近操作: (方法, 客户端, 对象, 成功, 时间, 执行内容, 失败原因); 内容与原因各限 500 字.
    mcp_activity: VecDeque<(String, String, String, bool, u64, String, String)>,
}

#[derive(Clone)]
struct ConnectionTab {
    id: String,
    connection_id: Option<String>,
}

struct LiveSession {
    id: String,
    // 会话代次 (建立时的毫秒时间戳): 同一标签重连沿用 ID, 界面据此识别旧文件列表需要作废.
    link: u64,
    // 目录读取与文件操作按连接串行, 一台慢服务器不影响其他连接.
    file_slot: Arc<Semaphore>,
    // 目录浏览与文件操作复用的 SFTP 会话 (受 file_slot 串行保护), 省去每次开通道、启动和初始化的往返; 失效时重建.
    browse_sftp: Mutex<Option<Arc<russh_sftp::client::RawSftpSession>>>,
    // 最近一次目录读取尚未完成的属主、时间补全: (标识, 目录, 后台任务), 界面凭标识取结果并就地更新.
    file_details: Mutex<Option<(u64, String, tokio::task::JoinHandle<FileDetails>)>>,
    // 最近一次补全任务的取消句柄: 结果被取走等待时也能取消, 换目录、断开、等待超时都会停掉它.
    file_details_abort: Mutex<Option<tokio::task::AbortHandle>>,
    // 本终端历史按预算分配的字节数 (行数 x 列数 x 每格约 32 字节), 各终端合计用于新终端的额度计算.
    history_bytes: std::sync::atomic::AtomicUsize,
    // 传输专用 SSH 连接 (按需建立, 会话内复用), 以及上次建立失败的时间和原因; 失败后一段时间内直接用主连接.
    transfer: tokio::sync::Mutex<(Option<Arc<client::Handle<SshHandler>>>, Option<(Instant, String)>)>,
    // 主连接认证成功的密码或私钥, 只在内存中用于建立传输连接, 会话释放时清零.
    transfer_password: Mutex<Option<Zeroizing<String>>>,
    transfer_key: Mutex<Option<Arc<keys::PrivateKey>>>,
    // 独立连接上成功传完的 SFTP 会话, 下一个文件直接复用, 省去开通道、启动和初始化的往返. 上传与下载的会话类型不同, 分开存放.
    idle_upload: Mutex<Vec<IdleSftp<(Arc<russh_sftp::client::RawSftpSession>, russh_sftp::protocol::Version)>>>,
    idle_download: Mutex<Vec<IdleSftp<russh_sftp::client::SftpSession>>>,
    // 建立主连接时所用代理配置的快照 (含解密后的密码); 传输连接沿用, 代理修改或删除在新会话生效.
    proxy: Mutex<Option<Arc<ProxyRoute>>>,
    // 主连接已确认的主机指纹, 传输连接必须一致, 不再重复询问.
    host_fingerprint: Mutex<Option<String>>,
    // 最近一次断开原因: [主连接, 传输连接]; 断线后显示在状态栏和任务错误里, 区分保活超时、服务器断开和网络错误.
    link_errors: Mutex<[Option<String>; 2]>,
    // 同一会话所有上传共享的在途写请求上限 (每个 32 KiB), 多个上传各自调节时总量也有界.
    upload_inflight: Arc<Semaphore>,
    upload_main_inflight: Arc<Semaphore>,
    connection: Connection,
    actor: Actor,
    status: Mutex<String>,
    handle: Mutex<Option<Arc<client::Handle<SshHandler>>>>,
    terminal_tx: Mutex<Option<mpsc::Sender<TerminalInput>>>,
    // 终端通道代次: 每次打开终端换新值 (在 terminal_tx 锁内更新), 界面带上它写入, 旧终端的待发输入不会进入新终端.
    terminal_gen: AtomicU64,
    // 只保留最新尺寸: 发送端忙于大段输入时连续调整窗口, 中间尺寸可以丢弃, 最后一次不能丢.
    resize_tx: Mutex<Option<tokio::sync::watch::Sender<(u16, u16)>>>,
    terminal: Mutex<Terminal>,
    // 终端通道已打开或正在打开; 终端退出后 SSH 连接保留, 文件传输和独立命令照常使用.
    shell: AtomicBool,
    shell_stop: Mutex<Option<oneshot::Sender<()>>>,
    monitoring: AtomicBool,
    trust: Mutex<Option<TrustPrompt>>,
    auth_prompts: Mutex<Vec<AuthPrompt>>,
    auth_answers: Mutex<Option<oneshot::Sender<Vec<String>>>>,
    connect_cancel: Mutex<Option<oneshot::Sender<()>>>,
    lease: Mutex<Option<Lease>>,
    last_user_input: Mutex<Option<Instant>>,
    // 终端输入行状态: (用户输入未回车的字符数, 正处于括号粘贴中, 上一块末尾未完的控制序列).
    // 大段粘贴按块写入, 状态要跨块延续; 重新打开终端时清空. 字符数大于 0 时提示 MCP 不要直接接着输入 (按键推测, 仅供参考).
    input_line: Mutex<(usize, bool, String)>,
    // MCP 上次代填管理员密码的时间, 用于拒绝短时间内重复代填.
    admin_input: Mutex<Option<Instant>>,
    monitor: Mutex<Option<Monitor>>,
    identity_cache: Mutex<HashMap<(char, u32), (Option<String>, Instant)>>,
    file_time_cache: tokio::sync::Mutex<Option<ServerFileTimes>>,
}

// 代理配置与解密后的代理密码.
type ProxyRoute = (crate::config::Proxy, Option<Zeroizing<String>>);
// (所在连接, 放回时间, 会话).
type IdleSftp<T> = (Arc<client::Handle<SshHandler>>, Instant, T);

// 补全后的目录条目, 以及服务器时区与时间提示.
type FileDetails = (Vec<Value>, (Option<String>, Option<String>));

struct ServerFileTimes {
    timezone: Option<String>,
    formatted: HashMap<u64, String>,
    hint: Option<String>,
    expires_at: Instant,
}

struct SshHandler {
    session: Weak<LiveSession>,
    store: Arc<ConfigStore>,
    // 传输专用连接: 只接受主连接已确认的主机指纹.
    transfer: bool,
}
struct TerminalInput {
    data: Vec<u8>,
    actor: Actor,
    lease_id: Option<String>,
}
struct TrustPrompt {
    fingerprint: String,
    changed: bool,
    answer: oneshot::Sender<bool>,
}
#[derive(Clone, Serialize)]
struct AuthPrompt {
    prompt: String,
    echo: bool,
}
struct Lease {
    id: String,
    until: Instant,
}
#[derive(Clone, Serialize)]
struct Monitor {
    cpu_percent: f64,
    memory_used_bytes: u64,
    memory_total_bytes: u64,
    swap_used_bytes: u64,
    swap_total_bytes: u64,
    memory_cache_bytes: u64,
    rx_bytes_per_sec: u64,
    tx_bytes_per_sec: u64,
    processes: Vec<ProcessMetric>,
    processes_truncated: bool,
    interfaces: Vec<InterfaceMetric>,
    interfaces_truncated: bool,
    error: Option<String>,
    history: MonitorHistory,
}
#[derive(Clone)]
struct ProcSample {
    cpu_total: u64,
    cpu_idle: u64,
    memory_total: u64,
    memory_used: u64,
    swap_total: u64,
    swap_used: u64,
    memory_cache: u64,
    interfaces: Vec<InterfaceSample>,
    interfaces_truncated: bool,
    processes: Vec<RawProcess>,
    processes_truncated: bool,
    clock_ticks: u64,
    cpu_count: u32,
    // 本次是否含进程列表 (每 3 次采样一次) 与服务器运行秒数.
    has_processes: bool,
    uptime: Option<f64>,
    sampled_at: Instant,
}
#[derive(Clone)]
struct RawProcess {
    pid: u32,
    name: String,
    ticks: u64,
    start_time: u64,
    rss_bytes: u64,
}
#[derive(Clone)]
struct InterfaceSample {
    name: String,
    rx_bytes: u64,
    tx_bytes: u64,
}
#[derive(Clone, Serialize)]
struct ProcessMetric {
    pid: u32,
    name: String,
    cpu_percent: f64,
    memory_bytes: u64,
}
#[derive(Clone, Serialize)]
struct InterfaceMetric {
    name: String,
    rx_bytes: u64,
    tx_bytes: u64,
    rx_bytes_per_sec: u64,
    tx_bytes_per_sec: u64,
    history: InterfaceHistory,
}
#[derive(Clone, Default, Serialize)]
struct InterfaceHistory {
    rx_bytes_per_sec: VecDeque<u64>,
    tx_bytes_per_sec: VecDeque<u64>,
}
#[derive(Clone, Default, Serialize)]
struct MonitorHistory {
    cpu_percent: VecDeque<f64>,
    rx_bytes_per_sec: VecDeque<u64>,
    tx_bytes_per_sec: VecDeque<u64>,
}
struct Terminal {
    parser: vt100::Parser,
    // 本终端保留的历史行数, 重新打开终端时沿用.
    scrollback: usize,
    seq: u64,
    filter: TerminalFilter,
    decoded: Vec<u8>,
    csi: Vec<u8>,
    modified_keys: u8,
    // Kitty 键盘协议的模式栈, 只兑现第 1 位 (消除歧义), 其余位不记录.
    kitty_keys: Vec<u8>,
    // 0 方块, 1 下划线, 2 竖线; 来自 DECSCUSR.
    cursor_shape: u8,
    // 同步输出 (DEC 2026) 开始时间: 期间不推进 seq, 界面只显示完整的一帧.
    sync_since: Option<Instant>,
    osc: Vec<u8>,
    clipboard: Option<String>,
}
#[derive(Clone, Copy, Default)]
enum TerminalFilter {
    #[default]
    Ground,
    Escape,
    Csi,
    Osc,
    OscEscape,
}
impl Terminal {
    // OSC 52 只允许远端写入本机剪贴板 (vim、tmux 的复制), 不会把本机剪贴板内容返回给远端.
    fn process(&mut self, c_data: &[u8]) -> Vec<u8> {
        let mut z_decoded = std::mem::take(&mut self.decoded);
        let mut z_replies = Vec::new();
        for &byte in c_data {
            match self.filter {
                TerminalFilter::Ground if byte == 0x1b => {
                    self.filter = TerminalFilter::Escape;
                }
                TerminalFilter::Ground => self.emit(&mut z_decoded, byte),
                TerminalFilter::Escape if byte == b']' => {
                    self.osc.clear();
                    self.filter = TerminalFilter::Osc;
                }
                TerminalFilter::Escape if byte == b'[' => {
                    self.emit(&mut z_decoded, 0x1b);
                    self.emit(&mut z_decoded, byte);
                    self.csi.clear();
                    self.filter = TerminalFilter::Csi;
                }
                TerminalFilter::Escape => {
                    self.emit(&mut z_decoded, 0x1b);
                    if byte == 0x1b {
                        self.filter = TerminalFilter::Escape;
                    } else {
                        self.emit(&mut z_decoded, byte);
                        if byte == b'c' { self.reset_modes(); }
                        self.filter = TerminalFilter::Ground;
                    }
                }
                TerminalFilter::Csi if byte == 0x1b => {
                    self.csi.clear();
                    self.filter = TerminalFilter::Escape;
                }
                TerminalFilter::Csi => {
                    self.emit(&mut z_decoded, byte);
                    if self.csi.len() < 64 { self.csi.push(byte); }
                    if (0x40..=0x7e).contains(&byte) {
                        // 仅实现可兑现的 xterm 扩展键、Kitty 键盘协议第 1 位与查询回复.
                        let z_reply = match self.csi.as_slice() {
                            b">4;1m" => { self.modified_keys = 1; None }
                            b">4;2m" => { self.modified_keys = 2; None }
                            // Vim退出使用空参数, 与显式0一样复位扩展键.
                            b">4;0m" | b">4;m" | b">4m" | b">4n" => { self.modified_keys = 0; None }
                            b"?4m" => Some(format!("\u{1b}[>4;{}m", self.modified_keys)),
                            b"?2026h" => { self.sync_since.get_or_insert_with(Instant::now); None }
                            b"?2026l" => { self.sync_since = None; None }
                            b"?2026$p" => Some(format!("\u{1b}[?2026;{}$y", if self.sync_since.is_some() { 1 } else { 2 })),
                            b"?u" => Some(format!("\u{1b}[?{}u", self.kitty_keys.last().copied().unwrap_or(0))),
                            [b'>', z_flags @ .., b'u'] => {
                                if self.kitty_keys.len() >= 16 { self.kitty_keys.remove(0); }
                                self.kitty_keys.push(csi_number(z_flags, 0) as u8 & 1);
                                None
                            }
                            [b'<', z_count @ .., b'u'] => {
                                let l_count = csi_number(z_count, 1).max(1) as usize;
                                self.kitty_keys.truncate(self.kitty_keys.len().saturating_sub(l_count));
                                None
                            }
                            [b'=', z_params @ .., b'u'] => {
                                let mut l_params = z_params.split(|c| *c == b';');
                                let l_flags = csi_number(l_params.next().unwrap_or_default(), 0) as u8 & 1;
                                let l_current = self.kitty_keys.last().copied().unwrap_or(0);
                                let l_next = match csi_number(l_params.next().unwrap_or_default(), 1) {
                                    2 => l_current | l_flags,
                                    3 => l_current & !l_flags,
                                    _ => l_flags,
                                };
                                match self.kitty_keys.last_mut() { Some(c) => *c = l_next, None => self.kitty_keys.push(l_next) }
                                None
                            }
                            [z_shape @ .., b' ', b'q'] => {
                                self.cursor_shape = match csi_number(z_shape, 0) { 3 | 4 => 1, 5 | 6 => 2, _ => 0 };
                                None
                            }
                            b"c" | b"0c" => Some("\u{1b}[?1;2c".to_owned()),
                            b"5n" => Some("\u{1b}[0n".to_owned()),
                            b"6n" => {
                                self.parser.process(&z_decoded);
                                z_decoded.clear();
                                self.seq = self.seq.wrapping_add(1);
                                let (z_row, z_col) = self.parser.screen().cursor_position();
                                Some(format!("\u{1b}[{};{}R", z_row + 1, z_col + 1))
                            }
                            _ => None,
                        };
                        if let Some(z_reply) = z_reply {
                            if z_replies.len() + z_reply.len() <= 256 { z_replies.extend_from_slice(z_reply.as_bytes()); }
                        }
                        self.csi.clear();
                        self.filter = TerminalFilter::Ground;
                    } else if matches!(byte, 0x18 | 0x1a) {
                        self.csi.clear();
                        self.filter = TerminalFilter::Ground;
                    }
                }
                TerminalFilter::Osc if byte == 0x07 => {
                    self.finish_osc();
                    self.filter = TerminalFilter::Ground;
                }
                TerminalFilter::Osc if matches!(byte, 0x18 | 0x1a) => {
                    self.osc.clear();
                    self.filter = TerminalFilter::Ground;
                }
                TerminalFilter::Osc if byte == 0x1b => {
                    self.filter = TerminalFilter::OscEscape;
                }
                TerminalFilter::Osc => {
                    // 只保留 OSC 52 写剪贴板请求, 其他 OSC 不累积内容.
                    if self.osc.len() < 3 || (self.osc.starts_with(b"52;") && self.osc.len() <= MAX_OUTPUT.div_ceil(3) * 4 + 32) {
                        self.osc.push(byte);
                    }
                }
                TerminalFilter::OscEscape if byte == b'\\' => {
                    self.finish_osc();
                    self.filter = TerminalFilter::Ground;
                }
                TerminalFilter::OscEscape if byte == 0x07 => {
                    self.finish_osc();
                    self.filter = TerminalFilter::Ground;
                }
                TerminalFilter::OscEscape if matches!(byte, 0x18 | 0x1a) => {
                    self.osc.clear();
                    self.filter = TerminalFilter::Ground;
                }
                TerminalFilter::OscEscape if byte == b']' => {
                    self.osc.clear();
                    self.filter = TerminalFilter::Osc;
                }
                TerminalFilter::OscEscape => {
                    // 新的 ESC 控制序列也会结束异常的 OSC, 保持终端可恢复.
                    self.emit(&mut z_decoded, 0x1b);
                    if byte == 0x1b {
                        self.filter = TerminalFilter::Escape;
                    } else {
                        self.emit(&mut z_decoded, byte);
                        self.filter = TerminalFilter::Ground;
                    }
                }
            }
        }
        if !z_decoded.is_empty() {
            self.parser.process(&z_decoded);
            self.bump();
        }
        z_decoded.clear();
        self.decoded = z_decoded;
        z_replies
    }

    fn finish_osc(&mut self) {
        if self.osc.len() <= MAX_OUTPUT.div_ceil(3) * 4 + 32 {
            if let Some(z_payload) = self.osc.strip_prefix(b"52;") {
                if let Some(z_split) = z_payload.iter().position(|c| *c == b';') {
                    let (z_selection, z_data) = z_payload.split_at(z_split);
                    // '?' 查询不返回任何内容, 远端始终不能读取本机剪贴板.
                    if z_selection.is_empty() || z_selection == b"c" {
                        if let Ok(z_data) = STANDARD.decode(&z_data[1..]) {
                            if z_data.len() <= MAX_OUTPUT {
                                if let Ok(z_text) = String::from_utf8(z_data) { self.clipboard = Some(z_text); }
                            }
                        }
                    }
                }
            }
        }
        self.osc.clear();
    }

    fn emit(&mut self, c_decoded: &mut Vec<u8>, c_byte: u8) {
        c_decoded.push(c_byte);
        if c_decoded.len() == MAX_TERMINAL_DECODED_CHUNK {
            self.parser.process(c_decoded);
            self.bump();
            c_decoded.clear();
        }
    }

    // 同步输出期间画面只到一半, 暂不通知界面; 程序异常未结束同步时 150ms 后照常刷新.
    fn bump(&mut self) {
        if self.sync_since.is_none_or(|c| c.elapsed() >= Duration::from_millis(150)) {
            self.seq = self.seq.wrapping_add(1);
        }
    }

    // 低 2 位为 xterm modifyOtherKeys 模式, 第 3 位表示 Kitty 键盘协议已开启.
    fn key_mode(&self) -> u8 {
        self.modified_keys | if self.kitty_keys.last().is_some_and(|c| *c & 1 != 0) { 4 } else { 0 }
    }

    // 画面未变或只取事件 (翻看历史、拖选) 时的结果: 仍带上按键编码所需的输入模式, 回到底部后的第一次输入按最新模式编码.
    fn unchanged(&self, c_clipboard: Option<String>) -> Value {
        let z_screen = self.parser.screen();
        json!({"unchanged":true,"seq":self.seq,"clipboard":c_clipboard,"bracketed_paste":z_screen.bracketed_paste(),"application_cursor":z_screen.application_cursor(),"modified_keys":self.key_mode(),"mouse":mouse_mode(z_screen)})
    }

    // 换成同尺寸的空白终端 (清空历史与模式), 用于终端关闭和重新打开.
    fn reset_screen(&mut self) {
        let (z_rows, z_cols) = self.parser.screen().size();
        self.parser = vt100::Parser::new(z_rows, z_cols, self.scrollback);
        self.filter = TerminalFilter::default();
        self.reset_modes();
        self.seq = self.seq.wrapping_add(1);
    }

    // RIS (ESC c) 复位终端时一并复位本层记录的键盘、光标和同步状态.
    fn reset_modes(&mut self) {
        self.modified_keys = 0;
        self.kitty_keys.clear();
        self.cursor_shape = 0;
        self.sync_since = None;
    }
}
struct Task {
    task_id: String,
    session_id: String,
    // 建立任务时会话的连接快照, 授权检查同时核对登录身份.
    connection: Connection,
    created_at_ms: u64,
    created_at: Instant,
    source: String,
    // MCP 任务的发起客户端名称, 用户任务为空.
    client: String,
    kind: String,
    label: String,
    // 传输任务的本机路径, 命令为空.
    local: String,
    started_at_ms: Option<u64>,
    finished_at_ms: Option<u64>,
    status: String,
    bytes: u64,
    total: Option<u64>,
    speed_bytes_per_sec: u64,
    last_progress_bytes: u64,
    last_progress_at: Instant,
    // 文件夹任务: bytes 与 total 为整个文件夹, 下面几项为文件计数和当前文件; base_bytes 是已结束文件的字节数.
    directory: bool,
    files_done: u64,
    files_total: u64,
    current: String,
    file_bytes: u64,
    file_total: u64,
    base_bytes: u64,
    stdout: String,
    stderr: String,
    truncated: bool,
    exit_code: Option<i32>,
    error: Option<String>,
    cancel: Option<oneshot::Sender<()>>,
}

impl Backend {
    pub fn new(c_store: Arc<ConfigStore>) -> Arc<Self> {
        Arc::new(Self {
            commands: crate::commands::CommandStore::new(c_store.directory().join("commands.json")),
            store: c_store,
            state: Mutex::new(State::default()),
            transfer_slots: Arc::new(Semaphore::new(3)),
            command_slots: Arc::new(Semaphore::new(4)),
            terminal_changed: tokio::sync::Notify::new(),
            mcp_http: tokio::sync::Mutex::new((None, Err("MCP 服务未启动".into()))),
        })
    }

    pub async fn call(
        self: &Arc<Self>,
        c_method: &str,
        c_args: Value,
        c_actor: Actor,
    ) -> Result<Value> {
        if c_method == "mcp_heartbeat" {
            // 心跳只刷新客户端在线状态, 不计入操作记录.
            return self.mcp_heartbeat(&c_args, c_actor);
        }
        if c_method == "warm_transfer" {
            // 只是提前建连接, 不计入操作记录.
            return self.warm_transfer(&c_args, c_actor);
        }
        // _client 由 MCP 服务按会话注入, 用于区分同时在线的多个 AI 客户端.
        let z_activity = (c_actor == Actor::Mcp).then(|| {
            (string_arg(&c_args, "_client"), self.mcp_activity_detail(c_method, &c_args))
        });
        let z_result = match c_method {
            "state" => self.state_json(c_actor),
            "get_settings" => {
                require_user(c_actor)?;
                Ok(serde_json::to_value(self.store.settings()?)?)
            }
            "set_layout" => {
                require_user(c_actor)?;
                self.store.set_layout(serde_json::from_value(c_args.get("layout").cloned().context("缺少 layout")?)?)?;
                Ok(json!({"saved":true}))
            }
            "set_expanded_groups" => {
                require_user(c_actor)?;
                self.store.set_expanded_groups(serde_json::from_value(c_args.get("groups").cloned().unwrap_or_default()).context("groups 必须是字符串数组")?)?;
                Ok(json!({"saved":true}))
            }
            "clear_recent_connections" => {
                require_user(c_actor)?;
                let z_id = c_args.get("connection_id").and_then(Value::as_str);
                self.store.clear_recent(z_id)?;
                Ok(json!({"cleared":true,"connection_id":z_id}))
            }
            // 拖动标签排序: 把标签移到 before 标签之前, before 为空或已关闭则放到最后.
            "move_tab" => {
                require_user(c_actor)?;
                let z_id = required_string(&c_args, "session_id")?;
                let z_before = c_args.get("before").and_then(Value::as_str).unwrap_or("");
                let mut z_state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
                let z_from = z_state.tabs.iter().position(|c| c.id == z_id).context("标签已关闭")?;
                let z_tab = z_state.tabs.remove(z_from);
                let z_at = z_state.tabs.iter().position(|c| c.id == z_before).unwrap_or(z_state.tabs.len());
                z_state.tabs.insert(z_at, z_tab);
                Ok(json!({"moved":true}))
            }
            "set_theme" => {
                require_user(c_actor)?;
                let z_dark = c_args
                    .get("dark_theme")
                    .and_then(Value::as_bool)
                    .context("主题设置需要布尔值")?;
                self.store.set_dark_theme(z_dark)?;
                Ok(json!({"dark_theme":z_dark}))
            }
            "set_tab_show_group" => {
                require_user(c_actor)?;
                let z_show = c_args.get("show").and_then(Value::as_bool).context("标签分组设置需要布尔值")?;
                self.store.set_tab_show_group(z_show)?;
                Ok(json!({"show":z_show}))
            }
            "set_connection_columns" => {
                require_user(c_actor)?;
                self.store.set_connection_columns(serde_json::from_value(c_args.get("columns").cloned().unwrap_or_default()).context("columns 必须是整数数组")?)?;
                Ok(json!({"saved":true}))
            }
            "set_download_dir" => {
                require_user(c_actor)?;
                let z_path = c_args.get("path").and_then(Value::as_str).context("缺少 path")?;
                self.store.set_download_dir(z_path)?;
                Ok(json!({"path":z_path}))
            }
            // 拖动排序: 连接分组、连接 (可跨分组)、命令分组与命令.
            "reorder_connection_groups" => {
                require_user(c_actor)?;
                self.store.reorder_connection_groups(serde_json::from_value(c_args.get("groups").cloned().unwrap_or_default()).context("groups 必须是字符串数组")?)?;
                Ok(json!({"saved":true}))
            }
            "move_connection" => {
                require_user(c_actor)?;
                let z_order = serde_json::from_value(c_args.get("order").cloned().unwrap_or_default()).context("order 必须是字符串数组")?;
                self.store.move_connection(required_string(&c_args, "connection_id")?, required_string(&c_args, "group")?, z_order)?;
                Ok(json!({"saved":true}))
            }
            "reorder_commands" => {
                require_user(c_actor)?;
                let z_list = |c_key: &str| c_args.get(c_key).map(|c| serde_json::from_value::<Vec<String>>(c.clone())).transpose();
                self.commands.reorder(z_list("groups").context("groups 必须是字符串数组")?, z_list("order").context("order 必须是字符串数组")?)?;
                Ok(json!({"saved":true}))
            }
            "save_connection_group" | "delete_connection_group" => {
                require_user(c_actor)?;
                let z_name = required_string(&c_args, "name")?;
                if c_method == "delete_connection_group" {
                    self.store.delete_connection_group(z_name)?;
                } else {
                    self.store.save_connection_group(c_args.get("old_name").and_then(Value::as_str).unwrap_or(""), z_name)?;
                }
                Ok(json!({"name":z_name}))
            }
            "set_terminal_scrollback" => {
                require_user(c_actor)?;
                let z_lines = c_args.get("lines").and_then(Value::as_u64).context("历史行数需要整数")?;
                self.store.set_terminal_scrollback(u32::try_from(z_lines).context("历史行数无效")?)?;
                Ok(json!({"lines":z_lines}))
            }
            "set_terminal_font" => {
                require_user(c_actor)?;
                let z_family = c_args.get("family").and_then(Value::as_str).unwrap_or("").trim();
                ensure!(
                    z_family.chars().count() <= 64 && !z_family.chars().any(char::is_control),
                    "字体名称无效"
                );
                let z_size = c_args.get("size").and_then(Value::as_u64).context("字号需要整数")?;
                ensure!((9..=28).contains(&z_size), "字号范围为 9-28");
                self.store.set_terminal_font(z_family, z_size as u32)?;
                Ok(json!({"family":z_family,"size":z_size}))
            }
            "save_connection" => self.save_connection(c_args, c_actor),
            // 编辑连接时回填已保存的登录密码与单独的 sudo 密码, 供界面按需明文查看; 仅限用户界面, MCP 不可调用.
            "connection_secrets" => {
                require_user(c_actor)?;
                let z_id = string_arg(&c_args, "id");
                let z_password = self.store.secret(&z_id)?;
                let z_admin = self.store.admin_secret(&z_id)?;
                Ok(json!({"password":z_password.as_deref().map_or("", |c| c.as_str()),"admin_password":z_admin.as_deref().map_or("", |c| c.as_str())}))
            }
            "save_proxy" => {
                require_user(c_actor)?;
                let z_proxy = serde_json::from_value(c_args.get("proxy").cloned().context("缺少 proxy")?)?;
                let z_proxy = self.store.save_proxy(z_proxy, c_args.get("password").and_then(Value::as_str))?;
                Ok(json!({"proxy_id":z_proxy.id,"name":z_proxy.name}))
            }
            "delete_proxy" => {
                require_user(c_actor)?;
                Ok(json!({"direct":self.store.delete_proxy(required_string(&c_args, "proxy_id")?)?}))
            }
            // 为一台或一组服务器选择代理, proxy_id 为空表示直连.
            "set_connection_proxy" => {
                require_user(c_actor)?;
                let z_ids: Vec<String> = match c_args.get("group").and_then(Value::as_str) {
                    Some(l_group) => self.store.list().into_iter().filter(|c| c.group == l_group).map(|c| c.id).collect(),
                    None => serde_json::from_value(c_args.get("connection_ids").cloned().context("缺少 connection_ids")?)?,
                };
                let z_proxy = c_args.get("proxy_id").and_then(Value::as_str).filter(|c| !c.is_empty());
                Ok(json!({"changed":self.store.set_connection_proxy(&z_ids, z_proxy)?,"count":z_ids.len()}))
            }
            "save_saved_command" => self.save_saved_command(c_args, c_actor),
            "delete_saved_command" => self.delete_saved_command(c_args, c_actor),
            "save_command_group" => self.save_command_group(c_args, c_actor),
            "delete_command_group" => self.delete_command_group(c_args, c_actor),
            "send_saved_command" => self.send_saved_command(c_args, c_actor).await,
            "import_commands" => {
                require_user(c_actor)?;
                let (z_added, z_skipped) = self.commands.import(Path::new(required_string(&c_args, "path")?))?;
                Ok(json!({"added":z_added,"skipped":z_skipped}))
            }
            "export_commands" => {
                require_user(c_actor)?;
                Ok(json!({"count":self.commands.export(Path::new(required_string(&c_args, "path")?))?}))
            }
            "send_command" => self.send_command(c_args, c_actor).await,
            "list_local_files" => self.list_local_files(c_args).await,
            "import_finalshell" => self.import_finalshell(c_args, c_actor),
            "export_finalshell" => self.export_finalshell(c_args, c_actor),
            "remove_connection" => self.remove_connection(c_args, c_actor),
            "new_tab" => self.new_tab(c_actor),
            "connect" => self.connect(c_args, c_actor),
            "reconnect" => self.reconnect(c_args, c_actor).await,
            "open_terminal" => self.open_terminal(c_args, c_actor),
            "close_terminal" => {
                let z_session = self.session(required_string(&c_args, "session_id")?, c_actor)?;
                ensure!(z_session.shell.load(Ordering::Acquire), "终端未打开");
                // 释放输入通道后写入任务关闭终端通道; 读取立即结束, 按"连接仍在"处理.
                z_session.terminal_tx.lock().unwrap_or_else(PoisonError::into_inner).take();
                z_session.resize_tx.lock().unwrap_or_else(PoisonError::into_inner).take();
                if let Some(z_stop) = z_session.shell_stop.lock().unwrap_or_else(PoisonError::into_inner).take() {
                    let _ = z_stop.send(());
                }
                Ok(json!({"closed": true}))
            }
            "focus_session" => self.focus_session(c_args, c_actor),
            "trust_host" => self.trust_host(c_args, c_actor),
            "answer_auth" => self.answer_auth(c_args, c_actor),
            "read_terminal" => self.read_terminal(c_args, c_actor),
            "resize_terminal" => self.resize_terminal(c_args, c_actor),
            "write_terminal" => self.write_terminal(c_args, c_actor).await,
            "input_admin_password" => self.input_admin_password(c_args, c_actor).await,
            "disconnect" => self.disconnect(c_args, c_actor).await,
            "run_command" => self.run_command(c_args, c_actor).await,
            "get_task" => self.get_task(c_args, c_actor),
            "list_files" | "check_remote_names" | "create_directory" | "create_file" | "set_permissions" | "rename_file" | "delete_files" => {
                match self.file_call(c_method, c_args.clone(), c_actor).await {
                    // 复用的 SFTP 会话失效 (网络抖动、服务器端 sftp-server 退出): 丢弃后重建. 读目录直接重试一次;
                    // 修改类操作只在请求尚未发出 (会话已关闭) 时重试, 批量删除不重试, 避免部分完成后重复执行.
                    Err(err) if transient_sftp_error(&err) => {
                        if let Ok(l_session) = self.session(required_string(&c_args, "session_id")?, c_actor) {
                            l_session.browse_sftp.lock().unwrap_or_else(PoisonError::into_inner).take();
                        }
                        if matches!(c_method, "list_files" | "check_remote_names") || (c_method != "delete_files" && format!("{err:#}").contains("session closed")) {
                            self.file_call(c_method, c_args, c_actor).await
                        } else {
                            Err(err)
                        }
                    }
                    // 读目录的其他错误 (连接被重置、通道意外结束等) 也丢弃复用的 SFTP 会话:
                    // 坏掉的会话不一定报超时, 留着会让之后每次刷新都在它上面失败.
                    Err(err) if matches!(c_method, "list_files" | "check_remote_names") => {
                        if let Ok(l_session) = self.session(required_string(&c_args, "session_id")?, c_actor) {
                            l_session.browse_sftp.lock().unwrap_or_else(PoisonError::into_inner).take();
                        }
                        Err(err)
                    }
                    l_result => l_result,
                }
            }
            "file_details" => self.file_details(c_args, c_actor).await,
            "delete_files_fast" => self.delete_files_fast(c_args, c_actor).await,
            "copy_file" => self.copy_file(c_args, c_actor).await,
            "upload_file" | "download_file" | "upload_directory" | "download_directory" => self.transfer(c_method, c_args, c_actor),
            "cancel_task" => self.cancel_task(c_args, c_actor),
            _ => bail!("未知操作: {c_method}"),
        };
        if matches!(
            c_method,
            "save_connection"
                | "import_finalshell"
                | "save_saved_command"
                | "import_commands"
                | "save_proxy"
                | "delete_proxy"
                | "set_connection_proxy"
                | "delete_saved_command"
                | "save_command_group"
                | "delete_command_group"
                | "save_connection_group"
                | "delete_connection_group"
                | "send_saved_command"
                | "send_command"
                | "export_finalshell"
                | "remove_connection"
                | "connect"
                | "reconnect"
                | "disconnect"
                | "trust_host"
                | "answer_auth"
                | "run_command"
                | "create_directory"
                | "create_file"
                | "set_permissions"
                | "rename_file"
                | "delete_files"
                | "delete_files_fast"
                | "copy_file"
                | "upload_file"
                | "download_file"
                | "upload_directory"
                | "download_directory"
                | "cancel_task"
        ) {
            let z_log_result = match &z_result {
                Ok(l_value) => self.store.record_operation(c_actor, c_method, l_value),
                Err(_) => self
                    .store
                    .record_operation(c_actor, c_method, &json!({"success":false})),
            };
            if let Err(err) = z_log_result {
                eprintln!("操作记录保存失败: {err}");
            }
        }
        if let Some((z_client, (z_detail, z_info))) = z_activity {
            let z_error = z_result.as_ref().err().map(|err| safe_error(&err.to_string()).chars().take(500).collect()).unwrap_or_default();
            if let Ok(mut z_state) = self.state.lock() {
                z_state.mcp_activity.push_front((c_method.to_owned(), z_client, z_detail, z_result.is_ok(), epoch_ms(), z_info, z_error));
                z_state.mcp_activity.truncate(200);
            }
        }
        z_result
    }

    fn save_connection(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        require_user(c_actor)?;
        let z_connection: Connection = serde_json::from_value(
            c_args
                .get("connection")
                .cloned()
                .context("缺少 connection")?,
        )?;
        let z_password = c_args.get("password").and_then(Value::as_str);
        let z_connection = self.store.save_edited(z_connection, z_password, c_args.get("admin_password").and_then(Value::as_str))?;
        // 撤销授权或改了登录身份时, 取消不再被允许的 MCP 任务.
        {
            let z_state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            for z_task in z_state.tasks.values() {
                if let Ok(mut z_task) = z_task.lock() {
                    if z_task.source == "mcp" && z_task.connection.id == z_connection.id && !mcp_permits(&z_connection, &z_task.connection) {
                        if let Some(z_cancel) = z_task.cancel.take() { let _ = z_cancel.send(()); }
                    }
                }
            }
        }
        Ok(json!({"connection":z_connection}))
    }

    fn import_finalshell(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        require_user(c_actor)?;
        let z_path = PathBuf::from(required_string(&c_args, "path")?);
        let z_group = c_args.get("group").and_then(Value::as_str);
        let (z_connections, z_skipped) = self.store.import_finalshell(&z_path, z_group)?;
        Ok(json!({"connections": z_connections, "skipped": z_skipped}))
    }

    fn save_saved_command(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        require_user(c_actor)?;
        let z_command =
            serde_json::from_value(c_args.get("command").cloned().context("缺少 command")?)?;
        Ok(json!({"command": self.commands.save(z_command)?}))
    }

    fn delete_saved_command(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        require_user(c_actor)?;
        self.commands
            .remove(required_string(&c_args, "command_id")?)?;
        Ok(json!({"removed":true}))
    }

    fn save_command_group(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        require_user(c_actor)?;
        let z_old_name = match c_args.get("old_name") {
            Some(l_value) => l_value.as_str().context("old_name必须是字符串")?,
            None => "",
        };
        let z_name = required_string(&c_args, "name")?;
        self.commands.save_group(z_old_name, z_name)?;
        Ok(json!({"saved":true,"name":z_name}))
    }

    fn delete_command_group(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        require_user(c_actor)?;
        let z_name = required_string(&c_args, "name")?;
        self.commands.remove_group(z_name)?;
        Ok(json!({"removed":true,"name":z_name}))
    }

    async fn send_saved_command(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        require_user(c_actor)?;
        let z_id = required_string(&c_args, "command_id")?;
        let z_command = self
            .commands
            .list()?
            .into_iter()
            .find(|c| c.id == z_id)
            .context("常用命令不存在")?;
        self.send_command(
            json!({"session_id":required_string(&c_args,"session_id")?,"command":z_command.command}),
            c_actor,
        ).await
    }

    async fn send_command(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        require_user(c_actor)?;
        let z_session_id = required_string(&c_args, "session_id")?;
        let z_command = required_string(&c_args, "command")?.trim_end_matches(['\r', '\n']);
        ensure!(!z_command.trim().is_empty() && z_command.len() <= 8192 && !z_command.contains('\0'), "命令为空、含 NUL 或超过 8 KiB");
        let z_session = self.session(z_session_id, c_actor)?;
        let z_bracketed = z_session.terminal.lock().unwrap_or_else(PoisonError::into_inner)
            .parser.screen().bracketed_paste();
        // 多行内容作为一次粘贴交给远端 Shell, 最后再按回车.
        let z_data = if z_bracketed {
            format!("\u{1b}[200~{z_command}\u{1b}[201~\r")
        } else {
            format!("{z_command}\r")
        };
        self.write_terminal(
            json!({"session_id":z_session_id,"data":z_data}),
            c_actor,
        ).await
    }

    async fn list_local_files(&self, c_args: Value) -> Result<Value> {
        let z_path = PathBuf::from(required_string(&c_args, "path")?);
        // 本机目录不占用远端文件操作名额.
        tokio::task::spawn_blocking(move || -> Result<Value> {
            let z_path = z_path.canonicalize().context("本地目录不存在")?;
            ensure!(z_path.is_dir(), "请选择本地目录");
            let mut z_entries = Vec::new();
            let mut z_name_bytes = 0usize;
            let mut z_truncated = false;
            for z_entry in std::fs::read_dir(&z_path)? {
                let Ok(z_entry) = z_entry else { continue };
                let z_file_path = z_entry.path();
                let z_name = z_entry.file_name().to_string_lossy().into_owned();
                let z_path_text = z_file_path.to_string_lossy().into_owned();
                z_name_bytes = z_name_bytes.saturating_add(z_name.len() + z_path_text.len());
                if z_entries.len() >= MAX_LIST_ENTRIES || z_name_bytes > MAX_LIST_NAME_BYTES { z_truncated = true; break; }
                let Ok(z_metadata) = std::fs::symlink_metadata(&z_file_path) else { continue };
                let z_file_type = z_metadata.file_type();
                let z_kind = if z_file_type.is_symlink() {
                    "symlink"
                } else if z_file_type.is_dir() {
                    "directory"
                } else {
                    "file"
                };
                #[cfg(unix)]
                let z_permissions = {
                    use std::os::unix::fs::PermissionsExt;
                    Some(format_unix_permissions(z_metadata.permissions().mode()))
                };
                #[cfg(not(unix))]
                let z_permissions: Option<String> = None;
                #[cfg(unix)]
                let z_owner = {
                    use std::os::unix::fs::MetadataExt;
                    Some(format!("{}/{}", z_metadata.uid(), z_metadata.gid()))
                };
                #[cfg(not(unix))]
                let z_owner: Option<String> = None;
                let z_modified_time = z_metadata
                    .modified()
                    .ok()
                    .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                    .map(|duration| duration.as_secs());
                z_entries.push(json!({"name":z_name,"path":z_path_text,"is_dir":z_kind == "directory","kind":z_kind,"size":z_metadata.len(),"modified_time":z_modified_time,"permissions":z_permissions,"owner":z_owner}));
            }
            z_entries.sort_by(|a,b| b["is_dir"].as_bool().cmp(&a["is_dir"].as_bool()).then(a["name"].as_str().cmp(&b["name"].as_str())));
            // 分页同远端目录: MCP 结果有大小上限, 默认每次 500 条, 用 offset 继续读取.
            let z_total = z_entries.len();
            let z_offset = c_args.get("offset").and_then(Value::as_u64).unwrap_or(0) as usize;
            let z_limit = c_args.get("limit").and_then(Value::as_u64).unwrap_or(500) as usize;
            let z_entries = z_entries.into_iter().skip(z_offset).take(z_limit).collect::<Vec<_>>();
            Ok(json!({"path":z_path.to_string_lossy(),"entries":z_entries,"total":z_total,"offset":z_offset,"truncated":z_truncated}))
        }).await.context("读取本地目录失败")?
    }

    fn export_finalshell(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        require_user(c_actor)?;
        let z_path = PathBuf::from(required_string(&c_args, "path")?);
        let z_password = c_args
            .get("include_password")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if c_args.get("all").and_then(Value::as_bool) == Some(true) {
            let z_count = self.store.export_all_finalshell(&z_path, z_password, c_args.get("group").and_then(Value::as_str))?;
            Ok(json!({"exported": true, "count":z_count}))
        } else {
            let z_id = required_string(&c_args, "connection_id")?;
            self.store.export_finalshell(z_id, &z_path, z_password)?;
            Ok(json!({"exported": true, "count":1}))
        }
    }

    fn remove_connection(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        require_user(c_actor)?;
        let z_id = required_string(&c_args, "connection_id")?;
        ensure!(
            !self.state.lock().unwrap_or_else(PoisonError::into_inner)
                .sessions
                .values()
                .any(|s| s.connection.id == z_id),
            "连接正在使用，断开后再删除"
        );
        self.store.remove(z_id)?;
        Ok(json!({"removed": true}))
    }

    fn new_tab(&self, c_actor: Actor) -> Result<Value> {
        require_user(c_actor)?;
        let mut z_state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        ensure!(z_state.tabs.len() < MAX_TABS, "最多打开 32 个标签");
        let z_id = uuid::Uuid::new_v4().to_string();
        z_state.tabs.push(ConnectionTab {
            id: z_id.clone(),
            connection_id: None,
        });
        z_state.active_session_id = Some(z_id.clone());
        Ok(json!({"session_id":z_id,"empty":true}))
    }

    fn connect(self: &Arc<Self>, c_args: Value, c_actor: Actor) -> Result<Value> {
        let z_conn = self.store.get(required_string(&c_args, "connection_id")?)?;
        authorize(&z_conn, c_actor)?;
        let mut z_state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        ensure!(
            z_state.sessions.len() < MAX_SESSIONS,
            "最多同时连接 8 个会话"
        );
        let z_id = if let Some(z_id) = c_args.get("tab_id").and_then(Value::as_str) {
            ensure!(z_state.tabs.iter().any(|c| c.id == z_id), "标签已关闭");
            ensure!(
                !z_state.sessions.contains_key(z_id),
                "标签已连接, 请使用重新连接"
            );
            z_id.to_owned()
        } else {
            ensure!(z_state.tabs.len() < MAX_TABS, "最多打开 32 个标签");
            let z_id = uuid::Uuid::new_v4().to_string();
            z_state.tabs.push(ConnectionTab {
                id: z_id.clone(),
                connection_id: None,
            });
            z_id
        };
        if let Some(z_tab) = z_state.tabs.iter_mut().find(|c| c.id == z_id) {
            z_tab.connection_id = Some(z_conn.id.clone());
        }
        let z_rows = bounded_u16(&c_args, "rows", 30, 1, MAX_TERMINAL_LINES);
        let z_cols = bounded_u16(&c_args, "cols", 100, 1, MAX_TERMINAL_COLS);
        let z_scrollback = self.store.settings().map(|c| c.terminal_scrollback as usize).unwrap_or(2000).clamp(1000, MAX_SCROLLBACK);
        // 按各终端已记录的分配额统计, 不需要锁住终端, 不会漏算. 宽度至少按 200 列估算, 窗口后来变宽也大多在额度内;
        // 超过时调整尺寸处会把实际占用记回. 最低 1000 行优先于总预算, 保证每个标签都有可用的历史.
        let z_used = z_state.sessions.values().map(|c| c.history_bytes.load(Ordering::Relaxed)).sum::<usize>();
        let z_width = usize::from(z_cols).max(200);
        let z_scrollback = z_scrollback.min((TERMINAL_HISTORY_BUDGET.saturating_sub(z_used) / (z_width * 32)).max(1000));
        let (z_tx, z_rx) = mpsc::channel::<TerminalInput>(32);
        let (z_resize_tx, z_resize_rx) = tokio::sync::watch::channel((0u16, 0u16));
        let z_session = Arc::new(LiveSession {
            id: z_id.clone(),
            link: epoch_ms(),
            file_slot: Arc::new(Semaphore::new(1)),
            browse_sftp: Mutex::new(None),
            file_details: Mutex::new(None),
            file_details_abort: Mutex::new(None),
            history_bytes: std::sync::atomic::AtomicUsize::new(z_scrollback * z_width * 32),
            transfer: tokio::sync::Mutex::new((None, None)),
            transfer_password: Mutex::new(None),
            transfer_key: Mutex::new(None),
            idle_upload: Mutex::new(Vec::new()),
            idle_download: Mutex::new(Vec::new()),
            proxy: Mutex::new(None),
            host_fingerprint: Mutex::new(None),
            link_errors: Mutex::new([None, None]),
            upload_inflight: Arc::new(Semaphore::new(UPLOAD_SESSION_INFLIGHT)),
            upload_main_inflight: Arc::new(Semaphore::new(UPLOAD_MAIN_INFLIGHT)),
            connection: z_conn,
            actor: c_actor,
            status: Mutex::new("connecting".into()),
            handle: Mutex::new(None),
            terminal_tx: Mutex::new(Some(z_tx)),
            terminal_gen: AtomicU64::new(epoch_ms()),
            resize_tx: Mutex::new(Some(z_resize_tx)),
            terminal: Mutex::new(Terminal {
                parser: vt100::Parser::new(z_rows, z_cols, z_scrollback),
                scrollback: z_scrollback,
                // 同一标签重连会沿用会话 ID, 序号从时间戳开始, 界面不会把新终端误判为未变化.
                seq: epoch_ms(),
                filter: TerminalFilter::default(),
                decoded: Vec::with_capacity(MAX_TERMINAL_DECODED_CHUNK),
                csi: Vec::with_capacity(64),
                modified_keys: 0,
                kitty_keys: Vec::new(),
                cursor_shape: 0,
                sync_since: None,
                osc: Vec::new(),
                clipboard: None,
            }),
            trust: Mutex::new(None),
            auth_prompts: Mutex::new(Vec::new()),
            auth_answers: Mutex::new(None),
            shell: AtomicBool::new(false),
            shell_stop: Mutex::new(None),
            monitoring: AtomicBool::new(false),
            connect_cancel: Mutex::new(None),
            lease: Mutex::new(None),
            last_user_input: Mutex::new(None),
            input_line: Mutex::new((0, false, String::new())),
            admin_input: Mutex::new(None),
            monitor: Mutex::new(None),
            identity_cache: Mutex::new(HashMap::new()),
            file_time_cache: tokio::sync::Mutex::new(None),
        });
        z_state.sessions.insert(z_id.clone(), z_session.clone());
        if c_args.get("focus").and_then(Value::as_bool) != Some(false) {
            z_state.active_session_id = Some(z_id.clone());
        }
        drop(z_state);
        let (z_cancel_tx, z_cancel_rx) = oneshot::channel();
        if let Ok(mut cancel) = z_session.connect_cancel.lock() {
            *cancel = Some(z_cancel_tx);
        }
        let z_password = c_args
            .get("password")
            .and_then(Value::as_str)
            .map(|s| Zeroizing::new(s.to_owned()));
        let z_files_only = c_args.get("files_only").and_then(Value::as_bool) == Some(true);
        let z_backend = self.clone();
        tokio::spawn(async move {
            let z_cancel_session = z_session.clone();
            let z_connect_session = z_session.clone();
            tokio::select! {
                _ = z_cancel_rx => set_status(&z_cancel_session, "disconnected".into()),
                _ = z_backend.connect_task(z_connect_session, z_rx, z_resize_rx, z_password, z_files_only) => {},
            }
        });
        Ok(json!({"session_id": z_id,"empty":false}))
    }

    async fn reconnect(self: &Arc<Self>, c_args: Value, c_actor: Actor) -> Result<Value> {
        let z_id = required_string(&c_args, "session_id")?.to_owned();
        let (z_connection_id, z_session) = {
            let z_state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let z_tab = z_state.tabs.iter().find(|c| c.id == z_id).context("标签已关闭")?;
            (
                z_tab.connection_id.clone().context("请先选择服务器")?,
                z_state.sessions.get(&z_id).cloned(),
            )
        };
        let z_connection = self.store.get(&z_connection_id)?;
        authorize(&z_connection, c_actor)?;
        if let Some(z_session) = &z_session {
            if c_args.get("only_disconnected").and_then(Value::as_bool) == Some(true) {
                let z_status = z_session.status.lock().unwrap_or_else(PoisonError::into_inner);
                if !matches!(z_status.as_str(), "disconnected" | "authentication_failed")
                    && !z_status.starts_with("error:")
                {
                    return Ok(json!({"session_id":z_id,"skipped":true}));
                }
            }
        }
        // 在原标签内重连: 先断开旧会话但保留标签, 再用同一标签 ID 建立新会话.
        // 新会话在认证完成前拒绝终端输入, 旧会话残留的排队输入不会进入新连接.
        // 进行中的传输不取消: 旧连接关闭后转入自动重连, 在新会话上从断点继续.
        self.disconnect_session(&z_id, c_actor, false, true).await?;
        // 断开期间用户可能已切到别的标签: 未指定时按此刻的当前标签判断是否聚焦 (之后 connect 同步完成, 不再有等待),
        // 避免抢回焦点让输入落到别的服务器.
        let z_active = self.state.lock().unwrap_or_else(PoisonError::into_inner).active_session_id.as_deref() == Some(z_id.as_str());
        let mut z_args = json!({"connection_id":z_connection.id,"tab_id":z_id,"focus":z_active});
        for z_key in ["rows", "cols", "password", "focus", "files_only"] {
            if let Some(z_value) = c_args.get(z_key) {
                z_args[z_key] = z_value.clone();
            }
        }
        self.connect(z_args, c_actor)
    }

    async fn connect_task(
        self: Arc<Self>,
        c_session: Arc<LiveSession>,
        c_input: mpsc::Receiver<TerminalInput>,
        c_resize: tokio::sync::watch::Receiver<(u16, u16)>,
        c_password: Option<Zeroizing<String>>,
        c_files_only: bool,
    ) {
        // 发起连接即记入最近连接 (快速连接置顶), 不论之后是否成功.
        if let Err(err) = self.store.record_recent(&c_session.connection.id) {
            eprintln!("最近连接保存失败: {err}");
        }
        let z_handler = SshHandler {
            session: Arc::downgrade(&c_session),
            store: self.store.clone(),
            transfer: false,
        };
        // 代理在会话开始时取一次, 主连接与之后的传输连接用同一份配置.
        let z_proxy = match c_session.connection.proxy_id.as_deref() {
            None => None,
            Some(l_id) => match self.store.proxy_with_secret(l_id) {
                Ok(l_route) => Some(Arc::new(l_route)),
                Err(err) => {
                    set_status(&c_session, format!("error: {}", safe_error(&format!("{err:#}"))));
                    return;
                }
            },
        };
        *c_session.proxy.lock().unwrap_or_else(PoisonError::into_inner) = z_proxy.clone();
        let mut z_handle = match tokio::time::timeout(
            Duration::from_secs(150),
            ssh_connect(&c_session.connection, z_proxy.as_deref(), z_handler),
        )
        .await
        {
            Ok(Ok(handle)) => handle,
            Ok(Err(err)) => {
                set_status(
                    &c_session,
                    format!("error: {}", safe_error(&format!("{err:#}"))),
                );
                return;
            }
            Err(_) => {
                set_status(&c_session, "error: SSH连接超时".into());
                return;
            }
        };
        let z_auth_result = tokio::time::timeout(Duration::from_secs(120), async {
            match &c_session.connection.auth {
                AuthMethod::Password => {
                    let z_known = match c_password.as_ref() {
                        Some(l_secret) => Some(l_secret.clone()),
                        None => self.store.secret(&c_session.connection.id)?,
                    };
                    if let Some(l_secret) = z_known {
                        if z_handle.authenticate_password(c_session.connection.username.clone(), l_secret.as_str()).await?.success() {
                            *c_session.transfer_password.lock().unwrap_or_else(PoisonError::into_inner) = Some(l_secret);
                            return Ok(true);
                        }
                        // 保存的密码已失效时改为弹窗重新输入, 不直接判定失败.
                    }
                    // 与 OpenSSH 一致, 输错可重试, 共 3 次.
                    for i in 0..3 {
                        let l_prompt = format!(
                            "{}{}@{} 的 SSH 密码",
                            if i > 0 { "密码错误, 请重新输入 " } else { "" },
                            c_session.connection.username,
                            c_session.connection.host
                        );
                        let l_secret = self.ask_secret(&c_session, l_prompt).await?;
                        if z_handle.authenticate_password(c_session.connection.username.clone(), l_secret.as_str()).await?.success() {
                            *c_session.transfer_password.lock().unwrap_or_else(PoisonError::into_inner) = Some(l_secret.clone());
                            self.remember_secret(&c_session, l_secret)?;
                            return Ok(true);
                        }
                    }
                    Ok(false)
                }
                AuthMethod::PrivateKey => {
                    // 未选私钥 (如从 FinalShell 导入的私钥连接) 或文件已移走时给出明确提示, 界面提供"编辑连接".
                    let z_key_file = self.store.private_key_file(&c_session.connection)?;
                    let path = z_key_file.to_str().context("私钥路径包含无效字符")?;
                    let z_metadata = std::fs::metadata(path).with_context(|| format!("私钥文件不存在或无法读取, 请编辑连接重新选择私钥: {path}"))?;
                    ensure!(
                        z_metadata.is_file() && z_metadata.len() <= 1024 * 1024,
                        "私钥必须是 1 MiB 以内的普通文件"
                    );
                    let z_saved = match c_password.as_ref() {
                        Some(l_secret) => Some(l_secret.clone()),
                        None => self.store.secret(&c_session.connection.id)?,
                    };
                    let key = match keys::load_secret_key(path, z_saved.as_deref().map(String::as_str)) {
                        Ok(l_key) => l_key,
                        // 私钥加密且没有可用口令时弹窗询问, 成功后按"记住密码"设置保存.
                        Err(_) => {
                            let mut l_key = None;
                            for i in 0..3 {
                                let l_prompt = format!("{}私钥 {path} 的口令", if i > 0 { "口令错误, 请重新输入 " } else { "" });
                                let l_passphrase = self.ask_secret(&c_session, l_prompt).await?;
                                if let Ok(l_loaded) = keys::load_secret_key(path, Some(l_passphrase.as_str())) {
                                    self.remember_secret(&c_session, l_passphrase)?;
                                    l_key = Some(l_loaded);
                                    break;
                                }
                            }
                            l_key.context("私钥口令不正确或不是受支持的私钥格式")?
                        }
                    };
                    let key = Arc::new(key);
                    let z_hash = z_handle.best_supported_rsa_hash().await.ok().flatten().flatten();
                    let z_key = PrivateKeyWithHashAlg::new(key.clone(), z_hash);
                    let z_success = z_handle
                        .authenticate_publickey(c_session.connection.username.clone(), z_key)
                        .await?
                        .success();
                    if z_success {
                        *c_session.transfer_key.lock().unwrap_or_else(PoisonError::into_inner) = Some(key);
                    }
                    Ok(z_success)
                }
                AuthMethod::KeyboardInteractive => {
                    self.keyboard_auth(&c_session, &mut z_handle).await
                }
            }
        })
        .await;
        let z_auth: Result<bool> =
            z_auth_result.unwrap_or_else(|_| Err(anyhow::anyhow!("SSH认证超时")));
        // 外层认证期限可能先结束, 同时撤下尚在等待的输入弹窗.
        if let Ok(mut z_prompts) = c_session.auth_prompts.lock() { z_prompts.clear(); }
        if let Ok(mut z_answers) = c_session.auth_answers.lock() { z_answers.take(); }
        match z_auth {
            Ok(true) => {}
            Ok(false) => {
                set_status(&c_session, "authentication_failed".into());
                return;
            }
            Err(err) => {
                set_status(
                    &c_session,
                    format!("error: {}", safe_error(&err.to_string())),
                );
                return;
            }
        }
        if c_session.actor == Actor::Mcp
            && !self.mcp_permitted(&c_session.connection)
        {
            set_status(&c_session, "error: 此连接已撤销 MCP 授权".into());
            return;
        }
        let z_handle = Arc::new(z_handle);
        *c_session.handle.lock().unwrap_or_else(PoisonError::into_inner) = Some(z_handle.clone());
        watch_connection(&c_session, &z_handle);
        // 仅文件传输: 不开终端和监控, 节省远端进程与带宽.
        if c_files_only {
            set_status(&c_session, "connected".into());
            shell_closed(&c_session, "");
            return;
        }
        c_session.shell.store(true, Ordering::Release);
        self.run_shell(&c_session, &z_handle, c_input, c_resize).await;
    }

    // 在已认证的连接上打开终端通道并转发输入输出, 结束时只在 SSH 连接也断开时关闭会话.
    async fn run_shell(
        &self,
        c_session: &Arc<LiveSession>,
        c_handle: &Arc<client::Handle<SshHandler>>,
        mut c_input: mpsc::Receiver<TerminalInput>,
        mut c_resize: tokio::sync::watch::Receiver<(u16, u16)>,
    ) {
        let (rows, cols) = terminal_size(c_session);
        // 启动请求在确认前到达的输出先暂存, 确认后再交给终端.
        let mut z_early: Vec<Vec<u8>> = Vec::new();
        let z_opened = async {
            // 请求失败或超时时由守卫关闭通道, 服务器保持通道也不会占住会话名额.
            let mut z_channel = ClosingChannel(Some(open_channel(c_handle, 5).await.context("打开终端通道超时")?));
            tokio::time::timeout(
                Duration::from_secs(15),
                z_channel.request_pty(false, "xterm-256color", cols as u32, rows as u32, 0, 0, &[]),
            )
            .await
            .context("请求终端超时")??;
            tokio::time::timeout(Duration::from_secs(15), z_channel.request_shell(true))
                .await
                .context("启动终端超时")??;
            // request_shell 只代表请求已发出; 服务器拒绝 (如只允许 SFTP 的账号) 会回 Failure 但可能保持通道.
            tokio::time::timeout(Duration::from_secs(15), async {
                loop {
                    match z_channel.wait().await {
                        Some(ChannelMsg::Success) => return Ok(()),
                        Some(ChannelMsg::Failure) => bail!("服务器拒绝启动终端"),
                        Some(ChannelMsg::Data { data }) | Some(ChannelMsg::ExtendedData { data, .. }) => z_early.push(data.to_vec()),
                        Some(ChannelMsg::Eof | ChannelMsg::Close) | None => bail!("终端通道已被服务器关闭"),
                        Some(_) => {}
                    }
                }
            })
            .await
            .context("等待终端启动确认超时")??;
            anyhow::Ok(z_channel.0.take().context("终端通道已释放")?)
        }
        .await;
        let z_channel = match z_opened {
            Ok(z_channel) => z_channel,
            // 部分账号只允许 SFTP, 终端打不开时保留连接供文件传输使用.
            Err(err) => {
                set_status(c_session, "connected".into());
                shell_closed(c_session, &format!("终端打开失败: {}. 文件传输仍可使用.", safe_error(&format!("{err:#}"))));
                return;
            }
        };
        if !z_early.is_empty() {
            for l_data in &z_early {
                let _ = c_session.terminal.lock().map(|mut c| c.process(l_data));
            }
            self.terminal_changed.notify_one();
        }
        // 终端就绪后才标记已连接, 界面随后才打开 SFTP 读目录, 与登录过程错开.
        set_status(c_session, "connected".into());
        // 监控的采样命令在终端启动后再开始: 与登录同时打开的命令会话会让部分系统 (Ubuntu pam_motd) 不显示登录欢迎信息.
        start_monitor(c_session);
        let (mut z_reader, z_writer) = z_channel.split();
        let z_session_for_write = c_session.clone();
        let z_store_for_write = self.store.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    input = c_input.recv() => match input {
                        Some(input) => {
                            let l_connected = z_session_for_write.status.lock().is_ok_and(|c| c.as_str() == "connected");
                            let allowed = l_connected && (input.actor == Actor::User || z_store_for_write.get(&z_session_for_write.connection.id).is_ok_and(|c| mcp_permits(&c, &z_session_for_write.connection)) && z_session_for_write.lease.lock().ok().and_then(|l| l.as_ref().map(|l| l.id == input.lease_id.as_deref().unwrap_or("") && l.until > Instant::now())).unwrap_or(false));
                            if allowed && z_writer.data_bytes(input.data).await.is_err() { break; }
                        }
                        None => break,
                    },
                    resize = c_resize.changed() => {
                        // 发送端只在终端结束时释放, 已关闭的通道会立即返回, 不退出会空转.
                        if resize.is_err() { break; }
                        let (rows, cols) = *c_resize.borrow_and_update();
                        if z_writer.window_change(cols as u32, rows as u32, 0, 0).await.is_err() { break; }
                    }
                }
            }
            // 输入通道被释放 (关闭终端) 时主动关闭通道, SSH 连接保留.
            // 关闭终端时远端 Shell 可能仍在运行, 服务器要等它退出才回应关闭; 先发挂断信号让它尽快结束.
            let _ = z_writer.signal(russh::Sig::HUP).await;
            let _ = z_writer.eof().await;
            let _ = z_writer.close().await;
        });
        // 主动关闭终端时不等服务器回应, 立即结束读取.
        let (z_stop_tx, mut z_stop_rx) = oneshot::channel::<()>();
        *c_session.shell_stop.lock().unwrap_or_else(PoisonError::into_inner) = Some(z_stop_tx);
        let mut z_stopped = false;
        loop {
            let z_message = tokio::select! {
                l_message = z_reader.wait() => l_message,
                _ = &mut z_stop_rx => { z_stopped = true; break; }
            };
            match z_message {
                Some(ChannelMsg::Data { data }) | Some(ChannelMsg::ExtendedData { data, .. }) => {
                    let z_replies = c_session.terminal.lock().map(|mut c| c.process(&data)).unwrap_or_default();
                    self.terminal_changed.notify_one();
                    if !z_replies.is_empty() {
                        if let Ok(z_tx) = c_session.terminal_tx.lock() {
                            if let Some(z_tx) = z_tx.as_ref() {
                                let _ = z_tx.try_send(TerminalInput { data: z_replies, actor: Actor::User, lease_id: None });
                            }
                        }
                    }
                }
                Some(ChannelMsg::Eof | ChannelMsg::Close) | None => break,
                Some(_) => {}
            }
        }
        c_session.shell_stop.lock().unwrap_or_else(PoisonError::into_inner).take();
        if !z_stopped && c_handle.is_closed() {
            close_session(c_session, &dropped_status(c_session), true);
            c_session.handle.lock().unwrap_or_else(PoisonError::into_inner).take();
        } else {
            shell_closed(c_session, "");
        }
        self.terminal_changed.notify_one();
    }

    // 在保持连接的会话上重新打开终端 (仅文件传输连接或终端退出后).
    fn open_terminal(self: &Arc<Self>, c_args: Value, c_actor: Actor) -> Result<Value> {
        let z_session = self.session(required_string(&c_args, "session_id")?, c_actor)?;
        ensure!(z_session.status.lock().unwrap_or_else(PoisonError::into_inner).as_str() == "connected", "SSH会话尚未连接");
        let z_handle = z_session.handle.lock().unwrap_or_else(PoisonError::into_inner).clone().context("SSH会话尚未连接")?;
        ensure!(!z_session.shell.swap(true, Ordering::AcqRel), "终端已打开");
        z_session.terminal.lock().unwrap_or_else(PoisonError::into_inner).reset_screen();
        *z_session.input_line.lock().unwrap_or_else(PoisonError::into_inner) = (0, false, String::new());
        let (z_tx, z_rx) = mpsc::channel::<TerminalInput>(32);
        let (z_resize_tx, z_resize_rx) = tokio::sync::watch::channel((0u16, 0u16));
        {
            let mut l_tx = z_session.terminal_tx.lock().unwrap_or_else(PoisonError::into_inner);
            // 同一毫秒内重开也要换代次.
            z_session.terminal_gen.fetch_max(epoch_ms().max(z_session.terminal_gen.load(Ordering::Acquire) + 1), Ordering::AcqRel);
            *l_tx = Some(z_tx);
        }
        *z_session.resize_tx.lock().unwrap_or_else(PoisonError::into_inner) = Some(z_resize_tx);
        let z_backend = self.clone();
        tokio::spawn(async move { z_backend.run_shell(&z_session, &z_handle, z_rx, z_resize_rx).await });
        Ok(json!({"opened": true}))
    }

    async fn ask_secret(&self, c_session: &LiveSession, c_prompt: String) -> Result<Zeroizing<String>> {
        let mut z_answers = self.request_auth_answers(c_session, vec![AuthPrompt { prompt: c_prompt, echo: false }]).await?;
        ensure!(z_answers.len() == 1, "登录认证已取消");
        Ok(Zeroizing::new(z_answers.remove(0)))
    }

    // 认证成功后的密码或口令写入连接文件, 下次连接不再询问.
    fn remember_secret(&self, c_session: &LiveSession, c_secret: Zeroizing<String>) -> Result<()> {
        let z_connection = self.store.get(&c_session.connection.id)?;
        self.store.save(z_connection, Some(c_secret.as_str()))?;
        Ok(())
    }

    async fn keyboard_auth(
        &self,
        c_session: &Arc<LiveSession>,
        c_handle: &mut client::Handle<SshHandler>,
    ) -> Result<bool> {
        use client::KeyboardInteractiveAuthResponse as Kbd;
        let mut z_result = tokio::time::timeout(
            Duration::from_secs(30),
            c_handle.authenticate_keyboard_interactive_start(
                c_session.connection.username.clone(),
                None::<String>,
            ),
        )
        .await
        .context("keyboard-interactive认证启动超时")??;
        for _ in 0..8 {
            match z_result {
                Kbd::Success => return Ok(true),
                Kbd::Failure { .. } => return Ok(false),
                Kbd::InfoRequest { prompts, .. } => {
                    if prompts.len() > 32 {
                        return Ok(false);
                    }
                    let z_prompts: Vec<AuthPrompt> = prompts
                        .iter()
                        .map(|p| AuthPrompt {
                            prompt: p.prompt.clone(),
                            echo: p.echo,
                        })
                        .collect();
                    let z_answers = self.request_auth_answers(c_session, z_prompts).await?;
                    z_result = c_handle
                        .authenticate_keyboard_interactive_respond(z_answers)
                        .await?;
                }
            }
        }
        Ok(false)
    }

    async fn request_auth_answers(&self, c_session: &LiveSession, c_prompts: Vec<AuthPrompt>) -> Result<Vec<String>> {
        if c_prompts.is_empty() { return Ok(Vec::new()); }
        let (tx, rx) = oneshot::channel();
        *c_session.auth_answers.lock().unwrap_or_else(PoisonError::into_inner) = Some(tx);
        *c_session.auth_prompts.lock().unwrap_or_else(PoisonError::into_inner) = c_prompts;
        set_status(c_session, "auth_required".into());
        let z_result = tokio::time::timeout(Duration::from_secs(120), rx).await;
        if let Ok(mut z_prompts) = c_session.auth_prompts.lock() { z_prompts.clear(); }
        if let Ok(mut z_answers) = c_session.auth_answers.lock() { z_answers.take(); }
        let z_answers = z_result.context("等待认证输入超时")?.context("登录认证已取消")?;
        if c_session.actor == Actor::Mcp && !self.mcp_permitted(&c_session.connection) {
            bail!("此连接已撤销 MCP 授权");
        }
        set_status(c_session, "authenticating".into());
        Ok(z_answers)
    }

    fn focus_session(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        require_user(c_actor)?;
        let z_id = required_string(&c_args, "session_id")?.to_owned();
        let mut z_state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        ensure!(z_state.tabs.iter().any(|c| c.id == z_id), "标签已关闭");
        z_state.active_session_id = Some(z_id.clone());
        Ok(json!({"focused": true,"session_id":z_id}))
    }

    fn trust_host(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        require_user(c_actor)?;
        let z_session = self.session(required_string(&c_args, "session_id")?, c_actor)?;
        let z_decision = required_string(&c_args, "decision")?;
        let mut z_trust = z_session.trust.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(prompt) = z_trust.take() else {
            bail!("当前没有待确认的主机指纹");
        };
        let z_accept = match z_decision {
            "once" => true,
            "permanent" => {
                self.store.trust_host(
                    &z_session.connection.host,
                    z_session.connection.port,
                    &prompt.fingerprint,
                )?;
                true
            }
            "reject" => false,
            _ => {
                *z_trust = Some(prompt);
                bail!("decision 只支持 once/permanent/reject");
            }
        };
        let _ = prompt.answer.send(z_accept);
        Ok(json!({"accepted": z_accept}))
    }

    fn answer_auth(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        require_user(c_actor)?;
        let z_session = self.session(required_string(&c_args, "session_id")?, c_actor)?;
        let z_answers: Vec<String> =
            serde_json::from_value(c_args.get("answers").cloned().context("缺少 answers")?)?;
        ensure!(
            z_answers.len() <= 32 && z_answers.iter().all(|s| s.len() <= 4096),
            "认证答案超出限制"
        );
        if let Some(tx) = z_session.auth_answers.lock().unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            let _ = tx.send(z_answers);
        }
        Ok(json!({"submitted": true}))
    }

    fn read_terminal(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        let z_session = self.session(required_string(&c_args, "session_id")?, c_actor)?;
        let z_max = bounded_u16(&c_args, "max_lines", 200, 1, MAX_SCROLLBACK as u16) as usize;
        let z_offset = c_args
            .get("scroll_offset")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            .min(MAX_SCROLLBACK as u64) as usize;
        let z_after_seq = c_args.get("after_seq").and_then(Value::as_u64);
        let mut z_term = z_session.terminal.lock().unwrap_or_else(PoisonError::into_inner);
        // 程序开启同步输出后漏发结束标记且不再输出时, 不会再有 bump 触发刷新; 超过 150ms 视为同步结束, 照常显示.
        if z_term.sync_since.is_some_and(|c| c.elapsed() >= Duration::from_millis(150)) {
            z_term.sync_since = None;
            z_term.seq = z_term.seq.wrapping_add(1);
        }
        // 每 25ms 轮询一次, 仅在有待写剪贴板时才读取连接配置复核权限.
        let z_clipboard = if c_actor == Actor::User {
            z_term.clipboard.take()
        } else { None };
        if c_args.get("events_only").and_then(Value::as_bool) == Some(true) {
            require_user(c_actor)?;
            return Ok(z_term.unchanged(z_clipboard));
        }
        if z_offset == 0 && z_after_seq == Some(z_term.seq) {
            return Ok(z_term.unchanged(z_clipboard));
        }
        let z_seq = z_term.seq;
        // 只读取当前一页, 避免每次按键回显都复制整份历史缓存.
        let z_screen = z_term.parser.screen_mut();
        let z_previous_offset = z_screen.scrollback();
        z_screen.set_scrollback(MAX_SCROLLBACK);
        let z_history_lines = z_screen.scrollback();
        z_screen.set_scrollback(z_offset);
        let z_actual_offset = z_screen.scrollback();
        let z_text = tail_lines(&z_screen.contents(), z_max);
        let (row, col) = z_screen.cursor_position();
        let z_alternate = z_screen.alternate_screen();
        let mut z_result = json!({"text":z_text,"seq":z_seq,"cols":z_screen.size().1,"rows":z_screen.size().0,"cursor_row":row,"cursor_col":col,"alternate_screen":z_alternate,"bracketed_paste":z_screen.bracketed_paste(),"application_cursor":z_screen.application_cursor(),"hide_cursor":z_screen.hide_cursor(),"scrollback_lines":z_history_lines,"scroll_offset":z_actual_offset});
        if c_actor == Actor::Mcp && z_actual_offset == 0 {
            // 光标所在行与用户未提交输入的标记: MCP 据此判断提示符后是否有用户输入了一半的命令 (翻看历史时不给).
            z_result["cursor_line"] = json!(z_screen.rows(0, z_screen.size().1).nth(usize::from(row)).unwrap_or_default().trim_end());
            z_result["user_input_pending"] = json!(z_session.input_line.lock().unwrap_or_else(PoisonError::into_inner).0 > 0);
        }
        if c_actor == Actor::User {
            z_result["mouse"] = json!(mouse_mode(z_screen));
            // 界面按 VT 网格逐行显示 (长行在终端宽度处折行, 与普通终端一致);
            // 文字按行给出供选择复制, 折行处记入 wrapped, 复制时不插入换行.
            let z_cols = z_screen.size().1;
            let z_rows = z_screen.size().0.min(MAX_TERMINAL_LINES);
            z_result["text"] = json!(z_screen.rows(0, z_cols).take(usize::from(z_rows)).collect::<Vec<_>>().join("\n"));
            z_result["wrapped"] = json!((0..z_rows).filter(|r| !z_alternate && z_screen.row_wrapped(*r)).collect::<Vec<_>>());
            // 片段行号从历史顶端算起: 滚动或新输出时同一行编号不变, 界面只需增删出入视野的行.
            let z_base = (z_history_lines - z_actual_offset) as i32;
            let mut z_runs = Vec::new();
            page_runs(z_screen, z_rows, z_base, &mut z_runs);
            // 翻看历史时额外给出上下各一页, 滚动时新露出的行已有内容, 不会先空白再跳出文字.
            if z_actual_offset > 0 && !z_alternate {
                for z_page in [z_actual_offset + usize::from(z_rows), z_actual_offset.saturating_sub(usize::from(z_rows))] {
                    z_screen.set_scrollback(z_page);
                    let l_page = z_screen.scrollback();
                    if l_page != z_actual_offset {
                        page_runs(z_screen, z_rows, z_base + z_actual_offset as i32 - l_page as i32, &mut z_runs);
                    }
                }
            }
            z_result["runs"] = json!(z_runs);
        }
        z_screen.set_scrollback(z_previous_offset);
        z_result["modified_keys"] = json!(z_term.key_mode());
        z_result["cursor_shape"] = json!(z_term.cursor_shape);
        if let Some(z_text) = z_clipboard { z_result["clipboard"] = json!(z_text); }
        Ok(z_result)
    }

    fn resize_terminal(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        require_user(c_actor)?;
        let z_session = self.session(required_string(&c_args, "session_id")?, c_actor)?;
        let rows = bounded_u16(&c_args, "rows", 30, 1, MAX_TERMINAL_LINES);
        let cols = bounded_u16(&c_args, "cols", 100, 1, MAX_TERMINAL_COLS);
        if let Ok(mut t) = z_session.terminal.lock() {
            t.parser.screen_mut().set_size(rows, cols);
            t.seq = t.seq.wrapping_add(1);
            // 比分配时更宽: 历史行数无法在不丢内容的情况下缩减, 把实际占用记回, 之后新开的终端相应少分.
            z_session.history_bytes.fetch_max(t.scrollback * usize::from(cols) * 32, Ordering::Relaxed);
        }
        if let Some(tx) = z_session.resize_tx.lock().unwrap_or_else(PoisonError::into_inner)
            .as_ref()
        {
            tx.send_replace((rows, cols));
        }
        Ok(json!({"rows":rows,"cols":cols}))
    }

    async fn write_terminal(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        let z_session = self.session(required_string(&c_args, "session_id")?, c_actor)?;
        ensure!(
            z_session.status.lock().unwrap_or_else(PoisonError::into_inner)
                .as_str()
                == "connected",
            "SSH会话尚未连接"
        );
        let z_data = required_string(&c_args, "data")?;
        ensure!(z_data.len() <= MAX_INPUT, "终端输入单次不能超过 16 KiB");
        let z_lease_id = {
            let mut z_user_input = z_session.last_user_input.lock().unwrap_or_else(PoisonError::into_inner);
            let mut z_lease_id = None;
            if c_actor == Actor::Mcp {
                ensure!(
                    z_user_input.is_none_or(|c_time| c_time.elapsed() >= Duration::from_millis(800)),
                    "用户正在输入, 请稍后再操作此终端"
                );
                let mut z_lease = z_session.lease.lock().unwrap_or_else(PoisonError::into_inner);
                // 授权检查在 session() 完成. 连续 AI 输入复用内部标识, 人工输入撤销排队数据.
                if z_lease.as_ref().is_none_or(|l| l.until <= Instant::now()) {
                    *z_lease = Some(Lease {
                        id: uuid::Uuid::new_v4().to_string(),
                        until: Instant::now(),
                    });
                }
                if let Some(z_current) = z_lease.as_mut() {
                    z_current.until = Instant::now() + Duration::from_secs(20);
                }
                z_lease_id = z_lease.as_ref().map(|l| l.id.clone());
            } else {
                *z_user_input = Some(Instant::now());
                *z_session.lease.lock().unwrap_or_else(PoisonError::into_inner) = None;
            }
            z_lease_id
        };
        let z_tx = {
            let l_tx = z_session.terminal_tx.lock().unwrap_or_else(PoisonError::into_inner);
            // 界面带上发起输入时的终端代次; 期间终端已重开 (含 MCP 重开) 就不写入新终端.
            if let Some(l_gen) = c_args.get("terminal").and_then(Value::as_u64) {
                ensure!(l_gen == z_session.terminal_gen.load(Ordering::Acquire), "终端已重新打开, 之前的输入已取消");
            }
            l_tx.clone().context(if c_actor == Actor::Mcp {
                "该会话为仅文件连接, 终端未打开; 可用 run_command 执行命令"
            } else {
                "终端未打开, 可右键标签选择“重新连接终端”"
            })?
        };
        let z_input = TerminalInput {
            data: z_data.as_bytes().to_vec(),
            actor: c_actor,
            lease_id: z_lease_id,
        };
        if c_actor == Actor::User {
            // 保留有界队列, 等待容量而不丢弃粘贴尾部或结束标记.
            z_tx.send(z_input).await.map_err(|_| anyhow::anyhow!("终端输入通道已关闭"))?;
        } else {
            z_tx.try_send(z_input).map_err(|_| anyhow::anyhow!("终端输入队列已满"))?;
        }
        // 输入已进入队列才更新, 被拒绝的请求不改变标记. 用户输入按顺序看结束时的状态;
        // MCP 自己输入的文字不算用户输入, 只有它提交或清空了这一行时才清除标记.
        // vim、less 等全屏程序里的按键不是命令行输入, 不计入.
        let z_alternate = z_session.terminal.lock().unwrap_or_else(PoisonError::into_inner).parser.screen().alternate_screen();
        let mut z_line = z_session.input_line.lock().unwrap_or_else(PoisonError::into_inner);
        if z_alternate {
            *z_line = (0, false, String::new());
        } else {
            line_pending(&mut z_line, z_data, c_actor == Actor::User);
        }
        drop(z_line);
        Ok(json!({"written": z_data.len()}))
    }

    // 向终端输入连接设置的管理员密码并回车. 密码只在本程序内取出写入终端, 不出现在 MCP 参数、返回值和操作记录中.
    async fn input_admin_password(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        let z_session_id = required_string(&c_args, "session_id")?;
        let z_session = self.session(z_session_id, c_actor)?;
        let z_conn = self.store.get(&z_session.connection.id)?;
        let mut z_data = match z_conn.admin_password {
            AdminPassword::None => bail!("此连接未设置管理员密码, 可在编辑连接中设置"),
            // 优先用本次登录实际使用的密码 (含连接时临时输入的), 其次是连接中保存的密码.
            AdminPassword::Login => {
                ensure!(z_conn.auth == AuthMethod::Password, "此连接不是密码登录, 管理员密码需单独设置");
                let l_login = z_session.transfer_password.lock().unwrap_or_else(PoisonError::into_inner).clone();
                match l_login {
                    Some(l_password) => l_password,
                    None => self.store.secret(&z_conn.id)?.context("此连接没有保存登录密码")?,
                }
            }
            AdminPassword::Custom => self.store.admin_secret(&z_conn.id)?.context("管理员密码未保存, 请重新设置")?,
        };
        if c_actor == Actor::Mcp {
            // 只在密码提示处输入: 回显已关闭, 密码不会显示或进入 Shell 历史. 短时间内再次需要输入多半是密码错误, 交还用户处理, 避免触发锁定.
            let z_line = {
                let l_term = z_session.terminal.lock().unwrap_or_else(PoisonError::into_inner);
                let l_screen = l_term.parser.screen();
                let l_row = usize::from(l_screen.cursor_position().0);
                l_screen.rows(0, l_screen.size().1).nth(l_row).unwrap_or_default()
            };
            ensure!(password_prompt(&z_line), "终端当前不是密码提示, 未输入");
            let z_last = *z_session.admin_input.lock().unwrap_or_else(PoisonError::into_inner);
            ensure!(z_last.is_none_or(|c| c.elapsed() >= Duration::from_secs(15)), "距上次代填不足 15 秒, 可能密码错误, 请用户自行输入");
        }
        z_data.push('\r');
        self.write_terminal(json!({"session_id":z_session_id,"data":z_data.as_str()}), c_actor).await?;
        if c_actor == Actor::Mcp {
            *z_session.admin_input.lock().unwrap_or_else(PoisonError::into_inner) = Some(Instant::now());
        }
        Ok(json!({"entered":true}))
    }

    async fn disconnect(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        let z_id = required_string(&c_args, "session_id")?.to_owned();
        // keep_tab: 只断开连接, 标签保留以便稍后重连.
        let z_close_tab = c_args.get("keep_tab").and_then(Value::as_bool) != Some(true);
        self.disconnect_session(&z_id, c_actor, z_close_tab, false).await?;
        Ok(json!({"disconnected":true}))
    }

    async fn disconnect_session(
        &self,
        c_id: &str,
        c_actor: Actor,
        c_close_tab: bool,
        c_keep_transfers: bool,
    ) -> Result<()> {
        let (z_session, z_connection_id) = {
            let z_state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let z_tab = z_state.tabs.iter().find(|c| c.id == c_id).context("标签已关闭")?;
            (z_state.sessions.get(c_id).cloned(), z_tab.connection_id.clone())
        };
        let z_handle = if let Some(z_session) = z_session {
            if c_actor == Actor::Mcp {
                ensure!(self.mcp_permitted(&z_session.connection), "此连接未授权 MCP");
            }
            close_session(&z_session, "disconnected", false);
            z_session.handle.lock().unwrap_or_else(PoisonError::into_inner)
                .take()
        } else {
            // 已断开只剩标签: MCP 只能处理已授权连接的标签 (重连或关闭).
            if c_actor == Actor::Mcp {
                ensure!(z_connection_id.and_then(|c| self.store.get(&c).ok()).is_some_and(|c| c.mcp_allowed), "此连接未授权 MCP");
            }
            None
        };
        {
            let mut z_state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            for z_task in z_state.tasks.values() {
                if let Ok(mut z_task) = z_task.lock() {
                    if z_task.session_id == c_id && !(c_keep_transfers && matches!(z_task.kind.as_str(), "upload" | "download")) {
                        if let Some(z_cancel) = z_task.cancel.take() { let _ = z_cancel.send(()); }
                    }
                }
            }
            z_state.sessions.remove(c_id);
            if c_close_tab {
                z_state.tabs.retain(|c| c.id != c_id);
                if z_state.active_session_id.as_deref() == Some(c_id) {
                    z_state.active_session_id = z_state.tabs.last().map(|c| c.id.clone());
                }
            }
        }
        if let Some(z_handle) = z_handle {
            let _ = tokio::time::timeout(
                Duration::from_secs(2),
                z_handle.disconnect(Disconnect::ByApplication, "关闭连接", "zh-CN"),
            )
            .await;
        }
        Ok(())
    }

    async fn run_command(self: &Arc<Self>, c_args: Value, c_actor: Actor) -> Result<Value> {
        let z_session = self.session(required_string(&c_args, "session_id")?, c_actor)?;
        let z_command = required_string(&c_args, "command")?.to_owned();
        ensure!(
            !z_command.trim().is_empty() && z_command.len() <= MAX_INPUT,
            "命令为空或超过 16 KiB"
        );
        let timeout = c_args
            .get("timeout_seconds")
            .and_then(Value::as_u64)
            .unwrap_or(30)
            .clamp(1, 120);
        let max_output = c_args
            .get("max_output_bytes")
            .and_then(Value::as_u64)
            .unwrap_or(MAX_OUTPUT as u64)
            .min(MAX_COMMAND_OUTPUT as u64) as usize;
        let z_created = self.new_task(
            z_session,
            c_actor,
            "command",
            z_command.clone(),
            TaskSpec::Command {
                command: z_command,
                timeout,
                max_output,
            },
            string_arg(&c_args, "_client"),
        )?;
        // wait_seconds 内完成则直接返回输出, 省去再调 get_task; 本机调用 20 秒超时, 等待上限 15 秒.
        let z_wait = c_args.get("wait_seconds").and_then(Value::as_u64).unwrap_or(if c_actor == Actor::Mcp { 10 } else { 0 }).min(15);
        let Some(z_task_id) = z_created.get("task_id").and_then(Value::as_str).filter(|_| z_wait > 0) else {
            return Ok(z_created);
        };
        let z_task = self.task(z_task_id, c_actor)?;
        let z_deadline = Instant::now() + Duration::from_secs(z_wait);
        loop {
            {
                let l_task = z_task.lock().unwrap_or_else(PoisonError::into_inner);
                if matches!(l_task.status.as_str(), "completed" | "failed" | "cancelled") || Instant::now() >= z_deadline {
                    return Ok(serde_json::to_value(TaskView::from(&*l_task))?);
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn list_files(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        let z_session = self.session(required_string(&c_args, "session_id")?, c_actor)?;
        let z_dirs_only = c_args.get("dirs_only").and_then(Value::as_bool) == Some(true);
        let z_request = c_args
            .get("path")
            .and_then(Value::as_str)
            .unwrap_or(".")
            .to_owned();
        // 界面带上目录树已展开的节点时, 同时读取路径上尚未展开的上级目录.
        let z_expanded = c_args.get("tree_expanded").and_then(Value::as_array).filter(|_| c_actor == Actor::User)
            .map(|c| c.iter().filter_map(Value::as_str).map(str::to_owned).collect::<std::collections::HashSet<_>>());
        // 同一连接的文件操作排队执行; 前面的操作卡住时不无限等待, 报错后刷新即可重试.
        let z_permit = tokio::time::timeout(Duration::from_secs(30), z_session.file_slot.clone().acquire_owned())
            .await
            .context("等待其他文件操作完成超时")??;
        let z_handle = z_session.handle.lock().unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .cloned()
            .context("SSH 尚未连接")?;
        let raw = self.browse_sftp(&z_session, &z_handle).await?;
        // 请求尽量同时发出: 走代理时耗时取决于串行往返次数, 解析路径与打开目录并行, 各级上级目录也并行, 不随路径深度增加.
        // 放进独立任务: 外层被取消 (如 MCP 调用整体超时) 时仍会读完并关闭句柄, 复用的 SFTP 会话不会积累未关闭的目录句柄.
        // 读完先返回结果, 再关闭句柄; 名额到全部关闭才释放, 传输让出浏览通道时以此为准.
        let z_task_session = z_session.clone();
        let (z_result_tx, z_result_rx) = oneshot::channel();
        tokio::spawn(async move {
            let _z_permit = z_permit;
            let (z_resolved, z_opened) = tokio::join!(
                resolve_remote_path(&raw, z_request.clone()),
                open_remote_directory(&raw, &z_task_session, z_request),
            );
            let z_opened = match z_opened {
                Ok(l_handle) => l_handle,
                Err(err) => {
                    let _ = z_result_tx.send(Err(err));
                    return;
                }
            };
            let mut z_handles = vec![z_opened.clone()];
            let z_result = async {
                let l_path = z_resolved?;
                let l_tasks = missing_tree_ancestors(&l_path, z_expanded.as_ref())
                    .into_iter()
                    .map(|c| tokio::spawn(read_tree_directory(raw.clone(), z_task_session.clone(), c)))
                    .collect::<Vec<_>>();
                let l_listed = read_remote_entries(&raw, z_opened, z_dirs_only).await;
                // 目标目录读取失败也要等上级任务结束, 它们的句柄同样在这里关闭. 读不了的上级跳过, 不影响文件列表.
                let mut l_ancestors = Vec::new();
                for l_task in l_tasks {
                    let Ok((l_ancestor, l_handle)) = l_task.await else { continue; };
                    l_ancestors.extend(l_ancestor);
                    z_handles.extend(l_handle);
                }
                Ok::<_, anyhow::Error>((l_path, l_listed?, l_ancestors))
            }
            .await;
            let _ = z_result_tx.send(z_result);
            let z_closing = z_handles.into_iter()
                .map(|c| tokio::spawn(close_remote_directory(raw.clone(), z_task_session.clone(), c)))
                .collect::<Vec<_>>();
            for l_task in z_closing {
                let _ = l_task.await;
            }
        });
        let (z_path, (entries, truncated, owner_ids), z_ancestors) = z_result_rx.await.context("读取目录任务异常")??;
        // 目录树只需子目录名: 不查属主和时间, 也不占用文件列表的补全槽位.
        if z_dirs_only {
            let mut l_dirs = entries.into_iter().filter(|c| c["is_dir"].as_bool() == Some(true)).collect::<Vec<_>>();
            sort_remote_entries(&mut l_dirs);
            return Ok(json!({"session_id":z_session.id,"path":z_path,"entries":l_dirs,"truncated":truncated}));
        }
        // 用户名与时间换算各自开通道, 此时已不占名额, 慢时不挡住同一连接的其他目录操作.
        // 缓存命中时立即完成; 需要远端查询则先返回基础列表 (属主显示数字 ID, 时间按 UTC), 查询在后台继续,
        // 界面凭 details_token 取结果后就地更新这两列.
        // 补全任务只带名称、时间、属主三项, 不复制整份列表; 同一会话只保留最新目录的补全, 旧的直接取消, 释放内存和通道.
        z_session.file_details.lock().unwrap_or_else(PoisonError::into_inner).take();
        if let Some(l_old) = z_session.file_details_abort.lock().unwrap_or_else(PoisonError::into_inner).take() {
            l_old.abort();
        }
        let z_detail_session = z_session.clone();
        let mut z_detailed = entries
            .iter()
            .map(|c| json!({"name":c["name"],"modified_time":c["modified_time"],"owner":c["owner"]}))
            .collect::<Vec<_>>();
        let mut z_details = tokio::spawn(async move {
            resolve_file_owners(&z_detail_session, &z_handle, &owner_ids, &mut z_detailed).await;
            let l_times = resolve_file_times(&z_detail_session, &z_handle, &mut z_detailed).await;
            (z_detailed, l_times)
        });
        *z_session.file_details_abort.lock().unwrap_or_else(PoisonError::into_inner) = Some(z_details.abort_handle());
        let mut entries = entries;
        let ((z_server_timezone, z_time_hint), z_details_token) =
            match tokio::time::timeout(Duration::from_millis(100), &mut z_details).await {
                Ok(Ok((l_detailed, l_times))) => {
                    for (l_entry, l_detail) in entries.iter_mut().zip(l_detailed) {
                        l_entry["owner"] = l_detail["owner"].clone();
                        l_entry["modified_text"] = l_detail.get("modified_text").cloned().unwrap_or(Value::Null);
                    }
                    (l_times, None)
                }
                Ok(Err(_)) => ((None, None), None),
                Err(_) => {
                    let l_token = epoch_ms();
                    *z_session.file_details.lock().unwrap_or_else(PoisonError::into_inner) = Some((l_token, z_path.clone(), z_details));
                    ((None, None), Some(l_token))
                }
            };
        sort_remote_entries(&mut entries);
        // 分页: MCP 结果有大小上限, 默认每次 500 条, 用 offset 继续读取; 界面一次取完整列表.
        let z_total = entries.len();
        let z_offset = c_args.get("offset").and_then(Value::as_u64).unwrap_or(0) as usize;
        let z_limit = c_args.get("limit").and_then(Value::as_u64).map(|c| c as usize)
            .or((c_actor == Actor::Mcp).then_some(500))
            .unwrap_or(z_total);
        if z_offset > 0 || z_limit < z_total {
            entries = entries.into_iter().skip(z_offset).take(z_limit).collect();
        }
        Ok(json!({"session_id":z_session.id,"path":z_path,"entries":entries,"total":z_total,"offset":z_offset,"truncated":truncated,"server_timezone":z_server_timezone,"time_hint":z_time_hint,"details_token":z_details_token,"tree_ancestors":z_ancestors}))
    }

    async fn file_call(&self, c_method: &str, c_args: Value, c_actor: Actor) -> Result<Value> {
        match c_method {
            "list_files" => self.list_files(c_args, c_actor).await,
            "check_remote_names" => self.check_remote_names(c_args, c_actor).await,
            "set_permissions" => self.set_permissions(c_args, c_actor).await,
            "rename_file" => self.rename_file(c_args, c_actor).await,
            "delete_files" => self.delete_files(c_args, c_actor).await,
            _ => self.create_remote_entry(c_method, c_args, c_actor).await,
        }
    }

    // 上传前的重名检查: 只查要上传的几个名字是否已存在, 不读整个目录, 也不查属主和时间.
    async fn check_remote_names(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        let z_session = self.session(required_string(&c_args, "session_id")?, c_actor)?;
        let z_directory = required_string(&c_args, "directory")?.trim_end_matches('/').to_owned();
        let z_names: Vec<String> = serde_json::from_value(c_args.get("names").cloned().context("缺少 names")?)?;
        ensure!(z_names.len() <= 64, "一次最多检查 64 个名称");
        ensure!(z_names.iter().all(|c| !c.is_empty() && !c.contains('/')), "文件名无效");
        // 与其他文件操作一样全程持有名额, 等所有查询结束 (不再引用浏览通道) 才释放: 传输让出浏览通道时以此为准.
        // 各名称并发查询, 不逐个等待往返.
        let z_permit = z_session.file_slot.clone().acquire_owned().await?;
        let z_handle = z_session.handle.lock().unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .cloned()
            .context("SSH 尚未连接")?;
        let z_sftp = self.browse_sftp(&z_session, &z_handle).await?;
        let mut z_checks = tokio::task::JoinSet::new();
        for z_name in z_names {
            let (l_sftp, l_path) = (z_sftp.clone(), format!("{z_directory}/{z_name}"));
            z_checks.spawn(async move { (z_name, tokio::time::timeout(Duration::from_secs(10), l_sftp.lstat(l_path)).await) });
        }
        drop(z_sftp);
        // 先等全部查询结束再判断结果, 中途出错也不会留下仍在使用通道的查询.
        let z_results = z_checks.join_all().await;
        drop(z_permit);
        let mut z_existing = Vec::new();
        for l_done in z_results {
            match l_done {
                (l_name, Ok(Ok(_))) => z_existing.push(l_name),
                (_, Ok(Err(russh_sftp::client::error::Error::Status(l_status))))
                    if l_status.status_code == russh_sftp::protocol::StatusCode::NoSuchFile => {}
                (_, Ok(Err(err))) => return Err(err.into()),
                (_, Err(_)) => bail!("检查远端文件超时"),
            }
        }
        Ok(json!({"existing":z_existing}))
    }

    // 取目录读取的属主、时间补全结果, 只返回这两项, 界面就地更新, 不重读目录也不清空选择.
    async fn file_details(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        let z_session = self.session(required_string(&c_args, "session_id")?, c_actor)?;
        let z_token = c_args.get("token").and_then(Value::as_u64).context("缺少 token")?;
        let (z_path, z_task) = {
            let mut z_slot = z_session.file_details.lock().unwrap_or_else(PoisonError::into_inner);
            ensure!(z_slot.as_ref().is_some_and(|c| c.0 == z_token), "目录已刷新");
            let (_, l_path, l_task) = z_slot.take().context("目录已刷新")?;
            (l_path, l_task)
        };
        let mut z_task = z_task;
        let (z_entries, (z_timezone, z_hint)) = match tokio::time::timeout(Duration::from_secs(30), &mut z_task).await {
            Ok(l_result) => l_result.context("查询属主与时间异常")?,
            Err(_) => {
                z_task.abort();
                bail!("查询属主与时间超时");
            }
        };
        let z_entries = z_entries
            .iter()
            .map(|c| json!({"name":c["name"],"owner":c["owner"],"modified_text":c.get("modified_text")}))
            .collect::<Vec<_>>();
        Ok(json!({"session_id":z_session.id,"path":z_path,"entries":z_entries,"server_timezone":z_timezone,"time_hint":z_hint}))
    }

    async fn create_remote_entry(
        &self,
        c_method: &str,
        c_args: Value,
        c_actor: Actor,
    ) -> Result<Value> {
        require_user(c_actor)?;
        let z_session_id = required_string(&c_args, "session_id")?;
        let z_parent = required_string(&c_args, "path")?;
        let z_name = required_string(&c_args, "name")?;
        ensure!(z_parent.len() <= 4096, "远端父目录路径过长");
        ensure!(
            !z_parent.chars().any(|c| c.is_control()),
            "远端父目录路径包含控制字符"
        );
        ensure!(
            z_name.len() <= 255
                && !z_name.trim().is_empty()
                && z_name != "."
                && z_name != ".."
                && !z_name.contains('/')
                && !z_name.chars().any(|c| c.is_control()),
            "新名称不能为空、为.或..、含路径分隔符或控制字符, 且最多255字节"
        );
        let z_session = self.session(z_session_id, c_actor)?;
        let _z_permit = z_session.file_slot.clone().acquire_owned().await?;
        let z_handle = z_session.handle.lock().unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .cloned()
            .context("SSH 尚未连接")?;
        let z_sftp = self.browse_sftp(&z_session, &z_handle).await?;
        let z_resolved =
            tokio::time::timeout(Duration::from_secs(10), z_sftp.realpath(z_parent.to_owned()))
                .await
                .context("解析远端父目录超时")??;
        let z_directory = z_resolved
            .files
            .into_iter()
            .next()
            .context("远端父目录没有返回实际路径")?
            .filename;
        ensure!(
            !z_directory.is_empty()
                && z_directory.len() <= 4096
                && !z_directory.chars().any(|c| c.is_control()),
            "远端父目录返回的实际路径无效"
        );
        let z_path = format!("{}/{z_name}", z_directory.trim_end_matches('/'));
        ensure!(z_path.len() <= 4096, "新路径过长");
        match tokio::time::timeout(Duration::from_secs(5), z_sftp.lstat(z_path.clone())).await {
            Ok(Ok(_)) => bail!("目标名称已存在"),
            Ok(Err(russh_sftp::client::error::Error::Status(status)))
                if status.status_code == russh_sftp::protocol::StatusCode::NoSuchFile => {}
            Ok(Err(err)) => return Err(err.into()),
            Err(_) => bail!("检查目标名称超时"),
        }
        if c_method == "create_directory" {
            tokio::time::timeout(
                Duration::from_secs(10),
                z_sftp.mkdir(
                    z_path.clone(),
                    russh_sftp::protocol::FileAttributes::default(),
                ),
            )
            .await
            .context("新建远端目录超时, 请刷新目录确认结果")??;
        } else {
            // 独占创建由服务器检查, 即使预检查后出现同名项也不覆盖.
            let z_flags = russh_sftp::protocol::OpenFlags::CREATE
                | russh_sftp::protocol::OpenFlags::WRITE
                | russh_sftp::protocol::OpenFlags::EXCLUDE;
            let z_file = tokio::time::timeout(
                Duration::from_secs(10),
                z_sftp.open(
                    z_path.clone(),
                    z_flags,
                    russh_sftp::protocol::FileAttributes::default(),
                ),
            )
            .await
            .context("新建远端文件超时, 请刷新目录确认结果")??;
            tokio::time::timeout(Duration::from_secs(3), z_sftp.close(z_file.handle))
                .await
                .context("空文件已创建, 关闭文件超时")?
                .context("空文件已创建, 关闭文件失败")?;
        }
        Ok(json!({"session_id":z_session.id,"path":z_path,"directory":z_directory}))
    }

    async fn set_permissions(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        require_user(c_actor)?;
        let z_session_id = required_string(&c_args, "session_id")?;
        let z_path = required_string(&c_args, "path")?;
        ensure!(
            !z_path.is_empty() && z_path.len() <= 4096,
            "远端路径长度无效"
        );
        ensure!(
            !z_path.chars().any(|c| c.is_control()),
            "远端路径包含控制字符"
        );
        let z_permissions = c_args
            .get("permissions")
            .and_then(Value::as_u64)
            .context("权限值需要0到0777的整数")?;
        ensure!(z_permissions <= 0o777, "权限值超出0777");
        let z_session = self.session(z_session_id, c_actor)?;
        let _z_permit = z_session.file_slot.clone().acquire_owned().await?;
        let z_handle = z_session.handle.lock().unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .cloned()
            .context("SSH 尚未连接")?;
        let z_sftp = self.browse_sftp(&z_session, &z_handle).await?;
        let z_attrs =
            tokio::time::timeout(Duration::from_secs(10), z_sftp.lstat(z_path.to_owned()))
                .await
                .context("检查远端文件超时")??;
        ensure!(!z_attrs.attrs.is_symlink(), "拒绝修改符号链接权限");
        let z_file_mode = z_attrs
            .attrs
            .permissions
            .context("远端文件未提供类型权限信息")?;
        let mut z_set_attrs = russh_sftp::protocol::FileAttributes::default();
        // 界面只编辑普通读写执行位, 保留未展示的文件类型及特殊权限位.
        z_set_attrs.permissions = Some((z_file_mode & 0o177000) | z_permissions as u32);
        tokio::time::timeout(
            Duration::from_secs(10),
            z_sftp.setstat(z_path.to_owned(), z_set_attrs),
        )
        .await
        .context("修改远端权限超时")??;
        Ok(json!({"session_id":z_session.id,"path":z_path,"permissions":z_permissions}))
    }

    async fn rename_file(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        require_user(c_actor)?;
        let z_session_id = required_string(&c_args, "session_id")?;
        let z_path = required_string(&c_args, "path")?;
        let z_new_name = required_string(&c_args, "new_name")?;
        validate_remote_file_path(z_path)?;
        ensure!(
            z_new_name.len() <= 255 && !z_new_name.trim().is_empty(),
            "新名称长度或内容无效"
        );
        ensure!(
            z_new_name != "."
                && z_new_name != ".."
                && !z_new_name.contains('/')
                && !z_new_name.chars().any(|c| c.is_control()),
            "新名称无效"
        );
        let z_parent = z_path
            .rsplit_once('/')
            .map(|(parent, _)| parent)
            .unwrap_or("");
        let z_new_path = if z_parent.is_empty() && !z_path.starts_with('/') {
            z_new_name.to_owned()
        } else if z_parent.is_empty() || z_parent == "/" {
            format!("/{z_new_name}")
        } else {
            format!("{z_parent}/{z_new_name}")
        };
        ensure!(z_new_path.len() <= 4096, "新路径过长");
        let z_session = self.session(z_session_id, c_actor)?;
        let _z_permit = z_session.file_slot.clone().acquire_owned().await?;
        let z_handle = z_session.handle.lock().unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .cloned()
            .context("SSH 尚未连接")?;
        let z_sftp = self.browse_sftp(&z_session, &z_handle).await?;
        let z_target_exists =
            tokio::time::timeout(Duration::from_secs(5), z_sftp.lstat(z_new_path.clone())).await;
        match z_target_exists {
            Ok(Ok(_)) => bail!("目标名称已存在"),
            Ok(Err(russh_sftp::client::error::Error::Status(status)))
                if status.status_code == russh_sftp::protocol::StatusCode::NoSuchFile => {}
            Ok(Err(err)) => return Err(err.into()),
            Err(_) => bail!("检查目标名称超时"),
        }
        tokio::time::timeout(
            Duration::from_secs(10),
            z_sftp.rename(z_path.to_owned(), z_new_path.clone()),
        )
        .await
        .context("重命名远端文件超时")??;
        Ok(json!({"session_id":z_session.id,"path":z_path,"new_path":z_new_path}))
    }

    async fn delete_files(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        // 同时在途的删除请求数: 逐项删除每项要两次往返, 并发后大批量删除不必逐个等待.
        const DELETE_CONCURRENCY: usize = 16;
        require_user(c_actor)?;
        let z_session_id = required_string(&c_args, "session_id")?;
        let z_paths = c_args
            .get("paths")
            .and_then(Value::as_array)
            .context("paths需要数组")?;
        ensure!(
            !z_paths.is_empty() && z_paths.len() <= MAX_DELETE_ITEMS,
            "一次最多删除 {MAX_DELETE_ITEMS} 项"
        );
        let z_session = self.session(z_session_id, c_actor)?;
        let _z_permit = z_session.file_slot.clone().acquire_owned().await?;
        let z_handle = z_session.handle.lock().unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .cloned()
            .context("SSH 尚未连接")?;
        let z_sftp = self.browse_sftp(&z_session, &z_handle).await?;
        // 整体期限随项数增加; 到期后未开始的项标记为未处理.
        let z_deadline = tokio::time::Instant::now() + Duration::from_secs(30) + Duration::from_millis(100) * z_paths.len() as u32;
        let mut z_results = vec![Value::Null; z_paths.len()];
        let mut z_running = tokio::task::JoinSet::new();
        let mut z_next = 0;
        while z_next < z_paths.len() || !z_running.is_empty() {
            while z_next < z_paths.len() && z_running.len() < DELETE_CONCURRENCY {
                let i = z_next;
                z_next += 1;
                let Some(l_path) = z_paths[i].as_str() else {
                    z_results[i] = json!({"path":null,"success":false,"error":"路径必须是字符串"});
                    continue;
                };
                if let Err(err) = validate_remote_file_path(l_path) {
                    z_results[i] = json!({"path":l_path,"success":false,"error":safe_error(&err.to_string())});
                    continue;
                }
                let l_remaining = z_deadline.saturating_duration_since(tokio::time::Instant::now());
                if l_remaining.is_zero() {
                    z_results[i] = json!({"path":l_path,"success":false,"error":"整体操作超时，未处理"});
                    continue;
                }
                let (l_sftp, l_path) = (z_sftp.clone(), l_path.to_owned());
                z_running.spawn(async move {
                    let l_operation = async {
                        let l_attrs = l_sftp.lstat(l_path.clone()).await?;
                        if l_attrs.attrs.is_dir() {
                            l_sftp.rmdir(l_path.clone()).await?;
                        } else if l_attrs.attrs.is_symlink() || l_attrs.attrs.is_regular() {
                            l_sftp.remove(l_path.clone()).await?;
                        } else {
                            bail!("仅支持普通文件、符号链接或空目录")
                        }
                        Ok::<(), anyhow::Error>(())
                    };
                    let l_result = match tokio::time::timeout(l_remaining.min(Duration::from_secs(10)), l_operation).await {
                        Ok(Ok(())) => json!({"path":l_path,"success":true}),
                        Ok(Err(err)) => json!({"path":l_path,"success":false,"error":safe_error(&err.to_string())}),
                        Err(_) => json!({"path":l_path,"success":false,"error":"删除超时"}),
                    };
                    (i, l_result)
                });
            }
            if let Some(l_done) = z_running.join_next().await {
                let (i, l_result) = l_done.context("删除任务异常")?;
                z_results[i] = l_result;
            }
        }
        Ok(json!({"session_id":z_session.id,"results":z_results}))
    }

    // 快速删除: 由服务器执行 rm -rf, 可递归删除目录, 大量小文件时远快于逐项 SFTP 删除.
    // 路径经标准输入以 NUL 分隔交给 xargs, 不拼入命令行, 文件名中的空格、引号不会被 shell 解释.
    async fn delete_files_fast(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        require_user(c_actor)?;
        let z_session = self.session(required_string(&c_args, "session_id")?, c_actor)?;
        let z_paths = c_args
            .get("paths")
            .and_then(Value::as_array)
            .context("paths需要数组")?
            .iter()
            .map(|c| c.as_str().context("路径必须是字符串"))
            .collect::<Result<Vec<_>>>()?;
        ensure!(!z_paths.is_empty() && z_paths.len() <= MAX_DELETE_ITEMS, "一次最多删除 {MAX_DELETE_ITEMS} 项");
        let mut z_input = Vec::new();
        for z_path in &z_paths {
            validate_remote_file_path(z_path)?;
            ensure!(z_path.starts_with('/'), "快速删除仅接受绝对路径");
            z_input.extend_from_slice(z_path.as_bytes());
            z_input.push(0);
        }
        // rm 只给出整体退出码, 无法区分单项成败; 失败时整批标记并附第一行错误.
        let z_error = exec_with_input(&z_session, "LC_ALL=C exec xargs -0 rm -rf --", &z_input, 300)
            .await
            .context("快速删除未完成, 服务器可能仍在删除, 请稍后刷新")?;
        let z_results = z_paths
            .iter()
            .map(|c| match &z_error {
                None => json!({"path":c,"success":true}),
                Some(l_error) => json!({"path":c,"success":false,"error":l_error}),
            })
            .collect::<Vec<_>>();
        Ok(json!({"session_id":z_session.id,"results":z_results}))
    }

    // 在同一目录创建副本: 目标名由界面按现有文件计算, cp -n 保证不覆盖已存在的文件.
    async fn copy_file(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        require_user(c_actor)?;
        let z_session = self.session(required_string(&c_args, "session_id")?, c_actor)?;
        let z_source = required_string(&c_args, "path")?;
        let z_target = required_string(&c_args, "new_path")?;
        for z_path in [z_source, z_target] {
            validate_remote_file_path(z_path)?;
            ensure!(z_path.starts_with('/'), "仅接受绝对路径");
        }
        let z_input = format!("{z_source}\0{z_target}\0");
        if let Some(z_error) = exec_with_input(&z_session, "LC_ALL=C exec xargs -0 cp -a -n --", z_input.as_bytes(), 300).await? {
            bail!("创建副本失败: {z_error}");
        }
        Ok(json!({"session_id":z_session.id,"path":z_target}))
    }

    fn transfer(self: &Arc<Self>, c_method: &str, c_args: Value, c_actor: Actor) -> Result<Value> {
        let z_upload = c_method.starts_with("upload");
        let z_directory = c_method.ends_with("_directory");
        // 文件夹传输只开放给界面, MCP 工具范围不变.
        if z_directory {
            require_user(c_actor)?;
        }
        let z_session = self.session(required_string(&c_args, "session_id")?, c_actor)?;
        let local = PathBuf::from(required_string(&c_args, "local_path")?);
        // 相对路径会按程序工作目录解析, 文件落到用户想不到的位置; Windows 上还要求带盘符.
        ensure!(local.is_absolute(), "本机路径必须是绝对路径");
        let remote = required_string(&c_args, "remote_path")?.to_owned();
        validate_remote_file_path(&remote)?;
        let overwrite = c_args.get("overwrite")
                .and_then(Value::as_bool)
                .unwrap_or(false);
        let z_total = if z_upload {
            let l_metadata = local.metadata().context("读取上传源文件信息失败")?;
            ensure!(if z_directory { l_metadata.is_dir() } else { l_metadata.is_file() }, "上传源不存在");
            (!z_directory).then_some(l_metadata.len())
        } else if c_actor == Actor::User && !z_directory {
            // 界面列表的长度仅用于排队进度, 开始传输后以实际文件信息为准.
            c_args.get("size_hint").and_then(Value::as_u64)
        } else {
            None
        };
        if !z_upload {
            ensure!(!local.exists() || overwrite, "下载目标已存在");
        }
        self.new_task(
            z_session,
            c_actor,
            if z_upload {
                "upload"
            } else {
                "download"
            },
            remote.clone(),
            TaskSpec::Transfer {
                upload: z_upload,
                local,
                remote,
                overwrite,
                total: z_total,
                directory: z_directory,
            },
            string_arg(&c_args, "_client"),
        )
    }

    fn cancel_task(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        let z_task = self.task(required_string(&c_args, "task_id")?, c_actor)?;
        if let Some(tx) = z_task.lock().unwrap_or_else(PoisonError::into_inner)
            .cancel
            .take()
        {
            let _ = tx.send(());
        }
        Ok(json!({"cancel_requested":true}))
    }

    fn get_task(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        let z_task = self.task(required_string(&c_args, "task_id")?, c_actor)?;
        let mut z_view = TaskView::from(&*z_task.lock().unwrap_or_else(PoisonError::into_inner));
        // 界面任务输出最多 1 MiB, 转义后会超过 MCP 结果上限而无法查询; MCP 每路只取前 64 KiB, 与 run_command 默认一致.
        if c_actor == Actor::Mcp {
            for l_text in [&mut z_view.stdout, &mut z_view.stderr] {
                if l_text.len() > MAX_OUTPUT {
                    l_text.truncate(l_text.floor_char_boundary(MAX_OUTPUT));
                    z_view.truncated = true;
                }
            }
        }
        Ok(serde_json::to_value(z_view)?)
    }

    fn new_task(
        self: &Arc<Self>,
        c_session: Arc<LiveSession>,
        c_actor: Actor,
        c_kind: &str,
        c_label: String,
        c_spec: TaskSpec,
        c_client: String,
    ) -> Result<Value> {
        let mut z_state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if z_state.tasks.len() >= MAX_TASKS {
            let z_finished = z_state
                .tasks
                .iter()
                .filter_map(|(id, task)| {
                    task.lock().ok().and_then(|task| {
                        if matches!(task.status.as_str(), "completed" | "failed" | "cancelled") {
                            Some((task.created_at, id.clone()))
                        } else {
                            None
                        }
                    })
                })
                .min_by_key(|(created_at, _)| *created_at)
                .map(|(_, id)| id);
            if let Some(id) = z_finished {
                z_state.tasks.remove(&id);
            }
        }
        ensure!(z_state.tasks.len() < MAX_TASKS, "任务队列已满");
        let z_id = uuid::Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();
        let z_task = Arc::new(Mutex::new(Task {
            task_id: z_id.clone(),
            session_id: c_session.id.clone(),
            connection: c_session.connection.clone(),
            created_at_ms: epoch_ms(),
            created_at: Instant::now(),
            source: actor_name(c_actor).into(),
            client: c_client,
            kind: c_kind.into(),
            label: c_label,
            local: match &c_spec {
                TaskSpec::Transfer { local, .. } => local.display().to_string(),
                _ => String::new(),
            },
            started_at_ms: None,
            finished_at_ms: None,
            status: "queued".into(),
            bytes: 0,
            total: match &c_spec {
                TaskSpec::Transfer { total, .. } => *total,
                _ => None,
            },
            speed_bytes_per_sec: 0,
            last_progress_bytes: 0,
            last_progress_at: Instant::now(),
            directory: matches!(c_spec, TaskSpec::Transfer { directory: true, .. }),
            files_done: 0,
            files_total: 0,
            current: String::new(),
            file_bytes: 0,
            file_total: 0,
            base_bytes: 0,
            stdout: String::new(),
            stderr: String::new(),
            truncated: false,
            exit_code: None,
            error: None,
            cancel: Some(tx),
        }));
        z_state.tasks.insert(z_id.clone(), z_task.clone());
        drop(z_state);
        let z_self = self.clone();
        tokio::spawn(async move {
            let mut z_cancel_rx = rx;
            let permit = tokio::select! { biased; _ = &mut z_cancel_rx => { mark_task(&z_task, "cancelled", None); return; }, p = (if matches!(c_spec, TaskSpec::Transfer { .. }) { &z_self.transfer_slots } else { &z_self.command_slots }).clone().acquire_owned() => match p { Ok(p) => p, Err(_) => return } };
            if c_actor == Actor::Mcp
                && !z_self.mcp_permitted(&c_session.connection)
            {
                mark_task(&z_task, "failed", Some("MCP 授权已撤销".into()));
                return;
            }
            // 传输先进入准备阶段 (建立连接、初始化 SFTP、检查和打开文件), 开始收发数据时才转为进行中并开始测速.
            mark_task(&z_task, if matches!(c_spec, TaskSpec::Transfer { .. }) { "preparing" } else { "running" }, None);
            let z_run = z_self
                .run_task(&c_session, &z_task, c_spec, &mut z_cancel_rx)
                .await;
            drop(permit);
            match z_run {
                Ok(()) => mark_task(&z_task, "completed", None),
                // 外层可能附加了连接断开原因, 按最内层判断是否为取消; 错误保留完整原因链 (含系统错误码), 便于判断失败原因.
                Err(err) => mark_task(
                    &z_task,
                    if err.root_cause().to_string() == "任务已取消" {
                        "cancelled"
                    } else {
                        "failed"
                    },
                    Some(safe_error(&format!("{err:#}"))),
                ),
            }
            z_self.trim_task_outputs();
        });
        Ok(json!({"task_id":z_id}))
    }

    // 已结束任务的输出合计不超过预算: 从最新的开始保留, 超出部分释放输出文字, 任务状态、退出码等保留.
    fn trim_task_outputs(&self) {
        let z_state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let mut z_done = z_state
            .tasks
            .values()
            .filter_map(|c| c.lock().ok().filter(|l| matches!(l.status.as_str(), "completed" | "failed" | "cancelled")).map(|l| (l.created_at, c.clone())))
            .collect::<Vec<_>>();
        z_done.sort_by_key(|c| std::cmp::Reverse(c.0));
        let mut z_kept = 0usize;
        for (_, l_task) in z_done {
            let Ok(mut l_task) = l_task.lock() else { continue };
            z_kept += l_task.stdout.len() + l_task.stderr.len();
            if z_kept > TASK_OUTPUT_BUDGET && !(l_task.stdout.is_empty() && l_task.stderr.is_empty()) {
                l_task.stdout = "(输出已释放: 只保留最近任务的输出)".into();
                l_task.stderr = String::new();
            }
        }
    }

    async fn run_task(
        &self,
        c_session: &Arc<LiveSession>,
        c_task: &Arc<Mutex<Task>>,
        c_spec: TaskSpec,
        c_cancel: &mut oneshot::Receiver<()>,
    ) -> Result<()> {
        match c_spec {
            TaskSpec::Command {
                command,
                timeout,
                max_output,
            } => {
                let z_handle = c_session.handle.lock().unwrap_or_else(PoisonError::into_inner)
                    .as_ref()
                    .cloned()
                    .context("SSH 尚未连接")?;
                let mut channel = tokio::select! {
                    biased;
                    _ = &mut *c_cancel => bail!("任务已取消"),
                    z_result = open_channel(&z_handle, 5) => ClosingChannel(Some(z_result.context("打开命令通道超时")?)),
                };
                let z_started = tokio::select! {
                    biased;
                    _ = &mut *c_cancel => false,
                    z_result = tokio::time::timeout(Duration::from_secs(10), channel.exec(true, command)) => { z_result.context("执行命令请求超时")??; true },
                };
                if !z_started {
                    channel.shutdown().await;
                    bail!("任务已取消");
                }
                let mut stdout = Vec::new();
                let mut stderr = Vec::new();
                let mut code = None;
                let mut z_signal = None;
                let mut kept = 0usize;
                let mut truncated = false;
                let mut published = Instant::now();
                let result = tokio::time::timeout(Duration::from_secs(timeout), async {
                    // 没有后续输入接口, 立即结束标准输入 (同 ssh -n), 读取 stdin 的命令 (wc -c 等) 不会一直等到超时.
                    // 网络堵塞时发送也可能等待, 同样受取消和命令期限控制.
                    tokio::select! {
                        _ = &mut *c_cancel => return false,
                        _ = channel.eof() => {}
                    }
                    loop {
                        tokio::select! {
                            _ = &mut *c_cancel => break false,
                            msg = channel.wait() => match msg {
                                Some(ChannelMsg::Data { data }) => {
                                    let (n, cut) = append_limited(&mut stdout, &data, max_output.saturating_sub(kept)); kept += n; truncated |= cut;
                                }
                                Some(ChannelMsg::ExtendedData { data, .. }) => {
                                    let (n, cut) = append_limited(&mut stderr, &data, max_output.saturating_sub(kept)); kept += n; truncated |= cut;
                                }
                                Some(ChannelMsg::ExitStatus { exit_status }) => code = Some(exit_status as i32),
                                Some(ChannelMsg::ExitSignal { signal_name, error_message, .. }) => z_signal = Some(format!("{signal_name:?}: {error_message}")),
                                // EOF 只代表没有更多输出, 退出码仍可能在随后到达.
                                Some(ChannelMsg::Eof) => {},
                                Some(ChannelMsg::Close) | None => break true,
                                Some(_) => {}
                            }
                        }
                        if published.elapsed() >= Duration::from_millis(100) {
                            publish_command_output(c_task, &stdout, &stderr, truncated);
                            published = Instant::now();
                        }
                    }
                }).await;
                match result {
                    Err(_) => {
                        channel.shutdown().await;
                        publish_command_output(c_task, &stdout, &stderr, truncated);
                        bail!("远程命令超时");
                    }
                    Ok(false) => {
                        channel.shutdown().await;
                        publish_command_output(c_task, &stdout, &stderr, truncated);
                        bail!("任务已取消");
                    }
                    Ok(true) => {}
                }
                publish_command_output(c_task, &stdout, &stderr, truncated);
                if let Ok(mut task) = c_task.lock() {
                    task.exit_code = code;
                }
                if let Some(z_signal) = z_signal { bail!("远程命令被信号终止: {}", safe_error(&z_signal)); }
                ensure!(code.is_some(), "命令通道已关闭, 未收到退出码");
                Ok(())
            }
            TaskSpec::Transfer {
                upload,
                local,
                remote,
                overwrite,
                directory,
                ..
            } => {
                let mut z_session = c_session.clone();
                if directory {
                    self.run_directory_transfer(&mut z_session, c_task, upload, local, remote, overwrite, c_cancel).await
                } else {
                    self.transfer_file(&mut z_session, c_task, upload, local, remote, overwrite, c_cancel).await
                }
            }
        }
    }

    // 传输一个文件, 网络中断时自动重连续传; 标签重连后 c_session 换成新会话, 文件夹任务的后续文件沿用.
    async fn transfer_file(
        &self,
        c_session: &mut Arc<LiveSession>,
        c_task: &Arc<Mutex<Task>>,
        upload: bool,
        local: PathBuf,
        remote: String,
        overwrite: bool,
        c_cancel: &mut oneshot::Receiver<()>,
    ) -> Result<()> {
        let z_part_created = AtomicBool::new(false);
        let z_committing = AtomicBool::new(false);
        // 断点: (从头连续可靠写入的字节数, 首次打开时源文件的 (长度, 修改时间)); 重试时从断点继续, 并确认源文件未变.
        let z_resume = Mutex::new((0u64, None));
        // 标签重连后同一标签 ID 换成新会话, 重试时改用新会话.
        let mut z_session = c_session.clone();
        let mut z_failures = 0u32;
        let z_result = loop {
            let l_before = z_resume.lock().unwrap_or_else(PoisonError::into_inner).0;
            let mut l_transfer = Box::pin(self.run_transfer(&z_session, c_task, upload, local.clone(), remote.clone(), overwrite, &z_part_created, &z_committing, &z_resume));
            // 进入提交阶段 (改名落盘) 后取消不再中断: 提交会照常完成, 等它结束才能如实报告结果并刷新目录.
            let l_result = tokio::select! {
                r = &mut l_transfer => r,
                _ = &mut *c_cancel => if z_committing.load(Ordering::Acquire) { l_transfer.as_mut().await } else { Err(anyhow::anyhow!("任务已取消")) },
            };
            // 先释放传输占用的 SFTP 通道再重试或清理: 每条连接只允许一个通道的服务器上, 之后才能打开新通道.
            drop(l_transfer);
            // 通道关闭类错误本身看不出原因, 附上连接断开的原因 (传输连接优先).
            let Err(err) = l_result else { break Ok(()) };
            let err = {
                let l_errors = z_session.link_errors.lock().unwrap_or_else(PoisonError::into_inner);
                match (&l_errors[1], &l_errors[0]) {
                    (Some(l_reason), _) => err.context(format!("传输连接已断开: {l_reason}")),
                    (None, Some(l_reason)) => err.context(format!("连接已断开: {l_reason}")),
                    _ => err,
                }
            };
            // 取消、提交阶段出错 (临时文件已归提交处理) 和服务器明确拒绝、本地读写等非网络错误不重试.
            if err.root_cause().to_string() == "任务已取消" || z_committing.load(Ordering::Acquire) || !transfer_retryable(&z_session, &err) {
                break Err(err);
            }
            // 本次有新进度就重新计数: 长时间传输中偶发的多次中断都能恢复, 连续失败才放弃.
            if z_resume.lock().unwrap_or_else(PoisonError::into_inner).0 > l_before {
                z_failures = 0;
            }
            z_failures += 1;
            if z_failures > TRANSFER_RETRIES {
                break Err(err.context(format!("自动重连 {TRANSFER_RETRIES} 次仍失败")));
            }
            mark_task(c_task, "retrying", Some(safe_error(&format!("{err:#}"))));
            // 等待逐次加长 (2、4、8、16 秒, 之后每 30 秒), 合计约 4 分钟; 等待期间可取消.
            let l_wait = Duration::from_secs((1u64 << z_failures.min(5)).min(30));
            tokio::select! {
                biased;
                _ = &mut *c_cancel => break Err(anyhow::anyhow!("任务已取消")),
                _ = tokio::time::sleep(l_wait) => {}
            }
            // 标签已重连: 只在仍是同一服务器和登录身份时改用新会话, MCP 任务还要求新连接仍有授权.
            // 重连过程中旧会话已移除、新会话尚未登记时沿用旧会话, 本次失败后继续等待.
            let l_current = self.state.lock().unwrap_or_else(PoisonError::into_inner).sessions.get(&z_session.id).cloned();
            if let Some(l_current) = l_current.filter(|c| !Arc::ptr_eq(c, &z_session)) {
                let (l_same, l_mcp) = {
                    let l_task = c_task.lock().unwrap_or_else(PoisonError::into_inner);
                    let (l_old, l_new) = (&l_task.connection, &l_current.connection);
                    (l_old.id == l_new.id && l_old.host == l_new.host && l_old.port == l_new.port && l_old.username == l_new.username && l_old.proxy_id == l_new.proxy_id, l_task.source == "mcp")
                };
                if !l_same {
                    break Err(err.context("标签已重新连接到其他服务器或账号, 无法续传"));
                }
                if l_mcp && !self.mcp_permitted(&l_current.connection) {
                    break Err(anyhow::anyhow!("MCP 授权已撤销"));
                }
                z_session = l_current;
            }
            mark_task(c_task, "preparing", None);
        };
        // 失败或取消时只清理本任务创建的临时文件 (取消会直接丢弃传输过程, 只能在此处理); 远端清理尽力而为.
        if z_result.is_err() && z_part_created.load(Ordering::Acquire) {
            let l_tag = c_task.lock().unwrap_or_else(PoisonError::into_inner).task_id.chars().take(8).collect::<String>();
            if upload {
                if let Ok(Ok((l_raw, _, _))) = tokio::time::timeout(Duration::from_secs(10), self.open_transfer_raw(&z_session, c_task)).await {
                    let _ = tokio::time::timeout(Duration::from_secs(10), l_raw.remove(part_name(&remote, &l_tag))).await;
                }
            } else {
                let _ = tokio::fs::remove_file(local_part_path(&local, &l_tag)).await;
            }
        }
        *c_session = z_session;
        z_result
    }

    // 文件夹占一个任务: 先扫描并建好目录结构, 再逐个文件走单文件传输 (临时文件、续传、自动重连都相同), 不受任务队列上限影响.
    // 单个文件失败不影响其余文件, 结束时汇总; 取消或连接恢复不了时才停止, 已完成的文件保留.
    async fn run_directory_transfer(
        &self,
        c_session: &mut Arc<LiveSession>,
        c_task: &Arc<Mutex<Task>>,
        c_upload: bool,
        c_local: PathBuf,
        c_remote: String,
        c_overwrite: bool,
        c_cancel: &mut oneshot::Receiver<()>,
    ) -> Result<()> {
        let (z_files, z_notes) = tokio::select! {
            biased;
            _ = &mut *c_cancel => bail!("任务已取消"),
            l_result = self.prepare_directory(c_session, c_task, c_upload, &c_local, &c_remote, c_overwrite) => l_result?,
        };
        {
            let mut l_task = c_task.lock().unwrap_or_else(PoisonError::into_inner);
            l_task.files_total = z_files.len() as u64;
            l_task.total = Some(z_files.iter().map(|c| c.2).sum());
            l_task.stderr = z_notes;
        }
        mark_task(c_task, "running", None);
        let mut z_failed = 0;
        let mut z_first_error = String::new();
        for (z_local, z_remote, z_size) in z_files {
            let z_name = z_remote[c_remote.len() + 1..].to_owned();
            {
                let mut l_task = c_task.lock().unwrap_or_else(PoisonError::into_inner);
                // 提交阶段收到的取消不打断当时的文件, 但接收端已被消耗不能再等待; 发送端被取走即表示已请求取消.
                ensure!(l_task.cancel.is_some(), "任务已取消");
                l_task.current = z_name.clone();
                l_task.file_bytes = 0;
                l_task.file_total = z_size;
            }
            let l_result = self.transfer_file(c_session, c_task, c_upload, z_local, z_remote, c_overwrite, c_cancel).await;
            if let Err(err) = &l_result {
                // 重连次数已用尽的网络错误, 后面的文件同样传不了.
                if err.root_cause().to_string() == "任务已取消" || transfer_retryable(c_session, err) {
                    return l_result;
                }
            }
            {
                let mut l_task = c_task.lock().unwrap_or_else(PoisonError::into_inner);
                match l_result {
                    Ok(()) => {
                        l_task.base_bytes += l_task.file_total;
                        l_task.files_done += 1;
                    }
                    Err(err) => {
                        let l_error = safe_error(&format!("{err:#}"));
                        directory_note(&mut l_task.stderr, format!("失败 {z_name}: {l_error}"));
                        if z_failed == 0 {
                            z_first_error = l_error;
                        }
                        z_failed += 1;
                        l_task.total = l_task.total.map(|c| c.saturating_sub(z_size));
                    }
                }
                l_task.bytes = l_task.base_bytes;
            }
            // 失败的文件可能停在准备或重连状态.
            mark_task(c_task, "running", None);
        }
        ensure!(z_failed == 0, "{z_failed} 个文件传输失败, 其余文件已完成, 详见错误输出. 首个错误: {z_first_error}");
        Ok(())
    }

    // 扫描文件夹并建好目标目录结构, 返回文件列表 (本机路径, 远端路径, 大小) 与跳过项说明.
    // 符号链接不跟随, 避免循环和传出所选目录; 下载时目标的上级目录已由用户选定.
    async fn prepare_directory(
        &self,
        c_session: &Arc<LiveSession>,
        c_task: &Arc<Mutex<Task>>,
        c_upload: bool,
        c_local: &Path,
        c_remote: &str,
        c_overwrite: bool,
    ) -> Result<(Vec<(PathBuf, String, u64)>, String)> {
        use russh_sftp::{client::error::Error as SftpError, protocol::FileAttributes};
        if c_upload {
            let (l_local, l_remote) = (c_local.to_path_buf(), c_remote.to_owned());
            let (z_dirs, z_files, mut z_notes) = tokio::task::spawn_blocking(move || scan_local_directory(&l_local, &l_remote))
                .await
                .context("扫描本地文件夹异常")??;
            // 扫描通道用完即释放, 服务器每连接只允许一个通道时后面的文件传输才能打开新通道.
            let (z_raw, _, _) = self.open_transfer_raw(c_session, c_task).await?;
            for (i, z_dir) in z_dirs.into_iter().enumerate() {
                let l_made = tokio::time::timeout(Duration::from_secs(60), z_raw.mkdir(z_dir.clone(), FileAttributes::default()))
                    .await
                    .context("新建远端目录超时")?;
                let Err(err) = l_made else { continue };
                ensure!(matches!(err, SftpError::Status(_)), "新建远端目录失败: {err}");
                // 未确认覆盖时根目录必须由本任务新建, 不合并到已有目录.
                ensure!(i > 0 || c_overwrite, "远端目标已存在或无法创建: {err}");
                let l_exists = tokio::time::timeout(Duration::from_secs(60), z_raw.stat(z_dir.clone()))
                    .await
                    .context("检查远端目录超时")?
                    .is_ok_and(|c| c.attrs.is_dir());
                if !l_exists {
                    ensure!(i > 0, "新建远端目录失败: {err}");
                    directory_note(&mut z_notes, format!("失败 {z_dir}: 新建目录失败: {err}"));
                }
            }
            return Ok((z_files, z_notes));
        }
        let (z_raw, _, _) = self.open_transfer_raw(c_session, c_task).await?;
        let mut z_files = Vec::new();
        let mut z_notes = String::new();
        let mut z_dirs = Vec::new();
        let mut z_pending = vec![(c_remote.to_owned(), c_local.to_path_buf())];
        while let Some((l_remote, l_local)) = z_pending.pop() {
            let l_entries = match read_remote_directory(&z_raw, &l_remote).await {
                Ok(c) => c,
                Err(err) if z_dirs.is_empty() || !err.chain().any(|c| matches!(c.downcast_ref::<SftpError>(), Some(SftpError::Status(_)))) => return Err(err),
                Err(err) => {
                    directory_note(&mut z_notes, format!("跳过 {l_remote}: {err:#}"));
                    continue;
                }
            };
            z_dirs.push(l_local.clone());
            // Windows 替换非法字符、不区分大小写的文件系统都可能让同目录两个远端名落到同一本机名, 后者不能覆盖前者.
            let mut l_seen = std::collections::HashSet::new();
            for l_entry in l_entries {
                let l_name = l_entry.filename;
                if l_name == "." || l_name == ".." {
                    continue;
                }
                let l_path = format!("{l_remote}/{l_name}");
                // 文件名由服务器返回, 不可信: 含分隔符或跳转分量的名字不能拼进本机路径.
                if l_name.contains('/') || validate_remote_file_path(&l_path).is_err() {
                    directory_note(&mut z_notes, format!("跳过 {l_path}: 文件名无效"));
                    continue;
                }
                let l_attrs = l_entry.attrs;
                if l_attrs.is_symlink() {
                    directory_note(&mut z_notes, format!("跳过 {l_path}: 符号链接"));
                    continue;
                }
                // 服务器未返回类型时按普通文件处理, 传输时再由实际文件信息确认.
                if !l_attrs.is_dir() && !l_attrs.is_regular() && l_attrs.permissions.is_some() {
                    directory_note(&mut z_notes, format!("跳过 {l_path}: 特殊文件"));
                    continue;
                }
                let l_target = local_file_name(&l_name);
                if !l_seen.insert(local_name_key(&l_target)) {
                    directory_note(&mut z_notes, format!("跳过 {l_path}: 本机同目录已有同名项"));
                    continue;
                }
                ensure!(z_files.len() + z_dirs.len() + z_pending.len() < MAX_DIRECTORY_ITEMS, "文件夹超过 {MAX_DIRECTORY_ITEMS} 项, 请分批传输");
                if l_attrs.is_dir() {
                    z_pending.push((l_path, l_local.join(l_target)));
                } else {
                    z_files.push((l_local.join(l_target), l_path, l_attrs.size.unwrap_or(0)));
                }
            }
        }
        drop(z_raw);
        z_files.sort_by(|c_left, c_right| c_left.1.cmp(&c_right.1));
        if c_overwrite {
            tokio::fs::create_dir_all(c_local).await.context("创建本地文件夹失败")?;
        } else {
            tokio::fs::create_dir(c_local).await.context("创建本地文件夹失败")?;
        }
        for l_dir in z_dirs.iter().skip(1) {
            if let Err(err) = tokio::fs::create_dir_all(l_dir).await {
                directory_note(&mut z_notes, format!("失败 {}: 创建目录失败: {err}", l_dir.display()));
            }
        }
        Ok((z_files, z_notes))
    }

    // 传输专用 SFTP 通道; 单个请求超时放宽到 60 秒, 慢速线路上排队中的请求不会被误判超时.
    // 第二项为是否走传输专用连接: 退回主连接时调用方采用更保守的在途量, 避免拖慢终端.
    async fn open_transfer_stream(&self, c_session: &Arc<LiveSession>, c_task: &Arc<Mutex<Task>>) -> Result<(russh::ChannelStream<client::Msg>, Option<Arc<client::Handle<SshHandler>>>)> {
        let (z_handle, z_dedicated) = self.transfer_handle(c_session, Some(c_task)).await?;
        let z_link = z_dedicated.then(|| z_handle.clone());
        match open_sftp_stream(&z_handle).await {
            Ok(l_stream) => Ok((l_stream, z_link)),
            // 目录浏览可能因主连接开不了通道而借用了独立连接, 空闲的传输会话也占着通道; 服务器每连接只允许一个通道时, 先让出再试一次.
            // 浏览操作全程持有 file_slot: 拿到它说明在途浏览已结束、通道只剩缓存引用; 重试期间一直持有, 浏览不会重新抢占.
            // 浏览通道丢弃后之后浏览会自行重开.
            Err(err) if z_dedicated && (c_session.browse_sftp.lock().unwrap_or_else(PoisonError::into_inner).is_some() || has_idle_sftp(c_session)) => {
                let _l_permit = tokio::time::timeout(Duration::from_secs(30), c_session.file_slot.clone().acquire_owned())
                    .await
                    .with_context(|| format!("{err:#}; 等待目录浏览结束超时"))??;
                let z_idle = has_idle_sftp(c_session);
                clear_idle_sftp(c_session);
                if c_session.browse_sftp.lock().unwrap_or_else(PoisonError::into_inner).take().is_some() || z_idle {
                    // 等关闭通道的消息先送到服务器.
                    tokio::time::sleep(Duration::from_millis(300)).await;
                }
                Ok((open_sftp_stream(&z_handle).await.with_context(|| format!("{err:#}; 让出目录浏览通道后重试仍失败"))?, z_link))
            }
            Err(err) => Err(err),
        }
    }

    // 打开文件选择框时提前建好独立连接: 用户选文件的这段时间连接已就绪, 建不起来也已在等待之外得出结果.
    fn warm_transfer(self: &Arc<Self>, c_args: &Value, c_actor: Actor) -> Result<Value> {
        require_user(c_actor)?;
        let z_session = self.session(required_string(c_args, "session_id")?, c_actor)?;
        let z_self = self.clone();
        tokio::spawn(async move {
            let _ = z_self.transfer_handle(&z_session, None).await;
        });
        Ok(json!({}))
    }

    // 上传占满上行时, russh 会话循环要等整批数据写入 TCP 才继续处理收到的数据, 同一连接上的
    // 终端回显、监控和目录操作都会被拖住. 传输改走独立 SSH 连接, 按需建立并在会话内复用;
    // 无法建立 (键盘交互认证、服务器限制连接数等) 时退回主连接, 60 秒内不再重试.
    // 主连接意外断开后会话仍保留传输凭据, 传输重试可单独建立独立连接继续 (见 close_session).
    async fn transfer_handle(&self, c_session: &Arc<LiveSession>, c_task: Option<&Arc<Mutex<Task>>>) -> Result<(Arc<client::Handle<SshHandler>>, bool)> {
        let z_main = c_session.handle.lock().unwrap_or_else(PoisonError::into_inner).as_ref().cloned();
        let mut z_slot = c_session.transfer.lock().await;
        if let Some(l_handle) = z_slot.0.as_ref().filter(|c| !c.is_closed()) {
            return Ok((l_handle.clone(), true));
        }
        z_slot.0 = None;
        // 旧传输连接的断开原因不再适用于之后的任务 (新建或退回主连接).
        c_session.link_errors.lock().unwrap_or_else(PoisonError::into_inner)[1] = None;
        // 先确认有可复用的凭据, 没有 (键盘交互认证等) 时不必先建连再失败.
        let z_reusable = c_session.transfer_key.lock().unwrap_or_else(PoisonError::into_inner).is_some()
            || c_session.transfer_password.lock().unwrap_or_else(PoisonError::into_inner).is_some();
        let z_reason = if !z_reusable {
            "当前认证方式不能自动建立独立连接".to_owned()
        } else if let Some((_, l_reason)) = z_slot.1.as_ref().filter(|c| c.0.elapsed() < Duration::from_secs(60) && z_main.is_some()) {
            l_reason.clone()
        } else {
            match tokio::time::timeout(Duration::from_secs(10), self.connect_transfer(c_session)).await {
                Ok(Ok(l_handle)) => {
                    let l_handle = Arc::new(l_handle);
                    // 建立期间会话已被关闭或标签已重连 (不再登记为该标签的会话): 不保留新连接.
                    let l_registered = self.state.lock().unwrap_or_else(PoisonError::into_inner).sessions.get(&c_session.id).is_some_and(|c| Arc::ptr_eq(c, c_session));
                    if !l_registered {
                        let _ = l_handle.disconnect(Disconnect::ByApplication, "关闭连接", "zh-CN").await;
                        bail!("SSH 尚未连接");
                    }
                    z_slot.0 = Some(l_handle.clone());
                    z_slot.1 = None;
                    return Ok((l_handle, true));
                }
                l_failed => {
                    let l_reason = match l_failed { Ok(Err(err)) => safe_error(&format!("{err:#}")), _ => "建立独立连接超时".into() };
                    z_slot.1 = Some((Instant::now(), l_reason.clone()));
                    l_reason
                }
            }
        };
        // 回退记在任务详情里, 便于判断传输时终端变慢的原因.
        if let Some(Ok(mut l_task)) = c_task.map(|c| c.lock()) {
            l_task.stdout = format!("经主连接传输 ({z_reason}), 传输期间终端和监控可能变慢");
        }
        Ok((z_main.context("SSH 尚未连接")?, false))
    }

    // 取会话复用的浏览 SFTP 会话, 没有时新建 (首次加载失败或会话失效后, 刷新即在此重连文件部分). 调用方持有 file_slot, 不会并发重复创建.
    // 先在主连接上开; 主连接开不了新通道 (服务器会话名额已满、线路繁忙不回应) 时改用独立连接, 终端与监控不受影响.
    async fn browse_sftp(&self, c_session: &Arc<LiveSession>, c_handle: &Arc<client::Handle<SshHandler>>) -> Result<Arc<russh_sftp::client::RawSftpSession>> {
        if let Some(l_sftp) = c_session.browse_sftp.lock().unwrap_or_else(PoisonError::into_inner).clone() {
            return Ok(l_sftp);
        }
        let z_stream = match open_sftp_stream(c_handle).await {
            Ok(l_stream) => l_stream,
            Err(err) => {
                let (l_handle, l_dedicated) = self.transfer_handle(c_session, None).await?;
                if !l_dedicated {
                    return Err(err);
                }
                // 空闲的传输会话可能占着服务器允许的通道.
                clear_idle_sftp(c_session);
                open_sftp_stream(&l_handle).await.with_context(|| format!("主连接{err:#}; 改用独立连接也失败"))?
            }
        };
        let z_sftp = russh_sftp::client::RawSftpSession::new(z_stream);
        z_sftp.set_timeout(10);
        tokio::time::timeout(Duration::from_secs(10), z_sftp.init())
            .await
            .context("初始化SFTP超时")??;
        let z_sftp = Arc::new(z_sftp);
        *c_session.browse_sftp.lock().unwrap_or_else(PoisonError::into_inner) = Some(z_sftp.clone());
        Ok(z_sftp)
    }

    async fn connect_transfer(&self, c_session: &Arc<LiveSession>) -> Result<client::Handle<SshHandler>> {
        let z_handler = SshHandler { session: Arc::downgrade(c_session), store: self.store.clone(), transfer: true };
        let z_user = c_session.connection.username.clone();
        let z_proxy = c_session.proxy.lock().unwrap_or_else(PoisonError::into_inner).clone();
        // 关闭会话时代理配置已清空: 不能当成直连, 否则会暴露本机地址.
        ensure!(z_proxy.is_some() || c_session.connection.proxy_id.is_none(), "会话已关闭, 不建立传输连接");
        let mut z_handle = ssh_connect(&c_session.connection, z_proxy.as_deref(), z_handler).await?;
        let z_key = c_session.transfer_key.lock().unwrap_or_else(PoisonError::into_inner).clone();
        let z_success = if let Some(l_key) = z_key {
            let l_hash = z_handle.best_supported_rsa_hash().await.ok().flatten().flatten();
            z_handle.authenticate_publickey(z_user, PrivateKeyWithHashAlg::new(l_key, l_hash)).await?.success()
        } else {
            let l_password = c_session.transfer_password.lock().unwrap_or_else(PoisonError::into_inner).clone().context("当前认证方式不能自动建立传输连接")?;
            z_handle.authenticate_password(z_user, l_password.as_str()).await?.success()
        };
        ensure!(z_success, "传输连接认证失败");
        Ok(z_handle)
    }

    async fn open_transfer_raw(&self, c_session: &Arc<LiveSession>, c_task: &Arc<Mutex<Task>>) -> Result<(Arc<russh_sftp::client::RawSftpSession>, russh_sftp::protocol::Version, Option<Arc<client::Handle<SshHandler>>>)> {
        if let Some((l_link, (l_raw, l_version))) = take_idle_sftp(&c_session.idle_upload) {
            return Ok((l_raw, l_version, Some(l_link)));
        }
        let (z_stream, z_link) = self.open_transfer_stream(c_session, c_task).await?;
        let z_raw = russh_sftp::client::RawSftpSession::new_with_config(
            z_stream,
            russh_sftp::client::Config { request_timeout_secs: 60, ..russh_sftp::client::Config::default() },
        );
        let z_version = tokio::time::timeout(Duration::from_secs(10), z_raw.init())
            .await
            .context("建立SFTP会话超时")??;
        Ok((Arc::new(z_raw), z_version, z_link))
    }

    // 传输先写同目录、带任务标识的临时文件 (排他创建), 完成后改为目标名: 中途失败或取消时目标保持原样.
    // c_part_created 记录临时文件是否由本任务创建, 调用方据此只清理自己的文件; c_committing 表示已进入提交阶段.
    // c_resume 为断点 (见 run_task): 临时文件已存在时重新打开它, 从断点继续.
    async fn run_transfer(
        &self,
        c_session: &Arc<LiveSession>,
        c_task: &Arc<Mutex<Task>>,
        c_upload: bool,
        c_local: PathBuf,
        c_remote: String,
        c_overwrite: bool,
        c_part_created: &AtomicBool,
        c_committing: &AtomicBool,
        c_resume: &Mutex<(u64, Option<(u64, u64)>)>,
    ) -> Result<()> {
        let z_tag = c_task.lock().unwrap_or_else(PoisonError::into_inner).task_id.chars().take(8).collect::<String>();
        if c_upload {
            use russh_sftp::protocol::{FileAttributes, OpenFlags};
            // 每次写 32 KiB, 进度按服务器确认的字节计算; 按确认往返时间自适应在途数量, 排队延迟明显超过基线时减少.
            // 专用连接上没有交互流量: 直接从 64 个 (2 MiB) 起步, 未出现排队前每个确认加一 (每轮往返约翻倍),
            // 最多 256 个 (8 MiB); 退回主连接时从 8 个起步、最多 64 个且缓慢增长, 不拖慢键盘回显.
            const UPLOAD_CHUNK: usize = 32 * 1024;
            ensure!(
                tokio::fs::metadata(&c_local).await?.is_file(),
                "上传源必须是普通文件"
            );
            let mut source = tokio::fs::File::open(&c_local).await?;
            let z_source = source.metadata().await?;
            let total = z_source.len();
            let z_mtime = z_source.modified().ok().and_then(|c| c.duration_since(UNIX_EPOCH).ok()).map_or(0, |c| c.as_nanos() as u64);
            let z_start = resume_offset(c_resume, c_part_created, (total, z_mtime))?;
            let (z_raw, z_version, z_link) = self.open_transfer_raw(c_session, c_task).await?;
            let z_dedicated = z_link.is_some();
            let z_part = part_name(&c_remote, &z_tag);
            // 续传时临时文件已由本任务创建, 只写不截断; 断点之后残留的数据会被重新写入覆盖.
            let z_flags = if c_part_created.load(Ordering::Acquire) { OpenFlags::WRITE } else { OpenFlags::CREATE | OpenFlags::WRITE | OpenFlags::EXCLUDE };
            // 检查目标与打开临时文件同时发出, 省一次往返. 临时文件不影响目标; 下面报错时由调用方按 c_part_created 清理.
            let (z_stat, z_opened) = tokio::join!(
                tokio::time::timeout(Duration::from_secs(60), z_raw.stat(c_remote.clone())),
                tokio::time::timeout(Duration::from_secs(60), z_raw.open(z_part.clone(), z_flags, FileAttributes::default())),
            );
            if matches!(z_opened, Ok(Ok(_))) {
                c_part_created.store(true, Ordering::Release);
            }
            // 只有"不存在"算正常结果; 超时、权限和连接错误直接报错.
            let z_existing = match z_stat.context("检查远端目标超时")? {
                Ok(l_attrs) => Some(l_attrs),
                Err(russh_sftp::client::error::Error::Status(l_status))
                    if l_status.status_code == russh_sftp::protocol::StatusCode::NoSuchFile => None,
                Err(err) => return Err(anyhow::Error::from(err).context("检查远端目标失败")),
            };
            let z_exists = z_existing.is_some();
            ensure!(c_overwrite || !z_exists, "远端目标已存在");
            let z_handle = z_opened.context("打开远端临时文件超时")??.handle;
            if z_start > 0 {
                source.seek(SeekFrom::Start(z_start)).await.context("定位本地文件续传位置失败")?;
            }
            mark_task(c_task, "running", None);
            update_progress(c_task, z_start, total);
            let z_written = async {
                let mut z_inflight = tokio::task::JoinSet::new();
                let mut z_offset = z_start;
                let mut z_acked = z_start;
                // 写请求并发完成, 确认总数不能当断点: 只取从头连续确认的位置, 之后已确认的段暂存在此按偏移衔接.
                let mut z_contiguous = z_start;
                let mut z_done = HashMap::new();
                let mut z_eof = false;
                let (mut z_limit, z_min, z_max, z_slack) = if z_dedicated {
                    (64.0f64, 16.0f64, 256.0f64, Duration::from_millis(300))
                } else {
                    (8.0, 2.0, 64.0, Duration::from_millis(200))
                };
                let mut z_slow_start = z_dedicated;
                // 基线取最近两个 10 秒窗口内的最小往返时间: 拥塞时窗口收缩、队列排空后会测到真实延迟,
                // 持续拥塞不会被逐渐当成正常延迟; 线路变慢后最多约 20 秒重新校准.
                let mut z_base_rtt = Duration::MAX;
                let mut z_window_min = Duration::MAX;
                let mut z_window_at = Instant::now();
                let mut z_decreased = Instant::now();
                while !z_eof || !z_inflight.is_empty() {
                    while !z_eof && (z_inflight.len() as f64) < z_limit {
                        let mut l_buf = vec![0u8; UPLOAD_CHUNK];
                        let mut l_len = 0;
                        while l_len < UPLOAD_CHUNK {
                            let l_read = source.read(&mut l_buf[l_len..]).await.context("读取本地文件失败")?;
                            if l_read == 0 {
                                z_eof = true;
                                break;
                            }
                            l_len += l_read;
                        }
                        if l_len == 0 {
                            break;
                        }
                        l_buf.truncate(l_len);
                        // 同一会话的多个上传共享在途总量; 名额随请求完成释放.
                        let l_permit = if z_dedicated { &c_session.upload_inflight } else { &c_session.upload_main_inflight }.clone().acquire_owned().await?;
                        let (l_raw, l_handle, l_offset) = (z_raw.clone(), z_handle.clone(), z_offset);
                        z_offset += l_len as u64;
                        let l_sent = Instant::now();
                        z_inflight.spawn(async move {
                            let _l_permit = l_permit;
                            l_raw.write(l_handle, l_offset, l_buf).await.map(|_| (l_offset, l_len as u64, l_sent.elapsed()))
                        });
                    }
                    if let Some(l_done) = z_inflight.join_next().await {
                        let (l_offset, l_len, l_rtt) = l_done.context("写入任务异常")?.context("写入远端文件失败")?;
                        z_acked += l_len;
                        z_done.insert(l_offset, l_len);
                        while let Some(l_next) = z_done.remove(&z_contiguous) {
                            z_contiguous += l_next;
                        }
                        c_resume.lock().unwrap_or_else(PoisonError::into_inner).0 = z_contiguous;
                        update_progress(c_task, z_acked, total);
                        z_window_min = z_window_min.min(l_rtt);
                        z_base_rtt = z_base_rtt.min(l_rtt);
                        if z_window_at.elapsed() >= Duration::from_secs(10) {
                            z_base_rtt = z_window_min;
                            z_window_min = Duration::MAX;
                            z_window_at = Instant::now();
                        }
                        if l_rtt > z_base_rtt + z_slack {
                            // 同一轮往返内的多个超时确认只减一次, 避免一次拥塞把窗口压到最低.
                            if z_decreased.elapsed() > l_rtt {
                                z_limit = (z_limit * 0.85).max(z_min);
                                z_decreased = Instant::now();
                                z_slow_start = false;
                            }
                        } else {
                            z_limit = (z_limit + if z_slow_start { 1.0 } else { 1.0 / z_limit.max(1.0) }).min(z_max);
                        }
                    }
                }
                ensure!(z_offset == total, "上传源在传输期间被修改");
                Ok::<_, anyhow::Error>(())
            }
            .await;
            let z_closed = tokio::time::timeout(Duration::from_secs(60), z_raw.close(z_handle)).await;
            z_written?;
            z_closed.context("关闭远端文件超时")?.context("关闭远端文件失败")?;
            // 经过续传的文件提交前核对长度, 不把缺段或多余数据的文件改为正式名.
            if z_start > 0 {
                let l_size = tokio::time::timeout(Duration::from_secs(60), z_raw.stat(z_part.clone()))
                    .await
                    .context("检查远端临时文件超时")??
                    .attrs
                    .size;
                ensure!(l_size.is_none_or(|c| c == total), "续传后远端临时文件长度不符");
            }
            // 提交阶段 (保留权限、改名) 放进独立任务: 取消只丢弃等待, 不会停在旧文件已改名、新文件未就位的中间状态.
            // 临时文件此后归提交任务负责, 外层不再清理.
            c_part_created.store(false, Ordering::Release);
            c_committing.store(true, Ordering::Release);
            let z_posix_rename = z_version.extensions.get("posix-rename@openssh.com").is_some_and(|c| c == "1");
            let z_permissions = z_existing.and_then(|c| c.attrs.permissions);
            let z_commit = tokio::spawn(commit_upload(z_raw.clone(), z_part, c_remote, c_overwrite, z_exists, z_posix_rename, z_permissions));
            z_commit.await.context("提交上传任务异常")??;
            // 只放回成功结束的会话: 失败或取消时可能还有未完成的请求.
            if let Some(l_link) = z_link {
                put_idle_sftp(&c_session.idle_upload, l_link, (z_raw, z_version));
            }
        } else {
            if let Ok(z_metadata) = tokio::fs::metadata(&c_local).await {
                ensure!(c_overwrite, "本地目标已存在");
                ensure!(z_metadata.is_file(), "本地目标必须是普通文件");
            }
            let (sftp, z_link) = match take_idle_sftp(&c_session.idle_download) {
                Some((l_link, l_sftp)) => (l_sftp, Some(l_link)),
                None => {
                    let (l_stream, l_link) = self.open_transfer_stream(c_session, c_task).await?;
                    // 保持默认的 16 个预读请求, 限制多文件共享慢速线路时的排队量.
                    let l_sftp = tokio::time::timeout(
                        Duration::from_secs(10),
                        russh_sftp::client::SftpSession::new_with_config(
                            l_stream,
                            russh_sftp::client::Config { request_timeout_secs: 60, ..russh_sftp::client::Config::default() },
                        ),
                    )
                    .await
                    .context("建立SFTP会话超时")??;
                    (l_sftp, l_link)
                }
            };
            // 读取信息与打开文件同时发出, 省一次往返.
            let (z_metadata, z_source) = tokio::join!(
                tokio::time::timeout(Duration::from_secs(60), sftp.metadata(c_remote.clone())),
                tokio::time::timeout(Duration::from_secs(60), sftp.open(c_remote)),
            );
            let z_metadata = z_metadata.context("读取远端文件信息超时")??;
            ensure!(z_metadata.file_type().is_file(), "下载源必须是普通文件");
            let total = z_metadata.size.context("服务器未返回文件长度")?;
            let z_start = resume_offset(c_resume, c_part_created, (total, z_metadata.mtime.map_or(0, u64::from)))?;
            let mut source = z_source.context("打开远端源文件超时")??;
            if z_start > 0 {
                source.seek(SeekFrom::Start(z_start)).await.context("定位远端文件续传位置失败")?;
            }
            let mut source = source.take(total - z_start);
            let z_part = local_part_path(&c_local, &z_tag);
            let mut dest = if c_part_created.load(Ordering::Acquire) {
                // 续传: 截掉断点之后可能残留的半截数据, 再从断点追加.
                let mut l_dest = tokio::fs::OpenOptions::new().write(true).open(&z_part).await.context("打开本地临时文件失败")?;
                l_dest.set_len(z_start).await.context("截断本地临时文件失败")?;
                l_dest.seek(SeekFrom::Start(z_start)).await.context("定位本地临时文件失败")?;
                l_dest
            } else {
                tokio::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&z_part)
                    .await
                    .context("创建本地临时文件失败")?
            };
            c_part_created.store(true, Ordering::Release);
            mark_task(c_task, "running", None);
            update_progress(c_task, z_start, total);
            let mut buf = vec![0; 256 * 1024];
            let mut count = z_start;
            let mut z_last_data_at = Instant::now();
            let mut z_read_retries = 0;
            loop {
                let z_read_wait = Duration::from_secs(60).saturating_sub(z_last_data_at.elapsed());
                let n = match tokio::time::timeout(z_read_wait, source.read(&mut buf))
                    .await
                    .context("读取远端文件超时")?
                {
                    Ok(l_read) => l_read,
                    // 库按请求入队计时, 后排预读可能在仍有数据到达时超时. 回到已落盘位置清掉过期预读,
                    // 每次有新数据才重置空闲计时; 连续 60 秒无数据仍失败, 不因重试延长等待.
                    Err(err) if err.kind() == std::io::ErrorKind::TimedOut
                        && z_read_retries < 2
                        && z_last_data_at.elapsed() < Duration::from_secs(60) =>
                    {
                        z_read_retries += 1;
                        source.get_mut().seek(SeekFrom::Start(count)).await.context("恢复远端文件读取位置失败")?;
                        source.set_limit(total - count);
                        continue;
                    }
                    Err(err) => return Err(anyhow::Error::from(err).context("读取远端文件失败")),
                };
                if n == 0 {
                    break;
                }
                tokio::time::timeout(Duration::from_secs(60), dest.write_all(&buf[..n]))
                    .await
                    .context("写入本地文件超时")??;
                count += n as u64;
                // 已交给本机文件系统的数据即为可靠断点 (同一进程内续传, 不需要先落到磁盘).
                c_resume.lock().unwrap_or_else(PoisonError::into_inner).0 = count;
                // 本地落盘另有超时, 不把写盘耗时算作远端无数据等待.
                z_last_data_at = Instant::now();
                z_read_retries = 0;
                update_progress(c_task, count, total);
            }
            ensure!(count == total, "下载源在传输期间被截断");
            tokio::time::timeout(Duration::from_secs(60), dest.flush())
                .await
                .context("刷新本地文件超时")??;
            drop(dest);
            c_committing.store(true, Ordering::Release);
            // 不覆盖时先排他创建目标占住名字 (已存在则失败, 并发的同名下载只有一个能成功), 再用临时文件替换这个空占位;
            // 先检查再改名有空档, rename 本身会替换已有文件. 覆盖时 rename 直接替换, 不会出现目标缺失.
            // 提交放进独立任务: 取消只丢弃等待, 不会停在占位已建、改名未完成的中间状态留下空文件.
            // 临时文件仍由外层失败处理删除; 被提前删掉时这里改名失败并回收占位.
            let z_commit = tokio::spawn(async move {
                if !c_overwrite {
                    match tokio::fs::OpenOptions::new().write(true).create_new(true).open(&c_local).await {
                        Ok(_) => {}
                        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => bail!("本地目标已存在"),
                        Err(err) => return Err(anyhow::Error::from(err).context("创建本地目标失败")),
                    }
                }
                if let Err(err) = rename_local(&z_part, &c_local).await {
                    if !c_overwrite {
                        let _ = tokio::fs::remove_file(&c_local).await;
                    }
                    return Err(anyhow::Error::from(err).context("本地文件改名失败"));
                }
                Ok(())
            });
            z_commit.await.context("提交下载任务异常")??;
            // 先关闭远端文件再放回, 放回的会话没有未完成的句柄; 只放回成功结束的会话.
            drop(source);
            if let Some(l_link) = z_link {
                put_idle_sftp(&c_session.idle_download, l_link, sftp);
            }
        }
        Ok(())
    }

    fn mcp_heartbeat(&self, c_args: &Value, c_actor: Actor) -> Result<Value> {
        ensure!(c_actor == Actor::Mcp, "仅 MCP 服务使用心跳");
        let z_id = required_string(c_args, "client_id")?;
        let z_name = c_args.get("name").and_then(Value::as_str).unwrap_or("AI 客户端");
        ensure!(z_id.len() <= 64, "客户端标识过长");
        let mut z_state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        // 不再请求的客户端顺带清理, 防止异常客户端堆积.
        z_state.mcp_clients.retain(|_, c| c.1.elapsed() < Duration::from_secs(60));
        if z_state.mcp_clients.len() < 16 || z_state.mcp_clients.contains_key(z_id) {
            let l_name = z_name.chars().filter(|c| !c.is_control()).take(48).collect();
            z_state.mcp_clients.insert(z_id.to_owned(), (l_name, Instant::now()));
        }
        Ok(json!({"ok":true}))
    }

    // 侧栏记录: 对象 ("[分组] 服务器", 没有服务器时给简短说明) 与点击后浮层显示的执行内容 (输入、命令、路径或任务).
    fn mcp_activity_detail(&self, c_method: &str, c_args: &Value) -> (String, String) {
        let z_task = c_args.get("task_id").and_then(Value::as_str).and_then(|c| {
            let l_task = self.state.lock().ok()?.tasks.get(c).cloned()?;
            let l_task = l_task.lock().ok()?;
            Some((l_task.connection.id.clone(), l_task.label.clone()))
        });
        let z_connection = c_args
            .get("session_id")
            .and_then(Value::as_str)
            .and_then(|c| Some(self.state.lock().ok()?.sessions.get(c)?.connection.id.clone()))
            .or_else(|| c_args.get("connection_id").and_then(Value::as_str).map(str::to_owned))
            .or_else(|| z_task.as_ref().map(|c| c.0.clone()));
        let z_target = match (c_method, z_connection.and_then(|c| self.store.get(&c).ok())) {
            (_, Some(l_connection)) => format!("[{}] {}", if l_connection.group.is_empty() { "默认" } else { &l_connection.group }, l_connection.name),
            ("state", None) => "全部会话".into(),
            ("list_local_files", None) => "本机".into(),
            _ => String::new(),
        };
        let z_arg = |c_key: &str| c_args.get(c_key).and_then(Value::as_str).unwrap_or_default();
        let z_info = match c_method {
            // MCP 已把 key、enter 换算进 data, 控制键显示为符号.
            "write_terminal" => visible_keys(z_arg("data")),
            "input_admin_password" => "[管理员密码]".into(),
            "run_command" => z_arg("command").to_owned(),
            "upload_file" => format!("{}\n到 {}", z_arg("local_path"), z_arg("remote_path")),
            "download_file" => format!("{}\n到 {}", z_arg("remote_path"), z_arg("local_path")),
            "list_files" | "list_local_files" => z_arg("path").to_owned(),
            "get_task" | "cancel_task" => z_task.map(|c| c.1).unwrap_or_default(),
            "connect" | "reconnect" if c_args.get("files_only").and_then(Value::as_bool) == Some(true) => "仅文件".into(),
            _ => String::new(),
        };
        (z_target, z_info.chars().take(500).collect())
    }

    pub fn data_dir(&self) -> &Path {
        self.store.directory()
    }

    // 按设置启动、换端口或关闭 MCP 服务, 返回网址或未运行的原因. 先等旧监听任务结束 (端口释放) 再绑定新端口.
    pub async fn apply_mcp(self: &Arc<Self>) -> std::result::Result<String, String> {
        let mut z_http = self.mcp_http.lock().await;
        if let Some(l_task) = z_http.0.take() {
            l_task.abort();
            let _ = l_task.await;
        }
        let z_result = match self.store.settings() {
            Err(err) => Err(format!("{err:#}")),
            Ok(l_settings) if !l_settings.mcp_enabled => Err("MCP 服务已关闭".into()),
            Ok(l_settings) => match crate::mcp::start_http(self.clone(), l_settings.mcp_port).await {
                Ok((l_url, l_task)) => {
                    z_http.0 = Some(l_task);
                    Ok(l_url)
                }
                Err(err) => Err(format!("{err:#}")),
            },
        };
        self.set_mcp_running(z_result.is_ok());
        z_http.1 = z_result.clone();
        z_result
    }

    // 设置页修改开关与端口: 只由界面直接调用, 不经 call 分派, MCP 客户端无法改动自己的服务.
    pub async fn set_mcp(self: &Arc<Self>, c_enabled: bool, c_port: u16) -> Result<std::result::Result<String, String>> {
        self.store.set_mcp(c_enabled, c_port)?;
        Ok(self.apply_mcp().await)
    }

    pub async fn mcp_status(&self) -> std::result::Result<String, String> {
        self.mcp_http.lock().await.1.clone()
    }

    fn set_mcp_running(&self, c_running: bool) {
        if let Ok(mut z_state) = self.state.lock() {
            z_state.mcp_running = c_running;
        }
    }

    fn state_json(&self, c_actor: Actor) -> Result<Value> {
        let z_connections = self.store.list();
        let z_settings = self.store.settings()?;
        let z_state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let mut sessions = Vec::new();
        for z_tab in &z_state.tabs {
            let Some(session) = z_state.sessions.get(&z_tab.id) else {
                let z_conn = z_connections
                    .iter()
                    .find(|c| Some(&c.id) == z_tab.connection_id.as_ref());
                // 已断开只剩标签的会话: MCP 也能看到已授权连接的标签, 才能用 reconnect 在原标签重连.
                if c_actor == Actor::User || z_conn.is_some_and(|c| c.mcp_allowed) {
                    sessions.push(json!({"session_id":z_tab.id,"connection_id":z_tab.connection_id,"name":z_conn.map(|c|c.name.as_str()).unwrap_or("快速连接"),"group":z_conn.map(|c|c.group.as_str()).unwrap_or(""),"status":if z_tab.connection_id.is_none(){"empty"}else{"disconnected"},"empty":z_tab.connection_id.is_none()}));
                }
                continue;
            };
            let z_allowed = z_connections
                .iter()
                .any(|c| mcp_permits(c, &session.connection));
            if c_actor == Actor::Mcp && !z_allowed {
                continue;
            }
            let trust = session.trust.lock().ok().and_then(|p| {
                p.as_ref()
                    .map(|t| json!({"fingerprint":t.fingerprint,"changed":t.changed}))
            });
            let prompts = session
                .auth_prompts
                .lock()
                .map(|p| p.clone())
                .unwrap_or_default();
            let monitor = session
                .monitor
                .lock()
                .ok()
                .and_then(|m| m.as_ref().and_then(|m| serde_json::to_value(m).ok()));
            let z_owner = session
                .lease
                .lock()
                .ok()
                .and_then(|l| l.as_ref().map(|l| l.until > Instant::now()))
                .unwrap_or(false)
                && z_allowed;
            sessions.push(json!({"session_id":session.id,"link":session.link,"terminal":session.terminal_gen.load(Ordering::Acquire),"connection_id":session.connection.id,"name":session.connection.name,"group":self.store.get(&session.connection.id).map(|c| c.group).unwrap_or_else(|_| session.connection.group.clone()),"status":session.status.lock().map(|s|s.clone()).unwrap_or_default(),"shell":session.shell.load(Ordering::Acquire),"empty":false,"owner":if z_owner{"mcp"}else{"user"},"trust":trust,"auth_prompts":prompts,"monitor":monitor}));
        }
        let mut tasks: Vec<(Instant, Value)> = z_state
            .tasks
            .values()
            .filter_map(|t| {
                t.lock().ok().and_then(|t| {
                    if c_actor == Actor::User
                        || z_connections
                            .iter()
                            .any(|c| mcp_permits(c, &t.connection))
                    {
                        let mut l_summary = task_summary(&t);
                        // 命令最长 16 KiB, 200 条历史会超过 MCP 结果上限而让 state 整体失败; MCP 只给摘要, 完整内容用 get_task.
                        if c_actor == Actor::Mcp && t.label.len() > 256 {
                            l_summary["label"] = json!(format!("{}…", &t.label[..t.label.floor_char_boundary(256)]));
                        }
                        Some((t.created_at, l_summary))
                    } else {
                        None
                    }
                })
            })
            .collect();
        tasks.sort_by(|a, b| b.0.cmp(&a.0));
        let tasks: Vec<Value> = tasks.into_iter().map(|(_, task)| task).collect();
        // 当前标签已断开 (只剩标签) 时按标签记录的连接判断授权, 与上面 sessions 列出的离线标签一致.
        let active = z_state.active_session_id.as_ref().filter(|id| {
            c_actor == Actor::User
                || match z_state.sessions.get(*id) {
                    Some(l_session) => z_connections.iter().any(|c| mcp_permits(c, &l_session.connection)),
                    None => z_state.tabs.iter().find(|c| &c.id == *id).and_then(|c| c.connection_id.as_ref())
                        .is_some_and(|l_id| z_connections.iter().any(|c| &c.id == l_id && c.mcp_allowed)),
                }
        });
        let z_connections: Vec<_> = if c_actor == Actor::User {
            z_connections
        } else {
            z_connections
                .into_iter()
                .filter(|c| c.mcp_allowed)
                .collect()
        };
        if c_actor == Actor::Mcp {
            // MCP 只需连接、会话和任务; 设置与常用命令属于界面数据.
            let mut z_result = json!({"connections":z_connections,"sessions":sessions,"tasks":tasks,"active_session_id":active});
            // 未授权的连接对 AI 完全不可见, 列表为空时说明原因, 避免 AI 误判为通信或同步故障.
            if z_connections.is_empty() {
                z_result["hint"] = json!("用户尚未授权任何连接给 MCP. 请用户在 HiPShell 连接编辑中勾选“允许 MCP 使用此连接”后再调用 state.");
            }
            return Ok(z_result);
        }
        let (z_commands, z_command_groups, z_commands_error) = if c_actor == Actor::User {
            match self
                .commands
                .list()
                .and_then(|c| self.commands.groups().map(|l_groups| (c, l_groups)))
            {
                Ok((l_commands, l_groups)) => (l_commands, l_groups, String::new()),
                Err(err) => (Vec::new(), Vec::new(), err.to_string()),
            }
        } else {
            (Vec::new(), Vec::new(), String::new())
        };
        let mut z_result =
            json!({"connections":z_connections,"sessions":sessions,"tasks":tasks,"active_session_id":active,"quick_commands":z_commands,"quick_commands_error":z_commands_error,"settings":z_settings});
        if c_actor == Actor::User {
            // 代理列表给界面选择用, 不含密码.
            z_result["proxies"] = json!(self.store.proxies().unwrap_or_default().iter().map(|c| json!({
                "id":c.id,"name":c.name,"kind":c.kind,"host":c.host,"port":c.port,"username":c.username,
                "has_password":crate::config::ConfigStore::proxy_has_password(c)
            })).collect::<Vec<_>>());
        }
        if c_actor == Actor::User {
            z_result["command_groups"] = json!(z_command_groups);
            z_result["mcp_status"] = json!({
                "running":z_state.mcp_running,
                "clients":z_state.mcp_clients.values().filter(|c| c.1.elapsed() < Duration::from_secs(15)).map(|c| c.0.as_str()).collect::<Vec<_>>(),
                "activity":z_state.mcp_activity.iter().map(|(c_method,c_client,c_detail,c_ok,c_at,c_info,c_error)| json!({"method":c_method,"client":c_client,"detail":c_detail,"ok":c_ok,"at_ms":c_at,"info":c_info,"error":c_error})).collect::<Vec<_>>()
            });
        }
        Ok(z_result)
    }

    fn session(&self, c_id: &str, c_actor: Actor) -> Result<Arc<LiveSession>> {
        let z_session = self.state.lock().unwrap_or_else(PoisonError::into_inner)
            .sessions
            .get(c_id)
            .cloned()
            .context("会话不存在")?;
        if c_actor == Actor::Mcp {
            ensure!(self.mcp_permitted(&z_session.connection), "此连接未授权 MCP");
        }
        Ok(z_session)
    }
    fn mcp_permitted(&self, c_opened: &Connection) -> bool {
        self.store.get(&c_opened.id).is_ok_and(|c| mcp_permits(&c, c_opened))
    }
    fn task(&self, c_id: &str, c_actor: Actor) -> Result<Arc<Mutex<Task>>> {
        let z_state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let z_task = z_state.tasks.get(c_id).cloned().context("任务不存在")?;
        if c_actor == Actor::Mcp {
            let task = z_task.lock().unwrap_or_else(PoisonError::into_inner);
            ensure!(self.mcp_permitted(&task.connection), "此连接未授权 MCP");
        }
        Ok(z_task)
    }
}

enum TaskSpec {
    Command {
        command: String,
        timeout: u64,
        max_output: usize,
    },
    Transfer {
        upload: bool,
        local: PathBuf,
        remote: String,
        overwrite: bool,
        total: Option<u64>,
        directory: bool,
    },
}
#[derive(Serialize)]
struct TaskView {
    task_id: String,
    session_id: String,
    connection_id: String,
    created_at_ms: u64,
    source: String,
    client: String,
    kind: String,
    label: String,
    local: String,
    started_at_ms: Option<u64>,
    finished_at_ms: Option<u64>,
    status: String,
    bytes: u64,
    total: Option<u64>,
    speed_bytes_per_sec: u64,
    directory: bool,
    files_done: u64,
    files_total: u64,
    current: String,
    file_bytes: u64,
    file_total: u64,
    stdout: String,
    stderr: String,
    truncated: bool,
    exit_code: Option<i32>,
    error: Option<String>,
}
impl From<&Task> for TaskView {
    fn from(t: &Task) -> Self {
        Self {
            task_id: t.task_id.clone(),
            session_id: t.session_id.clone(),
            connection_id: t.connection.id.clone(),
            created_at_ms: t.created_at_ms,
            source: t.source.clone(),
            client: t.client.clone(),
            kind: t.kind.clone(),
            label: t.label.clone(),
            local: t.local.clone(),
            started_at_ms: t.started_at_ms,
            finished_at_ms: t.finished_at_ms,
            status: t.status.clone(),
            bytes: t.bytes,
            total: t.total,
            speed_bytes_per_sec: current_speed(t),
            directory: t.directory,
            files_done: t.files_done,
            files_total: t.files_total,
            current: t.current.clone(),
            file_bytes: t.file_bytes,
            file_total: t.file_total,
            stdout: t.stdout.clone(),
            stderr: t.stderr.clone(),
            truncated: t.truncated,
            exit_code: t.exit_code,
            error: t.error.clone(),
        }
    }
}

impl client::Handler for SshHandler {
    type Error = russh::Error;
    // 记录断开原因; 返回值与库默认实现一致. 本端主动关闭也会走到这里, 但那时状态已由关闭流程设置, 不会显示.
    async fn disconnected(&mut self, c_reason: client::DisconnectReason<Self::Error>) -> std::result::Result<(), Self::Error> {
        let (z_text, z_result) = match c_reason {
            client::DisconnectReason::ReceivedDisconnect(l_info) => (
                format!("服务器断开 ({:?}) {}", l_info.reason_code, safe_error(&l_info.message)),
                Ok(()),
            ),
            client::DisconnectReason::Error(err) => (
                match &err {
                    russh::Error::KeepaliveTimeout => "保活超时, 约 60 秒未收到服务器任何数据 (网络或代理链路中断)".to_owned(),
                    russh::Error::InactivityTimeout => "无活动超时, 90 秒内数据未能发出或未收到数据 (网络或代理链路堵塞)".to_owned(),
                    russh::Error::Disconnect => "连接被关闭, 未收到断开说明 (服务器、代理或网络中断)".to_owned(),
                    l_err => safe_error(&format!("网络错误: {l_err}")),
                },
                Err(err),
            ),
        };
        if let Some(l_session) = self.session.upgrade() {
            l_session.link_errors.lock().unwrap_or_else(PoisonError::into_inner)[usize::from(self.transfer)] = Some(z_text);
        }
        z_result
    }
    async fn check_server_key(
        &mut self,
        c_key: &PublicKeyOrCertificate,
    ) -> std::result::Result<bool, Self::Error> {
        let Some(session) = self.session.upgrade() else {
            return Ok(false);
        };
        if session.actor == Actor::Mcp
            && !self.store.get(&session.connection.id).is_ok_and(|c| mcp_permits(&c, &session.connection))
        {
            return Ok(false);
        }
        let z_fingerprint = match c_key {
            PublicKeyOrCertificate::PublicKey { key, .. } => {
                key.fingerprint(HashAlg::Sha256).to_string()
            }
            PublicKeyOrCertificate::Certificate(cert) => {
                keys::PublicKey::from(cert.public_key().clone())
                    .fingerprint(HashAlg::Sha256)
                    .to_string()
            }
        };
        if self.transfer {
            return Ok(session.host_fingerprint.lock().unwrap_or_else(PoisonError::into_inner).as_deref() == Some(z_fingerprint.as_str()));
        }
        let z_known = self
            .store
            .known_host(&session.connection.host, session.connection.port)
            .ok()
            .flatten();
        if z_known.as_deref() == Some(z_fingerprint.as_str()) {
            *session.host_fingerprint.lock().unwrap_or_else(PoisonError::into_inner) = Some(z_fingerprint);
            return Ok(true);
        }
        let (tx, rx) = oneshot::channel();
        let changed = z_known.is_some();
        if let Ok(mut trust) = session.trust.lock() {
            *trust = Some(TrustPrompt {
                fingerprint: z_fingerprint.clone(),
                changed,
                answer: tx,
            });
        }
        set_status(&session, "host_key_confirmation".into());
        match tokio::time::timeout(Duration::from_secs(120), rx).await {
            Ok(Ok(accepted)) => {
                let z_accepted = accepted
                    && (session.actor != Actor::Mcp
                        || self.store.get(&session.connection.id).is_ok_and(|c| mcp_permits(&c, &session.connection)));
                if z_accepted {
                    *session.host_fingerprint.lock().unwrap_or_else(PoisonError::into_inner) = Some(z_fingerprint);
                }
                Ok(z_accepted)
            }
            _ => {
                if let Ok(mut trust) = session.trust.lock() {
                    trust.take();
                }
                Ok(false)
            }
        }
    }
}

fn require_user(c_actor: Actor) -> Result<()> {
    ensure!(c_actor == Actor::User, "此操作仅限用户");
    Ok(())
}
// MCP 授权同时核对会话实际登录的身份: 连接改了主机、端口、用户或代理后重新授权, 不连带授权仍在线的旧身份会话和任务.
fn mcp_permits(c_current: &Connection, c_opened: &Connection) -> bool {
    c_current.mcp_allowed
        && c_current.id == c_opened.id
        && c_current.host == c_opened.host
        && c_current.port == c_opened.port
        && c_current.username == c_opened.username
        && c_current.proxy_id == c_opened.proxy_id
}
fn authorize(c_connection: &Connection, c_actor: Actor) -> Result<()> {
    if c_actor == Actor::Mcp {
        ensure!(c_connection.mcp_allowed, "此连接未授权 MCP");
    }
    Ok(())
}
// 在独立通道执行命令, 参数经标准输入传入而不拼进命令行. 成功返回 None, 失败返回 stderr 第一行.
async fn exec_with_input(c_session: &LiveSession, c_command: &str, c_input: &[u8], c_seconds: u64) -> Result<Option<String>> {
    let z_handle = c_session.handle.lock().unwrap_or_else(PoisonError::into_inner)
        .as_ref()
        .cloned()
        .context("SSH 尚未连接")?;
    let (z_code, z_stderr) = tokio::time::timeout(Duration::from_secs(c_seconds), async {
        let mut z_channel = ClosingChannel(Some(open_channel(&z_handle, c_seconds).await.context("打开命令通道失败")?));
        z_channel.exec(true, c_command).await.context("服务器未允许执行命令")?;
        z_channel.data(c_input).await.context("发送参数失败")?;
        z_channel.eof().await.context("结束参数输入失败")?;
        let mut z_code = None;
        let mut z_stderr = Vec::new();
        while let Some(z_msg) = z_channel.wait().await {
            match z_msg {
                ChannelMsg::ExtendedData { data, .. } if z_stderr.len() < 4096 => z_stderr.extend_from_slice(&data),
                ChannelMsg::ExitStatus { exit_status } => z_code = Some(exit_status),
                _ => {}
            }
        }
        Ok::<_, anyhow::Error>((z_code, z_stderr))
    })
    .await
    .with_context(|| format!("命令超过 {c_seconds} 秒未完成"))??;
    Ok((z_code != Some(0)).then(|| {
        String::from_utf8_lossy(&z_stderr)
            .lines()
            .next()
            .unwrap_or("命令执行失败")
            .chars()
            .take(200)
            .collect()
    }))
}

fn string_arg(c_args: &Value, c_key: &str) -> String {
    c_args.get(c_key).and_then(Value::as_str).unwrap_or("").to_owned()
}
fn required_string<'a>(c_args: &'a Value, c_key: &str) -> Result<&'a str> {
    c_args
        .get(c_key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .with_context(|| format!("缺少参数 {c_key}"))
}

fn validate_remote_file_path(c_path: &str) -> Result<()> {
    ensure!(
        !c_path.is_empty() && c_path.len() <= 4096,
        "远端路径长度无效"
    );
    ensure!(
        c_path != "/" && c_path != "." && c_path != "..",
        "拒绝操作根目录或相对占位路径"
    );
    ensure!(
        !c_path.chars().any(|c| c.is_control()),
        "远端路径包含控制字符"
    );
    ensure!(
        !c_path.split('/').any(|part| part == "." || part == ".."),
        "远端路径包含相对跳转分量"
    );
    ensure!(
        c_path
            .rsplit('/')
            .next()
            .is_some_and(|name| !name.is_empty()),
        "远端路径缺少文件名"
    );
    Ok(())
}
fn bounded_u16(c_args: &Value, c_key: &str, c_default: u16, c_min: u16, c_max: u16) -> u16 {
    c_args
        .get(c_key)
        .and_then(Value::as_u64)
        .map(|n| n.clamp(c_min as u64, c_max as u64) as u16)
        .unwrap_or(c_default)
}
fn set_status(c_session: &LiveSession, c_status: String) {
    if let Ok(mut s) = c_session.status.lock() {
        *s = c_status;
    }
}
// 终端结束但连接保留: 释放输入通道 (写入任务随之退出), 清空画面; 界面居中显示提示, 这里只写入失败原因.
fn shell_closed(c_session: &LiveSession, c_notice: &str) {
    c_session.shell.store(false, Ordering::Release);
    c_session.terminal_tx.lock().unwrap_or_else(PoisonError::into_inner).take();
    c_session.resize_tx.lock().unwrap_or_else(PoisonError::into_inner).take();
    // 清空旧画面并隐藏光标, 只留提示, 避免残留的光标停在旧位置像是还能输入.
    let mut z_term = c_session.terminal.lock().unwrap_or_else(PoisonError::into_inner);
    z_term.reset_screen();
    z_term.parser.process(format!("\x1b[?25l\x1b[2m{c_notice}\x1b[0m").as_bytes());
}

fn start_monitor(c_session: &Arc<LiveSession>) {
    if c_session.monitoring.swap(true, Ordering::AcqRel) {
        return;
    }
    let z_session = c_session.clone();
    tokio::spawn(async move {
        monitor_loop(z_session.clone()).await;
        z_session.monitoring.store(false, Ordering::Release);
    });
}

// 仅文件连接或终端已关闭时没有读取循环发现断线, 定时检查 SSH 连接, 断开后更新状态以便重连.
// 有终端时由终端读取结束处理; 会话被释放或换了新连接时退出.
fn watch_connection(c_session: &Arc<LiveSession>, c_handle: &Arc<client::Handle<SshHandler>>) {
    let z_session = Arc::downgrade(c_session);
    let z_handle = c_handle.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(3)).await;
            let Some(l_session) = z_session.upgrade() else { break; };
            if !l_session.handle.lock().unwrap_or_else(PoisonError::into_inner).as_ref().is_some_and(|c| Arc::ptr_eq(c, &z_handle)) {
                break;
            }
            if z_handle.is_closed() {
                if !l_session.shell.load(Ordering::Acquire) {
                    close_session(&l_session, &dropped_status(&l_session), true);
                    l_session.handle.lock().unwrap_or_else(PoisonError::into_inner).take();
                }
                break;
            }
        }
    });
}

// 意外断线的状态: 有断开原因时按错误显示 (状态栏左侧提示并可重试), 否则为已断开.
fn dropped_status(c_session: &LiveSession) -> String {
    match c_session.link_errors.lock().unwrap_or_else(PoisonError::into_inner)[0].as_deref() {
        Some(l_reason) => format!("error: 连接已断开: {l_reason}"),
        None => "disconnected".into(),
    }
}

// 按输入顺序判断提示符处的状态: 回车、换行、Ctrl+C、清行 (Ctrl+U) 结束当前行, 之后的文字 (含方向键调出历史)
// 又成为未提交的输入. 括号粘贴 (ESC[200~ … ESC[201~) 内的换行只插入编辑区, 不算提交.
// c_line 为终端的输入行状态: 是否在括号粘贴中跨块延续; 块末尾半截的起止标记留到下一块拼上再判断.
// 返回 (是否结束过一行, 结束时是否有未提交输入; None 表示没有可判断的内容).
// 按发出的按键推测命令行上未提交的字符数: 文字加 1, 退格减 1, 回车/Ctrl+C/Ctrl+U 清零; 方向键等控制序列与 Alt 组合键跳过.
// MCP 自己输入的文字不计入, 只有它提交或清空这一行时清零. 历史命令 (↑)、Tab 补全无法得知, 不改变计数.
fn line_pending(c_line: &mut (usize, bool, String), c_data: &str, c_user: bool) {
    let z_text = std::mem::take(&mut c_line.2) + c_data;
    let z_bytes = z_text.as_bytes();
    let mut i = 0;
    while i < z_bytes.len() {
        if z_bytes[i] == 0x1b {
            // 控制序列: ESC [ 参数 结尾字节, ESC O 字符, 或 ESC 字符 (Alt 组合); 不完整时留到下一块.
            let l_end = match z_bytes.get(i + 1) {
                None => None,
                Some(b'[') => z_bytes[i + 2..].iter().position(|c| (0x40..=0x7e).contains(c)).map(|c| i + 2 + c + 1),
                Some(b'O') => (i + 2 < z_bytes.len()).then_some(i + 3),
                Some(_) => Some(i + 2),
            };
            let Some(l_end) = l_end else {
                c_line.2 = z_text[i..].to_owned();
                break;
            };
            // ESC 后跟多字节字符时 l_end 落在字符中间, 用 get 避免按字节切片 panic.
            match z_text.get(i..l_end) {
                Some("\u{1b}[200~") => c_line.1 = true,
                Some("\u{1b}[201~") => c_line.1 = false,
                _ => {}
            }
            // Alt 组合可能跟着多字节字符, 跳到字符边界.
            i = (l_end..=z_bytes.len()).find(|c| z_text.is_char_boundary(*c)).unwrap_or(z_bytes.len());
            continue;
        }
        let l_char = z_text[i..].chars().next().unwrap_or_default();
        i += l_char.len_utf8();
        if !c_line.1 && matches!(l_char, '\r' | '\n' | '\u{3}' | '\u{15}') {
            c_line.0 = 0;
        } else if !c_user {
        } else if matches!(l_char, '\u{7f}' | '\u{8}') {
            c_line.0 = c_line.0.saturating_sub(1);
        } else if c_line.1 || !l_char.is_control() {
            c_line.0 += 1;
        }
    }
}

// c_dropped: 连接意外断开 (非用户断开). 此时保留传输凭据、代理和独立传输连接, 进行中的传输可自行重连续传;
// 用户断开、重连或关闭标签时再经 disconnect_session 以 false 调用清理.
fn close_session(c_session: &LiveSession, c_status: &str, c_dropped: bool) {
    set_status(c_session, c_status.into());
    if let Ok(mut cancel) = c_session.connect_cancel.lock() {
        if let Some(tx) = cancel.take() {
            let _ = tx.send(());
        }
    }
    if let Ok(mut trust) = c_session.trust.lock() {
        trust.take();
    }
    if let Ok(mut answers) = c_session.auth_answers.lock() {
        answers.take();
    }
    if let Ok(mut prompts) = c_session.auth_prompts.lock() {
        prompts.clear();
    }
    if let Ok(mut tx) = c_session.terminal_tx.lock() {
        tx.take();
    }
    if let Ok(mut tx) = c_session.resize_tx.lock() {
        tx.take();
    }
    c_session.browse_sftp.lock().unwrap_or_else(PoisonError::into_inner).take();
    clear_idle_sftp(c_session);
    c_session.file_details.lock().unwrap_or_else(PoisonError::into_inner).take();
    if let Some(l_task) = c_session.file_details_abort.lock().unwrap_or_else(PoisonError::into_inner).take() {
        l_task.abort();
    }
    if c_dropped {
        return;
    }
    // 传输连接随会话关闭; 正在建立的会由 transfer_handle 发现会话已移除后自行关闭.
    c_session.transfer_password.lock().unwrap_or_else(PoisonError::into_inner).take();
    c_session.transfer_key.lock().unwrap_or_else(PoisonError::into_inner).take();
    c_session.proxy.lock().unwrap_or_else(PoisonError::into_inner).take();
    if let Some(l_handle) = c_session.transfer.try_lock().ok().and_then(|mut c| c.0.take()) {
        tokio::spawn(async move {
            let _ = tokio::time::timeout(Duration::from_secs(2), l_handle.disconnect(Disconnect::ByApplication, "关闭连接", "zh-CN")).await;
        });
    }
}
fn safe_error(c_error: &str) -> String {
    c_error
        .chars()
        .filter(|c| !c.is_control())
        .take(512)
        .collect()
}
fn terminal_size(c_session: &LiveSession) -> (u16, u16) {
    c_session
        .terminal
        .lock()
        .map(|t| t.parser.screen().size())
        .unwrap_or((30, 100))
}
// 解析控制序列中的十进制参数, 空参数或非法内容取默认值.
fn csi_number(c_param: &[u8], c_default: u32) -> u32 {
    std::str::from_utf8(c_param).ok().and_then(|c| c.parse().ok()).unwrap_or(c_default)
}

fn tail_lines(c_text: &str, c_max: usize) -> String {
    let mut lines: VecDeque<&str> = c_text.lines().collect();
    while lines.len() > c_max {
        lines.pop_front();
    }
    lines.into_iter().collect::<Vec<_>>().join("\n")
}
fn append_limited(c_buf: &mut Vec<u8>, c_data: &[u8], c_remaining: usize) -> (usize, bool) {
    let n = c_data.len().min(c_remaining);
    c_buf.extend_from_slice(&c_data[..n]);
    (n, n < c_data.len())
}
fn publish_command_output(
    c_task: &Arc<Mutex<Task>>,
    c_stdout: &[u8],
    c_stderr: &[u8],
    c_truncated: bool,
) {
    if let Ok(mut task) = c_task.lock() {
        task.stdout = output_text(c_stdout);
        task.stderr = output_text(c_stderr);
        task.truncated = c_truncated;
    }
}
fn task_summary(c_task: &Task) -> Value {
    json!({"task_id":c_task.task_id,"session_id":c_task.session_id,"connection_id":c_task.connection.id,"created_at_ms":c_task.created_at_ms,"source":c_task.source,"client":c_task.client,"kind":c_task.kind,"label":c_task.label,"local":c_task.local,"started_at_ms":c_task.started_at_ms,"finished_at_ms":c_task.finished_at_ms,"status":c_task.status,"bytes":c_task.bytes,"total":c_task.total,"speed_bytes_per_sec":current_speed(c_task),"stdout_bytes":c_task.stdout.len(),"stderr_bytes":c_task.stderr.len(),"truncated":c_task.truncated,"exit_code":c_task.exit_code,"error":c_task.error})
}

// 终端输入中的控制键换成文字标记: [回车]、[Tab]、[Esc]、[退格]、[Ctrl+C] 等; 不用 ⏎ ⌃ 等符号, Windows 字体可能显示为方块.
// 光标行是否为密码提示, 如 "[sudo] password for user:"、"Password:"、"密码：".
fn password_prompt(c_line: &str) -> bool {
    let z_line = c_line.trim_end().to_lowercase();
    (z_line.ends_with(':') || z_line.ends_with('：')) && ["password", "passphrase", "密码", "口令"].iter().any(|c| z_line.contains(c))
}

fn visible_keys(c_data: &str) -> String {
    c_data
        .chars()
        .map(|l| match l {
            '\r' | '\n' => " [回车]".to_owned(),
            '\t' => "[Tab]".to_owned(),
            '\u{1b}' => "[Esc]".to_owned(),
            '\u{7f}' => "[退格]".to_owned(),
            l if (1..=26).contains(&(l as u32)) => format!("[Ctrl+{}]", char::from(b'A' + l as u8 - 1)),
            l if l.is_control() => String::new(),
            l => l.to_string(),
        })
        .collect()
}

// 0 未开启鼠标上报, 1 传统编码, 2 SGR 编码; 界面据此把全屏程序中的滚轮转成鼠标事件.
fn mouse_mode(c_screen: &vt100::Screen) -> u8 {
    match (c_screen.mouse_protocol_mode(), c_screen.mouse_protocol_encoding()) {
        (vt100::MouseProtocolMode::None, _) => 0,
        (_, vt100::MouseProtocolEncoding::Sgr) => 2,
        _ => 1,
    }
}

fn epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64)
        .unwrap_or(0)
}

fn actor_name(c_actor: Actor) -> &'static str {
    match c_actor {
        Actor::User => "user",
        Actor::Mcp => "mcp",
    }
}
fn output_text(c_bytes: &[u8]) -> String {
    let mut z_bytes = &c_bytes[..c_bytes.len().min(MAX_COMMAND_OUTPUT)];
    if let Err(err) = std::str::from_utf8(z_bytes) {
        if err.error_len().is_none() {
            z_bytes = &z_bytes[..err.valid_up_to()];
        }
    }
    String::from_utf8_lossy(z_bytes).into_owned()
}
fn color_hex(c_color: vt100::Color, c_default: &str) -> String {
    match c_color {
        vt100::Color::Default => c_default.to_owned(),
        vt100::Color::Idx(i) => {
            let (r, g, b) = ansi_rgb(i);
            format!("#{r:02x}{g:02x}{b:02x}")
        }
        vt100::Color::Rgb(r, g, b) => format!("#{r:02x}{g:02x}{b:02x}"),
    }
}
fn ansi_rgb(c_index: u8) -> (u8, u8, u8) {
    const C: [(u8, u8, u8); 16] = [
        (0, 0, 0),
        (205, 0, 0),
        (0, 205, 0),
        (205, 205, 0),
        (0, 0, 238),
        (205, 0, 205),
        (0, 205, 205),
        (229, 229, 229),
        (127, 127, 127),
        (255, 0, 0),
        (0, 255, 0),
        (255, 255, 0),
        (92, 92, 255),
        (255, 0, 255),
        (0, 255, 255),
        (255, 255, 255),
    ];
    if c_index < 16 {
        C[c_index as usize]
    } else if c_index < 232 {
        let n = c_index - 16;
        let f = |x: u8| if x == 0 { 0 } else { 55 + 40 * x };
        (f(n / 36), f((n / 6) % 6), f(n % 6))
    } else {
        let v = 8 + (c_index - 232) * 10;
        (v, v, v)
    }
}
// 把上传完成的临时文件改为目标名; 失败时删除临时文件, 已存在的目标保持原样.
async fn commit_upload(
    c_raw: Arc<russh_sftp::client::RawSftpSession>,
    c_part: String,
    c_remote: String,
    c_overwrite: bool,
    c_exists: bool,
    c_posix_rename: bool,
    c_permissions: Option<u32>,
) -> Result<()> {
    use russh_sftp::protocol::{FileAttributes, Packet, StatusCode};
    let z_result = async {
        // 覆盖时沿用旧文件权限 (如可执行位、0600), 新建的临时文件默认受服务器 umask 影响.
        if let Some(l_permissions) = c_permissions {
            let l_attrs = FileAttributes { permissions: Some(l_permissions & 0o7777), ..FileAttributes::default() };
            tokio::time::timeout(Duration::from_secs(60), c_raw.setstat(c_part.clone(), l_attrs))
                .await
                .context("设置远端文件权限超时")?
                .context("设置远端文件权限失败")?;
        }
        let z_rename = |c_from: String, c_to: String| {
            let l_raw = c_raw.clone();
            async move {
                tokio::time::timeout(Duration::from_secs(60), l_raw.rename(c_from, c_to))
                    .await
                    .context("远端文件改名超时")?
                    .context("远端文件改名失败")
            }
        };
        if !c_overwrite {
            // 标准 SFTP 改名不覆盖已有文件: 并发的同名上传中后完成者失败, 不会覆盖先完成者.
            z_rename(c_part.clone(), c_remote.clone()).await.context("远端目标已存在或无法改名")?;
        } else if c_posix_rename {
            // OpenSSH 扩展: 原子替换, 任何时刻目标都是完整的旧文件或新文件.
            let mut l_data = Vec::with_capacity(c_part.len() + c_remote.len() + 8);
            for l_path in [&c_part, &c_remote] {
                l_data.extend_from_slice(&(l_path.len() as u32).to_be_bytes());
                l_data.extend_from_slice(l_path.as_bytes());
            }
            match tokio::time::timeout(Duration::from_secs(60), c_raw.extended("posix-rename@openssh.com", l_data))
                .await
                .context("远端文件改名超时")??
            {
                Packet::Status(l_status) if l_status.status_code == StatusCode::Ok => {}
                Packet::Status(l_status) => bail!("远端文件改名失败: {}", l_status.error_message),
                _ => bail!("远端文件改名返回异常"),
            }
        } else if c_exists {
            // 不支持原子替换: 旧文件先改为备份名, 新文件就位后删除备份; 失败时恢复旧文件.
            let l_backup = format!("{c_part}.old");
            z_rename(c_remote.clone(), l_backup.clone()).await?;
            if let Err(err) = z_rename(c_part.clone(), c_remote.clone()).await {
                let _ = z_rename(l_backup, c_remote.clone()).await;
                return Err(err);
            }
            let _ = tokio::time::timeout(Duration::from_secs(60), c_raw.remove(l_backup)).await;
        } else {
            z_rename(c_part.clone(), c_remote.clone()).await?;
        }
        Ok(())
    }
    .await;
    if z_result.is_err() {
        let _ = tokio::time::timeout(Duration::from_secs(10), c_raw.remove(c_part)).await;
    }
    z_result
}
// 传输错误能否重连续传: 超时、SSH/SFTP 通道错误, 或连接已断开 (此时表面错误常是"通道已关闭"、"SSH 尚未连接"等).
// 服务器明确返回的状态 (无权限、磁盘满、文件不存在) 和本地读写错误重试也不会好转, 直接失败.
fn transfer_retryable(c_session: &LiveSession, c_err: &anyhow::Error) -> bool {
    use russh_sftp::{client::error::Error as SftpError, protocol::StatusCode};
    let z_network = |c: &SftpError| match c {
        SftpError::Status(l_status) => matches!(l_status.status_code, StatusCode::NoConnection | StatusCode::ConnectionLost),
        _ => true,
    };
    let z_down = c_session.handle.lock().unwrap_or_else(PoisonError::into_inner).as_ref().is_none_or(|c| c.is_closed())
        || c_session.transfer.try_lock().ok().and_then(|c| c.0.clone()).is_some_and(|c| c.is_closed());
    z_down || c_err.chain().any(|c| {
        c.is::<tokio::time::error::Elapsed>()
            || c.is::<russh::Error>()
            || c.downcast_ref::<SftpError>().is_some_and(z_network)
            // SFTP 文件读取把库错误包在 io::Error 里.
            || c.downcast_ref::<std::io::Error>().is_some_and(|l| {
                l.get_ref().and_then(|l| l.downcast_ref::<SftpError>()).is_some_and(z_network)
                    || matches!(l.kind(), std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted | std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::NotConnected)
            })
    })
}
// 本次传输的起点: 临时文件已有续传数据时要求源文件 (长度, 修改时间) 与首次一致并返回断点; 否则从头开始并记下源文件信息.
fn resume_offset(c_resume: &Mutex<(u64, Option<(u64, u64)>)>, c_part_created: &AtomicBool, c_source: (u64, u64)) -> Result<u64> {
    let mut z_resume = c_resume.lock().unwrap_or_else(PoisonError::into_inner);
    if c_part_created.load(Ordering::Acquire) && z_resume.0 > 0 {
        ensure!(z_resume.1 == Some(c_source), "源文件在中断期间被修改, 无法续传");
        return Ok(z_resume.0);
    }
    *z_resume = (0, Some(c_source));
    Ok(0)
}
// 遍历本机文件夹, 返回待建的远端目录 (根在最前)、文件列表 (本机路径, 远端路径, 大小) 与跳过项说明.
fn scan_local_directory(c_local: &Path, c_remote: &str) -> Result<(Vec<String>, Vec<(PathBuf, String, u64)>, String)> {
    let mut z_dirs = vec![c_remote.to_owned()];
    let mut z_files = Vec::new();
    let mut z_notes = String::new();
    let mut z_pending = vec![(c_local.to_path_buf(), c_remote.to_owned())];
    while let Some((l_local, l_remote)) = z_pending.pop() {
        let l_entries = match std::fs::read_dir(&l_local) {
            Ok(c) => c,
            Err(err) if l_local == c_local => return Err(anyhow::Error::from(err).context("读取上传文件夹失败")),
            Err(err) => {
                directory_note(&mut z_notes, format!("跳过 {}: {err}", l_local.display()));
                continue;
            }
        };
        for l_entry in l_entries {
            let l_entry = match l_entry {
                Ok(c) => c,
                Err(err) => {
                    directory_note(&mut z_notes, format!("跳过 {} 中的一项: {err}", l_local.display()));
                    continue;
                }
            };
            let l_path = l_entry.path();
            let l_target = l_entry.file_name().to_str().map(|c| format!("{l_remote}/{c}"));
            let Some(l_target) = l_target.filter(|c| validate_remote_file_path(c).is_ok()) else {
                directory_note(&mut z_notes, format!("跳过 {}: 文件名无效", l_path.display()));
                continue;
            };
            let l_metadata = match std::fs::symlink_metadata(&l_path) {
                Ok(c) => c,
                Err(err) => {
                    directory_note(&mut z_notes, format!("跳过 {}: {err}", l_path.display()));
                    continue;
                }
            };
            ensure!(z_files.len() + z_dirs.len() < MAX_DIRECTORY_ITEMS, "文件夹超过 {MAX_DIRECTORY_ITEMS} 项, 请分批传输");
            if l_metadata.is_dir() {
                z_dirs.push(l_target.clone());
                z_pending.push((l_path, l_target));
            } else if l_metadata.is_file() {
                z_files.push((l_path, l_target, l_metadata.len()));
            } else {
                directory_note(&mut z_notes, format!("跳过 {}: 符号链接或特殊文件", l_path.display()));
            }
        }
    }
    z_files.sort_by(|c_left, c_right| c_left.1.cmp(&c_right.1));
    Ok((z_dirs, z_files, z_notes))
}
// 读出远端目录的全部条目; 打开失败原样返回, 由调用方区分权限问题和连接问题.
async fn read_remote_directory(c_raw: &russh_sftp::client::RawSftpSession, c_path: &str) -> Result<Vec<russh_sftp::protocol::File>> {
    let z_handle = tokio::time::timeout(Duration::from_secs(60), c_raw.opendir(c_path.to_owned()))
        .await
        .context("打开远端目录超时")??
        .handle;
    let mut z_entries = Vec::new();
    let z_result = loop {
        match tokio::time::timeout(Duration::from_secs(60), c_raw.readdir(z_handle.clone())).await {
            Ok(Ok(l_page)) => z_entries.extend(l_page.files),
            Ok(Err(russh_sftp::client::error::Error::Status(l_status)))
                if l_status.status_code == russh_sftp::protocol::StatusCode::Eof => break Ok(()),
            Ok(Err(err)) => break Err(anyhow::Error::from(err)),
            Err(err) => break Err(anyhow::Error::from(err).context("读取远端目录超时")),
        }
        if z_entries.len() > MAX_DIRECTORY_ITEMS {
            break Err(anyhow::anyhow!("文件夹超过 {MAX_DIRECTORY_ITEMS} 项, 请分批传输"));
        }
    };
    let _ = tokio::time::timeout(Duration::from_secs(10), c_raw.close(z_handle)).await;
    z_result.map(|_| z_entries)
}
// 记录文件夹任务的跳过或失败项, 超出保留条数后只提示一次.
fn directory_note(c_notes: &mut String, c_text: String) {
    let z_lines = c_notes.lines().count();
    if z_lines < MAX_DIRECTORY_NOTES {
        c_notes.push_str(&safe_error(&c_text));
        c_notes.push('\n');
    } else if z_lines == MAX_DIRECTORY_NOTES {
        c_notes.push_str("... 其余条目已省略\n");
    }
}
// 远端文件名在 Windows 上可能含非法字符 (Linux 允许 : ? * 等), 下载时替换为下划线.
pub(crate) fn local_file_name(c_name: &str) -> String {
    if !cfg!(windows) {
        return c_name.to_owned();
    }
    let z_name = c_name
        .chars()
        .map(|c| if matches!(c, '<' | '>' | ':' | '"' | '|' | '?' | '*' | '\\') { '_' } else { c })
        .collect::<String>();
    // Windows 不允许文件名以点或空格结尾.
    let z_trimmed = z_name.trim_end_matches(['.', ' ']);
    if z_trimmed.is_empty() {
        return "_".into();
    }
    // 设备保留名 (CON、NUL、COM1 等, 带扩展名也算) 会写到设备而不是文件, 加前缀避开.
    let z_stem = z_trimmed.split('.').next().unwrap_or_default().to_ascii_uppercase();
    // 上标 ¹²³ 也算 (COM¹、LPT² 等).
    let z_reserved = matches!(z_stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || matches!(z_stem.strip_prefix("COM").or_else(|| z_stem.strip_prefix("LPT")), Some("1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "¹" | "²" | "³"));
    if z_reserved { format!("_{z_trimmed}") } else { z_trimmed.to_owned() }
}
// 本机同名比较键: Windows 与 macOS 默认文件系统不区分大小写, Readme 与 README 指向同一文件, 按小写比较;
// macOS 还把 é 的组合与分解两种编码视为同名, 先统一为分解形式.
pub(crate) fn local_name_key(c_name: &str) -> String {
    #[cfg(all(target_os = "macos", feature = "desktop"))]
    let c_name = icu_normalizer::DecomposingNormalizerBorrowed::new_nfd().normalize(c_name);
    if cfg!(any(windows, target_os = "macos")) { c_name.to_lowercase() } else { c_name.to_string() }
}
// 临时文件与目标同目录, 隐藏文件名带任务标识: ".名称.任务前8位.hipssh-part", 同名并发任务互不干扰.
fn part_name(c_remote: &str, c_tag: &str) -> String {
    match c_remote.rsplit_once('/') {
        Some((l_dir, l_name)) => format!("{l_dir}/.{}.{c_tag}.hipssh-part", part_stem(l_name)),
        None => format!(".{}.{c_tag}.hipssh-part", part_stem(c_remote)),
    }
}
fn local_part_path(c_local: &Path, c_tag: &str) -> PathBuf {
    let z_name = c_local.file_name().map(|c| c.to_string_lossy().into_owned()).unwrap_or_default();
    c_local.with_file_name(format!(".{}.{c_tag}.hipssh-part", part_stem(&z_name)))
}
// Windows 上杀毒软件、索引或同步服务常在文件刚关闭时打开扫描, 期间改名报拒绝访问 (5) 或共享冲突 (32);
// 不重试会导致下载失败并清掉临时文件和占位, 本地什么都不留. 逐次加长等待, 合计约 5 秒.
async fn rename_local(c_from: &Path, c_to: &Path) -> std::io::Result<()> {
    let mut i = 0;
    loop {
        match tokio::fs::rename(c_from, c_to).await {
            Err(err) if cfg!(windows) && i < 10 && matches!(err.raw_os_error(), Some(5 | 32)) => {
                i += 1;
                tokio::time::sleep(Duration::from_millis(100 * i)).await;
            }
            l_result => return l_result,
        }
    }
}
// 文件名上限常为 255 字节, 临时名另加 22 字节、覆盖备份再加 ".old": 原名截到 200 字节, 唯一性由任务标识保证.
fn part_stem(c_name: &str) -> &str {
    &c_name[..c_name.floor_char_boundary(200)]
}
// 通道守卫: 正常结束、出错或超时被丢弃时都向服务器发送关闭, 不长期占用会话名额.
// 服务器要等远端命令退出才释放会话, 常驻命令 (如监控循环) 不会因通道关闭自行结束, 先发挂断信号.
// russh 对已关闭的通道不会重复发送, 提前显式关闭也安全.
struct ClosingChannel(Option<russh::Channel<client::Msg>>);
impl std::ops::Deref for ClosingChannel {
    type Target = russh::Channel<client::Msg>;
    fn deref(&self) -> &Self::Target {
        self.0.as_ref().expect("通道已释放")
    }
}
impl std::ops::DerefMut for ClosingChannel {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut().expect("通道已释放")
    }
}
impl ClosingChannel {
    // 取消或超时时先发挂断信号再关闭, 有界等待: russh 关闭后即移除通道, 之后的信号不会再发出, 远端命令会继续运行.
    // 网络堵塞超过 3 秒时保留通道, 由守卫在后台继续发送挂断与关闭, 网络恢复后仍会终止远端命令.
    async fn shutdown(&mut self) {
        let Some(l_channel) = self.0.as_ref() else { return };
        let z_done = tokio::time::timeout(Duration::from_secs(3), async {
            let _ = l_channel.signal(russh::Sig::HUP).await;
            let _ = l_channel.eof().await;
            let _ = l_channel.close().await;
        })
        .await
        .is_ok();
        if z_done {
            self.0.take();
        }
    }
}
impl Drop for ClosingChannel {
    fn drop(&mut self) {
        if let Some(l_channel) = self.0.take() {
            tokio::spawn(async move {
                let _ = l_channel.signal(russh::Sig::HUP).await;
                let _ = l_channel.eof().await;
                let _ = l_channel.close().await;
            });
        }
    }
}

// 打开会话通道. 超时放弃后服务器仍可能随后打开该通道, 若无人关闭会一直占用服务器的会话名额
// (OpenSSH MaxSessions 默认 10), 积满后新通道全部失败; 因此超时后由后台任务等待并关闭迟到的通道.
async fn open_channel(c_handle: &Arc<client::Handle<SshHandler>>, c_seconds: u64) -> Result<russh::Channel<client::Msg>> {
    // 通道在任务内即由守卫持有: 超时或外层被取消而放弃等待时, 任务结果随之丢弃, 迟到的通道自动关闭.
    let z_handle = c_handle.clone();
    let z_open = tokio::spawn(async move { z_handle.channel_open_session().await.map(|c| ClosingChannel(Some(c))) });
    let mut z_guard = tokio::time::timeout(Duration::from_secs(c_seconds), z_open)
        .await
        // 保留超时错误本身, 传输据此判断可以重试.
        .map_err(|err| anyhow::Error::new(err).context(format!("等待服务器响应超时 ({c_seconds} 秒)")))?
        .context("打开通道任务异常")??;
    z_guard.0.take().context("通道已释放")
}
// 打开 SFTP 子系统通道. 守卫一直持有到子系统启动成功: 等待期间超时或被取消时通道随之关闭, 不占用服务器会话名额.
async fn open_sftp_stream(c_handle: &Arc<client::Handle<SshHandler>>) -> Result<russh::ChannelStream<client::Msg>> {
    // 经代理或线路繁忙时服务器回应会慢, 留足 15 秒; 失败原因 (超时或服务器拒绝, 如会话数达到 MaxSessions) 一并报出.
    let mut z_channel = ClosingChannel(Some(open_channel(c_handle, 15).await.context("打开 SFTP 通道失败")?));
    tokio::time::timeout(Duration::from_secs(10), z_channel.request_subsystem(true, "sftp"))
        .await
        .context("启动SFTP超时")??;
    Ok(z_channel.0.take().context("通道已释放")?.into_stream())
}
// 取最近放回的可用会话; 连接已断或放置过久的直接丢弃.
fn take_idle_sftp<T>(c_idle: &Mutex<Vec<IdleSftp<T>>>) -> Option<(Arc<client::Handle<SshHandler>>, T)> {
    let mut z_idle = c_idle.lock().unwrap_or_else(PoisonError::into_inner);
    while let Some((l_link, l_saved, l_sftp)) = z_idle.pop() {
        if !l_link.is_closed() && l_saved.elapsed() < TRANSFER_IDLE {
            return Some((l_link, l_sftp));
        }
    }
    None
}

fn put_idle_sftp<T>(c_idle: &Mutex<Vec<IdleSftp<T>>>, c_link: Arc<client::Handle<SshHandler>>, c_sftp: T) {
    let mut z_idle = c_idle.lock().unwrap_or_else(PoisonError::into_inner);
    if z_idle.len() < TRANSFER_IDLE_MAX {
        z_idle.push((c_link, Instant::now(), c_sftp));
    }
}

fn has_idle_sftp(c_session: &LiveSession) -> bool {
    !c_session.idle_upload.lock().unwrap_or_else(PoisonError::into_inner).is_empty()
        || !c_session.idle_download.lock().unwrap_or_else(PoisonError::into_inner).is_empty()
}

fn clear_idle_sftp(c_session: &LiveSession) {
    c_session.idle_upload.lock().unwrap_or_else(PoisonError::into_inner).clear();
    c_session.idle_download.lock().unwrap_or_else(PoisonError::into_inner).clear();
}

async fn resolve_remote_path(c_raw: &russh_sftp::client::RawSftpSession, c_path: String) -> Result<String> {
    let z_resolved = tokio::time::timeout(Duration::from_secs(10), c_raw.realpath(c_path))
        .await
        .context("解析远端目录超时")??;
    let z_path = z_resolved
        .files
        .into_iter()
        .next()
        .context("远端目录没有返回实际路径")?
        .filename;
    ensure!(!z_path.is_empty() && z_path.len() <= 4096, "远端目录路径无效");
    Ok(z_path)
}

async fn open_remote_directory(c_raw: &russh_sftp::client::RawSftpSession, c_session: &LiveSession, c_path: String) -> Result<String> {
    let z_opened = match tokio::time::timeout(Duration::from_secs(10), c_raw.opendir(c_path)).await {
        Ok(l_opened) => l_opened.map_err(anyhow::Error::from),
        Err(_) => Err(anyhow::anyhow!("打开远端目录超时")),
    };
    // 外层计时或 SFTP 库自身超时: 迟到的句柄无从关闭, 丢弃复用会话, 句柄随会话关闭释放.
    if z_opened.as_ref().is_err_and(transient_sftp_error) {
        c_session.browse_sftp.lock().unwrap_or_else(PoisonError::into_inner).take();
    }
    Ok(z_opened?.handle)
}

async fn read_remote_entries(c_raw: &russh_sftp::client::RawSftpSession, c_handle: String, c_dirs_only: bool) -> Result<(Vec<Value>, bool, Vec<(Option<u32>, Option<u32>)>)> {
    let mut entries = Vec::with_capacity(128);
    let mut truncated = false;
    let mut name_bytes = 0usize;
    let mut owner_ids = Vec::with_capacity(128);
    loop {
        let page = tokio::time::timeout(Duration::from_secs(10), c_raw.readdir(c_handle.clone()))
            .await
            .context("读取目录超时")?;
        let page = match page {
            Ok(page) => page,
            Err(russh_sftp::client::error::Error::Status(status))
                if status.status_code == russh_sftp::protocol::StatusCode::Eof =>
            {
                break;
            }
            Err(err) => return Err(err.into()),
        };
        let mut limit_reached = false;
        for file in page.files {
            if file.filename == "." || file.filename == ".." {
                continue;
            }
            // 目录树的上限只统计目录, 不能让前面的普通文件挤掉后面的子目录.
            if c_dirs_only && !file.attrs.is_dir() {
                continue;
            }
            if entries.len() >= MAX_LIST_ENTRIES
                || name_bytes.saturating_add(file.filename.len()) > MAX_LIST_NAME_BYTES
            {
                truncated = true;
                limit_reached = true;
                break;
            }
            name_bytes += file.filename.len();
            let z_kind = if file.attrs.is_dir() {
                "directory"
            } else if file.attrs.is_symlink() {
                "symlink"
            } else {
                "file"
            };
            let z_permissions = file.attrs.permissions.map(format_unix_permissions);
            let z_owner = match (file.attrs.uid, file.attrs.gid) {
                (Some(uid), Some(gid)) => Some(format!("{uid}/{gid}")),
                _ => None,
            };
            owner_ids.push((file.attrs.uid, file.attrs.gid));
            let z_modified_time = file.attrs.mtime.map(u64::from);
            let z_mode = file.attrs.permissions.map(|mode| mode & 0o777);
            entries.push(json!({"name":file.filename,"is_dir":z_kind == "directory","kind":z_kind,"size":file.attrs.size.unwrap_or(0),"modified_time":z_modified_time,"permissions":z_permissions,"mode":z_mode,"owner":z_owner}));
        }
        if limit_reached {
            break;
        }
    }
    Ok((entries, truncated, owner_ids))
}

// 无论读取成功、出错还是超时都关闭目录句柄; 关闭失败时丢弃复用会话, 不留下未关闭的句柄.
async fn close_remote_directory(c_raw: Arc<russh_sftp::client::RawSftpSession>, c_session: Arc<LiveSession>, c_handle: String) {
    let z_closed = tokio::time::timeout(Duration::from_secs(3), c_raw.close(c_handle)).await;
    if !matches!(z_closed, Ok(Ok(_))) {
        c_session.browse_sftp.lock().unwrap_or_else(PoisonError::into_inner).take();
    }
}

fn sort_remote_entries(c_entries: &mut [Value]) {
    c_entries.sort_by(|a, b| {
        b["is_dir"]
            .as_bool()
            .cmp(&a["is_dir"].as_bool())
            .then(a["name"].as_str().cmp(&b["name"].as_str()))
    })
}

// 路径上目录树尚未展开的上级目录, 由根向下; 深度与目录树上限一致.
fn missing_tree_ancestors(c_path: &str, c_expanded: Option<&std::collections::HashSet<String>>) -> Vec<String> {
    let Some(z_expanded) = c_expanded else { return Vec::new(); };
    let mut z_ancestors = Vec::new();
    let mut z_path = c_path;
    while let Some((l_parent, _)) = z_path.trim_end_matches('/').rsplit_once('/') {
        z_path = if l_parent.is_empty() { "/" } else { l_parent };
        z_ancestors.push(z_path.to_owned());
    }
    z_ancestors.reverse();
    z_ancestors.truncate(32);
    z_ancestors.retain(|c| !z_expanded.contains(c));
    z_ancestors
}

// 读取目录树节点的子目录; 句柄交给调用方在返回结果后统一关闭.
async fn read_tree_directory(c_raw: Arc<russh_sftp::client::RawSftpSession>, c_session: Arc<LiveSession>, c_path: String) -> (Option<Value>, Option<String>) {
    let Ok(z_handle) = open_remote_directory(&c_raw, &c_session, c_path.clone()).await else { return (None, None); };
    let z_listed = read_remote_entries(&c_raw, z_handle.clone(), true).await.ok().map(|(mut l_dirs, l_truncated, _)| {
        sort_remote_entries(&mut l_dirs);
        json!({"path":c_path,"entries":l_dirs,"truncated":l_truncated})
    });
    (z_listed, Some(z_handle))
}

// 建立到服务器的 TCP 连接 (连接选了代理时经代理), 再完成 SSH 握手; 主连接与传输连接共用.
// TCP 建连与代理握手各自限时, 卡住时报出具体阶段; 外层总期限还包含主机指纹确认等人工等待.
async fn ssh_connect(c_conn: &Connection, c_proxy: Option<&ProxyRoute>, c_handler: SshHandler) -> Result<client::Handle<SshHandler>> {
    let z_stream = match c_proxy {
        None => tokio::time::timeout(Duration::from_secs(30), tokio::net::TcpStream::connect((c_conn.host.as_str(), c_conn.port)))
            .await
            .map_err(|_| anyhow::anyhow!("连接服务器 {}:{} 超时", c_conn.host, c_conn.port))?
            .with_context(|| format!("无法连接服务器 {}:{}", c_conn.host, c_conn.port))?,
        Some((l_proxy, l_password)) => {
            proxy_stream(l_proxy, l_password.as_deref().map(String::as_str), &c_conn.host, c_conn.port)
                .await
                .with_context(|| format!("经代理「{}」连接失败", l_proxy.name))?
        }
    };
    let _ = z_stream.set_nodelay(true);
    let z_config = ssh_config(c_handler.transfer);
    Ok(client::connect_stream(z_config, z_stream, c_handler).await?)
}

// 经代理打开到目标的隧道: SOCKS5 (可带用户名密码) 或 HTTP CONNECT. 握手完成后返回的连接直接承载 SSH.
async fn proxy_stream(c_proxy: &crate::config::Proxy, c_password: Option<&str>, c_host: &str, c_port: u16) -> Result<tokio::net::TcpStream> {
    let mut z_stream = tokio::time::timeout(Duration::from_secs(15), tokio::net::TcpStream::connect((c_proxy.host.as_str(), c_proxy.port)))
        .await
        .map_err(|_| anyhow::anyhow!("连接代理 {}:{} 超时", c_proxy.host, c_proxy.port))?
        .with_context(|| format!("无法连接代理 {}:{}", c_proxy.host, c_proxy.port))?;
    tokio::time::timeout(Duration::from_secs(20), proxy_handshake(&mut z_stream, c_proxy, c_password, c_host, c_port))
        .await
        .map_err(|_| anyhow::anyhow!("代理无响应 (握手超过 20 秒)"))??;
    Ok(z_stream)
}

async fn proxy_handshake(c_stream: &mut tokio::net::TcpStream, c_proxy: &crate::config::Proxy, c_password: Option<&str>, c_host: &str, c_port: u16) -> Result<()> {
    let z_user = c_proxy.username.as_str();
    let z_password = c_password.unwrap_or_default();
    if c_proxy.kind == "http" {
        // IPv6 地址在 CONNECT 目标中需加方括号.
        let z_target = if c_host.contains(':') { format!("[{c_host}]:{c_port}") } else { format!("{c_host}:{c_port}") };
        let mut z_request = Zeroizing::new(format!("CONNECT {z_target} HTTP/1.1\r\nHost: {z_target}\r\n"));
        if !z_user.is_empty() {
            let l_token = Zeroizing::new(STANDARD.encode(format!("{z_user}:{z_password}")));
            z_request.push_str(&format!("Proxy-Authorization: Basic {}\r\n", l_token.as_str()));
        }
        z_request.push_str("\r\n");
        c_stream.write_all(z_request.as_bytes()).await.context("发送代理请求失败")?;
        // 逐字节读到响应头结束, 不多读隧道中随后到达的 SSH 数据.
        let mut z_head = Vec::with_capacity(256);
        while !z_head.ends_with(b"\r\n\r\n") {
            ensure!(z_head.len() < 8192, "代理响应头过长");
            z_head.push(c_stream.read_u8().await.context("代理关闭了连接")?);
        }
        let z_status = String::from_utf8_lossy(&z_head).lines().next().unwrap_or_default().to_owned();
        let z_code = z_status.split_whitespace().nth(1).unwrap_or_default();
        ensure!(z_code != "407", "代理需要认证, 或用户名密码错误");
        ensure!(z_code == "200", "代理拒绝连接: {}", safe_error(&z_status));
        return Ok(());
    }
    // SOCKS5: 协商认证方式 (无认证 / 用户名密码).
    let z_auth = !z_user.is_empty();
    c_stream.write_all(if z_auth { &[5, 2, 0, 2][..] } else { &[5, 1, 0][..] }).await.context("发送代理请求失败")?;
    let mut z_reply = [0u8; 2];
    c_stream.read_exact(&mut z_reply).await.context("代理关闭了连接")?;
    ensure!(z_reply[0] == 5, "代理不是 SOCKS5 代理");
    match z_reply[1] {
        0 => {}
        2 if z_auth => {
            ensure!(z_user.len() <= 255 && z_password.len() <= 255, "代理用户名或密码过长");
            let mut l_message = Zeroizing::new(vec![1u8, z_user.len() as u8]);
            l_message.extend_from_slice(z_user.as_bytes());
            l_message.push(z_password.len() as u8);
            l_message.extend_from_slice(z_password.as_bytes());
            c_stream.write_all(&l_message).await.context("发送代理认证失败")?;
            c_stream.read_exact(&mut z_reply).await.context("代理关闭了连接")?;
            ensure!(z_reply[1] == 0, "代理用户名或密码错误");
        }
        _ => bail!("代理不接受当前认证方式{}", if z_auth { "" } else { ", 可能需要用户名和密码" }),
    }
    // 连接请求: 地址为 IP 时按 IP 发送, 否则交给代理解析域名.
    let mut z_request = vec![5u8, 1, 0];
    match c_host.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V4(l_ip)) => { z_request.push(1); z_request.extend_from_slice(&l_ip.octets()); }
        Ok(std::net::IpAddr::V6(l_ip)) => { z_request.push(4); z_request.extend_from_slice(&l_ip.octets()); }
        Err(_) => {
            ensure!(c_host.len() <= 255, "服务器地址过长");
            z_request.push(3);
            z_request.push(c_host.len() as u8);
            z_request.extend_from_slice(c_host.as_bytes());
        }
    }
    z_request.extend_from_slice(&c_port.to_be_bytes());
    c_stream.write_all(&z_request).await.context("发送代理请求失败")?;
    let mut z_head = [0u8; 4];
    c_stream.read_exact(&mut z_head).await.context("代理关闭了连接")?;
    let z_reason = match z_head[1] {
        0 => None,
        2 => Some("代理规则不允许"),
        3 => Some("代理所在网络不可达"),
        4 => Some("代理无法到达服务器"),
        5 => Some("服务器拒绝连接"),
        6 => Some("代理连接服务器超时"),
        _ => Some("代理连接失败"),
    };
    if let Some(l_reason) = z_reason {
        bail!("{l_reason} (SOCKS5 错误码 {})", z_head[1]);
    }
    // 跳过代理返回的绑定地址与端口.
    let z_skip = match z_head[3] {
        1 => 4,
        4 => 16,
        3 => usize::from(c_stream.read_u8().await.context("代理关闭了连接")?),
        _ => bail!("代理响应格式无效"),
    } + 2;
    let mut z_rest = vec![0u8; z_skip];
    c_stream.read_exact(&mut z_rest).await.context("代理关闭了连接")?;
    Ok(())
}

// 主连接与传输连接共用的 SSH 参数.
fn ssh_config(c_transfer: bool) -> Arc<client::Config> {
    let mut z_config = client::Config::default();
    // 接收窗口决定下载在途数据上限, 剩一半时才补充. 主连接取 OpenSSH 默认的 2 MiB, 下载数据不会大量排在终端输出前面;
    // 传输专用连接上没有交互流量, 取 16 MiB, 高延迟线路上下载也能跑满带宽 (每个下载最多占用这么多缓冲).
    z_config.window_size = if c_transfer { 16 * 1024 * 1024 } else { 2 * 1024 * 1024 };
    z_config.maximum_packet_size = 32 * 1024;
    z_config.channel_buffer_size = 32;
    z_config.nodelay = true;
    // 保活: 15 秒未收到任何数据发一次, 连续 4 次仍无数据 (约 60 秒) 判定失联, 网络切换或链路中断时生效. 写入 TCP 卡住时保活无法发出,
    // 由无活动超时兜底 (收到任何数据都会重置), 结束后界面显示已断开.
    z_config.keepalive_interval = Some(Duration::from_secs(15));
    z_config.keepalive_max = 3;
    z_config.inactivity_timeout = Some(Duration::from_secs(90));
    // 旧服务器与网络设备 (CentOS 6、交换机、老 NAS) 只支持旧算法: 追加在默认列表之后, 作为兜底.
    // 协商取本端列表中第一个双方都支持的算法, 新服务器仍用安全算法; 算法列表受密钥交换签名保护, 中间人无法强迫降级.
    let mut z_kex = z_config.preferred.kex.to_vec();
    // 扩展标记 (ext-info-c 等) 不是真正的算法, 兜底算法插在它们之前.
    let z_at = z_kex.iter().position(|c| *c == russh::kex::EXTENSION_SUPPORT_AS_CLIENT).unwrap_or(z_kex.len());
    z_kex.splice(z_at..z_at, [
        russh::kex::ECDH_SHA2_NISTP256,
        russh::kex::ECDH_SHA2_NISTP384,
        russh::kex::ECDH_SHA2_NISTP521,
        russh::kex::DH_G14_SHA1,
        russh::kex::DH_GEX_SHA1,
        russh::kex::DH_G1_SHA1,
    ]);
    z_config.preferred.kex = z_kex.into();
    let mut z_cipher = z_config.preferred.cipher.to_vec();
    z_cipher.extend([russh::cipher::AES_128_GCM, russh::cipher::AES_256_CBC, russh::cipher::AES_192_CBC, russh::cipher::AES_128_CBC]);
    z_config.preferred.cipher = z_cipher.into();
    let mut z_mac = z_config.preferred.mac.to_vec();
    z_mac.extend([russh::mac::HMAC_SHA1_ETM, russh::mac::HMAC_SHA1]);
    z_config.preferred.mac = z_mac.into();
    Arc::new(z_config)
}
fn transient_sftp_error(c_err: &anyhow::Error) -> bool {
    let z_text = format!("{c_err:#}");
    ["sender dropped", "session closed", "Timeout", "超时"].iter().any(|c| z_text.contains(c))
}
// 超过 2 秒没有新进度视为停滞, 显示 0, 不沿用上一次的速度.
fn current_speed(c_task: &Task) -> u64 {
    if c_task.status == "running" && c_task.last_progress_at.elapsed() > Duration::from_secs(2) {
        0
    } else {
        c_task.speed_bytes_per_sec
    }
}
fn mark_task(c_task: &Arc<Mutex<Task>>, c_status: &str, c_error: Option<String>) {
    if let Ok(mut t) = c_task.lock() {
        let z_was_running = t.status == "running";
        t.status = c_status.into();
        t.error = c_error;
        // 开始与结束时间用于详情中的用时和平均速度.
        if c_status == "running" {
            // 自动重连后再次进入进行中时保留首次开始时间, 用时和平均速度按整个任务计算.
            t.started_at_ms.get_or_insert_with(epoch_ms);
            // 测速从真正开始传输算起, 不含排队等待的时间; 文件夹任务每个文件都会标记, 已在进行中时不重置, 否则小文件测不出速度.
            if !z_was_running {
                t.last_progress_at = Instant::now();
                t.last_progress_bytes = t.bytes;
            }
        } else if matches!(c_status, "completed" | "failed" | "cancelled") {
            t.finished_at_ms = Some(epoch_ms());
        }
    }
}
// c_bytes、c_total 为当前文件的进度; 文件夹任务另累计到整个文件夹.
fn update_progress(c_task: &Arc<Mutex<Task>>, c_bytes: u64, c_total: u64) {
    if let Ok(mut t) = c_task.lock() {
        let z_bytes = t.base_bytes + c_bytes;
        let now = Instant::now();
        let elapsed = now.duration_since(t.last_progress_at).as_secs_f64();
        if elapsed >= 0.5 {
            // 平滑显示速度: 单个采样受请求确认批次影响会忽高忽低.
            let l_sample = (z_bytes.saturating_sub(t.last_progress_bytes)) as f64 / elapsed;
            t.speed_bytes_per_sec = if t.speed_bytes_per_sec == 0 {
                l_sample as u64
            } else {
                (t.speed_bytes_per_sec as f64 * 0.6 + l_sample * 0.4) as u64
            };
            t.last_progress_bytes = z_bytes;
            t.last_progress_at = now;
        }
        t.bytes = z_bytes;
        if t.directory {
            t.file_bytes = c_bytes;
            t.file_total = c_total;
            // 文件可能在扫描后变大.
            t.total = t.total.map(|c| c.max(z_bytes));
        } else {
            t.total = Some(c_total);
        }
    }
}
fn cell_text(c_cell: &vt100::Cell) -> &str {
    if c_cell.has_contents() {
        c_cell.contents()
    } else {
        " "
    }
}
// 生成一页终端网格的文字片段; c_shift 为该页首行从历史顶端算起的行号.
fn page_runs(c_screen: &vt100::Screen, c_rows: u16, c_shift: i32, c_runs: &mut Vec<Value>) {
    for r in 0..c_rows {
        let mut c = 0;
        let mut run: Option<(u16, String, CellStyle, u16)> = None;
        while c < c_screen.size().1.min(MAX_TERMINAL_COLS) {
            let Some(cell) = c_screen.cell(r, c) else {
                c += 1;
                continue;
            };
            if cell.is_wide_continuation() {
                c += 1;
                continue;
            }
            let text = if cell.has_contents() {
                cell.contents()
            } else {
                " "
            };
            let width = if cell.is_wide() { 2 } else { 1 };
            let style = (
                cell.fgcolor(),
                cell.bgcolor(),
                cell.bold(),
                cell.underline(),
                cell.inverse(),
                cell.dim(),
                cell.italic(),
            );
            let visible = cell.has_contents()
                || style.0 != vt100::Color::Default
                || style.1 != vt100::Color::Default
                || style.3
                || style.4;
            if !visible {
                if let Some(active) = run.take() {
                    c_runs.push(run_json(i32::from(r) + c_shift, active));
                }
                c += width;
                continue;
            }
            let can_extend = run.as_ref().is_some_and(
                |(start, text, run_style, run_width)| {
                    *run_style == style
                        && *start + *run_width == c
                        && width == 1
                        && text.is_ascii()
                        && cell_text(cell).is_ascii()
                },
            );
            if can_extend {
                if let Some((_, text, _, run_width)) = run.as_mut() {
                    text.push_str(cell_text(cell));
                    *run_width += width;
                }
            } else {
                if let Some(active) = run.take() {
                    c_runs.push(run_json(i32::from(r) + c_shift, active));
                }
                run = Some((c, text.to_owned(), style, width));
            }
            c += width;
        }
        if let Some(active) = run {
            c_runs.push(run_json(i32::from(r) + c_shift, active));
        }
    }
}

// 前景, 背景, 粗体, 下划线, 反显, 暗淡, 斜体.
type CellStyle = (vt100::Color, vt100::Color, bool, bool, bool, bool, bool);

fn run_json(c_row: i32, c_run: (u16, String, CellStyle, u16)) -> Value {
    let (col, text, (fg, bg, bold, underline, inverse, dim, italic), width) = c_run;
    let z_fg = color_hex(fg, "#d0d0d0");
    let z_bg = color_hex(bg, "#000000");
    let z_fg_default = if inverse {
        bg == vt100::Color::Default
    } else {
        fg == vt100::Color::Default
    };
    let z_bg_default = if inverse {
        fg == vt100::Color::Default
    } else {
        bg == vt100::Color::Default
    };
    json!({"row":c_row,"col":col,"text":text,"fg":if inverse{&z_bg}else{&z_fg},"bg":if inverse{&z_fg}else{&z_bg},"fg_default":z_fg_default,"bg_default":z_bg_default,"bold":bold,"underline":underline,"inverse":inverse,"dim":dim,"italic":italic,"width":width})
}

async fn resolve_file_times(
    c_session: &Arc<LiveSession>,
    c_handle: &Arc<client::Handle<SshHandler>>,
    c_entries: &mut [Value],
) -> (Option<String>, Option<String>) {
    let mut z_timestamps = c_entries
        .iter()
        .filter_map(|c| c.get("modified_time").and_then(Value::as_u64))
        .collect::<Vec<_>>();
    z_timestamps.sort_unstable();
    z_timestamps.dedup();
    // 同一会话的查询串行, 不为同时刷新的目录重复启动转换进程.
    let mut z_cache = c_session.file_time_cache.lock().await;
    if let Some(z_times) = z_cache.as_ref() {
        if z_times.expires_at > Instant::now()
            && (z_times.timezone.is_none()
                || z_timestamps
                    .iter()
                    .all(|c| z_times.formatted.contains_key(c)))
        {
            return apply_file_times(z_times, c_entries);
        }
    }
    let z_times = match query_file_times(c_handle, &z_timestamps).await {
        Ok(l_times) => l_times,
        Err(err) => ServerFileTimes {
            timezone: None,
            formatted: HashMap::new(),
            hint: Some(format!(
                "服务器时区查询失败, 修改时间按 UTC 显示: {}",
                safe_error(&err.to_string())
            )),
            expires_at: Instant::now() + Duration::from_secs(60),
        },
    };
    let z_result = apply_file_times(&z_times, c_entries);
    *z_cache = Some(z_times);
    z_result
}

async fn query_file_times(
    c_handle: &Arc<client::Handle<SshHandler>>,
    c_timestamps: &[u64],
) -> Result<ServerFileTimes> {
    ensure!(c_timestamps.len() <= MAX_LIST_ENTRIES, "远端文件时间超过 {MAX_LIST_ENTRIES} 项上限");
    let mut z_input = String::from("now\n");
    for z_timestamp in c_timestamps {
        z_input.push_str(&format!("@{z_timestamp}\n"));
    }
    ensure!(z_input.len() <= 512 * 1024, "远端文件时间输入超过512KiB上限");
    // 时间戳仅走标准输入, 文件名和路径不会拼入远端命令. 两种工具都按日期查询本地时区规则.
    let z_command = r#"LC_ALL=C; export LC_ALL
z_timezone=${TZ-}
if [ -z "$z_timezone" ]; then
    z_timezone=$(readlink /etc/localtime 2>/dev/null)
    case "$z_timezone" in */zoneinfo/*) z_timezone=${z_timezone#*/zoneinfo/};; *) z_timezone=;; esac
fi
printf 'HIPSSH_ZONE %s\n' "$z_timezone"
if date --version >/dev/null 2>&1; then
    exec date --file=- '+HIPSSH_TIME %s %Y-%m-%d %H:%M %Z %z'
elif command -v python3 >/dev/null 2>&1; then
    exec python3 -c 'import sys,time
if hasattr(time,"tzset"): time.tzset()
for z_line in sys.stdin:
    z_line=z_line.strip()
    z_epoch=int(time.time()) if z_line=="now" else int(z_line[1:])
    print("HIPSSH_TIME",z_epoch,time.strftime("%Y-%m-%d %H:%M %Z %z",time.localtime(z_epoch)))'
else
    exit 127
fi"#;
    // 守卫负责关闭: 超时或切换目录中止查询时也会关闭通道, 不留下等待输入的远端进程.
    let mut z_channel = None;
    let z_result = tokio::time::timeout(Duration::from_secs(5), async {
        z_channel = Some(ClosingChannel(Some(
            open_channel(c_handle, 5).await.context("打开服务器时间通道失败")?,
        )));
        let z_channel = z_channel.as_mut().unwrap();
        z_channel
            .exec(true, z_command)
            .await
            .context("服务器未允许执行时间转换命令")?;
        z_channel
            .data(z_input.as_bytes())
            .await
            .context("发送文件时间失败")?;
        z_channel.eof().await.context("结束文件时间输入失败")?;
        let mut z_output = Vec::with_capacity(4096);
        let mut z_output_bytes = 0usize;
        let mut z_exit_status = None;
        while let Some(z_msg) = z_channel.wait().await {
            match z_msg {
                ChannelMsg::Data { data } => {
                    z_output_bytes = z_output_bytes.saturating_add(data.len());
                    ensure!(z_output_bytes <= 2 * 1024 * 1024, "服务器时间输出超过2MiB上限");
                    z_output.extend_from_slice(&data);
                }
                ChannelMsg::ExtendedData { data, .. } => {
                    z_output_bytes = z_output_bytes.saturating_add(data.len());
                    ensure!(z_output_bytes <= 2 * 1024 * 1024, "服务器时间输出超过2MiB上限");
                }
                ChannelMsg::ExitStatus { exit_status } => z_exit_status = Some(exit_status),
                _ => {}
            }
        }
        ensure!(
            z_exit_status == Some(0),
            "服务器没有可用的 GNU date/Python 3 批量转换或命令执行失败"
        );
        Ok::<_, anyhow::Error>(z_output)
    })
    .await;
    drop(z_channel);
    let z_output = z_result.context("读取服务器文件时间超时")??;
    parse_file_times(&z_output, c_timestamps)
}

fn parse_file_times(c_output: &[u8], c_timestamps: &[u64]) -> Result<ServerFileTimes> {
    let z_text = std::str::from_utf8(c_output).context("服务器时间输出不是UTF-8")?;
    let mut z_zone = None;
    let mut z_current_zone = None;
    let mut z_formatted = HashMap::with_capacity(c_timestamps.len());
    for z_line in z_text.lines() {
        if let Some(z_name) = z_line.strip_prefix("HIPSSH_ZONE ") {
            ensure!(
                z_zone.is_none()
                    && z_name.len() <= 128
                    && !z_name.chars().any(char::is_control),
                "服务器时区名称无效"
            );
            z_zone = Some(z_name);
            continue;
        }
        let Some(z_time) = z_line.strip_prefix("HIPSSH_TIME ") else {
            continue;
        };
        let (z_timestamp, z_display) = z_time.split_once(' ').context("服务器时间行无效")?;
        let z_calendar = z_display.get(..16).context("服务器时间格式无效")?;
        ensure!(
            z_display.len() <= 64
                && !z_display.chars().any(char::is_control)
                && z_calendar.bytes().enumerate().all(|(i, c)| match i {
                    4 | 7 => c == b'-',
                    10 => c == b' ',
                    13 => c == b':',
                    _ => c.is_ascii_digit(),
                }),
            "服务器时间格式无效"
        );
        let z_offset = z_display.rsplit_once(' ').context("服务器时间缺少时区")?.1;
        ensure!(
            z_offset.len() == 5
                && matches!(z_offset.as_bytes()[0], b'+' | b'-')
                && z_offset.as_bytes()[1..].iter().all(u8::is_ascii_digit),
            "服务器时间缺少有效UTC偏移"
        );
        if z_current_zone.is_none() {
            z_current_zone = Some(
                z_display
                    .get(17..)
                    .context("服务器时间缺少时区名称")?
                    .trim()
                    .to_owned(),
            );
            continue;
        }
        let z_timestamp: u64 = z_timestamp.parse().context("服务器时间戳无效")?;
        ensure!(
            c_timestamps.binary_search(&z_timestamp).is_ok()
                && !z_formatted.contains_key(&z_timestamp),
            "服务器时间戳重复或超出请求范围"
        );
        z_formatted.insert(z_timestamp, z_display.to_owned());
    }
    ensure!(
        z_formatted.len() == c_timestamps.len(),
        "服务器没有返回全部文件时间"
    );
    let z_current_zone = z_current_zone.context("服务器没有返回当前时区")?;
    let z_timezone = match z_zone.filter(|c| !c.is_empty()) {
        Some(z_name) => format!("{z_name} ({z_current_zone})"),
        None => z_current_zone,
    };
    Ok(ServerFileTimes {
        timezone: Some(z_timezone),
        formatted: z_formatted,
        hint: None,
        expires_at: Instant::now() + Duration::from_secs(300),
    })
}

fn apply_file_times(
    c_times: &ServerFileTimes,
    c_entries: &mut [Value],
) -> (Option<String>, Option<String>) {
    for z_entry in c_entries {
        z_entry["modified_text"] = z_entry
            .get("modified_time")
            .and_then(Value::as_u64)
            .and_then(|c| c_times.formatted.get(&c))
            .map(|c| Value::String(c.clone()))
            .unwrap_or(Value::Null);
    }
    (c_times.timezone.clone(), c_times.hint.clone())
}

async fn resolve_file_owners(
    c_session: &Arc<LiveSession>,
    c_handle: &Arc<client::Handle<SshHandler>>,
    c_owner_ids: &[(Option<u32>, Option<u32>)],
    c_entries: &mut [Value],
) {
    const IDENTITY_CACHE_TTL: Duration = Duration::from_secs(600);
    const MAX_IDENTITY_LOOKUPS: usize = 256;
    const MAX_IDENTITY_CACHE: usize = 1024;
    let z_now = Instant::now();
    let mut z_missing = Vec::new();
    if let Ok(mut z_cache) = c_session.identity_cache.lock() {
        z_cache.retain(|_, (_, expiry)| *expiry > z_now);
        for (uid, gid) in c_owner_ids {
            for (kind, id) in [('U', *uid), ('G', *gid)] {
                if let Some(id) = id {
                    let z_key = (kind, id);
                    if !z_cache.contains_key(&z_key)
                        && !z_missing.contains(&z_key)
                        && z_missing.len() < MAX_IDENTITY_LOOKUPS
                    {
                        z_missing.push(z_key);
                    }
                }
            }
        }
    }
    if !z_missing.is_empty() {
        let mut z_specs = String::new();
        for (kind, id) in &z_missing {
            z_specs.push_str(&format!(" {kind}:{id}"));
        }
        let z_command = format!(
            "for spec in{z_specs}; do kind=${{spec%%:*}}; id=${{spec#*:}}; if [ \"$kind\" = U ]; then name=$(getent passwd \"$id\" 2>/dev/null | cut -d: -f1); [ -n \"$name\" ] || name=$(id -un \"$id\" 2>/dev/null); else name=$(getent group \"$id\" 2>/dev/null | cut -d: -f1); fi; case \"$name\" in *[!A-Za-z0-9_.@+-]*) name=;; esac; printf 'HIPSSH_ID\\t%s\\t%s\\t%s\\n' \"$kind\" \"$id\" \"$name\"; done"
        );
        let z_resolved = tokio::time::timeout(Duration::from_secs(5), async {
            let mut z_channel = ClosingChannel(Some(open_channel(c_handle, 5).await.ok()?));
            tokio::time::timeout(Duration::from_secs(5), z_channel.exec(true, z_command))
                .await
                .ok()?
                .ok()?;
            let mut z_output = Vec::with_capacity(4096);
            while let Some(msg) = z_channel.wait().await {
                if let ChannelMsg::Data { data } | ChannelMsg::ExtendedData { data, .. } = msg {
                    if z_output.len().saturating_add(data.len()) > 16 * 1024 {
                        return None;
                    }
                    z_output.extend_from_slice(&data);
                }
            }
            Some(z_output)
        })
        .await
        .ok()
        .flatten();
        // 查询成功但没有名字的 ID 长期缓存; 查询失败 (超时、通道打不开) 只短暂缓存,
        // 避免网络恢复后仍长时间显示数字, 也不会每次刷新都重复等待.
        let z_expiry = z_now + if z_resolved.is_some() { IDENTITY_CACHE_TTL } else { Duration::from_secs(30) };
        if let Ok(mut z_cache) = c_session.identity_cache.lock() {
            for z_key in &z_missing {
                cache_file_identity(&mut z_cache, *z_key, None, z_expiry, MAX_IDENTITY_CACHE);
            }
        }
        if let Some(z_output) = z_resolved {
            if let Ok(mut z_cache) = c_session.identity_cache.lock() {
                for z_line in String::from_utf8_lossy(&z_output).lines() {
                    let mut z_parts = z_line.splitn(4, '\t');
                    if z_parts.next() != Some("HIPSSH_ID") {
                        continue;
                    }
                    let (Some(z_kind), Some(z_id), Some(z_name)) =
                        (z_parts.next(), z_parts.next(), z_parts.next())
                    else {
                        continue;
                    };
                    let (Some(z_kind), Ok(z_id)) = (z_kind.chars().next(), z_id.parse::<u32>())
                    else {
                        continue;
                    };
                    if z_missing.contains(&(z_kind, z_id))
                        && z_name.len() <= 256
                        && !z_name.is_empty()
                        && !z_name.chars().any(|c| c.is_control() || c == '/')
                    {
                        cache_file_identity(
                            &mut z_cache,
                            (z_kind, z_id),
                            Some(z_name.to_owned()),
                            z_now + IDENTITY_CACHE_TTL,
                            MAX_IDENTITY_CACHE,
                        );
                    }
                }
            }
        }
    }
    let Ok(z_cache) = c_session.identity_cache.lock() else {
        return;
    };
    for (i, (uid, gid)) in c_owner_ids.iter().enumerate() {
        if let (Some(uid), Some(gid)) = (uid, gid) {
            let z_uid = z_cache
                .get(&('U', *uid))
                .and_then(|(name, _)| name.as_deref())
                .unwrap_or_else(|| "");
            let z_gid = z_cache
                .get(&('G', *gid))
                .and_then(|(name, _)| name.as_deref())
                .unwrap_or_else(|| "");
            let z_owner = format!(
                "{}/{}",
                if z_uid.is_empty() {
                    uid.to_string()
                } else {
                    z_uid.to_owned()
                },
                if z_gid.is_empty() {
                    gid.to_string()
                } else {
                    z_gid.to_owned()
                }
            );
            c_entries[i]["owner"] = json!(z_owner);
        }
    }
}

fn cache_file_identity(
    c_cache: &mut HashMap<(char, u32), (Option<String>, Instant)>,
    c_key: (char, u32),
    c_name: Option<String>,
    c_expiry: Instant,
    c_limit: usize,
) {
    if !c_cache.contains_key(&c_key) && c_cache.len() >= c_limit {
        if let Some(z_oldest) = c_cache
            .iter()
            .min_by_key(|(_, (_, expiry))| *expiry)
            .map(|(key, _)| *key)
        {
            c_cache.remove(&z_oldest);
        }
    }
    c_cache.insert(c_key, (c_name, c_expiry));
}

fn format_unix_permissions(c_mode: u32) -> String {
    let mut z_chars = ['-'; 10];
    z_chars[0] = match c_mode & 0o170000 {
        0o040000 => 'd',
        0o120000 => 'l',
        0o010000 => 'p',
        0o020000 => 'c',
        0o060000 => 'b',
        0o140000 => 's',
        _ => '-',
    };
    for (i, bit) in [
        0o400, 0o200, 0o100, 0o040, 0o020, 0o010, 0o004, 0o002, 0o001,
    ]
    .into_iter()
    .enumerate()
    {
        if c_mode & bit != 0 {
            z_chars[i + 1] = ['r', 'w', 'x'][i % 3];
        }
    }
    if c_mode & 0o4000 != 0 {
        z_chars[3] = if c_mode & 0o100 != 0 { 's' } else { 'S' };
    }
    if c_mode & 0o2000 != 0 {
        z_chars[6] = if c_mode & 0o010 != 0 { 's' } else { 'S' };
    }
    if c_mode & 0o1000 != 0 {
        z_chars[9] = if c_mode & 0o001 != 0 { 't' } else { 'T' };
    }
    z_chars.iter().collect()
}

// 监控使用一条常驻通道: 远端脚本每秒输出一次 CPU、内存与网络, 每 3 次附带进程列表 (读取全部 /proc/*/stat 开销较大),
// 不必每次新开通道. 速率按服务器 /proc/uptime 计算, 不受网络延迟抖动影响. 通道中断后 3 秒重连.
// 偶发中断自动重连且保留上次数据, 不提示; 连续约 15 秒取不到数据才显示"监控暂不可用".
async fn monitor_loop(c_session: Arc<LiveSession>) {
    let mut z_failed_since: Option<Instant> = None;
    loop {
        if c_session.status.lock().map(|s| s.as_str() != "connected").unwrap_or(true) {
            return;
        }
        let z_started = Instant::now();
        if let Err(err) = monitor_stream(&c_session).await {
            // 运行过一段时间才中断的视为新一轮失败的开始.
            if z_started.elapsed() > Duration::from_secs(15) {
                z_failed_since = None;
            }
            let l_since = *z_failed_since.get_or_insert(z_started);
            if l_since.elapsed() >= Duration::from_secs(15) {
                apply_monitor_error(&c_session, &err);
            }
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

// exec 2>/dev/null: 读取进程信息时进程恰好退出会产生错误输出, 不能混入采样数据.
const MONITOR_SCRIPT: &str = "exec 2>/dev/null; i=0; while :; do if [ $((i % 3)) -eq 0 ]; then count=0; truncated=0; printf 'HIPSSH_PROC_BEGIN\\n'; for f in /proc/[0-9]*/stat; do [ -r \"$f\" ] || continue; if [ \"$count\" -ge 512 ]; then truncated=1; break; fi; line=; while IFS= read -r part || [ -n \"$part\" ]; do line=\"$line$part\"; done < \"$f\"; printf 'P\\t%s\\n' \"$line\"; count=$((count+1)); done; printf 'HIPSSH_PROC_END\\t%s\\n' \"$truncated\"; printf 'HIPSSH_PAGE_SIZE\\t%s\\n' \"$(getconf PAGESIZE 2>/dev/null)\"; printf 'HIPSSH_CLK_TCK\\t%s\\n' \"$(getconf CLK_TCK 2>/dev/null)\"; fi; read up rest < /proc/uptime; printf 'HIPSSH_UPTIME\\t%s\\n' \"$up\"; cat /proc/stat /proc/meminfo; printf 'HIPSSH_NET_BEGIN\\n'; cat /proc/net/dev; printf 'HIPSSH_NET_END\\nHIPSSH_SAMPLE_END\\n'; i=$((i+1)); sleep 1; done";

async fn monitor_stream(c_session: &Arc<LiveSession>) -> Result<()> {
    const SAMPLE_END: &[u8] = b"HIPSSH_SAMPLE_END\n";
    let z_handle = c_session.handle.lock().unwrap_or_else(PoisonError::into_inner)
        .as_ref()
        .cloned()
        .context("SSH 尚未连接")?;
    // 守卫保证通道在出错、卡住或会话结束时关闭, 不占用服务器会话名额.
    let mut channel = ClosingChannel(Some(open_channel(&z_handle, 5).await.context("打开监控通道超时")?));
    tokio::time::timeout(Duration::from_secs(5), channel.exec(true, MONITOR_SCRIPT))
        .await
        .context("启动远端监控命令超时")??;
    let mut z_previous: Option<ProcSample> = None;
    let mut z_previous_proc: Option<ProcSample> = None;
    let mut z_buffer = Vec::with_capacity(64 * 1024);
    loop {
        // 正常每秒都有输出, 长时间无输出说明命令卡住, 关闭后重连.
        let z_message = tokio::time::timeout(Duration::from_secs(10), channel.wait())
            .await
            .context("远端监控无响应")?;
        match z_message {
            Some(ChannelMsg::Data { data }) => {
                z_buffer.extend_from_slice(&data);
                ensure!(z_buffer.len() <= 1024 * 1024, "远端监控采样输出超过1MiB上限");
                while let Some(l_end) = z_buffer.windows(SAMPLE_END.len()).position(|c| c == SAMPLE_END) {
                    let l_chunk = z_buffer.drain(..l_end + SAMPLE_END.len()).collect::<Vec<_>>();
                    let l_sample = parse_proc_sample(&String::from_utf8_lossy(&l_chunk))?;
                    apply_monitor_sample(c_session, &mut z_previous, &mut z_previous_proc, l_sample);
                }
            }
            Some(ChannelMsg::Failure) => bail!("服务器拒绝执行监控命令"),
            Some(ChannelMsg::Eof | ChannelMsg::Close) | None => bail!("远端监控已结束"),
            Some(_) => {}
        }
    }
}

// 两次采样的间隔: 优先用服务器运行时间, 缺少时退回本机收到的时间.
fn sample_elapsed(c_new: &ProcSample, c_old: &ProcSample) -> f64 {
    match (c_new.uptime, c_old.uptime) {
        (Some(l_new), Some(l_old)) if l_new > l_old => l_new - l_old,
        _ => c_new.sampled_at.duration_since(c_old.sampled_at).as_secs_f64(),
    }
    .max(0.001)
}

fn apply_monitor_sample(
    c_session: &LiveSession,
    c_previous: &mut Option<ProcSample>,
    c_previous_proc: &mut Option<ProcSample>,
    z_sample: ProcSample,
) {
    let (z_cpu_percent, z_rx_rate, z_tx_rate, mut z_interfaces) =
        if let Some(z_old) = c_previous.take() {
            let z_elapsed = sample_elapsed(&z_sample, &z_old);
            let z_cpu_percent = if z_sample.cpu_total > z_old.cpu_total {
                (z_sample
                    .cpu_total
                    .saturating_sub(z_old.cpu_total)
                    .saturating_sub(z_sample.cpu_idle.saturating_sub(z_old.cpu_idle)))
                    as f64
                    / (z_sample.cpu_total - z_old.cpu_total) as f64
                    * 100.0
            } else {
                0.0
            };
            let z_old_interfaces: HashMap<&str, &InterfaceSample> = z_old
                .interfaces
                .iter()
                .map(|i| (i.name.as_str(), i))
                .collect();
            let (z_interfaces, z_rx, z_tx) = build_interface_metrics(
                &z_sample.interfaces,
                &z_old_interfaces,
                z_elapsed,
            );
            (z_cpu_percent.clamp(0.0, 100.0), z_rx, z_tx, z_interfaces)
        } else {
            let (z_interfaces, _, _) =
                build_interface_metrics(&z_sample.interfaces, &HashMap::new(), 1.0);
            (0.0, 0, 0, z_interfaces)
        };
    // 进程列表每 3 次采样才有一次, 其间沿用上一次的结果; 进程 CPU 按相邻两次进程采样计算.
    let z_processes = z_sample.has_processes.then(|| {
        let l_processes = match c_previous_proc.as_ref() {
            Some(l_old) => build_process_metrics(&z_sample, l_old, sample_elapsed(&z_sample, l_old)),
            None => build_process_metrics(&z_sample, &z_sample, 1.0),
        };
        (l_processes, z_sample.processes_truncated || z_sample.processes.len() > 128)
    });
    if let Ok(mut monitor) = c_session.monitor.lock() {
        let mut history = monitor
            .as_ref()
            .map(|m| m.history.clone())
            .unwrap_or_default();
        push_monitor_history(&mut history, z_cpu_percent, z_rx_rate, z_tx_rate);
        for z_interface in &mut z_interfaces {
            if let Some(z_old_monitor) = monitor.as_ref() {
                if let Some(z_old_interface) = z_old_monitor
                    .interfaces
                    .iter()
                    .find(|i| i.name == z_interface.name)
                {
                    z_interface.history = z_old_interface.history.clone();
                }
            }
            push_interface_history(
                &mut z_interface.history,
                z_interface.rx_bytes_per_sec,
                z_interface.tx_bytes_per_sec,
            );
        }
        *monitor = Some(Monitor {
            cpu_percent: z_cpu_percent,
            memory_used_bytes: z_sample.memory_used,
            memory_total_bytes: z_sample.memory_total,
            swap_used_bytes: z_sample.swap_used,
            swap_total_bytes: z_sample.swap_total,
            memory_cache_bytes: z_sample.memory_cache,
            rx_bytes_per_sec: z_rx_rate,
            tx_bytes_per_sec: z_tx_rate,
            processes_truncated: z_processes.as_ref().map(|c| c.1).unwrap_or_else(|| monitor.as_ref().is_some_and(|m| m.processes_truncated)),
            processes: match z_processes {
                Some((l_processes, _)) => l_processes,
                None => monitor.as_ref().map(|m| m.processes.clone()).unwrap_or_default(),
            },
            interfaces: z_interfaces,
            interfaces_truncated: z_sample.interfaces_truncated,
            error: None,
            history,
        });
    }
    if z_sample.has_processes {
        *c_previous_proc = Some(z_sample.clone());
    }
    *c_previous = Some(z_sample);
}

fn apply_monitor_error(c_session: &LiveSession, err: &anyhow::Error) {
    if let Ok(mut monitor) = c_session.monitor.lock() {
        let prior = monitor.as_ref();
        *monitor = Some(Monitor {
            cpu_percent: prior.as_ref().map(|m| m.cpu_percent).unwrap_or(0.0),
            memory_used_bytes: prior.as_ref().map(|m| m.memory_used_bytes).unwrap_or(0),
            memory_total_bytes: prior
                .as_ref()
                .map(|m| m.memory_total_bytes)
                .unwrap_or(0),
            swap_used_bytes: prior.as_ref().map(|m| m.swap_used_bytes).unwrap_or(0),
            swap_total_bytes: prior.as_ref().map(|m| m.swap_total_bytes).unwrap_or(0),
            memory_cache_bytes: prior
                .as_ref()
                .map(|m| m.memory_cache_bytes)
                .unwrap_or(0),
            rx_bytes_per_sec: 0,
            tx_bytes_per_sec: 0,
            processes: prior.map(|m| m.processes.clone()).unwrap_or_default(),
            processes_truncated: prior.map(|m| m.processes_truncated).unwrap_or(false),
            interfaces: prior
                .map(|m| {
                    m.interfaces
                        .iter()
                        .cloned()
                        .map(|mut c_interface| {
                            c_interface.rx_bytes_per_sec = 0;
                            c_interface.tx_bytes_per_sec = 0;
                            c_interface
                        })
                        .collect()
                })
                .unwrap_or_default(),
            interfaces_truncated: prior
                .map(|m| m.interfaces_truncated)
                .unwrap_or(false),
            error: Some(safe_error(&format!("{err:#}"))),
            history: prior.map(|m| m.history.clone()).unwrap_or_default(),
        });
    }
}

fn push_monitor_history(c_history: &mut MonitorHistory, c_cpu: f64, c_rx: u64, c_tx: u64) {
    if c_history.cpu_percent.len() >= MAX_MONITOR_HISTORY {
        c_history.cpu_percent.pop_front();
        c_history.rx_bytes_per_sec.pop_front();
        c_history.tx_bytes_per_sec.pop_front();
    }
    c_history.cpu_percent.push_back(c_cpu);
    c_history.rx_bytes_per_sec.push_back(c_rx);
    c_history.tx_bytes_per_sec.push_back(c_tx);
}

fn parse_proc_sample(c_text: &str) -> Result<ProcSample> {
    ensure!(c_text.lines().any(|c| c == "HIPSSH_NET_END"), "远端监控采样未完整返回");
    let z_has_processes = c_text.lines().any(|c| c.starts_with("HIPSSH_PROC_END\t"));
    let mut z_uptime = None;
    let mut z_cpu = None;
    // 逻辑 CPU 数 (/proc/stat 的 cpu0、cpu1… 行), 进程 CPU 换算为整机占比用.
    let mut z_cpu_count = 0u32;
    let mut z_mem_total = 0;
    let mut z_mem_available = 0;
    let mut z_swap_total = 0;
    let mut z_swap_free = 0;
    let mut z_cached = 0;
    let mut z_sreclaimable = 0;
    let mut z_shmem = 0;
    let mut z_interfaces = Vec::new();
    let mut z_raw_processes = Vec::new();
    let mut z_processes = Vec::new();
    let mut z_processes_truncated = false;
    let mut z_page_size = 0u64;
    let mut z_clock_ticks = 0u64;
    let mut z_in_net = false;
    for line in c_text.lines() {
        if let Some(z_raw) = line.strip_prefix("P\t") {
            if z_raw_processes.len() < 512 {
                z_raw_processes.push(z_raw);
            } else {
                z_processes_truncated = true;
            }
            continue;
        }
        if let Some(z_value) = line.strip_prefix("HIPSSH_PROC_END\t") {
            z_processes_truncated |= z_value == "1";
            continue;
        }
        if let Some(z_value) = line.strip_prefix("HIPSSH_UPTIME\t") {
            z_uptime = z_value.trim().parse::<f64>().ok();
            continue;
        }
        if let Some(z_value) = line.strip_prefix("HIPSSH_PAGE_SIZE\t") {
            z_page_size = z_value.parse().unwrap_or(0);
            continue;
        }
        if let Some(z_value) = line.strip_prefix("HIPSSH_CLK_TCK\t") {
            z_clock_ticks = z_value.parse().unwrap_or(0);
            continue;
        }
        if line == "HIPSSH_NET_BEGIN" {
            z_in_net = true;
            continue;
        }
        if line == "HIPSSH_NET_END" {
            z_in_net = false;
            continue;
        }
        if line == "HIPSSH_PROC_BEGIN" {
            continue;
        }
        if z_in_net {
            if let Some((z_name, z_values)) = line.split_once(':') {
                let z_name = z_name.trim();
                if z_name != "lo" && !z_name.is_empty() {
                    let z_values: Vec<u64> = z_values
                        .split_whitespace()
                        .filter_map(|v| v.parse().ok())
                        .collect();
                    if z_values.len() >= 9 {
                        if z_interfaces.len() < 33 {
                            z_interfaces.push(InterfaceSample {
                                name: z_name
                                    .chars()
                                    .filter(|c| !c.is_control())
                                    .take(64)
                                    .collect(),
                                rx_bytes: z_values[0],
                                tx_bytes: z_values[8],
                            });
                        }
                    }
                }
            }
            continue;
        }
        if line.strip_prefix("cpu").is_some_and(|c| c.starts_with(|l: char| l.is_ascii_digit())) {
            z_cpu_count += 1;
            continue;
        }
        if line.starts_with("cpu ") {
            let values: Vec<u64> = line
                .split_whitespace()
                .skip(1)
                .filter_map(|v| v.parse().ok())
                .collect();
            if values.len() >= 4 {
                // guest时间已经计入user/nice, 不重复累加末尾guest字段.
                let total = values
                    .iter()
                    .take(8)
                    .fold(0u64, |a, b| a.saturating_add(*b));
                let idle = values
                    .get(3)
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(values.get(4).copied().unwrap_or(0));
                z_cpu = Some((total, idle));
            }
            continue;
        }
        if let Some((key, value)) = line.split_once(':') {
            let key = key.trim();
            match key {
                "MemTotal" | "MemAvailable" | "SwapTotal" | "SwapFree" | "Cached"
                | "SReclaimable" | "Shmem" => {
                    let z_value = value
                        .split_whitespace()
                        .next()
                        .and_then(|v| v.parse::<u64>().ok())
                        .unwrap_or(0);
                    match key {
                        "MemTotal" => z_mem_total = z_value,
                        "MemAvailable" => z_mem_available = z_value,
                        "SwapTotal" => z_swap_total = z_value,
                        "SwapFree" => z_swap_free = z_value,
                        "Cached" => z_cached = z_value,
                        "SReclaimable" => z_sreclaimable = z_value,
                        "Shmem" => z_shmem = z_value,
                        _ => {}
                    }
                }
                _ => {}
            }
        }
    }
    let (cpu, idle) = z_cpu.context("远端 /proc/stat 不可用")?;
    ensure!(z_mem_total > 0, "远端 /proc/meminfo 不可用");
    let z_swap_total_bytes = z_swap_total.saturating_mul(1024);
    // 页大小与时钟频率只随进程列表输出 (每 3 次一次), 其余采样只含 CPU、内存与网络.
    ensure!(
        !z_has_processes || (z_page_size > 0 && z_clock_ticks > 0),
        "远端进程采样参数不可用"
    );
    for z_raw in z_raw_processes.iter().take(512) {
        if let Some(z_process) = parse_process_stat(z_raw, z_page_size) {
            z_processes.push(z_process);
        }
    }
    let z_interfaces_truncated = z_interfaces.len() > 32;
    z_interfaces.truncate(32);
    Ok(ProcSample {
        cpu_total: cpu,
        cpu_idle: idle,
        memory_total: z_mem_total.saturating_mul(1024),
        memory_used: z_mem_total
            .saturating_sub(z_mem_available)
            .saturating_mul(1024),
        swap_total: z_swap_total_bytes,
        swap_used: z_swap_total_bytes.saturating_sub(z_swap_free.saturating_mul(1024)),
        memory_cache: z_cached
            .saturating_add(z_sreclaimable)
            .saturating_sub(z_shmem)
            .saturating_mul(1024),
        interfaces: z_interfaces,
        interfaces_truncated: z_interfaces_truncated,
        processes: z_processes,
        processes_truncated: z_processes_truncated,
        clock_ticks: z_clock_ticks,
        cpu_count: z_cpu_count.max(1),
        has_processes: z_has_processes,
        uptime: z_uptime,
        sampled_at: Instant::now(),
    })
}

fn parse_process_stat(c_text: &str, c_page_size: u64) -> Option<RawProcess> {
    let (z_pid, z_rest) = c_text.split_once(" (")?;
    let z_pid = z_pid.parse::<u32>().ok()?;
    let z_close = z_rest.rfind(") ")?;
    let z_name: String = z_rest[..z_close]
        .chars()
        .filter(|c| !c.is_control())
        .take(128)
        .collect();
    let z_fields: Vec<&str> = z_rest[z_close + 2..].split_whitespace().collect();
    if z_fields.len() <= 21 {
        return None;
    }
    let z_ticks = z_fields
        .get(11)?
        .parse::<u64>()
        .ok()?
        .saturating_add(z_fields.get(12)?.parse::<u64>().ok()?);
    let z_start_time = z_fields.get(19)?.parse::<u64>().ok()?;
    let z_rss_pages = z_fields.get(21)?.parse::<i64>().ok()?.max(0) as u64;
    Some(RawProcess {
        pid: z_pid,
        name: z_name,
        ticks: z_ticks,
        start_time: z_start_time,
        rss_bytes: z_rss_pages.saturating_mul(c_page_size),
    })
}

fn build_process_metrics(
    c_sample: &ProcSample,
    c_old: &ProcSample,
    c_elapsed: f64,
) -> Vec<ProcessMetric> {
    let z_old: HashMap<u32, &RawProcess> = c_old.processes.iter().map(|p| (p.pid, p)).collect();
    let z_all: Vec<ProcessMetric> = c_sample
        .processes
        .iter()
        .map(|p| {
            let z_cpu = z_old
                .get(&p.pid)
                .filter(|old| old.start_time == p.start_time && p.ticks >= old.ticks)
                .map(|old| {
                    // 按整机占比 (所有逻辑 CPU 合计 100%), 与上方整机 CPU 使用率同一口径.
                    p.ticks.saturating_sub(old.ticks) as f64 * 100.0
                        / c_sample.clock_ticks as f64
                        / c_elapsed
                        / f64::from(c_sample.cpu_count)
                })
                .unwrap_or(0.0);
            ProcessMetric {
                pid: p.pid,
                name: p.name.clone(),
                cpu_percent: z_cpu,
                memory_bytes: p.rss_bytes,
            }
        })
        .collect();
    let mut z_cpu_order: Vec<usize> = (0..z_all.len()).collect();
    z_cpu_order.sort_by(|a, b| {
        z_all[*b]
            .cpu_percent
            .partial_cmp(&z_all[*a].cpu_percent)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let mut z_mem_order: Vec<usize> = (0..z_all.len()).collect();
    z_mem_order.sort_by_key(|i| std::cmp::Reverse(z_all[*i].memory_bytes));
    let mut z_selected = Vec::with_capacity(128);
    for i in z_cpu_order
        .iter()
        .take(64)
        .chain(z_mem_order.iter().take(64))
    {
        if !z_selected.contains(i) {
            z_selected.push(*i);
        }
    }
    for i in z_cpu_order.iter() {
        if z_selected.len() >= 128 {
            break;
        }
        if !z_selected.contains(i) {
            z_selected.push(*i);
        }
    }
    let mut z_result: Vec<ProcessMetric> =
        z_selected.into_iter().map(|i| z_all[i].clone()).collect();
    z_result.sort_by(|a, b| {
        b.cpu_percent
            .partial_cmp(&a.cpu_percent)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.memory_bytes.cmp(&a.memory_bytes))
    });
    z_result
}

fn build_interface_metrics(
    c_current: &[InterfaceSample],
    c_old: &HashMap<&str, &InterfaceSample>,
    c_elapsed: f64,
) -> (Vec<InterfaceMetric>, u64, u64) {
    let mut z_rx_total = 0u64;
    let mut z_tx_total = 0u64;
    let z_interfaces = c_current
        .iter()
        .take(32)
        .map(|current| {
            let (z_rx_rate, z_tx_rate) = c_old
                .get(current.name.as_str())
                .map(|old| {
                    let z_rx = if current.rx_bytes >= old.rx_bytes {
                        ((current.rx_bytes - old.rx_bytes) as f64 / c_elapsed) as u64
                    } else {
                        0
                    };
                    let z_tx = if current.tx_bytes >= old.tx_bytes {
                        ((current.tx_bytes - old.tx_bytes) as f64 / c_elapsed) as u64
                    } else {
                        0
                    };
                    (z_rx, z_tx)
                })
                .unwrap_or((0, 0));
            z_rx_total = z_rx_total.saturating_add(z_rx_rate);
            z_tx_total = z_tx_total.saturating_add(z_tx_rate);
            InterfaceMetric {
                name: current.name.clone(),
                rx_bytes: current.rx_bytes,
                tx_bytes: current.tx_bytes,
                rx_bytes_per_sec: z_rx_rate,
                tx_bytes_per_sec: z_tx_rate,
                history: InterfaceHistory::default(),
            }
        })
        .collect();
    (z_interfaces, z_rx_total, z_tx_total)
}

fn push_interface_history(c_history: &mut InterfaceHistory, c_rx: u64, c_tx: u64) {
    if c_history.rx_bytes_per_sec.len() >= MAX_MONITOR_HISTORY {
        c_history.rx_bytes_per_sec.pop_front();
        c_history.tx_bytes_per_sec.pop_front();
    }
    c_history.rx_bytes_per_sec.push_back(c_rx);
    c_history.tx_bytes_per_sec.push_back(c_tx);
}
