use crate::{
    config::ConfigStore,
    model::{Actor, AdminPassword, AuthMethod, Connection},
};
use anyhow::{Context, Result, bail, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::{Buf, Bytes};
use futures_util::{FutureExt, StreamExt, stream::FuturesUnordered};
use russh::{
    ChannelMsg, Disconnect, client,
    keys::{self, HashAlg, PrivateKeyWithHashAlg, PublicKeyOrCertificate},
};
use russh_sftp::client::RawSftpSession;
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    io::SeekFrom,
    path::{Path, PathBuf},
    // 锁中毒只在持锁线程 panic 后出现, 数据仍可用, 统一直接取回而不逐处转换为错误.
    sync::{Arc, Mutex, PoisonError, Weak, atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering}},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::{Notify, Semaphore, mpsc, oneshot},
};
use zeroize::Zeroizing;

const MAX_SESSIONS: usize = 8;
const MAX_TASKS: usize = 2500;
// 一个文件夹传输任务最多包含的条目数 (文件与子目录合计), 扫描时超出即报错, 不做部分传输.
const MAX_DIRECTORY_ITEMS: usize = 200_000;
// 文件夹任务详情中保留的跳过或失败条目数.
const MAX_DIRECTORY_NOTES: usize = 200;
const MAX_TABS: usize = 32;
const MAX_INPUT: usize = 16 * 1024;
// 独立命令输出的默认保留量 (MCP 默认按此返回, 避免一次塞满上下文); 界面可要求更多, 上限 MAX_COMMAND_OUTPUT.
const MAX_OUTPUT: usize = 65_536;
const MAX_COMMAND_OUTPUT: usize = 1024 * 1024;
// 同一会话所有上传的在途写请求总数 (每个 ~32 KiB, 确认后即释放): 专用连接 192 个 (~6 MiB);
// 退回主连接时另用 48 个 (~1.5 MiB) 的总额度, 多个上传合计也不拖慢终端回显.
const UPLOAD_SESSION_INFLIGHT: usize = 192;
const UPLOAD_MAIN_INFLIGHT: usize = 48;
// 单文件上传上限控制在 OpenSSH 默认 2 MiB 通道窗口以内 (48 * ~32 KiB ≈ 1.5 MiB), 避免顶满窗口触发 russh 全局停发.
const UPLOAD_WINDOW: usize = 48;
const UPLOAD_MAIN_WINDOW: usize = 16;
// 留足 SFTP 写头与长句柄余量, 常见的 32 KiB SSH 通道包无需拆分.
const UPLOAD_CHUNK: usize = 32 * 1024 - 256;
// 下载使用与上传相同的 RawSftpSession 精确滑窗: 单块留出 13 字节 SFTP DATA 头, 匹配 32 KiB SSH 包且避免短读清空整条流水线.
const DOWNLOAD_WINDOW: usize = 96;
const DOWNLOAD_MAIN_WINDOW: usize = 24;
const DOWNLOAD_CHUNK: usize = 32 * 1024 - 128;
// 上传下载自适应爬升的起始并发块数: 既能 1-2 个 RTT 拉满高速线路, 又能避免极慢线路起步即堆积大量在途请求.
const TRANSFER_INITIAL_WINDOW: usize = 8;
const TRANSFER_SPEED_SAMPLE: Duration = Duration::from_millis(250);
const TRANSFER_SPEED_WINDOW: Duration = Duration::from_secs(1);
const TRANSFER_STALL: Duration = Duration::from_secs(60);
const SSH_IO_TIMEOUT: Duration = Duration::from_secs(180);
// 传输遇到网络错误时连续自动重连续传的次数上限 (有新进度即重新计数).
const TRANSFER_RETRIES: u32 = 10;
// 传输用完的 SFTP 会话最多留这么久、这么多个 (与最大同时传输数一致); 过期的不再复用, 服务器也可能已回收.
const TRANSFER_IDLE: Duration = Duration::from_secs(60);
const TRANSFER_IDLE_MAX: usize = 16;
// 独立传输连接连续未收到数据的探活阈值 (毫秒): 高于 15 秒底层保活间隔, 避免选文件或短暂停顿后多一次 ping 往返.
const TRANSFER_IDLE_PROBE_MS: u64 = 30_000;
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
    // 目录请求和补全标识共用全局序号, 标签重连后旧标识也不会重复.
    list_seq: AtomicU64,
    // 传输与独立命令分开限流, 两个大文件传输不会挡住命令执行.
    transfer_slots: TransferSlots,
    command_slots: Arc<Semaphore>,
    // 终端有新输出时唤醒界面刷新; 只保留一个许可, 连续输出自然合并.
    pub(crate) terminal_changed: tokio::sync::Notify,
    // MCP 服务的监听任务与状态 (网址, 或未运行的原因); 异步锁让启动、换端口、关闭依次进行.
    mcp_http: tokio::sync::Mutex<(Option<tokio::task::JoinHandle<()>>, std::result::Result<String, String>)>,
}

struct TransferSlots {
    // 只有队首任务等空位, 后面的任务按 Tokio 锁的顺序排队, 取消时自动让出队首.
    head: tokio::sync::Mutex<()>,
    counts: Mutex<(usize, usize)>, // (并发上限, 已占用)
    changed: Notify,
}

struct TransferSlot<'a>(&'a TransferSlots);

impl TransferSlots {
    fn new(c_limit: usize) -> Self {
        Self { head: tokio::sync::Mutex::new(()), counts: Mutex::new((c_limit, 0)), changed: Notify::new() }
    }

    async fn acquire(&self) -> TransferSlot<'_> {
        let _z_head = self.head.lock().await;
        loop {
            {
                let mut l_counts = self.counts.lock().unwrap_or_else(PoisonError::into_inner);
                if l_counts.1 < l_counts.0 {
                    l_counts.1 += 1;
                    return TransferSlot(self);
                }
            }
            self.changed.notified().await;
        }
    }

    fn resize(&self, c_limit: usize) {
        self.counts.lock().unwrap_or_else(PoisonError::into_inner).0 = c_limit;
        self.changed.notify_one();
    }
}

impl Drop for TransferSlot<'_> {
    fn drop(&mut self) {
        self.0.counts.lock().unwrap_or_else(PoisonError::into_inner).1 -= 1;
        self.0.changed.notify_one();
    }
}

#[derive(Default)]
struct State {
    sessions: HashMap<String, Arc<LiveSession>>,
    tabs: Vec<ConnectionTab>,
    tasks: HashMap<String, Arc<Mutex<Task>>>,
    active_session_id: Option<String>,
    // 监控侧栏是否展开: 与 active_session_id/sessions 共用同一把 state 锁, 避免启停竞态泄漏后台监控任务.
    monitor_enabled: bool,
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
    // 目录读取和修改按连接串行, 一台慢服务器不影响其他连接.
    file_slot: Arc<tokio::sync::Mutex<()>>,
    // 目录浏览与文件操作复用的 SFTP 会话 (受 file_slot 串行保护), 省去每次开通道、启动和初始化的往返; 失效时重建.
    // 第二项为所在独立连接的对象地址 (主连接为 0), 该连接退役时摘除缓存 (见 retire_transfer); 第三项为通道是否仍存活.
    browse_sftp: Mutex<Option<(Arc<RawSftpSession>, usize, Arc<AtomicBool>)>>,
    file_details: Mutex<Option<FileDetailsTask>>,
    // 本终端历史按预算分配的字节数 (行数 x 列数 x 每格约 32 字节), 各终端合计用于新终端的额度计算.
    history_bytes: std::sync::atomic::AtomicUsize,
    // 传输专用 SSH 连接 (按需建立, 会话内复用), 以及上次建立失败的时间和原因; 失败后一段时间内直接用主连接.
    transfer: tokio::sync::Mutex<(Option<SshLink>, Option<(Instant, String)>)>,
    // 主连接认证成功后可重用的凭据, 只驻留内存.
    transfer_auth: Mutex<Option<TransferAuth>>,
    // 独立连接上成功传完的 SFTP 会话 (上传与下载统一复用 RawSftpSession), 下一个文件直接复用, 省去开通道、启动和初始化的往返.
    idle_sftp: Mutex<Vec<IdleSftp<(Arc<russh_sftp::client::RawSftpSession>, russh_sftp::protocol::Version)>>>,
    // 建立主连接时所用代理配置的快照 (含解密后的密码); 传输连接沿用, 代理修改或删除在新会话生效.
    proxy: Mutex<Option<Arc<ProxyRoute>>>,
    // 主连接已确认的主机指纹, 传输连接必须一致, 不再重复询问.
    host_fingerprint: Mutex<Option<String>>,
    main_link: Arc<SshLinkState>,
    // 同一会话的多个上传共用在途写请求名额 (每个约 32 KiB).
    upload_inflight: Arc<Semaphore>,
    upload_main_inflight: Arc<Semaphore>,
    connection: Connection,
    actor: Actor,
    status: Mutex<String>,
    handle: Mutex<Option<Arc<client::Handle<SshHandler>>>>,
    // 主连接套接字的副本, 断开时用于强制终止 (见 ssh_connect).
    socket: Mutex<Option<SshSocket>>,
    // 主连接最近一次 SSH 探活往返延迟 (毫秒), -1 表示尚未测量或已失效.
    latency_ms: AtomicI64,
    // 独立传输连接的套接字副本, 以及会话已关闭 (不再接受新的独立连接) 的标记; 两者在同一把锁内读写, 发布与关闭不会错过.
    transfer_socket: Mutex<(Option<SshSocket>, bool)>,
    // (当前独立连接的对象地址, 无则为 0; 它是否出过可重试错误): 可疑时复用前先探活, 出站卡住 (内置保活发现不了) 时退役重建.
    // 状态只属于当前连接: 已退役或被替换的旧连接上的迟到错误不会改动它 (见 mark_suspect), 放回空闲池也据此判断 (见 put_idle_sftp).
    transfer_suspect: Mutex<(usize, bool)>,
    terminal_tx: Mutex<Option<mpsc::Sender<TerminalInput>>>,
    // 终端通道代次: 每次打开终端换新值 (在 terminal_tx 锁内更新), 界面带上它写入, 旧终端的待发输入不会进入新终端.
    terminal_gen: AtomicU64,
    // 只保留最新尺寸: 发送端忙于大段输入时连续调整窗口, 中间尺寸可以丢弃, 最后一次不能丢.
    resize_tx: Mutex<Option<tokio::sync::watch::Sender<(u16, u16)>>>,
    terminal: Mutex<Terminal>,
    // 终端通道已打开或正在打开; 终端退出后 SSH 连接保留, 文件传输和独立命令照常使用.
    shell: AtomicBool,
    // 终端 PTY 与 Shell 已由服务端确认就绪: 仅在确认后才允许启动监控脚本, 避免登录握手或仅 SFTP 账号抢开监控通道.
    shell_ready: AtomicBool,
    shell_stop: Mutex<Option<oneshot::Sender<()>>>,
    // 监控任务控制: (代次, 停止信号). 代次递增保证快速收起再展开时旧任务收尾不会清除新任务句柄.
    monitor_ctrl: Mutex<(u64, Option<oneshot::Sender<()>>)>,
    // 远端已明确返回无 /proc (退出码 42), 再次展开侧栏时不重复拉起脚本.
    monitor_unsupported: AtomicBool,
    trust: Mutex<Option<TrustPrompt>>,
    // 待回答的认证请求: (随机请求标识, 提示, 回答通道). 三者放在一起: 状态快照拿到的提示与标识一定属于同一轮,
    // 界面带回标识在同一把锁内核对并取走, 迟到或重复的回答不会交给下一轮.
    auth_answers: Mutex<Option<(String, Vec<AuthPrompt>, oneshot::Sender<Vec<String>>)>>,
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

impl LiveSession {
    fn ssh_handle(&self) -> Result<Arc<client::Handle<SshHandler>>> {
        self.handle.lock().unwrap_or_else(PoisonError::into_inner).clone().context("SSH 尚未连接")
    }
}

// 代理配置与解密后的代理密码.
type ProxyRoute = (crate::config::Proxy, Option<Zeroizing<String>>);

#[derive(Clone)]
enum TransferAuth {
    Password(Zeroizing<String>),
    Key(Arc<keys::PrivateKey>),
}
// (所在连接, 放回时间, 通道是否仍存活, 会话).
type IdleSftp<T> = (SshLink, Instant, Arc<AtomicBool>, T);

#[derive(Clone)]
struct SshLink {
    handle: Arc<client::Handle<SshHandler>>,
    state: Arc<SshLinkState>,
    dedicated: bool,
}
impl SshLink {
    fn key(&self) -> usize {
        Arc::as_ptr(&self.handle) as usize
    }

    fn transfer_idle(&self) -> bool {
        mono_ms().saturating_sub(self.state.last_rx_mono()) >= TRANSFER_IDLE_PROBE_MS
    }

    async fn probe_transfer(&self) -> bool {
        if stale_opens(self.key(), Duration::from_secs(30)) != 0 || !ssh_ping(&self.handle, 10).await {
            return false;
        }
        self.state.mark_pong();
        true
    }
}

// 每条 SSH 连接的运行期统计与断连诊断状态. 旧传输连接退役后可能仍在收包或回调断开.
#[derive(Default)]
struct SshLinkState {
    // 读写热路径计数器: 全无锁更新, 供断连诊断和传输空闲判定使用.
    rx_bytes: AtomicU64,
    tx_bytes: AtomicU64,
    last_tcp_rx_mono: AtomicU64,
    last_tcp_tx_mono: AtomicU64,
    last_data_mono: AtomicU64,
    last_pong_mono: AtomicU64,
    // 休眠时钟采样的 1 秒无锁节流门控, 避免高频收包重复加锁.
    rx_clock_gate_mono: AtomicU64,
    kex_count: AtomicU64,
    // 本端主动断开、取消或退役时标记, 避免把正常关闭记入断连诊断与日志.
    user_closed: AtomicBool,
    diag: Mutex<LinkDiag>,
}

#[derive(Default, Clone)]
struct LinkDiag {
    // 认证通过后的上线时刻: 0 表示尚未完成认证, 握手/认证期断连不计入已建连断连日志.
    connected_mono: u64,
    connected_awake: u64,
    connected_wall: u64,
    // 最近一次收包/探活时刻的 (休眠停表毫秒, 墙钟毫秒), 成对更新避免数据撕裂.
    rx_clock_awake: u64,
    rx_clock_wall: u64,
    // watch_connection 观测到的系统休眠跨度 (与 awake/wall 差值互为兜底).
    sleep_secs: u64,
    sleep_at_mono: u64,
    via_proxy: bool,
    local_addr: Option<std::net::SocketAddr>,
    peer_addr: Option<std::net::SocketAddr>,
    remote_sshid: String,
    cipher: String,
    kex: String,
    strict_kex: bool,
    error: Option<String>,
    detail: Option<String>,
}

impl SshLinkState {
    fn mark_route(
        &self,
        c_via_proxy: bool,
        c_local: Option<std::net::SocketAddr>,
        c_peer: Option<std::net::SocketAddr>,
    ) {
        let mut z_diag = self.diag.lock().unwrap_or_else(PoisonError::into_inner);
        z_diag.via_proxy = c_via_proxy;
        z_diag.local_addr = c_local;
        z_diag.peer_addr = c_peer;
    }

    fn mark_authenticated(&self) {
        let mut z_diag = self.diag.lock().unwrap_or_else(PoisonError::into_inner);
        z_diag.connected_mono = mono_ms();
        z_diag.connected_awake = awake_ms();
        z_diag.connected_wall = epoch_ms();
    }

    fn touch_rx_clock(&self, c_mono: u64) {
        let z_prev = self.rx_clock_gate_mono.load(Ordering::Relaxed);
        if z_prev != 0 && c_mono.saturating_sub(z_prev) < 1000 {
            return;
        }
        self.rx_clock_gate_mono.store(c_mono, Ordering::Relaxed);
        let mut z_diag = self.diag.lock().unwrap_or_else(PoisonError::into_inner);
        z_diag.rx_clock_awake = awake_ms();
        z_diag.rx_clock_wall = epoch_ms();
    }

    fn mark_data(&self) {
        let z_mono = mono_ms();
        self.last_data_mono.store(z_mono, Ordering::Relaxed);
        self.touch_rx_clock(z_mono);
    }

    fn mark_pong(&self) {
        let z_mono = mono_ms();
        self.last_pong_mono.store(z_mono, Ordering::Relaxed);
        self.touch_rx_clock(z_mono);
    }

    fn mark_kex(&self, c_names: &russh::Names, c_sshid: &[u8]) {
        let z_mono = mono_ms();
        self.kex_count.fetch_add(1, Ordering::Relaxed);
        self.rx_clock_gate_mono.store(z_mono, Ordering::Relaxed);
        let mut z_diag = self.diag.lock().unwrap_or_else(PoisonError::into_inner);
        z_diag.rx_clock_awake = awake_ms();
        z_diag.rx_clock_wall = epoch_ms();
        if z_diag.remote_sshid.is_empty() && !c_sshid.is_empty() {
            z_diag.remote_sshid = safe_error(&String::from_utf8_lossy(c_sshid));
        }
        z_diag.cipher = c_names.cipher.as_ref().to_owned();
        z_diag.kex = c_names.kex.as_ref().to_owned();
        z_diag.strict_kex = c_names.strict_kex();
    }

    fn mark_tcp_rx(&self, c_bytes: usize) {
        self.rx_bytes.fetch_add(c_bytes as u64, Ordering::Relaxed);
        self.last_tcp_rx_mono.store(mono_ms(), Ordering::Relaxed);
    }

    fn mark_tcp_tx(&self, c_bytes: usize) {
        self.tx_bytes.fetch_add(c_bytes as u64, Ordering::Relaxed);
        self.last_tcp_tx_mono.store(mono_ms(), Ordering::Relaxed);
    }

    fn mark_user_closed(&self) {
        self.user_closed.store(true, Ordering::Release);
    }

    fn record_sleep(&self, c_secs: u64) {
        let mut z_diag = self.diag.lock().unwrap_or_else(PoisonError::into_inner);
        z_diag.sleep_secs = c_secs;
        z_diag.sleep_at_mono = mono_ms();
    }

    fn last_rx_mono(&self) -> u64 {
        self.last_tcp_rx_mono
            .load(Ordering::Relaxed)
            .max(self.last_data_mono.load(Ordering::Relaxed))
            .max(self.last_pong_mono.load(Ordering::Relaxed))
    }

    fn has_error(&self) -> bool {
        self.diag.lock().unwrap_or_else(PoisonError::into_inner).error.is_some()
    }

    fn error(&self) -> Option<String> {
        self.diag.lock().unwrap_or_else(PoisonError::into_inner).error.clone()
    }

    fn detail(&self) -> Option<String> {
        self.diag.lock().unwrap_or_else(PoisonError::into_inner).detail.clone()
    }

    fn set_error_once(&self, c_error: String, c_detail: Option<String>) -> bool {
        let mut z_diag = self.diag.lock().unwrap_or_else(PoisonError::into_inner);
        if z_diag.error.is_some() {
            return false;
        }
        z_diag.error = Some(c_error);
        z_diag.detail = c_detail;
        true
    }
}

// 补全后的目录条目, 以及服务器时区与时间提示.
type FileDetails = (Vec<Value>, (Option<String>, Option<String>));

#[derive(Default)]
struct BrowseOperation {
    channel: Option<Weak<RawSftpSession>>,
    write_started: bool,
}

struct FileDetailsTask {
    seq: u64,
    // 结果已被界面取走等待时, 新请求仍可取消旧任务.
    abort: tokio::task::AbortHandle,
    pending: Option<(String, tokio::task::JoinHandle<FileDetails>)>,
}

struct ServerFileTimes {
    timezone: Option<String>,
    formatted: HashMap<u64, String>,
    hint: Option<String>,
    expires_at: Instant,
}

struct SshHandler {
    session: Weak<LiveSession>,
    store: Arc<ConfigStore>,
    link: Arc<SshLinkState>,
    // 传输专用连接: 只接受主连接已确认的主机指纹.
    transfer: bool,
}
struct TerminalInput {
    data: Vec<u8>,
    actor: Actor,
    lease_id: Option<String>,
    // 代填的密码: 写入任务发送前再确认光标行仍是密码提示.
    password: bool,
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
    // Kitty 普通屏和备用屏各有自己的模式栈, 只支持第 1 位 (消除歧义).
    kitty_keys: [Vec<u8>; 2],
    // 0 方块, 1 下划线, 2 竖线; 来自 DECSCUSR.
    cursor_shape: u8,
    // 同步输出 (DEC 2026) 开始时间: 期间不推进 seq, 界面只显示完整的一帧.
    sync_since: Option<Instant>,
    osc: Vec<u8>,
    clipboard: Option<String>,
    // 解析器出错后已重建 (见 recover), 读取循环据此请求远端重绘一次.
    recovered: bool,
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
    // vt100 0.16 在宽字符 (如中文) 贴近行尾、尺寸变化与插删、滚动区域等序列组合时会数组越界 panic (0.18 已修复).
    // panic 若发生在读取循环持画面锁期间: 读取任务随之结束、画面锁中毒, 终端从此不再更新, 输入通道也关闭.
    // 因此解析和改尺寸仍隔离 panic 作为防御, 出错时按当前画面重建解析器 (见 recover), 终端继续工作.
    fn process(&mut self, c_data: &[u8]) -> Vec<u8> {
        let (z_rows, z_cols) = screen_size(self.parser.screen());
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.process_raw(c_data))).unwrap_or_else(|_| {
            self.recover(z_rows, z_cols);
            Vec::new()
        })
    }

    fn resize(&mut self, c_rows: u16, c_cols: u16) {
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.parser.screen_mut().set_size(non_zero(c_rows), non_zero(c_cols)))).is_err() {
            self.recover(c_rows, c_cols);
        }
        self.seq = self.seq.wrapping_add(1);
    }

    // 解析器内部状态已不可信: 按可见画面 (含备用屏、光标位置) 和输入模式重建同尺寸的新解析器, 这一块剩余数据丢弃.
    // 重建带不过去的有: 普通屏的历史行、处于备用屏时底下的普通屏画面、滚动区域与原点模式; 读取旧画面本身也可能出错, 此时换成空白终端.
    // 因此标记 recovered, 由读取循环请求远端整屏重绘 (见 run_shell), tmux、vim 等全屏程序会按新状态重画, 不在错位的画面上继续增量刷新.
    fn recover(&mut self, c_rows: u16, c_cols: u16) {
        let z_saved = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let l_screen = self.parser.screen();
            (l_screen.alternate_screen(), l_screen.contents_formatted(), l_screen.input_mode_formatted())
        }));
        self.parser = vt100::Parser::new(non_zero(c_rows), non_zero(c_cols), self.scrollback);
        if let Ok((l_alternate, l_contents, l_modes)) = z_saved {
            let l_restored = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if l_alternate {
                    self.parser.process(b"\x1b[?1049h");
                }
                self.parser.process(l_contents.as_bytes());
                self.parser.process(l_modes.as_bytes());
            }));
            if l_restored.is_err() {
                self.parser = vt100::Parser::new(non_zero(c_rows), non_zero(c_cols), self.scrollback);
            }
        }
        self.filter = TerminalFilter::default();
        self.decoded.clear();
        self.csi.clear();
        self.osc.clear();
        self.sync_since = None;
        self.recovered = true;
        self.seq = self.seq.wrapping_add(1);
    }

    // OSC 52 只允许远端写入本机剪贴板 (vim、tmux 的复制), 不会把本机剪贴板内容返回给远端.
    fn process_raw(&mut self, c_data: &[u8]) -> Vec<u8> {
        // 普通文本直接交给解析器, 不经过转义序列缓冲.
        if matches!(self.filter, TerminalFilter::Ground) && !c_data.contains(&0x1b) {
            for l_chunk in c_data.chunks(MAX_TERMINAL_DECODED_CHUNK) {
                self.parser.process(l_chunk);
                self.bump();
            }
            return Vec::new();
        }
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
                            b"?u" | [b'>' | b'<' | b'=', .., b'u'] => {
                                // 同一包里可能先切换备用屏再设置键盘模式, 先把切屏交给解析器.
                                self.parser.process(&z_decoded);
                                z_decoded.clear();
                                self.bump();
                                self.kitty_key_control()
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
                                // 同步输出期间不提前发布半帧.
                                self.bump();
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

    fn kitty_key_control(&mut self) -> Option<String> {
        let z_screen = usize::from(self.parser.screen().alternate_screen());
        let z_keys = &mut self.kitty_keys[z_screen];
        match self.csi.as_slice() {
            b"?u" => return Some(format!("\u{1b}[?{}u", z_keys.last().copied().unwrap_or(0))),
            [b'>', z_flags @ .., b'u'] => {
                if z_keys.len() >= 16 { z_keys.remove(0); }
                z_keys.push(csi_number(z_flags, 0) as u8 & 1);
            }
            [b'<', z_count @ .., b'u'] => {
                let l_count = csi_number(z_count, 1).max(1) as usize;
                z_keys.truncate(z_keys.len().saturating_sub(l_count));
            }
            [b'=', z_params @ .., b'u'] => {
                let mut l_params = z_params.split(|c| *c == b';');
                let l_flags = csi_number(l_params.next().unwrap_or_default(), 0) as u8 & 1;
                let l_current = z_keys.last().copied().unwrap_or(0);
                let l_next = match csi_number(l_params.next().unwrap_or_default(), 1) {
                    2 => l_current | l_flags,
                    3 => l_current & !l_flags,
                    _ => l_flags,
                };
                match z_keys.last_mut() { Some(c) => *c = l_next, None => z_keys.push(l_next) }
            }
            _ => {}
        }
        None
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
        let z_screen = usize::from(self.parser.screen().alternate_screen());
        self.modified_keys | if self.kitty_keys[z_screen].last().is_some_and(|c| *c & 1 != 0) { 4 } else { 0 }
    }

    // 画面未变或只取事件 (翻看历史、拖选) 时的结果: 仍带上按键编码所需的输入模式, 回到底部后的第一次输入按最新模式编码.
    fn unchanged(&self, c_clipboard: Option<String>) -> Value {
        // 也带上终端尺寸: 界面拖选或翻看历史时只走这条轻量结果, 据此仍能发现尺寸与界面不符并重新同步.
        let z_screen = self.parser.screen();
        json!({"unchanged":true,"seq":self.seq,"rows":z_screen.size().0,"cols":z_screen.size().1,"clipboard":c_clipboard,"bracketed_paste":z_screen.bracketed_paste(),"application_cursor":z_screen.application_cursor(),"modified_keys":self.key_mode(),"mouse":mouse_mode(z_screen),"mouse_tracking":mouse_tracking(z_screen)})
    }

    // 换成同尺寸的空白终端 (清空历史与模式), 用于终端关闭和重新打开.
    fn reset_screen(&mut self) {
        let (z_rows, z_cols) = self.parser.screen().size();
        self.parser = vt100::Parser::new(z_rows, z_cols, self.scrollback);
        self.filter = TerminalFilter::default();
        // 旧终端尚未取走的复制请求一并作废, 不覆盖用户之后的剪贴板.
        self.clipboard = None;
        self.reset_modes();
        self.seq = self.seq.wrapping_add(1);
    }

    // RIS (ESC c) 复位终端时一并复位本层记录的键盘、光标和同步状态.
    fn reset_modes(&mut self) {
        self.modified_keys = 0;
        self.kitty_keys.iter_mut().for_each(Vec::clear);
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
    last_progress_at: Instant,
    speed_samples: VecDeque<(Instant, u64)>,
    // 文件夹任务: bytes 与 total 为整个文件夹, 下面几项为文件计数和当前文件; base_bytes 是已结束文件的字节数.
    directory: bool,
    files_done: u64,
    files_failed: u64,
    files_total: u64,
    current: String,
    file_bytes: u64,
    file_total: u64,
    base_bytes: u64,
    // 文件夹只保留数据传输耗时最长的一份诊断. 重试原因保留到任务结束.
    transfer_diagnostics: Option<(Duration, String)>,
    transfer_retries: u32,
    last_transfer_error: Option<String>,
    // 传输任务当前是否回退经主连接传输 (界面据此提示).
    via_main_link: bool,
    stdout: String,
    stderr: String,
    truncated: bool,
    exit_code: Option<i32>,
    error: Option<String>,
    cancel: Option<oneshot::Sender<()>>,
}

impl Backend {
    pub fn new(c_store: Arc<ConfigStore>) -> Arc<Self> {
        let z_transfers = c_store.settings().map_or(3, |c| c.transfer_concurrency as usize);
        Arc::new(Self {
            commands: crate::commands::CommandStore::new(c_store.directory().join("commands.json")),
            store: c_store,
            state: Mutex::new(State::default()),
            list_seq: AtomicU64::new(0),
            transfer_slots: TransferSlots::new(z_transfers),
            command_slots: Arc::new(Semaphore::new(4)),
            terminal_changed: tokio::sync::Notify::new(),
            mcp_http: tokio::sync::Mutex::new((None, Err("MCP 服务未启动".into()))),
        })
    }

    // 界面同步监控侧栏展开状态: 仅在展开时对当前活跃且带终端的会话启动监控脚本.
    pub fn set_monitor_enabled(&self, c_enabled: bool) {
        {
            let mut l_state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            l_state.monitor_enabled = c_enabled;
        }
        self.sync_monitors();
    }

    // 按监控开关与当前活跃标签启停各会话的远端监控通道:
    // 在持 state 锁内统一决定并更新各会话的 monitor_ctrl 代次与停止句柄, 锁外发送停止信号和派生任务, 避免 TOCTOU 竞态泄漏.
    fn sync_monitors(&self) {
        let mut z_to_start: Vec<(Arc<LiveSession>, u64, oneshot::Receiver<()>)> = Vec::new();
        let mut z_to_stop: Vec<(Arc<LiveSession>, Option<oneshot::Sender<()>>)> = Vec::new();
        {
            let l_state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let l_enabled = l_state.monitor_enabled;
            let l_active = l_state.active_session_id.as_deref();
            for l_session in l_state.sessions.values() {
                let l_want = l_enabled
                    && l_active == Some(l_session.id.as_str())
                    && l_session.shell_ready.load(Ordering::Acquire)
                    && l_session.status.lock().is_ok_and(|c| c.as_str() == "connected");
                if l_want {
                    if let Some((l_gen, l_rx)) = reserve_monitor(l_session) {
                        z_to_start.push((l_session.clone(), l_gen, l_rx));
                    }
                } else {
                    let l_tx = disarm_monitor(l_session);
                    if l_tx.is_some() || !l_session.monitor_unsupported.load(Ordering::Acquire) {
                        z_to_stop.push((l_session.clone(), l_tx));
                    }
                }
            }
        }
        for (l_session, l_tx) in z_to_stop {
            if let Some(l_tx) = l_tx {
                let _ = l_tx.send(());
            }
            clear_monitor_if_idle(&l_session);
        }
        for (l_session, l_gen, l_rx) in z_to_start {
            spawn_monitor(l_session, l_gen, l_rx);
        }
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
            "set_language" => {
                require_user(c_actor)?;
                let z_lang = required_string(&c_args, "language")?;
                self.store.set_language(z_lang)?;
                Ok(json!({"language":z_lang}))
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
            "set_transfer_limits" => {
                require_user(c_actor)?;
                let z_value = |c_key: &str| c_args.get(c_key).and_then(Value::as_u64).and_then(|c| u32::try_from(c).ok()).context("传输设置需要整数");
                let (z_concurrency, z_batch) = (z_value("concurrency")?, z_value("batch")?);
                self.store.set_transfer_limits(z_concurrency, z_batch)?;
                self.transfer_slots.resize(z_concurrency as usize);
                Ok(json!({"concurrency":z_concurrency,"batch":z_batch}))
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
                let z_direct = self.store.delete_proxy(required_string(&c_args, "proxy_id")?);
                self.cancel_revoked_mcp_tasks();
                Ok(json!({"direct":z_direct?}))
            }
            // 为一台或一组服务器选择代理, proxy_id 为空表示直连.
            "set_connection_proxy" => {
                require_user(c_actor)?;
                let z_ids: Vec<String> = match c_args.get("group").and_then(Value::as_str) {
                    Some(l_group) => self.store.list().into_iter().filter(|c| c.group == l_group).map(|c| c.id).collect(),
                    None => serde_json::from_value(c_args.get("connection_ids").cloned().context("缺少 connection_ids")?)?,
                };
                let z_proxy = c_args.get("proxy_id").and_then(Value::as_str).filter(|c| !c.is_empty());
                // 逐条保存可能部分成功后报错: 无论结果都按实际配置复核撤权.
                let z_changed = self.store.set_connection_proxy(&z_ids, z_proxy);
                self.cancel_revoked_mcp_tasks();
                Ok(json!({"changed":z_changed?,"count":z_ids.len()}))
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
            "list_local_files" => self.list_local_files(c_args, c_actor).await,
            "import_finalshell" => self.import_finalshell(c_args, c_actor),
            "export_finalshell" => self.export_finalshell(c_args, c_actor),
            "remove_connection" => self.remove_connection(c_args, c_actor),
            "new_tab" => self.new_tab(c_actor),
            "connect" => self.connect(c_args, c_actor),
            "reconnect" => self.reconnect(c_args, c_actor).await,
            "open_terminal" => self.open_terminal(c_args, c_actor),
            "close_terminal" => {
                let z_session = self.session_args(&c_args, c_actor)?;
                // 持输入通道锁完成检查与摘除, 与 open_terminal 的发布互斥, 不会漏掉刚打开终端的关闭信号.
                let mut z_tx = z_session.terminal_tx.lock().unwrap_or_else(PoisonError::into_inner);
                ensure!(z_session.shell.load(Ordering::Acquire), "终端未打开");
                // 释放输入通道后写入任务关闭终端通道; 读取立即结束, 按"连接仍在"处理.
                z_tx.take();
                z_session.resize_tx.lock().unwrap_or_else(PoisonError::into_inner).take();
                if let Some(z_stop) = z_session.shell_stop.lock().unwrap_or_else(PoisonError::into_inner).take() {
                    let _ = z_stop.send(());
                }
                z_session.shell_ready.store(false, Ordering::Release);
                stop_monitor(&z_session);
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
            "list_files" | "check_remote_names" | "create_directory" | "create_file" | "set_permissions" | "rename_file" | "delete_files" =>
                self.file_call(c_method, &c_args, c_actor).await,
            "file_details" => self.file_details(c_args, c_actor).await,
            "delete_files_fast" => self.delete_files_fast(c_args, c_actor).await,
            "copy_file" => self.copy_file(c_args, c_actor).await,
            "upload_file" | "download_file" | "upload_directory" | "download_directory" => self.transfer(c_method, c_args, c_actor).await,
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
        self.cancel_revoked_mcp_tasks();
        Ok(json!({"connection":z_connection}))
    }

    // 撤销授权或改了登录身份 (含代理) 后, 取消不再被允许的 MCP 任务. 编辑连接、删除代理、批量设置代理都会改变身份.
    // 先在 state 锁内筛出仍在运行的 MCP 任务, 锁外读取磁盘配置核对权限, 避免持全局锁扫描全部历史任务.
    fn cancel_revoked_mcp_tasks(&self) {
        let z_active: Vec<(Arc<Mutex<Task>>, Connection)> = {
            let l_state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            l_state.tasks.values().filter_map(|c_task| {
                let l_task = c_task.lock().ok()?;
                (l_task.source == "mcp" && l_task.cancel.is_some()).then(|| (c_task.clone(), l_task.connection.clone()))
            }).collect()
        };
        for (l_task, l_connection) in z_active {
            if !self.store.get(&l_connection.id).is_ok_and(|c| mcp_permits(&c, &l_connection))
                && let Ok(mut l_task) = l_task.lock()
                && let Some(l_cancel) = l_task.cancel.take()
            {
                let _ = l_cancel.send(());
            }
        }
    }

    // 关闭全局 MCP 服务时同步取消仍在排队或运行的 MCP 后台任务.
    fn cancel_all_mcp_tasks(&self) {
        let z_state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        for l_task in z_state.tasks.values() {
            if let Ok(mut l_task) = l_task.lock()
                && l_task.source == "mcp"
                && let Some(l_cancel) = l_task.cancel.take()
            {
                let _ = l_cancel.send(());
            }
        }
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
        let z_session = self.session_args(&c_args, c_actor)?;
        // 读取粘贴模式时记下终端代次随输入核对: 按这个终端的模式编码的内容, 不写入期间重开的新终端.
        let z_terminal = z_session.terminal_gen.load(Ordering::Acquire);
        let z_bracketed = z_session.terminal.lock().unwrap_or_else(PoisonError::into_inner)
            .parser.screen().bracketed_paste();
        // 多行内容作为一次粘贴交给远端 Shell, 最后再按回车.
        let z_data = if z_bracketed {
            format!("\u{1b}[200~{z_command}\u{1b}[201~\r")
        } else {
            format!("{z_command}\r")
        };
        self.write_terminal(
            json!({"session_id":z_session_id,"_link":z_session.link,"data":z_data,"terminal":z_terminal}),
            c_actor,
        ).await
    }

    async fn list_local_files(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        let z_raw = required_string(&c_args, "path")?;
        ensure!(!z_raw.contains('\0'), "本地路径无效");
        let z_path = PathBuf::from(z_raw);
        ensure!(z_path.is_absolute(), "本机路径必须是绝对路径");
        if c_actor == Actor::Mcp {
            ensure!(!z_path.components().any(|c| matches!(c, std::path::Component::ParentDir)), "本机路径不能包含上级目录跳转 (..)");
        }
        let z_config_dir = self.store.directory().to_path_buf();
        // 本机目录不占用远端文件操作名额.
        tokio::task::spawn_blocking(move || -> Result<Value> {
            let z_path = z_path.canonicalize().context("本地目录不存在")?;
            ensure!(z_path.is_dir(), "请选择本地目录");
            if c_actor == Actor::Mcp {
                ensure!(!path_in_dir(&z_path, &z_config_dir), "禁止 MCP 访问 HiPShell 配置目录");
            }
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
        drop(z_state);
        self.sync_monitors();
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
        let (z_cancel_tx, z_cancel_rx) = oneshot::channel();
        let z_session = Arc::new(LiveSession {
            id: z_id.clone(),
            link: epoch_ms(),
            file_slot: Arc::new(tokio::sync::Mutex::new(())),
            browse_sftp: Mutex::new(None),
            file_details: Mutex::new(None),
            history_bytes: std::sync::atomic::AtomicUsize::new(z_scrollback * z_width * 32),
            transfer: tokio::sync::Mutex::new((None, None)),
            transfer_auth: Mutex::new(None),
            idle_sftp: Mutex::new(Vec::new()),
            proxy: Mutex::new(None),
            host_fingerprint: Mutex::new(None),
            main_link: Arc::new(SshLinkState::default()),
            upload_inflight: Arc::new(Semaphore::new(UPLOAD_SESSION_INFLIGHT)),
            upload_main_inflight: Arc::new(Semaphore::new(UPLOAD_MAIN_INFLIGHT)),
            connection: z_conn,
            actor: c_actor,
            status: Mutex::new("connecting".into()),
            handle: Mutex::new(None),
            socket: Mutex::new(None),
            latency_ms: AtomicI64::new(-1),
            transfer_socket: Mutex::new((None, false)),
            transfer_suspect: Mutex::new((0, false)),
            terminal_tx: Mutex::new(Some(z_tx)),
            terminal_gen: AtomicU64::new(epoch_ms()),
            resize_tx: Mutex::new(Some(z_resize_tx)),
            terminal: Mutex::new(Terminal {
                parser: vt100::Parser::new(non_zero(z_rows), non_zero(z_cols), z_scrollback),
                scrollback: z_scrollback,
                // 同一标签重连会沿用会话 ID, 序号从时间戳开始, 界面不会把新终端误判为未变化.
                seq: epoch_ms(),
                filter: TerminalFilter::default(),
                decoded: Vec::with_capacity(MAX_TERMINAL_DECODED_CHUNK),
                csi: Vec::with_capacity(64),
                modified_keys: 0,
                kitty_keys: Default::default(),
                cursor_shape: 0,
                sync_since: None,
                osc: Vec::new(),
                clipboard: None,
                recovered: false,
            }),
            trust: Mutex::new(None),
            auth_answers: Mutex::new(None),
            shell: AtomicBool::new(false),
            shell_ready: AtomicBool::new(false),
            shell_stop: Mutex::new(None),
            monitor_ctrl: Mutex::new((0, None)),
            monitor_unsupported: AtomicBool::new(false),
            // 取消句柄在会话公开前装好: 刚登记就被断开时也能停下连接任务.
            connect_cancel: Mutex::new(Some(z_cancel_tx)),
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
        self.sync_monitors();
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
                _ = z_cancel_rx => {
                    set_status(&z_cancel_session, "disconnected".into());
                    z_cancel_session.main_link.mark_user_closed();
                    // 连接任务此时已被丢弃: 它可能在断开摘走套接字之后才存入副本并启动握手, 由这里收尾关闭.
                    kill_socket(&z_cancel_session);
                }
                _ = z_backend.connect_task(z_connect_session, z_rx, z_resize_rx, z_password, z_files_only) => {},
            }
        });
        Ok(json!({"session_id": z_id,"empty":false}))
    }

    async fn reconnect(self: &Arc<Self>, c_args: Value, c_actor: Actor) -> Result<Value> {
        let z_id = required_string(&c_args, "session_id")?.to_owned();
        let z_connection_id = {
            let z_state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let z_tab = z_state.tabs.iter().find(|c| c.id == z_id).context("标签已关闭")?;
            z_tab.connection_id.clone().context("请先选择服务器")?
        };
        let z_connection = self.store.get(&z_connection_id)?;
        authorize(&z_connection, c_actor)?;
        // 在原标签内重连: 先断开旧会话但保留标签, 再用同一标签 ID 建立新会话.
        // 新会话在认证完成前拒绝终端输入, 旧会话残留的排队输入不会进入新连接.
        // 进行中的传输不取消: 旧连接关闭后转入自动重连, 在新会话上从断点继续.
        // MCP 只能重连已断开或失败的标签 (与工具说明一致), 不能借重连打断用户在线的终端; 是否离线在断开时对同一会话对象判断.
        let z_only_offline = c_actor == Actor::Mcp || c_args.get("only_disconnected").and_then(Value::as_bool) == Some(true);
        if !self.disconnect_session(&z_id, c_actor, false, true, z_only_offline).await? {
            return Ok(json!({"session_id":z_id,"skipped":true}));
        }
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
            link: c_session.main_link.clone(),
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
        // 期限覆盖 TCP 与代理 (最多 50 秒)、指纹确认弹窗 (120 秒) 和握手往返, 不在用户核对指纹时提前结束.
        let z_connected = tokio::time::timeout(
            Duration::from_secs(210),
            ssh_connect(&c_session.connection, z_proxy.as_deref(), z_handler, &c_session.socket),
        )
        .await;
        // 握手由 russh 在后台任务里进行, 放弃等待后它可能仍在等指纹确认或卡在密钥交换: 撤下弹窗并关闭套接字, 让握手随之结束.
        if !matches!(z_connected, Ok(Ok(_))) {
            c_session.trust.lock().unwrap_or_else(PoisonError::into_inner).take();
            c_session.main_link.mark_user_closed();
            kill_socket(&c_session);
        }
        let mut z_handle = match z_connected {
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
        // 认证不设总期限: 每轮输入弹窗各自限时 120 秒, 多轮提示不互相挤占; 网络卡住由连接的无活动超时结束, 认证调用随之报错.
        let z_user = &c_session.connection.username;
        let z_saved_secret = || match c_password {
            Some(l_secret) => Ok(Some(l_secret)),
            None => self.store.secret(&c_session.connection.id),
        };
        let z_auth: Result<bool> = async {
            match &c_session.connection.auth {
                AuthMethod::Password => {
                    if let Some(l_secret) = z_saved_secret()?
                        && authenticate_password(&mut z_handle, z_user, l_secret.as_str(), 60).await?
                    {
                        *c_session.transfer_auth.lock().unwrap_or_else(PoisonError::into_inner) = Some(TransferAuth::Password(l_secret));
                        return Ok(true);
                    }
                    // 与 OpenSSH 一致, 输错可重试, 共 3 次.
                    for i in 0..3 {
                        let l_prompt = format!(
                            "{}{}@{} 的 SSH 密码",
                            if i > 0 { "密码错误, 请重新输入 " } else { "" },
                            z_user,
                            c_session.connection.host
                        );
                        let l_secret = self.ask_secret(&c_session, l_prompt).await?;
                        if authenticate_password(&mut z_handle, z_user, l_secret.as_str(), 60).await? {
                            self.remember_secret(&c_session, l_secret.as_str());
                            *c_session.transfer_auth.lock().unwrap_or_else(PoisonError::into_inner) = Some(TransferAuth::Password(l_secret));
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
                    let z_saved = z_saved_secret()?;
                    let key = match keys::load_secret_key(path, z_saved.as_deref().map(String::as_str)) {
                        Ok(l_key) => l_key,
                        // 私钥加密且没有可用口令时弹窗询问, 成功后按"记住密码"设置保存.
                        Err(_) => {
                            let mut l_key = None;
                            for i in 0..3 {
                                let l_prompt = format!("{}私钥 {path} 的口令", if i > 0 { "口令错误, 请重新输入 " } else { "" });
                                let l_passphrase = self.ask_secret(&c_session, l_prompt).await?;
                                if let Ok(l_loaded) = keys::load_secret_key(path, Some(l_passphrase.as_str())) {
                                    self.remember_secret(&c_session, l_passphrase.as_str());
                                    l_key = Some(l_loaded);
                                    break;
                                }
                            }
                            l_key.context("私钥口令不正确或不是受支持的私钥格式")?
                        }
                    };
                    let key = Arc::new(key);
                    let z_success = authenticate_key(&mut z_handle, z_user, key.clone(), 15, 60).await?;
                    if z_success {
                        *c_session.transfer_auth.lock().unwrap_or_else(PoisonError::into_inner) = Some(TransferAuth::Key(key));
                    }
                    Ok(z_success)
                }
                AuthMethod::KeyboardInteractive => {
                    self.keyboard_auth(&c_session, &mut z_handle).await
                }
            }
        }
        .await;
        // 认证出错时可能还有等待中的输入弹窗, 一并撤下.
        if let Ok(mut z_answers) = c_session.auth_answers.lock() { z_answers.take(); }
        match z_auth {
            Ok(true) => {}
            Ok(false) => {
                set_status(&c_session, "authentication_failed".into());
                c_session.main_link.mark_user_closed();
                kill_socket(&c_session);
                return;
            }
            Err(err) => {
                set_status(
                    &c_session,
                    format!("error: {}", safe_error(&err.to_string())),
                );
                c_session.main_link.mark_user_closed();
                kill_socket(&c_session);
                return;
            }
        }
        if c_session.actor == Actor::Mcp
            && !self.mcp_permitted(&c_session.connection)
        {
            set_status(&c_session, "error: 此连接已撤销 MCP 授权".into());
            c_session.main_link.mark_user_closed();
            kill_socket(&c_session);
            return;
        }
        c_session.main_link.mark_authenticated();
        let z_handle = Arc::new(z_handle);
        *c_session.handle.lock().unwrap_or_else(PoisonError::into_inner) = Some(z_handle.clone());
        watch_connection(&self, &c_session, &z_handle);
        // 仅文件传输: 不开终端和监控, 节省远端进程与带宽. 先清理再标记已连接: 标记后界面即可打开终端, 不能再被这里的清理摘掉.
        if c_files_only {
            shell_closed(&c_session, "");
            set_status(&c_session, "connected".into());
            return;
        }
        let z_stop = {
            let _l_tx = c_session.terminal_tx.lock().unwrap_or_else(PoisonError::into_inner);
            c_session.shell.store(true, Ordering::Release);
            install_shell_stop(&c_session)
        };
        self.run_shell(&c_session, &z_handle, c_input, c_resize, z_stop).await;
    }

    // 在已认证的连接上打开终端通道并转发输入输出, 结束时只在 SSH 连接也断开时关闭会话.
    // c_stop: 关闭信号, 由调用方在标记终端已打开的同时装好, 启动确认期间用户关闭终端也能立即结束.
    async fn run_shell(
        &self,
        c_session: &Arc<LiveSession>,
        c_handle: &Arc<client::Handle<SshHandler>>,
        mut c_input: mpsc::Receiver<TerminalInput>,
        mut c_resize: tokio::sync::watch::Receiver<(u16, u16)>,
        mut z_stop_rx: oneshot::Receiver<()>,
    ) {
        let (rows, cols) = terminal_size(c_session);
        // 启动请求在确认前到达的输出先暂存 (最多 1 MiB, 超出丢弃), 确认后再交给终端.
        let mut z_early: Vec<Vec<u8>> = Vec::new();
        let mut z_early_bytes = 0usize;
        let z_opened = async {
            // 请求失败或超时时由守卫关闭通道, 服务器保持通道也不会占住会话名额.
            let mut z_channel = ClosingChannel(Some(open_channel(c_handle, 20).await.context("打开终端通道超时")?));
            // PTY 与 Shell 请求都要求答复并逐个确认: 只确认 Shell 时, 服务器拒绝分配 PTY 也会显示终端已就绪, 之后调整尺寸无效.
            // 发出请求只代表已入队; 服务器拒绝 (如只允许 SFTP 的账号) 会回 Failure 但可能保持通道.
            tokio::time::timeout(Duration::from_secs(20), async {
                z_channel.request_pty(true, "xterm-256color", cols as u32, rows as u32, 0, 0, &[]).await?;
                ensure!(channel_reply(&mut z_channel, &mut z_early, &mut z_early_bytes).await?, "服务器拒绝分配终端 (PTY)");
                z_channel.request_shell(true).await?;
                ensure!(channel_reply(&mut z_channel, &mut z_early, &mut z_early_bytes).await?, "服务器拒绝启动终端");
                anyhow::Ok(())
            })
            .await
            .context("等待终端启动确认超时")??;
            anyhow::Ok(z_channel.0.take().context("终端通道已释放")?)
        };
        let z_opened = tokio::select! {
            l_opened = z_opened => Some(l_opened),
            _ = &mut z_stop_rx => None,
        };
        let z_channel = match z_opened {
            Some(Ok(z_channel)) => z_channel,
            // 启动期间用户关闭了终端; 未完成的通道由守卫关闭, SSH 连接保留.
            None => {
                set_status(c_session, "connected".into());
                shell_closed(c_session, "");
                self.terminal_changed.notify_one();
                return;
            }
            // 部分账号只允许 SFTP, 终端打不开时保留连接供文件传输使用; 若其实是连接已断, 由连接检查改为断开状态.
            Some(Err(err)) => {
                c_session.shell_stop.lock().unwrap_or_else(PoisonError::into_inner).take();
                set_status(c_session, "connected".into());
                shell_closed(c_session, &format!("终端打开失败: {}. 文件传输仍可使用.", safe_error(&format!("{err:#}"))));
                self.terminal_changed.notify_one();
                return;
            }
        };
        // 终端就绪后才标记已连接, 界面随后才打开 SFTP 读目录, 与登录过程错开.
        set_status(c_session, "connected".into());
        c_session.shell_ready.store(true, Ordering::Release);
        // 监控的采样命令在终端启动后且侧栏展开、为当前标签时才启动: 既避开登录欢迎信息 (Ubuntu pam_motd), 又避免后台挂机持续拉流.
        self.sync_monitors();
        let (mut z_reader, z_writer) = z_channel.split();
        // 终端协议回复 (光标位置、设备属性等) 走单独的有界队列: 不与用户输入争用, 输入队列满时远端等待的回复也能发出.
        // 读取循环只能 try_send, 不能等待, 否则会卡住整条连接的收包; 回复队列满时才丢弃.
        let (z_reply_tx, mut z_reply_rx) = mpsc::channel::<Vec<u8>>(64);
        let z_session_for_write = c_session.clone();
        let z_store_for_write = self.store.clone();
        tokio::spawn(async move {
            // 是否仍允许写入: 用户输入只要求已连接; MCP 输入还要求授权未撤销、租约未被人工输入收回.
            let z_allowed = |c_actor: Actor, c_lease: Option<&str>| {
                z_session_for_write.status.lock().is_ok_and(|c| c.as_str() == "connected")
                    && (c_actor == Actor::User
                        || z_store_for_write.get(&z_session_for_write.connection.id).is_ok_and(|c| mcp_permits(&c, &z_session_for_write.connection))
                            && z_session_for_write.lease.lock().ok().and_then(|l| l.as_ref().map(|l| l.id == c_lease.unwrap_or("") && l.until > Instant::now())).unwrap_or(false))
            };
            // 代填密码在真正发送前再确认光标行仍是密码提示: 排队期间提示消失 (已输入、超时) 时不把密码送进命令行.
            let z_at_prompt = || {
                let l_term = z_session_for_write.terminal.lock().unwrap_or_else(PoisonError::into_inner);
                let l_screen = l_term.parser.screen();
                let l_row = usize::from(l_screen.cursor_position().0);
                password_prompt(&l_screen.rows(0, screen_size(l_screen).1).nth(l_row).unwrap_or_default())
            };
            // 输入经独立写端发送, 当前一块在等待对端窗口时 resize、回复与关闭仍能处理; 下一块要等当前一块写完, 保持顺序.
            // 人工输入和协议回复由 russh 在对端窗口更新时唤醒. 需复核的输入每块不超过 1 KiB 和当前窗口, 在写端之外确认窗口后再交出:
            // 只有本任务写这个通道, 确认后窗口不会被别人用掉, 交出的块只需等出站队列名额.
            // MCP 输入核对授权与租约 (人工接管或撤权后放弃剩余部分), 代填密码核对光标行仍是密码提示: 每次轮询写端之前复核,
            // 扣窗口与拿到出站队列名额后入队都在紧随其后的同一次轮询里同步完成, 复核与提交之间没有等待点, 队列腾出名额时也先复核再入队.
            // 等名额期间复核失败就丢弃写端, 数据不会再发出; russh 已为这块扣了窗口, 本地少算不超过 1 KiB (对端按自己的消耗补发窗口,
            // 窗口上限远大于此, 不会因此停住). 写端每块新建, 丢弃的块不会被下一次写入带出去.
            let z_start = |c_bytes: Vec<u8>, c_actor: Actor, c_lease: Option<String>, c_password: bool| {
                let (z_allowed, z_at_prompt, z_writer) = (&z_allowed, &z_at_prompt, &z_writer);
                Box::pin(async move {
                    if c_actor == Actor::User && !c_password {
                        if z_allowed(c_actor, None) {
                            z_writer.data_bytes(c_bytes).await.map_err(std::io::Error::other)?;
                        }
                        return Ok(());
                    }
                    let z_valid = || z_allowed(c_actor, c_lease.as_deref()) && (!c_password || z_at_prompt());
                    let mut l_start = 0;
                    while l_start < c_bytes.len() {
                        if !z_valid() {
                            return Ok(());
                        }
                        let l_window = z_writer.writable_packet_size().await.min(1024);
                        if l_window == 0 {
                            tokio::time::sleep(Duration::from_millis(200)).await;
                            continue;
                        }
                        // 分块边界避开 UTF-8 多字节字符的中间 (后续字节为 10xxxxxx): 中途放弃时不留下半个字符.
                        let mut l_end = (l_start + l_window).min(c_bytes.len());
                        while l_end < c_bytes.len() && l_end > l_start + 1 && c_bytes[l_end] & 0xC0 == 0x80 {
                            l_end -= 1;
                        }
                        let mut l_tx = z_writer.make_writer();
                        let mut l_write = std::pin::pin!(l_tx.write_all(&c_bytes[l_start..l_end]));
                        let l_done = std::future::poll_fn(|c_cx| {
                            if !z_valid() {
                                return std::task::Poll::Ready(None);
                            }
                            l_write.as_mut().poll(c_cx).map(Some)
                        })
                        .await;
                        match l_done {
                            Some(l_done) => l_done?,
                            None => return Ok(()),
                        }
                        l_start = l_end;
                    }
                    std::io::Result::Ok(())
                })
            };
            let mut z_sending = None;
            // 尺寸调整也不在分支内等待: 出站队列满时仍能响应关闭; 新尺寸到来时丢弃尚未发出的旧尺寸, 只发最新的.
            let mut z_resizing = None;
            loop {
                tokio::select! {
                    l_done = async { z_sending.as_mut().unwrap().await }, if z_sending.is_some() => {
                        let l_done: std::io::Result<()> = l_done;
                        z_sending = None;
                        if l_done.is_err() { break; }
                    }
                    l_reply = z_reply_rx.recv(), if z_sending.is_none() => match l_reply {
                        Some(l_reply) => z_sending = Some(z_start(l_reply, Actor::User, None, false)),
                        // 发送端随读取循环结束而释放: 终端已在收尾, 不再空转.
                        None => break,
                    },
                    input = c_input.recv(), if z_sending.is_none() => match input {
                        Some(input) => z_sending = Some(z_start(input.data, input.actor, input.lease_id, input.password)),
                        None => break,
                    },
                    l_done = async { z_resizing.as_mut().unwrap().await }, if z_resizing.is_some() => {
                        let l_done: Result<(), russh::Error> = l_done;
                        z_resizing = None;
                        if l_done.is_err() { break; }
                    }
                    resize = c_resize.changed() => {
                        // 发送端在终端结束或关闭时释放, 已关闭的通道会立即返回, 借此结束写入任务 (包括正在等待窗口的输入).
                        if resize.is_err() { break; }
                        let (rows, cols) = *c_resize.borrow_and_update();
                        z_resizing = Some(Box::pin(z_writer.window_change(cols as u32, rows as u32, 0, 0)));
                    }
                }
            }
            // 输入通道被释放 (关闭终端) 时主动关闭通道, SSH 连接保留.
            // 关闭终端时远端 Shell 可能仍在运行, 服务器要等它退出才回应关闭; 先发挂断信号让它尽快结束.
            drop(z_sending);
            drop(z_resizing);
            let _ = tokio::time::timeout(Duration::from_secs(15), async {
                let _ = z_writer.signal(russh::Sig::HUP).await;
                let _ = z_writer.eof().await;
                let _ = z_writer.close().await;
            })
            .await;
        });
        // 先处理完画面 (释放画面锁) 再投递回复.
        let z_feed = |c_data: &[u8]| {
            // 锁中毒也照常写入: 原先在中毒时丢弃数据, 画面会从此停住.
            let (l_replies, l_recovered) = {
                let mut l_term = c_session.terminal.lock().unwrap_or_else(PoisonError::into_inner);
                let l_replies = l_term.process(c_data);
                (l_replies, std::mem::take(&mut l_term.recovered).then(|| screen_size(l_term.parser.screen())))
            };
            if !l_replies.is_empty() {
                let _ = z_reply_tx.try_send(l_replies);
            }
            if let Some(l_size) = l_recovered {
                request_redraw(c_session, l_size);
            }
        };
        if !z_early.is_empty() {
            for l_data in &z_early {
                z_feed(l_data);
            }
            self.terminal_changed.notify_one();
        }
        let mut z_stopped = false;
        let mut z_channel_dropped = false;
        loop {
            let z_message = tokio::select! {
                l_message = z_reader.wait() => l_message,
                _ = &mut z_stop_rx => { z_stopped = true; break; }
            };
            match z_message {
                Some(ChannelMsg::Data { data }) | Some(ChannelMsg::ExtendedData { data, .. }) => {
                    z_feed(&data);
                    self.terminal_changed.notify_one();
                }
                Some(ChannelMsg::Eof | ChannelMsg::Close) => break,
                None => {
                    z_channel_dropped = true;
                    break;
                }
                Some(_) => {}
            }
        }
        drop(z_reader);
        c_session.shell_stop.lock().unwrap_or_else(PoisonError::into_inner).take();
        // 通道因底层连接中断返回 None 时, russh 可能晚几毫秒才把 Handle 标记为已关闭;
        // 短暂等待后再判定, 避免先误清屏显示"终端已关闭"再跳成断线状态.
        if !z_stopped && z_channel_dropped && !c_handle.is_closed() {
            for _ in 0..10 {
                if c_handle.is_closed() || c_session.main_link.has_error() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        if !z_stopped && (c_handle.is_closed() || c_session.main_link.has_error()) {
            close_session(c_session, &dropped_status(c_session).await, true);
            c_session.handle.lock().unwrap_or_else(PoisonError::into_inner).take();
        } else {
            shell_closed(c_session, "");
        }
        self.terminal_changed.notify_one();
    }

    // 在保持连接的会话上重新打开终端 (仅文件传输连接或终端退出后).
    fn open_terminal(self: &Arc<Self>, c_args: Value, c_actor: Actor) -> Result<Value> {
        let z_session = self.session_args(&c_args, c_actor)?;
        ensure!(z_session.status.lock().unwrap_or_else(PoisonError::into_inner).as_str() == "connected", "SSH会话尚未连接");
        let z_handle = z_session.ssh_handle()?;
        let (z_tx, z_rx) = mpsc::channel::<TerminalInput>(32);
        let (z_resize_tx, z_resize_rx) = tokio::sync::watch::channel((0u16, 0u16));
        // 持输入通道锁一次装好终端状态、代次、输入与尺寸通道和关闭信号, 与 close_terminal 互斥.
        // 锁内不取终端画面锁: 读取循环持画面锁时会取输入通道锁发送协议回复, 反向加锁会死锁.
        let z_stop = {
            let mut l_tx = z_session.terminal_tx.lock().unwrap_or_else(PoisonError::into_inner);
            ensure!(!z_session.shell.swap(true, Ordering::AcqRel), "终端已打开");
            // 同一毫秒内重开也要换代次.
            z_session.terminal_gen.fetch_max(epoch_ms().max(z_session.terminal_gen.load(Ordering::Acquire) + 1), Ordering::AcqRel);
            *l_tx = Some(z_tx);
            *z_session.resize_tx.lock().unwrap_or_else(PoisonError::into_inner) = Some(z_resize_tx);
            install_shell_stop(&z_session)
        };
        // 终端未打开前没有读取循环写画面, 清屏放在锁外.
        z_session.terminal.lock().unwrap_or_else(PoisonError::into_inner).reset_screen();
        *z_session.input_line.lock().unwrap_or_else(PoisonError::into_inner) = (0, false, String::new());
        let z_backend = self.clone();
        tokio::spawn(async move { z_backend.run_shell(&z_session, &z_handle, z_rx, z_resize_rx, z_stop).await });
        Ok(json!({"opened": true}))
    }

    async fn ask_secret(&self, c_session: &LiveSession, c_prompt: String) -> Result<Zeroizing<String>> {
        let mut z_answers = self.request_auth_answers(c_session, vec![AuthPrompt { prompt: c_prompt, echo: false }]).await?;
        ensure!(z_answers.len() == 1, "登录认证已取消");
        Ok(Zeroizing::new(z_answers.remove(0)))
    }

    // 认证成功后的密码或口令写入连接文件, 下次连接不再询问. 保存失败不影响本次已成功的登录, 只记录日志.
    fn remember_secret(&self, c_session: &LiveSession, c_secret: &str) {
        if let Err(err) = self.store.get(&c_session.connection.id).and_then(|c| self.store.save(c, Some(c_secret))) {
            eprintln!("保存连接密码失败: {err:#}");
        }
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
                    z_result = tokio::time::timeout(
                        Duration::from_secs(60),
                        c_handle.authenticate_keyboard_interactive_respond(z_answers),
                    )
                    .await
                    .context("SSH 交互认证响应超时")??;
                }
            }
        }
        Ok(false)
    }

    async fn request_auth_answers(&self, c_session: &LiveSession, c_prompts: Vec<AuthPrompt>) -> Result<Vec<String>> {
        if c_prompts.is_empty() { return Ok(Vec::new()); }
        let (tx, rx) = oneshot::channel();
        *c_session.auth_answers.lock().unwrap_or_else(PoisonError::into_inner) = Some((uuid::Uuid::new_v4().to_string(), c_prompts, tx));
        set_status(c_session, "auth_required".into());
        let z_result = tokio::time::timeout(Duration::from_secs(120), rx).await;
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
        drop(z_state);
        self.sync_monitors();
        Ok(json!({"focused": true,"session_id":z_id}))
    }

    fn trust_host(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        require_user(c_actor)?;
        let z_session = self.session_args(&c_args, c_actor)?;
        let z_decision = required_string(&c_args, "decision")?;
        let mut z_trust = z_session.trust.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(prompt) = z_trust.take() else {
            bail!("当前没有待确认的主机指纹");
        };
        // 核对界面展示的指纹: 弹窗期间同一标签已重连时, 旧弹窗的确认不能用于新连接, 更不能永久信任未经核对的指纹.
        if string_arg(&c_args, "fingerprint") != prompt.fingerprint {
            *z_trust = Some(prompt);
            bail!("主机指纹已变化, 请重新核对");
        }
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
        let z_session = self.session_args(&c_args, c_actor)?;
        let z_answers: Vec<String> =
            serde_json::from_value(c_args.get("answers").cloned().context("缺少 answers")?)?;
        ensure!(
            z_answers.len() <= 32 && z_answers.iter().all(|s| s.len() <= 4096),
            "认证答案超出限制"
        );
        // 按请求标识核对并取走回答通道 (同一把锁内完成): 弹窗期间提示已换成下一轮或别的连接时, 迟到的回答不会交给新的提示.
        let z_tx = {
            let mut l_slot = z_session.auth_answers.lock().unwrap_or_else(PoisonError::into_inner);
            let l_id = c_args.get("auth_id").and_then(Value::as_str);
            l_slot.take_if(|c| Some(c.0.as_str()) == l_id).context("认证提示已失效, 请重新输入")?.2
        };
        let _ = z_tx.send(z_answers);
        Ok(json!({"submitted": true}))
    }

    fn read_terminal(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        let z_session = self.session_args(&c_args, c_actor)?;
        if c_args.get("events_only").and_then(Value::as_bool) == Some(true) {
            require_user(c_actor)?;
        }
        let mut z_term = z_session.terminal.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(Self::read_terminal_locked(&z_session, &mut z_term, &c_args, c_actor))
    }

    // 面板缩放等场景下的非阻塞快路径: 在无锁竞争时同步完成 resize + read, 避免界面分两帧异步重排.
    #[cfg(feature = "desktop")]
    pub(crate) fn try_resize_and_read_terminal(&self, c_session_id: &str, c_resize: Option<(u16, u16)>, c_args: &Value) -> Option<Result<Value>> {
        let z_session = {
            let l_state = self.state.try_lock().ok()?;
            l_state.sessions.get(c_session_id).cloned()
        };
        let Some(z_session) = z_session else {
            return Some(Err(anyhow::anyhow!("会话不存在")));
        };
        let mut z_term = z_session.terminal.try_lock().ok()?;
        if let Some((l_rows, l_cols)) = c_resize {
            let l_resize_tx = z_session.resize_tx.try_lock().ok()?;
            let l_rows = l_rows.clamp(1, MAX_TERMINAL_LINES);
            let l_cols = l_cols.clamp(1, MAX_TERMINAL_COLS);
            z_term.resize(l_rows, l_cols);
            z_session.history_bytes.fetch_max(z_term.scrollback * usize::from(l_cols) * 32, Ordering::Relaxed);
            if let Some(l_tx) = l_resize_tx.as_ref() {
                l_tx.send_replace((l_rows, l_cols));
            }
        }
        Some(Ok(Self::read_terminal_locked(&z_session, &mut z_term, c_args, Actor::User)))
    }

    fn read_terminal_locked(c_session: &LiveSession, c_term: &mut Terminal, c_args: &Value, c_actor: Actor) -> Value {
        let z_max = bounded_u16(c_args, "max_lines", 200, 1, MAX_SCROLLBACK as u16) as usize;
        let z_offset = c_args
            .get("scroll_offset")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            .min(MAX_SCROLLBACK as u64) as usize;
        let z_after_seq = c_args.get("after_seq").and_then(Value::as_u64);
        // 程序开启同步输出后漏发结束标记且不再输出时, 不会再有 bump 触发刷新; 超过 150ms 视为同步结束, 照常显示.
        if c_term.sync_since.is_some_and(|c| c.elapsed() >= Duration::from_millis(150)) {
            c_term.sync_since = None;
            c_term.seq = c_term.seq.wrapping_add(1);
        }
        // 每 25ms 轮询一次, 仅在有待写剪贴板时才读取连接配置复核权限.
        let z_clipboard = if c_actor == Actor::User {
            c_term.clipboard.take()
        } else { None };
        if c_args.get("events_only").and_then(Value::as_bool) == Some(true) {
            return c_term.unchanged(z_clipboard);
        }
        if z_offset == 0 && z_after_seq == Some(c_term.seq) {
            return c_term.unchanged(z_clipboard);
        }
        let z_seq = c_term.seq;
        // 只读取当前一页, 避免每次按键回显都复制整份历史缓存.
        let z_screen = c_term.parser.screen_mut();
        let z_previous_offset = z_screen.scrollback();
        z_screen.set_scrollback(MAX_SCROLLBACK);
        let z_history_lines = z_screen.scrollback();
        z_screen.set_scrollback(z_offset);
        let z_actual_offset = z_screen.scrollback();
        let (row, col) = z_screen.cursor_position();
        let z_alternate = z_screen.alternate_screen();
        let mut z_result = json!({"seq":z_seq,"cols":z_screen.size().1,"rows":z_screen.size().0,"cursor_row":row,"cursor_col":col,"alternate_screen":z_alternate,"bracketed_paste":z_screen.bracketed_paste(),"application_cursor":z_screen.application_cursor(),"hide_cursor":z_screen.hide_cursor(),"scrollback_lines":z_history_lines,"scroll_offset":z_actual_offset});
        if c_actor != Actor::User {
            z_result["text"] = json!(tail_lines(&z_screen.contents(), z_max));
        }
        if c_actor == Actor::Mcp && z_actual_offset == 0 {
            // 光标所在行与用户未提交输入的标记: MCP 据此判断提示符后是否有用户输入了一半的命令 (翻看历史时不给).
            z_result["cursor_line"] = json!(z_screen.rows(0, screen_size(z_screen).1).nth(usize::from(row)).unwrap_or_default().trim_end());
            z_result["user_input_pending"] = json!(c_session.input_line.lock().unwrap_or_else(PoisonError::into_inner).0 > 0);
        }
        if c_actor == Actor::User {
            z_result["mouse"] = json!(mouse_mode(z_screen));
            z_result["mouse_tracking"] = json!(mouse_tracking(z_screen));
            // 界面按 VT 网格逐行显示 (长行在终端宽度处折行, 与普通终端一致);
            // 文字按行给出供选择复制, 折行处记入 wrapped, 复制时不插入换行.
            let (z_rows, z_cols) = screen_size(z_screen);
            let z_rows = z_rows.min(MAX_TERMINAL_LINES);
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
        z_result["modified_keys"] = json!(c_term.key_mode());
        z_result["cursor_shape"] = json!(c_term.cursor_shape);
        if let Some(z_text) = z_clipboard { z_result["clipboard"] = json!(z_text); }
        z_result
    }

    fn resize_terminal(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        require_user(c_actor)?;
        let z_session = self.session_args(&c_args, c_actor)?;
        let rows = bounded_u16(&c_args, "rows", 30, 1, MAX_TERMINAL_LINES);
        let cols = bounded_u16(&c_args, "cols", 100, 1, MAX_TERMINAL_COLS);
        {
            let mut t = z_session.terminal.lock().unwrap_or_else(PoisonError::into_inner);
            t.resize(rows, cols);
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
        let z_session = self.session_args(&c_args, c_actor)?;
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
            password: c_args.get("_password").and_then(Value::as_bool) == Some(true),
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
        // 输入已进入该终端的发送队列 (按顺序发出); 发送前授权被撤销或终端关闭时剩余部分不再发送, 结果以 read_terminal 为准.
        Ok(json!({"queued": z_data.len()}))
    }

    // 向终端输入连接设置的管理员密码并回车. 密码只在本程序内取出写入终端, 不出现在 MCP 参数、返回值和操作记录中.
    async fn input_admin_password(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        let z_session_id = required_string(&c_args, "session_id")?;
        let z_session = self.session_args(&c_args, c_actor)?;
        let z_conn = self.store.get(&z_session.connection.id)?;
        let mut z_data = match z_conn.admin_password {
            AdminPassword::None => bail!("此连接未设置管理员密码, 可在编辑连接中设置"),
            // 优先用本次登录实际使用的密码 (含连接时临时输入的), 其次是连接中保存的密码.
            AdminPassword::Login => {
                ensure!(z_conn.auth == AuthMethod::Password, "此连接不是密码登录, 管理员密码需单独设置");
                let l_login = match z_session.transfer_auth.lock().unwrap_or_else(PoisonError::into_inner).as_ref() {
                    Some(TransferAuth::Password(c)) => Some(c.clone()),
                    _ => None,
                };
                match l_login {
                    Some(l_password) => l_password,
                    None => self.store.secret(&z_conn.id)?.context("此连接没有保存登录密码")?,
                }
            }
            AdminPassword::Custom => self.store.admin_secret(&z_conn.id)?.context("管理员密码未保存, 请重新设置")?,
        };
        // 记下检查时的终端代次随输入核对: 期间终端重开或标签重连时不把密码交给新的 Shell.
        let z_terminal = z_session.terminal_gen.load(Ordering::Acquire);
        // 界面与 MCP 都只在密码提示处输入: 回显已关闭, 密码不会显示或进入 Shell 历史; 误按快捷键时不把密码送进命令行.
        let z_line = {
            let l_term = z_session.terminal.lock().unwrap_or_else(PoisonError::into_inner);
            let l_screen = l_term.parser.screen();
            let l_row = usize::from(l_screen.cursor_position().0);
            l_screen.rows(0, screen_size(l_screen).1).nth(l_row).unwrap_or_default()
        };
        ensure!(password_prompt(&z_line), "终端当前不是密码提示, 未输入");
        if c_actor == Actor::Mcp {
            // 短时间内再次需要输入多半是密码错误, 交还用户处理, 避免触发锁定.
            let z_last = *z_session.admin_input.lock().unwrap_or_else(PoisonError::into_inner);
            ensure!(z_last.is_none_or(|c| c.elapsed() >= Duration::from_secs(15)), "距上次代填不足 15 秒, 可能密码错误, 请用户自行输入");
        }
        z_data.push('\r');
        // 带上密码标记: 写入任务真正发送前再核对一次密码提示 (排队期间提示可能已消失, 消失则不发送).
        self.write_terminal(json!({"session_id":z_session_id,"data":z_data.as_str(),"terminal":z_terminal,"_password":true}), c_actor).await?;
        if c_actor == Actor::Mcp {
            *z_session.admin_input.lock().unwrap_or_else(PoisonError::into_inner) = Some(Instant::now());
        }
        // 只代表已排队: 真正发送前密码提示若已消失则不发送, 是否生效以 read_terminal 为准.
        Ok(json!({"queued":true}))
    }

    async fn disconnect(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        let z_id = required_string(&c_args, "session_id")?.to_owned();
        // keep_tab: 只断开连接, 标签保留以便稍后重连.
        let z_close_tab = c_args.get("keep_tab").and_then(Value::as_bool) != Some(true);
        self.disconnect_session(&z_id, c_actor, z_close_tab, false, false).await?;
        Ok(json!({"disconnected":true}))
    }

    async fn disconnect_session(
        &self,
        c_id: &str,
        c_actor: Actor,
        c_close_tab: bool,
        c_keep_transfers: bool,
        c_only_offline: bool,
    ) -> Result<bool> {
        let (z_session, z_connection_id) = {
            let z_state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let z_tab = z_state.tabs.iter().find(|c| c.id == c_id).context("标签已关闭")?;
            (z_state.sessions.get(c_id).cloned(), z_tab.connection_id.clone())
        };
        // c_only_offline: 只处理已断开或失败的会话, 在线时不动它并返回 false. 检查、授权和之后的摘除都针对取到的同一个对象.
        if c_only_offline && z_session.as_ref().is_some_and(|c| {
            let l_status = c.status.lock().unwrap_or_else(PoisonError::into_inner);
            !matches!(l_status.as_str(), "disconnected" | "authentication_failed") && !l_status.starts_with("error:")
        }) {
            return Ok(false);
        }
        if let Some(z_session) = &z_session {
            if c_actor == Actor::Mcp {
                ensure!(self.mcp_permitted(&z_session.connection), "此连接未授权 MCP");
            }
            close_session(z_session, "disconnected", false);
        } else {
            // 已断开只剩标签: MCP 只能处理已授权连接的标签 (重连或关闭).
            if c_actor == Actor::Mcp {
                ensure!(z_connection_id.and_then(|c| self.store.get(&c).ok()).is_some_and(|c| c.mcp_allowed), "此连接未授权 MCP");
            }
        }
        // 并发重连可能已在同一标签装入新会话: 只断开时不动它和它的任务; 关闭标签时连同新会话一并关闭.
        let z_newer = {
            let mut z_state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let z_newer = z_state.sessions.get(c_id).filter(|c| !z_session.as_ref().is_some_and(|l| Arc::ptr_eq(l, c))).cloned();
            // MCP 关闭标签时新会话的授权也要核对, 未授权就整体放弃, 不关闭用户的新连接.
            ensure!(
                !(c_close_tab && c_actor == Actor::Mcp && z_newer.as_ref().is_some_and(|c| !self.mcp_permitted(&c.connection))),
                "标签已重新连接, 请重试"
            );
            if z_newer.is_none() || c_close_tab {
                for z_task in z_state.tasks.values() {
                    if let Ok(mut z_task) = z_task.lock() {
                        if z_task.session_id == c_id && !(c_keep_transfers && matches!(z_task.kind.as_str(), "upload" | "download")) {
                            if let Some(z_cancel) = z_task.cancel.take() { let _ = z_cancel.send(()); }
                        }
                    }
                }
                z_state.sessions.remove(c_id);
            }
            if c_close_tab {
                z_state.tabs.retain(|c| c.id != c_id);
                if z_state.active_session_id.as_deref() == Some(c_id) {
                    z_state.active_session_id = z_state.tabs.last().map(|c| c.id.clone());
                }
            }
            z_newer.filter(|_| c_close_tab)
        };
        if let Some(z_newer) = &z_newer {
            close_session(z_newer, "disconnected", false);
        }
        self.sync_monitors();
        for z_closed in z_session.iter().chain(z_newer.iter()) {
            let z_handle = z_closed.handle.lock().unwrap_or_else(PoisonError::into_inner).take();
            let z_socket = z_closed.socket.lock().unwrap_or_else(PoisonError::into_inner).take();
            // disconnect 只代表消息已入队: 通道积压时 russh 暂停出站队列, 消息可能一直发不出去. 留时间正常告别, 之后直接关闭套接字,
            // 旧连接和挂在上面的后台任务都随之结束. 放进独立任务: 调用方被取消 (如 MCP 调用超时) 时强关也照样执行.
            tokio::spawn(async move {
                if let Some(l_handle) = &z_handle {
                    let _ = tokio::time::timeout(
                        Duration::from_secs(2),
                        l_handle.disconnect(Disconnect::ByApplication, "关闭连接", "zh-CN"),
                    )
                    .await;
                    let _ = tokio::time::timeout(Duration::from_secs(3), async {
                        while !l_handle.is_closed() { tokio::time::sleep(Duration::from_millis(100)).await; }
                    })
                    .await;
                }
                if let Some(l_socket) = z_socket {
                    let _ = l_socket.stream.shutdown(std::net::Shutdown::Both);
                }
            });
        }
        Ok(true)
    }

    async fn run_command(self: &Arc<Self>, c_args: Value, c_actor: Actor) -> Result<Value> {
        let z_session = self.session_args(&c_args, c_actor)?;
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

    async fn file_call(self: &Arc<Self>, c_method: &str, c_args: &Value, c_actor: Actor) -> Result<Value> {
        if !matches!(c_method, "list_files" | "check_remote_names") {
            require_user(c_actor)?;
        }
        let z_session = self.session_args(c_args, c_actor)?;
        // 重试属于原请求, 不能取代后来发起的目录补全.
        let z_seq = self.list_seq.fetch_add(1, Ordering::Relaxed) + 1;
        let mut z_retried = false;
        loop {
            let mut l_operation = BrowseOperation::default();
            let l_result = match c_method {
                "list_files" => self.list_files(&z_session, c_args, c_actor, z_seq, &mut l_operation).await,
                "check_remote_names" => self.check_remote_names(&z_session, c_args, &mut l_operation).await,
                "set_permissions" => self.set_permissions(&z_session, c_args, &mut l_operation).await,
                "rename_file" => self.rename_file(&z_session, c_args, &mut l_operation).await,
                "delete_files" => self.delete_files(&z_session, c_args, &mut l_operation).await,
                _ => self.create_remote_entry(&z_session, c_method, c_args, &mut l_operation).await,
            };
            match l_result {
                Err(err) if sftp_retryable(&err) => {
                    if let Some(l_raw) = l_operation.channel.and_then(|c| c.upgrade()) {
                        drop_browse_sftp(&z_session, &l_raw);
                    }
                    // 读取和写入前的准备可以重试一次. 已开始修改时, 服务器可能已经执行.
                    if z_retried || l_operation.write_started {
                        return Err(err);
                    }
                    ensure!(Arc::ptr_eq(&self.session_args(c_args, c_actor)?, &z_session), "标签已重新连接, 请刷新");
                    z_retried = true;
                }
                l_result => return l_result,
            }
        }
    }

    async fn list_files(self: &Arc<Self>, c_session: &Arc<LiveSession>, c_args: &Value, c_actor: Actor, c_seq: u64, c_operation: &mut BrowseOperation) -> Result<Value> {
        let z_dirs_only = c_args.get("dirs_only").and_then(Value::as_bool) == Some(true);
        let z_names_only = c_args.get("names_only").and_then(Value::as_bool) == Some(true);
        let z_request = c_args
            .get("path")
            .and_then(Value::as_str)
            .unwrap_or(".")
            .to_owned();
        // 界面带上目录树已展开的节点时, 同时读取路径上尚未展开的上级目录.
        let z_expanded = c_args.get("tree_expanded").and_then(Value::as_array).filter(|_| c_actor == Actor::User)
            .map(|c| c.iter().filter_map(Value::as_str).map(str::to_owned).collect::<std::collections::HashSet<_>>());
        // 同一连接的文件操作排队执行; 前面的操作卡住时不无限等待, 报错后刷新即可重试.
        let z_permit = tokio::time::timeout(Duration::from_secs(30), c_session.file_slot.clone().lock_owned())
            .await
            .map_err(|_| anyhow::anyhow!("等待其他文件操作完成超时"))?;
        let z_handle = c_session.ssh_handle()?;
        let raw = self.browse_sftp(c_session, &z_handle).await?;
        c_operation.channel = Some(Arc::downgrade(&raw));
        // 请求尽量同时发出: 走代理时耗时取决于串行往返次数, 解析路径与打开目录并行, 各级上级目录也并行, 不随路径深度增加.
        // 放进独立任务: 外层被取消 (如 MCP 调用整体超时) 时仍会读完并关闭句柄, 复用的 SFTP 会话不会积累未关闭的目录句柄.
        // 读完先返回结果, 再关闭句柄; 名额到全部关闭才释放, 传输让出浏览通道时以此为准.
        let z_task_session = c_session.clone();
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
                let l_listed = read_remote_entries(&raw, &z_opened, z_dirs_only).await;
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
        let (z_path, (mut entries, truncated, owner_ids), z_ancestors) = z_result_rx.await.context("读取目录任务异常")??;
        // 目录树只需子目录名, 上传"保留两者"只需已有文件名: 均不查属主和时间, 也不顶掉当前目录的补全槽位.
        // 读取期间标签已重连 (同一会话 ID 换了新连接): 旧连接的列表不再返回, 免得界面察觉重连前把它当新连接的列表启用文件操作.
        let z_current = || self.state.lock().unwrap_or_else(PoisonError::into_inner).sessions.get(&c_session.id).is_some_and(|c| Arc::ptr_eq(c, c_session));
        if z_dirs_only || z_names_only {
            ensure!(z_current(), "标签已重新连接, 请刷新目录");
            sort_remote_entries(&mut entries);
            return Ok(json!({"session_id":c_session.id,"path":z_path,"entries":entries,"truncated":truncated}));
        }
        // 用户名与时间换算各自开通道, 此时已不占名额, 慢时不挡住同一连接的其他目录操作.
        // 缓存命中时立即完成; 需要远端查询则先返回基础列表 (属主显示数字 ID, 时间按 UTC), 查询在后台继续,
        // 界面凭 details_token 取结果后就地更新这两列.
        // 补全任务只带名称、时间、属主三项, 不复制整份列表; 同一会话只保留界面最新目录的补全, 旧的直接取消, 释放内存和通道.
        // MCP 取不了后续补全结果, 在调用内等待并且不占用界面的补全槽位, 不会顶掉界面正在等的补全.
        let z_detail_session = c_session.clone();
        let mut z_detailed = entries
            .iter()
            .map(|c| json!({"name":c["name"],"modified_time":c["modified_time"],"owner":c["owner"]}))
            .collect::<Vec<_>>();
        let mut z_details = tokio::spawn(async move {
            resolve_file_owners(&z_detail_session, &z_handle, &owner_ids, &mut z_detailed).await;
            let l_times = resolve_file_times(&z_detail_session, &z_handle, &mut z_detailed).await;
            (z_detailed, l_times)
        });
        // 以发起顺序取代旧补全, 先发起但后返回的列表不能取消新目录的补全.
        if c_actor == Actor::User {
            let mut l_slot = c_session.file_details.lock().unwrap_or_else(PoisonError::into_inner);
            if l_slot.as_ref().is_none_or(|c| c.seq < c_seq) {
                if let Some(l_old) = l_slot.replace(FileDetailsTask { seq: c_seq, abort: z_details.abort_handle(), pending: None }) {
                    l_old.abort.abort();
                }
            }
        }
        let z_ready = if c_actor == Actor::User {
            (&mut z_details).now_or_never()
        } else {
            tokio::time::timeout(Duration::from_secs(5), &mut z_details).await.ok()
        };
        let ((z_server_timezone, z_time_hint), z_details_token) =
            match z_ready {
                Some(Ok((l_detailed, l_times))) => {
                    for (l_entry, l_detail) in entries.iter_mut().zip(l_detailed) {
                        l_entry["owner"] = l_detail["owner"].clone();
                        l_entry["modified_text"] = l_detail.get("modified_text").cloned().unwrap_or(Value::Null);
                    }
                    (l_times, None)
                }
                Some(Err(_)) => ((None, None), None),
                None => {
                    // 只在补全仍是本会话最新的一个时登记 (同时有多个目录请求时, 后发起的已取代并取消了它).
                    let mut l_slot = c_session.file_details.lock().unwrap_or_else(PoisonError::into_inner);
                    if let Some(l_latest) = l_slot.as_mut().filter(|c| c.seq == c_seq) {
                        l_latest.pending = Some((z_path.clone(), z_details));
                        ((None, None), Some(c_seq))
                    } else {
                        z_details.abort();
                        ((None, None), None)
                    }
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
        ensure!(z_current(), "标签已重新连接, 请刷新目录");
        // 浏览目录时后台静默预热独立传输连接, 下载起步即可直接走高速通道.
        if c_actor == Actor::User
            && c_session.transfer_suspect.lock().unwrap_or_else(PoisonError::into_inner).0 == 0
            && c_session.transfer_auth.lock().unwrap_or_else(PoisonError::into_inner).is_some()
            && c_session.transfer.try_lock().is_ok()
        {
            let l_self = self.clone();
            let l_session = c_session.clone();
            tokio::spawn(async move {
                let _ = l_self.transfer_handle(&l_session, None).await;
            });
        }
        Ok(json!({"session_id":c_session.id,"link":c_session.link,"path":z_path,"entries":entries,"total":z_total,"offset":z_offset,"truncated":truncated,"server_timezone":z_server_timezone,"time_hint":z_time_hint,"details_token":z_details_token,"tree_ancestors":z_ancestors}))
    }

    // 上传前的重名检查: 只查要上传的几个名字是否已存在, 不读整个目录, 也不查属主和时间.
    async fn check_remote_names(&self, c_session: &Arc<LiveSession>, c_args: &Value, c_operation: &mut BrowseOperation) -> Result<Value> {
        let z_directory = required_string(c_args, "directory")?.trim_end_matches('/');
        let z_names: Vec<String> = serde_json::from_value(c_args.get("names").cloned().context("缺少 names")?)?;
        ensure!(z_names.len() <= 2000, "一次最多检查 2000 个名称");
        ensure!(z_names.iter().all(|c| !c.is_empty() && !c.contains('/')), "文件名无效");
        // lstat 不打开句柄, 直接在主连接已有的 SFTP 通道上并发查询, 不必等目录遍历完成.
        // 新建通道或借用独立连接上的浏览通道时仍持有 file_slot, 让出通道时可等查询结束.
        let z_cached = c_session.browse_sftp.lock().unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .filter(|c| c.1 == 0 && c.2.load(Ordering::Acquire))
            .map(|c| c.0.clone());
        let _z_permit;
        let z_sftp = match z_cached {
            Some(l_cached) => l_cached,
            None => {
                _z_permit = tokio::time::timeout(Duration::from_secs(5), c_session.file_slot.lock())
                    .await
                    .map_err(|_| anyhow::anyhow!("等待目录浏览通道超时"))?;
                self.browse_sftp(c_session, &c_session.ssh_handle()?).await?
            }
        };
        c_operation.channel = Some(Arc::downgrade(&z_sftp));
        let mut z_existing = Vec::new();
        for l_chunk in z_names.chunks(64) {
            let mut l_checks = l_chunk.iter().map(|l_name| {
                let l_sftp = &z_sftp;
                async move { remote_entry_exists(l_sftp, format!("{z_directory}/{l_name}")).await.map(|c| c.then_some(l_name)) }
            }).collect::<FuturesUnordered<_>>();
            // 先等本组全部查询结束再判断结果, 中途出错也不会留下仍在使用通道的查询.
            let mut l_error = None;
            while let Some(l_done) = l_checks.next().await {
                match l_done {
                    Ok(Some(l_name)) => z_existing.push(l_name),
                    Ok(None) => {}
                    Err(err) => { l_error.get_or_insert(err); }
                }
            }
            if let Some(err) = l_error {
                return Err(err);
            }
        }
        Ok(json!({"existing":z_existing}))
    }

    // 取目录读取的属主、时间补全结果, 只返回这两项, 界面就地更新, 不重读目录也不清空选择.
    async fn file_details(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        let z_session = self.session_args(&c_args, c_actor)?;
        let z_token = c_args.get("token").and_then(Value::as_u64).context("缺少 token")?;
        let (z_path, mut z_task) = z_session.file_details.lock().unwrap_or_else(PoisonError::into_inner)
            .as_mut().filter(|c| c.seq == z_token).and_then(|c| c.pending.take()).context("目录已刷新")?;
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
        c_session: &Arc<LiveSession>,
        c_method: &str,
        c_args: &Value,
        c_operation: &mut BrowseOperation,
    ) -> Result<Value> {
        let z_parent = required_string(c_args, "path")?;
        let z_name = required_string(c_args, "name")?;
        ensure!(z_parent.len() <= 4096, "远端父目录路径过长");
        ensure!(
            !z_parent.chars().any(|c| c.is_control()),
            "远端父目录路径包含控制字符"
        );
        validate_new_remote_name(z_name)?;
        let (_z_permit, z_sftp) = self.lock_browse(c_session, c_operation).await?;
        let z_directory = resolve_remote_path(&z_sftp, z_parent.to_owned()).await?;
        ensure!(!z_directory.chars().any(char::is_control), "远端父目录返回的实际路径无效");
        let z_path = format!("{}/{z_name}", z_directory.trim_end_matches('/'));
        ensure!(z_path.len() <= 4096, "新路径过长");
        ensure!(!remote_entry_exists(&z_sftp, z_path.clone()).await?, "目标名称已存在");
        c_operation.write_started = true;
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
        Ok(json!({"session_id":c_session.id,"path":z_path,"directory":z_directory}))
    }

    async fn set_permissions(&self, c_session: &Arc<LiveSession>, c_args: &Value, c_operation: &mut BrowseOperation) -> Result<Value> {
        let z_path = required_string(c_args, "path")?;
        ensure!(z_path.len() <= 4096, "远端路径长度无效");
        ensure!(
            !z_path.chars().any(|c| c.is_control()),
            "远端路径包含控制字符"
        );
        let z_permissions = c_args
            .get("permissions")
            .and_then(Value::as_u64)
            .context("权限值需要0到0777的整数")?;
        ensure!(z_permissions <= 0o777, "权限值超出0777");
        let (_z_permit, z_sftp) = self.lock_browse(c_session, c_operation).await?;
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
        c_operation.write_started = true;
        tokio::time::timeout(
            Duration::from_secs(10),
            z_sftp.setstat(z_path.to_owned(), z_set_attrs),
        )
        .await
        .context("修改远端权限超时")??;
        Ok(json!({"session_id":c_session.id,"path":z_path,"permissions":z_permissions}))
    }

    async fn rename_file(&self, c_session: &Arc<LiveSession>, c_args: &Value, c_operation: &mut BrowseOperation) -> Result<Value> {
        let z_path = required_string(c_args, "path")?;
        let z_new_name = required_string(c_args, "new_name")?;
        validate_remote_file_path(z_path)?;
        validate_new_remote_name(z_new_name)?;
        let z_new_path = match z_path.rsplit_once('/') {
            Some(("/", _)) => format!("/{z_new_name}"),
            Some((l_parent, _)) => format!("{l_parent}/{z_new_name}"),
            None => z_new_name.to_owned(),
        };
        ensure!(z_new_path.len() <= 4096, "新路径过长");
        let (_z_permit, z_sftp) = self.lock_browse(c_session, c_operation).await?;
        ensure!(!remote_entry_exists(&z_sftp, z_new_path.clone()).await?, "目标名称已存在");
        c_operation.write_started = true;
        tokio::time::timeout(
            Duration::from_secs(10),
            z_sftp.rename(z_path.to_owned(), z_new_path.clone()),
        )
        .await
        .context("重命名远端文件超时")??;
        Ok(json!({"session_id":c_session.id,"path":z_path,"new_path":z_new_path}))
    }

    async fn delete_files(&self, c_session: &Arc<LiveSession>, c_args: &Value, c_operation: &mut BrowseOperation) -> Result<Value> {
        // 同时在途的删除请求数: 逐项删除每项要两次往返, 并发后大批量删除不必逐个等待.
        const DELETE_CONCURRENCY: usize = 16;
        let z_paths = c_args
            .get("paths")
            .and_then(Value::as_array)
            .context("paths需要数组")?;
        ensure!(
            !z_paths.is_empty() && z_paths.len() <= MAX_DELETE_ITEMS,
            "一次最多删除 {MAX_DELETE_ITEMS} 项"
        );
        let (_z_permit, z_sftp) = self.lock_browse(c_session, c_operation).await?;
        c_operation.write_started = true;
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
        Ok(json!({"session_id":c_session.id,"results":z_results}))
    }

    async fn lock_browse<'a>(&self, c_session: &'a Arc<LiveSession>, c_operation: &mut BrowseOperation) -> Result<(tokio::sync::MutexGuard<'a, ()>, Arc<RawSftpSession>)> {
        let z_guard = c_session.file_slot.lock().await;
        let z_sftp = self.browse_sftp(c_session, &c_session.ssh_handle()?).await?;
        c_operation.channel = Some(Arc::downgrade(&z_sftp));
        Ok((z_guard, z_sftp))
    }

    // 快速删除: 由服务器执行 rm -rf, 可递归删除目录, 大量小文件时远快于逐项 SFTP 删除.
    // 路径经标准输入以 NUL 分隔交给 xargs, 不拼入命令行, 文件名中的空格、引号不会被 shell 解释.
    async fn delete_files_fast(&self, c_args: Value, c_actor: Actor) -> Result<Value> {
        require_user(c_actor)?;
        let z_session = self.session_args(&c_args, c_actor)?;
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
        let z_session = self.session_args(&c_args, c_actor)?;
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

    async fn transfer(self: &Arc<Self>, c_method: &str, c_args: Value, c_actor: Actor) -> Result<Value> {
        let z_upload = c_method.starts_with("upload");
        let z_directory = c_method.ends_with("_directory");
        // 文件夹传输只开放给界面, MCP 工具范围不变.
        if z_directory {
            require_user(c_actor)?;
        }
        let z_session = self.session_args(&c_args, c_actor)?;
        let z_local_raw = required_string(&c_args, "local_path")?;
        ensure!(!z_local_raw.contains('\0'), "本机路径无效");
        let local = PathBuf::from(z_local_raw);
        // 相对路径会按程序工作目录解析, 文件落到用户想不到的位置; Windows 上还要求带盘符.
        ensure!(local.is_absolute() && local.file_name().is_some(), "本机路径必须是有效绝对路径");
        if c_actor == Actor::Mcp {
            ensure!(!local.components().any(|c| matches!(c, std::path::Component::ParentDir)), "本机路径不能包含上级目录跳转 (..)");
        }
        let remote = required_string(&c_args, "remote_path")?.to_owned();
        validate_remote_file_path(&remote)?;
        let overwrite = c_args.get("overwrite")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let z_size_hint = (c_actor == Actor::User && !z_upload && !z_directory)
            .then(|| c_args.get("size_hint").and_then(Value::as_u64))
            .flatten();
        let z_config_dir = self.store.directory().to_path_buf();
        let z_local_check = local.clone();
        // 本地元数据、悬空符号链接与 MCP 配置目录隔离放到阻塞线程核对, 避免慢速挂载盘卡住 Tokio 工作线程.
        let z_total = tokio::task::spawn_blocking(move || -> Result<Option<u64>> {
            if c_actor == Actor::Mcp {
                ensure!(!path_in_dir(&z_local_check, &z_config_dir), "禁止 MCP 访问 HiPShell 配置目录");
            }
            if z_upload {
                let l_metadata = z_local_check.metadata().context("读取上传源文件信息失败")?;
                ensure!(if z_directory { l_metadata.is_dir() } else { l_metadata.is_file() }, "上传源不存在");
                Ok((!z_directory).then_some(l_metadata.len()))
            } else {
                // 使用 symlink_metadata 识别悬空符号链接, 避免 overwrite=false 直到下载完毕提交时才报错.
                ensure!(std::fs::symlink_metadata(&z_local_check).is_err() || overwrite, "下载目标已存在");
                Ok(z_size_hint)
            }
        })
        .await
        .context("检查本地路径异常")??;
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
        let mut z_task = z_task.lock().unwrap_or_else(PoisonError::into_inner);
        let z_requested = z_task.finished_at_ms.is_none() && z_task.cancel.take().is_some_and(|c| c.send(()).is_ok());
        Ok(json!({"cancel_requested":z_requested}))
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
        if z_state.tasks.len() == MAX_TASKS {
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
                .map(|(_, id)| id)
                .context("任务队列已满")?;
            z_state.tasks.remove(&z_finished);
        }
        // 与断开在同一把锁内核对: 会话已被移除或换成新连接时不再登记, 否则会绕过断开时的任务取消.
        ensure!(
            z_state.sessions.get(&c_session.id).is_some_and(|c| Arc::ptr_eq(c, &c_session)),
            "会话已断开或已重新连接, 请重试"
        );
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
            last_progress_at: Instant::now(),
            speed_samples: VecDeque::new(),
            directory: matches!(c_spec, TaskSpec::Transfer { directory: true, .. }),
            files_done: 0,
            files_failed: 0,
            files_total: 0,
            current: String::new(),
            file_bytes: 0,
            file_total: 0,
            base_bytes: 0,
            transfer_diagnostics: None,
            transfer_retries: 0,
            last_transfer_error: None,
            via_main_link: false,
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
            let z_transfer = matches!(c_spec, TaskSpec::Transfer { .. });
            let z_slot = tokio::select! {
                biased;
                _ = &mut z_cancel_rx => { mark_task(&z_task, "cancelled", None); return; },
                l_slot = async {
                    if z_transfer {
                        Ok((Some(z_self.transfer_slots.acquire().await), None))
                    } else {
                        z_self.command_slots.acquire().await.map(|c| (None, Some(c)))
                    }
                } => match l_slot { Ok(l_slot) => l_slot, Err(_) => return },
            };
            if c_actor == Actor::Mcp
                && !z_self.mcp_permitted(&c_session.connection)
            {
                mark_task(&z_task, "failed", Some("MCP 授权已撤销".into()));
                return;
            }
            // 传输先进入准备阶段 (建立连接、初始化 SFTP、检查和打开文件), 开始收发数据时才转为进行中并开始测速.
            mark_task(&z_task, if z_transfer { "preparing" } else { "running" }, None);
            let z_run = z_self
                .run_task(&c_session, &z_task, c_spec, &mut z_cancel_rx)
                .await;
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
            drop(z_slot);
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
            z_kept += l_task.stdout.len() + l_task.stderr.len() + l_task.transfer_diagnostics.as_ref().map_or(0, |(_, c)| c.len());
            if z_kept > TASK_OUTPUT_BUDGET && !(l_task.stdout.is_empty() && l_task.stderr.is_empty() && l_task.transfer_diagnostics.is_none()) {
                l_task.stdout = "(输出已释放: 只保留最近任务的输出)".into();
                l_task.stderr = String::new();
                l_task.transfer_diagnostics = None;
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
                let z_handle = c_session.ssh_handle()?;
                let mut channel = tokio::select! {
                    biased;
                    _ = &mut *c_cancel => bail!("任务已取消"),
                    z_result = open_channel(&z_handle, 15) => ClosingChannel(Some(z_result.context("打开命令通道超时")?)),
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
                // 没有后续输入接口, 立即结束标准输入 (同 ssh -n), 读取 stdin 的命令 (wc -c 等) 不会一直等到超时.
                // EOF 与读取同时进行: 网络堵塞时 EOF 可能排队等待, 期间不读取输出会塞满通道队列, 卡住整条连接.
                let mut z_stdin = channel.make_writer();
                let mut z_eof_sent = false;
                // 结果: Ok(true) 正常结束, Ok(false) 取消; 服务器拒绝执行或结束输入失败时报错, 不等到超时.
                let result = tokio::time::timeout(Duration::from_secs(timeout), async {
                    loop {
                        tokio::select! {
                            _ = &mut *c_cancel => break Ok(false),
                            l_sent = z_stdin.shutdown(), if !z_eof_sent => {
                                z_eof_sent = true;
                                l_sent.context("结束命令输入失败")?;
                            }
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
                                Some(ChannelMsg::Failure) => bail!("服务器拒绝执行命令"),
                                Some(ChannelMsg::Close) | None => break Ok(true),
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
                    Ok(Err(err)) => {
                        channel.shutdown().await;
                        publish_command_output(c_task, &stdout, &stderr, truncated);
                        return Err(err);
                    }
                    Ok(Ok(false)) => {
                        channel.shutdown().await;
                        publish_command_output(c_task, &stdout, &stderr, truncated);
                        bail!("任务已取消");
                    }
                    Ok(Ok(true)) => {}
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
        let z_part = PartOwner::new();
        let z_committing = AtomicBool::new(false);
        // 断点: (从头连续可靠写入的字节数, 首次打开时源文件的 (长度, 修改时间)); 重试时从断点继续, 并确认源文件未变.
        let mut z_resume = (0u64, None);
        // 上传取消时沿用原通道关闭句柄、删除临时文件, 免去重新建立 SFTP 的往返.
        let mut z_remote_part = None;
        let mut z_failures = 0u32;
        let z_result = loop {
            let l_before = z_resume.0;
            let mut l_link_used = None;
            let mut l_transfer = Box::pin(self.run_transfer(c_session, c_task, upload, local.clone(), remote.clone(), overwrite, &z_part, &z_committing, &mut z_resume, &mut l_link_used, &mut z_remote_part));
            // 进入提交阶段 (改名落盘) 后取消不再中断: 提交会照常完成, 等它结束才能如实报告结果并刷新目录.
            let l_result = tokio::select! {
                biased;
                _ = &mut *c_cancel => if z_committing.load(Ordering::Acquire) { l_transfer.as_mut().await } else { Err(anyhow::anyhow!("任务已取消")) },
                r = &mut l_transfer => r,
            };
            drop(l_transfer);
            let Err(err) = l_result else { break Ok(()) };
            // 取消、提交阶段出错 (临时文件已归提交处理) 和服务器明确拒绝、本地读写等非网络错误不重试.
            if !sftp_retryable(&err) {
                break Err(err);
            }
            let err = transfer_error(err, l_link_used.as_ref());
            if z_committing.load(Ordering::Acquire) {
                break Err(err);
            }
            if !channel_open_refused(&err) {
                mark_suspect(c_session, l_link_used.as_ref());
            }
            // 本次有新进度就重新计数: 长时间传输中偶发的多次中断都能恢复, 连续失败才放弃.
            if z_resume.0 > l_before {
                z_failures = 0;
            }
            z_failures += 1;
            if z_failures > TRANSFER_RETRIES {
                break Err(err.context(format!("自动重连 {TRANSFER_RETRIES} 次仍失败")));
            }
            // 重试前释放旧通道, 兼容每连接只允许打开一个通道的服务器.
            drop(z_remote_part.take());
            if let Err(l_stop) = self.retry_wait(c_session, c_task, err, z_failures, c_cancel).await {
                break Err(l_stop);
            }
        };
        // 失败或取消时只清理确认由本任务创建的临时文件 (取消会直接丢弃传输过程, 只能在此处理); 创建仍在进行时稍等它出结果.
        // 清理尽力而为, 未确认删除 (已不存在也算删除) 时与下面结果未知的文件一并报告.
        let mut z_leftover = Vec::new();
        let z_state = if z_result.is_err() { z_part.abandon().await } else { PART_NONE };
        if z_state == PART_OWNED {
            let l_tag = z_part.tag(c_task);
            let l_removed = tokio::time::timeout(Duration::from_secs(10), async {
                if upload {
                    let l_path = part_name(&remote, &l_tag);
                    if let Some((l_raw, l_handle)) = z_remote_part.take()
                        && remove_remote_part(&l_raw, l_path.clone(), l_handle).await
                    {
                        return true;
                    }
                    match self.open_transfer_raw(c_session, c_task, None).await {
                        Ok((l_raw, _, _)) => remove_remote_part(&l_raw, l_path, None).await,
                        Err(_) => false,
                    }
                } else {
                    let l_path = local_part_path(&local, &l_tag);
                    match retry_local_file_change(|| tokio::fs::remove_file(&l_path)).await {
                        Ok(()) => true,
                        Err(err) => err.kind() == std::io::ErrorKind::NotFound,
                    }
                }
            }).await.unwrap_or(false);
            if !l_removed {
                z_leftover.push(l_tag);
            }
        }
        // 创建结果始终未能确认 (不能确认归属, 不删除) 或清理失败的临时文件可能遗留, 在任务结果里写明位置供用户核对.
        // 按交接时的快照判断, 之后状态再变 (迟到的创建自行删除) 也照样报告.
        let mut z_unknown = z_part.unknown_tags(c_task, matches!(z_state, PART_PENDING | PART_UNKNOWN));
        z_unknown.append(&mut z_leftover);
        if z_unknown.is_empty() {
            return z_result;
        }
        let z_paths = z_unknown
            .iter()
            .map(|c| if upload { part_name(&remote, c) } else { local_part_path(&local, c).display().to_string() })
            .collect::<Vec<_>>()
            .join(", ");
        let z_note = format!("临时文件创建结果未知或清理失败, 可能遗留: {z_paths}");
        if z_result.is_ok() {
            directory_note(&mut c_task.lock().unwrap_or_else(PoisonError::into_inner).stderr, z_note.clone());
        }
        z_result.map_err(|err| err.context(z_note))
    }

    // 网络错误后按次数退避等待, 再按需改用重连后的会话; 返回错误表示不再重试 (取消、换了服务器或账号、MCP 授权撤销).
    async fn retry_wait(
        &self,
        c_session: &mut Arc<LiveSession>,
        c_task: &Arc<Mutex<Task>>,
        c_err: anyhow::Error,
        c_failures: u32,
        c_cancel: &mut oneshot::Receiver<()>,
    ) -> Result<()> {
        mark_task(c_task, "retrying", Some(safe_error(&format!("{c_err:#}"))));
        // 等待逐次加长 (2、4、8、16 秒, 之后每 30 秒), 合计约 4 分钟; 等待期间可取消.
        let z_wait = Duration::from_secs((1u64 << c_failures.min(5)).min(30));
        tokio::select! {
            biased;
            _ = &mut *c_cancel => bail!("任务已取消"),
            _ = tokio::time::sleep(z_wait) => {}
        }
        // 标签已重连: 只在仍是同一服务器和登录身份时改用新会话, MCP 任务还要求新连接仍有授权.
        // 重连过程中旧会话已移除、新会话尚未登记时沿用旧会话, 本次失败后继续等待.
        let z_current = self.state.lock().unwrap_or_else(PoisonError::into_inner).sessions.get(&c_session.id).cloned();
        // 新会话的主机指纹在握手完成后才有: 尚未确认时本轮仍沿用旧会话, 下次失败后再判断.
        let z_print = |c: &LiveSession| c.host_fingerprint.lock().unwrap_or_else(PoisonError::into_inner).clone();
        if let Some(z_current) = z_current.filter(|c| !Arc::ptr_eq(c, c_session) && z_print(c).is_some()) {
            // 同一地址和账号也可能换了一台机器 (指纹不同): 续传数据来自不同服务器会拼出错误的文件, 不能续传.
            if z_print(&z_current) != z_print(c_session) {
                return Err(c_err.context("标签已重新连接, 服务器主机指纹与原连接不同, 无法续传"));
            }
            let (z_same, z_mcp) = {
                let l_task = c_task.lock().unwrap_or_else(PoisonError::into_inner);
                let (l_old, l_new) = (&l_task.connection, &z_current.connection);
                (l_old.id == l_new.id && l_old.host == l_new.host && l_old.port == l_new.port && l_old.username == l_new.username && l_old.proxy_id == l_new.proxy_id, l_task.source == "mcp")
            };
            if !z_same {
                return Err(c_err.context("标签已重新连接到其他服务器或账号, 无法续传"));
            }
            if z_mcp && !self.mcp_permitted(&z_current.connection) {
                return Err(c_err.context("MCP 授权已撤销"));
            }
            *c_session = z_current;
        }
        mark_task(c_task, "preparing", None);
        Ok(())
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
        // 扫描和建目录阶段的网络中断同样退避重试; 上传的根目录若已由本任务建好, 重试时不当作"已存在".
        let mut z_root_made = ROOT_NONE;
        let mut z_failures = 0u32;
        let (z_files, z_notes, z_failed_dirs) = loop {
            let mut l_link_used = None;
            let l_result = tokio::select! {
                biased;
                _ = &mut *c_cancel => bail!("任务已取消"),
                l_result = self.prepare_directory(c_session, c_task, c_upload, &c_local, &c_remote, c_overwrite, &mut z_root_made, &mut l_link_used) => l_result,
            };
            let err = match l_result {
                Ok(l_prepared) => break l_prepared,
                Err(err) => err,
            };
            if !sftp_retryable(&err) {
                return Err(err);
            }
            let err = transfer_error(err, l_link_used.as_ref());
            z_failures += 1;
            if z_failures > TRANSFER_RETRIES {
                return Err(err.context(format!("自动重连 {TRANSFER_RETRIES} 次仍失败")));
            }
            if !channel_open_refused(&err) {
                mark_suspect(c_session, l_link_used.as_ref());
            }
            self.retry_wait(c_session, c_task, err, z_failures, c_cancel).await?;
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
                if err.root_cause().to_string() == "任务已取消" || sftp_retryable(err) {
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
                        l_task.files_failed += 1;
                        l_task.total = l_task.total.map(|c| c.saturating_sub(z_size));
                    }
                }
                l_task.bytes = l_task.base_bytes;
            }
            // 失败的文件可能停在准备或重连状态.
            mark_task(c_task, "running", None);
        }
        if z_failed_dirs != 0 {
            bail!("{z_failed_dirs} 个目录创建失败, {z_failed} 个文件传输失败, 详见错误输出");
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
        c_root_made: &mut u8,
        c_link_used: &mut Option<SshLink>,
    ) -> Result<(Vec<(PathBuf, String, u64)>, String, usize)> {
        use russh_sftp::{client::error::Error as SftpError, protocol::FileAttributes};
        if c_upload {
            let (l_local, l_remote, l_task) = (c_local.to_path_buf(), c_remote.to_owned(), c_task.clone());
            let (z_dirs, mut z_files, mut z_notes) = tokio::task::spawn_blocking(move || scan_local_directory(&l_local, &l_remote, &l_task))
                .await
                .context("扫描本地文件夹异常")??;
            // 建完目录后若是独立连接上的会话则放回 idle_upload, 首个文件直接复用省去再开通道和初始化往返;
            // 主连接或仅允许单通道的服务器仍会按需释放或让出通道.
            let (z_raw, z_version, z_link) = self.open_transfer_raw(c_session, c_task, Some(c_link_used)).await?;
            let mut z_dirs_iter = z_dirs.into_iter();
            if let Some(z_root) = z_dirs_iter.next() {
                // 根目录的创建结果 (见 run_directory_transfer): 超时或断线时结果未知, 记下以便重试时如实提示.
                let mut z_unknown = || {
                    if *c_root_made == ROOT_NONE {
                        *c_root_made = ROOT_UNKNOWN;
                    }
                };
                // 超时保留原错误类型, 外层据此按网络错误重试.
                let l_made = match tokio::time::timeout(Duration::from_secs(15), z_raw.mkdir(z_root.clone(), FileAttributes::default())).await {
                    Ok(l_made) => l_made,
                    Err(err) => {
                        z_unknown();
                        return Err(anyhow::Error::new(err).context("新建远端目录超时"));
                    }
                };
                if let Err(err) = l_made {
                    // 超时、连接中断等非服务器明确答复的错误保留原类型返回, 由外层按网络错误重试.
                    if !sftp_definite(&err) {
                        z_unknown();
                        return Err(anyhow::Error::from(err).context("新建远端目录失败"));
                    }
                    // 未确认覆盖时根目录必须由本任务新建 (含之前中断的尝试), 不合并到已有目录.
                    // 之前的尝试创建结果未知时, 已存在的目录可能就是本任务建的, 如实提示由用户核对.
                    ensure!(
                        c_overwrite || *c_root_made == ROOT_MADE,
                        "{}: {err}",
                        if *c_root_made == ROOT_UNKNOWN {
                            "远端根目录在之前的中断中创建结果未知, 现已存在, 请核对后重试"
                        } else {
                            "远端目标已存在或无法创建"
                        }
                    );
                    let l_exists = match tokio::time::timeout(Duration::from_secs(15), z_raw.lstat(z_root)).await.context("检查远端目录超时")? {
                        Ok(l_attrs) => l_attrs.attrs.file_type().is_dir(),
                        Err(l_err) if sftp_definite(&l_err) => false,
                        Err(l_err) => return Err(anyhow::Error::from(l_err).context("检查远端目录失败")),
                    };
                    ensure!(l_exists, "新建远端目录失败: {err}");
                } else {
                    *c_root_made = ROOT_MADE;
                }
            }
            // 子目录按路径深度分层, 每层最多 16 并发创建. 父目录未就位的分支不再发请求.
            let mut z_ready = HashSet::from([c_remote.to_owned()]);
            let mut z_failed = 0;
            let mut z_layers: std::collections::BTreeMap<usize, Vec<String>> = std::collections::BTreeMap::new();
            for l_dir in z_dirs_iter {
                let l_depth = l_dir.bytes().filter(|c| *c == b'/').count();
                z_layers.entry(l_depth).or_default().push(l_dir);
            }
            for (_, l_layer) in z_layers {
                for l_chunk in l_layer.chunks(16) {
                    let mut l_jobs = tokio::task::JoinSet::new();
                    for l_dir in l_chunk {
                        if !l_dir.rsplit_once('/').is_some_and(|(c, _)| z_ready.contains(c)) {
                            continue;
                        }
                        let (l_raw, l_dir) = (z_raw.clone(), l_dir.clone());
                        l_jobs.spawn(async move {
                            let l_made = tokio::time::timeout(Duration::from_secs(15), l_raw.mkdir(l_dir.clone(), FileAttributes::default()))
                                .await
                                .map_err(|err| anyhow::Error::new(err).context("新建远端目录超时"))?;
                            let Err(err) = l_made else {
                                return Ok((l_dir, None));
                            };
                            if !sftp_definite(&err) {
                                return Err(anyhow::Error::from(err).context("新建远端目录失败"));
                            }
                            // 允许使用已有的真实目录, 避免沿符号链接写入目标树之外.
                            let l_exists = match tokio::time::timeout(Duration::from_secs(15), l_raw.lstat(l_dir.clone())).await.context("检查远端目录超时")? {
                                Ok(l_attrs) => l_attrs.attrs.file_type().is_dir(),
                                Err(l_err) if sftp_definite(&l_err) => false,
                                Err(l_err) => return Err(anyhow::Error::from(l_err).context("检查远端目录失败")),
                            };
                            let l_note = (!l_exists).then(|| format!("失败 {l_dir}: 新建目录失败: {err}"));
                            Ok((l_dir, l_note))
                        });
                    }
                    let mut l_error: Option<anyhow::Error> = None;
                    for l_done in l_jobs.join_all().await {
                        match l_done {
                            Ok((_, Some(l_note))) => {
                                z_failed += 1;
                                directory_note(&mut z_notes, l_note);
                            }
                            Ok((l_dir, None)) => { z_ready.insert(l_dir); }
                            Err(err) => { l_error.get_or_insert(err); }
                        }
                    }
                    if let Some(err) = l_error {
                        return Err(err);
                    }
                }
            }
            if let Some((l_link, l_alive)) = z_link {
                put_idle_sftp(c_session, l_link, l_alive, (z_raw, z_version));
            }
            z_files.retain(|c| c.1.rsplit_once('/').is_some_and(|(l_parent, _)| z_ready.contains(l_parent)));
            return Ok((z_files, z_notes, z_failed));
        }
        const MAX_DIRECTORY_DEPTH: usize = 64;
        let (z_raw, z_version, z_link) = self.open_transfer_raw(c_session, c_task, Some(c_link_used)).await?;
        let mut z_files = Vec::new();
        let mut z_notes = String::new();
        let mut z_dirs = Vec::new();
        let mut z_pending = vec![(c_remote.to_owned(), c_local.to_path_buf(), 0usize)];
        let mut z_clean = true;
        while let Some((l_remote, l_local, l_depth)) = z_pending.pop() {
            let (l_entries, l_closed) = match read_remote_directory(&z_raw, &l_remote).await {
                Ok(c) => c,
                // 只跳过服务器明确拒绝的子目录 (无权限等); 连接中断类状态和其他错误返回, 由外层重试, 不漏传目录.
                Err(err) if z_dirs.is_empty() || !err.chain().any(|c| c.downcast_ref::<SftpError>().is_some_and(sftp_definite)) => return Err(err),
                Err(err) => {
                    z_clean = false;
                    directory_note(&mut z_notes, format!("跳过 {l_remote}: {err:#}"));
                    continue;
                }
            };
            z_clean &= l_closed;
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
                let l_kind = l_attrs.file_type();
                if l_kind.is_symlink() {
                    directory_note(&mut z_notes, format!("跳过 {l_path}: 符号链接"));
                    continue;
                }
                // 服务器未返回类型时按普通文件处理, 传输时再由实际文件信息确认.
                if l_kind.is_other() && l_attrs.permissions.is_some() {
                    directory_note(&mut z_notes, format!("跳过 {l_path}: 特殊文件"));
                    continue;
                }
                let l_target = local_file_name(&l_name);
                if !l_seen.insert(local_name_key(&l_target)) {
                    directory_note(&mut z_notes, format!("跳过 {l_path}: 本机同目录已有同名项"));
                    continue;
                }
                ensure!(z_files.len() + z_dirs.len() + z_pending.len() < MAX_DIRECTORY_ITEMS, "文件夹超过 {MAX_DIRECTORY_ITEMS} 项, 请分批传输");
                if l_kind.is_dir() {
                    // 限制最大深度, 防止 bind mount 环路或 READDIR 跟随符号链接的服务端无限展开.
                    if l_depth >= MAX_DIRECTORY_DEPTH {
                        directory_note(&mut z_notes, format!("跳过 {l_path}: 目录层级超过 {MAX_DIRECTORY_DEPTH} 层"));
                        continue;
                    }
                    z_pending.push((l_path, l_local.join(l_target), l_depth + 1));
                } else {
                    z_files.push((l_local.join(l_target), l_path, l_attrs.size.unwrap_or(0)));
                }
            }
        }
        // 只在所有目录句柄均确认关闭后放回空闲池, 防止残留未关闭目录句柄污染后续复用.
        if z_clean && let Some((l_link, l_alive)) = z_link {
            put_idle_sftp(c_session, l_link, l_alive, (z_raw, z_version));
        } else {
            drop(z_raw);
        }
        z_files.sort_by(|c_left, c_right| c_left.1.cmp(&c_right.1));
        if c_overwrite && let Some(l_parent) = c_local.parent() {
            tokio::fs::create_dir_all(l_parent).await.context("创建本地目标上级目录失败")?;
        }
        create_local_transfer_dir(c_local, c_overwrite).await.context("创建本地文件夹失败")?;
        let mut z_ready = HashSet::from([c_local.to_owned()]);
        let mut z_failed = 0;
        for l_dir in z_dirs.into_iter().skip(1) {
            if !l_dir.parent().is_some_and(|c| z_ready.contains(c)) {
                continue;
            }
            match create_local_transfer_dir(&l_dir, true).await {
                Ok(()) => { z_ready.insert(l_dir); }
                Err(err) => {
                    z_failed += 1;
                    directory_note(&mut z_notes, format!("失败 {}: {err:#}", l_dir.display()));
                }
            }
        }
        z_files.retain(|c| c.0.parent().is_some_and(|l_parent| z_ready.contains(l_parent)));
        Ok((z_files, z_notes, z_failed))
    }

    // 传输专用 SFTP 通道; 单个请求超时放宽到 120 秒, 慢速线路上排队中的请求不会被误判超时.
    // 第二项为是否走传输专用连接: 退回主连接时调用方采用更保守的在途量, 避免拖慢终端.
    // c_link_used: 选定连接时立即记下 (见 transfer_file), 之后打开通道或初始化失败也能把这条连接记为可疑.
    async fn open_transfer_stream(&self, c_session: &Arc<LiveSession>, c_task: &Arc<Mutex<Task>>, c_link_used: Option<&mut Option<SshLink>>) -> Result<(SftpStream, Option<SshLink>)> {
        let z_used = self.transfer_handle(c_session, c_link_used.is_some().then_some(c_task)).await?;
        record_transfer_link(c_task, c_link_used, &z_used);
        let z_handle = &z_used.handle;
        let z_key = z_used.key();
        let z_link = z_used.dedicated.then(|| z_used.clone());
        match open_sftp_stream(z_handle).await {
            Ok(l_stream) => Ok((l_stream, z_link)),
            // 服务器拒绝打开新通道时, 先归还空闲通道. 只有浏览也占用这条连接时才等待目录操作.
            Err(err)
                if z_used.dedicated
                    && !z_handle.is_closed()
                    && channel_open_refused(&err)
                    && (c_session.browse_sftp.lock().unwrap_or_else(PoisonError::into_inner).as_ref().is_some_and(|c| c.1 == z_key)
                        || has_idle_on(c_session, z_key)) =>
            {
                let z_idle = has_idle_on(c_session, z_key);
                drop_idle_on(c_session, z_key);
                let z_has_browse = c_session.browse_sftp.lock().unwrap_or_else(PoisonError::into_inner).as_ref().is_some_and(|c| c.1 == z_key);
                let z_permit = if z_has_browse {
                    Some(tokio::time::timeout(Duration::from_secs(30), c_session.file_slot.lock())
                        .await
                        .with_context(|| format!("{err:#}, 等待目录浏览结束超时"))?)
                } else {
                    None
                };
                let z_browse = if z_permit.is_some() {
                    let mut l_browse = c_session.browse_sftp.lock().unwrap_or_else(PoisonError::into_inner);
                    l_browse.as_ref().is_some_and(|c| c.1 == z_key) && l_browse.take().is_some()
                } else {
                    false
                };
                if z_browse || z_idle {
                    // 等关闭通道的消息先送到服务器, 按实测 RTT 自适应 (至少 300ms, 至多 1200ms).
                    let l_wait_ms = (c_session.latency_ms.load(Ordering::Relaxed).max(0) as u64 + 250).clamp(300, 1200);
                    tokio::time::sleep(Duration::from_millis(l_wait_ms)).await;
                }
                Ok((open_sftp_stream(z_handle).await.with_context(|| format!("{err:#}; 让出目录浏览通道后重试仍失败"))?, z_link))
            }
            Err(err) => {
                if z_used.dedicated {
                    drop_idle_on(c_session, z_key);
                }
                Err(err)
            }
        }
    }

    // 打开文件选择框时提前建好独立连接: 用户选文件的这段时间连接已就绪, 建不起来也已在等待之外得出结果.
    fn warm_transfer(self: &Arc<Self>, c_args: &Value, c_actor: Actor) -> Result<Value> {
        require_user(c_actor)?;
        let z_session = self.session_args(c_args, c_actor)?;
        if z_session.status.lock().unwrap_or_else(PoisonError::into_inner).as_str() != "connected" {
            return Ok(json!({}));
        }
        let z_self = self.clone();
        tokio::spawn(async move {
            let _ = z_self.transfer_handle(&z_session, None).await;
        });
        Ok(json!({}))
    }

    // 独立 SSH 连接让传输和终端、监控各用自己的出站队列, 按需建立并在会话内复用.
    // 无法建立 (键盘交互认证、服务器限制连接数等) 时退回主连接, 60 秒内不再重试.
    // 主连接意外断开后会话仍保留传输凭据, 传输重试可单独建立独立连接继续 (见 close_session).
    async fn transfer_handle(&self, c_session: &Arc<LiveSession>, c_task: Option<&Arc<Mutex<Task>>>) -> Result<SshLink> {
        let z_main = c_session.handle.lock().unwrap_or_else(PoisonError::into_inner)
            .as_ref().filter(|c| !c.is_closed()).cloned();
        let mut z_slot = c_session.transfer.lock().await;
        if let Some(l_link) = z_slot.0.as_ref().filter(|c| !c.handle.is_closed()) {
            // 这条连接上的传输出过可重试错误, 或长时间未收到数据: 先快速探活 (5 秒超时).
            // ping 与普通消息同走出站队列, 出站卡住或静默断线时只有这样才能及时发现, 避免在开通道阶段白等 30-50 秒.
            let l_key = l_link.key();
            let l_suspect = *c_session.transfer_suspect.lock().unwrap_or_else(PoisonError::into_inner) == (l_key, true);
            if !l_suspect && !l_link.transfer_idle() {
                return Ok(l_link.clone());
            }
            if l_link.probe_transfer().await {
                c_session.transfer_suspect.lock().unwrap_or_else(PoisonError::into_inner).1 = false;
                return Ok(l_link.clone());
            }
        }
        if let Some(l_link) = z_slot.0.take() {
            retire_transfer(c_session, l_link);
        }
        // 先确认有可复用的凭据, 没有 (键盘交互认证等) 时不必先建连再失败.
        let z_auth = c_session.transfer_auth.lock().unwrap_or_else(PoisonError::into_inner).clone();
        let z_recent_failure = z_slot.1.as_ref().filter(|c| Instant::now() < c.0 && z_main.is_some());
        let z_reason = match (z_auth, z_recent_failure) {
            (None, _) => "当前认证方式不能自动建立独立连接".into(),
            (Some(_), Some((_, l_reason))) => l_reason.clone(),
            (Some(l_auth), None) => {
                // 握手失败或被取消时, 守卫关闭套接字以结束 russh 后台任务.
                let l_socket = SocketGuard(Mutex::new(None));
                let l_wait = Duration::from_secs(if z_main.is_some() { 15 } else { 30 });
                let l_state = Arc::new(SshLinkState::default());
                let l_connected = tokio::time::timeout(l_wait, self.connect_transfer(c_session, &l_socket.0, l_state.clone(), l_auth)).await;
                let (l_cooldown, l_reason) = match l_connected {
                    Ok(Ok(Some(l_handle))) => {
                        let l_registered = self.state.lock().unwrap_or_else(PoisonError::into_inner)
                            .sessions.get(&c_session.id).is_some_and(|c| Arc::ptr_eq(c, c_session));
                        // 关闭与发布持同一把锁. 未发布的套接字由守卫关闭.
                        {
                            let mut l_published = c_session.transfer_socket.lock().unwrap_or_else(PoisonError::into_inner);
                            if !l_registered || l_published.1 {
                                return Err(anyhow::Error::new(russh::Error::Disconnect).context("SSH 尚未连接"));
                            }
                            l_published.0 = l_socket.take();
                        }
                        l_state.mark_authenticated();
                        l_state.mark_pong();
                        let l_link = SshLink { handle: Arc::new(l_handle), state: l_state, dedicated: true };
                        *c_session.transfer_suspect.lock().unwrap_or_else(PoisonError::into_inner) = (l_link.key(), false);
                        z_slot.0 = Some(l_link.clone());
                        z_slot.1 = None;
                        return Ok(l_link);
                    }
                    Ok(Ok(None)) => (60, "传输连接认证失败".into()),
                    Ok(Err(err)) => (8, safe_error(&format!("{err:#}"))),
                    Err(_) => (8, "建立独立连接超时".into()),
                };
                z_slot.1 = Some((Instant::now() + Duration::from_secs(l_cooldown), l_reason.clone()));
                l_reason
            }
        };
        let z_main = z_main.filter(|c| !c.is_closed()).ok_or(russh::Error::Disconnect)
            .with_context(|| format!("SSH 尚未连接 ({z_reason})"))?;
        // 回退记在任务详情和摘要中, 便于界面直接提示传输变慢的原因.
        if let Some(Ok(mut l_task)) = c_task.map(|c| c.lock()) {
            l_task.via_main_link = true;
            l_task.stdout = format!("经主连接传输 ({z_reason}), 传输期间终端和监控可能变慢");
        }
        Ok(SshLink { handle: z_main, state: c_session.main_link.clone(), dedicated: false })
    }

    // 取会话复用的浏览 SFTP 会话, 没有时新建 (首次加载失败或会话失效后, 刷新即在此重连文件部分). 调用方持有 file_slot, 不会并发重复创建.
    // 先在主连接上开; 主连接开不了新通道 (服务器会话名额已满、线路繁忙不回应) 时改用独立连接, 终端与监控不受影响.
    async fn browse_sftp(&self, c_session: &Arc<LiveSession>, c_handle: &Arc<client::Handle<SshHandler>>) -> Result<Arc<russh_sftp::client::RawSftpSession>> {
        {
            // 与 retire_transfer、下方写回保持同一加锁顺序 (先 transfer_suspect 后 browse_sftp), 避免反转死锁.
            let l_suspect = *c_session.transfer_suspect.lock().unwrap_or_else(PoisonError::into_inner);
            let mut l_slot = c_session.browse_sftp.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some((l_sftp, l_link, l_alive)) = l_slot.as_ref() {
                let l_link_ok = *l_link == 0 || l_suspect == (*l_link, false);
                if l_alive.load(Ordering::Acquire) && l_link_ok {
                    return Ok(l_sftp.clone());
                }
                l_slot.take();
            }
        }
        // 第二项: 改用的独立连接 (对象地址), 主连接为 0.
        let (z_stream, z_link) = match open_sftp_stream(c_handle).await {
            Ok(l_stream) => (l_stream, 0),
            Err(err) => {
                let l_link = self.transfer_handle(c_session, None).await?;
                if !l_link.dedicated {
                    return Err(err);
                }
                // 空闲的传输会话可能占着服务器允许的通道.
                clear_idle_sftp(c_session);
                (open_sftp_stream(&l_link.handle).await.with_context(|| format!("主连接{err:#}, 改用独立连接也失败"))?, l_link.key())
            }
        };
        let z_alive = z_stream.alive();
        let z_sftp = russh_sftp::client::RawSftpSession::new(z_stream);
        z_sftp.set_timeout(10);
        tokio::time::timeout(Duration::from_secs(10), z_sftp.init())
            .await
            .context("初始化SFTP超时")??;
        let z_sftp = Arc::new(z_sftp);
        // 独立连接上的会话只在它仍是当前连接且未可疑时缓存: 与退役 (retire_transfer) 同持可疑标记锁, 已退役或可疑的连接不会再被缓存引用.
        // 不缓存时本次照常使用, 用完随引用释放.
        let z_suspect = c_session.transfer_suspect.lock().unwrap_or_else(PoisonError::into_inner);
        if z_link == 0 || *z_suspect == (z_link, false) {
            *c_session.browse_sftp.lock().unwrap_or_else(PoisonError::into_inner) = Some((z_sftp.clone(), z_link, z_alive));
        }
        drop(z_suspect);
        Ok(z_sftp)
    }

    // c_socket: 握手开始前存入套接字副本 (见 ssh_connect), 调用方超时放弃时据此关闭.
    async fn connect_transfer(&self, c_session: &Arc<LiveSession>, c_socket: &Mutex<Option<SshSocket>>, c_link: Arc<SshLinkState>, c_auth: TransferAuth) -> Result<Option<client::Handle<SshHandler>>> {
        let z_handler = SshHandler { session: Arc::downgrade(c_session), store: self.store.clone(), link: c_link, transfer: true };
        let z_user = &c_session.connection.username;
        let z_proxy = c_session.proxy.lock().unwrap_or_else(PoisonError::into_inner).clone();
        // 关闭会话时代理配置已清空: 不能当成直连, 否则会暴露本机地址.
        ensure!(z_proxy.is_some() || c_session.connection.proxy_id.is_none(), "会话已关闭, 不建立传输连接");
        let mut z_handle = ssh_connect(&c_session.connection, z_proxy.as_deref(), z_handler, c_socket).await?;
        let z_success = match c_auth {
            TransferAuth::Key(l_key) => authenticate_key(&mut z_handle, z_user, l_key, 10, 20).await?,
            TransferAuth::Password(l_password) => authenticate_password(&mut z_handle, z_user, l_password.as_str(), 20).await?,
        };
        Ok(z_success.then_some(z_handle))
    }

    async fn open_transfer_raw(&self, c_session: &Arc<LiveSession>, c_task: &Arc<Mutex<Task>>, c_link_used: Option<&mut Option<SshLink>>) -> Result<(Arc<russh_sftp::client::RawSftpSession>, russh_sftp::protocol::Version, Option<(SshLink, Arc<AtomicBool>)>)> {
        if let Some((l_link, l_alive, (l_raw, l_version))) = take_idle_sftp(c_session).await {
            record_transfer_link(c_task, c_link_used, &l_link);
            return Ok((l_raw, l_version, Some((l_link, l_alive))));
        }
        let (z_stream, z_link) = self.open_transfer_stream(c_session, c_task, c_link_used).await?;
        let z_alive = z_stream.alive();
        // 单请求超时放宽到 180 秒, 配合自适应滑窗, 慢速线路上正常排队的读写请求不会触发误超时.
        let z_raw = russh_sftp::client::RawSftpSession::new_with_config(
            z_stream,
            russh_sftp::client::Config { request_timeout_secs: 180, ..russh_sftp::client::Config::default() },
        );
        let z_version = tokio::time::timeout(Duration::from_secs(12), z_raw.init())
            .await
            .context("建立SFTP会话超时")??;
        Ok((Arc::new(z_raw), z_version, z_link.map(|c| (c, z_alive))))
    }

    // 传输先写同目录、带任务标识的临时文件 (排他创建), 完成后改为目标名: 中途失败或取消时目标保持原样.
    // c_part 记录临时文件的归属 (见 PartOwner), 调用方据此只清理确认属于本任务的文件; c_committing 表示已进入提交阶段.
    // c_resume 为断点 (见 run_task): 临时文件已存在时重新打开它, 从断点继续.
    async fn run_transfer(
        &self,
        c_session: &Arc<LiveSession>,
        c_task: &Arc<Mutex<Task>>,
        c_upload: bool,
        c_local: PathBuf,
        c_remote: String,
        c_overwrite: bool,
        c_part: &Arc<PartOwner>,
        c_committing: &AtomicBool,
        c_resume: &mut (u64, Option<(u64, u64)>),
        c_link_used: &mut Option<SshLink>,
        c_remote_part: &mut Option<(Arc<RawSftpSession>, Option<String>)>,
    ) -> Result<()> {
        c_part.settle();
        let z_tag = c_part.tag(c_task);
        if c_upload {
            use russh_sftp::protocol::{FileAttributes, OpenFlags};
            let ((mut source, z_source), (z_raw, z_version, z_link)) = tokio::try_join!(
                async {
                    ensure!(tokio::fs::metadata(&c_local).await?.is_file(), "上传源必须是普通文件");
                    let l_file = tokio::fs::File::open(&c_local).await?;
                    let l_metadata = l_file.metadata().await?;
                    ensure!(l_metadata.is_file(), "上传源必须是普通文件");
                    Ok((l_file, l_metadata))
                },
                self.open_transfer_raw(c_session, c_task, Some(c_link_used)),
            )?;
            let z_remote_part = c_remote_part.insert((z_raw.clone(), None));
            let total = z_source.len();
            let z_mtime = z_source.modified().ok().and_then(|c| c.duration_since(UNIX_EPOCH).ok()).map(|c| c.as_nanos() as u64);
            let mut z_start = resume_offset(c_resume, c_part.owned(), total, z_mtime)?;
            let z_check_resume = z_start > 0;
            let z_part = part_name(&c_remote, &z_tag);
            // 续传: 按读写方式打开 (不隐式创建), 之后全部按这个句柄操作, 不再按路径: 路径期间被替换也不会动到别的文件.
            // 先按句柄核对长度, 再比对断点前一段 (最多 32 KiB) 与本地源文件是否一致; 长度不足、无法确认或内容不符时,
            // 按句柄截断后从头重传. 已不存在时重新排他创建.
            let mut z_resumed = None;
            if z_start > 0 {
                match tokio::time::timeout(Duration::from_secs(15), z_raw.open(z_part.clone(), OpenFlags::READ | OpenFlags::WRITE, FileAttributes::default())).await.context("打开远端临时文件超时")? {
                    Ok(l_opened) => {
                        z_remote_part.1 = Some(l_opened.handle.clone());
                        let l_size = tokio::time::timeout(Duration::from_secs(15), z_raw.fstat(l_opened.handle.clone()))
                            .await
                            .context("检查远端临时文件超时")??
                            .attrs
                            .size;
                        let l_tail = z_start.min(32 * 1024);
                        let l_same = l_size.is_some_and(|c| c >= z_start) && {
                            let mut l_local = vec![0u8; l_tail as usize];
                            source.seek(SeekFrom::Start(z_start - l_tail)).await.context("定位本地文件续传位置失败")?;
                            source.read_exact(&mut l_local).await.context("读取本地文件失败")?;
                            let l_remote = read_remote_exact(&z_raw, &l_opened.handle, z_start - l_tail, l_tail as usize, Duration::from_secs(60)).await?;
                            l_remote == l_local
                        };
                        if !l_same {
                            z_start = 0;
                            c_resume.0 = 0;
                            let l_attrs = FileAttributes { size: Some(0), ..FileAttributes::default() };
                            tokio::time::timeout(Duration::from_secs(15), z_raw.fsetstat(l_opened.handle.clone(), l_attrs))
                                .await
                                .context("截断远端临时文件超时")??;
                        }
                        z_resumed = Some(l_opened.handle);
                    }
                    Err(russh_sftp::client::error::Error::Status(l_status))
                        if l_status.status_code == russh_sftp::protocol::StatusCode::NoSuchFile =>
                    {
                        c_part.state.store(PART_NONE, Ordering::Release);
                        z_start = 0;
                        c_resume.0 = 0;
                    }
                    Err(err) => return Err(anyhow::Error::from(err).context("打开远端临时文件失败")),
                }
            }
            // 已确认由本任务创建而需从头重传: 截断重写. 否则排他创建, 在独立任务里发出并由它登记结果 (见 PartOwner):
            // 调用方此时被取消, 结果也会落到归属记录里; 服务器明确拒绝 (如同名文件已存在) 记为未创建, 超时或断线记为结果未知,
            // 两者都不会被清理或截断.
            let z_open = async {
                if let Some(l_handle) = z_resumed {
                    return anyhow::Ok(l_handle);
                }
                let l_opened = if c_part.owned() {
                    tokio::time::timeout(Duration::from_secs(15), z_raw.open(z_part.clone(), OpenFlags::CREATE | OpenFlags::WRITE | OpenFlags::TRUNCATE, FileAttributes::default()))
                        .await
                        .context("打开远端临时文件超时")??
                } else {
                    let l_gen = c_part.round.load(Ordering::Acquire);
                    c_part.state.store(PART_PENDING, Ordering::Release);
                    let (l_raw, l_path, l_owner) = (z_raw.clone(), z_part.clone(), c_part.clone());
                    let l_created = tokio::spawn(async move {
                        let l_result = tokio::time::timeout(
                            Duration::from_secs(15),
                            l_raw.open(l_path.clone(), OpenFlags::CREATE | OpenFlags::WRITE | OpenFlags::EXCLUDE, FileAttributes::default()),
                        )
                        .await;
                        let l_cleanup = l_owner.record(l_gen, match &l_result {
                            Ok(Ok(_)) => PART_OWNED,
                            Ok(Err(err)) if sftp_definite(err) => PART_NONE,
                            _ => PART_UNKNOWN,
                        });
                        // 迟到的创建确认由创建任务清理, 对应文件名在外层按可能遗留报告.
                        if l_cleanup && let Ok(Ok(l_opened)) = &l_result {
                            let _ = tokio::time::timeout(Duration::from_secs(10), remove_remote_part(&l_raw, l_path, Some(l_opened.handle.clone()))).await;
                        }
                        l_result
                    });
                    l_created.await.context("创建远端临时文件任务异常")?.context("创建远端临时文件超时")??
                };
                z_remote_part.1 = Some(l_opened.handle.clone());
                Ok(l_opened.handle)
            };
            // 检查目标与打开临时文件同时发出, 省一次往返. 临时文件不影响目标; 下面报错时由调用方按归属清理.
            // 用 lstat 而非 stat: 识别悬空符号链接并避免沿符号链接覆写, 同时禁止把已有远端目录当作文件覆写.
            let (z_stat, z_opened) = tokio::join!(
                tokio::time::timeout(Duration::from_secs(15), z_raw.lstat(c_remote.clone())),
                z_open,
            );
            // 只有"不存在"算正常结果; 超时、权限和连接错误直接报错.
            let z_existing = match z_stat.context("检查远端目标超时")? {
                Ok(l_attrs) => Some(l_attrs),
                Err(russh_sftp::client::error::Error::Status(l_status))
                    if l_status.status_code == russh_sftp::protocol::StatusCode::NoSuchFile => None,
                Err(err) => return Err(anyhow::Error::from(err).context("检查远端目标失败")),
            };
            let z_exists = z_existing.is_some();
            ensure!(c_overwrite || !z_exists, "远端目标已存在");
            if let Some(l_existing) = &z_existing {
                let l_kind = l_existing.attrs.file_type();
                ensure!(!l_kind.is_dir(), "远端目标已是目录, 无法作为文件覆写");
                ensure!(!l_kind.is_symlink(), "远端目标是符号链接, 无法覆写");
            }
            let z_handle = z_opened?;
            // 仅续传需要定位, 首次打开的本地句柄已经在文件开头.
            if z_check_resume {
                source.seek(SeekFrom::Start(z_start)).await.context("定位本地文件续传位置失败")?;
            }
            mark_task(c_task, "running", None);
            update_progress(c_task, z_start, total, false);
            let z_written = async {
                let (z_sem, z_max_window) = if z_link.is_some() {
                    (&c_session.upload_inflight, UPLOAD_WINDOW)
                } else {
                    (&c_session.upload_main_inflight, UPLOAD_MAIN_WINDOW)
                };
                // 起始窗口快速爬升, 慢速线路按实测吞吐限制在途量 (约 6 秒排队), 高速线路 1-2 个 RTT 即可拉满上限.
                let mut z_window = TRANSFER_INITIAL_WINDOW.min(z_max_window);
                let mut z_offset = z_start;
                let mut z_acked = z_start;
                // 只保留未确认块的偏移. 最早的未确认块是续传断点, 较晚的确认也能立即腾出发送名额.
                let mut z_unconfirmed = BTreeSet::new();
                // 只测本次实际发送的数据. 续传的已有字节不计入进度用时.
                let z_data_start = Instant::now();
                // 近 2 秒滚动速率用于滑窗收放, 避免大文件前半段高速拉高全程均值后遇限速无法及时收缩窗口.
                let mut z_rate_at = z_data_start;
                let mut z_rate_bytes = z_start;
                let mut z_recent_rate: Option<f64> = None;
                let z_send_size = total - z_start;
                let z_checkpoints = [z_send_size.div_ceil(20), z_send_size.div_ceil(4), z_send_size.div_ceil(2), z_send_size];
                let mut z_first_send = None;
                let mut z_first_ack = None;
                let mut z_last_ack = None;
                let mut z_longest_ack_gap = (Duration::ZERO, 0, 0);
                let mut z_confirmed_times = [None; 4];
                let z_socket_writes = upload_socket_writes(c_session, z_link.as_ref().map(|(c, _)| &c.handle));
                let z_socket_start = z_socket_writes.as_ref().map(|c| c.lock().unwrap_or_else(PoisonError::into_inner).totals());
                let mut z_socket_samples = [None; 2];
                let (z_chunks_tx, mut z_chunks_rx) = mpsc::channel::<Result<Vec<u8>>>(32);
                let mut z_reader_set = tokio::task::JoinSet::new();
                z_reader_set.spawn(async move {
                    if let Err(err) = prefetch_upload(source, total - z_start, &z_chunks_tx).await {
                        let _ = z_chunks_tx.send(Err(err)).await;
                    }
                });
                let mut z_writes = FuturesUnordered::new();
                let mut z_reading = true;
                let z_ack_timeout = tokio::time::sleep(TRANSFER_STALL);
                tokio::pin!(z_ack_timeout);
                while z_reading || !z_writes.is_empty() {
                    // 等共享名额时继续收确认, 保留同一次 acquire 的排队位置, 避免其他文件不断抢回刚释放的名额.
                    let l_next = async {
                        let Some(l_chunk) = z_chunks_rx.recv().await else { return anyhow::Ok(None); };
                        let l_chunk = l_chunk?;
                        let l_permit = z_sem.clone().acquire_owned().await?;
                        Ok(Some((l_chunk, l_permit)))
                    };
                    tokio::pin!(l_next);
                    let l_request = loop {
                        if !z_reading && z_writes.is_empty() {
                            break None;
                        }
                        let l_target = transfer_target_window(z_window, z_max_window, z_acked - z_start, z_data_start.elapsed(), z_recent_rate, UPLOAD_CHUNK, 6.0);
                        let l_can_queue = z_reading && z_writes.len() < l_target;
                        tokio::select! {
                            biased;
                            Some(l_first) = z_writes.next(), if !z_writes.is_empty() => {
                                let l_previous = z_acked;
                                let l_inflight = z_writes.len() + 1;
                                let mut l_acked_blocks = 0usize;
                                let l_recorded = (|| {
                                    let mut l_done = Some(l_first);
                                    while let Some(l_result) = l_done {
                                        let (l_offset, l_len) = l_result?;
                                        z_acked += l_len;
                                        l_acked_blocks += 1;
                                        z_unconfirmed.remove(&l_offset);
                                        l_done = z_writes.next().now_or_never().flatten();
                                    }
                                    anyhow::Ok(())
                                })();
                                let l_contiguous = z_unconfirmed.first().copied().unwrap_or(z_offset);
                                c_resume.0 = l_contiguous;
                                update_progress(c_task, z_acked, total, true);
                                if z_acked > l_previous {
                                    let l_rate_dt = z_rate_at.elapsed();
                                    if l_rate_dt >= Duration::from_secs(2) {
                                        z_recent_rate = Some((z_acked - z_rate_bytes) as f64 / l_rate_dt.as_secs_f64());
                                        z_rate_at = Instant::now();
                                        z_rate_bytes = z_acked;
                                    }
                                    z_window = z_window.saturating_add(l_acked_blocks.saturating_mul(2)).min(z_max_window);
                                    z_ack_timeout.as_mut().reset(tokio::time::Instant::now() + TRANSFER_STALL);
                                    let l_elapsed = z_data_start.elapsed();
                                    z_first_ack.get_or_insert(l_elapsed);
                                    if let Some(l_last) = z_last_ack.replace(l_elapsed) {
                                        let l_gap = l_elapsed - l_last;
                                        if l_gap > z_longest_ack_gap.0 {
                                            z_longest_ack_gap = (l_gap, l_previous, l_inflight);
                                        }
                                    }
                                    for (i, (l_time, l_target)) in z_confirmed_times.iter_mut().zip(z_checkpoints).enumerate() {
                                        if l_time.is_none() && z_acked - z_start >= l_target {
                                            *l_time = Some(l_elapsed);
                                            if let (Some(l_sample), Some(l_writes)) = (z_socket_samples.get_mut(i), &z_socket_writes) {
                                                let (l_bytes, l_wait) = l_writes.lock().unwrap_or_else(PoisonError::into_inner).totals();
                                                *l_sample = Some((z_offset - z_start, l_bytes, l_wait));
                                            }
                                        }
                                    }
                                }
                                l_recorded?;
                            }
                            _ = &mut z_ack_timeout, if !z_writes.is_empty() => {
                                // SSH 仍可能回应保活, 但 SFTP 不再确认写入. 从最后一个连续确认的断点重试.
                                return Err(std::io::Error::new(std::io::ErrorKind::ConnectionAborted, "上传 60 秒未收到写入确认").into());
                            }
                            l_next = &mut l_next, if l_can_queue => break l_next?,
                        }
                    };
                    let Some((l_chunk, l_permit)) = l_request else {
                        z_reading = false;
                        continue;
                    };
                    let l_offset = z_offset;
                    let l_len = l_chunk.len() as u64;
                    if z_writes.is_empty() {
                        z_ack_timeout.as_mut().reset(tokio::time::Instant::now() + TRANSFER_STALL);
                    }
                    z_offset += l_len;
                    z_unconfirmed.insert(l_offset);
                    let (l_raw, l_handle) = (z_raw.clone(), z_handle.clone());
                    let mut l_write = Box::pin(async move {
                        let _l_permit = l_permit;
                        l_raw.write(l_handle, l_offset, l_chunk).await.context("写入远端文件失败")?;
                        anyhow::Ok((l_offset, l_len))
                    });
                    // RawSftpSession::write 在第一次轮询时入队. 按偏移顺序入队, 之后并发等待各自的确认.
                    z_first_send.get_or_insert_with(|| z_data_start.elapsed());
                    let l_ready = l_write.as_mut().now_or_never();
                    z_writes.push(async move {
                        match l_ready {
                            Some(l_result) => l_result,
                            None => l_write.await,
                        }
                    });
                }
                if let Some(l_exit) = z_reader_set.join_next().await {
                    l_exit.context("读取本地文件任务异常")?;
                }
                ensure!(z_acked == total, "上传数据不完整");
                if let (Some(l_sent), Some(l_ack), [Some(l_early), Some(l_quarter), Some(l_half), Some(l_end)]) =
                    (z_first_send, z_first_ack, z_confirmed_times)
                    && let Ok(mut t) = c_task.lock()
                    && (!t.directory || t.transfer_diagnostics.as_ref().is_none_or(|(l_duration, _)| l_end > *l_duration))
                {
                    use std::fmt::Write as _;
                    let mut l_report = String::new();
                    if t.directory {
                        l_report.push_str("文件夹内传输用时最长的文件\n");
                    }
                    let _ = writeln!(l_report, "本次上传 {z_send_size} 字节");
                    if z_start > 0 {
                        let _ = writeln!(l_report, "续传起点 {z_start} 字节 · 进度按本次上传计算");
                    }
                    let _ = write!(
                        l_report,
                        "上传用时(秒)  首块入队 {:.2} · 首块确认 {:.2} · 5% {:.2} · 25% {:.2} · 50% {:.2} · 100% {:.2}",
                        l_sent.as_secs_f64(), l_ack.as_secs_f64(), l_early.as_secs_f64(), l_quarter.as_secs_f64(),
                        l_half.as_secs_f64(), l_end.as_secs_f64(),
                    );
                    let (l_gap, l_bytes, l_inflight) = z_longest_ack_gap;
                    if !l_gap.is_zero() {
                        let _ = write!(
                            l_report,
                            "\n最长确认间隔 {:.2} 秒 · 起始进度 {:.1}% · 当时待确认 {l_inflight}/{z_max_window} 块",
                            l_gap.as_secs_f64(),
                            (l_bytes - z_start) as f64 * 100.0 / z_send_size as f64,
                        );
                    }
                    if let Some((l_start_bytes, l_start_wait)) = z_socket_start {
                        for (l_percent, l_sample) in [5, 25].into_iter().zip(z_socket_samples) {
                            if let Some((l_queued, l_bytes, l_wait)) = l_sample {
                                let _ = write!(
                                    l_report,
                                    "\n{l_percent}% 累计 · SFTP 入队 {l_queued} B · 连接写入 TCP {} B · TCP 写入耗时 {:.2} 秒",
                                    l_bytes.saturating_sub(l_start_bytes),
                                    l_wait.saturating_sub(l_start_wait).as_secs_f64(),
                                );
                            }
                        }
                    }
                    t.transfer_diagnostics = Some((l_end, l_report));
                }
                Ok::<_, anyhow::Error>(())
            }
            .await;
            // 写入已失败时仅短暂尝试关闭句柄 (3 秒), 避免断线或拥塞时再白等 60 秒才进入重试.
            let z_closed = tokio::time::timeout(
                Duration::from_secs(if z_written.is_ok() { 60 } else { 3 }),
                z_raw.close(z_handle),
            )
            .await;
            if matches!(z_closed, Ok(Ok(_))) {
                z_remote_part.1 = None;
            }
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
            // 提交阶段 (保留权限、改名) 放进独立任务, 取消时等待实际结果, 避免留下旧文件已改名、新文件未就位的中间状态.
            // 临时文件此后归提交任务负责, 外层不再清理.
            c_part.state.store(PART_NONE, Ordering::Release);
            c_committing.store(true, Ordering::Release);
            let z_posix_rename = z_version.extensions.get("posix-rename@openssh.com").is_some_and(|c| c == "1");
            let z_permissions = z_existing.and_then(|c| c.attrs.permissions);
            let z_commit = tokio::spawn(commit_upload(z_raw.clone(), z_part, c_remote, c_overwrite, z_exists, z_posix_rename, z_permissions));
            z_commit.await.context("提交上传任务异常")??;
            // 只放回成功结束的会话: 失败或取消时可能还有未完成的请求.
            if let Some((l_link, l_alive)) = z_link {
                put_idle_sftp(c_session, l_link, l_alive, (z_raw, z_version));
            }
        } else {
            use russh_sftp::protocol::{FileAttributes, OpenFlags, StatusCode};
            // 用 symlink_metadata 识别悬空符号链接与 Windows 目录符号链接 (mklink /D): 不覆盖时提前报错, 覆盖时拒绝目录与目录符号链接.
            if let Ok(z_metadata) = tokio::fs::symlink_metadata(&c_local).await {
                ensure!(c_overwrite, "本地目标已存在");
                ensure!(!z_metadata.is_dir() && (z_metadata.is_file() || z_metadata.file_type().is_symlink()), "本地目标必须是普通文件");
            }
            // 下载改用 RawSftpSession 精确滑窗: 按文件总长派发读请求, 不越过 EOF 多发预读,
            // 遇短读仅补缺口而不清空后续并发请求, 完成后显式 close 句柄再放回统一空闲池.
            let (z_raw, z_version, z_link) = self.open_transfer_raw(c_session, c_task, Some(c_link_used)).await?;
            // 先打开再按句柄取信息: 按路径分别查询时, 路径恰好被替换会让长度检查与实际读取对应两个文件.
            let z_handle = tokio::time::timeout(
                Duration::from_secs(15),
                z_raw.open(c_remote, OpenFlags::READ, FileAttributes::default()),
            )
            .await
            .context("打开远端源文件超时")??
            .handle;
            let z_metadata = tokio::time::timeout(Duration::from_secs(15), z_raw.fstat(z_handle.clone()))
                .await
                .context("读取远端文件信息超时")??
                .attrs;
            ensure!(z_metadata.file_type().is_file(), "下载源必须是普通文件");
            let total = z_metadata.size.context("服务器未返回文件长度")?;
            let mut z_start = resume_offset(c_resume, c_part.owned(), total, z_metadata.mtime.map(u64::from))?;
            let z_part = local_part_path(&c_local, &z_tag);
            // 续传: 打开本任务已创建的临时文件 (不隐式创建), 按句柄核对长度; 被截短时从头下载, 否则截掉断点之后
            // 可能残留的半截数据, 再从断点追加. 已被删除时重新创建.
            let mut z_dest = None;
            if c_part.owned() {
                match tokio::fs::OpenOptions::new().read(true).write(true).open(&z_part).await {
                    Ok(mut l_dest) => {
                        // 源文件只按长度和秒级修改时间判断未变, 同一秒内改写且长度相同时分辨不出: 再比对断点前一段 (最多 32 KiB)
                        // 本地已写内容与远端当前内容, 不一致就从头下载, 不拼接新旧内容.
                        let l_tail = z_start.min(32 * 1024);
                        let l_same = z_start == 0 || l_dest.metadata().await.context("读取本地临时文件失败")?.len() >= z_start && {
                            let mut l_local = vec![0u8; l_tail as usize];
                            l_dest.seek(SeekFrom::Start(z_start - l_tail)).await.context("定位本地临时文件失败")?;
                            l_dest.read_exact(&mut l_local).await.context("读取本地临时文件失败")?;
                            let l_remote = read_remote_exact(&z_raw, &z_handle, z_start - l_tail, l_tail as usize, Duration::from_secs(60)).await?;
                            l_local == l_remote
                        };
                        if !l_same {
                            z_start = 0;
                            c_resume.0 = 0;
                        }
                        l_dest.set_len(z_start).await.context("截断本地临时文件失败")?;
                        l_dest.seek(SeekFrom::Start(z_start)).await.context("定位本地临时文件失败")?;
                        z_dest = Some(l_dest);
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                        c_part.state.store(PART_NONE, Ordering::Release);
                        z_start = 0;
                        c_resume.0 = 0;
                    }
                    Err(err) => return Err(anyhow::Error::from(err).context("打开本地临时文件失败")),
                }
            }
            let dest = match z_dest {
                Some(l_dest) => l_dest,
                // 排他创建放到阻塞线程并由它登记结果 (见 PartOwner): 目标在网络盘或卡住的挂载点上时不占用异步线程,
                // 调用方被取消时结果也会落到归属记录里. 同名文件已存在时报错且不记为本任务所有, 不会被清理.
                None => {
                    let l_gen = c_part.round.load(Ordering::Acquire);
                    c_part.state.store(PART_PENDING, Ordering::Release);
                    let (l_path, l_owner) = (z_part.clone(), c_part.clone());
                    let l_file = tokio::task::spawn_blocking(move || {
                        let l_result = std::fs::OpenOptions::new().write(true).create_new(true).open(&l_path);
                        // 建成时任务已结束或已换名: 没人再用这个文件, 自行删除.
                        if l_owner.record(l_gen, if l_result.is_ok() { PART_OWNED } else { PART_NONE }) {
                            drop(l_result);
                            let _ = std::fs::remove_file(&l_path);
                            return Err(std::io::ErrorKind::Interrupted.into());
                        }
                        l_result
                    })
                    .await
                    .context("创建本地临时文件任务异常")?
                    .context("创建本地临时文件失败")?;
                    tokio::fs::File::from_std(l_file)
                }
            };
            // 合并网络分块, 减少提交给本地文件的后台写入次数.
            let mut dest = tokio::io::BufWriter::with_capacity(256 * 1024, dest);
            mark_task(c_task, "running", None);
            update_progress(c_task, z_start, total, false);
            let mut count = z_start;
            let z_data_start = Instant::now();
            let mut z_rate_at = z_data_start;
            let mut z_rate_bytes = z_start;
            let mut z_recent_rate: Option<f64> = None;
            let mut z_last_data_at = z_data_start;
            let mut z_read_elapsed = Duration::ZERO;
            let mut z_write_elapsed = Duration::ZERO;
            let z_max_window = if z_link.is_some() { DOWNLOAD_WINDOW } else { DOWNLOAD_MAIN_WINDOW };
            let mut z_window = TRANSFER_INITIAL_WINDOW.min(z_max_window);
            let mut z_chunk_size = DOWNLOAD_CHUNK;
            let mut z_next_offset = z_start;
            // 按偏移顺序保存已派发的读请求: 队头按序落盘, 后台 SFTP 读任务独立消费后续回包, 无需乱序大缓存.
            let mut z_pending: VecDeque<(u64, u32, std::pin::Pin<Box<dyn std::future::Future<Output = std::result::Result<russh_sftp::protocol::Data, russh_sftp::client::error::Error>> + Send>>)> = VecDeque::with_capacity(z_max_window);
            let z_read_result: Result<()> = loop {
                let l_target = transfer_target_window(z_window, z_max_window, count - z_start, z_data_start.elapsed(), z_recent_rate, z_chunk_size, 8.0);
                while z_pending.len() < l_target && z_next_offset < total {
                    let l_offset = z_next_offset;
                    let l_len = (total - l_offset).min(z_chunk_size as u64) as u32;
                    z_next_offset += l_len as u64;
                    let (l_raw, l_handle) = (z_raw.clone(), z_handle.clone());
                    let mut l_fut = Box::pin(async move { l_raw.read(l_handle, l_offset, l_len).await });
                    // 首次轮询立即把 SSH_FXP_READ 送入出站队列, 实现多块并发在途.
                    let l_ready = l_fut.as_mut().now_or_never();
                    let l_fut: std::pin::Pin<Box<dyn std::future::Future<Output = std::result::Result<russh_sftp::protocol::Data, russh_sftp::client::error::Error>> + Send>> = match l_ready {
                        Some(l_result) => Box::pin(std::future::ready(l_result)),
                        None => l_fut,
                    };
                    z_pending.push_back((l_offset, l_len, l_fut));
                }
                let Some((l_offset, l_expected, l_fut)) = z_pending.pop_front() else {
                    break Ok(());
                };
                let l_read_at = Instant::now();
                let l_read = tokio::time::timeout(TRANSFER_STALL.saturating_sub(z_last_data_at.elapsed()), l_fut).await;
                z_read_elapsed += l_read_at.elapsed();
                let l_data = match l_read {
                    Ok(Ok(l_packet)) => l_packet.data,
                    Ok(Err(russh_sftp::client::error::Error::Status(l_status)))
                        if l_status.status_code == StatusCode::Eof => break Ok(()),
                    Ok(Err(err)) => break Err(anyhow::Error::from(err).context("读取远端文件失败")),
                    Err(err) => break Err(anyhow::Error::new(err).context("读取远端文件超时")),
                };
                if l_data.is_empty() {
                    break Ok(());
                }
                // 防止异常或恶意服务端返回超过请求长度的数据导致后续并发块偏移错位.
                ensure!(l_data.len() <= l_expected as usize, "服务器返回的数据块长度超出请求范围");
                // 本机磁盘超时按 TimedOut 报出, 重试判断据此与网络超时区分, 不进入网络退避.
                let l_write_at = Instant::now();
                tokio::time::timeout(TRANSFER_STALL, dest.write_all(&l_data))
                    .await
                    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "写入本地文件超时"))??;
                z_write_elapsed += l_write_at.elapsed();
                let mut l_got = l_data.len();
                // 服务端单次返回少于请求长度 (短读): 仅就地补读本块缺口并下调后续块长, 不丢弃已在途的并发请求.
                if l_got < l_expected as usize {
                    z_chunk_size = l_got.clamp(4096, DOWNLOAD_CHUNK);
                    let l_gap = l_expected as usize - l_got;
                    let l_gap_at = Instant::now();
                    let l_rest = match read_remote_exact(
                        &z_raw,
                        &z_handle,
                        l_offset + l_got as u64,
                        l_gap,
                        TRANSFER_STALL.saturating_sub(z_last_data_at.elapsed()).max(Duration::from_secs(10)),
                    ).await {
                        Ok(l_bytes) => l_bytes,
                        Err(err) => break Err(err),
                    };
                    z_read_elapsed += l_gap_at.elapsed();
                    let l_gap_write_at = Instant::now();
                    tokio::time::timeout(TRANSFER_STALL, dest.write_all(&l_rest))
                        .await
                        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "写入本地文件超时"))??;
                    z_write_elapsed += l_gap_write_at.elapsed();
                    l_got += l_rest.len();
                    if l_got < l_expected as usize {
                        count += l_got as u64;
                        break Ok(());
                    }
                }
                count += l_got as u64;
                let l_rate_dt = z_rate_at.elapsed();
                if l_rate_dt >= Duration::from_secs(2) {
                    z_recent_rate = Some((count - z_rate_bytes) as f64 / l_rate_dt.as_secs_f64());
                    z_rate_at = Instant::now();
                    z_rate_bytes = count;
                }
                // 本地落盘另有超时, 不把写盘耗时算作远端无数据等待.
                z_last_data_at = Instant::now();
                z_window = z_window.saturating_add(2).min(z_max_window);
                update_progress(c_task, count, total, true);
            };
            let z_pending_empty = z_pending.is_empty();
            drop(z_pending);
            // 读远端出错时也写完本地缓冲, 再更新续传断点, 避免重连后的截断与后台写入并发.
            let z_flush_at = Instant::now();
            tokio::time::timeout(TRANSFER_STALL, dest.flush())
                .await
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "刷新本地文件超时"))??;
            z_write_elapsed += z_flush_at.elapsed();
            c_resume.0 = count;
            // 显式等待远端文件句柄关闭: 只有成功关闭且无残留请求的会话才会放回复用池, 避免污染下一次下载.
            let z_closed = tokio::time::timeout(
                Duration::from_secs(if z_read_result.is_ok() { 15 } else { 3 }),
                z_raw.close(z_handle),
            )
            .await;
            z_read_result?;
            ensure!(count == total, "下载源在传输期间被截断");
            let z_elapsed = z_data_start.elapsed();
            if count > z_start && let Ok(mut t) = c_task.lock()
                && (!t.directory || t.transfer_diagnostics.as_ref().is_none_or(|(c, _)| z_elapsed > *c))
            {
                t.transfer_diagnostics = Some((z_elapsed, format!(
                    "下载数据 {} B · 用时 {:.2} 秒 · 等待远端数据 {:.2} 秒 · 等待本地写入 {:.2} 秒",
                    count - z_start, z_elapsed.as_secs_f64(), z_read_elapsed.as_secs_f64(), z_write_elapsed.as_secs_f64(),
                )));
            }
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
                if let Err(err) = retry_local_file_change(|| tokio::fs::rename(&z_part, &c_local)).await {
                    if !c_overwrite {
                        let _ = tokio::fs::remove_file(&c_local).await;
                    }
                    return Err(anyhow::Error::from(err).context("本地文件改名失败"));
                }
                Ok(())
            });
            z_commit.await.context("提交下载任务异常")??;
            // 只放回远端句柄已确认关闭、且无未完成请求的干净会话.
            if z_pending_empty
                && matches!(z_closed, Ok(Ok(_)))
                && let Some((l_link, l_alive)) = z_link
            {
                put_idle_sftp(c_session, l_link, l_alive, (z_raw, z_version));
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
        crate::mcp::reset_diagnostics();
        if let Ok(mut l_state) = self.state.lock() {
            l_state.mcp_clients.clear();
        }
        let z_result = match self.store.settings() {
            Err(err) => Err(format!("{err:#}")),
            Ok(l_settings) if !l_settings.mcp_enabled => {
                self.cancel_all_mcp_tasks();
                crate::mcp::clear_persisted_sessions(self.data_dir());
                Err("MCP 服务已关闭".into())
            }
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

    // 进程退出前立即停止 MCP 监听并释放端口, 保留持久化会话供下次启动无缝续接.
    pub async fn stop_mcp(&self) {
        let mut z_http = self.mcp_http.lock().await;
        if let Some(l_task) = z_http.0.take() {
            l_task.abort();
            let _ = l_task.await;
        }
        crate::mcp::reset_diagnostics();
        if let Ok(mut l_state) = self.state.lock() {
            l_state.mcp_running = false;
            l_state.mcp_clients.clear();
        }
        z_http.1 = Err("MCP 服务已关闭".into());
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
        let (z_commands, z_command_groups, z_commands_error, z_proxies) = if c_actor == Actor::User {
            let (l_commands, l_groups, l_error) = match self
                .commands
                .list()
                .and_then(|c| self.commands.groups().map(|l_groups| (c, l_groups)))
            {
                Ok((l_commands, l_groups)) => (l_commands, l_groups, String::new()),
                Err(err) => (Vec::new(), Vec::new(), err.to_string()),
            };
            // 代理列表给界面选择用, 不含密码; 磁盘读取在获取全局状态锁前完成, 避免阻塞终端刷新.
            let l_proxies = self
                .store
                .proxies()
                .unwrap_or_default()
                .iter()
                .map(|c| {
                    json!({
                        "id":c.id,"name":c.name,"kind":c.kind,"host":c.host,"port":c.port,"username":c.username,
                        "has_password":crate::config::ConfigStore::proxy_has_password(c)
                    })
                })
                .collect::<Vec<_>>();
            (l_commands, l_groups, l_error, l_proxies)
        } else {
            (Vec::new(), Vec::new(), String::new(), Vec::new())
        };
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
            // 提示与请求标识在同一把锁内一起取, 保证属于同一轮.
            let (auth_id, prompts) = session
                .auth_answers
                .lock()
                .ok()
                .and_then(|c| c.as_ref().map(|c| (Some(c.0.clone()), c.1.clone())))
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
            let z_latency = session.latency_ms.load(Ordering::Relaxed);
            let z_group = z_connections
                .iter()
                .find(|c| c.id == session.connection.id)
                .map(|c| c.group.as_str())
                .unwrap_or(session.connection.group.as_str());
            let z_disconnect_detail = (c_actor == Actor::User).then(|| session.main_link.detail()).flatten();
            sessions.push(json!({"session_id":session.id,"link":session.link,"terminal":session.terminal_gen.load(Ordering::Acquire),"connection_id":session.connection.id,"name":session.connection.name,"group":z_group,"status":session.status.lock().map(|s|s.clone()).unwrap_or_default(),"shell":session.shell.load(Ordering::Acquire),"empty":false,"owner":if z_owner{"mcp"}else{"user"},"trust":trust,"auth_prompts":prompts,"auth_id":auth_id,"monitor":monitor,"latency_ms":(z_latency >= 0).then_some(z_latency),"disconnect_detail":z_disconnect_detail}));
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
        let z_tasks_total = tasks.len();
        let mut tasks: Vec<Value> = tasks.into_iter().map(|(_, task)| task).collect();
        // MAX_TASKS 可达 2500 (单批 2000 项), MCP 单次响应上限 1 MiB / 2 MiB: 优先保留进行中与最近任务, 防止 state 永久超限.
        const MCP_MAX_STATE_TASKS: usize = 200;
        let z_tasks_truncated = c_actor == Actor::Mcp && tasks.len() > MCP_MAX_STATE_TASKS;
        if z_tasks_truncated {
            let mut l_active = Vec::new();
            let mut l_done = Vec::new();
            for l_task in tasks {
                if matches!(l_task.get("status").and_then(Value::as_str), Some("queued" | "preparing" | "running" | "retrying" | "cancelling")) {
                    l_active.push(l_task);
                } else {
                    l_done.push(l_task);
                }
            }
            l_active.truncate(MCP_MAX_STATE_TASKS);
            let l_rest = MCP_MAX_STATE_TASKS.saturating_sub(l_active.len());
            l_active.extend(l_done.into_iter().take(l_rest));
            tasks = l_active;
        }
        // 当前标签已断开 (只剩标签) 时按标签记录的连接判断授权, 与上面 sessions 列出的离线标签一致.
        let active = z_state
            .active_session_id
            .as_ref()
            .filter(|id| {
                c_actor == Actor::User
                    || match z_state.sessions.get(*id) {
                        Some(l_session) => z_connections.iter().any(|c| mcp_permits(c, &l_session.connection)),
                        None => z_state.tabs.iter().find(|c| &c.id == *id).and_then(|c| c.connection_id.as_ref())
                            .is_some_and(|l_id| z_connections.iter().any(|c| &c.id == l_id && c.mcp_allowed)),
                    }
            })
            .cloned();
        let z_mcp_status = (c_actor == Actor::User).then(|| {
            json!({
                "running":z_state.mcp_running,
                "clients":z_state.mcp_clients.values().filter(|c| c.1.elapsed() < Duration::from_secs(15)).map(|c| c.0.as_str()).collect::<Vec<_>>(),
                "activity":z_state.mcp_activity.iter().map(|(c_method,c_client,c_detail,c_ok,c_at,c_info,c_error)| json!({"method":c_method,"client":c_client,"detail":c_detail,"ok":c_ok,"at_ms":c_at,"info":c_info,"error":c_error})).collect::<Vec<_>>()
            })
        });
        drop(z_state);
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
            if z_tasks_truncated {
                z_result["tasks_truncated"] = json!(true);
                z_result["tasks_total"] = json!(z_tasks_total);
            }
            // 未授权的连接对 AI 完全不可见, 列表为空时说明原因, 避免 AI 误判为通信或同步故障.
            if z_connections.is_empty() {
                z_result["hint"] = json!("用户尚未授权任何连接给 MCP. 请用户在 HiPShell 连接编辑中勾选“允许 MCP 使用此连接”后再调用 state.");
            }
            return Ok(z_result);
        }
        let mut z_result = json!({
            "connections":z_connections,"sessions":sessions,"tasks":tasks,"active_session_id":active,
            "quick_commands":z_commands,"quick_commands_error":z_commands_error,"settings":z_settings,
            "proxies":z_proxies,"command_groups":z_command_groups
        });
        if let Some(l_mcp_status) = z_mcp_status {
            z_result["mcp_status"] = l_mcp_status;
        }
        Ok(z_result)
    }

    // 按参数取会话. 界面对列表中的文件发起操作时带上 _link (所展示列表所属连接的代次): 标签已重连就拒绝,
    // 每次查找都核对, 旧列表上的操作和重试都不会落到新连接上. 没带时 (MCP、终端操作) 不核对.
    fn session_args(&self, c_args: &Value, c_actor: Actor) -> Result<Arc<LiveSession>> {
        let z_session = self.session(required_string(c_args, "session_id")?, c_actor)?;
        if let Some(l_link) = c_args.get("_link").and_then(Value::as_u64) {
            ensure!(z_session.link == l_link, "标签已重新连接, 请刷新");
        }
        Ok(z_session)
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
    files_failed: u64,
    files_total: u64,
    current: String,
    file_bytes: u64,
    file_total: u64,
    stdout: String,
    stderr: String,
    truncated: bool,
    exit_code: Option<i32>,
    error: Option<String>,
    transfer_retries: u32,
    last_transfer_error: Option<String>,
}
impl From<&Task> for TaskView {
    fn from(t: &Task) -> Self {
        let mut z_stdout = t.stdout.clone();
        let z_diagnostics = t.transfer_diagnostics.as_ref().map_or("", |(_, c)| c.as_str());
        if !z_diagnostics.is_empty() {
            if !z_stdout.is_empty() {
                z_stdout.push('\n');
            }
            z_stdout.push_str(z_diagnostics);
        }
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
            status: task_status(t).into(),
            bytes: t.bytes,
            total: t.total,
            speed_bytes_per_sec: current_speed(t),
            directory: t.directory,
            files_done: t.files_done,
            files_failed: t.files_failed,
            files_total: t.files_total,
            current: t.current.clone(),
            file_bytes: t.file_bytes,
            file_total: t.file_total,
            stdout: z_stdout,
            stderr: t.stderr.clone(),
            truncated: t.truncated,
            exit_code: t.exit_code,
            error: t.error.clone(),
            transfer_retries: t.transfer_retries,
            last_transfer_error: t.last_transfer_error.clone(),
        }
    }
}

impl client::Handler for SshHandler {
    type Error = russh::Error;
    // 主连接和传输连接各自记下收到业务数据、密钥交换与断开信息.
    async fn data(&mut self, _c_channel: russh::ChannelId, _c_data: &[u8], _c_session: &mut client::Session) -> std::result::Result<(), Self::Error> {
        self.link.mark_data();
        Ok(())
    }
    async fn extended_data(&mut self, _c_channel: russh::ChannelId, _c_ext: u32, _c_data: &[u8], _c_session: &mut client::Session) -> std::result::Result<(), Self::Error> {
        self.link.mark_data();
        Ok(())
    }
    async fn kex_done(&mut self, _c_shared_secret: Option<&[u8]>, c_names: &russh::Names, c_session: &mut client::Session) -> std::result::Result<(), Self::Error> {
        self.link.mark_kex(c_names, c_session.remote_sshid());
        Ok(())
    }
    // 记录断开原因与诊断快照; 返回值与库默认实现一致. 本端主动关闭也会走到这里, 但 user_closed 已置位, 不写入断连日志.
    async fn disconnected(&mut self, c_reason: client::DisconnectReason<Self::Error>) -> std::result::Result<(), Self::Error> {
        let (z_summary, z_raw, z_result) = match c_reason {
            client::DisconnectReason::ReceivedDisconnect(l_info) => {
                let l_msg = safe_error(&l_info.message);
                (
                    format!("服务器断开 ({:?}) {l_msg}", l_info.reason_code),
                    format!("ReceivedDisconnect({:?}: {l_msg})", l_info.reason_code),
                    Ok(()),
                )
            }
            client::DisconnectReason::Error(err) => {
                let (l_summary, l_raw) = describe_ssh_error(&err);
                (l_summary, l_raw, Err(err))
            }
        };
        let z_target = self
            .session
            .upgrade()
            .map(|c| format!("{} ({}:{})", c.connection.name, c.connection.host, c.connection.port))
            .unwrap_or_else(|| "-".into());
        record_link_disconnect(
            &self.link,
            self.store.directory(),
            &z_target,
            self.transfer,
            &z_summary,
            &z_raw,
        );
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
        // 本会话已确认过主机指纹 (含主连接密钥重协商与独立传输连接): 直接在内存核对,
        // 避免"本次信任"在重协商时改写在线状态并弹窗卡死, 重协商期间指纹变化则立即拒绝.
        if let Some(l_confirmed) = session.host_fingerprint.lock().unwrap_or_else(PoisonError::into_inner).as_deref() {
            return Ok(l_confirmed == z_fingerprint.as_str());
        }
        if self.transfer {
            return Ok(false);
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
    let z_handle = c_session.ssh_handle()?;
    let (_, z_stderr, z_code) = tokio::time::timeout(Duration::from_secs(c_seconds), async {
        let mut z_channel = ClosingChannel(Some(open_channel(&z_handle, c_seconds).await.context("打开命令通道失败")?));
        exec_collect(&mut z_channel, c_command, c_input, 64 * 1024).await
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
// 等待通道请求 (PTY、Shell) 的答复, 期间到达的输出暂存 (合计最多 1 MiB, 超出丢弃). 返回服务器是否同意.
async fn channel_reply(c_channel: &mut russh::Channel<client::Msg>, c_early: &mut Vec<Vec<u8>>, c_early_bytes: &mut usize) -> Result<bool> {
    loop {
        match c_channel.wait().await {
            Some(ChannelMsg::Success) => return Ok(true),
            Some(ChannelMsg::Failure) => return Ok(false),
            Some(ChannelMsg::Data { data }) | Some(ChannelMsg::ExtendedData { data, .. }) => {
                *c_early_bytes += data.len();
                if *c_early_bytes <= 1024 * 1024 { c_early.push(data.to_vec()); }
            }
            Some(ChannelMsg::Eof | ChannelMsg::Close) | None => bail!("终端通道已被服务器关闭"),
            Some(_) => {}
        }
    }
}
// 执行命令并收集输出, 返回 (标准输出, 标准错误前 4KiB, 退出码); 标准输出超过 c_limit 时报错.
// 输入与读取同时进行: 先写完再读时, 远端边读边输出会塞满通道队列, russh 主循环随之停止收包, 整条连接卡住.
async fn exec_collect(c_channel: &mut ClosingChannel, c_command: &str, c_input: &[u8], c_limit: usize) -> Result<(Vec<u8>, Vec<u8>, Option<u32>)> {
    c_channel.exec(true, c_command).await.context("服务器未允许执行命令")?;
    let mut z_writer = c_channel.make_writer();
    // 写完输入后关闭写端 (发送 EOF), 读取 stdin 的命令才会结束.
    let z_send = async move {
        z_writer.write_all(c_input).await?;
        z_writer.shutdown().await
    };
    tokio::pin!(z_send);
    let mut z_sent = false;
    let (mut z_stdout, mut z_stderr, mut z_code) = (Vec::new(), Vec::new(), None);
    loop {
        tokio::select! {
            l_result = &mut z_send, if !z_sent => {
                z_sent = true;
                l_result.context("发送命令输入失败")?;
            }
            l_message = c_channel.wait() => match l_message {
                Some(ChannelMsg::Data { data }) => {
                    ensure!(z_stdout.len() + data.len() <= c_limit, "命令输出超过 {} KiB 上限", c_limit / 1024);
                    z_stdout.extend_from_slice(&data);
                }
                Some(ChannelMsg::ExtendedData { data, .. }) => {
                    let l_room = 4096usize.saturating_sub(z_stderr.len());
                    z_stderr.extend_from_slice(&data[..data.len().min(l_room)]);
                }
                Some(ChannelMsg::ExitStatus { exit_status }) => z_code = Some(exit_status),
                // exec 请求要求答复: 服务器拒绝但保留通道时立即报错, 不等到外层超时.
                Some(ChannelMsg::Failure) => bail!("服务器拒绝执行命令"),
                Some(_) => {}
                None => break,
            }
        }
    }
    Ok((z_stdout, z_stderr, z_code))
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
        !c_path.chars().any(|c| c.is_control()),
        "远端路径包含控制字符"
    );
    ensure!(
        !c_path.split('/').any(|part| part == "." || part == ".."),
        "远端路径包含相对跳转分量"
    );
    ensure!(!c_path.ends_with('/'), "远端路径缺少文件名");
    Ok(())
}

fn validate_new_remote_name(c_name: &str) -> Result<()> {
    ensure!(
        c_name.len() <= 255
            && !c_name.trim().is_empty()
            && !matches!(c_name, "." | "..")
            && !c_name.contains('/')
            && !c_name.chars().any(char::is_control),
        "新名称不能为空、为.或..、含路径分隔符或控制字符, 且最多255字节"
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
// 装好终端的关闭信号. 调用方持 terminal_tx 锁并同时标记终端已打开, 与 close_terminal 互斥.
fn install_shell_stop(c_session: &LiveSession) -> oneshot::Receiver<()> {
    let (z_tx, z_rx) = oneshot::channel();
    *c_session.shell_stop.lock().unwrap_or_else(PoisonError::into_inner) = Some(z_tx);
    z_rx
}
// 清理完成后才标记终端已关闭: 先标记时并发的 open_terminal 会装好新终端的输入与尺寸通道, 随后又被这里取走.
// 请求远端整屏重绘: 先通知远端尺寸少一列, 300 毫秒后恢复为本地当前尺寸. 两次尺寸确实变化才会触发 SIGWINCH,
// 间隔让写入任务把两次分别发出 (尺寸通道只保留最新值). 期间用户调整尺寸时以用户的为准.
fn request_redraw(c_session: &Arc<LiveSession>, c_size: (u16, u16)) {
    let (z_rows, z_cols) = c_size;
    let z_nudge = if z_cols > 1 { z_cols - 1 } else { z_cols + 1 };
    let z_sent = c_session.resize_tx.lock().unwrap_or_else(PoisonError::into_inner).as_ref().map(|c| c.send_replace((z_rows, z_nudge)));
    if z_sent.is_none() {
        return;
    }
    let z_session = c_session.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let l_size = screen_size(z_session.terminal.lock().unwrap_or_else(PoisonError::into_inner).parser.screen());
        if let Some(l_tx) = z_session.resize_tx.lock().unwrap_or_else(PoisonError::into_inner).as_ref() {
            l_tx.send_replace(l_size);
        }
    });
}

fn shell_closed(c_session: &LiveSession, c_notice: &str) {
    c_session.terminal_tx.lock().unwrap_or_else(PoisonError::into_inner).take();
    c_session.resize_tx.lock().unwrap_or_else(PoisonError::into_inner).take();
    c_session.shell_ready.store(false, Ordering::Release);
    stop_monitor(c_session);
    // 清空旧画面并隐藏光标, 只留提示, 避免残留的光标停在旧位置像是还能输入.
    {
        let mut z_term = c_session.terminal.lock().unwrap_or_else(PoisonError::into_inner);
        z_term.reset_screen();
        z_term.parser.process(format!("\x1b[?25l\x1b[2m{c_notice}\x1b[0m").as_bytes());
    }
    c_session.shell.store(false, Ordering::Release);
}

fn reserve_monitor(c_session: &LiveSession) -> Option<(u64, oneshot::Receiver<()>)> {
    if c_session.monitor_unsupported.load(Ordering::Acquire) {
        return None;
    }
    let mut l_ctrl = c_session.monitor_ctrl.lock().unwrap_or_else(PoisonError::into_inner);
    if l_ctrl.1.is_some() {
        return None;
    }
    l_ctrl.0 = l_ctrl.0.wrapping_add(1);
    let (l_tx, l_rx) = oneshot::channel();
    l_ctrl.1 = Some(l_tx);
    Some((l_ctrl.0, l_rx))
}

fn spawn_monitor(c_session: Arc<LiveSession>, c_gen: u64, c_stop_rx: oneshot::Receiver<()>) {
    let z_session = Arc::downgrade(&c_session);
    tokio::spawn(async move {
        monitor_loop(z_session.clone(), c_stop_rx).await;
        if let Some(l_session) = z_session.upgrade() {
            let mut l_ctrl = l_session.monitor_ctrl.lock().unwrap_or_else(PoisonError::into_inner);
            if l_ctrl.0 == c_gen {
                l_ctrl.1 = None;
            }
            // 停流收尾时若已无新监控接替且非 /proc 不支持, 清空旧采样, 防止在途包残留过期 MiniBar 或拼接断档曲线.
            if l_ctrl.1.is_none() && !l_session.monitor_unsupported.load(Ordering::Acquire) {
                *l_session.monitor.lock().unwrap_or_else(PoisonError::into_inner) = None;
            }
        }
    });
}

fn disarm_monitor(c_session: &LiveSession) -> Option<oneshot::Sender<()>> {
    let mut l_ctrl = c_session.monitor_ctrl.lock().unwrap_or_else(PoisonError::into_inner);
    l_ctrl.0 = l_ctrl.0.wrapping_add(1);
    l_ctrl.1.take()
}

fn clear_monitor_if_idle(c_session: &LiveSession) {
    if c_session.monitor_unsupported.load(Ordering::Acquire) {
        return;
    }
    let l_ctrl = c_session.monitor_ctrl.lock().unwrap_or_else(PoisonError::into_inner);
    if l_ctrl.1.is_none() {
        *c_session.monitor.lock().unwrap_or_else(PoisonError::into_inner) = None;
    }
}

fn stop_monitor(c_session: &LiveSession) {
    if let Some(l_tx) = disarm_monitor(c_session) {
        let _ = l_tx.send(());
    }
    clear_monitor_if_idle(c_session);
}

// russh 在连接结束、回应通道被释放时同样返回 Ok, 需要核对连接仍在.
async fn ssh_ping(c_handle: &client::Handle<SshHandler>, c_seconds: u64) -> bool {
    matches!(tokio::time::timeout(Duration::from_secs(c_seconds), c_handle.send_ping()).await, Ok(Ok(())))
        && !c_handle.is_closed()
}

// 仅文件连接或终端已关闭时没有读取循环发现断线, 定时检查 SSH 连接, 断开后更新状态以便重连.
// 有终端时由终端读取结束处理; 会话被释放或换了新连接时退出. 顺带回收过期的空闲传输会话.
fn watch_connection(c_backend: &Arc<Backend>, c_session: &Arc<LiveSession>, c_handle: &Arc<client::Handle<SshHandler>>) {
    let z_backend = Arc::downgrade(c_backend);
    let z_session = Arc::downgrade(c_session);
    let z_handle = c_handle.clone();
    let z_log_dir = c_backend.store.directory().to_path_buf();
    let z_target = format!("{} ({}:{})", c_session.connection.name, c_session.connection.host, c_session.connection.port);
    tokio::spawn(async move {
        // 活跃标签每 15 秒探活一次 (避免每 3 秒高频发送 keepalive@openssh.com 触发严格服务端或设备限制),
        // 非活跃标签或探活未回应时用 20 秒间隔; 本地状态检查与空闲会话回收仍保持 3 秒粒度.
        let z_active_interval = Duration::from_secs(15);
        let z_idle_interval = Duration::from_secs(20);
        let z_tick = Duration::from_secs(3);
        let mut z_probed = Instant::now();
        let mut z_checked_opens = z_probed;
        let mut z_first = true;
        let mut z_last_tick = Instant::now();
        let mut z_last_wall = epoch_ms();
        let mut z_missed = 0u32;
        let mut z_stale_hits = 0u32;
        loop {
            let l_sleep_for = if z_first { Duration::from_secs(1) } else { z_tick };
            tokio::time::sleep(l_sleep_for).await;
            let l_mono_elapsed = z_last_tick.elapsed();
            let l_wall_now = epoch_ms();
            let l_tick_sleep = sleep_gap_secs(l_mono_elapsed, l_wall_now.saturating_sub(z_last_wall), l_sleep_for);
            z_last_tick = Instant::now();
            z_last_wall = l_wall_now;

            let Some(l_session) = z_session.upgrade() else { break; };
            if !l_session.handle.lock().unwrap_or_else(PoisonError::into_inner).as_ref().is_some_and(|c| Arc::ptr_eq(c, &z_handle)) {
                break;
            }
            if let Some(l_secs) = l_tick_sleep {
                // 休眠或进程长时间挂起后清零丢包计数并延后探测, 给唤醒后的网络恢复留出窗口.
                l_session.main_link.record_sleep(l_secs);
                z_missed = 0;
                z_stale_hits = 0;
                z_probed = z_last_tick;
            }
            purge_idle_sftp(&l_session);

            let l_connected = l_session.status.lock().unwrap_or_else(PoisonError::into_inner).as_str() == "connected";
            let l_active = z_missed == 0 && z_backend.upgrade().is_some_and(|c| {
                c.state.try_lock().is_ok_and(|l| l.active_session_id.as_deref() == Some(l_session.id.as_str()))
            });
            let l_interval = if l_active { z_active_interval } else { z_idle_interval };
            if l_connected && !z_handle.is_closed() && (z_first || z_probed.elapsed() >= l_interval) {
                z_first = false;
                let l_ping_at = Instant::now();
                let l_ping_wall = epoch_ms();
                let l_pong = ssh_ping(&z_handle, 15).await;
                z_probed = Instant::now();
                // 探活最长可耗时 15 秒, 同步刷新 tick 基准避免下一轮误计入定时器漂移.
                z_last_tick = z_probed;
                z_last_wall = epoch_ms();
                let l_ping_elapsed = l_ping_at.elapsed();
                if l_pong {
                    z_missed = 0;
                    l_session.main_link.mark_pong();
                    l_session.latency_ms.store(l_ping_elapsed.as_millis().min(i64::MAX as u128) as i64, Ordering::Relaxed);
                    check_stale_opens(&l_session, &z_handle, &z_log_dir, &z_target, &mut z_checked_opens, &mut z_stale_hits);
                } else if let Some(l_secs) = sleep_gap_secs(l_ping_elapsed, z_last_wall.saturating_sub(l_ping_wall), Duration::from_secs(15)) {
                    // 休眠发生在探活期间且唤醒后连接可能已断, 在 is_closed 判断之前记录休眠跨度.
                    l_session.main_link.record_sleep(l_secs);
                    z_missed = 0;
                    z_stale_hits = 0;
                } else if !z_handle.is_closed() {
                    z_stale_hits = 0;
                    handle_watch_miss(&l_session, &z_log_dir, &z_target, &mut z_missed);
                }
            }
            // 终端仍开着时由终端读取循环收尾; 它若先看到连接未断而只按终端关闭处理, 下一轮由这里接手, 不能就此退出.
            if z_handle.is_closed() && !l_session.shell.load(Ordering::Acquire) {
                close_session(&l_session, &dropped_status(&l_session).await, true);
                l_session.handle.lock().unwrap_or_else(PoisonError::into_inner).take();
                break;
            }
        }
    });
}

// 已打开的终端可继续使用; 只有无终端连接连续两次发现大量卡死通道请求时才回收.
fn check_stale_opens(
    c_session: &LiveSession,
    c_handle: &Arc<client::Handle<SshHandler>>,
    c_log_dir: &Path,
    c_target: &str,
    c_checked_opens: &mut Instant,
    c_stale_hits: &mut u32,
) {
    if c_session.shell.load(Ordering::Acquire) {
        *c_stale_hits = 0;
        return;
    }
    if c_checked_opens.elapsed() < Duration::from_secs(15) {
        return;
    }
    *c_checked_opens = Instant::now();
    *c_stale_hits = if stale_opens(Arc::as_ptr(c_handle) as usize, Duration::from_secs(45)) >= 4 {
        *c_stale_hits + 1
    } else {
        0
    };
    if *c_stale_hits < 2 {
        return;
    }
    let _z_tx = c_session.terminal_tx.lock().unwrap_or_else(PoisonError::into_inner);
    if !c_session.shell.load(Ordering::Acquire) {
        record_link_disconnect(
            &c_session.main_link,
            c_log_dir,
            c_target,
            false,
            "服务器长时间未答复通道打开请求, 无法再打开终端或执行命令",
            "Watchdog(stale_channel_opens)",
        );
        kill_socket(c_session);
    }
}

fn handle_watch_miss(
    c_session: &LiveSession,
    c_log_dir: &Path,
    c_target: &str,
    c_missed: &mut u32,
) {
    *c_missed += 1;
    if *c_missed >= 2 {
        c_session.latency_ms.store(-1, Ordering::Relaxed);
    }
    let z_last = c_session.main_link.last_rx_mono();
    let z_receiving = z_last > 0 && mono_ms().saturating_sub(z_last) < 60_000;
    // 每轮失败约 15 秒超时 + 20 秒间隔 ≈ 35 秒: 无收包 4 轮约 2 分钟, 有收包 6 轮约 3 分钟.
    if *c_missed < if z_receiving { 6 } else { 4 } {
        return;
    }
    let z_summary = if z_receiving {
        "探活超时, 约 3 分钟发出的数据没有得到服务器回应 (连接发送受阻)"
    } else {
        "探活超时, 约 2 分钟没有收到服务器回应 (网络或代理链路中断)"
    };
    record_link_disconnect(
        &c_session.main_link,
        c_log_dir,
        c_target,
        false,
        z_summary,
        &format!("Watchdog(ping_missed={}, receiving={z_receiving})", *c_missed),
    );
    kill_socket(c_session);
}

// 检测休眠或进程长时间挂起: 墙钟比单调时钟多跨 15 秒 (macOS Instant 停表), 或实际耗时超出预期 25 秒 (Windows QPC 不断表).
fn sleep_gap_secs(c_mono: Duration, c_wall_ms: u64, c_expected: Duration) -> Option<u64> {
    let z_wall_drift = c_wall_ms.saturating_sub(c_mono.as_millis() as u64);
    if z_wall_drift > 15_000 {
        return Some(z_wall_drift / 1000);
    }
    let z_over = c_mono.saturating_sub(c_expected);
    (z_over > Duration::from_secs(25)).then_some(z_over.as_secs())
}

fn describe_ssh_error(c_err: &russh::Error) -> (String, String) {
    match c_err {
        russh::Error::KeepaliveTimeout => (
            "保活超时, 长时间未收到服务器任何数据 (网络或代理链路中断)".into(),
            "KeepaliveTimeout".into(),
        ),
        russh::Error::InactivityTimeout => (
            "无活动超时, 长时间数据未能发出或未收到数据 (网络或代理链路堵塞)".into(),
            "InactivityTimeout".into(),
        ),
        russh::Error::Disconnect => (
            "连接被关闭, 未收到断开说明 (服务器、代理或网络中断)".into(),
            "Disconnect".into(),
        ),
        russh::Error::IO(l_io) => {
            let l_detail = safe_error(&l_io.to_string());
            let l_summary = match l_io.kind() {
                std::io::ErrorKind::UnexpectedEof => {
                    if l_detail.is_empty() || l_detail == "early eof" {
                        "SSH 数据流提前结束, 未收到服务器断开通知".into()
                    } else {
                        format!("SSH 数据流提前结束, 未收到服务器断开通知 ({l_detail})")
                    }
                }
                std::io::ErrorKind::ConnectionReset => "连接被对端或代理重置 (Connection reset)".into(),
                std::io::ErrorKind::ConnectionAborted | std::io::ErrorKind::BrokenPipe => "本地网络或代理链路已中断".into(),
                std::io::ErrorKind::TimedOut => "网络读写超时, 连接已中断".into(),
                _ => format!("网络错误: {l_detail}"),
            };
            (l_summary, format!("IO({:?}: {l_detail})", l_io.kind()))
        }
        l_err => {
            let l_text = safe_error(&l_err.to_string());
            (format!("网络错误: {l_text}"), l_text)
        }
    }
}

// 汇总断连诊断: 写入首次原因供界面展示与复制, 已认证会话的意外断连同时追加无凭据日志到 disconnect.log.
fn record_link_disconnect(
    c_link: &SshLinkState,
    c_log_dir: &Path,
    c_target: &str,
    c_transfer: bool,
    c_summary: &str,
    c_raw: &str,
) {
    use std::fmt::Write as _;

    // 本端主动断开/取消/认证失败关套接字或已记录过首次原因时直接跳过, 避免污染诊断与重复计算.
    if c_link.user_closed.load(Ordering::Acquire) || c_link.has_error() {
        return;
    }
    let z_now_mono = mono_ms();
    let z_now_awake = awake_ms();
    let z_now_wall = epoch_ms();
    let z_diag = c_link.diag.lock().unwrap_or_else(PoisonError::into_inner).clone();
    let z_rx_bytes = c_link.rx_bytes.load(Ordering::Relaxed);
    let z_tx_bytes = c_link.tx_bytes.load(Ordering::Relaxed);
    let z_data_mono = c_link.last_data_mono.load(Ordering::Relaxed);
    let z_pong_mono = c_link.last_pong_mono.load(Ordering::Relaxed);
    let z_tcp_rx_mono = c_link.last_tcp_rx_mono.load(Ordering::Relaxed);
    let z_tcp_tx_mono = c_link.last_tcp_tx_mono.load(Ordering::Relaxed);
    let z_rekeys = c_link.kex_count.load(Ordering::Relaxed).saturating_sub(1);

    let z_fake_ip = !z_diag.via_proxy && (is_fake_ip(z_diag.peer_addr) || is_fake_ip(z_diag.local_addr));
    let z_route = if z_diag.via_proxy {
        "经代理"
    } else if z_fake_ip {
        "直连 (命中 Fake-IP/TUN)"
    } else {
        "直连"
    };

    let z_authenticated = z_diag.connected_mono != 0;
    let z_uptime_secs = z_authenticated.then(|| {
        let l_mono = z_now_mono.saturating_sub(z_diag.connected_mono) / 1000;
        let l_wall = (z_diag.connected_wall != 0).then(|| z_now_wall.saturating_sub(z_diag.connected_wall) / 1000).unwrap_or(0);
        l_mono.max(l_wall)
    });

    // 合并休眠停表时钟 (macOS Instant / Windows QueryUnbiasedInterruptTime) 与 watch_connection 观测到的休眠跨度.
    let (z_ref_awake, z_ref_wall) = if z_diag.rx_clock_awake != 0 && z_diag.rx_clock_wall != 0 {
        (z_diag.rx_clock_awake, z_diag.rx_clock_wall)
    } else {
        (z_diag.connected_awake, z_diag.connected_wall)
    };
    let z_wall_sleep = (z_ref_awake != 0 && z_ref_wall != 0).then(|| {
        let l_awake_gap = z_now_awake.saturating_sub(z_ref_awake) / 1000;
        let l_wall_gap = z_now_wall.saturating_sub(z_ref_wall) / 1000;
        (l_wall_gap > l_awake_gap.saturating_add(15)).then_some(l_wall_gap - l_awake_gap)
    }).flatten();
    let z_watch_sleep = (z_diag.sleep_secs > 0 && z_now_mono.saturating_sub(z_diag.sleep_at_mono) <= 300_000)
        .then_some(z_diag.sleep_secs);
    let z_sleep_secs = z_wall_sleep.into_iter().chain(z_watch_sleep).max();

    let z_ago = |c_mono: u64| -> Option<u64> {
        (c_mono != 0).then(|| z_now_mono.saturating_sub(c_mono) / 1000)
    };

    // 界面展示的精简摘要: 保留核心原因、路径、时长、休眠、收包/探活间隔与重协商次数.
    let mut z_ui = c_summary.to_owned();
    let _ = write!(z_ui, " · {z_route}");
    if let Some(l_secs) = z_uptime_secs {
        let _ = write!(z_ui, " · 已连接 {}", format_diag_duration(l_secs));
    }
    if let Some(l_sleep) = z_sleep_secs {
        let _ = write!(z_ui, " · 期间疑似系统休眠约 {l_sleep} 秒");
    }
    let z_data_ago = z_ago(z_data_mono);
    let z_pong_ago = z_ago(z_pong_mono);
    if let Some(l_data) = z_data_ago {
        let _ = write!(z_ui, " · 业务收包 {l_data} 秒前");
    }
    if let Some(l_pong) = z_pong_ago {
        let _ = write!(z_ui, " · 探活 {l_pong} 秒前");
    }
    if z_data_ago.is_none() && z_pong_ago.is_none() && let Some(l_tcp) = z_ago(z_tcp_rx_mono) {
        let _ = write!(z_ui, " · TCP 收包 {l_tcp} 秒前");
    }
    if z_rekeys > 0 {
        let _ = write!(z_ui, " · 已重协商 {z_rekeys} 次");
    }

    if !z_authenticated {
        c_link.set_error_once(z_ui, None);
        return;
    }

    // 完整诊断行 (不含密码、密钥、用户名和终端内容), 仅已认证会话用于一键复制与 disconnect.log 落盘.
    let z_addrs = match (z_diag.local_addr, z_diag.peer_addr) {
        (Some(l_local), Some(l_peer)) => format!("{l_local} -> {l_peer}"),
        (_, Some(l_peer)) => format!("-> {l_peer}"),
        _ => "-".into(),
    };
    let z_fmt_ago = |c_mono: u64| z_ago(c_mono).map_or_else(|| "-".into(), |c| format!("{c}s"));
    let z_detail = format!(
        "[{}] {} {} | 原因: {} | 原始错误: {} | 路径: {} ({}) | 服务端: {} | 算法: cipher={} kex={} strict_kex={} rekeys={} | 已连接: {} | 流量: rx={} tx={} | 间隔: 业务={} 探活={} tcp收={} tcp发={} | 休眠: {}",
        format_utc_timestamp(z_now_wall),
        if c_transfer { "transfer" } else { "main" },
        safe_error(c_target),
        c_summary,
        safe_error(c_raw),
        z_route,
        z_addrs,
        if z_diag.remote_sshid.is_empty() { "-" } else { z_diag.remote_sshid.as_str() },
        if z_diag.cipher.is_empty() { "-" } else { z_diag.cipher.as_str() },
        if z_diag.kex.is_empty() { "-" } else { z_diag.kex.as_str() },
        z_diag.strict_kex,
        z_rekeys,
        z_uptime_secs.map_or_else(|| "-".into(), format_diag_duration),
        format_diag_bytes(z_rx_bytes),
        format_diag_bytes(z_tx_bytes),
        z_fmt_ago(z_data_mono),
        z_fmt_ago(z_pong_mono),
        z_fmt_ago(z_tcp_rx_mono),
        z_fmt_ago(z_tcp_tx_mono),
        z_sleep_secs.map_or_else(|| "无".into(), |c| format!("{c}s")),
    );

    if c_link.set_error_once(z_ui, Some(z_detail.clone()))
        && !c_link.user_closed.load(Ordering::Acquire)
    {
        append_disconnect_log(c_log_dir.to_path_buf(), z_detail);
    }
}

fn append_disconnect_log(c_dir: PathBuf, c_line: String) {
    static Z_LOG_LOCK: Mutex<()> = Mutex::new(());
    const MAX_LOG_BYTES: u64 = 256 * 1024;
    tokio::task::spawn_blocking(move || {
        let _l_guard = Z_LOG_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
        let _ = std::fs::create_dir_all(&c_dir);
        let z_path = c_dir.join("disconnect.log");
        if std::fs::metadata(&z_path).is_ok_and(|c| c.len() >= MAX_LOG_BYTES) {
            let l_old = c_dir.join("disconnect.log.old");
            // 先直接替换 (Windows 下 Rust std 已带 MOVEFILE_REPLACE_EXISTING), 仅失败时再删旧备份重试, 避免源文件被占用时白白丢失 .old.
            if std::fs::rename(&z_path, &l_old).is_err() {
                let _ = std::fs::remove_file(&l_old);
                let _ = std::fs::rename(&z_path, &l_old);
            }
        }
        let mut z_opts = std::fs::OpenOptions::new();
        z_opts.create(true).append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            z_opts.mode(0o600);
        }
        if let Ok(mut l_file) = z_opts.open(&z_path) {
            use std::io::Write as _;
            let _ = writeln!(l_file, "{c_line}");
        }
    });
}

fn is_fake_ip(c_addr: Option<std::net::SocketAddr>) -> bool {
    matches!(c_addr, Some(std::net::SocketAddr::V4(l_v4)) if {
        let l_ip = l_v4.ip().octets();
        l_ip[0] == 198 && (l_ip[1] & 0xfe) == 18
    })
}

fn format_diag_duration(c_secs: u64) -> String {
    if c_secs >= 60 {
        format!("{} 分 {} 秒", c_secs / 60, c_secs % 60)
    } else {
        format!("{c_secs} 秒")
    }
}

fn format_diag_bytes(c_bytes: u64) -> String {
    if c_bytes >= 1024 * 1024 * 1024 {
        format!("{:.2} GiB", c_bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if c_bytes >= 1024 * 1024 {
        format!("{:.2} MiB", c_bytes as f64 / (1024.0 * 1024.0))
    } else if c_bytes >= 1024 {
        format!("{:.1} KiB", c_bytes as f64 / 1024.0)
    } else {
        format!("{c_bytes} B")
    }
}

fn format_utc_timestamp(c_epoch_ms: u64) -> String {
    let z_secs = c_epoch_ms / 1000;
    let z_days = (z_secs / 86_400) as i64 + 719_468;
    let z_era = z_days.div_euclid(146_097);
    let z_doe = z_days.rem_euclid(146_097) as u64;
    let z_yoe = (z_doe - z_doe / 1460 + z_doe / 36_524 - z_doe / 146_096) / 365;
    let z_y = z_yoe as i64 + z_era * 400;
    let z_doy = z_doe - (365 * z_yoe + z_yoe / 4 - z_yoe / 100);
    let z_mp = (5 * z_doy + 2) / 153;
    let z_d = z_doy - (153 * z_mp + 2) / 5 + 1;
    let z_m = if z_mp < 10 { z_mp + 3 } else { z_mp - 9 };
    let z_year = z_y + i64::from(z_m <= 2);
    let z_tod = z_secs % 86_400;
    format!("{z_year:04}-{z_m:02}-{z_d:02}T{:02}:{:02}:{:02}Z", z_tod / 3600, (z_tod % 3600) / 60, z_tod % 60)
}

// 意外断线的状态: 有断开原因时按错误显示 (状态栏左侧提示并可重试), 否则为已断开.
// russh 先关闭连接句柄再回调记录原因, 稍等片刻 (最多 1 秒) 以免显示成没有原因的"已断开".
async fn dropped_status(c_session: &LiveSession) -> String {
    for _ in 0..10 {
        if c_session.main_link.has_error() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    match c_session.main_link.error() {
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
    c_session.latency_ms.store(-1, Ordering::Relaxed);
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
    if let Ok(mut tx) = c_session.terminal_tx.lock() {
        tx.take();
    }
    if let Ok(mut tx) = c_session.resize_tx.lock() {
        tx.take();
    }
    c_session.shell_ready.store(false, Ordering::Release);
    stop_monitor(c_session);
    c_session.browse_sftp.lock().unwrap_or_else(PoisonError::into_inner).take();
    clear_idle_sftp(c_session);
    if let Some(l_task) = c_session.file_details.lock().unwrap_or_else(PoisonError::into_inner).take() {
        l_task.abort.abort();
    }
    if c_dropped {
        // 连接已判定失联: 关闭套接字, 确保残留的收发也随之结束.
        c_session.shell.store(false, Ordering::Release);
        kill_socket(c_session);
        return;
    }
    // 用户主动断开、重连或关闭标签: 标记为主动关闭, 避免后续套接字收尾写入意外断连日志.
    c_session.main_link.mark_user_closed();
    // 传输连接随会话关闭; 正在建立的会由 transfer_handle 发现会话已移除后自行关闭.
    c_session.transfer_auth.lock().unwrap_or_else(PoisonError::into_inner).take();
    c_session.proxy.lock().unwrap_or_else(PoisonError::into_inner).take();
    // 礼貌断开后关闭套接字副本: 出站被卡住时断开消息发不出去, 连接和挂在上面的任务也能结束.
    // 同一把锁内标记已关闭: 正在建立的独立连接据此不再发布 (见 transfer_handle).
    let z_socket = {
        let mut l_socket = c_session.transfer_socket.lock().unwrap_or_else(PoisonError::into_inner);
        l_socket.1 = true;
        l_socket.0.take()
    };
    if let Some(l_socket) = &z_socket {
        l_socket.link.mark_user_closed();
    }
    let z_handle = c_session.transfer.try_lock().ok().and_then(|mut c| c.0.take());
    if z_handle.is_some() || z_socket.is_some() {
        tokio::spawn(async move {
            if let Some(l_handle) = z_handle {
                l_handle.state.mark_user_closed();
                let _ = tokio::time::timeout(Duration::from_secs(2), l_handle.handle.disconnect(Disconnect::ByApplication, "关闭连接", "zh-CN")).await;
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            if let Some(l_socket) = z_socket { let _ = l_socket.stream.shutdown(std::net::Shutdown::Both); }
        });
    }
}
// 退役探活失败的独立连接: 调用方随后从槽位移除, 这里移除空闲池里它上面的会话, 之后不再分配新任务, 也不会放回 (见 put_idle_sftp).
// 正在用它的任务继续: 探活失败也可能只是上行一时拥塞, 不切断恢复后的传输; 真正卡住的任务每个请求都有超时, 会出错并换新连接重试.
// 使用者按连接引用计数: 传输任务持有连接, 浏览缓存、空闲池、建目录等 SFTP 会话经通道流持有 (见 SftpStream).
// 所有使用者释放后关闭 (只剩后台等待通道确认的任务时也算, 它们随连接关闭结束, 见 open_channel); 会话关闭时立即关闭.
fn retire_transfer(c_session: &Arc<LiveSession>, c_link: SshLink) {
    let z_key = c_link.key();
    let z_handle = c_link.handle;
    let z_state = c_link.state;
    // 同一把锁内清除当前连接标记并摘除这条连接上的浏览缓存: 之后不会再缓存或放回它上面的会话; 在执行的浏览请求持自己的引用跑完.
    {
        let mut l_suspect = c_session.transfer_suspect.lock().unwrap_or_else(PoisonError::into_inner);
        *l_suspect = (0, false);
        let mut l_browse = c_session.browse_sftp.lock().unwrap_or_else(PoisonError::into_inner);
        if l_browse.as_ref().is_some_and(|c| c.1 == z_key) {
            l_browse.take();
        }
    }
    drop_idle_on(c_session, z_key);
    let z_socket = c_session.transfer_socket.lock().unwrap_or_else(PoisonError::into_inner).0.take();
    let z_session = Arc::downgrade(c_session);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            // 本任务与每个等待通道确认的后台任务各持有一份引用.
            let l_closed = z_session.upgrade().is_none_or(|c| c.transfer_socket.lock().unwrap_or_else(PoisonError::into_inner).1);
            if Arc::strong_count(&z_handle) <= 1 + stale_opens(z_key, Duration::ZERO) || z_handle.is_closed() || l_closed {
                break;
            }
        }
        // 仅在仍存活、准备主动回收时标记 user_closed, 保留已意外断开的真实诊断与日志.
        if !z_handle.is_closed() {
            z_state.mark_user_closed();
        }
        let _ = tokio::time::timeout(Duration::from_secs(2), z_handle.disconnect(Disconnect::ByApplication, "关闭连接", "zh-CN")).await;
        if let Some(l_socket) = z_socket {
            let _ = l_socket.stream.shutdown(std::net::Shutdown::Both);
        }
    });
}
// 网络类错误发生在独立连接上时把它记为可疑: 复用前先探活 (见 transfer_handle), 它上面的空闲会话也不再借出. 主连接上的错误不牵连独立连接.
// 只标记仍是当前连接的那条: 已退役或被替换的旧连接不再分配任务, 它的迟到错误不能覆盖当前连接的状态.
// 持可疑标记锁移除空闲会话, 与 put_idle_sftp 互斥: 标记之后不会再有会话放回这条连接.
fn mark_suspect(c_session: &LiveSession, c_link_used: Option<&SshLink>) {
    if let Some(l_link) = c_link_used.filter(|c| c.dedicated) {
        let l_key = l_link.key();
        let mut l_suspect = c_session.transfer_suspect.lock().unwrap_or_else(PoisonError::into_inner);
        if l_suspect.0 == l_key {
            l_suspect.1 = true;
        }
        drop_idle_on(c_session, l_key);
    }
}
// 记下实际使用的连接. 尝试结束时只取这条连接的断开原因.
fn record_transfer_link(c_task: &Arc<Mutex<Task>>, c_link_used: Option<&mut Option<SshLink>>, c_link: &SshLink) {
    if let Some(l_used) = c_link_used {
        *l_used = Some(c_link.clone());
        if c_link.dedicated && let Ok(mut l_task) = c_task.lock() {
            l_task.via_main_link = false;
            l_task.stdout.clear();
            l_task.stdout.push_str("经独立 SSH 连接传输");
        }
    }
}
fn transfer_error(c_err: anyhow::Error, c_link: Option<&SshLink>) -> anyhow::Error {
    if let Some(l_link) = c_link
        && let Some(l_reason) = l_link.state.error()
    {
        let l_path = if l_link.dedicated { "独立传输连接" } else { "主 SSH 连接" };
        return c_err.context(format!("{l_path}已断开: {l_reason}"));
    }
    c_err
}
// 写入统计与用于强制断开的套接字副本一同发布, 避免把新连接的流量记进还在上传的旧连接.
struct SshSocket {
    stream: std::net::TcpStream,
    writes: Arc<Mutex<TcpWriteStats>>,
    link: Arc<SshLinkState>,
}
fn upload_socket_writes(
    c_session: &LiveSession,
    c_link: Option<&Arc<client::Handle<SshHandler>>>,
) -> Option<Arc<Mutex<TcpWriteStats>>> {
    if let Some(l_link) = c_link {
        let l_current = c_session.transfer_suspect.lock().unwrap_or_else(PoisonError::into_inner);
        if l_current.0 != Arc::as_ptr(l_link) as usize {
            return None;
        }
        return c_session.transfer_socket.lock().unwrap_or_else(PoisonError::into_inner).0.as_ref().map(|c| c.writes.clone());
    }
    c_session.socket.lock().unwrap_or_else(PoisonError::into_inner).as_ref().map(|c| c.writes.clone())
}
// 待发布的套接字副本: 被丢弃 (调用方取消、建立失败、未能发布) 时标记主动关闭并终止套接字;
// 发布成功时用 take 转移所有权.
struct SocketGuard(Mutex<Option<SshSocket>>);
impl SocketGuard {
    fn take(&self) -> Option<SshSocket> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).take()
    }
}
impl Drop for SocketGuard {
    fn drop(&mut self) {
        if let Some(l_socket) = self.take() {
            l_socket.link.mark_user_closed();
            let _ = l_socket.stream.shutdown(std::net::Shutdown::Both);
        }
    }
}
// 直接关闭主连接的套接字 (见 ssh_connect), russh 的读写随即出错, 会话任务结束.
fn kill_socket(c_session: &LiveSession) {
    if let Some(l_socket) = c_session.socket.lock().unwrap_or_else(PoisonError::into_inner).take() {
        let _ = l_socket.stream.shutdown(std::net::Shutdown::Both);
    }
}
fn safe_error(c_error: &str) -> String {
    c_error
        .chars()
        .filter(|c| !c.is_control())
        .take(512)
        .collect()
}

async fn authenticate_password(c_handle: &mut client::Handle<SshHandler>, c_user: &str, c_password: &str, c_seconds: u64) -> Result<bool> {
    Ok(tokio::time::timeout(Duration::from_secs(c_seconds), c_handle.authenticate_password(c_user, c_password))
        .await.context("SSH 密码认证超时")??.success())
}

async fn authenticate_key(c_handle: &mut client::Handle<SshHandler>, c_user: &str, c_key: Arc<keys::PrivateKey>, c_hash_seconds: u64, c_seconds: u64) -> Result<bool> {
    let z_hash = match tokio::time::timeout(Duration::from_secs(c_hash_seconds), c_handle.best_supported_rsa_hash()).await {
        Ok(Ok(Some(l_hash))) => l_hash,
        _ => None,
    };
    let z_key = PrivateKeyWithHashAlg::new(c_key, z_hash);
    Ok(tokio::time::timeout(Duration::from_secs(c_seconds), c_handle.authenticate_publickey(c_user, z_key))
        .await.context("SSH 公钥认证超时")??.success())
}
fn terminal_size(c_session: &LiveSession) -> (u16, u16) {
    screen_size(c_session.terminal.lock().unwrap_or_else(PoisonError::into_inner).parser.screen())
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
    let z_stdout_bytes = c_task.stdout.len() + c_task.transfer_diagnostics.as_ref().map_or(0, |(_, l_report)| l_report.len() + usize::from(!c_task.stdout.is_empty()));
    json!({"task_id":c_task.task_id,"session_id":c_task.session_id,"connection_id":c_task.connection.id,"created_at_ms":c_task.created_at_ms,"source":c_task.source,"client":c_task.client,"kind":c_task.kind,"label":c_task.label,"local":c_task.local,"started_at_ms":c_task.started_at_ms,"finished_at_ms":c_task.finished_at_ms,"status":task_status(c_task),"bytes":c_task.bytes,"total":c_task.total,"speed_bytes_per_sec":current_speed(c_task),"directory":c_task.directory,"files_done":c_task.files_done,"files_failed":c_task.files_failed,"files_total":c_task.files_total,"file_bytes":c_task.file_bytes,"file_total":c_task.file_total,"main_link":c_task.via_main_link,"stdout_bytes":z_stdout_bytes,"stderr_bytes":c_task.stderr.len(),"truncated":c_task.truncated,"exit_code":c_task.exit_code,"error":c_task.error})
}

fn task_status(c_task: &Task) -> &str {
    if c_task.cancel.is_none() && c_task.finished_at_ms.is_none() {
        "cancelling"
    } else {
        &c_task.status
    }
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

// vt100 0.18 的终端尺寸为 NonZeroU16 (行, 列): 读取时换成 u16, 构造与改尺寸时把 0 按 1 处理.
fn screen_size(c_screen: &vt100::Screen) -> (u16, u16) {
    let (z_rows, z_cols) = c_screen.size();
    (z_rows.get(), z_cols.get())
}
fn non_zero(c_value: u16) -> std::num::NonZeroU16 {
    std::num::NonZeroU16::new(c_value).unwrap_or(std::num::NonZeroU16::MIN)
}

// 0 未开启鼠标上报, 1 传统编码, 2 SGR 编码, 3 UTF-8 编码.
fn mouse_mode(c_screen: &vt100::Screen) -> u8 {
    match (c_screen.mouse_protocol_mode(), c_screen.mouse_protocol_encoding()) {
        (vt100::MouseProtocolMode::None, _) => 0,
        (_, vt100::MouseProtocolEncoding::Sgr) => 2,
        (_, vt100::MouseProtocolEncoding::Utf8) => 3,
        _ => 1,
    }
}

fn mouse_tracking(c_screen: &vt100::Screen) -> u8 {
    use vt100::MouseProtocolMode;
    match c_screen.mouse_protocol_mode() {
        MouseProtocolMode::None => 0,
        MouseProtocolMode::Press => 1,
        MouseProtocolMode::PressRelease => 2,
        MouseProtocolMode::ButtonMotion => 3,
        MouseProtocolMode::AnyMotion => 4,
    }
}

fn epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64)
        .unwrap_or(0)
}
// 单调时钟毫秒 (自进程内首次调用起): 用于时长判断, 不受系统时间调整影响. 从 1 起计, 0 表示从未记录.
fn mono_ms() -> u64 {
    static START: std::sync::LazyLock<Instant> = std::sync::LazyLock::new(Instant::now);
    START.elapsed().as_millis() as u64 + 1
}

// 休眠停表时钟毫秒: macOS 上 Instant 休眠停表; Windows 上 Instant (QPC) 不断表, 改用 QueryUnbiasedInterruptTime.
#[cfg(windows)]
fn awake_ms() -> u64 {
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn QueryUnbiasedInterruptTime(c_time: *mut u64) -> i32;
    }
    let mut z_ticks = 0u64;
    if unsafe { QueryUnbiasedInterruptTime(&raw mut z_ticks) } != 0 {
        z_ticks / 10_000 + 1
    } else {
        mono_ms()
    }
}

#[cfg(not(windows))]
fn awake_ms() -> u64 {
    mono_ms()
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
// 按实测速率限制在途请求数: 高速线路很快放开到上限, 极慢或骤降线路优先按近 2 秒速率控制在 c_queue_seconds 秒排队量内, 避免后排请求假超时.
fn transfer_target_window(
    c_window: usize,
    c_max_window: usize,
    c_done_bytes: u64,
    c_elapsed: Duration,
    c_recent_rate: Option<f64>,
    c_chunk_size: usize,
    c_queue_seconds: f64,
) -> usize {
    if c_done_bytes < 256 * 1024 || c_elapsed < Duration::from_millis(500) {
        return c_window.min(c_max_window);
    }
    let z_rate = c_recent_rate.unwrap_or_else(|| c_done_bytes as f64 / c_elapsed.as_secs_f64());
    let z_cap = (z_rate * c_queue_seconds / c_chunk_size.max(1) as f64) as usize;
    c_window.min(z_cap.clamp(4, c_max_window))
}

// 从远端句柄按偏移读满指定字节数 (或遇 EOF 提前结束), 容忍服务端短读; 整体受 c_timeout 限制.
async fn read_remote_exact(
    c_raw: &RawSftpSession,
    c_handle: &str,
    c_offset: u64,
    c_len: usize,
    c_timeout: Duration,
) -> Result<Vec<u8>> {
    use russh_sftp::{client::error::Error as SftpError, protocol::StatusCode};
    let z_deadline = tokio::time::Instant::now() + c_timeout;
    let mut z_out = Vec::with_capacity(c_len);
    while z_out.len() < c_len {
        let l_remaining = z_deadline.saturating_duration_since(tokio::time::Instant::now());
        if l_remaining.is_zero() {
            return Err(std::io::Error::new(std::io::ErrorKind::ConnectionAborted, "读取远端文件超时").into());
        }
        let l_want = (c_len - z_out.len()).min(DOWNLOAD_CHUNK) as u32;
        let l_reply = tokio::time::timeout(l_remaining, c_raw.read(c_handle, c_offset + z_out.len() as u64, l_want))
            .await
            .map_err(|err| anyhow::Error::new(err).context("读取远端文件超时"))?;
        match l_reply {
            Ok(l_data) if l_data.data.is_empty() => break,
            Ok(l_data) => {
                ensure!(l_data.data.len() <= l_want as usize, "服务器返回的数据块长度超出请求范围");
                z_out.extend_from_slice(&l_data.data);
            }
            Err(SftpError::Status(l_status)) if l_status.status_code == StatusCode::Eof => break,
            Err(err) => return Err(anyhow::Error::from(err).context("读取远端文件失败")),
        }
    }
    Ok(z_out)
}

async fn prefetch_upload(
    mut c_source: impl tokio::io::AsyncRead + Unpin,
    c_bytes: u64,
    c_chunks: &mpsc::Sender<Result<Vec<u8>>>,
) -> Result<()> {
    let mut z_buffer = vec![0; c_bytes.min((UPLOAD_CHUNK * 16) as u64) as usize];
    let mut z_remaining = c_bytes;
    // 首批最多读一个包, 后续按批读盘. 磁盘短读时直接发已读到的字节.
    let mut z_batch = UPLOAD_CHUNK;
    while z_remaining > 0 {
        let l_target = z_remaining.min(z_batch as u64) as usize;
        let l_read = tokio::time::timeout(TRANSFER_STALL, c_source.read(&mut z_buffer[..l_target]))
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "读取本地文件超时"))?
            .context("读取本地文件失败")?;
        ensure!(l_read != 0, "上传源在传输期间被截断");
        for l_chunk in z_buffer[..l_read].chunks(UPLOAD_CHUNK) {
            if c_chunks.send(Ok(l_chunk.to_vec())).await.is_err() {
                return Ok(());
            }
        }
        z_remaining -= l_read as u64;
        z_batch = z_buffer.len();
    }
    let z_extra = tokio::time::timeout(TRANSFER_STALL, c_source.read(&mut [0]))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "读取本地文件超时"))?
        .context("读取本地文件失败")?;
    ensure!(z_extra == 0, "上传源在传输期间被修改");
    Ok(())
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
    use russh_sftp::{client::error::Error as SftpError, protocol::{FileAttributes, Packet, StatusCode}};
    // 提交结果无法确认时保留临时文件 (里面是新数据), 失败处理不删除.
    let mut z_keep_part = false;
    let z_result = async {
        // 覆盖时尽量沿用旧文件普通权限位 (0777, 避开 SUID/SGID); 不支持 SETSTAT 或受限文件系统明确拒绝时不中止整次提交.
        if let Some(l_permissions) = c_permissions {
            let l_attrs = FileAttributes { permissions: Some(l_permissions & 0o0777), ..FileAttributes::default() };
            let l_set = tokio::time::timeout(Duration::from_secs(60), c_raw.setstat(c_part.clone(), l_attrs))
                .await
                .context("设置远端文件权限超时")?;
            if let Err(err) = l_set && !sftp_definite(&err) {
                return Err(anyhow::Error::from(err).context("设置远端文件权限失败"));
            }
        }
        let z_rename = async |c_from: String, c_to: String| {
            tokio::time::timeout(Duration::from_secs(60), c_raw.rename(c_from, c_to))
                .await
                .context("远端文件改名超时")?
                .map(|_| ())
                .context("远端文件改名失败")
        };
        if c_overwrite && c_exists && !c_posix_rename {
            // 不支持原子替换: 旧文件先改为备份名, 新文件就位后删除备份; 失败时恢复旧文件.
            // 改名超时或回包丢失时结果未知, 按实际文件状态判断 (用 lstat 识别悬空符号链接): Some(是否存在), 查不到为 None.
            let l_backup = format!("{c_part}.old");
            let z_present = async |c_path: String| {
                match tokio::time::timeout(Duration::from_secs(30), c_raw.lstat(c_path)).await {
                    Ok(Ok(_)) => Some(true),
                    Ok(Err(SftpError::Status(l_status))) if l_status.status_code == StatusCode::NoSuchFile => Some(false),
                    _ => None,
                }
            };
            // 状态无法确认 (多为连接已断) 或旧文件恢复失败时保留临时文件, 并在错误里写明旧文件与新文件所在, 由用户核对.
            let z_unknown = |c_err: anyhow::Error| c_err.context(format!("连接异常, 无法确认提交结果: 旧文件可能已改名为 {l_backup}, 新文件保留在 {c_part}, 请核对"));
            if let Err(err) = z_rename(c_remote.clone(), l_backup.clone()).await {
                match (z_present(c_remote.clone()).await, z_present(l_backup.clone()).await) {
                    // 改名其实没有生效 (目标还在, 备份不存在). 两者都在时可能是旧文件已移走、目标又被别处新建, 按无法确认处理.
                    (Some(true), Some(false)) => return Err(err),
                    // 旧文件其实已改为备份名: 先恢复回去, 不让正式文件消失.
                    (Some(false), Some(true)) => {
                        if let Err(l_restore) = z_rename(l_backup.clone(), c_remote.clone()).await {
                            z_keep_part = true;
                            return Err(z_unknown(err.context(format!("恢复旧文件失败: {l_restore:#}"))));
                        }
                        return Err(err);
                    }
                    _ => {
                        z_keep_part = true;
                        return Err(z_unknown(err));
                    }
                }
            }
            if let Err(err) = z_rename(c_part.clone(), c_remote.clone()).await {
                match (z_present(c_part.clone()).await, z_present(c_remote.clone()).await) {
                    // 临时文件已不在而目标存在: 新文件多半已就位, 但路径存在证明不了目标就是本任务的文件 (可能被别处改写),
                    // 不删备份, 报告结果未知.
                    (Some(false), Some(true)) => {
                        return Err(err.context(format!("无法确认提交结果: 新文件可能已就位于 {c_remote}, 旧文件备份保留在 {l_backup}, 请核对后自行删除备份")));
                    }
                    // 新文件未就位: 恢复旧文件并报告恢复结果.
                    (Some(true), Some(false)) => {
                        if let Err(l_restore) = z_rename(l_backup.clone(), c_remote.clone()).await {
                            z_keep_part = true;
                            return Err(z_unknown(err.context(format!("恢复旧文件失败: {l_restore:#}"))));
                        }
                        return Err(err);
                    }
                    _ => {
                        z_keep_part = true;
                        return Err(z_unknown(err));
                    }
                }
            }
            let _ = tokio::time::timeout(Duration::from_secs(60), c_raw.remove(l_backup)).await;
            return Ok(());
        }
        let z_renamed = if c_overwrite && c_posix_rename {
            // OpenSSH 扩展允许原子覆盖. 普通改名仍由服务器检查同名冲突.
            let mut l_data = Vec::with_capacity(c_part.len() + c_remote.len() + 8);
            for l_path in [&c_part, &c_remote] {
                l_data.extend_from_slice(&(l_path.len() as u32).to_be_bytes());
                l_data.extend_from_slice(l_path.as_bytes());
            }
            tokio::time::timeout(Duration::from_secs(60), c_raw.extended("posix-rename@openssh.com", l_data))
                .await
                .context("远端文件改名超时")
                .and_then(|c| match c? {
                    Packet::Status(l_status) if l_status.status_code == StatusCode::Ok => Ok(()),
                    Packet::Status(l_status) => Err(SftpError::Status(l_status).into()),
                    _ => Err(SftpError::UnexpectedPacket.into()),
                })
        } else {
            z_rename(c_part.clone(), c_remote.clone()).await
        };
        if let Err(err) = z_renamed {
            // 服务器明确拒绝时照常清理. 超时或断线时保留临时文件, 供核对提交结果.
            if err.chain().any(|c| c.downcast_ref::<SftpError>().is_some_and(sftp_definite)) {
                return Err(err.context("远端目标已存在或无法改名"));
            }
            z_keep_part = true;
            return Err(err.context(format!("连接异常, 无法确认提交结果: 新文件可能已就位于 {c_remote}, 也可能仍在 {c_part}, 请核对")));
        }
        Ok(())
    }
    .await;
    if z_result.is_err() && !z_keep_part {
        let _ = tokio::time::timeout(Duration::from_secs(10), c_raw.remove(c_part)).await;
    }
    z_result
}
// 服务器明确答复的 SFTP 状态 (无权限、已存在、磁盘满等); 连接中断类状态不算, 超时与通道错误也不算.
fn sftp_definite(c_err: &russh_sftp::client::error::Error) -> bool {
    use russh_sftp::protocol::StatusCode;
    matches!(c_err, russh_sftp::client::error::Error::Status(l_status) if !matches!(l_status.status_code, StatusCode::NoConnection | StatusCode::ConnectionLost))
}
// 按本次操作返回的错误重试. 同一标签上的另一条连接可能同时断开, 与本次错误无关.
fn sftp_retryable(c_err: &anyhow::Error) -> bool {
    use russh_sftp::client::error::Error as SftpError;
    let z_network = |c: &SftpError| !sftp_definite(c) && !matches!(c, SftpError::Limited(_));
    c_err.chain().any(|c| {
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
// 取不到修改时间时无法判断源文件是否变过, 不续传, 每次从头开始.
fn resume_offset(c_resume: &mut (u64, Option<(u64, u64)>), c_owned: bool, c_size: u64, c_mtime: Option<u64>) -> Result<u64> {
    let z_source = c_mtime.map(|c| (c_size, c));
    if c_owned && c_resume.0 > 0 && z_source.is_some() {
        ensure!(c_resume.1 == z_source, "源文件在中断期间被修改, 无法续传");
        return Ok(c_resume.0);
    }
    *c_resume = (0, z_source);
    Ok(0)
}
// 遍历本机文件夹, 返回待建的远端目录 (根在最前)、文件列表 (本机路径, 远端路径, 大小) 与跳过项说明.
fn scan_local_directory(c_local: &Path, c_remote: &str, c_task: &Mutex<Task>) -> Result<(Vec<String>, Vec<(PathBuf, String, u64)>, String)> {
    const MAX_DIRECTORY_DEPTH: usize = 64;
    let z_cancelled = || c_task.lock().unwrap_or_else(PoisonError::into_inner).cancel.is_none();
    let mut z_dirs = vec![c_remote.to_owned()];
    let mut z_files = Vec::new();
    let mut z_notes = String::new();
    let mut z_pending = vec![(c_local.to_path_buf(), c_remote.to_owned(), 0usize)];
    while let Some((l_local, l_remote, l_depth)) = z_pending.pop() {
        ensure!(!z_cancelled(), "任务已取消");
        let l_entries = match std::fs::read_dir(&l_local) {
            Ok(c) => c,
            Err(err) if l_local == c_local => return Err(anyhow::Error::from(err).context("读取上传文件夹失败")),
            Err(err) => {
                directory_note(&mut z_notes, format!("跳过 {}: {err}", l_local.display()));
                continue;
            }
        };
        for (i, l_entry) in l_entries.enumerate() {
            // spawn_blocking 一旦开始不能中止, 目录扫描主动检查取消, 大目录也能尽快释放线程.
            if i > 0 && i % 128 == 0 {
                ensure!(!z_cancelled(), "任务已取消");
            }
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
                if l_depth >= MAX_DIRECTORY_DEPTH {
                    directory_note(&mut z_notes, format!("跳过 {}: 目录层级超过 {MAX_DIRECTORY_DEPTH} 层", l_path.display()));
                    continue;
                }
                z_dirs.push(l_target.clone());
                z_pending.push((l_path, l_target, l_depth + 1));
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

async fn create_local_transfer_dir(c_path: &Path, c_reuse: bool) -> Result<()> {
    match tokio::fs::create_dir(c_path).await {
        Ok(()) => Ok(()),
        Err(err) if c_reuse => {
            // 按路径本身检查类型, 避免沿符号链接或 Windows 目录联接写入其他位置.
            let l_metadata = tokio::fs::symlink_metadata(c_path).await.map_err(|_| err)?;
            ensure!(l_metadata.is_dir(), "本地目标必须是普通目录");
            Ok(())
        }
        Err(err) => Err(err.into()),
    }
}

// 读出远端目录的全部条目, 并返回目录句柄是否已确认关闭; 打开失败原样返回, 由调用方区分权限问题和连接问题.
async fn read_remote_directory(c_raw: &russh_sftp::client::RawSftpSession, c_path: &str) -> Result<(Vec<russh_sftp::protocol::File>, bool)> {
    let z_handle = tokio::time::timeout(Duration::from_secs(15), c_raw.opendir(c_path.to_owned()))
        .await
        .context("打开远端目录超时")??
        .handle;
    let mut z_entries = Vec::new();
    let z_result = read_directory_pages(c_raw, &z_handle, Duration::from_secs(60), |c_files| {
        ensure!(z_entries.len() + c_files.len() <= MAX_DIRECTORY_ITEMS, "文件夹超过 {MAX_DIRECTORY_ITEMS} 项, 请分批传输");
        z_entries.extend(c_files);
        Ok(false)
    }).await;
    let z_closed = matches!(tokio::time::timeout(Duration::from_secs(10), c_raw.close(z_handle)).await, Ok(Ok(_)));
    z_result.map(|_| (z_entries, z_closed))
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
    // 设备保留名 (CON、NUL、COM1 等, 带扩展名或点前尾随空格如 "CON .txt" 也算) 会写到设备而不是文件, 加前缀避开.
    let z_stem = z_trimmed.split('.').next().unwrap_or_default().trim_end_matches(' ').to_ascii_uppercase();
    // 上标 ¹²³ 也算 (COM¹、LPT² 等).
    let z_reserved = matches!(z_stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || matches!(z_stem.strip_prefix("COM").or_else(|| z_stem.strip_prefix("LPT")), Some("1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "¹" | "²" | "³"));
    if z_reserved { format!("_{z_trimmed}") } else { z_trimmed.to_owned() }
}
// 判断本机路径是否位于 c_dir 之内 (用于隔离 MCP 访问 HiPShell 配置目录).
// 先尝试整路径 canonicalize; 若目标尚未创建则逐分量推进, 已存在前缀先 canonicalize 再处理后续 .. (避免先词法折叠绕过符号链接),
// 并兼容 Windows \\?\UNC\ 与 \\?\ 前缀以及 macOS/Windows 大小写不敏感比较.
fn path_in_dir(c_path: &Path, c_dir: &Path) -> bool {
    let z_strip_verbatim = |c_p: PathBuf| -> PathBuf {
        #[cfg(windows)]
        if let Some(l_s) = c_p.to_str() {
            if let Some(l_unc) = l_s.strip_prefix(r"\\?\UNC\") {
                return PathBuf::from(format!(r"\\{l_unc}"));
            }
            if let Some(l_rest) = l_s.strip_prefix(r"\\?\") {
                return PathBuf::from(l_rest);
            }
        }
        c_p
    };
    let z_normalize = |c_input: &Path| -> PathBuf {
        if let Ok(l_canon) = c_input.canonicalize() {
            return z_strip_verbatim(l_canon);
        }
        let mut l_out = PathBuf::new();
        for l_comp in c_input.components() {
            match l_comp {
                std::path::Component::CurDir => {}
                std::path::Component::ParentDir => {
                    if let Ok(l_canon) = l_out.canonicalize() {
                        l_out = z_strip_verbatim(l_canon);
                    }
                    l_out.pop();
                }
                l_other => {
                    l_out.push(l_other.as_os_str());
                    if let Ok(l_canon) = l_out.canonicalize() {
                        l_out = z_strip_verbatim(l_canon);
                    }
                }
            }
        }
        z_strip_verbatim(l_out)
    };
    let z_target = z_normalize(c_path);
    let z_root = z_normalize(c_dir);
    let mut z_t_iter = z_target.components();
    for l_r in z_root.components() {
        let Some(l_t) = z_t_iter.next() else {
            return false;
        };
        if local_name_key(&l_r.as_os_str().to_string_lossy()) != local_name_key(&l_t.as_os_str().to_string_lossy()) {
            return false;
        }
    }
    true
}
// 本机同名比较键: Windows 与 macOS 默认文件系统不区分大小写, Readme 与 README 指向同一文件, 按小写比较;
// macOS 还把 é 的组合与分解两种编码视为同名, 先统一为分解形式.
pub(crate) fn local_name_key(c_name: &str) -> String {
    #[cfg(all(target_os = "macos", feature = "desktop"))]
    let c_name = icu_normalizer::DecomposingNormalizerBorrowed::new_nfd().normalize(c_name);
    if cfg!(any(windows, target_os = "macos")) { c_name.to_lowercase() } else { c_name.to_string() }
}
// 临时文件与目标同目录, 隐藏文件名带任务标识: ".名称.任务前8位.hipssh-part", 同名并发任务互不干扰.
// 本任务临时文件的归属, 取消安全: 创建在独立任务里完成并自行登记结果, 调用方被取消时结果仍会落到这里.
// 只有确认由本任务创建的文件才会被清理或截断; 结果未知 (超时、连接中断) 的文件不碰, 重试换用新名字.
struct PartOwner {
    // PART_NONE / PART_OWNED / PART_PENDING / PART_UNKNOWN.
    state: std::sync::atomic::AtomicU8,
    // 临时文件名代次, 结果未知时加一.
    round: std::sync::atomic::AtomicU32,
    // 任务已结束 (失败或取消后不再等待创建结果): 之后才成功的创建由创建任务自行删除.
    abandoned: AtomicBool,
    // 因结果未知而换名放弃的代次: 这些名字的文件可能遗留, 任务结束时报告.
    lost: Mutex<Vec<u32>>,
}
// 文件夹上传根目录的创建结果: 未创建 / 本任务已建成 / 中断时结果未知.
const ROOT_NONE: u8 = 0;
const ROOT_MADE: u8 = 1;
const ROOT_UNKNOWN: u8 = 2;
const PART_NONE: u8 = 0;
const PART_OWNED: u8 = 1;
const PART_PENDING: u8 = 2;
const PART_UNKNOWN: u8 = 3;
impl PartOwner {
    fn new() -> Arc<Self> {
        Arc::new(Self { state: std::sync::atomic::AtomicU8::new(PART_NONE), round: std::sync::atomic::AtomicU32::new(0), abandoned: AtomicBool::new(false), lost: Mutex::new(Vec::new()) })
    }
    fn owned(&self) -> bool {
        self.state.load(Ordering::Acquire) == PART_OWNED
    }
    // 当前代次的临时文件标识 (任务标识前 8 位, 换过名字时加代次).
    fn tag(&self, c_task: &Arc<Mutex<Task>>) -> String {
        part_tag(c_task, self.round.load(Ordering::Acquire))
    }
    // 每次尝试开始时调用: 上次创建结果未知或仍在进行时换用新名字并从头开始, 旧文件留在原处不清理 (记下以便报告).
    fn settle(&self) {
        if matches!(self.state.load(Ordering::Acquire), PART_PENDING | PART_UNKNOWN) {
            let z_old = self.round.fetch_add(1, Ordering::AcqRel);
            self.lost.lock().unwrap_or_else(PoisonError::into_inner).push(z_old);
            self.state.store(PART_NONE, Ordering::Release);
        }
    }
    // 可能遗留的临时文件标识: 换名放弃的各代次, c_current 为真时加上当前代次.
    fn unknown_tags(&self, c_task: &Arc<Mutex<Task>>, c_current: bool) -> Vec<String> {
        let mut z_rounds = self.lost.lock().unwrap_or_else(PoisonError::into_inner).clone();
        if c_current {
            z_rounds.push(self.round.load(Ordering::Acquire));
        }
        z_rounds.into_iter().map(|c| part_tag(c_task, c)).collect()
    }
    // 创建任务登记结果: 期间已换名时不改状态 (旧名字的结果与当前文件无关).
    // 返回创建任务是否应自行删除刚建成的文件: 已换名 (这个文件不会再被使用) 或任务已结束 (没人再清理).
    // 与 abandon 交接清理责任: 两边都按 SeqCst 先写自己的标记再读对方的, 至少一方看到对方 (见 abandon).
    fn record(&self, c_gen: u32, c_state: u8) -> bool {
        let z_current = self.round.load(Ordering::Acquire) == c_gen;
        if z_current {
            self.state.store(c_state, Ordering::SeqCst);
        }
        c_state == PART_OWNED && (!z_current || self.abandoned.load(Ordering::SeqCst))
    }
    // 任务失败或取消时只短暂等待创建结果. 随后创建成功的由创建任务接手清理.
    // PART_OWNED: 由调用方删除; PART_PENDING / PART_UNKNOWN: 调用方按可能遗留报告. 此后才成功的创建由创建任务自行删除,
    // 它的删除结果无人等待, 但这个名字已按快照报告过, 不会漏报; 两边都删也无妨.
    async fn abandon(&self) -> u8 {
        let z_deadline = Instant::now() + Duration::from_secs(2);
        while self.state.load(Ordering::Acquire) == PART_PENDING && Instant::now() < z_deadline {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        self.abandoned.store(true, Ordering::SeqCst);
        self.state.load(Ordering::SeqCst)
    }
}
fn part_tag(c_task: &Arc<Mutex<Task>>, c_round: u32) -> String {
    let z_base = c_task.lock().unwrap_or_else(PoisonError::into_inner).task_id.chars().take(8).collect::<String>();
    match c_round {
        0 => z_base,
        l_round => format!("{z_base}-{l_round}"),
    }
}
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
async fn remove_remote_part(c_raw: &RawSftpSession, c_path: String, c_handle: Option<String>) -> bool {
    use russh_sftp::{client::error::Error, protocol::StatusCode};
    if let Some(l_handle) = c_handle {
        let _ = tokio::time::timeout(Duration::from_secs(2), c_raw.close(l_handle)).await;
    }
    match c_raw.remove(c_path).await {
        Ok(_) => true,
        Err(Error::Status(l_status)) => l_status.status_code == StatusCode::NoSuchFile,
        Err(_) => false,
    }
}
// Windows 上杀毒软件、索引或同步服务可能占用刚关闭的文件, 改名和删除都需重试共享冲突.
async fn retry_local_file_change<F: std::future::Future<Output = std::io::Result<()>>>(mut c_operation: impl FnMut() -> F) -> std::io::Result<()> {
    let mut i = 0;
    loop {
        match c_operation().await {
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
    // 网络堵塞超过 3 秒时不再等待, 后台任务继续发送挂断与关闭, 网络恢复后仍会终止远端命令.
    async fn shutdown(&mut self) {
        if let Some(l_task) = self.0.take().map(close_channel) {
            let _ = tokio::time::timeout(Duration::from_secs(3), l_task).await;
        }
    }
}
impl Drop for ClosingChannel {
    fn drop(&mut self) {
        if let Some(l_channel) = self.0.take() {
            close_channel(l_channel);
        }
    }
}
// 先丢弃读取端再发关闭: russh 主循环把收到的数据逐条 await 送入通道的有界队列, 关闭期间远端仍在输出 (如监控脚本每秒一次)
// 而读取端无人读取时, 队列满后整条 SSH 连接 (终端、SFTP、保活) 都会卡住; 而关闭消息又要主循环处理才能发出, 形成死锁.
// 读取端释放后 russh 投递失败直接丢弃数据, 主循环不受影响.
fn close_channel(c_channel: russh::Channel<client::Msg>) -> tokio::task::JoinHandle<()> {
    let (_, z_writer) = c_channel.split();
    // 出站被卡住时消息一直入不了队: 最多等 30 秒就放弃, 不让后台任务长期滞留 (连接本身由探活或断开流程回收).
    tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(30), async {
            let _ = z_writer.signal(russh::Sig::HUP).await;
            let _ = z_writer.eof().await;
            let _ = z_writer.close().await;
        })
        .await;
    })
}

// 打开会话通道. 超时放弃后服务器仍可能随后打开该通道, 若无人关闭会一直占用服务器的会话名额
// (OpenSSH MaxSessions 默认 10), 积满后新通道全部失败; 因此超时后由后台任务等待并关闭迟到的通道.
async fn open_channel(c_handle: &Arc<client::Handle<SshHandler>>, c_seconds: u64) -> Result<russh::Channel<client::Msg>> {
    // 通道在任务内即由守卫持有: 超时或外层被取消而放弃等待时, 任务结果随之丢弃, 迟到的通道自动关闭.
    // 后台不设上限: 请求可能已发出, 中途放弃等待后服务器的确认再到达时 russh 会确认通道但无处投递, 应用拿不到它去关闭,
    // 服务器的会话名额会一直被占用. 因此一直等到确认 (由守卫关闭) 或连接结束; 卡住的连接由探活和退役流程关闭套接字, 任务随之结束.
    // 服务器回应保活却长期不答复通道请求时: 未答复请求较多就不再发出新请求, 等待任务不会无限累积; 独立连接的探活也据此判定失败.
    let z_key = Arc::as_ptr(c_handle) as usize;
    if stale_opens(z_key, Duration::from_secs(30)) >= 4 {
        // 按连接层错误返回, 传输据此重试并对这条连接探活.
        return Err(anyhow::Error::new(russh::Error::SendError).context("服务器长时间未答复通道打开请求"));
    }
    let z_started = Instant::now();
    PENDING_OPENS.lock().unwrap_or_else(PoisonError::into_inner).entry(z_key).or_default().push(z_started);
    let z_handle = c_handle.clone();
    let z_open = tokio::spawn(async move {
        let l_result = z_handle.channel_open_session().await.map(|c| ClosingChannel(Some(c)));
        let mut l_pending = PENDING_OPENS.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(l_list) = l_pending.get_mut(&z_key) {
            if let Some(i) = l_list.iter().position(|c| *c == z_started) {
                l_list.swap_remove(i);
            }
            if l_list.is_empty() {
                l_pending.remove(&z_key);
            }
        }
        l_result
    });
    let mut z_guard = tokio::time::timeout(Duration::from_secs(c_seconds), z_open)
        .await
        // 保留超时错误本身, 传输据此判断可以重试.
        .map_err(|err| anyhow::Error::new(err).context(format!("等待服务器响应超时 ({c_seconds} 秒)")))?
        .context("打开通道任务异常")??;
    z_guard.0.take().context("通道已释放")
}
// 各连接 (按对象地址) 尚未得到服务器答复的通道打开请求的发出时间. 等待任务持有连接引用, 记录存在期间地址不会被复用.
static PENDING_OPENS: std::sync::LazyLock<Mutex<HashMap<usize, Vec<Instant>>>> = std::sync::LazyLock::new(Mutex::default);
// 连接上发出已超过 c_age 仍未答复的通道打开请求数.
fn stale_opens(c_key: usize, c_age: Duration) -> usize {
    PENDING_OPENS.lock().unwrap_or_else(PoisonError::into_inner).get(&c_key).map_or(0, |c| c.iter().filter(|l| l.elapsed() >= c_age).count())
}
fn channel_open_refused(c_err: &anyhow::Error) -> bool {
    c_err.chain().any(|c| matches!(c.downcast_ref::<russh::Error>(), Some(russh::Error::ChannelOpenFailure(_))))
}
// 打开 SFTP 子系统通道. 守卫一直持有到子系统启动成功: 等待期间超时或被取消时通道随之关闭, 不占用服务器会话名额.
async fn open_sftp_stream(c_handle: &Arc<client::Handle<SshHandler>>) -> Result<SftpStream> {
    // 多任务并发建通道或经代理线路繁忙时控制耗时在 12 秒内, 失败原因一并报出.
    let mut z_channel = ClosingChannel(Some(open_channel(c_handle, 12).await.context("打开 SFTP 通道失败")?));
    // request_subsystem 只代表请求已入队; 等到服务器明确答复, 拒绝时在这里报错, 调用方的回退逻辑才能生效.
    tokio::time::timeout(Duration::from_secs(12), async {
        z_channel.request_subsystem(true, "sftp").await?;
        loop {
            match z_channel.wait().await {
                Some(ChannelMsg::Success) => return anyhow::Ok(()),
                Some(ChannelMsg::Failure) => bail!("服务器拒绝启动 SFTP 子系统"),
                Some(ChannelMsg::Eof | ChannelMsg::Close) | None => {
                    return Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "启动 SFTP 时通道已关闭").into());
                }
                Some(_) => {}
            }
        }
    })
    .await
    .context("启动SFTP超时")??;
    Ok(SftpStream::new(z_channel.0.take().context("通道已释放")?, c_handle.clone()))
}

// SFTP 通道流. russh 的 ChannelStream 读写共用一个对象, russh-sftp 读取任务异常退出后写任务仍持有它, 读取端不释放也无人读取,
// 收到的数据塞满通道队列后 russh 主循环停止收包, 整条 SSH 连接 (终端、监控、保活) 随之卡住.
// 这里由后台任务持续读取通道转入队列: 流被丢弃时关闭通道; 队列 5 秒无人取走视为读取方已退出 (正常读取方逐包转交, 不会积压),
// 释放读取端并关闭通道, russh 直接丢弃后续数据, 整条连接最多受阻约 5 秒.
// 通道结束后写端可能还停在等待对端窗口: 由 closed 唤醒并报错; 单次写入 60 秒无进展也报错, 写任务随之退出, 不长期滞留.
struct SftpStream {
    rx: mpsc::Receiver<Bytes>,
    pending: Bytes,
    tx: std::pin::Pin<Box<dyn tokio::io::AsyncWrite + Send>>,
    // (通道是否已结束, 等待中的写端唤醒器).
    closed: Arc<Mutex<(bool, Option<std::task::Waker>)>>,
    alive: Arc<AtomicBool>,
    stall: Option<std::pin::Pin<Box<tokio::time::Sleep>>>,
    // 所在连接的引用, 随 SFTP 会话 (含浏览缓存、空闲池、建目录) 存活: 独立连接退役时按引用计数判断是否仍有使用者 (见 retire_transfer).
    _link: Arc<client::Handle<SshHandler>>,
}
impl SftpStream {
    fn new(c_channel: russh::Channel<client::Msg>, c_link: Arc<client::Handle<SshHandler>>) -> Self {
        let z_tx = Box::pin(c_channel.make_writer());
        let (mut z_reader, z_writer) = c_channel.split();
        // 扩大收包缓冲容量并放宽消费超时: 多块并发下载偶遇本机磁盘刷盘抖动时不误断通道.
        let (z_queue, z_rx) = mpsc::channel(256);
        let z_closed = Arc::new(Mutex::new((false, None::<std::task::Waker>)));
        let z_alive = Arc::new(AtomicBool::new(true));
        let z_pump_closed = z_closed.clone();
        let z_pump_alive = z_alive.clone();
        tokio::spawn(async move {
            loop {
                let l_message = tokio::select! {
                    l_message = z_reader.wait() => l_message,
                    _ = z_queue.closed() => break,
                };
                match l_message {
                    Some(ChannelMsg::Data { data }) if !data.is_empty() => {
                        if !matches!(tokio::time::timeout(Duration::from_secs(15), z_queue.send(data)).await, Ok(Ok(()))) {
                            break;
                        }
                    }
                    Some(ChannelMsg::Eof | ChannelMsg::Close) | None => break,
                    Some(_) => {}
                }
            }
            // 先释放读取端与队列 (读取方随即读到结束), 唤醒等待中的写端, 再关闭通道释放服务器会话名额.
            drop(z_reader);
            drop(z_queue);
            z_pump_alive.store(false, Ordering::Release);
            let l_waker = {
                let mut l_closed = z_pump_closed.lock().unwrap_or_else(PoisonError::into_inner);
                l_closed.0 = true;
                l_closed.1.take()
            };
            if let Some(l_waker) = l_waker { l_waker.wake(); }
            let _ = tokio::time::timeout(Duration::from_secs(30), z_writer.close()).await;
        });
        Self { rx: z_rx, pending: Bytes::new(), tx: z_tx, closed: z_closed, alive: z_alive, stall: None, _link: c_link }
    }

    fn alive(&self) -> Arc<AtomicBool> {
        self.alive.clone()
    }
}
impl tokio::io::AsyncRead for SftpStream {
    fn poll_read(mut self: std::pin::Pin<&mut Self>, c_cx: &mut std::task::Context<'_>, c_buf: &mut tokio::io::ReadBuf<'_>) -> std::task::Poll<std::io::Result<()>> {
        while self.pending.is_empty() {
            match self.rx.poll_recv(c_cx) {
                std::task::Poll::Ready(Some(l_data)) => self.pending = l_data,
                // 队列结束即通道结束, 不填数据表示 EOF.
                std::task::Poll::Ready(None) => return std::task::Poll::Ready(Ok(())),
                std::task::Poll::Pending => return std::task::Poll::Pending,
            }
        }
        let z_count = self.pending.len().min(c_buf.remaining());
        c_buf.put_slice(&self.pending[..z_count]);
        self.pending.advance(z_count);
        std::task::Poll::Ready(Ok(()))
    }
}
impl tokio::io::AsyncWrite for SftpStream {
    fn poll_write(mut self: std::pin::Pin<&mut Self>, c_cx: &mut std::task::Context<'_>, c_buf: &[u8]) -> std::task::Poll<std::io::Result<usize>> {
        let z_this = &mut *self;
        {
            let mut l_closed = z_this.closed.lock().unwrap_or_else(PoisonError::into_inner);
            if l_closed.0 {
                return std::task::Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "SFTP 通道已结束")));
            }
            l_closed.1 = Some(c_cx.waker().clone());
        }
        match z_this.tx.as_mut().poll_write(c_cx, c_buf) {
            std::task::Poll::Pending => {
                let l_stall = z_this.stall.get_or_insert_with(|| Box::pin(tokio::time::sleep(Duration::from_secs(60))));
                if l_stall.as_mut().poll(c_cx).is_ready() {
                    z_this.stall = None;
                    // 按网络错误归类 (可重连续传), 与本机磁盘超时 (TimedOut, 不重试) 区分.
                    return std::task::Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::ConnectionAborted, "SFTP 写入 60 秒无进展")));
                }
                std::task::Poll::Pending
            }
            l_ready => {
                z_this.stall = None;
                l_ready
            }
        }
    }
    fn poll_flush(mut self: std::pin::Pin<&mut Self>, c_cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        self.tx.as_mut().poll_flush(c_cx)
    }
    // 关闭写端 (发送 EOF) 同样受通道结束通知和 60 秒停滞限制: russh-sftp 收尾时会等它完成, 出站卡住时不能一直悬着.
    fn poll_shutdown(mut self: std::pin::Pin<&mut Self>, c_cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        let z_this = &mut *self;
        {
            let mut l_closed = z_this.closed.lock().unwrap_or_else(PoisonError::into_inner);
            if l_closed.0 {
                return std::task::Poll::Ready(Ok(()));
            }
            l_closed.1 = Some(c_cx.waker().clone());
        }
        match z_this.tx.as_mut().poll_shutdown(c_cx) {
            std::task::Poll::Pending => {
                let l_stall = z_this.stall.get_or_insert_with(|| Box::pin(tokio::time::sleep(Duration::from_secs(60))));
                if l_stall.as_mut().poll(c_cx).is_ready() {
                    z_this.stall = None;
                    return std::task::Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::ConnectionAborted, "SFTP 关闭 60 秒无进展")));
                }
                std::task::Poll::Pending
            }
            l_ready => {
                z_this.stall = None;
                l_ready
            }
        }
    }
}
// 取最近放回的可用会话; 连接已断、通道已结束或放置过久的直接丢弃.
// 若独立连接已闲置 >= 30 秒, 先快速探活 (10 秒超时), 失败直接退役该连接, 避免后续 transfer_handle 再次等待.
async fn take_idle_sftp(c_session: &Arc<LiveSession>) -> Option<(SshLink, Arc<AtomicBool>, (Arc<RawSftpSession>, russh_sftp::protocol::Version))> {
    let mut z_candidate = None;
    {
        let mut z_idle = c_session.idle_sftp.lock().unwrap_or_else(PoisonError::into_inner);
        while let Some((l_link, l_saved, l_alive, l_sftp)) = z_idle.pop() {
            if !l_link.handle.is_closed() && l_alive.load(Ordering::Acquire) && l_saved.elapsed() < TRANSFER_IDLE {
                z_candidate = Some((l_link, l_alive, l_sftp));
                break;
            }
        }
    }
    let (z_link, z_alive, z_sftp) = z_candidate?;
    let z_key = z_link.key();
    if z_link.transfer_idle() {
        // 与建连共用同一把锁, 一次探活成功后, 同时取空闲会话的其他任务可直接复用.
        let mut l_slot = c_session.transfer.lock().await;
        if !l_slot.0.as_ref().is_some_and(|c| c.key() == z_key) {
            return None;
        }
        if z_link.transfer_idle() && !z_link.probe_transfer().await {
            drop(z_sftp);
            l_slot.0 = None;
            retire_transfer(c_session, z_link);
            return None;
        }
    }
    if *c_session.transfer_suspect.lock().unwrap_or_else(PoisonError::into_inner) != (z_key, false)
        || z_link.handle.is_closed()
        || !z_alive.load(Ordering::Acquire)
    {
        return None;
    }
    Some((z_link, z_alive, z_sftp))
}

// 空闲会话只在主连接在线、所在连接仍是当前独立连接 (未退役、未被替换) 且未被记为可疑时放回: 主连接掉线后定时回收 (watch_connection) 已停止,
// 放回的通道会一直占着服务器会话名额; 退役或可疑连接上的会话放回会绕过探活被再次借出.
// 持可疑标记锁判断并放回, 与 mark_suspect 互斥; 当前连接的地址与它在同一把锁内发布和清除 (见 transfer_handle).
fn put_idle_sftp(c_session: &LiveSession, c_link: SshLink, c_alive: Arc<AtomicBool>, c_sftp: (Arc<RawSftpSession>, russh_sftp::protocol::Version)) {
    let z_suspect = c_session.transfer_suspect.lock().unwrap_or_else(PoisonError::into_inner);
    if *z_suspect != (c_link.key(), false)
        || c_link.handle.is_closed()
        || !c_alive.load(Ordering::Acquire)
        || c_session.status.lock().unwrap_or_else(PoisonError::into_inner).as_str() != "connected"
    {
        return;
    }
    let mut z_idle = c_session.idle_sftp.lock().unwrap_or_else(PoisonError::into_inner);
    if z_idle.len() < TRANSFER_IDLE_MAX {
        z_idle.push((c_link, Instant::now(), c_alive, c_sftp));
    }
}

fn has_idle_on(c_session: &LiveSession, c_key: usize) -> bool {
    c_session.idle_sftp.lock().unwrap_or_else(PoisonError::into_inner).iter().any(|c| c.0.key() == c_key)
}

fn clear_idle_sftp(c_session: &LiveSession) {
    c_session.idle_sftp.lock().unwrap_or_else(PoisonError::into_inner).clear();
}

// 移除空闲池里位于某条独立连接 (按对象地址) 上的会话.
fn drop_idle_on(c_session: &LiveSession, c_key: usize) {
    c_session.idle_sftp.lock().unwrap_or_else(PoisonError::into_inner).retain(|c| c.0.key() != c_key);
}
// 定时回收放置过久、连接已断或通道已关闭的空闲会话: 只在取用时检查的话, 之后没有传输就会一直占着服务器的会话名额 (MaxSessions).
fn purge_idle_sftp(c_session: &LiveSession) {
    c_session.idle_sftp.lock().unwrap_or_else(PoisonError::into_inner).retain(|c| !c.0.handle.is_closed() && c.2.load(Ordering::Acquire) && c.1.elapsed() < TRANSFER_IDLE);
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

async fn remote_entry_exists(c_raw: &RawSftpSession, c_path: String) -> Result<bool> {
    use russh_sftp::{client::error::Error, protocol::StatusCode};
    match tokio::time::timeout(Duration::from_secs(5), c_raw.lstat(c_path)).await.context("检查远端名称超时")? {
        Ok(_) => Ok(true),
        Err(Error::Status(l_status)) if l_status.status_code == StatusCode::NoSuchFile => Ok(false),
        Err(err) => Err(err.into()),
    }
}

async fn open_remote_directory(c_raw: &russh_sftp::client::RawSftpSession, c_session: &LiveSession, c_path: String) -> Result<String> {
    let z_opened = tokio::time::timeout(Duration::from_secs(10), c_raw.opendir(c_path))
        .await
        .context("打开远端目录超时")
        .and_then(|c| Ok(c?.handle));
    // 外层计时或 SFTP 库自身超时: 迟到的句柄无从关闭, 丢弃复用会话, 句柄随会话关闭释放.
    if z_opened.as_ref().is_err_and(sftp_retryable) {
        drop_browse_sftp(c_session, c_raw);
    }
    z_opened
}

// 只丢弃出错的那个复用会话: 期间其他请求已重建的新会话不受影响.
fn drop_browse_sftp(c_session: &LiveSession, c_raw: &russh_sftp::client::RawSftpSession) {
    let mut z_slot = c_session.browse_sftp.lock().unwrap_or_else(PoisonError::into_inner);
    if z_slot.as_ref().is_some_and(|c| std::ptr::eq(c.0.as_ref(), c_raw)) {
        z_slot.take();
    }
}

// 同一目录连续发 4 个 READDIR, 按请求顺序消费回包. EOF 或达到上限时, 库会丢弃剩余请求的回包.
async fn read_directory_pages(
    c_raw: &RawSftpSession,
    c_handle: &str,
    c_timeout: Duration,
    mut c_page: impl FnMut(Vec<russh_sftp::protocol::File>) -> Result<bool>,
) -> Result<bool> {
    let z_pages = futures_util::stream::repeat_with(|| tokio::time::timeout(c_timeout, c_raw.readdir(c_handle))).buffered(4);
    tokio::pin!(z_pages);
    while let Some(l_page) = z_pages.next().await {
        match l_page.context("读取远端目录超时")? {
            Ok(l_page) => {
                if c_page(l_page.files)? { return Ok(true); }
            }
            Err(russh_sftp::client::error::Error::Status(l_status))
                if l_status.status_code == russh_sftp::protocol::StatusCode::Eof => break,
            Err(err) => return Err(err.into()),
        }
    }
    Ok(false)
}

async fn read_remote_entries(c_raw: &RawSftpSession, c_handle: &str, c_dirs_only: bool) -> Result<(Vec<Value>, bool, Vec<(Option<u32>, Option<u32>)>)> {
    let mut entries = Vec::with_capacity(128);
    let mut name_bytes = 0usize;
    let mut owner_ids = if c_dirs_only { Vec::new() } else { Vec::with_capacity(128) };
    let z_truncated = read_directory_pages(c_raw, c_handle, Duration::from_secs(10), |c_files| {
        for file in c_files {
            if file.filename == "." || file.filename == ".." {
                continue;
            }
            // 目录树的上限只统计目录, 不能让前面的普通文件挤掉后面的子目录.
            let l_is_dir = file.attrs.is_dir();
            if c_dirs_only && !l_is_dir {
                continue;
            }
            if entries.len() >= MAX_LIST_ENTRIES
                || name_bytes.saturating_add(file.filename.len()) > MAX_LIST_NAME_BYTES
            {
                return Ok(true);
            }
            name_bytes += file.filename.len();
            if c_dirs_only {
                entries.push(json!({"name":file.filename,"is_dir":true}));
                continue;
            }
            let z_kind = if l_is_dir {
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
            entries.push(json!({"name":file.filename,"is_dir":l_is_dir,"kind":z_kind,"size":file.attrs.size.unwrap_or(0),"modified_time":z_modified_time,"permissions":z_permissions,"mode":z_mode,"owner":z_owner}));
        }
        Ok(false)
    }).await?;
    Ok((entries, z_truncated, owner_ids))
}

// 无论读取成功、出错还是超时都关闭目录句柄; 关闭失败时丢弃复用会话, 不留下未关闭的句柄.
async fn close_remote_directory(c_raw: Arc<russh_sftp::client::RawSftpSession>, c_session: Arc<LiveSession>, c_handle: String) {
    let z_closed = tokio::time::timeout(Duration::from_secs(3), c_raw.close(c_handle)).await;
    if !matches!(z_closed, Ok(Ok(_))) {
        drop_browse_sftp(&c_session, &c_raw);
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
    let z_listed = read_remote_entries(&c_raw, &z_handle, true).await.ok().map(|(mut l_dirs, l_truncated, _)| {
        sort_remote_entries(&mut l_dirs);
        json!({"path":c_path,"entries":l_dirs,"truncated":l_truncated})
    });
    (z_listed, Some(z_handle))
}

// 建立到服务器的 TCP 连接 (连接选了代理时经代理), 再完成 SSH 握手; 主连接与传输连接共用.
// TCP 建连与代理握手各自限时, 卡住时报出具体阶段; 外层总期限还包含主机指纹确认等人工等待.
// c_socket: 握手开始前存入底层套接字的副本. russh 有通道积压时暂停处理出站队列, 断开消息可能一直发不出去;
// 握手卡住 (如停在密钥交换) 时外层放弃等待, russh 的后台任务仍在. 关闭副本 (shutdown) 可直接终止这些连接.
async fn ssh_connect(c_conn: &Connection, c_proxy: Option<&ProxyRoute>, c_handler: SshHandler, c_socket: &Mutex<Option<SshSocket>>) -> Result<client::Handle<SshHandler>> {
    let z_stream = match c_proxy {
        None => tokio::time::timeout(Duration::from_secs(30), tokio::net::TcpStream::connect((bare_host(&c_conn.host), c_conn.port)))
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
    let z_std = z_stream.into_std().context("读取连接套接字失败")?;
    configure_tcp_keepalive(&z_std);
    let z_writes = Arc::new(Mutex::new(TcpWriteStats::default()));
    let z_link = c_handler.link.clone();
    *c_socket.lock().unwrap_or_else(PoisonError::into_inner) = Some(SshSocket {
        stream: z_std.try_clone().context("复制连接套接字失败")?,
        writes: z_writes.clone(),
        link: z_link.clone(),
    });
    let z_local = z_std.local_addr().ok();
    let z_peer = z_std.peer_addr().ok();
    let z_stream = tokio::net::TcpStream::from_std(z_std).context("读取连接套接字失败")?;
    z_link.mark_route(c_proxy.is_some(), z_local, z_peer);
    let z_config = ssh_config(c_handler.transfer);
    // 收紧用户态写缓冲 (独立连接 512 KiB, 主连接 256 KiB): 既解耦 russh 收发避免阻塞读取确认,
    // 又保留真实 TCP 背压, 防止多 MiB 密文积压拖慢控制包、保活回复和同连接下载.
    let z_write_buffer = if c_handler.transfer { 512 * 1024 } else { 256 * 1024 };
    let (z_read, z_write) = z_stream.into_split();
    Ok(client::connect_stream(z_config, buffer_ssh_writes(z_read, z_write, z_write_buffer, z_writes, z_link), c_handler).await?)
}

// 同一连接所有出站字节, 含 SSH 协议开销和其他通道. write 成功只表示 TCP 接收了数据.
#[derive(Default)]
struct TcpWriteStats {
    bytes: u64,
    elapsed: Duration,
    pending_since: Option<Instant>,
}
impl TcpWriteStats {
    fn totals(&self) -> (u64, Duration) {
        (self.bytes, self.elapsed + self.pending_since.map_or(Duration::ZERO, |c| c.elapsed()))
    }
}

// russh 要等加密后的整批数据写入套接字, 才继续读服务器的确认和窗口更新.
// 用有界缓冲把 TCP 写入交给单独任务, 慢上行时收到的确认也能及时处理.
fn buffer_ssh_writes(
    c_read: tokio::net::tcp::OwnedReadHalf,
    mut c_write: tokio::net::tcp::OwnedWriteHalf,
    c_buffer_bytes: usize,
    c_stats: Arc<Mutex<TcpWriteStats>>,
    c_link: Arc<SshLinkState>,
) -> impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static {
    use std::io;
    use tokio::io::AsyncBufReadExt;

    let (z_writer, z_reader) = tokio::io::duplex(c_buffer_bytes);
    let (z_wake_tx, z_wake_rx) = oneshot::channel();
    // 读写两端共享真实写错误: 缓冲满时 russh 阻塞在写端, 避免 DuplexStream 关闭后只看到泛型 BrokenPipe.
    let z_write_error: Arc<Mutex<Option<io::Error>>> = Arc::new(Mutex::new(None));
    let z_task_error = z_write_error.clone();
    let z_write_link = c_link.clone();
    tokio::spawn(async move {
        let mut z_reader = tokio::io::BufReader::with_capacity(64 * 1024, z_reader);
        let z_result = async {
            loop {
                let l_data = z_reader.fill_buf().await?;
                if l_data.is_empty() {
                    return c_write.shutdown().await;
                }
                // 按实际写入进展计时, 与独立连接的无活动超时一致. 写入中断时唤醒 SSH 读写端.
                let l_started = Instant::now();
                c_stats.lock().unwrap_or_else(PoisonError::into_inner).pending_since = Some(l_started);
                let l_written = tokio::time::timeout(SSH_IO_TIMEOUT, c_write.write(l_data)).await;
                {
                    let mut l_stats = c_stats.lock().unwrap_or_else(PoisonError::into_inner);
                    if let Ok(Ok(l_count)) = l_written {
                        l_stats.bytes += l_count as u64;
                        if l_count > 0 {
                            z_write_link.mark_tcp_tx(l_count);
                        }
                    }
                    l_stats.elapsed += l_started.elapsed();
                    l_stats.pending_since = None;
                }
                let l_count = l_written
                    .map_err(|_| io::Error::new(io::ErrorKind::ConnectionAborted, "SSH TCP 写入长时间无进展"))??;
                if l_count == 0 {
                    return Err(io::ErrorKind::WriteZero.into());
                }
                z_reader.consume(l_count);
            }
        }.await;
        if let Err(err) = z_result {
            *z_task_error.lock().unwrap_or_else(PoisonError::into_inner) = Some(err);
            let _ = z_wake_tx.send(());
            let _ = c_write.shutdown().await;
        }
    });
    tokio::io::join(
        SshSocketReader { inner: c_read, write_wake: z_wake_rx, write_error: z_write_error.clone(), link: c_link },
        SshSocketWriter { inner: z_writer, write_error: z_write_error },
    )
}

fn take_write_error(c_slot: &Mutex<Option<std::io::Error>>) -> Option<std::io::Error> {
    c_slot.lock().unwrap_or_else(PoisonError::into_inner).take()
}

struct SshSocketReader {
    inner: tokio::net::tcp::OwnedReadHalf,
    write_wake: oneshot::Receiver<()>,
    write_error: Arc<Mutex<Option<std::io::Error>>>,
    link: Arc<SshLinkState>,
}

impl tokio::io::AsyncRead for SshSocketReader {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        c_cx: &mut std::task::Context<'_>,
        c_buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if !self.write_wake.is_terminated()
            && let std::task::Poll::Ready(Ok(())) = std::pin::Pin::new(&mut self.write_wake).poll(c_cx)
            && let Some(err) = take_write_error(&self.write_error)
        {
            return std::task::Poll::Ready(Err(err));
        }
        let z_want = c_buf.remaining();
        let z_before = c_buf.filled().len();
        let z_read = std::pin::Pin::new(&mut self.inner).poll_read(c_cx, c_buf);
        if let std::task::Poll::Ready(Ok(())) = &z_read {
            let l_got = c_buf.filled().len().saturating_sub(z_before);
            if l_got > 0 {
                self.link.mark_tcp_rx(l_got);
            } else if z_want > 0 {
                // 0 字节 EOF: 先核对写任务错误和内核 SO_ERROR, 避免真实网络错误被掩盖为 UnexpectedEof.
                if let Some(err) = take_write_error(&self.write_error) {
                    return std::task::Poll::Ready(Err(err));
                }
                if let Ok(Some(err)) = self.inner.as_ref().take_error() {
                    return std::task::Poll::Ready(Err(err));
                }
            }
        }
        z_read
    }
}

struct SshSocketWriter {
    inner: tokio::io::DuplexStream,
    write_error: Arc<Mutex<Option<std::io::Error>>>,
}

impl SshSocketWriter {
    fn map_err(&self, c_err: std::io::Error) -> std::io::Error {
        if c_err.kind() == std::io::ErrorKind::BrokenPipe
            && let Some(l_err) = take_write_error(&self.write_error)
        {
            return l_err;
        }
        c_err
    }
}

impl tokio::io::AsyncWrite for SshSocketWriter {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        c_cx: &mut std::task::Context<'_>,
        c_buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner)
            .poll_write(c_cx, c_buf)
            .map_err(|err| self.map_err(err))
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        c_cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner)
            .poll_flush(c_cx)
            .map_err(|err| self.map_err(err))
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        c_cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner)
            .poll_shutdown(c_cx)
            .map_err(|err| self.map_err(err))
    }
}

// 开启系统级 TCP 保活: 空闲期间由内核定时发 TCP Keep-Alive 探测包, 防止 NAT 路由器或代理提前关闭长连接.
#[cfg(target_os = "macos")]
fn configure_tcp_keepalive(c_stream: &std::net::TcpStream) {
    use std::ffi::c_void;
    use std::os::fd::AsRawFd;

    unsafe extern "C" {
        fn setsockopt(c_fd: i32, c_level: i32, c_name: i32, c_val: *const c_void, c_len: u32) -> i32;
    }
    const SOL_SOCKET: i32 = 0xffff;
    const SO_KEEPALIVE: i32 = 0x0008;
    const IPPROTO_TCP: i32 = 6;
    const TCP_KEEPALIVE: i32 = 0x10;
    const TCP_KEEPINTVL: i32 = 0x101;
    const TCP_KEEPCNT: i32 = 0x102;

    let z_fd = c_stream.as_raw_fd();
    let z_on: i32 = 1;
    // 与 watch_connection 约 2-3 分钟的容忍窗口对齐, 避免短暂网络抖动被内核 TCP 保活在 45 秒误杀.
    let z_idle: i32 = 45;
    let z_interval: i32 = 15;
    let z_count: i32 = 6;
    let z_size = std::mem::size_of::<i32>() as u32;
    unsafe {
        setsockopt(z_fd, SOL_SOCKET, SO_KEEPALIVE, (&raw const z_on).cast(), z_size);
        setsockopt(z_fd, IPPROTO_TCP, TCP_KEEPALIVE, (&raw const z_idle).cast(), z_size);
        setsockopt(z_fd, IPPROTO_TCP, TCP_KEEPINTVL, (&raw const z_interval).cast(), z_size);
        setsockopt(z_fd, IPPROTO_TCP, TCP_KEEPCNT, (&raw const z_count).cast(), z_size);
    }
}

#[cfg(windows)]
fn configure_tcp_keepalive(c_stream: &std::net::TcpStream) {
    use std::ffi::c_void;
    use std::os::windows::io::AsRawSocket;

    #[repr(C)]
    struct TcpKeepalive {
        onoff: u32,
        keepalivetime: u32,
        keepaliveinterval: u32,
    }
    #[link(name = "ws2_32")]
    unsafe extern "system" {
        fn WSAIoctl(
            c_socket: usize,
            c_code: u32,
            c_in: *const c_void,
            c_in_len: u32,
            c_out: *mut c_void,
            c_out_len: u32,
            c_returned: *mut u32,
            c_overlapped: *mut c_void,
            c_completion: *mut c_void,
        ) -> i32;
    }
    const SIO_KEEPALIVE_VALS: u32 = 0x9800_0004;

    let z_socket = c_stream.as_raw_socket() as usize;
    let z_vals = TcpKeepalive { onoff: 1, keepalivetime: 45_000, keepaliveinterval: 15_000 };
    let mut z_bytes = 0u32;
    unsafe {
        WSAIoctl(
            z_socket,
            SIO_KEEPALIVE_VALS,
            (&raw const z_vals).cast(),
            std::mem::size_of::<TcpKeepalive>() as u32,
            std::ptr::null_mut(),
            0,
            &raw mut z_bytes,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        );
    }
}

#[cfg(not(any(target_os = "macos", windows)))]
fn configure_tcp_keepalive(_: &std::net::TcpStream) {}

// 地址可能写成带方括号的 IPv6 (如 [::1]): 去掉括号, 直连解析和代理握手都按裸地址处理.
fn bare_host(c_host: &str) -> &str {
    c_host.strip_prefix('[').and_then(|c| c.strip_suffix(']')).unwrap_or(c_host)
}

// 经代理打开到目标的隧道: SOCKS5 (可带用户名密码) 或 HTTP CONNECT. 握手完成后返回的连接直接承载 SSH.
async fn proxy_stream(c_proxy: &crate::config::Proxy, c_password: Option<&str>, c_host: &str, c_port: u16) -> Result<tokio::net::TcpStream> {
    let mut z_stream = tokio::time::timeout(Duration::from_secs(15), tokio::net::TcpStream::connect((bare_host(&c_proxy.host), c_proxy.port)))
        .await
        .map_err(|_| anyhow::anyhow!("连接代理 {}:{} 超时", c_proxy.host, c_proxy.port))?
        .with_context(|| format!("无法连接代理 {}:{}", c_proxy.host, c_proxy.port))?;
    tokio::time::timeout(Duration::from_secs(20), proxy_handshake(&mut z_stream, c_proxy, c_password, c_host, c_port))
        .await
        .map_err(|_| anyhow::anyhow!("代理无响应 (握手超过 20 秒)"))??;
    Ok(z_stream)
}

async fn proxy_handshake(c_stream: &mut tokio::net::TcpStream, c_proxy: &crate::config::Proxy, c_password: Option<&str>, c_host: &str, c_port: u16) -> Result<()> {
    let c_host = bare_host(c_host);
    let z_user = c_proxy.username.as_str();
    let z_password = c_password.unwrap_or_default();
    if c_proxy.kind == "http" {
        // IPv6 地址在 CONNECT 目标中需加方括号.
        let z_target = if c_host.contains(':') { format!("[{c_host}]:{c_port}") } else { format!("{c_host}:{c_port}") };
        let mut z_request = Zeroizing::new(format!("CONNECT {z_target} HTTP/1.1\r\nHost: {z_target}\r\nProxy-Connection: keep-alive\r\nConnection: keep-alive\r\n"));
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
            // 用户名密码子协商的版本号为 1.
            ensure!(z_reply[0] == 1, "代理认证响应格式无效");
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
    ensure!(z_head[0] == 5 && z_head[2] == 0, "代理响应格式无效");
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
    z_config.channel_buffer_size = if c_transfer { 128 } else { 64 };
    z_config.nodelay = true;
    // 默认 1 小时定时重协商在高延迟代理或频繁收发下容易与服务器严格密钥交换冲突并触发断流, 拉长到 24 小时 (仍保留 1 GiB 流量上限防 nonce 耗尽).
    z_config.limits.rekey_time_limit = Duration::from_secs(86_400);
    // 传输连接: 15 秒未收到任何数据发一次保活, 连续 6 次仍无数据 (约 90 秒) 判定失联; 写入 TCP 卡住时由 180 秒无活动超时兜底.
    // 主连接改由 watch_connection 探活: russh 的保活在认证前到期后不重置计时器 (等用户输入密码时空转占满 CPU),
    // 也发现不了"出站队列被已关闭通道的积压卡住"; 探活的 ping 与普通消息同走出站队列, 超时即关闭套接字.
    // 主连接不设全局 inactivity_timeout (认证网络调用已各自限时): 若系统短时睡眠后时钟跨越该阈值, russh 唤醒后会直接误杀健康连接.
    if c_transfer {
        z_config.keepalive_interval = Some(Duration::from_secs(15));
        z_config.keepalive_max = 6;
        z_config.inactivity_timeout = Some(SSH_IO_TIMEOUT);
    } else {
        z_config.keepalive_interval = None;
        z_config.inactivity_timeout = None;
    }
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
    // 优先硬件加速的 AES-GCM, 降低大文件高速传输时的加解密 CPU 占用; 保留原有默认算法与老设备兜底.
    let mut z_cipher = vec![russh::cipher::AES_256_GCM, russh::cipher::AES_128_GCM];
    for l_cipher in z_config.preferred.cipher.iter().copied().chain([
        russh::cipher::AES_256_CBC,
        russh::cipher::AES_192_CBC,
        russh::cipher::AES_128_CBC,
    ]) {
        if !z_cipher.contains(&l_cipher) {
            z_cipher.push(l_cipher);
        }
    }
    z_config.preferred.cipher = z_cipher.into();
    let mut z_mac = z_config.preferred.mac.to_vec();
    z_mac.extend([russh::mac::HMAC_SHA1_ETM, russh::mac::HMAC_SHA1]);
    z_config.preferred.mac = z_mac.into();
    Arc::new(z_config)
}
// 超过 1.5 秒未收到新进度时按已过时间平滑衰减, 超过 6 秒视为停滞再归 0, 避免代理抖动下速度闪烁归 0.
fn current_speed(c_task: &Task) -> u64 {
    match task_status(c_task) {
        "running" => {}
        "cancelling" => return 0,
        _ => return c_task.speed_bytes_per_sec,
    }
    let z_elapsed = c_task.last_progress_at.elapsed().as_secs_f64();
    if z_elapsed > 6.0 {
        0
    } else if z_elapsed > 1.5 {
        ((c_task.speed_bytes_per_sec as f64) * 1.5 / z_elapsed) as u64
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
            // 重连后重新测速. 文件夹任务已在进行中时沿用采样, 否则连续的小文件测不出速度.
            if !z_was_running {
                t.last_progress_at = Instant::now();
                t.speed_samples.clear();
            }
        } else if matches!(c_status, "preparing" | "retrying") {
            t.speed_bytes_per_sec = 0;
            if c_status == "retrying" {
                t.transfer_retries += 1;
                t.last_transfer_error = t.error.clone();
            }
        } else if matches!(c_status, "completed" | "failed" | "cancelled") {
            t.speed_bytes_per_sec = 0;
            t.speed_samples.clear();
            t.finished_at_ms = Some(epoch_ms());
        }
    }
}
// c_bytes、c_total 为当前文件的进度; 文件夹任务另累计到整个文件夹. c_data 表示已确认上传或写入的下载数据.
fn update_progress(c_task: &Arc<Mutex<Task>>, c_bytes: u64, c_total: u64, c_data: bool) {
    if let Ok(mut t) = c_task.lock() {
        let z_bytes = t.base_bytes + c_bytes;
        let z_now = Instant::now();
        if z_bytes < t.bytes || t.speed_samples.back().is_some_and(|&(_, l_bytes)| z_bytes < l_bytes)
            || (!c_data && z_bytes != t.bytes)
        {
            // 续传定位改变的进度不计入测速.
            t.speed_bytes_per_sec = 0;
            t.last_progress_at = z_now;
            t.speed_samples.clear();
        }
        if t.speed_samples.is_empty() {
            t.speed_samples.push_back((z_now, z_bytes));
        }
        if c_data && z_bytes > t.bytes {
            sample_transfer_speed(&mut t, z_bytes, z_now);
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
fn sample_transfer_speed(c_task: &mut Task, c_bytes: u64, c_now: Instant) {
    c_task.last_progress_at = c_now;
    let z_samples = &mut c_task.speed_samples;
    if z_samples.back().is_some_and(|&(l_at, _)| c_now.duration_since(l_at) < TRANSFER_SPEED_SAMPLE) {
        return;
    }
    // 每 250 毫秒至多取一个点, 保留最接近滚动 1 秒窗口起点的前一个点.
    z_samples.push_back((c_now, c_bytes));
    while z_samples.get(1).is_some_and(|&(l_at, _)| c_now.duration_since(l_at) >= TRANSFER_SPEED_WINDOW) {
        z_samples.pop_front();
    }
    let (z_start, z_bytes) = z_samples[0];
    if c_now > z_start {
        c_task.speed_bytes_per_sec = ((c_bytes - z_bytes) as f64 / c_now.duration_since(z_start).as_secs_f64()) as u64;
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
        while c < screen_size(c_screen).1.min(MAX_TERMINAL_COLS) {
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
    let z_result = tokio::time::timeout(Duration::from_secs(12), async {
        z_channel = Some(ClosingChannel(Some(
            open_channel(c_handle, 12).await.context("打开服务器时间通道失败")?,
        )));
        let (z_output, _, z_exit_status) =
            exec_collect(z_channel.as_mut().unwrap(), z_command, z_input.as_bytes(), 2 * 1024 * 1024).await?;
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
        // 只有正常退出 (退出码 0) 才算查询成功: 执行被拒绝或中途断开时输出不完整, 不能把漏掉的 ID 长期缓存为"没有名字".
        let z_resolved = tokio::time::timeout(Duration::from_secs(12), async {
            let mut z_channel = ClosingChannel(Some(open_channel(c_handle, 12).await.ok()?));
            let (z_output, _, z_code) = exec_collect(&mut z_channel, &z_command, &[], 16 * 1024).await.ok()?;
            (z_code == Some(0)).then_some(z_output)
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
// 不必每次新开通道. 速率按服务器 /proc/uptime 计算, 不受网络延迟抖动影响. 通道中断后 3 秒重连, 连续失败时逐次放慢.
// 偶发中断自动重连且保留上次数据, 不提示; 连续约 15 秒取不到数据才显示"监控暂不可用".
async fn monitor_loop(c_session: Weak<LiveSession>, mut c_stop: oneshot::Receiver<()>) {
    let mut z_failed_since: Option<Instant> = None;
    // 连续快速结束的次数 (运行超过 15 秒才结束的不算).
    let mut z_streak = 0u32;
    let z_online = |c: &LiveSession| {
        c.status.lock().is_ok_and(|s| s.as_str() == "connected")
            && c.shell_ready.load(Ordering::Acquire)
            && c.handle.lock().unwrap_or_else(PoisonError::into_inner).as_ref().is_some_and(|l| !l.is_closed())
    };
    loop {
        if !matches!(c_stop.try_recv(), Err(oneshot::error::TryRecvError::Empty)) {
            return;
        }
        let Some(l_session) = c_session.upgrade() else { return; };
        if !z_online(&l_session) {
            return;
        }
        let z_started = Instant::now();
        let mut z_sampled = false;
        let z_result = monitor_stream(&l_session, &mut z_sampled, &mut c_stop).await;
        if !z_online(&l_session) {
            return;
        }
        match z_result {
            Ok(None) => return,
            Ok(Some(false)) => {
                l_session.monitor_unsupported.store(true, Ordering::Release);
                apply_monitor_error(&l_session, &anyhow::anyhow!("服务器不支持 /proc 系统监控"));
                return;
            }
            Ok(Some(true)) => {}
            Err(err) => {
                let l_now = Instant::now();
                // 确实收到过采样且稳定运行超过 15 秒后才中断的, 以当前时刻作为新一轮失败起点.
                if z_sampled && z_started.elapsed() > Duration::from_secs(15) {
                    z_failed_since = None;
                    z_streak = 0;
                }
                let l_since = *z_failed_since.get_or_insert(if z_sampled { l_now } else { z_started });
                if l_since.elapsed() >= Duration::from_secs(15) {
                    apply_monitor_error(&l_session, &err);
                }
            }
        }
        drop(l_session);
        // 连续很快失败时逐次加长间隔到 60 秒, 不每 3 秒新开一次命令; 收到停止信号时立即退出.
        z_streak = z_streak.saturating_add(1);
        tokio::select! {
            biased;
            _ = &mut c_stop => return,
            _ = tokio::time::sleep(Duration::from_secs((3u64 << z_streak.saturating_sub(1).min(5)).min(60))) => {}
        }
    }
}

// exec 2>/dev/null: 读取进程信息时进程恰好退出会产生错误输出, 不能混入采样数据.
// 启动先检查 /proc 可读性 (不支持时返回 42 停止重试); 输出末尾检测写管道失败以便通道关闭后远端进程立即退出.
const MONITOR_SCRIPT: &str = "exec 2>/dev/null; [ -r /proc/stat ] && [ -r /proc/meminfo ] || exit 42; i=0; while :; do if [ $((i % 3)) -eq 0 ]; then count=0; truncated=0; printf 'HIPSSH_PROC_BEGIN\\n'; for f in /proc/[0-9]*/stat; do [ -r \"$f\" ] || continue; if [ \"$count\" -ge 512 ]; then truncated=1; break; fi; line=; while IFS= read -r part || [ -n \"$part\" ]; do line=\"$line$part\"; done < \"$f\"; printf 'P\\t%s\\n' \"$line\"; count=$((count+1)); done; printf 'HIPSSH_PROC_END\\t%s\\n' \"$truncated\"; printf 'HIPSSH_PAGE_SIZE\\t%s\\n' \"$(getconf PAGESIZE 2>/dev/null)\"; printf 'HIPSSH_CLK_TCK\\t%s\\n' \"$(getconf CLK_TCK 2>/dev/null)\"; fi; read up rest < /proc/uptime; printf 'HIPSSH_UPTIME\\t%s\\n' \"$up\"; cat /proc/stat /proc/meminfo; printf 'HIPSSH_NET_BEGIN\\n'; cat /proc/net/dev; printf 'HIPSSH_NET_END\\nHIPSSH_SAMPLE_END\\n' || exit 0; i=$((i+1)); sleep 1; done";

async fn monitor_stream(c_session: &Arc<LiveSession>, c_sampled: &mut bool, c_stop: &mut oneshot::Receiver<()>) -> Result<Option<bool>> {
    const SAMPLE_END: &[u8] = b"HIPSSH_SAMPLE_END\n";
    let z_handle = c_session.ssh_handle()?;
    // 守卫保证通道在出错、卡住、停止或会话结束时关闭, 不占用服务器会话名额.
    let z_opened = tokio::select! {
        biased;
        _ = &mut *c_stop => return Ok(None),
        l_res = open_channel(&z_handle, 15) => l_res.context("打开监控通道超时")?,
    };
    let mut channel = ClosingChannel(Some(z_opened));
    tokio::select! {
        biased;
        _ = &mut *c_stop => return Ok(None),
        l_res = tokio::time::timeout(Duration::from_secs(10), channel.exec(true, MONITOR_SCRIPT)) => {
            l_res.context("启动远端监控命令超时")??;
        }
    }
    let mut z_previous: Option<ProcSample> = None;
    let mut z_previous_proc: Option<ProcSample> = None;
    let mut z_buffer = Vec::with_capacity(64 * 1024);
    let mut z_last_sample = Instant::now();
    let mut z_exit_code = None;
    loop {
        // 正常每秒都有完整采样; 按上一次完整采样计时, 持续有输出却凑不成采样时同样判定卡住, 关闭后重连.
        // 每轮先检查绝对期限: 剩余时间为 0 时 timeout 仍会先轮询一次读取, 消息不断时会一直越过期限.
        // 跨国代理抖动或远端遍历 512 进程耗时较长时留足 25 秒, 避免短暂抖动反复重启监控通道.
        ensure!(z_last_sample.elapsed() < Duration::from_secs(25), "远端监控无响应");
        let z_message = tokio::select! {
            biased;
            _ = &mut *c_stop => return Ok(None),
            l_res = tokio::time::timeout(Duration::from_secs(25).saturating_sub(z_last_sample.elapsed()), channel.wait()) => {
                l_res.context("远端监控无响应")?
            }
        };
        match z_message {
            Some(ChannelMsg::Data { data }) => {
                z_buffer.extend_from_slice(&data);
                while let Some(l_end) = z_buffer.windows(SAMPLE_END.len()).position(|c| c == SAMPLE_END) {
                    // 单帧上限在解析前检查, 一帧跨过上限且恰好带结束标记时也不会被解析.
                    ensure!(l_end <= 1024 * 1024, "远端监控采样输出超过1MiB上限");
                    let l_chunk = z_buffer.drain(..l_end + SAMPLE_END.len()).collect::<Vec<_>>();
                    let l_sample = parse_proc_sample(&String::from_utf8_lossy(&l_chunk))?;
                    apply_monitor_sample(c_session, &mut z_previous, &mut z_previous_proc, l_sample);
                    *c_sampled = true;
                    z_last_sample = Instant::now();
                }
                // 取出完整采样后再检查: 只限制单帧未完成部分的长度, 一次到达多帧时不误报.
                ensure!(z_buffer.len() <= 1024 * 1024, "远端监控采样输出超过1MiB上限");
            }
            Some(ChannelMsg::ExitStatus { exit_status }) => z_exit_code = Some(exit_status),
            Some(ChannelMsg::Failure) => bail!("服务器拒绝执行监控命令"),
            // EOF 只代表没有更多输出, 退出码 (如 42 表示无 /proc) 可能紧随其后到达.
            Some(ChannelMsg::Eof) => {}
            Some(ChannelMsg::Close) | None => {
                if z_exit_code == Some(42) {
                    return Ok(Some(false));
                }
                bail!("远端监控已结束");
            }
            Some(_) => {}
        }
    }
}

// 两次采样的间隔: 优先用服务器运行时间, 缺少时退回本机收到的时间.
fn sample_elapsed(c_new: &ProcSample, c_old: &ProcSample) -> f64 {
    match (c_new.uptime, c_old.uptime) {
        (Some(l_new), Some(l_old)) if l_new > l_old => (l_new - l_old).max(0.001),
        // 本机接收间隔: 积压的多帧集中到达时会接近 0, 产生尖峰; 远端脚本每次采样至少间隔 1 秒, 以此为下限.
        _ => c_new.sampled_at.duration_since(c_old.sampled_at).as_secs_f64().max(1.0),
    }
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
    let mut z_mem_available = None;
    // 旧内核 (3.14 之前) 没有 MemAvailable, 按空闲 + 缓冲 + 可回收缓存估算, 不显示成内存用满.
    let mut z_mem_free = 0;
    let mut z_buffers = 0;
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
                "MemTotal" | "MemAvailable" | "MemFree" | "Buffers" | "SwapTotal" | "SwapFree" | "Cached"
                | "SReclaimable" | "Shmem" => {
                    let z_value = value
                        .split_whitespace()
                        .next()
                        .and_then(|v| v.parse::<u64>().ok())
                        .unwrap_or(0);
                    match key {
                        "MemTotal" => z_mem_total = z_value,
                        "MemAvailable" => z_mem_available = Some(z_value),
                        "MemFree" => z_mem_free = z_value,
                        "Buffers" => z_buffers = z_value,
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
    let z_cache = z_cached.saturating_add(z_sreclaimable).saturating_sub(z_shmem);
    let z_available = z_mem_available.unwrap_or_else(|| z_mem_free.saturating_add(z_buffers).saturating_add(z_cache));
    Ok(ProcSample {
        cpu_total: cpu,
        cpu_idle: idle,
        memory_total: z_mem_total.saturating_mul(1024),
        memory_used: z_mem_total
            .saturating_sub(z_available)
            .saturating_mul(1024),
        swap_total: z_swap_total_bytes,
        swap_used: z_swap_total_bytes.saturating_sub(z_swap_free.saturating_mul(1024)),
        memory_cache: z_cache.saturating_mul(1024),
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
