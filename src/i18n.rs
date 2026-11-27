use std::sync::atomic::{AtomicU8, Ordering};

// 界面语言: 0 中文 (默认), 1 英文; 全局原子存储, 界面线程切换后后台弹窗与自检立即生效.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lang {
    Zh = 0,
    En = 1,
}

static CURRENT: AtomicU8 = AtomicU8::new(0);

#[inline]
pub fn current() -> Lang {
    if CURRENT.load(Ordering::Relaxed) == 1 {
        Lang::En
    } else {
        Lang::Zh
    }
}

#[inline]
pub fn is_en() -> bool {
    current() == Lang::En
}

pub fn set(c_lang: Lang) {
    CURRENT.store(c_lang as u8, Ordering::Relaxed);
}

pub fn from_code(c_code: &str) -> Lang {
    if c_code.eq_ignore_ascii_case("en") {
        Lang::En
    } else {
        Lang::Zh
    }
}

pub fn code(c_lang: Lang) -> &'static str {
    match c_lang {
        Lang::Zh => "zh",
        Lang::En => "en",
    }
}

#[inline]
pub fn t(c_zh: &'static str, c_en: &'static str) -> &'static str {
    if is_en() { c_en } else { c_zh }
}

// 默认分组在配置和逻辑中固定存为 "默认", 仅在显示层按语言映射为 "Default".
#[inline]
pub fn display_group(c_group: &str) -> &str {
    if c_group.is_empty() || c_group == "默认" {
        t("默认", "Default")
    } else {
        c_group
    }
}

// 后台兜底客户端名称显示层映射.
#[inline]
pub fn display_client(c_name: &str) -> &str {
    if is_en() && c_name == "AI 客户端" {
        "AI Client"
    } else {
        c_name
    }
}

// 界面报错与状态提示本地化: 中文模式将常见服务器英文错误转为中文, 英文模式将后台中文错误与断连摘要转为英文.
// 只用于界面展示, 不影响后台状态前缀匹配和 MCP 协议返回.
pub fn localize_error(c_error: &str) -> String {
    if !is_en() {
        return replace_table(c_error, EN_TO_ZH);
    }
    let mut z_text = c_error.to_owned();
    // 先处理带数字或路径参数的动态模板, 再做静态短语替换 (长短语在前避免中英混杂).
    replace_span(&mut z_text, " · 已连接 ", " 秒", |c| {
        localize_cn_duration_body(c).map(|l_dur| format!(" · Connected {l_dur}"))
    });
    replace_digits(&mut z_text, " · 期间疑似系统休眠约 ", " 秒", |c| format!(" · Possible sleep ~{c}s"));
    replace_digits(&mut z_text, " · 业务收包 ", " 秒前", |c| format!(" · Data {c}s ago"));
    replace_digits(&mut z_text, " · 探活 ", " 秒前", |c| format!(" · Keepalive {c}s ago"));
    replace_digits(&mut z_text, " · TCP 收包 ", " 秒前", |c| format!(" · TCP rx {c}s ago"));
    replace_digits(&mut z_text, " · 已重协商 ", " 次", |c| format!(" · Rekeyed {c}x"));
    replace_digits(&mut z_text, "等待服务器响应超时 (", " 秒)", |c| format!("Server response timed out ({c}s)"));
    replace_digits(&mut z_text, "自动重连 ", " 次仍失败", |c| format!("Failed after {c} auto-reconnects"));
    replace_digits(&mut z_text, "端口 ", " 被其他程序占用, 请换一个", |c| format!("Port {c} is in use by another app; pick another"));
    replace_digits(&mut z_text, "端口 ", " 被占用, 请换一个", |c| format!("Port {c} is in use; pick another"));
    replace_digits(&mut z_text, "命令超过 ", " 秒未完成", |c| format!("Command timed out after {c}s"));
    replace_digits(&mut z_text, "命令输出超过 ", " KiB 上限", |c| format!("Command output exceeded {c} KiB limit"));
    replace_digits(&mut z_text, "文件夹超过 ", " 项, 请分批传输", |c| format!("Folder exceeds {c} items; transfer in smaller batches"));
    replace_digits(&mut z_text, "一次最多删除 ", " 项", |c| format!("Delete up to {c} items at once"));
    replace_digits(&mut z_text, "最多保存 ", " 个代理", |c| format!("Maximum {c} proxies allowed"));
    replace_digits(&mut z_text, "远端文件时间超过 ", " 项上限", |c| format!("Remote file timestamps exceeded {c} limit"));
    replace_digits(&mut z_text, "FinalShell 认证方式 ", " 暂不支持", |c| format!("FinalShell auth type {c} is not supported"));
    replace_span(&mut z_text, "连接服务器 ", " 超时", |c| {
        (!c.contains(char::is_whitespace)).then(|| format!("Connection to server {c} timed out"))
    });
    replace_span(&mut z_text, "连接代理 ", " 超时", |c| {
        (!c.contains(char::is_whitespace)).then(|| format!("Connection to proxy {c} timed out"))
    });
    replace_span(&mut z_text, "经代理「", "」连接失败", |c| Some(format!("Connection via proxy \"{c}\" failed")));
    replace_span(&mut z_text, "代理名称不能是「", "」", |c| Some(format!("Proxy name cannot be \"{c}\"")));
    replace_span(&mut z_text, "读取导入文件 ", " 失败", |c| Some(format!("Failed to read import file {c}")));
    replace_span(&mut z_text, "解析导入文件 ", " 失败", |c| Some(format!("Failed to parse import file {c}")));
    replace_span(&mut z_text, "导入文件 ", " 顶层必须是对象或数组", |c| Some(format!("Import file {c} root must be an object or array")));
    replace_span(&mut z_text, "导入文件 ", " 没有连接记录", |c| Some(format!("Import file {c} has no host entries")));
    replace_span(&mut z_text, "准备导出连接 ", " 失败", |c| Some(format!("Failed to prepare host {c} for export")));
    replace_span(&mut z_text, "已导入 ", " 条连接, 保存后续连接失败", |c| {
        is_digits(c).then(|| format!("Imported {c} host(s), failed to save remaining"))
    });
    replace_span(&mut z_text, "旧文件可能已改名为 ", ", 请核对", |c| {
        c.split_once(", 新文件保留在 ").map(|(l_a, l_b)| {
            format!("old file may have been renamed to {l_a}, new file kept at {l_b}; please check")
        })
    });
    replace_span(&mut z_text, "新文件可能已就位于 ", ", 请核对后自行删除备份", |c| {
        c.split_once(", 旧文件备份保留在 ").map(|(l_a, l_b)| {
            format!("new file may be at {l_a}, old backup kept at {l_b}; check and remove backup manually")
        })
    });
    replace_span(&mut z_text, "新文件可能已就位于 ", ", 请核对", |c| {
        c.split_once(", 也可能仍在 ").map(|(l_a, l_b)| {
            format!("new file may be at {l_a} or still at {l_b}; please check")
        })
    });
    replace_folder_summary(&mut z_text);
    replace_table(&z_text, ZH_TO_EN)
}

// 断连完整诊断行本地化 (用于「复制诊断」按钮, 按 " | " 字段结构转换, 不改写主机名与原始错误).
pub fn localize_disconnect_detail(c_detail: &str) -> String {
    if !is_en() || c_detail.is_empty() {
        return c_detail.to_owned();
    }
    let mut z_parts = Vec::new();
    for l_part in c_detail.split(" | ") {
        if let Some(l_val) = l_part.strip_prefix("原因: ") {
            z_parts.push(format!("Reason: {}", localize_error(l_val)));
        } else if let Some(l_val) = l_part.strip_prefix("原始错误: ") {
            z_parts.push(format!("Raw error: {l_val}"));
        } else if let Some(l_val) = l_part.strip_prefix("路径: ") {
            let l_route = if let Some(l_rest) = l_val.strip_prefix("直连 (命中 Fake-IP/TUN)") {
                format!("Direct (Fake-IP/TUN){l_rest}")
            } else if let Some(l_rest) = l_val.strip_prefix("经代理") {
                format!("Via Proxy{l_rest}")
            } else if let Some(l_rest) = l_val.strip_prefix("直连") {
                format!("Direct{l_rest}")
            } else {
                l_val.to_owned()
            };
            z_parts.push(format!("Route: {l_route}"));
        } else if let Some(l_val) = l_part.strip_prefix("服务端: ") {
            z_parts.push(format!("Server: {l_val}"));
        } else if let Some(l_val) = l_part.strip_prefix("算法: ") {
            z_parts.push(format!("Crypto: {l_val}"));
        } else if let Some(l_val) = l_part.strip_prefix("已连接: ") {
            let l_dur = l_val
                .strip_suffix(" 秒")
                .and_then(localize_cn_duration_body)
                .unwrap_or_else(|| l_val.to_owned());
            z_parts.push(format!("Connected: {l_dur}"));
        } else if let Some(l_val) = l_part.strip_prefix("流量: ") {
            z_parts.push(format!("Traffic: {l_val}"));
        } else if let Some(l_val) = l_part.strip_prefix("间隔: ") {
            let l_gaps = l_val
                .replace("业务=", "data=")
                .replace("探活=", "keepalive=")
                .replace("tcp收=", "tcp_rx=")
                .replace("tcp发=", "tcp_tx=");
            z_parts.push(format!("Last: {l_gaps}"));
        } else if let Some(l_val) = l_part.strip_prefix("休眠: ") {
            let l_sleep = if l_val == "无" { "none" } else { l_val };
            z_parts.push(format!("Sleep: {l_sleep}"));
        } else {
            z_parts.push(l_part.to_owned());
        }
    }
    z_parts.join(" | ")
}

// 传输任务的 stdout / stderr 诊断文本本地化 (仅用于上传/下载任务, 不触碰远端命令的原始输出).
pub fn localize_transfer_output(c_text: &str) -> String {
    if !is_en() || c_text.is_empty() {
        return c_text.to_owned();
    }
    let mut z_lines = Vec::new();
    for l_line in c_text.lines() {
        z_lines.push(localize_transfer_line(l_line));
    }
    let mut z_out = z_lines.join("\n");
    if c_text.ends_with('\n') {
        z_out.push('\n');
    }
    z_out
}

// 命令任务仅本地化系统自动释放输出时的占位提示, 保留远端命令原始输出.
pub fn localize_command_output(c_text: &str) -> &str {
    if is_en() && c_text == "(输出已释放: 只保留最近任务的输出)" {
        "(Output evicted: only recent task outputs are kept)"
    } else {
        c_text
    }
}

// MCP 侧栏活动目标与执行内容本地化.
pub fn localize_mcp_detail(c_detail: &str) -> String {
    if !is_en() {
        return c_detail.to_owned();
    }
    match c_detail {
        "全部会话" => "All sessions".into(),
        "本机" => "Local".into(),
        _ => match c_detail.strip_prefix("[默认] ") {
            Some(l_rest) => format!("[Default] {l_rest}"),
            None => c_detail.to_owned(),
        },
    }
}

pub fn localize_mcp_info(c_method: &str, c_info: &str) -> String {
    if !is_en() || c_info.is_empty() {
        return c_info.to_owned();
    }
    match c_method {
        "input_admin_password" if c_info == "[管理员密码]" => "[Admin password]".into(),
        "upload_file" | "download_file" => c_info.replacen("\n到 ", "\nto ", 1),
        "connect" | "reconnect" if c_info == "仅文件" => "Files only".into(),
        "write_terminal" => c_info.replace(" [回车]", " [Enter]").replace("[退格]", "[Backspace]"),
        _ => c_info.to_owned(),
    }
}

// 登录认证弹窗提示本地化 (密码 / 私钥口令).
pub fn localize_auth_prompt(c_prompt: &str) -> String {
    if !is_en() {
        return c_prompt.to_owned();
    }
    if let Some(l_rest) = c_prompt.strip_suffix(" 的 SSH 密码") {
        if let Some(l_target) = l_rest.strip_prefix("密码错误, 请重新输入 ") {
            return format!("Incorrect password. Re-enter SSH password for {l_target}");
        }
        return format!("SSH password for {l_rest}");
    }
    if let Some(l_rest) = c_prompt.strip_suffix(" 的口令") {
        if let Some(l_path) = l_rest.strip_prefix("口令错误, 请重新输入 私钥 ") {
            return format!("Incorrect passphrase. Re-enter passphrase for key {l_path}");
        }
        if let Some(l_path) = l_rest.strip_prefix("私钥 ") {
            return format!("Passphrase for key {l_path}");
        }
    }
    c_prompt.to_owned()
}

fn localize_transfer_line(c_line: &str) -> String {
    if c_line == "经独立 SSH 连接传输" {
        return "Transferred via dedicated SSH connection".into();
    }
    if c_line == "文件夹内传输用时最长的文件" {
        return "Slowest file in folder transfer".into();
    }
    if c_line == "(输出已释放: 只保留最近任务的输出)" {
        return "(Output evicted: only recent task outputs are kept)".into();
    }
    if c_line == "... 其余条目已省略" {
        return "... remaining items omitted".into();
    }
    if let Some(l_rest) = c_line.strip_prefix("经主连接传输 (")
        && let Some(l_reason) = l_rest.strip_suffix("), 传输期间终端和监控可能变慢")
    {
        return format!(
            "Transferred via main connection ({}); terminal and monitor may slow down during transfer",
            localize_error(l_reason)
        );
    }
    if let Some(l_n) = c_line.strip_prefix("本次上传 ").and_then(|c| c.strip_suffix(" 字节")) {
        return format!("Uploaded {l_n} bytes");
    }
    if let Some(l_n) = c_line.strip_prefix("续传起点 ").and_then(|c| c.strip_suffix(" 字节 · 进度按本次上传计算")) {
        return format!("Resumed at {l_n} bytes · progress is for this transfer");
    }
    if let Some(l_rest) = c_line.strip_prefix("上传用时(秒)  首块入队 ") {
        return format!(
            "Upload timing (s)  first queued {}",
            l_rest.replace(" · 首块确认 ", " · first ack ")
        );
    }
    if let Some(l_rest) = c_line.strip_prefix("最长确认间隔 ")
        && let Some(l_body) = l_rest.strip_suffix(" 块")
    {
        return format!(
            "Longest ack gap {} chunks",
            l_body
                .replace(" 秒 · 起始进度 ", "s · at ")
                .replace(" · 当时待确认 ", " · inflight ")
        );
    }
    if c_line.contains("% 累计 · SFTP 入队 ") && c_line.ends_with(" 秒") {
        let l_trimmed = &c_line[..c_line.len() - " 秒".len()];
        return format!(
            "{}s",
            l_trimmed
                .replace("% 累计 · SFTP 入队 ", "% total · SFTP queued ")
                .replace(" · 连接写入 TCP ", " · TCP written ")
                .replace(" · TCP 写入耗时 ", " · TCP write ")
        );
    }
    if let Some(l_rest) = c_line.strip_prefix("下载数据 ")
        && let Some(l_body) = l_rest.strip_suffix(" 秒")
    {
        return format!(
            "Downloaded {}s",
            l_body
                .replace(" · 用时 ", " · elapsed ")
                .replace(" 秒 · 等待远端数据 ", "s · remote wait ")
                .replace(" 秒 · 等待本地写入 ", "s · local write ")
        );
    }
    if let Some(l_rest) = c_line.strip_prefix("临时文件创建结果未知或清理失败, 可能遗留: ") {
        return format!("Temp file state unknown or cleanup failed; may remain: {l_rest}");
    }
    if let Some(l_rest) = c_line.strip_prefix("失败 ") {
        if let Some((l_name, l_err)) = l_rest.split_once(": ") {
            return format!("Failed {l_name}: {}", localize_error(l_err));
        }
        return format!("Failed {l_rest}");
    }
    if let Some(l_rest) = c_line.strip_prefix("跳过 ") {
        if let Some((l_path, l_reason)) = l_rest.rsplit_once(": ") {
            let l_en_reason = match l_reason {
                "文件名无效" => "invalid filename".into(),
                "符号链接" => "symlink".into(),
                "特殊文件" => "special file".into(),
                "符号链接或特殊文件" => "symlink or special file".into(),
                "本机同目录已有同名项" => "local item with same name already exists".into(),
                _ => {
                    if let Some(l_depth) = l_reason.strip_prefix("目录层级超过 ").and_then(|c| c.strip_suffix(" 层")) {
                        format!("folder depth exceeds {l_depth} levels")
                    } else {
                        localize_error(l_reason)
                    }
                }
            };
            let l_path = l_path.strip_suffix(" 中的一项").map_or_else(
                || l_path.to_owned(),
                |c| format!("{c} (an entry)"),
            );
            return format!("Skipped {l_path}: {l_en_reason}");
        }
        return format!("Skipped {l_rest}");
    }
    localize_error(c_line)
}

fn localize_cn_duration_body(c_body: &str) -> Option<String> {
    if let Some((l_min, l_sec)) = c_body.split_once(" 分 ") {
        if is_digits(l_min) && is_digits(l_sec) {
            return Some(format!("{l_min}m {l_sec}s"));
        }
        return None;
    }
    is_digits(c_body).then(|| format!("{c_body}s"))
}

fn is_digits(c_text: &str) -> bool {
    !c_text.is_empty() && c_text.bytes().all(|c| c.is_ascii_digit())
}

fn replace_digits(c_text: &mut String, c_prefix: &str, c_suffix: &str, c_fmt: impl Fn(&str) -> String) {
    replace_span(c_text, c_prefix, c_suffix, |c| is_digits(c).then(|| c_fmt(c)));
}

fn replace_span(
    c_text: &mut String,
    c_prefix: &str,
    c_suffix: &str,
    c_fmt: impl Fn(&str) -> Option<String>,
) {
    let mut z_cursor = 0;
    while let Some(l_rel_start) = c_text[z_cursor..].find(c_prefix) {
        let l_start = z_cursor + l_rel_start;
        let l_mid_start = l_start + c_prefix.len();
        let l_line_end = c_text[l_mid_start..]
            .find('\n')
            .map_or(c_text.len(), |c| l_mid_start + c);
        if let Some(l_rel_end) = c_text[l_mid_start..l_line_end].find(c_suffix) {
            let l_mid_end = l_mid_start + l_rel_end;
            let l_end = l_mid_end + c_suffix.len();
            if let Some(l_replacement) = c_fmt(&c_text[l_mid_start..l_mid_end]) {
                c_text.replace_range(l_start..l_end, &l_replacement);
                z_cursor = l_start + l_replacement.len();
                continue;
            }
        }
        z_cursor = l_mid_start;
    }
}

fn replace_folder_summary(c_text: &mut String) {
    if let Some(l_idx) = c_text.find(" 个目录创建失败, ") {
        let l_num_start = c_text[..l_idx]
            .rfind(|c: char| !c.is_ascii_digit())
            .map_or(0, |c| c + 1);
        let l_dirs = &c_text[l_num_start..l_idx];
        let l_after = l_idx + " 个目录创建失败, ".len();
        if is_digits(l_dirs)
            && let Some(l_rel) = c_text[l_after..].find(" 个文件传输失败, 详见错误输出")
        {
            let l_files = &c_text[l_after..l_after + l_rel];
            if is_digits(l_files) {
                let l_rep = format!("{l_dirs} folder(s) and {l_files} file(s) failed; see stderr");
                c_text.replace_range(l_num_start..l_after + l_rel + " 个文件传输失败, 详见错误输出".len(), &l_rep);
            }
        }
    }
    if let Some(l_idx) = c_text.find(" 个文件传输失败, 其余文件已完成, 详见错误输出. 首个错误: ") {
        let l_num_start = c_text[..l_idx]
            .rfind(|c: char| !c.is_ascii_digit())
            .map_or(0, |c| c + 1);
        let l_files = &c_text[l_num_start..l_idx];
        if is_digits(l_files) {
            let l_rep = format!("{l_files} file(s) failed; others completed (see stderr). First error: ");
            c_text.replace_range(l_num_start..l_idx + " 个文件传输失败, 其余文件已完成, 详见错误输出. 首个错误: ".len(), &l_rep);
        }
    }
}

fn replace_table(c_error: &str, c_table: &[(&str, &str)]) -> String {
    let mut z_text = c_error.to_owned();
    for &(l_from, l_to) in c_table {
        if z_text.contains(l_from) {
            z_text = z_text.replace(l_from, l_to).replace(&format!("{l_to}: {l_to}"), l_to);
        }
    }
    z_text
}

// 较长短语优先匹配, 避免子串替换产生中英混杂.
const ZH_TO_EN: &[(&str, &str)] = &[
    // 断连原因、路径与看门狗
    ("保活超时, 长时间未收到服务器任何数据 (网络或代理链路中断)", "Keepalive timed out; no data from server (network or proxy lost)"),
    ("无活动超时, 长时间数据未能发出或未收到数据 (网络或代理链路堵塞)", "Inactivity timeout; no data sent or received (network or proxy stalled)"),
    ("连接被关闭, 未收到断开说明 (服务器、代理或网络中断)", "Connection closed without disconnect notice (server, proxy, or network)"),
    ("SSH 数据流提前结束, 未收到服务器断开通知", "SSH stream ended unexpectedly without server disconnect notice"),
    ("连接被对端或代理重置 (Connection reset)", "Connection reset by peer or proxy"),
    ("本地网络或代理链路已中断", "Local network or proxy link interrupted"),
    ("网络读写超时, 连接已中断", "Network I/O timed out; connection lost"),
    ("服务器长时间未答复通道打开请求, 无法再打开终端或执行命令", "Server did not reply to channel open requests; cannot open terminal or run commands"),
    ("探活超时, 约 3 分钟发出的数据没有得到服务器回应 (连接发送受阻)", "Keepalive timed out (~3m without reply; outbound blocked)"),
    ("探活超时, 约 2 分钟没有收到服务器回应 (网络或代理链路中断)", "Keepalive timed out (~2m without reply; network or proxy lost)"),
    (" · 直连 (命中 Fake-IP/TUN)", " · Direct (Fake-IP/TUN)"),
    (" · 经代理", " · Via Proxy"),
    (" · 直连", " · Direct"),
    ("服务器断开 (", "Server disconnected ("),
    ("网络错误: ", "Network error: "),
    ("独立传输连接已断开: ", "Dedicated transfer link lost: "),
    ("主 SSH 连接已断开: ", "Main SSH link lost: "),
    // 连接、认证、代理与会话
    ("私钥文件不存在或无法读取, 请编辑连接重新选择私钥: ", "Private key file not found or unreadable; edit host to pick a key: "),
    ("私钥必须是 1 MiB 以内的普通文件", "Private key must be a regular file under 1 MiB"),
    ("当前认证方式不能自动建立独立连接", "Current auth method cannot auto-open a dedicated link"),
    ("传输连接认证失败", "Transfer link authentication failed"),
    ("建立独立连接超时", "Dedicated link timed out"),
    ("会话已关闭, 不建立传输连接", "Session closed; skipping transfer link"),
    ("服务器拒绝分配终端 (PTY)", "Server refused to allocate a PTY"),
    ("服务器拒绝启动终端", "Server refused to start shell"),
    ("终端打开失败:", "Failed to open terminal:"),
    ("文件传输仍可使用.", "File transfer is still active."),
    ("标签已重新连接, 请刷新目录", "Tab reconnected; please refresh folder"),
    ("标签已重新连接, 请刷新", "Tab reconnected; please refresh"),
    ("标签已重新连接, 请重试", "Tab reconnected; please retry"),
    ("标签已重新连接", "Tab reconnected"),
    ("会话已断开或已重新连接, 请重试", "Session disconnected or reconnected; please retry"),
    ("标签已连接, 请使用重新连接", "Tab already connected; use Reconnect"),
    ("标签已关闭", "Tab closed"),
    ("最多打开 32 个标签", "Maximum 32 tabs open"),
    ("最多同时连接 8 个会话", "Maximum 8 concurrent connections"),
    ("连接正在使用，断开后再删除", "Host is in use; disconnect before deleting"),
    ("SSH连接超时", "SSH connection timed out"),
    ("SSH会话尚未连接", "SSH session not connected"),
    ("SSH 尚未连接", "SSH not connected"),
    ("无法连接服务器", "Unable to connect to server"),
    ("无法连接代理", "Unable to connect to proxy"),
    ("代理无响应 (握手超过 20 秒)", "Proxy did not respond (handshake > 20s)"),
    ("代理需要认证, 或用户名密码错误", "Proxy requires auth or credentials are invalid"),
    ("代理拒绝连接: ", "Proxy refused connection: "),
    ("代理不是 SOCKS5 代理", "Proxy is not a SOCKS5 server"),
    ("代理用户名或密码过长", "Proxy username or password too long"),
    ("代理认证响应格式无效", "Invalid proxy auth response"),
    ("代理用户名或密码错误", "Invalid proxy username or password"),
    ("代理不接受当前认证方式, 可能需要用户名和密码", "Proxy rejected auth method; username and password may be required"),
    ("代理不接受当前认证方式", "Proxy rejected auth method"),
    ("代理响应头过长", "Proxy response headers too large"),
    ("代理响应格式无效", "Invalid proxy response"),
    ("代理规则不允许", "Blocked by proxy rules"),
    ("代理所在网络不可达", "Proxy network unreachable"),
    ("代理无法到达服务器", "Proxy cannot reach server"),
    ("服务器拒绝连接", "Server refused connection"),
    ("代理连接服务器超时", "Proxy connection to server timed out"),
    ("代理连接失败", "Proxy connection failed"),
    ("连接失败", "Connection failed"),
    ("代理关闭了连接", "Proxy closed the connection"),
    ("SOCKS5 错误码 ", "SOCKS5 error code "),
    ("连接已断开", "Connection lost"),
    ("连接超时", "Connection timed out"),
    ("会话已断开", "Session disconnected"),
    ("会话不存在", "Session not found"),
    ("登录认证已取消", "Authentication cancelled"),
    ("认证答案超出限制", "Too many auth responses"),
    ("当前没有待确认的主机指纹", "No pending host key verification"),
    ("主机指纹已变化, 请重新核对", "Host key changed; verify again"),
    // 终端与 MCP
    ("终端通道已被服务器关闭", "Terminal channel closed by server"),
    ("终端已重新打开, 之前的输入已取消", "Terminal reopened; previous input cancelled"),
    ("该会话为仅文件连接, 终端未打开; 可用 run_command 执行命令", "Files-only session without terminal; use run_command"),
    ("终端未打开, 可右键标签选择“重新连接终端”", "Terminal is closed; right-click tab to reopen"),
    ("终端未打开", "Terminal is not open"),
    ("终端已打开", "Terminal is already open"),
    ("终端输入通道已关闭", "Terminal input channel closed"),
    ("终端输入队列已满", "Terminal input queue full"),
    ("终端输入单次不能超过 16 KiB", "Terminal input cannot exceed 16 KiB"),
    ("用户正在输入, 请稍后再操作此终端", "User is typing; wait before writing to terminal"),
    ("此连接未设置管理员密码, 可在编辑连接中设置", "Admin password not configured; set it in Edit Host"),
    ("此连接不是密码登录, 管理员密码需单独设置", "Host does not use password login; set a dedicated admin password"),
    ("终端当前不是密码提示, 未输入", "Terminal is not at a password prompt; skipped"),
    ("距上次代填不足 15 秒, 可能密码错误, 请用户自行输入", "Filled less than 15s ago; password may be wrong, enter manually"),
    ("此连接已撤销 MCP 授权", "MCP access revoked for this host"),
    ("MCP 授权已撤销", "MCP access revoked"),
    ("此连接未授权 MCP", "MCP access not enabled for this host"),
    ("MCP 服务未启动", "MCP server not running"),
    ("MCP 服务已关闭", "MCP server stopped"),
    ("禁止 MCP 访问 HiPShell 配置目录", "MCP cannot access HiPShell config directory"),
    ("此操作仅限用户", "User action required"),
    // 传输、SFTP、文件操作与命令
    ("打开 SFTP 通道失败", "Failed to open SFTP channel"),
    ("服务器拒绝启动 SFTP 子系统", "Server refused to start SFTP subsystem"),
    ("启动 SFTP 时通道已关闭", "Channel closed while starting SFTP"),
    ("SFTP 写入 60 秒无进展", "SFTP write stalled for 60s"),
    ("SFTP 关闭 60 秒无进展", "SFTP close stalled for 60s"),
    ("SFTP 通道已结束", "SFTP channel closed"),
    ("SSH TCP 写入长时间无进展", "SSH TCP write stalled"),
    ("上传 60 秒未收到写入确认", "Upload write ack timed out (60s)"),
    ("上传数据不完整", "Uploaded data incomplete"),
    ("上传源在传输期间被截断", "Upload source truncated during transfer"),
    ("上传源在传输期间被修改", "Upload source modified during transfer"),
    ("上传源必须是普通文件", "Upload source must be a regular file"),
    ("上传源不存在", "Upload source not found"),
    ("下载源在传输期间被截断", "Download source truncated during transfer"),
    ("下载源必须是普通文件", "Download source must be a regular file"),
    ("服务器返回的数据块长度超出请求范围", "Server returned data chunk larger than requested"),
    ("服务器未返回文件长度", "Server did not return file size"),
    ("续传后远端临时文件长度不符", "Remote temp file size mismatch after resume"),
    ("源文件在中断期间被修改, 无法续传", "Source file changed during interruption; cannot resume"),
    ("打开远端源文件超时", "Timed out opening remote file"),
    ("读取远端文件信息超时", "Timed out reading remote file info"),
    ("检查远端临时文件超时", "Timed out checking remote temp file"),
    ("读取远端文件超时", "Timed out reading remote file"),
    ("读取远端文件失败", "Failed to read remote file"),
    ("写入远端文件失败", "Failed to write remote file"),
    ("关闭远端文件超时", "Timed out closing remote file"),
    ("关闭远端文件失败", "Failed to close remote file"),
    ("写入本地文件超时", "Timed out writing local file"),
    ("读取本地文件超时", "Timed out reading local file"),
    ("刷新本地文件超时", "Timed out flushing local file"),
    ("创建本地临时文件任务异常", "Local temp file task failed"),
    ("创建本地临时文件失败", "Failed to create local temp file"),
    ("打开本地临时文件失败", "Failed to open local temp file"),
    ("读取本地临时文件失败", "Failed to read local temp file"),
    ("定位本地临时文件失败", "Failed to seek local temp file"),
    ("截断本地临时文件失败", "Failed to truncate local temp file"),
    ("读取本地文件任务异常", "Local file reader task failed"),
    ("创建本地目标失败", "Failed to create local destination"),
    ("本地文件改名失败", "Failed to rename local file"),
    ("提交上传任务异常", "Upload commit task failed"),
    ("提交下载任务异常", "Download commit task failed"),
    ("连接异常, 无法确认提交结果: ", "Connection lost; commit result unknown: "),
    ("无法确认提交结果: ", "Commit result unknown: "),
    ("恢复旧文件失败: ", "Failed to restore previous file: "),
    ("远端根目录在之前的中断中创建结果未知, 现已存在, 请核对后重试", "Remote root folder already exists after prior interruption; verify and retry"),
    ("远端目标已是目录, 无法作为文件覆写", "Remote destination is a folder; cannot overwrite as file"),
    ("远端目标是符号链接, 无法覆写", "Remote destination is a symlink; cannot overwrite"),
    ("远端目标已存在或无法创建", "Remote destination already exists or cannot be created"),
    ("远端目标已存在", "Remote destination already exists"),
    ("本地目标必须是普通文件", "Local destination must be a regular file"),
    ("本地目标必须是普通目录", "Local destination must be a regular folder"),
    ("本地目标已存在", "Local destination already exists"),
    ("下载目标已存在", "Download destination already exists"),
    ("目标名称已存在", "Target name already exists"),
    ("新建远端目录失败: ", "Failed to create remote folder: "),
    ("新建目录失败: ", "Failed to create folder: "),
    ("等待其他文件操作完成超时", "Timed out waiting for another file operation"),
    ("等待目录浏览通道超时", "Timed out waiting for browse channel"),
    ("让出目录浏览通道后重试仍失败", "Retry still failed after releasing browse channel"),
    ("等待目录浏览结束超时", "timed out waiting for folder listing"),
    ("主连接", "Main link "),
    (", 改用独立连接也失败", "; dedicated link also failed"),
    ("查询属主与时间超时", "Timed out loading file owner and timestamps"),
    ("解析远端目录超时", "Timed out resolving remote path"),
    ("远端目录没有返回实际路径", "Remote server did not return resolved path"),
    ("远端目录路径无效", "Invalid remote folder path"),
    ("远端父目录路径过长", "Remote parent path too long"),
    ("远端父目录路径包含控制字符", "Remote parent path contains control characters"),
    ("远端父目录返回的实际路径无效", "Invalid resolved remote parent path"),
    ("远端路径长度无效", "Invalid remote path length"),
    ("远端路径包含控制字符", "Remote path contains control characters"),
    ("远端路径包含相对跳转分量", "Remote path cannot contain relative segments"),
    ("远端路径缺少文件名", "Remote path missing filename"),
    ("新路径过长", "New path too long"),
    ("新名称不能为空、为.或..、含路径分隔符或控制字符, 且最多255字节", "Name must be non-empty, <= 255 bytes, and not . / .. or contain separators"),
    ("权限值超出0777", "Permissions must be <= 0777"),
    ("拒绝修改符号链接权限", "Refusing to change symlink permissions"),
    ("仅支持普通文件、符号链接或空目录", "Only regular files, symlinks, or empty folders are supported"),
    ("整体操作超时，未处理", "Batch operation timed out; skipped"),
    ("删除超时", "Delete timed out"),
    ("快速删除仅接受绝对路径", "Fast delete requires an absolute path"),
    ("仅接受绝对路径", "Absolute path required"),
    ("创建副本失败: ", "Duplicate failed: "),
    ("本机路径必须是有效绝对路径", "Local path must be a valid absolute path"),
    ("本机路径必须是绝对路径", "Local path must be absolute"),
    ("本机路径不能包含上级目录跳转 (..)", "Local path cannot contain '..'"),
    ("本机路径无效", "Invalid local path"),
    ("本地路径无效", "Invalid local path"),
    ("请选择本地目录", "Please select a local folder"),
    ("文件名无效", "Invalid filename"),
    ("一次最多检查 2000 个名称", "Up to 2000 names can be checked at once"),
    ("等待服务器响应超时", "Server response timed out"),
    ("任务队列已满", "Task queue is full"),
    ("任务已取消", "Task cancelled"),
    ("服务器拒绝执行命令", "Server refused to run command"),
    ("远程命令被信号终止: ", "Remote command killed by signal: "),
    ("远程命令超时", "Remote command timed out"),
    ("命令通道已关闭, 未收到退出码", "Command channel closed without exit code"),
    ("命令执行失败", "Command failed"),
    ("命令为空、含 NUL 或超过 8 KiB", "Command is empty, contains NUL, or exceeds 8 KiB"),
    ("命令为空或超过 16 KiB", "Command is empty or exceeds 16 KiB"),
    // 时区与监控
    ("服务器时区查询失败, 修改时间按 UTC 显示: ", "Failed to query server timezone; showing UTC times: "),
    ("服务器没有可用的 GNU date/Python 3 批量转换或命令执行失败", "Server lacks GNU date/Python 3 or command failed"),
    ("服务器时区名称无效", "Invalid server timezone"),
    ("服务器时间格式无效", "Invalid server time format"),
    ("服务器时间缺少有效UTC偏移", "Server time missing valid UTC offset"),
    ("服务器时间戳重复或超出请求范围", "Duplicate or out-of-range server timestamps"),
    ("服务器没有返回全部文件时间", "Server did not return all file timestamps"),
    ("远端文件时间输入超过512KiB上限", "Remote file timestamp input exceeded 512 KiB"),
    ("服务器不支持 /proc 系统监控", "Server does not support /proc monitoring"),
    ("远端监控无响应", "Remote monitor timed out"),
    ("远端监控采样输出超过1MiB上限", "Monitor sample output exceeded 1 MiB"),
    ("服务器拒绝执行监控命令", "Server refused monitor command"),
    ("远端监控已结束", "Remote monitor stopped"),
    ("远端监控采样未完整返回", "Incomplete monitor sample"),
    ("远端 /proc/meminfo 不可用", "Remote /proc/meminfo unavailable"),
    ("远端进程采样参数不可用", "Remote process metrics unavailable"),
    // 配置、分组、代理与常用命令校验
    ("连接名称无效", "Invalid host name"),
    ("主机地址无效", "Invalid host address"),
    ("服务器地址过长", "Host address too long"),
    ("端口不能为 0", "Port cannot be 0"),
    ("端口需在 1024-65535 之间", "Port must be between 1024 and 65535"),
    ("用户名无效", "Invalid username"),
    ("未选择私钥文件", "No private key selected"),
    ("分组名称不能为空", "Group name cannot be empty"),
    ("分组名称过长", "Group name is too long"),
    ("分组名称无效", "Invalid group name"),
    ("分组名称重复", "Group name already exists"),
    ("连接分组最多 64 个", "Maximum 64 host groups allowed"),
    ("默认分组不能改名", "Default group cannot be renamed"),
    ("默认分组不能删除", "Default group cannot be deleted"),
    ("分组顺序无效", "Invalid group order"),
    ("连接顺序无效", "Invalid host order"),
    ("连接 ID 无效", "Invalid host ID"),
    ("连接 ID 与文件名不一致", "Host ID does not match filename"),
    ("请输入管理员密码", "Please enter admin password"),
    ("管理员密码过长或含换行", "Admin password too long or contains line breaks"),
    ("代理名称为空或过长", "Proxy name is empty or too long"),
    ("代理名称重复", "Proxy name already exists"),
    ("代理类型只支持 SOCKS5 或 HTTP", "Proxy type must be SOCKS5 or HTTP"),
    ("代理地址无效", "Invalid proxy host"),
    ("代理端口不能为 0", "Proxy port cannot be 0"),
    ("代理用户名无效", "Invalid proxy username"),
    ("代理密码过长", "Proxy password too long"),
    ("代理不存在, 请刷新后重新选择", "Proxy not found; refresh and select again"),
    ("连接所选的代理已删除, 请重新选择代理", "Selected proxy was deleted; pick another"),
    ("代理不存在", "Proxy not found"),
    ("下载目录不存在或不是绝对路径", "Download folder does not exist or is not absolute"),
    ("下载目录路径无效", "Invalid download folder path"),
    ("历史行数只能是 1000、2000、5000 或 10000", "Scrollback must be 1000, 2000, 5000, or 10000"),
    ("同时传输数范围为 ", "Concurrent transfers must be "),
    ("单次传输数量范围为 ", "Max items per batch must be "),
    ("字体名称无效", "Invalid font name"),
    ("字号范围为 9-28", "Font size must be between 9 and 28"),
    ("常用命令分组不存在, 请先新增分组", "Command group not found; create one first"),
    ("常用命令分组不存在", "Command group not found"),
    ("常用命令分组名称重复", "Command group already exists"),
    ("常用命令最多保存 256 条", "Maximum 256 commands allowed"),
    ("常用命令超过 256 条", "Commands exceed 256 limit"),
    ("常用命令最多保存 64 个分组", "Maximum 64 command groups allowed"),
    ("常用命令分组超过 64 个", "Command groups exceed 64 limit"),
    ("至少保留一个命令分组", "At least one command group is required"),
    ("常用命令不存在", "Command not found"),
    ("命令名称为空或过长", "Command name is empty or too long"),
    ("命令分组名称超过 128 字节", "Command group name exceeds 128 bytes"),
    ("命令分组名称不能含控制字符或路径分隔符", "Command group name cannot contain control chars or separators"),
    ("读取常用命令失败, 文件格式不正确", "Failed to read commands; invalid file format"),
    ("保存常用命令失败", "Failed to save commands"),
    ("写入导出文件失败", "Failed to write export file"),
    ("命令文件超过 1 MiB", "Commands file exceeds 1 MiB"),
    ("命令配置超过 1 MiB", "Commands config exceeds 1 MiB"),
    // FinalShell 导入导出与字体下载
    ("仅支持 FinalShell SSH 连接 (conection_type=100)", "Only FinalShell SSH hosts (conection_type=100) are supported"),
    ("FinalShell 记录引用了未解析的代理 ID", "FinalShell entry references an unresolved proxy ID"),
    ("FinalShell 字符编码暂不支持", "Unsupported FinalShell charset"),
    ("FinalShell 文件没有连接记录", "No host entries found in FinalShell file"),
    ("FinalShell 顶层必须是对象或数组", "FinalShell root must be an object or array"),
    ("FinalShell 记录格式无效", "Invalid FinalShell entry format"),
    ("读取 FinalShell 文件失败", "Failed to read FinalShell file"),
    ("解析 FinalShell JSON 失败", "Failed to parse FinalShell JSON"),
    ("解析 FinalShell 连接记录失败", "Failed to parse FinalShell host entry"),
    ("解析 FinalShell 密码失败", "Failed to parse FinalShell password"),
    ("密码解析失败 ", "Password decode failed: "),
    ("没有可导入的连接", "No importable hosts found"),
    ("没有可导出的连接", "No hosts to export"),
    ("当前连接没有可导出的密码", "Current host has no exportable password"),
    ("导入路径必须是普通文件或目录", "Import path must be a regular file or folder"),
    ("导入路径没有连接配置", "No host configs found in import path"),
    ("导出路径必须是普通目录", "Export path must be a regular folder"),
    ("导出目标已存在或不可访问: ", "Export destination exists or is inaccessible: "),
    ("单次最多导入 512 条连接", "Up to 512 hosts can be imported at once"),
    ("单次最多导出 512 条连接", "Up to 512 hosts can be exported at once"),
    ("单次最多处理 512 条连接", "Up to 512 hosts can be processed at once"),
    ("连接配置最多保存 512 条", "Maximum 512 hosts allowed"),
    ("单次导入总大小不能超过 32 MiB", "Total import size cannot exceed 32 MiB"),
    ("单次导出总大小不能超过 32 MiB", "Total export size cannot exceed 32 MiB"),
    ("不支持下载该字体", "Font download not supported"),
    ("下载失败, 请检查网络或代理", "Download failed; check network or proxy"),
    ("下载文件过大", "Downloaded file is too large"),
    ("解压失败", "Extraction failed"),
    ("发布包中缺少 ", "Release package missing "),
    (" 不是有效的 TrueType 字体", " is not a valid TrueType font"),
    ("命令退出码 ", "Command exited with "),
    ("被占用, 请换一个", "is in use; pick another port"),
    ("端口 ", "Port "),
];

const EN_TO_ZH: &[(&str, &str)] = &[
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
