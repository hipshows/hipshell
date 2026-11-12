use crate::{
    config::ConfigStore,
    model::{AdminPassword, AuthMethod, Connection},
};
use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use des::{
    Des,
    cipher::{BlockDecrypt, BlockEncrypt, KeyInit, generic_array::GenericArray},
};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
};
use zeroize::{Zeroize, Zeroizing};

const MAX_FILE_SIZE: u64 = 1024 * 1024;
pub(super) const MAX_CONNECTIONS: usize = 512;
const MAX_IMPORT_BYTES: usize = 32 * 1024 * 1024;
const GROUP_DESCRIPTION_PREFIX: &str = "HiPssh group: ";

#[derive(Deserialize)]
struct FinalShellRecord {
    #[serde(default)]
    conection_type: i64,
    name: String,
    host: String,
    port: u16,
    #[serde(default)]
    user_name: String,
    #[serde(default)]
    password: String,
    #[serde(default = "default_auth_type")]
    authentication_type: i64,
    #[serde(default)]
    terminal_encoding: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    proxy_id: Option<String>,
    #[serde(default)]
    hipshell: Option<NativeOptions>,
}

#[derive(Serialize, Deserialize)]
struct NativeOptions {
    id: String,
    #[serde(default)]
    group: String,
    #[serde(default)]
    auth: AuthMethod,
    #[serde(default)]
    private_key_path: Option<String>,
    #[serde(default)]
    mcp_allowed: bool,
    #[serde(default)]
    mcp_identity: Option<String>,
    // HiPShell 代理管理中的代理 ID; 与 FinalShell 自己的 proxy_id 无关.
    #[serde(default)]
    proxy: Option<String>,
    #[serde(default)]
    admin: AdminPassword,
    // 单独设置的管理员密码, 与 password 同样按 FinalShell 方式加密.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    admin_password: String,
}

fn default_auth_type() -> i64 {
    1
}

pub(super) fn native_records(c_path: &Path) -> Result<(Vec<serde_json::Value>, String)> {
    let z_bytes = read_file(c_path)?;
    let z_value: serde_json::Value =
        serde_json::from_slice(&z_bytes).context("解析 FinalShell JSON 失败")?;
    let z_records = match z_value {
        serde_json::Value::Array(records) => records,
        serde_json::Value::Object(_) => vec![z_value],
        _ => anyhow::bail!("FinalShell 顶层必须是对象或数组"),
    };
    ensure!(!z_records.is_empty(), "FinalShell 文件没有连接记录");
    ensure!(
        z_records.len() <= MAX_CONNECTIONS,
        "单次最多处理 512 条连接"
    );
    ensure!(
        z_records.iter().all(serde_json::Value::is_object),
        "FinalShell 记录格式无效"
    );
    Ok((z_records, format!("{:x}", md5::compute(z_bytes))))
}

pub(super) fn native_record_connection(
    c_value: &serde_json::Value,
    c_id: &str,
) -> Result<Connection> {
    let z_record: FinalShellRecord =
        serde_json::from_value(c_value.clone()).context("解析 FinalShell 连接记录失败")?;
    ensure!(
        z_record.conection_type == 100,
        "仅支持 FinalShell SSH 连接 (conection_type=100)"
    );
    // authentication_type: 1 密码, 2 私钥. 私钥存在 FinalShell 自己的密钥库 (secret_key_id), 无法迁移;
    // 私钥连接导入为"私钥认证、未选私钥", 连接时提示选择本地私钥. 密码连接残留的 secret_key_id 直接忽略.
    let z_auth = match z_record.authentication_type {
        1 => AuthMethod::Password,
        2 => AuthMethod::PrivateKey,
        _ if z_record.hipshell.is_some() => AuthMethod::Password,
        l_type => anyhow::bail!("FinalShell 认证方式 {l_type} 暂不支持"),
    };
    let z_proxy = z_record
        .proxy_id
        .as_deref()
        .map(str::trim)
        .filter(|id| !id.is_empty() && *id != "0" && *id != "-1");
    ensure!(z_proxy.is_none(), "FinalShell 记录引用了未解析的代理 ID");
    if let Some(z_encoding) = z_record.terminal_encoding.as_deref() {
        ensure!(
            z_encoding.is_empty()
                || z_encoding.eq_ignore_ascii_case("UTF-8")
                || z_encoding.eq_ignore_ascii_case("UTF8"),
            "FinalShell 字符编码暂不支持"
        );
    }
    let z_group = z_record
        .description
        .as_deref()
        .and_then(|desc| desc.strip_prefix(GROUP_DESCRIPTION_PREFIX))
        .unwrap_or("默认")
        .to_owned();
    let mut z_conn = Connection {
        id: c_id.to_owned(),
        name: z_record.name,
        host: z_record.host,
        port: z_record.port,
        username: z_record.user_name,
        group: z_group,
        auth: z_auth,
        private_key_path: None,
        mcp_allowed: false,
        proxy_id: None,
        admin_password: AdminPassword::None,
    };
    if let Some(z_options) = z_record.hipshell {
        uuid::Uuid::parse_str(&z_options.id).context("HiPShell 连接 ID 无效")?;
        z_conn.group = z_options.group;
        z_conn.auth = z_options.auth;
        z_conn.private_key_path = z_options.private_key_path;
        z_conn.proxy_id = z_options.proxy.filter(|c| !c.is_empty());
        z_conn.admin_password = z_options.admin;
        // 外部修改连接身份后, 文件中的旧授权不能继续用于新主机或认证方式.
        z_conn.mcp_allowed = z_options.mcp_allowed
            && z_options.mcp_identity.as_deref() == Some(connection_identity(&z_conn)?.as_str());
    }
    if z_conn.group.trim().is_empty() {
        z_conn.group = "默认".into();
    }
    z_conn.validate()?;
    Ok(z_conn)
}

fn connection_identity(c_conn: &Connection) -> Result<String> {
    let z_bytes = serde_json::to_vec(&(
        &c_conn.host,
        c_conn.port,
        &c_conn.username,
        &c_conn.auth,
        &c_conn.private_key_path,
    ))?;
    Ok(format!("{:x}", md5::compute(z_bytes)))
}

pub(super) fn native_secret(c_path: &Path, c_index: usize) -> Result<Option<Zeroizing<String>>> {
    let (z_records, _) = native_records(c_path)?;
    let z_value = z_records
        .get(c_index)
        .context("FinalShell 连接索引已失效")?;
    record_secret(z_value)
}

fn record_secret(c_value: &serde_json::Value) -> Result<Option<Zeroizing<String>>> {
    let z_record: FinalShellRecord =
        serde_json::from_value(c_value.clone()).context("解析 FinalShell 密码失败")?;
    if z_record.password.is_empty() {
        Ok(None)
    } else {
        Ok(Some(Zeroizing::new(decrypt_password(&z_record.password)?)))
    }
}

// c_password / c_admin (登录密码 / 管理员密码): None 保留文件中原有密码, Some("") 清空, 其余按 FinalShell 兼容格式加密写入.
pub(super) fn write_native_record(
    c_path: &Path,
    c_original: Option<&serde_json::Value>,
    c_conn: &Connection,
    c_password: Option<&str>,
    c_admin: Option<&str>,
    c_expected_fingerprint: Option<&str>,
    c_overwrite: bool,
) -> Result<()> {
    if let Some(z_expected) = c_expected_fingerprint {
        ensure!(
            format!("{:x}", md5::compute(read_file(c_path)?)) == z_expected,
            "连接文件已在外部修改, 请重新打开确认"
        );
    }
    let z_encode = |c: Option<&str>| c.map(|c| if c.is_empty() { Ok(String::new()) } else { encrypt_password(c) }).transpose();
    let z_bytes = encode_record(c_original, c_conn, z_encode(c_password)?.as_deref(), z_encode(c_admin)?.as_deref())?;
    if c_overwrite {
        write_atomic(c_path, &z_bytes)
    } else {
        write_new(c_path, &z_bytes)
    }
}

// c_password / c_admin 为已加密的密码, None 保留原记录中的值.
fn encode_record(
    c_original: Option<&serde_json::Value>,
    c_conn: &Connection,
    c_password: Option<&str>,
    c_admin: Option<&str>,
) -> Result<Vec<u8>> {
    let mut z_map = c_original
        .and_then(serde_json::Value::as_object)
        .cloned()
        .unwrap_or_default();
    // 不再单独设置管理员密码时一并清除已保存的.
    let z_admin = match c_admin {
        _ if c_conn.admin_password != AdminPassword::Custom => String::new(),
        Some(l_admin) => l_admin.to_owned(),
        None => z_map
            .get("hipshell")
            .and_then(|c| c.get("admin_password"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned(),
    };
    z_map.insert("conection_type".into(), serde_json::json!(100));
    z_map.insert("name".into(), serde_json::json!(c_conn.name));
    z_map.insert("host".into(), serde_json::json!(c_conn.host));
    z_map.insert("port".into(), serde_json::json!(c_conn.port));
    z_map.insert("user_name".into(), serde_json::json!(c_conn.username));
    // 随当前认证方式更新 (1 密码, 2 私钥); FinalShell 没有键盘交互类型, 按密码记录.
    z_map.insert(
        "authentication_type".into(),
        serde_json::json!(if c_conn.auth == AuthMethod::PrivateKey { 2 } else { 1 }),
    );
    z_map
        .entry("terminal_encoding")
        .or_insert(serde_json::json!("UTF-8"));
    let z_description = z_map.get("description").and_then(serde_json::Value::as_str);
    if z_description.is_none_or(|s| s.is_empty() || s.starts_with(GROUP_DESCRIPTION_PREFIX)) {
        z_map.insert(
            "description".into(),
            serde_json::json!(format!("{GROUP_DESCRIPTION_PREFIX}{}", c_conn.group)),
        );
    }
    if let Some(z_password) = c_password {
        z_map.insert("password".into(), serde_json::json!(z_password));
    } else {
        z_map.entry("password").or_insert(serde_json::json!(""));
    }
    z_map.insert(
        "hipshell".into(),
        serde_json::to_value(NativeOptions {
            id: c_conn.id.clone(),
            group: c_conn.group.clone(),
            auth: c_conn.auth.clone(),
            private_key_path: c_conn.private_key_path.clone(),
            mcp_allowed: c_conn.mcp_allowed,
            mcp_identity: c_conn
                .mcp_allowed
                .then(|| connection_identity(c_conn))
                .transpose()?,
            proxy: c_conn.proxy_id.clone(),
            admin: c_conn.admin_password,
            admin_password: z_admin,
        })?,
    );
    let z_bytes = serde_json::to_vec_pretty(&z_map).context("编码 FinalShell JSON 失败")?;
    ensure!(
        z_bytes.len() as u64 <= MAX_FILE_SIZE,
        "FinalShell 文件超过 1 MiB 限制"
    );
    Ok(z_bytes)
}

// 返回已导入的连接和跳过的记录 (文件名: 原因). 个别记录无法导入 (如 RDP、非 UTF-8 编码、引用代理) 时跳过,
// 其余照常导入, 避免文件夹中一条记录导致整批失败; 全部无法导入时报错.
pub(super) fn import(
    c_store: &ConfigStore,
    c_path: &Path,
    c_group: Option<&str>,
) -> Result<(Vec<Connection>, Vec<String>)> {
    let z_records = import_records(c_path)?;
    let mut z_pending = Vec::with_capacity(z_records.len());
    let mut z_skipped = Vec::new();
    for (z_path, z_value) in z_records {
        let z_id = uuid::Uuid::new_v4().to_string();
        let z_name = z_path.file_name().map(|c| c.to_string_lossy().into_owned()).unwrap_or_default();
        let mut z_conn = match native_record_connection(&z_value, &z_id) {
            Ok(l_conn) => l_conn,
            Err(err) => {
                z_skipped.push(format!("{z_name}: {err:#}"));
                continue;
            }
        };
        // 在连接管理中选中了分组时导入到该分组, 否则沿用文件自带的分组.
        if let Some(l_group) = c_group.filter(|c| !c.trim().is_empty()) {
            z_conn.group = l_group.to_owned();
        }
        // 文件自身不提供操作授权, 认证方式和本地私钥路径可保留; 文件中的密码直接沿用.
        z_conn.mcp_allowed = false;
        let z_secret = if z_conn.auth == AuthMethod::Password {
            match record_secret(&z_value) {
                Ok(l_secret) => l_secret,
                Err(err) => {
                    z_skipped.push(format!("{z_name}: 密码解析失败 {err:#}"));
                    continue;
                }
            }
        } else {
            None
        };
        z_pending.push((z_conn, z_value, z_secret));
    }
    ensure!(
        !z_pending.is_empty(),
        "没有可导入的连接{}",
        z_skipped.first().map(|c| format!(": {c}")).unwrap_or_default()
    );
    ensure!(
        c_store.list().len() + z_pending.len() <= MAX_CONNECTIONS,
        "连接配置最多保存 512 条"
    );
    let mut z_connections = Vec::with_capacity(z_pending.len());
    for (z_conn, z_original, z_secret) in z_pending {
        let z_count = z_connections.len();
        let z_conn = c_store
            .save_imported(z_conn, &z_original, z_secret.as_deref().map(String::as_str))
            .with_context(|| format!("已导入 {z_count} 条连接, 保存后续连接失败"))?;
        z_connections.push(z_conn);
    }
    Ok((z_connections, z_skipped))
}

fn import_records(c_path: &Path) -> Result<Vec<(PathBuf, serde_json::Value)>> {
    let z_meta = fs::symlink_metadata(c_path).context("读取导入路径失败")?;
    let z_directory = z_meta.is_dir() && !z_meta.file_type().is_symlink();
    ensure!(
        z_directory || z_meta.file_type().is_file(),
        "导入路径必须是普通文件或目录"
    );
    let mut z_files = Vec::new();
    if z_directory {
        let mut z_directories = vec![c_path.to_path_buf()];
        let mut z_entries = 0usize;
        let mut z_directory_count = 0usize;
        while let Some(z_directory) = z_directories.pop() {
            z_directory_count += 1;
            ensure!(z_directory_count <= 128, "导入目录最多包含 128 个子目录");
            for z_entry in fs::read_dir(&z_directory).context("读取导入目录失败")? {
                let z_entry = z_entry.context("读取导入目录条目失败")?;
                z_entries += 1;
                ensure!(z_entries <= 2048, "导入目录最多包含 2048 个条目");
                let z_type = z_entry.file_type().context("读取导入文件类型失败")?;
                if z_type.is_dir() {
                    z_directories.push(z_entry.path());
                } else if z_type.is_file() {
                    let z_path = z_entry.path();
                    if z_path.extension().and_then(|s| s.to_str()) == Some("json")
                        && !matches!(
                            z_path.file_name().and_then(|s| s.to_str()),
                            Some(
                                "settings.json"
                                    | "known_hosts.json"
                                    | "commands.json"
                            )
                        )
                    {
                        z_files.push(z_path);
                        ensure!(
                            z_files.len() <= MAX_CONNECTIONS,
                            "导入目录最多包含 512 个 JSON 文件"
                        );
                    }
                }
            }
        }
        z_files.sort();
    } else {
        z_files.push(c_path.to_path_buf());
    }
    let mut z_records = Vec::new();
    let mut z_total_bytes = 0usize;
    for z_path in z_files {
        let z_bytes = read_file(&z_path)
            .with_context(|| format!("读取导入文件 {} 失败", z_path.display()))?;
        z_total_bytes += z_bytes.len();
        ensure!(
            z_total_bytes <= MAX_IMPORT_BYTES,
            "单次导入总大小不能超过 32 MiB"
        );
        let z_value: serde_json::Value = match serde_json::from_slice(&z_bytes) {
            Ok(z_value) => z_value,
            Err(_) if z_directory && !is_connection_filename(&z_path) => continue,
            Err(err) => {
                return Err(err).with_context(|| format!("解析导入文件 {} 失败", z_path.display()));
            }
        };
        let z_values = match z_value {
            serde_json::Value::Array(values) => values,
            serde_json::Value::Object(_) => vec![z_value],
            _ if z_directory && !is_connection_filename(&z_path) => continue,
            _ => anyhow::bail!("导入文件 {} 顶层必须是对象或数组", z_path.display()),
        };
        ensure!(
            z_values.len() <= MAX_CONNECTIONS,
            "单个导入文件最多包含 512 条记录"
        );
        ensure!(
            !z_values.is_empty() || (z_directory && !is_connection_filename(&z_path)),
            "导入文件 {} 没有连接记录",
            z_path.display()
        );
        for (i, z_value) in z_values.into_iter().enumerate() {
            let z_connection = z_value.get("conection_type").is_some()
                || z_value.get("hipshell").is_some()
                || z_value.get("host").is_some();
            if !z_connection && z_directory && !is_connection_filename(&z_path) {
                continue;
            }
            ensure!(
                z_value.is_object() && z_connection,
                "导入文件 {} 第 {} 条不是有效连接",
                z_path.display(),
                i + 1
            );
            z_records.push((z_path.clone(), z_value));
            ensure!(
                z_records.len() <= MAX_CONNECTIONS,
                "单次最多导入 512 条连接"
            );
        }
    }
    ensure!(!z_records.is_empty(), "导入路径没有连接配置");
    Ok(z_records)
}

fn is_connection_filename(c_path: &Path) -> bool {
    c_path
        .file_stem()
        .and_then(|s| s.to_str())
        .is_some_and(|s| s.ends_with("_connect_config") || uuid::Uuid::parse_str(s).is_ok())
}

pub(super) fn export(
    c_store: &ConfigStore,
    c_id: &str,
    c_path: &Path,
    c_include_password: bool,
) -> Result<()> {
    let z_bytes = export_bytes(c_store, c_id, c_include_password)?;
    write_atomic(c_path, &z_bytes)
}

fn export_bytes(c_store: &ConfigStore, c_id: &str, c_include_password: bool) -> Result<Vec<u8>> {
    let z_conn = c_store.get(c_id)?;
    let z_original = c_store.native_record(c_id)?;
    let z_password = if c_include_password && z_conn.auth == AuthMethod::Password {
        let z_password = c_store.secret(c_id)?;
        ensure!(z_password.is_some(), "当前连接没有可导出的密码");
        z_password
    } else {
        None
    };
    let z_encrypted = z_password
        .as_deref()
        .map(|s| encrypt_password(s.as_str()))
        .transpose()?;
    // 不导出密码时管理员密码也一并去掉.
    encode_record(
        z_original.as_ref(),
        &z_conn,
        Some(z_encrypted.as_deref().unwrap_or("")),
        (!c_include_password).then_some(""),
    )
}

// c_group 为 Some 时只导出该分组, 每条连接一个文件.
pub(super) fn export_all(
    c_store: &ConfigStore,
    c_directory: &Path,
    c_include_password: bool,
    c_group: Option<&str>,
) -> Result<usize> {
    let z_connections = c_store
        .list()
        .into_iter()
        .filter(|c| c_group.is_none_or(|l| c.group == l))
        .collect::<Vec<_>>();
    ensure!(
        z_connections.len() <= MAX_CONNECTIONS,
        "单次最多导出 512 条连接"
    );
    ensure!(!z_connections.is_empty(), "没有可导出的连接");
    if c_directory.exists() {
        let z_meta = fs::symlink_metadata(c_directory).context("读取导出目录失败")?;
        ensure!(
            z_meta.is_dir() && !z_meta.file_type().is_symlink(),
            "导出路径必须是普通目录"
        );
    }
    let mut z_pending = Vec::with_capacity(z_connections.len());
    let mut z_total_bytes = 0usize;
    for z_conn in z_connections {
        let z_path = c_directory.join(format!("{}_connect_config.json", z_conn.id));
        ensure!(
            fs::symlink_metadata(&z_path)
                .is_err_and(|err| err.kind() == std::io::ErrorKind::NotFound),
            "导出目标已存在或不可访问: {}",
            z_path.display()
        );
        let z_bytes = export_bytes(c_store, &z_conn.id, c_include_password)
            .with_context(|| format!("准备导出连接 {} 失败", z_conn.name))?;
        z_total_bytes += z_bytes.len();
        ensure!(
            z_total_bytes <= MAX_IMPORT_BYTES,
            "单次导出总大小不能超过 32 MiB"
        );
        z_pending.push((z_path, z_bytes));
    }
    fs::create_dir_all(c_directory).context("创建导出目录失败")?;
    for (i, (z_path, z_bytes)) in z_pending.iter().enumerate() {
        write_new(z_path, z_bytes)
            .with_context(|| format!("已导出 {i} 条连接, 写入 {} 失败", z_path.display()))?;
    }
    Ok(z_pending.len())
}

pub(super) fn decrypt_password(c_encoded: &str) -> Result<String> {
    let z_blob = STANDARD
        .decode(c_encoded)
        .context("FinalShell 密码不是有效的 Base64")?;
    ensure!(
        z_blob.len() >= 16 && z_blob.len() % 8 == 0,
        "FinalShell DES 密文长度无效"
    );
    let (z_head, z_ciphertext) = z_blob.split_at(8);
    let z_key = Zeroizing::new(derive_key(z_head)?);
    let z_cipher =
        Des::new_from_slice(&z_key[..]).map_err(|_| anyhow::anyhow!("FinalShell DES 密钥无效"))?;
    let mut z_plaintext = Zeroizing::new(z_ciphertext.to_vec());
    for z_chunk in z_plaintext.chunks_exact_mut(8) {
        let mut z_block = GenericArray::clone_from_slice(z_chunk);
        z_cipher.decrypt_block(&mut z_block);
        z_chunk.copy_from_slice(&z_block);
        z_block.as_mut_slice().zeroize();
    }
    let z_padding = *z_plaintext.last().context("FinalShell 密文为空")? as usize;
    ensure!(
        (1..=8).contains(&z_padding)
            && z_plaintext[z_plaintext.len() - z_padding..]
                .iter()
                .all(|b| *b as usize == z_padding),
        "FinalShell 密码填充无效"
    );
    let z_length = z_plaintext.len() - z_padding;
    let z_result = String::from_utf8(z_plaintext[..z_length].to_vec())
        .context("FinalShell 密码不是有效 UTF-8")?;
    Ok(z_result)
}

pub(super) fn encrypt_password(c_password: &str) -> Result<String> {
    let mut z_head = [0u8; 8];
    let z_key = loop {
        rand::rngs::OsRng.fill_bytes(&mut z_head);
        match derive_key(&z_head) {
            Ok(z_key) => break Zeroizing::new(z_key),
            Err(_) => continue,
        }
    };
    let z_cipher =
        Des::new_from_slice(&z_key[..]).map_err(|_| anyhow::anyhow!("FinalShell DES 密钥无效"))?;
    let mut z_plaintext = Zeroizing::new(c_password.as_bytes().to_vec());
    let z_padding = 8 - (z_plaintext.len() % 8);
    z_plaintext.extend(std::iter::repeat(z_padding as u8).take(z_padding));
    for z_chunk in z_plaintext.chunks_exact_mut(8) {
        let mut z_block = GenericArray::clone_from_slice(z_chunk);
        z_cipher.encrypt_block(&mut z_block);
        z_chunk.copy_from_slice(&z_block);
        z_block.as_mut_slice().zeroize();
    }
    let mut z_blob = Zeroizing::new(z_head.to_vec());
    z_blob.extend_from_slice(&z_plaintext);
    z_head.zeroize();
    Ok(STANDARD.encode(&z_blob[..]))
}

// 按 MeatShell FinalShell 适配实现独立复现 Java Random 派生, 算法来源 MIT OR Apache-2.0:
// https://github.com/yituorou/meatshell/blob/main/src/config/impls/finalshell.rs
fn derive_key(c_head: &[u8]) -> Result<[u8; 8]> {
    ensure!(c_head.len() == 8, "FinalShell 密码头长度无效");
    let z_divisor = JavaRandom::new(signed_byte(c_head[5])).next_int(127);
    ensure!(z_divisor != 0, "FinalShell 密码头产生无效的 DES 密钥除数");
    let z_seed = 3_680_984_568_597_093_857_i64 / z_divisor as i64;
    let mut z_random = JavaRandom::new(z_seed);
    for _ in 0..signed_byte(c_head[0]).max(0) {
        z_random.next_long();
    }
    let mut z_secondary = JavaRandom::new(z_random.next_long());
    let z_values = [
        signed_byte(c_head[4]),
        z_secondary.next_long(),
        signed_byte(c_head[7]),
        signed_byte(c_head[3]),
        z_secondary.next_long(),
        signed_byte(c_head[1]),
        z_random.next_long(),
        signed_byte(c_head[2]),
    ];
    let mut z_material = Zeroizing::new(Vec::with_capacity(64));
    for z_value in z_values {
        z_material.extend_from_slice(&z_value.to_be_bytes());
    }
    let mut z_digest = md5::compute(&z_material[..]).0;
    let mut z_key = [0u8; 8];
    z_key.copy_from_slice(&z_digest[..8]);
    z_digest.zeroize();
    Ok(z_key)
}

struct JavaRandom {
    z_seed: u64,
}

impl JavaRandom {
    const MULTIPLIER: u64 = 0x5DEECE66D;
    const ADDEND: u64 = 0xB;
    const MASK: u64 = (1 << 48) - 1;

    fn new(c_seed: i64) -> Self {
        Self {
            z_seed: ((c_seed as u64) ^ Self::MULTIPLIER) & Self::MASK,
        }
    }
    fn next(&mut self, c_bits: u32) -> u32 {
        self.z_seed = self
            .z_seed
            .wrapping_mul(Self::MULTIPLIER)
            .wrapping_add(Self::ADDEND)
            & Self::MASK;
        (self.z_seed >> (48 - c_bits)) as u32
    }
    fn next_int(&mut self, c_bound: u32) -> u32 {
        if c_bound.is_power_of_two() {
            return ((c_bound as u64 * self.next(31) as u64) >> 31) as u32;
        }
        loop {
            let z_bits = self.next(31);
            let z_value = z_bits % c_bound;
            if (z_bits as i32)
                .wrapping_sub(z_value as i32)
                .wrapping_add((c_bound - 1) as i32)
                >= 0
            {
                return z_value;
            }
        }
    }
    fn next_long(&mut self) -> i64 {
        ((self.next(32) as i32 as i64) << 32).wrapping_add(self.next(32) as i32 as i64)
    }
}

fn signed_byte(c_byte: u8) -> i64 {
    (c_byte as i8) as i64
}

fn write_atomic(c_path: &Path, c_bytes: &[u8]) -> Result<()> {
    if let Ok(z_meta) = fs::symlink_metadata(c_path) {
        ensure!(
            z_meta.file_type().is_file(),
            "FinalShell 目标必须是普通文件"
        );
    }
    let z_temp: PathBuf = c_path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
    let mut z_options = fs::OpenOptions::new();
    z_options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        z_options.mode(0o600);
    }
    let z_result = (|| -> Result<()> {
        let mut z_file = z_options
            .open(&z_temp)
            .context("创建 FinalShell 临时文件失败")?;
        z_file
            .write_all(c_bytes)
            .context("写入 FinalShell 临时文件失败")?;
        z_file.sync_all().context("同步 FinalShell 临时文件失败")?;
        drop(z_file);
        fs::rename(&z_temp, c_path).context("替换 FinalShell 文件失败")
    })();
    if z_result.is_err() {
        let _ = fs::remove_file(&z_temp);
    }
    z_result
}

fn write_new(c_path: &Path, c_bytes: &[u8]) -> Result<()> {
    let mut z_options = fs::OpenOptions::new();
    z_options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        z_options.mode(0o600);
    }
    let mut z_file = z_options
        .open(c_path)
        .context("创建 FinalShell 文件失败, 不覆盖已有文件")?;
    let z_result = z_file
        .write_all(c_bytes)
        .and_then(|_| z_file.sync_all())
        .context("写入 FinalShell 文件失败");
    drop(z_file);
    if z_result.is_err() {
        let _ = fs::remove_file(c_path);
    }
    z_result
}

fn read_file(c_path: &Path) -> Result<Vec<u8>> {
    let z_meta = fs::symlink_metadata(c_path).context("读取 FinalShell 文件信息失败")?;
    ensure!(
        z_meta.file_type().is_file(),
        "FinalShell 路径必须是普通文件"
    );
    ensure!(
        z_meta.len() <= MAX_FILE_SIZE,
        "FinalShell 文件超过 1 MiB 限制"
    );
    let z_file = fs::File::open(c_path).context("打开 FinalShell 文件失败")?;
    let mut z_bytes = Vec::new();
    z_file
        .take(MAX_FILE_SIZE + 1)
        .read_to_end(&mut z_bytes)
        .context("读取 FinalShell 文件失败")?;
    ensure!(
        z_bytes.len() as u64 <= MAX_FILE_SIZE,
        "FinalShell 文件超过 1 MiB 限制"
    );
    Ok(z_bytes)
}
