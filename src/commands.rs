use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};

const MAX_COMMANDS: usize = 256;
const MAX_GROUPS: usize = 64;
const MAX_FILE_BYTES: usize = 1024 * 1024;
const DEFAULT_GROUP: &str = "默认";

#[derive(Clone, Serialize, Deserialize)]
pub struct SavedCommand {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub group: String,
    pub command: String,
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct CommandData {
    #[serde(default)]
    groups: Vec<String>,
    commands: Vec<SavedCommand>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum StoredCommands {
    Current(CommandData),
    Legacy(Vec<SavedCommand>),
}

pub struct CommandStore {
    path: PathBuf,
    cache: Mutex<Option<CommandData>>,
}

impl CommandStore {
    pub fn new(c_path: PathBuf) -> Self {
        Self {
            path: c_path,
            cache: Mutex::new(None),
        }
    }

    pub fn list(&self) -> Result<Vec<SavedCommand>> {
        let mut z_cache = self
            .cache
            .lock()
            .map_err(|_| anyhow::anyhow!("命令配置锁异常"))?;
        self.load(&mut z_cache)?;
        Ok(z_cache.as_ref().unwrap().commands.clone())
    }

    pub fn groups(&self) -> Result<Vec<String>> {
        let mut z_cache = self
            .cache
            .lock()
            .map_err(|_| anyhow::anyhow!("命令配置锁异常"))?;
        self.load(&mut z_cache)?;
        Ok(z_cache.as_ref().unwrap().groups.clone())
    }

    pub fn save(&self, mut c_command: SavedCommand) -> Result<SavedCommand> {
        if c_command.id.is_empty() {
            c_command.id = uuid::Uuid::new_v4().to_string();
        }
        validate(&c_command)?;
        c_command.group = normalize_group(&c_command.group).to_owned();
        let mut z_cache = self
            .cache
            .lock()
            .map_err(|_| anyhow::anyhow!("命令配置锁异常"))?;
        self.load(&mut z_cache)?;
        let mut z_next = z_cache.as_ref().unwrap().clone();
        ensure!(
            z_next.groups.contains(&c_command.group),
            "常用命令分组不存在, 请先新增分组"
        );
        if let Some(z_existing) = z_next.commands.iter_mut().find(|c| c.id == c_command.id) {
            *z_existing = c_command.clone();
        } else {
            ensure!(
                z_next.commands.len() < MAX_COMMANDS,
                "常用命令最多保存 256 条"
            );
            z_next.commands.push(c_command.clone());
        }
        self.write(&z_next)?;
        *z_cache = Some(z_next);
        Ok(c_command)
    }

    pub fn remove(&self, c_id: &str) -> Result<()> {
        let mut z_cache = self
            .cache
            .lock()
            .map_err(|_| anyhow::anyhow!("命令配置锁异常"))?;
        self.load(&mut z_cache)?;
        let mut z_next = z_cache.as_ref().unwrap().clone();
        ensure!(
            z_next.commands.iter().any(|c| c.id == c_id),
            "常用命令不存在"
        );
        z_next.commands.retain(|c| c.id != c_id);
        self.write(&z_next)?;
        *z_cache = Some(z_next);
        Ok(())
    }

    pub fn save_group(&self, c_old_name: &str, c_name: &str) -> Result<()> {
        validate_group(c_old_name)?;
        validate_group(c_name)?;
        ensure!(!c_name.trim().is_empty(), "分组名称不能为空");
        let mut z_cache = self
            .cache
            .lock()
            .map_err(|_| anyhow::anyhow!("命令配置锁异常"))?;
        self.load(&mut z_cache)?;
        let mut z_next = z_cache.as_ref().unwrap().clone();
        if c_old_name.trim().is_empty() {
            ensure!(
                !z_next.groups.iter().any(|c| c == c_name),
                "常用命令分组名称重复"
            );
            ensure!(z_next.groups.len() < MAX_GROUPS, "常用命令最多保存 64 个分组");
            z_next.groups.push(c_name.to_owned());
        } else {
            let z_index = z_next
                .groups
                .iter()
                .position(|c| c == c_old_name)
                .context("常用命令分组不存在")?;
            if c_old_name == c_name {
                return Ok(());
            }
            ensure!(
                !z_next.groups.iter().any(|c| c == c_name),
                "常用命令分组名称重复"
            );
            z_next.groups[z_index] = c_name.to_owned();
            for z_command in &mut z_next.commands {
                if z_command.group == c_old_name {
                    z_command.group = c_name.to_owned();
                }
            }
        }
        self.write(&z_next)?;
        *z_cache = Some(z_next);
        Ok(())
    }

    pub fn remove_group(&self, c_name: &str) -> Result<()> {
        validate_group(c_name)?;
        let z_name = normalize_group(c_name);
        let mut z_cache = self
            .cache
            .lock()
            .map_err(|_| anyhow::anyhow!("命令配置锁异常"))?;
        self.load(&mut z_cache)?;
        let mut z_next = z_cache.as_ref().unwrap().clone();
        ensure!(
            z_next.groups.iter().any(|c| c == z_name),
            "常用命令分组不存在"
        );
        ensure!(z_next.groups.len() > 1, "至少保留一个命令分组");
        z_next.groups.retain(|c| c != z_name);
        for z_command in &mut z_next.commands {
            if z_command.group == z_name {
                z_command.group = z_next.groups[0].clone();
            }
        }
        self.write(&z_next)?;
        *z_cache = Some(z_next);
        Ok(())
    }

    // 拖动排序: 传入完整的分组顺序或命令 ID 顺序, 必须与现有内容一一对应.
    pub fn reorder(&self, c_groups: Option<Vec<String>>, c_order: Option<Vec<String>>) -> Result<()> {
        let mut z_cache = self
            .cache
            .lock()
            .map_err(|_| anyhow::anyhow!("命令配置锁异常"))?;
        self.load(&mut z_cache)?;
        let mut z_next = z_cache.as_ref().unwrap().clone();
        if let Some(z_groups) = c_groups {
            let mut l_sorted = z_groups.clone();
            l_sorted.sort();
            let mut l_current = z_next.groups.clone();
            l_current.sort();
            ensure!(l_sorted == l_current, "命令分组顺序与现有分组不一致");
            z_next.groups = z_groups;
        }
        if let Some(z_order) = c_order {
            ensure!(z_order.len() == z_next.commands.len(), "命令顺序与现有命令不一致");
            let mut l_commands = Vec::with_capacity(z_order.len());
            for z_id in &z_order {
                let l_index = z_next.commands.iter().position(|c| &c.id == z_id).context("命令顺序与现有命令不一致")?;
                l_commands.push(z_next.commands.swap_remove(l_index));
            }
            z_next.commands = l_commands;
        }
        self.write(&z_next)?;
        *z_cache = Some(z_next);
        Ok(())
    }

    // 导出为单个文件, 格式与命令配置相同, 换电脑后在新机器导入即可.
    pub fn export(&self, c_path: &Path) -> Result<usize> {
        let mut z_cache = self
            .cache
            .lock()
            .map_err(|_| anyhow::anyhow!("命令配置锁异常"))?;
        self.load(&mut z_cache)?;
        let z_data = z_cache.as_ref().unwrap();
        // 与内部保存同为紧凑格式, 大小规则一致, 能保存的配置导出后一定能原样导入.
        fs::write(c_path, serde_json::to_vec(z_data)?).context("写入导出文件失败")?;
        Ok(z_data.commands.len())
    }

    // 导入并合并: 补齐缺少的分组; 同分组下名称和内容都相同的命令视为已有, 跳过; 无效或超出上限的也跳过.
    // 导入的命令换用新 ID, 不会与现有命令冲突或覆盖现有命令. 返回 (导入数, 跳过数).
    pub fn import(&self, c_path: &Path) -> Result<(usize, usize)> {
        let z_import = read_commands(c_path)?;
        let mut z_cache = self
            .cache
            .lock()
            .map_err(|_| anyhow::anyhow!("命令配置锁异常"))?;
        self.load(&mut z_cache)?;
        let mut z_next = z_cache.as_ref().unwrap().clone();
        for z_group in &z_import.groups {
            let l_group = normalize_group(z_group);
            if validate_group(l_group).is_ok() && !z_next.groups.iter().any(|c| c == l_group) && z_next.groups.len() < MAX_GROUPS {
                z_next.groups.push(l_group.to_owned());
            }
        }
        let (mut z_added, mut z_skipped) = (0, 0);
        for mut z_command in z_import.commands {
            z_command.id = uuid::Uuid::new_v4().to_string();
            z_command.group = normalize_group(&z_command.group).to_owned();
            // 先校验再改动分组表: 跳过的命令不能留下非法或多余的分组, 否则整份配置下次加载会校验失败.
            let l_duplicate = z_next.commands.iter().any(|c| c.group == z_command.group && c.name == z_command.name && c.command == z_command.command);
            let l_group_ready = z_next.groups.contains(&z_command.group) || z_next.groups.len() < MAX_GROUPS;
            if l_duplicate || validate(&z_command).is_err() || !l_group_ready || z_next.commands.len() >= MAX_COMMANDS {
                z_skipped += 1;
                continue;
            }
            if !z_next.groups.contains(&z_command.group) {
                z_next.groups.push(z_command.group.clone());
            }
            z_next.commands.push(z_command);
            z_added += 1;
        }
        self.write(&z_next)?;
        *z_cache = Some(z_next);
        Ok((z_added, z_skipped))
    }

    fn load(&self, c_cache: &mut Option<CommandData>) -> Result<()> {
        if c_cache.is_some() {
            return Ok(());
        }
        let mut z_data = if self.path.exists() {
            read_commands(&self.path)?
        } else {
            CommandData::default()
        };
        ensure!(z_data.commands.len() <= MAX_COMMANDS, "常用命令超过 256 条");
        ensure!(z_data.groups.len() <= MAX_GROUPS, "常用命令分组超过 64 个");
        let mut z_groups = Vec::new();
        for z_group in z_data.groups {
            validate_group(&z_group)?;
            let l_group = normalize_group(&z_group).to_owned();
            ensure!(!z_groups.contains(&l_group), "常用命令分组名称重复");
            z_groups.push(l_group);
        }
        if z_groups.is_empty() {
            z_groups.push(DEFAULT_GROUP.to_owned());
        }
        let mut z_ids = std::collections::HashSet::new();
        for z_command in &mut z_data.commands {
            validate(z_command)?;
            ensure!(z_ids.insert(z_command.id.clone()), "常用命令 ID 重复");
            z_command.group = normalize_group(&z_command.group).to_owned();
            // 旧数组没有独立的分组表, 从命令补齐分组以保留已有分类.
            if !z_groups.contains(&z_command.group) {
                z_groups.push(z_command.group.clone());
            }
        }
        ensure!(z_groups.len() <= MAX_GROUPS, "常用命令分组超过 64 个");
        z_data.groups = z_groups;
        *c_cache = Some(z_data);
        Ok(())
    }

    fn write(&self, c_data: &CommandData) -> Result<()> {
        let z_bytes = serde_json::to_vec(c_data)?;
        ensure!(z_bytes.len() <= MAX_FILE_BYTES, "命令配置超过 1 MiB");
        let z_temp = self
            .path
            .with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
        let z_result = (|| -> Result<()> {
            let mut z_options = fs::OpenOptions::new();
            z_options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                z_options.mode(0o600);
            }
            let mut z_file = z_options.open(&z_temp)?;
            z_file.write_all(&z_bytes)?;
            z_file.sync_all()?;
            drop(z_file);
            fs::rename(&z_temp, &self.path).context("保存常用命令失败")
        })();
        if z_result.is_err() {
            let _ = fs::remove_file(&z_temp);
        }
        z_result
    }
}

// 读取命令文件 (配置文件或导出文件), 兼容早期只有命令数组的格式; 内容校验由调用方处理.
fn read_commands(c_path: &Path) -> Result<CommandData> {
    let mut z_bytes = Vec::new();
    fs::File::open(c_path)?
        .take((MAX_FILE_BYTES + 1) as u64)
        .read_to_end(&mut z_bytes)?;
    ensure!(z_bytes.len() <= MAX_FILE_BYTES, "命令文件超过 1 MiB");
    Ok(match serde_json::from_slice(&z_bytes).context("读取常用命令失败, 文件格式不正确")? {
        StoredCommands::Current(l_data) => l_data,
        StoredCommands::Legacy(l_commands) => CommandData {
            groups: Vec::new(),
            commands: l_commands,
        },
    })
}

fn validate(c_command: &SavedCommand) -> Result<()> {
    uuid::Uuid::parse_str(&c_command.id).context("常用命令 ID 无效")?;
    ensure!(
        !c_command.name.trim().is_empty() && c_command.name.len() <= 128,
        "命令名称为空或过长"
    );
    validate_group(&c_command.group)?;
    ensure!(
        !c_command.command.trim().is_empty()
            && c_command.command.len() <= 8192
            && !c_command.command.contains('\0'),
        "命令为空、含 NUL 或超过 8 KiB"
    );
    Ok(())
}

fn validate_group(c_name: &str) -> Result<()> {
    ensure!(c_name.len() <= 128, "命令分组名称超过 128 字节");
    ensure!(
        !c_name.chars().any(char::is_control) && !c_name.contains(['/', '\\']),
        "命令分组名称不能含控制字符或路径分隔符"
    );
    Ok(())
}

fn normalize_group(c_name: &str) -> &str {
    if c_name.trim().is_empty() {
        DEFAULT_GROUP
    } else {
        c_name
    }
}
