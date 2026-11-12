use anyhow::{Context, Result, ensure};
use slint::fontique_011::fontique;
use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};

// 可一键下载的开源等宽字体 (均为 SIL OFL 或 MIT 许可): 名称、固定版本的发布包、包内常规与粗体文件名.
pub(crate) const DOWNLOADS: [(&str, &str, [&str; 2]); 3] = [
    (
        "JetBrains Mono",
        "https://github.com/JetBrains/JetBrainsMono/releases/download/v2.304/JetBrainsMono-2.304.zip",
        ["JetBrainsMono-Regular.ttf", "JetBrainsMono-Bold.ttf"],
    ),
    (
        "Fira Code",
        "https://github.com/tonsky/FiraCode/releases/download/6.2/Fira_Code_v6.2.zip",
        ["FiraCode-Regular.ttf", "FiraCode-Bold.ttf"],
    ),
    (
        "Hack",
        "https://github.com/source-foundry/Hack/releases/download/v3.003/Hack-v3.003-ttf.zip",
        ["Hack-Regular.ttf", "Hack-Bold.ttf"],
    ),
];

#[cfg(target_os = "macos")]
const SYSTEM: [&str; 6] = ["Menlo", "Monaco", "SF Mono", "Courier New", "Andale Mono", "PT Mono"];
#[cfg(not(target_os = "macos"))]
const SYSTEM: [&str; 5] = ["Consolas", "Cascadia Mono", "Cascadia Code", "Courier New", "Lucida Console"];

pub(crate) fn directory(c_data_dir: &Path) -> PathBuf {
    c_data_dir.join("fonts")
}

// 启动时注册已下载的字体; 只在本程序内生效, 不写入系统字体目录.
pub(crate) fn register_downloaded(c_dir: &Path) {
    let Ok(z_entries) = std::fs::read_dir(c_dir) else {
        return;
    };
    let mut z_collection = slint::fontique_011::shared_collection();
    for z_path in z_entries.flatten().map(|c| c.path()) {
        if z_path.extension().is_some_and(|c| c.eq_ignore_ascii_case("ttf")) {
            if let Ok(z_bytes) = std::fs::read(&z_path) {
                z_collection.register_fonts(fontique::Blob::new(Arc::new(z_bytes)), None);
            }
        }
    }
}

// 当前可用的候选字体 (系统已安装或已下载), 未安装的不出现在列表里.
pub(crate) fn installed() -> Vec<String> {
    let mut z_collection = slint::fontique_011::shared_collection();
    SYSTEM
        .iter()
        .chain(DOWNLOADS.iter().map(|c| &c.0))
        .filter(|c| z_collection.family_by_name(c).is_some())
        .map(|c| c.to_string())
        .collect()
}

// 下载发布包并取出常规与粗体两个文件. 使用系统自带的 curl 与 tar (macOS 与 Windows 10 及以上均内置),
// 不引入额外依赖; 只接受固定地址, 文件经 TrueType 头校验后才放入字体目录.
pub(crate) fn download(c_name: &str, c_dir: &Path) -> Result<Vec<PathBuf>> {
    let (_, z_url, z_files) = DOWNLOADS
        .iter()
        .find(|c| c.0 == c_name)
        .context("不支持下载该字体")?;
    // 临时解压目录放在程序自己的字体目录下 (用完删除), 免安装版不在系统临时目录留下任何文件.
    let z_work = c_dir.join(format!(".download-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&z_work)?;
    let z_result = (|| {
        let z_zip = z_work.join("font.zip");
        run(Command::new("curl").args(["-fsSL", "--max-time", "180", "-o"]).arg(&z_zip).arg(z_url))
            .context("下载失败, 请检查网络或代理")?;
        ensure!(std::fs::metadata(&z_zip)?.len() <= 64 * 1024 * 1024, "下载文件过大");
        run(Command::new("tar").arg("-xf").arg(&z_zip).arg("-C").arg(&z_work)).context("解压失败")?;
        std::fs::create_dir_all(c_dir)?;
        let mut z_installed = Vec::new();
        for z_file in z_files {
            let z_source = find(&z_work, z_file, 6).with_context(|| format!("发布包中缺少 {z_file}"))?;
            let z_bytes = std::fs::read(&z_source)?;
            ensure!(
                z_bytes.len() <= 8 * 1024 * 1024
                    && (z_bytes.starts_with(&[0, 1, 0, 0]) || z_bytes.starts_with(b"true")),
                "{z_file} 不是有效的 TrueType 字体"
            );
            let z_target = c_dir.join(z_file);
            std::fs::write(&z_target, z_bytes)?;
            z_installed.push(z_target);
        }
        Ok(z_installed)
    })();
    let _ = std::fs::remove_dir_all(&z_work);
    z_result
}

pub(crate) fn register_files(c_files: &[PathBuf]) {
    let mut z_collection = slint::fontique_011::shared_collection();
    for z_path in c_files {
        if let Ok(z_bytes) = std::fs::read(z_path) {
            z_collection.register_fonts(fontique::Blob::new(Arc::new(z_bytes)), None);
        }
    }
}

fn run(c_command: &mut Command) -> Result<()> {
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        // 不弹出命令行窗口.
        c_command.creation_flags(0x0800_0000);
    }
    let z_status = c_command.status()?;
    ensure!(z_status.success(), "命令退出码 {z_status}");
    Ok(())
}

fn find(c_dir: &Path, c_name: &str, c_depth: usize) -> Option<PathBuf> {
    for z_entry in std::fs::read_dir(c_dir).ok()?.flatten() {
        let z_path = z_entry.path();
        if z_path.is_dir() && c_depth > 0 {
            if let Some(z_found) = find(&z_path, c_name, c_depth - 1) {
                return Some(z_found);
            }
        } else if z_path.file_name().is_some_and(|c| c == c_name) {
            return Some(z_path);
        }
    }
    None
}
