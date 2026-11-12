use std::{env, fs, path::PathBuf};

fn main() {
    println!("cargo:rerun-if-changed=ui");
    println!("cargo:rerun-if-changed=packaging/macos/Info.plist");
    // 把 Info.plist 嵌入可执行文件: 不经 .app 直接运行时, 系统文件对话框等也按中文本地化.
    if env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        let z_plist = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap()).join("packaging/macos/Info.plist");
        println!("cargo:rustc-link-arg-bins=-Wl,-sectcreate,__TEXT,__info_plist,{}", z_plist.display());
    }
    // Windows: 嵌入 exe 图标 (资源管理器、快捷方式、固定到任务栏) 与文件详情; 版本号取自 Cargo.toml.
    // 运行中的窗口图标由界面设置, 此处负责程序文件本身. 需在 Windows 上编译 (使用 SDK 的 rc.exe).
    #[cfg(windows)]
    if env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows") {
        println!("cargo:rerun-if-changed=assets/HiPShell.ico");
        let mut z_resource = winresource::WindowsResource::new();
        z_resource
            .set_icon("assets/HiPShell.ico")
            .set("ProductName", "HiPShell")
            .set("FileDescription", "HiPShell")
            .set("CompanyName", "HiPsHoW")
            .set("LegalCopyright", "© 2026 HiPsHoW")
            .set("OriginalFilename", "hipssh.exe")
            // 启用通用控件 6.0: rfd 的自定义按钮确认框依赖它, 缺少清单时程序无法启动.
            .set_manifest(
                r#"<assembly xmlns="urn:schemas-microsoft-com:asm.v1" manifestVersion="1.0">
<dependency><dependentAssembly><assemblyIdentity type="win32" name="Microsoft.Windows.Common-Controls" version="6.0.0.0" processorArchitecture="*" publicKeyToken="6595b64144ccf1df" language="*"/></dependentAssembly></dependency>
</assembly>"#,
            );
        z_resource.compile().expect("嵌入 Windows 图标与版本信息失败");
    }
    if env::var_os("CARGO_FEATURE_DESKTOP").is_none() {
        return;
    }
    let z_root = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let z_widgets = z_root
        .join("ui/widgets")
        .to_string_lossy()
        .replace('\\', "/");
    let z_ui = z_root
        .join("ui/app.slint")
        .to_string_lossy()
        .replace('\\', "/");
    // 宏只接受字面路径; 构建时生成绝对路径, 搬动项目后仍可覆盖菜单尺寸.
    let z_source = format!(
        "slint::slint! {{ #[include_path = {z_widgets:?}] #[include_path = \"builtin:/fluent\"] export {{ AppWindow }} from {z_ui:?}; }}"
    );
    let z_output = PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("app_ui.rs");
    fs::write(z_output, z_source).expect("生成界面入口失败");
}
