# HiPShell Windows 编译说明

本目录是 HiPShell 的完整源码副本 (Rust + Slint 的 SSH 客户端), 用于在 Windows 上编译出免安装版 `hipssh.exe`. 请按下列步骤指导用户操作; 只需编译, 不要修改源码.

## 结果

- 编译产物: `target\release\hipssh.exe` (单文件, 已嵌入图标、版本信息与清单).
- `build.bat` 会再把它放进 `dist\HiPShell-Ver.x.y-windows-x64\`, 并压缩为同名 `.zip`.
- 免安装: exe 放在任意可写文件夹即可运行, 首次运行在 exe 旁创建 `config\` 保存全部数据, 不写注册表或系统目录. 不要放在 `C:\Program Files` 等普通用户无写权限的位置.

## 一次性准备

1. **Visual Studio 生成工具** (Build Tools for Visual Studio 2022 或更新):
   安装时勾选工作负载 **"使用 C++ 的桌面开发"**, 保持默认组件 (含 MSVC 编译器与 Windows 10/11 SDK).
   SDK 中的 `rc.exe` 用于嵌入图标, 缺少会导致编译失败.
2. **Rust**: 从 https://rustup.rs 下载 `rustup-init.exe`, 运行并选默认安装 (工具链 `stable-x86_64-pc-windows-msvc`).
   安装后新开终端, 确认 `cargo --version` 与 `rustc --version` 可用.

不需要 CMake、NASM、LLVM 或 Perl (Skia 使用预编译库, 见下文).

## 编译

在本目录 (含 `Cargo.toml` 的目录) 打开 PowerShell 或命令提示符:

```bat
build.bat
```

或手动执行:

```bat
cargo build --release --locked
```

- 首次编译会从 crates.io 下载依赖, 耗时较长 (约 5-15 分钟); 之后增量编译很快.
- 界面使用 Skia 渲染 (文字更清晰), 首次编译还会从 GitHub 下载 Skia 预编译库 (几十 MB), 访问 GitHub 慢时先设置代理.
- `--locked` 表示严格使用 `Cargo.lock` 中的依赖版本, 不要去掉, 也不要运行 `cargo update`.
- `.cargo\config.toml` 将并行编译任务限制为 2 个, 内存充足时可保持不变.

## 常见问题

| 现象 | 处理 |
|---|---|
| 下载依赖超时或失败 | 网络受限时先设置代理再编译: PowerShell 中 `$env:HTTPS_PROXY="http://127.0.0.1:端口"`, cmd 中 `set HTTPS_PROXY=http://127.0.0.1:端口` |
| skia-bindings 编译失败, 提示下载失败或找不到 clang / python | Skia 预编译库没下载下来, 转而尝试从源码编译. 设置代理 (同上) 后删除 `target` 文件夹重新编译, 不要去装 LLVM |
| `link.exe not found` 或找不到 MSVC | 未安装 "使用 C++ 的桌面开发" 工作负载; 安装后重新打开终端 |
| `rc.exe` 找不到 / 嵌入 Windows 图标与版本信息失败 | 缺少 Windows SDK: 在 Visual Studio Installer 中为生成工具补装 Windows 10/11 SDK; 或在 "Developer PowerShell for VS" 中编译 |
| 编译时间很长或内存不足 | 正常现象; 关闭其他大型程序, 不要同时运行多个编译 |
| 运行时提示无法创建配置目录 | exe 所在文件夹不可写, 换到用户自己的目录 (如 `D:\HiPShell\`) |
| 杀毒软件拦截新编译的 exe | 未签名程序的常见情况, 将该文件夹加入信任 |

## 编译后检查

1. 资源管理器中 `hipssh.exe` 显示 HiPShell 图标; 右键 → 属性 → 详细信息, 产品名 HiPShell, 版本 1.3.0, 版权 HiPsHoW.
2. 双击运行, 窗口标题为 `HiPShell Ver.1.3`, exe 旁出现 `config\` 文件夹.
3. 连接服务器后上传一个远端已存在的同名文件, 弹出的确认框按钮应为 "覆盖 / 保留两者 / 取消".
4. 在终端中粘贴多行文本, 每行只执行一次, 不出现多余的空回车.
5. 标签、状态栏、按钮文字清晰; 最大化后最小化再恢复, 界面完整显示.

如有编译报错, 请把完整错误输出反馈给开发者, 不要自行修改源码或依赖版本.
