# HiPShell

中文 | [English](README.md)

**你和 AI 共用同一个 Shell.**

HiPShell 是轻量 SSH 客户端 (Rust + Slint, macOS / Windows), 内置 MCP 服务. LLM (Claude Code、Codex 等) 直接在**你正在看的终端**里操作: 它输入的每条命令你都看得到, 你做过的它也看得到, 双方随时可以接手.

> ⚠️ 本项目代码主要由 LLM 编写, 可能存在问题, 请自行审查, 风险自负.

## 特色

常见的 AI + SSH 方案是让模型另开一条看不见的连接. HiPShell 反过来:

- **共用会话** – AI 在你的终端标签里输入, 沿用当前目录、`sudo -i`、虚拟环境、tmux.
- **人优先** – 人工输入优先; 发现你输入了一半的命令, AI 先询问再清空.
- **密码不经过 LLM** – `sudo`/`su` 密码由 HiPShell 代填 (`input_admin_password`).
- **按连接授权** – AI 只能看到你授权的服务器, 无权自行授权、信任新主机或读取凭据.
- **服务器零安装** – MCP 运行在本机 `127.0.0.1`, 只接受本机请求.

## 功能

- 密码 / 私钥 (OpenSSH、PEM、PuTTY `.ppk`) / keyboard-interactive 认证, SOCKS5 / HTTP 代理, 兼容老服务器算法
- 完整 VT 终端: vim、tmux、鼠标、Kitty 键盘协议、OSC 52 剪贴板
- SFTP 文件管理, 拖入上传, 断点续传与进度
- Linux 监控: CPU、内存、进程、网速
- 常用命令分组, 独立后台执行, 任务列表
- 读写 **FinalShell** 连接配置 (导入 / 导出)
- 单文件免安装, 无数据库

## MCP

启动 HiPShell, 在连接编辑中授权 MCP, 然后在 AI 客户端登记一次:

```json
{"mcpServers":{"hipshell":{"type":"http","url":"http://127.0.0.1:7391/mcp"}}}
```

Claude Code: `claude mcp add --transport http --scope user hipshell http://127.0.0.1:7391/mcp`

| 类别 | 工具 |
|---|---|
| 连接 | `state` `connect` `reconnect` `open_terminal` `disconnect` |
| 终端 | `read_terminal` `write_terminal` `input_admin_password` |
| 执行 | `run_command` (独立通道, 不影响终端) |
| 文件 | `list_files` `list_local_files` `upload_file` `download_file` |
| 任务 | `get_task` `cancel_task` |

端口与开关在 设置 → MCP. 无界面运行: `hipssh --serve`.

## 编译

需要 Rust (stable).

```sh
cargo build --release          # 产物: target/release/hipssh
sh run-macos.sh                # macOS: 在 dist/ 生成 .dmg, 并生成 windows-build/ 源码副本
```

Windows: 把 `windows-build/` 拷到装有 Visual Studio 生成工具 (使用 C++ 的桌面开发) 的机器, 运行 `build.bat`. 详见 [packaging/windows/README.md](packaging/windows/README.md).

版本号: 修改 `Cargo.toml` 与 `packaging/macos/Info.plist`, 再执行 `cargo update -p hipssh --offline` 同步 `Cargo.lock`.

## 数据位置

- macOS: `~/Library/Application Support/HiPShell`
- Windows: exe 旁的 `config\` (免安装)

保存的密码使用 FinalShell 兼容加密, **可以还原**, 请妥善保管该目录.

## 许可

[MIT](LICENSE) – 可自由使用、修改、再发布, 包括商用.

致谢: 图标来自 [Lucide](https://lucide.dev) (ISC); FinalShell 密码兼容参考 [MeatShell](https://github.com/yituorou/meatshell) (MIT / Apache-2.0).
