# HiPShell

[中文](README.zh-CN.md) | English

**One shell, shared by you and your AI.**

HiPShell is a lightweight SSH client (Rust + Slint, macOS / Windows) with a built-in MCP server. An LLM agent (Claude Code, Codex, ...) works in the **same live terminal** you are looking at: you see every command it types, it sees what you did, and either of you can take over at any time.

> ⚠️ Most of this code was written by LLMs. It may contain bugs. Review it and use at your own risk.

## Why

Usual AI + SSH setups give the model its own hidden connection. HiPShell does the opposite:

- **Shared session** – the agent types into your terminal tab; current directory, `sudo -i`, virtualenvs, tmux all carry over.
- **You stay in control** – human input wins; the agent asks before clearing a half-typed command.
- **Passwords never reach the LLM** – `sudo`/`su` prompts are filled by HiPShell itself (`input_admin_password`).
- **Per-connection authorization** – the agent only sees servers you allow; it cannot grant itself access, trust new hosts, or read credentials.
- **Nothing to install on servers** – MCP runs locally at `127.0.0.1`, only local requests accepted.

## Features

- Password / private key (OpenSSH, PEM, PuTTY `.ppk`) / keyboard-interactive auth, SOCKS5 / HTTP proxy, legacy algorithms
- Full VT terminal: vim, tmux, mouse, Kitty keyboard protocol, OSC 52 clipboard
- SFTP browser, drag & drop upload, resumable transfers with progress
- Linux monitor: CPU, memory, processes, network
- Saved command groups, background command execution, task list
- Reads and writes **FinalShell** connection configs (import / export)
- Single portable binary, no database

## MCP

Start HiPShell, enable MCP for a connection (Edit connection), then register once in your agent:

```json
{"mcpServers":{"hipshell":{"type":"http","url":"http://127.0.0.1:7391/mcp"}}}
```

Claude Code: `claude mcp add --transport http --scope user hipshell http://127.0.0.1:7391/mcp`

| Group | Tools |
|---|---|
| Connection | `state` `connect` `reconnect` `open_terminal` `disconnect` |
| Terminal | `read_terminal` `write_terminal` `input_admin_password` |
| Command | `run_command` (separate channel, does not touch your terminal) |
| Files | `list_files` `list_local_files` `upload_file` `download_file` |
| Tasks | `get_task` `cancel_task` |

Port and on/off switch are in Settings → MCP. Headless mode: `hipssh --serve`.

## Build

Requires Rust (stable).

```sh
cargo build --release          # binary: target/release/hipssh
sh run-macos.sh                # macOS: build .dmg into dist/, and a windows-build/ source copy
```

Windows: copy `windows-build/` to a Windows machine with Visual Studio Build Tools (C++ desktop) and run `build.bat`. See [packaging/windows/README.md](packaging/windows/README.md).

## Data

- macOS: `~/Library/Application Support/HiPShell`
- Windows: `config\` next to the exe (portable)

Saved passwords use FinalShell-compatible encryption, which is **reversible**. Protect this folder.

## License

[MIT](LICENSE) – use, modify, and redistribute freely, including commercially.

Credits: icons from [Lucide](https://lucide.dev) (ISC); FinalShell password compatibility based on [MeatShell](https://github.com/yituorou/meatshell) (MIT / Apache-2.0).
