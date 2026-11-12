#!/bin/sh
# 用法:
#   sh run-macos.sh            编译并打包: 生成 dist/HiPShell-Ver.x.y-macos-<架构>.dmg (拖拽安装包) 与 windows-build/ (Windows 编译源码)
#   sh run-macos.sh install    开发调试: 编译后直接更新 /Applications/HiPShell.app 并启动 (需先关闭窗口)
# 数据固定在 ~/Library/Application Support/HiPShell, 与程序位置无关.
set -eu

if [ "$(uname -s)" != Darwin ]; then
    printf '%s\n' '这个入口用于 macOS.' >&2
    exit 1
fi

task_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$task_dir"

# 组装应用包: 包内只有程序与图标, 运行数据不写入包内 (修改包内文件会破坏签名).
assemble_bundle() {
    task_bundle=$1
    mkdir -p "$task_bundle/Contents/MacOS" "$task_bundle/Contents/Resources/zh-Hans.lproj"
    cp packaging/macos/Info.plist "$task_bundle/Contents/Info.plist"
    cp assets/HiPShell.icns "$task_bundle/Contents/Resources/HiPShell.icns"
    # 原子替换可执行文件, 独立副本避免应用包签名改写 Cargo 产物.
    task_executable=$(mktemp "$task_bundle/Contents/MacOS/.hipssh.XXXXXX")
    trap 'rm -f "$task_executable"' 0
    cp -p target/release/hipssh "$task_executable"
    mv -f "$task_executable" "$task_bundle/Contents/MacOS/hipssh"
    codesign --force --sign - --timestamp=none "$task_bundle"
    codesign --verify --strict "$task_bundle"
}

if [ "${1:-}" != install ]; then
    cargo build --release --locked --offline --jobs 2
    # 对外版本取 Cargo.toml 的主次版本, 如 Ver.1.0.
    task_version=$(sed -n 's/^version = "\([0-9]*\)\.\([0-9]*\)\..*"/\1.\2/p' Cargo.toml | head -n 1)
    task_arch=$(uname -m)
    task_name="HiPShell-Ver.$task_version-macos-$task_arch"
    task_stage="dist/$task_name"
    rm -rf "$task_stage" "dist/$task_name.dmg"
    mkdir -p "$task_stage"
    assemble_bundle "$task_stage/HiPShell.app"
    # 拖拽安装: 卷内放应用包和指向 /Applications 的"应用程序", 打开后把图标拖过去即可.
    mkdir "$task_stage/.background"
    cp packaging/macos/dmg-background.tiff "$task_stage/.background/background.tiff"
    # 先做可写镜像, 挂载后放入窗口布局 (背景、图标位置), 再压缩为只读 dmg.
    # 卷名固定为 HiPShell (不带版本): 布局里的背景图按卷名和路径引用, 换版本也能沿用同一份布局模板.
    task_volume="HiPShell"
    task_rw="dist/$task_name-rw.dmg"
    task_layout=packaging/macos/dmg-layout.dsstore
    hdiutil create -quiet -volname "$task_volume" -srcfolder "$task_stage" -ov -format UDRW "$task_rw"
    rm -rf "$task_stage"
    # 同名卷已挂载时会挂到别的路径, 先卸载.
    [ -d "/Volumes/$task_volume" ] && hdiutil detach -quiet "/Volumes/$task_volume"
    hdiutil attach -quiet -readwrite -noverify -noautoopen "$task_rw"
    # 新版 macOS 的 Finder 对指向 /Applications 的符号链接只显示空白图标: 改用替身并贴上系统的应用程序文件夹图标.
    # 替身失败时退回符号链接, 安装功能不受影响.
    task_link="/Volumes/$task_volume/应用程序"
    osascript -e "tell application \"Finder\" to make new alias file at (POSIX file \"/Volumes/$task_volume\" as alias) to (POSIX file \"/Applications\" as alias) with properties {name:\"应用程序\"}" >/dev/null \
        && osascript -l JavaScript -e "ObjC.import('AppKit'); const w = \$.NSWorkspace.sharedWorkspace; w.setIconForFileOptions(w.iconForFile('/Applications'), '$task_link', 0)" >/dev/null \
        || { rm -f "$task_link"; ln -s /Applications "$task_link"; }
    if [ -f "$task_layout" ]; then
        # 已有布局模板: 直接放入, 不打开 Finder 窗口.
        cp "$task_layout" "/Volumes/$task_volume/.DS_Store"
    else
        # 首次打包 (或删除模板后): 由 Finder 打开窗口设置样式, 完成后保存为布局模板, 以后不再打开窗口.
        # 窗口内容区 660x400, 与背景图一致; 图标坐标为图标中心, 对齐背景上的箭头两端. 需允许终端控制 Finder.
        osascript <<EOF || printf '%s\n' '设置安装窗口样式失败 (可能未允许终端控制 Finder), 已生成无背景的 dmg.' >&2
tell application "Finder"
    tell disk "$task_volume"
        open
        set current view of container window to icon view
        set toolbar visible of container window to false
        set statusbar visible of container window to false
        set the bounds of container window to {200, 120, 860, 548}
        set z_options to the icon view options of container window
        set arrangement of z_options to not arranged
        set icon size of z_options to 112
        set text size of z_options to 13
        set background picture of z_options to file ".background:background.tiff"
        set position of item "HiPShell.app" of container window to {170, 200}
        set position of item "应用程序" of container window to {490, 200}
        update without registering applications
        close
        -- 窗口尺寸要关闭后重新打开再设置, 才会写入 .DS_Store.
        open
        delay 1
        set the bounds of container window to {200, 120, 860, 548}
        delay 2
        close
    end tell
end tell
EOF
        # 等 Finder 把视图设置写入 .DS_Store 再保存.
        sleep 2
        [ -f "/Volumes/$task_volume/.DS_Store" ] && cp "/Volumes/$task_volume/.DS_Store" "$task_layout"
    fi
    rm -rf "/Volumes/$task_volume/.fseventsd"
    sync
    hdiutil detach -quiet "/Volumes/$task_volume"
    hdiutil convert -quiet "$task_rw" -format UDZO -imagekey zlib-level=9 -ov -o "dist/$task_name.dmg"
    rm -f "$task_rw"
    # Windows 编译用的源码副本: 只放编译所需文件, 说明与编译脚本来自 packaging/windows.
    rm -rf windows-build
    mkdir -p windows-build/assets windows-build/packaging/macos windows-build/.cargo
    cp -R src ui Cargo.toml Cargo.lock build.rs windows-build/
    cp .cargo/config.toml windows-build/.cargo/
    cp assets/HiPShell.ico assets/hipshell-icon.png assets/hipshell-window-icon.png windows-build/assets/
    cp packaging/macos/Info.plist windows-build/packaging/macos/
    cp packaging/windows/README.md packaging/windows/build.bat windows-build/
    printf '%s\n' "已生成 dist/$task_name.dmg 与 windows-build/"
    exit 0
fi

# 只检查主窗口 (MCP 由主窗口进程提供, 没有其他常驻进程).
gui_running() {
    pgrep -fx '.*/HiPShell\.app/Contents/MacOS/hipssh' >/dev/null 2>&1
}
if gui_running; then
    printf '%s\n' '请先关闭 HiPShell 窗口, 再更新启动.' >&2
    exit 1
fi
cargo build --release --locked --offline --jobs 2
if gui_running; then
    printf '%s\n' '构建期间 HiPShell 已启动, 请先关闭后再更新.' >&2
    exit 1
fi
assemble_bundle /Applications/HiPShell.app
# 已确认没有运行实例, 新建进程避免向刚退出的旧实例发送重开事件.
shift
open -n /Applications/HiPShell.app --args "$@"
