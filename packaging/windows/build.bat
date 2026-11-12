@echo off
rem HiPShell Windows 编译与打包: 生成 dist\HiPShell-Ver.x.y-windows-x64\hipssh.exe 及同名 zip.
chcp 65001 >nul
setlocal
cd /d "%~dp0"

where cargo >nul 2>nul
if errorlevel 1 (
    echo 未找到 cargo, 请先安装 Rust: https://rustup.rs
    exit /b 1
)

cargo build --release --locked
if errorlevel 1 (
    echo 编译失败, 请查看上方错误信息.
    exit /b 1
)

rem 对外版本取 Cargo.toml 的主次版本, 如 Ver.1.0.
for /f "usebackq delims=" %%v in (`powershell -NoProfile -Command "$m=[regex]::Match((Get-Content Cargo.toml -Raw),'(?m)^version = .(\d+)\.(\d+)'); $m.Groups[1].Value + '.' + $m.Groups[2].Value"`) do set VER=%%v
if "%VER%"=="" (
    echo 无法从 Cargo.toml 读取版本号.
    exit /b 1
)

set NAME=HiPShell-Ver.%VER%-windows-x64
if exist "dist\%NAME%" rmdir /s /q "dist\%NAME%"
if exist "dist\%NAME%.zip" del /q "dist\%NAME%.zip"
mkdir "dist\%NAME%"
copy /y "target\release\hipssh.exe" "dist\%NAME%\hipssh.exe" >nul
powershell -NoProfile -Command "Compress-Archive -Path 'dist\%NAME%' -DestinationPath 'dist\%NAME%.zip'"

echo.
echo 已生成 dist\%NAME%\hipssh.exe 与 dist\%NAME%.zip
endlocal
