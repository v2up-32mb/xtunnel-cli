@echo off
REM x-tunnel 跨平台编译脚本 (Windows CMD)
REM 支持交叉编译多个平台

setlocal enabledelayedexpansion

REM 默认参数
set VERBOSE=false
set CLEAN=false

REM 解析命令行参数
:parse_args
if "%~1"=="" goto end_parse
if /i "%~1"=="-h" goto show_help
if /i "%~1"=="--help" goto show_help
if /i "%~1"=="-v" (
    set VERBOSE=true
    shift
    goto parse_args
)
if /i "%~1"=="--verbose" (
    set VERBOSE=true
    shift
    goto parse_args
)
if /i "%~1"=="-c" (
    set CLEAN=true
    shift
    goto parse_args
)
if /i "%~1"=="--clean" (
    set CLEAN=true
    shift
    goto parse_args
)
echo 错误: 未知参数 '%~1'
goto show_help

:show_help
echo 用法: %~nx0 [选项]
echo.
echo 选项:
echo   -h, --help     显示此帮助信息
echo   -v, --verbose  显示详细的编译命令
echo   -c, --clean    清理 bin 目录后重新编译
echo.
echo 此脚本将编译以下平台的 client:
echo   - linux/amd64
echo   - linux/arm64
echo   - linux/armv7
echo   - windows/amd64
echo   - windows/386
echo.
echo 所有二进制文件将输出到 bin/ 目录
exit /b 0

:end_parse

REM 检查 Go 是否安装
where go >nul 2>&1
if errorlevel 1 (
    echo 错误: 未找到 Go 工具链
    echo 请先安装 Go: https://golang.org/dl/
    exit /b 1
)

REM 显示 Go 版本
for /f "tokens=3" %%i in ('go version') do set GO_VERSION=%%i
echo Go 版本: %GO_VERSION%
echo.

REM 定义平台列表
set PLATFORMS=linux/amd64 linux/arm64 linux/armv7 windows/amd64 windows/386

REM 创建输出目录
if not exist bin mkdir bin

REM 如果需要清理,删除旧的二进制文件
if "%CLEAN%"=="true" (
    echo 清理 bin 目录...
    del /q bin\xtunnel-*.* 2>nul
)

REM 统计变量
set /a TOTAL_BUILDS=5
set /a SUCCESS_COUNT=0
set /a FAILED_COUNT=0

REM 记录开始时间
set START_TIME=%time%

echo ========================================
echo 开始交叉编译 x-tunnel
echo ========================================
echo.

REM 遍历平台
for %%P in (%PLATFORMS%) do (
    for %%T in (client) do (
        call :build_binary %%T %%P
    )
)

REM 显示统计信息
echo.
echo ========================================
echo 编译完成
echo ========================================
echo 总计: %TOTAL_BUILDS% 个二进制文件
echo 成功: %SUCCESS_COUNT%
if %FAILED_COUNT% GTR 0 (
    echo 失败: %FAILED_COUNT%
)
echo.

REM 列出生成的文件
echo 生成的文件:
dir /b bin\xtunnel-*.* 2>nul
if errorlevel 1 (
    echo 警告: 没有找到生成的文件
)

REM 返回状态
if %FAILED_COUNT% GTR 0 exit /b 1
exit /b 0

REM ===== 编译函数 =====
:build_binary
setlocal
set TARGET=%~1
set PLATFORM=%~2

REM 解析平台信息
for /f "tokens=1,2 delims=/" %%a in ("%PLATFORM%") do (
    set OS=%%a
    set ARCH_VARIANT=%%b
)

REM 处理 armv7 特殊情况
set ARCH=%ARCH_VARIANT%
set ARCH_NAME=%ARCH_VARIANT%
if "%ARCH_VARIANT%"=="armv7" (
    set ARCH=arm
    set ARCH_NAME=armv7
)

REM 处理 Windows 文件扩展名
set EXT=
if "%OS%"=="windows" set EXT=.exe

REM 输出文件名
set OUTPUT=bin\xtunnel-%TARGET%-%OS%-%ARCH_NAME%%EXT%

REM 源文件列表（本分支仅保留客户端）
set SOURCES=.\client\cmd\x-tunnel-client

REM 显示编译信息
<nul set /p "=编译: %TARGET% %OS%/%ARCH_NAME%... "

REM 设置环境变量
set GOOS=%OS%
set GOARCH=%ARCH%
set CGO_ENABLED=0

REM 处理 ARM 架构的特殊情况
if "%ARCH%"=="arm" set GOARM=7

REM 构建编译命令
set BUILD_CMD=go build -trimpath -ldflags="-s -w" -o %OUTPUT% %SOURCES%

REM 执行编译
if "%VERBOSE%"=="true" (
    echo.
    echo %BUILD_CMD%
)

REM 执行编译并重定向输出
%BUILD_CMD% >nul 2>&1
if errorlevel 1 (
    echo [失败]
    endlocal
    set /a FAILED_COUNT+=1
    exit /b 1
) else (
    echo [成功]
    endlocal
    set /a SUCCESS_COUNT+=1
    exit /b 0
)
