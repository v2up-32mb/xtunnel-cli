@echo off
REM x-tunnel Windows 交叉编译脚本（仅客户端）
REM 用于编译 Windows x86 和 amd64 架构的客户端

chcp 65001 >nul
echo ====================================
echo   x-tunnel Windows 客户端编译脚本
echo ====================================
echo.

REM 禁用 CGO
set CGO_ENABLED=0

REM 创建 bin 目录
if not exist bin mkdir bin
echo [1/2] 创建 bin 目录...

REM 编译 Windows amd64 客户端
echo [2/2] 编译 Windows amd64 客户端...
set GOOS=windows
set GOARCH=amd64
go build -tags client -o bin/xtunnel-client-amd64.exe .
if %errorlevel% neq 0 (
    echo 错误: 客户端 amd64 编译失败
    pause
    exit /b 1
)

REM 编译 Windows 386 客户端
echo [2/2] 编译 Windows x86 客户端...
set GOOS=windows
set GOARCH=386
go build -tags client -o bin/xtunnel-client-386.exe .
if %errorlevel% neq 0 (
    echo 错误: 客户端 x86 编译失败
    pause
    exit /b 1
)

echo.
echo ====================================
echo   编译完成！
echo ====================================
echo 输出目录: bin\
echo.
echo 生成的文件:
dir /b bin\*.exe
echo.
pause
