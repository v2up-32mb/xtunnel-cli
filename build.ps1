# x-tunnel Windows 交叉编译脚本（仅客户端）
# 用于编译 Windows x86 和 amd64 架构的客户端

Write-Host "====================================" -ForegroundColor Cyan
Write-Host "  x-tunnel Windows 客户端编译脚本" -ForegroundColor Cyan
Write-Host "====================================" -ForegroundColor Cyan
Write-Host ""

# 禁用 CGO
$env:CGO_ENABLED = "0"

# 创建 bin 目录
Write-Host "[1/2] 创建 bin 目录..." -ForegroundColor Yellow
if (-not (Test-Path "bin")) {
    New-Item -ItemType Directory -Path "bin" | Out-Null
}

# 编译 Windows amd64 客户端
Write-Host "[2/2] 编译 Windows amd64 客户端..." -ForegroundColor Yellow
$env:GOOS = "windows"
$env:GOARCH = "amd64"
go build -tags client -o bin/xtunnel-client-amd64.exe .
if ($LASTEXITCODE -ne 0) {
    Write-Host "错误: 客户端 amd64 编译失败" -ForegroundColor Red
    exit 1
}

# 编译 Windows 386 客户端
Write-Host "[2/2] 编译 Windows x86 客户端..." -ForegroundColor Yellow
$env:GOOS = "windows"
$env:GOARCH = "386"
go build -tags client -o bin/xtunnel-client-386.exe .
if ($LASTEXITCODE -ne 0) {
    Write-Host "错误: 客户端 x86 编译失败" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "====================================" -ForegroundColor Green
Write-Host "  编译完成！" -ForegroundColor Green
Write-Host "====================================" -ForegroundColor Green
Write-Host "输出目录: bin\" -ForegroundColor White
Write-Host ""
Write-Host "生成的文件:" -ForegroundColor White
Get-ChildItem -Path "bin\*.exe" | ForEach-Object {
    Write-Host ("  - {0} ({1:N2} MB)" -f $_.Name, ($_.Length / 1MB)) -ForegroundColor White
}
Write-Host ""
