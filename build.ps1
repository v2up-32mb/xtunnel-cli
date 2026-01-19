# x-tunnel 跨平台编译脚本 (PowerShell)
# 支持交叉编译多个平台

param(
    [switch]$h,
    [switch]$help,
    [switch]$v,
    [switch]$verbose,
    [switch]$c,
    [switch]$clean
)

# 显示帮助信息
function Show-Help {
    Write-Host "用法: .\build.ps1 [选项]"
    Write-Host ""
    Write-Host "选项:"
    Write-Host "  -h, -help       显示此帮助信息"
    Write-Host "  -v, -verbose    显示详细的编译命令"
    Write-Host "  -c, -clean      清理 bin 目录后重新编译"
    Write-Host ""
    Write-Host "此脚本将编译以下平台的 client 和 server:"
    Write-Host "  - linux/amd64"
    Write-Host "  - linux/arm64"
    Write-Host "  - linux/armv7"
    Write-Host "  - windows/amd64"
    Write-Host "  - windows/386"
    Write-Host ""
    Write-Host "所有二进制文件将输出到 bin/ 目录"
    exit 0
}

# 处理帮助参数
if ($h -or $help) {
    Show-Help
}

# 检查 Go 是否安装
if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
    Write-Host "错误: 未找到 Go 工具链" -ForegroundColor Red
    Write-Host "请先安装 Go: https://golang.org/dl/"
    exit 1
}

# 显示 Go 版本
$goVersion = (go version).Split()[2]
Write-Host "Go 版本: $goVersion" -ForegroundColor Green
Write-Host ""

# 定义平台配置
$platforms = @(
    @{ OS = "linux"; Arch = "amd64"; OutputArch = "amd64" }
    @{ OS = "linux"; Arch = "arm64"; OutputArch = "arm64" }
    @{ OS = "linux"; Arch = "arm"; ArmVersion = "7"; OutputArch = "armv7" }
    @{ OS = "windows"; Arch = "amd64"; OutputArch = "amd64" }
    @{ OS = "windows"; Arch = "386"; OutputArch = "386" }
)

# 创建输出目录
New-Item -ItemType Directory -Force -Path bin | Out-Null

# 如果需要清理，删除旧的二进制文件
if ($clean) {
    Write-Host "清理 bin 目录..." -ForegroundColor Yellow
    Remove-Item -Path "bin\xtunnel-*.*" -Force -ErrorAction SilentlyContinue
}

# 统计变量
$totalBuilds = $platforms.Count * 2  # 每个平台编译 client 和 server
$successCount = 0
$failedCount = 0
$failedBuilds = @()

# 记录开始时间
$startTime = Get-Date

Write-Host "========================================" -ForegroundColor Green
Write-Host "开始交叉编译 x-tunnel" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""

# 编译函数
function Build-Binary {
    param(
        [string]$target,
        [hashtable]$platform
    )

    $os = $platform.OS
    $arch = $platform.Arch
    $archName = $platform.OutputArch

    # 设置环境变量
    $env:GOOS = $os
    $env:GOARCH = $arch
    $env:CGO_ENABLED = "0"

    # 处理 ARM 架构的特殊情况
    if ($arch -eq "arm" -and $platform.ArmVersion) {
        $env:GOARM = $platform.ArmVersion
    }

    # 处理 Windows 文件扩展名
    $ext = ""
    if ($os -eq "windows") {
        $ext = ".exe"
    }

    # 输出文件名
    $output = "bin\xtunnel-${target}-${os}-${archName}${ext}"

    # 源文件列表
    if ($target -eq "client") {
        $sources = ".\client\cmd\x-tunnel-client"
    } else {
        $sources = ".\server\cmd\x-tunnel-server"
    }

    # 显示编译信息
    Write-Host -NoNewline "编译: ${target} ${os}/${archName}... "

    # 构建编译命令
    $buildCmd = "go build -trimpath -ldflags='-s -w' -o ${output} ${sources}"

    # 显示详细命令
    if ($verbose) {
        Write-Host ""
        Write-Host $buildCmd -ForegroundColor Gray
    }

    # 执行编译
    $outputPath = "$output"
    $errorOutput = & go build -trimpath -ldflags="-s -w" -o $outputPath $sources.Split() 2>&1

    if ($LASTEXITCODE -eq 0) {
        Write-Host "[成功]" -ForegroundColor Green
        return $true
    } else {
        Write-Host "[失败]" -ForegroundColor Red
        if ($verbose -and $errorOutput) {
            Write-Host $errorOutput -ForegroundColor Red
        }
        return $false
    }
}

# 遍历所有平台
foreach ($platform in $platforms) {
    $os = $platform.OS
    $archName = $platform.OutputArch

    # 编译 client
    $result = Build-Binary -target "client" -platform $platform
    if ($result) {
        $successCount++
    } else {
        $failedCount++
        $failedBuilds += "client ${os}/${archName}"
    }

    # 编译 server
    $result = Build-Binary -target "server" -platform $platform
    if ($result) {
        $successCount++
    } else {
        $failedCount++
        $failedBuilds += "server ${os}/${archName}"
    }
}

# 计算耗时
$endTime = Get-Date
$duration = ($endTime - $startTime).TotalSeconds

# 显示统计信息
Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "编译完成" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host "总计: $totalBuilds 个二进制文件"
Write-Host "成功: $successCount" -ForegroundColor Green

if ($failedCount -gt 0) {
    Write-Host "失败: $failedCount" -ForegroundColor Red
    Write-Host ""
    Write-Host "失败的平台:" -ForegroundColor Red
    foreach ($failed in $failedBuilds) {
        Write-Host "  ✗ $failed" -ForegroundColor Red
    }
}

Write-Host "耗时: $([math]::Round($duration, 2)) 秒"

# 列出生成的文件
Write-Host ""
Write-Host "生成的文件:" -ForegroundColor Green
$files = Get-ChildItem -Path "bin\xtunnel-*.*" -ErrorAction SilentlyContinue
if ($files) {
    $files | ForEach-Object {
        $size = [math]::Round($_.Length / 1MB, 2)
        Write-Host "  $($size.ToString('0.00')) MB  $($_.Name)"
    }
} else {
    Write-Host "  警告: 没有找到生成的文件" -ForegroundColor Yellow
}

# 返回状态
if ($failedCount -gt 0) {
    exit 1
}

exit 0
