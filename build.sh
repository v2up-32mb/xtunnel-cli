#!/bin/bash
# x-tunnel 跨平台编译脚本
# 支持新的 pkg 构建方式和旧的构建方式

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# 显示帮助信息
show_help() {
    echo "用法: $0 [选项]"
    echo ""
    echo "选项:"
    echo "  -h, --help     显示此帮助信息"
    echo "  -v, --verbose  显示详细的编译命令"
    echo "  -c, --clean    清理 bin 目录后重新编译"
    echo "  --legacy       使用旧的构建方式（已废弃）"
    echo "  --new          使用新的 pkg 构建方式（默认）"
    echo ""
    echo "构建方式:"
    echo "  新方式 (默认): 使用 client/pkg 和 server/pkg"
    echo "    输出: bin/xtunnel-client 和 bin/xtunnel-server"
    echo ""
    echo "  旧方式 (--legacy): 使用根目录的旧源文件（已废弃）"
    echo "    输出: bin/xtunnel-{client,server}-{os}-{arch}"
    echo ""
    echo "支持的平台:"
    echo "  - linux/amd64"
    echo "  - linux/arm64"
    echo "  - linux/armv7"
    echo "  - windows/amd64"
    echo "  - windows/386"
}

# 默认参数
VERBOSE=false
CLEAN=false
USE_NEW=true  # 默认使用新的构建方式

# 解析命令行参数
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_help
            exit 0
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -c|--clean)
            CLEAN=true
            shift
            ;;
        --legacy)
            USE_NEW=false
            shift
            ;;
        --new)
            USE_NEW=true
            shift
            ;;
        *)
            echo -e "${RED}错误: 未知参数 '$1'${NC}"
            show_help
            exit 1
            ;;
    esac
done

# 检查 Go 是否安装
if ! command -v go &> /dev/null; then
    echo -e "${RED}错误: 未找到 Go 工具链${NC}"
    echo "请先安装 Go: https://golang.org/dl/"
    exit 1
fi

# 显示 Go 版本
GO_VERSION=$(go version | awk '{print $3}')
echo -e "${GREEN}Go 版本: ${GO_VERSION}${NC}"

# 定义平台数组
platforms=(
    "linux/amd64"
    "linux/arm64"
    "linux/armv7"
    "windows/amd64"
    "windows/386"
)

# 创建输出目录
mkdir -p bin

# 如果需要清理，删除旧的二进制文件
if [ "$CLEAN" = true ]; then
    echo -e "${YELLOW}清理 bin 目录...${NC}"
    rm -f bin/xtunnel-* || true
fi

# 显示构建方式
if [ "$USE_NEW" = true ]; then
    echo -e "${GREEN}使用新的 pkg 构建方式 (client/cmd, server/cmd)${NC}"
else
    echo -e "${YELLOW}使用旧的构建方式 (已废弃)${NC}"
fi

# 统计变量
total_builds=0
success_count=0
failed_count=0
failed_builds=()

# 记录开始时间
start_time=$(date +%s)

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}开始交叉编译 x-tunnel${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# 编译函数（新版）
build_binary_new() {
    local target=$1      # client 或 server
    local os=$2          # GOOS
    local arch=$3        # GOARCH
    local variant=$4     # armv7 (可选)

    # 设置环境变量
    export GOOS=$os
    export GOARCH=$arch
    export CGO_ENABLED=0

    # 处理 ARM 架构的特殊情况
    local arch_name=$arch
    if [ "$arch" = "arm" ]; then
        export GOARM=7
        arch_name="armv7"
    fi

    # 处理 Windows 文件扩展名
    local ext=""
    if [ "$os" = "windows" ]; then
        ext=".exe"
    fi

    # 输出文件名
    local output="bin/xtunnel-${target}-${os}-${arch_name}${ext}"

    # 源路径
    local source_path=""
    if [ "$target" = "client" ]; then
        source_path="./client/cmd/x-tunnel-client"
    else
        source_path="./server/cmd/x-tunnel-server"
    fi

    # 构建编译命令
    local build_cmd="go build -trimpath -ldflags=\"-s -w\" -o ${output} ${source_path}"

    # 显示编译信息
    echo -n "编译: ${target} ${os}/${arch_name}... "

    # 执行编译
    if [ "$VERBOSE" = true ]; then
        echo ""
        echo $build_cmd
    fi

    if eval $build_cmd > /dev/null 2>&1; then
        echo -e "${GREEN}✓${NC}"
        ((success_count++))
        return 0
    else
        echo -e "${RED}✗${NC}"
        ((failed_count++))
        failed_builds+=("${target} ${os}/${arch_name}")
        return 1
    fi
}

# 编译函数（旧版 - 已废弃）
build_binary_legacy() {
    local target=$1      # client 或 server
    local os=$2          # GOOS
    local arch=$3        # GOARCH
    local variant=$4     # armv7 (可选)

    # 设置环境变量
    export GOOS=$os
    export GOARCH=$arch
    export CGO_ENABLED=0

    # 处理 ARM 架构的特殊情况
    local arch_name=$arch
    if [ "$arch" = "arm" ]; then
        export GOARM=7
        arch_name="armv7"
    fi

    # 处理 Windows 文件扩展名
    local ext=""
    if [ "$os" = "windows" ]; then
        ext=".exe"
    fi

    # 输出文件名
    local output="bin/xtunnel-${target}-${os}-${arch_name}-legacy${ext}"

    # 源文件列表
    local sources=""
    if [ "$target" = "client" ]; then
        sources="x-tunnel-client.go client_*.go relay_manager.go ip_strategy.go common.go protocol.go"
    else
        sources="x-tunnel-server.go server_pool.go server_cert.go ip_strategy.go common.go protocol.go"
    fi

    # 构建编译命令
    local build_cmd="go build -trimpath -ldflags=\"-s -w\" -o ${output} ${sources}"

    # 显示编译信息
    echo -n "编译: ${target} ${os}/${arch_name} (legacy)... "

    # 执行编译
    if [ "$VERBOSE" = true ]; then
        echo ""
        echo $build_cmd
    fi

    if eval $build_cmd > /dev/null 2>&1; then
        echo -e "${GREEN}✓${NC}"
        ((success_count++))
        return 0
    else
        echo -e "${RED}✗${NC}"
        ((failed_count++))
        failed_builds+=("${target} ${os}/${arch_name}")
        return 1
    fi
}

# 主编译逻辑
if [ "$USE_NEW" = true ]; then
    # 新方式：遍历所有平台编译
    for platform in "${platforms[@]}"; do
        # 解析平台信息
        IFS='/' read -r os arch_variant <<< "$platform"

        # 处理 armv7 特殊情况
        arch=$arch_variant
        if [ "$arch_variant" = "armv7" ]; then
            arch="arm"
        fi

        ((total_builds++))
        build_binary_new "client" "$os" "$arch" "$arch_variant"

        ((total_builds++))
        build_binary_new "server" "$os" "$arch" "$arch_variant"
    done
else
    # 旧方式：每个平台编译一次
    for platform in "${platforms[@]}"; do
        # 解析平台信息
        IFS='/' read -r os arch_variant <<< "$platform"

        # 处理 armv7 特殊情况
        arch=$arch_variant
        if [ "$arch_variant" = "armv7" ]; then
            arch="arm"
        fi

        ((total_builds++))
        build_binary_legacy "client" "$os" "$arch" "$arch_variant"

        ((total_builds++))
        build_binary_legacy "server" "$os" "$arch" "$arch_variant"
    done
fi

# 计算耗时
end_time=$(date +%s)
duration=$((end_time - start_time))

# 显示统计信息
echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}编译完成${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "总计: ${total_builds} 个二进制文件"
echo -e "${GREEN}成功: ${success_count}${NC}"
if [ $failed_count -gt 0 ]; then
    echo -e "${RED}失败: ${failed_count}${NC}"
    echo ""
    echo -e "${RED}失败的平台:${NC}"
    for failed in "${failed_builds[@]}"; do
        echo -e "  ${RED}✗${NC} ${failed}"
    done
fi
echo -e "耗时: ${duration} 秒"

# 列出生成的文件
echo ""
echo -e "${GREEN}生成的文件:${NC}"
ls -lh bin/ | grep xtunnel | awk '{printf "  %s  %s\n", $5, $9}'

# 返回状态
if [ $failed_count -gt 0 ]; then
    exit 1
fi

exit 0
