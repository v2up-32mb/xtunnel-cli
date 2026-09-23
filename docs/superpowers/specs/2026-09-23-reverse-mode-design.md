# 反向流量通道（Reverse Mode）设计

> 状态：已评审定稿（v3）
> 日期：2026-09-23
>
> v3 修订（评审决策落地）：
> 1. 私网过滤 v1 不实现，移入「未来规划」（§7）
> 2. 绑定失败语义：多 `-l` 值逐个注册，失败的打客户端日志并继续其余；**全部失败 → 客户端 fatal 退出**
> 3. 服务端每客户端反向监听器数量上限默认 **3**（flag `-max-reverse-listeners`）
> 4. 新增正/反向流程图（§4a）
>
> v2 修订：
> 1. 去掉 connID `rev-` 前缀——靠各端的双连接表（正向表/反向表）按 connID 查找区分，无需前缀
> 2. 反向模式复用正向的**广播竞争**机制，仅上下行竞争方向互换（严格机械镜像）
> 3. 服务端**不新增监听 flag**：客户端 `-r` 时把 `-l` 的参数值传给服务端，由服务端按值监听，端口被占用则报错
> 4. 多客户端策略：监听器按"注册它的客户端"一一对应，端口 N 的流量只从注册者客户端出去，无负载均衡
> 5. 新增"私网过滤"功能详解章节（评审要求）

## 1. 需求与拓扑

客户端增加 `-r/--reverse` 开关。开启后，客户端不再打开本地代理端口，而是把 `-l` 的监听参数值传给服务端，由**服务端**按该值开监听；从该端口进入的流量经隧道由**客户端**侧出去。

```
正向:  应用 → [客户端 :1080] ══隧道══> [服务端] → 目标   (出口 = 服务端 IP)
反向:  应用 → [服务端 :30000] ══隧道══> [客户端] → 目标   (出口 = 客户端 IP)
```

- 客户端 A `-l socks5://0.0.0.0:30000 -r`、客户端 B `-l socks5://0.0.0.0:30001 -r`：30000 的流量从 A 出，30001 的流量从 B 出，一一对应，无任何负载均衡。
- 监听协议与鉴权由 `-l` 值决定（`socks5://[user:pass@]addr` / `http://[user:pass@]addr`），故 socks5 与 http 天然都支持。

## 2. 总体设计：角色互换 + 广播竞争换向

正向模式中客户端是"请求方"（广播拨号请求）、服务端是"拨号方"（首达占用 + 本地拨号）。反向模式两者角色互换，**竞争机制完全复用，只是方向对调**：

| 环节 | 正向 | 反向 |
|---|---|---|
| 谁广播 `MsgTCPConnect` | 客户端（请求方） | 服务端（请求方，只发给**拥有该监听器的那个客户端**的全部通道） |
| 首达占用 | 服务端：第一个送达的通道占用连接 | 客户端：第一个送达的通道占用连接 |
| 首达通道的含义 | 请求方→拨号方 数据通道（C→S） | 请求方→拨号方 数据通道（S→C） |
| 谁广播 `MsgSelectUplink` | 服务端（拨号方），携带首达通道号 | 客户端（拨号方），携带首达通道号 |
| 谁竞争 `MsgSelectUplink` | 客户端：最快收到的通道成为自己的收包通道（S→C 下行） | 服务端：最快收到的通道成为自己的收包通道（C→S 下行） |
| 谁发 `MsgSelectDownlink` | 客户端经首达通道确认自己选的下行 | 服务端经首达通道确认自己选的下行 |
| 谁发 `MsgConnStatus` | 服务端拨号后广播 OK/ERR | 客户端拨号后广播 OK/ERR |

即：**"最快收到 MsgSelectUplink 的一方"永远是为自己的收包路径选通道的一方**——正向是客户端，反向换成服务端。两端代码各含一半逻辑，反向即把对端的那一半在同侧镜像实现（服务端镜像 `clientPool` 的请求方逻辑，客户端镜像 `server/pkg` 的拨号方逻辑），已验证的竞争/路由/清理模式全部沿用。

多通道收益与正向一致：拨号阶段广播天然容忍个别死通道；数据阶段双通道分离（收发各走竞争出的最优通道）。

**connID 无需前缀**：正向连接由客户端生成 UUID、登记在"正向连接表"；反向连接由服务端生成 UUID、登记在"反向连接表"。消息分发时先查本端的两张表：
- 服务端收到 `MsgSelectUplink/MsgConnStatus/MsgTCPData/...`：connID 在正向表 → 走现有正向逻辑；在反向表 → 走反向逻辑；都查不到 → 忽略。
- 客户端收到 `MsgTCPConnect`（新增 case）→ 必为反向拨号请求；`MsgSelectUplink/MsgTCPData/MsgTCPClose` 同样先查正向表再查反向表。

UUID 冲突概率可忽略，且每条连接只存在于一张表中，分发无歧义。

## 3. 协议改动（唯一一处，仅新增常量）

消息格式（`EncodeMessage/DecodeMessage`）**零变更**。`MsgTCPConnect/MsgSelectUplink/MsgSelectDownlink/MsgConnStatus/MsgTCPData/MsgTCPClose` 全部按上表复用（meta 语义不变：`MsgTCPConnect` 的 meta 仍为 `[ipStrategy, target]`）。

新增 2 个控制消息类型（加法式变更，旧端 switch 无此 case 自动忽略）：

| 类型 | 方向 | 用途 |
|---|---|---|
| `MsgReverseListen` (0x20) | 客户端→服务端 | meta = 监听参数值原文（如 `socks5://user:pass@0.0.0.0:30000`）；connID = 监听器 ID（uuid） |
| `MsgReverseListenResult` (0x21) | 服务端→客户端 | meta[0] = OK/ERR，meta[1:] = 失败原因文本（如 `bind: address already in use`） |

> 为什么不能用握手 query 参数替代：端口占用错误需要一条"从服务端回到客户端"的结果通路，query 参数没有响应路径；升级失败（拒绝 101）会误杀整条隧道，不可接受。且消息方式天然支持重连后重新注册、服务端重启后自动恢复。

## 4a. 流程图

### 正向（现状基线）

```
应用                  客户端                                服务端                          目标
 │ SOCKS5 握手          │                                    │                              │
 │────────────────────▶│                                    │                              │
 │ CONNECT target      │                                    │                              │
 │────────────────────▶│                                    │                              │
 │                     │ 生成 connID(uuid)，登记正向表          │                              │
 │                     │ 广播 MsgTCPConnect(connID,          │                              │
 │                     │  [策略,target]) 到通道 1..N ───────▶│ 通道 X 首达占用，登记           │
 │                     │                                    │ 广播 MsgSelectUplink([X])     │
 │                     │ ◀──────────────────────────────────│ 到通道 1..N                   │
 │                     │ 最快收到者 ⇒ 通道 Y = 下行(S→C)        │ 异步 net.DialTimeout(target) ─▶│
 │                     │ 经上行通道 X 发                      │                              │
 │                     │ MsgSelectDownlink([Y]) ────────────▶│ 校验来自 X，记录下行=Y          │
 │                     │                                    │ ◀────── 连接成功 ─────────────│
 │                     │ ◀─── 广播 MsgConnStatus(OK) ────────│                              │
 │ ◀── SOCKS5 回复成功 ──│                                    │                              │
 │                     │                                    │                              │
 │ 数据 ───────────────▶│ 上行：MsgTCPData 单播通道 X           │                              │
 │                     │───────────────────────────────────▶│ 写入目标 ────────────────────▶│
 │ ◀────────────────── │ 下行：通道 Y 的 MsgTCPData            │ ◀────── 目标数据 ─────────────│
 │      写入本地 conn    │ ◀──────────────────────────────────│                              │
 │                     │                                    │                              │
 │ 任一侧关闭/EOF ⇒ MsgTCPClose ⇒ 双端清理并注销                 │                              │
```

### 反向（新增）

```
应用                  服务端                                客户端                          目标
 │ SOCKS5 握手          │                                    │                              │
 │────────────────────▶│                                    │                              │
 │ CONNECT target      │                                    │                              │
 │────────────────────▶│                                    │                              │
 │                     │ 生成 connID(uuid)，登记反向表          │                              │
 │                     │ 广播 MsgTCPConnect(connID,          │                              │
 │                     │  [策略,target]) 到该客户端 ─────────▶│ 通道 X 首达占用，登记           │
 │                     │  的通道 1..N                        │ 广播 MsgSelectUplink([X])     │
 │                     │ ◀──────────────────────────────────│ 到服务端全部通道                │
 │                     │ 最快收到者 ⇒ 通道 Y = P2(C→S)         │ 异步 net.DialTimeout(target) ─▶│
 │                     │ 经首达通道 X 发                      │                              │
 │                     │ MsgSelectDownlink([Y]) ────────────▶│ 校验来自 X，记录 P2=Y          │
 │                     │                                    │ ◀────── 连接成功 ─────────────│
 │                     │ ◀─── 广播 MsgConnStatus(OK) ────────│                              │
 │ ◀── SOCKS5 回复成功 ──│                                    │                              │
 │                     │                                    │                              │
 │ 数据 ───────────────▶│ P1(S→C)：MsgTCPData 单播通道 X        │                              │
 │                     │───────────────────────────────────▶│ 写入本地 conn ───────────────▶│
 │ ◀────────────────── │ C→S：通道 Y 的 MsgTCPData             │ ◀────── 目标数据 ─────────────│
 │      写入管道→SOCKS5  │ ◀──────────────────────────────────│                              │
 │                     │                                    │                              │
 │ 任一侧关闭/EOF ⇒ MsgTCPClose ⇒ 双端清理并注销                 │                              │
```

> 两图逐行同构，仅"客户端/服务端"角色互换：首达竞争决定请求方发包通道（正向 X=C→S、反向 X=S→C）；最快收到 MsgSelectUplink 的一方为自己的收包通道选路（正向=客户端选 Y，反向=服务端选 Y）。

### 监听器注册（仅反向，连接建立前的一次性流程）

```
客户端启动: -r -l socks5://0.0.0.0:30000[,http://0.0.0.0:8080]
 │
 │ 通道 1..N 建立后，由首个就绪通道发送：
 │   MsgReverseListen(id=uuid-1, meta="socks5://0.0.0.0:30000")
 │   MsgReverseListen(id=uuid-2, meta="http://0.0.0.0:8080")
 ▼
服务端 按 (clientID, 监听值) 幂等处理：
   首次 → bind：成功 → 回 OK，监听器归属该 clientID
                失败 → 回 ERR+原因（端口被占/超 -max-reverse-listeners）
   重复 → 幂等回 OK
 ▼
客户端汇总每轮注册结果：
   ≥1 个 OK → 继续运行（ERR 的打日志）
   全部 ERR → fatal 退出（仅启动首轮；运行中重连的重注册只记日志）
```

### HotPair 预热路径（-hotpair 启用时）

正向现状：`-hotpair` 时代理请求到达**不走广播竞争**——`AcquirePrimary()` 取 Ready 状态预热 Pair，`MsgTCPConnect` 单播到 `pair.UplinkChID`（首帧 ≈ 1 RTT）；`selectDownlink` 直接 CAS 采用预绑定 `pair.DownlinkChID`。竞争被 PairWarmer 后台提前执行：循环广播 `MsgPrebindRequest`（connID=`prebind-<uuid>`，target=`x-tunnel.prebind`）→ 服务端首达占用、回 `MsgSelectUplink`、立即注销不拨号 → 客户端竞争收包确定 RX → Pair `{TX,RX}` Ready。兜底：无 Ready Pair 或单播发送失败 → 回退广播+竞争并废弃该通道。
注：拨号期的 `MsgSelectUplink` 广播与 `MsgSelectDownlink` 应答与目标拨号**并行**，无首字节延迟损失，仅少量控制帧。
**为何正向不做 connID 复用（回归教训，曾实测 8 路并发仅 2 路成功）**：正向 Pair 为**共享复用**（`AcquirePrimary` 引用计数，多连接同用一对通道），连接 connID 必须每连接唯一——复用 prebind connID 会导致并发连接互相覆盖、被服务端当重复请求丢弃。反向能复用 prebind connID 是因为其 Pair **一次性消费**（`AcquirePair` 弹出），每连接独占，connID 天然唯一。

反向按对称镜像在**服务端**实现 ReversePairWarmer（回程竞争 P2 只有请求方能做，故 Warmer 必须在服务端，客户端无法代劳）：

```
预热(后台循环，三步完成全部选路):
  ① 服务端向已授权客户端广播 MsgPrebindRequest("prebind-x", 复用 0x10)
  ② 客户端通道 X 首达占用 → 广播 MsgSelectUplink("prebind-x",[X])（不拨号）
  ③ 服务端竞争收包，最快通道 Y = P2 → Pair{P1:X, P2:Y} 就绪，
     并立即经 P1 回 MsgSelectDownlink("prebind-x",[Y]) —— 客户端据此补全
     sendCh=P2 并保留 warm 状态。至此双方均持有 {P1,P2}，上下行竞争全部完成。
请求时:  AcquirePair → 复用 prebind connID 作为关联凭证
                → MsgTCPConnect("prebind-x",[策略,target]) 单播 P1（拨号期零选路消息）
                → 客户端凭 warm 状态直接提升为真实连接（recvCh=P1、sendCh=P2 已知）
                → 回 MsgConnStatus。无竞争，首帧 ≈ 1 RTT
兑底:    无 Ready Pair → 回退 §4b 广播+竞争流程；客户端 warm 状态丢失（TTL 到期/
         通道抖动）→ 客户端重新广播 MsgSelectUplink → 服务端采用其收包通道作为
         发包通道并幂等补发一次 MsgSelectDownlink 修复；通道死亡(断连)
                → InvalidateChannel 废弃包含该通道的 Pair 并重新预热
         warm 状态 5s TTL 兜底清理，防服务端 Pair 未被消费时泄漏
```

配置（v3 修订）：预热开关由**客户端启动参数**决定（与 `-l` 同一控制权归属）——客户端 `-r -hotpair` 时，每条通道就绪发送新增消息 `MsgReverseHotPair (0x22)`（空 meta/payload）授权服务端为本客户端预热；服务端**零配置项**，预热器常驻、参数为内部常量（每客户端 1 对、30s 刷新），仅对已授权且已注册监听器的客户端生效；客户端全部通道掉线撤销授权，重连需重新授权。旧服务端 switch 无此 case 自动忽略（无预热但功能不受影响）；旧客户端不发该消息，服务端永不预热。预绑定/预热请求与响应仍复用 `MsgPrebindRequest`/`MsgSelectUplink`，无格式变更。

## 4c. 反向连接建立时序说明（P1 = 服务端发包通道，P2 = 客户端发包通道）

```
服务端(请求方)                                    客户端(拨号方)
│ SOCKS5 accept → 解析 target
│ 生成 connID(uuid)，登记反向表
│ 广播 MsgTCPConnect(connID,[策略,target]) ──┐
│                                            ├──▶ 通道 X 首达占用：
│                                            │    反向表登记{target, P1=X}
│                                            │    广播 MsgSelectUplink(connID,[X])
│ ◀── 各通道陆续收到 MsgSelectUplink ────────┤    （同时异步 net.DialTimeout(target)）
│ 竞争：最快收到的通道 Y 记为 P2               │
│ 经 P1 发 MsgSelectDownlink(connID,[Y]) ────┤    ◀── 校验来自 P1，记录 P2=Y
│                                            │
│ ◀──────────── 广播 MsgConnStatus(OK/ERR) ──┤    拨号完成
│ OK → 管道就绪，DialStream 返回               │
│                                            │
│ [S→C] SOCKS5 读 → MsgTCPData               │    收包校验 ch==P1 → 写入本地 conn
│      P2 已知前广播，已知后单播 P1            │
│ [C→S]                                      │    本地 conn 读 → MsgTCPData
│      收包校验 ch==P2 → 写入管道 → SOCKS5     │    P2 已知前广播，已知后单播 P2
│                                            │
│ 任一侧 EOF/错误 → MsgTCPClose → 双端清理     │
```

- `ipStrategy` 字节沿用：v1 客户端忽略该字节，用自身 `-ips` 配置（域名由出口方=客户端解析）。
- 背压：S→C 走服务端既有全局写队列 + `MsgBackpressure`；C→S 走客户端既有 `writeQueues`。零新增状态机。
- 通道死亡：两端 `cleanupChannel` 关闭绑定该通道的反向连接（对齐正向现有行为）。

## 4d. 全生命周期时序（-hotpair 启用；从客户端启动到断开）

图例：〔B〕=广播；〔P1〕〔P2〕〔通道N〕=单播；时间自上而下。P1=服务端发包通道（客户端首达占用），P2=客户端发包通道（服务端首达竞争）。

```
 客户端  -r -hotpair -l socks5://127.0.0.1:28180              服务端（零配置）
─────────────────────────────────────────────────────────────────────────────────
 ① 启动
        │ ═ WSS 拨号 ×N（TLS/ECH，token 走 subprotocol）════════▶ │ 通道 1..N 注册
        │ ◀──────────────── 101 Switching Protocols ──────────── │ 日志：通道 X 已连接
        │                                                        │
 ② 授权+│ ═ 每条通道就绪 reverseOnChannelReady ═══════════════════
   监听 │ ── MsgReverseHotPair (0x22) 〔通道N〕 ─────────────────▶ │ 预热授权 enabled+={client}
        │                                                        │ Kick：立即补足预热
        │ ── MsgReverseListen(lid,"socks5://…:28180") 〔通道N〕 ─▶ │ 绑定 127.0.0.1:28180（幂等）
        │ ◀───── MsgReverseListenResult(OK) 〔同通道〕 ─────────── │ 日志：反向监听已开启
        │                                                        │
 ③ Pair │ ◀── ①MsgPrebindRequest("prebind-x",[策略,prebind]) 〔B〕─ │ 预热循环（授权 ∩ 有监听器）
   预热 │ 通道 X 首达占用 rc{warm,recvCh=X}（不拨号）               │
        │ ── ②MsgSelectUplink("prebind-x",[X]) 〔B〕 ───────────▶ │ 竞争：最快到达通道 Y
        │ ◀── ③MsgSelectDownlink("prebind-x",[Y]) 〔P1=X〕 ────── │ Pair{P1:X, P2:Y} Ready（一次性）
        │ 校验来自 P1=X → 补全 sendCh=Y，保留 warm 状态            │ 日志：Pair 构建完成
        │ （至此上下行竞争全部完成，双方均持有 {P1,P2}）            │
        │                                                        │
 ④ 代理 │                                                 curl ──▶│ SOCKS5 :28180 accept（握手在服务端本地）
   请求 │                                                        │ 解析 target；AcquirePair → 消费 {P1,P2}
        │ ◀── MsgTCPConnect("prebind-x",[策略,target]) 〔P1〕 ──── │ 复用 prebind connID（关联凭证）
        │ 凭 warm 状态直接提升为真实连接（零选路消息）              │ 单播直达（免竞争，首帧 ≈1 RTT）
        │ net.DialTimeout(target) → 成功                          │ 启动上行泵（管道→P1）
        │ ── MsgConnStatus("prebind-x",OK) 〔B〕 ───────────────▶ │ settle → DialStream 返回管道 conn
        │                                                        │ xshared 双向 io.Copy（用户 ↔ conn）
        │ ◀──────── 上行 MsgTCPData 〔P1〕 ────────────────────── │ 用户请求 → 管道 → 上行泵
        │ 校验 ch==P1 → 写本地 conn ──▶ 目标                      │
        │ ◀──────── 下行 MsgTCPData 〔P2〕 ──────────────────────▶│ 目标响应 → 客户端读泵(sendCh=P2)
        │ 目标 ──▶ 读泵                                          │ 校验 ch==P2 → 写管道 → 用户
        │                                                        │
 ⑤ 连接 │ curl 结束 → SOCKS5 拆隧道 → rc.app 关闭                  │
   关闭 │ ◀── MsgTCPClose(connID) 〔P1〕 ──────────────────────── │ 上行泵管道 EOF → 通知并清理
        │ 关本地 conn，反向表删除                                  │ 反向表删除
        │ （若目标先关：客户端 ── MsgTCPClose 〔P2〕 ──▶ 服务端清理）│
        │                                                        │
 ⑥ 断开 │ SIGINT/SIGTERM → Shutdown()                             │
        │ ── WebSocket Close Frame (1000) 〔每通道〕 ────────────▶ │ readLoop 退出 → cleanupChannel ×2
        │                                                        │ 最后一条 = clientGone：
        │                                                        │  · 监听注销 → 28180 端口释放
        │                                                        │  · 预热授权撤销 + Pair 全废弃
        │                                                        │  · 残留反向连接清理
        │                                                        │ 日志：通道 X 已断开
─────────────────────────────────────────────────────────────────────────────────
```

- 无 `-hotpair` 时：跳过 ② 的授权帧与 ③ 整个阶段；④ 中服务端无 Pair 可取，回退 §4c 广播竞争流程（生成全新 connID）。
- 客户端 warm 状态丢失（5s TTL 到期未被消费/通道抖动）：④ 的 MsgTCPConnect 走普通路径——广播 `MsgSelectUplink` 重新选路，服务端采用其收包通道作为发包通道并幂等补发一次 `MsgSelectDownlink` 修复，功能不中断。
- 服务端参数（预热 1 对、30s 刷新）为内部常量；开关与监听值均由客户端启动参数决定。
- 通道中途死亡：两端 `cleanupChannel` 废弃绑定该通道的 Pair 与反向连接（对齐 §4c 兜底）。

## 5. 监听器生命周期

- **注册**：客户端每建好一条通道就发送 `MsgReverseListen`（每个 `-l` 值一条）。服务端按 `(clientID, 监听值)` 幂等去重——重复注册直接回 OK，不重复 bind。每通道都发的好处：服务端重启后首个重连通道自动恢复全部监听，无需额外状态同步。
- **端口冲突与绑定失败**：bind 失败（含与其他客户端、与服务端自身 HTTPS 端口冲突）→ `MsgReverseListenResult(ERR, 原因)`。
  - 多 `-l` 值逐个注册：某个失败只打客户端日志，其余继续；**一轮注册结果全部为 ERR（客户端没有任何成功监听器）→ fatal 退出**。
  - 启动首轮注册全败 → fatal；**运行中**通道重连引发的重新注册，结果仅记日志、不触发 fatal（避免网络抖动/端口竞争导致运行中的进程意外退出）。重连后对已成功的监听器服务端幂等回 OK；对先前失败的监听器重试 bind（端口若已释放可自愈）。
  - 同一客户端重复请求同一端口为幂等成功；**不同客户端**请求同一端口 → 后到者 ERR。
- **数量上限**：每客户端反向监听器数上限，flag `-max-reverse-listeners`，默认 **3**；超限回 ERR（`listener limit reached`）。
- **注销**：客户端最后一个通道断开（读循环全部退出）→ 服务端关闭该 clientID 的全部反向监听器并释放端口。重连后按幂等注册自动重建。短暂网络抖动会导致监听器短暂下线（经其建立的 SOCKS5 连接断开），重连即恢复；v1 接受此语义，可选加短暂 grace period。
- **服务端自保护**：每客户端/全局反向监听器数量上限（默认如 8，可配置），防止恶意客户端耗尽端口；单监听并发连接数沿用 `xshared` 的 `WithMaxConns`（服务端给默认值）。
- **旧服务端兼容**：老服务端无 `MsgReverseListen` case，静默忽略；客户端 N 秒内未收到任何 `MsgReverseListenResult` 时打警告"服务端不支持反向模式"。老客户端不会发注册消息，新服务端无感知。

## 6. 改动清单

### 6.1 上游 `xtunnel` 库（发 v0.2.0）

> 注：反向 HotPair 的 ReversePairWarmer 落在 CLI 服务端（见 6.2），`xtunnel` 库内现有 PairWarmer 不动。

| 文件 | 改动 |
|---|---|
| `protocol/protocol.go` | + `MsgReverseListen`/`MsgReverseListenResult` 常量（仅常量，格式不变） |
| `config.go` | + `EnableReverse bool`、`ReverseListeners []string`（解析后的 `-l` 值） |
| `reverse_client.go`（新）+ `pool.go` 小改 | 客户端补齐"拨号方"半边（镜像 CLI 服务端 `handler.go` 现有逻辑）：① `handleChannel` 新增 `case MsgTCPConnect`：反向表首达占用 → 广播 `MsgSelectUplink` → 异步本地拨号（`ConnectTimeout`，`ResolveWithStrategy` 用自身 `-ips`）→ 广播 `MsgConnStatus(OK/ERR)`；② 本地 conn 读泵 → `MsgTCPData`（P2 已知前广播/后单播）；③ `MsgSelectDownlink` 反向分支：校验来自 P1 → 记录 P2；④ `MsgTCPData`/`MsgTCPClose` 反向分支：P1 校验/关闭清理；⑤ `cleanupChannel` 清理该通道反向连接；⑥ 通道就绪时发送 `MsgReverseListen`（各 listener 一条）；⑦ 处理 `MsgReverseListenResult`（OK 静默/ERR 告警，超时未回告警"服务端不支持"） |
| 测试 | `reverse_client_test.go`：拨号成功/失败、竞争选路、P1/P2 校验丢弃、通道断开清理、`EnableReverse=false` 忽略 `MsgTCPConnect`、注册幂等 |

### 6.2 CLI 服务端 `server/pkg`

| 文件 | 改动 |
|---|---|
| `reverse.go`（新） | ① `ReverseListenerManager`：`(clientID, 监听值) → listener` 注册表；bind 用 `xshared/socks5`、`xshared/httpproxy`（协议/鉴权/max-conns 由监听值决定）；结果回 `MsgReverseListenResult`；每客户端监听器数上限（`-max-reverse-listeners`，默认 3）；② `reverseDialer` 实现 `dialer.Dialer`（镜像 `clientPool` 请求方逻辑）：登记反向表 → 取 Pair 或广播 `MsgTCPConnect` → 竞争 `MsgSelectUplink` 得 P2 → 等 `MsgConnStatus`（带超时）→ 经 P1 发 `MsgSelectDownlink` → 返回内存管道 conn；上行泵管道读 → `MsgTCPData`；③ 反向连接表 + P1/P2 收包校验 |
| `reverse_pair_warmer.go`（新，可选） | 服务端预热器（常驻）：为已授权（`MsgReverseHotPair`）且已注册监听器的客户端维护 `{P1,P2}` 预热 Pair，复用 `MsgPrebindRequest`；镜像 `InvalidateChannel`/刷新/兑底逻辑 |
| `pool.go` 小改 | `handleMessage`：`MsgReverseListen`/`MsgSelectUplink`（反向分支）/`MsgConnStatus`（反向分支）分发；`MsgTCPData`/`MsgTCPClose` 反向分支；`cleanupChannel`：清反向连接 + 客户端最后通道断开时注销其监听器 |
| `server.go` | Manager 随 Server 启停 |
| `cmd/main.go` | 新 flag：`-max-reverse-listeners`（默认 3）。无预热相关配置——是否预热完全由客户端 `-hotpair` 决定 |
| 测试 | `reverse_test.go`：注册/幂等/端口冲突 ERR、最后通道断开注销、竞争时序、多客户端端口隔离（A:30000/B:30001 各自出流） |

### 6.3 CLI 客户端 `client/cmd`

| 文件 | 改动 |
|---|---|
| `main.go` | + `-r/--reverse`（bool）。开启时：`-l` 值不再启动本地监听，改为解析后填入 `cfg.ReverseListeners`；`-l` 仍为必填（它定义反向监听）。解析逻辑复用现有 `parseListenAddrs`。注册结果处理：ERR 打日志，**启动首轮全部 ERR → fatal 退出**；运行中重注册仅记日志 |
| `config_file.go` | JSON 配置 + `reverse` 布尔字段 |

## 7. 私网过滤（未来规划，v1 不实现）

> 评审决策：v1 不做任何目的地过滤；本节保留作为后续规划输入。

### 威胁模型

反向模式下，**拨号目标由服务端侧监听端口的使用者决定**，客户端在本机网络里执行拨号。任何能连上服务端该端口的人（服务端未做鉴权时甚至可以是任意路人）都可以让客户端去连**客户端所在网络**的任意 `host:port`：

| 攻击样例 | 后果 |
|---|---|
| `curl --socks5 server:30000 http://192.168.1.1` | 客户端家中路由器管理页 |
| `... http://127.0.0.1:6379`（未授权 Redis） | 读写客户端本机服务 |
| `... http://169.254.169.254/latest/meta-data/iam/...` | 客户端若为云主机 → 窃取云凭据 |
| `... http://10.0.0.x/nas` | 客户端局域网 NAS/打印机/IoT |

对称性说明：正向模式同样存在（客户端用户可借隧道触达服务端内网），并非新类别风险，只是方向反了——**现在是客户端的内网暴露给服务端侧的使用者**。客户端主人与服务端主人通常是同一人（自用）时无增量风险；二者不同时才构成真实威胁。

### 功能形态

客户端侧开关（拨号动作发生在客户端，过滤只能做在客户端）：

- flag：`-reverse-block-private`（命名可再议）。
- 行为：收到反向拨号请求后、本地 `net.Dial` 之前，解析目标（按自身 `-ips` 策略）；若解析结果命中**环回(127/8、::1)、RFC1918(10/8、172.16/12、192.168/16)、CGNAT(100.64/10)、链路本地(169.254/16、fe80::/10)** → 拒绝拨号，回 `MsgConnStatus(ERR)`，并打一条警告日志（含目标与命中类别）。
- 防 DNS rebinding：解析后**用解析出的 IP 建连**（而非把域名再交给系统解析一次），避免"检查时公网 IP、连接时被 rebind 成内网 IP"的 TOCTOU。
- 未开启时行为与现在完全一致（任意目标可达）。

### 规划形态（届时再定默认值）

- 客户端侧开关（如 `-reverse-block-private`），拨号前按自身 `-ips` 解析，命中环回/RFC1918/CGNAT/链路本地 → 回 `MsgConnStatus(ERR)` + 警告日志；用解析出的 IP 建连防 DNS rebinding。
- 两个候选默认：默认关闭+开关（与正向信任模型对称，不误伤内网穿透合法用途）／默认开启+放行开关（更保守但开箱即坏）。

## 8. 兼容性矩阵

| 组合 | 行为 |
|---|---|
| 新客户端 + 新服务端 | 正常工作 |
| 新客户端 + 老服务端 | `MsgReverseListen` 被忽略，客户端超时告警"服务端不支持反向模式"，正向功能不受影响 |
| 老客户端 + 新服务端 | 老客户端不发注册消息 → 无监听产生，服务端零影响 |
| 反向收到未知消息（各组合错发） | 两端 switch 无 case 静默忽略，无副作用 |

## 9. 评审决策记录（已定稿）

1. 私网过滤：v1 不实现，写入未来规划（§7）。
2. 绑定失败：多 `-l` 值逐个注册，失败打日志继续其余；**启动首轮全部失败 → fatal**；运行中重注册仅记日志。
3. 每客户端反向监听器上限：默认 **3**，flag `-max-reverse-listeners`。

## 10. 工作量与发布

- `xtunnel` 库 ~400 行 + 测试；CLI 服务端 ~500 行 + 测试（含可选 ReversePairWarmer 另计 ~300 行）；CLI 客户端 ~50 行。
- 顺序：protocol 常量 + 库客户端拨号方半边 + 单测 → CLI 服务端请求方半边 + Manager + 单测 → 本地 E2E（A 客户端 30000 / B 客户端 30001，`curl --socks5` 验证分别从 A/B 出口；占用 30000 再注册验证 ERR；客户端全部 -l 值被占验证 fatal；拔通道验证清理与监听器注销）→ 发版。
- 发布链：`xtunnel` 打 `v0.2.0` → CLI `go.mod` 升引用 → CLI release；本地联调用 `replace`。
