# stream-runtime-go

本地可运行的实时流处理运行时：用本地数据源与状态存储模拟事件流，
在乱序、迟到、故障恢复与下游变慢等情况下给出一致且可解释的结果。

## 架构

```
MemSource ──> WindowOperator ──(有界通道, 背压)──> Sink (MemSink)
   │                │                                  │
   │                └─ 水位线推进 / 迟到记录            │
   └─ SeekOffset 重放        └─ CheckpointStore (本地文件, 原子写入)
```

- `stream/source.go`：可重放数据源，按位移（offset）消费与重置，`Bounds()` 暴露有效位移范围。
- `stream/watermark.go`：水位线生成器。
- `stream/window.go` / `operator.go`：滚动窗口分配与聚合算子，`Config()` 暴露实际生效的运行配置。
- `stream/config.go`：`RuntimeConfig`（算子名、窗口大小、允许乱序度）与配置指纹。
- `stream/checkpoint.go`：检查点存储与校验。
- `stream/pipeline.go`：流水线编排、背压、屏障同步检查点、下游隔离重试、故障隔离。
- `stream/errors.go`：可区分的错误类别与带上下文的 `OpError`。

## 事件时间与水位线语义

- 每条记录携带事件时间 `EventTime`，窗口按事件时间划分（滚动窗口，
  大小由 `NewWindowAssigner(size)` 指定，负时间戳向负无穷取整）。
- 水位线 = `已见最大事件时间 - 允许乱序度`，单调不减，恢复后不回退。
- 窗口 `End <= 水位线` 时触发输出，输出后状态即清理，**不会重复计算**；
  同一窗口的多个 key 按 (窗口起始, key) 排序输出，顺序确定。
- **迟到数据**：事件时间严格小于当前水位线的记录被确定性拒绝，
  不进入任何窗口，逐条记录到 `LateRecords()`（含记录、当时水位线与原因），
  可审计、可复现。同一批输入重复执行，窗口结果与迟到列表完全一致。

## 检查点兼容规则

检查点内容：格式版本、拓扑指纹、**生效的运行配置**、源位移、水位线、
窗口中间状态、CRC32 校验和。写入采用临时文件 + 重命名，避免半截文件。

### 运行配置（RuntimeConfig）

影响状态语义的参数组成 `RuntimeConfig{OperatorName, WindowSize, AllowedLateness}`，
随检查点一起落盘。调参（改窗口大小、允许乱序时长）后重启，旧检查点会被
明确拒绝，而不是默默接受后一个结果都不出。背压类参数（`MaxInFlight`、
`SendTimeout`、`SinkRetries` 等）不影响状态语义，调整它们**不会**使旧检查点失效。

推荐用 `NewCheckpointStoreWithConfig(dir, cfg, maxAge)` 创建存储：流水线启动前
还会核对存储声明的配置与算子实际生效的配置（`WindowOperator.Config()`），
两者不一致同样拒绝启动——恢复行为以真实生效的参数为准。

### 加载校验顺序与可区分错误

加载时按以下顺序校验，任一失败即**拒绝运行**（不会从中间状态继续），原因可区分：

| 情况 | 错误 | 判定 |
|---|---|---|
| 文件无法解析 / 校验和不匹配 | `ErrCheckpointCorrupt` | `errors.Is` |
| 来自更早状态结构（格式版本过旧） | `ErrCheckpointVersionMismatch` | `errors.Is` |
| 运行配置对不上（窗口大小/乱序度/算子名） | `ErrCheckpointConfigMismatch` | `errors.Is` |
| 拓扑指纹不一致（旧式按字符串创建的存储） | `ErrCheckpointIncompatible` | `errors.Is` |
| 检查点源位移超出当前数据范围 `[min, max]` | `ErrCheckpointOffsetOutOfRange` | `errors.Is` |
| 超过保留时长 `maxAge` | `ErrCheckpointExpired` | `errors.Is` |
| 文件不存在 | 正常冷启动 | 返回 `(nil, nil)` |

**位置越界**：检查点记录的消费位移必须落在当前数据源 `Bounds()` 范围内。
本地数据被裁过、回放区间变窄时，越界即拒绝并给出位移与当前范围，
不会悄悄从最近位置续读、吐出一批空窗口结果。

**恢复一致性**：检查点只覆盖已确认写入汇的结果——保存前先发“屏障”消息，
等汇把此前所有结果落盘后才快照。崩溃后从检查点重放源记录，汇按
`(窗口, key)` 幂等覆盖写，因此重启或重复恢复后输出**不重复、不丢失**。

## 背压、下游重试与故障隔离

- 算子与汇之间是容量为 `MaxInFlight` 的有界通道：下游变慢时通道填满、
  上游阻塞，**不会无上限堆积内存，也不会静默丢数据**。
- 阻塞超过 `SendTimeout` 返回 `ErrBackpressureTimeout`（含算子名与位移上下文）。
- **下游偶发抖动**：`SinkRetries > 0` 时，汇写失败在同一次运行内做有限次
  隔离重试（间隔 `SinkRetryBackoff`）。重试发生在汇协程内，有界通道容量不变，
  背压与内存封顶不被破坏；结果不复制、不重排，汇按 `(窗口, key)` 幂等，
  因此重试**不重复、不丢失**。每次重试都有日志
  （`[sink-retry] attempt=k/n window=... key=... err=...`）。
- 重试额度用完返回 `ErrSinkRetryExhausted`（与普通算子失败 `ErrOperatorFailed`
  可区分），`OpError` 携带算子名、源位移与失败窗口结果定位上下文。
- 汇写失败（未配重试时）、算子 panic 都被隔离并归类为 `ErrOperatorFailed`，
  以 `OpError{Op, Offset, Kind, Msg}` 携带可定位上下文；已提交的检查点不受影响，
  修复后从检查点恢复，结果仍正确。

## 本地验证

```bash
go build ./...        # 编译
go test ./...         # 全量测试（日志含水位线/检查点/记录处理流水）
go test ./stream/ -run TestPipeline -v   # 查看流水线详细日志
go test ./... -count=2                   # 重复执行，验证结论可复现
go test ./... -race                      # 竞态检测
```

测试覆盖：

- `operator_test.go`：乱序归窗、迟到确定性拒绝、重复执行结果一致、负时间戳窗口。
- `checkpoint_test.go`：保存/加载往返、损坏、不兼容、过期可区分；
  配置变化拒绝、旧格式版本拒绝、三类新错误两两可区分。
- `pipeline_test.go`：端到端确定性、下游故障后恢复不重不漏、背压超时、
  慢下游最终完成、损坏/不兼容检查点拒绝运行、算子 panic 隔离；
  调参后重启拒绝恢复、检查点位移越界拒绝恢复、
  下游连续失败后重试成功、重试额度用完可区分定位。
