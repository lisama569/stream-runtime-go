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

- `stream/source.go`：可重放数据源，按位移（offset）消费与重置。
- `stream/watermark.go`：水位线生成器。
- `stream/window.go` / `operator.go`：滚动窗口分配与聚合算子。
- `stream/checkpoint.go`：检查点存储与校验。
- `stream/pipeline.go`：流水线编排、背压、屏障同步检查点、故障隔离。
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

检查点内容：格式版本、配置指纹、源位移、水位线、窗口中间状态、CRC32 校验和。
写入采用临时文件 + 重命名，避免半截文件。

加载时按以下顺序校验，失败即**拒绝运行**（不会从中间状态继续），原因可区分：

| 情况 | 错误 | 判定 |
|---|---|---|
| 文件无法解析 / 校验和不匹配 | `ErrCheckpointCorrupt` | `errors.Is` |
| 格式版本不一致（更早/更新状态结构的老检查点） | `ErrCheckpointLegacyFormat`（同时是 `ErrCheckpointIncompatible`） | `errors.Is` |
| 运行配置不一致（算子名/窗口大小/允许乱序度变化） | `ErrCheckpointConfigMismatch`（同时是 `ErrCheckpointIncompatible`） | `errors.Is` |
| 消费位置落在当前数据范围之外 | `ErrCheckpointOffsetOutOfRange` | `errors.Is` |
| 超过保留时长 `maxAge` | `ErrCheckpointExpired` | `errors.Is` |
| 文件不存在 | 正常冷启动 | 返回 `(nil, nil)` |

**哪些算不兼容**：配置指纹 `TopologyHash(opName, windowSize, allowedLateness)`，
即算子名、窗口大小、允许乱序度任一变化都视为配置不一致；格式版本
（`CheckpointFormatVersion`，状态结构变更时递增）不一致则单独归为旧格式。
调参重启时旧检查点会被明确拒绝，不会默默接受然后一个结果都不出。

**哪些算位置越界**：检查点记录的 `SourceOffset` 落在源当前有效区间
`[min, max]`（由可选接口 `RangedSource.OffsetRange()` 报告，`MemSource`
支持 `TrimBefore` 模拟本地数据被裁）之外——无论是超过数据末尾（回放区间
变窄）还是落在被裁掉的头部之前——都以 `ErrCheckpointOffsetOutOfRange`
拒绝并给出位移与有效区间，**不会**悄悄从最近位置续读、吐出一批空窗口结果。
源未实现 `RangedSource` 时跳过该校验。

**恢复一致性**：检查点只覆盖已确认写入汇的结果——保存前先发“屏障”消息，
等汇把此前所有结果落盘后才快照。崩溃后从检查点重放源记录，汇按
`(窗口, key)` 幂等覆盖写，因此重启或重复恢复后输出**不重复、不丢失**。

## 背压与故障隔离

- 算子与汇之间是容量为 `MaxInFlight` 的有界通道：下游变慢时通道填满、
  上游阻塞，**不会无上限堆积内存，也不会静默丢数据**。
- 阻塞超过 `SendTimeout` 返回 `ErrBackpressureTimeout`（含算子名与位移上下文）。
- **下游抖动重试**：汇写失败时在同一次运行内做有限次数
  （`SinkMaxRetries`，间隔 `SinkRetryBackoff`）的原位重试——重试在写汇
  协程内完成，结果仍占着有界通道的位置，背压与内存上限不被破坏；
  已算出的结果按原顺序逐条重试（汇按窗口+键幂等），不重不漏。
  每次重试都带上下文日志（第几次/总额度、窗口、键、源位移、错误）。
- 重试额度用完才让本次运行失败，错误为 `ErrSinkRetriesExhausted`
  （同时是 `ErrOperatorFailed`，与背压超时等类别可区分），
  `OpError` 中定位到出问题的窗口、键与位移。
- 算子 panic 也被隔离并归类为 `ErrOperatorFailed`，
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
- `checkpoint_test.go`：保存/加载往返、损坏、旧格式、配置不一致、过期
  各类错误可区分。
- `pipeline_test.go`：端到端确定性、下游故障后恢复不重不漏、背压超时、
  慢下游最终完成、损坏/不兼容检查点拒绝运行、算子 panic 隔离、
  调参（窗口大小变化）后拒绝恢复、消费位置越界（尾部超出/头部被裁）拒绝恢复、
  下游连续失败后重试成功且不重不漏、重试额度耗尽后带上下文失败。
