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

检查点内容：格式版本、拓扑指纹、源位移、水位线、窗口中间状态、CRC32 校验和。
写入采用临时文件 + 重命名，避免半截文件。

加载时按以下顺序校验，失败即**拒绝运行**（不会从中间状态继续），原因可区分：

| 情况 | 错误 | 判定 |
|---|---|---|
| 文件无法解析 / 校验和不匹配 | `ErrCheckpointCorrupt` | `errors.Is` |
| 格式版本或拓扑指纹不一致 | `ErrCheckpointIncompatible` | `errors.Is` |
| 超过保留时长 `maxAge` | `ErrCheckpointExpired` | `errors.Is` |
| 文件不存在 | 正常冷启动 | 返回 `(nil, nil)` |

拓扑指纹 `TopologyHash(opName, windowSize, allowedLateness)`：算子名、窗口大小、
允许乱序度任一变化都视为不兼容。

**恢复一致性**：检查点只覆盖已确认写入汇的结果——保存前先发“屏障”消息，
等汇把此前所有结果落盘后才快照。崩溃后从检查点重放源记录，汇按
`(窗口, key)` 幂等覆盖写，因此重启或重复恢复后输出**不重复、不丢失**。

## 背压与故障隔离

- 算子与汇之间是容量为 `MaxInFlight` 的有界通道：下游变慢时通道填满、
  上游阻塞，**不会无上限堆积内存，也不会静默丢数据**。
- 阻塞超过 `SendTimeout` 返回 `ErrBackpressureTimeout`（含算子名与位移上下文）。
- 汇写失败、算子 panic 都被隔离并归类为 `ErrOperatorFailed`，
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
- `checkpoint_test.go`：保存/加载往返、损坏、不兼容、过期三类错误可区分。
- `pipeline_test.go`：端到端确定性、下游故障后恢复不重不漏、背压超时、
  慢下游最终完成、损坏/不兼容检查点拒绝运行、算子 panic 隔离。
