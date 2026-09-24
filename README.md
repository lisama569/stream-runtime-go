# stream-runtime-go

一个**本地可运行**的实时流处理运行时：用本地数据源与本地文件状态存储模拟事件流，
重点解决乱序事件在 **窗口计算、故障恢复、下游变慢** 三类情况下结果的
**一致性与可解释性**。无外部依赖（不依赖 Kafka/Flink/数据库），`go test` 即可复现。

## 目录结构

| 路径 | 职责 |
| --- | --- |
| `pkg/stream/types.go` | `Event` / `Watermark` / `WindowResult` / `LateRecord` 等核心类型 |
| `pkg/stream/source.go` | 确定性本地数据源，按下标回放，偏移量参与检查点 |
| `pkg/stream/watermark.go` | 单调水位线生成器 `wm = maxEventTime - outOfOrderness` |
| `pkg/stream/window.go` | 按 key 的滚动事件时间窗口聚合、水位线触发、迟到语义 |
| `pkg/stream/sink.go` | 有界队列下游：背压、故障注入与隔离、幂等去重 |
| `pkg/stream/checkpoint.go` | 检查点原子写 + CRC32 校验 + 版本/拓扑兼容校验 |
| `pkg/stream/pipeline.go` | 串联 Source→Watermark→Window→Sink，检查点屏障与恢复 |
| `pkg/stream/errors.go` | 可区分的错误类别（哨兵错误 + 带上下文的错误类型） |
| `pkg/stream/logger.go` | 水位线/检查点/记录处理流水日志（写文件 + 内存缓冲，供测试断言） |
| `cmd/demo/main.go` | 本地命令行演示：乱序、迟到、检查点恢复、下游故障 |

## 1. 事件时间与水位线语义

- 每条记录带**事件时间** `EventTime`（业务发生时刻），与处理时刻解耦。
- 水位线（watermark）是一条随观察到的最大事件时间推进的时间戳：

  ```
  watermark = maxObservedEventTime - maxOutOfOrderness
  ```

  含义为「事件时间 `<= watermark` 的记录理论上都已到达」。水位线**全局单调不减**。
- 窗口为按 key 分组的滚动窗口 `[start, start+size)`。窗口在
  **水位线推进到 `wm >= start+size` 时恰好触发一次**（on-watermark timer）。
  触发顺序按 `(windowStart, key)` 稳定排序，因此同一批输入的输出完全确定。
- **迟到数据的确定语义**（稳定、可重放，不重复聚合、不漏算存活窗口）：
  - 记录到达时窗口**尚未触发**（`wm < windowEnd`）：正常聚合。即使其事件时间
    已落后当前水位线（日志标 `behind-watermark-accepted`），只要窗口还存活就计入——
    这正是乱序容忍度覆盖的范围。
  - 窗口**已触发后**到达：判定为迟到，`reason=window-fired`，**稳定丢弃**并记入
    `LateRecords`（日志 `LATE-DROP`）。绝不二次聚合，也不会再次触发窗口。
  - 乱序容忍度越大，越能吸收迟到，但窗口结果产出越晚；这是显式的权衡。
- 流结束时以 `maxSeenEventTime+1` 的水位线强制闭合所有存活窗口，保证确定性收尾。

## 2. 检查点与兼容规则

检查点是一份一致性快照，原子落盘到 `CheckpointDir/checkpoint.json`：

```
sourceOffset          已处理到的源偏移（恢复后从这里重放，at-least-once 读取）
wmMaxSeen/wmCurrent   水位线状态
windowState           每 key 每窗口的聚合值 + 已触发窗口集合 + 迟到记录
sinkState             已确认输出的去重集合 + 在途队列
version / topologyId / createdAtMs / checksum(CRC32)
```

- **原子写**：写临时文件 → `fsync` → `rename`，任何时刻读到的要么是上一个完整
  检查点，要么是新的完整检查点，不会读到半成品。
- **恢复加载按固定顺序校验，任一不过即明确拒绝，绝不从中间状态继续运行**：

  | 情况 | 返回错误（`errors.Is` 可区分） |
  | --- | --- |
  | 文件不存在（首次启动） | `ErrCheckpointMissing`（视为正常全新启动） |
  | JSON 损坏 / 截断 | `ErrCheckpointCorrupt` |
  | CRC32 校验和不符（被截断/篡改） | `ErrCheckpointCorrupt` |
  | 检查点外层格式版本不同 | `ErrCheckpointIncompat` |
  | 窗口内部状态版本或窗口大小不同 | `ErrCheckpointIncompat` |
  | 拓扑指纹 `topologyId` 不同（如改了窗口大小/乱序边界） | `ErrCheckpointIncompat` |
  | 年龄超过配置的 TTL | `ErrCheckpointExpired` |

- **重启 / 重复恢复不重不漏（effectively-once 输出）**：
  - 源按 `sourceOffset` 重放（at-least-once）；
  - 窗口持久化「已触发窗口集合」，恢复后同一窗口不会二次触发；
  - 下游按 `(key, windowStart)` 幂等去重，重放产生的重复投递被跳过；
  - 检查点**只在下游把在途结果全部确认后**才提交（屏障 `WaitUntilIdle`），
    因此快照里不会出现「窗口触发了但结果丢在半路」的情况。

## 3. 背压与故障隔离策略

数据链路为 `Run` 主循环 + 一个后台 drainer：

- **背压（下游变慢）**：下游内部是**容量上限为 `SinkCapacity` 的在途队列**。
  队列满时 `Offer` **阻塞**等待 drainer 腾出空间，背压沿链路自然传导：
  - 绝不静默丢数据；
  - 内存占用始终 `<= SinkCapacity`，不会无上限堆积；
  - 等待超过 `OfferTimeout` 归类为可区分的 `ErrBackpressure`；
  - 等待下游排空超过 `IdleTimeout`（检查点屏障）同样归类为 `ErrBackpressure`。
- **单算子失败与故障隔离**：`SetFault(every, max)` 可注入「每处理 N 条一次」的
  瞬时下游失败。失败以 `*SinkError`（`errors.Is(err, ErrOperatorFailed)`）返回，
  携带可定位上下文（算子/窗口主键）：
  - 失败被隔离在**队首单条记录**，该记录保留不丢，drainer 自动重试直到成功；
  - 其它记录与主循环不受影响，恢复后最终结果仍完整正确；
  - 超过 `MaxRetries` 才升级为致命错误并停止，避免无限重试。

## 4. 本地验证方法

```bash
# 编译 + 静态检查
go build ./... && go vet ./...

# 全部单测（含水位线/检查点/记录处理流水，用 -v 查看 t.Log 输出）
go test -v ./...

# 竞态检测 + 重复运行验证稳定性
go test -race -count=2 ./...

# 演示
go run ./cmd/demo -fresh                      # 乱序 + 迟到 + 检查点
go run ./cmd/demo -dir ./.runtime-demo       # 复用检查点重启：输出不重复
go run ./cmd/demo -fresh -fault              # 注入下游瞬时故障：隔离 + 重试
```

参数：`-dir` 检查点目录、`-fresh` 清空后全新启动、`-fault` 注入下游故障。

### 测试覆盖与可复现结论

| 测试 | 覆盖场景 | 可区分结论 |
| --- | --- | --- |
| `TestWindowOutOfOrderAndLate` / `TestWindowFireExactlyOnce` | 乱序计入、触发后迟到稳定丢弃、窗口只触发一次 | `window-fired` 迟到 vs 正常聚合 |
| `TestWatermarkMonotonic` | 水位线单调、确定性推进 | — |
| `TestCheckpoint{RoundTrip,Expired,Corrupt,Incompatible,AtomicSave}` | 过期/损坏/版本与拓扑不兼容分别拒绝 | `ErrCheckpointExpired/Corrupt/Incompat/Missing` |
| `TestPipelineCrashRecovery` | 处理到一半崩溃 → 恢复 → 重复恢复，不重不漏 | 与一次跑全量结果逐值相等 |
| `TestPipelineDeterministicReplay` | 同批输入跑两遍 | 窗口结果与迟到集合完全一致 |
| `TestSinkBackpressure` / `TestPipelineBackpressureEndToEnd` | 队列满阻塞、超时、不丢数据不超容量 | `ErrBackpressure` |
| `TestSinkFaultIsolation` / `TestPipelineFaultInjection` | 单条失败隔离、重试成功、结果完整 | `*SinkError`/`ErrOperatorFailed` + 定位上下文 |
| `TestSinkDedup` / `TestSinkSnapshotRestore` | 重复投递幂等、恢复后不重复确认 | 输出条数稳定 |

测试通过 `TextLogger` 打印水位线推进（`WATERMARK ->`）、检查点
（`CHECKPOINT-SAVED` / `RECOVERED`）、每条记录流水（`REC-IN` / `LATE-DROP` /
`WINDOW-FIRE` / `SINK-OUT` / `BACKPRESSURE-WAIT`），用 `go test -v` 可见。
