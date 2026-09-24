package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/lisama569/stream-runtime-go/pkg/stream"
)

// demoEvents 是一批故意乱序、并夹带迟到记录的输入（同一批重复执行结果必须一致）。
// 窗口大小 1000ms，乱序容忍 500ms。
func demoEvents() []stream.Event {
	return []stream.Event{
		{Key: "a", EventTime: 100, Value: 1},
		{Key: "a", EventTime: 200, Value: 2},
		{Key: "b", EventTime: 150, Value: 10},
		{Key: "a", EventTime: 600, Value: 4}, // 乱序但仍在容忍度内
		{Key: "a", EventTime: 400, Value: 8}, // 乱序：事件时间更老，窗口未触发，照常计入
		{Key: "b", EventTime: 900, Value: 20},
		{Key: "a", EventTime: 1200, Value: 16}, // 推动水位线越过 1000，触发窗口 [0,1000)
		{Key: "a", EventTime: 300, Value: 32},  // 迟到：窗口已触发 → 稳定丢弃
		{Key: "b", EventTime: 800, Value: 40},  // 迟到：窗口已触发 → 稳定丢弃
		{Key: "a", EventTime: 1500, Value: 64},
		{Key: "a", EventTime: 2100, Value: 128}, // 记录进入窗口 [2000,3000)
		{Key: "a", EventTime: 2600, Value: 512}, // 推动水位线到 2100：触发窗口 [1000,2000)
		{Key: "a", EventTime: 1300, Value: 256}, // 迟到：窗口 [1000,2000) 已触发 → 稳定丢弃
	}
}

func main() {
	dir := flag.String("dir", "./.runtime-demo", "检查点目录")
	fresh := flag.Bool("fresh", false, "忽略/清除已有检查点，全新启动")
	fault := flag.Bool("fault", false, "注入下游瞬时故障")
	flag.Parse()

	log := stream.NewTextLogger(os.Stdout)
	if *fresh {
		_ = os.RemoveAll(*dir)
	}

	cfg := stream.PipelineConfig{
		Source:          stream.NewLocalSource(demoEvents()),
		WatermarkBound:  500,
		WindowSizeMs:    1000,
		SinkCapacity:    2,
		SinkDelay:       5 * time.Millisecond,
		CheckpointDir:   *dir,
		CheckpointTTL:   0,
		OfferTimeout:    5 * time.Second,
		IdleTimeout:     10 * time.Second,
		CheckpointEvery: 1,
		Logger:          log,
	}
	p, err := stream.NewPipeline(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "build pipeline:", err)
		os.Exit(1)
	}
	if *fault {
		p.SetFault(3, 2) // 每处理约 3 条注入一次瞬时失败，最多 2 次
	}

	ctx := context.Background()
	if _, err := p.Run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "run pipeline:", err)
		os.Exit(1)
	}

	fmt.Println("\n==== 最终窗口输出（稳定顺序）====")
	for _, r := range p.Outputs() {
		fmt.Printf("key=%s window=[%d,%d) sum=%d count=%d\n",
			r.Key, r.WindowID, r.WindowEnd, r.Sum, r.Count)
	}
	if late := p.LateRecords(); len(late) > 0 {
		fmt.Println("\n==== 迟到记录（稳定丢弃、可解释）====")
		for _, l := range late {
			fmt.Printf("key=%s eventTime=%d value=%d wm=%d reason=%s\n",
				l.Event.Key, l.Event.EventTime, l.Event.Value, l.Watermark, l.Reason)
		}
	}
	if f := p.Failures(); len(f) > 0 {
		fmt.Printf("\n==== 被隔离并恢复的下游失败：%d 次 ====\n", len(f))
	}
}
