package stream

import (
	"reflect"
	"testing"
)

func newTestOperator() *WindowOperator {
	return NewWindowOperator("win", NewWindowAssigner(100), NewWatermarkGenerator(50))
}

// 乱序事件应被正确归入各自窗口，水位线单调推进。
func TestOperatorOutOfOrder(t *testing.T) {
	op := newTestOperator()
	input := []Record{
		{Key: "a", Value: 1, EventTime: 100},
		{Key: "a", Value: 2, EventTime: 150},
		{Key: "b", Value: 5, EventTime: 120},
		{Key: "a", Value: 4, EventTime: 110}, // 乱序但水位线(100)未越过窗口，仍应计入
		{Key: "a", Value: 3, EventTime: 260}, // 推进水位线到 210，触发窗口 [100,200)
	}
	var got []WindowResult
	for i, r := range input {
		r.Offset = int64(i)
		got = append(got, op.Process(r)...)
		t.Logf("[test] offset=%d ts=%d watermark=%d emitted=%d", r.Offset, r.EventTime, op.Watermark(), len(got))
	}
	want := []WindowResult{
		{Window: Window{Start: 100, End: 200}, Key: "a", Sum: 1 + 2 + 4, Count: 3},
		{Window: Window{Start: 100, End: 200}, Key: "b", Sum: 5, Count: 1},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	if len(op.LateRecords()) != 0 {
		t.Fatalf("unexpected late records: %+v", op.LateRecords())
	}
}

// 水位线之后的迟到数据必须被确定性拒绝并记录原因。
func TestOperatorLateDataRejected(t *testing.T) {
	op := newTestOperator()
	op.Process(Record{Key: "a", Value: 1, EventTime: 500}) // 水位线 -> 450
	out := op.Process(Record{Key: "a", Value: 9, EventTime: 100})
	if len(out) != 0 {
		t.Fatalf("late record should not emit, got %+v", out)
	}
	lates := op.LateRecords()
	if len(lates) != 1 || lates[0].Record.EventTime != 100 || lates[0].Watermark != 450 {
		t.Fatalf("late record not tracked correctly: %+v", lates)
	}
	t.Logf("[test] late record rejected at watermark=%d reason=%q", lates[0].Watermark, lates[0].Reason)
}

// 同一批输入重复执行，窗口结果必须完全一致（不重不漏）。
func TestOperatorDeterministicReplay(t *testing.T) {
	input := []Record{
		{Key: "a", Value: 1, EventTime: 10},
		{Key: "a", Value: 2, EventTime: 30},
		{Key: "b", Value: 7, EventTime: 20},
		{Key: "a", Value: 3, EventTime: 300},
		{Key: "a", Value: 4, EventTime: 15}, // 迟到（水位线已推进到 250）
		{Key: "a", Value: 5, EventTime: 5},  // 迟到（水位线 250）
		{Key: "b", Value: 8, EventTime: 310},
	}
	run := func() ([]WindowResult, []LateRecord) {
		op := newTestOperator()
		var res []WindowResult
		for i, r := range input {
			r.Offset = int64(i)
			res = append(res, op.Process(r)...)
		}
		return res, op.LateRecords()
	}
	r1, l1 := run()
	r2, l2 := run()
	if !reflect.DeepEqual(r1, r2) {
		t.Fatalf("non-deterministic results:\n run1=%+v\n run2=%+v", r1, r2)
	}
	if !reflect.DeepEqual(l1, l2) || len(l1) != 2 {
		t.Fatalf("non-deterministic late records: %+v vs %+v", l1, l2)
	}
	// 窗口 [0,100) 内 a=1+2=3（迟到的 4、5 不计入），b=7。
	want := []WindowResult{
		{Window: Window{Start: 0, End: 100}, Key: "a", Sum: 3, Count: 2},
		{Window: Window{Start: 0, End: 100}, Key: "b", Sum: 7, Count: 1},
	}
	if !reflect.DeepEqual(r1, want) {
		t.Fatalf("got %+v, want %+v", r1, want)
	}
}

// 负事件时间也应落在正确窗口（取模向负无穷取整）。
func TestWindowAssignNegative(t *testing.T) {
	a := NewWindowAssigner(100)
	w := a.Assign(-1)
	if w.Start != -100 || w.End != 0 || !w.Contains(-1) {
		t.Fatalf("bad window for -1: %+v", w)
	}
}
