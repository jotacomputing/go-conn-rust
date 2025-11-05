package main

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"time"

	"oms/queue"
)

func main() {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	q, err := queue.OpenQueue("/tmp/sex")
	if err != nil {
		panic(err)
	}
	defer q.Close()

	fmt.Println("[OMS] Go Producer")
	fmt.Println("[OMS] Running forever (Press Ctrl+C to stop)")

	var atomicCount int64

	order := queue.Order{
		ClientID: 1001,
		Quantity: 100,
		Price:    50000,
		Side:     0,
		Status:   0,
		Symbol:   [8]byte{'K', 'O', 'H', 'L', 'I', 0, 0, 0},
	}

	// Stats goroutine
	go func() {
		var lastCount int64
		lastTime := time.Now()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			now := time.Now()
			elapsed := now.Sub(lastTime).Seconds()
			current := atomic.LoadInt64(&atomicCount)

			ops := float64(current - lastCount)
			throughput := ops / elapsed

			fmt.Printf("[OMS] %.0f orders/sec (%.2f million/sec)\n", throughput, throughput/1e6)

			lastCount = current
			lastTime = now
		}
	}()

	// INFINITE LOOP
	count := int64(0)
	var ts uint64 = uint64(time.Now().UnixNano())
	updateTick := int64(0)

	for {
		if updateTick%10000 == 0 {
			ts = uint64(time.Now().UnixNano())
		}

		count++
		order.OrderID = uint64(count)
		order.Timestamp = ts

		for {
			if err := q.Enqueue(order); err == nil {
				atomic.AddInt64(&atomicCount, 1)
				break
			}
		}

		updateTick++
	}
}
