package main

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"time"

	"oms/queue"
)

func main() {
	// Lock to OS thread for consistent performance
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// Open SHM queue
	q, err := queue.OpenQueue("/tmp/sex")
	if err != nil {
		panic(fmt.Sprintf("Failed to open queue: %v", err))
	}
	defer q.Close()

	fmt.Println("[OMS] Go Producer - High Performance Mode")
	fmt.Println("[OMS] Running forever (Press Ctrl+C to stop)")

	var atomicCount atomic.Int64

	// Stats goroutine (runs in background)
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		var lastCount int64
		lastTime := time.Now()

		for range ticker.C {
			now := time.Now()
			elapsed := now.Sub(lastTime).Seconds()
			current := atomicCount.Load()

			ops := float64(current - lastCount)
			throughput := ops / elapsed

			fmt.Printf("[OMS] %.0f orders/sec (%.2f million/sec)\n",
				throughput, throughput/1e6)

			lastCount = current
			lastTime = now
		}
	}()

	// Main producer loop
	count := int64(0)
	basePrice := uint64(50000)

	// Pre-allocate order to avoid allocations in hot loop
	var order queue.Order

	for {
		count++

		// ✅ CRITICAL: Alternate sides for matching
		side := uint8(count % 2) // 0 = BID, 1 = ASK

		// Update order fields
		order.OrderID = uint64(count)
		order.ClientID = 1001
		order.Quantity = 100
		order.Price = basePrice
		order.Side = side // ✅ Alternates every order
		order.Status = 0
		order.Symbol = 0
		order.Timestamp = uint64(time.Now().UnixNano())

		// Enqueue with retry (non-blocking)
		for {
			if err := q.Enqueue(order); err == nil {
				atomicCount.Add(1)
				break
			}
			// Queue full, yield CPU briefly
			runtime.Gosched()
		}
	}
}
