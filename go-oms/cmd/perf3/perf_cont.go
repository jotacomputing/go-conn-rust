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
		panic(fmt.Sprintf("Failed to open queue: %v", err))
	}
	defer q.Close()

	fmt.Println("[OMS] Go Producer - FINAL FIX")
	fmt.Println("[OMS] Using concentrated price levels")

	var atomicCount atomic.Int64

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

	count := int64(0)
	//basePrice := uint64(50000)
	
	// ✅ CRITICAL: Use only 3 price levels
	prices := []uint64{49999, 50000, 50001}

	var order queue.Order

	for {
		count++
		side := uint8(count % 2)
		
		// ✅ Rotate through 3 prices (33% chance of match at each level)
		priceIdx := int(count / 2 % 3)
		price := prices[priceIdx]

		quantity := uint32(100)

		order.OrderID = uint64(count)
		order.ClientID = 1001
		order.Quantity = quantity
		order.Price = price
		order.Side = side
		order.Status = 0
		order.Symbol = 0
		order.Timestamp = uint64(time.Now().UnixNano())

		for {
			if err := q.Enqueue(order); err == nil {
				atomicCount.Add(1)
				break
			}
			runtime.Gosched()
		}
	}
}
