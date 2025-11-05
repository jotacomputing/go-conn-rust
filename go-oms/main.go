package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"oms/queue"
)

const queueFilePath = "/tmp/sex"

func main() {
	// Parse command line args for different test scenarios
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "init":
			testInit()
		case "single":
			testSingleOrder()
		case "batch":
			testBatch()
		case "stream":
			testContinuousStream()
		case "monitor":
			testMonitor()
		default:
			printUsage()
		}
	} else {
		printUsage()
	}
}

func printUsage() {
	fmt.Println(`
Usage: go run main.go [command]

Commands:
  init       - Initialize queue with validation
  single     - Send a single test order
  batch      - Send 10,000 orders in rapid succession
  stream     - Continuously stream orders (press Ctrl+C to stop)
  monitor    - Monitor queue depth in real-time (requires queue already open)`)
}

// testInit initializes the queue and validates structure
func testInit() {
	fmt.Println("[TEST] Initializing shared memory queue...")

	q, err := queue.CreateQueue(queueFilePath)
	if err != nil {
		log.Fatalf("Failed to create queue: %v", err)
	}
	defer q.Close()

	fmt.Printf("[TEST] Queue initialized successfully\n")
	fmt.Printf("[TEST] Capacity: %d orders\n", q.Capacity())
	fmt.Printf("[TEST] Queue depth: %d\n", q.Depth())
	fmt.Printf("[TEST] File: %s (size: ~3.2 MB)\n", queueFilePath)

	// Create status queue too
	fmt.Println("\n[TEST] Initializing status feedback queue...")
	statusQ, err := queue.CreateQueue(queueFilePath + "_status")
	if err != nil {
		log.Fatalf("Failed to create status queue: %v", err)
	}
	defer statusQ.Close()

	fmt.Printf("[TEST] Status queue initialized successfully\n")
	fmt.Printf("[TEST] File: %s_status (size: ~3.2 MB)\n", queueFilePath)
}

// testSingleOrder sends a single test order
func testSingleOrder() {
	q, err := queue.OpenQueue(queueFilePath)
	if err != nil {
		log.Fatalf("Failed to open queue: %v", err)
	}
	defer q.Close()

	order := queue.Order{
		OrderID:   1,
		ClientID:  1001,
		Symbol:    [8]byte{'K', 'O', 'H', 'L', 'I', 0, 0, 0},
		Quantity:  100,
		Price:     50000,
		Side:      0, // buy
		Timestamp: uint64(time.Now().UnixNano()),
		Status:    0, // pending
	}

	if err := q.Enqueue(order); err != nil {
		log.Fatalf("Failed to enqueue: %v", err)
	}

	fmt.Printf("[TEST] Single order sent successfully\n")
	fmt.Printf("       OrderID: %d\n", order.OrderID)
	fmt.Printf("       Symbol: %s\n", string(order.Symbol[:]))
	fmt.Printf("       Qty: %d @ %d\n", order.Quantity, order.Price)
	fmt.Printf("       Queue depth: %d\n", q.Depth())
}

// testBatch sends 10,000 orders rapidly
func testBatch() {
	fmt.Println("[TEST] Sending batch of 10,000 orders...")

	q, err := queue.OpenQueue(queueFilePath)
	if err != nil {
		log.Fatalf("Failed to open queue: %v", err)
	}
	defer q.Close()

	symbols := []string{"KOHLI", "ROHIT", "DHONI"}
	sides := []uint8{0, 1} // buy, sell
	clients := []uint32{1001, 1002, 1003}

	startTime := time.Now()
	successCount := 0
	backpressureCount := 0

	for i := 1; i <= 100000; i++ {
		order := queue.Order{
			OrderID:   uint64(i),
			ClientID:  clients[i%len(clients)],
			Quantity:  uint32(100 + (i % 900)),
			Price:     uint64(50000 + (i % 5000)),
			Side:      sides[i%2],
			Timestamp: uint64(time.Now().UnixNano()),
			Status:    0,
		}

		// Copy symbol
		symbol := symbols[i%len(symbols)]
		for j := 0; j < len(symbol) && j < 8; j++ {
			order.Symbol[j] = symbol[j]
		}

		// Try enqueue with retries on backpressure
		retries := 0
		maxRetries := 3
		for {
			if err := q.Enqueue(order); err == nil {
				successCount++
				break
			} else if retries < maxRetries {
				backpressureCount++
				retries++
				time.Sleep(time.Duration(1<<uint(retries)) * time.Millisecond)
			} else {
				log.Printf("Failed to enqueue order %d after retries", i)
				break
			}
		}

		// Progress indicator
		if i%1000 == 0 {
			elapsed := time.Since(startTime).Seconds()
			throughput := float64(i) / elapsed
			fmt.Printf("[TEST] Progress: %d/%d orders (%.0f orders/sec), depth: %d\n",
				i, 100000, throughput, q.Depth())
		}
	}

	elapsed := time.Since(startTime).Seconds()
	throughput := float64(successCount) / elapsed

	fmt.Printf("\n[TEST] Batch complete\n")
	fmt.Printf("       Sent: %d orders\n", successCount)
	fmt.Printf("       Backpressure events: %d\n", backpressureCount)
	fmt.Printf("       Time: %.2fs\n", elapsed)
	fmt.Printf("       Throughput: %.0f orders/sec\n", throughput)
	fmt.Printf("       Queue depth: %d\n", q.Depth())
}

// testContinuousStream continuously generates orders
func testContinuousStream() {
	fmt.Println("[TEST] Starting continuous order stream (Ctrl+C to stop)...")

	q, err := queue.OpenQueue(queueFilePath)
	if err != nil {
		log.Fatalf("Failed to open queue: %v", err)
	}
	defer q.Close()

	symbols := []string{"KOHLI", "ROHIT", "DHONI", "SMITH", "WARNER"}
	sides := []uint8{0, 1}
	clients := []uint32{1001, 1002, 1003, 1004, 1005}

	orderID := uint64(1)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	statsTicket := time.NewTicker(5 * time.Second) // Stats every 5 seconds
	defer statsTicket.Stop()

	var totalSent uint64
	startTime := time.Now()

	for {
		select {
		case <-ticker.C:
			order := queue.Order{
				OrderID:   orderID,
				ClientID:  clients[rand.Intn(len(clients))],
				Quantity:  uint32(100 + rand.Intn(900)),
				Price:     uint64(50000 + rand.Intn(5000)),
				Side:      sides[rand.Intn(2)],
				Timestamp: uint64(time.Now().UnixNano()),
				Status:    0,
			}

			symbol := symbols[rand.Intn(len(symbols))]
			for j := 0; j < len(symbol) && j < 8; j++ {
				order.Symbol[j] = symbol[j]
			}

			if err := q.Enqueue(order); err != nil {
				fmt.Printf("[TEST] Backpressure: %v (queue depth: %d)\n", err, q.Depth())
				time.Sleep(5 * time.Millisecond)
				continue
			}

			totalSent++
			orderID++

		case <-statsTicket.C:
			elapsed := time.Since(startTime).Seconds()
			throughput := float64(totalSent) / elapsed
			fmt.Printf("[TEST] Stats: %d orders sent, %.0f orders/sec, depth: %d\n",
				totalSent, throughput, q.Depth())
		}
	}
}

// testMonitor continuously monitors queue depth
func testMonitor() {
	fmt.Println("[TEST] Monitoring queue depth (Ctrl+C to stop)...")
	fmt.Println("[TEST] Waiting for queue to be created...")

	// Keep trying to open until it exists
	var q *queue.Queue
	var err error
	for {
		q, err = queue.OpenQueue(queueFilePath)
		if err == nil {
			break
		}
		fmt.Printf("[TEST] Queue not ready, retrying... %v\n", err)
		time.Sleep(1 * time.Second)
	}
	defer q.Close()

	fmt.Println("[TEST] Queue opened, starting monitoring...")

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	maxDepth := uint64(0)

	for range ticker.C {
		depth := q.Depth()
		if depth > maxDepth {
			maxDepth = depth
		}

		capacity := q.Capacity()
		fillPercent := float64(depth) / float64(capacity) * 100

		fmt.Printf("[MONITOR] Depth: %8d / %8d (%.1f%%), Max: %d\n",
			depth, capacity, fillPercent, maxDepth)
	}
}
