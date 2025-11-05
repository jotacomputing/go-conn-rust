use rust_me::queue::{Order, Queue, QueueError};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("[Engine] Starting Rust matching engine (rustc 1.91.0)...");

    // Open order queue created by Go OMS
    let mut order_queue = Queue::open("/tmp/sex")?;
    println!("[Engine] Connected to order queue");

    // Open status feedback queue
    let mut status_queue = Queue::open("/tmp/sex_status")?;
    println!("[Engine] Connected to status queue");

    println!("[Engine] Waiting for orders (spinning)...\n");

    let mut order_count = 0u64;
    let start = Instant::now();

    loop {
        // Try to dequeue with spinning for lower latency
        match order_queue.dequeue_spin(100)? {
            Some(order) => {
                order_count += 1;

                // Execute trade (simplified)
                let executed = execute_order(&order);

                // Send status back to Go OMS
                let mut status = order;
                status.status = if executed { 1 } else { 2 };

                // Handle backpressure on status queue
                if let Err(QueueError::QueueFull { depth }) = status_queue.enqueue(status) {
                    eprintln!(
                        "[Engine] Status queue backpressure (depth {}), dropping status for order {}",
                        depth, order.order_id
                    );
                }

                // Report throughput every 1000 orders
                if order_count % 1000 == 0 {
                    let elapsed = start.elapsed().as_secs_f64();
                    let throughput = order_count as f64 / elapsed;
                    println!(
                        "[Engine] Processed {} orders in {:.2}s ({:.0} orders/sec), queue depth: {}",
                        order_count,
                        elapsed,
                        throughput,
                        order_queue.depth()
                    );
                }
            }
            None => {
                // Queue empty after spinning
                std::thread::yield_now();
            }
        }
    }
}

/// Simulate order execution (matching engine logic goes here)
fn execute_order(order: &Order) -> bool {
    // Validate order
    if order.quantity == 0 || order.price == 0 {
        return false; // Reject invalid
    }

    // In production: check order book, execute match, update positions
    // For now: accept 90% of orders
    order.order_id % 10 != 0
}
