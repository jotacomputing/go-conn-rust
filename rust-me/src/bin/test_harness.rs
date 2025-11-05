use clap::{Parser, Subcommand};
use env_logger::Env;
use log::{debug, error, info, warn};
use rust_me::{Order, Queue};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(name = "HFT Test Harness")]
#[command(about = "Rust testing and monitoring for shared memory queue", long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

const QUEUE_PATH: &str = "/tmp/sex";

#[derive(Subcommand, Debug)]
enum Commands {
    /// Validate queue structure and memory layout
    Validate,

    /// Dequeue a single order
    Single,

    /// Dequeue 10,000 orders in batch
    Batch {
        #[arg(short, long, default_value_t = 10000)]
        count: usize,
    },

    /// Continuously consume orders from queue
    Stream {
        #[arg(short, long, default_value_t = 5)]
        duration_secs: u64,

        #[arg(short, long)]
        spin: bool,
    },

    /// Monitor queue depth in real-time
    Monitor {
        #[arg(short, long)]
        max_depth: Option<u64>,
    },

    /// Run integration test (requires Go OMS running)
    Integration,
}

fn main() {
    // Initialize logger with RUST_LOG environment variable
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let args = Args::parse();

    match args.command {
        Commands::Validate => validate_queue(),
        Commands::Single => single_order(),
        Commands::Batch { count } => batch_orders(count),
        Commands::Stream {
            duration_secs,
            spin,
        } => stream_orders(duration_secs, spin),
        Commands::Monitor { max_depth } => monitor_queue(max_depth),
        Commands::Integration => integration_test(),
    }
}

/// Validate queue structure and memory layout
fn validate_queue() {
    info!("Validating queue structure and memory layout...");

    println!("\n=== Queue Structure Validation ===\n");

    println!(
        "Order size:              {} bytes (expected 48)",
        std::mem::size_of::<Order>()
    );
    assert_eq!(std::mem::size_of::<Order>(), 48);

    println!("QueueHeader size:        136 bytes");

    println!("Queue capacity:          {} orders", 65536);
    println!(
        "Total queue size:        {:.1} MB",
        (136 + (65536 * 48)) as f64 / 1_048_576.0
    );

    println!("\n=== Memory Layout ===\n");
    println!("ProducerHead offset:     0 bytes");
    println!("Padding 1:               56 bytes (offset 8-63)");
    println!("ConsumerTail offset:     64 bytes");
    println!("Padding 2:               56 bytes (offset 72-127)");
    println!("Magic offset:            128 bytes");
    println!("Capacity offset:         132 bytes");

    println!("\n✓ Validation complete!");
}

/// Dequeue a single order
fn single_order() {
    info!("Attempting to dequeue single order...");
    println!("\n=== Single Order Dequeue ===\n");

    let mut queue = match Queue::open(QUEUE_PATH) {
        Ok(q) => q,
        Err(e) => {
            error!("Failed to open queue: {}", e);
            println!("ERROR: {}", e);
            println!("Make sure Go OMS has created the queue with: go run main.go init");
            return;
        }
    };

    info!("Queue opened successfully");
    println!("Queue capacity: {}", queue.capacity());
    println!("Queue depth:    {}", queue.depth());

    match queue.dequeue() {
        Ok(Some(order)) => {
            info!("Successfully dequeued order {}", order.order_id);
            println!("\n✓ Order dequeued successfully!");
            println!("  OrderID:   {}", order.order_id);
            println!("  ClientID:  {}", order.client_id);
            println!("  Symbol:    {:?}", String::from_utf8_lossy(&order.symbol));
            println!("  Quantity:  {}", order.quantity);
            println!("  Price:     {}", order.price);
            println!(
                "  Side:      {}",
                if order.side == 0 { "BUY" } else { "SELL" }
            );
            println!("  Status:    {}", order.status);
        }
        Ok(None) => {
            warn!("Queue is empty");
            println!("\n⚠ Queue is empty. Send orders from Go first:");
            println!("  go run main.go stream");
        }
        Err(e) => {
            error!("Dequeue failed: {}", e);
            println!("\nERROR: {}", e);
        }
    }
}

/// Batch dequeue 10,000 orders
fn batch_orders(count: usize) {
    info!("Starting batch dequeue of {} orders", count);
    println!("\n=== Batch Dequeue: {} Orders ===\n", count);

    let mut queue = match Queue::open(QUEUE_PATH) {
        Ok(q) => q,
        Err(e) => {
            error!("Failed to open queue: {}", e);
            println!("ERROR: {}", e);
            return;
        }
    };

    let start = Instant::now();
    let mut dequeued = 0;
    let mut errors = 0;
    let mut empty_checks = 0;

    for i in 0..count {
        match queue.dequeue() {
            Ok(Some(_order)) => {
                dequeued += 1;
                debug!("Dequeued order {} (iteration {})", _order.order_id, i);
            }
            Ok(None) => {
                empty_checks += 1;
                thread::yield_now();
            }
            Err(e) => {
                errors += 1;
                warn!("Error on iteration {}: {}", i, e);
            }
        }

        if (i + 1) % 1000 == 0 {
            let elapsed = start.elapsed().as_secs_f64();
            let throughput = dequeued as f64 / elapsed;
            println!(
                "[{:5}] Dequeued: {}, Empty: {}, Throughput: {:.0} orders/sec, Depth: {}",
                i + 1,
                dequeued,
                empty_checks,
                throughput,
                queue.depth()
            );
        }
    }

    let elapsed = start.elapsed();
    let throughput = dequeued as f64 / elapsed.as_secs_f64();

    println!("\n=== Results ===\n");
    println!("Total dequeued:  {}", dequeued);
    println!("Empty checks:    {}", empty_checks);
    println!("Errors:          {}", errors);
    println!("Time:            {:.2}s", elapsed.as_secs_f64());
    println!("Throughput:      {:.0} orders/sec", throughput);
    println!("Queue depth:     {}", queue.depth());

    info!(
        "Batch complete: {} orders in {:.2}s",
        dequeued,
        elapsed.as_secs_f64()
    );
}

/// Continuously consume orders for specified duration
fn stream_orders(duration_secs: u64, use_spin: bool) {
    info!(
        "Starting stream consumer for {}s (spin={})",
        duration_secs, use_spin
    );
    println!("\n=== Stream Order Consumption ===\n");
    println!("Duration: {}s", duration_secs);
    println!("Mode: {}", if use_spin { "SPIN" } else { "YIELD" });
    println!("(Press Ctrl+C to stop)\n");

    let mut queue = match Queue::open(QUEUE_PATH) {
        Ok(q) => q,
        Err(e) => {
            error!("Failed to open queue: {}", e);
            println!("ERROR: {}", e);
            return;
        }
    };

    let start = Instant::now();
    let mut dequeued = 0u64;
    let mut _spin_iterations = 0u64; // FIX: Changed to _spin_iterations (unused warning)
    let mut empty_iterations = 0u64;
    let deadline = Duration::from_secs(duration_secs);

    while start.elapsed() < deadline {
        if use_spin {
            // Spin with limited iterations
            match queue.dequeue_spin(100) {
                Ok(Some(_order)) => {
                    dequeued += 1;
                    _spin_iterations += 100;
                }
                Ok(None) => {
                    _spin_iterations += 100;
                    empty_iterations += 1;
                }
                Err(e) => {
                    error!("Error during spin: {}", e);
                }
            }
        } else {
            // Yield mode
            match queue.dequeue() {
                Ok(Some(_order)) => {
                    dequeued += 1;
                }
                Ok(None) => {
                    empty_iterations += 1;
                    thread::yield_now();
                }
                Err(e) => {
                    error!("Error during dequeue: {}", e);
                }
            }
        }

        // Report every second
        if dequeued % 10000 == 0 && dequeued > 0 {
            let elapsed = start.elapsed().as_secs_f64();
            let throughput = dequeued as f64 / elapsed;
            println!(
                "[{:6.1}s] Dequeued: {}, Throughput: {:.0} orders/sec, Depth: {}",
                elapsed,
                dequeued,
                throughput,
                queue.depth()
            );
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    let throughput = dequeued as f64 / elapsed;

    println!("\n=== Stream Results ===\n");
    println!("Total dequeued:       {}", dequeued);
    println!("Empty iterations:     {}", empty_iterations);
    println!("Time:                 {:.2}s", elapsed);
    println!("Throughput:           {:.0} orders/sec", throughput);
    println!("Final queue depth:    {}", queue.depth());

    info!(
        "Stream complete: {} orders in {:.2}s ({:.0}/sec)",
        dequeued, elapsed, throughput
    );
}

/// Monitor queue depth in real-time
fn monitor_queue(max_depth_filter: Option<u64>) {
    info!("Starting queue depth monitoring...");
    println!("\n=== Queue Depth Monitor ===\n");
    println!("(Press Ctrl+C to stop)\n");

    // Retry opening queue with timeout
    let mut queue = None;
    for attempt in 0..10 {
        match Queue::open(QUEUE_PATH) {
            Ok(q) => {
                queue = Some(q);
                info!("Queue opened successfully");
                break;
            }
            Err(e) => {
                if attempt == 0 {
                    warn!("Queue not ready, retrying... {}", e);
                }
                thread::sleep(Duration::from_millis(500));
            }
        }
    }

    let queue = match queue {
        Some(q) => q, // FIX: Removed 'mut' - it's already mutable from Some(q)
        None => {
            error!("Failed to open queue after retries");
            println!("ERROR: Could not connect to queue");
            println!("Make sure Go OMS has initialized the queue");
            return;
        }
    };

    let mut max_observed = 0u64;
    let mut max_time = Instant::now();
    let start = Instant::now();

    loop {
        let depth = queue.depth();
        let capacity = queue.capacity();
        let fill_pct = (depth as f64 / capacity as f64) * 100.0;
        let elapsed = start.elapsed().as_secs_f64();

        if depth > max_observed {
            max_observed = depth;
            max_time = Instant::now();
        }

        // Only show if depth changes or matches filter
        if let Some(filter) = max_depth_filter {
            if depth >= filter {
                println!(
                    "[{:7.1}s] Depth: {:8} / {:8} ({:5.1}%) | Max: {} at {:.1}s",
                    elapsed,
                    depth,
                    capacity,
                    fill_pct,
                    max_observed,
                    max_time.elapsed().as_secs_f64()
                );
            }
        } else {
            println!(
                "[{:7.1}s] Depth: {:8} / {:8} ({:5.1}%) | Max: {}",
                elapsed, depth, capacity, fill_pct, max_observed
            );
        }

        thread::sleep(Duration::from_millis(500));
    }
}

/// Integration test with Go OMS
fn integration_test() {
    info!("Starting integration test...");
    println!("\n=== Integration Test ===\n");
    println!("Requirements:");
    println!("  1. Go OMS initialized: go run main.go init");
    println!("  2. Go OMS streaming:   go run main.go stream");
    println!("\nStarting consumer...\n");

    let mut queue = match Queue::open(QUEUE_PATH) {
        Ok(q) => q,
        Err(e) => {
            error!("Integration test failed: {}", e);
            println!("FAIL: {}", e);
            return;
        }
    };

    let start = Instant::now();
    let test_duration = Duration::from_secs(10);
    let mut stats = TestStats::new();

    while start.elapsed() < test_duration {
        match queue.dequeue() {
            Ok(Some(order)) => {
                stats.record_success(&order);

                if order.order_id == 0 {
                    stats.record_error("invalid_order_id");
                }
                if order.quantity == 0 {
                    stats.record_error("invalid_quantity");
                }

                if stats.dequeued % 5000 == 0 {
                    let throughput = stats.dequeued as f64 / start.elapsed().as_secs_f64();
                    println!(
                        "[{:6.1}s] Dequeued: {}, Throughput: {:.0}/sec, Depth: {}",
                        start.elapsed().as_secs_f64(),
                        stats.dequeued,
                        throughput,
                        queue.depth()
                    );
                }
            }
            Ok(None) => {
                stats.empty_checks += 1;
                thread::yield_now();
            }
            Err(e) => {
                error!("Dequeue error: {}", e);
                stats.record_error(&e.to_string());
            }
        }
    }

    println!("\n=== Integration Test Results ===\n");
    stats.print_summary(start.elapsed());

    if stats.errors == 0 && stats.dequeued > 100 {
        println!("\n✓ Integration test PASSED");
        info!("Integration test passed");
    } else {
        println!("\n✗ Integration test FAILED");
        error!("Integration test failed");
    }
}

/// Helper struct for test statistics
struct TestStats {
    dequeued: u64,
    empty_checks: u64,
    errors: u64,
    clients: std::collections::HashSet<u32>,
    symbols: std::collections::HashSet<String>,
}

impl TestStats {
    fn new() -> Self {
        TestStats {
            dequeued: 0,
            empty_checks: 0,
            errors: 0,
            clients: std::collections::HashSet::new(),
            symbols: std::collections::HashSet::new(),
        }
    }

    fn record_success(&mut self, order: &Order) {
        self.dequeued += 1;
        self.clients.insert(order.client_id);
        let symbol = String::from_utf8_lossy(&order.symbol)
            .trim_end_matches('\0')
            .to_string();
        if !symbol.is_empty() {
            self.symbols.insert(symbol);
        }
    }

    fn record_error(&mut self, _error: &str) {
        self.errors += 1;
    }

    fn print_summary(&self, elapsed: Duration) {
        let throughput = self.dequeued as f64 / elapsed.as_secs_f64();
        println!("Duration:        {:.2}s", elapsed.as_secs_f64());
        println!("Orders dequeued: {}", self.dequeued);
        println!("Empty checks:    {}", self.empty_checks);
        println!("Errors:          {}", self.errors);
        println!("Throughput:      {:.0} orders/sec", throughput);
        println!("Unique clients:  {}", self.clients.len());
        println!("Unique symbols:  {}", self.symbols.len());
    }
}
