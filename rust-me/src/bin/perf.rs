use clap::Parser;
use rust_me::Queue;
use std::time::Instant; // Import clap

/// HFT performance benchmark consumer
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Number of orders to consume
    #[arg(long, default_value_t = 10)]
    orders: u64,

    /// Path to the queue file
    #[arg(long, default_value = "/tmp/sex")]
    queue: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse(); // Parse arguments

    println!("[PERF] Initializing queue: {}", args.queue);
    let mut queue = Queue::open(&args.queue)?; // Use arg for path

    println!(
        "[PERF] Rust consumer: consuming {} orders...\n",
        args.orders
    );

    let start = Instant::now();
    let mut count = 0u64;

    loop {
        match queue.dequeue() {
            Ok(Some(_order)) => {
                count += 1;
                if count == args.orders {
                    break; // We're done, break the (only) loop
                }
            }
            Ok(None) => {
                // Queue is empty, spin and try again
                std::hint::spin_loop();
            }
            Err(e) => {
                println!("Queue error: {}", e);
                break; // Error, exit
            }
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    // Use the actual count for throughput
    let throughput = count as f64 / elapsed;

    println!("\n=== Rust Consumer Results ===");
    println!("Orders consumed: {}", count);
    println!("Time: {:.2}s", elapsed);
    println!(
        "Throughput: {:.0} orders/sec ({:.2} million/sec)",
        throughput,
        throughput / 1e6
    );

    Ok(())
}
