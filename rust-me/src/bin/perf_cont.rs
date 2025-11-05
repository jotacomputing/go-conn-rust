use rust_me::Queue;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("[MATCH] Rust Consumer");
    println!("[MATCH] Running forever (Press Ctrl+C to stop)\n");

    let mut queue = Queue::open("/tmp/sex")?;

    let atomic_count = Arc::new(AtomicU64::new(0));
    let count_clone = atomic_count.clone();

    // Stats goroutine
    thread::spawn(move || {
        let mut last_count = 0u64;
        let mut last_time = std::time::Instant::now();

        loop {
            thread::sleep(Duration::from_secs(2));

            let now = std::time::Instant::now();
            let elapsed = now.duration_since(last_time).as_secs_f64();
            let current = count_clone.load(Ordering::Relaxed);

            let ops = (current - last_count) as f64;
            let throughput = ops / elapsed;

            println!(
                "[MATCH]: {:.0} orders/sec ({:.2} million/sec)",
                throughput,
                throughput / 1e6
            );

            last_count = current;
            last_time = now;
        }
    });

    // INFINITE LOOP
    loop {
        match queue.dequeue() {
            Ok(Some(_order)) => {
                atomic_count.fetch_add(1, Ordering::Relaxed);
            }
            Ok(None) => {
                std::hint::spin_loop();
            }
            Err(_) => break Ok(()),
        }
    }
}
