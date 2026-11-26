use std::path::PathBuf;

use benchbox_datagen_core::{Benchmark, write_refresh_sets};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let scale: f64 = args[1].parse().expect("scale factor");
    let sets: i32 = args[2].parse().expect("sets");
    let dir = PathBuf::from(&args[3]);
    let seed: Option<i32> = args.get(4).map(|s| s.parse().expect("rng seed"));
    let stats = write_refresh_sets(Benchmark::Tpcds, scale, sets, seed, &dir).expect("refresh");
    for stat in stats {
        println!("{} {} {}", stat.path.display(), stat.rows, stat.bytes);
    }
}
