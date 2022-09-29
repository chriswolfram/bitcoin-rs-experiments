mod rates_table;
use std::hash::Hash;
use bitcoin_explorer::{BitcoinDB, SBlock};
use chrono::prelude::*;
use indicatif::ProgressBar;
use serde::ser::Serialize;
use serde_json;
use std::collections::HashMap;
use std::fs::File;
use std::hash::Hasher;
use std::path::Path;

fn main() {
    let result_directory = Path::new("/Users/christopher/git/bitcoin-rs-experiments/results/");
    let db = BitcoinDB::new(
        Path::new("/Users/christopher/Documents/bitcoin-core/"),
        false,
    )
    .unwrap();

    let exchange_rates = rates_table::get_rates_table();

    write_to_file(result_directory, "number_unique_miners_over_time", || {
        number_unique_miners_over_time(&db, 1000, 1000)
    })
    .expect("Failed number_unique_miners_over_time");

    write_to_file(result_directory, "transaction_size_distribution", || {
        transaction_size_distribution(&db, 0.001)
    })
    .expect("Failed transaction_size_distribution");

    write_to_file(
        result_directory,
        "transaction_size_distribution_usd",
        || transaction_size_distribution_usd(&db, &exchange_rates, 0.001),
    )
    .expect("Failed transaction_size_distribution_usd");

    write_to_file(result_directory, "transaction_volume_time_series", || {
        transaction_volume_time_series(&db, 60 * 60 * 24)
    })
    .expect("Failed transaction_volume_time_series");

    write_to_file(
        result_directory,
        "large_transaction_count_time_series_1000000",
        || large_transaction_count_time_series(&db, &exchange_rates, 1000000.0, 60 * 60 * 24),
    )
    .expect("Failed large_transaction_count_time_series_1000000");

    write_to_file(
        result_directory,
        "large_transaction_count_time_series_100000",
        || large_transaction_count_time_series(&db, &exchange_rates, 100000.0, 60 * 60 * 24),
    )
    .expect("Failed large_transaction_count_time_series_100000");

    write_to_file(
        result_directory,
        "large_transaction_wallet_time_series_1000000",
        || large_transaction_wallet_time_series(&db, &exchange_rates, 1000000.0, 60 * 60 * 24 * 7),
    )
    .expect("Failed large_transaction_wallet_time_series_1000000");
}

fn write_to_file<T: Serialize>(
    result_directory: &Path,
    name: &str,
    f: impl Fn() -> T,
) -> Option<()> {
    let mut output_path = result_directory.join(name);
    output_path.set_extension("json");

    if output_path.exists() {
        println!("Already computed {}", name);
        return Some(());
    }

    println!("Computing {}:", name);
    let start = std::time::Instant::now();
    let data = f();
    println!("Finished {} in time {:?}", name, start.elapsed());
    let output_file = File::create(output_path).ok()?;
    serde_json::to_writer(output_file, &data).ok()
}

fn number_unique_miners_over_time(
    db: &BitcoinDB,
    window_size: usize,
    stride: usize,
) -> Vec<(u32, usize, usize)> {
    let mut res = Vec::new();
    res.reserve(db.get_block_count() / stride);

    let mut wallets = std::collections::HashSet::new();
    wallets.reserve(window_size);

    let progress = ProgressBar::new(db.get_block_count() as u64);
    let mut buffer: std::collections::VecDeque<bitcoin_explorer::Address> =
        std::collections::VecDeque::with_capacity(window_size);
    for (i, b) in progress.wrap_iter(db.iter_block::<SBlock>(0, db.get_block_count()).enumerate()) {
        if i >= window_size && i % stride == 0 {
            wallets.clear();
            for addr in buffer.iter() {
                wallets.insert(addr.clone());
            }
            res.push((b.header.time, i, wallets.len()));
        }
        let tx = b.txdata.first().expect("No coinbase transaction.");
        let max_value_output = tx
            .output
            .iter()
            .max_by_key(|o| o.value)
            .expect("No coinbase transaction recipients.");
        for addr in max_value_output.addresses.iter() {
            buffer.push_back(addr.clone());
            if buffer.len() > window_size {
                buffer.pop_front();
            }
        }
    }

    progress.finish_with_message("Completed: number_unique_miners_over_time");
    res
}

fn transaction_size_distribution(db: &BitcoinDB, bin_size: f64) -> Vec<(f64, u64)> {
    let mut res = HashMap::new();

    let progress = ProgressBar::new(db.get_block_count() as u64);
    for b in progress.wrap_iter(db.iter_block::<SBlock>(0, db.get_block_count())) {
        for t in b.txdata.iter() {
            let total_value: u64 = t.output.iter().map(|o| o.value).sum();
            let counter = res
                .entry(((total_value as f64).log10() / bin_size).floor() as u64)
                .or_insert(0);
            *counter += 1;
        }
    }

    progress.finish_with_message("Completed: transaction_size_distribution");
    res.keys()
        .map(|k| f64::powf(10f64, (*k as f64) * bin_size))
        .zip(res.values().map(|v| *v))
        .collect()
}

fn transaction_size_distribution_usd(
    db: &BitcoinDB,
    exchange_rates: &HashMap<Date<Utc>, f64>,
    bin_size: f64,
) -> Vec<(f64, u64)> {
    let mut res = HashMap::new();

    let progress = ProgressBar::new(db.get_block_count() as u64);
    for b in progress.wrap_iter(db.iter_block::<SBlock>(0, db.get_block_count())) {
        for t in b.txdata.iter() {
            let total_value: u64 = t.output.iter().map(|o| o.value).sum();
            let date: Date<Utc> =
                DateTime::from_utc(NaiveDateTime::from_timestamp(b.header.time as i64, 0), Utc)
                    .date();

            if let Some(rate) = exchange_rates.get(&date) {
                let usd_value = total_value as f64 * rate;
                let counter = res
                    .entry(((usd_value as f64).log10() / bin_size).floor() as u64)
                    .or_insert(0);
                *counter += 1;
            }
        }
    }

    progress.finish_with_message("Completed: transaction_size_distribution_usd");
    res.keys()
        .map(|k| f64::powf(10f64, (*k as f64) * bin_size))
        .zip(res.values().map(|v| *v))
        .collect()
}

fn transaction_volume_time_series(db: &BitcoinDB, bin_size: i64) -> Vec<(i64, u64)> {
    let mut res = HashMap::new();

    let progress = ProgressBar::new(db.get_block_count() as u64);
    for b in progress.wrap_iter(db.iter_block::<SBlock>(0, db.get_block_count())) {
        for t in b.txdata.iter() {
            let total_value: u64 = t.output.iter().map(|o| o.value).sum();
            let counter = res
                .entry((b.header.time as f64 / bin_size as f64).floor() as i64)
                .or_insert(0);
            *counter += total_value;
        }
    }

    res.keys()
        .map(|k| k * bin_size)
        .zip(res.values().map(|v| *v))
        .collect()
}

fn large_transaction_count_time_series(
    db: &BitcoinDB,
    exchange_rates: &HashMap<Date<Utc>, f64>,
    threshold: f64,
    bin_size: i64,
) -> Vec<(i64, u64)> {
    let mut res = HashMap::new();

    let progress = ProgressBar::new(db.get_block_count() as u64);
    for b in progress.wrap_iter(db.iter_block::<SBlock>(0, db.get_block_count())) {
        for t in b.txdata.iter() {
            let total_value: u64 = t.output.iter().map(|o| o.value).sum();
            let date: Date<Utc> =
                DateTime::from_utc(NaiveDateTime::from_timestamp(b.header.time as i64, 0), Utc)
                    .date();

            if let Some(rate) = exchange_rates.get(&date) {
                let usd_value = total_value as f64 * rate;
                if usd_value > threshold {
                    let counter = res
                        .entry((b.header.time as f64 / bin_size as f64).floor() as i64)
                        .or_insert(0);
                    *counter += 1;
                }
            }
        }
    }

    progress.finish_with_message("Completed: transaction_size_distribution_usd");
    res.keys()
        .map(|k| k * bin_size)
        .zip(res.values().map(|v| *v))
        .collect()
}

fn large_transaction_wallet_time_series(
    db: &BitcoinDB,
    exchange_rates: &HashMap<Date<Utc>, f64>,
    threshold: f64,
    bin_size: i64,
) -> Vec<(i64, u64)> {
    let mut res = HashMap::new();
    let mut current_bin = 0;
    let mut wallet_totals = HashMap::new();

    let progress = ProgressBar::new(db.get_block_count() as u64);
    for b in progress.wrap_iter(db.iter_block::<SBlock>(0, db.get_block_count())) {
        let date: Date<Utc> =
            DateTime::from_utc(NaiveDateTime::from_timestamp(b.header.time as i64, 0), Utc).date();
        if let Some(rate) = exchange_rates.get(&date) {
            let bin =
                (b.header.time as f64 / bin_size as f64).floor() as i64 * b.header.time as i64;
            if bin != current_bin {
                if current_bin != 0 {
                    let satoshi_threshold = threshold / rate;
                    let count = wallet_totals
                        .values()
                        .filter(|v| **v as f64 > satoshi_threshold)
                        .count() as u64;
                    res.insert(bin, count);
                }
                current_bin = bin;
            }
            for t in b.txdata.iter() {
                for output in t.output.iter() {
                    for addr in output.addresses.iter() {
                        let mut hasher = std::collections::hash_map::DefaultHasher::new();
                        addr.hash(&mut hasher);
                        *wallet_totals.entry(hasher.finish()).or_insert(0) += output.value;
                    }
                }
            }
        }
    }

    progress.finish_with_message("Completed: transaction_size_distribution_usd");
    res.keys()
        .map(|k| k * bin_size)
        .zip(res.values().map(|v| *v))
        .collect()
}
