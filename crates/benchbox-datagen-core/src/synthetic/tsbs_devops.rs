use std::collections::HashMap;
use std::f64::consts::FRAC_1_SQRT_2;
use std::fmt::Write;
use std::sync::{Arc, LazyLock};

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringBuilder, TimestampMicrosecondArray};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use super::delimited::TextFormat;
use super::rng::Stream;
use super::{BLOCK_ROWS, Generator, Table, block_len};
use crate::generate::KeyRange;
use crate::schema::{self, Column, Fixed, Int, Text, Timestamp};

const BENCHMARK: &str = "tsbs_devops";
const HOSTS_PER_SCALE: f64 = 100.0;
const MIN_HOSTS: u64 = 10;
const INTERVAL_SECONDS: u64 = 10;
const TIMESTAMPS: u64 = 2 * 24 * 60 * 60 / INTERVAL_SECONDS;
const START_MICROS: i64 = 1_704_067_200_000_000;
const INTERVAL_MICROS: i64 = INTERVAL_SECONDS as i64 * 1_000_000;
const GC_PERIOD: u64 = 360;
const GIB: i64 = 1 << 30;

const REGIONS: [&str; 4] = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"];
const DATACENTERS: [&str; 3] = ["dc1", "dc2", "dc3"];
const RACKS: [&str; 4] = ["rack-a", "rack-b", "rack-c", "rack-d"];
const OS_TYPES: [&str; 4] = ["linux", "linux", "linux", "windows"];
const ARCHITECTURES: [&str; 3] = ["x86_64", "x86_64", "arm64"];
const TEAMS: [&str; 5] = ["platform", "backend", "frontend", "data", "infra"];
const SERVICES: [&str; 6] = ["api", "web", "worker", "cache", "db", "queue"];
const ENVIRONMENTS: [&str; 4] = ["prod", "prod", "staging", "dev"];
const MEMORY_GIB: [i64; 4] = [8, 16, 32, 64];
const DEVICES: [&str; 2] = ["sda", "sdb"];
const INTERFACES: [&str; 2] = ["eth0", "lo"];
const SINES: [f64; 13] = [
    0.0,
    0.258_819_045_102_520_74,
    0.499_999_999_999_999_94,
    FRAC_1_SQRT_2,
    0.866_025_403_784_438_6,
    0.965_925_826_289_068_3,
    1.0,
    0.965_925_826_289_068_3,
    0.866_025_403_784_438_7,
    FRAC_1_SQRT_2,
    0.499_999_999_999_999_94,
    0.258_819_045_102_521,
    1.224_646_799_147_353_2e-16,
];

const COLUMNS: [(&str, &[Column]); 5] = [
    (
        "tags",
        &[
            ("hostname", Text, true),
            ("region", Text, true),
            ("datacenter", Text, true),
            ("rack", Text, true),
            ("os", Text, true),
            ("arch", Text, true),
            ("team", Text, true),
            ("service", Text, true),
            ("service_version", Text, true),
            ("service_environment", Text, true),
        ],
    ),
    (
        "cpu",
        &[
            ("time", Timestamp, true),
            ("hostname", Text, true),
            ("usage_user", Fixed(2), true),
            ("usage_system", Fixed(2), true),
            ("usage_idle", Fixed(2), true),
            ("usage_nice", Fixed(2), true),
            ("usage_iowait", Fixed(2), true),
            ("usage_irq", Fixed(2), true),
            ("usage_softirq", Fixed(2), true),
            ("usage_steal", Fixed(2), true),
            ("usage_guest", Fixed(2), true),
            ("usage_guest_nice", Fixed(2), true),
        ],
    ),
    (
        "mem",
        &[
            ("time", Timestamp, true),
            ("hostname", Text, true),
            ("total", Int, true),
            ("available", Int, true),
            ("used", Int, true),
            ("free", Int, true),
            ("cached", Int, true),
            ("buffered", Int, true),
            ("used_percent", Fixed(2), true),
            ("available_percent", Fixed(2), true),
        ],
    ),
    (
        "disk",
        &[
            ("time", Timestamp, true),
            ("hostname", Text, true),
            ("device", Text, true),
            ("reads_completed", Int, true),
            ("reads_merged", Int, true),
            ("sectors_read", Int, true),
            ("read_time_ms", Int, true),
            ("writes_completed", Int, true),
            ("writes_merged", Int, true),
            ("sectors_written", Int, true),
            ("write_time_ms", Int, true),
            ("io_in_progress", Int, true),
            ("io_time_ms", Int, true),
            ("weighted_io_time_ms", Int, true),
        ],
    ),
    (
        "net",
        &[
            ("time", Timestamp, true),
            ("hostname", Text, true),
            ("interface", Text, true),
            ("bytes_recv", Int, true),
            ("bytes_sent", Int, true),
            ("packets_recv", Int, true),
            ("packets_sent", Int, true),
            ("err_in", Int, true),
            ("err_out", Int, true),
            ("drop_in", Int, true),
            ("drop_out", Int, true),
        ],
    ),
];

static SCHEMAS: LazyLock<HashMap<&'static str, SchemaRef>> =
    LazyLock::new(|| schema::build(&COLUMNS));

fn tsbs_schema(table: &str) -> SchemaRef {
    SCHEMAS
        .get(table)
        .cloned()
        .expect("every TSBS DevOps table has a schema")
}

pub(super) struct TsbsDevops;

impl Generator for TsbsDevops {
    fn name(&self) -> &'static str {
        "tsbs_devops"
    }

    fn tables(&self) -> &'static [&'static str] {
        &["tags", "cpu", "mem", "disk", "net"]
    }

    fn schema(&self, table: &str) -> Option<SchemaRef> {
        SCHEMAS.get(table).cloned()
    }

    fn row_count(&self, table: &str, scale_factor: f64) -> u64 {
        row_count(table, scale_factor)
    }

    fn open(&self, table: &str, scale_factor: f64, seed: u64) -> Arc<dyn Table> {
        let rows = row_count(table, scale_factor);
        match table {
            "tags" => Arc::new(Tags {
                rows,
                fields: Stream::new(BENCHMARK, "tags", "fields", seed),
            }),
            "cpu" => Arc::new(Cpu {
                rows,
                hosts: Stream::new(BENCHMARK, "cpu", "hosts", seed),
                fields: Stream::new(BENCHMARK, "cpu", "fields", seed),
            }),
            "mem" => Arc::new(Mem {
                rows,
                hosts: Stream::new(BENCHMARK, "mem", "hosts", seed),
                fields: Stream::new(BENCHMARK, "mem", "fields", seed),
            }),
            "disk" => Arc::new(Disk {
                rows,
                series: Stream::new(BENCHMARK, "disk", "series", seed),
            }),
            "net" => Arc::new(Net {
                rows,
                series: Stream::new(BENCHMARK, "net", "series", seed),
            }),
            other => unreachable!("table {other:?} was validated by the caller"),
        }
    }

    fn key_ranges(&self, _scale_factor: f64) -> Vec<KeyRange> {
        Vec::new()
    }

    fn text_format(&self, _table: &str) -> TextFormat {
        TextFormat::csv(b',', true)
    }
}

fn host_count(scale_factor: f64) -> u64 {
    ((HOSTS_PER_SCALE * scale_factor) as u64).max(MIN_HOSTS)
}

fn row_count(table: &str, scale_factor: f64) -> u64 {
    let hosts = host_count(scale_factor);
    match table {
        "tags" => hosts,
        "cpu" | "mem" => hosts * TIMESTAMPS,
        "disk" | "net" => hosts * 2 * TIMESTAMPS,
        other => unreachable!("table {other:?} was validated by the caller"),
    }
}

fn int(values: Vec<i64>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}

fn float(values: Vec<f64>) -> ArrayRef {
    Arc::new(Float64Array::from(values))
}

fn times(values: Vec<i64>) -> ArrayRef {
    Arc::new(TimestampMicrosecondArray::from(values))
}

fn text(mut builder: StringBuilder) -> ArrayRef {
    Arc::new(builder.finish())
}

fn batch(schema: SchemaRef, columns: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(schema, columns).expect("generated columns match the schema")
}

fn timestamp(step: u64) -> i64 {
    START_MICROS + step as i64 * INTERVAL_MICROS
}

fn hundredths(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

fn hostname(host: u64, out: &mut String) {
    out.clear();
    write!(out, "host_{host}").expect("string write");
}

fn diurnal_factor(step: u64) -> f64 {
    let hour = (step * INTERVAL_SECONDS / 3_600 % 24) as usize;
    if (6..=18).contains(&hour) {
        1.0 + 0.5 * SINES[hour - 6]
    } else {
        0.7
    }
}

fn series_spans(first: u64, rows: usize) -> Vec<(u64, u64, u64)> {
    let end = first + rows as u64;
    let mut spans = Vec::new();
    let mut row = first;
    while row < end {
        let series = row / TIMESTAMPS;
        let stop = ((series + 1) * TIMESTAMPS).min(end);
        spans.push((
            series,
            row - series * TIMESTAMPS,
            stop - series * TIMESTAMPS,
        ));
        row = stop;
    }
    spans
}

#[derive(Debug)]
struct Tags {
    rows: u64,
    fields: Stream,
}

impl Table for Tags {
    fn schema(&self) -> SchemaRef {
        tsbs_schema("tags")
    }

    fn rows(&self) -> u64 {
        self.rows
    }

    fn block(&self, index: u64) -> RecordBatch {
        let rows = block_len(self.rows, index);
        let first = index * BLOCK_ROWS;
        let mut draw = self.fields.block(index);
        let mut builders: [StringBuilder; 10] = std::array::from_fn(|_| StringBuilder::new());
        let mut scratch = String::new();
        for host in first..first + rows as u64 {
            hostname(host, &mut scratch);
            builders[0].append_value(&scratch);
            builders[1].append_value(REGIONS[host as usize % REGIONS.len()]);
            builders[2].append_value(DATACENTERS[host as usize % DATACENTERS.len()]);
            builders[3].append_value(RACKS[host as usize % RACKS.len()]);
            builders[4].append_value(draw.pick(&OS_TYPES));
            builders[5].append_value(draw.pick(&ARCHITECTURES));
            builders[6].append_value(draw.pick(&TEAMS));
            builders[7].append_value(draw.pick(&SERVICES));
            scratch.clear();
            write!(scratch, "1.{}.{}", draw.range(0, 9), draw.range(0, 99)).expect("string write");
            builders[8].append_value(&scratch);
            builders[9].append_value(draw.pick(&ENVIRONMENTS));
        }
        batch(self.schema(), builders.into_iter().map(text).collect())
    }
}

#[derive(Debug)]
struct Cpu {
    rows: u64,
    hosts: Stream,
    fields: Stream,
}

impl Table for Cpu {
    fn schema(&self) -> SchemaRef {
        tsbs_schema("cpu")
    }

    fn rows(&self) -> u64 {
        self.rows
    }

    fn block(&self, index: u64) -> RecordBatch {
        let rows = block_len(self.rows, index);
        let first = index * BLOCK_ROWS;
        let mut draw = self.fields.block(index);
        let mut stamps = Vec::with_capacity(rows);
        let mut names = StringBuilder::with_capacity(rows, rows * 8);
        let mut values: [Vec<f64>; 10] = std::array::from_fn(|_| Vec::with_capacity(rows));
        let mut scratch = String::new();
        let mut current = u64::MAX;
        let (mut base_user, mut base_system) = (0.0, 0.0);
        for row in first..first + rows as u64 {
            let (host, step) = (row / TIMESTAMPS, row % TIMESTAMPS);
            if host != current {
                current = host;
                let mut constants = self.hosts.block(host);
                base_user = 10.0 + constants.unit_f64() * 20.0;
                base_system = 5.0 + constants.unit_f64() * 10.0;
                hostname(host, &mut scratch);
            }
            let factor = diurnal_factor(step);
            let noise = draw.unit_f64() * 10.0;
            let user = (base_user * factor + noise).clamp(0.0, 100.0);
            let system = (base_system * factor + noise * 0.5).clamp(0.0, 100.0);
            let idle = (100.0 - user - system).max(0.0);
            let rest = [
                draw.unit_f64() * 2.0,
                draw.unit_f64() * 5.0,
                draw.unit_f64() * 0.5,
                draw.unit_f64() * 0.5,
                draw.unit_f64() * 0.1,
                draw.unit_f64() * 0.5,
                draw.unit_f64() * 0.1,
            ];
            stamps.push(timestamp(step));
            names.append_value(&scratch);
            for (column, value) in values
                .iter_mut()
                .zip([user, system, idle].into_iter().chain(rest))
            {
                column.push(hundredths(value));
            }
        }
        let mut columns = vec![times(stamps), text(names)];
        columns.extend(values.into_iter().map(float));
        batch(self.schema(), columns)
    }
}

#[derive(Debug)]
struct Mem {
    rows: u64,
    hosts: Stream,
    fields: Stream,
}

impl Table for Mem {
    fn schema(&self) -> SchemaRef {
        tsbs_schema("mem")
    }

    fn rows(&self) -> u64 {
        self.rows
    }

    fn block(&self, index: u64) -> RecordBatch {
        let rows = block_len(self.rows, index);
        let first = index * BLOCK_ROWS;
        let mut draw = self.fields.block(index);
        let mut stamps = Vec::with_capacity(rows);
        let mut names = StringBuilder::with_capacity(rows, rows * 8);
        let mut ints: [Vec<i64>; 6] = std::array::from_fn(|_| Vec::with_capacity(rows));
        let mut percents: [Vec<f64>; 2] = std::array::from_fn(|_| Vec::with_capacity(rows));
        let mut scratch = String::new();
        let mut current = u64::MAX;
        let (mut total, mut base_used_percent) = (0_i64, 0.0);
        for row in first..first + rows as u64 {
            let (host, step) = (row / TIMESTAMPS, row % TIMESTAMPS);
            if host != current {
                current = host;
                let mut constants = self.hosts.block(host);
                total = *constants.pick(&MEMORY_GIB) * GIB;
                base_used_percent = 40.0 + constants.unit_f64() * 30.0;
                hostname(host, &mut scratch);
            }
            let trend = (step as f64 / TIMESTAMPS as f64) * 10.0;
            let gc_drop = if step % GC_PERIOD == 0 { -15.0 } else { 0.0 };
            let used_percent =
                (base_used_percent + trend + gc_drop + draw.unit_f64() * 5.0).clamp(10.0, 95.0);
            let used = (total as f64 * used_percent / 100.0) as i64;
            let free = total - used;
            let cached = (used as f64 * 0.3 * draw.unit_f64()) as i64;
            let buffered = (used as f64 * 0.1 * draw.unit_f64()) as i64;
            let available = free + cached + buffered;
            stamps.push(timestamp(step));
            names.append_value(&scratch);
            for (column, value) in ints
                .iter_mut()
                .zip([total, available, used, free, cached, buffered])
            {
                column.push(value);
            }
            percents[0].push(hundredths(used_percent));
            percents[1].push(hundredths(available as f64 * 100.0 / total as f64));
        }
        let mut columns = vec![times(stamps), text(names)];
        columns.extend(ints.into_iter().map(int));
        columns.extend(percents.into_iter().map(float));
        batch(self.schema(), columns)
    }
}

#[derive(Debug)]
struct Disk {
    rows: u64,
    series: Stream,
}

impl Table for Disk {
    fn schema(&self) -> SchemaRef {
        tsbs_schema("disk")
    }

    fn rows(&self) -> u64 {
        self.rows
    }

    fn block(&self, index: u64) -> RecordBatch {
        let rows = block_len(self.rows, index);
        let mut stamps = Vec::with_capacity(rows);
        let mut names = StringBuilder::with_capacity(rows, rows * 8);
        let mut devices = StringBuilder::with_capacity(rows, rows * 3);
        let mut ints: [Vec<i64>; 11] = std::array::from_fn(|_| Vec::with_capacity(rows));
        let mut scratch = String::new();
        for (series, low, high) in series_spans(index * BLOCK_ROWS, rows) {
            hostname(series / 2, &mut scratch);
            let device = DEVICES[series as usize % 2];
            let mut draw = self.series.block(series);
            let (mut reads_total, mut writes_total) = (0_i64, 0_i64);
            let (mut read_time_total, mut write_time_total) = (0_i64, 0_i64);
            for step in 0..high {
                let multiplier = if draw.unit_f64() < 0.05 { 10 } else { 1 };
                let reads = draw.range(10, 99) * multiplier;
                let writes = draw.range(5, 49) * multiplier;
                let read_time = (reads as f64 * (0.5 + draw.unit_f64() * 2.0)) as i64;
                let write_time = (writes as f64 * (1.0 + draw.unit_f64() * 5.0)) as i64;
                reads_total += reads;
                writes_total += writes;
                read_time_total += read_time;
                write_time_total += write_time;
                let in_progress = draw.range(0, 4);
                if step < low {
                    continue;
                }
                let io_time = read_time_total + write_time_total;
                stamps.push(timestamp(step));
                names.append_value(&scratch);
                devices.append_value(device);
                let values = [
                    reads_total,
                    (reads as f64 * 0.1) as i64,
                    reads_total * 8,
                    read_time_total,
                    writes_total,
                    (writes as f64 * 0.1) as i64,
                    writes_total * 8,
                    write_time_total,
                    in_progress,
                    io_time,
                    (io_time as f64 * 1.1) as i64,
                ];
                for (column, value) in ints.iter_mut().zip(values) {
                    column.push(value);
                }
            }
        }
        let mut columns = vec![times(stamps), text(names), text(devices)];
        columns.extend(ints.into_iter().map(int));
        batch(self.schema(), columns)
    }
}

#[derive(Debug)]
struct Net {
    rows: u64,
    series: Stream,
}

impl Table for Net {
    fn schema(&self) -> SchemaRef {
        tsbs_schema("net")
    }

    fn rows(&self) -> u64 {
        self.rows
    }

    fn block(&self, index: u64) -> RecordBatch {
        let rows = block_len(self.rows, index);
        let mut stamps = Vec::with_capacity(rows);
        let mut names = StringBuilder::with_capacity(rows, rows * 8);
        let mut interfaces = StringBuilder::with_capacity(rows, rows * 4);
        let mut ints: [Vec<i64>; 8] = std::array::from_fn(|_| Vec::with_capacity(rows));
        let mut scratch = String::new();
        for (series, low, high) in series_spans(index * BLOCK_ROWS, rows) {
            hostname(series / 2, &mut scratch);
            let interface = INTERFACES[series as usize % 2];
            let multiplier = if interface == "lo" { 0.1 } else { 1.0 };
            let mut draw = self.series.block(series);
            let (mut recv_total, mut sent_total) = (0_i64, 0_i64);
            let (mut packets_recv_total, mut packets_sent_total) = (0_i64, 0_i64);
            for step in 0..high {
                let recv = (draw.range(1_000, 99_999) as f64 * multiplier) as i64;
                let sent = (draw.range(500, 49_999) as f64 * multiplier) as i64;
                recv_total += recv;
                sent_total += sent;
                packets_recv_total += recv / 1_500;
                packets_sent_total += sent / 1_500;
                let err_in = i64::from(draw.unit_f64() < 0.001);
                let err_out = i64::from(draw.unit_f64() < 0.001);
                let drop_in = i64::from(draw.unit_f64() < 0.002);
                let drop_out = i64::from(draw.unit_f64() < 0.002);
                if step < low {
                    continue;
                }
                stamps.push(timestamp(step));
                names.append_value(&scratch);
                interfaces.append_value(interface);
                let values = [
                    recv_total,
                    sent_total,
                    packets_recv_total,
                    packets_sent_total,
                    err_in,
                    err_out,
                    drop_in,
                    drop_out,
                ];
                for (column, value) in ints.iter_mut().zip(values) {
                    column.push(value);
                }
            }
        }
        let mut columns = vec![times(stamps), text(names), text(interfaces)];
        columns.extend(ints.into_iter().map(int));
        batch(self.schema(), columns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(batch: &RecordBatch, name: &str) -> Vec<i64> {
        let array = batch.column_by_name(name).expect("column exists");
        array
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int column")
            .values()
            .to_vec()
    }

    fn whole(generator: &TsbsDevops, table: &str, scale_factor: f64) -> RecordBatch {
        let source = generator.open(table, scale_factor, 42);
        let blocks: Vec<_> = (0..source.rows().div_ceil(BLOCK_ROWS))
            .map(|index| source.block(index))
            .collect();
        arrow::compute::concat_batches(&source.schema(), &blocks).expect("blocks concatenate")
    }

    #[test]
    fn row_counts_are_closed_form_in_the_scale_factor() {
        let counts = |scale_factor| {
            ["tags", "cpu", "mem", "disk", "net"].map(|table| row_count(table, scale_factor))
        };
        assert_eq!(counts(0.01), [10, 172_800, 172_800, 345_600, 345_600]);
        assert_eq!(counts(0.1), counts(0.01));
        assert_eq!(
            counts(1.0),
            [100, 1_728_000, 1_728_000, 3_456_000, 3_456_000]
        );
        assert_eq!(counts(2.5)[0], 250);
        assert_eq!(counts(10.0)[3], 34_560_000);
    }

    #[test]
    fn cumulative_counters_continue_across_block_boundaries() {
        let generator = TsbsDevops;
        for (table, columns) in [
            (
                "disk",
                &["reads_completed", "writes_completed", "io_time_ms"][..],
            ),
            ("net", &["bytes_recv", "bytes_sent", "packets_recv"][..]),
        ] {
            let data = whole(&generator, table, 0.01);
            for name in columns {
                let values = column(&data, name);
                for (row, pair) in values.windows(2).enumerate() {
                    let next = row as u64 + 1;
                    if next.is_multiple_of(TIMESTAMPS) {
                        assert!(
                            pair[1] < pair[0],
                            "{table}.{name} resets at series {}",
                            next / TIMESTAMPS
                        );
                    } else {
                        assert!(pair[1] >= pair[0], "{table}.{name} decreases at row {next}");
                    }
                }
            }
        }
    }

    #[test]
    fn cpu_and_mem_values_stay_inside_their_documented_bounds() {
        let generator = TsbsDevops;
        let mem = whole(&generator, "mem", 0.01);
        let total = column(&mem, "total");
        let used = column(&mem, "used");
        let free = column(&mem, "free");
        let available = column(&mem, "available");
        for row in 0..total.len() {
            assert_eq!(used[row] + free[row], total[row]);
            assert!(available[row] >= free[row]);
            assert!(used[row] * 100 <= total[row] * 95);
            assert!(used[row] * 100 >= total[row] * 10 - 100);
        }
        let cpu = whole(&generator, "cpu", 0.01);
        for name in ["usage_user", "usage_system", "usage_idle"] {
            let values = cpu
                .column_by_name(name)
                .expect("column exists")
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("float column")
                .values()
                .to_vec();
            assert!(
                values.iter().all(|value| (0.0..=100.0).contains(value)),
                "{name}"
            );
        }
    }

    #[test]
    fn metric_doubles_hold_exactly_the_two_decimals_the_text_form_prints() {
        let generator = TsbsDevops;
        for table in ["cpu", "mem"] {
            let data = whole(&generator, table, 0.01);
            for (index, field) in data.schema().fields().iter().enumerate() {
                if field.data_type() != &arrow::datatypes::DataType::Float64 {
                    continue;
                }
                let values = data
                    .column(index)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("float column");
                for value in values.values() {
                    let printed = format!("{value:.2}");
                    assert_eq!(
                        printed.parse::<f64>().unwrap(),
                        *value,
                        "{table}.{}",
                        field.name()
                    );
                }
            }
        }
    }
}
