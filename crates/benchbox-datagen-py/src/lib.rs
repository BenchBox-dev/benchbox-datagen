use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use benchbox_datagen_core::{
    Benchmark, Error, FileStats, OutputRequest, QueryRequest, TableRequest, active_locations,
    generate_query as core_generate_query, key_ranges, record_batches, row_counts_cancellable,
    table_scales, write_benchmark_cancellable, write_refresh_sets,
};
use pyo3::exceptions::{PyOSError, PyOverflowError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_arrow::PyRecordBatchReader;

const SIGNAL_POLL: Duration = Duration::from_millis(20);

fn value_error(error: Error) -> PyErr {
    PyValueError::new_err(error.to_string())
}

fn seed_value(seed: Option<&Bound<'_, PyAny>>) -> PyResult<Option<u64>> {
    seed.map(|seed| {
        seed.extract::<u64>().map_err(|error| {
            if error.is_instance_of::<PyOverflowError>(seed.py()) {
                PyValueError::new_err("seed must be between 0 and 2**64 - 1")
            } else {
                error
            }
        })
    })
    .transpose()
}

#[pyclass(frozen, module = "benchbox_datagen._native")]
struct TableStream {
    reader: Py<PyRecordBatchReader>,
}

#[pymethods]
impl TableStream {
    #[pyo3(signature = (requested_schema = None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.reader
            .bind(py)
            .call_method1("__arrow_c_stream__", (requested_schema,))
    }
}

#[pyfunction]
#[pyo3(signature = (benchmark, table, scale_factor, part = 1, parts = 1, seed = None))]
fn generate_table(
    py: Python<'_>,
    benchmark: &str,
    table: &str,
    scale_factor: f64,
    part: i32,
    parts: i32,
    seed: Option<&Bound<'_, PyAny>>,
) -> PyResult<TableStream> {
    let seed = seed_value(seed)?;
    let request = TableRequest {
        benchmark: benchmark.parse().map_err(value_error)?,
        table: table.to_owned(),
        scale_factor,
        part,
        parts,
        seed,
    };
    let reader = PyRecordBatchReader::new(record_batches(&request).map_err(value_error)?);
    Ok(TableStream {
        reader: Py::new(py, reader)?,
    })
}

#[pyfunction]
fn table_names(benchmark: &str) -> PyResult<Vec<&'static str>> {
    Ok(benchmark
        .parse::<Benchmark>()
        .map_err(value_error)?
        .tables()
        .to_vec())
}

#[pyclass(frozen, module = "benchbox_datagen._native", get_all)]
struct FileStat {
    table: String,
    path: String,
    rows: u64,
    bytes: u64,
}

#[pymethods]
impl FileStat {
    fn __repr__(&self) -> String {
        format!(
            "FileStat(table={:?}, path={:?}, rows={}, bytes={})",
            self.table, self.path, self.rows, self.bytes
        )
    }
}

impl From<FileStats> for FileStat {
    fn from(stats: FileStats) -> Self {
        Self {
            table: stats.table.to_owned(),
            path: stats.path.to_string_lossy().into_owned(),
            rows: stats.rows,
            bytes: stats.bytes,
        }
    }
}

#[pyfunction]
#[pyo3(signature = (benchmark, scale_factor, output_dir, format = "parquet", compression = None, threads = None, parts = 1, seed = None))]
#[allow(clippy::too_many_arguments)]
fn write_tables(
    py: Python<'_>,
    benchmark: &str,
    scale_factor: f64,
    output_dir: PathBuf,
    format: &str,
    compression: Option<&str>,
    threads: Option<usize>,
    parts: i32,
    seed: Option<&Bound<'_, PyAny>>,
) -> PyResult<Vec<FileStat>> {
    let seed = seed_value(seed)?;
    let threads = non_zero_threads(threads)?;
    let request = OutputRequest {
        benchmark: benchmark.parse().map_err(value_error)?,
        scale_factor,
        format: format.parse().map_err(value_error)?,
        compression: compression
            .map(|name| {
                name.parse()
                    .map_err(|error: benchbox_datagen_writers::Error| {
                        PyValueError::new_err(error.to_string())
                    })
            })
            .transpose()?,
        threads,
        parts,
        seed,
        output_dir,
    };
    let cancel = Arc::new(AtomicBool::new(false));
    let worker = {
        let cancel = Arc::clone(&cancel);
        thread::spawn(move || write_benchmark_cancellable(&request, &cancel))
    };
    while !worker.is_finished() {
        py.detach(|| thread::sleep(SIGNAL_POLL));
        if let Err(signal) = py.check_signals() {
            cancel.store(true, Ordering::Relaxed);
            let _ = py.detach(|| worker.join());
            return Err(signal);
        }
    }
    let outcome = worker
        .join()
        .map_err(|_| PyRuntimeError::new_err("the writer thread panicked"))?;
    let stats = outcome.map_err(|error| match error {
        Error::Write(message) => PyOSError::new_err(message),
        Error::Encode(message) => PyRuntimeError::new_err(message),
        Error::Cancelled => PyRuntimeError::new_err(error.to_string()),
        other => value_error(other),
    })?;
    Ok(stats.into_iter().map(FileStat::from).collect())
}

fn non_zero_threads(threads: Option<usize>) -> PyResult<Option<NonZeroUsize>> {
    threads
        .map(|count| {
            NonZeroUsize::new(count)
                .ok_or_else(|| PyValueError::new_err("threads must be at least 1"))
        })
        .transpose()
}

#[pyfunction(name = "row_counts")]
#[pyo3(signature = (benchmark, scale_factor, threads = None))]
fn row_counts_py<'py>(
    py: Python<'py>,
    benchmark: &str,
    scale_factor: f64,
    threads: Option<usize>,
) -> PyResult<Bound<'py, PyDict>> {
    let benchmark = benchmark.parse::<Benchmark>().map_err(value_error)?;
    let threads = non_zero_threads(threads)?;
    let cancel = Arc::new(AtomicBool::new(false));
    let worker = {
        let cancel = Arc::clone(&cancel);
        thread::spawn(move || row_counts_cancellable(benchmark, scale_factor, threads, &cancel))
    };
    while !worker.is_finished() {
        py.detach(|| thread::sleep(SIGNAL_POLL));
        if let Err(signal) = py.check_signals() {
            cancel.store(true, Ordering::Relaxed);
            let _ = py.detach(|| worker.join());
            return Err(signal);
        }
    }
    let counts = worker
        .join()
        .map_err(|_| PyRuntimeError::new_err("the row counting thread panicked"))?
        .map_err(|error| match error {
            Error::Cancelled => PyRuntimeError::new_err(error.to_string()),
            other => value_error(other),
        })?;
    let result = PyDict::new(py);
    for (table, rows) in counts {
        result.set_item(table, rows)?;
    }
    Ok(result)
}

#[pyfunction]
#[pyo3(signature = (benchmark, scale_factor, output_dir, sets = 1, rng_seed = None))]
fn write_refresh(
    py: Python<'_>,
    benchmark: &str,
    scale_factor: f64,
    output_dir: PathBuf,
    sets: i32,
    rng_seed: Option<i32>,
) -> PyResult<Vec<FileStat>> {
    let benchmark = benchmark.parse::<Benchmark>().map_err(value_error)?;
    let cancel = Arc::new(AtomicBool::new(false));
    let worker = {
        let cancel = Arc::clone(&cancel);
        thread::spawn(move || {
            write_refresh_sets(
                benchmark,
                scale_factor,
                sets,
                rng_seed,
                &output_dir,
                &cancel,
            )
        })
    };
    while !worker.is_finished() {
        py.detach(|| thread::sleep(SIGNAL_POLL));
        if let Err(signal) = py.check_signals() {
            cancel.store(true, Ordering::Relaxed);
            let _ = py.detach(|| worker.join());
            return Err(signal);
        }
    }
    let stats = worker
        .join()
        .map_err(|_| PyRuntimeError::new_err("the refresh writer thread panicked"))?
        .map_err(|error| match error {
            Error::Write(message) => PyOSError::new_err(message),
            Error::Cancelled => PyRuntimeError::new_err(error.to_string()),
            other => value_error(other),
        })?;
    Ok(stats.into_iter().map(FileStat::from).collect())
}

#[pyfunction(name = "table_scales")]
fn table_scales_py<'py>(
    py: Python<'py>,
    benchmark: &str,
    scale_factor: f64,
) -> PyResult<Bound<'py, PyDict>> {
    let benchmark = benchmark.parse::<Benchmark>().map_err(value_error)?;
    let scales = py
        .detach(|| table_scales(benchmark, scale_factor))
        .map_err(value_error)?;
    let result = PyDict::new(py);
    for scale in scales {
        let entry = PyDict::new(py);
        entry.set_item("id_count", scale.id_count)?;
        entry.set_item("source_rows", scale.source_rows)?;
        result.set_item(scale.table, entry)?;
    }
    Ok(result)
}

#[pyfunction(name = "active_locations")]
fn active_locations_py<'py>(
    py: Python<'py>,
    benchmark: &str,
    scale_factor: f64,
) -> PyResult<Bound<'py, PyDict>> {
    let benchmark = benchmark.parse::<Benchmark>().map_err(value_error)?;
    let active = py
        .detach(|| active_locations(benchmark, scale_factor))
        .map_err(value_error)?;
    let result = PyDict::new(py);
    result.set_item("active_cities", active.cities)?;
    result.set_item("active_counties", active.counties)?;
    result.set_item("active_states", active.states)?;
    Ok(result)
}

#[pyfunction(name = "key_ranges")]
fn key_ranges_py<'py>(
    py: Python<'py>,
    benchmark: &str,
    scale_factor: f64,
) -> PyResult<Bound<'py, PyDict>> {
    let benchmark = benchmark.parse::<Benchmark>().map_err(value_error)?;
    let ranges = py
        .detach(|| key_ranges(benchmark, scale_factor))
        .map_err(value_error)?;
    let result = PyDict::new(py);
    for range in ranges {
        let table = match result.get_item(range.table)? {
            Some(table) => table.cast_into::<PyDict>()?,
            None => {
                let table = PyDict::new(py);
                result.set_item(range.table, &table)?;
                table
            }
        };
        table.set_item(range.column, (range.min, range.max))?;
    }
    Ok(result)
}

#[pyfunction]
#[pyo3(signature = (benchmark, query, template_text, distributions_text, scale_factor, seed = None, stream = None, default_params = false))]
#[allow(clippy::too_many_arguments)]
fn generate_query(
    benchmark: &str,
    query: &str,
    template_text: &str,
    distributions_text: &str,
    scale_factor: f64,
    seed: Option<i64>,
    stream: Option<u32>,
    default_params: bool,
) -> PyResult<(String, Vec<String>)> {
    if benchmark != "tpch" {
        return Err(PyValueError::new_err(format!(
            "query generation is not available for benchmark {benchmark:?}"
        )));
    }
    let generated = core_generate_query(&QueryRequest {
        query,
        template: template_text,
        distributions: distributions_text,
        scale_factor,
        seed,
        stream,
        default_params,
    })
    .map_err(|error| PyValueError::new_err(error.to_string()))?;
    Ok((generated.text, generated.params))
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    module.add_class::<TableStream>()?;
    module.add_function(wrap_pyfunction!(generate_table, module)?)?;
    module.add_class::<FileStat>()?;
    module.add_function(wrap_pyfunction!(table_names, module)?)?;
    module.add_function(wrap_pyfunction!(write_tables, module)?)?;
    module.add_function(wrap_pyfunction!(row_counts_py, module)?)?;
    module.add_function(wrap_pyfunction!(key_ranges_py, module)?)?;
    module.add_function(wrap_pyfunction!(active_locations_py, module)?)?;
    module.add_function(wrap_pyfunction!(table_scales_py, module)?)?;
    module.add_function(wrap_pyfunction!(write_refresh, module)?)?;
    module.add_function(wrap_pyfunction!(generate_query, module)?)?;
    Ok(())
}
