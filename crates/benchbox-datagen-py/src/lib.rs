use benchbox_datagen_core::{Benchmark, Error, TableRequest, record_batches};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_arrow::PyRecordBatchReader;

fn value_error(error: Error) -> PyErr {
    PyValueError::new_err(error.to_string())
}

#[pyfunction]
#[pyo3(signature = (benchmark, table, scale_factor, part = 1, parts = 1))]
fn generate_table(
    benchmark: &str,
    table: &str,
    scale_factor: f64,
    part: i32,
    parts: i32,
) -> PyResult<PyRecordBatchReader> {
    let request = TableRequest {
        benchmark: benchmark.parse().map_err(value_error)?,
        table: table.to_owned(),
        scale_factor,
        part,
        parts,
    };
    Ok(PyRecordBatchReader::new(
        record_batches(&request).map_err(value_error)?,
    ))
}

#[pyfunction]
fn table_names(benchmark: &str) -> PyResult<Vec<&'static str>> {
    Ok(benchmark
        .parse::<Benchmark>()
        .map_err(value_error)?
        .tables()
        .to_vec())
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    module.add_function(wrap_pyfunction!(generate_table, module)?)?;
    module.add_function(wrap_pyfunction!(table_names, module)?)?;
    Ok(())
}
