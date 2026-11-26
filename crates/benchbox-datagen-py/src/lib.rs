use benchbox_datagen_core::{Benchmark, Error, TableRequest, record_batches};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_arrow::PyRecordBatchReader;

fn value_error(error: Error) -> PyErr {
    PyValueError::new_err(error.to_string())
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
#[pyo3(signature = (benchmark, table, scale_factor, part = 1, parts = 1))]
fn generate_table(
    py: Python<'_>,
    benchmark: &str,
    table: &str,
    scale_factor: f64,
    part: i32,
    parts: i32,
) -> PyResult<TableStream> {
    let request = TableRequest {
        benchmark: benchmark.parse().map_err(value_error)?,
        table: table.to_owned(),
        scale_factor,
        part,
        parts,
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

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    module.add_class::<TableStream>()?;
    module.add_function(wrap_pyfunction!(generate_table, module)?)?;
    module.add_function(wrap_pyfunction!(table_names, module)?)?;
    Ok(())
}
