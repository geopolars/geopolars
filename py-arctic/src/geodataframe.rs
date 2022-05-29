#![allow(unused)]
use arctic::geoseries::GeoSeries;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use py_polars::PyDataFrame;

#[pyclass(extends=PyDataFrame, subclass)]
struct GeoDataFrame {
    val2: usize,
}

#[pymethods]
impl GeoDataFrame {
    #[new]
    fn new() -> (Self, PyDataFrame) {
        (SubClass { val2: 15 }, PyDataFrame::new())
    }

    // pub fn method(&self) -> PyResult<usize> {
    //     Ok(self.val1)
    // }
}

#[pyclass(subclass)]
pub struct BaseClass {
    val1: usize,
}

#[pymethods]
impl BaseClass {
    #[new]
    fn new() -> Self {
        BaseClass { val1: 10 }
    }

    pub fn method(&self) -> PyResult<usize> {
        Ok(self.val1)
    }
}

#[pyclass(extends=BaseClass, subclass)]
pub struct SubClass {
    val2: usize,
}

#[pymethods]
impl SubClass {
    #[new]
    fn new() -> (Self, BaseClass) {
        (SubClass { val2: 15 }, BaseClass::new())
    }

    fn method2(self_: PyRef<'_, Self>) -> PyResult<usize> {
        let super_ = self_.as_ref(); // Get &BaseClass
        super_.method().map(|x| x * self_.val2)
    }
}

#[pyclass(extends=SubClass)]
pub struct SubSubClass {
    val3: usize,
}

#[pymethods]
impl SubSubClass {
    #[new]
    fn new() -> PyClassInitializer<Self> {
        PyClassInitializer::from(SubClass::new()).add_subclass(SubSubClass { val3: 20 })
    }

    fn method3(self_: PyRef<'_, Self>) -> PyResult<usize> {
        let v = self_.val3;
        let super_ = self_.into_super(); // Get PyRef<'_, SubClass>
        SubClass::method2(super_).map(|x| x * v)
    }
}


// fn main() {
//     use pyo3::prelude::*;

//     Python::with_gil(|py| {
//         let subsub = pyo3::PyCell::new(py, SubSubClass::new()).unwrap();
//         pyo3::py_run!(py, subsub, "assert subsub.method3() == 3000")
//     });
// }
