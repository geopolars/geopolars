use crate::geo_traits::PointTrait;
use arrow2::buffer::Buffer;

/// An arrow equivalent of a Point
#[derive(Debug, Clone)]
pub struct Point<'a> {
    pub x: &'a Buffer<f64>,
    pub y: &'a Buffer<f64>,
    pub geom_index: usize,
}

impl PointTrait for Point<'_> {
    fn x(&self) -> f64 {
        self.x[self.geom_index]
    }

    fn y(&self) -> f64 {
        self.y[self.geom_index]
    }

    fn x_y(&self) -> (f64, f64) {
        (self.x[self.geom_index], self.y[self.geom_index])
    }
}

impl From<Point<'_>> for geo::Point {
    fn from(value: Point<'_>) -> Self {
        (&value).into()
    }
}

impl From<&Point<'_>> for geo::Point {
    fn from(value: &Point<'_>) -> Self {
        geo::Point::new(value.x(), value.y())
    }
}
