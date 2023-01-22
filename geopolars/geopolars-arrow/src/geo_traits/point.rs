use geo::{Coord, Point};

pub trait PointTrait: Send + Sync {
    /// x component of this point
    fn x(&self) -> f64;

    /// y component of this point
    fn y(&self) -> f64;

    /// Returns a tuple that contains the x/horizontal & y/vertical component of the point.
    fn x_y(&self) -> (f64, f64);
}

impl PointTrait for Point<f64> {
    fn x(&self) -> f64 {
        self.0.x
    }

    fn y(&self) -> f64 {
        self.0.y
    }

    fn x_y(&self) -> (f64, f64) {
        (self.0.x, self.0.y)
    }
}

impl PointTrait for Coord<f64> {
    fn x(&self) -> f64 {
        self.x
    }

    fn y(&self) -> f64 {
        self.y
    }

    fn x_y(&self) -> (f64, f64) {
        (self.x, self.y)
    }
}

impl PointTrait for &Coord<f64> {
    fn x(&self) -> f64 {
        self.x
    }

    fn y(&self) -> f64 {
        self.y
    }

    fn x_y(&self) -> (f64, f64) {
        (self.x, self.y)
    }
}

#[cfg(test)]
mod tests {
    use super::PointTrait;
    use crate::GeometryArrayTrait;
    use crate::PointArray;

    #[test]
    fn test_point_function_geo() {
        fn identity(point: &impl PointTrait) -> &impl PointTrait {
            point
        }

        let point = geo::point!(x: 1., y: 2.);
        let output = identity(&point);

        assert_eq!(point.x_y(), output.x_y());

        let arrow_point_array: PointArray = vec![point].into();
        let arrow_point_scalar = &arrow_point_array.get(0).unwrap();
        let output_arrow_point_scalar = identity(arrow_point_scalar);

        assert_eq!(arrow_point_scalar.x(), output_arrow_point_scalar.x());
    }
}
