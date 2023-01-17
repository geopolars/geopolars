use geo::{Coord, CoordNum, Point};

pub trait PointTrait {
    type Scalar: CoordNum;

    /// x component of this point
    fn x(&self) -> Self::Scalar;

    /// y component of this point
    fn y(&self) -> Self::Scalar;

    /// Returns a tuple that contains the x/horizontal & y/vertical component of the point.
    fn x_y(&self) -> (Self::Scalar, Self::Scalar);
}

impl<T: CoordNum> PointTrait for Point<T> {
    type Scalar = T;

    fn x(&self) -> T {
        self.0.x
    }

    fn y(&self) -> T {
        self.0.y
    }

    fn x_y(&self) -> (T, T) {
        (self.0.x, self.0.y)
    }
}

impl<T: CoordNum> PointTrait for Coord<T> {
    type Scalar = T;

    fn x(&self) -> T {
        self.x
    }

    fn y(&self) -> T {
        self.y
    }

    fn x_y(&self) -> (T, T) {
        (self.x, self.y)
    }
}

#[cfg(test)]
mod tests {
    use super::PointTrait;
    use crate::PointArray;

    #[test]
    fn test_point_function_geo() {
        fn identity(point: &impl PointTrait<Scalar = f64>) -> (f64, f64) {
            point.x_y()
        }

        let point = geo::point!(x: 1., y: 2.);
        let output = identity(&point);

        assert_eq!(point.x_y(), output);

        let arrow_point_array: PointArray = vec![point].into();
        let arrow_point_scalar = &arrow_point_array.get(0).unwrap();
        let output_arrow_point_scalar = identity(arrow_point_scalar);

        assert_eq!(arrow_point_scalar.x(), output_arrow_point_scalar.0);
    }
}
