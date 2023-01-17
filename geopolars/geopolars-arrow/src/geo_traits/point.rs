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
