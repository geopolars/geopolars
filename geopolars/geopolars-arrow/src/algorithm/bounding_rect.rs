use crate::geo_traits::{
    LineStringTrait, MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait,
    PolygonTrait,
};
use crate::{LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon};
use geo::{coord, Rect};

#[derive(Debug, Clone, Copy)]
struct BoundingRect {
    minx: f64,
    miny: f64,
    maxx: f64,
    maxy: f64,
}

impl BoundingRect {
    /// New
    pub fn new() -> Self {
        BoundingRect {
            minx: f64::INFINITY,
            miny: f64::INFINITY,
            maxx: -f64::INFINITY,
            maxy: -f64::INFINITY,
        }
    }

    pub fn update(&mut self, point: impl PointTrait) {
        if point.x() < self.minx {
            self.minx = point.x();
        }
        if point.y() < self.miny {
            self.miny = point.y();
        }
        if point.x() > self.maxx {
            self.maxx = point.x();
        }
        if point.y() > self.maxy {
            self.maxy = point.y();
        }
    }
}

impl From<BoundingRect> for Rect {
    fn from(value: BoundingRect) -> Self {
        let min_coord = coord! { x: value.minx, y: value.miny };
        let max_coord = coord! { x: value.maxx, y: value.maxy };
        Rect::new(min_coord, max_coord)
    }
}

impl From<BoundingRect> for ([f64; 2], [f64; 2]) {
    fn from(value: BoundingRect) -> Self {
        ([value.minx, value.miny], [value.maxx, value.maxy])
    }
}

pub fn bounding_rect_point(geom: &'_ Point) -> ([f64; 2], [f64; 2]) {
    let mut rect = BoundingRect::new();
    rect.update(geom);
    rect.into()
}

pub fn bounding_rect_multipoint(geom: &'_ MultiPoint) -> ([f64; 2], [f64; 2]) {
    let mut rect = BoundingRect::new();
    for geom_idx in 0..geom.num_points() {
        let point = geom.point(geom_idx).unwrap();
        rect.update(point);
    }
    rect.into()
}

pub fn bounding_rect_linestring(geom: &'_ LineString) -> ([f64; 2], [f64; 2]) {
    let mut rect = BoundingRect::new();
    for geom_idx in 0..geom.num_points() {
        let point = geom.point(geom_idx).unwrap();
        rect.update(point);
    }
    rect.into()
}

pub fn bounding_rect_multilinestring(geom: &'_ MultiLineString) -> ([f64; 2], [f64; 2]) {
    let mut rect = BoundingRect::new();
    for geom_idx in 0..geom.num_lines() {
        let linestring = geom.line(geom_idx).unwrap();
        for coord_idx in 0..linestring.num_points() {
            let point = linestring.point(coord_idx).unwrap();
            rect.update(point);
        }
    }
    rect.into()
}

pub fn bounding_rect_polygon(geom: &'_ Polygon) -> ([f64; 2], [f64; 2]) {
    let mut rect = BoundingRect::new();
    let exterior_ring = geom.exterior();
    for coord_idx in 0..exterior_ring.num_points() {
        let point = exterior_ring.point(coord_idx).unwrap();
        rect.update(point);
    }

    for interior_idx in 0..geom.num_interiors() {
        let linestring = geom.interior(interior_idx).unwrap();
        for coord_idx in 0..linestring.num_points() {
            let point = linestring.point(coord_idx).unwrap();
            rect.update(point);
        }
    }
    rect.into()
}

pub fn bounding_rect_multipolygon(geom: &'_ MultiPolygon) -> ([f64; 2], [f64; 2]) {
    let mut rect = BoundingRect::new();
    for geom_idx in 0..geom.num_polygons() {
        let polygon = geom.polygon(geom_idx).unwrap();

        let exterior_ring = polygon.exterior();
        for coord_idx in 0..exterior_ring.num_points() {
            let point = exterior_ring.point(coord_idx).unwrap();
            rect.update(point);
        }

        for interior_idx in 0..polygon.num_interiors() {
            let linestring = polygon.interior(interior_idx).unwrap();
            for coord_idx in 0..linestring.num_points() {
                let point = linestring.point(coord_idx).unwrap();
                rect.update(point);
            }
        }
    }

    rect.into()
}

// TODO: add tests from geo
