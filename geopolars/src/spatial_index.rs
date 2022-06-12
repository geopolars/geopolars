use geo::{
    prelude::BoundingRect, Geometry, Line, LineString, MultiLineString, MultiPoint, MultiPolygon,
    Point, Polygon,
};
use polars::prelude::{PolarsError, Series};
use rstar::{RTree, RTreeObject, AABB};

use crate::util::iter_geom;

pub enum NodeEnvelope {
    Point([f64; 2]),
    BBox([[f64; 2]; 2]),
}

impl From<Point<f64>> for NodeEnvelope {
    fn from(point: Point<f64>) -> Self {
        NodeEnvelope::Point([point.x(), point.y()])
    }
}

impl From<Polygon<f64>> for NodeEnvelope {
    fn from(polygon: Polygon<f64>) -> Self {
        let envelope = polygon.bounding_rect().unwrap();
        NodeEnvelope::BBox([
            [envelope.min().x, envelope.min().y],
            [envelope.max().x, envelope.max().y],
        ])
    }
}

impl From<MultiPolygon<f64>> for NodeEnvelope {
    fn from(multi_polygon: MultiPolygon<f64>) -> Self {
        let envelope = multi_polygon.bounding_rect().unwrap();
        NodeEnvelope::BBox([
            [envelope.min().x, envelope.min().y],
            [envelope.max().x, envelope.max().y],
        ])
    }
}

impl From<MultiPoint<f64>> for NodeEnvelope {
    fn from(multi_point: MultiPoint<f64>) -> Self {
        let envelope = multi_point.bounding_rect().unwrap();
        NodeEnvelope::BBox([
            [envelope.min().x, envelope.min().y],
            [envelope.max().x, envelope.max().y],
        ])
    }
}

impl From<LineString<f64>> for NodeEnvelope {
    fn from(line: LineString<f64>) -> Self {
        let envelope = line.bounding_rect().unwrap();
        NodeEnvelope::BBox([
            [envelope.min().x, envelope.min().y],
            [envelope.max().x, envelope.max().y],
        ])
    }
}

impl From<MultiLineString<f64>> for NodeEnvelope {
    fn from(multi_line: MultiLineString<f64>) -> Self {
        let envelope = multi_line.bounding_rect().unwrap();
        NodeEnvelope::BBox([
            [envelope.min().x, envelope.min().y],
            [envelope.max().x, envelope.max().y],
        ])
    }
}

impl From<Line<f64>> for NodeEnvelope {
    fn from(line: Line<f64>) -> Self {
        let envelope = line.bounding_rect();
        NodeEnvelope::BBox([
            [envelope.min().x, envelope.min().y],
            [envelope.max().x, envelope.max().y],
        ])
    }
}

pub struct TreeNode {
    pub index: usize,
    pub envelope: NodeEnvelope,
}

impl RTreeObject for TreeNode {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        match self.envelope {
            NodeEnvelope::Point(point) => AABB::from_point(point),
            NodeEnvelope::BBox(bbox) => AABB::from_corners(bbox[0], bbox[1]),
        }
    }
}

impl TryFrom<Geometry<f64>> for NodeEnvelope {
    type Error = PolarsError;
    fn try_from(geom: Geometry<f64>) -> Result<Self, Self::Error> {
        match geom {
            Geometry::Polygon(poly) => Ok(poly.into()),
            Geometry::MultiPolygon(multi_poly) => Ok(multi_poly.into()),
            Geometry::Point(point) => Ok(point.into()),
            Geometry::MultiPoint(multi_point) => Ok(multi_point.into()),
            Geometry::Line(line) => Ok(line.into()),
            Geometry::LineString(line_string) => Ok(line_string.into()),
            Geometry::MultiLineString(multi_line_string) => Ok(multi_line_string.into()),

            _ => Err(PolarsError::ComputeError(std::borrow::Cow::Borrowed(
                "Geometry type not currently supported for indexing",
            ))),
        }
    }
}

pub struct SpatialIndex {
    pub r_tree: RTree<TreeNode>,
}

impl SpatialIndex {}

impl TryFrom<Series> for SpatialIndex {
    type Error = PolarsError;

    fn try_from(series: Series) -> Result<Self, Self::Error> {
        let mut r_tree: RTree<TreeNode> = RTree::new();
        for (index, geom) in iter_geom(&series).enumerate() {
            let node = TreeNode {
                index,
                envelope: geom.try_into()?,
            };
            r_tree.insert(node)
        }
        Ok(SpatialIndex { r_tree })
    }
}

#[cfg(test)]
mod tests {
    use geo::{polygon, Geometry, Point, Polygon};
    use polars::prelude::{PolarsError, Series};
    use rstar::AABB;

    use crate::{geoseries::GeoSeries, spatial_index::SpatialIndex};

    #[test]
    fn spatial_index_points() {
        let v: Vec<Point<f64>> = vec![
            Point::new(0.0, 10.0),
            Point::new(1.0, 1.0),
            Point::new(10.0, 0.0),
            Point::new(1.0, -1.0),
            Point::new(0.0, -10.0),
            Point::new(-1.0, -1.0),
            Point::new(-10.0, 0.0),
            Point::new(-1.0, 1.0),
            Point::new(0.0, 10.0),
        ];

        let geoms: Vec<Geometry<f64>> = v.into_iter().map(|p| p.into()).collect();

        let series = Series::from_geom_vec(&geoms).unwrap();
        let spatial_index: Result<SpatialIndex, PolarsError> = series.try_into();
        assert!(
            spatial_index.is_ok(),
            "Spatial index should be created correctly"
        );

        let spatial_index = spatial_index.unwrap();
        let in_envelope = spatial_index
            .r_tree
            .locate_in_envelope(&AABB::from_corners([0.0, 0.0], [20.0, 20.0]));
        let indexes: Vec<usize> = in_envelope.map(|node| node.index).collect();

        assert!(indexes.contains(&0));
        assert!(indexes.contains(&1));
        assert!(indexes.contains(&2));
        assert!(indexes.contains(&8));
        assert_eq!(indexes.len(), 4);
    }

    #[test]
    fn spatial_index_polygons() {
        let v: Vec<Polygon<f64>> = vec![
            polygon![
                (x:0.,y:0.),
                (x:10.,y:0.),
                (x:10.,y:10.),
                (x:0.,y:10.),
            ],
            polygon![
                (x:0.,y:0.),
                (x:-10.,y:0.),
                (x:-10.,y:-10.),
                (x:0.,y:-10.),
            ],
        ];

        let geoms: Vec<Geometry<f64>> = v.into_iter().map(|p| p.into()).collect();

        let series = Series::from_geom_vec(&geoms).unwrap();
        let spatial_index: Result<SpatialIndex, PolarsError> = series.try_into();
        assert!(
            spatial_index.is_ok(),
            "Spatial index should be created correctly"
        );

        let spatial_index = spatial_index.unwrap();
        let in_envelope = spatial_index
            .r_tree
            .locate_in_envelope(&AABB::from_corners([0.0, 0.0], [20.0, 20.0]));
        let indexes: Vec<usize> = in_envelope.map(|node| node.index).collect();
        assert!(indexes.contains(&0));
        assert_eq!(indexes.len(), 1);
    }
}
