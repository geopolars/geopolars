use std::sync::Arc;

use geo::{
    prelude::BoundingRect, Geometry, Line, LineString, MultiLineString, MultiPoint, MultiPolygon,
    Point, Polygon,
};
use polars::error::ErrString;
use polars::prelude::{DataFrame, JoinType, NamedFrom, PolarsError, PolarsResult, Series};
use rstar::{RTree, RTreeObject, AABB};

use crate::util::{geom_at_index, iter_geom, Predicate};

pub struct SpatialJoinArgs<'a> {
    pub join_type: JoinType,
    pub predicate: Predicate,
    pub l_suffix: Option<&'a str>,
    pub r_suffix: Option<&'a str>,
    pub l_index: Option<Arc<SpatialIndex>>,
    pub r_index: Option<Arc<SpatialIndex>>,
}

impl<'a> Default for SpatialJoinArgs<'a> {
    fn default() -> Self {
        Self {
            join_type: JoinType::Inner,
            predicate: Predicate::Intersects,
            l_suffix: Some("_left"),
            r_suffix: Some("_right"),
            l_index: None,
            r_index: None,
        }
    }
}

pub fn spatial_join(
    lhs: &DataFrame,
    rhs: &DataFrame,
    options: SpatialJoinArgs,
) -> PolarsResult<DataFrame> {
    use geo::algorithm::{contains::Contains, intersects::Intersects};

    let lhs_geometry = lhs.column("geometry")?;
    let rhs_geometry = rhs.column("geometry")?;

    // If we where not given a left index, generate one on the fly
    let spatial_index_left: Arc<SpatialIndex> = options.l_index.unwrap_or_else(|| {
        let spatial_index_left: SpatialIndex = lhs_geometry
            .try_into()
            .map_err(|_| {
                PolarsError::ComputeError(ErrString::from(
                    "Failed to generate the spatial index for the left dataframe",
                ))
            })
            .unwrap();
        Arc::new(spatial_index_left)
    });

    // If we where not given a right index, generate one on the fly
    let spatial_index_right: Arc<SpatialIndex> = options.r_index.unwrap_or_else(|| {
        let spatial_index_right: SpatialIndex = rhs_geometry
            .try_into()
            .map_err(|_| {
                PolarsError::ComputeError(ErrString::from(
                    "Failed to generate the spatial index for the left dataframe",
                ))
            })
            .unwrap();
        Arc::new(spatial_index_right)
    });

    // Use the r-tree to generate potential overlaps between the two geometry sets
    let potential_overlaps = spatial_index_left
        .r_tree
        .intersection_candidates_with_other_tree(&spatial_index_right.r_tree);

    let mut left_series: Vec<usize> = vec![];
    let mut right_series: Vec<usize> = vec![];

    // Explicitly check which of the potential overlaps actually hit using the
    // provided geometry check
    for intersection in potential_overlaps {
        let (lhs_node, rhs_node) = intersection;

        let lhs_geom = geom_at_index(lhs_geometry, lhs_node.index)?;
        let rhs_geom = geom_at_index(rhs_geometry, rhs_node.index)?;

        let actual_hit = match (&lhs_geom, &rhs_geom, &options.predicate) {
            // Points and Polygons
            (Geometry::Point(point), Geometry::Polygon(poly), _) => poly.contains(point),
            (Geometry::Polygon(poly), Geometry::Point(point), _) => poly.contains(point),

            // Points and MultiPolygons
            (Geometry::Point(point), Geometry::MultiPolygon(poly), _) => poly.contains(point),
            (Geometry::MultiPolygon(poly), Geometry::Point(point), _) => poly.contains(point),

            // Polygon and Polygon
            (Geometry::Polygon(poly_lhs), Geometry::Polygon(poly_rhs), Predicate::Contains) => {
                poly_lhs.contains(poly_rhs)
            }
            (Geometry::Polygon(poly_lhs), Geometry::Polygon(poly_rhs), Predicate::Intersects) => {
                poly_lhs.intersects(poly_rhs)
            }

            // Multi Polygon and Polygon
            (
                Geometry::MultiPolygon(poly_lhs),
                Geometry::Polygon(poly_rhs),
                Predicate::Contains,
            ) => poly_lhs.contains(poly_rhs),
            (
                Geometry::MultiPolygon(poly_lhs),
                Geometry::Polygon(poly_rhs),
                Predicate::Intersects,
            ) => poly_lhs.intersects(poly_rhs),

            // Polygon and MultiPolygon
            (
                Geometry::Polygon(poly_lhs),
                Geometry::MultiPolygon(poly_rhs),
                Predicate::Intersects,
            ) => poly_lhs.intersects(poly_rhs),

            // Line and Point
            (Geometry::Line(line), Geometry::Point(point), _) => line.contains(point),
            (Geometry::Point(point), Geometry::Line(line), _) => line.contains(point),

            // LineString and Point
            (Geometry::LineString(line), Geometry::Point(point), _) => line.contains(point),
            (Geometry::Point(point), Geometry::LineString(line), _) => line.contains(point),

            // MultiLineString and Point
            (Geometry::MultiLineString(line), Geometry::Point(point), _) => line.contains(point),
            (Geometry::Point(point), Geometry::MultiLineString(line), _) => line.contains(point),
            _ => false,
        };

        if actual_hit {
            left_series.push(lhs_node.index);
            right_series.push(rhs_node.index);
        }
    }

    // Now we have two vecs with the alligned left right node indexes we perform a
    // join using polars existing code.
    let lhs_index: Vec<u64> = (0..lhs.shape().0).map(|i| i as u64).collect();
    let rhs_index: Vec<u64> = (0..rhs.shape().0).map(|i| i as u64).collect();

    let lhs_index = Series::new("lhs_index", lhs_index);
    let rhs_index = Series::new("rhs_index", rhs_index);

    let lhs_join_series: Vec<u64> = left_series.iter().map(|i| *i as u64).collect();
    let rhs_join_series: Vec<u64> = right_series.iter().map(|i| *i as u64).collect();

    let lhs_join_series = Series::new("lhs_join", lhs_join_series);
    let rhs_join_series = Series::new("rhs_join", rhs_join_series);

    let join_df: DataFrame = DataFrame::new(vec![lhs_join_series, rhs_join_series])?;

    let mut lhs_with_index = lhs.hstack(&[lhs_index])?;
    let mut rhs_with_index = rhs.hstack(&[rhs_index])?;

    // Apply the suffixes if specified
    if let Some(suffix) = options.l_suffix {
        lhs_with_index.get_columns_mut().iter_mut().for_each(|c| {
            if c.name() != "lhs_index" {
                c.rename(&format!("{}{}", c.name(), suffix));
            }
        });
    };

    // Apply the suffixes if specified
    if let Some(suffix) = options.r_suffix {
        rhs_with_index.get_columns_mut().iter_mut().for_each(|c| {
            if c.name() != "rhs_index" {
                c.rename(&format!("{}{}", c.name(), suffix));
            }
        });
    };

    // Finish up the join
    match options.join_type {
        JoinType::Inner => {
            let join_one = lhs_with_index.inner_join(&join_df, ["lhs_index"], ["lhs_join"])?;
            let join_two = join_one.inner_join(&rhs_with_index, ["rhs_join"], ["rhs_index"])?;
            let result = join_two.drop("lhs_index")?.drop("rhs_join")?;
            Ok(result)
        }
        JoinType::Left => {
            let join_one = lhs_with_index.left_join(&join_df, ["lhs_index"], ["lhs_join"])?;
            let join_two = join_one.left_join(&rhs_with_index, ["rhs_join"], ["rhs_index"])?;
            let result = join_two.drop("lhs_index")?.drop("rhs_join")?;
            Ok(result)
        }
        _ => Err(PolarsError::ComputeError(ErrString::from(
            "Failed to generate the spatial index for the left dataframe",
        ))),
    }
}

#[derive(Debug)]
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

#[derive(Debug)]
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

            _ => Err(PolarsError::ComputeError(ErrString::from(
                "Geometry type not currently supported for indexing",
            ))),
        }
    }
}

pub struct SpatialIndex {
    pub r_tree: RTree<TreeNode>,
}

impl SpatialIndex {}

impl<'a> TryFrom<&'a Series> for SpatialIndex {
    type Error = PolarsError;

    fn try_from(series: &'a Series) -> Result<Self, Self::Error> {
        let mut r_tree: RTree<TreeNode> = RTree::new();
        for (index, geom) in iter_geom(series).enumerate() {
            let node = TreeNode {
                index,
                envelope: geom.try_into()?,
            };
            r_tree.insert(node)
        }
        Ok(SpatialIndex { r_tree })
    }
}

impl TryFrom<Series> for SpatialIndex {
    type Error = PolarsError;

    fn try_from(series: Series) -> std::result::Result<Self, Self::Error> {
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
    use std::sync::Arc;

    use crate::{
        geoseries::GeoSeries,
        spatial_index::{spatial_join, SpatialIndex, SpatialJoinArgs},
    };
    use geo::{polygon, Geometry, Point, Polygon};
    use polars::prelude::{DataFrame, JoinType, NamedFrom, PolarsError, Series};
    use rstar::AABB;

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

    #[test]
    fn spatial_join_test() {
        let points: Vec<Point<f64>> = vec![
            Point::new(0.0, 10.0),
            Point::new(1.0, 1.0),
            Point::new(10.0, 1.0),
            Point::new(1.0, -1.0),
            Point::new(0.0, -10.0),
            Point::new(-1.0, -1.0),
            Point::new(-10.0, 0.0),
            Point::new(-1.0, 1.0),
            Point::new(0.0, 10.0),
        ];

        let geoms: Vec<Geometry<f64>> = points.into_iter().map(|p| p.into()).collect();

        let point_series = Series::from_geom_vec(&geoms).unwrap();
        let point_value_series = Series::new("point_values", [1., 2., 3., 4., 5., 6., 7., 8., 9.]);
        let point_df: DataFrame = DataFrame::new(vec![point_series, point_value_series]).unwrap();

        let polygons: Vec<Geometry<f64>> = vec![Geometry::Polygon(polygon![
            (x:0., y:0.),
            (x:20., y:0.),
            (x:20., y:20.),
            (x:0., y: 20.)
        ])];

        let polygon_series = Series::from_geom_vec(&polygons).unwrap();
        let polygon_label_series = Series::new("string_col", ["test"]);

        let polygon_df: DataFrame =
            DataFrame::new(vec![polygon_series, polygon_label_series]).unwrap();

        let inner_options = SpatialJoinArgs {
            join_type: JoinType::Inner,
            ..Default::default()
        };

        let inner_result: DataFrame = spatial_join(&point_df, &polygon_df, inner_options).unwrap();

        let left_options = SpatialJoinArgs {
            join_type: JoinType::Left,
            ..Default::default()
        };

        let left_result: DataFrame = spatial_join(&point_df, &polygon_df, left_options).unwrap();

        assert_eq!(inner_result.shape(), (2, 4));
        assert_eq!(left_result.shape(), (9, 4));

        println!("inner {}", inner_result);
        println!("left {}", left_result);
    }

    #[test]
    fn spatial_join_test_with_suffixes() {
        let points: Vec<Point<f64>> = vec![
            Point::new(0.0, 10.0),
            Point::new(1.0, 1.0),
            Point::new(10.0, 1.0),
            Point::new(1.0, -1.0),
            Point::new(0.0, -10.0),
            Point::new(-1.0, -1.0),
            Point::new(-10.0, 0.0),
            Point::new(-1.0, 1.0),
            Point::new(0.0, 10.0),
        ];

        let geoms: Vec<Geometry<f64>> = points.into_iter().map(|p| p.into()).collect();

        let point_series = Series::from_geom_vec(&geoms).unwrap();
        let point_value_series = Series::new("point_values", [1., 2., 3., 4., 5., 6., 7., 8., 9.]);
        let point_df: DataFrame = DataFrame::new(vec![point_series, point_value_series]).unwrap();

        let polygons: Vec<Geometry<f64>> = vec![Geometry::Polygon(polygon![
            (x:0., y:0.),
            (x:20., y:0.),
            (x:20., y:20.),
            (x:0., y: 20.)
        ])];

        let polygon_series = Series::from_geom_vec(&polygons).unwrap();
        let polygon_label_series = Series::new("string_col", ["test"]);

        let polygon_df: DataFrame =
            DataFrame::new(vec![polygon_series, polygon_label_series]).unwrap();

        let inner_options = SpatialJoinArgs {
            join_type: JoinType::Inner,
            l_suffix: Some("_left!"),
            r_suffix: Some("_right!"),
            ..Default::default()
        };

        let inner_result: DataFrame = spatial_join(&point_df, &polygon_df, inner_options).unwrap();

        let left_options = SpatialJoinArgs {
            join_type: JoinType::Left,
            ..Default::default()
        };

        let left_result: DataFrame = spatial_join(&point_df, &polygon_df, left_options).unwrap();

        assert_eq!(inner_result.shape(), (2, 4));
        assert_eq!(left_result.shape(), (9, 4));

        let col_names: Vec<String> = inner_result
            .get_columns()
            .iter()
            .map(|c| c.name().into())
            .collect();

        assert_eq!(
            col_names,
            vec![
                "geometry_left!",
                "point_values_left!",
                "geometry_right!",
                "string_col_right!"
            ]
        );

        println!("inner {}", inner_result);
        println!("left {}", left_result);
    }

    #[test]
    fn spatial_join_test_with_precomputed_indexes() {
        let points: Vec<Point<f64>> = vec![
            Point::new(0.0, 10.0),
            Point::new(1.0, 1.0),
            Point::new(10.0, 1.0),
            Point::new(1.0, -1.0),
            Point::new(0.0, -10.0),
            Point::new(-1.0, -1.0),
            Point::new(-10.0, 0.0),
            Point::new(-1.0, 1.0),
            Point::new(0.0, 10.0),
        ];

        let geoms: Vec<Geometry<f64>> = points.into_iter().map(|p| p.into()).collect();

        let point_series = Series::from_geom_vec(&geoms).unwrap();
        let point_value_series = Series::new("point_values", [1., 2., 3., 4., 5., 6., 7., 8., 9.]);
        let point_df: DataFrame = DataFrame::new(vec![point_series, point_value_series]).unwrap();

        let point_index: SpatialIndex = point_df
            .column("geometry")
            .expect("The dataframe to have a geometry column")
            .try_into()
            .expect("To be able to generate the point index");

        let polygons: Vec<Geometry<f64>> = vec![Geometry::Polygon(polygon![
            (x:0., y:0.),
            (x:20., y:0.),
            (x:20., y:20.),
            (x:0., y: 20.)
        ])];

        let polygon_series = Series::from_geom_vec(&polygons).unwrap();
        let polygon_label_series = Series::new("string_col", ["test"]);

        let polygon_df: DataFrame =
            DataFrame::new(vec![polygon_series, polygon_label_series]).unwrap();

        let polygon_index: SpatialIndex = polygon_df
            .column("geometry")
            .expect("The dataframe to have a geometry column")
            .try_into()
            .expect("To be able to generate the point index");

        let inner_options = SpatialJoinArgs {
            join_type: JoinType::Inner,
            l_index: Some(Arc::new(point_index)),
            r_index: Some(Arc::new(polygon_index)),
            ..Default::default()
        };

        let inner_result: DataFrame = spatial_join(&point_df, &polygon_df, inner_options).unwrap();

        let left_options = SpatialJoinArgs {
            join_type: JoinType::Left,
            ..Default::default()
        };

        let left_result: DataFrame = spatial_join(&point_df, &polygon_df, left_options).unwrap();

        assert_eq!(inner_result.shape(), (2, 4));
        assert_eq!(left_result.shape(), (9, 4));

        println!("inner {}", inner_result);
        println!("left {}", left_result);
    }
}
