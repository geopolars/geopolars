use crate::{
    geoseries::GeoSeries,
    spatial_index::SpatialIndex,
    util::{geom_at_index, Predicate},
};
use geo::Geometry;
use polars::prelude::{DataFrame, JoinType, NamedFrom, Result, Series};

pub trait GeoDataFrame {
    fn centroid(&self) -> Result<Series>;
    fn convex_hull(&self) -> Result<Series>;
    fn spatial_join(
        lhs: &DataFrame,
        rhs: &DataFrame,
        join_type: JoinType,
        predicate: Predicate,
        lsuffix: Option<&str>,
        rsuffix: Option<&str>,
    ) -> Result<DataFrame>;
}

impl GeoDataFrame for DataFrame {
    fn centroid(&self) -> Result<Series> {
        let geom_column = self.column("geometry")?;
        geom_column.centroid()
    }

    fn convex_hull(&self) -> Result<Series> {
        let geom_column = self.column("geometry")?;
        geom_column.convex_hull()
    }

    fn spatial_join(
        lhs: &DataFrame,
        rhs: &DataFrame,
        join_type: JoinType,
        predicate: Predicate,
        _lsuffix: Option<&str>,
        _rsuffix: Option<&str>,
    ) -> Result<DataFrame> {
        use geo::algorithm::{contains::Contains, intersects::Intersects};

        let lhs_geometry = lhs.column("geometry")?;
        let rhs_geometry = rhs.column("geometry")?;

        let spatial_index_left: SpatialIndex = lhs_geometry.try_into()?;
        let spatial_index_right: SpatialIndex = rhs_geometry.try_into()?;

        let potential_overlaps = spatial_index_left
            .r_tree
            .intersection_candidates_with_other_tree(&spatial_index_right.r_tree);

        let mut left_series: Vec<usize> = vec![];
        let mut right_series: Vec<usize> = vec![];

        for intersection in potential_overlaps {
            let (lhs_node, rhs_node) = intersection;

            let lhs_geom = geom_at_index(lhs_geometry, lhs_node.index)?;
            let rhs_geom = geom_at_index(rhs_geometry, rhs_node.index)?;

            let actual_hit = match (&lhs_geom, &rhs_geom, &predicate) {
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
                (
                    Geometry::Polygon(poly_lhs),
                    Geometry::Polygon(poly_rhs),
                    Predicate::Intersects,
                ) => poly_lhs.intersects(poly_rhs),

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
                (Geometry::MultiLineString(line), Geometry::Point(point), _) => {
                    line.contains(point)
                }
                (Geometry::Point(point), Geometry::MultiLineString(line), _) => {
                    line.contains(point)
                }
                _ => false,
            };

            if actual_hit {
                left_series.push(lhs_node.index);
                right_series.push(rhs_node.index);
            }
        }

        let lhs_index: Vec<u64> = (0..lhs.shape().0).map(|i| i as u64).collect();
        let rhs_index: Vec<u64> = (0..rhs.shape().0).map(|i| i as u64).collect();

        let lhs_index = Series::new("lhs_index", lhs_index);
        let rhs_index = Series::new("rhs_index", rhs_index);

        let lhs_join_series: Vec<u64> = left_series.iter().map(|i| *i as u64).collect();
        let rhs_join_series: Vec<u64> = right_series.iter().map(|i| *i as u64).collect();

        let lhs_join_series = Series::new("lhs_join", lhs_join_series);
        let rhs_join_series = Series::new("rhs_join", rhs_join_series);

        let join_df: DataFrame = DataFrame::new(vec![lhs_join_series, rhs_join_series])?;

        let lhs_with_index = lhs.hstack(&[lhs_index])?;
        let rhs_with_index = rhs.hstack(&[rhs_index])?;

        match join_type {
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
            JoinType::Outer => unimplemented!(),
            JoinType::Cross => unimplemented!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{geodataframe::GeoDataFrame, geoseries::GeoSeries, util::Predicate};
    use geo::{polygon, Geometry, Point};
    use polars::prelude::{DataFrame, JoinType, NamedFrom, Series};

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
        let point_df = DataFrame::new(vec![point_series, point_value_series]).unwrap();

        let polygons: Vec<Geometry<f64>> = vec![Geometry::Polygon(polygon![
            (x:0., y:0.),
            (x:20., y:0.),
            (x:20., y:20.),
            (x:0., y: 20.)
        ])];

        let polygon_series = Series::from_geom_vec(&polygons).unwrap();
        let polygon_label_series = Series::new("string_col", ["test"]);

        let polygon_df = DataFrame::new(vec![polygon_series, polygon_label_series]).unwrap();

        let inner_result: DataFrame = DataFrame::spatial_join(
            &point_df,
            &polygon_df,
            JoinType::Inner,
            Predicate::Contains,
            None,
            None,
        )
        .unwrap();
        let left_result: DataFrame = DataFrame::spatial_join(
            &point_df,
            &polygon_df,
            JoinType::Left,
            Predicate::Contains,
            None,
            None,
        )
        .unwrap();

        assert_eq!(inner_result.shape(), (2, 4));
        assert_eq!(left_result.shape(), (9, 4));

        println!("inner {}", inner_result);
        println!("left {}", left_result);
    }
}
