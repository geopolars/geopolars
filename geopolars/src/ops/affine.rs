use crate::error::Result;
use crate::util::from_geom_vec;
use geo::algorithm::affine_ops::AffineTransform;
use geo::algorithm::bounding_rect::BoundingRect;
use geo::algorithm::centroid::Centroid;
use geo::Geometry;
use geo::{map_coords::MapCoords, Point};
use polars::prelude::Series;

use crate::util::iter_geom;

/// Used to express the origin for a given transform. Can be specified either be with reference to
/// the geometry being transformed (Centroid, Center) or some arbitrary point.
///
/// - Centroid: Use the centriod of each geometry in the series as the transform origin.
/// - Center: Use the center of each geometry in the series as the transform origin. The center is
///   defined as the center of the bounding box of the geometry
/// - Point: Define a single point to transform each geometry in the series about.
pub enum TransformOrigin {
    Centroid,
    Center,
    Point(Point),
}

pub(crate) fn affine_transform(
    series: &Series,
    matrix: impl Into<AffineTransform<f64>>,
) -> Result<Series> {
    affine_transform_wkb(series, matrix)
}

pub(crate) fn rotate(series: &Series, angle: f64, origin: TransformOrigin) -> Result<Series> {
    rotate_wkb(series, angle, origin)
}

pub(crate) fn scale(
    series: &Series,
    xfact: f64,
    yfact: f64,
    origin: TransformOrigin,
) -> Result<Series> {
    scale_wkb(series, xfact, yfact, origin)
}

pub(crate) fn skew(series: &Series, xs: f64, ys: f64, origin: TransformOrigin) -> Result<Series> {
    skew_wkb(series, xs, ys, origin)
}

fn affine_transform_wkb(
    series: &Series,
    matrix: impl Into<AffineTransform<f64>>,
) -> Result<Series> {
    let transform: AffineTransform<f64> = matrix.into();
    let output_vec: Vec<Geometry> = iter_geom(series)
        .map(|geom| geom.map_coords(|c| transform.apply(c)))
        .collect();

    from_geom_vec(&output_vec)
}

fn rotate_wkb(series: &Series, angle: f64, origin: TransformOrigin) -> Result<Series> {
    match origin {
        TransformOrigin::Centroid => {
            let rotated_geoms: Vec<Geometry<f64>> = iter_geom(series)
                .map(|geom| {
                    let centroid = geom.centroid().unwrap();
                    let transform = AffineTransform::rotate(angle, centroid);
                    geom.map_coords(|c| transform.apply(c))
                })
                .collect();
            from_geom_vec(&rotated_geoms)
        }
        TransformOrigin::Center => {
            let rotated_geoms: Vec<Geometry<f64>> = iter_geom(series)
                .map(|geom| {
                    let center = geom.bounding_rect().unwrap().center();
                    let transform = AffineTransform::rotate(angle, center);
                    geom.map_coords(|c| transform.apply(c))
                })
                .collect();
            from_geom_vec(&rotated_geoms)
        }
        TransformOrigin::Point(point) => {
            let transform = AffineTransform::rotate(angle, point);
            affine_transform_wkb(series, transform)
        }
    }
}

fn scale_wkb(series: &Series, xfact: f64, yfact: f64, origin: TransformOrigin) -> Result<Series> {
    match origin {
        TransformOrigin::Centroid => {
            let rotated_geoms: Vec<Geometry<f64>> = iter_geom(series)
                .map(|geom| {
                    let centroid = geom.centroid().unwrap();
                    let transform = AffineTransform::scale(xfact, yfact, centroid);
                    geom.map_coords(|c| transform.apply(c))
                })
                .collect();
            from_geom_vec(&rotated_geoms)
        }
        TransformOrigin::Center => {
            let rotated_geoms: Vec<Geometry<f64>> = iter_geom(series)
                .map(|geom| {
                    let center = geom.bounding_rect().unwrap().center();
                    let transform = AffineTransform::scale(xfact, yfact, center);
                    geom.map_coords(|c| transform.apply(c))
                })
                .collect();
            from_geom_vec(&rotated_geoms)
        }
        TransformOrigin::Point(point) => {
            let transform = AffineTransform::scale(xfact, yfact, point);
            affine_transform_wkb(series, transform)
        }
    }
}

fn skew_wkb(series: &Series, xs: f64, ys: f64, origin: TransformOrigin) -> Result<Series> {
    match origin {
        TransformOrigin::Centroid => {
            let rotated_geoms: Vec<Geometry<f64>> = iter_geom(series)
                .map(|geom| {
                    let centroid = geom.centroid().unwrap();
                    let transform = AffineTransform::skew(xs, ys, centroid);
                    geom.map_coords(|c| transform.apply(c))
                })
                .collect();
            from_geom_vec(&rotated_geoms)
        }
        TransformOrigin::Center => {
            let rotated_geoms: Vec<Geometry<f64>> = iter_geom(series)
                .map(|geom| {
                    let center = geom.bounding_rect().unwrap().center();
                    let transform = AffineTransform::skew(xs, ys, center);
                    geom.map_coords(|c| transform.apply(c))
                })
                .collect();
            from_geom_vec(&rotated_geoms)
        }
        TransformOrigin::Point(point) => {
            let transform = AffineTransform::skew(xs, ys, point);
            affine_transform_wkb(series, transform)
        }
    }
}
