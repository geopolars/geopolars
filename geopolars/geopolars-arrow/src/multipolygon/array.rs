use crate::enum_::GeometryType;
use crate::error::GeoArrowError;
use crate::trait_::GeometryArray;
use arrow2::array::{Array, ListArray, PrimitiveArray, StructArray};
use arrow2::bitmap::utils::{BitmapIter, ZipValidity};
use arrow2::bitmap::Bitmap;
use arrow2::buffer::Buffer;
use arrow2::datatypes::{DataType, Field};
use arrow2::offset::OffsetsBuffer;
use geozero::{GeomProcessor, GeozeroGeometry};
use rstar::RTree;

use super::MutableMultiPolygonArray;

/// A [`GeometryArray`] semantically equivalent to `Vec<Option<MultiPolygon>>` using Arrow's
/// in-memory representation.
#[derive(Debug, Clone)]
pub struct MultiPolygonArray {
    /// Buffer of x coordinates
    x: Buffer<f64>,

    /// Buffer of y coordinates
    y: Buffer<f64>,

    /// Offsets into the polygon array where each geometry starts
    geom_offsets: OffsetsBuffer<i64>,

    /// Offsets into the ring array where each polygon starts
    polygon_offsets: OffsetsBuffer<i64>,

    /// Offsets into the coordinate array where each ring starts
    ring_offsets: OffsetsBuffer<i64>,

    /// Validity bitmap
    validity: Option<Bitmap>,
}

pub(super) fn check(
    x: &[f64],
    y: &[f64],
    validity_len: Option<usize>,
    geom_offsets: &OffsetsBuffer<i64>,
) -> Result<(), GeoArrowError> {
    // TODO: check geom offsets and ring_offsets?
    if validity_len.map_or(false, |len| len != geom_offsets.len()) {
        return Err(GeoArrowError::General(
            "validity mask length must match the number of values".to_string(),
        ));
    }

    if x.len() != y.len() {
        return Err(GeoArrowError::General(
            "x and y arrays must have the same length".to_string(),
        ));
    }
    Ok(())
}

impl MultiPolygonArray {
    /// Create a new MultiPolygonArray from parts
    /// # Implementation
    /// This function is `O(1)`.
    pub fn new(
        x: Buffer<f64>,
        y: Buffer<f64>,
        geom_offsets: OffsetsBuffer<i64>,
        polygon_offsets: OffsetsBuffer<i64>,
        ring_offsets: OffsetsBuffer<i64>,
        validity: Option<Bitmap>,
    ) -> Self {
        check(&x, &y, validity.as_ref().map(|v| v.len()), &geom_offsets).unwrap();
        Self {
            x,
            y,
            geom_offsets,
            polygon_offsets,
            ring_offsets,
            validity,
        }
    }

    /// Create a new MultiPolygonArray from parts
    /// # Implementation
    /// This function is `O(1)`.
    pub fn try_new(
        x: Buffer<f64>,
        y: Buffer<f64>,
        geom_offsets: OffsetsBuffer<i64>,
        polygon_offsets: OffsetsBuffer<i64>,
        ring_offsets: OffsetsBuffer<i64>,
        validity: Option<Bitmap>,
    ) -> Result<Self, GeoArrowError> {
        check(&x, &y, validity.as_ref().map(|v| v.len()), &geom_offsets)?;
        Ok(Self {
            x,
            y,
            geom_offsets,
            polygon_offsets,
            ring_offsets,
            validity,
        })
    }

    /// Returns the number of geometries in this array
    #[inline]
    pub fn len(&self) -> usize {
        self.geom_offsets.len()
    }

    /// Returns true if the array is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the optional validity.
    #[inline]
    pub fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    /// Returns a clone of this [`PrimitiveArray`] sliced by an offset and length.
    /// # Implementation
    /// This operation is `O(1)` as it amounts to increase two ref counts.
    /// # Examples
    /// ```
    /// use arrow2::array::PrimitiveArray;
    ///
    /// let array = PrimitiveArray::from_vec(vec![1, 2, 3]);
    /// assert_eq!(format!("{:?}", array), "Int32[1, 2, 3]");
    /// let sliced = array.slice(1, 1);
    /// assert_eq!(format!("{:?}", sliced), "Int32[2]");
    /// // note: `sliced` and `array` share the same memory region.
    /// ```
    /// # Panic
    /// This function panics iff `offset + length > self.len()`.
    #[inline]
    #[must_use]
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        unsafe { self.slice_unchecked(offset, length) }
    }

    /// Returns a clone of this [`PrimitiveArray`] sliced by an offset and length.
    /// # Implementation
    /// This operation is `O(1)` as it amounts to increase two ref counts.
    /// # Safety
    /// The caller must ensure that `offset + length <= self.len()`.
    #[inline]
    #[must_use]
    pub unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Self {
        let validity = self
            .validity
            .clone()
            .map(|bitmap| bitmap.slice_unchecked(offset, length))
            .and_then(|bitmap| (bitmap.unset_bits() > 0).then_some(bitmap));

        let geom_offsets = self
            .geom_offsets
            .clone()
            .slice_unchecked(offset, length + 1);

        Self {
            x: self.x.clone(),
            y: self.y.clone(),
            geom_offsets,
            polygon_offsets: self.polygon_offsets.clone(),
            ring_offsets: self.ring_offsets.clone(),
            validity,
        }
    }
}

// Implement geometry accessors
impl MultiPolygonArray {
    pub fn value(&self, i: usize) -> crate::MultiPolygon {
        crate::MultiPolygon {
            x: &self.x,
            y: &self.y,
            geom_offsets: &self.geom_offsets,
            polygon_offsets: &self.polygon_offsets,
            ring_offsets: &self.ring_offsets,
            geom_index: i,
        }
    }

    pub fn get(&self, i: usize) -> Option<crate::MultiPolygon> {
        if self.is_null(i) {
            return None;
        }

        Some(self.value(i))
    }

    pub fn iter_values(&self) -> impl Iterator<Item = crate::MultiPolygon> + '_ {
        (0..self.len()).map(|i| self.value(i))
    }

    pub fn iter(
        &self,
    ) -> ZipValidity<crate::MultiPolygon, impl Iterator<Item = crate::MultiPolygon> + '_, BitmapIter>
    {
        ZipValidity::new_with_validity(self.iter_values(), self.validity())
    }

    // TODO: Need to test this
    /// Returns the value at slot `i` as a geo object.
    pub fn value_as_geo(&self, i: usize) -> geo::MultiPolygon {
        self.value(i).into()
    }

    /// Gets the value at slot `i` as a geo object, additionally checking the validity bitmap
    pub fn get_as_geo(&self, i: usize) -> Option<geo::MultiPolygon> {
        if self.is_null(i) {
            return None;
        }

        Some(self.value_as_geo(i))
    }

    /// Iterator over geo Geometry objects, not looking at validity
    pub fn iter_geo_values(&self) -> impl Iterator<Item = geo::MultiPolygon> + '_ {
        (0..self.len()).map(|i| self.value_as_geo(i))
    }

    /// Iterator over geo Geometry objects, taking into account validity
    pub fn iter_geo(
        &self,
    ) -> ZipValidity<geo::MultiPolygon, impl Iterator<Item = geo::MultiPolygon> + '_, BitmapIter>
    {
        ZipValidity::new_with_validity(self.iter_geo_values(), self.validity())
    }

    // GEOS from not implemented for MultiLineString I suppose
    //
    // /// Returns the value at slot `i` as a GEOS geometry.
    // #[cfg(feature = "geos")]
    // pub fn value_as_geos(&self, i: usize) -> geos::Geometry {
    //     (&self.value_as_geo(i)).try_into().unwrap()
    // }

    // /// Gets the value at slot `i` as a GEOS geometry, additionally checking the validity bitmap
    // #[cfg(feature = "geos")]
    // pub fn get_as_geos(&self, i: usize) -> Option<geos::Geometry> {
    //     if self.is_null(i) {
    //         return None;
    //     }

    //     self.get_as_geo(i).as_ref().map(|g| g.try_into().unwrap())
    // }

    // /// Iterator over GEOS geometry objects
    // #[cfg(feature = "geos")]
    // pub fn iter_geos_values(&self) -> impl Iterator<Item = geos::Geometry> + '_ {
    //     (0..self.len()).map(|i| self.value_as_geos(i))
    // }

    // /// Iterator over GEOS geometry objects, taking validity into account
    // #[cfg(feature = "geos")]
    // pub fn iter_geos(
    //     &self,
    // ) -> ZipValidity<geos::Geometry, impl Iterator<Item = geos::Geometry> + '_, BitmapIter> {
    //     ZipValidity::new_with_validity(self.iter_geos_values(), self.validity())
    // }

    pub fn into_arrow(self) -> ListArray<i64> {
        // Data type
        let coord_field_x = Field::new("x", DataType::Float64, false);
        let coord_field_y = Field::new("y", DataType::Float64, false);
        let struct_data_type = DataType::Struct(vec![coord_field_x, coord_field_y]);
        let inner_list_data_type = DataType::LargeList(Box::new(Field::new(
            "vertices",
            struct_data_type.clone(),
            false,
        )));
        let middle_list_data_type = DataType::LargeList(Box::new(Field::new(
            "rings",
            inner_list_data_type.clone(),
            false,
        )));
        let outer_list_data_type = DataType::LargeList(Box::new(Field::new(
            "polygons",
            middle_list_data_type.clone(),
            true,
        )));

        // Validity
        let validity: Option<Bitmap> = if let Some(validity) = self.validity {
            validity.into()
        } else {
            None
        };

        // Array data
        let array_x = PrimitiveArray::new(DataType::Float64, self.x, None).boxed();
        let array_y = PrimitiveArray::new(DataType::Float64, self.y, None).boxed();

        // Coord struct array
        let coord_array = StructArray::new(struct_data_type, vec![array_x, array_y], None).boxed();

        // Rings array
        let inner_list_array =
            ListArray::new(inner_list_data_type, self.ring_offsets, coord_array, None).boxed();

        // Polygons array
        let middle_list_array = ListArray::new(
            middle_list_data_type,
            self.polygon_offsets,
            inner_list_array,
            None,
        )
        .boxed();

        // Geometry array
        ListArray::new(
            outer_list_data_type,
            self.geom_offsets,
            middle_list_array,
            validity,
        )
    }

    /// Build a spatial index containing this array's geometries
    pub fn rstar_tree(&self) -> RTree<crate::MultiPolygon> {
        let mut tree = RTree::new();
        self.iter().flatten().for_each(|geom| tree.insert(geom));
        tree
    }
}

impl TryFrom<ListArray<i64>> for MultiPolygonArray {
    type Error = GeoArrowError;

    fn try_from(value: ListArray<i64>) -> Result<Self, Self::Error> {
        let geom_offsets = value.offsets();
        let validity = value.validity();

        let first_level_dyn_array = value.values();
        let first_level_array = first_level_dyn_array
            .as_any()
            .downcast_ref::<ListArray<i64>>()
            .unwrap();

        let polygon_offsets = first_level_array.offsets();
        let second_level_dyn_array = first_level_array.values();
        let second_level_array = second_level_dyn_array
            .as_any()
            .downcast_ref::<ListArray<i64>>()
            .unwrap();

        let ring_offsets = second_level_array.offsets();
        let coords_dyn_array = second_level_array.values();
        let coords_array = coords_dyn_array
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        let x_array_values = coords_array.values()[0]
            .as_any()
            .downcast_ref::<PrimitiveArray<f64>>()
            .unwrap();
        let y_array_values = coords_array.values()[1]
            .as_any()
            .downcast_ref::<PrimitiveArray<f64>>()
            .unwrap();

        Ok(Self::new(
            x_array_values.values().clone(),
            y_array_values.values().clone(),
            geom_offsets.clone(),
            polygon_offsets.clone(),
            ring_offsets.clone(),
            validity.cloned(),
        ))
    }
}

impl TryFrom<Box<dyn Array>> for MultiPolygonArray {
    type Error = GeoArrowError;

    fn try_from(value: Box<dyn Array>) -> Result<Self, Self::Error> {
        let arr = value.as_any().downcast_ref::<ListArray<i64>>().unwrap();
        arr.clone().try_into()
    }
}

impl GeometryArray for MultiPolygonArray {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    #[inline]
    fn len(&self) -> usize {
        self.len()
    }

    #[inline]
    fn geometry_type(&self) -> GeometryType {
        GeometryType::WKB
    }

    fn validity(&self) -> Option<&Bitmap> {
        self.validity()
    }

    fn slice(&self, offset: usize, length: usize) -> Box<dyn GeometryArray> {
        Box::new(self.slice(offset, length))
    }

    unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Box<dyn GeometryArray> {
        Box::new(self.slice_unchecked(offset, length))
    }

    fn to_boxed(&self) -> Box<dyn GeometryArray> {
        Box::new(self.clone())
    }
}

impl From<Vec<Option<geo::MultiPolygon>>> for MultiPolygonArray {
    fn from(other: Vec<Option<geo::MultiPolygon>>) -> Self {
        let mut_arr: MutableMultiPolygonArray = other.into();
        mut_arr.into()
    }
}

impl From<Vec<geo::MultiPolygon>> for MultiPolygonArray {
    fn from(other: Vec<geo::MultiPolygon>) -> Self {
        let mut_arr: MutableMultiPolygonArray = other.into();
        mut_arr.into()
    }
}

impl GeozeroGeometry for MultiPolygonArray {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        let num_geometries = self.len();
        processor.geometrycollection_begin(num_geometries, 0)?;

        for geom_idx in 0..num_geometries {
            let (start_polygon_idx, end_polygon_idx) = self.geom_offsets.start_end(geom_idx);

            processor.multipolygon_begin(end_polygon_idx - start_polygon_idx, geom_idx)?;

            for polygon_idx in start_polygon_idx..end_polygon_idx {
                let (start_ring_idx, end_ring_idx) = self.polygon_offsets.start_end(polygon_idx);

                processor.polygon_begin(
                    false,
                    end_ring_idx - start_ring_idx,
                    polygon_idx - start_polygon_idx,
                )?;

                for ring_idx in start_ring_idx..end_ring_idx {
                    let (start_coord_idx, end_coord_idx) = self.ring_offsets.start_end(ring_idx);

                    processor.linestring_begin(
                        false,
                        end_coord_idx - start_coord_idx,
                        ring_idx - start_ring_idx,
                    )?;

                    for coord_idx in start_coord_idx..end_coord_idx {
                        processor.xy(
                            self.x[coord_idx],
                            self.y[coord_idx],
                            coord_idx - start_coord_idx,
                        )?;
                    }

                    processor.linestring_end(false, ring_idx - start_ring_idx)?;
                }

                processor.polygon_end(false, polygon_idx - start_polygon_idx)?;
            }

            processor.multipolygon_end(geom_idx)?;
        }

        processor.geometrycollection_end(num_geometries - 1)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow2::offset::Offsets;
    use geo::{polygon, MultiPolygon};
    use geozero::ToWkt;

    fn mp0() -> MultiPolygon {
        MultiPolygon::new(vec![
            polygon![
                (x: -111., y: 45.),
                (x: -111., y: 41.),
                (x: -104., y: 41.),
                (x: -104., y: 45.),
            ],
            polygon!(
                exterior: [
                    (x: -111., y: 45.),
                    (x: -111., y: 41.),
                    (x: -104., y: 41.),
                    (x: -104., y: 45.),
                ],
                interiors: [
                    [
                        (x: -110., y: 44.),
                        (x: -110., y: 42.),
                        (x: -105., y: 42.),
                        (x: -105., y: 44.),
                    ],
                ],
            ),
        ])
    }

    fn mp1() -> MultiPolygon {
        MultiPolygon::new(vec![
            polygon![
                (x: -111., y: 45.),
                (x: -111., y: 41.),
                (x: -104., y: 41.),
                (x: -104., y: 45.),
            ],
            polygon![
                (x: -110., y: 44.),
                (x: -110., y: 42.),
                (x: -105., y: 42.),
                (x: -105., y: 44.),
            ],
        ])
    }

    #[test]
    fn geo_roundtrip_accurate() {
        let arr: MultiPolygonArray = vec![mp0(), mp1()].into();
        assert_eq!(arr.value_as_geo(0), mp0());
        assert_eq!(arr.value_as_geo(1), mp1());
    }

    #[test]
    fn geo_roundtrip_accurate_option_vec() {
        let arr: MultiPolygonArray = vec![Some(mp0()), Some(mp1()), None].into();
        assert_eq!(arr.get_as_geo(0), Some(mp0()));
        assert_eq!(arr.get_as_geo(1), Some(mp1()));
        assert_eq!(arr.get_as_geo(2), None);
    }

    #[test]
    fn geozero_process_geom() -> geozero::error::Result<()> {
        let arr: MultiPolygonArray = vec![mp0(), mp1()].into();
        let wkt = arr.to_wkt()?;
        let expected = "GEOMETRYCOLLECTION(MULTIPOLYGON(((-111 45,-111 41,-104 41,-104 45,-111 45)),((-111 45,-111 41,-104 41,-104 45,-111 45),(-110 44,-110 42,-105 42,-105 44,-110 44))),MULTIPOLYGON(((-111 45,-111 41,-104 41,-104 45,-111 45)),((-110 44,-110 42,-105 42,-105 44,-110 44))))";
        assert_eq!(wkt, expected);
        Ok(())
    }

    #[test]
    fn slice() {
        let arr: MultiPolygonArray = vec![mp0(), mp1()].into();
        let sliced = arr.slice(1, 1);
        assert_eq!(sliced.len(), 1);
        assert_eq!(sliced.get_as_geo(0), Some(mp1()));
    }

    /// This data is taken from the first 9 rows of a new zealand building polygons file found at
    /// https://storage.googleapis.com/open-geodata/linz-examples/nz-building-outlines.parquet as
    /// of 2023-01-22
    #[test]
    fn rstar_integration() {
        let geom_offsets: Offsets<i64> =
            unsafe { Offsets::new_unchecked(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]) };
        let polygon_offsets: Offsets<i64> =
            unsafe { Offsets::new_unchecked(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]) };
        let ring_offsets: Offsets<i64> =
            unsafe { Offsets::new_unchecked(vec![0, 5, 10, 15, 20, 33, 38, 44, 49, 54, 60]) };

        let x = vec![
            1890386.1694890875,
            1890380.8937914434,
            1890378.139244447,
            1890383.4149420918,
            1890386.1694890875,
            1796386.7560675316,
            1796394.0530763683,
            1796392.6357543375,
            1796385.338745501,
            1796386.7560675316,
            1811431.727472759,
            1811437.1832724523,
            1811447.5803616669,
            1811442.343218308,
            1811431.727472759,
            1776387.291795326,
            1776394.4101653676,
            1776390.8981247419,
            1776383.7797547,
            1776387.291795326,
            1776318.3750832868,
            1776321.880712318,
            1776332.2000606267,
            1776329.8129422984,
            1776332.4130679918,
            1776331.1026064188,
            1776332.3406099626,
            1776331.189385827,
            1776325.9398052464,
            1776326.396350046,
            1776320.699624227,
            1776321.5862544328,
            1776318.3750832868,
            1818262.417014431,
            1818267.6501438397,
            1818260.7209490805,
            1818255.4878196719,
            1818262.417014431,
            1818162.055510153,
            1818172.3522828324,
            1818149.2445113712,
            1818139.1191835117,
            1818139.0196774134,
            1818162.055510153,
            1818431.183652207,
            1818435.6053153735,
            1818437.9022725022,
            1818433.4806093357,
            1818431.183652207,
            1818162.8216453018,
            1818165.7085734177,
            1818177.451098379,
            1818174.564170263,
            1818162.8216453018,
            1818325.818513726,
            1818302.9283248608,
            1818293.0897133811,
            1818340.3134423152,
            1818350.7122521787,
            1818325.818513726,
            1818360.3905021998,
            1818378.051651615,
            1818382.842858081,
            1818381.622839916,
            1818366.9485025338,
            1818360.3905021998,
            1818059.2416859225,
            1818061.6131321345,
            1818064.0811138835,
            1818061.7096676712,
            1818059.2416859225,
        ];

        let y = vec![
            5501282.718158767,
            5501274.906068024,
            5501276.766281581,
            5501284.578372323,
            5501282.718158767,
            5560662.125554955,
            5560659.936452304,
            5560655.2120455345,
            5560657.401148185,
            5560662.125554955,
            5632559.044094654,
            5632566.071904428,
            5632558.000479907,
            5632551.254329139,
            5632559.044094654,
            5576785.711756392,
            5576781.765703435,
            5576775.430257599,
            5576779.376310556,
            5576785.711756392,
            5576879.268873175,
            5576890.864415365,
            5576887.744612391,
            5576879.848759452,
            5576879.062674941,
            5576874.728071271,
            5576874.353791131,
            5576870.545895912,
            5576872.132978412,
            5576873.643088134,
            5576875.365354079,
            5576878.298053991,
            5576879.268873175,
            5544984.559394175,
            5544975.256053005,
            5544971.358380953,
            5544980.661722123,
            5544984.559394175,
            5544096.615767146,
            5544078.547342337,
            5544065.500097226,
            5544083.432906809,
            5544083.609140501,
            5544096.615767146,
            5544343.385507157,
            5544346.1339450935,
            5544342.438620343,
            5544339.690182406,
            5544343.385507157,
            5544769.715283917,
            5544778.211100944,
            5544774.2209225595,
            5544765.725105532,
            5544769.715283917,
            5544199.1567608975,
            5544187.4461706225,
            5544204.638895733,
            5544231.579671562,
            5544213.381754294,
            5544199.1567608975,
            5544147.679728531,
            5544158.263432041,
            5544150.268289962,
            5544145.017569979,
            5544136.446718057,
            5544147.679728531,
            5544447.607808408,
            5544448.9295981005,
            5544444.501748492,
            5544443.1799588,
            5544447.607808408,
        ];

        let mut_arr = MutableMultiPolygonArray::try_new(
            x,
            y,
            geom_offsets,
            polygon_offsets,
            ring_offsets,
            None,
        )
        .unwrap();
        let arr: MultiPolygonArray = mut_arr.into();
        let _tree = arr.rstar_tree();
    }
}
