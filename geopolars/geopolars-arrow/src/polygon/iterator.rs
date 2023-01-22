use crate::geo_traits::PolygonTrait;
use crate::{GeometryArrayTrait, LineString};
use crate::{Polygon, PolygonArray};
use arrow2::bitmap::utils::{BitmapIter, ZipValidity};
use arrow2::trusted_len::TrustedLen;

/// Iterator of values of a [`PolygonArray`]
#[derive(Clone, Debug)]
pub struct PolygonArrayValuesIter<'a> {
    array: &'a PolygonArray,
    index: usize,
    end: usize,
}

impl<'a> PolygonArrayValuesIter<'a> {
    #[inline]
    pub fn new(array: &'a PolygonArray) -> Self {
        Self {
            array,
            index: 0,
            end: array.len(),
        }
    }
}

impl<'a> Iterator for PolygonArrayValuesIter<'a> {
    type Item = Polygon<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            return None;
        }
        let old = self.index;
        self.index += 1;
        Some(self.array.value(old))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.end - self.index, Some(self.end - self.index))
    }
}

unsafe impl<'a> TrustedLen for PolygonArrayValuesIter<'a> {}

impl<'a> DoubleEndedIterator for PolygonArrayValuesIter<'a> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            None
        } else {
            self.end -= 1;
            Some(self.array.value(self.end))
        }
    }
}

impl<'a> IntoIterator for &'a PolygonArray {
    type Item = Option<Polygon<'a>>;
    type IntoIter = ZipValidity<Polygon<'a>, PolygonArrayValuesIter<'a>, BitmapIter<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> PolygonArray {
    /// Returns an iterator of `Option<Point>`
    pub fn iter(&'a self) -> ZipValidity<Polygon<'a>, PolygonArrayValuesIter<'a>, BitmapIter<'a>> {
        ZipValidity::new_with_validity(PolygonArrayValuesIter::new(self), self.validity())
    }

    /// Returns an iterator of `Point`
    pub fn values_iter(&'a self) -> PolygonArrayValuesIter<'a> {
        PolygonArrayValuesIter::new(self)
    }
}

/// Iterator of values of a [`PointArray`]
#[derive(Clone, Debug)]
pub struct PolygonInteriorIterator<'a> {
    geom: &'a Polygon<'a>,
    index: usize,
    end: usize,
}

impl<'a> PolygonInteriorIterator<'a> {
    #[inline]
    pub fn new(geom: &'a Polygon<'a>) -> Self {
        Self {
            geom,
            index: 0,
            end: geom.num_interiors(),
        }
    }
}

impl<'a> Iterator for PolygonInteriorIterator<'a> {
    type Item = crate::LineString<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            return None;
        }
        let old = self.index;
        self.index += 1;
        self.geom.interior(old)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.end - self.index, Some(self.end - self.index))
    }
}

unsafe impl<'a> TrustedLen for PolygonInteriorIterator<'a> {}

impl<'a> DoubleEndedIterator for PolygonInteriorIterator<'a> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            None
        } else {
            self.end -= 1;
            self.geom.interior(self.end)
        }
    }
}

impl<'a> IntoIterator for &'a Polygon<'a> {
    type Item = LineString<'a>;
    type IntoIter = PolygonInteriorIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> Polygon<'a> {
    /// Returns an iterator of `Point`
    pub fn iter(&'a self) -> PolygonInteriorIterator<'a> {
        PolygonInteriorIterator::new(self)
    }
}
