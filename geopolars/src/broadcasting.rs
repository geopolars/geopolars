use polars::{
    prelude::AnyValue,
    series::{Series, SeriesIter},
};

pub enum BroadcastableParameter<'a, T>
where
    T: Into<AnyValue<'a>>,
{
    Val(T),
    Series(&'a Series),
}

impl<'a, T> IntoIterator for BroadcastableParameter<'a, T>
where
    T: Into<AnyValue<'a>>,
{
    type Item = AnyValue<'a>;
    type IntoIter = BroadcastIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            BroadcastableParameter::Series(s) => {
                let series_iterator = s.iter();
                BroadcastIter::Series(series_iterator)
            }
            BroadcastableParameter::Val(v) => BroadcastIter::Value(v.into()),
        }
    }
}

pub enum BroadcastIter<'a> {
    Series(SeriesIter<'a>),
    Value(AnyValue<'a>),
}

impl<'a> Iterator for BroadcastIter<'a> {
    type Item = AnyValue<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BroadcastIter::Series(s) => s.next(),
            BroadcastIter::Value(v) => Some(v.to_owned()),
        }
    }
}

impl<'a> From<f64> for BroadcastableParameter<'a, f64> {
    fn from(val: f64) -> BroadcastableParameter<'a, f64> {
        BroadcastableParameter::Val(val)
    }
}

impl<'a> From<i64> for BroadcastableParameter<'a, i64> {
    fn from(val: i64) -> BroadcastableParameter<'a, i64> {
        BroadcastableParameter::Val(val)
    }
}

impl<'a> From<u64> for BroadcastableParameter<'a, u64> {
    fn from(val: u64) -> BroadcastableParameter<'a, u64> {
        BroadcastableParameter::Val(val)
    }
}

impl<'a, T> From<&'a Series> for BroadcastableParameter<'a, T>
where
    T: Into<AnyValue<'a>>,
{
    fn from(val: &'a Series) -> BroadcastableParameter<T> {
        BroadcastableParameter::Series(val)
    }
}
