#[derive(Debug)]
pub enum UssError {
    UnknownError,
    StaticError(&'static str),
    DynamicError(String),
    IoProblem,
    MmapProblem,
    MutexPoison,
}

pub type UssResult<T> = Result<T, UssError>;

pub fn to_error<T, V>(err: T) -> UssResult<V>
where
    T: std::fmt::Debug,
{
    Err(UssError::DynamicError(format!("{:?}", err)))
}
