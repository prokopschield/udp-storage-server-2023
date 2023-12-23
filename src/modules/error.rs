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

pub fn to_error<T>(err: T) -> UssError
where
    T: std::fmt::Debug,
{
    UssError::DynamicError(format!("{:?}", err))
}

pub fn to_error_result<T, V>(err: T) -> UssResult<V>
where
    T: std::fmt::Debug,
{
    Err(to_error(err))
}
