#![deny(missing_docs)]

use failure::Error;

pub(crate) type Result<T> = std::result::Result<T, Error>;
