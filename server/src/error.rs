use thiserror::Error;

#[derive(Error, Debug)]
pub enum ServerError {
    #[error("Unauthorized user")]
    UnAuth
}
