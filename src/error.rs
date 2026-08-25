#[derive(thiserror::Error, Debug)]
pub enum DalError {
    #[error("missing configuration parameter `{0}`")]
    KeyNotFound(String),
    #[error("failed to open configuration file `{0}`")]
    ConfigFileNotFound(String),
    #[error("invalid configuration value for `{key}`: {reason}")]
    InvalidValue { key: String, reason: String },
    #[error("failed to open database: {0}")]
    DbOpen(#[from] rusqlite::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
