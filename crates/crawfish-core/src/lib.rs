pub mod action;
pub mod config;
pub mod contract;
pub mod governance;
pub mod lifecycle;
pub mod traits;

pub use action::*;
pub use config::*;
pub use contract::*;
pub use governance::*;
pub use lifecycle::*;
pub use traits::*;

use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_timestamp() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .to_string()
}
