use lazy_static::lazy_static;  
  
// 添加常量定义  
pub const NATIVE_CONNECTION: &str = "nativeConnectionHandle";  
pub const NATIVE_TABLE: &str = "nativeTableHandle";  
  
// 扩展现有宏  
#[macro_export]  
macro_rules! ok_or_throw_with_return {  
    ($env:expr, $result:expr, $ret:expr) => {  
        match $result {  
            Ok(value) => value,  
            Err(err) => {  
                Error::from(err).throw(&mut $env);  
                return $ret;  
            }  
        }  
    };  
}  
  
#[macro_export]  
macro_rules! ok_or_throw_without_return {  
    ($env:expr, $result:expr) => {  
        match $result {  
            Ok(value) => value,  
            Err(err) => {  
                Error::from(err).throw(&mut $env);  
                return;  
            }  
        }  
    };  
}  
  
mod connection;  
mod table;  
pub mod error;  
mod ffi;  
mod traits;  
  
pub use error::{Error, Result};  
pub use connection::BlockingConnection;  
pub use table::BlockingTable;  
  
lazy_static! {  
    static ref RT: tokio::runtime::Runtime = tokio::runtime::Builder::new_multi_thread()  
        .enable_all()  
        .build()  
        .expect("Failed to create tokio runtime");  
}