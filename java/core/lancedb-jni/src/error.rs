use jni::JNIEnv;  
use jni::objects::JObject;  
use std::str::Utf8Error;
  
#[derive(Debug)]  
pub enum Error {  
    Lance(lance::Error),  
    LanceDB(lancedb::Error),  
    Jni(jni::errors::Error),  
    Arrow(String),
    InvalidInput(String),  
    TableNotFound(String),  
    Other(String),  
}  
  
impl Error {  
    pub fn throw(&self, env: &mut JNIEnv) {  
        let (exception_class, message) = match self {  
            Error::Lance(e) => ("com/lancedb/lancedb/LanceDBException", e.to_string()),  
            Error::LanceDB(e) => ("com/lancedb/lancedb/LanceDBException", e.to_string()),  
            Error::Jni(e) => ("java/lang/RuntimeException", e.to_string()),  
            Error::Arrow(msg) => ("com/lancedb/lancedb/LanceDBException", msg.clone()),
            Error::InvalidInput(msg) => ("com/lancedb/lancedb/InvalidArgumentException", msg.clone()),  
            Error::TableNotFound(name) => ("com/lancedb/lancedb/TableNotFoundException", format!("Table not found: {}", name)),  
            Error::Other(msg) => ("com/lancedb/lancedb/LanceDBException", msg.clone()),  
        };  
  
        if let Err(e) = env.throw_new(exception_class, &message) {  
            eprintln!("Failed to throw Java exception: {}", e);  
        }  
    }  
}  
  
impl From<lance::Error> for Error {  
    fn from(err: lance::Error) -> Self {  
        Error::Lance(err)  
    }  
}  
  
impl From<lancedb::Error> for Error {  
    fn from(err: lancedb::Error) -> Self {  
        Error::LanceDB(err)  
    }  
}  
  
impl From<jni::errors::Error> for Error {  
    fn from(err: jni::errors::Error) -> Self {  
        Error::Jni(err)  
    }  
}

impl From<Utf8Error> for Error {
    fn from(err: Utf8Error) -> Self {
        Error::Arrow(format!("UTF-8 error: {}", err))
    }
}
  
pub type Result<T> = std::result::Result<T, Error>;