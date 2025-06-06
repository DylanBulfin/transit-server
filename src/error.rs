use std::{fmt::Display, net::AddrParseError};

use tokio::sync::mpsc::error::SendError;
use zip::result::ZipError;

use crate::server::db_transit::ScheduleResponse;

#[derive(Debug)]
pub enum ScheduleError {
    SendError(SendError<ScheduleResponse>),
    ZipError(ZipError),
    ReqwestError(reqwest::Error),
    AddrParseError(AddrParseError),
    TransferError(tonic::transport::Error),
    IOError(std::io::Error),
    HyperError(hyper::Error),
    HyperHttpError(hyper::http::Error),
    HyperLegacyError(hyper_util::client::legacy::Error),

    RawError(String),
}

impl Display for ScheduleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScheduleError::SendError(send_error) => f.write_fmt(format_args!("{}", send_error)),
            ScheduleError::ZipError(zip_error) => f.write_fmt(format_args!("{}", zip_error)),
            ScheduleError::ReqwestError(error) => f.write_fmt(format_args!("{}", error)),
            ScheduleError::AddrParseError(addr_parse_error) => {
                f.write_fmt(format_args!("{}", addr_parse_error))
            }
            ScheduleError::TransferError(error) => f.write_fmt(format_args!("{}", error)),
            ScheduleError::IOError(error) => f.write_fmt(format_args!("{}", error)),
            ScheduleError::HyperError(error) => f.write_fmt(format_args!("{}", error)),
            ScheduleError::HyperHttpError(error) => f.write_fmt(format_args!("{}", error)),
            ScheduleError::HyperLegacyError(error) => f.write_fmt(format_args!("{}", error)),
            ScheduleError::RawError(s) => f.write_str(s),
        }
    }
}

impl std::error::Error for ScheduleError {}

impl From<SendError<ScheduleResponse>> for ScheduleError {
    fn from(value: SendError<ScheduleResponse>) -> Self {
        Self::SendError(value)
    }
}

impl From<ZipError> for ScheduleError {
    fn from(value: ZipError) -> Self {
        Self::ZipError(value)
    }
}

impl From<reqwest::Error> for ScheduleError {
    fn from(value: reqwest::Error) -> Self {
        Self::ReqwestError(value)
    }
}

impl From<AddrParseError> for ScheduleError {
    fn from(value: AddrParseError) -> Self {
        Self::AddrParseError(value)
    }
}

impl From<tonic::transport::Error> for ScheduleError {
    fn from(value: tonic::transport::Error) -> Self {
        Self::TransferError(value)
    }
}

impl From<String> for ScheduleError {
    fn from(value: String) -> Self {
        Self::RawError(value)
    }
}

impl From<&str> for ScheduleError {
    fn from(value: &str) -> Self {
        Self::RawError(value.to_owned())
    }
}

impl From<std::io::Error> for ScheduleError {
    fn from(value: std::io::Error) -> Self {
        Self::IOError(value)
    }
}

impl From<hyper::Error> for ScheduleError {
    fn from(value: hyper::Error) -> Self {
        Self::HyperError(value)
    }
}

impl From<hyper::http::Error> for ScheduleError {
    fn from(value: hyper::http::Error) -> Self {
        Self::HyperHttpError(value)
    }
}

impl From<hyper_util::client::legacy::Error> for ScheduleError {
    fn from(value: hyper_util::client::legacy::Error) -> Self {
        Self::HyperLegacyError(value)
    }
}
