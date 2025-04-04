use std::net::AddrParseError;

use tokio::sync::mpsc::error::SendError;
use zip::result::ZipError;

use crate::shared::db_transit::ScheduleResponse;

#[derive(Debug)]
pub enum ScheduleError {
    SendError(SendError<ScheduleResponse>),
    ZipError(ZipError),
    ReqwestError(reqwest::Error),
    AddrParseError(AddrParseError),
    TransferError(tonic::transport::Error),

    RawError(String),
}

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
