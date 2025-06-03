use std::fmt::Display;

#[macro_export]
macro_rules! create_logger {
    ($file:literal) => {
        use std::io::Write as _;

        static LOGGER_QUEUE: std::sync::LazyLock<
            tokio::sync::RwLock<std::collections::VecDeque<::transit_server::log::LogEntry>>,
        > = std::sync::LazyLock::new(
            || tokio::sync::RwLock::new(std::collections::VecDeque::new()),
        );
        const LOGGER_FILE: &'static str = $file;
        static LOGGER: std::sync::LazyLock<tokio::sync::RwLock<::transit_server::log::Logger>> =
            std::sync::LazyLock::new(|| {
                tokio::sync::RwLock::new(::transit_server::log::Logger::new(LOGGER_FILE))
            });

        async fn logger_loop() -> Result<(), ScheduleError> {
            loop {
                while LOGGER_QUEUE.read().await.len() == 0 {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }

                while let Some(entry) = LOGGER_QUEUE.write().await.pop_front() {
                    let s: String = format!(
                        "{}: [{}] - {}\n",
                        ::transit_server::shared::get_nyc_datetime().time(),
                        entry.sev,
                        entry.content
                    );

                    let r1 = std::io::stdout().write_fmt(format_args!("{}", s));
                    let r2 = std::fs::OpenOptions::new()
                        .write(true)
                        .create(true)
                        .append(true)
                        .open(LOGGER.read().await.file_name)
                        .unwrap()
                        .write_fmt(format_args!("{}", s));

                    match (r1, r2) {
                        (Ok(_), Ok(_)) => (),
                        (Err(e), Ok(_)) => {
                            eprintln!("Error logging to stdout: {}", e);
                        }
                        (Ok(_), Err(e)) => {
                            eprintln!("Error logging to file: {}", e);
                        }
                        // Throw stdout error up the chain, this is unrecoverable
                        (Err(e), Err(_e2)) => Err(e)?,
                    }

                    // Leave time for new logs to be added
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }

        async fn add_log(entry: ::transit_server::log::LogEntry) {
            LOGGER_QUEUE.write().await.push_back(entry)
        }

        async fn debug(content: String) {
            add_log(::transit_server::log::LogEntry::new(
                content,
                ::transit_server::log::Severity::Debug,
            ))
            .await;
        }
        async fn info(content: String) {
            add_log(::transit_server::log::LogEntry::new(
                content,
                ::transit_server::log::Severity::Information,
            ))
            .await;
        }
        async fn warn(content: String) {
            add_log(::transit_server::log::LogEntry::new(
                content,
                ::transit_server::log::Severity::Warning,
            ))
            .await;
        }
        async fn error(content: String) {
            add_log(::transit_server::log::LogEntry::new(
                content,
                ::transit_server::log::Severity::Error,
            ))
            .await;
        }
        async fn critical(content: String) {
            add_log(::transit_server::log::LogEntry::new(
                content,
                ::transit_server::log::Severity::Critical,
            ))
            .await;
        }
    };
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub enum Severity {
    Debug,
    Information,
    Warning,
    Error,
    Critical,
}

impl Display for Severity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Severity::Debug => "DBG",
            Severity::Information => "INFO",
            Severity::Warning => "WARN",
            Severity::Error => "ERR",
            Severity::Critical => "CRIT",
        };

        f.write_str(s)
    }
}

#[derive(Debug)]
pub struct LogEntry {
    pub content: String,
    pub sev: Severity,
}

impl LogEntry {
    pub fn new(content: String, sev: Severity) -> Self {
        Self { content, sev }
    }
}

pub struct Logger {
    pub file_name: &'static str,
}

impl Logger {
    pub fn new(file_name: &'static str) -> Self {
        Self { file_name }
    }
}
