#[macro_export]
macro_rules! create_logger {
    ($file:literal) => {
        use std::io::Write as _;

        static LOGGER_QUEUE: std::sync::LazyLock<
            tokio::sync::RwLock<std::collections::VecDeque<::transit_server::log::LogEntry>>,
        > = std::sync::LazyLock::new(
            || tokio::sync::RwLock::new(std::collections::VecDeque::new()),
        );
        static LOGGER: std::sync::LazyLock<tokio::sync::RwLock<::transit_server::log::Logger>> =
            std::sync::LazyLock::new(|| {
                tokio::sync::RwLock::new(::transit_server::log::Logger::new($file))
            });

        async fn logger_loop() {
            loop {
                while LOGGER_QUEUE.read().await.len() == 0 {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }

                while let Some(entry) = LOGGER_QUEUE.write().await.pop_front() {
                    println!("{:?}: {}", entry.sev, entry.content);
                    std::fs::OpenOptions::new()
                        .write(true)
                        .create(true)
                        .append(true)
                        .open(LOGGER.read().await.file_name)
                        .unwrap()
                        .write_fmt(format_args!("{:?}: {}\n", entry.sev, entry.content))
                        .unwrap_or_default(); // Silence failure to write to file error
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
