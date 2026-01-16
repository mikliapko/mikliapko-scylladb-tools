use scylla::{Session, SessionBuilder};
use std::env;
use std::fs::OpenOptions;
use std::io::Write;
use std::time::Duration;
use chrono::Local;

const KEYSPACE_NAME: &str = "myapp";
const TABLE_NAME: &str = "comments";

fn log_print(message: &str, log_file: &mut std::fs::File) {
    let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S");
    let formatted = format!("[{}] {}", timestamp, message);
    println!("{}", formatted);
    writeln!(log_file, "{}", formatted).unwrap();
    log_file.flush().unwrap();
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
        eprintln!("Usage: cargo run -- <contact_point> <username> <password>");
        std::process::exit(1);
    }
    let contact_point = &args[1];
    let username = &args[2];
    let password = &args[3];

    let log_filename = format!(
        "select_monitor_{}.log",
        Local::now().format("%Y%m%d_%H%M%S")
    );
    let mut log_file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(&log_filename)
        .expect("Cannot open log file");

    log_print("Starting SELECT monitor script", &mut log_file);
    log_print(&format!("Log file: {}", log_filename), &mut log_file);

    let session: Session = SessionBuilder::new()
        .known_node(contact_point)
        .user(username, password)
        .build()
        .await
        .expect("Failed to connect to ScyllaDB");

    session
        .use_keyspace(KEYSPACE_NAME, false)
        .await
        .expect("Failed to set keyspace");

    loop {
        match session
            .query(format!("SELECT COUNT(*) FROM {}", TABLE_NAME), &[])
            .await
        {
            Ok(res) => {
                let count: i64 = res.rows
                    .as_ref()
                    .and_then(|rows| rows.get(0))
                    .and_then(|row| row.columns[0].as_ref())
                    .and_then(|val| val.as_bigint())
                    .unwrap_or(0);
                log_print(
                    &format!("SUCCESS: Row count in {}: {}", TABLE_NAME, count),
                    &mut log_file,
                );
            }
            Err(e) => {
                log_print(&format!("ERROR: {}", e), &mut log_file);
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}
