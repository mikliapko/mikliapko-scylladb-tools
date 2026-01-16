#!/usr/bin/env python3
import sys
import time
from datetime import datetime
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster

KEYSPACE_NAME = "myapp"
TABLE_NAME = "comments"

def log_print(message, log_file):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    formatted = f"[{timestamp}] {message}"
    print(formatted)
    if log_file:
        print(formatted, file=log_file)
        log_file.flush()

def main():
    if len(sys.argv) != 4:
        print("Usage: python select_monitor.py <contact_point> <username> <password>")
        sys.exit(1)

    contact_point, username, password = sys.argv[1:4]
    log_filename = f"select_monitor_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_file = open(log_filename, 'w')

    log_print("Starting SELECT monitor script", log_file)
    log_print(f"Log file: {log_filename}", log_file)

    cluster = Cluster(
        contact_points=[contact_point],
        port=9042,
        auth_provider=PlainTextAuthProvider(username=username, password=password)
    )
    session = cluster.connect(KEYSPACE_NAME)

    while True:
        try:
            result = session.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
            count = result.one()[0]
            log_print(f"SUCCESS: Row count in {TABLE_NAME}: {count}", log_file)
        except Exception as e:
            log_print(f"ERROR: {e}", log_file)
        time.sleep(5)

if __name__ == "__main__":
    main()
