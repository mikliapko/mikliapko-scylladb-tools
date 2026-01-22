#!/usr/bin/env python3
import json
import sys
import time
import random
from datetime import datetime
from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from textwrap import dedent

KEYSPACE_NAME = "myapp"
TABLE_NAME = "comments"
INDEX_NAME = "comment_ann_index"

# Global log file handle
log_file = None

def log_print(*args, **kwargs):
    """Print to both stdout and log file with timestamp"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    message = ' '.join(str(arg) for arg in args)
    formatted_message = f"[{timestamp}] {message}"

    # Print to stdout
    print(formatted_message, **{k: v for k, v in kwargs.items() if k != 'sep'})
    # Write to log file
    if log_file:
        print(formatted_message, **{k: v for k, v in kwargs.items() if k != 'sep'}, file=log_file)
        log_file.flush()  # Ensure immediate write

NUM_NEW_ENTRIES = 240


def main():
    global log_file

    if len(sys.argv) != 4:
        print("Usage: python vector_search_insert_test.py <contact_point> <username> <password>")
        sys.exit(1)

    contact_point = sys.argv[1]
    username = sys.argv[2]
    password = sys.argv[3]

    # Open log file with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"vector_search_test_{timestamp}.log"
    log_file = open(log_filename, 'w')

    log_print(f"Starting vector search test - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log_print(f"Log file: {log_filename}")
    log_print(f"Contact point: {contact_point}")
    log_print()

    cluster = Cluster(
        contact_points=[contact_point],
        port=9042,
        auth_provider=PlainTextAuthProvider(username=username, password=password)
    )
    session = cluster.connect()

    log_print("Creating keyspace...")
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE_NAME}
        WITH replication = {{
            'class': 'NetworkTopologyStrategy',
            'replication_factor': 3
        }}
    """)

    log_print("Creating table...")
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {KEYSPACE_NAME}.{TABLE_NAME} (
            record_id timeuuid,
            id uuid,
            commenter text,
            comment text,
            comment_vector vector<float, 64>,
            created_at timestamp,
            PRIMARY KEY (id, created_at)
        )
    """)

    log_print("Loading test data...")
    with open("initial_data.json", "r") as f:
        comments = json.load(f)

    log_print(f"Inserting first 20 entries...")
    for i, (commenter, comment, vector) in enumerate(comments[:20], start=1):
        vector_str = ','.join(str(v) for v in vector)
        session.execute(
            f"INSERT INTO {KEYSPACE_NAME}.{TABLE_NAME} "
            f"(record_id, id, commenter, comment, comment_vector, created_at) "
            f"VALUES (now(), uuid(), '{commenter}', '{comment}', [{vector_str}], toTimestamp(now()))"
        )
        log_print(f"  [{i}/20] Inserted: {commenter}")

    log_print("\nCreating vector search index...")
    session.execute(f"""
        CREATE CUSTOM INDEX IF NOT EXISTS {INDEX_NAME}
        ON {KEYSPACE_NAME}.{TABLE_NAME}(comment_vector)
        USING 'vector_index'
        WITH OPTIONS = {{'similarity_function': 'COSINE'}}
    """)

    log_print("\nWaiting for index to be built (10 seconds)...")
    time.sleep(10)

    # Generate and insert new entries with validation
    failed_validations = 0
    log_print(f"\n{'='*70}")
    log_print(f"Generating {NUM_NEW_ENTRIES} new entries with ANN validation")
    log_print(f"{'='*70}")

    for i in range(1, NUM_NEW_ENTRIES + 1):
        # Generate random vector of length 64 with values between 0 and 1
        new_vector = [round(random.uniform(0.0, 1.0), 2) for _ in range(64)]
        vector_str = ','.join(str(v) for v in new_vector)

        # Generate unique commenter name
        new_commenter = f"GeneratedUser{i}"
        new_comment = f"This is auto-generated comment number {i} for testing vector search"

        log_print(f"\n[{i}/{NUM_NEW_ENTRIES}] Inserting: {new_commenter}")
        log_print(f"  Comment: {new_comment[:50]}...")
        log_print(f"  Vector (first 10 values): [{', '.join(str(v) for v in new_vector[:10])}, ...]")

        # Insert the new entry
        session.execute(
            f"INSERT INTO {KEYSPACE_NAME}.{TABLE_NAME} "
            f"(record_id, id, commenter, comment, comment_vector, created_at) "
            f"VALUES (now(), uuid(), '{new_commenter}', '{new_comment}', [{vector_str}], toTimestamp(now()))"
        )

        log_print(f"  ✓ Inserted successfully")

        # Get total row count
        query = SimpleStatement(
            f"SELECT COUNT(*) FROM {KEYSPACE_NAME}.{TABLE_NAME}",
            consistency_level=ConsistencyLevel.QUORUM,
        )
        count_result = session.execute(query)
        total_rows = count_result.one()[0]
        log_print(f"  Total rows in table: {total_rows}")

        # Wait a bit to ensure the insert is propagated
        time.sleep(45)

        # Run ANN search using the same vector
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_print(f"  Running ANN search with the inserted vector... at {timestamp}")
        query = SimpleStatement(
            f"SELECT commenter, comment "
            f"FROM {KEYSPACE_NAME}.{TABLE_NAME} "
            f"ORDER BY comment_vector ANN OF [{vector_str}] "
            f"LIMIT 3",
            consistency_level=ConsistencyLevel.QUORUM
        )
        result = session.execute(query)

        rows = list(result)
        if rows:
            first_result_commenter = rows[0].commenter
            log_print(f"  Top 3 ANN results:")
            for idx, row in enumerate(rows, start=1):
                log_print(f"    {idx}. {row.commenter}: {row.comment[:50]}...")

            # Validate that the first result is the latest inserted entry
            if first_result_commenter == new_commenter:
                log_print(f"  ✓ VALIDATION PASSED: First result matches inserted entry ({new_commenter})")
            else:
                log_print(f"  ✗ VALIDATION FAILED: First result is '{first_result_commenter}', expected '{new_commenter}'")
                failed_validations += 1
        else:
            log_print(f"  ✗ VALIDATION FAILED: No results returned from ANN search")
            failed_validations += 1

    log_print(f"\n{'='*70}")
    log_print(f"Test completed! Inserted {NUM_NEW_ENTRIES} new entries with validation")
    log_print(f"Failed validations: {failed_validations}/{NUM_NEW_ENTRIES}")
    if failed_validations == 0:
        log_print(f"✓ All validations passed!")
    else:
        log_print(f"✗ {failed_validations} validation(s) failed")
    log_print(f"{'='*70}")

    cluster.shutdown()
    log_print("\nDone!")
    log_print(f"Log saved to: {log_filename}")

    # Close log file
    if log_file:
        log_file.close()


if __name__ == "__main__":
    main()
