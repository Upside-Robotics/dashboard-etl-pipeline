"""
Quick connection test — fetches 5 latest rows from robot_executive_state
"""

import json
from postgres_connector import PostgreSQLConnector
from config import ETL_CONFIG

connector = PostgreSQLConnector()

if not connector.connect():
    print("Connection failed.")
    exit(1)

try:
    count = connector.execute_query("SELECT COUNT(*) AS total FROM robot_executive_state")
    print(f"\nTotal rows in robot_executive_state: {count[0]['total']}\n")

    rows = connector.execute_query(
        "SELECT * FROM robot_executive_state ORDER BY write_time DESC LIMIT 5"
    )
    print(f"5 most recent rows:\n")
    for i, row in enumerate(rows, 1):
        print(f"--- Row {i} ---")
        print(json.dumps(row, indent=2, default=str))
finally:
    connector.disconnect()
