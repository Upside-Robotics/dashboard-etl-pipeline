# Robot ETL Pipeline

ETL (Extract-Transform-Load) pipeline for moving large volumes of data from PostgreSQL to a data warehouse.

## Features

- **Batch Processing**: Efficiently handles large datasets using batch processing
- **Multiple Output Formats**: JSONL and CSV support
- **Logging**: Comprehensive logging to both file and console
- **Table Inspection**: View table schemas and row counts before extraction
- **Error Handling**: Robust error handling and recovery
- **Extensible Design**: Easy to add warehouse loading modules (Snowflake, Redshift, BigQuery, etc.)

## Project Structure

```
robot-etl-pipeline/
├── config.py                 # Configuration file (database credentials, settings)
├── postgres_connector.py      # PostgreSQL connection and data retrieval
├── etl_pipeline.py          # Main ETL orchestration
├── requirements.txt         # Python dependencies
├── extracted_data/          # Output directory for extracted data
├── etl_pipeline.log        # Log file (auto-generated)
└── README.md               # This file
```

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configuration

Edit `config.py` to configure:
- PostgreSQL connection details (already set with your credentials)
- Batch size for data processing
- Source table name (defaults to `robot_executive_state`)

### 3. Run Extraction

```bash
python etl_pipeline.py
```

## Usage Examples

### Basic Extraction (from Python)

```python
from postgres_connector import PostgreSQLConnector
from config import ETL_CONFIG

connector = PostgreSQLConnector()
connector.connect()

# Get table information
table_info = connector.get_table_info('robot_executive_state')
print(f"Row count: {table_info['row_count']}")

# Retrieve data in batches
for batch in connector.retrieve_data_batched('robot_executive_state'):
    # Process batch
    print(f"Processing {len(batch)} rows")

connector.disconnect()
```

### Custom Query

```python
connector = PostgreSQLConnector()
connector.connect()

results = connector.execute_query(
    "SELECT * FROM robot_executive_state WHERE status = %s LIMIT 100",
    ('completed',)
)

for row in results:
    print(row)

connector.disconnect()
```

## Key Classes

### PostgreSQLConnector

Handles all database operations:
- `connect()`: Establish database connection
- `disconnect()`: Close connection
- `get_table_info(table_name)`: Get table metadata
- `retrieve_data_batched(table_name, batch_size)`: Retrieve data in memory-efficient batches
- `retrieve_data_simple(table_name)`: Simple full table load (use for small tables only)
- `execute_query(query, params)`: Execute custom SQL queries

### ETLPipeline

Orchestrates the extraction process:
- `extract_from_postgres(table_name, output_format)`: Main extraction method
- Supports JSONL and CSV output formats
- Automatically handles serialization of special types (datetime, bytes, etc.)
- Provides extraction statistics (row count, duration, throughput)

## Output Files

Extracted data is saved to the `extracted_data/` directory with the format:
```
robot_executive_state_YYYYMMDD_HHMMSS.jsonl
```

### JSONL Format Example
```json
{"id": 1, "state": "active", "timestamp": "2026-05-07T10:30:00"}
{"id": 2, "state": "inactive", "timestamp": "2026-05-07T10:31:00"}
```

## Logging

Logs are written to both:
- **Console**: Real-time output
- **File**: `etl_pipeline.log` for persistent records

## Performance Considerations

- **Batch Size**: Configured to 10,000 rows per batch (tune in `config.py` based on memory)
- **Memory Efficiency**: Batch processing prevents loading entire datasets into memory
- **Large Tables**: Can handle multi-million row tables efficiently

## Next Steps

1. **Test Connection**: Run the extraction to verify your database connection works
2. **Warehouse Setup**: Once your data warehouse is ready, create warehouse-specific modules in this codebase
3. **Load Module**: Implement `warehouse_loader.py` to handle loading extracted data
4. **Transformation**: Add data transformation logic as needed between extraction and loading
5. **Orchestration**: Consider using Airflow or similar tools for production scheduling

## Future Enhancements

- [ ] Warehouse loading modules (Snowflake, Redshift, BigQuery, etc.)
- [ ] Data transformation capabilities
- [ ] Incremental loading (changed data capture)
- [ ] Data validation and quality checks
- [ ] Scheduling and automation (Airflow integration)
- [ ] Monitoring and alerting
- [ ] Compression support for output files

## Troubleshooting

### Connection Issues
- Verify PostgreSQL host and port are accessible
- Check username/password credentials
- Ensure firewall allows connections to 10.0.10.238:5432

### Memory Issues with Large Tables
- Reduce `batch_size` in `config.py`
- Process tables in smaller chunks using WHERE clauses

### Performance Tuning
- Increase `batch_size` for faster processing (if memory allows)
- Ensure PostgreSQL server resources are available
- Consider running extraction during off-peak hours

## License

Internal use only
