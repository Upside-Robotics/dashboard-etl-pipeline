"""
ETL Pipeline - PostgreSQL Connection and Data Retrieval Module
"""

import psycopg2
from psycopg2 import sql, errors
import logging
from typing import Iterator, Dict, List, Any
from config import POSTGRES_CONFIG, ETL_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PostgreSQLConnector:
    """Handles connections and data retrieval from PostgreSQL"""
    
    def __init__(self, config: Dict = None):
        """
        Initialize PostgreSQL connector
        
        Args:
            config: Database configuration dict (uses POSTGRES_CONFIG if None)
        """
        self.config = config or POSTGRES_CONFIG
        self.connection = None
        
    def connect(self) -> bool:
        """
        Establish connection to PostgreSQL database
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.connection = psycopg2.connect(
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password'],
                connect_timeout=ETL_CONFIG['timeout']
            )
            logger.info(f"Successfully connected to PostgreSQL at {self.config['host']}")
            return True
        except errors.OperationalError as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during connection: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from PostgreSQL")
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get information about a table (column names, types, row count)
        
        Args:
            table_name: Name of the table to inspect
            
        Returns:
            dict: Table metadata including columns and row count
        """
        try:
            with self.connection.cursor() as cur:
                # Get column information
                cur.execute(f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = %s
                    ORDER BY ordinal_position
                """, (table_name,))
                
                columns = cur.fetchall()
                
                # Get row count
                cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(
                    sql.Identifier(table_name)
                ))
                row_count = cur.fetchone()[0]
                
                table_info = {
                    'table_name': table_name,
                    'columns': [{'name': col[0], 'type': col[1]} for col in columns],
                    'row_count': row_count
                }
                
                logger.info(f"Table '{table_name}' has {row_count} rows and {len(columns)} columns")
                return table_info
                
        except errors.ProgrammingError as e:
            logger.error(f"Table '{table_name}' not found: {e}")
            return {}
        except Exception as e:
            logger.error(f"Error retrieving table info: {e}")
            return {}
    
    def retrieve_data_batched(
        self, 
        table_name: str, 
        batch_size: int = None,
        limit: int = None
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        Retrieve data from table in batches for memory efficiency
        
        Args:
            table_name: Name of the table to retrieve from
            batch_size: Number of rows per batch (uses ETL_CONFIG if None)
            limit: Maximum total rows to retrieve (None = all)
            
        Yields:
            List of dictionaries representing rows in each batch
        """
        batch_size = batch_size or ETL_CONFIG['batch_size']
        
        try:
            with self.connection.cursor() as cur:
                # Use server-side cursor for large datasets
                cur.arraysize = batch_size
                
                if limit:
                    query = sql.SQL("SELECT * FROM {} LIMIT {}").format(
                        sql.Identifier(table_name),
                        sql.Literal(limit)
                    )
                else:
                    query = sql.SQL("SELECT * FROM {}").format(
                        sql.Identifier(table_name)
                    )
                
                cur.execute(query)
                
                # Get column names
                column_names = [desc[0] for desc in cur.description]
                
                total_rows = 0
                while True:
                    rows = cur.fetchmany(batch_size)
                    if not rows:
                        break
                    
                    # Convert tuples to dictionaries
                    batch_data = [
                        dict(zip(column_names, row))
                        for row in rows
                    ]
                    
                    total_rows += len(batch_data)
                    logger.info(f"Retrieved batch of {len(batch_data)} rows (total: {total_rows})")
                    
                    yield batch_data
                
                logger.info(f"Completed retrieval of {total_rows} rows from '{table_name}'")
                
        except errors.ProgrammingError as e:
            logger.error(f"Query error: {e}")
            yield []
        except Exception as e:
            logger.error(f"Error during data retrieval: {e}")
            yield []
    
    def retrieve_data_simple(
        self, 
        table_name: str,
        limit: int = None
    ) -> List[Dict[str, Any]]:
        """
        Simple data retrieval (loads entire result into memory)
        Use for smaller datasets only
        
        Args:
            table_name: Name of the table to retrieve from
            limit: Maximum rows to retrieve (None = all)
            
        Returns:
            List of dictionaries representing rows
        """
        try:
            with self.connection.cursor() as cur:
                if limit:
                    query = sql.SQL("SELECT * FROM {} LIMIT {}").format(
                        sql.Identifier(table_name),
                        sql.Literal(limit)
                    )
                else:
                    query = sql.SQL("SELECT * FROM {}").format(
                        sql.Identifier(table_name)
                    )
                
                cur.execute(query)
                
                column_names = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                
                data = [
                    dict(zip(column_names, row))
                    for row in rows
                ]
                
                logger.info(f"Retrieved {len(data)} rows from '{table_name}'")
                return data
                
        except Exception as e:
            logger.error(f"Error during data retrieval: {e}")
            return []
    
    def execute_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """
        Execute custom SQL query
        
        Args:
            query: SQL query string
            params: Query parameters (optional)
            
        Returns:
            List of dictionaries representing query results
        """
        try:
            with self.connection.cursor() as cur:
                cur.execute(query, params)
                
                if cur.description is None:
                    self.connection.commit()
                    logger.info("Query executed successfully")
                    return []
                
                column_names = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                
                data = [
                    dict(zip(column_names, row))
                    for row in rows
                ]
                
                logger.info(f"Query returned {len(data)} rows")
                return data
                
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            return []


def main():
    """Example usage of PostgreSQL connector"""
    connector = PostgreSQLConnector()
    
    # Connect to database
    if not connector.connect():
        logger.error("Failed to establish connection")
        return
    
    try:
        # Get table information
        table_info = connector.get_table_info(ETL_CONFIG['source_table'])
        if table_info:
            print("\n=== Table Information ===")
            print(f"Table: {table_info['table_name']}")
            print(f"Row Count: {table_info['row_count']}")
            print(f"Columns: {len(table_info['columns'])}")
            print("\nColumn Details:")
            for col in table_info['columns'][:10]:  # Show first 10 columns
                print(f"  - {col['name']}: {col['type']}")
            if len(table_info['columns']) > 10:
                print(f"  ... and {len(table_info['columns']) - 10} more columns")
        
        # Retrieve data in batches
        print(f"\n=== Retrieving Data from '{ETL_CONFIG['source_table']}' ===")
        batch_count = 0
        for batch in connector.retrieve_data_batched(ETL_CONFIG['source_table'], limit=None):
            batch_count += 1
            if batch_count == 1:
                print(f"Sample row: {batch[0]}")
        
    finally:
        connector.disconnect()


if __name__ == "__main__":
    main()
