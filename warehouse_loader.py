"""
S3 and Redshift loader utilities
"""

import logging
from pathlib import Path
from typing import Optional, Dict

import boto3
from botocore.exceptions import BotoCoreError, ClientError
import psycopg2
from psycopg2 import sql, OperationalError, DatabaseError

logger = logging.getLogger(__name__)


class S3Uploader:
    """Uploads local files to Amazon S3"""

    def __init__(
        self,
        bucket_name: str,
        region_name: str,
        prefix: Optional[str] = None,
        aws_config: Optional[Dict[str, Optional[str]]] = None,
    ):
        self.bucket_name = bucket_name
        self.region_name = region_name
        self.prefix = prefix or ""
        self.aws_config = aws_config or {}
        self.session = self._create_session()
        self.client = self.session.client("s3", region_name=self.region_name)

    def _create_session(self):
        profile_name = self.aws_config.get("profile_name")
        access_key = self.aws_config.get("aws_access_key_id")
        secret_key = self.aws_config.get("aws_secret_access_key")
        session_token = self.aws_config.get("aws_session_token")

        if profile_name:
            return boto3.Session(profile_name=profile_name, region_name=self.region_name)

        if access_key and secret_key:
            return boto3.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                aws_session_token=session_token,
                region_name=self.region_name,
            )

        return boto3.Session(region_name=self.region_name)

    def upload_file(self, local_path: Path, key: Optional[str] = None) -> str:
        if not key:
            key = local_path.name
            if self.prefix:
                key = f"{self.prefix.rstrip('/')}/{key}"

        try:
            logger.info(f"Uploading {local_path} to s3://{self.bucket_name}/{key}")
            self.client.upload_file(str(local_path), self.bucket_name, key)
            return f"s3://{self.bucket_name}/{key}"

        except (BotoCoreError, ClientError) as ex:
            logger.error(f"S3 upload failed: {ex}")
            raise


class RedshiftConnector:
    """Connects to Amazon Redshift and runs queries"""

    def __init__(self, config: Dict[str, str]):
        self.config = config
        self.connection = None

    def connect(self) -> bool:
        try:
            self.connection = psycopg2.connect(
                host=self.config["host"],
                port=self.config["port"],
                dbname=self.config["database"],
                user=self.config["user"],
                password=self.config["password"],
                sslmode=self.config.get("sslmode", "require"),
                connect_timeout=30,
            )
            logger.info(f"Connected to Redshift at {self.config['host']}")
            return True
        except OperationalError as ex:
            logger.error(f"Redshift connection failed: {ex}")
            return False
        except Exception as ex:
            logger.error(f"Unexpected Redshift connection error: {ex}")
            return False

    def disconnect(self):
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from Redshift")

    def execute_query(self, query: str) -> None:
        with self.connection.cursor() as cur:
            cur.execute(query)
            self.connection.commit()

    def copy_from_s3(
        self,
        target_table: str,
        s3_uri: str,
        region: str,
        iam_role_arn: Optional[str] = None,
        aws_credentials: Optional[Dict[str, Optional[str]]] = None,
        file_format: str = "csv",
        delimiter: str = ",",
        ignore_header: int = 1,
    ) -> bool:
        if not iam_role_arn and not (aws_credentials and aws_credentials.get("aws_access_key_id") and aws_credentials.get("aws_secret_access_key")):
            logger.error("No IAM role or AWS credentials configured for Redshift COPY")
            return False

        target_table_sql = self._format_table_name(target_table)
        s3_uri_literal = sql.Literal(s3_uri)

        if iam_role_arn:
            credentials_clause = sql.SQL("IAM_ROLE {role}").format(role=sql.Literal(iam_role_arn))
        else:
            access_key = aws_credentials["aws_access_key_id"]
            secret_key = aws_credentials["aws_secret_access_key"]
            session_token = aws_credentials.get("aws_session_token")
            credentials_string = f"aws_access_key_id={access_key};aws_secret_access_key={secret_key}"
            if session_token:
                credentials_string += f";token={session_token}"
            credentials_clause = sql.SQL("CREDENTIALS {creds}").format(creds=sql.Literal(credentials_string))

        format_clause = sql.SQL("FORMAT AS CSV") if file_format.lower() == "csv" else sql.SQL("FORMAT AS JSON 'auto'")
        delimiter_clause = sql.SQL("DELIMITER {delimiter}").format(delimiter=sql.Literal(delimiter))
        ignore_header_clause = sql.SQL("IGNOREHEADER {ignore_header}").format(ignore_header=sql.Literal(ignore_header))

        copy_query = sql.SQL(
            "COPY {target} FROM {s3_uri} {credentials} {format_clause} {delimiter_clause} {ignore_header_clause} REGION {region} TIMEFORMAT 'auto' COMPUPDATE OFF STATUPDATE OFF"
        ).format(
            target=target_table_sql,
            s3_uri=s3_uri_literal,
            credentials=credentials_clause,
            format_clause=format_clause,
            delimiter_clause=delimiter_clause,
            ignore_header_clause=ignore_header_clause,
            region=sql.Literal(region),
        )

        try:
            truncate_query = sql.SQL("TRUNCATE TABLE {target}").format(target=target_table_sql)
            with self.connection.cursor() as cur:
                cur.execute(truncate_query)
                logger.info(f"Truncated target table {target_table} before COPY")
                cur.execute(copy_query)
            self.connection.commit()
            logger.info("Redshift COPY completed successfully")
            with self.connection.cursor() as cur:
                cur.execute(sql.SQL("SELECT COUNT(*) FROM {target}").format(target=target_table_sql))
                row_count = cur.fetchone()[0]
            logger.info(f"Row count after COPY: {row_count} rows in {target_table}")
            return True
        except (DatabaseError, OperationalError) as ex:
            logger.error(f"Redshift COPY failed: {ex}")
            return False

    def _format_table_name(self, table_name: str) -> sql.Composed:
        parts = [part.strip() for part in table_name.split('.') if part.strip()]
        return sql.SQL('.').join([sql.Identifier(part) for part in parts])
