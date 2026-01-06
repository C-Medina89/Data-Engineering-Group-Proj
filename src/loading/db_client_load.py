import logging
import os
from contextlib import AbstractContextManager
from typing import Any, List, Optional, Sequence, Tuple

import pg8000.dbapi

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

class WarehouseDBClient(AbstractContextManager):

    # Warehouse Postgres client (Loading Zone).
    # - Uses pg8000.dbapi for standard cursor/commit semantics
    # - Supports efficient cursor.executemany()


    def __init__(self):

        self.host = os.getenv("WAREHOUSE_HOST")
        self.port = int(os.getenv("WAREHOUSE_PORT", "5432"))
        self.database = os.getenv("WAREHOUSE_DB")
        self.user = os.getenv("WAREHOUSE_USER")
        self.password = os.getenv("WAREHOUSE_PASSWORD")

        missing = [k for k, v in {
            "WAREHOUSE_HOST": self.host,
            "WAREHOUSE_DB": self.database,
            "WAREHOUSE_USER": self.user,
            "WAREHOUSE_PASSWORD": self.password,
        }.items() if not v]
        if missing:
            raise ValueError(f"Missing required env vars: {', '.join(missing)}")
        self.conn = None
        logger.info(
            "Initialising WarehouseDBClient host=%s port=%s db=%s user=%s",
            self.host, self.port, self.database, self.user
        )

    def __enter__(self) -> "WarehouseDBClient":
        self.conn = pg8000.dbapi.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
        )
        self.conn.autocommit = False
        return self
    
    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if self.conn is None:
            return False
        try:
            if exc_type is None:
                self.conn.commit()
                logger.info("Transaction committed")
            else:
                self.conn.rollback()
                logger.info("Transaction rolled back due to exception: %s", exc_value)
        finally:
            try:
                self.conn.close()
                logger.info("Database connection closed")
            except Exception as e:
                logger.exception("Error closing database connection: %s", e)
            self.conn = None

    def _require_connection(self) -> None:
        if self.conn is None:
            raise RuntimeError("Database connection is not established. Use 'with' context manager.")

    def execute(self, sql: str, params: Optional[Sequence[Any]] = None) -> None:
        
        # Execute a single statement.
        # Uses positional params (%s placeholders in SQL).
        self._require_connection()
        logger.debug("Executing SQL: %s", sql)
        cur = self.conn.cursor()
        try:
            if params is None:
                cur.execute(sql)
            else:
                cur.execute(sql, params)
        finally:
            cur.close()

    def executemany(self, sql: str, param_seq: List[Sequence[Any]], chunk_size: int = 1000) -> None:
        
        # Execute a statement multiple times with different params.
        # Uses positional params (%s placeholders in SQL).
        # Splits param_seq into chunks to avoid very large single executions.

        self._require_connection() 

        if not param_seq:
            logger.info("No parameters provided for executemany; skipping execution.")
            return
        
        logger.info("Executing SQL many times: %s with %s param sets", sql, len(param_seq))
        
        cur = self.conn.cursor()
        try:
            for i in range(0, len(param_seq), chunk_size):
                chunk = param_seq[i:i + chunk_size]
                logger.info("  Executing chunk %s - %s", i, i + len(chunk) - 1)
                cur.executemany(sql, chunk)
        finally:
            cur.close()

    def fetchall(self, sql: str, params: Optional[Sequence[Any]] = None) -> List[Tuple]:
        
        # Execute a query and fetch all results.
        # Uses positional params (%s placeholders in SQL).
        self._require_connection()
        logger.info("Fetching all results for SQL: %s with params=%s", sql, params)

        cur = self.conn.cursor()
        try:
            if params is None:
                cur.execute(sql)
            else:
                cur.execute(sql, params)
            results = cur.fetchall()
            logger.info("Fetched %s rows", len(results))
            return results
        finally:
            cur.close() 