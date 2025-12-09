from dotenv import load_dotenv
from pg8000.native import Connection
import pg8000
import os 
import logging



logger = logging.getLogger()
logger.setLevel(logging.INFO)


load_dotenv()


class DatabaseClient():
    def __init__(self):
        logger.info("Initializing DatabaseClient...")    
        
        self.host = os.getenv("DB_HOST")
        self.database = os.getenv("DB_NAME")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.port = int(os.getenv("DB_PORT", 5432))
        
        logger.info(
            f"Loaded DB env variables: host={self.host}, database={self.database}, user={self.user}, port={self.port}"
        )

        if not all([self.host, self.database, self.user, self.password, self.port]):
            logger.error("Missing required DB environment variables!")
            raise ValueError("One or more database environment variables are missing.")

        try:
            self.conn = Connection(
                user = self.user,
                host = self.host,
                database = self.database,
                password = self.password,
                port = self.port)
        
            logger.info("Database connection successfully.")
        except Exception as e:
            logger.exception("Failed to database connection.")
            raise

        

    def run(self, sql: str, params: dict | None = None):
        logger.info(f"Executing SQL: {sql} | params={params}")

        try:
            if params:
                rows = self.conn.run(sql, **params)
            else:
                rows = self.conn.run(sql)

            column_names = [col["name"] for col in self.conn.columns]
            result = [dict(zip(column_names, row)) for row in rows]

            logger.info(f"SQL executed successfully. Returned {len(result)} rows.")
            return result

        except Exception as e:
            logger.exception(f"Error executing SQL: {sql}")
            raise
    
    def fetch_preview(self, table_name: str, limit: int = 10):
        
        logger.info(f"Fetching preview from table '{table_name}' (limit={limit})")
        
        sql = f"SELECT * FROM {table_name} LIMIT :limit"
        preview_table = self.run(sql, {"limit": limit})
        return preview_table

    def close(self):
        try:
            self.conn.close()
            logger.info("Database connection closed.")
        except:
            logger.warning("Failed to close database connection cleanly.")


    