from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from utils.logger import setup_logger

logger = setup_logger(__name__)

class MySQLLoader:
    def __init__(self, db_config: dict, table_name: str):
        """
        Initialize MySQL loader with database configuration.
        
        Args:
            db_config: Dictionary with keys: host, port, database, user, password
            table_name: Target table name for data loading
        """
        self.db_config = db_config
        self.table_name = table_name
        self.engine = self._create_engine()

    def _create_engine(self):
        """Create SQLAlchemy engine."""
        connection_url = (
            f"mysql+pymysql://{self.db_config['user']}:{self.db_config['password']}"
            f"@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
        )
        return create_engine(connection_url, echo=False, pool_pre_ping=True)

    def load(self, df: pd.DataFrame, if_exists: str = 'append', chunksize: int = 1000):
        """
        Bulk insert DataFrame into MySQL table.
        
        Args:
            df: Pandas DataFrame to load
            if_exists: How to behave if table exists ('fail', 'replace', 'append')
            chunksize: Number of rows to insert per batch
        """
        try:
            logger.info(f"Loading {len(df)} rows into {self.table_name}")
            df.to_sql(
                name=self.table_name,
                con=self.engine,
                if_exists=if_exists,
                index=False,
                chunksize=chunksize,
                method='multi'  # faster for many rows
            )
            logger.info(f"Successfully loaded {len(df)} rows")
        except SQLAlchemyError as e:
            logger.error(f"Database load failed: {e}")
            raise

    def test_connection(self):
        """Test database connectivity."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                result.fetchone()
            logger.info("MySQL connection successful")
        except Exception as e:
            logger.error(f"MySQL connection failed: {e}")
            raise

    def execute_query(self, query: str):
        """Execute a custom SQL query."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                conn.commit()
                logger.info(f"Query executed successfully")
                return result
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise

    def close(self):
        """Close database connection."""
        self.engine.dispose()
        logger.info("Database connection closed")
