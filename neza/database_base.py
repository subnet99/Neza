"""
Database Module
Provides basic functionality for interacting with PostgreSQL database
"""

import os
import psycopg2
import psycopg2.extras
import psycopg2.pool
import bittensor as bt
import threading

from dotenv import load_dotenv

load_dotenv()


class ConnectionContext:
    """Context manager for database connections"""

    def __init__(self, db_connection):
        self.db_connection = db_connection
        self.conn = None

    def __enter__(self):
        self.conn = self.db_connection.get_connection()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            if exc_type is not None:
                try:
                    self.conn.rollback()
                except:
                    pass
            self.db_connection.put_connection(self.conn)
        return False


class DatabaseConnection:
    """
    Database Connection Base Class
    Provides basic database connection and initialization functionality
    """

    # Class-level connection pool
    _connection_pool = None
    _pool_lock = threading.Lock()

    def __init__(self):
        """Initialize database connection configuration"""
        self.db_config = {
            "dbname": os.environ.get("DB_NAME", "video_subnet"),
            "user": os.environ.get("DB_USER", "postgres"),
            "password": os.environ.get("DB_PASSWORD", "postgres"),
            "host": os.environ.get("DB_HOST", "localhost"),
            "port": os.environ.get("DB_PORT", "5432"),
        }

        # Initialize connection pool
        self._init_connection_pool()

        # Ensure database exists
        self._ensure_database_exists()

    def _init_connection_pool(self):
        """Initialize connection pool"""
        with DatabaseConnection._pool_lock:
            if DatabaseConnection._connection_pool is None:
                min_conn = int(os.environ.get("DB_POOL_MIN", "5"))
                max_conn = int(os.environ.get("DB_POOL_MAX", "50"))

                try:
                    DatabaseConnection._connection_pool = (
                        psycopg2.pool.ThreadedConnectionPool(
                            minconn=min_conn, maxconn=max_conn, **self.db_config
                        )
                    )
                    bt.logging.info(
                        f"Database connection pool initialized: min={min_conn}, max={max_conn}"
                    )
                except Exception as e:
                    bt.logging.error(f"Error initializing connection pool: {str(e)}")
                    raise

    def _ensure_database_exists(self):
        """Ensure database exists, create it if it doesn't"""
        conn = None
        try:
            # Try to connect to the target database using direct connection for initialization
            conn = psycopg2.connect(**self.db_config)
            conn.close()
            bt.logging.info(
                f"Successfully connected to database {self.db_config['dbname']}"
            )
        except psycopg2.OperationalError as e:
            if "does not exist" in str(e):
                bt.logging.warning(
                    f"Database {self.db_config['dbname']} does not exist, attempting to create..."
                )
                self._create_database()
            else:
                # Other connection errors
                bt.logging.error(f"Database connection error: {str(e)}")
                raise

    def _create_database(self):
        """Create database"""
        # Connect to default postgres database
        default_db_config = self.db_config.copy()
        default_db_config["dbname"] = "postgres"  # Use default postgres database

        conn = None
        try:
            # Connect to default database
            conn = psycopg2.connect(**default_db_config)
            conn.autocommit = True  # Creating database requires autocommit mode
            cursor = conn.cursor()

            # Check if database already exists
            cursor.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (self.db_config["dbname"],),
            )
            if cursor.fetchone():
                bt.logging.info(f"Database {self.db_config['dbname']} already exists")
                return

            # Create database
            cursor.execute(f"CREATE DATABASE {self.db_config['dbname']}")
            bt.logging.info(f"Successfully created database {self.db_config['dbname']}")

        except Exception as e:
            bt.logging.error(f"Error creating database: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()

    def get_connection(self, timeout=30):
        """Get database connection from pool with timeout"""
        if DatabaseConnection._connection_pool is None:
            raise RuntimeError("Connection pool not initialized")

        import time

        start_time = time.time()

        while True:
            try:
                # getconn() doesn't support timeout parameter, so we implement retry logic manually
                conn = DatabaseConnection._connection_pool.getconn()
                if conn:
                    conn.autocommit = False  # Use explicit transactions
                return conn
            except psycopg2.pool.PoolError as e:
                # Pool is exhausted, wait a bit and retry
                elapsed = time.time() - start_time
                if elapsed > timeout:
                    bt.logging.error(
                        f"Connection pool exhausted, timeout after {timeout}s"
                    )
                    raise RuntimeError(
                        "Connection pool exhausted, too many concurrent database operations"
                    )
                time.sleep(0.1)
            except Exception as e:
                bt.logging.error(
                    f"Error getting database connection from pool: {str(e)}"
                )
                raise

    def put_connection(self, conn):
        """Return connection to pool"""
        if DatabaseConnection._connection_pool is None or conn is None:
            return

        try:
            DatabaseConnection._connection_pool.putconn(conn)
        except Exception as e:
            bt.logging.error(f"Error returning connection to pool: {str(e)}")
            try:
                conn.close()
            except:
                pass

    def get_connection_context(self):
        """Get connection context manager for automatic connection management"""
        return ConnectionContext(self)
