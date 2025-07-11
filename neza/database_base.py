"""
Database Module
Provides basic functionality for interacting with PostgreSQL database
"""

import os
import psycopg2
import psycopg2.extras
import bittensor as bt

from dotenv import load_dotenv

load_dotenv()


class DatabaseConnection:
    """
    Database Connection Base Class
    Provides basic database connection and initialization functionality
    """

    def __init__(self):
        """Initialize database connection configuration"""
        self.db_config = {
            "dbname": os.environ.get("DB_NAME", "video_subnet"),
            "user": os.environ.get("DB_USER", "postgres"),
            "password": os.environ.get("DB_PASSWORD", "postgres"),
            "host": os.environ.get("DB_HOST", "localhost"),
            "port": os.environ.get("DB_PORT", "5432"),
        }

        # Ensure database exists
        self._ensure_database_exists()

    def _ensure_database_exists(self):
        """Ensure database exists, create it if it doesn't"""
        try:
            # Try to connect to the target database
            self.get_connection().close()
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

    def get_connection(self):
        """Get database connection"""
        try:
            conn = psycopg2.connect(**self.db_config)
            conn.autocommit = False  # Use explicit transactions
            return conn
        except Exception as e:
            bt.logging.error(f"Error getting database connection: {str(e)}")
            raise
