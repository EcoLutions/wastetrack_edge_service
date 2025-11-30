import os
import logging
from peewee import SqliteDatabase, Model
from config.database_config import DatabaseConfig

logger = logging.getLogger(__name__)

# Ensure data directory exists
os.makedirs(os.path.dirname(DatabaseConfig.DATABASE_PATH), exist_ok=True)

# Create SQLite database connection
database = SqliteDatabase(
    DatabaseConfig.DATABASE_PATH,
    pragmas={
        'foreign_keys': 1,  # Enable foreign key constraints
        'journal_mode': 'wal',  # Write-Ahead Logging for better concurrency
        'cache_size': -1 * 64000,  # 64MB cache
        'synchronous': 1  # NORMAL mode (balance between safety and speed)
    }
)

logger.info(f"SQLite database configured: {DatabaseConfig.DATABASE_PATH}")


class BaseModel(Model):
    """
    Base model for all Peewee models

    Automatically connects to the configured database
    """

    class Meta:
        database = database