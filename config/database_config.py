import os
from dotenv import load_dotenv

load_dotenv()


class DatabaseConfig:
    """
    Configuration of local database SQLite
    """

    # Path to SQLite database file
    DATABASE_PATH = os.getenv('DATABASE_PATH', './data/edge_service.db')

    # Timeout for database connections in seconds
    TIMEOUT = 10

    # Enable foreign key support
    FOREIGN_KEYS = True

    @classmethod
    def get_connection_string(cls) -> str:
        """Get the SQLite connection string."""
        return cls.DATABASE_PATH