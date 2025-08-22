import os

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:
    """Configuration class for the Content Discovery service."""

    # MongoDB Configuration
    MONGODB_URI: str = os.getenv(
        "MONGODB_URI", "mongodb://admin:admin@localhost:47027/?directConnection=true"
    )
    MONGODB_DATABASE: str = os.getenv("MONGODB_DATABASE", "content-discovery")
    MONGODB_MAX_POOL_SIZE: int = int(os.getenv("MONGODB_MAX_POOL_SIZE", "100"))
    MONGODB_MIN_POOL_SIZE: int = int(os.getenv("MONGODB_MIN_POOL_SIZE", "10"))
    MONGODB_MAX_IDLE_TIME_MS: int = int(os.getenv("MONGODB_MAX_IDLE_TIME_MS", "30000"))
    MONGODB_CONNECT_TIMEOUT_MS: int = int(
        os.getenv("MONGODB_CONNECT_TIMEOUT_MS", "10000")
    )
    MONGODB_SERVER_SELECTION_TIMEOUT_MS: int = int(
        os.getenv("MONGODB_SERVER_SELECTION_TIMEOUT_MS", "5000")
    )
    MONGODB_RETRY_WRITES: bool = (
        os.getenv("MONGODB_RETRY_WRITES", "true").lower() == "true"
    )
    MONGODB_RETRY_READS: bool = (
        os.getenv("MONGODB_RETRY_READS", "true").lower() == "true"
    )

    # Logging Configuration
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    @classmethod
    def get_mongodb_config(cls) -> dict:
        """Get MongoDB connection configuration as a dictionary."""
        return {
            "uri": cls.MONGODB_URI,
            "database": cls.MONGODB_DATABASE,
            "maxPoolSize": cls.MONGODB_MAX_POOL_SIZE,
            "minPoolSize": cls.MONGODB_MIN_POOL_SIZE,
            "maxIdleTimeMS": cls.MONGODB_MAX_IDLE_TIME_MS,
            "connectTimeoutMS": cls.MONGODB_CONNECT_TIMEOUT_MS,
            "serverSelectionTimeoutMS": cls.MONGODB_SERVER_SELECTION_TIMEOUT_MS,
            "retryWrites": cls.MONGODB_RETRY_WRITES,
            "retryReads": cls.MONGODB_RETRY_READS,
        }


# Global config instance
config = Config()
