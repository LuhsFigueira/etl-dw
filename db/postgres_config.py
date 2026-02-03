import os
from sqlalchemy import create_engine


class PostgresDatabaseConfig:
    def __init__(
        self,
        user: str,
        password: str,
        host: str,
        port: int,
        database: str,
        driver: str = "psycopg2"
    ):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.driver = driver

    def get_uri(self) -> str:
        return (
            f"postgresql+{self.driver}://"
            f"{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    def create_engine(self):
        return create_engine(
            self.get_uri(),
            pool_pre_ping=True,
            future=True
        )
