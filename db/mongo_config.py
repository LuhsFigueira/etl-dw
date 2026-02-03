import os
from pymongo import MongoClient


class MongoDatabaseConfig:
    def __init__(
        self,
        user: str,
        password: str,
        host: str,
        port: int,
        database: str,
        auth_source: str = "admin"
    ):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.auth_source = auth_source

    def get_uri(self) -> str:
        return (
            f"mongodb://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/"
            f"{self.database}?authSource={self.auth_source}"
        )

    def connect(self):
        client = MongoClient(self.get_uri())
        db = client[self.database]

        # health check
        db.command("ping")

        return db
