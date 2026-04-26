from functools import lru_cache
import importlib
import os

from sqlalchemy.engine import URL

try:
    _pydantic_settings = importlib.import_module("pydantic_settings")
    BaseSettings = _pydantic_settings.BaseSettings
    SettingsConfigDict = _pydantic_settings.SettingsConfigDict
except (
    ModuleNotFoundError
):  # pragma: no cover - fallback for environments without the package

    class SettingsConfigDict(dict):
        pass

    class BaseSettings:
        model_config = SettingsConfigDict()

        def __init__(self, **overrides):
            config = getattr(self, "model_config", {})
            prefix = config.get("env_prefix", "")
            for name, default in self.__class__.__dict__.items():
                if (
                    name.startswith("_")
                    or name in {"model_config"}
                    or callable(default)
                    or isinstance(default, property)
                ):
                    continue
                value = overrides.get(
                    name, os.getenv(f"{prefix}{name.upper()}", default)
                )
                if (
                    isinstance(default, int)
                    and isinstance(value, str)
                    and value.isdigit()
                ):
                    value = int(value)
                setattr(self, name, value)


class MinioSettings(BaseSettings):
    endpoint: str = "http://minio:9000"
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin"
    bucket_name: str = "currency-raw"

    model_config = SettingsConfigDict(
        env_prefix="MINIO_", env_file=".env", extra="ignore"
    )


class PostgresSettings(BaseSettings):
    host: str = "postgres"
    port: int = 5432
    db: str = "currency_db"
    user: str = "postgres"
    password: str = "postgres"

    model_config = SettingsConfigDict(
        env_prefix="POSTGRES_", env_file=".env", extra="ignore"
    )

    @property
    def sqlalchemy_url(self) -> str:
        return str(
            URL.create(
                "postgresql+psycopg2",
                username=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.db,
            )
        )


class StreamingSettings(BaseSettings):
    bootstrap_servers: str = "localhost:19092"
    topic: str = "currency-stream"
    group_id: str = "currency-consumer-group"
    producer_interval_seconds: int = 10
    max_backoff_seconds: int = 60
    vwap_window_seconds: int = 60

    model_config = SettingsConfigDict(
        env_prefix="KAFKA_", env_file=".env", extra="ignore"
    )


@lru_cache
def get_minio_settings() -> MinioSettings:
    return MinioSettings()


@lru_cache
def get_postgres_settings() -> PostgresSettings:
    return PostgresSettings()


@lru_cache
def get_streaming_settings() -> StreamingSettings:
    return StreamingSettings()
