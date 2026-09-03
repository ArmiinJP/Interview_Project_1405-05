from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000

    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:9092"

    KAFKA_REQUEST_TOPIC: str = "price_requests"
    KAFKA_RESULT_TOPIC: str = "price_results"
    KAFKA_MONITORING_TOPIC: str = "monitoring_events"

    KAFKA_CLIENT_ID: str = "price-api"
    KAFKA_MONITORING_CLIENT_ID: str = "monitoring-producer"
    KAFKA_CONSUMER_GROUP: str = "price-api-results"

    REQUEST_TIMEOUT_SECONDS: float = 5.0

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )


settings = Settings()

