"""
공통 설정 관리 모듈
로컬 개발 환경과 AWS 환경 모두 지원
"""
import os
from typing import Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum


class Environment(Enum):
    LOCAL = "local"
    DEVELOPMENT = "dev"
    STAGING = "staging"
    PRODUCTION = "prod"


@dataclass
class DatabaseConfig:
    """데이터베이스 설정"""
    # MySQL 설정 (환율 데이터용)
    mysql_host: str = "localhost"
    mysql_port: int = 3306
    mysql_database: str = "currency_db"
    mysql_username: str = "currency_user"
    mysql_password: str = ""
    
    # Redis 설정
    redis_host: str = "localhost"  # 기본값 추가
    redis_port: int = 6379
    redis_password: str = ""  # 로컬에서는 빈 문자열
    redis_ssl: bool = False
    
    # MongoDB 설정
    mongodb_host: str = "localhost"
    mongodb_port: int = 27017
    mongodb_database: str = "ranking_db"
    mongodb_username: str = "ranking_user"
    mongodb_password: str = ""
    mongodb_auth_source: str = "admin"
    selections_collection: str = "travel_destination_selections"
    rankings_collection: str = "ranking_results"


@dataclass
class ExternalAPIConfig:
    """외부 API 설정"""
    # 한국은행 API
    bok_api_key: str = "ZURBIILP4LALM1P1VYTO"
    bok_base_url: str = "https://ecos.bok.or.kr/api"

@dataclass
class MessagingConfig:
    """메시징 설정"""
    # Kafka 설정
    kafka_bootstrap_servers: str
    kafka_security_protocol: str
    kafka_topics: Dict[str, str]  # 토픽 이름을 키로, 토픽 이름을 값으로 저장


@dataclass
class AppConfig:
    """애플리케이션 전체 설정"""
    environment: Environment
    service_name: str
    service_version: str = "1.0.0"
    
    # 로깅 설정
    log_level: str = "INFO"
    log_format: str = "json"  # json 또는 text
    
    # 성능 설정
    request_timeout: int = 30
    max_retries: int = 3
    cache_ttl: int = 600  # 10분
    
    # 보안 설정
    cors_origins: list = None
    rate_limit_per_minute: int = 100
    
    # 데이터베이스 설정
    database: DatabaseConfig = None
    
    # 외부 API 설정
    external_apis: ExternalAPIConfig = None
    
    # 메시징 설정
    messaging: MessagingConfig = None


class ConfigManager:
    """설정 관리자"""
    
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.environment = Environment(os.getenv("ENVIRONMENT", "local"))
        self._config = None
    
    def get_config(self) -> AppConfig:
        """설정 로드 (캐싱됨)"""
        if self._config is None:
            self._config = self._load_config()
        return self._config
    
    def _load_config(self) -> AppConfig:
        return AppConfig(
            environment=Environment.LOCAL,
            service_name=self.service_name,
            service_version=os.getenv("SERVICE_VERSION", "1.0.0-local"),
            log_level=os.getenv("LOG_LEVEL", "DEBUG"),

            sqs_endpoint=os.getenv("SQS_ENDPOINT"),
            
            database=DatabaseConfig(
                # 로컬 MySQL (Docker Compose)
                mysql_host=os.getenv("DB_HOST", "localhost"),
                mysql_port=int(os.getenv("DB_PORT", "3306")),
                mysql_database=os.getenv("DB_NAME", "currency_db"),
                mysql_username=os.getenv("DB_USER", "currency_user"),  # Docker Compose와 일치
                mysql_password=os.getenv("DB_PASSWORD", "password"),
                
                # 로컬 Redis (Docker Compose)
                redis_host=os.getenv("REDIS_HOST", "localhost"),
                redis_port=int(os.getenv("REDIS_PORT", "6379")),
                redis_password=os.getenv("REDIS_PASSWORD", ""),
                redis_ssl=False,
                
                # MongoDB 설정 (여행지 선택 및 랭킹 데이터용)
                mongodb_host=os.getenv("MONGODB_HOST", "localhost"),
                mongodb_port=int(os.getenv("MONGODB_PORT", "27017")),
                mongodb_database=os.getenv("MONGODB_DATABASE", "ranking_db"),
                mongodb_username=os.getenv("MONGODB_USERNAME", "ranking_user"),
                mongodb_password=os.getenv("MONGODB_PASSWORD", ""),
                mongodb_auth_source=os.getenv("MONGODB_AUTH_SOURCE", "admin"),
                selections_collection=os.getenv("SELECTIONS_COLLECTION", "travel_destination_selections"),
                rankings_collection=os.getenv("RANKINGS_COLLECTION", "ranking_results")
            ),
            
            external_apis=ExternalAPIConfig(
                bok_api_key=os.getenv("BOK_API_KEY", "")
            ),
            
            messaging=MessagingConfig(
                kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                kafka_security_protocol="PLAINTEXT",
                kafka_topics={
                    "exchange-rates": os.getenv("KAFKA_EXCHANGE_RATES_TOPIC", "exchange-rates"),
                    "user-events": os.getenv("KAFKA_USER_EVENTS_TOPIC", "user-events"),
                    "ranking-events": os.getenv("KAFKA_RANKING_EVENTS_TOPIC", "ranking-events"),
                    "dlq": os.getenv("KAFKA_DLQ_TOPIC", "dlq")
                }
            ),
            
            cors_origins=["http://localhost:3000", "http://localhost:8000"]
        )
    
# 전역 설정 인스턴스 (서비스별로 초기화)
config_manager: Optional[ConfigManager] = None


def init_config(service_name: str) -> AppConfig:
    """설정 초기화"""
    global config_manager
    config_manager = ConfigManager(service_name)
    return config_manager.get_config()


def get_config() -> AppConfig:
    """현재 설정 반환"""
    if config_manager is None:
        raise RuntimeError("Config not initialized. Call init_config() first.")
    return config_manager.get_config()