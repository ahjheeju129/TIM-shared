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
    # MySQL 설정
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
    
    # SQS 설정
    sqs_queue_urls: Dict[str, str] # 토픽 이름을 키로, 큐 URL을 값으로 저장
    sqs_region: str = "ap-northeast-2"


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
    sqs_endpoint: Optional[str] = None
    dynamodb_endpoint: Optional[str] = None
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
        """환경에 따른 설정 로드"""
        if self.environment == Environment.LOCAL:
            return self._load_local_config()
        else:
            return self._load_aws_config()
    
    def _load_local_config(self) -> AppConfig:
        """로컬 개발 환경 설정"""
        return AppConfig(
            environment=Environment.LOCAL,
            service_name=self.service_name,
            service_version=os.getenv("SERVICE_VERSION", "1.0.0-local"),
            log_level=os.getenv("LOG_LEVEL", "DEBUG"),

            sqs_endpoint=os.getenv("SQS_ENDPOINT"),
            dynamodb_endpoint=os.getenv("DYNAMODB_ENDPOINT"),
            
            database=DatabaseConfig(
                # 로컬 MySQL (Docker Compose)
                aurora_host=os.getenv("DB_HOST", "localhost"),
                aurora_port=int(os.getenv("DB_PORT", "3306")),
                aurora_database=os.getenv("DB_NAME", "currency_db"),
                aurora_username=os.getenv("DB_USER", "currency_user"),  # Docker Compose와 일치
                aurora_password=os.getenv("DB_PASSWORD", "password"),
                
                # 로컬 Redis (Docker Compose)
                redis_host=os.getenv("REDIS_HOST", "localhost"),
                redis_port=int(os.getenv("REDIS_PORT", "6379")),
                redis_password=os.getenv("REDIS_PASSWORD", ""),
                redis_ssl=False,
                
                # 로컬에서는 DynamoDB Local 사용
                # LocalStack 초기화 스크립트(01-create-aws-resources.sh)는 ap-northeast-2 리전을 사용하므로
                # 여기서도 동일한 리전을 사용하도록 ENV(AWS_REGION)과 일치시킵니다.
                dynamodb_region=os.getenv("AWS_REGION", "ap-northeast-2"),
                selections_table="travel_destination_selections",
                rankings_table="RankingResults"
            ),
            
            external_apis=ExternalAPIConfig(
                bok_api_key=os.getenv("BOK_API_KEY", ""),
                fed_api_key=os.getenv("FED_API_KEY", ""),
                backup_api_key=os.getenv("BACKUP_API_KEY", "")
            ),
            
            messaging=MessagingConfig(
                kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                kafka_security_protocol="PLAINTEXT",
                sqs_region=os.getenv("AWS_REGION", "ap-northeast-2"),
                # --- [핵심 수정: 여러 큐 URL 환경 변수를 읽어 딕셔너리로 만듦] ---
                sqs_queue_urls={
                    "exchange-rates": os.getenv("SQS_EXCHANGE_RATES_QUEUE_URL", ""),
                    "user-events": os.getenv("SQS_USER_EVENTS_QUEUE_URL", ""),
                    "ranking-events": os.getenv("SQS_RANKING_EVENTS_QUEUE_URL", ""),
                    "dlq": os.getenv("SQS_DLQ_URL", "")
                }
            ),
            
            cors_origins=["http://localhost:3000", "http://localhost:8000"]
        )
    
    def _load_aws_config(self) -> AppConfig:
        """AWS 환경 설정 (Parameter Store 사용)"""
        # TODO: AWS Parameter Store에서 설정 로드
        # 현재는 환경 변수로 대체
        return AppConfig(
            environment=self.environment,
            service_name=self.service_name,
            service_version=os.getenv("SERVICE_VERSION", "1.0.0"),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            
            database=DatabaseConfig(
                # Aurora 설정
                aurora_host=os.getenv("AURORA_ENDPOINT", ""),
                aurora_port=int(os.getenv("AURORA_PORT", "3306")),
                aurora_database=os.getenv("AURORA_DATABASE", "currency_db"),
                aurora_username=os.getenv("AURORA_USERNAME", "currency_user"),
                # TODO: Parameter Store에서 로드
                aurora_password=os.getenv("AURORA_PASSWORD", ""),
                
                # ElastiCache Redis 설정
                redis_host=os.getenv("REDIS_ENDPOINT", ""),
                redis_port=int(os.getenv("REDIS_PORT", "6379")),
                redis_ssl=True,  # AWS에서는 SSL 사용
                
                # DynamoDB 설정
                dynamodb_region=os.getenv("DYNAMODB_REGION", os.getenv("AWS_REGION", "ap-northeast-2")),
                selections_table=os.getenv("SELECTIONS_TABLE", "travel_destination_selections"),
                rankings_table=os.getenv("RANKINGS_TABLE", "RankingResults")
            ),
            
            external_apis=ExternalAPIConfig(
                # TODO: Parameter Store에서 로드
                bok_api_key=os.getenv("BOK_API_KEY", ""),
                fed_api_key=os.getenv("FED_API_KEY", ""),
                backup_api_key=os.getenv("BACKUP_API_KEY", "")
            ),
            
            messaging=MessagingConfig(
                # MSK 설정
                kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", ""),
                kafka_security_protocol="SSL",
                
                # SQS 설정
                sqs_queue_url=os.getenv("SQS_QUEUE_URL", ""),
                sqs_region=os.getenv("AWS_REGION", "ap-northeast-2")
            )
        )
    
    def _load_from_parameter_store(self, parameter_name: str) -> str:
        """AWS Parameter Store에서 값 로드"""
        # AWS 배포 시 수정 필요사항:
        # 1. IAM 역할에 ssm:GetParameter 권한 추가
        # 2. Parameter Store에 비밀번호 등 민감 정보 저장
        # 3. 아래 주석 해제
        
        # import boto3
        # try:
        #     ssm = boto3.client('ssm')
        #     response = ssm.get_parameter(Name=parameter_name, WithDecryption=True)
        #     return response['Parameter']['Value']
        # except Exception as e:
        #     logger.error(f"Failed to get parameter {parameter_name}", error=e)
        #     # 폴백으로 환경변수 사용
        #     return os.getenv(parameter_name.replace('/', '_').upper(), "")
        
        return os.getenv(parameter_name.replace('/', '_').upper(), "")


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