"""
공통 데이터 모델 정의
Pydantic 모델을 사용한 데이터 검증 및 직렬화
"""
from datetime import datetime
from decimal import Decimal
from typing import List, Optional, Dict, Any, Union
from enum import Enum
from pydantic import BaseModel, Field, field_validator, model_validator


# 공통 Enum 정의
class CurrencyCode(str, Enum):
    """지원하는 통화 코드"""
    USD = "USD"
    JPY = "JPY"
    EUR = "EUR"
    GBP = "GBP"
    CNY = "CNY"
    AUD = "AUD"
    CAD = "CAD"
    CHF = "CHF"
    HKD = "HKD"
    SGD = "SGD"
    KRW = "KRW"
    PRICE_INDEX = "PRICE-INDEX"  # 물가 지수 조회용 특별 코드


class CountryCode(str, Enum):
    """지원하는 국가 코드"""
    US = "US"
    JP = "JP"
    EU = "EU"
    GB = "GB"
    CN = "CN"
    AU = "AU"
    CA = "CA"
    CH = "CH"
    HK = "HK"
    SG = "SG"
    KR = "KR"


class RankingPeriod(str, Enum):
    """랭킹 기간"""
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class HistoryPeriod(str, Enum):
    """이력 조회 기간"""
    ONE_WEEK = "1w"
    ONE_MONTH = "1m"
    SIX_MONTHS = "6m"


class TrendDirection(str, Enum):
    """트렌드 방향"""
    UPWARD = "upward"
    DOWNWARD = "downward"
    STABLE = "stable"


class RankChange(str, Enum):
    """순위 변동"""
    UP = "UP"
    DOWN = "DOWN"
    SAME = "SAME"
    NEW = "NEW"


# 기본 응답 모델
class BaseResponse(BaseModel):
    """기본 API 응답 모델"""
    success: bool = True
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    version: str = "v1"
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() + 'Z'
        }


class ErrorResponse(BaseResponse):
    """에러 응답 모델"""
    success: bool = False
    error: Dict[str, Any]


class SuccessResponse(BaseResponse):
    """성공 응답 모델"""
    data: Dict[str, Any]


# Currency Service 모델들
class ExchangeRate(BaseModel):
    """환율 정보 모델"""
    currency_code: str
    currency_name: str
    deal_base_rate: Decimal
    tts: Optional[Decimal] = None  # 송금 보낼 때
    ttb: Optional[Decimal] = None  # 받을 때
    source: str
    recorded_at: datetime
    
    class Config:
        json_encoders = {
            Decimal: lambda v: float(v),
            datetime: lambda v: v.isoformat() + 'Z'
        }


class CurrencyInfo(BaseModel):
    """통화 상세 정보 모델"""
    currency_code: str
    currency_name: str
    country_code: str
    country_name: str
    symbol: str
    current_rate: Decimal
    tts: Optional[Decimal] = None
    ttb: Optional[Decimal] = None
    last_updated: datetime
    source: str
    
    class Config:
        json_encoders = {
            Decimal: lambda v: float(v),
            datetime: lambda v: v.isoformat() + 'Z'
        }


class LatestRatesRequest(BaseModel):
    """최신 환율 조회 요청"""
    symbols: Optional[List[str]] = None
    base: str = "KRW"


class LatestRatesResponse(SuccessResponse):
    """최신 환율 조회 응답"""
    data: Dict[str, Any] = Field(..., example={
        "base": "KRW",
        "timestamp": 1725525000,
        "rates": {
            "USD": 1392.4,
            "JPY": 9.46
        },
        "source": "redis_cache",
        "cache_hit": True
    })


class PriceIndex(BaseModel):
    """물가 지수 모델"""
    country_code: str
    country_name: str
    base_country: str = "KR"
    bigmac_index: Decimal
    starbucks_index: Decimal
    composite_index: Decimal
    price_data: Dict[str, Any]
    last_updated: datetime
    
    class Config:
        json_encoders = {
            Decimal: lambda v: float(v),
            datetime: lambda v: v.isoformat() + 'Z'
        }


# Ranking Service 모델들
class UserSelection(BaseModel):
    """사용자 선택 기록 모델"""
    user_id: str = Field(..., min_length=1, max_length=100)
    country_code: str
    session_id: Optional[str] = Field(None, max_length=100)
    referrer: Optional[str] = Field(None, max_length=500)
    
    @field_validator('user_id')
    @classmethod
    def validate_user_id(cls, v):
        if not v or v.isspace():
            raise ValueError('user_id cannot be empty or whitespace')
        return v


class SelectionRecord(BaseModel):
    """선택 기록 (DB 저장용)"""
    selection_date: str  # YYYY-MM-DD
    selection_timestamp_userid: str  # YYYYMMDDHHMMSS_userid
    country_code: str
    country_name: str
    user_id: str
    session_id: Optional[str] = None
    ip_address_hash: Optional[str] = None
    user_agent_hash: Optional[str] = None
    referrer: Optional[str] = None
    created_at: datetime
    ttl: int  # DynamoDB TTL
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() + 'Z'
        }


class RankingItem(BaseModel):
    """랭킹 아이템 모델"""
    rank: int = Field(..., ge=1)
    country_code: str
    country_name: str
    score: int = Field(..., ge=0)
    percentage: Decimal = Field(..., ge=0, le=100)
    change: str
    change_value: int = 0
    previous_rank: Optional[int] = Field(None, ge=1)
    
    class Config:
        json_encoders = {
            Decimal: lambda v: float(v)
        }


class RankingResponse(SuccessResponse):
    """랭킹 조회 응답"""
    data: Dict[str, Any] = Field(..., example={
        "period": "daily",
        "total_selections": 9876,
        "last_updated": "2025-09-05T10:00:00Z",
        "ranking": [
            {
                "rank": 1,
                "country_code": "JP",
                "country_name": "일본",
                "score": 1502,
                "percentage": 15.2,
                "change": "UP",
                "change_value": 2
            }
        ]
    })


class CountryStats(BaseModel):
    """국가별 통계 모델"""
    country_code: str
    country_name: str
    period: str
    total_selections: int = Field(..., ge=0)
    daily_average: Decimal = Field(..., ge=0)
    peak_day: Optional[str] = None
    peak_selections: Optional[int] = Field(None, ge=0)
    growth_rate: Optional[Decimal] = None
    daily_breakdown: List[Dict[str, Any]] = []
    
    class Config:
        json_encoders = {
            Decimal: lambda v: float(v)
        }


# History Service 모델들
class HistoryDataPoint(BaseModel):
    """이력 데이터 포인트"""
    date: str  # YYYY-MM-DD
    rate: Decimal
    change: Decimal
    change_percent: Decimal
    volume: Optional[int] = Field(None, ge=0)
    
    class Config:
        json_encoders = {
            Decimal: lambda v: float(v)
        }


class HistoryStatistics(BaseModel):
    """이력 통계 모델"""
    average: Decimal
    min: Decimal
    max: Decimal
    volatility: Decimal
    trend: str
    correlation: Optional[Decimal] = None
    data_points: int = Field(..., ge=0)
    
    class Config:
        json_encoders = {
            Decimal: lambda v: float(v)
        }


class HistoryRequest(BaseModel):
    """환율 이력 조회 요청"""
    period: str
    target: str
    base: str = "KRW"
    interval: str = Field("daily", pattern="^(daily|hourly)$")


class HistoryResponse(SuccessResponse):
    """환율 이력 조회 응답"""
    data: Dict[str, Any] = Field(..., example={
        "base": "KRW",
        "target": "USD",
        "period": "1m",
        "interval": "daily",
        "data_points": 30,
        "results": [
            {
                "date": "2025-08-05",
                "rate": 1380.5,
                "change": 2.3,
                "change_percent": 0.17
            }
        ],
        "statistics": {
            "average": 1385.2,
            "min": 1375.8,
            "max": 1395.6,
            "volatility": 0.85,
            "trend": "stable"
        }
    })


class TechnicalIndicators(BaseModel):
    """기술적 지표 모델"""
    sma_20: Optional[Decimal] = None  # 20일 이동평균
    sma_50: Optional[Decimal] = None  # 50일 이동평균
    rsi: Optional[Decimal] = None     # RSI
    bollinger_upper: Optional[Decimal] = None
    bollinger_lower: Optional[Decimal] = None
    
    class Config:
        json_encoders = {
            Decimal: lambda v: float(v)
        }


class CurrencyComparison(BaseModel):
    """통화 비교 모델"""
    currency: str
    current_rate: Decimal
    period_change_percent: Decimal
    volatility: Decimal
    performance_rank: int = Field(..., ge=1)
    sharpe_ratio: Optional[Decimal] = None
    
    class Config:
        json_encoders = {
            Decimal: lambda v: float(v)
        }


# Data Ingestor 모델들
class ExternalAPISource(BaseModel):
    """외부 API 소스 정보"""
    name: str
    url: str
    api_key: Optional[str] = None
    timeout: int = 30
    retry_count: int = 3
    is_active: bool = True


class CollectionResult(BaseModel):
    """데이터 수집 결과"""
    source: str
    success: bool
    currency_count: int = 0
    error_message: Optional[str] = None
    collection_time: datetime
    processing_time_ms: int
    raw_data: Optional[List[Any]] = None  # RawExchangeRateData 리스트
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() + 'Z'
        }


class RawExchangeRateData(BaseModel):
    """원시 환율 데이터 (수집된 데이터)"""
    currency_code: str
    rate: Union[str, float, Decimal]
    source: str
    timestamp: datetime
    metadata: Optional[Dict[str, Any]] = {}
    
    @field_validator('currency_code')
    @classmethod
    def validate_currency_code(cls, v):
        return v.upper()
    
    @field_validator('rate')
    @classmethod
    def validate_rate(cls, v):
        try:
            rate_value = float(v)
            if rate_value <= 0:
                raise ValueError('Rate must be positive')
            return Decimal(str(rate_value))
        except (ValueError, TypeError):
            raise ValueError('Invalid rate format')


# 공통 유틸리티 모델들
class HealthCheck(BaseModel):
    """헬스 체크 모델"""
    status: str = "healthy"
    services: Dict[str, Dict[str, Any]] = {}
    dependencies: Dict[str, str] = {}
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() + 'Z'
        }


class ServiceInfo(BaseModel):
    """서비스 정보 모델"""
    api_version: str
    services: Dict[str, str]
    deployment_date: datetime
    environment: str
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() + 'Z'
        }


class PaginationInfo(BaseModel):
    """페이지네이션 정보"""
    current_page: int = Field(..., ge=1)
    total_pages: int = Field(..., ge=1)
    has_next: bool
    has_previous: bool
    total_items: Optional[int] = Field(None, ge=0)
    items_per_page: int = Field(..., ge=1)


# 검증 헬퍼 함수들
def validate_currency_code(currency_code: str) -> str:
    """통화 코드 검증"""
    try:
        return currency_code.upper()
    except ValueError:
        raise ValueError(f"Unsupported currency code: {currency_code}")


def validate_country_code(country_code: str) -> str:
    """국가 코드 검증"""
    try:
        return country_code.upper()
    except ValueError:
        raise ValueError(f"Unsupported country code: {country_code}")


def validate_period(period: str, period_type: str = "history") -> str:
    """기간 검증"""
    return period


# 모델 변환 헬퍼 함수들
def exchange_rate_to_dict(rate: ExchangeRate) -> Dict[str, Any]:
    """ExchangeRate 모델을 딕셔너리로 변환"""
    return {
        "currency_code": rate.currency_code,
        "currency_name": rate.currency_name,
        "deal_base_rate": float(rate.deal_base_rate),
        "tts": float(rate.tts) if rate.tts else None,
        "ttb": float(rate.ttb) if rate.ttb else None,
        "source": rate.source,
        "recorded_at": rate.recorded_at.isoformat() + 'Z'
    }


def dict_to_exchange_rate(data: Dict[str, Any]) -> ExchangeRate:
    """딕셔너리를 ExchangeRate 모델로 변환"""
    return ExchangeRate(
        currency_code=data["currency_code"],
        currency_name=data["currency_name"],
        deal_base_rate=Decimal(str(data["deal_base_rate"])),
        tts=Decimal(str(data["tts"])) if data.get("tts") else None,
        ttb=Decimal(str(data["ttb"])) if data.get("ttb") else None,
        source=data["source"],
        recorded_at=datetime.fromisoformat(data["recorded_at"].replace('Z', '+00:00'))
    )