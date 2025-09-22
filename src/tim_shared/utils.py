"""
공통 유틸리티 함수들
날짜 처리, 데이터 변환, 검증 등
"""
import hashlib
import uuid
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Any, Optional, Union, Tuple
import asyncio
import aiohttp
from functools import wraps
import time

import logging
from .exceptions import ValidationError, InvalidParameterError

logger = logging.getLogger(__name__)


# 날짜/시간 유틸리티
class DateTimeUtils:
    """날짜/시간 관련 유틸리티"""
    
    @staticmethod
    def utc_now() -> datetime:
        """현재 UTC 시간 반환"""
        return datetime.now(timezone.utc)
    
    @staticmethod
    def to_iso_string(dt: datetime) -> str:
        """datetime을 ISO 문자열로 변환"""
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat().replace('+00:00', 'Z')
    
    @staticmethod
    def from_iso_string(iso_string: str) -> datetime:
        """ISO 문자열을 datetime으로 변환"""
        if iso_string.endswith('Z'):
            iso_string = iso_string[:-1] + '+00:00'
        return datetime.fromisoformat(iso_string)
    
    @staticmethod
    def get_date_range(period: str) -> Tuple[datetime, datetime]:
        """기간 문자열을 시작/종료 날짜로 변환"""
        end_date = DateTimeUtils.utc_now()
        
        if period == "1w":
            start_date = end_date - timedelta(weeks=1)
        elif period == "1m":
            start_date = end_date - timedelta(days=30)
        elif period == "6m":
            start_date = end_date - timedelta(days=180)
        elif period == "1y":
            start_date = end_date - timedelta(days=365)
        else:
            raise InvalidParameterError("period", period, "1w, 1m, 6m, 1y")
        
        return start_date, end_date
    
    @staticmethod
    def get_date_string(dt: datetime = None) -> str:
        """날짜를 YYYY-MM-DD 형식으로 반환"""
        if dt is None:
            dt = DateTimeUtils.utc_now()
        return dt.strftime('%Y-%m-%d')
    
    @staticmethod
    def get_timestamp_string(dt: datetime = None) -> str:
        """타임스탬프를 YYYYMMDDHHMMSS 형식으로 반환"""
        if dt is None:
            dt = DateTimeUtils.utc_now()
        return dt.strftime('%Y%m%d%H%M%S')
    
    @staticmethod
    def get_ttl_timestamp(days: int = 365) -> int:
        """TTL용 Unix 타임스탬프 반환 (기본 1년 후)"""
        future_date = DateTimeUtils.utc_now() + timedelta(days=days)
        return int(future_date.timestamp())


# 데이터 변환 유틸리티
class DataUtils:
    """데이터 변환 관련 유틸리티"""
    
    @staticmethod
    def safe_decimal(value: Union[str, int, float, Decimal], decimal_places: int = 4) -> Decimal:
        """안전한 Decimal 변환"""
        try:
            if isinstance(value, Decimal):
                return value.quantize(Decimal('0.' + '0' * decimal_places), rounding=ROUND_HALF_UP)
            
            decimal_value = Decimal(str(value))
            return decimal_value.quantize(Decimal('0.' + '0' * decimal_places), rounding=ROUND_HALF_UP)
        except (ValueError, TypeError) as e:
            raise ValidationError(f"Invalid decimal value: {value}", "value", value)
    
    @staticmethod
    def safe_float(value: Union[str, int, float, Decimal]) -> float:
        """안전한 float 변환"""
        try:
            return float(value)
        except (ValueError, TypeError) as e:
            raise ValidationError(f"Invalid float value: {value}", "value", value)
    
    @staticmethod
    def safe_int(value: Union[str, int, float]) -> int:
        """안전한 int 변환"""
        try:
            return int(value)
        except (ValueError, TypeError) as e:
            raise ValidationError(f"Invalid integer value: {value}", "value", value)
    
    @staticmethod
    def calculate_percentage(part: Union[int, float], total: Union[int, float], decimal_places: int = 2) -> Decimal:
        """백분율 계산"""
        if total == 0:
            return Decimal('0')
        
        percentage = (part / total) * 100
        return DataUtils.safe_decimal(percentage, decimal_places)
    
    @staticmethod
    def calculate_change_percent(old_value: Union[int, float, Decimal], new_value: Union[int, float, Decimal]) -> Decimal:
        """변화율 계산 (백분율)"""
        if old_value == 0:
            return Decimal('0')
        
        change = ((new_value - old_value) / old_value) * 100
        return DataUtils.safe_decimal(change, 4)
    
    @staticmethod
    def round_to_significant_digits(value: Union[int, float, Decimal], digits: int = 4) -> Decimal:
        """유효숫자로 반올림"""
        if value == 0:
            return Decimal('0')
        
        decimal_value = Decimal(str(value))
        return decimal_value.quantize(Decimal('0.' + '0' * digits), rounding=ROUND_HALF_UP)


# 검증 유틸리티
class ValidationUtils:
    """데이터 검증 관련 유틸리티"""
    
    @staticmethod
    def validate_currency_code(currency_code: str) -> str:
        """통화 코드 검증"""
        if not currency_code or not isinstance(currency_code, str):
            raise ValidationError("Currency code is required", "currency_code", currency_code)
        
        currency_code = currency_code.upper().strip()
        
        if not re.match(r'^[A-Z]{3}$', currency_code):
            raise ValidationError("Currency code must be 3 uppercase letters", "currency_code", currency_code)
        
        # 지원하는 통화 코드 목록
        supported_currencies = {"USD", "JPY", "EUR", "GBP", "CNY", "AUD", "CAD", "CHF", "HKD", "SGD", "KRW"}
        
        if currency_code not in supported_currencies:
            raise ValidationError(
                f"Unsupported currency code: {currency_code}",
                "currency_code",
                currency_code
            )
        
        return currency_code
    
    @staticmethod
    def validate_country_code(country_code: str) -> str:
        """국가 코드 검증"""
        if not country_code or not isinstance(country_code, str):
            raise ValidationError("Country code is required", "country_code", country_code)
        
        country_code = country_code.upper().strip()
        
        if not re.match(r'^[A-Z]{2,3}$', country_code):
            raise ValidationError("Country code must be 2-3 uppercase letters", "country_code", country_code)
        
        # 지원하는 국가 코드 목록
        supported_countries = {"US", "JP", "EU", "GB", "CN", "AU", "CA", "CH", "HK", "SG", "KR"}
        
        if country_code not in supported_countries:
            raise ValidationError(
                f"Unsupported country code: {country_code}",
                "country_code",
                country_code
            )
        
        return country_code
    
    @staticmethod
    def validate_period(period: str, valid_periods: List[str]) -> str:
        """기간 검증"""
        if not period or not isinstance(period, str):
            raise ValidationError("Period is required", "period", period)
        
        period = period.lower().strip()
        
        if period not in valid_periods:
            raise ValidationError(
                f"Invalid period: {period}. Valid periods: {', '.join(valid_periods)}",
                "period",
                period
            )
        
        return period
    
    @staticmethod
    def validate_user_id(user_id: str) -> str:
        """사용자 ID 검증"""
        if not user_id or not isinstance(user_id, str):
            raise ValidationError("User ID is required", "user_id", user_id)
        
        user_id = user_id.strip()
        
        if len(user_id) < 1 or len(user_id) > 100:
            raise ValidationError("User ID must be 1-100 characters", "user_id", user_id)
        
        # 특수문자 제한 (기본적인 영숫자, 하이픈, 언더스코어만 허용)
        if not re.match(r'^[a-zA-Z0-9_-]+$', user_id):
            raise ValidationError("User ID contains invalid characters", "user_id", user_id)
        
        return user_id
    
    @staticmethod
    def validate_positive_number(value: Union[int, float, str], field_name: str) -> Union[int, float]:
        """양수 검증"""
        try:
            num_value = float(value)
            if num_value <= 0:
                raise ValidationError(f"{field_name} must be positive", field_name, value)
            return num_value
        except (ValueError, TypeError):
            raise ValidationError(f"Invalid number format for {field_name}", field_name, value)


# 보안 유틸리티
class SecurityUtils:
    """보안 관련 유틸리티"""
    
    @staticmethod
    def hash_string(text: str, algorithm: str = 'sha256') -> str:
        """문자열 해시화"""
        if algorithm == 'sha256':
            return hashlib.sha256(text.encode('utf-8')).hexdigest()
        elif algorithm == 'md5':
            return hashlib.md5(text.encode('utf-8')).hexdigest()
        else:
            raise ValueError(f"Unsupported hash algorithm: {algorithm}")
    
    @staticmethod
    def generate_uuid() -> str:
        """UUID 생성"""
        return str(uuid.uuid4())
    
    @staticmethod
    def generate_correlation_id() -> str:
        """상관관계 ID 생성"""
        return f"req_{int(time.time())}_{uuid.uuid4().hex[:8]}"
    
    @staticmethod
    def sanitize_user_input(text: str, max_length: int = 1000) -> str:
        """사용자 입력 정제"""
        if not text:
            return ""
        
        # 기본 정제: 앞뒤 공백 제거, 길이 제한
        sanitized = text.strip()[:max_length]
        
        # HTML 태그 제거 (기본적인 XSS 방지)
        sanitized = re.sub(r'<[^>]+>', '', sanitized)
        
        # SQL 인젝션 방지를 위한 기본 문자 이스케이프
        dangerous_chars = ["'", '"', ';', '--', '/*', '*/']
        for char in dangerous_chars:
            sanitized = sanitized.replace(char, '')
        
        return sanitized


# HTTP 클라이언트 유틸리티
class HTTPUtils:
    """HTTP 요청 관련 유틸리티"""
    
    @staticmethod
    async def make_request(
        method: str,
        url: str,
        headers: Dict[str, str] = None,
        params: Dict[str, Any] = None,
        json_data: Dict[str, Any] = None,
        timeout: int = 30,
        retries: int = 3
    ) -> Dict[str, Any]:
        """HTTP 요청 실행 (재시도 포함)"""
        
        headers = headers or {}
        headers.setdefault('User-Agent', 'CurrencyService/1.0')
        
        for attempt in range(retries + 1):
            try:
                timeout_config = aiohttp.ClientTimeout(total=timeout)
                
                async with aiohttp.ClientSession(timeout=timeout_config) as session:
                    async with session.request(
                        method=method,
                        url=url,
                        headers=headers,
                        params=params,
                        json=json_data
                    ) as response:
                        
                        response_data = {
                            'status_code': response.status,
                            'headers': dict(response.headers),
                            'url': str(response.url)
                        }
                        
                        # 응답 본문 처리
                        content_type = response.headers.get('content-type', '').lower()
                        
                        if 'application/json' in content_type:
                            response_data['data'] = await response.json()
                        else:
                            response_data['data'] = await response.text()
                        
                        # HTTP 에러 상태 코드 체크
                        if response.status >= 400:
                            logger.warning(
                                f"HTTP request failed",
                                method=method,
                                url=url,
                                status_code=response.status,
                                attempt=attempt + 1
                            )
                            
                            if attempt == retries:  # 마지막 시도
                                raise aiohttp.ClientResponseError(
                                    request_info=response.request_info,
                                    history=response.history,
                                    status=response.status,
                                    message=f"HTTP {response.status}"
                                )
                        else:
                            return response_data
                            
            except asyncio.TimeoutError:
                logger.warning(
                    f"HTTP request timeout",
                    method=method,
                    url=url,
                    timeout=timeout,
                    attempt=attempt + 1
                )
                
                if attempt == retries:
                    raise
                    
            except Exception as e:
                logger.warning(
                    f"HTTP request error",
                    method=method,
                    url=url,
                    error=str(e),
                    attempt=attempt + 1
                )
                
                if attempt == retries:
                    raise
            
            # 재시도 전 대기 (지수 백오프)
            if attempt < retries:
                wait_time = 2 ** attempt
                await asyncio.sleep(wait_time)


# 성능 측정 유틸리티
class PerformanceUtils:
    """성능 측정 관련 유틸리티"""
    
    @staticmethod
    def measure_time(func):
        """함수 실행 시간 측정 데코레이터"""
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                return result
            finally:
                end_time = time.time()
                duration_ms = (end_time - start_time) * 1000
                logger.debug(
                    f"Function execution completed",
                    function=func.__name__,
                    duration_ms=round(duration_ms, 2)
                )
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                end_time = time.time()
                duration_ms = (end_time - start_time) * 1000
                logger.debug(
                    f"Function execution completed",
                    function=func.__name__,
                    duration_ms=round(duration_ms, 2)
                )
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper


# 캐시 키 생성 유틸리티
class CacheUtils:
    """캐시 키 생성 관련 유틸리티"""
    
    @staticmethod
    def generate_cache_key(prefix: str, *args, **kwargs) -> str:
        """캐시 키 생성"""
        key_parts = [prefix]
        
        # 위치 인자들 추가
        for arg in args:
            key_parts.append(str(arg))
        
        # 키워드 인자들 추가 (정렬된 순서로)
        for key, value in sorted(kwargs.items()):
            key_parts.append(f"{key}:{value}")
        
        return ":".join(key_parts)
    
    @staticmethod
    def generate_rate_cache_key(currency_code: str) -> str:
        """환율 캐시 키 생성"""
        return f"rate:{currency_code.upper()}"
    
    @staticmethod
    def generate_ranking_cache_key(period: str) -> str:
        """랭킹 캐시 키 생성"""
        return f"ranking:{period}"
    
    @staticmethod
    def generate_history_cache_key(period: str, base: str, target: str) -> str:
        """이력 캐시 키 생성"""
        return f"chart:{period}:{base.upper()}:{target.upper()}"


# 페이지네이션 유틸리티
class PaginationUtils:
    """페이지네이션 관련 유틸리티"""
    
    @staticmethod
    def calculate_pagination(
        total_items: int,
        page: int = 1,
        items_per_page: int = 10
    ) -> Dict[str, Any]:
        """페이지네이션 정보 계산"""
        
        # 입력값 검증
        page = max(1, page)
        items_per_page = max(1, min(100, items_per_page))  # 최대 100개로 제한
        
        total_pages = (total_items + items_per_page - 1) // items_per_page
        total_pages = max(1, total_pages)
        
        # 현재 페이지가 총 페이지 수를 초과하는 경우
        if page > total_pages:
            page = total_pages
        
        offset = (page - 1) * items_per_page
        
        return {
            'current_page': page,
            'total_pages': total_pages,
            'items_per_page': items_per_page,
            'total_items': total_items,
            'offset': offset,
            'limit': items_per_page,
            'has_next': page < total_pages,
            'has_previous': page > 1
        }


# 통계 계산 유틸리티
class StatisticsUtils:
    """통계 계산 관련 유틸리티"""
    
    @staticmethod
    def calculate_basic_stats(values: List[Union[int, float]]) -> Dict[str, float]:
        """기본 통계 계산 (평균, 최소, 최대, 표준편차)"""
        if not values:
            return {
                'count': 0,
                'mean': 0,
                'min': 0,
                'max': 0,
                'std_dev': 0
            }
        
        count = len(values)
        mean = sum(values) / count
        min_val = min(values)
        max_val = max(values)
        
        # 표준편차 계산
        if count > 1:
            variance = sum((x - mean) ** 2 for x in values) / (count - 1)
            std_dev = variance ** 0.5
        else:
            std_dev = 0
        
        return {
            'count': count,
            'mean': round(mean, 4),
            'min': min_val,
            'max': max_val,
            'std_dev': round(std_dev, 4)
        }
    
    @staticmethod
    def calculate_percentiles(values: List[Union[int, float]], percentiles: List[int] = None) -> Dict[str, float]:
        """백분위수 계산"""
        if not values:
            return {}
        
        if percentiles is None:
            percentiles = [25, 50, 75, 95, 99]
        
        sorted_values = sorted(values)
        n = len(sorted_values)
        
        result = {}
        for p in percentiles:
            if p < 0 or p > 100:
                continue
            
            index = (p / 100) * (n - 1)
            
            if index.is_integer():
                result[f'p{p}'] = sorted_values[int(index)]
            else:
                lower_index = int(index)
                upper_index = lower_index + 1
                
                if upper_index < n:
                    weight = index - lower_index
                    result[f'p{p}'] = (
                        sorted_values[lower_index] * (1 - weight) +
                        sorted_values[upper_index] * weight
                    )
                else:
                    result[f'p{p}'] = sorted_values[lower_index]
        
        return result