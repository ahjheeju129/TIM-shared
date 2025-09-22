"""
공통 예외 클래스 정의
서비스 전반에서 사용되는 커스텀 예외들
"""
from typing import Dict, Any, Optional


class BaseServiceException(Exception):
    """서비스 기본 예외 클래스"""
    
    def __init__(
        self, 
        message: str, 
        error_code: str = None, 
        details: Dict[str, Any] = None,
        status_code: int = 500
    ):
        super().__init__(message)
        self.message = message
        self.error_code = error_code or self.__class__.__name__
        self.details = details or {}
        self.status_code = status_code
    
    def to_dict(self) -> Dict[str, Any]:
        """예외를 딕셔너리로 변환"""
        return {
            "code": self.error_code,
            "message": self.message,
            "details": self.details
        }


# 클라이언트 에러 (4xx)
class ClientError(BaseServiceException):
    """클라이언트 에러 기본 클래스"""
    
    def __init__(self, message: str, error_code: str = None, details: Dict[str, Any] = None):
        super().__init__(message, error_code, details, 400)


class ValidationError(ClientError):
    """데이터 검증 에러"""
    
    def __init__(self, message: str, field: str = None, value: Any = None):
        details = {}
        if field:
            details["field"] = field
        if value is not None:
            details["provided_value"] = str(value)
        
        super().__init__(message, "VALIDATION_ERROR", details)


class InvalidParameterError(ClientError):
    """잘못된 파라미터 에러"""
    
    def __init__(self, parameter: str, value: Any = None, expected: str = None):
        message = f"Invalid parameter: {parameter}"
        details = {"parameter": parameter}
        
        if value is not None:
            details["provided"] = str(value)
        if expected:
            details["expected"] = expected
        
        super().__init__(message, "INVALID_PARAMETER", details)


class MissingParameterError(ClientError):
    """필수 파라미터 누락 에러"""
    
    def __init__(self, parameter: str):
        message = f"Required parameter missing: {parameter}"
        details = {"parameter": parameter}
        super().__init__(message, "MISSING_PARAMETER", details)


class InvalidCurrencyCodeError(ClientError):
    """잘못된 통화 코드 에러"""
    
    def __init__(self, currency_code: str):
        message = f"Unsupported currency code: {currency_code}"
        details = {
            "currency_code": currency_code,
            "supported_currencies": ["USD", "JPY", "EUR", "GBP", "CNY", "AUD", "CAD", "CHF", "HKD", "SGD"]
        }
        super().__init__(message, "INVALID_CURRENCY_CODE", details)


class InvalidCountryCodeError(ClientError):
    """잘못된 국가 코드 에러"""
    
    def __init__(self, country_code: str):
        message = f"Unsupported country code: {country_code}"
        details = {
            "country_code": country_code,
            "supported_countries": ["US", "JP", "EU", "GB", "CN", "AU", "CA", "CH", "HK", "SG", "KR"]
        }
        super().__init__(message, "INVALID_COUNTRY_CODE", details)


class InvalidPeriodError(ClientError):
    """잘못된 기간 에러"""
    
    def __init__(self, period: str, valid_periods: list):
        message = f"Invalid period: {period}"
        details = {
            "period": period,
            "valid_periods": valid_periods
        }
        super().__init__(message, "INVALID_PERIOD", details)


class RateLimitExceededError(ClientError):
    """요청 제한 초과 에러"""
    
    def __init__(self, limit: int, window: int, retry_after: int = None):
        message = f"Rate limit exceeded: {limit} requests per {window} seconds"
        details = {
            "limit": limit,
            "window_seconds": window
        }
        if retry_after:
            details["retry_after_seconds"] = retry_after
        
        super().__init__(message, "RATE_LIMIT_EXCEEDED", details)
        self.status_code = 429


class NotFoundError(ClientError):
    """리소스를 찾을 수 없음 에러"""
    
    def __init__(self, resource: str, identifier: str = None):
        message = f"Resource not found: {resource}"
        details = {"resource": resource}
        if identifier:
            details["identifier"] = identifier
        
        super().__init__(message, "NOT_FOUND", details)
        self.status_code = 404


# 서버 에러 (5xx)
class ServerError(BaseServiceException):
    """서버 에러 기본 클래스"""
    
    def __init__(self, message: str, error_code: str = None, details: Dict[str, Any] = None):
        super().__init__(message, error_code, details, 500)


class DatabaseError(ServerError):
    """데이터베이스 에러"""
    
    def __init__(self, message: str, operation: str = None, table: str = None):
        details = {}
        if operation:
            details["operation"] = operation
        if table:
            details["table"] = table
        
        super().__init__(message, "DATABASE_ERROR", details)


class CacheError(ServerError):
    """캐시 에러"""
    
    def __init__(self, message: str, operation: str = None, key: str = None):
        details = {}
        if operation:
            details["operation"] = operation
        if key:
            details["key"] = key
        
        super().__init__(message, "CACHE_ERROR", details)


class ExternalAPIError(ServerError):
    """외부 API 에러"""
    
    def __init__(self, message: str, api_name: str = None, status_code: int = None, response_body: str = None):
        details = {}
        if api_name:
            details["api_name"] = api_name
        if status_code:
            details["api_status_code"] = status_code
        if response_body:
            details["api_response"] = response_body[:500]  # 응답 내용 제한
        
        super().__init__(message, "EXTERNAL_API_ERROR", details)


class MessagingError(ServerError):
    """메시징 시스템 에러"""
    
    def __init__(self, message: str, system: str = None, topic: str = None):
        details = {}
        if system:
            details["messaging_system"] = system
        if topic:
            details["topic"] = topic
        
        super().__init__(message, "MESSAGING_ERROR", details)


class ConfigurationError(ServerError):
    """설정 에러"""
    
    def __init__(self, message: str, config_key: str = None):
        details = {}
        if config_key:
            details["config_key"] = config_key
        
        super().__init__(message, "CONFIGURATION_ERROR", details)


class ServiceUnavailableError(ServerError):
    """서비스 사용 불가 에러"""
    
    def __init__(self, message: str, service: str = None, retry_after: int = None):
        details = {}
        if service:
            details["service"] = service
        if retry_after:
            details["retry_after_seconds"] = retry_after
        
        super().__init__(message, "SERVICE_UNAVAILABLE", details)
        self.status_code = 503


# 데이터 처리 관련 에러
class DataProcessingError(ServerError):
    """데이터 처리 에러"""
    
    def __init__(self, message: str, data_type: str = None, processing_step: str = None):
        details = {}
        if data_type:
            details["data_type"] = data_type
        if processing_step:
            details["processing_step"] = processing_step
        
        super().__init__(message, "DATA_PROCESSING_ERROR", details)


class DataValidationError(ServerError):
    """데이터 검증 에러"""
    
    def __init__(self, message: str, data_source: str = None, validation_rule: str = None):
        details = {}
        if data_source:
            details["data_source"] = data_source
        if validation_rule:
            details["validation_rule"] = validation_rule
        
        super().__init__(message, "DATA_VALIDATION_ERROR", details)


# 비즈니스 로직 관련 에러
class BusinessLogicError(ServerError):
    """비즈니스 로직 에러"""
    
    def __init__(self, message: str, business_rule: str = None):
        details = {}
        if business_rule:
            details["business_rule"] = business_rule
        
        super().__init__(message, "BUSINESS_LOGIC_ERROR", details)


class CalculationError(BusinessLogicError):
    """계산 에러"""
    
    def __init__(self, message: str, calculation_type: str = None, input_data: Dict[str, Any] = None):
        details = {}
        if calculation_type:
            details["calculation_type"] = calculation_type
        if input_data:
            details["input_data"] = input_data
        
        super().__init__(message, "CALCULATION_ERROR")
        self.details.update(details)


# 인증/권한 관련 에러
class AuthenticationError(ClientError):
    """인증 에러"""
    
    def __init__(self, message: str = "Authentication failed"):
        super().__init__(message, "AUTHENTICATION_ERROR")
        self.status_code = 401


class AuthorizationError(ClientError):
    """권한 에러"""
    
    def __init__(self, message: str = "Access denied", required_permission: str = None):
        details = {}
        if required_permission:
            details["required_permission"] = required_permission
        
        super().__init__(message, "AUTHORIZATION_ERROR", details)
        self.status_code = 403


# 예외 처리 헬퍼 함수들
def handle_database_exception(e: Exception, operation: str = None, table: str = None) -> DatabaseError:
    """데이터베이스 예외를 DatabaseError로 변환"""
    if isinstance(e, BaseServiceException):
        return e
    
    return DatabaseError(
        message=f"Database operation failed: {str(e)}",
        operation=operation,
        table=table
    )


def handle_external_api_exception(
    e: Exception, 
    api_name: str = None, 
    status_code: int = None,
    response_body: str = None
) -> ExternalAPIError:
    """외부 API 예외를 ExternalAPIError로 변환"""
    if isinstance(e, BaseServiceException):
        return e
    
    return ExternalAPIError(
        message=f"External API call failed: {str(e)}",
        api_name=api_name,
        status_code=status_code,
        response_body=response_body
    )


def handle_cache_exception(e: Exception, operation: str = None, key: str = None) -> CacheError:
    """캐시 예외를 CacheError로 변환"""
    if isinstance(e, BaseServiceException):
        return e
    
    return CacheError(
        message=f"Cache operation failed: {str(e)}",
        operation=operation,
        key=key
    )


# 예외 매핑 딕셔너리 (HTTP 상태 코드별)
EXCEPTION_STATUS_MAPPING = {
    ValidationError: 400,
    InvalidParameterError: 400,
    MissingParameterError: 400,
    InvalidCurrencyCodeError: 400,
    InvalidCountryCodeError: 400,
    InvalidPeriodError: 400,
    AuthenticationError: 401,
    AuthorizationError: 403,
    NotFoundError: 404,
    RateLimitExceededError: 429,
    DatabaseError: 500,
    CacheError: 500,
    ExternalAPIError: 500,
    MessagingError: 500,
    ConfigurationError: 500,
    DataProcessingError: 500,
    DataValidationError: 500,
    BusinessLogicError: 500,
    CalculationError: 500,
    ServiceUnavailableError: 503
}


def get_http_status_code(exception: BaseServiceException) -> int:
    """예외에 해당하는 HTTP 상태 코드 반환"""
    return getattr(exception, 'status_code', EXCEPTION_STATUS_MAPPING.get(type(exception), 500))