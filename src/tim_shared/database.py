"""
데이터베이스 연결 및 관리 모듈
Aurora MySQL, Redis, MongoDB 지원
"""
import asyncio
import json
from typing import Dict, List, Any, Optional, Union
from decimal import Decimal
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
import aiomysql
import motor.motor_asyncio 
from pymongo import MongoClient

try:
    import redis.asyncio as aioredis
    REDIS_AVAILABLE = True
except ImportError:
    try:
        import aioredis
        REDIS_AVAILABLE = True
    except ImportError:
        REDIS_AVAILABLE = False
import boto3
from botocore.exceptions import ClientError

from .config import get_config, Environment

def get_logger_safe():
    """안전한 로거 가져오기"""
    try:
        from .logging import get_logger
        return get_logger(__name__)
    except:
        import logging
        return logging.getLogger(__name__)

# 전역 로거 초기화
logger = get_logger_safe()


class DatabaseManager:
    """데이터베이스 연결 관리자"""
    
    # TODO: AWS 연결을 위한 수정 필요
    # - IAM 역할에 RDS, ElastiCache, DocumentDB 접근 권한 추가
    # - Parameter Store에서 DB 비밀번호 로드 활성화 (주석 해제)
    # - VPC 보안 그룹에서 포트 허용 (3306: MySQL, 6379: Redis, 27017: MongoDB)
    
    def __init__(self):
        self.config = get_config()
        self._mysql_pool = None
        self._redis_client = None
        self._mongodb_client = None
        self._mongodb_database = None
        self._selections_collection = None
        self._rankings_collection = None
    
    async def initialize(self):
        """데이터베이스 연결 초기화"""
        logger = get_logger_safe()
        logger.info("Initializing database connections")
        
        # MySQL 연결 풀 초기화 
        try:
            await self._init_mysql()
        except Exception as e:
            get_logger_safe().warning("MySQL initialization failed, continuing without MySQL")
        
        # Redis 연결 초기화
        try:
            await self._init_redis()
        except Exception as e:
            get_logger_safe().warning("Redis initialization failed, continuing without Redis")

        # MongoDB 초기화 
        try:
            await self._init_mongodb()
        except Exception as e:
            get_logger_safe().warning("MongoDB initialization failed, continuing without MongoDB")
        
        get_logger_safe().info("Database connections initialized (some may be unavailable)")
    
    async def _init_mysql(self):
        """MySQL 연결 풀 초기화"""
        # TODO: 실시간 서비스 변경 - Aurora MySQL 연결로 실제 DB 사용
        # - 로컬 mock 대신 실제 Aurora 클러스터 엔드포인트 사용
        # - 데이터 수집 시 실제 환율 이력 저장 (data-ingestor에서 호출)
        # AWS 배포 시 수정 필요사항:
        # 1. Aurora Serverless v2 클러스터 생성
        # 2. VPC 보안 그룹에서 MySQL 포트(3306) 허용
        # 3. Parameter Store에 DB 비밀번호 저장
        # 4. IAM 역할에 ssm:GetParameter 권한 추가
        # 5. 아래 주석 해제하여 Parameter Store에서 비밀번호 로드
        try:
            db_config = self.config.database
            
            password = db_config.aurora_password
            if self.config.environment != Environment.LOCAL and not password:
                password = await self._get_parameter_store_value(
                    f"/{self.config.service_name}/db/password"
                )
            
            self._mysql_pool = await aiomysql.create_pool(
                host=db_config.mysql_host,
                port=db_config.mysql_port,
                user=db_config.mysql_username,
                password=db_config.mysql_password,
                db=db_config.mysql_database,
                charset='utf8mb4',
                autocommit=True,
                minsize=1,
                maxsize=10,  # TODO: AWS Lambda에서는 1로 설정
                echo=self.config.environment == Environment.LOCAL #echo 설정값 : 실행되는 모든 SQL 쿼리를 콘솔에 출력하는 옵셥
                # 로컬 환경에서는 쿼리 출컬, 운영 및 배포에서는 출력 안함
            )
            
            logger.info(
                "MySQL connection pool created",
                host=db_config.aurora_host,
                database=db_config.aurora_database
            )
            
        except Exception as e:
            logger.error("Failed to initialize MySQL connection", error=e)
            raise
    
    async def _init_redis(self):
        """Redis 연결 초기화"""
        # TODO: 실시간 서비스 변경 - ElastiCache Redis로 캐시 사용
        # - 로컬 mock 대신 실제 ElastiCache 클러스터 엔드포인트 사용
        # - 환율 데이터 캐싱으로 응답 속도 향상 (TTL 10분)
        # AWS 배포 시 수정 필요사항:
        # 1. ElastiCache Redis 클러스터 생성 (클러스터 모드 활성화)
        # 2. VPC 보안 그룹에서 Redis 포트(6379) 허용
        # 3. 전송 중 암호화 및 저장 시 암호화 활성화
        # 4. Parameter Store에 Redis 인증 토큰 저장 (필요시)
        try:
            db_config = self.config.database
            
            # Redis 연결 URL 구성
            if db_config.redis_ssl:
                # AWS ElastiCache (SSL 사용)
                redis_url = f"rediss://:{db_config.redis_password}@{db_config.redis_host}:{db_config.redis_port}"
            else:
                # 로컬 Redis
                if db_config.redis_password:
                    redis_url = f"redis://:{db_config.redis_password}@{db_config.redis_host}:{db_config.redis_port}"
                else:
                    redis_url = f"redis://{db_config.redis_host}:{db_config.redis_port}"
            
            if REDIS_AVAILABLE:
                # aioredis 사용
                if db_config.redis_ssl:
                    # AWS ElastiCache (SSL 사용)
                    redis_url = f"rediss://:{db_config.redis_password}@{db_config.redis_host}:{db_config.redis_port}"
                else:
                    # 로컬 Redis
                    if db_config.redis_password:
                        redis_url = f"redis://:{db_config.redis_password}@{db_config.redis_host}:{db_config.redis_port}"
                    else:
                        redis_url = f"redis://{db_config.redis_host}:{db_config.redis_port}"
                
                self._redis_client = aioredis.from_url(redis_url, decode_responses=True)
            else:
                logger.warning("Redis client not available, using mock client")
                self._redis_client = None
            
            # 연결 테스트
            if self._redis_client:
                try:
                    await self._redis_client.ping()
                except Exception as e:
                    logger.warning(f"Redis ping failed: {e}")
                    # 연결 실패해도 계속 진행 (로컬 개발 시)
            
            logger.info(
                "Redis connection established",
                host=db_config.redis_host,
                port=db_config.redis_port,
                ssl=db_config.redis_ssl
            )
            
        except Exception as e:
            logger.error("Failed to initialize Redis connection", error=e)
            raise

    def _init_mongodb(self):
        """MongoDB 초기화"""
        try:
            db_config = self.config.database
            
            # MongoDB 연결 문자열 구성
            if db_config.mongodb_username and db_config.mongodb_password:
                connection_string = f"mongodb://{db_config.mongodb_username}:{db_config.mongodb_password}@{db_config.mongodb_host}:{db_config.mongodb_port}/{db_config.mongodb_database}?authSource={db_config.mongodb_auth_source}"
            else:
                connection_string = f"mongodb://{db_config.mongodb_host}:{db_config.mongodb_port}/{db_config.mongodb_database}"
            
            # 비동기 MongoDB 클라이언트 생성
            self._mongodb_client = motor.motor_asyncio.AsyncIOMotorClient(connection_string)
            self._mongodb_database = self._mongodb_client[db_config.mongodb_database]
            
            # 컬렉션 참조
            self._selections_collection = self._mongodb_database[db_config.selections_collection]
            self._rankings_collection = self._mongodb_database[db_config.rankings_collection]
            
            logger.info(
                "MongoDB connection established",
                host=db_config.mongodb_host,
                port=db_config.mongodb_port,
                database=db_config.mongodb_database
            )
            
        except Exception as e:
            logger.error("Failed to initialize MongoDB connection", error=e)
            raise
    
    
    async def _get_parameter_store_value(self, parameter_name: str) -> str:
        """AWS Parameter Store에서 값 조회"""
        # TODO: AWS 배포 시 구현
        try:
            ssm = boto3.client('ssm')
            response = ssm.get_parameter(Name=parameter_name, WithDecryption=True)
            return response['Parameter']['Value']
        except Exception as e:
            logger.error(f"Failed to get parameter {parameter_name}", error=e)
            raise
    
    @asynccontextmanager
    async def get_mysql_connection(self):
        """MySQL 연결 컨텍스트 매니저"""
        if not self._mysql_pool:
            raise RuntimeError("MySQL pool not initialized")
        
        async with self._mysql_pool.acquire() as conn:
            try:
                yield conn
            except Exception as e:
                await conn.rollback()
                raise
    
    def get_redis_client(self):
        """Redis 클라이언트 반환"""
        if not self._redis_client:
            raise RuntimeError("Redis client not initialized")
        return self._redis_client
    
    def get_mongodb_collection(self, collection_name: str):
        """MongoDB 컬렉션 반환"""
        if not self._mongodb_database:
            raise RuntimeError("MongoDB database not initialized")
        return self._mongodb_database[collection_name]
    
    def get_mongodb_client(self):
        """MongoDB 클라이언트 반환"""
        if not self._mongodb_client:
            raise RuntimeError("MongoDB client not initialized")
        return self._mongodb_client
    
    async def close(self):
        """모든 연결 종료"""
        logger.info("Closing database connections")
        
        if self._mysql_pool:
            self._mysql_pool.close()
            await self._mysql_pool.wait_closed()
        
        if self._redis_client:
            await self._redis_client.close()
        
        if self._mongodb_client:
            self._mongodb_client.close()
        
        logger.info("Database connections closed")


# 전역 데이터베이스 매니저
db_manager: Optional[DatabaseManager] = None


async def init_database():
    """데이터베이스 초기화"""
    global db_manager
    db_manager = DatabaseManager()
    await db_manager.initialize()


def get_db_manager() -> DatabaseManager:
    """데이터베이스 매니저 반환"""
    if db_manager is None:
        raise RuntimeError("Database not initialized. Call init_database() first.")
    return db_manager


# 편의 함수들
def get_mysql_connection():
    """MySQL 연결 반환"""
    return get_db_manager().get_mysql_connection()


def get_redis_client():
    """Redis 클라이언트 반환"""
    return get_db_manager().get_redis_client()


def get_mongodb_collection(collection_name: str):
    """MongoDB 컬렉션 반환"""
    return get_db_manager().get_mongodb_collection(collection_name)


# 데이터베이스 헬퍼 클래스들
class RedisHelper:
    """Redis 작업 헬퍼"""
    
    def __init__(self):
        try:
            self.client = get_redis_client()
        except RuntimeError:
            # Redis가 초기화되지 않은 경우 None으로 설정
            self.client = None
    
    async def set_json(self, key: str, value: Dict[str, Any], ttl: int = None):
        """JSON 데이터를 Redis에 저장"""
        if not self.client:
            logger.warning("Redis client not available, skipping set_json")
            return
        
        json_str = json.dumps(value, ensure_ascii=False, default=str)
        try:
            await self.client.set(key, json_str, ex=ttl)
        except Exception as e:
            logger.warning(f"Redis set_json failed: {e}")
    
    async def get_json(self, key: str) -> Optional[Dict[str, Any]]:
        """Redis에서 JSON 데이터 조회"""
        if not self.client:
            return None
        
        try:
            json_str = await self.client.get(key)
            if json_str:
                return json.loads(json_str)
        except Exception as e:
            logger.warning(f"Redis get_json failed: {e}")
        return None
    
    async def set_hash(self, key: str, mapping: Dict[str, Any], ttl: int = None):
        """해시 데이터를 Redis에 저장"""
        if not self.client:
            logger.warning("Redis client not available, skipping set_hash")
            return
        
        try:
            # 모든 값을 문자열로 변환
            str_mapping = {k: str(v) for k, v in mapping.items()}
            await self.client.hset(key, mapping=str_mapping)
            
            if ttl:
                await self.client.expire(key, ttl)
        except Exception as e:
            logger.warning(f"Redis set_hash failed: {e}")
    
    async def get_hash(self, key: str) -> Dict[str, str]:
        """Redis에서 해시 데이터 조회"""
        if not self.client:
            return {}
        
        try:
            return await self.client.hgetall(key)
        except Exception as e:
            logger.warning(f"Redis get_hash failed: {e}")
            return {}
    
    async def delete(self, *keys: str) -> int:
        """키 삭제"""
        if not self.client:
            return 0
        try:
            return await self.client.delete(*keys)
        except Exception as e:
            logger.warning(f"Redis delete failed: {e}")
            return 0

    async def delete_pattern(self, pattern: str) -> int:
        """패턴에 매칭되는 키들을 삭제 (SCAN 기반)"""
        if not self.client:
            return 0
        try:
            deleted = 0
            # scan_iter는 비차단 스캔으로, 대규모 키에서도 안전
            async for key in self.client.scan_iter(match=pattern):
                try:
                    deleted += await self.client.delete(key)
                except Exception as inner:
                    logger.warning(f"Redis delete for key {key} failed: {inner}")
            return deleted
        except Exception as e:
            logger.warning(f"Redis delete_pattern failed: {e}")
            return 0
    
    async def exists(self, key: str) -> bool:
        """키 존재 여부 확인"""
        if not self.client:
            return False
        try:
            return bool(await self.client.exists(key))
        except Exception as e:
            logger.warning(f"Redis exists failed: {e}")
            return False

    async def set(self, key: str, value: str, ttl: int = None):
        """문자열 데이터를 Redis에 저장"""
        if not self.client:
            logger.warning("Redis client not available, skipping set")
            return

        try:
            await self.client.set(key, value, ex=ttl)
        except Exception as e:
            logger.warning(f"Redis set failed: {e}")

    async def get(self, key: str) -> Optional[str]:
        """Redis에서 문자열 데이터 조회"""
        if not self.client:
            return None

        try:
            return await self.client.get(key)
        except Exception as e:
            logger.warning(f"Redis get failed: {e}")
            return None


class MySQLHelper:
    """MySQL 작업 헬퍼"""
    
    async def execute_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """쿼리 실행 및 결과 반환"""
        async with get_mysql_connection() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, params)
                return await cursor.fetchall()
    
    async def execute_insert(self, query: str, params: tuple = None) -> int:
        """INSERT 쿼리 실행 및 생성된 ID 반환"""
        async with get_mysql_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, params)
                return cursor.lastrowid
    
    async def execute_update(self, query: str, params: tuple = None) -> int:
        """UPDATE/DELETE 쿼리 실행 및 영향받은 행 수 반환"""
        async with get_mysql_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, params)
                return cursor.rowcount


class MongoDBHelper:
    """MongoDB 작업 헬퍼"""
    
    def __init__(self, collection_name: str):
        self.collection = get_mongodb_collection(collection_name)
        self.collection_name = collection_name
    
    async def insert_one(self, document: Dict[str, Any]) -> str:
        """문서 하나 삽입"""
        try:
            result = await self.collection.insert_one(document)
            return str(result.inserted_id)
        except Exception as e:
            logger.error(f"Failed to insert document to {self.collection_name}", error=e)
            raise
    
    async def insert_many(self, documents: List[Dict[str, Any]]) -> List[str]:
        """문서 여러 개 삽입"""
        try:
            result = await self.collection.insert_many(documents)
            return [str(id) for id in result.inserted_ids]
        except Exception as e:
            logger.error(f"Failed to insert documents to {self.collection_name}", error=e)
            raise
    
    async def find_one(self, filter_dict: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """문서 하나 조회"""
        try:
            return await self.collection.find_one(filter_dict)
        except Exception as e:
            logger.error(f"Failed to find document in {self.collection_name}", error=e)
            raise
    
    async def find_many(self, filter_dict: Dict[str, Any] = None, limit: int = None) -> List[Dict[str, Any]]:
        """문서 여러 개 조회"""
        try:
            cursor = self.collection.find(filter_dict or {})
            if limit:
                cursor = cursor.limit(limit)
            return await cursor.to_list(length=limit)
        except Exception as e:
            logger.error(f"Failed to find documents in {self.collection_name}", error=e)
            raise
    
    async def update_one(self, filter_dict: Dict[str, Any], update_dict: Dict[str, Any]) -> int:
        """문서 하나 업데이트"""
        try:
            result = await self.collection.update_one(filter_dict, {"$set": update_dict})
            return result.modified_count
        except Exception as e:
            logger.error(f"Failed to update document in {self.collection_name}", error=e)
            raise
    
    async def update_many(self, filter_dict: Dict[str, Any], update_dict: Dict[str, Any]) -> int:
        """문서 여러 개 업데이트"""
        try:
            result = await self.collection.update_many(filter_dict, {"$set": update_dict})
            return result.modified_count
        except Exception as e:
            logger.error(f"Failed to update documents in {self.collection_name}", error=e)
            raise
    
    async def delete_one(self, filter_dict: Dict[str, Any]) -> int:
        """문서 하나 삭제"""
        try:
            result = await self.collection.delete_one(filter_dict)
            return result.deleted_count
        except Exception as e:
            logger.error(f"Failed to delete document from {self.collection_name}", error=e)
            raise
    
    async def delete_many(self, filter_dict: Dict[str, Any]) -> int:
        """문서 여러 개 삭제"""
        try:
            result = await self.collection.delete_many(filter_dict)
            return result.deleted_count
        except Exception as e:
            logger.error(f"Failed to delete documents from {self.collection_name}", error=e)
            raise