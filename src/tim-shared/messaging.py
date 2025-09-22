"""
메시징 시스템 관리 모듈
Kafka와 SQS를 지원하는 통합 메시징 인터페이스
"""
import json
import asyncio
import platform
from typing import Dict, Any, Optional, Callable, List
from datetime import datetime
import uuid

# Windows 운영체제일 경우, asyncio 정책을 변경하여 SelectorEventLoop를 사용하도록 설정
if platform.system() == "Windows":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from .config import get_config, Environment
from .exceptions import MessagingError

def get_logger_safe():
    """안전한 로거 가져오기"""
    try:
        from .logging import get_logger
        return get_logger(__name__)
    except:
        import logging
        return logging.getLogger(__name__)

# 전역 로거 초기화 (지연 로딩)
logger = get_logger_safe()

try:
    from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
    from aiokafka.errors import KafkaError
    KAFKA_AVAILABLE = True
    logger.info("aiokafka successfully imported")
except ImportError as e:
    KAFKA_AVAILABLE = False
    logger.warning("aiokafka not available, Kafka functionality disabled", error=str(e))

try:
    import boto3
    from botocore.exceptions import ClientError
    SQS_AVAILABLE = True
except ImportError:
    SQS_AVAILABLE = False
    logger.warning("boto3 not available, SQS functionality disabled")


class MessageProducer:
    """메시지 프로듀서 (Kafka 우선, SQS 폴백)"""
    
    # TODO: AWS 연결을 위한 수정 필요
    # - LocalStack 대신 실제 MSK (Kafka) 클러스터 엔드포인트 사용 (bootstrap_servers)
    # - IAM 또는 SASL/SCRAM 인증 설정 (security_protocol='SASL_SSL', sasl_mechanism='AWS_MSK_IAM')
    # - SQS 큐 URL 실제 설정 (sqs_queue_url)
    # - VPC 보안 그룹에서 MSK 포트 허용 (9092 PLAINTEXT, 9094 TLS, 9096 SASL_SSL)
    # - IAM 역할에 kafka-cluster:Connect, kafka-cluster:WriteData, sqs:SendMessage 권한 추가
    # 실시간 서비스 변경: data-ingestor에서 실제 메시지 스트리밍으로 환율 데이터 실시간 처리
    
    def __init__(self):
        self.config = None  # 초기화 시점에서 로드
        self.kafka_producer = None
        self.sqs_client = None
        self._initialized = False
        self.use_kafka = False # Kafka 사용 여부를 인스턴스 변수로 관리
    
    async def initialize(self):
        """프로듀서 초기화"""
        if self._initialized:
            return
        
        # 필요한 모듈을 함수 내부에서 import하여 순환 참조 및 초기화 순서 문제 방지
        from .config import get_config, Environment
        from .exceptions import MessagingError

        try:
            self.config = get_config()
            
            # --- [핵심 수정] Kafka 사용 가능 여부를 이 시점에서 최종 결정하고 로그 기록 ---
            self.use_kafka = KAFKA_AVAILABLE and self.config.environment != Environment.LOCAL
            
            if self.use_kafka:
                logger.info(f"Kafka is enabled for '{self.config.environment.value}' environment.")
                if self.config.messaging.kafka_bootstrap_servers:
                    await self._init_kafka_producer()
            else:
                logger.info("Kafka is disabled for this environment. SQS will be used as a fallback if available.")
            
            if SQS_AVAILABLE: # SQS는 LocalStack을 통해 로컬에서도 사용 가능할 수 있음
                self._init_sqs_client()
            
            self._initialized = True
            logger.info("Message producer initialized successfully")
            
        except Exception as e:
            logger.error("Failed to initialize message producer", error=e, exc_info=True)
            raise MessagingError("Producer initialization failed", system="kafka_sqs")
    
    
    async def _init_kafka_producer(self):
        """Kafka 프로듀서 초기화"""
        # TODO: 실시간 서비스 변경 - MSK Kafka로 메시지 스트리밍
        # ... (기존 주석 및 TODO 내용은 동일)
        try:
            self.kafka_producer = AIOKafkaProducer(
                bootstrap_servers=self.config.messaging.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                security_protocol=self.config.messaging.kafka_security_protocol,
                retry_backoff_ms=1000,
                request_timeout_ms=30000
                # AWS MSK 배포 시 추가 설정:
                # ssl_context=ssl.create_default_context(),
                # sasl_mechanism='AWS_MSK_IAM',
                # sasl_oauth_token_provider=MSKTokenProvider()
            )
            
            await self.kafka_producer.start()
            logger.info("Kafka producer initialized",
                        servers=self.config.messaging.kafka_bootstrap_servers)
            
        except Exception as e:
            logger.warning("Failed to initialize Kafka producer. It will be disabled.", error=str(e))
            self.kafka_producer = None # 초기화 실패 시 None으로 설정
    
    def _init_sqs_client(self):
        """SQS 클라이언트 초기화 (LocalStack 지원)"""
        # 필요한 모듈을 이 시점에서 import
        from .config import Environment
        
        try:
            client_kwargs = {
                'region_name': self.config.messaging.sqs_region
            }
            
            # --- [핵심 수정] ---
            # 로컬 환경일 경우 LocalStack 엔드포인트를 사용하도록 설정
            if self.config.environment == Environment.LOCAL:
                # docker-compose.yml에 정의된 SQS_ENDPOINT 환경변수 값을 사용
                # config.py에서 이 값을 self.config.sqs_endpoint 등으로 로드해야 합니다.
                # 이전 docker-compose.yml을 보면 DYNAMODB_ENDPOINT와 SQS_ENDPOINT가 동일합니다.
                endpoint_url = getattr(self.config, 'sqs_endpoint', None) or getattr(self.config, 'dynamodb_endpoint', None)
                if endpoint_url:
                    client_kwargs['endpoint_url'] = endpoint_url
            # --- [수정 끝] ---
            
            self.sqs_client = boto3.client('sqs', **client_kwargs)
            logger.info("SQS client initialized", extra=client_kwargs)

        except Exception as e:
            logger.warning("Failed to initialize SQS client. It will be disabled.", error=str(e))
            self.sqs_client = None
    
    async def send_message(
        self, 
        topic: str, 
        message: Dict[str, Any], 
        key: str = None,
        partition: int = None
    ) -> bool:
        """
        메시지 전송 (Kafka 우선, SQS 폴백)
        
        Args:
            topic: 토픽/큐 이름
            message: 메시지 내용
            key: 파티션 키 (Kafka용)
            partition: 파티션 번호 (Kafka용)
            
        Returns:
            전송 성공 여부
        """
        if not self._initialized:
            await self.initialize()
        
        # 메시지에 메타데이터 추가
        enriched_message = {
            **message,
            'timestamp': datetime.utcnow().isoformat(),
            'message_id': str(uuid.uuid4()),
            'producer_service': self.config.service_name
        }
        
        # Kafka 전송 시도
        if self.kafka_producer:
            try:
                await self.kafka_producer.send_and_wait(
                    topic=topic,
                    value=enriched_message,
                    key=key,
                    partition=partition
                )
                
                logger.debug(
                    "Message sent to Kafka",
                    topic=topic,
                    key=key,
                    message_id=enriched_message['message_id']
                )
                return True
                
            except Exception as e:
                logger.warning("Kafka send failed, trying SQS fallback", error=e, exc_info=True)
        
        # SQS 폴백
        queue_url = self.config.messaging.sqs_queue_urls.get(topic)
        if self.sqs_client and queue_url:
            try:
                response = self.sqs_client.send_message(
                    QueueUrl=queue_url,
                    MessageBody=json.dumps(enriched_message, default=str),
                    MessageAttributes={
                        'topic': {
                            'StringValue': topic,
                            'DataType': 'String'
                        },
                        'producer_service': {
                            'StringValue': self.config.service_name,
                            'DataType': 'String'
                        }
                    }
                )
                
                logger.debug(
                    "Message sent to SQS",
                    queue_url=queue_url,
                    message_id=response['MessageId']
                )
                return True
                
            except Exception as e:
                logger.error("SQS send also failed", error=e, exc_info=True)
        
        # 모든 전송 방법 실패
        logger.error("All messaging systems failed to send the message", topic=topic)
        # from .exceptions import MessagingError # 필요 시점에 임포트
        # raise MessagingError(
        #     f"Failed to send message to topic {topic}",
        #     system="kafka_sqs",
        #     topic=topic
        # )
        return False # 실패 시 False 반환으로 변경하여 유연성 확보
    
    async def close(self):
        """프로듀서 종료"""
        if self.kafka_producer:
            await self.kafka_producer.stop()
            logger.info("Kafka producer stopped")
        
        self._initialized = False


class MessageConsumer:
    """메시지 컨슈머"""
    
    def __init__(self, topics: List[str], group_id: str):
        self.config = None  # 초기화 시점에서 로드
        self.topics = topics
        self.group_id = group_id
        self.kafka_consumer = None
        self.sqs_client = None
        self._running = False
        self._initialized = False # 초기화 상태 플래그 추가

    async def initialize(self):
        """컨슈머 초기화"""
        if self._initialized:
            return

        from .config import get_config, Environment
        from .exceptions import MessagingError

        try:
            # 설정 로드
            self.config = get_config()
            
            # Kafka 컨슈머 초기화
            if KAFKA_AVAILABLE and self.config.messaging.kafka_bootstrap_servers:
                await self._init_kafka_consumer()
            
            # SQS 클라이언트 초기화 (AWS 환경에서만)
            if SQS_AVAILABLE and self.config.environment != Environment.LOCAL:
                self._init_sqs_client()

            self._initialized = True
            logger.info("Message consumer initialized successfully")
            
        except Exception as e:
            logger.error("Failed to initialize message consumer", error=e, exc_info=True)
            raise MessagingError("Consumer initialization failed", system="kafka_sqs")
    
    async def _init_kafka_consumer(self):
        """Kafka 컨슈머 초기화"""
        try:
            self.kafka_consumer = AIOKafkaConsumer(
                *self.topics,
                bootstrap_servers=self.config.messaging.kafka_bootstrap_servers,
                group_id=self.group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                security_protocol=self.config.messaging.kafka_security_protocol,
                auto_offset_reset='latest',
                enable_auto_commit=True
            )
            
            await self.kafka_consumer.start()
            logger.info("Kafka consumer initialized", 
                        topics=self.topics, group_id=self.group_id)
            
        except Exception as e:
            logger.warning("Failed to initialize Kafka consumer", error=str(e))
            self.kafka_consumer = None
    
    def _init_sqs_client(self):
        """SQS 클라이언트 초기화 (LocalStack 지원)"""
        # 필요한 모듈을 이 시점에서 import
        from .config import Environment
        
        try:
            client_kwargs = {
                'region_name': self.config.messaging.sqs_region
            }
            
            # --- [핵심 수정] ---
            # 로컬 환경일 경우 LocalStack 엔드포인트를 사용하도록 설정
            if self.config.environment == Environment.LOCAL:
                # docker-compose.yml에 정의된 SQS_ENDPOINT 환경변수 값을 사용
                # config.py에서 이 값을 self.config.sqs_endpoint 등으로 로드해야 합니다.
                # 이전 docker-compose.yml을 보면 DYNAMODB_ENDPOINT와 SQS_ENDPOINT가 동일합니다.
                endpoint_url = getattr(self.config, 'sqs_endpoint', None) or getattr(self.config, 'dynamodb_endpoint', None)
                if endpoint_url:
                    client_kwargs['endpoint_url'] = endpoint_url
            # --- [수정 끝] ---
            
            self.sqs_client = boto3.client('sqs', **client_kwargs)
            logger.info("SQS client initialized", extra=client_kwargs)

        except Exception as e:
            logger.warning("Failed to initialize SQS client. It will be disabled.", error=str(e))
            self.sqs_client = None
    
    async def start_consuming(self, message_handler: Callable[[Dict[str, Any]], None]):
        """
        메시지 소비 시작
        
        Args:
            message_handler: 메시지 처리 함수
        """
        if not self._initialized:
            await self.initialize()
        
        self._running = True
        logger.info("Starting message consumption", topics=self.topics)
        
        from .exceptions import MessagingError

        # Kafka 소비 우선
        if self.kafka_consumer:
            await self._consume_kafka(message_handler)
        elif self.sqs_client:
            await self._consume_sqs(message_handler)
        else:
            logger.error("No messaging system available for consumption.")
            raise MessagingError("No messaging system available for consumption")
    
    async def _consume_kafka(self, message_handler: Callable):
        """Kafka 메시지 소비"""
        try:
            async for message in self.kafka_consumer:
                if not self._running:
                    break
                
                try:
                    # message_handler가 비동기 함수일 수 있으므로 await 처리
                    await asyncio.coroutine(message_handler)(message.value)
                    logger.debug(
                        "Message processed from Kafka",
                        topic=message.topic,
                        partition=message.partition,
                        offset=message.offset
                    )
                    
                except Exception as e:
                    logger.error(
                        "Message processing failed",
                        error=e,
                        topic=message.topic,
                        message_value=message.value,
                        exc_info=True
                    )
                    
        except Exception as e:
            logger.error("Kafka consumption error", error=e, exc_info=True)
            from .exceptions import MessagingError
            raise MessagingError("Kafka consumption failed", system="kafka")
    
    async def _consume_sqs(self, message_handler: Callable):
        """SQS 메시지 소비"""
        from .exceptions import MessagingError
        if not self.config.messaging.sqs_queue_url:
            raise MessagingError("SQS queue URL not configured")
        
        try:
            while self._running:
                response = self.sqs_client.receive_message(
                    QueueUrl=self.config.messaging.sqs_queue_url,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=20,  # Long polling
                    MessageAttributeNames=['All']
                )
                
                messages = response.get('Messages', [])
                
                if not messages:
                    await asyncio.sleep(1) # 메시지가 없을 경우 짧은 대기
                    continue

                for message in messages:
                    if not self._running:
                        break
                    try:
                        message_body = json.loads(message['Body'])
                        # message_handler가 비동기 함수일 수 있으므로 await 처리
                        await asyncio.coroutine(message_handler)(message_body)
                        
                        # 메시지 삭제
                        self.sqs_client.delete_message(
                            QueueUrl=self.config.messaging.sqs_queue_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        
                        logger.debug("Message processed from SQS", 
                                     message_id=message['MessageId'])
                        
                    except Exception as e:
                        logger.error("SQS message processing failed", error=e, exc_info=True)
                
        except Exception as e:
            logger.error("SQS consumption error", error=e, exc_info=True)
            raise MessagingError("SQS consumption failed", system="sqs")
    
    async def stop(self):
        """컨슈머 중지"""
        self._running = False
        
        if self.kafka_consumer:
            await self.kafka_consumer.stop()
            logger.info("Kafka consumer stopped")


# 전역 프로듀서 인스턴스 (싱글턴 패턴)
_message_producer: Optional[MessageProducer] = None
_producer_lock = asyncio.Lock() # 동시성 문제 방지를 위한 Lock

async def get_message_producer() -> MessageProducer:
    """메시지 프로듀서 인스턴스 반환 (비동기 싱글턴)"""
    global _message_producer
    
    if _message_producer is None:
        async with _producer_lock:
            # Lock을 획득한 후 다시 한번 확인 (더블 체크 락킹)
            if _message_producer is None:
                producer = MessageProducer()
                await producer.initialize()
                _message_producer = producer
    
    return _message_producer


async def send_message(topic: str, message: Dict[str, Any], key: str = None) -> bool:
    """편의 함수: 메시지 전송"""
    producer = await get_message_producer()
    return await producer.send_message(topic, message, key)


# 메시지 타입별 편의 함수들
async def send_exchange_rate_update(rate_data: Dict[str, Any]) -> bool:
    """환율 업데이트 메시지 전송"""
    return await send_message(
        topic="exchange-rates",
        message={
            "type": "exchange_rate_update",
            "data": rate_data
        },
        key=rate_data.get("currency_code")
    )


async def send_user_selection_event(selection_data: Dict[str, Any]) -> bool:
    """사용자 선택 이벤트 메시지 전송"""
    return await send_message(
        topic="user-events",
        message={
            "type": "user_selection",
            "data": selection_data
        },
        key=selection_data.get("user_id")
    )


async def send_ranking_calculation_trigger(period: str) -> bool:
    """랭킹 계산 트리거 메시지 전송"""
    return await send_message(
        topic="ranking-events",
        message={
            "type": "calculate_ranking",
            "period": period,
            "triggered_at": datetime.utcnow().isoformat()
        }
    )