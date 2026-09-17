import json
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
import httpx
from aiokafka import AIOKafkaProducer

# Cau hinh Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("OrderService-Python")

class AppState:
    def __init__(self):
        self.web_client: httpx.AsyncClient = None
        self.kafka_producer: AIOKafkaProducer = None

state = AppState()

# BUG-02: Ham tuan tu hoa JSON cho Kafka Value
def json_serializer(value):
    return json.dumps(value).encode('utf-8')

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Tuong duong WebClient Config voi Timeout 5 giay
    state.web_client = httpx.AsyncClient(timeout=5.0)
    logger.info("HTTP WebClient (Async) da duoc khoi tao voi timeout 5 giay.")

    # Khoi tao ket noi Kafka (localhost:9092) voi JsonSerializer
    state.kafka_producer = AIOKafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=json_serializer
    )
    try:
        await state.kafka_producer.start()
        logger.info("Ket noi thanh cong den Kafka Broker tai localhost:9092")
    except Exception as e:
        logger.warning(f"Chua ket noi duoc toi Kafka: {e}. Chuong trinh van khoi dong.")

    yield

    # Giai phong tai nguyen khi dong app
    await state.web_client.aclose()
    await state.kafka_producer.stop()
    logger.info("Da dong ket noi WebClient va Kafka Producer.")

# BUG-01: Khoi chay bang may chu non-blocking (FastAPI/Uvicorn tuong duong Netty) thay vi Tomcat blocking
app = FastAPI(
    title="StoreX Non-Blocking Order Service",
    lifespan=lifespan
)

@app.get("/health")
async def health():
    return {"status": "UP", "platform": "FastAPI / Netty-equivalent"}

@app.post("/api/v1/orders")
async def create_order(order: dict):
    # Gui message toi Kafka
    event = {
        "event_type": "ORDER_CREATED",
        "payload": order
    }
    try:
        await state.kafka_producer.send_and_wait("order-topic", value=event)
        logger.info(f"Da gui message den Kafka: {event}")
    except Exception as e:
        logger.error(f"Gặp loi khi gui event toi Kafka: {e}")

    # Goi API ben ngoai bang WebClient chia se chung voi timeout 5 giay
    try:
        # Su dung httpbin de test timeout thuc te
        response = await state.web_client.get("https://httpbin.org/delay/1")
        external_data = response.json()
    except httpx.TimeoutException:
        logger.error("Yeu cau bi huy do qua 5 giay khong nhan duoc phan hoi!")
        raise HTTPException(status_code=504, detail="External partner service timed out after 5 seconds")
    except Exception as e:
        logger.error(f"Loi goi API ben ngoai: {e}")
        external_data = {"status": "mock_fallback", "error": str(e)}

    return {
        "message": "Order processed non-blocking successfully!",
        "order": order,
        "partner_response": external_data
    }