# BÀI TẬP 1: KHỞI TẠO NỀN TẢNG NON-BLOCKING VÀ KẾT NỐI KAFKA CLUSTER

Dự án này cung cấp mã nguồn hoàn chỉnh giải quyết yêu cầu kỹ thuật và các lỗi tiềm ẩn (BUG-01, BUG-02) bằng cả hai ngôn ngữ **Java (Spring Boot / WebFlux)** và **Python (FastAPI / Asynchronous)** để phục vụ tốt nhất cho kiểm thử.

## 1. Cách giải quyết các BUG & Yêu cầu kỹ thuật

- **BUG-01 (Tomcat vs Netty)**:
  - *Trong Java*: Loại bỏ hoàn toàn `spring-boot-starter-web` khỏi tệp `pom.xml` và sử dụng `spring-boot-starter-webflux`. Nhờ đó, ứng dụng tự động chạy trên nền tảng non-blocking **Netty** (mặc định của WebFlux) thay vì Tomcat.
  - *Trong Python*: Sử dụng **FastAPI** và chạy bằng máy chủ ASGI **Uvicorn** là nền tảng Non-Blocking/Asynchronous hiệu năng cao cực kỳ ổn định, tránh các server blocking kiểu WSGI.

- **BUG-02 (Kafka Serializer)**:
  - *Trong Java*: Cấu hình `spring.kafka.producer.value-serializer` thành `org.springframework.kafka.support.serializer.JsonSerializer` trong `application.yml` giúp tự động chuyển đổi các Java Object phức tạp sang JSON một cách mượt mà.
  - *Trong Python*: Sử dụng bộ tuần tự hóa `json_serializer` truyền vào thuộc tính `value_serializer` của `AIOKafkaProducer` để serialize payload dictionary sang JSON string dạng byte.

- **HTTP Client (Timeout sau 5 giây)**:
  - *Trong Java*: Định nghĩa một Bean `WebClient` dùng chung với cấu hình `ReadTimeoutHandler`, `WriteTimeoutHandler` và `responseTimeout` được cài đặt chính xác là `5 giây` thông qua Reactor Netty.
  - *Trong Python*: Sử dụng thư viện non-blocking `httpx.AsyncClient` cài đặt `timeout=5.0` được chia sẻ thông qua vòng đời `lifespan` của FastAPI.

---

## 2. Hướng dẫn chạy chương trình

### Chạy phiên bản Python (FastAPI)
1. Cài đặt các thư viện cần thiết:
   ```bash
   pip install -r requirements.txt
   ```

2. Đảm bảo bạn đã khởi chạy Kafka Broker cục bộ tại địa chỉ `localhost:9092`.

3. Khởi chạy ứng dụng chạy trên port `8080`:
   ```bash
   python app.py
   ```

4. Kiểm tra sức khỏe hệ thống (Health Check) tại:
   `http://localhost:8080/health`

### Chạy phiên bản Java (Spring Boot)
1. Đảm bảo bạn có JDK 17+ cài đặt sẵn.
2. Chạy ứng dụng bằng lệnh Maven:
   ```bash
   mvn spring-boot:run
   ```
3. Ứng dụng sẽ khởi chạy trên Netty Server tại cổng `8080`.