# Báo cáo phản tư — Day 28 Track 2

- Người thực hiện: Nguyễn Công Đạt
- Hình thức: Cá nhân
- Ngày cập nhật: 03/09/2026

## 1. Đánh đổi kỹ thuật (Trade-offs)

### IP01 và IP10 — Header Kafka

Hàm `event_headers` luôn gửi `idempotency-key` dưới dạng `bytes` và chỉ thêm
`traceparent` khi có trace hợp lệ. Cách này tránh phát tán một W3C header rỗng và
giữ đúng hợp đồng của Kafka. Đánh đổi là phía gọi phải chịu trách nhiệm kiểm tra
định dạng `traceparent`; hàm này chỉ làm nhiệm vụ truyền nguyên giá trị qua
boundary, không tự sửa một trace context sai.

### IP03 — Khử trùng trước Delta MERGE

Hàm `dedupe_latest` duyệt đầu vào đúng một lần, lưu sự kiện mới nhất theo
`idempotency_key`, rồi sắp xếp kết quả theo khóa. So sánh cặp
`(occurred_at, event_id)` giúp kết quả không phụ thuộc thứ tự Kafka giao bản tin,
kể cả khi hai sự kiện có cùng thời điểm. Giải pháp có độ phức tạp thời gian
`O(n + k log k)` và cần `O(k)` bộ nhớ với `k` khóa duy nhất. Đây là chi phí chấp
nhận được để tạo nguồn MERGE xác định và chống ghi trùng; với batch rất lớn cần
đưa bước khử trùng xuống Spark thay vì gom toàn bộ khóa trong một tiến trình.

### IP04 — Hợp đồng Feast tập trung

Hàm `feast_online_request` lấy danh sách đặc trưng trực tiếp từ `FEATURE_REFS`
trong `contracts.py`. Cách này giảm nguy cơ registry và API bị lệch nhau, nhưng
tạo phụ thuộc có chủ đích vào hợp đồng dùng chung: mọi thay đổi feature phải được
version hóa và kiểm thử tương thích trước khi triển khai.

### IP07 và IP08 — Readiness rõ mức độ

Hàm `readiness_status` trả `not_ready` ngay khi một dependency bắt buộc lỗi,
`degraded` khi chỉ dependency tùy chọn lỗi và `ready` khi không có lỗi. Fail-fast
giúp loại pod không an toàn khỏi gateway sớm; đánh đổi là hệ thống cần phân loại
`mandatory` chính xác, nếu cấu hình sai có thể từ chối lưu lượng không cần thiết
hoặc che khuất một dependency quan trọng.

## 2. Khoảng trống trước khi chạy production (Production gaps)

1. IP07 chưa được xác minh với GPU-backed vLLM thật (máy làm bài không có GPU).
   `lab28 inspect`/`lab28 ready` xác nhận `vllm.reachable=false`,
   `is_real_vllm=false`; các span `lab28.vllm.chat_completion` và các span phía
   serving của `/ask` (`lab28.api.ask`, `lab28.feast.get_online_features`,
   `lab28.mlflow.resolve_release`, `lab28.qdrant.query`) do đó không xuất hiện
   trong `evidence/ip10-trace.json`. Cần lưu `/version`, danh sách `/v1/models`
   và các metric bắt đầu bằng `vllm:` trên một máy có GPU thật; không dùng máy
   chủ giả OpenAI-compatible để thay thế.
2. Nhánh LangSmith của IP10 chưa được xác minh khi chưa có
   `LANGSMITH_API_KEY`. Local OTLP/Jaeger đã chứng minh được một trace ID
   (`04cff965f1a9453f9436fa7b53406225`) với các span bắt buộc phía ingestion
   (`lab28.gateway.request`, `lab28.api.ingest`, `lab28.kafka.produce`,
   `lab28.kafka.consume`, `lab28.airflow.dag`, `lab28.spark.delta_merge`);
   phần LangSmith-only vẫn UNVERIFIED.
3. Kafka trong lab dùng cấu hình một broker và replication factor bằng 1, phù
   hợp cho thực hành nhưng chưa chịu lỗi như production. Cần cụm nhiều broker,
   replication phù hợp, giám sát ISR và diễn tập mất broker.
4. Cần hoàn thiện quản lý bí mật, TLS/mTLS, xác thực và phân quyền cho các dịch
   vụ; bí mật phải đến từ secret manager, không nằm trong Git, ConfigMap hoặc
   evidence.
5. Load test (`evidence/load-test-8-workers.json`, `evidence/load-test-16-workers.json`)
   cho thấy gateway rate-limit chặn phần lớn traffic burst tới `/ready`
   (8 workers: 33×200 / 167×429, p95≈642ms, p99≈862ms; 16 workers: 27×200 /
   173×429, p95≈3.3s, p99≈3.4s). Đây là hành vi bảo vệ đúng thiết kế của
   gateway, không phải năng lực xử lý thật của pipeline `/ask` hay `/api/v1/*`
   — cần kiểm thử tải riêng cho các endpoint đó (khác `/ready`), với vLLM GPU
   thật, trước khi kết luận năng lực production. CPU/RAM lúc tải nằm trong
   `evidence/resource-usage-during-load.txt` (API lên ~92% CPU, MLflow ~51%,
   do mỗi lần readiness probe tải lại model artifact từ MLflow).
6. Manifest tĩnh đã hợp lệ (`scripts/validate_manifests.py` pass) nhưng vẫn
   cần bằng chứng triển khai thật trên một cluster Kubernetes/Argo CD: sync,
   phát hiện drift, self-heal, rollback desired revision/image và smoke test
   sau rollback. Máy làm bài không có cluster nên mục này còn UNVERIFIED.
7. Cần chính sách backup/restore, retention, capacity planning, cảnh báo theo
   SLO, kiểm thử nâng cấp schema không gián đoạn và quy trình xử lý sự cố có
   phân công owner rõ ràng.

## 3. Đóng góp của từng thành viên

| Thành viên | Vai trò | Đóng góp |
|---|---|---|
| Nguyễn Công Đạt | Cá nhân, phụ trách toàn bộ bài | Hoàn thiện truyền Kafka header cho trace/idempotency; khử trùng nguồn Delta MERGE; tạo request Feast từ hợp đồng chuẩn; cài đặt readiness ba mức; chạy và đối chiếu fast suite, Ruff, integration matrix, portability và manifest validation; tổng hợp kiến trúc, đánh đổi và khoảng trống production. |

### Kết quả đã xác minh tại máy làm bài

| Hạng mục | Kết quả |
|---|---|
| Fast suite (`starter-tests` và `tests`) | `87 passed` |
| Ruff | `All checks passed` |
| Integration matrix (`scripts/verify_matrix.py`) | `245 checks passed` |
| Portability (`scripts/check_portability.py`) | Đạt |
| Kubernetes/GitOps manifest contracts (`scripts/validate_manifests.py`) | Đạt |
| IT-J1 golden path (`test_j1_golden_path.py`) | `12 passed, 3 skipped` (gpu gate) |
| IT-J2 idempotent replay (`test_j2_idempotent_replay.py`) | `9 passed` |
| Full live integration suite (`not gpu and not langsmith`) | `56 passed, 16 skipped` |
| Evidence JSON (IP01–IP10 + integration-report.json) | Đủ 12 file trong `evidence/`, khớp `integration-matrix.yaml` |
| MLflow release | `lab28-rag-release v4`, run `a4fd10c5472640a7bb97e6c1e53d3628`, alias `champion` |
| Failure injection (Qdrant down/up) | Không mất dữ liệu: 24 điểm trước và sau (`evidence/failure-injection-qdrant.json`) |
| Load test 8 / 16 workers | `evidence/load-test-8-workers.json`, `evidence/load-test-16-workers.json` — gateway rate-limit hoạt động đúng (xem gap #5) |
| GPU vLLM (IP07) và LangSmith (IP10) | `UNVERIFIED` — máy làm bài không có GPU/`LANGSMITH_API_KEY` |
| Argo CD sync / drift / rollback thật | `UNVERIFIED` — không có cluster Kubernetes để triển khai |

