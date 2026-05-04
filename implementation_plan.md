# Kế hoạch Triển khai Mô hình ML (Daily Demand Forecasting - Direct Forecasting)

Kế hoạch này vạch ra các bước hoàn chỉnh để xây dựng và tích hợp mô hình XGBoost dự báo nhu cầu Taxi hàng ngày vào hệ thống Data Pipeline. 
**(Cập nhật: Sử dụng Phương pháp Direct Forecasting, không dùng Lag theo ngày để tránh Error Propagation khi batch latency lên tới 1 tháng).**

## Mục tiêu
1. Biến đổi dữ liệu hiện có ở tầng **Silver Layer** thành dữ liệu đặc trưng (Features) tĩnh dài hạn theo dạng bảng.
2. Cấu trúc lại chu trình ML để Train trên tháng T-1, dự đoán tháng T, và Re-train khi tháng T lộ diện dữ liệu thực tế.
3. Lên lịch trình dự kiến hoàn thành trong 5 ngày.

---

## Lịch trình Triển khai (Timeline Estimate: 5 Days)

Mục tiêu hoàn thành trong 5 ngày hoàn toàn **KHẢ THI**. Dưới đây là phân bổ chi tiết:

### Khoảng thời gian: Mức nỗ lực dự kiến (1 Ngày = 1 Phase)

- **Ngày 1: Data Preparation & Feature Engineering Pipeline**
    - Cài đặt thư viện (`xgboost`, `scikit-learn`, `s3fs`).
    - Viết script PySpark `etls/ml_feature_engineering.py` trích xuất `lag_28_days`, `day_of_week`...
    - Sửa file `taxi_dag.py` để Airflow tự động chạy tạo Features khi có tháng mới.
- **Ngày 2: Baseline Model & Offline Training**
    - Viết Notebook / Python script để load dữ liệu bằng `s3fs`.
    - Phân chia Train/Validation theo time-series (Vd: Train = T1/2026 trở về trước, Val = T2/2026).
    - Tạo Baseline XGBoost model và đánh giá MAE/RMSE sơ bộ.
- **Ngày 3: Production Training Script**
    - Đóng gói code huấn luyện thành `scripts/train_xgboost.py`.
    - Viết luồng tự động lưu model (`.json`) và metrics lên MinIO (có thể tích hợp MLflow tuỳ nhu cầu).
- **Ngày 4: Daily Inference Pipeline (Luồng dự đoán)**
    - Viết `scripts/predict_daily_demand.py` để load model trên MinIO và nhả số dự báo.
    - Tạo `dags/ml_inference_dag.py` chạy lịch `@monthly` (hoặc `@daily` tuỳ cách bạn muốn push kết quả) để dự đoán sẵn cho 30 ngày tiếp theo.
- **Ngày 5: Continuous Training (Drift Check) & End-to-End Test**
    - Viết function đối chiếu (So sánh số dự đoán Tháng N với số thực tế Tháng N khi pipeline gốc cập nhật).
    - Lên logic (Trigger) để tự động kích hoạt lại `train_xgboost.py` (Re-train đưa tháng N vào làm Train set để chuẩn bị dự đoán tháng N+1).
    - Test kiểm thử toàn bộ hệ thống từ Bronze tới Model.

---

## Các thay đổi đề xuất (Implementation Details)

### 1. Thành phần: Feature Engineering (Luồng Batch Hàng Tháng)
#### [NEW] `etls/ml_feature_engineering.py`
Tạo một script PySpark làm nhiệm vụ sau:
- Đọc dữ liệu từ Silver Layer.
- **Group by:** `DATE` và `PULocationID` -> Tính tổng số chuyến đi (`target_demand`).
- **Tạo Time-based features:** `day_of_week`, `is_weekend`, `month`.
- **Tạo Long-Term Lag features (Window function):** Kéo demand của đúng **28 ngày trước** (`lag_28d`) và **35 ngày trước** (`lag_35d`). Điều này đảm bảo khi dự đoán ngày 15/03, biến `lag_28d` sẽ rơi vào giữa tháng 02 (mà ta đã có dữ liệu thực).
- Ghi Parquet ra `minio://gold/ml_features/daily_demand/`.

#### [MODIFY] `dags/taxi_dag.py`
Sửa lại đồ thị DAG thành mô hình chữ V:
```python
bronze_ingestion_task >> silver_transform_task
silver_transform_task >> [gold_transform_task, transform_silver_to_ml_features]
```

### 2. Thành phần: Huấn luyện Mô hình & Re-Train (Continuous Training)
#### [NEW] `scripts/train_xgboost.py`
- Lọc `Train Set`: Dữ liệu `<= Tháng T-1`.
- Lọc `Validation Set`: Dữ liệu `Tháng T` (Ví dụ Tháng 2/2026).
- Chạy huấn luyện và lưu file model `xgboost_demand_model.json` lên `minio://ml-models/`.
- *(Tuỳ chọn)* Chèn logic: Sau khi so khớp dữ liệu dự đoán và thực tế bị lệch (Drift), cờ báo `Retrain` sẽ nạp thêm tháng 2 vào làm Train set.

### 3. Thành phần: Khai thác Dự đoán (Batch Inference)
#### [NEW] `scripts/predict_monthly_batch.py`
- Khi lấy được Model mới nhất, dựa trên các feature đầu vào (`lag_28_days`...) model sẽ tiến hành nhả list kết quả dự đoán cùng lúc cho trọn vẹn 30 ngày của Tháng T+1 (Ví dụ: Dự đoán trọn tháng 3).
- Lưu kết quả vào MinIO / Iceberg phục vụ Dashboard.

---

## Open Questions
> [!IMPORTANT]
> **Vui lòng xác nhận để tôi bắt đầu viết code (Giai đoạn Ngày 1 & 2):**
> 1. Hiện tại cấu hình kết nối MinIO gốc của hệ thống nằm ở đâu (ví dụ: `AWS_ACCESS_KEY` trong file `airflow.env` hay file config riêng)? Để tôi trỏ đường dẫn lưu file Feature PySpark cho chuẩn xác.
> 2. Bucket nào trên hệ thống dể lưu Gold Layer (tên bucket là gì)?

## Verification Plan
... (Sẽ chạy cụ thể từng Unit test khi bắt tay hoàn thiện từng ngày)
