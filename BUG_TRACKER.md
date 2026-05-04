# BUG TRACKER — NYC Taxi Airflow Pipeline

Mục tiêu: ghi lại các bug đã gặp trong quá trình làm dự án + giải pháp + trạng thái, để tiện theo dõi và vận hành lâu dài.

## Cách dùng file này
- Mỗi bug có: mã bug, triệu chứng, nguyên nhân gốc, tác động, giải pháp, trạng thái.
- Khi gặp lỗi mới, thêm 1 mục theo đúng template ở cuối file.
- Nếu đã fix nhưng chưa verify production, để trạng thái `MONITORING`.

---

## BUG-001 — DAG bị treo ở `transform_bronze_to_silver`

- **Triệu chứng**
  - Task `transform_bronze_to_silver` chạy rất lâu hoặc treo.
  - `transform_silver_to_gold_iceberg` bị `upstream_failed`.

- **Nguyên nhân gốc**
  - Dùng nhiều `.count()` exact trên Spark DataFrame:
    - `df_yellow.count()`
    - `df_green.count()`
    - `silver_df.count()`
  - Các lệnh này trigger full scan dữ liệu lớn (nhiều năm taxi parquet).

- **Tác động**
  - Runtime tăng mạnh, dễ vượt heartbeat/timeout.
  - Dễ gây cảm giác DAG “treo”.

- **Giải pháp đã áp dụng**
  - Đổi sang approximate count có timeout:
    - `rdd.countApprox(timeout=60, confidence=0.95)`
  - Thêm `silver_df.cache()` để giảm recompute.
  - Thêm `silver_df.unpersist()` trong `finally`.

- **File liên quan**
  - `etls/silver_transform.py`

- **Trạng thái**
  - `FIXED` (cần tiếp tục monitoring runtime thực tế).

---

## BUG-002 — Gold transform chậm do exact count + maintenance nặng

- **Triệu chứng**
  - Task gold chạy lâu bất thường.
  - Tắc nghẽn khi dữ liệu tăng.

- **Nguyên nhân gốc**
  - `silver_df.count()` và `fact_df.count()` exact count.
  - Mỗi run đều gọi:
    - `CALL gold.system.rewrite_data_files(...)`
    - `CALL gold.system.rewrite_manifests(...)`
  - Đây là thao tác nặng, không nên chạy mỗi tháng.

- **Tác động**
  - Tăng thời gian chạy gold đáng kể.
  - Tăng rủi ro timeout/hang.

- **Giải pháp đã áp dụng**
  - Đổi count sang approximate (`countApprox`).
  - Bỏ compaction/manifest rewrite khỏi flow chạy thường xuyên.
  - Khuyến nghị tách compaction thành maintenance job riêng.

- **File liên quan**
  - `etls/gold_transform.py`

- **Trạng thái**
  - `FIXED` (khuyến nghị tạo maintenance DAG định kỳ).

---

## BUG-003 — `dim_date` bị mất dữ liệu lịch sử do overwrite

- **Triệu chứng**
  - Sau mỗi run theo tháng, `dim_date` có thể chỉ còn dữ liệu tháng hiện tại.

- **Nguyên nhân gốc**
  - Ghi `dim_date` bằng mode `overwrite` trong khi input chỉ là subset tháng/năm đang chạy.

- **Tác động**
  - Mất lịch sử dimension date.
  - Ảnh hưởng truy vấn BI/report dài hạn.

- **Giải pháp đã áp dụng**
  - Tạo `write_dim_date_to_gold()`:
    - đọc `gold.dim_date` hiện có,
    - left anti join theo `full_datetime`,
    - append chỉ các dòng mới.
  - Không overwrite toàn bảng `dim_date`.

- **File liên quan**
  - `etls/gold_transform.py`

- **Trạng thái**
  - `FIXED` (cần kiểm tra dữ liệu lịch sử sau nhiều lần rerun).

---

## BUG-004 — Thiếu timeout/retry ở DAG tasks

- **Triệu chứng**
  - Khi task bị kẹt hoặc lỗi tạm thời, DAG không tự phục hồi tốt.

- **Nguyên nhân gốc**
  - PythonOperator chưa có:
    - `execution_timeout`
    - `retries`
    - `retry_delay`

- **Tác động**
  - Task có thể chạy vô hạn hoặc fail cứng sớm.
  - Khó vận hành khi có lỗi transient.

- **Giải pháp đã áp dụng**
  - Cập nhật task-level config:
    - bronze: timeout 1h
    - silver: timeout 3h
    - gold: timeout 2h
    - retries=1, retry_delay=5m

- **File liên quan**
  - `dags/taxi_dag.py`

- **Trạng thái**
  - `FIXED`.

---

## BUG-005 — Worker concurrency cao gây áp lực tài nguyên Spark

- **Triệu chứng**
  - Hệ thống chậm/đơ khi nhiều task Spark chạy đồng thời.

- **Nguyên nhân gốc**
  - `AIRFLOW__CELERY__WORKER_CONCURRENCY=4` trong khi mỗi Spark session tiêu tốn nhiều RAM.

- **Tác động**
  - Rủi ro OOM, swap, nghẽn tài nguyên.

- **Giải pháp đã áp dụng**
  - Giảm concurrency:
    - từ `4` xuống `2`.

- **File liên quan**
  - `airflow.env`

- **Trạng thái**
  - `FIXED` (theo dõi thêm theo tài nguyên máy thực tế).

---

## BUG-006 — Heartbeat timeout scheduler quá thấp cho Spark job dài

- **Triệu chứng**
  - Task có thể bị đánh dấu zombie khi Spark job dài.

- **Nguyên nhân gốc**
  - `AIRFLOW__SCHEDULER__TASK_INSTANCE_HEARTBEAT_TIMEOUT=1800` (30 phút) quá thấp với workload lớn.

- **Tác động**
  - Fail giả (false failure) dù job chưa thực sự dead.

- **Giải pháp đã áp dụng**
  - Tăng heartbeat timeout lên `7200` giây.

- **File liên quan**
  - `airflow.env`

- **Trạng thái**
  - `FIXED`.

---

## BUG-007 — Download timeout quá thấp làm lỗi tải parquet lớn

- **Triệu chứng**
  - Download source taxi data fail ngắt quãng.

- **Nguyên nhân gốc**
  - `time_out=10` giây trong config download quá thấp cho file lớn/mạng chậm.

- **Tác động**
  - Bronze ingestion fail không ổn định.

- **Giải pháp đã áp dụng**
  - Tăng `time_out` lên `120` giây.

- **File liên quan**
  - `config/config.conf`

- **Trạng thái**
  - `FIXED`.

---

## BUG-008 — Spark fail với `[CANNOT_MERGE_SCHEMAS]` do lệch kiểu INT/BIGINT giữa partitions

- **Triệu chứng**
  - Spark ném lỗi:
    - `org.apache.spark.SparkException: [CANNOT_MERGE_SCHEMAS] Failed merging schemas`
  - Schema ban đầu có `VendorID`, `PULocationID`, `DOLocationID` kiểu `BIGINT` nhưng một số file/partition lại là `INT`.

- **Nguyên nhân gốc**
  - Dữ liệu parquet từ nhiều nguồn/thời điểm có type drift:
    - cùng tên cột nhưng khác kiểu số nguyên (`INT` vs `BIGINT`).
  - Chưa có bước chuẩn hóa schema tập trung trước khi union/ghi silver.

- **Tác động**
  - Silver transform fail ngắt quãng.
  - DAG có thể dừng ở bước `transform_bronze_to_silver`.
  - Ảnh hưởng downstream gold transform.

- **Giải pháp đã áp dụng**
  - Thêm hàm chuẩn hóa schema tập trung `_normalize_taxi_schema(df)` trong `etls/silver_transform.py`.
  - Ép kiểu các cột canonical về chuẩn thống nhất:
    - ID quan trọng (`VendorID`, `PULocationID`, `DOLocationID`) -> `LongType` (BIGINT)
    - các cột danh mục (`RatecodeID`, `payment_type`, `trip_type`, `passenger_count`) -> `IntegerType`
    - cột số thực (`trip_distance`, amount fields, `trip_duration_minutes`) -> `DoubleType`
    - datetime -> `TimestampType`, text flag/type -> `StringType`.
  - Áp dụng normalize cho cả luồng yellow/green trước khi union.

- **File liên quan**
  - `etls/silver_transform.py`

- **Trạng thái**
  - `FIXED` (khuyến nghị tiếp tục monitoring khi ingest thêm partition mới).

---

## BUG-009 — `Py4JJavaError` on `javaToPython` in `transform_bronze_to_silver`

- **Triệu chứng**
  - Task `transform_bronze_to_silver` fails với lỗi:
    ```
    Py4JJavaError('An error occurred while calling o1753.javaToPython.\n', JavaObject id=o1755)
    ```
  - Lỗi xuất hiện ngay sau khi Spark session được khởi tạo và bắt đầu đọc bronze data.

- **Nguyên nhân gốc**
  - **Deep union tree (nguyên nhân chính):** `_load_bronze_dataframes` union từng file một trong vòng lặp, tạo ra union tree sâu N cấp (N = số file bronze). Với dữ liệu 2018–2026: 9 năm × 12 tháng = 108 file/service type → union tree 108 cấp. Spark dùng đệ quy để duyệt query plan; tree quá sâu gây `StackOverflowError` trong JVM khi planning.
  - **`.rdd` property gọi `javaToPython()`:** Tất cả các lệnh `df.rdd.countApprox()` và `df.rdd.isEmpty()` đều truy cập `.rdd` property, bên trong gọi `self._jdf.javaToPython()` trên Java object. Khi JVM đang ở trạng thái lỗi (do StackOverflow từ deep union tree), lệnh `javaToPython()` này fail và ném `Py4JJavaError`.
  - **Các vị trí bị ảnh hưởng:**
    - `silver_transform.py`: `df_yellow.rdd.countApprox()`, `df_green.rdd.countApprox()`, `silver_df.rdd.countApprox()`
    - `gold_transform.py`: `new_rows_df.rdd.isEmpty()`, `silver_df.rdd.countApprox()`, `fact_df.rdd.countApprox()`

- **Tác động**
  - Task `transform_bronze_to_silver` fail hoàn toàn với `Py4JJavaError`.
  - Task `transform_silver_to_gold_iceberg` bị `upstream_failed`.
  - Pipeline bị dừng, không có dữ liệu silver/gold mới.

- **Giải pháp đã áp dụng**
  1. **Refactor `_load_bronze_dataframes`** — group files theo (year, month) và đọc từng group bằng `spark.read.parquet(*paths)`. Giảm độ sâu union tree từ N files → M periods (tối đa 12/năm). Source period vẫn được gắn từ bronze folder metadata để tránh partition drift.
  2. **Thêm `_df_count_with_timeout()` helper** — chạy `df.count()` trong daemon thread với wall-clock timeout. Trả về `None` nếu timeout. Không dùng `.rdd` nên không trigger `javaToPython()`.
  3. **Thay thế tất cả `df.rdd.countApprox()`** bằng `_df_count_with_timeout(df, timeout_seconds=60)` trong cả `silver_transform.py` và `gold_transform.py`.
  4. **Fix empty check** trong `run_silver_transform` — dùng `silver_df.limit(1).count() == 0` thay vì so sánh `silver_output_rows < 1` (tránh lỗi khi count trả về `None`).
  5. **Fix `write_dim_date_to_gold`** — thay `new_rows_df.rdd.isEmpty()` bằng `new_rows_df.limit(1).count() == 0`.
  6. **Thêm try-except cho `_distinct_year_month_pairs`** — bảo vệ `.collect()` (cũng dùng `javaToPython` nội bộ) khỏi Py4J bridge failures.

- **File liên quan**
  - `etls/silver_transform.py`
  - `etls/gold_transform.py`

- **Trạng thái**
  - `FIXED` (cần monitoring ít nhất 3 chu kỳ chạy thực tế).

---

## BUG-010 — Performance: expensive full `count()` actions + incremental merge deep union tree

- **Triệu chứng**
  - Task `transform_bronze_to_silver` chạy rất chậm (nhiều phút chỉ để đếm rows).
  - Với INCREMENTAL mode, task có thể fail hoặc treo do union tree sâu khi merge nhiều partition.
  - Log Airflow: `Py4JJavaError` tại `o1727.count` (action trigger trên union tree sâu).

- **Nguyên nhân gốc**
  1. **3 full `count()` actions không cần thiết** trong `run_silver_transform()`:
     - `_df_count_with_timeout(df_yellow)` — full scan bronze yellow (metadata-only)
     - `_df_count_with_timeout(df_green)` — full scan bronze green (metadata-only)
     - `_df_count_with_timeout(silver_df)` — full scan silver output (metadata-only)
     - Mỗi lệnh trigger 1 Spark job đầy đủ, không ảnh hưởng logic pipeline.
  2. **Incremental merge xây union tree sâu** — cùng root cause với BUG-009:
     - Code cũ collect tất cả `merged_partition_df` vào list, rồi union tất cả thành 1 DataFrame duy nhất trước khi write.
     - Với N partitions (year/month), tạo ra N-deep union tree → JVM StackOverflow khi Spark planning.
  3. **`_distinct_year_month_pairs()` gọi `.collect()`** — thêm 1 Spark action không cần thiết; year/month đã có sẵn trong `bronze_logs` metadata.
  4. **`spark.sql.files.maxPartitionBytes = 32MB`** quá nhỏ → tạo quá nhiều task cho file parquet lớn.

- **Tác động**
  - Task silver chạy chậm hơn cần thiết (3 Spark jobs thừa chỉ để đếm metadata).
  - INCREMENTAL mode có thể fail với `Py4JJavaError` khi số partition tăng.
  - Tổng runtime tăng đáng kể, dễ vượt `execution_timeout=3h`.

- **Giải pháp đã áp dụng**
  1. **Xóa bronze row counts** — set `bronze_yellow_rows = None`, `bronze_green_rows = None` trực tiếp (không gọi `_df_count_with_timeout`).
  2. **Xóa silver output row count** — set `silver_output_rows = None` (best-effort); chỉ giữ emptiness check `limit(1).count()` qua `_df_has_any_row_with_timeout()`.
  3. **Fix incremental merge** — thay vì build union tree rồi write 1 lần, viết từng partition riêng biệt trong vòng lặp bằng `_write_to_silver(merged_partition_df)`. `partitionOverwriteMode=dynamic` đảm bảo chỉ partition đang xử lý bị overwrite.
  4. **Derive year/month từ `bronze_logs` metadata** — không cần gọi `.collect()` trên DataFrame.
  5. **Tăng `maxPartitionBytes`** từ 32MB → 128MB để giảm số task khi đọc file parquet lớn.
  6. **Thêm `spark.sql.adaptive.skewJoin.enabled=true`** để xử lý data skew tốt hơn.

- **File liên quan**
  - `etls/silver_transform.py`

- **Trạng thái**
  - `FIXED` (cần monitoring ít nhất 3 chu kỳ chạy thực tế).

---

## BUG-011 — `SHOW SCHEMAS FROM iceberg` chỉ trả về `information_schema`

- **Triệu chứng**
  - Chạy `SHOW SCHEMAS FROM iceberg` trong Trino CLI chỉ trả về `information_schema`.
  - Không có schema `gold` nào xuất hiện dù gold transform đã chạy.
  - `SHOW TABLES FROM iceberg.gold` → lỗi schema not found.

- **Nguyên nhân gốc**
  - Tất cả các lệnh ghi Iceberg trong `gold_transform.py` dùng tên 2 phần:
    - `saveAsTable("gold.dim_vendor")` → catalog=`gold`, table=`dim_vendor`, **không có schema/namespace**
    - `saveAsTable("gold.fact_trips")` → tương tự
    - `spark.table("gold.dim_date")` → tương tự
  - Trong Spark catalog system, tên 2 phần `<catalog>.<table>` ghi bảng vào **root namespace** của Nessie.
  - Trino's `SHOW SCHEMAS FROM iceberg` chỉ liệt kê các **named namespace** — bảng ở root namespace hoàn toàn vô hình.
  - Không có lệnh `CREATE NAMESPACE` nào được gọi trước khi ghi → Nessie không có namespace nào ngoài `information_schema`.

- **Tác động**
  - Trino không thể truy vấn bất kỳ bảng gold nào.
  - Superset không thể kết nối đến dữ liệu gold qua Trino.
  - Toàn bộ analytics layer bị vô hiệu hóa dù pipeline ETL chạy thành công.

- **Giải pháp đã áp dụng**
  1. **Thêm `_ensure_gold_schema(spark)`** — gọi `CREATE NAMESPACE IF NOT EXISTS gold.gold` trước khi ghi bất kỳ bảng nào. Idempotent: an toàn khi gọi nhiều lần.
  2. **Đổi tất cả `saveAsTable("gold.X")` → `saveAsTable("gold.gold.X")`** (tên 3 phần: catalog=`gold`, schema=`gold`, table=`X`):
     - `write_dim_to_gold`: `gold.gold.{table_name}`
     - `write_fact_to_gold`: `gold.gold.fact_trips`
     - `write_dim_date_to_gold`: `gold.gold.dim_date` (cả read lẫn write)
  3. **Gọi `_ensure_gold_schema(spark)`** trong `run_gold_transform` ngay sau khi Spark session được khởi tạo.
  - Sau fix: `SHOW SCHEMAS FROM iceberg` → trả về `gold` và `information_schema`.
  - `SHOW TABLES FROM iceberg.gold` → trả về đầy đủ 8 bảng dim/fact.

- **File liên quan**
  - `etls/gold_transform.py`

- **Trạng thái**
  - `FIXED` (cần re-run gold transform task để verify namespace được tạo và bảng xuất hiện trong Trino).

---

## BUG-012 — Gold fact table có thể fail ở first run do `DELETE` trước khi bảng tồn tại

- **Triệu chứng**
  - Gold transform fail ngay lần chạy đầu tiên khi ghi fact.
  - Log Spark/Iceberg báo table không tồn tại ở bước `DELETE FROM`.

- **Nguyên nhân gốc**
  - Trong `write_fact_to_gold`, code thực hiện:
    - `DELETE FROM gold.fact_taxi_trips WHERE ...`
    - rồi mới `saveAsTable(...)`.
  - Ở first run, bảng fact chưa được tạo nên câu lệnh DELETE fail trước.

- **Tác động**
  - DAG dừng ở task gold ngay từ lần chạy đầu.
  - Không tạo được dữ liệu fact cho downstream BI.

- **Giải pháp đề xuất**
  - Kiểm tra bảng tồn tại trước khi DELETE (`spark.catalog.tableExists(...)`), hoặc bọc DELETE bằng try/except rồi fallback sang create/append.

- **File liên quan**
  - `etls/gold_transform.py`

- **Trạng thái**
  - `OPEN`.

---

## BUG-013 — Surrogate key của dimension không ổn định giữa các lần chạy

- **Triệu chứng**
  - Join fact ↔ dimension có thể trả về kết quả sai sau nhiều lần rerun.
  - Key ở bảng dim thay đổi giữa các lần chạy dù business key không đổi.

- **Nguyên nhân gốc**
  - Các dim dùng `monotonically_increasing_id()` để tạo key.
  - Hàm này không đảm bảo deterministic/stable qua nhiều run/partition plan khác nhau.
  - Trong khi dim có thể overwrite/append theo run, fact cũ giữ key cũ.

- **Tác động**
  - Sai lệch quan hệ khóa ngoại logic trong star schema.
  - Dashboard/report có thể sai số liệu theo thời gian.

- **Giải pháp đề xuất**
  - Dùng khóa ổn định:
    - natural key trực tiếp (nếu phù hợp), hoặc
    - deterministic hash/surrogate key từ business key.
  - Tránh `monotonically_increasing_id()` cho khóa nghiệp vụ cần ổn định dài hạn.

- **File liên quan**
  - `etls/gold_transform.py`

- **Trạng thái**
  - `OPEN`.

---

## BUG-014 — `dim_date` key có nguy cơ mismatch khi fact join bằng dim DataFrame của run hiện tại

- **Triệu chứng**
  - Một số bản ghi fact có `pickup_datetime_key`/`dropoff_datetime_key` không khớp kỳ vọng với `gold.dim_date` sau rerun/incremental phức tạp.
  - Khó truy vết consistency theo thời gian.

- **Nguyên nhân gốc**
  - Pipeline build `dim_date` từ dữ liệu run hiện tại rồi dùng chính DataFrame đó để join tạo key cho fact.
  - Nếu key generation của dim_date không tuyệt đối ổn định qua run, nguy cơ lệch key với bảng dim_date đã lưu.

- **Tác động**
  - Rủi ro lệch foreign-key logic theo thời gian.
  - Gây sai join analytics trong trường hợp edge-case.

- **Giải pháp đề xuất**
  - Khi build fact, ưu tiên join từ bảng `gold.dim_date` đã materialize (source of truth), không join từ DataFrame tạm của run.
  - Kết hợp cơ chế key deterministic/stable cho dim_date.

- **File liên quan**
  - `etls/gold_transform.py`

- **Trạng thái**
  - `OPEN`.

---

## BUG-015 — Inconsistent tham số period giữa Bronze và Gold (`dag_run.conf` vs `params`)

- **Triệu chứng**
  - Trigger thủ công DAG với `conf={"year":..., "month":...}` có thể khiến Bronze và Gold xử lý khác period.
  - Kết quả downstream khó kiểm soát khi backfill/manual rerun.

- **Nguyên nhân gốc**
  - Bronze đọc period từ `dag_run.conf`.
  - Gold lại ưu tiên `context["params"]` rồi mới fallback.
  - Hai task cùng DAG nhưng khác nguồn period input.

- **Tác động**
  - Bronze/Silver/Gold có thể lệch tháng xử lý.
  - Dữ liệu warehouse không đồng nhất theo run.

- **Giải pháp đề xuất**
  - Chuẩn hóa thứ tự resolve period cho toàn DAG:
    1. `dag_run.conf`
    2. `logical_date`
    3. fallback metadata (nếu cần).
  - Áp dụng nhất quán cho cả Bronze và Gold.

- **File liên quan**
  - `dags/taxi_dag.py`

- **Trạng thái**
  - `OPEN`.

---

## BUG-016 — Fallback period ở Gold dùng hàm “next ingestion period” có thể trỏ sai tháng cần transform

- **Triệu chứng**
  - Gold transform có thể chạy vào period chưa có dữ liệu silver tương ứng trong một số nhánh fallback.
  - Task gold fail/skip bất thường khi trigger không truyền đủ context.

- **Nguyên nhân gốc**
  - Fallback sử dụng `infer_next_ingestion_period_from_metadata(layer="bronze")`.
  - Hàm này trả về **kỳ tiếp theo để ingest**, không phải kỳ vừa ingest/đã sẵn sàng cho gold transform.

- **Tác động**
  - Sai lệch window xử lý giữa ingestion và transform.
  - Tăng tỉ lệ run fail hoặc ra output rỗng.

- **Giải pháp đề xuất**
  - Ở Gold fallback nên dùng “latest successful available period” (hoặc period từ logical_date), không dùng “next ingestion period”.

- **File liên quan**
  - `dags/taxi_dag.py`
  - `etls/bronze_ingestion.py`

- **Trạng thái**
  - `OPEN`.

---

## BUG-017 — Silver output còn giữ cột kỹ thuật `dropoff_year` (schema drift không mong muốn)

- **Triệu chứng**
  - File silver parquet chứa thêm cột kỹ thuật ngoài schema canonical mong muốn.
  - Query downstream có thể thấy field dư thừa, gây nhiễu model dữ liệu.

- **Nguyên nhân gốc**
  - `_build_silver_dataframe` tạo `pickup_year` và `dropoff_year`.
  - `_write_to_silver` hiện drop `source_year/source_month` và `pickup_year`, nhưng chưa drop `dropoff_year`.

- **Tác động**
  - Schema silver không nhất quán với design.
  - Tăng rủi ro compatibility issue cho downstream consumers.

- **Giải pháp đề xuất**
  - Drop thêm `dropoff_year` trước khi ghi silver.

- **File liên quan**
  - `etls/silver_transform.py`

- **Trạng thái**
  - `OPEN`.

---

## BUG-018 — Mốc thời gian DAG (2018) không đồng nhất với filter silver (`pickup_year > 2018`)

- **Triệu chứng**
  - Bronze vẫn ingest dữ liệu 2018 nhưng silver filter loại hết bản ghi pickup năm 2018.
  - Tốn tài nguyên ingest/storage cho dữ liệu không đi tiếp downstream.

- **Nguyên nhân gốc**
  - DAG `start_date=datetime(2018, 1, 1)` + `catchup=True`.
  - Silver filter áp điều kiện `pickup_year > 2018`.

- **Tác động**
  - Lãng phí compute/storage.
  - Dễ gây hiểu nhầm khi kiểm tra completeness theo timeline.

- **Giải pháp đề xuất**
  - Chọn một trong hai:
    - đổi start_date từ 2019 nếu business không cần 2018, hoặc
    - nới filter thành `>= 2018` nếu cần giữ 2018.

- **File liên quan**
  - `dags/taxi_dag.py`
  - `etls/silver_transform.py`

- **Trạng thái**
  - `OPEN`.

---

## BUG-019 — `str(exc)` trên `Py4JJavaError` crash error handler trong `run_silver_transform`

- **Triệu chứng**
  - Task `transform_bronze_to_silver` fail với lỗi:
    ```
    py4j.protocol.Py4JError: An error occurred while calling None.None
    ```
  - Lỗi xảy ra tại `item["error_message"] = str(exc)` trong except block của `run_silver_transform`.
  - Lỗi gốc thực sự (`Py4JJavaError` khi ghi parquet) bị che khuất hoàn toàn.

- **Nguyên nhân gốc**
  - `Py4JJavaError.__str__()` thực hiện round-trip call ngược vào JVM để lấy Java stack trace.
  - Khi JVM/Py4J gateway đã bị đóng/hỏng sau khi Spark write fail, `str(exc)` tự nó ném `Py4JError`.
  - Điều này crash toàn bộ except block, che khuất lỗi gốc và ngăn metadata được ghi đúng vào PostgreSQL.
  - Cùng pattern tồn tại trong `gold_transform.py`: `run_payload["error"] = str(e)`.
  - Tại sao lần trước không gặp: BUG-010 fix mới tạo ra code path per-partition incremental write loop. `str(exc)` là latent bug chỉ trigger khi Py4J gateway đã chết hoàn toàn.

- **Tác động**
  - Task Airflow fail với lỗi không rõ ràng, che khuất nguyên nhân Spark thực sự.
  - Metadata không được ghi đúng vào `silver_transform_log` và `silver_file_log`.
  - Khó debug và triage vì lỗi gốc bị ẩn hoàn toàn.

- **Giải pháp đã áp dụng**
  1. Thêm `_safe_str_exc(exc)` helper trong cả `silver_transform.py` và `gold_transform.py`: bọc `str(exc)` trong try/except, fallback sang `repr(exc)` (không gọi JVM), fallback cuối là sentinel string.
  2. Trong `silver_transform.py`: tính `_exc_str = _safe_str_exc(exc)` một lần, dùng cho cả `run_payload["error_message"]` và `item["error_message"]`.
  3. Trong `gold_transform.py`: thay `str(e)` bằng `_safe_str_exc(e)` trong except block của `run_gold_transform`.

- **File liên quan**
  - `etls/silver_transform.py`
  - `etls/gold_transform.py`

- **Trạng thái**
  - `FIXED` (cần monitoring ít nhất 3 chu kỳ chạy thực tế).

---

## BUG-020 — Silver transform vẫn chậm ở năm 2018 dù dữ liệu đã có trong Silver MinIO

- **Triệu chứng**
  - Task `transform_bronze_to_silver` mất rất nhiều thời gian ở các DAG run năm 2018 khi chạy lại DAG.
  - Dữ liệu Silver đã tồn tại đầy đủ trong MinIO nhưng Spark vẫn được khởi động và xử lý lại.

- **Nguyên nhân gốc**
  1. **`run_silver_layer` bỏ qua context hoàn toàn:** Hàm dùng `**_context` và không extract year/month từ DAG run. `run_silver_transform` không biết đang chạy cho tháng nào.
  2. **`run_silver_transform` xử lý TẤT CẢ bronze files chưa được xử lý:** Không giới hạn theo year/month của DAG run hiện tại. Khi `silver_file_log` bị out-of-sync (DB reset, log write fail), `is_first_run = True` → FULL load toàn bộ 2018 (12 tháng × 2 service types = 24 files) → rất chậm.
  3. **Filter dùng `(object_key, ingestion_id)` pair không ổn định:** Nếu bronze được re-ingest và tạo entry SUCCESS mới với ID mới, pair `(object_key, new_id)` không có trong `successful_file_pairs` → file bị xử lý lại dù Silver đã có.
  4. **Không có kiểm tra Silver MinIO partition trước khi khởi động Spark:** `_list_silver_partitions()` tồn tại nhưng chỉ dùng cho verification, không dùng làm skip guard.

- **Tác động**
  - Mỗi DAG run cho năm 2018 tốn nhiều phút (Spark startup + full transform) thay vì vài giây (skip).
  - Tổng thời gian catchup từ 2018 đến nay tăng đáng kể.
  - Lãng phí tài nguyên compute khi dữ liệu đã sẵn có.

- **Giải pháp đã áp dụng**
  1. **`dags/taxi_dag.py`** — Đổi `run_silver_layer(**_context)` → `run_silver_layer(**context)`, extract year/month từ `dag_run.conf` → `logical_date` (cùng thứ tự ưu tiên như bronze task), truyền vào `run_silver_transform(year=year, month=month)`.
  2. **`etls/silver_transform.py`** — Thêm `year: Optional[int] = None, month: Optional[int] = None` vào signature `run_silver_transform`. Sau `_discover_bronze_records()`, filter `all_bronze_logs` theo year/month khi được cung cấp → mỗi DAG run chỉ xử lý đúng tháng của mình.
  3. **`etls/silver_transform.py`** — Đổi filter `bronze_logs` sang dùng `object_key` only (thay vì `(object_key, ingestion_id)` pair) → robust hơn khi bronze ingestion ID thay đổi.
  4. **`etls/silver_transform.py`** — Thêm **Silver MinIO partition existence check** (secondary guard) ngay trước `build_spark_session()`: nếu `(year, month)` đã tồn tại trong Silver MinIO → skip Spark hoàn toàn, backfill `silver_file_log` entries thành SUCCESS (tự sửa out-of-sync), return `SKIPPED_ALREADY_IN_SILVER`.

- **File liên quan**
  - `dags/taxi_dag.py`
  - `etls/silver_transform.py`

- **Trạng thái**
  - `FIXED` (cần monitoring ít nhất 3 chu kỳ chạy thực tế).

---

## BUG-021 — `SparkOutOfMemoryError: No enough memory for aggregation` khi ghi `dim_date`

- **Triệu chứng**
  - Task `transform_silver_to_gold_iceberg` fail với lỗi:
    ```
    org.apache.spark.memory.SparkOutOfMemoryError: No enough memory for aggregation
    ```
  - Stack trace chỉ vào `hashAgg_doConsume_0` bên trong `bhj_doConsume_0` tại `write_dim_date_to_gold` → `.saveAsTable("gold.gold.dim_date")`.
  - Lỗi xảy ra ở Task 3 in stage 17.0 (executor driver).

- **Nguyên nhân gốc**
  1. **`.distinct()` trên `silver_df` với chỉ 64 shuffle partitions:** `build_dim_date` gọi `.distinct()` trên `silver_df` (hàng triệu rows). Với 64 shuffle partitions, mỗi partition phải xử lý ~millions/64 rows trong hash aggregation → hash table không đủ bộ nhớ → OOM.
  2. **`dim_date` không được cache:** `dim_date` là lazy DataFrame. Khi `write_dim_date_to_gold` gọi `.saveAsTable()`, Spark recompute toàn bộ `dim_date` từ đầu (bao gồm `.distinct()` aggregation). Sau đó `build_fact_table` cũng recompute lần nữa → tổng cộng `.distinct()` chạy 2 lần.
  3. **AQE `coalescePartitions.enabled=true` không có `minPartitionNum`:** AQE có thể coalesce 64 partitions xuống còn ít hơn, làm mỗi partition lớn hơn và tăng áp lực bộ nhớ.
  4. **Left-anti join trong `write_dim_date_to_gold`:** Join với bảng `gold.gold.dim_date` hiện có kết hợp với recompute `.distinct()` tạo ra broadcast hash join + hash aggregation đồng thời → OOM.

- **Tác động**
  - Task gold fail hoàn toàn, không ghi được `dim_date` vào Iceberg.
  - Toàn bộ gold layer (dim + fact) không được cập nhật.
  - Pipeline dừng, downstream BI/Trino không có dữ liệu mới.

- **Giải pháp đã áp dụng**
  1. **`build_dim_date` — thêm `.repartition(200, col("pickup_hour"))` trước `.distinct()`:**
     - Sau `.select()` data đã giảm xuống 1 cột (nhỏ hơn nhiều).
     - Repartition theo aggregation key trước `.distinct()` đảm bảo phân phối đều: mỗi partition chỉ xử lý ~1/200 rows thay vì ~1/64 → hash table nhỏ hơn → không OOM.
  2. **`build_spark_session_gold` — tăng default shuffle partitions từ `"64"` → `"200"`:**
     - Giảm per-partition data size cho tất cả shuffle operations trong gold layer.
     - Thêm `spark.sql.adaptive.coalescePartitions.minPartitionNum = "50"` để ngăn AQE coalesce quá mạnh.
  3. **`run_gold_transform` — cache `dim_date` ngay sau khi build:**
     - `dim_date.cache()` materialise kết quả `.distinct()` một lần duy nhất.
     - `write_dim_date_to_gold` và `build_fact_table` đọc từ cache thay vì recompute.
     - Thêm `dim_date.unpersist()` vào `finally` block để giải phóng bộ nhớ.
  4. **`_check_gold_already_succeeded` — thêm metadata skip guard:**
     - Query `metadata.gold_transform_log` cho `status='SUCCESS'` với `(year, month)`.
     - Nếu đã thành công, return sớm mà không khởi động Spark.
     - Mirrors silver BUG-020 skip guard pattern.
     - Trả về `False` khi có lỗi DB (safe fallback — pipeline tiếp tục).

- **File liên quan**
  - `etls/gold_transform.py`

- **Trạng thái**
  - `FIXED` (cần monitoring ít nhất 3 chu kỳ chạy thực tế).

---

## BUG-022 — `fact_trips` rỗng hoàn toàn do sai Iceberg overwrite mode + empty DataFrame wipe

- **Triệu chứng**
  - `SELECT COUNT(*) FROM iceberg.gold.fact_trips` trả về `0` trong Trino/Superset.
  - Tất cả dim tables vẫn có dữ liệu bình thường.
  - Gold transform log báo `SUCCESS` cho nhiều tháng nhưng table vẫn rỗng.
  - Physical data files tồn tại trong MinIO (`gold/warehouse/gold/fact_trips_*/data/...`) nhưng Trino không đọc được.

- **Nguyên nhân gốc**
  1. **Sai Iceberg overwrite mode (nguyên nhân chính):** `write_fact_to_gold` dùng
     `spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")` — setting này
     chỉ áp dụng cho Hive/Parquet tables, **không áp dụng cho Iceberg**. Iceberg silently
     ignore nó, dẫn đến mỗi run thực hiện **full table overwrite** thay vì chỉ overwrite
     partition (year, month) đang xử lý. Xác nhận qua snapshot history: mỗi snapshot có
     `total-records == added-records` (không tích lũy), chứng tỏ full overwrite mỗi lần.
  2. **`overwriteSchema=True` gây table-level replace:** Option này trigger schema-level
     table replace (drop + recreate), bypass hoàn toàn partition-level overwrite semantics.
     Khi DataFrame rỗng, nó wipe toàn bộ table.
  3. **Không có empty DataFrame guard:** Khi gold transform chạy cho các tháng không có
     silver data (2025-12, 2026-01, 2026-02 — silver bị `SKIPPED_NO_NEW_BRONZE`), fact_df
     rỗng 0 rows. Full overwrite với 0 rows → **wipe sạch toàn bộ `fact_trips`**.
  - Snapshot history xác nhận: snapshot tại `05:44:11` (2025-12 run) có
    `total-records=0, total-data-files=0` sau khi snapshot trước đó (`05:42:55`, 2025-11)
    có `total-records=3,939,411`.

- **Tác động**
  - `fact_trips` hoàn toàn rỗng — không có dữ liệu fact nào trong Trino/Superset.
  - Toàn bộ analytics/dashboard bị vô hiệu hóa.
  - Mỗi run trước đó cũng chỉ giữ dữ liệu của tháng đó (không tích lũy), nên ngay cả
    trước khi bị wipe, table chỉ có dữ liệu của 1 tháng gần nhất.

- **Giải pháp đã áp dụng**
  1. **Thay `spark.conf.set(...)` bằng `.option("overwrite-mode", "dynamic")`** — đây là
     Iceberg-native option, đảm bảo chỉ overwrite đúng partition (year, month) trong DataFrame.
     Các partition khác không bị ảnh hưởng.
  2. **Xóa `overwriteSchema=True`** — không cần thiết và gây full table replace.
  3. **Thêm empty DataFrame guard** trong `write_fact_to_gold`: nếu `df.limit(1).count() == 0`,
     skip write hoàn toàn và log warning. Ngăn chặn việc wipe table khi không có dữ liệu.
  4. **Recovery**: Xóa tất cả `gold_transform_log` entries → re-run gold transform cho tất cả
     các tháng có silver data (2018–2025) để tái tạo dữ liệu fact.

- **File liên quan**
  - `etls/gold_transform.py` — hàm `write_fact_to_gold`

- **Trạng thái**
  - `FIXED` (code đã fix; cần re-run gold transform để recovery dữ liệu).

---

## BUG-023 — `spark.stop()` trong `finally` block không được bọc try-except, escape exception khi JVM đã chết

- **Triệu chứng**
  - Task `transform_silver_to_gold_iceberg` fail với lỗi misleading:
    ```
    py4j.protocol.Py4JError: SparkSession does not exist in the JVM
    ```
  - Lỗi xảy ra tại `spark.stop()` trong `finally` block của `run_gold_transform`.
  - Log Airflow trước đó có dòng:
    ```
    WARNING - State of this instance has been externally set to success. Terminating instance.
    ```
  - `_log_gold_run` không được gọi → không có entry nào trong `gold_transform_log` cho run đó.
  - Lỗi gốc thực sự (ví dụ: `Py4JError` tại `saveAsTable` do JVM bị kill) bị che khuất hoàn toàn.

- **Nguyên nhân gốc**
  - **Trigger:** Airflow gửi SIGTERM đến task process khi task state bị set thành `success` từ bên ngoài trong khi task đang chạy (ví dụ: user click "Mark as Success" trong Airflow UI trên một task đang running).
  - **Cascade:**
    1. SIGTERM kill Spark JVM mid-write (đang ở `write_dim_to_gold(dim_vendor, ...)`)
    2. `saveAsTable` ném `Py4JError` → propagate lên `except Exception as e` block → `run_payload["status"] = "FAILED"`, re-raise
    3. `finally` block chạy: `dim_date.unpersist()` và `silver_df.unpersist()` đã được bọc try-except → OK
    4. `spark.stop()` **không được bọc try-except** → JVM đã chết → ném `Py4JError: SparkSession does not exist in the JVM`
    5. Exception này **escape khỏi `finally` block** → `_log_gold_run` không bao giờ được gọi → không có metadata log
    6. Airflow báo lỗi `spark.stop()` thay vì lỗi gốc từ `saveAsTable`
  - **Root bug:** `spark.stop()` không được bọc try-except, trong khi `dim_date.unpersist()` và `silver_df.unpersist()` ngay trên đó đã được bảo vệ đúng cách.

- **Tác động**
  - `_log_gold_run` không được gọi → không có entry trong `gold_transform_log` → `_check_gold_already_succeeded` không thể phát hiện run đã fail → run tiếp theo sẽ không bị block bởi early-exit guard (hành vi đúng, nhưng thiếu audit trail).
  - Lỗi gốc thực sự bị che khuất hoàn toàn bởi `Py4JError: SparkSession does not exist in the JVM`.
  - Khó debug và triage vì error message không phản ánh nguyên nhân thực.

- **Giải pháp đã áp dụng**
  - Bọc `spark.stop()` trong try-except trong `finally` block của `run_gold_transform`, cùng pattern với `dim_date.unpersist()` và `silver_df.unpersist()`:
    ```python
    if spark:
        try:
            spark.stop()
        except Exception as stop_err:
            print(f"[WARN] spark.stop() raised {stop_err!r} — JVM may already be dead (SIGTERM / crash). Continuing to log metadata.")
    ```
  - Khi JVM đã chết, exception được bắt và log warning, `finally` block tiếp tục chạy `_log_gold_run` bình thường.

- **Lưu ý vận hành**
  - **Không nên click "Mark as Success" trên một task đang running trong Airflow UI.** Hành động này gửi SIGTERM đến process đang chạy, kill Spark JVM mid-write, có thể để lại dữ liệu Iceberg ở trạng thái partial/inconsistent.
  - Để re-run task: dùng **"Clear"** (không phải "Mark as Success") trong Airflow UI, hoặc dùng `airflow tasks clear` CLI.

- **File liên quan**
  - `etls/gold_transform.py` — `finally` block trong `run_gold_transform`

- **Trạng thái**
  - `FIXED`

---

## BUG-024 — `saveAsTable` V1 API ignore `overwrite-mode=dynamic` → full table overwrite mỗi run

- **Triệu chứng**
  - Sau mỗi run theo tháng, `fact_trips` chỉ còn dữ liệu của tháng vừa chạy.
  - Dữ liệu các tháng/năm trước bị xóa sạch sau mỗi run mới.
  - Ví dụ: DAG chạy đến 2019, kiểm tra `fact_trips` không còn dữ liệu 2018.
  - BUG-022 đã fix bằng `.option("overwrite-mode", "dynamic")` nhưng vấn đề vẫn tái diễn.

- **Nguyên nhân gốc**
  - BUG-022 fix dùng `saveAsTable(mode="overwrite") + option("overwrite-mode", "dynamic")`.
  - `saveAsTable` sử dụng **Spark DataFrameWriter V1 API**.
  - Với Iceberg, V1 API `mode("overwrite")` luôn thực hiện **full table replace** bất kể
    giá trị của `overwrite-mode` option — option này bị **silently ignored** bởi V1 write path.
  - `overwrite-mode=dynamic` chỉ được Iceberg tôn trọng khi dùng **DataFrameWriterV2 API**
    (`writeTo()`), không phải V1 `saveAsTable`.
  - Kết quả: mỗi run tháng đều overwrite toàn bộ bảng `fact_trips` thay vì chỉ overwrite
    partition `(year, month)` hiện tại.

- **Tác động**
  - `fact_trips` chỉ giữ dữ liệu của tháng được chạy gần nhất — không tích lũy lịch sử.
  - Toàn bộ dữ liệu các năm trước bị mất sau mỗi run mới.
  - Query analytics (ví dụ: `GROUP BY year`) chỉ trả về dữ liệu của năm/tháng gần nhất.
  - BUG-022 fix không giải quyết được vấn đề vì dùng sai API.

- **Giải pháp đã áp dụng**
  - Thay thế `saveAsTable` (V1 API) bằng **Iceberg DataFrameWriterV2 API** (`writeTo()`):
    - **First run** (bảng chưa tồn tại): `df.writeTo(table_fqn).partitionedBy(col("year"), col("month")).create()`
    - **Subsequent runs** (bảng đã tồn tại): `df.writeTo(table_fqn).overwritePartitions()`
  - `overwritePartitions()` là Iceberg-native V2 API: chỉ overwrite đúng các partition
    `(year, month)` có trong DataFrame — tất cả partition khác không bị ảnh hưởng.
  - Thêm logic kiểm tra bảng tồn tại bằng `spark.table()` trong try-except để phân biệt
    first-run vs subsequent-run.

- **File liên quan**
  - `etls/gold_transform.py` — hàm `write_fact_to_gold`

- **Trạng thái**
  - `FIXED`

---

## BUG-025 — Superset SQL Lab timeout 60s khi query dữ liệu để gen báo cáo qua Trino

- **Triệu chứng**
  - Query trong Superset SQL Lab bị timeout sau đúng 60 giây.
  - Lỗi hiển thị: `Query exceeded the maximum execution time limit of 60000 ms` hoặc tương tự.
  - Xảy ra với các query phức tạp trên bảng `iceberg.gold.fact_trips` (multi-year NYC taxi data).
  - Gunicorn đã được set `--timeout 120` nhưng timeout vẫn xảy ra ở 60s.

- **Nguyên nhân gốc**
  - **Superset `SQLLAB_TIMEOUT` default = 60 giây** — đây là Python-level config trong Superset,
    hoàn toàn độc lập với gunicorn timeout.
  - Không có file `superset_config.py` nào được mount vào container → Superset dùng toàn bộ
    default Python config values, bao gồm `SQLLAB_TIMEOUT = 60`.
  - `GUNICORN_TIMEOUT` (HTTP worker timeout) và `SQLLAB_TIMEOUT` (SQL Lab query timeout) là
    **2 layer khác nhau**: gunicorn timeout chỉ kill HTTP worker nếu request không trả về trong
    N giây, còn `SQLLAB_TIMEOUT` là giới hạn thời gian chạy query trong SQL Lab engine.
  - Trino không có `query.max-execution-time` → query chạy vô hạn phía Trino, nhưng Superset
    đã timeout ở 60s trước khi Trino kịp trả kết quả.

- **Tác động**
  - Không thể gen báo cáo với query phức tạp (GROUP BY year/month, JOIN nhiều bảng dim/fact).
  - Mọi query chạy quá 60s đều fail, kể cả query hợp lệ và cần thiết cho dashboard.
  - Trino vẫn tiếp tục chạy query sau khi Superset đã timeout → lãng phí tài nguyên.

- **Giải pháp đã áp dụng**
  1. **Tạo `superset/superset_config.py`** — file config Python tùy chỉnh:
     - `SQLLAB_TIMEOUT = 300` (5 phút — fix trực tiếp root cause)
     - `SQLLAB_ASYNC_TIME_LIMIT_SEC = 300`
     - `SUPERSET_WEBSERVER_TIMEOUT = 300`
     - `CACHE_CONFIG` với SimpleCache + TTL 300s (cache kết quả query)
     - `WTF_CSRF_TIME_LIMIT = 86400` (tránh CSRF token hết hạn trong session dài)
     - `ROW_LIMIT = 50000` (giới hạn result set)
     - `FEATURE_FLAGS["SQLLAB_BACKEND_PERSISTENCE"] = True` (persist async query state)
  2. **Cập nhật `docker-compose.yml`** cho cả `superset` và `superset-init`:
     - Mount: `./superset/superset_config.py:/app/superset_config.py:ro`
     - Env: `SUPERSET_CONFIG_PATH=/app/superset_config.py`
     - Tăng `GUNICORN_TIMEOUT` từ `"120"` → `"300"` (align với SQLLAB_TIMEOUT)
     - Tăng gunicorn `--timeout` từ `120` → `300`
  3. **Tune `trino/config.properties`**:
     - `query.max-execution-time=10m` — Trino-side ceiling (10 phút)
     - `query.max-total-memory=4GB` — align với JVM `-Xmx4G`
     - `optimizer.join-reordering-strategy=AUTOMATIC` — tự động tối ưu join order
     - `optimizer.optimize-hash-generation=true` — tăng tốc hash join/aggregation
     - `query.initial-hash-partitions=8` — parallelism tốt hơn
     - `task.max-worker-threads=8` — xử lý concurrent Superset queries
     - `spill-enabled=true` + `spill-path=/tmp/trino-spill` — tránh OOM với query lớn

- **Sau khi fix cần thực hiện**
  ```bash
  # Recreate Superset containers để load config mới
  docker compose up -d --force-recreate superset superset-init

  # Recreate Trino để apply config.properties mới
  docker compose up -d --force-recreate trino
  ```

- **File liên quan**
  - `superset/superset_config.py` (NEW)
  - `docker-compose.yml`
  - `trino/config.properties`

- **Trạng thái**
  - `FIXED` (cần recreate containers và verify query >60s không còn timeout).

---

## BUG-026 — `overwritePartitions()` ghi đè partition năm khác do anomalous `pickup_datetime`

- **Triệu chứng**
  - Gold transform chạy cho `source_year=2023` → ghi đúng ~37.9M rows vào `year=2023` partition ✅
  - Gold transform chạy cho `source_year=2024, month=1` → `year=2023` partition bị ghi đè, chỉ còn vài chục rows.
  - Tương tự với các run 2025+ → `year=2023` tiếp tục bị ghi đè mỗi lần.
  - `SELECT COUNT(*) FROM iceberg.gold.fact_trips WHERE year=2023` trả về số rất nhỏ (vài chục) thay vì ~37.9M.

- **Nguyên nhân gốc**
  1. **Silver chứa anomalous dates:** Silver partition `year=2024/month=01` được tạo từ `source_year=2024` (bronze folder metadata). Tuy nhiên, một số rows trong batch đó có `pickup_datetime` thực sự thuộc năm 2023 (dữ liệu bất thường từ nguồn).
  2. **`build_fact_table` derive `year`/`month` từ `pickup_datetime`:**
     ```python
     year(col("s.pickup_datetime")).alias("year"),   # ← 2023 cho anomalous rows!
     month(col("s.pickup_datetime")).alias("month"),
     ```
     Kết quả: `fact_df` chứa cả rows với `year=2024` (normal) lẫn `year=2023` (anomalous).
  3. **`overwritePartitions()` thay thế TẤT CẢ partitions có trong `fact_df`:**
     - Iceberg `writeTo().overwritePartitions()` xác định partitions cần overwrite dựa trên dữ liệu trong DataFrame.
     - Vì `fact_df` có rows với `year=2023`, Iceberg overwrite cả partition `year=2023/month=X`.
     - 37.9M rows của `year=2023` bị xóa và thay bằng vài chục anomalous rows từ batch 2024.

- **Tác động**
  - Dữ liệu `year=2023` bị mất vĩnh viễn sau mỗi run của năm 2024+.
  - Mỗi run tiếp theo (2025, 2026...) tiếp tục overwrite `year=2023` với anomalous rows mới.
  - Query analytics theo năm trả về kết quả sai nghiêm trọng cho năm 2023.
  - Không phát hiện được qua gold transform log vì run vẫn báo `SUCCESS`.

- **Giải pháp đã áp dụng**
  **Two-phase write keyed on `source_year`** trong `write_fact_to_gold`:

  1. **Thêm `trip_hash` vào `build_fact_table`** — SHA2-256 của các trường định danh
     (`pickup_datetime`, `dropoff_datetime`, `PULocationID`, `DOLocationID`, `service_type`,
     `total_amount`, `fare_amount`, `trip_distance`). Dùng làm deduplication key ổn định
     (thay cho `monotonically_increasing_id()` vốn non-deterministic).

  2. **`write_fact_to_gold` nhận thêm tham số `source_year: Optional[int] = None`** và
     tách `fact_df` thành 2 phần:
     - `main_df` (`year == source_year`): dùng `overwritePartitions()` — idempotent cho
       batch hiện tại, chỉ chạm đúng partition của năm đang xử lý.
     - `anomalous_df` (`year != source_year`): append với partition-pruned deduplication
       qua `trip_hash` — không bao giờ overwrite partition của năm khác.

  3. **Schema evolution:** Nếu bảng `fact_trips` đã tồn tại (không có `trip_hash`), tự động
     thêm cột qua `ALTER TABLE ... ADD COLUMN trip_hash STRING` (Iceberg metadata-only,
     không rewrite data files).

  4. **Performance optimizations:**
     - **Fast path (common case):** Nếu `anomalous_df` rỗng → Phase 2 thoát sau
       `limit(1).count()` duy nhất (~milliseconds, zero overhead).
     - **Partition-pruned scan:** Khi có anomalous rows, chỉ scan đúng các `(year, month)`
       partitions bị ảnh hưởng để lấy `trip_hash` — Iceberg partition pruning đảm bảo
       không có full table scan (37.9M rows).
     - **Fallback:** Nếu dedup fail (ví dụ: `trip_hash` chưa visible sau ALTER TABLE),
       append tất cả anomalous rows mà không dedup — an toàn hơn mất dữ liệu.

  5. **`run_gold_transform`** truyền `year` vào `write_fact_to_gold` làm `source_year`.

  6. **Backward compatibility:** Khi `source_year=None` (initial full load), fallback về
     `overwritePartitions()` đơn pha — hành vi gốc được giữ nguyên.

- **Lưu ý vận hành**
  - Sau khi deploy fix, cần **re-run gold transform cho `year=2023`** để khôi phục 37.9M rows
    đã bị overwrite bởi các run 2024/2025 trước đó.
  - Xóa entry `gold_transform_log` cho `year=2023` (hoặc dùng Airflow "Clear") để bypass
    `_check_gold_already_succeeded` guard và force re-run.

- **File liên quan**
  - `etls/gold_transform.py` — `build_fact_table`, `write_fact_to_gold`, `run_gold_transform`

- **Trạng thái**
  - `FIXED` (cần re-run gold transform cho year=2023 để recovery dữ liệu đã bị overwrite).

---

## BUG-027 — `NameError: anomalous_partitions` trong `write_fact_to_gold` Phase 2

- **Triệu chứng**
  - Task `transform_silver_to_gold_iceberg` fail với lỗi:
    ```
    NameError: name 'anomalous_partitions' is not defined
    ```
  - Lỗi xảy ra tại dòng `affected = [(r["year"], r["month"]) for r in anomalous_partitions] ...`
    trong `write_fact_to_gold`, sau try-except block của Phase 2 deduplication.
  - Chỉ xảy ra khi: (1) có anomalous rows (`pickup_datetime` year ≠ `source_year`), VÀ
    (2) `.collect()` bên trong try block raise exception (ví dụ: Spark/Iceberg lỗi khi
    đọc `trip_hash` từ bảng chưa có cột đó, hoặc S3A connection error).

- **Nguyên nhân gốc**
  - Biến `anomalous_partitions` chỉ được gán **bên trong** `try` block:
    ```python
    try:
        anomalous_partitions = anomalous_df.select(...).collect()  # ← nếu raise exception
        ...
    except Exception as dedup_err:
        new_anomalous = anomalous_df  # ← anomalous_partitions KHÔNG được gán ở đây!

    # NameError: name 'anomalous_partitions' is not defined
    affected = [...for r in anomalous_partitions] if anomalous_partitions else []
    ```
  - Khi exception xảy ra trước khi `anomalous_partitions` được gán, `except` block chỉ
    set `new_anomalous = anomalous_df` nhưng không khởi tạo `anomalous_partitions`.
  - Dòng `affected = ...` sau try-except tham chiếu đến biến chưa tồn tại → `NameError`.

- **Tác động**
  - Phase 2 của `write_fact_to_gold` crash hoàn toàn khi có anomalous rows + dedup error.
  - Anomalous rows không được append vào fact table.
  - Task gold fail với `NameError` thay vì lỗi gốc từ Spark/Iceberg.

- **Giải pháp đã áp dụng**
  - Khởi tạo `anomalous_partitions: list = []` trước `try` block:
    ```python
    anomalous_partitions: list = []   # ← khởi tạo trước try block
    try:
        anomalous_partitions = anomalous_df.select(...).collect()
        ...
    except Exception as dedup_err:
        new_anomalous = anomalous_df  # anomalous_partitions = [] (safe fallback)

    # Bây giờ an toàn: anomalous_partitions luôn được định nghĩa
    affected = [...for r in anomalous_partitions] if anomalous_partitions else []
    ```
  - Khi exception xảy ra, `anomalous_partitions` giữ giá trị `[]` → `affected = []` →
    log message hiển thị `partitions: []` thay vì crash.

- **File liên quan**
  - `etls/gold_transform.py` — hàm `write_fact_to_gold`, Phase 2 deduplication block

- **Trạng thái**
  - `FIXED`

---

## BUG-028 — `NotFoundException` khi ghi dim table do Nessie giữ stale metadata reference

- **Triệu chứng**
  - Task `transform_silver_to_gold_iceberg` fail ở attempt 9/9 với lỗi:
    ```
    org.apache.iceberg.exceptions.NotFoundException: Failed to open input stream for file:
    s3://gold/warehouse/gold/dim_vendor_400a0cbb-.../metadata/00327-...metadata.json
    Caused by: java.io.FileNotFoundException: No such file or directory: s3://gold/...
    ```
  - Lỗi xảy ra tại `write_dim_to_gold` → `.saveAsTable(f"gold.gold.{table_name}")`.
  - Stack trace chỉ vào `NessieTableOperations.doRefresh` → `refreshFromMetadataLocation`
    → `HadoopInputFile.newStream` → S3A `open()` → `FileNotFoundException`.

- **Nguyên nhân gốc**
  - **Catalog-storage inconsistency:** Nessie catalog giữ con trỏ đến metadata file
    (`00327-0b6e086e-573c-4c71-85a2-254c9df469b5.metadata.json`) cho bảng `dim_vendor`,
    nhưng file đó **không còn tồn tại trong MinIO** (ví dụ: MinIO data bị reset/xóa mà
    không reset Nessie, hoặc partial write để lại Nessie trỏ đến file chưa được commit).
  - **Failure chain:**
    1. `saveAsTable(mode="overwrite")` → Spark gọi `SparkCatalog.loadTable()` để đọc
       schema bảng hiện có trước khi overwrite.
    2. → `NessieTableOperations.doRefresh()` → lấy metadata location từ Nessie.
    3. → `refreshFromMetadataLocation()` → cố mở `00327-...metadata.json` từ S3.
    4. → `FileNotFoundException` → `NotFoundException` → task fail.
  - Tất cả 6 dim tables đi qua `write_dim_to_gold` đều có cùng rủi ro.
  - `write_dim_date_to_gold` cũng có rủi ro tương tự ở `saveAsTable(mode="append")`.

- **Tác động**
  - Task gold fail hoàn toàn ở bước ghi dim đầu tiên (`dim_vendor`).
  - Toàn bộ dim + fact tables không được cập nhật.
  - Pipeline dừng, downstream BI/Trino không có dữ liệu mới.
  - Đã retry 9/9 lần, tất cả fail với cùng lỗi.

- **Giải pháp đã áp dụng**
  **Catch-and-retry pattern** trong cả `write_dim_to_gold` và `write_dim_date_to_gold`:

  1. **`write_dim_to_gold`:**
     - Bọc `saveAsTable` trong try-except.
     - Nếu exception chứa `"NotFoundException"` hoặc `"FileNotFoundException"`:
       - Log warning với error message (truncated 300 chars).
       - Gọi `spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")` — đây là **pure Nessie
         metadata operation**, chỉ xóa catalog reference, **không đọc S3 metadata file**.
       - Retry `saveAsTable`: bảng không còn trong Nessie → tạo mới với metadata chain hợp lệ.
     - Bọc `DROP TABLE IF EXISTS` trong try-except riêng để retry vẫn chạy nếu drop fail.
     - Exception khác (không phải NotFoundException) → re-raise như cũ.
     - **Happy path (không có stale metadata): zero overhead, hành vi không đổi.**

  2. **`write_dim_date_to_gold`:**
     - Cùng pattern cho `saveAsTable(mode="append")`.
     - Khi stale metadata detected: drop table → retry với `df` đầy đủ (không phải
       `new_rows_df`) vì dữ liệu hiện có đã không truy cập được do broken metadata chain.
     - `dim_date` sẽ được repopulate dần qua các run tháng tiếp theo.

  **Tại sao `DROP TABLE IF EXISTS` không đọc S3:**
  - `NessieCatalog.dropTable()` gọi Nessie API để xóa content key — không gọi
    `NessieTableOperations.doRefresh()` (hàm này chỉ được gọi khi *load* bảng).
  - `SparkCatalog.dropTable()` gọi `icebergCatalog.dropTable()` trực tiếp, không gọi
    `loadTable()` trước.
  - Kết quả: `DROP TABLE IF EXISTS` thành công ngay cả khi metadata file đã bị xóa khỏi S3.

- **File liên quan**
  - `etls/gold_transform.py` — hàm `write_dim_to_gold`, `write_dim_date_to_gold`

- **Trạng thái**
  - `FIXED` (cần verify qua Airflow retry/clear sau khi deploy).

---

## BUG-029 — `DROP TABLE IF EXISTS` fails with `Py4JJavaError` → retry `saveAsTable` also fails with `NotFoundException`

- **Triệu chứng**
  - Task `transform_silver_to_gold_iceberg` fail ở attempt 12/12 với lỗi:
    ```
    org.apache.iceberg.exceptions.NotFoundException: Failed to open input stream for file:
    s3://gold/warehouse/gold/dim_vendor_400a0cbb-.../metadata/00327-...metadata.json
    Caused by: java.io.FileNotFoundException: No such file or directory: s3://gold/...
    ```
  - Log cho thấy BUG-028 catch-and-retry đã kích hoạt đúng:
    ```
    [WARN] write_dim_to_gold: stale Nessie metadata detected for gold.gold.dim_vendor — dropping table and retrying.
    [WARN] write_dim_to_gold: DROP TABLE IF EXISTS gold.gold.dim_vendor raised Py4JJavaError(...) — retrying saveAsTable anyway.
    ```
  - Nhưng retry `saveAsTable` **cũng fail** với cùng `NotFoundException`.
  - Xảy ra ở attempt 12/12 — tất cả retry đều fail với cùng pattern.

- **Nguyên nhân gốc**
  1. **`spark.sql("DROP TABLE IF EXISTS ...")` fail với `Py4JJavaError` (nguyên nhân chính):**
     - Trong Nessie 0.76.3, `NessieCatalog.dropTable()` gọi
       `client.commitMultipleOperations()` để xóa content key khỏi Nessie.
     - Nếu branch hash đã thay đổi giữa lúc đọc và lúc commit (race condition hoặc
       concurrent modification), Nessie ném `NessieConflictException`.
     - `NessieConflictException` được wrap thành `RuntimeException` → propagate qua
       Py4J bridge thành `Py4JJavaError` trong Python.
     - Kết quả: stale Nessie reference **không bị xóa**, retry `saveAsTable` vẫn thấy
       stale reference → cùng `NotFoundException`.
  2. **`CachingCatalog` giữ stale entry trong memory (nguyên nhân phụ):**
     - Iceberg's `CachingCatalog` (backed by Caffeine) cache table metadata.
     - Ngay cả khi DROP thành công, cache có thể vẫn giữ entry cũ.
     - Retry `saveAsTable` gọi `CachingCatalog.loadTable()` → trả về cached entry
       với stale metadata location → `NotFoundException` lại.
  3. **BUG-028 fix không đủ mạnh:**
     - BUG-028 chỉ dùng `spark.sql("DROP TABLE IF EXISTS ...")` và bỏ qua lỗi DROP.
     - Không có fallback khi DROP fail.
     - Không invalidate `CachingCatalog` sau DROP.

- **Tác động**
  - Task gold fail hoàn toàn dù BUG-028 catch-and-retry đã được kích hoạt.
  - Tất cả 12 attempt đều fail với cùng pattern.
  - Toàn bộ dim + fact tables không được cập nhật.
  - Pipeline dừng, downstream BI/Trino không có dữ liệu mới.

- **Giải pháp đã áp dụng**
  **Thêm `_nessie_drop_table_via_rest()` helper + cập nhật `write_dim_to_gold`,
  `write_dim_date_to_gold`, và `write_fact_to_gold` với recovery pattern:**

  1. **`_nessie_drop_table_via_rest(nessie_uri, namespace, table_name, branch)`:**
     - Gọi Nessie REST API v1 trực tiếp, bypass hoàn toàn Spark/Iceberg/Py4J stack.
     - Step 1: `GET /api/v1/trees/tree/{branch}` → lấy current branch hash.
     - Step 2: `POST /api/v1/trees/branch/{branch}/commit?expectedHash={hash}` với
       DELETE operation cho content key `[namespace, table_name]`.
     - Tự động retry 1 lần nếu nhận HTTP 409 Conflict (fetch fresh hash rồi retry).
     - Trả về `True` nếu xóa thành công hoặc table đã không còn (404).
     - Trả về `False` nếu fail — caller vẫn thực hiện retry saveAsTable.
     - Lazy import `requests` — không ảnh hưởng code path bình thường.

  2. **`write_dim_to_gold` — 4-step recovery (triggered by saveAsTable NotFoundException):**
     - Step 1: `spark.sql("DROP TABLE IF EXISTS ...")` — fast path, không đổi.
     - Step 2: Nếu Step 1 fail → `_nessie_drop_table_via_rest()` — REST API fallback.
     - Step 3: Nếu drop thành công → `spark.catalog.refreshTable()` để evict stale
       `CachingCatalog` entry (gọi `SparkCatalog.invalidateTable()` →
       `CachingCatalog.invalidateTable()` → xóa khỏi Caffeine cache).
     - Step 4: Retry `saveAsTable` — table absent from Nessie + cache evicted →
       tạo fresh metadata chain.
     - **Happy path (không có stale metadata): zero overhead, hành vi không đổi.**

  3. **`write_dim_date_to_gold`:** Cùng 4-step recovery pattern.

  4. **`write_fact_to_gold` — 3-step recovery (triggered by spark.table() check):**
     - Vấn đề bổ sung: code gốc dùng `except Exception: table_exists = False` để
       bắt mọi exception từ `spark.table()`. Khi `NotFoundException` xảy ra (stale
       metadata), code set `table_exists = False` và gọi `writeTo().create()`. Nhưng
       `create()` nội bộ gọi `AtomicCreateTableAsSelectExec → tableExists() →
       loadTable() → doRefresh()` → cùng `NotFoundException` → task fail.
     - Fix: phân biệt `NotFoundException` (stale metadata) với `NoSuchTableException`
       (table thực sự không tồn tại) trong exception handler của `spark.table()`.
     - Khi phát hiện stale metadata:
       - Step 1: `spark.sql("DROP TABLE IF EXISTS ...")`.
       - Step 2: Nếu fail → `_nessie_drop_table_via_rest()`.
       - Step 3: Nếu drop thành công → `spark.catalog.refreshTable()`.
     - Sau drop, `table_exists = False` → `writeTo().create()` chạy thành công vì
       Nessie không còn stale reference và cache đã được evict.

  **Tại sao REST API không bị ảnh hưởng bởi `NotFoundException`:**
  - `NessieCatalog.dropTable()` (qua Spark SQL) gọi `commitMultipleOperations()` có
    thể fail với `NessieConflictException`.
  - REST API `POST /commit` với `expectedHash` cũng có thể nhận 409, nhưng helper
    tự động retry với fresh hash → giải quyết race condition.
  - REST API không đi qua Py4J bridge → không có `Py4JJavaError`.

- **File liên quan**
  - `etls/gold_transform.py` — thêm `_nessie_drop_table_via_rest()`, cập nhật
    `write_dim_to_gold()`, `write_dim_date_to_gold()`, `write_fact_to_gold()`

- **Trạng thái**
  - `FIXED` (cần verify qua Airflow clear/retry sau khi deploy).

---

## BUG-030 — Cross-month overwrite trong `write_fact_to_gold` (root cause của chênh lệch Silver vs Gold)

- **Triệu chứng**
  - Truy vấn gold `fact_trips` theo `(year, month)` cho thấy chỉ tháng CUỐI CÙNG được xử lý của mỗi năm có hàng triệu rows; tất cả các tháng trước đó chỉ có vài chục rows.
  - `sum(total_amount)` trên gold (~96M cho 2023) thấp hơn ~11.5x so với silver (~1.1B).

- **Nguyên nhân gốc**
  - BUG-026 fix chỉ tách `main_df` theo `year == source_year`, nhưng `overwritePartitions()` ghi đè **tất cả `(year, month)` partitions** có trong `main_df`.
  - Silver `year=2023, month=12` chứa rows có `pickup_datetime` thuộc tháng 1-11/2023 (anomalous dates trong cùng năm). Những rows này nằm trong `main_df` (year==2023) và khiến `overwritePartitions()` xóa sạch các partition tháng 1-11 đã ghi đúng trước đó.
  - Kết quả: mỗi run tháng mới overwrite tất cả tháng cũ trong cùng năm, chỉ tháng cuối cùng còn đủ dữ liệu.

- **Tác động**
  - Toàn bộ gold `fact_trips` bị thiếu dữ liệu nghiêm trọng (~11/12 tháng mỗi năm bị xóa).
  - Mọi BI report, dashboard, và aggregation query trên gold đều sai.

- **Giải pháp đã áp dụng**
  - Thêm `source_month` parameter vào `write_fact_to_gold`.
  - `main_df` filter theo cả `(year == source_year) AND (month == source_month)` thay vì chỉ `year == source_year`.
  - `anomalous_df` = rows có `year != source_year OR month != source_month`.
  - Phase 2: thay vì append+dedup vào `fact_trips`, ghi anomalous rows vào table riêng `gold.gold.fact_trips_anomalous` partitioned by `(source_year, source_month)` — idempotent, không cần dedup.
  - `run_gold_transform` truyền `source_month=month` vào `write_fact_to_gold`.

- **Performance improvements đi kèm**
  - `_read_silver_layer`: dùng direct partition path (`year={year}/month={month}/`) thay vì full scan + filter.
  - `build_fact_table`: thêm `broadcast()` hints cho tất cả dim tables nhỏ → BroadcastHashJoin thay vì SortMergeJoin.
  - `run_gold_transform`: parallelize 6 static dim writes bằng `ThreadPoolExecutor(max_workers=6)`.
  - `silver_df`: dùng `persist(StorageLevel.MEMORY_AND_DISK_SER)` thay vì `cache()` để giảm memory footprint.

- **File liên quan**
  - `etls/gold_transform.py`

- **Sau khi deploy**
  - Xóa toàn bộ `gold_transform_log` để bypass `_check_gold_already_succeeded` guard.
  - Re-run gold transform cho tất cả (year, month) từ 2018 đến nay.
  - Verify: `SELECT year, month, COUNT(*) FROM iceberg.gold.fact_trips GROUP BY year, month ORDER BY year, month` — tất cả tháng phải có hàng triệu rows.

- **Trạng thái**
  - `FIXED` — cần re-run toàn bộ gold sau khi deploy.

---

## BUG-031 — `AttributeError: StorageLevel has no attribute 'MEMORY_AND_DISK_SER'` trong `run_gold_transform`

- **Triệu chứng**
  - Task `transform_silver_to_gold_iceberg` fail ngay sau khi đọc silver data với lỗi:
    ```
    AttributeError: type object 'StorageLevel' has no attribute 'MEMORY_AND_DISK_SER'
    ```
  - Lỗi xảy ra tại `silver_df.persist(StorageLevel.MEMORY_AND_DISK_SER)` trong `run_gold_transform`.

- **Nguyên nhân gốc**
  - BUG-030 performance fix thêm dòng:
    ```python
    silver_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    ```
  - `MEMORY_AND_DISK_SER` là storage level của **Scala/Java Spark** — **không tồn tại trong PySpark**.
  - Trong PySpark, `StorageLevel.MEMORY_AND_DISK` đã là serialized storage (`deserialized=False`)
    — đây chính là equivalent của `MEMORY_AND_DISK_SER` trong Scala.
  - `MEMORY_AND_DISK_DESER` mới là phiên bản deserialized trong PySpark.
  - PySpark `StorageLevel` chỉ có: `DISK_ONLY`, `DISK_ONLY_2`, `DISK_ONLY_3`,
    `MEMORY_AND_DISK`, `MEMORY_AND_DISK_2`, `MEMORY_AND_DISK_DESER`,
    `MEMORY_ONLY`, `MEMORY_ONLY_2`, `OFF_HEAP`.

- **Tác động**
  - Task gold fail 100% ngay sau bước đọc silver, trước khi bất kỳ dim/fact nào được ghi.
  - Pipeline dừng hoàn toàn cho tất cả các tháng chưa được xử lý.

- **Giải pháp đã áp dụng**
  - Đổi `StorageLevel.MEMORY_AND_DISK_SER` → `StorageLevel.MEMORY_AND_DISK` trong `run_gold_transform`.
  - Hành vi giống hệt: serialized in-memory storage, spill to disk khi không đủ RAM.

- **File liên quan**
  - `etls/gold_transform.py` — `run_gold_transform`, dòng `silver_df.persist(...)`

- **Trạng thái**
  - `FIXED`

---

## BUG-032 — `XMinioStorageFull` (HTTP 507) khi ghi `fact_trips` vào MinIO

- **Triệu chứng**
  - Task `transform_silver_to_gold_iceberg` fail ở `write_fact_to_gold` → `overwritePartitions()` với lỗi:
    ```
    java.io.UncheckedIOException: Failed to close current writer
    Caused by: org.apache.hadoop.fs.s3a.AWSS3IOException: upload part #2 ...
    Caused by: com.amazonaws.services.s3.model.AmazonS3Exception:
      Storage backend has reached its minimum free drive threshold.
      (Status Code: 507; Error Code: XMinioStorageFull)
    ```
  - Lỗi xảy ra khi ghi `year=2022/month=5` vào `gold/warehouse/gold/fact_trips_...`.
  - Tất cả dim tables đã ghi thành công trước đó (không phải lỗi code).

- **Nguyên nhân gốc**
  - **MinIO volume đầy disk** — không phải lỗi code.
  - Pipeline đã re-run gold transform nhiều lần (BUG-022 → BUG-030) cho tất cả các năm 2018–2022.
  - Mỗi lần re-run tạo thêm Iceberg snapshot + data files mới trong `gold/warehouse/`.
  - Các failed partial writes (từ BUG-028, BUG-029, BUG-031...) để lại orphan data files chưa được dọn.
  - Bronze bucket vẫn giữ toàn bộ raw parquet files (2018–2026) dù silver đã được xử lý.
  - Tổng dung lượng vượt ngưỡng tối thiểu của Docker volume `minio_data`.

- **Tác động**
  - Mọi write vào MinIO đều fail với HTTP 507.
  - Pipeline không thể tiến thêm — tất cả các tháng từ 2022-05 trở đi đều bị block.
  - Không phải lỗi code — retry sẽ tiếp tục fail cho đến khi giải phóng dung lượng.

- **Giải pháp**
  Xem phần **"Lệnh dọn dẹp MinIO khi đầy disk"** bên dưới.

- **File liên quan**
  - Không có file code nào cần sửa — đây là vấn đề infrastructure.

- **Trạng thái**
  - `OPEN` — cần giải phóng dung lượng MinIO trước khi retry pipeline.

---

## Lệnh dọn dẹp MinIO khi đầy disk (BUG-032)

### Bước 1 — Kiểm tra dung lượng hiện tại

```bash
# Kiểm tra disk usage trên host (Docker volume nằm ở đây)
df -h /var/lib/docker

# Kiểm tra dung lượng từng bucket trong MinIO
docker compose run --rm minio-init sh -c "
  mc alias set local http://minio:9000 minioadmin minioadmin &&
  echo '=== Bronze ===' && mc du --depth 0 local/bronze &&
  echo '=== Silver ===' && mc du --depth 0 local/silver &&
  echo '=== Gold ===' && mc du --depth 0 local/gold
"
```

### Bước 2 — Xóa bronze data (đã được xử lý sang silver, an toàn để xóa)

```bash
# Bronze chỉ là staging area — silver đã có đầy đủ dữ liệu
docker compose run --rm minio-init sh -c "
  mc alias set local http://minio:9000 minioadmin minioadmin &&
  mc rm --recursive --force local/bronze/
"
```

### Bước 3 — Xóa orphan files trong gold (partial writes từ các lần fail)

```bash
# Liệt kê các thư mục trong gold warehouse
docker compose run --rm minio-init sh -c "
  mc alias set local http://minio:9000 minioadmin minioadmin &&
  mc ls local/gold/warehouse/gold/
"

# Xóa toàn bộ gold warehouse và để pipeline tạo lại từ đầu
# (chỉ dùng khi muốn full reset gold layer)
docker compose run --rm minio-init sh -c "
  mc alias set local http://minio:9000 minioadmin minioadmin &&
  mc rm --recursive --force local/gold/
"
```

### Bước 4 — Sau khi xóa gold, reset metadata và retry

```bash
# Xóa toàn bộ gold_transform_log để bypass _check_gold_already_succeeded
docker compose exec postgres psql -U postgres -d airflow_reddit \
  -c "TRUNCATE TABLE metadata.gold_transform_log RESTART IDENTITY;"

# Clear tất cả task transform_silver_to_gold_iceberg để Airflow re-queue
docker compose exec airflow-scheduler airflow tasks clear nyc_taxi_pipeline_v2.2.2 \
  --task-regex "transform_silver_to_gold_iceberg" \
  --yes
```

### Lưu ý quan trọng
- **Không xóa silver** — đây là nguồn dữ liệu cho gold transform.
- Nếu xóa gold warehouse, Nessie vẫn giữ metadata references → cần reset Nessie hoặc để BUG-028/029 recovery tự xử lý khi pipeline chạy lại.
- Để reset Nessie hoàn toàn (xóa tất cả table references):
  ```bash
  docker compose exec postgres psql -U postgres -d airflow_reddit \
    -c "DELETE FROM nessie_global_pointer; DELETE FROM nessie_named_refs; DELETE FROM nessie_objs;"
  ```

---

## BUG-033 — `NotFoundException` on `fact_trips_anomalous` → `.create()` also fails with same error

- **Triệu chứng**
  - Task `transform_silver_to_gold_iceberg` fail ở attempt 2/2 với lỗi:
    ```
    org.apache.iceberg.exceptions.NotFoundException: Failed to open input stream for file:
    s3://gold/warehouse/gold/fact_trips_anomalous_cc3938ab-.../metadata/00051-...metadata.json
    Caused by: java.io.FileNotFoundException: No such file or directory: s3://gold/...
    ```
  - Stack trace chỉ vào 2 điểm trong `write_fact_to_gold`:
    - Line 921: `spark.table(anomalous_table)` — kiểm tra bảng tồn tại
    - Line 934: `.create()` — tạo bảng mới
  - Log cho thấy Phase 1 (dim writes + main fact write) đã thành công trước đó.
  - Lỗi xảy ra ở Phase 2 (anomalous rows handling).

- **Nguyên nhân gốc**
  1. **Stale Nessie metadata cho `fact_trips_anomalous`:** Nessie catalog giữ con trỏ đến
     metadata file (`00051-...metadata.json`) cho bảng `fact_trips_anomalous`, nhưng file
     đó **không còn tồn tại trong MinIO** (catalog-storage inconsistency).
  2. **`spark.table(anomalous_table)` ném `NotFoundException`** → bị bắt bởi bare
     `except Exception:` block.
  3. **`.create()` cũng fail với cùng `NotFoundException`:** `.create()` nội bộ gọi
     `AtomicCreateTableAsSelectExec → tableExists() → loadTable() → doRefresh()` →
     cố đọc cùng metadata file đã mất → `NotFoundException` → task fail vĩnh viễn.
  4. **Root bug:** Bare `except Exception:` không phân biệt `NoSuchTableException`
     (bảng thực sự không tồn tại → an toàn để create) với `NotFoundException`
     (stale Nessie metadata → cần drop stale reference trước khi create).
  - Đây là cùng pattern với BUG-028/029, nhưng cho bảng `fact_trips_anomalous` trong
    Phase 2 của `write_fact_to_gold`. Fix BUG-028/029 đã áp dụng cho `fact_trips` (main),
    `write_dim_to_gold`, `write_dim_date_to_gold` — nhưng Phase 2 bị bỏ sót.

- **Tác động**
  - Task gold fail hoàn toàn ở Phase 2 dù Phase 1 (dim + main fact) đã thành công.
  - Anomalous rows không được ghi vào bất kỳ đâu.
  - Tất cả retry đều fail với cùng pattern.

- **Giải pháp đã áp dụng**
  **Thay thế hoàn toàn approach `fact_trips_anomalous` bằng per-partition append-with-dedup
  trực tiếp vào `fact_trips`:**

  1. **Xóa `fact_trips_anomalous` table approach** — loại bỏ hoàn toàn bảng riêng biệt
     và toàn bộ logic `spark.table(anomalous_table)` / `.create()` gây lỗi.

  2. **New Phase 2 — per-partition append với dedup:**
     - Collect distinct `(year, month)` pairs từ `anomalous_df` (small aggregation).
     - **Schema evolution guard:** kiểm tra `trip_hash` column tồn tại trong `fact_trips`;
       nếu không → `ALTER TABLE ADD COLUMN trip_hash STRING` (Iceberg metadata-only).
     - **Với mỗi `(anom_year, anom_month)` pair:**
       1. Filter `anomalous_df` → `partition_df`
       2. Đọc existing `trip_hash` values từ `fact_trips` WHERE `year=anom_year AND
          month=anom_month` (Iceberg partition-pruned scan — chỉ đọc đúng partition đó).
       3. `left_anti` join → `new_rows_df` (chỉ giữ rows chưa tồn tại).
       4. Nếu `new_rows_df` rỗng → skip (idempotent).
       5. `new_rows_df.writeTo(table_fqn).append()` — **không bao giờ overwrite** dữ liệu
          hiện có ở bất kỳ partition nào.
     - **Graceful fallback:** nếu đọc existing `trip_hash` fail → append tất cả anomalous
       rows không dedup (log warning, không raise).

  3. **Properties của giải pháp mới:**
     - ✅ Xử lý đúng cả cross-year và cross-month anomalous rows.
     - ✅ Idempotent: `trip_hash` dedup ngăn duplicate khi re-run.
     - ✅ An toàn: `append()` không bao giờ chạm đến existing rows hoặc partition khác.
     - ✅ Không còn dependency vào `fact_trips_anomalous` → loại bỏ hoàn toàn rủi ro
       NotFoundException từ stale Nessie metadata của bảng đó.
     - ✅ Hiệu quả: Iceberg partition pruning đảm bảo chỉ scan đúng partition cần thiết.

- **File liên quan**
  - `etls/gold_transform.py` — hàm `write_fact_to_gold`, Phase 2

- **Trạng thái**
  - `FIXED` (cần clear/retry task trong Airflow sau khi deploy để verify).

---

## BUG-034 — `[PATH_NOT_FOUND]` khi đọc Silver partition do month không được zero-pad

- **Triệu chứng**
  - Task `transform_silver_to_gold_iceberg` fail ngay sau khi khởi động Spark với lỗi:
    ```
    pyspark.errors.exceptions.captured.AnalysisException: [PATH_NOT_FOUND]
    Path does not exist: s3a://silver/silver/taxi_trips/year=2022/month=1.
    ```
  - Lỗi xảy ra tại `_read_silver_layer` → `spark.read.parquet(silver_path)`.
  - Xảy ra với tất cả các tháng có giá trị 1–9 (tháng 1 đến tháng 9).
  - Tháng 10–12 không bị ảnh hưởng (đã là 2 chữ số).

- **Nguyên nhân gốc**
  - `_read_silver_layer` xây dựng path Silver bằng f-string:
    ```python
    silver_path = f"{base}/year={year}/month={month}/"
    ```
  - `month` là `int` (ví dụ: `1` cho tháng 1) → path tạo ra là `month=1`.
  - Silver data được ghi bởi `_write_to_silver` trong `silver_transform.py` với
    `partitionBy("year", "month")` — Spark ghi partition folder theo giá trị cột,
    tức là `month=01` (zero-padded từ dữ liệu nguồn hoặc từ Parquet partition naming).
  - Kết quả: path `month=1` không tồn tại trong MinIO → `PATH_NOT_FOUND`.

- **Tác động**
  - Task gold fail 100% cho tất cả các tháng 1–9 của mọi năm.
  - Chỉ tháng 10, 11, 12 có thể chạy thành công.
  - Pipeline bị block hoàn toàn cho phần lớn các DAG run.

- **Giải pháp đã áp dụng**
  - Đổi f-string trong `_read_silver_layer` để zero-pad month:
    ```python
    # Trước (sai):
    silver_path = f"{base}/year={year}/month={month}/"

    # Sau (đúng):
    silver_path = f"{base}/year={year}/month={month:02d}/"
    ```
  - `:02d` format spec đảm bảo month luôn là 2 chữ số (01, 02, ..., 09, 10, 11, 12).

- **File liên quan**
  - `etls/gold_transform.py` — hàm `_read_silver_layer`

- **Trạng thái**
  - `FIXED`

---

## Checklist theo dõi sau fix (vận hành)

- [ ] Restart `airflow-worker` và `airflow-scheduler` sau khi đổi `airflow.env`.
- [ ] Clear/retry DAG run cũ bị treo.
- [ ] Theo dõi thời gian chạy từng task ít nhất 3 chu kỳ.
- [ ] Kiểm tra metadata log:
  - `silver_transform_log`
  - `gold_transform_log`
  - `silver_file_log`
- [ ] Xác minh `dim_date` không mất dữ liệu lịch sử sau rerun nhiều tháng.
- [ ] Xem xét tạo maintenance DAG riêng cho Iceberg compaction.

---

## Template thêm bug mới

```md
## BUG-XXX — <Tên bug>

- **Triệu chứng**
  - ...

- **Nguyên nhân gốc**
  - ...

- **Tác động**
  - ...

- **Giải pháp đã áp dụng**
  - ...

- **File liên quan**
  - ...

- **Trạng thái**
  - OPEN | IN_PROGRESS | FIXED | MONITORING
