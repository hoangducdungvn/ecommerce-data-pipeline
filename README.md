# E-Commerce Data Lakehouse & ETL Pipeline

![CI Status](https://github.com/hoangducdungvn/ecommerce-data-pipeline/actions/workflows/ci-pipeline.yml/badge.svg)

## Tổng quan dự án
Dự án xây dựng một luồng dữ liệu (Data Pipeline) tự động, áp dụng kiến trúc **Medallion (Bronze -> Silver -> Gold)** để xử lý hàng nghìn giao dịch thương mại điện tử. 

## Công nghệ sử dụng
* **Data Processing:** Apache Spark (PySpark)
* **Orchestration:** Apache Airflow
* **Database:** Microsoft SQL Server, PostgreSQL
* **Infrastructure:** Docker & Docker Compose
* **CI/CD:** GitHub Actions, Pytest

## Điểm nhấn kỹ thuật (Technical Highlights)
1. **Memory & Storage Optimization:** Áp dụng kỹ thuật `partitionBy` (theo Năm, Tháng, MarketId) khi ghi file Parquet để giải quyết bài toán Data Skewness và tối ưu hóa bộ nhớ truy vấn.
2. **Data Modeling:** Xây dựng mô hình Star Schema ở lớp Gold với `Fact_Order_Lines`, `Dim_Customer`, `Dim_Product` phục vụ cho hệ thống Recommendation.
3. **Traceability:** Lập lịch và theo dõi trực quan trạng thái pipeline hằng ngày qua Airflow UI.
4. **Automated Testing:** Tích hợp CI kiểm tra tính toàn vẹn của logic transform dữ liệu trước khi merge code.

## How to run
1. Clone repository này.
2. Cấp quyền: `echo -e "AIRFLOW_UID=$(id -u)" > .env`
3. Khởi động các dịch vụ: `docker-compose up -d --build`
4. Truy cập Airflow UI tại `http://localhost:8080` và bật DAG `ecommerce_daily_pipeline`.    