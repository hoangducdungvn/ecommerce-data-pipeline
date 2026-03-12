from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Cấu hình mặc định cho luồng chạy
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False, # Trong thực tế có thể set True để gửi cảnh báo
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Khởi tạo DAG (Directed Acyclic Graph)
with DAG(
    'ecommerce_daily_pipeline',
    default_args=default_args,
    description='Luồng ETL xử lý dữ liệu E-commerce hằng ngày',
    schedule_interval=timedelta(days=1), # Lập lịch chạy mỗi ngày 1 lần
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['ecommerce', 'spark', 'etl'],
) as dag:

    # Task: Chạy file Spark thông qua Bash command
    run_spark_job = BashOperator(
        task_id='run_pyspark_etl',
        bash_command='python /opt/airflow/dags/etl_job.py', # Sửa đúng chữ dags ở đây
    )

    # Nếu có nhiều task, bạn có thể thiết lập thứ tự chạy tại đây
    # Ví dụ: task_1 >> run_spark_job >> task_3
    run_spark_job