from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
#from airflow.operators.python import (
        #BranchPythonOperator)

DAG_ID = "wiki_spark"

with DAG(
    DAG_ID,
    default_args={
        "depends_on_past": True,
        "retries": 1,
        "retry_delay": timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=5,
    description="wiki spark submit",
    schedule="10 10 * * *",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 31),
    catchup=True,
    tags=["spark", "submit", "wiki"],
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="all_done")
    
    ### 혹시 몰라서 코드는 킵해둠
    # def check_exists_meta():
    #     import os
    #     if os.path.exists(f'{RAW_BASE}/_SUCCESS'):
    #         return append_meta.task_id   
    #     else:
    #         return save_parquet.task_id
    
    # exists_meta = BranchPythonOperator(
    #     task_id="exists.meta",
    #     python_callable=check_exists_meta
    # )
    
    # SPARK_HOME= "/home/sgcho0907/app/spark-3.5.1-bin-hadoop3" # GCP
    # PY_PATH= "/home/sgcho/code/test/wiki_save_parquet.py" # LOCAL
    
    save_parquet = BashOperator(
        task_id='save.parquet',
        bash_command="""
            echo "DT=====> {{ ds_nodash }}"
            
            ssh -i ~/.ssh/gcp_key jademin2033@34.64.51.231 \
            "/home/jademin2033/code/test/run.sh {{ ds }}"
            
            # 에러 처리: Spark 작업 실패 시 에러 코드 반환 및 로그 출력
            if [ $? -ne 0 ]; then
                echo "Spark job failed!"
                exit 1
            fi
        """,        
        env={
            # "SPARK_HOME": SPARK_HOME, 
            # "PY_PATH": PY_PATH,
            }
        )
    
    start >> save_parquet >> end