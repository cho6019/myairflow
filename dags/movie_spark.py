from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
        BranchPythonOperator
)

DAG_ID = "movie_spark"

with DAG(
    DAG_ID,
    default_args={
        "depends_on_past": True,
        "retries": 1,
        "retry_delay": timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=5,
    description="movie spark sbumit",
    schedule="10 10 * * *",
    start_date=datetime(2024, 8, 1),
    end_date=datetime(2024, 12, 31),
    catchup=True,
    tags=["spark", "sbumit", "movie"],
) as dag:
    SPARK_HOME="/home/ubuntu/app/spark-3.5.1-bin-hadoop3"
    SCRIPT_BASE="/home/ubuntu/code/myairflow/pyspark"
    META_PATH = "/home/ubuntu/data/movie_spark/meta"
    RAW_PATH = '/home/ubuntu/data/movie_after/dailyboxoffice'

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="none_failed")
    
    def check_exists_meta():
        import os
        if os.path.exists(f'{META_PATH}/_SUCCESS'):
            return append_meta.task_id   
        else:
            return create_meta.task_id

    # spark_submit = BashOperator(
    #     task_id='submit', 
    #     bash_command='$SPARK_HOME/bin/spark-submit $SCRIPT_BASE/py.py {{ ds_nodash }}',
    #     env={"SPARK_HOME": SPARK_HOME, "SCRIPT_BASE": SCRIPT_BASE}
    #     )
    
    exists_meta = BranchPythonOperator(
        task_id = 'exists.meta',
        python_callable=check_exists_meta
    )
    
    create_meta = BashOperator(
        task_id = 'create.meta',
        bash_command='$SPARK_HOME/bin/spark-submit $SCRIPT_BASE/movie_meta.py $RAW_PATH/dt={{ ds_nodash }} create $META_PATH',
        env={"SPARK_HOME": SPARK_HOME,
             "SCRIPT_BASE": SCRIPT_BASE,
             "META_PATH": META_PATH,
             "RAW_PATH": RAW_PATH}
    )
    
    append_meta = BashOperator(
        task_id = 'append.meta',
        bash_command='$SPARK_HOME/bin/spark-submit $SCRIPT_BASE/movie_meta.py $RAW_PATH/dt={{ ds_nodash }} append $META_PATH',
        env={"SPARK_HOME": SPARK_HOME,
             "SCRIPT_BASE": SCRIPT_BASE,
             "META_PATH": META_PATH,
             "RAW_PATH": RAW_PATH}
    )
    
    
    start >> exists_meta
    exists_meta >> [create_meta, append_meta] >> end