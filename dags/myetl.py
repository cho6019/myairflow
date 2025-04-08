from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import PythonVirtualenvOperator
import pendulum
import requests
import os
from myairflow import DataManager as DM


local_tz = pendulum.timezone("Asia/Seoul")

    
# Directed Acyclic Graph
with DAG(
    "myetl",
    schedule="@hourly",
    # schedule="* * * * *",
    # start_date=datetime(2025, 3, 10)
    start_date=pendulum.datetime(2025, 3, 12, tz="Asia/Seoul")
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    make_data = BashOperator(task_id="make_data",
                             bash_command="""
                             bash ~/airflow/make_data.sh ~/data/{{ data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d/%H') }}
                             """)
    
    load_data = PythonVirtualenvOperator(task_id="load_data",
                                         python_callable = DM.data_load,
                                         op_args=["{{ data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d/%H') }}"],
                                         requirements=["git+https://github.com/cho6019/myairflow.git"]
                                         
                                         )
    
    agg_data = PythonVirtualenvOperator(task_id="load_data",
                                        python_callable = DM.data_agg,
                                        op_args=["{{ data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d/%H') }}"],
                                        requirements=["git+https://github.com/cho6019/myairflow.git"]
                                        )

    
    start >> make_data >> load_data >> agg_data >> end