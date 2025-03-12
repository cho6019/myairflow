from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pendulum
import requests
import os


def generate_bash_commands(columns: list):
    cmds = []
    max_length = max(len(c) for c in columns)
    for c in columns:
        padding = " " * (max_length - len(c))
        cmds.append(f'echo "{c}{padding} : ====> {{{{ {c} }}}}"')
    return "\n".join(cmds)

def send_discord():
    WEBHOOK_ID = os.getenv('WEBHOOK_ID')
    WEBHOOK_TOKEN = os.getenv('WEBHOOK_TOKEN')
    WEBHOOK_URL = "https://discordapp.com/api/webhooks/1337288429733675008/L5yzKpqhuY0f-rIBPQYYNKcDjeaVsYrEUr-n8UTukV_yL_M7w_PmLfZ4_BguAn1D9I-h"
    data = { "content": "mkdir이 정상적으로 실행되지 않았습니다" }
    response = requests.post(WEBHOOK_URL, json=data)

local_tz = pendulum.timezone("Asia/Seoul")

def print_kwargs(dag, task, data_interval_start, **kwargs):
    ds = data_interval_start.in_tz('Asia/Seoul').format('YYYYMMDDHH')
    msg = f"{dag.dag_id} {task.task_id} {ds} OK / CHO "
    from myairflow.send_notify import send_noti
    send_noti(msg)
    
# Directed Acyclic Graph
with DAG(
    "seoul",
    schedule="@hourly",
    # schedule="* * * * *",
    # start_date=datetime(2025, 3, 10)
    start_date=pendulum.datetime(2025, 3, 11, tz="Asia/Seoul")
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    send_notification = PythonOperator(
            task_id="send_notification",
            python_callable=print_kwargs
        )
    
    
    columns_b1 = [
    "data_interval_start", "data_interval_end", "logical_date", "ds", "ds_nodash",
    # "exception",
    "ts", "ts_nodash_with_tz", "ts_nodash", "prev_data_interval_start_success",
    "prev_data_interval_end_success", "prev_start_date_success", "prev_end_date_success",
    "inlets", "inlet_events", "outlets", "outlet_events", "dag", "task", "macros",
    "task_instance", "ti", "params", "var.value", "var.json", "conn", "task_instance_key_str",
    "run_id", "dag_run", "map_index_template", "expanded_ti_count", "triggering_dataset_events"
    ]
    cmds_b1 = generate_bash_commands(columns_b1)
   
    b1 = BashOperator(
        task_id="b_1", 
        bash_command=f"""
            echo "date ====================> `date`"
            {cmds_b1}
        """)
    
    cmds_b2_1 = [
    "execution_date",
    "next_execution_date","next_ds","next_ds_nodash",
    "prev_execution_date","prev_ds","prev_ds_nodash",
    "yesterday_ds","yesterday_ds_nodash",
    "tomorrow_ds", "tomorrow_ds_nodash",
    "prev_execution_date_success",
    "conf"
    ]
    
    cmds_b2_1 = generate_bash_commands(cmds_b2_1)
   
    b2_1 = BashOperator(task_id="b_2_1", 
                        bash_command=f"""
                        {cmds_b2_1}
                        """)
    
    b2_2 = BashOperator(task_id="b_2_2", 
                        bash_command="""
                        echo "data_interval_start : {{ data_interval_start.in_tz('Asia/Seoul') }}"
                        """)
    
    mkdir = BashOperator(task_id="mkdir",
                         bash_command="""
                         echo "data_interval_start : {{ data_interval_start.in_tz('Asia/Seoul') }}"
                         mkdir -p ~/data/seoul/{{ execution_date.strftime('%Y/%m/%d/%H') }}
                         """)
    
    # send_notification = BashOperator(task_id="send_notification",
    #                                  bash_command="""
    #                                  echo "mkdir_failed_info : {{ ds_nodash }}"
    #                                  python ~/code/myairflow/dags/alert.py
    #                                  """,
    #                                  trigger_rule="one_failed",)
    
    # send_notification = PythonOperator(task_id="send_notification",
    #                                  python_callable=send_discord,
    #                                  trigger_rule="one_failed",)
    
    
    start >> b1 >> [b2_1, b2_2] >> mkdir >> end
    mkdir >> send_notification