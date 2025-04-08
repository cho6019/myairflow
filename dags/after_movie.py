from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
        PythonVirtualenvOperator,
)
from airflow.sensors.filesystem import FileSensor
from pprint import pprint

with DAG(
    'after_movie',
    default_args={
        'depends_on_past': True,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=5,
    description='movie',
    schedule="10 10 * * *",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 3),
    catchup=True,
    tags=['api', 'movie', 'sensor'],
) as dag:
    REQUIREMENTS = ["git+https://github.com/cho6019/movie.git@main"]
    #"git+https://github.com/ppabam/movie.git@0.4.0",
    BASE_DIR = "/home/ubuntu/data/movie_after"

    
    start = EmptyOperator(task_id='start')
    
    check_done = FileSensor(task_id="check.done",
                            filepath="/home/ubuntu/data/movies/done/dailyboxoffice/{{ds_nodash}}/_DONE",
                            fs_conn_id="done_flag",
                            poke_interval=180,  # 3분마다 체크
                            timeout=3600,  # 1시간 후 타임아웃
                            mode="reschedule",)
    
    # def combine_unique_parquet(base_path):
    #     import pandas as pd
    #     import os
    #     combined_df = pd.DataFrame()
    #     parquet_files = [f for f in os.listdir(base_path) if f.endswith(".parquet")]
    
    #     for file in parquet_files:
    #         file_path = os.path.join(base_path, file)
    #         df = pd.read_parquet(file_path)
    #         combined_df = pd.concat([combined_df, df], ignore_index=True)
    
    #      # 중복 제거
    #     combined_df = combined_df.drop_duplicates()
    #     return combined_df
    
    def fn_gen_meta(**kwargs):
        import json
        import pandas as pd
        import os
        from movie.api.call import combine_unique_parquet
        base_path = kwargs["base_path"]
        ds_nodash = kwargs["ds_nodash"]
        print(json.dumps(kwargs, indent=4, ensure_ascii=False))
        # TODO f"{base_path}/meta/meta.parquet -> 경로로 저장
        try:
            combine = pd.read_parquet(f"{base_path}/meta/meta.parquet")
            add = combine_unique_parquet(f"/home/ubuntu/data/movies/dailyboxoffice/dt={ds_nodash}")
            combine = combine.combine_first(add)
            combine = combine.drop_duplicates(subset=["movieCd"], keep="last")
            combine.to_parquet(f"{base_path}/meta/meta.parquet", index=False)
        except FileNotFoundError:
            combine = combine_unique_parquet(f"/home/ubuntu/data/movies/dailyboxoffice/dt={ds_nodash}")
            combine = combine.drop_duplicates(subset=["movieCd"], keep="last")
            combine.to_parquet(f"{base_path}/meta/meta.parquet", index=False)
        

    gen_meta = PythonVirtualenvOperator(
        task_id="gen.meta",
        python_callable=fn_gen_meta,
        requirements=REQUIREMENTS,
        system_site_packages=False,
        op_kwargs={"base_path": BASE_DIR,
                   "ds_nodash": "{{ ds_nodash }}"
                   },
    )

    def fn_gen_movie(base_path, ds_nodash, **kwargs):
        import json
        import pandas as pd
        from movie.api.call import save_df, fill_unique_ranking
        print(json.dumps(kwargs, indent=4, ensure_ascii=False))
        print(f"base_path: {base_path}")
        # TODO -> f"{base_path}/dailyboxoffice/ 생성
        # movie airflow 의 merge.data 와 같은 동작 ( meta.parquet 사용 )
        # 파티션은 dt, multiMovieYn, repNationCd
        PATH = f"{base_path}/meta/meta.parquet"
        load_df = pd.read_parquet(PATH)
        df = fill_unique_ranking(load_df, ds_nodash)
        save_path = save_df(df, f"{base_path}/dailyboxoffice/", ['dt', 'multiMovieYn', 'repNationCd'])
        
        print("::group::movie df merge save...")
        print("save_path--->" + save_path)
        print("ds_nodash--->" + ds_nodash)
        print("::endgroup::")

    gen_movie = PythonVirtualenvOperator(
        task_id="gen.movie",
        python_callable=fn_gen_movie,
        requirements=REQUIREMENTS,
        system_site_packages=False,
        op_kwargs={"base_path": BASE_DIR,
                   "ds_nodash": "{{ ds_nodash }}"},
    )

    make_done = BashOperator(
        task_id="make.done",
        bash_command="""
        DONE_BASE=$BASE_DIR/done
        echo $DONE_BASE
        mkdir -p $DONE_BASE/{{ ds_nodash }}
        touch $DONE_BASE/{{ ds_nodash }}/_DONE
        """,
        env={'BASE_DIR':BASE_DIR},
        append_env = True
    )

    
    end = EmptyOperator(task_id='end')
    
    start >> check_done >> gen_meta >> gen_movie >> make_done >> end