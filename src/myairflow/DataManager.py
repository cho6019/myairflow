import pandas as pd
from datetime import datetime, timedelta



def data_load(timestamp):
    dir_path = '~/data'
    data_c = pd.read_csv(f"{dir_path}{timestamp}/data.csv")
    data_p = data_c.to_parquet(f"{dir_path}{timestamp}/data.parquet", engine='pyarrow')
    return data_p

def data_agg(timestamp):
    dir_path = '~/data'
    data_a = pd.read_parquet(f"{dir_path}{timestamp}/data.parquet", engine='pyarrow')
    data_a = data_a.groupby(['name', 'value']).size().reset_index(name='count')
    data_a.to_csv(f"{dir_path}{timestamp}/agg.csv", index=False)