"""Biblioteca com funções úteis para a manipulação de grandes volumes de dados com o PySpark.
"""
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Window
import pandas as pd
import datetime

now = lambda: datetime.datetime.now()

def get_info_dataframe(df, columns=None):
    """Obtém informações sobre os IDs de um dataframe `df`.
    """
    import pandas as pd
    if not columns: columns = df.columns
    if not isinstance(columns, list): columns = [columns] 
    tot = df.count()
    new_df = pd.DataFrame()
    new_df = df.select(*(F.count(c).alias(c) for c in columns)).toPandas().rename(index={0: 'notNull'}).astype(int)
    new_df = new_df.append(pd.DataFrame({c: {'null': tot - new_df.loc['notNull', c]} for c in columns}))
    new_df = new_df.append(df.select(*(F.countDistinct(c).alias(c) for c in columns)).toPandas().rename(index={0: 'uniqueNotNull'}).astype(int))
    new_df = new_df.append(pd.DataFrame({c: {'unique': new_df.loc['uniqueNotNull', c] + (0 if new_df.loc['null', c] == 0 else 1)} for c in columns}))
    new_df = new_df.append(pd.DataFrame({c: {'duplicatesNotNull': new_df.loc['notNull', c] - new_df.loc['uniqueNotNull', c]} for c in columns}))
    new_df = new_df.append(pd.DataFrame({c: {'duplicates': tot - new_df.loc['unique', c]} for c in columns}))
    return new_df.T

def checkpoint(df, temp_path, repartition=None, storage_level=None, unpersist=True, retry_seconds=5, debug=True):
    # Write temp path
    temp_code = f"{now().strftime('%Y%m%d%H%M%S')}_checkpoint"
    path = f'{temp_path[:-1]}/{temp_code}' if temp_path.endswith('/') else f'{temp_path}/{temp_code}'
    # debug
    if debug: print(f'{now()}: writing temp files in `{path}`...')
    if debug: ts = now()
    
    if repartition:
        df.repartition(repartition).write.parquet(path, mode='overwrite')
    else:
        df.write.parquet(path, mode='overwrite')
    if debug: print(f'{now()}: Checkpoint execution time: {now() - ts}')
    if unpersist: df.unpersist()
    # Read and return
    try:
        return spark.read.parquet(path).persist(storage_level) if storage_level else spark.read.parquet(path)
    except:
        time.sleep(retry_seconds) # wait `retry_seconds` seconds
        return spark.read.parquet(path).persist(storage_level) if storage_level else spark.read.parquet(path)
    
def count_null_columns(df, columns=None):
    return df.withColumn(
        'count_nulls',
        sum([F.col(x).isNull().cast('int') for x in (columns if columns else df.columns)]
    )
