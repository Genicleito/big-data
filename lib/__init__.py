"""Biblioteca com funções úteis para a manipulação de grandes volumes de dados com o PySpark.
"""
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import Window
import pandas as pd

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
