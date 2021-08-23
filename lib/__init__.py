"""Biblioteca com funções úteis para a manipulação de grandes volumes de dados com o PySpark.
"""
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import Window
import pandas as pd

def get_info_dataframe(df, columns, debug=True):
    """Obtém informações sobre as colunas de um dataframe `df`.
    """
    if not isinstance(columns, list): columns = [columns]
    tot = df.count()
    d = {col: {} for col in columns}
    for col in (columns if not debug else tqdm(columns)):
        d[col]['null'] = df.filter(F.col(col).isNull()).count()
        d[col]['notNull'] = tot - d[col]['null']
        d[col]['unique'] = df.select(col).distinct().count()
        d[col]['uniqueNotNull'] = df.select(col).filter(F.col(col).isNotNull()).distinct().count()
        d[col]['duplicates'] = tot - d[col]['unique']
        d[col]['duplicatesNotNull'] = d[col]['notNull'] - d[col]['uniqueNotNull']
    return pd.DataFrame(d).T
