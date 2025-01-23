import sys
import os
import pyodbc
from sqlalchemy import create_engine, insert, text
import pandas as pd

db = {
    'DRIVER': '{ODBC Driver 17 for SQL Server}',
    'SERVER': 'localhost',
    'DATABASE': 'airflow',
    'PORT': '1433',
    'UID': 'willian',
    'PWD': 'billpoker13!'
}

engine = create_engine(f"mssql+pymssql://{db['UID']}:{db['PWD']}@{db['SERVER']}:{db['PORT']}/{db['DATABASE']}")
con = engine.connect().execution_options(stream_results=True)

df = pd.read_sql("SELECT @@VERSION as versao", con)
print(df)

with engine.connect() as conn:
    result = conn.execute(text("insert into dbo.test(texto) values ('teste1')"))
    conn.commit()


