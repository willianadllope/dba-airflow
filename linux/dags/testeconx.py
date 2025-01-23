import sys
import os
import shutil
from time import time
from collections import namedtuple
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import urllib as ul
import snowflake as sf
from snowflake import connector

db = {
    'DRIVER': '{ODBC Driver 17 for SQL Server}',
    'SERVER': '192.168.0.35',
    'DATABASE': 'systax_app',
    'PORT': '1433',
    'UID': 'willian',
    'PWD': 'billpoker13!'
}


Consultas = namedtuple('Consultas',['consulta','tabela','limite'])

'''
props = ul.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                "SERVER=" + db['SERVER'] + ";"
                                "DATABASE=" + db['DATABASE'] + ";"
                                "uid="+db['UID']+";pwd="+db['PWD']+";"
                                "Encrypt=yes;TrustServerCertificate=yes;")
con = create_engine("mssql+pyodbc:///?odbc_connect={}".format(props))

print(con);

df = pd.read_sql("SELECT @@VERSION as versao", con)
print(df)
'''
engine = create_engine(f"mssql+pymssql://{db['UID']}:{db['PWD']}@{db['SERVER']}:{db['PORT']}/{db['DATABASE']}")
con = engine.connect().execution_options(stream_results=True)

df = pd.read_sql("SELECT @@VERSION as versao", con)
print(df)

