import sys
import os
import shutil
import pyodbc
from time import time
from collections import namedtuple
from sqlalchemy import create_engine, text
from datetime import datetime
import pandas as pd
import urllib as ul
import snowflake as sf
from snowflake import connector

db = {
    'DRIVER': '{ODBC Driver 17 for SQL Server}',
    'SERVER': '192.168.0.35',
    'DATABASE': 'airflow',
    'PORT': '1433',
    'UID': 'willian',
    'PWD': 'billpoker13!'
}


engine = create_engine(f"mssql+pymssql://{db['UID']}:{db['PWD']}@{db['SERVER']}:{db['PORT']}/{db['DATABASE']}")


#with engine.connect() as conn:
#    result = conn.execute(text("insert into dbo.test(texto) values ('teste B 1')"))
#    conn.commit()
conx = engine.raw_connection()
cursor = conx.cursor()
cursor.execute("dbo.pr_insert 'teste04b 2'")
conx.commit()
cursor.close()
