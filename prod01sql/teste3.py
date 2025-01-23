import pyodbc

SERVER = 'prod01sql.systax.com.br'
DATABASE = 'systax_app'
USERNAME = 'willian'
PASSWORD = 'billpoker13!'

connectionString = f'DRIVER={}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'
conn = pyodbc.connect(connectionString)
