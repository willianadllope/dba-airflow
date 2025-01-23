from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from sqlalchemy.orm import sessionmaker

# # create connection object wih the
# # Bridg provided snowflake connection details provided
sf_engine = create_engine(URL(
         account='ZK69750',
         user='willian',
         password='BillPoker13',
         database='DB_TABELAO',
         schema='DBO',
         warehouse='COMPUTE_WH',
         role='ACCOUNTADMIN',
))
Session = sessionmaker(bind=sf_engine, autocommit=False)
session = Session()
comando='select id from DB_TABELAO.DBO.clientes;'
results = session.execute(comando)
print(results)