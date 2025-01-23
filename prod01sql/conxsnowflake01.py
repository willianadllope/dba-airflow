import snowflake as sf
from snowflake import connector
import sys
print('iniciou')
conn = sf.connector.connect(
    user='SYSTAXSNOW24',
    password="Dkjj$@8$g@hgsgj!!",
    account='DJDYJNY-ZK69750',
    warehouse='COMPUTE_WH',
    database='DB_TABELAO',
    schema='DBO'
)
print('conectou')

cs = conn.cursor()
print('conectou 2')

results = cs.execute('select current_version()').fetchone()
print(results[0])

comando='select count(1) as total from DB_TABELAO.DBO.clientes;'
results = cs.execute(comando).fetchone()
print(results)


comando='execute task full.task_teste_chamada01;'
results = cs.execute(comando).fetchone()
print(results)


cs.close()

print('fechou')
