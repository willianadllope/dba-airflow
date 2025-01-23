import snowflake as sf
from snowflake import connector
import sys
import glob
from time import time
from datetime import datetime


if len(sys.argv)  >= 2:
    tabela = sys.argv[1]
if len(sys.argv)  >= 3:
    tipoExecucao = sys.argv[2] # FULL | INCREMENTAL      
print('tabela: ' +tabela)
print('tipo: ' +tipoExecucao)

print('iniciou')
conn = sf.connector.connect(
user='SYSTAXSNOW24',
    password="Dkjj$@8$g@hgsgj!!",
    account='DJDYJNY-ZK69750',
    warehouse='COMPUTE_WH',
    database='DB_TABELAO',
    schema=tipoExecucao
)
print('conectou')

# populate the file_name and stage_name with the arguments
file_name =tipoExecucao+'\\'+tabela+'\*'
#sys.argv[1]
stage_name = 'DB_TABELAO.'+tipoExecucao+'.STAGE_FILES_TABELAO/tabelao/'+tabela+'/'
#sys.argv[2]
print('stage: ' +stage_name)

cs = conn.cursor()
print('cursor aberto')
#try:
#    cs.execute(f"PUT file://{file_name} @{stage_name} auto_compress=false")
#    print(cs.fetchall())
#finally:
#    cs.close()

# parquet_prod01sql stage_files_snowflake_tabelao COPY INTO dbo.interno_cean_relacionado from @stage_files_snowflake_tabelao/tabelao/cean_relacionado/ FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' ) MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE

arquivos = [f for f in glob.glob(tipoExecucao+'\\'+tabela+'\*.parquet')]
arquivos.sort()
try:
    for arq in arquivos:
        print('INICIO: '+datetime.now().strftime("%Y-%m-%d %H %M %S"))
        print(arq)
        cs.execute(f"PUT file://{arq} @{stage_name} auto_compress=false")
        print('FIM: '+datetime.now().strftime("%Y-%m-%d %H %M %S"))
        print("----------------")
finally:
    cs.close()        
print('cursor fechado')    