import config
from time import time
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd

db = config.pg_config_prod01

engine = create_engine(f"mssql+pymssql://{db['UID']}:{db['PWD']}@{db['SERVER']}:{db['PORT']}/{db['DATABASE']}")
con = engine.connect().execution_options(stream_results=True)

def export_query_to_parquet(sql, fileprefix, limit):
    """ export data from given table to parquet """
    time_step = time()
    print("Let's export", fileprefix)
    lines = 0
    for i, df in enumerate(pd.read_sql(sql, con, chunksize=limit)):
		# by chunk of 1M rows if needed
        t_step = time()
        current_date = datetime.now()
        formatted_previous_day = current_date.strftime("%Y%m%d%H%M%S")
        file_name = fileprefix+'/'+fileprefix + str(i) + formatted_previous_day+'.parquet'   
        df.to_parquet(file_name, index=False)
        lines += df.shape[0]
        print('  ', file_name, df.shape[0], f'lines ({round(time() - t_step, 2)}s)')
    print("  ", lines, f"lines exported {'' if i==0 else f' in {i} files'} ({round(time() - time_step, 2)}s)")


# clientes id integer, flag_setor integer, situacao integer, deletado integer, flag_retorno_ipi integer
# tributos_internos_cache_config  flag_homolog int,ent_sai varchar;
# ts_atual ts bigint datahora varchar(20)
# agrupamento_produtos cod_prod varchar(250), origem_produto int, id_cli int
# custom_prod ncm varchar(8), ex_tipi varchar(10), 
def main():
    sql_queries = [
        """
        SELECT cod_prod, origem_produto, id_cli
        FROM systax_app.dbo.agrupamento_produtos (NOLOCK)
		WHERE hierarquia = 1 AND (vigencia_ate IS NULL or vigencia_ate >= getdate());        
        """,
        """
        SELECT ponteiro_tabelao as ts, convert(varchar(20),data,120)  
        FROM systax_app.dbo. ts_diario (NOLOCK)
        WHERE servidor = 'win04' 
        ORDER BY ts DESC
        """,
        """
        SELECT id, flag_setor, situacao, deletado, flag_retorno_ipi 
        FROM systax_app.dbo.clientes (NOLOCK);
        """,
        """
        SELECT a.id, a.id_cli, a.cod_prod, a.origem_produto, a.cean14, a.status, convert(bigint, a.ts) as ts, a.ncm, a.ex_tipi 
        FROM systax_app.dbo.custom_prod a(NOLOCK)
        WHERE exists(SELECT TOP 1 1 FROM systax_app.snowflake.enviar_custom_prod ecp (NOLOCK) WHERE ecp.id = a.id);
        """,
        """
        SELECT id, cean14_padrao, cean14_vinculado, convert(bigint,ts) as ts 
        FROM systax_app.dbo.cean_relacionado a(NOLOCK)
        WHERE exists(SELECT TOP 1 1 FROM systax_app.snowflake.enviar_cean_relacionado ecp (NOLOCK) WHERE ecp.id = a.id);
        """,
        """
        SELECT id, id_cliente, grupo_cache, id_config_setor, id_config_icms, id_config_ipi, id_config_pis, id_config_cofins,id_config_antecipacao, origem_produto_alternativo, flag_homolog, ent_sai
        FROM systax_app.dbo.tributos_internos_cache_config a(NOLOCK)
        WHERE exists(SELECT TOP 1 1 FROM systax_app.snowflake.enviar_tributos_internos_cache_config ecp (NOLOCK) WHERE ecp.id = a.id);
        """,
        """
        SELECT id, id_grupo, id_trib_inter_config, id_cli, convert(bigint,ts) as ts 
        FROM systax_app.dbo.grupo_config a(NOLOCK)
        WHERE exists(SELECT TOP 1 1 FROM systax_app.snowflake.enviar_grupo_config ecp (NOLOCK) WHERE ecp.id = a.id);
        """,
        """
        SELECT id, id_grupo, id_custom_prod, id_cli, convert(bigint,ts) as ts 
        FROM systax_app.dbo.grupo_custom_prod a(NOLOCK)
        WHERE exists(SELECT TOP 1 1 FROM systax_app.snowflake.enviar_grupo_custom_prod ecp (NOLOCK) WHERE ecp.id = a.id);
        """,
        """
        SELECT id, id_cache_rep, hash_retorno_erp, id_config, cod_produto, origem_produto, cst, convert(bigint, ts) as ts, vigencia_de, vigencia_ate
        FROM systax_app.dbo.tributos_internos_cache a(NOLOCK)
        WHERE exists(SELECT TOP 1 1 FROM systax_app.snowflake.enviar_tributos_internos_cache ecp (NOLOCK) WHERE ecp.id = a.id);
        """,
        """
        SELECT id, id_cache_rep, hash_retorno_erp, convert(bigint,ts) as ts, vigencia_de, vigencia_ate
        FROM systax_app.dbo.tributos_internos_cache_st a (NOLOCK)
        WHERE exists(SELECT TOP 1 1 FROM systax_app.snowflake.enviar_tributos_internos_cache_st ecp (NOLOCK) WHERE ecp.id = a.id);
        """
    ]

    file_prefixes = ['agrupamento_produtos',
                     'ts_diario',
                     'clientes',
                     'custom_prod',
                     'cean_relacionado', 
                     'tributos_internos_cache_config',
                     'grupo_config',
                     'grupo_custom_prod',
                     'tributos_internos_cache',
                     'tributos_internos_cache_st',
                     ]
    
    path_prefixes = [ 'prod01sql_parquet/tabelao/agrupamento_produtos',
                      'prod01sql_parquet/tabelao/ts_diario',
                      'prod01sql_parquet/tabelao/clientes',
                      'prod01sql_parquet/tabelao/custom_prod',
                      'prod01sql_parquet/tabelao/cean_relacionado', 
                      'prod01sql_parquet/tabelao/tributos_internos_cache_config',
                      'prod01sql_parquet/tabelao/grupo_config',
                      'prod01sql_parquet/tabelao/grupo_custom_prod',
                      'prod01sql_parquet/tabelao/tributos_internos_cache',
                      'prod01sql_parquet/tabelao/tributos_internos_cache_st'
                     ]
    chunk_sizes = [ 100000,
                    100000,
                    100000,
                    1000000, 
                    1000000,
                    1000000,
                    1000000,
                    10000000,
                    250000,
                    250000
                ]

    for sql_query, file_prefix, path_prefix, limit in zip(sql_queries, file_prefixes, path_prefixes, chunk_sizes):
        export_query_to_parquet(sql_query, file_prefix, limit)

if __name__ == "__main__":
    main()
