from collections import namedtuple
import sys


Consultas = namedtuple('Consultas',['consulta','tabela','arquivo','limite'])

sql_queries = [
    Consultas(consulta="""
                SELECT cod_prod, origem_produto, id_cli
                FROM systax_app.dbo.agrupamento_produtos (NOLOCK)
                WHERE hierarquia = 1 AND (vigencia_ate IS NULL or vigencia_ate >= getdate());
                """, 
                tabela='agrupamento_produtos',
                arquivo='prod01sql_parquet/tabelao/agrupamento_produtos',
                limite=100000
            ),
    Consultas(consulta="""
                SELECT ponteiro_tabelao as ts, convert(varchar(20),data,120)  
                    FROM systax_app.dbo.ts_diario (NOLOCK)
                    WHERE servidor = 'win04' 
                    ORDER BY ts DESC
                """, 
                tabela='ts_diario',
                arquivo='prod01sql_parquet/tabelao/ts_diario',
                limite=100000
            ),
    Consultas(consulta="""
                SELECT id, flag_setor, situacao, deletado, flag_retorno_ipi 
                FROM systax_app.dbo.clientes (NOLOCK);
                """, 
                tabela='clientes',
                arquivo='prod01sql_parquet/tabelao/clientes',
                limite=100000
            ),
    Consultas(consulta="""
            SELECT a.id, a.id_cli, a.cod_prod, a.origem_produto, a.cean14, a.status, convert(bigint, a.ts) as ts, a.ncm, a.ex_tipi 
                FROM systax_app.dbo.custom_prod a(NOLOCK)
                WHERE exists(SELECT TOP 1 1 FROM systax_app.snowflake.enviar_custom_prod ecp (NOLOCK) WHERE ecp.id = a.id);
                """,
                tabela='custom_prod',
                arquivo='prod01sql_parquet/tabelao/custom_prod',
                limite=1000000),
    Consultas(consulta="""
            SELECT id, cean14_padrao, cean14_vinculado, convert(bigint,ts) as ts 
                FROM systax_app.dbo.cean_relacionado a(NOLOCK)
                WHERE exists(SELECT TOP 1 1 FROM systax_app.snowflake.enviar_cean_relacionado ecp (NOLOCK) WHERE ecp.id = a.id);
                """,
                tabela='cean_relacionado',
                arquivo='prod01sql_parquet/tabelao/cean_relacionado',
                limite=1000000),
    Consultas(consulta="""
            SELECT id, id_cliente, grupo_cache, id_config_setor, id_config_icms, id_config_ipi, id_config_pis, id_config_cofins,id_config_antecipacao, origem_produto_alternativo, flag_homolog, ent_sai
                FROM systax_app.dbo.tributos_internos_cache_config a(NOLOCK)
                WHERE exists(SELECT TOP 1 1 FROM systax_app.snowflake.enviar_tributos_internos_cache_config ecp (NOLOCK) WHERE ecp.id = a.id);
                """,
                tabela='tributos_internos_cache_config',
                arquivo='prod01sql_parquet/tabelao/tributos_internos_cache_config',
                limite=100000),
    Consultas(consulta="""
            SELECT id, id_grupo, id_trib_inter_config, id_cli, convert(bigint,ts) as ts 
                FROM systax_app.dbo.grupo_config a(NOLOCK)
                WHERE exists(SELECT TOP 1 1 FROM systax_app.snowflake.enviar_grupo_config ecp (NOLOCK) WHERE ecp.id = a.id);
                """,
                tabela='grupo_config',
                arquivo='prod01sql_parquet/tabelao/grupo_config',
                limite=1000000),
    Consultas(consulta="""
            SELECT id, id_grupo, id_custom_prod, id_cli, convert(bigint,ts) as ts 
                FROM systax_app.dbo.grupo_custom_prod a(NOLOCK)
                WHERE exists(SELECT TOP 1 1 FROM systax_app.snowflake.enviar_grupo_custom_prod ecp (NOLOCK) WHERE ecp.id = a.id);
                """,
                tabela='grupo_custom_prod',
                arquivo='prod01sql_parquet/tabelao/grupo_custom_prod',
                limite=10000000),
    Consultas(consulta="""
            SELECT id, id_cache_rep, hash_retorno_erp, id_config, cod_produto, origem_produto, cst, convert(bigint, ts) as ts, vigencia_de, vigencia_ate
                FROM systax_app.dbo.tributos_internos_cache a(NOLOCK)
                WHERE exists(SELECT TOP 1 1 FROM systax_app.snowflake.enviar_tributos_internos_cache ecp (NOLOCK) WHERE ecp.id = a.id);
                """,
                tabela='tributos_internos_cache',
                arquivo='prod01sql_parquet/tabelao/tributos_internos_cache',
                limite=2500000),
    Consultas(consulta="""
                SELECT id, id_cache_rep, hash_retorno_erp, convert(bigint,ts) as ts, vigencia_de, vigencia_ate
                FROM systax_app.dbo.tributos_internos_cache_st a (NOLOCK)
                WHERE exists(SELECT TOP 1 1 FROM systax_app.snowflake.enviar_tributos_internos_cache_st ecp (NOLOCK) WHERE ecp.id = a.id);
                """,
                tabela='tributos_internos_cache_st',
                arquivo='prod01sql_parquet/tabelao/tributos_internos_cache_st',
                limite=2500000),
    Consultas(consulta="""
                SELECT id FROM systax_app.snowflake.apagar_custom_prod;
                """,
                tabela='apagar_custom_prod',
                arquivo='prod01sql_parquet/tabelao/apagar_custom_prod',
                limite=1000000),
    Consultas(consulta="""
                SELECT id FROM systax_app.snowflake.apagar_cean_relacionado;
                """,
                tabela='apagar_cean_relacionado',
                arquivo='prod01sql_parquet/tabelao/apagar_cean_relacionado',
                limite=1000000),
    Consultas(consulta="""
                SELECT id FROM systax_app.snowflake.apagar_grupo_custom_prod;
                """,
                tabela='apagar_grupo_custom_prod',
                arquivo='prod01sql_parquet/tabelao/apagar_grupo_custom_prod',
                limite=1000000),
    Consultas(consulta="""
                SELECT id FROM systax_app.snowflake.apagar_grupo_config;
                """,
                tabela='apagar_grupo_config',
                arquivo='prod01sql_parquet/tabelao/apagar_grupo_config',
                limite=1000000),
    Consultas(consulta="""
                SELECT id FROM systax_app.snowflake.apagar_grupos;
                """,
                tabela='apagar_grupos',
                arquivo='prod01sql_parquet/tabelao/apagar_grupos',
                limite=1000000),
    Consultas(consulta="""
                SELECT id FROM systax_app.snowflake.apagar_clientes;
                """,
                tabela='apagar_clientes',
                arquivo='prod01sql_parquet/tabelao/apagar_clientes',
                limite=1000000),
    Consultas(consulta="""
                SELECT id FROM systax_app.snowflake.apagar_tributos_internos_cache;
                """,
                tabela='apagar_tributos_internos_cache',
                arquivo='prod01sql_parquet/tabelao/apagar_tributos_internos_cache',
                limite=1000000),
    Consultas(consulta="""
                SELECT id FROM systax_app.snowflake.apagar_tributos_internos_cache_st;
                """,
                tabela='apagar_tributos_internos_cache_st',
                arquivo='prod01sql_parquet/tabelao/apagar_tributos_internos_cache_st',
                limite=1000000),
    Consultas(consulta="""
                SELECT id FROM systax_app.snowflake.apagar_tributos_internos_cache_config;
                """,
                tabela='apagar_tributos_internos_cache_config',
                arquivo='prod01sql_parquet/tabelao/apagar_tributos_internos_cache_config',
                limite=1000000)
]

def main():
    find = sys.argv[1]
    indexAtual = 0
    index = 0
    print(len(sys.argv))
    for consulta in sql_queries:
        if (consulta.tabela==find):
            index = indexAtual
        indexAtual = indexAtual + 1
    
    #for consulta in sql_queries:
    #        print(consulta.consulta + '=>' + consulta.tabela)

    print(sql_queries[index])

if __name__ == "__main__":
    main()

    
