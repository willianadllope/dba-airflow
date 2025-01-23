import config
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

db = config.pg_config_prod01

Consultas = namedtuple('Consultas',['consulta','tabela','limite'])

#engine = create_engine(f"mssql+pymssql://{db['UID']}:{db['PWD']}@{db['SERVER']}:{db['PORT']}/{db['DATABASE']}")
#con = engine.connect().execution_options(stream_results=True)

props = ul.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                "SERVER=" + db['SERVER'] + ";"
                                "DATABASE=" + db['DATABASE'] + ";"
                                "uid="+db['UID']+";pwd="+db['PWD']+";"
                                "Encrypt=yes;TrustServerCertificate=yes;")
con = create_engine("mssql+pyodbc:///?odbc_connect={}".format(props))

connSnow = sf.connector.connect(
user='SYSTAXSNOW24',
    password="Dkjj$@8$g@hgsgj!!",
    account='DJDYJNY-ZK69750',
    warehouse='COMPUTE_WH',
    database='DB_TABELAO',
    schema='DBO'
)

def export_query_to_parquet(sql,tipo, fileprefix, limit, strposicao='001'):
    """ export data from given table to parquet """
    time_step = time()
    print("Let's export", fileprefix)
    lines = 0
    for i, df in enumerate(pd.read_sql(sql, con, chunksize=limit)):
		# by chunk of 1M rows if needed
        t_step = time()
        current_date = datetime.now()
        formatted_previous_day = current_date.strftime("%Y%m%d%H%M%S")
        file_name = tipo+'/'+fileprefix+'/'+fileprefix + '_'+strposicao + '_'+str(i) +'_'+ formatted_previous_day+'.parquet'   
        df.to_parquet(file_name, index=False)
        lines += df.shape[0]
        print('  ', file_name, df.shape[0], f'lines ({round(time() - t_step, 2)}s)')
    print("  ", lines, f"lines exported {'' if i==0 else f' in {i} files'} ({round(time() - time_step, 2)}s)")

def delete_files_directory(tipo, diretorio):
    # Specify the path of the directory to be deleted
    directory_path = 'P:\\tabelao\\'+tipo+'\\'+diretorio
    # Check if the directory exists before attempting to delete it
    if os.path.exists(directory_path):
        shutil.rmtree(directory_path)
        print(f"The directory {directory_path} has been deleted.")
    else:
        print(f"The directory {directory_path} does not exist.")     
    os.makedirs(directory_path)

def send_parquet_snowflake(tipo, tabela):
    # populate the file_name and stage_name with the arguments
    file_name = tipo+'\\'+tabela+'\*'
    STAGE_SCHEMA = 'FULL'
    if(tipo=='INCREMENTAL'):
        STAGE_SCHEMA = 'INCREMENTAL'
    stage_name = 'DB_TABELAO.'+STAGE_SCHEMA+'.STAGE_FILES_TABELAO/tabelao/'+tabela+'/'
    cs = connSnow.cursor()
    print('Enviando '+tabela)
    try:
        cs.execute(f"PUT file://{file_name} @{stage_name} auto_compress=false")
        print(cs.fetchall())
    finally:
        cs.close()
    print('Enviado '+tabela)

sql_queries = [
        Consultas(consulta="""
                  SELECT cod_prod, origem_produto, id_cli
                    FROM systax_app.dbo.agrupamento_produtos (NOLOCK)
		            WHERE hierarquia = 1 AND (vigencia_ate IS NULL or vigencia_ate >= getdate());
                  """, 
                  tabela='agrupamento_produtos',
                  limite=100000
                ),
        Consultas(consulta="""
                  SELECT ponteiro_tabelao as ts, convert(varchar(20),data,120) as datahora  
                        FROM systax_app.dbo.ts_diario (NOLOCK)
                        WHERE servidor = 'win04' 
                        ORDER BY ponteiro_tabelao DESC
                  """, 
                  tabela='ts_diario',
                  limite=100000
                ),
        Consultas(consulta="""
                  SELECT id, flag_setor, situacao, deletado, flag_retorno_ipi 
                    FROM systax_app.dbo.clientes (NOLOCK);
                  """, 
                  tabela='clientes',
                  limite=100000
                ),
        Consultas(consulta="""
                SELECT id,
                        id_cli,
                        cod_prod,
                        id_prod,
                        cean,
                        descricao,
                        ex_prod,
                        ex_benef,
                        ncm,
                        ncm_sugerida,
                        ncm_duvida,
                        status_integracao,
                        status,
                        cadastro_ok,
                        convert(varchar(10),vigente_de,120) as vigencia_de,
                        convert(varchar(10),vigente_ate,120) as vigencia_ate,
                        sem_produto,
                        ex_tipi,
                        id_grupo,
                        lista_id_tipo,
                        id_marca,
                        id_embalagem,
                        qtd,
                        pred_aliq,
                        classe_bebida,
                        controle,
                        convert(bigint,ts) as ts,
                        prod_padrao,
                        convert(varchar(10),dt_criacao,120) as dt_criacao,
                        questionamento,
                        origem_produto,
                        cean_original,
                        quest_config,
                        cean_revisado,
                        status_med,
                        ean_lista,
                        medicamento_lista,
                        medicamento_tipo,
                        flag_vinculacao_especial,
                        revisao_info,
                        cadastro_fiscal_ok,
                        convert(varchar(10),dh_entrega_cadastro_fiscal,120) as dh_entrega_cadastro_fiscal,
                        ncm_ex_sugestao,
                        flag_base_nao_usar,
                        flag_medicamento,
                        cean14_med,
                        tipo_vinculacao,
                        flag_fora_agrupamento_prod,
                        flag_multiplicacao_origem,
                        cean14 
                    FROM systax_app.dbo.custom_prod a(NOLOCK)
                    WHERE exists(SELECT TOP 1 1 FROM systax_app.[SNOWFLAKE].enviar_custom_prod ecp (NOLOCK) WHERE ecp.id = a.id);
                  """,
                  tabela='custom_prod',
                  limite=1000000),
        Consultas(consulta="""
                SELECT id, cean14_padrao, cean14_vinculado, convert(bigint,ts) as ts 
                    FROM systax_app.dbo.cean_relacionado a(NOLOCK)
                    WHERE exists(SELECT TOP 1 1 FROM systax_app.[SNOWFLAKE].enviar_cean_relacionado ecp (NOLOCK) WHERE ecp.id = a.id);
                  """,
                  tabela='cean_relacionado',
                  limite=1000000),
        Consultas(consulta="""
                SELECT id,
                        id_cliente,
                        apelido,
                        grupo_cache,
                        cod_nat_op,
                        destinacao,
                        uf_origem,
                        uf_destino,
                        cnpj_ex_origem,
                        cod_prod_exemplo,
                        data_recalcular,
                        flag_exec,
                        alternativo_ex_origem,
                        id_cliente_prod,
                        id_cliente_config_prod,
                        frequencia,
                        id_mun_origem,
                        id_mun_destino,
                        dias_futuros,
                        id_config_principal,
                        id_config_icms,
                        id_config_ipi,
                        id_config_pis,
                        id_config_cofins,
                        id_config_antecipacao,
                        prioridade,
                        id_config_manual_pis,
                        id_config_manual_cofins,
                        id_config_manual_ipi,
                        id_config_manual_antecipacao,
                        cnae_destinatario,
                        grupo_str,
                        id_config_setor,
                        ent_sai,
                        id_rel_futuro,
                        finalidade,
                        total_produtos,
                        total_produtos_calculados,
                        flag_desconsiderar,
                        origem_produto_alternativo,
                        cenario_ind_prod,
                        id_prod_adic,
                        st_cest,
                        convert(varchar(20),dt_criacao,120) as dt_criacao,
                        origem_criacao,
                        flag_homolog,
                        trib_cigarro_calculo,
                        criacao_automatica,
                        flag_revisao,
                        flag_origem_escrituracao,
                        cenario_inativo
                    FROM systax_app.dbo.tributos_internos_cache_config a(NOLOCK)
                    WHERE exists(SELECT TOP 1 1 FROM systax_app.[SNOWFLAKE].enviar_tributos_internos_cache_config ecp (NOLOCK) WHERE ecp.id = a.id);
                  """,
                  tabela='tributos_internos_cache_config',
                  limite=100000),
        Consultas(consulta="""
                SELECT id, id_grupo, id_trib_inter_config, id_cli, convert(bigint,ts) as ts 
                    FROM systax_app.dbo.grupo_config a(NOLOCK)
                    WHERE exists(SELECT TOP 1 1 FROM systax_app.[SNOWFLAKE].enviar_grupo_config ecp (NOLOCK) WHERE ecp.id = a.id);
                  """,
                  tabela='grupo_config',
                  limite=1000000),
        Consultas(consulta="""
                SELECT id, id_ex_origem, id_familia, ordem, ordem_inversa FROM systax_regras.dbo.ex_origem_cache_familia (NOLOCK);
                  """,
                  tabela='ex_origem_cache_familia',
                  limite=1000000),
        Consultas(consulta="""
                    SELECT id FROM systax_app.snowflake.apagar_custom_prod;
                  """,
                  tabela='apagar_custom_prod',
                  limite=1000000),
        Consultas(consulta="""
                    SELECT id FROM systax_app.snowflake.apagar_cean_relacionado;
                  """,
                  tabela='apagar_cean_relacionado',
                  limite=1000000),
        Consultas(consulta="""
                    SELECT id FROM systax_app.snowflake.apagar_grupo_custom_prod;
                  """,
                  tabela='apagar_grupo_custom_prod',
                  limite=1000000),
        Consultas(consulta="""
                    SELECT id FROM systax_app.snowflake.apagar_grupo_config;
                  """,
                  tabela='apagar_grupo_config',
                  limite=1000000),
        Consultas(consulta="""
                    SELECT id FROM systax_app.snowflake.apagar_clientes;
                  """,
                  tabela='apagar_clientes',
                  limite=1000000),
        Consultas(consulta="""
                    SELECT id FROM systax_app.snowflake.apagar_tributos_internos_cache;
                  """,
                  tabela='apagar_tributos_internos_cache',
                  limite=1000000),
        Consultas(consulta="""
                    SELECT id FROM systax_app.snowflake.apagar_tributos_internos_cache_st;
                  """,
                  tabela='apagar_tributos_internos_cache_st',
                  limite=1000000),
        Consultas(consulta="""
                    SELECT id FROM systax_app.snowflake.apagar_tributos_internos_cache_config;
                  """,
                  tabela='apagar_tributos_internos_cache_config',
                  limite=1000000),
        Consultas(consulta="""
                SELECT id, id_grupo, id_custom_prod, id_cli, convert(bigint,ts) as ts 
                    FROM systax_app.dbo.grupo_custom_prod a(NOLOCK)
                    WHERE exists(SELECT TOP 1 1 FROM systax_app.[SNOWFLAKE].enviar_grupo_custom_prod ecp (NOLOCK) WHERE ecp.id = a.id);
                  """,
                  tabela='grupo_custom_prod',
                  limite=10000000),
        Consultas(consulta="""
                  SELECT  id,
                          id_tributos_internos,
                          p_red_bc,
                          aliquota_st,
                          mva,
                          mva_ajustado,
                          mva_lista_positiva_ajustado,
                          mva_lista_negativa_ajustado,
                          mva_lista_neutra_ajustado,
                          valor_pauta,
                          convert(varchar(10),vigencia_de,120) as vigencia_de,
                          convert(varchar(10),vigencia_ate,120) as vigencia_ate,
                          regra,
                          regra_bc,
                          regra_aliq,
                          convert(varchar(20),data,120) as data,
                          convert(bigint,ts) as ts,
                          id_config,
                          dispositivo_legal,
                          observacoes,
                          hash_config,
                          id_cache_rep,
                          unidade_pauta,
                          hash_retorno_erp,
                          carga_media,
                          hash_retorno_erp_debug,
                          flag_revisao,
                          bc_composicao,
                          generico,
                          severidade,
                          estatisticas,
                          fcp,
                          bc_composicao_fcp
                    FROM systax_app.dbo.tributos_internos_cache_st a (NOLOCK)
                    WHERE exists(SELECT TOP 1 1 FROM systax_app.[SNOWFLAKE].enviar_tributos_internos_cache_st ecp (NOLOCK) WHERE ecp.id = a.id);
                  """,
                  tabela='tributos_internos_cache_st',
                  limite=2500000),
        Consultas(consulta="""
                SELECT  id,
                        id_cliente,
                        id_config,
                        cod_produto,
                        cst,
                        cfop,
                        p_red_bc,
                        aliquota,
                        valor_pauta,
                        convert(varchar(10),vigencia_de,120) as vigencia_de,
                        convert(varchar(10),vigencia_ate,120) as vigencia_ate,
                        dispositivo_legal,
                        observacoes,
                        id_produto,
                        regra,
                        regra_bc,
                        regra_aliq,
                        convert(varchar(10),data,120) as data,
                        convert(bigint,ts) as ts,
                        dif_aliq_nao_contrib,
                        hash_config,
                        tipo_antecipacao,
                        encerra_trib,
                        responsavel,
                        perc_fixo,
                        ant_mva,
                        ant_mva_ajustado,
                        ant_mva_lista_positiva_ajustado,
                        ant_mva_lista_negativa_ajustado,
                        ant_mva_lista_neutra_ajustado,
                        id_cache_rep,
                        unidade_pauta,
                        hash_retorno_erp,
                        inf_adicionais,
                        p_red_aliq,
                        valor_unid_trib,
                        p_red_val_imp,
                        aliq_especifica,
                        hash_retorno_erp_debug,
                        flag_revisao,
                        codigo_natureza_receita,
                        bc_composicao,
                        p_red_bc_interna_dest,
                        aliq_interna_dest,
                        cred_indicador_credito,
                        cred_cst_entrada,
                        cred_percentual_credito,
                        convert(varchar(10),cred_vigencia_de,120) as cred_vigencia_de,
                        convert(varchar(10),cred_vigencia_ate,120) as cred_vigencia_ate,
                        cred_dispositivo_legal,
                        cred_observacoes,
                        cfop_entrada,
                        versao,
                        generico,
                        percentual_diferimento,
                        aliquota_desonerada,
                        cred_generico,
                        severidade,
                        estatisticas,
                        fcp,
                        cenq,
                        cest,
                        uf_dest_cst,
                        uf_dest_fcp,
                        uf_dest_dispositivo_legal,
                        convert(varchar(10),uf_dest_vigencia_de,120) as uf_dest_vigencia_de,
                        convert(varchar(10),uf_dest_vigencia_ate,120) as uf_dest_vigencia_ate,
                        uf_dest_aliq_interna_dest,
                        uf_dest_generico,
                        uf_dest_perc_partilha_dest,
                        uf_dest_observacoes,
                        uf_dest_bc_composicao,
                        uf_dest_valor_pauta,
                        uf_dest_p_red_bc,
                        uf_dest_unidade_pauta,
                        origem_produto,
                        prioridade,
                        usuario_aprovacao,
                        convert(varchar(20),data_aprovacao,120) as data_aprovacao,
                        tipo_aprovacao,
                        bc_composicao_sn_especial,
                        aliquota_sn_especial,
                        bc_composicao_fcp,
                        uf_dest_bc_composicao_fcp,
                        aliquota_cf,
                        carga_media
                    FROM systax_app.dbo.tributos_internos_cache a(NOLOCK)
                    WHERE exists(SELECT TOP 1 1 FROM systax_app.[SNOWFLAKE].enviar_tributos_internos_cache ecp (NOLOCK) WHERE ecp.id = a.id and ecp.posicao = [POSICAO] );
                  """,
                  tabela='tributos_internos_cache',
                  limite=2500000)
]

# clientes id integer, flag_setor integer, situacao integer, deletado integer, flag_retorno_ipi integer
# tributos_internos_cache_config  flag_homolog int,ent_sai varchar;
# ts_atual ts bigint datahora varchar(20)
# agrupamento_produtos cod_prod varchar(250), origem_produto int, id_cli int
# custom_prod ncm varchar(8), ex_tipi varchar(10), 

def main():
    posicao = 1
    posicao_final = 500
    stringposicao = '001'
    if len(sys.argv)  >= 2:
      findTabela = sys.argv[1]
      if findTabela != 'ALL':
        indexAtual = 0
        index = 0
        for consulta in sql_queries:
          if (consulta.tabela==findTabela):
            index = indexAtual
          indexAtual = indexAtual + 1
        sqls = [
          sql_queries[index]
        ]
      else:
        sqls = sql_queries
    else:
      sqls = sql_queries

    if len(sys.argv)  >= 3:
      tipoExecucao = sys.argv[2] # FULL | INCREMENTAL
    if len(sys.argv)  >= 4:
      posicao = int(sys.argv[3]) # inicio do loop
    if len(sys.argv)  >= 5:
      posicao_final = int(sys.argv[4]) # fim do loop
    print('TIPO: ' + tipoExecucao)
    for consulta in sqls:
        cons = consulta.consulta.replace('[SNOWFLAKE]','snowflake_'+tipoExecucao.lower())
        if(posicao==1):
          delete_files_directory(tipoExecucao, consulta.tabela)
        if(consulta.tabela=='tributos_internos_cache'):
          while posicao<=posicao_final:
            stringposicao = str(posicao)
            if(posicao < 10):
               stringposicao = '00'+stringposicao
            if(posicao >=10 and posicao < 100):
               stringposicao = '0'+stringposicao
            print('POSICAO: '+stringposicao)
            consulta_posicao = cons.replace('[POSICAO]',stringposicao)
            export_query_to_parquet(consulta_posicao, tipoExecucao, consulta.tabela, consulta.limite, stringposicao)
            posicao = posicao + 1
          if posicao==999:
              send_parquet_snowflake(tipoExecucao, consulta.tabela)
        else: ## nao eh tributos_internos_cache
          export_query_to_parquet(cons, tipoExecucao, consulta.tabela, consulta.limite)
          send_parquet_snowflake(tipoExecucao, consulta.tabela)
## exemplos:
## envio_snowflake.py ALL FULL 1 500
## envio_snowflake.py tributos_internos_cache incremental 1 500
## envio_snowflake.py tributos_internos_cache FULL 999  => somente envio da tributos_internos_cache
## envio_snowflake.py tributos_internos_cache incremental 999  => somente envio da tributos_internos_cache

if __name__ == "__main__":
    print('INICIO: '+datetime.now().strftime("%Y-%m-%d %H %M %S"))
    main()
    print('FIM: '+datetime.now().strftime("%Y-%m-%d %H %M %S"))

    