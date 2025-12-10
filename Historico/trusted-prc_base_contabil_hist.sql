CREATE OR REPLACE PROCEDURE `sandbox-financeiro-471714.ds_trusted_contabil.prc_base_contabil_hist`(p_data_execucao DATE)
begin

    -- definir a data de referência da execução
      
    declare v_data_atual date default p_data_execucao;
    declare v_ano int64;
    declare v_mes int64;

    -- criar tabela temporária com os ano/mês que precisam ser processados
    
    create or replace temp table tmp_meses (ano int64, mes int64);

    insert into tmp_meses
    select distinct
        extract(year from dt_competencia) as ano,
        extract(month from dt_competencia) as mes
    from sandbox-financeiro-471714.ds_raw_contabil.vw_contabil_hist
    where date(dh_carga) = v_data_atual;
 
    -- Loop para cada ANO/MÊS detectado automaticamente
    
    while exists (select 1 from tmp_meses) do

        -- pegar o primeiro mês da lista
        set v_ano = (select ano from tmp_meses order by ano, mes limit 1);
        set v_mes = (select mes from tmp_meses order by ano, mes limit 1);

        -- remover dados antigos da trusted para este ano/mês
        delete from sandbox-financeiro-471714.ds_trusted_contabil.tb_contabil_hist
        where extract(year from dt_competencia) = v_ano
        and extract(month from dt_competencia) = v_mes;

       ---INICO DO PROCESSO PARA INSERIR DADOS NOVOS
       insert into sandbox-financeiro-471714.ds_trusted_contabil.tb_contabil_hist
        -- identificar a última carga daquele mês na tabela histórica (raw)      
        with ultimas_cargas as (
            select
                extract(year from dt_competencia) as ano,
                extract(month from dt_competencia) as mes,
                max(dh_carga) as ultima_dh_carga
            from sandbox-financeiro-471714.ds_raw_contabil.vw_contabil_hist
            where extract(year from dt_competencia) = v_ano
              and extract(month from dt_competencia) = v_mes
            group by 1,2),
 
        -- filtrar somente os registros da última carga
        
        hraz_filtrada as (
            select h.*
            from sandbox-financeiro-471714.ds_raw_contabil.vw_contabil_hist h
            join ultimas_cargas u
              on u.ano = extract(year from h.dt_competencia)
             and u.mes = extract(month from h.dt_competencia)
             and u.ultima_dh_carga = h.dh_carga),
  
        --informações auxiliares:
        hraz_enriquecida as (
            select
                h.*,
                -- ordem de chegada da nota (primeira vez no mês)
                row_number() over (
                    partition by h.cd_empresa, h.cd_conta_contabil, h.cd_documento
                    order by h.dh_carga
                ) as rn_primeira_vez,
                -- classificação da nota: se primeira vez → "nova", senão → "existente"
                case 
                    when row_number() over (
                        partition by h.cd_empresa, h.cd_conta_contabil, h.cd_documento
                        order by h.dh_carga
                    ) = 1 then 'nova'
                    else 'existente'
                end as sn_nota_nova,
                -- cnpj raiz (8 dígitos)
                substr(regexp_replace(h.cd_fornecedor, r'\D', ''), 1, 8) as cnpj_raiz
            from hraz_filtrada h)

        -- inserir o mês processado na TRUSTED, já com todos os de/paras
        select
            g.cd_empresa, 
            g.cd_conta_contabil, 
            g.ds_conta_contabil, 
            g.cd_centro_custo, 
            g.ds_centro_custo,
            e.filial as cd_filial,
            e.empresa as nm_empresa,
            g.dt_competencia, 
            g.cd_lote, 
            g.cd_documento, 
            g.ds_titulo, 
            c.ds_classificacao_n1,
            c.ds_classificacao_n2,
            c.ds_classificacao_n3,
            c.nm_conta_gerencial,
            g.ds_razao, 
            g.ds_origem, 
            g.vl_valor, 
            g.cd_fornecedor, 
            g.ds_fornecedor, 
            g.cd_cliente, 
            g.ds_cliente, 
            g.cd_consiliacao, 
            g.cd_projeto, 
            g.ds_projeto, 
            g.sequencia,  
            g.dt_consulta, 
            g.dt_filtro_inicial, 
            g.dt_filtro_final, 
            g.dh_carga,
            g.rn_primeira_vez,
            g.sn_nota_nova,
            g.cnpj_raiz

        from hraz_enriquecida g
        left join sandbox-financeiro-471714.ds_raw_contabil.tb_depara_empresa_mxm e on left(cast(g.cd_centro_custo as string), 3) = e.cdcusto
        left join sandbox-financeiro-471714.ds_refined_depara.vw_depara_conta_contabil c on g.cd_conta_contabil = c.cd_conta_contabil;

        -- remover o mês processado da tabela temporária
        
        delete from tmp_meses 
        where ano = v_ano and mes = v_mes;

    end while;

end;