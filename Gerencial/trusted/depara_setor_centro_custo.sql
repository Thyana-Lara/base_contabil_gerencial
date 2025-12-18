SELECT filial, codigo_centro_de_custo_mxm,nome_centro_de_custo_mxm,classificacao_onco, 

FROM `sandbox-financeiro-471714.ds_raw_depara.tb_depara_centro_custo` c

left join xinteroper-data-platform-prd.ds_refined_noc_hospital.tb_setor s on c.codigo_centro_de_custo_mxm = REGEXP_REPLACE(trim(s.cd_cen_cus), r'^0+', '') 