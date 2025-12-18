create table if not exists 
`sandbox-financeiro-471714.ds_trusted_contabil.tb_chaves_lancamento_nf_versionadas`
(
    ano int64,
    mes int64,
    chave_nota string,
    dh_versao datetime,
    sn_nota_nova string
);


insert into `sandbox-financeiro-471714.ds_trusted_contabil.tb_chaves_lancamento_nf_versionadas`
(
    ano,
    mes,
    chave_nota,
    dh_versao,
    sn_nota_nova
)
select
    extract(year from dt_competencia) as ano,
    extract(month from dt_competencia) as mes,
    concat(
        cast(cd_empresa as string), '-',
        cast(cd_conta_contabil as string), '-',
        cd_documento, '-',
        ds_titulo, '-',
        cast(sequencia as string)
    ) as chave_nota,
    current_datetime('America/Sao_Paulo') as dh_versao,
    'nova' as sn_nota_nova
from `sandbox-financeiro-471714.ds_trusted_contabil.tb_contabil_hist`;

