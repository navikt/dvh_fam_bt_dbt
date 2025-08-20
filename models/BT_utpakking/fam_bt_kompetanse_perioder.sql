{{
    config(
        materialized='incremental'
    )
}}

with barnetrygd_meta_data as (
    select * from {{ref ('bt_meldinger_til_aa_pakke_ut')}}
),

bt_fagsak as (
    select * from {{ ref ('fam_bt_fagsak') }}
),

pre_final as (
    select *
    from barnetrygd_meta_data,
    json_table(melding, '$'
        columns (
            nested path '$.kompetanseperioder[*]'
            columns (
                tom                            varchar2(255) path '$.tom'
               ,fom                            varchar2(255) path '$.fom'
               ,sokersaktivitet                varchar2(255) path '$.sokersaktivitet'
               ,sokers_aktivitetsland          varchar2(255) path '$.sokersAktivitetsland'
               ,annenforelder_aktivitet        varchar2(255) path '$.annenForeldersAktivitet'
               ,annenforelder_aktivitetsland   varchar2(255) path '$.annenForeldersAktivitetsland'
               ,barnets_bostedsland            varchar2(255) path '$.barnetsBostedsland'
               ,kompetanse_resultat            varchar2(255) path '$.resultat'
            )
        )
    ) j
    where fom is not null
    --where json_value (melding, '$.kompetanseperioder.size()' )> 0
  ),

final as (
    select p.*,
           f.pk_bt_fagsak as fk_bt_fagsak
    from pre_final p
    join bt_fagsak f
    on p.kafka_offset = f.kafka_offset
    where p.fom is not null
)

select
  --ROWNUM as PK_BT_KOMPETANSE_PERIODER,
    dvh_fambt_kafka.hibernate_sequence.nextval as pk_bt_kompetanse_perioder,
    fom,
    tom,
    sokersaktivitet,
    annenforelder_aktivitet,
    annenforelder_aktivitetsland,
    kompetanse_resultat,
    barnets_bostedsland,
    localtimestamp as lastet_dato,
    fk_bt_fagsak,
    sokers_aktivitetsland,
    kafka_offset
from final