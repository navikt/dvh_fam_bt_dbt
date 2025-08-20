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
            behandlings_id varchar2(255) path '$.behandlingsId',
            nested path '$.utbetalingsperioderV2[*]'
            columns (
                utbetalt_per_mnd   varchar2(255) path '$.utbetaltPerMnd'
               ,stønad_fom         varchar2(255) path '$.stønadFom'
               ,stønad_tom         varchar2(255) path '$.stønadTom'
               ,hjemmel            varchar2(255) path '$.hjemmel'
            )
        )
    ) j
    where stønad_fom is not null
      --where json_value (melding, '$.utbetalingsperioderV2.size()' )> 0
),

final as (
    select
        p.behandlings_id,
        p.utbetalt_per_mnd,
        p.stønad_fom,
        p.stønad_tom,
        p.hjemmel,
        p.kafka_offset,
        p.kafka_mottatt_dato,
        pk_bt_fagsak as fk_bt_fagsak
    from pre_final p
    join bt_fagsak b
    on p.kafka_offset = b.kafka_offset
)

select
    dvh_fambt_kafka.hibernate_sequence.nextval pk_bt_utbetaling
   ,utbetalt_per_mnd
   ,to_date(stønad_fom, 'YYYY-MM-DD') stønad_fom
   ,to_date(stønad_tom, 'YYYY-MM-DD') stønad_tom
   ,hjemmel
   ,fk_bt_fagsak
   ,kafka_offset
   ,behandlings_id
   ,localtimestamp as lastet_dato
from final