{{
    config(
        materialized='incremental'
    )
}}

with barnetrygd_meta_data as (
  select * from {{ref ('bt_meldinger_til_aa_pakke_ut')}}
),

bt_fagsak AS (
  SELECT * FROM {{ ref ('fam_bt_fagsak') }}
),

pre_final as (
select * from barnetrygd_meta_data,
  json_table(melding, '$'
    COLUMNS (
      behandlings_id     VARCHAR2 PATH '$.behandlingsId',
         NESTED             PATH '$.utbetalingsperioderV2[*]'
         COLUMNS (
         utbetalt_per_mnd   VARCHAR2 PATH '$.utbetaltPerMnd'
        ,stønad_fom         VARCHAR2 PATH '$.stønadFom'
        ,stønad_tom         VARCHAR2 PATH '$.stønadTom'
        ,hjemmel            VARCHAR2 PATH '$.hjemmel'
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
    pk_BT_FAGSAK as FK_BT_FAGSAK
  from pre_final p
  join bt_fagsak b
  on p.kafka_offset = b.kafka_offset
)

select
  dvh_fambt_kafka.hibernate_sequence.nextval PK_BT_UTBETALING
  ,UTBETALT_PER_MND
  ,TO_DATE(STØNAD_FOM, 'YYYY-MM-DD') STØNAD_FOM
  ,TO_DATE(STØNAD_TOM, 'YYYY-MM-DD') STØNAD_TOM
  ,HJEMMEL
  ,FK_BT_FAGSAK
  ,KAFKA_OFFSET
  ,BEHANDLINGS_ID
  ,localtimestamp AS lastet_dato
from final
