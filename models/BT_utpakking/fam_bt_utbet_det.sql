{{
    config(
        materialized='incremental'
    )
}}

with barnetrygd_meta_data as (
  select * from {{ref ('bt_meldinger_til_aa_pakke_ut')}}
),


bt_utbetaling AS (
  SELECT * FROM {{ ref ('fam_bt_utbetaling') }}
),

bt_person AS (
  SELECT * FROM {{ ref ('fam_bt_person') }}
),

pre_final AS (
SELECT * FROM barnetrygd_meta_data,
  JSON_TABLE(melding, '$'
    COLUMNS (
      behandlings_id            VARCHAR2 PATH '$.behandlingsId',
      NESTED                    PATH '$.utbetalingsperioderV2[*]'
      COLUMNS (
      stønadfom                 VARCHAR2 PATH '$.stønadFom'
      ,stønadtom                VARCHAR2 PATH '$.stønadTom'
      ,NESTED                   PATH '$.utbetalingsDetaljer[*]'
      COLUMNS (
      klassekode                VARCHAR2 PATH '$.klassekode'
      ,delytelse_id             VARCHAR2 PATH '$.delytelseId'
      ,ytelse_type              VARCHAR2 PATH '$.ytelseType'
      ,utbetalt_pr_mnd          VARCHAR2 PATH '$..utbetaltPrMnd'
      ,person_ident             VARCHAR2 PATH '$.person.personIdent'
      ,delingsprosentYtelse     VARCHAR2 PATH '$.person.delingsprosentYtelse'
      ,rolle                    VARCHAR2 PATH '$.person.rolle'
      )))
      ) j
  ),

joining_pre_final as (
  select
    person_ident,
    delingsprosentYtelse,
    nvl(b.fk_person1, -1) fk_person1,
    KLASSEKODE,
    DELYTELSE_ID,
    UTBETALT_PR_MND,
    KAFKA_OFFSET,
    rolle,
    BEHANDLINGS_ID,
    TO_DATE(stønadfom, 'YYYY-MM-DD') stønadfom,
    TO_DATE(stønadtom, 'YYYY-MM-DD') stønadtom,
    YTELSE_TYPE,
    kafka_mottatt_dato
  from
    pre_final
  left outer join dt_person.ident_off_id_til_fk_person1 b on
    pre_final.person_ident=b.off_id
    and b.gyldig_fra_dato<=pre_final.kafka_mottatt_dato
    and b.gyldig_til_dato>=kafka_mottatt_dato
    --and b.skjermet_kode=0
  where person_ident is not null
),

final as (
  select
    p.KLASSEKODE,
    p.DELYTELSE_ID,
    p.UTBETALT_PR_MND,
    p.KAFKA_OFFSET,
    p.stønadfom,
    p.stønadtom,
    p.BEHANDLINGS_ID,
    p.YTELSE_TYPE,
    p.kafka_mottatt_dato,
    u.PK_BT_UTBETALING as FK_BT_UTBETALING,
    per.PK_BT_PERSON as FK_BT_PERSON
  from joining_pre_final p
  join
  (
    select fk_person1, kafka_offset, delingsprosent_ytelse, rolle, max(pk_bt_person) as pk_bt_person
    from bt_person
    group by fk_person1, kafka_offset, delingsprosent_ytelse, rolle
  ) per
  on p.fk_person1 = per.fk_person1 and p.kafka_offset = per.kafka_offset
  and p.delingsprosentYtelse = per.delingsprosent_ytelse
  and p.rolle = per.rolle
  join bt_utbetaling u
  on p.stønadfom = u.stønad_fom and p.stønadtom = u.stønad_tom and p.kafka_offset = u.kafka_offset
)

select
--ROWNUM as PK_BT_UTBET_DET
  dvh_fambt_kafka.hibernate_sequence.nextval as PK_BT_UTBET_DET
  ,KLASSEKODE
  ,DELYTELSE_ID
  ,UTBETALT_PR_MND
  ,FK_BT_PERSON
  ,FK_BT_UTBETALING
  ,KAFKA_OFFSET
  ,BEHANDLINGS_ID
  ,localtimestamp AS lastet_dato
  ,YTELSE_TYPE
from final

