{{
    config(
        materialized='incremental'
    )
}}

with barnetrygd_meta_data as (
  select * from {{ref ('bt_meldinger_til_aa_pakke_ut')}}
),

pre_final_fagsak_person as (
select * from barnetrygd_meta_data,
  json_table(melding, '$'
    COLUMNS (
        behandlings_id            VARCHAR2 PATH '$.behandlingsId'
        ,person_ident              VARCHAR2 PATH '$.personV2[*].personIdent'
        ,rolle                     VARCHAR2 PATH '$.personV2[*].rolle'
        ,bostedsland               VARCHAR2 PATH '$.personV2[*].bostedsland'
        ,delingsprosent_ytelse     VARCHAR2 PATH '$.personV2[*].delingsprosentYtelse'
         )
        ) j
),

pre_final_utbet_det_person as (
select * from barnetrygd_meta_data,
  json_table(melding, '$'
    COLUMNS (
      behandlings_id            VARCHAR2 PATH '$.behandlingsId',
      NESTED                    PATH '$.utbetalingsperioderV2[*].utbetalingsDetaljer[*]'
      COLUMNS (
      person_ident               VARCHAR2 PATH '$.person.personIdent'
      ,rolle                     VARCHAR2 PATH '$.person.rolle'
      ,bostedsland               VARCHAR2 PATH '$.person.bostedsland'
      ,delingsprosent_ytelse      VARCHAR2 PATH '$.person.delingsprosentYtelse'
        )
      )) j
      where person_ident is not null
      --where json_value (melding, '$.utbetalingsperioderV2.utbetalingsDetaljer.size()' )> 0

),

final_fagsak_person as (
  select
    person_ident
    ,nvl(b.fk_person1, -1) fk_person1
    ,behandlings_id
    ,rolle
    ,bostedsland
    ,delingsprosent_ytelse
    ,kafka_offset
    ,KAFKA_MOTTATT_DATO
    ,1 as soker_flagg
  from
    pre_final_fagsak_person
  left outer join dt_person.ident_off_id_til_fk_person1 b on
    pre_final_fagsak_person.person_ident=b.off_id
    and b.gyldig_fra_dato<=pre_final_fagsak_person.kafka_mottatt_dato
    and b.gyldig_til_dato>=kafka_mottatt_dato
    --and b.skjermet_kode=0
),

final_utbet_det_person as (
  select
    person_ident
    ,nvl(b.fk_person1, -1) fk_person1
    ,behandlings_id
    ,rolle
    ,bostedsland
    ,delingsprosent_ytelse
    ,kafka_offset
    ,KAFKA_MOTTATT_DATO
    ,0 as soker_flagg
  from
    pre_final_utbet_det_person
  left outer join dt_person.ident_off_id_til_fk_person1 b on
    pre_final_utbet_det_person.person_ident=b.off_id
    and b.gyldig_fra_dato<=pre_final_utbet_det_person.kafka_mottatt_dato
    and b.gyldig_til_dato>=kafka_mottatt_dato
    --and b.skjermet_kode=0
  where person_ident is not null
),

final as (
  select * from final_fagsak_person
  union
  select * from final_utbet_det_person
)

select
  dvh_fambt_kafka.hibernate_sequence.nextval as PK_BT_PERSON
  ,cast(null as varchar2(30)) ANNENPART_BOSTEDSLAND
  ,cast(null as varchar2(30)) ANNENPART_PERSONIDENT
  ,cast(null as varchar2(30)) ANNENPART_STATSBORGERSKAP
  ,cast(BOSTEDSLAND as VARCHAR2(255 CHAR)) BOSTEDSLAND
  ,cast(null as varchar2(30)) DELINGSPROSENT_OMSORG
  ,DELINGSPROSENT_YTELSE
  ,case when fk_person1 = -1 then person_ident
        else null
   end person_ident
  ,cast(null as varchar2(30)) PRIMÆRLAND
  ,ROLLE
  ,cast(null as varchar2(30)) SEKUNDÆRLAND
  ,FK_PERSON1
  ,KAFKA_OFFSET
  ,BEHANDLINGS_ID
  ,localtimestamp AS lastet_dato
  ,localtimestamp AS OPPDATERT_DATO
  ,soker_flagg
from final




