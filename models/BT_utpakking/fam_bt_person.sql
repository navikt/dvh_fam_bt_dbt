{{
    config(
        materialized='incremental'
    )
}}

with barnetrygd_meta_data as (
    select * from {{ref ('bt_meldinger_til_aa_pakke_ut')}}
),

pre_final_fagsak_person as (
    select *
    from barnetrygd_meta_data,
    json_table(melding, '$'
        columns (
            behandlings_id            varchar2(255) path '$.behandlingsId'
           ,person_ident              varchar2(255) path '$.personV2[*].personIdent'
           ,rolle                     varchar2(255) path '$.personV2[*].rolle'
           ,bostedsland               varchar2(255) path '$.personV2[*].bostedsland'
           ,delingsprosent_ytelse     varchar2(255) path '$.personV2[*].delingsprosentYtelse'
        )
    ) j
),

pre_final_utbet_det_person as (
    select *
    from barnetrygd_meta_data,
    json_table(melding, '$'
        columns (
            behandlings_id varchar2(255) path '$.behandlingsId',
            nested path '$.utbetalingsperioderV2[*].utbetalingsDetaljer[*]'
            columns (
                person_ident            varchar2(255) path '$.person.personIdent'
               ,rolle                   varchar2(255) path '$.person.rolle'
               ,bostedsland             varchar2(255) path '$.person.bostedsland'
               ,delingsprosent_ytelse   varchar2(255) path '$.person.delingsprosentYtelse'
            )
        )
    ) j
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
       ,kafka_mottatt_dato
       ,1 as soker_flagg
    from pre_final_fagsak_person
    left outer join dt_person.ident_off_id_til_fk_person1 b
    on pre_final_fagsak_person.person_ident = b.off_id
    and b.gyldig_fra_dato <= pre_final_fagsak_person.kafka_mottatt_dato
    and b.gyldig_til_dato >= kafka_mottatt_dato
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
       ,kafka_mottatt_dato
       ,0 as soker_flagg
    from pre_final_utbet_det_person
    left outer join dt_person.ident_off_id_til_fk_person1 b
    on pre_final_utbet_det_person.person_ident = b.off_id
    and b.gyldig_fra_dato <= pre_final_utbet_det_person.kafka_mottatt_dato
    and b.gyldig_til_dato >= kafka_mottatt_dato
    --and b.skjermet_kode=0
    where person_ident is not null
),

final as (
  select * from final_fagsak_person
  union
  select * from final_utbet_det_person
)

select
    dvh_fambt_kafka.hibernate_sequence.nextval as pk_bt_person
   ,cast(null as varchar2(30)) annenpart_bostedsland
   ,cast(null as varchar2(30)) annenpart_personident
   ,cast(null as varchar2(30)) annenpart_statsborgerskap
   ,cast(bostedsland as varchar2(255 char)) bostedsland
   ,cast(null as varchar2(30)) delingsprosent_omsorg
   ,delingsprosent_ytelse
   ,case when fk_person1 = -1 then person_ident
         else null
    end person_ident
   ,cast(null as varchar2(30)) primærland
   ,rolle
   ,cast(null as varchar2(30)) sekundærland
   ,fk_person1
   ,kafka_offset
   ,behandlings_id
   ,localtimestamp as lastet_dato
   ,localtimestamp as oppdatert_dato
   ,soker_flagg
from final