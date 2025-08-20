{{
    config(
        materialized='incremental'
    )
}}

with barnetrygd_meta_data as (
  select * from {{ref ('bt_meldinger_til_aa_pakke_ut')}}
),

bt_person as (
  select *
  from {{ ref ('fam_bt_person') }}
  where (rolle = 'SØKER' and soker_flagg = 1) or rolle = 'BARN'
),

statsborgerskap_soker as (
    select *
    from barnetrygd_meta_data,
    json_table(melding, '$'
        columns (
            person_ident varchar2(255) path '$.personV2[*].personIdent'
           ,nested path '$.personV2[*].statsborgerskap[*]'
            columns (
                statsborgerskap_soker varchar2(255) path '$'
            )
        )
    ) j
),

statsborgerskap_barn as (
    select *
    from barnetrygd_meta_data,
    json_table(melding, '$'
        columns (
            nested path '$.utbetalingsperioderV2[*]'
            columns (
                nested path '$.utbetalingsDetaljer[*]'
                columns (
                    person_ident_barn varchar2(255) path '$.person.personIdent'
                   ,rolle             varchar2(255) path '$.person.rolle'
                   ,nested path '$.person.statsborgerskap[*]'
                    columns (
                        statsborgerskap_barn varchar2(255) path '$'
                    )
                )
            )
        )
    )j
    --where json_value (melding, '$.utbetalingsperioderV2.utbetalingsDetaljer.size()' )> 0
),

pre_final_soker as (
    select
        person_ident
       ,statsborgerskap_soker as statsborgerskap
       ,nvl(b.fk_person1, -1) fk_person1
       ,kafka_offset
       ,kafka_mottatt_dato
    from statsborgerskap_soker
    left outer join dt_person.ident_off_id_til_fk_person1 b
    on person_ident = b.off_id
    and b.gyldig_fra_dato <= kafka_mottatt_dato
    and b.gyldig_til_dato >= kafka_mottatt_dato
    and b.skjermet_kode = 0
),

pre_final_barn as (
    select
        person_ident_barn
       ,statsborgerskap_barn as statsborgerskap
       ,nvl(b.fk_person1, -1) fk_person1
       ,kafka_offset
       ,kafka_mottatt_dato
    from statsborgerskap_barn
    left outer join dt_person.ident_off_id_til_fk_person1 b
    on person_ident_barn = b.off_id
    and b.gyldig_fra_dato <= kafka_mottatt_dato
    and b.gyldig_til_dato >= kafka_mottatt_dato
    and b.skjermet_kode = 0
),

pre_final as (
    select * from pre_final_soker
    union
    select * from pre_final_barn
),

final as (
    select
        p.statsborgerskap
       ,p.kafka_mottatt_dato
       ,p.kafka_offset
       ,p.fk_person1
       ,per.pk_bt_person as fk_bt_person
    from pre_final p
    join bt_person per
    on p.fk_person1 = per.fk_person1
    and p.kafka_offset = per.kafka_offset
)

select
    dvh_fambt_kafka.hibernate_sequence.nextval as pk_statsborgerskap
   ,statsborgerskap
   ,localtimestamp as lastet_dato
   ,fk_bt_person
from final