{{
    config(
        materialized='incremental'
    )
}}

with barnetrygd_meta_data as (
    select * from {{ref ('bt_meldinger_til_aa_pakke_ut')}}
),


bt_utbetaling as (
    select * from {{ ref ('fam_bt_utbetaling') }}
),

bt_person as (
    select * from {{ ref ('fam_bt_person') }}
),

pre_final as (
    select *
    from barnetrygd_meta_data,
    json_table(melding, '$'
        columns (
            behandlings_id varchar2(255) path '$.behandlingsId',
            nested path '$.utbetalingsperioderV2[*]'
            columns (
                stønadfom   varchar2(255) path '$.stønadFom'
               ,stønadtom   varchar2(255) path '$.stønadTom'
               ,nested path '$.utbetalingsDetaljer[*]'
                columns (
                    klassekode            varchar2(255) path '$.klassekode'
                   ,delytelse_id          varchar2(255) path '$.delytelseId'
                   ,ytelse_type           varchar2(255) path '$.ytelseType'
                   ,utbetalt_pr_mnd       varchar2(255) path '$..utbetaltPrMnd'
                   ,person_ident          varchar2(255) path '$.person.personIdent'
                   ,delingsprosentytelse  varchar2(255) path '$.person.delingsprosentYtelse'
                   ,rolle                 varchar2(255) path '$.person.rolle'
                )
            )
        )
    ) j
),

joining_pre_final as (
    select
        person_ident,
        delingsprosentytelse,
        nvl(b.fk_person1, -1) fk_person1,
        klassekode,
        delytelse_id,
        utbetalt_pr_mnd,
        kafka_offset,
        rolle,
        behandlings_id,
        to_date(stønadfom, 'YYYY-MM-DD') stønadfom,
        to_date(stønadtom, 'YYYY-MM-DD') stønadtom,
        ytelse_type,
        kafka_mottatt_dato
    from pre_final
    left outer join dt_person.ident_off_id_til_fk_person1 b
    on pre_final.person_ident = b.off_id
    and b.gyldig_fra_dato <= pre_final.kafka_mottatt_dato
    and b.gyldig_til_dato >= kafka_mottatt_dato
    --and b.skjermet_kode=0
    where person_ident is not null
),

final as (
    select
        p.klassekode,
        p.delytelse_id,
        p.utbetalt_pr_mnd,
        p.kafka_offset,
        p.stønadfom,
        p.stønadtom,
        p.behandlings_id,
        p.ytelse_type,
        p.kafka_mottatt_dato,
        u.pk_bt_utbetaling as fk_bt_utbetaling,
        per.pk_bt_person as fk_bt_person
    from joining_pre_final p
    join
    (
      select fk_person1, kafka_offset, delingsprosent_ytelse, rolle, max(pk_bt_person) as pk_bt_person
      from bt_person
      group by fk_person1, kafka_offset, delingsprosent_ytelse, rolle
    ) per
    on p.fk_person1 = per.fk_person1 and p.kafka_offset = per.kafka_offset
    and p.delingsprosentytelse = per.delingsprosent_ytelse
    and p.rolle = per.rolle
    join bt_utbetaling u
    on p.stønadfom = u.stønad_fom and p.stønadtom = u.stønad_tom and p.kafka_offset = u.kafka_offset
)

select
--ROWNUM as PK_BT_UTBET_DET
    dvh_fambt_kafka.hibernate_sequence.nextval as pk_bt_utbet_det
   ,klassekode
   ,delytelse_id
   ,utbetalt_pr_mnd
   ,fk_bt_person
   ,fk_bt_utbetaling
   ,kafka_offset
   ,behandlings_id
   ,localtimestamp as lastet_dato
   ,ytelse_type
from final