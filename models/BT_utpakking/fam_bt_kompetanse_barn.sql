{{
    config(
        materialized='incremental'
    )
}}

with barnetrygd_meta_data as (
    select * from {{ref ('bt_meldinger_til_aa_pakke_ut')}}
),

bt_komp_barn as (
    select * from {{ ref ('fam_bt_kompetanse_perioder') }}
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
               ,kompetanse_resultat            varchar2(255) path '$.resultat'
               ,barnets_bostedsland            varchar2(255) path '$.barnetsBostedsland'
               ,sokersaktivitet                varchar2(255) path '$.sokersaktivitet'
               ,sokersaktivitetsland           varchar2(255) path '$.sokersAktivitetsland'
               ,annenforeldersaktivitet        varchar2(255) path '$.annenForeldersAktivitet'
               ,annenforeldersaktivitetsland   varchar2(255) path '$.annenForeldersAktivitetsland'
               ,nested path '$.barnsIdenter[*]'
                columns (
                  personidentbarn  varchar2(255) path '$[*]'
                )
            )
        )
    ) j
        --where json_value (melding, '$.kompetanseperioder.size()' )> 0
),

joining_pre_final as (
    select
        personidentbarn,
        nvl(b.fk_person1, -1) fk_person1,
        tom,
        fom,
        kompetanse_resultat,
        sokersaktivitet,
        sokersaktivitetsland,
        annenforeldersaktivitet,
        annenforeldersaktivitetsland,
        kafka_offset,
        kafka_mottatt_dato,
        barnets_bostedsland
    from pre_final
    left outer join dt_person.ident_off_id_til_fk_person1 b
    on pre_final.personidentbarn=b.off_id
    and b.gyldig_fra_dato<=pre_final.kafka_mottatt_dato
    and b.gyldig_til_dato>=kafka_mottatt_dato
    --and b.skjermet_kode=0
),

final as (
    select
        j.fk_person1,
        j.fom,
        j.tom,
        j.kafka_offset,
        j.kompetanse_resultat,
        j.kafka_mottatt_dato,
        j.barnets_bostedsland,
        k.pk_bt_kompetanse_perioder as fk_bt_kompetanse_perioder
    from joining_pre_final j
    join bt_komp_barn k
    on coalesce(j.fom,'-1') = coalesce(k.fom,'-1') and coalesce(j.tom,'-1') = coalesce(k.tom,'-1')
    and coalesce(j.kompetanse_resultat,'-1') = coalesce(k.kompetanse_resultat,'-1')
    and coalesce(j.barnets_bostedsland,'-1') = coalesce(k.barnets_bostedsland,'-1')
    and coalesce(j.sokersaktivitet,'-1') = coalesce(k.sokersaktivitet,'-1')
    and coalesce(j.sokersaktivitetsland,'-1') = coalesce(k.sokers_aktivitetsland,'-1')
    and coalesce(j.annenforeldersaktivitet,'-1') = coalesce(k.annenforelder_aktivitet,'-1')
    and coalesce(j.annenforeldersaktivitetsland,'-1') = coalesce(k.annenforelder_aktivitetsland,'-1')
    and j.kafka_offset = k.kafka_offset
)

select
    dvh_fambt_kafka.hibernate_sequence.nextval as pk_bt_kompetanse_barn,
    fk_bt_kompetanse_perioder,
    fk_person1,
    localtimestamp as lastet_dato
from final