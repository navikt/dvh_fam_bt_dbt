{{
    config(
        materialized='incremental'
    )
}}

with barnetrygd_meta_data as (
  select * from {{ref ('bt_meldinger_til_aa_pakke_ut')}}
),

bt_komp_barn AS (
  SELECT * FROM {{ ref ('fam_bt_kompetanse_perioder') }}
),

bt_fagsak AS (
  SELECT * FROM {{ ref ('fam_bt_fagsak') }}
),

pre_final as (
select * from barnetrygd_meta_data,
  json_table(melding, '$'
    COLUMNS (
        nested path '$.kompetanseperioder[*]'
    COLUMNS (
         tom                            VARCHAR2 PATH '$.tom'
        ,fom                            VARCHAR2 PATH '$.fom'
        ,kompetanse_Resultat            VARCHAR2 PATH '$.resultat'
        ,barnets_bostedsland            Varchar2 path '$.barnetsBostedsland'
        ,sokersaktivitet                Varchar2 path '$.sokersaktivitet'
        ,sokersAktivitetsland           Varchar2 path '$.sokersAktivitetsland'
        ,annenForeldersAktivitet        Varchar2 path '$.annenForeldersAktivitet'
        ,annenForeldersAktivitetsland   Varchar2 path '$.annenForeldersAktivitetsland'
        ,nested path '$.barnsIdenter[*]'
         columns (
         personidentbarn   varchar2 path '$[*]'
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
    kompetanse_Resultat,
    sokersaktivitet,
    sokersAktivitetsland,
    annenForeldersAktivitet,
    annenForeldersAktivitetsland,
    kafka_offset,
    kafka_mottatt_dato,
    barnets_bostedsland
  from
    pre_final
  left outer join dt_person.ident_off_id_til_fk_person1 b on
    pre_final.personidentbarn=b.off_id
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
    j.kompetanse_Resultat,
    j.kafka_mottatt_dato,
    j.barnets_bostedsland,
    k.pK_BT_KOMPETANSE_PERIODER as fK_BT_KOMPETANSE_PERIODER
  from joining_pre_final j
  join bt_komp_barn k
  on COALESCE(j.fom,'-1') = COALESCE(k.fom,'-1') and COALESCE(j.tom,'-1') = COALESCE(k.tom,'-1')
  and COALESCE(j.kompetanse_Resultat,'-1') = COALESCE(k.kompetanse_Resultat,'-1')
  and COALESCE(j.barnets_bostedsland,'-1') = COALESCE(k.barnets_bostedsland,'-1')
  and COALESCE(j.sokersaktivitet,'-1') = COALESCE(k.SOKERSAKTIVITET,'-1')
  and COALESCE(j.sokersAktivitetsland,'-1') = COALESCE(k.SOKERS_AKTIVITETSLAND,'-1')
  and COALESCE(j.annenForeldersAktivitet,'-1') = COALESCE(k.ANNENFORELDER_AKTIVITET,'-1')
  and COALESCE(j.annenForeldersAktivitetsland,'-1') = COALESCE(k.ANNENFORELDER_AKTIVITETSLAND,'-1')
  and j.kafka_offset = k.kafka_offset
)

select
  dvh_fambt_kafka.hibernate_sequence.nextval as PK_BT_KOMPETANSE_BARN,
  FK_BT_KOMPETANSE_PERIODER,
  FK_PERSON1,
  localtimestamp as LASTET_DATO
from final