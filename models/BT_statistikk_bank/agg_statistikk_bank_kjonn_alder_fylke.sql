{{
    config(
        materialized='incremental'
    )
}}

with mottaker as (
    select *
    from {{ ref('stg_fak_statistikk_bank_mottaker') }}
)
,

--Gruppere på nivå aar_kvartal, kjønn, aldersgruppe og fylke
agg as (
    select
        aar_kvartal
       ,kjonn_besk
       ,alder_gruppe_besk
       ,nåværende_fylke_nr_navn
       ,count(distinct case when barn_selv_mottaker_flagg = 0 then fk_person1 end) as antall
       ,count(distinct case when barn_selv_mottaker_flagg = 1 then fk_person1 end) as antall_mottaker_barn
    from mottaker
    group by
        aar_kvartal
       ,kjonn_besk
       ,alder_gruppe_besk
       ,nåværende_fylke_nr_navn
)
,

--Gruppere på aar_kvartal og kjønn
kjonn_i_alt as (
    select
        mottaker.aar_kvartal
       ,mottaker.kjonn_besk
       ,alder_gruppe_alt.alder_gruppe_besk
       ,fylke_alt.nåværende_fylke_nr_navn
       ,count(distinct case when barn_selv_mottaker_flagg = 0 then fk_person1 end) as antall
       ,count(distinct case when barn_selv_mottaker_flagg = 1 then fk_person1 end) as antall_mottaker_barn
    from mottaker

    join {{ source('bt_statistikk_bank_dvh_fam_bt', 'dim_bt_alder_gruppe') }} alder_gruppe_alt
    on alder_gruppe_alt.alder_gruppe_besk = '00 I alt'

    join {{ source('bt_statistikk_bank_dvh_fam_bt', 'dim_bt_navarende_fylke') }} fylke_alt
    on fylke_alt.nåværende_fylkenavn = 'I alt'

    group by
        mottaker.aar_kvartal
       ,mottaker.kjonn_besk
       ,alder_gruppe_alt.alder_gruppe_besk
       ,fylke_alt.nåværende_fylke_nr_navn
)
,

--Gruppere på aar_kvartal, kjønn og aldersgruppe
kjonn_alder_gruppe as (
    select
        mottaker.aar_kvartal
       ,mottaker.kjonn_besk
       ,mottaker.alder_gruppe_besk
       ,fylke_alt.nåværende_fylke_nr_navn
       ,count(distinct case when barn_selv_mottaker_flagg = 0 then fk_person1 end) as antall
       ,count(distinct case when barn_selv_mottaker_flagg = 1 then fk_person1 end) as antall_mottaker_barn
    from mottaker

    join {{ source('bt_statistikk_bank_dvh_fam_bt', 'dim_bt_navarende_fylke') }} fylke_alt
    on fylke_alt.nåværende_fylkenavn = 'I alt'

    group by
        mottaker.aar_kvartal
       ,mottaker.kjonn_besk
       ,mottaker.alder_gruppe_besk
       ,fylke_alt.nåværende_fylke_nr_navn
)
,

--Gruppere på aar_kvartal, kjønn og fylke
kjonn_fylke as (
    select
        mottaker.aar_kvartal
       ,mottaker.kjonn_besk
       ,alder_gruppe_alt.alder_gruppe_besk
       ,mottaker.nåværende_fylke_nr_navn
       ,count(distinct case when barn_selv_mottaker_flagg = 0 then fk_person1 end) as antall
       ,count(distinct case when barn_selv_mottaker_flagg = 1 then fk_person1 end) as antall_mottaker_barn
    from mottaker

    join {{ source('bt_statistikk_bank_dvh_fam_bt', 'dim_bt_alder_gruppe') }} alder_gruppe_alt
    on alder_gruppe_alt.alder_gruppe_besk = '00 I alt'

    group by
        mottaker.aar_kvartal
       ,mottaker.kjonn_besk
       ,alder_gruppe_alt.alder_gruppe_besk
       ,mottaker.nåværende_fylke_nr_navn
)
,

--Gruppere på aar_kvartal, alder og fylke
alder_fylke as (
    select
        mottaker.aar_kvartal
       ,kjonn_alt.kjonn_besk
       ,mottaker.alder_gruppe_besk
       ,mottaker.nåværende_fylke_nr_navn
       ,count(distinct case when barn_selv_mottaker_flagg = 0 then fk_person1 end) as antall
       ,count(distinct case when barn_selv_mottaker_flagg = 1 then fk_person1 end) as antall_mottaker_barn
    from mottaker

    join {{ source('bt_statistikk_bank_dvh_fam_bt', 'dim_bt_kjonn') }} kjonn_alt
    on kjonn_alt.kjonn_besk = 'I alt'

    group by
        mottaker.aar_kvartal
       ,kjonn_alt.kjonn_besk
       ,mottaker.alder_gruppe_besk
       ,mottaker.nåværende_fylke_nr_navn
),

--Gruppere på aar_kvartal og fylke
fylke_i_alt as (
    select
        mottaker.aar_kvartal
       ,kjonn_alt.kjonn_besk
       ,alder_gruppe_alt.alder_gruppe_besk
       ,mottaker.nåværende_fylke_nr_navn
       ,count(distinct case when barn_selv_mottaker_flagg = 0 then fk_person1 end) as antall
       ,count(distinct case when barn_selv_mottaker_flagg = 1 then fk_person1 end) as antall_mottaker_barn
    from mottaker

    join {{ source('bt_statistikk_bank_dvh_fam_bt', 'dim_bt_alder_gruppe') }} alder_gruppe_alt
    on alder_gruppe_alt.alder_gruppe_besk = '00 I alt'

    join {{ source('bt_statistikk_bank_dvh_fam_bt', 'dim_bt_kjonn') }} kjonn_alt
    on kjonn_alt.kjonn_besk = 'I alt'

    group by
        mottaker.aar_kvartal
       ,kjonn_alt.kjonn_besk
       ,alder_gruppe_alt.alder_gruppe_besk
       ,mottaker.nåværende_fylke_nr_navn
)
,

--Gruppere på aar_kvartal og alder
alder_i_alt as (
    select
        mottaker.aar_kvartal
       ,kjonn_alt.kjonn_besk
       ,mottaker.alder_gruppe_besk
       ,fylke_alt.nåværende_fylke_nr_navn
       ,count(distinct case when barn_selv_mottaker_flagg = 0 then fk_person1 end) as antall
       ,count(distinct case when barn_selv_mottaker_flagg = 1 then fk_person1 end) as antall_mottaker_barn
    from mottaker

    join {{ source('bt_statistikk_bank_dvh_fam_bt', 'dim_bt_navarende_fylke') }} fylke_alt
    on fylke_alt.nåværende_fylkenavn = 'I alt'

    join {{ source('bt_statistikk_bank_dvh_fam_bt', 'dim_bt_kjonn') }} kjonn_alt
    on kjonn_alt.kjonn_besk = 'I alt'

    group by
        mottaker.aar_kvartal
       ,kjonn_alt.kjonn_besk
       ,mottaker.alder_gruppe_besk
       ,fylke_alt.nåværende_fylke_nr_navn
)
,

--I alt
i_alt as (
    select
        mottaker.aar_kvartal
       ,kjonn_alt.kjonn_besk
       ,alder_gruppe_alt.alder_gruppe_besk
       ,fylke_alt.nåværende_fylke_nr_navn
       ,count(distinct case when barn_selv_mottaker_flagg = 0 then fk_person1 end) as antall
       ,count(distinct case when barn_selv_mottaker_flagg = 1 then fk_person1 end) as antall_mottaker_barn
    from mottaker

    join {{ source('bt_statistikk_bank_dvh_fam_bt', 'dim_bt_navarende_fylke') }} fylke_alt
    on fylke_alt.nåværende_fylkenavn = 'I alt'

    join {{ source('bt_statistikk_bank_dvh_fam_bt', 'dim_bt_kjonn') }} kjonn_alt
    on kjonn_alt.kjonn_besk = 'I alt'

    join {{ source('bt_statistikk_bank_dvh_fam_bt', 'dim_bt_alder_gruppe') }} alder_gruppe_alt
    on alder_gruppe_alt.alder_gruppe_besk = '00 I alt'

    group by
        mottaker.aar_kvartal
       ,kjonn_alt.kjonn_besk
       ,alder_gruppe_alt.alder_gruppe_besk
       ,fylke_alt.nåværende_fylke_nr_navn
)
select *
from agg

union all
select *
from kjonn_i_alt

union all
select *
from kjonn_alder_gruppe

union all
select *
from kjonn_fylke

union all
select *
from alder_fylke

union all
select *
from fylke_i_alt

union all
select *
from alder_i_alt

union all
select *
from i_alt