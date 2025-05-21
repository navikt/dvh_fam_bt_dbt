{{
    config(
        materialized='table'
    )
}}

with mottaker as (
    select *
    from {{ source('bt_statistikk_bank_dvh_fam_bt', 'stg_fak_statistikk_bank_mottaker') }}
)
,

fylke as (
    select *
    from {{ source('bt_statistikk_bank_dvh_fam_bt','dim_bt_navarende_fylke') }}
),

--Gruppere på nivå aar_kvartal og fylke
--Returnere alle fylker
agg as (
    select
        mottaker.aar_kvartal
       ,fylke.nåværende_fylke_nr_navn
       ,fylke.nåværende_fylkenavn
       ,count(distinct case when mottaker.barn_selv_mottaker_flagg = 0 then mottaker.fk_person1 end) as antall
       ,count(distinct case when mottaker.barn_selv_mottaker_flagg = 1 then mottaker.fk_person1 end) as antall_mottaker_barn
    from fylke

    left outer join mottaker
    on fylke.nåværende_fylke_nr_navn = mottaker.nåværende_fylke_nr_navn

    where fylke.nåværende_fylkenavn != 'I alt'

    group by
        mottaker.aar_kvartal
       ,fylke.nåværende_fylke_nr_navn
       ,fylke.nåværende_fylkenavn
)
,

i_alt as (
    select
        agg.aar_kvartal
       ,fylke.nåværende_fylke_nr_navn
       ,fylke.nåværende_fylkenavn
       ,sum(agg.antall) as antall
       ,count(agg.antall_mottaker_barn) as antall_mottaker_barn
    from fylke

    join agg
    on 1 = 1

    where fylke.nåværende_fylkenavn = 'I alt'

    group by
        agg.aar_kvartal
       ,fylke.nåværende_fylke_nr_navn
       ,fylke.nåværende_fylkenavn
)
,

alle as (
    select
        agg.aar_kvartal
       ,agg.nåværende_fylke_nr_navn
       ,agg.antall
       ,round(agg.antall/i_alt.antall*100,1) as prosent
       ,agg.antall_mottaker_barn
    from agg
    join i_alt
    on agg.aar_kvartal = i_alt.aar_kvartal

    union all
    select
        aar_kvartal
       ,nåværende_fylke_nr_navn
       ,antall
       ,round(100,1) as prosent
       ,antall_mottaker_barn
    from i_alt
)
select alle.*
      ,localtimestamp as lastet_dato
from alle