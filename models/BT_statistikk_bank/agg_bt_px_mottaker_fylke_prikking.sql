{{
    config(
        materialized='table'
    )
}}

with mottaker as (
    select *
    from {{ source('bt_statistikk_bank_dvh_fam_bt','stg_fak_statistikk_bank_mottaker') }}
    where barn_selv_mottaker_flagg = 0 --Barn selv mottar barnetrygd telles ikke
)
,

fylke as (
    select *
    from {{ source('bt_statistikk_bank_dvh_fam_bt','dim_bt_navarende_fylke') }}
),

--Returnere en full liste med alle fylker og alle perioder. Dette er PX format spesifikk.
full_liste as (
    select
        fylke.*
       ,periode.aar_kvartal
       ,periode.forste_dato_i_perioden
       ,periode.siste_dato_i_perioden
    from {{ source('bt_statistikk_bank_dvh_fam_bt','dim_bt_navarende_fylke') }} fylke

    full outer join {{ source('bt_statistikk_bank_dvh_fam_bt', 'dim_bt_periode') }} periode
    on 1 = 1
)
,

--Gruppere på nivå aar_kvartal og fylke
agg as (
    select
        full_liste.aar_kvartal
       ,full_liste.nåværende_fylke_nr_navn
       ,full_liste.nåværende_fylkenavn
       ,case when count(distinct mottaker.fk_person1) < 10 then
                  round(count(distinct mottaker.fk_person1)+5, -1) --Prikking: Round antall mindre enn 5 oppover til nærmeste tier
             else count(distinct mottaker.fk_person1)
        end antall
    from full_liste

    left outer join mottaker
    on full_liste.aar_kvartal = mottaker.aar_kvartal
    and full_liste.nåværende_fylke_nr_navn = mottaker.nåværende_fylke_nr_navn

    where full_liste.nåværende_fylkenavn != 'I alt'

    group by
        full_liste.aar_kvartal
       ,full_liste.nåværende_fylke_nr_navn
       ,full_liste.nåværende_fylkenavn
)
,

--nåværende_fylkenavn = 'I alt'
i_alt as (
    select
        full_liste.aar_kvartal
       ,full_liste.nåværende_fylke_nr_navn
       ,full_liste.nåværende_fylkenavn
       ,sum(agg.antall) as antall
    from full_liste

    left outer join agg
    on full_liste.aar_kvartal = agg.aar_kvartal

    where full_liste.nåværende_fylkenavn = 'I alt'

    group by
        full_liste.aar_kvartal
       ,full_liste.nåværende_fylke_nr_navn
       ,full_liste.nåværende_fylkenavn
)
,

alle as (
    select
        agg.aar_kvartal
       ,agg.nåværende_fylke_nr_navn
       ,agg.antall
       ,case when i_alt.antall != 0 then round(agg.antall/i_alt.antall*100,1) else 0 end prosent
    from agg
    join i_alt
    on agg.aar_kvartal = i_alt.aar_kvartal

    union all
    select
        aar_kvartal
       ,nåværende_fylke_nr_navn
       ,antall
       ,100 as prosent
    from i_alt
)
select
    substr(aar_kvartal,1,4)||'K'||substr(aar_kvartal,6,1) as aar_kvartal
   ,nåværende_fylke_nr_navn
   ,antall
   ,prosent
   ,localtimestamp as lastet_dato
from alle