{{
    config(
        materialized='incremental'
    )
}}

with barn as (
    select *
    from {{ source('bt_statistikk_bank_dvh_fam_bt', 'stg_fak_statistikk_bank_barn') }}
)
,

--Returnere lik liste for alle fylke og alle periode. Dette er PX format spesifikk.
fylke_periode as (
    select fylke.*, periode.*
    from {{ source('bt_statistikk_bank_dvh_fam_bt','dim_bt_navarende_fylke') }} fylke

    full outer join {{ source('bt_statistikk_bank_dvh_fam_bt', 'dim_bt_periode') }} periode
    on 1 = 1
),

--Gruppere på nivå aar_kvartal og fylke
--Returnere alle fylker
agg as (
    select
        fylke_periode.aar_kvartal
       ,fylke_periode.nåværende_fylke_nr_navn
       ,fylke_periode.nåværende_fylkenavn
       ,count(distinct barn.fkb_person1) as antall
       ,1 as aggregert_nivaa
    from fylke_periode

    left outer join barn
    on fylke_periode.nåværende_fylke_nr_navn = barn.nåværende_fylke_nr_navn
    and fylke_periode.aar_kvartal = barn.aar_kvartal

    where fylke_periode.nåværende_fylkenavn != 'I alt'

    group by
        fylke_periode.aar_kvartal
       ,fylke_periode.nåværende_fylke_nr_navn
       ,fylke_periode.nåværende_fylkenavn
)
,

i_alt as (
    select
        agg.aar_kvartal
       ,fylke_periode.nåværende_fylke_nr_navn
       ,fylke_periode.nåværende_fylkenavn
       ,sum(agg.antall) as antall
       ,2 as aggregert_nivaa
    from fylke_periode

    join agg
    on fylke_periode.aar_kvartal = agg.aar_kvartal

    where fylke_periode.nåværende_fylkenavn = 'I alt'

    group by
        agg.aar_kvartal
       ,fylke_periode.nåværende_fylke_nr_navn
       ,fylke_periode.nåværende_fylkenavn
)
,

alle as (
    select
        agg.aar_kvartal
       ,agg.nåværende_fylke_nr_navn
       ,agg.antall
       ,case when i_alt.antall != 0 then round(agg.antall/i_alt.antall*100,1) else 0 end prosent
       ,agg.aggregert_nivaa
    from agg
    join i_alt
    on agg.aar_kvartal = i_alt.aar_kvartal

    union all
    select
        aar_kvartal
       ,nåværende_fylke_nr_navn
       ,antall
       ,round(100,1) as prosent
       ,aggregert_nivaa
    from i_alt
)
,

--Prikking: Round antall mindre enn 5 oppover til nærmeste tier
--Tester prikking
prikking as (
    select
        alle.aar_kvartal
       ,alle.nåværende_fylke_nr_navn
       ,alle.antall
       ,alle.aggregert_nivaa
       ,round(alle.antall+5, -1) as antall_prikking

       --Nivå opp
       ,nivaa_opp.nåværende_fylke_nr_navn nåværende_fylke_nr_navn_nivaa_opp
       ,nivaa_opp.antall as antall_nivaa_opp
       ,nivaa_opp.aggregert_nivaa aggregert_nivaa_nivaa_opp
       ,round(nivaa_opp.antall+5, -1)as antall_prikking_nivaa_opp
    from alle

    join alle nivaa_opp
    on alle.aar_kvartal = nivaa_opp.aar_kvartal
    and alle.aggregert_nivaa < nivaa_opp.aggregert_nivaa

    where alle.antall between 1 and 4530
),
prikking_liste as
(
    select distinct
           aar_kvartal
          ,nåværende_fylke_nr_navn
          ,antall
          ,aggregert_nivaa
          ,antall_prikking
    from prikking

    union all
    select distinct
           aar_kvartal
          ,nåværende_fylke_nr_navn_nivaa_opp
          ,antall_nivaa_opp
          ,aggregert_nivaa_nivaa_opp
          ,antall_prikking_nivaa_opp
    from prikking
)
--select * from prikking_liste where aar_kvartal = 202501;

select
    alle.aar_kvartal
   ,alle.nåværende_fylke_nr_navn
   ,coalesce(prikking_liste.antall_prikking, alle.antall) as antall
   ,alle.prosent
   ,alle.aggregert_nivaa
   ,localtimestamp as lastet_dato
from alle

left join prikking_liste
on alle.aar_kvartal = prikking_liste.aar_kvartal
and alle.nåværende_fylke_nr_navn = prikking_liste.nåværende_fylke_nr_navn

--Last opp kun ny periode siden siste periode fra tabellen
{% if is_incremental() %}

where aar_kvartal > (select coalesce(max(aar_kvartal), 201500) from {{ this }})

{% endif %}